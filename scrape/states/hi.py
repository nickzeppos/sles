"""Hawaii bill and resolution details/history for one requested biennium.

HB/SB measures carry over, so prefer the even-year copy and add odd-year-only
bills. Resolution numbering resets annually; retain both years separately.
"""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io

import csv
from datetime import date, datetime
from pathlib import Path
import re
import time
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL = 'https://data.capitol.hawaii.gov'
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = ['bill_number', 'session', 'session_type', 'title', 'summary', 'report_title',
                  'companion_bills', 'package', 'introducers', 'bill_url', 'source_year', 'session_id']
HISTORY_HEADER = ['bill_number', 'session', 'session_type', 'chamber', 'action_date', 'action', 'order', 'source_year', 'session_id']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}', term):
        raise ValueError('HI term must be YYYY_YYYY, for example 2025_2026')
    start, end = map(int, term.split('_'))
    if start % 2 != 1 or end != start + 1:
        raise ValueError('HI term must start in an odd year and end the next year')
    if end > date.today().year:
        raise ValueError(f'HI term {term} includes a future year')
    return start, end


def parse_directory(html, year):
    soup = BeautifulSoup(html, 'lxml')
    records = {}
    for link in soup.select('a[href]'):
        name = link.get_text(strip=True)
        match = re.match(r'^(HB|SB|HCR|SCR|HR|SR)(\d+)(?:_|-|\.)', name, re.I)
        if not match or not re.search(r'\.(?:pdf|html?)$', name, re.I):
            continue
        if f'/sessions/session{year}/bills/' not in link['href'].lower():
            raise ValueError(f'HI directory link from wrong year: {link["href"]}')
        prefix, number = match[1].upper(), str(int(match[2]))
        bill = prefix + number
        records[bill] = {'number': bill, 'year': year, 'session_id': str(year), 'kind': 'Regular',
                         'url': BASE_URL + '/session/archives/measure_indiv_Archives.aspx?' + urlencode({'billtype': prefix, 'billnumber': number, 'year': year})}
    if not records:
        raise ValueError(f'HI bill directory is empty or unrecognized: {year}')
    return list(records.values())


def combine_years(later, earlier):
    seen_bills = {r['number'] for r in later if re.fullmatch(r'[HS]B\d+', r['number'])}
    return later + [r for r in earlier if r['number'] not in seen_bills]


def parse_special(html, session_id):
    soup = BeautifulSoup(html, 'lxml')
    table = soup.find('table', id=re.compile(r'GridViewReports$'))
    if table is None:
        raise ValueError(f'HI special session index missing: {session_id}')
    if 'No records at this time.' in table.get_text(' ', strip=True):
        return []
    records = {}
    for link in table.select('a[href]'):
        url = urljoin(BASE_URL, link['href'])
        if 'measure_indiv' not in url.lower():
            continue
        params = {k.lower():v for k,v in parse_qs(urlparse(url).query).items()}
        if params.get('year', [''])[0].lower() != session_id.lower():
            raise ValueError(f'HI wrong special session link: {url}')
        prefix = params.get('billtype', [''])[0].upper()
        number = params.get('billnumber', [''])[0]
        if prefix in ('GM', 'JC', 'DC'):
            continue
        if prefix not in ('HB', 'SB', 'HR', 'SR', 'HCR', 'SCR') or not number.isdigit():
            raise ValueError(f'HI unrecognized special-session measure: {url}')
        bill = prefix + str(int(number))
        records[bill] = {'number': bill, 'year': int(session_id[:4]), 'session_id': session_id,
                         'kind': f'Special ({session_id})', 'url': url}
    return list(records.values())


def parse_bill(html, record, term):
    soup = BeautifulSoup(html, 'lxml')
    if re.search(r'Measure .+? not found', soup.get_text(), re.I):
        raise ValueError(f'HI measure does not exist: {record["url"]}')
    title = soup.title.get_text(' ', strip=True) if soup.title else ''
    number = record['number']
    match = re.fullmatch(r'([A-Z]+)(\d+)', number)
    if not re.search(rf'\b{match[1]}\s*{match[2]}\b', title, re.I):
        raise ValueError(f'HI bill identity missing/mismatched: {number}: {title}')
    table = soup.select_one('#measure-info')
    if table is None:
        raise ValueError(f'HI measure details missing: {number}')
    fields = {}
    for row in table.select('tr'):
        label, value = row.find('th'), row.find('td')
        if label and value:
            fields[label.get_text(' ', strip=True).rstrip(':').strip()] = value.get_text(' ', strip=True)
    if not fields.get('Measure Title') or not fields.get('Introducer(s)'):
        raise ValueError(f'HI title/introducer missing: {number}')
    histories = []
    table = soup.find('table', id=re.compile(r'GridViewStatus$'))
    if table is None:
        raise ValueError(f'HI history missing: {number}')
    rows = [r for r in table.select('tr') if r.find('td')]
    for i, row in enumerate(rows):
        cells = row.find_all('td')
        if len(cells) != 3:
            raise ValueError(f'HI malformed history: {number}')
        when = datetime.strptime(cells[0].get_text(strip=True), '%m/%d/%Y').date().isoformat()
        action = cells[2].get_text(' ', strip=True)
        if not action:
            raise ValueError(f'HI empty action: {number}')
        histories.append([number, term, record['kind'], cells[1].get_text(strip=True), when, action,
                          len(rows)-i, record['year'], record['session_id']])
    if not histories:
        raise ValueError(f'HI empty history: {number}')
    detail = [number, term, record['kind'], fields['Measure Title'], fields.get('Description', ''),
              fields.get('Report Title', ''), fields.get('Companion', ''), fields.get('Package', ''),
              fields['Introducer(s)'].replace(', ', '; '), record['url'], record['year'], record['session_id']]
    return detail, histories


class MeasureNotFound(ValueError):
    """The official status site explicitly reports that a measure does not exist."""


def _fetch(http, url):
    response = http.get(url, timeout=60)
    response.raise_for_status()
    missing_heading=any(h.get_text(' ',strip=True)=='Measure Not Found' for h in BeautifulSoup(response.text,'lxml').select('h1')) if 'measurenotfound' in response.url.lower() else False
    if 'measurenotfound' in response.url.lower() and ('This measure does not exist' in response.text or missing_heading):
        raise MeasureNotFound(f'HI source reports nonexistent measure: {url}')
    if 'measurenotfound' in response.url.lower() or 'errorpage' in response.url.lower():
        raise ValueError(f'HI requested measure redirected to an error page: {url}')
    time.sleep(1)
    return response.text


@scrape_run
def scrape(state, term, verbose=False, force_fetch=False):
    if state.upper() != 'HI':
        raise ValueError('Hawaii scraper requires state HI')
    start, end = parse_term(term)
    folder = REPO_ROOT / '.data/HI/bill'
    outputs = [folder / f'HI_{name}_{term}.csv' for name in ('Bill_Details', 'Bill_Histories')]
    manifest = folder / f'.HI_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs, manifest]) and not force_fetch:
        print(f'Skipping HI {term}: completed outputs exist (use --force-fetch to refresh)')
        return
    cache = folder / '.cache' / term
    cache.mkdir(parents=True, exist_ok=True)
    details, histories, special_counts, missing = [], [], {}, []
    with make_session() as http:
        indexes = {}
        for year in (end, start):
            indexes[year] = parse_directory(_fetch(http, f'{BASE_URL}/sessions/session{year}/bills/'), year)
        records = combine_years(indexes[end], indexes[start])
        # Keep Connor's bounded special-session checks, but require explicit
        # empty grids and never reuse a previous session's table on failure.
        for year in (start, end):
            for letter in 'abcde':
                session_id = f'{year}{letter}'
                rows = parse_special(_fetch(http, f'{BASE_URL}/session/splsession.aspx?year={session_id}'), session_id)
                special_counts[session_id] = len(rows)
                records.extend(rows)
        keys = [(r['number'], r['session_id']) for r in records]
        if len(keys) != len(set(keys)):
            raise ValueError('HI duplicate measure/session identities')
        print(f'HI {term}: {len(records)} bills/resolutions; carried bills deduplicated, annual resolutions retained', flush=True)
        for i, record in enumerate(records, 1):
            local = cache / f'{record["session_id"]}_{record["number"]}.html'
            try:
                html = cache_io.read_text(local, encoding='utf-8') if cache_io.exists(local) and not force_fetch else _fetch(http, record['url'])
            except MeasureNotFound:
                missing.append(record)
                print(f'HI source explicitly reports missing measure: {record["session_id"]} {record["number"]}', flush=True)
                continue
            parsed = parse_bill(html, record, term)
            pending = local.with_suffix('.html.tmp'); cache_io.write_text(pending, html, encoding='utf-8'); cache_io.replace(pending, local)
            details.append(parsed[0]); histories.extend(parsed[1])
            if verbose or i % 25 == 0 or i == len(records):
                print(f'HI {i}/{len(records)} {record["session_id"]} {record["number"]}: {len(parsed[1])} actions', flush=True)
    staged = []
    try:
        for target, header, rows in zip(outputs, (DETAILS_HEADER, HISTORY_HEADER), (details, histories)):
            pending = target.with_suffix('.csv.tmp'); staged.append((pending, target))
            with pending.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle); writer.writerow(header); writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending, target in staged: cache_io.replace(pending, target)

        write_manifest(manifest, {'term': term, 'details': len(details), 'histories': len(histories), 'special_session_counts': special_counts, 'source_missing_measures': missing})
    finally:
        for pending, _ in staged: cache_io.unlink(pending, missing_ok=True)
    print(f'HI {term}: saved {len(details):,} bills/resolutions and {len(histories):,} histories')
