"""Maryland bulk bill details with complete histories from official bill pages."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io

import csv
from datetime import date, datetime
import json
from pathlib import Path
import re
import time
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL = 'https://mgaleg.maryland.gov'
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = ['bill_number', 'session', 'session_year', 'title', 'main_sponsor', 'all_sponsors',
                  'status', 'crossfile_bill', 'committees', 'general_topics', 'subjects', 'summary', 'bill_url', 'term']
HISTORY_HEADER = ['bill_number', 'session', 'session_year', 'chamber', 'action_date', 'action', 'order',
                  'term', 'calendar_date', 'document_urls']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}', term):
        raise ValueError('MD term must be YYYY_YYYY, for example 2025_2026')
    start, end = map(int, term.split('_'))
    if start % 2 != 1 or end != start + 1 or end > date.today().year:
        raise ValueError('MD requires an odd-starting consecutive two-year term without future years')
    return start, end


def select_sessions(html, term):
    years = parse_term(term)
    result = []
    for option in BeautifulSoup(html, 'lxml').select('#valueSessions option[value]'):
        key = option['value'].lower()
        if re.fullmatch(r'\d{4}(?:rs|s\d+)', key) and int(key[:4]) in years:
            result.append({'id': key, 'year': int(key[:4]), 'label': option.get_text(' ', strip=True)})
    if not all(str(y) + 'rs' in {s['id'] for s in result} for y in years):
        raise ValueError(f'MD did not list both regular sessions for {term}')
    if len(result) != len({s['id'] for s in result}):
        raise ValueError('MD duplicate sessions')
    return sorted(result, key=lambda s: s['id'])


def parse_index(html, session):
    soup = BeautifulSoup(html, 'lxml')
    table = soup.select_one('#billIndex')
    if table is None or table.find('tbody') is None:
        raise ValueError(f'MD missing chamber index: {session["id"]}')
    records = {}
    for row in table.select('tbody tr'):
        cell = row.find('td')
        link = cell.find('a', href=True) if cell else None
        if link is None:
            raise ValueError('MD malformed bill index row')
        number = link.get_text(strip=True).upper()
        if not re.fullmatch(r'[HS][BJS]\d{4}', number):
            raise ValueError(f'MD unexpected bill index number: {number}')
        url = urljoin(BASE_URL, link['href'])
        query = parse_qs(urlparse(url).query)
        if query.get('ys', [''])[0].lower() != session['id']:
            # Current-session links can omit ys; add it explicitly for stable output.
            if 'ys' in query:
                raise ValueError('MD wrong-session bill link')
            url += ('&' if '?' in url else '?') + 'ys=' + session['id']
        if number in records:
            raise ValueError('MD duplicate chamber-index bill')
        records[number] = url
    return records


def validate_bulk(data, session, index):
    if not isinstance(data, list):
        raise ValueError('MD malformed bulk data')
    records = {}
    for bill in data:
        number = bill['BillNumber']
        if number in records or bill['YearAndSession'] != session['label']:
            raise ValueError('MD duplicate or wrong-session bulk bill')
        if not bill.get('Title') or 'Sponsors' not in bill:
            raise ValueError(f'MD incomplete bulk record: {number}')
        records[number] = bill
    if set(records) != set(index):
        raise ValueError(f'MD bulk/index disagreement: bulk-only {sorted(set(records)-set(index))[:10]}, index-only {sorted(set(index)-set(records))[:10]}')
    return records


def _date(value):
    return datetime.strptime(value, '%m/%d/%Y').date().isoformat() if value else ''


def parse_bill(bill, html, session, url, term):
    soup = BeautifulSoup(html, 'lxml')
    heading = soup.select_one('h2 a')
    number = bill['BillNumber']
    if heading is None or heading.get_text(strip=True).upper() != number:
        raise ValueError(f'MD bill identity mismatch: {url}')
    if not urlparse(urljoin(BASE_URL, heading.get('href', ''))).path.lower().startswith('/' + session['id'] + '/'):
        raise ValueError(f'MD bill page has wrong session: {url}')
    committees = list(dict.fromkeys(bill.get(k) for k in (
        'CommitteePrimaryOrigin', 'CommitteeSecondaryOrigin', 'CommitteePrimaryOpposite', 'CommitteeSecondaryOpposite') if bill.get(k)))
    primary = re.sub(r'^(?:Delegate|Senator)\s+', '', bill.get('SponsorPrimary') or '')
    sponsors = '; '.join(s['Name'] for s in bill['Sponsors'])
    details = [number, session['id'], session['year'], bill['Title'], primary, sponsors,
               bill.get('Status') or '', bill.get('CrossfileBillNumber') or '', '; '.join(committees),
               '; '.join(s['Name'] for s in bill.get('BroadSubjects') or []),
               '; '.join(s['Name'] for s in bill.get('NarrowSubjects') or []), bill.get('Synopsis') or '', url, term]
    # The page repeats its history in desktop/mobile layouts; consume just one.
    table = soup.select_one('#detailsHistoryMobile')
    if table is None:
        raise ValueError(f'MD missing complete history table: {url}')
    histories = []
    for block in table.select('dl'):
        fields = {}
        for label in block.select('dt'):
            value = label.find_next_sibling('dd')
            if value is None: raise ValueError('MD malformed history field')
            fields[label.get_text(strip=True)] = value
        if not all(k in fields for k in ('Chamber', 'Calendar Date', 'Legislative Date', 'Action')):
            raise ValueError('MD missing history fields')
        action = fields['Action'].get_text(' ', strip=True)
        if not action: raise ValueError('MD empty history action')
        documents = '; '.join(urljoin(BASE_URL, a['href']) for a in fields['Action'].select('a[href]'))
        # Document rows have blank dates/chambers in the source. Preserve them,
        # without inventing a date from adjacent legislative actions.
        histories.append([number, session['id'], session['year'], fields['Chamber'].get_text(strip=True),
                          _date(fields['Legislative Date'].get_text(strip=True)), action, len(histories)+1,
                          term, _date(fields['Calendar Date'].get_text(strip=True)), documents])
    if not histories or not any(row[4] for row in histories):
        raise ValueError(f'MD missing dated legislative history: {url}')
    return details, histories


def _fetch(http, url):
    response = http.get(url, timeout=60)
    response.raise_for_status()
    time.sleep(1)
    return response


@scrape_run
def scrape(state, term, verbose=False, force_fetch=False):
    if state.upper() != 'MD': raise ValueError('Maryland scraper requires state MD')
    parse_term(term)
    folder = REPO_ROOT / '.data/MD/bill'
    outputs = [folder / f'MD_{name}_{term}.csv' for name in ('Bill_Details', 'Bill_Histories')]
    manifest = folder / f'.MD_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs, manifest]) and not force_fetch:
        print(f'Skipping MD {term}: completed outputs exist (use --force-fetch to refresh)'); return
    cache = folder / '.cache' / term
    cache.mkdir(parents=True, exist_ok=True)
    details, histories, counts = [], [], {}
    with make_session() as http:
        sessions = select_sessions(_fetch(http, BASE_URL + '/mgawebsite/search/legislation').text, term)
        for session in sessions:
            index = {}
            for chamber in ('house', 'senate'):
                local = cache / f'{session["id"]}_{chamber}_index.html'
                url = BASE_URL + f'/mgawebsite/Legislation/Index/{chamber}?ys={session["id"]}'
                html = cache_io.read_text(local, encoding='utf-8') if cache_io.exists(local) and not force_fetch else _fetch(http, url).text
                group = parse_index(html, session)
                if set(group) & set(index): raise ValueError('MD duplicate bill across chamber indexes')
                index.update(group)
                cache_io.write_text(local, html, encoding='utf-8')
            local = cache / f'{session["id"]}_bulk.json'
            if cache_io.exists(local) and not force_fetch:
                data = json.loads(cache_io.read_text(local, encoding='utf-8'))
            else:
                response = _fetch(http, BASE_URL + f'/{session["id"]}/misc/billsmasterlist/legislation.json')
                if not response.text.strip() and not index:
                    # Listed 2025 special session: both official chamber indexes
                    # are empty and the bulk endpoint returns only whitespace.
                    data = []
                else:
                    data = response.json()
            records = validate_bulk(data, session, index)
            pending = local.with_suffix('.json.tmp'); cache_io.write_text(pending, json.dumps(data), encoding='utf-8'); cache_io.replace(pending, local)
            counts[session['id']] = len(records)
            print(f'MD {session["id"]}: {len(records)} bills/resolutions; bulk/index counts agree', flush=True)
            for i, (number, bill) in enumerate(records.items(), 1):
                local = cache / f'{session["id"]}_{number}.html'
                html = cache_io.read_text(local, encoding='utf-8') if cache_io.exists(local) and not force_fetch else _fetch(http, index[number]).text
                parsed = parse_bill(bill, html, session, index[number], term)
                pending = local.with_suffix('.html.tmp'); cache_io.write_text(pending, html, encoding='utf-8'); cache_io.replace(pending, local)
                details.append(parsed[0]); histories.extend(parsed[1])
                if verbose or i % 25 == 0 or i == len(records):
                    print(f'MD {session["id"]} {i}/{len(records)} {number}: {len(parsed[1])} history rows', flush=True)
    staged = []
    try:
        for target, header, rows in zip(outputs, (DETAILS_HEADER, HISTORY_HEADER), (details, histories)):
            pending = target.with_suffix('.csv.tmp'); staged.append((pending, target))
            with pending.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle); writer.writerow(header); writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending, target in staged: cache_io.replace(pending, target)

        write_manifest(manifest, {'term': term, 'details': len(details), 'histories': len(histories), 'session_counts': counts})
    finally:
        for pending, _ in staged: cache_io.unlink(pending, missing_ok=True)
    print(f'MD {term}: saved {len(details):,} bills/resolutions and {len(histories):,} history rows')
