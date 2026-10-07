"""Arkansas House and Senate bills for one explicitly selected biennium.

Uses the public, paginated bill index and bill status pages. As in Connor's
Arkansas scraper, the scope is HB/SB bills (not resolutions).
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

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = 'https://www.arkleg.state.ar.us'
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = ['bill_num', 'ga_num', 'session', 'primary_sponsors', 'act_num',
                  'intro_date', 'cosponsors', 'title', 'bill_url', 'term', 'session_id']
HISTORY_HEADER = ['bill_num', 'ga_num', 'session', 'chamber', 'action_date', 'action',
                  'order', 'vote_url', 'term', 'session_id']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}', term):
        raise ValueError('AR term must be YYYY_YYYY, for example 2025_2026')
    start, end = map(int, term.split('_'))
    if start % 2 != 1 or end != start + 1:
        raise ValueError('AR term must start in an odd year and end the next year')
    if end > date.today().year:
        raise ValueError(f'AR term {term} includes a future year')
    return start, end


def select_sessions(html, term):
    start, end = parse_term(term)
    soup = BeautifulSoup(html, 'lxml')
    sessions = []
    for option in soup.select('#ddBienniumSessionViewBills option[value]'):
        value = option['value']
        if value.startswith(f'{start}/'):
            label = option.get_text(' ', strip=True)
            if not re.search(rf'\b({start}|{end})$', label):
                raise ValueError(f'Arkansas session outside term: {label}')
            sessions.append({'id': value, 'name': label})
    if not sessions or len({s['id'] for s in sessions}) != len(sessions):
        raise ValueError(f'AR has no unambiguous sessions for {term}')
    return sorted(sessions, key=lambda s: s['id'])


def parse_listing(html, session_id, bill_type, page_url):
    soup = BeautifulSoup(html, 'lxml')
    bills = {}
    for a in soup.select('div.measureTitle a[href]'):
        url = urljoin(BASE_URL, a['href'])
        params = parse_qs(urlparse(url).query)
        number = a.get_text(strip=True)
        if (params.get('ddBienniumSession') != [session_id] or params.get('id') != [number]
                or not re.fullmatch(bill_type + r'\d+', number)):
            raise ValueError(f'Unexpected Arkansas bill link: {url}')
        # Mobile and desktop links intentionally repeat each bill.
        bills[number] = url
    footer = soup.select_one('.tableSectionFooter')
    if not bills:
        content = soup.select_one('#bodyContent')
        if content and re.search(r'no (bills|records|results)', content.get_text(' ', strip=True), re.I):
            return [], None
        raise ValueError(f'Empty or unrecognized Arkansas index: {page_url}')
    match = re.search(r'Page (\d+) of (\d+)', footer.get_text(' ', strip=True) if footer else '')
    if not match:
        raise ValueError('Arkansas pagination is missing')
    page, last = map(int, match.groups())
    next_url = None
    if page < last:
        link = next((a for a in footer.select('a[href]') if a.get_text(strip=True) == str(page + 1)), None)
        if not link:
            raise ValueError(f'Arkansas missing page {page + 1}')
        next_url = urljoin(page_url, link['href'])
    return list(bills.items()), next_url


def _date(text):
    return datetime.strptime(text.split()[0], '%m/%d/%Y').date().isoformat() if text else ''


def parse_bill(html, number, session, term, url):
    soup = BeautifulSoup(html, 'lxml')
    heading = soup.select_one('h1')
    label = heading.get_text(' ', strip=True) if heading else ''
    match = re.match(r'([A-Z]+\d+)\s+-\s+(.+)', label)
    if not match or match[1] != number:
        raise ValueError(f'Arkansas bill identity/title missing: {number}')
    fields = {}
    for cell in soup.select('div[role=gridcell]'):
        name = cell.get_text(' ', strip=True)
        if name in ('Lead Sponsor:', 'Act Number:', 'Introduction Date:', 'Co-Sponsors:'):
            value = cell.find_next_sibling('div', role='gridcell')
            if value:
                fields[name] = value.get_text(' ', strip=True)
    if not fields.get('Lead Sponsor:') or not fields.get('Introduction Date:'):
        raise ValueError(f'Arkansas sponsor/introduction missing: {number}')
    history_heading = next((h for h in soup.select('h3') if h.get_text(strip=True) == 'Bill Status History'), None)
    grid = history_heading.find_next('div', role='grid') if history_heading else None
    if grid is None:
        raise ValueError(f'Arkansas history missing: {number}')
    # General Assembly numbering: the 1987 biennium is the 76th Assembly.
    assembly = 76 + (int(term[:4]) - 1987) // 2
    suffix = 'th' if 10 <= assembly % 100 <= 20 else {1: 'st', 2: 'nd', 3: 'rd'}.get(assembly % 10, 'th')
    ga = f'{assembly}{suffix} General Assembly'
    histories = []
    rows = [row for row in grid.select('div[role=row]') if row.select('div[role=gridcell]')]
    for i, row in enumerate(rows):
        cells = {int(c['aria-colindex']): c for c in row.select('div[role=gridcell][aria-colindex]')}
        if not all(k in cells for k in (1, 2, 3, 4)):
            raise ValueError(f'Malformed Arkansas history: {number}')
        action = cells[3].get_text(' ', strip=True)
        if not action:
            raise ValueError(f'Empty Arkansas action: {number}')
        vote = cells[4].select_one('a[href]')
        histories.append([number, ga, session['name'], cells[1].get_text(strip=True),
                          _date(cells[2].get_text(' ', strip=True)), action, len(rows) - i,
                          urljoin(BASE_URL, vote['href']) if vote else '', term, session['id']])
    if not histories:
        raise ValueError(f'Empty Arkansas history: {number}')
    act = re.search(r'\d+', fields.get('Act Number:', ''))
    details = [number, ga, session['name'], fields['Lead Sponsor:'], act[0] if act else '',
               _date(fields['Introduction Date:']), fields.get('Co-Sponsors:', ''), match[2], url, term, session['id']]
    return details, histories


def _fetch(http, url):
    response = http.get(url, timeout=45)
    response.raise_for_status()
    time.sleep(1)
    return response.text


@scrape_run
def scrape(state, term, verbose=False, force_fetch=False):
    if state.upper() != 'AR':
        raise ValueError('Arkansas scraper requires state AR')
    start, _ = parse_term(term)
    folder = REPO_ROOT / '.data/AR/bill'
    detail_path = folder / f'AR_Bill_Details_{term}.csv'
    history_path = folder / f'AR_Bill_Histories_{term}.csv'
    manifest = folder / f'.AR_scrape_{term}.json'
    if all(cache_io.exists(p) for p in (detail_path, history_path, manifest)) and not force_fetch:
        print(f'Skipping AR {term}: completed outputs exist (use --force-fetch to refresh)')
        return
    cache = folder / '.cache' / term
    cache.mkdir(parents=True, exist_ok=True)
    details, histories, counts = [], [], {}
    with requests.Session() as http:
        http.mount('https://', HTTPAdapter(max_retries=Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])))
        sessions = select_sessions(_fetch(http, BASE_URL + '/Bills/ViewBills?' + urlencode({'type': 'HB', 'ddBienniumSession': f'{start}/{start}R'})), term)
        for session in sessions:
            bills = {}
            for kind in ('HB', 'SB'):
                url = BASE_URL + '/Bills/ViewBills?' + urlencode({'type': kind, 'ddBienniumSession': session['id']})
                visited = set()
                while url:
                    if url in visited:
                        raise ValueError('Arkansas pagination loop')
                    visited.add(url)
                    records, url = parse_listing(_fetch(http, url), session['id'], kind, url)
                    for number, bill_url in records:
                        if number in bills:
                            raise ValueError(f'Duplicate Arkansas index bill: {number}')
                        bills[number] = bill_url
            if not bills:
                raise ValueError(f'No Arkansas bills for {session}')
            counts[session['id']] = len(bills)
            print(f"AR {session['name']}: {len(bills)} bills", flush=True)
            for i, (number, url) in enumerate(bills.items(), 1):
                path = cache / f"{session['id'].replace('/', '_')}_{number}.html"
                if cache_io.exists(path) and not force_fetch:
                    html = cache_io.read_text(path, encoding='utf-8')
                    parsed = parse_bill(html, number, session, term, url)
                else:
                    html = _fetch(http, url)
                    parsed = parse_bill(html, number, session, term, url)
                    pending = path.with_suffix('.html.tmp')
                    cache_io.write_text(pending, html, encoding='utf-8')
                    cache_io.replace(pending, path)
                details.append(parsed[0])
                histories.extend(parsed[1])
                if verbose or i % 25 == 0 or i == len(bills):
                    print(f'  {i}/{len(bills)} {number}: {len(parsed[1])} actions', flush=True)
    staged = []
    try:
        for path, header, rows in ((detail_path, DETAILS_HEADER, details), (history_path, HISTORY_HEADER, histories)):
            pending = path.with_suffix('.csv.tmp')
            staged.append((pending, path))
            with pending.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow(header)
                writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending, path in staged:
            cache_io.replace(pending, path)

        write_manifest(manifest, {'term': term, 'details': len(details), 'histories': len(histories), 'session_counts': counts})
    finally:
        for pending, _ in staged:
            cache_io.unlink(pending, missing_ok=True)
    print(f'AR {term}: saved {len(details):,} bills and {len(histories):,} history rows')
