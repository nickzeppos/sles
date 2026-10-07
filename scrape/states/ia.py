"""Iowa Bill Book details and histories for an explicit legislative term."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io

import csv
from datetime import date, datetime
import json
from pathlib import Path
import re
import time
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL = 'https://www.legis.iowa.gov'
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = ['bill_number', 'session', 'session_dates', 'primary_sponsor', 'cosponsors',
                  'floor_managers', 'H_floor_manager', 'S_floor_manager', 'summary',
                  'related_bills', 'companion_bills', 'bill_url', 'lobbying_url', 'term', 'sponsors_source', 'supporting_document_urls']
HISTORY_HEADER = ['bill_number', 'session', 'action_date', 'action', 'order', 'term']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}', term):
        raise ValueError('IA term must be YYYY_YYYY, for example 2025_2026')
    start, end = map(int, term.split('_'))
    if start % 2 != 1 or end != start + 1 or end > date.today().year:
        raise ValueError('IA requires an odd-starting consecutive two-year term without future years')
    return start, end


def select_session(html, term):
    start, end = parse_term(term)
    matches = []
    for option in BeautifulSoup(html, 'lxml').select('select[name=gaList] option[value]'):
        dates = re.search(r'\((\d{2}/\d{2}/\d{4}) - (\d{2}/\d{2}/\d{4})\)', option.get_text())
        if dates and int(dates[1][-4:]) == start and int(dates[2][-4:]) == end + 1:
            matches.append({'id': option['value'], 'dates': f'{dates[1]} - {dates[2]}'})
    if len(matches) != 1:
        raise ValueError(f'IA expected one assembly for {term}, got {matches}')
    return matches[0]


def parse_listing(html, session):
    soup = BeautifulSoup(html, 'lxml')
    chosen = soup.select_one('select[name=gaList] option[selected]')
    if chosen is None or chosen['value'] != session['id']:
        raise ValueError('IA wrong assembly selected in bill index')
    records = []
    for chamber in ('house', 'senate'):
        options = soup.select(f'#{chamber}Select option[value]')
        if not options:
            raise ValueError(f'IA missing {chamber} bill list')
        for option in options:
            number = option['value'].strip()
            if number == '-1':
                continue
            if not re.fullmatch(r'[HS](?:F|SB|JR|CR|R) \d+', number):
                raise ValueError(f'IA unexpected bill number: {number}')
            records.append(number)
    if not records or len(records) != len(set(records)):
        raise ValueError('IA empty or duplicate bill index')
    return records


def bill_url(session, number):
    return BASE_URL + '/legislation/billTracking/billHistory?' + urlencode({
        'ga': session['id'], 'billName': number.replace(' ', '')})


def parse_bill(html, related_html, session, number, term):
    soup = BeautifulSoup(html, 'lxml')
    if soup.select_one('.notificationBar.error'):
        raise ValueError(f'IA source reports an error: {number}')
    main = soup.select_one('div.divideVert')
    if main is None:
        raise ValueError(f'IA missing bill content: {number}')
    identity = main.find('a', href=True)
    query = parse_qs(urlparse(identity['href']).query) if identity else {}
    if query.get('ga') != [session['id']] or query.get('ba', [''])[0].replace(' ', '') != number.replace(' ', ''):
        raise ValueError(f'IA bill/session mismatch: {number}')
    table = main.find_next('table', class_='billActionTable')
    if table is None:
        raise ValueError(f'IA missing history table: {number}')
    # Only this bill's header/table; subsequent tables belong to related bills.
    header = main
    sponsor = header.select_one('div.divideVert') if header else None
    if sponsor is None:
        raise ValueError(f'IA missing sponsor/summary section: {number}')
    sponsor_text = sponsor.get_text(' ', strip=True)
    if sponsor_text and not sponsor_text.startswith('By '):
        raise ValueError(f'IA unexpected sponsor format: {number}')
    names = re.sub(r'^By\s+', '', sponsor_text)
    parts = names.split(', ', 1)
    primary = parts[0]
    cosponsors = parts[1].replace(' and ', '; ').replace(', ', '; ') if len(parts) == 2 else ''
    summary_node = sponsor.find_next_sibling('div')
    summary = summary_node.get_text(' ', strip=True) if summary_node else ''
    if not summary:
        raise ValueError(f'IA empty summary: {number}')
    floor = header.find(string=re.compile('Floor Managers:'))
    floor_names = '; '.join(a.get_text(' ', strip=True) for a in floor.parent.select('a')) if floor else ''
    related = '; '.join(a.get_text(' ', strip=True) for a in header.select('a[href^="#"]'))
    companion = header.select_one('span[title="Companion Bills"]')
    companions = '; '.join(a.get_text(' ', strip=True) for a in companion.parent.select('a')) if companion else ''
    lobby = header.find('a', string='Lobbyist Declarations')
    lobby_url = urljoin(BASE_URL, lobby['href']) if lobby else ''
    info = BeautifulSoup(related_html, 'lxml').select_one('table.billRelatedInfo')
    if info is None:
        raise ValueError(f'IA missing related-info response: {number}')
    related_ids = [parse_qs(urlparse(a['href']).query) for a in info.select('a[href]')]
    if not any(q.get('ga') == [session['id']] and q.get('ba', [''])[0].replace(' ', '') == number.replace(' ', '') for q in related_ids):
        raise ValueError(f'IA related-info identity mismatch: {number}')
    floor_chambers = {'House': [], 'Senate': []}
    for row in info.select('tr'):
        label = row.find('span')
        chamber = label.get_text(strip=True).rstrip(':') if label else ''
        if chamber in floor_chambers:
            floor_chambers[chamber].extend(a.get_text(' ', strip=True) for a in row.select('a'))
    histories, documents = [], []
    no_data = 'Data not available for the selected bill.' in table.get_text(' ', strip=True)
    for row in table.select('tr'):
        cells = row.find_all('td')
        if not cells or no_data:
            continue
        if len(cells) not in (2, 3):
            raise ValueError(f'IA malformed history row: {number}')
        when = cells[0].get_text(strip=True)
        if not when:
            if any(c.get_text(strip=True) for c in cells[1:]):
                links = cells[-1].select('a[href]')
                if not links or any(not urljoin(BASE_URL, a['href']).startswith(BASE_URL + '/docs/publications/') for a in links):
                    raise ValueError(f'IA undated action: {number}')
                documents.extend(urljoin(BASE_URL, a['href']) for a in links)
            continue
        when = datetime.strptime(when, '%B %d, %Y').date().isoformat()
        action = cells[-1].get_text(' ', strip=True)
        if not action:
            raise ValueError(f'IA empty action: {number}')
        histories.append([number, session['id'], when, action, len(histories) + 1, term])
    if not histories and not no_data:
        raise ValueError(f'IA unexplained empty history: {number}')
    details = [number, session['id'], session['dates'], primary, cosponsors, floor_names,
               '; '.join(floor_chambers['House']), '; '.join(floor_chambers['Senate']), summary,
               related, companions, bill_url(session, number), lobby_url, term, names, '; '.join(documents)]
    return details, histories


def _fetch(http, url):
    for attempt in range(3):
        response = http.get(url, timeout=60)
        response.raise_for_status()
        time.sleep(1)
        soup = BeautifulSoup(response.text, 'lxml')
        incomplete = soup.select_one('.notificationBar.error') is not None
        if '/billTracking/billHistory?' in url:
            incomplete = incomplete or soup.select_one('div.divideVert') is None
        if not incomplete:
            return response.text
        if attempt < 2:
            delay = 15 * (attempt + 1)
            print(f'IA incomplete response; waiting {delay}s before retrying {url}', flush=True)
            time.sleep(delay)
    raise RuntimeError(f'IA source repeatedly returned incomplete bill content: {url}')


@scrape_run
def scrape(state, term, verbose=False, force_fetch=False):
    if state.upper() != 'IA':
        raise ValueError('Iowa scraper requires state IA')
    parse_term(term)
    folder = REPO_ROOT / '.data/IA/bill'
    outputs = [folder / f'IA_{name}_{term}.csv' for name in ('Bill_Details', 'Bill_Histories')]
    manifest = folder / f'.IA_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs, manifest]) and not force_fetch:
        print(f'Skipping IA {term}: completed outputs exist (use --force-fetch to refresh)')
        return
    cache = folder / '.cache' / term
    cache.mkdir(parents=True, exist_ok=True)
    details, histories, empty_histories = [], [], []
    with make_session() as http:
        session = select_session(_fetch(http, BASE_URL + '/legislation/BillBook'), term)
        html = _fetch(http, BASE_URL + '/legislation/BillBook?ga=' + session['id'])
        records = parse_listing(html, session)
        cache_io.write_text(cache / 'index.html', html, encoding='utf-8')
        print(f'IA assembly {session["id"]}: {len(records)} bills/resolutions/study bills', flush=True)
        for i, number in enumerate(records, 1):
            local = cache / (number.replace(' ', '') + '.json')
            if cache_io.exists(local) and not force_fetch:
                payload = json.loads(cache_io.read_text(local, encoding='utf-8'))
            else:
                related_url = BASE_URL + '/legislation/BillBook?' + urlencode({
                    'ga': session['id'], 'billName': number, 'billVersion': 'i',
                    'action': 'getBillRelatedInfo', 'bl': 'false'})
                payload = {'html': _fetch(http, bill_url(session, number)), 'related_html': _fetch(http, related_url)}
            parsed = parse_bill(payload['html'], payload['related_html'], session, number, term)
            pending = local.with_suffix('.json.tmp'); cache_io.write_text(pending, json.dumps(payload), encoding='utf-8'); cache_io.replace(pending, local)
            details.append(parsed[0]); histories.extend(parsed[1])
            if not parsed[1]:
                empty_histories.append(number)
            if verbose or i % 25 == 0 or i == len(records):
                print(f'IA {i}/{len(records)} {number}: {len(parsed[1])} actions', flush=True)
    staged = []
    try:
        for target, header, rows in zip(outputs, (DETAILS_HEADER, HISTORY_HEADER), (details, histories)):
            pending = target.with_suffix('.csv.tmp'); staged.append((pending, target))
            with pending.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle); writer.writerow(header); writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending, target in staged:
            cache_io.replace(pending, target)

        write_manifest(manifest, {'term': term, 'session': session,
                                      'details': len(details), 'histories': len(histories),
                                      'source_empty_histories': empty_histories})
    finally:
        for pending, _ in staged:
            cache_io.unlink(pending, missing_ok=True)
    print(f'IA {term}: saved {len(details):,} bills/resolutions/study bills and {len(histories):,} histories')
