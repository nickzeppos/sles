"""Georgia's public legislature API for one explicitly selected term.

Chrome initializes the same anonymous API session as the public site. Its
short-lived authorization stays in memory; cached files contain only bill data.
"""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

import csv
from datetime import date
import json
from pathlib import Path
import re
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = 'https://www.legis.ga.gov'
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = ['bill_number', 'session', 'sponsors', 'oppo_chamber_sponsors', 'title',
                  'house_comm', 'senate_comm', 'summary', 'bill_url', 'term', 'session_id', 'legislation_id', 'footnotes']
HISTORY_HEADER = ['bill_number', 'session', 'action_date', 'action', 'order', 'term', 'session_id', 'legislation_id']
VOTES_HEADER = ['bill_number', 'session', 'vote_date', 'vote_id', 'num_yeas', 'num_no',
                'num_notvoting', 'num_excused', 'term', 'session_id', 'legislation_id']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}', term):
        raise ValueError('GA term must be YYYY_YYYY, for example 2025_2026')
    start, end = map(int, term.split('_'))
    if start % 2 != 1 or end != start + 1:
        raise ValueError('GA term must start in an odd year and end the next year')
    if end > date.today().year:
        raise ValueError(f'GA term {term} includes a future year')
    return start, end


def select_sessions(records, term):
    start, end = parse_term(term)
    selected = []
    for row in records:
        name = row['description']
        if re.match(rf'^{start}-{end} Regular Session$', name) or re.match(rf'^({start}|{end}) (?:\d+(?:st|nd|rd|th) )?Special Session$', name):
            selected.append(row)
    if not any(s['description'] == f'{start}-{end} Regular Session' for s in selected):
        raise ValueError(f'GA does not list the requested regular session: {term}')
    if len({s['id'] for s in selected}) != len(selected):
        raise ValueError('GA duplicate session IDs')
    return sorted(selected, key=lambda s: s['id'])


class PublicAPI:
    def __enter__(self):
        self.http = requests.Session()
        self.http.mount('https://', HTTPAdapter(max_retries=Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])))
        self.initialize()
        return self

    def __exit__(self, *args):
        self.http.close()

    def initialize(self):
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        options = Options(); options.add_argument('--headless')
        options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        driver = webdriver.Chrome(options=options)
        try:
            driver.set_page_load_timeout(60)
            # Public current search initializes anonymous API access. Actual
            # scraper selection comes from requested session metadata below.
            driver.get(BASE_URL + '/search')
            WebDriverWait(driver, 45).until(lambda d: d.find_elements('css selector', '#session option'))
            # Session requests already carry the site's anonymous API header.
            found = False
            for item in driver.get_log('performance'):
                message = json.loads(item['message'])['message']
                if message['method'] != 'Network.requestWillBeSent':
                    continue
                request = message['params']['request']
                if '/api/sessions' not in request['url']:
                    continue
                for key, value in request.get('headers', {}).items():
                    if key.lower() == 'authorization':
                        self.http.headers['Authorization'] = value
                        found = True
            if not found:
                raise RuntimeError('GA public browser session did not initialize API authorization')
        finally:
            driver.quit()

    def request(self, path, body=None):
        time.sleep(0.75)
        method = self.http.get if body is None else self.http.post
        kwargs = {} if body is None else {'json': body}
        response = method(BASE_URL + path, timeout=60, **kwargs)
        if response.status_code == 401:
            self.initialize()
            response = method(BASE_URL + path, timeout=60, **kwargs)
        response.raise_for_status()
        return response.json()


def get_bills(api, session, cache=None, force_fetch=False):
    records, seen = [], set()
    total = None
    page = 0
    while total is None or len(records) < total:
        offset = len(records)
        local = cache / f'index_{session["id"]}_{offset}.json' if cache else None
        if local and local.exists() and not force_fetch:
            payload = json.loads(local.read_text(encoding='utf-8'))
        else:
            payload = api.request(f'/api/Legislation/Search/200/{page}', {
                'committeeIds': [], 'documentTypes': [], 'legislationTypes': [], 'chamberTypes': [],
                'legislationNumber': None, 'sessionId': session['id'], 'sponsorIds': [],
                'titleIds': [], 'currentStatus': None,
            })
        if total is not None and payload['resultCount'] != total:
            raise ValueError('GA search total changed during pagination')
        total = payload['resultCount']
        if not payload['results']:
            raise ValueError(f'GA empty index page for {session["description"]}')
        for bill in payload['results']:
            if bill['session']['id'] != session['id'] or bill['legislationId'] in seen:
                raise ValueError('GA duplicate or wrong-session index record')
            seen.add(bill['legislationId']); records.append(bill)
        if local:
            pending = local.with_suffix('.json.tmp'); pending.write_text(json.dumps(payload), encoding='utf-8'); pending.replace(local)
        print(f'GA {session["description"]} index: {len(records)}/{total}', flush=True)
        page += 1
    if len(records) != total:
        raise ValueError('GA search count mismatch')
    return records


def parse_bill(bill, identity, session, term):
    if bill['id'] != identity or bill['session']['id'] != session['id']:
        raise ValueError(f'GA bill/session identity mismatch: {identity}')
    chamber = {1: 'H', 2: 'S'}.get(bill['chamber'])
    kind = {1: 'B', 2: 'R'}.get(bill['documentType'])
    if not chamber or not kind or not str(bill['number']).isdigit():
        raise ValueError(f'GA unknown bill type/number: {identity}')
    number = f"{chamber}{kind} {bill['number']}{bill.get('suffix') or ''}"
    if not bill.get('title'):
        raise ValueError(f'GA empty title: {identity}')
    primary, opposite = [], []
    for sponsor in sorted(bill.get('sponsors') or [], key=lambda s: s['sequence']):
        name = (sponsor['name'].strip() + ' ' + (sponsor.get('district') or '')).strip()
        (primary if sponsor['chamber'] == bill['chamber'] else opposite).append(name)
    if 'sponsors' not in bill:
        raise ValueError(f'GA missing sponsor field: {identity}')
    # A sponsor may withdraw, leaving an explicitly empty source list (HB474).
    # Keep that absence and the explanatory source footnotes.
    committees = bill.get('committees') or []
    details = [number, session['description'], '; '.join(primary), '; '.join(opposite), bill['title'],
               '; '.join(c['name'] for c in committees if c['chamber'] == 1),
               '; '.join(c['name'] for c in committees if c['chamber'] == 2),
               (bill.get('firstReader') or '').strip(), f'{BASE_URL}/legislation/{identity}', term, session['id'], identity,
               (bill.get('footnotes') or '').strip()]
    histories = []
    for order, action in enumerate(reversed(bill.get('statusHistory') or []), 1):
        when = date.fromisoformat(action['date'].split('T')[0]).isoformat()
        if not action['name']:
            raise ValueError(f'GA empty history action: {identity}')
        histories.append([number, session['description'], when, action['name'].strip(), order, term, session['id'], identity])
    if not histories:
        raise ValueError(f'GA empty history: {identity}')
    votes = []
    for vote in bill.get('votes') or []:
        votes.append([number, session['description'], date.fromisoformat(vote['date'].split('T')[0]).isoformat(),
                      vote['id'], vote['yea'], vote['nay'], vote['notVoting'], vote['excused'], term, session['id'], identity])
    return details, histories, votes


@scrape_run
def scrape(state, term, verbose=False, force_fetch=False):
    if state.upper() != 'GA':
        raise ValueError('Georgia scraper requires state GA')
    parse_term(term)
    folder = REPO_ROOT / '.data/GA/bill'
    outputs = [folder / f'GA_{name}_{term}.csv' for name in ('Bill_Details', 'Bill_Histories', 'Agg_Votes')]
    manifest = folder / f'.GA_scrape_{term}.json'
    if all(p.exists() for p in [*outputs, manifest]) and not force_fetch:
        print(f'Skipping GA {term}: completed outputs exist (use --force-fetch to refresh)')
        return
    cache = folder / '.cache' / term
    cache.mkdir(parents=True, exist_ok=True)
    details, histories, votes, counts = [], [], [], {}
    empty_sponsors = []
    with PublicAPI() as api:
        sessions = select_sessions(api.request('/api/sessions'), term)
        for session in sessions:
            bills = get_bills(api, session, cache, force_fetch)
            counts[str(session['id'])] = len(bills)
            for i, item in enumerate(bills, 1):
                identity = item['legislationId']
                local = cache / f'{identity}.json'
                bill = json.loads(local.read_text(encoding='utf-8')) if local.exists() and not force_fetch else api.request(f'/api/legislation/detail/{identity}')
                parsed = parse_bill(bill, identity, session, term)
                if not bill.get('sponsors'):
                    empty_sponsors.append(identity)
                if str(bill['number']) != str(item['number']) or bill['documentType'] != item['documentType'] or bill['chamber'] != item['chamberType']:
                    raise ValueError(f'GA index/detail disagreement: {identity}')
                pending = local.with_suffix('.json.tmp'); pending.write_text(json.dumps(bill), encoding='utf-8'); pending.replace(local)
                details.append(parsed[0]); histories.extend(parsed[1]); votes.extend(parsed[2])
                if verbose or i % 25 == 0 or i == len(bills):
                    print(f'GA {session["description"]} {i}/{len(bills)} {parsed[0][0]}: {len(parsed[1])} actions', flush=True)
    staged = []
    try:
        for target, header, rows in zip(outputs, (DETAILS_HEADER, HISTORY_HEADER, VOTES_HEADER), (details, histories, votes)):
            pending = target.with_suffix('.csv.tmp'); staged.append((pending, target))
            with pending.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle); writer.writerow(header); writer.writerows(rows)
        manifest.unlink(missing_ok=True)
        for pending, target in staged: pending.replace(target)

        write_manifest(manifest, {'term': term, 'details': len(details), 'histories': len(histories), 'votes': len(votes), 'session_counts': counts,
                                      'source_empty_sponsors': empty_sponsors})
    finally:
        for pending, _ in staged: pending.unlink(missing_ok=True)
    print(f'GA {term}: saved {len(details):,} bills/resolutions, {len(histories):,} actions and {len(votes):,} vote records')
