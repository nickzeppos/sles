"""Delaware legislation, histories and aggregate votes for an explicit term.

Uses the legislature's public search API in ordinary headless Chrome. The
site limits responses to 20 records even when a larger pageSize is requested.
"""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

import csv
from datetime import date, datetime
import json
from pathlib import Path
import re
import time
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

BASE_URL = 'https://legis.delaware.gov'
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = ['bill_number', 'parent_bill', 'legislationId', 'session_num', 'session',
                  'sponsor', 'secondary_sponsors', 'cosponsors', 'intro_date', 'status',
                  'status_id', 'status_date', 'title', 'summary', 'bill_url', 'term']
HISTORY_HEADER = ['bill_number', 'parent_bill', 'legislationId', 'session_num', 'action_date',
                  'actionId', 'action', 'order', 'term']
VOTES_HEADER = ['bill_number', 'parent_bill', 'legislationId', 'session_num', 'chamber',
                'RollCallId', 'vote_date', 'result', 'vote_req', 'num_yeas', 'num_no',
                'num_vacant', 'num_conflict', 'num_notvoting', 'num_absent', 'term']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}', term):
        raise ValueError('DE term must be YYYY_YYYY, for example 2025_2026')
    start, end = map(int, term.split('_'))
    if start % 2 != 1 or end != start + 1:
        raise ValueError('DE term must start in an odd year and end the next year')
    if end > date.today().year:
        raise ValueError(f'DE term {term} includes a future year')
    return start, end


def select_assembly(html, term):
    start, end = parse_term(term)
    soup = BeautifulSoup(html, 'lxml')
    selected = []
    for label in soup.select('.gaCheckboxes label[for]'):
        match = re.fullmatch(r'(\d{4})\s*-\s*(\d{4})\s*\(GA (\d+)\)', label.get_text(strip=True))
        # Delaware's dropdown labels include the preceding election year.
        if match and (int(match[1]), int(match[2])) == (start - 1, end):
            checkbox = soup.find('input', id=label['for'], type='checkbox')
            if checkbox is None or checkbox.get('value') != match[3]:
                raise ValueError('DE assembly label and checkbox disagree')
            selected.append(int(match[3]))
    if len(selected) != 1:
        raise ValueError(f'DE did not identify exactly one assembly for {term}')
    return selected[0]


def _date(value):
    if not value:
        return ''
    match = re.fullmatch(r'/Date\((-?\d+)(?:[+-]\d{4})?\)/', value)
    if match:
        return datetime.fromtimestamp(int(match[1])/1000, ZoneInfo('America/New_York')).date().isoformat()
    return datetime.strptime(value, '%m/%d/%y').date().isoformat()


def checked_data(payload):
    if payload.get('Errors') or not isinstance(payload.get('Data'), list):
        raise ValueError(f'DE malformed/error API response: {payload.get("Errors")}')
    return payload['Data']


def parse_bill(bill, html, actions, votes, assembly, term):
    if bill['AssemblyId'] != assembly:
        raise ValueError(f'DE bill from wrong assembly: {bill["LegislationId"]}')
    if bill['LegislationTypeId'] not in (1, 2, 3, 4, 6):
        raise ValueError(f'Unexpected DE legislation type: {bill["LegislationTypeId"]}')
    number, identity = bill['LegislationNumber'], bill['LegislationId']
    parent = bill.get('SubstituteParentLegislationDisplayCode') or ''
    if not number or not bill.get('LongTitle'):
        raise ValueError(f'DE bill number/title missing: {identity}')
    soup = BeautifulSoup(html, 'lxml')
    sponsors = []
    for name in ('Additional Sponsor(s):', 'Co-Sponsor(s):'):
        label = next((l for l in soup.select('label') if l.get_text(strip=True) == name), None)
        if label is None:
            raise ValueError(f'DE sponsor section missing: {identity} {name}')
        sponsors.append(label.find_next('div').get_text(' ', strip=True))
    detail = [number, parent, identity, assembly, bill['AssemblyName'], bill.get('Sponsor') or '',
              *sponsors, _date(bill.get('IntroductionDateTime')), bill.get('StatusName') or '',
              bill.get('LegislationStatusId') or '', _date(bill.get('LegislationStatusDateTime')),
              bill['LongTitle'], (bill.get('Synopsis') or '').replace('\r\n', ' '),
              f'{BASE_URL}/BillDetail?LegislationId={identity}', term]
    histories = []
    for i, row in enumerate(checked_data(actions), 1):
        if row['LegislationId'] != identity:
            raise ValueError(f'DE history identity mismatch: {identity}')
        histories.append([number, parent, identity, assembly, _date(row['OccuredAtDateTime']),
                          row['LegislationActionLogId'], row['ActionDescription'], i, term])
    if len(histories) != actions['Total']:
        raise ValueError(f'DE incomplete/empty history: {identity}')
    if not histories:
        section = soup.select_one('#RecentReports')
        if section is None or 'No Records Available' not in section.get_text(' ', strip=True):
            raise ValueError(f'DE empty history not confirmed by bill page: {identity}')
    vote_rows = []
    for row in checked_data(votes):
        if row['LegislationId'] != identity or row['AssemblyId'] != assembly:
            raise ValueError(f'DE vote identity mismatch: {identity}')
        vote_rows.append([number, parent, identity, assembly, row['ChamberName'], row['RollCallId'],
                          _date(row['TakenAtDateTime']), row['RollCallResultTypeName'], row['VoteRequirementCode'],
                          row['YesTotal'], row['NoTotal'], row['VacantTotal'], row['ConflictTotal'],
                          row['NotVotingTotal'], row.get('AbsentTotal', ''), term])
    if len(vote_rows) != votes['Total']:
        raise ValueError(f'DE incomplete votes: {identity}')
    if not vote_rows:
        section = soup.select_one('#VotingReports')
        if section is None or 'No Records Available' not in section.get_text(' ', strip=True):
            raise ValueError(f'DE empty votes not confirmed by bill page: {identity}')
    return detail, histories, vote_rows


class Browser:
    def __enter__(self):
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        options = Options(); options.add_argument('--headless')
        self.driver = webdriver.Chrome(options=options)
        self.driver.set_page_load_timeout(60)
        self.driver.set_script_timeout(60)
        return self

    def __exit__(self, *args):
        self.driver.quit()

    def page(self, path):
        from selenium.common.exceptions import TimeoutException
        from selenium.webdriver.support.ui import WebDriverWait
        for attempt in range(3):
            try:
                self.driver.get(BASE_URL + path)
                time.sleep(1)
                if path.startswith('/BillDetail'):
                    # Require loaded grids, including their explicit empty state.
                    for grid in ('RecentReports', 'VotingReports'):
                        WebDriverWait(self.driver, 45).until(lambda d, grid=grid: d.find_elements(
                            'css selector', f'#{grid} tr[data-uid], #{grid} .k-grid-norecords'))
                return self.driver.page_source
            except TimeoutException:
                if attempt == 2:
                    raise RuntimeError(f'DE page/grids did not finish loading: {path}')
                print(f'DE page load interrupted: {path}; waiting 60s before retry', flush=True)
                time.sleep(60)

    def grid(self, name):
        # The page has already fetched these public API records. Read its loaded
        # data instead of issuing a duplicate request for each grid.
        payload = self.driver.execute_script('''
            const grid = window.jQuery(arguments[0]).data('kendoGrid');
            if (!grid) return null;
            const source = grid.dataSource;
            return JSON.parse(JSON.stringify({Data: source.data().toJSON(), Total: source.total()},
                function(key, value) {
                    return this[key] instanceof Date ? '/Date(' + this[key].getTime() + ')/' : value;
                }));
        ''', '#' + name)
        if not isinstance(payload, dict) or len(checked_data(payload)) != payload.get('Total'):
            raise ValueError(f'DE grid data is incomplete: {name}')
        return payload

    def post(self, path, form):
        time.sleep(1)
        for attempt in range(4):
            result = self.driver.execute_async_script('''
            const [path, form, done] = arguments;
            fetch(path, {method:'POST', headers:{'X-Requested-With':'XMLHttpRequest'},
                         body:new URLSearchParams(form)})
                .then(async r=>done({status:r.status, text:await r.text()}))
                .catch(e=>done({error:String(e)}));
            ''', path, form)
            if result.get('status') == 200:
                break
            if result.get('status') not in (None, 429, 500, 502, 503, 504) or attempt == 3:
                break
            delay = 60 * (attempt + 1)
            print(f'DE request interrupted ({result.get("status", result.get("error"))}); waiting {delay}s', flush=True)
            time.sleep(delay)
        if result.get('status') != 200:
            raise RuntimeError(f'DE API request failed: {path}: {result.get("status", result.get("error"))}')
        if not result['text'].strip() and path in (
            '/json/BillDetail/GetRecentReportsByLegislationId',
            '/json/BillDetail/GetVotingReportsByLegislationId',
        ):
            # These endpoints return a blank HTTP 200 for some empty grids.
            # parse_bill additionally requires explicit "No Records Available"
            # in the corresponding section of the official bill page.
            return {'Data': [], 'Total': 0, '_empty_response': True}
        payload = json.loads(result['text'])
        checked_data(payload)
        return payload


def get_bills(browser, assembly, cache=None, force_fetch=False):
    records, seen = [], set()
    total = None
    page = 1
    while total is None or len(records) < total:
        form = {'page': str(page), 'pageSize': '20', 'selectedGA[0]': str(assembly),
                'coSponsorCheck': 'True', 'sort': '', 'group': '', 'filter': ''}
        for i, kind in enumerate((1, 2, 3, 4, 6)):
            form[f'selectedLegislationTypeId[{i}]'] = str(kind)
        local = cache / f'index_{assembly}_{page}.json' if cache else None
        if local is not None and local.exists() and not force_fetch:
            payload = json.loads(local.read_text(encoding='utf-8'))
        else:
            payload = browser.post('/json/AllLegislation/GetAllLegislation', form)
        if total is not None and payload['Total'] != total:
            raise ValueError('DE index count changed during pagination; rerun')
        total = payload['Total']
        rows = checked_data(payload)
        if not rows:
            raise ValueError('DE empty page before reaching index total')
        for bill in rows:
            identity = bill['LegislationId']
            if identity in seen or bill['AssemblyId'] != assembly:
                raise ValueError(f'DE duplicate/wrong-assembly index record: {identity}')
            seen.add(identity); records.append(bill)
        if local is not None:
            pending = local.with_suffix('.json.tmp')
            pending.write_text(json.dumps(payload), encoding='utf-8')
            pending.replace(local)
        print(f'DE index: {len(records)}/{total}', flush=True)
        page += 1
    if len(records) != total:
        raise ValueError('DE index count mismatch')
    return records


@scrape_run
def scrape(state, term, verbose=False, force_fetch=False):
    if state.upper() != 'DE':
        raise ValueError('Delaware scraper requires state DE')
    parse_term(term)
    folder = REPO_ROOT / '.data/DE/bill'
    outputs = [folder / f'DE_{name}_{term}.csv' for name in ('Bill_Details', 'Bill_Histories', 'Agg_Votes')]
    manifest = folder / f'.DE_scrape_{term}.json'
    if all(p.exists() for p in [*outputs, manifest]) and not force_fetch:
        print(f'Skipping DE {term}: completed outputs exist (use --force-fetch to refresh)'); return
    cache = folder / '.cache' / term
    cache.mkdir(parents=True, exist_ok=True)
    details, histories, votes = [], [], []
    empty_histories = []
    with Browser() as browser:
        assembly = select_assembly(browser.page('/AllLegislation'), term)
        bills = get_bills(browser, assembly, cache, force_fetch)
        for i, bill in enumerate(bills, 1):
            identity = bill['LegislationId']
            path = cache / f'{identity}.json'
            if path.exists() and not force_fetch:
                payload = json.loads(path.read_text(encoding='utf-8'))
            else:
                payload = {'html': browser.page(f'/BillDetail?LegislationId={identity}')}
                payload['actions'] = browser.grid('RecentReports')
                payload['votes'] = browser.grid('VotingReports')
            parsed = parse_bill(bill, payload['html'], payload['actions'], payload['votes'], assembly, term)
            if not parsed[1]:
                empty_histories.append(identity)
            pending = path.with_suffix('.json.tmp'); pending.write_text(json.dumps(payload), encoding='utf-8'); pending.replace(path)
            details.append(parsed[0]); histories.extend(parsed[1]); votes.extend(parsed[2])
            if verbose or i % 25 == 0 or i == len(bills):
                print(f'DE {i}/{len(bills)} {bill["LegislationNumber"]}: {len(parsed[1])} actions', flush=True)
    staged = []
    try:
        for target, header, rows in zip(outputs, (DETAILS_HEADER, HISTORY_HEADER, VOTES_HEADER), (details, histories, votes)):
            pending = target.with_suffix('.csv.tmp'); staged.append((pending, target))
            with pending.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle); writer.writerow(header); writer.writerows(rows)
        manifest.unlink(missing_ok=True)
        for pending, target in staged: pending.replace(target)

        write_manifest(manifest, {'term': term, 'assembly': assembly, 'details': len(details), 'histories': len(histories), 'votes': len(votes),
                                      'source_empty_histories': empty_histories})
    finally:
        for pending, _ in staged: pending.unlink(missing_ok=True)
    print(f'DE {term}: saved {len(details):,} bills/resolutions, {len(histories):,} actions and {len(votes):,} vote summaries')
