"""Florida bills, resolutions, histories and votes for one requested term.

Preserves Connor's bill/action parsing, with explicit session discovery,
validated pagination, cached source pages and complete-run output publication.
"""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io

import csv
import datetime
from pathlib import Path
import re
import time
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

import requests
from scrape.http import ScrapeSession
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = 'https://www.flsenate.gov'
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = ['session', 'bill_num', 'primary_sponsor', 'short_title', 'bill_url', 'bill_type', 'sponsors', 'cosponsors', 'summary', 'term']
HISTORY_HEADER = ['session', 'bill_num', 'chamber', 'action_date', 'action', 'order', 'term']
VOTES_HEADER = ['session', 'bill_num', 'vote_id', 'vote_date', 'vote_body', 'vote_result', 'vote_url', 'term']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}', term):
        raise ValueError('FL term must be YYYY_YYYY, for example 2025_2026')
    start, end = map(int, term.split('_'))
    if start % 2 != 1 or end != start + 1:
        raise ValueError('FL term must start in an odd year and end the next year')
    if end > datetime.date.today().year:
        raise ValueError(f'FL term {term} includes a future year')
    return start, end


def select_sessions(html, term):
    years = parse_term(term)
    soup = BeautifulSoup(html, 'lxml')
    sessions = [o['value'] for o in soup.select('#session-name option[value]')
                if re.fullmatch(r'\d{4}[A-Z]?', o['value']) and int(o['value'][:4]) in years]
    if not all(str(y) in sessions for y in years) or len(sessions) != len(set(sessions)):
        raise ValueError(f'FL missing/duplicate regular sessions for {term}')
    return sorted(sessions)


def parse_listing(html, session):
    soup = BeautifulSoup(html, 'lxml')
    table = soup.select_one('#billListDiv tbody')
    if table is None:
        raise ValueError(f'FL missing bill index: {session}')
    records = []
    for row in table.select('tr'):
        link = row.select_one('th a[href]')
        cells = row.select('td')
        if link is None or len(cells) < 2:
            raise ValueError(f'FL malformed index row: {session}')
        url = urljoin(BASE_URL, link['href'])
        if not urlparse(url).path.startswith(f'/Session/Bill/{session}/'):
            raise ValueError(f'FL wrong-session bill link: {url}')
        records.append([session, link.get_text(' ', strip=True), ' '.join(cells[1].get_text().split()),
                        cells[0].get_text(' ', strip=True), url])
    if not records or len({r[4] for r in records}) != len(records):
        raise ValueError(f'FL empty/duplicate bill index: {session}')
    pages = [1]
    # The current page is a span rather than a link, including the final page.
    pages.extend(int(span.get_text(strip=True)) for span in soup.select('.ListPagination span')
                 if span.get_text(strip=True).isdigit())
    for link in soup.select('.ListPagination a[href]'):
        query = {k.lower(): v for k, v in parse_qs(urlparse(link['href']).query).items()}
        if 'pagenumber' in query:
            pages.append(int(query['pagenumber'][0]))
    return records, max(pages)


def parse_bill(bill_row, html, term):
    this_year = bill_row[0]
    this_bill_num = bill_row[1]
    this_title = bill_row[3]
    this_url = bill_row[4]
    bill_soup = BeautifulSoup(html, "lxml")
    if 'A bill with that number does not exist in the selected session' in bill_soup.get_text().strip():
        raise ValueError(f"FL missing bill data: {this_url}")
    search_title = re.escape(this_title)
    summary = bill_soup.find("span", string = re.compile(search_title + ';'))
    if summary is None:
        summary = bill_soup.find("span", string = re.compile(search_title + r'\s+;'))
        if summary is None:
            summary = bill_soup.find('span', string = re.compile(search_title + '$'))
            if summary is None:
                raise ValueError(f"FL missing bill data: {this_url}")
    other_details = summary.parent.find_previous_sibling()
    summary = summary.parent.text.strip().replace(this_title + '; ', '')
    other_details = re.split(' by | by\r\n | by\n| by\r', other_details.text.strip())
    bill_type = other_details[0].strip()
    if len(other_details) == 1 and 'by' not in other_details[0]:
        print('*** ---> NO SPONSORS ON PAGE: {}'.format(this_url))
        sponsors = ''
        cosponsors = ''
    else:
        sponsors = other_details[1].split('(CO-INTRODUCERS)')
        if len(sponsors) == 2:
            cosponsors = ' '.join(sponsors[1].replace("\r\n", "").split())
            sponsors = ' '.join(sponsors[0].replace("\r\n", "").split())
        else:
            sponsors = ' '.join(sponsors[0].replace("\r\n", "").split())
            cosponsors = ''
    sponsors = re.sub(' ;$', '', re.sub(' ; ', '; ', sponsors))
    cosponsors = re.sub(' ;$', '', re.sub(' ; ', '; ', cosponsors))
    hist_tab = bill_soup.find('div', id = 'tabBodyBillHistory').find("tbody")
    rows = hist_tab.find_all('tr')
    bill_history = []
    order = 1
    for row in rows:
        cells = row.find_all('td')
        date = cells[0].text
        if date[-3] == '/':
            date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')
        else:
            date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
        chamber = cells[1].text
        actions = re.split('\r\n| .HJ [0-9][0-9]+; | .SJ [0-9][0-9]+; ', cells[2].text.strip())
        actions = [i for i in actions if i != '']
        for action in actions:
            bill_history.append([this_year, this_bill_num, chamber, date, action.replace('• ', ''), order])
            order += 1
    vote_tab = bill_soup.find('div', id='tabBodyVoteHistory')
    if vote_tab is None:
        raise ValueError(f"FL vote section missing: {this_url}")
    bill_votes = []
    for table in vote_tab.find_all('table'):
        headers = [h.get_text(' ', strip=True).lower() for h in table.select('thead th')]
        id_name = next((name for name in ('version', 'vote') if name in headers), None)
        if id_name is None or not all(name in headers for name in ('date', 'result')):
            raise ValueError(f"FL unrecognized vote columns: {headers}")
        body_name = next((name for name in ('committee', 'chamber') if name in headers), None)
        if body_name is None:
            raise ValueError(f"FL vote table missing committee/chamber: {headers}")
        for row in table.select('tbody tr'):
            cells = row.find_all('td')
            if len(cells) != len(headers):
                raise ValueError(f"FL malformed vote row: {this_url}")
            fields = dict(zip(headers, cells))
            result = fields['result']
            link = result.find('a', href=True)
            bill_votes.append([this_year, this_bill_num,
                               fields[id_name].get_text(' ', strip=True),
                               fields['date'].get_text(' ', strip=True).split()[0],
                               fields[body_name].get_text(' ', strip=True),
                               result.get_text(' ', strip=True),
                               urljoin(BASE_URL, link['href']) if link else ''])
    if not bill_history:
        raise ValueError(f"FL missing history: {this_url}")
    bill_details = bill_row + [bill_type, sponsors, cosponsors, summary, term]
    for row in bill_history:
        row.append(term)
    for row in bill_votes:
        row[3] = datetime.datetime.strptime(row[3], '%m/%d/%Y' if len(row[3].split('/')[-1]) == 4 else '%m/%d/%y').date().isoformat()
        row.append(term)
    return([bill_details, bill_history, bill_votes])


def _fetch(http, url):
    for attempt in range(4):
        response = http.get(url, timeout=60)
        if response.status_code != 429 or attempt == 3:
            break
        delay = max(Retry().get_retry_after(response) or 0, 300 * (attempt + 1))
        print(f'FL rate limit: waiting {delay}s before retrying {url}', flush=True)
        time.sleep(delay)
    response.raise_for_status()
    time.sleep(4)
    return response.text


@scrape_run
def scrape(state, term, verbose=False, force_fetch=False):
    if state.upper() != 'FL':
        raise ValueError('Florida scraper requires state FL')
    parse_term(term)
    folder = REPO_ROOT / '.data/FL/bill'
    outputs = [folder / f'FL_{name}_{term}.csv' for name in ('Bill_Details', 'Bill_Histories', 'Agg_Votes')]
    manifest = folder / f'.FL_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs, manifest]) and not force_fetch:
        print(f'Skipping FL {term}: completed outputs exist (use --force-fetch to refresh)')
        return
    cache = folder / '.cache' / term
    cache.mkdir(parents=True, exist_ok=True)
    details, histories, votes, counts = [], [], [], {}
    with ScrapeSession() as http:
        # Handle 429 visibly in _fetch; urllib3 would otherwise silently sleep.
        http.mount('https://', HTTPAdapter(max_retries=Retry(total=3, backoff_factor=2,
                                                          status_forcelist=[500, 502, 503, 504],
                                                          respect_retry_after_header=False)))
        sessions = select_sessions(_fetch(http, BASE_URL + '/Session/Bills'), term)
        for session in sessions:
            records, seen = [], set()
            page, last = 1, 1
            while page <= last:
                local = cache / f'index_{session}_{page}.html'
                params = {'chamber': 'both', 'searchOnlyCurrentVersion': 'True', 'isIncludeAmendments': 'False',
                          'isFirstReference': 'True', 'citationType': 'FL Statutes', 'pageNumber': page}
                url = f'{BASE_URL}/Session/Bills/{session}?' + urlencode(params)
                html = cache_io.read_text(local, encoding='utf-8') if cache_io.exists(local) and not force_fetch else _fetch(http, url)
                group, reported_last = parse_listing(html, session)
                if page == 1:
                    last = reported_last
                elif reported_last != last:
                    raise ValueError(f'FL page count changed: {session}')
                for bill in group:
                    if bill[4] in seen:
                        raise ValueError(f'FL duplicate across index pages: {bill[4]}')
                    seen.add(bill[4]); records.append(bill)
                pending = local.with_suffix('.html.tmp'); cache_io.write_text(pending, html, encoding='utf-8'); cache_io.replace(pending, local)
                if verbose or page % 10 == 0 or page == last:
                    print(f'FL {session} index: page {page}/{last}', flush=True)
                page += 1
            counts[session] = len(records)
            print(f'FL {session}: {len(records)} bills/resolutions', flush=True)
            for i, bill in enumerate(records, 1):
                local = cache / f'{session}_{urlparse(bill[4]).path.rsplit("/", 1)[-1]}.html'
                html = cache_io.read_text(local, encoding='utf-8') if cache_io.exists(local) and not force_fetch else _fetch(http, bill[4])
                parsed = parse_bill(bill, html, term)
                pending = local.with_suffix('.html.tmp'); cache_io.write_text(pending, html, encoding='utf-8'); cache_io.replace(pending, local)
                details.append(parsed[0]); histories.extend(parsed[1]); votes.extend(parsed[2])
                if verbose or i % 25 == 0 or i == len(records):
                    print(f'FL {session} {i}/{len(records)} {bill[1]}: {len(parsed[1])} actions', flush=True)
    staged = []
    try:
        for target, header, rows in zip(outputs, (DETAILS_HEADER, HISTORY_HEADER, VOTES_HEADER), (details, histories, votes)):
            pending = target.with_suffix('.csv.tmp'); staged.append((pending, target))
            with pending.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle); writer.writerow(header); writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending, target in staged: cache_io.replace(pending, target)

        write_manifest(manifest, {'term': term, 'details': len(details), 'histories': len(histories), 'votes': len(votes), 'session_counts': counts})
    finally:
        for pending, _ in staged: cache_io.unlink(pending, missing_ok=True)
    print(f'FL {term}: saved {len(details):,} bills/resolutions, {len(histories):,} actions and {len(votes):,} vote records')
