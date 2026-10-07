"""Idaho bill details and histories for one explicit two-year term.

Retains the original scraper's committee authors, floor sponsors and statement
of purpose links. SOP PDF contact extraction remains a separate workflow.
"""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io

import csv
from datetime import date, datetime
from pathlib import Path
import re
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = 'https://legislature.idaho.gov'
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = ['bill_number', 'session', 'title', 'status', 'author', 'cosponsors', 'summary',
                  'bill_url', 'requestor_SOP', 'requestor_SOP_full', 'SOP_url', 'H_floor_sponsor', 'S_floor_sponsor', 'bill_version', 'term']
HISTORY_HEADER = ['bill_number', 'session', 'action_date', 'action', 'order', 'term']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}', term):
        raise ValueError('ID term must be YYYY_YYYY, for example 2025_2026')
    start, end = map(int, term.split('_'))
    if start % 2 != 1 or end != start + 1:
        raise ValueError('ID term must start in an odd year and end the next year')
    if end > date.today().year:
        raise ValueError(f'ID term {term} includes a future year')
    return start, end


def select_sessions(html, term):
    years = parse_term(term)
    soup = BeautifulSoup(html, 'lxml')
    selected = []
    for option in soup.select('#ddlsessions option[value]'):
        url = urljoin(BASE_URL, option['value'])
        match = re.fullmatch(r'/sessioninfo/(\d{4}[^/]*)/legislation/minidata/?', urlparse(url).path)
        if match and int(match[1][:4]) in years:
            selected.append({'id': match[1], 'url': url, 'year': int(match[1][:4])})
    if not all(str(y) in {s['id'] for s in selected} for y in years):
        raise ValueError(f'ID did not list regular sessions for each year of {term}')
    if len({s['id'] for s in selected}) != len(selected):
        raise ValueError('ID duplicate sessions')
    return sorted(selected, key=lambda s:s['id'])


def parse_listing(html, session):
    soup = BeautifulSoup(html, 'lxml')
    records = []
    for row in soup.find_all('tr', id=re.compile(r'^bill[A-Z]')):
        cells = row.find_all('td')
        if len(cells) < 5 or not cells[0].find('a', href=True):
            raise ValueError('ID malformed index row')
        link = cells[0].find('a', href=True)
        version = link.get_text(strip=True)
        url = urljoin(BASE_URL, link['href'])
        number = urlparse(url).path.rstrip('/').rsplit('/',1)[-1].upper()
        if not re.fullmatch(re.escape(number)+r'[a-zA-Z]*(?:,[a-zA-Z]+)*', version, re.I):
            raise ValueError(f'ID unexpected displayed bill number: {version}')
        if urlparse(url).path.rstrip('/').lower() != f'/sessioninfo/{session["id"]}/legislation/{number}'.lower():
            raise ValueError(f'ID bill/session mismatch in index: {url}')
        records.append([number, session['id'], cells[1].get_text(' ',strip=True),
                        ' '.join(c.get_text(' ',strip=True) for c in cells[3:5]).strip(), url, version])
    if not records or len({r[0] for r in records}) != len(records):
        raise ValueError(f'ID empty or duplicate bill index: {session["id"]}')
    return records


def parse_bill(html, record, term):
    number, session, title, status, url, version = record
    soup = BeautifulSoup(html, 'lxml')
    tables = soup.select('table.bill-table')
    if len(tables) != 3:
        raise ValueError(f'ID expected author/summary/history tables: {url}')
    heading = tables[0].get_text(' ',strip=True)
    match = re.match(re.escape(number) + r'[a-zA-Z]*(?:,[a-zA-Z]+)*\s+by\s+(.+)', heading)
    if not match:
        raise ValueError(f'ID bill identity/author missing: {url}')
    author = match[1]
    cosponsors = soup.find('a', string=re.compile('Legislative Co-sponsors'))
    sop = soup.find('a', id=re.compile('SOP$')) or soup.find('a', string=re.compile('Statement of Purpose'))
    # Preserve the original fallback URL for the separate SOP PDF workflow.
    sop_url = urljoin(BASE_URL,sop['href']) if sop else url.replace(BASE_URL,BASE_URL+'/wp-content/uploads').rstrip('/')+'SOP.pdf'
    summary = tables[1].get_text(' ',strip=True)
    histories = []
    for row in tables[2].select('tr'):
        cells = row.find_all('td')
        if not cells: continue
        if len(cells) not in (3,4) or (len(cells)==4 and cells[3].get_text(strip=True)):
            raise ValueError(f'ID malformed action row: {url}')
        when = cells[1].get_text(strip=True)
        if not when:
            if not histories: raise ValueError(f'ID undated first action: {url}')
            when = histories[-1][2]
        else:
            when = datetime.strptime(when+'/'+session[:4],'%m/%d/%Y').date().isoformat()
        action = cells[2].get_text(' ',strip=True).replace('\xa0',' ')
        if not action: raise ValueError(f'ID empty action: {url}')
        histories.append([number,session,when,action,len(histories)+1,term])
    if not histories or not title or not summary:
        raise ValueError(f'ID missing title/summary/history: {url}')
    floor = [re.sub(r'\s+',' ',text).strip() for text in soup.find_all(string=re.compile(r'Floor\sSponsor'))]
    floor = [re.sub(r'^Floor Sponsors?\s*-\s*','',name) for name in floor]
    house, senate = '', ''
    if floor:
        if number.startswith('H'):
            house = 'Representative '+floor[0]
            if len(floor)>1: senate='Senator '+floor[1]
        else:
            senate='Senator '+floor[0]
            if len(floor)>1: house='Representative '+floor[1]
    detail = [number,session,title,status,author,urljoin(BASE_URL,cosponsors['href']) if cosponsors else '',
              summary,url,'in_PDF','in_PDF',sop_url,house,senate,version,term]
    return detail,histories


def _fetch(http,url):
    response=http.get(url,timeout=60);response.raise_for_status();time.sleep(1)
    return response.text


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='ID': raise ValueError('Idaho scraper requires state ID')
    start,end=parse_term(term)
    folder=REPO_ROOT/'.data/ID/bill'
    outputs=[folder/f'ID_{name}_{term}.csv' for name in ('Bill_Details','Bill_Histories')]
    manifest=folder/f'.ID_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping ID {term}: completed outputs exist (use --force-fetch to refresh)');return
    cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    details,histories,counts=[],[],{}
    with requests.Session() as http:
        http.mount('https://',HTTPAdapter(max_retries=Retry(total=3,backoff_factor=2,status_forcelist=[429,500,502,503,504])))
        sessions=select_sessions(_fetch(http,f'{BASE_URL}/sessioninfo/{end}/legislation/minidata/'),term)
        for session in sessions:
            records=parse_listing(_fetch(http,session['url']),session)
            counts[session['id']]=len(records)
            print(f'ID {session["id"]}: {len(records)} bills/resolutions',flush=True)
            for i,record in enumerate(records,1):
                local=cache/f'{session["id"]}_{record[0]}.html'
                html=cache_io.read_text(local, encoding='utf-8') if cache_io.exists(local) and not force_fetch else _fetch(http,record[4])
                parsed=parse_bill(html,record,term)
                pending=local.with_suffix('.html.tmp');cache_io.write_text(pending, html,encoding='utf-8');cache_io.replace(pending, local)
                details.append(parsed[0]);histories.extend(parsed[1])
                if verbose or i%25==0 or i==len(records):print(f'ID {session["id"]} {i}/{len(records)} {record[0]}: {len(parsed[1])} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)

        write_manifest(manifest, {'term':term,'details':len(details),'histories':len(histories),'session_counts':counts})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'ID {term}: saved {len(details):,} bills/resolutions and {len(histories):,} histories')
