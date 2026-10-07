"""Michigan bills and resolutions, using counted searches and official histories."""
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

BASE_URL='https://www.legislature.mi.gov'
REPO_ROOT=Path(__file__).resolve().parents[2]
TYPES={'HB':'House Bill','SB':'Senate Bill','HR':'House Resolution','SR':'Senate Resolution',
       'HJR':'House Joint Resolution','SJR':'Senate Joint Resolution','HCR':'House Concurrent Resolution','SCR':'Senate Concurrent Resolution'}
DETAILS_HEADER=['bill_number','session','bill_type','summary','sponsors','keywords','bill_url','term']
HISTORY_HEADER=['bill_number','session','action_date','journal_page','action','order','term','document_urls']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('MI term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('MI requires an odd-starting consecutive two-year term without future years')
    return start,end


def validate_session(html,term):
    parse_term(term);label=term.replace('_','-')
    options=BeautifulSoup(html,'lxml').select('#sessionAutoUpdate option')
    if not any(o.get('value')==label and o.get_text(strip=True)==label for o in options):raise ValueError(f'MI source does not list session {label}')
    return label


def parse_listing(html,term,kind):
    years=parse_term(term);session=term.replace('_','-');soup=BeautifulSoup(html,'lxml');text=soup.get_text(' ',strip=True)
    if f'Legislative Session(s): {session}' not in text or f'Document Type(s): {kind}' not in text:raise ValueError('MI search returned wrong session or document type')
    count=re.search(r'\(([\d,]+) results? found\)',text)
    if not count:raise ValueError('MI missing search count')
    total=int(count[1].replace(',',''));records=[]
    tables=[t for t in soup.select('table') if [h.get_text(strip=True) for h in t.select('thead th')]==['Document','Type','Description']]
    if len(tables)!=1:
        if total==0 and not tables:return []
        raise ValueError('MI missing/ambiguous search table')
    for row in tables[0].select('tbody tr'):
        cells=row.find_all('td',recursive=False)
        if len(cells)!=3:raise ValueError('MI malformed search row')
        link=cells[0].find('a',href=True)
        if link is None:raise ValueError('MI missing bill link')
        query=parse_qs(urlparse(link['href']).query);number=query.get('objectName',[''])[0]
        m=re.fullmatch(r'(\d{4})-([A-Z]+)-([A-Z]+|\d+)',number)
        if not m or int(m[1]) not in years or TYPES.get(m[2])!=kind or cells[1].get_text(strip=True)!=kind:raise ValueError('MI wrong bill identity in search')
        records.append({'number':number,'kind':kind,'url':BASE_URL+'/Bills/Bill?'+urlencode({'ObjectName':number}),
                        'summary':cells[2].get_text(' ',strip=True).split('Last Action:',1)[0].strip()})
    if len(records)!=total or len({r['number'] for r in records})!=total:raise ValueError(f'MI search row count mismatch: {len(records)}/{total}')
    return records


def parse_bill(html,record,term):
    soup=BeautifulSoup(html,'lxml');year,code,number=record['number'].split('-');heading=soup.select_one('#BillHeading')
    expected=f'{TYPES[code]} {int(number) if number.isdigit() else number} of {year}'
    heading_text=heading.get_text(' ',strip=True) if heading else ''
    if not re.fullmatch(re.escape(expected)+r'(?: \(\s*(?:Public|Local) Act \d+ of \d{4}\s*\))?',heading_text):raise ValueError(f'MI wrong bill page: {record["url"]}')
    sponsors=soup.select_one('#SponsorList');summary=soup.select_one('#ObjectSubject');keywords=soup.select_one('#CateogryList')
    if sponsors is None or summary is None:raise ValueError(f'MI missing sponsors/summary sections: {record["url"]}')
    names='; '.join(a.get_text(' ',strip=True) for a in sponsors.select('a[href]') if 'DistrictMaps' not in a['href'])
    detail=[record['number'],term.replace('_','-'),record['kind'],summary.get_text(' ',strip=True),names,
            '; '.join(a.get_text(' ',strip=True) for a in keywords.select('a')) if keywords else '',record['url'],term]
    header=soup.find('h2',string=re.compile(r'^\s*History\s*$'));table=header.find_next('table') if header else None
    if table is None or [h.get_text(strip=True) for h in table.select('thead th')]!=['Date','Journal','Action']:raise ValueError('MI missing history table')
    history=[]
    for row in table.select('tbody tr'):
        cells=row.find_all('td',recursive=False)
        if len(cells)!=3:raise ValueError('MI malformed history row')
        when=datetime.strptime(cells[0].get_text(strip=True),'%m/%d/%Y').date().isoformat();action=cells[2].get_text(' ',strip=True)
        if not action:raise ValueError('MI empty history action')
        documents='; '.join(urljoin(BASE_URL,a['href']) for a in cells[2].select('a[href]'))
        history.append([record['number'],term.replace('_','-'),when,cells[1].get_text(' ',strip=True),action,len(history)+1,term,documents])
    if not history or not detail[3]:raise ValueError('MI empty history or summary')
    return detail,history


def _fetch(http,url):
    # Official /Home/AutomatedDataCollectionPolicy caps requests at one/second.
    time.sleep(1.1)
    response=http.get(url,timeout=120);response.raise_for_status();return response.text


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='MI':raise ValueError('Michigan scraper requires state MI')
    parse_term(term);folder=REPO_ROOT/'.data/MI/bill';cache=folder/'.cache'/term
    outputs=[folder/f'MI_{name}_{term}.csv' for name in ('Bill_Details','Bill_Histories')];manifest=folder/f'.MI_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping MI {term}: completed outputs exist (use --force-fetch to refresh)');return
    cache.mkdir(parents=True,exist_ok=True);details,histories,counts=[],[],{}
    with make_session() as http:
        session=validate_session(_fetch(http,BASE_URL+'/Bills'),term);records=[]
        for code,kind in TYPES.items():
            local=cache/f'{code}_index.html';url=BASE_URL+'/Search/ExecuteSearch?'+urlencode({'sessions':session,'docTypes':kind})
            html=cache_io.read_text(local, encoding='utf-8') if cache_io.exists(local) and not force_fetch else _fetch(http,url)
            group=parse_listing(html,term,kind);cache_io.write_text(local, html,encoding='utf-8');records.extend(group);counts[code]=len(group)
            print(f'MI {session} {kind}: {len(group)} records; source count agrees',flush=True)
        if len({r['number'] for r in records})!=len(records):raise ValueError('MI duplicate instruments across type searches')
        for i,record in enumerate(records,1):
            local=cache/f'{record["number"]}.html'
            html=cache_io.read_text(local, encoding='utf-8') if cache_io.exists(local) and not force_fetch else _fetch(http,record['url'])
            d,h=parse_bill(html,record,term);pending=local.with_suffix('.html.tmp');cache_io.write_text(pending, html,encoding='utf-8');cache_io.replace(pending, local)
            details.append(d);histories.extend(h)
            if verbose or i%25==0 or i==len(records):print(f'MI {i}/{len(records)} {record["number"]}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'details':len(details),'histories':len(histories),'type_counts':counts})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'MI {term}: saved {len(details):,} bills/resolutions and {len(histories):,} histories')
