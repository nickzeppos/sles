"""Nebraska annual full-list indexes and complete unicameral bill histories."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io

import csv
from datetime import date, datetime
import io
from pathlib import Path
import re
import time
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL='https://nebraskalegislature.gov'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_number','session','primary_sponsor','status','summary','bill_url','source_year','source_id','introduced_url']
HISTORY_HEADER=['bill_number','session','chamber','action_date','action','journal_page','order','source_year','source_id','journal_url','vote_url']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term): raise ValueError('NE term must be YYYY_YYYY, for example 2025_2026')
    start,end=map(int,term.split('_'))
    if start<2007 or start%2!=1 or end!=start+1 or end>date.today().year:
        raise ValueError('NE requires an odd-starting consecutive two-year term from 2007, without future years')
    return start,end,(start-2007)//2+100


def validate_session(html,term):
    start,end,assembly=parse_term(term)
    soup=BeautifulSoup(html,'lxml')
    choices={o.get('value'):o.get_text(' ',strip=True) for o in soup.select('select[name=Legislature] option')}
    if str(assembly) not in choices: raise ValueError('NE requested legislature not listed in source')
    specials=[key for key in choices if re.fullmatch(str(assembly)+r'S\d+',key or '')]
    if specials: raise ValueError(f'NE special sessions {specials} require separate session identity support')
    return assembly


def normalize_number(number):
    match=re.fullmatch(r'(LB|LR)(\d+)(A|CA)?',number)
    if not match: raise ValueError(f'NE unexpected document number: {number}')
    return match[1]+match[2].zfill(4)+(match[3] or '')


def parse_listing(html,export,year):
    soup=BeautifulSoup(html,'lxml')
    if not any(h.get_text(' ',strip=True)==f'Search for {year} Introduced Legislation (Full List)' for h in soup.select('h2')):
        raise ValueError('NE wrong annual index')
    tables=soup.select('table')
    if len(tables)!=1: raise ValueError('NE ambiguous bill table')
    rows=[]
    for tr in tables[0].select('tbody tr'):
        cells=tr.find_all('td',recursive=False)
        if len(cells)!=4: raise ValueError('NE malformed index row')
        values=[c.get_text(' ',strip=True) for c in cells]
        number=values[0];normalize_number(number)
        a=cells[0].find('a',href=True)
        if a is None: raise ValueError('NE missing document URL')
        url=urljoin(BASE_URL,a['href']);parsed=urlparse(url)
        identifier=parse_qs(parsed.query).get('DocumentID',[''])[0]
        if parsed.netloc!='nebraskalegislature.gov' or parsed.path!='/bills/view_bill.php' or not identifier.isdigit():
            raise ValueError('NE invalid document URL')
        rows.append({'number':number,'sponsor':values[1],'status':values[2],'summary':values[3],
                     'url':url,'id':identifier,'year':year})
    csv_rows=list(csv.DictReader(io.StringIO(export.lstrip('\ufeff\r\n'))))
    indexed=[(r['number'],r['id']) for r in rows]
    exported=[(r['Document'].strip(),r['Document ID'].strip()) for r in csv_rows]
    if not rows or len(set(indexed))!=len(rows) or indexed!=exported:
        raise ValueError('NE HTML and CSV full indexes disagree or contain duplicates')
    return rows


def parse_bill(html,record,term):
    soup=BeautifulSoup(html,'lxml');assembly=parse_term(term)[2]
    heading=soup.select_one('.main-content h2')
    if heading is None or not heading.get_text(' ',strip=True).startswith(record['number']+' - '):
        raise ValueError(f'NE wrong bill page: {record["url"]}')
    intro={urljoin(record['url'],a['href']) for a in soup.select('a[href]') if a.get_text(' ',strip=True)=='Introduced'}
    expected=f'{BASE_URL}/FloorDocs/{assembly}/PDF/Intro/{record["number"]}.pdf'
    if intro!={expected}: raise ValueError(f'NE introduced text does not match bill/legislature: {intro}')
    dates=re.findall(r'Date of Introduction:\s*([A-Za-z]+ \d{1,2}, \d{4})',soup.get_text(' ',strip=True))
    if not dates or any(datetime.strptime(d,'%B %d, %Y').year!=record['year'] for d in dates):
        raise ValueError('NE introduction year mismatch')
    number=normalize_number(record['number'])
    detail=[number,term,record['sponsor'],record['status'],record['summary'],record['url'],record['year'],record['id'],expected]
    tables=soup.select('table.history')
    if len(tables)!=1: raise ValueError('NE missing/ambiguous history table')
    history=[]
    for tr in tables[0].select('tbody tr'):
        cells=tr.find_all('td',recursive=False)
        if len(cells) not in (3,4): raise ValueError('NE unexpected history row')
        when=datetime.strptime(cells[0].get_text(' ',strip=True),'%b %d, %Y').date().isoformat()
        action=cells[1].get_text(' ',strip=True)
        if not action: raise ValueError('NE empty action')
        journal='; '.join(urljoin(record['url'],a['href']) for a in cells[2].select('a[href]'))
        vote='; '.join(urljoin(record['url'],a['href']) for a in cells[3].select('a[href]')) if len(cells)==4 else ''
        history.append([number,term,'Unicameral',when,action,cells[2].get_text(' ',strip=True),0,record['year'],record['id'],journal,vote])
    if not history: raise ValueError('NE empty history')
    for i,row in enumerate(history):row[6]=len(history)-i
    return detail,history


def _cached(http,path,url,force):
    if cache_io.exists(path) and not force:return cache_io.read_text(path, encoding='utf-8')
    response=http.get(url,timeout=60);response.raise_for_status();time.sleep(1)
    html=response.text
    pending=path.with_suffix(path.suffix+'.tmp');cache_io.write_text(pending, html,encoding='utf-8');cache_io.replace(pending, path)
    return html


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='NE':raise ValueError('Nebraska scraper requires state NE')
    start,end,assembly=parse_term(term)
    folder=REPO_ROOT/'.data/NE/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'NE_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')]
    manifest=folder/f'.NE_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping NE {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,records,counts=[],[],[],{}
    with make_session() as http:
        validate_session(_cached(http,cache/'sessions.html',BASE_URL+'/bills/',force_fetch),term)
        for year in (start,end):
            url=BASE_URL+f'/bills/search_by_date.php?SessionDay={year}'
            html=_cached(http,cache/f'index_{year}.html',url,force_fetch)
            export=_cached(http,cache/f'index_{year}.csv',url+'&print=csv',force_fetch)
            group=parse_listing(html,export,year);counts[str(year)]=len(group);records.extend(group)
            print(f'NE {year}: {len(group)} bills/resolutions',flush=True)
        if len({r['id'] for r in records})!=len(records):raise ValueError('NE duplicate document across annual indexes')
        for i,record in enumerate(records,1):
            html=_cached(http,cache/f'{record["id"]}.html',record['url'],force_fetch)
            detail,history=parse_bill(html,record,term);details.append(detail);histories.extend(history)
            if verbose or i%25==0 or i==len(records):print(f'NE {i}/{len(records)} {record["number"]}: {len(history)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)

        write_manifest(manifest, {'term':term,'assembly':assembly,'counts':counts,'details':len(details),
                                      'histories':len(histories),})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'NE {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
