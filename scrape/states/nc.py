"""North Carolina's official session catalog, complete report and bill pages."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io
import csv
from datetime import date, datetime
from pathlib import Path
import re
import time
from urllib.parse import urljoin,urlparse
from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL='https://www.ncleg.gov'
CATALOG='https://webservices.ncleg.net/sessionselectlist/false'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_num','term','session','bill_type','companion','primary_sponsors','cosponsors','short_title','attributes','counties','statutes','keywords','bill_url','source_session']
HISTORY_HEADER=['bill_num','term','session','action_date','chamber','action','order','votes','source_session','document_urls']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('NC term must be YYYY_YYYY, for example 2025_2026')
    start,end=map(int,term.split('_'))
    if start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('NC requires an odd-starting consecutive term without future years')
    return start,end


def select_sessions(html,term):
    start,end=parse_term(term);soup=BeautifulSoup(html,'lxml');sessions=[]
    for option in soup.select('option[data-session-year]'):
        if option['data-session-year']!=str(start):continue
        label=option.get_text(' ',strip=True);sid=option['value']
        if not re.fullmatch(str(start)+r'(?:E\d+)?',sid):raise ValueError('NC unsupported session identifier')
        if sid==str(start):
            if label!=f'{start}-{end} Session':raise ValueError('NC source term label mismatch')
            kind='RS'
        else:kind='SS'+sid.split('E')[1]
        sessions.append({'id':sid,'label':label,'session':str(start)+'-'+kind})
    if sum(s['id']==str(start) for s in sessions)!=1:raise ValueError('NC regular session not uniquely listed')
    return sessions


def parse_listing(html,session):
    soup=BeautifulSoup(html,'lxml');table=soup.select_one('#bill-report')
    if table is None or '</html>' not in html.lower():raise ValueError('NC missing/truncated report')
    if re.search(r'["\x27]?serverSide["\x27]?\s*:\s*true',html,re.I):raise ValueError('NC report now uses server pagination')
    rows=[];seen=set()
    for tr in table.select('tbody tr'):
        cells=tr.find_all('td',recursive=False)
        if len(cells)!=9:raise ValueError('NC malformed index row')
        text=cells[0].get_text(' ',strip=True);m=re.fullmatch(r'([HS](?:B|R|JR|CR))\s*(\d+)(?:\s*\(\s*=\s*([^)]*)\))?',text)
        if not m:raise ValueError(f'NC unknown document identifier {text}')
        a=cells[3].find('a',href=True)
        if a is None:raise ValueError('NC missing bill link')
        url=urljoin(BASE_URL,a['href']);expected=BASE_URL+f'/BillLookUp/{session["id"]}/{m[1][0]}{int(m[2])}'
        if url!=expected:raise ValueError('NC index bill/session URL mismatch')
        if url in seen:raise ValueError('NC duplicate indexed bill')
        seen.add(url);rows.append({'number':m[1]+m[2].zfill(4),'route':m[1][0]+str(int(m[2])),
                                  'prefix':m[1],'digits':int(m[2]),'companion':m[3] or '',
                                  'title':a.get_text(' ',strip=True),'url':url})
    if not rows:raise ValueError('NC empty bill index')
    return rows


def fields(container):
    found={}
    for label in container.find_all('div'):
        if label.string and label.get_text(strip=True).endswith(':'):
            value=label.find_next_sibling('div')
            if value is not None:found[label.get_text(strip=True).rstrip(':')]=value
    return found


def parse_bill(html,record,session,term):
    soup=BeautifulSoup(html,'lxml');heading=soup.select_one('div.h2')
    types={'B':'Bill','R':'Resolution','JR':'Joint Resolution','CR':'Concurrent Resolution'}
    full=('House' if record['prefix'][0]=='H' else 'Senate')+' '+types[record['prefix'][1:]]+' '+str(record['digits'])
    if heading is None or not re.fullmatch(re.escape(full)+r'(?:\s*/\s*SL\s+\d{4}-\d+)?(?:\s*\(\s*=[HS]\d+\s*\))?',heading.get_text(' ',strip=True)) or soup.title is None or f'({session["label"]})' not in soup.title.get_text(' ',strip=True):
        raise ValueError('NC bill/session identity mismatch')
    info=fields(soup)
    for key in ('Sponsors','Attributes','Counties','Statutes','Keywords'):
        if key not in info:raise ValueError(f'NC missing {key}')
    primary=[];cosponsors=[]
    for group in info['Sponsors'].find_all('div',recursive=False):
        names=[a.get_text(' ',strip=True) for a in group.select('a')]
        (primary if '(Primary)' in group.get_text() else cosponsors).extend(names)
    if not primary and not cosponsors:raise ValueError('NC no named sponsors')
    detail=[record['number'],term,session['session'],types[record['prefix'][1:]],record['companion'],
            '; '.join(primary),'; '.join(cosponsors),record['title'],info['Attributes'].get_text(' ',strip=True),
            info['Counties'].get_text(' ',strip=True),info['Statutes'].get_text(' ',strip=True),
            info['Keywords'].get_text(' ',strip=True).lower(),record['url'],session['id']]
    heading=next((h for h in soup.select('h6') if h.get_text(' ',strip=True)=='History'),None)
    if heading is None:raise ValueError('NC missing history')
    history=[]
    for row in heading.find_next('div',class_='card-body').select('div.row'):
        values=fields(row)
        if not {'Date','Chamber','Action'}<=values.keys():raise ValueError('NC malformed history row')
        raw=values['Date'].get_text(' ',strip=True)
        when=datetime.strptime(raw,'%m/%d/%Y').date().isoformat() if raw else ''
        action=values['Action'].get_text(' ',strip=True)
        if not action:raise ValueError('NC empty action')
        votes=values.get('Votes');vote_text=''
        if votes is not None:
            links=votes.select('a[href]')
            vote_text='; '.join(a.get_text(' ',strip=True)+' ~~ '+urljoin(BASE_URL,a['href']) for a in links)
            if not links and votes.get_text(' ',strip=True) not in ('','None'):raise ValueError('NC vote text without expected link')
        docs=values.get('Documents')
        urls='; '.join(urljoin(BASE_URL,a['href']) for a in docs.select('a[href]')) if docs else ''
        history.append([record['number'],term,session['session'],when,values['Chamber'].get_text(' ',strip=True),action,0,vote_text,session['id'],urls])
    if not history:raise ValueError('NC empty history')
    for i,row in enumerate(history):row[6]=len(history)-i
    return detail,history


def _cached(http,path,url,force):
    if cache_io.exists(path) and not force:return cache_io.read_text(path, encoding='utf-8')
    r=http.get(url,timeout=60);r.raise_for_status();time.sleep(1)
    pending=path.with_suffix('.html.tmp');cache_io.write_text(pending, r.text,encoding='utf-8');cache_io.replace(pending, path);return r.text


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='NC':raise ValueError('North Carolina scraper requires state NC')
    parse_term(term);folder=REPO_ROOT/'.data/NC/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'NC_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.NC_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping NC {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,counts=[],[],{}
    with make_session() as http:
        sessions=select_sessions(_cached(http,cache/'sessions.html',CATALOG,force_fetch),term)
        for session in sessions:
            sid=session['id'];records=parse_listing(_cached(http,cache/f'{sid}_index.html',BASE_URL+f'/Legislation/Bills/LastActionByYear/{sid}/All',force_fetch),session)
            counts[sid]=len(records);print(f'NC {session["label"]}: {len(records)} measures',flush=True)
            for i,record in enumerate(records,1):
                html=_cached(http,cache/f'{sid}_{record["route"]}.html',record['url'],force_fetch)
                detail,history=parse_bill(html,record,session,term);details.append(detail);histories.extend(history)
                if verbose or i%25==0 or i==len(records):print(f'NC {i}/{len(records)} {record["number"]}: {len(history)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'sessions':sessions,'counts':counts,'details':len(details),'histories':len(histories),'source_without_primary_sponsor':[d[0] for d in details if not d[5]],})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'NC {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
