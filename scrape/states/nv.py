"""Nevada NELIS sessions, paginated measures, histories and past hearings."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io
import csv
from datetime import date, datetime
from pathlib import Path
import re
import time
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL='https://www.leg.state.nv.us'
ROOT=BASE_URL+'/App/NELIS/REL'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_number','session','session_year','session_type','intro_date','primary_sponsors','cosponsors','title','fiscal_notes','summary','digest','bill_url','term','source_session','source_id','previous_session_url']
HISTORY_HEADER=['bill_number','session','session_year','session_type','chamber','action_date','action','order','term','source_session','source_id','source_kind','source_urls']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('NV term must be YYYY_YYYY, for example 2025_2026')
    start,end=map(int,term.split('_'))
    if start<2011 or start%2!=1 or end!=start+1 or end>date.today().year:
        raise ValueError('NV requires an odd-starting consecutive term from 2011 without future years')
    return start,end


def select_sessions(html,term):
    start,end=parse_term(term);soup=BeautifulSoup(html,'lxml');sessions={}
    for a in soup.select('div[aria-labelledby=session] a[href]'):
        label=a.get_text(' ',strip=True)
        m=re.fullmatch(r'(\d+(?:st|nd|rd|th)) \((\d{4})\) (Special )?Session',label)
        if not m:continue
        year=int(m[2])
        if not start<=year<=end:continue
        url=urljoin(ROOT,a['href']);sid=urlparse(url).path.rsplit('/',1)[-1]
        if url!=ROOT+'/'+m[1]+m[2]+('Special' if m[3] else ''):raise ValueError('NV session link/label mismatch')
        sessions[sid]={'id':sid,'url':url,'year':year,'ordinal':m[1],'kind':'S' if m[3] else 'R'}
    if sum(s['kind']=='R' for s in sessions.values())!=1:raise ValueError('NV regular session not uniquely listed')
    return sorted(sessions.values(),key=lambda s:(s['year'],s['kind'],int(re.match(r'\d+',s['ordinal'])[0])))


def parse_listing(html,session,page):
    soup=BeautifulSoup(html,'lxml');form=soup.select_one('form#form0')
    if form is None or urljoin(BASE_URL,form.get('action',''))!=session['url']+'/HomeBill/BillsTab':raise ValueError('NV wrong session index')
    match=re.search(r'\b\d+-\d+ of ([\d,]+) results\b',soup.get_text(' ',strip=True))
    if not match:raise ValueError('NV missing index count')
    total=int(match[1].replace(',',''));rows=[]
    active={a.get_text(strip=True) for a in soup.select('.pagination .active a')}
    if active!={str(page)}:raise ValueError('NV wrong index page')
    for a in soup.select('#billList a[href*="/Bill/"]'):
        number=a.get_text(' ',strip=True);url=urljoin(BASE_URL,a['href'])
        if not re.fullmatch(r'(?:AB|SB|AJR|ACR|AR|SJR|SCR|SR|IP)\d+\*?',number):raise ValueError(f'NV unknown document {number}')
        m=re.fullmatch(re.escape(session['url'])+r'/Bill/(\d+)/Overview',url)
        if not m:raise ValueError('NV index bill/session mismatch')
        rows.append({'number':number,'id':m[1],'url':url})
    # NELIS incorrectly prints 1-50 on every page; validate its active page,
    # actual row count and total instead of treating that display as an offset.
    if len(rows)!=min(50,total-(page-1)*50):raise ValueError('NV incomplete bill page')
    return rows,total


def validate_bill(html,record,session):
    soup=BeautifulSoup(html,'lxml');key=soup.select_one('#billKey');strip=soup.select_one('#tabstrip.bill-detail-tabstrip')
    if key is None or key.get('value')!=record['id'] or strip is None or urljoin(BASE_URL,strip.get('data-urlprocess',''))!=session['url']+'/Bill/FillSelectedBillTab':
        raise ValueError('NV wrong bill/session page')
    title=' '.join(soup.title.get_text(' ',strip=True).split()) if soup.title else ''
    suffix=r'(?: of the \d+(?:st|nd|rd|th) \(\d{4}\) Session)?' if record['number'].endswith('*') else ''
    if not re.fullmatch(re.escape(record['number'])+suffix+r' Overview',title):raise ValueError('NV bill number mismatch')


def parse_overview(html,record,session,term):
    soup=BeautifulSoup(html,'lxml');fields={}
    for label in soup.select('div.font-weight-bold'):
        value=label.find_next_sibling('div')
        if value is not None:fields[label.get_text(' ',strip=True)]=value
    def field(name):return fields[name].get_text(' ',strip=True) if name in fields else ''
    def names(pattern):return '; '.join(dict.fromkeys(a.get_text(' ',strip=True) for k,v in fields.items() if re.fullmatch(pattern,k) for a in v.select('a')))
    summary=field('Summary');introduced=field('Introduction Date')
    if not summary or not introduced:raise ValueError('NV missing summary/introduction')
    introduced=datetime.strptime(introduced,'%A, %B %d, %Y').date().isoformat()
    previous=fields.get('Previous Session Overview');previous_url=''
    if previous is not None:
        a=previous.find('a',href=True)
        if a is None:raise ValueError('NV previous-session marker missing link')
        previous_url=urljoin(BASE_URL,a['href'])
    if record['number'].endswith('*')!=bool(previous_url):raise ValueError('NV previous-session identity marker mismatch')
    title=soup.select_one('#title');digest=soup.select_one('#digest')
    detail=[record['number'],session['ordinal'],session['year'],session['kind'],introduced,
            names(r'Primary Sponsors?'),names(r'Co-Sponsors?'),title.get_text(' ',strip=True) if title else '',
            field('Fiscal Notes'),summary,digest.get_text(' ',strip=True) if digest else '',record['url'],term,session['id'],record['id'],previous_url]
    tables=[t for t in soup.select('table') if t.find('caption') and t.find('caption').get_text(' ',strip=True)=='Bill History']
    if len(tables)!=1:raise ValueError('NV missing/ambiguous history')
    actions=[]
    for tr in tables[0].select('tbody tr'):
        cells={c.get('data-th'):c for c in tr.find_all('td',recursive=False)}
        if not {'Date','Action'}<=cells.keys():raise ValueError('NV malformed history')
        when=datetime.strptime(cells['Date'].get_text(' ',strip=True),'%b %d, %Y').date().isoformat()
        action=cells['Action'].get_text(' ',strip=True)
        if not action:raise ValueError('NV empty action')
        links='; '.join(urljoin(BASE_URL,a['href']) for a in tr.select('a[href]'))
        actions.append((when,action,'history',links))
    if not actions:raise ValueError('NV empty history')
    heading=next((h for h in soup.select('h2') if h.get_text(' ',strip=True)=='Past Hearings'),None)
    if heading is None:raise ValueError('NV missing past hearings section')
    section=heading.find_parent('div',class_='row')
    for tr in section.select('table tbody tr'):
        cells={c.get('data-th'):c for c in tr.find_all('td',recursive=False)}
        if not {'Date','Committee','Recommendation'}<=cells.keys():raise ValueError('NV malformed hearing')
        when=datetime.strptime(cells['Date'].get_text(' ',strip=True),'%b %d, %Y').date().isoformat()
        action=cells['Committee'].get_text(' ',strip=True)+' Committee Hearing ~ Outcome: '+cells['Recommendation'].get_text(' ',strip=True)
        links='; '.join(dict.fromkeys(urljoin(BASE_URL,a['href']) for a in tr.select('a[href]')))
        actions.append((when,action,'hearing',links))
    histories=[[record['number'],session['ordinal'],session['year'],session['kind'],'',d,a,i,term,session['id'],record['id'],kind,links]
               for i,(d,a,kind,links) in enumerate(sorted(actions,key=lambda r:r[0]),1)]
    return detail,histories


def _cached(http,path,url,force):
    if cache_io.exists(path) and not force:return cache_io.read_text(path, encoding='utf-8')
    r=http.get(url,timeout=60);r.raise_for_status();time.sleep(1)
    pending=path.with_suffix('.html.tmp');cache_io.write_text(pending, r.text,encoding='utf-8');cache_io.replace(pending, path);return r.text


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='NV':raise ValueError('Nevada scraper requires state NV')
    parse_term(term);folder=REPO_ROOT/'.data/NV/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'NV_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.NV_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping NV {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,counts=[],[],{}
    with make_session() as http:
        sessions=select_sessions(_cached(http,cache/'sessions.html',ROOT,force_fetch),term)
        for session in sessions:
            sid=session['id'];records=[];total=None;page=1;seen=set()
            while total is None or len(records)<total:
                html=_cached(http,cache/f'{sid}_index_{page}.html',session['url']+f'/HomeBill/BillsTab?Page={page}&Filters.PageSize=50',force_fetch)
                group,count=parse_listing(html,session,page)
                if total is not None and count!=total:raise ValueError('NV index total changed')
                for r in group:
                    if r['id'] in seen:raise ValueError('NV duplicate indexed bill')
                    seen.add(r['id'])
                records.extend(group);total=count;page+=1
                print(f'NV {sid} index: {len(records)}/{total}',flush=True)
            counts[sid]=len(records)
            for i,record in enumerate(records,1):
                validate_bill(_cached(http,cache/f'{sid}_{record["id"]}.html',record['url'],force_fetch),record,session)
                html=_cached(http,cache/f'{sid}_{record["id"]}_overview.html',session['url']+'/Bill/FillSelectedBillTab?selectedTab=Overview&billKey='+record['id'],force_fetch)
                detail,history=parse_overview(html,record,session,term);details.append(detail);histories.extend(history)
                if verbose or i%25==0 or i==len(records):print(f'NV {sid} {i}/{len(records)} {record["number"]}: {len(history)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'sessions':sessions,'counts':counts,'details':len(details),'histories':len(histories),})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'NV {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
