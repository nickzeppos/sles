"""Kentucky bills and resolutions from official session records."""
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
from scrape.http import make_session

BASE_URL = 'https://apps.legislature.ky.gov'
SESSION_URL = 'https://legislature.ky.gov/Legislation/Pages/default.aspx'
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = ['bill_number','session_year','session_type','title','sponsors','summary','bill_url','term','enacted_summary','source_status']
HISTORY_HEADER = ['bill_number','session_year','session_type','action_date','action','order','term']
# User verified the current official session directory in their browser on
# 2026-09-30. That directory denies this client's requests; its older mirror
# stops at 2023. Keep the verified fallback narrowly scoped and explicit.
VERIFIED_SESSIONS = {'2025_2026': ['25rs','26rs']}


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term): raise ValueError('KY term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start%2!=1 or end!=start+1 or end>date.today().year: raise ValueError('KY requires an odd-starting consecutive two-year term without future years')
    if start<2009: raise ValueError('KY native parser supports the modern session archive from 2009 onward')
    return start,end


def select_sessions(html,term):
    years=parse_term(term);records={}
    soup=BeautifulSoup(html,'lxml')
    for a in soup.select('a[href]'):
        url=urljoin(SESSION_URL,a['href']);m=re.fullmatch(r'/record/(\d{2}(?:rs|ss\d*))/record\.html?',urlparse(url).path,re.I)
        if m and urlparse(url).hostname=='apps.legislature.ky.gov' and 2000+int(m[1][:2]) in years:
            key=m[1].lower();records[key]={'id':key,'year':2000+int(key[:2]),'type':key[2:].upper(),'url':url}
    if not all(str(y)[2:]+'rs' in records for y in years): raise ValueError('KY session directory does not cover both requested years')
    return sorted(records.values(),key=lambda x:x['id'])


def parse_listing(html,session):
    soup=BeautifulSoup(html,'lxml')
    if soup.title is None or not soup.title.get_text(strip=True).upper().startswith(session['id'].upper()+' '): raise ValueError('KY wrong-session index')
    records={}
    for a in soup.select('a[href]'):
        if not re.fullmatch(r'[HS][A-Z]+\d+\.html',a['href'],re.I): continue
        number=a['href'][:-5].upper();url=urljoin(session['url'],a['href'])
        records[number]={'number':number,'url':url}
    if not records: raise ValueError('KY empty bill index')
    return list(records.values())


def parse_bill(html,record,session,term):
    soup=BeautifulSoup(html,'lxml')
    identity=re.sub(r'\s+','',soup.title.get_text() if soup.title else '').upper()
    if identity!=(session['id']+record['number']).upper(): raise ValueError(f'KY wrong bill/session: {record["url"]}')
    table=soup.find('table');fields={}
    if table is None: raise ValueError('KY missing bill information')
    for row in table.select('tr'):
        label,value=row.find('th'),row.find('td')
        if label and value: fields.setdefault(label.get_text(' ',strip=True),value)
    if 'Sponsor' in fields and 'Sponsors' not in fields:fields['Sponsors']=fields['Sponsor']
    status=fields['Last Action'].get_text(' ',strip=True) if 'Last Action' in fields else ''
    withdrawn=re.fullmatch(r'(\d{2}/\d{2}/\d{2}):\s*WITHDRAWN',status)
    if withdrawn and 'Title' not in fields and 'Summary of Original Version' not in fields and 'Sponsors' in fields:
        # Source publishes only the sponsor and dated withdrawal for these
        # placeholders. Preserve that evidence without inventing a title.
        when=datetime.strptime(withdrawn[1],'%m/%d/%y').date().isoformat()
        return ([record['number'],session['year'],session['type'],'',fields['Sponsors'].get_text(' ',strip=True),'',
                 record['url'],term,'',status],
                [[record['number'],session['year'],session['type'],when,'WITHDRAWN',1,term]])
    if not all(k in fields for k in ('Title','Sponsors','Summary of Original Version')): raise ValueError(f'KY incomplete bill information: {record["url"]}')
    spans=fields['Sponsors'].find_all('span',recursive=False)
    sponsors='; '.join(s.get_text(' ',strip=True).rstrip(', ') for s in spans) if spans else fields['Sponsors'].get_text(' ',strip=True)
    detail=[record['number'],session['year'],session['type'],fields['Title'].get_text(' ',strip=True),sponsors,
            fields['Summary of Original Version'].get_text(' ',strip=True),record['url'],term,
            fields['Summary of Enacted Version'].get_text(' ',strip=True) if 'Summary of Enacted Version' in fields else '',status]
    heading=soup.find('h4',string=re.compile(r'^\s*Actions\s*$'))
    table=heading.find_next('table') if heading else None
    if table is None: raise ValueError('KY missing action table')
    histories=[]
    for row in table.select('tr'):
        label=row.find('th');actions=row.select('td li')
        if label is None or not actions: raise ValueError('KY malformed dated actions')
        when=datetime.strptime(label.get_text(strip=True),'%m/%d/%y').date().isoformat()
        for action in actions:
            text=action.get_text(' ',strip=True)
            if not text: raise ValueError('KY empty action')
            histories.append([record['number'],session['year'],session['type'],when,text,len(histories)+1,term])
    if not histories or not detail[3]: raise ValueError('KY missing title/history')
    return detail,histories


def _fetch(http,url):
    r=http.get(url,timeout=60);r.raise_for_status();time.sleep(1);return r.text


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='KY': raise ValueError('Kentucky scraper requires state KY')
    parse_term(term);folder=REPO_ROOT/'.data/KY/bill';cache=folder/'.cache'/term
    outputs=[folder/f'KY_{name}_{term}.csv' for name in ('Bill_Details','Bill_Histories')];manifest=folder/f'.KY_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping KY {term}: completed outputs exist (use --force-fetch to refresh)');return
    cache.mkdir(parents=True,exist_ok=True);details,histories,counts=[],[],{};catalog_source=SESSION_URL
    with make_session() as http:
        try:
            directory=_fetch(http,SESSION_URL);sessions=select_sessions(directory,term)
        except (requests.RequestException,ValueError):
            if term in VERIFIED_SESSIONS:
                catalog_source='User-verified official session directory, 2026-09-30'
                sessions=[{'id':key,'year':2000+int(key[:2]),'type':key[2:].upper(),'url':f'{BASE_URL}/record/{key}/record.html'} for key in VERIFIED_SESSIONS[term]]
                print('KY using browser-verified session coverage: '+', '.join(VERIFIED_SESSIONS[term]),flush=True)
            else:
                catalog_source=BASE_URL+'/record/pastses.html'
                sessions=select_sessions(_fetch(http,catalog_source),term)
        for session in sessions:
            local=cache/f'{session["id"]}_index.html';url=session['url'].replace('record.html','bills_and_amendments_by_date.html')
            html=cache_io.read_text(local, encoding='utf-8') if cache_io.exists(local) and not force_fetch else _fetch(http,url)
            records=parse_listing(html,session);cache_io.write_text(local, html,encoding='utf-8');counts[session['id']]=len(records)
            print(f'KY {session["id"]}: {len(records)} bills/resolutions',flush=True)
            for i,record in enumerate(records,1):
                local=cache/f'{session["id"]}_{record["number"]}.html'
                html=cache_io.read_text(local, encoding='utf-8') if cache_io.exists(local) and not force_fetch else _fetch(http,record['url'])
                d,h=parse_bill(html,record,session,term);pending=local.with_suffix('.html.tmp');cache_io.write_text(pending, html,encoding='utf-8');cache_io.replace(pending, local)
                details.append(d);histories.extend(h)
                if verbose or i%25==0 or i==len(records):print(f'KY {session["id"]} {i}/{len(records)} {record["number"]}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'details':len(details),'histories':len(histories),'session_counts':counts,'session_catalog_source':catalog_source})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'KY {term}: saved {len(details):,} bills/resolutions and {len(histories):,} histories')
