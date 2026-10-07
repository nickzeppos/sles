"""Mississippi official XML instruments and histories for one two-year term."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io

import csv
from datetime import date, datetime
from pathlib import Path
import re
import time
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL='https://billstatus.ls.state.ms.us'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_number','session_year','session_type','author','coauthors','title','status','revenue_bill','vote_req','house_comm','senate_comm','summary','bill_url','term','source_session','introduced_author_text','introduced_author_url']
HISTORY_HEADER=['bill_number','session_year','session_type','chamber','action_date','action','order','term','vote_url','source_session']
ORDINALS={'First':1,'Second':2,'Third':3,'Fourth':4,'Fifth':5,'Sixth':6,'Seventh':7,'Eighth':8,'Ninth':9}


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('MS term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('MS requires an odd-starting consecutive two-year term without future years')
    if start<2009:raise ValueError('MS native scraper supports XML session archives from 2009 onward')
    return start,end


def session_type(label):
    if label=='Regular Session':return 'RS'
    if label=='Extraordinary Session':return 'ES'
    m=re.fullmatch(r'(\d+)(?:st|nd|rd|th) Extraordinary Session',label)
    if m:return 'ES'+m[1]
    m=re.fullmatch(r'(\w+) Extraordinary Session',label)
    if m and m[1] in ORDINALS:return 'ES'+str(ORDINALS[m[1]])
    raise ValueError(f'MS unrecognized session label: {label}')


def select_sessions(html,term):
    years=parse_term(term);records=[]
    for a in BeautifulSoup(html,'lxml').select('a[href]'):
        if 'mainmenu' not in a['href']:continue
        label=a.get_text(' ',strip=True);m=re.fullmatch(r'(\d{4}) (.+)',label)
        if not m or int(m[1]) not in years:continue
        kind=session_type(m[2]);url=urljoin(BASE_URL,a['href']).replace('http://','https://');path=urlparse(url).path
        key=re.fullmatch(r'/(\d{4}(?:\d+E)?)/pdf/mainmenu\.htm',path)
        if not key or int(key[1][:4])!=int(m[1]):raise ValueError('MS session URL mismatch')
        records.append({'id':key[1],'year':int(m[1]),'type':kind,'index':url.replace('mainmenu.htm','all_measures/allmsrs.xml')})
    if not all(any(s['year']==y and s['type']=='RS' for s in records) for y in years):raise ValueError('MS source did not list both requested regular sessions')
    if len({s['id'] for s in records})!=len(records):raise ValueError('MS duplicate session')
    return sorted(records,key=lambda s:s['id'])


def _number(value):
    m=re.fullmatch(r'([A-Z]+)\s*(\d+)',value.strip())
    if not m:raise ValueError(f'MS malformed measure number: {value}')
    return m[1]+m[2].zfill(4)


def parse_listing(data,session):
    root=ET.fromstring(data)
    label=root.findtext('FORMATSESSION','');year,_,kind=label.partition(' ')
    if root.tag!='LASTACTION' or year!=str(session['year']) or session_type(kind)!=session['type']:raise ValueError('MS wrong-session index')
    if root.findtext('TITLELINE')!='Report of All Measures':raise ValueError('MS incomplete measure index')
    records=[]
    for group in root.findall('MSRGROUP'):
        number=_number(group.findtext('MEASURE',''));link=group.findtext('ACTIONLINK','').strip();url=urljoin(session['index'],link)
        expected=f'/{session["id"]}/pdf/history/{re.sub(r"[0-9]","",number)}/{number}.xml'
        if not link or urlparse(url).hostname!=urlparse(BASE_URL).hostname or urlparse(url).path!=expected:raise ValueError('MS index identity mismatch')
        records.append({'number':number,'url':url})
    if not records or len({r['number'] for r in records})!=len(records):raise ValueError('MS empty or duplicate measure index')
    return records


def parse_bill(data,record,session,term):
    root=ET.fromstring(data)
    if root.tag!='HISTORY' or root.findtext('YEAR')!=str(session['year']) or session_type(root.findtext('SESSION',''))!=session['type']:
        raise ValueError('MS wrong-session history')
    if _number(root.findtext('MEASURE/SHORT_MSRID',''))!=record['number']:raise ValueError('MS wrong bill history')
    def text(path):return (root.findtext(path) or '').strip()
    author='; '.join((e.text or '').strip() for e in root.findall('AUTHORS/PRINCIPAL/P_NAME'))
    coauthors='; '.join((e.text or '').strip() for e in root.findall('AUTHORS/ADDITIONAL/CO_NAME'))
    detail=[record['number'],session['year'],session['type'],author,coauthors,text('LONGTITLE'),text('BACKGROUND/DISPOSITION'),
            text('BACKGROUND/REVENUE'),text('BACKGROUND/VOTETYPE'),'; '.join((e.text or '').strip() for e in root.findall('COMMITTEES/HOUSE/H_NAME')),
            '; '.join((e.text or '').strip() for e in root.findall('COMMITTEES/SENATE/S_NAME')),text('SHORTTITLE'),record['url'],term,session['id']]
    if not detail[11] or root.find('AUTHORS') is None:raise ValueError(f'MS missing summary/authors section: {record["url"]}')
    detail.extend(['',''])
    if not detail[5] and not record['number'].startswith('SN'):raise ValueError(f'MS missing bill title: {record["url"]}')
    histories=[]
    for action in root.findall('ACTION'):
        order=int(action.findtext('ACT_NUMBER',''));raw=(action.findtext('ACT_DESC') or '').strip()
        m=re.fullmatch(r'(\d{2}/\d{2})\s+(?:\(([HS])\)\s+)?(.+)',raw)
        if not m or order!=len(histories)+1:raise ValueError('MS malformed/gapped action history')
        when=datetime.strptime(m[1]+'/'+str(session['year']),'%m/%d/%Y').date().isoformat()
        vote=(action.findtext('ACT_VOTE') or '').strip()
        histories.append([record['number'],session['year'],session['type'],m[2] or '',when,m[3],order,term,urljoin(record['url'],vote) if vote else '',session['id']])
    if not histories:raise ValueError(f'MS empty history: {record["url"]}')
    return detail,histories


def _fetch(http,url):
    r=http.get(url,timeout=60);r.raise_for_status();time.sleep(1);return r.content


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='MS':raise ValueError('Mississippi scraper requires state MS')
    parse_term(term);folder=REPO_ROOT/'.data/MS/bill';cache=folder/'.cache'/term
    outputs=[folder/f'MS_{name}_{term}.csv' for name in ('Bill_Details','Bill_Histories')];manifest=folder/f'.MS_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping MS {term}: completed outputs exist (use --force-fetch to refresh)');return
    cache.mkdir(parents=True,exist_ok=True);details,histories,counts=[],[],{};missing_authors=[]
    with make_session() as http:
        sessions=select_sessions(_fetch(http,BASE_URL+'/sessions.htm'),term)
        for session in sessions:
            local=cache/f'{session["id"]}_index.xml';data=cache_io.read_bytes(local) if cache_io.exists(local) and not force_fetch else _fetch(http,session['index'])
            records=parse_listing(data,session);cache_io.write_bytes(local, data);counts[session['id']]=len(records)
            print(f'MS {session["id"]}: {len(records)} instruments',flush=True)
            for i,record in enumerate(records,1):
                local=cache/f'{session["id"]}_{record["number"]}.xml';data=cache_io.read_bytes(local) if cache_io.exists(local) and not force_fetch else _fetch(http,record['url'])
                d,h=parse_bill(data,record,session,term);pending=local.with_suffix('.xml.tmp');cache_io.write_bytes(pending, data);cache_io.replace(pending, local)
                if not d[3]:
                    root=ET.fromstring(data);link=root.findtext('DOCUMENTS/INTRO/INTRO_OTHER','')
                    if not link:raise ValueError('MS missing author and introduced document')
                    url=urljoin(record['url'],link);intro=cache/f'{session["id"]}_{record["number"]}_introduced.html'
                    raw=cache_io.read_bytes(intro) if cache_io.exists(intro) and not force_fetch else _fetch(http,url)
                    source=BeautifulSoup(raw,'lxml');heading=source.title.get_text(' ',strip=True) if source.title else ''
                    identity=re.match(r'^([A-Z]+\s*\d+) \(As Introduced\) - (\d{4})\b',heading)
                    if not identity or _number(identity[1])!=record['number'] or int(identity[2])!=session['year']:raise ValueError('MS introduced document identity mismatch')
                    lines=[p.get_text(' ',strip=True) for p in source.select('p') if re.match(r'^By:',p.get_text(' ',strip=True))]
                    if len(lines)!=1:raise ValueError('MS introduced author line missing/ambiguous')
                    cache_io.write_bytes(intro, raw);d[-2:]=[lines[0],url]
                    missing_authors.append({'session':session['id'],'bill_number':record['number'],'xml_url':record['url'],'introduced_author_text':lines[0],'introduced_author_url':url})
                details.append(d);histories.extend(h)
                if verbose or i%25==0 or i==len(records):print(f'MS {session["id"]} {i}/{len(records)} {record["number"]}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'details':len(details),'histories':len(histories),'session_counts':counts,'source_missing_primary_authors':missing_authors})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'MS {term}: saved {len(details):,} instruments and {len(histories):,} histories')
