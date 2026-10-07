"""Pennsylvania's official counted bill indexes and complete bill histories."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io
import csv
from datetime import date
from pathlib import Path
import re
import time
from urllib.parse import urlencode,urljoin,urlparse
from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL='https://www.palegis.us'
REPO_ROOT=Path(__file__).resolve().parents[2]
USER_AGENT='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
DETAILS_HEADER=['bill_number','session','title','primary_sponsor','all_sponsors','status','bill_url','term','source_session']
HISTORY_HEADER=['bill_number','session','chamber','action_date','action','order','term','source_session','printer_number','source_links']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('PA term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start<1969 or start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('PA requires an odd-starting consecutive term from 1969 without future years')
    return start,end


def select_sessions(html,term):
    start,end=parse_term(term);soup=BeautifulSoup(html,'lxml');sessions=[]
    for o in soup.select('#sessionSelect option'):
        if not o.get('value','').startswith(str(start)+'_'):continue
        label=o.get_text(' ',strip=True)
        if not label.startswith(f'{start}-{end} '):raise ValueError('PA session label mismatch')
        sid=o['value'];m=re.fullmatch(str(start)+r'_(\d+)',sid)
        if not m:raise ValueError('PA unknown session identifier')
        sessions.append({'id':sid,'label':label,'year':start,'special':m[1]})
    if sum(s['special']=='0' for s in sessions)!=1:raise ValueError('PA regular term not uniquely listed')
    return sessions


def parse_listing(html,session,chamber):
    soup=BeautifulSoup(html,'lxml');selected=soup.select_one('#sessionSelect option[selected]')
    if selected is None or selected['value']!=session['id']:raise ValueError('PA index session mismatch')
    body='House' if chamber=='H' else 'Senate';count=re.search(r'Total # of '+body+r' Bills:\s*([\d,]+)',soup.get_text(' ',strip=True))
    if not count or '</html>' not in html.lower():raise ValueError('PA missing/truncated counted bill index')
    records=[];seen=set()
    for a in soup.select('a[href]'):
        if not re.search(r'/legislation/bills/\d+',a['href']):continue
        url=urljoin(BASE_URL,a['href']);path=urlparse(url).path
        m=re.fullmatch(r'/legislation/bills/'+str(session['year'])+r'/(?:[^/]+/)?([hs]b)(\d+)',path)
        if not m or m[1][0].upper()!=chamber:raise ValueError('PA indexed bill URL/chamber mismatch')
        number=m[1].upper()+m[2].zfill(4)
        if re.sub(r'\s+','',a.get_text()).upper()!=m[1].upper()+str(int(m[2])):raise ValueError('PA index text/URL mismatch')
        if number in seen:raise ValueError('PA duplicate bill in index')
        seen.add(number);records.append({'number':number,'prefix':m[1].upper(),'digits':int(m[2]),'url':url})
    if len(records)!=int(count[1].replace(',','')) or not records:raise ValueError('PA incomplete/empty bill index')
    return records


def parse_bill(html,record,session,term):
    soup=BeautifulSoup(html,'lxml');expected=('House' if record['prefix']=='HB' else 'Senate')+' Bill '+str(record['digits'])
    if soup.title is None or not soup.title.get_text(' ',strip=True).startswith(expected+' Information; '+session['label']):raise ValueError('PA bill/session identity mismatch')
    title=soup.select_one('#shortTitle-wrapper')
    if title is None or not title.get_text(strip=True):raise ValueError('PA missing bill title')
    heading=soup.find(string=lambda t:t and t.strip()=='Prime Sponsor')
    group=heading.parent.find_next_sibling() if heading else None
    sponsor=group.find('strong') if group else None
    if sponsor is None:raise ValueError('PA missing prime sponsor')
    names=[sponsor.get_text(' ',strip=True)]
    top=soup.select_one('#top');end=soup.select_one('#HistorySection')
    if top is None or end is None:raise ValueError('PA missing bill/history section')
    # Only sponsor cards before the history section; related bills below it
    # also have member links, which must not become this bill's sponsors.
    for node in group.find_next_siblings():
        if node==end:break
        for strong in node.select('strong'):
            if strong.select_one('a[href*="/members/bio/"]'):names.append(strong.get_text(' ',strip=True))
    names=list(dict.fromkeys(names));status=end.find_next_sibling()
    if status is None or 'Last Action:' not in status.get_text():raise ValueError('PA missing last action')
    detail=[record['number'],session['label'],title.get_text(' ',strip=True),names[0],'; '.join(names),status.get_text(' ',strip=True).removeprefix('Last Action:').strip(),record['url'],term,session['id']]
    accordion=soup.select_one('#billActions');table=accordion.find('table') if accordion else None
    if table is None:raise ValueError('PA missing full history table')
    chamber='House' if record['prefix']=='HB' else 'Senate';histories=[]
    months={name:i+1 for i,name in enumerate(('jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'))}
    for tr in table.select('tr'):
        cells=tr.find_all('td',recursive=False)
        if len(cells)!=2:raise ValueError('PA malformed history row')
        text=' '.join(cells[1].get_text(' ',strip=True).split())
        if text in ('In the House','In the Senate'):
            chamber=text.removeprefix('In the ');continue
        if text.startswith('Signed in House'):chamber='House'
        elif text.startswith('Signed in Senate'):chamber='Senate'
        elif re.search(r'Governor|Veto|Filed in the Office of the Secret|Pamphlet Laws|^Act No\.',text,re.I):chamber='Executive'
        elif re.search(r'by the Electorate|Vote by Electorate',text,re.I):chamber='Electorate'
        matches=list(re.finditer(r'([A-Z][a-z]+)\.?\s+(\d{1,2}),\s*(\d{4})',text))
        if not matches:raise ValueError('PA undated history row: '+text)
        match=matches[-1];month=months.get(match[1][:3].lower())
        if not month:raise ValueError('PA unknown action month')
        when=date(int(match[3]),month,int(match[2])).isoformat()
        action=(text[:match.start()]+text[match.end():]).strip(' ,')
        links='; '.join(urljoin(BASE_URL,a['href']) for a in cells[1].select('a[href]'))
        histories.append([record['number'],session['label'],chamber,when,action,len(histories)+1,term,session['id'],cells[0].get_text(' ',strip=True),links])
    if not histories:raise ValueError('PA empty bill history')
    return detail,histories


def _cached(http,path,url,force):
    if cache_io.exists(path) and not force:return cache_io.read_text(path)
    r=http.get(url,timeout=60);r.raise_for_status();time.sleep(1)
    tmp=path.with_suffix('.html.tmp');cache_io.write_text(tmp, r.text,encoding='utf-8');cache_io.replace(tmp, path);return r.text


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='PA':raise ValueError('Pennsylvania scraper requires state PA')
    parse_term(term);folder=REPO_ROOT/'.data/PA/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'PA_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.PA_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping PA {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,counts=[],[],{}
    with make_session() as http:
        http.headers['User-Agent']=USER_AGENT
        sessions=select_sessions(_cached(http,cache/'sessions.html',BASE_URL+'/legislation/bills',force_fetch),term)
        for session in sessions:
            sid=session['id'];records=[]
            for chamber in ('H','S'):
                url=BASE_URL+'/legislation/bills/bill-index?'+urlencode({'display':'index','sessYr':session['year'],'sessInd':session['special'],'billBody':chamber,'filter':'bills'})
                group=parse_listing(_cached(http,cache/f'{sid}_{chamber}_index.html',url,force_fetch),session,chamber);records.extend(group);counts[sid+'_'+chamber]=len(group)
            print(f'PA {session["label"]}: {len(records)} HB/SB bills',flush=True)
            for i,record in enumerate(records,1):
                html=_cached(http,cache/f'{sid}_{record["number"]}.html',record['url'],force_fetch);d,h=parse_bill(html,record,session,term);details.append(d);histories.extend(h)
                if verbose or i%25==0 or i==len(records):print(f'PA {sid} {i}/{len(records)} {record["number"]}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'scope':'HB/SB bills, as in Connor source','sessions':sessions,'counts':counts,'details':len(details),'histories':len(histories),})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'PA {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
