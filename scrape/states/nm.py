"""New Mexico official session searches and complete action lists."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io
import csv
from datetime import date, datetime
from pathlib import Path
import re
import time
from urllib.parse import parse_qs,urljoin,urlparse
from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL='https://www.nmlegis.gov'
SEARCH=BASE_URL+'/Legislation/Legislation_List'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_number','session_year','session_type','sponsors','title','status','comm_reports','bill_url','term','source_session','emergency_clause']
HISTORY_HEADER=['bill_number','session_year','session_type','action_date','legislative_day','action','order','term','source_session']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('NM term must be YYYY_YYYY, for example 2025_2026')
    start,end=map(int,term.split('_'))
    if start<1997 or start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('NM requires an odd-starting consecutive term from 1997 without future years')
    return start,end


def select_sessions(html,term):
    start,end=parse_term(term);soup=BeautifulSoup(html,'lxml');sessions=[]
    kinds={'Regular':'RS','Special':'S','1st Special':'S1','2nd Special':'S2','3rd Special':'S3','Extraordinary':'ES'}
    for option in soup.select('#MainContent_ddlSessionStart option'):
        label=option.get_text(' ',strip=True);year,kind=label.split(' ',1)
        if not start<=int(year)<=end:continue
        if kind not in kinds:raise ValueError(f'NM unrecognized session type {kind}')
        sessions.append({'id':option['value'],'year':int(year),'kind':kinds[kind],'label':label})
    if {s['year'] for s in sessions if s['kind']=='RS'}!={start,end}:raise ValueError('NM source missing requested regular session')
    return sorted(sessions,key=lambda s:int(s['id']))


def form_data(html,session,page):
    soup=BeautifulSoup(html,'lxml')
    form={i['name']:i.get('value','') for i in soup.select('input[type=hidden][name]')}
    form.update({'__EVENTTARGET':'','__EVENTARGUMENT':'','ctl00$MainContent$ddlSessionStart':session['id'],
                 'ctl00$MainContent$ddlSessionEnd':session['id'],'ctl00$MainContent$chkSearchBills':'on',
                 'ctl00$MainContent$chkSearchMemorials':'on','ctl00$MainContent$chkSearchResolutions':'on',
                 'ctl00$MainContent$ddlResultsPerPage':'2000'})
    if page==1:form['ctl00$MainContent$btnSearch']='Go'
    else:form.update({'__EVENTTARGET':'ctl00$MainContent$gridViewLegislation','__EVENTARGUMENT':f'Page${page}'})
    return form


def number(value):
    m=re.fullmatch(r'\*?\s*([HS](?:B|M|JM|JR|R|CR))\s+(\d+)',value)
    if not m:raise ValueError(f'NM invalid bill number {value}')
    return m[1]+m[2].zfill(4)


def parse_listing(html,session,page):
    soup=BeautifulSoup(html,'lxml');counter=soup.select_one('#MainContent_lblRecordCount')
    match=re.fullmatch(r'Displaying ([\d,]+) of ([\d,]+) result\(s\)',counter.get_text(' ',strip=True) if counter else '')
    if not match:raise ValueError('NM missing index count')
    displayed,total=[int(x.replace(',','')) for x in match.groups()];rows=[]
    for tr in soup.select('#MainContent_gridViewLegislation tr'):
        if 'gridview-pagination' in tr.get('class',[]):continue
        cells=tr.find_all('td',recursive=False)
        if not cells:continue
        if len(cells)!=5:raise ValueError('NM malformed index row')
        if cells[4].get_text(' ',strip=True)!=session['label']:raise ValueError('NM index session mismatch')
        a=cells[0].find('a',href=True);label=a.get_text(' ',strip=True);num=number(label)
        url=urljoin(SEARCH,a['href']);q=parse_qs(urlparse(url).query)
        expected=num[0]+q.get('legType',[''])[0]+q.get('legNo',[''])[0].zfill(4)
        # The source year query includes a suffix for special sessions; the
        # visible session label is authoritative and checked again on detail.
        if expected!=num or q.get('chamber',[''])[0]!=num[0] or not q.get('year',[''])[0].startswith(str(session['year'])[-2:]):
            raise ValueError('NM bill URL identity mismatch')
        rows.append({'number':num,'url':url,'title':cells[1].get_text(' ',strip=True),
                     'sponsors':'; '.join(a.get_text(' ',strip=True) for a in cells[2].select('a')),
                     'emergency':label.startswith('*')})
    if len(rows)!=displayed or len(rows)!=min(2000,total-(page-1)*2000):raise ValueError('NM incomplete index page')
    pager=soup.select_one('tr.gridview-pagination')
    if total>2000:
        active=[s.get_text(strip=True) for s in pager.select('span')] if pager else []
        if str(page) not in active:raise ValueError('NM wrong index page')
    return rows,total


def parse_bill(html,record,session,term):
    soup=BeautifulSoup(html,'lxml')
    def text(identifier):
        node=soup.select_one('#'+identifier)
        if node is None:raise ValueError(f'NM missing {identifier}')
        return node.get_text(' ',strip=True)
    if text('MainContent_formViewLegislationTitle_lblSession')!=session['label']+' Session' or number(text('MainContent_formViewLegislationTitle_lblBillID'))!=record['number']:
        raise ValueError('NM bill/session identity mismatch')
    status=text('MainContent_formViewLegislation_linkLocation')
    reports='; '.join(a.get_text(' ',strip=True) for a in soup.select('a[id^="MainContent_tabContainerLegislation_tabPanelReports_dataListReports_linkPDF_"]'))
    detail=[record['number'],session['year'],session['kind'],record['sponsors'],record['title'],status,reports,record['url'],term,session['id'],int(record['emergency'])]
    count=int(text('MainContent_tabContainerLegislation_tabPanelActions_lblActionsCount'))
    nodes=soup.select('span[id^="MainContent_tabContainerLegislation_tabPanelActions_dataListActions_lblAction_"]')
    if len(nodes)!=count:raise ValueError('NM action count mismatch')
    history=[]
    for i,node in enumerate(nodes,1):
        parts=list(node.stripped_strings);dates=[x.removeprefix('Calendar Day: ') for x in parts if x.startswith('Calendar Day:')]
        days=[x.removeprefix('Legislative Day: ') for x in parts if x.startswith('Legislative Day:')]
        action=' '.join(x for x in parts if not x.startswith(('Calendar Day:','Legislative Day:')))
        if len(dates)>1 or len(days)>1 or not action:raise ValueError('NM malformed action')
        when=datetime.strptime(dates[0],'%m/%d/%Y').date().isoformat() if dates else ''
        history.append([record['number'],session['year'],session['kind'],when,'LD: '+days[0] if days else '',action,i,term,session['id']])
    return detail,history


def _fetch(http,url,form=None):
    for attempt in range(3):
        r=http.get(url,timeout=60) if form is None else http.post(url,data=form,timeout=90)
        if r.status_code!=403 or attempt==2:break
        print(f'NM temporary HTTP 403; waiting {30*(attempt+1)}s before retry',flush=True)
        time.sleep(30*(attempt+1))
    r.raise_for_status();time.sleep(2);return r.text


def _save(path,html):
    pending=path.with_suffix('.html.tmp');cache_io.write_text(pending, html,encoding='utf-8');cache_io.replace(pending, path)


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='NM':raise ValueError('New Mexico scraper requires state NM')
    parse_term(term);folder=REPO_ROOT/'.data/NM/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'NM_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.NM_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping NM {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,counts,empty=[],[],{},[]
    with make_session() as http, make_session() as bill_http:
        landing=_fetch(http,SEARCH);sessions=select_sessions(landing,term);_save(cache/'sessions.html',landing)
        for session in sessions:
            records=[];total=None;page=1;previous=landing
            while total is None or len(records)<total:
                local=cache/f'{session["id"]}_index_{page}.html'
                html=cache_io.read_text(local) if cache_io.exists(local) and not force_fetch else _fetch(http,SEARCH,form_data(previous,session,page))
                group,count=parse_listing(html,session,page)
                if total is not None and count!=total:raise ValueError('NM index count changed')
                records.extend(group);total=count;_save(local,html);previous=html;page+=1
                print(f'NM {session["label"]} index: {len(records)}/{total}',flush=True)
            if len({r['number'] for r in records})!=len(records):raise ValueError('NM duplicate indexed bill')
            counts[session['id']]=len(records)
            for i,record in enumerate(records,1):
                local=cache/f'{session["id"]}_{record["number"]}.html'
                html=cache_io.read_text(local) if cache_io.exists(local) and not force_fetch else _fetch(bill_http,record['url'])
                detail,history=parse_bill(html,record,session,term);_save(local,html);details.append(detail);histories.extend(history)
                if not history:empty.append([session['id'],record['number']])
                if verbose or i%25==0 or i==len(records):print(f'NM {session["label"]} {i}/{len(records)} {record["number"]}: {len(history)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'sessions':sessions,'counts':counts,'details':len(details),'histories':len(histories),'source_empty_histories':empty,})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'NM {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
