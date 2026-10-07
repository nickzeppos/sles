"""Ohio official legislature search, summaries and dated status tables."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io
import csv
from datetime import date, datetime
import json
from pathlib import Path
import re
import time
from urllib.parse import urlencode, urljoin
from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL='https://www.legislature.ohio.gov'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_number','session_year','session_num','sponsors','cosponsors','status','subjects','title','committees','long_title','bill_url','source_session']
HISTORY_HEADER=['bill_number','session_year','session_num','action_date','chamber','action','committee','order','source_session']
TYPES={'HB':'House Bill','HR':'House Resolution','HCR':'House Concurrent Resolution','HJR':'House Joint Resolution','SB':'Senate Bill','SR':'Senate Resolution','SCR':'Senate Concurrent Resolution','SJR':'Senate Joint Resolution'}


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('OH term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start<2015 or start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('OH requires an odd-starting consecutive term from 2015 without future years')
    return start,end


def select_sessions(html,term):
    start,end=parse_term(term);soup=BeautifulSoup(html,'lxml');result=[]
    for label in soup.select('#general-assembly-radio-selector label.radio-choice-option'):
        text=label.get_text(' ',strip=True)
        regular=re.fullmatch(r'(\d+)(?:st|nd|rd|th) \((\d{4})-(\d{4})\)',text)
        special=re.fullmatch(r'(\d+)(?:st|nd|rd|th) - Special Session \((\d{4})\)',text)
        selected=regular and (int(regular[2]),int(regular[3]))==(start,end) or special and start<=int(special[2])<=end
        if not selected:continue
        node=label.select_one('input[id]')
        if node is None:raise ValueError('OH missing catalog session value')
        sid=node['id'].removeprefix('option-');assembly=(regular or special)[1]
        if not re.fullmatch(assembly+r'(?:-s\d+)?',sid):raise ValueError('OH invalid catalog session value')
        result.append({'id':sid,'assembly':assembly,'label':text,'special':bool(special)})
    if sum(not s['special'] for s in result)!=1:raise ValueError('OH regular term not uniquely listed')
    return result


def parse_listing(html,session,start):
    soup=BeautifulSoup(html,'lxml');view=soup.select_one('#view-state')
    if view is None or str(json.loads(view.get_text()).get('generalAssembly'))!=session['id']:raise ValueError('OH index session mismatch')
    result=soup.select_one('#search-view-results');table=soup.select_one('table.legislation-table')
    if result is None or table is None:raise ValueError('OH missing search results')
    count=re.search(r'Results\s+([\d,]+)\s*-\s*([\d,]+)\s+of\s+([\d,]+)',result.get_text(' ',strip=True))
    if not count:raise ValueError('OH missing result count')
    first,last,total=[int(x.replace(',','')) for x in count.groups()]
    rows=[]
    for tr in table.select('tbody tr'):
        a=tr.select_one('.number-cell a[href]');title=tr.select_one('.short-title-cell');status=tr.select_one('.version-cell')
        if a is None or title is None or status is None:raise ValueError('OH malformed result row')
        url=urljoin(BASE_URL+'/legislation/',a['href']);m=re.fullmatch(re.escape(BASE_URL+'/legislation/'+session['id']+'/')+r'([a-z]+)(\d+)',url)
        if not m or m[1].upper() not in TYPES:raise ValueError('OH bill URL/term mismatch')
        prefix=m[1].upper();digits=int(m[2]);printed=re.sub(r'No\.|[.\s]','',a.get_text()).upper()
        if printed!=prefix+str(digits):raise ValueError('OH index identity mismatch')
        rows.append({'number':prefix+str(digits).zfill(4),'prefix':prefix,'digits':digits,'url':url,'title':title.get_text(' ',strip=True),'status':status.get_text(' ',strip=True)})
    if first!=start or len(rows)!=last-first+1 or not rows or last>total:raise ValueError('OH pagination gap/count mismatch')
    return rows,last,total


def _identity(soup,record,session,status=False):
    expected=TYPES[record['prefix']]+' '+str(record['digits'])+(' Status' if status else '')
    heading=soup.find('h1');title=soup.title
    if heading is None or heading.get_text(' ',strip=True)!=expected or title is None or not re.search(r'\|\s*'+session['assembly']+r'(?:st|nd|rd|th) General Assembly\s*\|',title.get_text()):
        raise ValueError('OH detail bill/assembly identity mismatch: '+record['url'])


def parse_bill(html,actions,record,session,term):
    soup=BeautifulSoup(html,'lxml');_identity(soup,record,session)
    def section(label,selector):
        h=next((h for h in soup.select('h2') if h.get_text(' ',strip=True)==label),None)
        if h is None:return ''
        node=h.find_next('div',class_=selector)
        if node is None:raise ValueError('OH missing '+label+' content')
        return node
    primary=section('Primary Sponsors','media-grid')
    sponsors='; '.join(n.get_text(' ',strip=True) for n in primary.select('.media-overlay-caption-text-line-1')) if primary else ''
    if not sponsors:raise ValueError('OH missing primary sponsors')
    cosponsors='; '.join(n.get_text(' ',strip=True) for group in soup.select('.legislation-cosponsors-inner') for n in group.find_all('div',recursive=False) if 'legislation-cosponsors-header' not in n.get('class',[]) and n.get_text(strip=True))
    tags=[]
    for label in ('Subjects','Committees'):
        node=section(label,'tag-link-group');tags.append('; '.join(a.get_text(' ',strip=True) for a in node.select('a')) if node else '')
    long=soup.select_one('#long-title')
    detail=[record['number'],term,session['assembly'],sponsors,cosponsors,record['status'],tags[0],record['title'],tags[1],long.get_text(' ',strip=True) if long else '',record['url'],session['id']]
    source=BeautifulSoup(actions,'lxml');_identity(source,record,session,True)
    table=source.select_one('table.legislation-status-table')
    if table is None:raise ValueError('OH missing status table')
    history=[]
    for row in table.select('tbody tr'):
        cells=[row.select_one('.'+name+'-cell') for name in ('date','chamber','action','committee')]
        if any(c is None for c in cells):raise ValueError('OH malformed action row')
        raw,chamber,action,committee=[c.get_text(' ',strip=True) for c in cells]
        when=datetime.strptime(raw,'%m-%d-%Y').date().isoformat()
        if not action:raise ValueError('OH empty action')
        history.append([record['number'],term,session['assembly'],when,chamber,action,committee,0,session['id']])
    if not history and [h.get_text(' ',strip=True) for h in table.select('thead th')]!=['Date','Chamber','Action','Committee']:
        raise ValueError('OH malformed empty history table')
    for i,row in enumerate(history):row[7]=len(history)-i
    return detail,history


def _cached(http,path,url,force):
    if cache_io.exists(path) and not force:return cache_io.read_text(path, encoding='utf-8')
    r=http.get(url,timeout=90);r.raise_for_status();time.sleep(1)
    tmp=path.with_suffix('.html.tmp');cache_io.write_text(tmp, r.text,encoding='utf-8');cache_io.replace(tmp, path);return r.text


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='OH':raise ValueError('Ohio scraper requires state OH')
    parse_term(term);folder=REPO_ROOT/'.data/OH/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'OH_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.OH_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping OH {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,counts=[],[],{};empty_histories=[]
    with make_session() as http:
        sessions=select_sessions(_cached(http,cache/'sessions.html',BASE_URL+'/legislation/search',force_fetch),term)
        for session in sessions:
            sid=session['id'];records=[];seen=set()
            for chamber in ('House','Senate'):
                start=1;total=None
                while True:
                    params={'generalAssembly':sid,'start':start,'pageSize':100,'sort':'Number','extendedLegislationTypes':','.join(v for v in TYPES.values() if v.startswith(chamber))}
                    url=BASE_URL+'/legislation/search?'+urlencode(params)
                    rows,last,reported=parse_listing(_cached(http,cache/f'{sid}_{chamber}_{start}.html',url,force_fetch),session,start)
                    if total is not None and total!=reported:raise ValueError('OH changing search count')
                    total=reported
                    for row in rows:
                        if row['number'] in seen:raise ValueError('OH duplicate indexed bill')
                        if not TYPES[row['prefix']].startswith(chamber):raise ValueError('OH chamber filter mismatch')
                        seen.add(row['number']);records.append(row)
                    print(f'OH {sid} {chamber}: indexed {last}/{total}',flush=True)
                    if last==total:break
                    start=last+1
                counts[sid+'_'+chamber]=total
            for i,record in enumerate(records,1):
                html=_cached(http,cache/f'{sid}_{record["number"]}.html',record['url'],force_fetch)
                actions=_cached(http,cache/f'{sid}_{record["number"]}_status.html',record['url']+'/status',force_fetch)
                d,h=parse_bill(html,actions,record,session,term);details.append(d);histories.extend(h)
                if not h:empty_histories.append({'session':sid,'bill_number':record['number'],'url':record['url']+'/status'})
                if verbose or i%25==0 or i==len(records):print(f'OH {sid} {i}/{len(records)} {record["number"]}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'sessions':sessions,'counts':counts,'details':len(details),'histories':len(histories),'source_empty_histories':empty_histories,})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'OH {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
