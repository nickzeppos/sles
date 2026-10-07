"""Utah's published session indexes and the public JSON used by bill pages."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io
import csv
from datetime import date, datetime
import json
from pathlib import Path
import re
import time
from urllib.parse import urljoin,urlparse,parse_qs,urlencode
from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL='https://le.utah.gov'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_id','session','primary_sponsor','floor_sponsor','drafting_attorney','chapter_num','last_action','title','description','comm_note','bill_url','term','source_id','highlighted_provisions','versions']
HISTORY_HEADER=['bill_id','session','chamber','location','action_date','action','vote','vote_url','order','term','source_class','source_code','source_timestamp']
BROWSER_HEADERS={'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36','Referer':BASE_URL+'/bills/bills_By_Session.jsp'}


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('UT term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start<2025 or start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('UT current JSON scraper requires an odd-starting consecutive term from 2025 without future years')
    return start,end


def _text(node):return ' '.join(node.get_text(' ',strip=True).split()) if node else ''


def select_sessions(html,years):
    soup=BeautifulSoup(html,'lxml');sessions={}
    for a in soup.select('a[href]'):
        label=_text(a);m=re.match(r'^(\d{4}) (.+Session)$',label)
        if not m or int(m[1]) not in years:continue
        if 'Veto Override' in label:continue
        url=urljoin(BASE_URL,a['href']);q=parse_qs(urlparse(url).query);sid=q.get('session',[''])[0]
        if not re.fullmatch(m[1]+r'(GS|[SHXY][0-9A-Z]+)',sid):raise ValueError('UT unrecognized session URL: '+url)
        sessions[sid]={'id':sid,'label':label,'url':url}
    if not {str(y)+'GS' for y in years}<=set(sessions):raise ValueError('UT requested regular session missing from catalog')
    return [sessions[s] for s in sorted(sessions)]


def parse_listing(html,session):
    soup=BeautifulSoup(html,'lxml')
    if 'Bills and Resolutions for the '+session['label'] not in _text(soup) or '</html>' not in html.lower():raise ValueError('UT bill index session mismatch or truncated response')
    records=[];seen=set();sid=session['id'];directory=sid[:4] if sid.endswith('GS') else sid.lower()
    for a in soup.select('a.billlink[href]'):
        url=urljoin(BASE_URL,a['href']);m=re.fullmatch(r'/~'+directory+r'/bills/static/([HS][A-Z]*\d{3,4})\.html',urlparse(url).path,re.I)
        if not m:raise ValueError('UT bill URL/session mismatch: '+url)
        number=m[1];visible=re.match(r'([HS](?:\.[A-Z])*\.)\s*(\d+)',_text(a))
        if not visible or visible[1].replace('.','')!=re.match(r'[A-Z]+',number)[0] or int(visible[2])!=int(re.search(r'\d+',number)[0]):raise ValueError('UT bill label mismatch')
        if number in seen:raise ValueError('UT duplicate bill in index')
        seen.add(number);records.append({'number':number,'url':url})
    if not records:raise ValueError('UT empty bill index')
    return records


def parse_bill(data,record,session,term):
    sid=session['id'];number=record['number']
    if data['sessionID']!=sid or data['billNumber']!=number or data['year']!=sid[:4]:raise ValueError('UT bill JSON identity mismatch')
    if not data['shortTitle'].strip() or not data['primeSponsorName'].strip():raise ValueError('UT missing title or primary sponsor')
    label=data['billNumberShort'];committees=data['recommendingCommitteeList'];versions=data['billVersionList']
    detail=[label,sid,data['primeSponsorName'],data.get('floorSponsorName',''),data.get('draftingAttorney',''),'',data.get('lastAction',''),data['shortTitle'].strip(),data.get('generalProvisions',''),json.dumps(committees) if committees else '',record['url'],term,number,data.get('highlightedProvisions',''),json.dumps(versions)]
    actions=data['actionHistoryList']
    if not isinstance(actions,list) or not actions:raise ValueError('UT missing action list')
    histories=[]
    for action in actions:
        raw=action['actionDate'];m=re.match(r'^\d{1,2}/\d{1,2}/\d{4}',raw)
        if not action['description']:raise ValueError('UT malformed action')
        when=(datetime.strptime(m[0],'%m/%d/%Y') if m else datetime.fromisoformat(raw)).date().isoformat();kind=action['actionClass'];vote_url=''
        if action['voteID']:
            vote_url=BASE_URL+('/mtgvotes.jsp?'+urlencode({'voteid':action['voteID']}) if kind=='A' else '/DynaBill/svotes.jsp?'+urlencode({'sessionid':sid,'voteid':action['voteID'],'house':action['voteHouse']}))
        histories.append([label,sid,{'H':'House','S':'Senate','G':'Executive','F':'Fiscal'}.get(kind,''),action['owner'],when,action['description'],action['voteStr'],vote_url,len(histories)+1,term,kind,action['actionCode'],raw])
    return detail,histories


def _cached(http,path,url,force):
    if cache_io.exists(path) and not force:return cache_io.read_text(path)
    r=http.get(url,timeout=90);r.raise_for_status();time.sleep(0.75)
    if 'requested URL was rejected' in r.text:raise ValueError('UT server rejected request: '+url)
    r.encoding='utf-8';pending=path.with_suffix(path.suffix+'.tmp');cache_io.write_text(pending, r.text);cache_io.replace(pending, path);return r.text


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='UT':raise ValueError('Utah scraper requires state UT')
    years=parse_term(term);folder=REPO_ROOT/'.data/UT/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'UT_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.UT_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping UT {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,counts=[],[],{}
    with make_session() as http:
        http.headers.update(BROWSER_HEADERS)
        sessions=select_sessions(_cached(http,cache/'sessions.html',BASE_URL+'/bills/bills_By_Session.jsp',force_fetch),years)
        for session in sessions:
            sid=session['id'];records=parse_listing(_cached(http,cache/(sid+'_index.html'),session['url'],force_fetch),session);counts[sid]=len(records)
            print(f'UT {sid}: {len(records)} instruments',flush=True)
            for i,record in enumerate(records,1):
                data=json.loads(_cached(http,cache/(sid+'_'+record['number']+'.json'),BASE_URL+'/data/'+sid+'/'+record['number']+'.json',force_fetch));d,h=parse_bill(data,record,session,term);details.append(d);histories.extend(h)
                if verbose or i%25==0 or i==len(records):print(f'UT {sid} {i}/{len(records)} {record["number"]}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'sessions':sessions,'counts':counts,'details':len(details),'histories':len(histories),'unavailable_source_fields':['chapter_num'],})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'UT {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
