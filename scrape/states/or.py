"""Oregon's public OData API: counted sessions, measures and expanded histories."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest
import csv
from datetime import date
import json
from pathlib import Path
import re
import time
from urllib.parse import urlencode
from scrape.http import make_session

API='https://api.oregonlegislature.gov/odata/odataservice.svc/'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_id','session','LC_num','status','rev_impact','fiscal_impact','keywords','chapter_num','vetoed','primary_sponsors','cosponsors','by_request','description','summary','bill_url','term','sponsor_records','document_urls']
HISTORY_HEADER=['bill_id','session','chamber','action_date','action','order','comm_order','vote','term','source_id']
COMMITTEE_HEADER=['session','code','name','type','chamber','parent_code']
LEGISLATOR_HEADER=['session','code','title','first_name','last_name','chamber','party','district']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('OR term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start<2007 or start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('OR requires an odd-starting consecutive term from 2007 without future years')
    return start,end


def select_sessions(rows,term):
    start,end=parse_term(term);result=[]
    for s in rows:
        match=re.fullmatch(r'(\d{4})([RSI])(\d+)',s['SessionKey'])
        if not match:raise ValueError('OR unknown session key')
        if not start<=int(match[1])<=end or match[2]=='I':continue
        if not s['SessionName'].startswith(match[1]):raise ValueError('OR session key/name mismatch')
        result.append(s)
    if {int(s['SessionKey'][:4]) for s in result if 'R' in s['SessionKey']}!={start,end}:raise ValueError('OR requested regular years not both listed')
    return sorted(result,key=lambda s:s['SessionKey'])


def _clean(value):return ' '.join(str(value or '').split())


def parse_bill(b,session,term):
    sid=session['SessionKey'];prefix=b['MeasurePrefix'];num=b['MeasureNumber']
    if b['SessionKey']!=sid or not re.fullmatch(r'[HS](?:B|R|JR|CR|JM|M)',prefix):raise ValueError('OR bill/session identity mismatch')
    number=prefix+str(num).zfill(4)
    for collection in ('MeasureSponsors','MeasureDocuments','MeasureHistoryActions','CommitteeAgendaItems'):
        if collection not in b or not isinstance(b[collection],list):raise ValueError('OR missing expanded '+collection)
        for item in b[collection]:
            if (item['SessionKey'],item['MeasurePrefix'],item['MeasureNumber'])!=(sid,prefix,num):raise ValueError('OR expanded record identity mismatch')
        if any('next' in k.lower() and k.startswith(collection) and v for k,v in b.items()):raise ValueError('OR expanded collection requires additional pagination')
    sponsors={'Chief':[],'Regular':[]}
    source_sponsors=sorted(b['MeasureSponsors'],key=lambda s:int(s['MeasureSponsorId']))
    for s in source_sponsors:
        if s['SponsorType']=='Member':name=s['LegislatoreCode']
        elif s['SponsorType']=='Committee':name=s['CommitteeCode']+' (Committee)'
        elif s['SponsorType']=='Presession':name='Introduced presession by request'
        else:raise ValueError('OR unknown sponsor type '+s['SponsorType'])
        if not name or s['SponsorLevel'] not in sponsors:raise ValueError('OR missing sponsor name/unknown level')
        sponsors[s['SponsorLevel']].append(name)
    detail=[number,sid,b.get('LCNumber') or '',_clean(b.get('CurrentLocation')),_clean(b.get('RevenueImpact')),_clean(b.get('FiscalImpact')),re.sub(r'^Relating to |\.$','',_clean(b.get('RelatingTo'))),b.get('ChapterNumber') or '',b.get('Vetoed'),'; '.join(sponsors['Chief']),'; '.join(sponsors['Regular']),_clean(b.get('AtTheRequestOf')),_clean(b.get('CatchLine')),_clean(b.get('MeasureSummary')),f'https://olis.oregonlegislature.gov/liz/{sid}/Measures/Overview/{prefix}{num}',term,json.dumps(source_sponsors,ensure_ascii=False),'; '.join(d['DocumentUrl'] for d in b['MeasureDocuments'])]
    histories=[];seen=set()
    for i,item in enumerate(b['CommitteeAgendaItems'],1):
        key='committee:'+str(item['CommitteeAgendaItemId'])
        if key in seen:raise ValueError('OR duplicate agenda action')
        seen.add(key);when=date.fromisoformat(item['MeetingDate'].split('T')[0]).isoformat();code=item['CommitteCode']
        action=f'{code} Committee ~ {item["MeetingType"]}: {item["Action"] or ""}'
        histories.append([number,sid,code[0],when,action,'',i,'',term,key])
    for i,item in enumerate(b['MeasureHistoryActions'],1):
        key='history:'+str(item['MeasureHistoryId'])
        if key in seen:raise ValueError('OR duplicate history action')
        seen.add(key);when=date.fromisoformat(item['ActionDate'].split('T')[0]).isoformat()
        if not item['ActionText']:raise ValueError('OR blank action text')
        histories.append([number,sid,item['Chamber'],when,item['ActionText'],i,'',item.get('VoteText') or '',term,key])
    return detail,histories


def _cached(http,path,route,force):
    if path.exists() and not force:return json.loads(path.read_text())
    r=http.get(API+route,headers={'Accept':'application/json'},timeout=120);r.raise_for_status();time.sleep(1);data=r.json()
    pending=path.with_suffix('.json.tmp');pending.write_text(json.dumps(data));pending.replace(path);return data


def _collection(http,cache,key,route,force,expand=None):
    rows=[];total=None
    while True:
        params={'$inlinecount':'allpages','$top':100,'$skip':len(rows)}
        if expand:params.update({'$expand':expand,'$orderby':'MeasurePrefix,MeasureNumber'})
        data=_cached(http,cache/f'{key}_{len(rows)}.json',route+'?'+urlencode(params),force)
        if 'odata.count' not in data or not isinstance(data.get('value'),list):raise ValueError('OR missing collection count/list')
        reported=int(data['odata.count'])
        if total is not None and total!=reported:raise ValueError('OR collection count changed during pagination')
        total=reported;page=data['value']
        if not page and len(rows)<total:raise ValueError('OR premature end of collection')
        rows.extend(page)
        if len(rows)>total:raise ValueError('OR collection exceeds reported count')
        if len(rows)==total:return rows
        print(f'OR {key}: {len(rows)}/{total}',flush=True)


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='OR':raise ValueError('Oregon scraper requires state OR')
    parse_term(term);folder=REPO_ROOT/'.data/OR/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'OR_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories','Committees','Legislators')];manifest=folder/f'.OR_scrape_{term}.json'
    if all(p.exists() for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping OR {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,committees,legislators,counts=[],[],[],[],{}
    with make_session() as http:
        sessions=select_sessions(_collection(http,cache,'sessions','LegislativeSessions',force_fetch),term)
        for session in sessions:
            sid=session['SessionKey'];route=f"LegislativeSessions('{sid}')/"
            for kind,keys,target in [('Committees',('CommitteeCode','CommitteeName','CommitteeType','HouseOfAction','ParentCommitteeCode'),committees),('Legislators',('LegislatorCode','Title','FirstName','LastName','Chamber','Party','DistrictNumber'),legislators)]:
                lookup=_collection(http,cache,sid+'_'+kind,route+kind,force_fetch);seen=set()
                for row in lookup:
                    if row['SessionKey']!=sid or row[keys[0]] in seen:raise ValueError('OR lookup session mismatch/duplicate')
                    seen.add(row[keys[0]]);target.append([sid,*[row.get(k) or '' for k in keys]])
            bills=_collection(http,cache,sid+'_Measures',route+'Measures',force_fetch,'MeasureSponsors,MeasureDocuments,MeasureHistoryActions,CommitteeAgendaItems')
            counts[sid]=len(bills);seen=set()
            for b in bills:
                key=(b['MeasurePrefix'],b['MeasureNumber'])
                if key in seen:raise ValueError('OR duplicate measure across pages')
                seen.add(key);d,h=parse_bill(b,session,term);details.append(d);histories.extend(h)
            print(f'OR {sid}: parsed {len(bills)} measures',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER,COMMITTEE_HEADER,LEGISLATOR_HEADER),(details,histories,committees,legislators)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        manifest.unlink(missing_ok=True)
        for pending,target in staged:pending.replace(target)
        keys={(h[0],h[1]) for h in histories}
        write_manifest(manifest, {'term':term,'sessions':sessions,'counts':counts,'details':len(details),'histories':len(histories),'source_empty_histories':[{'bill':d[0],'session':d[1]} for d in details if (d[0],d[1]) not in keys],'sponsor_order':'MeasureSponsorId (Connor convention); raw PrintOrder also preserved in sponsor_records',})
    finally:
        for pending,_ in staged:pending.unlink(missing_ok=True)
    print(f'OR {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
