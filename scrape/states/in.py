"""Indiana General Assembly's public session, bill and action APIs."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest
import csv
from datetime import date, datetime
import json
from pathlib import Path
import re
import time
from urllib.parse import urlencode
from scrape.http import make_session

BASE_URL='https://iga.in.gov'
API=BASE_URL+'/api/'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_number','session','title','authors','coauthors','cosponsors','summary','bill_url','sponsors','term','source_id','source_session','status','dead','long_title']
HISTORY_HEADER=['bill_number','session','action_date','chamber','action','order','term','source_id','source_sequence','bill_version','committee_id','vote_id','rollcall_id']
BROWSER_HEADERS={'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36','Accept':'application/json','Content-Type':'application/json','Referer':BASE_URL+'/legislative/2026/bills'}


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('IN term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start<2015 or start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('IN requires an odd-starting consecutive term from 2015 without future years')
    return start,end


def select_sessions(data,term):
    start,end=parse_term(term);result=[]
    for s in data['years']:
        match=re.fullmatch(r'(\d{4})(ss\d*)?',str(s['year']))
        if not match:raise ValueError('IN unknown session year identifier')
        if not start<=int(match[1])<=end:continue
        if s['lpid']!='session_'+s['year'] or bool(match[2])!=s['special']:raise ValueError('IN inconsistent session catalog')
        result.append(s)
    if {s['year'] for s in result if not s['special']}!={str(start),str(end)}:raise ValueError('IN regular years not both listed')
    if len({s['assembly_id'] for s in result})!=1:raise ValueError('IN term crosses assemblies')
    return sorted(result,key=lambda s:s['year'])


def _identity(b,session):
    prefix=b['prefix'];number=str(b['number']);base=prefix+number.zfill(4)
    if prefix not in ('HB','SB','HR','SR','HC','SC','HJ','SJ') or b['base_name']!=base:raise ValueError('IN bill number mismatch')
    if not b['apn'].startswith(session['assembly_id']+'/'+session['year']+'/cabinet/'):raise ValueError('IN bill session/assembly mismatch')
    if not b['name'].startswith(base+'.'):raise ValueError('IN bill version identity mismatch')
    return base


def parse_listing(data,session):
    rows=data.get('bills')
    if not isinstance(rows,list) or not rows:raise ValueError('IN empty/malformed bill index')
    if any(k.lower() in ('next','nextpage','nexttoken') and v for k,v in data.items()):raise ValueError('IN unexpected index pagination')
    seen=set()
    for b in rows:
        key=_identity(b,session)
        if key in seen:raise ValueError('IN duplicate indexed bill')
        seen.add(key)
        if not b['url'].startswith('/legislative/'+session['year']+'/'):raise ValueError('IN index URL session mismatch')
    return sorted(rows,key=lambda b:b['base_name'])


def parse_bill(data,record,session,term):
    b=data['bill'];number=_identity(b,session)
    if number!=record['base_name']:raise ValueError('IN detail/index bill mismatch')
    users=data['users']
    if not isinstance(users,dict):raise ValueError('IN unexpected sponsors structure')
    allowed={'author','co-author','sponsor','co-sponsor','opp_advisor','advisor','chair','conferee','opp_conferee'}
    if set(users)-allowed:raise ValueError('IN unknown sponsor role: '+str(set(users)-allowed))
    def names(role):
        values=users.get(role,[])
        if not isinstance(values,list):raise ValueError('IN malformed sponsor role list')
        return '; '.join(' '.join(str(v.get(k) or '').strip() for k in ('honorific','first_name','last_name')).strip() for v in sorted(values,key=lambda v:int(v.get('rank') or 0)))
    details=[number,session['year'],b.get('short_description') or record.get('description') or '',names('author'),names('co-author'),names('co-sponsor'),b.get('digest') or '',BASE_URL+record['url'],names('sponsor'),term,b['id'],session['lpid'],b.get('stage_verbose') or b.get('stage') or '',b.get('dead'),b.get('title') or '']
    actions=data['bill_actions']
    if not isinstance(actions,list):raise ValueError('IN malformed action list')
    histories=[];seen=set()
    for action in actions:
        if action['bill_name']!=number or action['id'] in seen:raise ValueError('IN action identity mismatch/duplicate')
        seen.add(action['id']);when=datetime.strptime(action['date'],'%m/%d/%Y, %H:%M:%S').date().isoformat()
        if not action['text'] or action['chamber'] not in ('house','senate'):raise ValueError('IN missing action/chamber')
        histories.append([number,session['year'],when,action['chamber'],action['text'],len(histories)+1,term,action['id'],action['sequence'],action.get('bill_version') or '',action.get('committee_id') or '',action.get('vote_id') or '',action.get('rollcall_id') or ''])
    if not histories and b.get('status')!='not_released' and any((details[2],details[3],details[6])):raise ValueError('IN populated bill has no actions')
    return details,histories


def _cached(http,path,route,force):
    if path.exists() and not force:return json.loads(path.read_text())
    response=http.get(API+route,timeout=90);response.raise_for_status();time.sleep(1)
    if 'json' not in response.headers.get('Content-Type','').lower():raise ValueError('IN API returned HTML instead of JSON: '+route)
    data=response.json();pending=path.with_suffix('.json.tmp');pending.write_text(json.dumps(data));pending.replace(path);return data


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='IN':raise ValueError('Indiana scraper requires state IN')
    parse_term(term);folder=REPO_ROOT/'.data/IN/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'IN_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.IN_scrape_{term}.json'
    if all(p.exists() for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping IN {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,counts=[],[],{}
    with make_session() as http:
        http.headers.update(BROWSER_HEADERS)
        sessions=select_sessions(_cached(http,cache/'sessions.json','getSessionYears?upToYear=2014',force_fetch),term)
        for session in sessions:
            year=session['year'];records=parse_listing(_cached(http,cache/f'{year}_index.json','getBills?'+urlencode({'session_lpid':session['lpid']}),force_fetch),session)
            counts[year]=len(records);print(f'IN {year}: {len(records)} indexed bills/resolutions',flush=True)
            for i,record in enumerate(records,1):
                number=record['base_name'];data=_cached(http,cache/f'{year}_{number}.json','getBillDetails?'+urlencode({'session_lpid':session['lpid'],'bill_basename':number}),force_fetch)
                d,h=parse_bill(data,record,session,term);details.append(d);histories.extend(h)
                if verbose or i%25==0 or i==len(records):print(f'IN {year} {i}/{len(records)} {number}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        manifest.unlink(missing_ok=True)
        for pending,target in staged:pending.replace(target)
        keys={(h[0],h[1]) for h in histories}
        write_manifest(manifest, {'term':term,'sessions':sessions,'counts':counts,'details':len(details),'histories':len(histories),'source_empty_histories':[{'bill':d[0],'session':d[1]} for d in details if (d[0],d[1]) not in keys],})
    finally:
        for pending,_ in staged:pending.unlink(missing_ok=True)
    print(f'IN {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
