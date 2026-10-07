"""South Dakota's public legislative API, including cataloged special sessions."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest
import csv
from datetime import date, datetime
import json
from pathlib import Path
import re
import time
from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL='https://sdlegislature.gov'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_id','session','session_year','primary_sponsor','all_sponsors','summary','keywords','bill_url','source_id','sponsor_records','term']
HISTORY_HEADER=['bill_id','session','session_year','action_date','action','order','vote_url','chamber','committee','assigned_committee','document_id','vote','source_action','term']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('SD term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start<2009 or start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('SD requires an odd-starting consecutive term from 2009 without future years')
    return start,end


def select_sessions(rows,years):
    sessions=[r for r in rows if int(r['Year'][:4]) in years]
    if {int(r['Year']) for r in sessions if not r['SpecialSession']}!=set(years):raise ValueError('SD catalog lacks requested regular session')
    if len({r['SessionId'] for r in sessions})!=len(sessions):raise ValueError('SD duplicate sessions')
    return sorted(sessions,key=lambda r:(r['Year'],r['SessionId']))


def parse_listing(rows,session):
    if not isinstance(rows,list) or not rows:raise ValueError('SD empty bill index')
    ids=set();numbers=set()
    for row in rows:
        number=row['BillType']+str(row['BillNumberOnly'])
        if row['Year']!=session['Year'] or row['BillId'] in ids or number in numbers:raise ValueError('SD index identity mismatch or duplicate')
        if not re.fullmatch(r'[HS][A-Z]*\d+',number):raise ValueError('SD unknown bill number')
        ids.add(row['BillId']);numbers.add(number)
    return rows


def parse_bill(bill,actions,record,session,term):
    number=record['BillType']+str(record['BillNumberOnly']);year=int(session['Year'][:4]);label=session['Year']
    if bill['BillId']!=record['BillId'] or bill['SessionId']!=session['SessionId'] or bill['BillType']+str(bill['BillNumber'])!=number:raise ValueError('SD bill identity mismatch')
    if not bill['Title'].strip():raise ValueError('SD missing title')
    sponsors=bill['BillSponsor'];primary=[]
    for sponsor in sponsors:
        if sponsor['SponsorType'] not in ('P','C') or sponsor['MemberType'] not in ('H','S'):raise ValueError('SD unknown sponsor role')
        if sponsor['SponsorType']=='P':primary.append(('Representative ' if sponsor['MemberType']=='H' else 'Senator ')+sponsor['Member']['UniqueName'])
    all_sponsors=' '.join(BeautifulSoup(bill['BillCommitteeSponsor'],'lxml').get_text(' ',strip=True).split())
    if not primary and not sponsors:primary=[re.split(r' at the request',all_sponsors)[0]]
    if not all_sponsors:raise ValueError('SD missing sponsors')
    detail=[number,label,year,'; '.join(primary),all_sponsors,bill['Title'],'; '.join(k['Keyword'] for k in bill['Keywords']),BASE_URL+'/Session/Bill/'+str(bill['BillId']),bill['BillId'],json.dumps(sponsors),term]
    if not isinstance(actions,list) or not actions:raise ValueError('SD missing action log')
    histories=[]
    for action in actions:
        when=datetime.fromisoformat(action['ActionDate']).date().isoformat();text=action['StatusText'] or action['Description']
        if not text:raise ValueError('SD blank action')
        committee=action.get('ActionCommittee') or {};assigned=action.get('AssignedCommittee') or {};vote=action.get('Vote') or {}
        if action.get('ShowCommitteeName') or text in ('Do Pass','Tabled'):
            context=committee.get('FullName') or (action.get('ConferenceCommittee') or {}).get('Name','')
            if context:text=context+' '+text
        if action.get('ShowAssignedCommittee') and assigned:text+=' '+assigned['FullName']
        if action.get('ShowPassed') or action.get('ShowFailed'):
            result={'P':'Passed','F':'Failed','D':'Failed for lack of a second','W':'Withdrawn'}.get(action.get('Result'))
            if result:text+=', '+result
        if action.get('ActionDate2'):text+=' on '+datetime.fromisoformat(action['ActionDate2']).date().isoformat()
        if vote:text+=f", YEAS {vote.get('Yeas',0)}, NAYS {vote.get('Nays',0)}"
        # Preserve the entire source record alongside readable action/context fields.
        histories.append([number,label,year,when,text,len(histories)+1,BASE_URL+'/Session/Vote/'+str(vote['VoteId']) if vote else '',committee.get('Body',''),committee.get('FullName',''),assigned.get('FullName',''),action.get('DocumentId') or '',json.dumps(vote) if vote else '',json.dumps(action),term])
    return detail,histories


def _get(http,cache,name,route,force):
    path=cache/(name+'.json')
    if path.exists() and not force:return json.loads(path.read_text())
    r=http.get(BASE_URL+'/api/'+route,timeout=90);r.raise_for_status();data=r.json();time.sleep(0.5)
    pending=path.with_suffix('.json.tmp');pending.write_text(json.dumps(data));pending.replace(path);return data


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='SD':raise ValueError('South Dakota scraper requires state SD')
    years=parse_term(term);folder=REPO_ROOT/'.data/SD/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'SD_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.SD_scrape_{term}.json'
    if all(p.exists() for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping SD {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,counts=[],[],{}
    with make_session() as http:
        sessions=select_sessions(_get(http,cache,'sessions','Sessions',force_fetch),years)
        for session in sessions:
            sid=str(session['SessionId']);records=parse_listing(_get(http,cache,sid+'_index','Bills/Session/'+sid,force_fetch),session);counts[session['Year']]=len(records)
            print(f'SD {session["Year"]}: {len(records)} instruments',flush=True)
            for i,record in enumerate(records,1):
                bid=str(record['BillId']);bill=_get(http,cache,bid,'Bills/'+bid,force_fetch);actions=_get(http,cache,bid+'_actions','Bills/ActionLog/'+bid,force_fetch)
                d,h=parse_bill(bill,actions,record,session,term);details.append(d);histories.extend(h)
                if verbose or i%25==0 or i==len(records):print(f'SD {session["Year"]} {i}/{len(records)} {d[0]}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        manifest.unlink(missing_ok=True)
        for pending,target in staged:pending.replace(target)
        write_manifest(manifest, {'term':term,'counts':counts,'details':len(details),'histories':len(histories),'sessions':sessions,})
    finally:
        for pending,_ in staged:pending.unlink(missing_ok=True)
    print(f'SD {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
