"""Wyoming's official LSO bill-information API for an explicit two-year term."""
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

API='https://web.wyoleg.gov/LsoService/api/BillInformation'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_number','session','primary_sponsor','cosponsors','status','chapter_num','shortTitle','title','bill_url','term','source_id','source_year','special_session','roll_calls']
HISTORY_HEADER=['bill_number','session','action_date','chamber','action','voteid','order','term','source_timestamp']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('WY term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start<2015 or start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('WY requires an odd-starting consecutive term from 2015 without future years')
    return start,end


def parse_listing(rows,year):
    if not isinstance(rows,list) or not rows:raise ValueError('WY empty/malformed annual index')
    seen=set()
    for record in rows:
        if record['year']!=year or not re.fullmatch(r'(HB|SF|HJ|SJ)\d{4}',record['billNum']):raise ValueError('WY index bill/year mismatch')
        key=(record['billNum'],record.get('specialSessionValue'))
        if key in seen:raise ValueError('WY duplicate indexed bill')
        seen.add(key)
    return rows


def parse_bill(data,record,term):
    number=record['billNum'];year=record['year'];special=record.get('specialSessionValue')
    if data['bill']!=number or data['year']!=year or data.get('specialSessionValue')!=special:raise ValueError('WY bill/session identity mismatch')
    session=str(year)+('-RS' if special is None else '-SS'+str(special));primary=[];others=[]
    for sponsor in data['sponsors']:
        if not isinstance(sponsor['primarySponsor'],bool) or not sponsor['name']:raise ValueError('WY malformed sponsor')
        name=' '.join(filter(None,[sponsor.get('sponsorTitle'),sponsor['name']]))
        (primary if sponsor['primarySponsor'] else others).append(name)
    if not primary or not data['billTitle'].strip() or not data['catchTitle'].strip():raise ValueError('WY missing sponsor/title')
    detail=[number,session,'; '.join(primary),'; '.join(others),data['billStatus'],data['chapter'],data['catchTitle'].strip(),' '.join(data['billTitle'].split()),API+f'/{year}/{number}',term,data['id'],year,special if special is not None else '',json.dumps(data['rollCalls'])]
    actions=data['billActions']
    if not isinstance(actions,list) or not actions:raise ValueError('WY missing action history')
    histories=[]
    for i,action in enumerate(actions):
        if action['billInformationID']!=data['id'] or not action['statusMessage'].strip():raise ValueError('WY action identity mismatch or empty action')
        when=datetime.fromisoformat(action['statusDate']).date().isoformat()
        histories.append([number,session,when,action['location'],action['statusMessage'].strip(),action['voteId'],len(actions)-i,term,action['statusDate']])
    return detail,histories


def _cached(http,path,url,force):
    if path.exists() and not force:return json.loads(path.read_text())
    r=http.get(url,timeout=120);r.raise_for_status();data=r.json();time.sleep(0.75)
    pending=path.with_suffix('.json.tmp');pending.write_text(json.dumps(data));pending.replace(path);return data


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='WY':raise ValueError('Wyoming scraper requires state WY')
    years=parse_term(term);folder=REPO_ROOT/'.data/WY/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'WY_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.WY_scrape_{term}.json'
    if all(p.exists() for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping WY {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,counts=[],[],{}
    with make_session() as http:
        for year in years:
            url=API+'?'+urlencode({'$filter':f'Year eq {year}','$orderby':'BillNum'})
            records=parse_listing(_cached(http,cache/f'{year}_index.json',url,force_fetch),year);counts[str(year)]=len(records)
            print(f'WY {year}: {len(records)} instruments',flush=True)
            for i,record in enumerate(records,1):
                number=record['billNum'];data=_cached(http,cache/f'{year}_{number}.json',API+f'/{year}/{number}',force_fetch);d,h=parse_bill(data,record,term);details.append(d);histories.extend(h)
                if verbose or i%25==0 or i==len(records):print(f'WY {year} {i}/{len(records)} {number}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        manifest.unlink(missing_ok=True)
        for pending,target in staged:pending.replace(target)
        write_manifest(manifest, {'term':term,'counts':counts,'details':len(details),'histories':len(histories),})
    finally:
        for pending,_ in staged:pending.unlink(missing_ok=True)
    print(f'WY {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
