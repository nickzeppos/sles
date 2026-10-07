"""New York Assembly's public term-filtered search, covering both chambers."""
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

BASE_URL='https://nyassembly.gov/leg/'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_number','session','sponsor','cosponsor','multi_sponsor','descrip','summary','bill_url','term','canonical_bill_number','companion','source_status']
HISTORY_HEADER=['bill_number','session','chamber','action_date','action','order','term','action_bill_number']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('NY term must be YYYY_YYYY, for example 2025_2026')
    start,end=map(int,term.split('_'))
    if start<1999 or start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('NY requires an odd-starting consecutive term from 1999 without future years')
    return start,end


def validate_term(html,term):
    start,end=parse_term(term);soup=BeautifulSoup(html,'lxml')
    matches=[o for o in soup.select('#term option') if o.get('value')==str(start) and o.get_text(strip=True)==f'{start}-{str(end)[-2:]}']
    if len(matches)!=1:raise ValueError('NY requested term not listed in source')


def parse_listing(html,term):
    start,_=parse_term(term);validate_term(html,term)
    if '</html>' not in html.lower():raise ValueError('NY truncated search response')
    soup=BeautifulSoup(html,'lxml');rows=[];seen=set()
    for a in soup.select('a[href^="?bn"]'):
        url=urljoin(BASE_URL,a['href']);query=parse_qs(urlparse(url).query);number=a.get_text(strip=True)
        if query.get('term')!=[str(start)] or query.get('bn',[''])[0].strip()!=number:raise ValueError('NY index term/number mismatch')
        if not re.fullmatch(r'[ABCEJKRS]?\d{5}',number):raise ValueError(f'NY unknown indexed instrument {number}')
        if number in seen:raise ValueError('NY duplicate indexed bill')
        seen.add(number)
        description=str(a.next_sibling).strip() if a.next_sibling else ''
        if '<' in description:raise ValueError('NY unexpected description markup')
        rows.append({'number':number,'url':url,'description':description})
    if not rows:raise ValueError('NY empty search results')
    for a in soup.select('a[href]'):
        if re.fullmatch(r'(?:next|next page|more results)',a.get_text(' ',strip=True),re.I):raise ValueError('NY search now paginates; update parser')
    return rows


def _same_bill(value,number):
    # The source's blank-prefix zero placeholder is normalized to A00000.
    if number == '00000': number = 'A00000'
    return re.fullmatch(re.escape(number)+r'[A-Z]*',value) is not None


def parse_bill(html,record,term):
    start,_=parse_term(term);soup=BeautifulSoup(html,'lxml')
    term_input=soup.select_one('input[name=term]');bill_input=soup.select_one('input[name=bn]')
    if term_input is None or term_input.get('value')!=str(start) or bill_input is None or not _same_bill(bill_input.get('value','').strip(),record['number']):
        raise ValueError('NY bill form identity mismatch')
    content=soup.get_text(' ',strip=True)
    if 'Unable to Contact Server' in content:raise ValueError('NY source could not contact its bill database')
    summary_heading=soup.select_one('#jump_to_Summary');actions_heading=soup.select_one('#jump_to_Actions')
    if summary_heading is None:
        if re.search(r'"\s*'+re.escape(bill_input['value'].strip())+r'\s*" was not found\.',content):return None
        raise ValueError('NY missing summary without explicit bill-not-found response')
    if actions_heading is None:raise ValueError('NY missing actions section')
    fields={};summaries=[]
    for row in summary_heading.find_next('table').select('tr'):
        cells=row.find_all('td',recursive=False)
        if len(cells)==2:fields[cells[0].get_text(' ',strip=True)]=cells[1].get_text(' ',strip=True)
        elif len(cells)==1 and cells[0].get('colspan')=='2':summaries.append(cells[0].get_text(' ',strip=True))
    canonical=fields.get('BILL NO','')
    if not _same_bill(canonical,record['number']):raise ValueError('NY summary bill identity mismatch')
    if not {'SPONSOR','COSPNSR','MLTSPNSR'}<=fields.keys():raise ValueError('NY missing sponsor fields')
    companion=fields.get('SAME AS','');companion='' if companion=='No Same As' else companion
    status='inactive_in_session' if 'This bill is not active in this session.' in content else ''
    detail=[record['number'],start,fields['SPONSOR'],fields['COSPNSR'],fields['MLTSPNSR'],record['description'],
            summaries[-1] if summaries else '',record['url'],term,canonical,companion,status]
    histories=[];action_bill=''
    for row in actions_heading.find_next('table').select('tr'):
        values=[c.get_text(' ',strip=True) for c in row.find_all('td',recursive=False)]
        if not any(values):continue
        if values[0]=='BILL NO':
            if len(values)!=2 or not _same_bill(values[1],record['number']):raise ValueError('NY history bill identity mismatch')
            action_bill=values[1];continue
        # Substituted bills have an indented identity heading followed by
        # indented, dated actions. Keep their own bill number on those rows.
        indented=not values[0]
        while values and not values[0]:values=values[1:]
        if indented and len(values)==1:
            substitute=re.fullmatch(r'([ABCEJKRS]\d{5}[A-Z]*)\s+AMEND=([A-Z]*)\s+.*',values[0])
            if not substitute:raise ValueError(f'NY unknown substitute heading {values}')
            action_bill=substitute[1]+substitute[2];continue
        if len(values)!=2 or not action_bill:raise ValueError(f'NY unexpected history row {values}')
        when=datetime.strptime(values[0],'%m/%d/%Y').date().isoformat();action=values[1]
        if not action:raise ValueError('NY blank action')
        # Preserve Connor's source-capitalization convention for these legacy
        # action tables. It is a chamber inference, not a separate source field.
        chamber='Senate' if action.upper()==action else 'Assembly'
        histories.append([record['number'],start,chamber,when,action,len(histories)+1,term,action_bill])
    if not histories:
        if action_bill and not any((record['description'], fields['SPONSOR'], fields['COSPNSR'], fields['MLTSPNSR'], *summaries)):
            detail[-1]='empty_source_placeholder'
        else:raise ValueError('NY empty history for otherwise populated instrument')
    return detail,histories


def _fetch(http,url,form=None):
    r=http.get(url,timeout=60) if form is None else http.post(url,data=form,timeout=120)
    r.raise_for_status();time.sleep(1);return r.text


def _save(path,html):
    pending=path.with_suffix('.html.tmp');cache_io.write_text(pending, html,encoding='utf-8');cache_io.replace(pending, path)


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='NY':raise ValueError('New York scraper requires state NY')
    start,end=parse_term(term);folder=REPO_ROOT/'.data/NY/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'NY_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.NY_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping NY {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,missing=[],[],[]
    with make_session() as http:
        landing=_fetch(http,BASE_URL+'?sh=advanced');validate_term(landing,term)
        index=cache/'index.html'
        html=cache_io.read_text(index) if cache_io.exists(index) and not force_fetch else _fetch(http,BASE_URL+'?sh=advanced',{'evt_fld':'Search','by':'a','term':str(start),'leg_type':'A','comm_status':'A'})
        records=parse_listing(html,term);_save(index,html);print(f'NY {term}: {len(records)} indexed instruments',flush=True)
        for i,record in enumerate(records,1):
            local=cache/f'{record["number"]}.html';url=record['url']+'&Summary=Y&Actions=Y'
            html=cache_io.read_text(local) if cache_io.exists(local) and not force_fetch else _fetch(http,url)
            parsed=parse_bill(html,record,term);_save(local,html)
            if parsed is None:missing.append(record)
            else:details.append(parsed[0]);histories.extend(parsed[1])
            if verbose or i%25==0 or i==len(records):print(f'NY {i}/{len(records)} {record["number"]}: {len(parsed[1]) if parsed else "source not found"}',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'indexed':len(records),'details':len(details),'histories':len(histories),'source_not_found':missing,'empty_source_placeholders':[d[0] for d in details if d[-1]=='empty_source_placeholder'],'chamber_method':'Legacy action capitalization convention (inferred)',})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'NY {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
