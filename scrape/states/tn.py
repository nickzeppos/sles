"""Tennessee's current official bill indexes, including extraordinary sessions."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io
import csv
from datetime import date, datetime
from pathlib import Path
import re
import time
from urllib.parse import urljoin,urlparse,parse_qs
from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL='https://wapp.capitol.tn.gov/apps/'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_id','ga_num','term','session','companion_bill_num','prime_sponsor','coprime_sponsors','companion_sponsor','title','abstract','fiscal_summary','summary','bill_url']
HISTORY_HEADER=['bill_id','ga_num','term','session','action_date','chamber','action','order']
VOTES_HEADER=['bill_id','ga_num','term','session','house_vote_data','senate_vote_data']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('TN term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start<1995 or start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('TN requires an odd-starting consecutive term from 1995 without future years')
    return (start-1797)//2


def _text(node):return ' '.join(node.get_text(' ',strip=True).split()) if node else ''


def _identity(soup,ga,term):
    heading=soup.find('h1');years=term.replace('_','-')
    if not re.search(r'\b'+str(ga)+r'(?:st|nd|rd|th) General Assembly \('+years+r'\)',_text(heading)):raise ValueError('TN assembly/year heading mismatch')
    if '</html>' not in str(soup).lower():raise ValueError('TN truncated page')


def parse_catalog(html,ga,term):
    soup=BeautifulSoup(html,'lxml');_identity(soup,ga,term);ranges=[];special=[]
    for a in soup.select('a[href]'):
        url=urljoin(BASE_URL,a['href']);q=parse_qs(urlparse(url).query)
        if urlparse(url).path.lower().endswith('/indexes/billindex'):
            if q.get('ga')!=[str(ga)]:raise ValueError('TN range points to wrong assembly')
            start=q['startNum'][0];end=q['endNum'][0];m=re.fullmatch(r'(HB|SB|HJR|SJR|HR|SR)(\d+)',start);n=re.fullmatch(r'(HB|SB|HJR|SJR|HR|SR)(\d+)',end)
            if not m or not n or m[1]!=n[1] or int(n[2])<int(m[2]):raise ValueError('TN invalid bill range')
            ranges.append({'url':url,'prefix':m[1],'start':int(m[2]),'end':int(n[2])})
    for option in soup.select('#ssDropdown option[value]'):
        url=option['value'];q=parse_qs(urlparse(url).query)
        if q.get('ga')!=[str(ga)] or not q.get('specsessNum'):raise ValueError('TN special-session identity mismatch')
        special.append({'url':url,'session':'SS'+q['specsessNum'][0]})
    if not ranges or {r['prefix'] for r in ranges}!={'HB','SB','HJR','SJR','HR','SR'}:raise ValueError('TN missing bill categories')
    if len({r['url'] for r in ranges})!=len(ranges):raise ValueError('TN duplicate ranges')
    return ranges,special


def parse_listing(html,ga,term,session,group=None):
    soup=BeautifulSoup(html,'lxml');_identity(soup,ga,term);records=[];seen=set()
    if session!='RS' and 'Extraordinary Session' not in _text(soup.find('h1')):raise ValueError('TN missing special-session heading')
    for a in soup.select('a[href]'):
        url=urljoin(BASE_URL,a['href']);q={k.lower():v for k,v in parse_qs(urlparse(url).query).items()}
        if urlparse(url).path.lower().endswith('/billinfo/default'):
            number=_text(a)
            if not re.fullmatch(r'(HB|SB|HJR|SJR|HR|SR)\d{4}',number) or q.get('billnumber')!=[number] or q.get('ga')!=[str(ga)]:raise ValueError('TN index bill identity mismatch')
            if number in seen:raise ValueError('TN duplicate index bill')
            seen.add(number);records.append({'number':number,'url':url,'session':session})
    if not records:raise ValueError('TN empty bill index')
    if group:
        expected={group['prefix']+str(n).zfill(4) for n in range(group['start'],group['end']+1)}
        if seen!=expected:raise ValueError('TN index does not contain its complete advertised range')
    return records


def parse_bill(html,record,ga,term):
    soup=BeautifulSoup(html,'lxml');_identity(soup,ga,term);number=record['number'];base=[number,ga,term,record['session']]
    header=soup.select_one('#udpBillInfo h2');link=header.find('a',href=True) if header else None
    if link is None or re.sub(r'\s','',_text(link))!=number or f'/Bills/{ga}/Bill/{number}.pdf'.lower() not in link['href'].lower():raise ValueError('TN bill heading/PDF identity mismatch')
    prime=header.select_one('small a');primary=_text(prime).lstrip('*')
    if not primary:raise ValueError('TN missing prime sponsor')
    companion=soup.select_one('#udpBillInfo h3');comp=companion.find('a',href=True) if companion else None
    comp_number=re.sub(r'\s','',_text(comp));comp_sponsor=_text(companion.select_one('small a') if companion else None).lstrip('*')
    abstract=soup.select_one('.abstract-container')
    if not _text(abstract):raise ValueError('TN missing abstract')
    panel=soup.select_one('#tabpanel-summary')
    if panel is None:raise ValueError('TN missing summary panel')
    def summary(label):
        node=panel.find('h3',string=label)
        if node is None:raise ValueError('TN missing '+label)
        return _text(node.find_next_sibling('div'))
    detail=base+[comp_number,primary,_text(soup.select_one('#divCoPrimeSponsors')),comp_sponsor,_text(soup.select_one('#divCaptionText')),_text(abstract),summary('Fiscal Summary'),summary('Bill Summary'),record['url']]
    table=soup.select_one('#gvBillActionHistory')
    if table is None or _text(table.find('th'))!=number:raise ValueError('TN missing/wrong own-bill history')
    rows=[r for r in table.select('tr') if r.find('td')];histories=[]
    for i,row in enumerate(rows):
        cells=row.find_all('td',recursive=False)
        if len(cells)!=2:raise ValueError('TN malformed history')
        when=datetime.strptime(_text(cells[1]),'%m/%d/%Y').date().isoformat();classes=row.get('class',[])
        if classes and (len(classes)!=1 or classes[0] not in ('house','senate')):raise ValueError('TN unknown history chamber')
        histories.append(base+[when,classes[0] if classes else '',_text(cells[0]),len(rows)-i])
    if not histories:raise ValueError('TN empty history')
    votes=[]
    for selector in ('#pnlHouseVotes','#pnlSenateVotes'):
        block=soup.select_one(selector)
        if block is None:raise ValueError('TN missing vote panel')
        votes.append(_text(block))
    return detail,histories,base+votes


def _cached(http,path,url,force):
    if cache_io.exists(path) and not force:return cache_io.read_text(path)
    r=http.get(url,timeout=90);r.raise_for_status();r.encoding='utf-8';time.sleep(0.75)
    pending=path.with_suffix('.html.tmp');cache_io.write_text(pending, r.text,encoding='utf-8');cache_io.replace(pending, path);return r.text


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='TN':raise ValueError('Tennessee scraper requires state TN')
    ga=parse_term(term);folder=REPO_ROOT/'.data/TN/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'TN_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories','Vote_Details')];manifest=folder/f'.TN_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping TN {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,votes,counts=[],[],[],{}
    with make_session() as http:
        ranges,special=parse_catalog(_cached(http,cache/'catalog.html',BASE_URL+f'Indexes/BillsByIndex?ga={ga}',force_fetch),ga,term);records=[]
        for i,group in enumerate(ranges):
            records.extend(parse_listing(_cached(http,cache/f'range_{i}.html',group['url'],force_fetch),ga,term,'RS',group))
        counts['RS']=len(records)
        for session in special:
            rows=parse_listing(_cached(http,cache/(session['session']+'_index.html'),session['url'],force_fetch),ga,term,session['session']);records.extend(rows);counts[session['session']]=len(rows)
        if len({r['number'] for r in records})!=len(records):raise ValueError('TN overlapping session bill numbers')
        print(f'TN {ga}: {len(records)} instruments, {counts}',flush=True)
        for i,record in enumerate(records,1):
            d,h,v=parse_bill(_cached(http,cache/(record['number']+'.html'),record['url'],force_fetch),record,ga,term);details.append(d);histories.extend(h);votes.append(v)
            if verbose or i%25==0 or i==len(records):print(f'TN {i}/{len(records)} {record["number"]}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER,VOTES_HEADER),(details,histories,votes)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'ga':ga,'counts':counts,'details':len(details),'histories':len(histories),'source_blank_actions':[dict(zip(HISTORY_HEADER,h)) for h in histories if not h[6]],})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'TN {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
