"""West Virginia annual bill/resolution indexes and full action tables."""
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

BASE_URL='https://www.wvlegislature.gov/Bill_Status/'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_number','session_year','session_type','primary_sponsor','cosponsors','summary','subjects','companion','bill_url','term','effective_note']
HISTORY_HEADER=['bill_number','session_year','session_type','chamber','action_date','action','journal_page','order','term','links']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('WV term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start<1993 or start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('WV requires an odd-starting consecutive term from 1993 without future years')
    return start,end


def _text(node):return ' '.join(node.get_text(' ',strip=True).split()) if node else ''


def session_label(year,kind):
    return f'{year} Regular Session' if kind=='RS' else f'{year} '+{'1x':'1st','2x':'2nd','3x':'3rd','4x':'4th','5x':'5th','6x':'6th','7x':'7th'}[kind]+' Special Session'


def parse_catalog(html,years):
    soup=BeautifulSoup(html,'lxml');available={o['value'] for o in soup.select('#sel_year option[value]')}
    if not set(map(str,years))<=available:raise ValueError('WV requested years not in catalog')
    types=[o['value'].upper() if o['value'].lower()=='rs' else o['value'].lower() for o in soup.select('#sel_sessiontype option[value]')]
    if 'RS' not in types or any(not re.fullmatch(r'RS|[1-7]x',k) for k in types):raise ValueError('WV unknown session selector')
    return types


def parse_listing(html,year,kind):
    soup=BeautifulSoup(html,'lxml');label=session_label(year,kind)
    if _text(soup.find('h1'))!='Bill Status - '+label or '</html>' not in html.lower():raise ValueError('WV index session mismatch or truncated page')
    table=soup.select_one('table#results') or soup.select_one('table.tabborder')
    if table is None:raise ValueError('WV missing index table')
    records=[];seen=set()
    for a in table.select('tr > td:first-child a[href]'):
        if 'history.cfm' not in a['href'].lower():continue
        number=re.sub(r'\s+','',_text(a));m=re.fullmatch(r'(HB|SB|HCR|SCR|HJR|SJR|HR|SR)(\d+)',number)
        url=urljoin(BASE_URL,a['href']);q={k.lower():v for k,v in parse_qs(urlparse(url).query).items()}
        if not m or q.get('year')!=[str(year)] or [v.lower() for v in q.get('sessiontype',[])]!=[kind.lower()] or (q.get('input') or q.get('input4'))!=[str(int(m[2]))]:raise ValueError('WV index bill/session identity mismatch')
        if number in seen:raise ValueError('WV duplicate bill index record')
        seen.add(number);records.append({'number':m[1]+m[2].zfill(4),'prefix':m[1],'digits':str(int(m[2])),'year':year,'kind':kind,'url':url})
    if not records and f'The {label} does not exist.' not in _text(soup):raise ValueError('WV empty index without explicit nonexistent-session message')
    return records


def parse_bill(html,record,term):
    soup=BeautifulSoup(html,'lxml');year=record['year'];kind=record['kind'];number=record['number'];prefix=record['prefix']
    if _text(soup.find('h1'))!='Bill Status - '+session_label(year,kind):raise ValueError('WV detail session mismatch')
    types={'HB':'House Bill','SB':'Senate Bill','HCR':'House Concurrent Resolution','SCR':'Senate Concurrent Resolution','HJR':'House Joint Resolution','SJR':'Senate Joint Resolution','HR':'House Resolution','SR':'Senate Resolution'}
    if _text(soup.find('h2'))!=types[prefix]+' '+record['digits']:raise ValueError('WV detail bill mismatch')
    def field(pattern,required=False):
        node=soup.find('strong',string=re.compile(pattern))
        if node is None:
            if required:raise ValueError('WV missing '+pattern)
            return None
        return node.find_parent('td').find_next_sibling('td')
    summary=_text(field('^SUMMARY',True));primary=_text(field('^LEAD SPONSOR',True))
    if not primary:
        originating=[_text(td).removeprefix('Originating in ') for td in soup.select('#action-table td') if re.fullmatch(r'Originating in (House|Senate) .+',_text(td))]
        if len(set(originating))==1:primary=originating[0]
    if not summary:raise ValueError('WV blank summary')
    def names(pattern):
        cell=field(pattern);return '; '.join(_text(a) for a in cell.select('a')) if cell else ''
    table=soup.select_one('#action-table') or soup.select_one('table.tabborder')
    if table is None:raise ValueError('WV missing action table')
    rows=[r for r in table.select('tr') if r.find('td')];histories=[];notes=[]
    for row in rows:
        cells=row.find_all('td',recursive=False)
        if len(cells)==1 and cells[0].get('colspan')=='4':notes.append(_text(cells[0]));continue
        if len(cells)!=4:raise ValueError('WV malformed action row')
        chamber,action,raw,journal=map(_text,cells)
        if chamber not in ('H','S','') or not action:raise ValueError('WV missing action/unknown chamber')
        when=datetime.strptime(raw,'%m/%d/%y').date().isoformat() if raw else ''
        links='; '.join(urljoin(record['url'],a['href']) for a in row.select('a[href]'))
        histories.append([number,year,kind,chamber,when,action,journal,0,term,links])
    if not histories:raise ValueError('WV empty history')
    for i,h in enumerate(histories):h[7]=len(histories)-i
    detail=[number,year,kind,primary,names('^SPONSORS'),summary,names('^SUBJECT'),_text(field('SAME AS|SIMILAR TO')),record['url'],term,'; '.join(notes)]
    return detail,histories


def _cached(http,path,url,force):
    if cache_io.exists(path) and not force:return cache_io.read_text(path)
    r=http.get(url,timeout=90);r.raise_for_status();time.sleep(0.75)
    pending=path.with_suffix('.html.tmp');cache_io.write_text(pending, r.text);cache_io.replace(pending, path);return r.text


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='WV':raise ValueError('West Virginia scraper requires state WV')
    years=parse_term(term);folder=REPO_ROOT/'.data/WV/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'WV_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.WV_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping WV {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,counts=[],[],{}
    with make_session() as http:
        http.headers['User-Agent']='Mozilla/5.0'
        kinds=parse_catalog(_cached(http,cache/'catalog.html',BASE_URL+'bill_status.cfm',force_fetch),years)
        records=[]
        for year in years:
            for kind in kinds:
                count=0
                for endpoint,btype in (('Bills_all_bills.cfm','bill'),('res_list.cfm','res')):
                    url=BASE_URL+f'{endpoint}?year={year}&sessiontype={kind}&btype={btype}'
                    rows=parse_listing(_cached(http,cache/f'{year}_{kind}_{btype}.html',url,force_fetch),year,kind);records.extend(rows);count+=len(rows)
                counts[f'{year}_{kind}']=count
        if len({(r['year'],r['kind'],r['number']) for r in records})!=len(records):raise ValueError('WV overlapping indexes')
        print(f'WV {term}: {len(records)} instruments; {counts}',flush=True)
        for i,record in enumerate(records,1):
            d,h=parse_bill(_cached(http,cache/f'{record["year"]}_{record["kind"]}_{record["number"]}.html',record['url'],force_fetch),record,term);details.append(d);histories.extend(h)
            if verbose or i%25==0 or i==len(records):print(f'WV {i}/{len(records)} {record["year"]} {record["kind"]} {record["number"]}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'counts':counts,'details':len(details),'histories':len(histories),'source_without_primary_sponsor':[{'bill':d[0],'year':d[1],'session':d[2]} for d in details if not d[3]],})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'WV {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
