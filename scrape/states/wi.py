"""Wisconsin proposal hierarchy and complete histories, including special sessions."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io
import csv
from datetime import date, datetime
from pathlib import Path
import re
import time
from urllib.parse import urlparse,urljoin
from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL='https://docs.legis.wisconsin.gov'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_number','session','session_type','primary_sponsor','cosponsors','outchamber_sponsors','status','summary','bill_url','term','sponsor_source']
HISTORY_HEADER=['bill_number','session','session_type','chamber','action_date','action','journal_page','order','links','term']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('WI term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start<1995 or start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('WI requires an odd-starting consecutive term from 1995 without future years')
    return start,end


def _text(node):return ' '.join(node.get_text(' ',strip=True).split()) if node else ''


def parse_children(html,path):
    soup=BeautifulSoup(html,'lxml');lists=soup.select('ul.docLinks,ul.infoLinks')
    if len(lists)!=1 or '</html>' not in html.lower():raise ValueError('WI missing/ambiguous or truncated proposal index')
    records=[];seen=set()
    for a in lists[0].select('a[href]'):
        child=urlparse(a['href']).path
        if not child.startswith(path+'/') or '/' in child[len(path)+1:]:raise ValueError('WI unexpected index hierarchy link: '+child)
        if child in seen:raise ValueError('WI duplicate index link')
        seen.add(child);records.append({'path':child,'label':_text(a)})
    if not records:raise ValueError('WI empty proposal index')
    return records


def parse_sponsors(cell):
    text=_text(cell);m=re.match(r'Introduced(?: privately)? by (?:Representatives? |Senators? )?(.+)',text,re.I)
    if not m:raise ValueError('WI unrecognized introduction sponsor text')
    sections=re.split(r';?\s*cosponsored by (?:Representatives? |Senators? )?',m[1],flags=re.I,maxsplit=1)
    # The linked names retain surnames such as B. Jacobson and Cabral-Guevara.
    inside=[];outside=[];other=False
    for node in cell.descendants:
        if isinstance(node,str) and re.search(r'cosponsored by',node,re.I):other=True
        if getattr(node,'name',None)=='a' and '/legislator/' in node.get('href',''):
            (outside if other else inside).append(_text(node))
    if inside:return inside[0],'; '.join(inside[1:]),'; '.join(outside),text
    # Committee authors are plain text rather than legislator links.
    return sections[0].strip(' ;.'),'',sections[1].strip(' ;.') if len(sections)>1 else '',text


def parse_bill(html,record,term):
    soup=BeautifulSoup(html,'lxml');path=record['path'];start,end=parse_term(term);parts=path.split('/');kind=parts[3];base=parts[-1];m=re.fullmatch(r'(ab|sb|ajr|sjr|ar|sr)(\d+)',base)
    if not m:raise ValueError('WI unknown proposal number')
    number=m[1]+m[2].zfill(4)
    if not soup.find(string=lambda s:s and s.strip()==path):raise ValueError('WI page canonical path differs from index')
    if _text(soup.title)!=record['label']:raise ValueError('WI page/index title mismatch')
    title=soup.select_one('.proposalTitle');description=title.find_next_sibling() if title else None
    summary=_text(description.find('p') if description else None)
    if not summary:raise ValueError('WI missing proposal summary')
    heading=soup.find('h2',string=re.compile('^History$'));table=heading.find_next('table',class_='history') if heading else None
    if table is None:raise ValueError('WI missing complete History table')
    histories=[];introduction=None
    for row in table.select('tr'):
        cells=row.find_all('td',recursive=False)
        if not cells:continue
        if len(cells)!=3:raise ValueError('WI malformed history row')
        date_match=re.match(r'^(\d{1,2}/\d{1,2}/\d{4})\b',_text(cells[0]));chamber=cells[0].select_one('abbr.house')
        if not date_match:raise ValueError('WI missing action date')
        when=datetime.strptime(date_match[1],'%m/%d/%Y').date().isoformat();action=_text(cells[1])
        if not action:raise ValueError('WI blank action')
        if re.match(r'Introduced(?: privately)? by ',action,re.I):
            if introduction is not None:raise ValueError('WI multiple introduction sponsor rows')
            introduction=cells[1]
        histories.append([number,start,kind,_text(chamber),when,action,_text(cells[2]),len(histories)+1,'; '.join(urljoin(BASE_URL,a['href']) for a in row.select('a[href]')),term])
    if histories and introduction is None:raise ValueError('WI history lacks introduction sponsors')
    primary,co,out,source=parse_sponsors(introduction) if introduction else ('','','','')
    detail=[number,start,kind,primary,co,out,_text(soup.select_one('.propStatus')).removeprefix('Status: '),summary,BASE_URL+path,term,source]
    return detail,histories


def _cached(http,path,url,force):
    if cache_io.exists(path) and not force:return cache_io.read_text(path)
    r=http.get(url,timeout=90);r.raise_for_status();time.sleep(0.75)
    pending=path.with_suffix('.html.tmp');cache_io.write_text(pending, r.text);cache_io.replace(pending, path);return r.text


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='WI':raise ValueError('Wisconsin scraper requires state WI')
    start,end=parse_term(term);folder=REPO_ROOT/'.data/WI/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'WI_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.WI_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping WI {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,counts=[],[],{}
    with make_session() as http:
        def page(path):return _cached(http,cache/(path.strip('/').replace('/','_')+'.html'),BASE_URL+path,force_fetch)
        root=f'/{start}/proposals';sessions=parse_children(page(root),root);records=[]
        if not any(s['path']==root+'/reg' for s in sessions):raise ValueError('WI regular session not cataloged')
        for session in sessions:
            before=len(records)
            for chamber in parse_children(page(session['path']),session['path']):
                for category in parse_children(page(chamber['path']),chamber['path']):
                    records.extend(parse_children(page(category['path']),category['path']))
            counts[session['path'].split('/')[-1]]=len(records)-before
        if len({r['path'] for r in records})!=len(records):raise ValueError('WI overlapping indexes')
        print(f'WI {term}: {len(records)} instruments; {counts}',flush=True)
        for i,record in enumerate(records,1):
            d,h=parse_bill(page(record['path']),record,term);details.append(d);histories.extend(h)
            if verbose or i%25==0 or i==len(records):print(f'WI {i}/{len(records)} {record["path"]}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        keys={(h[0],h[2]) for h in histories}
        write_manifest(manifest, {'term':term,'counts':counts,'details':len(details),'histories':len(histories),'source_empty_histories':[{'bill':d[0],'session':d[2]} for d in details if (d[0],d[2]) not in keys],})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'WI {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
