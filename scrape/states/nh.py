"""New Hampshire's counted legacy search, bill status, text and dockets."""
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

BASE_URL='https://gc.nh.gov/bill_status/legacy/bs2016/'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_number','session_year','sponsors','sponsors_on_bill','title','lsr','local_gov','chapter_num','S_status','S_floor_date','S_comm','H_status','H_floor_date','H_comm','bill_url','hist_url','vote_url','term','source_bill_label']
HISTORY_HEADER=['bill_number','session_year','chamber','action_date','action','order','term','source_lsr']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('NH term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start%2!=1 or end!=start+1 or start<1989 or end>date.today().year:raise ValueError('NH requires an odd-starting consecutive term from 1989 without future years')
    return start,end


def search_form(html,year):
    soup=BeautifulSoup(html,'lxml')
    if soup.select_one('input[name=txtsessionyear]') is None:raise ValueError('NH missing search form')
    form={n['name']:n.get('value','') for n in soup.select('input[name]') if n.get('type','text').lower() in ('hidden','text')}
    for n in soup.select('select[name]'):
        o=n.select_one('option[selected]') or n.find('option');form[n['name']]=o.get('value','') if o else ''
    form.update({'txtsessionyear':str(year),'ddlobody':'','sortoption':'billnumber','cmdsubmit':'Submit'})
    return form


def parse_listing(html,year):
    soup=BeautifulSoup(html,'lxml');text=soup.get_text(' ',strip=True);count=re.search(r'Bills Found\s*:\s*(\d+)',text)
    if not count:raise ValueError('NH response is not a counted result list')
    records=[];seen=set()
    for a in soup.select('a[href]'):
        if 'bill_docket.aspx' not in a['href'].lower():continue
        cell=a.find_parent('td');big=cell.find('big');label=cell.find('i')
        if big is None or label is None or ' '.join(label.get_text(' ',strip=True).split())!=f'Session Year {year}':raise ValueError('NH index identity/year mismatch')
        source_label=re.sub(r'\s+','',big.get_text())
        match=re.fullmatch(r'([A-Z]+\d+)(?:-(?:FN|A|L))*',source_label)
        if not match:raise ValueError('NH unknown bill number: '+source_label)
        number=match[1]
        links={}
        for link in cell.select('a[href]'):
            url=urljoin(BASE_URL,link['href']);path=urlparse(url).path.lower();params=parse_qs(urlparse(url).query)
            if any(p in path for p in ('bill_docket.aspx','bill_status.aspx','billtext.aspx')):
                if params.get('sy')!=[str(year)]:raise ValueError('NH link year mismatch')
            if 'bill_docket.aspx' in path:links['docket']=url;links['lsr']=params['lsr'][0]
            elif 'bill_status.aspx' in path:links['status']=url
            elif 'billtext.aspx' in path and params.get('txtFormat')==['html']:links['text']=url
            elif 'roll' in path:links['vote']=url
        if not {'docket','status','text','lsr'}<=links.keys():raise ValueError('NH missing record links')
        if links['lsr'] in seen:raise ValueError('NH duplicate source identifier')
        seen.add(links['lsr']);title_cell=cell.find_next_sibling('td');title_label=title_cell.find('b',string=re.compile('^Title:')) if title_cell else None
        if title_label is None:raise ValueError('NH missing index title')
        title=str(title_label.next_sibling).strip()
        if not title:raise ValueError('NH empty index title')
        records.append({'number':number,'year':year,'title':title,'source_label':source_label,**links})
    if len(records)!=int(count[1]) or not records:raise ValueError('NH incomplete/empty result list')
    return records


def _date(value):
    return datetime.strptime(value,'%m/%d/%Y').date().isoformat() if value else ''


def _guard(soup,record,docket=False):
    # The bill's own reciprocal status/docket link carries both session and LSR.
    target='bill_status.aspx' if docket else 'bill_docket.aspx'
    matches=[]
    for a in soup.select('a[href]'):
        if target in a['href'].lower():
            p=parse_qs(urlparse(a['href']).query)
            matches.append(p.get('sy')==[str(record['year'])] and int(p.get('lsr',['-1'])[0])==int(record['lsr']))
    if not matches or not all(matches):raise ValueError('NH detail session/LSR mismatch')
    text=soup.get_text(' ',strip=True)
    if not re.search(r'\b'+re.escape(record['number'])+r'\b',text):raise ValueError('NH detail bill number mismatch')


def parse_bill(html,docket,bill_text,record,term):
    soup=BeautifulSoup(html,'lxml');_guard(soup,record)
    def flag(label):
        node=soup.find('em',string=re.compile(label))
        if node is None:raise ValueError('NH missing '+label)
        value=node.find_next('b')
        return value.get_text(' ',strip=True) if value else ''
    sponsor_table=soup.select_one('#drep')
    if sponsor_table is None:raise ValueError('NH missing sponsors table')
    sponsors='; '.join(n.get_text(' ',strip=True).replace('\xa0',' ') for n in sponsor_table.select('td') if n.get_text(strip=True))
    source_text=BeautifulSoup(bill_text,'lxml');body=source_text.get_text(' ',strip=True)
    if not re.search(str(record['year'])+r'\s+SESSION',body,re.I):raise ValueError('NH bill text session mismatch')
    sponsor_line=next((n for n in source_text.select('p') if re.match(r'(?:SPONSORS?|INTRODUCED BY)\s*:',n.get_text(' ',strip=True),re.I)),None)
    on_bill=sponsor_line.get_text(' ',strip=True) if sponsor_line else ''
    headings=[b.get_text(' ',strip=True) for b in soup.find_all('b') if b.get_text(' ',strip=True) in ('House Status','Senate Status')]
    if len(headings)!=2 or set(headings)!={'House Status','Senate Status'}:raise ValueError('NH missing chamber status sections')
    info={}
    for offset,label in zip((0,8),headings):
        values=[]
        for n in (1,7,4):
            row=soup.select_one('#Tr'+str(n+offset));cells=row.find_all('td',recursive=False) if row else []
            if len(cells)!=3:raise ValueError('NH malformed chamber status')
            values.append(cells[2].get_text(' ',strip=True))
        values[1]=_date(values[1]);info[label]=values
    detail=[record['number'],record['year'],sponsors,on_bill,record['title'],record['lsr'],flag('Local Gov'),flag('Chapter#'),*info['Senate Status'],*info['House Status'],record['status'],record['docket'],record.get('vote',''),term,record['source_label']]
    source=BeautifulSoup(docket,'lxml');_guard(source,record,True)
    outer=source.select_one('#Table1');table=outer.find('table') if outer else None
    if table is None:raise ValueError('NH missing docket table')
    rows=table.find_all('tr');header=[c.get_text(' ',strip=True) for c in rows[0].find_all('td',recursive=False)] if rows else []
    if header!=['Date','Body','Description']:raise ValueError('NH unexpected docket columns')
    history=[]
    for row in rows[1:]:
        cells=row.find_all('td',recursive=False)
        if not cells:continue
        if len(cells)!=3:raise ValueError('NH malformed docket row')
        raw,chamber,action=[c.get_text(' ',strip=True) for c in cells]
        if not action:raise ValueError('NH empty action')
        # Preserve blank dates; Connor accidentally copied the previous action
        # text into the date column for these rows.
        history.append([record['number'],record['year'],chamber,_date(raw),action,len(history)+1,term,record['lsr']])
    if not history:raise ValueError('NH empty docket')
    return detail,history


def _save(path,html):
    tmp=path.with_suffix('.html.tmp');cache_io.write_text(tmp, html,encoding='utf-8');cache_io.replace(tmp, path)


def _fetch(http,url,form=None):
    r=http.get(url,timeout=60) if form is None else http.post(url,data=form,timeout=90)
    r.raise_for_status();time.sleep(1);return r.text


def repair_docket(http, page, status, record):
    """Recover the source's embedded metadata error through its own bill link."""
    if 'Advanced_BillStatus.Bill_Docket.getdata()' not in page:return page
    soup=BeautifulSoup(status,'lxml')
    links=[urljoin(BASE_URL,a['href']) for a in soup.select('a[href]')
           if a.get_text(' ',strip=True)=='Bill Docket' and 'bill_docket.aspx' in a['href'].lower()]
    if len(set(links))!=1:raise ValueError('NH missing unique reciprocal docket link')
    url=links[0];params=parse_qs(urlparse(url).query)
    if params.get('sy')!=[str(record['year'])] or int(params.get('lsr',['-1'])[0])!=int(record['lsr']):
        raise ValueError('NH reciprocal docket identity mismatch')
    for candidate in (url,url.replace('https://gc.nh.gov/','https://www.gencourt.state.nh.us/')):
        fresh=_fetch(http,candidate)
        if 'Advanced_BillStatus.Bill_Docket.getdata()' in fresh:continue
        _guard(BeautifulSoup(fresh,'lxml'),record,True)
        return fresh
    raise ValueError('NH docket metadata error persists on reciprocal and alternate-host routes')


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='NH':raise ValueError('New Hampshire scraper requires state NH')
    years=parse_term(term);folder=REPO_ROOT/'.data/NH/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'NH_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.NH_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping NH {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,counts=[],[],{}
    with make_session() as http:
        for year in years:
            index=cache/f'{year}_index.html'
            if cache_io.exists(index) and not force_fetch:html=cache_io.read_text(index)
            else:
                form=search_form(_fetch(http,BASE_URL),year);html=_fetch(http,BASE_URL,form)
            records=parse_listing(html,year);_save(index,html);counts[str(year)]=len(records);print(f'NH {year}: {len(records)} instruments',flush=True)
            for i,record in enumerate(records,1):
                pages=[]
                for kind in ('status','docket','text'):
                    local=cache/f'{year}_{record["lsr"]}_{kind}.html'
                    page=cache_io.read_text(local) if cache_io.exists(local) and not force_fetch else _fetch(http,record[kind])
                    if kind=='docket':page=repair_docket(http,page,pages[0],record)
                    _save(local,page);pages.append(page)
                d,h=parse_bill(*pages,record,term);details.append(d);histories.extend(h)
                if verbose or i%25==0 or i==len(records):print(f'NH {year} {i}/{len(records)} {record["number"]}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'counts':counts,'details':len(details),'histories':len(histories),})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'NH {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
