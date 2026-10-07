"""Oklahoma's counted Text of Measures searches and official bill histories."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io
import csv
from datetime import date, datetime
from pathlib import Path
import re
import time
from urllib.parse import urlencode,urlparse,parse_qs,unquote
from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL='https://www.oklegislature.gov'
SEARCH=BASE_URL+'/TextOfMeasures.aspx'
REPO_ROOT=Path(__file__).resolve().parents[2]
PREFIX='ctl00_ContentPlaceHolder1_'
DETAILS_HEADER=['bill_number','session','intro_date','H_author','S_author','coauthors','short_title','bill_url','term','source_session','source_author','source_other_author','index_link_bill']
HISTORY_HEADER=['bill_number','session','chamber','action_date','journal_page','action','order','term','source_session']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('OK term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start%2!=1 or end!=start+1 or start<1993 or end>date.today().year:raise ValueError('OK requires an odd-starting consecutive term from 1993 without future years')
    return start,end


def select_sessions(html,term):
    start,end=parse_term(term);soup=BeautifulSoup(html,'lxml');sessions=[]
    specials={'Special':'SS1','First Special':'SS1','Second Special':'SS2','Third Special':'SS3','Fourth Special':'SS4'}
    for option in soup.select('#'+PREFIX+'cbxSessionId option'):
        label=option.get_text(' ',strip=True);m=re.match(r'(\d{4}) (.+)',label)
        if not m or not start<=int(m[1])<=end:continue
        kind=re.sub(r' Session(?: \((?:web|PROD)\))?$','',m[2]);code='RS' if kind=='Regular' else specials.get(kind)
        if not code:raise ValueError('OK unknown session type: '+label)
        sid=option['value']
        if not re.fullmatch(m[1][-2:]+r'(?:00|\dX)',sid):raise ValueError('OK session ID/year mismatch')
        sessions.append({'id':sid,'year':int(m[1]),'session':m[1]+'-'+code,'label':label})
    if {s['year'] for s in sessions if s['session'].endswith('-RS')}!={start,end}:raise ValueError('OK requested regular years not both listed')
    return sorted(sessions,key=lambda s:(s['year'],s['id']))


def search_form(html,session,chamber):
    soup=BeautifulSoup(html,'lxml');form={n['name']:n.get('value','') for n in soup.select('input[type=hidden][name]')}
    if '__VIEWSTATE' not in form:raise ValueError('OK missing ASP.NET form')
    form.update({'__EVENTTARGET':'ctl00$ContentPlaceHolder1$cbxSessionId','ctl00$ContentPlaceHolder1$rbChamber':chamber,'ctl00$ContentPlaceHolder1$format':'rbPDF','ctl00$ContentPlaceHolder1$cbxSessionId':session['id'],'ctl00$ContentPlaceHolder1$lbxMeasureStatus':'INT','ctl00$ContentPlaceHolder1$Button1':'Search'})
    return form


def parse_listing(html,session,chamber):
    soup=BeautifulSoup(html,'lxml');selected=soup.select_one('#'+PREFIX+'cbxSessionId option[selected]')
    if selected is None or selected['value']!=session['id']:raise ValueError('OK index session mismatch')
    table=soup.select_one('#'+PREFIX+'tblTomData')
    if table is None:raise ValueError('OK missing result table')
    count=re.search(r'([\d,]+) Records Found',table.get_text(' ',strip=True))
    if count is None:raise ValueError('OK missing search count')
    records=[];seen=set()
    for row in table.find_all('tr',recursive=False)[2:]:
        cells=row.find_all('td',recursive=False)
        if not row.get_text(strip=True) or re.fullmatch(r'[\d,]+ Records Found',row.get_text(' ',strip=True)):continue
        if len(cells)!=4:raise ValueError('OK malformed index row')
        number=cells[0].get_text(' ',strip=True)
        if not re.fullmatch(chamber+r'(?:B|R|JR|CR)\d+',number):raise ValueError('OK unknown/wrong-chamber bill number: '+number)
        link=cells[1].find('a',href=True)
        if link is None:raise ValueError('OK missing bill details link')
        params=parse_qs(urlparse(link['href']).query)
        if params.get('Session')!=[session['id']]:raise ValueError('OK index session link mismatch')
        pdf=cells[0].find('a',href=True)
        if pdf is None or not re.search(r'/'+re.escape(number)+r' int\.pdf$',unquote(urlparse(pdf['href']).path),re.I):raise ValueError('OK index PDF identity mismatch')
        if number in seen:raise ValueError('OK duplicate indexed measure')
        seen.add(number);raw=cells[1].get_text(' ',strip=True);intro=datetime.strptime(raw,'%m/%d/%Y').date().isoformat() if raw else ''
        title=cells[2].get_text(' ',strip=True);authors=cells[3].get_text(' ',strip=True)
        parts=[x.strip() for x in authors.split(',') if x.strip()]
        if any(not re.search(r'\([HS]\)$',x) for x in parts):raise ValueError('OK author lacks chamber annotation')
        house='; '.join(x for x in parts if x.endswith('(H)'));senate='; '.join(x for x in parts if x.endswith('(S)'))
        records.append({'number':number,'intro':intro,'house':house,'senate':senate,'title':title,'index_link_bill':params.get('Bill',[''])[0],'url':BASE_URL+'/BillInfo.aspx?'+urlencode({'Bill':number,'Session':session['id']})})
    if len(records)!=int(count[1].replace(',','')):raise ValueError('OK incomplete search results')
    return records


def parse_bill(html,record,session,term):
    soup=BeautifulSoup(html,'lxml');identity=soup.select_one('#'+PREFIX+'lblBillDisplay');selected=soup.select_one('#'+PREFIX+'cbxSessionId option[selected]')
    if identity is not None and identity.get_text(' ',strip=True)=='Invalid bill number' and record['title'].strip().lower() in ('','test') and not any(record[k] for k in ('house','senate')):
        return None
    if identity is None or re.sub(r'\s+','',identity.get_text())!=record['number'] or selected is None or selected['value']!=session['id']:raise ValueError('OK bill/session identity mismatch')
    authors=[]
    for key in ('lnkAuth','lnkOtherAuth'):
        node=soup.select_one('#'+PREFIX+key);authors.append(node.get_text(' ',strip=True) if node else '')
    coauthors=[];table=soup.select_one('#'+PREFIX+'TabContainer1_TabPanel6_tblCoAuth')
    if table:
        # Current names are above the pending-request section. Do not count
        # requested additions as current authors.
        for cell in table.select('td'):
            text=cell.get_text(' ',strip=True)
            if 'All Submitted Requests' in text:break
            if re.fullmatch(r'.+\([HS]\)',text):coauthors.append(text)
    details=[record['number'],session['session'],record['intro'],record['house'],record['senate'],'; '.join(coauthors),record['title'],record['url'],term,session['id'],*authors,record['index_link_bill']]
    table=soup.select_one('#'+PREFIX+'TabContainer1_TabPanel1_tblHouseActions')
    if table is None:raise ValueError('OK missing history table')
    history=[]
    explicit_none=False
    for row in table.find_all('tr',recursive=False)[2:]:
        if not row.get_text(strip=True):continue
        if row.get_text(' ',strip=True)=='None':explicit_none=True;continue
        cells=row.find_all('td',recursive=False)
        if len(cells)!=4:raise ValueError('OK malformed history row')
        action,journal,raw,chamber=[c.get_text(' ',strip=True) for c in cells]
        when=datetime.strptime(raw,'%m/%d/%Y').date().isoformat() if raw else ''
        if not action:raise ValueError('OK blank action')
        history.append([record['number'],session['session'],chamber,when,journal,action,len(history)+1,term,session['id']])
    if not history and not explicit_none:raise ValueError('OK empty history without explicit None marker')
    if not record['title'] and history:raise ValueError('OK populated bill is missing its index title')
    return details,history


def _save(path,html):
    pending=path.with_suffix('.html.tmp');cache_io.write_text(pending, html,encoding='utf-8');cache_io.replace(pending, path)


def _fetch(http,url,form=None):
    r=http.get(url,timeout=60) if form is None else http.post(url,data=form,timeout=120)
    r.raise_for_status();time.sleep(1);return r.text


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='OK':raise ValueError('Oklahoma scraper requires state OK')
    parse_term(term);folder=REPO_ROOT/'.data/OK/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'OK_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.OK_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping OK {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories,counts=[],[],{};missing=[]
    with make_session() as http:
        home=_fetch(http,SEARCH);sessions=select_sessions(home,term)
        for session in sessions:
            sid=session['id'];records=[]
            for chamber in ('H','S'):
                local=cache/f'{sid}_{chamber}_index.html'
                html=cache_io.read_text(local) if cache_io.exists(local) and not force_fetch else _fetch(http,SEARCH,search_form(_fetch(http,SEARCH),session,chamber))
                rows=parse_listing(html,session,chamber);_save(local,html);records.extend(rows);counts[sid+'_'+chamber]=len(rows)
                print(f'OK {session["label"]} {chamber}: {len(rows)} measures',flush=True)
            for i,record in enumerate(records,1):
                local=cache/f'{sid}_{record["number"]}.html';html=cache_io.read_text(local) if cache_io.exists(local) and not force_fetch else _fetch(http,record['url'])
                parsed=parse_bill(html,record,session,term);_save(local,html)
                if parsed is None:
                    missing.append({'session':sid,**record});continue
                d,h=parsed;details.append(d);histories.extend(h)
                if verbose or i%25==0 or i==len(records):print(f'OK {sid} {i}/{len(records)} {record["number"]}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        history_keys={(h[0],h[8]) for h in histories}
        write_manifest(manifest, {'term':term,'sessions':sessions,'counts':counts,'source_invalid_placeholders':missing,'details':len(details),'histories':len(histories),'source_empty_histories':[{'bill':d[0],'session':d[9]} for d in details if (d[0],d[9]) not in history_keys],'index_link_discrepancies':[{'bill':d[0],'session':d[9],'linked_bill':d[-1]} for d in details if d[0]!=d[-1]],})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'OK {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
