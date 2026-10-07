"""South Carolina's counted prime-sponsor searches, including committees."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io
import csv
from datetime import date, datetime
from pathlib import Path
import re
import time
from urllib.parse import urlencode,urljoin,parse_qs,urlparse
from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL='https://www.scstatehouse.gov'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_number','session','bill_type','primary_sponsor','sponsors','title','summary','bill_url','source_session']
HISTORY_HEADER=['bill_number','session','chamber','action_date','action','journal_page','order','source_session','source_links']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('SC term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start<1999 or start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('SC requires an odd-starting consecutive term from 1999 without future years')
    return start,end


def select_session(html,term):
    start,end=parse_term(term);soup=BeautifulSoup(html,'lxml');found=[]
    for option in soup.select('#session option'):
        m=re.fullmatch(r'(\d+) - \((\d{4})-(\d{4})\)',option.get_text(' ',strip=True))
        if m and (int(m[2]),int(m[3]))==(start,end):
            if m[1]!=option['value']:raise ValueError('SC catalog session mismatch')
            found.append({'id':m[1],'label':f'{start}-{end}'})
    if len(found)!=1:raise ValueError('SC requested term not uniquely listed')
    return found[0]


def parse_sponsors(html,chamber):
    soup=BeautifulSoup(html,'lxml');selector='#Representative' if chamber=='H' else '#Senator';options=soup.select(selector+' option')
    if not options:raise ValueError('SC missing sponsor selector')
    rows=[];seen=set()
    for option in options:
        code=option['value']
        if code=='0':continue
        if code in seen or not code.isdigit():raise ValueError('SC duplicate/invalid sponsor code')
        seen.add(code);rows.append({'id':code,'chamber':chamber,'name':' '.join(option.get_text().split())})
    if not rows:raise ValueError('SC empty sponsor directory')
    return rows


def parse_results(html,sponsor,session,term):
    soup=BeautifulSoup(html,'lxml');box=soup.select_one('#resultsbox')
    if box is None:raise ValueError('SC missing sponsor results')
    text=' '.join(box.get_text(' ',strip=True).split())
    if not text.startswith(f'Session {session["id"]} - ({session["label"]})'):raise ValueError('SC result session mismatch')
    header=box.find('div',recursive=False);prime=header.find('a',href=re.compile('/member.php')) if header else None
    if prime is None or parse_qs(urlparse(prime['href']).query).get('code')!=[sponsor['id']]:raise ValueError('SC prime sponsor query mismatch')
    items=[a.parent for a in box.select('a[name]')]
    if 'No prime sponsored legislation during this session' in text:
        if items:raise ValueError('SC contradictory empty results')
        return []
    totals=[]
    for tr in box.select('table tr'):
        values=[c.get_text(' ',strip=True) for c in tr.find_all('td',recursive=False)]
        if len(values)==6 and values[0]=='TOTAL' and values[-1].isdigit():totals.append(int(values[-1]))
    if len(totals)!=1 or totals[0]!=len(items):raise ValueError('SC truncated/incomplete sponsor search')
    records=[];seen=set()
    for item in items:
        anchor=item.find('a',attrs={'name':True});span=item.find('span');text=span.get_text(' ',strip=True) if span else ''
        m=re.fullmatch(r'([HS])[\s*]*(\d+)\s+(.+?)(?:,?\s+[Bb]y)\s*(.*)',text)
        if not m or m[2]!=anchor['name']:raise ValueError('SC bill heading identity mismatch')
        number=m[1]+m[2]
        if number in seen:raise ValueError('SC duplicate bill in sponsor results')
        seen.add(number);kind=re.sub(r'\([^)]*\)','',m[3]).strip()
        names=[a.get_text(' ',strip=True) for a in span.select('a[href]')]
        sponsors='; '.join(names) if names else m[4].strip()
        label=item.find('b',string=re.compile('^Summary:'))
        if label is None:raise ValueError('SC missing summary heading')
        title=str(label.next_sibling).strip()
        chunks=[str(n).strip() for n in item.children if isinstance(n,str) and n.strip()]
        summary=' '.join(n for n in chunks if re.match(r'^(?:A|AN)\s',n)).removesuffix(' - ratified title')
        if not title or not summary:raise ValueError('SC empty title/summary')
        link=item.find('a',string=re.compile('^View full text$'))
        expected=f'/sess{session["id"]}_{session["label"]}/bills/{int(m[2])}.htm'
        allowed={expected,*(f'/sess{session["id"]}_{session["label"]}/appropriations{year}/gab{m[2]}.htm' for year in term.split('_'))}
        if link is None or link['href'] not in allowed:raise ValueError('SC full-text session/bill mismatch')
        expected=link['href']
        detail=[number,term,kind,sponsor['name'],sponsors,title,summary,urljoin(BASE_URL,expected),session['id']]
        table=item.find('table')
        if table is None:raise ValueError('SC missing bill history')
        history=[]
        for row in table.select('tr'):
            cells=row.find_all('td',recursive=False)
            if len(cells)!=3:raise ValueError('SC malformed history row')
            raw,chamber,action=[' '.join(c.get_text(' ',strip=True).split()) for c in cells]
            when=datetime.strptime(raw,'%m/%d/%y').date().isoformat()
            journals=[a for a in cells[2].select('a[href]') if 'journal' in a.get_text().lower()]
            journal='; '.join(a.get_text(' ',strip=True) for a in journals)
            for a in journals:action=action.replace('('+a.get_text(' ',strip=True)+')','').strip()
            links='; '.join(urljoin(BASE_URL,a['href']) for a in cells[2].select('a[href]'))
            history.append([number,term,chamber,when,action,journal,len(history)+1,session['id'],links])
        if not history:raise ValueError('SC empty history')
        records.append((detail,history))
    return records


def _fetch(http,url,form=None):
    r=http.get(url,timeout=90) if form is None else http.post(url,data=form,timeout=90);r.raise_for_status();time.sleep(1);return r.text


def _save(path,html):
    tmp=path.with_suffix('.html.tmp');cache_io.write_text(tmp, html,encoding='utf-8');cache_io.replace(tmp, path)


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='SC':raise ValueError('South Carolina scraper requires state SC')
    parse_term(term);folder=REPO_ROOT/'.data/SC/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'SC_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.SC_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping SC {term}: completed outputs exist (use --force-fetch to refresh)');return
    bills={};counts={}
    with make_session() as http:
        http.headers['User-Agent']='Mozilla/5.0'
        session=select_session(_fetch(http,BASE_URL+'/actionsearch.php'),term);sponsors=[]
        for chamber in ('H','S'):
            local=cache/f'{chamber}_sponsors.html';html=cache_io.read_text(local) if cache_io.exists(local) and not force_fetch else _fetch(http,BASE_URL+'/sponsorsearch.php',{'GETMEMBERS':chamber,'SESSION':session['id'],'PERM_SPONSOR_CODE':'0','PAGETYPE':'0'})
            sponsors.extend(parse_sponsors(html,chamber));_save(local,html)
        for i,sponsor in enumerate(sponsors,1):
            key=sponsor['chamber']+'_'+sponsor['id'];local=cache/f'{key}.html'
            params={'session':session['id'],'Senator':sponsor['id'] if sponsor['chamber']=='S' else '0','Representative':sponsor['id'] if sponsor['chamber']=='H' else '0','prime':'Y','summary':'B','headerfooter':'1'}
            html=cache_io.read_text(local) if cache_io.exists(local) and not force_fetch else _fetch(http,BASE_URL+'/sponsorsearch.php?'+urlencode(params))
            records=parse_results(html,sponsor,session,term);_save(local,html);counts[key]=len(records)
            for detail,history in records:
                if detail[0] in bills:
                    old,old_history=bills[detail[0]]
                    if old[:3]+old[4:]!=detail[:3]+detail[4:] or old_history!=history:raise ValueError('SC conflicting bill data across sponsor results')
                    old[3]+='; '+sponsor['name']
                else:bills[detail[0]]=(detail,history)
            print(f'SC {i}/{len(sponsors)} {sponsor["name"]}: {len(records)} records; {len(bills)} unique',flush=True)
    details=[bills[k][0] for k in sorted(bills)];histories=[row for k in sorted(bills) for row in bills[k][1]]
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'session':session,'sponsors':sponsors,'counts':counts,'details':len(details),'histories':len(histories),})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'SC {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
