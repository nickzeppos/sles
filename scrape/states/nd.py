"""North Dakota assembly-wide bill index and dated action tables."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io
import csv
from datetime import date, datetime
from pathlib import Path
import re
import time
from urllib.parse import urljoin,urlparse
from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL='https://ndlegis.gov'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_number','ga_num','term','session','primary_sponsor','all_sponsors','title','bill_url','source_session']
HISTORY_HEADER=['bill_number','ga_num','term','session','chamber','action_date','action','version','roll_call','journal_page','order','source_session','source_urls']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('ND term must be YYYY_YYYY, for example 2025_2026')
    start,end=map(int,term.split('_'))
    if start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('ND requires an odd-starting consecutive term without future years')
    return start,end,(start-1889)//2+1


def select_assembly(html,term):
    start,end,assembly=parse_term(term);soup=BeautifulSoup(html,'lxml');base=f'{BASE_URL}/assembly/{assembly}-{start}/'
    sessions={}
    for a in soup.select('a[href]'):
        url=urljoin(BASE_URL,a['href']).rstrip('/')
        if not url.startswith(base):continue
        part=url.removeprefix(base)
        if re.fullmatch(r'regular|special(?:-\d+)?',part):sessions[part]=url
    if 'regular' not in sessions:raise ValueError('ND requested assembly not listed')
    return {'number':assembly,'base':base,'sessions':sessions}


def parse_listing(html,assembly,term):
    soup=BeautifulSoup(html,'lxml');rows=[];seen=set();source_paths=set()
    for card in soup.select('div.col.bill'):
        def attr(name):
            node=card.select_one('li.'+name)
            if node is None:raise ValueError(f'ND missing index attribute {name}')
            return node.get_text(' ',strip=True)
        number=attr('bill-name');m=re.fullmatch(r'(HB|SB|HCR|SCR|HR|SR|HMR|SMR) (\d{4})',number)
        if not m or attr('assembly')!=str(assembly['number']):raise ValueError('ND index bill/assembly mismatch')
        a=card.select_one('a.card-link[href]');url=urljoin(assembly['base'],a['href']) if a else ''
        path=re.fullmatch(re.escape(assembly['base'])+r'(regular|special(?:-\d+)?)/bill-overview/bo'+m[2]+r'\.html',url)
        if not path or path[1] not in assembly['sessions']:raise ValueError('ND unlisted session or wrong bill URL')
        source_paths.add(path[1]);sid=attr('session');label=card.select_one('.session-badge').get_text(' ',strip=True)
        years=re.findall(r'\b\d{4}\b',label)
        if len(years)!=1 or int(years[0]) not in parse_term(term)[:2]:raise ValueError('ND session year outside requested term')
        key=(sid,number)
        if key in seen:raise ValueError('ND duplicate indexed bill')
        seen.add(key);kind='RS' if path[1]=='regular' else 'SS'+(path[1].split('-')[1] if '-' in path[1] else '1')
        rows.append({'number':number,'digits':m[2],'url':url,'sid':sid,'kind':kind,'label':label,'year':int(years[0]),'path':path[1]})
    if not rows or source_paths!=set(assembly['sessions']):raise ValueError('ND index missing a listed session')
    return rows


def validate_page(soup,record,assembly,kind):
    title=soup.select_one('h1.page-title')
    if title is None or title.get_text(' ',strip=True)!=record['number']+' - '+kind:raise ValueError('ND wrong bill page')
    # Both pages repeat the current session immediately above the bill heading.
    text=soup.get_text(' ',strip=True)
    label=(f'{record["year"]} Regular Session' if record['kind']=='RS' else record['label'].replace('Special Session (','Special Session - ').rstrip(')'))
    if f'{label} ({assembly["number"]}' not in text:raise ValueError('ND page session mismatch')


def parse_bill(overview,actions,record,assembly,term):
    soup=BeautifulSoup(overview,'lxml');validate_page(soup,record,assembly,'Overview')
    title_head=soup.find('h5',string='Title');sponsor_head=soup.find('h5',string='Sponsors')
    if title_head is None or sponsor_head is None:raise ValueError('ND missing title/sponsors')
    title=title_head.find_next('p',class_='show-more').get_text(' ',strip=True)
    sponsors=sponsor_head.find_next('p').get_text(' ',strip=True).removeprefix('Introduced by ')
    sponsors='; '.join(x.strip() for x in sponsors.split(',') if x.strip());primary=sponsors.split(';')[0]
    if not title:raise ValueError('ND empty title')
    detail=[record['number'],assembly['number'],term,record['kind'],primary,sponsors,title,record['url'],record['sid']]
    soup=BeautifulSoup(actions,'lxml');validate_page(soup,record,assembly,'Actions')
    table=soup.select_one('#action-table')
    if table is None:
        if re.search(r'Prefile Withdrawn',title,re.I):return detail,[[record['number'],assembly['number'],term,record['kind'],'','','Prefile Withdrawn','','','',1,record['sid'],'']]
        raise ValueError('ND missing action table')
    history=[];last_date=''
    for row in table.find('tbody').find_all('tr',recursive=False):
        cells=row.find_all(['td','th'],recursive=False)
        if len(cells) not in (3,6):raise ValueError('ND malformed action row')
        for modal in row.select('.modal'):modal.decompose()
        values=[c.get_text(' ',strip=True) for c in cells]
        if not values[0] and not re.search('[A-Za-z]',values[2]):continue
        raw=cells[0].get('data-sort','')
        if raw:
            if not re.fullmatch(r'\d{14}',raw):raise ValueError('ND malformed full action timestamp')
            when=datetime.strptime(raw,'%Y%m%d%H%M%S').date()
            if when.strftime('%m/%d')!=values[0]:raise ValueError('ND displayed/full action date disagreement')
            when=when.isoformat()
        elif values[0]:
            when=datetime.strptime(values[0]+'/'+str(record['year']),'%m/%d/%Y').date().isoformat()
        elif last_date:when=last_date
        else:raise ValueError('ND undated initial action')
        last_date=when
        extra=values[3:] if len(values)==6 else ['','','']
        links='; '.join(dict.fromkeys(urljoin(record['url'].replace('bill-overview/bo','bill-actions/ba'),a['href']) for a in row.select('a[href]') if a['href'] and not a['href'].startswith('#')))
        history.append([record['number'],assembly['number'],term,record['kind'],values[1],when,values[2],*extra,len(history)+1,record['sid'],links])
    if not history:raise ValueError('ND empty action table')
    return detail,history


def _cached(http,path,url,force):
    if cache_io.exists(path) and not force:return cache_io.read_text(path, encoding='utf-8')
    r=http.get(url,timeout=60);r.raise_for_status();time.sleep(1)
    pending=path.with_suffix('.html.tmp');cache_io.write_text(pending, r.text,encoding='utf-8');cache_io.replace(pending, path);return r.text


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='ND':raise ValueError('North Dakota scraper requires state ND')
    parse_term(term);folder=REPO_ROOT/'.data/ND/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'ND_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.ND_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping ND {term}: completed outputs exist (use --force-fetch to refresh)');return
    details,histories=[],[]
    with make_session() as http:
        assembly=select_assembly(_cached(http,cache/'sessions.html',BASE_URL+'/assembly',force_fetch),term)
        records=parse_listing(_cached(http,cache/'index.html',assembly['base']+'bill-index.html',force_fetch),assembly,term)
        print(f'ND {term}: {len(records)} bills/resolutions across {len(assembly["sessions"])} sessions',flush=True)
        for i,record in enumerate(records,1):
            key=record['sid']+'_'+record['digits']
            overview=_cached(http,cache/f'{key}.html',record['url'],force_fetch)
            actions=_cached(http,cache/f'{key}_actions.html',record['url'].replace('bill-overview/bo','bill-actions/ba'),force_fetch)
            detail,history=parse_bill(overview,actions,record,assembly,term);details.append(detail);histories.extend(history)
            if verbose or i%25==0 or i==len(records):print(f'ND {i}/{len(records)} {record["number"]}: {len(history)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'assembly':assembly,'details':len(details),'histories':len(histories),})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'ND {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
