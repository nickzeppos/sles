"""Texas official FTP inventory and Legislature Online bill histories."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io
import csv
from datetime import date, datetime
from ftplib import FTP
import json
from pathlib import Path
import re
import time
import xml.etree.ElementTree as ET
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL='https://capitol.texas.gov'
FTP_HOST='ftp.legis.state.tx.us'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_number','session','summary','keywords','authors','coauthors','sponsors','cosponsors','house_committee','hc_status','senate_committee','sc_status','bill_url','term']
HISTORY_HEADER=['bill_number','session','chamber','action_date','action','comment','journal_page','order','time','links','term']
CATEGORIES={'house_bills':'HB','house_concurrent_resolutions':'HCR','house_joint_resolutions':'HJR','house_resolutions':'HR','senate_bills':'SB','senate_concurrent_resolutions':'SCR','senate_joint_resolutions':'SJR','senate_resolutions':'SR'}


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('TX term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start<1995 or start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('TX requires an odd-starting consecutive term from 1995 without future years')
    return (start-1847)//2


def select_sessions(names,legislature):
    selected=sorted(n for n in names if re.fullmatch(str(legislature)+r'(R|[1-9]\d*)',n))
    if str(legislature)+'R' not in selected:raise ValueError('TX FTP lacks requested regular session')
    return sorted(selected,key=lambda s:(not s.endswith('R'),s))


def inventory(session):
    records=[];seen=set()
    with FTP(FTP_HOST,timeout=60) as ftp:
        ftp.login();root=f'/bills/{session}/billhistory';ftp.cwd(root);folders=ftp.nlst()
        categories=[name for name in folders if name in CATEGORIES]
        if not categories:raise ValueError('TX missing bill history directories')
        for category in categories:
            prefix=CATEGORIES[category];ftp.cwd(root+'/'+category);groups=ftp.nlst()
            for group in groups:
                if not re.fullmatch(prefix+r'\d+_'+prefix+r'\d+',group):raise ValueError('TX unexpected inventory group: '+group)
                ftp.cwd(root+'/'+category+'/'+group)
                for filename in ftp.nlst():
                    m=re.fullmatch(prefix+r'\s+(\d+)\.xml',filename,re.I)
                    if not m:raise ValueError('TX unexpected inventory filename: '+filename)
                    number=prefix+str(int(m[1]))
                    if number in seen:raise ValueError('TX duplicate bill inventory record')
                    seen.add(number);records.append({'number':number,'session':session,'xml_path':root+'/'+category+'/'+group+'/'+filename,'url':BASE_URL+f'/BillLookup/History.aspx?LegSess={session}&Bill={number}'})
    if not records:raise ValueError('TX empty inventory')
    return records


def _text(node):return ' '.join(node.get_text(' ',strip=True).split()) if node else ''


def parse_bill(html,record,term):
    soup=BeautifulSoup(html,'lxml');session=record['session'];number=record['number'];m=re.fullmatch(r'([A-Z]+)(\d+)',number);label=session[:-1]+'('+session[-1]+')';display=m[1]+' '+m[2]
    if _text(soup.title)!=f'{label} History for {display} | Texas Legislature Online':raise ValueError('TX bill/session title mismatch')
    heading=soup.find('h1')
    if not _text(heading).startswith(f'History for {label} {display} by '):raise ValueError('TX bill heading mismatch')
    summary=soup.select_one('#lblCaptionText')
    if not _text(summary):raise ValueError('TX missing caption')
    def field(name):return _text(soup.select_one('#'+name)).replace(' | ','; ')
    authors=field('lblAuthor')
    if not authors:raise ValueError('TX missing author')
    committees={}
    for slot in (1,2):
        label_node=soup.select_one(f'#lblComm{slot}CommitteeLabel')
        if not _text(label_node):continue
        label=_text(label_node)
        if label not in ('House Committee:','Senate Committee:'):raise ValueError('TX unknown committee chamber')
        committees[label.split()[0]]=(field(f'lblComm{slot}CommitteeValue'),field(f'lblComm{slot}CommitteeStatusValue'))
    keywords=soup.select_one('#lblSubjects');keywords='; '.join(keywords.stripped_strings) if keywords else ''
    detail=[display,session,_text(summary),keywords,authors,field('lblCoauthor'),field('lblSponsor'),field('lblCosponsor'),*committees.get('House',('','')),*committees.get('Senate',('','')),record['url'],term]
    table=soup.select_one('table.actions')
    if table is None:raise ValueError('TX missing action table')
    rows=[r for r in table.select('tr') if r.find('td')];histories=[]
    for i,row in enumerate(rows):
        cells=row.find_all('td',recursive=False)
        if len(cells)!=6:raise ValueError('TX malformed action row')
        chamber,action,comment,raw,clock,journal=map(_text,cells)
        if chamber not in ('H','S','E') or not action:raise ValueError('TX missing action/chamber')
        when=datetime.strptime(raw,'%m/%d/%Y').date().isoformat()
        links='; '.join(urljoin(record['url'],a['href']) for a in row.select('a[href]'))
        histories.append([display,session,{'H':'House','S':'Senate','E':'Executive'}[chamber],when,action,comment,journal,len(rows)-i,clock,links,term])
    if not histories:raise ValueError('TX empty history')
    return detail,histories


def _save(path,data):
    pending=path.with_suffix(path.suffix+'.tmp');cache_io.write_text(pending, data,encoding='utf-8');cache_io.replace(pending, path)


def parse_xml(data,record,term):
    root=ET.fromstring(data);session=record['session'];number=record['number']
    m=re.fullmatch(r'([A-Z]+)(\d+)',number);display=m[1]+' '+m[2]
    if root.tag!='billhistory' or root.get('bill')!=session[:-1]+'('+session[-1]+') '+display:
        raise ValueError('TX XML bill/session mismatch')
    def text(path):return (root.findtext(path) or '').strip()
    if not text('caption') or not text('authors'):raise ValueError('TX XML missing caption/author')
    committees=[]
    for chamber in ('house','senate'):
        node=root.find('committees/'+chamber)
        committees.extend([node.get('name',''),node.get('status','')] if node is not None else ['',''])
    source='ftp://'+FTP_HOST+record['xml_path']
    detail=[display,session,text('caption'),'; '.join(n.text or '' for n in root.findall('subjects/subject')),text('authors'),text('coauthors'),text('sponsors'),text('cosponsors'),*committees,source,term]
    history=[]
    for action in root.findall('actions/action'):
        code=action.findtext('actionNumber','');description=action.findtext('description','')
        if not code or code[0] not in 'HSE' or not description:raise ValueError('TX XML malformed action')
        when=datetime.strptime(action.findtext('date',''),'%m/%d/%Y').date().isoformat()
        history.append([display,session,{'H':'House','S':'Senate','E':'Executive'}[code[0]],when,description,action.findtext('comment',''),'',len(history)+1,action.findtext('actionTimestamp',''),source,term])
    if not history:raise ValueError('TX XML missing actions')
    return detail,history


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='TX':raise ValueError('Texas scraper requires state TX')
    legislature=parse_term(term);folder=REPO_ROOT/'.data/TX/bill';cache=folder/'.cache'/term;cache.mkdir(parents=True,exist_ok=True)
    outputs=[folder/f'TX_{kind}_{term}.csv' for kind in ('Bill_Details','Bill_Histories')];manifest=folder/f'.TX_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping TX {term}: completed outputs exist (use --force-fetch to refresh)');return
    catalog=cache/'sessions.json'
    if cache_io.exists(catalog) and not force_fetch:names=json.loads(cache_io.read_text(catalog))
    else:
        with FTP(FTP_HOST,timeout=60) as ftp:
            ftp.login();ftp.cwd('/bills');names=ftp.nlst()
        _save(catalog,json.dumps(names))
    sessions=select_sessions(names,legislature);details,histories,counts=[],[],{};xml_fallbacks=[]
    with make_session() as http:
        for session in sessions:
            local=cache/(session+'_index.json')
            if cache_io.exists(local) and not force_fetch:records=json.loads(cache_io.read_text(local))
            else:records=inventory(session);_save(local,json.dumps(records))
            counts[session]=len(records);print(f'TX {session}: {len(records)} bills/resolutions',flush=True)
            for i,record in enumerate(records,1):
                local=cache/(session+'_'+record['number']+'.html')
                if cache_io.exists(local) and not force_fetch:html=cache_io.read_text(local)
                else:
                    r=http.get(record['url'],timeout=90);r.raise_for_status();html=r.text;time.sleep(0.75);_save(local,html)
                if _text(BeautifulSoup(html,'lxml').title)=='Texas Legislature Online':
                    xmlpath=cache/(session+'_'+record['number']+'.xml')
                    if cache_io.exists(xmlpath) and not force_fetch:data=cache_io.read_bytes(xmlpath)
                    else:
                        with FTP(FTP_HOST,timeout=60) as ftp:
                            ftp.login();chunks=[];ftp.retrbinary('RETR '+record['xml_path'],chunks.append);data=b''.join(chunks)
                    d,h=parse_xml(data,record,term);cache_io.write_bytes(xmlpath, data);xml_fallbacks.append(record)
                else:d,h=parse_bill(html,record,term)
                details.append(d);histories.extend(h)
                if verbose or i%25==0 or i==len(records):print(f'TX {session} {i}/{len(records)} {record["number"]}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'counts':counts,'details':len(details),'histories':len(histories),'source_xml_fallbacks':xml_fallbacks,})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'TX {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
