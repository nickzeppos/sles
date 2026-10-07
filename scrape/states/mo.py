"""Missouri House XML exports and Senate HTML for an explicit two-year term."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io

from collections import Counter
import csv
from datetime import date, datetime
import io
from pathlib import Path, PurePosixPath
import re
import time
from urllib.parse import parse_qs, urlencode, urljoin, urlparse
import xml.etree.ElementTree as ET
import zipfile

from bs4 import BeautifulSoup
from scrape.http import make_session

HOUSE='https://house.mo.gov'
EXPORT='https://documents.house.mo.gov'
SENATE='https://www.senate.mo.gov'
REPO_ROOT=Path(__file__).resolve().parents[2]
DETAILS_HEADER=['bill_number','session','session_year','session_type','title','description','primary_sponsor','ps_url','cosponsors','outchamber_sposnor','LR_number','committees','effective_date','summary','bill_url','term','chamber','source_session','summary_urls']
HISTORY_HEADER=['bill_number','session','session_year','session_type','chamber','action_date','action','journal_page','order','term','source_session','source_action_id','journal_url','comments']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}',term):raise ValueError('MO term must be YYYY_YYYY')
    start,end=map(int,term.split('_'))
    if start%2!=1 or end!=start+1 or end>date.today().year:raise ValueError('MO requires an odd-starting consecutive two-year term without future years')
    if start<2013:raise ValueError('MO native House export archives start in 2013')
    return start,end


def select_sessions(data,term):
    years=parse_term(term);root=ET.fromstring(data);records=[]
    if root.tag!='ROOT':raise ValueError('MO invalid XML session catalog')
    for row in root.findall('Session'):
        year=int(row.findtext('SessionYear','0'))
        if year not in years:continue
        code=row.findtext('SessionCode','');identity=row.findtext('ID','');assembly=row.findtext('GeneralAssembly','')
        if not re.fullmatch(r'R|S\d+',code) or not identity.isdigit():raise ValueError('MO unrecognized session code')
        expected=88+(years[0]-1995)//2
        if not assembly.startswith(str(expected)):raise ValueError('MO term/assembly mismatch')
        records.append({'id':identity,'year':year,'code':code,'assembly':assembly.split(' General Assembly')[0],
                        'label':row.findtext('Name'),'type':'RS' if code=='R' else 'ES'+code[1:]})
    if not all(any(s['year']==y and s['code']=='R' for s in records) for y in years):raise ValueError('MO did not list both regular sessions')
    if len({s['id'] for s in records})!=len(records):raise ValueError('MO duplicate session')
    return sorted(records,key=lambda s:s['id'])


def archive_links(html):
    result={}
    for a in BeautifulSoup(html,'lxml').select('a[href]'):
        url=urljoin(EXPORT,a['href']);m=re.fullmatch(r'/xml/(\d+)\.zip',urlparse(url).path)
        if m:result[m[1]]=url
    return result


def parse_house_listing(data,session):
    root=ET.fromstring(data);records=[]
    if root.tag!='ROOT':raise ValueError('MO invalid House index')
    for row in root.findall('BillXML'):
        number=row.findtext('BillType','')+row.findtext('BillNumber','');url=row.findtext('BillXMLLink','')
        if row.findtext('SessionYear')!=str(session['year']) or row.findtext('SessionCode')!=session['code']:raise ValueError('MO wrong-session House index')
        if not re.fullmatch(r'H[A-Z]*\d+',number):raise ValueError('MO unexpected House instrument')
        if url!=f'{EXPORT}/xml/{session["id"]}-{number}.xml':raise ValueError('MO wrong House XML link')
        records.append({'number':number,'url':url})
    if not records:raise ValueError('MO empty House index')
    # Archived catalogs repeat HR1/HR2 with different export timestamps but
    # the same canonical XML link. They identify one bill, not two bills.
    return list({r['number']:r for r in records}.values())


def _number(value):
    m=re.fullmatch(r'([A-Z]+)\s*(\d+)',value.strip())
    if not m:raise ValueError(f'MO invalid bill number: {value}')
    return m[1]+m[2].zfill(4)


def parse_house_bill(data,record,session,term):
    root=ET.fromstring(data);bill=root.find('BillInformation')
    if bill is None or bill.findtext('BillNumber')!=record['number']:raise ValueError('MO House bill identity mismatch')
    if bill.find('PastSessionAppropriationBill') is not None:return None
    def txt(path):return (bill.findtext(path) or '').strip()
    sponsors=bill.findall('Sponsor');primary=[s for s in sponsors if s.findtext('SponsorType')=='Sponsor']
    cosponsors=[s.findtext('FullName','') for s in sponsors if s.findtext('SponsorType')=='Co-Sponsor']
    # Conferees share this XML container but are not bill sponsors.
    unknown={s.findtext('SponsorType') for s in sponsors}-{'Sponsor','Co-Sponsor','Handler','HouseConferee','SenateConferee'}
    if unknown:raise ValueError(f'MO unexpected XML sponsor types: {unknown}')
    number=_number(record['number']);url=HOUSE+'/Bill.aspx?'+urlencode({'bill':record['number'],'year':session['year'],'code':session['code']})
    sponsor_urls='; '.join(HOUSE+'/MemberDetails.aspx?'+urlencode({'district':s.findtext('District'),'year':session['year'],'code':session['code']}) for s in primary if s.findtext('District'))
    committees=list(dict.fromkeys((e.text or '').strip() for e in bill.findall('Hearings/CommitteeName')))
    summary_urls='; '.join((e.text or '').strip() for e in bill.findall('BillSummary/SummaryTextLink'))
    detail=[number,session['assembly'],session['year'],session['type'],txt('Title/ShortTitle'),txt('Title/LongTitle'),
            '; '.join(s.findtext('FullName','') for s in primary),sponsor_urls,'; '.join(cosponsors),
            '; '.join(s.findtext('FullName','') for s in sponsors if s.findtext('SponsorType')=='Handler'),txt('CurrentLRNumber'),
            '; '.join(committees),txt('ProposedEffectiveDate'),'',url,term,'lower',session['id'],summary_urls]
    histories=[];seen=set();previous=-1
    for action in bill.findall('Action'):
        link=(action.findtext('Link') or '').strip();query=parse_qs(urlparse(link).query)
        if query.get('bill')!=[record['number']] or query.get('year')!=[str(session['year'])] or [x.strip() for x in query.get('code',[])]!=[session['code']]:raise ValueError('MO wrong-session House action link')
        identity=action.findtext('Guid','');sequence=int(action.findtext('ActivitySequence','0'))
        if not identity or identity in seen or sequence<previous:raise ValueError('MO duplicate/unordered House action')
        seen.add(identity);previous=sequence;description=(action.findtext('Description') or '').strip()
        when=date.fromisoformat(action.findtext('PubDate','')).isoformat()
        match=re.search(r'\(([HS])\)\s*$',description);chamber=match[1] if match else ''
        journals=[]
        for label in ('House','Senate'):
            start=action.findtext(label+'JournalStartPage');end=action.findtext(label+'JournalEndPage')
            if start:journals.append(label[0]+start+('-'+end if end and end!=start else ''))
        histories.append([number,session['assembly'],session['year'],session['type'],chamber,when,description,
                          '; '.join(journals),len(histories)+1,term,session['id'],identity,action.findtext('JournalLink',''),action.findtext('Comments','')])
    if not histories or not detail[5] or not detail[6]:raise ValueError(f'MO incomplete House bill: {record["url"]}')
    return detail,histories


def senate_session_links(html,year):
    links={}
    for a in BeautifulSoup(html,'lxml').select('a[href]'):
        url=urljoin(SENATE,a['href']);q=parse_qs(urlparse(url).query)
        if urlparse(url).path.lower()!='/billtracking/bills/billlist':continue
        if q.get('year')!=[str(year)]:raise ValueError('MO Senate year selector returned wrong year')
        code=q.get('session',[''])[0]
        if not re.fullmatch(r'R|E\d+',code):raise ValueError('MO unknown Senate session code')
        if code in links and links[code]!=url:raise ValueError('MO conflicting Senate session routes')
        links[code]=url
    if 'R' not in links:raise ValueError('MO missing Senate regular-session link')
    return links


def parse_senate_listing(html,session):
    soup=BeautifulSoup(html,'lxml');records=[]
    if soup.title is None or f'Bill List - {session["year"]}' not in soup.title.get_text():raise ValueError('MO wrong Senate index year')
    for number_node in soup.select('.bill-number'):
        link=number_node.select_one('a[href]')
        if link is None:
            card=number_node.find_parent(class_='card__body');title=card.select_one('.bill-title') if card else None
            if title is None or title.get_text(strip=True)!='WITHDRAWN':raise ValueError('MO non-withdrawn Senate bill lacks a link')
            button=next((b for b in card.select('[data-modal-url]') if parse_qs(urlparse(b['data-modal-url']).query).get('handler')==['Actions']),None)
            if button is None:raise ValueError('MO withdrawn bill lacks action link')
            url=urljoin(SENATE,button['data-modal-url']);q=parse_qs(urlparse(url).query)
            if q.get('year')!=[str(session['year'])]:raise ValueError('MO withdrawn bill wrong year')
            sponsor=card.select_one('.bill-sponsor-handler a[href]')
            records.append({'number':_number(number_node.get_text(strip=True)),'url':url,'billid':q['billId'][0],
                            'withdrawn':True,'sponsor':sponsor.get_text(' ',strip=True) if sponsor else '',
                            'sponsor_url':sponsor['href'] if sponsor else ''})
            continue
        number=_number(link.get_text(strip=True));url=urljoin(SENATE,link['href']);q=parse_qs(urlparse(url).query)
        if q.get('year')!=[str(session['year'])] or not q.get('billid',[''])[0].isdigit():raise ValueError('MO bad Senate bill link')
        records.append({'number':number,'url':url,'billid':q['billid'][0]})
    counts=Counter(re.sub(r'\d','',r['number']) for r in records)
    names={'Senate Bills':'SB','Senate Concurrent Resolutions':'SCR','Senate Joint Resolutions':'SJR','Senate Resolutions':'SR'}
    reported={}
    for card in soup.select('.stat-card'):
        label=card.select_one('.stat-label');value=card.select_one('.stat-number')
        if label is None or value is None or label.get_text(strip=True) not in names:raise ValueError('MO unexpected Senate count card')
        reported[names[label.get_text(strip=True)]]=int(value.get_text(strip=True).replace(',',''))
    if set(reported)!=set(names.values()) or any(counts[k]!=v for k,v in reported.items()):raise ValueError('MO Senate index counts disagree')
    # Remonstrances (SRM) appear in the all-types list but have no summary card.
    if set(counts)-set(names.values())-{'SRM'}:raise ValueError('MO unexpected Senate instrument type')
    if len({r['number'] for r in records})!=len(records) or len({r['billid'] for r in records})!=len(records):raise ValueError('MO duplicate Senate bills')
    return records


def parse_senate_bill(html,record,session,term):
    soup=BeautifulSoup(html,'lxml');heading=soup.select_one('.main-header-text');description=soup.select_one('.main-header-description')
    if heading is None or _number(heading.get_text(' ',strip=True).split(' - ',1)[0])!=record['number']:raise ValueError('MO wrong Senate bill detail')
    fields={}
    for item in soup.select('.detail-grid__item'):
        label=item.select_one('.detail-grid__label');value=item.select_one('.detail-grid__value')
        if label is None or value is None:raise ValueError('MO malformed Senate detail field')
        fields[label.get_text(' ',strip=True)]=value
    if not all(k in fields for k in ('Sponsor','LR Number','Title')) or description is None:raise ValueError('MO incomplete Senate detail')
    def text(key):return fields[key].get_text(' ',strip=True) if key in fields else ''
    actions_url=''
    for button in soup.select('[data-modal-url]'):
        q=parse_qs(urlparse(button['data-modal-url']).query)
        if q.get('handler')==['Actions']:
            if q.get('year')!=[str(session['year'])] or q.get('billId')!=[record['billid']]:raise ValueError('MO wrong Senate action link')
            actions_url=urljoin(SENATE,button['data-modal-url'])
        if q.get('handler')==['CoSponsors']:raise ValueError('MO separate Senate cosponsor panel requires parsing')
    if not actions_url:raise ValueError('MO missing Senate all-actions link')
    summary=soup.select_one('.text-content--preformatted');handler=text('House Handler');handler='' if handler=='N/A' else handler
    detail=[record['number'],session['assembly'],session['year'],session['type'],text('Title'),description.get_text(' ',strip=True),
            text('Sponsor'),'; '.join(urljoin(SENATE,a['href']) for a in fields['Sponsor'].select('a[href]')),
            text('Co-Sponsors') or text('Cosponsors'),handler,text('LR Number'),text('Committee'),text('Effective Date'),
            summary.get_text(' ',strip=True) if summary else '',record['url'],term,'upper',session['id'],'']
    return detail,actions_url


def parse_senate_actions(html,record,session,term):
    soup=BeautifulSoup(html,'lxml');container=soup.select_one('[data-bill-identifier]')
    if container is None or _number(container['data-bill-identifier'])!=record['number']:raise ValueError('MO wrong Senate history')
    rows=container.select('#actionsTable tbody tr');histories=[]
    for row in rows:
        cells=row.find_all('td',recursive=False)
        if len(cells)!=3:raise ValueError('MO malformed Senate action')
        when=datetime.strptime(cells[0].get_text(strip=True),'%m/%d/%Y').date().isoformat();action=cells[1].get_text(' ',strip=True)
        if not action:raise ValueError('MO empty Senate action')
        histories.append([record['number'],session['assembly'],session['year'],session['type'],'',when,action,
                          cells[2].get_text(' ',strip=True),len(histories)+1,term,session['id'],'',
                          '; '.join(urljoin(SENATE,a['href']) for a in cells[2].select('a[href]')),''])
    if not histories:raise ValueError('MO empty Senate history')
    return histories


def _fetch(http,url):
    time.sleep(1);r=http.get(url,timeout=120);r.raise_for_status();return r.content


def _cached(http,url,path,force):
    # House asks clients not to re-poll XML exports more than twice an hour.
    if cache_io.exists(path) and (not force or (url.startswith(EXPORT) and time.time()-cache_io.stat(path).st_mtime<1800)):
        return cache_io.read_bytes(path)
    data=_fetch(http,url);pending=path.with_suffix(path.suffix+'.tmp');cache_io.write_bytes(pending, data);cache_io.replace(pending, path);return data


@scrape_run
def scrape(state,term,verbose=False,force_fetch=False):
    if state.upper()!='MO':raise ValueError('Missouri scraper requires state MO')
    years=parse_term(term);folder=REPO_ROOT/'.data/MO/bill';cache=folder/'.cache'/term
    outputs=[folder/f'MO_{name}_{term}.csv' for name in ('Bill_Details','Bill_Histories')];manifest=folder/f'.MO_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping MO {term}: completed outputs exist (use --force-fetch to refresh)');return
    cache.mkdir(parents=True,exist_ok=True);details,histories,counts,carryovers=[],[],{},[]
    with make_session() as http:
        script=_cached(http,EXPORT+'/SessionSet.js?v=3',cache/'SessionSet.js',force_fetch).decode('utf-8-sig')
        base=re.search(r"var baseURL\s*=\s*'([^']+)'",script)
        if not base or not base[1].startswith(EXPORT+'/xml/'):raise ValueError('MO missing public XML base URL')
        sessions=select_sessions(_cached(http,base[1]+'SessionList.XML',cache/'sessions.xml',force_fetch),term)
        archives=archive_links(_cached(http,EXPORT+'/XMLPastExports.html',cache/'archives.html',force_fetch))
        senate_links={}
        for year in years:
            url=SENATE+f'/BillTracking/LegislativeInformation?handler=YearSelected&SelectedYear={year}'
            senate_links[year]=senate_session_links(_cached(http,url,cache/f'senate_{year}_sessions.html',force_fetch),year)
            expected={s['code'].replace('S','E',1) for s in sessions if s['year']==year}
            if set(senate_links[year])!=expected:raise ValueError('MO House/Senate session catalogs disagree')
        for session in sessions:
            archive=None;members={}
            if session['id'] in archives:
                data=_cached(http,archives[session['id']],cache/f'{session["id"]}.zip',force_fetch);archive=zipfile.ZipFile(io.BytesIO(data))
                for name in archive.namelist():
                    leaf=PurePosixPath(name).name.lower()
                    if leaf in members:raise ValueError('MO duplicate archive filenames')
                    members[leaf]=name
                index=archive.read(members[f'{session["id"]}-billlist.xml'])
            else:
                index=_cached(http,f'{EXPORT}/xml/{session["id"]}-BillList.XML',cache/f'{session["id"]}_house_index.xml',force_fetch)
            records=parse_house_listing(index,session);saved=0
            print(f'MO {session["id"]} lower: {len(records)} XML records',flush=True)
            for i,record in enumerate(records,1):
                if archive:data=archive.read(members[f'{session["id"]}-{record["number"]}.xml'.lower()])
                else:data=_cached(http,record['url'],cache/f'{session["id"]}_{record["number"]}.xml',force_fetch)
                parsed=parse_house_bill(data,record,session,term)
                if parsed is None:carryovers.append([session['id'],record['number']]);continue
                details.append(parsed[0]);histories.extend(parsed[1]);saved+=1
                if verbose or i%50==0 or i==len(records):print(f'MO {session["id"]} lower {i}/{len(records)}: {record["number"]}',flush=True)
            if archive:archive.close()
            counts[session['id']+'_lower']=saved
            url=senate_links[session['year']][session['code'].replace('S','E',1)]
            records=parse_senate_listing(_cached(http,url,cache/f'{session["id"]}_senate_index.html',force_fetch),session)
            counts[session['id']+'_upper']=len(records);print(f'MO {session["id"]} upper: {len(records)} instruments',flush=True)
            for i,record in enumerate(records,1):
                if record.get('withdrawn'):
                    d=[record['number'],session['assembly'],session['year'],session['type'],'','WITHDRAWN',record['sponsor'],record['sponsor_url'],
                       '','','','','','',record['url'],term,'upper',session['id'],'']
                    url=record['url']
                else:
                    html=_cached(http,record['url'],cache/f'{session["id"]}_{record["number"]}.html',force_fetch)
                    d,url=parse_senate_bill(html,record,session,term)
                html=_cached(http,url,cache/f'{session["id"]}_{record["number"]}_actions.html',force_fetch)
                h=parse_senate_actions(html,record,session,term);details.append(d);histories.extend(h)
                if verbose or i%25==0 or i==len(records):print(f'MO {session["id"]} upper {i}/{len(records)} {record["number"]}: {len(h)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending,target in staged:cache_io.replace(pending, target)
        write_manifest(manifest, {'term':term,'details':len(details),'histories':len(histories),'session_chamber_counts':counts,'excluded_administrative_carryovers':carryovers})
    finally:
        for pending,_ in staged:cache_io.unlink(pending, missing_ok=True)
    print(f'MO {term}: saved {len(details):,} instruments and {len(histories):,} histories')
