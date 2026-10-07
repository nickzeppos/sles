"""Montana's public Bill Explorer API, including unintroduced drafting requests."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

import csv
from datetime import date, datetime
import json
from pathlib import Path
import re
import time

from scrape.http import make_session

API = 'https://bearbeta.legmt.gov'
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = ['bill_number', 'lc_number', 'session_year', 'session_type', 'draft',
                  'sponsor', 'requester', 'drafter', 'title', 'subjects', 'bill_url',
                  'term', 'source_session', 'source_id', 'status', 'carrier', 'by_request_of', 'requester_type', 'requester_id']
HISTORY_HEADER = ['bill_number', 'lc_number', 'session_year', 'session_type', 'chamber',
                  'action_date', 'action', 'order', 'term', 'source_session', 'source_id',
                  'source_action_id', 'timestamp', 'result', 'vote_id']
LOOKUPS = {
    'LEGISLATOR': 'legislators/v1/legislators',
    'STAFF': 'legislators/v1/staffMembers',
    'NON_STANDING_COMMITTEE': 'committees/v1/nonStandingCommittees',
    'STANDING_COMMITTEE': 'committees/v1/standingCommittees',
    'ORGANIZATION': 'legislators/v1/organizations',
    'AGENCY': 'legislators/v1/organizations',
    'AGENCY_ON_BEHALF_OF_NON_STANDING_COMMITTEE': 'legislators/v1/organizations',
    'AGENCY_ON_BEHALF_OF_STANDING_COMMITTEE': 'legislators/v1/organizations',
    'ELECTED_OFFICIAL_POSITION': 'legislators/v1/electedOfficialPositions',
}


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}', term):
        raise ValueError('MT term must be YYYY_YYYY, for example 2025_2026')
    start, end = map(int, term.split('_'))
    if start % 2 != 1 or end != start + 1 or end > date.today().year:
        raise ValueError('MT requires an odd-starting consecutive two-year term without future years')
    if start < 2025:
        raise ValueError('MT native scraper supports the 2025+ API; earlier terms use a different archive schema')
    return start, end


def select_sessions(current, archive, term):
    start, end = parse_term(term)
    if any(start <= s['sessionYear'] <= end for s in archive):
        raise ValueError('MT requested term includes archived sessions; archive parser required')
    selected = [dict(s) for s in current if start <= date.fromisoformat(s['startDate'][:10]).year <= end]
    regular = [s for s in selected if s['type'] == 'REGULAR']
    if len(regular) != 1 or int(regular[0]['startDate'][:4]) != start:
        raise ValueError('MT source does not list exactly one regular session for the term')
    legislature = regular[0]['legislature']
    if int(legislature['startDate'][:4]) != start or int(legislature['endDate'][:4]) != end:
        raise ValueError('MT legislature year boundaries do not match requested term')
    specials = {}
    for s in sorted(selected, key=lambda s: (s['startDate'], s['id'])):
        if s['legislature']['id'] != legislature['id']:
            raise ValueError('MT session belongs to a different legislature')
        s['year'] = int(s['startDate'][:4])
        if s['type'] == 'REGULAR': s['kind'] = 'RS'
        elif s['type'] == 'SPECIAL':
            specials[s['year']] = specials.get(s['year'], 0) + 1
            s['kind'] = f'SS{specials[s["year"]]}'
        else: raise ValueError(f'MT unknown session type: {s["type"]}')
    if len({s['id'] for s in selected}) != len(selected): raise ValueError('MT duplicate sessions')
    return selected


def parse_page(data, session, offset, limit):
    rows = data['content']; total = data['totalElements']
    if data['pageable']['offset'] != offset or data['numberOfElements'] != len(rows):
        raise ValueError('MT pagination position/count mismatch')
    if len(rows) != min(limit, total-offset) or data['last'] != (offset+len(rows) == total):
        raise ValueError('MT truncated bill page')
    for b in rows:
        if b['sessionId'] != session['id'] or b['draft']['sessionId'] != session['id']:
            raise ValueError('MT bill outside requested session')
        if not re.fullmatch(r'LC\d+', b['draft']['draftNumber']): raise ValueError('MT invalid draft identifier')
    return rows, total


def lookup_name(data, identifier):
    if data['id'] != identifier: raise ValueError('MT name lookup identity mismatch')
    if data.get('committeeDetails'):
        name = data['committeeDetails']['committeeCode']['name']
    elif data.get('lastName'):
        name = ' '.join(filter(None, (data.get('firstName'), data.get('lastName'))))
    else:
        name = data.get('name') or data.get('description') or data.get('title')
    if not name: raise ValueError(f'MT unnamed lookup {identifier}')
    return name


def parse_bill(bill, session, term, resolve):
    draft = bill['draft']; sid = session['id']
    if bill['sessionId'] != sid or draft['sessionId'] != sid: raise ValueError('MT bill/session mismatch')
    lc = draft['draftNumber']; number = bill.get('billNumber')
    if number:
        code = bill['billType']['code']
        if code not in ('HB','SB','HJ','SJ','HR','SR'): raise ValueError(f'MT unknown bill type {code}')
        number = f'{code} {number}'
    else: number = ''
    title = draft['shortTitle']
    if not title: raise ValueError(f'MT missing title for {lc}')
    actions = draft['billStatuses']
    if not actions: raise ValueError(f'MT empty history for {lc}')
    actions = sorted(actions, key=lambda a: (a['timeStamp'], a['id']), reverse=True)
    if len({a['id'] for a in actions}) != len(actions): raise ValueError('MT duplicate action IDs')
    status = actions[0]['billStatusCode']['name']
    detail = [number, lc, session['year'], session['kind'], int(not number),
              resolve('LEGISLATOR',bill.get('sponsorId')), resolve(draft.get('requesterType'),draft.get('requesterId')),
              resolve('STAFF',bill.get('drafterStaffMemberId')), title,
              '; '.join(s['subjectCode']['description'] for s in draft['subjects']),
              f'https://bills.legmt.gov/laws/bill/{sid}/{lc}?open_tab=sum', term, sid, bill['id'], status,
              resolve('LEGISLATOR',bill.get('carrierId')),
              '; '.join(resolve(r['billRequestType'],r['byRequestOfId']) for r in draft.get('byRequestOfs',[])), draft.get('requesterType') or '', draft.get('requesterId') or '']
    history = []
    for i, a in enumerate(actions):
        timestamp = datetime.fromisoformat(a['timeStamp'])
        action = a['billStatusCode']['name']
        if not action: raise ValueError('MT blank action')
        committee = resolve('STANDING_COMMITTEE', a.get('standingCommitteeId'))
        if committee: action += f' ~ [{committee}]'
        # The API marks drafting actions HOUSE even on Senate drafts; use the
        # published action prefix, preserving LC as chamberless drafting work.
        chamber = re.match(r'^\(([HS])\)', action)
        vote = a.get('vote') or {}
        if vote and vote['sessionId'] != sid: raise ValueError('MT vote outside requested session')
        history.append([number, lc, session['year'],session['kind'], chamber[1] if chamber else '',
                        timestamp.date().isoformat(),action,len(actions)-i,term,sid,bill['id'],a['id'],
                        a['timeStamp'],a.get('result') or '',vote.get('id','')])
    return detail, history


def _cached(http, cache, name, route, force, payload=None):
    path = cache / (name+'.json')
    if path.exists() and not force: return json.loads(path.read_text())
    response = (http.get(API+'/'+route,timeout=90) if payload is None else
                http.post(API+'/'+route,json=payload,timeout=90))
    response.raise_for_status(); data = response.json(); time.sleep(1)
    pending=path.with_suffix('.json.tmp'); pending.write_text(json.dumps(data),encoding='utf-8');pending.replace(path)
    return data


@scrape_run
def scrape(state, term, verbose=False, force_fetch=False):
    if state.upper() != 'MT': raise ValueError('Montana scraper requires state MT')
    parse_term(term)
    folder=REPO_ROOT/'.data/MT/bill'; cache=folder/'.cache'/term
    outputs=[folder/f'MT_{name}_{term}.csv' for name in ('Bill_Details','Bill_Histories')]
    manifest=folder/f'.MT_scrape_{term}.json'
    if all(p.exists() for p in [*outputs,manifest]) and not force_fetch:
        print(f'Skipping MT {term}: completed outputs exist (use --force-fetch to refresh)'); return
    cache.mkdir(parents=True,exist_ok=True)
    details, histories, counts = [], [], {}
    with make_session() as http:
        current=_cached(http,cache,'sessions','legislators/v1/sessions',force_fetch)
        archive=_cached(http,cache,'archive_sessions','archive/v1/sessions',force_fetch)
        sessions=select_sessions(current,archive,term)
        names={}; missing_names=[]
        def resolve(kind, identifier):
            if identifier is None: return ''
            key=(kind,identifier)
            if key not in names:
                if kind not in LOOKUPS: raise ValueError(f'MT unknown requester type {kind}')
                if kind in ('STANDING_COMMITTEE','NON_STANDING_COMMITTEE'):
                    filter_name='standingCommitteeIds' if kind=='STANDING_COMMITTEE' else 'nonStandingCommitteeIds'
                    result=_cached(http,cache,f'lookup_search_{kind}_{identifier}',
                                   LOOKUPS[kind]+'/search?limit=500&offset=0',force_fetch,{filter_name:[identifier]})
                    if result['totalElements']==0 and result['content']==[]:
                        names[key]=''
                        missing_names.append({'kind':kind,'id':identifier,'source_result':'empty committee search'})
                        return ''
                    if result['totalElements']!=1 or len(result['content'])!=1:raise ValueError('MT ambiguous committee lookup')
                    data=result['content'][0]
                else:
                    data=_cached(http,cache,f'lookup_{kind}_{identifier}',f'{LOOKUPS[kind]}/{identifier}',force_fetch)
                names[key]=lookup_name(data,identifier)
            return names[key]
        for session in sessions:
            sid=session['id']; records=[]; total=None; limit=100; ids=set(); lcs=set()
            while total is None or len(records)<total:
                offset=len(records)
                route=('bills/v1/bills/search?includeCounts=false&sort=billType.sortOrder,desc'
                       f'&sort=billNumber,asc&sort=draft.draftNumber,asc&limit={limit}&offset={offset // limit}')
                data=_cached(http,cache,f'{sid}_bills_{offset}',route,force_fetch,{'sessionIds':[sid]})
                group,count=parse_page(data,session,offset,limit)
                if total is not None and count != total: raise ValueError('MT total changed while paginating')
                for bill in group:
                    if bill['id'] in ids or bill['draft']['draftNumber'] in lcs: raise ValueError('MT duplicate bill/draft')
                    ids.add(bill['id']);lcs.add(bill['draft']['draftNumber'])
                records.extend(group);total=count
                print(f'MT {session["year"]}{session["kind"]} index: {len(records)}/{total}',flush=True)
            counts[str(sid)]=len(records)
            for i,bill in enumerate(records,1):
                detail,history=parse_bill(bill,session,term,resolve)
                details.append(detail);histories.extend(history)
                if verbose or i%100==0 or i==len(records): print(f'MT {i}/{len(records)} {detail[0] or detail[1]}: {len(history)} actions',flush=True)
    staged=[]
    try:
        for target,header,rows in zip(outputs,(DETAILS_HEADER,HISTORY_HEADER),(details,histories)):
            pending=target.with_suffix('.csv.tmp');staged.append((pending,target))
            with pending.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(header);writer.writerows(rows)
        manifest.unlink(missing_ok=True)
        for pending,target in staged: pending.replace(target)

        write_manifest(manifest, {'term':term,'sessions':sessions,'counts':counts,'details':len(details),
                                      'histories':len(histories),'unintroduced_drafts':sum(not d[0] for d in details),'missing_name_lookups':missing_names,
                                      })
    finally:
        for pending,_ in staged: pending.unlink(missing_ok=True)
    print(f'MT {term}: wrote {len(details)} details and {len(histories)} histories',flush=True)
