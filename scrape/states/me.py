"""Maine instruments, committee activity and chamber histories for one term."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io

import csv
from datetime import date, datetime
import json
from pathlib import Path
import re
import time
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL = 'https://legislature.maine.gov'
SEARCH_URL = BASE_URL + '/LawMakerWeb/advancedsearch.asp'
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = ['LD_num', 'paper_num', 'term', 'session', 'title', 'primary_sponsor', 'cosponsors',
                  'reference_comm', 'reference_date', 'status', 'chapter_num', 'bill_url',
                  'chamber_status_url', 'requested_term', 'source_id', 'display_paper_num']
HISTORY_HEADER = ['LD_num', 'paper_num', 'term', 'session', 'chamber', 'action_date',
                  'action_general', 'action_detailed', 'order', 'requested_term', 'source_id']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}', term):
        raise ValueError('ME term must be YYYY_YYYY, for example 2025_2026')
    start, end = map(int, term.split('_'))
    if start % 2 != 1 or end != start + 1 or end > date.today().year:
        raise ValueError('ME requires an odd-starting consecutive two-year term without future years')
    if start < 2003:
        raise ValueError('ME native scraper supports the LawMakerWeb archive from 2003 onward')
    return 121 + (start - 2003) // 2


def select_session(html, term):
    assembly = parse_term(term)
    options = BeautifulSoup(html, 'lxml').select('select[name=LegSession] option[value]')
    selected = [o for o in options if re.fullmatch(str(assembly) + r'(?:st|nd|rd|th) Legislature', o.get_text(strip=True))]
    if len(selected) != 1:
        raise ValueError(f'ME assembly {assembly} is not uniquely listed')
    return {'assembly': assembly, 'id': selected[0]['value']}


def search_form(html, session):
    soup = BeautifulSoup(html, 'lxml')
    form = {n['name']: n.get('value', '') for n in soup.select('input[name]')
            if n.get('type', 'text').lower() not in ('checkbox', 'radio', 'submit', 'button')}
    for select in soup.select('select[name]'):
        choice = select.select_one('option[selected]') or select.find('option')
        if choice:
            form[select['name']] = choice.get('value', choice.get_text())
    form.update({'LegSession': session['id'], 'search': 'search'})
    return form


def parse_listing(html):
    soup = BeautifulSoup(html, 'lxml')
    match = re.search(r'Results (\d+) to (\d+) \(of (\d+)\)', soup.get_text(' ', strip=True))
    if not match:
        raise ValueError('ME missing search result count')
    first, last, total = map(int, match.groups())
    records = []
    for link in soup.select('a[href]'):
        if not re.match(r'summary\.asp\?ID=\d+$', link['href'], re.I):
            continue
        url = urljoin(SEARCH_URL, link['href'])
        identity = parse_qs(urlparse(url).query)['ID'][0]
        number = re.sub(r'\s+', '', link.get_text()).upper()
        if not re.fullmatch(r'[A-Z]+\d+', number):
            raise ValueError(f'ME invalid instrument number: {number}')
        records.append({'number': number, 'url': url, 'id': identity})
    if len(records) != last - first + 1 or not 1 <= first <= last <= total:
        raise ValueError('ME search page count mismatch')
    return records, first, last, total


def _fetch(http, url, form=None):
    response = http.post(url, data=form, timeout=60) if form is not None else http.get(url, timeout=60)
    response.raise_for_status()
    time.sleep(0.75)
    return response


def get_bills(http, search_html, session, cache, force_fetch):
    # Search pagination depends on this ordinary public search-session cookie.
    response = _fetch(http, urljoin(SEARCH_URL, 'doadvancedsearch.asp'), search_form(search_html, session))
    records, seen, total = [], set(), None
    while True:
        group, first, last, reported = parse_listing(response.text)
        if first != len(records) + 1 or (total is not None and total != reported):
            raise ValueError('ME search pagination gap or changing total')
        total = reported
        for record in group:
            if record['id'] in seen:
                raise ValueError('ME repeated source record across search pages')
            seen.add(record['id']); records.append(record)
        local = cache / f'index_{first}.html'
        pending = local.with_suffix('.html.tmp'); cache_io.write_text(pending, response.text, encoding='utf-8'); cache_io.replace(pending, local)
        if last % 250 == 0 or last == total:
            print(f'ME index: {last}/{total} instruments', flush=True)
        if last == total: break
        next_url = urljoin(SEARCH_URL, f'searchresults.asp?StartWith={last + 1}')
        # Fetch each page in the current search session, never reuse pages from
        # an older result ordering while combining them with a refreshed index.
        response = _fetch(http, next_url)
    return records


def _bill_info(soup):
    heading = next((n for n in soup.select('td.sectionheading') if n.get_text(strip=True) == 'Bill Info'), None)
    table = heading.find_parent('table') if heading else None
    cells = table.select('td.sectionbody') if table else []
    if len(cells) < 2:
        raise ValueError('ME missing Bill Info section')
    identity = re.sub(r'\s+', '', cells[0].get_text()).upper()
    match = re.fullmatch(r'(LD\d+)\(([A-Z]+\d+)\)', identity)
    if match:
        return match[1], match[2], cells[1].get_text(' ', strip=True)
    if not re.fullmatch(r'[A-Z]+\d+', identity):
        raise ValueError(f'ME invalid Bill Info identity: {identity}')
    return '', identity, cells[1].get_text(' ', strip=True)


def _canonical(number):
    if not number: return ''
    match = re.fullmatch(r'([A-Z]+)(\d+)', number)
    if not match: raise ValueError(f'ME invalid number: {number}')
    return match[1] + match[2].zfill(4)


def _label_value(soup, label, separator=' '):
    node = next((n for n in soup.select('td') if n.get_text(' ', strip=True).rstrip(':') == label), None)
    value = node.find_next_sibling('td') if node else None
    return value.get_text(separator, strip=True) if value else ''


def display_url(record, session):
    params = {'PID': '1456', 'snum': session['assembly']}
    if record['number'].startswith('LD'):
        params.update({'paper': '', 'paperld': 'l', 'ld': int(record['number'][2:])})
    else:
        params['paper'] = record['number']
    return BASE_URL + '/legis/bills/display_ps.asp?' + urlencode(params)


def parse_bill(payload, record, session, term):
    summary = BeautifulSoup(payload['summary'], 'lxml')
    ld, paper, title = _bill_info(summary)
    if record['number'] not in (ld, paper):
        raise ValueError(f'ME index/detail identity mismatch: {record["id"]}')
    sponsors = BeautifulSoup(payload['sponsors'], 'lxml')
    actions = BeautifulSoup(payload['actions'], 'lxml')
    if _bill_info(sponsors)[:2] != (ld, paper) or _bill_info(actions)[:2] != (ld, paper):
        raise ValueError('ME sponsor/action response identity mismatch')
    primary = _label_value(sponsors, 'Sponsored By', '; ')
    cosponsors = _label_value(sponsors, 'Cosponsored By', '; ')
    reference = _label_value(summary, 'Reference Committee')
    reference_date, status, chapter, session_name = '', '', _label_value(summary, 'Chapter'), ''
    rows = []
    display_paper = ''
    display = BeautifulSoup(payload['display'], 'html5lib')
    if 'Cannot find requested paper' not in display.get_text():
        heading = display.select_one('#siteName')
        if heading is None or not heading.get_text(strip=True).startswith(str(session['assembly'])):
            raise ValueError('ME display page has wrong legislature')
        session_name = heading.get_text(' ', strip=True).split('Legislature, ', 1)[-1]
        paper_input = display.select_one('input[name=paperld]')
        paper_label = paper_input.find_next_sibling('span') if paper_input else None
        display_paper = _canonical(re.sub(r'\s+', '', paper_label.get_text()).upper()) if paper_label else ''
        if display_paper != _canonical(paper):
            ld_input=display.select_one('input[name=paperld][value=l]')
            ld_label=ld_input.find_next_sibling('span') if ld_input else None
            displayed_ld='LD'+re.sub(r'\s+','',ld_label.get_text()) if ld_label else ''
            if not display_paper or displayed_ld!=ld or  ' '.join(title.strip('"“”').split()) not in ' '.join(display.get_text(' ',strip=True).split()):
                raise ValueError('ME display page has wrong paper number and no matching LD/title')
        status = '; '.join(n.get_text(' ', strip=True) for n in display.select('span[class*=tlnk-final]'))
        law = display.find('span', class_='story_heading', string=re.compile('Chaptered Law'))
        if law:
            block = law.find_parent('p').select_one('span.tlnk-dnld')
            if block: chapter = block.get_text(' ', strip=True)
        comm_label = display.find('span', class_='inlineHeading', string='Referred to')
        if comm_label:
            values = comm_label.find_next_siblings('span', class_='inlineData')
            if len(values) >= 2:
                reference = values[0].get_text(' ', strip=True)
                reference_date = datetime.strptime(values[1].get_text(' ', strip=True).replace('.', ''), '%b %d, %Y').date().isoformat()
        for row in display.select('table[name=CDtab] tr'):
            cells = row.find_all('td')
            if not cells: continue
            if len(cells) != 3: raise ValueError('ME malformed committee docket')
            when = datetime.strptime(cells[0].get_text(strip=True), '%b %d, %Y').date().isoformat()
            action, result = (cells[i].get_text(' ', strip=True) for i in (1, 2))
            rows.append(('Joint Committee', when, action, action + (' - ' + result if result else '')))
    heading = next((n for n in actions.select('td.sectionheading') if n.get_text(strip=True) == 'Date'), None)
    table = heading.find_parent('table') if heading else None
    if table:
        for row in table.select('tr'):
            cells = row.find_all('td', recursive=False)
            if not cells or 'sectionheading' in cells[0].get('class', []): continue
            if len(cells) != 3: raise ValueError('ME malformed chamber docket')
            when = datetime.strptime(cells[0].get_text(strip=True), '%m/%d/%Y').date().isoformat()
            action = cells[2].get_text(' ', strip=True)
            if not action: raise ValueError('ME empty chamber action')
            rows.append((cells[1].get_text(' ', strip=True), when, '', action))
    elif not all(_label_value(summary, label) == 'None' for label in ('Last House Action', 'Last Senate Action')):
        raise ValueError('ME missing chamber docket without confirmed empty source actions')
    ld, paper = _canonical(ld), _canonical(paper)
    details = [ld, paper, session['assembly'], session_name, title, primary, cosponsors,
               reference, reference_date, status, chapter, display_url(record, session), record['url'], term, record['id'], display_paper]
    histories = [[ld, paper, session['assembly'], session_name, chamber, when, general, action, order, term, record['id']]
                 for order, (chamber, when, general, action) in enumerate(rows, 1)]
    return details, histories


@scrape_run
def scrape(state, term, verbose=False, force_fetch=False):
    if state.upper() != 'ME': raise ValueError('Maine scraper requires state ME')
    parse_term(term)
    folder = REPO_ROOT / '.data/ME/bill'
    outputs = [folder / f'ME_{name}_{term}.csv' for name in ('Bill_Details', 'Bill_Histories')]
    manifest = folder / f'.ME_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs, manifest]) and not force_fetch:
        print(f'Skipping ME {term}: completed outputs exist (use --force-fetch to refresh)'); return
    cache = folder / '.cache' / term
    cache.mkdir(parents=True, exist_ok=True)
    details, histories, empty = [], [], []
    with make_session() as http:
        search_html = _fetch(http, SEARCH_URL).text
        session = select_session(search_html, term)
        index = cache / 'records.json'
        if cache_io.exists(index) and not force_fetch:
            saved = json.loads(cache_io.read_text(index, encoding='utf-8'))
            if saved['session'] != session or len(saved['records']) != saved['total']:
                raise ValueError('ME cached discovery/session mismatch')
            records = saved['records']
        else:
            records = get_bills(http, search_html, session, cache, force_fetch)
            pending = index.with_suffix('.json.tmp'); cache_io.write_text(pending, json.dumps({'session': session, 'records': records, 'total': len(records)}), encoding='utf-8'); cache_io.replace(pending, index)
        for i, record in enumerate(records, 1):
            local = cache / (record['id'] + '.json')
            if cache_io.exists(local) and not force_fetch:
                payload = json.loads(cache_io.read_text(local, encoding='utf-8'))
            else:
                payload = {'summary': _fetch(http, record['url']).text}
                for key, page in (('sponsors', 'sponsors'), ('actions', 'dockets')):
                    payload[key] = _fetch(http, BASE_URL + f'/LawMakerWeb/{page}.asp?ID={record["id"]}').text
                payload['display'] = _fetch(http, display_url(record, session)).text
            parsed = parse_bill(payload, record, session, term)
            pending = local.with_suffix('.json.tmp'); cache_io.write_text(pending, json.dumps(payload), encoding='utf-8'); cache_io.replace(pending, local)
            details.append(parsed[0]); histories.extend(parsed[1])
            if not parsed[1]: empty.append(record['id'])
            if verbose or i % 25 == 0 or i == len(records):
                print(f'ME {i}/{len(records)} {record["number"]}: {len(parsed[1])} actions', flush=True)
    staged = []
    try:
        for target, header, rows in zip(outputs, (DETAILS_HEADER, HISTORY_HEADER), (details, histories)):
            pending = target.with_suffix('.csv.tmp'); staged.append((pending, target))
            with pending.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle); writer.writerow(header); writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending, target in staged: cache_io.replace(pending, target)

        write_manifest(manifest, {'term': term, 'session': session, 'details': len(details), 'histories': len(histories), 'source_empty_histories': empty})
    finally:
        for pending, _ in staged: cache_io.unlink(pending, missing_ok=True)
    print(f'ME {term}: saved {len(details):,} instruments and {len(histories):,} histories')
