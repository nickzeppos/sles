"""Louisiana bill details and histories using the public ASP.NET search forms."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

import csv
from datetime import date, datetime
import json
from pathlib import Path
import re
import time
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL = 'https://www.legis.la.gov/Legis/'
PREFIX = 'ctl00$ctl00$PageBody$PageContent$'
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = ['bill_number', 'session', 'session_key', 'short_title', 'status', 'author',
                  'all_authors', 'bill_url', 'vote_url', 'term']
HISTORY_HEADER = ['bill_number', 'session', 'session_key', 'chamber', 'action_date', 'journal_page', 'action', 'order', 'term']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}', term):
        raise ValueError('LA term must be YYYY_YYYY, for example 2025_2026')
    start, end = map(int, term.split('_'))
    if start % 2 != 1 or end != start + 1 or end > date.today().year:
        raise ValueError('LA requires an odd-starting consecutive two-year term without future years')
    return start, end


def select_sessions(html, term):
    years = parse_term(term)
    records = []
    for option in BeautifulSoup(html, 'lxml').select('select[id$=ddlSessions] option[value]'):
        label = option.get_text(' ', strip=True)
        if label[:4].isdigit() and int(label[:4]) in years:
            key = option['value']
            if not re.fullmatch(r'\d{2}\d?(?:RS|ES|OS|VS)', key) or key[:2] != label[2:4]:
                raise ValueError(f'LA unrecognized session ID: {key}')
            records.append({'id': key, 'year': int(label[:4]), 'label': label})
    if not all(f'{y % 100:02d}RS' in {r['id'] for r in records} for y in years):
        raise ValueError(f'LA missing regular session for {term}')
    if len(records) != len({r['id'] for r in records}):
        raise ValueError('LA duplicate sessions')
    return sorted(records, key=lambda r: (r['year'], r['id']))


def _form(html):
    soup = BeautifulSoup(html, 'lxml')
    values = {x['name']: x.get('value', '') for x in soup.select('input[name]')
              if x.get('type', 'text') not in ('submit', 'button', 'checkbox', 'radio')}
    for select in soup.select('select[name]'):
        chosen = select.select_one('option[selected]') or select.find('option')
        if chosen:
            values[select['name']] = chosen.get('value', chosen.get_text())
    values['__EVENTTARGET'] = ''; values['__EVENTARGUMENT'] = ''
    return values


def _fetch(http, url, form=None):
    response = http.post(url, data=form, timeout=60) if form is not None else http.get(url, timeout=60)
    response.raise_for_status()
    time.sleep(1)
    return response


def parse_results(html, url, session, kind):
    soup = BeautifulSoup(html, 'lxml')
    label = soup.select_one('[id$=LabelSession]')
    if label is None or session['label'] not in label.get_text(' ', strip=True):
        raise ValueError('LA search results have wrong session')
    single = soup.select_one('[id$=LabelBillID]')
    if single:
        number = single.get_text(strip=True)
        if not re.fullmatch(re.escape(kind) + r'\d+', number) or not urlparse(url).path.endswith('/BillInfo.aspx'):
            raise ValueError('LA single-result identity mismatch')
        return [(number, url)], 1, None
    marker = soup.select_one('[id$=LabelTotalInstruments]')
    match = re.fullmatch(r'There are (\d+) Instruments in this List', marker.get_text(' ', strip=True) if marker else '')
    if not match:
        raise ValueError('LA missing search result count')
    total = int(match[1])
    records = []
    for link in soup.select('a[href*="BillInfo.aspx"]'):
        number = link.get_text(strip=True)
        if number == 'more...':
            continue
        if not re.fullmatch(re.escape(kind) + r'\d+', number):
            raise ValueError(f'LA unexpected instrument: {number}')
        target = urljoin(BASE_URL, link['href'])
        identity = parse_qs(urlparse(target).query).get('i')
        if not identity or len(identity) != 1 or not identity[0].isdigit():
            raise ValueError('LA missing bill identity')
        records.append((number, BASE_URL + 'BillInfo.aspx?i=' + identity[0]))
    if total and not records:
        raise ValueError('LA nonempty search with no bill links')
    next_link = next((a for a in soup.select('a') if a.get_text(strip=True) == '>'), None)
    target = None
    if next_link is not None and not next_link.has_attr('disabled'):
        match = re.fullmatch(r"javascript:__doPostBack\('([^']+)',''\)", next_link.get('href', ''))
        if not match:
            raise ValueError('LA unrecognized next-page control')
        target = match[1]
    return records, total, target


def get_bills(http, session, cache, force_fetch):
    url = BASE_URL + 'BillSearch.aspx?sid=' + session['id']
    initial = _fetch(http, url)
    soup = BeautifulSoup(initial.text, 'lxml')
    if 'There are no bills associated with' in soup.get_text():
        return []
    options = soup.select('select[id$=ddlInstTypes] option[value]') or soup.select('select[id$=ddlInstTypes2] option[value]')
    if not options:
        raise ValueError(f'LA missing instrument types: {session["id"]}')
    kinds = [(o.get_text(strip=True), o['value']) for o in options if o.get_text(strip=True) != 'ACT']
    records = []
    for kind, value in kinds:
        local = cache / f'{session["id"]}_{kind}_index.json'
        if local.exists() and not force_fetch:
            payload = json.loads(local.read_text(encoding='utf-8'))
            group, total = payload['records'], payload['total']
        else:
            initial = _fetch(http, url)
            if BeautifulSoup(initial.text, 'lxml').select_one('select[id$=ddlInstTypes2]'):
                expanded = initial
            else:
                form = _form(initial.text); form['__EVENTTARGET'] = PREFIX + 'btnHeadRange'
                expanded = _fetch(http, url, form)
            form = _form(expanded.text)
            form.update({PREFIX + 'ddlInstTypes2': value, '__EVENTTARGET': PREFIX + 'ddlInstTypes2'})
            chosen = _fetch(http, url, form)
            soup = BeautifulSoup(chosen.text, 'lxml')
            first, last = soup.select_one('input[id$=tbBillNumStart]'), soup.select_one('input[id$=tbBillNumStop]')
            button = soup.select_one('input[id$=btnSearchByInstRange]')
            if first is None or last is None or button is None:
                raise ValueError('LA missing range controls')
            destination = re.search(r'"(BillSearchList\.aspx\?srch=r)"', button.get('onclick', ''))
            if not destination:
                raise ValueError('LA missing search form destination')
            form = _form(chosen.text)
            form.update({PREFIX + 'ddlInstTypes2': value, first['name']: first['value'], last['name']: last['value'], button['name']: button['value']})
            response = _fetch(http, urljoin(url, destination[1]), form)
            group, seen, total = [], set(), None
            while True:
                items, reported_total, next_target = parse_results(response.text, response.url, session, kind)
                if total is not None and total != reported_total:
                    raise ValueError('LA result total changed during pagination')
                total = reported_total
                for item in items:
                    if item[0] in seen:
                        raise ValueError('LA repeated bill across result pages')
                    seen.add(item[0]); group.append(item)
                if not next_target:
                    break
                form = _form(response.text); form['__EVENTTARGET'] = next_target
                response = _fetch(http, response.url, form)
            if len(group) != total:
                raise ValueError(f'LA incomplete {kind} search: {len(group)}/{total}')
            pending = local.with_suffix('.json.tmp'); pending.write_text(json.dumps({'records': group, 'total': total}), encoding='utf-8'); pending.replace(local)
        if len(group) != total:
            raise ValueError('LA cached index count mismatch')
        records.extend(group)
        print(f'LA {session["id"]} {kind}: {total} instruments', flush=True)
    if len(records) != len({r[0] for r in records}) or len(records) != len({r[1] for r in records}):
        raise ValueError('LA duplicate bill identity')
    return records


def parse_bill(html, authors_html, record, session, term):
    number, url = record
    soup = BeautifulSoup(html, 'lxml')
    def field(suffix):
        node = soup.select_one('[id$=' + suffix + ']')
        if node is None:
            raise ValueError(f'LA missing {suffix}: {url}')
        return node.get_text(' ', strip=True)
    if field('LabelBillID') != number or field('LabelSession') != session['label']:
        raise ValueError(f'LA bill/session identity mismatch: {url}')
    author, title, status = field('LinkAuthor'), field('LabelShortTitle'), field('LabelCurrentStatus')
    if not author or not title:
        raise ValueError(f'LA missing author/title: {url}')
    authors = BeautifulSoup(authors_html, 'lxml')
    if authors.title is None or authors.title.get_text(strip=True) != 'Authors - ' + number:
        raise ValueError('LA author response has wrong bill identity')
    table = authors.find('table')
    if table is None:
        raise ValueError('LA missing author table')
    all_authors = '; '.join(a.get_text(' ', strip=True) for a in table.select('a'))
    vote_link = soup.find('a', href=re.compile(r'BillDocs.+t=votes'))
    votes_url = url.replace('BillInfo', 'BillDocs') + '&t=votes' if vote_link else ''
    details = [number, session['year'], session['id'], title, status.removeprefix('Current Status:').strip(),
               author, all_authors, url, votes_url, term]
    heading = soup.find('th', string=re.compile(r'^\s*Chamber\s*$'))
    table = heading.find_parent('table') if heading else None
    if table is None:
        raise ValueError(f'LA missing history: {url}')
    histories = []
    for row in table.select('tr'):
        cells = row.find_all('td', recursive=False)
        if not cells: continue
        if len(cells) < 4:
            raise ValueError('LA malformed history row')
        when = datetime.strptime(cells[0].get_text(strip=True) + '/' + str(session['year']), '%m/%d/%Y').date().isoformat()
        chamber, journal, action = (cells[i].get_text(' ', strip=True) for i in (1, 2, 3))
        if not action:
            raise ValueError('LA empty history action')
        histories.append([number, session['year'], session['id'], chamber, when, journal, action, 0, term])
    if not histories:
        raise ValueError('LA empty history')
    for i, row in enumerate(histories): row[7] = len(histories) - i
    return details, histories


@scrape_run
def scrape(state, term, verbose=False, force_fetch=False):
    if state.upper() != 'LA':
        raise ValueError('Louisiana scraper requires state LA')
    parse_term(term)
    folder = REPO_ROOT / '.data/LA/bill'
    outputs = [folder / f'LA_{name}_{term}.csv' for name in ('Bill_Details', 'Bill_Histories')]
    manifest = folder / f'.LA_scrape_{term}.json'
    if all(p.exists() for p in [*outputs, manifest]) and not force_fetch:
        print(f'Skipping LA {term}: completed outputs exist (use --force-fetch to refresh)')
        return
    cache = folder / '.cache' / term
    cache.mkdir(parents=True, exist_ok=True)
    details, histories, counts = [], [], {}
    with make_session() as http:
        sessions = select_sessions(_fetch(http, BASE_URL + 'SessionInfo/SessionInfo.aspx').text, term)
        for session in sessions:
            records = get_bills(http, session, cache, force_fetch)
            counts[session['id']] = len(records)
            for i, record in enumerate(records, 1):
                local = cache / f'{session["id"]}_{record[0]}.json'
                if local.exists() and not force_fetch:
                    payload = json.loads(local.read_text(encoding='utf-8'))
                else:
                    payload = {'html': _fetch(http, record[1]).text,
                               'authors_html': _fetch(http, record[1].replace('BillInfo', 'BillDocs') + '&t=authors').text}
                parsed = parse_bill(payload['html'], payload['authors_html'], record, session, term)
                pending = local.with_suffix('.json.tmp'); pending.write_text(json.dumps(payload), encoding='utf-8'); pending.replace(local)
                details.append(parsed[0]); histories.extend(parsed[1])
                if verbose or i % 25 == 0 or i == len(records):
                    print(f'LA {session["id"]} {i}/{len(records)} {record[0]}: {len(parsed[1])} actions', flush=True)
    staged = []
    try:
        for target, header, rows in zip(outputs, (DETAILS_HEADER, HISTORY_HEADER), (details, histories)):
            pending = target.with_suffix('.csv.tmp'); staged.append((pending, target))
            with pending.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle); writer.writerow(header); writer.writerows(rows)
        manifest.unlink(missing_ok=True)
        for pending, target in staged: pending.replace(target)

        write_manifest(manifest, {'term': term, 'details': len(details), 'histories': len(histories), 'session_counts': counts})
    finally:
        for pending, _ in staged: pending.unlink(missing_ok=True)
    print(f'LA {term}: saved {len(details):,} instruments and {len(histories):,} histories')
