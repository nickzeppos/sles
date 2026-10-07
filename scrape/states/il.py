"""Illinois bill status from ILGA's repository designated for automated access."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io

import csv
from datetime import date, datetime
from pathlib import Path
import re
import time
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL = 'https://ftp.ilga.gov'
REPO_ROOT = Path(__file__).resolve().parents[2]
KINDS = {'HB': 'HB', 'SB': 'SB', 'HR': 'HR', 'SR': 'SR', 'HJ': 'HJR',
         'SJ': 'SJR', 'HC': 'HJRCA', 'SC': 'SJRCA', 'JS': 'JSR'}
DETAILS_HEADER = ['bill_number', 'session_num', 'session', 'title', 'summary',
                  'house_sponsors', 'senate_sponsors', 'bill_url', 'vote_url',
                  'witness_slip_url', 'term', 'session_id']
HISTORY_HEADER = ['bill_number', 'session_num', 'session', 'action_date',
                  'action_chamber', 'action', 'order', 'term', 'session_id']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}', term):
        raise ValueError('IL term must be YYYY_YYYY, for example 2025_2026')
    start, end = map(int, term.split('_'))
    if start % 2 != 1 or end != start + 1 or end > date.today().year:
        raise ValueError('IL requires an odd-starting consecutive two-year term without future years')
    return (start - 1997) // 2 + 90


def parse_listing(html, assembly):
    base = f'{BASE_URL}/Legislation/{assembly}/BillStatus/XML/'
    records = {}
    for link in BeautifulSoup(html, 'lxml').select('a[href]'):
        url = urljoin(base, link['href'])
        filename = urlparse(url).path.rsplit('/', 1)[-1]
        match = re.fullmatch(r'(\d{3})(\d{2})([A-Z]{2})(\d{4})\.xml', filename, re.I)
        if not match:
            continue
        ga, session, code, number = match.groups()
        code = code.upper()
        if int(ga) != assembly or not url.startswith(base):
            raise ValueError(f'IL wrong assembly/source in index: {url}')
        if code in ('AM', 'EO'):
            continue
        if code not in KINDS:
            raise ValueError(f'IL unknown measure type: {code}')
        if filename in records:
            raise ValueError(f'IL duplicate file: {filename}')
        records[filename] = {'filename': filename, 'url': url, 'assembly': assembly,
                             'session': session, 'number': f'{KINDS[code]} {int(number)}'}
    if not records:
        raise ValueError(f'IL no bill status files for assembly {assembly}')
    return [records[key] for key in sorted(records)]


def _text(element):
    return ' '.join(''.join(element.itertext()).split()) if element is not None else ''


def parse_bill(content, record, term):
    # Parse bytes so the XML declaration controls UTF-8 versus ISO-8859-1.
    # The official export sometimes embeds XML-forbidden control bytes in prose.
    # Replace those separators with spaces; retain the untouched bytes in cache.
    content = re.sub(rb'[\x00-\x08\x0b\x0c\x0e-\x1f]', b' ', content)
    root = ET.fromstring(content)
    title = _text(root.find('title'))
    if title != f'Illinois General Assembly - Bill Status for {record["number"]}':
        raise ValueError(f'IL bill identity mismatch: {record["filename"]}')
    short = _text(root.find('shortdesc'))
    synopsis = _text(root.find('synopsis/SynopsisText'))
    if root.find('shortdesc') is None:
        raise ValueError(f'IL missing description field: {record["filename"]}')
    sponsors = {'House': [], 'Senate': []}
    chamber = None
    sponsor_node = root.find('sponsor')
    if sponsor_node is None:
        raise ValueError(f'IL missing sponsor section: {record["filename"]}')
    for node in sponsor_node:
        if node.tag.startswith('sponsorhead'):
            label = _text(node)
            if label not in ('House Sponsors', 'Senate Sponsors'):
                raise ValueError(f'IL unknown sponsor heading: {label}')
            chamber = label.split()[0]
        elif node.tag in ('sponsors', 'altsponsors'):
            if chamber is None:
                raise ValueError('IL sponsor names precede chamber heading')
            sponsors[chamber].append(_text(node))
    assembly, session = record['assembly'], record['session']
    session_name = 'Regular Session' if session == '00' else f'Special Session {int(session)}'
    session_id = f'{assembly}-{session}'
    url = record['url'].replace('/XML/', '/HTML/').removesuffix('.xml') + '.html'
    # The repository has no vote/witness-slip permalinks; retain empty fields.
    details = [record['number'], assembly, session_name, short, synopsis,
               '; '.join(sponsors['House']), '; '.join(sponsors['Senate']), url,
               '', '', term, session_id]
    actions = root.find('actions')
    nodes = list(actions) if actions is not None else []
    if not nodes or len(nodes) % 3:
        raise ValueError(f'IL missing/malformed history: {record["filename"]}')
    histories = []
    for i in range(0, len(nodes), 3):
        group = nodes[i:i + 3]
        if [n.tag for n in group] != ['statusdate', 'chamber', 'action']:
            raise ValueError(f'IL unexpected history layout: {record["filename"]}')
        when = datetime.strptime(_text(group[0]), '%m/%d/%Y').date().isoformat()
        action_chamber, action = _text(group[1]), _text(group[2])
        if not action:
            raise ValueError(f'IL empty action: {record["filename"]}')
        histories.append([record['number'], assembly, session_name, when,
                          action_chamber, action, len(histories) + 1, term, session_id])
    return details, histories


@scrape_run
def scrape(state, term, verbose=False, force_fetch=False):
    if state.upper() != 'IL':
        raise ValueError('Illinois scraper requires state IL')
    assembly = parse_term(term)
    folder = REPO_ROOT / '.data/IL/bill'
    outputs = [folder / f'IL_{name}_{term}.csv' for name in ('Bill_Details', 'Bill_Histories')]
    manifest = folder / f'.IL_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs, manifest]) and not force_fetch:
        print(f'Skipping IL {term}: completed outputs exist (use --force-fetch to refresh)')
        return
    cache = folder / '.cache' / term
    cache.mkdir(parents=True, exist_ok=True)
    details, histories = [], []
    missing_text = []
    with make_session() as http:
        index = http.get(f'{BASE_URL}/Legislation/{assembly}/BillStatus/XML/', timeout=60)
        index.raise_for_status()
        records = parse_listing(index.text, assembly)
        cache_io.write_text(cache / 'index.html', index.text, encoding='utf-8')
        print(f'IL assembly {assembly}: {len(records)} bills/resolutions', flush=True)
        for i, record in enumerate(records, 1):
            local = cache / record['filename']
            if cache_io.exists(local) and not force_fetch:
                content = cache_io.read_bytes(local)
            else:
                response = http.get(record['url'], timeout=60)
                response.raise_for_status()
                content = response.content
                time.sleep(1)
            parsed = parse_bill(content, record, term)
            if not parsed[0][3] or not parsed[0][4]:
                missing_text.append({'bill':record['number'],'session':record['session'],'url':record['url'],
                                     'missing_fields':[name for name,value in zip(('title','summary'),parsed[0][3:5]) if not value]})
            pending = local.with_suffix('.xml.tmp'); cache_io.write_bytes(pending, content); cache_io.replace(pending, local)
            details.append(parsed[0]); histories.extend(parsed[1])
            if verbose or i % 25 == 0 or i == len(records):
                print(f'IL {i}/{len(records)} {record["number"]}: {len(parsed[1])} actions', flush=True)
    staged = []
    try:
        for target, header, rows in zip(outputs, (DETAILS_HEADER, HISTORY_HEADER), (details, histories)):
            pending = target.with_suffix('.csv.tmp'); staged.append((pending, target))
            with pending.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle); writer.writerow(header); writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending, target in staged:
            cache_io.replace(pending, target)

        write_manifest(manifest, {'term': term, 'assembly': assembly,
                                      'details': len(details), 'histories': len(histories),
                                      'source_missing_text': missing_text})
    finally:
        for pending, _ in staged:
            cache_io.unlink(pending, missing_ok=True)
    print(f'IL {term}: saved {len(details):,} bills/resolutions and {len(histories):,} histories')
