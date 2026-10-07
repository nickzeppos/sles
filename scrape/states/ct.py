"""Connecticut's official website bill histories and introduced bill PDFs.

A requested biennium is two explicit session years. Every status page must
identify its requested year. The legacy FTP copy is stale; use HTTPS indexes.
"""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io

import csv
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
import re
import time
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
from scrape.http import make_session as _make_session

WEB_URL = 'https://cga.ct.gov'
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = ['bill_num', 'session', 'bill_type', 'primary_sponsors', 'title', 'purpose',
                  'cosponsors', 'introduced_by', 'bill_url', 'proposed_bill_pdf_url', 'term']
HISTORY_HEADER = ['bill_num', 'session', 'action_date', 'action', 'order', 'bill_url', 'term']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}', term):
        raise ValueError('CT term must be YYYY_YYYY, for example 2025_2026')
    start, end = map(int, term.split('_'))
    if start % 2 != 1 or end != start + 1:
        raise ValueError('CT term must start in an odd year and end the next year')
    if end > date.today().year:
        raise ValueError(f'CT term {term} includes a future year')
    return start, end


def parse_listing(html, year, kind):
    soup = BeautifulSoup(html, 'lxml')
    heading = soup.select_one('h3.text-center')
    if heading is None or f'In Session Year {year}' not in heading.get_text(' ', strip=True):
        raise ValueError(f'CT index does not identify requested year {year}')
    records = []
    for row in soup.select('table.footable tbody tr'):
        link = row.select_one('a[href]')
        if link is None:
            raise ValueError('CT malformed index row')
        params = parse_qs(urlparse(link['href']).query)
        match = re.fullmatch(r'([HS][A-Z])(\d+)', link.get_text(strip=True))
        if not match or match[1] != kind or params.get('which_year') != [str(year)]:
            raise ValueError('CT wrong bill type/year in index')
        number = match[1] + '-' + str(int(match[2])).zfill(4)
        records.append((number, f'/{year}/cbs/{kind[0]}/{number}.htm'))
    if not records or len({r[0] for r in records}) != len(records):
        raise ValueError(f'Empty/duplicate CT {year} {kind} bill index')
    return sorted(records)


def parse_status(raw, number, year, term, url):
    soup = BeautifulSoup(raw, 'lxml')
    text = soup.get_text('\n', strip=True)
    actual_year = re.search(r'Session Year\s+(\d{4})', text)
    if not actual_year or int(actual_year[1]) != year:
        raise ValueError(f'CT requested {year} {number}, source says Session Year {actual_year[1] if actual_year else "missing"}; refusing to relabel it')
    subject = soup.find('meta', attrs={'name': re.compile('^subject$', re.I)})
    prefix, digits = number.split('-')
    if subject and subject.get('content', '').upper() != prefix + str(int(digits)):
        raise ValueError(f'CT bill identity mismatch: {number}')
    labels = ['Introducer(s):', 'Title:', 'Statement of Purpose:', 'Bill History:', 'Co-sponsor(s):']
    positions = []
    for label in labels:
        position = text.find(label)
        if position < 0:
            raise ValueError(f'CT missing {label} for {number}')
        positions.append(position)
    if positions != sorted(positions):
        raise ValueError(f'CT unexpected field order: {number}')
    sections = [text[positions[i] + len(labels[i]):positions[i+1] if i+1 < len(labels) else len(text)].strip() for i in range(len(labels))]
    sponsors, title, purpose, history, cosponsors = sections
    histories = []
    for line in history.splitlines():
        line = line.strip()
        if not line:
            continue
        match = re.match(r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})\s+(.+)', line)
        if not match:
            raise ValueError(f'CT unrecognized history row for {number}: {line[:120]}')
        when = match[1].replace('-', '/')
        fmt = '%m/%d/%Y' if len(when.split('/')[-1]) == 4 else '%m/%d/%y'
        histories.append([number, year, datetime.strptime(when, fmt).date().isoformat(), match[2], len(histories)+1, url, term])
    if not title:
        raise ValueError(f'CT missing title: {number}')
    pdf_path = f'/{year}/TOB/{prefix[0].lower()}/pdf/{year}{prefix}-{digits.zfill(5)}-R00-{prefix[0]}B.PDF'
    fields = [number, year, '', '; '.join(sponsors.splitlines()), ' '.join(title.split()),
              ' '.join(purpose.split()), '; '.join(cosponsors.splitlines()), '', url,
              f'https://www.cga.ct.gov{pdf_path}', term]
    return fields, histories, pdf_path


def parse_pdf(raw):
    reader = PdfReader(BytesIO(raw))
    text = '\n'.join(page.extract_text() or '' for page in reader.pages)
    if not text.strip():
        raise ValueError('CT introduced bill PDF has no extractable text')
    kind = 'Proposed Bill' if re.search(r'Proposed (Bill|House|Senate)', text) else 'Raised Bill' if re.search(r'Raised (Bill|House|Senate)', text) else 'Unknown'
    match = re.search(r'Introduced by:\s*\n(.*?)(?:\n\s*\n|\n\s*AN ACT|\n\s*RESOLUTION)', text, re.S)
    introduced = '; '.join(re.sub(r'\s+', ' ', line).strip() for line in match[1].splitlines() if line.strip()) if match else ''
    return kind, introduced


def _retrieve(http, path):
    response = http.get(WEB_URL + path, timeout=60)
    response.raise_for_status()
    time.sleep(0.75)
    return response.content


@scrape_run
def scrape(state, term, verbose=False, force_fetch=False):
    if state.upper() != 'CT':
        raise ValueError('Connecticut scraper requires state CT')
    years = parse_term(term)
    folder = REPO_ROOT / '.data/CT/bill'
    detail_path = folder / f'CT_Bill_Details_{term}.csv'
    history_path = folder / f'CT_Bill_Histories_{term}.csv'
    manifest = folder / f'.CT_scrape_{term}.json'
    if all(cache_io.exists(p) for p in (detail_path, history_path, manifest)) and not force_fetch:
        print(f'Skipping CT {term}: completed outputs exist (use --force-fetch to refresh)')
        return
    cache = folder / '.cache' / term
    cache.mkdir(parents=True, exist_ok=True)
    details, histories, counts = [], [], {}
    with _make_session() as http:
        indexes = {}
        # Validate a page from EACH year before starting thousands of downloads.
        for year in years:
            records = []
            for kind in ('HB', 'SB', 'HJ', 'HR', 'SJ', 'SR'):
                response = http.post(WEB_URL + '/asp/CGABillInfo/CGABillInfoDisplay.asp', data={
                    'cboSessYr': str(year), 'optFindM': 'range',
                    'txtLowBill': kind + '0001', 'txtHiBill': kind + '9999',
                }, timeout=90)
                response.raise_for_status()
                group = parse_listing(response.content, year, kind)
                number, path = group[0]
                parse_status(_retrieve(http, path), number, year, term, WEB_URL + path)
                records.extend(group)
                print(f'CT {year} {kind}: {len(group)} indexed', flush=True)
                time.sleep(1)
            indexes[year] = records
        for year, records in indexes.items():
            counts[str(year)] = len(records)
            print(f'CT {year}: {len(records)} bills/resolutions', flush=True)
            for i, (number, path) in enumerate(records, 1):
                local = cache / f'{year}_{number}.html'
                raw = cache_io.read_bytes(local) if cache_io.exists(local) and not force_fetch else _retrieve(http, path)
                detail, history, pdf_path = parse_status(raw, number, year, term, WEB_URL + path)
                pdf_local = local.with_suffix('.pdf')
                pdf = cache_io.read_bytes(pdf_local) if cache_io.exists(pdf_local) and not force_fetch else _retrieve(http, pdf_path)
                detail[2], detail[7] = parse_pdf(pdf)
                for target, data in ((local, raw), (pdf_local, pdf)):
                    pending = target.with_suffix(target.suffix + '.tmp')
                    cache_io.write_bytes(pending, data)
                    cache_io.replace(pending, target)
                details.append(detail)
                histories.extend(history)
                if verbose or i % 25 == 0 or i == len(records):
                    print(f'  {i}/{len(records)} {number}: {len(history)} actions', flush=True)
    staged = []
    try:
        for path, header, rows in ((detail_path, DETAILS_HEADER, details), (history_path, HISTORY_HEADER, histories)):
            pending = path.with_suffix('.csv.tmp')
            staged.append((pending, path))
            with pending.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle); writer.writerow(header); writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending, target in staged:
            cache_io.replace(pending, target)

        history_keys = {(h[0], h[1]) for h in histories}
        write_manifest(manifest, {'term': term, 'details': len(details), 'histories': len(histories), 'year_counts': counts,
                                      'source_empty_histories': [{'year': d[1], 'number': d[0]} for d in details if (d[0], d[1]) not in history_keys]})
    finally:
        for pending, _ in staged:
            cache_io.unlink(pending, missing_ok=True)
    print(f'CT {term}: saved {len(details):,} bills/resolutions and {len(histories):,} histories')
