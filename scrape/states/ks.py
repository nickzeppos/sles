"""Kansas measures and complete paginated histories for a requested term."""
from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io

import csv
from datetime import date, datetime
from pathlib import Path
import re
import time
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from scrape.http import make_session

BASE_URL = 'https://www.kslegislature.gov'
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = ['bill_id', 'session', 'version_status', 'original_sponsor', 'current_sponsor',
                  'requested_by', 'title', 'bill_url', 'term']
HISTORY_HEADER = ['bill_id', 'session', 'chamber', 'action_date', 'action', 'order', 'journal_page', 'term']


def parse_term(term):
    if not re.fullmatch(r'\d{4}_\d{4}', term):
        raise ValueError('KS term must be YYYY_YYYY, for example 2025_2026')
    start, end = map(int, term.split('_'))
    if start % 2 != 1 or end != start + 1 or end > date.today().year:
        raise ValueError('KS requires an odd-starting consecutive two-year term without future years')
    return start, end


def select_sessions(home_url, archive_html, term):
    start, end = parse_term(term)
    records = {}
    if urlparse(home_url).path == f'/b{start}_{str(end)[2:]}/':
        records[term + '_RS'] = home_url
    for link in BeautifulSoup(archive_html, 'lxml').select('main a[href]'):
        label = link.get_text(' ', strip=True)
        if label.startswith(f'{start}-{end} Regular Sessions'):
            records[term + '_RS'] = urljoin(BASE_URL, link['href'])
        special = re.match(r'(\d{4}) Special Session\b', label)
        if special and int(special[1]) in (start, end):
            key = special[1] + '_SS'
            if key in records:
                raise ValueError('KS multiple special sessions need distinct source identifiers')
            records[key] = urljoin(BASE_URL, link['href'])
    if term + '_RS' not in records:
        raise ValueError(f'KS did not list the regular session for {term}')
    return [{'id': key, 'url': value} for key, value in sorted(records.items())]


def parse_page(html, url):
    soup = BeautifulSoup(html, 'lxml')
    marker = soup.select_one('.site-table-count')
    match = re.fullmatch(r'Showing ([\d,]+)[–-]([\d,]+) of ([\d,]+) (?:measures|entries)',
                         marker.get_text(' ', strip=True) if marker else '')
    if not match:
        raise ValueError(f'KS missing pagination count: {url}')
    first, last, total = (int(v.replace(',', '')) for v in match.groups())
    rows = soup.select('table.site-table tbody tr')
    if len(rows) != last - first + 1 or not 1 <= first <= last <= total:
        raise ValueError(f'KS page count mismatch: {url}')
    next_link = next((a for a in soup.select('nav[aria-label="Pagination"] a[hx-get]')
                      if a.get_text(strip=True) == 'Next'), None)
    next_url = urljoin(url, next_link['hx-get']) if next_link else None
    if bool(next_url) != (last < total):
        raise ValueError(f'KS missing/unexpected next page: {url}')
    if next_url and urlparse(next_url).netloc != urlparse(url).netloc:
        raise ValueError('KS unexpected pagination host')
    return rows, first, last, total, next_url


def _fetch(http, url):
    response = http.get(url, timeout=60)
    response.raise_for_status()
    time.sleep(1)
    return response


def pages(http, url, cache, key, force_fetch):
    page, count, total, seen = 1, 0, None, set()
    while url:
        if url in seen:
            raise ValueError('KS pagination cycle')
        seen.add(url)
        local = cache / f'{key}_{page}.html'
        html = cache_io.read_text(local, encoding='utf-8') if cache_io.exists(local) and not force_fetch else _fetch(http, url).text
        rows, first, last, reported_total, next_url = parse_page(html, url)
        if first != count + 1 or (total is not None and total != reported_total):
            raise ValueError(f'KS pagination overlap/gap or changing count: {url}')
        count, total = last, reported_total
        pending = local.with_suffix('.html.tmp'); cache_io.write_text(pending, html, encoding='utf-8'); cache_io.replace(pending, local)
        yield rows
        url = next_url
        page += 1
    if count != total:
        raise ValueError('KS incomplete pagination')


def parse_bill(html, record, action_rows, term):
    number, session, url = record
    soup = BeautifulSoup(html, 'lxml')
    identity = soup.select_one('main .bill-number-chip')
    if identity is None or re.sub(r'\s+', '', identity.get_text()).upper() != number:
        raise ValueError(f'KS bill identity mismatch: {url}')
    title = soup.select_one('.bill-hero-sub')
    if title is None or not title.get_text(strip=True):
        raise ValueError(f'KS missing title: {url}')
    versions = '; '.join(n.get_text(' ', strip=True) for n in soup.select('main .version-row summary .label'))
    sponsors = []
    for label in ('Original Sponsor', 'Current Sponsor'):
        heading = soup.find('h3', string=re.compile('^' + re.escape(label) + 's?$'))
        names = []
        if heading:
            for section in heading.find_next_siblings():
                if section.name == 'h3': break
                links = section.select('a.sponsor-link')
                names.extend(a.get_text(' ', strip=True) for a in links)
                if not links and section.get_text(strip=True):
                    names.append(section.get_text(' ', strip=True))
        sponsors.append('; '.join(names))
    requested = ''
    for row in soup.select('main .detail-row'):
        label, value = row.select_one('.label'), row.select_one('.value')
        if label and label.get_text(strip=True) == 'Requested By' and value:
            requested = value.get_text(' ', strip=True)
            if requested == '—': requested = ''
    match = re.fullmatch(r'([A-Z]+)(\d+)', number)
    if not match:
        raise ValueError(f'KS unexpected measure number: {number}')
    canonical = match[1] + match[2].zfill(4)
    details = [canonical, session, versions, *sponsors, requested, title.get_text(' ', strip=True), url, term]
    histories = []
    for row in action_rows:
        cells = row.find_all('td')
        if len(cells) < 4:
            raise ValueError(f'KS malformed history row: {url}')
        when = datetime.strptime(cells[0].get_text(' ', strip=True), '%a, %b %d, %Y').date().isoformat()
        chamber, action, journal = (cells[i].get_text(' ', strip=True) for i in (1, 2, 3))
        if not action:
            raise ValueError(f'KS empty history action: {url}')
        histories.append([canonical, session, chamber, when, action, len(action_rows) - len(histories),
                          '' if journal == '—' else journal, term])
    if not histories:
        raise ValueError(f'KS empty history: {url}')
    return details, histories


@scrape_run
def scrape(state, term, verbose=False, force_fetch=False):
    if state.upper() != 'KS':
        raise ValueError('Kansas scraper requires state KS')
    parse_term(term)
    folder = REPO_ROOT / '.data/KS/bill'
    outputs = [folder / f'KS_{name}_{term}.csv' for name in ('Bill_Details', 'Bill_Histories')]
    manifest = folder / f'.KS_scrape_{term}.json'
    if all(cache_io.exists(p) for p in [*outputs, manifest]) and not force_fetch:
        print(f'Skipping KS {term}: completed outputs exist (use --force-fetch to refresh)')
        return
    cache = folder / '.cache' / term
    cache.mkdir(parents=True, exist_ok=True)
    details, histories, counts, excluded = [], [], {}, []
    with make_session() as http:
        home = _fetch(http, BASE_URL + '/li/')
        archive = _fetch(http, urljoin(home.url, 'archive/'))
        sessions = select_sessions(home.url, archive.text, term)
        for session in sessions:
            root = _fetch(http, session['url'])
            soup = BeautifulSoup(root.text, 'lxml')
            link = next((a for a in soup.select('a[href]') if a.get_text(' ', strip=True).endswith('All Measures')), None)
            if link is None:
                raise ValueError(f'KS session uses an unsupported archive layout: {session["url"]}')
            listing_url = urljoin(root.url, link['href'])
            listing = BeautifulSoup(_fetch(http, listing_url).text, 'lxml').select_one('#measures-container[hx-get]')
            if listing is None:
                raise ValueError('KS missing measure-list route')
            index_url = urljoin(listing_url, listing['hx-get'])
            records, seen = [], set()
            for rows in pages(http, index_url, cache, session['id'] + '_index', force_fetch):
                for row in rows:
                    path = row.get('data-href')
                    kind = row.select_one('[data-label="Type"]')
                    if not path or kind is None:
                        raise ValueError('KS malformed measure row')
                    if path in seen:
                        raise ValueError('KS repeated measure across pages')
                    seen.add(path)
                    if kind.get_text(strip=True) == 'Appointment':
                        excluded.append(path)
                        continue
                    if kind.get_text(strip=True) not in ('Bill', 'Resolution'):
                        raise ValueError(f'KS unexpected measure kind: {kind.get_text()}')
                    url = urljoin(listing_url, path)
                    category = 'bills/' if kind.get_text(strip=True) == 'Bill' else 'resolutions/'
                    if not urlparse(url).path.startswith(urlparse(listing_url).path.removesuffix('measures/') + category):
                        raise ValueError('KS wrong-session measure link')
                    number = urlparse(url).path.rstrip('/').rsplit('/', 1)[-1].upper()
                    records.append((number, session['id'], url))
                if verbose or len(seen) % 100 == 0:
                    print(f'KS {session["id"]} index: {len(seen)} measures', flush=True)
            counts[session['id']] = len(records)
            for i, record in enumerate(records, 1):
                key = session['id'] + '_' + record[0]
                local = cache / (key + '.html')
                html = cache_io.read_text(local, encoding='utf-8') if cache_io.exists(local) and not force_fetch else _fetch(http, record[2]).text
                route = BeautifulSoup(html, 'lxml').select_one('#history-container[hx-get]')
                if route is None:
                    raise ValueError(f'KS missing history route: {record[2]}')
                history_url = urljoin(record[2], route['hx-get'])
                if urlparse(history_url).path.lower() != urlparse(record[2]).path.lower() + 'history/':
                    raise ValueError('KS history route does not match bill identity')
                action_rows = [row for group in pages(http, history_url, cache, key + '_history', force_fetch) for row in group]
                parsed = parse_bill(html, record, action_rows, term)
                pending = local.with_suffix('.html.tmp'); cache_io.write_text(pending, html, encoding='utf-8'); cache_io.replace(pending, local)
                details.append(parsed[0]); histories.extend(parsed[1])
                if verbose or i % 25 == 0 or i == len(records):
                    print(f'KS {session["id"]} {i}/{len(records)} {record[0]}: {len(parsed[1])} actions', flush=True)
    staged = []
    try:
        for target, header, rows in zip(outputs, (DETAILS_HEADER, HISTORY_HEADER), (details, histories)):
            pending = target.with_suffix('.csv.tmp'); staged.append((pending, target))
            with pending.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle); writer.writerow(header); writer.writerows(rows)
        cache_io.unlink(manifest, missing_ok=True)
        for pending, target in staged: cache_io.replace(pending, target)

        write_manifest(manifest, {'term': term, 'details': len(details), 'histories': len(histories), 'session_counts': counts,
                                      'excluded_appointments': excluded})
    finally:
        for pending, _ in staged: cache_io.unlink(pending, missing_ok=True)
    print(f'KS {term}: saved {len(details):,} bills/resolutions and {len(histories):,} histories')
