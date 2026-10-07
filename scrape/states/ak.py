"""Alaska bills and resolutions for one explicitly selected legislature.

Uses the complete All Measures Introduced index, rather than following the
old scraper's Next Bill chain. Raw bill pages are cached so failed/interrupted
runs resume without losing successfully downloaded pages.
"""

from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io

import csv
from datetime import date, datetime
from pathlib import Path
import re
import time
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.akleg.gov"
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = [
    "bill_id", "session", "bill_version", "status", "status_date", "short_title",
    "primary_sponsor", "cosponsors", "title", "keywords", "term", "bill_url",
]
HISTORY_HEADER = [
    "bill_id", "session", "chamber", "action_location", "action_date", "action",
    "journal_page", "journal_link", "order", "term",
]


def parse_term(term: str) -> tuple[int, int]:
    if not re.fullmatch(r"\d{4}_\d{4}", term):
        raise ValueError("AK term must be YYYY_YYYY, for example 2025_2026")
    start, end = map(int, term.split("_"))
    if start % 2 != 1 or end != start + 1:
        raise ValueError("AK term must start in an odd year and end the next year")
    if end > date.today().year:
        raise ValueError(f"AK term {term} includes a future year")
    return start, end


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _fetch(http: requests.Session, url: str) -> str:
    for attempt in range(4):
        response = http.get(url, timeout=45)
        if response.status_code != 429 or attempt == 3:
            response.raise_for_status()
            return response.text
        retry_after = response.headers.get("Retry-After", "")
        delay = max(60, int(retry_after)) if retry_after.isdigit() else 60 * (attempt + 1)
        print(f"  Alaska rate limit: waiting {delay}s before retrying", flush=True)
        time.sleep(delay)
    raise RuntimeError("Unreachable Alaska retry state")


def select_legislature(html: str, term: str) -> tuple[int, str]:
    years = parse_term(term)
    soup = BeautifulSoup(html, "lxml")
    found = set()
    pattern = re.compile(r"^(\d+)(?:st|nd|rd|th) Legislature \((\d{4})-(\d{4})\)$")
    for anchor in soup.find_all("a"):
        label = anchor.get_text(" ", strip=True)
        match = pattern.fullmatch(label)
        if match and (int(match[2]), int(match[3])) == years:
            found.add((int(match[1]), label))
    if len(found) != 1:
        raise ValueError(f"AK term {term} does not identify exactly one available legislature")
    return found.pop()


def parse_listing(html: str, legislature: int) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    bills = {}
    for anchor in soup.select("td.billRoot a[href]"):
        parts = urlsplit(anchor["href"])
        if parts.path != f"/basis/Bill/Detail/{legislature}":
            raise ValueError(f"AK listing links to another legislature: {anchor['href']}")
        root = re.sub(r"\s+", "", parse_qs(parts.query).get("Root", [""])[0])
        label = re.sub(r"\s+", "", anchor.get_text())
        if not re.fullmatch(r"[A-Z]+\d+", root) or root != label:
            raise ValueError(f"Invalid Alaska bill link: {anchor}")
        if root in bills:
            raise ValueError(f"Repeated Alaska bill in index: {root}")
        bills[root] = f"{BASE_URL}{parts.path}?{urlencode({'Root': root})}"
    if not bills:
        raise ValueError("Alaska All Measures Introduced index returned no bills")
    return list(bills.items())


def _field(soup: BeautifulSoup, label: str, required: bool = True) -> str:
    for item in soup.select("ul.information li"):
        span = item.find("span")
        if span and span.get_text(" ", strip=True).rstrip(":").strip().casefold() == label.casefold():
            value = span.find_next_sibling()
            if value is not None:
                return " ".join(value.get_text(" ", strip=True).split())
    if required:
        raise ValueError(f"Alaska bill page is missing {label}")
    return ""


def parse_bill(html: str, bill_id: str, session: str, term: str, bill_url: str) -> tuple[list, list] | None:
    soup = BeautifulSoup(html, "lxml")
    actual_id = re.sub(r"\s+", "", _field(soup, "Bill"))
    if actual_id != bill_id:
        raise ValueError(f"Alaska requested {bill_id} but received {actual_id}")
    short_title = _field(soup, "Short Title")
    # Alaska retains unused numbers in its index (e.g. 34th Legislature HB32).
    # These explicit placeholders are not introduced legislation.
    if short_title.upper() == "NOT INTRODUCED":
        return None
    sponsors = _field(soup, "Sponsor(s)", required=False)
    if not sponsors:
        # Older pages omit the suffix, but still carry a Sponsor label.
        sponsors = _field(soup, "Sponsor", required=False)
    if not sponsors:
        raise ValueError(f"Alaska bill is missing its sponsor: {bill_id}")
    primary, separator, cosponsors = sponsors.partition(",")
    primary = re.sub(r"^(?:REPRESENTATIVES?|SENATORS?)\s+", "", primary).strip()
    keywords = list(dict.fromkeys(a.get_text(" ", strip=True) for a in soup.find_all("a", href=re.compile(r"[?&]subject="))))
    status_date = _field(soup, "Status Date", required=False)
    if status_date:
        status_date = datetime.strptime(status_date, "%m/%d/%Y").date().isoformat()
    details = [
        bill_id, session, _field(soup, "Bill Version", required=False),
        _field(soup, "Current Status"), status_date, short_title,
        primary, cosponsors.strip(), _field(soup, "Title"), "--".join(keywords),
        term, bill_url,
    ]
    if not details[5] or not details[8]:
        raise ValueError(f"Alaska bill has no title: {bill_id}")
    table = soup.select_one("div.actions table")
    if table is None:
        raise ValueError(f"Alaska bill has no action table: {bill_id}")
    actions = []
    for row in table.find_all("tr"):
        if row.find("td") is None:
            continue
        when, text = row.find("time"), row.find("span", attrs={"data-label": "Text"})
        if when is None or text is None:
            raise ValueError(f"Alaska bill has an unrecognized action row: {bill_id}")
        action_date = date.fromisoformat(when["datetime"]).isoformat()
        action = " ".join(text.get_text(" ", strip=True).split())
        if not action:
            raise ValueError(f"Alaska bill has an empty action: {bill_id}")
        chamber_match = re.match(r"\(([HS])\)", action)
        chamber = chamber_match[1] if chamber_match else ""
        journal_cell = row.find("span", attrs={"data-label": "Page"})
        journal = journal_cell.find("a") if journal_cell else None
        actions.append([
            bill_id, session, chamber, " ".join(row.get("class", [])), action_date,
            action, journal.get_text(strip=True) if journal else "",
            urljoin(BASE_URL, journal["href"].replace(" ", "%20")) if journal else "",
            len(actions) + 1, term,
        ])
    if not actions:
        raise ValueError(f"Alaska bill has no history: {bill_id}")
    return details, actions


@scrape_run
def scrape(state: str, term: str, verbose: bool = False, force_fetch: bool = False):
    if state.upper() != "AK":
        raise ValueError("Alaska scraper requires state AK")
    parse_term(term)
    folder = REPO_ROOT / ".data" / "AK" / "bill"
    detail_path = folder / f"AK_Bill_Details_{term}.csv"
    history_path = folder / f"AK_Bill_Histories_{term}.csv"
    manifest_path = folder / f".AK_scrape_{term}.json"
    if all(cache_io.exists(p) for p in (detail_path, history_path, manifest_path)) and not force_fetch:
        print(f"Skipping AK {term}: completed outputs exist (use --force-fetch to refresh)")
        return
    cache = folder / ".cache" / term
    cache.mkdir(parents=True, exist_ok=True)
    details, histories, not_introduced = [], [], []
    with _make_session() as http:
        legislature, label = select_legislature(_fetch(http, f"{BASE_URL}/basis/Home/BillsandLaws"), term)
        index = _fetch(http, f"{BASE_URL}/basis/Bill/Range/{legislature}?bill1=&bill2=")
        bills = parse_listing(index, legislature)
        print(f"AK {term}: {label}; {len(bills)} bills/resolutions", flush=True)
        for number, (bill_id, url) in enumerate(bills, 1):
            path = cache / f"{bill_id}.html"
            cached = cache_io.exists(path) and not force_fetch
            html = cache_io.read_text(path, encoding="utf-8") if cached else _fetch(http, url)
            parsed = parse_bill(html, bill_id, label, term, url)
            if not cached:
                pending = path.with_suffix(".html.tmp")
                cache_io.write_text(pending, html, encoding="utf-8")
                cache_io.replace(pending, path)
                time.sleep(1.5)
            if parsed is None:
                not_introduced.append(bill_id)
                print(f"  {number}/{len(bills)} {bill_id}: NOT INTRODUCED (excluded)", flush=True)
                continue
            bill, actions = parsed
            details.append(bill)
            histories.extend(actions)
            if verbose or number % 25 == 0 or number == len(bills):
                print(f"  {number}/{len(bills)} {bill_id}: {len(actions)} actions" + (" [cached]" if cached else ""), flush=True)
    staged = []
    try:
        for path, header, rows in ((detail_path, DETAILS_HEADER, details), (history_path, HISTORY_HEADER, histories)):
            pending = path.with_suffix(".csv.tmp")
            staged.append((pending, path))
            with pending.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(header)
                writer.writerows(rows)
        cache_io.unlink(manifest_path, missing_ok=True)
        for pending, path in staged:
            cache_io.replace(pending, path)

        write_manifest(manifest_path, {
            "term": term, "legislature": legislature,
            "details": len(details), "histories": len(histories),
            "index_count": len(bills), "not_introduced": not_introduced,
        })
    finally:
        for pending, _ in staged:
            cache_io.unlink(pending, missing_ok=True)
    print(f"AK {term}: saved {len(details):,} instruments and {len(histories):,} history rows")
