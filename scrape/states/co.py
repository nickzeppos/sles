"""
Colorado Bill Scraper
"""

from scrape.reporting import scrape_run, write_manifest

from utils import cache as cache_io

import csv
import datetime
import math
import queue
import re
import threading
import time
import urllib.parse
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://leg.colorado.gov"
BILL_SEARCH_URL = f"{BASE_URL}/bills/bill-search"


def _make_session() -> requests.Session:
    """Create a requests session."""
    return _BrowserFallbackSession()


class _BrowserFallbackSession(requests.Session):
    """Use ordinary Chrome for public pages rejected by the HTTP client."""

    def __init__(self):
        super().__init__()
        self.browser = None

    def get(self, url, **kwargs):
        time.sleep(4)
        response = super().get(url, **kwargs)
        for attempt in range(3):
            if response.status_code != 429:
                break
            retry_after = response.headers.get("Retry-After", "")
            delay = max(60 * (attempt + 1), int(retry_after) if retry_after.isdigit() else 0)
            print(f"CO: rate limited; waiting {delay}s before retry", flush=True)
            time.sleep(delay)
            response = super().get(url, **kwargs)
        if response.status_code != 406:
            return response
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        if self.browser is None:
            options = Options()
            options.add_argument("--headless")
            self.browser = webdriver.Chrome(options=options)
            self.browser.set_page_load_timeout(60)
            print("CO: using Chrome for pages returning HTTP 406", flush=True)
        self.browser.get(response.url)
        WebDriverWait(self.browser, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "main"))
        )
        response = requests.Response()
        response.status_code = 200
        response.url = self.browser.current_url
        response.encoding = "utf-8"
        response._content = self.browser.page_source.encode("utf-8")
        return response

    def close(self):
        try:
            if self.browser is not None:
                self.browser.quit()
                self.browser = None
        finally:
            super().close()


def parse_term(term: str) -> list[str]:
    if not re.fullmatch(r"\d{4}_\d{4}", term):
        raise ValueError("CO term must be YYYY_YYYY, for example 2025_2026")
    start, end = map(int, term.split("_"))
    if start % 2 != 1 or end != start + 1:
        raise ValueError("CO term must start in an odd year and end the next year")
    if end > datetime.date.today().year:
        raise ValueError(f"CO term {term} includes a future year")
    return [str(start), str(end)]


def _get_sessions(
    years: list[str], http: requests.Session
) -> list[str]:
    """Scrape the bill-search page and return session values
    matching the given years."""
    resp = http.get(BILL_SEARCH_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "lxml")

    checkboxes = soup.find_all(
        "input", {"class": "session-filters-checkbox"}
    )
    sessions = [
        cb["value"]
        for cb in checkboxes
        if any(re.search(rf"\b{y}\b", cb["value"]) for y in years)
    ]
    if not sessions or any(not any(y in s for s in sessions) for y in years):
        raise ValueError(f"CO did not list sessions for each requested year: {years}")
    return sessions


def _fetch_index_page(
    sessions: list[str],
    page_num: int,
    term: str,
    cache_dir: Path,
    http: requests.Session,
    force_fetch: bool = False,
) -> BeautifulSoup:
    """Fetch or load a listing page, caching the pruned result."""
    cache_file = cache_dir / f".index_{term}_p{page_num}.html"

    if cache_io.exists(cache_file) and not force_fetch:
        return BeautifulSoup(
            cache_io.read_text(cache_file, encoding="utf-8"), "lxml"
        )

    params = [("sessions[]", s) for s in sessions]
    params += [
        ("sort", "Bill # Ascending"),
        ("page", str(page_num)),
    ]
    resp = http.get(
        BILL_SEARCH_URL + "?" + urllib.parse.urlencode(params),
        timeout=30,
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "lxml")

    # Prune to just the results list
    results_div = soup.find("div", id="all-search-results-data-list")
    for tag in results_div.find_all(["script", "style", "img"]):
        tag.decompose()
    cache_io.write_text(cache_file, str(results_div), encoding="utf-8")

    return soup


def _iter_bill_urls(
    sessions: list[str],
    term: str,
    cache_dir: Path,
    http: requests.Session,
    force_fetch: bool = False,
):
    """Yield bill URLs for the given sessions, one listing page
    at a time."""
    # Page 1 — also used to get total count
    soup = _fetch_index_page(sessions, 1, term, cache_dir, http, force_fetch)

    count_p = soup.select_one(
        "#all-search-results-data-list > "
        "div.filter-details-container > "
        "div.d-flex.justify-content-between.flex-xl-row"
        ".flex-column-reverse.filter-details-wrapper > p"
    )
    total = int(
        count_p.get_text().split("of")[1].split("bills")[0]
        .replace(",", "").strip()
    )
    total_pages = math.ceil(total / 25)
    if total <= 0:
        raise ValueError(f"CO index returned no bills for {term}")
    print(f"Total bills: {total} ({total_pages} pages)")

    seen = set()

    def _parse_page(s: BeautifulSoup) -> list[str]:
        urls = []
        for div in s.find_all("div", {"class": "bill-result"}):
            bill_id = div.find(
                "h2", {"class": "sponsor-bill-or-resolution-tag"}
            ).get_text().strip()
            if bill_id in seen:
                raise ValueError(f"Duplicate CO index bill: {bill_id}")
            seen.add(bill_id)
            urls.append(f"{BASE_URL}/bills/{bill_id}")
        return urls

    print(f" -- 1/{total_pages}")
    yield from _parse_page(soup)

    for page_num in range(2, total_pages + 1):
        page_soup = _fetch_index_page(
            sessions, page_num, term, cache_dir, http, force_fetch
        )
        print(f" -- {page_num}/{total_pages}")
        time.sleep(0.25)
        yield from _parse_page(page_soup)
    if len(seen) != total:
        raise ValueError(f"CO index count mismatch: expected {total}, got {len(seen)}")


def _fetch_and_cache(
    bill_url: str,
    cache_dir: Path,
    http: requests.Session,
    force_fetch: bool = False,
) -> Path:
    """Fetch a bill page and save raw HTML to cache_dir.

    Returns the path to the cached file.
    """
    bill_id = bill_url.rstrip("/").split("/")[-1]
    cache_file = cache_dir / f"{bill_id}.html"

    if cache_io.exists(cache_file) and not force_fetch:
        return cache_file

    resp = http.get(bill_url, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "lxml")
    main = soup.find("main")
    for tag in main.find_all(["script", "style", "img"]):
        tag.decompose()
    cache_io.write_text(cache_file, str(main), encoding="utf-8")
    time.sleep(0.5)
    return cache_file


SESSION_TYPE_MAP = {
    "Regular Session": "RS",
    "Extraordinary Session": "S1",
}


def _parse_bill(
    cache_file: Path, bill_url: str
) -> tuple[list, list[list]]:
    """Parse bill details and history from a cached HTML file.

    Returns (details_row, history_rows).
    """
    soup = BeautifulSoup(
        cache_io.read_text(cache_file, encoding="utf-8"), "lxml"
    )

    bill_number = bill_url.rstrip("/").split("/")[-1].upper()

    # bill_type, session, keywords from bill-specs-table
    spec_rows = soup.select("table.bill-specs-table tr")
    bill_type = spec_rows[0].find("td").get_text().strip()
    session_raw = spec_rows[1].find("td").get_text().strip()
    # e.g. "2023 Regular Session"
    session_parts = session_raw.split(" ", 1)
    session = session_parts[0]
    session_type = SESSION_TYPE_MAP.get(
        session_parts[1], session_parts[1]
    )
    keywords = "; ".join(
        span.get_text().strip()
        for span in spec_rows[2].select("span.subject-tag")
    ) if len(spec_rows) > 2 else ""

    # title
    title = soup.select_one(
        "div.full-bill-topic.pr-2 > h1"
    ).get_text().strip()

    # status
    status = "; ".join(
        p.get_text().strip()
        for p in soup.select(
            "section.status-bar-section p.step-text"
        )
    )

    # long_title
    long_title = soup.select_one(
        "p.bill-long-title.pr-2"
    ).get_text().strip()

    # summary — get all text from div, not just <p> elements
    summary_div = soup.select_one("div.bill-summary-content")
    summary = re.sub(
        r"\s+", " ", summary_div.get_text(separator=" ").strip()
    ) if summary_div else ""

    # primary_sponsors
    prime_block = soup.find(
        "div", {"class": "prime-sponsor-block"}
    )
    primary_sponsors = []
    for a in (prime_block.select("a.ps-link") if prime_block else []):
        tile = a.select_one(
            "div.prime-sponsor-tile-content"
        )
        role = tile.find("p").get_text().strip()
        name = tile.select_one(
            "p.prime-sponsor-name"
        ).get_text().strip()
        primary_sponsors.append(f"{role} {name}")
    primary_sponsors_str = "; ".join(primary_sponsors)

    # sponsors + cosponsors
    sponsors_block = soup.find("div", id="bill-sponsors")
    additional_div = sponsors_block.find(
        "div", {"class": "bill-detail-additional-sponsor"}
    ) if sponsors_block else None
    cosponsor_div = sponsors_block.find(
        "div", {"class": "bill-detail-co-sponsor"}
    ) if sponsors_block else None

    sponsors_str = "; ".join(
        a.get_text().strip()
        for a in additional_div.select(
            "div.sponsor-link a.link-primary"
        )
    ) if additional_div else ""

    cosponsors_str = "; ".join(
        a.get_text().strip()
        for a in cosponsor_div.select(
            "div.sponsor-link a.link-primary"
        )
    ) if cosponsor_div else ""

    # house_committee + senate_committee
    house_comm = ""
    senate_comm = ""
    comm_block = soup.find(
        "div", id="bill-activity-committees"
    )
    if comm_block:
        for acc in comm_block.select("div.gen-accordion"):
            raw = acc.find("h4").get_text().strip()
            name = re.sub(r"^.+\| ", "", raw)
            name = re.sub(r"\s*\(\d+\)$", "", name)
            if name.startswith("House "):
                house_comm = name[len("House "):]
            elif name.startswith("Senate "):
                senate_comm = name[len("Senate "):]

    details_row = [
        bill_number,
        session,
        session_type,
        bill_type,
        title,
        keywords,
        status,
        primary_sponsors_str,
        sponsors_str,
        cosponsors_str,
        house_comm,
        senate_comm,
        long_title,
        summary,
        bill_url,
    ]

    # bill history
    history_rows = []
    hist_block = soup.find("div", id="bill-histories")
    if hist_block:
        action_rows = hist_block.select("table tr")[1:]
        # table is newest-first; reverse to assign order 1=oldest
        action_rows = list(reversed(action_rows))
        for order, row in enumerate(action_rows, 1):
            cells = row.find_all("td")
            date = datetime.datetime.strptime(
                cells[0].get_text().strip(), "%m/%d/%Y"
            ).strftime("%Y-%m-%d")
            chamber = cells[1].get_text().strip()
            action = cells[2].get_text().strip()
            history_rows.append([
                bill_number,
                session,
                session_type,
                chamber,
                date,
                action,
                order,
            ])

    if not history_rows:
        raise ValueError(f"Colorado bill has no history: {bill_number}")

    return details_row, history_rows


def retry_failed(state: str, term: str, verbose: bool = False):
    """Retry URLs from .failed_<term>.txt, appending to existing CSVs."""
    repo_root = Path(__file__).parent.parent.parent
    bill_dir = repo_root / ".data" / state / "bill"
    failed_file = bill_dir / f".failed_{term}.txt"

    if not cache_io.exists(failed_file):
        print(f"No failed file found: {failed_file}")
        return

    urls = [
        u.strip()
        for u in cache_io.read_text(failed_file).splitlines()
        if u.strip()
    ]
    if not urls:
        print("No failed URLs to retry.")
        return

    parts = term.split("_")
    years = parts if len(parts) == 2 else [parts[0]]

    cache_dir = bill_dir / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    http = _make_session()

    # Open CSVs in append mode — no header rewrite
    sinks: dict[str, object] = {}
    for year in years:
        details_file = bill_dir / f"CO_Bill_Details_{year}.csv"
        histories_file = bill_dir / f"CO_Bill_Histories_{year}.csv"
        df = open(details_file, "a", newline="")
        hf = open(histories_file, "a", newline="")
        sinks[year] = (df, hf, csv.writer(df), csv.writer(hf))

    still_failed = []
    try:
        for num, url in enumerate(urls, 1):
            try:
                # Force re-fetch by removing cached file if present
                bill_id = url.rstrip("/").split("/")[-1]
                cached = cache_dir / f"{bill_id}.html"
                if cache_io.exists(cached):
                    cache_io.unlink(cached)

                cached = _fetch_and_cache(url, cache_dir, http)
                details, history = _parse_bill(cached, url)

                bill_session = details[1]
                if bill_session in sinks:
                    _, _, dw, hw = sinks[bill_session]
                    dw.writerow(details)
                    hw.writerows(history)

                print(f" ({num}/{len(urls)}) -- OK -- {url}")

            except Exception as e:
                print(
                    f" *** ({num}/{len(urls)}) -- FAILED AGAIN"
                    f" -- {url}\n     {e}"
                )
                still_failed.append(url)
    finally:
        http.close()
        for df, hf, _, __ in sinks.values():
            df.close()
            hf.close()

    # Rewrite failed file with only still-failing URLs
    if still_failed:
        cache_io.write_text(failed_file, "\n".join(still_failed) + "\n")
        print(f"\n{len(still_failed)} still failed: {failed_file}")
    else:
        cache_io.unlink(failed_file)
        print(f"\nAll retried successfully — removed {failed_file.name}")


def preview_bill(state: str, bill_id: str, verbose: bool = False):
    """Fetch, parse, and print details + history for a single bill.

    Does not write to any CSV or clean up the cache file.
    """
    repo_root = Path(__file__).parent.parent.parent
    cache_dir = repo_root / ".data" / state / "bill" / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    http = _make_session()
    bill_url = f"{BASE_URL}/bills/{bill_id}"
    try:
        cached = _fetch_and_cache(bill_url, cache_dir, http)
        details, history = _parse_bill(cached, bill_url)
    finally:
        http.close()

    DETAILS_COLS = [
        "bill_number", "session", "session_type", "bill_type",
        "title", "keywords", "status", "primary_sponsors",
        "sponsors", "cosponsors", "house_committee",
        "senate_committee", "long_title", "summary", "bill_url",
    ]
    HISTORY_COLS = [
        "bill_number", "session", "session_type", "chamber",
        "action_date", "action", "order",
    ]

    print("\n--- Bill Details ---")
    for col, val in zip(DETAILS_COLS, details):
        print(f"  {col}: {val}")

    print(f"\n--- Bill History ({len(history)} rows) ---")
    for row in history:
        print(
            "  " + " | ".join(
                f"{c}: {v}" for c, v in zip(HISTORY_COLS, row)
            )
        )


def _cleanup_cache_file(cache_file: Path):
    """Delete a single cached bill HTML file after parsing."""
    if cache_io.exists(cache_file):
        cache_io.unlink(cache_file)


@scrape_run
def scrape(state: str, term: str, verbose: bool = False, force_fetch: bool = False):
    """Main entry point for CO scraping.

    Args:
        state: "CO"
        term: e.g. "2023_2024" — must end in an even year
        verbose: Enable verbose logging
    """
    years = parse_term(term)

    repo_root = Path(__file__).parent.parent.parent
    cache_dir = repo_root / ".data" / state / "bill" / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    http = _make_session()

    sessions = _get_sessions(years, http)
    print(f"Sessions found for {term}: {sessions}")

    bill_url_iter = _iter_bill_urls(sessions, term, cache_dir, http, force_fetch)

    # Open one CSV sink per year
    DETAILS_HEADER = [
        "bill_number", "session", "session_type", "bill_type",
        "title", "keywords", "status", "primary_sponsors",
        "sponsors", "cosponsors", "house_committee",
        "senate_committee", "long_title", "summary", "bill_url",
    ]
    HISTORY_HEADER = [
        "bill_number", "session", "session_type", "chamber",
        "action_date", "action", "order",
    ]

    bill_dir = repo_root / ".data" / state / "bill"
    failed_file = bill_dir / f".failed_{term}.txt"

    sinks: dict[str, tuple] = {}
    staged = []
    for year in years:
        details_file = bill_dir / f"CO_Bill_Details_{year}.csv"
        histories_file = (
            bill_dir / f"CO_Bill_Histories_{year}.csv"
        )
        detail_pending = details_file.with_suffix(".csv.tmp")
        history_pending = histories_file.with_suffix(".csv.tmp")
        staged.extend([(detail_pending, details_file), (history_pending, histories_file)])
        df = open(detail_pending, "w", newline="", encoding="utf-8")
        hf = open(history_pending, "w", newline="", encoding="utf-8")
        dw = csv.writer(df)
        hw = csv.writer(hf)
        dw.writerow(DETAILS_HEADER)
        hw.writerow(HISTORY_HEADER)
        sinks[year] = (df, hf, dw, hw)

    fetch_q: queue.Queue = queue.Queue(maxsize=5)
    discovery_errors = []
    detail_count = history_count = 0
    failures = []

    def fetch_worker():
        try:
            for url in bill_url_iter:
                try:
                    cached = _fetch_and_cache(url, cache_dir, http, force_fetch)
                    fetch_q.put((url, cached))
                except Exception as e:
                    print(f" *** FETCH ERROR -- {url}\n     {e}")
                    fetch_q.put((url, None))
                    if isinstance(e, requests.HTTPError) and e.response.status_code == 429:
                        raise  # stop after exhausted rate-limit retries
        except Exception as e:
            discovery_errors.append(e)
        finally:
            fetch_q.put(None)  # wake the consumer even if index fetching failed

    fetcher = threading.Thread(target=fetch_worker, daemon=True)
    fetcher.start()

    try:
        num = 0
        while True:
            item = fetch_q.get()
            if item is None:
                break
            num += 1
            url, cached = item

            if cached is None:
                print(f" *** ({num}) -- SKIPPING (fetch failed) -- {url}")
                failures.append(url)
                continue

            try:
                details, history = _parse_bill(cached, url)

                bill_session = details[1]
                if bill_session not in sinks:
                    raise ValueError(f"Unexpected CO bill year {bill_session}: {url}")
                _, _, dw, hw = sinks[bill_session]
                dw.writerow(details)
                hw.writerows(history)
                detail_count += 1
                history_count += len(history)

                print(f" ({num}) -- {url}")

            except Exception as e:
                print(
                    f" *** ({num}) -- PARSE ERROR -- {url}\n"
                    f"     {e}"
                )
                failures.append(url)
                # leave cache file in place for inspection
    finally:
        fetcher.join()
        http.close()
        for df, hf, _, __ in sinks.values():
            df.close()
            hf.close()

    if failures:
        cache_io.write_text(failed_file, "\n".join(failures) + "\n", encoding="utf-8")
    if failures or discovery_errors:
        for pending, _ in staged:
            cache_io.unlink(pending, missing_ok=True)
        if discovery_errors:
            raise RuntimeError("CO index discovery failed; prior outputs retained") from discovery_errors[0]
        raise RuntimeError(f"CO has {len(failures)} failed bills; prior outputs retained; rerun to resume cached pages")
    manifest = bill_dir / f".CO_scrape_{term}.json"
    manifest.unlink(missing_ok=True)
    for pending, target in staged:
        cache_io.replace(pending, target)
    write_manifest(manifest, {"term": term, "sessions": sessions,
                              "details": detail_count, "histories": history_count})
    cache_io.unlink(failed_file, missing_ok=True)
    print("\n\n --- ALL DONE --- \n")
