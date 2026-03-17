"""
Virginia Bill Scraper

Faithful port of VA_State_Leg_Scrape_Auto_CHP.py.

Scrapes bill details and histories from the Virginia Legislative
Information System (LIS): http://lis.virginia.gov

Notes:
- Sessions are labeled yearly but bill numbers increase through two-year terms
- Special sessions are folded into bill histories (reintroduced bills keep
  their number; actions are recorded in the second session's page)
"""

from __future__ import annotations

import csv
import datetime
import json
import re
import time
from pathlib import Path
from unicodedata import normalize

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "http://lis.virginia.gov"

# CSV column headers
DETAILS_HEADER = [
    "bill_id",
    "term",
    "session_year",
    "session",
    "short_title",
    "sponsor",
    "house_sponsors",
    "senate_sponsors",
    "summary",
    "bill_url",
]

HISTORY_HEADER = [
    "bill_id",
    "term",
    "session_year",
    "session",
    "action_date",
    "chamber",
    "action",
    "order",
    "action_details_url",
]


def _make_session() -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=15,  # 15s, 30s, 60s
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_page_soup(
    url: str, session: requests.Session, parser: str = "lxml"
) -> BeautifulSoup | str:
    """Fetch a page and return its BeautifulSoup parse tree.

    Returns 'HTTP Error' on 4xx/5xx responses (matching old script behavior).
    Retries on timeouts with backoff: 15s -> 60s -> 120s.
    """
    try:
        resp = session.get(url, timeout=20)
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        return "HTTP Error"
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
        print("\n ~~> Retrying Bill Request")
        time.sleep(15)
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            return "HTTP Error"
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ):
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            try:
                resp = session.get(url, timeout=60)
                resp.raise_for_status()
            except requests.exceptions.HTTPError:
                return "HTTP Error"
            except Exception:
                print("\n ~~> SOCKET TIMEOUT --- Retrying Bill Request")
                time.sleep(120)
                resp = session.get(url, timeout=60)
                resp.raise_for_status()

    time.sleep(0.5)
    return BeautifulSoup(resp.content, parser)


def get_term_bills(
    s_yr: int, session: requests.Session, verbose: bool = False
) -> list[list]:
    """Scrape bill listing pages for a two-year term.

    Returns list of [bill_num, term, year, session_name, title, url].
    Iterates session_num 1-9 per year; breaks on "Sorry" response.
    Handles "More..." pagination links.
    """
    term = f"{s_yr}-{s_yr + 1}"
    print(f"\n\n\t~~~~ Gathering Bill URLs for the {term} Session ~~~~\n")

    term_bills = []
    for yr in [s_yr, s_yr + 1]:
        yr_str = str(yr)[2:4]
        for session_iter in range(1, 10):
            this_url = (
                f"{BASE_URL}/cgi-bin/legp604.exe?"
                f"{yr_str}{session_iter}+lst+ALL"
            )
            time.sleep(0.75)  # extra delay for listing pages
            this_soup = get_page_soup(this_url, session)
            if isinstance(this_soup, str):
                break

            # Check for "Sorry" response indicating no more sessions
            check_response = this_soup.body.find_all(
                string=re.compile(
                    "Sorry, your request could not be processed"
                )
            )
            if check_response and check_response[0].strip() == (
                "Sorry, your request could not be processed at this time."
            ):
                break

            # Extract session name from page header
            main_div = this_soup.find("div", {"id": "mainC"})
            if main_div is None:
                # Page didn't load properly — retry once
                print("  ~~> Page missing mainC div, retrying...")
                time.sleep(5)
                this_soup = get_page_soup(this_url, session)
                if isinstance(this_soup, str):
                    break
                main_div = this_soup.find("div", {"id": "mainC"})
                if main_div is None:
                    print("  ~~> Still missing mainC div, skipping session")
                    break
            session_name = (
                main_div.find_all("h2")[0].get_text().strip()
            )

            page_num = 1
            print(f" ~~~~~~~~~ {session_name} ~~~~~~~~~ ")

            while True:
                # Find bill links for this session
                pattern = re.compile(
                    rf"{yr_str}{session_iter}\+sum"
                )
                these_bills = this_soup.find_all("a", href=pattern)
                these_bills = [
                    [
                        a.text.strip(),
                        term,
                        yr,
                        session_name,
                        a.parent.get_text().strip(),
                        BASE_URL + a["href"],
                    ]
                    for a in these_bills
                ]
                term_bills.extend(these_bills)
                print(f"  -- {page_num}")

                # Check for "More..." pagination
                more_pattern = re.compile(
                    rf"{yr_str}{session_iter}\+lst\+ALL\+"
                )
                check_for_more = this_soup.find_all("a", href=more_pattern)
                if len(check_for_more) == 2:
                    this_url = BASE_URL + check_for_more[0]["href"]
                    time.sleep(0.75)  # extra delay for listing pages
                    this_soup = get_page_soup(this_url, session)
                    if isinstance(this_soup, str):
                        break
                    page_num += 1
                else:
                    break

    return term_bills


def get_bill_data(
    bill_row: list, session: requests.Session
) -> list | str:
    """Scrape an individual bill page for details and history.

    Returns [bill_details_row, bill_history_rows] or 'HTTP Error'.
    """
    bill_num, term, s_yr, session_name, descrip, bill_url = bill_row

    # Format bill ID: "HB 1" -> "HB0001"
    parts = bill_num.split(" ")
    bill_id = parts[0] + parts[1].zfill(4)

    # Fetch bill summary page
    bill_soup = get_page_soup(bill_url, session)
    if isinstance(bill_soup, str):
        return "HTTP Error"

    # Extract summary
    summary_match = bill_soup.find_all("h4", string=re.compile("SUMMARY"))
    summary = ""
    if summary_match:
        try:
            summary = (
                summary_match[0]
                .find_next("p")
                .get_text()
                .replace("\r\n", " ")
                .strip()
            )
        except Exception:
            summary = ""

    # Extract history
    history_match = bill_soup.find_all("h4", string=re.compile("HISTORY"))
    if not history_match:
        # Retry once if history section not found
        time.sleep(5)
        bill_soup = get_page_soup(bill_url, session)
        if isinstance(bill_soup, str):
            return "HTTP Error"
        history_match = bill_soup.find_all("h4", string=re.compile("HISTORY"))
        if not history_match:
            print(f"  ~~> WARNING: No HISTORY section for {bill_id}, skipping")
            return "HTTP Error"

    history_ul = history_match[0].find_next("ul", {"class": "linkSect"})
    history_items = history_ul.find_all("li")

    bill_history = []
    order = 1
    for row in history_items:
        row_text = row.get_text().replace("\xa0", "").strip()
        row_parts = row_text.split(" ", 2)
        # Parse date: MM/DD/YY -> YYYY-MM-DD
        date = datetime.datetime.strptime(
            row_parts[0], "%m/%d/%y"
        ).strftime("%Y-%m-%d")
        chamber = re.sub(":", "", row_parts[1])
        action = ""
        if len(row_parts) > 2:
            action = row_parts[2].replace("\r\n", "").strip()

        a_tag = row.find_all("a")
        action_details_url = ""
        if a_tag:
            action_details_url = BASE_URL + a_tag[0]["href"]

        bill_history.append([
            bill_id,
            term,
            s_yr,
            session_name,
            date,
            chamber,
            action,
            order,
            action_details_url,
        ])
        order += 1

    # Fetch sponsor page (replace +sum+ with +mbr+)
    sponsor_url = re.sub(r"\+sum\+", "+mbr+", bill_url)
    sponsor_soup = get_page_soup(sponsor_url, session)
    if isinstance(sponsor_soup, str):
        # If sponsor page fails, proceed with empty sponsors
        sponsor_soup = None

    house_sponsors = []
    senate_sponsors = []

    if sponsor_soup is not None:
        # House patrons
        house_matches = sponsor_soup.find_all(
            "h4", string=re.compile("HOUSE PATRONS")
        )
        for hm in house_matches:
            ul = hm.find_next("ul", {"class": "linkSect"})
            for li in ul.find_all("li"):
                house_sponsors.append(
                    normalize("NFKD", li.get_text().strip())
                )

        # Senate patrons
        senate_matches = sponsor_soup.find_all(
            "h4", string=re.compile("SENATE PATRONS")
        )
        for sm in senate_matches:
            ul = sm.find_next("ul", {"class": "linkSect"})
            for li in ul.find_all("li"):
                senate_sponsors.append(
                    normalize("NFKD", li.get_text().strip())
                )

    # Introducing sponsor = any patron with "chief patron" in name
    introducing_sponsor = "\n".join(
        s
        for s in house_sponsors + senate_sponsors
        if "chief patron" in s.lower()
    )
    house_sponsors_str = "; ".join(house_sponsors)
    senate_sponsors_str = "; ".join(senate_sponsors)

    bill_details = [
        bill_id,
        term,
        s_yr,
        session_name,
        descrip,
        introducing_sponsor,
        house_sponsors_str,
        senate_sponsors_str,
        summary,
        bill_url,
    ]

    return [bill_details, bill_history]


def _progress_path(bill_dir: Path, s_yr: int) -> Path:
    """Path to the incremental progress directory for a term."""
    return bill_dir / f".progress_{s_yr}_{s_yr + 1}"


def _save_bill_progress(
    progress_dir: Path, bill_url: str, details: list, history: list
):
    """Save one bill's scraped data to the progress directory."""
    # Use a sanitized filename based on bill URL
    safe_name = re.sub(r"[^\w]", "_", bill_url)
    with open(progress_dir / f"{safe_name}.json", "w") as f:
        json.dump({"details": details, "history": history}, f)


def _load_progress(progress_dir: Path) -> tuple[set, list, list]:
    """Load all previously scraped bills from the progress directory.

    Returns (scraped_urls, all_details_rows, all_history_rows).
    """
    scraped_urls: set[str] = set()
    all_details: list[list] = []
    all_history: list[list] = []

    if not progress_dir.exists():
        return scraped_urls, all_details, all_history

    for f in sorted(progress_dir.glob("*.json")):
        with open(f) as fh:
            data = json.load(fh)
        details = data["details"]
        history = data["history"]
        # The bill_url is the last element of the details row
        scraped_urls.add(details[-1])
        all_details.append(details)
        all_history.extend(history)

    return scraped_urls, all_details, all_history


def _cleanup_progress(progress_dir: Path):
    """Remove the progress directory after successful completion."""
    if progress_dir.exists():
        for f in progress_dir.glob("*.json"):
            f.unlink()
        progress_dir.rmdir()


def scrape(state: str, term: str, verbose: bool = False):
    """Main entry point for VA scraping.

    Saves progress incrementally per-bill so interrupted scrapes can resume.

    Args:
        state: "VA"
        term: e.g. "2024_2025" or "2022_2023"
        verbose: Enable verbose logging
    """
    # Parse term into start year
    parts = term.split("_")
    if len(parts) != 2:
        raise ValueError(f"Invalid term format: {term} (expected YYYY_YYYY)")
    s_yr = int(parts[0])

    repo_root = Path(__file__).parent.parent.parent
    bill_dir = repo_root / ".data" / state / "bill"

    # Check if already scraped (final CSVs exist)
    details_file = bill_dir / f"VA_Bill_Details_{s_yr}_{s_yr + 1}.csv"
    histories_file = bill_dir / f"VA_Bill_Histories_{s_yr}_{s_yr + 1}.csv"
    if details_file.exists():
        print(
            f"Skipping {s_yr}_{s_yr + 1}: "
            f"{details_file.name} already exists"
        )
        return

    http_session = _make_session()

    # Load any prior progress
    progress_dir = _progress_path(bill_dir, s_yr)
    progress_dir.mkdir(parents=True, exist_ok=True)
    scraped_urls, cached_details, cached_history = _load_progress(
        progress_dir
    )
    if scraped_urls:
        print(f"Resuming: {len(scraped_urls)} bills already cached")

    # Collect results (start with cached data)
    term_bill_details = [DETAILS_HEADER] + cached_details
    term_actions = [HISTORY_HEADER] + cached_history

    print(
        f"\n ------------------- Now Scraping the "
        f"{s_yr}-{s_yr + 1} Session ---------------------- \n"
    )

    # Get all bill URLs for the term
    term_urls = get_term_bills(s_yr, http_session, verbose)

    # Scrape each bill (skip already-cached ones)
    total = len(term_urls)
    for num, bill_row in enumerate(term_urls, 1):
        bill_url = bill_row[5]
        if bill_url in scraped_urls:
            print(f" ({num}/{total}) -- {bill_url} [cached]")
            continue

        bill_data = get_bill_data(bill_row, http_session)
        if bill_data == "HTTP Error":
            print(
                f" ********** \n ({num}/{total}) -- "
                f"{bill_row[0]} -- HTTP ERROR --- SKIPPING"
                f" \n **********"
            )
            continue

        # Save progress immediately
        _save_bill_progress(
            progress_dir, bill_url, bill_data[0], bill_data[1]
        )

        term_bill_details.append(bill_data[0])
        for action_row in bill_data[1]:
            term_actions.append(action_row)
        print(f" ({num}/{total}) -- {bill_url}")

    # Write final CSVs
    with open(details_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(term_bill_details)

    with open(histories_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(term_actions)

    # Clean up progress directory
    _cleanup_progress(progress_dir)

    print(
        f"\n\n\n ------------- {s_yr}-{s_yr + 1} Session "
        f"SCRAPED + DATA SAVED  -------------\n\n\n"
    )


def retry_errors(state: str, term: str, verbose: bool = False):
    """Re-scrape bills that got HTTP errors in a previous run.

    Reads the existing CSVs, re-fetches the bill listing to find URLs
    for missing bill IDs, then scrapes and appends them.
    Uses a 2s delay between requests to avoid rate limiting.
    """
    parts = term.split("_")
    s_yr = int(parts[0])

    repo_root = Path(__file__).parent.parent.parent
    bill_dir = repo_root / ".data" / state / "bill"
    details_file = bill_dir / f"VA_Bill_Details_{s_yr}_{s_yr + 1}.csv"
    histories_file = bill_dir / f"VA_Bill_Histories_{s_yr}_{s_yr + 1}.csv"

    if not details_file.exists():
        print(f"No existing file to retry: {details_file}")
        return

    # Read existing bill IDs
    import pandas as pd

    existing = pd.read_csv(details_file)
    existing_ids = set(existing["bill_id"].values)
    print(f"Existing bills: {len(existing_ids)}")

    # Get all bill URLs from listing pages
    http_session = _make_session()
    term_urls = get_term_bills(s_yr, http_session, verbose)

    # Find which bills are missing
    missing = []
    for bill_row in term_urls:
        bill_num = bill_row[0]
        bill_parts = bill_num.split(" ")
        bill_id = bill_parts[0] + bill_parts[1].zfill(4)
        if bill_id not in existing_ids:
            missing.append(bill_row)

    # Only retry HB and SB (resolutions are filtered out by the pipeline)
    missing_hb_sb = [
        r for r in missing
        if r[0].startswith("HB ") or r[0].startswith("SB ")
    ]
    missing_other = len(missing) - len(missing_hb_sb)

    print(
        f"\nMissing bills: {len(missing)} total "
        f"({len(missing_hb_sb)} HB/SB, {missing_other} resolutions)"
    )
    print(f"Retrying {len(missing_hb_sb)} HB/SB bills with 2s delay...\n")

    new_details = []
    new_history = []
    failed = []

    for num, bill_row in enumerate(missing_hb_sb, 1):
        time.sleep(1.5)  # extra delay on top of the 0.5s in get_page_soup
        bill_data = get_bill_data(bill_row, http_session)
        bill_id = bill_row[0].split(" ")[0] + bill_row[0].split(" ")[1].zfill(4)
        if bill_data == "HTTP Error":
            failed.append(bill_id)
            print(f"  ({num}/{len(missing_hb_sb)}) {bill_id} -- FAILED AGAIN")
        else:
            new_details.append(bill_data[0])
            new_history.extend(bill_data[1])
            print(f"  ({num}/{len(missing_hb_sb)}) {bill_id} -- OK")

    if not new_details:
        print("\nNo new bills recovered.")
        return

    # Append to existing CSVs
    with open(details_file, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(new_details)

    with open(histories_file, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(new_history)

    print(
        f"\nRecovered {len(new_details)} bills "
        f"({len(failed)} still failed: {failed})"
    )
    print(
        f"Updated: {details_file.name}, {histories_file.name}"
    )
