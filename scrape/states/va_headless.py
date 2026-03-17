"""
Virginia Bill Scraper — New LIS Website (Headless)

Scrapes bill details and histories from the new Virginia LIS SPA:
    https://lis.virginia.gov/bill-details/{session_code}/{bill_id}

Uses Playwright (headless Chromium) since the new site is a React SPA
that loads data dynamically via JavaScript.

Strategy:
    1. Navigate to the bill search page and intercept the API response
       from GetLegislationIdsListAsync to discover all bill numbers.
    2. For each missing bill, navigate to its detail page and intercept
       the API responses (bill details, patrons, history) — no DOM
       parsing needed.
    3. Append scraped data to the existing CSVs.

URL structure:
    session_code = YYYY + session_number (e.g., 20251 = 2025 RS,
                                          20242 = 2024 SS1)
    bill_id = prefix + number (e.g., HB2021, SB1021)

Output format matches the old scraper (va.py) exactly so the
estimation pipeline can consume both interchangeably.
"""

from __future__ import annotations

import base64
import csv
import json
import re
import time
from pathlib import Path
from unicodedata import normalize

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page

# CSV column headers (must match old scraper)
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

# Map session_number to session name suffix
SESSION_NAMES = {
    1: "SESSION",
    2: "SPECIAL SESSION I",
    3: "SPECIAL SESSION II",
    4: "SPECIAL SESSION III",
    5: "SPECIAL SESSION IV",
}

# LIS internal session IDs (discovered via API interception).
# Map (year, session_num) -> LIS sessionID integer.
# These are needed for the search API. Add new entries as needed.
LIS_SESSION_IDS = {
    (2024, 1): 55,   # 2024 Regular Session
    (2024, 2): 56,   # 2024 Special Session I
    (2025, 1): 57,   # 2025 Regular Session
}

# Map API ActorType values to old-scraper chamber names
ACTOR_TYPE_MAP = {
    "House": "House",
    "Senate": "Senate",
    "Governor": "Governor",
}


def _session_code(year: int, session_num: int) -> str:
    """Build the LIS URL session code (e.g., '20251')."""
    return f"{year}{session_num}"


def _session_name(year: int, session_num: int) -> str:
    """Build the human-readable session name."""
    suffix = SESSION_NAMES.get(
        session_num, f"SPECIAL SESSION {session_num}"
    )
    return f"{year} {suffix}"


def _bill_id_pad(prefix: str, number: int) -> str:
    """Zero-pad a bill number to 4 digits with prefix."""
    return f"{prefix}{str(number).zfill(4)}"


def _parse_api_date(iso_date: str) -> str:
    """Convert ISO datetime (2025-01-06T13:48:00) to YYYY-MM-DD."""
    if not iso_date:
        return ""
    return iso_date[:10]


def _extract_summary_text(html_summary: str) -> str:
    """Extract plain text from HTML summary."""
    if not html_summary:
        return ""
    soup = BeautifulSoup(html_summary, "lxml")
    return (
        soup.get_text()
        .replace("\r\n", " ")
        .replace("\n", " ")
        .strip()
    )


def discover_bill_numbers(
    page: Page,
    lis_session_id: int,
    verbose: bool = False,
) -> list[str]:
    """Get all bill numbers for a session via API interception.

    Navigates to the bill search page with a query that triggers
    GetLegislationIdsListAsync and captures the response.

    Returns list of bill numbers like ["HB9", "HB19", ..., "SB1495"].
    """
    captured = [None]

    def on_response(response):
        if "GetLegislationIdsListAsync" in response.url:
            ct = response.headers.get("content-type", "")
            if "json" in ct:
                try:
                    captured[0] = response.json()
                except Exception:
                    pass

    page.on("response", on_response)

    query = json.dumps({
        "selectedBillNumbers": "",
        "selectedKeywords": "",
        "selectedSession": lis_session_id,
        "selectedChapterNumber": "",
        "includeFailed": True,
        "SortBy": "Bill|ASC",
    })
    encoded = base64.b64encode(query.encode()).decode()
    url = f"https://lis.virginia.gov/bill-search?q={encoded}"

    page.goto(url, timeout=30000)
    page.wait_for_load_state("networkidle", timeout=15000)
    page.wait_for_timeout(3000)

    page.remove_listener("response", on_response)

    if not captured[0]:
        if verbose:
            print("  Warning: no API response captured")
        return []

    data = captured[0]
    if not data.get("Success"):
        if verbose:
            print(f"  API error: {data.get('FailureMessage')}")
        return []

    ids = data.get("LegislationIds", [])
    bill_numbers = [
        entry["LegislationNumber"] for entry in ids
    ]

    if verbose:
        print(f"  Found {len(bill_numbers)} bills in session")

    return bill_numbers


def scrape_bill(
    page: Page,
    session_code: str,
    bill_number_raw: str,
    year: int,
    session_num: int,
    term: str,
    verbose: bool = False,
) -> tuple[list, list[list]] | str:
    """Scrape a single bill via API interception.

    Navigates to the bill detail page and intercepts the JSON API
    responses for bill details, patrons, and history.

    Returns (details_row, history_rows) or "Error" on failure.
    """
    prefix = re.sub(r"\d.*", "", bill_number_raw)
    number = int(re.sub(r"^[A-Z]+", "", bill_number_raw))
    bill_id = _bill_id_pad(prefix, number)
    session_name_str = _session_name(year, session_num)

    url = (
        f"https://lis.virginia.gov"
        f"/bill-details/{session_code}/{bill_number_raw}"
    )

    # Set up API interception
    api = {"bill": None, "patrons": None, "events": None}

    def on_response(response):
        resp_url = response.url
        ct = response.headers.get("content-type", "")
        if "json" not in ct:
            return
        try:
            if "GetLegislationListAsync" in resp_url:
                api["bill"] = response.json()
            elif "GetLegislationPatronsByIdAsync" in resp_url:
                api["patrons"] = response.json()
            elif (
                "GetLegislationEventByLegislationIDAsync"
                in resp_url
            ):
                api["events"] = response.json()
        except Exception:
            pass

    page.on("response", on_response)

    try:
        page.goto(url, timeout=20000)
        page.wait_for_load_state("networkidle", timeout=15000)
        page.wait_for_timeout(1500)
    except Exception as e:
        page.remove_listener("response", on_response)
        if verbose:
            print(f"    Error loading {url}: {e}")
        return "Error"

    page.remove_listener("response", on_response)

    # Check if valid bill page loaded (not search redirect)
    title = page.title()
    if "Bill Search" in title:
        return "Error"

    # --- Extract bill details from API ---
    bill_data = api.get("bill")
    if not bill_data or not bill_data.get("Legislations"):
        if verbose:
            print(f"    No bill API data for {bill_id}")
        return "Error"

    leg = bill_data["Legislations"][0]

    # Short title: "HB 1876 <description>"
    description = (leg.get("Description") or "").strip()
    short_title = (
        f"{prefix} {number} {description}"
    )

    # Summary
    summary = _extract_summary_text(
        leg.get("LegislationSummary", "")
    )

    # --- Extract patrons from API ---
    patron_data = api.get("patrons")
    patrons = (
        patron_data.get("Patrons", []) if patron_data else []
    )

    # Find introducing sponsor (chief patron)
    sponsor = ""
    for p in patrons:
        if p.get("IsIntroducing") or p.get("Name") == "Chief Patron":
            name = normalize(
                "NFKD", p.get("MemberDisplayName", "")
            )
            display = p.get("DisplayName", "")
            if display:
                sponsor = f"{name} ({p['Name']})"
            else:
                sponsor = name
            break

    # If no introducing patron found, use first patron
    if not sponsor and patrons:
        p = patrons[0]
        name = normalize(
            "NFKD", p.get("MemberDisplayName", "")
        )
        sponsor = f"{name} ({p.get('Name', '')})"

    # Build house and senate sponsor lists
    house_sponsors = []
    senate_sponsors = []
    for p in patrons:
        name = normalize(
            "NFKD", p.get("MemberDisplayName", "")
        )
        display_name = p.get("DisplayName", "")
        if display_name:
            full = f"{name} {display_name}"
        else:
            full = name

        chamber_code = p.get("ChamberCode", "")
        if chamber_code == "H":
            house_sponsors.append(full)
        elif chamber_code == "S":
            senate_sponsors.append(full)

    house_sponsors_str = "; ".join(house_sponsors)
    senate_sponsors_str = "; ".join(senate_sponsors)

    # --- Extract history from API ---
    bill_history = []
    event_data = api.get("events")
    events = (
        event_data.get("LegislationEvents", [])
        if event_data else []
    )

    # Sort events by date and sequence
    events.sort(
        key=lambda e: (
            e.get("EventDate", ""),
            e.get("Sequence", 0),
        )
    )

    order = 1
    for evt in events:
        if not evt.get("IsPublic", True):
            continue

        action_date = _parse_api_date(
            evt.get("EventDate", "")
        )
        chamber = ACTOR_TYPE_MAP.get(
            evt.get("ActorType", ""), evt.get("ActorType", "")
        )
        action = (
            (evt.get("Description") or "")
            .replace("\r\n", "")
            .replace("\n", " ")
            .strip()
        )

        # Build action detail URL from references if available
        action_url = ""
        refs = evt.get("EventReferences", [])
        for ref in refs:
            ref_type = ref.get("ActionReferenceType", "")
            ref_id = ref.get("ReferenceID")
            if ref_type == "Committee" and ref_id:
                action_url = (
                    f"https://lis.virginia.gov"
                    f"/session-details/{session_code}"
                    f"/committee-information"
                )
                break
            elif ref_type == "VoteTally" and ref_id:
                action_url = (
                    f"https://lis.virginia.gov"
                    f"/bill-details/{session_code}"
                    f"/{bill_number_raw}"
                )
                break

        bill_history.append([
            bill_id,
            term,
            year,
            session_name_str,
            action_date,
            chamber,
            action,
            order,
            action_url,
        ])
        order += 1

    details_row = [
        bill_id,
        term,
        year,
        session_name_str,
        short_title,
        sponsor,
        house_sponsors_str,
        senate_sponsors_str,
        summary,
        url,
    ]

    return (details_row, bill_history)


def _progress_path(bill_dir: Path, s_yr: int) -> Path:
    """Path to the incremental progress directory."""
    return bill_dir / f".progress_headless_{s_yr}_{s_yr + 1}"


def _save_bill_progress(
    progress_dir: Path, bill_id: str, details: list, history: list
):
    """Save one bill's scraped data to the progress directory."""
    with open(progress_dir / f"{bill_id}.json", "w") as f:
        json.dump({"details": details, "history": history}, f)


def _load_progress(
    progress_dir: Path,
) -> tuple[set, list, list]:
    """Load previously scraped bills from the progress directory."""
    scraped_ids: set[str] = set()
    all_details: list[list] = []
    all_history: list[list] = []

    if not progress_dir.exists():
        return scraped_ids, all_details, all_history

    for f in sorted(progress_dir.glob("*.json")):
        with open(f) as fh:
            data = json.load(fh)
        details = data["details"]
        history = data["history"]
        scraped_ids.add(details[0])  # bill_id
        all_details.append(details)
        all_history.extend(history)

    return scraped_ids, all_details, all_history


def _cleanup_progress(progress_dir: Path):
    """Remove the progress directory after successful completion."""
    if progress_dir.exists():
        for f in progress_dir.glob("*.json"):
            f.unlink()
        progress_dir.rmdir()


def retry_missing(
    state: str, term: str, verbose: bool = False
):
    """Scrape bills missing from the existing CSVs.

    Reads the existing CSV to find already-scraped bill IDs, then
    discovers all bills via the LIS search API and scrapes any
    missing HB/SB bills from the new LIS site.
    """
    parts = term.split("_")
    s_yr = int(parts[0])
    term_dash = f"{s_yr}-{s_yr + 1}"

    repo_root = Path(__file__).parent.parent.parent
    bill_dir = repo_root / ".data" / state / "bill"
    details_file = (
        bill_dir / f"VA_Bill_Details_{s_yr}_{s_yr + 1}.csv"
    )
    histories_file = (
        bill_dir / f"VA_Bill_Histories_{s_yr}_{s_yr + 1}.csv"
    )

    if not details_file.exists():
        print(f"No existing file: {details_file}")
        return

    # Read existing bill IDs
    existing_ids = set()
    with open(details_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing_ids.add(row["bill_id"])
    print(f"Existing bills in CSV: {len(existing_ids)}")

    # Load any prior headless progress
    progress_dir = _progress_path(bill_dir, s_yr)
    progress_dir.mkdir(parents=True, exist_ok=True)
    cached_ids, cached_details, cached_history = _load_progress(
        progress_dir
    )
    if cached_ids:
        print(f"Resuming: {len(cached_ids)} bills already cached")
        existing_ids.update(cached_ids)

    # Determine which sessions to check
    sessions_to_check = []
    for yr in [s_yr, s_yr + 1]:
        for sn in range(1, 6):
            if (yr, sn) in LIS_SESSION_IDS:
                sessions_to_check.append((yr, sn))

    if not sessions_to_check:
        print(
            "No LIS session IDs configured for this term. "
            "Add entries to LIS_SESSION_IDS in va_headless.py."
        )
        return

    new_details = list(cached_details)
    new_history = list(cached_history)
    total_found = 0
    total_scraped = len(cached_ids)
    errors = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for yr, sn in sessions_to_check:
            lis_sid = LIS_SESSION_IDS[(yr, sn)]
            sc = _session_code(yr, sn)
            sname = _session_name(yr, sn)

            print(f"\nDiscovering bills for {sname}...")
            all_numbers = discover_bill_numbers(
                page, lis_sid, verbose
            )

            # Filter to HB/SB only
            hb_sb = [
                n for n in all_numbers
                if n.startswith("HB") or n.startswith("SB")
            ]

            # Find missing bills
            missing = []
            for raw_num in hb_sb:
                pfx = re.sub(r"\d.*", "", raw_num)
                num = int(re.sub(r"^[A-Z]+", "", raw_num))
                padded = _bill_id_pad(pfx, num)
                if padded not in existing_ids:
                    missing.append(raw_num)

            total_found += len(missing)
            if not missing:
                print(
                    f"  All {len(hb_sb)} HB/SB bills "
                    f"already scraped"
                )
                continue

            print(
                f"  {len(missing)} missing HB/SB bills "
                f"(of {len(hb_sb)} total)"
            )

            # Scrape missing bills
            for i, raw_num in enumerate(missing, 1):
                pfx = re.sub(r"\d.*", "", raw_num)
                num = int(re.sub(r"^[A-Z]+", "", raw_num))
                bid = _bill_id_pad(pfx, num)

                if bid in cached_ids:
                    continue

                result = scrape_bill(
                    page, sc, raw_num, yr, sn,
                    term_dash, verbose
                )
                if result == "Error":
                    errors += 1
                    print(
                        f"  ({i}/{len(missing)}) {bid} -- ERROR"
                    )
                    continue

                details, history = result
                new_details.append(details)
                new_history.extend(history)
                total_scraped += 1

                # Save progress incrementally
                _save_bill_progress(
                    progress_dir, bid, details, history
                )

                if verbose or i % 50 == 0 or i == len(missing):
                    print(
                        f"  ({i}/{len(missing)}) {bid} -- OK"
                    )

                # Small delay between requests
                time.sleep(0.3)

        browser.close()

    if not new_details:
        print("\nNo new bills found to scrape.")
        _cleanup_progress(progress_dir)
        return

    # Append to existing CSVs (only the non-cached ones)
    # Re-read progress to get all scraped bills
    final_ids, final_details, final_history = _load_progress(
        progress_dir
    )

    with open(details_file, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(final_details)

    with open(histories_file, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(final_history)

    _cleanup_progress(progress_dir)

    print(
        f"\nDone! Scraped {total_scraped} new bills "
        f"({total_found} found missing, {errors} errors)"
    )
    print(f"  Appended to {details_file.name}")
    print(f"  Appended to {histories_file.name}")
