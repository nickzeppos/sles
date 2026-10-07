"""Arizona bill details and actions for one explicitly selected term.

Session metadata and bill data come from the public legislature APIs. A
headless browser selects each session on the official index. Connor's action
labels are retained, with unknown action codes preserved verbatim.
"""

from __future__ import annotations

from scrape.reporting import scrape_run, write_manifest

import csv
from datetime import date, datetime
import json
from pathlib import Path
import re
import time

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://apps.azleg.gov"
REPO_ROOT = Path(__file__).resolve().parents[2]
DETAILS_HEADER = [
    "bill_number", "session_num", "session", "bill_id_num", "title",
    "intro_sponsor", "primary_sponsors", "cosponsors", "keywords", "summary",
    "bill_json", "term", "session_year", "session_name", "now_title", "bill_url",
]
HISTORY_HEADER = [
    "bill_number", "session_num", "session", "chamber", "action_date", "action",
    "term", "order",
]
CHAMBERS = {"H": "House", "S": "Senate", "G": "Governor", "SS": "Secretary of State"}
ACTION_NAMES = {'DP': 'Do pass',
 'DPA': 'Do pass amended',
 'DPA/SE': 'Do pass amended/strike everything',
 'SE': 'Strike everything',
 'DPA/SE ON RECON': 'Do pass amended/strike everything on reconsideration',
 'DP ON REREFER': 'Do pass on rereferral',
 'DPA/SE CORRECTED': 'Do pass amended/strike everything',
 'DPA:C&P': 'Do pass amended/constitutional and in proper form',
 'DPA/SE ON REREF': 'Do pass amended/strike everything on rereferral',
 'DPA (CORRECTED)': 'Do pass amended',
 'DP ON RECON': 'Do pass on reconsideration',
 'Passed': 'Passed',
 'Failed': 'Failed',
 'PASSED': 'Passed',
 'FAILED': 'Failed',
 'PFC': 'Proper for consideration',
 'C&P': 'Constitutional and in proper form',
 'HELD': 'Held',
 'DISC/HELD': 'Discussed and held',
 'RETAINED': 'Retained',
 'RET ON CAL': 'Retained on the Calendar',
 'RET FOR CON': 'returned for consideration',
 'None': 'None',
 'W/D': 'Withdrawn',
 'PFC W/FL': 'Proper for consideration with floor amendment',
 'PFCA': 'Proper for consideration amended',
 'PFCA W/FL': 'Proper for consideration amended with floor ammendment',
 'DISC/ONLY': 'Discussion only',
 'DP/PFC': 'Do pass/proper for consideration',
 'DPA/PFC': 'Do pass amended/proper for consideration',
 'DP/PFC W/FL': 'Do pass/proper for consideration with floor amendment',
 'DP/C&P': 'Do pass/constitutional and in proper form',
 'DPA/PFC W/FL': 'Do pass amended/proper for consideration with floor '
                 'amendment',
 'AMEND C&P': 'Amended constitutional and in proper form',
 'HELD ON RECON': 'Held on reconsideration',
 'DPA ON RECON': 'Do pass amended on reconsideration',
 'NOT HEARD': 'Not heard',
 'DP/PFCA': 'Do pass/proper for consideration amended',
 'C&P ON RECON': 'Constitutional and in proper form on reconsideration',
 'AM C&P ON RECON': 'Amended C&P on reconsideration',
 'REMOVAL REQ': 'Removal request from rules committee',
 'DPA ON REREFER': 'Do pass amended on rereferral',
 'C&P ON REREF': 'Constitutional and in proper form on rereferral',
 'CONCUR': 'Recommend to concur',
 'CONCUR FAILED': 'Motion to concur failed',
 'FAILED ON RECON': 'Failed on reconsideration',
 'DNP': 'Do not pass',
 'DPA/C&P': 'Do pass amended constitutional and in proper form',
 'NOT CONCUR': 'Recommend not concur',
 'RULE 8J PROPER': 'proper legislation and not deemed derogatory or insulting',
 'REC REREF TO COM': 'Recommend rereferal to committee',
 'REREF JUD': 'Rereferred to judiciary committee',
 'S/C': 'subcommittee',
 'S/C REPORTED': 'Subcommittee reported',
 'FURTHER AMENDED': 'Further amended',
 'DISC PETITION': 'Discharge petition',
 'DISC/S/C': 'Discussed in subcommittee',
 'REREF WM': 'Rereferred to Ways and Means Committee',
 'AM C&P ON REREF': 'Amend C&P on rereferral',
 'HELD 1 WK': 'Held in committee 1 week',
 'REREF GOVOP': 'Rerefferred to gov. op. committee',
 'DP W/MIN RPT': 'Do pass with minority report',
 'HELD INDEF': 'Held in committee indefinitely',
 'C&P AS AM BY JU': 'constitutional and in proper form as amended',
 'C&P AS AM BY EN': 'constitutional and in proper form as amended',
 'C&P AS AM BY GO': 'constitutional and in proper form as amended',
 'C&P AS AM GOVOP': 'constitutional and in proper form as amended',
 'C&P AS AM BY TR': 'constitutional and in proper form as amended',
 'C&P AS AM BY WM': 'constitutional and in proper form as amended',
 'C&P AS AM BY HE': 'constitutional and in proper form as amended',
 'C&P AS AM BY APPR': 'constitutional and in proper form as amended',
 'C&P W/FL': 'constitutional and in proper form with floor amendment'}


def parse_term(term: str) -> tuple[int, int]:
    if not re.fullmatch(r"\d{4}_\d{4}", term):
        raise ValueError("AZ term must be YYYY_YYYY, for example 2025_2026")
    start, end = map(int, term.split("_"))
    if start % 2 != 1 or end != start + 1:
        raise ValueError("AZ term must start in an odd year and end the next year")
    if end > date.today().year:
        raise ValueError(f"AZ term {term} includes a future year")
    return start, end


def _make_session() -> requests.Session:
    http = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    http.mount("https://", HTTPAdapter(max_retries=retry))
    return http


def _get_json(http, path: str, **params):
    response = http.get(f"{BASE_URL}/api/{path}/", params=params, timeout=45)
    response.raise_for_status()
    data = response.json()
    if data is None:
        raise ValueError(f"Arizona API returned no data: {response.url}")
    return data


def select_sessions(records: list[dict], term: str) -> list[dict]:
    years = parse_term(term)
    selected = []
    for record in records:
        name = record["Name"]
        match = re.match(r"^(\d{4}) - .+ - (.+) Session$", name)
        # The API also exposes training/orientation sessions absent from the
        # public bill dropdown. Only legislative regular/special sessions apply.
        if match and int(match[1]) in years and re.search(r"\b(Regular|Special)$", match[2]):
            selected.append({**record, "year": int(match[1]), "label": term + "_" + match[2].replace(" ", "_")})
    if {row["year"] for row in selected} != set(years):
        raise ValueError(f"Arizona does not list sessions for both years of {term}")
    if len({row["SessionId"] for row in selected}) != len(selected):
        raise ValueError("Duplicate Arizona session IDs")
    return sorted(selected, key=lambda row: (row["year"], row["SessionId"]))


def parse_listing(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    numbers = [a.get_text(strip=True).replace(" ", "") for a in soup.select("a.faqLink")]
    if not numbers or len(numbers) != len(set(numbers)):
        raise ValueError("Arizona bill index is empty or contains duplicate numbers")
    # The same index includes miscellaneous floor motions, which Connor skipped.
    return [number for number in numbers if not number.startswith("M")]


def get_session_bills(session: dict) -> list[str]:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import Select, WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    try:
        driver.set_page_load_timeout(45)
        driver.get("https://www.azleg.gov/bills/")
        select = Select(driver.find_element(By.CLASS_NAME, "selectSession"))
        # Selection reloads the page. Wait for the old element to detach before
        # reading the new table; merely seeing the selected value is too early.
        if select.first_selected_option.get_attribute("value") != str(session["SessionId"]):
            old = select._el
            select.select_by_value(str(session["SessionId"]))
            WebDriverWait(driver, 30).until(EC.staleness_of(old))
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, "a.faqLink")))
        actual = Select(driver.find_element(By.CLASS_NAME, "selectSession")).first_selected_option.get_attribute("value")
        if actual != str(session["SessionId"]):
            raise ValueError(f"Arizona selected session {actual}, expected {session['SessionId']}")
        return parse_listing(driver.page_source)
    finally:
        driver.quit()


def _date(value) -> str:
    if not value:
        return ""
    value = str(value).split("T")[0]
    return (datetime.strptime(value, "%m/%d/%Y").date() if "/" in value else date.fromisoformat(value)).isoformat()


def parse_actions(bill: dict, number: str, session: dict, term: str) -> list[list]:
    actions = []
    def add(chamber, when, description):
        actions.append([number, session["SessionId"], session["label"], chamber, _date(when), description, term, len(actions) + 1])
    if bill.get("DateIntroduced"):
        add(CHAMBERS[number[0]], bill["DateIntroduced"], f"{number} Introduced")
    for transmit in bill.get("BodyTransmittedTo") or []:
        body = transmit["LegislativeBody"].strip()
        add("", transmit["TransmitDate"], f"Transmitted to {CHAMBERS.get(body, body)}")
    for chamber in ("House", "Senate"):
        for code, label in (("1st", "First"), ("2nd", "Second")):
            read, waived = bill.get(chamber + code + "Read"), bill.get(chamber + code + "Waived")
            if read:
                add(chamber, read, f"{label} Reading Complete")
            elif waived:
                add(chamber, waived, f"{label} Reading Waived")
    for status in bill.get("StandingCommittee") or []:
        committee = status["Committee"]
        body = committee["LegislativeBody"].strip()
        chamber = CHAMBERS.get(body, body)
        kind = "Subcommittee" if committee.get("IsSubCommittee") else "Committee"
        actor = f"{chamber} {kind}~{committee['CommitteeShortName']} ({committee['CommitteeName']})"
        add(chamber, status.get("AssignedDate"), f"Assigned to {actor}")
        if status.get("ReportDate"):
            add(chamber, status["ReportDate"], f"Reported {status.get('Action') or 'None'} from {actor}")
        elif status.get("DischargeDate"):
            add(chamber, status["DischargeDate"], f"Discharged from {actor}")
    for status in bill.get("BillStatusAction") or []:
        committee = status["Committee"]
        if committee["TypeName"] == "Standing":
            continue
        name = committee["CommitteeName"]
        body = committee["LegislativeBody"].strip()
        chamber = CHAMBERS.get(body, body)
        raw_action = status.get("Action") or "None"
        translated = ACTION_NAMES.get(raw_action, raw_action)
        when = status.get("ReportDate")
        if name in ("Third Reading", "Final Reading"):
            add(chamber, when, f"{translated} {name}")
        elif name == "Concurrence":
            continue
        elif "Committee of the Whole" in name:
            add(chamber, when, f"{name} ~ {raw_action} ({translated})")
        elif name == "Conference Committee":
            if status.get("AssignedDate"):
                add(chamber, status["AssignedDate"], f"Bill Assigned to {chamber} Conference Committee")
            if when:
                add(chamber, when, f"Bill Reported From {chamber} Conference Committee")
        elif committee["TypeName"] == "Floor" and "Motion" in name:
            add(chamber, when, f"{chamber} Floor Motion - {name}")
        elif when and raw_action != "None":
            add(chamber, when, f"{name} ~ {raw_action} ({translated})")
    governor_date = bill.get("GovernorActionDate")
    if not governor_date:
        dates = [t["TransmitDate"] for t in bill.get("BodyTransmittedTo") or [] if t.get("TransmitDate")]
        governor_date = max(dates) if dates else None
    if bill.get("GovernorAction") in ("Signed", "Vetoed"):
        add("Executive", governor_date, f"{bill['GovernorAction']} by Governor")
    if bill.get("ChapterNumber"):
        add("Executive", governor_date, f"LAW - Chapter Number {bill['ChapterNumber']}")
    # Preserve Connor's event construction but provide chronological order.
    actions.sort(key=lambda row: (row[4] or "9999", row[7]))
    for order, row in enumerate(actions, 1):
        row[7] = order
    return actions


def parse_bill(payload: dict, number: str, session: dict, term: str) -> tuple[list, list]:
    bill = payload["bill"]
    if bill["Number"] != number or bill["SessionId"] != session["SessionId"]:
        raise ValueError(f"Arizona bill/session mismatch for {number}")
    primary, cosponsors, introducing = [], [], ""
    for sponsor in payload["sponsors"]:
        name = sponsor["Legislator"]["MemberShortName"]
        kind = sponsor["SponsorType"]
        if kind == "Prime (1st Signer)":
            introducing = name
        (primary if "Prime" in kind else cosponsors).append(name)
    if not (bill.get("ShortTitle") or bill.get("Description")):
        raise ValueError(f"Arizona bill has no title: {number}")
    details = [
        number, session["SessionId"], session["label"], bill["BillId"], bill.get("ShortTitle") or "",
        introducing, "; ".join(primary), "; ".join(cosponsors),
        "; ".join(k["Keyword"] for k in payload["keywords"]), bill.get("Description") or "",
        f"{BASE_URL}/api/Bill/?billNumber={number}&sessionId={session['SessionId']}",
        term, session["year"], session["Name"], bill.get("NOWTitle") or "",
        f"{BASE_URL}/BillStatus/BillOverview/{bill['BillId']}",
    ]
    actions = parse_actions(bill, number, session, term)
    if bill.get("DateIntroduced") and not actions:
        raise ValueError(f"Arizona introduced bill has no actions: {number}")
    return details, actions


@scrape_run
def scrape(state: str, term: str, verbose: bool = False, force_fetch: bool = False):
    if state.upper() != "AZ":
        raise ValueError("Arizona scraper requires state AZ")
    parse_term(term)
    folder = REPO_ROOT / ".data/AZ/bill"
    detail_path = folder / f"AZ_Bill_Details_{term}.csv"
    history_path = folder / f"AZ_Bill_Histories_{term}.csv"
    manifest_path = folder / f".AZ_scrape_{term}.json"
    if all(p.exists() for p in (detail_path, history_path, manifest_path)) and not force_fetch:
        print(f"Skipping AZ {term}: completed outputs exist (use --force-fetch to refresh)")
        return
    cache = folder / ".cache" / term
    cache.mkdir(parents=True, exist_ok=True)
    details, histories, counts = [], [], {}
    with _make_session() as http:
        sessions = select_sessions(_get_json(http, "Session"), term)
        for session in sessions:
            bills = get_session_bills(session)
            counts[str(session["SessionId"])] = len(bills)
            print(f"AZ {session['Name']}: {len(bills)} bills/resolutions", flush=True)
            for i, number in enumerate(bills, 1):
                path = cache / f"{session['SessionId']}_{number}.json"
                if path.exists() and not force_fetch:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                else:
                    bill = _get_json(http, "Bill", billNumber=number, sessionId=session["SessionId"])
                    payload = {
                        "bill": bill,
                        "sponsors": _get_json(http, "BillSponsor", id=bill["BillId"]),
                        "keywords": _get_json(http, "Keyword", billStatusId=bill["BillId"]),
                    }
                    parsed = parse_bill(payload, number, session, term)
                    pending = path.with_suffix(".json.tmp")
                    pending.write_text(json.dumps(payload), encoding="utf-8")
                    pending.replace(path)
                    time.sleep(0.3)
                parsed = parse_bill(payload, number, session, term)
                details.append(parsed[0])
                histories.extend(parsed[1])
                if verbose or i % 25 == 0 or i == len(bills):
                    print(f"  {i}/{len(bills)} {number}: {len(parsed[1])} actions", flush=True)
    staged = []
    try:
        for path, header, rows in ((detail_path, DETAILS_HEADER, details), (history_path, HISTORY_HEADER, histories)):
            pending = path.with_suffix(".csv.tmp")
            staged.append((pending, path))
            with pending.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(header)
                writer.writerows(rows)
        manifest_path.unlink(missing_ok=True)
        for pending, path in staged:
            pending.replace(path)

        write_manifest(manifest_path, {
            "term": term, "details": len(details), "histories": len(histories), "session_counts": counts,
        })
    finally:
        for pending, _ in staged:
            pending.unlink(missing_ok=True)
    print(f"AZ {term}: saved {len(details):,} instruments and {len(histories):,} history rows")
