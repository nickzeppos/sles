"""California bills and resolutions from the legislature's PubInfo bulk archive.

Faithful port of scrape/legacy/connor/CA_State_Leg_PubInfo_Cleaning.R. The
legislature publishes its bill database as one zip per two-year session at
https://downloads.leginfo.legislature.ca.gov/ (pubinfo_2025.zip for 2025-26,
refreshed weekly while the session runs): tab-separated table dumps plus one
XML file per bill version. The archive is cached under .data/CA/bill/.cache/
and read in place; --force-fetch downloads it again.

Details and histories keep the R script's columns and its value formatting
(R's type conversion, NA, quoting; see utils/r_compat.py), so pubinfo_2023.zip
reproduces the retained CA_Bill_Details_23_24.csv and CA_Bill_Histories_23_24.csv
byte for byte.
"""

from __future__ import annotations

from scrape.http import make_session
from scrape.reporting import scrape_run, write_manifest
from utils import cache as cache_io
from utils import r_compat
from utils.validate_term import term_years

from datetime import datetime
import hashlib
from pathlib import Path
import re
import zipfile

from lxml import etree

BASE_URL = "https://downloads.leginfo.legislature.ca.gov"
REPO_ROOT = Path(__file__).resolve().parents[2]
CAML = "{http://lc.ca.gov/legalservices/schemas/caml.1#}"
DETAILS_HEADER = [
    "bill_num", "bill_id", "term", "chamber", "title", "lead_author", "authors",
    "coauthors", "topic", "status", "chapter_num", "location",
]
HISTORY_HEADER = [
    "bill_num", "bill_id", "hist_id", "term", "chamber", "date", "order", "action",
    "action_code", "location", "end_status",
]

def read_table(archive: zipfile.ZipFile, name: str) -> list[list]:
    """Columns of one PubInfo table as the R script reads them: read.delim(header=FALSE,
    quote="`") then mutate_all(NULL -> NA), V1 = [0]."""
    columns = r_compat.read_delim(archive.read(name).decode("utf-8"), name, quote="`")
    return [[None if value == "NULL" else value for value in column] for column in columns]


# Bill version XML: xml_to_df() in the R script

def _clean(text: str) -> str:
    text = re.sub(" +", " ", re.sub("NULL|\n", " ", text))
    return re.sub(r" \.\Z", "", r_compat.str_trim(text))


def _description(data: bytes, source: str):
    description = etree.fromstring(data).find(CAML + "Description")
    if description is None:
        raise ValueError(f"{source}: no caml:Description (pre-1999 layout is not ported)")
    return description


def parse_version(data: bytes, source: str) -> dict:
    description = _description(data, source)
    info = description.find(CAML + "LegislativeInfo")
    session, kind, number = (info.findtext(CAML + tag) for tag in ("SessionNum", "MeasureType", "MeasureNum"))
    return {
        "source": source,
        "vf_bill_id": description.findtext(CAML + "Id"),
        "bill_num": f"{kind}-{number}" if float(session) == 0 else f"{kind}X{session}-{number}",
        "version_num": description.findtext(CAML + "VersionNum"),
    }


def parse_text(data: bytes, source: str) -> tuple[str, str]:
    """Title and topic. Only the kept version's are used, as in the R script,
    so amended versions' revision markup never needs interpreting."""
    description = _description(data, source)
    subject = description.find(CAML + "GeneralSubject")
    return (_clean(r_compat.paste_xml_list(description.find(CAML + "Title"), source)),
            _clean(r_compat.paste_xml_list(subject.find(CAML + "Subject") if subject is not None else None, source)))


def parse_archive(archive: zipfile.ZipFile, term: str, verbose: bool = False):
    """Details rows, history rows and source findings, as the R script builds them."""
    bills = read_table(archive, "BILL_TBL.dat")
    bill_ids, years, sessions, kinds, numbers = bills[0], bills[1], bills[2], bills[3], bills[4]
    chapters, locations, statuses = bills[9], bills[16], bills[17]
    bill_nums, terms, chambers = [], [], []
    for year, session, kind, number in zip(years, sessions, kinds, numbers):
        if session is None:
            bill_num = None
        elif session == 0 or session == "0":
            bill_num = f"{r_compat.as_character(kind)}-{r_compat.as_character(number)}"
        else:
            bill_num = f"{r_compat.as_character(kind)}X{r_compat.as_character(session)}-{r_compat.as_character(number)}"
        year = r_compat.as_character(year)
        bill_nums.append(bill_num)
        terms.append(f"{year[:4]}_{year[4:8]}" if year != "NA" else "NA_NA")
        chambers.append(None if bill_num is None else "H" if bill_num[0] in "AH" else bill_num[0])
    if set(terms) != {term}:
        raise ValueError(f"BILL_TBL.dat sessions {sorted(set(terms))} do not match term {term}")

    names = sorted(name for name in archive.namelist() if "BILL_VERSION_TBL_" in name)
    versions = {}
    for count, name in enumerate(names, 1):
        version = parse_version(archive.read(name), name)
        versions.setdefault(version["bill_num"], []).append(version)
        if verbose and count % 2000 == 0:
            print(f"  {count:,}/{len(names):,} bill versions", flush=True)
    kept = {}
    for bill_num, rows in versions.items():
        top = max(row["version_num"] for row in rows)
        kept[bill_num] = [row for row in rows if row["version_num"] == top]
        for row in kept[bill_num]:
            row["title"], row["topic"] = parse_text(archive.read(row["source"]), row["source"])

    authors = read_table(archive, "BILL_VERSION_AUTHORS_TBL.dat")
    credits = {}
    for version_id, house, name, role, primary in zip(authors[0], authors[2], authors[3], authors[4], authors[9]):
        house = {"ASSEMBLY": "H", "SENATE": "S"}.get(house, house)
        label = f"{r_compat.as_character(name)} ({r_compat.as_character(house)})"
        lead, listed, co = credits.setdefault(version_id, ([], [], []))
        if primary == "Y":
            lead.append(label)
        if role == "LEAD_AUTHOR":
            listed.append(label)
        elif role in ("PRINCIPAL_COAUTHOR", "COAUTHOR"):
            co.append(label)

    details = []
    for index, bill_num in enumerate(bill_nums):
        for version in kept.get(bill_num) or [None]:
            vf_id = version["vf_bill_id"] if version else None
            key = re.sub(r"_+0+|_+", "", vf_id) if vf_id is not None else None
            names_ = credits.get(key)
            lead, listed, co = ("; ".join(part) for part in names_) if names_ else (None, None, None)
            details.append([
                bill_num, bill_ids[index], terms[index], chambers[index],
                version["title"] if version else None, lead, listed, co,
                version["topic"] if version else None,
                statuses[index], chapters[index], locations[index],
            ])

    history = read_table(archive, "BILL_HISTORY_TBL.dat")
    by_id = {}
    for bill_id, bill_num, bill_term in zip(bill_ids, bill_nums, terms):
        by_id.setdefault(bill_id, []).append((bill_num, bill_term))
    dates = r_compat.as_date(history[2])
    histories = []
    for row, bill_id in enumerate(history[0]):
        for bill_num, bill_term in by_id.get(bill_id) or [(None, None)]:
            histories.append([
                bill_num, bill_id, history[1][row], bill_term, history[9][row], dates[row],
                history[6][row], history[3][row], history[7][row], history[10][row], history[12][row],
            ])

    findings = {
        "source_missing_text": [r[0] for r in details if r[4] is None],
        "source_missing_primary_authors": [r[0] for r in details if not r[5]],
        "source_missing_measures": sorted({bill_id for bill_id in history[0] if bill_id not in by_id}),
    }
    return details, histories, len(names), findings


def _download(http, url: str, path: Path) -> None:
    pending = path.with_suffix(".zip.tmp")
    try:
        with http.get(url, stream=True, timeout=120) as response:
            response.raise_for_status()
            with pending.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    handle.write(chunk)
        with zipfile.ZipFile(pending) as archive:
            archive.getinfo("BILL_TBL.dat")
        cache_io.replace(pending, path)
    finally:
        cache_io.unlink(pending, missing_ok=True)


@scrape_run
def scrape(state: str, term: str, verbose: bool = False, force_fetch: bool = False):
    if state.upper() != "CA":
        raise ValueError("California scraper requires state CA")
    year = term_years("CA", term)[0]
    folder = REPO_ROOT / ".data" / "CA" / "bill"
    detail_path = folder / f"CA_Bill_Details_{term}.csv"
    history_path = folder / f"CA_Bill_Histories_{term}.csv"
    manifest_path = folder / f".CA_scrape_{term}.json"
    if all(cache_io.exists(p) for p in (detail_path, history_path, manifest_path)) and not force_fetch:
        print(f"Skipping CA {term}: completed outputs exist (use --force-fetch to refresh)")
        return
    url = f"{BASE_URL}/pubinfo_{year}.zip"
    archive_path = folder / ".cache" / f"pubinfo_{year}.zip"
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    if force_fetch or not archive_path.exists():
        print(f"CA {term}: downloading {url}", flush=True)
        with make_session() as http:
            _download(http, url, archive_path)
    digest = hashlib.sha256()
    with archive_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    with zipfile.ZipFile(archive_path) as archive:
        exported = datetime(*archive.getinfo("BILL_TBL.dat").date_time).isoformat()
        print(f"CA {term}: {archive_path.name} exported {exported}", flush=True)
        details, histories, version_files, findings = parse_archive(archive, term, verbose)
    staged = []
    try:
        for path, header, rows in ((detail_path, DETAILS_HEADER, details), (history_path, HISTORY_HEADER, histories)):
            pending = path.with_suffix(".csv.tmp")
            staged.append((pending, path))
            r_compat.write_csv(pending, header, rows)
        cache_io.unlink(manifest_path, missing_ok=True)
        for pending, path in staged:
            cache_io.replace(pending, path)

        write_manifest(manifest_path, {
            "term": term, "source": url, "archive_exported": exported,
            "archive_bytes": archive_path.stat().st_size, "archive_sha256": digest.hexdigest(),
            "details": len(details), "histories": len(histories), "version_files": version_files,
            **findings,
        })
    finally:
        for pending, _ in staged:
            cache_io.unlink(pending, missing_ok=True)
    print(f"CA {term}: saved {len(details):,} bills and resolutions and {len(histories):,} history rows")
