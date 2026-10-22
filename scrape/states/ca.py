"""California bills and resolutions from the legislature's PubInfo bulk archive.

Faithful port of scrape/legacy/connor/CA_State_Leg_PubInfo_Cleaning.R. The
legislature publishes its bill database as one zip per two-year session at
https://downloads.leginfo.legislature.ca.gov/ (pubinfo_2025.zip for 2025-26,
refreshed weekly while the session runs): tab-separated table dumps plus one
XML file per bill version. The archive is cached under .data/CA/bill/.cache/
and read in place; --force-fetch downloads it again.

Details and histories keep the R script's columns and its value formatting
(R's type conversion, NA, quoting), so pubinfo_2023.zip reproduces the retained
CA_Bill_Details_23_24.csv and CA_Bill_Histories_23_24.csv byte for byte.
"""

from __future__ import annotations

from scrape.http import make_session
from scrape.reporting import scrape_run, write_manifest
from utils import cache as cache_io
from utils.validate_term import term_years

import csv
from datetime import date, datetime
import hashlib
import io
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

LOGICAL = {"T": True, "TRUE": True, "true": True, "True": True,
           "F": False, "FALSE": False, "false": False, "False": False}
INTEGER = re.compile(r"[+-]?\d+")
DOUBLE = re.compile(r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?")
# stringr::str_trim removes Unicode White_Space, not Python's wider isspace set.
WHITE_SPACE = "".join(map(chr, [0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20, 0x85, 0xA0, 0x1680,
                                *range(0x2000, 0x200B), 0x2028, 0x2029, 0x202F, 0x205F, 0x3000]))
TRIM = re.compile(f"^[{re.escape(WHITE_SPACE)}]+|[{re.escape(WHITE_SPACE)}]+$")


# R semantics: read.delim(header=FALSE, quote="`") then mutate_all(NULL -> NA)

def _type_convert(values: list[str]) -> list:
    """type.convert(as.is=TRUE): logical, integer, double, else character."""
    present = [v for v in values if v not in ("", "NA")]
    if all(v in LOGICAL for v in present):
        return [LOGICAL.get(v) for v in values]
    if all(INTEGER.fullmatch(v) and abs(int(v)) <= 2147483647 for v in present):
        return [int(v) if v not in ("", "NA") else None for v in values]
    if all(DOUBLE.fullmatch(v) for v in present):
        return [float(v) if v not in ("", "NA") else None for v in values]
    return [None if v in ("NA", "NULL") else v for v in values]


def read_table(archive: zipfile.ZipFile, name: str) -> list[list]:
    """Columns of one PubInfo table, typed as the R script saw them (V1 = [0])."""
    text = archive.read(name).decode("utf-8")
    rows = [row for row in csv.reader(io.StringIO(text, newline=""), delimiter="\t", quotechar="`") if row]
    widths = {len(row) for row in rows}
    if len(widths) != 1:
        raise ValueError(f"{name}: rows have differing field counts {sorted(widths)}")
    return [_type_convert(list(column)) for column in zip(*rows)]


def r_str(value) -> str:
    """as.character, as paste0 uses it."""
    if value is None:
        return "NA"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float):
        raise ValueError(f"non-integer numeric value {value!r}: R formatting not ported")
    return str(value)


def r_csv_field(value) -> str:
    """write.csv: character quoted with doubled quotes, NA and numbers bare."""
    if value is None:
        return "NA"
    if isinstance(value, str):
        return '"' + value.replace('"', '""') + '"'
    return r_str(value)


def write_r_csv(path: Path, header: list[str], rows: list[list]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write(",".join(r_csv_field(name) for name in header) + "\n")
        for row in rows:
            handle.write(",".join(r_csv_field(value) for value in row) + "\n")


# Bill version XML: xml_to_df() in the R script

def _node_text(text: str | None) -> str | None:
    """Text node as xmlTreeParse keeps it: blank nodes dropped, ends trimmed."""
    if text is None or not text.strip(" \t\n\r"):
        return None
    return text.strip(" \t\n\r")


def _r_paste(element, source: str) -> str:
    """xmlToList(element) collapsed by paste0(collapse = " "); NULL becomes ""."""
    if element is None:
        return ""
    items = [_node_text(element.text)]
    for child in element:
        if not isinstance(child.tag, str) or len(child):
            raise ValueError(f"{source}: unsupported markup inside {etree.QName(element).localname}")
        text = _node_text(child.text)
        if child.attrib and text is not None or len(child.attrib) > 1:
            raise ValueError(f"{source}: unsupported attributes inside {etree.QName(element).localname}")
        # Empty <span class="ThinSpace"/> lists as its attribute; an empty element as NULL.
        items.append(text if text is not None else next(iter(child.attrib.values()), "NULL"))
        items.append(_node_text(child.tail))
    return " ".join(item for item in items if item is not None)


def _clean(text: str) -> str:
    text = re.sub(" +", " ", re.sub("NULL|\n", " ", text))
    return re.sub(r" \.\Z", "", TRIM.sub("", text))


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
    return (_clean(_r_paste(description.find(CAML + "Title"), source)),
            _clean(_r_paste(subject.find(CAML + "Subject") if subject is not None else None, source)))


def _r_date(values: list) -> list:
    """format(as.Date(x), "%Y-%m-%d"), with the format chosen from the first value."""
    first = next((v for v in values if v is not None), None)
    if first is None:
        return values
    for pattern in (r"(\d{4})-(\d{1,2})-(\d{1,2})", r"(\d{4})/(\d{1,2})/(\d{1,2})"):
        if re.match(pattern, first):
            break
    else:
        raise ValueError(f"history date {first!r} is not in a standard unambiguous format")
    result = []
    for value in values:
        match = re.match(pattern, value) if value is not None else None
        try:
            result.append(date(*map(int, match.groups())).isoformat() if match else None)
        except ValueError:
            result.append(None)
    return result


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
            bill_num = f"{r_str(kind)}-{r_str(number)}"
        else:
            bill_num = f"{r_str(kind)}X{r_str(session)}-{r_str(number)}"
        year = r_str(year)
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
        label = f"{r_str(name)} ({r_str(house)})"
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
    dates = _r_date(history[2])
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
            write_r_csv(pending, header, rows)
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
