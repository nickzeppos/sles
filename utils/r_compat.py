"""R behavior that ports of R scripts reproduce, so their output matches R's.

Each function names the R call it follows. scrape/states/ca.py uses them to
reproduce the CSVs of Connor's PubInfo cleaning script byte for byte.
"""

from __future__ import annotations

import csv
from datetime import date
import io
from pathlib import Path
import re

from lxml import etree

LOGICAL = {"T": True, "TRUE": True, "true": True, "True": True,
           "F": False, "FALSE": False, "false": False, "False": False}
INTEGER = re.compile(r"[+-]?\d+")
DOUBLE = re.compile(r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?")
# stringr::str_trim removes Unicode White_Space, not Python's wider isspace set.
WHITE_SPACE = "".join(map(chr, [0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20, 0x85, 0xA0, 0x1680,
                                *range(0x2000, 0x200B), 0x2028, 0x2029, 0x202F, 0x205F, 0x3000]))
TRIM = re.compile(f"^[{re.escape(WHITE_SPACE)}]+|[{re.escape(WHITE_SPACE)}]+$")


def str_trim(text: str) -> str:
    """stringr::str_trim(text)."""
    return TRIM.sub("", text)


def type_convert(values: list[str]) -> list:
    """type.convert(as.is=TRUE): logical, integer, double, else character."""
    present = [v for v in values if v not in ("", "NA")]
    if all(v in LOGICAL for v in present):
        return [LOGICAL.get(v) for v in values]
    if all(INTEGER.fullmatch(v) and abs(int(v)) <= 2147483647 for v in present):
        return [int(v) if v not in ("", "NA") else None for v in values]
    if all(DOUBLE.fullmatch(v) for v in present):
        return [float(v) if v not in ("", "NA") else None for v in values]
    return [None if v == "NA" else v for v in values]


def read_delim(text: str, source: str, quote: str = '"') -> list[list]:
    """read.delim(header=FALSE, quote=quote): typed columns, V1 = [0]."""
    rows = [row for row in csv.reader(io.StringIO(text, newline=""), delimiter="\t", quotechar=quote) if row]
    widths = {len(row) for row in rows}
    if len(widths) != 1:
        raise ValueError(f"{source}: rows have differing field counts {sorted(widths)}")
    return [type_convert(list(column)) for column in zip(*rows)]


def as_character(value) -> str:
    """as.character(value), as paste0 uses it."""
    if value is None:
        return "NA"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float):
        raise ValueError(f"non-integer numeric value {value!r}: R formatting not ported")
    return str(value)


def csv_field(value) -> str:
    """One write.csv field: character quoted with doubled quotes, NA and numbers bare."""
    if value is None:
        return "NA"
    if isinstance(value, str):
        return '"' + value.replace('"', '""') + '"'
    return as_character(value)


def write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    """write.csv(row.names=FALSE), with LF line endings."""
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write(",".join(csv_field(name) for name in header) + "\n")
        for row in rows:
            handle.write(",".join(csv_field(value) for value in row) + "\n")


def xml_text(text: str | None) -> str | None:
    """Text node as XML::xmlTreeParse keeps it: blank nodes dropped, ends trimmed."""
    if text is None or not text.strip(" \t\n\r"):
        return None
    return text.strip(" \t\n\r")


def paste_xml_list(element, source: str) -> str:
    """paste0(XML::xmlToList(element), collapse = " "); NULL becomes ""."""
    if element is None:
        return ""
    items = [xml_text(element.text)]
    for child in element:
        if not isinstance(child.tag, str) or len(child):
            raise ValueError(f"{source}: unsupported markup inside {etree.QName(element).localname}")
        text = xml_text(child.text)
        if child.attrib and text is not None or len(child.attrib) > 1:
            raise ValueError(f"{source}: unsupported attributes inside {etree.QName(element).localname}")
        # Empty <span class="ThinSpace"/> lists as its attribute; an empty element as NULL.
        items.append(text if text is not None else next(iter(child.attrib.values()), "NULL"))
        items.append(xml_text(child.tail))
    return " ".join(item for item in items if item is not None)


def as_date(values: list) -> list:
    """format(as.Date(x), "%Y-%m-%d"), with the format chosen from the first value."""
    first = next((v for v in values if v is not None), None)
    if first is None:
        return values
    for pattern in (r"(\d{4})-(\d{1,2})-(\d{1,2})", r"(\d{4})/(\d{1,2})/(\d{1,2})"):
        if re.match(pattern, first):
            break
    else:
        raise ValueError(f"date {first!r} is not in a standard unambiguous format")
    result = []
    for value in values:
        match = re.match(pattern, value) if value is not None else None
        try:
            result.append(date(*map(int, match.groups())).isoformat() if match else None)
        except ValueError:
            result.append(None)
    return result
