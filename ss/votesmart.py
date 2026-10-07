"""Validation and table parsing for locally saved Vote Smart HTML."""

import re
from bs4 import BeautifulSoup


def validate_state_year(state, year):
    if not re.fullmatch(r"[A-Z]{2}", state) or not re.fullmatch(r"\d{4}", str(year)):
        raise ValueError("Provide a two-letter state and a single year, e.g. AL 2025.")


def parse_table(html, state, source):
    """Reject login/block pages instead of producing an empty CSV."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.interest-group-ratings-table")
    if table is None:
        raise RuntimeError(
            f"No Vote Smart bill table in {source}. The page may require login, "
            "be blocked, or have changed layout. No CSV was written."
        )
    headers = [cell.get_text(" ", strip=True).lower().rstrip(".") for cell in table.select("th")]
    if headers[:5] not in (
        ["date", "state", "bill no", "title", "outcome"],
        ["date", "state", "bill no", "title", "action"],
    ):
        raise RuntimeError(f"Unexpected bill-table columns in {source}; no CSV was written.")
    rows = []
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if not cells:
            continue
        if len(cells) != 5:
            raise RuntimeError(f"Unexpected bill row in {source}; no CSV was written.")
        values = [cell.get_text().strip() for cell in cells]
        if values[1].upper() != state:
            raise RuntimeError(f"Expected {state} data in {source}, found {values[1]}.")
        rows.append(values)
    return soup, rows


