"""Session-year windows shared by CLI operations (rules also read by R)."""
import csv
from pathlib import Path
import re

STATES = frozenset("AK AL AR AZ CA CO CT DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY".split())


def term_years(state: str, term: str) -> tuple[int, ...]:
    state = state.upper()
    if state not in STATES:
        raise ValueError(f"Unknown state: {state}")
    with Path(__file__).with_name("term_rules.csv").open(newline="") as handle:
        rules = {row["state"]: row for row in csv.DictReader(handle)}
    rule = rules.get(state, rules["default"])
    if state == "AL" and re.fullmatch(r"\d{4}_\d{4}", term or ""):
        start, end = map(int, term.split("_"))
        if start % 4 == 2 and end == start + 4:
            raise ValueError(
                f"Alabama terms use session years. For the legislature elected "
                f"in {start}, use {start + 1}_{end}."
            )
    if not re.fullmatch(r"\d{4}_\d{4}", term or ""):
        raise ValueError(f"{state} term must be YYYY_YYYY session years, e.g. {rule['example']}")
    start, end = map(int, term.split("_"))
    if (start < 1 or end - start + 1 != int(rule["years"])
            or start % int(rule["start_modulus"]) != int(rule["start_remainder"])):
        raise ValueError(
            f"{state} requires an aligned {rule['years']}-year session window, "
            f"e.g. {rule['example']}; got {term}"
        )
    return tuple(range(start, end + 1))
