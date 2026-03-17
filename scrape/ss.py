"""
Substantive & Significant (SS) Bill Parser

Parses saved VoteSmart HTML pages to extract SS bill lists.

The HTML pages should be manually downloaded from:
  https://justfacts.votesmart.org/bills/{STATE}/{YEAR}//

Save pages into .data/{STATE}/ss/raw/ with any filename ending in .html/.htm.
Files are sorted alphabetically, so name them to preserve page order, e.g.:
  VoteSmart_VA_2024_1.html
  VoteSmart_VA_2025_1.html
  VoteSmart_VA_2025_2.html

Output: {STATE}_SS_Bills_{YEAR}.csv per year found in the HTML files.
"""

import csv
import re
from pathlib import Path

from bs4 import BeautifulSoup


def scrape_ss(state: str, year: str, verbose: bool = False):
    """Parse saved VoteSmart HTML pages for a state/year.

    Args:
        state: State postal code (e.g., "VA")
        year: Single year (e.g., "2024")
        verbose: Enable verbose logging
    """
    state = state.upper()
    repo_root = Path(__file__).parent.parent
    ss_dir = repo_root / ".data" / state / "ss"
    raw_dir = ss_dir / "raw"

    # Check output
    out_file = ss_dir / f"{state}_SS_Bills_{year}.csv"
    if out_file.exists():
        print(f"Skipping: {out_file.name} already exists")
        return

    if not raw_dir.exists():
        raise FileNotFoundError(
            f"No raw HTML directory found at {raw_dir}\n"
            f"Download VoteSmart pages from:\n"
            f"  https://justfacts.votesmart.org/bills/{state}/{year}//\n"
            f"Save HTML files into {raw_dir}/"
        )

    # Find HTML files for this year
    html_files = sorted(
        f for f in raw_dir.iterdir()
        if f.suffix in (".html", ".htm")
        and f"_{year}_" in f.name
    )

    if not html_files:
        raise FileNotFoundError(
            f"No HTML files found matching year {year} in {raw_dir}/\n"
            f"Expected filenames containing '_{year}_', e.g. "
            f"VoteSmart_{state}_{year}_1.html"
        )

    if verbose:
        print(f"Found {len(html_files)} HTML file(s) for {state} {year}")

    # Parse all pages
    dates = []
    states = []
    bill_numbers = []
    titles = []
    actions = []

    for html_file in html_files:
        if verbose:
            print(f"  Parsing {html_file.name}")

        soup = BeautifulSoup(html_file.read_text(), "html.parser")
        table = soup.find(
            "table", {"class": "table interest-group-ratings-table"}
        )

        if table is None:
            print(
                f"  WARNING: No bill table found in {html_file.name} "
                f"(may have no significant bills)"
            )
            continue

        for row in table.find_all("tr"):
            columns = row.find_all("td")
            if len(columns) >= 5:
                dates.append(columns[0].text.strip())
                states.append(columns[1].text.strip())
                bill_numbers.append(columns[2].text.strip())
                titles.append(columns[3].text.strip())
                actions.append(columns[4].text.strip())

    # Write CSV
    rows = [["Date", "State", "Bill No", "Title", "Action"]]
    for i in range(len(dates)):
        rows.append([dates[i], states[i], bill_numbers[i], titles[i], actions[i]])

    with open(out_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    print(f"  {out_file.name}: {len(dates)} bills")
