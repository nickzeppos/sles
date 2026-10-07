"""
Substantive & Significant (SS) Bill Parser

Parses saved Vote Smart tables offline; never downloads pages or opens a browser.
Each run rebuilds the CSV from local HTML after all pages validate.

Save pages into .data/{STATE}/ss/raw/ with filenames ending in .html/.htm
or .html.gz/.htm.gz.
Files are sorted alphabetically, so name them to preserve page order, e.g.:
  VoteSmart_VA_2024_1.html
  VoteSmart_VA_2025_1.html
  VoteSmart_VA_2025_2.html

Output: {STATE}_SS_Bills_{YEAR}.csv for the requested year.
"""

from utils import cache as cache_io

import csv
import json
import tempfile
from pathlib import Path

from ss.votesmart import parse_table, validate_state_year


def parse_ss(state: str, year: str, verbose: bool = False):
    """Parse locally saved Vote Smart HTML pages for a state/year.

    Args:
        state: State postal code (e.g., "VA")
        year: Single year (e.g., "2024")
        verbose: Enable verbose logging
    """
    state = state.upper()
    validate_state_year(state, year)
    repo_root = Path(__file__).parent.parent
    ss_dir = repo_root / ".data" / state / "ss"
    raw_dir = ss_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    out_file = ss_dir / f"{state}_SS_Bills_{year}.csv"

    # Find HTML files for this year
    html_files = sorted(
        f for f in cache_io.html_files(raw_dir)
        if f"_{year}_" in f.name
    )

    # Downloaded batches have a manifest so an interrupted download cannot be
    # mistaken for a complete collection of manually saved pages.
    manifest = raw_dir / f"VoteSmart_{state}_{year}_download.json"
    if not html_files and cache_io.exists(manifest):
        names = json.loads(cache_io.read_text(manifest, encoding="utf-8"))
        html_files = [raw_dir / name for name in names]
        if not html_files or any(not cache_io.is_file(p) for p in html_files):
            raise RuntimeError("Incomplete saved Vote Smart batch; restore its missing HTML files or save pages directly in the raw directory.")

    if not html_files:
        raise FileNotFoundError(
            f"No saved Vote Smart HTML for {state} {year}. Save pages in {raw_dir} "
            f"as VoteSmart_{state}_{year}_H_1.html (and additional pages), "
            "then rerun parse-ss."
        )

    if verbose:
        print(f"Found {len(html_files)} HTML file(s) for {state} {year}")

    rows = []
    for html_file in html_files:
        if verbose:
            print(f"  Parsing {html_file.name}")
        _, page_rows = parse_table(cache_io.read_text(html_file, encoding="utf-8"), state, html_file.name)
        rows.extend(page_rows)

    # Publish only after every page has been validated.
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=ss_dir, delete=False,
    ) as stream:
        temporary = Path(stream.name)
        try:
            writer = csv.writer(stream)
            writer.writerow(["Date", "State", "Bill No", "Title", "Action"])
            writer.writerows(rows)
        except BaseException:
            cache_io.unlink(temporary, missing_ok=True)
            raise
    cache_io.replace(temporary, out_file)
    print(f"  {out_file.name}: {len(rows)} bill records")
