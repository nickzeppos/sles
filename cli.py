#!/usr/bin/env python3
"""
SLES CLI - State Legislative Effectiveness Scores

Unified entry point for scraping and estimation.

Usage:
    python cli.py <state> <term> <operation> [--verbose]

Operations:
    scrape    - Run state-specific bill data scraper/processor (Python)
    scrape-connor - Run an imported Connor scraper with repo data paths (no term)
    parse-ss  - Parse locally saved Vote Smart HTML pages (no network)
    commem    - Code commemorative bills from bill details (Python)
    estimate  - Run LES estimation (R; Python for CO)

Examples:
    python cli.py VA 2024_2025 scrape --verbose
    python cli.py WI scrape-connor
    python cli.py MO scrape-connor --chamber upper
    python cli.py VA 2024 parse-ss --verbose
    python cli.py VA 2024_2025 commem --verbose
    python cli.py WI 2023_2024 estimate
"""

import argparse
import subprocess
import sys
from pathlib import Path
from utils.validate_term import term_years


def main():
    parser = argparse.ArgumentParser(
        description="SLES CLI - State Legislative Effectiveness Scores"
    )
    parser.add_argument("state", help="State postal code (e.g., VA)")
    parser.add_argument(
        "term",
        nargs="?",
        help="Session years (AL: 2023_2026; NJ/VA: 2026_2027; others: 2025_2026), or SS year",
    )
    parser.add_argument(
        "operation",
        choices=[
            "scrape",
            "parse-ss", "scrape-connor", "commem", "estimate",
        ],
        help="Operation to perform",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose logging"
    )
    parser.add_argument(
        "--preview", metavar="BILL_ID",
        help="Preview parsed values for a single bill (e.g. HB23-1001)"
    )
    parser.add_argument(
        "--retry-failed", action="store_true",
        help="Retry URLs from .failed_<term>.txt, appending to existing CSVs"
    )
    parser.add_argument(
        "--force-fetch", action="store_true",
        help="Re-fetch all bill pages even if cached, overwriting cache"
    )
    parser.add_argument(
        "--chamber", choices=["upper", "lower"],
        help="Chamber for scrape-connor MO (default: both chambers)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show Connor scraper paths and data directory without running them"
    )

    args = parser.parse_args()
    args.state = args.state.upper()

    if args.operation not in ("scrape-connor", "parse-ss"):
        if not (args.preview and args.term is None):
            try:
                term_years(args.state, args.term)
            except ValueError as exc:
                parser.error(str(exc))

    if args.operation != "scrape-connor" and (args.chamber or args.dry_run):
        parser.error("--chamber and --dry-run are only available for scrape-connor")

    if args.operation == "scrape-connor":
        from scrape.connor import scrape_connor

        if args.term:
            parser.error("scrape-connor currently uses each script's own session selection; omit term")
        if args.preview or args.retry_failed or args.force_fetch:
            parser.error("scrape-connor does not support --preview, --retry-failed, or --force-fetch")
        try:
            returncode = scrape_connor(args.state, args.chamber, args.dry_run)
        except ValueError as exc:
            parser.error(str(exc))
        sys.exit(returncode)

    if args.operation == "scrape":
        if args.preview:
            run_scrape_preview(args.state, args.preview, args.verbose)
        elif args.retry_failed:
            if not args.term:
                parser.error("term is required for --retry-failed")
            run_scrape_retry_failed(args.state, args.term, args.verbose)
        else:
            if not args.term:
                parser.error("term is required for scrape without --preview")
            run_scrape(args.state, args.term, args.verbose, args.force_fetch)
    elif args.operation == "parse-ss":
        from ss.votesmart import validate_state_year
        try:
            validate_state_year(args.state, args.term)
        except ValueError as exc:
            parser.error(str(exc))
        if args.force_fetch or args.preview or args.retry_failed:
            parser.error("parse-ss only reads saved HTML; use --verbose for progress")
        run_parse_ss(args.state, args.term, args.verbose)
    elif args.operation == "commem":
        run_commem(args.state, args.term, args.verbose)
    elif args.operation == "estimate":
        run_estimate(args.state, args.term, args.verbose)


def run_scrape(state: str, term: str, verbose: bool, force_fetch: bool = False):
    """Run the Python scrape module."""
    from scrape.scrape import scrape_bills

    scrape_bills(state, term, verbose, force_fetch=force_fetch)


def run_scrape_preview(state: str, bill_id: str, verbose: bool):
    """Preview parsed details and history for a single bill."""
    from scrape.states.co import preview_bill

    preview_bill(state, bill_id, verbose)


def run_scrape_retry_failed(state: str, term: str, verbose: bool):
    """Retry failed URLs from .failed_<term>.txt."""
    from scrape.states.co import retry_failed

    retry_failed(state, term, verbose)


def run_parse_ss(state: str, year: str, verbose: bool):
    """Parse saved Vote Smart HTML for SS bills without network access."""
    from ss.ss import parse_ss

    parse_ss(state, year, verbose)


def run_commem(state: str, term: str, verbose: bool):
    """Code commemorative bills."""
    from commem.commem import code_commem

    code_commem(state, term, verbose)


def run_estimate(state: str, term: str, verbose: bool):
    """Run the state's estimator."""
    if state.upper() == "CO":
        from estimate.states.CO import estimate_les

        try:
            estimate_les(term, verbose)
        except (ValueError, FileNotFoundError) as error:
            print(str(error), file=sys.stderr)
            sys.exit(1)
        return

    root = Path(__file__).resolve().parent
    try:
        check = subprocess.run(
            ["Rscript", "--no-init-file", str(root / "utils" / "check_r_env.R")],
            cwd=str(root), capture_output=True, text=True,
        )
    except FileNotFoundError:
        print("Rscript was not found. Install R before estimating.", file=sys.stderr)
        sys.exit(1)
    if check.returncode:
        if verbose:
            print(check.stdout + check.stderr, file=sys.stderr, end="")
        print(
            "R dependencies need setup or updating. From the repo root, run:\n"
            "  Rscript -e 'renv::restore()'",
            file=sys.stderr,
        )
        sys.exit(1)

    module = root / "estimate" / "estimate.R"
    cmd = ["Rscript", str(module), state, term]
    if verbose:
        cmd.append("--verbose")

    print(f"Running estimation for {state} ({term})...")
    result = subprocess.run(cmd, cwd=str(root))
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
