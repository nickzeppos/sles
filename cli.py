#!/usr/bin/env python3
"""
SLES CLI - State Legislative Effectiveness Scores

Unified entry point for scraping and estimation.

Usage:
    python cli.py <state> <term> <operation> [--verbose]

Operations:
    scrape    - Run state-specific bill data scraper/processor (Python)
    scrape-ss - Parse saved VoteSmart HTML pages for SS bills (Python)
    commem    - Code commemorative bills from bill details (Python)
    estimate  - Run LES estimation pipeline (delegates to Rscript)

Examples:
    python cli.py VA 2024_2025 scrape --verbose
    python cli.py VA 2024 scrape-ss --verbose
    python cli.py VA 2024_2025 commem --verbose
    python cli.py WI 2023_2024 estimate
"""

import argparse
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="SLES CLI - State Legislative Effectiveness Scores"
    )
    parser.add_argument("state", help="State postal code (e.g., VA)")
    parser.add_argument(
        "term",
        nargs="?",
        help="Legislative term (e.g., 2024_2025) or year (e.g., 2024)",
    )
    parser.add_argument(
        "operation",
        choices=[
            "scrape", "scrape-retry", "scrape-headless",
            "scrape-ss", "commem", "estimate",
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

    args = parser.parse_args()

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
    elif args.operation == "scrape-retry":
        run_scrape_retry(args.state, args.term, args.verbose)
    elif args.operation == "scrape-headless":
        run_scrape_headless(args.state, args.term, args.verbose)
    elif args.operation == "scrape-ss":
        run_scrape_ss(args.state, args.term, args.verbose)
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


def run_scrape_retry(state: str, term: str, verbose: bool):
    """Retry HTTP errors from a previous scrape."""
    from scrape.states.va import retry_errors

    retry_errors(state, term, verbose)


def run_scrape_headless(state: str, term: str, verbose: bool):
    """Scrape missing bills from the new LIS site using Playwright."""
    from scrape.states.va_headless import retry_missing

    retry_missing(state, term, verbose)


def run_scrape_ss(state: str, year: str, verbose: bool):
    """Parse saved VoteSmart HTML for SS bills."""
    from scrape.ss import scrape_ss

    scrape_ss(state, year, verbose)


def run_commem(state: str, term: str, verbose: bool):
    """Code commemorative bills."""
    from commem.commem import code_commem

    code_commem(state, term, verbose)


def run_estimate(state: str, term: str, verbose: bool):
    """Delegate estimation to Rscript cli.R."""
    cli_r = Path(__file__).parent / "cli.R"
    cmd = ["Rscript", str(cli_r), state, term, "estimate"]
    if verbose:
        cmd.append("--verbose")

    print(f"Running estimation for {state} ({term})...")
    result = subprocess.run(cmd, cwd=str(Path(__file__).parent))
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
