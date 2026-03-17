"""
SLES Scraping Module

Dispatches to state-specific scrapers under scrape/states/<state>.py.
"""

import importlib
from pathlib import Path


def scrape_bills(state: str, term: str, verbose: bool = False):
    """Main scraping entry point.

    Dynamically imports the state module and calls its scrape() function.
    """
    state_upper = state.upper()
    state_lower = state.lower()

    # Validate state module exists
    module_name = f"scrape.states.{state_lower}"
    states_dir = Path(__file__).parent / "states"
    state_file = states_dir / f"{state_lower}.py"

    if not state_file.exists():
        raise FileNotFoundError(
            f"No scrape module found for state: {state_upper}\n"
            f"Expected file: {state_file}"
        )

    # Import and validate
    module = importlib.import_module(module_name)
    if not hasattr(module, "scrape"):
        raise AttributeError(
            f"Scrape module for {state_upper} must export a `scrape` function"
        )

    # Ensure output directory exists
    repo_root = Path(__file__).parent.parent
    bill_dir = repo_root / ".data" / state_upper / "bill"
    bill_dir.mkdir(parents=True, exist_ok=True)

    if verbose:
        print(f"Loaded scrape module for {state_upper}")

    # Dispatch
    module.scrape(state_upper, term, verbose)

    print("Scraping complete!")
