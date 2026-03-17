"""
Commemorative Bill Coding

Identifies commemorative bills by searching bill text fields for
commemorative terms, then excluding false positives via exclusion terms.

Logic (from Volden & Wiseman):
- Conservative coding: prefer false negatives over false positives
- If a bill appropriates money, it is NOT commemorative
- Bills that allocate authority or create formal bodies are NOT commemorative
- Major categories: commemorative license plates, bridge renamings,
  state symbols, holidays/awareness/celebration days

The pipeline only needs bill_id, term, session, and commem from the output.
"""

import re
from pathlib import Path

import pandas as pd

from commem.states import get_state_config

# Base commemorative terms from Volden & Wiseman, adapted for states
VW_COMMEM = [
    "expressing support", "urging", "condol", "commemorat",
    r" honor|^honor", "memoria", "congratul", "public holiday",
    "for the relief of", "for the private relief of",
    "retention of the name", "medal", "posthumous",
    "provide for correction", "to name", "rename",
    "to remove any doubt",
]

ADDITIONS = [
    "anniversary", "raise awareness", r"awareness (day|week|month)",
    "dedicating", "celebrat", "appreciat", r" commend|^commend",
    "official design", "official emblem", "remembrance",
    "state symbol", "proclamation",
]

EXCLUDE = [
    "appropriates", "appropriation", r"approp\.", "appropriating",
    "to appropriate", r"\$", "dollars", "to fund", "funding", "funds",
    "expenditure", "penalt", "felony", r"memorial (act|law)", "criminal",
    "lien", "statutory", "license fee", r"^tax| tax", "prohibit",
    "rainy day", "procedure", "contract", "firearm", "weapon",
    "inflation", "exempt", "legislative intent", "deposit", "budget",
    "tuition", "violation", "compensation", "promulgate", "regulation",
    "bonds", "jurisdiction", "liabilit", "task force", "annuity",
    "probate", "financ", r"honor[a-z]+ discharge", "revenue",
    "compliance", "sale of", "health benefit", "insurer", "primary care",
    "grant program", "purchase", "donation", "official language",
    "refund", "election", "capital improvements", "liquor sales",
]

# Case-sensitive pattern for named Day/Week/Month
DAY_WEEK_MONTH = r"[A-Z][a-z]+ Day\b|[A-Z][a-z]+ Week\b|[A-Z][a-z]+ Month\b"


def _matches_any(text: str, patterns: list[str], case_sensitive=False) -> bool:
    """Check if text matches any of the given regex patterns."""
    if pd.isna(text) or text == "":
        return False
    if not case_sensitive:
        text = text.lower()
    combined = "|".join(patterns)
    return bool(re.search(combined, text))


def code_commem(state: str, term: str, verbose: bool = False):
    """Code commemorative bills for a state/term.

    Reads bill details CSVs, applies commem term matching,
    and writes output to .data/{STATE}/commem/.

    Args:
        state: State postal code (e.g., "VA")
        term: Legislative term (e.g., "2024_2025")
        verbose: Enable verbose logging
    """
    state = state.upper()
    repo_root = Path(__file__).parent.parent
    bill_dir = repo_root / ".data" / state / "bill"
    commem_dir = repo_root / ".data" / state / "commem"
    commem_dir.mkdir(parents=True, exist_ok=True)

    out_file = commem_dir / f"{state}_Commem_Bills_{term}.csv"
    if out_file.exists():
        print(f"Skipping: {out_file.name} already exists")
        return

    # Get state config
    config = get_state_config(state)

    # Read bill details for this term
    bill_files = sorted(bill_dir.glob(f"{state}_Bill_Details_*.csv"))
    if not bill_files:
        raise FileNotFoundError(
            f"No bill details files found in {bill_dir}"
        )

    all_bills = pd.concat(
        [pd.read_csv(f, encoding="latin-1") for f in bill_files],
        ignore_index=True,
    )

    if verbose:
        print(f"Read {len(all_bills)} bills from {len(bill_files)} file(s)")

    # Apply state-specific cleaning and filtering
    all_bills = config["clean"](all_bills, term)

    if verbose:
        print(f"After cleaning/filtering: {len(all_bills)} bills")

    # Build term lists
    c_terms = VW_COMMEM + ADDITIONS + config.get("c_terms", [])
    e_terms = EXCLUDE + config.get("e_terms", [])

    # Which text fields to search (state-specific)
    search_fields = config["search_fields"]

    # Code commem: inclusion pass
    all_bills["commem"] = 0
    for field in search_fields:
        if field not in all_bills.columns:
            continue
        mask = all_bills[field].apply(
            lambda x: _matches_any(str(x), c_terms)
        )
        all_bills.loc[mask, "commem"] = 1

    # Case-sensitive Day/Week/Month pattern
    for field in search_fields:
        if field not in all_bills.columns:
            continue
        mask = all_bills[field].apply(
            lambda x: _matches_any(str(x), [DAY_WEEK_MONTH],
                                   case_sensitive=True)
            if pd.notna(x) else False
        )
        all_bills.loc[mask, "commem"] = 1

    # Keywords field (some states have it)
    if "keywords" in all_bills.columns:
        kw_terms = config.get("keyword_terms",
                              ["proclamations", "holidays", "place names"])
        mask = all_bills["keywords"].apply(
            lambda x: _matches_any(str(x), kw_terms)
        )
        all_bills.loc[mask, "commem"] = 1

    # Exclusion pass (overrides inclusions)
    for field in search_fields:
        if field not in all_bills.columns:
            continue
        mask = all_bills[field].apply(
            lambda x: _matches_any(str(x), e_terms)
        )
        all_bills.loc[mask, "commem"] = 0

    # State-specific manual fixes
    manual_fixes = config.get("manual_fixes")
    if manual_fixes:
        all_bills = manual_fixes(all_bills, term)

    # Select output columns
    out_cols = ["bill_id", "term", "session"]
    # Include whichever text fields exist for reference
    for col in ["short_title", "title", "summary", "keywords"]:
        if col in all_bills.columns:
            out_cols.append(col)
    out_cols.append("commem")

    out_cols = [c for c in out_cols if c in all_bills.columns]
    result = all_bills[out_cols]

    result.to_csv(out_file, index=False)

    n_commem = (result["commem"] == 1).sum()
    n_total = len(result)
    print(f"  {out_file.name}: {n_commem}/{n_total} bills coded as commemorative")
