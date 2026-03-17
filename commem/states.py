"""
State-specific commemorative bill coding configurations.

Each state config is a dict with:
  - clean: function(df, term) -> df — cleans/filters bill data, must produce
    bill_id, term, session columns
  - search_fields: list of column names to search for commem terms
  - c_terms: extra inclusion terms (added to base VW_COMMEM + ADDITIONS)
  - e_terms: extra exclusion terms (added to base EXCLUDE)
  - keyword_terms: terms to match in keywords column (if present)
  - manual_fixes: optional function(df, term) -> df for manual overrides
"""

import re

import pandas as pd


def _clean_va(df: pd.DataFrame, term: str) -> pd.DataFrame:
    """Clean VA bill details for commem coding."""
    start_year = int(term.split("_")[0])
    end_year = int(term.split("_")[1])

    df = df[df["session_year"].isin([start_year, end_year])].copy()
    df["term"] = term.replace("-", "_")

    # Derive session type from session name
    session_map = {
        "SESSION": "RS",
        "SPECIAL SESSION I": "SS1",
        "SPECIAL SESSION II": "SS2",
        "SPECIAL SESSION III": "SS3",
        "SPECIAL SESSION IV": "SS4",
    }
    df["session_type"] = df["session"].apply(
        lambda x: next(
            (v for k, v in session_map.items() if str(x).endswith(k)),
            "RS",
        )
    )
    df["session"] = (
        df["session_year"].astype(str) + "-" + df["session_type"]
    )

    df = df[df["bill_id"].str.match(r"^HB|^SB", na=False)].copy()
    df = df.drop_duplicates()
    return df


def _clean_nj(df: pd.DataFrame, term: str) -> pd.DataFrame:
    """Clean NJ bill details for commem coding."""
    df = df[df["session"] == term].copy() if "session" in df.columns else df.copy()

    # Rename columns to match pipeline expectations
    if "bill_number" in df.columns:
        # Convert A-0001 -> A0001
        df["bill_id"] = df["bill_number"].str.replace("-", "", regex=False)
        df = df.drop(columns=["bill_number"])
    df = df.rename(columns={"session": "term"})
    df["session"] = term  # NJ session = term (no sub-sessions)

    df = df[df["bill_id"].str.match(r"^A\d|^S\d", na=False)].copy()
    df = df.drop_duplicates()
    return df


# --- State configurations ---

STATES: dict[str, dict] = {
    "VA": {
        "clean": _clean_va,
        "search_fields": ["short_title", "summary"],
        "c_terms": [
            r"designates.+(bridge|road|highway)",
            "official state",
            "special license plates for supporters",
        ],
        "e_terms": [
            "state board", "sale", "school calendar", "property",
            "standards", "charter", "codifies",
            "establishes the commission", "^commission", "reimburs",
            "resettl", "emergency",
        ],
    },
    "NJ": {
        "clean": _clean_nj,
        "search_fields": ["title", "summary"],
        "c_terms": [
            r"^designates.+as the",
            r"directs.+to designat.+ as",
        ],
        "e_terms": [
            "alcohol", "acute", "pension", "reorg", "fundrais",
            "executive order", "interlocal", r'act\."$',
            "service preference", "^directs doe", "expands",
            "^clarifies", "^excludes", "^transfers", "^eliminates",
            "^requires",
        ],
    },
}


def get_state_config(state: str) -> dict:
    """Get the commem configuration for a state."""
    state = state.upper()
    if state not in STATES:
        raise ValueError(
            f"No commem configuration for state: {state}\n"
            f"Available: {', '.join(sorted(STATES.keys()))}"
        )
    return STATES[state]
