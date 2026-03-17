"""
New Jersey Bill Data Processor

Faithful port of NJ_State_Leg_Scrape_Auto.py.

NJ provides bulk bill tracking data as downloadable Access databases
(or text/CSV exports) from:
  https://www.njleg.state.nj.us/legislative-downloads?downloadType=Bill_Tracking

This module reads the pre-downloaded MAINBILL.TXT and BILLHIST.TXT files,
cleans and reformats them to match the pipeline's expected CSV format.

Prerequisites:
  1. Download DB{YYYY}_TEXT.zip from the NJ legislature site
  2. Extract MAINBILL.TXT and BILLHIST.TXT into:
     .data/NJ/bill/raw_{term}/
     e.g. .data/NJ/bill/raw_2024_2025/MAINBILL.TXT
"""

import datetime
import re
from pathlib import Path

import pandas as pd

# Action code dictionaries ported from old script

# Exact-match actions
ACTIONS = {
    "INT 1RA AWR 2RA": (
        "Introduced, 1st Reading without Reference, 2nd Reading",
        "introduction",
    ),
    "INT 1RS SWR 2RS": (
        "Introduced, 1st Reading without Reference, 2nd Reading",
        "introduction",
    ),
    "REP 2RA": (
        "Reported out of Assembly Committee, 2nd Reading",
        "committee-passage",
    ),
    "REP 2RS": (
        "Reported out of Senate Committee, 2nd Reading",
        "committee-passage",
    ),
    "REP/ACA 2RA": (
        "Reported out of Assembly Committee with Amendments, 2nd Reading",
        "committee-passage",
    ),
    "REP/SCA 2RS": (
        "Reported out of Senate Committee with Amendments, 2nd Reading",
        "committee-passage",
    ),
    "R/S SWR 2RS": (
        "Received in the Senate without Reference, 2nd Reading",
        None,
    ),
    "R/A AWR 2RA": (
        "Received in the Assembly without Reference, 2nd Reading",
        None,
    ),
    "R/A 2RAC": (
        "Received in the Assembly, 2nd Reading on Concurrence",
        None,
    ),
    "R/S 2RSC": (
        "Received in the Senate, 2nd Reading on Concurrence",
        None,
    ),
    "REP/ACS 2RA": (
        "Reported from Assembly Committee as a Substitute, 2nd Reading",
        None,
    ),
    "REP/SCS 2RS": (
        "Reported from Senate Committee as a Substitute, 2nd Reading",
        None,
    ),
    "AA 2RA": ("Assembly Floor Amendment Passed", "amendment-passage"),
    "SA 2RS": ("Senate Amendment", "amendment-passage"),
    "SUTC REVIEWED": (
        "Reviewed by the Sales Tax Review Commission",
        None,
    ),
    "PHBC REVIEWED": (
        "Reviewed by the Pension and Health Benefits Commission",
        None,
    ),
    "SUB FOR": ("Substituted for", None),
    "SUB BY": ("Substituted by", None),
    "PA PBH": ("Passed Assembly (Passed Both Houses)", "passage"),
    "PS PBH": ("Passed Senate (Passed Both Houses)", "passage"),
    "PA": ("Passed Assembly", "passage"),
    "PS": ("Passed Senate", "passage"),
    "PS FILE": ("Passed Senate and Filed", "passage"),
    "PA FILE": ("Passed Assembly and Filed", "passage"),
    "APP W/LIV": (
        "Approved with Line Item Veto",
        "executive-signature",
    ),
    "APP": ("Approved", "executive-signature"),
    "AV R/A": (
        "Absolute Veto, Received in the Assembly",
        "executive-veto",
    ),
    "AV R/S": (
        "Absolute Veto, Received in the Senate",
        "executive-veto",
    ),
    "CV R/A": (
        "Conditional Veto, Received in the Assembly",
        "executive-veto",
    ),
    "CV R/A 1RAG": (
        "Conditional Veto, Received in the Assembly, "
        "1st Reading/Governor Recommendation",
        "executive-veto",
    ),
    "CV R/A 2RAG": (
        "Conditional Veto, Received in the Assembly, "
        "2nd Reading/Governor Recommendation",
        "executive-veto",
    ),
    "CV R/S": (
        "Conditional Veto, Received in the Senate",
        "executive-veto",
    ),
    "PV": (
        "Pocket Veto - Bill not acted on by Governor-end of Session",
        "executive-veto",
    ),
    "2RSG": (
        "2nd Reading on Concur with Governor's Recommendations",
        None,
    ),
    "CV R/S 2RSG": (
        "Conditional Veto, Received, 2nd Reading on Concur "
        "with Governor's Recommendations",
        None,
    ),
    "CV R/S 1RSG": (
        "Conditional Veto, Received, 1st Reading on Concur "
        "with Governor's Recommendations",
        None,
    ),
    "R/S 2RSG": (
        "Received in the Senate, 2nd Reading - "
        "Concur. w/Gov's Recommendations",
        None,
    ),
    "R/A 2RAG": (
        "Received in the Assembly, 2nd Reading - "
        "Concur. w/Gov's Recommendations",
        None,
    ),
    "1RAG": ("First Reading/Governor Recommendations Only", None),
    "2RAG": (
        "2nd Reading in the Assembly on Concur. "
        "w/Gov's Recommendations",
        None,
    ),
    "R/A": ("Received in the Assembly", None),
    "REF SBA": (
        "Referred to Senate Budget and Appropriations Committee",
        "referral-committee",
    ),
    "RSND/V": ("Rescind Vote", None),
    "RSND/ACT OF": ("Rescind Action", None),
    "RCON/V": ("Reconsidered Vote", None),
    "CONCUR AA": ("Concurred by Assembly Amendments", None),
    "CONCUR SA": ("Concurred by Senate Amendments", None),
    "SS 2RS": ("Senate Substitution", None),
    "AS 2RA": ("Assembly Substitution", None),
    "ER": ("Emergency Resolution", None),
    "FSS": ("Filed with Secretary of State", None),
    "LSTA": ("Lost in the Assembly", None),
    "LSTS": ("Lost in the Senate", None),
    "SEN COPY ON DESK": ("Placed on Desk in Senate", None),
    "ASM COPY ON DESK": ("Placed on Desk in Assembly", None),
    "COMB/W": ("Combined with", None),
    "MOTION": ("Motion", None),
    "PUBLIC HEARING": ("Public Hearing Held", None),
    "PH ON DESK SEN": (
        "Public Hearing Placed on Desk Senate Transcript "
        "Placed on Desk",
        None,
    ),
    "PH ON DESK ASM": (
        "Public Hearing Placed on Desk Assembly Transcript "
        "Placed on Desk",
        None,
    ),
    "W": ("Withdrawn from Consideration", "withdrawal"),
}

# Partial-match actions (always include a committee name suffix)
COMM_ACTIONS = {
    "INT 1RA REF": (
        "Introduced in the Assembly, Referred to",
        "introduction",
    ),
    "INT 1RS REF": (
        "Introduced in the Senate, Referred to",
        "introduction",
    ),
    "R/S REF": (
        "Received in the Senate, Referred to",
        "referral-committee",
    ),
    "R/A REF": (
        "Received in the Assembly, Referred to",
        "referral-committee",
    ),
    "TRANS": ("Transferred to", "referral-committee"),
    "RCM": ("Recommitted to", "referral-committee"),
    "REP/ACA REF": (
        "Reported out of Assembly Committee with Amendments "
        "and Referred to",
        "referral-committee",
    ),
    "REP/ACS REF": (
        "Reported out of Senate Committee with Amendments "
        "and Referred to",
        "referral-committee",
    ),
    "REP REF": ("Reported and Referred to", "referral-committee"),
}

# Committee vote motion replacements
COM_VOTE_MOTIONS = {
    "r w/o rec.": "Reported without recommendation",
    "r w/o rec. ACS": (
        "Reported without recommendation out of Assembly "
        "committee as a substitute"
    ),
    "r w/o rec. SCS": (
        "Reported without recommendation out of Senate "
        "committee as a substitute"
    ),
    "r w/o rec. Sca": (
        "Reported without recommendation out of Senate "
        "committee with amendments"
    ),
    "r w/o rec. Aca": (
        "Reported without recommendation out of Assembly "
        "committee with amendments"
    ),
    "r/ACS": "Reported out of Assembly committee as a substitute",
    "r/Aca": "Reported out of Assembly committee with amendments",
    "r/SCS": "Reported out of Senate committee as a substitute",
    "r/Sca": "Reported out of Senate committee with amendments",
    "r/favorably": "Reported favorably out of committee",
}


def clean_bill_data(bdf: pd.DataFrame, session: str) -> pd.DataFrame:
    """Clean and reformat the MainBill data."""
    print(f"\n ~~~ Cleaning the Bill File for the {session} Session ~~~~\n")

    # Normalize column names to uppercase (early files were all caps)
    bdf.columns = [c.upper() for c in bdf.columns]
    # Handle truncated column names from older DBF files
    col_renames = {
        "CURRENTSTA": "CURRENTSTATUS",
        "EFFECTIVED": "EFFECTIVEDATE",
        "SECONDPRIM": "SECONDPRIME",
        "IDENTICALB": "IDENTICALBILLNUMBER",
    }
    bdf.columns = [col_renames.get(c, c) for c in bdf.columns]

    # Clean bill number: "A  " + 4 -> "A-0004"
    bdf["BILLTYPE"] = bdf["BILLTYPE"].str.strip()
    bdf["bill_number"] = (
        bdf["BILLTYPE"]
        + "-"
        + bdf["BILLNUMBER"].astype(str).str.zfill(4)
    )

    bdf["session"] = session
    bdf = bdf.fillna("")

    # Rename columns to match pipeline format
    bdf = bdf.rename(columns={
        "ABSTRACT": "title",
        "CURRENTSTATUS": "status",
        "SYNOPSIS": "summary",
        "INTRODATE": "intro_date",
        "EFFECTIVEDATE": "effective_date",
        "CHAPTERLAW": "chapter_num",
        "IDENTICALBILLNUMBER": "companion",
    })

    # Standardize dates to YYYY-MM-DD
    for col in ["intro_date", "effective_date"]:
        bdf[col] = pd.to_datetime(
            bdf[col], errors="coerce"
        ).dt.strftime("%Y-%m-%d").fillna("")

    # Clean companion bill numbers
    bdf["companion"] = bdf["companion"].apply(
        lambda x: re.sub(r"\(.+\)|\(.+\}| [a-z].+", "", str(x))
    )

    # Format companion bills and sponsors
    bdf["primary_sponsors"] = ""
    for i in range(len(bdf)):
        # Companion bills
        these_bills = str(bdf.loc[bdf.index[i], "companion"]).strip().split(" ")
        stems = [re.sub(r"[0-9]+", "", j) for j in these_bills]
        nums = [re.sub(r"[A-Z]+", "", j).zfill(4) for j in these_bills]
        formatted = "; ".join(
            f"{s}-{n}" for s, n in zip(stems, nums) if n != "0000"
        )
        bdf.iloc[i, bdf.columns.get_loc("companion")] = formatted

        # Sponsors
        sponsors = bdf[["FIRSTPRIME", "SECONDPRIME", "THIRDPRIME"]].iloc[i]
        bdf.iloc[i, bdf.columns.get_loc("primary_sponsors")] = "; ".join(
            j.strip() for j in list(sponsors) if str(j).strip() not in ("", "nan")
        )

    # Subset to final columns
    return bdf[[
        "bill_number", "session", "title", "status", "primary_sponsors",
        "intro_date", "effective_date", "chapter_num", "companion", "summary",
    ]]


def clean_hist_data(hdf: pd.DataFrame, session: str) -> pd.DataFrame:
    """Clean and reformat the BillHist data."""
    print(f"\n ~~~ Cleaning the Action File for the {session} Session ~~~~\n")

    hdf.columns = [c.upper() for c in hdf.columns]

    # Clean bill number
    hdf["BILLTYPE"] = hdf["BILLTYPE"].str.strip()
    hdf["bill_number"] = (
        hdf["BILLTYPE"]
        + "-"
        + hdf["BILLNUMBER"].astype(str).str.zfill(4)
    )

    hdf["session"] = session
    hdf = hdf.fillna("")

    hdf = hdf.rename(columns={
        "HOUSE": "chamber",
        "ACTION": "action",
        "SEQUENCE": "order",
        "DATEACTION": "action_date",
    })

    # Standardize dates to YYYY-MM-DD
    hdf["action_date"] = pd.to_datetime(
        hdf["action_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d").fillna("")

    # Code actions
    hdf["action_openstates"] = ""

    # Exact matches
    for code, (description, os_code) in ACTIONS.items():
        mask = hdf["action"] == code
        hdf.loc[mask, "action"] = description
        if os_code:
            hdf.loc[mask, "action_openstates"] = hdf.loc[
                mask, "action_openstates"
            ].apply(
                lambda x: "; ".join(
                    [i for i in [x, os_code] if i != ""]
                )
            )

    # Partial matches (committee actions)
    for code, (description, os_code) in COMM_ACTIONS.items():
        mask = hdf["action"].str.contains(code, regex=False)
        hdf.loc[mask, "action"] = hdf.loc[mask, "action"].str.replace(
            code, description, regex=False
        )
        if os_code:
            os_val = os_code if isinstance(os_code, str) else "; ".join(os_code)
            hdf.loc[mask, "action_openstates"] = hdf.loc[
                mask, "action_openstates"
            ].apply(
                lambda x, v=os_val: "; ".join(
                    [i for i in [x, v] if i != ""]
                )
            )

    # Committee vote motions
    for code, description in COM_VOTE_MOTIONS.items():
        hdf["action"] = hdf["action"].str.replace(
            code, description, regex=False
        )

    return hdf[[
        "bill_number", "session", "chamber", "action_date", "action", "order",
    ]]


def scrape(state: str, term: str, verbose: bool = False):
    """Main entry point for NJ bill data processing.

    Reads pre-downloaded MAINBILL.TXT and BILLHIST.TXT from
    .data/NJ/bill/raw_{term}/ and produces cleaned CSVs.

    Args:
        state: "NJ"
        term: e.g. "2024_2025"
        verbose: Enable verbose logging
    """
    repo_root = Path(__file__).parent.parent.parent
    bill_dir = repo_root / ".data" / state / "bill"

    # Check if already processed
    details_file = bill_dir / f"NJ_Bill_Details_{term}.csv"
    histories_file = bill_dir / f"NJ_Bill_Histories_{term}.csv"
    if details_file.exists():
        print(f"Skipping {term}: {details_file.name} already exists")
        return

    # Find raw data
    raw_dir = bill_dir / f"raw_{term}"
    main_file = raw_dir / "MAINBILL.TXT"
    hist_file = raw_dir / "BILLHIST.TXT"

    if not main_file.exists() or not hist_file.exists():
        raise FileNotFoundError(
            f"Raw data not found. Expected:\n"
            f"  {main_file}\n"
            f"  {hist_file}\n\n"
            f"Download DB{term.split('_')[0]}_TEXT.zip from:\n"
            f"  https://www.njleg.state.nj.us/legislative-downloads"
            f"?downloadType=Bill_Tracking\n"
            f"Extract MAINBILL.TXT and BILLHIST.TXT into {raw_dir}/"
        )

    print(f"\n Reading raw data from {raw_dir}/")

    bill_df = pd.read_csv(main_file, encoding="latin-1")
    hist_df = pd.read_csv(hist_file, encoding="latin-1")

    bill_df = clean_bill_data(bill_df, term)
    hist_df = clean_hist_data(hist_df, term)

    bill_df.to_csv(details_file, index=False)
    hist_df.to_csv(histories_file, index=False)

    print(f"\n ~~~ {term} Session - Data Cleaned and Saved! ~~~~")
    print(f"  {details_file.name}: {len(bill_df)} bills")
    print(f"  {histories_file.name}: {len(hist_df)} actions")
