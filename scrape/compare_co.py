"""
Compare a ~random sample of CO bill rows between new and old CSVs.

Usage:
    python scrape/compare_co.py 2023 details
    python scrape/compare_co.py 2023 histories
    python scrape/compare_co.py 2023 details --n 20 --col primary_sponsors
"""

import argparse
import csv
import random
import re
from pathlib import Path

# Util fns
def normalize(val: str) -> str:
    """(Note is stubborn, and I don't care too much about it."""
    val = re.sub(r"\s+\(Note:", "(Note:", val)
    return " ".join(val.lower().split())


def normalize_keywords(val: str) -> str:
    """Normalize keywords col — old CSVs use newlines, new uses '; '."""
    parts = [p.strip() for p in re.split(r"[\n;]+", val) if p.strip()]
    return "; ".join(p.lower() for p in parts)


def load_csv(path: Path) -> dict[str, dict]:
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        return {row["bill_number"]: row for row in reader}


def load_histories(path: Path) -> dict[str, list[dict]]:
    result: dict[str, list[dict]] = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            result.setdefault(row["bill_number"], []).append(row)
    return result

# Consts for pretty printing
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RESET = "\033[0m"
TRUNC = 120


def compare_rows(
    bill_id: str,
    new_row: dict,
    old_row: dict,
    label: str = "details",
):
    diffs = []
    all_cols = set(new_row) | set(old_row)
    for col in sorted(all_cols):
        new_val = new_row.get(col, "<missing>")
        old_val = old_row.get(col, "<missing>")
        norm = normalize_keywords if col == "keywords" else normalize
        if norm(new_val) != norm(old_val):
            diffs.append((col, old_val, new_val))

    if not diffs:
        print(f"  {bill_id} [{label}] — {GREEN}exact match{RESET}")
        return

    print(f"\n{'='*60}")
    print(f"  {bill_id} [{label}] — {len(diffs)} diff(s)")
    print(f"{'='*60}")

    # Summary: one line per diff, starting a few words before first diff
    for col, old_val, new_val in diffs:
        diverge = next(
            (i for i, (a, b) in enumerate(zip(old_val, new_val)) if a != b),
            min(len(old_val), len(new_val)),
        )
        # Find start of 3rd word before the diverge point
        words_before = old_val[:diverge].split()
        start_word = " ".join(words_before[max(0, len(words_before) - 3):])
        start_idx = old_val[:diverge].rfind(start_word)
        start_idx = max(0, start_idx)
        prefix = "…" if start_idx > 0 else ""

        old_snip = prefix + old_val[start_idx:start_idx + TRUNC] + ("…" if start_idx + TRUNC < len(old_val) else "")
        new_snip = prefix + new_val[start_idx:start_idx + TRUNC] + ("…" if start_idx + TRUNC < len(new_val) else "")
        print(f"  {RED}{col}{RESET}")
        print(f"    old: {old_snip}")
        print(f"    new: {new_snip}")

    # Full values for long fields
    long_diffs = [(col, o, n) for col, o, n in diffs if len(o) > TRUNC or len(n) > TRUNC]
    if long_diffs:
        print(f"\n--- Full values ---")
        for col, old_val, new_val in long_diffs:
            diverge = next(
                (i for i, (a, b) in enumerate(zip(old_val, new_val)) if a != b),
                min(len(old_val), len(new_val)),
            )
            print(f"  {RED}{col}{RESET} (first diff at char {diverge})")
            print(f"    old: {old_val}")
            print(f"    new: {new_val}")
            print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("year", help="e.g. 2023")
    parser.add_argument(
        "file", choices=["details", "histories"],
        help="Which file to compare"
    )
    parser.add_argument(
        "--n", type=int, default=10, help="Number of bills to sample"
    )
    parser.add_argument(
        "--col", help="Only compare this column (e.g. primary_sponsors)"
    )
    parser.add_argument(
        "--bill", help="Compare a specific bill (e.g. HB17-1322)"
    )
    args = parser.parse_args()

    repo_root = Path(__file__).parent.parent
    bill_dir = repo_root / ".data" / "CO" / "bill"
    old_dir = bill_dir / ".old"

    if args.file == "details":
        new_data = load_csv(bill_dir / f"CO_Bill_Details_{args.year}.csv")
        old_data = load_csv(old_dir / f"CO_Bill_Details_{args.year}.csv")

        common = sorted(set(new_data) & set(old_data))
        if args.bill:
            sample = [args.bill] if args.bill in common else []
        else:
            sample = random.sample(common, min(args.n, len(common)))
        print(f"\nComparing {len(sample)} sampled bills — {args.year} details\n")

        for bill_id in sorted(sample):
            new_row = new_data[bill_id]
            old_row = old_data[bill_id]
            if args.col:
                compare_rows(
                    bill_id,
                    {args.col: new_row.get(args.col, "<missing>")},
                    {args.col: old_row.get(args.col, "<missing>")},
                )
            else:
                compare_rows(bill_id, new_row, old_row)

    else:  # histories
        new_hist = load_histories(
            bill_dir / f"CO_Bill_Histories_{args.year}.csv"
        )
        old_hist = load_histories(
            old_dir / f"CO_Bill_Histories_{args.year}.csv"
        )

        common = sorted(set(new_hist) & set(old_hist))
        if args.bill:
            sample = [args.bill] if args.bill in common else []
        else:
            sample = random.sample(common, min(args.n, len(common)))
        print(f"\nComparing {len(sample)} sampled bills — {args.year} histories\n")

        for bill_id in sorted(sample):
            new_rows = sorted(new_hist[bill_id], key=lambda r: int(r["order"]))
            old_rows = sorted(old_hist[bill_id], key=lambda r: int(r["order"]))

            max_rows = max(len(old_rows), len(new_rows))
            col_w = 55

            print(f"\n{'='*60}")
            print(f"  {bill_id} [histories]  old={len(old_rows)} rows  new={len(new_rows)} rows")
            print(f"{'='*60}")
            print(f"  {'OLD':<{col_w}} | NEW")
            print(f"  {'-'*col_w}-+-{'-'*col_w}")


            def summary(r):
                if r is None:
                    return ""
                return f"{r['order']:>2}. {r.get('action_date','')} {r.get('chamber','')[:10]}"

            # Index new rows by (date, normalized action)
            new_by_key: dict[tuple, list] = {}
            for r in new_rows:
                key = (r.get("action_date",""), normalize(r.get("action","")))
                new_by_key.setdefault(key, []).append(r)
            new_used: set[int] = set()

            # Build aligned pairs from old's perspective
            aligned = []
            for old_r in old_rows:
                key = (old_r.get("action_date",""), normalize(old_r.get("action","")))
                candidates = [
                    r for r in new_by_key.get(key, [])
                    if id(r) not in new_used
                ]
                if candidates:
                    new_r = candidates[0]
                    new_used.add(id(new_r))
                    aligned.append((old_r, new_r))
                else:
                    aligned.append((old_r, None))

            # Append any new rows not matched to old
            for r in new_rows:
                if id(r) not in new_used:
                    aligned.append((None, r))

            diffs_to_print = []
            for old_r, new_r in aligned:
                old_sum = summary(old_r)
                new_sum = summary(new_r)

                if old_r is None:
                    status = f"{RED}EXTRA{RESET}"
                    diffs_to_print.append(("EXTRA", "", f"{new_sum} | {new_r.get('action','')}"))
                elif new_r is None:
                    status = f"{RED}MISSING{RESET}"
                    diffs_to_print.append(("MISSING", f"{old_sum} | {old_r.get('action','')}", ""))
                elif old_r["order"] == new_r["order"]:
                    status = f"{GREEN}MATCH{RESET}"
                else:
                    status = f"{YELLOW}INCREMENT{RESET}"
                    diffs_to_print.append(("INCREMENT", f"{old_sum} | {old_r.get('action','')}", f"{new_sum} | {new_r.get('action','')}"))

                print(f"  {old_sum:<28} | {new_sum:<28} | {status}")

            notable = [(k, o, n) for k, o, n in diffs_to_print if k in ("MISSING", "DIFF", "EXTRA")]
            if notable:
                print(f"\n--- Details ---")
                for kind, old_full, new_full in notable:
                    if old_full:
                        print(f"  {RED}old:{RESET} {old_full}")
                    if new_full:
                        print(f"  {RED}new:{RESET} {new_full}")
                    print()


if __name__ == "__main__":
    main()
