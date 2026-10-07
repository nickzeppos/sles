# LES estimation

See [current-term coverage](COVERAGE.md) for each state's scraper, SS,
commemorative coding, and roster readiness.
It also lists implemented estimators; implementation alone does not establish
that an estimator accepts the current scraper's outputs or works for a new term.

## Why does the procedure look like this?
The procedure looks like this because Nick picked up 23/24/25 halfway through,
and implemented things faithful to how they were working in the CEL post-doc's
repo (the `SLES`) dropbox. It can be improved in many ways. This is just the
current state!

## Practical overview

You need three kinds of input to estimate LES for a state and term: bill data,
bill importance data, and a legislator roster.

**“Scraped” bill data.** Run `python cli.py <STATE> <TERM> scrape`. Most scrapers
read the legislature's website directly; some use a browser or download bulk
files. Outputs live in `.data/<STATE>/bill/`, named
`<STATE>_Bill_Details_<TERM>.csv` and `<STATE>_Bill_Histories_<TERM>.csv`.
Some states, including Colorado, use annual files instead. Details contain bill
metadata such as titles and sponsors. Histories contain legislative actions,
with multiple rows per bill. In some states, bill numbers start over between
sessions, so session identifiers matter when joining the two.

**Bill importance data.** CEL classifies bills as commemorative (C), substantive
(S), or substantive and significant (SS). For state LES, a bill receives SS status
if it is the subject of a Project Vote Smart (PVS) “key vote.” Commemorative
classification uses text patterns and exclusions applied to bill titles and,
depending on the state, other text fields. Bills with neither designation are S;
SS takes precedence if a bill receives both flags.

For SS, manually save all relevant PVS bill-table pages for each year of the term
in `.data/<STATE>/ss/raw/`, using names such as `VoteSmart_WI_2023_H_1.html`.
Run `python cli.py <STATE> <YEAR> parse-ss` for each year. This creates annual SS
CSVs in `.data/<STATE>/ss/`; it does not log in or download pages. Where bill
numbers repeat across sessions, you may also need to review which session each
PVS entry belongs to.

For C, run `python cli.py <STATE> <TERM> commem` after scraping. It writes
`.data/<STATE>/commem/<STATE>_Commem_Bills_<TERM>.csv`. Check the classifications
and adjust the state-specific rules when needed. The command skips an existing
output, so move that file aside before regenerating it after input or rule changes.

**Term roster data.** Not every legislator sponsors a bill, but eligible members
with no sponsored bills still belong in the estimate with LES = 0. We therefore
need a roster independent of the scraped sponsor names. Download LegiScan's
`people.csv` for each relevant regular and special session and place it under
`.data/<STATE>/legiscan/<SESSION>/csv/people.csv`. The estimator combines these
rosters and matches them to bill sponsors. Review ambiguous or unmatched names;
a missing match can reflect a naming problem rather than zero sponsorship.

With those inputs prepared, run `python cli.py <STATE> <TERM> estimate`. Resolve
any sponsor or SS matching problems it flags, and review whether zero-bill roster
members should be included or excluded. Saved decisions live in
`.data/<STATE>/review/`. The completed run writes legislator scores and coded-bill
results to `.data/<STATE>/outputs/`. See [README.md](README.md#cli) for environment
setup and CLI usage.

Support is separate for each step; having a scraper does not mean the state's
estimator is implemented:

- `scrape` needs `scrape/states/<state>.py`, which handles that legislature's website or bulk files.
- `commem` needs an entry in `commem/states.py`, which identifies the input fields and classification rules.
- `estimate` needs `estimate/states/<STATE>.R`, which supplies the state's cleaning, matching, and bill-progress logic. Colorado uses `estimate/states/CO.py` instead.
- `parse-ss` uses the shared Vote Smart table parser; it does not need a separate state module.

The CLI does not automatically fall back to legacy estimation scripts when a
current state implementation is missing. Existing input CSVs can be used without
rerunning the commands that originally produced them.

## Scraper-to-estimator compatibility

Current scraping and estimation are at different stages of migration. Many R
estimators still read the previous generation's filenames and columns. A
successful scrape and an implemented estimator do not yet establish that the
two work together. This also matters when rescraping the previous term.

A source review on 2026-10-07 confirmed these filename mismatches. Each example
uses bill details; the corresponding histories have the same mismatch.

| State | Current scraper writes | Estimator selects |
|---|---|---|
| WI | `WI_Bill_Details_2025_2026.csv` | `WI_Bill_Details_2025.csv` |
| AZ | `AZ_Bill_Details_2025_2026.csv` | Files beginning `AZ_Bill_Details_2025_2026_` |
| AR | `AR_Bill_Details_2025_2026.csv` | Assembly-name suffix from the state's term map |
| ME | `ME_Bill_Details_2025_2026.csv` | Legislature-number suffix |
| MN, NC | `<STATE>_Bill_Details_2025_2026.csv` | `<STATE>_Bill_Details_2025_RS.csv` |
| MO | `MO_Bill_Details_2025_2026.csv` | Separate assembly-numbered House and Senate files |

These are confirmed examples, not a complete compatibility audit. Matching
filenames alone would not verify columns, session identifiers, sponsor fields,
or action interpretation. When adapting an estimator, check its file-loading
and cleaning functions against both CSV headers and sample records, then verify
bill-stage results before comparing scores. Renaming a file alone is not a
validated conversion. Preserve inputs needed to reproduce the previous term.

Colorado's 2023–24 source-input estimate has been checked against retained
reference results, as described below. That check does not validate its 2025–26
estimate or other states' current-generation inputs.

## Before treating scores as final

- Review the [scrape manifest and latest attempt](COVERAGE.md#scrape-run-manifests),
  confirm session coverage, and resolve or document source gaps.
- Check SS coverage for every year and resolve reused bill numbers across
  sessions. Spot-check commemorative classifications against bill titles.
- Resolve unmatched or ambiguous sponsors against the roster, including members
  who changed chambers. Review saved zero-bill inclusion/exclusion decisions.
- Check a sample of bills against source histories for committee action,
  passage, and enactment, especially where state rules changed.
- Review member counts, bill counts, and unusual score or rank changes against
  the previous term. Investigate differences without assuming they are errors;
  confirm that each difference reflects the inputs or intended scoring rules.

## Shared R pipeline

[estimate/estimate.R](estimate/estimate.R) runs the shared stages in
[estimate/pipeline/](estimate/pipeline/). Each state's settings and functions
live in `estimate/states/<STATE>.R`; `loader.R` loads that file for the pipeline.
Earlier estimation implementations are preserved in [estimate/legacy/](estimate/legacy/).

### Stage 1: Load Data (load_data.R)

Read raw CSV files from `.data/STATE/` into five dataframes:

- `bill_details`: Bill metadata, one row per bill per session. Raw column names vary by state. After state-specific preprocessing, keyed by (bill_id, session).
- `bill_history`: Action history, multiple rows per bill. Same state-specific preprocessing applies. Keyed by (bill_id, session), with `order` sequencing actions within each bill.
- `ss_bills`: Substantive & Significant bills from annual PVS CSVs. Raw PVS data includes dates and bill numbers, but no session identifier.
- `commem_bills`: Commemorative bill flags. Keyed by (bill_id, term, session).
- `legiscan`: Legislator metadata from Legiscan. Key columns: people_id, first_name, last_name, party, district.

SS files are selected for every year in the term. LegiScan rosters are combined
from sessions starting within the term and deduplicated by `(people_id, role)`
to preserve legislators who appear in both chambers.

### Stage 2: Clean Data (clean_data.R)

- `bill_details` is cleaned via state-specific hooks that standardize bill_id, term, and session, and derive a new LES_sponsor column from raw sponsor fields.
- `bill_history` is cleaned via a corresponding state-specific hook.
- `ss_bills` is filtered to state/term, bill_id standardized, and SS=1 flag added.

After cleaning, (bill_id, term, session) should uniquely identify a bill. The same bill_id can appear in different sessions (e.g., HB0001 in both Regular Session and Special Session are different bills).

### Stage 3: Join Data (join_data.R)

- State functions can transform SS bill identifiers and identify intentionally excluded bills when checking for missing SS matches.
- Where provided, `enrich_ss_with_session` assigns sessions using state-specific logic, including reviewed session-resolution CSVs.
- SS rows are deduplicated by `(join identifier, term, Title)`, ignoring year. The identifier is normally `bill_id`; states can provide an alternative.
- Session-resolved SS rows join on identifier and session. Rows without a resolved session join on identifier alone. Checks flag duplicate or inconsistent joins.
- Commemorative flags join on `(bill_id, term, session)`. SS classification takes precedence over commemorative classification.

Output: `bill_details` with SS and commem flags added.

### Stage 4: Compute Achievement (compute_achievement.R)

- For each row in bill_details, filter bill_history on (bill_id, session) and evaluate legislative stages achieved. Relies on state-specific hook to interpret bill action => step achievement correspondence.

Output: `leg_achievement_matrix` keyed by (bill_id, term, session, LES_sponsor, state) with stage columns (introduced, action_in_comm, action_beyond_comm, passed_chamber, law).

### Stage 5: Reconcile Legislators (reconcile_legislators.R)

- `bills` is created by joining `bill_details` to `leg_achievement_matrix` on shared columns (effectively bill_id, term, session, LES_sponsor).
- `all_sponsors` is derived by grouping bills by (LES_sponsor, chamber, term) and computing aggregate stats (num_sponsored_bills, sponsor_pass_rate, sponsor_law_rate). A `match_name_chamber` key is added as `tolower(LES_sponsor)-chamber_code` (e.g., "j. smith-h").
- `legiscan` is prepared with its own `match_name_chamber` key via state-specific logic that disambiguates legislators sharing last names (using initials or full first names as needed).
- `all_sponsors` is fuzzy-matched to `legiscan` via inexact_join on match_name_chamber. State-specific hooks handle custom match overrides of problematic cases.

Output: `legis_data` with key columns (sponsor, data_name, people_id, chamber, party, district) and sponsor stats.

### Stage 5.5: Validate Legislators (validate_legislators.R)

Review legislators with zero sponsored bills. The estimator reads or creates
`.data/<STATE>/review/<STATE>_Zero_LES_legislators_Coded.csv` and asks whether to
reuse existing decisions or review the entries again.

Entries marked `not_actually_in_chamber = TRUE` are excluded; retained zero-bill
legislators receive LES = 0. Decisions are saved for later runs. The current
pipeline does not read the older combined `.data/Zero_LES_legislators_Coded.csv`.

### Stage 6: Calculate Scores (calculate_scores.R)

- `bills` is renamed (LES_sponsor to sponsor) and validated to ensure all sponsors exist in `legis_data`. If any are missing, the pipeline halts with diagnostic output.
- LES scores are computed by joining bills to legis_data on (sponsor = data_name, chamber), weighting bill achievements (SS=10, regular=5, commemorative=1), and normalizing within chamber.

The estimator also calculates `LES_nw` with equal weights for all bill types.

Output: `les_scores`, including scores, ranks, legislator attributes, and bill counts by type and achievement stage.

### Stage 7: Write Outputs (write_outputs.R)

Writes two files to `.data/<STATE>/outputs/`:

- `<STATE>_LES_<term>.csv`: legislator scores and supporting measures.
- `<STATE>_coded_bills_<term>.csv`: bill identifiers, sponsors, achievement stages, SS and commemorative flags, and source URLs where available.

## Common Issues After First Run

When running estimation for a new state/term, watch for these warning messages that indicate manual fixes are needed:

### 1. Duplicate Matches (Fuzzy Matching Errors)

- Warning
  - `Found duplicate matches - multiple legiscan records matched to same sponsor name`
- What it means
  - Multiple legislators in legiscan data are fuzzy-matching to the same bill sponsor. This typically happens when:
    - Two people have similar names (e.g., "J. Smith" matches both "John Smith" and "Jane Smith")
    - A legislator appears twice in legiscan with inconsistent role/district data (data error)
- How to fix
  - Add custom_match entries in the state's `reconcile_legiscan_with_sponsors` hook to either:
    - Explicitly map ambiguous names to the correct person (e.g., `"j. smith-h" = "john smith-h"`)
    - Exclude incorrect matches by mapping to NA (e.g., `"j. smith-s" = NA_character_`)
- Example
  - WI 2023_2024 had LaTonya Johnson appearing with both role="Rep" and role="Sen" but same district="SD-006" (Senate district), causing both to match as Senate. Fixed by filtering out the incorrect entry in `adjust_legiscan_data`.

### 2. Unmatched Bill Sponsors

- Warning
  - `X bill sponsors not matched to legislators`
- What it means
  - Bills have sponsors that couldn't be matched to any legislator in legiscan data. Common causes:
    - Name spelling differences: Sponsor appears as "McDonald" in bills but "Mcdonald" in legiscan
    - Chamber switchers: Legislator switched chambers mid-term and legiscan deduplication removed one chamber entry
    - Missing legislators: Person sponsored bills but isn't in legiscan data
- How to fix
  - Check the printed unmatched sponsor names
  - For name spelling issues: Add name correction in `clean_sponsor_names` hook
  - For chamber switchers: Verify `distinct(people_id, role)` is used in load_data.R (keeps both chamber entries)
  - For legiscan inconsistencies: Add manual adjustments in `adjust_legiscan_data` hook
- Example
  - WI 2023_2024 had Dan Knodl switching House→Senate mid-term. Initial `distinct(people_id)` only kept one chamber entry. Fixed by changing to `distinct(people_id, role)` to preserve both.

### 3. Unmatched Legiscan Entries

- Warning
  - `X legiscan records unmatched to bill sponsors`
- What it means
  - Legislators appear in legiscan data but have no bills in the sponsor data. This is often legitimate (legislators who sponsored zero bills), but can indicate:
    - Name formatting mismatches between legiscan and bill data
    - Legislators who served but didn't sponsor bills (should appear with LES=0)
    - Data errors (person shouldn't be in roster for this term)
- How to fix
  - Check if these are legitimate zero-bill sponsors (common for newly elected members mid-term)
  - If name formatting issues, add corrections in state-specific hooks
  - If data errors, use the zero-LES legislator review workflow to mark them as `not_actually_in_chamber`

### 4. Missing SS Bills

- Warning
  - `X SS bills not found in bill details`
- What it means
  - Bills marked as Substantive & Significant in PVS data don't exist in the scraped bill details. Common causes:
    - Committee-sponsored bills: SS list includes bills sponsored by committees (excluded from LES calculation)
    - Bill ID formatting mismatches: SS data has "HB 123" but bills have "HB0123"
    - Genuinely missing bills: Data collection missed these bills
- How to fix
  - Implement `get_missing_ss_bills` hook to identify intentionally excluded bills (e.g., committee-sponsored)
  - Check bill_id formatting consistency between SS data and bill_details
  - For genuinely missing bills, investigate data collection issues
- Example
  - AR 2023_2024 had 3 committee-sponsored SS bills. Fixed by implementing `get_missing_ss_bills` hook that excludes sponsors like "Joint Budget Committee".

### 5. Chamber Switchers (Multiple Chamber Entries)

- Note
  - Not a warning, but affects output: Legislators who switched chambers mid-term should appear twice in LES output (once per chamber).
- What to check
  - Does legislator appear in both House and Senate rows?
  - Are bill counts distinct (split appropriately by chamber)?
  - Are LES scores calculated separately for each chamber?
- Expected behavior
  - Following historical state-level approach, chamber-switchers appear in BOTH chambers with separate scores. This differs from federal practice where editorial determination assigns to one chamber.
- Example
  - Dan Knodl served in both chambers during WI 2023_2024; inspect each chamber entry separately.

### Matching and SS deduplication details

Bill-to-legislator assignment in the score calculation uses exact sponsor-name
matching within chamber, after the earlier roster reconciliation. This avoids
substring collisions such as `rye` matching `puryear`.

PVS records key votes, so the same bill can appear more than once, including in
multiple years of a carry-over session. The SS join deduplicates on identifier,
term, and title rather than year. Reused bill numbers across sessions still need
state-specific session matching; repeated appearances in PVS alone do not resolve
which session a bill belongs to.

## State-Specific Oddities

### Colorado

Colorado runs through its Python implementation in
[estimate/states/CO.py](estimate/states/CO.py) and
[estimate/lib/co.py](estimate/lib/co.py):

```bash
python cli.py CO 2023_2024 estimate
```

It uses annual scraped bill and SS files, the term's commemorative CSV, and
LegiScan `people.csv` files from sessions within the term. No prior LES output
is required. Rosters are deduplicated by legislator ID and chamber; district
prefixes identify chambers because some Colorado role labels are incorrect.

Zero-bill members require decisions in
`.data/CO/review/CO_Zero_LES_legislators_Coded.csv`. The estimator writes missing
entries and stops for review; set `not_actually_in_chamber` to `TRUE` or `FALSE`
and rerun. The supplied 2023–24 decision preserves the Robert Rankin exclusion.

Outputs use the usual `.data/CO/outputs/CO_LES_<term>.csv` and
`CO_coded_bills_<term>.csv` paths. The Python output identifies members with
`roster_id` and `roster_id_col`. Saved reference results under `outputs/reference/`
are comparison fixtures only. The 2023–24 run matches their scores and bill
assignments; two internal name labels now follow LegiScan.

Current-term estimation uses the same source-input procedure, but 2025–26 has not
been validated as a complete estimate. Its SS and commemorative inputs and any
new review decisions must be supplied before estimating.


### Kansas

Kansas derives `LES_sponsor` from `original_sponsor` and `requested_by` in
[estimate/states/KS.R](estimate/states/KS.R). The examples below describe the
sponsor-selection hierarchy.

Where an identifiable legislator appears in the original_sponsor field, that is the LES_sponsor. This can happen one of two ways:

1. One identifiable legislator. (SB0001)

```
("Senator Steffen", "") => "steffen"
```

2. Multiple identifiable legislators. (SB0012)

```
("Senator Thompson; Senator Steffen", "") => "thompson"
```

In cases where no identifiable sponsor appears in the original_sponsor field, requested_by can present one of five ways (independent of name variants):

1. One identifiable legislator. (SB0021)

```
("Committee on Assessment and Taxation", "Senator Faust-Goudeau") => "faust-goudeau"
```

2. Multiple identifiable legislators. (HB2690 - not a real example, illustrative only)

```
("Committee on Corrections and Juvenile Justice", "Representative Barth and Representative Schmoe") => "barth"
```

3. An identifiable legislator, on behalf of a non-legislative entity. (SB0039)

```
("Committee on Federal and State Affairs", "Senator Bowers on behalf of Capitol Preservation Committee") => "bowers"
```

4. An identifiable legislator, on behalf of an identifiable legislator and a non-legislative entity. (HB2537)

```
("Committee on Local Government", "Representative Blex on behalf of Representative Bryce and the City of Independence") => "bryce"
```

5. An identifiable legislator, on behalf of multiple legislators. (HB2690)

```
("Committee on Energy, Utilities and Telecommunications", "Representative Delperdang on behalf of Representative Hoffman and Representative Carmichael") => "hoffman"
```

Patterns 4 and 5 handle requestor formats encountered in the 2023–2024 data.
