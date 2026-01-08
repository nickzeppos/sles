# SLES Quick Ref

## pipeline overview

### Stage 1: Load Data (load_data.R)

Read raw CSV files from `.data/STATE/` into five dataframes:

- `bill_details`: Bill metadata, one row per bill per session. Raw column names vary by state. After state-specific preprocessing, keyed by (bill_id, session).
- `bill_history`: Action history, multiple rows per bill. Same state-specific preprocessing applies. Keyed by (bill_id, session), with `order` sequencing actions within each bill.
- `ss_bills`: Substantive & Significant bills, derived from PVS data. Keyed only by bill_id with no session identifier.
- `commem_bills`: Commemorative bill flags. Keyed by (bill_id, term, session).
- `legiscan`: Legislator metadata from Legiscan. Key columns: people_id, first_name, last_name, party, district.

### Stage 2: Clean Data (clean_data.R)

- `bill_details` is cleaned via state-specific hooks that standardize bill_id, term, and session, and derive a new LES_sponsor column from raw sponsor fields.
- `bill_history` is cleaned via a corresponding state-specific hook.
- `ss_bills` is filtered to state/term, bill_id standardized, and SS=1 flag added.

After cleaning, (bill_id, term, session) should uniquely identify a bill. The same bill_id can appear in different sessions (e.g., HB0001 in both Regular Session and Special Session are different bills).

### Stage 3: Join Data (join_data.R)

- `ss_bills` is prepared for joining:
    1. deduplicated by (bill_id, term, Title) - the year column is ignored. Bills with the same bill_id but different Titles are treated as distinct. Any bills with identical (bill_id, term, Title) combinations will trigger duplicate assertions downstream, requiring manual review.
    2. validated via state-specific `get_missing_ss_bills` hook that flags SS bills missing from bill_details unless they were intentionally excluded (e.g., committee-sponsored)
    3. joined to `bill_details` on bill_id only since SS data has no session column. Duplicate assertions check if bill_id collides across sessions - if so, the pipeline halts for manual review (consistent with original workflow, which produced `_edited.csv` files for these cases).
- `commem_bills` is joined to `bill_details` on (bill_id, term, session).

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

### Stage 6: Calculate Scores (calculate_scores.R)

- `bills` is renamed (LES_sponsor to sponsor) and validated to ensure all sponsors exist in `legis_data`. If any are missing, the pipeline halts with diagnostic output.
- LES scores are computed by joining bills to legis_data on (sponsor = data_name, chamber), weighting bill achievements (SS=10, regular=5, commemorative=1), and normalizing within chamber.

Output: `les_scores` with 39 columns including detailed breakdowns (all/ss/s/c bills × 5 stages).

### Stage 7: Write Outputs (write_outputs.R)

- Writes `{STATE}_LES_{term}.csv` (les_scores) and `{STATE}_coded_bills_{term}.csv` (bills with achievement matrix) to `.data/STATE/outputs/`.

### Known Issues

1. **SS year-duplicate deduplication:** Some bills appear in PVS data for multiple years within the same term with identical titles, but distinct year values. The original codebase's distinct() call doesn't catch these because it includes year in the key, so duplicates persist through the SS join and inflate row counts if left untreated. In the original codebase, this would recurringly error as "merge failed" without any meaningful off ramps or notes. My temp fix: group by (bill_id, term) and keep only the earliest year for identical titles. Ideally we'd just make the initial distinct() call more intelligent, but, trying to remain faithful for now. See ss_duplicate_investigation.ipynb for full walkthrough.

- Back and forth with Connor on this suggests WI 23-24 is the first time data has necessary conditions for this to happen. Briefly:
- We merge PVS into scraped bills data to properly code SS column.
- PVS files come w/ the following columns: Date,State,Bill No,Title,Action.
- Among these, we use date, Bill No, and Title to help us identify the bill.
- In a single term, in a single session, a bill number will not refer to two distinct bills. 
- However, in all states, when there are multiple sessions, bill numbers will can be reused (often in the form of bill numbers restarting upon each new session). 
- This means, e.g., we could see two HB1 bills in a single term.
- On the bill side, distinguishing between these two bills is not difficult, because all records come with a session variable.
- This is not as easy on the PVS side, where it we do not have a session column.
- There is a manual review off-ramp that provides for us the Title of the two bills on the scraped data side, and asks us to flag a the records for removal that do NOT correspond to the bill listed on the PVS side.
- In general, this off-ramp is made to catch the case described above, where multiple sessions have produced duplicate bill numbers that can be resolved on the scraped bill side, but need to be disambiguated with respect to the PVS side such that the proper row can receive SS = 1.
- In the case where the reused bill number appears twice on the PVS side as well, and both records correspond to each member of the pair of reused bill number, no disambiguation is needed, since both should in this case be coded as SS = 1.
- Before the join and check for necessary disambiguation, the PVS data is deduplicated. 
- This is because we have an expectation that even if there are no duplicated bill id's caused by the multi-session case described above, we nonetheless expect some duplicates to naturally occur in PVS.
- This is because PVS is a record of key votes, not key bills. And so our SS definition is actually "those bills which per PVS receied key votes," rather than "those bills which PVS calls important."
- In some state scripts, previous code authors have used the year column, derived from the Date column, in the PVS data as an attempt to approximate session.
- In practice, this means including year in the distinct call on the PVS data.
- The rationale here being, if a bill number is an authentic, multi-session dupe, the records will have different year column values.
- This is complicated, though, by fact that about half of our states are carry-over states. That is, bill numbers are carried over from one year to the next, within a single term + session.
- I.e., in these states, it is not necessary for legislators to re-introduced bills, under new bills numbers, in the second year of a given term + session.
- In these states, therefore, the occurrence of the same bill number with two different year column values is not necessarily an indication of an authentic, multi-session disambiguation case.
- This produced problems because of the way the old codebase checked for the join problem case described above-- that condition which triggered manual review.
- The old codebase looked for duplicate usage of the same PVS row post-join, effectively flagging on a one-to-many case from PVS -> scraped bill data.
- But in the case where the state was a carry-over state, and a PVS record with the same bill number occurred in multiple years, and the distinct call on the deduplication of the PVS df pre-join was still being used, this netted two different (different on year!) rows in the PVS record corresponding to some records in the scraped bills df.
- In cases where the relationship was many-to-many (that is, there were dupes on the scraped bills data side, as well, corresponding to multiple sessions), the manual review stage was triggered anyway, and this ended up being handled in manual review.
- In cases where there wasn't, i.e., only 1 of the three possible relationships occurred, it didn't trigger.
- This evenutally led to an undocumented script failure.
- And, apparently, WI 23-24 was the first state + term to have this odd feature.


## Helpful for claude code ref, if using

## migration notes
Porting SLES from .dropbox/ to clean Git structure. Only doing estimation for now, not commem or compile.

TODO:  

## .data dir structure
- bill/          Bill details & histories
- commem/        Commemorative bills
- legiscan/      Legislator metadata by session
- ss/            Substantive & Significant bills
- outputs/       Generated LES scores

## general architectural notes
CLI → Operation Modules (estimate, commem, compile)
- Modules are standalone (dual CLI/Rscript usage)
- CLI orchestrates multi-op workflows
- Shared code in utils/ (paths, logging, libs, strings, +more as needed)

## code style (lintr rules)
- Implicit returns (no `return()`)
- Max 80 char lines
- snake_case naming
- Space before `(` in control flow (if, while, for), not function calls
- Prefer `for (i in seq_len(nrow(df)))` over `for (i in 1:nrow(df))`

## some design principles
- estimate.R: Thin orchestrator, calls pipeline stages sequentially
- Pipeline stages: Focused modules (~100-150 lines), clear input/output
- State files: Minimal - config + state-specific hooks only
- Generic logic in pipeline, state quirks in state files

---
Updated: 2026-01-02
