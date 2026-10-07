# Working in this repo

## Purpose and scope

This repo gives CEL postdocs the code and inputs to reproduce the previous
term's state LES estimates and collect bill data for the upcoming estimates.
Current-term work is primarily scraping; do not imply that every state can
already be estimated end to end.

The handoff covers these session-year windows:

| States | Previous term | Current term |
|---|---|---|
| AL | 2019_2022 | 2023_2026 |
| NJ, VA | 2024_2025 | 2026_2027 |
| All others | 2023_2024 | 2025_2026 |

Use session years, not election years. `utils/term_rules.csv` defines alignment;
Python validates through `utils/validate_term.py`, and R resolves inputs through
`utils/term_to_inputs.R`. Keep those conventions consistent.

Read [README.md](README.md) for layout and commands,
[ESTIMATE.md](ESTIMATE.md) for estimation and manual review, and
[COVERAGE.md](COVERAGE.md) for state readiness and scrape strategies.

## Code organization

- Keep `cli.py` as the single public CLI. Put implementation in its module;
  do not add nested CLIs or extra top-level dispatch files.
- Current bill scrapers live in `scrape/states/<state>.py`. Earlier generations
  belong in `scrape/legacy/`, separated by generation.
- `ss/ss.py` resolves saved HTML inputs and output paths; `ss/votesmart.py`
  parses and validates them. SS is an offline parsing workflow. Do not restore
  the retired login/download commands as part of routine maintenance.
- `commem/` handles commemorative bill classification. Preserve state-specific
  patterns, exclusions, and reviewed overrides when porting rules.
- `estimate/estimate.R` runs the shared R pipeline with functions from
  `estimate/states/<STATE>.R`. Colorado uses `estimate/states/CO.py`.
  Keep older implementations in `estimate/legacy/`; do not silently fall back
  to them when a current state module is missing.
- Share fetching and reporting mechanics where useful, while keeping source
  interpretation in state modules. `utils/cache.py` is shared by scraping and
  SS parsing; it does not belong exclusively to either module.

## Repository data

- `.data/<STATE>/` holds bill CSVs, SS flags, commemorative flags, LegiScan
  rosters, manual review inputs, and LES outputs for the retained terms.
- `review/` is for decisions consumed by estimation, such as sponsor/session
  matches and zero-bill exclusions, rather than diagnostic reports.
- Respect `.gitignore`. Shared guides must not depend on uncommitted files for
  required instructions or inputs.
- Preserve source data and reviewed decisions when checking changes. Use
  temporary directories for verification outputs rather than overwriting
  retained estimates.

## Scraping and estimation correctness

Use `scrape/reporting.py` for current full scrapes: decorate the state entry
with `scrape_run` and publish through `write_manifest` after CSV publication.
Retain state-specific findings; register new exception fields with the shared
reporter when they belong in its normalized exception list.

Keep the successful manifest distinct from the latest attempt report. A failed
rerun can leave an older success, and an interrupted process can leave a
`running` attempt. Stage CSV writes and invalidate the old success marker before
replacing outputs. Do not publish success for incomplete collection.

A working scraper, available inputs, an implemented estimator, and a validated
estimate are different claims. Update coverage only with evidence for the
specific claim and term. Preserve the audit date and identify the evidence used.

Before connecting a new scraper to an estimator, check filenames, CSV columns,
session identifiers, sponsor conventions, and action meanings. Several R
loaders still expect previous-generation outputs; renaming files alone does
not validate compatibility. Preserve previous-term reproducibility.

Check SS assignments when bill numbers repeat across sessions, preserve members
who changed chambers, and review zero-bill roster members. Keep source gaps and
manual exclusions visible. Never manufacture missing inputs or change scoring
rules merely to make a run finish or match a reference score.

## Dependencies and verification

Follow the environment setup in README. Python packages
belong in `requirements.txt`; R dependencies belong in `DESCRIPTION` and
`renv.lock`. Preserve the CLI's R dependency preflight. Browser setup is in the
README and is separate from installing Python packages.

Run checks appropriate to the change. Prefer fixtures and cached source
samples for parser work; a full live scrape is not a routine test. Compare bill
assignments, stages, counts, and scores when changing estimation logic, and
report what was actually checked. Tests are excluded from commits under the
repository's current policy. Documentation-only changes need
link and structure checks rather than new tests.

## Documentation style

Keep README focused on the repo tour, setup, and CLI commands. Explain the
procedure and manual decisions in ESTIMATE; record state coverage in COVERAGE.
Avoid copying readiness counts into multiple documents.

Use plain, specific language: “scrapers,” “bill details and histories,” and
“LES estimation.” Tree comments should explain a file's role in that procedure,
stay short, and avoid descriptions of incidental files. List uncommented tree
entries after the described entries within their group.

COVERAGE uses an HTML table so each state's scrape strategy can span the row
beneath its status cells. Preserve all 50 states, source links, and column spans
when editing it. Describe the strategy implemented in code without implying a
successful run where none has been verified.
