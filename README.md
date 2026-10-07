# Repo tour

```text
.
├── .data/                # Scrape outputs + other estimation inputs
├── cli.py                # Run scraping, coding, or estimation
├── commem/               # Commemorative bill classification
├── estimate/             # LES estimation
├── scrape/               # Bill scraping
├── ss/                   # SS input parsing
├── utils/                # Shared input, term, and text handling
├── .gitignore
├── .lintr
├── .Rprofile
├── COVERAGE.md
├── DESCRIPTION
├── ESTIMATE.md
├── renv/
├── renv.lock
├── requirements.txt
└── README.md
```

```text
.data/
├── Zero_LES_legislators_Coded.csv # Reviewed legislator exclusions
└── <STATE>/                       # Inputs for one state's estimates
    ├── bill/                      # Scraped bill data (details and history files)
    ├── ss/                        # SS data
    │   └── raw/                   # ProjectVoteSmart html
    ├── commem/                    # Commem bill data
    ├── legiscan/                  # Legiscan (roster) data
    │   └── <SESSION>/csv/         # Organized by session (can 1+ w/i a term)
    │       └── people.csv         # Actual data files
    ├── review/                    # Manual matches and exclusions
    └── outputs/                   # Computed LES and bill-level reports
```

```text
scrape/
├── scrape.py             # Run the state's scraper
├── states/               # State-specific scrape logic
│   └── <STATE>.py
├── http.py               # Fetch legislature source pages
├── reporting.py          # Record scrape results and failed attempts
├── connor.py             # Run Connor's scrapers
└── legacy/               # Previous scraper generations
    ├── connor/           # Connor's scrapers
    └── nick/             # Nick's scrapers (VA HTTP + browser-retry code)
```

```text
estimate/
├── estimate.R            # Run the pipeline stages in order
├── pipeline/             # Transform inputs into LES outputs
│   ├── load_data.R       # Read bills, flags, and rosters
│   ├── clean_data.R      # Normalize bill ids, legislator names
│   ├── join_data.R       # Attach SS and commemorative flags
│   ├── compute_achievement.R   # Score bill progress from actions
│   ├── reconcile_legislators.R # Match sponsors to roster members
│   ├── validate_legislators.R  # Apply any zero-LES corrections
│   ├── calculate_scores.R      # Compute SLES over full term df
│   └── write_outputs.R         # Save member and bill-level results
├── states/               # State-specific estimation code
│   ├── loader.R          # Load and validate the requested <STATE>.R
│   ├── CO.py             # Colorado estimation from source inputs
│   └── <STATE>.R         # State settings + functions used by the pipeline
├── lib/                  # estimation lib
│   ├── bill_history.R    # score bill achievement based on actions (history)
│   ├── les_calc.R        # Weight bill progress and compute LES
│   ├── co.py             # Colorado bill stages and LES calculation
│   └── assertions.R      # Check pipeline data consistency throughout
└── legacy/               # Earlier estimation implementations
    └── connor/           # Prior rules and scoring reference
```

```text
ss/
├── ss.py                 # Interop with cli to parse PVS html into ss data
└── votesmart.py          # Actual PVS html validation and parsing logic
```

```text
commem/
├── commem.py             # Generate commemorative flags from bills
└── states.py             # State commem logic.
```

```text
utils/
├── cache.py              # HTML cache ops for scraping and PVS parsing
├── check_r_env.R         # Check R dependencies before estimation
├── term_rules.csv        # Define session years for each estimate
├── validate_term.py      # Validate scraping and estimation terms
├── term_to_inputs.R      # Map term arg to SS files and roster sessions
├── paths.R               # Path manipulation, mostly for navigating .data
├── libs.R                # Load R deps
├── logging.R             # General logging utils, for prompt/input too
└── strings.R             # String normalization ops throughout
```

# cli
## setup
Requires Python 3.11+ for the CLI (tested with 3.14.7). R estimation was tested
with R 4.6.1, the version recorded in `renv.lock`.

I (Nick) use `venv` and `Rscript`, which (I think?) come with most recent Python and R
distros. Feel free to use a different local setup; though the main cli `estimate` command 
expects `Rscript` to be available, except for Colorado's Python estimator.

Install dependencies from root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
Rscript -e 'renv::restore()'
```

For VA scraping, install Playwright's Chromium browser from the active Python
environment (also rerun after upgrading Playwright):

```bash
python -m playwright install chromium
```

On Linux, use `python -m playwright install --with-deps chromium` to include
required system libraries. See [Playwright browser setup](https://playwright.dev/python/docs/browsers).

AZ, DE, GA, and CO's browser fallback use Selenium with Chrome.
[Selenium Manager](https://www.selenium.dev/selenium/docs/api/py/) manages the
browser and driver when needed; allow internet access for its first-run downloads.
These browser steps are only needed for scraping.

The R lockfile covers the current estimator. After changing deps in
`DESCRIPTION`, run `renv::install()` and `renv::snapshot()` in R.
For R estimators, the `estimate` CLI command checks R deps and prints the restore command
if setup/update needed.

Run from the root with the project env active:

```bash
source .venv/bin/activate
python cli.py <STATE> <TERM> <COMMAND> [OPTIONS]
python cli.py --help
```

```text
ARGUMENTS
  STATE              Two-letter state postal code, e.g. AL
  TERM               Session years: YYYY_YYYY
                       AL:     2023_2026 (four years)
                       NJ/VA:  2026_2027 (even-starting two years)
                       Others: 2025_2026 (odd-starting two years)
                     For parse-ss: one year, e.g. 2025
                     For scrape-connor: omit TERM

COMMANDS
  scrape             Scrape bill details and histories
  parse-ss           Parse locally saved Vote Smart HTML into an annual SS CSV
  commem             Code commemorative bills
  estimate           Run LES estimation (R; Python for CO)
  scrape-connor      Run Connor's scraper using its own session selection

OPTIONS
  -h, --help         Show CLI help
  --verbose          Show detailed progress
  --force-fetch      Refresh cached data for supported bill scrapers
  --preview BILL_ID  Preview one Colorado bill (scrape; TERM optional)
  --retry-failed     Retry Colorado failed URLs (scrape; TERM required)
  --chamber upper|lower
                     Select a Missouri chamber (scrape-connor; default: both)
  --dry-run          Show Connor's script paths without running (scrape-connor)
```

```bash
python cli.py AL 2023_2026 scrape
python cli.py AK 2025_2026 scrape --verbose
python cli.py AL 2025 parse-ss
python cli.py VA 2024_2025 commem
python cli.py WI 2023_2024 estimate
python cli.py MO scrape-connor --chamber upper
python cli.py WI scrape-connor --dry-run
```

For `parse-ss`, save HTML pages in `.data/<STATE>/ss/raw/`, for example
`VoteSmart_AL_2025_H_1.html` and `VoteSmart_AL_2025_S_1.html`.
`.htm` and gzip-compressed HTML are also accepted. Each run rebuilds
`AL_SS_Bills_2025.csv` from the saved pages; it never logs in or downloads data.

Commands require the corresponding state implementation and inputs.


See [current-term coverage](COVERAGE.md) for scraper, SS, commemorative coding,
roster readiness, and implemented estimators by state. Before estimating fresh
scrape outputs, check [scraper compatibility](ESTIMATE.md#scraper-to-estimator-compatibility).
