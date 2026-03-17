# Nebraska (NE) State Configuration
#
# NE is the only state with a unicameral legislature.
# Chamber is "U" during processing and "Senate" in final output.
# All in_majority = 0 because the legislature is officially nonpartisan.
#
# Bill types: LB (Legislative Bills)
# Bills carry over during regular sessions (one biennium).
# Special session bill numbers restart.
#
# Special sessions are identified via manually coded document IDs
# (b_id_doc_id = bill_id-DocumentID), with hardcoded term-specific lists.
# Bill histories for specials are recoded by date range.
#
# No cosponsorship data available.
#
# Name matching: sponsors use full name (lowercased, accents removed).
# Match key has NO chamber suffix (unicameral - single chamber).

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
logging <- source(
  file.path(repo_root, "utils/logging.R"),
  local = TRUE
)$value
libs <- source(
  file.path(repo_root, "utils/libs.R"),
  local = TRUE
)$value
strings <- source(
  file.path(repo_root, "utils/strings.R"),
  local = TRUE
)$value

# Extract functions
cli_log <- logging$cli_log
cli_warn <- logging$cli_warn
require_libs <- libs$require_libs
standardize_accents <- strings$standardize_accents

# Load required libraries
require_libs()
library(dplyr)
library(glue)
library(stringr)
library(readr)

# Stage 1 Hook: Get bill file suffix
#' NE bill files use full term as suffix:
#' e.g., NE_Bill_Details_2021_2022.csv
#'
#' @param term Term string (e.g., "2021_2022")
#' @return The term string itself
get_bill_file_suffix <- function(term) {
  term
}

# State configuration
# nolint start: line_length_linter
ne_config <- list(
  bill_types = c("LB"),

  # Nebraska unicameral flag - passed to evaluate_bill_history
  # Bypasses chamber-of-introduction filtering (only one chamber)
  nebraska = TRUE,

  # Step terms for evaluating bill history (old script lines 374-382)
  step_terms = list(
    aic = c(
      "notice of hearing", "public hearing", "com am[0-9]+"
    ),
    abc = c(
      "placed on general file", "placed on select file",
      "placed on final", "advanced to", "engross", "enroll",
      "^passed", "adopted$", "lost$", "pending$"
    ),
    pass = c(
      "^passed on", "^passed by", "^passed notwith",
      "president.+signed", "speaker.+signed"
    ),
    law = c(
      "approved by gov", "passed notwithstanding", "certificate"
    )
  )
)
# nolint end: line_length_linter

# ============================================================
# Helper: Appropriations bill ID cleaning
# ============================================================
# Bills with 'A' appended (e.g., LBA0001) need reformatting to LB0001A
# Same pattern for LR and LRC prefixes (old script lines 122-124, 348-351)
clean_appropriations_bill_id <- function(bill_id) {
  bill_id <- ifelse(
    grepl("A[0-9]", bill_id),
    paste0(gsub("LBA", "LB", bill_id), "A"),
    bill_id
  )
  bill_id <- ifelse(
    grepl("A[0-9]", bill_id),
    paste0(gsub("LRA", "LR", bill_id), "A"),
    bill_id
  )
  bill_id <- ifelse(
    grepl("A[0-9]", bill_id),
    paste0(gsub("LRCA", "LRC", bill_id), "A"),
    bill_id
  )
  bill_id
}

# ============================================================
# Helper: Special session bill lists
# ============================================================
# Manually coded document IDs (b_id_doc_id = bill_id-DocumentID)
# From old script lines 130-157
get_special_session_bills <- function(term) {
  if (term == "2007_2008") {
    # Special session started Nov 14, 2008
    c(
      "LB0001-6171", "LB0002-6197", "LB0003-6237",
      "LR0001-6245", "LR0002-6266", "LR0003-6265",
      "LR0004-6279", "LR0005-6283", "LR0006-6181",
      "LR0007-6296", "LR0008-6297", "LR0009-6304",
      "LR0010-6303", "LR0011-6280"
    )
  } else if (term == "2009_2010") {
    c(
      "LB0001-9421", "LB0002-9429", "LB0003-9428",
      "LB0004-9422", "LB0005-9433", "LB0006-9440",
      "LB0007-9441", "LB0008-9465", "LB0009-9466",
      "LB0010-9443", "LB0011-9457", "LB0012-9455",
      "LB0013-9438", "LB0014-9468", "LB0015-9453",
      "LB0016-9473",
      "LR0001-9388", "LR0002-9437", "LR0003-9436",
      "LR0004-9460", "LR0005-9312", "LR0006-9469",
      "LR0007-9471", "LR0008-9474", "LR0009-9480",
      "LR0010-9486", "LR0011-9488", "LR0012-9489",
      "LR0013-9490", "LR0014-9477", "LR0015-9475",
      "LR0016-9498", "LR0017-9472", "LR0018-9492",
      "LR0019-9503", "LR0020-9516", "LR0021-9523",
      "LR0022-9529", "LR0023-9518", "LR0024-9385",
      "LR0025-9539", "LR0026-9535", "LR0027-9536",
      "LR0028-9540", "LR0029-9538", "LR0030-9537",
      "LR0031-9559"
    )
  } else if (term == "2011_2012") {
    c(
      "LB0001-15149", "LB0002-15277", "LB0003-15295",
      "LB0004-15278", "LB0005-15121", "LB0006-15294",
      "LB0001A-15391", "LB0004A-15381",
      "LR0001-15246", "LR0002-15261", "LR0003-15281",
      "LR0004-15282", "LR0005-15283", "LR0006-15292",
      "LR0007-15293", "LR0008-15296", "LR0009-15173",
      "LR0010-15301", "LR0011-15297", "LR0012-15304",
      "LR0013-15302", "LR0014-15319", "LR0015-15318",
      "LR0016-15325", "LR0017-15305", "LR0018-15322",
      "LR0019-15345", "LR0020-15343", "LR0021-15356",
      "LR0022-15376", "LR0023-15386", "LR0024-15390",
      "LR0025-15395", "LR0026-15399", "LR0027-15400",
      "LR0028-15398", "LR0029-15394", "LR0030-15410",
      "LR0031-15418", "LR0032-15440", "LR0033-15441",
      "LR0034-15438", "LR0035-15435"
    )
  } else if (term == "2021_2022") {
    c(
      "LB0001-46675", "LB0002-46676", "LB0003-46677",
      "LB0004-46678", "LB0005-46671", "LB0006-46674",
      "LB0007-46673", "LB0008-46672", "LB0009-46696",
      "LB0010-46695", "LB0011-46683", "LB0012-46670",
      "LB0013-46698", "LB0014-46667", "LB0015-46687"
    )
  } else if (term == "2023_2024") {
    # nolint start: line_length_linter
    # 1st Special Session of 108th Legislature (Jul-Aug 2024)
    c(
      "LB0001-58119", "LB0002-58053", "LB0003-58118",
      "LB0004-58115", "LB0005-58120", "LB0006-58102",
      "LB0007-58084", "LB0008-58082", "LB0009-58052",
      "LB0010-58069", "LB0011-58092", "LB0012-58123",
      "LB0013-58107", "LB0014-58114", "LB0015-58061",
      "LB0016-58031", "LB0017-58101", "LB0018-58047",
      "LB0019-58085", "LB0020-58060", "LB0021-58106",
      "LB0022-58113", "LB0023-58117", "LB0024-58116",
      "LB0025-58023", "LB0026-58081", "LB0027-58133",
      "LB0028-58135", "LB0029-58132", "LB0030-58124",
      "LB0031-58148", "LB0032-58074", "LB0033-58144",
      "LB0034-58075", "LB0035-58129", "LB0036-58150",
      "LB0037-58143", "LB0038-58142", "LB0039-58072",
      "LB0040-58048", "LB0041-58022", "LB0042-58138",
      "LB0043-58137", "LB0044-58136", "LB0045-58216",
      "LB0046-58261", "LB0047-58272", "LB0048-58268",
      "LB0049-58265", "LB0050-58264", "LB0051-58262",
      "LB0052-58095", "LB0053-58108", "LB0054-58109",
      "LB0055-58267", "LB0056-58266", "LB0057-58098",
      "LB0058-58141", "LB0059-58271", "LB0060-58263",
      "LB0061-58097", "LB0062-58128", "LB0063-58270",
      "LB0064-58269", "LB0065-58094", "LB0066-58096",
      "LB0067-58110", "LB0068-58282", "LB0069-58111",
      "LB0070-58151", "LB0071-58065", "LB0072-58182",
      "LB0073-58248", "LB0074-58247", "LB0075-58245",
      "LB0076-58195", "LB0077-58244", "LB0078-58246",
      "LB0079-58147", "LB0080-58112", "LB0081-58089",
      "LR0025-58475",
      "LRC0001AAA-58087", "LRC0002AAA-58068",
      "LRC0003AAA-58077", "LRC0004AAA-58043",
      "LRC0006AAA-58104", "LRC0007AAA-58186",
      "LRC0014AAA-58193", "LRC0017AAA-58201",
      "LRC0018AAA-58202", "LRC0019AAA-58203",
      "LRC0020AAA-58204", "LRC0022AAA-58206",
      "LRC0023AAA-58273", "LRC0024AAA-58177"
    )
    # nolint end: line_length_linter
  } else {
    c()
  }
}

# ============================================================
# Helper: Special session date ranges for bill history
# ============================================================
# Bill histories don't have document IDs, so specials are identified by
# date range instead (old script lines 355-361)
get_special_session_date_range <- function(term) {
  if (term == "2007_2008") {
    list(start = as.Date("2008-11-01"), end = as.Date("2008-12-31"))
  } else if (term == "2009_2010") {
    list(start = as.Date("2009-11-01"), end = as.Date("2009-12-31"))
  } else if (term == "2011_2012") {
    list(start = as.Date("2011-11-01"), end = as.Date("2011-12-31"))
  } else if (term == "2023_2024") {
    # 1st Special Session: Jul 25 - Aug 21, 2024
    list(start = as.Date("2024-07-25"), end = as.Date("2024-08-31"))
  } else {
    NULL
  }
}

# ============================================================
# Stage 1.5: Preprocess raw data
# ============================================================
#' Rename bill_number -> bill_id in both dataframes.
#' From old script lines 119, 346.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  bill_details <- bill_details %>%
    rename(bill_id = "bill_number")

  bill_history <- bill_history %>%
    rename(bill_id = "bill_number")

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# ============================================================
# Stage 2: Clean bill details
# ============================================================
#' NE-specific transformations:
#' - Appropriations bill ID cleaning (LBA -> LB...A)
#' - Special session bill identification via b_id_doc_id
#' - Sponsor cleaning: lowercase, accent removal, title stripping
#' - Committee/board sponsored bill removal
#' - Name switch: Conrad -> Nantkes (2009_2010)
#'
#' From old script lines 117-336.
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2021_2022")
#' @param verbose Show detailed logging (default TRUE)
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Add term/session, clean appropriations IDs, create b_id_doc_id
  # (old script lines 118-125)
  bill_details <- bill_details %>%
    mutate(
      term = term,
      session = "RS",
      bill_id = clean_appropriations_bill_id(.data$bill_id),
      b_id_doc_id = paste(
        .data$bill_id,
        gsub(".+DocumentID=", "", .data$bill_url),
        sep = "-"
      )
    )

  # Recode special session bills (old script lines 162-164)
  spec_bills <- get_special_session_bills(term)
  if (length(spec_bills) > 0) {
    bill_details[
      bill_details$b_id_doc_id %in% spec_bills,
    ]$session <- "SS1"
  }

  # Remove helper column
  bill_details <- select(bill_details, -"b_id_doc_id")

  # Drop exact duplicates (old script line 168)
  bill_details <- distinct(bill_details)

  # Store unfiltered version
  all_bill_details <- bill_details

  # Derive bill type and filter to LB only (old script lines 172-174)
  bill_details <- bill_details %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    )

  if (verbose) {
    cli_log("Bill type distribution:")
    print(table(bill_details$bill_type))
  }

  bill_details <- bill_details %>%
    filter(.data$bill_type %in% ne_config$bill_types) %>%
    select(-"bill_type")

  if (verbose) {
    cli_log(glue("After filtering to LB: {nrow(bill_details)} bills"))
  }

  # Check for duplicate bill_ids within session (old script lines 178-181)
  if (nrow(distinct(bill_details, .data$bill_id, .data$session)) !=
    nrow(bill_details)) {
    cli_warn("CHECK FOR SPECIAL SESSION BILLS - duplicates found")
  }

  # Sponsor cleaning (old script lines 186-195)
  bill_details$primary_sponsor <- tolower(bill_details$primary_sponsor)
  bill_details$primary_sponsor <- standardize_accents(
    bill_details$primary_sponsor
  )
  bill_details$primary_sponsor <- gsub(
    "  +", " ", bill_details$primary_sponsor
  )

  # Clean titles/boards (old script line 195)
  bill_details$primary_sponsor <- gsub(
    "executive board: |, chairperson|speaker ",
    "",
    bill_details$primary_sponsor
  )

  # LES sponsor (old script line 198)
  bill_details$LES_sponsor <- bill_details$primary_sponsor

  # Term-specific name fixes (old script lines 201-204)
  if (term == "2009_2010") {
    # Danielle Nantkes changed name to Danielle Conrad mid-term
    # Keep as nantkes for matching (old script line 203)
    bill_details[
      bill_details$LES_sponsor == "conrad",
    ]$LES_sponsor <- "nantkes"
  }
  if (term == "2023_2024") {
    # "hansen, b." and "hansen" are both Ben Hansen
    bill_details[
      bill_details$LES_sponsor == "hansen, b.",
    ]$LES_sponsor <- "hansen"
  }

  # Drop committee/board-sponsored bills (old script lines 324-328)
  comm_bills <- bill_details %>%
    filter(grepl("committee| board", .data$LES_sponsor))

  if (nrow(comm_bills) > 0) {
    if (verbose) {
      cli_log(glue(
        "Dropping {nrow(comm_bills)} committee/board-sponsored bills"
      ))
    }
    bill_details <- bill_details %>%
      filter(!grepl("committee| board", .data$LES_sponsor))
  }

  # Drop empty/missing sponsors (old script lines 332-336)
  empty_bills <- bill_details %>%
    filter(.data$LES_sponsor == "" | is.na(.data$LES_sponsor))

  if (nrow(empty_bills) > 0) {
    if (verbose) {
      cli_log(glue(
        "Dropping {nrow(empty_bills)} bills without sponsor"
      ))
    }
    bill_details <- bill_details %>%
      filter(!(.data$LES_sponsor == "" | is.na(.data$LES_sponsor)))
  }

  list(
    all_bill_details = all_bill_details,
    bill_details = bill_details
  )
}

# ============================================================
# Stage 2: Clean bill history
# ============================================================
#' NE-specific bill history cleaning:
#' - Appropriations bill ID cleaning
#' - Add term/session
#' - Recode special session bills by date range
#'
#' From old script lines 341-365.
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2021_2022")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  # Add term/session, clean appropriations IDs (old script lines 345-351)
  bill_history <- bill_history %>%
    mutate(
      term = term,
      session = "RS",
      bill_id = clean_appropriations_bill_id(.data$bill_id),
      action_date = as.Date(.data$action_date)
    )

  # Recode special session bills by date range (old script lines 354-361)
  spec_bills <- get_special_session_bills(term)
  spec_bill_ids <- gsub("-.+", "", spec_bills)
  date_range <- get_special_session_date_range(term)

  if (length(spec_bill_ids) > 0 && !is.null(date_range)) {
    mask <- bill_history$bill_id %in% spec_bill_ids &
      bill_history$action_date >= date_range$start &
      bill_history$action_date <= date_range$end
    bill_history[mask, ]$session <- "SS1"
  }

  # Arrange (old script line 365)
  bill_history %>%
    arrange(
      .data$term, .data$session, .data$bill_id,
      .data$action_date, .data$order
    )
}

# ============================================================
# Stage 3: Transform SS bill IDs
# ============================================================
#' Fix SS bill IDs for specific terms.
#' 2021_2022: LB10001 -> LB0001, LB20002 -> LB0002
#'
#' From old script lines 214-217.
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return SS bills with corrected IDs
transform_ss_bills <- function(ss_bills, term) {
  if (term == "2021_2022") {
    ss_bills$bill_id[ss_bills$bill_id == "LB10001"] <- "LB0001"
    ss_bills$bill_id[ss_bills$bill_id == "LB20002"] <- "LB0002"
  }
  ss_bills
}

# ============================================================
# Stage 3: Enrich SS bills with session
# ============================================================
#' Map SS bills to correct session via human-reviewed CSV
#'
#' NE bill IDs restart in special sessions (SS1). A review CSV resolves
#' ambiguous bills; all others default to RS.
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return SS bills with session column added
enrich_ss_with_session <- function(ss_bills, term) {
  rs_session <- "RS"

  review_path <- file.path(
    repo_root, ".data", "NE", "review",
    "NE_SS_session_resolution.csv"
  )
  if (!file.exists(review_path)) {
    cli_warn(glue(
      "No SS session resolution file found at {review_path}. ",
      "All SS bills will use bill_id-only join."
    ))
    ss_bills$session <- NA_character_
    return(ss_bills)
  }

  review <- read.csv(review_path, stringsAsFactors = FALSE) %>%
    filter(.data$match == "Y") %>%
    select("bill_id", "pvs_title", "candidate_session")

  ss_bills$session <- NA_character_
  for (i in seq_len(nrow(ss_bills))) {
    resolved <- review %>%
      filter(
        .data$bill_id == ss_bills$bill_id[i],
        .data$pvs_title == ss_bills$Title[i]
      )
    if (nrow(resolved) == 1) {
      ss_bills$session[i] <- resolved$candidate_session
    }
  }

  n_resolved <- sum(!is.na(ss_bills$session))
  n_unresolved <- sum(is.na(ss_bills$session))
  if (n_resolved > 0) {
    cli_log(glue(
      "  {n_resolved} SS bill(s) resolved via review CSV"
    ))
  }
  if (n_unresolved > 0) {
    cli_log(glue(
      "  {n_unresolved} SS bill(s) unambiguous (bill_id-only join)"
    ))
  }

  ss_bills
}

# ============================================================
# Stage 3: Get missing SS bills
# ============================================================
#' Identify genuinely missing SS bills, excluding committee-sponsored.
#' From old script lines 224-233.
#'
#' @param ss_filtered SS bills dataframe
#' @param bill_details Filtered bill_details dataframe
#' @param all_bill_details Unfiltered bill_details dataframe
#' @return Dataframe of genuinely missing SS bills
get_missing_ss_bills <- function(ss_filtered, bill_details,
                                 all_bill_details) {
  missing_ss <- ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term"))

  if (nrow(missing_ss) == 0) {
    return(missing_ss %>% select("bill_id", "term"))
  }

  # Check all_bill_details for context (old script lines 228-232)
  missing_with_info <- missing_ss %>%
    left_join(
      all_bill_details %>% select("bill_id", "term", "summary"),
      by = c("bill_id", "term")
    )

  # Filter to valid bill types and exclude committee-sponsored
  genuinely_missing <- missing_with_info %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    ) %>%
    filter(.data$bill_type %in% ne_config$bill_types) %>%
    select(-"bill_type") %>%
    filter(
      !grepl("committee", .data$summary, ignore.case = TRUE)
    ) %>%
    select("bill_id", "term")

  genuinely_missing
}

# ============================================================
# Stage 4: Post-evaluate bill (status override)
# ============================================================
#' NE-specific status override logic.
#' If bill_stages coding misses something, the bill's status field
#' is used as a fallback.
#'
#' From old script lines 414-419.
#'
#' @param bill_stages Achievement result from evaluate_bill_history
#' @param bill_row The bill row from bill_details
#' @param bill_history Bill history dataframe for this bill
#' @return Modified bill_stages
post_evaluate_bill <- function(bill_stages, bill_row, bill_history) {
  bill_status <- bill_row$status

  if (bill_stages$law == 0 &&
    !is.na(bill_status) && bill_status == "Veto Overridden") {
    bill_stages$action_beyond_comm <- 1
    bill_stages$passed_chamber <- 1
    bill_stages$law <- 1
  } else if (bill_stages$passed_chamber == 0 &&
    !is.na(bill_status) &&
    bill_status %in% c("Governor Veto", "Passed")) {
    bill_stages$action_beyond_comm <- 1
    bill_stages$passed_chamber <- 1
  } else if (bill_stages$action_beyond_comm == 0 &&
    !is.na(bill_status) &&
    bill_status %in% c("Final Reading", "Select File")) {
    bill_stages$action_beyond_comm <- 1
  }

  bill_stages
}

# ============================================================
# Stage 5: Derive unique sponsors
# ============================================================
#' All chamber = "U" for unicameral.
#' From old script lines 484-492.
#'
#' @param bills Bills dataframe with achievement metrics
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregated metrics
derive_unique_sponsors <- function(bills, term) {
  bills %>%
    mutate(chamber = "U") %>%
    select("LES_sponsor", "chamber", "passed_chamber", "law") %>%
    mutate(term = term) %>%
    group_by(.data$LES_sponsor, .data$chamber, .data$term) %>%
    summarize(
      num_sponsored_bills = n(),
      sponsor_pass_rate = sum(.data$passed_chamber) / n(),
      sponsor_law_rate = sum(.data$law) / n(),
      .groups = "drop"
    )
}

# ============================================================
# Stage 5: Clean sponsor names
# ============================================================
#' NE match key uses full name WITHOUT chamber suffix (unicameral).
#' From old script lines 529-535.
#'
#' @param all_sponsors Sponsors dataframe
#' @param term Term string
#' @return Modified all_sponsors with match_name_chamber
clean_sponsor_names <- function(all_sponsors, term) {
  all_sponsors %>%
    arrange(.data$chamber, .data$LES_sponsor) %>%
    distinct() %>%
    mutate(
      match_name_chamber = tolower(.data$LES_sponsor)
    )
}

# ============================================================
# Stage 5: Adjust legiscan data
# ============================================================
#' NE match key uses name WITHOUT chamber suffix (unicameral).
#' Disambiguation: if 2+ legislators share last_name in same role,
#' use "X. Last_name" format.
#'
#' From old script lines 559-567.
#'
#' @param legiscan Legiscan dataframe
#' @param term Term string
#' @return Modified legiscan with match_name_chamber
adjust_legiscan_data <- function(legiscan, term) {
  legiscan %>%
    filter(.data$committee_id == 0) %>%
    group_by(.data$last_name, .data$role) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      match_name = case_when(
        .data$n >= 2 ~ paste0(
          substr(.data$first_name, 1, 1), ". ", .data$last_name
        ),
        TRUE ~ .data$last_name
      ),
      match_name_chamber = tolower(.data$match_name)
    )
}

# ============================================================
# Stage 5: Reconcile legiscan with sponsors
# ============================================================
#' Fuzzy matching with term-specific custom overrides.
#' From old script lines 575-609.
#'
#' @param all_sponsors Sponsors dataframe
#' @param legiscan_adjusted Adjusted legiscan dataframe
#' @param term Term string
#' @return Joined dataframe
reconcile_legiscan_with_sponsors <- function(all_sponsors,
                                              legiscan_adjusted,
                                              term) {
  if (term == "2019_2020") {
    joined <- inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "m. hansen" = "hansen, m.",
        "b. hansen" = "hansen, b."
      )
    )
  } else if (term == "2021_2022") {
    joined <- inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "m. cavanaugh" = "cavanaugh, m.",
        "m. hansen" = "hansen, m.",
        "b. hansen" = "hansen, b.",
        "jacobson" = NA_character_
      )
    )
  } else if (term == "2023_2024") {
    joined <- inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # Machaela Cavanaugh (SD-006) -> sponsor "cavanaugh, m."
        "m. cavanaugh" = "cavanaugh, m.",
        # John Cavanaugh (SD-009) -> sponsor "cavanaugh, j."
        "j. cavanaugh" = "cavanaugh, j."
      )
    )
  } else {
    # Default: no custom matches
    joined <- inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full"
    )
  }

  joined
}

# ============================================================
# Stage 5.5: Finalize legis_data
# ============================================================
#' Override chamber to "U" for all NE legislators.
#' The pipeline's default derives chamber from district (giving "S"),
#' but NE unicameral needs "U" for LES calculation grouping.
#'
#' From old script line 620.
#'
#' @param legis_data Legislator data from reconciliation
#' @param term Term string
#' @return Modified legis_data with chamber = "U"
finalize_legis_data <- function(legis_data, term) {
  legis_data %>%
    mutate(chamber = "U")
}

# ============================================================
# Stage 7: Prepare bills for LES calculation
# ============================================================
#' All bills get chamber = "U" for unicameral.
#' From old script line 643.
#'
#' @param bills_prepared Bills dataframe
#' @return Bills with chamber = "U"
prepare_bills_for_les <- function(bills_prepared) {
  bills_prepared %>%
    mutate(chamber = "U")
}

# Export configuration and hooks
c(
  ne_config,
  list(
    get_bill_file_suffix = get_bill_file_suffix,
    preprocess_raw_data = preprocess_raw_data,
    clean_bill_details = clean_bill_details,
    clean_bill_history = clean_bill_history,
    post_evaluate_bill = post_evaluate_bill,
    transform_ss_bills = transform_ss_bills,
    enrich_ss_with_session = enrich_ss_with_session,
    get_missing_ss_bills = get_missing_ss_bills,
    derive_unique_sponsors = derive_unique_sponsors,
    clean_sponsor_names = clean_sponsor_names,
    adjust_legiscan_data = adjust_legiscan_data,
    reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
    finalize_legis_data = finalize_legis_data,
    prepare_bills_for_les = prepare_bills_for_les
  )
)
