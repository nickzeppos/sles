# Ohio State Configuration
# State-specific constants and settings for LES estimation
#
# OH is a carryover state - bill IDs persist across the term.
# Sessions: Regular sessions identified by session_num (e.g., 135 for 135th GA).
# Special sessions: session_year format like "2024_S".
# Sponsor format: single primary sponsor in "sponsors" column, no cosponsors used.
# Name matching uses full names (LES_sponsor + chamber).

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}

logging <- source(file.path(repo_root, "utils/logging.R"), local = TRUE)$value
strings <- source(file.path(repo_root, "utils/strings.R"), local = TRUE)$value

# Extract functions
cli_log <- logging$cli_log
cli_warn <- logging$cli_warn
standardize_accents <- strings$standardize_accents

# Load required libraries
library(dplyr)
library(stringr)
library(glue)

# nolint start: line_length_linter
oh_config <- list(
  # Valid bill types for this state
  bill_types = c("HB", "SB"),

  # Sponsor patterns to drop from analysis
  # From old script lines 287-297: committee-sponsored and initiative bills
  drop_sponsor_pattern = "committee|initiative",

  # Step terms for evaluating bill history (from old script lines 359-364)
  # These are for t > 2014 (modern format)
  step_terms = list(
    aic = c("^reported"),
    abc = c(
      "^reported", "read on the floor", "motion to reconsider",
      "recommitted", "re-referred"
    ),
    pass = c("^passed", "received from the house", "received from the senate"),
    law = c("^effective", "signed")
  )
)
# nolint end: line_length_linter

#' Check if a bill should be dropped based on sponsor
#'
#' @param sponsor Sponsor name
#' @return TRUE if bill should be dropped, FALSE otherwise
should_drop_bill <- function(sponsor) {
  grepl(oh_config$drop_sponsor_pattern, sponsor, ignore.case = TRUE)
}

# Stage 1 Hook: Load bill files
#' Load bill files for Ohio
#'
#' OH may have multiple files per term including special sessions.
#' Files are named: {STATE}_Bill_Details_{TERM}.csv and {STATE}_Bill_Details_{YEAR}_S.csv
#'
#' @param bill_dir Path to bill directory
#' @param state State code ("OH")
#' @param term Term in format "YYYY_YYYY"
#' @param verbose Show detailed logging (default TRUE)
#' @return List with bill_details and bill_history dataframes
load_bill_files <- function(bill_dir, state, term, verbose = TRUE) {
  # Parse term years
  years <- strsplit(term, "_")[[1]]
  year1 <- years[1]
  year2 <- years[2]

  # Find all files for this term (regular + special sessions)
  all_files <- list.files(bill_dir, full.names = TRUE)

  # Match the main term file and any special session files for these years
  # e.g., OH_Bill_Details_2023_2024.csv and OH_Bill_Details_2024_S.csv
  detail_files <- all_files[grepl("Bill_Details", all_files) &
    (grepl(paste0("_", term, "\\.csv"), all_files) |
      grepl(paste0("_", year1, "_S\\.csv"), all_files) |
      grepl(paste0("_", year2, "_S\\.csv"), all_files))]

  history_files <- all_files[grepl("Bill_Histories", all_files) &
    (grepl(paste0("_", term, "\\.csv"), all_files) |
      grepl(paste0("_", year1, "_S\\.csv"), all_files) |
      grepl(paste0("_", year2, "_S\\.csv"), all_files))]

  if (length(detail_files) == 0) {
    cli_warn(glue("No bill detail files found for term {term}"))
    return(NULL)
  }

  if (verbose) {
    cli_log(glue("Found {length(detail_files)} bill detail files for {term}"))
  }

  # Load and combine bill details
  bill_details <- bind_rows(lapply(detail_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read.csv(f, stringsAsFactors = FALSE)
  }))

  # Load and combine bill histories
  bill_history <- bind_rows(lapply(history_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read.csv(f, stringsAsFactors = FALSE)
  }))

  # Remove duplicates
  bill_details <- distinct(bill_details)
  bill_history <- distinct(bill_history)

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 1.5 Hook: Preprocess raw data
#' Preprocess raw data for Ohio
#'
#' Normalizes raw CSV data into standard schema.
#' OH uses bill_number -> bill_id, session_num -> session.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  # Rename bill_number to bill_id
  bill_details <- bill_details %>%
    rename(bill_id = "bill_number")

  bill_history <- bill_history %>%
    rename(bill_id = "bill_number")

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

#' Clean bill details for Ohio
#'
#' OH-specific transformations:
#' - Create session from session_num (handling special sessions)
#' - Standardize accents in sponsor names
#' - Extract LES_sponsor (single primary sponsor)
#' - Drop committee-sponsored and initiative bills
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @param verbose Show detailed logging (default TRUE)
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Store unfiltered version
  all_bill_details <- bill_details

  # Add term column
  bill_details$term <- term

  # Create session column from session_year and session_num
  # For regular sessions: session = session_num (e.g., 135)
  # For special sessions: session_year is like "2024_S", keep as-is
  bill_details <- bill_details %>%
    mutate(
      session = ifelse(
        grepl("_S$", .data$session_year),
        paste0(.data$session_num, "S"),
        as.character(.data$session_num)
      )
    )

  # Derive bill_type and filter
  bill_details <- bill_details %>%
    mutate(bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))) %>%
    filter(.data$bill_type %in% oh_config$bill_types) %>%
    select(-"bill_type")

  # Clean sponsors field
  bill_details$sponsors <- tolower(bill_details$sponsors)
  bill_details$sponsors <- standardize_accents(bill_details$sponsors)
  bill_details$sponsors <- gsub("  +", " ", bill_details$sponsors)

  # Standardize separators to semicolons (from old script lines 145-147)
  # ", " and " and " become "; "
  bill_details$sponsors <- gsub(", | and ", "; ", bill_details$sponsors)
  # " &" becomes ";"
  bill_details$sponsors <- gsub(" &", ";", bill_details$sponsors)
  bill_details$sponsors <- gsub("  +", " ", bill_details$sponsors)
  bill_details$sponsors <- str_trim(bill_details$sponsors)

  # LES_sponsor is the first (primary) sponsor only (from old script line 148)
  bill_details$LES_sponsor <- gsub(";.+", "", bill_details$sponsors)
  bill_details$LES_sponsor <- str_trim(bill_details$LES_sponsor)

  # Drop committee-sponsored bills
  comm_bills <- bill_details %>%
    filter(should_drop_bill(.data$LES_sponsor))

  if (nrow(comm_bills) > 0) {
    if (verbose) {
      cli_log(glue(
        "Dropping {nrow(comm_bills)} committee/initiative-sponsored bills"
      ))
    }
    bill_details <- bill_details %>%
      filter(!should_drop_bill(.data$LES_sponsor))
  }

  # Drop bills with missing/empty sponsor
  empty_sponsor_bills <- bill_details %>%
    filter(.data$LES_sponsor == "" | is.na(.data$LES_sponsor))

  if (nrow(empty_sponsor_bills) > 0) {
    if (verbose) {
      cli_log(glue("Dropping {nrow(empty_sponsor_bills)} bills without sponsor"))
    }
    bill_details <- bill_details %>%
      filter(!(.data$LES_sponsor == "" | is.na(.data$LES_sponsor)))
  }

  list(
    all_bill_details = all_bill_details,
    bill_details = bill_details
  )
}

#' Clean bill history for Ohio
#'
#' OH-specific transformations:
#' - Create session from session_num (handling special sessions)
#' - Chamber standardization
#' - Actions already have order column
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  bill_history <- bill_history %>%
    mutate(
      term = term,
      session = ifelse(
        grepl("_S$", .data$session_year),
        paste0(.data$session_num, "S"),
        as.character(.data$session_num)
      )
    )

  # Clean action text
  bill_history$action <- str_trim(gsub("  +", " ",
    gsub("\\&nbsp", " ", bill_history$action)))

  # Standardize chamber names (from old script line 339)
  bill_history$chamber <- recode(
    bill_history$chamber,
    "H" = "House", "S" = "Senate", "G" = "Governor",
    "CC" = "Conference"
  )

  # Handle empty chamber for governor actions (from old script line 335)
  bill_history <- bill_history %>%
    mutate(
      chamber = ifelse(
        .data$chamber == "" & grepl("^effective|^signed", tolower(.data$action)),
        "Governor",
        .data$chamber
      )
    )

  # Order by session, bill_id, order
  bill_history <- bill_history %>%
    arrange(.data$session, .data$bill_id, .data$order)

  # Lowercase action for pattern matching
  bill_history$action <- tolower(bill_history$action)

  bill_history
}

#' Transform SS bills for Ohio
#'
#' OH SS files have format "HB 68" which needs to be converted to "HB0068".
#' From old script lines 85-88.
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return Transformed SS bills
transform_ss_bills <- function(ss_bills, term) {
  ss_bills %>%
    mutate(
      # Remove spaces and pad bill number to 4 digits
      bill_id = gsub(" ", "", .data$bill_id),
      bill_id = paste0(
        gsub("[0-9].+|[0-9]+", "", .data$bill_id),
        str_pad(gsub("^[A-Z]+", "", .data$bill_id), 4, pad = "0")
      )
    )
}

#' Enrich SS bills with session via human-reviewed CSV
#'
#' OH bill IDs restart in special sessions (e.g., 135 vs 135S).
#' A review CSV resolves ambiguous bills; all others default to the
#' regular session.
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return SS bills with session column added
enrich_ss_with_session <- function(ss_bills, term) {
  # Parse term to derive session_num for regular session
  # OH uses session_num (e.g., 135 for 2023_2024)
  years <- strsplit(term, "_")[[1]]
  # OH session numbering: 135th GA = 2023_2024
  # Formula: (start_year - 1753) / 2
  rs_session <- as.character((as.integer(years[1]) - 1753) / 2)

  review_path <- file.path(
    repo_root, ".data", "OH", "review",
    "OH_SS_session_resolution.csv"
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

#' Derive unique sponsors from bills
#'
#' OH doesn't have cosponsors in the data (from old script line 469).
#'
#' @param bills Dataframe of bills with achievement columns
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregate stats
derive_unique_sponsors <- function(bills, term) {
  bills %>%
    mutate(chamber = ifelse(substring(.data$bill_id, 1, 1) == "H", "H", "S")) %>%
    select("LES_sponsor", "chamber", "passed_chamber", "law") %>%
    mutate(term = term) %>%
    group_by(.data$LES_sponsor, .data$chamber, .data$term) %>%
    summarize(
      num_sponsored_bills = n(),
      sponsor_pass_rate = sum(.data$passed_chamber) / n(),
      sponsor_law_rate = sum(.data$law) / n(),
      .groups = "drop"
    ) %>%
    # No cosponsors in OH data
    mutate(num_cosponsored_bills = NA_integer_)
}

#' Clean sponsor names for matching
#'
#' OH uses full name matching (LES_sponsor + chamber).
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param term Term string
#' @return Dataframe with match_name_chamber column added
clean_sponsor_names <- function(all_sponsors, term) {
  all_sponsors %>%
    arrange(.data$chamber, .data$LES_sponsor) %>%
    distinct() %>%
    mutate(
      match_name_chamber = tolower(paste(
        .data$LES_sponsor,
        substr(.data$chamber, 1, 1),
        sep = "-"
      ))
    )
}

#' Adjust legiscan data for matching
#'
#' OH uses name directly for matching.
#' Chamber derived from district (HD/SD prefix) or role (Sen/Rep).
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string
#' @return Adjusted legiscan dataframe with match_name_chamber column
adjust_legiscan_data <- function(legiscan, term) {
  legiscan %>%
    filter(.data$committee_id == 0) %>%
    mutate(
      # Chamber from district prefix (HD-xxx or SD-xxx)
      match_name_chamber = tolower(paste(
        .data$name,
        substr(.data$district, 1, 1),
        sep = "-"
      ))
    ) %>%
    distinct(.data$match_name_chamber, .keep_all = TRUE)
}

#' Reconcile legiscan with sponsors
#'
#' Uses fuzzy matching with full names.
#' Term-specific custom_match blocks handle name variants and prevent
#' incorrect fuzzy matches.
#'
#' @param sponsors Dataframe of unique sponsors
#' @param legiscan Adjusted legiscan dataframe
#' @param term Term string
#' @return Joined dataframe of sponsors matched to legiscan records
# nolint start: line_length_linter
reconcile_legiscan_with_sponsors <- function(sponsors, legiscan, term) {
  if (term == "2019_2020") {
    # Custom matches for 2019_2020 (from old script lines 553-563)
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "randall gardner-s" = NA_character_,
        "anthony devitis-h" = NA_character_,
        "sarah latourette-h" = NA_character_,
        "larry householder-h" = NA_character_,
        "william seitz-h" = NA_character_,
        "louis terhar-s" = NA_character_,
        "jay edwards-h" = NA_character_,
        "james butler-h" = NA_character_,
        "joseph miller-h" = "joseph a. miller iii-h"
      )
    )
  } else if (term == "2021_2022") {
    # Custom matches for 2021_2022 (from old script lines 577-591)
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "larry householder-h" = NA_character_,
        "paul zeltwanger-h" = NA_character_,
        "bishara addison-h" = NA_character_,
        "nino vitale-h" = NA_character_,
        "sedrick denson-h" = NA_character_,
        "michael o'brien-h" = NA_character_,
        "joseph miller-h" = "joseph a. miller iii-h",
        "elgin rogers-h" = NA_character_,
        "shawn stevens-h" = NA_character_,
        "dale martin-s" = NA_character_,
        "matt huffman-s" = NA_character_,
        "mark johnson-h" = NA_character_,
        "paula hicks-hudson-s" = NA_character_
      )
    )
  } else if (term == "2023_2024") {
    # Custom matches for 2023_2024
    # Block incorrect fuzzy matches (legiscan key -> NA to prevent match)
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # "jason stephens" fuzzy matches "adam mathews" - block
        "jason stephens-h" = NA_character_,
        # "kris jordan" and "brian baldridge" fuzzy match "brian lorenz" - block
        "kris jordan-h" = NA_character_,
        "brian baldridge-h" = NA_character_,
        # "sedrick denson" fuzzy matches "derek merrin" - block
        "sedrick denson-h" = NA_character_,
        # "allison russo" fuzzy matches "jon cross" - block
        "allison russo-h" = NA_character_,
        # "matt huffman" fuzzy matches "matt dolan" - block
        "matt huffman-s" = NA_character_,
        # "scott oelslager" fuzzy matches "scott wiggam" - block
        "scott oelslager-h" = NA_character_,
        # cutrona switched from H to S in special session; block S record
        # from fuzzy-matching to house sponsor "al cutrona-h"
        "alessandro cutrona-s" = NA_character_
      )
    )
  } else {
    # Default for other terms
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full"
    )
  }
}
# nolint end: line_length_linter

#' Prepare bills for LES calculation
#'
#' Derives chamber from bill_id prefix (H/S).
#'
#' @param bills_prepared Bills dataframe
#' @return Bills with chamber column added
prepare_bills_for_les <- function(bills_prepared) {
  bills_prepared %>%
    mutate(chamber = ifelse(substring(.data$bill_id, 1, 1) == "H", "H", "S"))
}

# Export config and functions
list(
  bill_types = oh_config$bill_types,
  step_terms = oh_config$step_terms,
  load_bill_files = load_bill_files,
  preprocess_raw_data = preprocess_raw_data,
  clean_bill_details = clean_bill_details,
  clean_bill_history = clean_bill_history,
  transform_ss_bills = transform_ss_bills,
  enrich_ss_with_session = enrich_ss_with_session,
  derive_unique_sponsors = derive_unique_sponsors,
  clean_sponsor_names = clean_sponsor_names,
  adjust_legiscan_data = adjust_legiscan_data,
  reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
  prepare_bills_for_les = prepare_bills_for_les
)
