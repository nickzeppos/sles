# New Mexico State Configuration
# State-specific constants and settings for LES estimation
#
# NM is a non-carryover state - bill IDs restart each session.
# Sessions: RS (Regular), SS1/SS2/etc (Special Sessions)
# Session types in raw data use S1/S2/etc, need recoding to SS1/SS2/etc.
# Sponsor format: single sponsor, lowercase, no cosponsors in data.
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
nm_config <- list(
  # Valid bill types for this state
  bill_types = c("HB", "SB"),

  # Sponsor patterns to drop from analysis
  drop_sponsor_pattern = "committee",

  # Step terms for evaluating bill history (from old script lines 389-397)
  # NM: Actions are sporadically out of order - no dates provided
  step_terms = list(
    aic = c(
      "reported by", "do pass", "do not pass", "committee substitution",
      "without recommendation"
    ),
    abc = c(
      "reported by committee with", "placed.+calendar", "floor amendment",
      "^failed to pass", "by motion", "motion to", "withdrawn from comm.+placed on"
    ),
    pass = c("^passed", "referrals: cc", "failed to concur", "has concurred"),
    law = c("signed by gov", "chapter [0-9]+")
  )
)
# nolint end: line_length_linter

#' Check if a bill should be dropped based on sponsor
#'
#' @param sponsor Sponsor name
#' @return TRUE if bill should be dropped, FALSE otherwise
should_drop_bill <- function(sponsor) {
  grepl(nm_config$drop_sponsor_pattern, sponsor, ignore.case = TRUE)
}

# Stage 1 Hook: Load bill files
#' Load bill files for New Mexico
#'
#' NM has multiple sessions per term with naming pattern:
#' {STATE}_Bill_Details_{YEAR}_{SESSION_TYPE}.csv
#'
#' @param bill_dir Path to bill directory
#' @param state State code ("NM")
#' @param term Term in format "YYYY_YYYY"
#' @param verbose Show detailed logging (default TRUE)
#' @return List with bill_details and bill_history dataframes
load_bill_files <- function(bill_dir, state, term, verbose = TRUE) {
  # Parse term years
  years <- strsplit(term, "_")[[1]]
  year1 <- years[1]
  year2 <- years[2]

  # Find all files for this term
  all_files <- list.files(bill_dir, full.names = TRUE)

  # Pattern matches files starting with year1 or year2
  detail_files <- all_files[grepl("Bill_Details", all_files) &
    (grepl(paste0("_", year1, "_"), all_files) |
      grepl(paste0("_", year2, "_"), all_files))]

  history_files <- all_files[grepl("Bill_Histories", all_files) &
    (grepl(paste0("_", year1, "_"), all_files) |
      grepl(paste0("_", year2, "_"), all_files))]

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
#' Preprocess raw data for New Mexico
#'
#' Normalizes raw CSV data into standard schema.
#' NM uses bill_number -> bill_id, session_type needs recoding.
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

#' Clean bill details for New Mexico
#'
#' NM-specific transformations:
#' - Recode session_type (S1->SS1, etc.)
#' - Create session column from session_year + session_type
#' - Standardize accents in sponsor names
#' - Extract LES_sponsor (no secondary sponsors)
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

  # Recode session_type (S1->SS1, S2->SS2, etc.) and create session
  bill_details <- bill_details %>%
    mutate(
      session_type = recode(.data$session_type,
        "S1" = "SS1", "S2" = "SS2", "S3" = "SS3",
        "S4" = "SS4", "S5" = "SS5", "S6" = "SS6"
      ),
      session = paste(.data$session_year, .data$session_type, sep = "-"),
      # Handle comm_reports NAs for later coding check
      comm_reports = ifelse(is.na(.data$comm_reports), "", .data$comm_reports)
    )

  # Derive bill_type and filter
  bill_details <- bill_details %>%
    mutate(bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))) %>%
    filter(.data$bill_type %in% nm_config$bill_types) %>%
    select(-"bill_type")

  # Clean sponsors field
  bill_details$sponsors <- tolower(bill_details$sponsors)
  bill_details$sponsors <- standardize_accents(bill_details$sponsors)
  bill_details$sponsors <- gsub("  +", " ", bill_details$sponsors)

  # Remove nicknames in quotes (with or without leading space)
  bill_details$sponsors <- gsub(' ?"[^"]+" ?', " ", bill_details$sponsors)
  bill_details$sponsors <- gsub("  +", " ", bill_details$sponsors)
  bill_details$sponsors <- str_trim(bill_details$sponsors)

  # LES_sponsor is first sponsor (before semicolon if any)
  bill_details$LES_sponsor <- gsub(";.+", "", bill_details$sponsors)
  # Also strip trailing semicolons
  bill_details$LES_sponsor <- gsub(";$", "", bill_details$LES_sponsor)
  bill_details$LES_sponsor <- str_trim(bill_details$LES_sponsor)

  # Drop committee-sponsored bills
  comm_bills <- bill_details %>%
    filter(should_drop_bill(.data$LES_sponsor))

  if (nrow(comm_bills) > 0) {
    if (verbose) {
      cli_log(glue("Dropping {nrow(comm_bills)} committee-sponsored bills"))
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

#' Clean bill history for New Mexico
#'
#' NM-specific transformations:
#' - Recode session_type and create session
#' - Derive chamber from action text
#' - Re-order actions using legislative_day where available
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  bill_history <- bill_history %>%
    mutate(
      term = term,
      session_type = recode(.data$session_type,
        "S1" = "SS1", "S2" = "SS2", "S3" = "SS3",
        "S4" = "SS4", "S5" = "SS5", "S6" = "SS6"
      ),
      session = paste(.data$session_year, .data$session_type, sep = "-")
    )

  # Re-order using legislative days (NM actions are out of order)
  bill_history <- bill_history %>%
    group_by(.data$session, .data$bill_id) %>%
    filter(!grepl("legislative day: [0-9]+", tolower(.data$action))) %>%
    mutate(
      legislative_day = ifelse(
        .data$order == 1 & .data$order == max(.data$order) &
          (is.na(.data$legislative_day) | .data$legislative_day == ""),
        "LD: 1",
        .data$legislative_day
      ),
      legislative_day_num = as.numeric(gsub("LD: ", "", .data$legislative_day)),
      legislative_day_i = ifelse(
        is.na(.data$legislative_day_num),
        max(.data$legislative_day_num, na.rm = TRUE) + 1,
        .data$legislative_day_num
      )
    ) %>%
    arrange(.data$session, .data$bill_id, .data$legislative_day_i, .data$order) %>%
    mutate(order = seq_len(n())) %>%
    ungroup() %>%
    select(-"legislative_day_i", -"legislative_day_num")

  # Clean action text
  bill_history$action <- str_trim(gsub("  +", " ",
    gsub("\\&nbsp", " ", bill_history$action)))

  # Derive chamber from action text
  bill_history$chamber <- "U"
  bill_history$chamber <- ifelse(
    bill_history$order == 1,
    substring(bill_history$bill_id, 1, 1),
    bill_history$chamber
  )
  bill_history$chamber <- ifelse(
    grepl("^house", tolower(bill_history$action)),
    "H", bill_history$chamber
  )
  bill_history$chamber <- ifelse(
    grepl("^senate", tolower(bill_history$action)),
    "S", bill_history$chamber
  )
  bill_history$chamber <- ifelse(
    grepl("in the house", tolower(bill_history$action)),
    "H", bill_history$chamber
  )
  bill_history$chamber <- ifelse(
    grepl("in the senate", tolower(bill_history$action)),
    "S", bill_history$chamber
  )
  bill_history$chamber <- ifelse(
    bill_history$chamber == "U" & grepl("^Sent to", bill_history$action),
    substring(gsub("^Sent.+Referrals: ", "", bill_history$action), 1, 1),
    bill_history$chamber
  )
  bill_history$chamber <- ifelse(
    bill_history$chamber == "U" & grepl("^H[A-Z][A-Z]+", bill_history$action),
    "H", bill_history$chamber
  )
  bill_history$chamber <- ifelse(
    bill_history$chamber == "U" & grepl("^S[A-Z][A-Z]+", bill_history$action),
    "S", bill_history$chamber
  )
  bill_history$chamber <- ifelse(
    grepl("Gov", bill_history$action),
    "G", bill_history$chamber
  )

  # Handle passed chamber inference
  bill_history <- bill_history %>%
    group_by(.data$session, .data$bill_id) %>%
    mutate(
      action_lower = tolower(.data$action),
      passed_row = ifelse(
        any(grepl("^passed", .data$action_lower)),
        ifelse(
          substring(.data$bill_id, 1, 1) == "H",
          min(grep("^passed in the house", .data$action_lower)),
          min(grep("^passed in the senate", .data$action_lower))
        ),
        0
      ),
      chamber = ifelse(
        .data$chamber == "U" & .data$order <= .data$passed_row,
        substring(.data$bill_id, 1, 1),
        .data$chamber
      ),
      chamber = ifelse(
        .data$chamber == "U" & .data$passed_row == 0,
        substring(.data$bill_id, 1, 1),
        .data$chamber
      )
    ) %>%
    select(-"passed_row", -"action_lower") %>%
    ungroup()

  # Recode chamber names
  bill_history$chamber <- recode(
    bill_history$chamber,
    "H" = "House", "S" = "Senate", "G" = "Governor",
    "C" = "Conference", "U" = "Unknown"
  )

  # Lowercase action for pattern matching
  bill_history$action <- tolower(bill_history$action)

  bill_history
}

#' Transform SS bills for New Mexico
#'
#' NM is non-carryover - bill numbers restart each session.
#' PVS data may have encoded session prefixes (e.g., HB20002 -> HB0002).
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return Transformed SS bills
transform_ss_bills <- function(ss_bills, term) {
  # Strip session encoding from bill IDs (e.g., HB20002 -> HB0002)
  ss_bills %>%
    mutate(
      bill_id = gsub("^([HS]B)(\\d)(\\d{4})$", "\\1\\3", .data$bill_id)
    )
}

#' Enrich SS bills with session information
#'
#' NM is non-carryover - need year to disambiguate session.
#' Maps year to session (assumes regular session for that year).
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return SS bills with session column added
enrich_ss_with_session <- function(ss_bills, term) {
  # Map year to regular session (most SS bills are from RS)
  ss_bills %>%
    mutate(session = paste(.data$year, "RS", sep = "-"))
}

#' Derive unique sponsors from bills
#'
#' NM doesn't have cosponsors in the data.
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
    )
}

#' Clean sponsor names for matching
#'
#' NM uses full name matching (LES_sponsor + chamber).
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
#' NM uses name with middle initial if middle_name exists.
#' Chamber derived from role (Sen -> s, Rep -> h).
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string
#' @return Adjusted legiscan dataframe with match_name_chamber column
adjust_legiscan_data <- function(legiscan, term) {
  legiscan %>%
    filter(.data$committee_id == 0) %>%
    group_by(.data$last_name) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      # Use middle initial if middle_name exists
      match_name = ifelse(
        .data$middle_name == "" | is.na(.data$middle_name),
        .data$name,
        paste0(.data$first_name, " ", substr(.data$middle_name, 1, 1),
               ". ", .data$last_name)
      ),
      # Chamber from role (Sen/Rep)
      match_name_chamber = tolower(paste(
        .data$match_name,
        ifelse(substr(.data$role, 1, 1) == "S", "s", "h"),
        sep = "-"
      ))
    ) %>%
    group_by(.data$match_name) %>%
    mutate(n = n()) %>%
    ungroup() %>%
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
  if (term == "2023_2024") {
    # Custom matches for 2023_2024
    # Keys from adjust_legiscan_data include middle initials
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # Name variants (legiscan -> sponsor)
        "andres romero-h" = "g. andres romero-h",
        # NA_character_ blocks to prevent incorrect fuzzy matches
        "willie d. madrid-h" = NA_character_,
        "william hall-h" = NA_character_
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

#' Get missing SS bills for New Mexico
#'
#' Called before session enrichment, so join on bill_id + term only.
#' Session is added later by enrich_ss_with_session.
#'
#' @param ss_filtered Filtered SS bills for this term
#' @param bill_details Cleaned bill details (after filtering)
#' @param all_bill_details All bill details (before filtering)
#' @return Dataframe of genuinely missing SS bills
get_missing_ss_bills <- function(ss_filtered, bill_details, all_bill_details) {
  # At this point ss_filtered doesn't have session yet
  # Check if bill_id + term exists in bill_details
  ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term")) %>%
    left_join(
      all_bill_details %>% select("bill_id", "sponsors"),
      by = "bill_id"
    ) %>%
    filter(!is.na(.data$sponsors)) %>%
    filter(!should_drop_bill(.data$sponsors)) %>%
    select("bill_id", "term")
}

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
  bill_types = nm_config$bill_types,
  step_terms = nm_config$step_terms,
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
  get_missing_ss_bills = get_missing_ss_bills,
  prepare_bills_for_les = prepare_bills_for_les
)
