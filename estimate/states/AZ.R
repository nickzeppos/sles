# Arizona State Configuration
# Bill types, terms, and state-specific cleaning functions

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
logging <- source(file.path(repo_root, "utils/logging.R"),
  local = TRUE
)$value
libs <- source(file.path(repo_root, "utils/libs.R"), local = TRUE)$value
strings <- source(file.path(repo_root, "utils/strings.R"), local = TRUE)$value

# Extract functions
cli_log <- logging$cli_log
cli_warn <- logging$cli_warn
require_libs <- libs$require_libs
standardize_accents <- strings$standardize_accents

# Load required libraries
require_libs()
library(dplyr)
library(stringr)
library(glue)
library(readr)

# nolint start: line_length_linter
# Arizona Configuration
az_config <- list(
  # Valid bill types for AZ (House Bills and Senate Bills)
  bill_types = c("HB", "SB"),

  # Sponsor patterns to drop from analysis (committees, agencies, etc.)
  # These don't represent individual legislator effectiveness
  # From old script: filter(bills, !grepl('committee|^nrae$|maj nrae|^gov', intro_sponsor))
  drop_sponsor_pattern = "committee|^nrae$|maj nrae|^gov",

  # Step terms for evaluating bill history
  # Used to determine achievement levels: aic, abc, pass, law
  # From old script lines 395-402
  step_terms = list(
    aic = c(
      "^reported.+from.+committee",
      "committee report--"
    ),
    abc = c(
      "^reported.+from.+committee",
      "third reading",
      "committee of the whole",
      "floor motion"
    ),
    pass = c(
      "transmitted to house",
      "transmitted to senate",
      "final reading",
      "transmitted to gov",
      "conference comm"
    ),
    law = c(
      "^law",
      "chapter number"
    )
  ),

  # Session recoding map (from old script lines 128-130)
  # Maps raw session names to standard codes
  session_recode = list(
    "First_Regular" = "RS1",
    "Second_Regular" = "RS2",
    "First_Special" = "SS1",
    "Second_Special" = "SS2",
    "Third_Special" = "SS3",
    "Fourth_Special" = "SS4",
    "Fifth_Special" = "SS5",
    "Sixth_Special" = "SS6",
    "Seventh_Special" = "SS7",
    "Eighth_Special" = "SS8",
    "Ninth_Special" = "SS9",
    "Tenth_Special" = "SS10"
  )
)
# nolint end: line_length_linter

#' Check if a bill should be dropped based on sponsor
#'
#' Used to filter out bills that aren't sponsored by individual legislators
#' (e.g., committee-sponsored bills, NRAE, governor).
#'
#' @param sponsor Sponsor name
#' @return TRUE if bill should be dropped, FALSE otherwise
should_drop_bill <- function(sponsor) {
  grepl(az_config$drop_sponsor_pattern, sponsor, ignore.case = TRUE)
}

# Stage 1.5 Hook: Preprocess raw data to normalize column names

#' Preprocess raw data for Arizona
#'
#' Normalizes raw CSV data into standard schema expected by generic pipeline.
#' Handles AZ-specific column naming conventions.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string (unused but provided for consistency)
#' @return List with preprocessed bill_details and bill_history
preprocess_raw_data <- function(bill_details, bill_history, term) {
  # AZ uses "bill_number" in raw data, rename to "bill_id"
  bill_details <- bill_details %>%
    rename(bill_id = "bill_number") %>%
    mutate(bill_id = toupper(.data$bill_id))

  bill_history <- bill_history %>%
    rename(bill_id = "bill_number") %>%
    mutate(bill_id = toupper(.data$bill_id))

  # Recode session names to standard codes
  # Session format in data is "YYYY_YYYY_First_Regular", extract suffix
  if ("session" %in% names(bill_details)) {
    bill_details <- bill_details %>%
      mutate(session = case_when(
        grepl("First_Regular$", .data$session) ~ "RS1",
        grepl("Second_Regular$", .data$session) ~ "RS2",
        grepl("First_Special$", .data$session) ~ "SS1",
        grepl("Second_Special$", .data$session) ~ "SS2",
        grepl("Third_Special$", .data$session) ~ "SS3",
        grepl("Fourth_Special$", .data$session) ~ "SS4",
        grepl("Fifth_Special$", .data$session) ~ "SS5",
        TRUE ~ .data$session
      ))
  }

  if ("session" %in% names(bill_history)) {
    bill_history <- bill_history %>%
      mutate(session = case_when(
        grepl("First_Regular$", .data$session) ~ "RS1",
        grepl("Second_Regular$", .data$session) ~ "RS2",
        grepl("First_Special$", .data$session) ~ "SS1",
        grepl("Second_Special$", .data$session) ~ "SS2",
        grepl("Third_Special$", .data$session) ~ "SS3",
        grepl("Fourth_Special$", .data$session) ~ "SS4",
        grepl("Fifth_Special$", .data$session) ~ "SS5",
        TRUE ~ .data$session
      ))
  }

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 1 Hook: Load bill files (AZ has multiple session files per term)

#' Load bill files for Arizona
#'
#' AZ has separate files for each session within a term (First_Regular,
#' Second_Regular, etc.). This hook loads and combines them.
#'
#' @param bill_dir Path to bill directory
#' @param state State code ("AZ")
#' @param term Term in format "YYYY_YYYY"
#' @param verbose Show detailed logging (default TRUE)
#' @return List with bill_details and bill_history dataframes
load_bill_files <- function(bill_dir, state, term, verbose = TRUE) {
  # Find all bill detail files for this term
  detail_pattern <- glue("{state}_Bill_Details_{term}_")
  detail_files <- list.files(
    bill_dir, pattern = detail_pattern, full.names = TRUE
  )

  if (length(detail_files) == 0) {
    cli_warn(glue(
      "No bill detail files found matching pattern: {detail_pattern}"
    ))
    return(NULL)
  }

  if (verbose) cli_log(glue("Found {length(detail_files)} bill detail files for {term}"))

  # Load and combine bill details
  bill_details <- bind_rows(lapply(detail_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read_csv(f, show_col_types = FALSE)
  }))

  # Find and load bill history files
  history_pattern <- glue("{state}_Bill_Histories_{term}_")
  history_files <- list.files(
    bill_dir, pattern = history_pattern, full.names = TRUE
  )

  if (verbose) cli_log(glue("Found {length(history_files)} bill history files for {term}"))

  bill_history <- bind_rows(lapply(history_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read_csv(f, show_col_types = FALSE)
  }))

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 2 Hook: Clean bill details

#' Clean bill details for Arizona
#'
#' AZ-specific transformations for bill details:
#' - Create LES_sponsor from primary_sponsors column
#' - Filter to valid bill types (HB, SB)
#' - Drop committee/NRAE/governor sponsored bills
#' - Normalize sponsor names (lowercase, accent removal)
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @param verbose Show detailed logging (default TRUE)
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Store unfiltered version
  all_bill_details <- bill_details

  # Create LES_sponsor from primary_sponsors (lowercase, standardize accents)
  # AZ uses primary_sponsors, not intro_sponsor
  bill_details$LES_sponsor <- tolower(bill_details$primary_sponsors)
  bill_details$LES_sponsor <- standardize_accents(bill_details$LES_sponsor)

  # Add term and derive bill_type from bill_id
  bill_details <- bill_details %>%
    mutate(
      term = term,
      bill_type = toupper(gsub("[0-9].*", "", .data$bill_id))
    )

  # Filter to valid bill types
  keep_types <- az_config$bill_types
  bill_details <- bill_details %>%
    filter(.data$bill_type %in% keep_types)

  if (verbose) cli_log(glue("After bill type filter: {nrow(bill_details)} bills"))

  # Drop committee/NRAE/governor sponsored bills
  drop_bills <- bill_details %>%
    filter(should_drop_bill(.data$LES_sponsor))

  if (nrow(drop_bills) > 0) {
    if (verbose) cli_log(glue(
      "Dropping {nrow(drop_bills)} committee/NRAE/gov sponsored bills"
    ))
    bill_details <- bill_details %>%
      filter(!should_drop_bill(.data$LES_sponsor))
  }

  # Drop bills with missing/empty sponsor
  empty_sponsor_bills <- bill_details %>%
    filter(.data$LES_sponsor == "" | is.na(.data$LES_sponsor))

  if (nrow(empty_sponsor_bills) > 0) {
    if (verbose) cli_log(glue("Dropping {nrow(empty_sponsor_bills)} bills without sponsor"))
    bill_details <- bill_details %>%
      filter(!(.data$LES_sponsor == "" | is.na(.data$LES_sponsor)))
  }

  list(
    all_bill_details = all_bill_details,
    bill_details = bill_details
  )
}

# Stage 2 Hook: Clean bill history

#' Clean bill history for Arizona
#'
#' AZ-specific transformations for bill history:
#' - Lowercase action text for pattern matching
#' - Add term column
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  # From old script lines 353-358: handle dual date formats and derive order
  bill_history %>%
    group_by(.data$session, .data$bill_id) %>%
    mutate(
      term = term,
      # Lowercase action for pattern matching
      action = tolower(.data$action),
      # Normalize date format: handle both m/d/Y and Y-m-d formats
      action_date = ifelse(
        grepl("/", .data$action_date),
        format(as.Date(.data$action_date, format = "%m/%d/%Y"), "%Y-%m-%d"),
        .data$action_date
      )
    ) %>%
    arrange(.data$session, .data$bill_id, .data$action_date) %>%
    mutate(order = seq_len(n())) %>%
    ungroup()
}

# Stage 3 Hook: Enrich SS bills with session info

#' Add session info to SS bills for Arizona
#'
#' AZ is NOT a carryover state - bill numbers restart each session.
#' This means HB2394 in RS1 is a different bill from HB2394 in RS2.
#' We need to map year → session so SS bills can join correctly:
#' - 2023 → RS1 (First Regular)
#' - 2024 → RS2 (Second Regular)
#'
#' @param ss_bills SS bills dataframe (must have year column)
#' @param term Term string (e.g., "2023_2024")
#' @return SS bills with session column added
enrich_ss_with_session <- function(ss_bills, term) {
  # Parse term years
  years <- strsplit(term, "_")[[1]]
  first_year <- as.integer(years[1])
  second_year <- as.integer(years[2])

  # Map year to session
  ss_bills %>%
    mutate(session = case_when(
      .data$year == first_year ~ "RS1",
      .data$year == second_year ~ "RS2",
      TRUE ~ NA_character_
    ))
}

# Stage 5 Hook: Derive unique sponsors

#' Derive unique sponsors for Arizona
#'
#' Groups bills by sponsor and chamber to get aggregate sponsor statistics.
#' Arizona uses "H" prefix for House bills (HB) and "S" for Senate (SB).
#'
#' @param bills Bills dataframe with LES_sponsor column
#' @param term Term string (e.g., "2023_2024")
#' @return Dataframe of unique sponsors with aggregate stats
derive_unique_sponsors <- function(bills, term) {
  bills %>%
    mutate(chamber = ifelse(substring(.data$bill_id, 1, 1) == "H",
      "H", "S"
    )) %>%
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

# Stage 7 Hook: Prepare bills for LES calculation

#' Prepare bills for LES calculation for Arizona
#'
#' Derives chamber from bill_id prefix. Arizona uses "H" for House bills
#' (HB prefix) and "S" for Senate bills (SB prefix).
#'
#' @param bills_prepared Bills dataframe with sponsor column
#' @return Bills dataframe with chamber column added
prepare_bills_for_les <- function(bills_prepared) {
  bills_prepared %>%
    mutate(chamber = ifelse(substring(.data$bill_id, 1, 1) == "H",
      "H", "S"
    ))
}

# Stage 5 Hook: Clean sponsor names

#' Clean sponsor names for Arizona
#'
#' Creates match keys for fuzzy joining sponsors to legiscan data.
#' Based on old script lines 517-539.
#'
#' @param all_sponsors Sponsors dataframe
#' @param term Term string
#' @return Modified all_sponsors with match_name_chamber column
clean_sponsor_names <- function(all_sponsors, term) {
  # Extract last_name and first_name from LES_sponsor
  # Pattern: "lastname" or "lastname x" or "lastname xx" (initial suffix)
  all_sponsors <- all_sponsors %>%
    mutate(
      last_name = ifelse(
        !grepl(" [a-z]$| [a-z][a-z]$", .data$LES_sponsor),
        .data$LES_sponsor,
        gsub(" [a-z]$| [a-z][a-z]$", "", .data$LES_sponsor)
      ),
      first_name = ifelse(
        grepl(" [a-z]$| [a-z][a-z]$", .data$LES_sponsor),
        str_trim(str_extract(.data$LES_sponsor, " [a-z]$| [a-z][a-z]$")),
        ""
      )
    )

  # Term-specific name fixes (from old script lines 522-530)
  # Note: These are for historical terms, 2023_2024 may need its own fixes
  if (term >= "2015_2016" && term <= "2022_2023") {
    all_sponsors$last_name <- ifelse(
      all_sponsors$LES_sponsor == "ugenti-rita",
      "ugenti",
      all_sponsors$last_name
    )
  }

  # Rebuild last_name/first_name (old script line 533-539)
  all_sponsors <- all_sponsors %>%
    mutate(
      last_name = gsub(" .+", "", .data$LES_sponsor),
      first_name = ifelse(
        !grepl(" ", .data$LES_sponsor),
        NA,
        gsub(".+ ", "", .data$LES_sponsor)
      )
    ) %>%
    arrange(.data$chamber, .data$LES_sponsor) %>%
    distinct() %>%
    mutate(
      match_name_chamber = tolower(paste(
        .data$LES_sponsor,
        substr(.data$chamber, 1, 1),
        sep = "-"
      ))
    ) %>%
    select(-c("last_name", "first_name"))

  all_sponsors
}

# Stage 5 Hook: Adjust legiscan data

#' Adjust legiscan data for Arizona
#'
#' Cleans legiscan data and creates match keys for fuzzy joining.
#' Based on old script lines 561-574.
#'
#' @param legiscan Legiscan dataframe
#' @param term Term string
#' @return Modified legiscan with match_name_chamber column
adjust_legiscan_data <- function(legiscan, term) {
  # Term-specific adjustments (from old script lines 550-557)
  if (term == "2019_2020") {
    legiscan <- legiscan %>%
      mutate(
        role = ifelse(.data$people_id == 4273, "Sen", .data$role),
        district = case_when(
          .data$people_id == 10701 ~ "SD-015",
          .data$people_id == 4235 ~ "SD-008",
          .data$people_id == 14707 ~ "HD-012",
          .data$people_id == 4273 ~ "HD-015",
          TRUE ~ .data$district
        )
      )
  }

  # Build match keys (old script lines 561-574)
  # Handle duplicate last names by adding first/middle initials
  # Note: When middle_name is NA, only use first initial
  legiscan_adj <- legiscan %>%
    filter(.data$committee_id == 0) %>%
    mutate(
      # Clean middle_name: treat NA as empty string
      middle_init = ifelse(is.na(.data$middle_name), "",
                           substr(.data$middle_name, 1, 1))
    ) %>%
    group_by(.data$last_name) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      match_name = case_when(
        # If > 2 people share last name, use first + middle initial
        n > 2 ~ paste0(
          .data$last_name, " ",
          substr(.data$first_name, 1, 1),
          .data$middle_init
        ),
        # If exactly 2 people share last name, use first initial only
        n == 2 ~ paste(.data$last_name, substr(.data$first_name, 1, 1)),
        # Unique last name, use as-is
        TRUE ~ .data$last_name
      )
    ) %>%
    # Check if match_name still has duplicates
    group_by(.data$match_name) %>%
    mutate(n2 = n()) %>%
    mutate(
      # If still duplicates, add middle initial (may still collide if both NA)
      match_name = ifelse(
        n2 > 1,
        paste0(
          .data$last_name, " ",
          substr(.data$first_name, 1, 1),
          .data$middle_init
        ),
        .data$match_name
      )
    ) %>%
    ungroup() %>%
    mutate(
      match_name_chamber = tolower(paste(
        .data$match_name,
        substr(.data$district, 1, 1),
        sep = "-"
      ))
    ) %>%
    select(-"middle_init", -"n", -"n2")

  legiscan_adj
}

# Stage 5 Hook: Reconcile legiscan with sponsors

#' Reconcile legiscan with sponsors for Arizona
#'
#' Performs fuzzy matching between legiscan and sponsor data.
#' Based on old script lines 583-613.
#'
#' Note: Pipeline calls this as (sponsors, legiscan, term) but we swap
#' order for inexact_join since old script uses legiscan as x, sponsors as y.
#'
#' @param sponsors Sponsors dataframe (with match_name_chamber)
#' @param legiscan Legiscan dataframe (with match_name_chamber)
#' @param term Term string
#' @return Joined dataframe
reconcile_legiscan_with_sponsors <- function(sponsors, legiscan, term) {
  # Term-specific custom matches (from old script)
  # Note: custom_match maps legiscan (x) names to sponsor (y) names
  if (term == "2019_2020") {
    all_sponsors_2 <- inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "meza-h" = NA_character_
      )
    )
  } else if (term == "2021_2022") {
    all_sponsors_2 <- inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "fernandez c-h" = "fernandez-h"
      )
    )
  } else if (term == "2023_2024") {
    # Custom matches for 2023_2024:
    # - Anna Hernandez (Senate) listed as "HERNANDEZ" without initial
    # - Flavio Bravo served in both chambers, listed as "BRAVO" without initial
    # - Lupe Diaz (House) listed as "DIAZ" without initial
    # - New legislators (2024 session) with no sponsored bills - map to NA
    #   to prevent fuzzy matching to other sponsors
    all_sponsors_2 <- inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # Sponsor name fixes (sponsor listed without initial)
        "hernandez a-s" = "hernandez-s",
        "bravo f-s" = "bravo-s",
        "bravo f-h" = "bravo-h",
        "diaz l-h" = "diaz-h",
        "diaz e-s" = "diaz-s",
        # Legislators who served but sponsored zero bills - prevent fuzzy
        # matching to other sponsors. They appear with LES=0 via full join.
        "lucking-h" = NA_character_,
        "liguori-h" = NA_character_,
        "luna-najera-h" = NA_character_,
        "nardozzi-h" = NA_character_,
        "cavero-h" = NA_character_
      )
    )
  } else {
    # Default: no custom matches
    all_sponsors_2 <- inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full"
    )
  }

  all_sponsors_2
}

# TODO: Additional hooks that may be needed:
# - get_missing_ss_bills(ss_bills, bill_details, term)

# Export config and functions
# Flatten config to top level for loader validation
list(
  bill_types = az_config$bill_types,
  step_terms = az_config$step_terms,
  drop_sponsor_pattern = az_config$drop_sponsor_pattern,
  session_recode = az_config$session_recode,
  load_bill_files = load_bill_files,
  preprocess_raw_data = preprocess_raw_data,
  should_drop_bill = should_drop_bill,
  clean_bill_details = clean_bill_details,
  clean_bill_history = clean_bill_history,
  enrich_ss_with_session = enrich_ss_with_session,
  derive_unique_sponsors = derive_unique_sponsors,
  prepare_bills_for_les = prepare_bills_for_les,
  clean_sponsor_names = clean_sponsor_names,
  adjust_legiscan_data = adjust_legiscan_data,
  reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors
)
