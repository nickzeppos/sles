# Kentucky (KY) State Configuration
#
# KY has regular sessions (RS) within each term.
# Bill numbers DO restart between years (non-carryover state for specials).
# Multiple session files per term need to be combined (2023_RS + 2024_RS).
#
# Sponsor logic: First sponsor from semicolon-delimited sponsors field

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
logging <- source(file.path(repo_root, "utils/logging.R"), local = TRUE)$value
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
library(glue)
library(stringr)
library(readr)

# State configuration
ky_config <- list(
  bill_types = c("HB", "SB"),
  # Step terms for evaluating bill history (from old script lines 398-406)
  step_terms = list(
    aic = c(
      "^reported", "committee substitute", "committee amend",
      "posted in committee"
    ),
    abc = c(
      "^reported", "to calendar", "to consent calendar",
      "1st reading", "2nd reading", "3rd reading",
      "posted for passage", "floor amendment", "^defeated",
      "^passed", "recommitted to"
    ),
    pass = c(
      "^passed", "^3rd reading, passed"
    ),
    law = c(
      "^signed by gov", "\\(acts ch\\. [0-9]+\\)",
      "secretary of state.+ ch\\. [0-9]+", "became law without gov"
    )
  )
)

# Stage 1 Hook: Load bill files
#' Load bill files for Kentucky
#'
#' KY has separate files for each year's regular session (RS).
#' This hook loads and combines them.
#'
#' @param bill_dir Path to bill directory
#' @param state State code ("KY")
#' @param term Term in format "YYYY_YYYY"
#' @param verbose Show detailed logging (default TRUE)
#' @return List with bill_details and bill_history dataframes
load_bill_files <- function(bill_dir, state, term, verbose = TRUE) {
  # Parse term years
  years <- strsplit(term, "_")[[1]]
  term_start_year <- years[1]
  term_end_year <- years[2]

  # Find all bill detail files for this term
  all_files <- list.files(bill_dir, full.names = TRUE)
  detail_files <- all_files[
    grepl(glue("{state}_Bill_Details"), all_files) &
      (grepl(term_start_year, all_files) | grepl(term_end_year, all_files))
  ]

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
    read_csv(f, show_col_types = FALSE)
  }))

  # Find and load bill history files
  history_files <- all_files[
    grepl(glue("{state}_Bill_Histories"), all_files) &
      (grepl(term_start_year, all_files) | grepl(term_end_year, all_files))
  ]

  if (verbose) {
    cli_log(glue("Found {length(history_files)} bill history files for {term}"))
  }

  bill_history <- bind_rows(lapply(history_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read_csv(f, show_col_types = FALSE)
  }))

  # Remove any duplicate rows
  bill_details <- distinct(bill_details)
  bill_history <- distinct(bill_history)

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 1.5 Hook: Preprocess raw data
#' Preprocess raw data for Kentucky
#'
#' Normalizes raw CSV data into standard schema.
#' KY uses "bill_number" in raw data.
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

#' Fix sponsor names for specific KY terms
#'
#' Handles term-specific name corrections. From old script lines 157-196.
#'
#' @param names Vector of sponsor names
#' @param term Term string
#' @param bill_ids Vector of bill IDs (for context-aware fixes)
#' @param sessions Vector of session values
#' @return Vector of corrected names
fix_names <- function(names, term, bill_ids = NULL, sessions = NULL) {
  # Regina Huff = Regina Petrey Bunch - listed as Huff but Bunch in Klarner
  # Applies 2011-2021 per old script line 192-196
  if (term %in% c("2011_2012", "2013_2014", "2015_2016", "2017_2018",
                  "2019_2020", "2021_2022", "2023_2024")) {
    # Only apply to House bills (she was in the House)
    if (!is.null(bill_ids)) {
      house_bills <- substring(bill_ids, 1, 1) == "H"
      names[house_bills & names == "r. huff"] <- "r. bunch huff"
    } else {
      names[names == "r. huff"] <- "r. bunch huff"
    }
  }

  names
}

# Stage 2 Hook: Clean bill details
#' Clean bill details for Kentucky
#'
#' KY-specific transformations:
#' - Create LES_sponsor from sponsors (first sponsor)
#' - Normalize sponsor names (lowercase, accent removal)
#' - Filter to valid bill types (HB, SB)
#' - Create session from session_year and session_type
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @param verbose Show detailed logging (default TRUE)
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Store unfiltered version
  all_bill_details <- bill_details

  # Standardize sponsors - lowercase and remove accents
  bill_details$sponsors <- tolower(bill_details$sponsors)
  bill_details$sponsors <- standardize_accents(bill_details$sponsors)

  # Clean up jr./sr. formatting (from old script lines 153-155)
  bill_details$sponsors <- gsub(" jr\\.", " jr", bill_details$sponsors)
  bill_details$sponsors <- gsub(" sr\\.", " sr", bill_details$sponsors)
  bill_details$sponsors <- gsub(" st\\. ", " saint ", bill_details$sponsors)

  # Add term, session, and derive bill_type
  bill_details <- bill_details %>%
    mutate(
      term = term,
      session = paste(.data$session_year, .data$session_type, sep = "-"),
      # Standardize bill_id format (zero-pad to 4 digits)
      bill_id = paste0(
        gsub("[0-9]+", "", .data$bill_id),
        str_pad(gsub("^[A-Za-z]+", "", .data$bill_id), 4, pad = "0")
      ),
      bill_type = toupper(gsub("[0-9].*", "", .data$bill_id))
    )

  # Create LES_sponsor (first sponsor from semicolon/comma-delimited list)
  bill_details$LES_sponsor <- gsub(";.+|,.+", "", bill_details$sponsors)
  bill_details$LES_sponsor <- str_trim(bill_details$LES_sponsor)

  # Apply term-specific name fixes
  bill_details$LES_sponsor <- fix_names(
    bill_details$LES_sponsor, term,
    bill_details$bill_id, bill_details$session
  )

  # Filter to valid bill types
  keep_types <- ky_config$bill_types
  bill_details <- bill_details %>%
    filter(.data$bill_type %in% keep_types)

  if (verbose) {
    cli_log(glue("After bill type filter: {nrow(bill_details)} bills"))
  }

  # Drop bills with missing/empty sponsor
  empty_sponsor_bills <- bill_details %>%
    filter(.data$LES_sponsor == "" | is.na(.data$LES_sponsor))

  if (nrow(empty_sponsor_bills) > 0) {
    if (verbose) {
      cli_log(glue(
        "Dropping {nrow(empty_sponsor_bills)} bills without sponsor"
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

# Stage 2 Hook: Clean bill history
#' Clean bill history for Kentucky
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  bill_history %>%
    mutate(
      term = term,
      session = paste(.data$session_year, .data$session_type, sep = "-"),
      # Standardize bill_id format
      bill_id = paste0(
        gsub("[0-9]+", "", .data$bill_id),
        str_pad(gsub("^[A-Za-z]+", "", .data$bill_id), 4, pad = "0")
      ),
      # Derive chamber from action text (from old script lines 369-381)
      chamber = case_when(
        .data$order == 1 ~ substring(.data$bill_id, 1, 1),
        grepl("in House$|\\(H\\)$|^House|signed by.+House", .data$action) ~ "H",
        grepl("in Senate$|\\(S\\)$|^Senate|signed by.+Senate", .data$action) ~
          "S",
        grepl("to governor|by governor", tolower(.data$action)) ~ "G",
        grepl("conference", tolower(.data$action)) ~ "CC",
        TRUE ~ NA_character_
      )
    ) %>%
    group_by(.data$term, .data$session, .data$bill_id) %>%
    tidyr::fill(.data$chamber) %>%
    ungroup() %>%
    mutate(
      # Recode chamber names
      chamber = recode(.data$chamber,
                       "H" = "House", "S" = "Senate",
                       "G" = "Governor", "CC" = "Conference"),
      # Fix "passed over" so it doesn't match pass patterns
      action = gsub("^passed over", "bill passed over", .data$action)
    )
}

# Stage 3 Hook: Transform SS bill IDs
#' Transform SS bill IDs for Kentucky
#'
#' PVS encodes special session bills with unusual IDs like HB10001, HB50005.
#' These represent special session bills where the first digit after bill type
#' indicates the special session number (e.g., HB10001 = session 1, bill 1).
#' This hook remaps them to standard format (e.g., HB0001).
#'
#' From old script lines 208-229.
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return SS bills with transformed bill IDs
transform_ss_bills <- function(ss_bills, term) {
  # Pattern: HBx000y or SBx000y where x is session number, y is bill number
  # Convert to HB000y or SB000y (remove the session digit prefix)
  # Regex: ([HS]B)(\d)(\d{4}) -> \1\3

  ss_bills %>%
    mutate(
      bill_id = gsub("^([HS]B)(\\d)(\\d{4})$", "\\1\\3", .data$bill_id)
    )
}

# Stage 3 Hook: Enrich SS bills with session
#' Add session information to SS bills
#'
#' KY has separate sessions per year (2023-RS, 2024-RS) and bill numbers restart.
#' Map SS bills to their correct session based on the year column from PVS data.
#' This matches the old script's approach of using year in deduplication.
#'
#' @param ss_bills SS bills dataframe (already transformed)
#' @param term Term string
#' @return SS bills with session column added
enrich_ss_with_session <- function(ss_bills, term) {
  ss_bills %>%
    mutate(
      session = paste(.data$year, "RS", sep = "-")
    )
}

# Stage 3 Hook: Get missing SS bills
#' Identify genuinely missing SS bills vs intentionally excluded
#'
#' @param ss_filtered SS bills dataframe
#' @param bill_details Filtered bill_details dataframe
#' @param all_bill_details Unfiltered bill_details dataframe
#' @return Dataframe of genuinely missing SS bills
get_missing_ss_bills <- function(ss_filtered, bill_details, all_bill_details) {
  # Find SS bills missing from bill_details
  missing_ss <- ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term"))

  # Check if they exist in all_bill_details (unfiltered)
  missing_in_all <- missing_ss %>%
    semi_join(all_bill_details, by = "bill_id")

  # If they exist in all_bill_details, check if they're committee-sponsored
  if (nrow(missing_in_all) > 0) {
    missing_with_sponsor <- missing_in_all %>%
      left_join(
        all_bill_details %>% select("bill_id", "sponsors"),
        by = "bill_id"
      )

    # Genuinely missing = not committee-sponsored
    genuinely_missing <- missing_with_sponsor %>%
      filter(!grepl("committee", .data$sponsors, ignore.case = TRUE)) %>%
      select("bill_id", "term")

    return(genuinely_missing)
  }

  # If they don't exist in all_bill_details at all, they're genuinely missing
  missing_ss %>%
    anti_join(all_bill_details, by = "bill_id") %>%
    select("bill_id", "term")
}

# Stage 4 Hook: Derive unique sponsors
#' Derive unique sponsors for Kentucky
#'
#' @param bills Bills dataframe with achievement metrics
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregated metrics
derive_unique_sponsors <- function(bills, term) {
  bills %>%
    mutate(chamber = ifelse(substring(.data$bill_id, 1, 1) == "H",
                            "H", "S")) %>%
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

# Stage 5 Hook: Clean sponsor names
#' Clean sponsor names for matching
#'
#' Creates match keys for fuzzy joining.
#'
#' @param all_sponsors Sponsors dataframe
#' @param term Term string
#' @return Modified all_sponsors with match_name_chamber
clean_sponsor_names <- function(all_sponsors, term) {
  # Apply term-specific name fixes for matching
  if (term %in% c("2011_2012", "2013_2014", "2015_2016", "2017_2018",
                  "2019_2020", "2021_2022", "2023_2024")) {
    all_sponsors <- all_sponsors %>%
      mutate(
        # For matching purposes, use "bunch" as last name
        # (matches Klarner/legiscan which has "Bunch")
        last_name_for_match = ifelse(
          .data$LES_sponsor == "r. bunch huff",
          "bunch", NA_character_
        )
      )
  }

  all_sponsors %>%
    mutate(
      match_name_chamber = paste(
        tolower(.data$LES_sponsor),
        tolower(substr(.data$chamber, 1, 1)),
        sep = "-"
      )
    ) %>%
    select(-any_of("last_name_for_match"))
}

# Stage 5 Hook: Adjust legiscan data
#' Adjust legiscan data for Kentucky
#'
#' Creates match keys for fuzzy joining
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
      match_name = ifelse(.data$n >= 2,
                          glue("{substr(first_name, 1, 1)}. {last_name}"),
                          .data$last_name),
      match_name_chamber = tolower(paste(.data$match_name,
                                         substr(.data$district, 1, 1),
                                         sep = "-"))
    )
}

# Stage 5 Hook: Reconcile legiscan with sponsors
#' Reconcile legiscan with sponsors using fuzzy matching
#'
#' @param all_sponsors Sponsors dataframe
#' @param legiscan_adjusted Adjusted legiscan dataframe
#' @param term Term string
#' @return Joined dataframe
reconcile_legiscan_with_sponsors <- function(all_sponsors,
                                              legiscan_adjusted, term) {
  if (term == "2023_2024") {
    joined <- inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # Explicit correct mappings for names that fuzzy matching gets wrong:
        # Format: "legiscan_match_name_chamber" = "sponsor_match_name_chamber"

        # George Brown Jr. - legiscan has "Brown" but bills have "G. Brown Jr"
        "brown-h" = "g. brown jr-h",

        # Jim Gooch Jr. - legiscan has "Gooch" but bills have "J. Gooch Jr"
        "gooch-h" = "j. gooch jr-h",

        # Shelley Frommeyer - known as "S. Funke Frommeyer" in bills
        "frommeyer-s" = "s. funke frommeyer-s",

        # Suzanne Miles - legiscan has "Miles" but bills have "S. Miles"
        "miles-h" = "s. miles-h",

        # Block legislators with no bills (zero-LES) from incorrectly matching
        # to sponsors with similar names. These legislators legitimately
        # sponsored zero bills and should appear with LES=0.
        "griffee-h" = NA_character_,   # Peyton Griffee - no bills
        "gilbert-h" = NA_character_    # Courtney Gilbert - no bills
      )
    )

    # Add back any missing sponsors not matched
    missing_sponsors <- all_sponsors %>%
      anti_join(joined, by = "LES_sponsor")
    if (nrow(missing_sponsors) > 0) {
      joined <- bind_rows(joined, missing_sponsors)
    }

    joined
  } else {
    # Generic fuzzy matching for other terms
    joined <- inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full"
    )

    # Add back any missing sponsors
    missing_sponsors <- all_sponsors %>%
      anti_join(joined, by = "LES_sponsor")
    if (nrow(missing_sponsors) > 0) {
      joined <- bind_rows(joined, missing_sponsors)
    }

    joined
  }
}

# Stage 7 Hook: Prepare bills for LES calculation
#' Prepare bills for LES calculation
#'
#' Derives chamber from bill_id prefix (HB = H, SB = S)
#'
#' @param bills_prepared Bills dataframe
#' @return Bills with chamber column added
prepare_bills_for_les <- function(bills_prepared) {
  bills_prepared %>%
    mutate(chamber = ifelse(substring(.data$bill_id, 1, 1) == "H",
                            "H", "S"))
}

# Export configuration and hooks
c(
  ky_config,
  list(
    load_bill_files = load_bill_files,
    preprocess_raw_data = preprocess_raw_data,
    clean_bill_details = clean_bill_details,
    clean_bill_history = clean_bill_history,
    transform_ss_bills = transform_ss_bills,
    enrich_ss_with_session = enrich_ss_with_session,
    get_missing_ss_bills = get_missing_ss_bills,
    derive_unique_sponsors = derive_unique_sponsors,
    prepare_bills_for_les = prepare_bills_for_les,
    clean_sponsor_names = clean_sponsor_names,
    adjust_legiscan_data = adjust_legiscan_data,
    reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors
  )
)
