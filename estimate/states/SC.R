# South Carolina State Configuration
# State-specific constants and settings for LES estimation
#
# SC notes from old script:
# - Special sessions appear to be folded into full term - no separate delineation
# - Bill IDs uniquely identify bills across regular/specials in a term
# - Committee sponsors permitted
# - Bills sometimes referred to COUNTY DELEGATIONS instead of committees
# - Sponsor format: "Representative <name>" or "Senator <name>"

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
sc_config <- list(
  # Valid bill types for this state (filter out resolutions)
  bill_types = c("General Bill", "Joint Resolution"),

  # Sponsor patterns to drop from analysis
  drop_sponsor_pattern = "^senate|^house|committee",

  # Step terms for evaluating bill history (from old script lines 359-371)
  step_terms = list(
    aic = c(
      "committee report", "tabled in committee", "referred to subcommittee",
      "polled out of committee", "delegation report"
    ),
    abc = c(
      "committee report: fav", "committee report: majority fav",
      "committee report: recommend", "delegation report: fav",
      "delegation report: majority fav", "delegation report: recommend",
      "read second time", "second reading", "read third time", "third reading",
      "^amended", "^debate", "^objection", "recommitted to"
    ),
    pass = c(
      "read third time and sent to senate", "read third time and sent to house",
      "enrolled"
    ),
    law = c("signed by governor", "act no\\. [0-9]+", "effective date [0-9]+")
  )
)
# nolint end: line_length_linter

#' Check if a bill should be dropped based on sponsor
#'
#' @param sponsor Sponsor name
#' @return TRUE if bill should be dropped, FALSE otherwise
should_drop_bill <- function(sponsor) {
  grepl(sc_config$drop_sponsor_pattern, sponsor, ignore.case = TRUE)
}

#' Get file suffix for bill files
#'
#' SC uses term format directly (2023_2024) for file naming.
#'
#' @param term Term string (e.g., "2023_2024")
#' @return The suffix to use for bill file names
get_bill_file_suffix <- function(term) {
  term
}

# Stage 1.5 Hook: Preprocess raw data
#' Preprocess raw data for South Carolina
#'
#' Normalizes raw CSV data into standard schema.
#' SC uses bill_number -> bill_id, and session is already in term format.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  # Rename bill_number to bill_id
  if ("bill_number" %in% names(bill_details)) {
    bill_details <- bill_details %>%
      rename(bill_id = "bill_number")
  }

  if ("bill_number" %in% names(bill_history)) {
    bill_history <- bill_history %>%
      rename(bill_id = "bill_number")
  }

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

#' Clean bill details for South Carolina
#'
#' SC-specific transformations:
#' - Strip "Representative"/"Senator" prefix from sponsor
#' - Standardize accents in sponsor names
#' - Filter to valid bill types
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

  # Filter to valid bill types (exclude resolutions)
  bill_details <- bill_details %>%
    filter(.data$bill_type %in% sc_config$bill_types)

  # Clean primary_sponsor field - strip prefix
  bill_details$primary_sponsor <- tolower(bill_details$primary_sponsor)
  bill_details$primary_sponsor <- standardize_accents(bill_details$primary_sponsor)

  # LES_sponsor = primary_sponsor without "representative " or "senator " prefix
  bill_details$LES_sponsor <- gsub(
    "^representative |^senator ",
    "",
    bill_details$primary_sponsor
  )
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

#' Clean bill history for South Carolina
#'
#' SC-specific transformations:
#' - Handle blank chambers for executive actions
#' - Fill in blank chambers from adjacent observations
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  bill_history <- bill_history %>%
    mutate(term = term)

  # Order by bill_id, order
  bill_history <- bill_history %>%
    arrange(.data$bill_id, .data$order)

  # Code Executive chamber for governor actions
  exec_pattern <- paste0(
    "signed by governor|^ratified|^act no\\.|^effective date|",
    "^vetoed|^became law without gov"
  )
  bill_history <- bill_history %>%
    mutate(
      chamber = ifelse(
        grepl(exec_pattern, tolower(.data$action)) & .data$chamber == "",
        "Executive",
        .data$chamber
      )
    )

  # Fill blank chambers from adjacent observations
  bill_history <- bill_history %>%
    mutate(chamber = ifelse(.data$chamber == "", NA, .data$chamber)) %>%
    group_by(.data$bill_id) %>%
    tidyr::fill("chamber") %>%
    ungroup()

  # Clean action text
  bill_history$action <- str_trim(bill_history$action)

  bill_history
}

#' Derive unique sponsors from bills
#'
#' SC counts cosponsors from sponsors column.
#'
#' @param bills Dataframe of bills with achievement columns
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregate stats
derive_unique_sponsors <- function(bills, term) {
  # Get primary sponsors
  all_sponsors <- bills %>%
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

  # Compute cosponsorship
  all_sponsors$num_cosponsored_bills <- NA
  if ("sponsors" %in% names(bills)) {
    bills$cospon_match <- paste(
      bills$LES_sponsor,
      tolower(bills$sponsors),
      sep = "; "
    )
    for (i in seq_len(nrow(all_sponsors))) {
      c_sub <- bills[substring(bills$bill_id, 1, 1) %in%
                       ifelse(all_sponsors[i, ]$chamber == "H", "H", "S"), ]
      all_sponsors$num_cosponsored_bills[i] <- sum(
        grepl(all_sponsors[i, ]$LES_sponsor, tolower(c_sub$cospon_match))
      )
      # Subtract sponsored bills (counted in cospon due to column format)
      all_sponsors$num_cosponsored_bills[i] <-
        all_sponsors$num_cosponsored_bills[i] -
        all_sponsors$num_sponsored_bills[i]
      if (all_sponsors$num_cosponsored_bills[i] < 0) {
        all_sponsors$num_cosponsored_bills[i] <- 0
      }
    }
  }

  all_sponsors
}

#' Clean sponsor names for matching
#'
#' SC uses LES_sponsor + chamber for matching.
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
#' SC uses F.M. format (first initial + middle initial) for duplicates
#' to create unique match keys. Sponsor data uses similar format.
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string
#' @return Adjusted legiscan dataframe with match_name_chamber column
adjust_legiscan_data <- function(legiscan, term) {
  if (term == "2023_2024") {
    # Tedder is a chamber switcher: won special election for SD-042
    # mid-term. Legiscan only has House record (HD-109).
    tedder <- legiscan[legiscan$people_id == 21753, ]
    if (nrow(tedder) > 0) {
      tedder_senate <- tedder
      tedder_senate$role <- "Sen"
      tedder_senate$district <- "SD-042"
      legiscan <- bind_rows(legiscan, tedder_senate)
    }
  }

  legiscan %>%
    filter(.data$committee_id == 0) %>%
    group_by(.data$last_name, .data$role) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      # Always use F.M. format when there are duplicates to create unique keys
      # Handle NA middle_name by using empty string
      middle_init = ifelse(
        is.na(.data$middle_name) | .data$middle_name == "",
        "",
        tolower(substr(.data$middle_name, 1, 1))
      ),
      match_name = case_when(
        .data$n >= 2 & .data$middle_init != "" ~ paste0(
          tolower(substr(.data$first_name, 1, 1)), ".",
          .data$middle_init, ". ",
          .data$last_name
        ),
        .data$n >= 2 ~ paste0(
          tolower(substr(.data$first_name, 1, 1)), ". ",
          .data$last_name
        ),
        TRUE ~ .data$last_name
      ),
      match_name_chamber = tolower(paste(
        .data$match_name,
        substr(.data$district, 1, 1),
        sep = "-"
      ))
    )
}

#' Reconcile legiscan with sponsors
#'
#' Uses fuzzy matching. Term-specific custom_match handles name variants.
#'
#' @param sponsors Dataframe of unique sponsors
#' @param legiscan Adjusted legiscan dataframe
#' @param term Term string
#' @return Joined dataframe of sponsors matched to legiscan records
# nolint start: line_length_linter
reconcile_legiscan_with_sponsors <- function(sponsors, legiscan, term) {
  if (term == "2023_2024") {
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # Kevin Johnson uses K. in sponsor data (no middle initial)
        "k.l. johnson-s" = "k. johnson-s",
        # Michael Johnson goes by his middle name (Robert Michael Johnson)
        "r.m. johnson-s" = "m. johnson-s",
        # Murrell Smith is G. Murrell Smith (sponsor uses g.m. smith)
        "m. smith-h" = "g.m. smith-h",
        # Middle initial in legiscan but not in sponsor data
        "b.m. newton-h" = "b. newton-h",
        "s.o. jones-h" = "s. jones-h",
        "t.a. moore-h" = "t. moore-h",
        "w.k. jones-h" = "w. jones-h",
        # Zero-bill legislators: only sponsored resolutions (filtered out)
        # or arrived mid-term. Prevent spurious fuzzy matches.
        "alexander-h" = NA_character_,
        "anderson-h" = NA_character_,
        "atkinson-h" = NA_character_,
        "cash-s" = NA_character_,
        "gibson-h" = NA_character_,
        "guest-h" = NA_character_,
        "hager-h" = NA_character_,
        "neese-h" = NA_character_,
        "spann-wilder-h" = NA_character_,
        "vaughan-h" = NA_character_,
        "wheeler-h" = NA_character_,
        "whitmire-h" = NA_character_
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
  bill_types = sc_config$bill_types,
  step_terms = sc_config$step_terms,
  get_bill_file_suffix = get_bill_file_suffix,
  preprocess_raw_data = preprocess_raw_data,
  clean_bill_details = clean_bill_details,
  clean_bill_history = clean_bill_history,
  derive_unique_sponsors = derive_unique_sponsors,
  clean_sponsor_names = clean_sponsor_names,
  adjust_legiscan_data = adjust_legiscan_data,
  reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
  prepare_bills_for_les = prepare_bills_for_les
)
