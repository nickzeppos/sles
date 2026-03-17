# Oklahoma State Configuration
# State-specific constants and settings for LES estimation
#
# OK is a non-carryover state for special sessions - bill IDs restart.
# Bills carry over across regular sessions within a term.
# Sessions: RS (Regular), SS1/SS2/etc (Special Sessions)
# Session format in data: "2023-RS", "2023-SS1", etc.
# Sponsor format: H_author and S_author columns with "(H)" or "(S)" suffix.
# Coauthors tracked in separate column (semicolon-separated).

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
ok_config <- list(
  # Valid bill types for this state
  bill_types = c("HB", "SB"),

  # Sponsor patterns to drop from analysis
  drop_sponsor_pattern = "committee",

  # Step terms for evaluating bill history (from old script lines 552-557)
  step_terms = list(
    aic = c(
      "^cr", "do pass", "do not pass", "committee substitute",
      "^reported", "recommendation to the full comm", "; cr filed"
    ),
    abc = c(
      "do pass", "general order", "^amended", "^advanced",
      "engrossed", "third reading", "considered"
    ),
    pass = c(
      "measure passed", "emergency passed", "measure and emergency passed",
      "engrossed.+to (senate|house)"
    ),
    law = c("approved by governor", "becomes law without governor")
  )
)
# nolint end: line_length_linter

#' Check if a bill should be dropped based on sponsor
#'
#' @param sponsor Sponsor name
#' @return TRUE if bill should be dropped, FALSE otherwise
should_drop_bill <- function(sponsor) {
  grepl(ok_config$drop_sponsor_pattern, sponsor, ignore.case = TRUE)
}

# Stage 1 Hook: Load bill files
#' Load bill files for Oklahoma
#'
#' OK has multiple sessions per term: RS, SS1, SS2, etc.
#' File naming: {STATE}_Bill_Details_{YEAR}_{SESSION}.csv
#'
#' @param bill_dir Path to bill directory
#' @param state State code ("OK")
#' @param term Term in format "YYYY_YYYY"
#' @param verbose Show detailed logging (default TRUE)
#' @return List with bill_details and bill_history dataframes
load_bill_files <- function(bill_dir, state, term, verbose = TRUE) {
  # Parse term years
  years <- strsplit(term, "_")[[1]]
  year1 <- years[1]
  year2 <- years[2]

  # Find all files for this term (all sessions for these years)
  all_files <- list.files(bill_dir, full.names = TRUE)

  # Match files starting with year1 or year2
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
    df <- read.csv(f, stringsAsFactors = FALSE)
    if (nrow(df) == 0) return(NULL)
    if (verbose) cli_log(glue("  Loading {basename(f)} ({nrow(df)} rows)"))
    df
  }))

  # Load and combine bill histories
  bill_history <- bind_rows(lapply(history_files, function(f) {
    df <- read.csv(f, stringsAsFactors = FALSE)
    if (nrow(df) == 0) return(NULL)
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    df
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
#' Preprocess raw data for Oklahoma
#'
#' Normalizes raw CSV data into standard schema.
#' OK already uses bill_id column. Session needs parsing.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  # Bill details already have bill_id
  # Session format is "2023-RS" which we'll keep

  # Standardize bill IDs (ensure proper padding)
  bill_details <- bill_details %>%
    mutate(
      bill_id = paste0(
        gsub("[0-9]+", "", .data$bill_id),
        str_pad(gsub("^[A-Z]+", "", .data$bill_id), 4, pad = "0")
      )
    )

  bill_history <- bill_history %>%
    mutate(
      bill_id = paste0(
        gsub("[0-9]+", "", .data$bill_id),
        str_pad(gsub("^[A-Z]+", "", .data$bill_id), 4, pad = "0")
      )
    )

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

#' Clean bill details for Oklahoma
#'
#' OK-specific transformations:
#' - Handle duplicate bills that carry over from odd to even year
#' - Extract LES_sponsor from H_author or S_author based on bill prefix
#' - Standardize accents in sponsor names
#' - Drop "test" bills
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @param verbose Show detailed logging (default TRUE)
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Store unfiltered version
  all_bill_details <- bill_details

  # Parse term years
  years <- strsplit(term, "_")[[1]]
  year1 <- as.integer(years[1])

  # Add term column
  bill_details$term <- term

  # Handle duplicate bills that carry over (from old script lines 121-143)
  # For RS bills, keep only the second-year version if both have same intro_date
  dup_bills <- c()
  for (i in seq_len(nrow(bill_details))) {
    if (bill_details[i, ]$session == paste0(year1, "-RS")) {
      bill_sub <- bill_details %>%
        filter(
          .data$bill_id == bill_details[i, ]$bill_id,
          grepl("RS", .data$session)
        )
      if (nrow(bill_sub) > 1 &&
          length(unique(bill_sub$intro_date)) == 1) {
        dup_bills <- append(dup_bills, bill_details[i, ]$bill_id)
      }
    }
  }

  if (length(dup_bills) > 0 && verbose) {
    cli_log(glue("Removing {length(unique(dup_bills))} duplicate carryover bills"))
  }
  bill_details <- bill_details %>%
    filter(!(
      .data$bill_id %in% dup_bills &
        .data$session == paste0(year1, "-RS")
    ))

  # Derive bill_type and filter
  bill_details <- bill_details %>%
    mutate(bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))) %>%
    filter(.data$bill_type %in% ok_config$bill_types) %>%
    select(-"bill_type")

  # Drop "test" bills (from old script lines 158-160)
  # Also drop bills with test sponsor names (ztesthauthora, ztestsauthora, etc.)
  test_bills <- bill_details %>%
    filter(
      grepl("^test$|this is a test (bill|request)", tolower(.data$title)) |
        grepl("^ztest", tolower(.data$H_author)) |
        grepl("^ztest", tolower(.data$S_author)) |
        grepl("^HB999[0-9]|^SB999[0-9]|^SB900[0-9]|^SB901[0-9]", .data$bill_id)
    )
  if (nrow(test_bills) > 0 && verbose) {
    cli_log(glue("Dropping {nrow(test_bills)} test bills"))
  }
  bill_details <- bill_details %>%
    filter(
      !grepl("^test$|this is a test (bill|request)", tolower(.data$title)) &
        !grepl("^ztest", tolower(.data$H_author)) &
        !grepl("^ztest", tolower(.data$S_author)) &
        !grepl("^HB999[0-9]|^SB999[0-9]|^SB900[0-9]|^SB901[0-9]", .data$bill_id)
    )

  # Clean H_author and S_author fields
  bill_details$H_author <- tolower(bill_details$H_author)
  bill_details$H_author <- standardize_accents(bill_details$H_author)

  bill_details$S_author <- tolower(bill_details$S_author)
  bill_details$S_author <- standardize_accents(bill_details$S_author)

  # Get chamber_author based on bill prefix
  bill_details$chamber_author <- ifelse(
    substring(bill_details$bill_id, 1, 1) == "H",
    bill_details$H_author,
    ifelse(
      substring(bill_details$bill_id, 1, 1) == "S",
      bill_details$S_author,
      ""
    )
  )

  # LES_sponsor = chamber_author without the (H) or (S) suffix
  bill_details$LES_sponsor <- gsub(" \\(h\\)| \\(s\\)", "",
    bill_details$chamber_author)
  bill_details$LES_sponsor <- str_trim(bill_details$LES_sponsor)

  # Handle multi-sponsor fields: extract first sponsor only
  # Some bills have multiple sponsors like "howard; gollihare" or "west and olsen"
  bill_details$LES_sponsor <- gsub(";.+| and .+", "",
    bill_details$LES_sponsor)

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

#' Clean bill history for Oklahoma
#'
#' OK-specific transformations:
#' - Standardize bill IDs
#' - Chamber standardization
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  bill_history <- bill_history %>%
    mutate(term = term)

  # Order by session, bill_id, action_date, order
  bill_history <- bill_history %>%
    arrange(.data$session, .data$bill_id, .data$action_date, .data$order)

  # Standardize chamber names
  bill_history$chamber <- recode(
    bill_history$chamber,
    "H" = "House", "S" = "Senate", "G" = "Governor", "CC" = "Conference"
  )

  # Clean action text
  bill_history$action <- str_trim(bill_history$action)

  # Lowercase action for pattern matching
  bill_history$action <- tolower(bill_history$action)

  bill_history
}

#' Transform SS bills for Oklahoma
#'
#' OK SS files have format "HB 1792" -> "HB1792".
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
#' OK bill IDs restart in special sessions, so some bill_ids appear in
#' both RS and SS sessions. A review CSV resolves ambiguous bills;
#' all others default to the first RS session.
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return SS bills with session column added
enrich_ss_with_session <- function(ss_bills, term) {
  years <- strsplit(term, "_")[[1]]
  rs_session <- paste0(years[2], "-RS")

  review_path <- file.path(
    repo_root, ".data", "OK", "review",
    "OK_SS_session_resolution.csv"
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
#' OK has coauthors that are counted separately.
#'
#' @param bills Dataframe of bills with achievement columns
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregate stats
derive_unique_sponsors <- function(bills, term) {
  # Get primary sponsors
  all_sponsors <- bills %>%
    mutate(chamber = ifelse(substring(.data$bill_id, 1, 1) == "H", "H", "S")) %>%
    select("LES_sponsor", "chamber_author", "chamber",
           "passed_chamber", "law") %>%
    mutate(term = term) %>%
    group_by(.data$LES_sponsor, .data$chamber_author, .data$chamber, .data$term) %>%
    summarize(
      num_sponsored_bills = n(),
      sponsor_pass_rate = sum(.data$passed_chamber) / n(),
      sponsor_law_rate = sum(.data$law) / n(),
      .groups = "drop"
    )

  # Add coauthors who didn't sponsor any bills
  if ("coauthors" %in% names(bills)) {
    unique_cospon <- str_trim(unique(unlist(str_split(bills$coauthors, "; "))))
    unique_cospon <- unique_cospon[unique_cospon != "" &
                                   unique_cospon != "none" &
                                   !is.na(unique_cospon)]

    for (nonspon in unique_cospon) {
      if (!(nonspon %in% all_sponsors$chamber_author) && nonspon != "") {
        chamb <- toupper(gsub("\\(|\\)", "",
          str_extract(nonspon, "\\((h|s)\\)")))
        if (!is.na(chamb) && chamb %in% c("H", "S")) {
          all_sponsors <- bind_rows(
            all_sponsors,
            tibble(
              LES_sponsor = gsub(" \\((h|s)\\)", "", nonspon),
              chamber_author = nonspon,
              chamber = chamb,
              term = term,
              num_sponsored_bills = 0,
              sponsor_pass_rate = 0,
              sponsor_law_rate = 0
            )
          )
        }
      }
    }
  }

  # Count cosponsored bills
  all_sponsors$num_cosponsored_bills <- 0
  if ("coauthors" %in% names(bills)) {
    for (i in seq_len(nrow(all_sponsors))) {
      c_sub <- bills[substring(bills$bill_id, 1, 1) %in%
                       ifelse(all_sponsors[i, ]$chamber == "H", "H", "S"), ]
      sn <- all_sponsors[i, ]$chamber_author
      # Escape parentheses for regex
      sn <- gsub("\\(", "\\\\(", gsub("\\)", "\\\\)", sn))
      search_term <- paste0("^", sn, "$|^", sn, ";|; ", sn, "$|; ", sn, ";")
      all_sponsors$num_cosponsored_bills[i] <- sum(
        grepl(search_term, tolower(c_sub$coauthors))
      )
      # Adjust for overlap with sponsored
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
#' OK uses LES_sponsor + chamber for matching.
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
#' OK uses last_name or last_name (first_name) if multiple legislators share last name.
#' Chamber derived from district (HD/SD prefix).
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
      # Use last_name (first_name) if there are duplicates
      match_name = ifelse(
        .data$n >= 2,
        paste0(.data$last_name, " (", .data$first_name, ")"),
        .data$last_name
      ),
      match_name_chamber = tolower(paste(
        .data$match_name,
        substr(.data$district, 1, 1),
        sep = "-"
      ))
    ) %>%
    distinct(.data$match_name_chamber, .keep_all = TRUE)
}

#' Reconcile legiscan with sponsors
#'
#' Uses fuzzy matching with full names.
#' Term-specific custom_match blocks handle name variants.
#'
#' @param sponsors Dataframe of unique sponsors
#' @param legiscan Adjusted legiscan dataframe
#' @param term Term string
#' @return Joined dataframe of sponsors matched to legiscan records
# nolint start: line_length_linter
reconcile_legiscan_with_sponsors <- function(sponsors, legiscan, term) {
  if (term == "2019_2020") {
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "hall (elise)-h" = NA_character_,
        "caldwell (hurchel)-h" = "caldwell (trey)-h",
        "thompson-s" = "thompson (roger)-s",
        "dossett-s" = "dossett (j.j.)-s",
        "lowe-h" = "lowe (jason)-h"
      )
    )
  } else if (term == "2021_2022") {
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "matthews (kevin)-s" = "matthews-s",
        "hall (elise)-h" = NA_character_,
        "hall (chuck)-s" = "hall-s",
        "caldwell (hurchel)-h" = "caldwell (trey)-h",
        "roberts (eric)-h" = "roberts-h",
        "thompson-s" = "thompson (roger)-s",
        "dossett (jo)-s" = "dossett-s"
      )
    )
  } else if (term == "2023_2024") {
    # Custom matches for 2023_2024
    # Shane Jett appears with 2 roles in legiscan (Sen in 2023, Rep in 2024)
    # so n=2 and name becomes "jett (shane)" instead of "jett"
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # Jett appears as duplicate in legiscan, but sponsor data has just "jett"
        "jett (shane)-s" = "jett-s",
        # Matthews appears as duplicate? Map to simple form
        "matthews (kevin)-s" = "matthews-s",
        # Young appears as duplicate? Map to simple form
        "young (george)-s" = "young-s",
        # Caldwell: Trey Caldwell's legal first name is "Hurchel"
        "caldwell (hurchel)-h" = "caldwell (trey)-h",
        # Burns: George Burns (Senator) -> sponsor "burns" in Senate
        "burns (george)-s" = "burns-s",
        # Burns: Ty Burns (House Rep) -> sponsor "burns" in House
        "burns (ty)-h" = "burns-h"
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
  bill_types = ok_config$bill_types,
  step_terms = ok_config$step_terms,
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
