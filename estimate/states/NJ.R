# New Jersey (NJ) State Configuration
#
# NJ is a biennial legislature. Bill numbers do not restart for
# special sessions (folded into main term).
# Bill types: A (Assembly), S (Senate)
# Sponsor format: "Last, First" (comma-separated)
# Has cosponsorship data via primary_sponsors column
# (semicolon-separated multiple primary sponsors)
# SS join on bill_id + term only (no session disambiguation needed)
# Chapter number validation: if law==0 but chapter_num populated,
# that is an error.
#
# Source: .dropbox/old_estimate_scripts/NJ - Estimate LES AV.R

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}

logging <- source(file.path(repo_root, "utils/logging.R"),
  local = TRUE
)$value
strings <- source(file.path(repo_root, "utils/strings.R"),
  local = TRUE
)$value

# Extract functions
cli_log <- logging$cli_log
cli_warn <- logging$cli_warn
cli_error <- logging$cli_error
standardize_accents <- strings$standardize_accents

# Load required libraries
library(dplyr)
library(stringr)
library(glue)

# nolint start: line_length_linter
nj_config <- list(
  bill_types = c("A", "S"),
  even_year_start = TRUE,

  # Step terms from old script lines 371-375
  step_terms = list(
    aic = c(
      "reported out of", "reported from",
      "reported and referred", "public hearing"
    ),
    abc = c(
      "reported out of", "reported from",
      "reported and referred", "2nd reading",
      "motion", "assembly floor amendment",
      "senate amendment", "recommitted to"
    ),
    pass = c(
      "^passed assembly", "^passed senate",
      "received in the assembly", "received in the senate"
    ),
    # cvor = conditional veto override
    law = c("^approved", "^cvor$")
  )
)
# nolint end: line_length_linter

# Stage 1 Hook: Bill file suffix
#' NJ uses full term for file naming (NJ_Bill_Details_2024_2025.csv).
#'
#' @param term Term string (e.g., "2024_2025")
#' @return The suffix to use for bill file names
get_bill_file_suffix <- function(term) {
  term
}

# Stage 1.5 Hook: Preprocess raw data
#' Preprocess raw data for New Jersey
#'
#' Renames bill_number -> bill_id. Splits on "-" and pads to 4
#' digits: "A-1" -> "A0001". Sets term and session = term
#' (no sub-sessions).
#'
#' Faithful port of old script lines 135-140, 346-354.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  # --- Bill details ---
  bill_details <- bill_details %>%
    rename(bill_id = "bill_number")

  bill_parts <- str_split_fixed(bill_details$bill_id, "-", 2)
  bill_details$bill_id <- paste0(
    bill_parts[, 1],
    str_pad(as.numeric(bill_parts[, 2]), 4, pad = "0")
  )
  bill_details$term <- term
  bill_details$session <- term

  # Check for duplicates
  if (nrow(bill_details) != nrow(distinct(bill_details))) {
    cli_warn("Duplicate bill details detected")
  }

  # --- Bill history ---
  bill_history <- bill_history %>%
    rename(bill_id = "bill_number")

  bill_parts_h <- str_split_fixed(bill_history$bill_id, "-", 2)
  bill_history$bill_id <- paste0(
    bill_parts_h[, 1],
    str_pad(as.numeric(bill_parts_h[, 2]), 4, pad = "0")
  )
  bill_history$term <- term
  bill_history$session <- term

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 2 Hook: Clean bill details
#' Clean bill details for New Jersey
#'
#' 1. Filter to A/S bill types
#' 2. Lowercase primary_sponsors, standardize accents
#' 3. Check for by-request bills
#' 4. LES_sponsor = first sponsor before semicolon
#' 5. Drop committee-sponsored ("committee", "nrae", etc.)
#' 6. Drop empty sponsors and "open" reserved bill numbers
#'
#' Faithful port of old script lines 142-337.
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string
#' @param verbose Show detailed logging
#' @return List with all_bill_details and filtered bill_details
# nolint start: line_length_linter
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Store unfiltered
  all_bill_details <- bill_details

  # Filter to A/S (old script lines 144-146)
  bill_details <- bill_details %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    ) %>%
    filter(.data$bill_type %in% nj_config$bill_types) %>%
    select(-"bill_type")

  if (verbose) {
    cli_log(glue("Bills after type filter: {nrow(bill_details)}"))
  }

  # Lowercase and standardize accents (old script lines 154-159)
  bill_details$primary_sponsors <- tolower(
    bill_details$primary_sponsors
  )
  bill_details$primary_sponsors <- standardize_accents(
    bill_details$primary_sponsors
  )

  # Check for by-request bills (old script lines 162-165)
  if (any(grepl(
    "\\(br\\)|by request| br$", bill_details$primary_sponsors
  ))) {
    cli_warn("BY REQUEST bills detected - review needed")
  }

  # LES_sponsor = first sponsor before semicolon
  # (old script line 186)
  bill_details$LES_sponsor <- str_trim(gsub(
    ";.+", "", bill_details$primary_sponsors
  ))

  # Name variants
  bill_details$LES_sponsor[
    bill_details$LES_sponsor == "bailey, dave"
  ] <- "bailey, david"

  # Drop committee-sponsored bills (old script lines 321-323)
  comm_pattern <- "committee|^nrae$|maj nrae|^gov$"
  is_committee <- grepl(comm_pattern, bill_details$LES_sponsor)
  if (sum(is_committee) > 0 && verbose) {
    cli_log(glue(
      "Dropping {sum(is_committee)} committee-sponsored bills"
    ))
  }
  bill_details <- bill_details[!is_committee, ]

  # Drop empty sponsors (old script lines 328-331)
  empty <- bill_details$LES_sponsor == "" |
    is.na(bill_details$LES_sponsor)
  if (sum(empty) > 0) {
    if (verbose) {
      cli_log(glue(
        "Dropping {sum(empty)} bills without sponsor"
      ))
    }
    bill_details <- bill_details[!empty, ]
  }

  # Drop "open" reserved bill numbers (old script lines 334-337)
  is_open <- bill_details$LES_sponsor == "open"
  if (sum(is_open) > 0) {
    if (verbose) {
      cli_log(glue(
        "Dropping {sum(is_open)} reserved bill numbers"
      ))
    }
    bill_details <- bill_details[!is_open, ]
  }

  if (verbose) {
    cli_log(glue("After cleaning: {nrow(bill_details)} bills"))
  }

  list(
    all_bill_details = all_bill_details,
    bill_details = bill_details
  )
}
# nolint end: line_length_linter

# Stage 2 Hook: Clean bill history
#' Clean bill history for New Jersey
#'
#' Recode chamber: A->House, S->Senate, G->Governor.
#' Lowercase and trim actions. Arrange by bill_id, order.
#'
#' Faithful port of old script lines 356-358.
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  # Recode chamber (old script line 356)
  bill_history$chamber <- recode(
    toupper(bill_history$chamber),
    "A" = "House", "S" = "Senate", "G" = "Governor"
  )

  # Trim and lowercase actions
  bill_history$action <- str_trim(bill_history$action)
  bill_history$action <- tolower(bill_history$action)

  # Arrange
  bill_history <- bill_history %>%
    arrange(.data$bill_id, .data$order)

  bill_history
}

# Stage 3 Hook: Get missing SS bills
#' Get genuinely missing SS bills for New Jersey
#'
#' Standard: anti-join, filter to A/S, exclude committee-sponsored.
#'
#' Faithful port of old script lines 218-227.
#'
#' @param ss_filtered SS bills filtered to term and valid types
#' @param bill_details Filtered bill details
#' @param all_bill_details Unfiltered bill details
#' @return Dataframe of genuinely missing SS bills
get_missing_ss_bills <- function(ss_filtered, bill_details,
                                 all_bill_details) {
  missing <- ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term"))

  # Join to all_bill_details for sponsor info
  missing <- missing %>%
    left_join(
      all_bill_details %>%
        select("bill_id", "session", "primary_sponsors") %>%
        distinct(),
      by = c("bill_id", "term" = "session")
    )

  # Filter to A/S only
  missing <- missing %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    ) %>%
    filter(.data$bill_type %in% nj_config$bill_types) %>%
    select(-"bill_type")

  # Exclude committee-sponsored
  missing <- missing %>%
    filter(!grepl(
      "committee", .data$primary_sponsors,
      ignore.case = TRUE
    ))

  missing
}

# Stage 5 Hook: Derive unique sponsors
#' Derive unique sponsors from bills for New Jersey
#'
#' Chamber from bill_id prefix: A->H, S->S.
#' Cosponsorship via grep in primary_sponsors column.
#'
#' Faithful port of old script lines 472-480.
#'
#' @param bills Dataframe of bills with achievement columns
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregate stats
derive_unique_sponsors <- function(bills, term) {
  bills %>%
    mutate(
      chamber = ifelse(
        substring(.data$bill_id, 1, 1) == "A", "H", "S"
      )
    ) %>%
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

# Stage 5 Hook: Compute cosponsorship
#' Compute cosponsorship counts for New Jersey
#'
#' NJ cosponsorship is via the primary_sponsors column
#' (semicolon-separated). Note: bill_id prefix "A" maps to
#' chamber "H", "S" maps to chamber "S".
#'
#' Faithful port of old script lines 483-495.
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param bills Dataframe of bills with primary_sponsors column
#' @return Dataframe with num_cosponsored_bills added
compute_cosponsorship <- function(all_sponsors, bills) {
  all_sponsors$num_cosponsored_bills <- NA

  for (i in seq_len(nrow(all_sponsors))) {
    # NJ: A bills -> chamber H, S bills -> chamber S
    c_prefix <- ifelse(
      all_sponsors[i, ]$chamber == "H", "A", "S"
    )
    c_sub <- filter(
      bills, substring(.data$bill_id, 1, 1) == c_prefix
    )
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(
      all_sponsors[i, ]$LES_sponsor,
      tolower(c_sub$primary_sponsors)
    ))
    # Subtract own sponsored bills
    all_sponsors$num_cosponsored_bills[i] <-
      all_sponsors$num_cosponsored_bills[i] -
      all_sponsors$num_sponsored_bills[i]
  }

  if (min(all_sponsors$num_cosponsored_bills) < 0) {
    cli_warn("Negative cosponsor counts found")
    stop("Cosponsorship validation failed")
  }

  all_sponsors
}

# Stage 5 Hook: Clean sponsor names
#' Clean sponsor names for matching
#'
#' "Last, First" format. match_name_chamber = tolower(paste(
#' LES_sponsor, chamber_initial, sep="-")).
#'
#' Faithful port of old script line 518.
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

# Stage 5 Hook: Adjust legiscan data
#' Adjust legiscan data for New Jersey
#'
#' Filter committee_id==0. Build match_name with conditional
#' middle initial: "last, first" or "last, first M.".
#' match_name_chamber with district-derived chamber.
#'
#' Faithful port of old script lines 545-554.
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string
#' @return Adjusted legiscan with match_name_chamber column
adjust_legiscan_data <- function(legiscan, term) {
  result <- legiscan %>%
    filter(.data$committee_id == 0) %>%
    group_by(.data$last_name) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      match_name = ifelse(
        is.na(.data$middle_name) | .data$middle_name == "",
        paste(.data$last_name, .data$first_name, sep = ", "),
        paste0(
          .data$last_name, ", ", .data$first_name, " ",
          substr(.data$middle_name, 1, 1), "."
        )
      ),
      match_name_chamber = tolower(paste(
        .data$match_name,
        ifelse(
          substr(.data$district, 1, 1) == "S", "s", "h"
        ),
        sep = "-"
      ))
    )

  # Chamber switcher: Wimberly sponsors both A and S bills
  # Duplicate legiscan record for Senate side
  if (term == "2024_2025") {
    wimb <- result %>%
      filter(.data$last_name == "Wimberly") %>%
      mutate(
        district = gsub("^HD", "SD", .data$district),
        match_name_chamber = gsub("-h$", "-s", .data$match_name_chamber)
      )
    result <- bind_rows(result, wimb)
  }

  result %>%
    # Re-group and dedup (old script lines 552-554)
    group_by(.data$match_name) %>%
    mutate(n = n()) %>%
    ungroup()
}

# Stage 5 Hook: Reconcile legiscan with sponsors
#' Reconcile legiscan with sponsors
#'
#' Fuzzy join via inexact OSA. Custom matches will be built
#' iteratively after first run with legiscan data.
#'
#' @param sponsors Dataframe of unique sponsors
#' @param legiscan Adjusted legiscan dataframe
#' @param term Term string
#' @return Joined dataframe
# nolint start: line_length_linter
reconcile_legiscan_with_sponsors <- function(sponsors, legiscan,
                                             term) {
  if (term == "2022_2023") {
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "yustein, jacqueline s.-h" = NA_character_,
        "mccoy, tennille r.-h" = NA_character_,
        "marin, eliana p.-h" = "pintor marin, eliana-h",
        "smith, robert-s" = "smith, bob-s"
      )
    )
  } else if (term == "2024_2025") {
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "marin, eliana p.-h" = "pintor marin, eliana-h",
        "smith, robert-s" = "smith, bob-s"
      )
    )
  } else {
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

# Stage 7 Hook: Prepare bills for LES calculation
#' Derive chamber from bill_id prefix for LES scoring
#'
#' NJ: A prefix -> House, S prefix -> Senate.
#'
#' @param bills_prepared Bills dataframe
#' @return Bills with chamber column added
prepare_bills_for_les <- function(bills_prepared) {
  bills_prepared %>%
    mutate(
      chamber = ifelse(
        substring(.data$bill_id, 1, 1) == "A", "H", "S"
      )
    )
}

# Export config and functions
list(
  bill_types = nj_config$bill_types,
  even_year_start = nj_config$even_year_start,
  step_terms = nj_config$step_terms,
  get_bill_file_suffix = get_bill_file_suffix,
  preprocess_raw_data = preprocess_raw_data,
  clean_bill_details = clean_bill_details,
  clean_bill_history = clean_bill_history,
  get_missing_ss_bills = get_missing_ss_bills,
  derive_unique_sponsors = derive_unique_sponsors,
  compute_cosponsorship = compute_cosponsorship,
  clean_sponsor_names = clean_sponsor_names,
  adjust_legiscan_data = adjust_legiscan_data,
  reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
  prepare_bills_for_les = prepare_bills_for_les
)
