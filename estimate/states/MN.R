# Minnesota (MN) State Configuration
#
# MN has regular sessions where bills carry over within the term.
# Special sessions have restarting bill numbers.
# Single session file per term: e.g., MN_Bill_Details_2023_RS.csv
#
# Bill types: HF (House File), SF (Senate File)
# Bill type is derived from summary/description fields (complex regex)
# Sponsor from 'author' column (not 'sponsors')
#
# Name matching: sponsors use full name, legiscan uses last name
# (with first initial if duplicates in same chamber)

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

# Stage 1 Hook: Get bill file suffix
#' Get bill file suffix for MN
#'
#' MN bill files are named like MN_Bill_Details_2023_RS.csv
#' where the suffix is the session identifier.
#'
#' @param term Term string (e.g., "2023_2024")
#' @return File suffix string (e.g., "2023_RS")
get_bill_file_suffix <- function(term) {
  # For MN, the bill files use year + session type
  # 2023_2024 term -> 2023_RS (single regular session)
  term_start_year <- strsplit(term, "_")[[1]][1]
  paste0(term_start_year, "_RS")
}

# State configuration
# nolint start: line_length_linter
mn_config <- list(
  bill_types = c("HF", "SF"),

  # Step terms for evaluating bill history (from old script lines 387-398)
  step_terms = list(
    aic = c(
      "^comm rpt", "^committee report", "^committee rpt", "^comm report",
      "referred by chair"
    ),
    abc = c(
      "^comm rpt", "^committee report", "^committee rpt", "^comm report",
      "second reading", "third reading", "taken from table", "rules suspended",
      "^re-referred", "special order", "^amend", "point of order",
      "^general orders", "placed on calendar"
    ),
    pass = c(
      "third reading, passed", "third reading passed", "^bill was passed",
      "received from house", "received from senate", "^bill was repassed"
    ),
    law = c(
      "signed by gov", "governor.+approval", "effective date"
    )
  )
)
# nolint end: line_length_linter

# Stage 1.5 Hook: Preprocess raw data
#' Preprocess raw data for Minnesota
#'
#' Normalizes raw CSV data into standard schema.
#' MN uses "bill_number" in raw data, and session format uses underscore.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  # Rename bill_number -> bill_id
  bill_details <- bill_details %>%
    rename(bill_id = "bill_number") %>%
    mutate(bill_id = toupper(.data$bill_id))

  bill_history <- bill_history %>%
    rename(bill_id = "bill_number") %>%
    mutate(bill_id = toupper(.data$bill_id))

  # Convert session format: _RS -> -RS, _SS -> -SS (from old script line 129-130)
  bill_details <- bill_details %>%
    mutate(
      session = gsub("_RS", "-RS", .data$session),
      session = gsub("_SS", "-SS", .data$session),
      session = gsub("_S([0-9])", "-SS\\1", .data$session)
    )

  bill_history <- bill_history %>%
    mutate(
      session = gsub("_RS", "-RS", .data$session),
      session = gsub("_SS", "-SS", .data$session),
      session = gsub("_S([0-9])", "-SS\\1", .data$session)
    )

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

#' Derive bill type from summary/description
#'
#' MN derives bill_type from text fields, not bill_id prefix.
#' Ported from old script lines 143-155.
#'
#' @param summary Summary text
#' @param description Description text
#' @param bill_id Bill ID (for fallback)
#' @return Bill type string
derive_bill_type <- function(summary, description, bill_id) {
  summary_lower <- tolower(summary)
  desc_lower <- tolower(description)

  # Primary extraction from summary
  # nolint start: line_length_linter
  pattern <- "^a (bill|resolution|house resolution|house [a-z]+ resolution|senate resolution|senate [a-z]+ resolution|joint resolution|memorial resolution)|^conference committee report"
  # nolint end: line_length_linter

  bill_type <- gsub("^a ", "", str_extract(summary_lower, pattern))

  # If conference committee report, look deeper in summary
  if (!is.na(bill_type) && bill_type == "conference committee report") {
    inner_pattern <- "a (bill for an act|resolution|house resolution|house [a-z]+ resolution|senate resolution|senate [a-z]+ resolution|joint resolution|memorial resolution)"
    inner_match <- str_extract(summary_lower, inner_pattern)
    if (!is.na(inner_match)) {
      bill_type <- gsub("^a | for an act", "", inner_match)
    }
  }

  # Fallback to description if still NA
  if (is.na(bill_type)) {
    bill_type <- gsub("^a ", "", str_extract(desc_lower, pattern))
  }

  # Another fallback - first 45 chars of summary

  if (is.na(bill_type)) {
    bill_type <- gsub(
      "^a ", "",
      str_extract(substring(summary_lower, 1, 45), pattern)
    )
  }

  # Special case for senate resolutions
  if (is.na(bill_type)) {
    if (grepl("^a +senate res|^senate resolution|^a senate [a-z]+ res",
              desc_lower)) {
      bill_type <- "senate resolution"
    }
  }

  # Fallback based on bill_id prefix
  if (is.na(bill_type) && grepl("^SR|^HR", bill_id)) {
    bill_type <- "resolution"
  }

  # Default to bill
  if (is.na(bill_type)) {
    bill_type <- "bill"
  }

  # Remove house/senate prefix (line 154)
  bill_type <- gsub("house |senate ", "", bill_type)

  bill_type
}

# Stage 2 Hook: Clean bill details
#' Clean bill details for Minnesota
#'
#' MN-specific transformations:
#' - Derive bill_type from summary/description (not bill_id)
#' - Create LES_sponsor from author (lowercase, accent removal)
#' - Fill missing sponsors from coauthors
#' - Filter to valid bill types (bills only, not resolutions)
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

  # Derive bill_type from summary/description (not bill_id prefix)
  bill_details$bill_type <- mapply(
    derive_bill_type,
    bill_details$summary,
    bill_details$description,
    bill_details$bill_id
  )

  if (verbose) {
    cli_log("Bill type distribution:")
    print(table(bill_details$bill_type))
  }

  # Filter to bills only (drop resolutions) - line 159
  bill_details <- filter(bill_details, .data$bill_type == "bill")

  if (verbose) {
    cli_log(glue("After filtering to bills: {nrow(bill_details)} bills"))
  }

  # Standardize sponsor (author) - lowercase and remove accents (lines 173-186)
  bill_details$author <- tolower(bill_details$author)
  bill_details$author <- standardize_accents(bill_details$author)

  bill_details$coauthors <- tolower(bill_details$coauthors)
  bill_details$coauthors <- standardize_accents(bill_details$coauthors)

  # Check for "by request" bills (lines 189-191)
  if (any(grepl("\\(br\\)|by request| br$", bill_details$author))) {
    cli_warn("BY REQUEST bills found - review needed")
  }

  # Create LES_sponsor from author (line 196)
  bill_details$LES_sponsor <- bill_details$author

  # Fill in missing sponsors with first coauthor (lines 200-203)
  empty_sponsors <- bill_details$LES_sponsor == "" | is.na(bill_details$LES_sponsor)
  if (any(empty_sponsors)) {
    if (verbose) {
      cli_log(glue(
        "Filling {sum(empty_sponsors)} bills without sponsor using coauthors"
      ))
    }
    bill_details$LES_sponsor <- ifelse(
      empty_sponsors,
      gsub(";.+", "", bill_details$coauthors),
      bill_details$LES_sponsor
    )
  }

  # Drop committee-sponsored bills (lines 331-334)
  comm_bills <- bill_details %>%
    filter(grepl("committee", .data$author, ignore.case = TRUE))

  if (nrow(comm_bills) > 0) {
    if (verbose) {
      cli_log(glue("Dropping {nrow(comm_bills)} committee-sponsored bills"))
    }
    bill_details <- bill_details %>%
      filter(!grepl("committee", .data$author, ignore.case = TRUE))
  }

  # Drop bills with still-empty sponsor (lines 339-341)
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
#' Clean bill history for Minnesota
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  bill_history %>%
    mutate(
      term = term,
      # Rearrange + create order variable (from old script lines 371-375)
      action = tolower(.data$action)
    ) %>%
    arrange(.data$session, .data$bill_id, .data$action_date, .data$chamber_order) %>%
    group_by(.data$session, .data$bill_id) %>%
    mutate(order = row_number()) %>%
    ungroup()
}

# Stage 3 Hook: Transform SS bill IDs
#' Transform SS bill IDs for Minnesota
#'
#' PVS encodes special session bills with unusual IDs like HF10001, HF50005.
#' These represent special session bills where the first digit after bill type
#' indicates the special session number (e.g., HF10001 = session 1, bill 1).
#' This hook remaps them to standard format (e.g., HF0001).
#'
#' From old script lines 213-224.
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return SS bills with transformed bill IDs
transform_ss_bills <- function(ss_bills, term) {
  # Pattern: HFx000y or SFx000y where x is session number, y is bill number
  # Convert to HF000y or SF000y (remove the session digit prefix)
  # Regex: ([HS]F)(\d)(\d{4}) -> \1\3

  ss_bills %>%
    mutate(
      bill_id = gsub("^([HS]F)(\\d)(\\d{4})$", "\\1\\3", .data$bill_id)
    )
}

# Stage 3 Hook: Enrich SS bills with session
#' Add session information to SS bills
#'
#' MN bills carry over in regular sessions but restart in special sessions.
#' For regular sessions, we don't need session - join on bill_id and term.
#' For special sessions, we need to derive session from year.
#'
#' Based on old script logic (lines 247-287), MN uses year to handle
#' special session bills. For now, map year to session.
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return SS bills with session column added
enrich_ss_with_session <- function(ss_bills, term) {
  # MN regular session is carryover, so most SS bills don't need session
  # But we add it for consistency - map year to session
  # For 2023_2024 term: 2023 bills -> 2023-RS, 2024 bills -> 2023-RS
  # (bills carry over, so all go to the single RS)

  ss_bills %>%
    mutate(
      session = "2023-RS"  # MN 2023_2024 has single regular session
    )
}

# Stage 3 Hook: Get missing SS bills
#' Identify genuinely missing SS bills
#'
#' @param ss_filtered SS bills dataframe
#' @param bill_details Filtered bill_details dataframe
#' @param all_bill_details Unfiltered bill_details dataframe
#' @return Dataframe of genuinely missing SS bills
get_missing_ss_bills <- function(ss_filtered, bill_details, all_bill_details) {
  # Find SS bills missing from bill_details
  missing_ss <- ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term"))

  # Check if they exist in all_bill_details
  missing_with_info <- missing_ss %>%
    left_join(
      all_bill_details %>% select("bill_id", "author", "summary"),
      by = "bill_id"
    )

  # Filter to valid bill types (HF/SF) and non-committee sponsored
  keep_types <- mn_config$bill_types
  genuinely_missing <- missing_with_info %>%
    mutate(bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))) %>%
    filter(.data$bill_type %in% keep_types) %>%
    filter(!grepl("committee", .data$summary, ignore.case = TRUE)) %>%
    select("bill_id", "term")

  genuinely_missing
}

# Stage 5 Hook: Derive unique sponsors
#' Derive unique sponsors for Minnesota
#'
#' @param bills Bills dataframe with achievement metrics
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregated metrics
derive_unique_sponsors <- function(bills, term) {
  bills %>%
    mutate(
      # MN uses HF/SF, so H = House, S = Senate
      chamber = ifelse(substring(.data$bill_id, 1, 1) == "H", "H", "S")
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
#' Compute cosponsorship counts for Minnesota
#'
#' MN combines author + coauthors for cosponsorship matching.
#' From old script lines 534-549.
#'
#' @param all_sponsors Sponsors dataframe
#' @param bills Bills dataframe with coauthors column
#' @return Sponsors dataframe with num_cosponsored_bills added
compute_cosponsorship <- function(all_sponsors, bills) {
  # Initialize count column
  all_sponsors$num_cosponsored_bills <- NA

  # Create combined match column (author + coauthors)
  # From old script line 536
  bills$cospon_match <- tolower(paste(bills$author, bills$coauthors, sep = "; "))

  cli_log("Computing cosponsorship counts...")

  # Calculate cosponsorship counts
  for (i in seq_len(nrow(all_sponsors))) {
    # MN uses HF/SF, so H = House, S = Senate
    chamber_prefix <- ifelse(all_sponsors[i, ]$chamber == "H", "H", "S")
    c_sub <- filter(bills, substring(.data$bill_id, 1, 1) == chamber_prefix)
    all_sponsors$num_cosponsored_bills[i] <- sum(
      grepl(all_sponsors[i, ]$LES_sponsor, c_sub$cospon_match)
    )
    # Subtract primary sponsorship (from old script line 541)
    # "ONLY need to adjust this way if sponsored and cosponsored column are the same"
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] -
      all_sponsors$num_sponsored_bills[i]
  }

  # Validate no negative counts (from old script lines 547-549)
  if (min(all_sponsors$num_cosponsored_bills) < 0) {
    cli_warn("Negative cosponsor counts found - this indicates a problem")
    print(all_sponsors %>%
      filter(.data$num_cosponsored_bills < 0) %>%
      select("LES_sponsor", "chamber", "num_sponsored_bills", "num_cosponsored_bills"))
  } else {
    cli_log("Cosponsorship counts valid (no negatives)")
  }

  all_sponsors
}

# Stage 5 Hook: Clean sponsor names
#' Clean sponsor names for matching
#'
#' MN uses full name (LES_sponsor) for matching, not just last name.
#' From old script line 581.
#'
#' @param all_sponsors Sponsors dataframe
#' @param term Term string
#' @return Modified all_sponsors with match_name_chamber
clean_sponsor_names <- function(all_sponsors, term) {
  # Term-specific name fixes (from old script lines 560-582)
  if (term %in% c("2007_2008", "2009_2010")) {
    all_sponsors <- all_sponsors %>%
      mutate(
        LES_sponsor = case_when(
          .data$LES_sponsor == "torres ray" ~ "ray",
          .data$LES_sponsor == "erickson ropes" ~ "ropes",
          .data$LES_sponsor == "prettner solon" ~ "solon",
          TRUE ~ .data$LES_sponsor
        )
      )
  }
  if (term %in% c("2011_2012", "2013_2014", "2015_2016", "2017_2018")) {
    all_sponsors <- all_sponsors %>%
      mutate(
        LES_sponsor = ifelse(
          .data$LES_sponsor == "torres ray", "ray", .data$LES_sponsor
        )
      )
  }
  if (term == "2017_2018") {
    all_sponsors <- all_sponsors %>%
      mutate(
        LES_sponsor = ifelse(
          .data$LES_sponsor == "maye quade", "quade", .data$LES_sponsor
        )
      )
  }

  # Create match_name_chamber using full name (line 581)
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
#' Adjust legiscan data for Minnesota
#'
#' MN legiscan matching: if 2+ legislators share last_name in same role,
#' use "last_name first_initial", otherwise just last_name.
#' From old script lines 599-607.
#'
#' @param legiscan Legiscan dataframe
#' @param term Term string
#' @return Modified legiscan with match_name_chamber
adjust_legiscan_data <- function(legiscan, term) {
  # Term-specific adjustments
  if (term == "2019_2020") {
    # From old script lines 593-596
    legiscan <- bind_rows(
      legiscan %>% filter(!(.data$people_id %in% c(6423, 6466) &
                             .data$party == "I")),
      legiscan %>%
        filter(.data$people_id == 16555) %>%
        mutate(role = "Rep", district = "HD-011B")
    )
  }

  legiscan_adj <- legiscan %>%
    filter(.data$committee_id == 0) %>%
    group_by(.data$last_name, .data$role) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      # If 2+ legislators share last name in same role, add first initial
      # From old script lines 604-606
      match_name = case_when(
        .data$n >= 2 ~ paste0(.data$last_name, " ", substr(.data$first_name, 1, 1)),
        TRUE ~ .data$last_name
      ),
      match_name_chamber = tolower(paste(
        .data$match_name,
        substr(.data$district, 1, 1),
        sep = "-"
      ))
    )

  # Term-specific: Fix Anderson collision in 2023_2024

  # Both Paul H. Anderson and Patti Anderson have first names starting with "P"
  # so they get the same key "anderson p-h". Use full first name instead.
  if (term == "2023_2024") {
    legiscan_adj <- legiscan_adj %>%
      mutate(
        match_name_chamber = case_when(
          .data$people_id == 6381 ~ "anderson paul-h",   # Paul H. Anderson
          .data$people_id == 23742 ~ "anderson patti-h", # Patti Anderson
          TRUE ~ .data$match_name_chamber
        )
      )
  }

  legiscan_adj
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
  if (term == "2021_2022") {
    # From old script lines 629-655
    joined <- inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "hortman-h" = NA_character_,
        "drazkowski s-s" = NA_character_,
        "gruenhagen g-s" = NA_character_,
        "green s-s" = NA_character_,
        "lucero e-s" = NA_character_,
        "bahr c-s" = NA_character_,
        "morrison k-s" = NA_character_,
        "xiong t-s" = NA_character_,
        "boldon l-s" = NA_character_,
        "rasmusson j-s" = NA_character_,
        "hansen-h" = "hansen, r.-h",
        "hanson-h" = "hanson, j.-h"
      )
    )
  } else if (term == "2023_2024") {
    # Custom matches for 2023_2024 term
    # Confirmed by user lookup of example bills
    joined <- inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # Rick Hansen (HD-053B) -> sponsor "hansen, r."
        "hansen-h" = "hansen, r.-h",
        # Jessica Hanson (HD-055A) -> sponsor "hanson, j."
        "hanson-h" = "hanson, j.-h",
        # Liz Lee (HD-067A) -> sponsor "lee, k." (K for Liz is unusual but confirmed)
        "lee l-h" = "lee, k.-h",
        # Paul H. Anderson (HD-012A) -> sponsor "anderson, p. h."
        # Uses custom key set in adjust_legiscan_data
        "anderson paul-h" = "anderson, p. h.-h",
        # Patti Anderson (HD-033A) -> sponsor "anderson, p. e."
        "anderson patti-h" = "anderson, p. e.-h"
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

# Stage 7 Hook: Prepare bills for LES calculation
#' Prepare bills for LES calculation
#'
#' Derives chamber from bill_id prefix (HF = H, SF = S)
#'
#' @param bills_prepared Bills dataframe
#' @return Bills with chamber column added
prepare_bills_for_les <- function(bills_prepared) {
  bills_prepared %>%
    mutate(
      chamber = ifelse(substring(.data$bill_id, 1, 1) == "H", "H", "S")
    )
}

# Export configuration and hooks
c(
  mn_config,
  list(
    get_bill_file_suffix = get_bill_file_suffix,
    preprocess_raw_data = preprocess_raw_data,
    clean_bill_details = clean_bill_details,
    clean_bill_history = clean_bill_history,
    transform_ss_bills = transform_ss_bills,
    enrich_ss_with_session = enrich_ss_with_session,
    get_missing_ss_bills = get_missing_ss_bills,
    derive_unique_sponsors = derive_unique_sponsors,
    compute_cosponsorship = compute_cosponsorship,
    clean_sponsor_names = clean_sponsor_names,
    adjust_legiscan_data = adjust_legiscan_data,
    reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
    prepare_bills_for_les = prepare_bills_for_les
  )
)
