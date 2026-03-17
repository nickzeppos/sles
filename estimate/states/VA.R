# Virginia (VA) State Configuration
#
# VA is an odd-year legislature with carryover bills.
# Sessions: regular + special (I, II, III, etc.)
# Bill types: HB, SB
# Sponsor format: full name (first middle last), not last-name-only
# Has cosponsorship data via senate_sponsors/house_sponsors columns
# Carryover dedup: for RS bills, keep only most recent session_year
# per (bill_id, LES_sponsor). Keep all SS bills.
# Single-chamber passed correction in post_evaluate_bill.
# SS bill_id encoding: e.g., "HB90009" -> "HB0009"
#
# Source: .dropbox/old_estimate_scripts/VA - Estimate LES AV.R

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
standardize_accents <- strings$standardize_accents

# Load required libraries
library(dplyr)
library(stringr)
library(glue)

# nolint start: line_length_linter
va_config <- list(
  bill_types = c("HB", "SB"),
  even_year_start = TRUE,

  # Step terms from old script lines 474-484
  step_terms = list(
    aic = c(
      "assigned to .+ sub-comm", "assigned.+ sub:",
      "reported from",
      "subcommittee recomm", "subcommittee failed to",
      "tabled in", "failed to report",
      "^continued to \\d{4} in",
      "^passed by .+ in",
      "defeated in committee:", "stricken from docket by",
      "letter sent to"
    ),
    abc = c(
      "^reported from", "read second time", "read third time",
      "engrossed", "vote:",
      "passed house", "passed senate",
      "defeated by house", "defeated by senate"
    ),
    pass = c(
      "passed house", "passed senate",
      "agreed to by house", "agreed to by senate",
      "vote: passage", "vote: block vote passage",
      "vote: adoption", "vote: block vote adoption",
      "signed by speaker", "signed by president"
    ),
    law = c("^approved by gov", "^acts of assembly chapter text ")
  )
)
# nolint end: line_length_linter

# Stage 1 Hook: Bill file suffix
#' VA uses full term for file naming (VA_Bill_Details_2024_2025.csv).
#'
#' @param term Term string (e.g., "2024_2025")
#' @return The suffix to use for bill file names
get_bill_file_suffix <- function(term) {
  term
}

# Stage 1.5 Hook: Preprocess raw data
#' Preprocess raw data for Virginia
#'
#' Normalizes raw CSV data: converts term dash->underscore,
#' derives session from session column ("YYYY SESSION" -> "YYYY-RS",
#' "YYYY SPECIAL SESSION I" -> "YYYY-SS1", etc.)
#'
#' Faithful port of old script lines 134-141, 444-451.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  # --- Bill details ---
  bill_details$term <- gsub("-", "_", bill_details$term)

  bill_details <- bill_details %>%
    mutate(
      session_type = substring(.data$session, 6, nchar(.data$session)),
      session_type = recode(.data$session_type,
        "SESSION" = "RS",
        "SPECIAL SESSION I" = "SS1",
        "SPECIAL SESSION II" = "SS2",
        "SPECIAL SESSION III" = "SS3",
        "SPECIAL SESSION IV" = "SS4"
      ),
      session = paste0(.data$session_year, "-", .data$session_type)
    ) %>%
    select(-"session_type")

  bill_details <- distinct(bill_details) %>%
    arrange(.data$term, .data$session, .data$bill_id)

  # --- Bill history ---
  bill_history$term <- gsub("-", "_", bill_history$term)

  bill_history <- bill_history %>%
    mutate(
      session_type = substring(.data$session, 6, nchar(.data$session)),
      session_type = recode(.data$session_type,
        "SESSION" = "RS",
        "SPECIAL SESSION I" = "SS1",
        "SPECIAL SESSION II" = "SS2",
        "SPECIAL SESSION III" = "SS3",
        "SPECIAL SESSION IV" = "SS4"
      ),
      session = paste0(.data$session_year, "-", .data$session_type)
    ) %>%
    select(-"session_type")

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 2 Hook: Clean bill details
#' Clean bill details for Virginia
#'
#' VA-specific transformations:
#' - Filter to HB/SB
#' - Sponsor cleaning: remove "(chief patron)", lowercase, strip
#'   second chief patrons/semicolons, resigned/retired annotations,
#'   standardize_accents, remove periods/commas/nicknames
#' - Fill missing sponsors from senate/house_sponsors
#' - Carryover dedup: for RS bills, keep most recent session_year
#' - Drop empty sponsors
#'
#' Faithful port of old script lines 147-309.
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string
#' @param verbose Show detailed logging
#' @return List with all_bill_details and filtered bill_details
# nolint start: line_length_linter
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Derive bill_type, save all_bill_details before filtering
  bill_details <- bill_details %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    )
  all_bill_details <- bill_details

  # Filter to HB/SB
  bill_details <- bill_details %>%
    filter(.data$bill_type %in% va_config$bill_types) %>%
    select(-"bill_type")

  if (verbose) {
    cli_log(glue("Bills after type filter: {nrow(bill_details)}"))
  }

  # Sponsor cleaning (old script lines 156-165)
  bill_details$sponsor <- str_trim(gsub(
    "\\(chief patron\\)", "", tolower(bill_details$sponsor)
  ))
  bill_details$sponsor <- str_trim(gsub(
    "\\\n.+| \\(.+\\)$|;.+|-resigned.+|-seat vac.+|-retired.+",
    "", bill_details$sponsor
  ))

  # Standardize accents on sponsor and senate/house_sponsors
  bill_details$sponsor <- standardize_accents(bill_details$sponsor)
  bill_details$sponsor <- gsub("\\.|,", "", bill_details$sponsor)

  bill_details$senate_sponsors <- standardize_accents(
    tolower(gsub("\\.|,", "", bill_details$senate_sponsors))
  )
  bill_details$house_sponsors <- standardize_accents(
    tolower(gsub("\\.|,", "", bill_details$house_sponsors))
  )

  # Eliminate nicknames (old script lines 182-184)
  bill_details$sponsor <- gsub(
    "  +", " ", gsub('\\".+\\"|\\(.+\\)', "", bill_details$sponsor)
  )
  bill_details$senate_sponsors <- gsub(
    "  +", " ",
    gsub('\\".+\\"|\\(.+\\)', "", bill_details$senate_sponsors)
  )
  bill_details$house_sponsors <- gsub(
    "  +", " ",
    gsub('\\".+\\"|\\(.+\\)', "", bill_details$house_sponsors)
  )

  # LES_sponsor (old script lines 263-264)
  bill_details$LES_sponsor <- gsub(";$", "", bill_details$sponsor)

  # Name variants: normalize two-way variants in bill data
  bill_details$LES_sponsor[
    bill_details$LES_sponsor == "jd diggs"
  ] <- "jd danny diggs"
  bill_details$LES_sponsor[
    bill_details$LES_sponsor == "timmy f french"
  ] <- "timmy french"

  # Fill missing sponsors (old script lines 267-272)
  if (nrow(filter(
    bill_details,
    .data$LES_sponsor == "" | is.na(.data$LES_sponsor)
  )) > 0) {
    bill_details$LES_sponsor <- ifelse(
      is.na(bill_details$LES_sponsor) &
        grepl("^S", bill_details$bill_id) &
        !grepl(";", bill_details$senate_sponsors),
      tolower(bill_details$senate_sponsors),
      bill_details$LES_sponsor
    )
    bill_details$LES_sponsor <- ifelse(
      bill_details$LES_sponsor == "" &
        grepl("^S", bill_details$bill_id) &
        !grepl(";", bill_details$senate_sponsors),
      tolower(bill_details$senate_sponsors),
      bill_details$LES_sponsor
    )
    bill_details$LES_sponsor <- ifelse(
      is.na(bill_details$LES_sponsor) &
        grepl("^H", bill_details$bill_id) &
        !grepl(";", bill_details$house_sponsors),
      tolower(bill_details$house_sponsors),
      bill_details$LES_sponsor
    )
    bill_details$LES_sponsor <- ifelse(
      bill_details$LES_sponsor == "" &
        grepl("^H", bill_details$bill_id) &
        !grepl(";", bill_details$house_sponsors),
      tolower(bill_details$house_sponsors),
      bill_details$LES_sponsor
    )
  }

  # Drop raw sponsor column to avoid conflict with LES_sponsor
  # rename in calculate_scores (same pattern as MA)
  bill_details <- bill_details %>% select(-"sponsor")

  # Carryover dedup (old script lines 307-309)
  # For RS bills, keep only most recent session_year per
  # (bill_id, LES_sponsor). Keep all SS bills.
  rs_bills <- bill_details %>%
    filter(grepl("RS", .data$session)) %>%
    arrange(desc(.data$session), .data$bill_id) %>%
    distinct(.data$bill_id, .data$LES_sponsor, .keep_all = TRUE)
  bill_details <- bill_details %>%
    filter(grepl("SS", .data$session)) %>%
    bind_rows(rs_bills) %>%
    arrange(.data$session, .data$bill_id)

  # Drop empty sponsors (old script lines 294-296)
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
#' Clean bill history for Virginia
#'
#' Derives session from raw session column (same as preprocess).
#' Lowercases actions. Rewrites "defeated by" actions that are not
#' "defeated by house|senate" to "defeated in committee: ...".
#' Arranges by session, bill_id, order.
#'
#' Faithful port of old script lines 444-455.
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  # Lowercase and trim actions
  bill_history$action <- str_trim(bill_history$action)
  bill_history$action <- tolower(bill_history$action)

  # Rewrite "defeated by" (old script lines 457-460)
  bill_history$action <- ifelse(
    grepl("defeated by ", bill_history$action) &
      !grepl("defeated by (house|senate)", bill_history$action),
    gsub(
      "defeated by ",
      "defeated in committee: ",
      bill_history$action
    ),
    bill_history$action
  )

  # Arrange
  bill_history <- bill_history %>%
    arrange(.data$session, .data$bill_id, .data$order)

  bill_history
}

# Stage 3 Hook: Transform SS bills
#' Transform SS bills for Virginia
#'
#' Handles encoded session digits in bill IDs, e.g.,
#' "HB90009" -> "HB0009". Includes term-specific fixes
#' from old script lines 322-330.
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return Transformed SS bills
transform_ss_bills <- function(ss_bills, term) {
  if (term == "2020_2021") {
    ss_bills$bill_id[ss_bills$bill_id == "HB90009"] <- "HB0009"
    ss_bills$bill_id[ss_bills$bill_id == "HB10001"] <- "HB0001"
    ss_bills$bill_id[ss_bills$bill_id == "SB10001"] <- "SB0001"
  }
  if (term == "2022_2023") {
    ss_bills$bill_id[ss_bills$bill_id == "HB40004"] <- "HB0004"
    ss_bills$bill_id[ss_bills$bill_id == "SB40004"] <- "SB0004"
  }

  ss_bills
}

# Stage 3 Hook: Get missing SS bills
#' Get genuinely missing SS bills for Virginia
#'
#' Standard: anti-join, filter to HB/SB, exclude committee-sponsored.
#'
#' Faithful port of old script lines 337-346.
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
      all_bill_details %>% select("bill_id", "term", "sponsor"),
      by = c("bill_id", "term")
    )

  # Filter to HB/SB only
  missing <- missing %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    ) %>%
    filter(.data$bill_type %in% va_config$bill_types) %>%
    select(-"bill_type")

  # Exclude committee-sponsored and bills not in scraped data
  # (NA sponsor = either not scraped or empty sponsor in raw data)
  missing <- missing %>%
    filter(
      !is.na(.data$sponsor),
      !grepl("committee", .data$sponsor, ignore.case = TRUE)
    )

  missing
}

# Stage 4 Hook: Post-evaluate bill
#' Post-evaluate bill hook for Virginia
#'
#' Single-chamber passed correction: if passed_chamber==1 and
#' law==0 and no "communicated to (house|senate)" in history
#' and only one chamber in history -> passed_chamber=0.
#'
#' Faithful port of old script lines 516-520.
#'
#' @param bill_stages Dataframe row with bill achievement
#' @param bill_row Original bill details row
#' @param bill_history Bill history for this bill
#' @return Modified bill_stages
post_evaluate_bill <- function(bill_stages, bill_row,
                               bill_history) {
  if (bill_stages$passed_chamber == 1 &&
    bill_stages$law == 0) {
    has_communicated <- any(grepl(
      "communicated to (house|senate)",
      bill_history$action
    ))
    multi_chamber <- length(unique(bill_history$chamber)) > 1
    if (!has_communicated && !multi_chamber) {
      bill_stages$passed_chamber <- 0
    }
  }

  bill_stages
}

# Stage 5 Hook: Derive unique sponsors
#' Derive unique sponsors from bills for Virginia
#'
#' Chamber from bill_id prefix (H/S). Cosponsorship via grep
#' in combined LES_sponsor + senate_sponsors + house_sponsors.
#'
#' Faithful port of old script lines 595-617.
#'
#' @param bills Dataframe of bills with achievement columns
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregate stats
derive_unique_sponsors <- function(bills, term) {
  # Primary sponsors
  all_sponsors <- bills %>%
    mutate(
      chamber = ifelse(
        substring(.data$bill_id, 1, 1) == "H", "H", "S"
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

  all_sponsors
}

# Stage 5 Hook: Compute cosponsorship
#' Compute cosponsorship counts for Virginia
#'
#' VA uses combined cospon_match from LES_sponsor +
#' senate_sponsors + house_sponsors. Subtracts own bills.
#'
#' Faithful port of old script lines 606-616.
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param bills Dataframe of bills
#' @return Dataframe with num_cosponsored_bills added
compute_cosponsorship <- function(all_sponsors, bills) {
  all_sponsors$num_cosponsored_bills <- NA

  bills$cospon_match <- paste(
    bills$LES_sponsor, bills$senate_sponsors,
    bills$house_sponsors,
    sep = "; "
  )
  bills$cospon_match <- gsub("; na", "", bills$cospon_match)

  for (i in seq_len(nrow(all_sponsors))) {
    c_initial <- substring(all_sponsors[i, ]$chamber, 1, 1)
    c_sub <- filter(
      bills, substring(.data$bill_id, 1, 1) == c_initial
    )
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(
      all_sponsors[i, ]$LES_sponsor,
      tolower(c_sub$cospon_match)
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
#' Full-name matching: match_name_chamber = tolower(paste(
#' LES_sponsor, chamber_initial, sep="-")).
#'
#' Faithful port of old script line 643.
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
#' Adjust legiscan data for Virginia
#'
#' Full-name matching using first_name + middle_initial +
#' last_name. Filter committee_id==0.
#'
#' Faithful port of old script lines 674-676.
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string
#' @return Adjusted legiscan with match_name_chamber column
adjust_legiscan_data <- function(legiscan, term) {
  # Chamber switcher: Srinivasan served in House (HD-026) in 2024,
  # elected to Senate (SD-032) Jan 2025
  if (term == "2024_2025") {
    srinivasan <- legiscan %>%
      filter(.data$people_id == 25019)
    if (nrow(srinivasan) > 0) {
      legiscan <- bind_rows(
        legiscan,
        srinivasan %>% mutate(role = "Sen", district = "SD-032")
      )
    }
  }

  legiscan %>%
    filter(.data$committee_id == 0) %>%
    mutate(
      middle_init = ifelse(
        is.na(.data$middle_name) | .data$middle_name == "",
        "", substr(.data$middle_name, 1, 1)
      ),
      match_name_chamber = tolower(paste0(
        .data$first_name, " ",
        .data$middle_init, " ",
        .data$last_name, "-",
        substr(.data$district, 1, 1)
      ))
    )
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
        "c. todd  gilbert-h" = NA_character_,
        "jonathan  arnold-h" = NA_character_,
        "taylor m mason-s" = "t montgomery mason-s",
        "c.e.  hayes-h" = "ce cliff hayes jr-h",
        "lamont  bagby-s" = NA_character_
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
        "c.e.  hayes-h" = "ce cliff hayes jr-h",
        "j.j.  singh-h" = "jj singh-h",
        "luther  cifers-s" = "luther cifers iii-s",
        "don l scott-h" = "don scott-h",
        "j.d.  diggs-s" = "jd danny diggs-s",
        "timmy f french-s" = "timmy french-s",
        "kannan  srinivasan-s" = "kannan srinivasan-s"
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
#' @param bills_prepared Bills dataframe
#' @return Bills with chamber column added
prepare_bills_for_les <- function(bills_prepared) {
  bills_prepared %>%
    mutate(
      chamber = ifelse(
        substring(.data$bill_id, 1, 1) == "H", "H", "S"
      )
    )
}

# Export config and functions
list(
  bill_types = va_config$bill_types,
  even_year_start = va_config$even_year_start,
  step_terms = va_config$step_terms,
  get_bill_file_suffix = get_bill_file_suffix,
  preprocess_raw_data = preprocess_raw_data,
  clean_bill_details = clean_bill_details,
  clean_bill_history = clean_bill_history,
  transform_ss_bills = transform_ss_bills,
  get_missing_ss_bills = get_missing_ss_bills,
  post_evaluate_bill = post_evaluate_bill,
  derive_unique_sponsors = derive_unique_sponsors,
  compute_cosponsorship = compute_cosponsorship,
  clean_sponsor_names = clean_sponsor_names,
  adjust_legiscan_data = adjust_legiscan_data,
  reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
  prepare_bills_for_les = prepare_bills_for_les
)
