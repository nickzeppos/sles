# New Hampshire (NH) State Configuration
#
# NH is a carryover state — bill IDs carry over across the biennium.
# Special session bills have SS prefix or "SPECIAL SESSION BILL" in
# title. No special sessions in 2023_2024.
# Largest legislature: ~417 House + 24 Senate = ~441 legislators.
# Sponsor format: semicolon-delimited with party in parens.
# Name matching uses full names (LES_sponsor + chamber).
# No cosponsorship data (pipeline default sets to NA).
#
# Source of truth: .dropbox/old_estimate_scripts/NH - Estimate LES AV.R

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
nh_config <- list(
  bill_types = c("HB", "SB"),

  # Step terms for evaluating bill history
  # NH Abbreviations: gencourt.state.nh.us/bill_status/docket_abbrev.htm
  # Source: old script lines 431-440
  step_terms = list(
    aic = c(
      "^hearing", "^joint hearing", "maj rep", "min rep",
      "committee report", "comm am",
      "prop [a-z]+ am .+ hc [0-9]", "otp", "itl",
      "report rnf", "report re-ref", "subcom",
      "ought to pass", "inexpedient to legislate"
    ),
    abc = c(
      "committee report", "maj rep", "otp", "report re-ref",
      " aa ", "^aa ", "am vv", "fl am", "floor am"
    ),
    pass = c("passed", "enrolled", "ot3rdg"),
    law = c(
      "signed by gov", "chap\\.[0-9]+", "signed by the gov",
      "chapter [0-9]+", "chap: [0-9]+"
    )
  )
)
# nolint end: line_length_linter

# Stage 1 Hook: Load bill files
#' Load bill files for New Hampshire
#'
#' NH has separate files per year. Loads and combines both years.
#' Pattern: WV.R / KY.R / WY.R.
#'
#' @param bill_dir Path to bill directory
#' @param state State code ("NH")
#' @param term Term in format "YYYY_YYYY"
#' @param verbose Show detailed logging (default TRUE)
#' @return List with bill_details and bill_history dataframes
load_bill_files <- function(bill_dir, state, term, verbose = TRUE) {
  years <- strsplit(term, "_")[[1]]
  year1 <- years[1]
  year2 <- years[2]

  all_files <- list.files(bill_dir, full.names = TRUE)

  detail_files <- all_files[
    grepl(glue("{state}_Bill_Details"), all_files) &
      (grepl(year1, all_files) | grepl(year2, all_files))
  ]

  if (length(detail_files) == 0) {
    cli_warn(glue("No bill detail files found for term {term}"))
    return(NULL)
  }

  if (verbose) {
    cli_log(glue(
      "Found {length(detail_files)} bill detail files for {term}"
    ))
  }

  bill_details <- bind_rows(lapply(detail_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read.csv(f, stringsAsFactors = FALSE)
  }))

  history_files <- all_files[
    grepl(glue("{state}_Bill_Histories"), all_files) &
      (grepl(year1, all_files) | grepl(year2, all_files))
  ]

  if (verbose) {
    cli_log(glue(
      "Found {length(history_files)} bill history files for {term}"
    ))
  }

  bill_history <- bind_rows(lapply(history_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read.csv(f, stringsAsFactors = FALSE)
  }))

  bill_details <- distinct(bill_details)
  bill_history <- distinct(bill_history)

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 1.5 Hook: Preprocess raw data
#' Preprocess raw data for New Hampshire
#'
#' - Rename bill_number -> bill_id
#' - Standardize bill_id: strip suffixes (-FN, -A, -L, trailing
#'   uppercase), pad numeric portion to 4 digits
#' - Set term
#' Source: old script lines 137-139.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  standardize_bill_id <- function(bid) {
    # Strip suffixes: "-FN", "-A", "-L", trailing uppercase letters
    bid <- gsub("-.+$|[A-Z]+$", "", bid)
    # Extract prefix and numeric portion, pad to 4 digits
    prefix <- gsub("[0-9]+", "", bid)
    number <- gsub("[A-Z]+", "", bid)
    paste0(prefix, str_pad(number, 4, pad = "0"))
  }

  bill_details <- bill_details %>%
    rename(bill_id = "bill_number") %>%
    mutate(
      bill_id = standardize_bill_id(.data$bill_id),
      term = term
    ) %>%
    distinct()

  bill_history <- bill_history %>%
    rename(bill_id = "bill_number") %>%
    mutate(
      bill_id = standardize_bill_id(.data$bill_id),
      term = term
    ) %>%
    distinct()

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 2 Hook: Clean bill details
#' Clean bill details for New Hampshire
#'
#' 1. Store all_bill_details before filtering
#' 2. Derive session from session_year + SS detection
#' 3. Strip "SS" prefix from bill_id after session derivation
#' 4. Filter to bill_types (HB, SB)
#' 5. Lowercase + standardize_accents on sponsors
#' 6. Remove parentheticals: (aa...) and (#...)
#' 7. Extract LES_sponsor (first sponsor before ";"), strip party
#' 8. Clean chapter_num: convert "None" -> NA
#' 9. Drop committee-sponsored bills
#' 10. Drop empty/NA sponsors
#' Source: old script lines 141-379.
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @param verbose Show detailed logging (default TRUE)
#' @return List with all_bill_details and filtered bill_details
# nolint start: line_length_linter
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Store unfiltered version (before any filtering)
  all_bill_details <- bill_details

  # Derive session: SS prefix or "SPECIAL SESSION" in title
  # Source: old script line 145
  bill_details <- bill_details %>%
    mutate(
      session = ifelse(
        grepl("^SS", .data$bill_id) |
          grepl("SPECIAL SESSION", .data$title, ignore.case = TRUE),
        paste0(.data$session_year, "-SS"),
        paste0(.data$session_year, "-RS")
      )
    )

  # Strip SS prefix from bill_id after session derivation
  # Source: old script line 146
  bill_details$bill_id <- gsub("^SS", "", bill_details$bill_id)

  # Derive bill_type and filter to HB/SB
  # Source: old script lines 156-158
  bill_details <- bill_details %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    ) %>%
    filter(.data$bill_type %in% nh_config$bill_types) %>%
    select(-"bill_type")

  if (verbose) {
    cli_log(glue("After bill type filter: {nrow(bill_details)} bills"))
  }

  # Lowercase and standardize accents on sponsors
  # Source: old script lines 163-169
  bill_details$sponsors <- tolower(bill_details$sponsors)
  bill_details$sponsors <- standardize_accents(bill_details$sponsors)

  # Remove unnecessary parentheticals: (aa...) and (#...)
  # Source: old script line 171
  bill_details$sponsors <- gsub(
    " \\([a-z][a-z][^\\)]+\\)| \\(\\#[^\\)]+\\)", "",
    bill_details$sponsors
  )
  bill_details$sponsors <- gsub("  +", " ", bill_details$sponsors)

  # Extract LES_sponsor: first sponsor before semicolon, strip party
  # Source: old script lines 222-224
  bill_details$LES_sponsor <- gsub(";.+", "", bill_details$sponsors)
  bill_details$LES_sponsor <- gsub(" \\(.+", "", bill_details$LES_sponsor)
  bill_details$LES_sponsor <- str_trim(bill_details$LES_sponsor)

  # Clean chapter_num: convert "None" -> NA
  # Source: old script line 461
  bill_details$chapter_num <- ifelse(
    tolower(bill_details$chapter_num) == "none",
    NA,
    bill_details$chapter_num
  )

  # Check for "by request" bills
  if (any(grepl("request", bill_details$LES_sponsor))) {
    cli_warn("BY REQUEST bills detected -- review needed")
  }

  # Drop committee-sponsored bills
  # Source: old script lines 366-369
  is_committee <- grepl("committee", bill_details$LES_sponsor)
  if (sum(is_committee) > 0 && verbose) {
    cli_log(glue(
      "Dropping {sum(is_committee)} committee-sponsored bills"
    ))
  }
  bill_details <- bill_details[!is_committee, ]

  # Drop empty/NA sponsors
  # Source: old script lines 375-379
  bill_details$LES_sponsor[is.na(bill_details$LES_sponsor)] <- ""
  empty <- bill_details$LES_sponsor == ""
  if (sum(empty) > 0 && verbose) {
    cli_log(glue(
      "Dropping {sum(empty)} bills without sponsor"
    ))
  }
  bill_details <- bill_details[!empty, ]

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
#' Clean bill history for New Hampshire
#'
#' - Derive session from SS prefix in bill_id
#' - Strip SS prefix from bill_id
#' - Recode chamber: H->House, S->Senate, G->Governor, CC->Conference
#' - Lowercase + trim action
#' - Arrange by order
#' Source: old script lines 411-421.
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  bill_history <- bill_history %>%
    mutate(
      session = ifelse(
        grepl("^SS", .data$bill_id),
        paste0(.data$session_year, "-SS"),
        paste0(.data$session_year, "-RS")
      ),
      # Strip SS prefix after session derivation
      bill_id = gsub("^SS", "", .data$bill_id),
      # Recode chamber
      chamber = dplyr::recode(
        .data$chamber,
        "H" = "House", "S" = "Senate",
        "G" = "Governor", "CC" = "Conference"
      ),
      action = tolower(str_trim(.data$action))
    ) %>%
    arrange(.data$session, .data$bill_id, .data$order)

  bill_history
}

# Stage 4 Hook: Post-evaluate bill
#' Post-evaluate bill for New Hampshire
#'
#' Three adjustments from old script:
#' 1. Single-chamber passed correction (lines 471-474): If
#'    passed_chamber==1 AND bill history has only one unique chamber
#'    AND law!=1, set passed_chamber=0
#' 2. Chapter number fallback (lines 476-478): If law==0 AND
#'    chapter_num is populated (not NA), set abc=pass=law=1
#' 3. Floor date fallback (lines 480-482): If abc==0 AND bill has
#'    floor_date in its chamber, set abc=1
#'
#' @param bill_stages Dataframe with one row of bill achievement
#' @param bill_row Dataframe row from bill_details for this bill
#' @param bill_history Dataframe of bill history for this bill
#' @return Modified bill_stages
post_evaluate_bill <- function(bill_stages, bill_row, bill_history) {
  # 1. Single-chamber passed correction
  # Source: old script lines 471-474
  if (bill_stages$passed_chamber == 1 &&
    length(unique(bill_history$chamber)) == 1 &&
    bill_stages$law != 1) {
    bill_stages$passed_chamber <- 0
  }

  # 2. Chapter number fallback
  # Source: old script lines 476-478
  ch <- bill_row$chapter_num
  if (bill_stages$law == 0 && !is.na(ch) && ch != "") {
    bill_stages$action_beyond_comm <- 1
    bill_stages$passed_chamber <- 1
    bill_stages$law <- 1
  }

  # 3. Floor date fallback
  # Source: old script lines 480-482
  if (bill_stages$action_beyond_comm == 0) {
    is_senate <- grepl("^S", bill_row$bill_id)
    has_floor <- if (is_senate) {
      !is.na(bill_row$S_floor_date) && bill_row$S_floor_date != ""
    } else {
      !is.na(bill_row$H_floor_date) && bill_row$H_floor_date != ""
    }
    if (has_floor) {
      bill_stages$action_beyond_comm <- 1
    }
  }

  bill_stages
}

# Stage 3 Hook: Transform SS bills
#' Transform SS bills for New Hampshire
#'
#' Handle encoded session prefixes from prior terms:
#' e.g., "HB20002" -> "HB0002" (session digit embedded in bill number).
#' Pattern: letter prefix + single session digit + 4-digit bill number.
#' Source: old script lines 252-262.
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return Transformed SS bills
transform_ss_bills <- function(ss_bills, term) {
  ss_bills %>%
    mutate(
      bill_id = ifelse(
        grepl("^([HS]B)(\\d)(\\d{4})$", .data$bill_id),
        gsub("^([HS]B)(\\d)(\\d{4})$", "\\1\\3", .data$bill_id),
        .data$bill_id
      )
    )
}

# Stage 3 Hook: Get missing SS bills
#' Identify genuinely missing SS bills vs committee-sponsored
#'
#' NH has committee-sponsored bills that are intentionally excluded.
#' SS bills whose sponsors are committees are not genuinely missing.
#' Also filter out non-HB/SB bill types (e.g., CACR).
#' Source: old script lines 269-278.
#'
#' @param ss_filtered Filtered SS bills for this term
#' @param bill_details Cleaned bill details (after filtering)
#' @param all_bill_details All bill details (before filtering)
#' @return Dataframe of genuinely missing SS bills (bill_id, term)
get_missing_ss_bills <- function(ss_filtered, bill_details,
                                 all_bill_details) {
  missing <- ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term"))

  # Check if they exist in all_bill_details (before type filtering)
  missing_with_info <- missing %>%
    left_join(
      all_bill_details %>%
        select("bill_id", "term", "title") %>%
        distinct(),
      by = c("bill_id", "term")
    )

  # Filter to valid bill types only
  missing_with_info <- missing_with_info %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    ) %>%
    filter(.data$bill_type %in% nh_config$bill_types) %>%
    select(-"bill_type")

  # Filter out committee-sponsored bills
  genuinely_missing <- missing_with_info %>%
    filter(!grepl("committee", .data$title, ignore.case = TRUE)) %>%
    select("bill_id", "term")

  genuinely_missing
}

# Stage 5 Hook: Derive unique sponsors
#' Derive unique sponsors for New Hampshire
#'
#' Standard aggregation from bills. Then extract cosponsor-only
#' legislators from sponsors field (split by "; ", strip party).
#' Default all cosponsor-only to House (417/441 are House).
#' No cosponsorship computation — pipeline default sets
#' num_cosponsored_bills = NA.
#' Source: old script lines 548-579.
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

  # Add cosponsor-only legislators from sponsors field
  # Source: old script lines 560-579
  unique_cospon <- str_trim(unique(unlist(str_split(
    bills$sponsors, "; "
  ))))
  unique_cospon <- gsub(" \\(.+", "", unique_cospon)

  for (nonspon in unique_cospon) {
    if (is.na(nonspon) || nonspon == "") next
    if (nonspon %in% all_sponsors$LES_sponsor) next

    # Default to House (400 reps vs 24 senators)
    # Source: old script line 568 comment + line 576
    all_sponsors <- tibble::add_row(
      all_sponsors,
      LES_sponsor = nonspon,
      chamber = "H",
      term = term,
      num_sponsored_bills = 0,
      sponsor_pass_rate = 0,
      sponsor_law_rate = 0
    )
  }

  all_sponsors
}

# Stage 5 Hook: Clean sponsor names
#' Clean sponsor names for matching
#'
#' Creates match_name_chamber = tolower(paste(LES_sponsor,
#' chamber_initial, sep="-")). Full name matching (not last-name-only).
#' Source: old script line 679.
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
#' Adjust legiscan data for New Hampshire
#'
#' 1. Filter committee_id == 0
#' 2. Use legiscan name field directly as match_name (full name)
#' 3. Build match_name_chamber from name + district initial
#' 4. Deduplicate by match_name_chamber
#' Source: old script lines 696-702.
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string
#' @return Adjusted legiscan with match_name_chamber column
adjust_legiscan_data <- function(legiscan, term) {
  legiscan %>%
    filter(.data$committee_id == 0) %>%
    mutate(
      match_name = .data$name,
      match_name_chamber = tolower(paste(
        .data$match_name,
        substr(.data$district, 1, 1),
        sep = "-"
      ))
    ) %>%
    distinct(.data$match_name_chamber, .keep_all = TRUE)
}

# Stage 5 Hook: Reconcile legiscan with sponsors
#' Reconcile legiscan with sponsors using fuzzy matching
#'
#' Uses inexact_join with OSA method, full outer join.
#' Start with no custom_match entries — build iteratively.
#' Source: old script lines 710-761.
#'
#' @param sponsors Dataframe of unique sponsors
#' @param legiscan Adjusted legiscan dataframe
#' @param term Term string
#' @return Joined dataframe
# nolint start: line_length_linter
reconcile_legiscan_with_sponsors <- function(sponsors, legiscan,
                                             term) {
  if (term == "2023_2024") {
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # Name variants (legiscan -> sponsor)
        "becky whitley-s" = "rebecca whitley-s",
        "bob healey-h" = "robert healey-h",
        "candace moulton-h" = "candace gibbons-h",
        "deborah hobson-h" = "deb hobson-h",
        "leonard turcotte-h" = "len turcotte-h",
        "mary wallner-h" = "mary jane wallner-h",
        "riche colcombe-h" = "richa(c) colcombe-h",
        "roderick ladd-h" = "rick ladd-h",
        "stephen shurtleff-h" = "steve shurtleff-h",
        "william darby-h" = "will darby-h",
        "william gannon-s" = "bill gannon-s",
        # Zero-LES legiscan records (block from fuzzy matching)
        "aboul khan-h" = NA_character_,
        "alisson turcotte-h" = NA_character_,
        "ben bartlett-h" = NA_character_,
        "beth richards-h" = NA_character_,
        "bill conlin-h" = NA_character_,
        "bruce tatro-h" = NA_character_,
        "cathy kenny-h" = NA_character_,
        "cecilia rich-h" = NA_character_,
        "christopher herbert-h" = NA_character_,
        "clifford newton-h" = NA_character_,
        "daniel fitzpatrick-h" = NA_character_,
        "david fracht-h" = NA_character_,
        "david huot-h" = NA_character_,
        "dennis green-h" = NA_character_,
        "erik johnson-h" = NA_character_,
        "fred davis-h" = NA_character_,
        "gail pare-h" = NA_character_,
        "geoff smith-h" = NA_character_,
        "harry bean-h" = NA_character_,
        "henry noel-h" = NA_character_,
        "james connor-h" = NA_character_,
        "james mason-h" = NA_character_,
        "jeffrey rich-h" = NA_character_,
        "jennifer mandelbaum-h" = NA_character_,
        "jim qualey-h" = NA_character_,
        "john leavitt-h" = NA_character_,
        "judi lanza-h" = NA_character_,
        "juliet smith-h" = NA_character_,
        "kenneth vincent-h" = NA_character_,
        "louis juris-h" = NA_character_,
        "michael murphy-h" = NA_character_,
        "michael pedersen-h" = NA_character_,
        "molly howard-h" = NA_character_,
        "nicole leapley-h" = NA_character_,
        "paige beauchemin-h" = NA_character_,
        "paul tudor-h" = NA_character_,
        "peter lovett-h" = NA_character_,
        "richard beaudoin-h" = NA_character_,
        "robert menear-h" = NA_character_,
        "russell dumais-h" = NA_character_,
        "sean durkin-h" = NA_character_,
        "stephen boyd-h" = NA_character_,
        "stephen kennedy-h" = NA_character_,
        "susan treleaven-h" = NA_character_,
        "thomas kaczynski-h" = NA_character_,
        "thomas southworth-h" = NA_character_,
        "william dolan-h" = NA_character_,
        "william hatch-h" = NA_character_
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
#' NH bills use "H" prefix for House and "S" for Senate.
#' The generic fallback assumes Wisconsin's "A" prefix convention,
#' which would classify all NH bills as Senate.
#'
#' @param bills_prepared Dataframe of bills ready for LES calculation
#' @return Dataframe with chamber column set correctly
prepare_bills_for_les <- function(bills_prepared) {
  bills_prepared %>%
    mutate(
      chamber = ifelse(
        substring(.data$bill_id, 1, 1) == "H", "H", "S"
      )
    )
}

# Export configuration and hooks
c(
  nh_config,
  list(
    load_bill_files = load_bill_files,
    preprocess_raw_data = preprocess_raw_data,
    clean_bill_details = clean_bill_details,
    clean_bill_history = clean_bill_history,
    post_evaluate_bill = post_evaluate_bill,
    transform_ss_bills = transform_ss_bills,
    get_missing_ss_bills = get_missing_ss_bills,
    derive_unique_sponsors = derive_unique_sponsors,
    clean_sponsor_names = clean_sponsor_names,
    adjust_legiscan_data = adjust_legiscan_data,
    reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
    prepare_bills_for_les = prepare_bills_for_les
  )
)
