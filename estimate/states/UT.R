# Utah (UT) State Configuration
#
# UT notes from old script:
# - Sessions distinct year to year; HB/SB numbers start at 1 each year
# - Special sessions: SS1 = HB/SB1001+, SS2 = HB/SB2001+, etc.
# - Special session numbering is sequential across biennium
# - Bill IDs formatted as "H.B. 1" in raw data, standardized to "HB0001"
# - Sponsor format: "Rep. Lastname, Firstname M." or "Sen. ..."
# - No cosponsorship data (commented out in old script)
# - LRGC (Legislative Research General Counsel) as pseudo-chamber
# - Some bills distributed but not formally introduced on floor
# - Line item vetoes coded as law
# - Committee-sponsored bills permitted

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
library(readr)

# nolint start: line_length_linter
ut_config <- list(
  bill_types = c("HB", "SB"),

  # Step terms for evaluating bill history (from old script lines 411-421)
  step_terms = list(
    aic = c(
      "^(house|senate) comm.+(report|rpt)",
      "^(house|senate) comm.+motion",
      "^(house|senate) comm.+(recommend|favorable|on consent)",
      "^(house|senate) comm.+amendment recommendation"
    ),
    abc = c(
      "^(house|senate) comm.+(report|rpt)",
      "read 2nd", "read 3rd",
      "placed on (2nd|3rd)", "read 2nd \\& 3rd",
      "2nd reading", "3rd reading",
      "pass (2nd|3rd)", "passed (2nd|3rd)",
      "placed on.+calendar", "floor amendment",
      "^(house|senate) amended", "^(house|senate) substitute"
    ),
    pass = c(
      "^(house|senate) passed 3rd",
      "^(house|senate) pass 3rd",
      "^(house|senate) pass 2nd \\& 3rd",
      "^house.+to senate", "^senate.+to house"
    ),
    law = c(
      "governor signed",
      "became law w.+ governor signature",
      "governor line item veto"
    )
  )
)
# nolint end: line_length_linter

#' Standardize UT bill_id from "H.B. 1" format to "HB0001"
#'
#' @param bill_id Raw bill_id string(s)
#' @return Standardized bill_id string(s)
standardize_ut_bill_id <- function(bill_id) {
  paste0(
    gsub("\\.|( .+)", "", bill_id),
    str_pad(gsub(".+[A-Z]\\. ", "", bill_id), 4, pad = "0")
  )
}

# ============================================================
# Stage 1: Load bill files
# ============================================================
#' Load bill files for Utah
#'
#' UT has separate files per session (2023-RS, 2023-S1, 2024-RS, etc.).
#'
#' @param bill_dir Path to bill directory
#' @param state State code ("UT")
#' @param term Term in format "YYYY_YYYY"
#' @param verbose Show detailed logging (default TRUE)
#' @return List with bill_details and bill_history dataframes
load_bill_files <- function(bill_dir, state, term, verbose = TRUE) {
  years <- strsplit(term, "_")[[1]]
  term_start_year <- years[1]
  term_end_year <- years[2]

  all_files <- list.files(bill_dir, full.names = TRUE)
  detail_files <- all_files[
    grepl(glue("{state}_Bill_Details"), all_files) &
      (grepl(term_start_year, all_files) |
        grepl(term_end_year, all_files))
  ]
  history_files <- all_files[
    grepl(glue("{state}_Bill_Histories"), all_files) &
      (grepl(term_start_year, all_files) |
        grepl(term_end_year, all_files))
  ]

  if (length(detail_files) == 0) {
    cli_warn(glue("No bill detail files found for term {term}"))
    return(NULL)
  }

  if (verbose) {
    cli_log(glue(
      "Found {length(detail_files)} bill detail files"
    ))
  }

  bill_details <- bind_rows(lapply(detail_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read_csv(f, show_col_types = FALSE)
  }))

  if (verbose) {
    cli_log(glue(
      "Found {length(history_files)} bill history files"
    ))
  }

  bill_history <- bind_rows(lapply(history_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read_csv(f, show_col_types = FALSE)
  }))

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# ============================================================
# Stage 2: Clean bill details
# ============================================================
#' Clean bill details for Utah
#'
#' From old script lines 155-337:
#' - Standardize bill_id ("H.B. 1" -> "HB0001")
#' - Filter to HB/SB types
#' - Strip "Rep. " / "Sen. " prefix from sponsor
#' - Drop committee-sponsored and empty-sponsor bills
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @param verbose Show detailed logging (default TRUE)
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Standardize bill_id BEFORE saving all_bill_details
  # (old script standardizes first, then saves all_bills)
  bill_details <- bill_details %>%
    mutate(
      term = term,
      bill_id = standardize_ut_bill_id(.data$bill_id)
    ) %>%
    arrange(.data$session, .data$bill_id)

  # Derive bill_type from standardized bill_id
  bill_details <- bill_details %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    )

  # Store unfiltered version (with standardized bill_ids)
  all_bill_details <- bill_details

  if (verbose) {
    cli_log("Bill type distribution:")
    print(table(bill_details$bill_type))
  }

  # Filter to valid bill types
  bill_details <- bill_details %>%
    filter(.data$bill_type %in% ut_config$bill_types)

  # Clean whitespace/newlines from primary_sponsor
  # (some CSVs have multi-line fields, e.g., SB0136 in 2023-RS)
  bill_details$primary_sponsor <- gsub(
    "\\n|\\r", "", bill_details$primary_sponsor
  )
  bill_details$primary_sponsor <- str_trim(bill_details$primary_sponsor)

  # Lowercase and standardize accents in primary_sponsor
  bill_details$primary_sponsor <- tolower(bill_details$primary_sponsor)
  bill_details$primary_sponsor <- standardize_accents(
    bill_details$primary_sponsor
  )

  # Derive LES_sponsor: strip "rep. " and "sen. " prefix
  # (from old script line 192)
  bill_details$LES_sponsor <- gsub(
    "^rep\\. |^sen\\. ", "", bill_details$primary_sponsor
  )
  bill_details$LES_sponsor <- str_trim(bill_details$LES_sponsor)

  # Normalize sponsor name variants
  # Carol Spackman Moss appears as both "moss, carol spackman" (2023)
  # and "moss, carol s." (2024); legiscan has "moss, carol s."
  bill_details$LES_sponsor <- gsub(
    "^moss, carol spackman$", "moss, carol s.",
    bill_details$LES_sponsor
  )

  # Drop committee-sponsored bills (from old script lines 325-329)
  comm_bills <- bill_details %>%
    filter(grepl("committee", .data$LES_sponsor))
  if (nrow(comm_bills) > 0) {
    if (verbose) {
      cli_log(glue(
        "Dropping {nrow(comm_bills)} committee-sponsored bills"
      ))
    }
    bill_details <- bill_details %>%
      filter(!grepl("committee", .data$LES_sponsor))
  }

  # Drop bills with missing/empty sponsor (from old script lines 333-337)
  empty_bills <- bill_details %>%
    filter(.data$LES_sponsor == "" | is.na(.data$LES_sponsor))
  if (nrow(empty_bills) > 0) {
    if (verbose) {
      cli_log(glue(
        "Dropping {nrow(empty_bills)} bills without sponsor"
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

# ============================================================
# Stage 2: Clean bill history
# ============================================================
#' Clean bill history for Utah
#'
#' From old script lines 375-401:
#' - Standardize bill_id
#' - Fill chamber from location field
#' - Remove slashes from action text
#' - Recode chamber abbreviations
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  # Standardize bill_id and add term
  bill_history <- bill_history %>%
    mutate(
      term = term,
      bill_id = standardize_ut_bill_id(.data$bill_id)
    )

  # Order by session, bill_id, action_date, order
  bill_history <- bill_history %>%
    arrange(
      .data$term, .data$session, .data$bill_id,
      .data$action_date, .data$order
    )

  # Fill missing chamber from location (from old script lines 385-391)
  bill_history <- bill_history %>%
    mutate(
      chamber = ifelse(.data$chamber == "", NA, .data$chamber),
      # LRGC locations
      chamber = ifelse(
        is.na(.data$chamber) &
          (.data$location %in% c("LRGC", "LRGCEN") |
            grepl("^Legislative Research", .data$location)),
        "LRGC",
        .data$chamber
      ),
      # First entry: derive from bill_id prefix
      chamber = ifelse(
        is.na(.data$chamber) & .data$order == 1,
        substring(.data$bill_id, 1, 1),
        .data$chamber
      ),
      # House locations
      chamber = ifelse(
        is.na(.data$chamber) &
          (.data$location %in% c("HCLERK", "HSEC") |
            grepl("^House", .data$location)),
        "House",
        .data$chamber
      ),
      # Senate locations
      chamber = ifelse(
        is.na(.data$chamber) &
          (.data$location %in% c("SCLERK", "SSEC") |
            grepl("^Senate", .data$location)),
        "Senate",
        .data$chamber
      ),
      # Executive locations
      chamber = ifelse(
        is.na(.data$chamber) &
          (.data$location %in% c("LTGOV") |
            grepl("^Executive", .data$location)),
        "Executive",
        .data$chamber
      )
    )

  # Recode single-letter chambers (from old script line 393)
  bill_history$chamber <- dplyr::recode(
    bill_history$chamber,
    "H" = "House", "S" = "Senate",
    "G" = "Governor", "CC" = "Conference"
  )

  # Forward-fill chamber within each bill
  bill_history <- bill_history %>%
    group_by(.data$session, .data$bill_id) %>%
    tidyr::fill("chamber") %>%
    ungroup()

  # Remove slashes from action text (from old script lines 399-401)
  bill_history$action <- gsub(
    "^House \\/|^House\\/", "House ", bill_history$action
  )
  bill_history$action <- gsub(
    "^Senate \\/|^Senate\\/", "Senate ", bill_history$action
  )
  bill_history$action <- gsub("  +", " ", bill_history$action)

  # Lowercase and trim actions
  bill_history$action <- tolower(str_trim(bill_history$action))

  bill_history
}

# ============================================================
# Stage 3: Enrich SS with session
# ============================================================
#' Add session information to SS bills
#'
#' UT bill numbers indicate session within a year:
#' - Regular session: bill numbers < 1000 -> YYYY-RS
#' - Special session N: bill numbers N*1000+ -> YYYY-SN
#' (from old script lines 8-10: SS1=1001+, SS2=2001+, etc.)
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return SS bills with session column added
enrich_ss_with_session <- function(ss_bills, term) {
  ss_bills %>%
    mutate(
      bill_num = as.integer(gsub("^[A-Z]+", "", .data$bill_id)),
      session = ifelse(
        .data$bill_num >= 1000,
        paste0(.data$year, "-S", floor(.data$bill_num / 1000)),
        paste0(.data$year, "-RS")
      )
    ) %>%
    select(-"bill_num")
}

# ============================================================
# Stage 3: Get missing SS bills
# ============================================================
#' Identify genuinely missing SS bills
#'
#' Filters out committee-sponsored missing SS bills.
#' UT all_bill_details has standardized bill_ids (standardized before
#' saving), so default semi_join works.
#'
#' @param ss_filtered SS bills after filtering
#' @param bill_details Filtered bill_details
#' @param all_bill_details Unfiltered bill_details
#' @return Dataframe of genuinely missing SS bills
get_missing_ss_bills <- function(ss_filtered, bill_details,
                                 all_bill_details) {
  # Find SS bills not in filtered bill_details
  missing_ss <- ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term"))

  # Check if they exist in all_bill_details (already standardized)
  missing_in_data <- missing_ss %>%
    semi_join(all_bill_details, by = "bill_id")

  # Join to get sponsor info and filter out committee-sponsored
  # (from old script lines 222-230)
  missing_with_info <- missing_in_data %>%
    left_join(
      all_bill_details %>%
        select("bill_id", "primary_sponsor") %>%
        distinct(),
      by = "bill_id"
    )

  genuinely_missing <- missing_with_info %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    ) %>%
    filter(.data$bill_type %in% ut_config$bill_types) %>%
    filter(
      !grepl("committee", .data$primary_sponsor, ignore.case = TRUE)
    ) %>%
    select("bill_id", "term")

  genuinely_missing
}

# ============================================================
# Stage 4: Post-evaluate bill (achievement overrides)
# ============================================================
#' UT-specific achievement overrides
#'
#' From old script lines 462-469:
#' 1. Line item veto: if law=0 but chapter_num exists or last_action
#'    indicates governor signed/line item veto, set all stages to 1
#' 2. Passed chamber: if passed_chamber=0 but history shows transfer
#'    to governor or other body, set abc and pass to 1
#'
#' @param bill_stages Achievement result from evaluate_bill_history
#' @param bill_row The bill row from bill_details
#' @param bill_history Bill history dataframe for this bill
#' @return Modified bill_stages
post_evaluate_bill <- function(bill_stages, bill_row, bill_history) {
  # 1. Line item veto / passed without detection
  # (from old script lines 463-465)
  if (bill_stages$law == 0 &&
    (!is.na(bill_row$chapter_num) ||
      grepl(
        "Governor Signed|Became Law|Governor Line Item Veto",
        bill_row$last_action
      ))) {
    bill_stages$action_beyond_comm <- 1
    bill_stages$passed_chamber <- 1
    bill_stages$law <- 1
  }

  # 2. Passed chamber verification
  # (from old script lines 467-469)
  if (bill_stages$passed_chamber == 0 &&
    any(grepl(
      "to governor|to lieutenant gov|received from.+(house|senate)",
      bill_history$action
    ))) {
    bill_stages$action_beyond_comm <- 1
    bill_stages$passed_chamber <- 1
  }

  bill_stages
}

# ============================================================
# Stage 5: Derive unique sponsors
# ============================================================
#' Derive unique sponsors for Utah
#'
#' No cosponsorship data in UT (commented out in old script).
#'
#' @param bills Dataframe of bills with achievement columns
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregate stats
derive_unique_sponsors <- function(bills, term) {
  bills %>%
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
      num_cosponsored_bills = NA_real_,
      .groups = "drop"
    )
}

# ============================================================
# Stage 5: Clean sponsor names
# ============================================================
#' Clean sponsor names for matching
#'
#' UT uses full LES_sponsor + chamber initial for matching.
#' (from old script lines 602-608)
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

# ============================================================
# Stage 5: Adjust legiscan data
# ============================================================
#' Adjust legiscan data for matching
#'
#' UT uses "last_name, first_name middle_initial." format for all
#' legislators regardless of duplicate last names.
#' (from old script lines 625-631)
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string
#' @return Adjusted legiscan dataframe with match_name_chamber column
adjust_legiscan_data <- function(legiscan, term) {
  legiscan %>%
    filter(.data$committee_id == 0) %>%
    mutate(
      # Handle NA middle_name (read_csv reads blanks as NA)
      middle_init = ifelse(
        is.na(.data$middle_name) | .data$middle_name == "",
        "",
        substr(.data$middle_name, 1, 1)
      ),
      match_name = paste0(
        .data$last_name, ", ", .data$first_name, " ",
        .data$middle_init, "."
      ),
      match_name_chamber = tolower(paste(
        .data$match_name,
        substr(.data$district, 1, 1),
        sep = "-"
      ))
    )
}

# ============================================================
# Stage 5: Reconcile legiscan with sponsors
# ============================================================
#' Reconcile legiscan with sponsors using fuzzy matching
#'
#' Custom match entries will be added after first run identifies
#' mismatches.
#'
#' @param sponsors Sponsors dataframe
#' @param legiscan Adjusted legiscan dataframe
#' @param term Term string
#' @return Joined dataframe
reconcile_legiscan_with_sponsors <- function(sponsors, legiscan, term) {
  if (term == "2023_2024") {
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # Karen Mayne: resigned Jan 2023, zero-bill senator; prevent
        # spurious fuzzy match to Karen Kwan
        "mayne, karen .-s" = NA_character_
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

# ============================================================
# Stage 7: Prepare bills for LES calculation
# ============================================================
#' Prepare bills for LES calculation
#'
#' Derives chamber from bill_id prefix (H/S).
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

# Export configuration and hooks
c(
  ut_config,
  list(
    load_bill_files = load_bill_files,
    clean_bill_details = clean_bill_details,
    clean_bill_history = clean_bill_history,
    enrich_ss_with_session = enrich_ss_with_session,
    get_missing_ss_bills = get_missing_ss_bills,
    post_evaluate_bill = post_evaluate_bill,
    derive_unique_sponsors = derive_unique_sponsors,
    clean_sponsor_names = clean_sponsor_names,
    adjust_legiscan_data = adjust_legiscan_data,
    reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
    prepare_bills_for_les = prepare_bills_for_les
  )
)
