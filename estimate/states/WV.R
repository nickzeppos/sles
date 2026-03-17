# West Virginia (WV) State Configuration
#
# WV is an annual-session state with special sessions.
# Sessions: RS (Regular), SS1-SS7 (Special)
# Bill IDs restart across sessions and years.
# Sponsor format: last-name-only primary_sponsor
# Cosponsors: semicolon-separated in cosponsors column
# Has cosponsorship data
#
# Bill numbering per session type:
#   HB RS:  2001+ (2023), 4001+ (2024) - no overlap with specials
#   HB SS1: 0101-0199
#   HB SS2: 0201-0299
#   SB RS:  0001-0999
#   SB SS1: 1001-1999
#   SB SS2: 2001-2999

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
wv_config <- list(
  bill_types = c("HB", "SB"),

  # Step terms for evaluating bill history (from old script lines 457-470)
  step_terms = list(
    aic = c(
      "^reported (do|be|with|in)", "do pass", "^reported be adopted",
      "committee.+reported", "committee amendment", "^originating in"
    ),
    abc = c(
      "^reported", "(1st|2nd|3rd|first|second|third) reading",
      "read (1st|2nd|3rd)", "house calendar", "special calendar",
      "^effective", "roll no\\. [0-9]+", "voice vote",
      "suspension of", "unanimous consent"
    ),
    pass = c(
      "^passed", "communicated to (house|senate)",
      "ordered to (house|senate)", "to governor",
      "completed legislative action"
    ),
    law = c(
      "approved by governor", "chapter [0-9]+",
      "acts 19[0-9]+", "acts 20[0-9]+"
    )
  )
)
# nolint end: line_length_linter

# Stage 1 Hook: Load bill files
#' Load bill files for West Virginia
#'
#' WV has separate files per year. Loads and combines both years.
#' Pattern: KY.R
#'
#' @param bill_dir Path to bill directory
#' @param state State code ("WV")
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
#' Preprocess raw data for West Virginia
#'
#' - Rename bill_number -> bill_id
#' - Recode session_type: 1x->SS1, 2x->SS2, etc.
#' - Create session = paste0(session_year, "-", session_type)
#' - Set term. Distinct.
#' Source: old script lines 128-134.
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

  # Recode session_type
  recode_session <- function(st) {
    dplyr::recode(st,
      "1x" = "SS1", "2x" = "SS2", "3x" = "SS3",
      "4x" = "SS4", "5x" = "SS5", "6x" = "SS6", "7x" = "SS7"
    )
  }

  bill_details$session_type <- recode_session(bill_details$session_type)
  bill_details$session <- paste0(
    bill_details$session_year, "-", bill_details$session_type
  )
  bill_details$term <- term

  bill_history$session_type <- recode_session(bill_history$session_type)
  bill_history$session <- paste0(
    bill_history$session_year, "-", bill_history$session_type
  )
  bill_history$term <- term

  bill_details <- distinct(bill_details)
  bill_history <- distinct(bill_history)

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 2 Hook: Clean bill details
#' Clean bill details for West Virginia
#'
#' - Save all_bill_details before filtering
#' - Derive bill_type from bill_id prefix, filter to HB/SB
#' - Lowercase primary_sponsor and cosponsors
#' - Accent removal via standardize_accents
#' - Honorific removal (Mr. Speaker, Mr. President, etc.)
#' - Cosponsor cleanup: trailing parentheses -> semicolons
#' - Rename primary_sponsor -> LES_sponsor
#' - Drop empty/NA sponsors
#' Source: old script lines 138-399.
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @param verbose Show detailed logging (default TRUE)
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Store unfiltered version
  all_bill_details <- bill_details

  # Derive bill_type from bill_id prefix
  bill_details <- bill_details %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    )

  # Filter to HB/SB
  bill_details <- bill_details %>%
    filter(.data$bill_type %in% wv_config$bill_types) %>%
    select(-"bill_type")

  if (verbose) {
    cli_log(glue("After bill type filter: {nrow(bill_details)} bills"))
  }

  # Lowercase sponsors and cosponsors
  bill_details$primary_sponsor <- tolower(bill_details$primary_sponsor)
  bill_details$cosponsors <- tolower(bill_details$cosponsors)

  # Accent removal
  bill_details$primary_sponsor <- standardize_accents(
    bill_details$primary_sponsor
  )
  bill_details$cosponsors <- standardize_accents(bill_details$cosponsors)

  # Remove honorifics (from old script line 173)
  # nolint start: line_length_linter
  titles <- paste0(
    "\\((mr|ms|mrs). speaker\\)|\\((mr|ms|mrs). president\\)|",
    "^(mr|ms|mrs). speaker \\((mr|ms|mrs).|",
    "^(mr|ms|mrs). president \\((mr|ms|mrs).|",
    "\\)$|\\(acting president\\)|mr\\. speaker \\(mr\\. "
  )
  # nolint end: line_length_linter
  bill_details$primary_sponsor <- str_trim(
    gsub(titles, "", bill_details$primary_sponsor)
  )
  bill_details$cosponsors <- str_trim(
    gsub(titles, "", bill_details$cosponsors)
  )

  # Cosponsor cleanup: trailing parentheses -> semicolons
  # (from old script line 176)
  bill_details$cosponsors <- gsub(
    " \\);|\\);|\\)$", ";", bill_details$cosponsors
  )

  # In 2023, Wayne Clark was the only Clark in the House so sponsor
  # is just "clark". In 2024, Thomas Clark joined so Wayne became
  # "clark, w." Normalize the 2023 variant to match.
  if (term == "2023_2024") {
    bill_details <- bill_details %>%
      mutate(primary_sponsor = ifelse(
        .data$primary_sponsor == "clark" &
          .data$session_year == 2023,
        "clark, w.",
        .data$primary_sponsor
      ))
  }

  # Rename primary_sponsor -> LES_sponsor
  bill_details <- bill_details %>%
    rename(LES_sponsor = "primary_sponsor")

  # Check for by-request bills
  if (any(grepl("request", bill_details$LES_sponsor))) {
    cli_warn("BY REQUEST BILLS detected -- review needed")
  }

  # Drop empty/NA sponsors
  empty_sponsor_bills <- bill_details %>%
    filter(.data$LES_sponsor == "" | is.na(.data$LES_sponsor))

  if (nrow(empty_sponsor_bills) > 0 && verbose) {
    cli_log(glue(
      "Dropping {nrow(empty_sponsor_bills)} bills without sponsor"
    ))
  }

  bill_details <- bill_details %>%
    filter(!(.data$LES_sponsor == "" | is.na(.data$LES_sponsor)))

  if (verbose) {
    cli_log(glue("After cleaning: {nrow(bill_details)} bills"))
  }

  list(
    all_bill_details = all_bill_details,
    bill_details = bill_details
  )
}

# Stage 2 Hook: Clean bill history
#' Clean bill history for West Virginia
#'
#' - Recode chamber: H->House, S->Senate, G->Governor, CC->Conference
#' - Trim actions
#' - Arrange by order
#' Source: old script lines 443-493.
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  bill_history$chamber <- recode(
    bill_history$chamber,
    "H" = "House", "S" = "Senate",
    "G" = "Governor", "CC" = "Conference"
  )

  bill_history$action <- str_trim(bill_history$action)

  bill_history <- bill_history %>%
    arrange(.data$term, .data$session, .data$bill_id, .data$order)

  bill_history
}

# Stage 4 Hook: Post-evaluate bill
#' Post-evaluate bill for West Virginia
#'
#' If law==1 AND history contains "clerk's note...bill is null and void"
#' (case-insensitive), set law=0.
#' Source: old script lines 504-507.
#'
#' @param bill_stages Dataframe with one row of bill achievement
#' @param bill_row Dataframe row from bill_details for this bill
#' @param bill_history Dataframe of bill history for this bill
#' @return Modified bill_stages
post_evaluate_bill <- function(bill_stages, bill_row, bill_history) {
  if (bill_stages$law == 1 &&
    any(grepl(
      "clerk's note.+bill is null and void",
      tolower(bill_history$action)
    ))) {
    bill_stages$law <- 0
  }
  bill_stages
}

# Stage 3 Hook: Enrich SS bills with session
#' Enrich SS bills with session for West Virginia
#'
#' WV bill IDs restart across sessions and years. Assigns session to
#' each SS bill using PVS year + bill number pattern:
#'   SB 1001-1999 -> SS1
#'   SB 2001-2999 -> SS2
#'   HB 0101-0199 -> SS1
#'   HB 0201-0299 -> SS2
#'   Everything else -> RS
#' Falls back to review CSV for any edge cases.
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return SS bills with session column added
enrich_ss_with_session <- function(ss_bills, term) {
  ss_bills <- ss_bills %>%
    mutate(
      bill_num = as.integer(gsub("^[A-Z]+", "", .data$bill_id)),
      bill_prefix = gsub("[0-9]+", "", .data$bill_id),
      session_type = case_when(
        # Senate special sessions: identifiable by bill number range
        .data$bill_prefix == "SB" & .data$bill_num >= 2001 ~ "SS2",
        .data$bill_prefix == "SB" & .data$bill_num >= 1001 ~ "SS1",
        # House special sessions: identifiable by bill number range
        .data$bill_prefix == "HB" &
          .data$bill_num >= 201 & .data$bill_num <= 299 ~ "SS2",
        .data$bill_prefix == "HB" &
          .data$bill_num >= 101 & .data$bill_num <= 199 ~ "SS1",
        # Default to RS
        TRUE ~ "RS"
      ),
      session = paste0(.data$year, "-", .data$session_type)
    ) %>%
    select(-"bill_num", -"bill_prefix", -"session_type")

  # Override with review CSV for any ambiguous bills
  review_path <- file.path(
    repo_root, ".data", "WV", "review",
    "WV_SS_session_resolution.csv"
  )
  if (file.exists(review_path)) {
    review <- read.csv(review_path, stringsAsFactors = FALSE) %>%
      filter(.data$match == "Y") %>%
      select("bill_id", "pvs_title", "candidate_session")

    if (nrow(review) > 0) {
      n_resolved <- 0
      for (i in seq_len(nrow(ss_bills))) {
        resolved <- review %>%
          filter(
            .data$bill_id == ss_bills$bill_id[i],
            .data$pvs_title == ss_bills$Title[i]
          )
        if (nrow(resolved) == 1) {
          ss_bills$session[i] <- resolved$candidate_session
          n_resolved <- n_resolved + 1
        }
      }
      if (n_resolved > 0) {
        cli_log(glue(
          "  {n_resolved} SS bill(s) resolved via review CSV"
        ))
      }
    }
  }

  ss_bills
}

# Stage 5 Hook: Derive unique sponsors
#' Derive unique sponsors for West Virginia
#'
#' Standard aggregation (num_sponsored, pass_rate, law_rate) PLUS
#' adds cosponsor-only legislators from cosponsors field.
#' Source: old script lines 567-588.
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

  # Add cosponsor-only legislators (from old script lines 577-588)
  unique_cospon <- str_trim(unique(unlist(str_split(
    bills$cosponsors, "; "
  ))))
  for (nonspon in unique_cospon) {
    if (is.na(nonspon) || nonspon == "") next
    if (nonspon %in% all_sponsors$LES_sponsor) next

    chamb <- unique(substring(
      bills[grepl(nonspon, bills$cosponsors, fixed = TRUE), ]$bill_id,
      1, 1
    ))
    chamb <- ifelse(chamb == "H", "H", "S")
    if (length(chamb) == 1) {
      all_sponsors <- tibble::add_row(
        all_sponsors,
        LES_sponsor = nonspon,
        chamber = chamb,
        term = term,
        num_sponsored_bills = 0,
        sponsor_pass_rate = 0,
        sponsor_law_rate = 0
      )
    } else if ("H" %in% chamb && "S" %in% chamb) {
      cli_warn(glue(
        "CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS"
      ))
    }
  }

  all_sponsors
}

# Stage 5 Hook: Compute cosponsorship
#' Compute cosponsorship for West Virginia
#'
#' For each sponsor, count same-chamber bill occurrences via grepl
#' in cospon_match (LES_sponsor + cosponsors), subtract
#' num_sponsored_bills. Validate >= 0.
#' Source: old script lines 590-605. Pattern: WA.R.
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param bills Dataframe of bills with cosponsors column
#' @return Dataframe of sponsors with num_cosponsored_bills added
compute_cosponsorship <- function(all_sponsors, bills) {
  # Build cosponsor match column (from old script line 592)
  # Note: primary_sponsor was renamed to LES_sponsor, so
  # bills$primary_sponsor is NULL and effectively omitted from paste
  bills$cospon_match <- paste(
    bills$LES_sponsor, bills$cosponsors,
    sep = "; "
  )

  all_sponsors$num_cosponsored_bills <- NA_integer_
  for (i in seq_len(nrow(all_sponsors))) {
    c_sub <- bills[substring(bills$bill_id, 1, 1) %in%
      ifelse(all_sponsors[i, ]$chamber == "H", "H", "S"), ]
    all_sponsors$num_cosponsored_bills[i] <- sum(
      grepl(all_sponsors[i, ]$LES_sponsor, tolower(c_sub$cospon_match))
    )
    # Subtract own sponsored bills
    all_sponsors$num_cosponsored_bills[i] <-
      all_sponsors$num_cosponsored_bills[i] -
        all_sponsors$num_sponsored_bills[i]
  }

  # Validate non-negative
  if (min(all_sponsors$num_cosponsored_bills) < 0) {
    cli_error("Negative cosponsorship counts detected")
    stop("Cosponsorship validation failed")
  } else {
    cli_log("Cosponsorship counts valid (all non-negative)")
  }

  all_sponsors
}

# Stage 5 Hook: Clean sponsor names
#' Clean sponsor names for matching
#'
#' Creates match_name_chamber = tolower(paste(LES_sponsor,
#' chamber_initial, sep="-")). Removes any quoted substrings first.
#' Source: old script line 641.
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
        str_remove_all(.data$LES_sponsor, '"\\s*.*?\\s*"'),
        substr(.data$chamber, 1, 1),
        sep = "-"
      ))
    )
}

# Stage 5 Hook: Adjust legiscan data
#' Adjust legiscan data for West Virginia
#'
#' Filter committee_id==0. Group by last_name + substr(district,1,1).
#' If n>=2, use "last_name, first_initial." format.
#' Final key: tolower(paste(match_name, substr(district,1,1), sep="-")).
#' Source: old script lines 657-663.
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string
#' @return Adjusted legiscan with match_name_chamber column
adjust_legiscan_data <- function(legiscan, term) {
  legiscan %>%
    filter(.data$committee_id == 0) %>%
    # Filter role/district mismatches (e.g., Bill Hamilton listed as
    # Rep with SD-011 in 2023 1st SS — he's actually a Senator)
    filter(
      !(.data$role == "Rep" & grepl("^SD", .data$district)),
      !(.data$role == "Sen" & grepl("^HD", .data$district))
    ) %>%
    group_by(.data$last_name, substr(.data$district, 1, 1)) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      match_name = ifelse(
        .data$n >= 2,
        glue("{last_name}, {substr(first_name, 1, 1)}."),
        .data$last_name
      ),
      match_name_chamber = tolower(paste(
        .data$match_name,
        substr(.data$district, 1, 1),
        sep = "-"
      ))
    )
}

# Stage 5 Hook: Reconcile legiscan with sponsors
#' Reconcile legiscan with sponsors using fuzzy matching
#'
#' Uses inexact_join with OSA method, full outer join.
#' No custom_match entries initially for 2023_2024 - will add
#' after reviewing first run output.
#' Source: old script lines 671-700.
#'
#' @param sponsors Dataframe of unique sponsors
#' @param legiscan Adjusted legiscan dataframe
#' @param term Term string
#' @return Joined dataframe
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
        # Browning (HD-028) has 0 bills — block from matching "brooks"
        "browning-h" = NA_character_,
        # Roop (HD-044) has 0 bills — block from matching "ross"
        "roop-h" = NA_character_,
        # Elliott Pritt goes by first name "D." in legiscan first_name
        # but sponsors bills as "pritt, e."
        "pritt, d.-h" = "pritt, e.-h"
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
#' Prepare bills for LES calculation
#'
#' Derives chamber from bill_id prefix (H/S).
#' Source: old script line 733.
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
  wv_config,
  list(
    load_bill_files = load_bill_files,
    preprocess_raw_data = preprocess_raw_data,
    clean_bill_details = clean_bill_details,
    clean_bill_history = clean_bill_history,
    post_evaluate_bill = post_evaluate_bill,
    enrich_ss_with_session = enrich_ss_with_session,
    derive_unique_sponsors = derive_unique_sponsors,
    compute_cosponsorship = compute_cosponsorship,
    clean_sponsor_names = clean_sponsor_names,
    adjust_legiscan_data = adjust_legiscan_data,
    reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
    prepare_bills_for_les = prepare_bills_for_les
  )
)
