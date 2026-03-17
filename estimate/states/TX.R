# Texas State Configuration
# State-specific constants and settings for LES estimation
#
# TX notes from old script:
# - Multiple sessions per term: regular (88R) + special (881-884)
# - Authors/Coauthors = introducing chamber sponsors
# - Legislature number: s_num = 0.5 * (start_year - 2019) + 86
# - Session types: R->RS, 1->SS1, 2->SS2, etc.
# - Committee status overrides for achievement coding
# - Two-pass legiscan deduplication on last_name
# - Cosponsorship tracked via coauthors field

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
tx_config <- list(
  bill_types = c("HB", "SB"),
  drop_sponsor_pattern = "committee",
  step_terms = list(
    aic = c(
      "public hearing", "testimony",
      "pending in committee", "^reported f"
    ),
    abc = c(
      "^reported favorably w", "1st printing sent", "calendar",
      "read 2nd time", "point of order", "^amend", "motion to",
      "record vote", "read 3rd time", "rules suspended",
      "pass", "fail"
    ),
    pass = c(
      "^passed$", "^passed as amended$",
      "^reported engross", "sent to the senate",
      "sent to the house"
    ),
    law = c(
      "signed by the gov", "^effective",
      "filed w/o the gov.+ signature"
    )
  )
)
# nolint end: line_length_linter

#' Compute TX legislature number from term
#'
#' Formula: s_num = 0.5 * (start_year - 2019) + 86
#' e.g., 2023 -> 88
#'
#' @param term Term string in "YYYY_YYYY" format
#' @return Integer legislature number
get_legislature_num <- function(term) {
  start_year <- as.integer(strsplit(term, "_")[[1]][1])
  as.integer(0.5 * (start_year - 2019) + 86)
}

#' Standardize TX bill ID format
#'
#' Converts "SB 14" -> "SB0014", "HB 1234" -> "HB1234"
#'
#' @param bill_id Vector of bill IDs
#' @return Vector of standardized bill IDs
standardize_bill_id <- function(bill_id) {
  bill_id <- toupper(bill_id)
  paste0(
    gsub(" [0-9]+", "", bill_id),
    str_pad(gsub("[A-Z]+ ", "", bill_id), 4, pad = "0")
  )
}

#' Clean TX accent encoding issues
#'
#' Handles mojibake patterns (UTF-8 bytes interpreted as Latin-1)
#' found in TX data FIRST, then standard Unicode accents via
#' standardize_accents. Order matters: standardize_accents would
#' convert the tilde-a in mojibake before we can match the
#' two-character sequence.
#'
#' Faithful port of old script lines 150-164.
#'
#' @param x Character vector to clean
#' @return Cleaned character vector
clean_tx_accents <- function(x) {
  # Mojibake FIRST (before standardize_accents converts individual chars)
  x <- gsub("\u00e3\u00a1", "a", x) # a-tilde + inverted-! -> a
  x <- gsub("\u00e3\u00a9", "e", x) # a-tilde + copyright  -> e
  x <- gsub("\u00e3\u00b1", "n", x) # a-tilde + plus-minus -> n
  x <- gsub("\u00e3\u00ad", "i", x) # a-tilde + soft-hyphen -> i
  # Then standard Unicode accents
  x <- standardize_accents(x)
  x
}

# Stage 1 Hook: Load bill files
#' Load bill files for Texas
#'
#' TX has multiple session files per term: regular (88R) + special
#' sessions (881, 882, etc.). Discovers session suffixes from
#' filenames.
#'
#' Faithful port of old script lines 104-117, 336-350.
#'
#' @param bill_dir Path to bill directory
#' @param state State code ("TX")
#' @param term Term in format "YYYY_YYYY"
#' @param verbose Show detailed logging
#' @return List with bill_details and bill_history dataframes
load_bill_files <- function(bill_dir, state, term, verbose = TRUE) {
  s_num <- get_legislature_num(term)

  # Discover session suffixes from filenames
  all_files <- list.files(bill_dir)
  detail_files <- all_files[grepl("Bill_Details", all_files)]
  sessions <- gsub(".+Bill_Details_|\\.csv", "", detail_files)
  t_sessions <- sessions[startsWith(sessions, as.character(s_num))]

  if (verbose) {
    cli_log(glue("TX Legislature: {s_num}"))
    cli_log(glue(
      "  Sessions found: {paste(t_sessions, collapse = ', ')}"
    ))
  }

  # Load and combine bill details
  bill_details <- bind_rows(lapply(t_sessions, function(s) {
    path <- file.path(
      bill_dir, glue("{state}_Bill_Details_{s}.csv")
    )
    df <- read.csv(path, stringsAsFactors = FALSE)
    df$session <- as.character(df$session)
    df
  }))

  # Load and combine bill histories
  bill_history <- bind_rows(lapply(t_sessions, function(s) {
    path <- file.path(
      bill_dir, glue("{state}_Bill_Histories_{s}.csv")
    )
    df <- read.csv(path, stringsAsFactors = FALSE)
    df$session <- as.character(df$session)
    df
  }))

  if (verbose) {
    cli_log(glue(
      "  Details: {nrow(bill_details)} rows, ",
      "Histories: {nrow(bill_history)} rows"
    ))
  }

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 1.5 Hook: Preprocess raw data
#' Preprocess raw data for Texas
#'
#' Normalizes raw CSV data: adds term, recodes session to
#' "88-RS"/"88-SS1" format, renames bill_number -> bill_id,
#' standardizes bill IDs, deduplicates.
#'
#' Faithful port of old script lines 120-137, 352-363.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  s_num <- get_legislature_num(term)

  # --- Bill details ---
  bill_details$term <- term
  bill_details$session_type <- recode(
    gsub(glue("^{s_num}"), "", bill_details$session),
    "R" = "RS", "1" = "SS1", "2" = "SS2", "3" = "SS3",
    "4" = "SS4", "5" = "SS5", "6" = "SS6", "7" = "SS7"
  )
  bill_details$session <- paste0(
    s_num, "-", bill_details$session_type
  )
  bill_details <- select(bill_details, -"session_type")

  # Rename bill_number -> bill_id, standardize format
  bill_details <- bill_details %>%
    rename(bill_id = "bill_number") %>%
    mutate(bill_id = standardize_bill_id(.data$bill_id)) %>%
    arrange(.data$session, .data$bill_id)

  # Deduplicate
  bill_details <- distinct(bill_details)

  # --- Bill history ---
  bill_history$term <- term
  bill_history$session_type <- recode(
    gsub(glue("^{s_num}"), "", bill_history$session),
    "R" = "RS", "1" = "SS1", "2" = "SS2", "3" = "SS3",
    "4" = "SS4", "5" = "SS5", "6" = "SS6", "7" = "SS7"
  )
  bill_history$session <- paste0(
    s_num, "-", bill_history$session_type
  )
  bill_history <- select(bill_history, -"session_type")

  bill_history <- bill_history %>%
    rename(bill_id = "bill_number") %>%
    mutate(bill_id = standardize_bill_id(.data$bill_id)) %>%
    arrange(.data$session, .data$bill_id, .data$order)

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 2 Hook: Clean bill details
#' Clean bill details for Texas
#'
#' TX-specific transformations:
#' - Filter to HB/SB bill types
#' - Create main_chamb_comm_status for post_evaluate_bill
#' - Lowercase and standardize accents on authors/coauthors
#' - Fix Anchia encoding across all name columns
#' - Extract LES_sponsor (first author before semicolon)
#' - Extract nicknames
#' - Term-specific Rodriguez encoding fix (2013-2020)
#' - Drop committee-sponsored and empty-sponsor bills
#'
#' Faithful port of old script lines 139-329, 369.
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
    filter(.data$bill_type %in% tx_config$bill_types) %>%
    select(-"bill_type")

  if (verbose) {
    cli_log(glue("Bills after type filter: {nrow(bill_details)}"))
  }

  # Create main_chamb_comm_status (old script line 369)
  bill_details <- bill_details %>%
    mutate(
      main_chamb_comm_status = ifelse(
        substring(.data$bill_id, 1, 1) == "H",
        .data$hc_status,
        .data$sc_status
      )
    )

  # Lowercase and standardize accents on authors/coauthors
  # (old script lines 150-164)
  bill_details$authors <- clean_tx_accents(
    tolower(bill_details$authors)
  )
  bill_details$coauthors <- clean_tx_accents(
    tolower(bill_details$coauthors)
  )

  # Check for by-request bills (old script lines 167-169)
  if (any(grepl(
    "\\(br\\)|by request| br$", bill_details$authors
  ))) {
    cli_warn("BY REQUEST bills detected - review needed")
  }

  # LES_sponsor = first author before semicolon (old script line 174)
  bill_details$LES_sponsor <- tolower(
    gsub(";.+", "", bill_details$authors)
  )

  # Extract nickname (old script lines 178-179)
  bill_details$nickname <- str_trim(gsub(
    '\\"', "",
    str_extract(bill_details$LES_sponsor, ' ".+"$')
  ))
  bill_details$LES_sponsor <- str_trim(gsub(
    "  +", " ",
    gsub(' ".+"$', "", bill_details$LES_sponsor)
  ))

  # Term-specific Rodriguez encoding fix (old script lines 182-186)
  if (term %in% c(
    "2013_2014", "2015_2016", "2017_2018", "2019_2020"
  )) {
    s_mask <- grepl("rodr\u00e3|rodriguez", bill_details$LES_sponsor) &
      substring(bill_details$bill_id, 1, 1) == "S"
    bill_details$LES_sponsor[s_mask] <- "rodriguez, jose"
    bill_details <- bill_details %>%
      mutate(
        coauthors = ifelse(
          grepl("^S", .data$bill_id),
          gsub("rodr\u00e3.", "rodri", .data$coauthors),
          .data$coauthors
        ),
        coauthors = ifelse(
          grepl("^S", .data$bill_id),
          gsub("rodriguez", "rodriguez, jose", .data$coauthors),
          .data$coauthors
        )
      )
  }

  # Fix Anchia encoding across name columns (old script lines 188-189)
  fix_anchia <- function(x) {
    gsub("anch\u00e3\u00ada", "anchia", x, ignore.case = TRUE)
  }
  bill_details$authors <- fix_anchia(bill_details$authors)
  bill_details$coauthors <- fix_anchia(bill_details$coauthors)
  bill_details$LES_sponsor <- fix_anchia(bill_details$LES_sponsor)
  if ("sponsors" %in% names(bill_details)) {
    bill_details$sponsors <- fix_anchia(bill_details$sponsors)
  }
  if ("cosponsors" %in% names(bill_details)) {
    bill_details$cosponsors <- fix_anchia(bill_details$cosponsors)
  }

  # Drop committee-sponsored bills (old script lines 319-322)
  comm_bills <- bill_details %>%
    filter(grepl(tx_config$drop_sponsor_pattern, .data$LES_sponsor,
      ignore.case = TRUE
    ))
  if (nrow(comm_bills) > 0 && verbose) {
    cli_log(glue(
      "Dropping {nrow(comm_bills)} committee-sponsored bills"
    ))
  }
  bill_details <- bill_details %>%
    filter(!grepl(tx_config$drop_sponsor_pattern, .data$LES_sponsor,
      ignore.case = TRUE
    ))

  # Drop empty sponsors (old script lines 327-329)
  empty_bills <- bill_details %>%
    filter(.data$LES_sponsor == "" | is.na(.data$LES_sponsor))
  if (nrow(empty_bills) > 0) {
    if (verbose) {
      cli_log(glue(
        "Dropping {nrow(empty_bills)} bills without sponsor"
      ))
    }
    bill_details <- bill_details %>%
      filter(
        !(.data$LES_sponsor == "" | is.na(.data$LES_sponsor))
      )
  }

  list(
    all_bill_details = all_bill_details,
    bill_details = bill_details
  )
}
# nolint end: line_length_linter

# Stage 2 Hook: Clean bill history
#' Clean bill history for Texas
#'
#' TX bill history already has chamber column populated. Minimal
#' cleaning: lowercase actions for pattern matching.
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  bill_history$action <- tolower(bill_history$action)
  bill_history
}

# Stage 3 Hook: Transform SS bills
#' Transform SS bills for Texas
#'
#' Corrects bill_id format: "AB" -> "HB".
#' Applies term-specific bill_id corrections.
#'
#' Faithful port of old script lines 92, 202-212.
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return Transformed SS bills
# nolint start: line_length_linter
transform_ss_bills <- function(ss_bills, term) {
  # "AB" -> "HB" correction (old script line 92)
  ss_bills$bill_id <- gsub("AB", "HB", ss_bills$bill_id)

  # Term-specific corrections (old script lines 202-212)
  if (term == "2019_2020") {
    ss_bills$bill_id[ss_bills$bill_id == "HB10001"] <- "HB0001"
  }
  if (term == "2021_2022") {
    target_ids <- c(
      "SB60006", "SB10001", "SB20002", "SB30003",
      "HB90009", "SB70007", "SB40004", "SB90009", "SB80008"
    )
    mask <- ss_bills$bill_id %in% target_ids
    ss_bills$bill_id[mask] <- sub(
      "(SB|HB)\\d(\\d+)", "\\1\\2", ss_bills$bill_id[mask]
    )
  }

  ss_bills
}
# nolint end: line_length_linter

# Stage 3 Hook: Enrich SS bills with session
#' Enrich SS bills with session info for Texas
#'
#' TX bill numbers restart for special sessions, so bill_id alone
#' is ambiguous. For unambiguous bills (bill_id in only one
#' session), assigns that session. For ambiguous bills, reads
#' human-reviewed resolution CSV that maps each (bill_id, Title)
#' to its correct session.
#'
#' @param ss_bills SS bills dataframe (with bill_id, Title, etc.)
#' @param term Term string
#' @return SS bills with session column added
# nolint start: line_length_linter
enrich_ss_with_session <- function(ss_bills, term) {
  s_num <- get_legislature_num(term)
  rs_session <- paste0(s_num, "-RS")

  # Read human-reviewed resolution file for ambiguous bill_ids
  review_path <- file.path(
    repo_root, ".data", "TX", "review",
    "TX_SS_session_resolution.csv"
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

  # Assign session: use review CSV for resolved bills,
  # default to regular session for the rest
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
# nolint end: line_length_linter

# Stage 1 Hook: Transform commemorative bills
#' Transform commemorative bills for Texas
#'
#' TX commem file has non-standard formats:
#' - bill_id = "SB 16" (needs "SB0016")
#' - term = "88" (legislature number, needs "2023_2024")
#'
#' Faithful port of old script lines 71-73.
#'
#' @param commem_bills Commem bills dataframe
#' @param term Term string (pipeline term, e.g., "2023_2024")
#' @return Transformed commem bills
transform_commem_bills <- function(commem_bills, term) {
  commem_bills %>%
    mutate(
      bill_id = standardize_bill_id(.data$bill_id),
      term = .env$term
    ) %>%
    distinct()
}

# Stage 3 Hook: Get missing SS bills
#' Get genuinely missing SS bills for Texas
#'
#' Checks which SS bills are missing from bill_details after
#' filtering, excluding committee-sponsored bills.
#'
#' Faithful port of old script lines 219-228.
#'
#' @param ss_filtered SS bills filtered to term and valid types
#' @param bill_details Filtered bill details
#' @param all_bill_details Unfiltered bill details
#' @return Dataframe of genuinely missing SS bills
# nolint start: line_length_linter
get_missing_ss_bills <- function(ss_filtered, bill_details,
                                 all_bill_details) {
  missing <- ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term")) %>%
    left_join(
      all_bill_details %>% select("bill_id", "term", "authors"),
      by = c("bill_id", "term")
    ) %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    ) %>%
    filter(.data$bill_type %in% tx_config$bill_types) %>%
    select(-"bill_type") %>%
    filter(!grepl("committee", .data$authors, ignore.case = TRUE))

  missing
}
# nolint end: line_length_linter

# Stage 4 Hook: Post-evaluate bill
#' Post-evaluate bill hook for Texas
#'
#' Committee status overrides for achievement coding:
#' 1. If abc == 0 AND main_chamb_comm_status == "out of committee"
#'    -> abc = 1
#' 2. If pc == 0 AND opposing chamber has non-empty committee status
#'    -> abc = pc = 1
#'
#' Faithful port of old script lines 413-422.
#'
#' @param bill_stages Dataframe row with bill achievement
#' @param bill_row Original bill details row
#' @param bill_history Bill history for this bill
#' @return Modified bill_stages
# nolint start: line_length_linter
post_evaluate_bill <- function(bill_stages, bill_row, bill_history) {
  # Check if out of committee in originating chamber
  if (bill_stages$action_beyond_comm == 0 &&
    !is.na(bill_row$main_chamb_comm_status) &&
    tolower(bill_row$main_chamb_comm_status) == "out of committee") {
    bill_stages$action_beyond_comm <- 1
  }

  # Check if in or out of committee in opposing chamber
  b_prefix <- substring(bill_row$bill_id, 1, 1)
  if (bill_stages$passed_chamber == 0) {
    if (b_prefix == "H" &&
      !is.na(bill_row$sc_status) &&
      bill_row$sc_status != "") {
      bill_stages$action_beyond_comm <- 1
      bill_stages$passed_chamber <- 1
    } else if (b_prefix == "S" &&
      !is.na(bill_row$hc_status) &&
      bill_row$hc_status != "") {
      bill_stages$action_beyond_comm <- 1
      bill_stages$passed_chamber <- 1
    }
  }

  bill_stages
}
# nolint end: line_length_linter

# Stage 5 Hook: Derive unique sponsors
#' Derive unique sponsors from bills for Texas
#'
#' TX uses authors/coauthors (not primary_sponsor/cosponsors).
#' Adds cosponsor-only legislators from coauthors field.
#'
#' Faithful port of old script lines 484-516.
#'
#' @param bills Dataframe of bills with achievement columns
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregate stats
derive_unique_sponsors <- function(bills, term) {
  # Build sponsor set from authored bills
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

  # Add cosponsor-only legislators from coauthors
  # (old script lines 494-516)
  unique_cospon <- str_trim(
    unique(unlist(str_split(bills$coauthors, "; ")))
  )

  for (nonspon in unique_cospon) {
    if (nonspon != "" && !is.na(nonspon) &&
      !(nonspon %in% all_sponsors$LES_sponsor)) {
      ns_adj <- gsub(
        "\\(", "\\\\(",
        gsub("\\)", "\\\\)", nonspon)
      )
      chamb <- unique(substring(
        bills[grepl(ns_adj, bills$coauthors), ]$bill_id, 1, 1
      ))

      # Term-specific skips (old script lines 502-508)
      if (term == "2017_2018" && nonspon == "rodriguez") {
        next
      }

      # Skip names appearing in both chambers
      if ("H" %in% chamb && "S" %in% chamb) {
        cli_warn(glue(
          "CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS"
        ))
      } else {
        all_sponsors <- bind_rows(
          all_sponsors,
          tibble(
            LES_sponsor = nonspon,
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

  all_sponsors
}

# Stage 5 Hook: Compute cosponsorship
#' Compute cosponsorship counts for Texas
#'
#' For each sponsor, counts occurrences in coauthors of
#' same-chamber bills.
#'
#' Faithful port of old script lines 518-532.
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param bills Dataframe of bills with coauthors column
#' @return Dataframe with num_cosponsored_bills added
compute_cosponsorship <- function(all_sponsors, bills) {
  all_sponsors$num_cosponsored_bills <- NA

  for (i in seq_len(nrow(all_sponsors))) {
    c_sub <- filter(
      bills,
      substring(.data$bill_id, 1, 1) == all_sponsors[i, ]$chamber
    )
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(
      all_sponsors[i, ]$LES_sponsor,
      tolower(c_sub$coauthors)
    ))
  }

  if (min(all_sponsors$num_cosponsored_bills) < 0) {
    cli_warn("Negative cosponsor counts found")
    stop("Cosponsorship validation failed")
  }

  all_sponsors
}

# Stage 5 Hook: Clean sponsor names
#' Clean sponsor names for Texas
#'
#' Creates match_name_chamber for fuzzy matching.
#' Term-specific last_name overrides exist for old terms
#' (2005-2014) but are effectively dead code in the old script
#' (overwritten by subsequent re-derivation). Omitted here.
#'
#' Faithful port of old script lines 534-561.
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param term Term string
#' @return Dataframe with match_name_chamber column added
clean_sponsor_names <- function(all_sponsors, term) {
  # Create match_name_chamber (old script line 560)
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
#' Adjust legiscan data for matching
#'
#' Two-pass deduplication on last_name:
#' Pass 1: n > 2 -> "last first middle_init",
#'   n == 2 -> "last first", else -> last_name
#' Pass 2: Re-group, upgrade remaining n > 1 to
#'   "last first middle_init"
#'
#' Faithful port of old script lines 564-600.
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string
#' @return Adjusted legiscan with match_name_chamber column
# nolint start: line_length_linter
adjust_legiscan_data <- function(legiscan, term) {
  # Filter to non-committee members
  legiscan <- legiscan %>%
    filter(.data$committee_id == 0)

  # Term-specific adjustments (old script lines 572-581)
  if (term == "2019_2020") {
    legiscan <- bind_rows(
      legiscan,
      legiscan %>%
        filter(.data$people_id == 16834) %>%
        mutate(role = "Rep", district = "HD-076"),
      legiscan %>%
        filter(.data$people_id == 5904) %>%
        mutate(role = "Rep", district = "HD-119")
    )
  }
  if (term == "2021_2022") {
    legiscan <- legiscan %>%
      filter(.data$name != "Victoria Neave Criado")
  }

  # Two-pass dedup on last_name (old script lines 587-600)
  # Handle NA middle_name: replace with "" before concatenation
  legiscan <- legiscan %>%
    mutate(
      middle_init = ifelse(
        is.na(.data$middle_name), "",
        substr(.data$middle_name, 1, 1)
      )
    )

  # Pass 1
  legiscan <- legiscan %>%
    group_by(.data$last_name) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      match_name = case_when(
        .data$n > 2 ~ paste0(
          .data$last_name, " ", .data$first_name,
          .data$middle_init
        ),
        .data$n == 2 ~ paste(.data$last_name, .data$first_name),
        TRUE ~ .data$last_name
      )
    )

  # Pass 2: re-group on match_name, upgrade remaining dupes
  legiscan <- legiscan %>%
    group_by(.data$match_name) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      match_name = ifelse(
        .data$n > 1,
        paste0(
          .data$last_name, " ", .data$first_name,
          .data$middle_init
        ),
        .data$match_name
      ),
      match_name_chamber = tolower(paste(
        .data$match_name,
        substr(.data$district, 1, 1),
        sep = "-"
      ))
    )

  legiscan
}
# nolint end: line_length_linter

# Stage 5 Hook: Reconcile legiscan with sponsors
#' Reconcile legiscan with sponsors
#'
#' Faithful port of old script lines 609-651.
#' Term-specific custom_match for inexact_join.
#'
#' @param sponsors Dataframe of unique sponsors
#' @param legiscan Adjusted legiscan dataframe
#' @param term Term string
#' @return Joined dataframe of sponsors matched to legiscan
# nolint start: line_length_linter
reconcile_legiscan_with_sponsors <- function(sponsors, legiscan,
                                             term) {
  if (term == "2019_2020") {
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "gutierrez roland-s" = NA_character_,
        "bonnen dennis-h" = NA_character_,
        "blanco cesar-s" = NA_character_,
        "johnson nathan-s" = "johnson-s",
        "lucio eddie-s" = "lucio-s",
        "anderson-h" = 'anderson, charles "doc"-h',
        "munoz-h" = "munoz, jr.-h",
        "romero-h" = "romero, jr.-h",
        "sherman-h" = "sherman, sr.-h"
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
        "parker nathaniel-s" = NA_character_,
        "johnson nathan-s" = "johnson-s",
        "springer drew-h" = NA_character_,
        "lucio eddie-s" = "lucio-s",
        "munoz-h" = "munoz, jr.-h",
        "sherman-h" = "sherman, sr.-h",
        "phelan-h" = NA_character_,
        "romero-h" = "romero, jr.-h"
      )
    )
  } else if (term == "2023_2024") {
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # Fix dutton→button fuzzy mismatch
        "dutton-h" = "dutton, harold-h",
        # Sherman Sr.
        "sherman-h" = "sherman, sr.-h",
        # Senate Johnson (last name only in sponsor data)
        "johnson nathan-s" = "johnson-s",
        # Munoz Jr.
        "munoz-h" = "munoz, jr.-h",
        # Romero Jr.
        "romero-h" = "romero, jr.-h",
        # Ordaz Perez -> ordaz in sponsor data
        "ordaz perez-h" = "ordaz-h",
        # Christian Manuel (legiscan last_name=Hayes)
        "hayes christian-h" = "manuel-h",
        # Speaker Phelan - no bills sponsored
        "phelan-h" = NA_character_,
        # Fanny Jetton - prevent double-match to jacey
        "jetton fanny-h" = NA_character_
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
    mutate(
      chamber = ifelse(
        substring(.data$bill_id, 1, 1) == "H", "H", "S"
      )
    )
}

# Export config and functions
list(
  bill_types = tx_config$bill_types,
  step_terms = tx_config$step_terms,
  load_bill_files = load_bill_files,
  preprocess_raw_data = preprocess_raw_data,
  clean_bill_details = clean_bill_details,
  clean_bill_history = clean_bill_history,
  transform_ss_bills = transform_ss_bills,
  enrich_ss_with_session = enrich_ss_with_session,
  transform_commem_bills = transform_commem_bills,
  get_missing_ss_bills = get_missing_ss_bills,
  post_evaluate_bill = post_evaluate_bill,
  derive_unique_sponsors = derive_unique_sponsors,
  compute_cosponsorship = compute_cosponsorship,
  clean_sponsor_names = clean_sponsor_names,
  adjust_legiscan_data = adjust_legiscan_data,
  reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
  prepare_bills_for_les = prepare_bills_for_les
)
