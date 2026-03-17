# Oregon (OR) State Configuration
#
# OR has multiple sessions per biennium: regular + special sessions.
# Sessions: 2023R1, 2024R1, 2024S1, etc.
# Bill IDs do NOT restart across sessions (unique within term).
# Sponsor format: "Rep LastName" / "Sen LastName" (last-name-only)
# Has cosponsorship data (semicolon-separated)
#
# Source: .dropbox/old_estimate_scripts/OR - Estimate LES AV.R

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
or_config <- list(
  bill_types = c("HB", "SB"),

  # Step terms from old script lines 421-427
  step_terms = list(
    aic = c(
      "reported out",
      "^recommendation",
      "^(without|minority|majority) recommendation",
      "do pass", "do not pass", "be adopted", "do adopt",
      "work session held",
      "(work session|public hearing): heard",
      "(work session|public hearing) held"
    ),
    abc = c(
      "reported out",
      "^recommendation",
      "^(without|minority|majority) recommendation",
      "second reading", "third reading",
      "^pass", "^fail"
    ),
    pass = c("third reading.+passed", "^passed\\."),
    law = c(
      "governor signed", "^chapter [0-9]+",
      "effective date",
      "filed with secretary of state without governor's"
    )
  )
)
# nolint end: line_length_linter

# Stage 1 Hook: Load bill files
#' Load bill files for Oregon
#'
#' OR has multiple session files per term (2023R1, 2024R1, 2024S1).
#' Discovers session suffixes from filenames.
#'
#' Faithful port of old script lines 55-130, 366-380.
#'
#' @param bill_dir Path to bill directory
#' @param state State code ("OR")
#' @param term Term in format "YYYY_YYYY"
#' @param verbose Show detailed logging
#' @return List with bill_details and bill_history dataframes
load_bill_files <- function(bill_dir, state, term, verbose = TRUE) {
  years <- strsplit(term, "_")[[1]]
  year1 <- years[1]
  year2 <- years[2]

  # Discover session suffixes from filenames
  all_files <- list.files(bill_dir)
  detail_files <- all_files[grepl("Bill_Details", all_files)]
  sessions <- gsub(".+Bill_Details_|\\.csv", "", detail_files)
  t_sessions <- sessions[
    startsWith(sessions, year1) | startsWith(sessions, year2)
  ]

  if (verbose) {
    cli_log(glue(
      "Sessions found: {paste(t_sessions, collapse = ', ')}"
    ))
  }

  # Load and combine bill details
  bill_details <- bind_rows(lapply(t_sessions, function(s) {
    path <- file.path(
      bill_dir, glue("{state}_Bill_Details_{s}.csv")
    )
    df <- read.csv(path, stringsAsFactors = FALSE)
    df$session <- as.character(df$session)
    df$chapter_num <- as.numeric(df$chapter_num)
    df$LC_num <- as.character(df$LC_num)
    df
  }))

  # Load and combine bill histories
  bill_history <- bind_rows(lapply(t_sessions, function(s) {
    path <- file.path(
      bill_dir, glue("{state}_Bill_Histories_{s}.csv")
    )
    df <- read.csv(path, stringsAsFactors = FALSE)
    df$session <- as.character(df$session)
    df$action_date <- as.character(df$action_date)
    df
  }))

  bill_history <- distinct(bill_history)

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
#' Preprocess raw data for Oregon
#'
#' Converts session codes: "2023R1"->"2023-RS", "2024S1"->"2024-SS1".
#' The old script line 138 has a bug (nchar produces "SS2" instead of
#' "SS1") — use correct gsub approach from commem handling (line 73-75).
#' Adds term column.
#'
#' Faithful port of old script lines 133-138, 383-388.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  # Session conversion: R1->RS, S->SS (with number)
  recode_session <- function(session) {
    # First convert R1 -> RS
    session <- gsub("R1$", "RS", session)
    # Then convert S followed by digit(s) -> SS followed by same digits
    session <- gsub("S(\\d+)$", "SS\\1", session)
    # Insert dash between year and session type
    paste0(substr(session, 1, 4), "-", substring(session, 5))
  }

  # --- Bill details ---
  bill_details$term <- term
  bill_details$session <- recode_session(bill_details$session)

  # --- Bill history ---
  bill_history$term <- term
  bill_history$session <- recode_session(bill_history$session)

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 1 Hook: Transform commemorative bills
#' Transform commemorative bills for Oregon
#'
#' OR commem file has session in "2023R1" format. Convert to
#' "2023-RS" format to match bill data.
#'
#' Faithful port of old script lines 71-75.
#'
#' @param commem_bills Commem bills dataframe
#' @param term Term string
#' @return Transformed commem bills
transform_commem_bills <- function(commem_bills, term) {
  commem_bills <- commem_bills %>%
    mutate(
      term = .env$term,
      session = gsub("R1$", "RS", .data$session),
      session = gsub("S(\\d+)$", "SS\\1", .data$session),
      session = paste0(
        substr(.data$session, 1, 4), "-",
        substring(.data$session, 5)
      )
    )
  commem_bills
}

# Stage 2 Hook: Clean bill details
#' Clean bill details for Oregon
#'
#' 1. Derive bill_type, filter to HB/SB
#' 2. Lowercase + standardize accents on sponsors/cosponsors
#' 3. Strip "rep speaker" -> "rep" (line 174)
#' 4. Extract LES_sponsor (first sponsor before ";", strip title)
#' 5. Drop committee-sponsored and "introduced presession by request"
#' 6. Drop empty sponsors
#'
#' Faithful port of old script lines 141-359.
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string
#' @param verbose Show detailed logging
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Derive bill_type, save unfiltered
  bill_details <- bill_details %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    )
  all_bill_details <- bill_details

  # Filter to HB/SB
  bill_details <- bill_details %>%
    filter(.data$bill_type %in% or_config$bill_types) %>%
    select(-"bill_type")

  if (verbose) {
    cli_log(glue("Bills after type filter: {nrow(bill_details)}"))
  }

  # Lowercase sponsors and cosponsors
  bill_details$primary_sponsors <- tolower(bill_details$primary_sponsors)
  bill_details$cosponsors <- tolower(bill_details$cosponsors)

  # Standardize accents (old script lines 150-161)
  bill_details$primary_sponsors <- standardize_accents(
    bill_details$primary_sponsors
  )
  bill_details$cosponsors <- standardize_accents(
    bill_details$cosponsors
  )

  # Fill NAs
  bill_details$primary_sponsors <- ifelse(
    is.na(bill_details$primary_sponsors), "",
    bill_details$primary_sponsors
  )
  bill_details$cosponsors <- ifelse(
    is.na(bill_details$cosponsors), "",
    bill_details$cosponsors
  )

  # Strip "rep speaker" -> "rep" (old script line 174)
  # Strip "sen president" -> "sen" (analogous to speaker)
  bill_details$primary_sponsors <- gsub(
    "rep speaker", "rep",
    bill_details$primary_sponsors
  )
  bill_details$primary_sponsors <- gsub(
    "sen president", "sen",
    bill_details$primary_sponsors
  )
  bill_details$cosponsors <- gsub(
    "rep speaker", "rep",
    bill_details$cosponsors
  )
  bill_details$cosponsors <- gsub(
    "sen president", "sen",
    bill_details$cosponsors
  )

  # Extract primary_sponsor (first before ";")
  bill_details$primary_sponsor <- gsub(
    ";.+", "", bill_details$primary_sponsors
  )

  # Strip titles from primary_sponsors and cosponsors for later use
  # (old script lines 213-216)
  bill_details$primary_sponsors <- gsub(
    "^sen |^rep ", "", bill_details$primary_sponsors
  )
  bill_details$primary_sponsors <- gsub(
    " sen | rep ", " ", bill_details$primary_sponsors
  )
  bill_details$cosponsors <- gsub(
    "^sen |^rep ", "", bill_details$cosponsors
  )
  bill_details$cosponsors <- gsub(
    " sen | rep ", " ", bill_details$cosponsors
  )

  # LES_sponsor = primary_sponsor stripped of title
  # (old script line 220)
  bill_details$LES_sponsor <- gsub(
    "^sen |^rep ", "", bill_details$primary_sponsor
  )

  # Strip " jr" suffix from sponsor names (old script line 187-188)
  # e.g., "manning jr" -> "manning"
  bill_details$LES_sponsor <- gsub(
    " jr$", "", bill_details$LES_sponsor
  )

  # Drop committee-sponsored bills (old script lines 354-358)
  is_committee <- grepl("committee", bill_details$LES_sponsor)
  is_presession <- grepl(
    "introduced presession by request", bill_details$LES_sponsor
  )
  drop_mask <- is_committee | is_presession
  if (sum(drop_mask) > 0 && verbose) {
    cli_log(glue(
      "Dropping {sum(drop_mask)} committee/presession bills"
    ))
  }
  bill_details <- bill_details[!drop_mask, ]

  # Drop empty sponsors (old script lines 347-351)
  empty <- bill_details$LES_sponsor == "" |
    is.na(bill_details$LES_sponsor)
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

# Stage 2 Hook: Clean bill history
#' Clean bill history for Oregon
#'
#' Replace "J" chamber with NA, forward-fill within bill groups,
#' recode H->House, S->Senate, G->Governor, CC->Conference.
#' Trim actions. Reorder and renumber.
#'
#' Faithful port of old script lines 390-447.
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  # Replace J with NA for forward-fill (old script line 402)
  bill_history <- bill_history %>%
    mutate(chamber = ifelse(.data$chamber == "J", NA, .data$chamber))

  # Forward-fill chamber within bill groups (old script lines 403-405)
  bill_history <- bill_history %>%
    group_by(.data$term, .data$session, .data$bill_id) %>%
    tidyr::fill(.data$chamber) %>%
    ungroup()

  # Recode chamber (old script line 407)
  bill_history$chamber <- dplyr::recode(
    bill_history$chamber,
    "H" = "House", "S" = "Senate",
    "G" = "Governor", "CC" = "Conference"
  )

  # Trim actions (old script line 447)
  bill_history$action <- str_trim(bill_history$action)

  # Reorder and renumber (old script lines 392-397)
  bill_history <- bill_history %>%
    arrange(
      .data$term, .data$session, .data$bill_id,
      .data$action_date, .data$order
    ) %>%
    group_by(.data$term, .data$session, .data$bill_id) %>%
    mutate(order = seq_len(n())) %>%
    ungroup()

  # Drop comm_order if present
  if ("comm_order" %in% names(bill_history)) {
    bill_history <- select(bill_history, -"comm_order")
  }

  bill_history
}

# Stage 4 Hook: Post-evaluate bill
#' Post-evaluate bill for Oregon
#'
#' Chapter number fallback: if law==0 and chapter_num is not NA,
#' set abc=pass=law=1. Faithful port of old script lines 459-461.
#'
#' @param bill_stages Dataframe row with bill achievement
#' @param bill_row Original bill details row
#' @param bill_history Bill history for this bill
#' @return Modified bill_stages
post_evaluate_bill <- function(bill_stages, bill_row, bill_history) {
  if (bill_stages$law == 0 && !is.na(bill_row$chapter_num)) {
    bill_stages$action_beyond_comm <- 1
    bill_stages$passed_chamber <- 1
    bill_stages$law <- 1
  }
  bill_stages
}

# Stage 3 Hook: Get missing SS bills
#' Identify genuinely missing SS bills for Oregon
#'
#' OR has committee-sponsored bills that are intentionally excluded.
#' SS bills whose summary mentions "committee" are not genuinely missing.
#'
#' Faithful port of old script lines 246-254.
#'
#' @param ss_filtered Filtered SS bills for this term
#' @param bill_details Cleaned bill details (after filtering)
#' @param all_bill_details All bill details (before filtering)
#' @return Dataframe of genuinely missing SS bills
get_missing_ss_bills <- function(ss_filtered, bill_details,
                                 all_bill_details) {
  missing <- ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term"))

  # Look up details from all_bill_details
  missing_with_info <- missing %>%
    left_join(
      all_bill_details %>%
        select("bill_id", "term", "primary_sponsors", "summary") %>%
        distinct(),
      by = c("bill_id", "term")
    )

  # Filter to valid bill types
  missing_with_info <- missing_with_info %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    ) %>%
    filter(.data$bill_type %in% or_config$bill_types) %>%
    select(-"bill_type")

  # Filter out bills that were intentionally excluded:
  # - committee-sponsored (summary mentions "committee")
  # - empty/NA primary_sponsors
  # - introduced presession by request
  genuinely_missing <- missing_with_info %>%
    filter(
      !grepl("committee", .data$summary, ignore.case = TRUE),
      !is.na(.data$primary_sponsors),
      .data$primary_sponsors != "",
      !grepl(
        "introduced presession by request",
        tolower(.data$primary_sponsors)
      )
    ) %>%
    select("bill_id", "term")

  genuinely_missing
}

# Stage 5 Hook: Derive unique sponsors
#' Derive unique sponsors for Oregon
#'
#' Standard aggregation: group by (LES_sponsor, chamber, term).
#' Chamber derived from bill_id prefix.
#'
#' Faithful port of old script lines 521-531.
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

# Stage 5 Hook: Compute cosponsorship
#' Compute cosponsorship counts for Oregon
#'
#' Build cospon_match = paste(LES_sponsor, cosponsors, sep="; ").
#' For each sponsor: regex search in same-chamber bills, subtract
#' own sponsored bills. Validate non-negative.
#'
#' Faithful port of old script lines 549-558.
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param bills Dataframe of bills with cosponsors column
#' @return Dataframe with num_cosponsored_bills updated
compute_cosponsorship <- function(all_sponsors, bills) {
  bills$cospon_match <- paste(
    bills$LES_sponsor, bills$cosponsors, sep = "; "
  )

  for (i in seq_len(nrow(all_sponsors))) {
    c_sub <- bills[
      substring(bills$bill_id, 1, 1) %in%
        ifelse(all_sponsors[i, ]$chamber == "H", "H", "S"),
    ]
    # Escape regex special chars in sponsor name
    sn <- gsub("\\.", "\\\\.",
      gsub("\\(", "\\\\(",
        gsub("\\)", "\\\\)", all_sponsors[i, ]$LES_sponsor)
      )
    )
    search_term <- paste0(
      "^", sn, "$|^", sn, ";|; ", sn, "$|; ", sn, ";"
    )
    all_sponsors$num_cosponsored_bills[i] <- sum(
      grepl(search_term, tolower(c_sub$cospon_match))
    )
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
#' Creates match_name_chamber = tolower(paste(LES_sponsor,
#' chamber_initial, sep="-")). Last-name-only matching.
#'
#' Faithful port of old script line 598.
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
#' Adjust legiscan data for Oregon
#'
#' 1. Filter committee_id == 0
#' 2. Fix role from district prefix: SD->Sen, HD->Rep
#'    (2023 legiscan has role=Rep for ALL including senators)
#' 3. Two-pass dedup on last_name
#' 4. Create match_name_chamber
#'
#' Faithful port of old script lines 616-629.
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string
#' @return Adjusted legiscan with match_name_chamber column
adjust_legiscan_data <- function(legiscan, term) {
  # Filter to non-committee members
  legiscan <- legiscan %>%
    filter(.data$committee_id == 0)

  # Fix role from district prefix (OR-specific data issue)
  # Some sessions have role=Rep for all legislators including senators
  legiscan <- legiscan %>%
    mutate(
      role = ifelse(
        grepl("^SD", .data$district), "Sen",
        ifelse(grepl("^HD", .data$district), "Rep", .data$role)
      )
    ) %>%
    # Re-deduplicate after role fix (a senator may appear as both
    # Rep and Sen across sessions; after fixing, they're duplicates)
    distinct(.data$people_id, .data$role, .keep_all = TRUE)

  # Two-pass dedup on last_name (old script lines 616-629)
  # Pass 1: n > 2 -> "last first_init middle_init",
  #   n == 2 -> "last first_init", else -> last_name
  legiscan <- legiscan %>%
    group_by(.data$last_name) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      match_name = case_when(
        .data$n > 2 ~ paste0(
          .data$last_name, " ",
          substr(.data$first_name, 1, 1),
          substr(
            ifelse(
              is.na(.data$middle_name), "",
              .data$middle_name
            ), 1, 1
          )
        ),
        .data$n == 2 ~ paste(
          .data$last_name, substr(.data$first_name, 1, 1)
        ),
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
          .data$last_name, " ",
          substr(.data$first_name, 1, 1),
          substr(
            ifelse(
              is.na(.data$middle_name), "",
              .data$middle_name
            ), 1, 1
          )
        ),
        .data$match_name
      ),
      match_name_chamber = tolower(paste(
        .data$match_name,
        ifelse(
          substr(.data$role, 1, 1) == "S", "s", "h"
        ),
        sep = "-"
      ))
    )

  legiscan
}

# Stage 5 Hook: Reconcile legiscan with sponsors
#' Reconcile legiscan with sponsors
#'
#' Fuzzy join via inexact OSA. Custom matches TBD from first run.
#' No 2023_2024 entries exist in old script — start empty.
#'
#' Faithful port of old script lines 638-679.
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
        # Block legiscan entries with no matching sponsors
        "yunker-h" = NA_character_,
        "isadore-h" = NA_character_,
        # Reynolds is chamber switcher (HD->SD); all bills are HB
        # Block her Senate entry (0 Senate bills); keep House for matching
        "reynolds l-s" = NA_character_
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
c(
  or_config,
  list(
    load_bill_files = load_bill_files,
    preprocess_raw_data = preprocess_raw_data,
    transform_commem_bills = transform_commem_bills,
    clean_bill_details = clean_bill_details,
    clean_bill_history = clean_bill_history,
    post_evaluate_bill = post_evaluate_bill,
    get_missing_ss_bills = get_missing_ss_bills,
    derive_unique_sponsors = derive_unique_sponsors,
    compute_cosponsorship = compute_cosponsorship,
    clean_sponsor_names = clean_sponsor_names,
    adjust_legiscan_data = adjust_legiscan_data,
    reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
    prepare_bills_for_les = prepare_bills_for_les
  )
)
