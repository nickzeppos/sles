# Maine (ME) State Configuration
#
# ME uses legislature numbers (e.g., 131 = 2023_2024 term).
# Bill identifiers: paper_num (HP/SP) for bill_id, LD_num for SS joining.
# Maine is a carryover state - LD numbers are unique across the term.
# Law status determined by chapter_num column, not action patterns.
#
# Sponsor logic: primary_sponsor field, cleaned to remove title and location

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

# State configuration
me_config <- list(
  bill_types = c("HP", "SP"),

  # SS join column - ME joins SS data on LD_num, not bill_id
  # SS data has LD numbers, bill_details has both bill_id (HP/SP) and LD_num
  ss_join_column = "LD_num",

  # Term to legislature number mapping
  term_to_legislature = list(
    "2023_2024" = "131"
  ),

  # Step terms for evaluating bill history (from old script lines 395-414)
  step_terms = list(
    aic = c(
      "reported out", "otp", "ontp", "public hearing", "work session",
      "^voted$", "^voted - [a-z\\-]+$", "committee amendment",
      "divided report", "div. rep."
    ),
    abc = c(
      "reported out", "accepted report", "reports read", "engrossed",
      "enacted", "reconsidered", "removed from table", "amendment adopted",
      "first reading", "second reading", "report.+accepted",
      "consent calendar", "read a second time"
    ),
    pass = c(
      "^engrossed.+prevail", "(insisted|adhered).+engrossed.+prevails",
      "enacted.+prevail", "finally passed.+prevails",
      "passed to be engross", "passed to be enacted"
    ),
    # Law is determined by chapter_num column, not action patterns
    # This placeholder ensures the pattern matching runs but won't match
    law = c("zzzzzzzzz_placeholder_for_chapter_num_logic")
  ),

  # Session name mapping (bill data -> coded format)
  session_recode = list(
    "First Regular Session" = "RS1",
    "Second Regular Session" = "RS2",
    "First Special Session" = "SS1",
    "Second Special Session" = "SS2",
    "Third Special Session" = "SS3"
  )
)

#' Get legislature number for a term
#'
#' @param term Term string (e.g., "2023_2024")
#' @return Legislature number string (e.g., "131")
get_legislature_number <- function(term) {
  leg_num <- me_config$term_to_legislature[[term]]
  if (is.null(leg_num)) {
    # Calculate: 2023_2024 = 131, so formula is: 131 + (year - 2023) / 2
    first_year <- as.integer(strsplit(term, "_")[[1]][1])
    leg_num <- as.character(131 + (first_year - 2023) / 2)
    cli_warn(glue("Calculated legislature number for {term}: {leg_num}"))
  }
  leg_num
}

# Stage 1 Hook: Load bill files
#' Load bill files for Maine
#'
#' ME uses legislature numbers in filenames (e.g., ME_Bill_Details_131.csv).
#'
#' @param bill_dir Path to bill directory
#' @param state State code ("ME")
#' @param term Term in format "YYYY_YYYY"
#' @param verbose Show detailed logging (default TRUE)
#' @return List with bill_details and bill_history dataframes
load_bill_files <- function(bill_dir, state, term, verbose = TRUE) {
  leg_num <- get_legislature_number(term)

  # Find bill detail file
  detail_pattern <- glue("{state}_Bill_Details_{leg_num}")
  all_files <- list.files(bill_dir, full.names = TRUE)
  detail_files <- all_files[grepl(detail_pattern, all_files)]

  if (length(detail_files) == 0) {
    cli_warn(glue("No bill detail files found matching pattern: {detail_pattern}"))
    return(NULL)
  }

  if (verbose) {
    cli_log(glue("Found {length(detail_files)} bill detail files for {term}"))
  }

  # Load bill details
  bill_details <- bind_rows(lapply(detail_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read_csv(f, show_col_types = FALSE)
  }))

  # Find and load bill history files
  history_pattern <- glue("{state}_Bill_Histories_{leg_num}")
  history_files <- all_files[grepl(history_pattern, all_files)]

  if (verbose) {
    cli_log(glue("Found {length(history_files)} bill history files for {term}"))
  }

  bill_history <- bind_rows(lapply(history_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read_csv(f, show_col_types = FALSE)
  }))

  # Remove any duplicate rows
  bill_details <- distinct(bill_details)
  bill_history <- distinct(bill_history)

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 1.5 Hook: Preprocess raw data
#' Preprocess raw data for Maine
#'
#' Normalizes raw CSV data into standard schema.
#' ME uses paper_num as bill_id, keeps LD_num for SS joining.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  # Rename paper_num to bill_id (HP/SP format)
  bill_details <- bill_details %>%
    rename(bill_id = "paper_num") %>%
    mutate(
      bill_id = toupper(.data$bill_id),
      # Standardize LD_num format (ensure zero-padding)
      LD_num = ifelse(.data$LD_num == "" | is.na(.data$LD_num), NA,
                      paste0("LD", str_pad(gsub("^LD", "", .data$LD_num),
                                           4, pad = "0")))
    )

  bill_history <- bill_history %>%
    rename(bill_id = "paper_num") %>%
    mutate(bill_id = toupper(.data$bill_id))

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 2 Hook: Clean bill details
#' Clean bill details for Maine
#'
#' ME-specific transformations:
#' - Create LES_sponsor from primary_sponsor (remove title, remove "of [place]")
#' - Recode session names to standard format
#' - Filter to valid bill types (HP, SP)
#' - Filter out bills without LD_num (needed for SS joining)
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @param verbose Show detailed logging (default TRUE)
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Store unfiltered version
  all_bill_details <- bill_details

  # Recode session names to standard format (RS1, RS2, SS1, etc.)
  bill_details <- bill_details %>%
    mutate(
      session = case_when(
        grepl("First Regular", .data$session) ~ "RS1",
        grepl("Second Regular", .data$session) ~ "RS2",
        grepl("First Special", .data$session) ~ "SS1",
        grepl("Second Special", .data$session) ~ "SS2",
        grepl("Third Special", .data$session) ~ "SS3",
        TRUE ~ .data$session
      )
    )

  # Create LES_sponsor from primary_sponsor
  # Format: "Senator Eloise VITELLI of Sagadahoc" -> "vitelli"
  bill_details$LES_sponsor <- tolower(bill_details$primary_sponsor)
  bill_details$LES_sponsor <- standardize_accents(bill_details$LES_sponsor)

  # Remove "of [place]" suffix (from old script line 200)
  bill_details$LES_sponsor <- gsub(" of [a-z].+$", "", bill_details$LES_sponsor)

  # Remove title prefixes (from old script line 201)
  bill_details$LES_sponsor <- gsub(
    "representative |senator |speaker |president ", "",
    bill_details$LES_sponsor
  )

  # Trim whitespace
  bill_details$LES_sponsor <- str_trim(bill_details$LES_sponsor)

  # Add term and derive bill_type
  # Note: Use !!term to reference function parameter, not the existing column
  bill_details <- bill_details %>%
    mutate(
      term = !!term,
      # Standardize bill_id format (zero-pad to 4 digits)
      bill_id = paste0(
        gsub("[0-9]+", "", .data$bill_id),
        str_pad(gsub("^[A-Za-z]+", "", .data$bill_id), 4, pad = "0")
      ),
      bill_type = toupper(gsub("[0-9].*", "", .data$bill_id))
    )

  # Filter out resolutions by title (from old script line 163)
  # Note: Unlike other states, ME does NOT filter by bill_type here
  # because IB (Initiated Bills) etc. have LD_nums and should be included
  resolutions <- bill_details %>%
    filter(grepl("^resolution|^joint resolution|^a resolution|^a joint resolution",
                 tolower(.data$title)))

  if (nrow(resolutions) > 0) {
    if (verbose) {
      cli_log(glue("Dropping {nrow(resolutions)} resolutions by title"))
    }
    bill_details <- bill_details %>%
      filter(!grepl("^resolution|^joint resolution|^a resolution|^a joint resolution",
                    tolower(.data$title)))
  }

  if (verbose) {
    cli_log(glue("After resolution filter: {nrow(bill_details)} bills"))
  }

  # Filter out bills without LD_num (needed for SS joining)
  # From old script line 162
  bills_without_ld <- bill_details %>%
    filter(is.na(.data$LD_num))

  if (nrow(bills_without_ld) > 0) {
    if (verbose) {
      cli_log(glue("Dropping {nrow(bills_without_ld)} bills without LD_num"))
    }
    bill_details <- bill_details %>%
      filter(!is.na(.data$LD_num))
  }

  # Filter out bills without bill_id (needed for chamber derivation)
  bills_without_id <- bill_details %>%
    filter(is.na(.data$bill_id) | .data$bill_id == "")

  if (nrow(bills_without_id) > 0) {
    if (verbose) {
      cli_log(glue("Dropping {nrow(bills_without_id)} bills without bill_id"))
    }
    bill_details <- bill_details %>%
      filter(!is.na(.data$bill_id) & .data$bill_id != "")
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

# Stage 2 Hook: Clean bill history
#' Clean bill history for Maine
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  bill_history %>%
    mutate(
      term = !!term,
      # Recode session names
      session = case_when(
        grepl("First Regular", .data$session) ~ "RS1",
        grepl("Second Regular", .data$session) ~ "RS2",
        grepl("First Special", .data$session) ~ "SS1",
        grepl("Second Special", .data$session) ~ "SS2",
        grepl("Third Special", .data$session) ~ "SS3",
        TRUE ~ .data$session
      ),
      # Standardize bill_id format
      bill_id = paste0(
        gsub("[0-9]+", "", .data$bill_id),
        str_pad(gsub("^[A-Za-z]+", "", .data$bill_id), 4, pad = "0")
      ),
      # Lowercase action for pattern matching
      action = tolower(.data$action_detailed)
    )
}

# Stage 4 Hook: Post-evaluate bill (law determination via chapter_num)
#' Determine law status from chapter_num column
#'
#' Maine doesn't indicate law status in action text. Instead, if a bill
#' has a chapter number (e.g., "Chapter 98"), it became law.
#' From old script lines 451-452.
#'
#' @param bill_stages Achievement result from evaluate_bill_history
#' @param bill_row The bill row from bill_details (includes chapter_num)
#' @param bill_history Bill history dataframe (unused for ME)
#' @return Modified bill_stages with law determined from chapter_num
post_evaluate_bill <- function(bill_stages, bill_row, bill_history) {
  # Check if chapter_num indicates law (matches "chapter [0-9]+")
  chapter_num <- bill_row$chapter_num
  if (!is.na(chapter_num) && grepl("chapter [0-9]+", tolower(chapter_num))) {
    # Bill became law - set law=1 and backfill abc/pc
    bill_stages$action_beyond_comm <- 1
    bill_stages$passed_chamber <- 1
    bill_stages$law <- 1
  }
  bill_stages
}

# Stage 3 Hook: Transform SS bill IDs
#' Transform SS bill IDs for Maine
#'
#' SS data uses "LD XXXX" format. Convert to "LDXXXX" with zero-padding.
#' Also handle special session encoding (e.g., LD10001 -> LD0001).
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return SS bills with transformed bill IDs
transform_ss_bills <- function(ss_bills, term) {
  ss_bills %>%
    mutate(
      # Rename bill_id to LD_num for joining
      # SS data comes in as "LD 1610", need to convert to "LD1610" then "LD1610"
      LD_num = gsub(" ", "", .data$bill_id),
      LD_num = paste0("LD", str_pad(gsub("^LD", "", .data$LD_num), 4, pad = "0")),
      # Handle special session encoding (from old script lines 239-240)
      # LD10001 -> LD0001, LD20002 -> LD0002
      LD_num = gsub("^LD([0-9])([0-9]{4})$", "LD\\2", .data$LD_num)
    )
}

# Stage 3 Hook: Get missing SS bills
#' Identify genuinely missing SS bills vs intentionally excluded
#'
#' Maine joins SS on LD_num, not bill_id.
#' Bills are intentionally excluded if they:
#' - Have no sponsor (e.g., Initiated Bills are citizen-initiated)
#' - Are committee-sponsored
#'
#' @param ss_filtered SS bills dataframe
#' @param bill_details Filtered bill_details dataframe
#' @param all_bill_details Unfiltered bill_details dataframe
#' @return Dataframe of genuinely missing SS bills
get_missing_ss_bills <- function(ss_filtered, bill_details, all_bill_details) {
  # Find SS bills missing from bill_details (join on LD_num)
  missing_ss <- ss_filtered %>%
    anti_join(bill_details, by = c("LD_num", "term"))

  # Check if they exist in all_bill_details
  missing_in_all <- missing_ss %>%
    semi_join(all_bill_details, by = "LD_num")

  # If they exist in all_bill_details, check why they were excluded
  if (nrow(missing_in_all) > 0) {
    missing_with_sponsor <- missing_in_all %>%
      left_join(
        all_bill_details %>% select("LD_num", "primary_sponsor"),
        by = "LD_num"
      )

    # Also get title to check for resolutions
    missing_with_info <- missing_with_sponsor %>%
      left_join(
        all_bill_details %>% select("LD_num", "title"),
        by = "LD_num"
      )

    # Genuinely missing = has a sponsor AND not committee-sponsored AND not a resolution
    # Bills without sponsors (e.g., IB - Initiated Bills) are intentionally excluded
    # Resolutions are also intentionally excluded per old script line 163
    genuinely_missing <- missing_with_info %>%
      filter(
        !is.na(.data$primary_sponsor) &
        .data$primary_sponsor != "" &
        !grepl("committee", .data$primary_sponsor, ignore.case = TRUE) &
        !grepl("^resolution|^joint resolution|^a resolution|^a joint resolution",
               tolower(.data$title))
      ) %>%
      select("LD_num", "term")

    return(genuinely_missing)
  }

  # If they don't exist in all_bill_details at all, they're genuinely missing
  missing_ss %>%
    anti_join(all_bill_details, by = "LD_num") %>%
    select("LD_num", "term")
}

# Stage 4 Hook: Derive unique sponsors
#' Derive unique sponsors for Maine
#'
#' @param bills Bills dataframe with achievement metrics
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregated metrics
derive_unique_sponsors <- function(bills, term) {
  bills %>%
    mutate(chamber = ifelse(substring(.data$bill_id, 1, 1) == "H",
                            "H", "S")) %>%
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

# Stage 5 Hook: Clean sponsor names
#' Clean sponsor names for matching
#'
#' @param all_sponsors Sponsors dataframe
#' @param term Term string
#' @return Modified all_sponsors with match_name_chamber
clean_sponsor_names <- function(all_sponsors, term) {
  all_sponsors %>%
    mutate(
      match_name_chamber = paste(
        tolower(.data$LES_sponsor),
        tolower(substr(.data$chamber, 1, 1)),
        sep = "-"
      )
    )
}

# Stage 5 Hook: Adjust legiscan data
#' Adjust legiscan data for Maine
#'
#' @param legiscan Legiscan dataframe
#' @param term Term string
#' @return Modified legiscan with match_name_chamber
adjust_legiscan_data <- function(legiscan, term) {
  legiscan %>%
    filter(.data$committee_id == 0) %>%
    group_by(.data$last_name, .data$role) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      # Use full name for matching (matches old script line 651)
      match_name = .data$name,
      match_name_chamber = tolower(paste(.data$match_name,
                                         substr(.data$district, 1, 1),
                                         sep = "-"))
    )
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
  if (term == "2023_2024") {
    joined <- inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # Block wrong matches for duplicate cases (2023_2024)
        "abden simmons-h" = NA_character_,        # not aaron dana
        "clinton collamore-h" = NA_character_,    # not amanda collamore
        "matthew beck-h" = NA_character_,         # not arthur bell
        "tammy schmersal-burgess-h" = NA_character_,  # not caleb ness
        "kimberly pomerleau-h" = NA_character_,   # not cheryl golek
        "dean cray-h" = NA_character_,            # not glenn curry
        "james worth-h" = "j. worth-h",            # j. mark worth
        "joseph galletta-h" = NA_character_,      # not joseph rafferty
        "thomas lavigne-h" = NA_character_,       # not lucas lanigan
        "irene gifford-h" = NA_character_,        # not peter lyford
        "caldwell jackson-h" = NA_character_      # not troy jackson
      )
    )

    # Add back any missing sponsors
    missing_sponsors <- all_sponsors %>%
      anti_join(joined, by = "LES_sponsor")
    if (nrow(missing_sponsors) > 0) {
      joined <- bind_rows(joined, missing_sponsors)
    }

    joined
  } else {
    # Generic fuzzy matching for other terms
    joined <- inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full"
    )

    # Add back any missing sponsors
    missing_sponsors <- all_sponsors %>%
      anti_join(joined, by = "LES_sponsor")
    if (nrow(missing_sponsors) > 0) {
      joined <- bind_rows(joined, missing_sponsors)
    }

    joined
  }
}

# Stage 7 Hook: Prepare bills for LES calculation
#' Prepare bills for LES calculation
#'
#' Derives chamber from bill_id prefix (HP = H, SP = S)
#'
#' @param bills_prepared Bills dataframe
#' @return Bills with chamber column added
prepare_bills_for_les <- function(bills_prepared) {
  bills_prepared %>%
    mutate(chamber = ifelse(substring(.data$bill_id, 1, 1) == "H",
                            "H", "S"))
}

# Export configuration and hooks
c(
  me_config,
  list(
    load_bill_files = load_bill_files,
    preprocess_raw_data = preprocess_raw_data,
    clean_bill_details = clean_bill_details,
    clean_bill_history = clean_bill_history,
    post_evaluate_bill = post_evaluate_bill,
    transform_ss_bills = transform_ss_bills,
    get_missing_ss_bills = get_missing_ss_bills,
    derive_unique_sponsors = derive_unique_sponsors,
    prepare_bills_for_les = prepare_bills_for_les,
    clean_sponsor_names = clean_sponsor_names,
    adjust_legiscan_data = adjust_legiscan_data,
    reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors
  )
)
