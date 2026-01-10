# Kansas (KS) State Configuration
#
# KS has regular sessions (RS) and special sessions (SS) within each term.
# Bill numbers DO NOT restart between sessions (carryover state).
# Multiple session files per term need to be combined.
#
# Sponsor logic: Use original_sponsor if it starts with "representative" or "senator"
# Otherwise, use requested_by (first requestor if they're a member)

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
logging <- source(file.path(repo_root, "utils/logging.R"), local = TRUE)$value
libs <- source(file.path(repo_root, "utils/libs.R"), local = TRUE)$value

# Extract functions
cli_log <- logging$cli_log
cli_warn <- logging$cli_warn
require_libs <- libs$require_libs

# Load required libraries
require_libs()
library(dplyr)
library(glue)
library(stringr)
library(readr)

# State configuration
ks_config <- list(
  bill_types = c("HB", "SB"),
  # Step terms for evaluating bill history (from old script lines 419-425)
  step_terms = list(
    aic = c(
      "^hearing", "^committee report recommending",
      "^motion to withdraw from committee"
    ),
    abc = c(
      "^committee of the whole -",
      "^consent calendar"
    ),
    pass = c(
      "final action - passed", "final action - substitute passed",
      "^consent calendar passed"
    ),
    law = c(
      "^approved by governor"
    )
  )
)

# Utility: Standardize accents
standardize_accents <- function(text) {
  text <- gsub('á', 'a', text)
  text <- gsub('é', 'e', text)
  text <- gsub('ó', 'o', text)
  text <- gsub('í', 'i', text)
  text <- gsub('ñ', 'n', text)
  text
}

# Stage 1 Hook: Load bill files
#' Load bill files for Kansas
#'
#' KS has separate files for regular session (RS) and special session (SS).
#' This hook loads and combines them. Bill numbers DO NOT restart between
#' sessions (carryover state).
#'
#' @param bill_dir Path to bill directory
#' @param state State code ("KS")
#' @param term Term in format "YYYY_YYYY"
#' @param verbose Show detailed logging (default TRUE)
#' @return List with bill_details and bill_history dataframes
load_bill_files <- function(bill_dir, state, term, verbose = TRUE) {
  # Parse term years
  years <- strsplit(term, "_")[[1]]
  term_start_year <- years[1]
  term_end_year <- years[2]

  # Find all bill detail files for this term
  # Pattern matches both RS files (with full term) and SS files (year only)
  all_files <- list.files(bill_dir, full.names = TRUE)
  detail_files <- all_files[
    grepl(glue("{state}_Bill_Details"), all_files) &
    (grepl(term, all_files) |
     grepl(term_start_year, all_files) |
     grepl(term_end_year, all_files))
  ]

  if (length(detail_files) == 0) {
    cli_warn(glue(
      "No bill detail files found for term {term}"
    ))
    return(NULL)
  }

  if (verbose) {
    cli_log(glue("Found {length(detail_files)} bill detail files for {term}"))
  }

  # Load and combine bill details
  bill_details <- bind_rows(lapply(detail_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read_csv(f, show_col_types = FALSE)
  }))

  # Find and load bill history files
  history_files <- all_files[
    grepl(glue("{state}_Bill_Histories"), all_files) &
    (grepl(term, all_files) |
     grepl(term_start_year, all_files) |
     grepl(term_end_year, all_files))
  ]

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

# Stage 2 Hook: Clean bill details
#' Clean bill details for Kansas
#'
#' KS-specific transformations:
#' - Create LES_sponsor from original_sponsor or requested_by
#' - Handle "representative" and "senator" prefixes
#' - Filter to valid bill types (HB, SB)
#' - Normalize sponsor names (lowercase, accent removal)
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @param verbose Show detailed logging (default TRUE)
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Store unfiltered version
  all_bill_details <- bill_details

  # Standardize sponsors - lowercase and remove accents
  bill_details$original_sponsor <- tolower(bill_details$original_sponsor)
  bill_details$original_sponsor <- standardize_accents(
    bill_details$original_sponsor
  )

  bill_details$current_sponsor <- tolower(bill_details$current_sponsor)
  bill_details$current_sponsor <- standardize_accents(
    bill_details$current_sponsor
  )

  bill_details$requested_by <- tolower(bill_details$requested_by)
  bill_details$requested_by <- standardize_accents(bill_details$requested_by)

  # Add term and derive bill_type from bill_id
  bill_details <- bill_details %>%
    mutate(
      term = term,
      bill_type = toupper(gsub("[0-9].*", "", .data$bill_id))
    )

  # Derive LES_sponsor (from old script lines 182-220)
  bill_details <- bill_details %>%
    mutate(
      # Extract relevant requestors (first requestor if they're a member)
      relevant_requestors = ifelse(
        !grepl("^(representative|senator)", .data$original_sponsor) &
          grepl("^(representative|senator)", .data$requested_by),
        sub("^(.*?);.*", "\\1", .data$requested_by),
        NA
      ),
      # Handle "on behalf of" cases
      relevant_requestors = ifelse(
        grepl("on behalf of representative", .data$relevant_requestors),
        gsub("^.*on behalf of\\s*", "", .data$relevant_requestors),
        gsub(" on behalf of.*", "", .data$relevant_requestors)
      ),
      # Create LES_sponsor
      LES_sponsor = trimws(case_when(
        # Take first listed original sponsor if starts with rep/senator
        grepl("^(representative|senator)", .data$original_sponsor) ~
          sub("^(.*?);.*", "\\1", .data$original_sponsor),
        # Otherwise use first member requestor
        grepl("^(representative|senator)", .data$requested_by) ~
          sub("^(.*?);.*", "\\1", .data$relevant_requestors)
      )),
      # Handle multiple requestors - take first one (joint sponsor handling)
      # This improves on the original script by capturing joint sponsors that
      # would have been unmatched. Examples: "Barth and Schmoe" → "Barth",
      # "Bryce and the City" → "Bryce", "Eric Smith and X" → "Eric Smith"
      # Pattern 1: "representatives X and Y" (plural form)
      LES_sponsor = ifelse(
        grepl("^representatives ([^,]+) and ([^,]+)$", .data$LES_sponsor),
        sub("^representatives ([^,]+) and ([^,]+)$",
            "representative \\1", .data$LES_sponsor),
        gsub("representatives ([^,]+).*", "representative \\1",
             .data$LES_sponsor)
      ),
      LES_sponsor = ifelse(
        grepl("^senators ([^,]+) and ([^,]+)$", .data$LES_sponsor),
        sub("^senators ([^,]+) and ([^,]+)$", "senator \\1",
            .data$LES_sponsor),
        gsub("senators ([^,]+).*", "senator \\1", .data$LES_sponsor)
      ),
      # Pattern 2: "representative X and representative Y" (singular repeated)
      # This captures joint sponsors like "Representative Barth and Representative Schmoe"
      # We take the first sponsor, consistent with semicolon-delimited handling
      LES_sponsor = ifelse(
        grepl("^representative ([^ ]+) and representative", .data$LES_sponsor),
        sub("^representative ([^ ]+) and representative.*", "representative \\1",
            .data$LES_sponsor),
        .data$LES_sponsor
      ),
      LES_sponsor = ifelse(
        grepl("^senator ([^ ]+) and senator", .data$LES_sponsor),
        sub("^senator ([^ ]+) and senator.*", "senator \\1",
            .data$LES_sponsor),
        .data$LES_sponsor
      ),
      # Pattern 3: Handle "on behalf of" with joint sponsors
      # E.g., "Representative Blex on behalf of Representative Bryce and the City of..."
      # After "on behalf of" processing, this becomes "representative bryce and the city..."
      # We want to extract the legislator name before " and " (can be multi-word like "eric smith")
      # Pattern: ^representative (.+?) and - captures everything (non-greedy) up to " and "
      LES_sponsor = ifelse(
        grepl("^representative .+ and ", .data$LES_sponsor),
        sub("^representative (.+?) and .*", "representative \\1",
            .data$LES_sponsor),
        .data$LES_sponsor
      ),
      LES_sponsor = ifelse(
        grepl("^senator .+ and ", .data$LES_sponsor),
        sub("^senator (.+?) and .*", "senator \\1",
            .data$LES_sponsor),
        .data$LES_sponsor
      ),
      # Remove cross-chamber sponsors (HB sponsored by senator, SB by rep)
      LES_sponsor = ifelse(
        (.data$bill_type == "SB" &
           grepl("^representative", .data$LES_sponsor)) |
          (.data$bill_type == "HB" & grepl("^senator", .data$LES_sponsor)),
        NA, .data$LES_sponsor
      ),
      # Clean up relevant_requestors formatting
      relevant_requestors = gsub(" and ", "; ", .data$relevant_requestors),
      relevant_requestors = ifelse(
        grepl("^representatives", .data$relevant_requestors),
        gsub(", ", "; ", .data$relevant_requestors),
        .data$relevant_requestors
      ),
      # Remove title prefixes from sponsor names
      relevant_requestors = gsub("representative ", "",
                                  gsub("senator ", "",
                                       .data$relevant_requestors)),
      relevant_requestors = gsub("representatives ", "",
                                  gsub("senators ", "",
                                       .data$relevant_requestors)),
      LES_sponsor = gsub("representative ", "",
                         gsub("senator ", "", .data$LES_sponsor)),
      LES_sponsor = gsub("representatives ", "",
                         gsub("senators ", "", .data$LES_sponsor)),
      # Normalize variant name formats to standard "lastname, x." format
      # Some KS bills use "x. lastname" format instead of "lastname, x."
      LES_sponsor = case_when(
        .data$LES_sponsor == "eric smith" ~ "smith, e.",
        .data$LES_sponsor == "laura williams" ~ "williams, l.",
        .data$LES_sponsor == "a. smith" ~ "smith, a.",
        .data$LES_sponsor == "e. smith" ~ "smith, e.",
        .data$LES_sponsor == "b. carpenter" ~ "carpenter, b.",
        .data$LES_sponsor == "w. carpenter" ~ "carpenter, w.",
        .data$LES_sponsor == "d. miller" ~ "miller, d.",
        .data$LES_sponsor == "s. miller" ~ "miller, s.",
        .data$LES_sponsor == "v. miller" ~ "miller, v.",
        .data$LES_sponsor == "k. williams" ~ "williams, k.",
        .data$LES_sponsor == "l. williams" ~ "williams, l.",
        .data$LES_sponsor == "s. ruiz" ~ "ruiz, s.",
        .data$LES_sponsor == "l. ruiz" ~ "ruiz, l.",
        .data$LES_sponsor == "ruiz" ~ "ruiz, s.",
        .data$LES_sponsor == "wassinger" ~ "wasinger",
        .data$LES_sponsor == "clayton" ~ "sawyer clayton",
        TRUE ~ .data$LES_sponsor
      )
    )

  # Filter to valid bill types
  keep_types <- ks_config$bill_types
  bill_details <- bill_details %>%
    filter(.data$bill_type %in% keep_types)

  if (verbose) {
    cli_log(glue("After bill type filter: {nrow(bill_details)} bills"))
  }

  # Drop bills with missing/empty sponsor
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

  # Check for "by request" in sponsor names (should be handled above)
  if (any(grepl("request", bill_details$LES_sponsor, ignore.case = TRUE))) {
    cli_warn("Found 'request' in LES_sponsor - may need additional cleaning")
  }

  list(
    all_bill_details = all_bill_details,
    bill_details = bill_details
  )
}

# Stage 2 Hook: Clean bill history
#' Clean bill history for Kansas
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  # Add term column and return
  bill_history %>%
    mutate(term = term)
}

# Stage 3 Hook: Get missing SS bills
#' Identify genuinely missing SS bills vs intentionally excluded
#'
#' From old script line 267: Committee-sponsored bills are excluded
#'
#' @param ss_filtered SS bills dataframe
#' @param bill_details Filtered bill_details dataframe
#' @param all_bill_details Unfiltered bill_details dataframe
#' @return Dataframe of genuinely missing SS bills
get_missing_ss_bills <- function(ss_filtered, bill_details,
                                  all_bill_details) {
  # Find SS bills missing from bill_details
  missing_ss <- ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term"))

  # Check if they exist in all_bill_details (unfiltered)
  missing_in_all <- missing_ss %>%
    semi_join(all_bill_details, by = "bill_id")

  # If they exist in all_bill_details, check if they're committee-sponsored
  if (nrow(missing_in_all) > 0) {
    missing_with_sponsor <- missing_in_all %>%
      left_join(
        all_bill_details %>% select("bill_id", "original_sponsor"),
        by = "bill_id"
      )

    # Genuinely missing = not committee-sponsored
    genuinely_missing <- missing_with_sponsor %>%
      filter(!grepl("committee", .data$original_sponsor, ignore.case = TRUE)) %>%
      select("bill_id", "term")

    return(genuinely_missing)
  }

  # If they don't exist in all_bill_details at all, they're genuinely missing
  missing_ss %>%
    anti_join(all_bill_details, by = "bill_id") %>%
    select("bill_id", "term")
}

# Stage 4 Hook: Derive unique sponsors
#' Derive unique sponsors for Kansas
#'
#' From old script lines 518-528: Group by sponsor/chamber/term
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

# Stage 7 Hook: Prepare bills for LES calculation
#' Prepare bills for LES calculation
#'
#' Derives chamber from bill_id prefix (HB = H, SB = S)
#'
#' @param bills_prepared Bills dataframe
#' @return Bills with chamber column added
prepare_bills_for_les <- function(bills_prepared) {
  bills_prepared %>%
    mutate(chamber = ifelse(substring(.data$bill_id, 1, 1) == "H",
                            "H", "S"))
}

# Stage 5 Hook: Clean sponsor names
#' Clean sponsor names for matching
#'
#' Normalizes name variants and creates match keys for fuzzy joining.
#' KS bill data has inconsistent formats for legislators with common last names:
#' - Standard: "smith, a." (lastname, initial.)
#' - Variant: "a. smith" (initial. lastname)
#' - Full name: "eric smith", "laura williams"
#' This function normalizes all to standard format before creating match keys.
#'
#' @param all_sponsors Sponsors dataframe
#' @param term Term string
#' @return Modified all_sponsors with match_name_chamber
clean_sponsor_names <- function(all_sponsors, term) {
  # Name normalization is now done in clean_bill_details
  # Just create the match key here
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
#' Adjust legiscan data for Kansas
#'
#' Creates match keys for fuzzy joining
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
      match_name = ifelse(.data$n >= 2,
                          glue("{substr(first_name, 1, 1)}. {last_name}"),
                          .data$last_name),
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
        # Format differences: legiscan "x. lastname" -> bills "lastname, x."
        "b. carpenter-h" = "carpenter, b.-h",
        "w. carpenter-h" = "carpenter, w.-h",
        "l. ruiz-h" = "ruiz, l.-h",
        "s. ruiz-h" = "ruiz, s.-h",
        "a. smith-h" = "smith, a.-h",
        "e. smith-h" = "smith, e.-h",
        "c. smith-h" = "smith, c.-h",
        "d. miller-h" = "miller, d.-h",
        "s. miller-h" = "miller, s.-h",
        "v. miller-h" = "miller, v.-h",
        "k. williams-h" = "williams, k.-h",
        "l. williams-h" = "williams, l.-h",

        # Name variations
        "arnberger-blew-h" = "blew-h",  # Tory Arnberger-Blew -> Blew
        "sawyer-clayton-h" = "sawyer clayton-h",  # hyphenated vs space

        # Block incorrect fuzzy matches
        "bloom-h" = NA_character_,  # was matching "blew"
        "shultz-h" = NA_character_,  # was matching "goetz"
        "weigel-h" = NA_character_,  # was matching "eplee"
        "poetter parshall-h" = NA_character_,  # was matching "petersen"
        "gossage-s" = NA_character_,  # was matching "corson"
        "borjon-h" = NA_character_,  # was matching "corson"
        "haskins-h" = NA_character_,  # was matching "hawkins"
        "white-h" = NA_character_,  # was matching "hill"
        "hougland-h" = NA_character_,  # was matching "holland"
        "robinson-h" = NA_character_,  # was matching "johnson"
        "anderson-h" = NA_character_,  # was matching "sanders"
        "neighbor-h" = NA_character_,  # was matching "bergkamp"
        "bergquist-h" = NA_character_,  # was matching "bergkamp"
        "amyx-h" = NA_character_,  # was matching "blex"
        "droge-h" = NA_character_,  # was matching "bryce"
        "ryckman-s" = NA_character_,  # was matching "erickson"
        "stiens-h" = NA_character_,  # was matching "estes"
        "boyd-h" = NA_character_,  # was matching "hoye"
        "ware-s" = NA_character_  # was matching "warren"
      )
    )

    # inexact_join drops y-side rows that don't match - add them back
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

# Export configuration and hooks
c(
  ks_config,
  list(
    load_bill_files = load_bill_files,
    clean_bill_details = clean_bill_details,
    clean_bill_history = clean_bill_history,
    get_missing_ss_bills = get_missing_ss_bills,
    derive_unique_sponsors = derive_unique_sponsors,
    prepare_bills_for_les = prepare_bills_for_les,
    clean_sponsor_names = clean_sponsor_names,
    adjust_legiscan_data = adjust_legiscan_data,
    reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors
  )
)
