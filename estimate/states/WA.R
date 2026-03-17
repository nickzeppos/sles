# Washington State Configuration
# State-specific constants and settings for LES estimation
#
# WA notes from old script:
# - Bill IDs are unique for the two-year term (biennium)
# - Bills carry over from regular to special sessions via resolution
# - Single consolidated session per biennium
# - Sponsor format: "Last, First" (comma-separated)
# - Has cosponsorship data (semicolon-separated)
# - E suffix on bill_id = engrossed; 2E = second engrossed

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
wa_config <- list(
  # Valid bill types for this state
  bill_types = c("HB", "SB"),

  # Sponsor patterns to drop from analysis
  drop_sponsor_pattern = "committee",

  # Step terms for evaluating bill history (from old script lines 342-352)
  step_terms = list(
    aic = c(
      "committee_", "^minority", "do pass", "do not pass",
      "without recommendation", "do confirm", "committee amendment",
      "public hearing", "executive session", "executive action taken"
    ),
    abc = c(
      "committee_.+majority", "^passed to rules committee",
      "referred to rules 2 review", "second reading", "third reading",
      "committee amend.+adopted", "^amended\\."
    ),
    pass = c("third reading, passed"),
    law = c(
      "governor signed", "governor partially vetoed",
      "^chapter [0-9]+", "199[0-9] laws", "20[0-9][0-9] laws"
    )
  )
)
# nolint end: line_length_linter

#' Check if a bill should be dropped based on sponsor
#'
#' @param sponsor Sponsor name
#' @return TRUE if bill should be dropped, FALSE otherwise
should_drop_bill <- function(sponsor) {
  grepl(wa_config$drop_sponsor_pattern, sponsor, ignore.case = TRUE)
}

#' Get file suffix for bill files
#'
#' WA uses full term for file naming (WA_Bill_Details_2023_2024.csv).
#'
#' @param term Term string (e.g., "2023_2024")
#' @return The suffix to use for bill file names
get_bill_file_suffix <- function(term) {
  term
}

# Stage 1.5 Hook: Preprocess raw data
#' Preprocess raw data for Washington
#'
#' Normalizes raw CSV data into standard schema:
#' - Rename bill_number -> bill_id
#' - Convert term "2023-2024" -> "2023_2024"
#' - Set session = term (single biennium session)
#' - Remove engrossed (E/2E) suffixes from bill_id
#' - Clean companion field E prefix
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  # Rename bill_number to bill_id
  bill_details <- bill_details %>%
    rename(bill_id = "bill_number")
  bill_history <- bill_history %>%
    rename(bill_id = "bill_number")

  # Convert term format: "2023-2024" -> "2023_2024"
  bill_details$term <- gsub("-", "_", bill_details$term)
  bill_history$term <- gsub("-", "_", bill_history$term)

  # Add session = term (single biennium session)
  bill_details$session <- bill_details$term
  bill_history$session <- bill_history$term

  # Remove engrossed suffixes from bill_id
  bill_details$bill_id <- gsub("2E", "", bill_details$bill_id)
  bill_details$bill_id <- gsub("E", "", bill_details$bill_id)
  bill_history$bill_id <- gsub("2E", "", bill_history$bill_id)
  bill_history$bill_id <- gsub("E", "", bill_history$bill_id)

  # Clean companion field: remove leading E prefix
  if ("companion" %in% names(bill_details)) {
    bill_details$companion[is.na(bill_details$companion)] <- ""
    has_e <- startsWith(bill_details$companion, "E")
    bill_details$companion[has_e] <- substr(
      bill_details$companion[has_e], 2,
      nchar(bill_details$companion[has_e])
    )
  }

  # Remove duplicates
  bill_details <- distinct(bill_details)
  bill_history <- distinct(bill_history)

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

#' Clean bill details for Washington
#'
#' WA-specific transformations:
#' - Filter to bill_type "Bill" (exclude Gubernatorial Appointments, etc.)
#' - Derive bill_type from bill_id prefix, filter to HB/SB
#' - Lowercase + accent removal on primary_sponsor and cosponsors
#' - LES_sponsor = primary_sponsor with quoted text removed
#' - Check for by-request bills
#' - Drop empty/NA sponsors
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @param verbose Show detailed logging (default TRUE)
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Store unfiltered version (before any filtering)
  all_bill_details <- bill_details

  # Filter to bill_type "Bill" (from old script line 150: keep_types = "Bill")
  bill_details <- bill_details %>%
    filter(.data$bill_type == "Bill")

  # Derive bill_type from bill_id prefix and filter to HB/SB
  bill_details <- bill_details %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    ) %>%
    filter(.data$bill_type %in% wa_config$bill_types) %>%
    select(-"bill_type")

  # Lowercase and accent removal on primary_sponsor
  bill_details$primary_sponsor <- tolower(bill_details$primary_sponsor)
  bill_details$primary_sponsor <- standardize_accents(
    bill_details$primary_sponsor
  )

  # Lowercase and accent removal on cosponsors
  bill_details$cosponsors <- tolower(bill_details$cosponsors)
  bill_details$cosponsors <- standardize_accents(bill_details$cosponsors)

  # LES_sponsor = primary_sponsor with quoted text removed
  bill_details$LES_sponsor <- str_trim(
    gsub('\\"[^"]+\\"', "", bill_details$primary_sponsor)
  )

  # Also clean quoted text from cosponsors
  bill_details$cosponsors <- str_trim(
    gsub('\\"[^"]+\\"', "", bill_details$cosponsors)
  )

  # Check for by-request bills
  if (any(grepl("request", bill_details$LES_sponsor))) {
    cli_warn("BY REQUEST BILLS detected -- review needed")
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

  list(
    all_bill_details = all_bill_details,
    bill_details = bill_details
  )
}

#' Clean bill history for Washington
#'
#' WA-specific transformations:
#' - Recode chamber: O->Governor, E->derive from bill_id prefix,
#'   H->House, S->Senate, CC->Conference
#' - Prepend "Committee_" to actions from named committees
#' - Trim whitespace, lowercase actions
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  # Recode O to Governor
  bill_history$chamber <- ifelse(
    bill_history$chamber == "O", "Governor", bill_history$chamber
  )

  # E = engrossing chamber, derive from bill_id prefix
  bill_history <- bill_history %>%
    mutate(chamber = case_when(
      .data$chamber == "E" & grepl("HB", .data$bill_id) ~ "House",
      .data$chamber == "E" & grepl("SB", .data$bill_id) ~ "Senate",
      TRUE ~ .data$chamber
    ))

  # Recode remaining single-letter codes
  bill_history$chamber <- recode(
    bill_history$chamber,
    "H" = "House", "S" = "Senate",
    "G" = "Governor", "CC" = "Conference"
  )

  # Prepend "Committee_" to actions from named committees
  # Pattern: actions starting with 2+ uppercase letters followed by " - "
  bill_history$action <- ifelse(
    grepl("^[A-Z][A-Z]+ - ", bill_history$action),
    paste0("Committee_", bill_history$action),
    bill_history$action
  )

  # Trim whitespace
  bill_history$action <- str_trim(bill_history$action)

  # Order by term, bill_id, order
  bill_history <- bill_history %>%
    arrange(.data$term, .data$bill_id, .data$order)

  # Lowercase action for pattern matching
  bill_history$action <- tolower(bill_history$action)

  bill_history
}

#' Get missing SS bills for Washington
#'
#' WA-specific: also checks companion field. If an SS bill_id matches
#' a companion in bill_details, it's not truly missing (the companion
#' bill is carrying the legislation forward).
#'
#' @param ss_filtered Filtered SS bills for this term
#' @param bill_details Cleaned bill details (after filtering)
#' @param all_bill_details All bill details (before filtering)
#' @return Dataframe of genuinely missing SS bills (bill_id, term)
get_missing_ss_bills <- function(ss_filtered, bill_details,
                                 all_bill_details) {
  # Anti-join SS to bill_details by (bill_id, term)
  missing <- ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term"))

  # Also check companion field -- WA uses companion bills
  if ("companion" %in% names(bill_details)) {
    missing <- missing %>%
      anti_join(bill_details, by = c("bill_id" = "companion", "term"))
  }

  # Join to all_bill_details for sponsor info
  missing <- missing %>%
    left_join(
      all_bill_details %>% select("bill_id", "term", "primary_sponsor"),
      by = c("bill_id", "term")
    )

  # Filter to HB/SB only
  missing <- missing %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    ) %>%
    filter(.data$bill_type %in% c("HB", "SB")) %>%
    select(-"bill_type")

  # Exclude committee-sponsored
  missing <- missing %>%
    filter(!grepl("committee", .data$primary_sponsor, ignore.case = TRUE))

  # Return just bill_id and term for the assertion
  missing %>%
    select("bill_id", "term")
}

#' Derive unique sponsors from bills
#'
#' Stage 5 hook: Extracts unique sponsor set from bills data with aggregate
#' statistics. Also adds cosponsor-only legislators from cosponsors field.
#'
#' @param bills Dataframe of bills with achievement columns
#' @param term Term string (e.g., "2023_2024")
#' @return Dataframe of unique sponsors with aggregate stats
derive_unique_sponsors <- function(bills, term) {
  # Primary sponsors
  all_sponsors <- bills %>%
    mutate(
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

  # Add cosponsor-only legislators (from old script lines 458-468)
  unique_cospon <- str_trim(unique(unlist(str_split(
    bills$cosponsors, "; "
  ))))
  for (nonspon in unique_cospon) {
    if (is.na(nonspon) || nonspon == "") next
    if (nonspon %in% all_sponsors$LES_sponsor) next

    chamb <- unique(substring(
      bills[grepl(nonspon, bills$cosponsors), ]$bill_id, 1, 1
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

#' Compute cosponsorship counts
#'
#' Stage 5 hook: Counts cosponsored bills per legislator.
#' Uses cospon_match = paste(LES_sponsor, primary_sponsor, cosponsors)
#' then subtracts own sponsored bills.
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param bills Dataframe of bills with cosponsors column
#' @return Dataframe of sponsors with num_cosponsored_bills added
compute_cosponsorship <- function(all_sponsors, bills) {
  # Build cosponsor match column (from old script line 472)
  bills$cospon_match <- paste(
    bills$LES_sponsor, bills$primary_sponsor, bills$cosponsors,
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

#' Clean sponsor names for matching
#'
#' Stage 5 hook: Prepares sponsor names for fuzzy matching with legiscan.
#' WA uses full LES_sponsor + chamber for matching.
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param term Term string (e.g., "2023_2024")
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

#' Adjust legiscan data for matching
#'
#' Stage 5 hook: Prepares legiscan records for fuzzy matching.
#' WA uses "last_name, first_name" format for match_name.
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string (e.g., "2023_2024")
#' @return Adjusted legiscan dataframe with match_name_chamber column
adjust_legiscan_data <- function(legiscan, term) {
  # Term-specific enrichments (from old script lines 526-537)
  if (term == "2019_2020") {
    legiscan <- bind_rows(
      legiscan,
      legiscan %>%
        filter(.data$people_id == 10541) %>%
        mutate(role = "Rep", district = "HD-001"),
      legiscan %>%
        filter(.data$people_id == 16132) %>%
        mutate(role = "Rep", district = "HD-038")
    )
  }

  if (term == "2021_2022") {
    legiscan <- legiscan %>%
      mutate(
        district = ifelse(
          .data$people_id == 20686, "HD-008", .data$district
        )
      ) %>%
      bind_rows(
        legiscan %>%
          filter(.data$people_id == 18283) %>%
          mutate(role = "Sen", district = "SD-044")
      )
  }

  # Chamber switcher: Hansen served in House (HD-023B) in 2023,
  # appointed to Senate (SD-023) Aug 2023
  if (term == "2023_2024") {
    hansen <- legiscan %>%
      filter(.data$people_id == 13683)
    if (nrow(hansen) > 0) {
      legiscan <- bind_rows(
        legiscan,
        hansen %>% mutate(role = "Sen", district = "SD-023")
      )
    }
  }

  legiscan %>%
    filter(.data$committee_id == 0) %>%
    mutate(
      match_name = glue("{last_name}, {first_name}"),
      match_name_chamber = tolower(paste(
        .data$match_name,
        substr(.data$district, 1, 1),
        sep = "-"
      ))
    )
}

#' Reconcile legiscan with sponsors
#'
#' Stage 5 hook: Fuzzy matching between legiscan and bill sponsors.
#' Term-specific custom_match blocks handle name variants.
#'
#' @param sponsors Dataframe of unique sponsors
#' @param legiscan Adjusted legiscan dataframe
#' @param term Term string (e.g., "2023_2024")
#' @return Joined dataframe of sponsors matched to legiscan records
# nolint start: line_length_linter
reconcile_legiscan_with_sponsors <- function(sponsors, legiscan, term) {
  if (term == "2019_2020") {
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "graham, virginia-h" = "graham, jenny-h",
        "santos, sharon-h" = "santos, sharon tomiko-h"
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
        "wilcox, james-h" = NA_character_,
        "santos, sharon-h" = "santos, sharon tomiko-h",
        "jinkins, laurie-h" = NA_character_
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
        # Jinkins is Speaker (zero bills) - block from matching davis
        "jinkins, laurie-h" = NA_character_,
        # Hale is zero-bill - block from matching rule
        "hale, lilian-h" = NA_character_,
        # Santos goes by middle name "Sharon Tomiko" in sponsor data
        "santos, sharon-h" = "santos, sharon tomiko-h"
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
      chamber = ifelse(substring(.data$bill_id, 1, 1) == "H", "H", "S")
    )
}

# Export config and functions
list(
  bill_types = wa_config$bill_types,
  step_terms = wa_config$step_terms,
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
