# Wisconsin State Configuration
# State-specific constants and settings for LES estimation

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}

# Load string utilities
strings <- source(file.path(repo_root, "utils/strings.R"), local = TRUE)$value
standardize_accents <- strings$standardize_accents

# Load dplyr for pipe operations
library(dplyr)

# nolint start: line_length_linter
wi_config <- list(
  # Valid bill types for this state
  bill_types = c("AB", "SB"),

  # Step terms for evaluating bill history
  # Used to determine achievement levels: aic, abc, pass, law
  step_terms = list(
    aic = c(
      "public hearing held", "recommended by.+comm",
      "amend.+offered.+committee", "estimate received",
      "executive action taken",
      "placed on calendar [^\\s]+ by committee"
    ),
    abc = c(
      "^report", "placed on cal.+ by rules", "read a second time",
      "read a third time", "second reading", "third reading",
      "^failed to pass.+gov", "^referred to cal", "rules suspended"
    ),
    pass = c("^read.+ passed", "^passed", "enrolled"),
    law = c("approved by the gov", "^published", "wisconsin act [0-9]+")
  )
)
# nolint end: line_length_linter

# Stage 1.5 Hook: Preprocess raw data to normalize column names

#' Preprocess raw data for Wisconsin
#'
#' Normalizes raw CSV data into standard schema expected by generic pipeline.
#' Handles WI-specific column naming conventions.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string (unused but provided for consistency)
#' @return List with preprocessed bill_details and bill_history
preprocess_raw_data <- function(bill_details, bill_history, term) {
  # WI uses "bill_number" in raw data, rename to "bill_id"
  bill_details <- bill_details %>%
    rename(bill_id = "bill_number") %>%
    mutate(bill_id = toupper(.data$bill_id))

  bill_history <- bill_history %>%
    rename(bill_id = "bill_number") %>%
    mutate(bill_id = toupper(.data$bill_id))

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Helper functions for WI-specific cleaning

#' Fix sponsor names for specific WI terms
#'
#' Handles term-specific name corrections and standardizations.
#'
#' @param names Vector of sponsor names to fix
#' @param term Term string (e.g., "2023_2024")
#' @return Vector of corrected names
fix_names <- function(names, term) {
  if (term == "2023_2024") {
    names <- stringr::str_replace_all(names, "j. rodriguez", "jessie")
    names <- stringr::str_replace_all(names, "r. brooks", "robert brooks")
  }

  names
}

#' Fix bill IDs for special session bills
#'
#' Handles WI special session bill ID formatting.
#'
#' @param bill_ids Vector of bill IDs to fix
#' @param term Term string (e.g., "2023_2024")
#' @return Vector of corrected bill IDs
fix_bill_ids <- function(bill_ids, term) {
  if (term == "2023_2024") {
    bill_ids <- stringr::str_replace_all(
      bill_ids, "ss1_ab1", "ss1ab1"
    )
    bill_ids <- stringr::str_replace_all(
      bill_ids, "ss1_sb1", "ss1sb1"
    )
  }

  bill_ids
}

# State-specific hook functions

#' Clean bill details for Wisconsin
#'
#' WI-specific transformations for bill details:
#' - Create LES_sponsor from primary_sponsor (lowercase, trim periods)
#' - Derive bill_type from bill_id and filter to valid types
#' - Handle multiple sponsors (keep first only)
#' - Apply term-specific name corrections
#' - Fix special session bill IDs
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term) {
  # Store unfiltered version
  all_bill_details <- bill_details

  # Create LES_sponsor column (lowercase, remove trailing periods, standardize)
  bill_details$LES_sponsor <- tolower(bill_details$primary_sponsor)
  bill_details$LES_sponsor <- stringr::str_remove(
    bill_details$LES_sponsor, "\\.$"
  )
  bill_details$LES_sponsor <- standardize_accents(bill_details$LES_sponsor)

  # Add term, session and derive bill_type from bill_id
  bill_details <- bill_details %>%
    mutate(
      term = term,
      session = .data$session_type,
      bill_type = toupper(gsub("[0-9].*", "", .data$bill_id))
    )

  # Filter to valid bill types
  keep_types <- wi_config$bill_types
  bill_details <- bill_details %>%
    filter(.data$bill_type %in% keep_types)

  # Handle multiple sponsors - keep first only
  bill_details$LES_sponsor <- stringr::str_remove(
    bill_details$LES_sponsor, " and .*"
  )

  # Apply term-specific name fixes
  bill_details$LES_sponsor <- fix_names(bill_details$LES_sponsor, term)

  # Fix bill IDs for special sessions
  bill_details$bill_id <- fix_bill_ids(bill_details$bill_id, term)

  # Drop committee-sponsored bills
  comm_bills <- bill_details %>%
    filter(grepl(
      "committee|joint legislative council|information policy",
      .data$LES_sponsor
    ))

  if (nrow(comm_bills) > 0) {
    cli_log(glue("Dropping {nrow(comm_bills)} committee-sponsored bills"))
    bill_details <- bill_details %>%
      filter(!grepl(
        "committee|joint legislative council|information policy",
        .data$LES_sponsor
      ))
  }

  # Drop bills with missing/empty sponsor
  empty_sponsor_bills <- bill_details %>%
    filter(.data$LES_sponsor == "" | is.na(.data$LES_sponsor))

  if (nrow(empty_sponsor_bills) > 0) {
    cli_log(glue("Dropping {nrow(empty_sponsor_bills)} bills without sponsor"))
    bill_details <- bill_details %>%
      filter(!(.data$LES_sponsor == "" | is.na(.data$LES_sponsor)))
  }

  list(
    all_bill_details = all_bill_details,
    bill_details = bill_details
  )
}

#' Clean bill history for Wisconsin
#'
#' WI-specific transformations for bill history:
#' - Recode chamber names (Asm. -> House, Sen. -> Senate)
#' - Filter out problematic actions (survey committee)
#' - Lowercase actions for matching
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  bill_history %>%
    mutate(
      term = term,
      session = .data$session_type,
      chamber = recode(.data$chamber, "Asm." = "House", "Sen." = "Senate"),
      action = tolower(.data$action)
    ) %>%
    filter(!grepl("survey committee", .data$action)) %>%
    select(-"session_type")
}

#' Derive unique sponsors from bills
#'
#' Stage 5 hook: Extracts unique sponsor set from bills data with aggregate
#' statistics (num_sponsored_bills, pass rate, law rate).
#'
#' @param bills Dataframe of bills with achievement columns
#' @param term Term string (e.g., "2023_2024")
#' @return Dataframe of unique sponsors with aggregate stats
derive_unique_sponsors <- function(bills, term) {
  bills %>%
    mutate(chamber = ifelse(substring(.data$bill_id, 1, 1) == "A",
      "H", "S"
    )) %>%
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

#' Compute cosponsorship counts
#'
#' Stage 5 hook: Counts cosponsored bills per legislator. Includes disjoint
#' name set validation to ensure primary sponsors and cosponsors don't overlap.
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param bills Dataframe of bills with cosponsors column
#' @return Dataframe of sponsors with num_cosponsored_bills added
compute_cosponsorship <- function(all_sponsors, bills) {
  # Initialize count column
  all_sponsors$num_cosponsored_bills <- NA
  adjustment_needed <- TRUE

  while (adjustment_needed) {
    cli_log <- logging$cli_log
    cli_warn <- logging$cli_warn
    cli_error <- logging$cli_error

    cli_log("Ensuring primary and cosponsor columns are disjoint name sets.")

    # Calculate cosponsorship counts
    for (i in seq_len(nrow(all_sponsors))) {
      chamber_prefix <- ifelse(all_sponsors[i, ]$chamber == "H", "A", "S")
      c_sub <- filter(bills, substring(.data$bill_id, 1, 1) == chamber_prefix)
      all_sponsors$num_cosponsored_bills[i] <- sum(
        grepl(all_sponsors[i, ]$LES_sponsor, tolower(c_sub$cosponsors))
      )
    }

    # Check for negative counts (indicates overlap between primary and cosponsor)
    if (min(all_sponsors$num_cosponsored_bills) < 0) {
      cli_warn("Negative cosponsor counts found.")
      cli_warn("This suggests cosponsor and primary sponsor columns overlap.")
      cli_warn("Applying adjustment...")

      # Apply adjustment (subtract sponsored from cosponsored)
      all_sponsors$num_cosponsored_bills <- all_sponsors$num_cosponsored_bills -
        all_sponsors$num_sponsored_bills

      # Check if adjustment fixed the issue
      if (min(all_sponsors$num_cosponsored_bills) >= 0) {
        cli_log("Adjustment successful - no negative counts remain.")
        adjustment_needed <- FALSE
      } else {
        cli_error("Adjustment failed - still have negative counts.")
        cli_error(paste(
          "Problem sponsors:",
          nrow(filter(
            all_sponsors,
            .data$num_cosponsored_bills < 0
          ))
        ))
        stop("Cosponsorship adjustment failed")
      }
    } else {
      cli_log("No negative cosponsor counts found, disjoint assumption valid.")
      adjustment_needed <- FALSE
    }
  }

  all_sponsors
}

#' Clean sponsor names for matching
#'
#' Stage 5 hook: Applies term-specific name corrections and prepares sponsor
#' names for matching with legiscan data.
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param term Term string (e.g., "2023_2024")
#' @return Dataframe with match_name_chamber column added
clean_sponsor_names <- function(all_sponsors, term) {
  # Extract last_name and first_name
  all_sponsors <- all_sponsors %>%
    mutate(
      last_name = gsub(" .+", "", .data$LES_sponsor),
      first_name = ifelse(!grepl(" ", .data$LES_sponsor), NA,
        gsub(".+ ", "", .data$LES_sponsor)
      )
    )

  # Term-specific name fixes
  if (term == "1999_2000") {
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "roessler",
      "buettner", all_sponsors$last_name
    )
  }
  if (term %in% c("2001_2002")) {
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "starzyk",
      "kerkman", all_sponsors$last_name
    )
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "wade",
      "spillner", all_sponsors$last_name
    )
  }
  if (term %in% c("2003_2004")) {
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "morris",
      "morris-tatum", all_sponsors$last_name
    )
  }
  if (term %in% c("2009_2010", "2011_2012", "2013_2014")) {
    all_sponsors$last_name <- ifelse(
      all_sponsors$LES_sponsor == "bernard schaber",
      "schaber", all_sponsors$last_name
    )
  }
  if (term %in% c("2013_2014", "2015_2016", "2017_2018")) {
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "pope",
      "pope-roberts", all_sponsors$last_name
    )
  }
  if (term == "2015_2016") {
    all_sponsors$last_name <- ifelse(
      all_sponsors$LES_sponsor == "harris dodd",
      "harris", all_sponsors$last_name
    )
  }
  if (term == "2017_2018") {
    all_sponsors$last_name <- ifelse(
      all_sponsors$LES_sponsor == "felzkowski",
      "czaja", all_sponsors$last_name
    )
  }

  # Create match_name_chamber for fuzzy matching
  all_sponsors <- all_sponsors %>%
    arrange(.data$chamber, .data$LES_sponsor) %>%
    distinct() %>%
    mutate(
      match_name_chamber = tolower(paste(
        stringr::str_remove_all(.data$LES_sponsor, '"\\s*.*?\\s*"'),
        substr(.data$chamber, 1, 1),
        sep = "-"
      ))
    ) %>%
    select(-c("last_name", "first_name"))

  all_sponsors
}

#' Adjust legiscan data for matching
#'
#' Stage 5 hook: Applies term-specific adjustments to legiscan roster data.
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string (e.g., "2023_2024")
#' @return Adjusted legiscan dataframe with match_name_chamber column
adjust_legiscan_data <- function(legiscan, term) {
  # Term-specific adjustment for 2021_2022
  if (term == "2021_2022") {
    legiscan <- bind_rows(
      legiscan,
      legiscan %>%
        filter(.data$people_id == 15094) %>%
        mutate(role = "Sen", district = "SD-013")
    ) %>%
      filter(!.data$name %in% c("Melissa Agard", "Nancy Vander Meer"))
  }

  # Clean up data for matching
  legiscan_adj <- legiscan %>%
    filter(.data$committee_id == 0) %>%
    group_by(.data$last_name, .data$role) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      match_name = ifelse(.data$n >= 2,
        glue("{substr(first_name, 1, 1)}. {last_name}"),
        .data$last_name
      ),
      match_name_chamber = tolower(paste(.data$match_name,
        substr(.data$district, 1, 1),
        sep = "-"
      ))
    )

  legiscan_adj
}

#' Reconcile legiscan with sponsors
#'
#' Stage 5 hook: Performs fuzzy matching between legiscan records and bill
#' sponsors using term-specific custom matching rules.
#'
#' @param sponsors Dataframe of unique sponsors
#' @param legiscan Adjusted legiscan dataframe
#' @param term Term string (e.g., "2023_2024")
#' @return Joined dataframe of sponsors matched to legiscan records
reconcile_legiscan_with_sponsors <- function(sponsors, legiscan, term) {
  # Term-specific custom matches
  if (term == "2019_2020") {
    all_sponsors_2 <- inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "barca-h" = NA_character_
      )
    )
  } else if (term == "2021_2022") {
    all_sponsors_2 <- inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "d. hesselbein-s" = NA_character_,
        "m. spreitzer-s" = NA_character_,
        "j. james-s" = NA_character_,
        "r. cabral-guevara-s" = NA_character_,
        "taylor-s" = "l. taylor-s",
        "jagler-h" = NA_character_
      )
    )
  } else if (term == "2023_2024") {
    all_sponsors_2 <- inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full"
      # No custom matches needed for 2023_2024
    )
  } else {
    # Default: no custom matches
    all_sponsors_2 <- inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full"
    )
  }

  all_sponsors_2
}

# Export config and functions
# Flatten config to top level for loader validation
list(
  bill_types = wi_config$bill_types,
  step_terms = wi_config$step_terms,
  preprocess_raw_data = preprocess_raw_data,
  clean_bill_details = clean_bill_details,
  clean_bill_history = clean_bill_history,
  derive_unique_sponsors = derive_unique_sponsors,
  compute_cosponsorship = compute_cosponsorship,
  clean_sponsor_names = clean_sponsor_names,
  adjust_legiscan_data = adjust_legiscan_data,
  reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors
)
