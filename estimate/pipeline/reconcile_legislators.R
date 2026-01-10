# Pipeline Stage 5: Reconcile Legislators
# Matches bill sponsors with official legiscan legislator records

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
logging <- source(file.path(repo_root, "utils/logging.R"),
                  local = TRUE)$value
libs <- source(file.path(repo_root, "utils/libs.R"), local = TRUE)$value
assertions <- source(file.path(repo_root, "estimate/lib/assertions.R"),
                     local = TRUE)$value

# Extract functions
cli_log <- logging$cli_log
cli_warn <- logging$cli_warn
cli_error <- logging$cli_error
require_libs <- libs$require_libs
assert_no_unmatched_legiscan <- assertions$assert_no_unmatched_legiscan_records
assert_no_unmatched_sponsors <- assertions$assert_no_unmatched_bill_sponsors

# Load required libraries
require_libs()
library(dplyr)
library(glue)

#' Reconcile legislators for estimation
#'
#' Joins achievement matrix to bill details, derives unique sponsors,
#' matches them with legiscan data, and validates the reconciliation.
#'
#' @param data List from compute_achievement containing dataframes
#' @param state State postal code
#' @param term Term string (e.g., "2023_2024")
#' @return List containing data with legis_data added
reconcile_legislators <- function(data, state, term) {
  cli_log("Reconciling legislators with bill sponsors...")

  # Join achievement matrix to bill details to create unified bills dataset
  bills <- left_join(
    data$bill_details,
    data$leg_achievement_matrix,
    by = intersect(colnames(data$bill_details),
                    colnames(data$leg_achievement_matrix))
  )

  # Get state-specific functions
  state_config <- data$state_config

  # Derive unique sponsors (STATE-SPECIFIC)
  if (!is.null(state_config$derive_unique_sponsors)) {
    all_sponsors <- state_config$derive_unique_sponsors(bills, term)
  } else {
    # Generic implementation
    all_sponsors <- bills %>%
      mutate(chamber = ifelse(substring(.data$bill_id, 1, 1) == "A",
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

  # Compute cosponsorship (STATE-SPECIFIC)
  if (!is.null(state_config$compute_cosponsorship)) {
    all_sponsors <- state_config$compute_cosponsorship(all_sponsors, bills)
  } else {
    # Generic: set to NA
    all_sponsors$num_cosponsored_bills <- NA
  }

  # Clean sponsor names (STATE-SPECIFIC)
  if (!is.null(state_config$clean_sponsor_names)) {
    all_sponsors <- state_config$clean_sponsor_names(all_sponsors, term)
  } else {
    # Generic cleaning
    all_sponsors <- all_sponsors %>%
      mutate(
        match_name_chamber = tolower(paste(
          stringr::str_remove_all(.data$LES_sponsor, '"\\s*.*?\\s*"'),
          substr(.data$chamber, 1, 1),
          sep = "-"
        ))
      )
  }

  # Adjust legiscan data (STATE-SPECIFIC)
  if (!is.null(state_config$adjust_legiscan_data)) {
    legiscan_adjusted <- state_config$adjust_legiscan_data(data$legiscan, term)
  } else {
    # Generic cleaning
    legiscan_adjusted <- data$legiscan %>%
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

  # Reconcile legiscan with sponsors (STATE-SPECIFIC)
  if (!is.null(state_config$reconcile_legiscan_with_sponsors)) {
    all_sponsors_2 <- state_config$reconcile_legiscan_with_sponsors(
      all_sponsors, legiscan_adjusted, term
    )
  } else {
    # Generic fuzzy matching
    all_sponsors_2 <- inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full"
    )
  }

  # Get verbose flag from data
  verbose <- if (!is.null(data$verbose)) data$verbose else TRUE

  # Validate reconciliation
  # Check for unmatched legiscan (in legiscan but not in bills)
  unmatched_legiscan <- all_sponsors_2 %>%
    filter(is.na(.data$LES_sponsor)) %>%
    select("name", "match_name_chamber", "party", "district")
  assert_no_unmatched_legiscan(unmatched_legiscan, verbose)

  # Check for unmatched sponsors (in bills but not in legiscan)
  unmatched_sponsors <- all_sponsors_2 %>%
    filter(is.na(.data$name)) %>%
    select("LES_sponsor", "match_name_chamber", "num_sponsored_bills")
  assert_no_unmatched_sponsors(unmatched_sponsors, verbose)

  # Check for duplicate matches (multiple legiscan records matched to same sponsor)
  # Multiple legiscan records shouldn't map to the same LES_sponsor
  duplicate_matches <- all_sponsors_2 %>%
    filter(!is.na(.data$LES_sponsor) & !is.na(.data$name)) %>%
    group_by(.data$LES_sponsor, .data$chamber) %>%
    summarise(
      count = n(),
      legiscan_names = paste(unique(.data$name), collapse = ", "),
      .groups = "drop"
    ) %>%
    filter(.data$count > 1)

  if (nrow(duplicate_matches) > 0) {
    cli_warn(paste(
      "Found duplicate matches - multiple legiscan records",
      "matched to same sponsor name:"
    ))
    print(duplicate_matches)
    cli_warn(paste(
      "This indicates fuzzy matching errors. Add custom_match entries to",
      "prevent these incorrect matches."
    ))
  }

  # Create final legislator dataset
  legis_data <- all_sponsors_2 %>%
    rename(data_name = "LES_sponsor") %>%
    mutate(
      sponsor = ifelse(!is.na(.data$name), .data$name,
                      stringr::str_to_title(.data$match_name)),
      term = term,
      chamber = substr(.data$district, 1, 1)
    ) %>%
    select("sponsor", "data_name", "name", klarner_id = "people_id",
           "chamber", "party", "district", "term", "num_sponsored_bills",
           "num_cosponsored_bills", "sponsor_pass_rate",
           "sponsor_law_rate") %>%
    arrange(.data$chamber, .data$sponsor) %>%
    distinct()

  cli_log(glue("Reconciliation complete: {nrow(legis_data)} legislators"))

  # Return data with legis_data and bills added
  list(
    bill_details = data$bill_details,
    all_bill_details = data$all_bill_details,
    bill_history = data$bill_history,
    ss_bills = data$ss_bills,
    commem_bills = data$commem_bills,
    legiscan = data$legiscan,
    state_config = data$state_config,
    leg_achievement_matrix = data$leg_achievement_matrix,
    bills = bills,
    legis_data = legis_data,
    verbose = data$verbose
  )
}

# Export function
list(reconcile_legislators = reconcile_legislators)
