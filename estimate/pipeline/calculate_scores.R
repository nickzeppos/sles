# Pipeline Stage 6: Calculate Scores
# Calculates Legislative Effectiveness Scores for all legislators

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
logging <- source(file.path(repo_root, "utils/logging.R"),
                  local = TRUE)$value
libs <- source(file.path(repo_root, "utils/libs.R"), local = TRUE)$value
les_calc <- source(file.path(repo_root, "estimate/lib/les_calc.R"),
                   local = TRUE)$value
assertions <- source(file.path(repo_root, "estimate/lib/assertions.R"),
                     local = TRUE)$value

# Extract functions
cli_log <- logging$cli_log
require_libs <- libs$require_libs
calculate_les <- les_calc$calculate_les
assert_no_bills_missing_sponsors <- assertions$assert_no_bills_missing_sponsors

# Load required libraries
require_libs()
library(dplyr)

#' Calculate LES scores for estimation
#'
#' Final pipeline stage that prepares bill data and calculates LES scores
#' for all legislators in the term.
#'
#' @param data List from reconcile_legislators containing all data
#' @param state State postal code
#' @param term Term string (e.g., "2023_2024")
#' @return List containing data with les_scores added
calculate_scores <- function(data, state, term) {
  cli_log("Calculating Legislative Effectiveness Scores...")

  # Prepare bills data for LES calculation
  cli_log("Preparing bills data...")
  bills_prepared <- data$bills %>%
    rename(sponsor = "LES_sponsor")

  # Apply state-specific chamber derivation if available
  state_config <- data$state_config
  if (!is.null(state_config$prepare_bills_for_les)) {
    bills_prepared <- state_config$prepare_bills_for_les(bills_prepared)
  } else {
    # Generic: assume Wisconsin-style "A" prefix for Assembly/House
    bills_prepared <- bills_prepared %>%
      mutate(chamber = ifelse(substring(.data$bill_id, 1, 1) == "A",
        "H", "S"
      ))
  }

  # Validate all bills have identified sponsors in legis_data
  bills_missing_sponsors <- bills_prepared %>%
    filter(!.data$sponsor %in% data$legis_data$data_name)
  assert_no_bills_missing_sponsors(bills_missing_sponsors, data$legis_data)

  cli_log("All bills have identified sponsors. Proceeding to LES calculation.")

  # Calculate weighted LES scores
  # Using original weights: SS=10, regular=5, commemorative=1
  # Using equal stage weights (not inverse probability)
  les_scores <- calculate_les(
    state = state,
    bill_data = bills_prepared,
    legislator_data = data$legis_data,
    session = term,
    ss_weight = 10,
    reg_weight = 5,
    com_weight = 1,
    stage_weights = c(1, 1, 1, 1, 1)
  )

  # Calculate unweighted LES scores
  cli_log("Calculating unweighted LES scores...")
  les_scores_unweighted <- calculate_les(
    state = state,
    bill_data = bills_prepared,
    legislator_data = data$legis_data,
    session = term,
    ss_weight = 1,
    reg_weight = 1,
    com_weight = 1,
    stage_weights = c(1, 1, 1, 1, 1)
  )

  # Join unweighted scores to weighted scores
  les_scores <- les_scores %>%
    left_join(
      les_scores_unweighted %>%
        select("sponsor", "chamber", LES_nw = "LES"),
      by = c("sponsor", "chamber")
    )

  # Post-process to match original format
  les_scores <- les_scores %>%
    # Rename session to term
    rename(term = "session") %>%
    # Convert chamber codes to full names
    mutate(chamber = ifelse(.data$chamber == "H", "House", "Senate")) %>%
    # Join back legislator aggregate stats
    left_join(
      data$legis_data %>%
        select("sponsor", "chamber", "party", "district",
               "num_sponsored_bills", "sponsor_pass_rate",
               "sponsor_law_rate", "num_cosponsored_bills") %>%
        mutate(chamber = ifelse(.data$chamber == "H", "House", "Senate")),
      by = c("sponsor", "chamber")
    ) %>%
    # Rename legiscan ID column
    rename(legiscan_id = "klarner_id") %>%
    # Reorder columns to match original output format
    select(
      "sponsor", "data_name", "legiscan_id", "term", "chamber",
      "district", "party", "LES", "LES_rank",
      "BILL_wshare", "AIC_wshare", "ABC_wshare", "PASS_wshare", "LAW_wshare",
      "all_bills", "all_aic", "all_abc", "all_pass", "all_law",
      "ss_bills", "ss_aic", "ss_abc", "ss_pass", "ss_law",
      "s_bills", "s_aic", "s_abc", "s_pass", "s_law",
      "c_bills", "c_aic", "c_abc", "c_pass", "c_law",
      "LES_nw",
      "num_sponsored_bills", "sponsor_pass_rate", "sponsor_law_rate",
      "num_cosponsored_bills"
    )

  cli_log(glue::glue("Calculated scores for {nrow(les_scores)} legislators"))

  # Return data with scores added
  list(
    bill_details = data$bill_details,
    all_bill_details = data$all_bill_details,
    bill_history = data$bill_history,
    ss_bills = data$ss_bills,
    commem_bills = data$commem_bills,
    legiscan = data$legiscan,
    state_config = data$state_config,
    leg_achievement_matrix = data$leg_achievement_matrix,
    bills = data$bills,
    legis_data = data$legis_data,
    les_scores = les_scores
  )
}

# Export function
list(calculate_scores = calculate_scores)
