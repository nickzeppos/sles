# Pipeline Stage 4: Compute Achievement
# Computes legislative achievement matrix from bill history

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
logging <- source(file.path(repo_root, "utils/logging.R"),
                  local = TRUE)$value
libs <- source(file.path(repo_root, "utils/libs.R"), local = TRUE)$value
bill_history_utils <- source(file.path(repo_root,
                                       "estimate/lib/bill_history.R"),
                             local = TRUE)$value

# Extract functions
cli_log <- logging$cli_log
require_libs <- libs$require_libs
evaluate_bill_history <- bill_history_utils$evaluate_bill_history

# Load required libraries
require_libs()
library(dplyr)
library(tibble)

#' Compute achievement for estimation
#'
#' Computes legislative achievement matrix by evaluating bill history for
#' each bill in bill_details. Determines what stages each bill achieved:
#' introduced, action_in_comm, action_beyond_comm, passed_chamber, law.
#'
#' @param data List from join_data containing joined dataframes
#' @param state State postal code
#' @param term Term string (e.g., "2023_2024")
#' @return List containing data with leg_achievement_matrix added
compute_achievement <- function(data, state, term) {
  cli_log("Computing legislative achievement matrix...")

  # Use already-cleaned bill history from Stage 2
  # State-specific cleaning is handled in clean_data stage
  bill_history_clean <- data$bill_history

  # Get state-specific step terms from config
  step_terms <- data$state_config$step_terms

  # Initialize achievement matrix
  cli_log("Evaluating bill histories...")
  all_bill_stages <- tibble(
    bill_id = character(0),
    term = character(0),
    session = character(0),
    LES_sponsor = character(0),
    state = character(0),
    introduced = integer(0),
    action_in_comm = integer(0),
    action_beyond_comm = integer(0),
    passed_chamber = integer(0),
    law = integer(0)
  )

  # Loop through each bill in bill_details
  for (i in seq_len(nrow(data$bill_details))) {
    i_bill_id <- data$bill_details$bill_id[i]
    i_term <- data$bill_details$term[i]
    i_session <- data$bill_details$session[i]
    i_sponsor <- data$bill_details$LES_sponsor[i]

    # Get relevant slice of history data
    i_bill_history <- bill_history_clean %>%
      filter(.data$bill_id == i_bill_id & .data$term == i_term)

    # Evaluate this bill's history
    bill_stages <- evaluate_bill_history(
      bill_history = i_bill_history,
      this_id = i_bill_id,
      this_term = i_term,
      this_session = i_session,
      this_sponsor = i_sponsor,
      state = state,
      step_terms = step_terms
    )

    # Append to achievement matrix
    all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
  }

  cli_log(glue::glue("Computed achievement for {nrow(all_bill_stages)} bills"))

  # Return data with achievement matrix added
  list(
    bill_details = data$bill_details,
    all_bill_details = data$all_bill_details,
    bill_history = bill_history_clean,
    ss_bills = data$ss_bills,
    commem_bills = data$commem_bills,
    legiscan = data$legiscan,
    state_config = data$state_config,
    leg_achievement_matrix = all_bill_stages
  )
}

# Export function
list(compute_achievement = compute_achievement)
