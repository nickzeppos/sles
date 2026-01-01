# LES Calculation Utilities
# Core functions for computing Legislative Effectiveness Scores

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
logging <- source(file.path(repo_root, "utils/logging.R"),
                  local = TRUE)$value

# Extract functions
cli_log <- logging$cli_log
cli_warn <- logging$cli_warn

# Load required libraries (assume already loaded by pipeline)
library(dplyr)
library(tibble)
library(glue)

#' Calculate LES scores for legislators
#'
#' Computes Legislative Effectiveness Scores using weighted bill achievement.
#'
#' @param state State postal code
#' @param bill_data Dataframe of bills with sponsor, chamber, achievement
#' @param legislator_data Dataframe of legislators
#' @param session Session string (e.g., "2023_2024")
#' @param ss_weight Weight for substantive & significant bills (default 5)
#' @param reg_weight Weight for regular bills (default 1)
#' @param com_weight Weight for commemorative bills (default 1)
#' @param stage_weights Vector of 5 weights for achievement stages or
#'   "inverse_prob"
#' @return Dataframe of LES scores with detailed breakdowns
calculate_les <- function(
    state, bill_data, legislator_data, session,
    ss_weight = 5, reg_weight = 1, com_weight = 1,
    stage_weights = c(1, 1, 1, 1, 1)) {

  cli_log(glue("Calculating LES for {state} session {session}..."))

  # Match sponsors in bill_data to legislators
  cli_log("Matching bill sponsors to legislators...")
  bill_data$match_name <- NA
  bill_data$match_name <- as.character(bill_data$match_name)

  for (i in 1:nrow(legislator_data)) {
    # Escape special chars in name
    search_name <- gsub("\\)", "\\\\)", gsub("\\(", "\\\\(",
                                             legislator_data[i, ]$data_name))

    chamber <- legislator_data[i, ]$chamber
    matches <- grepl(search_name,
                     bill_data[bill_data$chamber == chamber, ]$sponsor)
    bill_data[bill_data$chamber == chamber, ]$match_name[matches] <-
      legislator_data[i, ]$sponsor
  }

  # Check for unmatched sponsors
  unmatched <- bill_data %>%
    filter(is.na(.data$match_name)) %>%
    select("bill_id", "sponsor") %>%
    distinct()

  if (nrow(unmatched) > 0) {
    cli_warn(glue(
      "Warning: {nrow(unmatched)} bill sponsors not matched to legislators"
    ))
    cli_warn("These bills will be excluded from LES calculation")
  }

  # Remove unmatched bills
  bill_data <- bill_data %>% filter(!is.na(.data$match_name))

  # Determine if using inverse probability weighting
  inverse_prob <- ifelse(length(stage_weights) == 1 &&
                           stage_weights[1] == "inverse_prob", TRUE, FALSE)

  # Initialize output dataframe
  les_dat <- tibble(
    sponsor = character(0),
    data_name = character(0),
    klarner_id = numeric(0),
    session = character(0),
    chamber = character(0),
    LES = numeric(0),
    LES_rank = numeric(0),
    all_bills = numeric(0),
    all_aic = numeric(0),
    all_abc = numeric(0),
    all_pass = numeric(0),
    all_law = numeric(0),
    ss_bills = numeric(0),
    s_bills = numeric(0),
    c_bills = numeric(0)
  )

  # Apply bill weights
  bill_data <- bill_data %>%
    mutate(
      bill_weight = reg_weight,
      bill_weight = ifelse(.data$commem == 1, com_weight, .data$bill_weight),
      bill_weight = ifelse(.data$SS == 1, ss_weight, .data$bill_weight)
    )

  # Calculate LES for each chamber
  for (c in unique(bill_data$chamber)) {
    chamber_bills <- bill_data %>% filter(.data$chamber == c)
    chamber_legislators <- legislator_data %>% filter(.data$chamber == c)
    N <- nrow(chamber_legislators)

    # Weighted step sums (denominators)
    BILL_denom <- sum(chamber_bills$bill_weight * chamber_bills$introduced)
    AIC_denom <- sum(chamber_bills$bill_weight *
                       chamber_bills$action_in_comm)
    ABC_denom <- sum(chamber_bills$bill_weight *
                       chamber_bills$action_beyond_comm)
    PASS_denom <- sum(chamber_bills$bill_weight *
                        chamber_bills$passed_chamber)
    LAW_denom <- sum(chamber_bills$bill_weight * chamber_bills$law)

    # Compute stage weights if inverse probability
    if (inverse_prob) {
      stage_weights <- c(
        1 / (sum(chamber_bills$introduced) / nrow(chamber_bills)),
        1 / (sum(chamber_bills$action_in_comm) / nrow(chamber_bills)),
        1 / (sum(chamber_bills$action_beyond_comm) / nrow(chamber_bills)),
        1 / (sum(chamber_bills$passed_chamber) / nrow(chamber_bills)),
        1 / (sum(chamber_bills$law) / nrow(chamber_bills))
      )
    }

    # Calculate LES for each legislator in chamber
    for (i in 1:nrow(chamber_legislators)) {
      sponsor_row <- chamber_legislators[i, ]
      sponsor_names <- sponsor_row$data_name

      sponsored_bills <- chamber_bills %>%
        filter(.data$sponsor %in% sponsor_names)

      if (nrow(sponsored_bills) == 0) {
        # Zero bills sponsored
        les_dat <- add_row(
          les_dat,
          sponsor = sponsor_row$sponsor,
          data_name = sponsor_row$data_name,
          klarner_id = sponsor_row$klarner_id,
          session = session,
          chamber = c,
          LES = 0,
          LES_rank = NA,
          all_bills = 0,
          all_aic = 0,
          all_abc = 0,
          all_pass = 0,
          all_law = 0,
          ss_bills = 0,
          s_bills = 0,
          c_bills = 0
        )
        next
      }

      # Compute member-level shares for each stage
      BILL_w <- stage_weights[1] * sum(sponsored_bills$bill_weight *
                                         sponsored_bills$introduced)
      AIC_w <- stage_weights[2] * sum(sponsored_bills$bill_weight *
                                        sponsored_bills$action_in_comm)
      ABC_w <- stage_weights[3] * sum(sponsored_bills$bill_weight *
                                        sponsored_bills$action_beyond_comm)
      PASS_w <- stage_weights[4] * sum(sponsored_bills$bill_weight *
                                         sponsored_bills$passed_chamber)
      LAW_w <- stage_weights[5] * sum(sponsored_bills$bill_weight *
                                        sponsored_bills$law)

      # Weight shares
      BILL_wshare <- BILL_w / BILL_denom
      AIC_wshare <- AIC_w / AIC_denom
      ABC_wshare <- ABC_w / ABC_denom
      PASS_wshare <- PASS_w / PASS_denom
      LAW_wshare <- LAW_w / LAW_denom

      # Count bills by type
      sponsor_counts <- sponsored_bills %>%
        summarize(
          all_bills = sum(.data$introduced),
          all_aic = sum(.data$action_in_comm),
          all_abc = sum(.data$action_beyond_comm),
          all_pass = sum(.data$passed_chamber),
          all_law = sum(.data$law),
          ss_bills = sum(.data$SS * .data$introduced),
          s_bills = sum(ifelse(.data$SS == 0 & .data$commem == 0,
                               .data$introduced, 0)),
          c_bills = sum(.data$commem * .data$introduced)
        )

      # Calculate LES
      adj_factor <- sum(stage_weights)
      LES <- N / adj_factor * (BILL_wshare + AIC_wshare + ABC_wshare +
                                 PASS_wshare + LAW_wshare)

      # Add row to output
      les_dat <- add_row(
        les_dat,
        sponsor = sponsor_row$sponsor,
        data_name = sponsor_row$data_name,
        klarner_id = sponsor_row$klarner_id,
        session = session,
        chamber = c,
        LES = LES,
        LES_rank = NA,
        all_bills = sponsor_counts$all_bills,
        all_aic = sponsor_counts$all_aic,
        all_abc = sponsor_counts$all_abc,
        all_pass = sponsor_counts$all_pass,
        all_law = sponsor_counts$all_law,
        ss_bills = sponsor_counts$ss_bills,
        s_bills = sponsor_counts$s_bills,
        c_bills = sponsor_counts$c_bills
      )
    }
  }

  # Assign ranks within each chamber
  les_dat <- les_dat %>%
    group_by(.data$chamber) %>%
    mutate(LES_rank = rank(desc(.data$LES), ties.method = "min")) %>%
    ungroup() %>%
    arrange(.data$chamber, .data$LES_rank)

  cli_log(glue("LES calculation complete: {nrow(les_dat)} legislators"))

  les_dat
}

# Export function
list(calculate_les = calculate_les)
