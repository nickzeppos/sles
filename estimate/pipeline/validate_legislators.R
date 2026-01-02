# Pipeline Stage 5.5: Validate Legislators
# Identifies zero-LES legislators and handles manual review for removal

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
logging <- source(file.path(repo_root, "utils/logging.R"),
  local = TRUE
)$value
libs <- source(file.path(repo_root, "utils/libs.R"), local = TRUE)$value
paths <- source(file.path(repo_root, "utils/paths.R"), local = TRUE)$value

# Extract functions
cli_log <- logging$cli_log
cli_warn <- logging$cli_warn
cli_error <- logging$cli_error
cli_prompt <- logging$cli_prompt
require_libs <- libs$require_libs
get_review_dir <- paths$get_review_dir

# Load required libraries
require_libs()
library(dplyr)
library(glue)

#' Validate legislators and handle zero-LES cases
#'
#' Identifies legislators with zero sponsored bills and provides workflow
#' for manual review to determine if they should be excluded (e.g., if they
#' didn't serve the full term due to resignation, death, etc.).
#'
#' @param data List from reconcile_legislators containing legis_data
#' @param state State postal code
#' @param term Term string (e.g., "2023_2024")
#' @return List containing data with legis_data potentially filtered
validate_legislators <- function(data, state, term) {
  cli_log("Validating legislators...")

  # Identify zero-LES legislators (in roster but no sponsored bills)
  zero_bill_sponsors <- data$legis_data %>%
    filter(is.na(.data$num_sponsored_bills) |
      .data$num_sponsored_bills == 0) %>%
    select("sponsor", "klarner_id", "chamber", "party", "district", "term") %>%
    mutate(
      state = state,
      term = term,
      legiscan_id = .data$klarner_id,
      not_actually_in_chamber = FALSE, # Default to keeping them
      notes = "Zero bills sponsored - needs manual review"
    ) %>%
    select(
      "state", "term", "sponsor", "legiscan_id", "chamber", "party",
      "district", "not_actually_in_chamber", "notes"
    )

  if (nrow(zero_bill_sponsors) == 0) {
    cli_log("No zero-LES legislators found.")
    return(data)
  }

  cli_warn(glue(
    "Found {nrow(zero_bill_sponsors)} legislators with zero ",
    "sponsored bills (will have zero LES):"
  ))
  print(zero_bill_sponsors %>%
    select("sponsor", "chamber", "party", "district"))

  # Check if review file already exists
  review_dir <- get_review_dir(state)
  if (!dir.exists(review_dir)) {
    dir.create(review_dir, recursive = TRUE)
  }

  review_file <- file.path(
    review_dir,
    glue("{state}_Zero_LES_legislators_Coded.csv")
  )

  if (file.exists(review_file)) {
    cli_log(glue("Zero-LES legislator file already exists. Reading it..."))
    zero_bill_sponsors_coded <- read.csv(review_file) %>%
      filter(.data$state == !!state & .data$term == !!term)

    cli_log(glue(
      "Found {nrow(zero_bill_sponsors_coded)} zero-LES ",
      "legislators in file for this state/term."
    ))

    use_cached <- cli_prompt(paste(
      "If you have already reviewed and coded",
      "this list, use cached decisions? [y/n]: "
    ))

    if (tolower(use_cached) == "y") {
      # Use cached decisions
      zero_bill_sponsors <- zero_bill_sponsors_coded
      cli_log("Using cached review decisions.")
    } else {
      # Re-review
      zero_bill_sponsors <- review_zero_les_legislators(zero_bill_sponsors)
      # Update the file (merge with other state/term data if exists)
      all_reviews <- read.csv(review_file) %>%
        filter(!(.data$state == !!state & .data$term == !!term))
      all_reviews <- bind_rows(all_reviews, zero_bill_sponsors)
      write.csv(all_reviews, review_file, row.names = FALSE)
      cli_log(glue("Updated review file: {review_file}"))
    }
  } else {
    # First time - need to review
    cli_warn("No review file found. Manual review required.")
    zero_bill_sponsors <- review_zero_les_legislators(zero_bill_sponsors)

    # Write the file
    write.csv(zero_bill_sponsors, review_file, row.names = FALSE)
    cli_log(glue("Wrote zero-LES legislator review file to: {review_file}"))
  }

  # Apply removals
  legislators_to_remove <- zero_bill_sponsors %>%
    filter(.data$not_actually_in_chamber == TRUE) %>%
    mutate(chamber = substr(.data$chamber, 1, 1))

  if (nrow(legislators_to_remove) > 0) {
    cli_log(glue(
      "Removing {nrow(legislators_to_remove)} legislators ",
      "flagged as not serving full term:"
    ))
    print(legislators_to_remove %>%
      select("sponsor", "chamber", "party", "district"))

    data$legis_data <- anti_join(
      data$legis_data,
      legislators_to_remove,
      by = c("klarner_id" = "legiscan_id", "chamber")
    )

    cli_log(glue("Legislators remaining: {nrow(data$legis_data)}"))
  } else {
    cli_log("No legislators flagged for removal.")
  }

  data
}

#' Interactively review zero-LES legislators
#'
#' Prompts user for each legislator to determine if they should be removed
#'
#' @param zero_bill_sponsors Dataframe of legislators to review
#' @return Updated dataframe with not_actually_in_chamber coded
review_zero_les_legislators <- function(zero_bill_sponsors) {
  cli_warn(paste(
    "Please review the following legislators with zero",
    "sponsored bills:"
  ))
  cli_warn(paste(
    "For each, determine if they served the FULL term or if",
    "they should be excluded"
  ))
  cli_warn(paste(
    "(e.g., resigned early, died, appointed mid-term",
    "replacement, etc.)"
  ))

  for (i in seq_len(nrow(zero_bill_sponsors))) {
    cat("\n")
    cli_log(glue("Legislator {i} of {nrow(zero_bill_sponsors)}:"))
    cli_log(glue("  Name: {sponsor}",
      "  Chamber: {chamber}",
      "  Party: {party}",
      "  District: {district}",
      sponsor = zero_bill_sponsors$sponsor[i],
      chamber = zero_bill_sponsors$chamber[i],
      party = zero_bill_sponsors$party[i],
      district = zero_bill_sponsors$district[i]
    ))

    user_input <- cli_prompt(paste(
      "Should this legislator be REMOVED",
      "(did NOT serve full term)? [y/n]: "
    ))

    if (tolower(user_input) == "y") {
      zero_bill_sponsors$not_actually_in_chamber[i] <- TRUE
      zero_bill_sponsors$notes[i] <- "Did not serve full term - excluded"
    } else {
      zero_bill_sponsors$not_actually_in_chamber[i] <- FALSE
      zero_bill_sponsors$notes[i] <- "Served full term - included with LES=0"
    }
  }

  zero_bill_sponsors
}

# Export function
list(validate_legislators = validate_legislators)
