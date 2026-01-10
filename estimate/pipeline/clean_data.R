# Pipeline Stage 2: Clean Data
# Cleans and standardizes loaded data

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
paths <- source(file.path(repo_root, "utils/paths.R"), local = TRUE)$value
logging <- source(file.path(repo_root, "utils/logging.R"),
                  local = TRUE)$value
libs <- source(file.path(repo_root, "utils/libs.R"), local = TRUE)$value
state_loader <- source(file.path(repo_root, "estimate/states/loader.R"),
                       local = TRUE)$value
assertions <- source(file.path(repo_root, "estimate/lib/assertions.R"),
                     local = TRUE)$value

# Extract functions
cli_log <- logging$cli_log
cli_warn <- logging$cli_warn
require_libs <- libs$require_libs
load_state_config <- state_loader$load_state_config
assert_no_duplicates <- assertions$assert_no_duplicates
assert_no_by_request <- assertions$assert_no_by_request_sponsored_bills

# Load required libraries
require_libs()
library(dplyr)
library(stringr)
library(glue)

#' Clean data for estimation
#'
#' Applies generic and state-specific cleaning to loaded data.
#'
#' @param data List from load_data containing raw dataframes
#' @param state State postal code
#' @param term Term string (e.g., "2023_2024")
#' @return List containing cleaned dataframes
clean_data <- function(data, state, term) {
  cli_log("Cleaning data...")

  # Get verbose flag from data
  verbose <- if (!is.null(data$verbose)) data$verbose else TRUE

  # Get state configuration from data (already loaded in load_data stage)
  state_config <- data$state_config

  # Clean SS bills (generic logic)
  ss_bills_clean <- data$ss_bills %>%
    filter(.data$State == state) %>%
    rename(bill_id = "Bill No") %>%
    mutate(
      # Date cleanup
      Date = gsub("Sept", "Sep", .data$Date),
      # Date parsing
      date = as.Date(gsub("\\.", "", .data$Date), "%B %d, %Y"),
      # Get year from date
      year = as.integer(format(.data$date, "%Y")),
      # Use year to get term
      term = ifelse(.data$year %% 2 == 1,
                    paste0(.data$year, "_", .data$year + 1),
                    paste0(.data$year - 1, "_", .data$year)),
      # Bill ID cleanup: uppercase
      bill_id = toupper(.data$bill_id),
      # Remove whitespace
      bill_id = gsub(" ", "", .data$bill_id),
      # Zero-pad bill IDs
      bill_id = paste0(
        gsub("[0-9].+", "", .data$bill_id),
        str_pad(gsub("^[A-Z]+", "", .data$bill_id), 4, pad = "0")
      ),
      # Create SS flag
      SS = 1
    ) %>%
    select(state = "State", "term", "year", "bill_id", everything()) %>%
    # Remove row-level duplicates (same bill appearing multiple times in CSV)
    distinct(.data$term, .data$bill_id, .data$SS, .data$year, .data$Title)

  # Clean bill details (STATE-SPECIFIC)
  # Call state-specific clean_bill_details if available
  if (!is.null(state_config$clean_bill_details)) {
    cleaned <- state_config$clean_bill_details(data$bill_details, term, verbose)
    all_bill_details <- cleaned$all_bill_details
    bill_details_clean <- cleaned$bill_details
  } else {
    # Generic fallback (expects bill_id column already exists)
    bill_details_clean <- data$bill_details %>%
      mutate(
        term = term,
        session = .data$session_type
      )
    all_bill_details <- bill_details_clean
  }

  # Validate cleaned bill details
  assert_no_duplicates(bill_details_clean, verbose)
  assert_no_by_request(bill_details_clean, verbose)

  # Clean bill history (STATE-SPECIFIC)
  # Call state-specific clean_bill_history if available
  if (!is.null(state_config$clean_bill_history)) {
    bill_history_clean <- state_config$clean_bill_history(
      data$bill_history, term
    )
  } else {
    # Generic fallback (expects bill_id column already exists)
    bill_history_clean <- data$bill_history
  }

  # Return cleaned data
  list(
    bill_details = bill_details_clean,
    all_bill_details = all_bill_details,
    bill_history = bill_history_clean,
    ss_bills = ss_bills_clean,
    commem_bills = data$commem_bills,
    legiscan = data$legiscan,
    state_config = state_config,
    verbose = data$verbose
  )
}

# Export function
list(clean_data = clean_data)
