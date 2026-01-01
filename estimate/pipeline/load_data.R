# Pipeline Stage 1: Load Data
# Loads and validates all required data files for a state/term

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
paths <- source(file.path(repo_root, "utils/paths.R"), local = TRUE)$value
logging <- source(file.path(repo_root, "utils/logging.R"),
                  local = TRUE)$value
libs <- source(file.path(repo_root, "utils/libs.R"), local = TRUE)$value

# Extract functions
get_bill_dir <- paths$get_bill_dir
get_ss_dir <- paths$get_ss_dir
get_commem_dir <- paths$get_commem_dir
get_legiscan_dir <- paths$get_legiscan_dir
cli_log <- logging$cli_log
cli_error <- logging$cli_error
require_libs <- libs$require_libs

# Load required libraries
require_libs()
library(readr)
library(dplyr)
library(glue)

#' Load data for estimation
#'
#' Loads all required CSV files for a given state and term.
#' Combines year-specific files (bills, history, SS) into unified datasets.
#'
#' @param state State postal code (e.g., "WI")
#' @param term Term in format "YYYY_YYYY" (e.g., "2023_2024")
#' @return List containing: bill_details, bill_history, ss_bills,
#'         commem_bills, legiscan
load_data <- function(state, term) {
  cli_log(glue("Loading data for {state} {term}..."))

  # Parse term into start and end years
  years <- strsplit(term, "_")[[1]]
  if (length(years) != 2) {
    cli_error(glue("Invalid term format: {term}. Expected YYYY_YYYY"))
    quit(status = 1)
  }
  term_start_year <- years[1]
  term_end_year <- years[2]

  # Get directory paths
  bill_dir <- get_bill_dir(state)
  ss_dir <- get_ss_dir(state)
  commem_dir <- get_commem_dir(state)
  legiscan_dir <- get_legiscan_dir(state)

  # Load bill details (single file with start year contains full term)
  cli_log("Loading bill details...")
  bill_details <- read_csv(
    file.path(bill_dir,
              glue("{state}_Bill_Details_{term_start_year}.csv")),
    show_col_types = FALSE
  )

  # Load bill histories (single file with start year contains full term)
  cli_log("Loading bill histories...")
  bill_history <- read_csv(
    file.path(bill_dir,
              glue("{state}_Bill_Histories_{term_start_year}.csv")),
    show_col_types = FALSE
  )

  # Load SS bills (two separate year files, combine them)
  cli_log("Loading Substantive & Significant bills...")
  ss_bills <- bind_rows(
    read_csv(file.path(ss_dir,
                       glue("{state}_SS_Bills_{term_start_year}.csv")),
             show_col_types = FALSE),
    read_csv(file.path(ss_dir,
                       glue("{state}_SS_Bills_{term_end_year}.csv")),
             show_col_types = FALSE)
  )

  # Load commemorative bills (single file for term)
  cli_log("Loading commemorative bills...")
  commem_bills <- read_csv(
    file.path(commem_dir, glue("{state}_Commem_Bills_{term}.csv")),
    show_col_types = FALSE
  )

  # Load legiscan data (from all sessions for this term)
  cli_log("Loading Legiscan legislator data...")
  # List all session directories
  legiscan_sessions <- list.dirs(legiscan_dir, full.names = FALSE,
                                 recursive = FALSE)
  # Filter to sessions starting with either term year
  legiscan_sessions <- legiscan_sessions[
    startsWith(legiscan_sessions, term_start_year) |
      startsWith(legiscan_sessions, term_end_year)
  ]

  # Read people.csv from each session and combine
  legiscan_list <- lapply(legiscan_sessions, function(session) {
    session_path <- file.path(legiscan_dir, session, "csv", "people.csv")
    read_csv(session_path, show_col_types = FALSE) %>%
      select(-c("votesmart_id", "knowwho_pid", "ballotpedia",
                "nickname", "suffix")) %>%
      distinct()
  })

  # Combine all sessions and deduplicate
  legiscan <- bind_rows(legiscan_list) %>% distinct()

  # Return all datasets
  list(
    bill_details = bill_details,
    bill_history = bill_history,
    ss_bills = ss_bills,
    commem_bills = commem_bills,
    legiscan = legiscan
  )
}

# Export function
list(load_data = load_data)
