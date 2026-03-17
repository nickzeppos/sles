# Pipeline Stage 7: Write Outputs
# Writes LES scores and bill stage codings to CSV files

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
get_outputs_dir <- paths$get_outputs_dir
cli_log <- logging$cli_log
require_libs <- libs$require_libs

# Load required libraries
require_libs()
library(dplyr)
library(glue)

# Standard columns for Bill_Stage_Codings output (matches old script format)
# These columns are consistent across all states
STANDARD_BILL_STAGE_COLUMNS <- c(

  "bill_id",
  "term",
  "session",
  "LES_sponsor",
  "introduced",
  "action_in_comm",
  "action_beyond_comm",
  "passed_chamber",
  "law",
  "bill_url",
  "SS",
  "commem",
  "state"
)

#' Write outputs for estimation
#'
#' Writes LES scores and bill stage codings to CSV files in the outputs
#' directory. Creates output directory if it doesn't exist.
#'
#' Bill stage codings use a standardized format matching the old script output,
#' with consistent columns across all states.
#'
#' @param data List from calculate_scores containing all data
#' @param state State postal code
#' @param term Term string (e.g., "2023_2024")
#' @return List containing data (unchanged)
write_outputs <- function(data, state, term) {
  # Get verbose flag from data
  verbose <- if (!is.null(data$verbose)) data$verbose else TRUE

  cli_log("Writing outputs to CSV files...")

  # Create outputs directory if needed
  outputs_dir <- get_outputs_dir(state)
  if (!dir.exists(outputs_dir)) {
    dir.create(outputs_dir, recursive = TRUE)
    if (verbose) cli_log(glue("Created outputs directory: {outputs_dir}"))
  }

  # Write LES scores
  les_file <- file.path(outputs_dir, glue("{state}_LES_{term}.csv"))
  if (verbose) cli_log(glue("Writing LES scores to {basename(les_file)}..."))
  write.csv(data$les_scores, les_file, row.names = FALSE)
  if (verbose) cli_log(glue("  {nrow(data$les_scores)} legislators written"))

  # Write bill stage codings (standardized format matching old script)
  # Select only columns that exist in the data
  available_cols <- intersect(STANDARD_BILL_STAGE_COLUMNS, colnames(data$bills))
  bill_stage_codings <- data$bills %>%
    select(all_of(available_cols))

  bills_file <- file.path(
    outputs_dir,
    glue("{state}_coded_bills_{term}.csv")
  )
  if (verbose) {
    cli_log(glue("Writing coded bills to {basename(bills_file)}..."))
  }
  write.csv(bill_stage_codings, bills_file, row.names = FALSE)
  if (verbose) cli_log(glue("  {nrow(bill_stage_codings)} bills written"))

  if (verbose) cli_log("Output files written successfully")

  # Return data unchanged
  data
}

# Export function
list(write_outputs = write_outputs)
