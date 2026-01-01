# Dynamic State Configuration Loader
# Loads state-specific config files without manual dispatch

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
paths <- source(file.path(repo_root, "utils/paths.R"), local = TRUE)$value
logging <- source(file.path(repo_root, "utils/logging.R"),
                  local = TRUE)$value
libs <- source(file.path(repo_root, "utils/libs.R"), local = TRUE)$value

# Extract functions from modules
get_repo_root <- paths$get_repo_root
cli_log <- logging$cli_log
cli_error <- logging$cli_error
require_libs <- libs$require_libs

# Load required libraries
require_libs()
library(glue)

#' Load state configuration
#'
#' Dynamically sources the state config file and returns its configuration.
#' No manual dispatch dictionary needed - discovers files automatically.
#'
#' @param state State postal code (e.g., "WI", "AR")
#' @return List containing state configuration
load_state_config <- function(state) {
  # Construct path to state config file
  repo_root <- get_repo_root()
  state_file <- file.path(repo_root, "estimate", "states",
                          paste0(state, ".R"))

  # Check if state config exists
  if (!file.exists(state_file)) {
    cli_error(glue("No configuration found for state: {state}"))
    cli_error(glue("Expected file: {state_file}"))
    quit(status = 1)
  }

  # Source the state file and return its config
  config <- source(state_file, local = TRUE)$value

  # Validate config has required fields
  required_fields <- c("bill_types", "step_terms")
  missing_fields <- setdiff(required_fields, names(config))

  if (length(missing_fields) > 0) {
    cli_error(glue(
      "State config for {state} missing required fields: {fields}",
      fields = paste(missing_fields, collapse = ", ")
    ))
    quit(status = 1)
  }

  cli_log(glue("Loaded configuration for {state}"))
  config
}

# Export function
list(load_state_config = load_state_config)
