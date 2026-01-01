#!/usr/bin/env Rscript
# SLES CLI - State Legislative Effectiveness Scores
#
# Usage: Rscript cli.R <state> <term> <operation>
#   state: State postal code (e.g., "WI")
#   term: Legislative term (e.g., "2023_2024")
#   operation: Operation to perform (currently only "estimate")
#
# Example: Rscript cli.R WI 2023_2024 estimate

# Set repo root for all modules to use
Sys.setenv(SLES_REPO_ROOT = normalizePath(getwd()))

# Parse command-line arguments
args <- commandArgs(trailingOnly = TRUE)

# Validate arguments
if (length(args) < 3) {
  cat("Error: Missing required arguments\n")
  cat("Usage: Rscript cli.R <state> <term> <operation>\n")
  cat("Example: Rscript cli.R WI 2023_2024 estimate\n")
  quit(status = 1)
}

state <- args[1]
term <- args[2]
operation <- args[3]

# Validate operation
valid_operations <- c("estimate")
if (!operation %in% valid_operations) {
  cat(sprintf("Error: Invalid operation '%s'\n", operation))
  cat(sprintf(
    "Valid operations: %s\n",
    paste(valid_operations, collapse = ", ")
  ))
  quit(status = 1)
}

# Route to appropriate operation
if (operation == "estimate") {
  cat(sprintf("Running estimation for %s (%s)...\n", state, term))

  # Source the estimation module
  estimate <- source("estimate/estimate.R", local = TRUE)$value

  # Run estimation
  estimate$estimate_les(state, term)

  cat("Estimation complete.\n")
}
