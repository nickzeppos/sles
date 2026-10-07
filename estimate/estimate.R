#!/usr/bin/env Rscript
# SLES Estimation Module
#
# Can be run directly via:
#   Rscript estimate/estimate.R <state> <term> [--verbose]
# Or sourced to call estimate_les(state, term, verbose).

run_directly <- sys.nframe() == 0
if (run_directly) {
  args <- commandArgs(trailingOnly = TRUE)
  if (!length(args) %in% c(2, 3) ||
      (length(args) == 3 && args[3] != "--verbose")) {
    stop(paste("Usage: Rscript estimate/estimate.R",
               "<state> <term> [--verbose]"), call. = FALSE)
  }
  state <- toupper(args[1])
  term <- args[2]
  verbose <- "--verbose" %in% args

  # Resolve paths from this module, even when launched outside the repo.
  script_arg <- grep("^--file=", commandArgs(), value = TRUE)[1]
  script_path <- normalizePath(sub("^--file=", "", script_arg))
  Sys.setenv(SLES_REPO_ROOT = dirname(dirname(script_path)))
}

# Get repo root for sourcing
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(getwd())
  Sys.setenv(SLES_REPO_ROOT = repo_root)
}

# Validate direct arguments before loading packages or touching data.
terms <- source(file.path(repo_root, "utils/term_to_inputs.R"), local = TRUE)$value
if (run_directly) invisible(terms$term_years(state, term))

# Source pipeline stages
load_data_module <- source(
  file.path(repo_root, "estimate/pipeline/load_data.R"),
  local = TRUE
)$value
clean_data_module <- source(
  file.path(repo_root, "estimate/pipeline/clean_data.R"),
  local = TRUE
)$value
join_data_module <- source(
  file.path(repo_root, "estimate/pipeline/join_data.R"),
  local = TRUE
)$value
compute_achievement_module <- source(
  file.path(repo_root, "estimate/pipeline/compute_achievement.R"),
  local = TRUE
)$value
reconcile_legislators_module <- source(
  file.path(repo_root, "estimate/pipeline/reconcile_legislators.R"),
  local = TRUE
)$value
validate_legislators_module <- source(
  file.path(repo_root, "estimate/pipeline/validate_legislators.R"),
  local = TRUE
)$value
calculate_scores_module <- source(
  file.path(repo_root, "estimate/pipeline/calculate_scores.R"),
  local = TRUE
)$value
write_outputs_module <- source(
  file.path(repo_root, "estimate/pipeline/write_outputs.R"),
  local = TRUE
)$value

# Main estimation function
estimate_les <- function(state, term, verbose = FALSE) {
  state <- toupper(state)
  invisible(terms$term_years(state, term))
  cat(sprintf("estimate_les called with state=%s, term=%s\n", state, term))

  # Stage 1: Load data (pass verbose immediately)
  data <- load_data_module$load_data(state, term, verbose)

  # Verbose flag already added in load_data, but ensure it's set
  data$verbose <- verbose

  # Stage 2: Clean data
  data <- clean_data_module$clean_data(data, state, term)

  # Stage 3: Join data
  data <- join_data_module$join_data(data, state, term)

  # Stage 4: Compute achievement
  data <- compute_achievement_module$compute_achievement(data, state, term)

  # Stage 5: Reconcile legislators
  data <- reconcile_legislators_module$reconcile_legislators(
    data,
    state, term
  )

  # Stage 5.5: Validate legislators (zero-LES review)
  data <- validate_legislators_module$validate_legislators(data, state, term)

  # Stage 6: Calculate scores
  data <- calculate_scores_module$calculate_scores(data, state, term)

  # Stage 7: Write outputs
  data <- write_outputs_module$write_outputs(data, state, term)

  cat("Pipeline complete!\n")

  invisible(data)
}

# If run directly (not sourced), execute estimation with command-line args
if (run_directly) {
  estimate_les(state, term, verbose = verbose)
}

# Export functions for sourcing
list(
  estimate_les = estimate_les
)
