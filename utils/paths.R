# Path utilities for SLES data and output files
# All paths are relative to repo root

# Get repo root directory
get_repo_root <- function() {
  repo_root <- Sys.getenv("SLES_REPO_ROOT")
  if (repo_root == "") {
    stop("SLES_REPO_ROOT environment variable not set")
  }
  repo_root
}

# Get data directory for a specific state
# state: Two-letter state postal code (e.g., "WI")
get_state_data_dir <- function(state) {
  file.path(get_repo_root(), ".data", state)
}

# Get bill data directory for a state
get_bill_dir <- function(state) {
  file.path(get_state_data_dir(state), "bill")
}

# Get commemorative bills directory for a state
get_commem_dir <- function(state) {
  file.path(get_state_data_dir(state), "commem")
}

# Get legiscan data directory for a state
get_legiscan_dir <- function(state) {
  file.path(get_state_data_dir(state), "legiscan")
}

# Get substantive & significant bills directory for a state
get_ss_dir <- function(state) {
  file.path(get_state_data_dir(state), "ss")
}

# Get outputs directory for a state
get_outputs_dir <- function(state) {
  file.path(get_state_data_dir(state), "outputs")
}

# Get review directory for manual data review files
get_review_dir <- function(state) {
  file.path(get_state_data_dir(state), "review")
}

# Check if a file/directory exists, error if not
exists_or_throw <- function(path) {
  if (!file.exists(path)) {
    stop(sprintf("File not found: %s", path))
  }
  invisible(TRUE)
}

# Export
list(
  get_repo_root = get_repo_root,
  get_state_data_dir = get_state_data_dir,
  get_bill_dir = get_bill_dir,
  get_commem_dir = get_commem_dir,
  get_legiscan_dir = get_legiscan_dir,
  get_ss_dir = get_ss_dir,
  get_outputs_dir = get_outputs_dir,
  get_review_dir = get_review_dir,
  exists_or_throw = exists_or_throw
)
