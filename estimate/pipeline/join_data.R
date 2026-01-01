# Pipeline Stage 3: Join Data
# Joins SS and commemorative flags to bill details

# SS DEDUPLICATION ISSUE:
# Some bills appear in PVS data for multiple years within a term with
# identical titles (e.g., AB0377 in both 2023 and 2024). This creates
# duplicates when joining SS flags to bill_details, corrupting the analysis.
#
# Solution: For bills appearing in multiple years with identical titles,
# keep only the earliest year. Bills with different titles across years
# are retained as they represent substantive changes.
#
# Investigation: See .dropbox/ss_duplicate_investigation.ipynb
# Known affected bills (WI 2023_2024): AB0377, AB0415, SB0139, SB0145,
#   SB0312
#
# Note: This differs from legacy behavior which allowed duplicates.
# Legacy approach is no longer supported.

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
logging <- source(file.path(repo_root, "utils/logging.R"),
                  local = TRUE)$value
libs <- source(file.path(repo_root, "utils/libs.R"), local = TRUE)$value

# Extract functions
cli_log <- logging$cli_log
cli_warn <- logging$cli_warn
cli_error <- logging$cli_error
require_libs <- libs$require_libs

# Load required libraries
require_libs()
library(dplyr)
library(glue)

#' Join data for estimation
#'
#' Joins SS and commemorative bill flags to bill details.
#' Applies SS deduplication logic to handle bills appearing in multiple
#' years.
#'
#' @param data List from clean_data containing cleaned dataframes
#' @param state State postal code
#' @param term Term string (e.g., "2023_2024")
#' @return List containing joined dataframes
join_data <- function(data, state, term) {
  cli_log("Joining SS and commemorative flags...")

  # Deduplicate SS bills (keep earliest year for identical titles)
  cli_log("Deduplicating SS bills...")
  ss_deduped <- data$ss_bills %>%
    filter(.data$term == !!term) %>%
    group_by(.data$bill_id, .data$term) %>%
    mutate(
      title_variations = n_distinct(.data$Title),
      is_duplicate_years = n() > 1
    ) %>%
    ungroup() %>%
    filter(
      !.data$is_duplicate_years |
        (.data$is_duplicate_years & .data$title_variations == 1)
    ) %>%
    group_by(.data$bill_id, .data$term, .data$Title) %>%
    slice_min(.data$year, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    select(-"title_variations", -"is_duplicate_years")

  # Join SS flags to bill details
  cli_log("Joining SS flags to bill details...")
  bill_details_with_ss <- data$bill_details %>%
    left_join(
      ss_deduped %>% select("bill_id", "SS"),
      by = "bill_id"
    ) %>%
    mutate(SS = ifelse(is.na(.data$SS), 0, .data$SS))

  # Join commemorative flags to bill details
  # Note: Join on bill_id, term, AND session to handle special sessions
  # (e.g., SB0001 exists in both regular and special sessions)
  cli_log("Joining commemorative flags to bill details...")
  bill_details_joined <- bill_details_with_ss %>%
    left_join(
      data$commem_bills %>%
        select("bill_id", "term", "session", "commem"),
      by = c("bill_id", "term", "session")
    ) %>%
    mutate(commem = ifelse(is.na(.data$commem), 0, .data$commem))

  # Can't be commem if it's SS
  bill_details_joined <- bill_details_joined %>%
    mutate(commem = ifelse(.data$SS == 1 & .data$commem == 1,
                           0, .data$commem))

  # Validate no missing SS bills from details
  cli_log("Validating SS bill coverage...")
  ss_in_details <- ss_deduped$bill_id
  ss_in_joined <- bill_details_joined %>%
    filter(.data$SS == 1) %>%
    pull("bill_id")

  missing_ss <- setdiff(ss_in_details, ss_in_joined)
  if (length(missing_ss) > 0) {
    cli_warn(glue(
      "Warning: {length(missing_ss)} SS bills not found in bill details:"
    ))
    cli_warn(glue("  {paste(missing_ss, collapse = ', ')}"))
  }

  # Return joined data
  list(
    bill_details = bill_details_joined,
    all_bill_details = data$all_bill_details,
    bill_history = data$bill_history,
    ss_bills = ss_deduped,
    commem_bills = data$commem_bills,
    legiscan = data$legiscan,
    state_config = data$state_config
  )
}

# Export function
list(join_data = join_data)
