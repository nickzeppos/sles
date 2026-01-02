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
  local = TRUE
)$value
libs <- source(file.path(repo_root, "utils/libs.R"), local = TRUE)$value
assertions <- source(file.path(repo_root, "estimate/lib/assertions.R"),
  local = TRUE
)$value

# Extract functions
cli_log <- logging$cli_log
cli_warn <- logging$cli_warn
cli_error <- logging$cli_error
require_libs <- libs$require_libs
assert_no_missing_ss <- assertions$assert_no_missing_ss_from_details
assert_join_wont_yield_dupes <- assertions$assert_join_wont_yield_dupes
assert_ss_join_validity <- assertions$assert_ss_join_bill_details_validity
assert_duplicates_resolved <- assertions$assert_duplicates_resolved_by_filtering

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

  # Get valid bill types from state config
  valid_bill_types <- data$state_config$bill_types

  # First filter to this term
  ss_term <- data$ss_bills %>%
    filter(.data$term == !!term)

  cli_log(glue("SS bills for term before type filtering: {nrow(ss_term)}"))

  # Filter to valid bill types
  ss_filtered <- ss_term %>%
    mutate(bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))) %>%
    filter(.data$bill_type %in% valid_bill_types) %>%
    select(-"bill_type")

  cli_log(glue("SS bills after type filtering: {nrow(ss_filtered)}"))

  # Check for SS bills that don't exist in bill_details
  # Use state-specific hook to determine which missing SS bills are genuinely
  # missing vs. intentionally excluded (e.g., committee-sponsored bills)
  cli_log("Checking for missing SS bills...")
  if (!is.null(data$state_config$get_missing_ss_bills)) {
    missing_ss_bills <- data$state_config$get_missing_ss_bills(
      ss_filtered, data$bill_details, data$all_bill_details
    )
  } else {
    # Default: flag any SS bill in all_bill_details but not in bill_details
    missing_ss_bills <- ss_filtered %>%
      anti_join(data$bill_details, by = c("bill_id", "term")) %>%
      semi_join(data$all_bill_details, by = "bill_id") %>%
      select("bill_id", "term")
  }

  assert_no_missing_ss(missing_ss_bills)

  # Now apply deduplication
  ss_deduped <- ss_filtered %>%
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

  cli_log(glue("SS bills after deduplication: {nrow(ss_deduped)}"))

  # Check for duplicate SS bills that would cause join issues
  duplicate_ss_bills <- ss_deduped %>%
    group_by(.data$bill_id) %>%
    summarise(count = n(), .groups = "drop")
  assert_join_wont_yield_dupes(duplicate_ss_bills)

  # Check for bill_details duplicates that might cause join issues
  bill_details_dupes <- data$bill_details %>%
    group_by(.data$bill_id, .data$term) %>%
    summarise(
      count = n(),
      sessions = paste(unique(.data$session), collapse = ", "),
      .groups = "drop"
    ) %>%
    filter(.data$count > 1)

  if (nrow(bill_details_dupes) > 0) {
    cli_warn("Bills appearing in multiple sessions within same term:")
    print(bill_details_dupes)
    # Check if state-specific filtering will resolve duplicates
    if (!is.null(data$state_config$drop_unwanted_bills)) {
      assert_duplicates_resolved(
        bill_details_dupes,
        data$bill_details,
        data$state_config$drop_unwanted_bills
      )
    }
  }

  # Record original row counts for post-join validation

  original_row_n <- c(nrow(data$bill_details), nrow(ss_deduped))

  # Join SS flags to bill details
  cli_log("Joining SS flags to bill details...")
  bill_details_with_ss <- data$bill_details %>%
    left_join(
      ss_deduped %>% select("bill_id", "SS"),
      by = "bill_id"
    ) %>%
    mutate(SS = ifelse(is.na(.data$SS), 0, .data$SS))

  # Validate SS join didn't change row counts unexpectedly
  assert_ss_join_validity(bill_details_with_ss, ss_deduped, original_row_n)

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
      0, .data$commem
    ))

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
