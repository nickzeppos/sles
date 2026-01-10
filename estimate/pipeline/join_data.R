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

  # Get verbose flag from data
  verbose <- if (!is.null(data$verbose)) data$verbose else TRUE

  # Get valid bill types from state config
  valid_bill_types <- data$state_config$bill_types

  # First filter to this term
  ss_term <- data$ss_bills %>%
    filter(.data$term == !!term)

  # Filter to valid bill types
  ss_filtered <- ss_term %>%
    mutate(bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))) %>%
    filter(.data$bill_type %in% valid_bill_types) %>%
    select(-"bill_type")

  # Check for SS bills that don't exist in bill_details
  # Use state-specific hook to determine which missing SS bills are genuinely
  # missing vs. intentionally excluded (e.g., committee-sponsored bills)
  if (!is.null(data$state_config$get_missing_ss_bills)) {
    genuinely_missing_ss_bills <- data$state_config$get_missing_ss_bills(
      ss_filtered, data$bill_details, data$all_bill_details
    )
  } else {
    # Default: flag any SS bill in all_bill_details but not in bill_details
    genuinely_missing_ss_bills <- ss_filtered %>%
      anti_join(data$bill_details, by = c("bill_id", "term")) %>%
      semi_join(data$all_bill_details, by = "bill_id") %>%
      select("bill_id", "term")
  }

  assert_no_missing_ss(genuinely_missing_ss_bills, verbose)

  # Track all SS bills missing from bill_details (including intentional exclusions)
  # for use in post-join validation
  all_missing_from_details <- ss_filtered %>%
    anti_join(data$bill_details, by = c("bill_id", "term")) %>%
    pull("bill_id")

  # Apply state-specific SS enrichment if available
  # Some states (e.g., AZ) have multiple sessions per term where bill numbers
  # restart. For these states, we need to map year → session so SS bills can
  # join correctly to bill_details.
  if (!is.null(data$state_config$enrich_ss_with_session)) {
    ss_filtered <- data$state_config$enrich_ss_with_session(ss_filtered, term)
  }

  # Now apply deduplication
  # Deduplicate by (bill_id, term, Title) - year is ignored
  # Bills with same bill_id but different Titles are treated as distinct
  # Bills with same bill_id AND same Title are true duplicates and will
  # trigger manual review downstream via duplicate assertions
  ss_deduped <- ss_filtered %>%
    distinct(.data$bill_id, .data$term, .data$Title, .keep_all = TRUE)

  # Check for duplicate SS bills that would cause join issues
  # If session is available, group by (bill_id, session)
  if ("session" %in% names(ss_deduped)) {
    duplicate_ss_bills <- ss_deduped %>%
      group_by(.data$bill_id, .data$session) %>%
      summarise(count = n(), .groups = "drop") %>%
      filter(.data$count > 1)
  } else {
    duplicate_ss_bills <- ss_deduped %>%
      group_by(.data$bill_id) %>%
      summarise(count = n(), .groups = "drop")
  }
  assert_join_wont_yield_dupes(duplicate_ss_bills, verbose)

  # Record original row counts for post-join validation
  original_row_n <- c(nrow(data$bill_details), nrow(ss_deduped))

  # Join SS flags to bill details
  # If SS data has session, join on (bill_id, session) to handle states
  # where bill numbers restart each session (e.g., AZ)
  if ("session" %in% names(ss_deduped)) {
    bill_details_with_ss <- data$bill_details %>%
      left_join(
        ss_deduped %>% select("bill_id", "session", "SS"),
        by = c("bill_id", "session")
      ) %>%
      mutate(SS = ifelse(is.na(.data$SS), 0, .data$SS))
  } else {
    bill_details_with_ss <- data$bill_details %>%
      left_join(
        ss_deduped %>% select("bill_id", "SS"),
        by = "bill_id"
      ) %>%
      mutate(SS = ifelse(is.na(.data$SS), 0, .data$SS))
  }

  # Validate SS join didn't change row counts unexpectedly
  assert_ss_join_validity(bill_details_with_ss, ss_deduped, original_row_n, verbose)

  # Check if any SS bills were ambiguously assigned (same SS row matched
  # to multiple bill_details rows). This would indicate join duplication.
  # Note: It's valid for the same bill_id to have SS=1 in multiple sessions
  # if PVS designated both versions as substantive & significant.
  # We only warn if the join created unexpected duplicates (e.g., one SS row
  # matched to multiple sessions when it should have matched to one).

  # Count how many bill_details rows each SS bill matched to
  if ("session" %in% names(ss_deduped)) {
    # For states with session-based joining, check if any (bill_id, session)
    # pair in SS matched to multiple bill_details rows (shouldn't happen)
    ss_with_matches <- ss_deduped %>%
      select("bill_id", "session") %>%
      left_join(
        bill_details_with_ss %>%
          filter(.data$SS == 1) %>%
          group_by(.data$bill_id, .data$session) %>%
          summarise(match_count = n(), .groups = "drop"),
        by = c("bill_id", "session")
      ) %>%
      filter(.data$match_count > 1)

    if (nrow(ss_with_matches) > 0) {
      cli_warn(paste(
        "SS bills matched to multiple rows - unexpected join duplication.",
        "Each (bill_id, session) in SS should match exactly one bill."
      ))
      print(ss_with_matches)
    }
  }

  # Join commemorative flags to bill details
  # Note: Join on bill_id, term, AND session to handle special sessions
  # (e.g., SB0001 exists in both regular and special sessions)
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

  # Validate no missing SS bills from details (excluding intentional exclusions)
  ss_in_details <- ss_deduped$bill_id
  ss_in_joined <- bill_details_joined %>%
    filter(.data$SS == 1) %>%
    pull("bill_id")

  missing_ss <- setdiff(ss_in_details, ss_in_joined)
  # Exclude bills that were already identified as intentionally missing
  genuinely_missing_post_join <- setdiff(missing_ss, all_missing_from_details)

  if (length(genuinely_missing_post_join) > 0) {
    cli_warn(glue(
      "Warning: {length(genuinely_missing_post_join)} SS bills not found in ",
      "bill details:"
    ))
    cli_warn(glue("  {paste(genuinely_missing_post_join, collapse = ', ')}"))
  }

  # Return joined data
  list(
    bill_details = bill_details_joined,
    all_bill_details = data$all_bill_details,
    bill_history = data$bill_history,
    ss_bills = ss_deduped,
    commem_bills = data$commem_bills,
    legiscan = data$legiscan,
    state_config = data$state_config,
    verbose = data$verbose
  )
}

# Export function
list(join_data = join_data)
