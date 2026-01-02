# Assertion Utilities
# Data validation functions for pipeline stages

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
logging <- source(file.path(repo_root, "utils/logging.R"),
                  local = TRUE)$value

# Extract functions
cli_log <- logging$cli_log
cli_warn <- logging$cli_warn
cli_error <- logging$cli_error
cli_prompt <- logging$cli_prompt

# Load required libraries
library(dplyr)
library(glue)

#' Assert no duplicate bills in bill_details
#'
#' Checks for duplicate rows and exits with error if found.
#'
#' @param bill_details Dataframe of bill details
assert_no_duplicates <- function(bill_details) {
  if (nrow(bill_details) != nrow(distinct(bill_details))) {
    cli_warn("Duplicate bills found after cleaning!")
    cli_warn(glue("Distinct rows: {nrow(distinct(bill_details))}, ",
                  "Total rows: {nrow(bill_details)}"))
    collapsed_row_numbers <- bill_details %>%
      group_by(across(everything())) %>%
      filter(n() > 1) %>%
      ungroup() %>%
      pull() %>%
      unique() %>%
      paste(collapse = ", ")
    cli_warn(glue("Duplicated row numbers: {collapsed_row_numbers}"))
    cli_error("Breaking on dupe detection.")
    stop("Duplicate bills found")
  } else {
    cli_log("No duplicate bills in scraped bill data")
  }
}

#' Assert no "by request" sponsored bills remain
#'
#' Checks for bills with "by request" in LES_sponsor and exits if found.
#'
#' @param bill_details Dataframe of bill details
assert_no_by_request_sponsored_bills <- function(bill_details) {
  if (any(grepl("by request", bill_details$LES_sponsor))) {
    cli_warn("Bills with 'by request' found in LES_sponsor after cleanup")
    rows_with_by_request <- which(grepl("by request", bill_details$LES_sponsor))
    cli_warn(glue("Rows with 'by request': ",
                  "{paste(rows_with_by_request, collapse = ', ')}"))
    cli_error("Exiting on 'by request' detection.")
    stop("'by request' bills found")
  } else {
    cli_log("No 'by request' bills found in LES_sponsor after cleanup.")
  }
}

#' Assert no SS bills missing from bill details
#'
#' Checks if any SS bills couldn't be joined to bill details.
#'
#' @param missing_ss_bills Dataframe of SS bills not found in bill_details
assert_no_missing_ss_from_details <- function(missing_ss_bills) {
  if (nrow(missing_ss_bills) > 0) {
    cli_warn(glue("Number of SS bills missing from bills data: ",
                  "{nrow(missing_ss_bills)}"))
    print(missing_ss_bills)
    cli_error("Exiting on missing SS bills from bills data.")
    stop("Missing SS bills")
  } else {
    cli_log("No SS bills missing from bills data.")
  }
}

#' Assert SS join won't yield duplicates
#'
#' Checks for duplicate bill_ids in SS data before joining.
#'
#' @param duplicate_ss_bills Dataframe with bill_id and count columns
assert_join_wont_yield_dupes <- function(duplicate_ss_bills) {
  dupes <- duplicate_ss_bills %>% filter(.data$count > 1)
  if (nrow(dupes) > 0) {
    cli_warn("Duplicate ss bills found after join.")
    print(dupes %>% select("bill_id", "count"))
    cli_error("Exiting on duplicate ss bills after join.")
    stop("Duplicate SS bills")
  } else {
    cli_log("No duplicate ss bills found after join.")
  }
}

#' Assert SS join to bill_details validity
#'
#' Validates row counts after joining SS to bill_details. Prompts user
#' if one-to-many relationships are detected.
#'
#' @param bills2 Dataframe of bills after join
#' @param ss_term2 Dataframe of SS bills after join
#' @param orig_row_n Vector of original row counts c(bills, ss_term)
assert_ss_join_bill_details_validity <- function(bills2, ss_term2, orig_row_n) {
  if (!identical(c(nrow(bills2), nrow(ss_term2)), orig_row_n)) {
    cli_warn("Row counts changed after joining bills and ss_term.")
    cli_warn(glue("Bill rows: pre-join {orig_row_n[1]} -> ",
                  "post-join {nrow(bills2)}"))
    cli_warn(glue("PVS rows: pre-join {orig_row_n[2]} -> ",
                  "post-join {nrow(ss_term2)}"))
    cli_warn(paste("If we arrive here, there's likely at least a one-to-many",
                   "relationship between PVS data and scraped bill data,",
                   "when joined on bill_id and term."))

    available_cols <- intersect(c("bill_id", "term", "session", "year"),
                                names(ss_term2))
    sort_col <- if ("year" %in% available_cols) "year" else "bill_id"

    one_to_many_check <- ss_term2 %>%
      group_by(.data$bill_id, .data$term) %>%
      filter(n() > 1) %>%
      select(all_of(available_cols)) %>%
      ungroup() %>%
      arrange(.data$bill_id, !!sym(sort_col))

    distinct_bill_ids <- unique(one_to_many_check$bill_id)
    cli_warn(glue("Number of unique bill_ids with one-to-many relationships: ",
                  "{length(distinct_bill_ids)}"))
    print(one_to_many_check)

    proceed_flag <- cli_prompt("Do you want to proceed? [y/n]: ")
    if (tolower(proceed_flag) != "y") {
      cli_error("Exiting.")
      stop("User cancelled on SS join validation")
    }
  } else {
    cli_log("Row counts valid after joining bills and ss_term.")
  }
}

#' Assert no unmatched legiscan records
#'
#' Checks for legiscan legislators not matched to bill sponsors.
#' Prompts user to decide whether to continue.
#'
#' @param unmatched_legiscan Dataframe of unmatched legiscan records
assert_no_unmatched_legiscan_records <- function(unmatched_legiscan) {
  if (nrow(unmatched_legiscan) > 0) {
    cli_warn("Legiscan records that didn't match to bill sponsors:")
    print(unmatched_legiscan)
    proceed_flag <- cli_prompt("Break on unmatched legiscan records? [y/n]: ")
    if (tolower(proceed_flag) == "y") {
      cli_error("Exiting on unmatched legiscan records.")
      stop("Unmatched legiscan records")
    }
  }
  cli_log(glue("{nrow(unmatched_legiscan)} legiscan records unmatched ",
               "to bill sponsors."))
}

#' Assert no unmatched bill sponsors
#'
#' Checks for bill sponsors not matched to legiscan legislators.
#' Prompts user to decide whether to continue.
#'
#' @param unmatched_sponsors Dataframe of unmatched sponsors
assert_no_unmatched_bill_sponsors <- function(unmatched_sponsors) {
  if (nrow(unmatched_sponsors) > 0) {
    cli_warn("Bill sponsors that didn't match to Legiscan records:")
    print(unmatched_sponsors)
    proceed_flag <- cli_prompt("Break on unmatched bill sponsors? [y/n]: ")
    if (tolower(proceed_flag) == "y") {
      cli_error("Exiting on unmatched bill sponsors.")
      stop("Unmatched bill sponsors")
    }
  }
  cli_log(glue("{nrow(unmatched_sponsors)} bill sponsors unmatched ",
               "to legiscan records."))
}

#' Assert no bills missing sponsors
#'
#' Checks for bills without identified sponsors and exits if found.
#'
#' @param bills_missing_sponsors Dataframe of bills without sponsors
assert_no_bills_missing_sponsors <- function(bills_missing_sponsors) {
  if (nrow(bills_missing_sponsors) > 0) {
    cli_warn("Bills found with missing id'd sponsors:")
    print(bills_missing_sponsors)
    cli_error("Exiting on bills with missing id'd sponsors.")
    stop("Bills missing sponsors")
  }
}

#' Assert duplicates resolved by filtering
#'
#' Tests if duplicate bills will be filtered out by downstream logic.
#'
#' @param bill_details_dupes Dataframe of duplicate bills
#' @param bill_details Full bill details dataframe
#' @param drop_unwanted_bills_fn Function to filter unwanted bills
assert_duplicates_resolved_by_filtering <- function(bill_details_dupes,
                                                    bill_details,
                                                    drop_unwanted_bills_fn) {
  if (nrow(bill_details_dupes) == 0) {
    return(invisible())
  }

  cli_log("Testing if duplicate bills will be filtered out by downstream...")

  duplicate_bill_ids <- bill_details_dupes$bill_id
  duplicate_bills_detail <- bill_details %>%
    filter(.data$bill_id %in% duplicate_bill_ids) %>%
    select("bill_id", "session", "primary_sponsor", "LES_sponsor")

  filtered_bills <- drop_unwanted_bills_fn(duplicate_bills_detail)

  cli_log(glue("Original duplicate bills: {nrow(duplicate_bills_detail)}"))
  cli_log(glue("After filtering: {nrow(filtered_bills)}"))

  if (nrow(filtered_bills) > 0) {
    remaining_dupes <- filtered_bills %>%
      group_by(.data$bill_id) %>%
      summarise(count = n(), .groups = "drop") %>%
      filter(.data$count > 1)

    if (nrow(remaining_dupes) > 0) {
      cli_warn("Bills still duplicated after filtering - may need more logic:")
      print(remaining_dupes)
    } else {
      cli_log("Filtering successfully resolves all duplicates.")
    }
  } else {
    cli_log("Filtering removes all duplicate bills - issue resolved.")
  }
}

# Export functions
list(
  assert_no_duplicates = assert_no_duplicates,
  assert_no_by_request_sponsored_bills = assert_no_by_request_sponsored_bills,
  assert_no_missing_ss_from_details = assert_no_missing_ss_from_details,
  assert_join_wont_yield_dupes = assert_join_wont_yield_dupes,
  assert_ss_join_bill_details_validity = assert_ss_join_bill_details_validity,
  assert_no_unmatched_legiscan_records = assert_no_unmatched_legiscan_records,
  assert_no_unmatched_bill_sponsors = assert_no_unmatched_bill_sponsors,
  assert_no_bills_missing_sponsors = assert_no_bills_missing_sponsors,
  assert_duplicates_resolved_by_filtering = assert_duplicates_resolved_by_filtering
)
