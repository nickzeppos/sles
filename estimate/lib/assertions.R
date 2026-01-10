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
#' @param verbose Show detailed logging
assert_no_duplicates <- function(bill_details, verbose = TRUE) {
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
    if (verbose) cli_log("No duplicate bills in scraped bill data")
  }
}

#' Assert no "by request" sponsored bills remain
#'
#' Checks for bills with "by request" in LES_sponsor and exits if found.
#'
#' @param bill_details Dataframe of bill details
#' @param verbose Show detailed logging
assert_no_by_request_sponsored_bills <- function(bill_details, verbose = TRUE) {
  if (any(grepl("by request", bill_details$LES_sponsor))) {
    cli_warn("Bills with 'by request' found in LES_sponsor after cleanup")
    rows_with_by_request <- which(grepl("by request", bill_details$LES_sponsor))
    cli_warn(glue("Rows with 'by request': ",
                  "{paste(rows_with_by_request, collapse = ', ')}"))
    cli_error("Exiting on 'by request' detection.")
    stop("'by request' bills found")
  } else {
    if (verbose) cli_log("No 'by request' bills found in LES_sponsor after cleanup.")
  }
}

#' Assert no SS bills missing from bill details
#'
#' Checks if any SS bills couldn't be joined to bill details.
#'
#' @param missing_ss_bills Dataframe of SS bills not found in bill_details
#' @param verbose Show detailed logging
assert_no_missing_ss_from_details <- function(missing_ss_bills, verbose = TRUE) {
  if (nrow(missing_ss_bills) > 0) {
    cli_warn(glue("Number of SS bills missing from bills data: ",
                  "{nrow(missing_ss_bills)}"))
    print(missing_ss_bills)
    cli_error("Exiting on missing SS bills from bills data.")
    stop("Missing SS bills")
  } else {
    if (verbose) cli_log("No SS bills missing from bills data.")
  }
}

#' Assert SS join won't yield duplicates
#'
#' Checks for duplicate bill_ids in SS data before joining.
#'
#' @param duplicate_ss_bills Dataframe with bill_id and count columns
#' @param verbose Show detailed logging
assert_join_wont_yield_dupes <- function(duplicate_ss_bills, verbose = TRUE) {
  dupes <- duplicate_ss_bills %>% filter(.data$count > 1)
  if (nrow(dupes) > 0) {
    cli_warn("Duplicate ss bills found after join.")
    print(dupes %>% select("bill_id", "count"))
    cli_error("Exiting on duplicate ss bills after join.")
    stop("Duplicate SS bills")
  } else {
    if (verbose) cli_log("No duplicate ss bills found after join.")
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
#' @param verbose Show detailed logging
assert_ss_join_bill_details_validity <- function(bills2, ss_term2, orig_row_n, verbose = TRUE) {
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
    if (verbose) cli_log("Row counts valid after joining bills and ss_term.")
  }
}

#' Assert no unmatched legiscan records
#'
#' Checks for legiscan legislators not matched to bill sponsors.
#' Warns about unmatched records but allows pipeline to continue to the
#' validate_legislators stage where proper manual review happens.
#'
#' @param unmatched_legiscan Dataframe of unmatched legiscan records
#' @param verbose Show detailed logging
assert_no_unmatched_legiscan_records <- function(unmatched_legiscan, verbose = TRUE) {
  if (nrow(unmatched_legiscan) > 0) {
    cli_warn("Legiscan records that didn't match to bill sponsors:")
    print(unmatched_legiscan)
    cli_warn(paste(
      "These will be handled in the validate_legislators stage where you can",
      "review and decide if they should be included with LES=0 or excluded."
    ))
  }
  if (verbose) {
    cli_log(glue("{nrow(unmatched_legiscan)} legiscan records unmatched ",
                 "to bill sponsors."))
  }
}

#' Assert no unmatched bill sponsors
#'
#' Checks for bill sponsors not matched to legiscan legislators.
#' Warns about unmatched records - these are sponsors in bill data who don't
#' appear in the legiscan roster, which typically indicates data quality issues.
#'
#' @param unmatched_sponsors Dataframe of unmatched sponsors
#' @param verbose Show detailed logging
assert_no_unmatched_bill_sponsors <- function(unmatched_sponsors, verbose = TRUE) {
  if (nrow(unmatched_sponsors) > 0) {
    cli_warn("Bill sponsors that didn't match to Legiscan records:")
    print(unmatched_sponsors)
    cli_warn(paste(
      "These sponsors appear in bills but not in legiscan roster.",
      "This may indicate name mismatches or data quality issues."
    ))
  }
  if (verbose) {
    cli_log(glue("{nrow(unmatched_sponsors)} bill sponsors unmatched ",
                 "to legiscan records."))
  }
}

#' Assert no bills missing sponsors
#'
#' Checks for bills without identified sponsors. If found, prints detailed
#' diagnostic information to help resolve via fix_names() or custom_match.
#'
#' @param bills_missing_sponsors Dataframe of bills without sponsors
#' @param legis_data Legislator data with data_name, sponsor, chamber columns
assert_no_bills_missing_sponsors <- function(bills_missing_sponsors,
                                             legis_data = NULL) {
  if (nrow(bills_missing_sponsors) > 0) {
    n_bills <- nrow(bills_missing_sponsors)
    unmatched_sponsors <- unique(bills_missing_sponsors$sponsor)
    n_sponsors <- length(unmatched_sponsors)

    cli_error(glue("\n",
      "===========================================================\n",
      "UNMATCHED SPONSORS: {n_sponsors} sponsor(s) on {n_bills} bill(s)\n",
      "===========================================================\n"))

    # Group bills by sponsor and show sample bill IDs + chamber
    for (spn in sort(unmatched_sponsors)) {
      sponsor_bills <- bills_missing_sponsors %>%
        filter(.data$sponsor == spn)
      sample_bills <- head(sponsor_bills$bill_id, 5)
      n_total <- nrow(sponsor_bills)
      bill_str <- paste(sample_bills, collapse = ", ")
      if (n_total > 5) {
        bill_str <- paste0(bill_str, " ... (", n_total, " total)")
      }
      # Derive chamber from bill_id prefix
      chamber <- ifelse(substring(sample_bills[1], 1, 1) == "H", "House", "Senate")
      cli_warn(glue("  \"{spn}\" ({chamber}): {bill_str}"))

      # Show similar legislators if legis_data provided
      if (!is.null(legis_data)) {
        # Extract last name from unmatched sponsor
        sponsor_parts <- strsplit(spn, " ")[[1]]
        last_name <- sponsor_parts[length(sponsor_parts)]

        # Find legislators with matching last name in same chamber
        chamber_code <- substring(sample_bills[1], 1, 1)
        similar <- legis_data %>%
          filter(tolower(.data$chamber) == tolower(chamber_code)) %>%
          filter(grepl(last_name, .data$sponsor, ignore.case = TRUE)) %>%
          select("sponsor", "data_name", "district") %>%
          distinct()

        if (nrow(similar) > 0) {
          cli_log("    Potential matches in legiscan:")
          for (i in seq_len(nrow(similar))) {
            cli_log(glue("      - {similar$sponsor[i]} ",
                         "(data_name: \"{similar$data_name[i]}\", ",
                         "{similar$district[i]})"))
          }
        }
      }
    }

    cli_error(glue("\n",
      "-----------------------------------------------------------\n",
      "TO RESOLVE:\n",
      "1. Look up bill IDs above on the legislature website\n",
      "2. Identify the correct legislator for each sponsor name\n",
      "3. Add to fix_names() if sponsor name is a variant:\n",
      "     names[names == \"variant\"] <- \"canonical\"\n",
      "4. OR add to custom_match in reconcile_legiscan_with_sponsors():\n",
      "     \"legiscan-key\" = \"sponsor-key\"\n",
      "-----------------------------------------------------------\n"))

    stop(glue("Pipeline halted: {n_sponsors} unmatched sponsor(s). ",
              "See above for resolution steps."))
  }
}

#' Assert duplicates resolved by filtering
#'
#' Tests if duplicate bills will be filtered out by downstream logic.
#'
#' @param bill_details_dupes Dataframe of duplicate bills
#' @param bill_details Full bill details dataframe
#' @param drop_unwanted_bills_fn Function to filter unwanted bills
#' @param verbose Show detailed logging
assert_duplicates_resolved_by_filtering <- function(bill_details_dupes,
                                                    bill_details,
                                                    drop_unwanted_bills_fn,
                                                    verbose = TRUE) {
  if (nrow(bill_details_dupes) == 0) {
    return(invisible())
  }

  if (verbose) cli_log("Testing if duplicate bills will be filtered out by downstream...")

  duplicate_bill_ids <- bill_details_dupes$bill_id
  duplicate_bills_detail <- bill_details %>%
    filter(.data$bill_id %in% duplicate_bill_ids) %>%
    select("bill_id", "session", "primary_sponsor", "LES_sponsor")

  filtered_bills <- drop_unwanted_bills_fn(duplicate_bills_detail)

  if (verbose) {
    cli_log(glue("Original duplicate bills: {nrow(duplicate_bills_detail)}"))
    cli_log(glue("After filtering: {nrow(filtered_bills)}"))
  }

  if (nrow(filtered_bills) > 0) {
    remaining_dupes <- filtered_bills %>%
      group_by(.data$bill_id) %>%
      summarise(count = n(), .groups = "drop") %>%
      filter(.data$count > 1)

    if (nrow(remaining_dupes) > 0) {
      cli_warn("Bills still duplicated after filtering - may need more logic:")
      print(remaining_dupes)
    } else {
      if (verbose) cli_log("Filtering successfully resolves all duplicates.")
    }
  } else {
    if (verbose) cli_log("Filtering removes all duplicate bills - issue resolved.")
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
