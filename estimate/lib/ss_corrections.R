# SS Corrections
# Reviewed corrections to Project Vote Smart (PVS) key-vote rows
#
# A PVS key-vote list sometimes prints the wrong bill number for a vote:
# MA 2025 lists the House's June 4 cannabis vote under H 4240, the number of
# the FY2026 budget. The saved HTML and the parsed SS CSV keep what PVS
# published, so both can always be regenerated; the reviewed correction lives
# in .data/<STATE>/review/<STATE>_SS_Corrections_<YEAR>.csv, and the state's
# transform_ss_bills hook applies it.
#
# Columns: bill_id and pvs_title identify the SS row (bill_id as clean_data
# leaves it, e.g. "H4240" or "HB0636"; pvs_title exactly as in the SS CSV);
# corrected_bill_id is the bill the vote was on, in the same format; evidence
# records how that was established. A correction that matches no SS row of its
# year stops estimation, because then the SS file or the correction changed.

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
logging <- source(file.path(repo_root, "utils/logging.R"),
                  local = TRUE)$value
terms <- source(file.path(repo_root, "utils/term_to_inputs.R"),
                local = TRUE)$value

# Extract functions
cli_log <- logging$cli_log
cli_error <- logging$cli_error

# Load required libraries
library(dplyr)
library(glue)

#' Apply reviewed SS corrections for a state and term
#'
#' Reads <STATE>_SS_Corrections_<YEAR>.csv for each year of the term, where
#' present, and replaces bill_id on the SS rows each correction identifies.
#'
#' @param ss_bills SS bills for the term (with year, bill_id, Title)
#' @param state State postal code
#' @param term Term string (e.g., "2025_2026")
#' @return ss_bills with corrected bill IDs
apply_ss_corrections <- function(ss_bills, state, term) {
  review_dir <- file.path(repo_root, ".data", state, "review")
  for (year in terms$term_years(state, term)) {
    path <- file.path(review_dir, glue("{state}_SS_Corrections_{year}.csv"))
    if (!file.exists(path)) next
    corrections <- readr::read_csv(
      path, col_types = readr::cols(.default = "c")
    )
    for (i in seq_len(nrow(corrections))) {
      hit <- ss_bills$year %in% year &
        ss_bills$bill_id %in% corrections$bill_id[i] &
        ss_bills$Title %in% corrections$pvs_title[i]
      if (!any(hit)) {
        cli_error(glue(
          "SS correction {corrections$bill_id[i]} -> ",
          "{corrections$corrected_bill_id[i]} in {basename(path)} ",
          "matches no {state} {year} SS row"
        ))
        stop("Unmatched SS correction")
      }
      ss_bills$bill_id[hit] <- corrections$corrected_bill_id[i]
    }
    cli_log(glue(
      "  Applied {nrow(corrections)} SS correction(s) from {basename(path)}"
    ))
  }
  ss_bills
}

# Export functions
list(apply_ss_corrections = apply_ss_corrections)
