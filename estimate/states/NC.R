# North Carolina (NC) State Configuration
#
# NC has regular sessions where bills carry over within the term (one biennium).
# Special session bill numbers restart.
# Multiple primary sponsors permitted + committee sponsorship permitted.
#
# Bill types: HB (House Bill), SB (Senate Bill)
# bill_type column is messy and needs cleaning; filter to "Bill" after cleaning.
# Bill IDs use bill_num in raw data, renamed to bill_id.
#
# Name matching: sponsors use last-name-only format (with first initial for
# disambiguation). Legiscan uses last name (with first initial if duplicates).

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
logging <- source(file.path(repo_root, "utils/logging.R"), local = TRUE)$value
libs <- source(file.path(repo_root, "utils/libs.R"), local = TRUE)$value
strings <- source(file.path(repo_root, "utils/strings.R"), local = TRUE)$value

# Extract functions
cli_log <- logging$cli_log
cli_warn <- logging$cli_warn
require_libs <- libs$require_libs
standardize_accents <- strings$standardize_accents

# Load required libraries
require_libs()
library(dplyr)
library(glue)
library(stringr)
library(readr)

# Stage 1 Hook: Get bill file suffix
#' Get bill file suffix for NC
#'
#' NC bill files are named like NC_Bill_Details_2023_RS.csv
#'
#' @param term Term string (e.g., "2023_2024")
#' @return File suffix string (e.g., "2023_RS")
get_bill_file_suffix <- function(term) {
  term_start_year <- strsplit(term, "_")[[1]][1]
  paste0(term_start_year, "_RS")
}

# State configuration
# nolint start: line_length_linter
nc_config <- list(
  bill_types = c("HB", "SB"),

  # Step terms for evaluating bill history (from old script lines 422-434)
  step_terms = list(
    aic = c(
      "^reptd", "assigned to.+subcomm"
    ),
    abc = c(
      "^reptd fav", "^reptd without", "^reptd com sub", "^reptd as amend",
      "amend adopted", "amend failed", "amend pending",
      "amends ruled material", "amend recon",
      "added to calendar", "placed on cal for", "passed 2nd", "failed 2nd"
    ),
    pass = c(
      "passed 3rd reading", "passed 2nd \\& 3rd reading", "^adopted$",
      "message sent to", "^rec from house", "^rec from senate"
    ),
    law = c(
      "signed by gov", "ch\\. sl [0-9]+", "^ratified", "ch.[0-9]+"
    )
  )
)
# nolint end: line_length_linter

# Stage 1.5 Hook: Preprocess raw data
#' Preprocess raw data for North Carolina
#'
#' Renames bill_num -> bill_id in both dataframes.
#' From old script lines 128, 402.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  bill_details <- bill_details %>%
    rename(bill_id = "bill_num")

  bill_history <- bill_history %>%
    rename(bill_id = "bill_num")

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 2 Hook: Clean bill details
#' Clean bill details for North Carolina
#'
#' NC-specific transformations:
#' - Clean messy bill_type column (strip parenthetical text, SL suffixes, etc.)
#' - Filter to bill_type == "Bill" (not resolutions)
#' - Zero-pad bill_id to 4 digits
#' - Accent normalization on sponsors
#' - Derive LES_sponsor as first primary sponsor
#' - Remove committee-sponsored bills
#' - Drop empty/NA sponsors
#'
#' From old script lines 131-382.
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @param verbose Show detailed logging (default TRUE)
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Clean bill type (old script lines 131-136)
  bill_details$bill_type <- gsub("\\(.+\\)", "", bill_details$bill_type)
  bill_details$bill_type <- gsub("\\/ SL.+", "", bill_details$bill_type)
  bill_details$bill_type <- ifelse(
    grepl("^Bill +\\/ Res\\.", bill_details$bill_type),
    "Resolution",
    bill_details$bill_type
  )
  bill_details$bill_type <- gsub("\\/.+", "", bill_details$bill_type)
  bill_details$bill_type <- str_trim(gsub("  +", " ", bill_details$bill_type))

  if (verbose) {
    cli_log("Bill type distribution:")
    print(table(bill_details$bill_type))
  }

  # Store unfiltered version (old script line 141)
  all_bill_details <- bill_details

  # Filter to bills only (old script line 142)
  bill_details <- filter(bill_details, .data$bill_type == "Bill") %>%
    select(-"bill_type")

  if (verbose) {
    cli_log(glue("After filtering to bills: {nrow(bill_details)} bills"))
  }

  # Zero-pad bill_id to 4 digits (ensure consistent format)
  bill_details <- bill_details %>%
    mutate(
      bill_id = paste0(
        gsub("[0-9].+", "", .data$bill_id),
        str_pad(gsub("^[A-Z]+", "", .data$bill_id), 4, pad = "0")
      )
    )

  # Accent normalization (old script lines 151-163)
  bill_details$primary_sponsors <- standardize_accents(
    bill_details$primary_sponsors
  )
  bill_details$cosponsors <- standardize_accents(bill_details$cosponsors)

  # Lowercase (old script lines 156, 163)
  bill_details$primary_sponsors <- tolower(bill_details$primary_sponsors)
  bill_details$cosponsors <- tolower(bill_details$cosponsors)

  # Strip (primary) tag (old script line 166)
  bill_details$primary_sponsors <- str_trim(
    gsub("\\(primary\\)", "", bill_details$primary_sponsors)
  )

  # Term-specific name disambiguation (old script lines 169-230)
  # None needed for 2023_2024 - all blocks are for older terms
  if (term == "1993_1994") {
    bill_details$primary_sponsors <- gsub(
      "winner of buncombe", "d. winner", bill_details$primary_sponsors
    )
    bill_details$primary_sponsors <- gsub(
      "winner of mecklenburg", "l. winner", bill_details$primary_sponsors
    )
    bill_details$cosponsors <- gsub(
      "winner of buncombe", "d. winner", bill_details$cosponsors
    )
    bill_details$cosponsors <- gsub(
      "winner of mecklenburg", "l. winner", bill_details$cosponsors
    )
  }
  if (term %in% c("1993_1994", "1995_1996", "2001_2002")) {
    bill_details$primary_sponsors <- gsub(
      "martin of guilford", "w. martin", bill_details$primary_sponsors
    )
    bill_details$cosponsors <- gsub(
      "martin of guilford", "w. martin", bill_details$cosponsors
    )
  }
  if (term == "1995_1996") {
    bill_details$primary_sponsors <- gsub(
      "ballentine", "ballantine", bill_details$primary_sponsors
    )
    bill_details$cosponsors <- gsub(
      "ballentine", "ballantine", bill_details$cosponsors
    )
  }
  # nolint start: line_length_linter
  if (term %in% c("1993_1994", "1995_1996", "1997_1998", "1999_2000", "2001_2002")) {
    bill_details$primary_sponsors <- gsub(
      "martin of pitt", "r. martin", bill_details$primary_sponsors
    )
    bill_details$cosponsors <- gsub(
      "martin of pitt", "r. martin", bill_details$cosponsors
    )
  }
  # nolint end: line_length_linter
  if (term == "2001_2002") {
    bill_details$primary_sponsors <- gsub(
      "shaw of cumberland", "l. shaw", bill_details$primary_sponsors
    )
    bill_details$cosponsors <- gsub(
      "shaw of cumberland", "l. shaw", bill_details$cosponsors
    )
    bill_details$primary_sponsors <- gsub(
      "shaw of guilford", "r. shaw", bill_details$primary_sponsors
    )
    bill_details$cosponsors <- gsub(
      "shaw of guilford", "r. shaw", bill_details$cosponsors
    )
  }
  if (term %in% c("2005_2006", "2007_2008", "2009_2010")) {
    bill_details$primary_sponsors <- gsub(
      "berger of franklin", "d. berger", bill_details$primary_sponsors
    )
    bill_details$cosponsors <- gsub(
      "berger of franklin", "d. berger", bill_details$cosponsors
    )
    bill_details$primary_sponsors <- gsub(
      "berger of rockingham", "p. berger", bill_details$primary_sponsors
    )
    bill_details$cosponsors <- gsub(
      "berger of rockingham", "p. berger", bill_details$cosponsors
    )
  }
  if (term == "2005_2006") {
    bill_details$primary_sponsors <- gsub(
      "^jones", "earl jones", bill_details$primary_sponsors
    )
    bill_details$primary_sponsors <- gsub(
      "; jones", "; earl jones", bill_details$primary_sponsors
    )
    bill_details$cosponsors <- gsub(
      "^jones", "earl jones", bill_details$cosponsors
    )
    bill_details$cosponsors <- gsub(
      "; jones", "; earl jones", bill_details$cosponsors
    )
  }

  # Derive LES_sponsor: first name before semicolon (old script line 213)
  bill_details$LES_sponsor <- gsub(";.+", "", bill_details$primary_sponsors)
  # Strip trailing dot (old script line 214)
  bill_details$LES_sponsor <- gsub("\\.$", "", bill_details$LES_sponsor)

  # Term-specific LES_sponsor disambiguation (old script lines 219-230)
  if (term == "1993_1994") {
    # nolint start: line_length_linter
    bill_details[
      bill_details$LES_sponsor == "hunt" &
        bill_details$session == "1993-RS" &
        bill_details$bill_id %in% c("H0219", "H0839", "H1083", "H1295"),
    ]$LES_sponsor <- "hunt, judy"
    bill_details[
      bill_details$LES_sponsor == "hunt" &
        substring(bill_details$bill_id, 1, 1) == "H",
    ]$LES_sponsor <- "hunt, john"
    # nolint end: line_length_linter
  } else if (term == "1997_1998") {
    # nolint start: line_length_linter
    hunter_h_bills <- c(
      "H0099", "H0100", "H0278", "H0279", "H0302", "H0367", "H0503",
      "H0504", "H0505", "H0506", "H0651", "H0805", "H0827", "H0944",
      "H1060", "H1178", "H1179", "H1539", "H1689"
    )
    hunter_r_bills <- c(
      "H0191", "H0192", "H0193", "H0507", "H0831", "H0873", "H0893",
      "H0947", "H1132", "H1139", "H1140", "H1158", "H1214", "H1399",
      "H1400"
    )
    bill_details[
      bill_details$LES_sponsor == "hunter" &
        bill_details$session == "1997-RS" &
        bill_details$bill_id %in% hunter_h_bills,
    ]$LES_sponsor <- "h. hunter"
    bill_details[
      bill_details$LES_sponsor == "hunter" &
        bill_details$session == "1997-RS" &
        bill_details$bill_id %in% hunter_r_bills,
    ]$LES_sponsor <- "r. hunter"

    wilson_c_bills <- c(
      "H0003", "H0497", "H0536", "H0587", "H0612", "H1162", "H1293",
      "H1317", "H1422", "H1424", "H1429", "H1481", "H1530", "H1702"
    )
    wilson_g_bills <- c("H0143", "H0994", "H1211")
    bill_details[
      bill_details$LES_sponsor == "wilson" &
        bill_details$session == "1997-RS" &
        bill_details$bill_id %in% wilson_c_bills,
    ]$LES_sponsor <- "c. wilson"
    bill_details[
      bill_details$LES_sponsor == "wilson" &
        bill_details$session == "1997-RS" &
        bill_details$bill_id %in% wilson_g_bills,
    ]$LES_sponsor <- "g. wilson"
    # nolint end: line_length_linter
  }

  # Check for by-request bills (old script lines 233-236)
  if (any(grepl(
    "\\(by request\\)|\\(by re\\)| +request",
    bill_details$LES_sponsor
  ))) {
    cli_warn("BY REQUEST bills found - review needed")
  }

  # Committee-sponsored bill removal (old script lines 361-371)
  # nolint start: line_length_linter
  comms <- c(
    "agriculture", "judiciary", "alcoholic beverage", "appropriations",
    "judiciary i", "local and regional government ii", "state government",
    "congressional redistricting", "health and human services",
    "rules, calendar, and operations of the house", "ethics", "rules"
  )
  # nolint end: line_length_linter
  comm_pattern <- paste(comms, collapse = "|")
  comm_bills <- bill_details %>%
    filter(
      grepl(comm_pattern, .data$LES_sponsor) |
        grepl("committee", .data$LES_sponsor)
    )

  if (nrow(comm_bills) > 0) {
    if (verbose) {
      cli_log(glue(
        "Dropping {nrow(comm_bills)} committee-sponsored bills"
      ))
    }
    bill_details <- bill_details %>%
      filter(
        !(grepl(comm_pattern, .data$LES_sponsor) |
          grepl("committee", .data$LES_sponsor))
      )
  }

  # Drop empty/NA sponsors (old script lines 378-381)
  empty_sponsor_bills <- bill_details %>%
    filter(.data$LES_sponsor == "" | is.na(.data$LES_sponsor))

  if (nrow(empty_sponsor_bills) > 0) {
    if (verbose) {
      cli_log(glue(
        "Dropping {nrow(empty_sponsor_bills)} bills without sponsor"
      ))
    }
    bill_details <- bill_details %>%
      filter(!(.data$LES_sponsor == "" | is.na(.data$LES_sponsor)))
  }

  list(
    all_bill_details = all_bill_details,
    bill_details = bill_details
  )
}

# Stage 2 Hook: Clean bill history
#' Clean bill history for North Carolina
#'
#' From old script lines 402-410, 454.
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  # Fix blank/NA chamber for ratified/governor actions (old script line 409)
  exec_pattern <- "^ratified|to gov\\.|signed by gov\\.|^ch\\. sl|^veto"
  needs_fix <- (bill_history$chamber == "" | is.na(bill_history$chamber)) &
    grepl(exec_pattern, tolower(bill_history$action))
  if (any(needs_fix)) {
    bill_history$chamber[needs_fix] <- "Executive"
  }

  bill_history %>%
    mutate(
      term = term,
      # Lowercase action and trim whitespace (old script line 454)
      action = str_trim(tolower(.data$action))
    ) %>%
    arrange(.data$session, .data$bill_id, .data$order)
}

# Stage 4 Hook: Post-evaluate bill (engrossed/rec-from override)
#' NC-specific post-evaluation of bill achievement stages
#'
#' If passed_chamber == 0 but history contains "engrossed" or "^rec from"
#' and the bill appeared in multiple chambers, force
#' action_beyond_comm = passed_chamber = 1.
#'
#' From old script lines 465-467.
#'
#' @param bill_stages Achievement result from evaluate_bill_history
#' @param bill_row The bill row from bill_details
#' @param bill_history Bill history dataframe for this bill
#' @return Modified bill_stages
post_evaluate_bill <- function(bill_stages, bill_row, bill_history) {
  if (bill_stages$passed_chamber == 0 &&
    any(grepl("engrossed|^rec from", tolower(bill_history$action))) &&
    length(unique(bill_history$chamber)) > 1) {
    bill_stages$action_beyond_comm <- 1
    bill_stages$passed_chamber <- 1
  }
  bill_stages
}

# Stage 3 Hook: Transform SS bill IDs
#' Transform SS bill IDs for North Carolina
#'
#' PVS encodes special session bills with unusual IDs like SB30003.
#' The first digit after bill type indicates the special session number.
#' Convert to standard format (e.g., SB30003 -> SB0003).
#' Same pattern as KY.
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return SS bills with transformed bill IDs
transform_ss_bills <- function(ss_bills, term) {
  ss_bills %>%
    mutate(
      bill_id = gsub("^([HS]B)(\\d)(\\d{4})$", "\\1\\3", .data$bill_id)
    )
}

# Stage 3 Hook: Get missing SS bills
#' Identify genuinely missing SS bills for NC
#'
#' Anti-join SS with bill_details, then check all_bill_details for
#' committee-sponsored exclusions. Bills not in all_bill_details at all
#' are data gaps (not scraped) and are logged but not treated as
#' genuinely missing.
#' From old script lines 261-269.
#'
#' @param ss_filtered SS bills dataframe
#' @param bill_details Filtered bill_details dataframe
#' @param all_bill_details Unfiltered bill_details dataframe
#' @return Dataframe of genuinely missing SS bills
get_missing_ss_bills <- function(ss_filtered, bill_details,
                                 all_bill_details) {
  missing_ss <- ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term"))

  if (nrow(missing_ss) == 0) {
    return(missing_ss %>% select("bill_id", "term"))
  }

  # Only consider bills that exist in all_bill_details (unfiltered)
  # Bills not in all_bill_details are data gaps (not scraped)
  missing_in_all <- missing_ss %>%
    semi_join(all_bill_details, by = c("bill_id", "term"))

  if (nrow(missing_in_all) == 0) {
    # All missing SS bills are data gaps, not exclusion errors
    not_in_data <- missing_ss %>%
      anti_join(all_bill_details, by = c("bill_id", "term"))
    if (nrow(not_in_data) > 0) {
      cli_warn(glue(
        "SS bills not found in scraped data (data gap): ",
        "{paste(not_in_data$bill_id, collapse = ', ')}"
      ))
    }
    return(missing_ss %>% filter(FALSE) %>% select("bill_id", "term"))
  }

  # Check committee-sponsored exclusions
  missing_with_info <- missing_in_all %>%
    left_join(
      all_bill_details %>% select("bill_id", "term", "primary_sponsors"),
      by = c("bill_id", "term")
    )

  # Filter to valid bill types and exclude committee-sponsored
  genuinely_missing <- missing_with_info %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    ) %>%
    filter(.data$bill_type %in% c("HB", "SB")) %>%
    select(-"bill_type") %>%
    filter(!grepl("committee", .data$primary_sponsors, ignore.case = TRUE)) %>%
    select("bill_id", "term")

  genuinely_missing
}

# Stage 5 Hook: Derive unique sponsors
#' Derive unique sponsors for North Carolina
#'
#' From old script lines 527-535.
#'
#' @param bills Bills dataframe with achievement metrics
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregated metrics
derive_unique_sponsors <- function(bills, term) {
  bills %>%
    mutate(
      chamber = ifelse(substring(.data$bill_id, 1, 1) == "H", "H", "S")
    ) %>%
    select("LES_sponsor", "chamber", "passed_chamber", "law") %>%
    mutate(term = term) %>%
    group_by(.data$LES_sponsor, .data$chamber, .data$term) %>%
    summarize(
      num_sponsored_bills = n(),
      sponsor_pass_rate = sum(.data$passed_chamber) / n(),
      sponsor_law_rate = sum(.data$law) / n(),
      .groups = "drop"
    )
}

# Stage 5 Hook: Compute cosponsorship
#' Compute cosponsorship counts for North Carolina
#'
#' NC uses grep-based approach: concatenate LES_sponsor, primary_sponsors,
#' and cosponsors into cospon_match, then count how many bills in the
#' same chamber mention each sponsor name.
#' From old script lines 538-561.
#'
#' @param all_sponsors Sponsors dataframe
#' @param bills Bills dataframe
#' @return Sponsors dataframe with num_cosponsored_bills added
compute_cosponsorship <- function(all_sponsors, bills) {
  all_sponsors$num_cosponsored_bills <- NA

  # Create combined match column (old script line 539)
  bills$cospon_match <- paste(
    bills$LES_sponsor, bills$primary_sponsors, bills$cosponsors,
    sep = "; "
  )
  bills$cospon_match <- gsub("; NA", "", bills$cospon_match)

  cli_log("Computing cosponsorship counts...")

  for (i in seq_len(nrow(all_sponsors))) {
    chamber_prefix <- ifelse(
      all_sponsors[i, ]$chamber == "H", "H", "S"
    )
    c_sub <- filter(
      bills, substring(.data$bill_id, 1, 1) == chamber_prefix
    )
    all_sponsors$num_cosponsored_bills[i] <- sum(
      grepl(all_sponsors[i, ]$LES_sponsor, tolower(c_sub$cospon_match))
    )
    # Subtract primary sponsorship (old script line 545)
    all_sponsors$num_cosponsored_bills[i] <-
      all_sponsors$num_cosponsored_bills[i] -
      all_sponsors$num_sponsored_bills[i]
  }

  # Validate no negative counts (old script lines 551-553)
  if (min(all_sponsors$num_cosponsored_bills) < 0) {
    cli_warn("Negative cosponsor counts found - this indicates a problem")
    print(all_sponsors %>%
      filter(.data$num_cosponsored_bills < 0) %>%
      select(
        "LES_sponsor", "chamber",
        "num_sponsored_bills", "num_cosponsored_bills"
      ))
  } else {
    cli_log("Cosponsorship counts valid (no negatives)")
  }

  # Term-specific NA overrides for manually disambiguated names
  # (grep can't distinguish them correctly) (old script lines 556-561)
  if (all_sponsors$term[1] == "1993_1994") {
    all_sponsors[
      all_sponsors$LES_sponsor %in% c("hunt, judy", "hunt, john"),
    ]$num_cosponsored_bills <- NA
  } else if (all_sponsors$term[1] == "1997_1998") {
    all_sponsors[
      all_sponsors$LES_sponsor %in% c("r. hunter", "h. hunter"),
    ]$num_cosponsored_bills <- NA
    all_sponsors[
      all_sponsors$LES_sponsor %in% c("c. wilson", "g. wilson"),
    ]$num_cosponsored_bills <- NA
  }

  all_sponsors
}

# Stage 5 Hook: Clean sponsor names
#' Clean sponsor names for matching
#'
#' Creates match_name_chamber using full LES_sponsor (with quoted
#' substrings removed).
#' From old script lines 606-612.
#'
#' @param all_sponsors Sponsors dataframe
#' @param term Term string
#' @return Modified all_sponsors with match_name_chamber
clean_sponsor_names <- function(all_sponsors, term) {
  # Term-specific first/last name overrides for matching
  # (old script lines 571-598)
  if (term == "1993_1994") {
    all_sponsors[
      all_sponsors$LES_sponsor == "thompson",
    ]$LES_sponsor <- "r. thompson"
    all_sponsors[
      all_sponsors$LES_sponsor == "wilson" &
        all_sponsors$chamber == "S",
    ]$LES_sponsor <- "p. wilson"
  }
  if (term %in% c("1995_1996", "1999_2000", "2003_2004")) {
    idx <- all_sponsors$LES_sponsor %in% c("g. wilson", "wilson, g")
    if (any(idx)) {
      # William E. 'Gene' Wilson - first name for matching is 'w'
      # Handled via match_name_chamber override below
    }
  }

  # Create match_name_chamber (old script line 611)
  all_sponsors %>%
    arrange(.data$chamber, .data$LES_sponsor) %>%
    distinct() %>%
    mutate(
      match_name_chamber = tolower(paste(
        str_remove_all(.data$LES_sponsor, "\"\\s*.*?\\s*\""),
        substr(.data$chamber, 1, 1),
        sep = "-"
      ))
    )
}

# Stage 5 Hook: Adjust legiscan data
#' Adjust legiscan data for North Carolina
#'
#' Filter committee_id == 0, add disambiguation for duplicate last names.
#' From old script lines 629-635.
#'
#' @param legiscan Legiscan dataframe
#' @param term Term string
#' @return Modified legiscan with match_name_chamber
adjust_legiscan_data <- function(legiscan, term) {
  legiscan_adj <- legiscan %>%
    filter(.data$committee_id == 0) %>%
    group_by(.data$last_name, .data$role) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      match_name = ifelse(
        .data$n >= 2,
        glue("{substr(first_name, 1, 1)}. {last_name}"),
        .data$last_name
      ),
      match_name_chamber = tolower(paste(
        .data$match_name,
        substr(.data$district, 1, 1),
        sep = "-"
      ))
    )

  # Term-specific: Fix Smith collision in 2023_2024
  # Carson Smith (HD-016) and Charles Smith (HD-044) both have
  # first_name starting with "C", so they get same key "c. smith-h".
  # Use full first name to disambiguate.
  if (term == "2023_2024") {
    legiscan_adj <- legiscan_adj %>%
      mutate(
        match_name_chamber = case_when(
          .data$people_id == 20523 ~ "carson smith-h",
          .data$people_id == 24216 ~ "charles smith-h",
          TRUE ~ .data$match_name_chamber
        )
      )
  }

  legiscan_adj
}

# Stage 5 Hook: Reconcile legiscan with sponsors
#' Reconcile legiscan with sponsors using fuzzy matching
#'
#' From old script lines 643-689.
#'
#' @param all_sponsors Sponsors dataframe
#' @param legiscan_adjusted Adjusted legiscan dataframe
#' @param term Term string
#' @return Joined dataframe
reconcile_legiscan_with_sponsors <- function(all_sponsors,
                                              legiscan_adjusted, term) {
  if (term == "2019_2020") {
    joined <- inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "cooper-suggs-h" = NA_character_,
        "smith-ingram-s" = "smith-s",
        "forde-hawkins-h" = "hawkins-h",
        "schollander-h" = NA_character_,
        "michaux-s" = NA_character_,
        "proctor-s" = NA_character_,
        "pate-s" = NA_character_,
        "craven-s" = NA_character_,
        "carter-h" = NA_character_,
        "baker-h" = NA_character_,
        "w. alexander-s" = "t. alexander-s",
        "j. johnson-h" = NA_character_
      )
    )
  } else if (term == "2021_2022") {
    joined <- inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "buansi-h" = NA_character_,
        "pyrtle-h" = NA_character_,
        "carter-h" = NA_character_,
        "loftis-h" = NA_character_
      )
    )
  } else if (term == "2023_2024") {
    joined <- inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # Maze (Neal) Jackson (HD-078) -> sponsor "n. jackson"
        "m. jackson-h" = "n. jackson-h",
        # Jennifer Balkcom (HD-117) -> sponsor "balkcom"
        "capps balkcom-h" = "balkcom-h",
        # Zack Forde-Hawkins (HD-031) -> sponsor "hawkins"
        "forde-hawkins-h" = "hawkins-h",
        # Buck Newton (SD-004) -> sponsor "b. newton"
        "e. newton-s" = "b. newton-s",
        # Block zero-bill legislators from false fuzzy matches
        "blust-h" = NA_character_,
        "willingham-h" = NA_character_,
        "branson-h" = NA_character_,
        "brinson-s" = NA_character_,
        "clark-h" = NA_character_,
        "rhyne-h" = NA_character_,
        "drakeford-h" = NA_character_,
        "eddins-h" = NA_character_,
        "jones-s" = NA_character_
      )
    )
  } else {
    # Default: no custom matches
    joined <- inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full"
    )
  }

  joined
}

# Stage 7 Hook: Prepare bills for LES calculation
#' Prepare bills for LES calculation
#'
#' Derives chamber from bill_id prefix (H = House, S = Senate).
#' From old script line 720.
#'
#' @param bills_prepared Bills dataframe
#' @return Bills with chamber column added
prepare_bills_for_les <- function(bills_prepared) {
  bills_prepared %>%
    mutate(
      chamber = ifelse(
        substring(.data$bill_id, 1, 1) == "H", "H", "S"
      )
    )
}

# Export configuration and hooks
c(
  nc_config,
  list(
    get_bill_file_suffix = get_bill_file_suffix,
    preprocess_raw_data = preprocess_raw_data,
    clean_bill_details = clean_bill_details,
    clean_bill_history = clean_bill_history,
    post_evaluate_bill = post_evaluate_bill,
    transform_ss_bills = transform_ss_bills,
    get_missing_ss_bills = get_missing_ss_bills,
    derive_unique_sponsors = derive_unique_sponsors,
    compute_cosponsorship = compute_cosponsorship,
    clean_sponsor_names = clean_sponsor_names,
    adjust_legiscan_data = adjust_legiscan_data,
    reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
    prepare_bills_for_les = prepare_bills_for_les
  )
)
