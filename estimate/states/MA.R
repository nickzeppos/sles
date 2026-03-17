# Massachusetts (MA) State Configuration
#
# MA is a biennial legislature with a single session per term
# (e.g., 193rd General Court = 2023-2024).
# Session ordinal: session_num = (start_year - 2019)/2 + 191
# Bill IDs: "H.1" format -> standardized to "H0001"
# Bill types: keep bill_type == "Bill" only (H/S prefix, NOT HB/SB)
# Sponsor: use "sponsor" if non-empty, else convert "filed_by"
# Joint committees: ignore_chamber_switch=TRUE, add_chamb="Joint"
# Full-name matching (not last-name-only)
#
# Source: .dropbox/old_estimate_scripts/MA - Estimate LES AV.R

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}

logging <- source(file.path(repo_root, "utils/logging.R"),
  local = TRUE
)$value
strings <- source(file.path(repo_root, "utils/strings.R"),
  local = TRUE
)$value

# Extract functions
cli_log <- logging$cli_log
cli_warn <- logging$cli_warn
standardize_accents <- strings$standardize_accents

# Load required libraries
library(dplyr)
library(stringr)
library(glue)

# nolint start: line_length_linter
ma_config <- list(
  bill_types = c("H", "S"),
  ignore_chamber_switch = TRUE,
  add_chamb = "Joint",

  # Step terms from old script lines 398-411
  step_terms = list(
    aic = c(
      "reported favorab", "^recommend", "committee recomm",
      "ought not to pass", "ought to pass",
      "^committee reported that",
      "public hearing", "hearing date",
      "hearing [a-z]+heduled",
      "discharged to.+comm"
    ),
    abc = c(
      "reported [a-z]+", "orders of the day",
      "^amendment", "motion to suspend",
      "read second", "read third", "third read"
    ),
    pass = c("passed.+engrossed", "enacted"),
    # Note: "pursant" is a typo in old script — kept faithfully
    law = c(
      "signed by the gov",
      "chapter [0-9]+ +of",
      "became law pursant to"
    )
  )
)
# nolint end: line_length_linter

#' Compute MA session ordinal from term
#'
#' Formula: session_num = (start_year - 2019)/2 + 191
#' e.g., 2023 -> 193
#'
#' @param term Term string in "YYYY_YYYY" format
#' @return Integer session number
get_session_num <- function(term) {
  start_year <- as.integer(strsplit(term, "_")[[1]][1])
  as.integer((start_year - 2019) / 2 + 191)
}

#' Format ordinal string (1st, 2nd, 3rd, 193rd, etc.)
#'
#' @param n Integer
#' @return Ordinal string
ordinal <- function(n) {
  suffix <- if (n %% 100 %in% c(11, 12, 13)) {
    "th"
  } else if (n %% 10 == 1) {
    "st"
  } else if (n %% 10 == 2) {
    "nd"
  } else if (n %% 10 == 3) {
    "rd"
  } else {
    "th"
  }
  paste0(n, suffix)
}

# Stage 1 Hook: Get bill file suffix
#' Get bill file suffix for Massachusetts
#'
#' MA files use ordinal session number (e.g., "193rd").
#'
#' @param term Term string
#' @return File suffix string (e.g., "193rd")
get_bill_file_suffix <- function(term) {
  ordinal(get_session_num(term))
}

# Stage 1.5 Hook: Preprocess raw data
#' Preprocess raw data for Massachusetts
#'
#' Renames bill_number -> bill_id. Standardizes bill IDs:
#' split on ".", strip newlines from number, zero-pad to 4 digits.
#' Adds term and session columns.
#'
#' Faithful port of old script lines 143-148, 385-391.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  # Standardize bill_id from "H.1" -> "H0001"
  standardize_bill_id <- function(bid) {
    id_parts <- str_split_fixed(bid, "\\.", 2)
    # Strip newlines/whitespace from number portion
    id_parts[, 2] <- gsub("\\n.+|\\r.+", "", id_parts[, 2])
    id_parts[, 2] <- str_trim(id_parts[, 2])
    paste0(id_parts[, 1], str_pad(id_parts[, 2], 4, pad = "0"))
  }

  # --- Bill details ---
  bill_details <- bill_details %>%
    rename(bill_id = "bill_number") %>%
    mutate(
      bill_id = standardize_bill_id(.data$bill_id),
      term = .env$term
    )

  # Check for duplicates
  if (nrow(bill_details) != nrow(distinct(bill_details))) {
    cli_warn("Duplicate bill details detected")
  }

  # --- Bill history ---
  bill_history <- bill_history %>%
    rename(bill_id = "bill_number") %>%
    mutate(
      bill_id = standardize_bill_id(.data$bill_id),
      term = .env$term
    )

  # Ensure session column exists on history
  if (!"session" %in% names(bill_history)) {
    bill_history$session <- bill_details$session[1]
  }

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 1 Hook: Transform commemorative bills
#' Transform commemorative bills for Massachusetts
#'
#' MA commem file has session but no term column.
#' Add term column.
#'
#' @param commem_bills Commem bills dataframe
#' @param term Term string
#' @return Transformed commem bills
transform_commem_bills <- function(commem_bills, term) {
  commem_bills %>%
    mutate(term = .env$term)
}

# Stage 2 Hook: Clean bill details
#' Clean bill details for Massachusetts
#'
#' 1. Filter to bill_type == "Bill"
#' 2. Sponsor logic: use sponsor if non-empty, else filed_by_adj
#' 3. Handle multi-sponsor " , " pattern
#' 4. Lowercase, standardize accents
#' 5. Name fixes (jr., creem, etc.)
#' 6. Drop committee-sponsored (complex regex)
#' 7. Drop agency/entity sponsors
#' 8. Drop by-request bills, governor, Secretary of Commonwealth
#' 9. Drop empty sponsors
#'
#' Faithful port of old script lines 151-379.
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string
#' @param verbose Show detailed logging
#' @return List with all_bill_details and filtered bill_details
# nolint start: line_length_linter
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Store unfiltered
  all_bill_details <- bill_details

  # Filter to Bills only (old script line 175)
  bill_details <- bill_details %>%
    filter(.data$bill_type %in% c("Bill") |
      grepl("^Bill", .data$bill_type))

  if (verbose) {
    cli_log(glue("Bills after type filter: {nrow(bill_details)}"))
  }

  # Sponsor extraction (old script lines 152-165)
  # filed_by_adj: convert "Last, First" to "First Last"
  bill_details$filed_by_adj <- ifelse(
    grepl(",", bill_details$filed_by),
    str_trim(paste(
      gsub(".+, ", "", bill_details$filed_by),
      gsub(",.+", "", bill_details$filed_by)
    )),
    bill_details$filed_by
  )

  # Use sponsor if non-empty, else filed_by_adj (old script line 153)
  # readr::read_csv converts empty strings to NA, so check both
  bill_details$LES_sponsor <- ifelse(
    is.na(bill_details$sponsor) | bill_details$sponsor == "",
    bill_details$filed_by_adj,
    bill_details$sponsor
  )

  # Handle multi-sponsor " , " pattern (old script lines 159-165)
  if (any(grepl(" , ", bill_details$LES_sponsor))) {
    bill_details$LES_sponsor <- ifelse(
      grepl(" , ", bill_details$LES_sponsor),
      bill_details$filed_by_adj,
      bill_details$LES_sponsor
    )
  }

  # Standardize (old script line 168)
  bill_details$LES_sponsor <- str_trim(
    gsub(" +", " ", tolower(bill_details$LES_sponsor))
  )

  # Name fixes (old script lines 179-211)
  fix_name <- function(df, old_name, new_name) {
    if (any(df$LES_sponsor == old_name, na.rm = TRUE)) {
      df$LES_sponsor[!is.na(df$LES_sponsor) &
        df$LES_sponsor == old_name] <- new_name
    }
    df
  }

  # Jr. pattern: "jr. lastname" -> proper format
  if (any(grepl("^jr\\.", bill_details$LES_sponsor))) {
    jr_mask <- grepl("^jr\\.", bill_details$LES_sponsor)
    for (jr_name in unique(bill_details$LES_sponsor[jr_mask])) {
      cli_warn(glue("FIX JR NAME: {jr_name}"))
    }
  }

  bill_details <- fix_name(
    bill_details, "bradley h. jones", "bradley h. jones, jr."
  )
  bill_details <- fix_name(
    bill_details, "harold p. naughton", "harold p. naughton, jr."
  )
  bill_details <- fix_name(
    bill_details, "cynthia s. creem", "cynthia stone creem"
  )

  # Standardize accents (old script lines 213-216)
  bill_details$LES_sponsor <- standardize_accents(
    bill_details$LES_sponsor
  )

  # Drop committee-sponsored bills (old script lines 331-334)
  comm_pattern <- paste0(
    "joint comm|house comm|senate comm|",
    "\\(h\\)|\\(s\\)|\\(j\\)|conference|committee"
  )
  is_committee <- grepl(comm_pattern, bill_details$LES_sponsor)
  if (sum(is_committee) > 0 && verbose) {
    cli_log(glue(
      "Dropping {sum(is_committee)} committee-sponsored bills"
    ))
  }
  bill_details <- bill_details[!is_committee, ]

  # Drop assorted agencies/entities/bill titles
  # (old script lines 338-349 + additional terms for bill titles)
  drop_assorted <- c(
    "exhibition center", "campaign finance", "budget",
    "bond bill", "life sentences", "transportation",
    "authorities", "insurance", "department", "safety",
    "improvements", "consumer", "house of rep", "senate",
    "financing", "firearms", "appropriations", "massachusetts",
    "perac", "civic engagement", "regulating",
    "renewable energy", "commission", "registration",
    "criminal justice", "auditor of the commonwealth",
    "retirement", "inspector", "office of", "opportunity",
    "education", "justice", "patients first", "climate policy",
    "general appropriation", "division of banks",
    "mobile telephones", "health and wellness",
    "covid-19", "it bond", "fy21", "enabling partnerships",
    "establishing uniform", "voter opportunities",
    "military spouse", "cannabis", "mental health",
    "holyoke soldiers", "reprecincting", "arpa bill",
    "work and family", "offshore wind", "infrastructure",
    "reproductive", "homes governance", "judicial",
    "sports wagering", "registry of motor",
    "economic growth", "preserving open",
    # Additional bill title/petition patterns
    "exploitation prevention", "affordable homes",
    "liquor licenses", "competitiveness",
    "information technology", "parentage equality",
    "long term care", "midwifery", "salary range",
    "servicemen and veterans"
  )
  bill_details <- bill_details %>%
    filter(!grepl(
      paste(drop_assorted, collapse = "|"),
      .data$LES_sponsor
    ))

  # Drop citizen petitioners for 2023_2024
  # (old script hard-codes these per term)
  if (term == "2023_2024") {
    citizen_petitioners <- c(
      "jacquelyn wehtje", "normand r. champigny"
    )
    bill_details <- bill_details %>%
      filter(!.data$LES_sponsor %in% citizen_petitioners)
  }

  # Drop by-request bills (old script lines 352-353)
  bill_details <- bill_details %>%
    filter(!(
      grepl("by request", tolower(.data$LES_sponsor)) |
        grepl("by request", tolower(.data$sponsor)) |
        grepl("by request", tolower(.data$presenter))
    ))

  # Drop governors (old script lines 362-367)
  # 2023_2024 term: Maura T. Healey
  if (term == "2023_2024") {
    bill_details <- bill_details %>%
      filter(.data$LES_sponsor != "maura t. healey")
  } else if (term %in% c(
    "2015_2016", "2017_2018", "2019_2020", "2021_2022"
  )) {
    bill_details <- bill_details %>%
      filter(.data$LES_sponsor != "charles d. baker")
  } else if (term %in% c(
    "2009_2010", "2011_2012", "2013_2014"
  )) {
    bill_details <- bill_details %>%
      filter(.data$LES_sponsor != "deval l. patrick")
  }

  # Drop Secretary of the Commonwealth + Lieutenant Gov
  # (old script lines 370-371)
  bill_details <- bill_details %>%
    filter(.data$LES_sponsor != "william francis galvin")
  bill_details <- bill_details %>%
    filter(.data$LES_sponsor != "timothy p. murray")

  # Drop empty/none sponsors (old script lines 376-379)
  empty <- bill_details$LES_sponsor %in% c("", "none") |
    is.na(bill_details$LES_sponsor)
  if (sum(empty) > 0 && verbose) {
    cli_log(glue(
      "Dropping {sum(empty)} bills without sponsor"
    ))
  }
  bill_details <- bill_details[!empty, ]

  # Drop original 'sponsor' column to avoid conflict with
  # calculate_scores rename(sponsor = "LES_sponsor")
  bill_details <- bill_details %>%
    select(-"sponsor")

  if (verbose) {
    cli_log(glue("After cleaning: {nrow(bill_details)} bills"))
  }

  list(
    all_bill_details = all_bill_details,
    bill_details = bill_details
  )
}
# nolint end: line_length_linter

# Stage 2 Hook: Clean bill history
#' Clean bill history for Massachusetts
#'
#' Minimal: history already has chamber as "House"/"Senate"/"Joint"/
#' "Executive". Trim actions, ensure ordering.
#'
#' Faithful port of old script lines 382-386.
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  bill_history$action <- str_trim(bill_history$action)

  bill_history <- bill_history %>%
    arrange(.data$bill_id, .data$order)

  bill_history
}

# Stage 3 Hook: Get missing SS bills
#' Identify genuinely missing SS bills for Massachusetts
#'
#' MA has committee-sponsored bills that are intentionally excluded.
#' SS bills whose sponsor mentions "committee" are not genuinely
#' missing. Also filter out non-Bill types.
#'
#' Faithful port of old script lines 230-238.
#'
#' @param ss_filtered Filtered SS bills for this term
#' @param bill_details Cleaned bill details (after filtering)
#' @param all_bill_details All bill details (before filtering)
#' @return Dataframe of genuinely missing SS bills
get_missing_ss_bills <- function(ss_filtered, bill_details,
                                 all_bill_details) {
  missing <- ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term"))

  # Look up info from all_bill_details
  missing_with_info <- missing %>%
    left_join(
      all_bill_details %>%
        select("bill_id", "term", "sponsor", "filed_by",
               "bill_type") %>%
        distinct(),
      by = c("bill_id", "term")
    )

  # Filter out expected drops:
  # 1. Bills not found in data at all (bill_type is NA)
  # 2. Non-Bill types (e.g., ReOrg Plan)
  # 3. Committee-sponsored
  # 4. No sponsor (both sponsor and filed_by are NA)
  # 5. Entity/agency sponsors (matching drop_assorted patterns)
  drop_pat <- paste(c(
    "firearms", "budget", "appropriation", "commission",
    "retirement", "department", "inspector", "office of",
    "massachusetts", "insurance",
    "exploitation prevention", "affordable homes",
    "liquor licenses", "competitiveness",
    "information technology", "parentage equality",
    "long term care", "midwifery", "salary range",
    "servicemen and veterans"
  ), collapse = "|")
  genuinely_missing <- missing_with_info %>%
    filter(
      !is.na(.data$bill_type),
      grepl("^Bill", .data$bill_type),
      !grepl("committee", .data$sponsor, ignore.case = TRUE),
      !(is.na(.data$sponsor) & is.na(.data$filed_by)),
      !grepl(drop_pat, .data$sponsor, ignore.case = TRUE)
    ) %>%
    select("bill_id", "term")

  genuinely_missing
}

# Stage 5 Hook: Derive unique sponsors
#' Derive unique sponsors for Massachusetts
#'
#' Standard aggregation. Chamber from bill_id prefix (H/S).
#'
#' Faithful port of old script lines 489-497.
#'
#' @param bills Dataframe of bills with achievement columns
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregate stats
derive_unique_sponsors <- function(bills, term) {
  bills %>%
    mutate(
      chamber = ifelse(
        substring(.data$bill_id, 1, 1) == "H", "H", "S"
      )
    ) %>%
    select("LES_sponsor", "chamber", "passed_chamber", "law") %>%
    mutate(term = term) %>%
    group_by(.data$LES_sponsor, .data$term, .data$chamber) %>%
    summarize(
      num_sponsored_bills = n(),
      sponsor_pass_rate = sum(.data$passed_chamber) / n(),
      sponsor_law_rate = sum(.data$law) / n(),
      .groups = "drop"
    )
}

# Stage 5 Hook: Compute cosponsorship
#' Compute cosponsorship counts for Massachusetts
#'
#' Complex name matching: accounts for middle name variants
#' ("First Middle Last" vs "First M. Last" vs "First Last"),
#' Jr/Sr suffixes.
#'
#' Faithful port of old script lines 500-523.
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param bills Dataframe of bills with cosponsors column
#' @return Dataframe with num_cosponsored_bills added
compute_cosponsorship <- function(all_sponsors, bills) {
  all_sponsors$num_cosponsored_bills <- NA

  bills$cospon_match <- gsub(
    " +", " ",
    paste(bills$LES_sponsor, tolower(bills$cosponsors), sep = "; ")
  )

  for (i in seq_len(nrow(all_sponsors))) {
    c_sub <- bills %>%
      filter(
        substring(.data$bill_id, 1, 1) ==
          ifelse(all_sponsors[i, ]$chamber == "H", "H", "S")
      )

    # Build search pattern accounting for name variants
    # (old script lines 506-516)
    search_name <- str_replace_all(
      all_sponsors[i, ]$LES_sponsor, "(\\W)", "\\\\\\1"
    )

    les_spon <- all_sponsors[i, ]$LES_sponsor
    if (grepl("^[a-z]+ [a-z]+ ", les_spon)) {
      # "First Middle Last" -> also match "First M. Last"
      search_name <- paste0(
        search_name, "|",
        gsub("\\\\ .+\\\\ ", " [a-z]\\\\. ", search_name)
      )
    } else if (!grepl(" [a-z]\\. ", les_spon)) {
      # "First Last" -> also match "First M. Last"
      search_name <- paste0(
        search_name, "|",
        gsub(" ", " [a-z]\\\\. ", search_name)
      )
    } else {
      # "First M. Last" -> also match "First Last"
      search_name <- paste0(
        search_name, "|",
        gsub("\\\\ [a-z]\\\\.\\\\ ", " ", search_name)
      )
    }

    # Handle Jr/Sr suffixes (old script lines 514-516)
    if (grepl(" jr\\.| sr\\.| ii+", les_spon)) {
      search_name <- paste0(
        search_name, "|",
        gsub(
          "\\\\ jr\\\\.|\\\\ sr\\\\.|\\\\ ii+", "",
          search_name
        )
      )
    }

    all_sponsors$num_cosponsored_bills[i] <- sum(
      grepl(search_name, tolower(c_sub$cospon_match))
    )
    # Subtract own sponsored bills
    all_sponsors$num_cosponsored_bills[i] <-
      all_sponsors$num_cosponsored_bills[i] -
        all_sponsors$num_sponsored_bills[i]
  }

  if (min(all_sponsors$num_cosponsored_bills) < 0) {
    cli_warn("Negative cosponsor counts found")
    stop("Cosponsorship validation failed")
  }

  all_sponsors
}

# Stage 5 Hook: Clean sponsor names
#' Clean sponsor names for matching
#'
#' Full-name matching: match_name_chamber = tolower(paste(
#' LES_sponsor, chamber_initial, sep="-")).
#'
#' Faithful port of old script line 559.
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param term Term string
#' @return Dataframe with match_name_chamber column added
clean_sponsor_names <- function(all_sponsors, term) {
  all_sponsors %>%
    arrange(.data$chamber, .data$LES_sponsor) %>%
    distinct() %>%
    mutate(
      match_name_chamber = tolower(paste(
        .data$LES_sponsor,
        substr(.data$chamber, 1, 1),
        sep = "-"
      ))
    )
}

# Stage 5 Hook: Adjust legiscan data
#' Adjust legiscan data for Massachusetts
#'
#' Full-name matching using first_name + middle_name + last_name.
#' match_name_chamber = "first middle last-H/S" with double spaces
#' collapsed.
#'
#' Faithful port of old script lines 574-577.
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string
#' @return Adjusted legiscan with match_name_chamber column
adjust_legiscan_data <- function(legiscan, term) {
  legiscan %>%
    filter(.data$committee_id == 0) %>%
    mutate(
      middle_name_clean = ifelse(
        is.na(.data$middle_name), "", .data$middle_name
      ),
      match_name_chamber = tolower(paste0(
        .data$first_name, " ",
        .data$middle_name_clean, " ",
        .data$last_name, "-",
        substr(.data$district, 1, 1)
      )),
      match_name_chamber = gsub("  ", " ", .data$match_name_chamber)
    ) %>%
    select(-"middle_name_clean")
}

# Stage 5 Hook: Reconcile legiscan with sponsors
#' Reconcile legiscan with sponsors
#'
#' Fuzzy join via inexact OSA. Custom matches TBD from first run.
#' Based on prior terms: likely block Senate President (Spilka)
#' and House Speaker (Mariano) if zero-bill.
#'
#' Faithful port of old script lines 586-619.
#'
#' @param sponsors Dataframe of unique sponsors
#' @param legiscan Adjusted legiscan dataframe
#' @param term Term string
#' @return Joined dataframe
# nolint start: line_length_linter
reconcile_legiscan_with_sponsors <- function(sponsors, legiscan,
                                             term) {
  if (term == "2023_2024") {
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # Speaker Mariano: 0 bills, false-matches to donald wong
        "ronald mariano-h" = NA_character_,
        # Senate President Spilka: 0 bills, false-matches to peake
        "karen e. spilka-s" = NA_character_
      )
    )
  } else {
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full"
    )
  }
}
# nolint end: line_length_linter

# Stage 7 Hook: Prepare bills for LES calculation
#' Derive chamber from bill_id prefix for LES scoring
#'
#' MA bills use "H" prefix for House and "S" for Senate.
#' CRITICAL: Generic fallback uses Wisconsin's "A" prefix convention,
#' which would misclassify all MA bills.
#'
#' Faithful port of old script line 652.
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

# Export config and functions
c(
  ma_config,
  list(
    get_bill_file_suffix = get_bill_file_suffix,
    preprocess_raw_data = preprocess_raw_data,
    transform_commem_bills = transform_commem_bills,
    clean_bill_details = clean_bill_details,
    clean_bill_history = clean_bill_history,
    get_missing_ss_bills = get_missing_ss_bills,
    derive_unique_sponsors = derive_unique_sponsors,
    compute_cosponsorship = compute_cosponsorship,
    clean_sponsor_names = clean_sponsor_names,
    adjust_legiscan_data = adjust_legiscan_data,
    reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
    prepare_bills_for_les = prepare_bills_for_les
  )
)
