# Wyoming (WY) State Configuration
#
# WY is a biennial legislature with annual sessions:
#   - General Session (odd year)
#   - Budget Session (even year)
# No special sessions in 2023_2024.
# Bill types: HB (House Bill) and SF (Senate File) — NOT SB!
# Sponsors include title prefixes ("Representative", "Senator")
# that must be stripped. Committee-sponsored bills are common
# and must be dropped.
# Has cosponsorship data with chamber-specific filtering.
#
# Bill format: "Representative Zwonitzer, Dn" → "zwonitzer, dn"
# Cosponsors: semicolon-separated with title prefixes
# Chamber cosponsor filtering: House bills strip Senators from
# cosponsors, Senate bills strip Representatives.

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
cli_error <- logging$cli_error
standardize_accents <- strings$standardize_accents

# Load required libraries
library(dplyr)
library(stringr)
library(glue)

# Committee/department names that appear as primary_sponsor.
# Bills matching these OR matching committee|commission|council
# regex are dropped. (From old script lines 332-338)
# nolint start: line_length_linter
wy_committees <- c(
  "agriculture", "appropriations", "cap financing",
  "corporations", "education", "judiciary", "labor",
  "mgt audit", "mgt council", "minerals", "revenue",
  "s07", "s08", "s10", "school facilities", "sdc",
  "transportation", "travel", "water", "cap con",
  "local govt", "m hlth & sa", "nat res fund", "tribal",
  "tribal relations", "develop", "util tax relief",
  "cap fin & inv", "dev/intellectual pgms", "scorporations",
  "slabor", "dev/intelec programs", "sch finance",
  "happropriations", "sappropriations", "educ acct",
  "sel sch fac", "fed nat res", "state ed acct", "hac",
  "heducation", "hrevenue", "school recalibration",
  "natural resource funding", "coal/mineral bankruptcies",
  "blockchain/technology", "recalibration"
)
# nolint end: line_length_linter

# nolint start: line_length_linter
wy_config <- list(
  bill_types = c("HB", "SF"),

  # Step terms for evaluating bill history (from old script lines 402-411)
  step_terms = list(
    aic = c(
      "^h[0-9]+ recommend", "^s[0-9]+ recommend",
      "^h[0-9]+ - [a-z]+:recommend",
      "^s[0-9]+ - [a-z]+:recommend",
      "do pass", "do not pass", "(without|no) recommendation"
    ),
    abc = c(
      "^h[0-9]+ recommended.+do pass",
      "^s[0-9]+ recommended.+do pass",
      "placed on general file", "rerefer",
      "^amendment", "^(h|s) amendments",
      "(passed|failed|considered in) cow",
      "cow:(pass|fail|consider)",
      "2nd reading", "3rd reading"
    ),
    pass = c("^(h|s) passed 3rd", "3rd reading:passed"),
    law = c("governor signed", "assigned chapter number")
  )
)
# nolint end: line_length_linter

# Stage 1 Hook: Load bill files
#' Load bill files for Wyoming
#'
#' WY has separate files per year. Loads and combines both years.
#' Pattern: WV.R / KY.R.
#'
#' @param bill_dir Path to bill directory
#' @param state State code ("WY")
#' @param term Term in format "YYYY_YYYY"
#' @param verbose Show detailed logging (default TRUE)
#' @return List with bill_details and bill_history dataframes
load_bill_files <- function(bill_dir, state, term, verbose = TRUE) {
  years <- strsplit(term, "_")[[1]]
  year1 <- years[1]
  year2 <- years[2]

  all_files <- list.files(bill_dir, full.names = TRUE)

  detail_files <- all_files[
    grepl(glue("{state}_Bill_Details"), all_files) &
      (grepl(year1, all_files) | grepl(year2, all_files))
  ]

  if (length(detail_files) == 0) {
    cli_warn(glue("No bill detail files found for term {term}"))
    return(NULL)
  }

  if (verbose) {
    cli_log(glue(
      "Found {length(detail_files)} bill detail files for {term}"
    ))
  }

  bill_details <- bind_rows(lapply(detail_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read.csv(f, stringsAsFactors = FALSE)
  }))

  history_files <- all_files[
    grepl(glue("{state}_Bill_Histories"), all_files) &
      (grepl(year1, all_files) | grepl(year2, all_files))
  ]

  if (verbose) {
    cli_log(glue(
      "Found {length(history_files)} bill history files for {term}"
    ))
  }

  bill_history <- bind_rows(lapply(history_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read.csv(f, stringsAsFactors = FALSE)
  }))

  bill_details <- distinct(bill_details)
  bill_history <- distinct(bill_history)

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 1.5 Hook: Preprocess raw data
#' Preprocess raw data for Wyoming
#'
#' - Rename bill_number -> bill_id
#' - Set term
#' - WY raw data already has session column ("2023-RS", "2024-RS")
#' - Distinct
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  bill_details <- bill_details %>%
    rename(bill_id = "bill_number") %>%
    mutate(term = term) %>%
    distinct()

  bill_history <- bill_history %>%
    rename(bill_id = "bill_number") %>%
    mutate(term = term) %>%
    distinct()

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 2 Hook: Clean bill details
#' Clean bill details for Wyoming
#'
#' 1. Store all_bill_details before filtering
#' 2. Derive bill_type from bill_id prefix, filter to HB/SF
#' 3. Lowercase primary_sponsor and cosponsors
#' 4. standardize_accents on both
#' 5. Strip title prefixes: "senator ", "representative "
#' 6. Chamber-specific cosponsor filtering (old script lines 196-201)
#' 7. Rename primary_sponsor -> LES_sponsor
#' 8. Drop committee-sponsored bills
#' 9. Drop empty/NA sponsors
#' 10. Drop withdrawn bills
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @param verbose Show detailed logging (default TRUE)
#' @return List with all_bill_details and filtered bill_details
# nolint start: line_length_linter
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Store unfiltered version (before any filtering)
  all_bill_details <- bill_details

  # Derive bill_type from bill_id prefix and filter
  bill_details <- bill_details %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    ) %>%
    filter(.data$bill_type %in% wy_config$bill_types) %>%
    select(-"bill_type")

  if (verbose) {
    cli_log(glue("After bill type filter: {nrow(bill_details)} bills"))
  }

  # Term-specific cosponsor normalization (before lowercasing)
  # Bare "Representative Larsen" → "Representative Larsen, L"
  # (Lloyd Larsen is the only Rep Larsen in 2023_2024)
  if (term == "2023_2024") {
    bill_details$cosponsors <- gsub(
      "Representative Larsen(;|$)", "Representative Larsen, L\\1",
      bill_details$cosponsors
    )
  }

  # Lowercase sponsors and cosponsors
  bill_details$primary_sponsor <- tolower(bill_details$primary_sponsor)
  bill_details$cosponsors <- tolower(bill_details$cosponsors)

  # Accent removal
  bill_details$primary_sponsor <- standardize_accents(
    bill_details$primary_sponsor
  )
  bill_details$cosponsors <- standardize_accents(bill_details$cosponsors)

  # Strip title prefixes (old script line 194)
  bill_details$primary_sponsor <- gsub(
    "senator |representative ", "", bill_details$primary_sponsor
  )
  bill_details$primary_sponsor <- str_trim(
    gsub("  +", " ", bill_details$primary_sponsor)
  )

  # Chamber-specific cosponsor filtering (old script lines 196-201)
  # House bills: remove senator cosponsors
  # Senate bills: remove representative cosponsors
  bill_details$chamber_cosponsors <- ifelse(
    substring(bill_details$bill_id, 1, 1) == "H",
    gsub("senator [^;]+(; |$)", "", bill_details$cosponsors),
    gsub("representative [^;]+(; |$)", "", bill_details$cosponsors)
  )
  # Edge case: single remaining cosponsor from wrong chamber
  bill_details$chamber_cosponsors <- ifelse(
    substring(bill_details$bill_id, 1, 1) == "H" &
      !grepl(";", bill_details$chamber_cosponsors),
    gsub("^senator.+", "", bill_details$chamber_cosponsors),
    bill_details$chamber_cosponsors
  )
  bill_details$chamber_cosponsors <- ifelse(
    substring(bill_details$bill_id, 1, 1) == "S" &
      !grepl(";", bill_details$chamber_cosponsors),
    gsub("^representative.+", "", bill_details$chamber_cosponsors),
    bill_details$chamber_cosponsors
  )
  # Clean up: strip trailing semicolons and title prefixes
  bill_details$chamber_cosponsors <- str_trim(
    gsub(";$", "", bill_details$chamber_cosponsors)
  )
  bill_details$chamber_cosponsors <- gsub(
    "senator |representative |;$", "",
    bill_details$chamber_cosponsors
  )
  bill_details$chamber_cosponsors <- str_trim(
    gsub("  +", " ", bill_details$chamber_cosponsors)
  )

  # Rename primary_sponsor -> LES_sponsor
  bill_details <- bill_details %>%
    rename(LES_sponsor = "primary_sponsor")

  # Drop committee-sponsored bills (old script lines 332-343)
  is_committee <- grepl(
    "committee|commission|council", bill_details$LES_sponsor
  ) | bill_details$LES_sponsor %in% wy_committees
  if (sum(is_committee) > 0 && verbose) {
    cli_log(glue(
      "Dropping {sum(is_committee)} committee-sponsored bills"
    ))
  }
  bill_details <- bill_details[!is_committee, ]

  # Drop empty/NA sponsors (old script lines 347-351)
  empty <- bill_details$LES_sponsor == "" | is.na(bill_details$LES_sponsor)
  if (sum(empty) > 0 && verbose) {
    cli_log(glue(
      "Dropping {sum(empty)} bills without sponsor"
    ))
  }
  bill_details <- bill_details[!empty, ]

  # Drop withdrawn bills (old script lines 354-358)
  withdrawn <- bill_details$LES_sponsor == "withdrawn"
  if (sum(withdrawn) > 0 && verbose) {
    cli_log(glue(
      "Dropping {sum(withdrawn)} withdrawn bills"
    ))
  }
  bill_details <- bill_details[!withdrawn, ]

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
#' Clean bill history for Wyoming
#'
#' WY history chambers are already spelled out ("House", "Senate",
#' "Governor", "LSO"). Handle empty chamber strings: set to
#' "Governor" if action contains "Governor", then forward-fill
#' remaining NAs. Arrange by order.
#' Source: old script lines 384-393.
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  bill_history <- bill_history %>%
    group_by(.data$term, .data$session, .data$bill_id) %>%
    mutate(
      chamber = ifelse(
        str_trim(.data$chamber) == "", NA, .data$chamber
      ),
      chamber = ifelse(
        grepl("Governor", .data$action), "Governor", .data$chamber
      )
    ) %>%
    tidyr::fill(.data$chamber) %>%
    ungroup() %>%
    arrange(.data$term, .data$session, .data$bill_id, .data$order)

  bill_history
}

# Stage 4 Hook: Post-evaluate bill
#' Post-evaluate bill for Wyoming
#'
#' Chapter number fallback: if law==0 AND chapter_num is populated
#' and not "CH0000", set abc=1, pass=1, law=1.
#' Source: old script lines 442-444.
#'
#' @param bill_stages Dataframe with one row of bill achievement
#' @param bill_row Dataframe row from bill_details for this bill
#' @param bill_history Dataframe of bill history for this bill
#' @return Modified bill_stages
post_evaluate_bill <- function(bill_stages, bill_row, bill_history) {
  ch <- bill_row$chapter_num
  if (bill_stages$law == 0 && !is.na(ch) && ch != "" && ch != "CH0000") {
    bill_stages$action_beyond_comm <- 1
    bill_stages$passed_chamber <- 1
    bill_stages$law <- 1
  }
  bill_stages
}

# Stage 3 Hook: Enrich SS bills with session
#' Enrich SS bills with session for Wyoming
#'
#' No special sessions in 2023_2024. Simple mapping:
#' session = paste0(year, "-RS").
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return SS bills with session column added
enrich_ss_with_session <- function(ss_bills, term) {
  ss_bills %>%
    mutate(session = paste0(.data$year, "-RS"))
}

# Stage 3 Hook: Get missing SS bills
#' Identify genuinely missing SS bills vs committee-sponsored
#'
#' WY has many committee-sponsored bills that are intentionally
#' excluded. SS bills whose sponsors are committees are not
#' genuinely missing.
#'
#' @param ss_filtered Filtered SS bills for this term
#' @param bill_details Cleaned bill details (after filtering)
#' @param all_bill_details All bill details (before filtering)
#' @return Dataframe of genuinely missing SS bills (bill_id, term)
get_missing_ss_bills <- function(ss_filtered, bill_details,
                                 all_bill_details) {
  missing <- ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term"))

  # Check if they exist in all_bill_details
  missing_with_sponsor <- missing %>%
    left_join(
      all_bill_details %>%
        select("bill_id", "term", "primary_sponsor") %>%
        distinct(),
      by = c("bill_id", "term")
    )

  # Filter out committee-sponsored bills
  is_committee <- grepl(
    "committee|commission|council",
    missing_with_sponsor$primary_sponsor,
    ignore.case = TRUE
  ) | tolower(missing_with_sponsor$primary_sponsor) %in% wy_committees

  genuinely_missing <- missing_with_sponsor[!is_committee, ] %>%
    select("bill_id", "term")

  genuinely_missing
}

# Stage 5 Hook: Derive unique sponsors
#' Derive unique sponsors for Wyoming
#'
#' Standard aggregation PLUS cosponsor-only legislators from
#' chamber_cosponsors (not raw cosponsors). Uses precise regex
#' for cosponsor name search to avoid partial matches.
#' Source: old script lines 505-529.
#'
#' @param bills Dataframe of bills with achievement columns
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregate stats
derive_unique_sponsors <- function(bills, term) {
  # Primary sponsors
  all_sponsors <- bills %>%
    mutate(
      chamber = ifelse(
        substring(.data$bill_id, 1, 1) == "H", "H", "S"
      )
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

  # Add cosponsor-only legislators from chamber_cosponsors
  # (old script lines 518-529)
  unique_cospon <- gsub(
    ";$", "",
    str_trim(unique(unlist(str_split(
      bills$chamber_cosponsors, "; "
    ))))
  )
  for (nonspon in unique_cospon) {
    if (is.na(nonspon) || nonspon == "") next
    if (nonspon %in% all_sponsors$LES_sponsor) next

    # Use precise regex to find chamber (old script line 521)
    ns_search <- paste0(
      "^", nonspon, "$|^", nonspon, ";|; ", nonspon, "$|; ",
      nonspon, ";"
    )
    chamb <- unique(substring(
      bills[grepl(ns_search, bills$chamber_cosponsors), ]$bill_id,
      1, 1
    ))
    if (length(chamb) == 1) {
      all_sponsors <- tibble::add_row(
        all_sponsors,
        LES_sponsor = nonspon,
        chamber = chamb,
        term = term,
        num_sponsored_bills = 0,
        sponsor_pass_rate = 0,
        sponsor_law_rate = 0
      )
    } else if ("H" %in% chamb && "S" %in% chamb) {
      cli_warn(glue(
        "CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS"
      ))
    }
  }

  all_sponsors
}

# Stage 5 Hook: Compute cosponsorship
#' Compute cosponsorship for Wyoming
#'
#' Build cospon_match = paste(LES_sponsor, chamber_cosponsors).
#' Count with precise regex pattern. Subtract num_sponsored_bills.
#' Source: old script lines 531-541.
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param bills Dataframe of bills with chamber_cosponsors column
#' @return Dataframe of sponsors with num_cosponsored_bills added
compute_cosponsorship <- function(all_sponsors, bills) {
  # Build cosponsor match column (old script line 532)
  bills$cospon_match <- paste(
    bills$LES_sponsor, bills$chamber_cosponsors, sep = "; "
  )

  all_sponsors$num_cosponsored_bills <- NA_integer_
  for (i in seq_len(nrow(all_sponsors))) {
    c_sub <- bills[substring(bills$bill_id, 1, 1) %in%
      ifelse(all_sponsors[i, ]$chamber == "H", "H", "S"), ]
    sn <- all_sponsors[i, ]$LES_sponsor
    # Precise regex to avoid partial matches (old script line 537)
    search_term <- paste0(
      "^", sn, ";|^", sn, "$|; ", sn, ";|; ", sn, "$"
    )
    all_sponsors$num_cosponsored_bills[i] <- sum(
      grepl(search_term, tolower(c_sub$cospon_match))
    )
    # Subtract own sponsored bills (old script line 540)
    all_sponsors$num_cosponsored_bills[i] <-
      all_sponsors$num_cosponsored_bills[i] -
        all_sponsors$num_sponsored_bills[i]
  }

  # Validate non-negative
  if (min(all_sponsors$num_cosponsored_bills) < 0) {
    cli_error("Negative cosponsorship counts detected")
    stop("Cosponsorship validation failed")
  } else {
    cli_log("Cosponsorship counts valid (all non-negative)")
  }

  all_sponsors
}

# Stage 5 Hook: Clean sponsor names
#' Clean sponsor names for matching
#'
#' Creates match_name_chamber = tolower(paste(LES_sponsor,
#' chamber_initial, sep="-")). Removes quoted substrings first.
#' Source: old script line 575.
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
        str_remove_all(.data$LES_sponsor, '"\\s*.*?\\s*"'),
        substr(.data$chamber, 1, 1),
        sep = "-"
      ))
    )
}

# Stage 5 Hook: Adjust legiscan data
#' Adjust legiscan data for Wyoming
#'
#' 1. Filter committee_id == 0
#' 2. Fix role/district mismatches (e.g., Laursen listed as
#'    Rep with SD-019 — actually a Senator)
#' 3. Group by last_name + role. If n >= 2, use "F. Last" format.
#' 4. Check for first-initial collisions within same group
#'    (e.g., Dan + David Zwonitzer both produce "D. Zwonitzer");
#'    if collision, use full first_name instead.
#' 5. Build match_name_chamber.
#' Source: old script lines 593-599.
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string
#' @return Adjusted legiscan with match_name_chamber column
adjust_legiscan_data <- function(legiscan, term) {
  legiscan <- legiscan %>%
    filter(.data$committee_id == 0)

  # Fix role/district mismatches: if role says Rep but district
  # is SD (Senate District), correct the role to Sen, and vice versa
  legiscan <- legiscan %>%
    mutate(
      role = case_when(
        .data$role == "Rep" & grepl("^SD", .data$district) ~ "Sen",
        .data$role == "Sen" & grepl("^HD", .data$district) ~ "Rep",
        TRUE ~ .data$role
      )
    )

  # Standard disambiguation: group by last_name + role
  legiscan <- legiscan %>%
    group_by(.data$last_name, .data$role) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      match_name = ifelse(
        .data$n >= 2,
        glue("{substr(first_name, 1, 1)}. {last_name}"),
        .data$last_name
      )
    )

  # Secondary dedup: check for first-initial collisions within
  # same (last_name, role) group. If two people have the same
  # initial (e.g., Dan + David → both "D."), use full first_name
  legiscan <- legiscan %>%
    mutate(initial = substr(.data$first_name, 1, 1))
  legiscan <- legiscan %>%
    group_by(.data$last_name, .data$role, .data$initial) %>%
    mutate(n_same_initial = n()) %>%
    ungroup() %>%
    mutate(
      match_name = ifelse(
        .data$n >= 2 & .data$n_same_initial >= 2,
        glue("{first_name} {last_name}"),
        .data$match_name
      )
    ) %>%
    select(-"initial", -"n_same_initial") %>%
    mutate(
      match_name_chamber = tolower(paste(
        .data$match_name,
        substr(.data$district, 1, 1),
        sep = "-"
      ))
    )

  legiscan
}

# Stage 5 Hook: Reconcile legiscan with sponsors
#' Reconcile legiscan with sponsors using fuzzy matching
#'
#' Uses inexact_join with OSA method, full outer join.
#' Custom match entries for Zwonitzer disambiguation.
#' Source: old script lines 607-639.
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
        "dan zwonitzer-h" = "zwonitzer, dn-h",
        "david zwonitzer-h" = "zwonitzer, dv-h",
        # Larson fuzzy-matches to "byron" — force correct match
        "larson-h" = "larson, jt-h",
        # Laursen fuzzy-matches to "landen" — force correct match
        "laursen-s" = "laursen, d-s"
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
#' Prepare bills for LES calculation
#'
#' Derives chamber from bill_id prefix (H/S).
#' Works for both HB and SF since SF starts with "S".
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
  wy_config,
  list(
    load_bill_files = load_bill_files,
    preprocess_raw_data = preprocess_raw_data,
    clean_bill_details = clean_bill_details,
    clean_bill_history = clean_bill_history,
    post_evaluate_bill = post_evaluate_bill,
    enrich_ss_with_session = enrich_ss_with_session,
    get_missing_ss_bills = get_missing_ss_bills,
    derive_unique_sponsors = derive_unique_sponsors,
    compute_cosponsorship = compute_cosponsorship,
    clean_sponsor_names = clean_sponsor_names,
    adjust_legiscan_data = adjust_legiscan_data,
    reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
    prepare_bills_for_les = prepare_bills_for_les
  )
)
