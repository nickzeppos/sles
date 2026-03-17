# South Dakota (SD) State Configuration
#
# SD notes from old script:
# - Special sessions recorded in separate files; bill numbers restart
# - Bills do NOT carry over from regular session to regular session
# - HB numbers start from HB1001, SB from SB1
# - One primary sponsor; multiple cosponsors permitted
# - Committee-sponsored bills permitted
# - Sponsor format: "Representative <name>" or "Senator <name>"
# - "(prime)" suffix on some sponsor names
# - "at the request of ..." suffix on some sponsor fields
# - Name format for disambiguation: "Last (First)" in sponsor data
# - Legiscan uses "Last (First)" for duplicate last names

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}

logging <- source(file.path(repo_root, "utils/logging.R"), local = TRUE)$value
strings <- source(file.path(repo_root, "utils/strings.R"), local = TRUE)$value

# Extract functions
cli_log <- logging$cli_log
cli_warn <- logging$cli_warn
standardize_accents <- strings$standardize_accents

# Load required libraries
library(dplyr)
library(stringr)
library(glue)
library(readr)

# nolint start: line_length_linter
sd_config <- list(
  bill_types = c("HB", "SB"),

  # Step terms for evaluating bill history (from old script lines 412-424)
  step_terms = list(
    aic = c(
      "committee hearing", "scheduled for hearing",
      "^(house|senate).+ amendment \\([a-z]-[0-9]+\\)",
      "committee.+(do pass|do not pass|without recommend)",
      "^committee:.+calendar"
    ),
    abc = c(
      "committee.+do pass.+passed",
      "motion to amend.+(h\\.j\\.|s\\.j\\.)",
      "^(house|senate).+on calendar", "second reading",
      "^house of representatives do pass", "^senate do pass",
      "^placed on consent",
      "motion to strike the \"not\""
    ),
    pass = c(
      "^house of representatives do pass.+passed",
      "^senate do pass.+passed"
    ),
    law = c(
      "signed by governor", "signed by the governor",
      "delivered to sec. of state",
      "delivered veto override to the secretary of state"
    )
  )
)
# nolint end: line_length_linter

# ============================================================
# Stage 1: Load bill files
# ============================================================
#' Load bill files for South Dakota
#'
#' SD has separate files per session (e.g., 2023-RS, 2024-RS).
#' Bills do NOT carry over between sessions.
#'
#' @param bill_dir Path to bill directory
#' @param state State code ("SD")
#' @param term Term in format "YYYY_YYYY"
#' @param verbose Show detailed logging (default TRUE)
#' @return List with bill_details and bill_history dataframes
load_bill_files <- function(bill_dir, state, term, verbose = TRUE) {
  years <- strsplit(term, "_")[[1]]
  term_start_year <- years[1]
  term_end_year <- years[2]

  # Find all bill detail and history files for this term
  all_files <- list.files(bill_dir, full.names = TRUE)
  detail_files <- all_files[
    grepl(glue("{state}_Bill_Details"), all_files) &
      (grepl(term_start_year, all_files) |
         grepl(term_end_year, all_files))
  ]
  history_files <- all_files[
    grepl(glue("{state}_Bill_Histories"), all_files) &
      (grepl(term_start_year, all_files) |
         grepl(term_end_year, all_files))
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

  # Load and combine bill details
  bill_details <- bind_rows(lapply(detail_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read_csv(f, show_col_types = FALSE)
  }))

  if (verbose) {
    cli_log(glue(
      "Found {length(history_files)} bill history files for {term}"
    ))
  }

  # Load and combine bill histories
  bill_history <- bind_rows(lapply(history_files, function(f) {
    if (verbose) cli_log(glue("  Loading {basename(f)}"))
    read_csv(f, show_col_types = FALSE)
  }))

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# ============================================================
# Stage 1.5: Preprocess raw data
# ============================================================
#' Preprocess raw data for South Dakota
#'
#' Normalizes column names: drops session_year, renames
#' all_sponsors -> sponsors.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  # Drop session_year, rename all_sponsors -> sponsors
  if ("session_year" %in% names(bill_details)) {
    bill_details <- bill_details %>% select(-"session_year")
  }
  if ("all_sponsors" %in% names(bill_details)) {
    bill_details <- bill_details %>% rename(sponsors = "all_sponsors")
  }

  if ("session_year" %in% names(bill_history)) {
    bill_history <- bill_history %>% select(-"session_year")
  }

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# ============================================================
# Stage 2: Clean bill details
# ============================================================
#' Clean bill details for South Dakota
#'
#' From old script lines 122-218:
#' - Zero-pad bill_id to 4 digits
#' - Handle "by request" and "(prime)" in sponsor fields
#' - Derive chamber_sponsors from sponsors
#' - Derive LES_sponsor from primary_sponsor
#' - Filter to valid bill types, drop committee/empty sponsors
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @param verbose Show detailed logging (default TRUE)
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Store unfiltered version before any modifications
  all_bill_details <- bill_details

  # Add term column + standardize bill_id (zero-pad to 4 digits)
  # From old script line 122-124
  bill_details <- bill_details %>%
    mutate(
      term = term,
      bill_id = paste0(
        gsub("[0-9]+", "", .data$bill_id),
        str_pad(gsub("^[A-Z]+", "", .data$bill_id), 4, pad = "0")
      )
    )

  # Handle "by request" and "at the request of" in sponsor fields
  # From old script lines 127-129
  bill_details <- bill_details %>%
    mutate(
      sponsors = str_trim(gsub(
        "at the request.+| by request", "", .data$sponsors
      )),
      primary_sponsor = gsub("  +", " ", str_trim(gsub(
        "by request", "", .data$primary_sponsor
      )))
    )

  # Derive bill_type and filter
  # From old script lines 132-135
  bill_details <- bill_details %>%
    mutate(bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id)))
  all_bill_details <- all_bill_details %>%
    mutate(bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id)))

  if (verbose) {
    cli_log("Bill type distribution:")
    print(table(bill_details$bill_type))
  }

  bill_details <- bill_details %>%
    filter(.data$bill_type %in% sd_config$bill_types)

  # Lowercase sponsors and standardize accents
  # From old script lines 140-145
  bill_details$sponsors <- tolower(bill_details$sponsors)
  bill_details$sponsors <- standardize_accents(bill_details$sponsors)

  # Derive chamber_sponsors (initiating chamber sponsors only)
  # From old script lines 198-206
  # For House bills: strip everything from "and senator" onwards
  # For Senate bills: strip everything from "and represent" onwards
  bill_details <- bill_details %>%
    mutate(
      chamber_sponsors = ifelse(
        substring(.data$bill_id, 1, 1) == "H",
        gsub("and senator.+", "", .data$sponsors),
        gsub("and represent.+", "", .data$sponsors)
      ),
      # Committee-only sponsors (no individual names) get empty string
      chamber_sponsors = ifelse(
        grepl("^the committee", .data$sponsors) &
          !grepl("representative|senator", .data$sponsors),
        "",
        .data$chamber_sponsors
      ),
      # Strip chamber prefix, convert separators to semicolons
      chamber_sponsors = gsub(
        ", and | and |, ", "; ",
        gsub("^represe[a-z]+ |^senat[a-z]+ ", "", .data$chamber_sponsors)
      )
    )
  bill_details$chamber_sponsors <- standardize_accents(
    bill_details$chamber_sponsors
  )

  # Derive LES_sponsor from primary_sponsor
  # From old script lines 208-217
  bill_details$primary_sponsor <- tolower(bill_details$primary_sponsor)
  bill_details$primary_sponsor <- standardize_accents(
    bill_details$primary_sponsor
  )
  bill_details$primary_sponsor <- gsub(
    "^senator |^representative |^introduced by | \\(prime\\)",
    "",
    bill_details$primary_sponsor
  )
  bill_details <- bill_details %>%
    rename(LES_sponsor = "primary_sponsor")

  # Drop committee-sponsored bills
  # From old script lines 348-353
  comm_bills <- bill_details %>%
    filter(grepl("committee", .data$LES_sponsor))
  if (nrow(comm_bills) > 0) {
    if (verbose) {
      cli_log(glue(
        "Dropping {nrow(comm_bills)} committee-sponsored bills"
      ))
    }
    bill_details <- bill_details %>%
      filter(!grepl("committee", .data$LES_sponsor))
  }

  # Drop bills with missing/empty sponsor
  # From old script lines 341-345
  empty_bills <- bill_details %>%
    filter(.data$LES_sponsor == "" | is.na(.data$LES_sponsor))
  if (nrow(empty_bills) > 0) {
    if (verbose) {
      cli_log(glue(
        "Dropping {nrow(empty_bills)} bills without sponsor"
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

# ============================================================
# Stage 2: Clean bill history
# ============================================================
#' Clean bill history for South Dakota
#'
#' From old script lines 373-403:
#' - Zero-pad bill_id
#' - Code chamber from action text (SD-specific patterns)
#' - Recode chamber abbreviations
#' - Adjust committee report actions
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  bill_history <- bill_history %>%
    mutate(
      term = term,
      # Zero-pad bill_id (from old script line 376)
      bill_id = paste0(
        gsub("[0-9]+", "", .data$bill_id),
        str_pad(gsub("^[A-Z]+", "", .data$bill_id), 4, pad = "0")
      )
    )

  # Order by term, session, bill_id, action_date, order
  # From old script lines 380-382
  bill_history <- bill_history %>%
    arrange(
      .data$term, .data$session, .data$bill_id,
      .data$action_date, .data$order
    )

  # Code chamber from action text
  # From old script lines 385-394
  bill_history <- bill_history %>%
    group_by(.data$term, .data$session, .data$bill_id) %>%
    mutate(
      chamber = ifelse(
        .data$order == 1,
        substring(.data$bill_id, 1, 1),
        NA_character_
      ),
      # Governor actions (check before chamber actions since these
      # often have HJ/SJ at end of line)
      chamber = ifelse(
        is.na(.data$chamber) &
          grepl("to the governor|by governor", tolower(.data$action)),
        "G",
        .data$chamber
      ),
      chamber = ifelse(
        is.na(.data$chamber) & grepl(
          "^house|read in house|referred to house| h\\.j\\. [0-9]+",
          tolower(.data$action)
        ),
        "H",
        .data$chamber
      ),
      chamber = ifelse(
        is.na(.data$chamber) & grepl(
          "^senate|read in senate|referred to senate| s\\.j\\. [0-9]+",
          tolower(.data$action)
        ),
        "S",
        .data$chamber
      )
    ) %>%
    tidyr::fill("chamber") %>%
    ungroup()

  # Recode chamber abbreviations (from old script line 397)
  bill_history$chamber <- dplyr::recode(
    bill_history$chamber,
    "H" = "House", "S" = "Senate", "G" = "Governor", "CC" = "Conference"
  )

  # Adjust committee report actions and clean action text
  # From old script lines 400-403
  bill_history <- bill_history %>%
    mutate(
      action = tolower(.data$action),
      action = gsub("\\s+", " ", .data$action),
      # Prefix committee reports that don't specify house/senate
      action = ifelse(
        grepl(
          "do pass|do not pass|report without recommend|place on.+calendar",
          .data$action
        ) & !grepl("house|senate", .data$action),
        paste0("committee: ", .data$action),
        .data$action
      )
    )

  # Trim whitespace (from old script line 444)
  bill_history$action <- str_trim(bill_history$action)

  bill_history
}

# ============================================================
# Stage 3: Transform SS bill IDs
# ============================================================
#' Transform SS bill IDs for South Dakota
#'
#' PVS sometimes encodes session-specific bill IDs with a session digit
#' prefix (e.g., SB60006 for session 6, bill SB0006). This strips the
#' session digit to produce standard 4-digit bill IDs.
#' From old script lines 228-233.
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return SS bills with transformed bill IDs
transform_ss_bills <- function(ss_bills, term) {
  # Strip session digit prefix from 5-digit bill numbers
  # Pattern: (HB|SB) + 1 session digit + 4 bill digits -> (HB|SB) + 4 digits
  ss_bills %>%
    mutate(
      bill_id = gsub("^([HS]B)(\\d)(\\d{4})$", "\\1\\3", .data$bill_id)
    )
}

# ============================================================
# Stage 3: Enrich SS with session
# ============================================================
#' Add session information to SS bills
#'
#' SD is a non-carryover state (bill numbers restart each session).
#' Map SS bill year -> "{year}-RS" session for correct joining.
#' From old script lines 288-289 (join on year).
#'
#' @param ss_bills SS bills dataframe
#' @param term Term string
#' @return SS bills with session column added
enrich_ss_with_session <- function(ss_bills, term) {
  ss_bills %>%
    mutate(session = paste0(.data$year, "-RS"))
}

# ============================================================
# Stage 3: Get missing SS bills
# ============================================================
#' Identify genuinely missing SS bills
#'
#' Excludes committee-sponsored bills and PVS data errors (bills with
#' wrong type prefix that don't exist in raw data) from the missing check.
#' From old script lines 240-248.
#'
#' @param ss_filtered SS bills after filtering
#' @param bill_details Filtered bill_details
#' @param all_bill_details Unfiltered bill_details
#' @return Dataframe of genuinely missing SS bills
get_missing_ss_bills <- function(ss_filtered, bill_details,
                                 all_bill_details) {
  # Find SS bills not in filtered bill_details
  missing_ss <- ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term"))

  # Standardize bill_ids in all_bill_details for lookup
  all_bd_std <- all_bill_details %>%
    mutate(
      bill_id = paste0(
        gsub("[0-9]+", "", .data$bill_id),
        str_pad(gsub("^[A-Z]+", "", .data$bill_id), 4, pad = "0")
      )
    )

  # Only keep missing bills that actually exist in raw data
  # (drops PVS data errors like wrong bill type prefix, e.g.,
  #  SB1259 in SS when actual bill is HB1259)
  missing_in_data <- missing_ss %>%
    semi_join(all_bd_std, by = "bill_id")

  # Check if they're committee-sponsored
  missing_with_info <- missing_in_data %>%
    left_join(
      all_bd_std %>% select("bill_id", "primary_sponsor"),
      by = "bill_id"
    )

  # Filter to valid bill types and exclude committee-sponsored
  genuinely_missing <- missing_with_info %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    ) %>%
    filter(.data$bill_type %in% sd_config$bill_types) %>%
    filter(!grepl("committee", .data$primary_sponsor, ignore.case = TRUE)) %>%
    select("bill_id", "term")

  genuinely_missing
}

# ============================================================
# Stage 4: Post-evaluate bill (achievement overrides)
# ============================================================
#' SD-specific achievement overrides
#'
#' From old script lines 456-473:
#' 1. Style/form vetoes: if governor returns for style/form and both
#'    chambers pass, count as law
#' 2. Veto overrides: if both chambers pass override, count as law
#' 3. Committee motions to amend: if no aic but has "motion to amend"
#'    (not floor motions with h.j./s.j.), count as aic
#' 4. Reconsidered passages: if passed_chamber=1 and law=0, and
#'    chamber reconsidered + only one chamber involved, undo pass
#'
#' @param bill_stages Achievement result from evaluate_bill_history
#' @param bill_row The bill row from bill_details
#' @param bill_history Bill history dataframe for this bill
#' @return Modified bill_stages
post_evaluate_bill <- function(bill_stages, bill_row, bill_history) {
  # 1. Style/form vetoes (old script lines 456-458)
  if (bill_stages$law == 0 &&
    any(grepl(
      "^house.+vetoed for style.+passed", bill_history$action
    )) &&
    any(grepl(
      "^senate.+vetoed for style.+passed", bill_history$action
    ))) {
    bill_stages$action_beyond_comm <- 1
    bill_stages$passed_chamber <- 1
    bill_stages$law <- 1
  }

  # 2. Veto overrides (old script lines 460-462)
  if (bill_stages$law == 0 &&
    any(grepl(
      "^house.+veto override passed", bill_history$action
    )) &&
    any(grepl(
      "^senate.+veto override passed", bill_history$action
    ))) {
    bill_stages$action_beyond_comm <- 1
    bill_stages$passed_chamber <- 1
    bill_stages$law <- 1
  }

  # 3. Committee motions to amend (old script lines 464-466)
  # If no aic, but has "motion to amend" that is NOT a floor motion
  # (floor motions have h.j./s.j. journal references)
  if (bill_stages$action_in_comm == 0 &&
    any(grepl("motion to amend", bill_history$action)) &&
    !any(grepl(
      "motion to amend.+(h\\.j\\.|s\\.j\\.)", bill_history$action
    ))) {
    bill_stages$action_in_comm <- 1
  }

  # 4. Reconsidered passages (old script lines 468-473)
  # If bill passed chamber but not law, and chamber reconsidered the
  # passage, and bill never left originating chamber, undo the pass
  if (bill_stages$passed_chamber == 1 && bill_stages$law == 0) {
    if (any(grepl(
      "(house of representatives|senate) reconsidered, passed",
      bill_history$action
    )) && length(unique(bill_history$chamber)) == 1) {
      bill_stages$passed_chamber <- 0
    }
  }

  bill_stages
}

# ============================================================
# Stage 5: Derive unique sponsors
# ============================================================
#' Derive unique sponsors for South Dakota
#'
#' Includes cosponsor-only legislators (those who cosponsored but
#' never primary-sponsored a bill).
#' From old script lines 531-558.
#'
#' @param bills Dataframe of bills with achievement columns
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregate stats
derive_unique_sponsors <- function(bills, term) {
  # Get primary sponsors
  all_sponsors <- bills %>%
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
      num_cosponsored_bills = NA_real_,
      .groups = "drop"
    )

  # Add cosponsor-only legislators (from old script lines 543-558)
  if ("chamber_sponsors" %in% names(bills)) {
    unique_cospon <- str_trim(unique(unlist(
      str_split(bills$chamber_sponsors, "; ")
    )))
    # Strip "(prime)" from cosponsor names
    unique_cospon <- gsub(" \\(prime\\)", "", unique_cospon)

    for (nonspon in unique_cospon) {
      if (!(nonspon %in% all_sponsors$LES_sponsor) && nonspon != "") {
        ns_adj <- gsub("\\(", "\\\\(", gsub("\\)", "\\\\)", nonspon))
        chamb <- unique(substring(
          bills[grepl(ns_adj, bills$chamber_sponsors), ]$bill_id, 1, 1
        ))
        if ("H" %in% chamb && "S" %in% chamb) {
          cli_warn(glue(
            "CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS"
          ))
        } else if (length(chamb) > 0) {
          all_sponsors <- bind_rows(
            all_sponsors,
            tibble(
              LES_sponsor = nonspon,
              chamber = chamb,
              term = term,
              num_sponsored_bills = 0,
              sponsor_pass_rate = 0,
              sponsor_law_rate = 0,
              num_cosponsored_bills = NA_real_
            )
          )
        }
      }
    }
  }

  all_sponsors
}

# ============================================================
# Stage 5: Compute cosponsorship
# ============================================================
#' Compute cosponsorship counts for South Dakota
#'
#' Uses chamber_sponsors field (initiating chamber sponsors only).
#' From old script lines 561-571.
#'
#' @param all_sponsors Sponsors dataframe
#' @param bills Bills dataframe with chamber_sponsors column
#' @return Sponsors dataframe with num_cosponsored_bills updated
compute_cosponsorship <- function(all_sponsors, bills) {
  if (!("chamber_sponsors" %in% names(bills))) {
    cli_warn("No chamber_sponsors column - skipping cosponsorship")
    return(all_sponsors)
  }

  cli_log("Computing cosponsorship counts...")

  # Build combined match column (sponsor + chamber cosponsors)
  # From old script line 561
  bills$cospon_match <- paste(
    bills$LES_sponsor, bills$chamber_sponsors, sep = "; "
  )

  for (i in seq_len(nrow(all_sponsors))) {
    # Match within same chamber only
    chamber_prefix <- ifelse(
      all_sponsors[i, ]$chamber == "H", "H", "S"
    )
    c_sub <- bills[substring(bills$bill_id, 1, 1) == chamber_prefix, ]

    # Escape parentheses in sponsor names for regex
    sn <- gsub("\\(", "\\\\(", gsub(
      "\\)", "\\\\)", all_sponsors[i, ]$LES_sponsor
    ))

    # Count occurrences using boundary-aware regex
    # From old script line 566
    search_term <- paste0(
      "^", sn, "$|^", sn, ";|; ", sn, "$|; ", sn, ";"
    )
    all_sponsors$num_cosponsored_bills[i] <- sum(
      grepl(search_term, tolower(c_sub$cospon_match))
    )

    # Subtract primary sponsorship count
    # From old script lines 568-569
    all_sponsors$num_cosponsored_bills[i] <-
      all_sponsors$num_cosponsored_bills[i] -
      all_sponsors$num_sponsored_bills[i]
  }

  # Validate no negative counts (from old script lines 576-578)
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

  all_sponsors
}

# ============================================================
# Stage 5: Clean sponsor names
# ============================================================
#' Clean sponsor names for matching
#'
#' SD uses full LES_sponsor + chamber initial for matching.
#' From old script lines 605-611.
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

# ============================================================
# Stage 5: Adjust legiscan data
# ============================================================
#' Adjust legiscan data for matching
#'
#' SD uses last_name for unique legislators, "last_name (first_name)"
#' for legislators sharing a last name within the same role.
#' From old script lines 635-643.
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string
#' @return Adjusted legiscan dataframe with match_name_chamber column
adjust_legiscan_data <- function(legiscan, term) {
  if (term == "2023_2024") {
    # Zikmund (17190) has both Sen (2023) and Rep (2024) records.
    # He sponsors SB bills, so keep Sen record only.
    legiscan <- legiscan %>%
      filter(!(.data$people_id == 17190 & .data$role == "Rep"))
  }

  legiscan %>%
    filter(.data$committee_id == 0) %>%
    group_by(.data$last_name) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      match_name = case_when(
        .data$n >= 2 ~ paste0(.data$last_name, " (", .data$first_name, ")"),
        TRUE ~ .data$last_name
      ),
      match_name_chamber = tolower(paste(
        .data$match_name,
        substr(.data$district, 1, 1),
        sep = "-"
      ))
    )
}

# ============================================================
# Stage 5: Reconcile legiscan with sponsors
# ============================================================
#' Reconcile legiscan with sponsors using fuzzy matching
#'
#' Term-specific custom_match entries handle name disambiguation.
#' Custom matches will be populated after the first run identifies
#' mismatches.
#'
#' @param all_sponsors Sponsors dataframe
#' @param legiscan_adjusted Adjusted legiscan dataframe
#' @param term Term string
#' @return Joined dataframe
# nolint start: line_length_linter
reconcile_legiscan_with_sponsors <- function(all_sponsors,
                                              legiscan_adjusted, term) {
  if (term == "2023_2024") {
    inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # Peterson disambiguation: two Petersons in sponsor data
        "lucas-peterson-h" = "peterson (sue)-h",
        "peterson-h" = "peterson (drew)-h",
        # Brent Hoffman (Sen) -> sponsor "hoffman"
        "hoffman (brent)-s" = "hoffman-s",
        # Block zero-bill legislators from wrong fuzzy matches
        "hoffman (charles)-h" = NA_character_,
        "walsh-s" = NA_character_,
        "conzet-h" = NA_character_,
        "wink-s" = NA_character_
      )
    )
  } else {
    # Default for other terms
    inexact::inexact_join(
      x = legiscan_adjusted,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full"
    )
  }
}
# nolint end: line_length_linter

# ============================================================
# Stage 7: Prepare bills for LES calculation
# ============================================================
#' Prepare bills for LES calculation
#'
#' Derives chamber from bill_id prefix (H/S).
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
  sd_config,
  list(
    load_bill_files = load_bill_files,
    preprocess_raw_data = preprocess_raw_data,
    clean_bill_details = clean_bill_details,
    clean_bill_history = clean_bill_history,
    transform_ss_bills = transform_ss_bills,
    enrich_ss_with_session = enrich_ss_with_session,
    get_missing_ss_bills = get_missing_ss_bills,
    post_evaluate_bill = post_evaluate_bill,
    derive_unique_sponsors = derive_unique_sponsors,
    compute_cosponsorship = compute_cosponsorship,
    clean_sponsor_names = clean_sponsor_names,
    adjust_legiscan_data = adjust_legiscan_data,
    reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
    prepare_bills_for_les = prepare_bills_for_les
  )
)
