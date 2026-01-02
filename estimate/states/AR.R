# Arkansas State Configuration
# Bill types, terms, and state-specific cleaning functions

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
logging <- source(file.path(repo_root, "utils/logging.R"),
  local = TRUE
)$value
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
library(stringr)
library(glue)

# Arkansas Configuration
ar_config <- list(
  # Valid bill types for AR (House Bills and Senate Bills)
  bill_types = c("HB", "SB"),

  # Sponsors to drop from analysis (committees, agencies, etc.)
  # These don't represent individual legislator effectiveness
  drop_sponsors = c(
    "efficiency", "house agri", "house cty co", "house management",
    "house mgmt. comm.", "jbc", "jic ins", "judiciary", "public hlth comm",
    "senate efficiency", "st. ag.", "insurance", "revenue", "state agencies"
  ),

  # Terms for AR (uses assembly numbers, but we map to years)
  # Format: "2023_2024" maps to "94th General Assembly"
  step_terms = list(
    aic = c(
      "referred to committee", "assigned to committee", "committed to committee",
      "reported by committee", "returned by committee", "do pass", "do not pass",
      "without recommendation"
    ),
    abc = c(
      "returned by the committee", "engrossed", "third time", "amendment.+read",
      "re-referred to", "committee of the whole", "placed on the calendar",
      "placed on calendar"
    ),
    pass = c(
      "third time and passed", "transmitted to the senate", "transmitted to the house"
    ),
    law = c("act [0-9]+")
  ),
  # Term to assembly mapping
  term_to_assembly = list(
    "2023_2024" = "94th_General_Assembly"
  )
)

#' Check if a bill should be dropped based on sponsor
#'
#' Used to filter out bills that aren't sponsored by individual legislators
#' (e.g., committee-sponsored bills, agency bills).
#'
#' @param sponsor Sponsor name
#' @return TRUE if bill should be dropped, FALSE otherwise
should_drop_bill <- function(sponsor) {
  tolower(sponsor) %in% ar_config$drop_sponsors |
    grepl("committee", sponsor, ignore.case = TRUE)
}

# Stage 1.5 Hook: Preprocess raw data to normalize column names

#' Preprocess raw data for Arkansas
#'
#' Normalizes raw CSV data into standard schema expected by generic pipeline.
#' Handles AR-specific column naming conventions.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string (unused but provided for consistency)
#' @return List with preprocessed bill_details and bill_history
preprocess_raw_data <- function(bill_details, bill_history, term) {
  # AR uses "bill_num" in raw data, rename to "bill_id"
  bill_details <- bill_details %>%
    rename(bill_id = "bill_num")

  bill_history <- bill_history %>%
    rename(bill_id = "bill_num")

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

#' Fix session variable in bill dataframes
#'
#' @param df Bill details or history dataframe
#' @param term Term string (e.g., "2023_2024")
#' @return Dataframe with fixed session column
fix_sessions <- function(df, term) {
  if (term == "2007_2008") {
    df$session <- recode(df$session,
      "Regular Session" = "Regular Session, 2007",
      "First Extraordinary Session of 86th General Assembly" =
        "First Extraordinary Session, 2008"
    )
  } else if (term == "2009_2010") {
    df$session <- recode(df$session,
      "Regular Session" = "Regular Session, 2009",
      "Fiscal Session" = "Fiscal Session, 2010"
    )
  } else if (term == "2011_2012") {
    df$session <- recode(df$session,
      "Regular Session" = "Regular Session, 2011",
      "Fiscal Session" = "Fiscal Session, 2012"
    )
  } else if (term == "2013_2014") {
    df$session <- recode(df$session,
      "Regular Session" = "Regular Session, 2013"
    )
  }
  df
}

#' Reshape bill details or history to standard format
#'
#' @param df Bill details or history dataframe (bill_num already renamed to bill_id)
#' @param term Term string (e.g., "2023_2024")
#' @return Reshaped dataframe
reshape_bill_data <- function(df, term) {
  df <- df %>%
    mutate(
      term = term,
      ga_num = gsub(" .+", "", .data$ga_num),
      bill_id = paste0(
        gsub("[0-9]+", "", .data$bill_id),
        str_pad(gsub("[A-Z]+", "", .data$bill_id), 4, pad = 0)
      ),
      session_year = gsub(".+, ", "", .data$session),
      session = gsub(", [0-9]+", "", .data$session),
      session = recode(.data$session,
        "Regular Session" = "RS",
        "First Extraordinary Session" = "SS1",
        "Second Extraordinary Session" = "SS2",
        "Third Extraordinary Session" = "SS3",
        "Fourth Extraordinary Session" = "SS4",
        "Fiscal Session" = "FS"
      ),
      session = paste(.data$session_year, .data$session, sep = "-")
    )
  df
}

#' Fix bill IDs for specific terms
#'
#' @param bill_ids Vector of bill IDs
#' @param term Term string
#' @return Fixed bill IDs
fix_bill_ids <- function(bill_ids, term) {
  if (term == "2019_2020") {
    bill_ids[bill_ids == "SB20002"] <- "SB0002"
  } else if (term == "2021_2022") {
    bill_ids[bill_ids == "SB10001"] <- "SB0001"
    bill_ids[bill_ids == "SB60006"] <- "SB0006"
    bill_ids[bill_ids == "SB20002"] <- "SB0002"
    bill_ids[bill_ids == "SB40004"] <- "SB0004"
  }
  bill_ids
}

#' Fix sponsor names for specific terms
#'
#' @param names Vector of sponsor names
#' @param term Term string
#' @return Fixed sponsor names
fix_names <- function(names, term) {
  if (term == "1997_1998") {
    names[names == "mcgeheee"] <- "mcgehee"
    names[names == "flangin"] <- "flanagin"
    names[names == "hudson et al"] <- "j. hudson et al"
  } else if (term == "1999_2000") {
    names[names == "brown"] <- "j. brown"
  } else if (term == "2003_2004") {
    names[names == "prater"] <- "l. prater"
    names[names == "elliott"] <- "j. elliott"
  } else if (term == "2009_2010") {
    names[names == "wyatt"] <- "d. wyatt"
  } else if (term == "2023_2024") {
    # S. Richardson = R. Scott Richardson (same person)
    names[names == "s. richardson"] <- "r. scott richardson"
  }
  names
}

#' Clean bill details for Arkansas
#'
#' Stage 2 hook: Cleans and standardizes AR bill details
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term) {
  # Sort and fix sessions (bill_id already created from bill_num)
  bill_details <- arrange(bill_details, .data$bill_id)
  bill_details <- fix_sessions(bill_details, term)
  bill_details <- reshape_bill_data(bill_details, term)

  # Standardize sponsor names (lowercase, remove accents, apply fixes)
  bill_details$primary_sponsors <- tolower(bill_details$primary_sponsors)
  bill_details$primary_sponsors <- standardize_accents(
    bill_details$primary_sponsors
  )
  bill_details$primary_sponsors <- fix_names(
    bill_details$primary_sponsors,
    term
  )

  # Create LES_sponsor (remove "et al" and "& ..." suffixes)
  bill_details$LES_sponsor <- gsub(
    " et al$| \\&.+", "",
    str_trim(bill_details$primary_sponsors)
  )

  # Keep copy before filtering
  all_bill_details <- bill_details

  # Derive bill_type and filter to valid types
  bill_details <- bill_details %>%
    mutate(bill_type = str_trim(toupper(gsub(
      "[0-9].+|[0-9]+", "",
      .data$bill_id
    )))) %>%
    filter(.data$bill_type %in% ar_config$bill_types)

  # Clean up extra spaces
  bill_details$LES_sponsor <- gsub("  +", " ", bill_details$LES_sponsor)

  # Fix bill IDs
  bill_details$bill_id <- fix_bill_ids(bill_details$bill_id, term)

  # Drop committee-sponsored and empty-sponsor bills
  comm_bills <- bill_details %>%
    filter(should_drop_bill(.data$LES_sponsor))

  if (nrow(comm_bills) > 0) {
    cli_log(glue("Dropping {nrow(comm_bills)} committee-sponsored bills"))
    bill_details <- bill_details %>%
      filter(!should_drop_bill(.data$LES_sponsor))
  }

  empty_sponsor_bills <- bill_details %>%
    filter(.data$LES_sponsor == "" | is.na(.data$LES_sponsor))

  if (nrow(empty_sponsor_bills) > 0) {
    cli_log(glue("Dropping {nrow(empty_sponsor_bills)} bills without sponsor"))
    bill_details <- bill_details %>%
      filter(!(.data$LES_sponsor == "" | is.na(.data$LES_sponsor)))
  }

  list(all_bill_details = all_bill_details, bill_details = bill_details)
}

#' Clean bill history for Arkansas
#'
#' Stage 2 hook: Cleans and standardizes AR bill history
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill history dataframe
clean_bill_history <- function(bill_history, term) {
  bill_history <- fix_sessions(bill_history, term)
  bill_history <- reshape_bill_data(bill_history, term)
  bill_history <- arrange(
    bill_history, .data$term, .data$session,
    .data$bill_id, .data$order
  )
  bill_history$action <- str_trim(bill_history$action)
  bill_history
}

#' Get bill file suffix for AR
#'
#' AR uses assembly names in file paths instead of years
#'
#' @param term Term string (e.g., "2023_2024")
#' @return File suffix string (e.g., "94th_General_Assembly")
get_bill_file_suffix <- function(term) {
  assembly <- ar_config$term_to_assembly[[term]]
  if (is.null(assembly)) {
    stop(glue("Unknown term for AR: {term}"))
  }
  assembly
}

# Stage 5 Hook: Derive unique sponsors for Arkansas
#'
#' Extracts unique sponsors from bills with chamber assignment
#'
#' @param bills Dataframe of bills with achievement matrix
#' @param term Term string (e.g., "2023_2024")
#' @return Dataframe of unique sponsors with stats
derive_unique_sponsors <- function(bills, term) {
  bills %>%
    mutate(chamber = ifelse(substring(.data$bill_id, 1, 1) == "H",
      "H", "S"
    )) %>%
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

# Stage 5 Hook: Compute cosponsorship for Arkansas
#'
#' AR does not track cosponsorship
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param bills Dataframe of bills
#' @return Dataframe with num_cosponsored_bills = NA
compute_cosponsorship <- function(all_sponsors, bills) {
  all_sponsors$num_cosponsored_bills <- NA
  all_sponsors
}

# Stage 5 Hook: Clean sponsor names for Arkansas
#'
#' Creates match_name_chamber for fuzzy matching
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param term Term string (e.g., "2023_2024")
#' @return Dataframe with match_name_chamber added
clean_sponsor_names <- function(all_sponsors, term) {
  # Handle term-specific name corrections
  if (term == "1997_1998") {
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "ingram",
        "owensingram", .data$LES_sponsor
      ))
  } else if (term == "2017_2018") {
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "barker",
        "eubanksbarker", .data$LES_sponsor
      ))
  }

  # Create match_name_chamber
  all_sponsors %>%
    arrange(.data$chamber, .data$LES_sponsor) %>%
    distinct() %>%
    mutate(match_name_chamber = tolower(
      paste(.data$LES_sponsor,
        substr(.data$chamber, 1, 1),
        sep = "-"
      )
    ))
}

# Stage 5 Hook: Adjust legiscan data for Arkansas
#'
#' Creates match_name and match_name_chamber for fuzzy matching.
#' Ported from original AR - Estimate LES NWZ.R (lines 596-611).
#'
#' Logic:
#' 1. Count distinct people per last_name
#' 2. If 1 person: use last_name
#' 3. If 2 people: use "F. LastName" (first initial)
#' 4. If 3+ people: use "FM. LastName" (first + middle initials)
#' 5. If still collisions after step 2-4: use "F. LastName" form
#'
#' Ambiguous cases are handled via custom_match in reconcile_legiscan_with_sponsors.
#'
#' @param legiscan Dataframe of legiscan legislator data
#' @param term Term string (e.g., "2023_2024")
#' @return Dataframe with match_name and match_name_chamber added
adjust_legiscan_data <- function(legiscan, term) {
  legiscan %>%
    filter(.data$committee_id == 0) %>%
    # Count distinct people per last name (use n_distinct to handle
    # same person appearing in multiple sessions)
    group_by(.data$last_name) %>%
    mutate(n = n_distinct(.data$first_name, .data$middle_name)) %>%
    ungroup() %>%
    mutate(match_name = case_when(
      .data$n > 2 ~ paste0(
        substr(.data$first_name, 1, 1),
        substr(.data$middle_name, 1, 1), ". ",
        .data$last_name
      ),
      .data$n == 2 ~ paste(substr(.data$first_name, 1, 1),
        .data$last_name,
        sep = ". "
      ),
      TRUE ~ .data$last_name
    )) %>%
    # Check for collisions after initial assignment - if same match_name
    # maps to different people, use full first name to disambiguate
    group_by(.data$match_name) %>%
    mutate(collision_count = n_distinct(.data$first_name, .data$last_name)) %>%
    ungroup() %>%
    mutate(match_name = ifelse(.data$collision_count > 1,
      paste(.data$first_name, .data$last_name),
      .data$match_name
    )) %>%
    mutate(match_name_chamber = tolower(paste(
      .data$match_name,
      substr(.data$district, 1, 1),
      sep = "-"
    )))
}

# Stage 5 Hook: Reconcile legiscan with sponsors for Arkansas
#'
#' Performs fuzzy matching with term-specific custom matches
#'
#' @param sponsors Dataframe of bill sponsors
#' @param legiscan Dataframe of adjusted legiscan data
#' @param term Term string (e.g., "2023_2024")
#' @return Dataframe of matched sponsors and legiscan records
reconcile_legiscan_with_sponsors <- function(sponsors, legiscan, term) {
  # Term-specific custom matching rules
  if (term == "2019_2020") {
    all_sponsors2 <- inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "deffenbaugh-h" = NA_character_,
        "springer-h" = NA_character_,
        "meeks-h" = "s. meeks-h",
        "hammer-s" = "k. hammer-s",
        "eads-s" = "l. eads-s",
        "gray-h" = "m. gray-h",
        "nicks-h" = NA_character_,
        "sturch-s" = "j. sturch-s",
        "ennett-h" = NA_character_,
        "mcgrew-h" = NA_character_,
        "walker-h" = NA_character_,
        "c. cooper-h" = NA_character_,
        "berry-h" = NA_character_
      )
    )
  } else if (term == "2021_2022") {
    all_sponsors2 <- inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "deffenbaugh-h" = NA_character_,
        "mcelroy-h" = NA_character_,
        "pitsch-s" = NA_character_,
        "springer-h" = NA_character_,
        "j. dotson-s" = NA_character_,
        "fulfer-s" = NA_character_,
        "meeks-h" = "s. meeks-h",
        "hammer-s" = "k. hammer-s",
        "clark-s" = "a. clark-s",
        "eads-s" = "l. eads-s",
        "mcnair-h" = NA_character_,
        "nicks-h" = NA_character_,
        "beaty-h" = "beaty jr.-h",
        "s. flowers-s" = NA_character_,
        "mayberry-h" = "j. mayberry-h",
        "d. garner-h" = NA_character_,
        "d. ferguson-h" = NA_character_,
        "gray-h" = "m. gray-h",
        "s. berry-h" = NA_character_
      )
    )
  } else if (term == "2023_2024") {
    # Custom matches for name disambiguation
    all_sponsors2 <- inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "meeks-h" = "s. meeks-h",
        "hammer-s" = "k. hammer-s",
        "beaty-h" = "beaty jr.-h",
        "mayberry-h" = "j. mayberry-h",
        "garner-h" = "d. garner-h",
        "r. richardson-h" = "r. scott richardson-h", # S. Richardson → R. Scott Richardson via fix_names
        "king-s" = "b. king-s",
        "love-s" = "f. love-s",
        "dotson-s" = "j. dotson-s",
        "davis-s" = "b. davis-s",
        "petty-s" = "j. petty-s",
        "mckee-s" = "m. mckee-s"
      )
    )
  } else {
    # Default: standard fuzzy matching
    all_sponsors2 <- inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full"
    )
  }

  all_sponsors2
}

# Stage 6 Hook: Prepare bills data for LES calculation
#'
#' Adds chamber column to bills based on bill_id prefix
#'
#' @param bills Dataframe of bills
#' @return Dataframe with chamber column added
prepare_bills_for_les <- function(bills) {
  bills %>%
    mutate(chamber = ifelse(substring(.data$bill_id, 1, 1) == "H",
      "H", "S"
    ))
}

#' Get missing SS bills for Arkansas
#'
#' Stage 3 hook: Determines which SS bills are genuinely missing from bill_details
#' vs. intentionally excluded (committee-sponsored or empty-sponsor bills).
#'
#' @param ss_filtered Filtered SS bills for this term
#' @param bill_details Cleaned bill details (after filtering)
#' @param all_bill_details All bill details (before filtering)
#' @return Dataframe of genuinely missing SS bills (bill_id, term)
get_missing_ss_bills <- function(ss_filtered, bill_details, all_bill_details) {
  ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term")) %>%
    left_join(
      all_bill_details %>% select("bill_id", "primary_sponsors"),
      by = "bill_id"
    ) %>%
    # Only flag if bill exists in all_bill_details with non-excluded sponsor
    filter(!is.na(.data$primary_sponsors)) %>%
    filter(.data$primary_sponsors != "") %>%
    filter(!should_drop_bill(.data$primary_sponsors)) %>%
    select("bill_id", "term")
}

# Export config and functions
list(
  bill_types = ar_config$bill_types,
  step_terms = ar_config$step_terms,
  preprocess_raw_data = preprocess_raw_data,
  clean_bill_details = clean_bill_details,
  clean_bill_history = clean_bill_history,
  get_bill_file_suffix = get_bill_file_suffix,
  derive_unique_sponsors = derive_unique_sponsors,
  compute_cosponsorship = compute_cosponsorship,
  clean_sponsor_names = clean_sponsor_names,
  adjust_legiscan_data = adjust_legiscan_data,
  reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
  prepare_bills_for_les = prepare_bills_for_les,
  get_missing_ss_bills = get_missing_ss_bills
)
