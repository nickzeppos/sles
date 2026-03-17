# Missouri State Configuration
# State-specific constants and settings for LES estimation
#
# MO notes from old script:
# - Data is split between House and Senate files (4 CSVs per term)
# - Bills do NOT carry over; numbers increment across sessions within term
# - Special session bill numbers restart at 1 for each special session
# - House sponsors are "last, first (district)" format
# - Senate sponsors are just last name
# - No cosponsorship data
# - Different action terminology for House vs Senate bill histories
# - Session types: 1st RS, 2nd RS -> RS; ES/1st ES -> SS1; 2nd ES -> SS2

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
library(tidyr)

# nolint start: line_length_linter
mo_config <- list(
  bill_types = c("HB", "SB"),
  drop_sponsor_pattern = "committee"
  # No static step_terms — uses get_step_terms hook for chamber-specific terms
)

# House step terms (old script lines 414-422)
mo_house_step_terms <- list(
  aic = c(
    "hearing room", "committee room", "^date:.+time:",
    "reported do pass", "reported do not pass",
    "voted do pass", "voted do not pass",
    "public hearing", "executive session"
  ),
  abc = c(
    "reported do pass", "third read", "^perfected",
    "ayes:", "noes:", "placed on.+calendar", "taken up for"
  ),
  pass = c(
    "third read and passed", "reported to the senate",
    "^signed by", "truly agreed to and finally passed"
  ),
  law = c(
    "approved by governor", "approved by the governor",
    "no action taken by governor", "vetoed in part by governor",
    "passed over veto", "approved.+acting governor"
  )
)

# Senate step terms (old script lines 425-431)
mo_senate_step_terms <- list(
  aic = c(
    "hearing conducted", "hearing scheduled",
    "voted do pass", "voted do not pass",
    "reported do", "reported from"
  ),
  abc = c(
    "reported from", "reported do pass", "third read",
    "perfected", "calendar", "taken up", "bill taken from comm"
  ),
  pass = c(
    "third read and passed", "truly agreed to and finally passed",
    "duly enrolled", "signed by"
  ),
  law = c(
    "signed by governor", "signed by the governor",
    "no action taken by governor", "vetoed in part by governor",
    "legislature voted to override", "signed by acting governor"
  )
)
# nolint end: line_length_linter

#' Check if a bill should be dropped based on sponsor
#'
#' @param sponsor Sponsor name
#' @return TRUE if bill should be dropped, FALSE otherwise
should_drop_bill <- function(sponsor) {
  grepl(mo_config$drop_sponsor_pattern, sponsor, ignore.case = TRUE)
}

#' Get ordinal suffix for MO General Assembly number
#'
#' @param n Integer GA number
#' @return String like "102nd"
get_ordinal <- function(n) {
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

#' Get chamber-specific step terms based on bill ID prefix
#'
#' @param bill_id Bill identifier
#' @param term Term string (unused, kept for hook signature)
#' @return List of step terms (aic, abc, pass, law)
get_step_terms <- function(bill_id, term) {
  if (substring(bill_id, 1, 1) == "H") {
    mo_house_step_terms
  } else {
    mo_senate_step_terms
  }
}

# Stage 1 Hook: Load bill files
#' Load bill files for Missouri
#'
#' MO has 4 files per term: House/Senate details and histories.
#' File naming uses ordinal GA number: e.g., 102nd for 2023_2024.
#' Formula: GA = (first_year - 1821) / 2 + 1
#'
#' @param bill_dir Path to bill directory
#' @param state State code ("MO")
#' @param term Term in format "YYYY_YYYY"
#' @param verbose Show detailed logging (default TRUE)
#' @return List with bill_details and bill_history dataframes
load_bill_files <- function(bill_dir, state, term, verbose = TRUE) {
  years <- strsplit(term, "_")[[1]]
  first_year <- as.integer(years[1])

  # Compute GA number and ordinal
  ga_num <- (first_year - 1821) / 2 + 1
  t_num <- get_ordinal(ga_num)

  if (verbose) {
    cli_log(glue("MO General Assembly: {t_num}"))
  }

  # Load House files
  h_detail_path <- file.path(
    bill_dir,
    glue("{state}_Bill_Details_{t_num}_House.csv")
  )
  h_hist_path <- file.path(
    bill_dir,
    glue("{state}_Bill_Histories_{t_num}_House.csv")
  )

  # Load Senate files
  s_detail_path <- file.path(
    bill_dir,
    glue("{state}_Bill_Details_{t_num}_Senate.csv")
  )
  s_hist_path <- file.path(
    bill_dir,
    glue("{state}_Bill_Histories_{t_num}_Senate.csv")
  )

  h_bills <- read.csv(h_detail_path, stringsAsFactors = FALSE)
  s_bills <- read.csv(s_detail_path, stringsAsFactors = FALSE)
  h_hist <- read.csv(h_hist_path, stringsAsFactors = FALSE)
  s_hist <- read.csv(s_hist_path, stringsAsFactors = FALSE)

  if (verbose) {
    cli_log(glue(
      "  House: {nrow(h_bills)} details, {nrow(h_hist)} history rows"
    ))
    cli_log(glue(
      "  Senate: {nrow(s_bills)} details, {nrow(s_hist)} history rows"
    ))
  }

  bill_details <- bind_rows(h_bills, s_bills)
  bill_history <- bind_rows(h_hist, s_hist)

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

# Stage 1.5 Hook: Preprocess raw data
#' Preprocess raw data for Missouri
#'
#' Normalizes raw CSV data: renames bill_number -> bill_id,
#' recodes session_type, creates session column, deduplicates.
#' Faithful port of old script lines 128-145, 384-393.
#'
#' @param bill_details Bill details dataframe
#' @param bill_history Bill history dataframe
#' @param term Term string
#' @return List with preprocessed dataframes
preprocess_raw_data <- function(bill_details, bill_history, term) {
  # Rename bill_number -> bill_id
  bill_details <- bill_details %>%
    rename(bill_id = "bill_number")
  bill_history <- bill_history %>%
    rename(bill_id = "bill_number")

  # Add term, recode session_type, create session
  bill_details <- bill_details %>%
    mutate(
      term = term,
      session_type = recode(.data$session_type,
        "1st RS" = "RS", "2nd RS" = "RS",
        "ES" = "SS1", "1st ES" = "SS1",
        "2nd ES" = "SS2", "3rd ES" = "SS3"
      ),
      session = paste0(.data$session_year, "-", .data$session_type)
    ) %>%
    select(-"session_type", -"session_year")

  bill_history <- bill_history %>%
    mutate(
      term = term,
      session_type = recode(.data$session_type,
        "1st RS" = "RS", "2nd RS" = "RS",
        "ES" = "SS1", "1st ES" = "SS1",
        "2nd ES" = "SS2", "3rd ES" = "SS3"
      ),
      session = paste0(.data$session_year, "-", .data$session_type)
    ) %>%
    select(-"session_type", -"session_year")

  # Deduplicate bill_details (old script lines 135-137)
  bill_details <- distinct(bill_details)

  list(
    bill_details = bill_details,
    bill_history = bill_history
  )
}

#' Clean bill details for Missouri
#'
#' MO-specific transformations:
#' - Filter to HB/SB bill types
#' - Extract primary sponsor (first before semicolon)
#' - Extract district from (NNN) suffix on House sponsors
#' - Lowercase and standardize accents
#' - Remove "Dr." title
#' - Term-specific name corrections
#' - Extract nicknames from parentheses/quotes
#' - Drop committee-sponsored and empty-sponsor bills
#'
#' Faithful port of old script lines 148-336.
#'
#' @param bill_details Dataframe of bill details
#' @param term Term string (e.g., "2023_2024")
#' @param verbose Show detailed logging (default TRUE)
#' @return List with all_bill_details and filtered bill_details
clean_bill_details <- function(bill_details, term, verbose = TRUE) {
  # Derive bill_type, save all_bill_details before filtering
  bill_details <- bill_details %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    )
  all_bill_details <- bill_details

  # Filter to HB/SB
  bill_details <- bill_details %>%
    filter(.data$bill_type %in% mo_config$bill_types) %>%
    select(-"bill_type")

  if (verbose) {
    cli_log(glue("Bills after type filter: {nrow(bill_details)}"))
  }

  # Strip semicolons+ from primary_sponsor (first sponsor only)
  bill_details$primary_sponsor <- gsub(
    ";.+", "", str_trim(bill_details$primary_sponsor)
  )

  # Extract district from (NNN) suffix on House sponsors
  bill_details$sponsor_dist <- gsub(
    "\\(|\\)", "",
    str_extract(bill_details$primary_sponsor, "\\([0-9]+\\)$")
  )
  bill_details$primary_sponsor <- str_trim(
    gsub("\\([0-9]+\\)$", "", bill_details$primary_sponsor)
  )

  # Lowercase and standardize accents
  bill_details$primary_sponsor <- tolower(bill_details$primary_sponsor)
  bill_details$primary_sponsor <- standardize_accents(
    bill_details$primary_sponsor
  )

  # Remove "Dr." title
  bill_details$primary_sponsor <- gsub(
    ", dr\\. ", ", ", bill_details$primary_sponsor
  )

  # Warn/stop on "by request" bills
  if (any(grepl(
    "\\(br\\)|by request| br$", bill_details$primary_sponsor
  ))) {
    cli_warn("BY REQUEST bills detected - review needed")
  }

  # Term-specific name corrections (old script lines 181-197)
  if (term == "1997_1998") {
    bill_details$primary_sponsor <- ifelse(
      str_trim(bill_details$primary_sponsor) == "demarce, karl",
      "demarce, karl a.", bill_details$primary_sponsor
    )
    bill_details$primary_sponsor <- ifelse(
      str_trim(bill_details$primary_sponsor) == "stokan, lana",
      "stokan, lana ladd", bill_details$primary_sponsor
    )
    bill_details$primary_sponsor <- ifelse(
      str_trim(bill_details$primary_sponsor) == "townley, merrill m",
      "townley, merrill m.", bill_details$primary_sponsor
    )
    bill_details$primary_sponsor <- ifelse(
      str_trim(bill_details$primary_sponsor) == "graham, jim",
      "graham, james", bill_details$primary_sponsor
    )
  } else if (term == "2003_2004") {
    bill_details$primary_sponsor <- ifelse(
      str_trim(bill_details$primary_sponsor) == "roark, brad",
      "roark, bradley g.", bill_details$primary_sponsor
    )
  } else if (term == "2007_2008") {
    bill_details$primary_sponsor <- ifelse(
      str_trim(bill_details$primary_sponsor) == "scharnhorst",
      "scharnhorst, dwight", bill_details$primary_sponsor
    )
  } else if (term == "2009_2010") {
    bill_details$primary_sponsor <- ifelse(
      str_trim(bill_details$primary_sponsor) == "fischer, linda",
      "black, linda", bill_details$primary_sponsor
    )
    bill_details$primary_sponsor <- ifelse(
      str_trim(bill_details$primary_sponsor) == "stacey newman",
      "newman, stacey", bill_details$primary_sponsor
    )
  } else if (term == "2021_2022") {
    bill_details$primary_sponsor <- ifelse(
      bill_details$primary_sponsor == "rehder",
      "thompson rehder", bill_details$primary_sponsor
    )
  }

  # Set LES_sponsor = trimmed primary_sponsor
  bill_details$LES_sponsor <- str_trim(bill_details$primary_sponsor)

  # Extract nicknames from parentheses/quotes
  bill_details$nickname <- str_trim(gsub(
    '\\(|\\)|"', "",
    str_extract(
      bill_details$LES_sponsor,
      ' \\([a-z]+\\)$| "[a-z]+"$'
    )
  ))
  bill_details$LES_sponsor <- str_trim(gsub(
    "  +", " ",
    gsub(' \\([a-z]+\\)$| "[a-z]+"$', "", bill_details$LES_sponsor)
  ))

  # Drop committee-sponsored bills
  comm_bills <- bill_details %>%
    filter(should_drop_bill(.data$LES_sponsor))

  if (nrow(comm_bills) > 0) {
    if (verbose) {
      cli_log(glue(
        "Dropping {nrow(comm_bills)} committee-sponsored bills"
      ))
    }
    bill_details <- bill_details %>%
      filter(!should_drop_bill(.data$LES_sponsor))
  }

  # Drop empty sponsors
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

#' Clean bill history for Missouri
#'
#' MO has different chamber detection strategies for House vs Senate
#' bill histories. Faithful port of old script lines 356-399.
#'
#' @param bill_history Dataframe of bill history
#' @param term Term string (e.g., "2023_2024")
#' @return Cleaned bill_history dataframe
clean_bill_history <- function(bill_history, term) {
  # Split into House and Senate histories by bill_id prefix
  h_hist <- bill_history %>%
    filter(substring(.data$bill_id, 1, 1) == "H")
  s_hist <- bill_history %>%
    filter(substring(.data$bill_id, 1, 1) == "S")

  # --- House chamber detection (old script lines 360-363) ---
  # Journal page works better than (H)/(S) suffix
  h_hist$chamber <- gsub(
    " | [0-9].+| [0-9]+|", "", h_hist$journal_page
  )
  h_hist$chamber <- ifelse(
    h_hist$chamber == "",
    gsub("\\(|\\)", "", str_extract(h_hist$action, "\\(H\\)$|\\(S\\)$")),
    h_hist$chamber
  )
  h_hist <- h_hist %>%
    group_by(.data$session, .data$bill_id) %>%
    fill("chamber") %>%
    ungroup()

  # --- Senate chamber detection (old script lines 370-378) ---
  # Extract chamber from action text after stripping common prefixes
  s_hist$chamber <- toupper(str_extract(
    gsub(
      paste0(
        "^referred |^reported from |^reported do pass |",
        "^hearing conducted |^hearing cancelled |",
        "^reported duly enrolled |^voted do pass |",
        "^voted do not pass |^reported truly perfected "
      ),
      "",
      tolower(s_hist$action)
    ),
    "^h |^s "
  ))
  # Fall back to action suffix " H" or " S" before adopted/defeated
  s_hist$chamber <- ifelse(
    is.na(s_hist$chamber),
    str_extract(
      gsub(" adopted$| Adopted$| defeated$| Defeated$", "", s_hist$action),
      " H$| S$"
    ),
    s_hist$chamber
  )
  s_hist$chamber <- str_trim(s_hist$chamber)
  # Handle specific signing actions
  s_hist$chamber <- ifelse(
    is.na(s_hist$chamber) &
      grepl("Signed by Senate President", s_hist$action),
    "S", s_hist$chamber
  )
  s_hist$chamber <- ifelse(
    is.na(s_hist$chamber) &
      grepl("Signed by House Speaker", s_hist$action),
    "H", s_hist$chamber
  )
  # Governor actions
  s_hist$chamber <- ifelse(
    grepl("Governor", s_hist$action), "G", s_hist$chamber
  )
  # Fall back to journal_page
  s_hist$chamber <- ifelse(
    is.na(s_hist$chamber) & s_hist$journal_page != "",
    gsub("[0-9]+|[0-9].+", "", s_hist$journal_page),
    s_hist$chamber
  )
  s_hist <- s_hist %>%
    group_by(.data$session, .data$bill_id) %>%
    fill("chamber") %>%
    ungroup()

  # Combine
  bill_history <- bind_rows(h_hist, s_hist)

  # Arrange by session, bill_id, order
  bill_history <- bill_history %>%
    arrange(.data$session, .data$bill_id, .data$order)

  # Fill remaining NA chambers from bill_id prefix (line 396)
  bill_history$chamber <- ifelse(
    is.na(bill_history$chamber),
    substring(bill_history$bill_id, 1, 1),
    bill_history$chamber
  )

  # Recode to full chamber names (line 399)
  bill_history$chamber <- recode(
    bill_history$chamber,
    "H" = "House", "S" = "Senate", "G" = "Governor"
  )

  # Lowercase action for pattern matching
  bill_history$action <- tolower(bill_history$action)

  bill_history
}

#' Transform SS bills for Missouri
#'
#' Faithful port of old script lines 83-93, 217-227.
#'
#' @param ss_bills SS bills dataframe (already filtered to term)
#' @param term Term string
#' @return Transformed SS bills
transform_ss_bills <- function(ss_bills, term) {
  # Term-specific ID corrections
  if (term == "2019_2020") {
    ss_bills$bill_id[ss_bills$bill_id == "HB20002"] <- "HB0002"
    ss_bills$bill_id[ss_bills$bill_id == "SB10001"] <- "SB0001"
  }
  if (term == "2021_2022") {
    ss_bills$bill_id[ss_bills$bill_id == "SB30003"] <- "SB0003"
    ss_bills$bill_id[ss_bills$bill_id == "SB40004"] <- "SB0004"
    ss_bills$bill_id[ss_bills$bill_id == "SB60006"] <- "SB0006"
    ss_bills$bill_id[ss_bills$bill_id == "HB90009"] <- "HB0009"
  }

  ss_bills
}

#' Get genuinely missing SS bills
#'
#' Faithful port of old script lines 235-244.
#' Checks which SS bills are missing from bill_details after
#' filtering, excluding committee-titled bills.
#'
#' @param ss_filtered SS bills filtered to term and valid types
#' @param bill_details Filtered bill details
#' @param all_bill_details Unfiltered bill details
#' @return Dataframe of genuinely missing SS bills
get_missing_ss_bills <- function(ss_filtered, bill_details,
                                 all_bill_details) {
  missing <- ss_filtered %>%
    anti_join(bill_details, by = c("bill_id", "term")) %>%
    left_join(
      all_bill_details %>% select("bill_id", "term", "title"),
      by = c("bill_id", "term")
    ) %>%
    mutate(
      bill_type = toupper(gsub("[0-9].+|[0-9]+", "", .data$bill_id))
    ) %>%
    filter(.data$bill_type %in% mo_config$bill_types) %>%
    select(-"bill_type") %>%
    filter(!grepl("committee", .data$title, ignore.case = TRUE))

  missing
}

#' Post-evaluate bill hook: Veto override detection
#'
#' Faithful port of old script lines 465-470.
#' If law == 0, check if BOTH chambers have override actions.
#'
#' @param bill_stages Dataframe row with bill achievement
#' @param bill_row Original bill details row
#' @param bill_history Bill history for this bill
#' @return Modified bill_stages
# nolint start: line_length_linter
post_evaluate_bill <- function(bill_stages, bill_row, bill_history) {
  if (nrow(bill_history) > 0 && bill_stages$law == 0) {
    h_override <- any(grepl(
      "house votes to override veto|h adopt.+ override|motion to override.+ h adopted",
      tolower(bill_history$action)
    ))
    s_override <- any(grepl(
      "senate votes to override veto|s adopt.+ override|motion to override.+ s adopted",
      tolower(bill_history$action)
    ))
    if (h_override && s_override) {
      bill_stages$passed_chamber <- 1
      bill_stages$law <- 1
    }
  }
  bill_stages
}
# nolint end: line_length_linter

#' Derive unique sponsors from bills
#'
#' Faithful port of old script lines 531-558.
#' MO has no cosponsorship data.
#'
#' @param bills Dataframe of bills with achievement columns
#' @param term Term string
#' @return Dataframe of unique sponsors with aggregate stats
derive_unique_sponsors <- function(bills, term) {
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

  # No cosponsorship data for MO
  all_sponsors$num_cosponsored_bills <- NA

  all_sponsors
}

#' Clean sponsor names for matching
#'
#' Faithful port of old script lines 553-611.
#' For MO, match_name_chamber uses LES_sponsor with nicknames
#' stripped out, plus chamber initial.
#'
#' Term-specific last_name overrides are from old script
#' lines 561-600 but only apply to old terms. Included
#' for completeness if running historical terms.
#'
#' @param all_sponsors Dataframe of unique sponsors
#' @param term Term string
#' @return Dataframe with match_name_chamber column added
# nolint start: line_length_linter
clean_sponsor_names <- function(all_sponsors, term) {
  # Term-specific last_name overrides (old script lines 561-600)
  # Only apply to historical terms; none needed for 2023_2024
  if (term == "1995_1996") {
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "murray, connie", "wible, connie", .data$LES_sponsor))
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "edwards-pavia, marilyn", "edwards, marilyn", .data$LES_sponsor))
  }
  if (term == "1997_1998") {
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "edwards-pavia, marilyn", "edwards, marilyn", .data$LES_sponsor))
  }
  if (term %in% c("1997_1998", "1999_2000", "2001_2002")) {
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "merideth iii, denny j.", "merideth, denny j.", .data$LES_sponsor))
  }
  if (term == "2001_2002") {
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "baker, lana ladd", "stokan, lana ladd", .data$LES_sponsor))
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "brooks, sharon sanders", "sandersbrooks, sharon sanders", .data$LES_sponsor))
  }
  if (term %in% c("2003_2004")) {
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "brooks, sharon sanders", "sandersbrooks, sharon sanders", .data$LES_sponsor))
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "jones, robin wright", "wrightjones, robin wright", .data$LES_sponsor))
  }
  if (term %in% c("2001_2002", "2003_2004", "2005_2006", "2007_2008")) {
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "st. onge, neal c.", "saintonge, neal c.", .data$LES_sponsor))
  }
  if (term == "2011_2012") {
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "hughes iv, leonard", "hughes, leonard", .data$LES_sponsor))
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "keeney taylor, shelley", "keeney, shelley", .data$LES_sponsor))
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "walton gray, rochelle", "gray, rochelle", .data$LES_sponsor))
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "mccann beatty, gail", "beatty, gail", .data$LES_sponsor))
  }
  if (term %in% c("2013_2014", "2015_2016")) {
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "walton gray, rochelle", "gray, rochelle", .data$LES_sponsor))
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "mccann beatty, gail", "beatty, gail", .data$LES_sponsor))
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "pierson sr., tommie", "pierson, tommie", .data$LES_sponsor))
  }
  if (term == "2017_2018") {
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "mccann beatty, gail", "beatty, gail", .data$LES_sponsor))
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "franks jr., bruce", "franks, bruce", .data$LES_sponsor))
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "baringer, donna", "mcbaringer, donna", .data$LES_sponsor))
    all_sponsors <- all_sponsors %>%
      mutate(LES_sponsor = ifelse(.data$LES_sponsor == "pierson jr., tommie", "pierson, tommie", .data$LES_sponsor))
  }

  # Create match_name_chamber (old script line 610)
  # Strip nickname quotes from LES_sponsor before matching
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
# nolint end: line_length_linter

#' Adjust legiscan data for matching
#'
#' Faithful port of old script lines 613-661.
#' MO legiscan: Senate match on last_name only;
#' House match on "last_name, first_name".
#'
#' @param legiscan Dataframe of legiscan legislator records
#' @param term Term string
#' @return Adjusted legiscan dataframe with match_name_chamber column
# nolint start: line_length_linter
adjust_legiscan_data <- function(legiscan, term) {
  # Filter to non-committee members
  legiscan <- legiscan %>%
    filter(.data$committee_id == 0)

  # Term-specific legiscan adjustments (old script lines 621-652)
  if (term == "2019_2020") {
    legiscan <- legiscan %>%
      mutate(district = case_when(
        .data$people_id == 18097 ~ "HD-092",
        .data$people_id == 7521 ~ "HD-086",
        .data$people_id == 20706 ~ "HD-155",
        .data$people_id == 15708 ~ "HD-115",
        .data$people_id == 18827 ~ "HD-025",
        .data$people_id == 19466 ~ "HD-023",
        .data$people_id == 15974 ~ "HD-148",
        TRUE ~ .data$district
      ))
  }
  if (term == "2021_2022") {
    legiscan <- legiscan %>%
      filter(
        !(.data$people_id == 22922 & .data$district == "HD-046"),
        !(.data$people_id == 21760 & .data$district == "HD-106"),
        !(.data$people_id == 22390 & .data$district == "HD-143"),
        !(.data$people_id == 22397 & .data$district == "HD-061"),
        !(.data$people_id == 21759 & .data$district == "HD-126"),
        !(.data$people_id == 22085 & .data$district == "HD-091"),
        !(.data$people_id == 21764 & .data$district == "HD-008"),
        !(.data$people_id == 22399 & .data$district == "HD-123"),
        !(.data$people_id == 22391 & .data$district == "HD-120"),
        !(.data$people_id == 22404 & .data$district == "HD-102"),
        !(.data$people_id == 18243 & .data$district == "HD-077"),
        .data$name != "Dean VanSchoiack"
      )
  }
  if (term == "2023_2024") {
    # Chamber switchers: remove stale Rep entries with SD districts
    # Rusty Black (19250), Mary Coleman (19956), Steven Roberts (18243)
    legiscan <- legiscan %>%
      filter(
        !(.data$people_id == 19250 & .data$role == "Rep"),
        !(.data$people_id == 19956 & .data$role == "Rep"),
        !(.data$people_id == 18243 & .data$role == "Rep")
      )
  }

  # Create match_name:
  # Senate unique last_name: last_name only (sponsor data is just last name)
  # Senate duplicate last_name: "first_name last_name" (sponsor data adds
  #   first name when disambiguation needed, space-separated)
  # House: "last_name, first_name" (sponsor data is "last, first" format)
  legiscan %>%
    group_by(.data$last_name, .data$role) %>%
    mutate(n = n()) %>%
    ungroup() %>%
    mutate(
      match_name = case_when(
        substr(.data$district, 1, 1) == "S" & .data$n >= 2 ~
          paste(.data$first_name, .data$last_name),
        substr(.data$district, 1, 1) == "S" ~
          .data$last_name,
        TRUE ~
          paste(.data$last_name, .data$first_name, sep = ", ")
      ),
      match_name_chamber = tolower(paste(
        .data$match_name,
        substr(.data$district, 1, 1),
        sep = "-"
      ))
    )
}
# nolint end: line_length_linter

#' Reconcile legiscan with sponsors
#'
#' Faithful port of old script lines 669-706.
#' Term-specific custom_match for inexact_join.
#'
#' @param sponsors Dataframe of unique sponsors
#' @param legiscan Adjusted legiscan dataframe
#' @param term Term string
#' @return Joined dataframe of sponsors matched to legiscan records
# nolint start: line_length_linter
reconcile_legiscan_with_sponsors <- function(sponsors, legiscan, term) {
  if (term == "2019_2020") {
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "fitzpatrick, scott-h" = NA_character_,
        "haahr, elijah-h" = NA_character_,
        "walker, nathan-h" = NA_character_,
        "walker, cora-h" = "walker, cora faith-h",
        "coleman, mary-h" = "coleman, mary elizabeth-h"
      )
    )
  } else if (term == "2021_2022") {
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "turnbaugh, annette-h" = NA_character_,
        "vescovo, rob-h" = NA_character_,
        "coleman, mary-h" = "coleman, mary elizabeth-h",
        "smith, david-h" = "smith, david tyson-h"
      )
    )
  } else if (term == "2023_2024") {
    inexact::inexact_join(
      x = legiscan,
      y = sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        # House Smiths: David Smith uses middle name in sponsor data
        "smith, david-h" = "smith, david tyson-h",
        # Suppress spurious fuzzy matches for legiscan-only entries
        # (no bills sponsored — resigned, speaker, etc.)
        "atchison, darrell-h" = NA_character_,
        "waller, ken-h" = NA_character_,
        "vescovo, rob-h" = NA_character_,
        "plocher, dean-h" = NA_character_
      )
    )
  } else {
    # Default for other terms
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

# Export config and functions
list(
  bill_types = mo_config$bill_types,
  get_step_terms = get_step_terms,
  load_bill_files = load_bill_files,
  preprocess_raw_data = preprocess_raw_data,
  clean_bill_details = clean_bill_details,
  clean_bill_history = clean_bill_history,
  transform_ss_bills = transform_ss_bills,
  get_missing_ss_bills = get_missing_ss_bills,
  post_evaluate_bill = post_evaluate_bill,
  derive_unique_sponsors = derive_unique_sponsors,
  clean_sponsor_names = clean_sponsor_names,
  adjust_legiscan_data = adjust_legiscan_data,
  reconcile_legiscan_with_sponsors = reconcile_legiscan_with_sponsors,
  prepare_bills_for_les = prepare_bills_for_les
)
