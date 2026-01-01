# Bill History Evaluation Utilities
# Functions for evaluating legislative achievement from bill history

# Load shared utilities
repo_root <- Sys.getenv("SLES_REPO_ROOT")
if (repo_root == "") {
  repo_root <- normalizePath(file.path(getwd(), "../.."))
}
strings <- source(file.path(repo_root, "utils/strings.R"),
                  local = TRUE)$value

# Extract functions
collapse_on_pipe <- strings$collapse_on_pipe

#' Get chamber of introduction from bill ID
#'
#' Determines the chamber where a bill was introduced based on bill ID format.
#'
#' @param bill_id Bill identifier (e.g., "SB0123", "AB0456")
#' @return Vector of [init_chamber, other_chamber]
get_chamber_of_introduction <- function(bill_id) {
  if (substring(tolower(bill_id), 1, 1) %in% c("s", "h", "a")) {
    # If bill ID starts with s, h, or a
    # Choose chamber: s => Senate, h/a => House
    init_chamber <- ifelse(substring(tolower(bill_id), 1, 1) == "s",
                           "Senate", "House")
  } else if (grepl("SB|HB|AB", bill_id)) {
    # For states with year-prefixed bills (e.g., 1997-SB-0123)
    # Choose chamber: SB => Senate, HB/AB => House
    init_chamber <- ifelse(grepl("SB", bill_id), "Senate", "House")
  }
  # Other chamber is opposite of intro chamber
  other_chamber <- ifelse(init_chamber == "House", "Senate", "House")

  c(init_chamber, other_chamber)
}

#' Evaluate bill history to determine legislative achievement
#'
#' Analyzes bill history actions to determine what stages a bill achieved:
#' - introduced (always 1)
#' - action_in_comm (action in committee)
#' - action_beyond_comm (action beyond committee)
#' - passed_chamber (passed chamber of introduction)
#' - law (became law)
#'
#' @param bill_history Dataframe of bill history actions
#' @param this_id Bill ID
#' @param this_term Term string
#' @param this_session Session string
#' @param this_sponsor Sponsor name
#' @param state State postal code
#' @param step_terms List of step_terms from state config (aic, abc, pass,
#'   law)
#' @param ignore_chamber_switch Logical, ignore chamber switches
#' @param nebraska Logical, Nebraska unicameral flag
#' @param add_chamb Additional chamber to include
#' @return Dataframe with one row containing bill achievement indicators
evaluate_bill_history <- function(
    bill_history, this_id, this_term, this_session, this_sponsor, state,
    step_terms,
    ignore_chamber_switch = FALSE, nebraska = FALSE, add_chamb = NULL) {

  # Extract step terms from config
  aic_terms <- step_terms$aic
  abc_terms <- step_terms$abc
  pass_terms <- step_terms$pass
  law_terms <- step_terms$law

  # Derive chambers
  chambers <- get_chamber_of_introduction(this_id)
  init_chamber <- chambers[1]
  other_chamber <- chambers[2]

  # Clean bill history
  bill_history$action <- tolower(bill_history$action)
  bill_history <- bill_history %>% arrange(.data$order)

  if (nrow(bill_history) == 0) {
    # If no bill history, steps are 0
    aic <- abc <- pc <- law <- 0
  } else {
    # Get proper subset of actions based on chamber of introduction and flags
    if (ignore_chamber_switch == TRUE) {
      if (!is.null(add_chamb)) {
        init_chamber <- append(init_chamber, add_chamb)
      }
      chamber_history <- filter(bill_history, .data$chamber %in% init_chamber)
    } else if (nebraska == TRUE) {
      chamber_history <- bill_history
    } else {
      in_chamber <- which(bill_history$chamber == init_chamber)
      not_in_chamber <- which(bill_history$chamber == other_chamber)

      chamber_switch <- not_in_chamber[which(not_in_chamber > min(in_chamber))]
      if (length(chamber_switch) > 0) {
        chamber_history <- bill_history[bill_history$order < min(
          chamber_switch
        ), ]
      } else {
        chamber_history <- bill_history
      }
    }

    # Compute step achievement
    aic <- max(grepl(collapse_on_pipe(aic_terms), chamber_history$action))
    abc <- max(grepl(collapse_on_pipe(abc_terms), chamber_history$action))
    pc <- max(grepl(collapse_on_pipe(pass_terms), chamber_history$action))
    law <- max(grepl(collapse_on_pipe(law_terms), bill_history$action))

    # Backfill rule: only aic can be 0 if any succeeding step(s) is (are) 1
    if (law == 1) {
      abc <- pc <- 1
    } else if (pc == 1) {
      abc <- 1
    }
  }

  # Return dataframe like original function
  data.frame(
    bill_id = this_id,
    term = this_term,
    session = this_session,
    LES_sponsor = this_sponsor,
    state = state,
    introduced = 1,
    action_in_comm = aic,
    action_beyond_comm = abc,
    passed_chamber = pc,
    law = law
  )
}

# Export functions
list(
  evaluate_bill_history = evaluate_bill_history,
  get_chamber_of_introduction = get_chamber_of_introduction
)
