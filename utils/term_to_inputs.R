# Same session-year rules as the Python CLI; no package dependencies.
rules <- read.csv(file.path(Sys.getenv("SLES_REPO_ROOT"),
                            "utils/term_rules.csv"), stringsAsFactors = FALSE)

term_years <- function(state, term) {
  state <- toupper(state)
  if (!state %in% state.abb) {
    stop(sprintf("Unknown state: %s", state), call. = FALSE)
  }
  rule <- rules[rules$state == state, ]
  if (nrow(rule) == 0) rule <- rules[rules$state == "default", ]
  if (length(term) != 1 || is.na(term) ||
      !grepl("^[0-9]{4}_[0-9]{4}$", term)) {
    stop(sprintf("%s term must be YYYY_YYYY session years, e.g. %s",
                 state, rule$example), call. = FALSE)
  }
  years <- as.integer(strsplit(term, "_")[[1]])
  start <- years[1]
  end <- years[2]
  if (state == "AL" && start %% 4 == 2 && end == start + 4) {
    stop(sprintf(paste("Alabama terms use session years. For the legislature",
                       "elected in %s, use %s_%s."),
                 start, start + 1, end), call. = FALSE)
  }
  if (start < 1 || end - start + 1 != rule$years ||
      start %% rule$start_modulus != rule$start_remainder) {
    stop(sprintf(paste("%s requires an aligned %s-year session window,",
                       "e.g. %s; got %s"),
                 state, rule$years, rule$example, term), call. = FALSE)
  }
  seq.int(start, end)
}

ss_files <- function(directory, state, term) {
  years <- term_years(state, term)
  file.path(directory, sprintf("%s_SS_Bills_%s.csv", toupper(state), years))
}

roster_sessions <- function(sessions, state, term) {
  years <- term_years(state, term)
  sessions[substr(sessions, 1, 4) %in% as.character(years)]
}

list(term_years = term_years, ss_files = ss_files,
     roster_sessions = roster_sessions)
