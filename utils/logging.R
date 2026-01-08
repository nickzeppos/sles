# Logging and CLI output utilities

# ASCII color codes
BLUE <- "\033[34m"
GREEN <- "\033[32m"
YELLOW <- "\033[33m"
RED <- "\033[31m"
RESET <- "\033[0m"

# Format timestamp without decimal places
format_timestamp <- function() {
  format(Sys.time(), "%Y-%m-%d %H:%M:%S", usetz = FALSE)
}

# Log message
cli_log <- function(msg) {
  writeLines(sprintf("%s[L] [%s]:%s %s", GREEN, format_timestamp(), RESET, msg))
}

# Warning message
cli_warn <- function(msg) {
  writeLines(sprintf("%s[W] [%s]:%s %s", YELLOW, format_timestamp(), RESET, msg))
}

# Error message
cli_error <- function(msg) {
  writeLines(sprintf("%s[E] [%s]:%s %s", RED, format_timestamp(), RESET, msg))
}

# Prompt for user input
cli_prompt <- function(prompt_msg) {
  writeLines(sprintf("%s[?] [%s]:%s %s", BLUE, format_timestamp(), RESET, prompt_msg))
  response <- readLines("stdin", n = 1)
  response
}

# Convert various data types to tibble
to_tibble <- function(data) {
  if (is.data.frame(data)) {
    tibble::as_tibble(data)
  } else if (is.matrix(data)) {
    tibble::as_tibble(as.data.frame(data))
  } else if (is.vector(data)) {
    tibble::tibble(values = data)
  } else {
    data_type <- typeof(data)
    cli_warn(sprintf("Non-tibblable input: %s", data_type))
    data
  }
}

# Log a data frame/tibble
cli_log_table <- function(df) {
  name <- deparse(substitute(df))
  writeLines(sprintf(
    "\n%s[L] [%s]:%s START DF: %s\n",
    GREEN, Sys.time(), RESET, name
  ))
  print(to_tibble(df))
  writeLines(sprintf(
    "\n%s[L] [%s]:%s END DF: %s\n",
    GREEN, Sys.time(), RESET, name
  ))
}

# Warn with a data frame/tibble
cli_warn_table <- function(df) {
  name <- deparse(substitute(df))
  writeLines(sprintf(
    "\n%s[W] [%s]:%s START DF: %s\n",
    YELLOW, Sys.time(), RESET, name
  ))
  print(to_tibble(df))
  writeLines(sprintf(
    "\n%s[W] [%s]:%s END DF: %s\n",
    YELLOW, Sys.time(), RESET, name
  ))
}

# Error with a data frame/tibble
cli_error_table <- function(df) {
  name <- deparse(substitute(df))
  writeLines(sprintf(
    "\n%s[E] [%s]:%s START DF: %s\n",
    RED, Sys.time(), RESET, name
  ))
  print(to_tibble(df))
  writeLines(sprintf(
    "\n%s[E] [%s]:%s END DF: %s\n",
    RED, Sys.time(), RESET, name
  ))
}

# Export
list(
  cli_log = cli_log,
  cli_warn = cli_warn,
  cli_error = cli_error,
  cli_prompt = cli_prompt,
  to_tibble = to_tibble,
  cli_log_table = cli_log_table,
  cli_warn_table = cli_warn_table,
  cli_error_table = cli_error_table
)
