# Library management utilities

# Libraries used across the SLES project
libs <- c(
  "dplyr",
  "glue",
  "readr",
  "stringr",
  "tidyr",
  "purrr",
  "inexact"
)

# Load required libraries
require_libs <- function(suppress_warnings = TRUE) {
  packages <- c(
    "dplyr", "glue", "readr", "stringr", "tidyr", "purrr", "inexact"
  )

  if (suppress_warnings) {
    suppressPackageStartupMessages(
      invisible(
        lapply(packages, library, character.only = TRUE)
      )
    )
  } else {
    lapply(packages, library, character.only = TRUE)
  }
  invisible(NULL)
}

# Export
list(
  libs = libs,
  require_libs = require_libs
)
