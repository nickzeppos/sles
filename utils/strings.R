# String manipulation utilities

# Collapse character vector with pipe separator
collapse_on_pipe <- function(cvec) {
  paste(cvec, collapse = "|")
}

# Standardize accented characters to ASCII equivalents
# Uses stringi for comprehensive Latin diacritic handling
standardize_accents <- function(names) {
  stringi::stri_trans_general(names, "Latin-ASCII")
}

# nolint start: commented_code_linter
# Legacy implementation (deprecated) - kept for reference
# standardize_accents_legacy <- function(names) {
#   names <- gsub("á", "a", names)  # \u00e1
#   names <- gsub("é", "e", names)  # \u00e9
#   names <- gsub("ó", "o", names)  # \u00f3
#   names <- gsub("í", "i", names)  # \u00ed
#   names <- gsub("ñ", "n", names)  # \u00f1
#   names
# }
# nolint end: commented_code_linter

# Export
list(
  collapse_on_pipe = collapse_on_pipe,
  standardize_accents = standardize_accents
)
