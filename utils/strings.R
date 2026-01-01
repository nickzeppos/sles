# String manipulation utilities

# Collapse character vector with pipe separator
collapse_on_pipe <- function(cvec) {
  paste(cvec, collapse = "|")
}

# Export
list(
  collapse_on_pipe = collapse_on_pipe
)
