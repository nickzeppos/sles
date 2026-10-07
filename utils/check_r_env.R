# Check the project library without activating renv or installing packages.
ready <- tryCatch({
  project <- getwd()
  roots <- unique(c(
    file.path(project, "renv", "library"),
    Sys.getenv("RENV_PATHS_LIBRARY"),
    Sys.getenv("RENV_PATHS_LIBRARY_ROOT")
  ))
  roots <- roots[nzchar(roots) & dir.exists(roots)]
  descriptions <- unlist(lapply(roots, list.files,
    pattern = "^DESCRIPTION$", recursive = TRUE, full.names = TRUE
  ))
  renv_dirs <- dirname(descriptions[basename(dirname(descriptions)) == "renv"])
  libraries <- unique(c(dirname(renv_dirs), .libPaths()))
  loadNamespace("renv", lib.loc = libraries)
  .libPaths(c(renv::paths$library(project = project), .Library))
  status <- renv::status(
    project = project, library = renv::paths$library(project = project),
    sources = FALSE
  )
  isTRUE(status$synchronized)
}, error = function(e) {
  message(conditionMessage(e))
  FALSE
})
quit(status = if (ready) 0L else 1L)
