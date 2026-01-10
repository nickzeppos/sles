# copy over old sponsor derivation logic

library(dplyr)

derive_sponsor_old <- function(original_sponsor, requested_by) {
  # Lowercase inputs
  original_sponsor <- tolower(original_sponsor)
  requested_by <- tolower(requested_by)

  ### LES Sponsor Var
  relevant_requestors <- ifelse(
    !grepl("^(representative|senator)", original_sponsor) &
      grepl("^(representative|senator)", requested_by),
    sub("^(.*?);.*", "\\1", requested_by),
    NA
  )


  # if rep requests on behalf of another rep, that second member is sponsor.
  # if member requests on behalf of anyone else, they are sponsor
  relevant_requestors <- ifelse(
    grepl("on behalf of representative", relevant_requestors),
    gsub("^.*on behalf of\\s*", "", relevant_requestors),
    gsub(" on behalf of.*", "", relevant_requestors)
  )

  LES_sponsor <- trimws(case_when(
    # take the first listed original sponsor
    grepl("^(representative|senator)", original_sponsor) ~
      sub("^(.*?);.*", "\\1", original_sponsor),
    # then, see if a member requested it
    grepl("^(representative|senator)", requested_by) ~
      sub("^(.*?);.*", "\\1", relevant_requestors)
  ))

  # if multiple requestors, take the first one
  LES_sponsor <- ifelse(
    grepl("^representatives ([^,]+) and ([^,]+)$", LES_sponsor),
    sub("^representatives ([^,]+) and ([^,]+)$", "representative \\1", LES_sponsor),
    gsub("representatives ([^,]+).*", "representative \\1", LES_sponsor)
  )
  LES_sponsor <- ifelse(
    grepl("^senators ([^,]+) and ([^,]+)$", LES_sponsor),
    sub("^senators ([^,]+) and ([^,]+)$", "senator \\1", LES_sponsor),
    gsub("senators ([^,]+).*", "senator \\1", LES_sponsor)
  )

  # if there is a cross-chamber requestor, remove
  # (skipping this check since we don't have bill_type in this test)

  # now fix the relevant requestors text
  relevant_requestors <- gsub(" and ", "; ", relevant_requestors)
  relevant_requestors <- ifelse(
    grepl("^representatives", relevant_requestors),
    gsub(", ", "; ", relevant_requestors),
    relevant_requestors
  )

  # remove title
  relevant_requestors <- gsub("representative ", "", gsub("senator ", "", relevant_requestors))
  relevant_requestors <- gsub("representatives ", "", gsub("senators ", "", relevant_requestors))
  LES_sponsor <- gsub("representative ", "", gsub("senator ", "", LES_sponsor))
  LES_sponsor <- gsub("representatives ", "", gsub("senators ", "", LES_sponsor))

  return(LES_sponsor)
}

# Test cases
cat("======\n\n")


case4_orig <- "Committee on Local Government"
case4_req <- "Representative Blex on behalf of Representative Bryce and the City of Independence"
result4 <- derive_sponsor_old(case4_orig, case4_req)

cat("original_sponsor:", case4_orig, "\n")
cat("requested_by:", case4_req, "\n")
cat("LES_sponsor:", result4, "\n")

cat("======\n\n")

case5_orig <- "Committee on Energy, Utilities and Telecommunications"
case5_req <- "Representative Delperdang on behalf of Representative Hoffman and Representative Carmichael"
result5 <- derive_sponsor_old(case5_orig, case5_req)
cat("original_sponsor:", case5_orig, "\n")
cat("requested_by:", case5_req, "\n")
cat("LES_sponsor:", result5, "\n")
