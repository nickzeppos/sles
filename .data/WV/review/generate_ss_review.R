# Generate WV SS Session Resolution Review CSV
# Identifies SS bills with ambiguous session assignment
# (bill_id appears in multiple sessions within the same year)

library(dplyr)
library(stringr)
library(glue)

state <- "WV"
term <- "2023_2024"
data_root <- normalizePath(file.path(getwd(), ".data", state))

# Load bill details for both years
bd1 <- read.csv(file.path(data_root, "bill", paste0(state, "_Bill_Details_2023.csv")),
                 stringsAsFactors = FALSE)
bd2 <- read.csv(file.path(data_root, "bill", paste0(state, "_Bill_Details_2024.csv")),
                 stringsAsFactors = FALSE)
bd <- bind_rows(bd1, bd2) %>% distinct()

# Recode session_type and create session
bd$session_type <- dplyr::recode(bd$session_type,
  "1x" = "SS1", "2x" = "SS2", "3x" = "SS3",
  "4x" = "SS4", "5x" = "SS5", "6x" = "SS6", "7x" = "SS7")
bd$session <- paste0(bd$session_year, "-", bd$session_type)
bd <- rename(bd, bill_id = bill_number)

# Load SS bills for both years
ss1 <- read.csv(file.path(data_root, "ss", paste0(state, "_SS_Bills_2023.csv")),
                 stringsAsFactors = FALSE)
ss2 <- read.csv(file.path(data_root, "ss", paste0(state, "_SS_Bills_2024.csv")),
                 stringsAsFactors = FALSE)
ss <- bind_rows(ss1, ss2) %>%
  filter(State == state) %>%
  rename(bill_id = Bill.No) %>%
  mutate(
    Date = gsub("Sept", "Sep", Date),
    date = as.Date(gsub("\\.", "", Date), "%B %d, %Y"),
    year = as.integer(format(date, "%Y")),
    bill_id = toupper(bill_id),
    bill_id = gsub(" ", "", bill_id),
    bill_id = paste0(
      gsub("[0-9].*", "", bill_id),
      str_pad(gsub("^[A-Z]+", "", bill_id), 4, pad = "0")
    ),
    SS = 1
  ) %>%
  distinct(bill_id, year, Title, .keep_all = TRUE)

# Filter to valid bill types (HB, SB)
ss <- ss %>%
  mutate(bill_type = toupper(gsub("[0-9].+|[0-9]+", "", bill_id))) %>%
  filter(bill_type %in% c("HB", "SB")) %>%
  select(-bill_type)

cat(glue("Total SS bills (HB/SB): {nrow(ss)}"), "\n")

# For each SS bill, determine the default session assignment
# SB >= 2001 -> SS2, SB >= 1001 -> SS1, else RS
ss <- ss %>%
  mutate(
    bill_num = as.integer(gsub("^[A-Z]+", "", bill_id)),
    bill_prefix = gsub("[0-9]+", "", bill_id),
    default_session_type = case_when(
      bill_prefix == "SB" & bill_num >= 2001 ~ "SS2",
      bill_prefix == "SB" & bill_num >= 1001 ~ "SS1",
      TRUE ~ "RS"
    ),
    default_session = paste0(year, "-", default_session_type)
  )

# For each SS bill, find which sessions it appears in within its PVS year
review_rows <- list()
for (i in seq_len(nrow(ss))) {
  bid <- ss$bill_id[i]
  yr <- ss$year[i]
  pvs_title <- ss$Title[i]

  # Find all sessions this bill_id appears in for this year
  candidate_sessions <- bd %>%
    filter(bill_id == bid, session_year == yr) %>%
    distinct(session, summary) %>%
    arrange(session)

  if (nrow(candidate_sessions) <= 1) {
    # Unambiguous: 0 or 1 session match - skip review
    next
  }

  # Ambiguous: bill_id appears in multiple sessions for same year
  for (j in seq_len(nrow(candidate_sessions))) {
    review_rows[[length(review_rows) + 1]] <- data.frame(
      bill_id = bid,
      pvs_title = pvs_title,
      pvs_year = yr,
      candidate_session = candidate_sessions$session[j],
      candidate_summary = candidate_sessions$summary[j],
      match = "",
      stringsAsFactors = FALSE
    )
  }
}

if (length(review_rows) > 0) {
  review_df <- bind_rows(review_rows) %>%
    arrange(bill_id, pvs_year, candidate_session)

  out_path <- file.path(data_root, "review", "WV_SS_session_resolution.csv")
  write.csv(review_df, out_path, row.names = FALSE)

  cat(glue("Wrote {nrow(review_df)} rows to {out_path}"), "\n")
  cat(glue("Ambiguous bills: {length(unique(review_df$bill_id))}"), "\n\n")

  # Print summary
  cat("Ambiguous SS bills:\n")
  for (bid in unique(review_df$bill_id)) {
    sub <- review_df[review_df$bill_id == bid, ]
    cat(glue("  {bid} (PVS year {sub$pvs_year[1]}):"), "\n")
    for (k in seq_len(nrow(sub))) {
      cat(glue("    {sub$candidate_session[k]}: {substr(sub$candidate_summary[k], 1, 60)}..."), "\n")
    }
  }
} else {
  cat("No ambiguous SS bills found!\n")
  # Still create an empty review CSV with headers
  review_df <- data.frame(
    bill_id = character(0), pvs_title = character(0),
    pvs_year = integer(0), candidate_session = character(0),
    candidate_summary = character(0), match = character(0),
    stringsAsFactors = FALSE
  )
  out_path <- file.path(data_root, "review", "WV_SS_session_resolution.csv")
  write.csv(review_df, out_path, row.names = FALSE)
  cat(glue("Wrote empty review CSV to {out_path}"), "\n")
}
