##########################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** WISCONSIN *** BY SESSION
##########################################################################

###################################
## SPECIAL SESSIONS:
## ---- Separate, but labeled in session variable -- Not many bills per special --> CODE AS S&S?
## MEMBER LISTS:
## https://docs.legis.wisconsin.gov/2015/legislators/assembly
## PROCESS:
## -- https://legis.wisconsin.gov/assembly/acc/media/1106/howabillbecomeslaw.pdf
## -- Glossary: http://legis.wisconsin.gov/about/glossary/
## Sponsorship/Authorship
## -- THERE are multisonsored bills -- see, e.g., https://docs.legis.wisconsin.gov/1995/related/author_index/assembly/A_Turner_Robert
## -----> He has no solo introduced bills but cointroduces a few
###########################

rm(list = ls())
options(stringsAsFactors = FALSE, scipen = 999)

library(tidyr)
library(dplyr)
library(stringr)
library(ggplot2)
library(humaniformat)
library(purrr)
library(readxl)
library(glue)
library(readr)
library(tibble)
library(foreach)
library(inexact)

this_state <- "WI"
keep_types <- c("AB", "SB")

#### Output Directory
# dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd(glue("../../LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Data Directory

data_files <- list.files(glue("../../../State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl("Bill_Details", data_files)]
sessions <- gsub(".+Bill_Details_|.csv", "", bill_files)
rm(data_files, bill_files)

### Terms/Sessions and Filespaths
terms <- 2023
t <- terms
t_plus_one <- t + 1
t_yrs <- as.numeric(terms)
t_yrs <- as.character(glue("{t_yrs}_{t_yrs + 1}"))
t_sessions <- sessions[startsWith(sessions, as.character(t)) | startsWith(sessions, as.character(t + 1))]

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- bind_rows(
  read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t}.csv")),
  read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t_plus_one}.csv"), colClasses = c("character"))
)

SS_bills <- SS_bills %>%
  filter(State == this_state) %>%
  rename(bill_id = Bill.No) %>%
  mutate(
    Date = gsub("Sept", "Sep", Date),
    date = as.Date(gsub("\\.", "", Date), "%B %d, %Y"),
    year = as.integer(format(date, "%Y")),
    term = ifelse(year %% 2 == 1, paste0(year, "_", year + 1), paste0(year - 1, "_", year)),
    bill_id = toupper(bill_id),
    bill_id = gsub(" ", "", bill_id),
    bill_id = paste0(gsub("[0-9].+", "", bill_id), str_pad(gsub("^[A-Z]+", "", bill_id), 4, pad = "0")),
    SS = 1
  ) %>%
  select(state = State, term, year, bill_id, everything())



###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[2]



### Skip Previously Estimated
if (glue("{this_state}_LES_{t_yrs}.csv") %in% list.files(".")) {
  print(glue(". \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n."))
  next
}

###### SESSION IN PROGRESS
print(glue("\n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} session! ~~~~~~~~~~~~~~ "))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t}.csv")
bills <- read.csv(bill_path)

### Clean Term/Session Variables
bills$term <- t_yrs
bills$session <- bills$session_type
bills <- select(bills, -session_type)

### Check for duplicates
if (nrow(bills) != nrow(distinct(bills))) {
  cat(" \n ~~~> DUPLICATE BILLS \n\n .")
  break
}

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_number) %>%
  mutate(bill_id = toupper(bill_id))

############### Drop Resolutions, Messages, Communications, Reports
bills <- mutate(bills, bill_type = toupper(gsub("[0-9].+", "", bill_id)))
table(bills$bill_type)
all_bills <- bills
bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)

############### Standardize Sponsors
bills$LES_sponsor <- gsub("\\.$|\\. +$", "", tolower(bills$primary_sponsor))
sort(table(bills$LES_sponsor))



### Split Bills with Multiple INtroducing Sponsors
### *** NOTE: THese are NOT alphabetized so assuming order signifies primary introducer vs not
bills$LES_sponsor <- gsub(" and .+", "", bills$LES_sponsor)

######### Bills By Request
if (any(grepl("by request", bills$LES_sponsor))) {
  print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK")
  break
  # print(glue("-----> KEEPING {nrow(filter(bills, grepl('by request', LES_sponsor)))} bill(s) introduced BY REQUEST"))
  # bills$LES_sponsor <- str_trim(gsub('\\(by request\\)', '', bills$LES_sponsor))
}

#### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
bills$LES_sponsor <- gsub("á", "a", bills$LES_sponsor)
bills$LES_sponsor <- gsub("é", "e", bills$LES_sponsor)
bills$LES_sponsor <- gsub("ó", "o", bills$LES_sponsor)
bills$LES_sponsor <- gsub("í", "i", bills$LES_sponsor)


#### Name Fixes
if (any(bills$LES_sponsor == "molepske jr" | bills$LES_sponsor == "molepske jr.")) {
  bills[bills$LES_sponsor %in% c("molepske jr", "molepske jr."), ]$LES_sponsor <- "molepske"
}
if (t_yrs == "2013_2014") {
  bills[bills$LES_sponsor == "cullen", ]$LES_sponsor <- "t. cullen"
}
if (t_yrs == "2017_2018") {
  bills[bills$LES_sponsor == "larson", ]$LES_sponsor <- "c. larson"
  bills[bills$LES_sponsor == "fitzgerald", ]$LES_sponsor <- "s. fitzgerald"
}
if (t_yrs == "1997_1998") {
  ### Carol A. Roessler = Carol A. Buettner --> Listed as both in Bill Data, but Buettner in Election Data
  bills[bills$LES_sponsor == "roessler", ]$LES_sponsor <- "buettner"
}
if (t_yrs == "2019_2020") {
  bills[bills$LES_sponsor == "l. myers", ]$LES_sponsor <- "myers"
  bills[bills$LES_sponsor == "b. meyers", ]$LES_sponsor <- "meyers"
}

###################
###### Merge in S&S Bills
###################
# *** For Wisconsin: Bills Carry Over from Regular to Regular Session BUT Bill Numbers RESTART for Specials
# ---> Special Sessions = Few Bills Sponsored by LEGISLATORS (vs Comms), Sessions Dated as MM/YY
# ---> Capping Bill numbers at SESSION MAX and ASSUMING bill is from SS with most bills proposed

if (t_yrs == "2019_2020") {
  SS_bills$bill_id[SS_bills$bill_id == "AB10001"] <- "AB0001"
  SS_bills$bill_id[SS_bills$bill_id == "AB40004"] <- "AB0004"
}
if (t_yrs == "2021_2022") {
  SS_bills$bill_id[SS_bills$bill_id == "SB10001"] <- "SB0001"
  SS_bills$bill_id[SS_bills$bill_id == "AB20002"] <- "AB0002"
  SS_bills$bill_id[SS_bills$bill_id == "SB80008"] <- "SB0008"
  SS_bills$bill_id[SS_bills$bill_id == "AB10001"] <- "AB0001"
  SS_bills$bill_id[SS_bills$bill_id == "AB40004"] <- "AB0004"
  SS_bills$bill_id[SS_bills$bill_id == "AB50005"] <- "AB0005"
}

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, year, Title) %>%
  mutate(SS = 1)

missing_SS_bills <- anti_join(
  SS_bills %>%
    mutate(SS = 1) %>%
    distinct(term, bill_id, SS, year, Title), bills,
  by = c("bill_id", "term")
) %>%
  left_join(all_bills %>% select(bill_id, term, primary_sponsor), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub("[0-9].+|[0-9]+", "", bill_id))) %>%
  filter(bill_type %in% keep_types) %>%
  select(-bill_type) %>%
  filter(!grepl("committee", primary_sponsor, ignore.case = T)) %>%
  arrange(primary_sponsor)
unique(missing_SS_bills$bill_id)



# now check to see if there are duplicate joins

duplicate_SS_bills <- SS_term %>%
  tibble::rownames_to_column() %>%
  left_join(bills,
    by = c("bill_id", "term")
  ) %>%
  group_by(rowname) %>%
  mutate(count = n()) %>%
  ungroup() %>%
  select(count, bill_id, term, session, Title, summary) %>%
  arrange(desc(count), bill_id)

if (nrow(duplicate_SS_bills %>% filter(count > 1)) > 0) {
  print("you have duplicates")
  SS_duplicates_exist <- 1
  break
} else {
  SS_duplicates_exist <- 0
}


# if you have duplicated bills, you have to go into this if statement. otherwise, do the else logic.

if (nrow(duplicate_SS_bills %>% filter(count > 1)) > 0) {
  # now have to remove the duplicated bills that don't correspond
  write.csv(duplicate_SS_bills, glue("../../../State Legislative Data/States/{this_state}/{this_state}_duplicate_SS_bills_{t_yrs}.csv"), row.names = F)
  # edit this file in Excel, create a column called filter, put in the value "remove" if the Title from PVS doesn't match the bill description
  SS_term <- read.csv(glue("../../../State Legislative Data/States/{this_state}/{this_state}_duplicate_SS_bills_{t_yrs}_edited.csv")) %>%
    filter(filter != "remove") %>%
    select(bill_id, term, session) %>%
    mutate(SS = 1) %>%
    distinct()



  bills <- bills %>%
    left_join(SS_term, by = c("bill_id", "term", "session")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS)) %>%
    distinct()
} else {
  orig_row_n <- c(nrow(bills), nrow(SS_term))
  bills2 <- bills %>%
    left_join(SS_term %>% select(-Title) %>% distinct(), by = c("bill_id", "term")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  SS_term2 <- SS_term %>%
    left_join(bills %>% select(bill_id, term, session), by = c("bill_id", "term"))
  if (!identical(c(nrow(bills2), nrow(SS_term2)), orig_row_n)) {
    print("merge failed")
    break
  } else {
    bills <- bills2
    SS_term <- SS_term2
    rm(bills2, SS_term2)
  }
}

### Check Missing
table(bills$SS)
SS_in_bills <- sum(bills$SS)
SS_in_PVS <- nrow(SS_term %>%
  mutate(bill_type = toupper(gsub("[0-9].+|[0-9]+", "", bill_id))) %>%
  filter(bill_type %in% keep_types))

if (SS_in_bills == SS_in_PVS) {
  print("all SS merged properly")
} else {
  print(glue("{SS_in_PVS} S&S bills in original dataset, but {SS_in_bills} S&S in our bills dataset"))

  # stuff in PVS, not in bills
  print(anti_join(SS_term, bills, by = c("bill_id", "term", "session")))

  # PVS bills that are duplicated in bills. not necessarily a problem!
  print(SS_term %>% group_by(bill_id, term) %>%
    mutate(count = n()) %>% filter(count > 1) %>% arrange(bill_id))

  # see if they are equal after removing double-counted bills. this is a crude proxy, you should examine more closely
  all.equal(
    SS_in_bills + nrow(anti_join(SS_term, bills, by = c("bill_id", "term", "session"))) +
      nrow(SS_term %>% group_by(bill_id, term) %>%
        mutate(count = n()) %>% filter(count > 1)) / 2,
    nrow(SS_term)
  )
}

rm(all_bills, missing_SS_bills, duplicate_SS_bills)


####################################################################
############### Code Commemorative
####################################################################

bills <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c("bill_id", "term", "session"))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)


#### DROP Committee Bills
if (any(grepl("committee", bills$LES_sponsor))) {
  print(glue("-----> DROPPING {nrow(filter(bills, grepl('committee', LES_sponsor)))} bill(s) introduced BY COMMITTEE"))
  bills <- filter(bills, !grepl("committee", LES_sponsor))
}

#### Drop Joint Legislative Council
if (any(grepl("legislative council|information policy", bills$LES_sponsor))) {
  print(glue("-----> DROPPING {nrow(filter(bills, grepl('legislative council|information policy', LES_sponsor)))} bill(s) introduced BY LEGISLATIVE COUNCIL/INFO POLICY"))
  bills <- filter(bills, !grepl("legislative council|information policy", LES_sponsor))
}

###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "") %>% View()
if (nrow(filter(bills, LES_sponsor == "")) > 0) {
  print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
  bills <- filter(bills, !(LES_sponsor == ""))
}

####################################################################
############### Code Bill History
####################################################################

bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t}.csv")
bill_hist <- read.csv(bill_hist_path)

######## Standardize BillHist Bill IDs
bill_hist <- rename(bill_hist, bill_id = bill_number) %>%
  mutate(bill_id = toupper(bill_id))

### Clean Term/Session Variables
bill_hist$term <- t_yrs
bill_hist$session <- bill_hist$session_type
bill_hist <- select(bill_hist, -session_type)

### Standardize Chamber Variable
bill_hist$chamber <- recode(bill_hist$chamber, "Asm." = "House", "Sen." = "Senate")

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source("../../Estimate LES/code_billhist_fx.R")

##### Set State-Specific Terms for Identifying Each Stage
# -- IS public hearing.+waived AIC? Per Senate rule 18, appears to be a way to get it out of committee without hearing: http://docs.legis.wisconsin.gov/2013/related/rules/senate/3/18/1m
# -- Executive action taken = committee executive action/debate -- See here, page 32: http://legis.wisconsin.gov/lrb/media/1093/14rb2.pdf
# -- AMendments: CUrrently only catching committee amendments; could add 'amend.+offered by rep|amend.+offered by sen'
# -----> Problem is that sometimes these are in committee stage, sometimes floor stage, and parsing will require an overhaul of the bill_hist fx script + not clear it matters
aic_t <- c(
  "public hearing held", "recommended by.+comm", "amend.+offered.+committee", "estimate received", "executive action taken",
  "placed on calendar [^\\s]+ by committee"
)
abc_t <- c(
  "^report", "placed on cal.+ by rules", "read a second time", "read a third time", "second reading",
  "third reading", "^failed to pass.+gov", "^referred to cal", "rules suspended"
)
# Note: '^referred to cal' needs ^ or else will catch first read
# ---- Failed to pass pursuant to Assembly/Senate Joint Res ZZ appears to just clear bills out at end of session --- Not coding as action
pc_t <- c("^read.+ passed", "^passed", "enrolled") # enrolled here to catch in case missed chamber passage
law_t <- c("approved by the gov", "^published", "wisconsin act [0-9]+")

# filter(bill_hist, grepl('executive action taken', tolower(action))) %>% select(action) %>% distinct()

####################
### Output Matrix
all_bill_stages <- tibble(
  bill_id = character(0),
  term = character(0),
  session = character(0),
  LES_sponsor = character(0),
  introduced = integer(0),
  action_in_comm = integer(0),
  action_beyond_comm = integer(0),
  passed_chamber = integer(0),
  law = integer(0),
  bill_url = character(0)
)

### Adjusting Actions that Create Issues
bill_hist$action <- ifelse(grepl("joint survey committee", tolower(bill_hist$action)), paste0("~Survey Committee~ ", bill_hist$action), bill_hist$action)

### Code History Stages for Each Bill --- Subset Hist File, Code, Save, Repeat
options(warn = 2)
for (i in 1:nrow(bills)) {
  b_id <- bills[i, ]$bill_id
  s_id <- bills[i, ]$session
  b_spon <- bills[i, ]$LES_sponsor
  hist_sub <- filter(bill_hist, bill_id == b_id, session == s_id)
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
  bill_stages$bill_url <- bills[i, ]$bill_url
  all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
  # print(i)
}
options(warn = 1)

### Check Codings
cat("\n")
all_bill_stages %>%
  mutate(chamber = substring(bill_id, 1, 1)) %>%
  group_by(session, chamber) %>%
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>%
  mutate(AIC_pct = round(AIC / N, 2), ABC_pct = round(ABC / N, 2), PASS_pct = round(PASS / N, 2), LAW_pct = round(LAW / N, 2)) %>%
  as.data.frame() %>%
  print()
# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% filter(!grepl('^by resolution|^first read|^prefiled', tolower(action))) %>% View()
# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 1,]$bill_id) %>% View()


read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-2}_{t-1}_Bill_Stage_Codings.csv")) %>%
  mutate(chamber = substring(bill_id, 1, 1)) %>%
  group_by(session, chamber) %>%
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>%
  mutate(AIC_pct = round(AIC / N, 2), ABC_pct = round(ABC / N, 2), PASS_pct = round(PASS / N, 2), LAW_pct = round(LAW / N, 2)) %>%
  as.data.frame() %>%
  filter(!grepl("-S", session)) %>%
  print()

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-4}_{t-3}_Bill_Stage_Codings.csv")) %>%
  mutate(chamber = substring(bill_id, 1, 1)) %>%
  group_by(session, chamber) %>%
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>%
  mutate(AIC_pct = round(AIC / N, 2), ABC_pct = round(ABC / N, 2), PASS_pct = round(PASS / N, 2), LAW_pct = round(LAW / N, 2)) %>%
  as.data.frame() %>%
  filter(!grepl("-S", session)) %>%
  print()

### MERGE to BILL DATA
bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages)))

### MERGE In COMMEMS
all_bill_stages <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(all_bill_stages, ., by = c("bill_id", "term", "session"))

### MERGE In S&S
all_bill_stages <- SS_term %>%
  select(bill_id, term, session, SS) %>%
  left_join(all_bill_stages, ., by = c("bill_id", "term", "session")) %>%
  mutate(SS = ifelse(is.na(SS), 0, SS))

### Adjust Commems if SS == 1
table(all_bill_stages$SS, all_bill_stages$commem)
all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
rm(SS_term)

### Save Stage Info **** MERGE WITH SS **********
write.csv(all_bill_stages, glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv"), row.names = FALSE)

rm(bill_hist_path, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist, aic_t, abc_t, pc_t, law_t)
rm(b_id, s_id, b_spon, bill_hist)

####################################################
############### Identify Unique Legislators via SLER
####################################################

## Import and Clean Sponsors Name to Match
all_sponsors <- bills %>%
  mutate(chamber = ifelse(substring(bill_id, 1, 1) == "A", "H", "S")) %>%
  select(LES_sponsor, chamber, passed_chamber, law) %>%
  mutate(term = t_yrs) %>%
  group_by(LES_sponsor, chamber, term) %>%
  summarize(
    num_sponsored_bills = n(),
    sponsor_pass_rate = sum(passed_chamber) / n(),
    sponsor_law_rate = sum(law) / n()
  ) %>%
  ungroup()

######## Number of Cosponsored Bills
all_sponsors$num_cosponsored_bills <- NA
for (i in 1:nrow(all_sponsors)) {
  c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i, ]$chamber == "H", "A", "S"))
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i, ]$LES_sponsor, tolower(c_sub$cosponsors)))
  ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  # all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
}
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))


if (min(all_sponsors$num_cosponsored_bills) < 0) {
  print("negative cosponsors")
  break
} else {
  print("cosponsors valid")
}

###########
### CLEAN NAMES
############
all_sponsors$last_name <- ifelse(!grepl("\\.", all_sponsors$LES_sponsor), all_sponsors$LES_sponsor, gsub("^[a-z]\\. ", "", all_sponsors$LES_sponsor))
all_sponsors$first_name <- ifelse(grepl("\\.", all_sponsors$LES_sponsor), gsub("\\..+", "", all_sponsors$LES_sponsor), "")
all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update Last Names for Matching
if (t_yrs == "1999_2000") {
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "roessler", "buettner", all_sponsors$last_name)
}
if (t_yrs %in% c("2001_2002")) {
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "starzyk", "kerkman", all_sponsors$last_name)
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "wade", "spillner", all_sponsors$last_name)
}
if (t_yrs %in% c("2003_2004")) {
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "morris", "morris-tatum", all_sponsors$last_name)
}
if (t_yrs %in% c("2009_2010", "2011_2012", "2013_2014")) {
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "bernard schaber", "schaber", all_sponsors$last_name)
}
if (t_yrs %in% c("2013_2014", "2015_2016", "2017_2018")) {
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "pope", "pope-roberts", all_sponsors$last_name)
}
if (t_yrs == "2015_2016") {
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "harris dodd", "harris", all_sponsors$last_name)
}
if (t_yrs == "2017_2018") {
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "felzkowski", "czaja", all_sponsors$last_name)
}





all_sponsors <- all_sponsors %>%
  mutate(
    last_name = gsub(" .+", "", LES_sponsor),
    first_name = ifelse(!grepl(" ", LES_sponsor), NA, gsub(".+ ", "", LES_sponsor))
  ) %>%
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(str_remove_all(LES_sponsor, '"\\s*.*?\\s*"'), substr(chamber, 1, 1), sep = "-"))) %>%
  select(-c(last_name, first_name))

legiscan_sessions <- list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions <- legiscan_sessions[startsWith(legiscan_sessions, as.character(t)) | startsWith(legiscan_sessions, as.character(t + 1))]

legiscan <- foreach(i = legiscan_sessions, .combine = rbind) %do%
  {
    read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
  } %>%
  select(-c(votesmart_id, knowwho_pid, ballotpedia, nickname, suffix)) %>%
  distinct()


if (t_yrs == "2021_2022") {
  legiscan <- bind_rows(
    legiscan,
    legiscan %>% filter(people_id == 15094) %>% mutate(role = "Sen", district = "SD-013")
  ) %>%
    filter(!name %in% c("Melissa Agard", "Nancy Vander Meer"))
}

legiscan_adj <- legiscan %>%
  filter(committee_id == 0) %>%
  group_by(last_name, role) %>%
  mutate(n = n()) %>%
  ungroup() %>%
  mutate(match_name = ifelse(n >= 2, glue("{substr(first_name,1,1)}. {last_name}"), last_name)) %>%
  mutate(match_name_chamber = tolower(paste(match_name, substr(district, 1, 1), sep = "-")))

# inexact::inexact_addin()
# left: legiscan_adj
# right: all_sponsors
# method: osa
# mode: full

if (t_yrs == "2019_2020") {
  all_sponsors2 <-
    # You added custom matches:
    inexact::inexact_join(
      x = legiscan_adj,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "barca-h" = NA_character_
      )
    )
}

if (t_yrs == "2021_2022") {
  all_sponsors2 <- # You added custom matches:
    # You added custom matches:
    inexact::inexact_join(
      x = legiscan_adj,
      y = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "d. hesselbein-s" = NA_character_,
        "m. spreitzer-s" = NA_character_,
        "j. james-s" = NA_character_,
        "r. cabral-guevara-s" = NA_character_,
        "taylor-s" = "l. taylor-s",
        "jagler-h" = NA_character_
      )
    )
}

#### Clean
legis_data <- all_sponsors2 %>%
  rename(data_name = LES_sponsor) %>%
  mutate(
    sponsor = ifelse(!is.na(name), name, str_to_title(match_name)),
    term = t_yrs,
    chamber = substr(district, 1, 1)
  ) %>%
  select(sponsor, data_name, name, klarner_id = people_id, chamber, party, district, term, num_sponsored_bills, num_cosponsored_bills, sponsor_pass_rate, sponsor_law_rate) %>%
  arrange(chamber, sponsor) %>%
  distinct()



# now need to remove zero-LES legislators who never actually served. see documentation file on how this is generated

removal_legislators <- read.csv("../../Estimate LES/Zero_LES_legislators_Coded.csv") %>%
  filter(state == this_state & term == t_yrs & not_actually_in_chamber == T) %>%
  mutate(chamber = substr(chamber, 1, 1))

if (nrow(removal_legislators) > 0) {
  legis_data <- anti_join(legis_data, removal_legislators,
    by = c("klarner_id" = "legiscan_id", "chamber")
  )
}


#######################
##### Estimate Scores + Add in Relatd Variables
#######################

### Check if bills in data without an ID'd sponsor
filter(bills, !(bills$LES_sponsor %in% legis_data$data_name))
bills <- bills %>% # select(bills, -sponsor) %>%
  rename(sponsor = LES_sponsor) %>%
  mutate(chamber = ifelse(substring(bill_id, 1, 1) == "A", "H", "S"))

### Standard LES: Same as Congressional Measure
cat(" \n ------> Estimating LES Scores ")
source("../../Estimate LES/calc_LES_fx.R")

LES <- calc_LES(bills, legis_data, t_yrs, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1, 1, 1, 1, 1))
summ_stats <- LES %>%
  group_by(chamber) %>%
  summarize(mean_LES = mean(LES))

### Need to use this isTRUE business otherwise will sometimes return 1 != 1 -- https://stackoverflow.com/questions/9508518/why-are-these-numbers-not-equal
if (!isTRUE(all.equal(sum(summ_stats$mean_LES), nrow(summ_stats)))) {
  print("----> CHECK LES --- MEAN != 1 ---> BREAK")
  print(summ_stats)
  break
}
rm(summ_stats)
# filter(LES, LES == 0)

#### LES Without Weights + Merge
LES_noWeights <- calc_LES(bills, legis_data, t_yrs, ss_weight = 5, reg_weight = 5, com_weight = 5, stage_weights = c(1, 1, 1, 1, 1))
LES_noWeights <- rename(LES_noWeights, LES_nw = LES) %>% select(1:6, LES_nw)
LES <- left_join(LES, LES_noWeights, by = intersect(colnames(LES), colnames(LES_noWeights)))
rm(LES_noWeights)

### Fix Term Variable
LES <- rename(LES, term = session)

#### Merge Agg Stats back in
LES <- legis_data %>%
  select(sponsor, chamber, party, district, num_sponsored_bills, sponsor_pass_rate, sponsor_law_rate, num_cosponsored_bills) %>%
  mutate(chamber = ifelse(chamber == "H", "House", "Senate")) %>%
  left_join(LES, ., by = c("sponsor", "chamber")) %>%
  select(-klarner_name) %>%
  rename(legiscan_id = klarner_id) %>%
  select(sponsor, data_name, legiscan_id, term, chamber, district, party, LES, everything())


#### If LES == 0 and
LES[LES$LES == 0 & is.na(LES$num_sponsored_bills), c("num_sponsored_bills", "sponsor_pass_rate", "sponsor_law_rate")] <- 0

############### Save LES
write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
rm(LES)


cat(glue(". \n  **************** SESSION {t_yrs} ~~> DONE  ***********************"))
cat("\n __________________________________________________________ \n")


################# ****** END LOOP

rm(all_sponsors, bills, legis_data, SS_bills, commem_bills, elec_year, i, keep_types)
rm(k_matches, klarner_sub, m_sub, km, bill_path, t_yrs, calc_LES, c_sub, t)

########################################################################################################################################################
########################################################################################################################################################
######## NAME FIX RECORDS
########################################################################################################################################################
########################################################################################################################################################

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1996 session! ~~~~~~~~~~~~~~
# -----> DROPPING 88 bill(s) introduced BY COMMITTEE
# -----> DROPPING 22 bill(s) introduced BY LEGISLATIVE COUNCIL
# GROBSCHMIDT -- Won Senate special in 1995, moved from House
# PANZER -- Won Senate special in 1993, moved from House
# SHIBILSKI -- Won Senate special in 1995
# WELCH, Robert t. -- Won Senate Special in 1995, moved from House # https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=7527
# TURNER -- No Introduced bills (but some coauthored) -- https://docs.legis.wisconsin.gov/1995/related/author_index/assembly/A_Turner_Robert

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 session! ~~~~~~~~~~~~~~
# -----> DROPPING 104 bill(s) introduced BY COMMITTEE
# -----> DROPPING 19 bill(s) introduced BY LEGISLATIVE COUNCIL
# SPILLNER -- Won 1997 special --- MAIDEN NAME (?) = WADE
# GROBSCHMIDT -- Won Senate special in 1995, moved from House
# PLACHE -- Won Senate special in 1996 following recall of George Petak, moved from House
# ROESSLER is labeled in election data by last name: BUETTNER --- Klarner data changes in 2000 (different Cand IDs, unfortunatley)
# ----> Note: She actually is listed under both names in the data these years, so must switch names mid-term
# DUEHOLM + LINTON + HUBLER = IN CHAMBER, NO PRIMARY INTRODUCED BILLS
# BRANCEL (ex-Speaker) -- Appointed as Secretary of Agriculture -- Timing Unclear, Assuming at start of term (new gov) --> DROP

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 session! ~~~~~~~~~~~~~~
# -----> DROPPING 109 bill(s) introduced BY COMMITTEE
# -----> DROPPING 30 bill(s) introduced BY LEGISLATIVE COUNCIL
# WAUKAU -- Won 1999 House special following Ourada resignation; lost subsequent general election
# LAZICH - Won April 1998 Senate special; moved from House
# OURADA resigned in Jan 29, 1999 ---> DROP
# KLUSMAN + RYBA = IN CHAMBER, No primary introduced bills

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 session! ~~~~~~~~~~~~~~
# -----> DROPPING 91 bill(s) introduced BY COMMITTEE
# -----> DROPPING 38 bill(s) introduced BY LEGISLATIVE COUNCIL
# HINES -- Won 2001 House special -- https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=43790
# STARZYK = Maiden name; changed to Samantha Kerkman
# WADE = Joan wade spillner
# KANAVAS = Won 2001 Senate Special
# WILLIAMS + RYBA + HEBL (TOM) + KREUSER = In Chamber, no primary introduced bills

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 session! ~~~~~~~~~~~~~~
# -----> DROPPING 87 bill(s) introduced BY COMMITTEE
# -----> DROPPING 32 bill(s) introduced BY LEGISLATIVE COUNCIL
# HONADEL + MOLEPSKE -- Won House Special
# ** Corrected MNISCHKE to NISCHKE in Klarner data **
# MORRIS --> Name change --> MORRIS-TATUM
# LENA TAYLOR -- Elected to Assembly in 2003 Special; then won 2004 election for Senate
# JULIE LASSA -- Won 2003 Senate Special; moved from House
# JEFF PLALE -- Won April 2003 Senate Special; moved from House in May
# GROBSCHMIDT appointed Asst. State Superintendent in Jan 2003 --- DROP
# COGGS = In house for ~ year -- Elected to Senate in Nov. 2003 Special -- https://web.archive.org/web/20040808065844/http://www.legis.state.wi.us/senate/sen06/s06bio.html
# RILEY + POWERS -- No evidence they weren't in chamber but appears to be last term for both
# TRAVIS (DAVID) = In chamber, no primary introduced bills

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 session! ~~~~~~~~~~~~~~
# -----> DROPPING 126 bill(s) introduced BY COMMITTEE
# -----> DROPPING 38 bill(s) introduced BY LEGISLATIVE COUNCIL
# Jeff PLALE -- Won April 2003 Senate Special; moved from House in May
# Robert TURNER = In chamber, no primary introduced bills

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 session! ~~~~~~~~~~~~~~
# -----> DROPPING 92 bill(s) introduced BY COMMITTEE
# -----> DROPPING 33 bill(s) introduced BY LEGISLATIVE COUNCIL
# WILLIAMS + YOUNG = In chamber, no primary introduced bills
# GRONEMUS = Last term, but appears to be in chamber

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 session! ~~~~~~~~~~~~~~
# -----> DROPPING 87 bill(s) introduced BY COMMITTEE
# -----> DROPPING 62 bill(s) introduced BY LEGISLATIVE COUNCIL
# MONTGOMERY + WILLIAMS + FITZGERALD + KERKMAN = In Chamber, no primary introduced bills
# Jeff WOOD -- Pleaded no-contest to charges on in Jan 2011 = After term but didn't resign during term, opted not to run again

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 session! ~~~~~~~~~~~~~~
# -----> DROPPING 104 bill(s) introduced BY COMMITTEE
# -----> DROPPING 15 bill(s) introduced BY LEGISLATIVE COUNCIL
# BILLINGS + TAYLOR + CRAIG + DOYLE + STROEBEL - Elected in 2011 House Special
# KING (Jessica) elected in 2011 Senate special; lost subsequent general
# SHILLING won 2011 special for Senate; moved from House
# PARISI resigned April 2011 to become County Exec (Taylor filled seat)
# GUNDERSON resigned January 2011 to join Dept of Nat. Resources --> DROP (Craig filled seat)
# HUEBSCH resigned Dec 30, 2010 after appointed to head Dept of Admin --> DROP (Doyle filled seat)
# GOTTLIEB resigned Jan 3. 2011 to be Sec. of Transpo. ---> DROP
# MEYER + STEINBRINK + JAUCH = In chamber, no primary introduced bills

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 session! ~~~~~~~~~~~~~~
# -----> DROPPING 66 bill(s) introduced BY COMMITTEE
# -----> DROPPING 26 bill(s) introduced BY LEGISLATIVE COUNCIL
# KULP + NEYLON + RODRIGUEZ + SKOWRONSKI won 2013 House special
# POPE = POPE-ROBERTS
# FARROW = Won Dec. 2012 Special for WI Senate --> DROP Assembly Win
# LEHMAN lost his senate seat in 2010, won recall election in 2012, did not seek reelection to Senate thereafter
# PETROWSKI won 2012 recall election for Senate seat
# DUPLICATES --> Only 1 Cullen in 2013-2014 Session --- David Cullen was out of office in at end of 2012

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 session! ~~~~~~~~~~~~~~
# -----> DROPPING 54 bill(s) introduced BY COMMITTEE
# -----> DROPPING 30 bill(s) introduced BY LEGISLATIVE COUNCIL
# DUCHOW won 2015 House Special
# HARRIS DODD == Nikiya Harris Dodd --> Sans Dodd in Klarner
# KAPENGA + STROEBEL won 2015 Senate specials; moved from House

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 session! ~~~~~~~~~~~~~~
# -----> DROPPING 119 bill(s) introduced BY COMMITTEE
# -----> DROPPING 18 bill(s) introduced BY LEGISLATIVE COUNCIL
# FELZKOWSKI == Mary CZAJA prior to Jan 2017
# KAPENGA won 2015 Senate Special
# DUPLICATES --> Only 1 Fitzgerald in Senate + 1 Larson in Senate --> Fixing Names
# FIELDS = In chamber, no primary introduced bills

# filter(klarner, grepl('fields', cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid)


##########################################################################################################
##########################################################################################################
##### MATCHING MISSING (SPECIALS) TO KLARNER USING OTHER TERMS WHERE SPONSOR IS PRESENT
##########################################################################################################

### Re-Load LES Data
LES_paths <- list.files(".", full.names = TRUE)
LES_paths <- LES_paths[!grepl("Archive|_All|Merged", LES_paths)]

LES <- LES_paths %>%
  lapply(read_csv, col_types = cols()) %>%
  bind_rows()

rm(LES_paths)

####### Fill in Missing Data from Candidates Elected in Specials using Subsequent Observations
missing <- unique(LES[is.na(LES$klarner_id), ]$sponsor)
for (name in missing) {
  name_sub <- filter(LES, grepl(glue("^{name},"), sponsor))
  exact <- TRUE
  if (nrow(name_sub) == 0) {
    name_sub <- filter(LES, grepl(glue("^{name}"), sponsor))
    exact <- FALSE
  }
  # If there is only ONE UNIQUE id that matches the name
  if (any(!is.na(name_sub$klarner_id)) & length(unique(na.omit(name_sub$klarner_id))) == 1) {
    LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name), ]$sponsor <- unique(name_sub[!is.na(name_sub$klarner_id), ]$sponsor)
    LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name), ]$klarner_name <- unique(na.omit(name_sub$klarner_name))
    LES[grepl(name, LES$sponsor) & is.na(LES$klarner_id), ]$klarner_id <- unique(na.omit(name_sub$klarner_id))
    print(glue(" ~~ {name} ~~ Matched to --> {unique(na.omit(name_sub$klarner_name))}"))
  } else {
    if (exact == TRUE) {
      k_sub <- filter(klarner, grepl(paste0("^", name, ","), cand))
    } else {
      k_sub <- filter(klarner, grepl(name, cand))
    }
    if (length(unique(k_sub$cand)) == 1) {
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name), ]$sponsor <- unique(k_sub$cand)
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name), ]$klarner_name <- unique(k_sub$cand)
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_id), ]$klarner_id <- unique(k_sub$candid)
      print(glue(" ~~ {name} ~~ Matched to --> {unique(k_sub$cand) }"))
    }
  }
}

### Manual Fixes

# ### Won in 2018 special -- will fix itself with updated klarner_data
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_id <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_name <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$sponsor <- "owlett, clint"


### Still missing = Elected in Specials
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[1]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, missing, still_missing)

## LENA TAYLOR -- First term via special election
LES[LES$sponsor %in% "taylor" & LES$term %in% "2003_2004", ]$klarner_id <- 269839
LES[LES$sponsor %in% "taylor" & LES$term %in% "2003_2004", ]$klarner_name <- "taylor, lena c."
LES[LES$sponsor %in% "taylor" & LES$term %in% "2003_2004", ]$sponsor <- "taylor, lena c."

### Jessica King -- Won a special in between two general losses
LES[LES$sponsor %in% "king" & LES$term %in% "2011_2012", ]$klarner_id <- 293219
LES[LES$sponsor %in% "king" & LES$term %in% "2011_2012", ]$klarner_name <- "king, jessica"
LES[LES$sponsor %in% "king" & LES$term %in% "2011_2012", ]$sponsor <- "king, jessica"

### Paul Farrow -- Moved to Senate after Special Win
LES[LES$sponsor %in% "farrow" & LES$term %in% "2013_2014", ]$klarner_id <- 307470
LES[LES$sponsor %in% "farrow" & LES$term %in% "2013_2014", ]$klarner_name <- "farrow, paul"
LES[LES$sponsor %in% "farrow" & LES$term %in% "2013_2014", ]$sponsor <- "farrow, paul"

###  John Lehman -- Lost Senate seat in 2010, won 2012 recall, didn't run again
LES[LES$sponsor %in% "lehman" & LES$term %in% "2013_2014", ]$klarner_id <- 247807
LES[LES$sponsor %in% "lehman" & LES$term %in% "2013_2014", ]$klarner_name <- "lehman, john w."
LES[LES$sponsor %in% "lehman" & LES$term %in% "2013_2014", ]$sponsor <- "lehman, john w."

############## Check for Duplicates
### ---> If two people with same last name and one won in a special, will lead to duplicates
for (t in unique(LES$term)) {
  print(glue(" ************ {t} ***************"))
  check_dup <- filter(LES, term == t) %>%
    group_by(chamber) %>%
    mutate(dup = duplicated(klarner_id) & !is.na(klarner_id))
  if (any(check_dup$dup)) {
    filter(LES, term == t & klarner_id %in% check_dup[check_dup$dup == TRUE, ]$klarner_id & !is.na(klarner_id)) %>%
      select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>%
      print()
  }
}
rm(check_dup, k_sub, exact, name_sub, missing, t, name)


########################################################################################################################################################
########################################################################################################################################################
######## AGGREGATE AND MATCH TO EXTERNAL
########################################################################################################################################################
########################################################################################################################################################

### Klarner State Leg. Election Data --- Using Adjusted File From Top
klarner_sub <- filter(klarner, sab == this_state & year >= min_year - 5 & outcome == "w")
klarner_sub <- select(klarner_sub, caseid, year, sen, ddez, dno, term, termz, cando, cand, candid, partyz, partyt, exper, outcome, etype)

#### KLARNER IS YEAR OF ELECTION, Not TERM
LES$exper <- LES$party <- LES$district <- NA
LES$district <- as.double(LES$district)
LES$party <- as.character(LES$party)
LES$exper <- as.character(LES$exper)

for (name in unique(LES$sponsor)) {
  this_sponsor_LES <- LES[LES$sponsor == name, ]
  sponsor_rows <- filter(klarner_sub, candid %in% na.omit(this_sponsor_LES$klarner_id))
  if (nrow(sponsor_rows) == 0) {
    ### Check Losers
    sponsor_rows <- filter(klarner, candid %in% na.omit(this_sponsor_LES$klarner_id))
    if (nrow(sponsor_rows) >= 1) {
      LES[LES$sponsor == name, ]$party <- sponsor_rows[1, ]$partyz
    }
  } else {
    for (t in this_sponsor_LES$term) {
      second_year <- as.numeric(str_split(t, "_")[[1]][2])
      ### Filling in by chamber to account for people who switch chambers mid-term
      for (c in this_sponsor_LES[this_sponsor_LES$term == t, ]$chamber) {
        sponsor_sub <- filter(sponsor_rows, (etype %in% spec_elec_codes & year == second_year) | year < second_year)
        sponsor_sub <- filter(sponsor_sub, sen == ifelse(c == "Senate", 1, 0))
        if (nrow(sponsor_sub) > 0) {
          sponsor_sub <- arrange(sponsor_sub, desc(year))
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c, ]$district <- sponsor_sub[1, ]$dno
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c, ]$party <- sponsor_sub[1, ]$partyz
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c, ]$exper <- sponsor_sub[1, ]$exper
        } else {
          if (nrow(sponsor_rows) > 0) {
            sponsor_rows <- arrange(sponsor_rows, year)
            LES[LES$sponsor == name & LES$term == t & LES$chamber == c, ]$party <- ifelse(is.logical(na.omit(unique(sponsor_rows$partyz))), NA, na.omit(unique(sponsor_rows$partyz))[1])
            # LES[LES$sponsor == name & LES$term == s & LES$chamber == c,]$exper <- ifelse(is.logical(na.omit(unique(sponsor_rows$exper))), NA, na.omit(unique(sponsor_rows$exper))[1] )
          }
        }
      }
    }
  }
  # print(name)
}

###### IF MISSING CHECK LOSERS
# filter(LES, is.na(party)) %>% select(sponsor, term, klarner_id, district, party, exper)
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)

#########################################################
############ Match to Hall/Fouirnaies
########################################################

library(readstata13)

### Hall and Fournaies 2018, Covers 1988 - 2014
### --> https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/PGCVDP
hf_data <- read.dta13("~/Dropbox/Data/Hall_Fouirnaies_2018/How_Do_interest_Groups_Seek_Access_to_Committees.dta")
hf_data <- filter(hf_data, state == this_state)
hf_data <- hf_data[, c(colnames(hf_data)[c(1:3, 7:14, 204, 208, 217)], colnames(hf_data)[grepl("cmt_", colnames(hf_data))])]
hf_data$term <- paste0(hf_data$year + 1, "_", hf_data$year + 2)
hf_data$chamber <- ifelse(hf_data$chamber == "senate", "Senate", "House")

### If > 2-Year Terms: Expand Senate Rows
senate <- filter(hf_data, CandId == "aaaa")
for (i in 1:nrow(hf_data)) {
  if (hf_data[i, ]$chamber == "House") next
  sen_sub <- filter(hf_data, CandId == hf_data[i, ]$CandId)
  if (!(paste0(hf_data[i, ]$year + 3, "_", hf_data[i, ]$year + 4) %in% sen_sub$term)) {
    new_row <- hf_data[i, ]
    new_row$MajorityMember <- NA
    new_row$term <- paste0(hf_data[i, ]$year + 3, "_", hf_data[i, ]$year + 4)
    senate <- bind_rows(senate, new_row)
  }
}
hf_data <- bind_rows(hf_data, senate)
rm(senate, new_row, sen_sub)

### Subset
hf_data <- filter(hf_data, year > min_year - 4) %>%
  distinct() %>%
  select(-MajorityMember)

### Merge
LES <- left_join(LES, select(hf_data, -year), by = c("term" = "term", "chamber" = "chamber", "klarner_id" = "CandId"))

### Check Duplicates
mutate(LES, check_dup = paste(sponsor, term, chamber, sep = "--")) %>%
  mutate(dup = duplicated(check_dup)) %>%
  filter(dup == TRUE)

### Set Committees to NA for Years without Data -- May have matched candids in year range
set_NA <- colnames(hf_data)
set_NA <- set_NA[!(set_NA %in% c("CandId", "chamber", "year", "term"))]
LES[LES$term == "2017_2018", set_NA] <- NA

rm(hf_data, set_NA, i)

#################################
### Shor and McCarty Data, 1993 - 2016
####################################

ideo <- readstata13::read.dta13("~/Dropbox/Data/Shor_McCarty_Data/shor_mccarty_1993_2016_individual_data_May_2018.dta")
ideo <- filter(ideo, st == this_state) %>% select(-st_id)
ideo$match_name <- str_extract(tolower(ideo$name), "[a-z]+, [a-z]+")
ideo$match_name <- ifelse(is.na(ideo$match_name), tolower(ideo$name), ideo$match_name)
ideo$last_name <- gsub(",.+", "", tolower(ideo$name))


#### Doing this row by row to more easily account for party, unique data_names, etc.
#### Starting with MT (May 7, 2019) this now cross-checks to make sure it doesn't match on last name if multiple smiths, for example.
#### FOR MT x 2: Supplemented GA Code to Match Party Switchers if One or Both Party-Terms Present in Data
LES$SM_name <- LES$SM_party <- LES$np_score <- NA
LES$SM_name <- as.character(LES$SM_name)
LES$SM_party <- as.character(LES$SM_party)
LES$np_score <- as.double(LES$np_score)

for (i in 1:nrow(LES)) {
  ####### **** CHECK LAST NAME + ACCOUNT FOR VARIATIONS IF NO MATCH *******
  check_last <- which(gsub(",.+|\\'", "", LES[i, ]$sponsor) == tolower(ideo$last_name))
  ### if none, adjust name
  if (length(check_last) == 0) {
    check_last <- which(gsub(",.+|\\'| |-", "", LES[i, ]$sponsor) == gsub(" |\\'|-", "", tolower(ideo$last_name)))
  }
  ## If Still None, Try Data Last Name
  if (length(check_last) == 0) {
    d_name <- str_split(LES[i, ]$data_name, " ")[[1]]
    check_last <- which(d_name[length(d_name)] == tolower(ideo$last_name))
  }

  ####### ***** IF MORE THAN ONE MATCH *******
  if (length(check_last) > 1) {
    ### Try Data Name
    ideo_match <- filter(ideo[check_last, ], match_name == LES[i, ]$data_name)
    ### CHeck First Initial
    if (nrow(ideo_match) != 1) {
      ideo_match <- ideo[check_last, ][substring(gsub(".+, ", "", tolower(ideo[check_last, ]$name)), 1, 1) == substring(gsub(".+, ", "", LES[i, ]$sponsor), 1, 1), ]
    }
    ### Check Last + First Name
    if (nrow(ideo_match) > 1) {
      ideo_match <- filter(ideo_match, match_name == gsub(" [a-z]\\.$", "", LES[i, ]$sponsor))
    }
    ### Check Party if Still Too Long
    if (nrow(ideo_match) == 2 & length(unique(ideo_match$name)) == 1 & any(ideo_match$party == "R") & any(ideo_match$party == "D")) {
      for (p in c("d", "r")) {
        if (any(LES[LES$sponsor == LES[i, ]$sponsor, ]$party == p)) {
          LES[LES$sponsor == LES[i, ]$sponsor & LES$party == p, ]$SM_name <- ideo_match[ideo_match$party == toupper(p), ]$name
          LES[LES$sponsor == LES[i, ]$sponsor & LES$party == p, ]$SM_party <- ideo_match[ideo_match$party == toupper(p), ]$party
          LES[LES$sponsor == LES[i, ]$sponsor & LES$party == p, ]$np_score <- ideo_match[ideo_match$party == toupper(p), ]$np_score
        }
      }
    } else if (nrow(ideo_match) > 1) {
      ideo_match <- filter(ideo_match, party == toupper(LES[i, ]$party))
    }

    ##### ****** IF ONE LAST NAME MATCH - VERIFY NOT IDENTICAL LAST NAMES *********
  } else if (length(check_last) == 1) {
    #### CHECK IF ANY OTHER LEGISLATORS WITH SAME LAST NAME
    lastname <- gsub(",.+|\\'", "", LES[i, ]$sponsor)
    num_with_same_last <- filter(LES, grepl(glue("^{lastname},"), sponsor)) %>%
      select(sponsor) %>%
      unlist() %>%
      unique()
    if (length(num_with_same_last) > 1) {
      ### Check FUll Name
      if (grepl(ideo[check_last, ]$match_name, LES[i, ]$sponsor)) {
        ideo_match <- ideo[check_last, ]
      } else {
        ## Set to 0 rows
        ideo_match <- filter(ideo, match_name == "zzzz")
      }
    } else {
      ideo_match <- ideo[check_last, ]
    }
  } else {
    # Set to 0 rows if no match
    ideo_match <- filter(ideo, match_name == "zzzz")
  }
  ### Save if ONE MATCH After whole process
  if (nrow(ideo_match) == 1) {
    LES[LES$sponsor == LES[i, ]$sponsor, ]$SM_name <- ideo_match$name
    LES[LES$sponsor == LES[i, ]$sponsor, ]$SM_party <- ideo_match$party
    LES[LES$sponsor == LES[i, ]$sponsor, ]$np_score <- ideo_match$np_score
    # cat(" \n Manually matched ", toupper(LES[i,]$sponsor), " to ", toupper(ideo_match$name), "\n .")
  }
  rm(ideo_match)
}
# select(LES, sponsor, SM_name) %>% distinct() %>% View()

### SM Names Matched to Multiple LES Sponsors
# ---> so this will catch all errors except those where Sponsor A is Matched to Voter Score B, when Sponsor B and Voter A are missing
filter(LES, !is.na(SM_name)) %>%
  group_by(SM_name) %>%
  summarize(name_matches = paste(unique(sponsor), collapse = "----")) %>%
  filter(grepl("----", name_matches))

### Fix Mismatches
# -- Two SM Records: One Reynolds, Other Martin Reynolds
LES[LES$sponsor %in% c("reynolds, martin l."), c("SM_name", "SM_party", "np_score")] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
## ** Notable Names -- Carlie 'Cydnie' Boland
mutate(LES, first_name_LES = gsub(", ", "", str_extract(sponsor, ", [a-z]+")), first_name_SM = gsub(", ", "", str_extract(tolower(SM_name), ", [a-z]+"))) %>%
  filter(first_name_LES != first_name_SM) %>%
  select(sponsor, SM_name) %>%
  distinct() %>%
  as.data.frame()

### MANUAL FIXES -- SM Data only goes through 2016 so matches after that are from earlier period
# filter(LES, is.na(np_score)) %>% filter( !(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('pott', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

name_matches <- data.frame(LES_name = "harris, nikiya", SM_name = "Harris Dodd, Nikiya")
# name_matches <- add_row(name_matches, LES_name = 'lemahieu, devin', SM_name = 'zzzzzzz') ### IS NOT DANIEL LEMAHIEU
name_matches <- add_row(name_matches, LES_name = "reynolds, martin l.", SM_name = "Reynolds, Martin") # Two reynolds, other has no first name
name_matches <- add_row(name_matches, LES_name = "schaber, penny bernard", SM_name = "Bernard Schaber, Penny")
name_matches <- add_row(name_matches, LES_name = "spillner, joan wade", SM_name = "Wade Spillner, Joan")
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')

for (i in 1:nrow(name_matches)) {
  LES[LES$sponsor == name_matches[i, ]$LES_name, ]$SM_name <- ideo[ideo$name == name_matches[i, ]$SM_name, ]$name
  LES[LES$sponsor == name_matches[i, ]$LES_name, ]$SM_party <- ideo[ideo$name == name_matches[i, ]$SM_name, ]$party
  LES[LES$sponsor == name_matches[i, ]$LES_name, ]$np_score <- ideo[ideo$name == name_matches[i, ]$SM_name, ]$np_score
  # print(i)
}

# #### Manual Edits (Needs more precision...)
LES[LES$sponsor == "potter, rosemary", ]$SM_name <- ideo[ideo$name == "Potter" & ideo$house1995 %in% 1, ]$name
LES[LES$sponsor == "potter, rosemary", ]$SM_party <- ideo[ideo$name == "Potter" & ideo$house1995 %in% 1, ]$party
LES[LES$sponsor == "potter, rosemary", ]$np_score <- ideo[ideo$name == "Potter" & ideo$house1995 %in% 1, ]$np_score

LES[LES$sponsor == "potter, calvin", ]$SM_name <- ideo[ideo$name == "Potter" & ideo$senate1995 %in% 1, ]$name
LES[LES$sponsor == "potter, calvin", ]$SM_party <- ideo[ideo$name == "Potter" & ideo$senate1995 %in% 1, ]$party
LES[LES$sponsor == "potter, calvin", ]$np_score <- ideo[ideo$name == "Potter" & ideo$senate1995 %in% 1, ]$np_score

### Check for PARTY SWITCHERs
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% arrange(sponsor, term) %>% as.data.frame()
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% arrange(sponsor, term) %>% as.data.frame()

rm(ideo, check_last, name_matches, d_name, i, lastname)

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1995 - 2020
LES[as.numeric(substring(LES$term, 1, 4)) %in% c(2009:2010) & LES$chamber == "House" & LES$party %in% "d", ]$in_majority <- 1
LES[as.numeric(substring(LES$term, 1, 4)) %in% c(1995:2008, 2011:2020) & LES$chamber == "House" & LES$party %in% "r", ]$in_majority <- 1

### Senate -- 1995 - 2020
# ** Control Shifted back and forth between 1995 and 1998
# --> Brian Rude (R) speaker 1995, Fred Risser (D) 1996 - 1997, Brian Rude (R) 1998 ---> Coding ALL 1
LES[as.numeric(substring(LES$term, 1, 4)) %in% c(1999:2002, 2007:2010) & LES$chamber == "Senate" & LES$party %in% "d", ]$in_majority <- 1
LES[as.numeric(substring(LES$term, 1, 4)) %in% c(2003:2006, 2011:2020) & LES$chamber == "Senate" & LES$party %in% "r", ]$in_majority <- 1



###########################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

##### Drop Nicknames
# filter(LES, grepl('\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
# LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

#### Manual Fixes
LES[LES$sponsor == "czaja, mary", ]$sponsor <- "felzkowski, mary czaja"
LES[LES$sponsor == "poperoberts, sondy", ]$sponsor <- "pope-roberts, sondy m."
LES[LES$sponsor == "towns, debi", ]$sponsor <- "towns, debra"
LES[LES$sponsor == "strachota, pat", ]$sponsor <- "strachota, patricia"
LES[LES$sponsor == "pasch, sandy", ]$sponsor <- "pasch, sandra"
LES[LES$sponsor == "johnson, la tonya", ]$sponsor <- "johnson, latonya"
# LES[LES$sponsor == 'zzzzz',]$sponsor <- 'zzzzzz'


##############################################
###  Save
##############################################

### Check Missingnes
LES %>%
  group_by(chamber, term) %>%
  summarize(
    Election_Data_Missing = round(sum(is.na(klarner_id)) / n(), 2),
    CM_Leadership_Missing = round(sum(is.na(SpeakerHouse)) / n(), 2),
    Ideology_Missing = round(sum(is.na(np_score)) / n(), 2)
  ) # %>% View()

###### SAVE Merged File
colnames(LES)
if (!dir.exists("Merged")) {
  dir.create("Merged")
}
write.csv(LES, glue("Merged/{this_state}_LES_All_M.csv"), row.names = FALSE)

### Save by Session
for (t in unique(LES$term)) {
  LES_sub <- filter(LES, term == t)
  write.csv(LES_sub, glue("Merged/{this_state}_LES_{t}_M.csv"), row.names = FALSE)
}

##############################################
###  ******* EXPLORE ***********
##############################################


library(ggplot2)
library(ggridges)
library(forcats)

LES %>%
  group_by(term, party) %>%
  summarize(
    mean_LES = mean(LES),
    max_LES = max(LES)
  )

#### Should split this by chamber
LES %>%
  mutate(t_factor = fct_rev(as.factor(gsub("_", "-", term)))) %>%
  filter(party %in% c("d", "r")) %>%
  ggplot(aes(y = t_factor)) +
  geom_density_ridges(aes(x = LES, fill = party), alpha = .8, color = "white", from = 0, to = 5) +
  xlab("LES") +
  ylab("Term") +
  ggtitle(glue("Legislative Effectiveness in {this_state}")) +
  scale_fill_cyclical(
    breaks = c("d", "r"),
    labels = c("d" = "Democrat", "r" = "Republican"),
    values = c("dodgerblue2", "red2"),
    name = "Party", guide = "legend"
  ) +
  theme_ridges(grid = FALSE) +
  facet_wrap(~chamber) +
  theme(axis.title.x = element_text(hjust = 0.5), axis.title.y = element_text(hjust = 0.5), legend.position = "bottom")

ggsave(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Figures/{this_state}_LES_ridgeplot.pdf"), height = 8, width = 7)

#########
ggplot2::ggplot(LES, aes(x = np_score, y = LES, color = party)) +
  geom_point() +
  geom_smooth(formula = "y ~ x", method = "lm", se = FALSE) +
  facet_wrap(~term) +
  scale_color_manual(values = c("dodgerblue2", "purple2", "red2", "gray50"))

##### CHECK OUTLIERS
## -- Sarah Waukau -- Odd -- Votes as an R, but ran as a D in the general election (that she lost) following her special win -- See: https://elections.wi.gov/sites/electionsuat.wi.gov/files/2000_State_Assembly_County_Returns.pdf
# filter(LES, party == 'd' & np_score > .25) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & np_score < -.25) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = "factor", type = "text")
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = "factor", type = "text")

# stargazer::stargazer(lm(LES ~ MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')
