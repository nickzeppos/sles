################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** NEW MEXICO *** BY SESSION
##############################################################

###################################
## (SPECIAL) SESSIONS:
## ---- Seperated out in files; bill numbers re-start; bills do not carry-over
## MEMBER LISTS:
## ---- Current and Former: https://www.nmlegis.gov/Members/Find_My_Legislator
## PROCESS/RULES:
## ---- House Rules: https://www.nmlegis.gov/Publications/Legislative_Procedure/house_rules_19.pdf
## ---- Senate Rules: https://www.nmlegis.gov/Publications/Legislative_Procedure/senate_rules_19.pdf
## Sponsorship/Authorship
## ---- Do not have cosponsors in data (and website doesn't record them until 2015)
###########################
## NOTES:
## **** Actions are out of order, don't have dates, but do have 'legisltive days' for most actions --> recoding that way
## **** Handful of 'No actions found' bills -- I've recoded using trimmed status descriptions
## **** Actions for 2014 especially bad, many missing
#############################

rm(list=ls())
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
library(inexact)
library(tibble)
library(foreach)


this_state <- 'NM'
keep_types <- c('HB', "SB")

#### Output Directory
#dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd(glue("../../LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Terms/Sessions and Filespaths

data_files <- list.files(glue("../../../State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
sessions <- gsub(".+Bill_Details_|.csv", '', bill_files)
rm(data_files, bill_files)

### Terms/Sessions and Filespaths
terms <- 2021
t = terms
t_plus_one = t + 1
t_yrs <- as.numeric(terms)
t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 1}'))
t_sessions = sessions[startsWith(sessions,as.character(t)) | startsWith(sessions,as.character(t+1))]


#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- bind_rows(read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t_plus_one}.csv")))

SS_bills <- SS_bills %>% 
  filter(State == this_state) %>%
  rename(bill_id = Bill.No) %>%
  mutate(Date = gsub("Sept","Sep",Date),
         date = as.Date(gsub("\\.","",Date), "%B %d, %Y"),
         year = as.integer(format(date, "%Y")),
         term = ifelse(year %% 2 == 1, paste0(year, "_", year + 1), paste0(year - 1, "_", year)), 
         bill_id = toupper(bill_id),
         bill_id = gsub(' ','',bill_id),
         bill_id = paste0(gsub("[0-9].+", '', bill_id), str_pad(gsub("^[A-Z]+", "", bill_id), 4, pad = "0")),
         SS = 1) %>%
  select(state = State, term, year, bill_id, everything())


###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[7]



### Skip Previously Estimated
if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
  print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
  next
}

###### SESSION IN PROGRESS
print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_sessions[1]}.csv")
bills <- read.csv(bill_path)

### If multiple sessions in different files, read in those as well
if(length(t_sessions) > 1){
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{s}.csv")
    s_bills <- read.csv(bill_path)
    bills <- bind_rows(bills, s_bills)
  }
  rm(s, s_bills)
}

### Clean Term/Session Variables
# --> need to adjust comm_reports NAs for later coding check
bills <- bills %>%
  mutate(term = t_yrs,
         session_type = recode(session_type, 'S1' = 'SS1', 'S2' = 'SS2', 'S3' = 'SS3', 'S4' = 'SS4', 'S5' = 'SS5', 'S6' = 'SS6'),
         session = paste(session_year, session_type, sep = "-"),
         comm_reports = ifelse(is.na(comm_reports), '', comm_reports))

### Drop duplicates
bills <- distinct(bills)

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_number)

############### Drop Resolutions, Messages, Communications, Reports
all_bills <- bills
## SN = Senate Nomination
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) 

##########################
####### Standardize Sponsors

bills$sponsors <- tolower(bills$sponsors)
bills$sponsors <- gsub('á', 'a', bills$sponsors)
bills$sponsors <- gsub('é', 'e', bills$sponsors)
bills$sponsors <- gsub('ó', 'o', bills$sponsors)
bills$sponsors <- gsub('í', 'i', bills$sponsors)
bills$sponsors <- gsub('ñ', 'n', bills$sponsors)
bills$sponsors <- gsub('  +', ' ', bills$sponsors)

### Clean Nicknames
bills$sponsor_nickname <- gsub('\\"', '', str_extract(bills$sponsors, '\\".+\\"'))
bills$sponsors <- gsub(' \\"[^\\"]+\\"', '', bills$sponsors)

### LES Sponsor Var
# -- Not clear any of these have second sponsors, but adjusting just in case
bills$LES_sponsor <- gsub(';.+', '', bills$sponsors)
table(bills$LES_sponsor)

#### Fix Errors
if(t_yrs == '1997_1998'){
  ### Jim Trujillo  not elected until 2003... Per PDFs of bills, all Jim Trujillo bills are Patsy Trujillo
  bills[bills$LES_sponsor == "jim r. trujillo",]$LES_sponsor <- "patsy trujillo knauer"
}

###################
###### Merge in S&S Bills
###################


# *** For NEW MEXICO: Bills DO NOT carry over during regular AND numbers re-start for all regular and special sessions
# ---> Need to merge on ID + SESSION
# ---> Capping Bill numbers at SESSION MAX and assuming bill is from SS with most bills proposed

if(t_yrs == "2019_2020"){
  SS_bills$bill_id[SS_bills$bill_id=="SB80008"]="SB0008"
  SS_bills$bill_id[SS_bills$bill_id=="HB20002"]="HB0002"
  SS_bills$bill_id[SS_bills$bill_id=="HB80008"]="HB0008"
  SS_bills$bill_id[SS_bills$bill_id=="HB10001"]="HB0001"
  SS_bills$bill_id[SS_bills$bill_id=="HB50005"]="HB0005"
  SS_bills$bill_id[SS_bills$bill_id=="SB40004"]="SB0004"
  SS_bills$bill_id[SS_bills$bill_id=="SB30003"]="SB0003"
  SS_bills$bill_id[SS_bills$bill_id=="SB70007"]="SB0007"
  SS_bills$bill_id[SS_bills$bill_id=="SB20002"]="SB0002"
}
if(t_yrs == "2021_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="SB10001"]="SB0001"
  SS_bills$bill_id[SS_bills$bill_id=="HB20002"]="HB0002"
  SS_bills$bill_id[SS_bills$bill_id=="HB40004"]="HB0004"
  SS_bills$bill_id[SS_bills$bill_id=="HB10001"]="HB0001"
  SS_bills$bill_id[SS_bills$bill_id=="HB50005"]="HB0005"
  SS_bills$bill_id[SS_bills$bill_id=="SB40004"]="SB0004"
  SS_bills$bill_id[SS_bills$bill_id=="SB70007"]="SB0007"
  SS_bills$bill_id[SS_bills$bill_id=="HB70007"]="HB0007"
  
}

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, year, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS, year,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,sponsors), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",sponsors, ignore.case=T)) %>%
  arrange(sponsors) 
unique(missing_SS_bills$bill_id) 


# now check to see if there are duplicate joins


duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills %>% mutate(year = as.integer(substr(session,1,4))), 
            by = c("bill_id", "term", "year")) %>%
  group_by(rowname) %>% mutate(count = n()) %>%
  ungroup() %>% 
  select(count,bill_id,term,session, Title, title) %>% 
  arrange(desc(count),bill_id)

if(nrow(duplicate_SS_bills %>% filter(count > 1)) > 0 ){
  print("you have duplicates"); SS_duplicates_exist <- 1; # break
} else{
  print("no duplicates")
  SS_duplicates_exist <- 0
}

# if you have duplicated bills, you have to go into this if statement. otherwise, do the else logic. 

if(nrow(duplicate_SS_bills %>% filter(count > 1)) > 0 ){
  # now have to remove the duplicated bills that don't correspond
  write.csv(duplicate_SS_bills, glue("../../../State Legislative Data/States/{this_state}/{this_state}_duplicate_SS_bills_{t_yrs}.csv"), row.names=F)
  # edit this file in Excel, create a column called filter, put in the value "remove" if the Title from PVS doesn't match the bill description
  SS_term = read.csv(glue("../../../State Legislative Data/States/{this_state}/{this_state}_duplicate_SS_bills_{t_yrs}_edited.csv")) %>%
    filter(filter != "remove") %>%
    select(bill_id,term,session) %>% mutate(SS = 1) %>% distinct()
  
  
  
  bills <- bills %>% 
    left_join(SS_term , by = c("bill_id", "term", "session")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS)) %>% 
    distinct()
} else {
  orig_row_n = c(nrow(bills),nrow(SS_term))
  bills2 <- bills %>% mutate(year = as.integer(substr(session,1,4)))%>% 
    left_join(SS_term %>% select(-Title) %>% distinct(), by = c("bill_id", "term","year")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  SS_term2 = SS_term %>% 
    left_join(bills %>% mutate(year = as.integer(substr(session,1,4))) %>% select(bill_id,term,session, year),by=c("bill_id","term","year"))
  if(!identical(c(nrow(bills2),nrow(SS_term2)),orig_row_n )){print("merge failed"); break} else{
    bills = bills2; SS_term = SS_term2; rm(bills2, SS_term2)
  }
}

### Check Missing
table(bills$SS)
SS_in_bills = sum(bills$SS)
SS_in_PVS = nrow(SS_term %>%
                   mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
                   filter(bill_type %in% keep_types) )

if(SS_in_bills == SS_in_PVS){
  print("all SS merged properly")
} else {
  print(glue("{SS_in_PVS} S&S bills in original dataset, but {SS_in_bills} S&S in our bills dataset"))
  
  # stuff in PVS, not in bills
  print(anti_join(SS_term, bills, by = c("bill_id", "term", "session")))
  
  # PVS bills that are duplicated in bills. not necessarily a problem!
  print(SS_term %>% group_by(bill_id, term) %>%
          mutate(count = n()) %>% filter(count > 1) %>% arrange(bill_id))
  
  # see if they are equal after removing double-counted bills. this is a crude proxy, you should examine more closely
  all.equal(SS_in_bills + nrow(anti_join(SS_term, bills, by = c("bill_id", "term", "session")))+
              nrow(SS_term %>% group_by(bill_id, term) %>%
                     mutate(count = n()) %>% filter(count > 1))/2,
            nrow(SS_term ))
}

rm(all_bills, missing_SS_bills, duplicate_SS_bills)


####################################################
############### Code Commemorative
####################################################

bills <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session'))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)


###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "" | is.na(LES_sponsor)) %>% View()
if(nrow(filter(bills, LES_sponsor == '')) > 0 | any(is.na(bills$LES_sponsor))){
  cat('\n')
  cat(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == '')) + sum(is.na(bills$LES_sponsor))} bill(s) without a sponsor"))
  bills <- filter(bills, !(LES_sponsor == '' | is.na(LES_sponsor)))     
}


#######################################
############### Code Bill History
#######################################
bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
bill_hist <- read.csv(bill_hist_path)

## If multiple sessions, read in those as well
if(length(t_sessions) > 1){
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
    s_hist <- read.csv(bill_path)
    bill_hist <- bind_rows(bill_hist, s_hist)
  }
  rm(s, s_hist)
}

### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
bill_hist <- bill_hist %>%
  rename(bill_id = bill_number) %>%
  mutate(term = t_yrs,
         session_type = recode(session_type, 'S1' = 'SS1', 'S2' = 'SS2', 'S3' = 'SS3', 'S4' = 'SS4', 'S5' = 'SS5', 'S6' = 'SS6'),
         session = paste(session_year, session_type, sep = "-"))

### Re-Order (Need to Fix Order Variable using "Legislative Days" --> won't be perfect, but should be mostly right)
bill_hist <- bill_hist %>%
  group_by(session, bill_id) %>%
  filter(!grepl("legislative day: [0-9]+", tolower(action))) %>%
  mutate(legislative_day = ifelse(order == 1 & order == max(order) & (is.na(legislative_day) | legislative_day == ''), "LD: 1", legislative_day)) %>%
  mutate(legislative_day = as.numeric(gsub("LD: ", '', legislative_day)),
         legislative_day_i = ifelse(is.na(legislative_day), max(legislative_day, na.rm = TRUE) + 1, legislative_day)
  ) %>%
  arrange(session, bill_id, legislative_day_i, order) %>%
  mutate(order = 1:n()) %>%
  ungroup() %>%
  select(-legislative_day_i)

### Clean Action Text
bill_hist$action <- str_trim(gsub('  +', ' ', gsub('\\&nbsp', ' ', bill_hist$action)))

### Code Chamber
#### *** Maybe just don't do this and code directly??? Actions are pretty clear all things considered
#### --- If assume that committee from originating chamber will report first, then checking for ANY report is probably fine
bill_hist$chamber <- "U" # U = Unknown
bill_hist$chamber <- ifelse(bill_hist$order == 1, substring(bill_hist$bill_id,1,1), bill_hist$chamber)
bill_hist$chamber <- ifelse(grepl("^house", tolower(bill_hist$action)), "H", bill_hist$chamber)
bill_hist$chamber <- ifelse(grepl("^senate", tolower(bill_hist$action)), "S", bill_hist$chamber)
# Need to do this after ^house/^senate because to catch stuff akin to "House bill passed in the senate"
bill_hist$chamber <- ifelse(grepl("in the house", tolower(bill_hist$action)), "H", bill_hist$chamber)
bill_hist$chamber <- ifelse(grepl("in the senate", tolower(bill_hist$action)), "S", bill_hist$chamber)
bill_hist$chamber <- ifelse(bill_hist$chamber == "U" & grepl("^Sent to", bill_hist$action), substring(gsub('^Sent.+Referrals: ', '', bill_hist$action),1,1), bill_hist$chamber)
bill_hist$chamber <- ifelse(bill_hist$chamber == "U" & grepl("^H[A-Z][A-Z]+", bill_hist$action), "H", bill_hist$chamber)
bill_hist$chamber <- ifelse(bill_hist$chamber == "U" & grepl("^S[A-Z][A-Z]+", bill_hist$action), "S", bill_hist$chamber)
bill_hist$chamber <- ifelse(grepl("Gov", bill_hist$action), "G", bill_hist$chamber)

bill_hist <- bill_hist %>%
  group_by(session, bill_id) %>%
  ### Find passed chamber row if present
  mutate(action = tolower(action),
         passed_row = ifelse(any(grepl("^passed", action)), ifelse(substring(bill_id,1,1) == "H", min(grep("^passed in the house", action)), min(grep("^passed in the senate", action))), 0),
         chamber = ifelse(chamber == "U" & order <= passed_row, substring(bill_id,1,1), chamber),
         chamber = ifelse(chamber == "U" & passed_row == 0, substring(bill_id,1,1), chamber)) %>%
  select(-passed_row) %>%
  ungroup()

### Re-Coding Chamber Variable
bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate", "G" = "Governor", "C" = "Conference", 'U' = "Unknown")

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
# *** NM: Actions are sporadically out of order -- no dates provided (besides "legislative days", which aren't always provided)
aic_t <- c('reported by', 'do pass', 'do not pass', 'committee substitution', 'without recommendation')
# For ABC: Need "committee with" in reported by or else will catch "Reported by committee to fall within the purview of a 30 day session"
# ---> Coding reported by.+ purview as AIC however as often involves clearing the committee on committees
abc_t <- c('reported by committee with', 'placed.+calendar', 'floor amendment', '^failed to pass', 'by motion', 'motion to',
           'withdrawn from comm.+placed on') 
## Need placed on for withdrawn as other uses don't always mean ABC and seem to be simple changes of committee assignment
pc_t <- c('^passed', 'referrals: cc', 'failed to concur', 'has concurred')
### If sent to conference, concurred, or failed to concur, necessarily passed
law_t <- c('signed by gov', 'chapter [0-9]+')

### Check Actions
# filter(bill_hist, grepl('^withdrawn', tolower(action))) %>% distinct(action) %>% View()
# filter(bill_hist, grepl('effective date', tolower(action))) %>% group_by(action) %>% summarize(n = n()) %>% arrange(desc(n)) %>% View()
# bill_hist[bill_hist$bill_id == "HF0791",])
# mutate(bill_hist, clean = gsub('committee~.+', 'committee', gsub("[0-9]+", '', action))) %>% distinct(clean) %>% unlist() %>% unname()

### Catch "No Actions Found" Bills and Correct Later
# -- 1998-RS: SB0100 has no sponsor, no actions, no bill text
# -- 2001-SS2: HB0001 = no actions list but detailed status with progress
# -- 2007-SS1: No bills have actions list, but all have detailed status line
if(t > 2014 & any(grepl('no actions found', bill_hist$action))){
  print(" ******** NO ACTIONS FOUND --> CORRECT IN LOOP **************")
  break
}
# filter(bill_hist, action == 'no actions found') %>% View()

####################
### Output Matrix
all_bill_stages = tibble(bill_id = character(0),
                         term = character(0),
                         session = character(0),
                         LES_sponsor = character(0),
                         introduced = integer(0),
                         action_in_comm = integer(0),
                         action_beyond_comm = integer(0),
                         passed_chamber = integer(0),
                         law = integer(0),
                         bill_url = character(0))

### Code History Stages for Each Bill --- Subset Hist File, Code, Save, Repeat
options(warn = 2)
for(i in 1:nrow(bills)){
  b_id = bills[i,]$bill_id
  s_id = bills[i,]$session
  b_spon = bills[i,]$LES_sponsor
  hist_sub <- filter(bill_hist, bill_id == b_id, term == t_yrs, session == s_id)
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t, 
                                    ignore_chamber_switch = TRUE, add_chamb = c("Unknown", 'Conference'))
  bill_stages$bill_url <- bills[i,]$bill_url
  ### Check Law Codings
  if(bill_stages$law == 0 & bills[i,]$status == "Chaptered"){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- bill_stages$law <- 1
  }
  ### Check Pass Codings --- In 2014, bunch of bills that passed chamber but not recorded in actions... can check using reports though (e.g., if have a house and senate report)
  # filter(bills, grepl("\\bH[A-Z]+ Committee Report", comm_reports) & grepl("\\bS[A-Z]+ Committee Report", comm_reports) & passed_chamber == 0) 
  if(bill_stages$passed_chamber == 0 & bills[i,]$status %in% c("Passed", "Vetoed", "Pocket Veto") ){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
  }
  if(t_yrs == "2013_2014"){
    if(bill_stages$passed_chamber == 0 & grepl("\\bH[A-Z]+ Committee Report", bills[i,]$comm_reports) & grepl("\\bS[A-Z]+ Committee Report", bills[i,]$comm_reports)){
      bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
    }
  }
  ### Check AIC Coding -- Need to avoid things like "fiscal impact report" 
  # table(gsub("^[A-Z][A-Z]+ ", "", unlist(str_split(bills$comm_reports, "; "))))
  if(bill_stages$action_in_comm == 0 & grepl("[A-Z][A-Z]+ (Committee Report|CR|CS)|Committee (Vote|Substitute)",  bills[i,]$comm_reports) ){
    bill_stages$action_in_comm <- 1
  }
  ### Check ABC Coding: Need to DROP reports by Senate Committee's Committee and House Rules and Order of Business Committee (Germane Rulings)
  check_reports <- gsub("(SCC|HRC) CR|(SCC|HRC) Committee Report", "", bills[i,]$comm_reports)
  if(bill_stages$action_beyond_comm == 0 & grepl("[A-Z][A-Z]+ (Committee Report|CR)", check_reports) ){
    bill_stages$action_in_comm <- 1
  }
  ### Fix 'No Actions Found' Bills --- Much of this will be fixed by the above corrections
  # filter(bill_hist, bill_id == "HB0011" & session == "2014-RS")
  if(s_id == '2001-SS2' & b_id == "HB0001"){
    bill_stages$action_in_comm <- bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
  }else if(s_id == '2007-SS1'){
    # See: https://www.nmlegis.gov/Legislation/Legislation_List --> No action on any of the senate bills...
    if(b_id %in% c("HB0001", "HB0002", "HB0003", "HB0004", "HB0005", "HB0006", "HB0008")){
      bill_stages$action_in_comm <- bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
    }else if(b_id %in% c("HB0007")){
      bill_stages$action_in_comm <- bill_stages$action_beyond_comm <- 1
    } 
  }else if(s_id == '2014-RS'){
    # HB 11 + HB302 - HB406 + SB269 - SB379
    # No AIC: HB 302-3; 307, 312-314; 317, 319-324; 326, 329, 331-2, 336, 339-41, 343-45, 347, 349-53, 357-8, 360-62, 364-72, 374-90, 392-406
    # No AIC: SB 270-74; 278, 280, 284-286, 289, 294-298, 300-303, 308-309, 311, 314, 317-18, 321, 324-26, 328-29, 332-346, 348-367, 369-376, 378
    reported <- c("HB0011", 'HB0304', "HB0306", "HB0308", "HB0309", "HB0310", 'HB0315', 'HB0316', 'HB0318', 'HB0325', 'HB0327', 
                  'HB0334', 'HB0342', 'HB0346', 'HB0354', 'HB0355', 'HB0356', 'HB0359', 'HB0373', 'HB0391',
                  'SB0269', 'SB0276', 'SB0277', 'SB0279', 'SB0281', 'SB0282', 'SB0283', 'SB0287', 'SB0288', 'SB0290', 'SB0291',
                  'SB0292', 'SB0293', 'SB0299', 'SB0305', 'SB0306', 'SB0310', 'SB0315', 'SB0316', 'SB0319', 'SB0322', 'SB0323',
                  'SB0327', 'SB0330', 'SB0331', 'SB0368', "SB0377", 'SB0379')
    if(b_id %in% reported){
      bill_stages$action_in_comm <- bill_stages$action_beyond_comm <- 1
    }else if(b_id %in% c("HB0305", 'HB0311', 'HB0333', 'HB0335', 'HB0337', 'HB0348', 'HB0363', 
                         'SB0275', 'SB0304', 'SB0312', 'SB0320')){
      bill_stages$action_in_comm <- bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
    }else if(b_id %in% c("HB0328", 'HB0330', 'HB0338', 'SB0307', 'SB0313', 'SB0347')){
      bill_stages$action_in_comm <- bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- bill_stages$law <- 1
    }
    rm(reported)
  }
  all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
  # print(i)
}
options(warn = 1)

### Check Codings
cat('\n')
all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law) ) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2)) %>% 
  as.data.frame() %>% print()

# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% filter(!grepl('^by resolution|^first read|^prefiled', tolower(action))) %>% View()
# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 1,]$bill_id) %>% View()


read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-2}_{t-1}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% print()

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-4}_{t-3}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% print()

### MERGE to BILL DATA
bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 

### MERGE In COMMEMS
all_bill_stages <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))

### MERGE In S&S
if(nrow(SS_term) > 0){
  all_bill_stages <- SS_term %>% 
    select(bill_id, term, session, SS) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term', "session")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
}else{
  all_bill_stages$SS <- 0
}

### Adjust Commems if SS == 1
table(all_bill_stages$SS, all_bill_stages$commem)
all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
rm(SS_term)

### Save Stage Info **** MERGE WITH SS **********
write.csv(all_bill_stages, glue('../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv'), row.names = FALSE )

rm(bill_hist_path, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist, aic_t, abc_t, pc_t, law_t)
rm(b_id, s_id, b_spon, bill_hist, check_reports)

####################################################
############### Identify Unique Legislators via SLER
####################################################

## Import and Clean Sponsors Name to Match
all_sponsors <- bills %>%
  mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'H', "H", "S")) %>%
  select(LES_sponsor, chamber, passed_chamber, law) %>%
  mutate(term = t_yrs) %>%
  group_by(LES_sponsor, chamber, term) %>%
  summarize(num_sponsored_bills = n(), 
            sponsor_pass_rate = sum(passed_chamber) / n(),
            sponsor_law_rate = sum(law) / n()) %>%
  ungroup()

# #### Adding Legislators Who COSPONSORED A BILL but did not SPONSOR
# unique_cospon <- str_trim(unique(unlist(str_split(bills$coauthors, '; '))))
# unique_cospon <- str_trim(gsub('\\*$', '', unique_cospon))
# for(nonspon in unique_cospon){
#   if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != '' & !is.na(nonspon)){
#     ## ****
#     chamb <- unique(substring(bills[grepl(nonspon, bills$coauthors),]$bill_id, 1, 1))
#     if("H" %in% chamb & "S" %in% chamb){
#       print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
#     }else{
#       all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0) 
#     }
#   }
# }

######## Cosponsorship Info 
all_sponsors$num_cosponsored_bills <- NA
# bills$cospon_match <- paste(bills$LES_sponsor, bills$coauthors, sep = '; ')
# for(i in 1:nrow(all_sponsors)){
#   c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
#   all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
#   ### ONLY need to adjust this way if sponsored and cosponsored column are the same
#   all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
# }
# bills <- select(bills, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))

#######################
#### CLEAN NAMES

full_names <- parse_names(all_sponsors$LES_sponsor) %>% select(-salutation)
full_names$first_name <- gsub('\\.$', '', full_names$first_name)
full_names$last_name <- gsub('\\,$', '', full_names$last_name)

# duplicates are okay here
all_sponsors <- left_join(all_sponsors, full_names, by = c("LES_sponsor" = "full_name"))
all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

rm(full_names)

#### Update First/Last Names for Matching 
if(t_yrs %in% c("1997_1998", "1999_2000", '2001_2002')){
  all_sponsors[all_sponsors$LES_sponsor == 'patsy trujillo knauer',]$last_name <-  "trujillo"
}
if(t >= 1997 & t <= 2018){
  all_sponsors[all_sponsors$LES_sponsor == 'sheryl williams stapleton',]$last_name <-  "williamsstapleton"
}
if(t >= 1997 & t <= 2004){ # In office through 2016, but name format changes
  all_sponsors[all_sponsors$LES_sponsor == 'sue wilson beffort',]$last_name <-  "wilson"
}
if(t >= 1997 & t <= 2004){
  all_sponsors[all_sponsors$LES_sponsor == 'j. paul taylor',]$first_name <-  "j. paul"
}
if(t >= 2005 & t <= 2018){
  all_sponsors[all_sponsors$LES_sponsor == 'gerald ortiz y pino',]$last_name <-  "ortizypino"
}
if(t >= 2013 & t <= 2018){
  all_sponsors[all_sponsors$LES_sponsor == 'patricia roybal caballero',]$last_name <-  "roybalcaballero"
}





all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(LES_sponsor,substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name, middle_name, suffix))

legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(t)) | startsWith(legiscan_sessions,as.character(t+1))]

legiscan = foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% select(-c(votesmart_id,knowwho_pid, ballotpedia)) %>%  distinct() 


if(t_yrs == "2019_2020") {
  legiscan = legiscan %>% mutate(district = ifelse(people_id == 6100 & role == "Rep", "HD-042", district))

}

if(t_yrs == "2021_2022"){
  legiscan = legiscan %>% filter(name != "Patricia Roybal Caballero") # some weird duplication issue
}

legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = ifelse(middle_name == "", name, paste0(first_name," ",substr(middle_name,1,1),". ",last_name))) %>%
  group_by(match_name) %>% 
  mutate(n = n()) %>% 
  mutate(match_name_chamber = tolower(paste(match_name,ifelse(substr(role,1,1)=="S","s","h"),sep="-")))


# inexact::inexact_addin()
# left: legiscan_adj
# right: all_sponsors
# method: osa
# mode: full

if(t_yrs == "2019_2020"){
  all_sponsors2 = 
    # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "left",
      custom_match = c(
        "art de la cruz-h" = NA_character_,
        "tara l. lujan-h" = NA_character_,
        "linda m. serrato-h" = NA_character_,
        "john p. woods-s" = "pat woods-s",
        "andres romero-h" = "g. andres romero-h"
      )
    )
  
}

if(t_yrs == "2021_2022"){
  all_sponsors2 = # You added custom matches:
    # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "randal s. crowder-h" = NA_character_,
        "art de la cruz-h" = NA_character_,
        "clemente sanchez-s" = NA_character_,
        "rodney d. montoya-h" = "rod montoya-h",
        "andres romero-h" = "g. andres romero-h"
      )
    )
  
  
}

#### Clean
legis_data <- all_sponsors2 %>%
  rename(data_name = LES_sponsor) %>%
  mutate(sponsor = ifelse(!is.na(name), name, str_to_title(match_name)), 
         term = t_yrs,
         chamber = substr(district,1,1)) %>%
  select(sponsor, data_name, name, klarner_id = people_id, chamber , party, district, term, num_sponsored_bills, num_cosponsored_bills, sponsor_pass_rate, sponsor_law_rate) %>%
  arrange(chamber, sponsor) %>% 
  distinct()



# now need to remove zero-LES legislators who never actually served. see documentation file on how this is generated

removal_legislators = read.csv("../../Estimate LES/Zero_LES_legislators_Coded.csv") %>% 
  filter(state == this_state & term == t_yrs & not_actually_in_chamber == T) %>% 
  mutate(chamber = substr(chamber,1,1))

if(nrow(removal_legislators) > 0){
  legis_data = anti_join(legis_data, removal_legislators,
                         by = c("klarner_id" = "legiscan_id", "chamber"))
}


########################
### Estimate Scores + Add in Relatd Variables
#########################

### Check if bills in data without an ID'd sponsor
View(filter(bills, !(bills$LES_sponsor %in% legis_data$data_name)))
bills <- bills %>% #select(-sponsor) %>%
  rename(sponsor = LES_sponsor) %>%
  mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'H', 'H', 'S'))

### Standard LES: Same as Congressional Measure
source('../../Estimate LES/calc_LES_fx.R')

LES <- calc_LES(bills, legis_data, t_yrs, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1))
summ_stats <- LES %>% group_by(chamber) %>% summarize(mean_LES = mean(LES))

### Need to use this isTRUE business otherwise will sometimes return 1 != 1 -- https://stackoverflow.com/questions/9508518/why-are-these-numbers-not-equal
if(!isTRUE(all.equal(sum(summ_stats$mean_LES), nrow(summ_stats)))){
  print("----> CHECK LES --- MEAN != 1 ---> BREAK")
  print(summ_stats)
  break
}
rm(summ_stats)
# filter(LES, LES == 0)

#### LES Without Weights + Merge
LES_noWeights <- calc_LES(bills, legis_data, t_yrs, ss_weight = 5, reg_weight = 5, com_weight = 5, stage_weights = c(1,1,1,1,1))
LES_noWeights <- rename(LES_noWeights, LES_nw = LES) %>% select(1:6, LES_nw)
LES <- left_join(LES, LES_noWeights, by = intersect(colnames(LES), colnames(LES_noWeights)))
rm(LES_noWeights)

### Fix Term Variable
LES <- rename(LES, term = session)

#### Merge Agg Stats back in
LES <- legis_data %>%
  select(sponsor, chamber, party,district,  num_sponsored_bills, sponsor_pass_rate, sponsor_law_rate, num_cosponsored_bills) %>%
  mutate(chamber = ifelse(chamber == "H", "House", "Senate")) %>%
  left_join(LES, ., by = c("sponsor", "chamber")) %>%
  select(-klarner_name) %>% 
  rename(legiscan_id = klarner_id) %>%
  select(sponsor, data_name, legiscan_id, term, chamber, district, party, LES, everything())

#### If LES == 0 and --- , "num_cosponsored_bills"
LES[LES$LES == 0 & is.na(LES$num_sponsored_bills), c('num_sponsored_bills', 'sponsor_pass_rate', 'sponsor_law_rate')] <- 0

############### Save LES
write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
rm(LES)


cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************"))


################# ****** END LOOP


rm(all_sponsors, bills, legis_data, SS_bills, H_elec_year, S_elec_year, i, keep_types)
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES) # 
rm(t, terms, klarner_gs, m_sub, commem_bills, t_sessions) # 


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by County Board/Governor --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
# ---> County Board proposes N individuals; governor picks one... Wonder what the consequences of this are???
########################################################################################################################
### FULL ROSTER: https://www.nmlegis.gov/Members/Former_Legislator_List
#########################


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1 bill(s) without a sponsor
#### ***** ALL NAMES FIXED *******

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
### IN CHAMBER:
# -- hanosh, george joseph


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- joseph cervantes
# -- marsha c. atkin -- may have won in general?
#### DROP:
# -- moran, james l. -- No evidence he was seated: marsha atkin recorded as holding seat that term: https://www.nmlegis.gov/Members/Former_Legislator_Districts?Chamber=H&District=60


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- richard p. cheney
# -- jim r. trujillo (name duplicate, won't print)
### APPOINTED ~ SENATE:
# -- clinton d. harden, jr. -- 2002
# -- gay g. kernan
# -- raymond kysar
#### DROP:
# -- lyons, patrick h. -- Left office in 2002: nmlegis.gov/Members/Former_Legislator?SponCode=SLYON
# -- bailey, shirley m. -- Left office in 2002: https://www.nmlegis.gov/Members/Former_Legislator?SponCode=SBAIL
# -- trujillo, patsy g. -- Resigned 1/6/2003: https://www.nmlegis.gov/Members/Former_Legislator?SponCode=HKNAU

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- william r. rehm
### APPOINTED ~ SENATE:
# -- garcia, thomas a.


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- john pena (not elected again)
# -- rodolpho s. martinez (last name duplicated, won't print)
### APPOINTED ~ SENATE:
# -- david ulibarri
# -- howie c. morales
# -- lynda m. lovejoy
#### DROP:
# -- tsosie, leonard -- Left office in 2007: https://www.nmlegis.gov/Members/Former_Legislator?SponCode=STSOS
# -- fidel, joseph -- Left office in 2006: https://www.nmlegis.gov/Members/Former_Legislator?SponCode=SFIDE


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- zachary j. cook
### IN HOUSE:
# -- gray, william j.
# -- bratton, donald e.
#### DROP:
# -- williams, dub -- resigned Jan 14 2009 -- https://archive.is/20141021203333/http://www.alamogordonews.com/ci_11446766


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- bob wooley
# -- jim w. hall (last name duplicated, won't print, should match to james hall in klarner)
### APPOINTED ~ SENATE:
# -- lisa curtis
# -- william f. burt
### IN HOUSE:
# -- chavez, ernest h.
# -- gray, william j.
# -- tyler, shirley a.
#### DROP:
# -- gardner, keith -- Left office in 2010: https://www.nmlegis.gov/Members/Former_Legislator?SponCode=HGARD
# -- duran, dianna j. -- Left office in 2010: https://www.nmlegis.gov/Members/Former_Legislator?SponCode=SDURA


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
#   session chamber   N AIC ABC PASS LAW
# 1 2013-RS       H 675 495 497  263  91
# 2 2013-RS       S 642 490 491  210 137
# 3 2014-RS       H 406 219 223   48  40
# 4 2014-RS       S 379 280 280   51  41
### APPOINTED ~ HOUSE:
# -- vickie perea
### IN HOUSE: 
# -- gray, william j.
### Fixed:
# -- terry h. mcmillan won election on recount; error in klarner

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
#   session chamber   N AIC ABC PASS LAW
# 1  2015-RS       H 633 424 431  240  71
# 2  2015-RS       S 726 498 499  196  87
# 3 2015-SS1       H   2   2   2    2   2
# 4 2015-SS1       S   1   1   1    1   1
# 5  2016-RS       H 371 200 216   99  45
# 6  2016-RS       S 344 245 245   64  47
# 7 2016-SS2       H   9   5   5    3   0
# 8 2016-SS2       S  12  11  11   11   7
### APPOINTED ~ HOUSE:
# -- idalia lechuga-tena
# -- stephanie maez (appointed 12/16/2014, resigned 11.5.2015)
### APPOINTED ~ SENATE:
# -- mimi stewart (via H)
# -- ted barela
#### DROP:
# -- mimi stewart IN HOUSE -- appointed to Senate in December after winning relection to House
# -- keller, timothy m. -- Left office in 2014: https://www.nmlegis.gov/Members/Former_Legislator?SponCode=SKELL


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
#   session chamber   N AIC ABC PASS LAW
# 1  2017-RS       H 577 369 378  228  81
# 2  2017-RS       S 538 354 355  206  66
# 3 2017-SS1       H   8   2   2    2   2
# 4 2017-SS1       S   8   4   4    2   1
# 5  2018-RS       H 370 166 208   95  43
# 6  2018-RS       S 317 200 200   61  37
### APPOINTED ~ HOUSE:
# -- gail armstrong -- appointed 1/17/2017 -- name duplicate, won't print!
#### DROP:
# -- tripp, don -- Left office in 2016: https://www.nmlegis.gov/Members/Former_Legislator?SponCode=HTRIP




# filter(klarner, grepl("taylor,", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 60 & sen == 0 & year == 2000) %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)
# filter(bills, grepl("ford", coauthors) & substring(bill_id,1,1) == 'S')



##########################################################################################################
##########################################################################################################
##### MATCHING MISSING (SPECIALS) TO KLARNER USING OTHER TERMS WHERE SPONSOR IS PRESENT
##########################################################################################################

### Re-Load LES Data
LES_paths <- list.files('.', full.names = TRUE)
LES_paths <- LES_paths[!grepl('Archive|_All|Merged', LES_paths)]

LES <- LES_paths %>%
  lapply(read_csv, col_types = cols()) %>%
  bind_rows 

rm(LES_paths)


####### Fill in Missing Data from Candidates Elected in Specials using Subsequent Observations
missing <- unique(LES[is.na(LES$klarner_id),]$sponsor)
for(name in missing){
  name_sub <- filter(LES, grepl(glue("^{name},"), sponsor)); exact = TRUE
  if(nrow(name_sub) == 0){
    name_sub <- filter(LES, grepl(glue("^{name}"), sponsor)) 
    exact <- FALSE
  }
  # If there is only ONE UNIQUE id that matches the name
  if(any(!is.na(name_sub$klarner_id)) & length(unique(na.omit(name_sub$klarner_id))) == 1 ){
    LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$sponsor <- unique(name_sub[!is.na(name_sub$klarner_id),]$sponsor) 
    LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$klarner_name <- unique(na.omit(name_sub$klarner_name))
    LES[grepl(name, LES$sponsor) & is.na(LES$klarner_id),]$klarner_id <- unique(na.omit(name_sub$klarner_id))   
    print(glue(' ~~ {name} ~~ Matched to --> {unique(na.omit(name_sub$klarner_name))}'))
  } else {
    if(exact == TRUE){
      k_sub <- filter(klarner, grepl(paste0('^', name, ','), cand))
    }else{
      k_sub <- filter(klarner, grepl(name, cand))  
    }
    if(length(unique(k_sub$cand)) == 1 & length(unique(k_sub$candid)) == 1){
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$sponsor <- unique(k_sub$cand) 
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$klarner_name <- unique(k_sub$cand) 
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_id),]$klarner_id <- unique(k_sub$candid) 
      print(glue(' ~~ {name} ~~ Matched to --> {unique(k_sub$cand) }'))  
    }
  }
}

### Fix Errors! Jim W. Hall != Jimmie C. Hall
LES[LES$data_name %in% "jim w. hall" & LES$term == "2011_2012",]$klarner_id <- NA
LES[LES$data_name %in% "jim w. hall" & LES$term == "2011_2012",]$klarner_name <- NA
LES[LES$data_name %in% "jim w. hall" & LES$term == "2011_2012",]$sponsor <- 'hall, jim w.'

### ****Still missing***** ---> Rest are not in Klarner or 2017-2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[3]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl('hall', cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, missing, still_missing)


### Fix Missing
name_matches <- data.frame(LES_name = 'cook, zachary', k_name = 'cook, zach j.')
name_matches <- add_row(name_matches, LES_name = 'hall, jim w.', k_name = 'hall, james w.')
name_matches <- add_row(name_matches, LES_name = 'barela, ted', k_name = 'barela, theodore (ted)')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}
rm(name_matches, i)


############## Check for Duplicates
### ---> If two people with same last name and one won in a special, will lead to duplicates
### Remaining = Switched from House to Senate
for(t in unique(LES$term)){
  print(glue(' ************ {t} ***************'))
  check_dup <- filter(LES, term == t) %>%
    group_by(chamber) %>%
    mutate(dup = duplicated(klarner_id) & !is.na(klarner_id)) 
  if(any(check_dup$dup)){
    filter(LES, term == t & klarner_id %in% check_dup[check_dup$dup == TRUE,]$klarner_id & !is.na(klarner_id)) %>%
      select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>%
      print()
  }
}
rm(check_dup, k_sub, exact, name_sub)

########################################################################################################################################################
########################################################################################################################################################
######## AGGREGATE AND MATCH TO EXTERNAL
########################################################################################################################################################
########################################################################################################################################################

### Klarner State Leg. Election Data --- Using Adjusted File From Top
klarner_sub <- filter(klarner, sab == this_state & year >= min_year - 5 & outcome == 'w')
klarner_sub <- select(klarner_sub, caseid, year, sen, ddez, dno, term, termz, cando, cand, candid, partyz, partyt, exper, outcome, etype)

#### KLARNER IS YEAR OF ELECTION, Not TERM
LES$exper <- LES$party <- LES$district <- NA
LES$district <- as.double(LES$district)
LES$party <- as.character(LES$party)
LES$exper <- as.character(LES$exper)

for(name in unique(LES$sponsor)){
  this_sponsor_LES <- LES[LES$sponsor == name,]
  sponsor_rows <- filter(klarner_sub, candid %in% na.omit(this_sponsor_LES$klarner_id ))
  if(nrow(sponsor_rows) == 0){
    ### Check Losers
    sponsor_rows <- filter(klarner, candid %in% na.omit(this_sponsor_LES$klarner_id ))
    if(nrow(sponsor_rows) >= 1){
      LES[LES$sponsor == name,]$party <- sponsor_rows[1,]$partyz
    }
  } else{
    for(t in this_sponsor_LES$term){
      second_year <- as.numeric(str_split(t, "_")[[1]][2])
      ### Filling in by chamber to account for people who switch chambers mid-term
      for(c in this_sponsor_LES[this_sponsor_LES$term == t,]$chamber){
        sponsor_sub <- filter(sponsor_rows, (etype %in% spec_elec_codes & year == second_year ) | year < second_year )
        sponsor_sub <- filter(sponsor_sub, sen == ifelse(c == "Senate", 1, 0))
        if(nrow(sponsor_sub) > 0 ){
          sponsor_sub <- arrange(sponsor_sub, desc(year))
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$district <- sponsor_sub[1,]$dno
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$party <- sponsor_sub[1,]$partyz
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$exper <- sponsor_sub[1,]$exper
        }else{
          if(nrow(sponsor_rows) > 0){
            sponsor_rows <- arrange(sponsor_rows, year)
            LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$party <- ifelse(is.logical(na.omit(unique(sponsor_rows$partyz))), NA, na.omit(unique(sponsor_rows$partyz))[1] )
            #LES[LES$sponsor == name & LES$term == s & LES$chamber == c,]$exper <- ifelse(is.logical(na.omit(unique(sponsor_rows$exper))), NA, na.omit(unique(sponsor_rows$exper))[1] )
          }
        }
      }
    }
  }
  # print(name)
}

###### IF MISSING, CHECK ELCTION LOSERS
# filter(LES, is.na(party)) %>% select(sponsor, term, klarner_id, district, party, exper)
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

### Not In Klarner
fill_missing <- data.frame(LES_name = "pena, john", new_name = 'pena, john', party = 'd', district = 5, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "maez, stephanie", new_name = 'maez, stephanie', party = 'd', district = 21, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "lechuga-tena, idalia", new_name = 'lechuga-tena, idalia', party = 'd', district = 21, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

#### *** 2017-2018 Appointees: Won't be needed once klarner updates ****
LES[LES$sponsor == "armstrong, gail", c('party', 'sponsor')] <- list('r', "armstrong, gail")
# LES[LES$sponsor == "zzzzzzzz", c('party', 'sponsor')] <- c('zzzzz', "zzzzzzz")

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t, fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

#### Clean Names
# filter(LES, grepl("fox", sponsor)) %>% select(1:7)
LES[LES$sponsor == 'williams, dub',]$sponsor <- "williams, walter c."
LES[LES$data_name %in% 'justine fox-young',]$sponsor <- "fox-young, justine"
LES[LES$data_name %in% 'sue wilson beffort',]$sponsor <- "beffort, sue wilson"


#########################################################
############ Match to Hall/Fouirnaies
########################################################

library(readstata13)

### Hall and Fournaies 2018, Covers 1988 - 2014
### --> https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/PGCVDP
hf_data <- read.dta13("~/Dropbox/Data/Hall_Fouirnaies_2018/How_Do_interest_Groups_Seek_Access_to_Committees.dta")
hf_data <- filter(hf_data, state == this_state)
hf_data <- hf_data[,c(colnames(hf_data)[c(1:3, 7:14, 204, 208, 217)], colnames(hf_data)[grepl('cmt_', colnames(hf_data))])]
hf_data$term <- paste0(hf_data$year + 1, "_", hf_data$year + 2)
hf_data$chamber <- ifelse(hf_data$chamber == 'senate', 'Senate', "House")

#### Doubling the Senate Rows + Adding back in
# **** ALL data should be right (assuming still in chamber) EXCEPT Majority Member IN STAGGERED STATES ********
senate <- filter(hf_data, chamber == "Senate")
senate$year <- senate$year + 2
senate$term <- paste0(senate$year + 1, "_", senate$year + 2)
senate$MajorityMember <- NA
hf_data <- bind_rows(hf_data, senate) %>% arrange(year, chamber)
rm(senate)

### Subset
hf_data <- filter(hf_data, year > min_year - 4) %>% distinct() %>% select(-MajorityMember)

### Merge
LES <- left_join(LES, select(hf_data, -year), by = c('term' = 'term', 'chamber' = 'chamber', 'klarner_id' = 'CandId'))

### Check Duplicates
mutate(LES, check_dup = paste(sponsor, term, chamber, sep = "--")) %>%
  mutate(dup = duplicated(check_dup)) %>%
  filter(dup == TRUE) #%>% View()

### Set Committees to NA for Years without Data -- May have matched candids in year range
set_NA <- colnames(hf_data)
set_NA <- set_NA[!(set_NA %in% c("CandId", 'chamber', "year", "term")) ]
LES[LES$term == '2017_2018', set_NA] <- NA
rm(hf_data, set_NA)


#################################
### Shor and McCarty Data, 1993 - 2016
####################################

# ***  NEW MEXICO: Decent number of 2009_2010 missing... 
# *** NOTE: Patricia Roybal Caballero is split over two rows (on Caballero, one Cabellero)

ideo <- readstata13::read.dta13("~/Dropbox/Data/Shor_McCarty_Data/shor_mccarty_1993_2016_individual_data_May_2018.dta")
ideo <- filter(ideo, st == this_state) %>% select(-st_id)
ideo$match_name <- str_extract(tolower(ideo$name), '[a-z]+, [a-z]+')
ideo$match_name <- ifelse(is.na(ideo$match_name), tolower(ideo$name), ideo$match_name)
ideo$last_name <- gsub(',.+', '', tolower(ideo$name))

#### Doing this row by row to more easily account for party, unique data_names, etc.
#### Starting with MT (May 7, 2019) this now cross-checks to make sure it doesn't match on last name if multiple smiths, for example.
LES$SM_name <- LES$SM_party <- LES$np_score <- NA
LES$SM_name <- as.character(LES$SM_name)
LES$SM_party <- as.character(LES$SM_party)
LES$np_score <- as.double(LES$np_score)

for(i in 1:nrow(LES)){
  ####### **** CHECK LAST NAME + ACCOUNT FOR VARIATIONS IF NO MATCH *******
  check_last <- which(gsub(",.+|\\'", '', LES[i,]$sponsor) == tolower(ideo$last_name) )
  ### if none, adjust name
  if(length(check_last) == 0){
    check_last <- which(gsub(",.+|\\'| |-", '', LES[i,]$sponsor) == gsub(" |\\'|-", '', tolower(ideo$last_name) ))
  }
  ## If Still None, Try Data Name
  if(length(check_last) == 0){
    d_name <- str_split(LES[i,]$data_name, " ")[[1]]
    check_last <- which(d_name[length(d_name)] == tolower(ideo$last_name) )
  }
  
  ####### ***** IF MORE THAN ONE MATCH *******
  if(length(check_last) > 1){
    ## Check First initial if more than 1 last name match
    ideo_match <- ideo[check_last,][substring(gsub('.+, ', '', tolower(ideo[check_last,]$name)), 1, 1) == substring(gsub(".+, ", '', LES[i,]$sponsor), 1, 1),]
    ### Check Party if Still Too Long
    if(nrow(ideo_match) > 1){
      ideo_match <- filter(ideo_match, party == toupper(LES[i,]$party))  
    }
    ### Check Last + First Name
    if(nrow(ideo_match) > 1){
      ideo_match <- filter(ideo_match, match_name == gsub(' [a-z]\\.$', '', LES[i,]$sponsor) )    
    }
    ##### ****** IF ONE LAST NAME MATCH - VERIFY NOT IDENTICAL LAST NAMES *********
  } else if(length(check_last) == 1){
    #### CHECK IF ANY OTHER LEGISLATORS WITH SAME LAST NAME
    lastname <- gsub(",.+|\\'", '', LES[i,]$sponsor)
    num_with_same_last <- filter(LES, grepl(glue('^{lastname},'), sponsor)) %>% select(sponsor) %>% unlist() %>% unique()
    if(length(num_with_same_last) > 1){
      ### Check FUll Name
      if(grepl(ideo[check_last,]$match_name, LES[i,]$sponsor)){
        ideo_match <- ideo[check_last,]   
      } else{
        ## Set to 0 rows
        ideo_match <- filter(ideo, match_name == 'zzzz')
      }
    } else {
      ideo_match <- ideo[check_last,]  
    }
  } else{
    # Set to 0 rows if no match
    ideo_match <- filter(ideo, match_name == 'zzzz')
  }
  ### Save if ONE MATCH After whole process
  if(nrow(ideo_match) == 1){
    LES[LES$sponsor == LES[i,]$sponsor,]$SM_name <- ideo_match$name
    LES[LES$sponsor == LES[i,]$sponsor,]$SM_party <- ideo_match$party
    LES[LES$sponsor == LES[i,]$sponsor,]$np_score <- ideo_match$np_score
    #cat(" \n Manually matched ", toupper(LES[i,]$sponsor), " to ", toupper(ideo_match$name), "\n .")
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

#### FIX MISMATCHES
# --> Greg Baca elected in 2017; BACA match is for 1996 and earlier + wrong chamber/party
LES[LES$sponsor %in% c('chavez, eleanor', 'baca, gregory a.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches in Which LES First Name != Shor-McCarty First Name
# ---> NO ISSUES
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1991_1992', '2017_2018')) ) %>% group_by(sponsor, party) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('chavez', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

### Lot of the missing are special election winners in 2016+
name_matches <- data.frame(LES_name = 'barnes, sarah maestas', SM_name = 'Maestas Barnes, Sarah')
name_matches <- add_row(name_matches, LES_name = 'beffort, sue wilson', SM_name = 'Wilson Beffort, Sue')
#name_matches <- add_row(name_matches, LES_name = 'chavez, eleanor', SM_name = 'zzzzzzz') # Could be name mix=up? ****** Not ^Chavez$
# name_matches <- add_row(name_matches, LES_name = 'curtis, lisa k.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'garcia, mary helen', SM_name = 'Garcia, Mary Helen')
name_matches <- add_row(name_matches, LES_name = 'garcia, mary jane m.', SM_name = 'Garcia, Mary Jane')
# name_matches <- add_row(name_matches, LES_name = 'giannini, karen e.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'hall, james w.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'king, gary k.', SM_name = 'King')
name_matches <- add_row(name_matches, LES_name = 'larranaga, lorenzo a.', SM_name = 'Larrañaga, Larry A.')
# name_matches <- add_row(name_matches, LES_name = 'lechuga-tena, idalia', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'ortizypino, gerald p.', SM_name = 'Ortiz Pino, Gerald')
name_matches <- add_row(name_matches, LES_name = 'richard, stephanie m.', SM_name = 'Garcia Richard, Stephanie')
# name_matches <- add_row(name_matches, LES_name = 'rodefer, benjamin hayden', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'romero, g. andres', SM_name = 'Romero, Andres')
name_matches <- add_row(name_matches, LES_name = 'taylor, j. paul', SM_name = 'Taylor, John Paul')
name_matches <- add_row(name_matches, LES_name = 'taylor, thomas c.', SM_name = 'Thomas C. Taylor') # **** Name backwards
# name_matches <- add_row(name_matches, LES_name = 'thomas, jack e.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'urioste, mario', SM_name = 'Urosite')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

##########
### MORE DETAILED FIXES
##########

#### Ted Barela (R) incorrectly listed as Elias Barela (D) [Note that Elias Barela is correctly matched to the D observation]
LES[LES$sponsor == 'barela, theodore (ted)',]$SM_name  <- ideo[ideo$name == 'Barela, Elias' & ideo$party %in% 'R',]$name
LES[LES$sponsor == 'barela, theodore (ted)',]$SM_party <- ideo[ideo$name == 'Barela, Elias' & ideo$party %in% 'R',]$party
LES[LES$sponsor == 'barela, theodore (ted)',]$np_score <- ideo[ideo$name == 'Barela, Elias' & ideo$party %in% 'R',]$np_score


########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########
# filter(LES, grepl("baca", sponsor)) %>% select(1:7, party)
# filter(ideo, grepl('baca', tolower(name)))

#### Andrew Nunez --- D to I to R
LES[LES$sponsor == 'nunez, andrew' & LES$term == '2011_2012',]$party <- 'i' # Switched in January 2011
LES[LES$sponsor == 'nunez, andrew' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Nuñez, Andrew' & ideo$party == 'D',]$name
LES[LES$sponsor == 'nunez, andrew' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Nuñez, Andrew' & ideo$party == 'D',]$party
LES[LES$sponsor == 'nunez, andrew' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Nuñez, Andrew' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'nunez, andrew' & LES$party == 'i',]$SM_name <-  ideo[ideo$name == 'Nuñez, Andrew' & ideo$party == 'X',]$name
LES[LES$sponsor == 'nunez, andrew' & LES$party == 'i',]$SM_party <- ideo[ideo$name == 'Nuñez, Andrew' & ideo$party == 'X',]$party
LES[LES$sponsor == 'nunez, andrew' & LES$party == 'i',]$np_score <- ideo[ideo$name == 'Nuñez, Andrew' & ideo$party == 'X',]$np_score
LES[LES$sponsor == 'nunez, andrew' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Nunez, Andrew' & ideo$party == 'R',]$name
LES[LES$sponsor == 'nunez, andrew' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Nunez, Andrew' & ideo$party == 'R',]$party
LES[LES$sponsor == 'nunez, andrew' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Nunez, Andrew' & ideo$party == 'R',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

##### Drop Nicknames
# filter(LES, grepl('\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)
LES[LES$sponsor == "watchman, leo 2",]$sponsor <- 'watchman, leo c.'

#### Manual Fixes
LES[LES$sponsor == 'trujillo, patsy g.',]$sponsor <- 'knauer, patsy trujillo'
LES[LES$sponsor == 'kidd, melvin d.',]$sponsor <- 'kidd, melvin don'
LES[LES$sponsor == 'martinez, w. ken',]$sponsor <- 'martinez, walter ken'
LES[LES$sponsor == 'cook, zach j.',]$sponsor <- 'cook, zachary j.'

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1997 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2014, 2017:2020) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2015:2016) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1997 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
#LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzz) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


##############################################
###  Save
##############################################

### Check Missingnes
LES %>%
  group_by(chamber, term) %>%
  summarize(
    Election_Data_Missing = round(sum(is.na(klarner_id))/n(), 2),
    CM_Leadership_Missing = round(sum(is.na(SpeakerHouse))/n(), 2),
    CM_Totals_Missing = round(sum(is.na(cmt_number))/n(), 2),
    Ideology_Missing = round(sum(is.na(np_score))/n(), 2)) %>%
  as.data.frame()

###### SAVE Merged File
colnames(LES)
if(!dir.exists("Merged")){dir.create("Merged")}
write.csv(LES, glue("Merged/{this_state}_LES_All_M.csv"), row.names = FALSE)

### Save by Session
for(t in unique(LES$term)){
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
  summarize(mean_LES = mean(LES),
            max_LES = max(LES)) # %>% View()

#### Should split this by chamber
LES %>%
  mutate(t_factor = fct_rev(as.factor(gsub('_', '-', term) ))) %>% 
  filter(party %in% c("d", "r")) %>%
  ggplot(aes(y = t_factor)) +
  geom_density_ridges(aes(x = LES, fill = party), alpha = .8, color = "white", from = 0, to = 5) +
  xlab("LES") + 
  ylab("Term") + 
  ggtitle(glue("Legislative Effectiveness in {this_state}")) +
  scale_fill_cyclical(
    breaks = c("d", "r"),
    labels = c('d' = "Democrat", 'r' = "Republican"),
    values = c("dodgerblue2", "red2"),
    name = "Party", guide = "legend") +
  theme_ridges(grid = FALSE) + 
  facet_wrap(~chamber) + 
  theme(axis.title.x = element_text(hjust = 0.5),axis.title.y = element_text(hjust = 0.5), legend.position = "bottom")

ggsave(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Figures/{this_state}_LES_ridgeplot.pdf"), height = 8, width = 7)

#########
ggplot2::ggplot(LES, aes(x = np_score, y = LES, color = party)) + 
  geom_point() + 
  geom_smooth(formula = "y ~ x", method = 'lm', se = FALSE) + 
  facet_wrap(~ term) + 
  scale_color_manual(values=c("dodgerblue2", 'gray50', "red2"))

##### CHECK OUTLIERS with mismatched parties
# filter(LES, party == 'd' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + in_majority + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

