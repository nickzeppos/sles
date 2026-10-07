

#####################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** NEVADA *** BY SESSION
#####################################


# *****************************************************************
########### ************** TO FIGURE OUT ***************************
# *****************************************************************
# (1) How to deal with committee sponsored bills, including bills jointly sponsored by TWO committees? 
# ---> What about committee of the whole??? Or 1995-1996 when House split control --> One D, One R chair of each committee....
# ---> Some committee (and non-comm) bills are introduced by request; in 95/96 can only identify them from actions
## *********** Script CAN attribute commitee bills to chair BUT currently dropping committee bills as not clear that's right
## _------------------> NEED TO FIGURE OUT HOW TO ADDRESS THESE
## -------------------> COULD SCRAPE AND USE TWHE BILL DRAFT REQUESTS TO NARROW SOME DOWN (many are by req or comm but some are legislators)
## **** NOTE: If re-scrape, need to fix early year cosponsor vars and prevent duplicates from Bill Versions


###################################
## SPECIAL SESSIONS:
## ---- Seperate files; bills can only be introduced by limited set of actors
## MEMBER LISTS:
## ---- 1861+ : https://www.leg.state.nv.us/dbtw-wpd/LegSim.htm
## ---- 2011+ : https://www.leg.state.nv.us/App/NELIS/REL/76th2011
## PROCESS/RULES:
## ---- ASSEMBLY RULES: https://www.leg.state.nv.us/Session/79th2017/Docs/SR_Assembly.pdf
## ---- SENATE RULES: https://www.leg.state.nv.us/Session/79th2017/Docs/SR_Senate.pdf
## Sponsorship/Authorship
## ---- Primary sponsor = First listed; Multiple sponsors permitted
## ---- COMMITTEE SPONSORED BILLS permitted
## COMMITTEE BILLS:
## -- Compiled List Needed via script + handcoded from: https://www.leg.state.nv.us/Session/ --> Committee Minutes
###########################
## NOTES:
## (1) Bill drat requests available in PDF in 1995-96: https://www.leg.state.nv.us/Session/68th1995/ ----> https://www.leg.state.nv.us/Session/68th1995/1995%20FINAL%20BDR%20LIST.pdf
## ---> Scrapable thereafter, with format change in 2011 -- But less info than Montana (e.g., no attorney)
## (2) Bill Introductions significantly limited in Specials -- Basically only the speaker and committees. OK? (See rule 42, house rules)
## (3) Postponed in Committeed as Evidence of AIC -------> NO
## (4) BILLS CAN CARRYOVER ACROSS GA's -- e.g., bill from 68th might get carried over to 69th 
## --> Not common, but will have session number appended as (e.g., "_68") or a Star at the end
## --> See: https://www.leg.state.nv.us/Session/68th1995/reports/HistoryLibraryNELIS.cfm?SessionNumber=Nelis_95R&DocumentType=SB&BillNo=501
## --> ANd: https://www.leg.state.nv.us/Session/69th1997/tracking/Detail.cfm?dbo_in_intro__introID=271
## -----> Dropping for now... long term, should check Term T + 1 to update existing statuses...? 
## -----> Problem is they seem to lack prior records and pick up where other left off (.e.g, straight to veto sustained???)
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

this_state <- 'NV'
keep_types <- c("AB", "SB")

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
commem_bills <- commem_bills %>%
  filter(!grepl("\\_[0-9]+$|\\*", bill_id)) %>% 
  mutate(bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = 0)))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- bind_rows(read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t_plus_one}.csv"),colClasses = c("character")))

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
# t <- terms[10]



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
bills$term <- t_yrs
bills$term_num <- bills$session
bills$session_type <- recode(bills$session_type, 'R' = 'RS', 'S' = 'SS', 'S2' = 'SS2')
bills$session <- paste(bills$session_year, bills$session_type, sep = '-')
bills <- select(bills, -c(session_year, session_type))

### Eliminate duplicates -- If engrossd, 2 obs... if N versions of bill, N obs -- Saving as var
# ---> This appears to be just a 1996 thing... So removing...
# bills <- bills %>% group_by(bill_number, session) %>% mutate(num_versions = n()) %>% ungroup() %>% distinct()
bills <- distinct(bills)

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_number) 
all_bills <- bills

### Dropping Bills Carried Over from past term
# --> See Q4 at top: these are not reintroduced but finishing action, often post-veto
# --> Also denoted by, eg, AB123*
if(any(grepl("_[0-9]+$|[0-9]+\\*", bills$bill_id))){
  num_carryover <- sum(grepl("_[0-9]+$|[0-9]+\\*", bills$bill_id))
  cat('\n')
  cat(glue('---> Dropping {num_carryover} Bills That Are Carried Over to Finish Business'))
  bills <- filter(bills, !grepl("_[0-9]+$|[0-9]+\\*", bill_id))
  rm(num_carryover)
}

#### Standardize Bill Numbers
bills <- mutate(bills, bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = 0)))

############### Drop Resolutions, Messages, Communications, Reports
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)

##########################
####### Standardize Sponsors

#### Standardize -- Format changes -- Can't get full cosponsor list after 2009_2010 Term
if(t >= 2018){
  bills$primary_sponsors = sub("^View \\d+ Primary Sponsors Close Primary Sponsors; ", "", bills$primary_sponsors)
}
if(t < 2011){
  bills$sponsors <- gsub(';$', '', gsub('; ;', ';', tolower(bills$sponsors)))
  bills$sponsors <- gsub('á', 'a', bills$sponsors)
  bills$sponsors <- gsub('é', 'e', bills$sponsors)
  bills$sponsors <- gsub('ó', 'o', bills$sponsors)
  bills$sponsors <- gsub('í', 'i', bills$sponsors)
  bills$sponsors <- gsub('ñ', 'n', bills$sponsors)
  #### Primary Sponsor = 1st Sponsor
  bills$LES_sponsor <- gsub(';.+', '', bills$sponsors)
}else{
  bills$primary_sponsors <- gsub(';$', '', gsub('; ;', ';', tolower(bills$primary_sponsors)))
  bills$primary_sponsors <- gsub('á', 'a', bills$primary_sponsors)
  bills$primary_sponsors <- gsub('é', 'e', bills$primary_sponsors)
  bills$primary_sponsors <- gsub('ó', 'o', bills$primary_sponsors)
  bills$primary_sponsors <- gsub('í', 'i', bills$primary_sponsors)
  bills$primary_sponsors <- gsub('ñ', 'n', bills$primary_sponsors)
  bills$LES_sponsor <- gsub('^senator |^assemblyman |^assemblywoman ', '', gsub(';.+', '', bills$primary_sponsors))
  table(bills$LES_sponsor)
}

#### Adjusting for Bills By Request -- Need to do BEFORE splitting
if(any(grepl("\\(by request\\)|\\(by re\\)| +request", bills$LES_sponsor))){
  #print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
  num_br <- nrow(filter(bills, grepl('\\(by request\\)|\\(by re\\)| +request', LES_sponsor)))
  cat('\n')
  cat(glue("-----> KEEPING {num_br} bill(s) introduced BY REQUEST"))
  bills$LES_sponsor <- str_trim(gsub('\\(by request\\)|\\(by re\\)| +request', '', bills$LES_sponsor))
  rm(num_br)
}


#### Manual Name Fixes
if(t_yrs == "2011_2012"){
  bills$LES_sponsor <- gsub('richard \\(skip\\) daly', 'richard daly', bills$LES_sponsor)
  bills$cosponsors <- gsub('richard \\(skip\\) daly', 'richard daly', tolower(bills$cosponsors))
}
if(t_yrs == "2019_2020"){
  bills$LES_sponsor[bills$bill_id=="SB0389"] = "keith pickard" # the scraper somehow didn't get the senate sponsors here
}

#### Fill in Missing Sponsors with Full Sponsor List
# bills$LES_sponsor <- ifelse(bills$LES_sponsor == "", gsub(';.+', '', bills$coauthors), bills$LES_sponsor)



###################
###### Merge in S&S Bills
###################
# *** For NEVADA: Bills carry over during regular (one biennium), but numbers re-start for all special sessions
# ---> Need to merge on Id and Session 
# ---> Capping Bill numbers at SESSION MAX and assuming bill is from SS with most bills proposed

if(t_yrs == "2019_2020"){
  SS_bills$bill_id[SS_bills$bill_id=="SB30003"]="SB0003"
  SS_bills$bill_id[SS_bills$bill_id=="SB40004"]="SB0004"
  SS_bills$bill_id[SS_bills$bill_id=="SB20002"]="SB0002"
  SS_bills$bill_id[SS_bills$bill_id=="AB40004"]="AB0004"
  SS_bills$bill_id[SS_bills$bill_id=="SB10001"]="SB0001"
  SS_bills$bill_id[SS_bills$bill_id=="AB30003"]="AB0003"
} 

if(t_yrs == "2021_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="SB10001"]="SB0001"
  SS_bills$bill_id[SS_bills$bill_id=="SB60006"]="SB0006"
} 

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, year, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS, year,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,summary), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",summary, ignore.case=T)) %>%
  arrange(summary) 
unique(missing_SS_bills$bill_id) 



# now check to see if there are duplicate joins


duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills %>% mutate(year = as.integer(substr(session,1,4))), 
            by = c("bill_id", "term", "year")) %>%
  group_by(rowname) %>% mutate(count = n()) %>%
  ungroup() %>% 
  select(count,bill_id,term,session, Title, summary) %>% 
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

############################################
############### Code Commemorative
############################################

bills <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session'))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)


#### List of Committees to Re-code
comms <- c('commerce', 'commerce and labor', 'education', 'education/procedures', 'finance', 'government affairs', 'health and human ser',
           'human res and fac', 'judiciary', 'labor and management', 'leg affairs and oper', 'nat res agri & min', 'natural resources',
           'taxation', 'transportation', 'ways and means', 'natural resources, agriculture, and mining', 'legislative affairs and operations',
           'human resources and facilities', 'infrastructure', 'health and human services', 'electons, procedures, and ethics', 'elections',
           'elections/procedures', 'elections, procedures, and ethics', 'elections procedures and ethics', 'committee of the whole',
           'elections procedures ethics and constitutional amendments', 'growth and infrastructure', 'human resources and education',
           'natural resources agriculture and mining', 'state revenue and education funding',   'constitutional amendments', 'energy',
           'joint rules', 'legislative operations and elections', 'transportation and homeland security', 'health and education',
           'select committee on corrections parole and probation', 'corrections parole and probation', 'energy infrastructure and transportation')

#### Uncomment to Compile List of Committees by chamber and term
# if(nrow(filter(bills, LES_sponsor %in% comms)) > 0 | any(grepl('committee', bills$LES_sponsor)) ){
#   these_comms <- filter(bills, LES_sponsor %in% comms | grepl('committee', LES_sponsor)) %>%
#     mutate(term = t_yrs, chamber = substring(bill_id, 1, 1)) %>%
#     group_by(term, chamber, LES_sponsor) %>%
#     summarize(chair = NA, n = n()) %>%
#     rename(committee = LES_sponsor, N = n)
#   committee_file <- bind_rows(committee_file, these_comms)
# }

##### Code Committee Chairs -- What to do about co-chairs???
# if(nrow(filter(bills, LES_sponsor %in% comms)) > 0 | any(grepl('committee', bills$LES_sponsor)) ){
#   cat('\n')
#   cat(glue('---> Attempting to Recode {nrow(filter(bills, LES_sponsor %in% comms | grepl("committee", LES_sponsor)))} Committed Sponsored Bills (N = {nrow(bills)})'))
#   these_chairs <- filter(comm_chairs, term == t_yrs & chair2 == '')
#   for(i in 1:nrow(these_chairs)){
#     chamb <- these_chairs[i,]$chamber
#     this_comm <- these_chairs[i,]$committee
#     bills[substring(bills$bill_id,1,1) == chamb & bills$LES_sponsor == this_comm,]$LES_sponsor <- these_chairs[i,]$chair
#   }
# }


#### Drop Uncoded Committees
if(nrow(filter(bills, LES_sponsor %in% comms)) > 0 | sum(grepl("committee", bills$LES_sponsor)) > 0){
  cat('\n')
  cat(glue('---> Dropping {nrow(filter(bills, LES_sponsor %in% comms | grepl("committee", LES_sponsor)))} Committed Sponsored Bills (N = {nrow(bills)})'))
  bills <- filter(bills, !(LES_sponsor %in% comms) & !grepl('committee', LES_sponsor))
}

###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "") %>% View()
if(nrow(filter(bills, LES_sponsor == '')) > 0){
  cat('\n')
  cat(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
  bills <- filter(bills, !(LES_sponsor == ''))     
}

############################################
############### Code Bill History
############################################

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

######## Standardize BillHist Bill IDs
bill_hist <- rename(bill_hist, bill_id = bill_number) %>%
  filter(!grepl("\\_|\\*", bill_id)) %>%
  mutate(bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = 0)))


### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
bill_hist$term <- t_yrs
bill_hist$term_num <- bill_hist$session
bill_hist$session_type <- recode(bill_hist$session_type, 'R' = 'RS', 'S' = 'SS', 'S2' = 'SS2')
bill_hist$session <- paste(bill_hist$session_year, bill_hist$session_type, sep = '-')
bill_hist <- select(bill_hist, -c(session_year, session_type)) %>% distinct()

### Fix Prefile Dates to T - 1 if December of T + By Request Indicators
bill_hist$action_date <- ifelse(grepl('^prefiled|^\\(by request\\)$', tolower(bill_hist$action)) & grepl(glue('^{t}-12'), bill_hist$action_date), 
                                gsub(glue('^{t}'), t-1, bill_hist$action_date), bill_hist$action_date )

if(any(grepl('^\\(by request\\)$', bill_hist$action))){
  bill_hist[bill_hist$action == '(by request)',]$action <- 'Introduced by request'  
}

### Rearrange + create order variable that covers both chambers
bill_hist <- arrange(bill_hist, session, bill_id, action_date) %>%
  group_by(session, bill_id) %>%
  mutate(order = 1:n()) %>%
  ungroup()

### Coding Chamber Variable WHere MIssing (Early Years)
if(sum(is.na(bill_hist$chamber)) + sum(bill_hist$chamber %in% '') == nrow(bill_hist)){
  ### Set First Action as Intro Chamber
  bill_hist$chamber <- ifelse(bill_hist$order == 1, substring(bill_hist$bill_id, 1, 1), bill_hist$chamber)
  ### Chamber Transitions --- In other chamber; Sent to other chamber; returned to chamber after veto
  bill_hist$chamber <- ifelse(grepl('^in senate|to assembly\\.$|^returned to senate', tolower(bill_hist$action)), 'S', bill_hist$chamber)
  bill_hist$chamber <- ifelse(grepl('^in assembly|to senate\\.$|^returned to assembly', tolower(bill_hist$action)), 'A', bill_hist$chamber)
  bill_hist$chamber <- ifelse(grepl('delivered to governor', tolower(bill_hist$action)), 'G', bill_hist$chamber)
  ### Fill in Gaps by Bill
  bill_hist <- bill_hist %>% group_by(session, bill_id) %>% fill(chamber) %>% ungroup()
}

### Standardize Chamber Variable
bill_hist$chamber <- recode(bill_hist$chamber, "A" = "House", "S" = "Senate", "G" = "Governor")

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
## AIC: Occassionally dates discussed in comm with a colon an no date
## --- Not coding 'postponed in comm' as action
aic_t <- c('dates discussed in comm.+[0-9]', '^from committee: [a-z]+', '^from concurrent committee on.+: [a-z]+',
           'from committee without rec', 'committee hearing')
abc_t <- c('from committee', 'read second time', 'engrossment', 'engrossed', 'read third time', 'declared.+emergency measure',
           'place on.+', 'taken from.+')
pc_t <- c('read third time.+passed', 'to senate\\.$', 'to assembly\\.$', 'title approved', 'to enrollment')
# -- enrollment = check
law_t <- c('approved by the governor', 'chapter [0-9]+', '^effective [a-z]+')

### Check Actions
# filter(bill_hist, grepl('from.+committee .+:', tolower(action))) %>% distinct(action) %>% View()
# filter(bill_hist, grepl('effective date', tolower(action))) %>% group_by(action) %>% summarize(n = n()) %>% arrange(desc(n)) %>% View()
# bill_hist[bill_hist$bill_id == "HF0791",])
# mutate(bill_hist, clean = gsub('committee~.+', 'committee', gsub("[0-9]+", '', action))) %>% distinct(clean) %>% unlist() %>% unname()

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
### Make Sure No Excess text in Bill Action
bill_hist$action <- str_trim(bill_hist$action)

### Code History Stages for Each Bill --- Subset Hist File, Code, Save, Repeat
options(warn = 2)
for(i in 1:nrow(bills)){
  b_id = bills[i,]$bill_id
  s_id = bills[i,]$session
  b_spon = bills[i,]$LES_sponsor
  hist_sub <- filter(bill_hist, bill_id == b_id, session == s_id)
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
  bill_stages$bill_url <- bills[i,]$bill_url
  ## Checking Action in Commmittee for pre-2011
  if(t < 2011){
    chamber_hearing <- ifelse(substring(b_id, 1, 1) == "A", bills[i,]$hcomm_hearing, bills[i,]$scomm_hearing)
    if(bill_stages$action_in_comm == 0 & !is.na(chamber_hearing)){
      bill_stages$action_in_comm <- 1
    }
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
# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% View()
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

## MERGE In COMMEMS
all_bill_stages <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))

### MERGE In S&S
all_bill_stages <- SS_term %>% 
  select(bill_id, term, session, SS) %>%
  left_join(all_bill_stages, ., by = c('bill_id', 'term', "session")) %>%
  mutate(SS = ifelse(is.na(SS), 0, SS))

### Adjust Commems if SS == 1
table(all_bill_stages$SS, all_bill_stages$commem)
all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
rm(SS_term)


### Save Stage Info **** MERGE WITH SS **********
write.csv(all_bill_stages, glue('../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv'), row.names = FALSE )

rm(bill_hist_path, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist, aic_t, abc_t, pc_t, law_t)
rm(b_id, s_id, b_spon, bill_hist)

####################################################
############### Identify Unique Legislators via SLER
####################################################

## Import and Clean Sponsors Name to Match
all_sponsors <- bills %>%
  mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'A', "H", "S")) %>%
  select(LES_sponsor, chamber, passed_chamber, law) %>%
  mutate(term = t_yrs) %>%
  group_by(LES_sponsor, chamber, term) %>%
  summarize(num_sponsored_bills = n(), 
            sponsor_pass_rate = sum(passed_chamber) / n(),
            sponsor_law_rate = sum(law) / n()) %>%
  ungroup()

######## Cosponsorship Info --- For NV: 2011+ Cosponsorship info may be sporadic
all_sponsors$num_cosponsored_bills <- NA
if(t < 2011){
  bills$cospon_match <- bills$sponsors # paste(bills$author, bills$coauthors, sep = '; ') 
}else{
  bills$cospon_match <- paste(bills$LES_sponsor, bills$primary_sponsors, tolower(bills$cosponsors), sep = '; ')
}
for(i in 1:nrow(all_sponsors)){
  c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'A', 'S')  )
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
  ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
}
bills <- select(bills, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills)) 


if(min(all_sponsors$num_cosponsored_bills) < 0){
  print("negative cosponsors"); break
} else{print("cosponsors valid")}

### No info for 1997:
if(t_yrs == "1997_1998" | sum(all_sponsors$num_cosponsored_bills, na.rm = TRUE) == 0){
  all_sponsors$num_cosponsored_bills <- NA
}

#######################
#### CLEAN NAMES
if(t < 2011){
  all_sponsors$last_name <- all_sponsors$LES_sponsor #gsub(',.+', '', all_sponsors$LES_sponsor)
  all_sponsors$first_name <- ''   # ifelse(grepl(',', all_sponsors$LES_sponsor), gsub('.+, ', '', all_sponsors$LES_sponsor), '')
}else{
  parsed_names <- map_df(all_sponsors$LES_sponsor, parse_names) %>% select(-salutation) %>% distinct() 
  parsed_names <- select(parsed_names, last_name, first_name, full_name)
  all_sponsors <- left_join(all_sponsors, parsed_names, by = c("LES_sponsor" = "full_name"))
}
all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update Last Names for Matching 
if(t >= 1999 & t <= 2008 & any(grepl("arberry jr\\.", all_sponsors$LES_sponsor)) ){
  all_sponsors[all_sponsors$LES_sponsor == "arberry jr.",]$last_name <-  "arberry"
}
if(t >= 1997 & t <= 2010){
  all_sponsors[all_sponsors$LES_sponsor == "mathews",]$last_name <-  "martinmathews"
}
if(t >= 2011 & t <= 2014){
  all_sponsors[all_sponsors$LES_sponsor == "marilyn dondero loop",]$last_name <-  "donderoloop"
}
if(t >= 2011 & t <= 2018){
  all_sponsors[all_sponsors$LES_sponsor == "irene bustamante adams",]$last_name <-  "bustamanteadams"
}


all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(LES_sponsor,substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name))

legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(t)) | startsWith(legiscan_sessions,as.character(t+1))]

legiscan = foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% select(-c(votesmart_id,knowwho_pid, ballotpedia)) %>%  distinct() 


# if(t_yrs == "2019_2020") {
#   legiscan = bind_rows(legiscan,
#                        legiscan %>% filter(people_id == 19565) %>% mutate(role = "Rep", district = "HD-060"),
#                        legiscan %>% filter(people_id == 19563) %>% mutate(role = "Rep", district = "HD-019"))
#   
# }

legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  mutate(match_name = name) %>%
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
      mode = "full",
      custom_match = c(
        "irene bustamante adams-h" = NA_character_,
        "kasina douglass-boone-h" = NA_character_,
        "nelson araujo-h" = NA_character_,
        "richard segerblom-s" = NA_character_,
        "elliot anderson-h" = NA_character_,
        "aaron ford-s" = NA_character_,
        "amber joiner-h" = NA_character_,
        "justin watkins-h" = NA_character_,
        "heidi gansert-s" = "heidi seevers gansert-s",
        "mark manendo-s" = NA_character_,
        "olivia diaz-h" = NA_character_,
        "bea duran-h" = NA_character_,
        "greg smith-h" = NA_character_,
        "richard daly-h" = "skip daly-h"
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
        "joyce woodhouse-s" = NA_character_,
        "tracy brown-may-h" = NA_character_,
        "yvanna cancela-s" = NA_character_,
        "don tatro-s" = NA_character_,
        "david parks-s" = NA_character_,
        "heidi gansert-s" = "heidi seevers gansert-s"
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


#####################################################################
##########Estimate Scores + Add in Relatd Variables
#####################################################################
### Check if bills in data without an ID'd sponsor
View(bills %>% filter( !(bills$LES_sponsor %in% legis_data$data_name)))
bills <- bills %>% #select(-sponsor) %>%
  rename(sponsor = LES_sponsor) %>%
  mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'A', 'H', 'S'))

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

#agg_stats, matches
rm(all_sponsors, bills, legis_data, SS_bills, H_elec_year, S_elec_year, i, keep_types)
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES, c_sub) # 
rm(t, terms, klarner_gs, comms, m_sub, parsed_names)
rm(commem_bills, chamber_hearing, t_sessions, year, comm_chairs)
# rm(these_chairs, this_comm, chamb)

### IF Compiling Committees:
# write.csv(committee_file, '~/Dropbox/Data/State Legislative Data/Committee_Info/NV_committee_chairs.csv', row.names = FALSE)
# rm(committee_file, these_comms)


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by APPOINTMENT of the Board of County Commissioners --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
# *********************
# ---> EXTENSIVE INFO ON ALL LEGISLATORS 1861 -- PRESENT: https://www.leg.state.nv.us/dbtw-wpd/LegSim.htm
# *********************
# Here too: https://www.leg.state.nv.us/Division/Research/Publications/NVLegislators/NVLegislators.pdf
########################################################################################################################


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1996 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 929 Committed Sponsored Bills (N = 1325)
### IN HOUSE:
# -- KRENZER --> Sponsors bills as committee co-chair
# -- TRIPPLE 
### IN SENATE:
# -- MARTINMATHEWS
# -- TOWNSEND (Chair)
### DROP:
# -- callister, matt -- reesigned January 12, 1995 --> O.C. Lee appointed -- See 


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 9 Bills That Are Carried Over to Finish Business
# ---> KEEPING 5 bill(s) introduced BY REQUEST
# ---> Dropping 774 Committed Sponsored Bills (N = 1166)
### IN SENATE:
# -- MORTENSON
# -- COFFIN

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 6 Bills That Are Carried Over to Finish Business
# ---> Dropping 712 Committed Sponsored Bills (N = 1263)
### IN SENATE:
# -- CARLTON


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 8 Bills That Are Carried Over to Finish Business
# ---> KEEPING 22 bill(s) introduced BY REQUEST
# ---> Dropping 699 Committed Sponsored Bills (N = 1292)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 7 Bills That Are Carried Over to Finish Business
# ---> Dropping 557 Committed Sponsored Bills (N = 1073)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 4 Bills That Are Carried Over to Finish Business
# ---> Dropping 575 Committed Sponsored Bills (N = 1119)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 6 Bills That Are Carried Over to Finish Business
# ---> Dropping 614 Committed Sponsored Bills (N = 1229)
### In House:
# -- ARBERRY (chair)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 8 Bills That Are Carried Over to Finish Business
# ---> Dropping 460 Committed Sponsored Bills (N = 1010)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~
# ---> Dropping 515 Committed Sponsored Bills (N = 1088)
### APPOINTED ~ SENATE:
# -- BROWER -- Jan 18, 2011 
### IN HOUSE:
# -- HOGAN
### IN SENATE:
# -- KIHUEN (chair)
### DROP
# -- raggio, william -- resigned Jan. 15, 2011

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 428 Committed Sponsored Bills (N = 1041)
### APPOINTED ~ SENATE:
# -- COHEN (lesley) -- Dec. 18, 2012
### IN HOUSE:
# -- SWANK
# -- BROOKS (steve) --> "Expelled March 28, 2013"!
### DROP
# -- mastroluca, april -- resigned Nov 30, 2012
# -- halseth, elizabeth -- resigned Feb 17, 2012.
# -- leslie, sheila -- resigned Feb 14, 2012. 


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 439 Committed Sponsored Bills (N = 1018)
### APPOINTED ~ HOUSE:
# -- JOINER -- Dec 30, 2014
# -- TROWBRIDGE -- Dec 16, 2014
### APPOINTED ~ SENATE
# -- LIPPARELLI -- Dec 2, 2014
### DROP:
# -- bobzien, david -- resigned dec 3, 2014 --> Reno City Council
# -- duncan, wesley -- resigned dec 4, 2014
# -- hutchison, mark -- resigned dec 1, 2014 --> Lt. Governor

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 422 Committed Sponsored Bills (N = 1077)
### APPOINTED ~ SENATE:
# -- CANCELA -- Dec 6, 2016
### DROP:
# -- kihuen, ruben -- Nov 16, 2016 --> US House
# -- smith, debbie -- Died in office ~ February 2016

# filter(klarner, grepl("ratti", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(klarner, ddez == 31 & sen == 1) %>% select(cand, year, sen, etype, outcome, ddez, candid)


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
    if(length(unique(k_sub$cand)) == 1){
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$sponsor <- unique(k_sub$cand) 
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$klarner_name <- unique(k_sub$cand) 
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_id),]$klarner_id <- unique(k_sub$candid) 
      print(glue(' ~~ {name} ~~ Matched to --> {unique(k_sub$cand) }'))  
    }
  }
}


### ****Still missing***** 
# ---> 3 Remaining = 1-term appointments OR 2017-2018 
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[3]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term)
# filter(klarner, grepl('zzzzz', cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, missing, name_sub, still_missing)

### Fix Missing
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')

# for(i in 1:nrow(name_matches)){
#   LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
#   LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
#   LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
# }
# rm(name_matches, i)


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

### Manually Fix Those Not in Klarner
LES[LES$sponsor == "trowbridge, glenn",]$party <- 'r'
LES[LES$sponsor == "trowbridge, glenn",]$district <- 37
LES[LES$sponsor == "trowbridge, glenn",]$exper <- 'none'

LES[LES$sponsor == "lipparelli, mark",]$party <- 'r'
LES[LES$sponsor == "lipparelli, mark",]$district <- 6
LES[LES$sponsor == "lipparelli, mark",]$exper <- 'none'

#### *** If runs for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "cancela, yvanna",]$party <- 'd'
LES[LES$sponsor == "cancela, yvanna",]$district <- 10
LES[LES$sponsor == "cancela, yvanna",]$exper <- 'none'

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)

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
### ---> Names are a mess... Need need to be parsed in detail... [ Extract suffixes, reformat, standardize]
### COULD Do this by cross-checking actice chamber-years....

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
# *** Note: John Regan = John "Jack" Regan;  Peggy (Penny) Pierce = Margaret 'Peggy' Pierce

### SM Names Matched to Multiple LES Sponsors
# ---> so this will catch all errors except those where Sponsor A is Matched to Voter Score B, when Sponsor B and Voter A are missing
filter(LES, !is.na(SM_name)) %>%
  group_by(SM_name) %>% 
  summarize(name_matches = paste(unique(sponsor), collapse = "----")) %>% 
  filter(grepl("----", name_matches))

#### FIX MISMATCHES
# LES[LES$sponsor %in% c('zzzzzzzzzz'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# ------> FOR NV ---> NO IDEO DATA FOR 1995-1996
# filter(LES, is.na(np_score)) %>% filter( !(term %in% c('1995_1996','2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('flor', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'anderson, paul', SM_name = 'Anderson, Dennis') ## Paul anderson == Dennis Paul Anderson
name_matches <- add_row(name_matches, LES_name = 'flores, ed', SM_name = 'Flores, Edgar')
# name_matches <- add_row(name_matches, LES_name = 'flores, lucy', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'hardy, warren b.', SM_name = 'Hardy II, Warren B')
name_matches <- add_row(name_matches, LES_name = 'ohrenschall, genie', SM_name = 'Ohrenschall, Eugenia')
name_matches <- add_row(name_matches, LES_name = 'segerblom, tick', SM_name = 'Segerblom, Richard')
name_matches <- add_row(name_matches, LES_name = 'titus, dina', SM_name = 'Titus, Alice') ## Alice Constandina 'Dina' Titus
#name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

#### Manual Edits (Needs more precision...)
LES[LES$sponsor == 'beers, bob 1',]$SM_name <- ideo[ideo$name == 'Beers, Robert' & ideo$house1999 %in% 1,]$name
LES[LES$sponsor == 'beers, bob 1',]$SM_party <- ideo[ideo$name == 'Beers, Robert' & ideo$house1999 %in% 1,]$party
LES[LES$sponsor == 'beers, bob 1',]$np_score <- ideo[ideo$name == 'Beers, Robert' & ideo$house1999 %in% 1,]$np_score

LES[LES$sponsor == 'beers, bob 2',]$SM_name <- ideo[ideo$name == 'Beers, Robert' & ideo$house2008 %in% 1,]$name
LES[LES$sponsor == 'beers, bob 2',]$SM_party <- ideo[ideo$name == 'Beers, Robert' & ideo$house2008 %in% 1,]$party
LES[LES$sponsor == 'beers, bob 2',]$np_score <- ideo[ideo$name == 'Beers, Robert' & ideo$house2008 %in% 1,]$np_score

### Below = Multipole records, 2013-2014 and 2015-2015
LES[LES$sponsor == 'sprinkle, michael',]$SM_name <- ideo[ideo$name ==  'Sprinkle, Michael' & ideo$house2015 %in% 1,]$name
LES[LES$sponsor == 'sprinkle, michael',]$SM_party <- ideo[ideo$name == 'Sprinkle, Michael' & ideo$house2015 %in% 1,]$party
LES[LES$sponsor == 'sprinkle, michael',]$np_score <- ideo[ideo$name == 'Sprinkle, Michael' & ideo$house2015 %in% 1,]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1995 - 2020
# *** SPLIT CONTROL 1995-1996 -- Co-share agreement --> Alternated control daily --> Coding ALL 0
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2014, 2017:2020) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2015:2016) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1
LES[LES$chamber == "House" & LES$term == '1995_1996',]$in_majority <- 0

### Senate -- 1995 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(2009:2014, 2017:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:2008, 2015:2016) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


############################################
######## FINAL NAME STANDARDIZATION
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

##### Drop Nicknames
# filter(LES, grepl('\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct() %>% arrange(sponsor)
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)
LES[LES$sponsor == 'beers, bob 1',]$sponsor <- "beers, robert t." # https://en.wikipedia.org/wiki/Bob_Beers_(politician,_born_1959)
LES[LES$sponsor == 'beers, bob 2',]$sponsor <- "beers, robert l." # https://en.wikipedia.org/wiki/Bob_Beers_(politician,_born_1951)

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
    Ideology_Missing = round(sum(is.na(np_score))/n(), 2))

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
            max_LES = max(LES)) 

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
  scale_color_manual(values=c("dodgerblue2", "red2"))

##### CHECK OUTLIERS
# filter(LES, party == 'd' & np_score > .25) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & np_score < -.25) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

