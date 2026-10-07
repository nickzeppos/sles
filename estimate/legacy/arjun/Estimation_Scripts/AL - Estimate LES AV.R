
###########################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR ******* ALABAMA *******
#############################################################################

###################################
## TERMS:
## ---- 4 years long for house and senate!
## ---- Earliest term that we have data is 1999 - 2002, but we're missing 1999. Keeping anyway because sessions are annual. 
## SESSIONS:
## ---- Sessions are ANNUAl -- Bills do NOT carry over from to the next
## ---- Special/Org Sessions in separate files
## MEMBER LISTS:
## ---- 
## PROCESS:
## ---- See: http://www.legislature.state.al.us/aliswww/ISD/AlaLegProcess_Desc.aspx
## ----> BILLS MUST GO THROUGH COMMITTEE: "the framers... inserted a provision in the Constitution stipulating that no bill may be enacted into law until it 
# has been referred to, acted upon by, and returned from, a standing committee in each house."
## ----> IF Reported: AIC
## Sponsorship/Authorship
## -- 
###########################

rm(list=ls())
options(stringsAsFactors = FALSE, scipen = 999)

library(tidyr)
library(dplyr)
library(stringr)
library(ggplot2)
library(humaniformat)
library(purrr)
library(lubridate)
library(readxl)
library(glue)
library(readr)
library(foreach)
library(tibble)
library(inexact)

this_state <- 'AL'
keep_types <- c("HB", "SB")


#### Output Directory
#dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd(glue("../../LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Data Directory

data_files <- list.files(glue("../../../State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
sessions <- gsub(".+Bill_Details_|.csv", '', bill_files)
rm(data_files, bill_files)

terms <- 2019
t = terms
t_yrs <- as.numeric(terms)
t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 3}'))
t_sessions = sessions[grepl(as.character(t),sessions) | grepl(as.character(t+1),sessions) |
                        grepl(as.character(t+2),sessions) | grepl(as.character(t+3),sessions)]


#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- bind_rows(read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t+1}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t+2}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t+3}.csv")))

SS_bills <- SS_bills %>% 
  filter(State == this_state) %>%
  rename(bill_id = Bill.No) %>%
  mutate(Date = gsub("Sept","Sep",Date),
         date = as.Date(gsub("\\.","",Date), "%B %d, %Y"),
         year = as.integer(format(date, "%Y")),
         term = paste0(min(year),"_",max(year)), 
         bill_id = toupper(bill_id),
         bill_id = gsub(' ','',bill_id),
         bill_id = paste0(gsub("[0-9].+", '', bill_id), str_pad(gsub("^[A-Z]+", "", bill_id), 4, pad = "0")),
         SS = 1) %>%
  select(state = State, term, year, bill_id, everything())

#### Check SS BIll Types ---> Correct to ensure standardized with Data Format
# table(SS_bills$bill_type)

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[1]



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
bills$bill_id <- paste0(gsub('[0-9]+', '', bills$bill_id), str_pad(gsub('^[A-Z]+', '', bills$bill_id), 4, pad = "0"))
bills$term <- t_yrs
bills$session_year <- str_extract(bills$session, paste(seq(t, t+3, 1), collapse = "|"))
bills$session <- gsub(' \\d{4}$', '', bills$session)
bills$session <- recode(bills$session, 'Regular Session' = 'RS', 'First Special Session' = 'SS1', 'Second Special Session' = 'SS2',
                        'Third Special Session' = 'SS3', 'Fourth Special Session' = 'SS4', 'Organizational Session' = 'OS')
bills$session <- paste(bills$session_year, bills$session, sep = '-')
bills <- select(bills, -session_year) 

# Format the dates consistently
bills$last_action_date <- format(mdy(bills$last_action_date),"%m/%d/%Y")
### Drop duplicates
bills <- distinct(bills)

######## Standardize the Bill ID Var
# bills <- rename(bills, bill_id = bill_number) 


############### Standardize Sponsors
bills$sponsor <- tolower(bills$sponsor)
bills$sponsor <- trimws(bills$sponsor)

#### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
bills$sponsor <- gsub('á', 'a', bills$sponsor)
bills$sponsor <- gsub('é', 'e', bills$sponsor)
bills$sponsor <- gsub('ó', 'o', bills$sponsor)
bills$sponsor <- gsub('í', 'i', bills$sponsor)
bills$sponsor <- gsub('ñ', 'n', bills$sponsor)

#### Adjusting for Bills By Request -- Need to do BEFORE splitting
if(any(grepl('\\(br\\)|by request| br$', bills$sponsor))){
  print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
  #print(glue("-----> KEEPING {nrow(filter(bills, grepl('(br)|by request', introducers)))} bill(s) introduced BY REQUEST"))
}


############### Drop Resolutions, Messages, Communications, Reports
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
all_bills <- bills 
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)

### Fix Chamber Var (It's status chamber right now)
bills$chamber <- substring(bills$bill_id, 1, 1)


### FIx First Initials
bills$sponsor <- ifelse(grepl('\\([a-z]\\)$|\\([a-z][a-z]\\)$', bills$sponsor), gsub('\\(|\\)', '', bills$sponsor), bills$sponsor)

#### LES Sponsor Variable --- Format = Last first initial. (needed)
bills$LES_sponsor <- bills$sponsor
sort(table(bills$LES_sponsor))


#### Manual Name Fixes
if(t_yrs == "2003_2006"){
  bills[bills$LES_sponsor == 'ford',]$LES_sponsor <- "ford c"
}else if(t_yrs == "2007_2010"){ 
  # Mike Hubbard -- Joe wins special ||| Jack williams -- phil wins special ||| Laura hall wins, albert hall dies pre-seating
  bills[bills$LES_sponsor == 'hubbard',]$LES_sponsor <- "hubbard m"
  bills[bills$LES_sponsor == 'williams',]$LES_sponsor <- 'williams j'
  bills[bills$LES_sponsor == 'hall',]$LES_sponsor <- 'hall l'
}else if(t_yrs == "2011_2014"){
  bills[bills$LES_sponsor == 'coleman-evans',]$LES_sponsor <- 'coleman'
  bills[bills$LES_sponsor == 'newton',]$LES_sponsor <- "newton c"
  bills[bills$LES_sponsor == 'holmes',]$LES_sponsor <- "holmes a"
}else if(t_yrs == '2015_2018'){ # Linda Coleman adopts joint name; Merika Coleman removes it; both mid-term
  bills[bills$chamber == 'S' & bills$LES_sponsor == 'coleman',]$LES_sponsor <- 'coleman-madison'
  bills[bills$chamber == 'H' & bills$LES_sponsor == 'coleman',]$LES_sponsor <- 'coleman-evans'   
  bills[bills$LES_sponsor == "hill", ]$LES_sponsor <- "hill j"
} 

###################
###### Merge in S&S Bills
###################
# *** For ALABAMA: Bills DO NOT Carryover, NEED TO MERGE ON YEAR AND SPECIAL
# ---> Capping Bill numbers at SESSION MAX and assuming bill is from SS with most bills proposed


if(t_yrs == "2019_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="HB20002"] = "HB0002"
  SS_bills$bill_id[SS_bills$bill_id=="HB50005"] = "HB0005"
  SS_bills$bill_id[SS_bills$bill_id=="HB40004"] = "HB0004"
  SS_bills$bill_id[SS_bills$bill_id=="HB60006"] = "HB0006"
  SS_bills$bill_id[SS_bills$bill_id=="SB20002"] = "SB0002"
}


SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, year, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS, year,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,sponsor), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",sponsor, ignore.case=T)) %>%
  arrange(sponsor) 
unique(missing_SS_bills$bill_id) 



# now check to see if there are duplicate joins


duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills %>% mutate(year = as.integer(substr(session,1,4))), 
            by = c("bill_id", "term", "year")) %>%
  group_by(rowname) %>% mutate(count = n()) %>%
  ungroup() %>% 
  select(count,bill_id,term,session, Title, short_title) %>% 
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

############### Code Commemorative
bills <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  distinct() %>% 
  left_join(bills, ., by = c('bill_id', 'term', 'session'))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)


#### DROP Committee Bills
if(any(grepl('committee', bills$sponsor))){
  print(glue("-----> DROPPING {nrow(filter(bills, grepl('committee', sponsor)))} bill(s) introduced BY COMMITTEE"))
  break
  # bills <- filter(bills, !grepl('committee', LES_sponsor))
}

###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "") %>% View()
if(nrow(filter(bills, LES_sponsor == '')) > 0){
  print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
  bills <- filter(bills, !(LES_sponsor == ''))     
}


############### Code Bill History
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

######## Standardize Bill Hist Bill IDs
bill_hist$bill_id <- paste0(gsub('[0-9]+', '', bill_hist$bill_id), str_pad(gsub('^[A-Z]+', '', bill_hist$bill_id), 4, pad = "0"))

### Clean Term/Session Variables
bill_hist$term <- t_yrs
bill_hist$session_year <- str_extract(bill_hist$session, paste(seq(t, t+3, 1), collapse = "|"))
bill_hist$session <- gsub(' \\d{4}$', '', bill_hist$session)
bill_hist$session <- recode(bill_hist$session, 'Regular Session' = 'RS', 'First Special Session' = 'SS1', 'Second Special Session' = 'SS2',
                            'Third Special Session' = 'SS3', 'Fourth Special Session' = 'SS4', 'Organizational Session' = 'OS')
bill_hist$session <- paste(bill_hist$session_year, bill_hist$session, sep = '-')
bill_hist <- select(bill_hist, -session_year)
bill_hist <- distinct(bill_hist)

### Standardize Chamber/Date Variable
bill_hist$chamber <- recode(toupper(bill_hist$chamber), "H" = "House", "S" = "Senate")
bill_hist$action_date <- format(mdy(bill_hist$action_date),"%m/%d/%Y")


#### Fix Date Errors ( Will get filled in with Max Bill Date)
if(t_yrs == '2003_2006'){
  bill_hist[!is.na(bill_hist$action_date) & bill_hist$action_date == "1998-02-18", ]$action_date <- NA
}else if(t_yrs == '2011_2014'){
  bill_hist[!is.na(bill_hist$action_date) & bill_hist$action_date == "2013-02-06", ]$action_date <- NA
}

### SPORADIC HISTORY ERRORS where bill info is correct but history does not match, often wildly differnt time period
# ---> IF this breaks, run the scraper again -- new checks should prevent this from happening too often
bill_hist$sy <- as.numeric(substring(bill_hist$session, 1, 4))
#filter(bill_hist, !(substring(action_date, 1, 4) %in% t:(t+3)))
errors <- bill_hist %>%
  group_by(session, bill_id) %>%
  filter( !(substr(action_date, nchar(action_date)-3, nchar(action_date)) %in% unique(sy) ) & !is.na(action_date)) %>%
  filter(!grepl('delivered to gov|enrolled|third reading passed', tolower(action))) # bunch of errors in 2012_RS (wrongly dated 2013)
if( nrow( errors ) > 1 ){
  print(' -----> ********** CHECK BILL HISTORY ERRORS ************** ')
  print(as.data.frame(select(errors, bill_id, session, action_date, chamber, action)))
  break
}
bill_hist <- select(bill_hist, -sy); rm(errors)


### Standardize Dates/Fill in NA's with most recent Date
if(any(is.na(bill_hist$action_date))){
  print(glue('-----> Filling in Action Dates for {sum(is.na(bill_hist$action_date))} of {nrow(bill_hist)} MISSING DATES'))
  bill_hist <- bill_hist %>% group_by(session, bill_id) %>% fill(action_date)  %>% ungroup()
}

## Fix Remaining Missing Dates
if(any(is.na(bill_hist$action_date))){
  bill_hist <- filter(bill_hist, !(is.na(action_date) & action == ''))
}

### Create Order Variable --- Everything Seems in Order
bill_hist <- bill_hist %>% group_by(session, bill_id) %>% arrange(action_date) %>% mutate(order = 1:n()) %>% ungroup()
bill_hist <- arrange(bill_hist, session, bill_id, order)

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
# -- See note above: if reported (read x2, AIC)
# -- Otherwise very few AIC terms and all that are present are identifiable only by specific committee name.. + typically recorded AFTER second read
# -- Exceptions are 'reported from' and (more rare) 'Acted on By' -- Neither used consistently however
aic_t <- c('read.+ second time', '^reported from', '^acted on by', 'favorable from')
abc_t <- c('read.+ second time', '^reported from', 'third reading', 'placed on the calendar', 'motion to')
# --> Amendment offered -- sometiems by comm, sometimes member, unclear when proposed so hard to code based on it
pc_t <- c('engrossed', 'enrolled', 'motion.+read a third time and pass.+adopted')
# -- 'third reading passed' doesn't necessarily mean made it out... seems to require "motion to [again] read a third time and pass [as amended] adopted"
# -- Enrolled = passed both, but keeping as check (though within chamber coding will limit it..)
law_t <- c('^assigned act no')
# filter(bill_hist, grepl('favorable from', tolower(action))) %>% select(action) %>% distinct()
# filter(bill_hist, bill_id == 'SB439' & session == '2000_RS') %>% select(chamber, action_date, action, order)

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
  hist_sub <- filter(bill_hist, bill_id == b_id, session == s_id)
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
  bill_stages$bill_url <- "No URL - Select Session Info Tab; Pick Session; Go to Bills Tab; Find Status"
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


read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-4}_{t-1}_Bill_Stage_Codings.csv")) %>% 
   mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% filter(!grepl("-S",session)) %>%  print()

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-8}_{t-5}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% filter(!grepl("-S",session)) %>% print()


### MERGE to BILL DATA
bills <- left_join(bills, all_bill_stages, by = c("bill_id", "term", "session", "LES_sponsor")) 

### MERGE In COMMEMS
all_bill_stages <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  distinct() %>% 
  left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))

### MERGE In S&S
all_bill_stages <- SS_term %>%
  select(bill_id, term, session, SS) %>%
  left_join(all_bill_stages, ., by = c('bill_id', 'term', "session")) 
rm(SS_term)

### Adjust Commems if SS == 1
all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)

### Save Stage Info **** MERGE WITH SS **********
write.csv(all_bill_stages, glue('../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv'), row.names = FALSE )

rm(bill_hist_path, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist, aic_t, abc_t, pc_t, law_t)
rm(b_id, s_id, b_spon, bill_hist)

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
  arrange(chamber, LES_sponsor) %>%
  ungroup()

######## NO Cosponsorship Info
all_sponsors$num_cosponsored_bills <- NA
# for(i in 1:nrow(all_sponsors)){
#   c_sub <- filter(bills, substring(bill_id, 1, 1) == all_sponsors[i,]$chamber)
#   all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$sponsor)))
#   ### ONLY need to adjust this way if sponsored and cosponsored column are the same
#   all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
# }
# all_sponsors <- select(all_sponsors, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))

#######################
#### CLEAN NAMES
#######################

all_sponsors$last_name <- ifelse(!grepl(' [a-z]$| [a-z][a-z]$', all_sponsors$LES_sponsor), all_sponsors$LES_sponsor, gsub(' [a-z]$| [a-z][a-z]$', '', all_sponsors$LES_sponsor))
all_sponsors$first_name <- ifelse(grepl(' [a-z]$| [a-z][a-z]$', all_sponsors$LES_sponsor), str_trim(str_extract(all_sponsors$LES_sponsor, " [a-z]$| [a-z][a-z]$")), '')
all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)


all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(LES_sponsor,substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name))


legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(terms)) | startsWith(legiscan_sessions,as.character(terms+1)) | 
                                        startsWith(legiscan_sessions,as.character(terms+2)) | startsWith(legiscan_sessions,as.character(terms+3))]

legiscan = foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% distinct()

if(t_yrs == "2019_2022") {
  legiscan = legiscan %>% filter(people_id != 23107) # patrice doesn't sponsor any HBs and otherwise she gets messed up with her dad (her predecessor)
  
}


legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = case_when(
    n > 2 ~ paste0(last_name, " ",substr(first_name,1,1), substr(middle_name,1,1)),
    n == 2 ~  paste(last_name,substr(first_name,1,1)),
    T ~ last_name)) %>%
  group_by(match_name) %>% 
  mutate(n = n()) %>% 
  mutate(match_name = ifelse(n > 1, paste0(last_name, " ",substr(first_name,1,1), substr(middle_name,1,1)),
                             match_name)) %>% 
  mutate(match_name_chamber = tolower(paste(match_name,substr(district,1,1),sep="-"))) 


# inexact::inexact_addin()
# left: legiscan_adj
# right: all_sponsors
# method: osa
# mode: full

if(t_yrs == "2019_2022"){
  all_sponsors2 = 
    # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "mccutcheon-h" = NA_character_,
        "dunn-s" = NA_character_,
        "forte-h" = NA_character_,
        "jones a-s" = "jones-s"
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

##############################################################
############### Estimate Scores + Add in Relatd Variables
##############################################################
### Check if bills in data without an ID'd sponsor
filter(bills, !(bills$LES_sponsor %in% legis_data$data_name))
bills <- bills %>% select(-sponsor) %>%
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

#### If LES == 0 and 
LES[LES$LES == 0 & is.na(LES$num_sponsored_bills), c('num_sponsored_bills', 'sponsor_pass_rate', 'sponsor_law_rate')] <- 0 

############### Save LES
write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
rm(LES)

cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************"))


################# ****** END LOOP

#agg_stats, matches
rm(all_sponsors, bills, legis_data, SS_bills, elec_year, i, keep_types, commem_bills)
rm(k_matches, klarner_sub, m_sub, km, bill_path, t_yrs, calc_LES) # c_sub
rm(t, terms, klarner_gs) #, parsed_names)

########################################################################################################################################################
############################################################################################################################################################# NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# *** Note: Can search within each session to get info about legislators via bills by sponsor
# -----> Most notably district -- but can't link to it because of how the AL website works

### ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2002 TERM! ~~~~~~~~~~~~~~ 
# -----> Filling in Action Dates for 6562 of 53038 MISSING DATES
### Won H SPECIAL: 
# -- BARTON (jim, 2001) -- http://bartonkinney.com/jim-barton/
# -- BRIDGES (duwayne, 2000) -- https://yellowhammernews.com/tag/duwayne-bridges/
# -- FORD, C (craig, 2000, succeed dad joe ford)https://en.wikipedia.org/wiki/Craig_Ford
# -- MCLAUGHLIN (jeffrey, 2001) -- https://en.wikipedia.org/wiki/Jeffrey_McLaughlin_(politician)
### NAME FIXES
# -- FORD, J == JOHNNY FORD, Dist 82, per AL sponsor info
# -- FORD = JOE FORD, District 28, per AL Website -- Died June 2000

### ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2006 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 16 bill(s) without a sponsor
# -----> Filling in Action Dates for 6603 of 55497 MISSING DATES
### WON H SPECIAL:
# -- DEMARCO (paul, 2005) -- https://www.ourcampaigns.com/RaceDetail.html?RaceID=205638
# -- WARREN (pebblin, 2005) -- https://www.ourcampaigns.com/RaceDetail.html?RaceID=137175
# -- WILLIAMS, J (jack 'jd', 2004) -- https://votesmart.org/candidate/biography/27636/jack-williams
# -- WILLIAMS, N (nick, 2005) -- https://votesmart.org/candidate/biography/27659/nick-williams
### WON S SPECIAL
# -- Singleton (bobby, 2005) -- https://en.wikipedia.org/wiki/Bobby_Singleton
### NAME FIX:
# - ford == CRAIG FORD after johnny ford leaves office
# - updated williams special winners to match down the line

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 2 bill(s) without a sponsor
# -----> Filling in Action Dates for 7269 of 61370 MISSING DATES
### WON H SPECIAL:
# -- BEECH (elaine, 2009) -- https://en.wikipedia.org/wiki/Elaine_Beech
# -- FIELDS (james c, 2008, lost 2010 general) -- https://en.wikipedia.org/wiki/James_C._Fields
# -- GIVAN (juandalynn, 2010) -- http://www.legislature.state.al.us/aliswww/ISD/ALRepresentative.aspx?OID_SPONSOR=85974&OID_PERSON=6665
# -- TAYLOR (butch. 2007) -- https://www.ourcampaigns.com/RaceDetail.html?RaceID=338240
### WON S SPECIAL:
# -- DUNN (priscilla, 2009, via H) -- https://en.wikipedia.org/wiki/Priscilla_Dunn
# -- IRONS (tammy, 2010 via H) - https://en.wikipedia.org/wiki/Tammy_Irons
# -- KEAHEY (george 'marc', 2009, via H) -- https://ballotpedia.org/George_M._%22Marc%22_Keahey
# -- PITTMAN (trip, 2007) -- https://en.wikipedia.org/wiki/Trip_Pittman
# -- SANFORD (paul, 2009) -- paul sanford alabama
# -- TAYLOR (bryan, 2010) -- https://en.wikipedia.org/wiki/Bryan_Taylor_(lawyer)
# -- WARD (cam, 2010) -- https://en.wikipedia.org/wiki/Cam_Ward_(politician)
### DROP:
# HALL (ALBERT) --- Died after election, before seating -- https://www.waff.com/story/5671893/rep-albert-hall-dies/

### ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2014 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1 bill(s) without a sponsor
# -----> Filling in Action Dates for 6029 of 51618 MISSING DATES
### WON H SPECIAL:
# -- BUTLER (mack, 2012) -- https://en.wikipedia.org/wiki/Mack_Butler
# -- CARNS (jim, 2012, past inc) -- https://en.wikipedia.org/wiki/Jim_Carns
# -- CLARKE (adline c., 2013) -- https://ballotpedia.org/Adline_C._Clarke
# -- POLIZOS (dimitri, 2013) -- https://en.wikipedia.org/wiki/Dimitri_Polizos
# -- SESSIONS (david, 2011) -- https://en.wikipedia.org/wiki/David_Sessions
# -- SHEDD (randall, 2013) -- https://ballotpedia.org/Randall_Shedd
# -- STANDRIDGE (david, 2012) -- https://en.wikipedia.org/wiki/David_Standridge
# -- WILCOX (margie, 1/2014) -- https://ballotpedia.org/Margie_Wilcox
# -- HOLMES (mike, 1/2014) -- WON"T SHOW BECAUSE LAST NAME = DUPLICATE -- https://ballotpedia.org/Mike_Holmes_(Alabama)
### WON S SPECIAL:
# -- HIGHTOWER (bill, 2013) -- https://en.wikipedia.org/wiki/Bill_Hightower
### IN HOUSE, NO BILLS:
# -- MCADORY, LAWRENCE -- https://ballotpedia.org/Lawrence_McAdory
# -- BANDY, GEORGE -- https://en.wikipedia.org/wiki/George_Bandy
# -- FORTE, BANDY -- https://ballotpedia.org/Berry_Forte
### NAME FIXs:
# -- 'newton' = 'newton c' = charles newton -- Name changed in system after Demetrius Newton passed away in 2013
# -- 'holmes' = 'holmes a' = alvin holmes --- Name changed in system after mike holmes elected in 2014
### DROP:
# -- COLLIER, JACK (spencer) -- Appointed post-election as AL homeland security director -- http://blog.al.com/live/2010/12/gov-elect_bentley_appoints_spe.html

### ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2018 TERM! ~~~~~~~~~~~~~~ 
# -----> Filling in Action Dates for 4957 of 46607 MISSING DATES
### WON H SPECIAL
# -- BLACKSHEAR (chris, 2016) -- https://ballotpedia.org/Chris_Blackshear
# -- CHESTNUT (prince, 2017) -- https://ballotpedia.org/Prince_Chestnut
# -- CRAWFORD (danny, 2016) -- https://ballotpedia.org/Danny_Crawford
# -- ELLIS (corley, 2016) -- https://ballotpedia.org/Corley_Ellis
# -- HOLLIS (rolanda, 2017) -- https://ballotpedia.org/Rolanda_Hollis
# -- LOVVORN (joe, 2016) -- https://ballotpedia.org/Joe_Lovvorn
### WON SENATE, KLARNER WRONG:
# -- SMITH (Harri anne) -- https://ballotpedia.org/Harri_Anne_Smith + https://ballotpedia.org/Melinda_McClendon
### NAME FIXES:
# IN H: coleman --> coleman-evans (merika)
# IN S: coleman --> coleman-madison (linda)
# ---> BOTH SWITCH NAMES MID-TERM; script will match to 'coleman' in respective chamber in klarner
# IN H: hill --> hill j = Jim Hill, district 50

#### 2019+
# polizos -- Died in office -- https://en.wikipedia.org/wiki/Dimitri_Polizos

# filter(klarner, grepl('coleman', cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(klarner, ddez == 29 & year > 2009 & sen == 1) %>% select(cand, year, sen, etype, outcome, ddez, candid)


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
      if(length(unique(k_sub$candid)) > 1){
        k_sub <- filter(k_sub, sen == ifelse(LES[LES$sponsor == name,]$chamber == "Senate", 1, 0))
      }
    }else{
      k_sub <- filter(klarner, grepl(name, cand))  
    }
    if(length(unique(k_sub$cand)) == 1 & length(unique(k_sub$candid)) == 1 ){
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$sponsor <- unique(k_sub$cand) 
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$klarner_name <- unique(k_sub$cand) 
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_id),]$klarner_id <- unique(k_sub$candid) 
      print(glue(' ~~ {name} ~~ Matched to --> {unique(k_sub$cand) }'))  
    }
  }
}

### Manual Fixes
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_id <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_name <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$sponsor <- "owlett, clint"

### ****Still missing***** 
# ---> Remaining = 2015-2018 Term
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[4]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name)
# rm(missing, name_sub, still_missing)

name_matches <- data.frame(LES_name = "warren", k_name = 'warren, pebblin w.')
name_matches <- add_row(name_matches, LES_name = 'williams, p', k_name = 'williams, phil 2')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', k_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}

# #### DETAILED MANUAL FIXES
# LES[LES$sponsor %in% "keith-agaran" & LES$term %in% "2009_2010",]$klarner_id <- 297229
# LES[LES$sponsor %in% "keith-agaran" & LES$term %in% "2009_2010",]$klarner_name <- "keithagaran, gil s. (coloma)"
# LES[LES$sponsor %in% "keith-agaran" & LES$term %in% "2009_2010",]$sponsor <- "keithagaran, gil s. (coloma)"

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
rm(check_dup, k_sub, exact)


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
### ******** WON"T NEED THESE THREE WITH NEXT KLARNER UPDATE **********
LES[LES$sponsor == "blackshear" & is.na(LES$klarner_id),]$party <- 'r'
LES[LES$sponsor == "blackshear" & is.na(LES$klarner_id),]$district <- 80
LES[LES$sponsor == "blackshear" & is.na(LES$klarner_id),]$exper <- 'none'
LES[LES$sponsor == "blackshear" & is.na(LES$klarner_id),]$sponsor <- 'blackshear, chris'

LES[LES$sponsor == "crawford" & is.na(LES$klarner_id),]$party <- 'r'
LES[LES$sponsor == "crawford" & is.na(LES$klarner_id),]$district <- 5
LES[LES$sponsor == "crawford" & is.na(LES$klarner_id),]$exper <- 'none'
LES[LES$sponsor == "crawford" & is.na(LES$klarner_id),]$sponsor <- 'crawford, danny'

LES[LES$sponsor == "lovvorn" & is.na(LES$klarner_id),]$party <- 'r'
LES[LES$sponsor == "lovvorn" & is.na(LES$klarner_id),]$district <- 79
LES[LES$sponsor == "lovvorn" & is.na(LES$klarner_id),]$exper <- 'none'
LES[LES$sponsor == "lovvorn" & is.na(LES$klarner_id),]$sponsor <- 'lovvorn, joe'

LES[LES$sponsor == "hollis" & is.na(LES$klarner_id),]$party <- 'd'
LES[LES$sponsor == "hollis" & is.na(LES$klarner_id),]$district <- 58
LES[LES$sponsor == "hollis" & is.na(LES$klarner_id),]$exper <- 'none'
LES[LES$sponsor == "hollis" & is.na(LES$klarner_id),]$sponsor <- 'hollis, rolanda'

LES[LES$sponsor == "chestnut" & is.na(LES$klarner_id),]$party <- 'd'
LES[LES$sponsor == "chestnut" & is.na(LES$klarner_id),]$district <- 67
LES[LES$sponsor == "chestnut" & is.na(LES$klarner_id),]$exper <- 'none' 
LES[LES$sponsor == "chestnut" & is.na(LES$klarner_id),]$sponsor <- 'chestnut, prince'

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t, i)

#########################################################
############ Match to Hall/Fouirnaies
########################################################

# ********* 4 YEAR TERMS FOR HOUSE IN ALABAMA ***************************

library(readstata13)

### Hall and Fournaies 2018, Covers 1988 - 2014
### --> https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/PGCVDP
hf_data <- read.dta13("~/Dropbox/Data/Hall_Fouirnaies_2018/How_Do_interest_Groups_Seek_Access_to_Committees.dta")
hf_data <- filter(hf_data, state == this_state)
hf_data <- hf_data[,c(colnames(hf_data)[c(1:3, 7:14, 204, 208, 217)], colnames(hf_data)[grepl('cmt_', colnames(hf_data))])]
hf_data$term <- paste0(hf_data$year + 1, "_", hf_data$year + 4)
hf_data$chamber <- ifelse(hf_data$chamber == 'senate', 'Senate', "House")

### Subset
hf_data <- filter(hf_data, year > min_year - 5) %>% distinct() %>% select(-MajorityMember)

### Merge
LES <- left_join(LES, select(hf_data, -year), by = c('term' = 'term', 'chamber' = 'chamber', 'klarner_id' = 'CandId'))

### Fix Leadership Error -- Not clear who was minority leader that term
LES[LES$sponsor == "guin, ken" & LES$term == "1999_2002",]$MajorityLeader <- 1 #https://en.wikipedia.org/wiki/Ken_Guin

### Check Duplicates
mutate(LES, check_dup = paste(sponsor, term, chamber, sep = "--")) %>%
  mutate(dup = duplicated(check_dup)) %>%
  filter(dup == TRUE)# %>% View()

rm(hf_data)

#################################
### Shor and McCarty Data, 1993 - 2016
####################################

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
# select(LES, sponsor, SM_name)  %>% distinct() %>% filter(stringdist::stringdist(sponsor, tolower(SM_name), method = "jw") > .1) %>% View()

### SM Names Matched to Multiple LES Sponsors
# ---> so this will catch all errors except those where Sponsor A is Matched to Voter Score B, when Sponsor B and Voter A are missing
filter(LES, !is.na(SM_name)) %>%
  group_by(SM_name) %>% 
  summarize(name_matches = paste(unique(sponsor), collapse = "----")) %>% 
  filter(grepl("----", name_matches))

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches: 
# -- Ken Guin = James 'Ken' Guin Jr
# -- Cam Ward == Cameron Robert Ward -- https://www.alreporter.com/2015/07/02/senator-cam-ward-arrested-for-dui/
# -- Barry Mask = Charles Barrett 'Barry' Mask -- https://ballotpedia.org/Charles_Barrett_Mask
# -- Allen Treadaway = Benjamin ALlen Treadaway -- https://vote-al.org/intro.aspx?state=al&id=altreadawaybenjaminallen
# -- Parker Griffith = Rol Park Griffith Jr -- https://en.wikipedia.org/wiki/Parker_Griffith
# -- Mac Buttram = Marvin 'mac' Buttram -- https://votesmart.org/candidate/biography/121507/mac-buttram#.XNmvW-tKjUo
# -- Wes Long = Oliver Wes Long -- https://adambrown.info/p/research/legislators/members/alabama/lower/oliver-wesley-long-60
# -- Ed Henry = William 'Ed' Henry -- https://en.wikipedia.org/wiki/Ed_Henry_(Alabama_politician)

mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

#### FIX MISMATCHES
LES[LES$sponsor %in% c('drake, dickie'), c('SM_name', 'SM_party', 'np_score')] <- NA

### NAME FIX
LES[LES$sponsor == "fridy, mall",]$sponsor <- 'fridy, matt'

#### FILL MISSING
# filter(LES, is.na(np_score) & !(term %in% c('2017_2018'))) %>% select(sponsor, klarner_name, data_name, term, chamber, np_score) %>% distinct() %>% as.data.frame()# %>% View()
# filter(ideo, grepl('chestnut', tolower(name))) %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

name_matches <- data.frame(LES_name = 'rogers, mike', SM_name = 'Rogers, Michael') # Maiden name
name_matches <- add_row(name_matches, LES_name = 'rogers, john w.', SM_name = 'Rogers Jr, John W')
name_matches <- add_row(name_matches, LES_name = 'figures, michael a.', SM_name = 'Figures')
name_matches <- add_row(name_matches, LES_name = 'williams, jack 1', SM_name = 'Williams, Jack D,') # = District 47
name_matches <- add_row(name_matches, LES_name = 'williams, jack 2', SM_name = 'Williams, Jack W.')
name_matches <- add_row(name_matches, LES_name = 'coleman, linda', SM_name = 'Coleman-Madison, Linda')
name_matches <- add_row(name_matches, LES_name = 'coleman, merika', SM_name = 'Coleman-Evans, Merika')
name_matches <- add_row(name_matches, LES_name = 'williams, phil 1', SM_name = 'Williams, Phil') # SENATE
name_matches <- add_row(name_matches, LES_name = 'williams, phil 2', SM_name = 'Williams, Phillip') # HOUSE
name_matches <- add_row(name_matches, LES_name = 'poole, bill', SM_name = 'Poole, William III')
name_matches <- add_row(name_matches, LES_name = 'drake, dickie', SM_name = 'Drake, E. Richard')
#name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

##########
### PARTY SWITCHES
#########
### Lesley Vance -- Switched Dem to Rep in Nov 2010 -- https://www.al.com/spotnews/2010/11/four_state_reps_switch_from_de.html
### Blain Galliher -- Switched Dem to Rep in September 2001 --- https://www.gadsdentimes.com/article/20010907/News/603218973
### Steve Hurst -- Switched Dem to Rep in Nov 2010 -- https://www.al.com/spotnews/2010/11/four_state_reps_switch_from_de.html
### Mike Millican-- Switched Dem to Rep in Nov 2010 -- https://www.al.com/spotnews/2010/11/four_state_reps_switch_from_de.html
### Alan Boothe -- Switched Dem to Rep in Nov 2010 -- https://www.al.com/spotnews/2010/11/four_state_reps_switch_from_de.html
### James M. Martin -- Lost 2010 Race; Switched to Rep. for 2014 election -- https://ballotpedia.org/James_Martin_(Alabama)
### Gerald Dial -- Switched parties in 2010 Election after being out a term -- https://www.tuscaloosanews.com/article/DA/20091013/News/606112100/TL/
### Jimmy Holley -- Switched Dem to Rep in Jan 2008 -- https://www.dothaneagle.com/news/jimmy-holley-switches-to-republican-party/article_9782ebe9-7a40-5f19-b0b1-2b031236edd3.html
### Jack Biddle -- Switched Dem to Rep in Mid 1980s -- Coded wrong in Klarner -- https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=6698
### Alan Harper -- Switched Dem to Rep in February 2012 -- https://www.tuscaloosanews.com/news/20120207/ala-rep-alan-harper-switches-to-republican-party
### Jerry Fielding -- Switched Dem to Rep in October 2012 -- https://www.wltz.com/2012/10/04/long-time-democrat-senator-jerry-fielding-switches-party/
### Jeff Enfinger -- Switched Rep to Dem in 2000 -- https://www.al.com/breaking/2010/10/former_state_sen_jeff_enfinger.html
### Daniel Boman -- Switch Rep to Dem in May 2011 -- https://www.gadsdentimes.com/news/20110526/west-alabama-legislator-switches-to-democratic-party


###### Updating Party for folks who switched mid-term -- See above -- Switching only if within ~first year of four
LES[LES$sponsor == 'vance, lesley' & LES$term == '2011_2014',]$party <- 'r'
LES[LES$sponsor == 'hurst, steve' & LES$term == '2011_2014',]$party <- 'r'
LES[LES$sponsor == 'millican, mike' & LES$term == '2011_2014',]$party <- 'r'
LES[LES$sponsor == 'boothe, alan' & LES$term == '2011_2014',]$party <- 'r'
LES[LES$sponsor == 'holley, jimmy w.' & LES$term == '2007_2010',]$party <- 'r'
LES[LES$sponsor == 'biddle, jack' & LES$term == '1999_2002',]$party <- 'r'
LES[LES$sponsor == 'harper, alan' & LES$term == '2011_2014',]$party <- 'r'
# LES[LES$sponsor == 'fielding, jerry l.' & term == '2011_2014',]$party <- 'r' # Switched Oct. 2012
LES[LES$sponsor == 'enfinger, jeff' & LES$term == '1999_2002',]$party <- 'd'
LES[LES$sponsor == 'boman, daniel h.' & LES$term == '2011_2014',]$party <- 'd'

#### Only need to do those with Multiple SM Rows
# filter(LES, grepl('enfinger', sponsor)) %>% select(sponsor, term, chamber, party, SM_name, SM_party, np_score)
# filter(ideo, grepl('harper', tolower(name)))

party_switch <- data.frame(LES_name = 'vance, lesley', SM_name = 'Vance, Lesley') 
party_switch <- add_row(party_switch, LES_name = 'millican, mike', SM_name = 'Millican, Michael')
party_switch <- add_row(party_switch, LES_name = 'hurst, steve', SM_name = 'Hurst, Ste') ### Loop Regexes to catch Steve and Stephen
party_switch <- add_row(party_switch, LES_name = 'boothe, alan', SM_name = 'Boothe, Alan')
party_switch <- add_row(party_switch, LES_name = 'holley, jimmy w.', SM_name = 'Holley, Jimmy')
party_switch <- add_row(party_switch, LES_name = 'harper, alan', SM_name = 'Harper, Alan')
party_switch <- add_row(party_switch, LES_name = 'galliher, blaine', SM_name = 'Galliher, Blaine')
party_switch <- add_row(party_switch, LES_name = 'dial, gerald', SM_name = 'Dial, Gerald')
#party_switch <- add_row(party_switch, LES_name = 'xxxxxxx', SM_name = 'xxxxxxx')
# party_switch <- add_row(party_switch, LES_name = 'xxxxxxx', SM_name = 'xxxxxxx')

for(i in 1:nrow(party_switch)){
  LES[LES$sponsor == party_switch[i,]$LES_name & LES$party == 'd',]$SM_name <- ideo[ideo$party == 'D'  & grepl(party_switch[i,]$SM_name, ideo$name),]$name
  LES[LES$sponsor == party_switch[i,]$LES_name & LES$party == 'd',]$SM_party <- ideo[ideo$party == 'D' & grepl(party_switch[i,]$SM_name, ideo$name) ,]$party
  LES[LES$sponsor == party_switch[i,]$LES_name & LES$party == 'd',]$np_score <- ideo[ideo$party == 'D' & grepl(party_switch[i,]$SM_name, ideo$name),]$np_score
  LES[LES$sponsor == party_switch[i,]$LES_name & LES$party == 'r',]$SM_name <- ideo[ideo$party == 'R'  & grepl(party_switch[i,]$SM_name, ideo$name),]$name
  LES[LES$sponsor == party_switch[i,]$LES_name & LES$party == 'r',]$SM_party <- ideo[ideo$party == 'R' & grepl(party_switch[i,]$SM_name, ideo$name),]$party
  LES[LES$sponsor == party_switch[i,]$LES_name & LES$party == 'r',]$np_score <- ideo[ideo$party == 'R' & grepl(party_switch[i,]$SM_name, ideo$name),]$np_score
}


#### *** Jame M. Martin -- Name Varies in SM Data
LES[LES$sponsor == 'martin, james m. (jimmy)' & LES$party == 'd',]$SM_name <- ideo[ideo$party == 'D'  & ideo$name == 'Martin, James',]$name
LES[LES$sponsor == 'martin, james m. (jimmy)' & LES$party == 'd',]$SM_party <- ideo[ideo$party == 'D' & ideo$name == 'Martin, James' ,]$party
LES[LES$sponsor == 'martin, james m. (jimmy)' & LES$party == 'd',]$np_score <- ideo[ideo$party == 'D' & ideo$name == 'Martin, James',]$np_score
LES[LES$sponsor == 'martin, james m. (jimmy)' & LES$party == 'r',]$SM_name <- ideo[ideo$party == 'R'  & ideo$name == 'Martin, James M',]$name
LES[LES$sponsor == 'martin, james m. (jimmy)' & LES$party == 'r',]$SM_party <- ideo[ideo$party == 'R' & ideo$name == 'Martin, James M',]$party
LES[LES$sponsor == 'martin, james m. (jimmy)' & LES$party == 'r',]$np_score <- ideo[ideo$party == 'R' & ideo$name == 'Martin, James M',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)
#rm(ideo, ideo_matches, LES_match, ideo_match, check_last, i)


############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################

### REMOVE NICKNAMES
LES$sponsor <- gsub('  +', ' ', gsub('\\([^\\)]+\\)', '', LES$sponsor))

### REMOVE NUMBERS FROM SPONSOR VAR --- Indicates Identical Names
# filter(LES, grepl(" [0-9]$", sponsor)) %>% select(1:6, district) %>% arrange(sponsor)
LES[LES$sponsor == "williams, jack 1",]$sponsor <- "williams, jack d."
LES[LES$sponsor == "williams, jack 2",]$sponsor <- "williams, jack w."
LES[LES$sponsor == "williams, phil 1",]$sponsor <- "williams, phillip w."
LES[LES$sponsor == "williams, phil 2",]$sponsor <- "williams, phil"
LES$sponsor <- gsub(' [0-9]$', '', LES$sponsor)

### Eliminate Excess White Space
LES$sponsor <- str_trim(LES$sponsor)

### Fix Names
# arrange(LES, data_name, term, chamber) %>% View()
LES[LES$sponsor == "crigler, r. p. jr.",]$sponsor <- "crigler, richard phillip jr."
LES[LES$sponsor == "lindsey, w. h.",]$sponsor <- "lindsey, wallace henry"
LES[LES$sponsor == "ward, cam",]$sponsor <- "ward, cameron robert"
LES[LES$sponsor == "glover, rusty",]$sponsor <- "glover, bejamin nash iii"
LES[LES$sponsor == "mask, barry",]$sponsor <- "mask, charles barrett"
LES[LES$sponsor == "griffith, parker",]$sponsor <- "griffith, rolf parker jr."
LES[LES$sponsor == "guin, ken",]$sponsor <- "guin, james ken jr."
LES[LES$sponsor == "treadaway, allen",]$sponsor <- "treadaway, benjamin allen"
LES[LES$sponsor == "buttram, mac",]$sponsor <- "buttram, marvin"
LES[LES$sponsor == "long, wes",]$sponsor <- "long, oliver wes"
LES[LES$sponsor == "henry, ed",]$sponsor <- "henry, william edward"
LES[LES$sponsor == "drake, dickie",]$sponsor <- "drake, edgar richard"
LES[LES$sponsor == "harbison, cory",]$sponsor <- "harbison, corey"
# LES[LES$sponsor == "zzzzzzzz",]$sponsor <- "zzzzzzzz"


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1999 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1999:2010) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2011:2020) & LES$chamber == 'House'  & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1999 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1999:2010) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2011:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


##############################################
###  Save
##############################################

### Check Missingnes
LES %>%
  group_by(chamber, term) %>%
  summarize(
    Election_Data_Missing = round(sum(is.na(klarner_id))/n(), 2),
    CM_Leadership_Missing = round(sum(is.na(SpeakerHouse))/n(), 2),
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
  scale_color_manual(values=c("dodgerblue2", "gray50", "red2"))

##### CHECK OUTLIERS
## *** Remaining = ['enfinger, jeff'; 'fielding, jerry l.'] = No matching SM record for correct party given timing of switch
# filter(LES, party == 'd' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & np_score < 0) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

