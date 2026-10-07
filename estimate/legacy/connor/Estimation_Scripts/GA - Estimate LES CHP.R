
#####################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** GEORGIA *** BY SESSION
#####################################

###################################
## SPECIAL SESSIONS:
## ---- Main Session is full two-years; special sessions occur in specific year; bills do not appear to carryover.
## ---- Separate files; bill numbers re-start, but have X[0-9] appended
## MEMBER LISTS:
## ---- Term by Term Assembly Info: https://en.wikipedia.org/wiki/146th_Georgia_General_Assembly
## ----> See linked rosters at bottom of each term page - connects to rosters saved in internet archive
## ---- http://www.house.ga.gov/Representatives/en-US/HouseMembersList.aspx
## ---- http://www.senate.ga.gov/senators/en-US/SenateMembersList.aspx
## PROCESS:
## ---- http://www.accg.org/library/how_a_bill_becomes_law.pdf
## ---- http://www.legis.ga.gov/Joint/LegCounsel/Documents/Legislative_Terms_associated_with_GA_General_Assembly.pdf
## Sponsorship/Authorship
## -- 
###########################
## ********* NOTES:
# (1) 'house 2nd read engrossed prevailed'  --> CODED AS ABC ---> But seems to happen more like simultaneously
# --- NOT CODING 'house notice of motion to engross' as abc bc happens at introduction
# --> Notice made at introduction, eventual passage of motion prevents bill from being amended in comm or on floor (see: http://www.legis.ga.gov/Joint/LegCounsel/Documents/Legislative_Terms_associated_with_GA_General_Assembly.pdf)
# --> in senate: "When a motion to engross is made, the motion shall be debatable. The debate is limited to ten minutes in support of such motion and ten minutes in opposition to such motion."
# ----> From 2013 rules: http://www.senate.ga.gov/sos/Documents/senaterules2013.pdf
######################

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
library(foreach)
library(inexact)
library(tibble)

this_state <- 'GA'
keep_types <- c("HB", "SB")

#### Output Directory
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

terms = 2023
sessions <- sort(gsub('.+Details_|.csv', '', bill_files))
t = terms
t_plus_one = t + 1
t_yrs <- as.numeric(terms)
t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 1}'))
data_files <- list.files(glue("../../../State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
sessions <- gsub('.+Bill_Details_|.csv', '', bill_files)
rm(data_files, bill_files)
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
#### Check SS BIll Types 
# table(SS_bills$bill_type)

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[3]



### Formulate 2-year terms -- Cover both regular and special sessions
t_yrs <- as.character(glue('{t}_{t+1}'))
t_sessions <- sessions[grepl(gsub('_', '|', t_yrs), sessions)]

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
    bill_path2 <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{s}.csv")
    s_bills <- read.csv(bill_path2)
    bills <- bind_rows(bills, s_bills)
  }
  rm(s, s_bills)
}

### Clean Term/Session Variables
bills$term <- t_yrs
bills$session_year <- gsub('-', '_', str_extract(bills$session, glue('{t}-{t+1}|{t}|{t+1}')))

bills$session <- str_trim(gsub(glue("{t}-{t+1}|{t}|{t+1}"), '', bills$session))
bills$session <- recode(bills$session, 'Regular Session' = 'RS', '1st Special Session' = 'SS1', 'Special Session' = 'SS1', 
                        '2nd Special Session' = 'SS2', '3rd Special Session' = 'SS3', '4th Special Session' = 'SS4')
bills$session <- paste(bills$session_year, bills$session, sep = '-')

### Check for duplicates
if(nrow(bills) != nrow(distinct(bills))){
  cat(" \n ~~~> DUPLICATE BILLS \n\n .")
  break
}

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_number)
special_nums <- str_extract(bills$bill_id, 'EX[0-9]+$')
bills$bill_id <- gsub('EX[0-9]+$', '', bills$bill_id)
bill_parts <- str_split_fixed(bills$bill_id, ' ', 2)
bills$bill_id <- paste0(bill_parts[,1], str_pad(bill_parts[,2], 4, pad = '0'), ifelse(is.na(special_nums), '', paste0('-', special_nums)))
rm(special_nums, bill_parts)

############### Drop Resolutions, Messages, Communications, Reports
all_bills <- bills
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)

############### Standardize Sponsors + Extracting + Ensuring Cosponsorship match down the line
bills$sponsors <- tolower(bills$sponsors)
bills$sponsors <- gsub('á', 'a', bills$sponsors)
bills$sponsors <- gsub('é', 'e', bills$sponsors)
bills$sponsors <- gsub('ó', 'o', bills$sponsors)
bills$sponsors <- gsub('í', 'i', bills$sponsors)

#### Adjusting for Bills By Request -- Need to do BEFORE splitting
if(any(grepl('\\(br\\)|by request| br$', bills$sponsors))){
  print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
  #print(glue("-----> KEEPING {nrow(filter(bills, grepl('(br)|by request', introducers)))} bill(s) introduced BY REQUEST"))
}



### Manual Fixes
if(t_yrs == "2001_2002"){
  bills$sponsors <- gsub('jackson, bill', 'jackson, william', bills$sponsors)
  bills$sponsors <- gsub('hudson, sistie', 'hudson, helen', bills$sponsors)
  bills$sponsors <- gsub('o`neal, larry', "o'neal, larry", bills$sponsors)
} else if(t_yrs == "2003_2004"){
  bills$sponsors <- gsub('stephens, mickey', 'stephens, edward', bills$sponsors)  # AKA 'Mickey'
} else if(t_yrs == "2007_2008"){
  bills$sponsors <- gsub('crawford, mack', 'crawford, robert', bills$sponsors)    
}
if( (t >= 2003 & t <= 2008) | (t >= 2013 & t <= 2018)  ){
  bills$sponsors <- gsub('thomas, "able" mable', 'thomas, able', bills$sponsors)      
}
if(t >= 2005 & t <= 2014){
  bills$sponsors <- gsub('williams, "coach"', 'williams, earnest', bills$sponsors)  
}
if(t >= 2007 & t <= 2014){
  bills$sponsors <- gsub('carter, buddy', 'carter, earl', bills$sponsors)   
}
if(t >= 2009 & t <= 2016){
  bills$sponsors <- gsub('jackson, bill', 'jackson, william', bills$sponsors)
}
if(t >= 2009 & t <= 2014){
  bills$sponsors <- gsub('epps, bubber', 'epps, james', bills$sponsors)
}
if(t >= 2015 & t <= 2018){
  bills$sponsors <- gsub('jones, jeff', 'jones, j. b.', bills$sponsors)    
  bills$sponsors <- gsub('rakestraw, paulette', 'braddock-rakestraw, paulette', bills$sponsors)
  bills$sponsors <- gsub('jones ii, harold', 'jones, ii, harold', bills$sponsors)
  bills$sponsors <- gsub('martin iv, p. k.', 'martin, iv, p. k.', bills$sponsors)
  bills$sponsors <- gsub('walker iii, larry ', 'walker, iii, larry ', bills$sponsors)
}

### LES Var
bills$LES_sponsor <- gsub(';.+', '', bills$sponsors)
bills$sponsor_dist <- str_extract(bills$LES_sponsor, '[0-9]+[a-z]+ p[0-9]+$|[0-9]+[a-z]+$') ## In 2003-2004 random p1/p2s at end
bills$LES_sponsor <- str_trim(gsub('[0-9]+[a-z]+ p[0-9]+$|[0-9]+[a-z]+$', '', bills$LES_sponsor))
table(bills$LES_sponsor)

## For Cosponsors: removing everything up to first semicolon if multiple sponsors
bills$cosponsors <- ifelse(grepl(';', bills$sponsors), sub(".+?; ", "", bills$sponsors), '')



###################
###### Merge in S&S Bills
###################
# *** For GEORGIA: Regular Session is full biennium; Special Session bills have EX1/2/3 appended
# *** For NOW: Assuming NO SPECIALS -- Need to update script to pull out EX1/2/3 from newspapers
# ---> ********* Script may need some work once we have those *******************

# max_Hspecial <- filter(bills, grepl("^H", bill_id) & grepl("SS", session)) %>% mutate(num = as.numeric(gsub('-EX[0-9]|[A-Z]+', '', bill_id))) %>% pull(num) 
# max_Hspecial <- ifelse(length(max_Hspecial) > 1, max(max_Hspecial), NA)
# max_Sspecial <- filter(bills, grepl("^S", bill_id) & grepl("SS", session)) %>% mutate(num = as.numeric(gsub('-EX[0-9]|[A-Z]+', '', bill_id))) %>% pull(num) 
# max_Sspecial <- ifelse(length(max_Sspecial) > 1, max(max_Sspecial), NA)

if(t_yrs == "2021_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="SB10001"] = "SB0001"
  SS_bills$bill_id[SS_bills$bill_id=="SB90009"] = "SB0009"
  SS_bills$bill_id[SS_bills$bill_id=="HB10001"] = "HB0001"
} 

if (t_yrs == "2023_2024"){
  SS_bills$bill_id[SS_bills$bill_id=="SB10001"] = "SB0001"
}

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, Title) %>% 
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
missing_SS_bills$bill_id

# now check to see if there are duplicate joins


duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills, 
            by = c("bill_id", "term")) %>%
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
    select(bill_id,term,session,year) %>% mutate(SS = 1) %>% distinct()
  
  
  
  bills <- bills %>% 
    left_join(SS_term %>% select(-year), by = c("bill_id", "term", "session")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS)) %>% 
    distinct()
} else {
  orig_row_n = c(nrow(bills),nrow(SS_term))
  bills <- bills %>% 
    left_join(SS_term %>% select(-Title) %>% distinct(), by = c("bill_id", "term")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  SS_term = SS_term %>% 
    left_join(bills %>% select(bill_id,term,session),by=c("bill_id","term"))
  if(!identical(c(nrow(bills),nrow(SS_term)),orig_row_n )){print("merge failed"); break}
}

rm(all_bills, missing_SS_bills, duplicate_SS_bills)

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
  anti_join(SS_term, bills, by = c("bill_id", "term", "session"))
  
  # PVS bills that are duplicated in bills. not necessarily a problem!
  SS_term %>% group_by(bill_id, term) %>%
    mutate(count = n()) %>% filter(count > 1) %>% arrange(bill_id)
  
  # see if they are equal after removing double-counted bills. this is a crude proxy, you should examine more closely
  all.equal(SS_in_bills + nrow(anti_join(SS_term, bills, by = c("bill_id", "term", "session")))+
              nrow(SS_term %>% group_by(bill_id, term) %>%
                     mutate(count = n()) %>% filter(count > 1))/2,
            nrow(SS_term ))
}

############################################################  
############### Code Commemorative
#############################################

bills <- commem_bills %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session'))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)


#### DROP Committee Bills
if(any(grepl('committee', bills$sponsors))){
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

############################################################
############### Code Bill History
############################################################
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

######## Standardize the Bill IDs
bill_hist <- rename(bill_hist, bill_id = bill_number)
special_nums <- str_extract(bill_hist$bill_id, 'EX[0-9]+$')
bill_hist$bill_id <- gsub('EX[0-9]+$', '', bill_hist$bill_id)
bill_parts <- str_split_fixed(bill_hist$bill_id, ' ', 2)
bill_hist$bill_id <- paste0(bill_parts[,1], str_pad(bill_parts[,2], 4, pad = '0'), ifelse(is.na(special_nums), '', paste0('-', special_nums)))
rm(special_nums, bill_parts)

### Clean Term/Session Variables
bill_hist$term <- t_yrs
bill_hist$session_year <- gsub('-', '_', str_extract(bill_hist$session, glue('{t}-{t+1}|{t}|{t+1}')))
bill_hist$session <- str_trim(gsub(glue("{t}-{t+1}|{t}|{t+1}"), '', bill_hist$session))
bill_hist$session <- recode(bill_hist$session, 'Regular Session' = 'RS', 'Special Session' = 'SS1', '1st Special Session' = 'SS1', 
                            '2nd Special Session' = 'SS2','3rd Special Session' = 'SS3', '4th Special Session' = 'SS4')
bill_hist$session <- paste(bill_hist$session_year, bill_hist$session, sep = '-')
bill_hist <- bill_hist %>%
  arrange(session, bill_id, order)

### Standardize Chamber Variable
bill_hist$chamber <- Hmisc::capitalize(str_extract(tolower(bill_hist$action), "^house|^senate"))
bill_hist$chamber <- ifelse(is.na(bill_hist$chamber), '', bill_hist$chamber )
# ----> Blanks are mostly executive/law info

# for some reason, we've got an extra +1 in the orders in the 19-22 scraping. i checked the code and it shouldn't be happening but i am doing this just in case
bill_hist = bill_hist %>% 
  group_by(bill_id, session, term, session_year) %>% 
  mutate(order = order - min(order) + 1)

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c('house committee', 'senate committee')
# ** NOTE: In S, 2nd read indicative of floor action; in H, 2nd read occurs while bill still in committee
abc_t <- c('committee favorably', 'senate read second', 'house third', 'senate third', 'engrossed prevailed', 'tabled',
           'taken from table', 'senate recommitted', 'house recommitted')
# --> not including 'house notice of motion to engross' as that happens at introduction (see notes at top about engross process)
pc_t <- c('house passed', 'senate passed', 'sent to gov', 'transmit.+senate', 'transmit.+house')
# --> from 3 on = cross-checks
law_t <- c('^act [0-9]+', 'signed by gov', '^effective date')
# filter(bill_hist, grepl('rereferred to committee', tolower(action))) %>% select(bill_id, action)
# filter(bill_hist, bill_id == 'SB0541') %>% select(chamber, action_date, action, order)

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
  bill_stages$bill_url <- bills[i,]$bill_url
  all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
  # print(i)
}
options(warn = 1)

### Occasionally a bill's history will have an "Effective Date" entry (which signals it became a law) even though it did not--
### Check for these cases and correct if they appear

if (t_yrs == "2023_2024"){
  all_bill_stages$law[all_bill_stages$bill_id == "SB0303"] <- 0
}

effective_bill_ids <- bill_hist$bill_id[grepl("^Effective Date", bill_hist$action)]
gov_bill_ids <- bill_hist$bill_id[grepl("Signed by Gov", bill_hist$action)]
problem_bills <- setdiff(effective_bill_ids, gov_bill_ids)
if (length(problem_bills) > 0){
  for (i in 1:length(problem_bills)){
    id <- problem_bills[i]
    if (all_bill_stages$law[all_bill_stages$bill_id == id] == 1){
      print(glue("CHECK FOR INCORRECT LAW CODING ({id})"))
    }
  }
} 

### Check Codings
cat('\n')
all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law) ) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2)) %>% 
  as.data.frame() %>% print()
# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% filter(!grepl('^by resolution|^first read|^prefiled', tolower(action))) %>% View()


read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-2}_{t-1}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% filter(!grepl("-S",session)) %>%  print()

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-4}_{t-3}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% filter(!grepl("-S",session)) %>% print()

# Not much information available about bill stages, but can get total number of introductions by chamber/session here: https://www.legis.ga.gov/search

### MERGE to BILL DATA
bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 

### MERGE In COMMEMS
all_bill_stages <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))

### MERGE In S&S
all_bill_stages <- SS_term %>%
    select(bill_id, term, SS, session) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session')) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))

### Adjust Commems if SS == 1 
all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
rm(SS_term)

### Save Stage Info **** MERGE WITH SS **********
write.csv(all_bill_stages, glue('../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv'), row.names = FALSE )

rm(bill_hist_path, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist, aic_t, abc_t, pc_t, law_t)
rm(b_id, s_id, b_spon, bill_hist)


##############################################################
######## Identify Unique Legislators via SLER
##########################################################

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

######## Cosponsorship Info
all_sponsors$num_cosponsored_bills <- NA
for(i in 1:nrow(all_sponsors)){
  c_sub <- filter(bills, substring(bill_id, 1, 1) == all_sponsors[i,]$chamber)
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, c_sub$cosponsors))
  ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  # all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
}
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))
if(min(all_sponsors$num_cosponsored_bills) < 0){
  print("negative cosponsors"); break
} else{print("cosponsors valid")}

###################
### CLEAN NAMES
###################

all_sponsors$last_name <- gsub(',.+', '', all_sponsors$LES_sponsor)
all_sponsors$suffix <- gsub('^, |\\.,$|,$', '', str_extract(all_sponsors$LES_sponsor, ',.+,'))
all_sponsors$first_name <- gsub('.+, ', '', all_sponsors$LES_sponsor)
all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update Last Names for Matching 
if(t >= 2001 & t <= 2012){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "benfield, stephanie", "stuckeybenfield", all_sponsors$last_name)
}
if(t_yrs == "2003_2004"){ # ALisha Morgan Thomas
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "morgan, alisha", "thomas", all_sponsors$last_name)
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "greene-johnson, teresa", "green-johnson", all_sponsors$last_name)
}
if(t_yrs == '2017_2018'){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "nelson, sheila", "clarknelson", all_sponsors$last_name)
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "lopez romero, brenda", "lopez", all_sponsors$last_name)
}




all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(LES_sponsor,substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name, suffix))


legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(t)) | startsWith(legiscan_sessions,as.character(t+1))]

legiscan = foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% distinct()


if(t_yrs == "2021_2022") {
  legiscan = bind_rows(legiscan %>% mutate(role = ifelse(people_id == 20325, "Sen", role)),
                       legiscan %>% filter(people_id == 20325) %>% mutate(role = "Rep", district = "HD-044"))
  
}
if (t_yrs == "2023_2024") {
  # One legislator appears twice--eliminate that
  legiscan = legiscan %>% filter(people_id != 23975)
}

legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  mutate(match_name_chamber = tolower(paste0(last_name,", ",first_name,"-",substr(district,1,1))))

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
        "lopez romero, brenda-h" = NA_character_,
        "gardner, patricia-h" = NA_character_,
        "gordon, j. craig-h" = NA_character_,
        "tate, horacena-s" = NA_character_,
        "summers, carden-s" = NA_character_,
        "howard, henry-h" = 'howard, henry "wayne"-h',
        "marin, pedro-h" = 'marin, pedro "pete"-h',
        "oliver, mary-h" = "oliver, mary margaret-h",
        "stephens, edward-h" = NA_character_,
        "hill, jack-s" = NA_character_,
        "dugan, michael-s" = NA_character_,
        "stephenson, pam-h" = NA_character_,
        "cooke, kevin-h" = NA_character_,
        "thomas, mable-h" = 'thomas, "able" mable-h',
        "rhett, michael-s" = "rhett, michael 'doc'-s",
        "williamson, hugh-h" = "williamson, bruce-h",
        "yearta, bill-h" = NA_character_,
        "powell, jay-h" = NA_character_,
        "hatchett, james-h" = "hatchett, matt-h",
        "williams, mary-h" = NA_character_,
        "williams, noel-h" = "williams, jr., noel-h"
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
        "kausche, angelika-h" = NA_character_,
        "morris, greg-h" = NA_character_,
        "howard, henry-h" = 'howard, henry "wayne"-h',
        "marin, pedro-h" = 'marin, pedro "pete"-h',
        "oliver, mary-h" = "oliver, mary margaret-h",
        "sharper, dexter-h" = NA_character_,
        "metze, marie-h" = NA_character_,
        "hopson, camia-h" = NA_character_,
        "taylor, rhonda-h" = NA_character_,
        "stephens, edward-h" = NA_character_,
        "rhett, michael-s" = "rhett, michael 'doc'-s",
        "nelson, sheila-h" = NA_character_,
        "deloach, homer-h" = "deloach, buddy-h",
        "smith, tyler-h" = "smith, tyler paul-h",
        "jackson, edna-h" = NA_character_,
        "smith, richard-h" = NA_character_,
        "hatchett, james-h" = "hatchett, matt-h",
        "williams, mary-h" = NA_character_,
        "williams, noel-h" = "williams, jr., noel-h"
      )
    )
  
  
}

if(t_yrs == "2023_2024"){
  all_sponsors2 = # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "panitch, esther-h" = NA_character_,
        "tate, horacena-s" = NA_character_,
        "adeyina, segun-h" = NA_character_,
        "howard, karlton-h" = NA_character_,
        "naghise, tish-h" = NA_character_,
        "richardson, gary-h" = NA_character_,
        "smith, richard-h" = NA_character_,
        "orrock, nan-s" = NA_character_,
        "marin, pedro-h" = 'marin, pedro "pete"-h',
        "oliver, mary-h" = "oliver, mary margaret-h",
        "paris, miriam-h" = NA_character_,
        "smith, michael-h" = NA_character_,
        "willis, inga-h" = NA_character_,
        "sampson, david-h" = NA_character_,
        "glanton, mike-h" = NA_character_,
        "smith, tyler-h" = "smith, tyler paul-h",
        "hatchett, james-h" = "hatchett, matt-h",
        "williams, mary-h" = "williams, mary frances-h",
        "williams, noel-h" = "williams, jr., noel-h"
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

###########################################################################
############### Estimate Scores + Add in Relatd Variables
###########################################################################

### Check if bills in data without an ID'd sponsor
# filter(bills, !(bills$primary_sponsor %in% legis_data$data_name))
bills <- bills %>% #select(-sponsor) %>%
  rename(sponsor = LES_sponsor) %>%
  mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'H', 'H', 'S'))

### Standard LES: Same as Congressional Measure
cat('------> Estimating LES Scores ')
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
LES[LES$LES == 0 & is.na(LES$num_sponsored_bills), c('num_sponsored_bills', 'sponsor_pass_rate', 'sponsor_law_rate', "num_cosponsored_bills")] <- 0

############### Save LES
write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
rm(LES)


cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************"))


################# ****** END LOOP

#agg_stats, matches
rm(all_sponsors, bills, legis_data, SS_bills, elec_year, i, keep_types)
rm(k_matches, klarner_sub, m_sub, km, bill_path, t_yrs, t_sessions, calc_LES) # c_sub
rm(t, terms, klarner_gs, c_sub, match_name2, commem_bills)

########################################################################################################################################################
############################################################################################################################################################# NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
### http://www.house.ga.gov/Representatives/en-US/HouseMembersList.aspx
### http://www.senate.ga.gov/senators/en-US/SenateMembersList.aspx
# ---> If not on these lists for a particular term, typically means they were never seated

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1 bill(s) without a sponsor
# APPOINTED/WON H SPECIAL: 
# -- GARDNER (pat)
# -- O'NEAL (larry)
# APPOINTED/WON S SPECIAL: 
# -- SHAFER (david) 
# -- WILLIAMS (roger, won't show bc of fixed duplicate)
# NAME FIX:
# -- HUDSON (Sistie) == Helen 'Sistie' Hudson
# -- O`NEAL --> O'NEAL, Larry
# IN HOUSE: SAILOR, MADDOX, REESE, DELOACH, ROBERTS, BLACK
# IN SENATE: HOOKS, THOMAS

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# APPOINTED/WON H SPECIAL: 
# -- SNOW (past inc) 
# -- JENKINS (CHARLES, won't show bc of fixed duplicate)
# NAME FIX:
# -- MORGAN --> Alisha MORGAN-THOMAS
# -- MICKEY STEPHENS = ED STEPHENS (served 2002-2004, then again 2008+)
# IN HOUSE: NEAL, MAXWELL, WILLIAMS (earnest), DIX, ANDERSON, RYNDERS, SHOLAR
# -- Note: neal won a 2004 special, but isn't on roster for some reason 
# IN SENATE: BOWEN

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# APPOINTED/WON H SPECIAL: 
# -- EVERSON
# -- HOWARD (Earnestine, won't show bc duplicate fixed)
# APPOINTED/WON S SPECIAL: 
# -- TARVER
# NAME FIX:
# -- COACH WILLIAMS --> EARNEST WILLIAMS 
# IN HOUSE: THOMAS; MCCLINTON; SAILOR; SIMS
# IN SENATE: HOOKS; STARR

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1 bill(s) without a sponsor
# APPOINTED/WON H SPECIAL: 
# -- RAMSEY 
# -- MADDOX (billy, won't show up bc of fixed duplicate)
# NAME FIXES:
# -- crawford, mack ---> crawford, robert
# IN HOUSE: REECE, HAMILTON, WIX, SINKFIELD, ABRAMS, LUCAS, SIMS, GORDON
# DROP: 
# -- lakly, dan --> replacement matt ramsey sworn in on jan 2007 -- http://www.house.ga.gov/representatives/en-US/member.aspx?Member=190&Session=21

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# APPOINTED/WON H SPECIAL: 
# -- DODSON 
# -- KIDD
# -- PURCELL
# APPOINTED/WON S SPECIAL: 
# -- CARTER (earl)
# -- DAVIS
# -- JAMES (donzella)
# NAME FIX:
# -- epps, bubber --> epps, james
# -- jackson, bill --> jackson, william
# IN HOUSE: SHIPP, YATES, JOHNSON, ABRAMS, MOSBY, RANDALL, FULLERTON 
# -- Shipp resigned April 2009
# -- Johnson resigned August 2009


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# APPOINTED/WON SPECIAL ~ HOUSE: 
# -- BEVERLY
# -- CARSON
# -- DICKEY
# -- DUNAHOO
# -- HIGHTOWER
# -- NIMMER
# -- WAITES
# ---> ROGERS (terry) - won't show up bc fixed duplicate
# APPOINTED/WON SPECIAL ~ SENATE: 
# -- CRANE
# -- WILkINSON
# IN HOUSE: DOBBS, TINUBU, JORDAN, WILLIAMS (earnest), THOMAS, TALTON, STEPHENS, GORDON
# -- Tinubu resigned 12/2011 to run for Congress
# DROP:
# -- SELLIER, tony --> Died Nov 2010
# -- WILLIAMS, mark --> resigned Dec. 2010 to serve commissioner of dept of natural resources
# --> See: https://en.wikipedia.org/wiki/151st_Georgia_General_Assembly

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1 bill(s) without a sponsor
# APPOINTED/WON SPECIAL ~ HOUSE: 
# -- EFSTRATION
# -- MOORE (lost subsequent primary)
# -- STOVER
# -- TARVIN
# -- TURNER
# APPOINTED/WON SPECIAL ~ SENATE: 
# -- BEACH
# -- BURKE
# IN HOUSE: DEFFENBAUGH,THOMAS, DOUGLAS, BENNETT, FLOYD, SIMS (barbara), FRAZIER, MURPHY, HOLMES, EPPS
# -- Murphy passed away Aug 2013
# IN SENATE: HILL, CHANCE, WILLIAMS (tom), JACKSON
# DROP:
# -- JERGUSON (sean) -- Won, then resigned to run for chip rogers senate seat in special (see below article)
# -- STOKELY (robert) -- WOn election, then was appoointed to a judicial position in late 2012 https://ballotpedia.org/Robert_Stokely
# -- ROGERS (chip) -- Resigned Dec 2012 - https://www.mdjonline.com/news/state-rep-from-cherokee-will-run-for-rogers-senate-seat/article_7d065711-addf-512f-82c3-715a44a46409.html
# -- BULLOCH (john) __ won, then resigned in Dec 2012 -- https://ballotpedia.org/John_Bulloch

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 2 bill(s) without a sponsor
# APPOINTED/WON SPECIAL ~ HOUSE: 
# -- BLACKMON
# -- GILLIGAN
# -- LOTT
# -- PIRKLE
# -- PRICE
# -- RAFFENSPERGER
# -- RHODES
# -- CARTER (doreen)
# -- BENNETT (taylor)
# -->  *** last two won't show because duplicated last names ***
# APPOINTED/WON SPECIAL ~ SENATE: 
# -- VANNESS (lost subsequent general)
# -- WALKER
# IN HOUSE: MEADOWS, THOMAS (erica), SMITH, WILLIAMS (earnest), FLOYD, MCCLAIN, SIMS (barbara), EALUM, BRYANT
# IN SENATE: SIMS (freddie), TOLLESON, CRANE
# DROP:
# -- RILEY (lynne) -- resigned in Nov 2014 -- https://ballotpedia.org/Lynne_Riley
# -- CHANNELL (mickey) -- resigned jan 2014 -- http://www.peachpundit.com/2014/11/28/representative-mickey-channell-retiring-legislature/

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# APPOINTED/WON SPECIAL ~ HOUSE: 
# -- CARPENTER
# -- CAUBLE
# -- GONZALEZ
# -- SCHOFIELD
# -- WALLACE
# APPOINTED/WON SPECIAL ~ SENATE: 
# -- JORDAN
# -- KIRKPATRICK
# -- PAYNE
# -- STRICKLAND
# -- WILLIAMS (nikema, won's show bc duplicate last fixed)
# IN HOUSE: 
# -- METZE, BEASLEY-TEAGUE, STOVER, WILLIAMS (earnest), HOWARD, FRAZIER, MCGOWAN, SHARPER
# IN SENATE: 
# -- HILL (resigned feb 2017)
# -- BETHEL (charlie) -- resigned to become appeals judge -- https://en.wikipedia.org/wiki/Charlie_Bethel

# filter(klarner, grepl("williams,", cand) & year >= 2010) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(year) %>% distinct()
# filter(klarner, ddez == 29 & sen == 1) %>% select(cand, year, sen, etype, outcome, ddez, candid)


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

### Manual Fixes
# ### Won in 2018 special -- will fix itself with updated klarner_data
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_id <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_name <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$sponsor <- "owlett, clint"


### ****Still missing*****  --> Rest are missing from Klarner OR 2017-2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[11]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, missing, name_sub, still_missing)

name_matches <- data.frame(LES_name = "o'neal, larry", k_name = 'oneal, larry') # Maiden name
name_matches <- add_row(name_matches, LES_name = 'kidd, e. culver "rusty"', k_name = 'kidd, e. culver (rusty)')
name_matches <- add_row(name_matches, LES_name = 'nimmer, chad', k_name = 'nimmer, john chadwick (chad)')
name_matches <- add_row(name_matches, LES_name = 'hightower, dustin', k_name = 'hightower, d.')
name_matches <- add_row(name_matches, LES_name = 'efstration, chuck', k_name = 'efstration, c. p. (chuck)')
name_matches <- add_row(name_matches, LES_name = 'tarvin, steve', k_name = 'tarvin, thomas s. (steve)')
name_matches <- add_row(name_matches, LES_name = 'burke, dean', k_name = 'burke, k. dean')
name_matches <- add_row(name_matches, LES_name = 'walker, larry', k_name = 'walker, larry 2')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', k_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}
rm(name_matches, i, name_sub)

############## Check for Duplicates
### ---> If two people with same last name and one won in a special, will lead to duplicates
### Remaining = Switched from House to Senate
for(t in unique(LES$term)){
  print(glue(' ************ {t} ***************'))
  check_dup <- filter(LES, term == t) %>%
    group_by(chamber) %>%
    mutate(dup = duplicated(klarner_id)) 
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
LES[LES$sponsor == "howard, ernestine",]$party <- 'd'
LES[LES$sponsor == "howard, ernestine",]$district <- 121
LES[LES$sponsor == "howard, ernestine",]$exper <- 'none'

### 2017-2018 
LES[LES$sponsor == "cauble, geoff", ]$party <- 'r'
LES[LES$sponsor == "wallace, jonathan", ]$party <- 'd'
LES[LES$sponsor == "carpenter, kasey", ]$party <- 'r'
LES[LES$sponsor == "gonzalez, deborah", ]$party <- 'd'
LES[LES$sponsor == "schofield, kim", ]$party <- 'd'
LES[LES$sponsor == "jordan, jennifer", ]$party <- 'd'
LES[LES$sponsor == "payne, chuck", ]$party <- 'r'
LES[LES$sponsor == "kirkpatrick, kay", ]$party <- 'r'
LES[LES$sponsor == "williams, nikema", ]$party <- 'd'

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

### Subset
hf_data <- filter(hf_data, year > min_year - 4) %>% distinct() %>% select(-MajorityMember)

### Merge
LES <- left_join(LES, select(hf_data, -year), by = c('term' = 'term', 'chamber' = 'chamber', 'klarner_id' = 'CandId'))

### Check Duplicates
mutate(LES, check_dup = paste(sponsor, term, chamber, sep = "--")) %>%
  mutate(dup = duplicated(check_dup)) %>%
  filter(dup == TRUE)

### Set Committees to NA for Years without Data -- May have matched candids in year range
set_NA <- colnames(hf_data)
set_NA <- set_NA[!(set_NA %in% c("CandId", 'chamber', "year", "term")) ]
LES[LES$term == '2017_2018', set_NA] <- NA

rm(hf_data, set_NA)

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
#### FOR GA: Added Code to Match Party Switchers if Both Present in Data
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
  ## If Still None, Try Data Last Name
  if(length(check_last) == 0){
    d_name <- str_split(LES[i,]$data_name, " ")[[1]]
    check_last <- which(d_name[length(d_name)] == tolower(ideo$last_name) )
  }
  
  ####### ***** IF MORE THAN ONE MATCH *******
  if(length(check_last) > 1){
    ### Try Data Name
    ideo_match <- filter(ideo[check_last,], match_name == LES[i,]$data_name)
    ### CHeck First Initial
    if(nrow(ideo_match) != 1){
      ideo_match <- ideo[check_last,][substring(gsub('.+, ', '', tolower(ideo[check_last,]$name)), 1, 1) == substring(gsub(".+, ", '', LES[i,]$sponsor), 1, 1) ,]  
    }
    ### Check Last + First Name
    if(nrow(ideo_match) > 1){
      ideo_match <- filter(ideo_match, match_name == gsub(' [a-z]\\.$', '', LES[i,]$sponsor) )    
    }
    ### Check Party if Still Too Long
    if(nrow(ideo_match) == 0){ ideo_match <- ideo[check_last,] }
    if(nrow(ideo_match) == 2 & length(unique(ideo_match$name)) == 1 & any(ideo_match$party == 'R') & any(ideo_match$party == 'D')){
      for(p in unique(LES[LES$sponsor == LES[i,]$sponsor,]$party)){
        LES[LES$sponsor == LES[i,]$sponsor & LES$party == p,]$SM_name <- ideo_match[ideo_match$party == toupper(p),]$name
        LES[LES$sponsor == LES[i,]$sponsor & LES$party == p,]$SM_party <- ideo_match[ideo_match$party == toupper(p),]$party
        LES[LES$sponsor == LES[i,]$sponsor & LES$party == p,]$np_score <- ideo_match[ideo_match$party == toupper(p),]$np_score
      }
      next
    } else if(nrow(ideo_match) > 1){
      ideo_match <- filter(ideo_match, party == toupper(LES[i,]$party))  
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
LES[LES$sponsor %in% c('williamson, bruce', 'crawford, robert m. (mack)', 'howard, henry d. (wayne)'), c('SM_name', 'SM_party', 'np_score')] <- NA
LES[LES$sponsor %in% c('powell, jay', 'hilton, scott', 'shaw, jay', 'smith, charlie jr.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

# == James 'Austin' Scott
LES[LES$sponsor %in% c('scott, austin'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('^h', tolower(name))) %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name) %>% as.data.frame()

name_matches <- data.frame(LES_name = 'beasleyteague, sharon', SM_name = 'Teague, Sharon Beasley')
name_matches <- add_row(name_matches, LES_name = 'braddock, paulette rakestraw', SM_name = 'Rakestraw-Braddock, Paulette')
name_matches <- add_row(name_matches, LES_name = 'crawford, robert m. (mack)', SM_name = 'Crawford, Mack')
name_matches <- add_row(name_matches, LES_name = 'deloach, buddy', SM_name = 'DeLoach, Homer M (Buddy)')
name_matches <- add_row(name_matches, LES_name = 'gillis, hugh', SM_name = 'Gillis Sr, Hugh M')
name_matches <- add_row(name_matches, LES_name = 'graves, tom', SM_name = 'Graves, John Jr.') # John Thomas Graves Jr
name_matches <- add_row(name_matches, LES_name = 'greenjohnson, teresa', SM_name = 'Greene-Johnson, T')
# howard, henry d. (wayne)
name_matches <- add_row(name_matches, LES_name = 'hudson, newt', SM_name = 'Hudson, W. Newt')
name_matches <- add_row(name_matches, LES_name = 'jones, harold v. ii.', SM_name = 'Jones II, Harold V')
name_matches <- add_row(name_matches, LES_name = 'jones, j. b. (jeff)', SM_name = 'Jones, Jeff')
name_matches <- add_row(name_matches, LES_name = 'martin, charles (chuck)', SM_name = 'Martin, Charles Jr.')
name_matches <- add_row(name_matches, LES_name = 'martin, jim 1', SM_name = 'Martin, James')
name_matches <- add_row(name_matches, LES_name = 'martin, p. k.', SM_name = 'Martin IV, P K')
name_matches <- add_row(name_matches, LES_name = 'miller, butch', SM_name = 'Miller, Cecil') # Cecil Terrell 'Butch' MIller
name_matches <- add_row(name_matches, LES_name = 'murphy, quincy', SM_name = 'Murphy, William') # William Quincy Murphy
name_matches <- add_row(name_matches, LES_name = 'powell, jay', SM_name = 'Powell, Alfred Jr.')  # https://justfacts.votesmart.org/candidate/biography/105245/alfred-powell-jr
name_matches <- add_row(name_matches, LES_name = 'ray, billy', SM_name = 'Ray, William II')
name_matches <- add_row(name_matches, LES_name = 'scott, austin', SM_name = 'Scott, James') # James 'Austin' Scott
name_matches <- add_row(name_matches, LES_name = 'smith, charlie jr.', SM_name = 'Smith Jr, Charles C')
name_matches <- add_row(name_matches, LES_name = 'stephens, bill', SM_name = 'Stephens, William')
name_matches <- add_row(name_matches, LES_name = 'stephens, mickey', SM_name = 'Stephens, Edward')
name_matches <- add_row(name_matches, LES_name = 'stuckeybenfield, stephanie', SM_name = 'Benfield, Stephanie')
name_matches <- add_row(name_matches, LES_name = 'thomas, able m.', SM_name = 'Thomas, Mable Able')
name_matches <- add_row(name_matches, LES_name = 'thomas, alisha', SM_name = 'Morgan, Alisha Thomas')
name_matches <- add_row(name_matches, LES_name = 'tolleson, ross', SM_name = 'Tolleson, Thorborn Jr.')
name_matches <- add_row(name_matches, LES_name = 'trammell, bob', SM_name = 'Trammell Jr, Robert T')
name_matches <- add_row(name_matches, LES_name = 'walker, larry 1', SM_name = 'Walker, Lawrence')
name_matches <- add_row(name_matches, LES_name = 'walker, larry 2', SM_name = 'Walker, Larry')
name_matches <- add_row(name_matches, LES_name = 'williamson, bruce', SM_name = 'Williamson III, Hugh B')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

#### Manual Edits (Needs more precision...)
LES[LES$sponsor == 'williams, roger',]$SM_name <-  ideo[ideo$name == 'Williams, Roger' & ideo$party == 'R',]$name
LES[LES$sponsor == 'williams, roger',]$SM_party <- ideo[ideo$name == 'Williams, Roger' & ideo$party == 'R',]$party
LES[LES$sponsor == 'williams, roger',]$np_score <- ideo[ideo$name == 'Williams, Roger' & ideo$party == 'R',]$np_score

# George 'Sonny' Perdue -- Switches to R after 1998
LES[LES$sponsor == 'perdue, sonny',]$SM_name <- ideo[ideo$name == 'Perdue, George' & ideo$party == 'R',]$name
LES[LES$sponsor == 'perdue, sonny',]$SM_party <- ideo[ideo$name == 'Perdue, George' & ideo$party == 'R',]$party
LES[LES$sponsor == 'perdue, sonny',]$np_score <- ideo[ideo$name == 'Perdue, George' & ideo$party == 'R',]$np_score

### Two Jason (Jay) Shaws -- May be related, second on is Jr, but different parties
LES[LES$sponsor == 'shaw, jay' & LES$party == 'd',]$SM_name <-  ideo[ideo$party == 'D' & ideo$name == 'Shaw, Jason',]$name
LES[LES$sponsor == 'shaw, jay' & LES$party == 'd',]$SM_party <- ideo[ideo$party == 'D' & ideo$name == 'Shaw, Jason',]$party
LES[LES$sponsor == 'shaw, jay' & LES$party == 'd',]$np_score <- ideo[ideo$party == 'D' & ideo$name == 'Shaw, Jason',]$np_score
LES[LES$sponsor == 'shaw, jason' & LES$party == 'r',]$SM_name <-  ideo[ideo$party == 'R' & ideo$name == 'Shaw, Jason',]$name
LES[LES$sponsor == 'shaw, jason' & LES$party == 'r',]$SM_party <- ideo[ideo$party == 'R' & ideo$name == 'Shaw, Jason',]$party
LES[LES$sponsor == 'shaw, jason' & LES$party == 'r',]$np_score <- ideo[ideo$party == 'R' & ideo$name == 'Shaw, Jason',]$np_score

#########
### PARTY SWITCHES --> Loop doesn't catch these (mostly) because SM D and R names are different
#######
### C. Ellis BLack -- Switched to R in 2010 - https://ballotpedia.org/Ellis_Black
LES[LES$sponsor == 'black, ellis' & LES$term == "2011_2012",]$party <- 'r'
LES[LES$sponsor == 'black, ellis' & as.numeric(substring(LES$term, 1, 4)) < 2011,]$SM_party <- 'D'
LES[LES$sponsor == 'black, ellis' & as.numeric(substring(LES$term, 1, 4)) >= 2011,]$SM_party <- 'R'
LES[LES$sponsor == 'black, ellis' & as.numeric(substring(LES$term, 1, 4)) < 2011,]$np_score <- ideo[ideo$name == 'Black, C.' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'black, ellis' & as.numeric(substring(LES$term, 1, 4)) >= 2011,]$np_score <- ideo[ideo$name == 'Black, C.' & ideo$party == 'R',]$np_score

### Alan Powell (mispelled in SM Data?)
LES[LES$sponsor == 'powell, alan t.' & LES$party == 'd',]$SM_name <- ideo[ideo$party == 'D'  & ideo$name == 'Powell, Allen T',]$name
LES[LES$sponsor == 'powell, alan t.' & LES$party == 'd',]$SM_party <- ideo[ideo$party == 'D' & ideo$name == 'Powell, Allen T' ,]$party
LES[LES$sponsor == 'powell, alan t.' & LES$party == 'd',]$np_score <- ideo[ideo$party == 'D' & ideo$name == 'Powell, Allen T',]$np_score
LES[LES$sponsor == 'powell, alan t.' & LES$party == 'r',]$SM_name <- ideo[ideo$party == 'R'  & ideo$name == 'Powell, Alan',]$name
LES[LES$sponsor == 'powell, alan t.' & LES$party == 'r',]$SM_party <- ideo[ideo$party == 'R' & ideo$name == 'Powell, Alan',]$party
LES[LES$sponsor == 'powell, alan t.' & LES$party == 'r',]$np_score <- ideo[ideo$party == 'R' & ideo$name == 'Powell, Alan',]$np_score

### Ann Purcell
LES[LES$sponsor == 'purcell, ann r.' & LES$party == 'd',]$SM_name <- ideo[ideo$party == 'D'  & ideo$name == 'Purcell, Ann R',]$name
LES[LES$sponsor == 'purcell, ann r.' & LES$party == 'd',]$SM_party <- ideo[ideo$party == 'D' & ideo$name == 'Purcell, Ann R' ,]$party
LES[LES$sponsor == 'purcell, ann r.' & LES$party == 'd',]$np_score <- ideo[ideo$party == 'D' & ideo$name == 'Purcell, Ann R',]$np_score
LES[LES$sponsor == 'purcell, ann r.' & LES$party == 'r',]$SM_name <- ideo[ideo$party == 'R'  & ideo$name == 'Purcell, Ann',]$name
LES[LES$sponsor == 'purcell, ann r.' & LES$party == 'r',]$SM_party <- ideo[ideo$party == 'R' & ideo$name == 'Purcell, Ann',]$party
LES[LES$sponsor == 'purcell, ann r.' & LES$party == 'r',]$np_score <- ideo[ideo$party == 'R' & ideo$name == 'Purcell, Ann',]$np_score

### Gerald Greene
LES[LES$sponsor == 'greene, gerald' & LES$party == 'd',]$SM_name <- ideo[ideo$party == 'D'  & ideo$name == 'Greene, Gerald E',]$name
LES[LES$sponsor == 'greene, gerald' & LES$party == 'd',]$SM_party <- ideo[ideo$party == 'D' & ideo$name == 'Greene, Gerald E' ,]$party
LES[LES$sponsor == 'greene, gerald' & LES$party == 'd',]$np_score <- ideo[ideo$party == 'D' & ideo$name == 'Greene, Gerald E',]$np_score
LES[LES$sponsor == 'greene, gerald' & LES$party == 'r',]$SM_name <- ideo[ideo$party == 'R'  & ideo$name == 'Greene, Gerald',]$name
LES[LES$sponsor == 'greene, gerald' & LES$party == 'r',]$SM_party <- ideo[ideo$party == 'R' & ideo$name == 'Greene, Gerald',]$party
LES[LES$sponsor == 'greene, gerald' & LES$party == 'r',]$np_score <- ideo[ideo$party == 'R' & ideo$name == 'Greene, Gerald',]$np_score

### Larry Parrish  
LES[LES$sponsor == 'parrish, larry (butch)' & LES$party == 'd',]$SM_name <- ideo[ideo$party == 'D'  & ideo$name == 'Parrish, Larry J "Butch"',]$name
LES[LES$sponsor == 'parrish, larry (butch)' & LES$party == 'd',]$SM_party <- ideo[ideo$party == 'D' & ideo$name == 'Parrish, Larry J "Butch"' ,]$party
LES[LES$sponsor == 'parrish, larry (butch)' & LES$party == 'd',]$np_score <- ideo[ideo$party == 'D' & ideo$name == 'Parrish, Larry J "Butch"',]$np_score
LES[LES$sponsor == 'parrish, larry (butch)' & LES$party == 'r',]$SM_name <- ideo[ideo$party == 'R'  & ideo$name == 'Parrish, Larry',]$name
LES[LES$sponsor == 'parrish, larry (butch)' & LES$party == 'r',]$SM_party <- ideo[ideo$party == 'R' & ideo$name == 'Parrish, Larry',]$party
LES[LES$sponsor == 'parrish, larry (butch)' & LES$party == 'r',]$np_score <- ideo[ideo$party == 'R' & ideo$name == 'Parrish, Larry',]$np_score

### James Epps
LES[LES$sponsor == 'epps, james a. (bubber)' & LES$party == 'd',]$SM_name <- ideo[ideo$party == 'D'  & ideo$name == 'Epps, James',]$name
LES[LES$sponsor == 'epps, james a. (bubber)' & LES$party == 'd',]$SM_party <- ideo[ideo$party == 'D' & ideo$name == 'Epps, James' ,]$party
LES[LES$sponsor == 'epps, james a. (bubber)' & LES$party == 'd',]$np_score <- ideo[ideo$party == 'D' & ideo$name == 'Epps, James',]$np_score
LES[LES$sponsor == 'epps, james a. (bubber)' & LES$party == 'r',]$SM_name <- ideo[ideo$party == 'R'  & ideo$name == 'Epps, James',]$name
LES[LES$sponsor == 'epps, james a. (bubber)' & LES$party == 'r',]$SM_party <- ideo[ideo$party == 'R' & ideo$name == 'Epps, James',]$party
LES[LES$sponsor == 'epps, james a. (bubber)' & LES$party == 'r',]$np_score <- ideo[ideo$party == 'R' & ideo$name == 'Epps, James',]$np_score

### Kathy Ashe
LES[LES$sponsor == 'ashe, kathy b.' & LES$party == 'd',]$SM_name <- ideo[ideo$party == 'D'  & ideo$name == 'Ashe, Kathy',]$name
LES[LES$sponsor == 'ashe, kathy b.' & LES$party == 'd',]$SM_party <- ideo[ideo$party == 'D' & ideo$name == 'Ashe, Kathy',]$party
LES[LES$sponsor == 'ashe, kathy b.' & LES$party == 'd',]$np_score <- ideo[ideo$party == 'D' & ideo$name == 'Ashe, Kathy',]$np_score
LES[LES$sponsor == 'ashe, kathy b.' & LES$party == 'r',]$SM_name <- ideo[ideo$party == 'R'  & ideo$name == 'Ashe, Kathy B',]$name
LES[LES$sponsor == 'ashe, kathy b.' & LES$party == 'r',]$SM_party <- ideo[ideo$party == 'R' & ideo$name == 'Ashe, Kathy B',]$party
LES[LES$sponsor == 'ashe, kathy b.' & LES$party == 'r',]$np_score <- ideo[ideo$party == 'R' & ideo$name == 'Ashe, Kathy B',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname)

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 2001 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(2001:2004) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2004:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 2001 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(2001:2002) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2002:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################

### REMOVE NUMBERS FROM SPONSOR VAR --- Indicates Identical Names
# filter(LES, grepl(" [0-9]$", sponsor)) %>% select(1:6, district, SM_name) %>% arrange(sponsor)
LES[LES$sponsor == "walker, larry 1",]$sponsor <- "walker, lawrence c. jr."
LES[LES$sponsor == "walker, larry 2",]$sponsor <- "walker, lawrence c. iii"
LES[LES$sponsor == "smith, paul 1",]$sponsor <- "smith, paul e."
LES[LES$sponsor == "martin, jim 1",]$sponsor <- "martin, james f."

### Manual Fixes
LES[LES$sponsor == 'everett, h. doug',]$sponsor <- 'everett, herman doug'
LES[LES$sponsor == 'stanleyturner, lanette',]$sponsor <- 'stanley-turner, lanette'
LES[LES$sponsor == 'sinkfield, mrs. georganna',]$sponsor <- 'sinkfield, georganna'
LES[LES$sponsor == 'tillman, e. c.',]$sponsor <- 'tillman, eugene c.'
LES[LES$sponsor == 'meyervonbremen, mike',]$sponsor <- 'meyer von bremen, michael'
LES[LES$sponsor == 'streat, van sr.',]$sponsor <- 'streat, donnie lavan sr.'
LES[LES$sponsor == 'cagle, l. s. casey',]$sponsor <- 'cagle, lowell s.' # "Casey"
LES[LES$sponsor == 'gordon, j. craig',]$sponsor <- 'gordon, joseph craig'
LES[LES$sponsor == 'dawkinshaigler, dee',]$sponsor <- 'dawkins-haigler, dee'
LES[LES$sponsor == 'hightower, d.',]$sponsor <- 'hightower, dustin'
LES[LES$sponsor == 'efstration, c. p. (chuck)',]$sponsor <- 'efstration, charles p.'
LES[LES$sponsor == 'caldwell, j. jr.',]$sponsor <- 'caldwell, johnnie jr.'
LES[LES$sponsor == 'frye, s.',]$sponsor <- 'frye, spencer'
LES[LES$sponsor == 'belton, d. c. (dave)',]$sponsor <- 'belton, david c.'
LES[LES$sponsor == 'kirk, g. m. (greg)',]$sponsor <- 'kirk, gregory m.'
LES[LES$sponsor == 'harbin, m. h. (marty)',]$sponsor <- 'harbin, marty h.'
LES[LES$sponsor == 'scott, austin',]$sponsor <- 'scott, james austin'
LES[LES$sponsor == 'powell, jay',]$sponsor <- 'powell, alfred j. jr.'
# LES[LES$sponsor == 'zzzzzz',]$sponsor <- 'zzzzz'

### REMOVE NICKNAMES
LES$sponsor <- gsub(' +', ' ', gsub('\\([^\\)]+\\)', '', LES$sponsor))

### Eliminate Excess White Space
LES$sponsor <- str_trim(LES$sponsor)

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
  scale_color_manual(values=c("dodgerblue2",  "gray50", "red2"))

##### CHECK OUTLIERS
# filter(LES, party == 'd' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party, SM_name)
# filter(LES, party == 'r' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party, SM_name)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

