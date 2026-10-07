################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** NEBRASKA *** BY SESSION
##############################################################

###################################
## (SPECIAL) SESSIONS:
## ---- Included, but not identified --> Manually recoding based of daily calendars during this period and actiond dates
## ---- Bills can carryover regular sessions, but numbers RESTART for specials
## ---- Bills with "A" appended are appropriations bills, which accompany fiscal impact bills
## MEMBER LISTS:
## ---- Blue Books: https://nebraskalegislature.gov/about/blue-book.php
## ---- Scanned blue books: http://nebraskaccess.ne.gov/bluebookbios.asp
## PROCESS/RULES:
## ---- https://nebraskalegislature.gov/about/lawmaking.php
## Sponsorship/Authorship
## ---- 
###########################
## NOTES:
## (1) CODING in_majority = 0 FOR ALL MEMBERS 
## ------> Why? Nonpartisan --> No formal party organization (or informal party caucus) that runs legislature; committee chairs, for example, are elected by members via secret ballot (and often cross parties); https://knowledgecenter.csg.org/kc/content/selecting-legislative-committee-chairs-process-largely-controlled-leadership-nebraska-one-no
## ------> See, also, for good quote re coalitions: https://www.pewtrusts.org/en/research-and-analysis/blogs/stateline/2013/01/29/as-legislatures-become-more-partisan-nebraska-holds-out
## (2) MIght be able to get old bills from Wayback Machine? 
# -- https://web.archive.org/web/19990208021706/http://www2.unicam.state.ne.us/
# -- https://web.archive.org/web/20010405150031/http://www.unicam.state.ne.us/documents/ssindex.htm
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
library(tibble)
library(inexact)
library(foreach)

this_state <- 'NE'
keep_types <- c('LB')
spec_elec_codes <- c('s', 'gs')

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

terms <- 2021
t = terms
t_plus_one = t + 1
t_yrs <- as.numeric(terms)
t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 1}'))
data_files <- list.files(glue("../../../State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
sessions <- gsub('.+Bill_Details_|.csv', '', bill_files)
t_sessions = sessions[grepl(as.character(t),sessions) | grepl(as.character(t+1),sessions)]
bill_files <- bill_files[grepl(as.character(t),bill_files) | grepl(as.character(t+1),bill_files)]
rm(data_files)


#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- bind_rows(read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t_plus_one}.csv"),
                               colClasses=c("character")))

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
# t <- terms[1]



### Formulate 2-year terms -- Cover both regular and special sessions

### Skip Previously Estimated
if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
  print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
  next
}

###### SESSION IN PROGRESS
print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_yrs}.csv")
bills <- read.csv(bill_path)

### Clean Term/Session Variables
bills <- bills %>%
  rename(bill_id = bill_number) %>%
  mutate(term = t_yrs,
         session = "RS",
         bill_id = ifelse(grepl('A[0-9]', bill_id), paste(gsub('LBA', 'LB', bill_id), 'A', sep = ''), bill_id),
         bill_id = ifelse(grepl('A[0-9]', bill_id), paste(gsub('LRA', 'LR', bill_id), 'A', sep = ''), bill_id),
         bill_id = ifelse(grepl('A[0-9]', bill_id), paste(gsub('LRCA', 'LRC', bill_id), 'A', sep = ''), bill_id),
         b_id_doc_id = paste(bill_id, gsub('.+DocumentID=', '', bill_url), sep = '-'))

### ***** MANUALLY CODE SPECIAL SESSION BILLS -- find by googling, e.g.: nebraska "101st Legislature, 1st Special Session - Day 1" and manually adjusting days forward to get all bills
# View(filter(bills, duplicated(bill_id)))

spec_bills <- c()
if(t_yrs == "2007_2008"){
  ## Special Session Started Nov 14, 2008 -- https://nebraskalegislature.gov/calendar/legislation.php?day=2008-11-14&sort=introducer
  spec_bills <- c("LB0001-6171", 'LB0002-6197', 'LB0003-6237', 'LR0001-6245', 'LR0002-6266', 'LR0003-6265', 'LR0004-6279', 
                  'LR0005-6283', 'LR0006-6181', 'LR0007-6296', 'LR0008-6297', 'LR0009-6304', 'LR0010-6303', 'LR0011-6280')
}else if(t_yrs == '2009_2010'){
  spec_bills <- c("LB0001-9421", 'LB0002-9429', 'LB0003-9428', "LB0004-9422", 'LB0005-9433', 'LB0006-9440', "LB0007-9441",
                  "LB0008-9465", "LB0009-9466", "LB0010-9443", "LB0011-9457", "LB0012-9455", "LB0013-9438", "LB0014-9468",
                  "LB0015-9453", "LB0016-9473", #"LB0017-zzzz", "LB0018-zzzz", "LB0019-zzzz", "LB0020-zzzz", "LB0021-zzzz",
                  'LR0001-9388', 'LR0002-9437', 'LR0003-9436', 'LR0004-9460', 'LR0005-9312', 'LR0006-9469', 'LR0007-9471',
                  'LR0008-9474', 'LR0009-9480', 'LR0010-9486', 'LR0011-9488', 'LR0012-9489', 'LR0013-9490', 'LR0014-9477', 
                  'LR0015-9475', 'LR0016-9498', 'LR0017-9472', 'LR0018-9492', 'LR0019-9503', 'LR0020-9516', 'LR0021-9523', 
                  'LR0022-9529', 'LR0023-9518', 'LR0024-9385', 'LR0025-9539', 'LR0026-9535', 'LR0027-9536', 'LR0028-9540', 
                  'LR0029-9538', 'LR0030-9537', 'LR0031-9559')
}else if(t_yrs == '2011_2012'){
  spec_bills <- c("LB0001-15149", 'LB0002-15277', 'LB0003-15295', "LB0004-15278",  'LB0005-15121', 'LB0006-15294',
                  "LB0001A-15391", "LB0004A-15381",
                  'LR0001-15246', 'LR0002-15261', 'LR0003-15281', 'LR0004-15282', 'LR0005-15283', 'LR0006-15292', 'LR0007-15293',
                  'LR0008-15296', 'LR0009-15173', 'LR0010-15301', 'LR0011-15297', 'LR0012-15304', 'LR0013-15302', 'LR0014-15319', 
                  'LR0015-15318', 'LR0016-15325', 'LR0017-15305', 'LR0018-15322', 'LR0019-15345', 'LR0020-15343', 'LR0021-15356', 
                  'LR0022-15376', 'LR0023-15386', 'LR0024-15390', 'LR0025-15395', 'LR0026-15399', 'LR0027-15400', 'LR0028-15398', 
                  'LR0029-15394', 'LR0030-15410', 'LR0031-15418', 'LR0032-15440', 'LR0033-15441', 'LR0034-15438', 'LR0035-15435')
} else if(t_yrs == "2021_2022"){
  spec_bills = c("LB0001-46675","LB0002-46676","LB0003-46677","LB0004-46678",
                 "LB0005-46671","LB0006-46674","LB0007-46673","LB0008-46672",
                 "LB0009-46696","LB0010-46695","LB0011-46683","LB0012-46670",
                 "LB0013-46698","LB0014-46667","LB0015-46687")
}

# THEN YOU NEED TO GO AND PUT THOSE BILLS BACK INTO THE COMMEM FILE AS WELL

### Re-code
if(length(spec_bills) > 0){
  bills[bills$b_id_doc_id %in% spec_bills,]$session <- 'SS1'
}
bills <- select(bills, -b_id_doc_id)

### Drop exact duplicates --- shouldn't catch Special Bills because summaries and bill urls will be different
bills <- distinct(bills)

############### Drop Resolutions, Messages, Communications, Reports
all_bills <- bills
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) 

### Break if Duplicates (Special Session Bills!) remain
# filter(bills, session != 'SS1') %>% filter(duplicated(bill_id))
if(nrow(distinct(bills, bill_id, session)) != nrow(bills)){
  print(" ***** CHECK FOR SPECIAL SESSION BILLS *********")
  break
}

##########################
####### Standardize Sponsors

bills$primary_sponsor <- tolower(bills$primary_sponsor)
bills$primary_sponsor <- gsub('á', 'a', bills$primary_sponsor)
bills$primary_sponsor <- gsub('é', 'e', bills$primary_sponsor)
bills$primary_sponsor <- gsub('ó', 'o', bills$primary_sponsor)
bills$primary_sponsor <- gsub('í', 'i', bills$primary_sponsor)
bills$primary_sponsor <- gsub('ñ', 'n', bills$primary_sponsor)
bills$primary_sponsor <- gsub('  +', ' ', bills$primary_sponsor)

### Clean Titles/Boards
bills$primary_sponsor <- gsub('executive board: |, chairperson|speaker ', '', bills$primary_sponsor)

#### LES SPONSOR VARIABLE
bills$LES_sponsor <- bills$primary_sponsor

#### Fix Errors/Name Switches
if(t_yrs == '2009_2010'){
  ### Danielle Nantkes changed name to Danielle Conrad mid-term (but keeping as nantkes for matching purpose; will standardize later)
  bills[bills$LES_sponsor == "conrad",]$LES_sponsor <- "nantkes"
}


###################
###### Merge in S&S Bills
###################
# *** For NEBRASKA: Bills carry over during regular (one biennium), but numbers re-start for all special sessions
# ---> ONLY EVER ONE Special Session --> Merge on ID and Session Type
# ---> Adjusting Special Bills to Regular if BIll NUM > Max in Special

if(t_yrs == "2021_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="LB10001"]="LB0001"
  SS_bills$bill_id[SS_bills$bill_id=="LB20002"]="LB0002"
}

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,summary), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",summary, ignore.case=T)) %>%
  arrange(summary) 
unique(missing_SS_bills$bill_id) 


# now check to see if there are duplicate joins

duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills , 
            by = c("bill_id", "term")) %>%
  group_by(rowname) %>% mutate(count = n()) %>%
  ungroup() %>% 
  select(count,bill_id,term,session, Title, summary) %>% 
  arrange(desc(count),bill_id)

if(nrow(duplicate_SS_bills %>% filter(count > 1)) > 0 ){
  print("you have duplicates"); SS_duplicates_exist <- 1; break
} else{
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
  bills2 <- bills %>% 
    left_join(SS_term %>% select(-Title) %>% distinct(), by = c("bill_id", "term")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  SS_term2 = SS_term %>% 
    left_join(bills %>% select(bill_id,term,session),by=c("bill_id","term"))
  if(!identical(c(nrow(bills2),nrow(SS_term2)),orig_row_n )){print("merge failed"); break} else{
    bills = bills2; SS_term = SS_term2; rm(bills2, SS_term2)
  }
}

### Check Missing
table(bills$SS)
SS_in_bills = sum(bills$SS)
SS_in_PVS = nrow(SS_term  )

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


#######################################################
############### Code Commemorative
#######################################################

bills <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session')) %>% distinct()
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)



###### Drop Committee Bills
if(nrow(filter(bills, grepl("committee| board", LES_sponsor))) > 0 ){
  cat('\n')
  cat(glue("-----> Dropping {nrow(filter(bills, grepl('committee| board', LES_sponsor)))} bill(s) sponsored by COMMITTEE"))
  bills <- filter(bills, !grepl('committee| board', LES_sponsor))    
}

###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "" | is.na(LES_sponsor)) %>% View()
if(nrow(filter(bills, LES_sponsor == '')) > 0 | any(is.na(bills$LES_sponsor))){
  cat('\n')
  cat(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == '')) + sum(is.na(bills$LES_sponsor))} bill(s) without a sponsor"))
  bills <- filter(bills, !(LES_sponsor == '' | is.na(LES_sponsor)))     
}

#######################################################
############### Code Bill History
#######################################################
bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_yrs}.csv")
bill_hist <- read.csv(bill_hist_path)

### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
bill_hist <- bill_hist %>%
  rename(bill_id = bill_number) %>%
  mutate(term = t_yrs,
         session = 'RS',
         bill_id = ifelse(grepl('A[0-9]', bill_id), paste(gsub('LBA', 'LB', bill_id), 'A', sep = ''), bill_id),
         bill_id = ifelse(grepl('A[0-9]', bill_id), paste(gsub('LRA', 'LR', bill_id), 'A', sep = ''), bill_id),
         bill_id = ifelse(grepl('A[0-9]', bill_id), paste(gsub('LRCA', 'LRC', bill_id), 'A', sep = ''), bill_id))

### Code Specials -- Need to do so by date because don't have bill urls here
spec_bills <- gsub('-.+', '', spec_bills)
if(t_yrs == "2007_2008"){
  bill_hist[bill_hist$bill_id %in% spec_bills & bill_hist$action_date >= as.Date("2008-11-01") & bill_hist$action_date <= as.Date("2008-12-31"),]$session <- 'SS1'
}else if(t_yrs == '2009_2010'){
  bill_hist[bill_hist$bill_id %in% spec_bills & bill_hist$action_date >= as.Date("2009-11-01") & bill_hist$action_date <= as.Date("2009-12-31"),]$session <- 'SS1'
}else if(t_yrs == '2011_2012'){
  bill_hist[bill_hist$bill_id %in% spec_bills & bill_hist$action_date >= as.Date("2011-11-01") & bill_hist$action_date <= as.Date("2011-12-31"),]$session <- 'SS1'
}
rm(spec_bills)

#### Re-Order
bill_hist <- arrange(bill_hist, term, session, bill_id, action_date, order)

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c('notice of hearing', 'public hearing', 'com am[0-9]+')
# ---> No record of reporting out; seems like bills can be put on general file without committee acton (in rare cases?) (and also then postponed indefinitely from there)
# ---> see, eg, https://nebraskalegislature.gov/bills/view_bill.php?DocumentID=4395
abc_t <- c('placed on general file', 'placed on select file', 'placed on final', 'advanced to', 'engross', 'enroll',
           '^passed', 'adopted$', 'lost$', 'pending$')
# Advanced to enrollment/engrossment
pc_t <- c('^passed on', '^passed by', '^passed notwith', 'president.+signed', 'speaker.+signed')
## Skipping Passed Over
law_t <- c('approved by gov', 'passed notwithstanding', 'certificate') 
# --> notwithstanding = veto override

### Check Actions
# filter(bill_hist, grepl('adopted$', tolower(action))) %>% distinct(action) %>% View()
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

### Code History Stages for Each Bill --- Subset Hist File, Code, Save, Repeat
options(warn = 2)
for(i in 1:nrow(bills)){
  b_id = bills[i,]$bill_id
  s_id = bills[i,]$session
  b_spon = bills[i,]$LES_sponsor
  hist_sub <- filter(bill_hist, bill_id == b_id, term == t_yrs, session == s_id)
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t, nebraska = TRUE)
  bill_stages$bill_url <- bills[i,]$bill_url
  ### NM: Hard to cross-check with the most of status variables.. not always precise, but should be lower bound (e.g., passed may also have become law, same for final reading)
  if(bill_stages$law == 0 & bills[i,]$status == 'Veto Overridden'){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- bill_stages$law <- 1
  }else if(bill_stages$passed_chamber == 0 & bills[i,]$status %in% c("Governor Veto", 'Passed')){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
  }else if(bill_stages$action_beyond_comm == 0 & bills[i,]$status %in% c("Final Reading", "Select File")){
    bill_stages$action_beyond_comm <- 1
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
rm(b_id, s_id, b_spon, bill_hist)

####################################################
############### Identify Unique Legislators via SLER
####################################################

## Import and Clean Sponsors Name to Match
all_sponsors <- bills %>%
  mutate(chamber = "U") %>%
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

all_sponsors$last_name <- all_sponsors$LES_sponsor
all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)



all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(LES_sponsor)) %>% 
  select(-c(last_name,first_name))


legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(t)) | startsWith(legiscan_sessions,as.character(t+1))]

legiscan = foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% select(-nickname,-votesmart_id,-ballotpedia) %>%  distinct()


# if(t_yrs == "2019_2020") {
#   legiscan = bind_rows(legiscan,
#                        legiscan %>% filter(people_id == 634) %>% mutate(role = "Sen", district = "SD-020"),
#                        legiscan %>% filter(people_id == 14653) %>% mutate(role = "Rep", district = "HD-029"))
#   
# }
# 
# if(t_yrs == "2021_2022") {
#   legiscan = bind_rows(legiscan %>% mutate(role = ifelse(people_id == 20325, "Sen", role)),
#                        legiscan %>% filter(people_id == 20325) %>% mutate(role = "Rep", district = "HD-044"))
#   
# }

legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name, role) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = case_when(
    n >= 2 ~ paste0(substr(first_name,1,1),". ", last_name),
    T ~ last_name)) %>%
  mutate(match_name_chamber = tolower(match_name))

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
        "m. hansen" = "hansen, m.",
        "b. hansen" = "hansen, b."
      )
    )
}

if(t_yrs == "2021_2022"){
  all_sponsors2 = # You added custom matches:
    # You added custom matches:
    # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "m. cavanaugh" = "cavanaugh, m.",
        "m. hansen" = "hansen, m.",
        "b. hansen" = "hansen, b.",
        "jacobson" = NA_character_
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
  distinct() %>% 
  mutate(chamber = "U")


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
filter(bills, !(bills$LES_sponsor %in% legis_data$data_name))
bills <- bills %>% #select(-sponsor) %>%
  rename(sponsor = LES_sponsor) %>%
  mutate(chamber = "U") #ifelse(substring(bill_id, 1, 1) == 'H', 'H', 'S'))

### Standard LES: Same as Congressional Measure
source('../../Estimate LES/calc_LES_fx.R')

LES <- calc_LES(bills, legis_data, t_yrs, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1))
LES$chamber <- "Unicameral"
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
LES_noWeights <- rename(LES_noWeights, LES_nw = LES) %>% mutate(chamber = "Unicameral") %>% select(1:6, LES_nw)
LES <- left_join(LES, LES_noWeights, by = intersect(colnames(LES), colnames(LES_noWeights)))
rm(LES_noWeights)

### Fix Term Variable
LES <- rename(LES, term = session)

#### Merge Agg Stats back in

LES <- legis_data %>%
  select(sponsor, chamber, party,district,  num_sponsored_bills, sponsor_pass_rate, sponsor_law_rate, num_cosponsored_bills) %>%
  mutate(chamber = "Unicameral") %>%
  left_join(LES, ., by = c("sponsor", "chamber")) %>%
  select(-klarner_name) %>% 
  rename(legiscan_id = klarner_id) %>%
  select(sponsor, data_name, legiscan_id, term, chamber, district, party, LES, everything()) %>% 
  mutate(chamber = "Senate")


#### If LES == 0 and --- , "num_cosponsored_bills"
LES[LES$LES == 0 & is.na(LES$num_sponsored_bills), c('num_sponsored_bills', 'sponsor_pass_rate', 'sponsor_law_rate')] <- 0

############### Save LES
write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
rm(LES)


cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************"))


################# ****** END LOOP

rm(all_sponsors, bills, legis_data, SS_bills, commem_bills, elec_year, i, keep_types)
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES) # 
rm(t, terms, klarner_gs) # 


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by GUBERNATORIAL APPOINTMENT --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
### ROSTER DATABASE: 
#########################

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 78 bill(s) without a sponsor
#   session    chamber    N  AIC ABC PASS LAW
# 1      RS Unicameral 1178 1087 554  349 342
# 2     SS1 Unicameral    3    2   2    2   2
### APPOINTED:
# -- FULTON
### DROP:
# -- foley, mike -- resigned Jan 2007, became state auditor as of 1/3/2007


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 38 bill(s) sponsored by COMMITTEE
#   session    chamber    N  AIC ABC PASS LAW
# 1      RS Unicameral 1140 1060 576  409 408
# 2     SS1 Unicameral   15   14   7    4   4
### APPOINTED:
# -- KRIST  (bob)
### NAME SWITCH:
# -- Danielle Nantkes switched to Danielle Conrad
### DROP:
# -- mines, mick -- resigned in October 31, 2007 to become lobbyist -- https://web.archive.org/web/20191003205944/https://journalstar.com/news/local/govt-and-politics/sen-mick-mines-to-leave-legislature/article_42cdf652-cc23-55c0-92be-21662c2de4c6.html


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 39 bill(s) sponsored by COMMITTEE
#   session    chamber    N  AIC ABC PASS LAW
# 1      RS Unicameral 1199 1102 589  502 491
# 2     SS1 Unicameral    8    5   5    4   4
### APPOINTED:
# -- BLOOMFIELD (dave)
# -- LAMBERT  (r. paul)
### DROP:
# -- giese, robert j. -- resigned Nov 2010; won county treasurer election: https://web.archive.org/web/20191003210317/https://journalstar.com/news/local/govt-and-politics/gov-heineman-seeks-applications-for-district-legislative-seat/article_2c02ec2a-a1ae-5b6c-a231-da85382a39a7.html


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 24 bill(s) sponsored by COMMITTEE
#   session    chamber    N  AIC ABC PASS LAW
# 1      RS Unicameral 1173 1075 575  392 390
### DROP:
# -- pankonin, dave -- resigned Aug 2011 -- https://web.archive.org/web/20160329020207/https://journalstar.com/news/unicameral/article_0fb91913-6c82-51a4-bffc-026387deae61.html


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 23 bill(s) sponsored by COMMITTEE
#   session    chamber    N  AIC ABC PASS LAW
# 1      RS Unicameral 1191 1066 635  450 443
### APPOINTED:
# -- FOX (nicole, didn't run again)
# -- SCHNOOR (david)
### DROP:
# -- price, scott -- resigned 11/1/2013 -- http://nebraskaccess.nebraska.gov/scripts/leg_search.asp?name2search=price%2C+scott&freetext=&district=&county=&body=
# -- janssen, charlie -- elected auditor, took new office 1/8/2015: http://nebraskaccess.nebraska.gov/scripts/leg_search.asp?name2search=janssen&freetext=&district=&county=&body=


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 19 bill(s) sponsored by COMMITTEE
#   session    chamber    N  AIC ABC PASS LAW
# 1      RS Unicameral 1165 1090 537  316 311
### APPOINTED:
# -- CLEMENTS (rob)
# -- THIBODEAU (theresa)

# filter(klarner, grepl("clements,", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 2 & sen == 1 & outcome == "w") %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)
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
# LES[LES$data_name %in% "jim w. hall" & LES$term == "2011_2012",]$klarner_id <- NA
# LES[LES$data_name %in% "jim w. hall" & LES$term == "2011_2012",]$klarner_name <- NA
# LES[LES$data_name %in% "jim w. hall" & LES$term == "2011_2012",]$sponsor <- 'hall, jim w.'

### ****Still missing***** ---> Rest are not in Klarner or 2017-2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[1]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl('hall', cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, missing, still_missing)


### Fix Missing
# name_matches <- data.frame(LES_name = 'zzzzzz', k_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')
#
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

### Not In Klarner
fill_missing <- data.frame(LES_name = "fox", new_name = 'fox, nicole', party = 'nonpart', district = 7, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'nonpart', district = zzzz, exper = 'zzzzzz')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'nonpart', district = zzzz, exper = 'zzzzzz')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

#### *** 2017-2018 Appointees: Won't be needed once klarner updates ****
LES[LES$sponsor == "thibodeau", c('party', 'sponsor')] <- list('nonpart', "thibodeau, theresa")
LES[LES$sponsor == "clements", c('party', 'sponsor')] <- list('nonpart', "clements, rob")

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t, fill_missing, i)


#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

#### Clean Names
# filter(LES, grepl("fox", sponsor)) %>% select(1:7)
LES[LES$sponsor == 'brasch, lydia',]$sponsor <- "brasch, lydia n."
# Name Switch mid-2009: http://nebraskaccess.nebraska.gov/scripts/leg_search.asp?name2search=conrad&freetext=&district=&county=&body=
LES[LES$sponsor %in% c('nantkes, danielle', 'conrad, danielle'),]$sponsor <- "conrad, danielle nantkes"


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
hf_data$chamber <- "Senate"

#### Doubling the Senate Rows + Adding back in
# **** ALL data should be right (assuming still in chamber) EXCEPT Majority Member IN STAGGERED STATES ********
staggered <- filter(hf_data, chamber == "Senate")
staggered$year <- staggered$year + 2
staggered$term <- paste0(staggered$year + 1, "_", staggered$year + 2)
staggered$MajorityMember <- NA
hf_data <- bind_rows(hf_data, staggered) %>% arrange(year, chamber)
rm(staggered)

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
# LES[LES$sponsor %in% c('chavez, eleanor', 'baca, gregory a.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches in Which LES First Name != Shor-McCarty First Name
# ---> NO ISSUES
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1991_1992', '2017_2018')) ) %>% group_by(sponsor, party) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('stuthman', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

### Lot of the missing are special election winners in 2016+
name_matches <- data.frame(LES_name = 'avery, bill', SM_name = 'Avery, William')
name_matches <- add_row(name_matches, LES_name = 'chambers, ernest', SM_name = 'Chambers, Ernie')
# name_matches <- add_row(name_matches, LES_name = 'hansen, matt', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'johnson, jerry', SM_name = 'Johnson, Jerry')
name_matches <- add_row(name_matches, LES_name = 'johnson, joel t.', SM_name = 'Johnson, Joel')
name_matches <- add_row(name_matches, LES_name = 'pirsch, pete', SM_name = 'Pirsch, Pete')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########
# filter(LES, grepl("stuth", sponsor)) %>% select(1:7, party)
# filter(ideo, grepl('stuth', tolower(name)))

# **** ODD BECAUSE TECHNICALLY NONPARTISAN BUT USING SM PARTIES (WHICH COME FROM MASKET APPARENTLY) ****

#### Brad Ashford -- Switched R to D in 2011 (after being previously D pre 1989)
LES[LES$sponsor == 'ashford, brad' & LES$term %in% c("2007_2008", '2009_2010'),]$SM_name <-  ideo[ideo$name == 'Ashford, Brad' & ideo$party == 'R',]$name
LES[LES$sponsor == 'ashford, brad' & LES$term %in% c("2007_2008", '2009_2010'),]$SM_party <- ideo[ideo$name == 'Ashford, Brad' & ideo$party == 'R',]$party
LES[LES$sponsor == 'ashford, brad' & LES$term %in% c("2007_2008", '2009_2010'),]$np_score <- ideo[ideo$name == 'Ashford, Brad' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'ashford, brad' & LES$term %in% c("2011_2012", '2013_2014'),]$SM_name <-  ideo[ideo$name == 'Ashford, Brad' & ideo$party == 'D',]$name
LES[LES$sponsor == 'ashford, brad' & LES$term %in% c("2011_2012", '2013_2014'),]$SM_party <- ideo[ideo$name == 'Ashford, Brad' & ideo$party == 'D',]$party
LES[LES$sponsor == 'ashford, brad' & LES$term %in% c("2011_2012", '2013_2014'),]$np_score <- ideo[ideo$name == 'Ashford, Brad' & ideo$party == 'D',]$np_score

#### Abbie Cornett --- Switched to R after 2006
LES[LES$sponsor == 'cornett, abbie',]$SM_name <-  ideo[ideo$name == 'Cornett, Abbie' & ideo$party == 'R',]$name
LES[LES$sponsor == 'cornett, abbie',]$SM_party <- ideo[ideo$name == 'Cornett, Abbie' & ideo$party == 'R',]$party
LES[LES$sponsor == 'cornett, abbie',]$np_score <- ideo[ideo$name == 'Cornett, Abbie' & ideo$party == 'R',]$np_score

#### Patrick Engel --- Switched to R after 2006
LES[LES$sponsor == 'engel, l. patrick',]$SM_name <-  ideo[ideo$name == 'Engel, L. Patrick' & ideo$party == 'R',]$name
LES[LES$sponsor == 'engel, l. patrick',]$SM_party <- ideo[ideo$name == 'Engel, L. Patrick' & ideo$party == 'R',]$party
LES[LES$sponsor == 'engel, l. patrick',]$np_score <- ideo[ideo$name == 'Engel, L. Patrick' & ideo$party == 'R',]$np_score

#### Arnie Stuthman --- Was R?
LES[LES$sponsor == 'stuthman, arnie',]$SM_name <-  ideo[ideo$name == 'Stuthman, Arnie' & ideo$party == 'R',]$name
LES[LES$sponsor == 'stuthman, arnie',]$SM_party <- ideo[ideo$name == 'Stuthman, Arnie' & ideo$party == 'R',]$party
LES[LES$sponsor == 'stuthman, arnie',]$np_score <- ideo[ideo$name == 'Stuthman, Arnie' & ideo$party == 'R',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################

##### Drop Nicknames
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers at End of (Klarner) Names
# filter(LES, grepl(' [0-9]$', sponsor))

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

# ************* CODING ALL IN NEBRAKSA AS NOT IN THE MAJORITY ***************************
# --> Because system is nonpartisan nor formal (or informal) caucus running the chamber
# --> For example, members are elected to committee chairmanships, so power wielded by parties is somewhat decentralized 
# ***************************************************

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
# LES %>%
#   mutate(t_factor = fct_rev(as.factor(gsub('_', '-', term) ))) %>% 
#   filter(party %in% c("d", "r")) %>%
#   ggplot(aes(y = t_factor)) +
#   geom_density_ridges(aes(x = LES, fill = party), alpha = .8, color = "white", from = 0, to = 5) +
#   xlab("LES") + 
#   ylab("Term") + 
#   ggtitle(glue("Legislative Effectiveness in {this_state}")) +
#   scale_fill_cyclical(
#     breaks = c("d", "r"),
#     labels = c('d' = "Democrat", 'r' = "Republican"),
#     values = c("dodgerblue2", "red2"),
#     name = "Party", guide = "legend") +
#   theme_ridges(grid = FALSE) + 
#   facet_wrap(~chamber) + 
#   theme(axis.title.x = element_text(hjust = 0.5),axis.title.y = element_text(hjust = 0.5), legend.position = "bottom")

# ggsave(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Figures/{this_state}_LES_ridgeplot.pdf"), height = 8, width = 7)

#########
ggplot2::ggplot(LES, aes(x = np_score, y = LES, color = party)) + 
  geom_point() + 
  geom_smooth(formula = "y ~ x", method = 'lm', se = FALSE) + 
  facet_wrap(~ term) + 
  scale_color_manual(values=c("dodgerblue2", 'gray50', "red2"))

##### CHECK OUTLIERS with mismatched parties
# filter(LES, party == 'd' & np_score > 0 & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & np_score < 0 & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + in_majority + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

