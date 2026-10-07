################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** MAINE *** BY SESSION
##############################################################

# -------------> CHECK CHAMBER_STATUS_URL in data... why SessionID='' in 117th

###################################
## (SPECIAL) SESSIONS:
## ---- If present, folded into the main legislative term
## MEMBER LISTS:
## ---- See notes below loop
## PROCESS/RULES:
## ---- Process: http://legislature.maine.gov/general/path-of-legislation-in-maine-detailed/9285
## ---- Glossary of Terms: https://legislature.maine.gov/LawMakerWeb/glossary_of_terms.asp
## ---- Glossary of Final Actions: http://www.mainelegislature.org/legis/lio/explanation.html
## Sponsorship/Authorship
## ---- "Bills have one prime sponsor and may have an unlimited number of cosponsors" (Process URL)
###########################
## NOTES:
## (0) NEED TO SKIP 112th -- Don't have first regular session!!!
## (1) The legislature convenes the first wednesday in DECEMBER after the election
## ----> 120th == Dec 2000 -- Dec 2002
## (2) Resolves != Resolutions: "Resolves have the force of law but do not amend statutes directly and are of very limited duration. 
## ---- Resolves are narrow in scope (the instrument for a one time occurrence such a temporary study commission)." (see glossary)
## (3) The format of the actions varies over time; major split is between the 119th and 120th... 
## ---- Starting with the 121st (120th uses both), chamber actions no longer recorded in same place as committee actions
## ---- ALSO NEED TO BE CAREFUL WITH NEWER BILLS to CHECK FINAL STATUS: Sometimes actions don't list signed by governor
## --------> See, e.g., http://legislature.maine.gov/LawMakerWeb/dockets.asp?ID=280008048
## (4) Maine is the only state with Tribal Representatives -- In 1996, they first sponsor a piece of legislation
## ---> All Tribal Reps, 1823+: https://legislature.maine.gov/lawlibrary/tribal-representatives-to-the-maine-legislature-1823/9257/
## ---> For History, see: https://legislature.maine.gov/lawlibrary/history-of-tribal-representation-in-maine/9261
#################################
### ~~~~~~ NOTES ~~~~~~~~~
# (1) How to handle engrossment process? (see process url) 
# -----> Basically bills have to pass both chambers to be engrossed, then pass both again to be enacted..
# -----> FOR NOW: coding bills that are passed to be ENGROSSED in intro chamber as PC
# -----> ALAN AGREES; analagous to a bill needing to be passed again by both chambers after a conference committee
# (2) Bunch of bills with no sponsors --- wasn't able to find the sponsor on the bill, may want to have RAs look into this?
# (3) Keep the Tribal Representatives? No.. For early part of time period, they didn't really sponsor bills. Also can't vote. 

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

this_state <- 'ME'
keep_types <- c('HP', 'SP')

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
# ----> NEED TO SKIP 112th --- DON"T HAVE FIRST REGULAR SESSION
terms <- 2021
t = terms
t_plus_one = t + 1
t_yrs <- as.numeric(terms)
t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 1}'))
data_files <- list.files(glue("../../../State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
sessions <- gsub('.+Bill_Details_|.csv', '', bill_files)
rm(data_files, bill_files)
l_num = 129 +  (t - 2019) / 2
t_sessions = sessions[startsWith(sessions,as.character(l_num)) ]



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

### Identify LD Nums for HPs and SPs
# for(i in 1:nrow(SS_bills)){
#   if(is.na(SS_bills[i,]$LD_num)){
#     match <- filter(commem_bills, term == SS_bills[i,]$term & bill_id == SS_bills[i,]$bill_id)
#     if(nrow(match) == 1){
#       SS_bills[i,]$LD_num <- match$LD_num
#     }else{ # These are all dropped from the analysis (resolutions, orders, no sponsor, etc)
#       #if(!(SS_bills[i,]$bill_id %in% c("HP1938", "HP0987", "HP1232", "HP1251")) ){break}
#     }
#   }
# }

# SS_bills <- filter(SS_bills, !is.na(LD_num))
# rm(match, i)


###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[9]



### Formulate 2-year terms -- Cover both regular and special sessions

### Skip Previously Estimated
if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
  print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
  next
}

###### SESSION IN PROGRESS
print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{l_num}.csv")
bills <- read.csv(bill_path)
bills <- arrange(bills, paper_num, LD_num)

### Drop duplicates
bills <- distinct(bills) 

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = paper_num) %>%
  mutate(term = t_yrs,
         leg_num = l_num,
         session = recode(session, 'First Regular Session' = 'RS1', 'Second Regular Session' = 'RS2', 'First Special Session' = 'SS1', 
                          'Second Special Session' = 'SS2', 'Third Special Session' = 'SS3', 'Fourth Special Session' = 'SS4', 
                          'Fifth Special Session' = 'SS5', 'Sixth Special Session' = 'SS6', 'Seventh Special Session' = 'SS7'),
         LD_num = ifelse(LD_num == '', NA, LD_num)) 

############### Drop Resolutions, Messages, Communications, Reports
all_bills <- bills
## The LD's -- LEGISLATIVE DOCUMENTS -- are the bills we want
bills <- filter(bills, !is.na(LD_num)) 
bills <- filter(bills, !grepl('^resolution|^joint resolution|^a resolution|^a joint resolution', tolower(title)))

if( any(grepl("resolution", substring(tolower(bills$title), 1, 25))) ){
  #filter(bills, grepl('resolution', tolower(bills$title))) %>% View()
  print(' ------------> CHECK IF ANY RESOLUTIONS IN DATA')
  stop()
}

#### Fix Bills without Session
sum(is.na(bills$LD_num))
if(t_yrs == '2003_2004'){
  bills[bills$LD_num == 'LD0195',]$session <- 'RS1'
}else if(t_yrs == '2005_2006'){
  bills[bills$LD_num == 'LD9000',]$session <- 'SS2'
}else if(t_yrs == '2007_2008'){
  bills[bills$LD_num %in% c('LD0166', 'LD0394'),]$session <- 'RS1'
  bills[bills$LD_num == 'LD2324',]$session <- 'SS1'
}

##########################
####### Standardize Sponsors
bills$primary_sponsor <- tolower(bills$primary_sponsor)
bills$primary_sponsor <- gsub('á', 'a', bills$primary_sponsor)
bills$primary_sponsor <- gsub('é', 'e', bills$primary_sponsor)
bills$primary_sponsor <- gsub('ó', 'o', bills$primary_sponsor)
bills$primary_sponsor <- gsub('í', 'i', bills$primary_sponsor)
bills$primary_sponsor <- gsub('ñ', 'n', bills$primary_sponsor)

bills$cosponsors <- tolower(bills$cosponsors)
bills$cosponsors <- gsub('á', 'a', bills$cosponsors)
bills$cosponsors <- gsub('é', 'e', bills$cosponsors)
bills$cosponsors <- gsub('ó', 'o', bills$cosponsors)
bills$cosponsors <- gsub('í', 'i', bills$cosponsors)
bills$cosponsors <- gsub('ñ', 'n', bills$cosponsors)

#### Add First Names to Duplicated Last Names Using Constituency Representated
bills$constituency <- gsub(' of ', '', str_extract(bills$primary_sponsor, ' of [a-z].+$'))
bills$primary_sponsor <- gsub(' of [a-z].+$$', '', bills$primary_sponsor)
bills$primary_sponsor <- gsub('representative |senator |speaker |president ', '', bills$primary_sponsor)

# t_names <- filter(fix_names, fy <= t & ly >= t) %>% mutate(constit = tolower(constit))
# if(nrow(t_names) > 0){
#   for(i in 1:nrow(t_names)){
#     if(nrow(filter(bills, primary_sponsor == t_names[i,]$dn & constituency == t_names[i,]$constit)) > 0){
#       bills[bills$primary_sponsor == t_names[i,]$dn & bills$constituency == t_names[i,]$constit,]$primary_sponsor <- t_names[i,]$replacement  
#     }
#     bills$cosponsors <- gsub(paste0(t_names[i,]$dn, " of ", t_names[i,]$constit), paste0(t_names[i,]$replacement, " of ", t_names[i,]$constit), bills$cosponsors)
#   }
# }

### Manual Fixes to Improve Matching -- Data Errors
if(t_yrs %in% c("2001_2002")){
  bills[bills$primary_sponsor == 'laverriere-bouc',]$primary_sponsor <- 'laverriere-boucher'
  bills$cosponsors <- gsub('laverriere-bouc', 'laverriere-boucher', bills$cosponsors)
}
if(t_yrs == '2003_2004'){
  bills[bills$primary_sponsor == 'michael brennan-xx',]$primary_sponsor <- 'michael brennan'
  bills$cosponsors <- gsub('michael brennan-xx', 'michael brennan', bills$cosponsors)
}

### LES Sponsor Var
bills <- rename(bills, LES_sponsor = primary_sponsor)
table(bills$LES_sponsor)

#### Adjusting for Bills By Request -- Need to do BEFORE splitting
if(any(grepl("request", bills$LES_sponsor))){
  print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
}

###################
###### Merge in S&S Bills
###################
# *** For MAINE: Bills Carryover across regulars and specials? Or at least the LD numbers are all unique
# ---> Merging based on legislative term

if(t_yrs=="2021_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="LD10001"]="LD0001"
  SS_bills$bill_id[SS_bills$bill_id=="LD20002"]="LD0002"
}

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, LD_num = bill_id, SS, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, LD_num = bill_id, SS,Title), bills,
                              by = c("LD_num", "term")) %>% 
  left_join(all_bills %>% select(LD_num,term,title), c("LD_num", "term")) %>%
  filter(! grepl("committee",title, ignore.case=T)) %>%
  arrange(title) 
unique(missing_SS_bills$LD_num) 


# now check to see if there are duplicate joins


duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills, 
            by = c("LD_num", "term")) %>%
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
  bills2 <- bills %>% 
    left_join(SS_term %>% select(-Title) %>% distinct(), by = c("LD_num", "term")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  SS_term2 = SS_term %>% 
    left_join(bills  %>% select(LD_num,term,session),by=c("LD_num","term"))
  if(!identical(c(nrow(bills2),nrow(SS_term2)),orig_row_n )){print("merge failed"); break} else{
    bills = bills2; SS_term = SS_term2; rm(bills2, SS_term2)
  }
}

### Check Missing
table(bills$SS)
SS_in_bills = sum(bills$SS)
SS_in_PVS = nrow(SS_term )

if(SS_in_bills == SS_in_PVS){
  print("all SS merged properly")
} else {
  print(glue("{SS_in_PVS} S&S bills in original dataset, but {SS_in_bills} S&S in our bills dataset"))
  
  # stuff in PVS, not in bills
  print(anti_join(SS_term, bills, by = c("LD_num", "term", "session")))
  
  # PVS bills that are duplicated in bills. not necessarily a problem!
  print(SS_term %>% group_by(LD_num, term) %>%
    mutate(count = n()) %>% filter(count > 1) %>% arrange(LD_num))
  
  # see if they are equal after removing double-counted bills. this is a crude proxy, you should examine more closely
  all.equal(SS_in_bills + nrow(anti_join(SS_term, bills, by = c("LD_num", "term", "session")))+
              nrow(SS_term %>% group_by(LD_num, term) %>%
                     mutate(count = n()) %>% filter(count > 1))/2,
            nrow(SS_term ))
}

rm(all_bills, missing_SS_bills, duplicate_SS_bills)

########################################################
############### Code Commemorative
########################################################

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

########################################################
############### Code Bill History
########################################################
bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{l_num}.csv")
bill_hist <- read.csv(bill_hist_path)

######## Clean Term/Session Variables + Standardize the Bill IDs
bill_hist <- bill_hist %>%
  rename(bill_id = paper_num) %>%
  mutate(term = t_yrs,
         leg_num = l_num,
         session = recode(session, 'First Regular Session' = 'RS1', 'Second Regular Session' = 'RS2', 'First Special Session' = 'SS1', 
                          'Second Special Session' = 'SS2', 'Third Special Session' = 'SS3', 'Fourth Special Session' = 'SS4', 
                          'Fifth Special Session' = 'SS5', 'Sixth Special Session' = 'SS6', 'Seventh Special Session' = 'SS7'),
         LD_num = ifelse(LD_num == '', NA, LD_num)) 

### Order by Order
bill_hist <- bill_hist %>% 
  arrange(term, session, bill_id, action_date, order) %>%
  group_by(term, session, bill_id) %>%
  mutate(order = 1:n()) %>%
  ungroup()

### Adjust Action Variable
bill_hist <- bill_hist %>%
  rename(action = action_detailed) %>%
  mutate(action = tolower(action)) %>%
  filter(bill_id %in% bills$bill_id)

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

#### Unique Maine Rules:
# Per Glossary -- http://legislature.maine.gov/LawMakerWeb/glossary_of_terms.asp
# *** Consent Calender: "In the House, a bill that does not involve a gain or loss of revenue and that has received a 
# --- unanimous "ought to pass" or "ought to pass as amended" committee report is placed on the consent calendar.  If there is
# --- no objection, after two legislative days, the bill is considered passed to be engrossed."
# *** First reading occurs after being reported out

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c('reported out', 'otp', 'ontp', 'public hearing', 'work session', '^voted$',
           '^voted - [a-z\\-]+$', 'committee amendment', 'divided report', 'div. rep.')
# '^report', 'rule 310.3', 'committee amendment'
# Rule 310.3 == Ought Not to Pass --- http://openmepolitics.com/glossary/joint-rule-310-3/
# --> these get reported out, but are basically dead on arrival... 
# --> recoding in loop as ABC = 0; will get coded as ABC = 1 in rare cases where bill is revived from dead
abc_t <- c('reported out', 'accepted report', 'reports read', 'engrossed', 'enacted', 'reconsidered', 'removed from table',
           'amendment adopted', 'first reading', 'second reading',
           'report.+accepted', 'consent calendar', 'read a second time')
# report accepted; report was accepted; report read and accepted
# -- From 121st on, sometimes will say report accepted after committee on bills in second reading's report
pc_t <- c('^engrossed.+prevail', '(insisted|adhered).+engrossed.+prevails', 'enacted.+prevail', 'finally passed.+prevails',
          'passed to be engross', 'passed to be enacted')
## -- After engrossment, it goes to the other chamber... http://legislature.maine.gov/general/path-of-legislation-in-maine-detailed/9285
## BUT... still has to then pass both chambers for enactment... even if both pass the same version. So both have to revote...
## SO: Coding engrossment as passed chamber and keeping enactment/finally passed as double check
## Related: "(insisted|adhered) to engrossed" occurs if out-chamber disagrees, in-chamber can reconsider and adhere or not adhere to initial engrossment (http://legislature.maine.gov/general/path-of-legislation-in-maine-detailed/9285)
## NEED ^engrossed or else will catch motions to table pending engrossment
## -- Finally passed is for Resolves (which have force of law)
law_t <- c('zzzzzzzzz')
# ---> As far as I can tell, no indication in ACTIONS LIST that governor signed a bill
# ---> == Need to use the chapter number variable (if there is a number present) as indicator of law status

### Check Actions
# filter(bill_hist, grepl('governor', tolower(action))) %>% distinct(action) %>% View()
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
  hist_sub <- filter(bill_hist, bill_id == b_id, term == t_yrs, session == s_id)
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t,
                                    add_chamb = "Joint Committee", ignore_chamber_switch = TRUE)
  bill_stages$bill_url <- bills[i,]$bill_url
  ### Check if Law
  if( grepl('chapter [0-9]+', tolower(bills[i,]$chapter_num))){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- bill_stages$law <- 1
  }
  ### Check if Passed 
  # -- Removing: "died between houses" as in some cases this can indicate passed OPPOSING chamber after intro chamber acceped ontp report
  if( grepl('engross|enacted|finally passed|veto', tolower(bills[i,]$status))){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
  }
  ### Check if ABC
  if(grepl('accepted.+report|enactment failed', tolower(bills[i,]$status))){
    bill_stages$action_in_comm <- bill_stages$action_beyond_comm <- 1
  }
  ### Check if Miscoded as ABC when Ought Not to Pass
  #-- NOT including "placed in legislative files \\(dead\\)" as this shows up sometimes when a bill dies other ways
  if(nrow(hist_sub) > 0){ # 1 random bill in 2003..
    if(bill_stages$action_beyond_comm == 1 & bill_stages$passed_chamber == 0 & grepl("joint rule 310|ontp", hist_sub[nrow(hist_sub),]$action)){
      bill_stages$action_beyond_comm <- 0
    }
  }
  #####
  all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
  # print(i)
}
options(warn = 1)

### Check Codings
cat('\n')
all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(term, session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law) ) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2)) %>% 
  as.data.frame() %>% print()
# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% View()
# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 1,]$bill_id) %>% View()


read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-2}_{t-1}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(term, session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% filter(!grepl("SS",session)) %>%  print()

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-4}_{t-3}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(term, session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% filter(!grepl("SS",session)) %>% print()

### MERGE to BILL DATA
bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 

### Add in LD_Nums
all_bill_stages <- left_join(all_bill_stages, select(bills, bill_id, LD_num), by = "bill_id")

### MERGE In COMMEMS
all_bill_stages <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))

### MERGE In S&S
all_bill_stages <- SS_term %>% 
  select(LD_num, term, SS, session) %>%
  left_join(all_bill_stages, ., by = c('LD_num', 'term', 'session')) %>%
  mutate(SS = ifelse(is.na(SS), 0, SS))

### Adjust Commems if SS == 1
table(all_bill_stages$SS, all_bill_stages$commem)
all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
rm(SS_term)

### Save Stage Info 
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
            sponsor_law_rate = sum(law) / n(),
            num_cosponsored_bills = NA) %>%
  ungroup()

#### Adding Legislators Who COSPONSORED A BILL but did not SPONSOR
# unique_cospon <- str_trim(unique(unlist(str_split(bills$cosponsors, '; '))))
# for(nonspon in unique_cospon){
#   if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != ''){
#     chamb <- unique(substring(bills[grepl(nonspon, bills$cosponsors),]$bill_id, 1, 1))
#     if("H" %in% chamb & "S" %in% chamb){
#       print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
#     }else{
#       all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0) 
#     }
#   }
# }

######## Cosponsorship Info 
## ---- FOR ME: This doesn't quite work... hard to parse cross-chamberm, and some rows use 'representatives' or 'senators'
# all_sponsors$num_cosponsored_bills <- NA
# bills$cospon_match <- paste(bills$LES_sponsor, bills$primary_sponsor, bills$cosponsors, sep = '; ')
# for(i in 1:nrow(all_sponsors)){
#   c_sub <- bills[substring(bills$bill_id, 1, 1) %in% ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S'),]
#   sn <- all_sponsors[i,]$LES_sponsor
#   ## NEED TO ACCOUNT FOR overlapping NAMES
#   search_term <- paste0("^", sn, '$| ', sn, '$| ', sn, ' of ')
#   all_sponsors$num_cosponsored_bills[i] <- sum(grepl(search_term, tolower(c_sub$cospon_match)))
#   ### ONLY need to adjust this way if sponsored and cosponsored column are the same
#   all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
# }
# bills <- select(bills, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))

#######################
#### CLEAN NAMES

if(t <= 2001){
  ### No First Names Included Prior to 2003 -- Except when added by me to identify duplicates!
  all_sponsors$first_name <- ifelse(grepl(' ', all_sponsors$LES_sponsor), gsub(' .+', '', all_sponsors$LES_sponsor), '')
  all_sponsors$last_name <- ifelse(all_sponsors$first_name == '', all_sponsors$LES_sponsor, gsub('^[^ ]+ ', '', all_sponsors$LES_sponsor))
}else{
  all_sponsors$first_name <- str_trim(str_extract(all_sponsors$LES_sponsor, '^[^ ]+ '))
  all_sponsors$first_name <- gsub('\\.$', '', all_sponsors$first_name)
  all_sponsors$last_name <- gsub('^[^ ]+ ', '', all_sponsors$LES_sponsor)
}

all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update Last Names for Matching 
if(t_yrs %in% c("1991_1992", "1993_1994")){
  all_sponsors[all_sponsors$LES_sponsor == 'st. onge',]$last_name <-  "saintonge"
}
if(t_yrs == '2001_2002'){
  all_sponsors[all_sponsors$LES_sponsor == 'peavey haskell',c('first_name', 'last_name')] <-  list('', 'peavey haskell')
}
if(t >= 2001 & t <= 2010){
  all_sponsors[all_sponsors$LES_sponsor %in% c('simpson', 'deborah simpson'),]$last_name <-  "pelletiersimpson"
}
if(t_yrs %in% c("2003_2004")){
  all_sponsors[all_sponsors$LES_sponsor == 'peter mills',]$first_name <-  "s."
}
if(t_yrs %in% c("2009_2010", '2011_2012', '2013_2014', '2015_2016')){
  all_sponsors[all_sponsors$LES_sponsor == 'henry beck',]$last_name <-  "murphy beck"
  all_sponsors[all_sponsors$LES_sponsor == 'diane russell',]$last_name <-  "russellnatera"
}
if(t_yrs == '2013_2014'){
  all_sponsors[all_sponsors$LES_sponsor == 'anita peavey haskell',]$last_name <-  "haskell"
}
if(t_yrs %in% c('2013_2014', '2015_2016', '2017_2018')){
  all_sponsors[all_sponsors$LES_sponsor == 'ryan tipping',]$last_name <-  "tippingspitz"
  all_sponsors[all_sponsors$LES_sponsor == 'kimberly monaghan',]$last_name <-  "monaghanderrig"
}
if(t_yrs == '2017_2018'){
  all_sponsors[all_sponsors$LES_sponsor == 'rachel talbot ross',]$last_name <-  "ross"
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
} %>% select(-nickname,-votesmart_id,-ballotpedia) %>%  distinct()



if(t_yrs == "2021_2022") {
  legiscan = bind_rows(legiscan ,
                       legiscan %>% filter(people_id == 21211) %>% mutate(district = "HD-132", role = "Rep"),
                       legiscan %>% filter(people_id == 17216) %>% mutate(district = "HD-044", role = "Rep"),
                       legiscan %>% filter(people_id == 16689) %>% mutate(district = "SD-063", role = "Sen"),
                       legiscan %>% filter(people_id == 16787) %>% mutate(district = "SD-033", role = "Sen"),
                       legiscan %>% filter(people_id == 17272) %>% mutate(district = "HD-129", role = "Rep"),
                       legiscan %>% filter(people_id == 17788) %>% mutate(district = "HD-132", role = "Rep")) %>% 
    mutate(role = ifelse(substr(district,1,1) == "H", "Rep","Sen"))

} 

legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name, role) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = name) %>%
  mutate(match_name_chamber = tolower(paste(match_name,substr(district,1,1),sep="-")))

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
        "theodore kryzak-h" = NA_character_,
        "dwayne prescott-h" = NA_character_,
        "gregory swallow-h" = NA_character_,
        "philip curtis-h" = NA_character_,
        "josanne dolloff-h" = NA_character_,
        "steven foster-h" = NA_character_,
        "aaron frey-h" = NA_character_,
        "stephen moriarty-h" = NA_character_,
        "randall hall-h" = NA_character_,
        "sean paulhus-h" = NA_character_,
        "kevin o'connell-h" = NA_character_,
        "roger reed-h" = NA_character_,
        "david haggan-h" = NA_character_,
        "gina mason-h" = NA_character_,
        "mark blier-h" = NA_character_,
        "dale denno-h" = NA_character_,
        "scott landry-h" = NA_character_
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
        "gina melaragno-h" = NA_character_,
        "shenna bellows-s" = NA_character_,
        "chad grignon-h" = NA_character_,
        "josanne dolloff-h" = NA_character_,
        "sheila lyman-h" = NA_character_,
        "susan austin-h" = NA_character_,
        "jeffery gifford-h" = NA_character_,
        "randall hall-h" = NA_character_,
        "braden sharpe-h" = NA_character_,
        "james thorne-h" = NA_character_,
        "susan bernard-h" = NA_character_,
        "peggy stanley-h" = NA_character_,
        "david haggan-h" = NA_character_,
        "cathy nadeau-h" = NA_character_,
        "james boyle-h" = NA_character_,
        "thomas martin-h" = NA_character_,
        "scott landry-h" = "h. landry-h",
        "kathy downes-h" = NA_character_,
        "scott cyrway-h" = NA_character_,
        "david woodsome-h" = NA_character_,
        "teresa pierce-s" = NA_character_,
        "peter lyford-s" = NA_character_,
        "matthew harrington-s" = NA_character_,
        "nicole grohoski-s" = NA_character_
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
rm(t, terms, klarner_gs, m_sub, fix_names, commem_bills, match_name2, t_names) # 
rm(l_num, leg_num)


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by SPECIAL ELECTION --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
### Rosters: 
# -- 1993-1994: http://lldc.mainelegislature.org/Open/Laws/1993/Laws1993v1_f0009-0021_CivGov.pdf
# -- Environmental Scorecards, 1985+: https://www.maineconservation.org/scores
# -- Legislative Record: https://www.maine.gov/legis/lawlib/lldl/legisrecord117.htm
# -- Historical Info, 1997+: https://web.archive.org/web/20120426231102/http://www.maine.gov/legis/house/history.htm
# -- Tribal Reps: https://legislature.maine.gov/lawlibrary/tribal-representatives-to-the-maine-legislature-1823/9257/
#########################


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1987_1988 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 143 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- DAGGETT (beverly)
# -- GLIDDEN (robert)
### IN HOUSE:
# -- nicholson, earl g.
# -- gurney, christopher
# -- brown, ada k. -- http://lldc.mainelegislature.org/Open/Rpts/kf4943_z99m322_1988.pdf
# -- walker, joseph g.
# -- parent, paul
# -- matthews, kenneth


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1989_1990 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 205 bill(s) without a sponsor
### IN HOUSE:
# -- paul, norman r.
# -- walker, joseph g. -- http://lldc.mainelegislature.org/Open/Sums/114/sum114-LD-2384.pdf
# -- pouliot, roger m.
# -- skoglund, james g.
# -- marston, bertram -- http://lldc.mainelegislature.org/Open/LegRec/114/House/LegRec_1988-12-07_HP_p0001-0023.pdf
# -- sherburne, weston -- http://lldc.mainelegislature.org/Open/LegRec/114/House/LegRec_1988-12-07_HP_p0001-0023.pdf


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1991_1992 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 112 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- VIGUE (marc)
### IN HOUSE:
# -- barth, alvin l. jr.
# -- lapointe, joanne d. -- https://archive.bangordailynews.com/1991/01/21/profiles-of-members-of-the-maine-house-of-representatives/
# -- ricekr, george f.
# -- nash, lawrence f.
# -- poulin, thomas
# -- salisbury, deale b.
# -- bowers, rodney v. -- http://lldc.mainelegislature.org/Open/Sums/115/sum115-LD-0147.pdf
# -- martin, hilda c.
### DROP:
# -- carter, donald v. -- died prior to january 3, 1991 -- http://lldc.mainelegislature.org/Open/LegRec/115/Senate/LegRec_1991-01-03_SP_pS0064-0076.pdf


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1993_1994 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 94 bill(s) without a sponsor
### IN HOUSE:
# -- dutremble, lucien -- ctrl+f lucien -- http://lldc.mainelegislature.org/Open/LegRec/116/House/LegRec_1993-04-15_HP_pH0496-0526.pdf
# -- cameron, robert a.
# -- gamache, albert p.
# -- ricker, george f.
# -- reed, william f.
# -- walker, ellen w.
# -- thompson, calvin a.
# -- pinette, elizabeth


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1996 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 121 bill(s) without a sponsor
### TRIBAL REPRESENTATIVES:
# -- BISULCA (paul joseph, tribal rep) -- https://legislature.maine.gov/lawlibrary/tribal-representatives-to-the-maine-legislature-1823/9257/
# -- MOORE (frederick j. iii) -- https://web.archive.org/web/20110128183243/http://www.maine.gov/legis/house/history/118th/hbiofram.htm
### WON SPECIAL ~ HOUSE:
# -- CARR (ralph) -- https://www.ourcampaigns.com/RaceDetail.html?RaceID=744268
### IN HOUSE:
# -- poirier, theodore m.
# -- truman, peter p.
# -- marvin, jean ginn
# -- gieringer, f. thomas jr.
# -- chizmar, nancy l. 
# -- ricker, george f.
# -- gamache, albert p.
# -- joseph, ruth
# -- rosebush, jon m.
### DROP:
# -- hale, mona walker -- died Feb. 13, 1995, had been in hospital for a few weeks: http://lldc.mainelegislature.org/Open/LegRec/117/House/LegRec_1995-02-14_HP_pH0137-0142.pdf
# -- oliver, james v. -- resigned after winning to take position with Peace Corps

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 123 bill(s) without a sponsor
### TRIBAL REP (No election data?)
# -- BISULCA (paul joseph)
# -- LORING (donna m.) -- https://web.archive.org/web/20110128183243/http://www.maine.gov/legis/house/history/118th/hbiofram.htm
# -- MOORE (frederick j. iii) -- https://web.archive.org/web/20110128183243/http://www.maine.gov/legis/house/history/118th/hbiofram.htm
### IN HOUSE:
# -- frechette, roger d.  
# -- nickerson, roy i.    
# -- sanborn, laura j.    


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 163 bill(s) without a sponsor
### TRIBAL REP (No election data?)
# -- LORING (donna m.) -- https://web.archive.org/web/20110128183243/http://www.maine.gov/legis/house/history/118th/hbiofram.htm
# -- SOCTOMAH (donald g.) -- https://web.archive.org/web/20110128175828/http://www.maine.gov/legis/house/history/119th/hbiofram.htm
### IN HOUSE:
# -- tobin, david l.
# -- jodrey, arlan r.
# -- nutting, robert w.
# -- richardson, earl e.
### DROP:
# -- gamache, albert p. -- died 2/22/99

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 191 bill(s) without a sponsor
### TRIBAL REP (No election data?)
# -- LORING (donna m.) -- https://web.archive.org/web/20110128183243/http://www.maine.gov/legis/house/history/118th/hbiofram.htm
# -- SOCTOMAH (donald g.) -- https://web.archive.org/web/20110128175828/http://www.maine.gov/legis/house/history/119th/hbiofram.htm
### IN HOUSE:
# -- estes, stephen c.
# -- tarazewich, frank j.
# -- jodrey, arlan r.
# -- cote, william r.
# -- chase, peter d.
# -- landry, sally


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 111 bill(s) without a sponsor
### TRIBAL REP (No election data?)
# -- LORING (donna m.) 
# -- MOORE (frederick; name duplicated --> won't print) 
### IN HOUSE: 
### *** Not clear why so many... but searching by primary sponsor suggests list is right: http://legislature.maine.gov/LawMakerWeb/advancedsearch.asp
# -- wheeler, walter a. sr.
# -- lewin, sarah o.
# -- brown, richard b.
# -- stone, oscar c.
# -- campbell, james j. sr.
# -- jacobsen, lawrence e.
# -- tobin, david l.
# -- austin, susan m.
# -- sykes, richard m.
# -- grose, carol a.
# -- sukeforth, gary e.
# -- rector, christopher
# -- mccormick, earle l.
# -- berube, robert a.
# -- mailhot, richard h.
# -- richardson, earl e.
# -- greeley, christian david
# -- churchill, eugene l.
# -- dugay, edward r.
# -- bierman, leonard earl
# -- wotton, raymond
# -- churchill, john w.


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 155 bill(s) without a sponsor
### TRIBAL REP (No election data?)
# -- LORING (donna m.) 
# -- MOORE (frederick; name duplicated --> won't print) 
# -- SOCKALEXIS (michael) -- https://bangordailynews.com/2008/09/25/obituaries/michael-j-sockalexis/
### Disputed Races --> Both presumably were seated after recounts: https://archive.bangordailynews.com/2004/12/31/panel-agrees-on-2-of-3-disputed-house-races/
# -- JACOBSEN (lawrence)
# -- ASH (walter)
### IN HOUSE:
# -- churchill, john w.
# -- mcleod, everett w. sr.
# -- richardson, david e.
# -- dugay, edward r.
# -- richardson, wesley e.
# -- davis, kimberly j.
# -- curtis, philip m.
# -- jodrey, arlan r.
# -- hamper, james m.
# -- marean, donald g.
# -- moulton, bradley s.
# -- ott, david n.


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 170 bill(s) without a sponsor
### TRIBAL REPRESENATIVE
# -- SOCTOMAH (donald)
# -- LORING (donna m.) 
### WON SPECIAL ~ HOUSE:
# -- BRIGGS (sheryl)
### IN HOUSE:
# -- gifford, jeffery a.
# -- richardson, earl e.
# -- rosen, kimberley c.
# -- thibodeau, michael
# -- beaulieu, michael gary
# -- peoples, ann e.


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 112 bill(s) without a sponsor
### TRIBAL REPRESENATIVE
# -- SOCTOMAH (donald)
# -- MITCHELL (theodore 'wayne')
### WON SPECIAL ~ HOUSE:
# -- HARVELL (lance)
### IN HOUSE:
# -- clark, tyler a.
# -- greeley, christian david
# -- richardson, david e.
# -- kent, peter s.
# -- mills, janet t.
# -- kaenrath, bryan t.

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 96 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- TURNER (beth)
### TRIBAL REPRESENATIVE
# -- SOCTOMAH (madonna)
# -- MITCHELL (theodore 'wayne')
### IN HOUSE:
# -- long, ricky d.
# -- foster, karen d.
# -- wagner, richard v.
# -- kaenrath, bryan t.
# -- driscoll, timothy e.
### DROP:
# -- mcleod, everett w. sr. -- died 12.10.2010


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 105 bill(s) without a sponsor
### TRIBAL REPRESENATIVE
# -- BEAR (henry)
# -- SOCTOMAH (madonna)
# -- MITCHELL (theodore 'wayne')
### WON SPECIAL ~ SENATE:
# -- VITELLI (eloise)
### IN HOUSE:
# -- clark, tyler a.
# -- reed, roger e.
# -- winsor, tom j.


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~
# -----> Dropping 124 bill(s) without a sponsor
### TRIBAL REPRESENATIVE
# -- BEAR (henry)
# -- DANA (matthew)
# -- MITCHELL (theodore 'wayne')
#### IN HOUSE:
# -- prescott, dwayne w.
# -- timmons, michael
# -- bickford, bruce a.
# -- herrick, lloyd c.
# -- gilbert, paul e.
# -- grant, gay m.
# -- reed, roger e.
# -- skolfield, thomas h.
#### DROP:
# -- dickerson, elizabeth e. --- resigned 1/12/2015 -- https://bangordailynews.com/2015/01/11/news/midcoast/democratic-state-representative-from-rockland-resigns/


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 90 bill(s) without a sponsor
### TRIBAL REPRESENATIVE
# -- BEAR (henry)
# -- DANA (matthew)
### IN HOUSE
# -- babbidge, christopher w.
# -- prescott, dwayne w.
# -- perkins, michael d.
# -- mcelwee, carol a.


# filter(klarner, grepl("loring", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 124 & sen ==0 & year < 2000) %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)

##########################################################################################################
##########################################################################################################
##### MATCHING MISSING (SPECIALS) TO KLARNER USING OTHER TERMS WHERE SPONSOR IS PRESENT
##########################################################################################################

library(readr)

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

### Error Fixes -- Mismatches
LES[LES$data_name %in% c('moore', 'frederick moore'),]$klarner_id <- NA
LES[LES$data_name %in% c('moore', 'frederick moore'),]$klarner_name <- NA
LES[LES$data_name %in% c('moore', 'frederick moore'),]$sponsor <- 'moore, frederick'# iii

### ****Still missing***** ---> All except carr (ralph) are Tribal Representatives
# still_missing <- unique(LES[is.na(LES$klarner_id),]$sponsor)
# name = still_missing[6]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl('eslick', cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, missing, name_sub, still_missing)

### Fix Missing
name_matches <- data.frame(LES_name = 'loring', k_name = 'loring, donna marie') ## Tribal Rep: but ran for senate in 2004
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

LES$district <- as.character(LES$district)

###### IF MISSING, CHECK ELCTION LOSERS
# filter(LES, is.na(party)) %>% select(sponsor, term, klarner_id, district, party, exper) %>% as.data.frame()
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

### Not In Klarner
fill_missing <- data.frame(LES_name = "carr", new_name = 'carr, ralph', party = 'd', district = "124", exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "bisulca", new_name = 'bisulca, paul joseph', party = NA, district = 'Tribal Representative', exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "moore, frederick", new_name = 'moore, frederick', party = NA, district = 'Tribal Representative', exper = 'none') # iii
fill_missing <- add_row(fill_missing, LES_name = "sockalexis, michael", new_name = 'sockalexis, michael', party = NA, district = 'Tribal Representative', exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "soctomah", new_name = 'soctomah, donald g.', party = NA, district = 'Tribal Representative', exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "soctomah, donald", new_name = 'soctomah, donald g.', party = NA, district = 'Tribal Representative', exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "soctomah, madonna", new_name = 'soctomah, madonna m.', party = NA, district = 'Tribal Representative', exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "mitchell, theodore", new_name = 'mitchell, theodore', party = NA, district = 'Tribal Representative', exper = 'none') #AKA wayne
fill_missing <- add_row(fill_missing, LES_name = "bear, henry", new_name = 'bear, henry j.', party = NA, district = 'Tribal Representative', exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "dana, matthew", new_name = 'dana, matthew', party = NA, district = 'Tribal Representative', exper = 'none') # ii
#fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')
for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

#### *** 2017_2018: If any of below run for reelection, won't be needed once klarner updates ****
# LES[LES$sponsor == "irwin, morgan",]$party <- 'r'

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t, fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()

### Fix Names
LES[LES$sponsor == 'cornellduhoux, alexander',]$sponsor <- 'cornell du houx, alexander'
LES[LES$sponsor %in% c("peaveyhaskell, anita p.", "haskell, anita peavey"),]$sponsor <- 'haskell, anita peavey'
LES[LES$sponsor == 'saintonge, vivina',]$sponsor <- 'saint onge, vivina'
LES[LES$sponsor == 'pelletiersimpson, deborah l.',]$sponsor <- 'simpson, deborah l.'
LES[LES$sponsor == 'murphybeck, henry e. m.',]$sponsor <- 'beck, henry e. m.'
LES[LES$sponsor == 'russellnatera, diane marie',]$sponsor <- 'russell, diane marie'
LES[LES$sponsor == 'tippingspitz, ryan d.',]$sponsor <- 'tipping-spitz, ryan d.'
LES[LES$sponsor == 'monaghanderrig, kimberly j.',]$sponsor <- 'monaghan-derrig, kimberly j.'
# LES[LES$sponsor == 'zzzzz',]$sponsor <- 'zzzzzz'


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
# senate <- filter(hf_data, chamber == "Senate")
# senate$year <- senate$year + 2
# senate$term <- paste0(senate$year + 1, "_", senate$year + 2)
# senate$MajorityMember <- NA
# hf_data <- bind_rows(hf_data, senate) %>% arrange(year, chamber)
# rm(senate)

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

# ************ MAINE: NO DATA for 2009-2010... Odd + Data Starts in 1996 ***********************

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

drop_names <- c('bailey, ronald c.', 'bailey, harry', 'bailey, donna', 'carr, robert b. sr.', 'dutremble, dennis l.','jones, sharon libby',
                'martin, james r.', 'mason, gina m.', 'mcgowan, patrick k.', 'mills, jeffrey n.', 'mitchell, james', 'murphy, thomas',
                'rice, sally', 'rice, chester a.', 'strout, donald a.', 'strout, barbara e.')
LES[LES$sponsor %in% drop_names, c('SM_name', 'SM_party', 'np_score')] <- NA
rm(drop_names)

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches: Anne 'Pinny' Beebe-Center
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

#### FIX MISMATCHES -- Unclear if match or not
LES[LES$sponsor %in% c('dexter, edward l.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1987_1988', '1989_1990', '1991_1992', '1993_1994', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('bailey', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'bailey, harry', SM_name = 'Bailey')
# name_matches <- add_row(name_matches, LES_name = 'bear, henry j.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'begley, charles m.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'bisulca, paul joseph', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'bustin, beverly', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'butterfield, steven j. ii', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'chase, gail m.', SM_name = 'Chase')
# name_matches <- add_row(name_matches, LES_name = 'cianchette, alton e.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'cohen, joan f.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'cornell du houx, alexander', SM_name = 'du Houx, Alexander')
name_matches <- add_row(name_matches, LES_name = 'crockett, patsy garside', SM_name = 'Garside Crockett, Patsy')
# name_matches <- add_row(name_matches, LES_name = 'dana, matthew', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'daughtry, matthea elisabeth', SM_name = 'Larsen Daughtry, Matthea E')
# ---> Thius seems to be him (district matches) but weird 2015 observation = Collapsed with Raymond Wallace... Odd
name_matches <- add_row(name_matches, LES_name = 'dexter, edward l.', SM_name = 'Dexter, Wallace')
name_matches <- add_row(name_matches, LES_name = 'diamond, g. william', SM_name = 'Diamond, William')
# name_matches <- add_row(name_matches, LES_name = 'dostie, stacy t.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'dunn, burchard a.', SM_name = 'Dunn')
# name_matches <- add_row(name_matches, LES_name = 'esty, donald e. jr.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'fitzpatrick, michael j.', SM_name = 'Fitzpatrick')
# name_matches <- add_row(name_matches, LES_name = 'flaherty, sean peter', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'gould, richard a.', SM_name = 'Gould')
# name_matches <- add_row(name_matches, LES_name = 'hanley, dana c.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'haskell, anita peavey', SM_name = 'Peavey Haskell, Anita')
# name_matches <- add_row(name_matches, LES_name = 'hathaway, w. john', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'johnson, birger t.', SM_name = 'Johnson')
name_matches <- add_row(name_matches, LES_name = 'jones, sharon libby', SM_name = 'Libby Jones, Sharon')
name_matches <- add_row(name_matches, LES_name = 'kumiega, walter a. iii', SM_name = 'Kumiega III, Walter A')
# name_matches <- add_row(name_matches, LES_name = 'legg, edward p.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'libby, jack l.', SM_name = 'Libby, J. L.')
# name_matches <- add_row(name_matches, LES_name = 'lord, willis a.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'loring, donna marie', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'luchini, louis joseph', SM_name = 'Luchini Jr, Louis Joseph')
# name_matches <- add_row(name_matches, LES_name = 'magnan, veronica', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'marrache, lisa tessier', SM_name = 'Marraché, Lisa')
# name_matches <- add_row(name_matches, LES_name = 'martin, james r.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'mccormick, dale', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'mitchell, theodore', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'moore, frederick', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'morrison, hugh a.', SM_name = 'Morrison')
name_matches <- add_row(name_matches, LES_name = 'nadeau, guy r.', SM_name = 'Nadeau')
# name_matches <- add_row(name_matches, LES_name = 'odea, john', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'oliver, james v.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'pendexter, joan m.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'pingree, rochelle m.', SM_name = 'Pingree, Chellie')
name_matches <- add_row(name_matches, LES_name = 'pinkham, wright h. sr.', SM_name = 'Pinkham, Wright Sr.')
name_matches <- add_row(name_matches, LES_name = 'pouliot, roger m.', SM_name = 'Pouliot')
name_matches <- add_row(name_matches, LES_name = 'rice, chester a.', SM_name = 'Rice')
name_matches <- add_row(name_matches, LES_name = 'richardson, fred l.', SM_name = 'Richardson')
# name_matches <- add_row(name_matches, LES_name = 'rotondi, dorothy a.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'short, stanley byron jr.', SM_name = 'Short Jr, Stanley Byron')
name_matches <- add_row(name_matches, LES_name = 'simpson, deborah l.', SM_name = 'Pelletier-Simpson')
# name_matches <- add_row(name_matches, LES_name = 'sockalexis, michael', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'soctomah, donald g.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'soctomah, madonna m.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'stevens, albert g.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'stone, richard i.', SM_name = 'Stone')
name_matches <- add_row(name_matches, LES_name = 'strout, donald a.', SM_name = 'Strout')
# name_matches <- add_row(name_matches, LES_name = 'vanwie, david a.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'wagner, joseph a.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'wheeler, walter a. sr.', SM_name = 'Wheeler, Walter Sr.')
# name_matches <- add_row(name_matches, LES_name = 'yackobitz, robert e.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

##########
### More Detailed Matches
###########

LES[LES$sponsor == 'burns, david c.',]$SM_name <-  ideo[ideo$name == 'Burns, David' & ideo$senate2013 %in% 1,]$name
LES[LES$sponsor == 'burns, david c.',]$SM_party <- ideo[ideo$name == 'Burns, David' & ideo$senate2013 %in% 1,]$party
LES[LES$sponsor == 'burns, david c.',]$np_score <- ideo[ideo$name == 'Burns, David' & ideo$senate2013 %in% 1,]$np_score
LES[LES$sponsor == 'burns, david r.',]$SM_name <-  ideo[ideo$name == 'Burns, David' & is.na(ideo$senate2013),]$name
LES[LES$sponsor == 'burns, david r.',]$SM_party <- ideo[ideo$name == 'Burns, David' & is.na(ideo$senate2013),]$party
LES[LES$sponsor == 'burns, david r.',]$np_score <- ideo[ideo$name == 'Burns, David' & is.na(ideo$senate2013),]$np_score


########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########
# Maine, switchers: https://archive.bangordailynews.com/1995/10/05/party-switching-shreds-democratic-power/

### No Data to Code Switch:
# *** John Michael --- D in 1993-1994; Indep in 2000 -- Don't have D SM record -- https://en.wikipedia.org/wiki/John_Michael_(politician)
# *** Michael Willettee -- D in House, R in Senate --- Don't have R NP Score
# *** Peggy Pendleton -- R in House 1988 - 1994; D in Senate/House, 1997-2010; no R np score -- https://www.deseret.com/1996/10/26/19273633/this-year-s-election-day-in-maine-is-a-family-affair
# *** Clyde Hichborn -- R to D in 1991 -- Don't have R SM record
# *** Hugh Morrison -- R to D in 1995 -- Don't have R SM record
# *** Stanley Moody -- R to D in 2005 -- Odd, either klarner innacuraccy or no R record in SM data

# *** James Campbell Sr --- D to Independent
LES[LES$sponsor == 'campbell, james j. sr.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Campbell, James Sr.' & ideo$party == 'R',]$name
LES[LES$sponsor == 'campbell, james j. sr.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Campbell, James Sr.' & ideo$party == 'R',]$party
LES[LES$sponsor == 'campbell, james j. sr.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Campbell, James Sr.' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'campbell, james j. sr.' & LES$party == 'nonmaj',]$SM_name <-  ideo[ideo$name == 'Campbell Sr, James J' & ideo$party == 'X',]$name
LES[LES$sponsor == 'campbell, james j. sr.' & LES$party == 'nonmaj',]$SM_party <- ideo[ideo$name == 'Campbell Sr, James J' & ideo$party == 'X',]$party
LES[LES$sponsor == 'campbell, james j. sr.' & LES$party == 'nonmaj',]$np_score <- ideo[ideo$name == 'Campbell Sr, James J' & ideo$party == 'X',]$np_score

# *** John McDonough --- D to R --- between 2002 and 2007 break
LES[LES$sponsor == 'mcdonough, john f.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'McDonough, John' & ideo$party == 'R',]$name
LES[LES$sponsor == 'mcdonough, john f.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'McDonough, John' & ideo$party == 'R',]$party
LES[LES$sponsor == 'mcdonough, john f.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'McDonough, John' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'mcdonough, john f.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'McDonough, John' & ideo$party == 'D',]$name
LES[LES$sponsor == 'mcdonough, john f.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'McDonough, John' & ideo$party == 'D',]$party
LES[LES$sponsor == 'mcdonough, john f.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'McDonough, John' & ideo$party == 'D',]$np_score

### THomas Saviello --- D to I to R
LES[LES$sponsor == 'saviello, thomas' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Saviello, Thomas' & ideo$party == 'D',]$name
LES[LES$sponsor == 'saviello, thomas' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Saviello, Thomas' & ideo$party == 'D',]$party
LES[LES$sponsor == 'saviello, thomas' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Saviello, Thomas' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'saviello, thomas' & LES$party == 'nonmaj',]$SM_name <-  ideo[ideo$name == 'Saviello, Thomas' & ideo$party == 'X',]$name
LES[LES$sponsor == 'saviello, thomas' & LES$party == 'nonmaj',]$SM_party <- ideo[ideo$name == 'Saviello, Thomas' & ideo$party == 'X',]$party
LES[LES$sponsor == 'saviello, thomas' & LES$party == 'nonmaj',]$np_score <- ideo[ideo$name == 'Saviello, Thomas' & ideo$party == 'X',]$np_score
LES[LES$sponsor == 'saviello, thomas' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Saviello, Thomas' & ideo$party == 'R',]$name
LES[LES$sponsor == 'saviello, thomas' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Saviello, Thomas' & ideo$party == 'R',]$party
LES[LES$sponsor == 'saviello, thomas' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Saviello, Thomas' & ideo$party == 'R',]$np_score

# *** Edgar Wheeler --- D to R in 1996/7 
LES[LES$sponsor == 'wheeler, edgar' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Wheeler, Edgar' & ideo$party == 'R',]$name
LES[LES$sponsor == 'wheeler, edgar' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Wheeler, Edgar' & ideo$party == 'R',]$party
LES[LES$sponsor == 'wheeler, edgar' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Wheeler, Edgar' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'wheeler, edgar' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Wheeler' & ideo$party == 'D',]$name
LES[LES$sponsor == 'wheeler, edgar' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Wheeler' & ideo$party == 'D',]$party
LES[LES$sponsor == 'wheeler, edgar' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Wheeler' & ideo$party == 'D',]$np_score

# *** June Meres --- D to R in 1996/7 
LES[LES$sponsor == 'meres, june c.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Meres, June' & ideo$party == 'R',]$name
LES[LES$sponsor == 'meres, june c.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Meres, June' & ideo$party == 'R',]$party
LES[LES$sponsor == 'meres, june c.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Meres, June' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'meres, june c.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Meres' & ideo$party == 'D',]$name
LES[LES$sponsor == 'meres, june c.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Meres' & ideo$party == 'D',]$party
LES[LES$sponsor == 'meres, june c.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Meres' & ideo$party == 'D',]$np_score

# *** Thomas Tyler --- D in 1995-1996; R in 2013
LES[LES$sponsor == 'tyler, thomas m.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Tyler, Thomas' & ideo$party == 'R',]$name
LES[LES$sponsor == 'tyler, thomas m.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Tyler, Thomas' & ideo$party == 'R',]$party
LES[LES$sponsor == 'tyler, thomas m.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Tyler, Thomas' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'tyler, thomas m.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Tyler' & ideo$party == 'D',]$name
LES[LES$sponsor == 'tyler, thomas m.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Tyler' & ideo$party == 'D',]$party
LES[LES$sponsor == 'tyler, thomas m.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Tyler' & ideo$party == 'D',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################

##### Drop Nicknames
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]$', sponsor))
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
### For Maine: https://web.archive.org/web/20170609150307/http://legislature.maine.gov/house/history/makeup.htm
# table(LES$term)

LES$in_majority <- 0

### House -- 1987 - 2020
# ** 1995-1996: 75 D, 75 R, 1 I -- Independent (plus others) must have caucused with Dems -- Dan A. Gwadosky (D) won speaker vote with 81 votes
# ---> See House Journal (page H-6) -- https://www.maine.gov/legis/lawlib/lldl/legisrecord117.htm
LES[as.numeric(substring(LES$term,1,4)) %in% c(1987:2010, 2013:2020) & LES$chamber == 'House' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2011:2012) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1987 - 2020
# ** 2001-2002: 17 R, 17 D, 1 I --> "power-sharing agreement which continued throughout the 2-year session" (see note on web.archive link above)
# ------> Per Tie Procedure page (NCSL), was a resignation agreement, whereby each party held power for a pre-determined period of time
# ------> Coding both as 'in_majority'
LES[as.numeric(substring(LES$term,1,4)) %in% c(1987:1994, 1997:2000, 2003:2010, 2013:2014, 2019:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:1996, 2011:2012, 2015:2018) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1
LES[LES$chamber == "Senate" & LES$term == "2001_2002",]$in_majority <- 1

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
    Ideology_Missing = round(sum(is.na(np_score))/n(), 2))  %>%
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
  scale_color_manual(values=c("dodgerblue2",  'gray50', "red2", 'gray50'))

##### CHECK OUTLIERS ---- No switchers remaining through 2018!
# filter(LES, party == 'd' & np_score > 0) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & np_score < 0) %>% select(sponsor, term, chamber, np_score, party, SM_party)


### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

