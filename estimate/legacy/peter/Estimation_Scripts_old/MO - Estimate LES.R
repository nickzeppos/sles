
#####################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** MISSOURI *** BY SESSION
#####################################


##################################
## SPECIAL SESSIONS:
## ---- Folded into main file; Bill numbers restart 
## MEMBER LISTS:
## ---- Senate: https://www.senate.mo.gov/senate-member-archive/
## ---- House/Senate: https://www.sos.mo.gov/archives/history/historicallistings/molega.asp
## PROCESS:
## ---- Bills do not carry over... See HJR0032 (1995), HJR0062 (1996) in 88th session -- Reintroduced...
## ---- HOUSE: https://house.mo.gov/content.aspx?info=/info/howbill.htm
## Sponsorship/Authorship
## -- Permits multiple primary but not shown until later years (2001+?)
## -- Cosponsorship records are disjointed (multiple primary too) --- often just lists the first + et al
###################################
## NOTE:
# (0) DATA is split between House and Senate files
# (1) Try to adjust for bills reintroduced in 2nd session? See carry over process note below.. 
# ----> On the one hand, not adjusting can inflate scores... on the other hand, anyone can do it, and if you're smart, you'll stay persistent..
# ----> See: filter(bills, duplicated(summary) & summary != '' & !grepl("FISCAL NOTE|will become effective|effective date|bill is effective|penalty provisions|emergency clause", summary)) %>% View()
# ----> Detection of this is hard: e.g., see 1995 HB0183, 1995 HB0243, 1996 HB0971 --> Same summary, same purpose, two by same sponsor, one not..
# (2) If a bill is reported DO NOT PASS, needs 82 members in House to vote to take it up... So... AIC but not ABC?
# (3) SB0383 -- Only action = 'Bill Withdrawn' --> Drop?
##################################

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

this_state <- 'MO'
min_year <- 1995
max_year <- 2018
keep_types <- c("HB", "SB")
spec_elec_codes <- c('s', 'gs')
sen_term_length <- 4 # Staggered? YES

#### Output Directory
#dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Data Directory
data_files <- list.files(glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/House"), full.names = TRUE)
house_files <- data_files[grepl('Bill_Details', data_files)]

### *********************************
### For now: DROP 100th = 2019, 101st = 2021
house_files <- house_files[!grepl("100th", house_files)]
house_files <- house_files[!grepl("101st", house_files)]
### ************************************  

terms <- seq(min_year, max_year, 2)
term_num <- sort(gsub('.+Details_|_House.csv', '', house_files))
rm(data_files, house_files)

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("~/Dropbox/Data/State Legislative Data/Commem Bills/{this_state}_Commem_Bills.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- read.csv("~/Dropbox/Data/State Legislative Data/Significant Bills/SS_Data/SS_Bills.csv")
SS_bills <- SS_bills %>% 
  filter(state == this_state) %>%
  rename(bill_id = bill_num) %>%
  mutate(term = ifelse(year %% 2 == 1, paste0(year, "_", year + 1), paste0(year - 1, "_", year)),
         bill_id = toupper(bill_id),
         SS = 1) %>%
  select(state, term, year, bill_id, everything())

#### Check SS BIll Types ---> Correct to ensure standardized with Data Format
# table(SS_bills$bill_type)

### Load Klarner Data 
load("~/Dropbox/Data/US Election Data/state_leg/klarner_data/196slers1967to2016_20180908.RData")
klarner <- table; rm(table)
klarner$sab <- toupper(klarner$sab)
klarner <- filter(klarner, year > min_year - 5 & sab == this_state)

#### FIX/DROP PROBLEMATIC KLARNER OBSERVATIONS (Doubles, Multi-race candidates, etc.)
# -- Patricia Pike falsely labels as Randy Pike 2014+ (was selected as candidate following death of husband (who won primary))
klarner[klarner$cand == "pike, randy" & klarner$year >= 2014 & klarner$etype == 'g',]$cand <- 'pike, patricia'
# *** TOMMIE PIERSON --> NEEDS TO ACCOUNT FOR JR/SR -- District 66, switch occurs at 2017 term
# --- Edited at bottom during standardization
# *** RORY ROWLAND --> District 29, 2016 General - Falsely coded as Robert Rowland (but see cando, matches rory)
klarner[klarner$cand == "rowland, robert (bob)" & klarner$year == 2016,]$cand <- 'rowland, rory'


###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- 1

for(t in 1:length(terms)){
  
  ### Formulate 2-year terms -- Cover both regular and special sessions
  t_yrs <- as.character(glue('{terms[t]}_{terms[t]+1}'))
  t_num <- term_num[t]
  
  ### Skip Previously Estimated
  if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
    print(glue('. \n ~~~~ SKIPPING {t_yrs} TERM ---> LES scores already estimated! \n.'))
    next
  }
  
  ###### SESSION IN PROGRESS
  print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))
  
  #### HOUSE
  h_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/House/{this_state}_Bill_Details_{t_num}_House.csv")
  h_bills <- read.csv(h_path)   

  #### SENATE
  s_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/Senate/{this_state}_Bill_Details_{t_num}_Senate.csv")
  s_bills <- read.csv(s_path)   
  
  #### COMBINE
  bills <- bind_rows(h_bills, s_bills)
  rm(h_path, h_bills, s_path, s_bills)
  
  ##### Clean Term/Session Variables
  bills$term <- t_yrs
  bills$ga_num <- t_num
  bills$session_type <- recode(bills$session_type, '1st RS' = 'RS', '2nd RS' = 'RS', 'ES' = 'SS1', '1st ES' = 'SS1', '2nd ES' = 'SS2', '3rd ES' = 'SS3')
  bills$session <- paste0(bills$session_year, '-', bills$session_type)
  bills <- select(bills, -c(session_type, session_year)) 
  
  ### Check for duplicates
  if(t_yrs == "1995_1996" | t_yrs == "2013_2014" | t_yrs == "2015_2016" | t_yrs == "2017_2018"){
    bills <- distinct(bills)
  }
  if(nrow(bills) != nrow(distinct(bills))){
    cat(" \n ~~~> DUPLICATE BILLS \n\n .")
    break
  }

  ######## Standardize the Bill IDs
  bills <- rename(bills, bill_id = bill_number)
  bills <- arrange(bills, session, bill_id)
  
  ############### Drop Resolutions, Messages, Communications, Reports
  bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
  # table(bills$bill_type)
  bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)
  
  ############### Standardize Sponsors + Extracting info
  # ** District only recorded for House Members
  # ** House data has full name (last, first +) /// Senate is just last
  # ** NOT doing cosposnored bills... for later terms, will often just refer to as {cosponsor 1, et al} -- too much parsing
  bills$primary_sponsor <- gsub(';.+', '', str_trim(bills$primary_sponsor))
  bills$sponsor_dist <- gsub('\\(|\\)', '',str_extract(bills$primary_sponsor, '\\([0-9]+\\)$'))
  bills$primary_sponsor <- str_trim(gsub('\\([0-9]+\\)$', '', bills$primary_sponsor))
  
  #### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
  bills$primary_sponsor <- tolower(bills$primary_sponsor)
  bills$primary_sponsor <- gsub('á|ã¡', 'a', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('é|ã©', 'e', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('ó', 'o', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('í', 'i', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('ñ|ã±', 'n', bills$primary_sponsor)
  
  ### Remove Titles (e.g., Dr.)
  bills$primary_sponsor <- gsub(', dr\\. ', ', ', bills$primary_sponsor)
  
  #### Adjusting for Bills By Request -- Need to do BEFORE splitting
  if(any(grepl('\\(br\\)|by request| br$', bills$primary_sponsor))){
    print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
    #print(glue("-----> KEEPING {nrow(filter(bills, grepl('(br)|by request', primary_sponsor)))} bill(s) introduced BY REQUEST"))
  }
  
  #### DROP Committee Bills
  if(any(grepl('committee', bills$primary_sponsor))){
    print(glue("-----> DROPPING {nrow(filter(bills, grepl('committee', primary_sponsor)))} bill(s) introduced BY COMMITTEE"))
    break
    # bills <- filter(bills, !grepl('committee', LES_sponsor))
  }
  
  ### Manual Adjustments --- Name Variations --> 1 Format
  if(t_yrs == "1997_1998"){
    bills$primary_sponsor <- ifelse(str_trim(bills$primary_sponsor) == "demarce, karl", "demarce, karl a.", bills$primary_sponsor)
    bills$primary_sponsor <- ifelse(str_trim(bills$primary_sponsor) == "stokan, lana", "stokan, lana ladd", bills$primary_sponsor)
    bills$primary_sponsor <- ifelse(str_trim(bills$primary_sponsor) == "townley, merrill m", "townley, merrill m.", bills$primary_sponsor)
    bills$primary_sponsor <- ifelse(str_trim(bills$primary_sponsor) == "graham, jim", "graham, james", bills$primary_sponsor)
  } else if(t_yrs == "2003_2004"){
    bills$primary_sponsor <- ifelse(str_trim(bills$primary_sponsor) == "roark, brad", "roark, bradley g.", bills$primary_sponsor)
  }else if(t_yrs == "2007_2008"){
    bills$primary_sponsor <- ifelse(str_trim(bills$primary_sponsor) == "scharnhorst", "scharnhorst, dwight", bills$primary_sponsor)
  }else if(t_yrs == "2009_2010"){
    ## Linda Fischer --> Mid-term name change
    bills$primary_sponsor <- ifelse(str_trim(bills$primary_sponsor) == "fischer, linda", "black, linda", bills$primary_sponsor)
    bills$primary_sponsor <- ifelse(str_trim(bills$primary_sponsor) == "stacey newman", "newman, stacey", bills$primary_sponsor)
  }
  
  #### LES Sponsor Variable
  bills$LES_sponsor <- str_trim(bills$primary_sponsor)
  # table(bills$LES_sponsor)
  
  #### Extract Nickname
  bills$nickname <- str_trim(gsub('\\(|\\)|"', '', str_extract(bills$LES_sponsor, ' \\([a-z]+\\)$| "[a-z]+"$')))
  bills$LES_sponsor <- str_trim(gsub('  +', ' ', gsub(' \\([a-z]+\\)$| "[a-z]+"$', '', bills$LES_sponsor)))
  
  ###### CHeck Missing Sponsors
  # filter(bills, LES_sponsor == "") %>% View()
  if(nrow(filter(bills, LES_sponsor == '')) > 0){
    print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
    bills <- filter(bills, !(LES_sponsor == ''))     
  }
  
  
  ###################
  ###### Merge in S&S Bills
  ###################
  # *** For MISSOURI: Bills do NOT carry-over but numbers increment up across sessions; BUT special session bills restart at 1 for each special
  # ---> Merge on ADJUSTED session variable to catch regular session matches + special sessions where necessary (also assuming SS1 if > 1 special)
  SS_term <- SS_bills %>% filter(term == t_yrs) %>% mutate(H_max = NA, S_max = NA, s_spec = NA, any_specials = any(grepl(paste0("-SS"), bills$session)))
  
  ## Adjusting Max Specials by yr
  for(yr in as.numeric(str_split(t_yrs, "_")[[1]]) ){
    if(any(grepl(paste0(yr, "-SS"), bills$session))){
      H_max <- filter(bills, grepl(yr, session) & grepl("^H", bill_id) & grepl("SS", session)) %>% mutate(num = as.numeric(gsub('[A-Z]+', '', bill_id))) %>% pull(num)
      H_max <- ifelse(length(H_max) >= 1, max(H_max), NA)
      S_max <- filter(bills, grepl(yr, session) & grepl("^S", bill_id) & grepl("SS", session)) %>% mutate(num = as.numeric(gsub('[A-Z]+', '', bill_id))) %>% pull(num)
      S_max <- ifelse(length(S_max) >= 1, max(S_max), NA)
      which_spec <- which(table(bills[grepl(yr, bills$session) & grepl("SS", bills$session),]$session) == max(table(bills[grepl(yr, bills$session) & grepl("SS", bills$session),]$session)))[1]
      which_spec <- names(table(bills[grepl(yr, bills$session) & grepl("SS", bills$session),]$session)[which_spec])
      SS_term[SS_term$year == yr,]$H_max <- H_max
      SS_term[SS_term$year == yr,]$S_max <- S_max
      SS_term[SS_term$year == yr,]$s_spec <- which_spec
      rm(H_max, S_max, which_spec)
    }
  }; rm(yr)
  
  SS_term <- SS_term %>%
    mutate(special = ifelse((grepl("^H", bill_id) & is.na(H_max)) | (grepl("^S", bill_id) & is.na(S_max)), 0, special),
           special = ifelse(!any_specials, 0, special),
           special = ifelse(grepl("^H", bill_id) & !is.na(H_max) & special == 1 & num_only > H_max, 0, special),
           special = ifelse(grepl("^S", bill_id) & !is.na(S_max) & special == 1 & num_only > S_max, 0, special),
           session_adj = ifelse(special == 0, paste0(t_yrs, '-RS'), ifelse(is.na(special_num), s_spec, paste0(year, '-SS', special_num)))) %>%
    distinct(term, session_adj, bill_id, SS)
  
  ### Merge
  bills <- bills %>%
    mutate(session_adj = ifelse(grepl("RS", session), paste0(term, "-RS"), session)) %>%
    left_join(SS_term, by = c("bill_id", "term", "session_adj")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  
  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id", "term", "session_adj"))
  
  ########################################################
  ############### Code Commemorative
  ########################################################
  
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session'))
  # table(bills$commem)
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  ########################################################
  ############### Code Bill History
  ########################################################
  
  #### HOUSE
  h_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/House/{this_state}_Bill_Histories_{t_num}_House.csv")
  h_hist <- read.csv(h_hist_path)
  
  ## Code Chamber --- Journal page works better than (H)/(S), so using first -- Catches Reported to Senate, e.g.
  h_hist$chamber <- gsub(' | [0-9].+| [0-9]+|', '', h_hist$journal_page)
  h_hist$chamber <- ifelse(h_hist$chamber == '', gsub('\\(|\\)', '', str_extract(h_hist$action, "\\(H\\)$|\\(S\\)$")), h_hist$chamber)
  h_hist <- h_hist %>% group_by(session, bill_number) %>% fill(chamber) %>% ungroup()
  
  ##### SENATE
  s_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/Senate/{this_state}_Bill_Histories_{t_num}_Senate.csv")
  s_hist <- read.csv(s_hist_path)
  
  ## Code Chamber --- Using actions that start with H or S after cleaning off some common action phrases
  ## Later years have journal pages so can use those too
  s_hist$chamber <- toupper(str_extract(gsub('^referred |^reported from |^reported do pass |^hearing conducted |^hearing cancelled |^reported duly enrolled |^voted do pass |^voted do not pass |^reported truly perfected ', '', tolower(s_hist$action)), "^h |^s "))
  s_hist$chamber <- ifelse(is.na(s_hist$chamber), str_extract(gsub(' adopted$| Adopted$| defeated$| Defeated$', '', s_hist$action), ' H$| S$'), s_hist$chamber)
  s_hist$chamber <- str_trim(s_hist$chamber)
  s_hist$chamber <- ifelse(is.na(s_hist$chamber) & grepl('Signed by Senate President', s_hist$action), "S", s_hist$chamber)
  s_hist$chamber <- ifelse(is.na(s_hist$chamber) & grepl('Signed by House Speaker', s_hist$action), "H", s_hist$chamber)
  s_hist$chamber <- ifelse(grepl("Governor", s_hist$action), "G", s_hist$chamber)
  s_hist$chamber <- ifelse(is.na(s_hist$chamber) & s_hist$journal_page != '', gsub('[0-9]+|[0-9].+', '', s_hist$journal_page), s_hist$chamber)
  s_hist <- s_hist %>% group_by(session, bill_number) %>% fill(chamber) %>% ungroup()
  
  #### Combine House and Senate
  bill_hist <- bind_rows(h_hist, s_hist)
  rm(h_hist_path, h_hist, s_hist_path, s_hist)
  
  ### Clean Term/Session Variables
  bill_hist$term <- t_yrs
  bill_hist$ga_num <- t_num
  bill_hist$session_type <- recode(bill_hist$session_type, '1st RS' = 'RS', '2nd RS' = 'RS', 'ES' = 'SS1', '1st ES' = 'SS1', '2nd ES' = 'SS2', '3rd ES' = 'SS3')
  bill_hist$session <- paste0(bill_hist$session_year, '-', bill_hist$session_type)
  bill_hist <- select(bill_hist, -c(session_type, session_year)) 
  
  ######## Standardize BillHist Bill IDs
  bill_hist <- rename(bill_hist, bill_id = bill_number)
  bill_hist <- arrange(bill_hist, session, bill_id, order)
  
  ### Fill in Missing Chambers (Prefiled bills and bills with 1 action)
  bill_hist$chamber <- ifelse(is.na(bill_hist$chamber), substring(bill_hist$bill_id, 1, 1), bill_hist$chamber)
  
  ### Standardize Chamber Variable
  bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate", 'G' = "Governor")
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  # ***** FORMATS DIFFER SOMEWHAT ACROSS HOUSE AND SENATE BILLS ************
  # -- Second reading occurs prior to committee referral
  
  ### House Terms
  # -- If Reported Do Not Pass -->  "(Such a bill will not be taken up by the House unless 82 members vote to take it up.)"
  # ----> Suggests this is not ABC
  H_aic_t <- c('hearing room', 'committee room', '^date:.+time:', 'reported do pass', "reported do not pass", 
               'voted do pass', 'voted do not pass', 'public hearing', 'executive session')
  H_abc_t <- c('reported do pass', 'third read', '^perfected', 'ayes:', 'noes:', 'placed on.+calendar', 'taken up for')
  # Perfected similar to engrossment in many chambers
  H_pc_t <- c('third read and passed', 'reported to the senate', '^signed by', 'truly agreed to and finally passed') 
  # signed by/truly agreed to = check
  # 'delivered to secretary of state' != LAW ALWAYS; see, e.g, https://house.mo.gov/billtracking/bills01/action01/aHB909.htm
  H_law_t <- c('approved by governor', 'approved by the governor', 'no action taken by governor', 'vetoed in part by governor',
               'passed over veto', 'approved.+acting governor')
  
  ### Senate Terms
  S_aic_t <- c('hearing conducted', 'hearing scheduled', 'voted do pass', 'voted do not pass', 'reported do', 'reported from')
  # Could add in hearing cancelled as well
  S_abc_t <- c('reported from', 'reported do pass', 'third read', 'perfected', 'calendar', 'taken up',
               'bill taken from comm')
  S_pc_t <- c('third read and passed', 'truly agreed to and finally passed', 'duly enrolled', 'signed by')
  S_law_t <- c('signed by governor', 'signed by the governor', 'no action taken by governor', 'vetoed in part by governor',
               'legislature voted to override', 'signed by acting governor')
  # ---> Senate doesn't record delivery to secretary of state -- except for some resolutions
  
  # filter(bill_hist, substring(bill_id, 1,1) == "S") %>%
  #   filter(grepl('governor', tolower(action) )) %>% #filter(!grepl('reported from|reported do pass', tolower(action))) %>% distinct(action) %>% as.data.frame()
  # bill_hist[bill_hist$action == 'Vetoed in Part by Governor (G)',]$bill_id
  # View(bill_hist[bill_hist$bill_id == "HB0214",])
 
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
    hist_sub <- filter(bill_hist, bill_id == b_id & session == s_id)
    ## Use differnt terms for House and Senate files
    if(substring(b_id, 1, 1) == "H"){
      bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, H_aic_t, H_abc_t, H_pc_t, H_law_t)  
    }else{
      bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, S_aic_t, S_abc_t, S_pc_t, S_law_t)        
    }
    ### Check Veto Overrides (Not always caught by terms in h_law/s_law)
    if(nrow(hist_sub) > 0){ # Can get rid of this if after 2003 rescrape
      if(bill_stages$law == 0 & any(grepl("house votes to override veto|h adopt.+ override|motion to override.+ h adopted", tolower(hist_sub$action))) & any(grepl("senate votes to override veto|s adopt.+ override|motion to override.+ s adopted", tolower(hist_sub$action)))){
        bill_stages$passed_chamber <- bill_stages$law <- 1
      }
    }
    bill_stages$bill_url <- bills[i,]$bill_url
    all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
    # print(i)
  }
  options(warn = 1)
  
  ### Check Codings
  cat('\n')
  all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
    summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% as.data.frame() %>% print()
  # filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% filter(!grepl('^by resolution|^first read|^prefiled', tolower(action))) %>% View()
  # filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 1,]$bill_id) %>% View()
  
  ### MERGE to BILL DATA
  bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 
  
  ### MERGE In COMMEMS
  all_bill_stages <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))
  
  ### MERGE In S&S
  all_bill_stages <- mutate(all_bill_stages, session_adj = ifelse(grepl("RS", session), paste0(term, '-RS'), session))
  all_bill_stages <- SS_term %>% 
    select(bill_id, term, session_adj, SS) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term', "session_adj")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS)) %>% 
    select(-session_adj)
  
  ### Adjust Commems if SS == 1
  # table(all_bill_stages$SS, all_bill_stages$commem)
  all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
  rm(SS_term)
  
  ### Save Stage Info **** MERGE WITH SS **********
  if(!dir.exists(glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}'))){
    dir.create(glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}'))
  }
  write.csv(all_bill_stages, glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv'), row.names = FALSE )
  
  rm(hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist, H_aic_t, H_abc_t, H_pc_t, H_law_t, S_aic_t, S_abc_t, S_pc_t, S_law_t)
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
    ungroup()
  
  ######## *** MO: INCOMPLETE SPONSORSHIP INFO ***
  # all_sponsors$num_cosponsored_bills <- NA
  # for(i in 1:nrow(all_sponsors)){
  #   c_sub <- filter(bills, substring(bill_id, 1, 1) == all_sponsors[i,]$chamber)
  #   all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$coauthors)))
  #   ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  #   # all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  # }
  # all_sponsors <- select(all_sponsors, -cospon_match)
  # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))
  
  ### Parsing Names --- First Name Adjustments removes first initials (if go by another name) + middle names
  all_sponsors$last_name <- gsub(',.+', '', all_sponsors$LES_sponsor)
  all_sponsors$first_name <- ifelse(grepl(',', all_sponsors$LES_sponsor), gsub('.+, ', '', all_sponsors$LES_sponsor), '')
  all_sponsors$first_name <- gsub('^[a-z]\\. | [a-z]\\.$', '', all_sponsors$first_name)
  all_sponsors$first_name <- gsub(' .+', '', all_sponsors$first_name)
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  
  #### Update Last Names for Matching 
  if(t_yrs == "1995_1996"){
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "murray, connie", "wible", all_sponsors$last_name)
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "edwards-pavia, marilyn", "edwards", all_sponsors$last_name)
  }
  if(t_yrs == '1997_1998'){
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "edwards-pavia, marilyn", "edwards", all_sponsors$last_name)
  }
  if(t_yrs %in% c("1997_1998", "1999_2000", "2001_2002")){
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "merideth iii, denny j.", "merideth", all_sponsors$last_name)
  }
  if(t_yrs == "2001_2002"){
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "baker, lana ladd", "stokan", all_sponsors$last_name)    
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "brooks, sharon sanders", "sandersbrooks", all_sponsors$last_name)   
  }
  if(t_yrs %in% c('2003_2004') ){
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "brooks, sharon sanders", "sandersbrooks", all_sponsors$last_name)   
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "jones, robin wright", "wrightjones", all_sponsors$last_name)   
  }
  if(t_yrs %in% c("2001_2002", '2003_2004', "2005_2006", "2007_2008") ){
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "st. onge, neal c.", "saintonge", all_sponsors$last_name)    
  }
  if(t_yrs %in% c("2011_2012") ){
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "hughes iv, leonard", "hughes", all_sponsors$last_name)    
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "keeney taylor, shelley", "keeney", all_sponsors$last_name)    
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "walton gray, rochelle", "gray", all_sponsors$last_name)    
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "mccann beatty, gail", "beatty", all_sponsors$last_name)    
  }
  if(t_yrs %in% c("2013_2014", "2015_2016") ){
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "walton gray, rochelle", "gray", all_sponsors$last_name)    
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "mccann beatty, gail", "beatty", all_sponsors$last_name)    
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "pierson sr., tommie", "pierson", all_sponsors$last_name)    
  }
  if(t_yrs %in% c("2017_2018") ){
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "mccann beatty, gail", "beatty", all_sponsors$last_name)    
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "franks jr., bruce", "franks", all_sponsors$last_name)    
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "baringer, donna", "mcbaringer", all_sponsors$last_name)    
    # all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "sssss", "beatty", all_sponsors$last_name)    
    # ** Klarner error -- Jr wins Seat; Sr previously -- So IDs will be same..
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "pierson jr., tommie", "pierson", all_sponsors$last_name)    
  }

  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  ### If Term is 2008-2009, e.g., including 2007, 2008, and Specials for 2009 (when present)
  elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% elec_year:(elec_year + 1) | (year == elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% (elec_year - sen_term_length + 2):(elec_year + 1) | (year == elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)
  
  ####################################################
  ############## Match Sponsors Names to Klarner Data, Forgoing Function because mostly only have last name
  ####################################################
  
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), '')))
  klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]+')))

  ### Fix Match Names
  if(t_yrs == "1995_1996"){
    klarner_sub[klarner_sub$cand == 'marshall, t. w. (tom)',]$match_name <- 'marshall, thomas'
  }
  if(t_yrs %in% c("1997_1998", "1999_2000", "2003_2004") ){
    all_sponsors[all_sponsors$LES_sponsor == 'davis, d. j.',]$match_name <- 'davis, d. j.'
    klarner_sub[klarner_sub$cand == 'davis, d. j.',]$match_name <- 'davis, d. j.'
  }
  if(t_yrs == "2001_2002"){
    all_sponsors[all_sponsors$LES_sponsor == 'green, tom',]$match_name <- 'green, thomas'
    all_sponsors[all_sponsors$LES_sponsor == 'johnson, rick',]$match_name <- 'johnson, richard'
  } 
  if(t_yrs %in% c("2003_2004")){
    all_sponsors[all_sponsors$LES_sponsor == 'johnson, rick',]$match_name <- 'johnson, richard'
    all_sponsors[all_sponsors$LES_sponsor == 'cooper, robert wayne',]$match_name <- 'cooper, wayne'
    all_sponsors[all_sponsors$LES_sponsor == 'harris, jeff',]$match_name <- 'harris, robert' ### Same district, must be a klarner error or actual first name
    all_sponsors[all_sponsors$LES_sponsor == 'scott',]$match_name <- 'scott, delbert' 
  } 
  if(t_yrs %in% c("2005_2006", "2007_2008")){
    all_sponsors$match_name <- ifelse(all_sponsors$LES_sponsor == 'johnson, rick', 'johnson, richard', all_sponsors$match_name)
    all_sponsors[all_sponsors$LES_sponsor == 'cooper, robert wayne',]$match_name <- 'cooper, wayne'
    all_sponsors[all_sponsors$LES_sponsor == 'harris, jeff',]$match_name <- 'harris, robert' 
  }   
  if(t_yrs %in% c("2017_2018")){
    all_sponsors[all_sponsors$LES_sponsor == 'barnes, jay',]$match_name <- 'barnes, jason'
  }
  
  ### Subset out General Specials
  klarner_gs <- filter(klarner_sub, etype == 'gs')
  klarner_sub <- filter(klarner_sub, etype != 'gs')
  
  ### Drop Duplicated Klarner Candidats
  klarner_sub <- arrange(klarner_sub, desc(year)) %>% group_by(sen) %>% filter(!duplicated(cand)) %>% ungroup()
  
  all_sponsors$elec_year <- all_sponsors$klarner_id <- all_sponsors$klarner_name <- NA
  all_sponsors$klarner_name <- as.character(all_sponsors$klarner_name)
  all_sponsors$klarner_id <- as.double(all_sponsors$klarner_id)
  all_sponsors$elec_year <- as.double(all_sponsors$elec_year)
  
  ### Loop though and Match
  for(i in 1:nrow(all_sponsors)){
    k_matches <- filter(klarner_sub, last_name == tolower(all_sponsors[i,]$last_name)  & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
    
    ## Acccount for lack of punctuation or spaces in Klarner Data
    if(nrow(k_matches) == 0 & grepl("-|'| |\\.|`", all_sponsors[i,]$last_name)){
      k_matches <- filter(klarner_sub, last_name == gsub("-|'| ||`", '', tolower(all_sponsors[i,]$last_name)))
    }
    
    ## Check Partial Names (e.g., maiden_name-last_name)
    if(nrow(k_matches) == 0 & grepl("-", all_sponsors[i,]$last_name)){
      k_matches <- filter(klarner_sub, last_name == gsub(".+-", '', tolower(all_sponsors[i,]$last_name)))
    }  
    
    ## Check GS 
    if(nrow(k_matches) == 0 & nrow(klarner_gs) > 1){
      k_matches <- filter(klarner_gs, last_name == tolower(all_sponsors[i,]$last_name)  & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
      if(nrow(k_matches) == 0){
        k_matches <- filter(klarner_gs, last_name == gsub("-|'| ||`", '', tolower(all_sponsors[i,]$last_name)))
      }
    }
    
    ### Save and Cross-Check
    if(nrow(k_matches) == 1){
      all_sponsors[i, ]$klarner_name <- k_matches$cand
      all_sponsors[i, ]$klarner_id <- k_matches$candid
      all_sponsors[i, ]$elec_year <- k_matches$year     
    } else if(nrow(k_matches) > 1 & length(unique(k_matches$cand)) != 1){
      ## Check Last, First
      m_sub <- grep(all_sponsors[i,]$match_name, k_matches$match_name)
      ### Check Without Punctuation --- Can't remove spaces unless do it for all_sponsors and k_matches
      if(length(m_sub) == 0){
        m_sub <- grep(gsub("-|'|`", '', all_sponsors[i,]$match_name), k_matches$match_name)        
      }
      # ## Check First Initial
      # if(length(m_sub) == 0){
      #   match_name2 <- tolower(paste0(all_sponsors[i,]$last_name, ", ", substring(all_sponsors[i,]$first_name, 1, 1) ))
      #   m_sub <- grep(match_name2, tolower(unlist(str_extract(k_matches$cand, '[a-zA-z]+, [a-zA-z]'))))
      # }
      ## Check Middle Name
      # if(length(m_sub) == 0 & !is.na(all_sponsors[i,]$middle_name)){
      #   match_name2 <- tolower(paste0(all_sponsors[i,]$last_name, ", ", all_sponsors[i,]$middle_name))
      #   m_sub <- grep(match_name2, k_matches$match_name)
      # }
      ### Save if Match
      if(length(m_sub) == 1){
        all_sponsors[i, ]$klarner_name <- k_matches[m_sub,]$cand
        all_sponsors[i, ]$klarner_id <- k_matches[m_sub,]$candid
        all_sponsors[i, ]$elec_year <- k_matches[m_sub,]$year     
      } else{
        print(glue("MULTIPLE MATCHES ::: {all_sponsors[i,]$LES_sponsor} ::: {i} ::: CHAMBER: {all_sponsors[i,]$chamber}"))
      }
    } else if(nrow(k_matches) > 1 & length(unique(k_matches$cand)) == 1){
      all_sponsors[i, ]$klarner_name <- unique(k_matches$cand)
      all_sponsors[i, ]$klarner_id <- unique(k_matches$candid)
      eyear <- as.numeric(str_split(t_yrs, "\\_")[[1]][1]) - 1
      all_sponsors[i, ]$elec_year <- k_matches[which(abs(k_matches$year - eyear) == min(abs(k_matches$year - eyear))),]$year
      rm(eyear)
    } else{
      print(glue("MISSING MATCH ::: {all_sponsors[i,]$LES_sponsor} ::: {i} ::: CHAMBER: {all_sponsors[i,]$chamber}"))
    }
  }
  #select(all_sponsors, LES_sponsor, klarner_name) %>% View()
  
  #### Fix Mismatches
  if(t_yrs == "1997_1998"){
    all_sponsors[all_sponsors$LES_sponsor == "thompson, betty" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "1999_2000"){
     all_sponsors[all_sponsors$LES_sponsor == "wilson, yvonne s." & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "2007_2008"){
    all_sponsors[all_sponsors$LES_sponsor == "kratky, michele" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "2015_2016"){
    all_sponsors[all_sponsors$LES_sponsor == "rowland, rory" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }
  
  #### CHeck for Duplicates from Matching
  duplicates <- filter(all_sponsors, !is.na(klarner_name)) %>% group_by(chamber, klarner_name) %>% filter(n() > 1)
  if(nrow(duplicates) > 0){
    cat('-----> Check DUPLICATE Sponsors \n ')
    print(select(duplicates, LES_sponsor, klarner_name, chamber) %>% as.data.frame())
  }; rm(duplicates)
  
  ##### CHECK IF ANY KLARNER CANDIDATES MISSING FROM DATA --- Exclude Senators elected T-1
  km <- filter(klarner_sub, !(candid %in% all_sponsors$klarner_id) )
  km <- filter(km, sen == 0 | (sen == 1 & year == as.numeric(substring(t_yrs, 1, 4)) - 1 ))
  
  ### Remove candidates who won but were never seated
  if(t_yrs == "1997_1998"){
    km <- filter(km, cand != 'sears, jim')
  } else if(t_yrs == "1999_2000"){
    km <- filter(km, cand != 'bland, mary')
  } else if(t_yrs == "2001_2002"){
    km <- filter(km, cand != 'riley, terry m.')
  } else if(t_yrs == "2005_2006"){
    km <- filter(km, cand != 'bishop, dan')
  } else if(t_yrs == "2013_2014"){
    km <- filter(km, cand != 'ruzicka, don')
  } else if(t_yrs == "2015_2016"){
    km <- filter(km, cand != 'torpey, noel')
  } else if(t_yrs == "2017_2018"){
    km <- filter(km, cand != 'jones, caleb')
  }

  #### Add Candidates who WON but Sponsored NO BILLS into Data
  if(nrow(km) > 0){
    cat(glue("\n-----> {as.numeric(substring(t_yrs, 1, 4)) - 1} KLARNER CANDIDATES MISSING MATCHES IN DATA ~~> ADDING INTO LIST OF LEGISLATORS: \n"))
    print(select(km, year, sab, sen, ddez, etype, deter, cand, candid, partyz, match_name) %>% as.data.frame())
    chamb <- ifelse(km$sen == 1, "S", "H")
    for(i in 1:nrow(km)){
      all_sponsors <- add_row(all_sponsors, chamber = chamb[i], term = t_yrs, klarner_name = km$cand[i], klarner_id = km$candid[i], elec_year = km$year[i])
    }
    rm(chamb)
  }
  
  #### Clean
  legis_data <- all_sponsors %>%
    rename(data_name = LES_sponsor) %>%
    mutate(sponsor = ifelse(!is.na(klarner_name), klarner_name, tolower(match_name))) %>%
    select(sponsor, data_name, klarner_name, klarner_id, chamber, term, elec_year, num_sponsored_bills, sponsor_pass_rate, sponsor_law_rate) %>%
    arrange(chamber, sponsor)
  
  ## NO COSPONSOR DATA:
  legis_data$num_cosponsored_bills <- NA
  
  ##########################################################
  ####### Estimate Scores + Add in Relatd Variables
  #############################################################
  ### Check if bills in data without an ID'd sponsor
  # filter(bills, !(bills$primary_sponsor %in% legis_data$data_name))
  bills <- bills %>% #select(-sponsors, cosponsors) %>%
    rename(sponsor = LES_sponsor) %>%
    mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'H', 'H', 'S'))
  
  ### Standard LES: Same as Congressional Measure
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/calc_LES_fx.R')
  
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
    select(sponsor, chamber, num_sponsored_bills, sponsor_pass_rate, sponsor_law_rate, num_cosponsored_bills) %>%
    mutate(chamber = ifelse(chamber == "H", "House", "Senate")) %>%
    left_join(LES, ., by = c("sponsor", "chamber"))
  
  #### If LES == 0 --> Fill in Missing Data
  LES[LES$LES == 0 & is.na(LES$num_sponsored_bills), c('num_sponsored_bills', 'sponsor_pass_rate', 'sponsor_law_rate')] <- 0
  
  ############### Save LES
  write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
  rm(LES)
  
  
  cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************"))
  
}
################# ****** END LOOP

rm(all_sponsors, bills, legis_data, SS_bills, elec_year, i, keep_types)
rm(k_matches, klarner_sub, m_sub, km, t_yrs, calc_LES, commem_bills) # c_sub
rm(t, terms, klarner_gs, t_num, term_num)

########################################################################################################################################################
############################################################################################################################################################# NAME FIX RECORDS 
############## MANUALLY CHECK/CLEANING NAMES AND MATCHES
########################################################################################################################################################
########################################################################################################################################################
### MEMBER LIST, BY Year of ELECTION: https://www.sos.mo.gov/archives/history/historicallistings/molegg
### Chamber Leadership: https://www.sos.mo.gov/archives/history/historicallistings/officers.asp


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1996 TERM! ~~~~~~~~~~~~~~ 
#   session chamber   N AIC ABC PASS LAW
# 1 1995-RS       H 757 571 273  142  79
# 2 1995-RS       S 478 438 184  129  83
# 3 1996-RS       H 880 654 290  149 101
# 4 1996-RS       S 503 463 202  159  97
## IN HOUSE:
# -- GRIFFIN (bob) -- Retired April 1996 (?) or at least that's when his replacement as speaker was picked 
# -- NORDWALD
# -- DANIEL (lloyd)
# -- KAUFFMAN
# -- ENZ
# -- SHELDON
# -- VOGEL
# -- MARBLE
# -- WANNENMACHER
## NAME FIX
# -- murray, connie == wible, connie (in klarner)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
#     session chamber    N AIC ABC PASS LAW
# 1  1997-RS       H  887 673 297  200 136
# 2  1997-RS       S  468 423 193  144  84
# 3 1997-SS1       H    4   4   4    4   3
# 4 1997-SS1       S    2   0   0    0   0
# 5  1998-RS       H 1051 778 278  203 136
# 6  1998-RS       S  518 470 188  153  89
## WON SPECIAL ~ HOUSE: 
# -- BARTLETT --  (https://house.mo.gov/content.aspx?info=/bills98/jrn98/jrn019.htm)
# -- DEMARCE
# -- HILGEMANN
# -- KLINDT
# -- MERIDETH
# -- THOMPSON (betty, won't show, duplicated last name) -- (https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=11047)
# -- Bartlett
## IN HOUSE:
# -- DANIELS
# -- DANIEL (lloyd)
# -- ENZ
# -- MILLER (ronnie)
# -- KASTEN
# -- PROST --- resigned early in term.. Merideth won his seat in April 1997 special... but he was there day 1 (https://house.mo.gov/content.aspx?info=/bills96/jrn96/jrn001.htm)
## DROP
# -- sears, jim -- died after winning office, never seated -- https://www.orlandosentinel.com/news/os-xpm-1996-11-29-9611280539-story.html
# NAME FIXES: 
# -- See script... many 1-term fixes

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
#   session chamber    N AIC ABC PASS LAW
# 1 1999-RS       H 1059 772 318  206 128
# 2 1999-RS       S  527 463 208  172  93
# 3 2000-RS       H 1097 733 178   99  54
# 4 2000-RS       S  554 483 160  124  21
## WON SPECIAL ~ HOUSE: 
# -- CURLS (melba)
# -- PHILLIPS (susan)
# -- RILEY (terry)
# -- WILSON (yvonne, won't show, name duplicated)
## WON SPECIAL ~ SENATE:
# -- BLAND (via H, 12/1998) 
## IN HOUSE:
# -- DANIELS (fletcher) -- passed away in March 1999 -- https://en.wikipedia.org/wiki/Fletcher_Daniels
# -- MURPHY
# -- ENZ
# -- KING
# -- BARTELSMEYER
# -- KASTEN
## DROP:
# -- 'bland, mary' IN HOUSE -- won senate special 12/1998 after reelected to House -- https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=6996

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
#     session chamber    N AIC ABC PASS LAW
# 1  2001-RS       H 1021 710 209  169 106
# 2  2001-RS       S  629 545 223  172  86
# 3 2001-SS1       H    5   5   3    3   3
# 4  2002-RS       H 1195 720 299  221 109
# 5  2002-RS       S  650 575 223  171  97
## WON SPECIAL ~ HOUSE:
# -- BLAND
# -- DAUS
# -- PAONE -- (tony, didn't run again -- https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=195047)
# -- QUINN
# -- SHOEMAKER
# -- WHORTON
## WON SPECIAL ~ SENATE:
# -- CAUTHORN (john, https://www.senate.mo.gov/ArchivedMembers/CauthornJohn.html)
# -- COLEMAN (maida, 2/2002, https://en.wikipedia.org/wiki/Maida_Coleman)
# -- DOUGHERTY (pat, 1/24/2001, https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=6989)
# -- KENNEDY (harry, 12/2001, https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=6979) 
# -- KLINDT (david, 1/24/2001, https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=6999)
## IN HOUSE:
# -- KlINDT (ever so briefly, won S special, but same timing as DOUGHERTY)
# -- PATEK (? -- per his linked in, left in 2001, but not clear when... election to fill his seat was Aug 2001, so must have been post-swearing in)
# -- CIERPIOT
# -- ENZ
# -- VOGEL
# -- MARSH
## DROP
# -- riley, terry m. -- seat filled by bland in feb 2001 special; his linkedin says he left office Dec 2000 (https://www.linkedin.com/in/terry-m-riley-81051a58)
## NAME FIXES:
# -- Lana Ladd BAKER matched to Lana Ladd STOKAN

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
#     session chamber   N AIC ABC PASS LAW
# 1  2003-RS       H 755 510 166  144  97
# 2  2003-RS       S 699 560 245  199 127
# 3 2003-SS1       H  23   7   6    6   4
# 4 2003-SS1       S   7   7   0    0   0
# 5 2003-SS2       H   7   0   0    0   0
# 6  2004-RS       H 997 556 208  181 116
# 7  2004-RS       S 700 530 222  174  86
## WON SPECIAL ~ HOUSE:
# -- MEADOWS (tim, 2/2004)
# -- SWINGER (terry, 11/2003)
## WON SPECIAL ~ SENATE:
# -- CALLAHAN (victor, 2004)
# -- COLEMAN (maida, 2002)
# -- KENNEDY (harry, 2001)
## IN HOUSE:
# -- YOUNG (terry); DARROUGH; YAEGER; BOUGH; KUESSNER
## NAME FIXES
# -- harris, jeff --> harris, robert h. == Either a Klarner error or its his actual first name and he doesn't use it
# -- scott in Senate = scott, delbert  -- via ps_url in data -- https://www.senate.mo.gov/04INFO/members/mem28.htm

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
#     session chamber    N AIC ABC PASS LAW
# 1  2005-RS       H  964 525 221  158 100
# 2  2005-RS       S  556 397 208  165  91
# 3 2005-SS1       H    6   3   3    2   2
# 4  2006-RS       H 1179 538 256  168  71
# 5  2006-RS       S  699 513 236  193  94
## WON SPECIAL ~ HOUSE:
# -- BOGETTO
# -- DAKE -- Unclear if Dake ever won again.. https://house.mo.gov/memberdetails.aspx?district=132&year=2006&code=R
# -- FRAME
# -- SCHARNHORST (dwight)
# -- SILVEY
# -- SMITH (jason)
## WON SPECIAL ~ SENATE:
# -- ALTER (bill, 2005)
# -- BARNITZ (frank, 2005)
# -- GOODMAN (jack, 2006)
## IN HOUSE:
# -- CURLS (melba); SCHOEMEHL (sue)
## DROP:
# -- bishop, dan -- passed away prior to start of term -- https://www.legacy.com/obituaries/name/dan-bishop-obituary?pid=2909311

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
#     session chamber    N AIC ABC PASS LAW
# 1  2007-RS       H 1292 596 283  148  75
# 2  2007-RS       S  709 502 259  174  55
# 3 2007-SS1       H    2   2   2    2   2
# 4 2007-SS1       S    1   0   0    0   0
# 5  2008-RS       H 1294 632 289  168  68
# 6  2008-RS       S  577 378 185  122  54
## WON SPECIAL ~ HOUSE: 
# -- PARKINSON (mark, 2/2008)
# -- KRATKY (michele, won't show, duplicate lastname, https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=162014)
## WON SPECIAL ~ SENATE:
# -- DEMPSEY (tom)
# -- GOODMAN (jack)
## IN HOUSE:
# -- SHIVELY; QUINN; WITTE; HAYWOOD; GEORGE; SELF; TODD

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
#     session chamber    N AIC ABC PASS LAW
# 1  2009-RS       H 1197 587 274  180 103
# 2  2009-RS       S  576 358 161  121  36
# 3  2010-RS       H 1265 435 225  141  69
# 4  2010-RS       S  491 320 149  112  30
# 5 2010-SS1       H    2   2   2    2   2
# 6 2010-SS1       S    2   1   1    0   0
## WON SPECIAL ~ HOUSE:
# -- AYRES (nita jane, lost or didn't run again)
# -- CONWAY
# -- NEWMAN
# -- WHITEHEAD (hope, lost or didn't run again)
## WON SPECIAL ~ SENATE:
# -- KEAVENY
## IN HOUSE:
# -- MCDONALD; VOGT; LIESE; CASEY; SELF; RICHARD (ron)
## NAME FIXES:
# -- fischer, linda --> black, linda -- https://ballotpedia.org/Linda_Black_(Missouri)
# -- stacey newman --> newman, stacey --> Runs again, ordered wrong this term..

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
#     session chamber    N AIC ABC PASS LAW
# 1  2011-RS       H 1022 626 267  196  92
# 2 2011-SS1       H   10   6   6    6   0
# 3  2012-RS       H 1072 689 316  238  70
# 4  2012-RS       S  479 304 151   91  28
## WON SPECIAL ~ HOUSE:
# -- ELLINGTON
# -- MCCREERY (lost subsquent primary, reelected 2 years later, https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=286670)
# -- MORGAN
# -- SOMMER
## WON SPECIAL ~ SENATE:
# -- CURLS (shalonn 'kiki')
## IN HOUSE:
# -- QUINN; PIERSON; CASEY

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
#     session chamber    N AIC ABC PASS LAW
# 1  2013-RS       H 1033 598 285  195  72
# 2  2013-RS       S  484 348 184  137  66
# 3 2013-SS1       H    2   1   1    0   0
# 4 2013-SS1       S    1   1   1    1   1
# 5  2014-RS       H 1248 774 370  216  90
# 6  2014-RS       S  509 347 190  135  69
## WON SPECIAL ~ HOUSE: 
# -- MOON
# -- PETERS (josh)
## IN HOUSE:
# -- ANDERS (ira); RUNIONS (joe); CARTER (chris); GANNON; PIKE; KEENEY; FOWLER
# -- Fowler resigned 12/2013 to accept appointment -- https://ballotpedia.org/Dennis_Fowler
## DROP
# -- ruzicka, don -- resigned 12/2012 to accept appointment -- https://ballotpedia.org/Don_Ruzicka

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
#    session chamber    N AIC ABC PASS LAW
# 1 2015-RS       H 1357 753 529  270  71
# 2 2015-RS       S  568 380 183  120  51
# 3 2016-RS       H 1455 732 550  251  77
# 4 2016-RS       S  583 403 188  113  51
## WON SPECIAL ~ HOUSE: 
# -- MCGEE (daron)
# -- PLOCHLER (dean)
# -- ROWLAND (rory, won't show up, last name duplicated)
## IN HOUSE:
# -- MCDONALD (tom); KEENEY
## DROP:
# -- torpey, noel -- resigned 12/2014 -- https://ballotpedia.org/Noel_Torpey

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
#     session chamber    N AIC ABC PASS LAW
# 1  2017-RS       H 1223 693 511  190  31
# 2  2017-RS       S  544 380 248   87  36
# 3 2017-SS1       H    6   4   1    1   1
# 4 2017-SS1       S    7   0   0    0   0
# 5 2017-SS2       H   17   4   2    0   0
# 6 2017-SS2       S    8   3   3    1   1
# 7  2018-RS       H 1508 723 567  233  81
# 8  2018-RS       S  556 394 161  108  58
# 9 2018-SS1       S    1   1   0    0   0
## WON SPECIAL ~ HOUSE:
# -- DINKINS (chris)
# -- KNIGHT (jeff)
# -- MORSE (herman)
# -- REVIS (mike, lost subsequent general election)
# -- WALSH
# -- WASHINGTON
## WON SPECIAL ~ SENATE:
# -- CIERPIOT (mike, via H, 11/2017)
# -- CRAWFORD (sandy, 11/2017)
# -- HUMMEL (jacob, took office on time, but resigned 12/2018 -- https://themissouritimes.com/55561/hummel-resigns-from-the-senate/)
## IN HOUSE
# -- CIERPIOT (approx half term pre senate special win)
## DROP
# -- jones, caleb -- resigned pre-swearing in to become deputy chief of staff to governor



# filter(klarner, grepl('hummel', cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(klarner, ddez == 43 & outcome == 'w') %>% select(cand, year, sen, etype, outcome, ddez, candid)


##########################################################################################################
##########################################################################################################
##### MATCHING MISSING (SPECIALS) TO KLARNER USING OTHER TERMS WHERE SPONSOR IS PRESENT
##########################################################################################################
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


########## ****Still missing*****
## Rest are 1-termers or 2017-2018 + RORY ROWLAND which is a klarner error
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[13]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name,  still_missing)

name_matches <- data.frame(LES_name = "bland", k_name = 'bland, mary')
name_matches <- add_row(name_matches, LES_name = 'whorton, james', k_name = 'whorton, jim')
name_matches <- add_row(name_matches, LES_name = 'daus, michael', k_name = 'daus, mike') 
name_matches <- add_row(name_matches, LES_name = 'shoemaker, christopher', k_name = 'shoemaker, chris')
name_matches <- add_row(name_matches, LES_name = 'dougherty', k_name = 'dougherty, patrick')
name_matches <- add_row(name_matches, LES_name = 'curls', k_name = 'curls, shalonn (kiki)')
name_matches <- add_row(name_matches, LES_name = 'cierpiot', k_name = 'cierpiot, mike')
name_matches <- add_row(name_matches, LES_name = 'crawford', k_name = 'crawford, sandy')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', k_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}

##### Matches requiring greater precision
# LES[LES$sponsor %in% "keith-agaran" & LES$term %in% "2009_2010",]$klarner_id <- 297229
# LES[LES$sponsor %in% "keith-agaran" & LES$term %in% "2009_2010",]$klarner_name <- "keithagaran, gil s. (coloma)"
# LES[LES$sponsor %in% "keith-agaran" & LES$term %in% "2009_2010",]$sponsor <- "keithagaran, gil s. (coloma)"

rm(name_matches, i)

############## Check for Duplicates
### ---> If two people with same last name and one won in a special, will lead to duplicates
### Remaining = Switched from House to Senate
for(t in unique(LES$term)){
  print(glue(' ************ {t} ***************'))
  check_dup <- filter(LES, term == t & !is.na(klarner_id)) %>%
    group_by(chamber) %>%
    mutate(dup = duplicated(klarner_id)) 
  if(any(check_dup$dup)){
    filter(LES, term == t & klarner_id %in% check_dup[check_dup$dup == TRUE,]$klarner_id & !is.na(klarner_id)) %>%
      select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>%
      print()
  }
}
rm(check_dup, k_sub, exact, missing, name_sub)


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

### Manually Fix Error (becasue matched to wrong guy)
LES[LES$sponsor == "rowland, rory",]$party <- 'd'
LES[LES$sponsor == "rowland, rory",]$district <- 29

### Manually Fix Those Not in Klarner
fill_missing <- data.frame(LES_name = "demarce, karl", party = 'd', district = 1, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "bartlett, robert", party = 'd', district = 60, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "paone, toby", party = 'd', district = 66, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "ayres, nita", party = 'r', district = 62, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "whitehead, hope", party = 'd', district = 57, exper = 'none')
#fill_missing <- add_row(fill_missing, LES_name = "rowland, rory", party = 'd', district = 29, exper = 'none')
#### Below this line are 2017-2018 Legislators --> likely not needed with klarner update
fill_missing <- add_row(fill_missing, LES_name = "washington, barbara", party = 'd', district = 23, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "morse, herman", party = 'r', district = 151, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "walsh, sara", party = 'r', district = 50, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "knight, jeff", party = 'r', district = 129, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "dinkins, chris", party = 'r', district = 144, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "revis, mike", party = 'd', district = 197, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzt", party = 'zzzz', district = zzzz, exper = 'none')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
}

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t, fill_missing, i)

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

### If > 2-Year Terms: Expand Senate Rows
senate <- filter(hf_data, CandId == 'aaaa')
for(i in 1:nrow(hf_data)){
  if(hf_data[i,]$chamber == "House") next
  sen_sub <- filter(hf_data, CandId == hf_data[i,]$CandId)
  if( !(paste0( hf_data[i,]$year + 3, "_",  hf_data[i,]$year + 4) %in% sen_sub$term) ){
    new_row <- hf_data[i,]
    new_row$MajorityMember <- NA
    new_row$term <- paste0( hf_data[i,]$year + 3, "_",  hf_data[i,]$year + 4)
    senate <- bind_rows(senate, new_row)
  }
}
hf_data <- bind_rows(hf_data, senate); rm(senate, new_row, sen_sub)

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

rm(hf_data, set_NA, i)

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

### Correct Errors
LES[LES$sponsor == 'smith, cody', c("SM_name", 'SM_party', 'np_score')] <- NA
LES[LES$sponsor == 'trent, curtis d.', c("SM_name", 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
## ** Notable Names: William Todd Akin
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# ---- Remaining are mostly from earlier years or later years
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('yate', tolower(name))) %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

name_matches <- data.frame(LES_name = 'beatty, gail mccann', SM_name = 'McCann Beatty, Gail')
# name_matches <- add_row(name_matches, LES_name = 'barnes, jim', SM_name = 'zzzzzzzzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'cooper, wayne', SM_name = 'Cooper, Robert Wayne')
name_matches <- add_row(name_matches, LES_name = 'davis, d. j.', SM_name = 'Davis, Dahlman')
name_matches <- add_row(name_matches, LES_name = 'edwards, marilyn', SM_name = 'Edwards-Pavia, Marilyn')
name_matches <- add_row(name_matches, LES_name = 'george, thomas (tom)', SM_name = 'George, Thomas')
name_matches <- add_row(name_matches, LES_name = 'gray, rochelle walton', SM_name = 'Walton Gray, Rochelle')
# name_matches <- add_row(name_matches, LES_name = 'griffin, bob f. 1', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'henderson, steve', SM_name = 'Henderson, Steven')
name_matches <- add_row(name_matches, LES_name = 'keeney, shelley (white)', SM_name = 'Keeney Taylor, Shelley')
name_matches <- add_row(name_matches, LES_name = 'liese, christopher a. (chris)', SM_name = 'Liese, topher A.') # https://votesmart.org/candidate/biography/9368/topher-a-liese
name_matches <- add_row(name_matches, LES_name = 'may, robert', SM_name = 'May, Bob')
name_matches <- add_row(name_matches, LES_name = 'mckenna, william (bill)', SM_name = 'McKenna') # https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=186358
name_matches <- add_row(name_matches, LES_name = 'oxford, jeanette mott', SM_name = 'Mott Oxford, Jeanette')
name_matches <- add_row(name_matches, LES_name = 'saintonge, neal c.', SM_name = 'St. Onge, Neal')
name_matches <- add_row(name_matches, LES_name = 'sandersbrooks, sharon', SM_name = 'Brooks, Sharon Sanders')
name_matches <- add_row(name_matches, LES_name = 'smith, joe', SM_name = 'Smith, Joseph')
name_matches <- add_row(name_matches, LES_name = 'walsh, gina', SM_name = 'Walsh, Regina')
name_matches <- add_row(name_matches, LES_name = 'ward, bob', SM_name = 'Ward, Robert D')
name_matches <- add_row(name_matches, LES_name = 'wible, connie', SM_name = 'Murray, Connie')
name_matches <- add_row(name_matches, LES_name = 'wilson, ken', SM_name = 'Wilson, Kenneth')
name_matches <- add_row(name_matches, LES_name = 'wilson, kevin bill', SM_name = 'Wilson, Kevin')
name_matches <- add_row(name_matches, LES_name = 'wilson, yvonne s.', SM_name = 'Wilson')
# name_matches <- add_row(name_matches, LES_name = 'yates, michael zane', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

# #### Manual Edits (Needs more precision...)
# LES[LES$sponsor == 'clemons, billy' & LES$term != '1995_1996',]$SM_name <- ideo[ideo$name == 'Clemons' & ideo$party == "D",]$name
# LES[LES$sponsor == 'clemons, billy' & LES$term != '1995_1996',]$SM_party <- ideo[ideo$name == 'Clemons' & ideo$party == "D",]$party
# LES[LES$sponsor == 'clemons, billy' & LES$term != '1995_1996',]$np_score <- ideo[ideo$name == 'Clemons' & ideo$party == "D",]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname)
#rm(ideo, ideo_matches, LES_match, ideo_match, check_last, i)


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1995 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:2002) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2003:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1995 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:2000) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2001:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()

##### Drop Nicknames
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

### Manual Fixes
LES[LES$sponsor == "pierson, tommie" & as.numeric(substring(LES$term, 1, 4)) < 2017,]$sponsor <- 'pierson, tommie sr.'
LES[LES$sponsor == "pierson, tommie" & as.numeric(substring(LES$term, 1, 4)) >= 2017,]$sponsor <- 'pierson, tommie l. jr.'
LES[LES$sponsor == "gratz, w. w.",]$sponsor <- 'gratz, william w.'
LES[LES$sponsor == "marshall, t. w.",]$sponsor <- 'marshall, thomas w.'
LES[LES$sponsor == "kelley, pat",]$sponsor <- 'kelley, patrick'
LES[LES$sponsor == "mitchell, j. b.",]$sponsor <- 'mitchell, jim b.'
LES[LES$sponsor == "kauffman, sandy",]$sponsor <- 'kauffman, sandra'
LES[LES$sponsor == "howard, j. t.",]$sponsor <- 'howard, jerry t.'
LES[LES$sponsor == "graves, sam",]$sponsor <- 'graves, samuel'
LES[LES$sponsor == "johnson, m. e.",]$sponsor <- 'johnson, mitchell e.'
LES[LES$sponsor == "holt, b. w.",]$sponsor <- 'holt, bruce w.'
LES[LES$sponsor == "spreng, c. m.",]$sponsor <- 'spreng, churie m.'
LES[LES$sponsor == "basye, c. ben",]$sponsor <- 'basye, charles ben'
#LES[LES$sponsor == "zzzzz",]$sponsor <- 'ssssss'

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
  scale_color_manual(values=c("dodgerblue2", "red2", "gray50"))

##### CHECK OUTLIERS
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')
# stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

