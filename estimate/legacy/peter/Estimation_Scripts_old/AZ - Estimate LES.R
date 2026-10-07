

#####################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** ARIZONA *** BY SESSION
#####################################

###################################
## SPECIAL SESSIONS:
## ---- Seperate files, bill numbers re-start
## MEMBER LISTS:
## ---- https://www.azleg.gov/MemberRoster/?body=S
## PROCESS:
## ---- http://libguides.law.asu.edu/ArizonaLaw/legislativeprocess
## Sponsorship/Authorship
## ---- Introducing sponsor clear in data
## ---- Committee sponsored bills permitted (rare -- in early years, at least...)
## ---- Starting in 2017, no longer identifies introducing sponsor, so using 1st primary (which seems to only be 1 person now anyway)
###########################
## NOTES:
## (1) AZ doesn't post list of actions --- actions constructed from API list of info, but not all details included (eg, if third reading passed)
## -----> So using a relatively more limited set of actions, but should still work the same based on how gathered
## (2)
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

this_state <- 'AZ'
min_year <- 1995
max_year <- 2018
keep_types <- c("HB", "SB")
spec_elec_codes <- c('s', 'gs')
sen_term_length <- 2 # Staggered? NA /// House Districts = Multimember (2 per), overlap with Senate districts

#### Output Directory
#dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Data Directory
data_files <- list.files(glue("~/Dropbox/Data/State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]

terms <- seq(min_year, max_year, 2)
sessions <- sort(gsub('.+Details_|.csv', '', bill_files))
rm(data_files, bill_files)

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("~/Dropbox/Data/State Legislative Data/Commem Bills/{this_state}_Commem_Bills.csv"))
commem_bills <- rename(commem_bills, session_short = session)

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- read.csv("~/Dropbox/Data/State Legislative Data/Significant Bills/SS_Data/SS_Bills.csv")
SS_bills <- SS_bills %>% 
  filter(state == this_state) %>%
  rename(bill_id = bill_num) %>%
  mutate(term = ifelse(year %% 2 == 1, paste0(year, "_", year + 1), paste0(year - 1, "_", year)),
         bill_id = toupper(bill_id),
         #bill_id = ifelse(bill_type == "S" & !grepl("^SB", bill_id), gsub("^S", "SB", bill_id), bill_id),
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
klarner[klarner$cand == 'peterson, david',]$cand <- "petersen, david"
# ----> IDs will still be off, but need to keep them to match to external data...

### hellon Beat Osterloh -- https://apps.azsos.gov/election/2000/General/Canvass2000GE.pdf
klarner[klarner$cand == "osterloh, mark" & klarner$year == 2000,]$outcome <- 'l'
klarner[klarner$cand == "hellon, toni" & klarner$year == 2000,]$outcome <- 'w'


###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[2]

for(t in terms){
  
  ### Formulate 2-year terms -- Cover both regular and special sessions
  t_yrs <- as.character(glue('{t}_{t+1}'))
  t_sessions <- sessions[grepl(t, sessions)]
  
  ### Skip Previously Estimated
  if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
    print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
    next
  }
  
  ###### SESSION IN PROGRESS
  print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))
  
  ############### Read in data
  bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_sessions[1]}.csv")
  bills <- read.csv(bill_path)   
  
  ### If multiple sessions in different files, read in those as well
  if(length(t_sessions) > 1){
    for(s in t_sessions[2:length(t_sessions)]){
      bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{s}.csv")
      s_bills <- read.csv(bill_path)
      bills <- bind_rows(bills, s_bills)
    }
    rm(s, s_bills)
  }
  
  ### Clean Term/Session Variables
  bills$term <- t_yrs
  bills$session <- gsub('^[0-9]+_[0-9]+_', '', bills$session)
  bills$session <- recode(bills$session, 'First_Regular' = 'RS1', 'Second_Regular' = 'RS2', 'First_Special' = 'SS1', 'Second_Special' = 'SS2', 
                          'Third_Special' = 'SS3', 'Fourth_Special' = 'SS4', 'Fifth_Special' = 'SS5', 'Sixth_Special' = 'SS6', 
                          'Seventh_Special' = 'SS7', 'Eighth_Special' = 'SS8', 'Ninth_Special' = 'SS9', 'Tenth_Special' = 'SS10')
  bills$session_year <- ifelse(bills$session == "RS1", as.numeric(substring(t_yrs, 1, 4)), ifelse(bills$session == 'RS2', as.numeric(substring(t_yrs, 6, 9)), NA))
  bills$session_year <- ifelse(is.na(bills$session_year) & bills$session_num < unique(bills[bills$session == "RS2",]$session_num),  as.numeric(substring(t_yrs, 1, 4)), ifelse(is.na(bills$session_year), as.numeric(substring(t_yrs, 6, 9)), bills$session_year))
  bills$session_short <- bills$session
  bills$session <- paste0(bills$session_year, "-", ifelse(grepl("^RS", bills$session_short), 'RS', bills$session_short)   )
  # table(bills$session, bills$session_num)
  
  ### Check for duplicates
  if(nrow(bills) != nrow(distinct(bills))){
    cat(" \n ~~~> DUPLICATE BILLS \n\n .")
    break
  }
  
  ######## Standardize the Bill IDs
  bills <- rename(bills, bill_id = bill_number)
  
  ############### Drop Resolutions, Messages, Communications, Reports
  bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
  # table(bills$bill_type)
  bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)
  
  ############### Standardize Sponsors + Extracting + Ensuring Cosponsorship match down the line
  #bills$sponsor_dist <- str_extract(bills$sponsor, 'HD [0-9]+|SD [0-9]+')
  #bills$sponsor_party <- str_extract(bills$sponsor, '\\([A-Z]\\)')
  # table(bills$sponsor_dist); table(bills$sponsor_party)
  
  #### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
  if(sum(is.na(bills$intro_sponsor)) == nrow(bills)){
    bills$intro_sponsor <- gsub(';.+', '', bills$primary_sponsors)
  }
  
  bills$intro_sponsor <- tolower(bills$intro_sponsor)
  bills$intro_sponsor <- gsub('á', 'a', bills$intro_sponsor)
  bills$intro_sponsor <- gsub('é', 'e', bills$intro_sponsor)
  bills$intro_sponsor <- gsub('ó', 'o', bills$intro_sponsor)
  bills$intro_sponsor <- gsub('í', 'i', bills$intro_sponsor)
  bills$intro_sponsor <- gsub('ñ', 'n', bills$intro_sponsor)
  
  ## Including all three and then subtracting off the difference for cosponsors because too much effort to extract the name off the front
  bills$cospon_match <- paste(tolower(bills$intro_sponsor), tolower(bills$primary_sponsors), tolower(bills$cosponsors), sep = ";")
  bills$cospon_match <- gsub('á', 'a', bills$cospon_match)
  bills$cospon_match <- gsub('é', 'e', bills$cospon_match)
  bills$cospon_match <- gsub('ó', 'o', bills$cospon_match)
  bills$cospon_match <- gsub('í', 'i', bills$cospon_match)
  bills$cospon_match <- gsub('ñ', 'n', bills$cospon_match)
  
  #### Adjusting for Bills By Request -- Need to do BEFORE splitting
  if(any(grepl('\\(br\\)|by request| br$', bills$intro_sponsor))){
    print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
    #print(glue("-----> KEEPING {nrow(filter(bills, grepl('(br)|by request', introducers)))} bill(s) introduced BY REQUEST"))
  }
  
  #### DROP Committee Bills
  ## Committees: (maj) nrae; 
  if(any(grepl('committee|^nrae$|maj nrae|^gov$', bills$intro_sponsor))){
    print(glue("-----> DROPPING {nrow(filter(bills, grepl('committee|^nrae$|maj nrae|^gov$', intro_sponsor)))} bill(s) introduced BY COMMITTEE"))
    bills <- filter(bills, !grepl('committee|^nrae$|maj nrae|^gov$', intro_sponsor))
  }
  
  #### LES Sponsor Variable
  bills$LES_sponsor <- bills$intro_sponsor
  # sort(table(bills$LES_sponsor))
  
  ## Fill In Missing SPonsors with Primary, Adjust Cosponsorship
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == '', gsub(';.+', '', tolower(bills$primary_sponsors)), bills$LES_sponsor )
  # for(i in 1:nrow(bills)){
  #   if(bills[i,]$intro_sponsor == ''){
  #     bills[i,]$cospon_match <- gsub(glue("^{bills[i,]$LES_sponsor};|^{bills[i,]$LES_sponsor}"), '', bills[i,]$cospon_match )
  #   }
  # }
  
  ###### CHeck Missing Sponsors
  # filter(bills, LES_sponsor == "") %>% View()
  if(nrow(filter(bills, LES_sponsor == '')) > 0){
    print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
    bills <- filter(bills, !(LES_sponsor == ''))     
  }
  
  ###################
  ###### Merge in S&S Bills
  ###################
  # *** For ARIZONA: Bills do NOT Carryover - Multiple Specials
  # ---> Capping Bill numbers at SESSION MAX and assuming bill is from SS with most bills proposed
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
           session = ifelse(special == 0, paste0(year, '-RS'), ifelse(is.na(special_num), s_spec, paste0(year, '-SS', special_num)))) %>%
    distinct(term, session, bill_id, SS)
  
  ### Merge
  bills <- bills %>%
    left_join(SS_term, by = c("bill_id", "term", "session")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  
  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id", "term", "session"))
  
  ############### Code Commemorative
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session_short, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session_short'))
  # table(bills$commem)

  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  ############### Code Bill History
  bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
  bill_hist <- read.csv(bill_hist_path)
  
  ## If multiple sessions, read in those as well
  if(length(t_sessions) > 1){
    for(s in t_sessions[2:length(t_sessions)]){
      bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
      s_hist <- read.csv(bill_path)
      bill_hist <- bind_rows(bill_hist, s_hist)
    }
    rm(s, s_hist)
  }

  ######## Standardize BillHist Bill IDs
  bill_hist <- rename(bill_hist, bill_id = bill_number) 
  
  ### Clean Term/Session Variables
  bill_hist$term <- t_yrs
  bill_hist$session <- gsub('^[0-9]+_[0-9]+_', '', bill_hist$session)
  bill_hist$session <- recode(bill_hist$session, 'First_Regular' = 'RS1', 'Second_Regular' = 'RS2', 'First_Special' = 'SS1', 'Second_Special' = 'SS2', 
                          'Third_Special' = 'SS3', 'Fourth_Special' = 'SS4', 'Fifth_Special' = 'SS5', 'Sixth_Special' = 'SS6', 
                          'Seventh_Special' = 'SS7', 'Eighth_Special' = 'SS8', 'Ninth_Special' = 'SS9', 'Tenth_Special' = 'SS10')
  bill_hist$session_year <- ifelse(bill_hist$session == "RS1", as.numeric(substring(t_yrs, 1, 4)), ifelse(bill_hist$session == 'RS2', as.numeric(substring(t_yrs, 6, 9)), NA))
  bill_hist$session_year <- ifelse(is.na(bill_hist$session_year) & bill_hist$session_num < unique(bill_hist[bill_hist$session == "RS2",]$session_num),  as.numeric(substring(t_yrs, 1, 4)), ifelse(is.na(bill_hist$session_year), as.numeric(substring(t_yrs, 6, 9)), bill_hist$session_year))
  bill_hist$session_short <- bill_hist$session
  bill_hist$session <- paste0(bill_hist$session_year, "-", ifelse(grepl("^RS", bill_hist$session_short), 'RS', bill_hist$session_short)   )
  
  ### Creating Order Variale
  # Note ----> *** BECAUSE OF HOW AZ CODES ACTIONS OCCURING ON SAME DAY MAY BE OUT OF ORDER ***
  bill_hist <- bill_hist %>%
    group_by(session, bill_id) %>%
    mutate(action_date = as.character(ifelse(grepl("/", action_date), format(as.Date(action_date, format = "%m/%d/%Y"), '%Y-%m-%d'), action_date))) %>%
    arrange(session, bill_id, action_date) %>%
    mutate(order = 1:n()) %>%
    ungroup()

  
  ### Standardize Chamber Variable
  # bill_hist$chamber <- recode(toupper(bill_hist$chamber), "H" = "House", "S" = "Senate")
  ### *** Could fill in blanks, which appear to be mostly just "Chapter Number Assigned" = LAW
  
  ### Check THird Reading Terms
  tr_terms <- filter(bill_hist, grepl('third reading', tolower(action))) %>% distinct(action) %>% unlist() %>% unname()
  for(tr in tr_terms){
    if(!(tr %in% c("None Third Reading", "Passed Third Reading", "Failed Third Reading"))){
      cat(" \n ************ CHECK THIRD READING TERMS ************** \n|")
      print(tr)
      break
    }
  }
  rm(tr, tr_terms)
  
  ### Fill in Missing Chambers
  bill_hist[grepl('transmitted to senate', tolower(bill_hist$action)) & bill_hist$chamber == '',]$chamber <- "House"
  bill_hist[grepl('transmitted to house', tolower(bill_hist$action)) & bill_hist$chamber == '',]$chamber <- "Senate"
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  #### STANDARDIZING REPORTED FROM + Withdrawals
  bill_hist$action <- gsub('reported failed from ', 'COMMITTEE REPORT--FAILED~', tolower(bill_hist$action))
  bill_hist$action <- gsub('reported held from ', 'COMMITTEE REPORT--HELD~', tolower(bill_hist$action))
  bill_hist$action <- gsub('reported disc/held from ', 'COMMITTEE REPORT--DISC/HELD~', tolower(bill_hist$action))
  bill_hist$action <- gsub('reported w/d from ', 'COMMITTEE--WITHDRAWN~', tolower(bill_hist$action))
  
  ##### Set State-Specific Terms for Identifying Each Stage
  # NOTE: In house, second reading = post-comm; in senate, second sometimes occurss BEFORE comm referral
  # ----> Not always true though? See, eg, HB2008, 1995_1995 SS5 --- Assigned, never reported, but read day after assignment to comm
  aic_t <- c("^reported.+from.+committee", 'committee report--')
  # ---> Committee Report = Recoded Above to capture AIC without bill making it to floor
  abc_t <- c('^reported.+from.+committee', 'third reading', 'committee of the whole', 'floor motion')
  pc_t <- c('transmitted to house', 'transmitted to senate', 'final reading', 'transmitted to gov', 'conference comm') # final = back to originating chamber 
  # ---> Third reading just means a vote on third reading happend... sometimes system says passed/failed, but not always
  # ------ Not clear if when it's coded as 'passed third reading' if it always accounts for higher vote thresholds.. safer to use transmitted to..
  # ---> Also note that anything coded RFE has higher threshold (2/3 or 3/4 for passage)
  law_t <- c('^law', "chapter number")
  
  ### Check Actions
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
    hist_sub <- filter(bill_hist, bill_id == b_id, session == s_id)
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t, 
                                      ignore_chamber_switch = TRUE)
    bill_stages$bill_url <- paste0("https://apps.azleg.gov/BillStatus/BillOverview/", bills[i,]$bill_id_num, "?Sessionid=", bills[i,]$session_num)
    all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
    # print(i)
  }
  options(warn = 1)
  
  ### Check Codings
  cat('\n')
  all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
    summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% as.data.frame() %>% print()
  # filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% filter(!grepl('^by resolution|^first read|^prefiled', tolower(action))) %>% View()
  
  ### MERGE to BILL DATA
  bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 
  
  ## MERGE In COMMEMS
  all_bill_stages <- commem_bills %>%
    filter(term == t_yrs) %>%
    left_join(distinct(bills, session, session_short), by = "session_short") %>%
    select(bill_id, term, session, commem) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))
  
  ### MERGE In S&S
  all_bill_stages <- SS_term %>%
    select(bill_id, term, session, SS) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term', "session")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  rm(SS_term)
  
  ### Adjust Commems if SS == 1
  all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
  
  ## Save Stage Info **** MERGE WITH SS **********
  if(!dir.exists(glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}'))){
    dir.create(glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}'))
  }
  write.csv(all_bill_stages, glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv'), row.names = FALSE )
  
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
    ungroup()
  
  ######## Get Cosponsorship Nums
  all_sponsors$num_cosponsored_bills <- NA
  for(i in 1:nrow(all_sponsors)){
    c_sub <- filter(bills, substring(bill_id, 1, 1) == all_sponsors[i,]$chamber)
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
    ### ONLY need to adjust this way if sponsored and cosponsored column are the same
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  }
  # all_sponsors <- select(all_sponsors, -cospon_match)
  # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))
  
  ############
  ### CLEAN NAMES
  ###############
  
  all_sponsors$last_name <- ifelse(!grepl(' [a-z]$| [a-z][a-z]$', all_sponsors$LES_sponsor), all_sponsors$LES_sponsor, gsub(' [a-z]$| [a-z][a-z]$', '', all_sponsors$LES_sponsor))
  all_sponsors$first_name <- ifelse(grepl(' [a-z]$| [a-z][a-z]$', all_sponsors$LES_sponsor), str_trim(str_extract(all_sponsors$LES_sponsor, ' [a-z]$| [a-z][a-z]$')), '')
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  
  #### Update Last Names for Matching 
  if( t >= 2001 & t <= 2014){
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "landrum taylor", "landrum", all_sponsors$last_name)
  }
  if(t >= 2007 & t <= 2010){
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "young wright", "wright", all_sponsors$last_name)
  }
  if(t >= 2015 & t <= 2022){ ## Won senate seat in 2018
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "ugenti-rita", "ugenti", all_sponsors$last_name)
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
  
  #######
  ### Create KLARNER Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), '')))
  klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]')))
  
  ### Edit Klarner Match Name
  if(t_yrs == "2003_2004"){
    all_sponsors[all_sponsors$LES_sponsor == "jackson, jr",]$last_name <- "jackson"
    all_sponsors[all_sponsors$LES_sponsor == "jackson sr.",]$last_name <- "jackson"
    klarner_sub[klarner_sub$cand == 'jackson, jack c. jr.',]$match_name <- 'jackson, jr'
    klarner_sub[klarner_sub$cand == 'jackson, jack c.',]$match_name <- 'jackson sr.'
  } 
  if(t >= 2005 & t <= 2012){
    all_sponsors[all_sponsors$LES_sponsor == "weiers j",]$match_name <- "weiers, james"
    all_sponsors[all_sponsors$LES_sponsor == "weiers jp",]$match_name <- "weiers, jerry"  ### Jerry = JP Weiers -- See bills: https://www.azleg.gov/house-member/?legislature=50&legislator=1210
    klarner_sub[klarner_sub$cand == 'weiers, james (jim)',]$match_name <- 'weiers, james'
    klarner_sub[klarner_sub$cand == 'weiers, jerry',]$match_name <- 'weiers, jerry'
  }
  if(t >= 2007 & t <= 2010){
    klarner_sub[klarner_sub$cand == 'campbell, chad',]$match_name <- 'campbell, ch'
    klarner_sub[klarner_sub$cand == 'campbell, cloves c. jr.',]$match_name <- 'campbell, cl'    
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
  # if(t_yrs == "2009_2010"){
  #   all_sponsors[all_sponsors$LES_sponsor == "art noonan" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  # }
  
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
    km <- filter(km, cand != 'king, ned')
  } else if(t_yrs == "2003_2004"){
    km <- filter(km, cand != 'guenther, herb')
    km <- filter(km, cand != 'valadez, ramon o.')
    km <- filter(km, !(cand == 'cannell, robert' & sen == 0))
    km <- filter(km, !(cand == 'soltero, victor' & sen == 0))
  } else if(t_yrs == "2009_2010"){
    km <- filter(km, cand != 'gallardo, steve')
  }
  
  #### Add Candidates who WON but Sponsored NO BILLS into Data
  if(nrow(km) > 0){
    cat(glue("-----> {as.numeric(substring(t_yrs, 1, 4)) - 1} KLARNER CANDIDATES MISSING MATCHES IN DATA ~~> ADDING INTO LIST OF LEGISLATORS: \n . "))
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
    select(sponsor, data_name, klarner_name, klarner_id, chamber, term, elec_year, num_sponsored_bills, sponsor_pass_rate, sponsor_law_rate, num_cosponsored_bills) %>%
    arrange(chamber, sponsor)
  
  ####################################################################
  ############### Estimate Scores + Add in Relatd Variables
  ####################################################################
  ### Check if bills in data without an ID'd sponsor
  # filter(bills, !(bills$primary_sponsor %in% legis_data$data_name))
  bills <- bills %>% #select(-sponsor) %>%
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
  
  #### If LES == 0 and --- , "num_cosponsored_bills"
  LES[LES$LES == 0 & is.na(LES$num_sponsored_bills), c('num_sponsored_bills', 'sponsor_pass_rate', 'sponsor_law_rate')] <- 0
  
  ############### Save LES
  write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
  rm(LES)
  
  
  cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************"))
  
}
################# ****** END LOOP

#agg_stats, matches
rm(all_sponsors, bills, legis_data, SS_bills, elec_year, i, keep_types)
rm(k_matches, klarner_sub, m_sub, km, bill_path, t_yrs, calc_LES, c_sub) # 
rm(t, terms, klarner_gs, commem_bills, t_sessions)


########################################################################################################################################################
############################################################################################################################################################# NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by APPOINTMENT --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
##########################################################################################################################################
# filter(klarner, grepl('chabin', cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(klarner, ddez == 46) %>% select(cand, year, sen, etype, outcome, ddez, candid)

# ~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1996 TERM! ~~~~~~~~~~~~~~ 
# -----> DROPPING 8 bill(s) introduced BY COMMITTEE
# -----> Dropping 1 bill(s) without a sponsor
# IN HOUSE: HANLEY (benjamin)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
# -----> DROPPING 7 bill(s) introduced BY COMMITTEE
# APPOINTED ~ HOUSE: 
# -- GLEASON; STEFFEY (https://votesmart.org/candidate/biography/15229/lela-steffey#.XNsHa-tKjUo)
# IN HOUSE: 
# -- MORTENSEN (paul)
# DROP:
# -- king, ned -- Not on roster for 1st RS -- 

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# IN HOUSE: MAIORANA (mark); CHEUVRONT (ken)
# IN SENATE: MITCHELL (harry e.)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# APPOINTED ~ HOUSE: 
# --- PIERCE (gary -- "twice elected", https://web.archive.org/web/20110902010618/http://www.cc.state.az.us/commissioners/pierce/default.asp)
# APPOINTED ~ SENATE: 
# -- JARRETT M (marilyn)
# -- YRUN (virginia, didn't run again, https://en.wikipedia.org/wiki/Virginia_Yrun)
# KLARNER FIX AT TOP:
# -- osterloh, mark -- coded as winner, actually lost to hellon, toni --> https://apps.azsos.gov/election/2000/General/Canvass2000GE.pdf

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~
# APPOINTED ~ HOUSE: 
# -- AGUIRRE A (amanda)
# -- PREZELSKI
# APPOINTED ~ SENATE: 
# -- CANNELL R (via H)
# -- HALE 
# -- SOLTERO (via H) -- https://www.azleg.gov/senate-member/?legislature=46&legislator=867
# DROP:
# -- guenther, herb -- never sworn in -- seat filled by cannell -- see: https://en.wikipedia.org/wiki/Robert_Cannell 
# -- cannell, robert IN HOUSE
# -- soltero, victor IN HOUSE
# -- valadez, ramon -- County Supervisor since 2003? https://tucson.com/news/local/govt-and-politics/elections/candidate-bio-ram-n-valadez/article_8fe0e4c5-46e4-5f59-9fd4-f541b135500e.html
# -----> Didn't take oath of office? See: https://en.wikipedia.org/wiki/Tom_Prezelski
# NAME FIXES
# jackson, jr // jackson sr. ---> Updated Klarner match names 

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# APPOINTED ~ HOUSE: 
# -- BARTO
# APPOINTED ~ SENATE:
# -- ABOUD
# NAME FIX
# -- weiers j // weiers jp --> Updated Match Names through 2012

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# APPOINTED ~ HOUSE: 
# -- CHABIN (tom) 
# -- WRIGHT (nancy young wright, jan 2008)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# APPOINTED ~ HOUSE: 
# -- TOVAR
# IN HOUSE: 
# -- BROWN (jack)
# -- CAJEROBEDFORD
# DROP: 
# -- gallardo, steve -- resigned after winning, didn't take oath -- https://azcapitoltimes.com/news/2009/01/09/gallardo-wont-take-oath-of-office/

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# APPOINTED ~ HOUSE: 
# -- PIERCE (justin)
# APPOINTED/WON RECALL ~ SENATE: 
# -- BURGES (judy)
# -- LEWIS (jerry, recall)
# -- LUJAN (david)
# IN SENATE: 
# -- MEZA (robert)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# APPOINTED ~ HOUSE: 
# -- CLINCO (demion)
# APPOINTED ~ SENATE: 
# -- BEGAY (carlyle)
# -- DALESSANDRO (andrea, via H)
# -- FARNSWOTH D (david)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# APPOINTED ~ HOUSE: 
# -- PLUMLEE (celeste)
# APPOINTED ~ SENATE: 
# -- SHERWOOD (andrew, via H)
# IN HOUSE: 
# -- CONTRERAS (lupe)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# APPOINTED ~ HOUSE: 
# -- TOMA (ben) -- https://en.wikipedia.org/wiki/Ben_Toma
# APPOINTED ~ SENATE: 
# -- GRAY (rick)


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


### Error Fixes
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_id <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_name <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$sponsor <- "owlett, clint"

### ****Still missing***** 
# ---> Remaining = Not in KLARNER or 2017-2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[3]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, missing, name_sub, still_missing)

### Fix Missing
name_matches <- data.frame(LES_name = "lewis", k_name = 'lewis, jerry')
name_matches <- add_row(name_matches, LES_name = 'gray', k_name = 'gray, rick')
for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}
rm(name_matches, i)


## JUSTIN PIERCE
LES[LES$sponsor %in% "pierce" & LES$term %in% "2011_2012",]$klarner_id <- 308794
LES[LES$sponsor %in% "pierce" & LES$term %in% "2011_2012",]$klarner_name <- "pierce, justin"
LES[LES$sponsor %in% "pierce" & LES$term %in% "2011_2012",]$sponsor <- "pierce, justin"

## GARY PIERCE
LES[LES$sponsor %in% "pierce" & LES$term %in% "2001_2002",]$klarner_id <- 11583
LES[LES$sponsor %in% "pierce" & LES$term %in% "2001_2002",]$klarner_name <- "pierce, gary"
LES[LES$sponsor %in% "pierce" & LES$term %in% "2001_2002",]$sponsor <- "pierce, gary"


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
rm(check_dup, exact, k_sub, missing, name_sub)


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
LES[LES$sponsor == "yrun",]$party <- 'd'
LES[LES$sponsor == "yrun",]$district <- 13
LES[LES$sponsor == "yrun",]$exper <- 'none'
LES[LES$sponsor == "yrun",]$sponsor <- 'yrun, virginia'

LES[LES$sponsor == "plumlee",]$party <- 'd'
LES[LES$sponsor == "plumlee",]$district <- 26
LES[LES$sponsor == "plumlee",]$exper <- 'none'
LES[LES$sponsor == "plumlee",]$sponsor <- 'plumlee, celeste'

### TOMA -- Won't be needed with Klarner Update **********
LES[LES$sponsor == "toma",]$party <- 'r'
LES[LES$sponsor == "toma",]$district <- 22
LES[LES$sponsor == "toma",]$exper <- 'none' # Peoria City Council, but exper = state leg experience
LES[LES$sponsor == "toma",]$sponsor <- 'toma, ben'

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

### If > 2-Year Terms: Expand Senate Rows
# senate <- filter(hf_data, CandId == 'aaaa')
# for(i in 1:nrow(hf_data)){
#   if(hf_data[i,]$chamber == "House") next
#   sen_sub <- filter(hf_data, CandId == hf_data[i,]$CandId)
#   if( !(paste0( hf_data[i,]$year + 3, "_",  hf_data[i,]$year + 4) %in% sen_sub$term) ){
#     new_row <- hf_data[i,]
#     new_row$term <- paste0( hf_data[i,]$year + 3, "_",  hf_data[i,]$year + 4)
#     senate <- bind_rows(senate, new_row)
#   }
# }
# hf_data <- bind_rows(hf_data, senate); rm(senate, new_row, sen_sub)

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

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches: Franklin 'Jake' Flake; Margaret 'Lynn' Pancrazi
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

#### FIX MISMATCHES
# LES[LES$sponsor %in% c('zzzzzzzz'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# filter(LES, is.na(np_score) & !(term %in% c('2017_2018'))) %>% select(sponsor, klarner_name, data_name, term, chamber, np_score) %>% distinct() %>% as.data.frame()# %>% View()
# filter(ideo, grepl('gabal', tolower(name))) %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

name_matches <- data.frame(LES_name = 'jarrett, marilyn', SM_name = 'Marilyn, Jarrett') 
name_matches <- add_row(name_matches, LES_name = 'weiers, james (jim)', SM_name = 'Weiers, James')
name_matches <- add_row(name_matches, LES_name = 'jackson, jack c.', SM_name = 'Jackson, Jack Sr.')
name_matches <- add_row(name_matches, LES_name = 'jackson, jack c. jr.', SM_name = 'Jackson, Jack Jr.')
name_matches <- add_row(name_matches, LES_name = 'barnes, stan', SM_name = 'Barnes')
name_matches <- add_row(name_matches, LES_name = 'landrum, leah', SM_name = 'Landrum Taylor, Leah')
name_matches <- add_row(name_matches, LES_name = 'prezelski, tom', SM_name = 'Prezeliski')
name_matches <- add_row(name_matches, LES_name = 'campbell, cloves c. jr.', SM_name = 'Campbell, Cloves Jr.')
name_matches <- add_row(name_matches, LES_name = 'wright, nancy young', SM_name = 'Young Wright, Nancy')
name_matches <- add_row(name_matches, LES_name = 'hernandez, lydia', SM_name = 'Hernández, Lydia')
name_matches <- add_row(name_matches, LES_name = 'gabaldon, rosanna', SM_name = 'Gabaldón, Rosanna')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

# #### Manual Edits (Needs more precision...)
# LES[LES$sponsor == 'anderson, whitney',]$SM_name <- ideo[ideo$name == 'Anderson' & ideo$senate1999 %in% 1,]$name
# LES[LES$sponsor == 'anderson, whitney',]$SM_party <- ideo[ideo$name == 'Anderson' & ideo$senate1999 %in% 1,]$party
# LES[LES$sponsor == 'anderson, whitney',]$np_score <- ideo[ideo$name == 'Anderson' & ideo$senate1999 %in% 1,]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)



############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################

### REMOVE NUMBERS FROM SPONSOR VAR --- Indicates Identical Names
LES$sponsor <- gsub(' [0-9]$', '', LES$sponsor)

### REMOVE NICKNAMES
LES$sponsor <- gsub('  +', ' ', gsub('\\([^\\)]+\\)', '', LES$sponsor))

### Eliminate Excess White Space
LES$sponsor <- str_trim(LES$sponsor)

### Manual Fixes
LES[LES$sponsor %in% c("landrum, leah"),]$sponsor <- 'taylor, leah landrum'
LES[LES$sponsor %in% c("preble, louann"),]$sponsor <- 'preble, lou-ann'
LES[LES$sponsor %in% c("flake, jake"),]$sponsor <- 'flake, franklin lars' # nickname = Jake
LES[LES$sponsor %in% c("wagner, bill"),]$sponsor <- 'wagner, frederick iii'
LES[LES$sponsor %in% c("farnsworth, eddie"),]$sponsor <- 'farnsworth, edwin w.'
LES[LES$sponsor %in% c("pancrazi, lynne"),]$sponsor <- 'pancrazi, margaret' # nickname = Lynne
LES[LES$sponsor %in% c("mesnard, j. d."),]$sponsor <- 'mesnard, javan d.'
LES[LES$sponsor %in% c("arredondo, p. ben"),]$sponsor <- 'arredondo, paul ben'
LES[LES$sponsor %in% c("contreras, lupe chavira"),]$sponsor <- 'contreras, guadalupe chavira'
#LES[LES$sponsor %in% c("zzzzz"),]$sponsor <- 'zzzzzz'

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1995 - 2020
#LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:1994, 2017:2018) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1995 - 2020
## **** Split Control in 2001_2002 term --> All coded 0
## --> Power-sharing agreement: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx

# LES[as.numeric(substring(LES$term,1,4)) %in% c(2007:2012) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:2000, 2003:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


##############################################
################  Save
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
# filter(LES, party == 'd' & np_score > 0) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & np_score < 0) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### No Variation on Party Control... So Sponsor fixed effects = problem...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

