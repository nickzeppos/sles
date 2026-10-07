################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** NORTH DAKOTA *** BY SESSION
##############################################################

###################################
## (SPECIAL) SESSIONS:
## ---- Bills carryover from regular to regular (one biennium)
## ---- For specials: Bill numbers DO NOT REPEAT! == continue from where regular session left off
## MEMBER LISTS:
## ---- Biographies: https://www.legis.nd.gov/biographies
## PROCESS/RULES:
## ---- https://www.legis.nd.gov/research-center/library/legislative-branch-function-and-process
## ------> "Committees are not allowed to hold legislation or kill bills in committee. All bills have a recorded roll call vote in the appropriate chamber."
## ------> ONLY EXCEPTION IS BILLS THAT ARE FORMALLY WITHDRAWN FROM CONSIDERATION
## Sponsorship/Authorship
## ---- Primary sponsor + cosponsors from both chambers
###########################
## NOTES:
##  **** Sessions sometimes start in DECEMBER of election year, hence, e.g., 1998_2000 ****
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

this_state <- 'ND'
min_year <- 1997 
max_year <- 2018
keep_types <- c('HB', "SB")
spec_elec_codes <- c('s', 'gs')
house_term_length <- 4 # STAGGERED HOUSE TERMS STARTING IN 2000
sen_term_length <- 4 # STAGGERED? YES

#### Output Directory
#dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Terms/Sessions and Filespaths
# term_years <- seq(min_year, max_year, 2) 

data_files <- list.files(glue("~/Dropbox/Data/State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
terms <- gsub('.+Bill_Details_|.csv', '', bill_files)
rm(data_files, bill_files)

######## Dropping 2019+ for now
terms <- terms[-which(terms %in% c("66th_2019_2020", "67th_2021_2022"))]

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
# -- Scott Meyer incorrectly listed as Shirley Meyer
klarner[klarner$cand == "meyer, shirley" & klarner$year == 2016,]$candid <- 999999999
klarner[klarner$cand == "meyer, shirley" & klarner$year == 2016,]$cand <- 'meyer, scott'
# ALSO: Amy Kliniske Changes name to Amy Warnke at some point


###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[8]

for(t in terms){
  
  ### Formulate 2-year terms -- Cover both regular and special sessions
  t_yrs <- gsub('^[0-9]+[a-z]+\\_', '', t)
  
  ### Skip Previously Estimated
  if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
    print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
    next
  }
  
  ###### SESSION IN PROGRESS
  print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))
  
  ############### Read in data
  bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t}.csv")
  bills <- read.csv(bill_path)
  
  ### Drop duplicates
  bills <- distinct(bills)
  
  ######## Standardize the Bill IDs
  bills <- rename(bills, bill_id = bill_number)
  
  ############### Drop Resolutions, Messages, Communications, Reports
  bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
  # table(bills$bill_type)
  bills <- filter(bills, bill_type %in% keep_types) 
  
  ############
  ### Fill in Missing Sponsors (e.g., no house sponsors listed on an HB)
  if(t_yrs == "2001_2002"){
    bills[bills$bill_id == "HB1348",]$all_sponsors <- 'Rep. Byerly, Eckre, Hawken; Sen. Fischer, Kilzer, Nething'
    bills[bills$bill_id == "HB1348",]$primary_sponsor <- 'Rep. Byerly'
  }
  
  ##########################
  ####### Standardize Sponsors
  bills$primary_sponsor <- tolower(bills$primary_sponsor)
  bills$primary_sponsor <- gsub('á', 'a', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('é', 'e', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('ó', 'o', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('í', 'i', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('ñ', 'n', bills$primary_sponsor)
  
  bills$chamber_cosponsors <- tolower(gsub('; \\(.+', '', bills$all_sponsors))
  bills$chamber_cosponsors <- ifelse(substring(bills$bill_id,1,1) == "H", gsub('; sen.+', '', bills$chamber_cosponsors),  gsub('; rep.+', '', bills$chamber_cosponsors))
  bills$chamber_cosponsors <- gsub("^rep\\. |^sen\\. ", "", bills$chamber_cosponsors)
  bills$chamber_cosponsors <- gsub('á', 'a', bills$chamber_cosponsors)
  bills$chamber_cosponsors <- gsub('é', 'e', bills$chamber_cosponsors)
  bills$chamber_cosponsors <- gsub('ó', 'o', bills$chamber_cosponsors)
  bills$chamber_cosponsors <- gsub('í', 'i', bills$chamber_cosponsors)
  bills$chamber_cosponsors <- gsub('ñ', 'n', bills$chamber_cosponsors)
  
  #### Manual Fixes for Duplicate Last Names or Missing Initials
  if(t_yrs == "1997_1998"){
    ### Missing First Initial on some cosponsor rows
    bills$chamber_cosponsors <- gsub('^kelsch', 'r.kelsch', bills$chamber_cosponsors)
    bills$chamber_cosponsors <- gsub(', kelsch', ', r.kelsch', bills$chamber_cosponsors)
  }
  
  ### LES Sponsor Var
  bills$LES_sponsor <- gsub("^rep\\. |^sen\\. ", "", bills$primary_sponsor)
  table(bills$LES_sponsor)
  
  ###### Drop Committee Bills
  # filter(bills, LES_sponsor == "" | is.na(LES_sponsor)) %>% View()
  committees <- c("agriculture", "appropriations", "education", "energy and natural resources", "finance and taxation",
                  "government and veterans affairs", "human services", "industry", "judiciary", "legislative management",
                  "natural resources", "political subdivisions", "transportation")
  if( nrow(filter(bills, grepl("committee|comittee|commission|council", LES_sponsor) | LES_sponsor %in% committees)) > 0 ){
    cat('\n')
    cat(glue("-----> Dropping {nrow(filter(bills, grepl('committee|comittee|commission|council', LES_sponsor) | LES_sponsor %in% committees))} bill(s) sponsored by committee"))
    bills <- filter(bills, !( grepl("committee|comittee|commission|council", LES_sponsor) | LES_sponsor %in% committees ))
  }
  
  ###### CHeck Missing Sponsors
  # filter(bills, LES_sponsor == "" | is.na(LES_sponsor)) %>% View()
  if(nrow(filter(bills, LES_sponsor == '')) > 0 | any(is.na(bills$LES_sponsor))){
    cat('\n')
    cat(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == '')) + sum(is.na(bills$LES_sponsor))} bill(s) without a sponsor"))
    bills <- filter(bills, !(LES_sponsor == '' | is.na(LES_sponsor)))     
  }

  ###################
  ###### Merge in S&S Bills
  ###################
  # *** For NORTH DAKOTA: Bills carry over during regular (one biennium) AND numbers DO NOT re-start for special sessions
  # ---> MERGE ON ID ONLY
  
  SS_term <- SS_bills %>% 
    filter(term == t_yrs) %>% 
    mutate(H_max = NA, S_max = NA, s_spec = NA, any_specials = any(grepl(paste0("SS"), bills$session))) %>%
    distinct(term, bill_id, SS)
  
  ### Merge
  bills <- bills %>%
    left_join(SS_term, by = c("bill_id", "term")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  
  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id", "term"))
  
  ###########################################################################
  ############### Code Commemorative
  ###########################################################################
  
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session'))
  # table(bills$commem)

  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  ###########################################################################
  ############### Code Bill History
  ###########################################################################
  
  bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t}.csv")
  bill_hist <- read.csv(bill_hist_path)

  ### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
  bill_hist <- bill_hist %>%
    rename(bill_id = bill_number)
  
  ### Order by Order
  bill_hist <- arrange(bill_hist, term, session, bill_id, order)
  
  ### Fill Missing Chamber Variables (Missing when second action listed in SAME chamber on SAME day)
  bill_hist <- group_by(bill_hist, term, session, bill_id) %>% 
    mutate(chamber = ifelse(str_trim(chamber) == '', NA, chamber)) %>%
    fill(chamber) %>%
    ungroup()

  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  # *** COmmittees are not allowed to hold legislatior or kill bills in commmittee!
  aic_t <- c('committee hearing', 'reported back', 'do pass', 'do not pass', 'divided committee report', 'majority report')
  abc_t <- c('reported back', 'placed on calendar', 'second reading', '^amendment', 'passed', 
             'failed', 'reconsidered', 'rereferred')
  pc_t <- c('second reading, passed')
  law_t <- c('signed by gov', 'filed with secretary of state')
  # --> Need 'filed with' to catch veto overrides
  
  ### Check Actions
  # filter(bill_hist, grepl('rereferred', tolower(action))) %>% distinct(action) %>% View()
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
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
    bill_stages$bill_url <- bills[i,]$bill_url
    # if(bill_stages$law == 0 & bills[i,]$status == "Law"){
    #   bill_stages$passed_chamber <- bill_stages$law <- 1
    # }
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
  all_bill_stages <- SS_term %>% 
    select(bill_id, term, SS) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term')) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  
  ### Adjust Commems if SS == 1
  # table(all_bill_stages$SS, all_bill_stages$commem)
  all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
  rm(SS_term)
  
  ### Save Stage Info **** MERGE WITH SS **********
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
  
  #### Adding Legislators Who COSPONSORED A BILL but did not SPONSOR
  # ** Note: chamber_cosponsors INCLUDES primary sponsor first
  unique_cospon <- str_trim(unique(unlist(str_split(bills$chamber_cosponsors, ', '))))
  unique_cospon <- unique_cospon[!(grepl("committee|council", unique_cospon) | unique_cospon %in% committees)]
  for(nonspon in unique_cospon){
    if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != '' & !is.na(nonspon)){
      chamb <- unique(substring(bills[grepl(nonspon, bills$chamber_cosponsors),]$bill_id, 1, 1))
      if("H" %in% chamb & "S" %in% chamb){
        print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
      }else{
        all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0) 
      }
    }
  }
  
  ######## Cosponsorship Info 
  all_sponsors$num_cosponsored_bills <- NA
  bills$cospon_match <- paste(bills$LES_sponsor, bills$chamber_cosponsors, sep = ', ')
  for(i in 1:nrow(all_sponsors)){
    c_sub <- bills[substring(bills$bill_id, 1, 1) %in% ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S'),]
    sn <- all_sponsors[i,]$LES_sponsor
    ## NEED TO ACCOUNT FOR overlapping NAMES
    search_term <- paste0("^", sn, ',|^', sn, '$|, ', sn, ',|, ', sn, '$')
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(search_term, tolower(c_sub$cospon_match)))
    ### ONLY need to adjust this way if sponsored and cosponsored column are the same
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  }
  bills <- select(bills, -cospon_match)
  # View(select(all_sponsors, LES_sponsor, chamber, num_sponsored_bills, num_cosponsored_bills))
  rm(sn, search_term)
  
  
  #######################
  #### CLEAN NAMES
  ########################
  all_sponsors$first_name <- str_extract(all_sponsors$LES_sponsor, "^[a-z]\\. |^[a-z]\\.")
  all_sponsors$first_name <- ifelse(is.na(all_sponsors$first_name), '', gsub('\\.$', '', str_trim(all_sponsors$first_name)))
  all_sponsors$last_name <- gsub('^[a-z]\\. |^[a-z]\\.', '', all_sponsors$LES_sponsor)
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  
  #### Update First/Last Names for Matching 
  if(t_yrs %in% c("1997_1998", "1999_2000")){
    all_sponsors[all_sponsors$LES_sponsor == 'st. aubyn',]$last_name <-  "saintaubyn"
  }
  if(t_yrs == '2003_2004'){
    all_sponsors[all_sponsors$LES_sponsor == 'nelson' & all_sponsors$chamber == 'S',]$first_name <-  "c"
    all_sponsors[all_sponsors$LES_sponsor == 'warnke',]$last_name <-  "kliniske"
  }
  if(t_yrs == "2005_2006"){
    all_sponsors[all_sponsors$LES_sponsor == 'horter',]$last_name <-  "dahl"
  }
  if(t_yrs == '2015_2016'){
    all_sponsors[all_sponsors$LES_sponsor == 'schreiber beck',]$last_name <-  "beck"
  }
  
  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  # *** STARTING IN 1998 ELECTION: BOTH HOUSE AND SENATE = 4-YEAR TERMS WITH STAGGERED ELECTIONS --> Need T-1 and T-3 ***
  # ---> Account for 2-to-4 year term switch AND Staggered House/Senate Terms 
  # ---> Seems like half the chamber was elected to 4-year vs 2 year terms in 1998 --> need to adjust for this in 2001_2002 (otherwise will print out unmatched 1998 winners)
  if(as.numeric(substring(t_yrs, 1, 4)) < 2000){
    H_elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
    S_elec_year <- H_elec_year - 2 
    
    klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% H_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
    klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% S_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
    klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)); rm(klarner_H, klarner_S)
  } else{
    elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 3
    klarner_sub <- filter(klarner, sab == this_state & (year %in% elec_year:(elec_year + 4 - 1) | (year == elec_year + 4 & etype %in% spec_elec_codes )))
  }

  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz, middle) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)

  ##########################################################################################
  ############## Match Sponsors Names to Klarner Data
  ###########################################################################
  
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), '')))
  klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]')))  
  
  ### Edit Match Name
  if(t_yrs %in% c('2015_2016', '2017_2018')){
    all_sponsors[all_sponsors$LES_sponsor == 'rich s. becker', c("first_name", "last_name", "match_name")] <- list("rich", 'becker', 'becker, rich')
    all_sponsors[all_sponsors$LES_sponsor == 'rick c. becker', c("first_name", "last_name", "match_name")] <- list("rick", 'becker', 'becker, rick')
    klarner_sub[klarner_sub$cand == 'becker, richard s.',]$match_name <- 'becker, rich'
    klarner_sub[klarner_sub$cand == 'becker, rick',]$match_name <- 'becker, rick'
  }
  
  ### Subset out General Specials
  klarner_gs <- filter(klarner_sub, etype == 'gs' & year != as.numeric(substring(t_yrs, 6,9)))
  klarner_sub <- filter(klarner_sub, etype != 'gs')
  
  ### For 2001-2002: DROPPING 1998 winners in districts that had house elections in 2000
  if(t_yrs == "2001_2002"){
    klarner_sub <- klarner_sub %>%
      group_by(sen, ddez) %>% 
      mutate(max_year = max(year)) %>%
      filter(!(sen == 0 & year < max_year)) %>%
      ungroup() %>%
      select(-max_year)    
  }
  
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
    if(nrow(k_matches) == 0 & nrow(klarner_gs) >= 1){
      k_matches <- filter(klarner_gs, last_name == tolower(all_sponsors[i,]$last_name)  & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
      if(nrow(k_matches) == 0){
        k_matches <- filter(klarner_gs, last_name == gsub("-|'| ||`", '', tolower(all_sponsors[i,]$last_name)))
      }
    } else if(nrow(klarner_gs) >= 1){
      gs_matches <- filter(klarner_gs, last_name == tolower(all_sponsors[i,]$last_name)  & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
      if(nrow(gs_matches) > 0){
        if(!(gs_matches$cand %in% k_matches$cand)){
          k_matches <- bind_rows(k_matches, gs_matches)
        }
      }
      rm(gs_matches)
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

  #### CHeck for Duplicates from Matching
  duplicates <- filter(all_sponsors, !is.na(klarner_name)) %>% group_by(chamber, klarner_name) %>% filter(n() > 1)
  if(nrow(duplicates) > 0){
    cat('-----> Check DUPLICATE Sponsors \n ')
    print(select(duplicates, LES_sponsor, klarner_name, chamber) %>% as.data.frame())
  }; rm(duplicates)
  
  ##### CHECK IF ANY KLARNER CANDIDATES MISSING FROM DATA
  km <- filter(klarner_sub, !(candid %in% all_sponsors$klarner_id) )
  km <- filter(km, year != as.numeric(substring(t_yrs, 6, 9)))
  
  #### Drop T-3 Missing IF there was a SPECIAL GENERAL ELECTION held at T-1
  elec_T1 <- as.numeric(substring(t_yrs, 1, 4)) - 1
  klarner_gs <- filter(klarner_gs, year == elec_T1)
  if(nrow(klarner_gs) > 0){
    drop <- c()
    for(i in 1:nrow(km)){
      if(km[i,]$year == elec_T1 - 2 & paste(km[i,]$sen, km[i,]$ddez, sep = '-') %in% paste(klarner_gs$sen, klarner_gs$ddez, sep = "-")){
        drop <- append(drop, i)
      }
    }
    if(length(drop) > 0){
      km <- km[-drop,]
    }
  }

  ### Remove candidates who won but were never seated or resigned half-way through 4-year term
  if(t_yrs == "2007_2008"){
    km <- filter(km, cand != 'trenbeath, thomas l.')
  }else if(t_yrs == '2011_2012'){
    km <- filter(km, cand != 'lindaas, elroy n.')
  } else if(t_yrs == '2013_2014'){
    km <- filter(km, cand != 'christmann, randel')
  } else if(t_yrs == '2015_2016'){
    km <- filter(km, cand != 'rust, david s.')
    km <- filter(km, cand != 'andrist, john')
  } else if(t_yrs == "2017_2018"){
    km <- filter(km, cand != 'wallman, kris')
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
    select(-c(first_name, match_name)) %>% #middle_name, last_name, suffix,
    select(sponsor, data_name, klarner_name, klarner_id, chamber, term, elec_year, num_sponsored_bills, sponsor_pass_rate, sponsor_law_rate, num_cosponsored_bills) %>%
    arrange(chamber, sponsor)
  
  ########################
  ### Estimate Scores + Add in Relatd Variables
  #########################
  
  ### Check if bills in data without an ID'd sponsor
  # filter(bills, !(bills$primary_sponsor %in% legis_data$data_name))
  bills <- bills %>% select(-primary_sponsor) %>%
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

rm(all_sponsors, bills, legis_data, SS_bills, H_elec_year, S_elec_year, i, keep_types)
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES) # 
rm(t, terms, klarner_gs, m_sub, nonspon, unique_cospon, c_sub, elec_T1, elec_year, drop, committees) # 
rm(commem_bills)

########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by POLITICAL PARTY APPOINTMENT --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
### FULL ROSTER: https://www.legis.nd.gov/biographies
#########################


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 236 bill(s) sponsored by committee
#     session chamber   N AIC ABC PASS LAW
# 1 1997_1998       H 356 349 347  219 192
# 2 1997_1998       S 289 285 282  192 159
#### IN HOUSE:
# -- thompson, lynn -- per https://www.legis.nd.gov/biography/lynn-j-thompson
# -- klein, matthew
#### IN SENATE:
# -- bowman, bill

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
#   session chamber   N AIC ABC PASS LAW
# 1 1999_2000       H 332 322 320  171 145
# 2 1999_2000       S 285 279 275  187 150
## ********************************************************************

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 322 bill(s) sponsored by committee
#   session chamber   N AIC ABC PASS LAW
# 1 2001_2002       H 314 308 305  197 170
# 2 2001_2002       S 296 289 288  191 161
### IN HOUSE:
# -- solberg, dorvan
# -- hunskor, bob
# -- gunter, g. jane
# -- bernstein, leroy g.

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 311 bill(s) sponsored by committee
#   session chamber   N AIC ABC PASS LAW
# 1 2003_2004       H 347 339 336  206 165
# 2 2003_2004       S 266 264 264  182 159
### IN HOUSE (for partial term):
# -- wentz, janet -- died sep. 15, 2003 -- https://www.legis.nd.gov/biography/janet-wentz

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 270 bill(s) sponsored by committee
#   session chamber   N AIC ABC PASS LAW
# 1 2005_2006       H 384 377 376  250 209
# 2 2005_2006       S 290 279 279  207 169
### IN HOUSE:
# -- owens, mark
# -- pietsch, vonnie
### NAME FIX:
# --> Stacey HORTER --> Stacey DAHL (which is how it is in klarner)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
#   session chamber   N AIC ABC PASS LAW
# 1 2007_2008       H 413 401 399  254 213
# 2 2007_2008       S 289 286 286  197 153
### APPOINTED ~ SENATE:
# -- OLAFSON (curtis)
### IN HOUSE:
# -- potter, louise (weezie)
### IN SENATE:
# -- pomeroy, jim
### DROP:
# -- trenbeath, thomas l. -- resigned Nov 24, 2006 -- https://www.legis.nd.gov/biography/thomas-l-trenbeath

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
#   session chamber   N AIC ABC PASS LAW
# 1 2009_2010       H 421 412 409  232 191
# 2 2009_2010       S 295 291 291  203 172
### IN HOUSE:
# -- winrich, lonny b.

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
#   session chamber   N AIC ABC PASS LAW
# 1 2011_2012       H 352 343 343  218 171
# 2 2011_2012       S 248 246 246  175 144
### APPOINTED ~ SENATE:
# -- murphy, phil
#### IN HOUSE:
# -- kroeber, joe
#### DROP:
# -- lindaas, elroy n. -- resigned Dec. 1, 2010 -- https://www.legis.nd.gov/biography/elroy-n-lindaas


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 239 bill(s) sponsored by committee
#   session chamber   N AIC ABC PASS LAW
# 1 2013_2014       H 344 335 335  214 170
# 2 2013_2014       S 260 253 252  174 135
### APPOINTED ~ SENATE:
# -- UNRUH (jessica)
### IN HOUSE:
# -- martinson, robert (bob)
### DROP:
# -- christmann, randel -- resigned Nov 21, 2012 -- https://www.legis.nd.gov/biography/randel-christmann


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 236 bill(s) sponsored by committee
#   session chamber   N AIC ABC PASS LAW
# 1 2015_2016       H 364 360 359  214 173
# 2 2015_2016       S 254 254 254  177 130
### APPOINTED ~ HOUSE:
# -- B. ANDERSON (bert, appointed 12/1/2014, matches to multiple incorrect andersons)
### APPOINTED ~ SENATE:
# -- RUST (david, via H, 12/1/2014)
### IN HOUSE:
# -- martinson, robert (bob)
### DROP:
# -- rust, david s. -- IN HOUSE; appointed to Seante
# -- andrist, john -- Resigned Nov 30, 2014

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 245 bill(s) sponsored by committee
#   session chamber   N AIC ABC PASS LAW
# 1 2017_2018       H 309 299 298  193 148
# 2 2017_2018       S 226 222 223  162 132
### APPOINTED ~ HOUSE:
# -- DOBERVICH (gretchen)
### IN HOUSE:
# -- martinson, robert (bob)
### DROP:
# -- wallman, kris -- resigned Oct 6, 2016 -- https://www.legis.nd.gov/biography/kris-wallman

## *** KLARNER ERROR --- MEYER == SCOTT MEYER, NOT SHIRLEY MEYER

# filter(klarner, grepl("dober", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 45 & sen == 1) %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)
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

### FIX MISMATCHES
# LES[LES$data_name %in% "carter" & LES$term == "2016_2019",]$klarner_id <- NA
# LES[LES$data_name %in% "carter" & LES$term == "2016_2019",]$klarner_name <- NA
# LES[LES$data_name %in% "carter" & LES$term == "2016_2019",]$sponsor <- 'carter, joel'

### ****Still missing***** ---> Rest are not in Klarner or 2017-2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[2]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, missing, still_missing)


### Fix Missing
name_matches <- data.frame(LES_name = 'murphy', k_name = 'murphy, phil')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')
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
# fill_missing <- data.frame(LES_name = "zzzzzz", new_name = 'zzzzzz', party = 'zzzzzz', district = zzzzzz, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')

# for(i in 1:nrow(fill_missing)){
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
# }

#### *** 2016_2019 Special winners: If any of below run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "dobervich", c('party', 'sponsor')] <- list('d', "dobervich, gretchen")
# LES[LES$sponsor == "zzzzzzzz", c('party', 'sponsor')] <- c('zzzzz', "zzzzzzz")

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)
# rm(fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

#### Name Change
LES[grepl("kliniske|warnke", LES$sponsor),]$sponsor <- "warnke, amy kliniske"

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

#### This will include new rows for terms where folks didn't hold office but won't be a problem as they won't merge
# --> e.g., if served 2000-2004, this will add a 2005-2006 row; but because they didn't serve that term, won't merge into LES data
new_rows <- filter(hf_data, year == 9999)
for(i in 1:nrow(hf_data)){
  cand_rows <- filter(hf_data, CandId == hf_data[i,]$CandId)
  if(!((hf_data[i,]$year + 2) %in% cand_rows$year)){
    new_row <- hf_data[i,]
    new_row$year <- new_row$year + 2
    new_row$term <- paste0(new_row$year + 1, "_", new_row$year + 2)
    new_rows <- bind_rows(new_rows, new_row)
  }
}

### Subset + COMBINE
hf_data <- bind_rows(hf_data, new_rows)
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
# --> Mick Grosz == Albert "Mick" Grosz, NOT Michael
LES[LES$sponsor %in% c('grosz, mick', 'murphy, paul', 'schneider, mary'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()


### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1991_1992', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('anderson', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

### Lot of the missing are special election winners in 2016+
name_matches <- data.frame(LES_name = 'anderson, howard c. jr.', SM_name = 'Anderson Jr, Howard C')
# name_matches <- add_row(name_matches, LES_name = 'anderson, bert', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'berg, james', SM_name = 'Berg')
name_matches <- add_row(name_matches, LES_name = 'clark, tony', SM_name = 'Clark')
name_matches <- add_row(name_matches, LES_name = 'grosz, mick', SM_name = 'Grosz, Albert') # Albert 'Mick' Grosz
name_matches <- add_row(name_matches, LES_name = 'martinson, robert (bob)', SM_name = 'Martinson, Bob')
name_matches <- add_row(name_matches, LES_name = 'murphy, paul', SM_name = 'Murphy')
name_matches <- add_row(name_matches, LES_name = 'oban, bill', SM_name = 'Oban')
name_matches <- add_row(name_matches, LES_name = 'olson, alice', SM_name = 'Olson')
name_matches <- add_row(name_matches, LES_name = 'pietsch, bill', SM_name = 'Pietsch, William')
name_matches <- add_row(name_matches, LES_name = 'saintaubyn, rod', SM_name = 'St. Aubyn, Rod')
# name_matches <- add_row(name_matches, LES_name = 'schneider, mary', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'stenehjem, allan', SM_name = 'Stenehjem')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

##########
### MORE DETAILED FIXES
##########

### Vernon Thompson --- SENATE
LES[LES$sponsor == 'thompson, vernon',]$SM_name <-  ideo[ideo$name == 'Thompson' & ideo$senate1997 %in% 1,]$name
LES[LES$sponsor == 'thompson, vernon',]$SM_party <- ideo[ideo$name == 'Thompson' & ideo$senate1997 %in% 1,]$party
LES[LES$sponsor == 'thompson, vernon',]$np_score <- ideo[ideo$name == 'Thompson' & ideo$senate1997 %in% 1,]$np_score

#### Lynn Thompson --- HOUSE
LES[LES$sponsor == 'thompson, lynn (jim)',]$SM_name <-  ideo[ideo$name == 'Thompson' & ideo$house1997 %in% 1,]$name
LES[LES$sponsor == 'thompson, lynn (jim)',]$SM_party <- ideo[ideo$name == 'Thompson' & ideo$house1997 %in% 1,]$party
LES[LES$sponsor == 'thompson, lynn (jim)',]$np_score <- ideo[ideo$name == 'Thompson' & ideo$house1997 %in% 1,]$np_score


########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########
# filter(LES, grepl("carmich", sponsor)) %>% select(1:7, party)

### Mike Brandenburg -- Switched D to R on October 2, 1997 -- In first term! -- https://votesmart.org/candidate/biography/11418/michael-don-brandenburg
LES[LES$sponsor == "brandenburg, mike" & LES$term == "1997_1998",]$party <- 'r'

# #### Fredie 'Videt' Carmichael
# LES[LES$sponsor == 'carmichael, videt' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Carmichael, Fredie' & ideo$party == 'D',]$name
# LES[LES$sponsor == 'carmichael, videt' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Carmichael, Fredie' & ideo$party == 'D',]$party
# LES[LES$sponsor == 'carmichael, videt' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Carmichael, Fredie' & ideo$party == 'D',]$np_score
# LES[LES$sponsor == 'carmichael, videt' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Carmichael, Fredie' & ideo$party == 'R',]$name
# LES[LES$sponsor == 'carmichael, videt' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Carmichael, Fredie' & ideo$party == 'R',]$party
# LES[LES$sponsor == 'carmichael, videt' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Carmichael, Fredie' & ideo$party == 'R',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################

##### Drop Nicknames
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

#### Adjust those with Numbers in Name
# *** Other Marvin Nelson pre-our data
LES[LES$sponsor == "nelson, marvin 2",]$sponsor <- 'nelson, marvin e.'

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1997 - 2020
#LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzz) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1996 - 2019
#LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzz) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


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
  scale_color_manual(values=c("dodgerblue2", "red2", 'gray50'))

##### CHECK OUTLIERS 
# filter(LES, party == 'd' & np_score > 0 & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & np_score < 0 & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + in_majority + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

