
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

this_state <- 'GA'
min_year <- 2001
max_year <- 2018
keep_types <- c("HB", "SB")
spec_elec_codes <- c('s', 'gs')
sen_term_length <- 2 # Staggered? NA

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

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- read.csv("~/Dropbox/Data/State Legislative Data/Significant Bills/SS_Data/SS_Bills.csv")
SS_bills <- SS_bills %>% 
  filter(state == this_state) %>%
  rename(bill_id = bill_num) %>%
  mutate(term = ifelse(year %% 2 == 1, paste0(year, "_", year + 1), paste0(year - 1, "_", year)),
         bill_id = toupper(bill_id),
         SS = 1) %>%
  select(state, term, year, bill_id, everything())

#### Check SS BIll Types 
# table(SS_bills$bill_type)

### Load Klarner Data 
load("~/Dropbox/Data/US Election Data/state_leg/klarner_data/196slers1967to2016_20180908.RData")
klarner <- table; rm(table)
klarner$sab <- toupper(klarner$sab)
klarner <- filter(klarner, year > min_year - 5 & sab == this_state)

#### FIX/DROP PROBLEMATIC KLARNER OBSERVATIONS (Doubles, Multi-race candidates, etc.)
## ** Ricky Williams incorrectly coded as return to office of roger williams..
klarner[klarner$cand == 'williams, roger' & klarner$year == 2016,]$cand <- "williams, ricky a."
# ----> IDs will be the same for these two... but at least identities sorted out

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[3]

for(t in terms){
  
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
  bills$session_year <- gsub('-', '_', str_extract(bills$session, glue('{t}-{t+1}|{t}|{t+1}')))
  
  bills$session <- str_trim(gsub(glue("{t}-{t+1}|{t}|{t+1}"), '', bills$session))
  bills$session <- recode(bills$session, 'Regular Session' = 'RS', '1st Special Session' = 'SS1', 'Special Session' = 'SS1', 
                          '2nd Special Session' = 'SS2', '3rd Special Session' = 'SS3', '4th Special Session' = 'SS4')
  bills$session <- paste(bills$session_year, bills$session, sep = '-')
  bills <- select(bills, -session_year)
  
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
  bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
  # table(bills$bill_type)
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
  
  #### DROP Committee Bills
  if(any(grepl('committee', bills$sponsors))){
    print(glue("-----> DROPPING {nrow(filter(bills, grepl('committee', sponsor)))} bill(s) introduced BY COMMITTEE"))
    break
    # bills <- filter(bills, !grepl('committee', LES_sponsor))
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
  # table(bills$LES_sponsor)
  
  ## For Cosponsors: removing everything up to first semicolon if multiple sponsors
  bills$cosponsors <- ifelse(grepl(';', bills$sponsors), sub(".+?; ", "", bills$sponsors), '')
  
  ###### CHeck Missing Sponsors
  # filter(bills, LES_sponsor == "") %>% View()
  if(nrow(filter(bills, LES_sponsor == '')) > 0){
    print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
    bills <- filter(bills, !(LES_sponsor == ''))     
  }

  
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
  SS_term <- SS_bills %>%
    filter(term == t_yrs) %>%
    distinct(term, bill_id, SS)
  
  ### Merge
  if(nrow(SS_term) > 0){
    bills <- bills %>% left_join(SS_term, by = c("bill_id", "term")) %>%mutate(SS = ifelse(is.na(SS), 0, SS))
  }else{
    bills$SS <- 0
  }

  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id", "term"))
  
  ############################################################  
  ############### Code Commemorative
  #############################################
  
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session'))
  # table(bills$commem)
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  ############################################################
  ############### Code Bill History
  ############################################################
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
  bill_hist <- select(bill_hist, -session_year) %>%
    arrange(session, bill_id, order)
  
  ### Standardize Chamber Variable
  bill_hist$chamber <- Hmisc::capitalize(str_extract(tolower(bill_hist$action), "^house|^senate"))
  bill_hist$chamber <- ifelse(is.na(bill_hist$chamber), '', bill_hist$chamber )
  # ----> Blanks are mostly executive/law info
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
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
  
  ### Check Codings
  cat('\n')
  all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
    summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% as.data.frame() %>% print()
  # filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% filter(!grepl('^by resolution|^first read|^prefiled', tolower(action))) %>% View()
  
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
      select(bill_id, term, SS) %>%
      left_join(all_bill_stages, ., by = c('bill_id', 'term')) %>%
      mutate(SS = ifelse(is.na(SS), 0, SS))
  }else{
    all_bill_stages$SS <- 0
  }

  ### Adjust Commems if SS == 1 
  all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
  rm(SS_term)
  
  ### Save Stage Info **** MERGE WITH SS **********
  if(!dir.exists(glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}'))){
    dir.create(glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}'))
  }
  write.csv(all_bill_stages, glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv'), row.names = FALSE )
  
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
  
  ##############################################################
  ############## Match Sponsors Names to Klarner Data, Forgoing Function because mostly only have last name
  ##############################################################
  
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), '')))
  klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]+')))
  
  ### Name Fix
  if(t_yrs %in% c("2015_2016", "2017_2018")){
    klarner_sub[klarner_sub$cand == "jones, j. b. (jeff)",]$match_name <- 'jones, j. b.'
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
      k_matches <- filter(klarner_sub, last_name == gsub("-.+", '', tolower(all_sponsors[i,]$last_name)))
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
      ## Check First Initial
      if(length(m_sub) == 0){
        match_name2 <- tolower(paste0(all_sponsors[i,]$last_name, ", ", substring(all_sponsors[i,]$first_name, 1, 1) ))
        m_sub <- grep(match_name2, tolower(unlist(str_extract(k_matches$cand, '[a-zA-z]+, [a-zA-z]'))))
      }
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
  if(t_yrs == "2001_2002"){
    all_sponsors[all_sponsors$LES_sponsor == "williams, roger" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  } else if(t_yrs == "2003_2004"){
    all_sponsors[all_sponsors$LES_sponsor == "jenkins, charles" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA    
  } else if(t_yrs == "2005_2006"){
    all_sponsors[all_sponsors$LES_sponsor == "howard, e., ernestine" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA    
  } else if(t_yrs == "2007_2008"){
    all_sponsors[all_sponsors$LES_sponsor == "maddox, billy" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA        
  }  else if(t_yrs == "2011_2012"){
    all_sponsors[all_sponsors$LES_sponsor == "rogers, terry" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA   
  } else if(t_yrs == "2015_2016"){
    all_sponsors[all_sponsors$LES_sponsor == "bennett, taylor" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA   
    all_sponsors[all_sponsors$LES_sponsor == "carter, doreen" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA   
  } else if(t_yrs == "2017_2018"){
    all_sponsors[all_sponsors$LES_sponsor == "williams, nikema" & all_sponsors$chamber == "S", c("klarner_name", "klarner_id", "elec_year")] <- NA  
  }
  

  #### CHeck for Duplicates from Matching
  duplicates <- filter(all_sponsors, !is.na(klarner_name)) %>% group_by(chamber, klarner_name) %>% filter(n() > 1)
  if(nrow(duplicates) > 0){  # any(duplicated(na.omit(all_sponsors$klarner_name)))
    cat('-----> Check DUPLICATE Sponsors \n ')
    print(select(duplicates, LES_sponsor, klarner_name, chamber) %>% as.data.frame())
  }; rm(duplicates)
  
  ##### CHECK IF ANY KLARNER CANDIDATES MISSING FROM DATA --- Exclude Senators elected T-1
  km <- filter(klarner_sub, !(candid %in% all_sponsors$klarner_id) )
  km <- filter(km, sen == 0 | (sen == 1 & year == as.numeric(substring(t_yrs, 1, 4)) - 1 ))
  all_sponsors <- ungroup(all_sponsors)
  
  ### Remove candidates who won but were never seated
  if(t_yrs == "2007_2008"){
    km <- filter(km, cand != 'lakly, dan')
  }else if(t_yrs == "2011_2012"){
    km <- filter(km, cand != 'sellier, tony')
    km <- filter(km, cand != 'williams, mark')
  } else if(t_yrs == "2013_2014"){
    km <- filter(km, cand != 'jerguson, sean')
    km <- filter(km, cand != 'rogers, chip')
    km <- filter(km, cand != 'stokely, robert')
    km <- filter(km, cand != 'bulloch, john')
  } else if(t_yrs == "2015_2016"){
    km <- filter(km, cand != 'riley, lynne')
    km <- filter(km, cand != 'channell, r. m. (mickey)')
  }else if(t_yrs == "2017_2018"){
    km <- filter(km, cand != 'bethel, charlie')
  }
  
  #### Add Candidates who WON but Sponsored NO BILLS into Data
  if(nrow(km) > 0){
    cat(glue("-----> {as.numeric(substring(t_yrs, 1, 4)) - 1} KLARNER CANDIDATES MISSING MATCHES IN DATA ~~> ADDING INTO LIST OF LEGISLATORS: \n . "))
    print(select(km, year, sen, ddez, etype, cand, candid, partyz) %>% as.data.frame()) 
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
  
  #### If LES == 0 and 
  LES[LES$LES == 0 & is.na(LES$num_sponsored_bills), c('num_sponsored_bills', 'sponsor_pass_rate', 'sponsor_law_rate', "num_cosponsored_bills")] <- 0
  
  ############### Save LES
  write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
  rm(LES)
  
  
  cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************"))
  
}
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

