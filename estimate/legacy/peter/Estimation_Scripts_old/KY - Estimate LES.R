################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** KENTUCKY *** BY SESSION
##############################################################

## **** IN THEORY CAN GET 1990ish+:
# -- Format: https://apps.legislature.ky.gov/Record/94SS/sr.htm   [or 94RS]
# -- https://apps.legislature.ky.gov/Record/94RS/h1.htm   [h2, h3, etc]
# ---> Works for even years? 90RS, 92RS, 94RS, 96RS [no bill pages for 96], 88RS/SS

###################################
## (SPECIAL) SESSIONS:
## ---- Bills do NOT carry over; Special Session bills restart at 1
## MEMBER LISTS:
## ---- Driectories, 2010 - 2019: https://legislature.ky.gov/LRC/Publications/Pages/GA-Directories.aspx
## PROCESS/RULES:
## ---- Process: https://legislature.ky.gov/LRC/Pages/Legislative-Process.aspx
## ---- Glossary: https://legislature.ky.gov/LRC/Pages/Glossary-of-Legislative-Terms.aspx
## ---- House Rules 2019: https://legislature.ky.gov/Legislators/Documents/HouseRules2019.pdf
## Sponsorship/Authorship
## ---- Cosponsors permitted; Primary/Introducing sponsor seemingly listed first,
## ---- BUT per glossary joint sponsors permitted (and theoretically equally responsible)
## ---- If want co-primary, need to scrape the bills by sponsor page -- primary bills have asterisks -- e.g., https://apps.legislature.ky.gov/record/0rs/spon_S.htm#T
###########################
## NOTES:
## (1) Was there a session in 1999??? NO! 
# --- There was an OS, but no RS or SS? https://web.archive.org/web/19990428171603/http://162.114.4.21/record/99OS/record.htm
# --- See also: https://web.archive.org/web/19990117084331/http://www.lrc.state.ky.us/lrcindex.htm
# --- Session rules: http://www.ncsl.org/research/about-state-legislatures/formal-organizational-sessions.aspx
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

this_state <- 'KY'
min_year <- 2001 # Have 2000 bills but no 1999 --> First full term w/ data starts in 2001
max_year <- 2018
keep_types <- c('HB', 'SB')
spec_elec_codes <- c('s', 'gs')
house_term_length <- 2
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
terms <- seq(min_year, max_year, 2)

data_files <- list.files(glue("~/Dropbox/Data/State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
sessions <- gsub(".+Bill_Details_|.csv", '', bill_files)
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

#### Check SS BIll Types ---> Correct to ensure standardized with Data Format
# table(SS_bills$bill_type)

### Load Klarner Data 
load("~/Dropbox/Data/US Election Data/state_leg/klarner_data/196slers1967to2016_20180908.RData")
klarner <- table; rm(table)
klarner$sab <- toupper(klarner$sab)
klarner <- filter(klarner, year > min_year - 5 & sab == this_state)

#### FIX/DROP PROBLEMATIC KLARNER OBSERVATIONS (Doubles, Multi-race candidates, etc.)
# --> C. Lynn BECHLER in wrong order in 2012 --> ID's don't match 2014+
klarner[klarner$cand == 'c. lynn bechler',]$cand <- 'bechler, c. lynn'
# --> Diane Saint Onge Formatting Issues --> IDs will be off
klarner[klarner$cand == 'stonge, diane',]$cand <- 'saintonge, diane'
# --> Melinda Prunty Falsely listed as Marshall Prunty (2012 loser)
klarner[klarner$cand == 'prunty, marshall e.' & klarner$year == 2016,]$cand <- 'prunty, melinda gibbons'
# ******** IDs will still be the same though!

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[9]

for(t in terms){
  
  ### Formulate 2-year terms -- 
  t_yrs <- as.character(glue('{t}_{t+1}'))
  t_sessions <- sessions[grepl(glue('{t}|{t+1}'), sessions)]
  
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
  
  ### Clean Term/Session Variables + Standardize the Bill IDs
  bills <- bills %>%
    rename(bill_id = bill_number) %>%
    mutate(term = t_yrs,
           session = paste(session_year, session_type, sep = "-"),
           bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('^[A-Za-z]+', '', bill_id), 4, pad = "0")))
  
  ### Drop duplicates
  bills <- distinct(bills)
  
  ############### Drop Resolutions, Messages, Communications, Reports
  bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
  # table(bills$bill_type)
  bills <- filter(bills, bill_type %in% keep_types) 
  
  ##########################
  ####### Standardize Sponsors
  
  bills$sponsors <- tolower(bills$sponsors)
  bills$sponsors <- gsub('á', 'a', bills$sponsors)
  bills$sponsors <- gsub('é', 'e', bills$sponsors)
  bills$sponsors <- gsub('ó', 'o', bills$sponsors)
  bills$sponsors <- gsub('í', 'i', bills$sponsors)
  bills$sponsors <- gsub('ñ', 'n', bills$sponsors)
  bills$sponsors <- gsub(' jr\\.', ' jr', bills$sponsors)
  bills$sponsors <- gsub(' sr\\.', ' sr', bills$sponsors)
  bills$sponsors <- gsub(' st\\. ', ' saint ', bills$sponsors)
  
  #### Fix Sponsor Errors
  if(t_yrs == "2001_2002"){
    # Johnny RAY Turner listed as J. Turner instead of Jo. Turner for one bill
    # Note: There is also a Johnnie L. Turner in the House
    bills[substring(bills$bill_id, 1, 1) == 'S',]$sponsors <- gsub('j. turner', 'jo. turner', bills[substring(bills$bill_id, 1, 1) == 'S',]$sponsors)
  }else if(t_yrs == '2005_2006'){
    # For whatever, reason, 'J. Dorsey Ridley' listed as J vs D in 2005 and 2006, respectively
    bills[substring(bills$bill_id, 1, 1) == 'S',]$sponsors <- gsub('j. ridley', 'd. ridley', bills[substring(bills$bill_id, 1, 1) == 'S',]$sponsors)
    ## D. Butler is Denver Butler (doesn't match because also a dwight butler already coded as dw. butler)
    bills[substring(bills$bill_id, 1, 1) == 'H',]$sponsors <- gsub('d. butler', 'de. butler', bills[substring(bills$bill_id, 1, 1) == 'H',]$sponsors)
  }else if(t_yrs == '2007_2008'){
    ## Names different across sessions: Robert Damron = B. Damron (bob); J. Lee = Ji. Lee = Jimmie Lee
    bills[substring(bills$bill_id, 1, 1) == 'H',]$sponsors <- gsub('b. damron', 'r. damron', bills[substring(bills$bill_id, 1, 1) == 'H',]$sponsors)
    bills[substring(bills$bill_id, 1, 1) == 'H',]$sponsors <- gsub('j. lee', 'ji. lee', bills[substring(bills$bill_id, 1, 1) == 'H',]$sponsors)
  }else if(t_yrs == '2013_2014'){
    #### Denver and Dwight Butler both coded as D. Butler... See: https://apps.legislature.ky.gov/record/13rs/sponsors_house.html + https://apps.legislature.ky.gov/record/14rs/sponsors_house.html
    ## Ordered c(primary, cosponsor) in order
    de_2013 <- c(161, 185, 186, 305, 353, 5, 119, 129, 310, 396)
    dw_2013 <- c(253, 272, 355, 368, 10, 127, 141, 224, 279, 285, 405)
    bills[bills$session == '2013-RS' & bills$bill_id %in% paste0('HB', str_pad(de_2013, 4, pad ='0')),]$sponsors <- gsub('d. butler', 'de. butler', bills[bills$session == '2013-RS' & bills$bill_id %in% paste0('HB', str_pad(de_2013, 4, pad ='0')),]$sponsors)
    bills[bills$session == '2013-RS' & bills$bill_id %in% paste0('HB', str_pad(dw_2013, 4, pad ='0')),]$sponsors <- gsub('d. butler', 'dw. butler', bills[bills$session == '2013-RS' & bills$bill_id %in% paste0('HB', str_pad(dw_2013, 4, pad ='0')),]$sponsors)
    bills[bills$session == '2013-RS' & bills$bill_id %in% c("HB0001", "HB0003"),]$sponsors <- gsub('d. butler; d. butler', 'de. butler; dw. butler', bills[bills$session == '2013-RS' & bills$bill_id %in% c("HB0001", "HB0003"),]$sponsors)
            
    de_2014 <- c(69, 99, 153, 251, 319, 364,458, 475, 481, 528, 529, 1, 3, 8, 17, 41, 67, 68, 70, 108, 171, 205, 207, 225, 281, 352, 378, 391, 396, 399, 401, 420, 424)
    dw_2014 <- c(182, 397, 579, 10, 13, 184, 198, 202, 218, 258, 327, 378, 407, 488, 529, 557, 575)
    bills[bills$session == '2014-RS' & bills$bill_id %in% paste0('HB', str_pad(de_2014, 4, pad ='0')),]$sponsors <- gsub('d. butler', 'de. butler', bills[bills$session == '2014-RS' & bills$bill_id %in% paste0('HB', str_pad(de_2014, 4, pad ='0')),]$sponsors)
    bills[bills$session == '2014-RS' & bills$bill_id %in% paste0('HB', str_pad(dw_2014, 4, pad ='0')),]$sponsors <- gsub('d. butler', 'dw. butler', bills[bills$session == '2014-RS' & bills$bill_id %in% paste0('HB', str_pad(dw_2014, 4, pad ='0')),]$sponsors)
    bills[bills$session == '2014-RS' & bills$bill_id == "HB0005",]$sponsors <- gsub('^d. butler', 'de. butler', bills[bills$session == '2014-RS' & bills$bill_id == "HB0005",]$sponsors)
    bills[bills$session == '2014-RS' & bills$bill_id == "HB0005",]$sponsors <- gsub('; d. butler', '; dw. butler', bills[bills$session == '2014-RS' & bills$bill_id == "HB0005",]$sponsors)
    rm(de_2013, de_2014, dw_2013, dw_2014)
  }else if(t_yrs == '2017_2018'){
    # Dan Johnson and DJ Johnson -- Need to adjust Klarner Match Name for DJ down below
    bills[substring(bills$bill_id, 1, 1) == "H",]$sponsors <- gsub('d\\. johnson', 'da. johnson', bills[substring(bills$bill_id, 1, 1) == "H",]$sponsors)
  }
  
  if(t >= 2011 & t<= 2021){
    # Regina Huff == Regina Petrey Bunch -- Listed as Huff from 2011 - 2018, but Bunch in Klarner (and internet sources)
    # --> Seems to mostly go by huff in official records (and her facebook..)
    bills[substring(bills$bill_id, 1, 1) == 'H',]$sponsors <- gsub('r. huff', 'r. bunch huff', bills[substring(bills$bill_id, 1, 1) == 'H',]$sponsors)
  }
  
  ### LES Sponsor Var
  bills$LES_sponsor <- gsub(';.+|,.+', '', bills$sponsors)
  # table(bills$LES_sponsor)
  
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
  # *** For KENTUCKY: Bills DO NOT carry over + Special Session numbers restart
  # --- Adjusting Special Mentions if out of range of observed bills + Assuming bills come from special with most bills 
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
  
  ############################################################
  ############### Code Commemorative
  ############################################################
  
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
  bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
  bill_hist <- read.csv(bill_hist_path)
  
  ### If multiple sessions, read in those as well
  if(length(t_sessions) > 1){
    for(s in t_sessions[2:length(t_sessions)]){
      bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
      s_hist <- read.csv(bill_path)
      bill_hist <- bind_rows(bill_hist, s_hist)
    }
    rm(s, s_hist)
  }

  ### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
  bill_hist <- bill_hist %>%
    rename(bill_id = bill_number) %>%
    mutate(term = t_yrs,
           session = paste(session_year, session_type, sep = "-"),
           bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('^[A-Za-z]+', '', bill_id), 4, pad = "0")))
  
  ### Create + Fill in Chamber Variable:
  bill_hist <- bill_hist %>%
    mutate(chamber = ifelse(order == 1, substring(bill_id, 1,1), NA),
           chamber = ifelse(is.na(chamber) & grepl("in House$|\\(H\\)$|^House|signed by.+House", action), "H", chamber),
           chamber = ifelse(is.na(chamber) & grepl("in Senate$|\\(S\\)$|^Senate|signed by.+Senate", action), "S", chamber),
           chamber = ifelse(is.na(chamber) & grepl("to governor|by governor", tolower(action)), "G", chamber),
           chamber = ifelse(is.na(chamber) & grepl("conference", tolower(action)), "CC", chamber)) %>%
    group_by(term, session, bill_id) %>%
    fill(chamber) %>%
    ungroup()
  
  ### Re-Coding Chamber Variable
  bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate", "G" = "Governor", "CC" = "Conference")
  
  ### Check if any unfavorable reports to figure out how to code
  if(any(grepl('unfav', tolower(bill_hist$action) ))){
    print("------> CHECK UNFAVORABLE BILLS")
    break
  }
  
  ### Adjust "Passed Over" so as not to accidentally code as Passed Chamber == 1
  bill_hist$action <- gsub("^passed over", 'bill passed over', bill_hist$action)
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  aic_t <- c('^reported', 'committee substitute', 'committee amend', 'posted in committee')
  # Favorably/without opinion 
  # CS implies hearing and new version
  # Posted in Committee --> put on a committee meeting agenda: see rule 49 in House 
  abc_t <- c('^reported', 'to calendar', 'to consent calendar', '1st reading', '2nd reading', '3rd reading',
             'posted for passage', 'floor amendment', '^defeated', '^passed', 'recommitted to')
  # 1st reading occurs after bill reported... sent to rules after 2nd, where it can then be recommmitted
  pc_t <- c('^passed', '^3rd reading, passed')
  law_t <- c('^signed by gov', '\\(acts ch\\. [0-9]+\\)', 'secretary of state.+ ch\\. [0-9]+', 'became law without gov')
  
  ### Check Actions
  # filter(bill_hist, grepl('override', tolower(action)) ) %>% distinct(action) %>% View() 
  # filter(bill_hist, grepl('effective date', tolower(action))) %>% group_by(action) %>% summarize(n = n()) %>% arrange(desc(n)) %>% View()
  # View(bill_hist[bill_hist$bill_id == "HB0225",])
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
    if(bill_stages$passed_chamber == 0 & any(grepl('enrolled', hist_sub$action)) ){
      bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
    }
    all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
    # print(i)
  }
  options(warn = 1)
  
  ### Check Codings
  cat('\n')
  all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
    summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% as.data.frame() %>% print()
  # filter(bill_hist, session == '2015-RS' & bill_id %in% all_bill_stages[all_bill_stages$session == '2015-RS' & all_bill_stages$action_in_comm == 0,]$bill_id) %>% View()
  # filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 0,]$bill_id) %>% View()
  
  ### MERGE to BILL DATA
  bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 
  
  ### MERGE In COMMEMS
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
              sponsor_law_rate = sum(law) / n(),
              num_cosponsored_bills = NA) %>%
    ungroup()
  
  #### Adding Legislators Who COSPONSORED A BILL but did not SPONSOR
  unique_cospon <- str_trim(unique(unlist(str_split(bills$sponsors, '; |, '))))
  for(nonspon in unique_cospon){
    if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != '' & !is.na(nonspon)){
      chamb <- unique(substring(bills[grepl(nonspon, bills$sponsors),]$bill_id, 1, 1))
      if("H" %in% chamb & "S" %in% chamb){
        print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
      }else{
        all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0) 
      }
    }
  }
  
  ######## Cosponsorship Info 
  bills$cospon_match <- paste(bills$LES_sponsor, bills$sponsors, sep = '; ')
  bills$cospon_match <- gsub(', ', '; ', bills$cospon_match)
  for(i in 1:nrow(all_sponsors)){
    c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
    sn <- all_sponsors[i,]$LES_sponsor
    sn <- gsub('\\)', '\\\\)', gsub("\\(", '\\\\(', sn))
    search_term <- paste0("^", sn, ';|^', sn, '$|; ', sn, ';|; ', sn, '$')
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(search_term, tolower(c_sub$cospon_match)))
    ### ONLY need to adjust this way if sponsored and cosponsored column are the same
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  }; rm(sn, search_term)
  bills <- select(bills, -cospon_match)
  # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))

  #######################
  #### CLEAN NAMES
  all_sponsors$last_name <- gsub('^[^ ]+ ', '', all_sponsors$LES_sponsor)
  all_sponsors$first_name <- gsub('\\.$', '', str_trim(str_extract(all_sponsors$LES_sponsor, "^[^ ]+ ")))
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  
  #### Update First/Last Names for Matching 
  if(t >= 2011 & t <= 2021){
    all_sponsors[all_sponsors$LES_sponsor == 'r. bunch huff',]$last_name <-  "bunch"
  }
  if(t_yrs == '2017_2018'){
    all_sponsors[all_sponsors$LES_sponsor == 'w. thomas',]$last_name <-  "woodthomas"
    all_sponsors[all_sponsors$LES_sponsor == 'a. scott',]$last_name <-  "woodsonscott"
    all_sponsors[all_sponsors$LES_sponsor == 'k. moser',]$last_name <-  "pooremoser"
  }
  
  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  H_elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  S_elec_year <- H_elec_year - 2 ### Staggered so need to get T - 1 and T - 3
  
  ### For Senate: Sente Election Year to House Year + 1 (so if 2000, 2000-2001; if 1998, 1998 to 2001)
  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% H_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% S_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz, middle) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)
  
  ###########################################################################
  ############## Match Sponsors Names to Klarner Data, Forgoing Function because mostly only have last name
  ###########################################################################
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), '')))
  klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]')))  
  
  ### Edit Match Name
  if(t_yrs == '2017_2018'){
    klarner_sub[klarner_sub$cand == 'johnson, d. j.',]$match_name <- 'johnson, dj'
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
    
    ## Adjust for Jr/Sr/II
    if(nrow(k_matches) == 0 & grepl(" jr| sr| iii| ii", all_sponsors[i,]$last_name)){
      k_matches <- filter(klarner_sub, last_name == gsub(" jr| sr| iii| ii", '', tolower(all_sponsors[i,]$last_name)))
    }  
    
    ## Check GS 
    if(nrow(k_matches) == 0 & nrow(klarner_gs) >= 1){
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
      
      ## Check Last, First Initial
      m_sub <- grep(all_sponsors[i,]$match_name, k_matches$match_name)
      ### Check Without Punctuation --- Can't remove spaces unless do it for all_sponsors and k_matches
      if(length(m_sub) == 0){
        m_sub <- grep(gsub("-|'|`", '', all_sponsors[i,]$match_name), k_matches$match_name)        
      }
      ## Check First Two Letters 
      if(length(m_sub) == 0){
        m_sub <- grep(all_sponsors[i,]$match_name, tolower(unlist(str_extract(k_matches$cand, '[a-zA-z]+, [a-zA-z][a-z]'))))
      }
      ## Check without Jr/Sr/Etc
      if(length(m_sub) == 0 & grepl(" jr| sr| iii| ii", all_sponsors[i,]$last_name)){
        m_sub <- grep(gsub(" jr| sr| iii| ii", '', all_sponsors[i,]$match_name), tolower(unlist(str_extract(k_matches$cand, '[a-zA-z]+, [a-zA-z]'))))
      }  
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
  if(t_yrs == "2011_2012"){
    all_sponsors[all_sponsors$LES_sponsor == "r. bunch huff" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }

  #### CHeck for Duplicates from Matching
  duplicates <- filter(all_sponsors, !is.na(klarner_name)) %>% group_by(chamber, klarner_name) %>% filter(n() > 1)
  if(nrow(duplicates) > 0){
    cat('-----> Check DUPLICATE Sponsors \n ')
    print(select(duplicates, LES_sponsor, klarner_name, chamber) %>% as.data.frame())
  }; rm(duplicates)
  
  ##### CHECK IF ANY KLARNER CANDIDATES MISSING FROM DATA --- Includes all Senaters elected at T-1
  km <- filter(klarner_sub, !(candid %in% all_sponsors$klarner_id) )
  km <- filter(km, sen == 0 | (sen == 1 & year %in% S_elec_year:(as.numeric(substring(t_yrs, 1, 4))) ))
  
  ### Remove candidates who won but were never seated
  if(t_yrs == "2005_2006"){
    km <- filter(km, cand != 'woodward, virginia l.')
    km <- filter(km, cand != 'herron, paul jr.')
  }else if(t_yrs == "2007_2008"){
    km <- filter(km, cand != 'woodward, virginia l.') # Need to drop twice; 4-yr senate term but never seated
  }else if(t_yrs == "2009_2010"){
    km <- filter(km, cand != 'mongiardo, daniel')
    km <- filter(km, cand != 'guthrie, brett')
  }else if(t_yrs == '2013_2014'){
    km <- filter(km, cand != 'williams, david l.')
    km <- filter(km, !(cand == 'gregory, sara beth' & sen == 0) )
  }else if(t_yrs == '2015_2016'){
    km <- filter(km, cand != 'stein, kathy w.')
    km <- filter(km, cand != 'blevins, dr. walter (doc)')
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
  
  ### Add Dana Seum Stephenson -- Seated despite ineligibility but then resigned 1/2006 -- See notes below
  if(t_yrs == '2005_2006'){
    all_sponsors <- add_row(all_sponsors, chamber = 'S', term = t_yrs, klarner_name = 'stephenson, dana seum', klarner_id = 273804, elec_year = 2004)
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


rm(all_sponsors, bills, legis_data, SS_bills, H_elec_year, S_elec_year, i, keep_types)
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES, house_term_length, sen_term_length) # 
rm(t, klarner_gs, m_sub, nonspon, chamb, unique_cospon, c_sub, commem_bills, t_sessions) # 


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by SPECIAL ELECTION --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
### FULL ROSTER: 
#########################


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 62 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- PASLEY (don)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 50 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- MEADE (charles)
### WON SPECIAL ~ SENATE:
# -- RIDLEY (j. dorsey)
# -- THAYER (damon)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 49 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- OSBORNE (david)
# -- WESTON (ron)
### WON SPECIAL ~ SENATE:
# -- RIDLEY (j. dorsey, T-1)
### DROP:
# -- woodward, virginia l. -- Lost, but R opponent didn't meet residency reqs --> court ruled woodward won but R Senate wouldn't seat her: wonhttps://www.upi.com/Top_News/2005/06/02/State-senate-flap-erupts-in-Kentucky/98551117743812/
# -- herron, paul jr. -- died June 2004
### ADD:
# stephenson, dana seum --- was seated instead of woodward, despite inelibility, but forced to resign jan 2006 after court order
# see: https://www.richmondregister.com/news/election/gop-won-t-commit-to-seating-democrat-in-close-kentucky/article_4c35f6b1-7ceb-5446-82d2-373603c7051a.html
# and: http://www.freerepublic.com/focus/f-news/1551747/posts


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 21 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- WEBB-EDGINGTON (alecia)
# -- STUMBO (greg, past H & attorney general, timing odd) -- https://www.ourcampaigns.com/RaceDetail.html?RaceID=418544
# -- OVERLY (sannie)
# -- COURSEY (will)
### WON SPECIAL ~ SENATE:
# -- SMITH (brandon)
# -- CLARK (perry)
### DROP:
# -- woodward, virginia l. -- See 2005_2006


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 36 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- YORK (jill)
# -- MILLS (terry)
### WON SPECIAL ~ SENATE:
# -- SMITH (brandon, T -1)
# -- HIGDON (jimmy, via H)
# -- REYNOLDS (mike)
# -- WEBB (robin, via H)
### DROP:
# -- mongiardo, daniel -- won election to be Lt. Gov in 2007, assumed office in Dec. 2007
# -- guthrie, brett -- won US House seat in 2008 election


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- ROWLAND (bart)
# -- HUFF BUNCH (regina, last name duplicate, won't print) -- https://votesmart.org/candidate/biography/135387/regina-huff

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- KAY (james)
# -- UPCHURCH (ken)
# -- MILES (suzanne)
### WON SPECIAL ~ SENATE:
# -- THOMAS (reginald)
# -- GREGORY (sara beth, via H)
### DROP:
# -- williams, david l. -- resigned Nov 2012 to take judgeship
# -- gregory, sara beth IN HOUSE -- won special for senate, took seat Dec. 2012


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- TACKETT (chuck)
# -- ELLIOTT (daniel)
# -- TAYLOR (joey)
# -- NICHOLLS (lew)
### WON SPECIAL ~ SENATE:
# -- THOMAS (reginald, T-1)
# -- WEST (stephen)
### DROP:
# -- stein, kathy w. -- resigned oct 14, 2013 to take judicial position
# -- blevins, dr. walter (doc) -- resigned january 4, 2015 to take county position


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- BELCHER (linda, pastH, on and off) -- https://www.usatoday.com/story/news/politics/2018/02/20/linda-belcher-defeats-widow-disgraced-kentucky-rep-dan-johnson/357269002/
# -- GOFORTH (robert)



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

#########
#### Name Fixes
######
# --> DENNY == DENVER JR --> Differentiate from Denver Sr
LES[LES$sponsor %in% "butler, denver",]$sponsor <- "butler, denver sr."
LES[LES$sponsor %in% "butler, denver (denny)",]$sponsor <- "butler, denver jr."

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
# still_missing <- unique(LES[is.na(LES$klarner_id),]$sponsor)
# name = still_missing[7]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl('goforth', cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, still_missing)


### Fix Missing
name_matches <- data.frame(LES_name = 'ridley, d', k_name = 'ridley, j. dorsey')
name_matches <- add_row(name_matches, LES_name = 'webb-edgington, a', k_name = 'webbedgington, alecia')
name_matches <- add_row(name_matches, LES_name = 'thomas, r', k_name = 'thomas, reginald')
name_matches <- add_row(name_matches, LES_name = 'belcher, l', k_name = 'belcher, linda howlett')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}
rm(name_matches, i, missing)

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
# fill_missing <- data.frame(LES_name = "jones, wilbert", new_name = 'jones, wilbert l.', party = 'd', district = 82, exper = 'none')
# # fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')
# 
# for(i in 1:nrow(fill_missing)){
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
# }

#### *** 2017_2018 Special winners: If any of below run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "goforth, r",]$district <- 89
LES[LES$sponsor == "goforth, r",]$party <- 'r'
LES[LES$sponsor == "goforth, r",]$sponsor <- 'goforth, robert'


rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)
# rm(fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

#### Fix Regina Bunch Huff
LES[LES$klarner_name %in% 'bunch, regina petrey',]$sponsor <- 'huff, regina bunch'
LES[LES$klarner_name %in% 'denton, julie carman',]$sponsor <- 'denton, julie carman rose'


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
# DROPPING ONE SENATOR ELECTED FOR SHORT TERM, Leads to Imputing Committees when Known
senate <- filter(senate, !(term == "2001_2002" & CandId == 72188))
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

# **********************************
# ** NOTE: NO ESTIMATES FOR LEGISLATORS WHO STARTED IN 2009 OR LATER ******
# ** ----> See, e.g., filter(ideo, house2009 %in% 1)
# *********************************

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
# *** Note: SM collapsed Denver Butler Sr/Jr into one row
LES[LES$sponsor %in% c('adams, julie raque', 'belcher, linda howlett', 'woodsonscott, attica', 'woodthomas, walker'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches:
# -- Joseph 'Jodie' Haydon; Bertram Robert Stivers; Steven Brett Guthrie; Frank Daniel Mongiardo
# -- James Edwin 'Ed' Worley; W. Brad Montell; 
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

## Fix Russ Meyer (!= Joseph Meyer)
LES[LES$sponsor %in% c('meyer, russ'), c('SM_name', 'SM_party', 'np_score')] <- NA


### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(as.numeric(substring(term, 1, 4)) < 2009  & as.numeric(substring(term, 1, 4)) < 2009) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('stephenson', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

name_matches <- data.frame(LES_name = 'adams, richard (dick)', SM_name = 'Adams, Richard')
name_matches <- add_row(name_matches, LES_name = 'harris, ernie', SM_name = 'Harris, Ernest Jr.')
name_matches <- add_row(name_matches, LES_name = 'stacy, john will', SM_name = 'Stacey, John')
#name_matches <- add_row(name_matches, LES_name = 'stephenson, dana seum', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'turner, tommy', SM_name = 'Turner, Thomas')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########
# filter(LES, grepl("seum", sponsor)) %>% select(1:7, party, SM_name, SM_party)
# filter(ideo, grepl('seum', tolower(name))) %>% select(1:5)

### Melvin Henley -- SWITCHES IN 2009 --> DON"T HAVE DEM ESTIMATE
# LES[LES$sponsor == 'henley, melvin b.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Henley, Melvin' & ideo$party == 'D',]$name
# LES[LES$sponsor == 'henley, melvin b.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Henley, Melvin' & ideo$party == 'D',]$party
# LES[LES$sponsor == 'henley, melvin b.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Henley, Melvin' & ideo$party == 'D',]$np_score
# LES[LES$sponsor == 'henley, melvin b.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Henley, Melvin' & ideo$party == 'R',]$name
# LES[LES$sponsor == 'henley, melvin b.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Henley, Melvin' & ideo$party == 'R',]$party
# LES[LES$sponsor == 'henley, melvin b.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Henley, Melvin' & ideo$party == 'R',]$np_score

#### Thomas Robert Kerr --- D to R
LES[LES$sponsor == 'kerr, thomas robert' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Kerr, Thomas' & ideo$party == 'D',]$name
LES[LES$sponsor == 'kerr, thomas robert' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Kerr, Thomas' & ideo$party == 'D',]$party
LES[LES$sponsor == 'kerr, thomas robert' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Kerr, Thomas' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'kerr, thomas robert' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Kerr, Thomas' & ideo$party == 'R',]$name
LES[LES$sponsor == 'kerr, thomas robert' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Kerr, Thomas' & ideo$party == 'R',]$party
LES[LES$sponsor == 'kerr, thomas robert' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Kerr, Thomas' & ideo$party == 'R',]$np_score

#### Dan Seum -- D to R -- Switched in 99 -- Might need to adjust earlier
LES[LES$sponsor == 'seum, dan (malano)' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Seum, Dan' & ideo$party == 'D',]$name
LES[LES$sponsor == 'seum, dan (malano)' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Seum, Dan' & ideo$party == 'D',]$party
LES[LES$sponsor == 'seum, dan (malano)' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Seum, Dan' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'seum, dan (malano)' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Seum, Dan' & ideo$party == 'R',]$name
LES[LES$sponsor == 'seum, dan (malano)' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Seum, Dan' & ideo$party == 'R',]$party
LES[LES$sponsor == 'seum, dan (malano)' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Seum, Dan' & ideo$party == 'R',]$np_score

#### Robert Leeper -- D to R to Other
LES[LES$sponsor == 'leeper, robert j. (bob)' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Leeper, Robert' & ideo$party == 'D',]$name
LES[LES$sponsor == 'leeper, robert j. (bob)' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Leeper, Robert' & ideo$party == 'D',]$party
LES[LES$sponsor == 'leeper, robert j. (bob)' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Leeper, Robert' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'leeper, robert j. (bob)' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Leeper, Robert' & ideo$party == 'R',]$name
LES[LES$sponsor == 'leeper, robert j. (bob)' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Leeper, Robert' & ideo$party == 'R',]$party
LES[LES$sponsor == 'leeper, robert j. (bob)' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Leeper, Robert' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'leeper, robert j. (bob)' & LES$party == 'nonmaj',]$SM_name <-  ideo[ideo$name == 'Leeper, Robert' & ideo$party == 'X',]$name
LES[LES$sponsor == 'leeper, robert j. (bob)' & LES$party == 'nonmaj',]$SM_party <- ideo[ideo$name == 'Leeper, Robert' & ideo$party == 'X',]$party
LES[LES$sponsor == 'leeper, robert j. (bob)' & LES$party == 'nonmaj',]$np_score <- ideo[ideo$name == 'Leeper, Robert' & ideo$party == 'X',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################

##### Drop Nicknames
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)
# KY: IF 1999 -- https://www.democraticunderground.com/discuss/duboard.php?az=view_all&address=104x2821922

LES$in_majority <- 0

### House -- 2000- 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(2000:2016) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2017:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 2000 - 2020 (Dems in power in 90s)
#LES[as.numeric(substring(LES$term,1,4)) %in% c(1992:1999) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2000:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


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
  scale_color_manual(values=c("dodgerblue2",  'gray50', "red2", 'gray50'))

##### CHECK OUTLIERS 
# filter(LES, party == 'd' & np_score > 0 & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & np_score < 0 & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + in_majority + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

