
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
library(readxl)
library(glue)
library(readr)

this_state <- 'AL'
min_year <- 2000 
max_year <- 2018
keep_types <- c("HB", "SB")
spec_elec_codes <- c('s', 'gs')
house_term_length <- 4 # !!!!!
sen_term_length <- 4 # Staggered? NO

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

terms <- seq(min_year - 1 , max_year, 4) # Term started 1999, but we have data for 2000 - 2002
sessions <- sort(gsub('.+Details_|.csv', '', bill_files))
rm(data_files, bill_files)

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("~/Dropbox/Data/State Legislative Data/Commem Bills/{this_state}_Commem_Bills.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- read.csv("~/Dropbox/Data/State Legislative Data/Significant Bills/SS_Data/SS_Bills.csv")
SS_bills <- SS_bills %>% 
  filter(state == this_state) %>%
  rename(bill_id = bill_num) %>%
  mutate(term = ifelse(year %% 4 == 3, paste0(year, "_", year + 3), 
                       ifelse(year %% 4 == 0, paste0(year - 1, "_", year + 2), 
                              ifelse(year %% 4 == 1, paste0(year - 2, "_", year + 1), paste0(year - 3, "_", year)))),
         bill_id = toupper(bill_id),
         bill_id = ifelse(bill_type == "S" & !grepl("^SB", bill_id), gsub("^S", "SB", bill_id), bill_id),
         bill_id = ifelse(bill_type == "H" & !grepl("^HB", bill_id), gsub("^H", "HB", bill_id), bill_id),
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
klarner[klarner$cand == 'keahy, marc',]$cand <- "keahey, george m. (marc)"
# ----> IDs will still be off, but need to keep them to match to external data...
klarner[klarner$year == 2014 & klarner$cand == "mcclendon, melinda" & klarner$etype == "g",]$outcome <- 'l'
# NOTE: 2014 District 29 Senate Election Wrong -- Melinda Mcclendon lost to Harri Anne Smith
# --- Oddly smith not even recorded in klarner for that race

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[1]

for(t in terms){
  
  ### Formulate 2-year terms -- Cover both regular and special sessions
  t_yrs <- as.character(glue('{t}_{t+3}'))
  t_sessions <- sessions[grepl(paste(seq(t, t+3, 1), collapse = "|"), sessions)]
  
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
  bills$bill_id <- paste0(gsub('[0-9]+', '', bills$bill_id), str_pad(gsub('^[A-Z]+', '', bills$bill_id), 4, pad = "0"))
  bills$term <- t_yrs
  bills$session_year <- str_extract(bills$session, paste(seq(t, t+3, 1), collapse = "|"))
  bills$session <- gsub(' \\d{4}$', '', bills$session)
  bills$session <- recode(bills$session, 'Regular Session' = 'RS', 'First Special Session' = 'SS1', 'Second Special Session' = 'SS2',
                          'Third Special Session' = 'SS3', 'Fourth Special Session' = 'SS4', 'Organizational Session' = 'OS')
  bills$session <- paste(bills$session_year, bills$session, sep = '-')
  bills <- select(bills, -session_year) 
  
  ### Drop duplicates
  bills <- distinct(bills)
  
  ######## Standardize the Bill ID Var
  # bills <- rename(bills, bill_id = bill_number) 
  
  ############### Drop Resolutions, Messages, Communications, Reports
  bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
  # table(bills$bill_type)
  bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)
  
  ### Fix Chamber Var (It's status chamber right now)
  bills$chamber <- substring(bills$bill_id, 1, 1)
  
  ############### Standardize Sponsors
  bills$sponsor <- tolower(bills$sponsor)
  
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
  
  #### DROP Committee Bills
  if(any(grepl('committee', bills$sponsor))){
    print(glue("-----> DROPPING {nrow(filter(bills, grepl('committee', sponsor)))} bill(s) introduced BY COMMITTEE"))
    break
    # bills <- filter(bills, !grepl('committee', LES_sponsor))
  }
  
  ### FIx First Initials
  bills$sponsor <- ifelse(grepl('\\([a-z]\\)$|\\([a-z][a-z]\\)$', bills$sponsor), gsub('\\(|\\)', '', bills$sponsor), bills$sponsor)
  
  #### LES Sponsor Variable --- Format = Last first initial. (needed)
  bills$LES_sponsor <- bills$sponsor
  # sort(table(bills$LES_sponsor))

  ###### CHeck Missing Sponsors
  # filter(bills, LES_sponsor == "") %>% View()
  if(nrow(filter(bills, LES_sponsor == '')) > 0){
    print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
    bills <- filter(bills, !(LES_sponsor == ''))     
  }
  
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
  SS_term <- SS_bills %>% filter(term == t_yrs) %>% mutate(H_max = NA, S_max = NA, s_spec = NA, any_specials = any(grepl(paste0("-SS"), bills$session)))
  
  ## Adjusting Max Specials by Year
  years <- as.numeric(str_split(t_yrs, "_")[[1]])
  for(yr in years[1]:years[2]){
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
  }
  
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
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session'))
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
  bill_hist$action_date <- as.Date(bill_hist$action_date, format = '%m/%d/%Y')
  
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
    filter( !(substring(action_date, 1, 4) %in% unique(sy) ) & !is.na(action_date)) %>%
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
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
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
    summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% as.data.frame() %>% print()
  # filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% filter(!grepl('^by resolution|^first read|^prefiled', tolower(action))) %>% View()
  
  ### MERGE to BILL DATA
  bills <- left_join(bills, all_bill_stages, by = c("bill_id", "term", "session", "LES_sponsor")) 
  
  ### MERGE In COMMEMS
  all_bill_stages <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))

  ### MERGE In S&S
  all_bill_stages <- SS_term %>%
    select(bill_id, term, session, SS) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term', "session")) 
  rm(SS_term)
  
  ### Adjust Commems if SS == 1
  all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
  
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

  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  ### If Term is 2008-2009, e.g., including 2007, 2008, and Specials for 2009 (when present)
  elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1

  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% elec_year:(elec_year + house_term_length - 1) | (year == elec_year + house_term_length & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% elec_year:(elec_year + sen_term_length - 1)  | (year == elec_year + sen_term_length & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)
  
  ############## Match Sponsors Names to Klarner Data, Forgoing Function because mostly only have last name
  
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), '')))
  klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]')))
  
  ### FIX NAMES
  if(t_yrs == "1999_2002"){
    klarner_sub[klarner_sub$cand == "ford, joe",]$match_name <- "ford, joe" # District 28
    all_sponsors[all_sponsors$LES_sponsor == 'ford',]$match_name <- 'ford, joe'
    
    klarner_sub[klarner_sub$cand == "ford, johnny",]$match_name <- "ford, johnny" # District 82 
    all_sponsors[all_sponsors$LES_sponsor == 'ford j',]$match_name <- 'ford, johnny'
  }
  if(t_yrs == "2003_2006"){
    all_sponsors[all_sponsors$LES_sponsor == 'williams j',]$match_name <- 'williams, jack 1' # dist 47
    all_sponsors[all_sponsors$LES_sponsor == 'williams n',]$match_name <- 'williams, nick'
  }
  if(t_yrs == "2015_2018"){ 
    klarner_sub[klarner_sub$cand == "williams, jack 2",]$match_name <- "williams, jw" # District 102
    klarner_sub[klarner_sub$cand == "williams, jack 1",]$match_name <- "williams, jd" # District 47
    
    all_sponsors[all_sponsors$last_name == 'coleman-evans',]$match_name <- 'coleman, m' # dist 47
    all_sponsors[all_sponsors$LES_sponsor == 'coleman-madison',]$match_name <- 'coleman, l'
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
      } else if(length(m_sub) > 1){
        print(glue("MULTIPLE MATCHES ::: {all_sponsors[i,]$LES_sponsor} ::: {i} ::: CHAMBER: {all_sponsors[i,]$chamber}"))
      } else{
        print(glue("MISSING MATCH ::: {all_sponsors[i,]$LES_sponsor} ::: {i} ::: CHAMBER: {all_sponsors[i,]$chamber}"))
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
  if(t_yrs == "2007_2010"){
    ## Phil Williams wins special, Jack Williams in chamber
    all_sponsors[all_sponsors$LES_sponsor == "williams p" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
    ## Joe Hubbard wins special, Mike Hubbard in Chamber
    all_sponsors[all_sponsors$LES_sponsor == "hubbard j" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == '2011_2014'){
    # Mike Holmes wins 1/2014 special, alvin holmes already in chamber
    all_sponsors[all_sponsors$LES_sponsor == "holmes m" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
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
  if(t_yrs == "2007_2010"){
    km <- filter(km, cand != 'hall, albert')
  }else if(t_yrs == "2011_2014"){
    km <- filter(km, cand != 'collier, jack (spencer)')
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
  
  ##############################################################
  ############### Estimate Scores + Add in Relatd Variables
  ##############################################################
  ### Check if bills in data without an ID'd sponsor
  # filter(bills, !(bills$primary_sponsor %in% legis_data$data_name))
  bills <- bills %>% select(-sponsor) %>%
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
  
  #### If LES == 0 and 
  LES[LES$LES == 0 & is.na(LES$num_sponsored_bills), c('num_sponsored_bills', 'sponsor_pass_rate', 'sponsor_law_rate')] <- 0 
  
  ############### Save LES
  write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
  rm(LES)
  
  cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************"))
  
}
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

