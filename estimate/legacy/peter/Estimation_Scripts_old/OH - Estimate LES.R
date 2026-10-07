

################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** OHIO *** BY SESSION
##############################################################

### ****** NEED TO SCRAPE THE NOTES FROM PAGES:
# --- SEE: https://www.lsc.ohio.gov/pages/reference/archives/notes/srl/default.aspx?G=124&T=HB&N=0500
# --- Although worth noting use seems to be rare...
# --- Could code presence of a note as AIC


###################################
## SPECIAL SESSIONS:
## ---- Missing 2015 Special -- Would need to handcode small number of bills
## ---- Seem to be very rare; if occur, folded into main term
## MEMBER LISTS:
## ---- See below; Wikipedia has some good tracking, but obviously, quality may vary.
## PROCESS/RULES:
## ---- 
## Sponsorship/Authorship
## ---- For many years, only have the primary sponsor; cosponsor info is sparse
###########################
## NOTES:
# (1) Extremely limited bill process info even in newer format... Assigned, reported, passed chamber, conference, law
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

this_state <- 'OH'
min_year <- 1997
max_year <- 2018
keep_types <- c("HB", "SB")
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
sessions <- gsub('.+Bill_Details_|.csv', '', bill_files)
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
## Barbara Boyd termed out in 2014; Daughter Janine won her seat
klarner[klarner$cand == 'boyd, barbara' & klarner$year >= 2014,]$cand <- "boyd, janine"
# ----> IDs will still be off, but need to keep them to match to external data...

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[7]

for(t in terms){
  
  ### Formulate 2-year terms -- Cover both regular and special sessions
  t_yrs <- as.character(glue('{t}_{t+1}'))
  
  ### Skip Previously Estimated
  if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
    print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
    next
  }
  
  ###### SESSION IN PROGRESS
  print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))
  
  ############### Read in data
  bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_yrs}.csv")
  bills <- read.csv(bill_path)
  
  ### Clean Term/Session Variables
  bills$term <- t_yrs
  bills <- rename(bills, session = session_num)
  bills <- select(bills, -c(session_year))
  
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
  
  ##########################
  ####### Standardize Sponsors
  
  bills$sponsors <- tolower(bills$sponsors)
  bills$sponsors <- gsub('á', 'a', bills$sponsors)
  bills$sponsors <- gsub('é', 'e', bills$sponsors)
  bills$sponsors <- gsub('ó', 'o', bills$sponsors)
  bills$sponsors <- gsub('í', 'i', bills$sponsors)
  bills$sponsors <- gsub('ñ', 'n', bills$sponsors)

  ### LES Sponsor Var
  bills$LES_sponsor <- gsub(';.+', '', bills$sponsors)
  bills$LES_sponsor <- str_trim(gsub('\\&.+', '', bills$LES_sponsor))
  
  #### Adjusting for Bills By Request -- Need to do BEFORE splitting
  if(any(grepl("request", bills$LES_sponsor))){
    # print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
    cat('\n')
    cat(glue("-----> KEEPING {nrow(filter(bills, grepl('request', LES_sponsor)))} bill(s) introduced BY REQUEST"))
    bills$LES_sponsor <- str_trim(gsub('\\(by request\\)', '', bills$LES_sponsor))
  }

  #### Drop Uncoded Committees
  if(any(grepl('committee', bills$LES_sponsor))){
    cat('\n')
    cat(glue('---> Dropping {sum(grepl("committee", bills$LES_sponsor))} Committed Sponsored Bills (N = {nrow(bills)})'))
    bills <- filter(bills, !grepl('committee', LES_sponsor))
  }

  ### Drop BIlls Proposed by Initiative
  if(any(grepl("initiative", bills$LES_sponsor) )){
    cat('\n'); cat(glue("-----> Dropping {sum(grepl('initiative', bills$LES_sponsor))} Bills Proposed Via Initiative"))
    bills <- filter(bills, !grepl('initiative', LES_sponsor))
  }
  
  ###### CHeck Missing Sponsors
  # filter(bills, LES_sponsor == "") %>% View()
  if(nrow(filter(bills, LES_sponsor == '')) > 0){
    cat('\n')
    cat(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
    bills <- filter(bills, !(LES_sponsor == ''))     
  }

  ###################
  ###### Merge in S&S Bills
  ###################
  # *** For OHIO: All bills folded into main legislative term == Carryover + Numbers do not restart
  # ----> Merge on ID only
  SS_term <- SS_bills %>% 
    filter(term == t_yrs) %>% 
    distinct(term, bill_id, SS)
  
  ### Merge
  bills <- bills %>%
    left_join(SS_term, by = c("bill_id", "term")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  
  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id", "term"))
  
  ####################################
  ############### Code Commemorative
  ####################################
  
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session'))
  # table(bills$commem)
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  ################################################
  ############### Code Bill History
  ####################################
  
  bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_yrs}.csv")
  bill_hist <- read.csv(bill_hist_path)
  
  ######## Standardize BillHist Bill IDs
  bill_hist <- rename(bill_hist, bill_id = bill_number) 
  
  ### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
  bill_hist$term <- t_yrs
  bill_hist <- rename(bill_hist, session = session_num)
  bill_hist <- select(bill_hist, -c(session_year))
  
  ### Fixes for Archive Data + New Data
  if(t <= 2014){
    #### Fix Unformatted Dates
    bill_hist$action_date <- ifelse(!grepl('-', bill_hist$action_date), as.character(as.Date(bill_hist$action_date, format = "%m/%d/%y")), bill_hist$action_date)
    
    ### Rearrange + create order variable that covers both chambers
    bill_hist <- arrange(bill_hist, session, bill_id, action_date) %>%
      group_by(session, bill_id) %>%
      mutate(order = 1:n()) %>%
      ungroup()
    
  }else{
    ### Fix Missing Chambers
    bill_hist[bill_hist$chamber == "" & grepl('^effective', tolower(bill_hist$action)),]$chamber <- "G"
  }
  
  ### Standardize Chamber Variable
  bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate", "G" = "Governor", "CC" = "Conference")
  
  ### Order by Order
  bill_hist <- arrange(bill_hist, session, bill_id, order)
  
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  if(t <= 2014){
    aic_t <- c('^committee report')
    abc_t <- c('^committee report: [a-z]+ and reported', '^committee report: reported', '^floor action')
    pc_t <- c('passed on 3rd consideration', 'conference committee')
    law_t <- c('approved by governor', 'effective date')
    # --> there are a few bills with 'approved by gov' and without 'effective date' but seem to be bills that had varying effective dates and date recorded as '00/00/00' so not recorded
    # --> Others (eg. SB45 in 1997/98) were approved but went to referendum.. hard to parse..
  }else{
    aic_t <- c('^reported')
    abc_t <- c('^reported', 'read on the floor', 'motion to reconsinder', 'recommitted', 're-referred')
    pc_t <- c('^passed', 'received from the house', 'received from the senate') # adopted is only resolutions = dropped
    law_t <- c('^effective')
  }
  
  ### Check Actions
  # filter(bill_hist, grepl('adopted', tolower(action))) %>% distinct(action) %>% View()
  # filter(bill_hist, grepl('effective date', tolower(action))) %>% group_by(action) %>% summarize(n = n()) %>% arrange(desc(n)) %>% View()
  # bill_hist[bill_hist$bill_id == "HF0791",])
  # mutate(bill_hist, clean = gsub('committee~.+', 'committee', gsub("[0-9]+", '', action))) %>% distinct(clean) %>% unlist() %>% unname()
  
  ####################
  ### Output Matrix
  all_bill_stages = tibble(bill_id = character(0),
                           term = character(0),
                           session = integer(0),
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
  # *** Need to ignore chamber switch or else will miscode bills that are passed in senate and transmitted to house on same day
  options(warn = 2)
  for(i in 1:nrow(bills)){
    b_id = bills[i,]$bill_id
    s_id = bills[i,]$session
    b_spon = bills[i,]$LES_sponsor
    hist_sub <- filter(bill_hist, bill_id == b_id, session == s_id)
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t,
                                      ignore_chamber_switch = TRUE)
    bill_stages$bill_url <- bills[i,]$bill_url
    all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
    # print(i)
  }
  options(warn = 1)
  
  ### Check Codings
  cat('\n')
  all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
    summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% as.data.frame() %>% print()
  # filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% View()
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
  
  ######## Cosponsorship Info --- For OH: Only have cosponsor info for most recent years
  all_sponsors$num_cosponsored_bills <- NA
  # bills$cospon_match <- paste(bills$LES_sponsor, bills$primary_sponsors, tolower(bills$cosponsors), sep = '; ')
  # for(i in 1:nrow(all_sponsors)){
  #   c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'A', 'S')  )
  #   all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
  #   ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  #   all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  # }
  # bills <- select(bills, -cospon_match)
  # # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills)) 

  #######################
  #### CLEAN NAMES
  if(t <= 2006){
    all_sponsors$last_name <- gsub(' [a-z]\\.[a-z]\\.$| [a-z]\\.$', '', all_sponsors$LES_sponsor)
    first_middle <- ifelse(grepl('\\.', all_sponsors$LES_sponsor), str_extract(all_sponsors$LES_sponsor, '[a-z]\\.[a-z]\\.$|[a-z]\\.$'), '')
    first_middle <- gsub('\\.', '', str_split_fixed(first_middle, '\\.', 2))
    all_sponsors$first_name <- first_middle[,1]
    all_sponsors$middle_name <- first_middle[,2]
  }else if(t <= 2014){
    all_sponsors$last_name <- gsub(' [a-z]$', '', all_sponsors$LES_sponsor)
    all_sponsors$first_name <- str_trim(ifelse(grepl(' [a-z]$', all_sponsors$LES_sponsor), str_extract(all_sponsors$LES_sponsor, ' [a-z]$'), ''))
  }else{
    parsed_names <- map_df(all_sponsors$LES_sponsor, parse_names) %>% select(-salutation) %>% distinct() 
    parsed_names <- select(parsed_names, last_name, middle_name, first_name, full_name)
    all_sponsors <- left_join(all_sponsors, parsed_names, by = c("LES_sponsor" = "full_name"))
    all_sponsors$middle_name <- gsub('\\.', '', all_sponsors$middle_name)
  }
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  
  #### Update Last Names for Matching 
  if(t_yrs %in% c("2003_2004", "2005_2006")){
    all_sponsors[all_sponsors$LES_sponsor %in% c("conwaykilbane", "conway kilbane"),]$last_name <-  "kilbane"
  }
  if(t_yrs == '2015_2016'){
    all_sponsors[all_sponsors$LES_sponsor == 'christie bryant kuhns',]$last_name <-  "bryant"
  }
  if(t_yrs == '2017_2018'){
    all_sponsors[all_sponsors$LES_sponsor == 'bernadine kennedy kent',]$last_name <-  "kennedykent"
  }

  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  H_elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  S_elec_year <- H_elec_year - 2 ### Staggered so need to get T - 1 and T - 3

  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% H_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% S_elec_year:(S_elec_year + sen_term_length - 1) | (year == S_elec_year + sen_term_length & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz, middle) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)
  
  
  ################################################
  ############## Match Sponsors Names to Klarner Data
  ################################################
  
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), '')))
  if(t <= 2014){
    klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]')))  
  }else{
    klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]+')))
  }
  
  ### Edit Match Name
  if(t_yrs %in% c("2001_2002", "2003_2004")){
    all_sponsors[all_sponsors$LES_sponsor == 'gardner r.a.',]$match_name <- 'gardner, randall'
    all_sponsors[all_sponsors$LES_sponsor == 'gardner r.l.',]$match_name <- 'gardner, robert'
    klarner_sub[klarner_sub$cand == 'gardner, randall',]$match_name <- 'gardner, randall'
    klarner_sub[klarner_sub$cand == 'gardner, robert a.',]$match_name <- 'gardner, robert'    
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
  # **** OHIO NOTE: Only CHecking First INitial Post Match-Name for 2015+ ***
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
      ## Check First Initial -- FOR OH -- ONLY 2015+
      if(t >= 2015 & length(m_sub) == 0){
        match_name2 <- tolower(paste0(all_sponsors[i,]$last_name, ", ", substring(all_sponsors[i,]$first_name, 1, 1) ))
        m_sub <- grep(match_name2, tolower(unlist(str_extract(k_matches$cand, '[a-zA-z]+, [a-zA-z]'))))
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
  if(t_yrs == "1999_2000"){
    all_sponsors[all_sponsors$LES_sponsor == "harris" & all_sponsors$chamber == "S", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }
  if(t_yrs == "2005_2006"){
    all_sponsors[all_sponsors$LES_sponsor == "mcgregor r." & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
    all_sponsors[all_sponsors$LES_sponsor == "white d." & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
    all_sponsors[all_sponsors$LES_sponsor == "miller d." & all_sponsors$chamber == "S", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == '2011_2012'){
    all_sponsors[all_sponsors$LES_sponsor == "hagan c" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
    all_sponsors[all_sponsors$LES_sponsor == "slaby m" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
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
  # km <- filter(km, cand != 'zzzzzzz')
  if(t_yrs == "1997_1998"){
    km <- filter(km, !(cand == 'sweeney, patrick a.' & sen == 0) )
    km <- filter(km, cand != 'kucinich, dennis j.')
  }else if(t_yrs == "1999_2000"){
    km <- filter(km, cand != 'johnson, tom 1')
    km <- filter(km, cand != 'suhadolnik, gary c.')
    km <- filter(km, cand != 'gillmor, karen l.')
  }else if(t_yrs == '2001_2002'){
    km <- filter(km, cand != 'schafrath, richard p.')
    km <- filter(km, cand != 'ray, roy l.')
  }else if(t_yrs == "2003_2004"){
    km <- filter(km, cand != 'mead, priscilla d.')
    km <- filter(km, cand != 'ryan, timothy')
  }else if(t_yrs == '2007_2008'){
    km <- filter(km, !(cand == 'faber, keith' & sen == 0) )
    km <- filter(km, cand != 'jordan, jim')
    km <- filter(km, cand != 'dann, marc')
  }else if(t_yrs == '2009_2010'){
    km <- filter(km, cand != 'mason, lance')
    km <- filter(km, cand != 'boccieri, john')
  }else if(t_yrs == '2011_2012'){
    km <- filter(km, !(cand == 'lehner, peggy' & sen == 0) )
    km <- filter(km, cand != 'zehringer, james')
    km <- filter(km, cand != 'husted, jon')
    km <- filter(km, cand != 'buehrer, stephen')
    km <- filter(km, cand != 'gibbs, bob')
  }else if(t_yrs == '2013_2014'){
    km <- filter(km, cand != 'buehrer, stephen')
    km <- filter(km, cand != 'daniels, david')
  }else if(t_yrs == "2017_2018"){
    km <- filter(km, cand != 'jones, shannon')
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
  
  #############################################################
  ########### Estimate Scores + Add in Relatd Variables
  #############################################################
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
rm(all_sponsors, bills, legis_data, SS_bills, H_elec_year, S_elec_year, i, keep_types)
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES, commem_bills) # 
rm(t, terms, klarner_gs, m_sub, first_middle, parsed_names, match_name2) # 


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by APPOINTMENT by the LEGISLATIVE CHAMBER (Within Party)--- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
# ***************
# -- List of All House Members Over Time by District -- https://en.wikipedia.org/wiki/Representative_history_of_the_Ohio_House_of_Representatives
# -- Link to Similar page for each Senate District (can switch between districts at bottom) - https://en.wikipedia.org/wiki/Ohio%27s_33rd_senatorial_district
# ***************
########################################################################################################################

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1 Bills Proposed Via Initiative
### APPOINTED ~ HOUSE:
# -- EVANS (david r.) -- https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=28142
# -- GOODMAN (david) -- https://en.wikipedia.org/wiki/David_Goodman_(politician)
# -- JOLIVETTE (gregory)
# -- MILLER (dale)
# -- PATTON (sylvester)
# -- SULZER (joseph)
# -- WILLAMOWSK (john)
### APPOINTED ~ SENATE:
# -- HAGAN (robert)
# -- HOTTINGER (jay, via H)
# -- SHOEMAKER (mike)
# -- SWEENEY (patrick)
### IN HOUSE:
# -- DAVIDSON (jo ann)
# -- LEWIS (lloyd, resigned 1/1998)
# -- HAGAN
#### IN SENATE:
# -- FINAN
# -- LONG (jan michael, resigned Feb 1997)
# -- VUKOVICN (joseph, resigned Feb 1997)
#### DROP
# -- sweeney, patrick a. IN HOUSE -- Resigned House prior to swearing in
# -- kucinich, dennis j. ---> Resigned January 1997 after winning US House seat

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE: 
# -- ASLANIDES; DISTEL; GOODING; HOLLISTER
# -- HUGHES; METTLER; PETERSON (jon); REDFERN (chris)
# -- ROBINSON (david)
# -- WIDENER (lost, ran 2 years later in new dist)
### APPOINTED ~ SENATE:
# -- SPADA
# -- HARRIS (bill) -- Switched mid-term --> Duplicated name, won't show
### IN HOUSE:
# -- BOGGS (resigned Feb 17, 1999)
# -- DAVIDSON; PERRY; METELSKY; 
# -- BENDER; LUCAS; LAWRENCE
# -- WESTON (resigned March 19, 1999)
### IN SENATE:
# -- FINAN
### DROP:
# -- johnson, tom 1 -- Resigned 1/5/1999 to direct OMB
# -- suhadolnik, gary c. -- Resigned 1/5/1999 to work for gov.
# -- gillmor, karen l. -- Resigned 12/1997

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- DEBOSE; KOZIURA; MASON (lance); MCGREGOR (jim); WIDOWFIELD
### APPOINTED ~ SENATE:
# -- COUGHLIN (via H, 2/6/2001)
# -- GOODMAN (via H, 10/2/2001)
# -- ROBERTS (tom, via H, 1/9/2002)
### IN HOUSE: 
# -- ALLEN; COUGHLIN (1 month); OTTERMAN; HOUSEHOLDER
### IN SENATE:
# -- FINAN
### DROP:
# -- schafrath, richard p. -- resigned august 15, 2000 
# -- ray, roy l. -- resigned 1/16/2001


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- COMBS (courtney); DEGEETER; MARTIN (earl); SLABY (marilyn)
### APPOINTED ~ SENATE:
# -- DANN; PADGETT; STIVERS; ZURZ
### IN HOUSE:
# -- ALLEN; OTTERMAN; PERRYl; MANNING (resigned March 2003)
# -- PATTON
### IN SENATE:
# -- WHITE; DIDONATO
### DROP:
# -- mead, priscilla d. --> resigned 12/31/2002
# -- ryan, timothy --> resigned 12/19/2002

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- FOLEY (mike)
# -- MCGREGOR; WHITE (dan) --> NEITHER WILL SHOW; Duplicate last names
### APPOINTED ~ SENATE:
# -- KEARNEY (eric)
# -- MILLER (dale) --> won't show, duplicate last name
### IN HOUSE
# -- HUSTED; OTTERMAN; EVANS (david)
### IN SENATE:
# -- HARRIS (bill)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 2 Bills Proposed Via Initiative
### APPOINTED ~ HOUSE:
# -- CIAFARDINI; GARDNER (randall); GERBERYY; GRADY; MECKLENBORG; SEARS; ZEHRINGER
### APPOINTED ~ SENATE:
# -- CAFARO; FABER; SAWYER (tom); SEITZ; TURNER (nina); WAGONER
### IN HOUSE:
# -- BARRETT (resigned 4/12/2008); 
# -- REDFERN 
# -- ASLANIDES
## IN SENATE:
# -- HARRIS
# -- ZURZ (~ month -- resigned 1/28/2007 to work for gov. -- zurz, kimberly a.)
#### DROP
# -- faber, keith -- Appointed to senate, 1/2/2007
# -- jordan, jim -- Elected to Congress, resigned 12/31/2006
# -- dann, marc -- Elected to be Ohio Attorney General, resigned 12/31/2006


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- BELCHER; HOLLINGTON (appointed to same seat back to back)
# -- O'FARRELL; REECE
### APPOINTED ~ SENATE:
# -- JONES (shannon)
# -- SCHIAVONI; STRAHORN; TURNER (nina, past term)
### IN HOUSE:
# -- BUDISH; MANDEL; BLESSING (jr); OTTERMAN; 
# -- SZOLLOSI; OELSLAGER; HALL (dave)
### IN SENATE:
# -- HARRIS
### DROP:
# -- mason, lance -- resigned Sep 16, 2008
# -- boccieri, john -- resigned Dec 31. 2008, elected to Congress


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- BUCHY; BUTLER; CERA; CONDITT; HILL
# -- HOLLINGTON; LYNCH; PELANDA; SPRAGUE; TERHAR
# -- HAGAN (name duplicate, won't show)
# -- SLABY M (marilyn, name duplicate, won't show)
### APPOINTED ~ SENATE:
# -- BALDERSON; BURKE; COLEY; EKLUND; 
# -- GENTILE; LEHNER; OBHOF
### IN HOUSE:
# -- BUDISH; ASHFORD
### DROP:
# -- lehner, peggy -- DROP IN HOUSE; appointed to Senate
# -- zehringer, james -- appointed sec. of agg., resigned 1/3/2011
# -- husted, jon -- elected as OH Sec of State
# -- buehrer, stephen -- appointed as admin for worker's comp
# -- gibbs, bob -- elected to Congress, resigned 12/31/2010


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ House:
# -- SHEEHY
### IN HOUSE:
# -- BOYD; CURTIN; MALLORY (dale)
# -- ASHFORD; SZOLLOSI (resigned 5/2013)
# -- LANDIS
## DROP:
# -- buehrer, stephen; appointed, see above
# -- daniels, david -- resigned April 2012


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~
### APPOINTED ~ House:
# -- MERRIN; BOCCIERI; ARNDT
### IN HOUSE:
# -- OBRIEN (michael)
# -- REINEKE


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ House:
# -- LANG; HOOPS; BROWN (richard); MCCLAIN (riordan)
# -- WILKIN; GALONSKI; SMITH (j. todd)
### APPOINTED ~ Senate:
# -- MCCOLLEY; WILSON
### IN HOUSE: 
# -- CELEBREZZE (nic)
# -- BISHOFF (resigned 5/21/2017)
# -- CONDITT (resigned 9/8/2017)
# -- ZELTWANGER
# -- ROSENBERGER (resigned 4.12.2018)
### DROP:
# --jones, shannon -- resigned 12/31/2016

# filter(klarner, grepl("jones, s", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year)
# filter(klarner, ddez == 31 & sen == 1) %>% select(cand, year, sen, etype, outcome, ddez, candid)


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

### Error Fixes -- Mismatches
LES[LES$data_name %in% "sweeney" & LES$term %in% "1997_1998",]$klarner_id <- 180392
LES[LES$data_name %in% "sweeney" & LES$term %in% "1997_1998",]$klarner_name <- 'sweeney, patrick a.'
LES[LES$data_name %in% "sweeney" & LES$term %in% "1997_1998",]$sponsor <- "sweeney, patrick a."

### ****Still missing***** ---> Rest are not in Klarner or 2017-2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[1]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid) # %>%  View()
# rm(name, still_missing)


### Fix Missing
name_matches <- data.frame(LES_name = 'patton', k_name = 'patton, sylvester d. jr.')
name_matches <- add_row(name_matches, LES_name = 'miller', k_name = 'miller, dale')
name_matches <- add_row(name_matches, LES_name = 'evans', k_name = 'evans, david r.')
name_matches <- add_row(name_matches, LES_name = 'goodman', k_name = 'goodman, david') # Both H and S
name_matches <- add_row(name_matches, LES_name = 'hagan', k_name = 'hagan, robert f.')
name_matches <- add_row(name_matches, LES_name = 'peterson', k_name = 'peterson, jon')
name_matches <- add_row(name_matches, LES_name = 'harris', k_name = 'harris, bill')
name_matches <- add_row(name_matches, LES_name = 'mcgregor', k_name = 'mcgregor, jim')
name_matches <- add_row(name_matches, LES_name = 'mason', k_name = 'mason, lance')
name_matches <- add_row(name_matches, LES_name = 'martin', k_name = 'martin, earl j.')
name_matches <- add_row(name_matches, LES_name = 'slaby', k_name = 'slaby, marilyn')
name_matches <- add_row(name_matches, LES_name = 'gardner', k_name = 'gardner, randall')
name_matches <- add_row(name_matches, LES_name = 'sawyer', k_name = 'sawyer, thomas c.')
name_matches <- add_row(name_matches, LES_name = "o'farrell", k_name = 'ofarrell, joshua')
name_matches <- add_row(name_matches, LES_name = 'grady', k_name = 'grady, colleen')
name_matches <- add_row(name_matches, LES_name = 'jones', k_name = 'jones, shannon')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}
rm(name_matches, i)


## MANUAL FIXES
# LES[LES$sponsor %in% "pierce" & LES$term %in% "2011_2012",]$klarner_id <- 308794
# LES[LES$sponsor %in% "pierce" & LES$term %in% "2011_2012",]$klarner_name <- "pierce, justin"
# LES[LES$sponsor %in% "pierce" & LES$term %in% "2011_2012",]$sponsor <- "pierce, justin"


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
rm(check_dup, k_sub, exact, missing, name_sub, name)


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
fill_missing <- data.frame(LES_name = "robinson", new_name = 'robinson, david j.', party = 'r', district = 27, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "ciafardini", new_name = 'ciafardini, andrew', party = 'r', district = 28, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "belcher", new_name = 'belcher, robin', party = 'd', district = 10, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "hollington", new_name = 'hollington, richard', party = 'r', district = 76, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')
for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

### Note Hollington appointed D76 twice! --> Correcting exper
LES[LES$sponsor == "hollington, richard" & LES$term == "2011_2012",]$exper <- 'pastinc' ## Appointed again after someone else won and resigned prior to office... so kind of inc...


#### *** 2017-2018 *** If any run for reelection, won't be needed once klarner updates 
LES[LES$sponsor == "mcclain, riordan",]$party <- 'r'
LES[LES$sponsor == "lang, george",]$party <- 'r'
LES[LES$sponsor == "brown, richard",]$party <- 'd'
LES[LES$sponsor == "galonski, tavia",]$party <- 'd'
LES[LES$sponsor == "smith, todd",]$party <- 'r'
LES[LES$sponsor == "wilkin, shane",]$party <- 'r'
LES[LES$sponsor == "wilson, steve",]$party <- 'r'

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)
rm(fill_missing, i)

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
LES[LES$sponsor %in% c('williams, bryan c.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# ---> NO IDEO DATA FOR 1995-1996
# filter(LES, is.na(np_score)) %>% filter( !(term %in% c('1995_1996','2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('krau|mett', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'barnes, john e. jr.', SM_name = 'Barnes Jr, John E') 
name_matches <- add_row(name_matches, LES_name = 'blessing, louis w. iii', SM_name = 'Blessing, Louis III')
name_matches <- add_row(name_matches, LES_name = 'blessing, louis w. jr.', SM_name = 'Blessing, Louis Jr.')
## *** Mother/Daughter are collapsed to Daughter row (even though Barbara served much longer)
name_matches <- add_row(name_matches, LES_name = 'boyd, barbara', SM_name = 'Boyd, Janine R.')
name_matches <- add_row(name_matches, LES_name = 'bryant, christie', SM_name = 'Bryant Kuhns, Christie')
# name_matches <- add_row(name_matches, LES_name = 'gooding, robert', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'healy, william j.', SM_name = 'Healy, William')
name_matches <- add_row(name_matches, LES_name = 'healy, william j. ii', SM_name = 'Healy, William II')
name_matches <- add_row(name_matches, LES_name = 'johnson, tom 1', SM_name = 'Johnson, Thomas')
# name_matches <- add_row(name_matches, LES_name = 'kraus, steven w.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'mettler, james', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'miller, ray', SM_name = 'Miller, I. Jr.') ### Matches to I. Ray Miller -- https://ballotpedia.org/Ray_Miller
name_matches <- add_row(name_matches, LES_name = 'mitchell, mike', SM_name = 'Mithcell') # **** SPELL ERROR ****
# name_matches <- add_row(name_matches, LES_name = 'ofarrell, joshua', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'robinson, david j.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'sykes, emilia', SM_name = 'Strong Sykes, Emilia')
name_matches <- add_row(name_matches, LES_name = 'walcher, kathleen', SM_name = 'Reed, Kathleen') ## Walcher REED -- https://www.toledoblade.com/local/politics/2006/10/08/Democrats-set-sights-on-gaining-seats/stories/200610080034

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

#### Manual Edits (Needs more precision...)
LES[LES$sponsor == 'williams, bryan c.',]$SM_name <- ideo[ideo$name == 'Williams' & ideo$house1997 %in% 1,]$name
LES[LES$sponsor == 'williams, bryan c.',]$SM_party <- ideo[ideo$name == 'Williams' & ideo$house1997 %in% 1,]$party
LES[LES$sponsor == 'williams, bryan c.',]$np_score <- ideo[ideo$name == 'Williams' & ideo$house1997 %in% 1,]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1997 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(2009:2010) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2008, 2011:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1997 - 2020
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzz) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

##### Drop Nicknames
# filter(LES, grepl('\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

#### Manual Fixes
LES[LES$sponsor == 'bryant, christie',]$sponsor <- 'kuhns, christie bryant'
LES[LES$sponsor == 'thomas, e. j.',]$sponsor <- 'thomas, edward j.'
LES[LES$sponsor == 'tiberi, pat',]$sponsor <- 'tiberi, patrick'
LES[LES$sponsor == 'lendrum, j. tom',]$sponsor <- 'lendrum, john tom'
LES[LES$sponsor == 'schindel, carolann',]$sponsor <- 'schindel, carol-ann'
#LES[LES$sponsor == 'zzzzz',]$sponsor <- 'zzzzzz'

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
  scale_color_manual(values=c("dodgerblue2", "red2"))

##### CHECK OUTLIERS
# filter(LES, party == 'd' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

