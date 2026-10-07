################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** UTAH *** BY SESSION
##############################################################


###################################
## (SPECIAL) SESSIONS:
## ---- Sessions distinct from year to year; bills do not carry over; HB/SB numbers start at 1
## ---- Special sessions permitted; included in seperate file; Bill Numbers change with Special Session
## ---------> SS1 = HB/SB1001+; SS2 = HB/SB2001+ and so on through end of BIENNIUM (even if crosses year)
## MEMBER LISTS:
## ---- Senate: 
## ---- House: 
## PROCESS/RULES:
## ---- Process: https://le.utah.gov/lrgc/billtolaw.pdf
## ---- Process: https://www.actionutah.org/how-a-bill-becomes-a-law-in-the-state-of-utah/
## ---- Rules: https://le.utah.gov/documents/legislativerules/legrules.htm
## Sponsorship/Authorship
## ---- Primary Sponsor = Sponsor from initiating chamber
## ---- Floor Sponsor = Sponsor from opposing chamber; assigned prior to introduction in that body
## ------> See: https://le.utah.gov/lrgc/billtolaw.pdf --> must be designated BEFORE bill is transferred
###########################
#### NOTES:
# (1) Coding Line Item Vetos as Equal to Bills that Pass without Line Item Veto
# (2) Bunch of bills from 2005 on, that are in the LRGC, but don't show up as having been introduced
# ---> They correspond tot he bills that are missing from printed status reports: https://le.utah.gov/~2006/status/hstat68.pdf
# ---> But bill page considers them "introduced" (or at least calls the original version the introduced version)
##########################

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

this_state <- 'UT'
min_year <- 1997
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

#### Check SS BIll Types
# table(SS_bills$bill_type)

### Load Klarner Data 
load("~/Dropbox/Data/US Election Data/state_leg/klarner_data/196slers1967to2016_20180908.RData")
klarner <- table; rm(table)
klarner$sab <- toupper(klarner$sab)
klarner <- filter(klarner, year > min_year - 5 & sab == this_state)

#### FIX/DROP PROBLEMATIC KLARNER OBSERVATIONS (Doubles, Multi-race candidates, etc.)
klarner[klarner$cand == 'edward, roger' & klarner$year == 2000,]$cand <- 'barrus, roger e.' # 1 of 7 misrecorded
# ---> IDs WILL BE OFF
klarner[klarner$cand == 'milner, ann' & klarner$year == 2014,]$cand <- 'millner, ann' # 1 of 1 misrecorded, but wins again in

### KLARNER MISSING:
# -- newbold, merlynn t. -- not in 2002 records, but beat Jack Ryser -- https://slco.org/clerk/electionsEK/results/results_arch/2002genelect.html
# -- dee, brad l. -- in 2002, dave vaughn coded as winning D-11; no record of him ever serving, plus no challenger listed

### Klarner Party errors 
# -- Coded As D in 2008 (first elec) but definitely ran as an R (challenged inc R in primary)
klarner[klarner$cand == 'gibson, francis d.' & klarner$year == 2008,]$partyz <- 'r'
# -- Coded As R in 2016 but no record ever switched parties
klarner[klarner$cand == 'poulson, marie h.' & klarner$year == 2016,]$partyz <- 'd'


###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[2]

for(t in terms){
  
  ### Formulate 2-year terms -- Cover both regular and special sessions
  t_yrs <- as.character(glue('{t}_{t+1}'))
  t_sessions <- sessions[grepl(glue('{t}|{t+1}'), sessions)]
  if(t == 2013){
    t_sessions <- t_sessions[-which(t_sessions == "2013_HS1")]
  }
  
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
      if(nrow(s_bills) == 0){ next }
      bills <- bind_rows(bills, s_bills)
    }
    rm(s, s_bills)
  }
  
  #### For 1997 - 2001: DROP ALL BUT FINAL SUBSTITUTE
  ## ---> Initial bill only includes actions up until substitution; final substitute includes all
  ## ---> This isn't a problem for 2002 (in 2001_2002 Term) but method shouldn't impact it since no 'Substitue' Language
  if(t <= 2001){
    options(warn = 2)
    bills <- bills %>%
      mutate(sub_num = ifelse(grepl("Substit", bill_id), str_trim(gsub('.+[0-9]+ ', '', bill_id)), 0),
             sub_num = recode(sub_num, 'Substitute' = '1', 'Second Substitute' = '2', 'Third Substitute' = '3', 'Fourth Substitute' = '4', 'Fifth Substitute' = '5', 'Sixth Substitute' = '6', 'Seventh Substitute' = '7'),
             sub_num = as.numeric(sub_num),
             bill_id = gsub(' [A-Za-z]+ Substit.+| Substit.+', '', bill_id)) %>%
      group_by(session, bill_id) %>%
      filter(sub_num == max(sub_num)) %>%
      ungroup()
    options(warn = 1)
  }
  if(any(grepl("sub", tolower(bills$bill_id)))){ 
    print("CHECK BILL IDS -- SUB FOUND"); break 
  }
  
  ######## Add Term Var + Standardize the Bill IDs
  bills <- bills %>%
    mutate(term = t_yrs,
           bill_id = paste0(gsub('\\.| .+', '', bill_id), str_pad(gsub('.+[A-Z]\\. ', '', bill_id), 4, pad = '0')) ) %>%
    arrange(session, bill_id)
    
  ############### Drop Resolutions, Messages, Communications, Reports
  bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
  # table(bills$bill_type)
  bills <- filter(bills, bill_type %in% keep_types) 
  
  ##########################
  ####### Standardize Sponsors
  
  bills$primary_sponsor <- tolower(bills$primary_sponsor)
  bills$primary_sponsor <- gsub('á', 'a', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('é', 'e', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('ó', 'o', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('í', 'i', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('ñ', 'n', bills$primary_sponsor)

  ### Manual Fixes
  if(t_yrs == '1997_1998'){
    bills[bills$primary_sponsor == 'rep. fox, c.',]$primary_sponsor <- 'rep. fox-finlinson, c.'
  }else if(t_yrs == '2015_2016'){
    bills[bills$bill_id %in% c('SB0073', 'SB0221', 'SB0226', 'SB0254') & bills$session == "2016-RS",]$primary_sponsor <- 'sen. madsen, mark b.'
  }else if(t_yrs == '2017_2018'){
    bills[bills$primary_sponsor == 'rep. thurston, norman k',]$primary_sponsor <- 'rep. thurston, norman k.'
  }
  
  
  ### LES Sponsor Var
  bills$LES_sponsor <- gsub('rep\\. |sen\\. ', '', bills$primary_sponsor)
  # table(bills$LES_sponsor)
  
  ### Need to Standardize the Names in 2001_2002
  # --- In 2002, Full First names are included --> Drop everything but the first initial to match 2001
  if(t_yrs == '2001_2002'){
    bills$LES_sponsor <- str_extract(bills$LES_sponsor, '^[^,]+, [a-z]')
  }
  
  #### Adjusting for Bills By Request -- Need to do BEFORE splitting
  if(any(grepl("request", bills$LES_sponsor))){
    print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
  }
  
  ###### Bills sponsored by committee
  if(nrow(filter(bills, grepl('committee', LES_sponsor))) > 0 ){
    cat('\n')
    cat(glue("-----> Dropping {nrow(filter(bills, grepl('committee', LES_sponsor))) } bill(s) sponsored by committee"))
    bills <- filter(bills, !grepl('committee', LES_sponsor))     
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
  # *** For UTAH: Regular Session Bills Carry Over + Numbers restart BUT Special Session Bill Numbers are Unique
  # ---> Merge on Adjusted Session Variable with Year-RS and SS
  SS_term <- SS_bills %>% 
    filter(term == t_yrs) %>% 
    mutate(session_adj = ifelse(num_only >= 1001, paste0("SS", substring(bill_id, 3, 3)), paste0(year, "-RS") )) %>%
    distinct(bill_id, term, session_adj, SS)
  
  ### Merge
  if(nrow(SS_term) > 0){
    bills <- bills %>%
      mutate(session_adj = ifelse(grepl("SS", session), gsub(".+-", '', session), session)) %>%
      left_join(SS_term, by = c("bill_id", "term", "session_adj")) %>%
      mutate(SS = ifelse(is.na(SS), 0, SS))
  }else{
    bills$SS <- 0
  }
  
  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id", "term", "session_adj"))
  
  ############################################################ 
  ############### Code Commemorative
  ############################################################
  
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session')) %>%
    mutate(commem = coalesce(commem, 0))
  # table(bills$commem)
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  ############################################################
  ############### Code Bill History
  ############################################################
  
  bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
  bill_hist <- read.csv(bill_hist_path)
  
  # If multiple sessions, read in those as well
  if(length(t_sessions) > 1){
    for(s in t_sessions[2:length(t_sessions)]){
      bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
      s_hist <- read.csv(bill_path)
      if(nrow(s_hist) == 0){ next }
      bill_hist <- bind_rows(bill_hist, s_hist)
    }
    rm(s, s_hist)
  }
  
  #### For 1997 - 2001: DROP ALL BUT FINAL SUBSTITUTE = Same as above
  if(t <= 2001){
    options(warn = 2)
    bill_hist <- bill_hist %>%
      mutate(sub_num = ifelse(grepl("Substit", bill_id), str_trim(gsub('.+[0-9]+ ', '', bill_id)), 0),
             sub_num = recode(sub_num, 'Substitute' = '1', 'Second Substitute' = '2', 'Third Substitute' = '3', 'Fourth Substitute' = '4', 'Fifth Substitute' = '5', 'Sixth Substitute' = '6', 'Seventh Substitute' = '7'),
             sub_num = as.numeric(sub_num),
             bill_id = gsub(' [A-Za-z]+ Substit.+| Substit.+', '', bill_id)) %>%
      group_by(session, bill_id) %>%
      filter(sub_num == max(sub_num)) %>%
      ungroup()
    options(warn = 1)
  }
  if(any(grepl("sub", tolower(bill_hist$bill_id)))){ 
    print("CHECK BILL IDS -- SUB FOUND"); break 
  }
  
  ######## Add Term Var + Standardize the Bill IDs
  bill_hist <- bill_hist %>%
    mutate(term = t_yrs,
           bill_id = paste0(gsub('\\.| .+', '', bill_id), str_pad(gsub('.+[A-Z]\\. ', '', bill_id), 4, pad = '0')) ) %>%
    arrange(session, bill_id)
  
  ### Order by Order
  bill_hist <- arrange(bill_hist, term, session, bill_id, action_date, order) 
  
  ### Fill Missing Chamber Variables + Re-Coding Chamber Variable
  bill_hist <- bill_hist %>%
    mutate(chamber = ifelse(chamber == "", NA, chamber),
           chamber = ifelse(is.na(chamber) & (location %in% c("LRGC", "LRGCEN") | grepl('^Legislative Research', location)), "LRGC", chamber),
           chamber = ifelse(is.na(chamber) & order == 1, substring(bill_id,1,1), chamber),
           chamber = ifelse(is.na(chamber) & (location %in% c("HCLERK", "HSEC") | grepl('^House', location)), "House", chamber),
           chamber = ifelse(is.na(chamber) & (location %in% c("SCLERK", "SSEC") | grepl('^House', location)), "Senate", chamber),
           chamber = ifelse(is.na(chamber) & (location %in% c("LTGOV") | grepl("^Executive", location)), "Executive", chamber)
           ) %>%
    mutate(chamber = recode(chamber, "H" = "House", "S" = "Senate", "G" = "Governor", "CC" = "Conference")) %>%
    group_by(session, bill_id) %>%
    fill(chamber) %>%
    ungroup()
  
  ### Get Rid of Slashes in Actions
  bill_hist$action <- gsub('^House \\/|^House\\/', 'House ', bill_hist$action)
  bill_hist$action <- gsub('^Senate \\/|^Senate\\/', 'Senate ', bill_hist$action)
  bill_hist$action <- gsub('  +', ' ', bill_hist$action)
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  # *** FORMAT CHANGES IN 2002 ****
  aic_t <- c('^(house|senate) comm.+(report|rpt)', '^(house|senate) comm.+motion',
             '^(house|senate) comm.+(recommend|favorable|on consent)',
             '^(house|senate) comm.+amendment recommendation')
  abc_t <- c('^(house|senate) comm.+(report|rpt)', 'read 2nd', 'read 3rd', 'placed on (2nd|3rd)', 
             'read 2nd \\& 3rd', '2nd reading', '3rd reading', 'pass (2nd|3rd)', 'passed (2nd|3rd)',
             'placed on.+calendar', 'floor amendment', '^(house|senate) amended',
             '^(house|senate) substitute')
  ## Enacting Clause Struck == Dead, Motion to Reconsider out of order thereafter (Occurs via motion or at end of session)
  pc_t  <- c('^(house|senate) passed 3rd', '^(house|senate) pass 3rd', '^(house|senate) pass 2nd \\& 3rd',
             '^house.+to senate', '^senate.+to house') 
  law_t <- c('governor signed', 'became law w.+ governor signature', 'governor line item veto')
  
  ### Check Actions
  # filter(bill_hist, grepl('unfav', tolower(action))) %>% distinct(action) %>% View()
  # filter(bill_hist, grepl('effective date', tolower(action))) %>% group_by(action) %>% summarize(n = n()) %>% arrange(desc(n)) %>% View()
  # bill_hist[bill_hist$bill_id == "H5549",]
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
  # ---> In 2005_2006: Bunch of bills that are "introduced" but only show up in LRGC, no recorded house actions
  options(warn = 2)
  count <- 0
  for(i in 1:nrow(bills)){
    b_id = bills[i,]$bill_id
    s_id = bills[i,]$session
    b_spon = bills[i,]$LES_sponsor
    hist_sub <- filter(bill_hist, bill_id == b_id, term == t_yrs, session == s_id)
    if( !("House" %in% unique(hist_sub$chamber) | "Senate" %in% unique(hist_sub$chamber)) ){
      all_bill_stages <- add_row(all_bill_stages, bill_id = b_id, term = t_yrs, session = s_id, LES_sponsor = b_spon,
                                 introduced = 1, action_in_comm = 0, action_beyond_comm = 0, passed_chamber = 0, law = 0, bill_url = bills[i,]$bill_url)
      count = count + 1
      next
    }
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t, add_chamb = "LRGC")
    bill_stages$bill_url <- bills[i,]$bill_url
    ##### Check if Passed Without/Over Governor Veto
    if(bill_stages$law == 0 & (!is.na(bills[i,]$chapter_num) | grepl('Governor Signed|Became Law|Governor Line Item Veto', bills[i,]$last_action)) ){
      bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- bill_stages$law <- 1
    }
    #### Check if Passed Chamber
    if(bill_stages$passed_chamber == 0 & any(grepl('to Governor|to Lieutenant Gov|received from.+(House|Senate)', hist_sub$action)) ){
      bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
    }
    ### If Held or Died in Comm, Report Still Sent to Rules... But Effectively Dead
    ### ---> Really hard to know what exactly is happening here... 
    # c_sub <- filter(hist_sub, chamber == ifelse(substring(bill_id,1,1) == "H", "House", "Senate")) %>% mutate(action = tolower(action))
    # if(bill_stages$passed_chamber == 0 & bill_stages$action_beyond_comm == 1 & nrow(c_sub) > 2 & any(grepl("enacting clause struck|strike enacting clause", c_sub$action)) ){
    #   if(any(grepl("^(house|senate) comm.+(report|rpt|return).+to rules$", c_sub[(nrow(c_sub) - 2):nrow(c_sub),]$action))){
    #     bill_stages$action_beyond_comm <- 0
    #   }
    # }; rm(c_sub)
    all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
    # print(i)
  }
  if(count > 0){
    cat('\n')
    print(glue("-----> {count} BILLS DISTRIBUTED BUT NOT FORMALLY INTRODUCED ON FLOOR"))
  }; rm(count)
  options(warn = 1)
  
  ### Check Codings
  cat('\n')
  all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
    summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% as.data.frame() %>% print()
  # filter(bill_hist, session == '2004-RS' & bill_id %in% all_bill_stages[all_bill_stages$session == '2004-RS' & all_bill_stages$action_in_comm == 0,]$bill_id) %>% View()
  # filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 1,]$bill_id) %>% View()
  
  ### MERGE to BILL DATA
  bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 
  
  ### MERGE In COMMEMS
  all_bill_stages <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))
  
  ### MERGE In S&S
  if(nrow(SS_term) > 0){
    all_bill_stages <- mutate(all_bill_stages, session_adj = ifelse(grepl("SS", session), gsub(".+-", '', session), session))
    all_bill_stages <- SS_term %>% 
      select(bill_id, term, session_adj, SS) %>%
      left_join(all_bill_stages, ., by = c('bill_id', 'term', "session_adj")) %>%
      mutate(SS = ifelse(is.na(SS), 0, SS)) %>%
      select(-session_adj)
  }else{
    all_bill_stages$SS <- 0
  }

  
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
  # unique_cospon <- str_trim(unique(unlist(str_split(bills$coauthors, '; '))))
  # for(nonspon in unique_cospon){
  #   if(!(nonspon %in% all_sponsors$chamber_author) & nonspon != ''){
  #     chamb <- #toupper(gsub('\\(|\\)', '', str_extract(nonspon, '\\((h|s)\\)')))
  #     all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber_author = nonspon, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0)
  #   }
  # }

  ######## Cosponsorship Info 
  # bills$cospon_match <- paste(bills$chamber_author, bills$coauthors, sep = "; ")
  # for(i in 1:nrow(all_sponsors)){
  #   c_sub <- bills[substring(bills$bill_id, 1, 1) %in% ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S'),]
  #   sn <- all_sponsors[i,]$chamber_author
  #   ## NEED TO ADD ESCAPE CHARACTERS
  #   sn <- gsub('\\(', '\\\\(', gsub('\\)', '\\\\)', sn))
  #   ## NEED TO ACCOUNT FOR OVERLAPPING NAMES
  #   search_term <- paste0("^", sn, '$|^', sn, ';|; ', sn, '$|; ', sn, ';')
  #   all_sponsors$num_cosponsored_bills[i] <- sum(grepl(search_term, tolower(c_sub$cospon_match)))
  #   ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  #   all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  # }
  # bills <- select(bills, -cospon_match)
  #View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))

  #######################
  #### CLEAN NAMES
  all_sponsors$last_name <- gsub(',.+', '', all_sponsors$LES_sponsor)
  if(t >= 2003){
    all_sponsors$first_name <-  gsub('.+, ', '', all_sponsors$LES_sponsor)
    all_sponsors$first_name <-  gsub(' .+', '', all_sponsors$first_name)
  }else{
    all_sponsors$first_name <- gsub('.+, ', '', all_sponsors$LES_sponsor)
    all_sponsors$first_name <- substring(all_sponsors$first_name, 1, 1)
  }
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  
  #### Update Last Names for Matching 
  if(t_yrs %in% c("1997_1998")){
    all_sponsors[all_sponsors$LES_sponsor == "fox-finlinson, c.",]$last_name <- 'fox'
  }
  if(t >= 2009 & t <= 2014){
    all_sponsors[all_sponsors$LES_sponsor == "escamilla (robles), luz",]$last_name <- 'escamilla'
  }

  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  H_elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  S_elec_year <- H_elec_year - 2 # Staggerred 4-Year Terms
  
  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% H_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% S_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz, middle) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)
  
  ############################################################
  ############## Match Sponsors Names to Klarner Data
  ############################################################
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), '')))
  if(t >= 2003){
    klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]+')))
  }else{
    klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]')))
  }
  
  ### Edit Match Name
  if(t_yrs %in% c("1997_1998", "1999_2000")){
    klarner_sub[klarner_sub$cand == 'johnson, max keele jr.',]$match_name <- 'johnson, k'
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
      k_matches <- filter(klarner_sub, last_name == gsub("-|'| ||`", '', tolower(all_sponsors[i,]$last_name)) & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
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
      ## Check First Initial 
      if(length(m_sub) == 0){
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
    all_sponsors[all_sponsors$LES_sponsor == "snow, m." & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == '2001_2002'){
    all_sponsors[all_sponsors$LES_sponsor == "suazo, a" & all_sponsors$chamber == "S", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == '2007_2008'){
    all_sponsors[all_sponsors$LES_sponsor == "mayne, karen" & all_sponsors$chamber == "S", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == '2013_2014'){
    all_sponsors[all_sponsors$LES_sponsor == "cox, jon" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }
  
  #### Check for Duplicates from Matching
  duplicates <- filter(all_sponsors, !is.na(klarner_name)) %>% group_by(chamber, klarner_name) %>% filter(n() > 1)
  if(nrow(duplicates) > 0){
    cat('-----> Check DUPLICATE Sponsors \n ')
    print(select(duplicates, LES_sponsor, klarner_name, chamber) %>% as.data.frame())
  }; rm(duplicates)
  
  ##### CHECK IF ANY KLARNER CANDIDATES MISSING FROM DATA --- Includes all Senaters elected at T-1
  km <- filter(klarner_sub, !(candid %in% all_sponsors$klarner_id) )
  km <- filter(km, sen == 0 | (sen == 1 & year %in%  S_elec_year:(as.numeric(substring(t_yrs, 1, 4))) ))
  
  ### Remove candidates who won but were never seated
  if(t_yrs == '1997_1998'){
    km <- filter(km, cand != "oscarson, kurt")
  }else if(t_yrs == '1999_2000'){
    km <- filter(km, !(cand == 'valentine, john' & sen == 0))
    km <- filter(km, cand != "peterson, craig")
  }else if(t_yrs == '2003_2004'){
    km <- filter(km, cand != "ryser, jackson")
    km <- filter(km, cand != "vaughn, dave")
    km <- filter(km, cand != "suazo, pete")
  }else if(t_yrs == '2005_2006'){
    km <- filter(km, cand != "styler, michael r.")
    km <- filter(km, cand != "blackham, leonard m.")
    km <- filter(km, cand != "evans, james")
    km <- filter(km, cand != "steele, david h.")
  }else if(t_yrs == '2007_2008'){
    km <- filter(km, cand != "alexander, jeff")
    km <- filter(km, cand != "blackham, leonard m.")
  }else if(t_yrs == '2011_2012'){
    km <- filter(km, cand != 'bigelow, ron')
  }else if(t_yrs == '2013_2014'){
    km <- filter(km, cand != "mcadams, ben")
    km <- filter(km, cand != "romero, ross i.")
    km <- filter(km, cand != "stowell, dennis e.")
  }else if(t_yrs == '2015_2016'){
    km <- filter(km, cand != "cox, spencer j.")
    km <- filter(km, cand != "valentine, john")
  }

  #### Add Candidates who WON but Sponsored NO BILLS into Data
  if(nrow(km) > 0){
    cat(glue("-----> {as.numeric(substring(t_yrs, 1, 4)) - 1} KLARNER CANDIDATES MISSING MATCHES IN DATA ~~> ADDING INTO LIST OF LEGISLATORS: \n ."))
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
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES, match_name2) # 
rm(t, terms, klarner_gs, m_sub, t_sessions, commem_bills)


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by GUBERNATORIAL APPOINTMENT --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
# ---------> Sometimes also holds special elections as well... but method is mostly moot anyway
########################################################################################################################
### Rosters: 
## ---- By Year: https://le.utah.gov/asp/roster/roster.asp
## ---- Alphabetical: https://le.utah.gov/asp/roster/complist.asp
## ---- Wayback: https://web.archive.org/web/20030212164416/http://www.le.state.ut.us/house/members2003/membertable1.asp
#########################
# filter(klarner, grepl("morin", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 124 & sen ==0 & year < 2000) %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- BECK, T. (trish)
# -- PACE, L. (loraine)
### DROP:
# oscarson, kurt -- won, never seated, implied by roster


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- SNOW, M. (marlon) -- Last name duplicated, won't print
### APPOINTED ~ SENATE:
# -- VALENTINE J. (john, via H)
### DROP:
# -- valentine, john -- IN HOUSE -- appointed to Senate after winning reelection in House
# -- peterson, craig -- left office in 1998


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- HUTCHINGS, E (eric)
# -- MASCARO, S (steven)
### APPOINTED ~ SENATE:
# -- SUAZO, A (alicia) -- Name duplicated, won't print
### IN SENATE:
# -- mansell, l. alma


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~
### WON GENERAL BUT MISSING FROM KLARNER?:
# -- dee, brad l. -- no record of a dave vaughn winnin D-11... Dee must have won --- https://web.archive.org/web/20030212164416/http://www.le.state.ut.us/house/members2003/membertable1.asp
# -- newbold, merlynn t. -- https://slco.org/clerk/electionsEK/results/results_arch/2002genelect.html
### APPOINTED ~ HOUSE:
# -- frank, craig a.
# -- webb, r. curt
### IN SENATE:
# -- mansell, l. alma
### DROP:
# -- ryser, jackson -- didn't win
# -- vaughn, dave -- didn't win
# -- suazo, pete -- resigned in 2001


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# ~~~~~> 97 BILLS DISTRIBUTED BUT NOT FORMALLY INTRODUCED ON FLOOR
### APPOINTED ~ HOUSE:
# -- wheeler, richard w. -- https://justfacts.votesmart.org/candidate/biography/50474/richard-wheeler
# -- wiley, larry b.
### APPOINTED ~ SENATE:
# -- goodfellow, brent h. (via H)
# -- mccoy, scott d.
### IN HOUSE:
# -- duckworth, carl w.
# -- mccartney, ty -- resigned january 28, 2005... drop him??? -- https://www.deseret.com/2005/1/27/19874000/mccartney-farewell-turns-into-rocky-roast
### IN SENATE:
# -- valentine, john
### DROP:
# -- styler, michael r. --- resigned after winning reelection
# -- blackham, leonard m. -- left office in 2004
# -- evans, james -- left office in 2004
# -- steele, david h. -- left office in 2003


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# ~~~~~> 155 BILLS DISTRIBUTED BUT NOT FORMALLY INTRODUCED ON FLOOR
### APPOINTED ~ HOUSE:
# -- chavez-houck, rebecca
# -- greenwood, richard a.
# -- herrod, christopher n.
# -- webb, r. curt
# -- winn, bradley a.
### APPOINTED ~ SENATE:
# -- mayne, karen -- lastname duplicated, won't print!
#### DROP:
# -- alexander, jeff -- left office in 2006
# -- blackham, leonard m. -- left office in 2004


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# ~~~~~> 201 BILLS DISTRIBUTED BUT NOT FORMALLY INTRODUCED ON FLOOR
### APPOINTED ~ HOUSE:
# -- anderson, johnny
# -- wright, bill
### APPOINTED ~ SENATE:
# -- adams, j. stuart (via H)
# -- mcadams, benjamin m.
# -- stevenson, jerry w. 


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# ~~~~~> 253 BILLS DISTRIBUTED BUT NOT FORMALLY INTRODUCED ON FLOOR
### APPOINTED ~ HOUSE:
# -- barlow, stewart e.
# -- cox, fred c. -- won special?
# -- doughty, brian
# -- mccay, daniel
# -- richardson, holly j.
# -- snow, v. lowry
### APPOINTED ~ SENATE:
# -- anderson, casey o.
# -- osmond, aaron
# -- weiler, todd
### IN HOUSE:
# -- wheatley, mark
# -- lockhart, becky
### DROP:
# -- bigelow, ron 


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# ~~~~~> 1 BILL DISTRIBUTED BUT NOT FORMALLY INTRODUCED ON FLOOR
### APPOINTED ~ HOUSE:
# -- spendlove, robert m. 
# -- cox, jon -- lastname duplicated, won't print!
### APPOINTED ~ SENATE:
# -- dabakis, jim
### IN HOUSE:
# -- lockhart, becky
### IN SENATE:
# -- niederhauser, wayne
### DROP:
# -- mcadams, ben -- left office in 2012
# -- romero, ross i. -- left office in 2012
# -- stowell, dennis e. -- left office in 2011


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED/WON SPECIAL ~ HOUSE:
# -- cox, jon
# -- hemingway, lynn n. --- left office in 2014, then appointed/won special to come back in 2016
# -- owens, derrin r.
### APPOINTED/WON SPECIAL ~ SENATE:
# -- fillmore, lincoln
# -- jackson, alvin b.
### HOUSE:
# -- hughes, greg
### DROP:
# -- cox, spencer j. -- left office to become lt. gov
# -- valentine, john -- left office in 2014


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED/WON SPECIAL ~ HOUSE:
# -- acton, cheryl k.
# -- robertson, adam 
### APPOINTED/WON SPECIAL ~ SENATE:
# -- zehnder, brian
### IN HOUSE:
# -- stanard, jon -- resigned February 2018. Scandal.


# filter(klarner, grepl("standard", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 11 & sen == 0 & year == 2002) %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)

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

### Error Fixes -- Mismatches
# LES[LES$data_name %in% c('brooks') & LES$term == "2017_2018",]$klarner_id <- NA
# LES[LES$data_name %in% c('brooks') & LES$term == "2017_2018",]$klarner_name <- NA
# LES[LES$data_name %in% c('brooks') & LES$term == "2017_2018",]$sponsor <- 'brooks, michael'

### ****Still missing*****
# still_missing <- unique(LES[is.na(LES$klarner_id),]$sponsor)
# name = still_missing[15]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl('zehnder', cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, still_missing)

### Fix Missing
name_matches <- data.frame(LES_name = 'webb, r.', k_name = 'webb, curt') 
name_matches <- add_row(name_matches, LES_name = 'wheeler, richard', k_name = 'wheeler, rick')
name_matches <- add_row(name_matches, LES_name = 'chavez-houck, rebecca', k_name = 'chavezhouck, rebecca')
name_matches <- add_row(name_matches, LES_name = 'mcadams, benjamin', k_name = 'mcadams, ben')
name_matches <- add_row(name_matches, LES_name = 'mccay, daniel', k_name = 'mccay, dan')
name_matches <- add_row(name_matches, LES_name = 'adams, j.', k_name = 'adams, stuart')
#name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')

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
rm(check_dup, k_sub, exact, name_sub, missing, t, name)


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
# filter(LES, is.na(party)) %>% select(sponsor, term, klarner_id, district, party, exper) %>% as.data.frame()
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

### Not In Klarner
fill_missing <- data.frame(LES_name = "snow, m", new_name = 'snow, marlon o.', party = 'r', district = 58, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "suazo, a", new_name = 'suazo, alicia l.', party = 'd', district = 2, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "doughty, brian", new_name = 'doughty, brian', party = 'd', district = 30, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "richardson, holly", new_name = 'richardson, holly j.', party = 'r', district = 57, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "cox, jon", new_name = 'cox, jon', party = 'r', district = 58, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "jackson, alvin", new_name = 'jackson, alvin b.', party = 'r', district = 14, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "zehnder, brian", new_name = 'zehnder, brian', party = 'r', district = 8, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

#### *** 2017_2018 (and 2015 Senate special winner): If any of below run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "acton, cheryl" & LES$term == '2017_2018', c('party', 'sponsor', 'district')] <- list('r', 'acton, cherly k.', 43)
LES[LES$sponsor == "robertson, adam" & LES$term == '2017_2018', c('party', 'sponsor', 'district')] <- list('r', 'robertson, adam', 63)


rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)
rm(fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()

### Fix Names
LES[LES$sponsor == "chavezhouck, rebecca",]$sponsor <- 'chavez-houck, rebecca'

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

# ********* UTAH IDEO DATA STARTS IN 1993 ***********
# -------> Issues with 2015-2016 records; sometimes same person split into new observation in 2016

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

# LES[LES$sponsor %in% c('zzzzzzzzz'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# Notable Names: M. Susan Lawrence; Richard CURT Webb
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

LES[LES$sponsor %in% c('johnson, max keele jr.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('ervin', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'adams, stuart', SM_name = 'Adams, J.') # J. Stuart Adams
name_matches <- add_row(name_matches, LES_name = 'fox, christine', SM_name = 'Fox-Finlinson, Christine')
### Alvin Jackson: Two Records split across 2015 & 2016
name_matches <- add_row(name_matches, LES_name = 'jackson, alvin b.', SM_name = 'Jackson, Alvin B.')
name_matches <- add_row(name_matches, LES_name = 'johnson, max keele jr.', SM_name = 'Johnson, Keele')
### Nelson Merrill: Two Records, one just 2015, the other 3 years
name_matches <- add_row(name_matches, LES_name = 'nelson, merrill', SM_name = 'Nelson, Merrill')
# name_matches <- add_row(name_matches, LES_name = 'seegmiller, f. jay', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'suazo, alicia l.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'wallis, brent', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

###################
### Additional Matches

# *** Two Records, split across 2015/2016, identical names
LES[LES$sponsor == 'millner, ann',]$SM_name <-  ideo[ideo$name == 'Millner, Ann' & ideo$senate2015 %in% 1,]$name
LES[LES$sponsor == 'millner, ann',]$SM_party <- ideo[ideo$name == 'Millner, Ann' & ideo$senate2015 %in% 1,]$party
LES[LES$sponsor == 'millner, ann',]$np_score <- ideo[ideo$name == 'Millner, Ann' & ideo$senate2015 %in% 1,]$np_score

# *** Two Records, one 2012-2015, the other just 2016
LES[LES$sponsor == 'weiler, todd',]$SM_name <-  ideo[ideo$name == 'Weiler, Todd' & ideo$senate2012 %in% 1,]$name
LES[LES$sponsor == 'weiler, todd',]$SM_party <- ideo[ideo$name == 'Weiler, Todd' & ideo$senate2012 %in% 1,]$party
LES[LES$sponsor == 'weiler, todd',]$np_score <- ideo[ideo$name == 'Weiler, Todd' & ideo$senate2012 %in% 1,]$np_score


########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########
# filter(LES, party == 'd' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)

# ***************** WATKINS = TEMPORARY FIX ****************************

#### *** Christine Watkins, Dem from 2009-2012, ran again in 2016 as Rep. BUT don't have NP Score for more recent term
LES[LES$sponsor == 'watkins, christine f.' & LES$term == "2017_2018",]$np_score <- NA
# https://le.utah.gov/asp/roster/complist.asp?letter=W

# *** Zzzzzzzzzzz
# LES[LES$sponsor == 'ervin, mike' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Ervin, Mike' & ideo$party == 'R',]$name
# LES[LES$sponsor == 'ervin, mike' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Ervin, Mike' & ideo$party == 'R',]$party
# LES[LES$sponsor == 'ervin, mike' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Ervin, Mike' & ideo$party == 'R',]$np_score
# LES[LES$sponsor == 'ervin, mike' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Ervin' & ideo$party == 'D',]$name
# LES[LES$sponsor == 'ervin, mike' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Ervin' & ideo$party == 'D',]$party
# LES[LES$sponsor == 'ervin, mike' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Ervin' & ideo$party == 'D',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################

##### Drop Nicknames
# filter(LES, grepl('\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]$', sponsor)) %>% distinct(sponsor)
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

#### Name Fixes
LES[LES$sponsor == 'fox, christine',]$sponsor <- 'fox-finlinson, christine'
LES[LES$sponsor == 'ure, r. david',]$sponsor <- 'ure, raymond david'
LES[LES$sponsor == 'philpot, morgon',]$sponsor <- 'philpot, jay morgan'
LES[LES$sponsor == 'wallace, peggy',]$sponsor <- 'wallace, margaret ann'
LES[LES$sponsor == 'mcgee, roz',]$sponsor <- 'mcgee, rosalind'
LES[LES$sponsor == 'webb, curt',]$sponsor <- 'webb, richard curt'
LES[LES$sponsor == 'edwards, becky',]$sponsor <- 'edwards, rebecca p.'
LES[LES$sponsor == 'duckworth, sue',]$sponsor <- 'duckworth, susan'
# LES[LES$sponsor == 'zzzzz',]$sponsor <- 'zzzzzz'

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1993 - 2020
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzzz) & LES$chamber == 'House' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1
  
### Senate -- 1993 - 2020
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzzz) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


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
  scale_color_manual(values=c("dodgerblue2", "red2", 'gray50'))

##### CHECK OUTLIERS ---> TONS of Dems with positive np_scores in Oklahoma...
# filter(LES, party == 'd' & np_score > 0) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & np_score < 0) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

