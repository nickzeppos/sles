

##########################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** NEW JERSEY *** BY SESSION
##########################################################################

###################################
## SPECIAL SESSIONS:
## ---- Folded into main term; bill numbers do not re-start.
## ----> E.g., ACR3 in 2006 is a special session bill but is just in the 2006 data as normal (https://www.njleg.state.nj.us/PropertyTaxSession/specialsessionpt.asp)
## MEMBER LISTS:
## ---- 
## PROCESS/RULES:
## ---- https://www.njleg.state.nj.us/legislativepub/Rules/AsmRules.pdf
## ---- https://www.njleg.state.nj.us/legislativepub/Rules/SenRules.pdf
## Sponsorship/Authorship
## ---- Multiple Primary Sponsors permitted but First Prime has key role (see rules)
## ---- NOT Listed alphabetically --> Using first sponsor 
###########################
## NOTES:
## (1) If scrape the main page (or in the dbfs?) can get LAST SESSION BILL NUMBER
## ---> See, e.g., S192 from 2006-2007 -- right at top 
## (2) WHAT TO DO ABOUT SUBSTITUTE BILLS and/or BILLS THAT ARE COMBINED? 
## ---> Would be hard to do for all states, but some states could account for this...
## (3) Is it ABC if a member makes a motion ot get a bill OUT OF COMMITEE (but doesn't succeed?) E.g., A0958 in 2006 + others... --> NO
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

this_state <- 'NJ'
min_year <- 1996
max_year <- 2017
keep_types <- c("A", "S")
spec_elec_codes <- c('s', 'gs')
sen_term_length <- 4 # Staggered? NO, BUT rotates in 1 2-year term every decade
# ---> Elections in years ending in 1, 3, and 7

#### Output Directory
#dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Data Directory
terms <- seq(min_year, max_year, 2)
# data_files <- list.files(glue("~/Dropbox/Data/State Legislative Data/States/{this_state}"), full.names = TRUE)
# bill_files <- data_files[grepl('Bill_Details', data_files)]
# rm(data_files, bill_files)

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("~/Dropbox/Data/State Legislative Data/Commem Bills/{this_state}_Commem_Bills.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- read.csv("~/Dropbox/Data/State Legislative Data/Significant Bills/SS_Data/SS_Bills.csv")
SS_bills <- SS_bills %>% 
  filter(state == this_state) %>%
  rename(bill_id = bill_num) %>%
  mutate(term = ifelse(year %% 2 == 0, paste0(year, "_", year + 1), paste0(year - 1, "_", year)),
         bill_id = toupper(bill_id),
         bill_id = gsub("B", "", bill_id),
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
klarner[klarner$cand == 'littel, robert e.',]$cand <- "littell, robert e."
klarner[klarner$cand == 'congilio, joseph',]$cand <- "coniglio, joseph"
klarner[klarner$cand == 'parkerspace, f.',]$cand <- "space, parker f."
# ----> IDs will still be off, but need to keep them to match to external data...

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[4]

for(t in terms){
  
  ### Formulate 2-year terms -- Cover both regular and special sessions
  t_yrs <- as.character(glue('{t}_{t+1}'))
  
  ### Skip Previously Estimated
  if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
    print(glue('. \n ~~~~ SKIPPING {t_yrs} TERM ---> LES scores already estimated! \n.'))
    next
  }
  
  ###### SESSION IN PROGRESS
  print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))
  
  ############### Read in data
  bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_yrs}.csv")
  bills <- read.csv(bill_path, skipNul = TRUE)   # Not clear why skipNul is necessary (might be the chapterNum col) but issues without it, and data seems identical
  
  ### Clean Term/Session Variables
  bills$term <- t_yrs
  ### NO SESSION INDICTOR; Coding as Term -- Bill numbers do not restart
  
  ### Check for duplicates
  if(nrow(bills) != nrow(distinct(bills))){
    cat(" \n ~~~> DUPLICATE BILLS \n\n .")
    break
  }
  
  ######## Standardize the Bill IDs
  bills <- rename(bills, bill_id = bill_number)
  
  bill_parts <- str_split_fixed(bills$bill_id, '-', 2)
  bills$bill_id <- paste0(bill_parts[,1], str_pad(as.numeric(bill_parts[,2]), 4, pad = '0')  )
  rm(bill_parts)
  
  ############### Drop Resolutions, Messages, Communications, Reports
  bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
  # table(bills$bill_type)
  bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)
  
  ############### Standardize Sponsors + Extracting + Ensuring Cosponsorship match down the line
  # bills$sponsor_dist <- str_extract(bills$sponsor, 'HD [0-9]+|SD [0-9]+')
  # bills$sponsor_party <- str_extract(bills$sponsor, '\\([A-Z]\\)')
  # table(bills$sponsor_dist); table(bills$sponsor_party)
  
  #### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
  bills$primary_sponsors <- tolower(bills$primary_sponsors)
  bills$primary_sponsors <- gsub('á', 'a', bills$primary_sponsors)
  bills$primary_sponsors <- gsub('é', 'e', bills$primary_sponsors)
  bills$primary_sponsors <- gsub('ó', 'o', bills$primary_sponsors)
  bills$primary_sponsors <- gsub('í', 'i', bills$primary_sponsors)
  bills$primary_sponsors <- gsub('ñ', 'n', bills$primary_sponsors)
  
  #### Adjusting for Bills By Request -- Need to do BEFORE splitting
  if(any(grepl('\\(br\\)|by request| br$', bills$primary_sponsors))){
    print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
    #print(glue("-----> KEEPING {nrow(filter(bills, grepl('(br)|by request', introducers)))} bill(s) introduced BY REQUEST"))
  }
  
  #### DROP Committee Bills
  ## Committees: (maj) nrae; 
  if(any(grepl('committee', bills$primary_sponsors))){
    print(glue("-----> DROPPING {nrow(filter(bills, grepl('committee', primary_sponsors)))} bill(s) introduced BY COMMITTEE"))
    bills <- filter(bills, !grepl('committee|^nrae$|maj nrae|^gov$', intro_sponsor))
  }
  
  #### Manual Name Fixes
  if(t_yrs == "2016_2017"){
    bills$primary_sponsors <- gsub('corrado, kristin$', 'corrado, kristin m.', bills$primary_sponsors)
    bills$primary_sponsors <- gsub('corrado, kristin;', 'corrado, kristin m.;', bills$primary_sponsors)
  }
  
  #### LES Sponsor Variable
  bills$LES_sponsor <- str_trim(gsub(';.+', '', bills$primary_sponsors))
  # sort(table(bills$LES_sponsor))
  
  ###### CHeck Missing Sponsors
  # filter(bills, LES_sponsor == "") %>% View()
  if(nrow(filter(bills, LES_sponsor == '')) > 0){
    print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
    bills <- filter(bills, !(LES_sponsor == ''))     
  }
  
  ###### DROP Bills that are just OPEN BILL NUMBERS reserved for later use
  if(nrow(filter(bills, LES_sponsor == 'open')) > 0){
    print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == 'open'))} reserved bill numbers"))
    bills <- filter(bills, !(LES_sponsor == 'open'))     
  }
  
  ### Manually Fix Sponsor Errors ---> First Sponsor = Out-Chamber
  # filter(bills, LES_sponsor == "sarlo, paul a." & substring(bill_id, 1,1) == "A")
  # filter(bills, grepl('toole', LES_sponsor)) %>% distinct(LES_sponsor)
  if(t_yrs == "1996_1997"){
    bills[bills$bill_id == "A1669",]$LES_sponsor <- "o'toole, kevin j."
    bills[bills$bill_id == "S0540",]$LES_sponsor <- "inverso, peter a."
  } else if(t_yrs == "1998_1999"){
    bills[bills$bill_id == "S0949",]$LES_sponsor <- "bryant, wayne r."
  } else if(t_yrs == "2002_2003"){
    bills[bills$bill_id == "A3689",]$LES_sponsor <- "gusciora, reed"
  } 

  ###################
  ###### Merge in S&S Bills
  ###################
  # *** For NEW JERSEY: Bills carry over during regular (one biennium) AND numbers DO NOT re-start for special sessions
  # ---> MERGE on ID only
  
  SS_term <- SS_bills %>% 
    filter(term == t_yrs)  %>%
    distinct(term, bill_id, SS)
  
  ### Merge
  bills <- bills %>%
    left_join(SS_term, by = c("bill_id", "term")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  
  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id", "term"))
  
  ####################################################
  ############### Code Commemorative
  ####################################################
  
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    mutate(session = t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session'))
  # table(bills$commem)
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  #################################################################
  ############### Code Bill History
  #################################################################
  bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_yrs}.csv")
  bill_hist <- read.csv(bill_hist_path)
  
  ######## Standardize BillHist Bill IDs
  bill_hist <- rename(bill_hist, bill_id = bill_number) 
  
  bill_parts <- str_split_fixed(bill_hist$bill_id, '-', 2)
  bill_hist$bill_id <- paste0(bill_parts[,1], str_pad(as.numeric(bill_parts[,2]), 4, pad = '0')  )
  rm(bill_parts)
  
  ### Clean Term/Session Variables
  bill_hist$term <- t_yrs

  ### Standardize Chamber Variable
  bill_hist$chamber <- recode(toupper(bill_hist$chamber), "A" = "House", "S" = "Senate", 'G' = "Governor")
  
  ### Updating Governor Terms
  # bill_hist$action <- ifelse(bill_hist$chamber == "Governor" & grepl("^Approved", bill_hist$action), paste0(bill_hist$action, ' by Governor'), bill_hist$action)
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  ## -- '1st Reading without Reference' --> SKIPPED COMMITTEE
  ## -- IF Add in RES: 'placed on desk' + 'public hearing held' + 'filed with secretary of state'
  ## -- What to do about substitutes/combinations???
  aic_t <- c('reported out of', 'reported from', 'reported and referred', "public hearing") # 'transferred to' = Transferred to different comm (not clear who is responsible)
  # ----> No records of action besides lack of referral and sparse public hearings
  abc_t <- c('reported out of', 'reported from', 'reported and referred', '2nd reading', 'motion', 'assembly floor amendment', 'senate amendment', 'recommitted to')
  pc_t <- c('^passed assembly', '^passed senate', 'received in the assembly', 'received in the senate')
  law_t <- c('^approved', '^cvor$')
  # cvor = 'conditional veto override'
  # ---> Captures both approved and approved with veto
  
  ### Check Actions
  # filter(bill_hist, grepl('^approved', tolower(action))) %>% distinct(action)
  # bill_hist[bill_hist$bill_id == "A4107",]
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
    hist_sub <- filter(bill_hist, bill_id == b_id)
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
    bill_stages$bill_url <- "Search by bill and session: https://www.njleg.state.nj.us/"
    all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
    # print(i)
  }
  options(warn = 1)
  
  ### Check Codings
  cat('\n')
  all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(chamber) %>% 
    summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% as.data.frame() %>% print()
  # filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% View()
  
  ### MERGE
  bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 
  
  ### Validate with Effective/Chapter Num Columns
  # filter(bills, law == 0 & !is.na(chapter_num)) %>% select(-summary)
  if(any(bills$law == 0 & !is.na(bills$chapter_num))){
    print('---> LAW CODING ERROR --- CHECK ACTIONS')
    break
  }
  
  ### MERGE In COMMEMS
  all_bill_stages <- commem_bills %>%
    filter(term == t_yrs) %>%
    mutate(session = t_yrs) %>%
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
    mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'A', "H", "S")) %>%
    select(LES_sponsor, chamber, passed_chamber, law) %>%
    mutate(term = t_yrs) %>%
    group_by(LES_sponsor, chamber, term) %>%
    summarize(num_sponsored_bills = n(), 
              sponsor_pass_rate = sum(passed_chamber) / n(),
              sponsor_law_rate = sum(law) / n()) %>%
    ungroup()
  
  ######## Cosponsorship Info --- This is only all primary sponsors, not full list of cosponsors
  all_sponsors$num_cosponsored_bills <- NA
  for(i in 1:nrow(all_sponsors)){
    c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'A', 'S')  )
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$primary_sponsors)))
    ### ONLY need to adjust this way if sponsored and cosponsored column are the same
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  }
  # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))
  
  #### CLEAN NAMES
  all_sponsors$full_name <- paste0(gsub('.+, ', '', all_sponsors$LES_sponsor), ' ', gsub(',.+', '', all_sponsors$LES_sponsor))
  parsed_names <- map_df(all_sponsors$full_name, parse_names) %>% select(-salutation) %>% distinct()
  all_sponsors <- left_join(all_sponsors, parsed_names, by = c("full_name" = "full_name"))
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  
  #### Update Last Names for Matching 
  if(substring(t_yrs, 1, 4) %in% seq(1998, 2015, 2)){ #1998 -- 2014
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "watson coleman, bonnie", "watson coleman", all_sponsors$last_name)
  }
  if(t_yrs %in% c("2014_2015", "2016_2017") ){ 
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "pintor marin, eliana", "pintormarin", all_sponsors$last_name)
  }
 
  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  ### If Term is 2008-2009, e.g., including 2007, 2008, and Specials for 2009 (when present)
  elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  ### Account for shifting Terms (2-4-4) --- Elections in, e.g, 2001, 2011, 2021 = 2-year, all others = 4-year
  sen_term_length <- ifelse( substring(elec_year,4,4) == 1, 2, 4)
  
  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% elec_year:(elec_year + 1) | (year == elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% (elec_year - sen_term_length + 2):(elec_year + 1) | (year == elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)
  
  #######################################################
  ############## Match Sponsors Names to Klarner Data
  #######################################################
  
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), '')))
  klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]+')))
  
  ### Edit Match Name
  if(t_yrs == "1998_1999"){
    all_sponsors[all_sponsors$LES_sponsor == "smith, bob",]$match_name <- "smith, robert g."
    klarner_sub[klarner_sub$cand == 'smith, robert g.',]$match_name <- 'smith, robert g.'
  }
  if(t_yrs == "2000_2001"){
    all_sponsors[all_sponsors$LES_sponsor == "smith, bob",]$match_name <- "smith, robert g."
    klarner_sub[klarner_sub$cand == 'smith, robert g.',]$match_name <- 'smith, robert g.'
    all_sponsors[all_sponsors$LES_sponsor == "smith, robert j.",]$match_name <- "smith, robert j."
    klarner_sub[klarner_sub$cand == 'smith, robert j.',]$match_name <- 'smith, robert j.'
  }
  if(t_yrs %in% c("2012_2013", "2014_2015") ){
    all_sponsors[all_sponsors$LES_sponsor == "brown, chris a.",]$match_name <- "brown, chris a."
    klarner_sub[klarner_sub$cand == 'brown, chris',]$match_name <- 'brown, chris a.'
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
      ## Check Middle Name
      if(length(m_sub) == 0 & !is.na(all_sponsors[i,]$middle_name)){
        match_name2 <- tolower(paste0(all_sponsors[i,]$last_name, ", ", all_sponsors[i,]$middle_name))
        m_sub <- grep(match_name2, k_matches$match_name)
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
  if(t_yrs == "2002_2003"){
    all_sponsors[all_sponsors$LES_sponsor == "kean, sean t." & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
    all_sponsors[all_sponsors$LES_sponsor == "smith, l. harvey" & all_sponsors$chamber == "S", c("klarner_name", "klarner_id", "elec_year")] <- NA
  } else if(t_yrs == "2008_2009"){
    all_sponsors[all_sponsors$LES_sponsor == "munoz, nancy f." & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  } else if(t_yrs == '2012_2013'){
    all_sponsors[all_sponsors$LES_sponsor == "decroce, bettylou" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }
  
  #### Check for Duplicates from Matching
  duplicates <- filter(all_sponsors, !is.na(klarner_name)) %>% group_by(chamber, klarner_name) %>% filter(n() > 1)
  if(nrow(duplicates) > 0){
    cat('-----> Check DUPLICATE Sponsors \n ')
    print(select(duplicates, LES_sponsor, klarner_name, chamber) %>% as.data.frame())
  }; rm(duplicates)
  
  ##### CHECK IF ANY KLARNER CANDIDATES MISSING FROM DATA --- Exclude Senators elected T-1
  km <- filter(klarner_sub, !(candid %in% all_sponsors$klarner_id) )
  km <- filter(km, sen == 0 | (sen == 1 & year == as.numeric(substring(t_yrs, 1, 4)) - 1 ))
  
  ### Remove candidates who won but were never seated
  if(t_yrs == "2006_2007"){
    km <- filter(km, cand != 'tucker, donald')
  }else if(t_yrs == "2010_2011"){
    km <- filter(km, cand != 'norcross, donald w.')
  }else if(t_yrs == '2012_2013'){
    km <- filter(km, cand != 'biondi, peter j.')
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
  
  ###########################################################
  ######## Estimate Scores + Add in Relatd Variables
  ###########################################################
  ### Check if bills in data without an ID'd sponsor
  # filter(bills, !(bills$primary_sponsor %in% legis_data$data_name))
  
  bills <- bills %>% #select(-sponsor) %>%
    rename(sponsor = LES_sponsor) %>%
    mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'A', 'H', 'S'))
  
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

rm(all_sponsors, bills, legis_data, SS_bills, elec_year, i, keep_types)
rm(k_matches, klarner_sub, m_sub, km, bill_path, t_yrs, calc_LES, c_sub) # 
rm(t, terms, klarner_gs, parsed_names, commem_bills)

########################################################################################################################################################
############################################################################################################################################################# NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by APPOINTMENT --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1996_1997 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 14 reserved bill numbers
# APPOINTED ~ HOUSE: 
# -- CHATZIDAKIS; POU; TALARICO; WEINGARTEN
# APPOINTED ~ SENATE: 
# -- BARK (via H, 1/1997)
# SPONSOR ERRORS:
# --Fixed (wrong chamber) for kenny bernard f.; rocco, john a.

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1998_1999 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 42 reserved bill numbers
# APPPOINTED ~ HOUSE:
## -- FAULKNER (served 10 weeks, lost geneal, https://en.wikipedia.org/wiki/Kenneth_William_Faulkner)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2000_2001 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 36 reserved bill numbers
## APPOINTED ~ HOUSE:
# -- KEAN (thomas); MUNOZ; PENNACHIO
## NAME FIXES
# -- Smith, bob = smith, robert g + updated match_name for smith, robert j

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2002_2003 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 333 reserved bill numbers
## APPOINTED ~ HOUSE:
# -- ALTAMURO (lost gen); BRAMNICK; CONOVER; DANCER; MCHOSE; RUMPF; SCALERA
# -- KEAN (sean, won't show, last name duplicated)
## APPOINTED ~ SENATE: 
# -- GEIST (lost gen); 
# -- KEAN (thomas, via H)
# -- SARLO (paul)
# -- SMITH (l. harvey, won't show, last name duplicated)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2004_2005 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 26 reserved bill numbers
## APPOINTED ~ HOUSE: 
# -- PRIETO
## APPOINTED ~ SENATE:
# -- DORIA; WEINBERG
## IN HOUSE:
# -- DIGAETANO (didn't run for reelection)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2006_2007 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 26 reserved bill numbers
## WON SPECIAL CONVENTION ~ HOUSE: 
# -- TRUITT
## APPOINTED ~ SENATE:
# -- DORIA; MCCULLOUGH; WEINBERG
## DROP:
# -- tucker, donald -- died 10/2005 -- https://en.wikipedia.org/wiki/Donald_Kofi_Tucker

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2008_2009 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 60 reserved bill numbers
## APPOINTED ~ HOUSE: 
# -- DIMAIO; QUIJANO; RILEY
# -- MUNOZ (name duplicated, wont show; appointed following death of husband, eric munoz in March 2009)
## APPOINTED ~ SENATE:
# -- KARROW (marcia, 2009)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2010_2011 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 28 reserved bill numbers
## APPOINTED ~ HOUSE: 
# -- BENSON; CIATTARELLI; DELANY (resigned 8/2011); 
# -- ODONNELL (jason); RYAN (kevin, didn't run again); 
# -- WILSON
## APPOINTED/WON SPECIAL ~ SENATE:
# -- ADDIEGO (11/2000); 
# -- GOODWIN (3/2010, lost 11/2010 special to greenstein)
# -- GREENSTEIN (11/2000)
# -- NORCROSS (donald, via H, was only there for 7 days...)
## DROP
# -- norcross, donald w. -- Was in house for 1 week before appointment to Senate

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2012_2013 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 27 reserved bill numbers
## APPOINTED ~ HOUSE:
# -- ANDRZEJCZAK; SIMON; SPACE (parker) 
# -- DECROCE (bettylou, appointed following husbands death, name won't show bc duplicated)
## NOTE:
# decroce, alex -- died 1/9/2012, but must have sponsored a bill... so keeping  -- https://en.wikipedia.org/wiki/Alex_DeCroce
## DROP:
# -- biondi, peter j. -- passed away 11/10/2011 -- https://ballotpedia.org/Peter_Biondi

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2014_2015 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 19 reserved bill numbers
## APPOINTED ~ HOUSE:
# -- DANIELSEN; HOLLEY; JONES (patricia); MUOIO; TALIAFERRO (adam)
## NAME FIX:
# -- Klarner errro: parkerspace, f. ---> space, parker f.

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2016_2017 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 57 reserved bill numbers
## APPOINTED/WON SPECIAL ~ HOUSE:
# -- KARABINCHAK; ROONEY; THOMSON ('ned'); WATSON (blonnie)
## APPOINTED ~ SENATE:
# -- BELL (lost gen); CORRADO; DIEGNAN
## NAME FIX
# -- Collapsed corrado, kristin + corrado, kristin m. = corrado, kristin m.



# filter(klarner, grepl("corrado", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid)
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

### Error Fixes
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_id <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_name <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$sponsor <- "owlett, clint"

### ****Still missing***** 
# ---> Remaining = Not in KLARNER or 2017-2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[7]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term)
# filter(klarner, grepl('thomson', cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, missing, name_sub, still_missing)


### Fix Missing
name_matches <- data.frame(LES_name = "faulkner, kenneth", k_name = 'faulkner, ken')
name_matches <- add_row(name_matches, LES_name = "o'donnell, jason", k_name = 'odonnell, jason')
name_matches <- add_row(name_matches, LES_name = 'danielsen, joe', k_name = 'danielsen, joseph f.')
#name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}
rm(name_matches)


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
rm(check_dup, k_sub, exact, name_sub, missing)


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
LES[LES$sponsor == "truitt, oadline",]$party <- 'd'
LES[LES$sponsor == "truitt, oadline",]$district <- 26
LES[LES$sponsor == "truitt, oadline",]$exper <- 'none'

LES[LES$sponsor == "ryan, kevin",]$party <- 'd'
LES[LES$sponsor == "ryan, kevin",]$district <- 36
LES[LES$sponsor == "ryan, kevin",]$exper <- 'none'

LES[LES$sponsor == "delany, patrick",]$party <- 'r'
LES[LES$sponsor == "delany, patrick",]$district <- 8
LES[LES$sponsor == "delany, patrick",]$exper <- 'none'

### REST WON"T Mtter with Klarner Update so just doing party
LES[LES$sponsor == "karabinchak, robert",]$party <- 'd'
LES[LES$sponsor == "rooney, kevin",]$party <- 'r'
LES[LES$sponsor == "thomson, edward",]$party <- 'r'
LES[LES$sponsor == "watson, blonnie",]$party <- 'd'
LES[LES$sponsor == "corrado, kristin",]$party <- 'r'

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t, i)

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
# **** ALL data should be right (assuming still in chamber) EXCEPT Majority Member IN STAGGERED STATES ??? ********
senate <- filter(hf_data, chamber == "Senate" & substring(year, 4, 4) %in% c(3, 7))
senate$year <- senate$year + 2
senate$term <- paste0(senate$year + 1, "_", senate$year + 2)
hf_data <- bind_rows(hf_data, senate) %>% arrange(year, chamber)
rm(senate)

### Subset
hf_data <- filter(hf_data, year > min_year - 4) %>% distinct() %>% select(-MajorityMember)

### Merge
LES <- left_join(LES, select(hf_data, -year), by = c('term' = 'term', 'chamber' = 'chamber', 'klarner_id' = 'CandId'))

### Check Duplicates
mutate(LES, check_dup = paste(sponsor, term, chamber, sep = "--")) %>%
  mutate(dup = duplicated(check_dup)) %>%
  filter(dup == TRUE) %>% select(sponsor, term, chamber)#%>% View()

### Set Committees to NA for Years without Data -- May have matched candids in year range
# set_NA <- colnames(hf_data)
# set_NA <- set_NA[!(set_NA %in% c("CandId", 'chamber', "year", "term")) ]
# LES[LES$term == '2016_2017', set_NA] <- NA
# ---> This should be fine, 2014-2015 was first 2 years, so 2016_2017 should be mostly right
rm(hf_data)

#################################
### Shor and McCarty Data, 1993 - 2016
####################################

ideo <- readstata13::read.dta13("~/Dropbox/Data/Shor_McCarty_Data/shor_mccarty_1993_2016_individual_data_May_2018.dta")
ideo <- filter(ideo, st == this_state) %>% select(-st_id)
ideo$match_name <- str_extract(tolower(ideo$name), '[a-z]+, [a-z]+')
ideo$match_name <- ifelse(is.na(ideo$match_name), tolower(ideo$name), ideo$match_name)
ideo$last_name <- gsub(',.+', '', tolower(ideo$name))

## ********** LOTS OF DUplicates from 2015-2016--> Dropping for now
## Not dropping because it seems they are incorrect names... e.g., two overlapping records for Nelson Albano, one beyond his period of servie
# ideo <- filter(ideo, !duplicated(paste(name, party)))

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
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('zzzzzzz')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('barnes.+iii', tolower(name))) %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

### ****** LOTS OF NAME DUPLICATES --- All commented have 2 identical (plus some of the filled in ones have multiple slightly different entries)
# name_matches <- add_row(name_matches, LES_name = 'albano, nelson', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'allen, diane', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'amodeo, john f.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'angelini, mary pat', SM_name = 'zzzzz')
name_matches <- data.frame(LES_name = 'barnes, peter j. iii', SM_name = 'Barnes III, Peter J') # ****** Listed x 3
name_matches <- add_row(name_matches, LES_name = 'barnes, peter j. jr.', SM_name = 'Barnes, Peter Jr.')
# name_matches <- add_row(name_matches, LES_name = 'baroni, bill', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'bateman, christopher', SM_name = 'Bateman, Christopher S')
# name_matches <- add_row(name_matches, LES_name = 'bell, colin', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'biondi, peter j.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'bramnick, jon', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'bucco, anthony r.', SM_name = 'Bucco, Anthony R.')
name_matches <- add_row(name_matches, LES_name = 'bucco, anthony m.', SM_name = 'Bucco, Anthony Jr.')
# name_matches <- add_row(name_matches, LES_name = 'buono, barbara a.', SM_name = 'zzzzz')
#name_matches <- add_row(name_matches, LES_name = 'cardinale, gerald', SM_name = 'zzzzz')
#name_matches <- add_row(name_matches, LES_name = 'casagrande, caroline', SM_name = 'zzzzz')
#name_matches <- add_row(name_matches, LES_name = 'chiappone, anthony', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'chiusano, gary r.', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'cohen, neil m.', SM_name = 'Cohen, Neil M.')
name_matches <- add_row(name_matches, LES_name = 'conaway, herbert c. jr.', SM_name = 'Conaway Jr, Herbert C')
# name_matches <- add_row(name_matches, LES_name = 'corrado, kristin', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'cryan, joseph', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'cunningham, glenn d.', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'cunningham, sandra bolden', SM_name = 'Cunningham, Sandra Bolden')
# name_matches <- add_row(name_matches, LES_name = 'decroce, alex', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'diegnan, patrick', SM_name = 'Diegnan Jr, Patrick J')
name_matches <- add_row(name_matches, LES_name = 'edwards, willis iii', SM_name = 'Edwards III, Willis')
# name_matches <- add_row(name_matches, LES_name = 'egan, joseph v.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'gill, nia h.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'haines, phil', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'huttle, valerie vainieri', SM_name = 'Vainieri Huttle, Valerie')
# name_matches <- add_row(name_matches, LES_name = 'kean, sean', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'kean, thomas h. jr.', SM_name = 'Kean Jr, Thomas H')
name_matches <- add_row(name_matches, LES_name = 'kyrillos, joseph m. jr.', SM_name = 'Kyrillos Jr, Joseph M')
# name_matches <- add_row(name_matches, LES_name = 'lustbader, monroe jay', SM_name = 'zzzzz')
#name_matches <- add_row(name_matches, LES_name = 'madden, fred', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'mccullough, james (sonny)', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'muoio, elizabeth maher', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'pennacchio, joseph', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'rice, ronald l.', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'rooney, john', SM_name = 'Rooney, John E.')
# name_matches <- add_row(name_matches, LES_name = 'rooney, kevin', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'russo, david c.', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'scutari, nicholas p.', SM_name = 'Scutari, Nicholas P')
# name_matches <- add_row(name_matches, LES_name = 'spencer, l. grace', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'stender, linda', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'thomson, edward', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'voss, joan', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'wagner, connie terranova', SM_name = 'Wagner, Concetta')
# name_matches <- add_row(name_matches, LES_name = 'whelan, jim', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

# #### Manual Edits (Needs more precision...)
LES[LES$sponsor == 'smith, robert g.',]$SM_name <- ideo[ideo$name == 'Smith, Robert' & ideo$senate2002 %in% 1,]$name
LES[LES$sponsor == 'smith, robert g.',]$SM_party <- ideo[ideo$name == 'Smith, Robert' & ideo$senate2002 %in% 1,]$party
LES[LES$sponsor == 'smith, robert g.',]$np_score <- ideo[ideo$name == 'Smith, Robert' & ideo$senate2002 %in% 1,]$np_score

LES[LES$sponsor == 'smith, robert j.',]$SM_name <- ideo[ideo$name == 'Smith, Robert' & ideo$house2004 %in% 1,]$name
LES[LES$sponsor == 'smith, robert j.',]$SM_party <- ideo[ideo$name == 'Smith, Robert' & ideo$house2004 %in% 1,]$party
LES[LES$sponsor == 'smith, robert j.',]$np_score <- ideo[ideo$name == 'Smith, Robert' & ideo$house2004 %in% 1,]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1996 - 2019
LES[as.numeric(substring(LES$term,1,4)) %in% c(2002:2019) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1996:2001) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1996 - 2019
# ** Split Control in SENATE in 2002_2003 -- Co-Presidents --- CODING ALL AS 0
LES[as.numeric(substring(LES$term,1,4)) %in% c(2004:2019) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1996:2001) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1

LES[LES$chamber == 'Senate' & LES$term == '2002_2003',]$in_majority <- 0




############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# select(LES, sponsor, data_name, klarner_name, SM_name, term, chamber) %>% arrange(sponsor, term, chamber) %>% View()
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

##### Drop Nicknames
# filter(LES, grepl('\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

#### Manual Fixes
LES[LES$sponsor == 'decroce, bettylou',]$sponsor <- 'decroce, betty lou'
LES[LES$sponsor == 'bassano, c. louis',]$sponsor <- 'bassano, charles louis'
LES[LES$sponsor == 'cruzperez, nilsa',]$sponsor <- 'cruz-perez, nilsa'
LES[LES$sponsor == 'garrett, e. scott',]$sponsor <- 'garrett, ernest scott'


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
  scale_color_manual(values=c("dodgerblue2", "red2", "gray50"))

##### CHECK OUTLIERS
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party, SM_name)
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party, SM_name)

### Who's the super liberal Republican??? Andrew R Ciesla... 
# --- SM have him as a D, but def an R: https://ballotpedia.org/Andrew_Ciesla
# --- Was an R leader! https://en.wikipedia.org/wiki/Andrew_R._Ciesla
# filter(LES, party == 'r' & np_score < -.5) %>% select(sponsor, SM_name, party, SM_party, SM_name, term, chamber, np_score, LES, LES_rank)


### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

