################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** DELAWARE *** BY SESSION
##############################################################

# *** IF re-scrape: fix the secondary sponsors gathering... no seperator (in 2012_2014 at least, weird spacing; fine in code, but odd)

###########
#### QUESTIONS
# (1) How to deal with the two substitute bills where the sponsor changes????

###################################
## (SPECIAL) SESSIONS:
## ---- Special Sessions permitted, but bills appear to carry over 
## MEMBER LISTS:
## ---- Inidividual member profiles all follow a similar pattern (but no easy list) -- see notes during cleaning at bottom
## PROCESS/RULES:
## ---- Glossary of Terms: https://legis.delaware.gov/Resources/GlossaryOfTerms
## Sponsorship/Authorship
## ---- 1 Primary Sponsor; Co-prime sponsors permitted but seperated out; Additional cosponsors also allowed.
###########################
## NOTES:
## (1) ACCOUNTING For SUBSTITUTES Recorded on Seperate pages as HS and SS
## --> So, SB10 will be substitute with new text and recorded as SS1 for SB10
## --> Coding rule: So long as sponsors match (the bills appear to usually (always?) be identical), tracking as a continuation of SB10
## ** SEE: SB344 -- http://legis.delaware.gov/BillDetail?LegislationId=14277
## ** AND: SS1 for SB344 -- http://legis.delaware.gov/BillDetail?LegislationId=14066
## (2) Sessions sometimes start in DECEMBER of election year, hence, e.g., 1998_2000 
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

this_state <- 'DE'
min_year <- 2002 # Have bills for 1999+, but no histories until 2003
max_year <- 2018
keep_types <- c('HB', 'SB')
spec_elec_codes <- c('s', 'gs')
house_term_length <- 2
sen_term_length <- 4 # STAGGERED? YES

#### Output Directory
# dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Terms/Sessions and Filespaths
election_years <- seq(min_year, max_year - 1, 2)
# data_files <- list.files(glue("~/Dropbox/Data/State Legislative Data/States/{this_state}"), full.names = TRUE)
# bill_files <- data_files[grepl('Bill_Details', data_files)]
# terms <- gsub('.+Bill_Details_|.csv', '', bill_files)
# rm(data_files, bill_files)

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
# klarner[klarner$cand == 'green, mrs. david (opal)',]$cand <- "green, david l."

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- election_years[2]

for(t in election_years){
  
  ### Formulate 2-year terms -- Cover both regular and special sessions
  # **** Saved as, eg, 1998_2000 because terms occassionally start in December but for standardization, saving as, eg, 1999_2000
  t_yrs <- as.character(glue('{t}_{t+2}'))
  save_yrs <-  as.character(glue('{t+1}_{t+2}'))
  #t_sessions <- sessions[grepl(glue('{t}|{t+1}|{t+2}|{t+3}'), sessions)]
  
  ### Skip Previously Estimated
  if(glue("{this_state}_LES_{save_yrs}.csv") %in% list.files('.')){
    print(glue('. \n ~~~~ SKIPPING {save_yrs} session ---> LES scores already estimated! \n.'))
    next
  }
  
  ###### SESSION IN PROGRESS
  print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {save_yrs} TERM! ~~~~~~~~~~~~~~ '))
  
  ############### Read in data
  bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_yrs}.csv")
  bills <- read.csv(bill_path)
  
  ### Clean Term/Session Variables
  bills <- bills %>%
    rename(ga_num = session_num) %>%
    mutate(term = save_yrs,
           session = save_yrs)
  
  ### Drop duplicates
  bills <- distinct(bills)
  
  ######## Standardize the Bill IDs
  bills <- bills %>%
    rename(bill_id = bill_number) %>%
    mutate(bill_id = paste0(gsub(' [0-9].+| [0-9]+', '', bill_id), str_pad(gsub('.+ ', '', bill_id), 4, pad = '0')),
           parent_bill = ifelse(parent_bill == '', '', paste0(gsub(' [0-9].+| [0-9]+', '', parent_bill), str_pad(gsub('.+ ', '', parent_bill), 4, pad = '0'))))
  
  ######### Create ID for Substitute Record
  # --- Is NOT NA when a bill was substituted and all subsequent records moved to a new bill page (but kept same sponsor)
  # TWO PROBLEM BILLS (e.g., different sponsors):
  # -- SB 42 (2005-2006) -- https://legis.delaware.gov/BillDetail/16755
  # -- HB 421 (2007-2008) -- https://legis.delaware.gov/BillDetail/18415
  bills$substitute_id <- NA
  options(warn = 2)
  for(i in 1:nrow(bills)){
    if(bills[i,]$bill_id %in% bills$parent_bill){
      bills[i,]$substitute_id <- paste(bills[bills$parent_bill == bills[i,]$bill_id,]$bill_id, collapse = "; ")
      if(any(!(bills[bills$parent_bill == bills[i,]$bill_id,]$sponsor %in% bills[i,]$sponsor))){
        if(bills[bills$parent_bill == bills[i,]$bill_id,]$sponsor == ''){
          ## Only One: "HB0187" in "2012_2014" # --> Sponsor name is missing but everything else is basically identical
          next
        }else if(paste(bills[i,]$bill_id, bills[i,]$term, sep="-") %in% c('SB0042-2005_2006', "HB0421-2007_2008")){
          ### For both, sponsor of substitute != sponsor of bill... Rare, but not clear how to adjust these systematically...
          next
        }else{
          print(' ***** SUBSTITTUE BILL SPONSOR DOES NOT MATCH ORIGINAL BILL SPONSOR ****** ')
          break
        }
      }
    }
  }; options(warn = 1)
  
  ############### Drop Resolutions, Messages, Communications, Reports
  bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
  # table(bills$bill_type)
  bills <- filter(bills, bill_type %in% keep_types) 
  
  ##########################
  ####### Standardize Sponsors
  
  bills$sponsor <- tolower(bills$sponsor)
  bills$sponsor <- gsub('á', 'a', bills$sponsor)
  bills$sponsor <- gsub('é', 'e', bills$sponsor)
  bills$sponsor <- gsub('ó', 'o', bills$sponsor)
  bills$sponsor <- gsub('í', 'i', bills$sponsor)
  bills$sponsor <- gsub('ñ', 'n', bills$sponsor)
  
  bills$secondary_sponsors <- tolower(bills$secondary_sponsors)
  bills$secondary_sponsors <- gsub('á', 'a', bills$secondary_sponsors)
  bills$secondary_sponsors <- gsub('é', 'e', bills$secondary_sponsors)
  bills$secondary_sponsors <- gsub('ó', 'o', bills$secondary_sponsors)
  bills$secondary_sponsors <- gsub('í', 'i', bills$secondary_sponsors)
  bills$secondary_sponsors <- gsub('ñ', 'n', bills$secondary_sponsors)
  
  bills$cosponsors <- str_trim(tolower(bills$cosponsors))
  bills$cosponsors <- gsub('á', 'a', bills$cosponsors)
  bills$cosponsors <- gsub('é', 'e', bills$cosponsors)
  bills$cosponsors <- gsub('ó', 'o', bills$cosponsors)
  bills$cosponsors <- gsub('í', 'i', bills$cosponsors)
  bills$cosponsors <- gsub('ñ', 'n', bills$cosponsors)
  
  ### LES Sponsor Var
  bills <- rename(bills, LES_sponsor = sponsor)
  # table(bills$LES_sponsor)
  
  ### Chamber Cosponsors
  bills$house_all_primary <- str_extract(bills$secondary_sponsors, "rep..+|reps..+")
  bills$house_all_primary <- gsub('reps\\. |rep\\. |---$', '', gsub("---.+|sen\\..+|sens\\..+", "", bills$house_all_primary))
  bills$house_all_primary <- ifelse(is.na(bills$house_all_primary), '', bills$house_all_primary)
  bills$house_all_primary <- gsub('  +', ' ', gsub(',', ', ', bills$house_all_primary))
  bills$senate_all_primary <- str_extract(bills$secondary_sponsors, "sen\\..+|sens\\..+")
  bills$senate_all_primary <- gsub('sens\\. |sen\\. |---$', '', gsub("---.+|rep\\..+|reps\\..+", "", bills$senate_all_primary))
  bills$senate_all_primary <- ifelse(is.na(bills$senate_all_primary), '', bills$senate_all_primary)
  bills$senate_all_primary <- gsub('  +', ' ', gsub(',', ', ', bills$senate_all_primary))
  
  bills$house_cosponsors <- str_extract(bills$cosponsors, "rep..+|reps..+")
  bills$house_cosponsors <- gsub('reps\\. |rep\\. |---$', '', gsub("---.+|sen\\..+|sens\\..+", "", bills$house_cosponsors))
  bills$house_cosponsors <- ifelse(is.na(bills$house_cosponsors), '', bills$house_cosponsors)
  bills$senate_cosponsors <- str_extract(bills$cosponsors, "sen\\..+|sens\\..+")
  bills$senate_cosponsors <- gsub('sens\\. |sen\\. |---$', '', gsub("---.+|rep\\..+|reps\\..+", "", bills$senate_cosponsors))
  bills$senate_cosponsors <- ifelse(is.na(bills$senate_cosponsors), '', bills$senate_cosponsors)
  
  bills$all_chamber_sponsors <- ifelse(substring(bills$bill_id,1,1) == "H", 
                                       paste(bills$LES_sponsor, bills$house_all_primary, bills$house_cosponsors, sep = ', '),
                                       paste(bills$LES_sponsor, bills$senate_all_primary, bills$senate_cosponsors, sep = ', '))
  bills$all_chamber_sponsors <- gsub(',$', '', gsub(', ,', ',', str_trim(bills$all_chamber_sponsors)))
  bills$all_chamber_sponsors <- str_trim(gsub('  +', ' ', gsub(',', ', ', bills$all_chamber_sponsors)))
  bills <- select(bills, -c(house_all_primary, house_cosponsors, senate_all_primary, senate_cosponsors))
  
  ##### Fix Duplicate Last Names without initial
  if(save_yrs == '2003_2004'){
    bills[bills$LES_sponsor == 'ennis',]$LES_sponsor <- "b. ennis" # Bruce Ennis (as opposed to D. Ennis)
    bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors <- gsub('^ennis', 'b. ennis', bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors)
    bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors <- gsub(', ennis', ', b. ennis', bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors)
  }
  if(save_yrs %in% c('2003_2004', '2005_2006', '2007_2008')){
    bills[bills$LES_sponsor == 'smith',]$LES_sponsor <- "w. smith" # Wayne Smith (as opposed to M. Smith)
    bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors <- gsub('^smith', 'w. smith', bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors)
    bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors <- gsub(', smith', ', w. smith', bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors)
  }
  # filter(all_sponsors, grepl("smith", LES_sponsor))
  # filter(klarner, grepl("smith", cand)) %>% select(cand, candid, year, sen, outcome, ddez) %>% filter(year == 2002)
  
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
  # *** For DELAWARE: Bills Carryover, Special Sessions folded into main biennial term
  SS_term <- SS_bills %>%
    filter(term == save_yrs) %>%
    mutate(session = save_yrs) %>%
    distinct(term, session, bill_id, SS)
  
  bills <- bills %>%
    left_join(SS_term, by = c("bill_id", "term", "session")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  
  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id", "term", "session"))
  
  #############################################
  ######### Code Commemorative
  #############################################
  bills <- commem_bills %>%
    filter(term == save_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session'))
  # table(bills$commem)

  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  ######################################################
  ############### Code Bill History
  #############################################
  bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_yrs}.csv")
  bill_hist <- read.csv(bill_hist_path)
  
  ### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
  bill_hist <- bill_hist %>%
    rename(ga_num = session_num,
           bill_id = bill_number) %>%
    mutate(term = save_yrs,
           session = save_yrs,
           bill_id = paste0(gsub(' [0-9].+| [0-9]+', '', bill_id), str_pad(gsub('.+ ', '', bill_id), 4, pad = '0')),
           parent_bill = ifelse(parent_bill == '', '', paste0(gsub(' [0-9].+| [0-9]+', '', parent_bill), str_pad(gsub('.+ ', '', parent_bill), 4, pad = '0'))),
           substitute_id = NA)
  
  ######### Create ID for Substitute Record to Match Record in Bills
  options(warn = 2)
  for(b_id in unique(bill_hist$bill_id)){
    if(grepl('^HS|^SS', b_id)){next}
    if(b_id %in% bill_hist$parent_bill){
      bill_hist[bill_hist$bill_id == b_id,]$substitute_id <- paste(unique(bill_hist[bill_hist$parent_bill == b_id,]$bill_id), collapse = "; ")
    }
  }; rm(b_id) 
  options(warn = 1)
  
  ### Order by Order
  bill_hist <- bill_hist %>%
    mutate(master_id = ifelse(grepl("^HS|^SS", bill_id), parent_bill, bill_id)) %>%
    arrange(term, session, master_id, action_date, order) %>%
    group_by(master_id) %>%
    # **** Initial Order not always right; so arranging by date and then keeping original order of actions on same date ******
    mutate(order = 1:n()) %>%
    ungroup()
  
  ### Create + Fill in Chamber Variable:
  bill_hist <- bill_hist %>%
    mutate(chamber = ifelse(order == 1 & substring(bill_id, 1,1) == "H", "H", NA),
           chamber = ifelse(order == 1 & substring(bill_id, 1,1) == "S", "S", chamber),
           chamber = ifelse(is.na(chamber) & grepl("in House|by House", action), "H", chamber),
           chamber = ifelse(is.na(chamber) & grepl("in Senate|by Senate", action), "S", chamber)
           ) %>%
    group_by(term, session, master_id) %>%
    fill(chamber) %>%
    ungroup()
  
  ### Re-Coding Chamber Variable
  bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate", "G" = "Governor", "CC" = "Conference")
  
  ### Clean Substitue Info From the Actions (e.e.g, HS 1 for HB 1 - Passed by)
  bill_hist$action <- str_trim(gsub("HS [0-9]+ for HB [0-9]+ (- +-|-+)", '', bill_hist$action))
  bill_hist$action <- str_trim(gsub("SS [0-9]+ for SB [0-9]+ (- +-|-+)", '', bill_hist$action))
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  aic_t <- c('reported out', '^tabled in') # "^assigned.+subcomm"
  ## The only committee actions are introduced, assigned to, or reported out
  abc_t <- c('reported out', 'amendment.+in (house|senate)', 'amendent (ha|sa).+passed', 'amendent (ha|sa).+defeated',
             'rules.+suspended.+(house|senate)', 'lifted from table')
  # -- Could do 'substituted in' but not clear when it happens... assume the floor but sometimes happens before assigned to comm? weird..
  pc_t <- c('^passed', 'vetoed', "passed by (house of representatives|senate)")
  # --> "^passed by" may technically be what we want. Occassional "passed in [chamber] by voice vote" (which is sometimes followed by "passed by [chamber], Votes:")
  law_t <- c('^signed by gov', '^enact')
  ## Enact = Enact w/o sign by governor

  ### Check Actions
  # filter(bill_hist, grepl('passed by', tolower(action)) ) %>% distinct(action) %>% View() 
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
    hist_sub <- filter(bill_hist, master_id == b_id, term == save_yrs, session == s_id)
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, save_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
    bill_stages$bill_url <- bills[i,]$bill_url
    if(bill_stages$law == 0 & tolower(bills[i,]$status) %in% c("enact w/o sign", "signed")){
      bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- bill_stages$law <- 1
    }
    if(bill_stages$passed_chamber == 0 & tolower(bills[i,]$status) %in% c("passed")){
      bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
    }
    if(bill_stages$action_beyond_comm == 0 & tolower(bills[i,]$status) %in% c("out of committee")){
      bill_stages$action_beyond_comm <- 1
    }
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
    filter(term == save_yrs) %>%
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
    mutate(term = save_yrs) %>%
    group_by(LES_sponsor, chamber, term) %>%
    summarize(num_sponsored_bills = n(), 
              sponsor_pass_rate = sum(passed_chamber) / n(),
              sponsor_law_rate = sum(law) / n(),
              num_cosponsored_bills = NA) %>%
    ungroup()
  
  #### Adding Legislators Who COSPONSORED A BILL but did not SPONSOR
  unique_cospon <- str_trim(unique(unlist(str_split(bills$all_chamber_sponsors, ', '))))
  for(nonspon in unique_cospon){
    if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != '' & !is.na(nonspon)){
      chamb <- unique(substring(bills[grepl(nonspon, bills$all_chamber_sponsors),]$bill_id, 1, 1))
      if("H" %in% chamb & "S" %in% chamb){
        print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
      }else{
        all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = chamb, term = save_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0) 
      }
    }
  }
  
  ######## Cosponsorship Info 
  #bills$cospon_match <- paste(bills$LES_sponsor, bills$coauthors, sep = '; ')
  for(i in 1:nrow(all_sponsors)){
    c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
    sn <- all_sponsors[i,]$LES_sponsor
    sn <- gsub('\\)', '\\\\)', gsub("\\(", '\\\\(', sn))
    search_term <- paste0("^", sn, ',|^', sn, '$|, ', sn, ',|, ', sn, '$')
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(search_term, tolower(c_sub$all_chamber_sponsors)))
    ### ONLY need to adjust this way if sponsored and cosponsored column are the same
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  }; rm(sn, search_term)
  # bills <- select(bills, -cospon_match)
  # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))

  #######################
  #### CLEAN NAMES
  all_sponsors$last_name <- gsub('^[^ ]+ ', '', all_sponsors$LES_sponsor)
  all_sponsors$first_name <- str_extract(all_sponsors$LES_sponsor, "[a-z]\\.[a-z]\\. |[a-z]\\. |[a-z] ")
  all_sponsors$first_name <- ifelse(is.na(all_sponsors$first_name), '', substring(all_sponsors$first_name,1,1))
 
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  
  #### Update First/Last Names for Matching 
  if(t_yrs %in% c("2008_2010", "2010_2012")){
    all_sponsors[all_sponsors$LES_sponsor == 'd.e. williams',]$first_name <-  "dennis e."
    all_sponsors[all_sponsors$LES_sponsor == 'd.p. williams',]$first_name <-  "dennis p."
  }
  if(t_yrs %in% c("2008_2010", "2010_2012", '2012_2014', '2014_2016', '2016_2018')){
    all_sponsors[all_sponsors$LES_sponsor == 'q. johnson',]$first_name <-  "s"
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
  
  ####################################################
  ############## Match Sponsors Names to Klarner Data, Forgoing Function because mostly only have last name
  ####################################################
  
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), '')))
  klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]')))  
  
  ### Edit Match Name
  if(t_yrs %in% c("2008_2010", "2010_2012")){
    klarner_sub[klarner_sub$cand == 'williams, dennis p.',]$match_name <- 'williams, dennis p.'
    klarner_sub[klarner_sub$cand == 'williams, dennis e.',]$match_name <- 'williams, dennis e.'
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
  
  #### Fix Mismatches
  if(t_yrs == "2006_2008"){
    all_sponsors[all_sponsors$LES_sponsor == "b. short" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
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
  if(t_yrs == "2002_2004"){
    km <- filter(km, cand != 'sharp, thomas b.')
  }else if(t_yrs == "2008_2010"){
    km <- filter(km, cand != 'mcwilliams, diana m.')
    km <- filter(km, cand != 'vaughn, james t.')
  }else if(t_yrs == "2012_2014"){
    km <- filter(km, cand != 'connor, dorinda a.')
    km <- filter(km, cand != 'booth, joseph w.')
    km <- filter(km, cand != 'bunting, george h. jr.')
  }else if(t_yrs == "2014_2016"){
    km <- filter(km, cand != 'venables, robert l. sr.')
  }else if(t_yrs == "2016_2018"){
    km <- filter(km, cand != 'halllong, bethany')
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
  bills <- bills %>% #select(-sponsor) %>%
    rename(sponsor = LES_sponsor) %>%
    mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'H', 'H', 'S'))
  
  ### Standard LES: Same as Congressional Measure
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/calc_LES_fx.R')
  
  LES <- calc_LES(bills, legis_data, save_yrs, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1))
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
  LES_noWeights <- calc_LES(bills, legis_data, save_yrs, ss_weight = 5, reg_weight = 5, com_weight = 5, stage_weights = c(1,1,1,1,1))
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
  write.csv(LES, glue("{this_state}_LES_{save_yrs}.csv"), row.names = FALSE)
  rm(LES)
  
  
  cat(glue(". \n *********************** SESSION {save_yrs} ~~> DONE  ***********************"))
  
}
################# ****** END LOOP


rm(all_sponsors, bills, legis_data, SS_bills, commem_bills, H_elec_year, S_elec_year, i, keep_types)
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES, house_term_length, sen_term_length) # 
rm(t, election_years, klarner_gs, m_sub, nonspon, unique_cospon, c_sub, save_yrs, chamb) # 


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by SPECIAL ELECTION --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
# Historical Election Results (back to 2003): https://www.sos.ms.gov/Elections-Voting/Pages/Election-Results-By-Year.aspx
########################################################################################################################
### FULL ROSTER: 
#########################


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
#     session chamber   N AIC ABC PASS LAW
# 1 2003_2004       H 549 398 435  342 209
# 2 2003_2004       S 355 239 284  244 200
### DROP:
# -- sharp, thomas b. -- no record of  him in chamber that term: https://legis.delaware.gov/AssemblyMember/141/Sharp


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
#     session chamber   N AIC ABC PASS LAW
# 1 2005_2006       H 546 381 416  329 220
# 2 2005_2006       S 407 268 316  267 221
# ***** NO ISSUES AFTER NAME CORRECTIONS ******


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
#     session chamber   N AIC ABC PASS LAW
# 1 2007_2008       H 526 369 407  321 225
# 2 2007_2008       S 328 227 265  236 197
#### APPOINTED/WON SPECIAL ~ HOUSE:
# -- CARSON (william)
# -- HASTINGS (greg)
# -- SHORT (byron) -- Last name duplicated; won't print
#### APPOINTED/WON SPECIAL ~ SENATE:
# -- ENNIS (bruce, via H)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 2 bill(s) without a sponsor
#     session chamber   N AIC ABC PASS LAW
# 1 2009_2010       H 496 355 376  314 269
# 2 2009_2010       S 321 208 250  222 207
#### APPOINTED/WON SPECIAL ~ HOUSE:
# -- BRIGGS KING (ruth)
# -- KOVACH (thomas)
#### APPOINTED/WON SPECIAL ~ SENATE:
# -- BOOTH (joseph, via H)
# -- ENNIS (bruce, T - 1, via H)
#### DROP:
# -- mcwilliams, diana m. -- Not in chamber in 2009: https://legis.delaware.gov/AssemblyMember/144/McWilliams
# -- vaughn, james t. -- Not in chamber in 2009: https://legis.delaware.gov/AssemblyMember/144/Vaughn


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 2 bill(s) without a sponsor
#     session chamber   N AIC ABC PASS LAW
# 1 2011_2012       H 405 290 297  253 219
# 2 2011_2012       S 273 215 229  212 189
# ***** NO ISSUES AFTER NAME CORRECTIONS ******


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
#     session chamber   N AIC ABC PASS LAW
# 1 2013_2014       H 424 325 334  287 261
# 2 2013_2014       S 267 205 226  203 182
### DROP:
# *** For all three, elections were held in Nov 2012 to fill replacement 
# *** (This may have been a redistricting thing -- all senate districts had an election that year...)
# -- connor, dorinda a. -- Not in chamber in 2013: https://legis.delaware.gov/AssemblyMember/146/Connor
# -- booth, joseph w. -- Not in chamber in 2013: https://legis.delaware.gov/AssemblyMember/146/Booth
# -- bunting, george h. jr. -- Not in chamber in 2013: https://legis.delaware.gov/AssemblyMember/146/Bunting


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 2 bill(s) without a sponsor
#     session chamber   N AIC ABC PASS LAW
# 1 2015_2016       H 443 336 343  274 239
# 2 2015_2016       S 291 223 237  209 190
### APPOINTED/WON SPECIAL ~ HOUSE:
# -- BENTZ (david)
### DROP:
# -- venables, robert l. sr. -- had to run in 2012 and 2014; lost 2014, but still showing up because won 2012 --> DROP


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
#     session chamber   N AIC ABC PASS LAW
# 1 2017_2018       H 481 389 379  315 290
# 2 2017_2018       S 265 199 210  184 161
### WON SPECIAL ~ SENATE:
# -- HANSEN (stephanie, via H)
### DROP:
# -- halllong, bethany -- resigned January 17, 2017 after being elected Lt. Gov. = Only in office 10 days



# filter(klarner, grepl("hansen", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 19 & sen == 1 & outcome == "w") %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)



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
# name = still_missing[3]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, missing, still_missing)


### Fix Missing
name_matches <- data.frame(LES_name = 'ennis', k_name = 'ennis, bruce c.')
name_matches <- add_row(name_matches, LES_name = 'king, s', k_name = 'king, ruth briggs')
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
LES[LES$sponsor == "hansen", c('party', 'sponsor')] <- list('d', "hansen, stephanie")
# LES[LES$sponsor == "zzzzzzzz", c('party', 'sponsor')] <- c('zzzzz', "zzzzzzz")

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)
# rm(fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

# *** NONE NEEDED AT PRESENT ***

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
# LES[LES$sponsor %in% c('zzzzzzzz'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches:
# -- Evelyn 'Tina' Fallon; 
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1991_1992', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('atkin', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

### Lot of the missing are special election winners in 2016+
name_matches <- data.frame(LES_name = 'king, ruth briggs', SM_name = 'Briggs King, Ruth')
#name_matches <- add_row(name_matches, LES_name = 'bentz, david', SM_name = 'zzzzzzz')
# ** Caulk: SM have him as switching to indep in 1997 but didn't happen until Feb. 2005 --> Using the record with most time coverage
name_matches <- add_row(name_matches, LES_name = 'caulk, wallace jr.', SM_name = 'Caulk, G. Wallace Jr.')
# ** Hudson: 2015-16 on 2nd row
name_matches <- add_row(name_matches, LES_name = 'hudson, deborah d.', SM_name = 'Hudson, Deborah')
# ** Smith: Not recorded as marshall in data but is her maiden name
name_matches <- add_row(name_matches, LES_name = 'smith, melanie george', SM_name = 'Marshall, Melanie George')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

##########
### MORE DETAILED FIXES
##########

### Dennis E. Williams AND Dennis P. Williams
LES[LES$sponsor == 'williams, dennis e.',]$SM_name <-  ideo[ideo$name == 'Williams, Dennis' & ideo$house2014 %in% 1,]$name
LES[LES$sponsor == 'williams, dennis e.',]$SM_party <- ideo[ideo$name == 'Williams, Dennis' & ideo$house2014 %in% 1,]$party
LES[LES$sponsor == 'williams, dennis e.',]$np_score <- ideo[ideo$name == 'Williams, Dennis' & ideo$house2014 %in% 1,]$np_score
LES[LES$sponsor == 'williams, dennis p.',]$SM_name <-  ideo[ideo$name == 'Williams, Dennis' & ideo$house2003 %in% 1,]$name
LES[LES$sponsor == 'williams, dennis p.',]$SM_party <- ideo[ideo$name == 'Williams, Dennis' & ideo$house2003 %in% 1,]$party
LES[LES$sponsor == 'williams, dennis p.',]$np_score <- ideo[ideo$name == 'Williams, Dennis' & ideo$house2003 %in% 1,]$np_score


########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########
# filter(LES, grepl("atkins", sponsor)) %>% select(1:7, party, SM_name, SM_party)

#### John Atkins -- Switched R to D in pre 2008 election
LES[LES$sponsor == 'atkins, john c.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Atkins, John' & ideo$party == 'D',]$name
LES[LES$sponsor == 'atkins, john c.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Atkins, John' & ideo$party == 'D',]$party
LES[LES$sponsor == 'atkins, john c.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Atkins, John' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'atkins, john c.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Atkins, John' & ideo$party == 'R',]$name
LES[LES$sponsor == 'atkins, john c.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Atkins, John' & ideo$party == 'R',]$party
LES[LES$sponsor == 'atkins, john c.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Atkins, John' & ideo$party == 'R',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################

##### Drop Nicknames
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

##### Specific Name Changes
LES[LES$sponsor == "paradee, w. charles iii",]$sponsor <- "paradee, william charles iii"


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 2003 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(2009:2020) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2003:2008) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 2003 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(2003:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
#LES[as.numeric(substring(LES$term,1,4)) %in% c(2012:2019) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


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

