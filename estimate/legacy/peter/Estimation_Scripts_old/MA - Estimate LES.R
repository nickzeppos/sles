

#####################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** MASSACHUSETTS *** BY SESSION
#####################################

# ******************
# ------> IF RE-SCRAPE: Fix Bill Numbers like 'S.52\n                    C' in 190th -- there's six of them -- removing below
#* ******************

#####################
# SPECIAL SESSIONS: 
# --- Extremely rare + if they do happen, appear to be lumped into two-year general assembly set of bills
# BILL INTRODUCTION: 
# --- Lots of bills with no sponsor in 186th; some missing in 187th
# --- See: https://www.csgmidwest.org/policyresearch/0417-bill-introduction.aspx
# --- If 'by request' shows up, those are filed by by constituents or some outside group
# --- From State Glossary: "Indicates that the legislative document does not have a legislator as a petitioner and is being filed by a legislator 
# under a citizen's right of free petition." ~ https://malegislature.gov/StateHouse/Glossary#b
# MA LEG PROCESS:
# --- https://www.massbar.org/advocacy/legislative-activities/the-legislative-process
# --- Bills often referred to JOINT committees, and this is where hearings are recorded so need to keep those
# COMM HEARINGS:
# --- "Every bill must then have a public hearing held by the committee to which it is assigned." -- Per: https://www.naswma.org/page/BilltoLaw
# --- --> WOuld imply being referred is AIC...
####################
#### ~~~~~~~~~~ NOTES ~~~~~~~~
## (1) **** FOR ATTRIBUTING CREDIT **** : Sponsor = Sponsor; if blank, use Filed By column
## --- Dropping ALL by request bills -- Legislators are basically required to do submit these under right of free petition
## (2) What about Initiative Petitions? --> https://malegislature.gov/Bills/186/H4454
## ----- Ultimately passed by people... does that make Jay Kaufman effective? ---> Dropping
## (3) Relatedly: what about "Proposal for Constitutional Amendment'
## ----- Needs to be passed 2 years in a row with a house election in between, then by voters ------> Dropping


rm(list=ls())
options(stringsAsFactors = FALSE, scipen = 100)

library(tidyr)
library(dplyr)
library(stringr)
library(ggplot2)
library(humaniformat)
library(purrr)
library(readxl)
library(glue)
library(readr)

this_state <- 'MA'
min_year <- 2009
keep_types <- c("Bill")
spec_elec_codes <- c('s')
sen_term_length <- 2

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
sessions <- gsub('.+Details_|.csv', '', bill_files)
rm(data_files, bill_files)

#### DROP NEW DATA 2019+ for now
sessions <- sessions[-which(sessions %in% c("191st", "192nd"))]

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("~/Dropbox/Data/State Legislative Data/Commem Bills/{this_state}_Commem_Bills.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- read.csv("~/Dropbox/Data/State Legislative Data/Significant Bills/SS_Data/SS_Bills.csv")
SS_bills <- SS_bills %>% 
  filter(state == this_state) %>%
  rename(bill_id = bill_num) %>%
  mutate(term = ifelse(year %% 2 == 1, paste0(year, "_", year + 1), paste0(year - 1, "_", year)),
         bill_id = toupper(bill_id),
         bill_id = gsub("B", "", bill_id), # MA uses H/S instead of HB/SB
         SS = 1) %>%
  select(state, term, year, bill_id, everything())

#### Check SS BIll Types ---> Correct to ensure standardized with Data Format
# table(SS_bills$bill_type)
# filter(SS_bills, bill_type == "S" | bill_type == "H")

### Load Klarner Data 
load("~/Dropbox/Data/US Election Data/state_leg/klarner_data/196slers1967to2016_20180908.RData")
klarner <- table; rm(table)
klarner$sab <- toupper(klarner$sab)
klarner <- filter(klarner, year > min_year - 5 & sab == this_state)

### Fix Klarner Error -- NOTE: IDs will be off but name is correct over time now
klarner[klarner$cand == "donaghue, eileen m." & klarner$year == 2010, ]$cand <- 'donoghue, eileen m.'

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# s <- sessions[3]

for(s in sessions){
  
  ### Add 19/20 onto years
  t_yrs <- as.numeric(gsub('[a-z]+', '', s)) - 186
  t_yrs <- (t_yrs * 2) + 2009
  t_yrs <- glue("{t_yrs}_{t_yrs + 1}")
  t_yrs <- as.character(t_yrs)
  
  ### Skip Previously Estimated
  if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
    print(glue('. \n ~~~~ SKIPPING {t_yrs} TERM ---> LES scores already estimated! \n.'))
    next
  }
  
  ###### SESSION IN PROGRESS
  print(glue('\n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))
  
  ############### Read in data
  bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{s}.csv")
  bills <- read.csv(bill_path)
  bills$term <- t_yrs
  
  ### Check for duplicates
  if(nrow(bills) != nrow(distinct(bills))){
    cat(" \n ~~~> DUPLICATE BILLS \n\n .")
    break
  }
  
  ######## Standardize the Bill IDs
  bills <- rename(bills, bill_id = bill_number)
  id_parts <- str_split_fixed(bills$bill_id, "\\.", 2)
  id_parts[,2] <- gsub('\n.+', '', id_parts[,2])
  bills$bill_id <- paste0(id_parts[,1], str_pad(id_parts[,2], 4, pad = "0"))
  rm(id_parts)
  
  ############### Drop Resolutions, Messages, Communications, Reports
  ## ** Not Distinguished by bill ID --- use scraped bill type
  bills <- filter(bills, bill_type %in% keep_types | grepl('^Bill', bill_type))
  
  ############### Standardize Sponsors
  ### Sponsor = Sponsor By; if blank, use Filed by column
  bills$filed_by_adj <- ifelse(grepl(',', bills$filed_by), str_trim(paste(gsub('.+, ', '', bills$filed_by), gsub(',.+', '', bills$filed_by))), bills$filed_by)
  bills$LES_sponsor <- ifelse(bills$sponsor == "", bills$filed_by_adj, bills$sponsor)
  ### Use First Cosponsor to fill in remaining? Not clear when this works and when it doesn't (e.g., sometimes can be missing agency?)
  #filter(bills, LES_sponsor == "" & cosponsors != "") %>% select(filed_by, LES_sponsor, sponsor, cosponsors) %>% View()
  #bills$LES_sponsor <- ifelse(bills$LES_sponsor == "", gsub(';.+', '', bills$cosponsors), bills$LES_sponsor)
  
  #### Split MultiSponsors
  if(any(grepl(' , ', bills$LES_sponsor))){
    bills$LES_sponsor <- ifelse(grepl(' , ', bills$LES_sponsor), bills$filed_by_adj, bills$LES_sponsor)  
    if(any(grepl(' , ', bills$LES_sponsor))){
      print(" --> Split Multipsponsors --- BREAK")
      break
    }
  }

  ### Standardize
  bills$LES_sponsor <- str_trim(gsub(' +', ' ', tolower(bills$LES_sponsor)))
  # sort(table(bills$LES_sponsor))
  
  #### Are there committee bills? If so, fix here:
  if(any(grepl('joint comm|house comm|senate comm|\\(h\\)|\\(s\\)|\\(j\\)|conference|committee', bills$LES_sponsor))){
    drop_sum <- sum(grepl('joint comm|house comm|senate comm|\\(h\\)|\\(s\\)|\\(j\\)|conference|committee', bills$LES_sponsor))
    print(glue("-----> Dropping {drop_sum} Committee Sponsored Bills"))
    bills <- filter(bills, !grepl('joint comm|house comm|senate comm|\\(h\\)|\\(s\\)|\\(j\\)|conference|committee', LES_sponsor))
  }
  
  ### Drop Assorted Agencies, Etc 
  drop_assorted <- c("exhibition center", "campaign finance", "budget", "bond bill", 'life sentences', 'transportation',
                     "authorities", "insurance", "department", "safety", "improvements", 'consumer', "house of rep", "senate",
                     "financing", "firearms", "appropriations", "massachusetts", "perac", "civic engagement", "regulating", 
                     "renewable energy", "commission", "registration", "criminal justice", "auditor of the commonwealth", 
                     "retirement", "inspector", "office of", "opportunity")
  bills <- filter(bills, !grepl(paste(drop_assorted, collapse = "|"), LES_sponsor))

  ### DROPPING BY REQUEST BILLS --- (Seems that legislators don't have a choice...? See glossary entry above)
  bills <- filter(bills, !(grepl('by request', tolower(LES_sponsor)) | grepl('by request', tolower(sponsor)) |grepl('by request', tolower(presenter)))  )
  
  #### Drop Governors
  if(t_yrs %in% c("2009_2010", "2011_2012", "2013_2014")){
    bills <- filter(bills, LES_sponsor != 'deval l. patrick')
  }
  if(t_yrs %in% c("2015_2016", "2017_2018", "2019_2020", "2021_2022")){
    bills <- filter(bills, LES_sponsor != 'charles d. baker')
  }
  
  ### Drop Secretary of the Commonwealth + Lieutenant Gov
  bills <- filter(bills, LES_sponsor != 'william francis galvin')
  bills <- filter(bills, LES_sponsor != 'timothy p. murray')
  
  #### Name Fixes
  if(any(grepl('jr. hart', bills$LES_sponsor))){
    bills[bills$LES_sponsor == "jr. hart",]$LES_sponsor <- "john a. hart jr."  
  }
  if(any(grepl('jr. humason', bills$LES_sponsor))){
    bills[bills$LES_sponsor == "jr. humason",]$LES_sponsor <- "donald f. humason, jr." 
  }
  if(any(bills$LES_sponsor == "bradley h. jones")){
    bills[bills$LES_sponsor == "bradley h. jones",]$LES_sponsor <- "bradley h. jones, jr."   
  }
  if(any(bills$LES_sponsor == 'susannah m. whipps')){
    bills[bills$LES_sponsor == "susannah m. whipps",]$LES_sponsor <- "susannah m. whipps-lee"     
  }
  if(any(bills$LES_sponsor == 'harold p. naughton')){
    bills[bills$LES_sponsor == "harold p. naughton",]$LES_sponsor <- "harold p. naughton, jr."     
  }
  if(any(bills$LES_sponsor == "shaunna o'connell")){
    bills[bills$LES_sponsor == "shaunna o'connell",]$LES_sponsor <- "shaunna l. o'connell"     
  }
  if(any(bills$LES_sponsor == "cynthia s. creem")){
    bills[bills$LES_sponsor == "cynthia s. creem",]$LES_sponsor <- "cynthia stone creem"     
  }
  if(any(bills$LES_sponsor == "john j. lawn")){
    bills[bills$LES_sponsor == "john j. lawn",]$LES_sponsor <- "john j. lawn, jr."     
  }
  if(any(bills$LES_sponsor == "jeffrey roy")){
    bills[bills$LES_sponsor == "jeffrey roy",]$LES_sponsor <- "jeffrey n. roy"     
  }
  
  if(any(grepl('^jr.', bills$LES_sponsor))){
    print("----> FIX JR NAMES:")
    print(filter(bills, grepl("^jr.", LES_sponsor)) %>% select(LES_sponsor) %>% distinct() %>% unlist())
  }
  
  #### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
  bills$LES_sponsor <- gsub('á', 'a', bills$LES_sponsor)
  bills$LES_sponsor <- gsub('é', 'e', bills$LES_sponsor)
  bills$LES_sponsor <- gsub('ó', 'o', bills$LES_sponsor)
  bills$LES_sponsor <- gsub('í', 'i', bills$LES_sponsor)

  ###### CHeck Missing Sponsors
  # filter(bills, LES_sponsor == "") %>% View()
  if(nrow(filter(bills, LES_sponsor %in% c('', 'none'))) > 0){
    print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor %in% c('', 'none')))} bills without a sponsor"))
    bills <- filter(bills, !(LES_sponsor %in% c('', 'none')))     
  }
  
  
  ###################
  ###### Merge in S&S Bills
  ###################
  # *** For MASSACHUSSETTS: Specials folded into main biennial term
  
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
  
  ####################################################################
  ############### Code Commemorative
  ###################################################################
  
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session'))
  # table(bills$commem)
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  ############### Code Bill History
  bill_hist_path <- gsub('_Bill_Details', '_Bill_Histories', bill_path) 
  bill_hist <- read.csv(bill_hist_path)
  bill_hist$term <- t_yrs
  
  ######## Standardize BillHist Bill IDs
  bill_hist <- rename(bill_hist, bill_id = bill_number)
  id_parts <- str_split_fixed(bill_hist$bill_id, "\\.", 2)
  bill_hist$bill_id <- paste0(id_parts[,1], str_pad(id_parts[,2], 4, pad = "0"))
  rm(id_parts)
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  # *** CHECK: "Reported (in part) by a new draft.+"
  
  # [a-z]+heduled --- Catches scheduled and rescheduled
  aic_t <- c('reported favorab', '^recommend', 'committee recomm', 'ought not to pass', 'ought to pass', 
             '^committee reported that', 'public hearing', 'hearing date', 'hearing [a-z]+heduled',
             'discharged to.+comm')
  # reported [a-z]+ catches everything except "reported (in part) by H01234"
  # discharged to = committeed decides bill is out of its jurisdiction and moves it elsewhere (https://malegislature.gov/StateHouse/Glossary#D)
  abc_t <- c('reported [a-z]+', 'orders of the day', '^amendment', 'motion to suspend', 
             'read second', 'read third', 'third read')
  # 'rules suspended' --> errors -- often happens to extend reporting dates
  pc_t <- c('passed.+engrossed', 'enacted') # If resolves included, need 'resolve passed'
  law_t <- c('signed by the gov', 'chapter [0-9]+ +of', 'became law pursant to')
  
  ### Output Matrix
  #all_bill_stages <- data.frame(matrix(nrow = 0, ncol = 10))
  #colnames(all_bill_stages) <- c("bill_id", "term", "session", "LES_sponsor", "introduced", "action_in_comm", "action_beyond_comm", "passed_chamber", "law", "bill_url")
  
  ### Code History Stages for Each Bill --- Subset Hist File, Code, Save, Repeat
  # ---> Add 'Joint' Chamber below as this is where comm hearings are often recorded (many MA comms are joint)
  options(warn = 2)
  for(i in 1:nrow(bills)){
    b_id = bills[i,]$bill_id
    s_id = bills[i,]$session
    b_spon = bills[i,]$LES_sponsor
    hist_sub <- filter(bill_hist, bill_id == b_id)
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t, 
                                      ignore_chamber_switch = TRUE, add_chamb = "Joint")
    bill_stages$bill_url <- bills[i,]$bill_url
    if(i == 1){
      all_bill_stages <- bill_stages
    }else{
      all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
    }
    #print(i)
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
    group_by(LES_sponsor, term, chamber) %>%
    summarize(num_sponsored_bills = n(), 
              sponsor_pass_rate = sum(passed_chamber) / n(),
              sponsor_law_rate = sum(law) / n()) %>%
    ungroup()
  
  ######## Cosponsorship Info --- 
  all_sponsors$num_cosponsored_bills <- NA
  bills$cospon_match <- gsub(' +', ' ', paste(bills$LES_sponsor, tolower(bills$cosponsors), sep = '; '))
  for(i in 1:nrow(all_sponsors)){
    c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
    ### Adjusting names + Adding Fix Middle/Maiden Names (e.g., Cynthin Stone Creem labeled as Cynthia S. Creem in sponsors)
    ### Also adding match for middle initial when missing (not standardized across filed by and cosponsors)
    search_name <- str_replace_all(all_sponsors[i,]$LES_sponsor, "(\\W)", "\\\\\\1")
    if(grepl("^[a-z]+ [a-z]+ ", all_sponsors[i,]$LES_sponsor)){
      search_name <- paste0(search_name, "|", gsub("\\\\ .+\\\\ ", " [a-z]\\\\. ", search_name))
    }else if(!grepl(" [a-z]\\. ", all_sponsors[i,]$LES_sponsor)){
      search_name <- paste0(search_name, "|", gsub(" ", " [a-z]\\\\. ", search_name))
    }else{
      search_name <- paste0(search_name, "|", gsub("\\\\ [a-z]\\\\.\\\\ ", " ", search_name))
    }
    if(grepl(" jr\\.| sr\\.| ii+", all_sponsors[i,]$LES_sponsor)){
      search_name <- paste0(search_name, "|", gsub("\\\\ jr\\\\.|\\\\ sr\\\\.|\\\\ ii+", "", search_name))
    }
    if(all_sponsors[i,]$LES_sponsor == "harriett l. chandler"){
      search_name <- paste0(search_name, "|", "harriette l\\. chandler")
    }
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(search_name, tolower(c_sub$cospon_match)))
    ### ONLY need to adjust this way if sponsored and cosponsored column are the same
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  }
  bills <- select(bills, -cospon_match)
  # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))
  # filter(bills, grepl("connell", tolower(cosponsors))) %>% 
  #   mutate(cosponsors = gsub("Connell.+", "Connell", cosponsors)) %>% 
  #   mutate(cosponsors = gsub(".+;", "", cosponsors)) %>% pull(cosponsors) %>% unique()
  
  #######
  ## CLEAN NAMES
  ##########
  all_sponsors <- left_join(all_sponsors, map_df(all_sponsors$LES_sponsor, parse_names), by = c("LES_sponsor" = "full_name")) %>%
    select(-salutation) %>%
    mutate(last_name = gsub(',$', '', last_name)) %>%
    arrange(chamber, LES_sponsor) %>%
    distinct() ## If someone switches chambers, merge above will double them up
  
  #### Update Last Names for Matching 
  if(any(all_sponsors$LES_sponsor == 'cheryl a. coakley-rivera')){
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "cheryl a. coakley-rivera", "rivera", all_sponsors$last_name)
  }
  if(any(all_sponsors$LES_sponsor == 'marie p. st. fleur')){
    all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "marie p. st. fleur", "saintfleur", all_sponsors$last_name)
  }
  
  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  ### If Term is 2008-2009, e.g., including 2007, 2008, and Specials for 2009 (when present)
  elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% elec_year:(elec_year + 1) | (year == elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% (elec_year - sen_term_length + 2):(elec_year + 1) | (year == elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)) %>% mutate(term = s); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)
  
  ################################################################################
  ############## Match Sponsors Names to Klarner Data, Forgoing Function because mostly only have last name
  ################################################################################
  
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(!is.na(all_sponsors$first_name), paste0(", ", all_sponsors$first_name), '')))
  klarner_sub$match_name <- tolower(unlist(str_extract_all(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]+')))
  
  ### Drop Duplicated Klarner Candidats
  klarner_sub <- arrange(klarner_sub, desc(year)) %>% filter(!duplicated(cand))
  
  all_sponsors$elec_year <- all_sponsors$klarner_id <- all_sponsors$klarner_name <- NA
  all_sponsors$klarner_name <- as.character(all_sponsors$klarner_name)
  all_sponsors$klarner_id <- as.double(all_sponsors$klarner_id)
  all_sponsors$elec_year <- as.double(all_sponsors$elec_year)
  ### Loop though and Match
  for(i in 1:nrow(all_sponsors)){
    k_matches <- filter(klarner_sub, last_name == tolower(all_sponsors[i,]$last_name))
    
    ## Acccount for lack of punctuation or spaces in Klarner Data
    if(nrow(k_matches) == 0 & grepl("-|'| |\\.", all_sponsors[i,]$last_name)){
      k_matches <- filter(klarner_sub, last_name == gsub("-|'| ", '', tolower(all_sponsors[i,]$last_name)))
    }
    
    ## Check Name Switch [klarner_match-data_match]
    if(nrow(k_matches) == 0 & grepl("-", all_sponsors[i,]$last_name)){
      k_matches <- filter(klarner_sub, last_name == gsub("-.+", '', tolower(all_sponsors[i,]$last_name)))
    }   
    
    ## Account for chamber if multiple matches
    if(nrow(k_matches) > 1){
      k_matches <- filter(klarner_sub, last_name == tolower(all_sponsors[i,]$last_name) & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
    }
    
    ### Save and Cross-Check
    if(nrow(k_matches) == 1){
      all_sponsors[i, ]$klarner_name <- k_matches$cand
      all_sponsors[i, ]$klarner_id <- k_matches$candid
      all_sponsors[i, ]$elec_year <- k_matches$year     
    } else if(nrow(k_matches) > 1 & length(unique(k_matches$cand)) != 1){
      ## Check Last, First
      m_sub <- grep(all_sponsors[i,]$match_name, k_matches$match_name)
      ## Check First Name
      if(length(m_sub) == 0){
        match_name2 <- tolower(paste0(all_sponsors[i,]$last_name, ", ", substring(all_sponsors[i,]$first_name, 1, 1)))
        m_sub <- grep(match_name2, paste0(k_matches$last_name, ", ", substring(gsub('.+, ', '', k_matches$match_name), 1, 1)))
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
        print(glue("MULTIPLE MATCHES ::: {all_sponsors[i,]$last_name} ::: {i}"))
      }
    } else if(nrow(k_matches) > 1 & length(unique(k_matches$cand)) == 1){
      all_sponsors[i, ]$klarner_name <- unique(k_matches$cand)
      all_sponsors[i, ]$klarner_id <- unique(k_matches$candid)
      eyear <- as.numeric(str_split(t_yrs, "\\_")[[1]][1]) - 1
      all_sponsors[i, ]$elec_year <- k_matches[which(abs(k_matches$year - eyear) == min(abs(k_matches$year - eyear))),]$year
      rm(eyear)
    } else if(any(grepl(all_sponsors[i,]$match_name, klarner_sub$match_name))){
      m_sub <- grep(all_sponsors[i,]$match_name, klarner_sub$match_name)
      all_sponsors[i, ]$klarner_name <- klarner_sub[m_sub,]$cand
      all_sponsors[i, ]$klarner_id <- klarner_sub[m_sub,]$candid
      all_sponsors[i, ]$elec_year <- klarner_sub[m_sub,]$year     
    } else{
      print(glue("MISSING MATCH ::: {all_sponsors[i,]$last_name} ::: {i}"))
    }
  }
  
  #### Fix Mismatches
  if(t_yrs == "2011_2012"){
    all_sponsors[all_sponsors$LES_sponsor == "timothy p. murray", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "2013_2014"){
    all_sponsors[all_sponsors$LES_sponsor == "daniel hunt", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "2015_2016"){
    all_sponsors[all_sponsors$LES_sponsor == "thomas p. walsh", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "2017_2018"){
    all_sponsors[all_sponsors$LES_sponsor == "john barrett", c("klarner_name", "klarner_id", "elec_year")] <- NA
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
  if(t_yrs == "2013_2014"){
    km <- filter(km, cand != 'spiliotis, joyce a.')
  }else if(t_yrs == "2015_2016"){
    km <- filter(km, cand != "beaton, matthew a.")
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
    mutate(sponsor = ifelse(!is.na(klarner_name), klarner_name, tolower(match_name)), 
           session = t_yrs) %>%
    select(sponsor, data_name, klarner_name, klarner_id, chamber, session, elec_year, num_sponsored_bills, sponsor_pass_rate, sponsor_law_rate, num_cosponsored_bills) %>%
    arrange(chamber, sponsor)
  
  ##############################################
  ######## Estimate Scores + Add in Relatd Variables
  ###############################################
  ### Check if bills in data without an ID'd sponsor
  # filter(bills, !(bills$primary_sponsor %in% legis_data$data_name))
  bills <- select(bills, -sponsor) %>%
    rename(sponsor = LES_sponsor) %>%
    mutate(chamber = substring(bill_id, 1, 1))
  
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
  
  
  cat(glue(". \n  **************** SESSION {t_yrs} ~~> DONE  ***********************"))
  
}
################# ****** END LOOP

#agg_stats, matches
rm(all_sponsors, bills, legis_data, SS_bills, elec_year, i, keep_types, drop_assorted)
rm(k_matches, klarner_sub, m_sub, match_name2, km, bill_path, s, t_yrs, drop_sum, calc_LES)
rm(commem_bills, c_sub, search_name)

#########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by SPECIAL ELECTION --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
## Member Lists:
###########
# filter(klarner, grepl("galvin, will", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(klarner, ddez == 29 & sen == 1 & outcome == 'w') %>% arrange(year, cand) %>% select(cand, year, sen, etype, outcome, ddez, candid)

# #~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 session! ~~~~~~~~~~~~~~ 
# -----> Dropping 2 Committee Sponsored Bills
# -----> Dropping 613 bills without a sponsor
### WON SPECIAL ~ HOUSE:
# -- michlewitz
### IN HOUSE:
# -- galvin, william c.

# #~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 session! ~~~~~~~~~~~~~~ 
# -----> Dropping 444 Committee Sponsored Bills
# -----> Dropping 97 bills without a sponsor
#### WON SPECIAL:
# Alicea --> Exact tie led to special that alicea lost, but in between he held onto his seat
# Orrall

## ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 session! ~~~~~~~~~~~~~~ 
# -----> Dropping 595 Committee Sponsored Bills
# -----> Dropping 64 bills without a sponsor
#### WON SPECIAL:
# -- CULLINANE
# -- RYAN
# -- LIVINGSTONE
# -- COLE
# -- DOLE
# -- MATEWSKY
### IN CHAMBER:
# Stephen Smith resigned after pleading guilty to charges
### DROP:
# Joyce Spiliotis passed away in late November 2012 -- won but was never seated

# #~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 session! ~~~~~~~~~~~~~~ 
# -----> Dropping 589 Committee Sponsored Bills
# -----> Dropping 123 bills without a sponsor
### WON SPECIAL:
# -- MADARO
# -- KANE
### IN CHAMEBER
# Carlo Basile 
### DROP
# Matthew Beaton resigned to become MA Sec of Energy

# # ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 session! ~~~~~~~~~~~~~~ 
# -----> Dropping 601 Committee Sponsored Bills
# -----> Dropping 74 bills without a sponsor
### WON SPECIAL
# -- VARGAS
# -- HAWKINS
# -- FRIEDMAN
# -- TRAN
# -- BARRETT
### IN CHAMBER
# Brian Dempsey -- Resigned July 19, 2017

##########################################################################################################
##########################################################################################################
##########################################################################################################
##########################################################################################################
##########################################################################################################
##########################################################################################################

### Re-Load LES Data
LES_paths <- list.files('.', full.names = TRUE)
LES_paths <- LES_paths[!grepl('Archive|_All|Merged', LES_paths)]

LES <- LES_paths %>%
  lapply(read_csv, col_types = cols()) %>%
  bind_rows 

rm(LES_paths)

####### Pre-Name Fixes
LES[LES$sponsor == 'cullinane, daniel',]$sponsor <- 'cullinane, dan'

####### Fill in Missing Data from Candidates Elected in Specials using Subsequent Observations
missing <- unique(LES[is.na(LES$klarner_id),]$sponsor)
for(name in missing){
  name_sub <- filter(LES, grepl(glue("^{name}"), sponsor))
  # If there is only ONE UNIQUE id that matches the name
  if(any(!is.na(name_sub$klarner_id)) & length(unique(na.omit(name_sub$klarner_id))) == 1 ){
    LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$sponsor <- unique(name_sub[!is.na(name_sub$klarner_id),]$sponsor) 
    LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$klarner_name <- unique(na.omit(name_sub$klarner_name))
    LES[grepl(name, LES$sponsor) & is.na(LES$klarner_id),]$klarner_id <- unique(na.omit(name_sub$klarner_id))   
    print(glue(' ~~ {name} ~~ Matched to --> {unique(na.omit(name_sub$klarner_name))}'))
  } else {
    k_sub <- filter(klarner, grepl(name, cand))
    if(length(unique(k_sub$cand)) == 1){
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$sponsor <- unique(k_sub$cand) 
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$klarner_name <- unique(k_sub$cand) 
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_id),]$klarner_id <- unique(k_sub$candid) 
      print(glue(' ~~ {name} ~~ Matched to --> {unique(k_sub$cand) }'))  
    }
  }
}

### Manual Fixes
# LES[LES$sponsor == "cullinane, daniel" & LES$session == "1997_1998",]$klarner_id <- 101754
# LES[LES$sponsor == "cullinane, daniel" & LES$session == "1997_1998",]$klarner_name <- "bullard, willis"
# LES[LES$sponsor == "cullinane, daniel" & LES$session == "1997_1998",]$sponsor <- "bullard, willis"

### FOUR Still missing = Elected in Late Specials
still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[1]; print(name)
# filter(LES, grepl(name, sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, session, chamber)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name)


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
rm(check_dup, k_sub, name_sub, missing, still_missing)


########################################################################################################################################################
########################################################################################################################################################
######## AGGREGATE AND MATCH TO EXTERNAL
########################################################################################################################################################
########################################################################################################################################################

### Klarner State Leg. Election Data --- Using Adjusted File From Top
klarner_sub <- filter(klarner, sab == this_state & year >= min_year - 2 & outcome == 'w')
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

#### *** 2017_2018: If any of below run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "barrett, john",]$party <- 'd'
LES[LES$sponsor == "hawkins, james",]$party <- 'd'
LES[LES$sponsor == "vargas, andres",]$party <- 'd'
LES[LES$sponsor == "friedman, cindy",]$party <- 'd'

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
ideo$match_name <- tolower(ideo$name)
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

### Fix
# --> Both Lewis's match to Lewis, J. --> But Both are D's, Lewis, J. is R --- so probably neither
# --> John D. Keenan = 7th Essex Dist, which jatches to Keenan, John Jr.
LES[LES$sponsor %in% c('lewis, jason m.', 'lewis, jack patrick', 'john d. keenan'), c('SM_name', 'SM_party', 'np_score')] <- NA


### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches: 
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

#### Mike Connolly != Edward Connolly; Daniel Cahill != Michael Cahill
LES[LES$sponsor %in% c('connolly, mike', 'cahill, daniel h.'), c('SM_name', 'SM_party', 'np_score')] <- NA

#####################
### MANUAL FIXES -- SM Data only goes through 2016 so matches after that are from earlier period

# ******************** LOTS of MISSINGNESS HERE *************************
# -----------> No HOUSE Data from 2009 -- 2016 -------------------------
# -----------> No SENATE Data from 2013 -- 2016 -------------------------
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(LES, is.na(np_score) & chamber == "Senate") %>% filter(!(term %in% c('2013_2014', '2015_2016', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('hedlund', tolower(name))) %>% as.data.frame() %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)


name_matches <- data.frame(LES_name = 'hedlund, robert l.', SM_name = 'Hedlund Jr, Robert L')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

# #### Manual Edits (Needs more precision...)
# LES[LES$sponsor == 'scott, martha',]$SM_name <- ideo[ideo$name == 'Scott' & ideo$senate2001 %in% 1,]$name
# LES[LES$sponsor == 'scott, martha',]$SM_party <- ideo[ideo$name == 'Scott' & ideo$senate2001 %in% 1,]$party
# LES[LES$sponsor == 'scott, martha',]$np_score <- ideo[ideo$name == 'Scott' & ideo$senate2001 %in% 1,]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 2009 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(2009:2020) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzzz) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 2009 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(2009:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzzz) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1

############################################
######## NAME STANDARDIZATION + Data Fixes
###########################################

##### Drop Nicknames
# filter(LES, grepl("\\(", sponsor)) %>% distinct(sponsor)
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]$', sponsor)) %>% distinct(sponsor)
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

#### Name Fixes
LES[LES$sponsor == 'oconnorives, kathleen a.',]$sponsor <- 'ives, kathleen a. oconnor'
LES[LES$sponsor == 'reinstein, kathianne',]$sponsor <- 'reinstein, kathi-anne'


##############################################
###  Save
##############################################

### Check Missingnes
LES %>%
  group_by(chamber, term) %>%
  summarize(
    Election_Data_Missing = round(sum(is.na(klarner_id))/n(), 2),
    CM_Leadership_Missing = round(sum(is.na(SpeakerHouse))/n(), 2),
    Ideology_Missing = round(sum(is.na(np_score))/n(), 2)) # %>% View()

###### SAVE Merged File
colnames(LES)
if(!dir.exists("Merged")){dir.create("Merged")}
write.csv(LES, glue("Merged/{this_state}_LES_All_M.csv"), row.names = FALSE)

### Save by TERM
for(t in unique(LES$term)){
  LES_sub <- filter(LES, term == t)
  write.csv(LES_sub, glue("Merged/{this_state}_LES_{t}_M.csv"), row.names = FALSE)  
}

rm(LES_sub)

######################################
#### Plot
#######################################

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
  scale_color_manual(values=c("dodgerblue2",  'gray50', "red2", 'gray50'))

##### CHECK OUTLIERS
# filter(LES, party == 'd' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')


