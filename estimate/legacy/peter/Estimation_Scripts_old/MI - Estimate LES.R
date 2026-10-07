

##############################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** MICHIGAN *** BY LEGISLATIVE TERM
###############################################################################

## *********** For codebook: 1995-1996 NO RESOLUTIONS, ONLY BILLS *************

#####################
##### NOTES
#####################
# SPECIAL SESSIONS
# --- If occur, folded into the main session; bill numbers do not re-start
# PROCESS:
# --- 
# BILL SPONSORSHIP
# --- 
# MEMBERS
# --- Details of all members of Michigan legislature: https://mdoe.state.mi.us/legislators/Legislator/ViewLegislators
####################

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

this_state <- 'MI'
min_year <- 1995
keep_types <- c("HB", "SB")
spec_elec_codes <- c('s', 'gs')
sen_term_length <- 4 #### -- TERMS ARE STAGGERED!

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
terms <- gsub('.+Details_|.csv', '', bill_files)
rm(data_files, bill_files)

#### DROP 2019+ FOR NOW
terms <- terms[-which(terms %in% c("2019_2020", "2021_2022"))]

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("~/Dropbox/Data/State Legislative Data/Commem Bills/{this_state}_Commem_Bills.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
# ** HBs start at 4000, SBs start at 1 + Bills continue to increment across years in term (e.g., 1995-HB-4900, 1996-HB-4901)
# ** --> Create an adjusted version of the id without the year in the bills df and merge on that **
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


###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t_yrs = terms[12]

for(t_yrs in terms){
  
  ### Skip Previously Estimated
  if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
    print(glue('. \n \n ~~~~ SKIPPING {t_yrs} TERM ---> LES scores already estimated! \n \n.'))
    next
  }
  
  ###### SESSION IN PROGRESS
  print(glue('. \n ~~~~ ESTIMATING SCORES FOR THE {t_yrs} session! \n .'))
  
  ############### Read in data
  bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_yrs}.csv")
  bills <- read.csv(bill_path)
  bills$term <- t_yrs
  
  ### Check for duplicates
  if(nrow(bills) != nrow(distinct(bills))){
    cat(" \n ~~~> DUPLICATE BILLS \n\n .")
    break
  }
  
  ######## Standardize the Bill IDs
  # *** Don't actually need the YYYY- in front to be unique.
  bills <- rename(bills, bill_id = bill_number)
  
  ############### Drop Resolutions
  bills <- mutate(bills, bill_type = gsub('-.+', '', gsub('.+[0-9]-', '', bill_id)))
  # table(bills$bill_type)
  bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)
  
  ##########################################
  ###### Standardize Sponsors
  ############################################
  
  #### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
  bills$sponsors <- tolower(bills$sponsors)
  bills$sponsors <- gsub('á', 'a', bills$sponsors)
  bills$sponsors <- gsub('é', 'e', bills$sponsors)
  bills$sponsors <- gsub('ó', 'o', bills$sponsors)
  bills$sponsors <- gsub('í', 'i', bills$sponsors)
  bills$sponsors <- gsub('ñ', 'n', bills$sponsors)
  
  ### First name = sponsor; subsequent = cosponsors (Not all sessions seem to list cosponsors)
  bills$LES_sponsor <- gsub(';.+', '', bills$sponsors)
  bills$LES_sponsor <- str_trim(bills$LES_sponsor)
  # sort(table(bills$LES_sponsor))
  
  #### Name Fixes
  bills$LES_sponsor <- gsub('michael oõbrien', "michael o'brien", bills$LES_sponsor)
  
  ###### CHeck Missing Sponsors
  # filter(bills, LES_sponsor == "") %>% View()
  if(nrow(filter(bills, LES_sponsor == '')) > 0){
    print(glue(" ~~> Dropping {nrow(filter(bills, LES_sponsor == ''))} bills without a sponsor"))
    bills <- filter(bills, LES_sponsor != "")     
  }

  ###################
  ###### Merge in S&S Bills
  ###################
  # *** For MICHIGAN: Specials Folded into Main Sessions --> Merging on Term
  # *** Note: Creating adjusted ID in bills to merge without year (id's continue to increment from odd to even year of session)
  
  bills$bill_id_adj <- gsub('\\d{4}\\-|\\-', '', bills$bill_id)
  H_range <- range(as.numeric(gsub("HB", "", bills[grepl("HB", bills$bill_id),]$bill_id_adj)))
  S_range <- range(as.numeric(gsub("SB", "", bills[grepl("SB", bills$bill_id),]$bill_id_adj)))
  SS_term <- SS_bills %>%
    filter(term == t_yrs) %>%
    ### Constraining to observe range of bills
    filter((bill_type == "HB" & num_only %in% H_range[1]:H_range[2]) | (bill_type == "SB" & num_only %in% S_range[1]:S_range[2])) %>% 
    distinct(term, bill_id, SS)
  
  ### Merge
  bills <- bills %>%
    left_join(SS_term, by = c("bill_id_adj" = "bill_id", "term" = "term")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS)) 
  
  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id" = "bill_id_adj", "term" = "term"))
  
  ####################################################
  ############### Code Commemorative
  ######################################################
  
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session'))
  # table(bills$commem)
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  ######################################################
  ############### Code Bill History
  ######################################################
  
  bill_hist_path <- gsub('_Bill_Details', '_Bill_Histories', bill_path) 
  bill_hist <- read.csv(bill_hist_path)
  bill_hist$term <- t_yrs
  
  ######## Standardize BillHist Bill IDs
  bill_hist <- rename(bill_hist, bill_id = bill_number)
  
  ### Use Journals to Code Action Chamber + Fill in Blanks
  bill_hist$chamber <- substring(gsub('Expected in ', '', bill_hist$journal_page), 1, 1)
  bill_hist <- bill_hist %>%
    group_by(bill_id) %>%
    mutate(chamber = ifelse(str_trim(chamber) == '', lag(chamber), chamber)) %>%
    ungroup()
  
  bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate")
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  # *** If reported always 'reported with recomm..' (in house) or either 'report.+ favorably' or 'report.+ with rec' in Senate
  # *** NOT coding fiscal analyses as AIC as not necessarily indicative of committee action (can be mandated based on content)
  aic_t <- c('^reported') ## can get discharged out as well
  abc_t <- c('^reported|(referred to|placed.+) second read|placed.+ third read|placed on immediate passage|^roll call|^amend|^substitute')
  pc_t <- c('^passed|enrolled')
  law_t <- c('approved by.+governor|assigned pa +[0-9]+')
  
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
  all_bill_stages %>% mutate(chamber = substring(bill_id, 6,6)) %>% group_by(session, chamber) %>% 
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
  all_bill_stages$bill_id_adj <- gsub('\\d{4}-|\\-', '', all_bill_stages$bill_id)
  all_bill_stages <- SS_term %>% 
    select(bill_id, term, SS) %>%
    left_join(all_bill_stages, ., by = c('bill_id_adj' = 'bill_id', 'term' = 'term')) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS)) %>%
    select(-bill_id_adj)
  
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
    mutate(chamber = ifelse(substring(gsub('[0-9]+-', '', bill_id), 1, 1) == 'H', "H", "S")) %>%
    select(LES_sponsor, chamber, passed_chamber, law) %>%
    mutate(term = t_yrs) %>%
    group_by(LES_sponsor, term, chamber) %>%
    summarize(num_sponsored_bills = n(), 
              sponsor_pass_rate = sum(passed_chamber) / n(),
              sponsor_law_rate = sum(law) / n()) %>%
    ungroup()
  
  ######## Cosponsorship Info 
  all_sponsors$num_cosponsored_bills <- NA
  for(i in 1:nrow(all_sponsors)){
    c_sub <- filter(bills, substring(bill_id, 6, 6) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, c_sub$sponsors))
    ### ONLY need to adjust this way if sponsored and cosponsored column are the same
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  }
  # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))
  rm(c_sub)
  
  ##################
  ## CLEAN NAMES
  ####################
  all_sponsors <- left_join(all_sponsors, map_df(all_sponsors$LES_sponsor, parse_names), by = c("LES_sponsor" = "full_name")) %>%
    select(-salutation) %>%
    #mutate(last_name = ifelse(is.na(last_name), first_name, last_name),first_name = ifelse(last_name == first_name, NA, first_name)) %>%
    mutate(last_name = gsub(',$', '', last_name)) %>%
    arrange(chamber, LES_sponsor) %>%
    distinct()
  
  #### Update Last Names for Matching 
  if(t_yrs %in% c("1999_2000", "2001_2002")){
    all_sponsors$last_name <- ifelse(all_sponsors$last_name == "hardman", "tinsleyhardman", all_sponsors$last_name)
    all_sponsors$last_name <- ifelse(all_sponsors$last_name == "clark", "clarkcoleman", all_sponsors$last_name)
    all_sponsors$last_name <- ifelse(all_sponsors$last_name == "roest", "vanderroest", all_sponsors$last_name)
    all_sponsors$last_name <- ifelse(all_sponsors$last_name == "reeves", "lipseyreeves", all_sponsors$last_name)
    all_sponsors$last_name <- ifelse(all_sponsors$last_name == "veen", "vanderveen", all_sponsors$last_name)  
  }
  if(t_yrs %in% c('2003_2004')){
    all_sponsors$last_name <- ifelse(all_sponsors$last_name == "hardman", "tinsleyhardman", all_sponsors$last_name)  
    all_sponsors$last_name <- ifelse(all_sponsors$last_name == "reeves", "lipseyreeves", all_sponsors$last_name)
    all_sponsors$last_name <- ifelse(all_sponsors$last_name == "veen", "vanderveen", all_sponsors$last_name)  
  }
  if(t_yrs %in% c("2005_2006")){
    all_sponsors$last_name <- ifelse(all_sponsors$last_name == "veen", "vanderveen", all_sponsors$last_name)  
  }
  if(t_yrs == '2011_2012'){
    all_sponsors$last_name <- ifelse(all_sponsors$last_name == "talabi", "tinsleytalabi", all_sponsors$last_name)    
    all_sponsors$last_name <- ifelse(all_sponsors$last_name == "oakes", "erwinoakes", all_sponsors$last_name)    
  }
  if(t_yrs == '2013_2014'){
    all_sponsors$last_name <- ifelse(all_sponsors$last_name == "talabi", "tinsleytalabi", all_sponsors$last_name)   
  }

  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  ### If Term is 2008-2009, e.g., including 2007, 2008, and Specials for 2009 (when present)
  elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% elec_year:(elec_year + 1) | (year == elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% (elec_year - sen_term_length + 2):(elec_year + 1) | (year == elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)) %>% mutate(term = t_yrs); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)
  
  ############## Match Sponsors Names to Klarner Data, Forgoing Function because mostly only have last name
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(!is.na(all_sponsors$first_name), paste0(", ", all_sponsors$first_name), '')))
  klarner_sub$match_name <- tolower(unlist(str_extract_all(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]+')))
  
  ### Fixing Gnodtke -- Listed as Gnodike in Klarner for some reason
  if(any(grepl('gnodtke', all_sponsors$match_name))){
    all_sponsors[grepl('gnodtke', all_sponsors$match_name),]$match_name <- 'gnodike, carl'
  }
  
  ### Fixing Lemmons
  if(t_yrs == '2005_2006'){
    all_sponsors[all_sponsors$LES_sponsor == "lamar lemmons iii",]$match_name <- 'lemmons, lamar iii'
    all_sponsors[all_sponsors$LES_sponsor == "lamar lemmons, jr.",]$match_name <- 'lemmons, lamar jr.'
    klarner_sub[klarner_sub$cand == 'lemmons, lamar iii',]$match_name <- 'lemmons, lamar iii'
    klarner_sub[klarner_sub$cand == 'lemmons, lamar jr.',]$match_name <- 'lemmons, lamar jr.'
  }
  
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
    if(nrow(k_matches) == 0 & grepl("-|'| ", all_sponsors[i,]$last_name)){
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
      m_sub <- grep(all_sponsors[i,]$match_name, k_matches$match_name)
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
  
  #### CHeck for Duplicates from Matching
  duplicates <- filter(all_sponsors, !is.na(klarner_name)) %>% group_by(chamber, klarner_name) %>% filter(n() > 1)
  if(nrow(duplicates) > 0){
    cat('-----> Check DUPLICATE Sponsors \n ')
    print(select(duplicates, LES_sponsor, klarner_name, chamber) %>% as.data.frame())
  }; rm(duplicates)
  
  ##### CHECK IF ANY KLARNER CANDIDATES MISSING FROM DATA --- Exclude Senators elected T-1
  km <- filter(klarner_sub, !(candid %in% all_sponsors$klarner_id) )
  km <- filter(km, sen == 0 | (sen == 1 & year == as.numeric(substring(t_yrs, 1, 4)) - 1 ))
  
  ### Drop Individuals who Won but were never seated
  if(t_yrs == "1995_1996"){
    km <- filter(km, cand != "jacobetti, dominic j.")
  }else if(t_yrs == "1999_2000"){
    km <- filter(km, cand != 'bouchard, michael')
  }else if(t_yrs == "2001_2002"){
    km <- filter(km, cand != 'kukuk, janet')
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
  
  ######################################################
  ######### Estimate Scores + Add in Relatd Variables
  ######################################################
  ### Check if bills in data without an ID'd sponsor
  # filter(bills, !(bills$primary_sponsor %in% legis_data$data_name))
  bills <- rename(bills, sponsor = LES_sponsor) %>%
    mutate(chamber = ifelse(substring(gsub('[0-9]+-', '', bill_id), 1, 1) == 'H', "H", "S"))
  
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
  
  cat(glue(" \n \n \n SESSION {t_yrs} ~~> DONE \n \n \n __________________________________________________________"))
}
################# ****** END LOOP

rm(all_sponsors, bills, legis_data, SS_bills, elec_year, i, keep_types)
rm(k_matches, klarner_sub, m_sub, km, bill_path, t_yrs, calc_LES, commem_bills)
rm(H_range, S_range)

########################################################################################################################################################
########################################################################################################################################################
######## NAME FIX RECORDS ---- 
########################################################################################################################################################
########################################################################################################################################################
####### NOTES -- Full Membe List is Here: http://www.akleg.gov/basis/mbr_info.asp?session=30

## ~~~~~~~~~~~~~ 1995-1996 ~~~~~~~~~~~~~
#### WON SPECIAL
# -- PRUSI
### IN CHAMBER
# Hillegonds = Only 1 cosponsered bill
### DROP:
# acobetti, dominic j. -- passed away Nov. 1994 - seat filled by Prusi
### NAME NOTES:
# Dick Posthumus != misspelled reference to posthumous bills; last name is posthumus

## ~~~~~~~~~~~~~ 1997-1998 ~~~~~~~~~~~~~
## WON SPECIAL
# -- SANBORN; BASHAM; BULLARD
## IN CHAMBER:
# -- hertel, curtis
# -- pitoniak, gregory 

## ~~~~~~~~~~~~~ 1999 - 2000 ~~~~~~~~~~~~~
## WON SPECIAL/APPOINTED
## -- JOHNSON (shirley)
## DROP
## -- bouchard, michael --  resigned to become Sherif of Oakland County

## ~~~~~~~~~~~~~ 2001-2002 ~~~~~~~~~~~~~
## WON SPECIAL
# -- PALMER; DURHAL; DROLET; HUMMEL; SCOTT (via H); 
# -- JOHNSON (shirley, T-1)
## IN CHAMBER:
# -- JOHNSON (rick) - Speaker of the House, 1 coauthored bill
# -- RISON (vera) -- Last term, minority whip.. Cosponsored 97, no introduced bills 
## DROP:
# -- kukuk, janet --  died November 2000


## ~~~~~~~~~~~~~ 2003-2004 ~~~~~~~~~~~~~
## WON SPECIAL: 
# -- MORTIMER -- decided not to run for reelection, but then was elected to fill a seat after someone passed away in 2003
## IN CHAMBER:
# -- JOHNSON (rick) -- Speaker, 20+ cosponored, no introduced bills
# -- SIKKEMA (ken) -- First two years of last term; no introduced bills


## ~~~~~~~~~~~~~ 2005-2006 ~~~~~~~~~~~~~
## IN CHAMBER:
# -- DEROCHE -- Speaker -- No introduced bills

## ~~~~~~~~~~~~~ 2007-2008  ~~~~~~~~~~~~~
## IN CHAMBER:
# -- DILLON -- Speaker -- No introduced bills

## ~~~~~~~~~~~~~ 2009-2010 ~~~~~~~~~~~~~
## WON SPECIAL:
# -- NOFS
## IN CHAMBER
# -- SHIRKEY
# -- ERWINOAKES
# --> both won 2010 specials (that are in Klarner data) -- Limited term


## ~~~~~~~~~~~~~ 2011-2012 ~~~~~~~~~~~~~
## WON SPECIAL:
# -- GRAVES
# -- GREIMEL
## IN CHAMBER:
# -- BOLGER -- Speaker, no bills

## ~~~~~~~~~~~~~ 2013-2014 ~~~~~~~~~~~~~
## WON SPECIAL:
# -- PHELPS
## IN CHAMBER
# -- GREIMEL -- Minority Leader -- No bills

## ~~~~~~~~~~~~~ 2015-2016 ~~~~~~~~~~~~~
## WON SPECIAL:
# -- LAGRAND
# -- HOWELL
# -- WHITEFORD

## ~~~~~~~~~~~~~ 2017-2018 ~~~~~~~~~~~~~
# ~~> Dropping 1 bills without a sponsor
## WON SPECIAL:
# -- CAMBENSY
# -- ANTHONY
# -- YANCEY
# -- HOLLIER -- elected in Nov 2018 special for Senate -- somehow proposes 2 in Dec. Must have been mid-4 year term
## IN CHAMBER:
# -- BANKS (brian) -- Resigned 2 months  into term (though cosponsored 14)
# -- ROBINSON (rose mary) -- Last term, no bills
# -- PLAWECKI (lauren) -- Elected in special upon death of mother; only served 2 months
# -- LEONARD (tom) -- SPeaker -- no introduced bills

# filter(bills, grepl("tom leonard", tolower(sponsors))) %>% select(sponsors)
# filter(klarner, grepl("holl", tolower(cand))) %>% select(cand, year, etype, outcome, sen) %>% filter(outcome == 'w')

#############################################################################################################################
#############################################################################################################################
#############################################################################################################################
#############################################################################################################################

library(readr)

### Re-Load LES Data
LES_paths <- list.files('.', full.names = TRUE)
LES_paths <- LES_paths[!grepl('Archive|_All|Merged', LES_paths)]

LES <- LES_paths %>%
  lapply(read_csv, col_types = cols()) %>%
  bind_rows 

rm(LES_paths)


###### Fill in Missing Data from Candidates Elected in Specials using Subsequent Observations
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

### Won Special to senate
LES[LES$sponsor == "bullard, bill" & LES$term == "1997_1998",]$klarner_id <- 101754
LES[LES$sponsor == "bullard, bill" & LES$term == "1997_1998",]$klarner_name <- "bullard, willis"
LES[LES$sponsor == "bullard, bill" & LES$term == "1997_1998",]$sponsor <- "bullard, willis"

### Won Special in Sep 2002 -- Wasn't reelected again until 2009
LES[LES$sponsor == "durhal, fred" & LES$term == "2001_2002",]$klarner_id <- 286792
LES[LES$sponsor == "durhal, fred" & LES$term == "2001_2002",]$klarner_name <- "durhal, fred jr."
LES[LES$sponsor == "durhal, fred" & LES$term == "2001_2002",]$sponsor <- "durhal, fred jr."

### FOUR Still missing = Elected in 2017-2018 Specials
still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[1]; print(name)
# filter(LES, grepl(name, sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, session, chamber)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid)
rm(name, missing, name_sub, still_missing)

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
rm(check_dup)


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
    next
  }
  for(s in this_sponsor_LES$term){
    second_year <- as.numeric(str_split(s, "_")[[1]][2])
    ### Filling in by chamber to account for people who switch chambers mid-term
    for(c in this_sponsor_LES[this_sponsor_LES$term == s,]$chamber){
      sponsor_sub <- filter(sponsor_rows, (etype %in% spec_elec_codes & year == second_year ) | year < second_year )
      sponsor_sub <- filter(sponsor_sub, sen == ifelse(c == "Senate", 1, 0))
      if(nrow(sponsor_sub) > 0 ){
        sponsor_sub <- arrange(sponsor_sub, desc(year))
        LES[LES$sponsor == name & LES$term == s & LES$chamber == c,]$district <- sponsor_sub[1,]$dno
        LES[LES$sponsor == name & LES$term == s & LES$chamber == c,]$party <- sponsor_sub[1,]$partyz
        LES[LES$sponsor == name & LES$term == s & LES$chamber == c,]$exper <- sponsor_sub[1,]$exper
      }else{
        if(nrow(sponsor_rows) > 0){
          sponsor_rows <- arrange(sponsor_rows, year)
          LES[LES$sponsor == name & LES$term == s & LES$chamber == c,]$party <- ifelse(is.logical(na.omit(unique(sponsor_rows$partyz))), NA, na.omit(unique(sponsor_rows$partyz))[1] )
          #LES[LES$sponsor == name & LES$term == s & LES$chamber == c,]$exper <- ifelse(is.logical(na.omit(unique(sponsor_rows$exper))), NA, na.omit(unique(sponsor_rows$exper))[1] )
        }
      }
    }
  }
  #print(name)
}

###### IF MISSING, CHECK ELCTION LOSERS
# filter(LES, is.na(party)) %>% select(sponsor, term, klarner_id, district, party, exper)
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

#### *** 2017_2018: If any of below run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "cambensy, sara",]$party <- 'd'
LES[LES$sponsor == "yancey, tenisha",]$party <- 'd'
LES[LES$sponsor == "anthony, sarah",]$party <- 'd'
LES[LES$sponsor == "hollier, adam",]$party <- 'd'

rm(klarner_sub, this_sponsor_LES, c, name, s, sponsor_rows, sponsor_sub, second_year)

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

### Expanding Senate to Terms (So Adding a Second Term)
senate <- filter(hf_data, CandId == 'aaaa')
for(i in 1:nrow(hf_data)){
  if(hf_data[i,]$chamber == "House") next
  sen_sub <- filter(hf_data, CandId == hf_data[i,]$CandId)
  if( !(paste0( hf_data[i,]$year + 3, "_",  hf_data[i,]$year + 4) %in% sen_sub$term) ){
    new_row <- hf_data[i,]  
    new_row$term <- paste0( hf_data[i,]$year + 3, "_",  hf_data[i,]$year + 4)
    senate <- bind_rows(senate, new_row)
  }
}
hf_data <- bind_rows(hf_data, senate); rm(senate, new_row, sen_sub)

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

rm(hf_data)

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

#### FIX MISMATCHES
LES[LES$sponsor %in% c('griffin, michael', 'obrien, michael', 'steil, glenn'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches: John "Jack" Brandenburg
# -- Andrew "rocky" raczkowski matches to Ray Raczkowski (via District and exact time period...)
# -- John Stahl matches to Benjamin Stahl (via district and exact time period)
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES -- SM Data only goes through 2016 so matches after that are from earlier period
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1993_1994', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('griff', tolower(name))) %>% select(name, party, st, np_score) 
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

name_matches <- data.frame(LES_name = 'clack, floyd', SM_name = 'Clack') # Two clacks, other has first name
#name_matches <- add_row(name_matches, LES_name = 'amash, justin', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'courser, todd', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'crawford, kathy', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'deshazor, larry', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'durhal, frederick', SM_name = 'Durhal, Fred Jr.')
name_matches <- add_row(name_matches, LES_name = 'emmons, joanne', SM_name = 'Emmons, Joanne Gregory')
name_matches <- add_row(name_matches, LES_name = 'emmons, judy', SM_name = 'Emmons, Judith K')
name_matches <- add_row(name_matches, LES_name = 'farrington, jeff', SM_name = 'Farrington, Jeffry')
# name_matches <- add_row(name_matches, LES_name = 'gamrat, cindy', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'geiss, erika', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'griffin, michael', SM_name = "Griffin")
# name_matches <- add_row(name_matches, LES_name = 'haase, jennifer', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'honigman, david (dave)', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'hood, morris w. iii', SM_name = 'Hood III, Morris W')
name_matches <- add_row(name_matches, LES_name = 'hood, morris wardelle jr.', SM_name = 'Hood, Morris Jr.')
name_matches <- add_row(name_matches, LES_name = 'howell, jim', SM_name = 'Howell')
# name_matches <- add_row(name_matches, LES_name = 'huckleberry, mike', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'jacobetti, dominic j.', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'johnson, shirley', SM_name = 'Johnson, R. Shirley')
# name_matches <- add_row(name_matches, LES_name = 'kennedy, deb', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'kilpatrick, carolyn', SM_name = 'Kilpatrick')
# name_matches <- add_row(name_matches, LES_name = 'kratz, jerry', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'leblanc, richard', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'lemmons, lamar iii', SM_name = 'Lemmons, L.')
# name_matches <- add_row(name_matches, LES_name = 'nerat, judy', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'oakes, stacy', SM_name = 'Erwin Oakes, Stacy')
name_matches <- add_row(name_matches, LES_name = 'obrien, michael', SM_name = "O'Brien")
name_matches <- add_row(name_matches, LES_name = 'proos, john m. iv', SM_name = 'Proos IV, John M')
# name_matches <- add_row(name_matches, LES_name = 'scripps, daniel collins', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'slezak, jim', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'smith, virgil', SM_name = 'Smith, Virgil Jr.')
name_matches <- add_row(name_matches, LES_name = 'smith, virgil c. jr.', SM_name = 'Smith, Virgil Clark Jr.')
name_matches <- add_row(name_matches, LES_name = 'steil, glenn', SM_name = 'Steil')
name_matches <- add_row(name_matches, LES_name = 'young, coleman a.', SM_name = 'Young II, Coleman A')
name_matches <- add_row(name_matches, LES_name = 'young, joseph (joe) jr.', SM_name = 'Young, Joseph Jr.')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

#### Manual Edits (Needs more precision...)
LES[LES$sponsor == 'scott, martha',]$SM_name <- ideo[ideo$name == 'Scott' & ideo$senate2001 %in% 1,]$name
LES[LES$sponsor == 'scott, martha',]$SM_party <- ideo[ideo$name == 'Scott' & ideo$senate2001 %in% 1,]$party
LES[LES$sponsor == 'scott, martha',]$np_score <- ideo[ideo$name == 'Scott' & ideo$senate2001 %in% 1,]$np_score

LES[LES$sponsor == 'scott, bettie cook',]$SM_name <- ideo[ideo$name == 'Scott' & ideo$house2007 %in% 1,]$name
LES[LES$sponsor == 'scott, bettie cook',]$SM_party <- ideo[ideo$name == 'Scott' & ideo$house2007 %in% 1,]$party
LES[LES$sponsor == 'scott, bettie cook',]$np_score <- ideo[ideo$name == 'Scott' & ideo$house2007 %in% 1,]$np_score


rm(ideo, check_last, name_matches)


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1995 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:1998, 2007:2010) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:1996, 1999:2006, 2011:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1995 - 2020
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzz) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()

##### Drop Nicknames
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

### Manual Fixes
LES[LES$sponsor == "clarkcoleman, irma",]$sponsor <- 'clark-coleman, irma'
LES[LES$sponsor == "pumford, m.",]$sponsor <- 'pumford, michael'
LES[LES$sponsor == "bernero, virg",]$sponsor <- 'bernero, virgil'
LES[LES$sponsor == "hopgood, hoonyung",]$sponsor <- 'hopgood, hoon-yung'
LES[LES$sponsor == "singh, sam",]$sponsor <- 'singh, samir'
# LES[LES$sponsor == "zzzz",]$sponsor <- 'zzzz'

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


######################
#### Plot
#######################


library(ggplot2)
library(ggridges)
library(forcats)

LES %>%
  group_by(term, party) %>%
  summarize(mean_LES = mean(LES)) 

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
# filter(LES, party == 'd' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')



