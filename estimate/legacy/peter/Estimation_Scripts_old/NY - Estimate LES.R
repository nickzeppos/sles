#################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** NEW YORK *** BY TERM
#################################################################

#####################
##### STATE-SPECIFIC NOTES:
#####################
# SPECIAL SESSIONS --- Bills from special terms are just included in the full set of bills -- session does not appear to restart
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
library(fastLink)
library(glue)
library(readr)

this_state <- 'NY'
min_year <- 1999
keep_types <- c("A", "S")
spec_elec_codes <- c('s')
sen_term_length <- 2

#### Output Directory
#dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
#setwd(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd(glue("../../LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Data Directory
data_files <- list.files(glue("../../../State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
terms <- gsub('.+Details_|.csv', '', bill_files)
rm(data_files, bill_files)

#### DROP 2019+ FOR NOW
terms <- terms[-which(terms %in% c("2019", "2021"))]

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/Commem Bills Old/{this_state}_Commem_Bills.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- read.csv("../../../State Legislative Data/Significant Bills/SS_Data/SS_Bills.csv")
SS_bills <- SS_bills %>% 
  filter(state == this_state) %>%
  rename(bill_id = bill_num) %>%
  mutate(term = ifelse(year %% 2 == 1, paste0(year, "_", year + 1), paste0(year - 1, "_", year)),
         bill_id = toupper(bill_id),
         bill_id = gsub("B", "", bill_id),
         ### NY Bills have 5 numbers
         bill_id = paste0(gsub("[0-9].+", '', bill_id), str_pad(gsub("^[A-Z]+", "", bill_id), 5, pad = "0")),
         SS = 1) %>%
  select(state, term, year, bill_id, everything())

#### Check SS BIll Types ---> Correct to ensure standardized with Data Format
# table(SS_bills$bill_type)

### Load Klarner Data 
load("../../../State Legislative Data/klarner_data/196slers1967to2016_20180908.RData")
klarner <- table; rm(table)
klarner$sab <- toupper(klarner$sab)
klarner <- filter(klarner, year > min_year - 5 & sab == this_state)

#### Fix/Note Klarner Errors
# -- Father/Son, Father died 2007-- Collapsed into 1 observation in Klarner -- Fixing Names but will still have same ID
klarner[klarner$cand == "zebrowski, kenneth p." & klarner$year <= 2006,]$cand <- "zebrowski, kenneth peter"
klarner[klarner$cand == "zebrowski, kenneth p." & klarner$year > 2006,]$cand <- "zebrowski, kenneth paul"
# THomas A. Hanna left office in 1982 -- This is Sean Hanna ---> previously ran in 2000, so matching to that ID
klarner[klarner$cand == "hanna, thomas a." & klarner$year >= 2010 & klarner$year <= 2012,]$cand <- "hanna, sean t."
klarner[klarner$cand == "hanna, sean t.",]$candid <- 171308
# D. Billy Jones != Denver L. Jones in 2016 - https://www.dbillyjones.net/meet-billy/
klarner[klarner$cand == "jones, denver l." & klarner$year == 2016,]$cand <- "jones, d. billy"


###########################
## LOOP THROUGH TERMS/termS
#########################
# t <- '1999'; t <- '2001'
  
for(t in terms){
  
  ### Add 19/20 onto years
  t_yrs <- as.numeric(t)
  t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 1}'))
  
  ### Skip Previously Estimated
  if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
    print(glue('. \n \n ~~~~ SKIPPING {t_yrs} TERM ---> LES scores already estimated! \n \n.'))
    next
  }
  
  ###### term IN PROGRESS
  print(glue(' \n ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM!  ~~~~~~~~~~~~~~~~~~ \n'))
  
  ############### Read in data
  bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t}.csv")
  bills <- read.csv(bill_path)
  bills$term <- t_yrs
  bills$session <- NA
  
  ### Check for duplicates
  if(nrow(bills) != nrow(distinct(bills))){
    cat(" \n ~~~> DUPLICATE BILLS \n\n .")
    break
  }
  
  ######## Standardize the Bill IDs
  bills <- rename(bills, bill_id = bill_number)

  ############### Drop Resolutions 
  # In NY, Resolutions start with B, C, E, J, K, L, R
  bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+', '', bill_id)))
  # table(bills$bill_type)
  bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)
  
  ############### Standardize Sponsors
  ### (MS) in sponsor name indicates a multisponsored bill 
  ### https://nyassembly.gov/Rules/?sec=r3#s3
  bills$LES_sponsor <- str_trim(gsub("\\(ms\\)", '', tolower(bills$sponsor)))
  bills$LES_sponsor <- gsub('rules \\(|\\)$', '', bills$LES_sponsor)
  
  ### Dropping committee bills without clear sponsor information 
  ### NOTE: It seems only rules can introduce bills on behalf of a member?
  # ---> See: https://nyassembly.gov/Rules/?sec=r4#s10
  # -->  "At any time during the term, a bill or resolution may be introduced by the Committee on Rules and shall be referred to a committee; 
  # provided however that all bills shall be referred to a standing committee other than the Committee on Rules, for consideration. 
  # A bill or resolution introduced at the request of a member shall, if the member so requests, have his or her name included on both the original and printed 
  # copies of the bill or resolution as follows:" 
  if(any(bills$LES_sponsor %in% c("budget", "rules"))){
    print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor %in% c('budget', 'rules')))} bill(s) sponsored by COMMITTEE"))
    bills <- filter(bills, !(LES_sponsor %in% c("budget", "rules")))
  }
  
  #### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
  bills$LES_sponsor <- gsub('á', 'a', bills$LES_sponsor)
  bills$LES_sponsor <- gsub('é', 'e', bills$LES_sponsor)
  bills$LES_sponsor <- gsub('ó', 'o', bills$LES_sponsor)
  bills$LES_sponsor <- gsub('í', 'i', bills$LES_sponsor)
  bills$LES_sponsor <- gsub('ñ', 'n', bills$LES_sponsor)

  #### Manual Fixes for Duplicates
  if(any(grepl('peoples-stoke$', bills$LES_sponsor))){
    bills$LES_sponsor <- gsub('peoples-stoke$', 'peoples-stokes', bills$LES_sponsor) 
  }
  if(any(grepl('rhodd-cumming$', bills$LES_sponsor))){
    bills$LES_sponsor <- gsub('rhodd-cumming$', 'rhodd-cummings', bills$LES_sponsor) 
  }
  
  ### CHeck Missing Sponsors
  # filter(bills, primary_sponsor == "") %>% View()
  if(any(bills$LES_sponsor == "")){
    print(glue(" ~~> Dropping {nrow(filter(bills, LES_sponsor == ''))} bills without a sponsor"))
    bills <- filter(bills, LES_sponsor != "") 
  }
  
  ###################
  ###### Merge in S&S Bills
  ###################
  # *** For NEW YORK: One long biennial session --> Merge on Bill ID, can ignore sessions
  SS_term <- SS_bills %>%
    filter(term == t_yrs) %>%
    distinct(term, bill_id, SS)
  
  ### Merge
  if(nrow(SS_term) > 0){
    bills <- bills %>% left_join(SS_term, by = c("bill_id", "term")) %>% mutate(SS = ifelse(is.na(SS), 0, SS))
  }else{
    bills$SS <- 0
  }
  
  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id", "term"))
  
  ################################################
  ############### Code Commemorative
  ################################################
  
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term'))
  # table(bills$commem)
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  ################################################
  ############### Code Bill History
  ################################################
  ##### ** Updated 11/25: Added logic to incorporate Substitute Bills (and credit legislators whose bills get substituted)
  
  bill_hist_path <- gsub('_Bill_Details', '_Bill_Histories', bill_path) 
  bill_hist <- read.csv(bill_hist_path)
  
  # sub_diagnostic <- bill_hist %>% 
  #   group_by(bill_number) %>% 
  #   summarize(sub_by = sum(grepl("substituted by", tolower(action))), 
  #             sub_for = sum(grepl("substituted for", tolower(action)))) %>% 
  #   filter(sub_by == 1 & sub_for == 1) %>% 
  #   pull(bill_number)

  # Note bills that are substituted for multiple bills
  mul_sub_bills <- bill_hist %>% 
    filter(grepl("substituted for", tolower(action))) %>% 
    mutate(sub_bill = paste0(toupper(str_sub(action, 17, 17)), 
                             str_pad(gsub("\\D+", "", action), width = 5, side = "left", pad = "0"))) %>% 
    group_by(bill_number) %>% 
    filter(bill_number != sub_bill) %>%
    filter(n_distinct(sub_bill) > 1) %>% 
    pull(bill_number) %>% unique()
  
  ######## Standardize BillHist Bill IDs
  bill_hist <- rename(bill_hist, bill_id = bill_number)
  bill_hist$term <- t_yrs
  bill_hist$session <- NA
  
  ### Code Chamber
  bill_hist$chamber <- ifelse(bill_hist$chamber == "Assembly", "House", "Senate")
  #bill_hist$chamber <- ifelse(toupper(bill_hist$action) == bill_hist$action, "Senate", "House")
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  source('../../Estimate LES/code_billhist_fx.R')
  
  ### Set State-Specific Terms for Identifying Each Stage
  # https://www.nysenate.gov/how-bill-becomes-law-1
  # https://www.brennancenter.org/sites/default/files/legacy/d/albanyreform_finalreport.pdf
  # http://documents.nycbar.org/files/legislativeglossary.pdf
  
  aic_t <- c("^reported", '^1st report', "held for consideration", "died in committee", "committee consideration")
  # If lacks majority support, dies in comm --> implies failed vote? http://www.nyc.gov/html/moiga/pages/state/process.shtml
  # Key point = not all bills have reported or died meaning some just see no action
  # Removing "to attorney-general for opinion" from AIC - not clear its not required
  abc_t <- c("^reported", '^(1st|2nd) report', "third reading", "3rd reading cal", "amended on third") 
  ### print number [0-9]+[a-z] - print number a/b/c means number changed to 123A/B/C after amended
  ### Dropping print number as leads to errors (sponsor can pull bill, amend, and reintroduce --> Print number change)
  ### Also Dropping amend and recommit
  pc_t <- c("passed assem", "repassed assem", "passed sen", "repassed sen", "^delivered to assembly", "^delivered to senate", "^delivered to gov")
  ### account for 'vote reconsidered - restored to third reading' ???
  law_t <- c("^signed chap")
  
  ### Check Actions
  # filter(bill_hist, grepl('^committee discharged', tolower(action))) %>% distinct(action) %>% View()
  
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
  
  ### Session Var
  bills$session <- bill_hist$session <- t_yrs
    
  ### Code History Stages for Each Bill --- Subset Hist File, Code, Save, Repeat
  options(warn = 2)
  for(i in 1:nrow(bills)){
    b_id = bills[i,]$bill_id
    b_spon = bills[i,]$LES_sponsor
    hist_sub <- filter(bill_hist, bill_id == b_id)
    
    # check if there is a substitution
    if(any(grepl("substituted by", hist_sub$action, ignore.case=T)) ){
      # take the last substituted by
      action = hist_sub %>% 
        filter(grepl("substituted by",action,ignore.case=T)) %>% 
        slice_tail(n=1) %>% 
        pull(action) 
      action_order = hist_sub %>% 
        filter(grepl("substituted by",action,ignore.case=T)) %>% 
        slice_tail(n=1) %>% 
        pull(order) 
      action = gsub("substituted by ","",action,ignore.case=T)
      sub_bill = paste0(
        str_to_upper(str_extract(action, "^[saSA]")),
        str_pad(str_extract(action, "(?<=^[saSA])\\d+"), 5, pad = "0")
      )
      sub_bill_hist <- filter(bill_hist, bill_id == sub_bill)
      # sub_bill_hist <- filter(hist_sub, order > action_order)
      
      # Take care of bill with multiple histories
      if (sub_bill %in% mul_sub_bills) {
        bill_num <- as.character(as.numeric(gsub("\\D+", "", b_id)))
        
        ### Add in a Substituted For Row if Missing
        if(!any(grepl("substituted for", sub_bill_hist$action, ignore.case=T))){
          sub_bill_hist <- bind_rows(
            hist_sub %>% 
              filter(grepl("substituted by",action,ignore.case=T)) %>% 
              slice_tail(n=1) %>%
              mutate(order = order + 0.5,
                     chamber = ifelse(substr(sub_bill,1,1) == "A", "House", "Senate"),
                     action = paste0('substituted for ', b_id)),
            sub_bill_hist
          )
        }
        
        sub_row <- min(c(1:nrow(sub_bill_hist))[grepl(bill_num, tolower(sub_bill_hist$action))])
        if (sum(grepl("substitution reconsidered", tolower(sub_bill_hist$action))) > 0){
          reconsidered_row <- min(c(1:nrow(sub_bill_hist))[grepl("substitution reconsidered", tolower(sub_bill_hist$action))])
          if (sub_row < reconsidered_row){
            sub_bill_hist <- sub_bill_hist[1:reconsidered_row, ]
          }
        } else if (sum(grepl("^died", tolower(sub_bill_hist$action))) > 0){
          died_row <- min(c(1:nrow(sub_bill_hist))[grepl("^died", tolower(sub_bill_hist$action))])
          if (sub_row < died_row){
            sub_bill_hist <- sub_bill_hist[1:died_row, ]
          }
        }
      }
      hist_sub <- bind_rows(hist_sub, sub_bill_hist) %>% 
        distinct(bill_id, session, chamber, action_date, action, term) %>% 
        arrange(action_date) %>% 
        mutate(order = row_number())
      
      rm(sub_bill_hist, action,sub_bill)
    }
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, t_yrs, b_spon, aic_t, abc_t, pc_t, law_t,
                                      ignore_chamber_switch = TRUE)
    bill_stages$bill_url <- bills[i,]$bill_url
    originating_chamber = ifelse(substr(b_id,1,1)=="A","House","Senate")
    # check if there is a reconsideration/bill died
    if(bill_stages$passed_chamber == 1 & any(grepl("vote reconsidered|died", hist_sub$action[hist_sub$chamber==originating_chamber], ignore.case=T)) ){
      last_row_reconsidered = max(hist_sub %>% 
                                    filter(chamber == originating_chamber) %>% 
                                    filter(grepl("vote reconsidered",action,ignore.case=T)) %>% 
                                    slice_tail(n=1) %>% pull(order),0)
      last_row_died = max(hist_sub %>% 
                            filter(chamber == originating_chamber) %>% 
                            filter(grepl("died",action,ignore.case=T)) %>% 
                            slice_tail(n=1) %>% pull(order),0)
      max_pass_row = max(hist_sub %>% 
                           filter(chamber == originating_chamber) %>% 
                           filter(grepl(paste(pc_t, collapse="|"),action,ignore.case=T)) %>% 
                           slice_tail(n=1) %>% pull(order),0)
      sum_pass_row = hist_sub %>% 
        filter(chamber == originating_chamber) %>% 
        filter(grepl(paste(pc_t, collapse="|"),action,ignore.case=T) & 
                 !grepl("deliver", action, ignore.case=T)) %>% 
        nrow()
      sum_reconsider_row = hist_sub %>% 
        filter(chamber == originating_chamber) %>% 
        filter(grepl("vote reconsidered",action,ignore.case=T)) %>% 
        nrow()
      if( (last_row_reconsidered > max_pass_row & ! (sum_pass_row > sum_reconsider_row)) |
          last_row_died > max_pass_row){
        # reconsideration happens after the last pass and there aren't more passes than reconsiders OR it dies after passing
        bill_stages$passed_chamber <- 0 # override because it didn't really pass
      }
    }
    
    # bill_stages$passed_chamber = ifelse(bill_stages$law==1, 1, bill_stages$passed_chamber)
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
  if(!dir.exists(glue('../../../State Legislative Data/Bill_Stage_Codings/{this_state}'))){
    dir.create(glue('../../../State Legislative Data/Bill_Stage_Codings/{this_state}'))
  }
  write.csv(all_bill_stages, glue('../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv'), row.names = FALSE )
  #~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv
  #test <- read.csv('../../../State Legislative Data/Bill_Stage_Codings/NY/NY_1999_2000_Bill_Stage_Codings.csv')
  rm(bill_hist_path, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist, aic_t, abc_t, pc_t, law_t)
  rm(bill_hist, b_id, b_spon)
  
  ####################################################
  ############### Identify Unique Legislators via SLER
  ####################################################
  
  ## Import and Clean Sponsors Name to Match
  all_sponsors <- bills %>%
    mutate(chamber = ifelse(substring(bill_id, 1, 1) == "A", "House", "Senate")) %>%
    select(LES_sponsor, chamber, passed_chamber, law) %>%
    mutate(term = t_yrs) %>%
    group_by(LES_sponsor, chamber, term) %>%
    summarize(num_sponsored_bills = n(), 
              sponsor_pass_rate = sum(passed_chamber) / n(),
              sponsor_law_rate = sum(law) / n()) %>%
    ungroup()

  ######## Cosponsorship Info --- 
  all_sponsors$num_cosponsored_bills <- NA
  bills$cospon_match <- paste(bills$LES_sponsor, tolower(bills$cosponsors), tolower(bills$multi_sponsors), sep = '; ')
  for(i in 1:nrow(all_sponsors)){
    c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'House', 'A', 'S')  )
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
    ### ONLY need to adjust this way if sponsored and cosponsored column are the same
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  }
  bills <- select(bills, -cospon_match)
  # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))
  
  
  ##################
  ### CLEAN NAMES
  ####################
  all_sponsors <- all_sponsors %>% 
    mutate(last_name = gsub(' .+', '', LES_sponsor),
           first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
    arrange(chamber, LES_sponsor) %>%
    distinct()
  
  ##### Manual Name Fixes to Ensure Klarner Match
  ## (1) Earlene Hill Hooper 
  if(t_yrs %in% c("1999_2000", "2001_2000")){
    all_sponsors[all_sponsors$last_name == "hill",]$first_name <- 'earlene'
    all_sponsors[all_sponsors$last_name == "hill",]$last_name <- 'hooper'
  }
  ## (2) Peoples-Stokes -- Still in office after 2008 but name corrects to peoples-stokes, which matches in script
  if(t_yrs %in% c("2003_2004", "2005_2006", "2007_2008")){
    all_sponsors[all_sponsors$last_name == "peoples",]$last_name <- 'peoplesstokes'
  }
  
  ## (3) Stacey Pheffer Amato
  if(t_yrs %in% c("2017_2018")){
    all_sponsors[all_sponsors$last_name == "pheffer",]$first_name <- 'stacey'
    all_sponsors[all_sponsors$last_name == "pheffer",]$last_name <- 'pheffer-amato'
  }
  
  ## (4) Addie Jenne Russell -- Dropped Russell in 2017
  ## Keeping Russel for simplicity to match to klarner...
  if(t_yrs %in% c("2017_2018")){
    all_sponsors[all_sponsors$last_name == "jenne",]$last_name <- 'russell'
  }
  
  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  ### NY SENATORS ELECTED TO 2-YEAR TERMS!
  elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% elec_year:(elec_year + 1) | (year == elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% (elec_year - sen_term_length + 2):(elec_year + 1) | (year == elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)) %>% mutate(term = t_yrs); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)

  ##### Account for fusion system
  klarner_sub <- klarner_sub %>%
    group_by(cand) %>%
    mutate(all_parties = paste(unique(party), collapse = "---") )  %>%
    mutate(party = ifelse(grepl('republican', all_parties), "modernrepublican", ifelse(grepl('democrat', all_parties), "democrat", NA)),
           partyz = ifelse(party == "modernrepublican", 'r', ifelse(party == 'democrat', 'd', NA))) %>%
    distinct()
  
  ############################################################
  ############## Match Sponsors Names to Klarner Data
  ############################################################
  
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- all_sponsors$LES_sponsor
  klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]')))
  ## First = Last firstinitial; second = last firstinitial + middle initial (this is the format NY uses)
  klarner_sub$match_name <- gsub(',', '', klarner_sub$match_name)
  klarner_sub$match_name2 <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]+ [a-z]\\.')))
  klarner_sub$match_name2 <- paste0(klarner_sub$match_name, ifelse(!is.na(klarner_sub$match_name2), gsub('[a-zA-z]+, [a-zA-z]+ |\\.$', '', klarner_sub$match_name2), '' ))
  
  ### Fix for De La Rosa
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == 'de la rosa', 'de la rosa', all_sponsors$last_name)
  
  ### Match loop
  all_sponsors$elec_year <- all_sponsors$klarner_id <- all_sponsors$klarner_name <- NA
  all_sponsors$klarner_name <- as.character(all_sponsors$klarner_name)
  all_sponsors$klarner_id <- as.double(all_sponsors$klarner_id)
  all_sponsors$elec_year <- as.double(all_sponsors$elec_year)
  
  for(i in 1:nrow(all_sponsors)){
    k_matches <- filter(klarner_sub, last_name == tolower(all_sponsors[i,]$last_name))
    ## Acccount for lack of punctuation or spaces in Klarner Data
    if(nrow(k_matches) == 0 & grepl("-|'| ", all_sponsors[i,]$last_name)){
      k_matches <- filter(klarner_sub, last_name == gsub("-|'| ", '', tolower(all_sponsors[i,]$last_name)))
    }
    ## Account for chamber if multiple matches
    if(nrow(k_matches) > 1){
      k_matches <- filter(klarner_sub, last_name == tolower(all_sponsors[i,]$last_name) & sen == ifelse(all_sponsors[i,]$chamber == "Senate", 1, 0))
    }
    ### Save and Cross-Check
    if(nrow(k_matches) == 1){
      all_sponsors[i, ]$klarner_name <- k_matches$cand
      all_sponsors[i, ]$klarner_id <- k_matches$candid
      all_sponsors[i, ]$elec_year <- k_matches$year     
      ###Check Names With Initials, Etc. 
    } else if(nrow(k_matches) > 1 & length(unique(k_matches$cand)) != 1){
      m_sub <- grep(all_sponsors[i,]$match_name, k_matches$match_name)
      ### Check Secondary Match Name
      if(length(m_sub) == 0){
        m_sub <- grep(all_sponsors[i,]$match_name, k_matches$match_name2) 
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
    } else {
      print(glue("MISSING MATCH ::: {all_sponsors[i,]$last_name} ::: {i} ::: {all_sponsors[i,]$chamber}"))
    }
  }
  
  #### Fix Mismatches
  if(t_yrs == "1999_2000"){
    all_sponsors[all_sponsors$LES_sponsor == "smith m" & all_sponsors$chamber == "Senate", c("klarner_name", "klarner_id", "elec_year")] <- NA
    all_sponsors[all_sponsors$LES_sponsor == "stavisky t" & all_sponsors$chamber == "Senate", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "2003_2004"){
    all_sponsors[all_sponsors$LES_sponsor == "gunther a" & all_sponsors$chamber == "House", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "2007_2008"){
    all_sponsors[all_sponsors$LES_sponsor == "zebrowski k" & all_sponsors$chamber == "House", c("klarner_name", "klarner_id", "elec_year")] <- NA
    all_sponsors[all_sponsors$LES_sponsor == "johnson c" & all_sponsors$chamber == "Senate", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "2009_2010"){
    all_sponsors[all_sponsors$LES_sponsor == "miller m" & all_sponsors$chamber == "House", c("klarner_name", "klarner_id", "elec_year")] <- NA
    all_sponsors[all_sponsors$LES_sponsor == "weprin d" & all_sponsors$chamber == "House", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "2017_2018"){
    all_sponsors[all_sponsors$LES_sponsor == "rosenthal d" & all_sponsors$chamber == "House", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }
  
  #### Check for Duplicates from Matching
  duplicates <- filter(all_sponsors, !is.na(klarner_name)) %>% group_by(chamber, klarner_name) %>% filter(n() > 1)
  if(nrow(duplicates) > 0){
    cat('-----> Check DUPLICATE Sponsors \n ')
    print(select(duplicates, LES_sponsor, klarner_name, chamber) %>% as.data.frame())
  }; rm(duplicates)
  
  
  ##### CHECK IF ANY KLARNER CANDIDATES MISSING FROM DATA --- Exclude Senators elected T-1
  km <- filter(klarner_sub, !(candid %in% all_sponsors$klarner_id) )

  ### Remove candidates who won but were never seated
  if(t_yrs == "2003_2004"){
    km <- filter(km, cand != 'davis, gloria')
  }else if(t_yrs == "2007_2008"){
    km <- filter(km, cand != 'balboni, michael a. l.')
  }
  
  #### Add Candidates who WON but Sponsored NO BILLS into Data
  if(nrow(km) > 0){
    cat(glue("-----> {as.numeric(substring(t_yrs, 1, 4)) - 1} KLARNER CANDIDATES MISSING MATCHES IN DATA ~~> ADDING INTO LIST OF LEGISLATORS: \n . "))
    print(select(km, year, sab, sen, ddez, etype, deter, cand, candid, partyz, match_name) %>% as.data.frame())
    chamb <- ifelse(km$sen == 1, "Senate", "House")
    for(i in 1:nrow(km)){
      all_sponsors <- add_row(all_sponsors, chamber = chamb[i], term = t_yrs, klarner_name = km$cand[i], klarner_id = km$candid[i], elec_year = km$year[i])
    }
    rm(chamb)
  }

  #### Clean
  legis_data <- all_sponsors %>%
    rename(data_name = LES_sponsor) %>%
    mutate(sponsor = ifelse(!is.na(klarner_name), klarner_name, tolower(match_name)), 
           term = t_yrs) %>%
    select(sponsor, data_name, klarner_name, klarner_id, chamber, term, elec_year, num_sponsored_bills, num_cosponsored_bills, sponsor_pass_rate, sponsor_law_rate) %>%
    arrange(chamber, sponsor)
  
  ##############################
  ###### Estimate Scores + Add in Related Variables
  #############################
  
  ### Check if bills in data without an ID'd sponsor
  bills <- select(bills, -sponsor) %>% 
    rename(sponsor = LES_sponsor) %>% 
    mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'A', "H", "S"))
  
  legis_data <- mutate(legis_data, chamber = ifelse(chamber == "House", "H", "S"))
  
  ### Standard LES: Same as Congressional Measure
  source('../../../State Legislatures/Estimate LES/calc_LES_fx.R')
  
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
  
  cat(glue(" ***************** \n \n \n TERM {t_yrs} ~~> DONE \n \n \n *************** ")); cat('\n')
}
################# ****** END LOOP


rm(all_sponsors, legis_data, SS_bills, elec_year, keep_types)
rm(commem_bills, t_yrs, t, c_sub, k_matches, klarner_sub, km, m_sub, bills, bill_path, calc_LES)

########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
#### Vacancies filled by SPECIAL ELECTION --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
##### Senate Wayback: https://web.archive.org/web/20000815061038/http://www.senate.state.ny.us/
##### Assembly Wayback: https://web.archive.org/web/20070628150818/http://assembly.state.ny.us/
###############################################################################


# filter(klarner, grepl("key, j", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(year, cand) %>% distinct() %>% as.data.frame() 
# filter(klarner, ddez == 17 & sen == 1 & outcome == 'w') %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)

# *** All information below is updated as of 11/25 (post bill substitution coding change)

# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 369 bill(s) sponsored by COMMITTEE
# ~~> Dropping 1 bills without a sponsor
#   session chamber     N  AIC  ABC PASS LAW
# 1 1999_2000       A 10691 5830 3736 2386 1174
# 2 1999_2000       S  8045 1484 3008 2240 1161
### WON SPECIAL ~ HOUSE:
# -- FINCH (gary)
# -- KOLB (brian)
### WON SPECIAL ~ SENATE:
# -- COPPOLA (alfred, lost subsequent) -- https://web.archive.org/web/20100913034712/http://artvoice.com/issues/v9n36/five_questions#SlideFrame_0
# -- MORAHAN (thomas)
# -- SMITH (malcom) --> Name duplicated, won't print
# -- STAVISKY (toby ann) --> Name dup, won't print


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 391 bill(s) sponsored by COMMITTEE
# ~~> Dropping 1 bills without a sponsor
#   session chamber     N  AIC  ABC PASS LAW
# 1 2001_2002       A 11107 5496 3603 2314 1162
# 2 2001_2002       S  7503 1435 2845 1992 1161
#### WON SPECIAL ~ HOUSE:
# -- MCDONALD (roy) -- https://www.nysenate.gov/senators/roy-j-mcdonald
# -- MCDONOUGH (david)
# -- MIRONES (matthew)
# -- ROBINSON (annettee m.)
# -- SANFORD (willaim e., lost 2002 elec) -- https://www.syracuse.com/opinion/2011/05/bill_sanford_su_crew_coach_ono.html
# -- TITUS (michele)
### WON SPECIAL ~ SENATE:
# -- ANDREWS (carl)
# -- KRUEGER (liz)


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 366 bill(s) sponsored by COMMITTEE
# ~~> Dropping 0 bills without a sponsor
#   session chamber     N  AIC  ABC PASS LAW
# 1 2003_2004       A 11142 5632 3702 2525 1339
# 2 2003_2004       S  7550 1813 3059 2392 1325
### WON SPECIAL ~ HOUSE:
# -- BENJAMIN (michael)
# -- FIELDS (ginny a.)
# -- SALADINO (joseph s.)
# -- GUNTHER (aileen m) -- last name duplicatd, won't print ***
### DROP:
# -- davis, gloria -- resigned shortly into term due to bribery scandal -- https://web.archive.org/web/20190515051821/https://nypost.com/2002/03/13/key-dem-probed-in-bribe-scandal/


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 317 bill(s) sponsored by COMMITTEE
# ~~> Dropping 0 bills without a sponsor
#   session chamber     N  AIC  ABC PASS LAW
# 1 2005_2006       A 11967 5335 3774 2885 1448
# 2 2005_2006       S  8126 2612 3603 2847 1420
### WON SPECIAL ~ HOUSE:
# -- ALESSI (marc)
# -- BOYLE (phillip, past H)
# -- CAMARA (karim)
# -- COLE (michael)
# -- FRIEDMAN (sylvia)
# -- GIGLIO (joseph)
# -- HAWLEY (stephen)
# -- HEVESI (andrew)
# -- MAISEL (alan)
# -- MCKEVITT (thomas)
# -- ROSENTHAL (linda)
# -- WALKER (rob)
### WON SPECIAL ~ Senate:
# -- COPPOLA (mark)
### IN CHAMBER:
# -- ferrara, donna -- resigned March 2005 after appointed to state post: https://www.liherald.com/stories/Ferrara-steps-down-from-Assembly,10946https://web.archive.org/save/https://www.liherald.com/stories/Ferrara-steps-down-from-Assembly,10946


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 278 bill(s) sponsored by COMMITTEE
# session chamber     N  AIC  ABC PASS LAW
# 1 2007_2008       A 11697 5364 3597 2572 1306
# 2 2007_2008       S  8488 2850 3726 2860 1236
#### WON SPECIAL ~ HOUSE:
# -- AMEDORE (george jr)
# -- KELLNER (micah z.)
# -- SCHIMEL (michelle)
# -- TITONE (matthew)
# -- TOBACCO (louis)
# -- ZEBROWSKI (kenneth PAUL) -- Name Dup, won't print -- father (kenneth peter) died march 2007, son succeeded -- SAME ID IN KLARNER
# -- JOHNSON (craig) -- Name dup, won't print
### IN CHAMBER (partial):
# -- dinapoli, thomas p. -- appointed state comptroller Feb 7, 2007
### DROP:
# -- balboni, michael a. l. -- appointed to state post Dec 26. 2006


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 583 bill(s) sponsored by COMMITTEE
# ~~> Dropping 2 bills without a sponsor
#   session chamber     N  AIC  ABC PASS LAW
# 1 2009_2010       A 11269 4495 2806 1907 1004
# 2 2009_2010       S  8240 2293 2772 1685  989
#### WON SPECIAL ~ HOUSE:
# -- CASTELLI (robert)
# -- CRESPO (marcos)
# -- GIBSON (vanessa)
# -- MONTENSANO (michael)
# -- MURRAY (dean)
# -- MILLER (michael) -- Name dup, wont print!! ****
# -- WEPRIN (david) -- Name dup, won't print!! ***


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 125 bill(s) sponsored by COMMITTEE
#   session chamber     N  AIC  ABC PASS LAW
# 1 2011_2012       A 10663 4262 2543 1803 1088
# 2 2011_2012       S  7663 2756 2832 2102 1062
### WON SPECIAL ~ HOUSE:
# -- BARRETT (didi)
# -- BRINDISI (anthony)
# -- ESPINAL (rafael)
# -- GOLDFEDER (phillip)
# -- KEARNS (michael p.)
# -- MAYER (shelley)
# -- QUART (dan)
# -- RYAN (sean)
# -- SIMANOWITZ (michael)
# -- SKARTADOS (frank, past H)
# -- WALTER (raymond)
#### WON SPECIAL ~ SENATE:
# -- STOROBIN (david, lost 2012 elec.)


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 115 bill(s) sponsored by COMMITTEE
#   session chamber     N  AIC  ABC PASS LAW
# 1 2013_2014       A 10121 3804 2566 1889 1089
# 2 2013_2014       S  7757 2331 2995 2311 1050
#### WON SPECIAL ~ HOUSE:
# -- DAVILA (maritza)
# -- PALUMBO (anthony)
# -- PICHARDO (victor)
#### WON SPECIAL ~ SENATE:
# -- TKACZYK (cecilia)
#### IN CHAMBER:
# -- rivera, jose
# -- amedore, george a. jr.

# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 102 bill(s) sponsored by COMMITTEE
# ~~> Dropping 14 bills without a sponsor
#   session chamber     N  AIC  ABC PASS LAW
# 1 2015_2016       A 10377 3656 2605 1831 1086
# 2 2015_2016       S  8047 2360 3337 2637 1066
#### WON SPECIAL ~ HOUSE:
# -- CANCEL (alice, lost 2016 elec)
# -- CASTORINA (ronald)
# -- HARRIS (pamela)
# -- HUNTER (pamela jo)
# -- HYNDMAN (alicia)
# -- RICHARDSON (diana)
# -- WILLIAMS (jamie r.)
#### WON SPECIAL ~ SENATE:
# -- AKSHAR (frederick)
#### IN CHAMBER (partial):
# -- camara, karim -- resigned Feb 20, 2015 for admin post: https://web.archive.org/web/20190411070136/https://www.nydailynews.com/blogs/dailypolitics/karim-camara-doubles-salary-joining-team-cumo-blog-entry-1.2174585


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 86 bill(s) sponsored by COMMITTEE
# ~~> Dropping 7 bills without a sponsor
#   session chamber     N  AIC  ABC PASS LAW
# 1 2017_2018       A 10931 3582 2700 1800 1007
# 2 2017_2018       S  9032 2681 3768 2784  982
#### WON SPECIAL ~ HOUSE:
# -- ASHBY (jacob, R, D-107)
# -- BOHEN (erik, I-D, D-142)
# -- EPSTEIN (harvey, D, D-74)
# -- ESPINAL (ari, D, D-39)
# -- FERNANDEZ (nathalie, D, D-80)
# -- MIKULIN (john, R, D-17)
# -- PELLEGRINO (christine, D, D-9)
# -- SMITH (doug m, R, D-5)
# -- STERN (steven h., D, D-10, NOT the same as Repub one who lost 2006-2016)
# -- TAGUE (christopher, R, D-102)
# -- TAYLOR (al, D, D-71)
# -- ROSENTHAL (daniel, D, D-27) -- Name dup, won't print
#### WON SPECIAL ~ SENATE:
# -- BENJAMIN (brian, D, D-30)
### IN CHAMBER (partial): 
# -- saladino, joseph s. -- resigned Jan. 31, 2017
# -- rivera, jose -- still in office

# filter(klarner, grepl("rivera, j", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(year, cand) %>% distinct() %>% as.data.frame()
# filter(klarner, ddez == 17 & sen == 1 & outcome == 'w') %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)


##########################################################################################################
##########################################################################################################
##### MATCHING MISSING (SPECIALS) TO KLARNER USING OTHER TERMS WHERE SPONSOR IS PRESENT
##########################################################################################################

### Re-Load LES Data
LES_paths <- list.files('.', full.names = TRUE)
LES_paths <- LES_paths[!grepl('Archive|_All|Merged|_tenure', LES_paths)]
LES_paths <- LES_paths[!grepl('2019|2021', LES_paths)]

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
  if( any(!is.na(name_sub$klarner_id)) & length(unique(na.omit(name_sub$klarner_id))) == 1 ){
    LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$sponsor <- unique(name_sub[!is.na(name_sub$klarner_id),]$sponsor) 
    LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$klarner_name <- unique(na.omit(name_sub$klarner_name))
    LES[grepl(name, LES$sponsor) & is.na(LES$klarner_id),]$klarner_id <- unique(na.omit(name_sub$klarner_id))   
    print(glue(' ~~ {name} ~~ Matched to --> {unique(na.omit(name_sub$klarner_name))}'))
  } else {
    if(exact == TRUE){
      k_sub <- filter(klarner, grepl(paste0('^', name, ','), cand))
    }else{
      search_name <- ifelse(grepl('^[a-z]+ [a-z]$', name), gsub(' ', ', ', name), name)
      k_sub <- filter(klarner, grepl(search_name, cand))  
    }
    if(length(unique(k_sub$cand)) == 1 & length(unique(k_sub$candid)) == 1){
      LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name), c("sponsor", "klarner_name")] <- unique(k_sub$cand) 
      LES[LES$sponsor %in% unique(k_sub$cand)  & is.na(LES$klarner_id),]$klarner_id <- unique(k_sub$candid) 
      print(glue(' ~~ {name} ~~ Matched to --> {unique(k_sub$cand) }'))  
    }
  }
}

###### Clear Mismatches
LES[LES$data_name %in% "pellegrino" & LES$term == "2017_2018", c("klarner_name", "klarner_id")] <- NA
LES[LES$data_name %in% "pellegrino" & LES$term == "2017_2018",]$sponsor <- "pellegrino, christine"
LES[LES$data_name %in% "benjamin" & LES$term == "2017_2018", c("klarner_name", "klarner_id")] <- NA
LES[LES$data_name %in% "benjamin" & LES$term == "2017_2018",]$sponsor <- "benjamin, brian"
LES[LES$data_name %in% "rosenthal d" & LES$term == "2017_2018", c("klarner_name", "klarner_id")] <- NA
LES[LES$data_name %in% "rosenthal d" & LES$term == "2017_2018",]$sponsor <- "rosenthal, daniel"
LES[LES$data_name %in% "espinal" & LES$term == "2017_2018", c("klarner_name", "klarner_id")] <- NA
LES[LES$data_name %in% "espinal" & LES$term == "2017_2018",]$sponsor <- "espinal, ari"

### ****Still missing*****
# still_missing <- unique(LES[is.na(LES$klarner_id),]$sponsor)
# name = still_missing[8]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl('miller, m', cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, missing, still_missing)

### Fix Missing
name_matches <- data.frame(LES_name = 'coppola', k_name = "coppola, alfred t.", term = '1999_2000')
name_matches <- add_row(name_matches, LES_name = 'mcdonald', k_name = 'mcdonald, roy j.', term = '2001_2002')
name_matches <- add_row(name_matches, LES_name = 'coppola', k_name = 'coppola, mark a.', term = '2005_2006')
name_matches <- add_row(name_matches, LES_name = 'friedman', k_name = 'friedman, sylvia', term = '2005_2006')
name_matches <- add_row(name_matches, LES_name = 'walker', k_name = 'walker, rob', term = '2005_2006')
name_matches <- add_row(name_matches, LES_name = 'hevesi', k_name = 'hevesi, andrew', term = '2005_2006')
name_matches <- add_row(name_matches, LES_name = 'zebrowski k', k_name = 'zebrowski, kenneth paul', term = '2007_2008')
name_matches <- add_row(name_matches, LES_name = 'johnson c', k_name = 'johnson, craig m.', term = '2007_2008')
name_matches <- add_row(name_matches, LES_name = 'miller m', k_name = 'miller, michael g.', term = '2009_2010')
name_matches <- add_row(name_matches, LES_name = 'murray', k_name = 'murray, dean', term = '2009_2010')
#### All below are 2017-2018 = No data in Klarner (and stern != prior stern)
# name_matches <- add_row(name_matches, LES_name = 'pellegrino, christine', k_name = 'zzzzzzzz', term = '2017_2018')
# name_matches <- add_row(name_matches, LES_name = 'stern', k_name = 'zzzzzzzz', term = '2017_2018')
# name_matches <- add_row(name_matches, LES_name = 'epstein', k_name = 'zzzzzzzz', term = '2017_2018')
# name_matches <- add_row(name_matches, LES_name = 'espinal, ari', k_name = 'zzzzzzzz', term = '2017_2018')
# name_matches <- add_row(name_matches, LES_name = 'rosenthal, daniel', k_name = 'zzzzzzzz', term = '2017_2018')
# name_matches <- add_row(name_matches, LES_name = 'fernandez', k_name = 'zzzzzzzz', term = '2017_2018')
# name_matches <- add_row(name_matches, LES_name = 'ashby', k_name = 'zzzzzzzz', term = '2017_2018')
# name_matches <- add_row(name_matches, LES_name = 'taylor', k_name = 'zzzzzzzz', term = '2017_2018')
# name_matches <- add_row(name_matches, LES_name = 'smith', k_name = 'zzzzzzzz', term = '2017_2018')
# name_matches <- add_row(name_matches, LES_name = 'tague', k_name = 'zzzzzzzz', term = '2017_2018')
# name_matches <- add_row(name_matches, LES_name = 'mikulin', k_name = 'zzzzzzzz', term = '2017_2018')
# name_matches <- add_row(name_matches, LES_name = 'bohen', k_name = 'zzzzzzzz', term = '2017_2018')
# name_matches <- add_row(name_matches, LES_name = 'benjamin, brian', k_name = 'zzzzzzzz', term = '2017_2018')
#name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz', term = 'zzzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & LES$term == name_matches[i,]$term & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & LES$term == name_matches[i,]$term & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name & LES$term == name_matches[i,]$term,]$sponsor <- name_matches[i,]$k_name
}
rm(name_matches, i)

############## Check for Duplicates
### ---> If two people with same last name and one won in a special, will lead to duplicates
### Remaining = Switched from House to Senate
# **** ZEBROWSKI in 2007-2008: Duplicate Klarner ID unavoidable here as he has them collapsed as 1... but changing names to differentiate
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
rm(check_dup, k_sub, exact, name_sub, missing, search_name, name, t)


########################################################################################################################################################
########################################################################################################################################################
######## AGGREGATE AND MATCH TO EXTERNAL
########################################################################################################################################################
########################################################################################################################################################

### Klarner State Leg. Election Data --- Using Adjusted File From Top
klarner_sub <- filter(klarner, sab == this_state & year >= min_year - 5 & outcome == 'w')
klarner_sub <- select(klarner_sub, year, sen, ddez, dno, term, termz, cando, cand, candid, partyz, partyt, exper, outcome, etype)

#### KLARNER IS YEAR OF ELECTION, Not TERM
LES$exper <- LES$party <- LES$district <- NA
LES$district <- as.double(LES$district)
LES$party <- as.character(LES$party)
LES$exper <- as.character(LES$exper)

for(name in unique(LES$sponsor)){
  this_sponsor_LES <- LES[LES$sponsor == name,]
  sponsor_rows <- filter(klarner_sub, candid %in% na.omit(this_sponsor_LES$klarner_id)) %>% distinct()
  if(nrow(sponsor_rows) == 0){
    ### Check Losers
    sponsor_rows <- filter(klarner, candid %in% na.omit(this_sponsor_LES$klarner_id ))
    if(nrow(sponsor_rows) >= 1){
      LES[LES$sponsor == name,]$party <- sponsor_rows[1,]$partyt
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
          if(nrow(sponsor_sub) >= 2){
            if(sponsor_sub$year[1] == sponsor_sub$year[2]){
              sponsor_sub = filter(sponsor_sub, etype %in% c('g', 'gs') )
            }
          }
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$district <- sponsor_sub[1,]$dno
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$party <- sponsor_sub[1,]$partyt
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$exper <- sponsor_sub[1,]$exper
        }else{
          if(nrow(sponsor_rows) > 0){
            sponsor_rows <- arrange(sponsor_rows, year)
            if(nrow(filter(sponsor_rows, etype %in% c("g", "gs"))) > 0 ){
              sponsor_rows <- filter(sponsor_rows, etype %in% c("g", "gs"))
            }
            LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$party <- ifelse(is.logical(na.omit(unique(sponsor_rows$partyt))), NA, na.omit(unique(sponsor_rows$partyt))[1] )
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

### Not In Klarner -- ALL FROM 2017-2018
fill_missing <- data.frame(LES_name = "pellegrino, christine", new_name = 'pellegrino, christine', party = 'd', district = 9, exper = 'none')
# Stern != the stern that LOST in prior elections and was a Repub.
fill_missing <- add_row(fill_missing, LES_name = "stern", new_name = 'stern, steven h.', party = 'd', district = 10, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "epstein", new_name = 'epstein, harvey', party = 'd', district = 74, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "espinal, ari", new_name = 'espinal, ari', party = 'd', district = 39, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "rosenthal, daniel", new_name = 'rosenthal, daniel', party = 'd', district = 27, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "fernandez", new_name = 'fernandez, nathalie', party = 'd', district = 80, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "ashby", new_name = 'ashby, jacob', party = 'r', district = 107, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "taylor", new_name = 'taylor, al', party = 'd', district = 71, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "smith", new_name = 'smith, doug m.', party = 'r', district = 5, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "tague", new_name = 'tague, christopher', party = 'r', district = 102, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "mikulin", new_name = 'mikulin, john', party = 'r', district = 17, exper = 'none')
# Bohen = Democratic-Caucusing Independent
fill_missing <- add_row(fill_missing, LES_name = "bohen", new_name = 'bohen, erik', party = 'd', district = 142, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "benjamin, brian", new_name = 'benjamin, brian', party = 'd', district = 30, exper = 'none')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)
rm(fill_missing, i)


########## 
### Fix Nonmaj Party Codes
#########
# filter(LES, party == "nonmaj") %>% select(1:6, party, district)
# filter(klarner, cand == "hoyt, william b. iii") %>% select(cand, year, sen, etype, outcome, partyz, partyt)

## Coppola was Dem when won special in Feb 2000, but lost D primary in Nov 2000 -- Ran anyway as indep/conservative and lost
# -- Challenged inc in both 2002/2004 in D primary, lost, but ran in general as R both times
LES[LES$sponsor == "coppola, alfred t." & LES$term == "1999_2000",]$party <- "d"
### Hoyt has been a Dem whole career
LES[LES$sponsor == "hoyt, william b. iii" & LES$term == "2001_2002",]$party <- "d"
### Won 2006 spcial as Dem, lost Dem primary in Sep, ran in general as Working Families cand
LES[LES$sponsor == "friedman, sylvia" & LES$term == "2005_2006",]$party <- "d"
### Coppola: Won 2006 special as Dem, lost Sep. primary, ran on conservative ticket
LES[LES$sponsor == "coppola, mark a." & LES$term == "2005_2006",]$party <- "d"
#### Won 2016 special as Dem, lost D primary for general, ran in general as Womens Equality party cand
LES[LES$sponsor == "cancel, alice" & LES$term == "2015_2016",]$party <- "d"


#########################################################
############ Match to Hall/Fouirnaies
########################################################

library(readstata13)

### Hall and Fournaies 2018, Covers 1988 - 2014
### --> https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/PGCVDP
hf_data <- read.dta13("../../../State Legislative Data/Hall_Fouirnaies_2018/How_Do_interest_Groups_Seek_Access_to_Committees.dta")
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
  filter(dup == TRUE) #%>% View()

### Set Committees to NA for Years without Data -- May have matched candids in year range
set_NA <- colnames(hf_data)
set_NA <- set_NA[!(set_NA %in% c("CandId", 'chamber', "year", "term")) ]
LES[LES$term == '2017_2018', set_NA] <- NA
rm(hf_data, set_NA)

#################################
### Shor and McCarty Data, 1993 - 2016
####################################

# *** NEW YORK SM Data starts in 1994 ***

ideo <- readstata13::read.dta13("../../../State Legislative Data/Shor_McCarty_Data/shor_mccarty_1993_2016_individual_data_May_2018.dta")
ideo <- filter(ideo, st == this_state) %>% select(-st_id)
ideo$match_name <- str_extract(tolower(ideo$name), '[a-z]+, [a-z]+')
ideo$match_name <- ifelse(is.na(ideo$match_name), tolower(ideo$name), ideo$match_name)
ideo$last_name <- gsub(',.+', '', tolower(ideo$name))

### Name Correction
ideo[ideo$name == "Diaz Jr, Ruben",]$name <- "Diaz Sr, Ruben"

## **** A HANDFUL OF 2015-2016 DUPLICATES ---> Eliminating for now...
#ideo <- filter(ideo, !duplicated(paste(name, party, sep = '-')))

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

LES[LES$sponsor %in% c('coppola, alfred t.', 'coppola, mark a.', 'diaz, ruben sr.', 'miller, melissa l.', 'delarosa, carmen n.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

LES[LES$sponsor %in% c('jones, d. billy'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES -- MOst of remaining missing = 2015-2016 special
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('cast', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'boyland, william f.', SM_name = 'Boyland, William')
name_matches <- add_row(name_matches, LES_name = 'boyland, william f. jr.', SM_name = 'Boyland, William Jr.')
# name_matches <- add_row(name_matches, LES_name = 'akshar, frederick j., ii', SM_name = 'zzzzzzz')
## *** SM BORELLI == Mispelled
name_matches <- add_row(name_matches, LES_name = 'borelli, joseph', SM_name = 'Borrelli, Joe')
# name_matches <- add_row(name_matches, LES_name = 'cancel, alice', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'castorina, ronald, jr.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'comrie, leroy g. jr.', SM_name = 'Comrie Jr, Leroy')
## *** Coppolas are collapsed into 1 observation (Senate 2000, 2006) -- Attributing to both as balanced and quite liberal (so should be directionally correct)
name_matches <- add_row(name_matches, LES_name = 'coppola, alfred t.', SM_name = 'Coppola')
name_matches <- add_row(name_matches, LES_name = 'coppola, mark a.', SM_name = 'Coppola')
## *** SM name fixed above: it's SR not JR
name_matches <- add_row(name_matches, LES_name = 'diaz, ruben sr.', SM_name = 'Diaz Sr, Ruben')
## *** Unclear if he is actually JR but timing matches up
name_matches <- add_row(name_matches, LES_name = 'flanagan, john', SM_name = 'Flanagan Jr, John J')
name_matches <- add_row(name_matches, LES_name = 'hooper, earlene hill', SM_name = 'Hill, Earlene H.')
name_matches <- add_row(name_matches, LES_name = 'rivera, j. gustavo', SM_name = 'Rivera, J.')
name_matches <- add_row(name_matches, LES_name = 'rivera, jose', SM_name = 'Rivera, José')
name_matches <- add_row(name_matches, LES_name = 'sepulveda, luis r.', SM_name = 'Sepúlveda, Luis Sepulveda')
# name_matches <- add_row(name_matches, LES_name = 'williams, jaime r.', SM_name = 'zzzzzzz')
## *** Zebrowskis are collapsed as JR and SR --> Matching ONLY to JR as he represents majority of data
name_matches <- add_row(name_matches, LES_name = 'zebrowski, kenneth paul', SM_name = 'Zebrowski Jr, Kenneth P')
# name_matches <- add_row(name_matches, LES_name = 'zebrowski, kenneth peter', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}


#######################
### PARTY SWITCHERS
#############################

#### Check for Potential Switchers
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()

## **** Nancy Lorraine Hoffman --- Switched to Republican in ~1998: https://www.nytimes.com/2004/11/10/nyregion/in-syracuse-a-shaky-hold-on-a-senate-seat.html
## -- Note: if go back further in time with data, will need to mach earlier observations to Dem. Score in SM data
LES[LES$sponsor == 'hoffmann, nancy larraine' & LES$term == "1999_2000",]$party <- 'r'
LES[LES$sponsor == 'hoffmann, nancy larraine' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Hoffmann, Nancy Larraine' & ideo$party == 'R',]$name
LES[LES$sponsor == 'hoffmann, nancy larraine' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Hoffmann, Nancy Larraine' & ideo$party == 'R',]$party
LES[LES$sponsor == 'hoffmann, nancy larraine' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Hoffmann, Nancy Larraine' & ideo$party == 'R',]$np_score
# LES[LES$sponsor == 'hoffmann, nancy larraine' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Hoffmann, Nancy Larraine' & ideo$party == 'D',]$name
# LES[LES$sponsor == 'hoffmann, nancy larraine' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Hoffmann, Nancy Larraine' & ideo$party == 'D',]$party
# LES[LES$sponsor == 'hoffmann, nancy larraine' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Hoffmann, Nancy Larraine' & ideo$party == 'D',]$np_score

## **** Fred Thiele Jr --- Republican 1989 - 2009, switched to Indep Oct 1 2009 and subsequently caucused with Dems: https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=6129
# LES[LES$sponsor == 'thiele, fred w. jr.', c("term", "party")]
LES[LES$sponsor == 'thiele, fred w. jr.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Thiele, Fred Jr.' & ideo$party == 'R',]$name
LES[LES$sponsor == 'thiele, fred w. jr.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Thiele, Fred Jr.' & ideo$party == 'R',]$party
LES[LES$sponsor == 'thiele, fred w. jr.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Thiele, Fred Jr.' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'thiele, fred w. jr.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Thiele, Fred Jr.' & ideo$party == 'D',]$name
LES[LES$sponsor == 'thiele, fred w. jr.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Thiele, Fred Jr.' & ideo$party == 'D',]$party
LES[LES$sponsor == 'thiele, fred w. jr.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Thiele, Fred Jr.' & ideo$party == 'D',]$np_score

## **** Michael Spano -- Swtiched R to D in July 2007: https://www.nytimes.com/2007/07/12/nyregion/12mbrfs-SWITCH.html
# LES[LES$sponsor == 'spano, michael j.', c("term", "party")]
LES[LES$sponsor == 'spano, michael j.' & LES$term == "2007_2008",]$party <- 'd'
LES[LES$sponsor == 'spano, michael j.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Spano, Michael J' & ideo$party == 'R',]$name
LES[LES$sponsor == 'spano, michael j.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Spano, Michael J' & ideo$party == 'R',]$party
LES[LES$sponsor == 'spano, michael j.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Spano, Michael J' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'spano, michael j.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Spano, Mike' & ideo$party == 'D',]$name
LES[LES$sponsor == 'spano, michael j.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Spano, Mike' & ideo$party == 'D',]$party
LES[LES$sponsor == 'spano, michael j.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Spano, Mike' & ideo$party == 'D',]$np_score

## **** Ronald Tocci -- Lost D Primary in 2002, ran in general as R and won
LES[LES$sponsor == 'tocci, ronald c.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Tocci, Ronald' & ideo$party == 'R',]$name
LES[LES$sponsor == 'tocci, ronald c.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Tocci, Ronald' & ideo$party == 'R',]$party
LES[LES$sponsor == 'tocci, ronald c.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Tocci, Ronald' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'tocci, ronald c.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Tocci, Ronald C.' & ideo$party == 'D',]$name
LES[LES$sponsor == 'tocci, ronald c.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Tocci, Ronald C.' & ideo$party == 'D',]$party
LES[LES$sponsor == 'tocci, ronald c.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Tocci, Ronald C.' & ideo$party == 'D',]$np_score

### Olga Mendez -- Left Democratic party in December 2002 --> Served as R in 2003-2004
LES[LES$sponsor == 'mendez, olga a.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Mendez, Olga' & ideo$party == 'R',]$name
LES[LES$sponsor == 'mendez, olga a.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Mendez, Olga' & ideo$party == 'R',]$party
LES[LES$sponsor == 'mendez, olga a.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Mendez, Olga' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'mendez, olga a.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Mendez, Olga A' & ideo$party == 'D',]$name
LES[LES$sponsor == 'mendez, olga a.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Mendez, Olga A' & ideo$party == 'D',]$party
LES[LES$sponsor == 'mendez, olga a.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Mendez, Olga A' & ideo$party == 'D',]$np_score

### Joseph Robach -- Left Democratic party in 2002, uncler when, ran for Senate as R
LES[LES$sponsor == 'robach, joseph e.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Robach, Joseph E.' & ideo$party == 'R',]$name
LES[LES$sponsor == 'robach, joseph e.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Robach, Joseph E.' & ideo$party == 'R',]$party
LES[LES$sponsor == 'robach, joseph e.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Robach, Joseph E.' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'robach, joseph e.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Robach, Joseph E.' & ideo$party == 'D',]$name
LES[LES$sponsor == 'robach, joseph e.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Robach, Joseph E.' & ideo$party == 'D',]$party
LES[LES$sponsor == 'robach, joseph e.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Robach, Joseph E.' & ideo$party == 'D',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1999 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1999:2020) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzzz) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1999 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(2009:2010, 2019:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1999:2008, 2011:2018) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1

#### NOTES + FIXES FOR Cross-Party Caucusing in 2009-2010, 2013-2014, 2017-2018
# 2009-2010: Dems won control, but handful of dems refused to support party leadership; both parties had control for a while
# ---> Ultimately, after a bit of stalemate, Pedro Espada (D) became majority leader --> D Control
# 2013-2014: "Independent Democratic Conference" (which formed in prior term) formed coalition with Republicans for Majority
# 2015-2016: Republicans won outright control -- IDC still worked with them, but not central piece of the coalition..?
# 2017-2018 - The IDC rejoined Dems BUT Simcha Felder (D) caucused with the Republicans to give them majority - https://www.vox.com/2018/4/23/17259112/new-york-special-election-shelley-mayer-julie-killian-simcha-felder
# ------> This happened over the course of the first year, up to April 2018 when the IDC dissolved and rejoined Dems...
# ------> Oddly it was Felder who urged them to rejoin... Weird: https://www.nytimes.com/2017/05/24/nyregion/simcha-felder-independent-democratic-conference-senate.html
# * Members: Jeff Klein (leader); Marisol Alcantara; Tony Avella; Jesse Hamilton; Jose Peralta; David Valesky
# * ------- David Carlucci; Diane Savino
# ---> See: (1) https://www.vox.com/policy-and-politics/2018/9/14/17859200/idc-new-york-primaries-democrats-biaggi-klein
# ---> See: (2) https://en.wikipedia.org/wiki/Independent_Democratic_Conference

## 2013-2014
# Source: Announcment pre 2013-2014 session: https://www.nysenate.gov/newsroom/press-releases/independent-democratic-conference-senate-republicans-announce-creation
# ---> Klein was formally part of the leadership -- lost that position in 2015 when R's took more seats
idc_2013 <- c("klein, jeffrey", "savino, diane j.", "valesky, david j.", "carlucci, david s.", "smith, malcolm a.")
LES[LES$term == '2013_2014' & LES$chamber == "Senate" & LES$sponsor %in% idc_2013,]$in_majority <- 1

## 2017-2018 -- Simcha Felder caucuased with R's --> Majority
LES[LES$term == '2017_2018' & LES$chamber == "Senate" & LES$sponsor == "felder, simcha",]$in_majority <- 1


############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

##### Drop Nicknames
# filter(LES, grepl('\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
LES$sponsor <- str_trim(gsub('  +', ' ', gsub('\\(.+\\)', '', LES$sponsor)))

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)
LES[LES$sponsor == "dandrea, robert 1",]$sponsor <- "d'andrea, robert"


### Manual Fixes
LES[LES$sponsor == "espinal, ari",]$sponsor <- 'espinal, aridia'
LES[LES$sponsor == "hyerspencer, donna j.",]$sponsor <- "hyer-spencer, donna janele"
LES[LES$sponsor == "brookkrasny, alec",]$sponsor <- "brook-krasny, alec"
LES[LES$sponsor == "delarosa, carmen n.",]$sponsor <- "de la rosa, carmen n."
LES[LES$sponsor == "hassellthompson, ruth h.",]$sponsor <- "hassell-thompson, ruth"
LES[LES$sponsor == "jeanpierre, kimberly",]$sponsor <- "jean-pierre, kimberly"
LES[LES$sponsor == "peoplesstokes, crystal d.",]$sponsor <- "peoples-stokes, crystal d."
LES[LES$sponsor == "phefferamato, stacey g.",]$sponsor <- "pheffer-amato, stacey g."
LES[LES$sponsor == "rhoddcummings, pauline",]$sponsor <- "rhodd-cummings, pauline"
LES[LES$sponsor == "stewartcousins, andrea",]$sponsor <- "stewart-cousins, andrea"


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

# ggsave(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Figures/{this_state}_LES_ridgeplot.pdf"), height = 8, width = 7)

#########
ggplot2::ggplot(LES, aes(x = np_score, y = LES, color = party)) + 
  geom_point() + 
  geom_smooth(formula = "y ~ x", method = 'lm', se = FALSE) + 
  facet_wrap(~ term) + 
  scale_color_manual(values=c("dodgerblue2", "red2", 'gray50'))

##### CHECK OUTLIERS ---- 
# filter(LES, party == 'd' & np_score > 0) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & np_score < 0) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')



