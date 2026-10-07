
################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** SOUTH CAROLINA *** BY SESSION
##############################################################

###################################
## SPECIAL SESSIONS:
## ---- Appear to be folded into the full term -- website has no delineation between regular and special session bills
## MEMBER LISTS:
## ---- HOUSE: https://www.scstatehouse.gov/member.php?chamber=H
## ---- SENATE: https://www.scstatehouse.gov/member.php?chamber=S
## PROCESS/RULES:
## ---- Senate Rules : https://scstatehouse.gov/senatepage/SRULES2019.pdf
## Sponsorship/Authorship
## ---- Commitee Sponsors Permitted 
## ---- Does not appear that multiple primary sponsors are permitted -- but script is validating this.
###########################
## NOTES:
## (1) Lots of party switches... May need to validate parties for some of the early years, when klarner and SM disagree...
## (2) Bills are sometimes referred to COUNTY DELEGATIONS instead of committees -- E.G.: H3072 1989-1990
## ----------> Treating as if referred to committee at present
## (3) Assume out of legislature if suspended??? See Jim Merrill in 2017 term --> Currently dropping
## (4) Pretty large number of Senate Bills skip committee -- doesn't seem to be a fluke -- most are placed on local/uncontested calendar and go straing to 2nd reading
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

this_state <- 'SC'
min_year <-1989
max_year <- 2018
keep_types <- c("HB", "SB")
spec_elec_codes <- c('s', 'gs')
house_term_length <- 2
sen_term_length <- 4 # STAGGERED? NO

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
         bill_id = gsub("B", "", bill_id),
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
klarner[klarner$cand == 'manley, sara',]$cand <- "manly, sarah g."
klarner[klarner$cand == 'hanly, sarah g.',]$cand <- "manly, sarah g."
klarner[klarner$cand == 'hattos, james g.',]$cand <- "mattos, james g."
klarner[klarner$cand == 'geise, warren k.',]$cand <- "giese, warren k."
klarner[klarner$cand == 'sonkinon, marion h.',]$cand <- "kinon, marion h. son"
# ---> ID's Will be off for some of these, but better than full mismatch...

## Official Results have Peeler Losing in 1992, but he is defintiely in office and official records to not note a gap (see p. 87): https://www.scvotes.org/files/ElectionReports/Election_Report_1992-1993.pdf
klarner[klarner$cand == 'peeler, harvey' & klarner$year == 1992,]$outcome <- 'w'
klarner[klarner$cand == 'sossman, larry' & klarner$year == 1992,]$outcome <- 'l'

## Adding Missing Middle Initials
klarner[klarner$cand == 'neal, joseph h.' & klarner$middle == '',]$middle <- 'h'
klarner[klarner$cand == 'neal, james m. (jimmy)' & klarner$middle == '',]$middle <- 'm'
klarner[klarner$cand == 'smith, james emerson jr.' & klarner$middle == '',]$middle <- 'e'

## Other Notes
# -- Mia S. McCleod == Mia Butler Garrick

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[9]

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
  bills <- arrange(bills, bill_number)
  
  ### Clean Term/Session Variables
  bills$term <- t_yrs
  
  ### Check for duplicates
  if(nrow(bills) != nrow(distinct(bills))){
    cat(" \n ~~~> DUPLICATE BILLS \n\n .")
    break
  }
  
  ######## Standardize the Bill IDs
  bills <- rename(bills, bill_id = bill_number)
  
  ############### Drop Resolutions, Messages, Communications, Reports
  # bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
  # table(bills$bill_type)
  bills <- filter(bills, !grepl('resolution', tolower(bill_type) )) %>% select(-bill_type)
  
  ##########################
  ####### Standardize Sponsors
  
  bills$primary_sponsor <- tolower(bills$primary_sponsor)
  bills$primary_sponsor <- gsub('á', 'a', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('é', 'e', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('ó', 'o', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('í', 'i', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('ñ', 'n', bills$primary_sponsor)
  
  bills$sponsors <- tolower(bills$sponsors)
  bills$sponsors <- gsub('á', 'a', bills$sponsors)
  bills$sponsors <- gsub('é', 'e', bills$sponsors)
  bills$sponsors <- gsub('ó', 'o', bills$sponsors)
  bills$sponsors <- gsub('í', 'i', bills$sponsors)
  bills$sponsors <- gsub('ñ', 'n', bills$sponsors)
  
  ### Manual Fixes to Improve Matching
  if(t_yrs %in% c("1989_1990", "1991_1992", "1993_1994")){### Two Alexanders, M.O. and T.C., but only M.O. has initials
    bills[bills$primary_sponsor == 'representative alexander',]$primary_sponsor <- 'representative t.c. alexander'
    bills$sponsors <- gsub('^alexander', 't.c. alexander', bills$sponsors)
    bills$sponsors <- gsub('; alexander', '; t.c. alexander', bills$sponsors)
  }
  if(t_yrs %in% c("1989_1990", "1991_1992")){
    bills[bills$primary_sponsor == 'representative cork',]$primary_sponsor <- 'representative h.a. cork'
    bills$sponsors <- gsub('^cork', 'h.a. cork', bills$sponsors)
    bills$sponsors <- gsub('; cork', '; h.a. cork', bills$sponsors)
  }
  if(t_yrs %in% c('1991_1992')){ ### Four Martins. "L.M. Martin" = Morgan Martin. First Initial CHanged Below. "Martin" = 'L.A. Martin"
    bills[bills$primary_sponsor == 'representative martin',]$primary_sponsor <- 'representative l.a. martin'
    bills$sponsors <- gsub('^martin', 'l.a. martin', bills$sponsors)
    bills$sponsors <- gsub('; martin', '; l.a. martin', bills$sponsors)
    
    bills[bills$primary_sponsor == 'senator hayes',]$primary_sponsor <- 'senator r.w. hayes'
    bills$sponsors <- gsub('^hayes', 'r.w. hayes', bills$sponsors)
    bills$sponsors <- gsub('; hayes', '; r.w. hayes', bills$sponsors)
  }
  if(t_yrs %in% c('1995_1996')){ ### Two Whippers -- J. Seth and Lucille Simmons --- J. Seth just listed as Whipper
    bills[bills$primary_sponsor == 'representative whipper',]$primary_sponsor <- 'representative j.s. whipper'
    bills$sponsors <- gsub('^whipper', 'j.s. whipper', bills$sponsors)
    bills$sponsors <- gsub('; whipper', '; j.s. whipper', bills$sponsors)
  }
  if(t_yrs %in% c('2017_2018')){ ### Two Rivers -- S. Rivers and Rivers --> Adding INitial
    bills[bills$primary_sponsor == 'representative rivers',]$primary_sponsor <- 'representative m.f. rivers'
    bills$sponsors <- gsub('^rivers', 'm.f. rivers', bills$sponsors)
    bills$sponsors <- gsub('; rivers', '; m.f. rivers', bills$sponsors)
  }
  
  ### LES Sponsor Var
  # *** Multi=primary will be tricky if exists because of how searching and how data is recorded...
  if( any(grepl(';', bills$primary_sponsor))){
    cat('\n')
    cat('------------> CHECK MULTI-PRIMARY SPONSORED BILLS **************** BREAK'); break
  }
  
  bills$LES_sponsor <- gsub('^representative |^senator ', '', bills$primary_sponsor)
  # bills$LES_sponsor <- str_trim(gsub('\\&.+', '', bills$LES_sponsor))
  
  #### Adjusting for Bills By Request -- Need to do BEFORE splitting
  if(any(grepl("request", bills$LES_sponsor))){
    print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
    # cat('\n'); cat(glue("-----> KEEPING {nrow(filter(bills, grepl('request', LES_sponsor)))} bill(s) introduced BY REQUEST"))
    # bills$LES_sponsor <- str_trim(gsub('\\(by request\\)', '', bills$LES_sponsor))
  }

  #### Drop Uncoded Committees
  if(any(grepl('^senate|^house|committee', bills$LES_sponsor))){
    cat('\n')
    cat(glue('---> Dropping {sum(grepl("^senate|^house|committee", bills$LES_sponsor))} Committed Sponsored Bills (N = {nrow(bills)})'))
    bills <- filter(bills, !grepl('^senate|^house|committee', LES_sponsor))
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
  # *** For SOUTH CAROLINA: Bill Numbers Uniquely Identify Bills across regular/specials in a legislative term
  # --> Merging on ID and TERM only, not session
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
  
  ##################################################
  ############### Code Commemorative
  ##################################################
  
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session'))
  # table(bills$commem)
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  ##################################################
  ############### Code Bill History
  ##################################################
  bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_yrs}.csv")
  bill_hist <- read.csv(bill_hist_path)
  
  ######## Standardize BillHist Bill IDs
  bill_hist <- rename(bill_hist, bill_id = bill_number) 
  
  ### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
  bill_hist$term <- t_yrs
  
  ### Order by Order
  bill_hist <- arrange(bill_hist, term, session, bill_id, order)
  
  ### Coding Chamber Variable + Fillin IN blanks with adjacent Observations
  if(nrow(filter(bill_hist, chamber == '')) > 0){
    bill_hist[grepl('signed by governor|^ratified|^act no\\.|^effective date|^vetoed|^became law without gov', tolower(bill_hist$action) ) & bill_hist$chamber == "",]$chamber <- 'Executive'
  }
  
  bill_hist <- bill_hist %>% 
    mutate(chamber = ifelse(chamber == '', NA, chamber)) %>% 
    group_by(bill_id) %>% 
    fill(chamber) %>% 
    ungroup()
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  aic_t <- c('committee report', 'tabled in committee', 'referred to subcommittee', 'polled out of committee', 
             'delegation report')
  # --> Per rules, polled out of committee is a vote by the committee to release it? - https://scstatehouse.gov/senatepage/SRULES2019.pdf
  # --> BILLS ARE SOMETIMES REFERRED TO COUNTY DELEGATIONS INSTEAD OF COMMITTE -- E.G.: H3072 1989-1990
  abc_t <- c('committee report: fav', 'committee report: majority fav', 'committee report: recommend', 
             'delegation report: fav', 'delegation report: majority fav', 'delegation report: recommend',
             'read second time', 'second reading', 'read third time', 'third reading', '^amended', '^debate', '^objection', 'recommitted to')
  # Recommend =  Recommended refer to different committee
  # Could ADD 'committee report: majority unfav.+minority fav... ---> But those seem likely to die given majority unfavorable
  # ---> PLaced on calendar without reference --> Skips committee
  pc_t <- c('read third time and sent to senate', 'read third time and sent to house', 'enrolled')
  ## Enrolled = check; only happens in second chamber -- will only be picked up if issues in chamber coding
  law_t <- c('signed by governor', 'act no\\. [0-9]+', 'effective date [0-9]+')

  ### Check Actions
  # filter(bill_hist, grepl('delegation report', tolower(action))) %>% distinct(action) %>% View()
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
  
  ### Make Sure No Excess text in Bill Action
  bill_hist$action <- str_trim(bill_hist$action)
  
  ### Code History Stages for Each Bill --- Subset Hist File, Code, Save, Repeat
  options(warn = 2)
  for(i in 1:nrow(bills)){
    b_id = bills[i,]$bill_id
    s_id = bills[i,]$session
    b_spon = bills[i,]$LES_sponsor
    hist_sub <- filter(bill_hist, bill_id == b_id, session == s_id)
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
    bill_stages$bill_url <- "Search via: https://www.scstatehouse.gov/billsearch.php"
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
  bills$cospon_match <- paste(bills$LES_sponsor, bills$primary_sponsors, tolower(bills$sponsors), sep = '; ')
  for(i in 1:nrow(all_sponsors)){
    c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
    ### ONLY need to adjust this way if sponsored and cosponsored column are the same
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  }
  bills <- select(bills, -cospon_match)
  # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))

  #######################
  #### CLEAN NAMES
  all_sponsors$last_name <- gsub('^[a-z]\\.[a-z]\\.[a-z]\\. |^[a-z]\\.[a-z]\\. |^[a-z]\\. ', '', all_sponsors$LES_sponsor)
  first_middle <- ifelse(grepl('\\.', all_sponsors$LES_sponsor), str_extract(all_sponsors$LES_sponsor, '^[a-z]\\.[a-z]\\.[a-z]\\. |^[a-z]\\.[a-z]\\. |^[a-z]\\. '), '')
  first_middle <- gsub('\\.', '', str_split_fixed(str_trim(first_middle), '\\.', 3))
  all_sponsors$first_name <- first_middle[,1]
  all_sponsors$middle_name <- first_middle[,2]
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  rm(first_middle)
  
  #### Update Last Names for Matching 
  if(t_yrs %in% c("1991_1992")){
    all_sponsors[all_sponsors$LES_sponsor == 'l.m. martin',]$first_name <-  "m"
  }
  if(t_yrs %in% c('1993_1994', '1995_1996', '1997_1998', '1999_2000') ){
    all_sponsors[all_sponsors$LES_sponsor == 'd. smith', c('first_name', "middle_name")] <- list('w', 'd')
    all_sponsors[all_sponsors$LES_sponsor == 'r. smith', c('first_name', "middle_name")] <- list('j', 'r')
  }
  if(t_yrs %in% c('1997_1998', '1999_2000') ){
    all_sponsors[all_sponsors$LES_sponsor == 'j. smith',]$middle_name <- 'e'
  }
  if(t_yrs %in% c('1999_2000') ){
    all_sponsors[all_sponsors$LES_sponsor == 'm. mcleod',]$first_name <- 'e' # Eugene Belton Mcleod Jr -- not sure where the M comes from but it matches in data
  }
  if(t_yrs %in% c('1999_2000', '2001_2002') ){
    all_sponsors[all_sponsors$LES_sponsor == 'meacham-richardson',]$last_name <- 'meacham'
  }else if(t_yrs == '2003_2004'){
    all_sponsors[all_sponsors$LES_sponsor == 'richardson' & all_sponsors$chamber == "H",]$last_name <- 'meacham'
  }
  if(t_yrs %in% c('2001_2002', '2003_2004', '2005_2006', '2007_2008', '2009_2010') ){
    all_sponsors[all_sponsors$LES_sponsor %in% c('a. young', 'young', 'a.d. young'),]$last_name <- 'youngbrickell' ### Annette D. Young (Brickell)
  }
  if(t >= 2001 & t <= 2018 ){
    all_sponsors[all_sponsors$LES_sponsor == 'g.m. smith',]$first_name <- 'm' # G. Murrell Smith
  }
  if(t_yrs %in% c("2009_2010", '2011_2012')){
    all_sponsors[all_sponsors$LES_sponsor == 'h.b. brown',]$first_name <- 'b' # H. Boyd Brown
  }
  if(t_yrs %in% c('2011_2012')){ ### Mia S. Mcleod = Mia Butler Garrick --> Eventually just goes by MS MCLEOD and matches to that in klarner
    all_sponsors[all_sponsors$LES_sponsor == 'butler garrick',]$last_name <- 'butler' 
  }else if(t_yrs == '2013_2014'){
    all_sponsors[all_sponsors$LES_sponsor == 'm.s. mcleod',]$last_name <- 'garrick'
  }
  if(t_yrs %in% c('2013_2014', '2015_2016', '2017_2018') ){
    all_sponsors[all_sponsors$LES_sponsor == 'norrell',]$last_name <- 'powersnorrell'     
  }
  if(t_yrs %in% c("2015_2016")){ # Donna Hicks --> Donna Wood
    all_sponsors[all_sponsors$LES_sponsor == 'hicks',]$last_name <- 'wood'     
  }
  if(t_yrs %in% c("2015_2016", '2017_2018')){
    all_sponsors[all_sponsors$LES_sponsor == 'v.s. moss',]$first_name <- 's'     
  }
  if(t_yrs %in% c('2017_2018')){
    all_sponsors[all_sponsors$LES_sponsor == 'm.b. matthews',]$last_name <- 'brightmatthews'     
  }
  
  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  H_elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  # S_elec_year <- H_elec_year - 2 ### Staggered so need to get T - 1 and T - 3
  if(H_elec_year %% 4 == 0){ # Senate elections = 2000, 2004,..., 2016, 2020
    S_elec_year <- H_elec_year
  }else{
    S_elec_year <- H_elec_year - 2
  }
  
  ### For Senate: Sente Election Year to House Year + 1 (so if 2000, 2000-2001; if 1998, 1998 to 2001)
  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% H_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% S_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz, middle) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)
  
  ############################################################
  ############## Match Sponsors Names to Klarner Data
  ################################################################################
  
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), '')))
  klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]')))  
  
  ### Edit Match Name
  if(t_yrs %in% c("1991_1992")){
    klarner_sub[klarner_sub$cand == 'elliott, d. larry',]$match_name <- 'elliott, l'
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
      ### CHeck Middle If Too Man
      if(length(m_sub) > 1){
        k_matches <- k_matches[m_sub,]
        m_sub <- which(substring(k_matches$middle, 1, 1) == all_sponsors[i,]$middle_name)
      }
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
  if(t_yrs == "1989_1990"){
    all_sponsors[all_sponsors$LES_sponsor == "h.a. cork" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "1991_1992"){
    all_sponsors[all_sponsors$LES_sponsor == "r.w. hayes" & all_sponsors$chamber == "S", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == '2015_2016'){
    all_sponsors[all_sponsors$LES_sponsor == "m.b. matthews" & all_sponsors$chamber == "S", c("klarner_name", "klarner_id", "elec_year")] <- NA
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
  if(t_yrs == "1989_1990"){
    km <- filter(km, !(cand == 'manly, sarah g.' & sen == 1) )
    km <- filter(km, cand != 'dewitt, e.')
    km <- filter(km, cand != 'scott, john')
  }else if(t_yrs == '1991_1992'){
    km <- filter(km, !(cand %in% c('blanding, larry', 'gordon, b. j. jr.', 'fant, ennis m.', 'lee, william richard')))
    km <- filter(km, !(cand %in% c('lindsay, john c.', 'mcleod, peden', 'manly, sarah g.')))
  }else if(t_yrs == '1995_1996'){
    km <- filter(km, cand != 'macaulay, alexander s. (alex)')
  }else if (t_yrs == '1999_2000'){
    km <- filter(km, cand != 'williams, dewitt')
    km <- filter(km, cand != 'rose, michael t.')
  }else if (t_yrs == '2003_2004'){
    km <- filter(km, cand != 'bauer, andre')
    km <- filter(km, cand != 'wilson, addison g.')
    km <- filter(km, cand != 'saleeby, edward e.')
    km <- filter(km, cand != 'passailaigue, ernest l.')
  }else if(t_yrs == '2007_2008'){
    km <- filter(km, cand != 'smith, j. verne')
  }else if(t_yrs == '2009_2010'){
    km <- filter(km, cand != 'henderson, phyllis')
    km <- filter(km, cand != 'phillips, olin r.')
  }else if(t_yrs == '2011_2012'){
    km <- filter(km, cand != 'harvin, cathy')
    km <- filter(km, cand != 'mulvaney, mick')
  }else if(t_yrs == '2015_2016'){
    km <- filter(km, cand != 'crawford, kris')
    km <- filter(km, cand != 'mcgill, john yancey')
    km <- filter(km, cand != 'ford, robert')
  }else if(t_yrs == '2017_2018'){
    km <- filter(km, cand != 'merrill, james h.')
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
  
  ###################################################
  ###### Estimate Scores + Add in Relatd Variables
  ##################################################
  
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
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES) # 
rm(t, terms, klarner_gs, m_sub, c_sub, commem_bills) # 


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by SPECIAL ELECTION --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
# CONTEXT: 1989-1990+ -- Big FBI Probe --> Lots of Turnover: https://www.nytimes.com/1991/03/10/us/2-south-carolina-legislators-guilty-of-corruption.html
# CONTEXT 2: Specials took place in 1997 after Court Ordered Redistricting --> Turnover

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1989_1990 TERM! ~~~~~~~~~~~~~~ 
#     session chamber    N AIC ABC PASS LAW
# 1 1989-1990       H 1423 780 588  510 340
# 2 1989-1990       S  970 295 395  331 220
### WON SPECIAL ~ HOUSE:
# -- M.H. KINON
# -- CORK (holly) -- Won't show, last name duplicated
### IN HOUSE:
# -- WILKES; FOSTER; BAILEY (kenneth)
### DROP: 
# manly, sarah g. IN SENATE --> see below
# dewitt, e. -- Won Dec. 1990 special for subsequent House term - https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=22692
# scott, john -- Same as above
### ODD CASE:
# -- Sara Manly -- Listed twice in Klarner, both specials, one H, One S, H spelled Manley -- same district..  But Never served in Senate
# ---> Dropping the Senate Special...

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1991_1992 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 1991-1992       H 1272 603 524  450 298
# 2 1991-1992       S 1070 282 375  322 199
### WON SPECIAL ~ HOUSE:
# -- ANDERSON (r); STONE (c); INABINETT (c)
# -- HARRELSON (j) ; JENNINGS (doug)
# -- TAYLOR (Levola S) -- https://www.scstatehouse.gov/member.php?code=1809090692&chamber=H
# -- HYATT (m)
### WON SPECIAL ~ SENATE:
# -- CARMICHAEL -- *** Expelled in 1981 from senate, servd time, won special in 1991; Senate debated seating him or not!: https://www.scstatehouse.gov/query.php?search=DOC&searchtext=carmichael&category=SENATEJOURNALS&year=1991&conid=31251286&result_pos=0&keyval=S10919910516&numrows=50#OCC1
# -- CORK (holly); COURTNEY; REESE
# -- HAYES (robert) -- Last name duplicated --> won't print out
### IN HOUSE:
# -- MARCHBANKS; SHIRLEY (bob); BEATTY; SHORT; ROGERS; DERRICK; FABER; MCBRIDE; BAILEY; E DEWITT (mccraw)
### DROP
# -- blanding, larry -- Corruption probe, must have resigned, not listed in chamber
# -- gordon, b. j. jr. -- Corruption probe, must have resigned, not listed in chamber
# -- fant, ennis m. -- Corruption probe, must have resigned, not listed in chamber
# -- lee, william richard -- Resigned in 1989  - https://www.scstatehouse.gov/member.php?code=1077272598&session=108
# -- lindsay, john c. -- Resigned in 1989 - https://www.scstatehouse.gov/member.php?code=1095454414&session=108
# -- mcleod, peden -- Resigned in 1989 - https://www.scstatehouse.gov/member.php?code=1290908936&session=108
# -- manly, sarah g. -- Never served in Senate -- Odd special record in Klarner

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1993_1994 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 1993-1994       H 1500 506 531  458 281
# 2 1993-1994       S  990 314 394  353 190
### IN HOUSE:
# GRAHAM (lindsey) -- on his wikipedia, but oddly not on the SC webpage
# KINON
# WILLIAMS (dewitt)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1996 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 1995-1996       H 1326 373 426  381 244
# 2 1995-1996       S 1010 240 327  299 173
### WON SPECIAL ~ HOUSE:
# -- LEE (brenda)
# -- LOFTIS
### WON SPECIAL ~ SENATE:
# -- ALEXANDER (thomas c)
# -- FAIR
# -- S. BOAN
### DROP:
# -- macaulay, alexander s. (alex) -- resigned 1993

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 1997-1998       H 1306 371 431  406 219
# 2 1997-1998       S  840 249 326  278 161
### WON SPECIAL ~ HOUSE:
# -- JG MCABEE (see below...)
# -- MCGEE
### WON SPECIAL ~ SENATE:
# -- GROOMS
### In HOUSE:
# -- MADDOX; PARKS; MCCRAW; PHILLIPS; CANTY; KINON; HINES; WOODRUM
### IN SENATE:
# -- WILLIAMS
### ODD CASE:
# -- JG MCABEE --> Lost General in 1996 to Anne Parks, Won Special in 1997 agains Parks... weird...
# ----> https://www.ourcampaigns.com/RaceDetail.html?RaceID=481715
# ----> https://www.newspapers.com/clip/13982013/mcabee_carnell_grateful_after_winning/
# ----> Why did this happen? Mandated Specials due to court-ordered re-district (see footnote 75) - https://scholarship.law.unc.edu/cgi/viewcontent.cgi?referer=&httpsredir=1&article=3939&context=nclr

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 1999-2000       H 1244 260 336  301 199
# 2 1999-2000       S  954 242 331  292 158
### WON SPECIAL ~ HOUSE: 
# -- HUGGINS; PERRY (robert)
### WON SPECIAL ~ SENATE:
# -- BAUER; BRANTON (1997); GROOMS (1997); RICHARDSON
### IN HOUSE:
# -- TROTTER; TAYLOR; MCMAHAND; RICE; MCCRAW; SPEARMAN; BOAN; HINES (mack)
# -- HINES (jesse); WOODRUM; RISER; CAVE
#### DROP:
# -- williams, dewitt -- in senate 96-97 - https://www.scstatehouse.gov/sess119_2011-2012/bills/4362.htm
# -- rose, michael t. -- resigned in 1997 -- https://ballotpedia.org/Mike_Rose_(South_Carolina)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2001-2002       H 1276 337 437  409 239
# 2 2001-2002       S  886 225 308  261 138
### WON SPECIAL ~ SENATE:
# -- KUHN
### IN HOUSE: 
# -- WEBB; WEEKS; HOSEY; MACK; RIVERS

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2003-2004       H 1231 313 399  373 170
# 2 2003-2004       S  824 237 291  238 140
### WON SPECIAL ~ HOUSE:
# -- G.R. SMITH -- Despite multiple matches
### WON SPECIAL ~ SENATE:
# -- CROMER; KNOTTS (2002, via H); KUHN (2001); MALLOY; SHEHEEN
### IN HOUSE:
# -- LEE; COLEMAN; EMORY; WEEKS; SMITH (don)
### DROP:
# -- bauer, andre -- resigned to become lt gov 1/15/2003 -- https://en.wikipedia.org/wiki/Andr%C3%A9_Bauer
# -- wilson, addison g. -- won US House seat Dec 2001
# -- saleeby, edward e. -- not listed on Senate page for 2003-2004
# -- passailaigue, ernest l. -- not listed on Senate page for 2003-2004

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2005-2006       H 1184 267 366  342 208
# 2 2005-2006       S  914 516 355  308 173
### WON SPECIAL ~ HOUSEL:
# -- BANNISTER
# -- MITCHELL
### IN HOUSE:
# -- OWENS; PHILLIPS; FRYE; M. HINES; J. HINES; ANDERSON; LLOYD (died April 2005)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2007-2008       H 1114 270 343  313 184
# 2 2007-2008       S  901 494 332  292 167
### WON SPECIAL ~ HOUSE:
# -- ERICKSON
# -- HUTSON
### WON SPECIAL ~ SENATE:
# -- CAMPBELL
# -- CEIPS
# -- MASSEY
# -- VAUGHN (via H, 11/7/2006)
### IN HOUSE:
# -- WHITMIRE
# -- PHILLIPS
# -- HINSON --> resigned 12.1.2007 https://www.scstatehouse.gov/member.php?code=0847727171&session=117
# -- CHELLIS -->resigned 8/2007 --- https://thetandd.com/news/breaking-news---legislature-selects-chellis-as-treasurer/article_052b06d5-c3a6-58a7-ba4e-891634ca56b1.html
# -- BRANTLEY 
### DROP:
# -- smith, j. verne -- died Dec 2006 -- https://www.legacy.com/obituaries/greenvilleonline/obituary.aspx?n=j-verne-smith&pid=140586914

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2009-2010       H 1080 184 284  263 143
# 2 2009-2010       S  850 438 301  247 141
### WON SPECIAL ~ HOUSE:
# -- NORMAN
### IN HOUSE:
# -- AGNEW; WILLIS; COLE; PARKER; HOWARD; STEWART
### IN SENATE:
# -- NICHOLSON
# -- WILLIAMS (kent)
### DROP:
# -- henderson, phyllis -- Miscoded as special or won Dec special for seat for subsequent term.
# -- phillips, olin r. -- died dec 2008 -- https://prabook.com/web/olin_ray.phillips/564204

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2011-2012       H 1109 224 308  282 158
# 2 2011-2012       S  851 421 265  210 119
### WON SPECIAL ~ HOUSE:
# -- JOHNSON (kevin)
# -- PUTNAM
### WON SPEICAL ~ SENATE:
# -- GREGORY (chauncey)
### IN HOUSE:
# -- PARKS; TRIBBLE; BIKAS --> odd case, basically wasn't there: https://patch.com/south-carolina/easley/state-representative-says-he-was-asked-to-leave-legislature
# -- MOSS; CHUMLEY; KNIGHT; SABB
### IN SENATE:
# -- NICHOLSON; WILLIAMS (kent)
### DROP: 
# -- harvin, cathy -- died december 2010 -- https://www.scnow.com/news/article_f306bfa9-d6ca-50eb-9f8e-aa41badb0904.html
# -- mulvaney, mick -- won US House seat in 2010 elections --> resigned


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2013-2014       H 1040 231 303  271 139
# 2 2013-2014       S  723 318 274  219 151
### WON SPECIAL ~ HOUSE:
# -- BURNS
### WON SPECIAL ~ SENATE:
# -- KIMPSON
### IN HOUSE:
# -- MOSS; ANTHONY; HAYES; BRANHAM; HOSEY; SABB
### IN SENATE:
# -- WILLIAMS (kent)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2015-2016       H 1091 247 311  273 149
# 2 2015-2016       S  680 250 231  185 113
### WON SPECIAL ~ HOUSE:
# -- FRY
### WON SPECIAL ~ SENATE:
# -- KIMPSON (lagged)
# -- M.B. MATTHEWS -- last name duplicate, won't print
### IN HOUSE:
# -- RILEY; DOUGLAS; KIRBY; HOSEY; ANDERSON; BRADLEY
### IN SENATE:
# -- WILLIAMS
### DROP
# -- crawford, kris -- resigned in December 2014 -- https://www.scnow.com/news/local/article_4f7c89d6-7fcd-11e4-ab2e-d7b28d47dcdb.html
# -- mcgill, john yancey -- resigned to become lt gov in june 2014
# -- ford, robert -- resigned in may 2013 following scandal

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2017-2018       H 1118 258 308  273 149
# 2 2017-2018       S  672 208 197  168 105
### WON SPECIAL ~ HOUSE:
# -- BRAWLEY; BRYANT; HENDERSON-MYERS; 
# -- MACE; PENDARVIS; TRANTHAM
### WON SPECIAL ~ SENATE:
# -- CASH
### IN HOUSE: 
# -- WHITMIRE; WEST; MITCHELL (resigned may 2017)
# -- ATKINSON; JORDAN; THIGPEN
# -- BLACKWELL; CASKEY
### DROP:
# merrill, james h. -- suspended in Dec 2016 after indictment; resigned august 2017 -- https://www.postandcourier.com/politics/rep-jim-merrill-indicted-in-s-c-statehouse-probe-suspended/article_fb72da58-c236-11e6-b694-bfc5d6df8e2d.html 



# filter(klarner, grepl("crawford, kr", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 12 & sen == 0 & outcome == 'w') %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)


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
LES[LES$data_name %in% "kuhn",]$klarner_id <- NA
LES[LES$data_name %in% "kuhn",]$klarner_name <- NA
LES[LES$data_name %in% "kuhn",]$sponsor <- 'kuhn, john r.'

### ****Still missing***** ---> Rest are not in Klarner or 2017-2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[15]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid) # %>%  View()
# rm(name, missing, name_sub, still_missing)


### Fix Missing
name_matches <- data.frame(LES_name = 'anderson', k_name = 'anderson, ralph')
# name_matches <- add_row(name_matches, LES_name = 'carmichael, a', k_name = 'zzzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'cork', k_name = 'cork, holly')
name_matches <- add_row(name_matches, LES_name = 'lee', k_name = 'lee, brenda')
name_matches <- add_row(name_matches, LES_name = 'alexander', k_name = 'alexander, thomas c.')
name_matches <- add_row(name_matches, LES_name = 'smith, g', k_name = 'smith, garry r.')
name_matches <- add_row(name_matches, LES_name = 'knotts', k_name = 'knotts, jake')
name_matches <- add_row(name_matches, LES_name = 'cromer', k_name = 'cromer, ronnie w.')
name_matches <- add_row(name_matches, LES_name = 'sheheen', k_name = 'sheheen, vincent')
name_matches <- add_row(name_matches, LES_name = 'mitchell', k_name = 'mitchell, harold jr.')
name_matches <- add_row(name_matches, LES_name = 'johnson', k_name = 'johnson, kevin l.')
name_matches <- add_row(name_matches, LES_name = 'gregory', k_name = 'gregory, greg')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}
rm(name_matches, i)


## MANUAL FIXES
## -- Kinon has two candid's for some reason
LES[LES$sponsor %in% "kinon, m" & LES$term %in% "1989_1990",]$klarner_id <- 208116
LES[LES$sponsor %in% "kinon, m" & LES$term %in% "1989_1990",]$klarner_name <- "kinon, marion h. son"
LES[LES$sponsor %in% "kinon, m" & LES$term %in% "1989_1990",]$sponsor <- "kinon, marion h. son"

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
rm(check_dup, exact, name_sub, missing, k_sub, t)


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

### Manually Fix Those Not in (My Subset of) Klarner
LES[LES$sponsor == "carmichael, a",]$klarner_id <- 205646
LES[LES$sponsor == "carmichael, a",]$party <- 'd' ## See debate, he won Dem Primary: https://www.scstatehouse.gov/query.php?search=DOC&searchtext=carmichael&category=SENATEJOURNALS&year=1991&conid=31251286&result_pos=0&keyval=S10919910516&numrows=50#OCC1
LES[LES$sponsor == "carmichael, a",]$district <- 28
LES[LES$sponsor == "carmichael, a",]$exper <- 'pastinc'
LES[LES$sponsor == "carmichael, a",]$sponsor <- 'carmichael, a. e. jr.' ## Matches to his earlier klarner records

LES[LES$sponsor == "kuhn, john r.",]$party <- 'r'
LES[LES$sponsor == "kuhn, john r.",]$district <- 43
LES[LES$sponsor == "kuhn, john r.",]$exper <- 'none'

### 2017-2018 -- *** If any run for reelection, won't be needed once klarner updates ****
fill_missing <- data.frame(LES_name = "pendarvis", new_name = 'pendarvis, marvin r.', party = 'd') #, district = zzzzz, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "henderson-myers", new_name = 'henderson-myers, rosalyn d.', party = 'd') #, district = zzzz, exper = 'zzzzzz')
fill_missing <- add_row(fill_missing, LES_name = "mace", new_name = 'mace, nancy', party = 'r')
fill_missing <- add_row(fill_missing, LES_name = "trantham", new_name = 'trantham, ashley b.', party = 'r') 
fill_missing <- add_row(fill_missing, LES_name = "cash", new_name = 'cash, richard j.', party = 'r') 

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  # LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  #LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)
rm(fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **

LES[LES$sponsor == 'henderson, phyllis',]$sponsor <- 'henderson, phyllis j.'
LES[LES$sponsor == 'campsen, chip',]$sponsor <- 'campsen, george (chip) iii'
LES[LES$sponsor == 'dewitt, e.',]$sponsor <- 'mccraw, e. dewitt'
LES[LES$sponsor == 'hayes, wes',]$sponsor <- 'hayes, robert wes jr.'
LES[LES$sponsor == 'knotts, jake',]$sponsor <- 'knotts, john m. (jake) jr.'
LES[LES$sponsor == 'mcginnis, alf',]$sponsor <- 'mcginnis, alfred c. sr.'
LES[LES$sponsor == 'mattos, james g.',]$sponsor <- 'mattos, james g. (jim)'

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
senate <- filter(hf_data, chamber == "Senate" & year %% 4 == 0)
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
#### FOR MT x 2: Supplemented GA Code to Match Party Switchers if One or Both Party-Terms Present in Data
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
    ideo_match <- filter(ideo[check_last,], match_name == LES[i,]$sponsor)
    # ### Try Data Name -- This can yield errors with Just Last Names in SM Data
    # ideo_match <- filter(ideo[check_last,], match_name == LES[i,]$data_name)
    ### CHeck First Initial
    if(nrow(ideo_match) != 1){
      ideo_match <- ideo[check_last,][substring(gsub('.+, ', '', tolower(ideo[check_last,]$name)), 1, 1) == substring(gsub(".+, ", '', LES[i,]$sponsor), 1, 1) ,]  
    }
    ### Check Last + First Name
    if(nrow(ideo_match) > 1){
      ideo_match <- filter(ideo_match, match_name == gsub(' [a-z]\\.$', '', LES[i,]$sponsor) )    
    }
    ### Check Party if Still Too Long
    if(nrow(ideo_match) == 2 & length(unique(ideo_match$name)) == 1 & any(ideo_match$party == 'R') &any(ideo_match$party == 'D')){
      for(p in c('d', 'r')){
        if(any(LES[LES$sponsor == LES[i,]$sponsor,]$party == p)){
          LES[LES$sponsor == LES[i,]$sponsor & LES$party == p,]$SM_name <- ideo_match[ideo_match$party == toupper(p),]$name
          LES[LES$sponsor == LES[i,]$sponsor & LES$party == p,]$SM_party <- ideo_match[ideo_match$party == toupper(p),]$party
          LES[LES$sponsor == LES[i,]$sponsor & LES$party == p,]$np_score <- ideo_match[ideo_match$party == toupper(p),]$np_score
        }
      }
    }else if(nrow(ideo_match) > 1){
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
# --> Cole row must match to Cole Sr. becaue he's 89-92 and he has an npscore but isn't listed as being in chamber 93+
# --> Garrick Double matches is right (klarner name variants)
LES[LES$sponsor %in% c('blackwell, bart', 'cole, derham jr.', 'elliott, d. larry', 'hayes, john c.', 
                       'pope, thomas h. iii', 'rivers, michael f., sr.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
## ** Notable Names -- W. Greg Ryberg ***
## -- Lee Bright == son of Marvin Bright Jr, so either iii or SM has it wrong
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% arrange(sponsor) %>% as.data.frame()

### Greg Gregory == CHAUNCY K Gregory, not "Gregory$"
LES[LES$sponsor %in% c('taylor, luther', 'gregory, greg'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Other Error -- JR != III ---> But they're collapsed into one...
# LES[LES$sponsor == 'mcelveen, joseph t. jr.', c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter( !(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('gregory,', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'altman, r. linwood', SM_name = 'Altman') 
name_matches <- add_row(name_matches, LES_name = 'bennett, l. edward', SM_name = 'Bennett')
name_matches <- add_row(name_matches, LES_name = 'boan, sammy', SM_name = 'Boan, O. Samuel')
name_matches <- add_row(name_matches, LES_name = 'brown, boyd', SM_name = 'Brown, Herbert')
name_matches <- add_row(name_matches, LES_name = 'burch, paul m.', SM_name = 'Burch')
name_matches <- add_row(name_matches, LES_name = 'chellis, converse', SM_name = 'Chellis III, Converse')
name_matches <- add_row(name_matches, LES_name = 'cole, derham jr.', SM_name = 'Cole Jr, J. Derham')
name_matches <- add_row(name_matches, LES_name = 'cooper, m. j.', SM_name = 'Cooper')
name_matches <- add_row(name_matches, LES_name = 'cork, bill', SM_name = 'Cork')
name_matches <- add_row(name_matches, LES_name = 'elliott, d. larry', SM_name = 'Elliott, L.')
name_matches <- add_row(name_matches, LES_name = 'goldfinch, stephen l. jr.', SM_name = 'Goldfinch, Stephen L.')
name_matches <- add_row(name_matches, LES_name = 'gregory, greg', SM_name = 'Gregory, Chauncey') # Chauncey K. "Greg" Gregory
name_matches <- add_row(name_matches, LES_name = 'gregory, jackson v.', SM_name = 'Gregory')
name_matches <- add_row(name_matches, LES_name = 'harris, anthony', SM_name = 'Harris, Charles') # C. Anthony Harris
name_matches <- add_row(name_matches, LES_name = 'harvin, c. alex iii', SM_name = 'Harvin, Charles Alexander III')
name_matches <- add_row(name_matches, LES_name = 'hayes, john c.', SM_name = 'Hayes')
name_matches <- add_row(name_matches, LES_name = 'hearn, joyce', SM_name = 'Hearn')
name_matches <- add_row(name_matches, LES_name = 'henderson, phyllis j.', SM_name = 'Henderson, Phyllis J.')
name_matches <- add_row(name_matches, LES_name = 'hinson, caldwell t.', SM_name = 'Hinson')
name_matches <- add_row(name_matches, LES_name = 'hodges, james h.', SM_name = 'Hodges')
name_matches <- add_row(name_matches, LES_name = 'hutto, anne peterson', SM_name = 'Peterson Hutto, Anne')
name_matches <- add_row(name_matches, LES_name = 'hutto, brad', SM_name = 'Hutto, Charles Bradley')
name_matches <- add_row(name_matches, LES_name = 'jefferson, joseph h. jr.', SM_name = 'Jefferson Jr, Joseph H')
name_matches <- add_row(name_matches, LES_name = 'johnson, james c. (jim)', SM_name = 'Johnson, J.C.')
name_matches <- add_row(name_matches, LES_name = 'johnson, james w. (jim) jr.', SM_name = 'Johnson, J.W.')
name_matches <- add_row(name_matches, LES_name = 'johnson, jeff', SM_name = 'Johnson, Jeffrey E.')
name_matches <- add_row(name_matches, LES_name = 'kennedy, ralph shealy', SM_name = 'Kennedy Jr, Ralph Shealy')
# name_matches <- add_row(name_matches, LES_name = 'keyserling, harriet', SM_name = 'zzzzzzz') # Both Keyserlings are Bill 
name_matches <- add_row(name_matches, LES_name = 'lee, william richard', SM_name = 'Lee')
name_matches <- add_row(name_matches, LES_name = 'limehouse, chip', SM_name = 'Limehouse III, Harry B') # Harry B. "Chip" Limehouse
name_matches <- add_row(name_matches, LES_name = 'limehouse, harry b. (chip) iii', SM_name = 'Limehouse III, Harry B')
name_matches <- add_row(name_matches, LES_name = 'long, jefferson marion jr.', SM_name = 'Long')
name_matches <- add_row(name_matches, LES_name = 'lourie, isadore', SM_name = 'Lourie')
name_matches <- add_row(name_matches, LES_name = 'mack, david iii', SM_name = 'Mack III, David J')
name_matches <- add_row(name_matches, LES_name = 'martin, john', SM_name = 'Martin') ## Must be him -- 1989-1992 and the SM record isn't in chamber 1993+
name_matches <- add_row(name_matches, LES_name = 'martin, morgan', SM_name = 'Martin, M.')
name_matches <- add_row(name_matches, LES_name = 'mcelveen, thomas', SM_name = 'McElveen, J. III')
name_matches <- add_row(name_matches, LES_name = 'mcleod, peden', SM_name = 'McLeod')
name_matches <- add_row(name_matches, LES_name = 'mcleod, walt', SM_name = 'McLeod III, Walton J')
name_matches <- add_row(name_matches, LES_name = 'meacham, becky', SM_name = 'Richardson, Rebecca Davis') ## Changes names
name_matches <- add_row(name_matches, LES_name = 'moss, steve', SM_name = 'Moss, V. Stephen')
name_matches <- add_row(name_matches, LES_name = 'moss, donna', SM_name = 'Moss') # Must be her -- 1989-1990 and the SM record isn't in chamber 1993+
name_matches <- add_row(name_matches, LES_name = 'neal, james m. (jimmy)', SM_name = 'Neal, James')
name_matches <- add_row(name_matches, LES_name = 'pope, thomas h. iii', SM_name = 'Pope')
name_matches <- add_row(name_matches, LES_name = 'quinn, richard m. jr.', SM_name = 'Quinn, Richard Jr.')
name_matches <- add_row(name_matches, LES_name = 'ridgeway, robert l. iii', SM_name = 'Ridgeway III, Robert L')
name_matches <- add_row(name_matches, LES_name = 'scott, tim', SM_name = 'Scott')
name_matches <- add_row(name_matches, LES_name = 'shealy, ryan', SM_name = 'Shealy')
name_matches <- add_row(name_matches, LES_name = 'short, paul e. jr.', SM_name = 'Short')
name_matches <- add_row(name_matches, LES_name = 'smith, greg', SM_name = 'Smith, G.')
name_matches <- add_row(name_matches, LES_name = 'smith, james emerson jr.', SM_name = 'Smith Jr, James E')
name_matches <- add_row(name_matches, LES_name = 'tallon, eddie', SM_name = 'Tallon Sr, Edward R')
name_matches <- add_row(name_matches, LES_name = 'taylor, j. adam', SM_name = 'Taylor, Adam Sr.')
name_matches <- add_row(name_matches, LES_name = 'taylor, luther', SM_name = 'Taylor')
name_matches <- add_row(name_matches, LES_name = 'thomas, paula h.', SM_name = 'Thomas')
name_matches <- add_row(name_matches, LES_name = 'wells, carole c.', SM_name = 'Wells')
name_matches <- add_row(name_matches, LES_name = 'white, brian', SM_name = 'White, W. Brian')
name_matches <- add_row(name_matches, LES_name = 'white, juanita', SM_name = 'White')
name_matches <- add_row(name_matches, LES_name = 'williams, marshall', SM_name = 'Williams')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

###########
### Party Switches and Manual Edits
###########
### Check Party Mismatches
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% arrange(sponsor, term) %>% as.data.frame()
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% arrange(sponsor, term) %>% as.data.frame()

LES[LES$sponsor == 'alexander, thomas c.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Alexander' & ideo$party == 'D',]$name
LES[LES$sponsor == 'alexander, thomas c.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Alexander' & ideo$party == 'D',]$party
LES[LES$sponsor == 'alexander, thomas c.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Alexander' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'alexander, thomas c.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Alexander, Thomas' & ideo$party == 'R',]$name
LES[LES$sponsor == 'alexander, thomas c.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Alexander, Thomas' & ideo$party == 'R',]$party
LES[LES$sponsor == 'alexander, thomas c.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Alexander, Thomas' & ideo$party == 'R',]$np_score

## Switched D to R March 1990, lost election, won a few years later as Rep.
LES[LES$sponsor == 'barfield, liston d.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Barfield' & ideo$party == 'D',]$name
LES[LES$sponsor == 'barfield, liston d.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Barfield' & ideo$party == 'D',]$party
LES[LES$sponsor == 'barfield, liston d.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Barfield' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'barfield, liston d.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Barfield, Liston D.' & ideo$party == 'R',]$name
LES[LES$sponsor == 'barfield, liston d.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Barfield, Liston D.' & ideo$party == 'R',]$party
LES[LES$sponsor == 'barfield, liston d.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Barfield, Liston D.' & ideo$party == 'R',]$np_score

#### William Boan, Changed D to R on Aug. 19, 1995 -- https://www.scstatehouse.gov/sess111_1995-1996/sj95/etcndx.htm
LES[LES$sponsor == 'boan, william d.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Boan' & ideo$party == 'D',]$name
LES[LES$sponsor == 'boan, william d.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Boan' & ideo$party == 'D',]$party
LES[LES$sponsor == 'boan, william d.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Boan' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'boan, william d.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Boan, William Daniel' & ideo$party == 'R',]$name
LES[LES$sponsor == 'boan, william d.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Boan, William Daniel' & ideo$party == 'R',]$party
LES[LES$sponsor == 'boan, william d.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Boan, William Daniel' & ideo$party == 'R',]$np_score

#### Cebron Daniel Chamblee -- Switched D to R on Nov 15, 1994: https://www.scstatehouse.gov/sess111_1995-1996/sj95/etcndx.htm
LES[LES$sponsor == 'chamblee, c. d.' & LES$term == "1995_1996",]$party <- 'r'
LES[LES$sponsor == 'chamblee, c. d.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Chamblee' & ideo$party == 'D',]$name
LES[LES$sponsor == 'chamblee, c. d.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Chamblee' & ideo$party == 'D',]$party
LES[LES$sponsor == 'chamblee, c. d.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Chamblee' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'chamblee, c. d.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Chamblee' & ideo$party == 'R',]$name
LES[LES$sponsor == 'chamblee, c. d.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Chamblee' & ideo$party == 'R',]$party
LES[LES$sponsor == 'chamblee, c. d.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Chamblee' & ideo$party == 'R',]$np_score

#### Charles Tyrone Courtney: Klarner has him as D in 1992, but official record says he's R at sitting in 1993 (ctrl-F: "Courtney (R)"): https://www.scstatehouse.gov/sess110_1993-1994/sj93/19930112.htm
# --- Must have won special to replace Horace Smith (D), who resigned in May 1991: https://www.goupstate.com/article/NC/19920331/News/605191978/SJ
# --> Won that special as a Republican: https://www.goupstate.com/news/19910821/judge-refuses-to-rule-on-issues-in-lanford-divorce/1
LES[LES$sponsor == 'courtney, c. tyrone' & LES$term %in% c("1991_1992", "1993_1994", "1995_1996"),]$party <- 'r'

LES[LES$sponsor == 'delleney, f. gregory greg jr.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Delleney, Francis Jr.' & ideo$party == 'D',]$name
LES[LES$sponsor == 'delleney, f. gregory greg jr.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Delleney, Francis Jr.' & ideo$party == 'D',]$party
LES[LES$sponsor == 'delleney, f. gregory greg jr.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Delleney, Francis Jr.' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'delleney, f. gregory greg jr.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Delleney Jr, F. Gregory' & ideo$party == 'R',]$name
LES[LES$sponsor == 'delleney, f. gregory greg jr.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Delleney Jr, F. Gregory' & ideo$party == 'R',]$party
LES[LES$sponsor == 'delleney, f. gregory greg jr.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Delleney Jr, F. Gregory' & ideo$party == 'R',]$np_score

### Felder: Changed D to R on Oct. 10, 1995: https://www.scstatehouse.gov/sess111_1995-1996/sj95/etcndx.htm
LES[LES$sponsor == 'felder, john' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'Felder' & ideo$party == 'D',]$name
LES[LES$sponsor == 'felder, john' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Felder' & ideo$party == 'D',]$party
LES[LES$sponsor == 'felder, john' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Felder' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'felder, john' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Felder' & ideo$party == 'R',]$name
LES[LES$sponsor == 'felder, john' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Felder' & ideo$party == 'R',]$party
LES[LES$sponsor == 'felder, john' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Felder' & ideo$party == 'R',]$np_score

LES[LES$sponsor == 'hayes, robert wes jr.' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'Hayes, R.W.' & ideo$party == 'D',]$name
LES[LES$sponsor == 'hayes, robert wes jr.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Hayes, R.W.' & ideo$party == 'D',]$party
LES[LES$sponsor == 'hayes, robert wes jr.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Hayes, R.W.' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'hayes, robert wes jr.' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Hayes, Robert Wesley' & ideo$party == 'R',]$name
LES[LES$sponsor == 'hayes, robert wes jr.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Hayes, Robert Wesley' & ideo$party == 'R',]$party
LES[LES$sponsor == 'hayes, robert wes jr.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Hayes, Robert Wesley' & ideo$party == 'R',]$np_score

### B.L. Hendricks: Start 1989 terma as D, started 1991 as R, exact switch date unclear
LES[LES$sponsor == 'hendricks, b. l. jr.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Hendricks, B' & ideo$party == 'D',]$name
LES[LES$sponsor == 'hendricks, b. l. jr.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Hendricks, B' & ideo$party == 'D',]$party
LES[LES$sponsor == 'hendricks, b. l. jr.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Hendricks, B' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'hendricks, b. l. jr.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Hendricks' & ideo$party == 'R',]$name
LES[LES$sponsor == 'hendricks, b. l. jr.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Hendricks' & ideo$party == 'R',]$party
LES[LES$sponsor == 'hendricks, b. l. jr.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Hendricks' & ideo$party == 'R',]$np_score

LES[LES$sponsor == 'huff, thomas e. (tom)' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'Huff' & ideo$party == 'D',]$name
LES[LES$sponsor == 'huff, thomas e. (tom)' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Huff' & ideo$party == 'D',]$party
LES[LES$sponsor == 'huff, thomas e. (tom)' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Huff' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'huff, thomas e. (tom)' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Huff' & ideo$party == 'R',]$name
LES[LES$sponsor == 'huff, thomas e. (tom)' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Huff' & ideo$party == 'R',]$party
LES[LES$sponsor == 'huff, thomas e. (tom)' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Huff' & ideo$party == 'R',]$np_score

LES[LES$sponsor == 'law, james' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Law' & ideo$party == 'D',]$name
LES[LES$sponsor == 'law, james' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Law' & ideo$party == 'D',]$party
LES[LES$sponsor == 'law, james' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Law' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'law, james' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Law, James Norris' & ideo$party == 'R',]$name
LES[LES$sponsor == 'law, james' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Law, James Norris' & ideo$party == 'R',]$party
LES[LES$sponsor == 'law, james' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Law, James Norris' & ideo$party == 'R',]$np_score

# ** Note: Name corrected below
LES[LES$sponsor == 'leatherman, huge k. sr.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Leatherman' & ideo$party == 'D',]$name
LES[LES$sponsor == 'leatherman, huge k. sr.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Leatherman' & ideo$party == 'D',]$party
LES[LES$sponsor == 'leatherman, huge k. sr.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Leatherman' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'leatherman, huge k. sr.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Leatherman, Hugh K.' & ideo$party == 'R',]$name
LES[LES$sponsor == 'leatherman, huge k. sr.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Leatherman, Hugh K.' & ideo$party == 'R',]$party
LES[LES$sponsor == 'leatherman, huge k. sr.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Leatherman, Hugh K.' & ideo$party == 'R',]$np_score

# ** Martin, L must be him as D in 1989_1990... It's from the SM data prior to 1993 and "^Martin$" doesn't match
LES[LES$sponsor == 'martin, larry' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Martin, L.' & ideo$party == 'D',]$name
LES[LES$sponsor == 'martin, larry' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Martin, L.' & ideo$party == 'D',]$party
LES[LES$sponsor == 'martin, larry' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Martin, L.' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'martin, larry' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Martin, Larry' & ideo$party == 'R',]$name
LES[LES$sponsor == 'martin, larry' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Martin, Larry' & ideo$party == 'R',]$party
LES[LES$sponsor == 'martin, larry' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Martin, Larry' & ideo$party == 'R',]$np_score

LES[LES$sponsor == 'mcabee, jennings' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'McAbee' & ideo$party == 'D',]$name
LES[LES$sponsor == 'mcabee, jennings' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'McAbee' & ideo$party == 'D',]$party
LES[LES$sponsor == 'mcabee, jennings' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'McAbee' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'mcabee, jennings' & LES$party == 'nonmaj',]$SM_name <- ideo[ideo$name == 'McAbee' & ideo$party == 'R',]$name
LES[LES$sponsor == 'mcabee, jennings' & LES$party == 'nonmaj',]$SM_party <- ideo[ideo$name == 'McAbee' & ideo$party == 'R',]$party
LES[LES$sponsor == 'mcabee, jennings' & LES$party == 'nonmaj',]$np_score <- ideo[ideo$name == 'McAbee' & ideo$party == 'R',]$np_score

LES[LES$sponsor == 'mcginnis, alfred c. sr.' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'McGinnis' & ideo$party == 'D',]$name
LES[LES$sponsor == 'mcginnis, alfred c. sr.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'McGinnis' & ideo$party == 'D',]$party
LES[LES$sponsor == 'mcginnis, alfred c. sr.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'McGinnis' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'mcginnis, alfred c. sr.' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'McGinnis' & ideo$party == 'R',]$name
LES[LES$sponsor == 'mcginnis, alfred c. sr.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'McGinnis' & ideo$party == 'R',]$party
LES[LES$sponsor == 'mcginnis, alfred c. sr.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'McGinnis' & ideo$party == 'R',]$np_score

### McKay: Changed D to I on Nov. 15, 1994 and ran as republican thereafter (per Klarner)
LES[LES$sponsor == 'mckay, woodrow m.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'McKay' & ideo$party == 'D',]$name
LES[LES$sponsor == 'mckay, woodrow m.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'McKay' & ideo$party == 'D',]$party
LES[LES$sponsor == 'mckay, woodrow m.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'McKay' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'mckay, woodrow m.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == "McKay, Woodrow Maxie 'Woody" & ideo$party == 'R',]$name
LES[LES$sponsor == 'mckay, woodrow m.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == "McKay, Woodrow Maxie 'Woody" & ideo$party == 'R',]$party
LES[LES$sponsor == 'mckay, woodrow m.' & LES$party == 'r',]$np_score <- ideo[ideo$name == "McKay, Woodrow Maxie 'Woody" & ideo$party == 'R',]$np_score

LES[LES$sponsor == 'moss, dennis carroll' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Moss, Dennis' & ideo$party == 'D',]$name
LES[LES$sponsor == 'moss, dennis carroll' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Moss, Dennis' & ideo$party == 'D',]$party
LES[LES$sponsor == 'moss, dennis carroll' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Moss, Dennis' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'moss, dennis carroll' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Moss, Dennis Carroll' & ideo$party == 'R',]$name
LES[LES$sponsor == 'moss, dennis carroll' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Moss, Dennis Carroll' & ideo$party == 'R',]$party
LES[LES$sponsor == 'moss, dennis carroll' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Moss, Dennis Carroll' & ideo$party == 'R',]$np_score

LES[LES$sponsor == 'odell, billy' & LES$party == 'd',]$SM_name <- ideo[ideo$name == "O'Dell, William" & ideo$party == 'D',]$name
LES[LES$sponsor == 'odell, billy' & LES$party == 'd',]$SM_party <- ideo[ideo$name == "O'Dell, William" & ideo$party == 'D',]$party
LES[LES$sponsor == 'odell, billy' & LES$party == 'd',]$np_score <- ideo[ideo$name == "O'Dell, William" & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'odell, billy' & LES$party == 'r',]$SM_name <- ideo[ideo$name == "O'Dell, William" & ideo$party == 'R',]$name
LES[LES$sponsor == 'odell, billy' & LES$party == 'r',]$SM_party <- ideo[ideo$name == "O'Dell, William" & ideo$party == 'R',]$party
LES[LES$sponsor == 'odell, billy' & LES$party == 'r',]$np_score <- ideo[ideo$name == "O'Dell, William" & ideo$party == 'R',]$np_score

### Harvey Peeler Jr., Dem from 1981 to Oct. 1989; Rep. from thereafter
## ---> No Dem observation so recoding 1989+ as r (even though 1989 is partial term)
LES[LES$sponsor == 'peeler, harvey' & LES$term %in% c("1989_1990", "1991_1992"),]$party <- 'r'
# LES[LES$sponsor == 'peeler, harvey' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Peeler, Harvey Jr.' & ideo$party == 'D',]$name
# LES[LES$sponsor == 'peeler, harvey' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Peeler, Harvey Jr.' & ideo$party == 'D',]$party
# LES[LES$sponsor == 'peeler, harvey' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Peeler, Harvey Jr.' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'peeler, harvey' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Peeler, Harvey Jr.' & ideo$party == 'R',]$name
LES[LES$sponsor == 'peeler, harvey' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Peeler, Harvey Jr.' & ideo$party == 'R',]$party
LES[LES$sponsor == 'peeler, harvey' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Peeler, Harvey Jr.' & ideo$party == 'R',]$np_score

LES[LES$sponsor == 'smith, j. verne' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'Smith, Jefferson Verne' & ideo$party == 'D',]$name
LES[LES$sponsor == 'smith, j. verne' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Smith, Jefferson Verne' & ideo$party == 'D',]$party
LES[LES$sponsor == 'smith, j. verne' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Smith, Jefferson Verne' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'smith, j. verne' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Smith, Jefferson Verne' & ideo$party == 'R',]$name
LES[LES$sponsor == 'smith, j. verne' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Smith, Jefferson Verne' & ideo$party == 'R',]$party
LES[LES$sponsor == 'smith, j. verne' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Smith, Jefferson Verne' & ideo$party == 'R',]$np_score

### Spearman: CHanged D to R on Aug. 28, 1995: https://www.scstatehouse.gov/sess111_1995-1996/sj95/etcndx.htm
LES[LES$sponsor == 'spearman, molly' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Spearman' & ideo$party == 'D',]$name
LES[LES$sponsor == 'spearman, molly' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Spearman' & ideo$party == 'D',]$party
LES[LES$sponsor == 'spearman, molly' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Spearman' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'spearman, molly' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Spearman, Molly Mitchell' & ideo$party == 'R',]$name
LES[LES$sponsor == 'spearman, molly' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Spearman, Molly Mitchell' & ideo$party == 'R',]$party
LES[LES$sponsor == 'spearman, molly' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Spearman, Molly Mitchell' & ideo$party == 'R',]$np_score

LES[LES$sponsor == 'townsend, ronald p.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Townsend' & ideo$party == 'D',]$name
LES[LES$sponsor == 'townsend, ronald p.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Townsend' & ideo$party == 'D',]$party
LES[LES$sponsor == 'townsend, ronald p.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Townsend' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'townsend, ronald p.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Townsend, Ronald Parker' & ideo$party == 'R',]$name
LES[LES$sponsor == 'townsend, ronald p.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Townsend, Ronald Parker' & ideo$party == 'R',]$party
LES[LES$sponsor == 'townsend, ronald p.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Townsend, Ronald Parker' & ideo$party == 'R',]$np_score

LES[LES$sponsor == 'waldrep, robert l.' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'Waldrep' & ideo$party == 'D',]$name
LES[LES$sponsor == 'waldrep, robert l.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Waldrep' & ideo$party == 'D',]$party
LES[LES$sponsor == 'waldrep, robert l.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Waldrep' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'waldrep, robert l.' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Waldrep, Bob Jr.' & ideo$party == 'R',]$name
LES[LES$sponsor == 'waldrep, robert l.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Waldrep, Bob Jr.' & ideo$party == 'R',]$party
LES[LES$sponsor == 'waldrep, robert l.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Waldrep, Bob Jr.' & ideo$party == 'R',]$np_score

### Wadrop -- Changed D to R, Jan 3. 1995: https://www.scstatehouse.gov/sess111_1995-1996/sj95/etcndx.htm
LES[LES$sponsor == 'waldrop, dave c. jr.' & LES$term == "1995_1996",]$party <- 'r'
LES[LES$sponsor == 'waldrop, dave c. jr.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Waldrop' & ideo$party == 'D',]$name
LES[LES$sponsor == 'waldrop, dave c. jr.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Waldrop' & ideo$party == 'D',]$party
LES[LES$sponsor == 'waldrop, dave c. jr.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Waldrop' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'waldrop, dave c. jr.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Waldrop' & ideo$party == 'R',]$name
LES[LES$sponsor == 'waldrop, dave c. jr.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Waldrop' & ideo$party == 'R',]$party
LES[LES$sponsor == 'waldrop, dave c. jr.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Waldrop' & ideo$party == 'R',]$np_score

LES[LES$sponsor == 'whatley, mickey' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Whatley, Michael Stewart' & ideo$party == 'D',]$name
LES[LES$sponsor == 'whatley, mickey' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Whatley, Michael Stewart' & ideo$party == 'D',]$party
LES[LES$sponsor == 'whatley, mickey' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Whatley, Michael Stewart' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'whatley, mickey' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Whatley, Michael Stewart' & ideo$party == 'R',]$name
LES[LES$sponsor == 'whatley, mickey' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Whatley, Michael Stewart' & ideo$party == 'R',]$party
LES[LES$sponsor == 'whatley, mickey' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Whatley, Michael Stewart' & ideo$party == 'R',]$np_score

### Worley: Changed D to R on Nov. 14, 1994
LES[LES$sponsor == 'worley, harold' & LES$term == "1995_1996",]$party <- 'r'
LES[LES$sponsor == 'worley, harold' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Worley' & ideo$party == 'D',]$name
LES[LES$sponsor == 'worley, harold' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Worley' & ideo$party == 'D',]$party
LES[LES$sponsor == 'worley, harold' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Worley' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'worley, harold' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Worley' & ideo$party == 'R',]$name
LES[LES$sponsor == 'worley, harold' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Worley' & ideo$party == 'R',]$party
LES[LES$sponsor == 'worley, harold' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Worley' & ideo$party == 'R',]$np_score

# He was a Rep in 1989-1990
LES[LES$sponsor == 'limehouse, thomas' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Limehouse' & ideo$party == 'R',]$name
LES[LES$sponsor == 'limehouse, thomas' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Limehouse' & ideo$party == 'R',]$party
LES[LES$sponsor == 'limehouse, thomas' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Limehouse' & ideo$party == 'R',]$np_score

### Two Rows
LES[LES$sponsor == 'gambrell, michael w.',]$SM_name  <- ideo[ideo$name == 'Gambrell, Michael W.' & ideo$house2008 %in% 1,]$name
LES[LES$sponsor == 'gambrell, michael w.',]$SM_party <- ideo[ideo$name == 'Gambrell, Michael W.' & ideo$house2008 %in% 1,]$party
LES[LES$sponsor == 'gambrell, michael w.',]$np_score <- ideo[ideo$name == 'Gambrell, Michael W.' & ideo$house2008 %in% 1,]$np_score

LES[LES$sponsor == 'mclellan, robert n.',]$SM_name  <- ideo[ideo$name == 'McLellan' & ideo$np_score == -0.017,]$name
LES[LES$sponsor == 'mclellan, robert n.',]$SM_party <- ideo[ideo$name == 'McLellan' & ideo$np_score == -0.017,]$party
LES[LES$sponsor == 'mclellan, robert n.',]$np_score <- ideo[ideo$name == 'McLellan' & ideo$np_score == -0.017,]$np_score

LES[LES$sponsor == 'mitchell, theo walker',]$SM_name  <- ideo[ideo$name == 'Mitchell' & ideo$senate1993 %in% 1,]$name
LES[LES$sponsor == 'mitchell, theo walker',]$SM_party <- ideo[ideo$name == 'Mitchell' & ideo$senate1993 %in% 1,]$party
LES[LES$sponsor == 'mitchell, theo walker',]$np_score <- ideo[ideo$name == 'Mitchell' & ideo$senate1993 %in% 1,]$np_score

### Technically, he switches away from dems? -- is Nonmaj in Klarner in latter two years
LES[LES$sponsor == 'keyserling, billy',]$SM_name <- ideo[ideo$name == 'Keyserling' & ideo$house1995 %in% 1,]$name
LES[LES$sponsor == 'keyserling, billy',]$SM_party <- ideo[ideo$name == 'Keyserling' & ideo$house1995 %in% 1,]$party
LES[LES$sponsor == 'keyserling, billy',]$np_score <- ideo[ideo$name == 'Keyserling' & ideo$house1995 %in% 1,]$np_score

#### James L. Mann Cromer Jr (Bubba) --- Fixing 'writein' as party:
LES[LES$sponsor == 'cromer, james l. mann (bubba) jr.' & LES$party == 'writein',]$party <- 'nonmaj'

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

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
LES[LES$sponsor %in% c("butler, mia", 'garrick, mia butler', 'mcleod, mia'),]$sponsor <- 'mcleod, mia s. butler'
LES[LES$sponsor == 'brightmatthews, margie',]$sponsor <- 'matthews, margie bright'
LES[LES$sponsor == 'wood, donna',]$sponsor <- 'hicks, donna wood'
LES[LES$sponsor == 'cole, j. derham',]$sponsor <- 'cole, j. derham sr.'
LES[LES$sponsor == 'cole, derham jr.',]$sponsor <- 'cole, j. derham jr.'
LES[LES$sponsor == 'pope, tommy',]$sponsor <- 'pope, thomas e.' # Does not appear to be related to thomas h pope iii (from 20 years earlier)
LES[LES$sponsor == 'elliott, dick',]$sponsor <- 'elliott, dick f.'
LES[LES$sponsor == 'elliott, d. larry',]$sponsor <- 'elliott, larry l.' # Klarner seems to be wrong here... L.L. Elliott -- https://www.scstatehouse.gov/member.php?code=531818118&chamber=H
LES[LES$sponsor == 'davenport, g. ralph jr.',]$sponsor <- 'davenport, guy ralph jr.'
LES[LES$sponsor == 'baxley, j. michael',]$sponsor <- 'baxley, john michael'
LES[LES$sponsor == 'leatherman, huge k. sr.',]$sponsor <- 'leatherman, hugh k. sr.'
LES[LES$sponsor == 'courtney, c. tyrone',]$sponsor <- 'courtney, charles tyrone'
LES[LES$sponsor == 'delleney, f. gregory greg jr.',]$sponsor <- 'delleney, francis gregory jr.'
LES[LES$sponsor == 'byrd, dr. alma w.',]$sponsor <- 'byrd, alma w.'
LES[LES$sponsor == 'cotty, w. johnny b.',]$sponsor <- 'cotty, william frank' #  Not clear where johnny comes from -- william frank cotty
LES[LES$sponsor == 'young, w. jeffrey',]$sponsor <- 'young, william jeffrey'
LES[LES$sponsor == 'mason, ruby',]$sponsor <- 'mason, rudolph marion'
LES[LES$sponsor == 'rodgers, edie',]$sponsor <- 'rodgers, edith'
LES[LES$sponsor == 'maddox, jesse c.',]$sponsor <- 'maddox, jesse cordell jr.'
LES[LES$sponsor == 'pinson, gene',]$sponsor <- 'pinson, lewis eugene'
LES[LES$sponsor == 'brannon, doug',]$sponsor <- 'brannon, norman doug'
LES[LES$sponsor == 'chamblee, c. d.',]$sponsor <- 'chamblee, cebron daniel'
LES[LES$sponsor == 'gregory, greg',]$sponsor <- 'gregory, chauncey k.'
LES[LES$sponsor == 'bennett, lin',]$sponsor <- 'bennett, linda'
LES[LES$sponsor == 'barrett, gresham',]$sponsor <- 'barrett, james gresham'
#LES[LES$sponsor == 'zzzzz',]$sponsor <- 'zzzzzz'

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1989 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1989:1994) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1989 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1989:2000) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2001:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


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
  scale_color_manual(values=c("dodgerblue2",  'gray50', "red2", 'gray50'))

##### CHECK OUTLIERS
# filter(LES, party == 'd' & np_score > .5) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & np_score < -.25) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

