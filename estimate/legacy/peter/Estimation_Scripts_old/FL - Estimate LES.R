
################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** FLORIDA *** BY SESSION
##############################################################

###########
#### QUESTIONS
# (1) What to do about (sub)committees? Hard to code given quality of data.. Need database of these...
# ----> Can get H (sub)committees 1998+ by going to house website -- https://www.myfloridahouse.gov/Sections/Committees/committees.aspx?LegislativeTermId=80
# ----> S (sub)comms 2008+ http://flsenate.gov/Committees
# (2) Unfavorable reports --> ABC?

###################################
## SPECIAL SESSIONS:
## ---- Folded into main file, typically have special session letter (A, B, C, etc) appended on to bill id
## ---- Legislators take office IMMEDIATELY after election, so ORG and Special A Sessions sometimes occur IN election year
## MEMBER LISTS:
## ---- http://archive.flsenate.gov/cgi-bin/View_Page.pl?File=index.html&Directory=Publications/Archive/Photos_on_display/&Tab=Welcome&Submenu=2
## ---- https://www.myfloridahouse.gov/FileStores/Web/HouseContent/Approved/ClerksOffice//house_counties_final.pdf
## PROCESS/RULES:
## ---- 
## Sponsorship/Authorship
## ---- Committee Sponsorship permitted -- HOWEVER, often includes name of the primary sponsor from committee??
## -----> Take SB 2910 in 2004: http://archive.flsenate.gov/Session/index.cfm?Mode=Bills&SubMenu=1&Tab=session&BI_Mode=ViewBillInfo&BillNum=2910&Chamber=Senate&Year=2004
## -----> Multiple committees + Subcommittee sponsor + PEADEN -- WHo just so happens to be the Appropriations subcommittee chair: http://archive.flsenate.gov/cgi-bin/view_page.pl?Tab=session&Submenu=1&FT=D&File=session/2004/Senate/bills/votes_com/html/SSB2910.AHS.html
## -----> ACTUALLY, CLICK ON THE BILL TEXT ---> SPONSOR NAME PEADEN: http://archive.flsenate.gov/cgi-bin/view_page.pl?Tab=session&Submenu=1&FT=D&File=sb2910.html&Directory=session/2004/Senate/bills/billtext/html/
## -----> BUT THIS CHANGES IN VERSION 4: http://archive.flsenate.gov/cgi-bin/view_page.pl?Tab=session&Submenu=1&FT=D&File=sb2910c3.html&Directory=session/2004/Senate/bills/billtext/html/
###########################
## NOTES:
## ** Lot of handcoding required for Sponsors with Same Last name -- Sometimes only 1 sponsor provided with an initial
# ----> House page often shows both but scraped using senate because data goes further back... But can use it to cross-check
## ** With solution to above, NUMBER OF COSPONSORED BILLS for these member will still be off.
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

this_state <- 'FL'
min_year <- 2001 #1999-2000 possible but need to check data quality
max_year <- 2018
keep_types <- c("HB", "SB")
spec_elec_codes <- c('s', 'gs')
house_term_length <- 2
sen_term_length <- 4 # STAGGERED? YES; 2 YEAR TERMS in REDISTRICTING YEARS

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
commem_bills <- commem_bills %>%
  mutate(pad_num = ifelse(grepl("SS", session), 5, 4),
         bill_id = paste0(gsub(' .+', '', bill_id), str_pad(gsub('^[A-Z]+ ', '', bill_id), pad_num, pad = 0))) %>%
  select(-pad_num)

#### SUBSTANTIVE AND SIGNIFICANT BILLS
# **** Senate Bill Numbers all EVEN; House bills ODD
SS_bills <- read.csv("~/Dropbox/Data/State Legislative Data/Significant Bills/SS_Data/SS_Bills.csv")
SS_bills <- SS_bills %>% 
  filter(state == this_state) %>%
  rename(bill_id = bill_num) %>%
  mutate(term = ifelse(year %% 2 == 1, paste0(year, "_", year + 1), paste0(year - 1, "_", year)),
         bill_id = toupper(bill_id),
         # ************* NEED TO FIX THE SS BILL EXTRACTION FOR THE SPECIAL BILLS -- RIGHT NOW, ALL RS ***************
         session = ifelse(grepl("[A-Z]$", bill_id), paste0(year, '-SS-', str_extract(bill_id, "[A-Z]$")), paste0(year, "-RS")),
         SS = 1) %>%
  select(state, term, year, bill_id, everything()) %>%
  filter((bill_type %in% c("HB", "H") & num_only %% 2 == 1) | (bill_type %in% c("SB", "S") & num_only %% 2 == 0))

#### Check SS BIll Types ---> Correct to ensure standardized with Data Format
# table(SS_bills$bill_type)

#### Data to Fill in Missing First Initials
# ** Compiled from the "FL - Get Name Duplicate Bills.R" Script in Legislator Info Folder, Loaded Above
name_dups <- read.csv("~/Dropbox/Data/State Legislative Data/Legislator_Info/FL_Duplicate_Name_Bills.csv")

### Load Klarner Data 
load("~/Dropbox/Data/US Election Data/state_leg/klarner_data/196slers1967to2016_20180908.RData")
klarner <- table; rm(table)
klarner$sab <- toupper(klarner$sab)
klarner <- filter(klarner, year > min_year - 5 & sab == this_state)

#### FIX/DROP PROBLEMATIC KLARNER OBSERVATIONS (Doubles, Multi-race candidates, etc.)
# klarner[klarner$cand == 'littel, robert e.',]$cand <- "littell, robert e."
# ----> IDs will still be off, but need to keep them to match to external data...

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[4]

for(t in terms){
  
  ### Formulate 2-year terms -- Cover both regular and special sessions
  t_yrs <- as.character(glue('{t}_{t+1}'))
  
  ### Sessions -- Need to Adjust For Organizational Sessions and Pre-Sessions
  # ** Orgs happen in NOV T-1, if they occur/are recorded
  # ** Occassional A Sessions at T-1: 2010A == 2011_2012; 2004A = 2005_2006; 2000A = 2001_2002
  t_sessions <- sessions[grepl(glue('{t-1}O|{t}|{t+1}'), sessions)]
  t_sessions <- t_sessions[!grepl(glue('{t+1}O|2000A|2004A|2010A'), t_sessions)]
  if(t_yrs %in% c('2005_2006', '2011_2012')){
    t_sessions <- c(t_sessions, as.character(glue("{t-1}A")))
  }
    
  ### Skip Previously Estimated
  if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
    print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
    next
  }
  
  ###### SESSION IN PROGRESS
  cat('\n'); print(glue('~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~'))
  
  ############### Read in data
  bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_sessions[1]}.csv")
  bills <- read.csv(bill_path)
  bills$session <- as.character(bills$session)
  
  ### If multiple sessions in different files, read in those as well
  if(length(t_sessions) > 1){
    for(s in t_sessions[2:length(t_sessions)]){
      bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{s}.csv")
      s_bills <- readr::read_csv(bill_path, col_types = readr::cols())
      s_bills$session <- as.character(s_bills$session)
      bills <- bind_rows(bills, s_bills)
    }
    rm(s, s_bills)
  }

  ### Clean Term/Session Variables
  bills$term <- t_yrs
  bills$session_year <- gsub('[A-Z]+', '', bills$session)
  bills$session_type <- recode(gsub('^[0-9]+', '', bills$session), 'A' = 'SS-A', 'B' = 'SS-B', 'C' = 'SS-C', 'D' = 'SS-D', 'E' = 'SS-E', 'F' = 'SS-F')
  bills$session_type <- ifelse(bills$session_type == '', 'RS', bills$session_type)
  bills$session <- paste(bills$session_year, bills$session_type, sep = '-')
  bills <- select(bills, -c(session_year, session_type))
  
  ### Check for duplicates
  if(nrow(bills) != nrow(distinct(bills))){
    cat(" \n ~~~~~~~> DUPLICATE BILLS \n ")
    break
  }
  
  ######## Standardize the Bill IDs
  bills <- rename(bills, bill_id = bill_num) %>%
    mutate(pad_num = ifelse(grepl("SS", session), 5, 4),
           bill_id = paste0(gsub(' .+', '', bill_id), str_pad(gsub('^[A-Z]+ ', '', bill_id), pad_num, pad = 0))) %>%
    select(-pad_num)
  
  ##########################
  ####### Standardize Sponsors
  
  ### Removing Non-Sponsor Info in Parentheses (Usually notes about similar bills) + Will Correctly Clean strings with mutliple () -- Need to use both terms or else will miss close parenthese on things like: "(CS by Criminal Justice (JC))"
  # ----> DON'T NEED AFTER SCRAPING NEW SITE
  # bills$sponsors <- str_trim(gsub("\\([^\\)]*\\)\\)|\\([^\\)]*\\)", "", bills$sponsors, perl=TRUE))
  # bills$cosponsors <- str_trim(gsub("\\([^\\)]*\\)\\)|\\([^\\)]*\\)", "", bills$cosponsors, perl=TRUE))
  # ### Excess Space
  # bills$sponsors <- gsub('  +', ' ', bills$sponsors)
  # bills$cosponsors <- gsub('  +', ' ', bills$cosponsors)

  ### Clean Accents
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
  
  bills$cosponsors <- tolower(bills$cosponsors)
  bills$cosponsors <- gsub('á', 'a', bills$cosponsors)
  bills$cosponsors <- gsub('é', 'e', bills$cosponsors)
  bills$cosponsors <- gsub('ó', 'o', bills$cosponsors)
  bills$cosponsors <- gsub('í', 'i', bills$cosponsors)
  bills$cosponsors <- gsub('ñ', 'n', bills$cosponsors)

  ### LES Sponsor Var
  bills$LES_sponsor <- ifelse(grepl('^sen\\.|^rep\\.', bills$primary_sponsor), gsub(',.+', '', bills$primary_sponsor), bills$primary_sponsor)
  bills$LES_sponsor <- str_trim(gsub('^sen\\. |^rep\\. ', '', bills$LES_sponsor))
  # table(bills$LES_sponsor)
  
  ### Add in First Initials if Present
  bills$alt_name <- gsub(';.+', '', bills$sponsors)
  bills$LES_sponsor <- ifelse(grepl(', [a-z]\\.', bills$alt_name), bills$alt_name, bills$LES_sponsor)
  bills$LES_sponsor <- gsub('\\.$', '', bills$LES_sponsor)

  ###############
  ### Automated Name Fixes --- Adding Missing First Initials
  ###############
  # *** CAN'T FIX COSPOSNORS THIS WAY --- COULD Get the counts for those via scraper and then fill in later... 
  
  name_sub <- filter(name_dups, term == t_yrs) %>% 
    mutate(bill_num = gsub('CS/', '', bill_num),
           pad_num = ifelse(grepl("[A-Z]$", bill_num), 5, 4),
           bill_num = paste0(gsub(' .+', '', bill_num), str_pad(gsub('^[A-Z]+ ', '', bill_num), pad_num, pad = 0)))
  if(nrow(name_sub) > 0){
    for(new_n in unique(name_sub$new_name)){
      ### Need to do by session -- Special bills should be OK though because they have A, B, C tacked on
      for(sy in unique(name_sub$session)){
        sy_sub <- filter(name_sub, substring(session, 1, 4) == sy & new_name == new_n)
        if(nrow(sy_sub) == 0) next
        bills[substring(bills$session, 1, 4) == sy & bills$bill_id %in% sy_sub$bill_num,]$LES_sponsor <- new_n
      }
    }
    rm(new_n, sy_sub, sy)
  }
  
  ##################
  #### Fix Remaining errors By Term 
  ###################
  # ** Seems to stem from Committee Bills that are FILED BY a specific Individual (which is coded here as primary_sponsor)
  # -----> and then substituted in committee stage
  # ** Some of these are unfixed from above process, some are unrelated
  # filter(bills, LES_sponsor == 'gibson' & substring(bill_id, 1,1) == "H") %>% select(session, bill_id, primary_sponsor, term, LES_sponsor)
  
  if(t_yrs == '2005_2006'){
    bills[bills$session == "2006-RS" & bills$bill_id == 'HB7213',]$LES_sponsor <- 'davis, d'
    bills[bills$session == "2006-RS" & bills$bill_id %in% c("HB7051", "HB7053"),]$LES_sponsor <- 'gibson, h'
  }else if(t_yrs == '2007_2008'){
    bills[bills$session == "2007-RS" & bills$bill_id %in% c('HB7165') ,]$LES_sponsor <- 'garcia, r'
    bills[bills$session == "2007-RS" & bills$bill_id %in% c("HB7065", "HB7111"),]$LES_sponsor <- 'gibson, h'
    bills[bills$session == "2007-RS" & bills$bill_id %in% c('HB0813', 'HB0957') ,]$LES_sponsor <- 'williams, t'
    bills[bills$session == "2008-RS" & bills$bill_id %in% c('HB0527', 'HB0931', 'HB1155') ,]$LES_sponsor <- 'williams, t'
  }else if(t_yrs == '2009_2010'){
    bills[bills$session == "2009-RS" & bills$bill_id %in% c('HB0053', 'HB1211') ,]$LES_sponsor <- 'garcia, l'
    bills[bills$session == "2010-RS" & bills$bill_id %in% c('HB0815') ,]$LES_sponsor <- 'garcia, l'
    bills[bills$session == "2009-RS" & bills$bill_id %in% c('HB0409', 'HB0643', 'HB0875') ,]$LES_sponsor <- 'jones, m'
    bills[bills$session == "2010-RS" & bills$bill_id %in% c('HB0467', 'HB0777', 'HB0795') ,]$LES_sponsor <- 'jones, m'
  }else if(t_yrs == '2011_2012'){
    bills[bills$session == "2011-RS" & bills$bill_id %in% c('HB0563') ,]$LES_sponsor <- 'jones, m'
    bills[bills$session == "2012-RS" & bills$bill_id %in% c('HB0495', 'HB1193') ,]$LES_sponsor <- 'jones, m'
    bills[bills$session == "2012-RS" & bills$bill_id %in% c('HB0533') ,]$LES_sponsor <- 'thompson, g'
  }else if(t_yrs %in% c('2013_2014', '2015_2016')){
    bills[bills$LES_sponsor == 'rodrigues',]$LES_sponsor <- 'rodrigues, r'
    bills[bills$LES_sponsor == 'rodriguez',]$LES_sponsor <- 'rodriguez, j'
  }

  #### Other Name Fix:
  if(t_yrs == '2017_2018'){
    bills[substring(bills$bill_id,1,1) == 'H' & bills$LES_sponsor == 'edwards',]$LES_sponsor <-  "edwards-walpole"
  }
  
  ###########################
  ##### Drop Resolutions, Messages, Communications, Reports
  ############################

  bills <-filter(bills, grepl('^HB|^SB', bill_id))
  # table(bills$bill_type)
  
  #table(gsub("[0-9].+", "", bills$bill_id))
  
  ########################
  ### DROP COMMITTEES
  drop_comms <- c('committee',
                  'administration', "agriculture", 'appropriations', 'banking', 'business', 'budget',
                  'children', '^claims', 'civil justice','community', 'communities', 'college', 
                  'crime prevention', 'commerce', ' council$', 'consumer services', 'communication', 
                  'comprehensive planning', 'domestic security',
                  'economic opp', 'economic dev', 'education', 'ethics and', 'elections', 'environmental', 
                  'families', 'family services', 'finance', 'fiscal policy', 'family security',
                  'government', 'growth management', 'health care', '^health',
                  'insurance', 'innovation', 'judiciary', '[a-z][a-z]+ justice$', 'long-term care',
                  'military', 'natural resources', 'oversight', 'public security', 'procedures', 'prek-12',
                  'public safety', 'reapportionment', 'regulation', 'regulated industries', 
                  'roads.+bridges', 'rules.+calendar', 'rules$', 'select.+council', 'select committee',
                  'technology', 'tourism', 'transportation', 'utilities', 'veterans',  'ways and means', 'workforce')
  
  if(any(grepl(paste(drop_comms, collapse = "|"), bills$LES_sponsor))){
    cat('\n')
    cat(glue('-----> Dropping {sum(grepl(paste(drop_comms, collapse = "|"), bills$LES_sponsor))} Committed Sponsored Bills (N = {nrow(bills)})'))
    bills <- filter(bills, !grepl(paste(drop_comms, collapse = "|"), LES_sponsor))
  }
  
  
  #### Adjusting for Bills By Request -- Need to do BEFORE splitting
  if(any(grepl("request", bills$LES_sponsor))){
    print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
    # cat('\n')
    # cat(glue("-----> KEEPING {nrow(filter(bills, grepl('request', LES_sponsor)))} bill(s) introduced BY REQUEST"))
    # bills$LES_sponsor <- str_trim(gsub('\\(by request\\)', '', bills$LES_sponsor))
  }
  
  ###### CHeck Missing Sponsors
  # filter(bills, LES_sponsor == "") %>% View()
  if(nrow(filter(bills, LES_sponsor == '')) > 0 | any(is.na(bills$LES_sponsor))){
    cat('\n')
    cat(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == '')) + sum(is.na(bills$LES_sponsor))} bill(s) without a sponsor"))
    bills <- filter(bills, !(LES_sponsor == '' | is.na(LES_sponsor)))     
  }

  ###################
  ###### Merge in S&S Bills
  ###################
  # *** For FLORIDA: Regular Session Bills DO NOT carryover; Special Sessions bills have A/B/C/D appended
  # *** For NOW: Newspaper coding does not extract the letters for special bills yet!
  # *** When have special bills, can either merge directly or cap special bills based on session maxes
  
  # SS_term <- SS_bills %>% filter(term == t_yrs) %>% mutate(H_max = NA, S_max = NA, s_spec = NA, any_specials = any(grepl(paste0("-SS"), bills$session)))
  # ## Adjusting Max Specials by yr
  # for(yr in as.numeric(str_split(t_yrs, "_")[[1]]) ){
  #   if(any(grepl(paste0(yr, "-SS"), bills$session))){
  #     H_max <- filter(bills, grepl(yr, session) & grepl("^H", bill_id) & grepl("SS", session)) %>% mutate(num = as.numeric(gsub('[A-Z]+', '', bill_id))) %>% pull(num)
  #     H_max <- ifelse(length(H_max) >= 1, max(H_max), NA)
  #     S_max <- filter(bills, grepl(yr, session) & grepl("^S", bill_id) & grepl("SS", session)) %>% mutate(num = as.numeric(gsub('[A-Z]+', '', bill_id))) %>% pull(num)
  #     S_max <- ifelse(length(S_max) >= 1, max(S_max), NA)
  #     which_spec <- which(table(bills[grepl(yr, bills$session) & grepl("SS", bills$session),]$session) == max(table(bills[grepl(yr, bills$session) & grepl("SS", bills$session),]$session)))[1]
  #     which_spec <- names(table(bills[grepl(yr, bills$session) & grepl("SS", bills$session),]$session)[which_spec])
  #     SS_term[SS_term$year == yr,]$H_max <- H_max
  #     SS_term[SS_term$year == yr,]$S_max <- S_max
  #     SS_term[SS_term$year == yr,]$s_spec <- which_spec
  #     rm(H_max, S_max, which_spec)
  #   }
  # }; rm(yr)
  # SS_term <- SS_term %>%
  #   mutate(special = ifelse((grepl("^H", bill_id) & is.na(H_max)) | (grepl("^S", bill_id) & is.na(S_max)), 0, special),
  #          special = ifelse(!any_specials, 0, special),
  #          special = ifelse(grepl("^H", bill_id) & !is.na(H_max) & special == 1 & num_only > H_max, 0, special),
  #          special = ifelse(grepl("^S", bill_id) & !is.na(S_max) & special == 1 & num_only > S_max, 0, special),
  #          session = ifelse(special == 0, paste0(year, '-RS'), ifelse(is.na(special_num), s_spec, paste0(year, '-SS', special_num)))) %>%
  #   distinct(term, session, bill_id, SS)
  
  SS_term <- SS_bills %>%
    filter(term == t_yrs) %>%
    mutate(session = paste0(year, '-RS')) %>%
    distinct(term, session, bill_id, SS)
  
  ### Merge
  bills <- bills %>%
    left_join(SS_term, by = c("bill_id", "term", "session")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  
  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id", "term", "session"))
  
  #############################################
  ############### Code Commemorative
  #############################################
  
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session'))
  # table(bills$commem)
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  #############################################
  ############### Code Bill History
  #############################################
  bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
  bill_hist <- read.csv(bill_hist_path)
  bill_hist$session <- as.character(bill_hist$session)
  
  ## If multiple sessions, read in those as well
  if(length(t_sessions) > 1){
    for(s in t_sessions[2:length(t_sessions)]){
      bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
      s_hist <- read.csv(bill_path)
      s_hist$session <- as.character(s_hist$session)
      bill_hist <- bind_rows(bill_hist, s_hist)
    }
    rm(s, s_hist)
  }
  
  ### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
  bill_hist$term <- t_yrs
  bill_hist$session_year <- gsub('[A-Z]+', '', bill_hist$session)
  bill_hist$session_type <- recode(gsub('^[0-9]+', '', bill_hist$session), 'A' = 'SS-A', 'B' = 'SS-B', 'C' = 'SS-C', 'D' = 'SS-D', 'E' = 'SS-E', 'F' = 'SS-F')
  bill_hist$session_type <- ifelse(bill_hist$session_type == '', 'RS', bill_hist$session_type)
  bill_hist$session <- paste(bill_hist$session_year, bill_hist$session_type, sep = '-')
  bill_hist <- select(bill_hist, -c(session_year, session_type))

  ######## Standardize BillHist Bill IDs
  bill_hist <- rename(bill_hist, bill_id = bill_num) %>%
    mutate(pad_num = ifelse(grepl("SS", session), 5, 4),
           bill_id = paste0(gsub(' .+', '', bill_id), str_pad(gsub('^[A-Z]+ ', '', bill_id), pad_num, pad = 0))) %>%
    select(-pad_num)
  
  ### Arrange
  bill_hist <- arrange(bill_hist, session, bill_id, order) # %>% group_by(session, bill_id) %>% mutate(order = 1:n()) %>% ungroup()
 
  ### Make Sure No Excess text in Bill Action *** This is important, especially for later years ***
  bill_hist$action <- str_trim(bill_hist$action)
  
  ### Fill in Missing Chamber Info
  bill_hist$chamber <- ifelse(str_trim(bill_hist$chamber) == "" & grepl('governor|filed with secretary of state|^chapter', tolower(bill_hist$action)), 'Executive', bill_hist$chamber)
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  aic_t <- c('on committee agenda', 'on subcommittee agenda', 'on council agenda', '^favorable|; favorable', 
             '^recommendation', '^unfavorable|; unfavorable', '^cs by|; cs by', '^cs[a-z\\/]+ by') 
  # ---> Need the semicolon and ^s for some actions for early years, when all actions clumped together and recorded in same row
  # ---> Last regez ('^cs[a-z\\/]+ by') catches cs/cs/cs by...or any other abbreviatd actions seperated by backslashes so long as they start with cs (otherwise could catch a lot of things, like signed by)
  # ---> "recommendation: fav" or "Recommendation: Unfavorable" = Subcommittee recommendations
  # ---> Council = Redistricting council, commerce council, etc, so functionally a ocmmittee 
  abc_t <- c('^favorable|; favorable', '^cs by|; cs by', 'read second time', 'amendment\\(s\\) adopted', 'amendment\\(s\\) failed', 'read third time',
             'special order calendar', 'local calendar', 'placed on consent calendar', 'placed on calendar -(hj|sj)',
             'read 2nd time', 'read 3rd time', 'on 3rd reading', 'on 2nd reading')
  pc_t <- c('read third time.+passed', 'read second and third time.+passed', '^passed', '^cs passed', 'ordered enrolled')
  law_t <- c('chapter no\\. [0-9]+', 'approved by governor', 'law without governor')
  
  ### Check Actions
  # filter(bill_hist, grepl('placed on calendar', tolower(action))) %>% distinct(action) %>% View()
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
    hist_sub <- filter(bill_hist, bill_id == b_id, session == s_id)
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
    bill_stages$bill_url <- bills[i,]$bill_url
    ### Dropping bills with "Died, not introduced"
    if(sum(bill_stages[6:9]) == 0 & grepl("died, not introduced", tolower(hist_sub[nrow(hist_sub),]$action)) ){
      bill_stages$introduced <- 0
    }
    all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
    # print(i)
  }
  options(warn = 1)
  
  ### Check Codings
  cat('\n')
  all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
    summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% as.data.frame() %>% print()
  ## Below will pull all bills with id, so may get wrong session
  # filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 1,]$bill_id) %>% View()
  
  ### MERGE
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
  rm(SS_term)

  ### Adjust Commems if SS == 1 
  all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
  
  ### Save Stage Info
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
  
  ######## Cosponsorship Info --- 
  all_sponsors$num_cosponsored_bills <- NA
  bills$cospon_match <- paste(bills$LES_sponsor, tolower(bills$sponsors), tolower(bills$cosponsors), sep = '; ')
  for(i in 1:nrow(all_sponsors)){
    c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
    ### ONLY need to adjust this way if sponsored and cosponsored column are the same
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  }
  bills <- select(bills, -cospon_match)
  # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))

  ######## Setting Num Cosponsored Bills to NA if Last Names Duplicated
  # ** These will be 0 because the consponsor variable is still just the last name
  if(nrow(name_sub) > 0){
    all_sponsors[all_sponsors$LES_sponsor %in% name_sub$new_name,]$num_cosponsored_bills <- NA
  }
  
  #######################
  #### CLEAN NAMES
  
  all_sponsors$first_name <- gsub(', |\\.$', '', str_extract(all_sponsors$LES_sponsor, ", [a-z]\\.$|, [a-z]$"))
  all_sponsors$first_name <- ifelse(is.na(all_sponsors$first_name), '', all_sponsors$first_name )
  all_sponsors$last_name <- gsub(', [a-z]\\.$|, [a-z]$', '', all_sponsors$LES_sponsor)
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  
  #### Update Last Names for Matching 
  if(t_yrs %in% c('2001_2002', "2003_2004", "2005_2006", '2007_2008')){
    all_sponsors[all_sponsors$LES_sponsor == 'dawson',]$last_name <-  "dawsonwhite"
  }
  if(t_yrs %in% c('2009_2010', '2013_2014', '2015_2016', '2017_2018')){ # ran for senate and lost 2010
    all_sponsors[all_sponsors$LES_sponsor == 'rader',]$last_name <-  "greensteinrader"
  }
  if(t_yrs %in% c('2013_2014', '2015_2016', '2017_2018')){
    all_sponsors[all_sponsors$LES_sponsor == 'braynon ii',]$last_name <-  "braynon"
  }
  if(t_yrs == '2013_2014'){
    all_sponsors[all_sponsors$LES_sponsor == 'castor dentel',]$last_name <-  "dentel"
  }
  if(t_yrs == '2017_2018'){
    all_sponsors[all_sponsors$LES_sponsor == 'williams',]$last_name <-  "hawkinswilliams"
    all_sponsors[all_sponsors$LES_sponsor == 'edwards-walpole',]$last_name <-  "edwards"
  }
  
  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  H_elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  S_elec_year <- H_elec_year - 2 ### Staggered so need to get T - 1 and T - 3
  
  ### Adjust 2016 -- ALL seats up in senate due to court-mandated redistricting
  if(t_yrs == '2017_2018'){
    S_elec_year <- H_elec_year
  }
    
  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% H_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% S_elec_year:(S_elec_year + sen_term_length - 1) | (year == S_elec_year + sen_term_length & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz, middle) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)
  
  ####################################################################################
  ############## Match Sponsors Names to Klarner Data, Forgoing Function because mostly only have last name
  ####################################################################################
  
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), '')))
  klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]'))) 
  
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
      # ## Check First Initial
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
  if(t_yrs == "2001_2002"){
    km <- filter(km, cand != 'bankhead, william g. (bill)')
    km <- filter(km, cand != 'forman, howard c.')
    km <- filter(km, cand != 'gutman, alberto (al)')
  }else if(t_yrs == '2003_2004'){
    km <- filter(km, !(cand %in% c('futch, howard e.', 'laurent, john', 'latvala, jack', 'sanderson, debby', 'rossin, tom')))
  }else if(t_yrs == '2005_2006'){
    km <- filter(km, !(cand %in% c('moriarty, timothy', 'cowin, anna', 'futch, howard e.', 'wassermanschultz, debbie'))) 
  }else if(t_yrs == '2007_2008'){
    km <- filter(km, cand != 'benson, holly')
  }else if(t_yrs == '2009_2010'){
    km <- filter(km, cand != 'posey, bill')
  }else if(t_yrs == '2011_2012'){
    km <- filter(km, cand != 'atwater, jeff')
    km <- filter(km, cand != 'gelber, dan')
  }else if(t_yrs == '2013_2014'){
    km <- filter(km, !(cand %in% c('storms, ronda', 'norman, jim', 'oelrich, steve', 'haridopolos, mike', 'rich, nan h.'))) 
  }else if(t_yrs == '2015_2016'){
    km <- filter(km, cand != 'thrasher, john')
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
  
  ############################################
  ##### Estimate Scores + Add in Relatd Variables
  ############################################
  
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
  
  
  cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************")); cat('\n')
  
}
################# ****** END LOOP

#agg_stats, matches
rm(all_sponsors, bills, legis_data, SS_bills, commem_bills, H_elec_year, S_elec_year, i, keep_types)
rm(k_matches, klarner_sub, km, bill_path, t_yrs, t_sessions, calc_LES) # 
rm(t, terms, klarner_gs, m_sub, c_sub, name_dups, name_sub, drop_comms) #  parsed_names


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by SPECIAL ELECTION --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
# ** HOUSE MEMBER LIST (with start dates): https://www.myfloridahouse.gov/Sections/Representatives/representatives.aspx?LegislativeTermId=82
# ** SENATE LIST 2008+: http://www.flsenate.gov/Senators/2008-2010
###############################
### NOTE: Because legislators take office immediately upon election, drop rules a bit different here
### ------> If sponsor no bills and retire/leave by january --> Drop

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~
# -----> Dropping 301 Committed Sponsored Bills (N = 4639)
# -----> Dropping 3 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- EVERS (greg)
### WON SPECIAL ~ SENATE:
# -- FUTCH; KING; WASSERMAN SCHULTZ; WISE
### IN HOUSE:
# -- FEENEY (speaker)
### IN SENATE:
# -- MCKAY (president)
### DROP:
# -- bankhead, william g. (bill) --- appointed to admin post -- https://www.orlandosentinel.com/news/os-xpm-1999-01-26-9901260093-story.html
# -- forman, howard c. -- resigned (had to) to run for clerk job -- https://www.sun-sentinel.com/news/fl-xpm-2000-01-15-0001150087-story.html
# -- gutman, alberto (al) -- resigned as part of plea bargain -- https://en.wikipedia.org/wiki/Alberto_Gutman

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~
# -----> Dropping 333 Committed Sponsored Bills (N = 4902)
# -----> Dropping 4 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- ALTMAN; BOGDANOFF; CARROLL; SULLIVAN (don)
### WON SPECIAL ~ SENATE:
# -- HARIDOPOLOS (via H)
### IN HOUSE:
# -- HARIDOPOLOS -- Won senate special in late march, in chamber for ~4.5 months -- https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=5099
### IN SENATE:
# -- KING
### DROP:
# -- futch, howard e. -- died January 24, 2003 -- https://www.sun-sentinel.com/news/fl-xpm-2003-01-24-0301231454-story.html
# -- laurent, john -- elected to judicial position -- https://ballotpedia.org/John_F._Laurent
# -- latvala, jack -- termed out post 2000-2002 (elected in 1994 special) -- https://ballotpedia.org/Jack_Latvala
# -- sanderson, debby -- didnt run again after 2000-2002 2-year term -- https://www.sun-sentinel.com/news/fl-xpm-2002-07-19-0207180664-story.html
# -- rossin, tom -- termed out after 2-year term -- elected in 1994 special -- https://www.sun-sentinel.com/news/fl-xpm-2002-09-18-0209180259-story.html

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~
# -----> Dropping 202 Committed Sponsored Bills (N = 4514)
### WON SPECIAL ~ HOUSE:
# -- SIMMONS
### WON SPECIAL ~ SENATE
# -- HARIDOPOLOS (past term)
### IN HOUSE:
# -- BENSE (speaker)
# -- GARDINER
### IN SENATE:
# -- LEE (prez); 
### DROP: 
# -- moriarty, timothy -- No records he was in office that term -- via waybackmachine
# -- cowin, anna -- resigned to run for superintendent -- https://www.orlandosentinel.com/news/os-xpm-2004-05-15-0405150044-story.html
# -- futch, howard e. -- died January 24, 2003 -- https://www.sun-sentinel.com/news/fl-xpm-2003-01-24-0301231454-story.html
# -- wassermanschultz, debbie -- won US House seat


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~
# -----> Dropping 237 Committed Sponsored Bills (N = 4625)
# -----> Dropping 15 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- BRAYNON (oscar); DORWORTH; FORD (clay); HUDSON; KELLY
# -- MCBURNEY; SASSO; SCHULTZ; SOTO
### WON SPECIAL ~ SENATE:
# -- DEAN (charles)
### IN HOUSE:
# -- MAHON -- left office 8/31/07
# -- BAXLEY -- left office june 2007
# -- DEAN -- elected to senate in special in june 2007
# -- QUINONES -- left office february 1st, 2007
# -- RUBIO -- speaker
### IN SENATE:
# -- PRUITT (prez)
### DROP
# -- benson, holly -- appointed to admin post dec 2006


### ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~
# -----> Dropping 207 Committed Sponsored Bills (N = 4443)
# -----> Dropping 16 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- BERNARD; CRUZ (janet); GAETZ
### WON SPECIAL ~ SENATE:
# -- NEGRON; THRASHER
### IN HOUSE:
# -- SANSOM -- left office 2/21/2010
# -- CRETUL (speaker)
# -- BRAYNON (oscar)
### DROP:
# -- posey, bill -- won US House seat

  
### ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~
# -----> Dropping 392 Committed Sponsored Bills (N = 3810)
### WON SPECIAL ~ HOUSE:
# -- OLIVA
# -- WATSON B
### WON SPECIAL ~ SENATE:
# -- BRAYNON II (via H, Feb. 2011)
# -- GIBSON (audrey, Oct 2011)
### IN HOUSE: 
# -- PROCTOR; CANNON (speaker); PRECOURT; LEGG; WEATHORFORD
# -- SNYDER; BRAYNON (until Feb 2011); LOPEZCANTERA; SAUNDERS
### DROP:
# -- atwater, jeff -- left at end of 2010 term
# -- gelber, dan -- left at end of 2010 term

### ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~
# -----> Dropping 313 Committed Sponsored Bills (N = 3281)
### WON SPECIAL ~ HOUSE:
# -- HILL (mike)
# -- MURPHY (amanda)
### IN HOUSE:
# -- CORCORAN; WEATHERFORD (speaker); MCKEEL; CRISAFULLI
### IN SENATE:
# -- GAETZ (prez)
### DROP:
# *** None shown on senate page for this term, and individual bios show all terms up to 2010-2012
# -- storms, ronda
# -- norman, jim
# -- oelrich, steve
# -- haridopolos, mike
# -- rich, nan h.


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~
# -----> Dropping 234 Committed Sponsored Bills (N = 3254)
### WON SPECIAL ~ HOUSE:
# -- RENNER
# -- STEVENSON
### WON SPECIAL ~ SENATE:
# -- HUTSON (via H)
### IN HOUSE:
# -- CRISAFULLI (speaker)
### In SENATE:
# -- GARDINER (prez)
### DROP:
# -- thrasher, john -- resigned Nov 9, 2014
  
# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~
# **********
# *** NOTE: ALL 40 SENATE SEATS WERE UP FOR ELECTION in 2016 -- Court-mandated redistricting ***  
# **********
# -----> Dropping 303 Committed Sponsored Bills (N = 5932)
### WON SPECIAL ~ HOUSE:
# -- FERNANDEZ; MCCLURE; OLSZEWSKI; PEREZ
### WON SPECIAL ~ SENATE:
# -- TADDEO
### IN HOUSE
# -- CORCORAN; OLIVA
### IN SENATE:
# -- SIMPSON; NEGRON

# filter(klarner, grepl('edwards', cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(klarner, ddez == 46) %>% select(cand, year, sen, etype, outcome, ddez, candid)



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

### Manual Fixes
# LES[LES$data_name %in% "carroll" & LES$term %in% "2017_2018",]$klarner_id <- NA
# LES[LES$data_name %in% "carroll" & LES$term %in% "2017_2018",]$klarner_name <- NA
# LES[LES$data_name %in% "carroll" & LES$term %in% "2017_2018",]$sponsor <- "zzzzzzzz"

### ****Still missing***** 
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[5]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, missing, still_missing)

name_matches <- data.frame(LES_name = "sullivan", k_name = 'sullivan, donald c.')
name_matches <- add_row(name_matches, LES_name = 'gaetz', k_name = 'gaetz, matt')
name_matches <- add_row(name_matches, LES_name = 'gibson', k_name = 'gibson, audrey')
name_matches <- add_row(name_matches, LES_name = 'hill', k_name = 'hill, mike')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', k_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}

############## Check for Duplicates
### ---> If two people with same last name and one won in a special, will lead to duplicates
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

#### *** 2017_2018: If any of below run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "perez",]$party <- 'r'
LES[LES$sponsor == "perez",]$sponsor <- 'perez, daniel' 
LES[LES$sponsor == "olszewski",]$party <- 'r'
LES[LES$sponsor == "olszewski",]$sponsor <- 'olszewski, robert' 
LES[LES$sponsor == "mcclure",]$party <- 'r'
LES[LES$sponsor == "mcclure",]$sponsor <- 'mcclure, lawrence' 
LES[LES$sponsor == "taddeo",]$party <- 'd'
LES[LES$sponsor == "taddeo",]$sponsor <- 'taddeo, annette' 

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

### If > 2-Year Terms: Expand Senate Rows
senate <- filter(hf_data, CandId == 'aaaa')
for(i in 1:nrow(hf_data)){
  if(hf_data[i,]$chamber == "House") next
  sen_sub <- filter(hf_data, CandId == hf_data[i,]$CandId)
  if( !(paste0( hf_data[i,]$year + 3, "_",  hf_data[i,]$year + 4) %in% sen_sub$term) ){
    new_row <- hf_data[i,]
    new_row$MajorityMember <- NA
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
  filter(dup == TRUE)

### Set Committees to NA for Years without Data -- May have matched candids in year range
set_NA <- colnames(hf_data)
set_NA <- set_NA[!(set_NA %in% c("CandId", 'chamber', "year", "term")) ]
LES[LES$term == '2017_2018', set_NA] <- NA

rm(hf_data, set_NA, i)

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
    ### Check Last + First Name
    ideo_match <- filter(ideo[check_last,], match_name == gsub(' [a-z]\\.$', '', LES[i,]$sponsor) )    
    
    ### CHeck First Initial
    if(nrow(ideo_match) != 1){
      ideo_match <- ideo[check_last,][substring(gsub('.+, ', '', tolower(ideo[check_last,]$name)), 1, 1) == substring(gsub(".+, ", '', LES[i,]$sponsor), 1, 1) ,]  
    }
    ### Try Data Name
    # if(nrow(ideo_match) != 1){
    #   ideo_match <- filter(ideo[check_last,], match_name == LES[i,]$data_name)
    # }
    ### Check Party if Still Too Long
    if(nrow(ideo_match) == 2 & length(unique(ideo_match$name)) == 1 & any(ideo_match$party == 'R') &any(ideo_match$party == 'D')){
      for(p in c('d', 'r')){
        if(any(LES[LES$sponsor == LES[i,]$sponsor,]$party == p)){
          LES[LES$sponsor == LES[i,]$sponsor & LES$party == p,]$SM_name <- ideo_match[ideo_match$party == toupper(p),]$name
          LES[LES$sponsor == LES[i,]$sponsor & LES$party == p,]$SM_party <- ideo_match[ideo_match$party == toupper(p),]$party
          LES[LES$sponsor == LES[i,]$sponsor & LES$party == p,]$np_score <- ideo_match[ideo_match$party == toupper(p),]$np_score
        }
      }
    } else if(nrow(ideo_match) > 1){
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
LES[LES$sponsor %in% c('smith, carlos guillermo'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
## ** Notable Names -- Dixon Alan Hays; Harold William 'Bill' Heller; Warren Keith Perry; Edwin Cary Pigman; Charles 'David' Hood
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### CHecking All Matched to a Single Name ---> The mismatched parties likely wrong
# filter(LES, !grepl(' ', SM_name) & !is.na(SM_name)) %>% select(sponsor, party, SM_name, SM_party, term) %>% distinct() %>% as.data.frame()
LES[LES$sponsor %in% c('clemons, chuck', 'fischer, jason'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# ---- Remaining are mostly from 2009_2010
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('spratt', tolower(name))) %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'bradley, rob', SM_name = 'Bradley, Robert') 
name_matches <- add_row(name_matches, LES_name = 'chestnut, charles s. iv', SM_name = 'Chestnut, Charles IV')
# name_matches <- add_row(name_matches, LES_name = 'coley, david a.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'cortes, bob', SM_name = 'Cortes, Robert')
name_matches <- add_row(name_matches, LES_name = 'dentel, karen castor', SM_name = 'Castor Dentel, Karen')
name_matches <- add_row(name_matches, LES_name = 'fant, jay', SM_name = 'Fant III, Julian')
name_matches <- add_row(name_matches, LES_name = 'greensteinrader, kevin j.', SM_name = 'Rader, Kevin J.G') # 2016 is row with J.G.
name_matches <- add_row(name_matches, LES_name = 'hill, mike', SM_name = 'Hill, Walter Byran') # Walter Byran "Mike" Hill
# name_matches <- add_row(name_matches, LES_name = 'lee, e. denise', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'nunez, jeanette', SM_name = 'Nuñez, Jeanette M')
name_matches <- add_row(name_matches, LES_name = 'quinones, john q.', SM_name = 'Quiñones, J.')
name_matches <- add_row(name_matches, LES_name = 'russell, dave', SM_name = 'Russell, David Jr.')
name_matches <- add_row(name_matches, LES_name = 'smith, christopher', SM_name = 'Smith, Christopher')
name_matches <- add_row(name_matches, LES_name = 'smith, rod', SM_name = 'Smith')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

#### Same Person, Multiple Rows
LES[LES$sponsor == 'diazdelaportilla, miguel',]$SM_name <-  ideo[ideo$name == 'Diaz de la Portilla, Miguel' & ideo$senate2011 %in% 1,]$name
LES[LES$sponsor == 'diazdelaportilla, miguel',]$SM_party <- ideo[ideo$name == 'Diaz de la Portilla, Miguel' & ideo$senate2011 %in% 1,]$party
LES[LES$sponsor == 'diazdelaportilla, miguel',]$np_score <- ideo[ideo$name == 'Diaz de la Portilla, Miguel' & ideo$senate2011 %in% 1,]$np_score

## Two Rows -- Other 'Garcia, Rodolfo Jr.' Is Actually be Rene Garcia based on years
LES[LES$sponsor == 'garcia, rodolfo (rudy) jr.',]$SM_name <-  ideo[ideo$name == 'Garcia, Rodolfo Jr.' & ideo$house1996 %in% 1,]$name
LES[LES$sponsor == 'garcia, rodolfo (rudy) jr.',]$SM_party <- ideo[ideo$name == 'Garcia, Rodolfo Jr.' & ideo$house1996 %in% 1,]$party
LES[LES$sponsor == 'garcia, rodolfo (rudy) jr.',]$np_score <- ideo[ideo$name == 'Garcia, Rodolfo Jr.' & ideo$house1996 %in% 1,]$np_score
LES[LES$sponsor == 'garcia, rene',]$SM_name <-  ideo[ideo$name == 'Garcia, Rodolfo Jr.' & ideo$senate2015 %in% 1,]$name
LES[LES$sponsor == 'garcia, rene',]$SM_party <- ideo[ideo$name == 'Garcia, Rodolfo Jr.' & ideo$senate2015 %in% 1,]$party
LES[LES$sponsor == 'garcia, rene',]$np_score <- ideo[ideo$name == 'Garcia, Rodolfo Jr.' & ideo$senate2015 %in% 1,]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname)

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1997 - 2020
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzz) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1997 - 2020
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzz) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


#############################
#### NAME STANDARDIZATION + OTHER FIXES
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

### Fix Klarner Name with Numeric
# filter(LES, grepl('[0-9]', sponsor))

### Remove Nicknames
LES$sponsor <- gsub(' +', ' ', gsub('\\([^\\)]+\\)', '', LES$sponsor))

### Richard Machek -- Party Wrong or Switched -- Labeled as Dem in House Page -- https://www.myfloridahouse.gov/Sections/Representatives/representatives.aspx?LegislativeTermId=79
# ** Possible he was on both the Dem and Rep Primary Ballot -- https://www.sun-sentinel.com/news/fl-xpm-2000-09-03-0009050147-story.html
LES[LES$sponsor == 'machek, richard' & LES$term == '2001_2002',]$party <- 'd'


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
# ****** 
# filter(LES, party == 'd' & np_score > .25) %>% select(sponsor, term, chamber, np_score, party, SM_party, SM_name)
# filter(LES, party == 'r' & np_score < -.25) %>% select(sponsor, term, chamber, np_score, party, SM_party, SM_name)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(chamber), data = LES), omit = 'factor', type = 'text')


