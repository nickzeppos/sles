

################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** New Hampshire *** BY SESSION
##############################################################

# ********************************************
# *******NOTE: WHEN RUNNING, ONLY PRINTING MISSING KLARNER SENATORS AS WAY TOO MANY HOUSE MEMBERS --> ASSUMING ALL IN CHAMBER
# *********************************************

##################
##### TO DO
# **** NEED TO DROP PEOPLE WHO JOURNAL RECORDS AS NEVER HAVING BEEN SEATED (See top of manual fixes below...)
# **** ESPECIALLY FROM HOUSE -- 66 People in 1989 who were elected and sponsored no bills... 
# ***** FOR NOW: ASSUMING IF YOU WON, YOU SERVED.
#####################

###################################
## (SPECIAL) SESSIONS:
## ---- Bills appear to carryover from regular to regular session, with numbers continuing to increment
## ---- Special Sessions bills have SS appended to front of bill (or SPECIAL SESSION BILL in title (see 1989))
## MEMBER LISTS:
## ---- 
## PROCESS/RULES:
## ---- See "Manual of the New Hampshire General Court" PDF
## Sponsorship/Authorship
## ---- Cap of 5 sponsors for any bill (1995-1996); rest can be cosponsors
## ---- If > 5 want to sponsor, requestor of bill gets to decide who is sposnor vs cosponsr
## -------> Requestor == Primary Sponsor
###########################
## NOTES:
## -- (1) If comm recommends ITL (inexpedient to leg) and house "adopts ITL report" --> ABC?
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

this_state <- 'NH'
min_year <- 1989
max_year <- 2018
keep_types <- c("HB", "SB", "SSHB", "SSSB") 
# HBI/SBI = Bill of Intent = Written explanation of a problem/concern -- no proposed change in law
# ---> Unclear if they can transition into regular bills DURING process (and thus switch from HBI to HB)
# CACR = Constitutional Amendment Concurrent Resolution
spec_elec_codes <- c('s', 'gs')
house_term_length <- 2
sen_term_length <- 2 # STAGGERED? NA

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
commem_bills$bill_id <- gsub("^SS", "", commem_bills$bill_id)

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
## Two Charles Laflammes in Klarner Data; Hillsborough 61 should be Paul -- http://gencourt.state.nh.us/house/caljourns/journals/2003/houjou2003_01.html
klarner[klarner$candid == 152107,]$cand <- "laflamme, paul" 

### quandt, marshall e. == quandt, lee --> https://www.seacoastonline.com/article/20081107/NEWS/811070326
klarner[klarner$cand == "quandt, lee",]$cand <- "quandt, marshall e." 
# --> IDs will be off

### Gallus won in 2010, not dorthy solomon ---> Votes are reversed; will still be wrong
klarner[klarner$cand == "solomon, dorothy" & klarner$year == 2010,]$outcome <- "l"
klarner[klarner$cand == "gallus, john" & klarner$year == 2010,]$outcome <- "w"

### Giuda is Wrong in 2017-2018... Fixing CandId + Name
klarner[klarner$cand == "giuda, brandon" & klarner$year == 2016,]$candid <- 145235
klarner[klarner$cand == "giuda, brandon" & klarner$year == 2016,]$cand <- "giuda, robert j."

### Spelling Error --> ID wrong
klarner[klarner$cand == "katakiores, phyllis" & klarner$year == 2008,]$candid <- 145339
klarner[klarner$cand == "katakiores, phyllis" & klarner$year == 2008,]$cand <- "katsakiores, phyllis hemeon"

### P. Judith Sullivan -- Appointed March 1999, won 2000 election for Carroll 2 --> Miscoded as Henry P. Sullivan
klarner[klarner$cand == "sullivan, henry p." & klarner$year == 2000,]$cand <- "sullivan, p. judith"

##########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[2]

for(t in terms){
  
  ### Formulate 2-year terms -- Cover both regular and special sessions
  t_yrs <- as.character(glue('{t}_{t+1}'))
  t_sessions <- sessions[grepl(glue('{t}|{t+1}'), sessions)]
  
  ### Skip Previously Estimated
  if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
    print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
    next
  }
  
  ###### SESSION IN PROGRESS
  print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))
  
  ############### Read in data
  bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_sessions[1]}.csv")
  bills <- read_csv(bill_path, col_types = cols())
  
  ### If multiple sessions in different files, read in those as well
  if(length(t_sessions) > 1){
    for(s in t_sessions[2:length(t_sessions)]){
      bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{s}.csv")
      s_bills <- read_csv(bill_path, col_types = cols())
      bills <- bind_rows(bills, s_bills)
    }
    rm(s, s_bills)
  }
  
  ######## Standardize the Bill IDs
  bills <- rename(bills, bill_id = bill_number) %>%
    mutate(bill_id = gsub("-.+$|[A-Z]+$", "", bill_id),
           bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = 0)))
  
  ### Clean Term/Session Variables + Removing SS from Special Bills
  bills <- bills %>%
    rename(session = session_year) %>%
    mutate(term = t_yrs,
           session = ifelse(grepl("^SS", bill_id) | grepl("SPECIAL SESSION BILL", title), paste0(session, '-SS'), paste0(session, '-RS')),
           bill_id = gsub("^SS", "", bill_id))
  
  ### Check for duplicates
  if(nrow(bills) != nrow(distinct(bills))){
    cat(" \n ~~~> DUPLICATE BILLS \n\n .")
    break
  }
  
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
  
  ### Remove unnecessary parentheticals
  bills$sponsors <- gsub(' \\([a-z][a-z][^\\)]+\\)| \\(\\#[^\\)]+\\)', '', bills$sponsors)
  bills$sponsors <- gsub('  +', ' ', bills$sponsors)
  
  #### Fix Issues with Specific Bills (MIssing Main Sponsor and Attributing Sponsorship to Wrong Chamber)
  if(t_yrs == '1993_1994'){
    bills[bills$bill_id %in% c("HB1104", "HB1134", "HB1247"),]$sponsors <- paste0('andrew christie, jr. (r); ', bills[bills$bill_id %in% c("HB1104", "HB1134", "HB1247"),]$sponsors)
    bills[bills$bill_id == "HB1442",]$sponsors <- paste0('patricia a. dowling (r); ', bills[bills$bill_id == "HB1442",]$sponsors)
  }else if(t_yrs == "1995_1996"){
    bills[bills$bill_id %in% c("HB0187"),]$sponsors <- "donna sytek (r); carl johnson (r); c. jeanne shaheen (d)"
    bills[bills$bill_id %in% c("HB0353"),]$sponsors <-  paste0('patricia a. dowling (r); ', bills[bills$bill_id == "HB0353",]$sponsors) ### See 1995 House Journal, page 49
    bills[bills$bill_id %in% c("HB0137", "HB0542"),]$sponsors <-  paste0('patricia a. dowling (r); ', bills[bills$bill_id %in% c("HB0137", "HB0542"),]$sponsors)
    bills[bills$bill_id %in% c("HB0463"),]$sponsors <- paste0('andrew christie, jr. (r); ', bills[bills$bill_id %in% c("HB0463"),]$sponsors)
  }else if(t_yrs == "1997_1998"){
    bills[bills$bill_id %in% c("HB0594"),]$sponsors <- "bonnie ham (r); edward gordon (r)"
    bills[bills$bill_id %in% c("HB0455", "HB0462", "HB0514", "HB0532"),]$sponsors <- paste0('andrew christie, jr. (r); ', bills[bills$bill_id %in% c("HB0455", "HB0462", "HB0514", "HB0532"),]$sponsors)
  }else if(t_yrs == "2001_2002"){
    bills[bills$bill_id %in% c("HB0135"),]$sponsors <- "robert rowe (r); edward gordon (r)"
  }else if(t_yrs == '2003_2004'){
    bills[bills$bill_id %in% c("HB0308"),]$sponsors <- "carolyn gargasz (r); sheila roberge (r)"
    bills[bills$bill_id %in% c("HB1167"),]$sponsors <- "john balcom (r+d); sheila roberge (r)"
    bills[bills$bill_id %in% c("HB1168"),]$sponsors <- paste0("john balcom (r+d); ", bills[bills$bill_id %in% c("HB1168"),]$sponsors) 
  }else if(t_yrs == '2005_2006'){
    bills[bills$bill_id %in% c("HB1185"),]$sponsors <- "carolyn gargasz (r); andre' martel (r)"
  }else if(t_yrs == '2007_2008'){
    bills[bills$bill_id %in% c("HB0292"),]$sponsors <- "carolyn gargasz (r); peter franklin (d); sheila roberge (r)"
    bills[bills$bill_id %in% c("HB0289", "HB0841", "HB1239"),]$sponsors <- paste0("carolyn gargasz (r); ", bills[bills$bill_id %in% c("HB0289", "HB0841", "HB1239"),]$sponsors) 
  }else if(t_yrs == '2009_2010'){
    bills[bills$bill_id %in% c("HB0116"),]$sponsors <- "carolyn gargasz (r); deborah reynolds (d)"
  }else if(t_yrs == '2011_2012'){
    bills[bills$bill_id %in% c("HB1481"),]$sponsors <- "adam schroadter (r); nancy stiles (r)"
  }else if(t_yrs == '2015_2016'){
    bills[bills$bill_id %in% c("HB0221"),]$sponsors <- "wayne burton (d); martha fuller clark (d)"
  }else if(t_yrs == '2017_2018'){
    bills[bills$bill_id %in% c("HB1695"),]$sponsors <- "herbert richardson (r); bob giuda (r)"
    bills[bills$bill_id %in% c("HB1462"),]$sponsors <- "herbert richardson (r); jeff woodburn (d)"
    bills[bills$bill_id %in% c("HB1287"),]$sponsors <- "brian stone (r); john reagan (r)"
  }
  #filter(bills, grepl("^martha fuller clark", sponsors) & substring(bill_id, 1,1) == "H") %>% select(bill_id, sponsors, sponsors_on_bill)
  #filter(klarner, grepl('burton', cand) & year == 2014) %>% select(cand, year, sen, outcome, partyz)
  
  #### Fix John O'Connor x2 in 2017-2018
  if(t_yrs == "2017_2018"){
    o_bills <- filter(bills, grepl("o'connor", sponsors))
    o_bills$which_oconnor <- str_extract(tolower(o_bills$sponsors_on_bill), "john [a-z]. o'connor")
    for(i in 1:nrow(o_bills)){
      bills[bills$bill_id == o_bills[i,]$bill_id,]$sponsors <- gsub("john o'connor", o_bills[i,]$which_oconnor, bills[bills$bill_id == o_bills[i,]$bill_id,]$sponsors)
    }
    rm(o_bills)
  }
  
  ### LES Sponsor Var
  bills$LES_sponsor <- gsub(';.+', '', bills$sponsors)
  bills$LES_spon_party <- gsub('\\(|\\)', '', str_extract(bills$LES_sponsor, '\\(.+\\)$'))
  bills$LES_sponsor <- gsub(' \\(.+', '', bills$LES_sponsor)
  ### Some recorded as D AND R
  
  #### Adjusting for Bills By Request -- Need to do BEFORE splitting
  if(any(grepl("request", bills$LES_sponsor))){
    print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
    # cat('\n')
    # cat(glue("-----> KEEPING {nrow(filter(bills, grepl('request', LES_sponsor)))} bill(s) introduced BY REQUEST"))
    # bills$LES_sponsor <- str_trim(gsub('\\(by request\\)', '', bills$LES_sponsor))
  }

  #### Drop Uncoded Committees
  if(any(grepl('committee', bills$LES_sponsor))){
    cat('\n')
    cat(glue('---> Dropping {sum(grepl("committee", bills$LES_sponsor))} Committed Sponsored Bills (N = {nrow(bills)})'))
    bills <- filter(bills, !grepl('committee', LES_sponsor))
  }
  
  #### Manual Name Fixes
  if(t_yrs %in% c("1993_1994", "1995_1996", "1997_1998")){
    bills$sponsors <- gsub('marjorie battles\\(-peirce\\)', 'marjorie battles', bills$sponsors)
    bills$LES_sponsor <- gsub('marjorie battles\\(-peirce\\)', 'marjorie battles', bills$LES_sponsor)
  }
  
  ###### CHeck Missing Sponsors
  # filter(bills, LES_sponsor == "" | is.na(LES_sponsor)) %>% View()
  bills[is.na(bills$LES_sponsor),]$LES_sponsor <- ''
  if(nrow(filter(bills, LES_sponsor == '')) > 0){
    cat('\n')
    cat(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
    bills <- filter(bills, !(LES_sponsor == ''))     
  }

  ###################
  ###### Merge in S&S Bills
  ###################
  # *** For NEW HAMPSHIRE: Regular Session bills carry over/ increment over years in biennium
  # --- For specials: VERY FEW but when present, ID has SS prefix (excluding 1989)
  # ---> Merging on ID + Assuming ALL SPECIALS are SS --> Only 11 across all years
  SS_term <- SS_bills %>% filter(term == t_yrs) %>% mutate(H_max = NA, S_max = NA, s_spec = NA, any_specials = any(grepl(paste0("-SS"), bills$session)))
  
  ## Adjusting Max Specials by Year
  for(year in as.numeric(str_split(t_yrs, "_")[[1]]) ){
    if(any(grepl(paste0(year, "-SS"), bills$session))){
      H_max <- filter(bills, grepl(year, session) & grepl("^H|^SSH", bill_id) & grepl("SS", session)) %>% mutate(num = as.numeric(gsub('[A-Z]+', '', bill_id))) %>% pull(num)
      H_max <- ifelse(length(H_max) >= 1, max(H_max), NA)
      S_max <- filter(bills, grepl(year, session) & grepl("^S|^SSS", bill_id) & grepl("SS", session)) %>% mutate(num = as.numeric(gsub('[A-Z]+', '', bill_id))) %>% pull(num)
      S_max <- ifelse(length(S_max) >= 1, max(S_max), NA)
      which_spec <- which(table(bills[grepl(year, bills$session) & grepl("SS", bills$session),]$session) == max(table(bills[grepl(year, bills$session) & grepl("SS", bills$session),]$session)))[1]
      which_spec <- names(table(bills[grepl(year, bills$session) & grepl("SS", bills$session),]$session)[which_spec])
      SS_term[SS_term$year == year,]$H_max <- H_max
      SS_term[SS_term$year == year,]$S_max <- S_max
      SS_term[SS_term$year == year,]$s_spec <- which_spec
      rm(H_max, S_max, which_spec)
    }
  }
  
  SS_term <- SS_term %>%
    mutate(special = ifelse((grepl("^H", bill_id) & is.na(H_max)) | (grepl("^S", bill_id) & is.na(S_max)), 0, special),
           special = ifelse(!any_specials, 0, special),
           special = ifelse(grepl("^H", bill_id) & !is.na(H_max) & special == 1 & num_only > H_max, 0, special),
           special = ifelse(grepl("^S", bill_id) & !is.na(S_max) & special == 1 & num_only > S_max, 0, special),
           session = ifelse(special == 0, 'RS', s_spec)) %>%
    distinct(term, session, bill_id, SS)
  
  ### Merge
  bills <- bills %>%
    mutate(session_adj = ifelse(grepl("RS", session), "RS", session)) %>% 
    left_join(SS_term, by = c("bill_id" = "bill_id", "term" = "term", "session_adj" = "session")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS)) 
  
  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id" = "bill_id", "term" = "term", "session" = "session_adj"))
  
  ############################
  ####### Code Commemorative
  
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
  
  bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
  bill_hist <- read_csv(bill_hist_path, col_types = cols())
  
  ### If multiple sessions in different files, read in those as well
  if(length(t_sessions) > 1){
    for(s in t_sessions[2:length(t_sessions)]){
      bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
      s_hist <- read_csv(bill_hist_path, col_types = cols())
      bill_hist <- bind_rows(bill_hist, s_hist)
    }
    rm(s, s_hist)
  }
  
  ### Drop Duplicates (Early years doubled?)
  if(nrow(bill_hist) != nrow(distinct(bill_hist))){
    print(" ~~~~ Check Bill Histories --> DUPLICATES")
    break
  }
  
  ######## Standardize BillHist Bill IDs
  bill_hist <- rename(bill_hist, bill_id = bill_number) %>%
    mutate(bill_id = gsub("-.+$|[A-Z]+$", "", bill_id),
           bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = 0)))
  
  ### Clean Term/Session Variables + Removing SS from Special Bills
  bill_hist <- bill_hist %>%
    rename(session = session_year) %>%
    mutate(term = t_yrs,
           session = ifelse(grepl("^SS", bill_id) | (session == 1989 & bill_id == "HB001"), paste0(session, '-SS'), paste0(session, '-RS')),
           bill_id = gsub("^SS", "", bill_id))
  
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
  ## NH Abbreviations: http://www.gencourt.state.nh.us/bill_status/docket_abbrev.htm
  aic_t <- c('^hearing', '^joint hearing', 'maj rep', 'min rep', 'committee report', "comm am", 
             "prop [a-z]+ am .+ hc [0-9]", 'otp', 'itl', 'report rnf', 'report re-ref', "subcom",
             "ought to pass", "inexpedient to legislate")
  # --> otp = ought to pass; itl = inexpedient to legislate; rnf = recommended but not funded (can't find these)
  abc_t <- c('committee report', 'maj rep', 'otp', 'report re-ref', ' aa ', '^aa ', 'am vv', 
             'fl am', 'floor am')
  #AA = amendemnt adopted
  pc_t <- c('passed', 'enrolled', 'ot3rdg')
  # --> senate, in 2000, starts using 'ot3rdg' for passed on third reading --- cross-checking that it actually swapped chambers should make sure this is right
  law_t <- c('signed by gov', 'chap\\.[0-9]+', 'signed by the gov', 'chapter [0-9]+', "chap: [0-9]+")
  # --> Plus know chapter numbers
  
  ##### Check Bill Search Terms
  # bill_hist %>%
  #   filter(bill_id == 'SB359') %>% as.data.frame()
  #   filter(grepl("ot3rdg", tolower(action))) %>%
  #   distinct(bill_id, action) %>%
  #   as.data.frame()
  
  # filter(bill_hist, grepl('adopted', tolower(action))) %>% distinct(action) %>% View()
  # filter(bill_hist, grepl('effective date', tolower(action))) %>% group_by(action) %>% summarize(n = n()) %>% arrange(desc(n)) %>% View()
  # mutate(bill_hist, clean = gsub('committee~.+', 'committee', gsub("[0-9]+", '', action))) %>% distinct(clean) %>% unlist() %>% unname()
  
  ####################
  ### Output Matrix
  #all_bill_stages <- data.frame(matrix(nrow = 0, ncol = 10))
  #colnames(all_bill_stages) <- c("bill_id", "term", "session", "LES_sponsor", "introduced", "action_in_comm", "action_beyond_comm", "passed_chamber", "law", "bill_url")
  
  ### Make Sure No Excess text in Bill Action
  bill_hist$action <- str_trim(bill_hist$action)
  bills$chapter_num <- ifelse(tolower(bills$chapter_num) == "none", NA, bills$chapter_num)
  ### Code History Stages for Each Bill --- Subset Hist File, Code, Save, Repeat
  options(warn = 2)
  for(i in 1:nrow(bills)){
    b_id = bills[i,]$bill_id
    s_id = bills[i,]$session
    b_spon = bills[i,]$LES_sponsor
    hist_sub <- filter(bill_hist, bill_id == b_id & session == s_id)
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
    ### ADJUSTING FOR BILLS with "PASSED" but didn't actually make it out
    if(bill_stages$passed_chamber == 1 & length(unique(hist_sub$chamber)) == 1 & bill_stages$law != 1){
      bill_stages$passed_chamber <- 0
      #print(i)
    }
    ### Adjusting for bills with Chapter Numbers that are Missing Actions
    if(bill_stages$law == 0 & !is.na(bills[i,]$chapter_num)){
      bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- bill_stages$law <- 1
    }
    ### Adjusting for Bills that have FLoor Dates but Coded ABC == 0 [Basically all of them]
    if(bill_stages$action_beyond_comm == 0 & ((grepl("^S", bills[i,]$bill_id) & !is.na(bills[i,]$S_floor_date)) | (grepl("^H", bills[i,]$bill_id) & !is.na(bills[i,]$H_floor_date))) ){
      bill_stages$action_beyond_comm <- 1
    }
    bill_stages$bill_url <- bills[i,]$bill_url
    
    ### Combine
    if(i == 1){
      all_bill_stages <- bill_stages
    }else{
      all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
    }
    # print(i)
  }
  options(warn = 1)
  
  ### Check Codings
  cat('\n')
  all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
    summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% as.data.frame() %>% print()
  # filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% View()
  # filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_beyond_comm == 0,]$bill_id) %>% View()
  
  ### MERGE to BILL DATA
  bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 
  
  ### MERGE In COMMEMS
  all_bill_stages <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))

  ### MERGE In S&S
  all_bill_stages <- mutate(all_bill_stages, session_adj = ifelse(grepl("RS", session), "RS", session))
  all_bill_stages <- SS_term %>% 
    select(bill_id, term, session, SS) %>%
    left_join(all_bill_stages, ., by = c('bill_id' = 'bill_id', 'term' = 'term', "session_adj" = "session")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS)) %>%
    select(-session_adj)
  
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
  
  #### Adding Legislators Who COSPONSORED A BILL but did not SPONSOR
  unique_cospon <- str_trim(unique(unlist(str_split(bills$sponsors, '; '))))
  unique_cospon <- gsub(' \\(.+', '', unique_cospon)
  for(nonspon in unique_cospon){
    if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != ''){
      if(nonspon == 'marjorie battles(-peirce)'){
        nonspon = 'marjorie battles\\(-peirce\\)'
      }
      chamb <- unique(substring(bills[grepl(nonspon, bills$sponsors),]$bill_id, 1, 1))
      ## 400 Reps, 24 Sen --> Coding ALL as in House --> will fix senate errors later
      if(t_yrs %in% c('1995_1996', '1997_1998') & nonspon == "joseph delahunty"){
        all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = "S", term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0) 
      }else if(t_yrs == "2007_2008" & nonspon %in% c("molly kelly", "betsi devries", "sylvia larsen", "jacalyn cilley", "peter bragdon")){
        all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = "S", term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0) 
      }else if(t_yrs == "2015_2016" & nonspon == "martha fuller clark"){
        all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = "S", term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0) 
      }else{
        all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = "H", term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0) 
      }
    }
  }
  
  ### Fix Incorrect Chambers
  # if(t_yrs == '1989_1990'){
  #   all_sponsors[all_sponsors$LES_sponsor %in% c("glenn stewart", 'maurice goulet', 'william driscoll'),]$chamber <- H
  # }
  
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
  parsed_names <- map_df(all_sponsors$LES_sponsor, parse_names) %>% distinct() 
  parsed_names <- select(parsed_names, last_name, middle_name, first_name, full_name)
  all_sponsors <- left_join(all_sponsors, parsed_names, by = c("LES_sponsor" = "full_name"))
  all_sponsors$last_name <- gsub('\\,$', '', all_sponsors$last_name)
  all_sponsors$middle_name <- gsub('\\.', '', all_sponsors$middle_name)
  all_sponsors$first_name <- gsub('\\.$', '', all_sponsors$first_name)
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  
  #### Update First/Last Names for Matching 
  if(t_yrs %in% c("1989_1990")){
    all_sponsors[all_sponsors$LES_sponsor == 'barbara remick-blinn',]$last_name <-  "remick"
  }
  if(t_yrs %in% c("1989_1990", "1991_1992")){
    all_sponsors[all_sponsors$LES_sponsor == 'james st. jean',]$last_name <-  "saintjean"
  }
  if(t %in% 1991:1996 | t %in% 1999:2004 | t %in% 2007:2012){
    all_sponsors[all_sponsors$LES_sponsor == 'karen hutchinson',]$last_name <-  "keeganhutchinson"
  }
  if(t %in% 1991:1998){
    all_sponsors[all_sponsors$LES_sponsor == 'alice calvert',]$last_name <-  "ziegra"
  }
  if(t == 1993){
    all_sponsors[all_sponsors$LES_sponsor == 'tom st. martin',]$last_name <-  "saintmartin"
  }
  if(t %in% 1995:1998){
    all_sponsors[all_sponsors$LES_sponsor == 'paul st hilaire',]$last_name <-  "sainthilaire"
    all_sponsors[all_sponsors$LES_sponsor == 'william williams',]$first_name <-  "bill"
  }
  if(t %in% 1997:2000){
    all_sponsors[all_sponsors$LES_sponsor == 'gerard st cyr',]$last_name <-  "saintcyr"
  }
  if(t %in% 1999:2000){
    all_sponsors[all_sponsors$LES_sponsor == 'linda garrish-thomas',]$last_name <-  "garrish"
  }
  if(t %in% 1999:2002){
    all_sponsors[all_sponsors$LES_sponsor == 'amy robb',]$last_name <-  "robbtheroux"
  }
  if(t_yrs %in% c("1999_2000", "2003_2004")){
    all_sponsors[all_sponsors$LES_sponsor == 'mary lou flayhan',]$last_name <-  "nowe"
  }
  if(t == 2001){
    all_sponsors[all_sponsors$LES_sponsor == 'pamela saia-rogers',]$last_name <-  "saia"
  }
  if(t %in% 2005:2012){
    all_sponsors[all_sponsors$LES_sponsor == 'robert williams',]$first_name <-  "bob" # Dist = Merrimack 11, see: http://www.gencourt.state.nh.us/legislation/2005/HB0169.html
  }
  if(t %in% 2007:2008){
    all_sponsors[all_sponsors$LES_sponsor == 'william chase',]$first_name <-  "bill" # Bill Chase in Klarner
  }
  if(t %in% 2007:2010){
    all_sponsors[all_sponsors$LES_sponsor == 'scott merrick',]$first_name <-  "d" # D. Scott Merrick in Klarner
    all_sponsors[all_sponsors$LES_sponsor == 'trinka russell',]$first_name <-  "kathleen" # Kathleen 'Trinka' Russell in Klarner
  }
  if(t_yrs %in% c("2009_2010", "2011_2012")){
    all_sponsors[all_sponsors$LES_sponsor == 'jeffrey st. cyr',]$last_name <-  "saintcyr"
  }
  if(t_yrs %in% c("2009_2010", "2011_2012", "2013_2014")){
    all_sponsors[all_sponsors$LES_sponsor == 'beatriz pastor',]$last_name <-  "pastorbodmer" 
  }
  if(t_yrs == "2011_2012"){
    all_sponsors[all_sponsors$LES_sponsor == "william o'connor",]$first_name <-  "bill" 
  }
  if(t >= 2011 & t <= 2018){
    all_sponsors[all_sponsors$LES_sponsor == 'steven smith',]$first_name <-  "stephen"
  }
  if(t_yrs == "2013_2014"){
    all_sponsors[all_sponsors$LES_sponsor == "kevin st.james",]$last_name <-  "saintjames" 
  }
  if(t_yrs %in% c("2013_2014", "2017_2018")){
    ## In chamber in 2015_2016 but doesn't sponsor bills so added later
    all_sponsors[all_sponsors$LES_sponsor == 'chip rice',]$first_name <-  "harold"
  }
  
  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  H_elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  S_elec_year <- H_elec_year ## 2-year terms

  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% H_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% S_elec_year:(S_elec_year + sen_term_length - 1) | (year == S_elec_year + sen_term_length & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz, middle) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)
  
  ### Need to fix for multiparty.... = Some candidates are both Dems and Reps
  ### Klarner has some of these coded but not all --- b = both
  klarner_sub <- klarner_sub %>%
    group_by(candid) %>%
    mutate(party = paste(party, collapse = '-'),
           party = ifelse(grepl('-', party), 'republicananddemocrat', party),
           partyz = paste(partyz, collapse = '-'),
           partyz = ifelse(grepl('d-r|r-d', partyz), 'b', partyz)) %>%
    distinct()
  
  ############################################################
  ############## Match Sponsors Names to Klarner Data
  ############################################################
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), '')))
  klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]+')))
  
  ### Fix Match Names
  if(t_yrs %in% c("1991_1992", "1993_1994")){
    all_sponsors[all_sponsors$LES_sponsor == "c. william johnson",]$match_name <- "johnson, c. william 1"
    klarner_sub[klarner_sub$cand == "johnson, c. william 1",]$match_name <- "johnson, c. william 1"
  }
  if(t_yrs == "1993_1994"){
    klarner_sub[klarner_sub$cand == "johnson, c. william 2",]$match_name <- "johnson, bill"
  }
  if(t_yrs %in% c("2001_2002", "2003_2004") ){
    klarner_sub[klarner_sub$cand == "gilbert",]$match_name <- "gilbert, jeffrey"
  }
  if(t_yrs %in% c("2005_2006") ){
    all_sponsors[all_sponsors$LES_sponsor == "william chase",]$match_name <- "chase, bill"
  }
  if(t_yrs %in% c("2007_2008", "2009_2010") ){
    all_sponsors[all_sponsors$LES_sponsor == "c. pennington brown",]$match_name <- "brown, penn"
  }
  if(t_yrs == "2017_2018"){
    all_sponsors[all_sponsors$LES_sponsor == "john j. o'connor",]$match_name <-  "oconnor, john j." 
    all_sponsors[all_sponsors$LES_sponsor == "john t. o'connor",]$match_name <-  "oconnor, john t."
    klarner_sub[klarner_sub$cand == "oconnor, john j.",]$match_name <- "oconnor, john j."
    klarner_sub[klarner_sub$cand == "oconnor, john t.",]$match_name <- "oconnor, john t."
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
      ## Check First Initial -- FOR OH -- ONLY 2015+
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
  if(t_yrs == "1989_1990"){
    all_sponsors[all_sponsors$LES_sponsor == "j allen bennett" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == '1991_1992'){
    all_sponsors[all_sponsors$LES_sponsor == "john sytek" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
    all_sponsors[all_sponsors$LES_sponsor == "phyllis katsakiores" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "1999_2000"){
    all_sponsors[all_sponsors$LES_sponsor == "william kelley" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "2001_2002"){
    all_sponsors[all_sponsors$LES_sponsor == "peter sullivan" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
    #all_sponsors[all_sponsors$LES_sponsor == "p judith sullivan" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "2009_2010"){
    #all_sponsors[all_sponsors$LES_sponsor == "phyllis katsakiores" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "2015_2016"){
    all_sponsors[all_sponsors$LES_sponsor == "yvonne dean-bailey" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
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
  # if(t_yrs == "1997_1998"){
  #   km <- filter(km, !(cand == 'sweeney, patrick a.' & sen == 0) )
  #   km <- filter(km, cand != 'kucinich, dennis j.')
  # }
  
  #### Add Candidates who WON but Sponsored NO BILLS into Data
  if(nrow(km) > 0){
    cat(glue("-----> {as.numeric(substring(t_yrs, 1, 4)) - 1} KLARNER CANDIDATES MISSING MATCHES IN DATA ~~> ADDING INTO LIST OF LEGISLATORS: \n . "))
    select(km, year, sab, sen, ddez, etype, deter, cand, candid, partyz, match_name) %>% 
      filter(sen == 1) %>% # ONLY PRINTING SENATORS AS WAY TOO MANY HOUSE MEMBERS --> ASSUMING ALL IN CHAMBER
      as.data.frame() %>% 
      print()
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
  
  #####################################################
  ############### Estimate Scores + Add in Relatd Variables
  #########################################################
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
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES) # 
rm(t, terms, klarner_gs, m_sub, parsed_names, match_name2) # 
rm(nonspon, unique_cospon, commem_bills)

########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by ZZZZZZZ -- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
## NOT VALIDATING/PRINTING ALL HOUSE MISSING -- ASSUMING IN CHAMBER GIVEN 400 Members
########################################################################################
## Digital Journals: https://www.library.unh.edu/search/digital/%2A%3A%2A?page=8&f%5B0%5D=category%3ANH%20State%20Publications%2A
## Web-based Journals Starting in 1997: http://gencourt.state.nh.us/house/caljourns/default.aspx
## ---> Includes elected but not sworn in!
## Senate Journals (1999+): http://gencourt.state.nh.us/Senate/calendars_journals/1999.html
#############################################


##### TO DROP:m----> Need to do this for all 1997+
# 1991_1992: 
# -- Elsie Vartanian -- Took federal job, never sowrn in (via Journal)
# -- Grafton County -- No reps District 2 or 10
# 1995_1996:
# --- District 7, Cheshire County, 1 elected but not sworn in
# --- District 9, Grafton County --- Elected but not sworn in... (1 person)
# --- DISTRICT 11, Hillsborough County, 1 Seat, vacant
# --- District 34, Hillsborough County, 1 elected but not sworn in, NOT Andrews or Taylor
# --- District 36, Hillsborough County, 1 seat, elected but not sworn in
# --- District 40, Hillsborough County, 1 Vacant seat, NOT Lionel W. Johnson or Leo P. Pepino
# --- District 47, Hillsborough County, 1 elected but not sworn in, NOT Pappas or Turgeon
# --- District 14 & 19, Merrimack County, 1 elected but not sworn in
# --- District 11, Rockingham County, 1 elected but not sworn in
# --- District 12, Rockingham County, 1 elected but not sworn in, NOT BISHOP or DOLAN
# --- District 20, Rockingham County, 2 elected but not sworn in, NOTHAWKINS or MAGOON or TUFTS
# --- District 29, Rockingham County, 1 elected but not sworn in, NOT ATTAR, BOUCHER, CARSON, DUNHAM, HUTCHINSON, PACKARD
# --- District 6, Strafford County, 1 elected but not sworn in, NOT DeChane
# --- District 17, Strafford County, 1 elected but not sworn in, NOT BROWN
# --- District 6, Sullivan County, 1 elected but not sworn in

##########################################

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1989_1990 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 4 bill(s) without a sponsor
# session chamber   N AIC ABC PASS LAW
# 1    1989       H 652 649 650  356 161
# 2    1989       S 204 204 203  152  39
# 3    1990       H 551 551 551  271 216
# 4    1990       S 112 112 112   44  34
# Won Special ~ HOUSE:
# -- gregory hanselman
# -- ralph shackett
### ELECTED but Not Seated?
# --> None reported in 1989 Journal

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1991_1992 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 2 bill(s) without a sponsor
# session chamber   N AIC ABC PASS LAW
# 1    1991       H 590 590 590  338 289
# 2    1991       S 203 202 144  126  95
# 3    1992       H 536 533 532  236 197
# 4    1992       S 199 198 157  135  89
### (Assume) Won Special ~ HOUSE:
# -- karen carpenter
# -- phyllis katsakiores ---- ?????
### NAME Correction
# -- alice calvert == alice s. ziegra --> through 1997_1998 term -- https://www.laconiadailysun.com/news/local/alton-s-holway-left-indelible-mark-on-sexual-violence-mores/article_79c7b279-3ea5-536e-bfdb-361ab17ba6a6.html


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1993_1994 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 8 bill(s) without a sponsor
#   session chamber   N AIC ABC PASS LAW
# 1    1993       H 483 483 483  263 227
# 2    1993       S 213 212 170  159 128
# 3    1994       H 601 600 600  315 249
# 4    1994       S 347 345 278  254 159
### Won SPECIAL ~ HOUSE:
# -- thomas stewart
### MISCODED as HOUSE:
# -- John Barnes, Jr. ---> Recoded 4 bills (+ more in subsequent sections)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1996 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 17 bill(s) without a sponsor
# session chamber   N AIC ABC PASS LAW
# 1    1995       H 470 469 469  267 224
# 2    1995       S 146 146 112   97  76
# 3    1996       H 626 625 625  265 213
# 4    1996       S 197 197 153  132  81
### Won Special?
# -- Lawrence Guaraldi
# -- Lawrence Guay
# -- Naida Kaen

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 9 bill(s) without a sponsor
#   session chamber   N AIC ABC PASS LAW
# 1    1997       H 559 559 558  280 253
# 2    1997       S 180 180 137  121  87
# 3    1998       H 732 731 731  311 261
# 4    1998       S 246 246 201  175 121
### WON SPECIAL - HOUSE
# -- christine konys
# -- frank sapareto
# -- larry cossette

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 8 bill(s) without a sponsor
#   session chamber   N AIC ABC PASS LAW
# 1    1999       H 507 506 507  263  51
# 2    1999       S 164 164 160   23  23
# 3    2000       H 634 633 633  251   1
# 4    2000       S 230 230 229    0   0
### WON SPECIAL ~ HOUSE
# -- john gallus
### Won Special, Name Duplicates:
# -- william kelley
### Name Fix:
# Amy Robb == Amy Robbtheroux
# Mary Lou Flayhan == Mary Lou Nowe --- http://appealslawyer.net/do/briefs/Mary_Lou_Flayhan_noa.pdf
# P Judith SUllivan  --- took office March 16, 1999: http://gencourt.state.nh.us/house/caljourns/journals/1999/houjou29.htm
# --> For 2000, she appears to be miscoded as "sullivan, henry p." in next term

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 11 bill(s) without a sponsor
#   session chamber   N AIC ABC PASS LAW
# 1    2001       H 496 495 496  238 208
# 2    2001       S 125 125 125   98  83
# 3    2002       H 542 542 542  266 190
# 4    2002       S 225 224 224  153  88
### WON SPECIAL - HOUSE
# -- david gleneck
# -- william johnson (d, belknao) -- He's seated early in 2001 but no record of him winning
# -- peter sullivan (name duplicated, won't print)
# -- p judith sullivan (name duplicated, won't print) (see above)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 14 bill(s) without a sponsor
#   session chamber   N AIC ABC PASS LAW
# 1    2003       H 611 611 611  276 181
# 2    2003       S 178 178 176  126  77
# 3    2004       H 450 450 450  164 125
# 4    2004       S 258 258 258  181 124
#### Name Issues:
# -- Two Charles LaFlammes in Klarner Data... Hillsborough 61 is actually paul laflamme -- Editing at top


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 10 bill(s) without a sponsor
#   session chamber   N AIC ABC PASS LAW
# 1    2005       H 539 538 351  255 186
# 2    2005       S 188 188 188  128 100
# 3    2006       H 788 772 401  326 238
# 4    2006       S 212 212 210  143  81
#### WON SPECIAL ~ HOUSE:
# -- gilman shattuck 
# -- jean jeudy
# -- joe osgood (first name actually phillip)
# -- larry brown
#### NAME FIX
# -- Robert Williams == Bob Williams, Merrimack 11 (through 2012)
# -- william chase == Bill Chase

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 15 bill(s) without a sponsor
# session chamber   N AIC ABC PASS LAW
# 1    2007       H 644 644 562  254 254
# 2    2007       S 197 197 195  160 124
# 3    2008       H 710 709 703  226 226
# 4    2008       S 289 289 289  211 156
### WON SPECIAL ~ HOUSE:
# -- james webber (jim)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 15 bill(s) without a sponsor
#   session chamber   N AIC ABC PASS LAW
# 1    2009       H 528 528 528  210 210
# 2    2009       S 171 171 170  131 114
# 3    2010       H 691 691 691  238 238
# 4    2010       S 245 245 245  180 139
### WON SPECIAL ~ HOUSE:
# -- andrew white
# -- kenneth weyler (or Klarner outcome wrong)
# -- marilinda garcia (or klarner outcome wrong)
### WON SPECIAL ~ SENATE:
# jeb bradley (== bradley, jeb 3 in Klarner)
### Name Fix:
# -- phyllis katsakiores ---> Klarner spelled wrong

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 16 bill(s) without a sponsor
#   session chamber   N AIC ABC PASS LAW
# 1    2011       H 456 455 455  174 174
# 2    2011       S 156 156 156  122  96
# 3    2012       H 752 750 750  174 174
# 4    2012       S 241 241 241  166 110
#### Won Special ~ HOUSE
# -- daler, jennifer (https://www.ledgertranscript.com/Archives/2015/12/teDaler-ml-121515)
# -- peter leishman
# -- robert perry
#### Klarner Error in Senate:
# -- gallus, john --> won reelection; Klarner has dorthy solomon as having won
#### Name Fixes:
# -- Marshall Quandt == Lee Quandt --> https://www.seacoastonline.com/article/20081107/NEWS/811070326


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 13 bill(s) without a sponsor
#   session chamber   N AIC ABC PASS LAW
# 1    2013       H 442 441 441  162 162
# 2    2013       S 164 164 164  128 114
# 3    2014       H 621 621 621  191 191
# 4    2014       S 252 252 252  179 138
### WON SPECIAL ~ HOUSE:
# -- mary heath
# -- william o'neil
#### NAME FIXES:
# -- chip rice == 'rice, harold l.' -- https://votesmart.org/candidate/biography/109922/chip-rice

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 51 bill(s) without a sponsor
#   session chamber   N AIC ABC PASS LAW
# 1    2015       H 456 456 456  156 156
# 2    2015       S 202 202 202  147 117
# 3    2016       H 682 679 679  172 172
# 4    2016       S 316 315 315  212 154
### WON SPECIAL ~ HOUSE:
# -- dennis green
# -- rio tilton
# -- yvonne dean-bailey --> Name duplicated, won't print


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 78 bill(s) without a sponsor
#   session chamber   N AIC ABC PASS LAW
# 1    2017       H 441 440 439  156 156
# 2    2017       S 197 197 197  125 101
# 3    2018       H 681 679 679  201 201
# 4    2018       S 337 336 336  249 174
### WON SPECIAL ~ HOUSE:
# -- casey conley
# -- charlie st. clair
# -- edith desmarais
# -- kari lerner
# -- kevin cavanaugh
# -- kristina schultz
# -- mark mclean
# -- vincent paul migliore
### Klarner House results wrong (recounts for both, see: https://sos.nh.gov/2016RepGen.aspx?id=8589964160):
# -- elizabeth ferreira 
# -- lisa freeman


# filter(klarner, grepl("migliore", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid, partyz) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 28 & sen == 1) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(bills, LES_sponsor == "joseph delahunty" & substring(bill_id, 1, 1) == "H") %>% select(bill_url)


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
missing <- missing[missing != "sullivan, p"]
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
### P Judith Sullivan --- Miscoded as Henry SUllivan in final term; adjusted above
LES[LES$data_name %in% "p judith sullivan",]$sponsor <- "sullivan, p. judith"

### ****Still missing***** ---> Rest are not in Klarner or 2017-2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[14]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term)
# filter(klarner, grepl("migl", cand) ) %>% select(cand, year, etype, sen, outcome, candid) # %>%  View()
# rm(name, still_missing)

### Fix Missing
name_matches <- data.frame(LES_name = 'hanselman, gregory', k_name = 'hanselman, greg')
#name_matches <- add_row(name_matches, LES_name = 'carpenter, karen', k_name = 'zzzzzzzz')
#name_matches <- add_row(name_matches, LES_name = 'stewart, thomas', k_name = 'zzzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'guaraldi, lawrence', k_name = 'guaraldi, larry')
name_matches <- add_row(name_matches, LES_name = 'konys, christine', k_name = 'konys, chris')
name_matches <- add_row(name_matches, LES_name = 'johnson, william', k_name = 'johnson, william g. (bill)')
name_matches <- add_row(name_matches, LES_name = 'osgood, joe', k_name = 'osgood, philip joe')
name_matches <- add_row(name_matches, LES_name = 'webber, james', k_name = 'webber, jim')
name_matches <- add_row(name_matches, LES_name = 'white, andrew', k_name = 'white, andy')
name_matches <- add_row(name_matches, LES_name = 'bradley, jeb', k_name = 'bradley, jeb 3')
name_matches <- add_row(name_matches, LES_name = "o'neil, william", k_name = 'oneil, william j.')
name_matches <- add_row(name_matches, LES_name = 'dean-bailey, yvonne', k_name = 'deanbailey, yvonne')
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
rm(check_dup, k_sub, exact, missing, name_sub)

################################################
################# NEW HAMPSHIRE NAME STANDARDIZATIONS TO DO
###################################################

### Alice Ziegra == Alice (Ziegra) Calvert??????
LES[LES$sponsor == "ziegra, alice s.",]$sponsor <- "calvert, alice ziegra"

### Harold Burns == Harold W. Burns
LES[LES$sponsor == "burns, harold",]$sponsor <- "burns, harold w."

### Sara 'Sally' Kelly == Sally Kelly
LES[LES$sponsor %in% c("kelly, sara (sally)", "kelly, sally"),]$sponsor <- "kelly, sara"

### Robert R Cushing Jr (1997-1998) == Robert Renny Cushing (2008+) -- 
LES[LES$sponsor %in% c("cushing, robert r. jr.", "cushing, robert renny"),]$sponsor <- "cushing, robert r. jr."

### Pappas == "Marc Pappas"
LES[LES$sponsor == "pappas",]$sponsor <- "pappas, marc"

### Gilbert == Jeffrey Gilbert (See data name)
LES[LES$sponsor == "gilbert",]$sponsor <- "gilbert, jeffrey"


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

###### Check Missing
# filter(LES, is.na(party)) %>% select(sponsor, term, klarner_id, district, party, exper)
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

### Fix Error
LES[LES$sponsor == "sullivan, p. judith",]$party <- 'r'
LES[LES$sponsor == "sullivan, p. judith",]$district <- 2

### Manually Fix Those Not in Klarner
LES[LES$sponsor == "carpenter, karen",]$party <- 'r' ## From sponsor page -- mus thave won a special and not run again
LES[LES$sponsor == "carpenter, karen",]$district <- 10 # Hillsborough
LES[LES$sponsor == "carpenter, karen",]$exper <- 'none'

LES[LES$sponsor == "stewart, thomas",]$party <- 'd'
LES[LES$sponsor == "stewart, thomas",]$district <- 41 # Hillsborough
LES[LES$sponsor == "stewart, thomas",]$exper <- 'none'

#### *** If any run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "migliore, vincent",]$party <- 'r'
LES[LES$sponsor == "desmarais, edith",]$party <- 'd'
LES[LES$sponsor == "schultz, kristina",]$party <- 'd'
LES[LES$sponsor == "cavanaugh, kevin",]$party <- 'd'
LES[LES$sponsor == "conley, casey",]$party <- 'd'
LES[LES$sponsor == "lerner, kari",]$party <- 'd'
LES[LES$sponsor == "st. clair, charlie",]$party <- 'd'

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

#### Doubling the Senate Rows + Adding back in for > 2-year term states (Issues here for staggered states)
# senate <- filter(hf_data, chamber == "Senate")
# senate$year <- senate$year + 2
# senate$term <- paste0(senate$year + 1, "_", senate$year + 2)
# senate$MajorityMember <- NA 
# hf_data <- bind_rows(hf_data, senate) %>% arrange(year, chamber)
# rm(senate)

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
      ideo_match <- filter(ideo_match, match_name == str_extract(LES[i,]$sponsor, "^[^,]+, [a-z]+") )    
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
  filter(grepl("----", name_matches)) %>%
  as.data.frame()

#### FIX MISMATCHES
mismatches <- c('adams, carls s.', 'barry, william m.', 'brown, jeffrey m.', 'brown, lewis w.',
                'brown, penn', 'brown, patricia berry', 'clark, martha fuller',
                'fraser, leo w.', 'graham, robert v., jr.', 'hall, douglas e.', 'johnson, c. william 1',
                'johnson, c. william 2', 'kelly, michael', 'king, frank p.', 'miller, don j.', 'miller, jeffrey c.',
                'pierce, david a.', 'smith, gerald r.', 'smith, leonard a.', 'thomas, john', 'walsh, robert r.',
                'ward, kathleen', "williams, bill", "williams, bob 2", 'wright, david b.',
                "bean, philip webb", 'boucher, laurent j.', 'boucher, lionel r.', 'cole, kenneth a.',
                "dube, ellen c.", 'hatch, william h.', 'johnson, william a.', 'oconnor, john j.',
                "peters, kenneth p.", 'peters, stanley w.', 'richards, beth', 'toomey, daniel',
                'willis, brenda', 'robinson, ellenann')
### *** --> Bradley, Jeb 1 and 3 are matched to single "Bradley, Jeb" observation
LES[LES$sponsor %in% mismatches, c('SM_name', 'SM_party', 'np_score')] <- NA
rm(mismatches)

### MANUAL FIXES 
# ---> NO IDEO DATA Until 1996
# filter(LES, is.na(np_score)) %>% filter( !(term %in% c('1989_1990', '1991_1992', '1993_1994','2017_2018')) ) %>% group_by(sponsor, party) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame() 
# filter(ideo, grepl('ahl', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'ahlgren, chris', SM_name = 'Ahlgren, Christopher') 
#name_matches <- add_row(name_matches, LES_name = 'alciere, tom', SM_name = 'zzzzzzzzz') 
#name_matches <- add_row(name_matches, LES_name = 'allison, james', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'asselin, robert paul', SM_name = 'Asselin') 
#name_matches <- add_row(name_matches, LES_name = 'barberia, richard a.', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'battles, marjorie', SM_name = 'Battles-Peirce, Marjorie') 
#name_matches <- add_row(name_matches, LES_name = 'benn, thomas', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'bergeron, normand r.', SM_name = 'Bergeron') 
name_matches <- add_row(name_matches, LES_name = 'bradley, paula', SM_name = 'Bradley, Paula') 
name_matches <- add_row(name_matches, LES_name = 'bradley, paula e.', SM_name = 'Bradley, Paula E.') 
name_matches <- add_row(name_matches, LES_name = 'brown, penn', SM_name = 'Brown, C. Pennington') 
name_matches <- add_row(name_matches, LES_name = 'buco, tom', SM_name = 'Buco, Thomas L') 
name_matches <- add_row(name_matches, LES_name = 'chabot, bob', SM_name = 'Chabot, Robert') 
name_matches <- add_row(name_matches, LES_name = 'champagne, norma greer', SM_name = 'Greer Champagne, Norma') 
name_matches <- add_row(name_matches, LES_name = 'chase, bill', SM_name = 'Chase, William') 
name_matches <- add_row(name_matches, LES_name = 'clark, martha fuller', SM_name = 'Fuller Clark, Martha') 
#name_matches <- add_row(name_matches, LES_name = 'cloutier, catherine a.', SM_name = 'zzzzzzzzz') 
#name_matches <- add_row(name_matches, LES_name = 'cote, charles h.', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'coughlin, anne', SM_name = 'Coughlin') 
#name_matches <- add_row(name_matches, LES_name = 'courchesne, judy', SM_name = 'zzzzzzzzz') 
#name_matches <- add_row(name_matches, LES_name = 'cox, dave', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'danais, richard', SM_name = 'Danias') # != Romeo
#name_matches <- add_row(name_matches, LES_name = 'desroches, michael r.', SM_name = 'zzzzzzzzz') 
#name_matches <- add_row(name_matches, LES_name = 'dobson, brian f.', SM_name = 'zzzzzzzzz') 
#name_matches <- add_row(name_matches, LES_name = 'dodge, emma m.', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'dumaine, dan', SM_name = 'Dumaine, Dudley D')  # = Dudley
#name_matches <- add_row(name_matches, LES_name = 'dykstra, leona', SM_name = 'zzzzzzzzz') 
#name_matches <- add_row(name_matches, LES_name = 'flanagan, jack b.', SM_name = 'zzzzzzzzz') ### One of the natalies should be Jack?
name_matches <- add_row(name_matches, LES_name = 'fraser, leo w.', SM_name = 'Fraser, Leo Jr.') 
name_matches <- add_row(name_matches, LES_name = 'garrish, linda l.', SM_name = 'Garrish Thomas, Linda') 
name_matches <- add_row(name_matches, LES_name = 'gordon, ned', SM_name = 'Gordon, Edward') 
name_matches <- add_row(name_matches, LES_name = 'gorman, donald w.', SM_name = 'Gorman') 
#name_matches <- add_row(name_matches, LES_name = 'gray, lawrence j.', SM_name = 'zzzzzzzzz') 
#name_matches <- add_row(name_matches, LES_name = 'greenleaf, ronald s. jr.', SM_name = 'zzzzzzzzz') 
#name_matches <- add_row(name_matches, LES_name = 'hackett, catherine', SM_name = 'zzzzzzzzz') 
# name_matches <- add_row(name_matches, LES_name = 'hambrick, patricia', SM_name = 'zzzzzzzzz') 
# name_matches <- add_row(name_matches, LES_name = 'hanlon, mark d.', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'hassan, maggie wood', SM_name = 'Wood Hassan, Margaret') 
name_matches <- add_row(name_matches, LES_name = 'hawkins, robert s. 1', SM_name = 'Hawkins') 
name_matches <- add_row(name_matches, LES_name = 'hildebrandtwarren, nancy', SM_name = 'Warren, Nancy') 
name_matches <- add_row(name_matches, LES_name = 'hoell, j. r.', SM_name = 'Hoell, JR') 
name_matches <- add_row(name_matches, LES_name = 'holden, carol h.', SM_name = 'Holden') 
# name_matches <- add_row(name_matches, LES_name = 'holmes, mary', SM_name = 'zzzzzzzzz') 
# name_matches <- add_row(name_matches, LES_name = 'hower, ann', SM_name = 'zzzzzzzzz') 
#name_matches <- add_row(name_matches, LES_name = 'huxley, robert', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'kelly, michael', SM_name = 'Kelly') 
name_matches <- add_row(name_matches, LES_name = 'kingsbury, h. thayer', SM_name = 'Kingsbury') 
name_matches <- add_row(name_matches, LES_name = 'ladd, roderick (rick)', SM_name = 'Ladd Jr, Roderick M') 
# name_matches <- add_row(name_matches, LES_name = 'lambert, bernard j.', SM_name = 'zzzzzzzzz') 
# name_matches <- add_row(name_matches, LES_name = 'langone, phyllis', SM_name = 'zzzzzzzzz') 
# name_matches <- add_row(name_matches, LES_name = 'laughlin, j. francis', SM_name = 'zzzzzzzzz') 
# name_matches <- add_row(name_matches, LES_name = 'laughton, stacie marie', SM_name = 'zzzzzzzzz') 
# name_matches <- add_row(name_matches, LES_name = 'legacy, earl g.', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'letourneau, bob', SM_name = 'Letourneau, Robert') 
name_matches <- add_row(name_matches, LES_name = 'little, jerry', SM_name = 'Little, Gerald') 
name_matches <- add_row(name_matches, LES_name = 'little, mike', SM_name = 'Little') 
# name_matches <- add_row(name_matches, LES_name = 'loder, suzanne k.', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'lovejoy, george a.', SM_name = 'Lovejoy') 
name_matches <- add_row(name_matches, LES_name = 'martel, andy', SM_name = 'Martel, André') 
name_matches <- add_row(name_matches, LES_name = 'martin, jim', SM_name = 'Martin, James') 
# name_matches <- add_row(name_matches, LES_name = 'mcclarin, jim', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'mcguire, bob', SM_name = 'McGuire, Robert') 
name_matches <- add_row(name_matches, LES_name = 'mcmahon, donald francis', SM_name = 'McMahon') 
# name_matches <- add_row(name_matches, LES_name = 'michelin, joseph f.', SM_name = 'zzzzzzzzz') 
# name_matches <- add_row(name_matches, LES_name = 'moncrief, keith', SM_name = 'zzzzzzzzz') 
# name_matches <- add_row(name_matches, LES_name = 'moore, josh', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'morse, chuck', SM_name = 'Morse, Charles W.') 
# name_matches <- add_row(name_matches, LES_name = 'nehring, william h.', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'nowe, mary lou', SM_name = 'Flayhan, Mary Lou') 
#name_matches <- add_row(name_matches, LES_name = 'oconnor, bill', SM_name = "O'Connor, William") # dates don't line up...
name_matches <- add_row(name_matches, LES_name = 'okeefe, patricia m.', SM_name = "O'Keefe, Patricia") 
name_matches <- add_row(name_matches, LES_name = 'okeefe, peter', SM_name = "O'Keefe, Peter") 
#name_matches <- add_row(name_matches, LES_name = 'oliver, bert', SM_name = 'zzzzzzzzz') 
#name_matches <- add_row(name_matches, LES_name = 'oneil, james m. (jim)', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'oneil, william j.', SM_name = "O'Neil, William J") # Two Records -- One has double space and period 
#name_matches <- add_row(name_matches, LES_name = 'orourke, joanne a.', SM_name = 'zzzzzzzzz') 
#name_matches <- add_row(name_matches, LES_name = 'packard, bonnie b.', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'perkins, lawrence b. (koko) jr.', SM_name = 'Perkins Jr, Lawrence B') 
name_matches <- add_row(name_matches, LES_name = 'peters, stanley w.', SM_name = 'Peters') 
#name_matches <- add_row(name_matches, LES_name = 'philbrook, paula l.', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'picconi, al', SM_name = 'Picconi,') 
name_matches <- add_row(name_matches, LES_name = 'quimby, charlotte houde', SM_name = 'Houde-Quimby, Charlotte') 
name_matches <- add_row(name_matches, LES_name = 'reynolds, charles d.', SM_name = 'Reynolds') 
name_matches <- add_row(name_matches, LES_name = 'richardson, barbara hull', SM_name = 'Hull Richardson, Barbara') 
# name_matches <- add_row(name_matches, LES_name = 'roberts, george b. jr.', SM_name = 'zzzzzzzzz') 
# name_matches <- add_row(name_matches, LES_name = 'roberts, kris edward', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'ross, james e.', SM_name = 'Ross') 
name_matches <- add_row(name_matches, LES_name = 'saintcyr, gerard', SM_name = 'St. Cyr, Gerard') 
name_matches <- add_row(name_matches, LES_name = 'saintcyr, jeffrey l.', SM_name = 'St. Cyr, Jeffrey') 
name_matches <- add_row(name_matches, LES_name = 'sainthilaire, paul e.', SM_name = 'St. Hilaire, Paul') 
# name_matches <- add_row(name_matches, LES_name = 'sallada, roland a.', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'scanlon, edward j.', SM_name = 'Scanlon') 
# name_matches <- add_row(name_matches, LES_name = 'senter, merilyn p.', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'shaw, randall f.', SM_name = 'Shaw') 
name_matches <- add_row(name_matches, LES_name = 'smith, donald h.', SM_name = 'Smith, Donald Jr.') 
name_matches <- add_row(name_matches, LES_name = 'smith, suzanne', SM_name = 'Smith, Suzanne J') # Smith, Suzanne == Smith, Suzanne J
#name_matches <- add_row(name_matches, LES_name = 'sweeney, dennis b.', SM_name = 'zzzzzzzzz') 
# name_matches <- add_row(name_matches, LES_name = 'tessimond, shane e.', SM_name = 'zzzzzzzzz') 
#name_matches <- add_row(name_matches, LES_name = 'thomas, john', SM_name = 'zzz') ### Collapsed with Douglas Thomas??? Doug only elected in 2014
# name_matches <- add_row(name_matches, LES_name = 'thomas, ricky', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'townsend, chuck', SM_name = 'Townsend, Charles') 
name_matches <- add_row(name_matches, LES_name = 'tucker, john h.', SM_name = 'Tucker') 
# name_matches <- add_row(name_matches, LES_name = 'twombly, jim', SM_name = 'zzzzzzzzz') ### Collapsed into two timothy twomblys
name_matches <- add_row(name_matches, LES_name = 'twombly, timothy', SM_name = 'Twombly, Timothy') 
# name_matches <- add_row(name_matches, LES_name = 'wadsworth, karen o.', SM_name = 'zzzzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'welch, donald j.', SM_name = 'Welch, Donald') 
name_matches <- add_row(name_matches, LES_name = 'wells, peter f. sr.', SM_name = 'Wells') 
name_matches <- add_row(name_matches, LES_name = 'white, jay t.', SM_name = 'White, Jay') 
name_matches <- add_row(name_matches, LES_name = 'white, john m.', SM_name = 'White, Jay T.') ### Mislabeld -- Must be John White -- Timings overlap
name_matches <- add_row(name_matches, LES_name = 'williams, bill', SM_name = 'Williams, William Jr.') 
name_matches <- add_row(name_matches, LES_name = 'williams, bob 2', SM_name = 'Williams, Robert W') 
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzzzz', SM_name = 'zzzzzzzzz') 
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzzzz', SM_name = 'zzzzzzzzz') 

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}


###############
#### Manual Edits (Needs more precision...)

### Two Ricahrd Eatons, 1 D, 1 R -- Unclear if Party Switch or Long Gap
LES[LES$sponsor == 'eaton, richard s.',]$SM_name <-  ideo[ideo$name == 'Eaton, Richard' & ideo$house2001 %in% 1,]$name
LES[LES$sponsor == 'eaton, richard s.',]$SM_party <- ideo[ideo$name == 'Eaton, Richard' & ideo$house2001 %in% 1,]$party
LES[LES$sponsor == 'eaton, richard s.',]$np_score <- ideo[ideo$name == 'Eaton, Richard' & ideo$house2001 %in% 1,]$np_score
LES[LES$sponsor == 'eaton, richard sutherland',]$SM_name <-  ideo[ideo$name == 'Eaton, Richard' & ideo$house2013 %in% 1,]$name
LES[LES$sponsor == 'eaton, richard sutherland',]$SM_party <- ideo[ideo$name == 'Eaton, Richard' & ideo$house2013 %in% 1,]$party
LES[LES$sponsor == 'eaton, richard sutherland',]$np_score <- ideo[ideo$name == 'Eaton, Richard' & ideo$house2013 %in% 1,]$np_score

### Two Natalie Flanagans, One like J. Flanagan
LES[LES$sponsor == 'flanagan, natalie s.',]$SM_name <-  ideo[ideo$name == 'Flanagan, Natalie' & ideo$house1997 %in% 1,]$name
LES[LES$sponsor == 'flanagan, natalie s.',]$SM_party <- ideo[ideo$name == 'Flanagan, Natalie' & ideo$house1997 %in% 1,]$party
LES[LES$sponsor == 'flanagan, natalie s.',]$np_score <- ideo[ideo$name == 'Flanagan, Natalie' & ideo$house1997 %in% 1,]$np_score

## Split evenly over 2 rows...
LES[LES$sponsor == 'woodburn, jeff',]$SM_name <-  ideo[ideo$name == 'Woodburn, Jeff' & ideo$senate2015 %in% 1,]$name
LES[LES$sponsor == 'woodburn, jeff',]$SM_party <- ideo[ideo$name == 'Woodburn, Jeff' & ideo$senate2015 %in% 1,]$party
LES[LES$sponsor == 'woodburn, jeff',]$np_score <- ideo[ideo$name == 'Woodburn, Jeff' & ideo$senate2015 %in% 1,]$np_score

## Sullivan 
LES[LES$sponsor == 'sullivan, p. judith',]$SM_name <-  ideo[ideo$name == 'Sullivan, P. Judith' & ideo$house2001 %in% 1,]$name
LES[LES$sponsor == 'sullivan, p. judith',]$SM_party <- ideo[ideo$name == 'Sullivan, P. Judith' & ideo$house2001 %in% 1,]$party
LES[LES$sponsor == 'sullivan, p. judith',]$np_score <- ideo[ideo$name == 'Sullivan, P. Judith' & ideo$house2001 %in% 1,]$np_score

########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########

# *** Steve Vaillancourt -- Ran as libertarian after losing dem primary, then switched to R
LES[LES$sponsor == 'vaillancourt, j. steve' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'Vaillancourt, Steve' & ideo$party == 'D',]$name
LES[LES$sponsor == 'vaillancourt, j. steve' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Vaillancourt, Steve' & ideo$party == 'D',]$party
LES[LES$sponsor == 'vaillancourt, j. steve' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Vaillancourt, Steve' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'vaillancourt, j. steve' & LES$party %in% c('r', "nonmaj"),]$SM_name <-  ideo[ideo$name == 'Vaillancourt, Steve' & ideo$party == 'R',]$name
LES[LES$sponsor == 'vaillancourt, j. steve' & LES$party %in% c('r', "nonmaj"),]$SM_party <- ideo[ideo$name == 'Vaillancourt, Steve' & ideo$party == 'R',]$party
LES[LES$sponsor == 'vaillancourt, j. steve' & LES$party %in% c('r', "nonmaj"),]$np_score <- ideo[ideo$name == 'Vaillancourt, Steve' & ideo$party == 'R',]$np_score

# *** Michael Downing -- Switched in 2006
LES[LES$sponsor == 'downing, michael w.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Downing, Michael' & ideo$party == 'D',]$name
LES[LES$sponsor == 'downing, michael w.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Downing, Michael' & ideo$party == 'D',]$party
LES[LES$sponsor == 'downing, michael w.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Downing, Michael' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'downing, michael w.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Downing, Michael' & ideo$party == 'R',]$name
LES[LES$sponsor == 'downing, michael w.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Downing, Michael' & ideo$party == 'R',]$party
LES[LES$sponsor == 'downing, michael w.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Downing, Michael' & ideo$party == 'R',]$np_score

# *** Sandra Keans -- Switched in 2008
LES[LES$sponsor == 'keans, sandra b.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Keans, Sandra' & ideo$party == 'D',]$name
LES[LES$sponsor == 'keans, sandra b.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Keans, Sandra' & ideo$party == 'D',]$party
LES[LES$sponsor == 'keans, sandra b.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Keans, Sandra' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'keans, sandra b.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Keans, Sandra' & ideo$party == 'R',]$name
LES[LES$sponsor == 'keans, sandra b.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Keans, Sandra' & ideo$party == 'R',]$party
LES[LES$sponsor == 'keans, sandra b.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Keans, Sandra' & ideo$party == 'R',]$np_score

# *** Jim Mackay -- Switched in 2010
LES[LES$sponsor == 'mackay, jim' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'MacKay, James' & ideo$party == 'D',]$name
LES[LES$sponsor == 'mackay, jim' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'MacKay, James' & ideo$party == 'D',]$party
LES[LES$sponsor == 'mackay, jim' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'MacKay, James' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'mackay, jim' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'MacKay, James' & ideo$party == 'R',]$name
LES[LES$sponsor == 'mackay, jim' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'MacKay, James' & ideo$party == 'R',]$party
LES[LES$sponsor == 'mackay, jim' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'MacKay, James' & ideo$party == 'R',]$np_score

# *** Peter Leishman -- Switched in 2004
LES[LES$sponsor == 'leishman, peter r.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Leishman, Peter R' & ideo$party == 'D',]$name
LES[LES$sponsor == 'leishman, peter r.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Leishman, Peter R' & ideo$party == 'D',]$party
LES[LES$sponsor == 'leishman, peter r.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Leishman, Peter R' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'leishman, peter r.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Leishman, Peter' & ideo$party == 'R',]$name
LES[LES$sponsor == 'leishman, peter r.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Leishman, Peter' & ideo$party == 'R',]$party
LES[LES$sponsor == 'leishman, peter r.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Leishman, Peter' & ideo$party == 'R',]$np_score

# *** Francis 'Frank' W. Davis -- Switched in 2002
LES[LES$sponsor == 'davis, francis w.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Davis, Frank W' & ideo$party == 'D',]$name
LES[LES$sponsor == 'davis, francis w.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Davis, Frank W' & ideo$party == 'D',]$party
LES[LES$sponsor == 'davis, francis w.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Davis, Frank W' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'davis, francis w.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Davis, Francis W.' & ideo$party == 'R',]$name
LES[LES$sponsor == 'davis, francis w.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Davis, Francis W.' & ideo$party == 'R',]$party
LES[LES$sponsor == 'davis, francis w.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Davis, Francis W.' & ideo$party == 'R',]$np_score

# *** Elizabeth Blanchard -- Switched in 2006
LES[LES$sponsor == 'blanchard, elizabeth d.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Blanchard, Elizabeth' & ideo$party == 'D',]$name
LES[LES$sponsor == 'blanchard, elizabeth d.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Blanchard, Elizabeth' & ideo$party == 'D',]$party
LES[LES$sponsor == 'blanchard, elizabeth d.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Blanchard, Elizabeth' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'blanchard, elizabeth d.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Blanchard, Elizabeth' & ideo$party == 'R',]$name
LES[LES$sponsor == 'blanchard, elizabeth d.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Blanchard, Elizabeth' & ideo$party == 'R',]$party
LES[LES$sponsor == 'blanchard, elizabeth d.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Blanchard, Elizabeth' & ideo$party == 'R',]$np_score

# *** Jane Kelley -- Switched in 2002, then back? -- Technically first switched in Jan 2001: https://www.seacoastonline.com/article/20010103/news/301039992?template=ampart
LES[LES$sponsor == 'kelley, jane' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Kelley, Jane' & ideo$party == 'D',]$name
LES[LES$sponsor == 'kelley, jane' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Kelley, Jane' & ideo$party == 'D',]$party
LES[LES$sponsor == 'kelley, jane' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Kelley, Jane' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'kelley, jane' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Kelley, Jane' & ideo$party == 'R',]$name
LES[LES$sponsor == 'kelley, jane' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Kelley, Jane' & ideo$party == 'R',]$party
LES[LES$sponsor == 'kelley, jane' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Kelley, Jane' & ideo$party == 'R',]$np_score

##### No Party Observations in SM Data
# Rudy Lessard -- Dem: 1992-1995, Rep: 1996+
# Stephanie Micklon -- 1 Early R Term Before SM Data
# Dana Hilliard -- Missing votes for his early (R) terms
# Naida Kaen -- Unclear if actually switched or just ran on both tickets early on
# Mark Fernald -- Odd switch in middle... No SM record
### Klarner or SM Party Wrong:
# -- James M. Johnson (SM: Johnson, James) listed as R when Klarner has him as D
# --> Seems like this should actually be William Johnson (D): See Belknap 4 -- http://gencourt.state.nh.us/house/caljourns/journals/2001/houjou2001_01.html
#### Klarner Party Wrong: Odd Mid-Tenure Switch
LES[LES$sponsor == "headd, james" & LES$term == "2009_2010", ]$party <- "r"
LES[LES$sponsor == "dallesandro, louis c." & LES$term == "2015_2016", ]$party <- "d"

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct() %>% arrange(sponsor)
### Elisabeth S Bardsley 1 = 1982 loser, same year as #2, weird 
LES[LES$sponsor == 'bardsley, elizabeth s. 2',]$sponsor <- 'bardsley, elizabeth s.'
### Jeb 1 and 3 are same person, gap in service -- https://en.wikipedia.org/wiki/Jeb_Bradley 
LES[LES$sponsor %in% c('bradley, jeb e. 1', 'bradley, jeb 3'),]$sponsor <- 'bradley, joseph e.'
### James Hogan 2 == 1990 loser, same year as james b hogan
LES[LES$sponsor == 'hogan, james b. 1',]$sponsor <- 'hogan, james b.'
### Jean Robert 1 and 2 have different initials
# LES[LES$sponsor == "jean, robert w. 1",]$sponsor <- "jean, robert w." # Lost 1988
LES[LES$sponsor == "jean, robert r. 2",]$sponsor <- "jean, robert r."
### C. William Johnson 1 == Clarence -- Lived in Bow, in Merrimack County: https://www.legacy.com/obituaries/name/c-william-johnson-obituary?pid=169484725
## --> what's weird is that hte NH electiosn site seems to consider them the same person, even though they ran simultaneously in 1992...
LES[LES$sponsor == 'johnson, c. william 1',]$sponsor <- 'johnson, clarence william'
LES[LES$sponsor == 'johnson, c. william 2',]$sponsor <- 'johnson, c. william'
### Gilman C. Shattuck 2 == 1996 loser, same year as #1 first ran
LES[LES$sponsor == 'shattuck, gilman c. 1',]$sponsor <- 'shattuck, gilman c.'
### Dennis P. Vachon 1 + 2 = same: https://ballotpedia.org/Dennis_Vachon -- Ran for senate inbetween multiple house terms
LES[LES$sponsor %in% c('vachon, dennis p. 1', "vachon, dennis p. 2"),]$sponsor <- 'vachon, dennis p.'
### No Individuals with Same Name... Weird
LES[LES$sponsor == 'hall, charles q. 4',]$sponsor <- 'hall, charles q.' # No Charles Q. Hall 1-3
LES[LES$sponsor == 'harris, joe 2',]$sponsor <- 'harris, joe' # No Joe Harris 1
LES[LES$sponsor == 'harris, sandra 1',]$sponsor <- 'harris, sandra c.' # No Sandra Harris 2+; also ran in 2012: https://www.sentinelsource.com/news/local/democrats-square-off-in-senate-race/article_ef111e78-9e53-5b28-bfd8-46b7788c6f6b.html
LES[LES$sponsor == 'hawkins, robert s. 1',]$sponsor <- 'hawkins, robert s.' # No 2+
LES[LES$sponsor == 'williams, bob 2',]$sponsor <- 'williams, bob' # Does not appear to = either robert williams (2002/2010 losers)
LES[LES$sponsor == 'williams, burton w. 5',]$sponsor <- 'williams, burton w.'
# LES[LES$sponsor == 'zzzzzzz',]$sponsor <- 'zzzzzzzz'

##### Drop Nicknames
# filter(LES, grepl('\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

#### Manual Fixes
## -- Paula Bradley == Paula E. Bradley --- NOTE: ID's still off
LES[grepl("bradley, paula", LES$sponsor),]$sponsor <- "bradley, paula e."
LES[LES$sponsor == 'mcdonoughwallace, alice t.',]$sponsor <- "mcdonough-wallace, alice t."
LES[LES$sponsor == 'shultis, betsy',]$sponsor <- "shultis, elizabeth"
LES[LES$sponsor == 'nowe, mary lou',]$sponsor <- "flayhan, mary lou nowe"
LES[LES$sponsor == 'spaulding, j.',]$sponsor <- "spaulding, jayne"
# LES[LES$sponsor == 'zzzzzz',]$sponsor <- "zzzzzz"



##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1989 -- 2018
### -- W Douglas Scamman was speaker in 1989 ---> R Control
### -- Harold Burns (d+r) speaker in 1991, but Caroline Gross (R) == Majority Leader
LES[as.numeric(substring(LES$term,1,4)) %in% c(1989:2006, 2011:2012, 2015:2018) & LES$chamber == 'House'  & LES$party %in% 'r',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2007:2010, 2013:2014, 2019:2020) & LES$chamber == 'House' & LES$party %in% 'd',]$in_majority <- 1

### Senate -- 1997 - 2020
### -- Clesson J. Blaisell died in August 1999; replaced by Beverly Hollingworth (D) -- http://gencourt.state.nh.us/Senate/calendars_journals/journals/1999/senorg.html
### -- Ballotpedia claims split control, but seems to be Dem Control (hollingworth elected comfortably)
LES[as.numeric(substring(LES$term,1,4)) %in% c(1989:1998, 2001:2006, 2011:2018) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1999:2000, 2007:2010, 2019:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1


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
ggplot2::ggplot(LES, aes(x = np_score, y = LES, color = SM_party)) + 
  geom_point() + 
  geom_smooth(formula = "y ~ x", method = 'lm', se = FALSE) + 
  facet_wrap(~ term) + scale_color_manual(values=c("dodgerblue2",  "red2", "gray"))

##### CHECK OUTLIERS
# -- Naida Kaen was originally an R -- http://gencourt.state.nh.us/house/caljourns/journals/1997/houjou1.htm
# -- Can't verify others...
# filter(LES, party == 'd' & np_score > .25 & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & np_score < -.25 & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ in_majority + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

