
################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** IOWA *** BY SESSION
##############################################################

##### FOR RE-SCRAPE
# (1) Update to Pull whether or not was a LSB Study Bill
# -----> All currently missing (I think?).. eg: https://www.legis.iowa.gov/legislation/billTracking/billHistory?ga=81&billName=HF102
# (2) RESCRAPE TO GET MISSING SPONSORS FROM BILL TEXT?: (e.g. https://www.legis.iowa.gov/legislation/BillBook?ga=80&ba=SF%202310)
# ---- Depending on scope of missingness, can fill in with Floor manager where necessary???

###########
#### QUESTIONS
# (1) What to do about bills that are introduced and then co-opted by a committee? See: https://www.legis.iowa.gov/legislation/billTracking/billHistory?ga=81&billName=SF2284

###################################
## (SPECIAL) SESSIONS:
## ---- Assembly does have special sessions, but included as data within broader term
## ---- Does not appear to distinguish special session bills via bill number
## MEMBER LISTS:
## ---- State Official Rosters: https://www.legis.iowa.gov/publications/otherResources/roster
## PROCESS/RULES:
## ---- Process: https://www.legis.iowa.gov/docs/publications/LP/696315.pdf
## Sponsorship/Authorship
## ---- TWO primary sponsors permitted -- BUT records are ordered, not alphabetized
## ---- Floor Managers play part of the role of sponsor (in other states) by opening/closing debate + guiding bill
## ----> See: https://isea.org/wp-content/uploads/2016/07/legislativeterminology.pdf
###########################
## NOTES:
## ** Using FLOOR MANAGER for INTRO CHAMBER as SPONSOR when bill is introduced BY COMMITTEE
## -- FROM PROCESS DOC: "After the committee completes work on the bill, the subcommittee’s chairperson usually becomes the bill’s floor manager."
## ~~~~~~~ BUT: DON'T HAVE FLOOR MANAGER FOR GA 76 - 79 
## *** Committee bills often introduced as study bills, then transition to real bills
## -----> problem is then we don't see the committee stage with the newfound bill -- e.g., https://www.legis.iowa.gov/legislation/billTracking/billHistory?ga=80&billName=SF2294
## -----> Added correction below for to  code as AIC = 1 if a related bill is HSB/SSB 

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

this_state <- 'IA'
min_year <-1995
max_year <- 2018
keep_types <- c('HF', 'SF')
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
term_nums <- gsub('.+Bill_Details_|.csv', '', bill_files)
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
# ** MISPELLINGS ** (ID will be wrong for all fixed observations)
klarner[klarner$cand == 'jatch, jack' & klarner$year == 2002,]$cand <- "hatch, jack"
klarner[klarner$cand == 'shickel, bill' & klarner$year == 2002,]$cand <- "schickel, bill"
# ** David Johnson switched districts, but is coded as two people
klarner[klarner$cand == 'johnson, dave' & klarner$year %in% 2011:2016,]$cand <- "johnson, david j."

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[6]

for(t in terms){
  
  ### Formulate 2-year terms -- Cover both regular and special sessions
  t_yrs <- as.character(glue('{t}_{t+1}'))
  ga_num <- term_nums[which(terms == t)]
  
  if(as.numeric(ga_num) < 80){
    cat("\n")
    cat("----> SKIPPING GA's BELOW 80 --- NEED TO FIGURE OUT WHAT TO DO ABOUT COMMITTEES ")
    next
  }
  
  ### Skip Previously Estimated
  if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
    print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
    next
  }
  
  ###### SESSION IN PROGRESS
  print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))
  
  ############### Read in data
  bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{ga_num}.csv")
  bills <- read.csv(bill_path)
  
  ### Clean Term/Session Variables
  bills <- bills %>%
    rename(term_dates = session_dates,
           bill_id = bill_number) %>% 
    mutate(term = t_yrs,
           session = t_yrs,
           ga_num = ga_num,
           bill_id = paste0(gsub(' .+', '', bill_id), str_pad(gsub('[A-Z]+ ', '', bill_id), 4, pad = "0" ))) %>%
    arrange(bill_id)
  
  ### Check for duplicates
  if(nrow(bills) != nrow(distinct(bills))){
    cat(" \n ~~~> DUPLICATE BILLS \n\n .")
    break
  }
  
  ############### Drop Resolutions, Messages, Communications, Reports
  bills <- mutate(bills, bill_type = str_trim(toupper(gsub('[0-9].+|[0-9]+', '', bill_id))))
  # table(bills$bill_type)
  bills <- filter(bills, bill_type %in% keep_types) 
  
  ##########################
  ####### Standardize Sponsors
  
  ### Fix First Initials Split Across Primary/Cosponsor Columns And Remove from Cosponsor Column
  split_first <- str_extract(bills$cosponsors, '^[A-Z]\\.; |^[A-Z]\\.$')
  split_first <- ifelse(is.na(split_first), '', paste0(', ', gsub('; ', '', split_first)))
  bills$primary_sponsor <- paste0(bills$primary_sponsor, split_first)
  bills$cosponsors <- gsub('^[A-Z]\\.; |^[A-Z]\\.$', '', bills$cosponsors)
  rm(split_first)
  
  ### Fix First Initials Within Cosponsor Column - First = Midline; Second = End of Line
  bills$cosponsors <- str_replace_all(bills$cosponsors, "; ([A-Z]\\.;)", ", \\1") ###//1 replaces with captured group
  bills$cosponsors <- str_replace_all(bills$cosponsors, "; ([A-Z]\\.)$",  ", \\1")
  
  # committees <- unique(bills$primary_sponsor[which(bills$primary_sponsor == toupper(bills$primary_sponsor) & bills$primary_sponsor != '')])
  bills$primary_sponsor <- tolower(bills$primary_sponsor)
  bills$primary_sponsor <- gsub('á', 'a', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('é', 'e', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('ó', 'o', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('í', 'i', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('ñ', 'n', bills$primary_sponsor)
  
  bills$cosponsors <- tolower(bills$cosponsors)
  bills$cosponsors <- gsub('á', 'a', bills$cosponsors)
  bills$cosponsors <- gsub('é', 'e', bills$cosponsors)
  bills$cosponsors <- gsub('ó', 'o', bills$cosponsors)
  bills$cosponsors <- gsub('í', 'i', bills$cosponsors)
  bills$cosponsors <- gsub('ñ', 'n', bills$cosponsors)
  
  bills$H_floor_manager <- tolower(bills$H_floor_manager)
  bills$H_floor_manager <- gsub('á', 'a', bills$H_floor_manager)
  bills$H_floor_manager <- gsub('é', 'e', bills$H_floor_manager)
  bills$H_floor_manager <- gsub('ó', 'o', bills$H_floor_manager)
  bills$H_floor_manager <- gsub('í', 'i', bills$H_floor_manager)
  bills$H_floor_manager <- gsub('ñ', 'n', bills$H_floor_manager)
  
  bills$S_floor_manager <- tolower(bills$S_floor_manager)
  bills$S_floor_manager <- gsub('á', 'a', bills$S_floor_manager)
  bills$S_floor_manager <- gsub('é', 'e', bills$S_floor_manager)
  bills$S_floor_manager <- gsub('ó', 'o', bills$S_floor_manager)
  bills$S_floor_manager <- gsub('í', 'i', bills$S_floor_manager)
  bills$S_floor_manager <- gsub('ñ', 'n', bills$S_floor_manager)
  
  ### Manual Fixes to Improve Matching
  if(t == 2003){
    # Unclear why incorrect but correct sponsor is listed on bill text and recorded sponsors don't exist
    bills[bills$primary_sponsor == 'babcock',]$primary_sponsor <- 'hahn' 
    bills[bills$bill_type == 'HF',]$cosponsors <- gsub('babcock', 'hahn', bills[bills$bill_type == 'HF',]$cosponsors)
    bills$primary_sponsor <- gsub('^young', 'hogg', bills$primary_sponsor)
    bills[bills$bill_type == 'HF',]$cosponsors <- gsub('young', 'hogg', bills[bills$bill_type == 'HF',]$cosponsors)
    # Hogg listed as Senate Floor Manager but is not in senate
    bills[bills$S_floor_manager == "hogg",]$S_floor_manager <- ''
    # Johnson Doesn't have prefix for Floor Manager Columns -- https://www.legis.iowa.gov/legislators/legislator?ga=80&personID=155
    bills[bills$S_floor_manager == "johnson",]$S_floor_manager <- "d. johnson"
    # All Coded as Taylor == T. Taylor
    bills[grepl("taylor", bills$primary_sponsor) & !grepl("taylor, d", bills$primary_sponsor),]$primary_sponsor <- gsub('taylor', 'taylor, t.', bills[grepl("taylor", bills$primary_sponsor) & !grepl("taylor, d", bills$primary_sponsor),]$primary_sponsor)
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('taylor$', 'taylor, t.', bills[bills$bill_type == "HF",]$cosponsors)
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('taylor;', 'taylor, t.;', bills[bills$bill_type == "HF",]$cosponsors)
    # David Miller (S) bills incorrectly labeled Helen Miller (H)
    bills[bills$bill_type == "SF",]$primary_sponsor <- gsub('miller, h\\.', 'miller', bills[bills$bill_type == "SF",]$primary_sponsor)
  }else if(t == 2005){
    bills[bills$bill_id == "HF2002",]$primary_sponsor <- 'raecker and kuhn' ### Fixing $s.sponsor
    ### Same corrections as 2003-2004
    bills$primary_sponsor <- gsub('^young', 'hogg', bills$primary_sponsor)
    bills[bills$bill_type == 'HF',]$cosponsors <- gsub('young', 'hogg', bills[bills$bill_type == 'HF',]$cosponsors)
    bills[grepl("taylor", bills$primary_sponsor) & !grepl("taylor, d", bills$primary_sponsor),]$primary_sponsor <- gsub('taylor', 'taylor, t.', bills[grepl("taylor", bills$primary_sponsor) & !grepl("taylor, d", bills$primary_sponsor),]$primary_sponsor)
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('taylor$', 'taylor, t.', bills[bills$bill_type == "HF",]$cosponsors)
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('taylor;', 'taylor, t.;', bills[bills$bill_type == "HF",]$cosponsors)
    bills[bills$S_floor_manager == "johnson",]$S_floor_manager <- "d. johnson"
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('smith, m.;', 'smith;', bills[bills$bill_type == "HF",]$cosponsors)
    bills[bills$bill_type == "SF",]$primary_sponsor <- gsub('miller, h\\.', 'miller', bills[bills$bill_type == "SF",]$primary_sponsor)
  }else if(t == 2007){
    bills[grepl("taylor", bills$primary_sponsor) & !grepl("taylor, d", bills$primary_sponsor),]$primary_sponsor <- gsub('taylor', 'taylor, t.', bills[grepl("taylor", bills$primary_sponsor) & !grepl("taylor, d", bills$primary_sponsor),]$primary_sponsor)
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('taylor$', 'taylor, t.', bills[bills$bill_type == "HF",]$cosponsors)
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('taylor;', 'taylor, t.;', bills[bills$bill_type == "HF",]$cosponsors)
    bills[bills$S_floor_manager == "johnson",]$S_floor_manager <- "d. johnson"
    # Only 1 smith in chamber at this point (mark)
    bills[bills$bill_type == "HF",]$primary_sponsor <- gsub('smith, m\\.', 'smith', bills[bills$bill_type == "HF",]$primary_sponsor)
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('smith, m\\.', 'smith', bills[bills$bill_type == "HF",]$cosponsors)
    ### Lensing is in the House
    bills[bills$bill_id == "SF0311", c("H_floor_manager", "S_floor_manager")] <- c("lensing", "")
    ### Only 1 Black in State Leg
    bills[bills$S_floor_manager == "black, d.",]$S_floor_manager <- "black"
  }else if(t == 2009){
    bills[grepl("taylor", bills$primary_sponsor) & !grepl("taylor, d", bills$primary_sponsor),]$primary_sponsor <- gsub('taylor', 'taylor, t.', bills[grepl("taylor", bills$primary_sponsor) & !grepl("taylor, d", bills$primary_sponsor),]$primary_sponsor)
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('taylor$', 'taylor, t.', bills[bills$bill_type == "HF",]$cosponsors)
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('taylor;', 'taylor, t.;', bills[bills$bill_type == "HF",]$cosponsors)
    bills[bills$S_floor_manager == "black, d.",]$S_floor_manager <- "black"
    bills[bills$H_floor_manager == "smith, m.",]$H_floor_manager <- "smith"
    ### User seems to be an error -- links lead to no individual and does not appear to be "huser" who is identified correctly..
    bills[bills$H_floor_manager == "user",]$H_floor_manager <- ''
  }else if(t == 2011){
    ## No match; presumably name error
    bills[bills$H_floor_manager == "user",]$H_floor_manager <- ''
    bills[bills$S_floor_manager == "johnson",]$S_floor_manager <- "d. johnson"
    bills[bills$S_floor_manager == "anderson, b.",]$S_floor_manager <- "anderson"
    ### brunkhorst not in chamber during this period. True FM not identified.
    bills[bills$S_floor_manager == "brunkhorst",]$S_floor_manager <- ""
  }else if(t == 2013){
    ## David Johnson sponsored the companion, but coded as sponsoring the house bill
    bills[bills$bill_id == "HF0010",]$primary_sponsor <- "jones" ### This gets corrected later to hess
    bills[bills$S_floor_manager == "johnson",]$S_floor_manager <- "d. johnson"
    ## Rich Taylor only Taylor in Senate
    bills[bills$S_floor_manager == "taylor, rich",]$S_floor_manager <- "taylor"
  }else if(t == 2015){
    bills[bills$S_floor_manager == "johnson",]$S_floor_manager <- "d. johnson"
    bills[bills$H_floor_manager == "olson",]$H_floor_manager <- "olson, r."
    ### Tom Moore electd Dec 22, 2015 --> Brian Moore coding changed from 'moore' to 'moore, b.'
    bills[bills$bill_type == "HF" & bills$primary_sponsor == "moore",]$primary_sponsor <- "moore, b."
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('moore$', 'moore, b.', bills[bills$bill_type == "HF",]$cosponsors)
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('moore;', 'moore, b.;', bills[bills$bill_type == "HF",]$cosponsors)
  }else if(t == 2017){
    ### Bills coded as both olson and olson, r. but only 1 olson in chamber (and none in senate either)
    bills[bills$primary_sponsor == "olson",]$primary_sponsor <- "olson, r."
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('olson$', 'olson, r.', bills[bills$bill_type == "HF",]$cosponsors)
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('olson;', 'olson, r.;', bills[bills$bill_type == "HF",]$cosponsors)
    ### Some Cosponsor bills coded as Moore, others Moore, T, but only 1 in chamber
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('moore$', 'moore, t.', bills[bills$bill_type == "HF",]$cosponsors)
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('moore;', 'moore, t.;', bills[bills$bill_type == "HF",]$cosponsors)
    ### Craig Johnson labeled both c. johnson and johnson, c.
    bills[bills$S_floor_manager == "johnson, c.",]$S_floor_manager <- "c. johnson"
    ### Gary Mohr listed as mohr, g. in floor_manager column
    bills[bills$H_floor_manager == "mohr, g.",]$H_floor_manager <- "mohr"
    ### Phil Miller won 2017 special -- All Helen Miller obs prior to this were just 'miller'
    bills[bills$primary_sponsor == "miller",]$primary_sponsor <- "miller, h."
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('miller$', 'miller, h.', bills[bills$bill_type == "HF",]$cosponsors)
    bills[bills$bill_type == "HF",]$cosponsors <- gsub('miller;', 'miller, h.;', bills[bills$bill_type == "HF",]$cosponsors)
  }
  
  ### LES Sponsor Var
  bills$LES_sponsor <- gsub(', [a-z][a-z].+| and.+', '', bills$primary_sponsor)
  # table(bills$LES_sponsor)
  
  #### Impute LES_Sponsor with Chamber Floor Manger IF Sponsor == COmmittee
  bills$chamber_floor_manager <- ifelse(bills$bill_type == "HF", bills$H_floor_manager, bills$S_floor_manager)
  if(as.numeric(ga_num) >= 80){
    #### COMMITTE BILLS
    bills$LES_sponsor <- ifelse(grepl("^committee", bills$LES_sponsor), bills$chamber_floor_manager,  bills$LES_sponsor)
    #### DEPARTMENT SPONSORED
    bills$LES_sponsor <- ifelse(grepl("^department", bills$LES_sponsor), bills$chamber_floor_manager,  bills$LES_sponsor)
  }
  
  ### Additional Fixes
  if(t == 2003){
    # Distinguishing between J. Van Fossen's -- Need to use bill textj/journal
    bills[bills$bill_id %in% c("HF0024", "HF0025", "HF0026", "HF0027", "HF0028", "HF0252", "HF2038", "HF2363", "HF2564"),]$LES_sponsor <- "vanfossen, james" ### James 'Jamie' K Van Fossen  
    bills[bills$bill_id %in% c("HF0188", "HF0250", "HF0318", "HF0542", "HF0028", "HF2151", "HF2282"),]$LES_sponsor <- "vanfossen, jim" ### James 'Jim' R Van Fossen
  }else if(t == 2005){
    # Distinguishing between J. Van Fossen's -- Need to use bill textj/journal
    bills[bills$bill_id %in% c("HF0002", "HF0024", "HF0026", "HF0102", "HF0134", "HF0866", "HF0878", "HF2639"),]$LES_sponsor <- "vanfossen, james"  ### James 'Jamie' K Van Fossen  
    bills[bills$bill_id %in% c("HF0228", "HF0286", "HF0673", "HF0745", "HF0755", "HF2254", "HF2286", "HF2456", "HF2510", "HF2523", "HF2546", "HF2573", "HF2668", "HF2785"),]$LES_sponsor <- "vanfossen, jim" ### James 'Jim' R Van Fossen
  }
  #filter(bills, LES_sponsor == "van fossen, j.")
  
  ### Fix Committee Bills PRIOR TO GA 80
  # if(ga_num == '76'){
  #   committees <- unique(bills[grepl('^committee', bills$LES_sponsor),]$LES_sponsor)
  # }
  
  ### Drop/Fix Any Remaining Committee Bills
  if(any(grepl('committee', bills$LES_sponsor))){
    cat('\n')
    print(glue("-----> Dropping {nrow(filter(bills, grepl('committee', LES_sponsor)))} bills sponsored by COMMITTEE"))
    bills <- filter(bills, !grepl('committee', LES_sponsor))     
  }
  if(any(grepl('department', bills$LES_sponsor))){
    cat('\n')
    print(glue("-----> Dropping {nrow(filter(bills, grepl('department', LES_sponsor)))} bills sponsored by a DEPARTMENT"))
    bills <- filter(bills, !grepl('department', LES_sponsor))     
  }
  
  ###### CHeck Missing Sponsors
  # filter(bills, LES_sponsor == "") %>% View()
  if(nrow(filter(bills, LES_sponsor == '')) > 0){
    cat('\n')
    print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
    bills <- filter(bills, !(LES_sponsor == ''))     
  }

  ###################
  ###### Merge in S&S Bills
  ###################
  # *** For IOWA: Special sessions folded in, bill numbers do not restart --> can merge on term
  SS_term <- SS_bills %>%
    filter(term == t_yrs) %>%
    distinct(term, bill_id, SS)
  
  if(nrow(SS_term) > 0){
    bills <- bills %>% left_join(SS_term, by = c("bill_id", "term")) %>% mutate(SS = ifelse(is.na(SS), 0, SS))
  }else{
    bills$SS <- 0
  }

  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id", "term"))
  
  ####################################################### 
  ############## Code Commemorative
  ###################################################### 
  
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session'))
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  #####################################################
  ############### Code Bill History
  ####################################################
  bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{ga_num}.csv")
  bill_hist <- read.csv(bill_hist_path)
  
  ######## Standardize BillHist Bill IDs
  bill_hist <- rename(bill_hist, bill_id = bill_number) 
  
  ### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
  bill_hist <- bill_hist %>%
    mutate(term = t_yrs,
           session = t_yrs,
           ga_num = ga_num,
           bill_id = paste0(gsub(' .+', '', bill_id), str_pad(gsub('[A-Z]+ ', '', bill_id), 4, pad = "0" )))
  
  ### Order by Order
  bill_hist <- arrange(bill_hist, term, bill_id, order)
  
  ### Code Chamber
  bill_hist$chamber <- str_extract(bill_hist$action, 'H.J. [0-9]+|S.J. [0-9]+|HCS\\.$|SCS\\.$')
  bill_hist$chamber <- substring(bill_hist$chamber, 1, 1)
  bill_hist <- bill_hist %>% group_by(bill_id) %>% fill(chamber) %>% ungroup()
  
  ### Standardize Chamber Variable
  bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate") # , "G" = "Governor", "CC" = "Conference"
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  aic_t <- c('subcommittee', '^committee report', '^recommended', 'committee amendment', 
             'placed on (ways and means|appropriations) calendar')
  # --> Comm Ammendment --> Even if record is only voted on in full house, its presence implies committee action
  # --> Need ^ for committee report otherwise will catch conference committee reports
  # --> NOBA != AIC: "NOBAs (Notes on Bills and Amendments) are prepared by the Fiscal Services Division of LSA""
  abc_t <- c('^placed on calendar', '^committee report', 'amendment.+(adopted|lost)', 'point of order', 'out of order')
  # 'placed on calendar' ---> Often done as "introduced, placed on calendar" --> Need ^
  pc_t <- c('^passed house', '^passed senate', 'enrolled', '^(house|senate) concurred')
  # Enrolled/concurred = check
  law_t <- c('signed by governor') 
  ## Override??? Can't find any examples..
  ### If Item Veto, still signed afterward
  
  ### Check Actions
  # filter(bill_hist, grepl('placed on calendar', tolower(action))) %>% mutate(action = gsub('H.J.+|S.J.+', '', action)) %>% distinct(action) %>% View()
  # filter(bill_hist, grepl('introduced, placed on', tolower(action))) %>% group_by(action) %>% summarize(n = n()) %>% arrange(desc(n)) %>% View()
  # bill_hist[bill_hist$bill_id == "HF0791",])
  
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
    bill_stages$bill_url <- bills[i,]$bill_url
    ### Mark AIC = 1 if bill came from H/S Study Bill (depending on chamber) -- Indicative of Committee Action
    related_bills <- bills[i,]$related_bills # | grepl("^committee", bills[i,]$primary_sponsor)
    if(bill_stages$action_in_comm == 0 & grepl(glue('{substring(b_id,1,1)}SB|LSB'), related_bills) ){
      bill_stages$action_in_comm <- 1
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
  
  ### Save Stage Info **** MERGE WITH COMMEM + SS
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
  # ** Note: chamber_cosponsors INCLUDES primary sponsor first
  unique_cospon <- gsub(';$', '', str_trim(unique(unlist(str_split(bills$cosponsors, '; ')))))
  for(nonspon in unique_cospon){
    if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != '' & !is.na(nonspon)){
      if(nonspon %in% c("van fossen, j.", "$s.sponsor")){
        next
      }
      ns_search <- paste0('^', nonspon, '$|^', nonspon, ';|; ', nonspon, '$|; ', nonspon, ';')
      chamb <- unique(substring(bills[grepl(ns_search, bills$cosponsors),]$bill_id, 1, 1))
      if("H" %in% chamb & "S" %in% chamb){
        print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
      }else{
        all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0) 
      }
    }
  }
  
  ######## Cosponsorship Info
  # all_sponsors$num_cosponsored_bills <- NA
  # bills$cospon_match <- paste(bills$LES_sponsor, bills$primary_sponsors, tolower(bills$cosponsors), sep = '; ')
  # for(i in 1:nrow(all_sponsors)){
  #   c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
  #   all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
  #   ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  #   all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  # }
  # bills <- select(bills, -cospon_match)
  # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))

  #######################
  #### CLEAN NAMES
  all_sponsors$last_name <- ifelse(grepl("^[a-z]\\.", all_sponsors$LES_sponsor), gsub('^[a-z]\\. ', '', all_sponsors$LES_sponsor), gsub(',.+', '', all_sponsors$LES_sponsor))
  all_sponsors$first_name <- ifelse(grepl("^[a-z]\\.", all_sponsors$LES_sponsor), substring(all_sponsors$LES_sponsor, 1, 1),
                                    ifelse(grepl(',', all_sponsors$LES_sponsor), gsub('^.+,', '', all_sponsors$LES_sponsor), ''))
  all_sponsors$first_name <- str_trim(gsub('\\.$', '', all_sponsors$first_name))
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  
  #### Update Last Names for Matching 
  if(t_yrs %in% c("2013_2014", "2015_2016", "2017_2018", "2019_2020")){
    all_sponsors[all_sponsors$LES_sponsor == 'jones',]$last_name <-  "hess"
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
  if(t_yrs %in% c("2003_2004", "2005_2006")){
    klarner_sub[klarner_sub$cand == 'vanfossen, james 1',]$match_name <- 'vanfossen, james'
    klarner_sub[klarner_sub$cand == 'vanfossen, jim 2',]$match_name <- 'vanfossen, jim'
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
      # ### CHeck Middle If Too Man
      # if(length(m_sub) > 1){
      #   k_matches <- k_matches[m_sub,]
      #   m_sub <- which(substring(k_matches$middle, 1, 1) == all_sponsors[i,]$middle_name)
      # }
      ### Check Without Punctuation --- Can't remove spaces unless do it for all_sponsors and k_matches
      if(length(m_sub) == 0){
        m_sub <- grep(gsub("-|'|`", '', all_sponsors[i,]$match_name), k_matches$match_name)        
      }
      ## Check First Initial (if First == Full)
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
  if(t_yrs == "2015_2016"){
    all_sponsors[all_sponsors$LES_sponsor == "moore, t." & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "2017_2018"){
    all_sponsors[all_sponsors$LES_sponsor == "miller, p." & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }

  #### CHeck for Duplicates from Matching
  duplicates <- filter(all_sponsors, !is.na(klarner_name)) %>% group_by(chamber, klarner_name) %>% filter(n() > 1)
  if(nrow(duplicates) > 0){  # any(duplicated(na.omit(all_sponsors$klarner_name)))
    cat('-----> Check DUPLICATE Sponsors \n ')
    print(select(duplicates, LES_sponsor, klarner_name, chamber) %>% as.data.frame())
  }; rm(duplicates)
  
  ##### CHECK IF ANY KLARNER CANDIDATES MISSING FROM DATA --- Includes all Senaters elected at T-1
  km <- filter(klarner_sub, !(candid %in% all_sponsors$klarner_id) )
  km <- filter(km, sen == 0 | (sen == 1 & year %in% S_elec_year:(as.numeric(substring(t_yrs, 1, 4))) ))
  
  ### Remove candidates who won but were never seated
  if(t_yrs == "2003_2004"){
    km <- filter(km, !(cand %in% c('redwine, john', 'king, steve', 'bartz, merlin e.')))
    km <- filter(km, !(cand %in% c('fiegen, thomas l.', 'deluhery, patrick j.', 'mckean, andy')))
  }else if(t_yrs == "2005_2006"){
    km <- filter(km, !(cand %in% c('veenstra, kenneth', 'hosch, julie', 'kramer, mary e.')))
    km <- filter(km, !(cand %in% c('drake, richard f.', 'sievers, bryan j.')))
  }else if(t_yrs == "2011_2012"){
    km <- filter(km, !(cand %in% c("noble, larry l.", "reynolds, kim")))
  }else if(t_yrs == "2013_2014"){
    km <- filter(km, !(cand %in% c("quirk, brian", "ward, pat", "noble, larry l.")))
  }else if(t_yrs == "2015_2016"){
    km <- filter(km, !(cand == "costello, mark" & sen == 0))
    km <- filter(km, !(cand %in% c("alons, dwayne arlan", "ernst, joni k.", "ward, pat")))
  }else if(t_yrs == "2017_2018"){
    km <- filter(km, !(cand == "lykam, jim" & sen == 0))
    km <- filter(km, cand != "seng, joe")
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
  
  ##################################
  ###### Estimate Scores + Add in Relatd Variables
  ####################################
  
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
rm(t, terms, klarner_gs, m_sub) # 
rm(chamb, ga_num, nonspon, ns_search, related_bills, term_nums, unique_cospon)


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by Special Election --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
#### Roster: https://www.legis.iowa.gov/legislation/findLegislation/findBillBySponsorOrManager
#######################################################################################################################
# filter(klarner, grepl("ragan", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 12 & sen == 0 & outcome == 'w') %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)

# ********************************************
# ----> SKIPPING GA's BELOW 80 (2003-2004) --- NEED TO FIGURE OUT WHAT TO DO ABOUT COMMITTEES 
# ********************************************

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 294 bill(s) without a sponsor
#     session chamber    N AIC ABC PASS LAW
# 1 2003_2004       H 1109 796 192  141 114
# 2 2003_2004       S  650 639 205   80  47
### WON SPECIAL ~ HOUSE:
# -- HUNTER (bruce)
# -- JACOBY (dave)
# -- SHOMSHOR (paul)
#### WON SPECIAL ~ SENATE:
# -- KETTERING (steve)
# -- WARD (pat = petricia)
### DROP
# -- redwine, john -- left office 1/12/2003
# -- king, steve -- left office after winning 2002 election for US House
# -- bartz, merlin e. -- appointed to US Dept of Ag post in January 2002
# -- fiegen, thomas l. -- left office 1/12/2003
# -- deluhery, patrick j. -- left office 1/12/2003 
# -- mckean, andy -- left office in January 2003 after winning election to county supervisor post


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 51 bill(s) without a sponsor
#     session chamber    N  AIC ABC PASS LAW
# 1 2005_2006       H 1656 1093 405  333 241
# 2 2005_2006       S  792  774 283  168 102
#### IN CHAMBER
# -- WORTHAN (gary, won 2006 special, which is in klarner)
# -- GASKILL (thurman)
#### DROP 
# -- veenstra, kenneth -- left office 1/9/2005
# -- hosch, julie -- lost 2004 senate general (after winning 2002, so must have been off-year special or redistricting)
# -- kramer, mary e. -- appointed US Ambassador to Barbados in 2004
# -- drake, richard f. -- left office 1/9/2005
# -- sievers, bryan j. -- lost 2004 senate general (after winning 2002, so must have been off-year special or redistricting)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 200 bill(s) without a sponsor
#     session chamber    N  AIC ABC PASS LAW
# 1 2007_2008       H 1570 1420 280  190 174
# 2 2007_2008       S  903  877 424  272 167
### *** NOTHING REMAINING AFTER FIXES ***** 


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 68 bill(s) without a sponsor
#     session chamber    N  AIC ABC PASS LAW
# 1 2009_2010       H 1318 1132 246  163 133
# 2 2009_2010       S  842  807 398  275 207
### WON SPECIAL ~ HOUSE:
# -- HANSON (curt)
# -- RUNNING-MARQUARD (kirsten)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 51 bill(s) without a sponsor
#     session chamber    N AIC ABC PASS LAW
# 1 2011_2012       H 1129 972 242  195 105
# 2 2011_2012       S  856 835 388  263 154
### WON SPECIAL ~ SENATE:
# -- ERNST (joni)
# -- MATHIS (liz)
# -- WHITVER (jack)
### DROP:
# -- noble, larry l. -- resigned dec. 7, 2010
# -- reynolds, kim -- left office Jan 2011 after being elected Lt. Gov


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1 bill(s) without a sponsor
#     session chamber    N AIC ABC PASS LAW
# 1 2013_2014       H 1127 921 262  222 147
# 2 2013_2014       S  702 674 345  225 120
### WON SPECIAL ~ HOUSE:
# -- GUSTAFSON (stan)
# -- MEYER (brian)
# -- PRICHARD (todd)
### WON SPECIAL ~ SENATE:
# -- GARRETT (julian, via H)
# -- SCHNEIDER (charles)
# -- WHITVER (jack)
#### Name Fix
# -- MEGAN JONES == MEGAN HESS
### DROP
# -- quirk, brian -- resigned Nov. 2012
# -- ward, pat -- died October 2012
# -- noble, larry l. -- resigned T - 1


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
#     session chamber    N AIC ABC PASS LAW
# 1 2015_2016       H 1134 816 273  233 151
# 2 2015_2016       S  840 829 410  263 130
### WON SPECIAL ~ HOUSE:
# -- HOLZ (chuck)
# -- KOOIKER (john, didn't run again)
# -- SIECK (david)
# -- MOORE, T. (TOM) ---> **Name Duplicated, won't print
### WON SPECIAL ~ SENATE:
# -- COSTELLO (mark, via H, won Dec 30 2014)
# -- SCHNEIDER (charles, elected T-1 = 2012)
### DROP:
# -- alons, dwayne arlan -- died in december 2014
# -- costello, mark -- in HOUSE, elected to Senate so never seated
# -- ernst, joni k. -- resigned Nov 2014 after winning US Senate seat
# -- ward, pat -- died T-1, 2012

 
# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1 bill(s) without a sponsor
#     session chamber    N AIC ABC PASS LAW
# 1 2017_2018       H 1157 709 389  248 197
# 2 2017_2018       S  936 924 387  213 147
### WON SPECIAL ~ HOUSE:
# -- BOSSMAN (jacob, R)
# -- JACOBSEN (jon, R)
# -- KURTH (monica, D)
# -- MILLER, P (PHIL) ---> ** NAME DUPLICATED, won't print **
### WON SPECIAL ~ SENATE:
# -- CARLIN (jim, via H, R)
# -- LYKAM (jim, via H, D)
#### DROP
# -- lykam, jim -- IN HOUSE (won Dec 2016 senate special)
# -- seng, joe -- died september 2016

# filter(klarner, grepl("carlin", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 12 & sen == 0 & outcome == 'w') %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)

##########################################################################################################
##########################################################################################################
##### MATCHING MISSING (SPECIALS) TO KLARNER USING OTHER TERMS WHERE SPONSOR IS PRESENT
##########################################################################################################

### Re-Load LES Data
LES_paths <- list.files('.', full.names = TRUE)
LES_paths <- LES_paths[!grepl('Merged', LES_paths)]

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
LES[LES$data_name %in% "jacobsen",]$klarner_id <- NA
LES[LES$data_name %in% "jacobsen",]$klarner_name <- NA
LES[LES$data_name %in% "jacobsen",]$sponsor <- 'jacobsen, jon'

### ****Still missing***** ---> Rest are not in Klarner or 2017-2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[4]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl('miller', cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, missing, still_missing)

### Fix Missing
name_matches <- data.frame(LES_name = 'running-marquardt', k_name = 'runningmarquardt, kirsten')
name_matches <- add_row(name_matches, LES_name = 'hanson', k_name = 'hanson, curt')
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
fill_missing <- data.frame(LES_name = "kooiker", new_name = 'kooiker, john j.', party = 'r', district = 4, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

#### *** 2017_2018: If any of below run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "bossman", c("sponsor", "party")] <- list('bossman, jacob', 'r')
LES[LES$sponsor == "jacobsen, jon", c("sponsor", "party")] <- list('jacobsen, jon', 'r')
LES[LES$sponsor == "kurth", c("sponsor", "party")] <- list('kurth, monica', 'd')
LES[LES$sponsor == "miller, p", c("sponsor", "party")] <- list('miller, philip d.', 'd')

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t, fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()

### Fix Names
LES[LES$sponsor == "vanfossen, james 1",]$sponsor <- 'van fossen, james k.'  ### James 'Jamie' K Van Fossen  
LES[LES$sponsor == 'vanfossen, jim 2',]$sponsor <- 'van fossen, james r.' ### James 'Jim' R Van Fossen
# --> Moving Maiden Name (which Klarner has to middle)
LES[LES$sponsor == 'hess, megan',]$sponsor <- 'jones, megan hess' 


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

#### IF NEEDED: DROPPING 2015_2016 Duplicates UNTIL SM DATA UPDATE 
# ideo <- filter(ideo, duplicated(paste(name, party)))

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

LES[LES$sponsor %in% c('smith, ras'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches: John 'Rod' Blalock
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

#### FIX MISMATCHES
# ** Amy Nielsen != Joyce Nielsen (Joyce appears to be from pre-1993)
LES[LES$sponsor %in% c('nielsen, amy'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('struy', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

name_matches <- data.frame(LES_name = 'iverson, stewart jr.', SM_name = 'Iverson Jr, Stewart E')
name_matches <- add_row(name_matches, LES_name = 'kelley, dan', SM_name = 'Kelley, Daniel')
name_matches <- add_row(name_matches, LES_name = 'may, mike', SM_name = 'May, William') # First name may be wrong, but district and time period match
name_matches <- add_row(name_matches, LES_name = 'taylor, dick', SM_name = 'Taylor, Richard') ## != Senator Rich Taylor
name_matches <- add_row(name_matches, LES_name = 'van fossen, james k.', SM_name = 'Van Fossen, James K')
name_matches <- add_row(name_matches, LES_name = 'van fossen, james r.', SM_name = 'Van Fossen, J.R.')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########

# *** Dawn Pettengill switched party (to R) on April 30, 2007 
# ---> Hard to code as she had basically half the in-session term as each
LES[LES$sponsor == 'pettengill, dawn' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Pettengill, Dawn' & ideo$party == 'D',]$name
LES[LES$sponsor == 'pettengill, dawn' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Pettengill, Dawn' & ideo$party == 'D',]$party
LES[LES$sponsor == 'pettengill, dawn' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Pettengill, Dawn' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'pettengill, dawn' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Pettengill, Dawn E.' & ideo$party == 'R',]$name
LES[LES$sponsor == 'pettengill, dawn' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Pettengill, Dawn E.' & ideo$party == 'R',]$party
LES[LES$sponsor == 'pettengill, dawn' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Pettengill, Dawn E.' & ideo$party == 'R',]$np_score

# *** DOUG STRUYK switched parties (to R) for 2004 election
# ---> NP_Score not split for time as D/R --> coding D as NA
LES[LES$sponsor == 'struyk, doug' & LES$party == 'd',]$SM_name <-  NA # ideo[ideo$name == 'Struyk, Doug' & ideo$party == 'D',]$name
LES[LES$sponsor == 'struyk, doug' & LES$party == 'd',]$SM_party <- NA # ideo[ideo$name == 'Struyk, Doug' & ideo$party == 'D',]$party
LES[LES$sponsor == 'struyk, doug' & LES$party == 'd',]$np_score <- NA # ideo[ideo$name == 'Struyk, Doug' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'struyk, doug' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Struyk, Doug' & ideo$party == 'R',]$name
LES[LES$sponsor == 'struyk, doug' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Struyk, Doug' & ideo$party == 'R',]$party
LES[LES$sponsor == 'struyk, doug' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Struyk, Doug' & ideo$party == 'R',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################

### Full Names
LES[LES$sponsor == "kelley, dan",]$sponsor <- 'kelley, daniel'
LES[LES$sponsor == "taylor, dick",]$sponsor <- 'taylor, richard d.'

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1995 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(2007:2010) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:2006, 2011:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1995 - 2020
# ** 2005-2006, split 25-25 --> Co-sharing agreement where power was split --> BOTH IN CONTROL
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:1996, 2007:2016) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2004, 2017:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1
LES[LES$chamber == 'Senate' & LES$term == "2005_2006",]$in_majority <- 1


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
  scale_color_manual(values=c("dodgerblue2", "red2", 'gray50'))

##### CHECK OUTLIERS ---- No switchers remaining through 2018!
# filter(LES, party == 'd' & np_score > 0) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & np_score < 0) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + in_majority + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')
