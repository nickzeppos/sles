#####################################
### ESTIMATE EFFECTIVENESS SCORES FOR TN
#####################################

### TO DO = CREATE AN EXTERNAL PERSON-CHAMBER-YEAR FILE for EACH STATE --- USE FASTLINK + SLER + Ideology Data

### ** Fix sponsor names -- dual chief patron bills (e.g, sb0935 2018 SESSION) are getting smushed together with first persons name, then second persons last name

### Check filter(bills, sponsor == '') %>% View() ---- Some bills doen't have a chief patron identified
### See http://lis.virginia.gov/cgi-bin/legp604.exe?001+sum+SB650 /// http://lis.virginia.gov/cgi-bin/legp604.exe?001+mbr+SB650
### If you read the text, introducing sponsors is Marsh, but not indicated in website
### ONLY 41 of these however... 

############
#### NOTES:
# NEED TO FIX THE COMMEMORATIVE CODING
# NEED TO CORRECT SPONSOR NAME ERRORS BEFORE DOING LES MERGE
# --- FOR NOW: Removing second primary sponsor.... but could add back in... 
# --- NEED TO match to external DB of names... maybe SLER election data... because, e.g., jennifer l. mcclellan vs jennifer l. mcclellan howell 
# ----------> Save and standardize to the name with the most mentions????
# --- Seems like some of the sponsor tags include second names without \n --- eg... grep all a. donald mceachins... 
# --- If Combining Sessions, need to create new bill numbers Year-Bill (fix in old scripts when new ones used)
# -----------The specials typically have unique bill numbers, but VA sessions are yearly...

#########################
### Questions
# - Distinguish between things that are adopted and become law? Do adopted things (e.g. rules/resolutions) just pass chamber?
# - Should we crdit people for AIC if Tabled in Committee? Seems odd to credit as being more effective for an act that leads to a bill's death

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

this_state <- 'VA'
min_year <- 2008


# dir.create("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/VA")
setwd("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/VA")

#######
## TH Legislative Data
## * 99th to 109th Sessions (1995-2016) -- Scraped from Webpage
bills <- read.csv("~/Dropbox/Data/State Legislative Data/States/VA/VA_Bill_Details_Full.csv")
bills = distinct(bills)

####################
## Substantive and Significant Bills 
####################
## If manual, add code from below

##### Automated
ss_bills_auto <- read.csv("~/Dropbox/Data/State Legislative Data/Significant Bills/SS_Data/VA_SS_Bills.csv")
ss_bills_auto$bill_num <- gsub("sjr", "sj", ss_bills_auto$bill_num)
ss_bills_auto$bill_num <- gsub("hjr", "hj", ss_bills_auto$bill_num)

#################
## Parsing/Standardizing Names
################

## ********* FIX NO CHIEF PATRON BILLS HERE???

## Edit + Lowercase Sponsor Names
bills$sponsor <- str_trim(gsub("\\(chief patron\\)", "", tolower(bills$sponsor)))
bills$house_sponsors <- tolower(bills$house_sponsors)
bills$senate_sponsors <- tolower(bills$senate_sponsors)

### DROPPING SECONDARY PRIME SPONSOR
bills$sponsor <- str_trim(gsub("\\\n.+| \\(.+\\)$|;.+|-resigned.+|-seat vac.+", "", bills$sponsor))
sort(table(bills$sponsor))
# *************** -------------> NEED TO STANDARDIZE NAMES --> Same person listed with diff. variations in data

### Eliminate Nicknames
bills$sponsor <- gsub('  ', ' ', gsub('\\".+\\"|\\(.+\\)', '', bills$sponsor))

## Isolate Term Years -- Most recent eletion was Nov 2017, so terms would be 2016-2017, 2018-2019
bills$session_year <- as.numeric(gsub(" .+", "", bills$session))
bills$term <- ifelse(bills$session_year %% 2 == 0, 
                     paste0(bills$session_year, "-", bills$session_year + 1),
                     paste0(bills$session_year - 1, "-", bills$session_year))

## Standardize Bill IDs
bills$bill_id <- paste0(gsub(" .+", "", tolower(bills$bill_id)), sprintf("%04s", gsub("[^0-9\\.]", "", bills$bill_id)))
bills$session_bill_id <- paste0(bills$session_year, "-", bills$bill_id)


#######################################
######## ******* TEMPORARY??? *******
######################################
### Drop Bills without a Chief Patron (41 Total)
bills <- filter(bills, sponsor != '')

###############
#### ********* TEMPORARY ************* Subset Bills DF
################

bills <- filter(bills, bills$session_year >= min_year)
# bills <- filter(bills, bills$session_year < 2017)

################################
### Merging to External Name DB
##############################
# *** Should do this whole thing by term.... 

all_names <- map_df(unique(bills$sponsor), parse_names) %>% select(-salutation)
all_names <- as.data.frame(sapply(all_names, function(x) str_trim(gsub(',', '', x))))

## Standardize Name to Match
all_names$Klarner_name <- paste0(all_names$last_name, ", ", all_names$first_name, ' ', all_names$middle_name, ' ', all_names$suffix)
all_names$Klarner_name <- tolower(gsub(" NA", '', all_names$Klarner_name))

### Shor and McCarty Data, 1993 - 2016
### ---> Names are a mess... would need to be parsed in detail... [ Extract suffixes, reformat, standardize]
# ideo <- readstata13::read.dta13("~/Dropbox/Data/Shor_McCarty_Data/shor_mccarty_1993_2016_individual_data_May_2018.dta")
# ideo <- filter(ideo, st == 'VA')

### Klarner State Leg. Election Data ---> Might Miss Appointed?
load("~/Dropbox/Data/US Election Data/state_leg/klarner_data/196slers1967to2016_20180908.RData")
klarner <- table; rm(table)
klarner$sab <- toupper(klarner$sab)
klarner <- filter(klarner, sab == this_state & year >= min_year - 2 & outcome == 'w')
# filter(klarner, year == 2011) %>% View()

### Hall and Fournaies 2018, Covers 1988 - 2014
### --> https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/PGCVDP
# hf_data <- readstata13::read.dta13("~/Dropbox/Data/Hall_Fouirnaies_2018/How_Do_interest_Groups_Seek_Access_to_Committees.dta")
# hf_data <- distinct(hf_data[,c('CandId', 'state', 'Klarner_name')])

##########################
####  Test Merge with FastLink
########################

# ### Subset to Match State
# hf_data <- filter(hf_data, state == this_state)
# hf_data$Klarner_name <- tolower(hf_data$Klarner_name)
# 
# ## MATCH TO HALL/FOUIRNAIES DATA
# hf_matches <- fastLink(all_names, hf_data, 
#                     varnames = c("Klarner_name"),
#                     stringdist.match = c("Klarner_name"), 
#                     partial.match = c("Klarner_name"),
#                     dedupe.matches = FALSE, 
#                     cut.a = .92,
#                     threshold.match = .85)
# hf_matches <- bind_cols(all_names[hf_matches$matches$inds.a,], hf_data[hf_matches$matches$inds.b,])
# hf_matches <- distinct(hf_matches, full_name, .keep_all = TRUE) %>%
#   select(Klarner_name, CandId, state, Klarner_name1)
# 
# matched_df <- left_join(all_names, hf_matches, by = "Klarner_name") %>% 
#   #select(-Klarner_name) %>%
#   rename(Klarner_name_HF = Klarner_name1)

##### MATCH TO KLARNER DATA
klarner$Klarner_name <- klarner$cand
klarner <- select(klarner, Klarner_name, candid) %>% distinct()
k_matches <- fastLink(all_names, klarner, 
                       varnames = c("Klarner_name"),
                       stringdist.match = c("Klarner_name"), 
                       partial.match = c("Klarner_name"),
                       dedupe.matches = FALSE, 
                       cut.a = .90,
                       threshold.match = .85)
k_matches <- bind_cols(all_names[k_matches$matches$inds.a,], klarner[k_matches$matches$inds.b,])
k_matches <- distinct(k_matches, full_name, .keep_all = TRUE) %>%
  select(Klarner_name, candid, Klarner_name1)

all_names <- left_join(all_names, k_matches, by = "Klarner_name") %>% 
  select(-Klarner_name) %>%
  rename(Klarner_name = Klarner_name1)

##### Swap in Klarner Name -- if matching to both HF and Klarner
#all_names$Klarner_name <- ifelse(!is.na(all_names$Klarner_name_HF), all_names$Klarner_name_HF, all_names$Klarner_name_K)
#all_names$CandId <- ifelse(!is.na(all_names$CandId.x), all_names$CandId.x, all_names$CandId.y)
# all_names <- select(all_names, -c(Klarner_name_HF, Klarner_name_K, CandId.x, CandId.y))

##### If No JW Match, Match on Last Name
for(i in 1:nrow(all_names)) {
  if(is.na(all_names[i,]$Klarner_name)){
    ln_match_rows <- grep(all_names[i,]$last_name, gsub(',.+$', '', klarner$Klarner_name))
    if(length(ln_match_rows) > 1){
      first_initials = gsub(', +', '', str_extract(klarner[ln_match_rows,]$Klarner_name, ', +[A-Za-z]'))
      first_initials = tolower(first_initials)
      ln_match_rows <- ln_match_rows[which(first_initials %in% substring(all_names[i,]$first_name, 1, 1))]
    }
    if(length(ln_match_rows) == 1){
      all_names[i,]$Klarner_name  <- klarner[ln_match_rows,]$Klarner_name
      all_names[i,]$candid  <- klarner[ln_match_rows,]$candid
      print(paste0(all_names[i,]$full_name, " ----> ", all_names[i,]$Klarner_name))
      Sys.sleep(2)
    } else {
      ln_match_rows <- grep(all_names[i,]$last_name, gsub(',.+$', '', klarner$Klarner_name))
      if(length(ln_match_rows) > 1){
        print('')
        print(paste0('****************', all_names[i,]$full_name, " ----> STILL HAS MUTLIPLE MATCHES ********************"))
        print('')
      }
    }
  }
}

### Fix Auto-Code Errors --- Seems to stem from recent sessions
all_names[all_names$full_name == 'dawn m. adams', c('Klarner_name', 'candid')] <- cbind(NA, NA)

#### Match Rate ~ 90%, with most errors being from recent term
sum(!is.na(all_names$Klarner_name))/nrow(all_names)

##### MANUAL FIXES WHRE APPLICABLE -- NONE APPLICABLE???
# filter(all_names, is.na(Klarner_name))
# filter(klarner, grepl('jones', Klarner_name))

all_names[all_names$full_name == 'nick rush',]$Klarner_name <- 'rush, larry n. (nick)'
all_names[all_names$full_name == 'nick rush',]$candid <- 320404

rm(k_matches, first_initials, i, ln_match_rows)

#############################
### Update Sponsor Variable to Be Unique
#####################################

### Creating Klarner-like name to fill in for missing names
all_names$Klarner_format <- paste0(all_names$last_name, ", ", all_names$first_name, ' ', all_names$middle_name, ' ', all_names$suffix)
all_names$Klarner_format <- tolower(gsub(" NA", '', all_names$Klarner_format))

#### Need to Edit Bill Name to Match whats in the ALL Names DF --- Parser drops commas and what not
bills$match_name <- str_trim(gsub(',', '', bills$sponsor))
  
### Merge and Update
bills <- left_join(bills, select(distinct(all_names), full_name, Klarner_name, Klarner_format, candid), by = c("match_name" = "full_name"))
# filter(bills, is.na(Klarner_name)) %>% select(sponsor) %>% unlist() %>% unique()
# filter(klarner, grepl('krup', cand)) %>% select(cand) %>% unlist() %>% unique()

bills$sponsor <- ifelse(!is.na(bills$Klarner_name), bills$Klarner_name, bills$Klarner_format)
bills <- select(bills, -Klarner_format)

rm(all_names, klarner)


#####################################################################################################################
#####################################################################################################################
#####################################################################################################################
#####################################################################################################################


#################
## Cleaning Data for Easier Matching
################

## Lowercase Descriptions
bills$short_title <- gsub('^[a-z]+ [0-9]+ |\\.$', '', tolower(bills$short_title))
bills$summary <- tolower(bills$summary)

### Create variable with last names
# name_split <- str_split(ss_bills$sponsor, " ")
# ss_bills$sponsor_last_name <- sapply(name_split, tail, 1)
# rm(name_split)


###################################
### Subset to Relevant Bill Types
###############################

### Check Types
unique(gsub('[0-9].*', '', bills$bill_id))

### Drop Certain Types
#bills <- filter(bills, !(gsub('[0-9].*', '', bills$bill_id) %in% c('zzzz')) )


#############################################
##### Match S&S Bills to Full Set of Bills
#############################################
# ***** THis could be a left_join but need to be careful about double matches? ******

bills$SS <- 0

#### This loop works so long as second year bill numbers start where first year ended in term (which is the case in VA)
for(i in 1:nrow(bills)){
  bill_range <- as.numeric(unlist(str_split(bills[i,]$term, "-")))
  auto_sub <- filter(ss_bills_auto, year %in% bill_range[1]:bill_range[2])
  auto_sub <- filter(auto_sub, bill_num == bills[i,]$bill_id)
  if(nrow(auto_sub) > 0){
    bills[i,]$SS <- 1
  }
  print(i)
}
rm(i, bill_range, auto_sub)

table(bills$SS)

#### *** NEED TO ACCOUNT FOR TERMS BETTER IF DOING THIS **** Yields mult-matches
# bills <- left_join(bills, select(ss_bills_auto, term, bill_num, SS_auto), by = c("term" = "term", "bill_id" = "bill_num")) %>%
#   mutate(SS_auto = ifelse(is.na(SS_auto), 0, SS_auto))

### What doesn't Match?
# anti_join(ss_bills_auto, bills, by = c("year" = "session_year", "bill_num" = "bill_id"))
# filter(bills, bill_id == "hb5025")

########################################
###### Identify Commemorative Bills
######################################
## *** DON'T USE: award 
# bills[grepl("award", tolower(bills$short_title)),]$short_title

## ***** MIGHT BE BETTER TO USE THE KEYWORD (before dash in main title --- e.g, naming and designating; )

### Removing: "recogni", "encourag", ---> lots of errors...
vw_congress_commem <- c("expressing support", "urging", "promoting", "condol", "commemorat", "honor", "memoria",
                        "congratul",  "public holiday", "rename", "for the private relief of", "designa", 
                        "for the relief of", "medal", "mint coin", "posthumous", "public holiday", "provide for correction",
                        "to name", "redisgnat", "to remove any doubt", "to rename", "retention of the name")
additions <- c("anniversary", "awareness", "dedicat", "festival", "celebrat", "commend", "winners") #"day.$|week.$|month.$"
#bills[grepl("encourag", tolower(bills$short_title)),]$short_title

title_match <- grepl(paste(c(vw_congress_commem, additions), collapse = "|"), tolower(bills$short_title))
summary_match <- grepl(paste(c(vw_congress_commem, additions), collapse = "|"), tolower(bills$summary))
# keyword_match <- grepl(paste(c(vw_congress_commem, additions), collapse = "|"), tolower(bills$keywords))

bills$commem <- ifelse(title_match | summary_match, 1, 0)
table(bills$commem)
# filter(bills, commem == 1) %>% select(short_title)

rm(title_match, summary_match, keyword_match, additions, vw_congress_commem)

###############################################
#### Function to take a given bill history for VA and code legislative stages
###############################################

# bill_history <- hist_sub
# this_id <- this_bill_id
# this_session <- this_session
# this_term <- this_term
evaluate_bill_hist <- function(bill_history, this_id, this_session, this_term){
  
  ### Lowercase
  bill_history$action <- tolower(bill_history$action)
  # bill_history$action <- gsub("\\.", "", bill_history$action)
  
  if(nrow(bill_history) > 0){
    
    ### Initiating Chamber
    init_chamber <- ifelse(substring(tolower(this_id), 1, 1) == "h", "House", "Senate")
    other_chamber <- ifelse(init_chamber == "House", "Senate", "House")
    
    ### Subset to Introduction Chamber
    in_chamber <- which(bill_history$chamber == init_chamber)
    left_chamber <- which(bill_history$chamber == other_chamber)
    # Adjusting for sporadic outchamber actions before in-chamber action
    left_chamber <- left_chamber[which(left_chamber > min(in_chamber))]
    if(length(left_chamber) > 0){
      chamber_history <- bill_history[bill_history$action_order < min(left_chamber),]
    }else{
      chamber_history <- bill_history
    }
    
    ### Action in Committee
    aic_terms <- c("assigned to .+ sub-comm", "assigned.+ sub:", "reported from", "subcommittee recomm", "subcommittee failed to", 
                   "tabled in", "failed to report")
    aic <- max(grepl(paste(aic_terms, collapse = "|"), chamber_history$action))
    # ** Double check this with Craig/Alan
    
    ### Action beyond committee -- the chamer specific terms will only matter for each speific chamber history
    abc_terms <- c("^reported from", "read second time", "read third time", "engrossed", "vote:", "passed house", "passed senate",
                   "defeated by house", "defeated by senate")
    abc <- max(grepl(paste(abc_terms, collapse = "|"), chamber_history$action))
    # * If Passed, then necessarily ABC
    
    ### Passed Chamber
    pc_terms <- c("passed house", "passed senate", "agreed to by house", "agreed to by senate", "vote: passage", "vote: adoption",
                  "signed by speaker", 'signed by president') # Including just in case
    pc <- max(grepl(paste(pc_terms, collapse = "|"), chamber_history$action))
    
    ### Law
    # *** This will not count companions that became law (which always are preceded by "Comp. became Pub Ch. N")
    law <- max(grepl("^approved by gov|^acts of assembly chapter text ", bill_history$action))
    
    ### Necessary Conditions
    # (1) If Law, must have received action beyond committee and passed chamber
    # (2) If passed chamber but not law, must have received action beyond committee
    if(law == 1){
      abc <- pc <- 1
    }else if(pc == 1){
      abc <- 1
    }
    
  } else {
    aic <- abc <- pc <- law <- 0
  }
  
  ##### DF to Return
  bill_stages <- data.frame(bill_id = this_id, session = this_session, term = this_term, 
                            introduced = 1, action_in_comm = aic, action_beyond_comm = abc, passed_chamber = pc, law = law)
  return(bill_stages)
  
}

# rm(abc, abc_terms, aic, i, init_chamber, law, pc, pc_terms, this_bill_id)

############################
### Code Bill Histories
############################

bill_hist <- read.csv("~/Dropbox/Data/State Legislative Data/States/VA/VA_Bill_Histories.csv")

## Isolate Term Years -- Most recent eletion was Nov 2017, so terms would be 2016-2017, 2018-2019
bill_hist$session_year <- as.numeric(gsub(" .+", "", bill_hist$session))
bill_hist$term <- ifelse(bill_hist$session_year %% 2 == 0, 
                         paste0(bill_hist$session_year, "-", bill_hist$session_year + 1),
                         paste0(bill_hist$session_year - 1, "-", bill_hist$session_year))

## Standardize Bill IDs
bill_hist$bill_id <- tolower(paste0(gsub(" .+", "", bill_hist$bill_id), sprintf("%04s", gsub("[^0-9\\.]", "", bill_hist$bill_id))))
bill_hist$session_bill_id <- paste0(bill_hist$session_year, "-", bill_hist$bill_id)

### Output Matrix
all_bill_stages <- data.frame(matrix(nrow = 0, ncol = 8))
colnames(all_bill_stages) <- c("bill_id", "session", "term", "introduced", "action_in_comm", "action_beyond_comm", "passed_chamber", "law")

### Code Each Bill
for(i in 1:nrow(bills)){
  
  this_bill_id <- bills[i,]$bill_id
  this_session <- bills[i,]$session
  this_term <- bills[i, ]$term
  hist_sub <- filter(bill_hist, bill_id == this_bill_id & session == this_session & this_term == term)
  
  ### Code History
  bill_stages <- evaluate_bill_hist(hist_sub, this_bill_id, this_session, this_term)
  
  ### Append to DF
  all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
  print(i)
}

bills <- left_join(bills, all_bill_stages, by = c("bill_id", "session", "term"))

rm(this_bill_id, this_session, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist)

##################################
#### Aggregate
##################################

#### ********** TEMPORARY **************
#### Drop NA Sponsors
# ----> Could take the first sponsor (see code below) and swap in, but not always right?

bills <- filter(bills, !is.na(sponsor))

###############
# first_sponsor <- ifelse(substring(bills$bill_id,1,1) == "h", bills$house_sponsors, bills$senate_sponsors)
# first_sponsor <- gsub("\\[\'|\\(chief patron\\)|,.+", "", first_sponsor)
# first_sponsor <- gsub(" \\'", "", first_sponsor)
# first_sponsor <- str_trim(gsub("]", "", first_sponsor, fixed = TRUE))
# bills$sponsor <- ifelse(bills$sponsor == "", first_sponsor, bills$sponsor)

#### CORRECT SYSTEMATIC SPONSOR ISSUES
# sort(table(bills$sponsor))
# bills$sponsor <- gsub("\\'", "", bills$sponsor)
# bills$sponsor <- gsub("-resign.+|\\(resigned\\)|\\(deceased\\)|;resigned|-seat vacat.+|\\(retire.+", "", bills$sponsor)

#### Drop Remaining Issues?
# filter(bills, sponsor == "[") %>% select(-summary)
# bills <- filter(bills, sponsor != "[|[]")


#######################################
# ### Expand DF to include a Row for each primary sponsor --- SHOULD BE 24108 ROWS
# bills_multi <- bills %>%
#   mutate(chamber = substring(bill_id, 1, 1)) %>%
#   # filter(bill_id == "H0001" & session == "2009-2010 Session") %>%
#   # filter(bill_id == "H0002" & session == "2009-2010 Session") %>%
#   filter(!grepl("\\['rules, calendar, and operations of the house'\\]", primary_sponsors)) %>%
#   mutate(primary_sponsor_list = str_split(gsub("\\[|\\]|\\'", "", primary_sponsors), ", "),
#          cosponsor_list = str_split(gsub("\\[|\\]|\\'", "", cosponsors), ", "),
#          num_primary = str_count(primary_sponsor_list, ",") + 1,
#          num_cosponsors = ifelse(nchar(cosponsor_list) == 0, 0, str_count(cosponsor_list, ",") + 1),
#          total_sponsors = num_primary + num_cosponsors) %>% 
#   # summarize(all_spon = sum(total_sponsors))
#   group_by(session, chamber, bill_id) %>%
#   slice(rep(1:n(), each = total_sponsors )) %>%
#   mutate(this_sponsor = ifelse(num_cosponsors == 0, unlist(primary_sponsor_list[1]), c(unlist(primary_sponsor_list[1]), unlist(cosponsor_list[1]))),
#        sponsor_type = ifelse(num_cosponsors == 0, rep("Primary", num_primary[1]), c(rep("Primary", num_primary[1]), rep("Cosponsor", num_cosponsors[1]))),
#        sponsor_type = ifelse(1:n() == 1, "Introducing Sponsor", sponsor_type))


### Drop bills with non-member sponsors sponsor
sort(table(bills$sponsor))
# bills <- filter(bills_multi, !grepl("ethics|agriculture|judiciary subcommittee a|judiciary|health and human services", this_sponsor)) 

#### Other Variables
# bills$agg_term <- ifelse(grepl("^2016", bills_multi$session), "2015-2016", gsub(" Session", "", bills_multi$session))
bills$chamber <- substring(bills$bill_id, 1, 1)


###################
### Function to Calculate LES Scores --- Could be Simplified to Get Rid of Variations and Standardized to be in a Separate File
calc_LES <- function(bill_data, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1)){
  
  # ***** DOING THESE BY ELECTORAL TERM NOT SESSION **********
  
  ### Do Inverse Probability Weighting?
  inverse_prob <- ifelse(stage_weights[1] == "inverse_prob", TRUE, FALSE)
  
  ##### Bill Weights
  bill_data$bill_weight <- reg_weight
  bill_data$bill_weight <- ifelse(bill_data$commem == 1, com_weight, bill_data$bill_weight)
  bill_data$bill_weight <- ifelse(bill_data$SS == 1, ss_weight, bill_data$bill_weight)
  
  ### Output DF
  LES_dat <- data.frame(matrix(nrow = 0, ncol = 10))
  colnames(LES_dat) <- c("sponsor", "term", "chamber", "LES", "LES_rank", "BILL_wshare", "AIC_wshare", "ABC_wshare", "PASS_wshare", "LAW_wshare")
  
  ##### Loop through terms
  for(t in unique(bill_data$term)){
    term_bills <- bill_data[bill_data$term == t,]
    
    #### Loop though House, Senate
    for(c in unique(bill_data$chamber)){
      
      chamber_term <- term_bills[term_bills$chamber == c,]
      N <- length(unique(chamber_term$sponsor))
      
      ### Sums for each term
      BILL_denom <- sum(chamber_term$bill_weight * chamber_term$introduced)
      AIC_denom <- sum(chamber_term$bill_weight * chamber_term$action_in_comm)
      ABC_denom <- sum(chamber_term$bill_weight * chamber_term$action_beyond_comm)
      PASS_denom <- sum(chamber_term$bill_weight * chamber_term$passed_chamber)
      LAW_denom <- sum(chamber_term$bill_weight * chamber_term$law)
      
      ###### BY CHAMBER AND TERM: Weight Stages by Inverse Probability of Success
      if(inverse_prob == TRUE){
        stage_weights <- rep(1, 5)
        stage_weights[1] <- 1 / (sum(chamber_term$introduced) / nrow(chamber_term))
        stage_weights[2] <- 1 / (sum(chamber_term$action_in_comm) / nrow(chamber_term))
        stage_weights[3] <- 1 / (sum(chamber_term$action_beyond_comm) / nrow(chamber_term))
        stage_weights[4] <- 1 / (sum(chamber_term$passed_chamber) / nrow(chamber_term))
        stage_weights[5] <- 1 / (sum(chamber_term$law) / nrow(chamber_term))
        
        print(paste0("TERM: ", t, " ---- Chamber: ", c, " ---- Stage Weights: "))
        print(round(stage_weights, 2))
      }
      
      ##### Loop through members within each chamber-term
      for(i in unique(chamber_term$sponsor)){
        member_chamber_term <- chamber_term[chamber_term$sponsor == i,]
        
        ### Member-Level Shares for each stage of the process
        BILL_w <- stage_weights[1] * sum(member_chamber_term$bill_weight * member_chamber_term$introduced)
        AIC_w <- stage_weights[2] * sum(member_chamber_term$bill_weight * member_chamber_term$action_in_comm)
        ABC_w <- stage_weights[3] * sum(member_chamber_term$bill_weight * member_chamber_term$action_beyond_comm)
        PASS_w <- stage_weights[4] * sum(member_chamber_term$bill_weight * member_chamber_term$passed_chamber)
        LAW_w <- stage_weights[5] * sum(member_chamber_term$bill_weight * member_chamber_term$law)
        
        #### Shares
        BILL_wshare <- BILL_w/BILL_denom
        AIC_wshare <- AIC_w/AIC_denom
        ABC_wshare <- ABC_w/ABC_denom
        PASS_wshare <- PASS_w/PASS_denom
        LAW_wshare <- LAW_w/LAW_denom
        
        #### Summing for Total LES measure --- Note N/5 Adjustment done at each earlier stage
        # Standard
        adj_factor <- sum(stage_weights)
        LES <- N / adj_factor * (BILL_wshare + AIC_wshare + ABC_wshare + PASS_wshare + LAW_wshare)
        
        ## Collapsed --- Need ot figure out how this changes the weighting factor...
        # numerator <- (BILL_w + AIC_w + ABC_w + PASS_w + LAW_w)
        # denom <- (BILL_w_denom + AIC_w_denom + ABC_w_denom + PASS_w_denom + LAW_w_denom)
        # LES <- numerator/denom * N
        
        #### Record
        LES_dat <- add_row(LES_dat, sponsor = i, term = t, chamber = c, LES = LES, LES_rank = NA,
                           BILL_wshare = BILL_wshare, AIC_wshare = AIC_wshare, ABC_wshare = ABC_wshare,
                           PASS_wshare = PASS_wshare, LAW_wshare = LAW_wshare)
        
        print(paste0(t, " --- ", c, " ---- ", i, ": ", LES))
      }
    }
  }
  
  ### Calculate Individual Rank Within Each Chamber-Term
  LES_dat <- LES_dat %>%
    mutate(chamber = ifelse(chamber == "h", "House", "Senate")) %>%
    group_by(term, chamber) %>%
    arrange(desc(LES), .by_group = TRUE) %>%
    mutate(LES_rank = 1:n())
  
  return(LES_dat)
}


##########################################
#### ESTIMATE LES
###########################################

### Standard LES: Same as Congressional Measure
LES <- calc_LES(bills, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1))
# LES %>% group_by(chamber, term) %>% summarize(mean_LES = mean(LES))

table(LES$chamber, LES$term)
# *** Should be 40 Senators, 100 Delegates by term

#filter(LES, chamber == "Senate" & term == '2008-2009') %>% arrange(sponsor) %>% View()


##############################################
###  Supplement with Chamber-Term Variables
##############################################

####### Agg Variables
agg_stats <- bills %>%
  group_by(term, chamber, sponsor) %>%
  summarize(num_sponsored_bills = n(),
            sponsor_pass_rate = sum(passed_chamber) / n(),
            sponsor_hit_rate = sum(law) / n()) %>%
  ungroup() %>%
  mutate(chamber = ifelse(chamber == "h", "House", "Senate")) 

LES <- left_join(LES, agg_stats, by = c("term", "chamber", "sponsor"))


### Simple Plots
ggplot(LES, aes(x = num_sponsored_bills, y = LES)) + geom_point() + facet_grid(chamber ~ term)
ggplot(LES, aes(x = sponsor_pass_rate, y = LES)) + geom_point() + facet_grid(chamber ~ term)
ggplot(LES, aes(x = sponsor_hit_rate, y = LES)) + geom_point() + facet_grid(chamber ~ term)

##############################################
###  Save
##############################################

# write.csv(filter(LES, term != '2018-2019'), "VA_LES_2008_2017.csv", row.names = FALSE)


##############################################
#### MERGE IN DATA
##############################################

rm(list=ls())
this_state <- 'VA'; min_year <- 2008

### Re-Load LES Data
LES <- read.csv('VA_LES_2008_2017.csv')

### Klarner State Leg. Election Data
load("~/Dropbox/Data/US Election Data/state_leg/klarner_data/196slers1967to2016_20180908.RData")
klarner <- table; rm(table)
klarner$sab <- toupper(klarner$sab)
klarner <- filter(klarner, sab == this_state & year >= min_year - 2 & outcome == 'w')
klarner <- select(klarner, caseid, year, sen, ddez, dno, term, termz, cando, cand, candid, partyz, partyt, exper, outcome)

### Fixing Klarner Error
klarner[klarner$cando == 'K. Rob Krupicka',]$cand <- 'krupicka, k. rob'

#### KLARNER IS YEAR OF ELECTION, Not TERM
senate_elec_years <- seq(1991, 2018, by = 4)
LES$exper <- LES$party <- LES$district <- LES$klarner_id <- NA
for(name in unique(LES$sponsor)){
  this_sponsor_LES <- LES[LES$sponsor == name,]
  for(t in this_sponsor_LES$term){
    elec_years <- as.numeric(str_split(t, "-")[[1]][1])
    elec_years <- c(elec_years - 1, elec_years)
    ### Handful of people switch chambers mid-term
    for(c in this_sponsor_LES[this_sponsor_LES$term == t,]$chamber){
      if(c == 'Senate' & !(elec_years[1] %in% senate_elec_years)){
        elec_years <- (elec_years[1] - 2):elec_years[2]
      } else if (c == 'Senate'){
        elec_years <- elec_years[1]:(elec_years[2] + 2)
      }
      klarner_sub <- filter(klarner, year %in% elec_years & sen == ifelse(c == "Senate", 1, 0)) 
      klarner_sub <- filter(klarner_sub, cand == name)
      if(nrow(klarner_sub) > 0 ){
        LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$district <- unique(klarner_sub$dno)
        LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$klarner_id <- unique(klarner_sub$candid)
        LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$party <- unique(klarner_sub$partyz)
        LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$exper <- unique(klarner_sub$exper)
      }else{
        ### Check subsequent term (if appointed or special or something)
        next_term = elec_years + 2
        klarner_sub <- filter(klarner, year %in% next_term & sen == ifelse(c == "Senate", 1, 0)) 
        klarner_sub <- filter(klarner_sub, cand == name)
        if(nrow(klarner_sub) > 0){
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$district <- unique(klarner_sub$dno)
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$klarner_id <- unique(klarner_sub$candid)
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$party <- unique(klarner_sub$partyz)
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$exper <- unique(klarner_sub$exper)
        }
      }
    }    
  }
  print(name)
}

#### Manual Fixes
# --------------------

rm(klarner, klarner_sub, this_sponsor_LES, c, elec_years, name, next_term, senate_elec_years, t)

#########################################################
############ Match to Hall/Fouirnaies
########################################################

### Hall and Fournaies 2018, Covers 1988 - 2014
### --> https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/PGCVDP
hf_data <- readstata13::read.dta13("~/Dropbox/Data/Hall_Fouirnaies_2018/How_Do_interest_Groups_Seek_Access_to_Committees.dta")
hf_data <- filter(hf_data, state == this_state)
hf_data <- hf_data[,c(colnames(hf_data)[c(1:14, 204:206, 208)], colnames(hf_data)[grepl('cmt_', colnames(hf_data))])]
hf_data$term <- paste0(hf_data$year + 1, "-", hf_data$year + 2)
#hf_data$match_name <- tolower(hf_data$Klarner_name)
#hf_data$sen <- ifelse(hf_data$chamber == 'senate', 1, 0)
hf_data$chamber <- ifelse(hf_data$chamber == 'senate', 'Senate', "House")

### Expanding Senate to Terms (So Adding a Second Term)
senate <- hf_data[hf_data$chamber == "Senate",]
senate$term <- paste0(senate$year + 3, "-", senate$year + 4)
hf_data <- bind_rows(hf_data, senate); rm(senate)

### Subset
hf_data <- filter(hf_data, year > min_year - 2)

### Merge
LES <- left_join(LES, hf_data, by = c('term' = 'term', 'chamber' = 'chamber', 'klarner_id' = 'CandId'))

# ### Loop Through to Catch Missing?? Except this is going to catch wrong year, put them on worng committees
# # ---> See knight, barry d. for example... wins special in 2009, so not in data...
# for(i in 1:nrow(test)){
#   if(is.na(test[i,]$Klarner_name)){
#     hf_sub <- filter(hf_data, CandId == test[i,]$klarner_id, chamber == test[i,]$chamber)  
#     ## Catching MidTerm Appointments/Specials
#     hf_sub <- filter(hf_sub, year %in% as.numeric(unlist(str_split(test[i,]$term, '-'))))
#     
#   }
# }
rm(hf_data)

#################################
### Shor and McCarty Data, 1993 - 2016
####################################
### ---> Names are a mess... Need need to be parsed in detail... [ Extract suffixes, reformat, standardize]
### COULD Do this by cross-checking actice chamber-years.... 

ideo <- readstata13::read.dta13("~/Dropbox/Data/Shor_McCarty_Data/shor_mccarty_1993_2016_individual_data_May_2018.dta")
ideo <- filter(ideo, st == 'VA') %>% select(-st_id)
ideo$match_name <- tolower(ideo$name)
LES_match <- select(LES, sponsor, klarner_id) %>% rename(match_name = sponsor) %>% distinct()

## MATCH TO SHOR/MCCARTY DATA
ideo_matches <- fastLink(LES_match, ideo,
                    varnames = c("match_name"),
                    stringdist.match = c("match_name"),
                    partial.match = c("match_name"),
                    dedupe.matches = TRUE, ### All LES Names Should be Unique Now
                    cut.a = .92,
                    threshold.match = .85)
ideo_matches <- bind_cols(LES_match[ideo_matches$matches$inds.a,], ideo[ideo_matches$matches$inds.b, c('name', 'party', 'np_score')])
ideo_matches <- rename(ideo_matches, SM_name = name) %>% 
  select(-c(klarner_id, party)) %>%
  distinct()

LES <- left_join(LES, ideo_matches, by = c("sponsor" = "match_name"))

rm(ideo, ideo_matches, LES_match)

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

LES <- select(LES, -party.y) %>% rename(party = party.x)

# write.csv(LES, "VA_LES_2008_2017_M.csv", row.names = FALSE)


#####################################################
### CHECK DATA
###########################################################

# LES <- read.csv("VA_LES_2008_2017_M.csv")
# 
# table(LES$chamber, LES$term)
# 
# LES %>%
#   group_by(chamber, term) %>%
#   summarize(N = n(),
#             speaker = sum(SpeakerHouse, na.rm = T), 
#             sen_n = sum(sen, na.rm = T), 
#             wshare_mean = mean(ABC_wshare),
#             les_sum = sum(LES))
# 
# LES %>%
#   summarize(N = n(),
#             speaker = sum(SpeakerHouse, na.rm = T), 
#             sen_n = sum(sen, na.rm = T), 
#             b_share = mean(BILL_wshare) * n() ,
#             aic_share = mean(AIC_wshare),
#             abc_share = mean(ABC_wshare),
#             p_share = mean(PASS_wshare),
#             l_mean = mean(LAW_wshare),
#             les_sum = sum(LES))
# 
# 
# filter(LES, PresidentSenate == 1)
# filter(LES, grepl('bollin', tolower(Klarner_name)))
# filter(hf_data, grepl('HOWELL, WILLIAM J.', toupper(Klarner_name)))
# filter(hf_data, year == 2011 & SpeakerHouse == 0)
# 
# 


#################################################################
### V1



# #######
# ## TH Legislative Data
# ## * 99th to 109th Sessions (1995-2016) -- Scraped from Webpage
# bills <- read.csv("~/Dropbox/Data/State Legislative Data/States/VA/VA_Bill_Details_Full.csv")
# bills = distinct(bills)
# 
# ######
# ## Substantive and Significant Bills --- Via Craig's RAs --- Collapsing from multiple sheets
# ######
# 
# ##### Manual
# va_files <- list.files("~/Dropbox/Data/State Legislative Data/Significant Bills/Manual_SS_Data/VA", full.names = TRUE)
# ss_bills <- map2_df(va_files, gsub("^.+Manual_SS_Data/[A-Z]+/", "", va_files), ~read_excel(.x, skip = 1) %>% mutate(id = .y)) %>%
#   select(1:4)
# ss_bills[ss_bills$`Bill #` == "Senate Joint Resolution 290", ]$`Bill #` <- "SJ 290"
# ss_bills <- mutate(ss_bills, year = gsub('VA_SS_BILLS_|.xlsx', '', id),
#                    state_abbr = substring(id, 1, 2)) %>%
#   rename(bill_num = `Bill #`, sponsor = Sponsor, doc_number = `Doc #`, state_file = id) %>% 
#   mutate(num_only = str_extract(bill_num, "\\d++"),
#          bill_type = gsub(' [0-9].+$| [0-9]|[0-9].+$|[0-9]', '', bill_num),
#          bill_num_z = tolower(paste0(gsub(' +[0-9].+$| [0-9].+$| [0-9]|[0-9].+$|[0-9]', '', bill_num), str_pad(str_extract(bill_num, "\\d++"), 4, pad= 0)))) %>%
#   distinct(bill_num_z, session_year, state_abbr, .keep_all = TRUE)
# 
# ##### Automated
# ss_bills_auto <- read.csv("~/Dropbox/Data/State Legislative Data/Significant Bills/SS_Data/VA_SS_Bills.csv")
# ss_bills_auto$bill_num <- gsub("sjr", "sj", ss_bills_auto$bill_num)
# ss_bills_auto$bill_num <- gsub("hjr", "hj", ss_bills_auto$bill_num)
# 
# 
# #################
# ## Cleaning Data for Easier Matching
# ################
# 
# ## Edit + Lowercase Sponsor Names
# bills$sponsor <- str_trim(gsub("\\(chief patron\\)", "", tolower(bills$sponsor)))
# bills$house_sponsors <- tolower(bills$house_sponsors)
# bills$senate_sponsors <- tolower(bills$senate_sponsors)
# #ss_bills$sponsor <- tolower(ss_bills$sponsor)
# 
# ### DROPPING SECONDARY PRIME SPONSOR
# bills$sponsor <- str_trim(gsub("\\\n.+ | \\(.+\\)$|;.+|-resigned.+", "", bills$sponsor))
# table(bills$sponsor)
# # *************** -------------> NEED TO STANDARDIZE NAMES --> Same person listed with diff. variations in data
# 
# ## Lowercase Descriptions
# bills$short_title <- gsub('^[a-z]+ [0-9]+ |\\.$', '', tolower(bills$short_title))
# bills$summary <- tolower(bills$summary)
# 
# #### Senator or Rep.
# # ss_bills$member_type <- ifelse(grepl("^rep\\.", ss_bills$sponsor ), "Representative", NA)
# # ss_bills$member_type <- ifelse(grepl("^sen\\.", ss_bills$sponsor ), "Senator", ss_bills$member_type)
# 
# ## **** Errors in SS File --- 2015, end of sheet
# # ss_bills$sponsor <- gsub("#error!", "", ss_bills$sponsor)
# 
# ### Create variable with last names
# # name_split <- str_split(ss_bills$sponsor, " ")
# # ss_bills$sponsor_last_name <- sapply(name_split, tail, 1)
# # rm(name_split)
# 
# ## Isolate Term Years -- Most recent eletion was Nov 2017, so terms would be 2016-2017, 2018-2019
# bills$session_year <- as.numeric(gsub(" .+", "", bills$session))
# bills$term <- ifelse(bills$session_year %% 2 == 0, 
#                      paste0(bills$session_year, "-", bills$session_year + 1),
#                      paste0(bills$session_year - 1, "-", bills$session_year))
# 
# ## Standardize Bill IDs
# bills$bill_id <- paste0(gsub(" .+", "", tolower(bills$bill_id)), sprintf("%04s", gsub("[^0-9\\.]", "", bills$bill_id)))
# bills$session_bill_id <- paste0(bills$session_year, "-", bills$bill_id)
# 
# ###############
# #### Subset NC Bills DF
# 
# bills <- filter(bills, bills$session_year >= 2008)
# 
# # ss_bills <- mutate(ss_bills, bill_number = gsub("SB ", "S", bill_number)) %>%
# #   mutate(bill_number = gsub("HB ", "H", bill_number)) %>%
# #   mutate(bill_number = paste0(substring(bill_number, 1, 1),
# #                               sprintf("%04s", gsub("[^0-9\\.]", "", bill_number)))) %>%
# #   arrange(year, bill_number)
# 
# # # * Fixing a differently formatted bill_number
# # ss_bills[ss_bills$bill_number == "H.164",]$bill_number <- "H0164"
# 
# 
# ##################################################################################################################################
# 
# #############################################
# ##### Match S&S Bills to Full Set of Bills
# #############################################
# 
# # ***** THis should probably be a left_join but need to be careful about double matches? ******
# 
# # i = 1
# bills$SS <- 0
# bills$SS_auto <- 0
# # ss_bills$matched <- 0
# # ss_bills_auto$matched <- 0
# # ss_bills$index <- 1:nrow(ss_bills)
# # ss_bills_auto$index <- 1:nrow(ss_bills_auto)
# 
# for(i in 1:nrow(bills)){
#   bill_range <- as.numeric(unlist(str_split(bills[i,]$term, "-")))
#   #### Manually Coded
#   bill_sub <- filter(ss_bills, as.numeric(year) %in% bill_range[1]:bill_range[2])
#   bill_sub <- filter(bill_sub, bill_num_z == bills[i,]$bill_id)
#   if(nrow(bill_sub) > 0){
#     bills[i,]$SS <- 1
#     #ss_bills[bill_sub$index,]$matched <- ss_bills[bill_sub$index,]$matched + 1
#     #match <- rep(NA, nrow(bill_sub))
#     #for(j in 1:nrow(bill_sub)){match[j] <- any(grepl(bill_sub$sponsor, bills[i,]$primary_sponsors))}
#     #bills[i,]$SS <- ifelse(any(match) == TRUE, 1, 0)
#   }
#   #### Auto Coded
#   auto_sub <- filter(ss_bills_auto, year %in% bill_range[1]:bill_range[2])
#   auto_sub <- filter(auto_sub, bill_num == bills[i,]$bill_id)
#   if(nrow(auto_sub) > 0){
#     bills[i,]$SS_auto <- 1
#     #ss_bills_auto[auto_sub$index,]$matched <- ss_bills_auto[auto_sub$index,]$matched + 1
#   }
#   print(i)
# }
# rm(i, bill_range, bill_sub, auto_sub) # match, j,
# 
# 
# # ss_bills$year <- as.numeric(ss_bills$year)
# # ss_bills$term <- ifelse(ss_bills$year %% 2 == 0, paste0(ss_bills$year, "-", ss_bills$year + 1),paste0(ss_bills$year - 1, "-", ss_bills$year))
# # ss_bills_auto$term <- ifelse(ss_bills_auto$year %% 2 == 0, paste0(ss_bills_auto$year, "-", ss_bills_auto$year + 1),paste0(ss_bills_auto$year - 1, "-", ss_bills_auto$year))
# 
# #### *** NEED TO ACCOUNT FOR TERMS BETTER IF DOING THIS **** Yields mult-matches
# # bills <- left_join(bills, select(ss_bills, term, bill_num_z, SS), by = c("term" = "term", "bill_id" = "bill_num_z")) %>%
# #   mutate(SS = ifelse(is.na(SS), 0, SS))
# # 
# # bills <- left_join(bills, select(ss_bills_auto, term, bill_num, SS_auto), by = c("term" = "term", "bill_id" = "bill_num")) %>%
# #   mutate(SS_auto = ifelse(is.na(SS_auto), 0, SS_auto))
# # 
# table(bills$SS_auto)
# table(bills$SS)
# 
# ### What doesn't Match?
# # anti_join(ss_bills, bills, by = c("term" = "term", "bill_num_z" = "bill_id"))
# # anti_join(ss_bills_auto, bills, by = c("term" = "term", "bill_num" = "bill_id"))
# # filter(bills, bill_id == "hb5025")
# 
# ### Old note to self... ???
# # **** 44 bills in the year range and with multiple bills with same ID
# # **** ---> Adjusted to check each bill, but in theory has higher chance of matching.
# 
# ########################################
# ###### Identify Commemorative Bills
# ######################################
# ## *** DON'T USE: award 
# # bills[grepl("award", tolower(bills$short_title)),]$short_title
# 
# ## ***** MIGHT BE BETTER TO USE THE KEYWORD (before dash in main title --- e.g, naming and designating; )
# 
# ### Removing: "recogni", "designa", "encourag", ---> lots of errors...
# vw_congress_commem <- c("expressing support", "urging", "promoting", "condol", "commemorat", "honor", "memoria",
#                         "congratul",  "public holiday", "rename", "for the private relief of", 
#                         "for the relief of", "medal", "mint coin", "posthumous", "public holiday", "provide for correction",
#                         "to name", "redisgnat", "to remove any doubt", "to rename", "retention of the name")
# additions <- c("anniversary", "awareness", "day.$|week.$|month.$", "dedicat", "festival", "celebrat", "commend")
# 
# 
# title_match <- grepl(paste(c(vw_congress_commem, additions), collapse = "|"), tolower(bills$short_title))
# summary_match <- grepl(paste(c(vw_congress_commem, additions), collapse = "|"), tolower(bills$summary))
# # keyword_match <- grepl(paste(c(vw_congress_commem, additions), collapse = "|"), tolower(bills$keywords))
# 
# bills$commem <- ifelse(title_match | summary_match, 1, 0)
# table(bills$commem)
# # filter(bills, commem == 1) %>% select(short_title)
# 
# rm(title_match, summary_match, keyword_match, additions, vw_congress_commem)
# 
# ###############################################
# #### Function to take a given bill history for VA and code legislative stages
# ###############################################
# 
# # bill_history <- hist_sub
# # this_id <- this_bill_id
# # this_session <- this_session
# # this_term <- this_term
# evaluate_bill_hist <- function(bill_history, this_id, this_session, this_term){
#   
#   ### Lowercase
#   bill_history$action <- tolower(bill_history$action)
#   # bill_history$action <- gsub("\\.", "", bill_history$action)
#   
#   if(nrow(bill_history) > 0){
#     
#     ### Initiating Chamber
#     init_chamber <- ifelse(substring(this_id, 1, 1) == "H", "House", "Senate")
#     other_chamber <- ifelse(init_chamber == "House", "Senate", "House")
#     
#     ### Subset to Introduction Chamber
#     in_chamber <- which(bill_history$chamber == init_chamber)
#     left_chamber <- which(bill_history$chamber == other_chamber)
#     # Adjusting for sporadic outchamber actions before in-chamber action
#     left_chamber <- left_chamber[which(left_chamber > min(in_chamber))]
#     if(length(left_chamber) > 0){
#       chamber_history <- bill_history[bill_history$action_order < min(left_chamber),]
#     }else{
#       chamber_history <- bill_history
#     }
# 
#     ### Action in Committee
#     aic_terms <- c("assigned to .+ sub-comm", "assigned.+ sub:", "reported from", "subcommittee recomm", "subcommittee failed to", 
#                    "tabled in", "failed to report")
#     aic <- max(grepl(paste(aic_terms, collapse = "|"), chamber_history$action))
#     # ** Double check this with Craig/Alan
#     # ** These also indicate ABC, but may also be AIC...
#     
#     ### Action beyond committee -- the chamer specific terms will only matter for each speific chamber history
#     abc_terms <- c("^reported from", "read second time", "read third time", "engrossed", "vote:", "passed house", "passed senate",
#                    "defeated by house", "defeated by senate")
#     abc <- max(grepl(paste(abc_terms, collapse = "|"), chamber_history$action))
#     # * If Passed, then necessarily ABC
#     
#     ### Passed Chamber
#     pc_terms <- c("passed house", "passed senate", "agreed to by house", "agreed to by senate", "vote: passage", "vote: adoption")
#     pc <- max(grepl(paste(pc_terms, collapse = "|"), chamber_history$action))
#     
#     ### Law
#     # *** This will not count companions that became law (which always are preceded by "Comp. became Pub Ch. N")
#     law <- max(grepl("^approved by gov|^acts of assembly chapter text ", bill_history$action))
#   } else {
#     aic <- abc <- pc <- law <- 0
#   }
#   
#   ##### DF to Return
#   bill_stages <- data.frame(bill_id = this_id, session = this_session, term = this_term, 
#                             introduced = 1, action_in_comm = aic, action_beyond_comm = abc, passed_chamber = pc, law = law)
#   return(bill_stages)
#   
# }
# 
# # rm(abc, abc_terms, aic, i, init_chamber, law, pc, pc_terms, this_bill_id)
# 
# ############################
# ### Code Bill Histories
# ############################
# 
# bill_hist <- read.csv("~/Dropbox/Data/State Legislative Data/States/VA/VA_Bill_Histories.csv")
# 
# ## Isolate Term Years -- Most recent eletion was Nov 2017, so terms would be 2016-2017, 2018-2019
# bill_hist$session_year <- as.numeric(gsub(" .+", "", bill_hist$session))
# bill_hist$term <- ifelse(bill_hist$session_year %% 2 == 0, 
#                      paste0(bill_hist$session_year, "-", bill_hist$session_year + 1),
#                      paste0(bill_hist$session_year - 1, "-", bill_hist$session_year))
# 
# ## Standardize Bill IDs
# bill_hist$bill_id <- tolower(paste0(gsub(" .+", "", bill_hist$bill_id), sprintf("%04s", gsub("[^0-9\\.]", "", bill_hist$bill_id))))
# bill_hist$session_bill_id <- paste0(bill_hist$session_year, "-", bill_hist$bill_id)
# 
# ### Output Matrix
# all_bill_stages <- data.frame(matrix(nrow = 0, ncol = 8))
# colnames(all_bill_stages) <- c("bill_id", "session", "term", "introduced", "action_in_comm", "action_beyond_comm", "passed_chamber", "law")
# 
# ### Code Each Bill
# for(i in 1:nrow(bills)){
#   
#   this_bill_id <- bills[i,]$bill_id
#   this_session <- bills[i,]$session
#   this_term <- bills[i, ]$term
#   hist_sub <- filter(bill_hist, bill_id == this_bill_id & session == this_session & this_term == term)
#   
#   ### Code History
#   bill_stages <- evaluate_bill_hist(hist_sub, this_bill_id, this_session, this_term)
#   
#   ### Append to DF
#   all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
#   print(i)
# }
# 
# bills <- left_join(bills, all_bill_stages, by = c("bill_id", "session", "term"))
# 
# rm(this_bill_id, this_session, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist)
# 
# ### Warning to investigate... In min(in_chamber) : no non-missing arguments to min; returning Inf
# 
# 
# ##################################
# #### Aggregate
# ##################################
# # ********************* NOTE: THIS IS WHERE SCRIPT CHANGES VS NON MULTISPONSOR VARIATIONS ******************
# 
# # name_split <- str_split(gsub("\\[|\\]|\\'", "", bills$primary_sponsor), ", ")
# # bills$introducing_sponsor <- tools::toTitleCase(sapply(name_split, head, 1))
# # rm(name_split)
# 
# #### Correct Bill(s) with No Sponsor --- Using Listed Senate or House Sponsors
# # filter(bills, sponsor == "") %>% View()
# first_sponsor <- ifelse(substring(bills$bill_id,1,1) == "h", bills$house_sponsors, bills$senate_sponsors)
# first_sponsor <- gsub("\\[\'|\\(chief patron\\)|,.+", "", first_sponsor)
# first_sponsor <- gsub(" \\'", "", first_sponsor)
# first_sponsor <- str_trim(gsub("]", "", first_sponsor, fixed = TRUE))
# bills$sponsor <- ifelse(bills$sponsor == "", first_sponsor, bills$sponsor)
# 
# #### CORRECT SYSTEMATIC SPONSOR ISSUES
# # sort(table(bills$sponsor))
# bills$sponsor <- gsub("\\'", "", bills$sponsor)
# bills$sponsor <- gsub("-resign.+|\\(resigned\\)|\\(deceased\\)|;resigned|-seat vacat.+|\\(retire.+", "", bills$sponsor)
# 
# #### CORRECT SPONSOR MISSPELLINGS? 
# 
# 
# #### Drop Remaining Issues?
# # filter(bills, sponsor == "[") %>% select(-summary)
# # bills <- filter(bills, sponsor != "[|[]")
# 
# 
# #######################################
# # ### Expand DF to include a Row for each primary sponsor --- SHOULD BE 24108 ROWS
# # bills_multi <- bills %>%
# #   mutate(chamber = substring(bill_id, 1, 1)) %>%
# #   # filter(bill_id == "H0001" & session == "2009-2010 Session") %>%
# #   # filter(bill_id == "H0002" & session == "2009-2010 Session") %>%
# #   filter(!grepl("\\['rules, calendar, and operations of the house'\\]", primary_sponsors)) %>%
# #   mutate(primary_sponsor_list = str_split(gsub("\\[|\\]|\\'", "", primary_sponsors), ", "),
# #          cosponsor_list = str_split(gsub("\\[|\\]|\\'", "", cosponsors), ", "),
# #          num_primary = str_count(primary_sponsor_list, ",") + 1,
# #          num_cosponsors = ifelse(nchar(cosponsor_list) == 0, 0, str_count(cosponsor_list, ",") + 1),
# #          total_sponsors = num_primary + num_cosponsors) %>% 
# #   # summarize(all_spon = sum(total_sponsors))
# #   group_by(session, chamber, bill_id) %>%
# #   slice(rep(1:n(), each = total_sponsors )) %>%
# #   mutate(this_sponsor = ifelse(num_cosponsors == 0, unlist(primary_sponsor_list[1]), c(unlist(primary_sponsor_list[1]), unlist(cosponsor_list[1]))),
# #        sponsor_type = ifelse(num_cosponsors == 0, rep("Primary", num_primary[1]), c(rep("Primary", num_primary[1]), rep("Cosponsor", num_cosponsors[1]))),
# #        sponsor_type = ifelse(1:n() == 1, "Introducing Sponsor", sponsor_type))
# 
# 
# ### Drop bills with non-member sponsors sponsor
# sort(table(bills$sponsor))
# # bills <- filter(bills_multi, !grepl("ethics|agriculture|judiciary subcommittee a|judiciary|health and human services", this_sponsor)) 
# 
# #### Other Variables
# # bills$agg_term <- ifelse(grepl("^2016", bills_multi$session), "2015-2016", gsub(" Session", "", bills_multi$session))
# bills$chamber <- substring(bills$bill_id, 1, 1)
# 
# 
# ###################
# ### Function to Calculate LES Scores --- Could be Simplified to Get Rid of Variations and Standardized to be in a Separate File
# calc_LES <- function(bill_data, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1)){
#   
#   # ***** DOING THESE BY ELECTORAL TERM NOT SESSION **********
#   
#   ### Do Inverse Probability Weighting?
#   inverse_prob <- ifelse(stage_weights[1] == "inverse_prob", TRUE, FALSE)
#   
#   ##### Bill Weights
#   bill_data$bill_weight <- reg_weight
#   bill_data$bill_weight <- ifelse(bill_data$commem == 1, com_weight, bill_data$bill_weight)
#   bill_data$bill_weight <- ifelse(bill_data$SS == 1, ss_weight, bill_data$bill_weight)
#   
#   ### Output DF
#   LES_dat <- data.frame(matrix(nrow = 0, ncol = 10))
#   colnames(LES_dat) <- c("sponsor", "term", "chamber", "LES", "LES_rank", "BILL_wshare", "AIC_wshare", "ABC_wshare", "PASS_wshare", "LAW_wshare")
#   
#   ##### Loop through terms
#   for(t in unique(bill_data$term)){
#     term_bills <- bill_data[bill_data$term == t,]
#     
#     #### Loop though House, Senate
#     for(c in unique(bill_data$chamber)){
#       
#       chamber_term <- term_bills[term_bills$chamber == c,]
#       N <- length(unique(chamber_term$sponsor))
#       
#       ### Sums for each term
#       BILL_denom <- sum(chamber_term$bill_weight * chamber_term$introduced)
#       AIC_denom <- sum(chamber_term$bill_weight * chamber_term$action_in_comm)
#       ABC_denom <- sum(chamber_term$bill_weight * chamber_term$action_beyond_comm)
#       PASS_denom <- sum(chamber_term$bill_weight * chamber_term$passed_chamber)
#       LAW_denom <- sum(chamber_term$bill_weight * chamber_term$law)
#       
#       ###### BY CHAMBER AND TERM: Weight Stages by Inverse Probability of Success
#       if(inverse_prob == TRUE){
#         stage_weights <- rep(1, 5)
#         stage_weights[1] <- 1 / (sum(chamber_term$introduced) / nrow(chamber_term))
#         stage_weights[2] <- 1 / (sum(chamber_term$action_in_comm) / nrow(chamber_term))
#         stage_weights[3] <- 1 / (sum(chamber_term$action_beyond_comm) / nrow(chamber_term))
#         stage_weights[4] <- 1 / (sum(chamber_term$passed_chamber) / nrow(chamber_term))
#         stage_weights[5] <- 1 / (sum(chamber_term$law) / nrow(chamber_term))
#         
#         print(paste0("TERM: ", t, " ---- Chamber: ", c, " ---- Stage Weights: "))
#         print(round(stage_weights, 2))
#       }
#       
#       ##### Loop through members within each chamber-term
#       for(i in unique(chamber_term$sponsor)){
#         member_chamber_term <- chamber_term[chamber_term$sponsor == i,]
#         
#         ### Member-Level Shares for each stage of the process
#         BILL_w <- stage_weights[1] * sum(member_chamber_term$bill_weight * member_chamber_term$introduced)
#         AIC_w <- stage_weights[2] * sum(member_chamber_term$bill_weight * member_chamber_term$action_in_comm)
#         ABC_w <- stage_weights[3] * sum(member_chamber_term$bill_weight * member_chamber_term$action_beyond_comm)
#         PASS_w <- stage_weights[4] * sum(member_chamber_term$bill_weight * member_chamber_term$passed_chamber)
#         LAW_w <- stage_weights[5] * sum(member_chamber_term$bill_weight * member_chamber_term$law)
#         
#         #### Shares
#         BILL_wshare <- BILL_w/BILL_denom
#         AIC_wshare <- AIC_w/AIC_denom
#         ABC_wshare <- ABC_w/ABC_denom
#         PASS_wshare <- PASS_w/PASS_denom
#         LAW_wshare <- LAW_w/LAW_denom
#         
#         #### Summing for Total LES measure --- Note N/5 Adjustment done at each earlier stage
#         # Standard
#         adj_factor <- sum(stage_weights)
#         LES <- N / adj_factor * (BILL_wshare + AIC_wshare + ABC_wshare + PASS_wshare + LAW_wshare)
#         
#         ## Collapsed --- Need ot figure out how this changes the weighting factor...
#         # numerator <- (BILL_w + AIC_w + ABC_w + PASS_w + LAW_w)
#         # denom <- (BILL_w_denom + AIC_w_denom + ABC_w_denom + PASS_w_denom + LAW_w_denom)
#         # LES <- numerator/denom * N
#         
#         #### Record
#         LES_dat <- add_row(LES_dat, sponsor = i, term = t, chamber = c, LES = LES, LES_rank = NA,
#                            BILL_wshare = BILL_wshare, AIC_wshare = AIC_wshare, ABC_wshare = ABC_wshare,
#                            PASS_wshare = PASS_wshare, LAW_wshare = LAW_wshare)
#         
#         print(paste0(t, " --- ", c, " ---- ", i, ": ", LES))
#       }
#     }
#   }
#   
#   ### Calculate Individual Rank Within Each Chamber-Term
#   LES_dat <- LES_dat %>%
#     mutate(chamber = ifelse(chamber == "h", "House", "Senate")) %>%
#     group_by(term, chamber) %>%
#     arrange(desc(LES), .by_group = TRUE) %>%
#     mutate(LES_rank = 1:n())
#   
#   return(LES_dat)
# }
# 
# 
# ##########################################
# #### ESTIMATE LES
# ###########################################
# 
# ### UNTIL SS MERGED IN
# # bills$SS <- 0
# 
# ### Standard LES: Same as Congressional Measure
# LES <- calc_LES(bills, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1))
# # LES_standard %>% group_by(chamber, term) %>% summarize(mean_LES = mean(LES))
# 
# 
# ##############################################
# ###  Supplement with Chamber-Term Variables
# ##############################################
# 
# ####### Agg Variables
# agg_stats <- bills %>%
#   group_by(term, chamber, sponsor) %>%
#   summarize(num_sponsored_bills = n(),
#             sponsor_pass_rate = sum(passed_chamber) / n(),
#             sponsor_hit_rate = sum(law) / n()) %>%
#   ungroup() %>%
#   mutate(chamber = ifelse(chamber == "h", "House", "Senate")) 
# 
# LES <- left_join(LES, agg_stats, by = c("term", "chamber", "sponsor"))
# #LES_auto <- left_join(LES_auto, agg_stats, by = c("term", "chamber", "sponsor"))
# 
# ####################
# ## Compare LES vs LES Automated
# #####################
# 
# 
# both_measures <- left_join(select(LES, sponsor, term, chamber, LES), 
#                            select(LES_auto, sponsor, term, chamber, LES), 
#                            by = c("term", "chamber", "sponsor"))
# 
# ggplot(both_measures, aes(x = LES.x, y = LES.y)) + 
#   geom_point() + 
#   facet_wrap(term ~ chamber) +
#   geom_abline(slope=1, intercept=0, lty = 2)
# ggsave("/Users/PB/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/VA/VA_manual_v_automated_LES.pdf", height = 8, width = 10)
# 
# both_measures %>% 
#   group_by(term, chamber) %>%
#   summarize(cross_corr = cor(LES.x, LES.y))
# 
# # Correlations between .996 and .999....
# 
# 
# #####################
# ### PARSE NAMES
# #####################
# 
# library(purrr)
# 
# legislator_names <- map_df(LES$sponsor, parse_names) %>% 
#   select(-salutation) %>%
#   distinct() %>%
#   mutate(nickname = str_extract(middle_name, '(?<=").*?(?=")'),
#          middle_name = gsub('".+"', '', middle_name),
#          last_name = gsub("[[:punct:]]", "", last_name))
# 
# LES <- left_join(LES, legislator_names, by = c("sponsor" = "full_name"))
# 
# ##############################################
# ###  Save
# ##############################################
# 
# # write.csv(LES, "VA_LES.csv", row.names = FALSE)
# 
# 
# 
# 
# 



