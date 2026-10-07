#####################################
### ESTIMATE EFFECTIVENESS SCORES FOR TN
#####################################

############
#### NOTES:
# EVENTUALLY WILL HAVE TO MERGE S&S BILLS IN
# NEED TO FIX THE COMMEMORATIVE CODING
# NEED TO CORRECT SPONSOR NAME ERRORS BEFORE DOING LES MERGE

#########################
### Questions
# - Distinguish between things that are adopted and become law? Do adopted things (e.g. rules/resolutions) just pass chamber?
# - Will need to address companions in this data --- Count companion passage? It reports it in Senate row if companion swapped in. Eg HB0010 in 109th

rm(list=ls())
options(stringsAsFactors = FALSE, scipen = 100)

library(tidyr)
library(dplyr)
library(stringr)
library(ggplot2)

setwd("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/TN")

#######
## TH Legislative Data
## * 99th to 109th Sessions (1995-2016) -- Scraped from Webpage

bills <- read.csv("~/Dropbox/Data/State Legislative Data/States/TN/TN_Bill_Details_Full.csv")

######
## Substantive and Significant Bills --- Via Craig's RAs --- Collapsing from multiple sheets

# filename <- "~/Dropbox/Data/State Legislative Data/Significant Bills/NC/ALL NC SS BILLS.xlsx"
# sheets <- readxl::excel_sheets(filename)
# ss_bills <- lapply(sheets, function(X) readxl::read_excel(filename, sheet = X))
# ss_bills <- lapply(ss_bills, as.data.frame)
# names(ss_bills) <- gsub("[^0-9\\.]", "", sheets)
# ss_bills <- bind_rows(ss_bills, .id = "year") %>%
#   rename(bill_number = `Bill Number`, sponsor = Sponsor) %>%
#   select(c(year, bill_number, sponsor)) %>%
#   filter(!is.na(bill_number))
# 
# rm(filename, sheets)

######
## Cleaning Data for Easier Matching

## Lowercase Sponsor Names
bills$primary_sponsor <- tolower(bills$sponsor)
bills$cosponsors <- tolower(bills$cosponsors)
#ss_bills$sponsor <- tolower(ss_bills$sponsor)

## Lowercase Descriptions
bills$short_title <- tolower(bills$short_title)
bills$fiscal_summary <- tolower(bills$fiscal_summary)
bills$summary <- tolower(bills$summary)

#### Senator or Rep.
# ss_bills$member_type <- ifelse(grepl("^rep\\.", ss_bills$sponsor ), "Representative", NA)
# ss_bills$member_type <- ifelse(grepl("^sen\\.", ss_bills$sponsor ), "Senator", ss_bills$member_type)

## **** Errors in SS File --- 2015, end of sheet
# ss_bills$sponsor <- gsub("#error!", "", ss_bills$sponsor)

### Create variable with last names
# name_split <- str_split(ss_bills$sponsor, " ")
# ss_bills$sponsor_last_name <- sapply(name_split, tail, 1)
# rm(name_split)

## Isolate Term Years
bills$session_num <- gsub(" .+", "", bills$session)
bills <- bills %>%
  mutate(session_start = recode(session_num, '109th' = 2015, '108th' = 2013, '107th' = 2011, '106th' = 2009, '105th' = 2007, 
                                '104th' = 2005, '103rd' = 2003, '102nd' = 2001, '101st' = 1999, '100th' = 1997, '99th' = 1995),
         session_end = session_start + 1)

###############
#### Subset NC Bills DF
# bills <- filter(bills, bills$session_start > zzzzz)

## Standardize Bill Names and Remove Parenthetical Comparison Bills
# bills$bill_id <- str_trim(gsub("\\(.+", "", bills$bill_id))
# bills$bill_id <- paste0(substring(bills$bill_id, 1, 1), sprintf("%04s", gsub("[^0-9\\.]", "", bills$bill_id)))

# ss_bills <- mutate(ss_bills, bill_number = gsub("SB ", "S", bill_number)) %>%
#   mutate(bill_number = gsub("HB ", "H", bill_number)) %>%
#   mutate(bill_number = paste0(substring(bill_number, 1, 1),
#                               sprintf("%04s", gsub("[^0-9\\.]", "", bill_number)))) %>%
#   arrange(year, bill_number)

# # * Fixing a differently formatted bill_number
# ss_bills[ss_bills$bill_number == "H.164",]$bill_number <- "H0164"


##################################################################################################################################

#############################################
##### Match S&S Bills to Full Set of Bills
#############################################

# # i = 1
# bills$SS <- 0
# 
# for(i in 1:nrow(bills)){
#   bill_range <- c(as.numeric(bills[i,]$min_year), as.numeric(bills[i,]$max_year))
#   bill_sub <- filter(ss_bills, as.numeric(year) %in% bill_range[1]:bill_range[2])
#   bill_sub <- filter(bill_sub, bill_number == bills[i,]$bill_id)
#   if(nrow(bill_sub) > 0){
#     match <- rep(NA, nrow(bill_sub))
#     for(j in 1:nrow(bill_sub)){
#       match[j] <- any(grepl(bill_sub$sponsor_last_name[j], bills[i,]$primary_sponsors))
#     }
#     bills[i,]$SS <- ifelse(any(match) == TRUE, 1, 0)
#   }
#   print(i)
# }
# rm(i, j, match, bill_range, bill_sub, ss_bills)
# **** 44 bills in the year range and with multiple bills with same ID
# **** ---> Adjusted to check each bill, but in theory has higher chance of matching.


########################################
###### Identify Commemorative Bills
######################################
## *** DON'T USE: award 
# bills[grepl("award", tolower(bills$short_title)),]$short_title

## ***** MIGHT BE BETTER TO USE THE KEYWORD (before dash in main title --- e.g, naming and designating; )

### Removing: "recogni", "designa", "encourag", ---> lots of errors...
vw_congress_commem <- c("expressing support", "urging", "promoting", "condol", "commemorat", "honor", "memoria",
                        "congratul",  "public holiday", "rename", "for the private relief of",
                        "for the relief of", "medal", "mint coin", "posthumous", "public holiday", "provide for correction",
                        "to name", "redisgnat", "to remove any doubt", "to rename", "retention of the name")
additions <- c("anniversary", "awareness", "day.$|week.$|month.$", "dedicat", "festival", "celebrat")


title_match <- grepl(paste(c(vw_congress_commem, additions), collapse = "|"), tolower(bills$short_title))
# summary_match <- grepl(paste(c(vw_congress_commem, additions), collapse = "|"), tolower(bills$summary))
# keyword_match <- grepl(paste(c(vw_congress_commem, additions), collapse = "|"), tolower(bills$keywords))

bills$commem <- ifelse(title_match, 1, 0)
table(bills$commem)
filter(bills, commem == 1) %>% select(short_title)

rm(title_match, summary_match, keyword_match, additions, vw_congress_commem)

###############################################
#### Function to take a given bill history for NC and code legislative stages
###############################################

# bill_history <- hist_sub
# this_id <- this_bill_id
# this_session <- this_session
evaluate_bill_hist <- function(bill_history, this_id, this_session){
  
  ### Lowercase
  bill_history$action <- tolower(bill_history$action)
  bill_history$action <- gsub("\\.", "", bill_history$action)
  
  if(nrow(bill_history) > 0){
    
    ### Initiating Chamber
    init_chamber <- ifelse(substring(this_id, 1, 1) == "H", "house", "senate")
    
    ### Subset to CHamber
    # chamber_sub <- grepl(paste0("ready for transmission to ", ifelse(init_chamber == "house", "sen\\.", "house")), bill_history$action)
    chamber_sub <- grepl("engrossed", bill_history$action)
    if(any(chamber_sub) == TRUE){
      first_inst <- max(which(chamber_sub)) # Reverse order means this max is the mention of engrossed (if happens to be multiple) with lowest order number
      chamber_history <- bill_history[bill_history$order <= length(chamber_sub) - first_inst + 1,]
    } else{
      chamber_history <- bill_history
    }
    
    ### Action in Committee
    aic <- max(grepl("placed on s/c cal|rec for pass|recommended for pass|action deferred in|placed on cal.+ comm", chamber_history$action))
    # ** Double check this with Craig/Alan
    # ** These also indicate ABC, but may also be AIC...
    
    ### Action beyond committee -- the chamer specific terms will only matter for each speific chamber history
    abc_terms <- c("^Rec for pass", "recommended for pass", "placed on regular calendar", "^h adopted am", "^senate adopted am",
                   "passed h", "passed senate", "amendment withdrawn", "engrossed")
    abc <- max(grepl(paste(abc_terms, collapse = "|"), chamber_history$action))
    # * If Passed, then necessarily ABC
    
    ### Passed Chamber
    pc_terms <- c("engrossed")
    pc <- max(grepl(paste(pc_terms, collapse = "|"), chamber_history$action))
    
    ### Law
    # *** This will not count companions that became law (which always are preceded by "Comp. became Pub Ch. N")
    law <- max(grepl("^pub ch|signed by governor", bill_history$action))
  } else {
    aic <- abc <- pc <- law <- 0
  }
  
  ##### DF to Return
  bill_stages <- data.frame(bill_id = this_id, session = this_session,
                            introduced = 1, action_in_comm = aic, action_beyond_comm = abc, passed_chamber = pc, law = law)
  return(bill_stages)
  
}

# rm(abc, abc_terms, aic, i, init_chamber, law, pc, pc_terms, this_bill_id)

############################
### Code Bill Histories
############################

bill_hist <- read.csv("~/Dropbox/Data/State Legislative Data/States/TN/TN_Bill_Histories.csv")
# bill_hist$bill_id <- str_trim(gsub("\\(.+", "", bill_hist$bill_id))
# bill_hist$bill_id <- paste0(substring(bill_hist$bill_id, 1, 1), sprintf("%04s", gsub("[^0-9\\.]", "", bill_hist$bill_id)))

### Output Matrix
all_bill_stages <- data.frame(matrix(nrow = 0, ncol = 7))
colnames(all_bill_stages) <- c("bill_id", "session", "introduced", "action_in_comm", "action_beyond_comm", "passed_chamber", "law")

### Code Each Bill
for(i in 1:nrow(bills)){
  
  this_bill_id <- bills[i,]$bill_id
  this_session <- bills[i,]$session
  hist_sub <- filter(bill_hist, bill_id == this_bill_id & session == this_session)
  
  ### Code History
  bill_stages <- evaluate_bill_hist(hist_sub, this_bill_id, this_session)
  
  ### Append to DF
  all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
  print(i)
}

bills <- left_join(bills, all_bill_stages, by = c("bill_id", "session"))

rm(this_bill_id, this_session, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist)


##################################
#### Aggregate
##################################
# ********************* NOTE: THIS IS WHERE SCRIPT CHANGES VS NON MULTISPONSOR VARIATIONS ******************

# name_split <- str_split(gsub("\\[|\\]|\\'", "", bills$primary_sponsor), ", ")
# bills$introducing_sponsor <- tools::toTitleCase(sapply(name_split, head, 1))
# rm(name_split)

#### Correct Bill(s) with No Sponsor
# filter(bills, sponsor == "")
bills[bills$sponsor == "" & bills$bill_id == "HB0835",]$sponsor <- "Ragan"
table(bills$sponsor)

#### CORRECT SPONSOR MISSPELLINGS? 
# ** EG HAILE AND HAILLE? FINNEY AND FINNEY L?

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
sort(table(bills_multi$this_sponsor))
# bills <- filter(bills_multi, !grepl("ethics|agriculture|judiciary subcommittee a|judiciary|health and human services", this_sponsor)) 

#### Other Variables
# bills$agg_term <- ifelse(grepl("^2016", bills_multi$session), "2015-2016", gsub(" Session", "", bills_multi$session))
bills$chamber <- substring(bills$bill_id, 1, 1)

###################
### Function to Calculate LES Scores --- Could be Simplified to Get Rid of Variations and Standardized to be in a Separate File
calc_LES <- function(bill_data, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1)){
  
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
  for(t in unique(bill_data$session_num)){
    term_bills <- bill_data[bill_data$session_num == t,]
    
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
    mutate(chamber = ifelse(chamber == "H", "House", "Senate")) %>%
    group_by(term, chamber) %>%
    arrange(desc(LES), .by_group = TRUE) %>%
    mutate(LES_rank = 1:n())
  
  return(LES_dat)
}


##########################################
#### ESTIMATE LES
###########################################

### UNTIL SS MERGED IN
# bills$SS <- 0

### Standard LES: Same as Congressional Measure
LES <- calc_LES(bills, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1))
# LES_standard %>% group_by(chamber, term) %>% summarize(mean_LES = mean(LES))


##############################################
###  Supplement with Chamber-Term Variables
##############################################

####### Agg Variables
agg_stats <- bills %>%
  group_by(session_num, chamber, sponsor) %>%
  summarize(num_sponsored_bills = n(),
            sponsor_pass_rate = sum(passed_chamber) / n(),
            sponsor_hit_rate = sum(law) / n()) %>%
  ungroup() %>%
  mutate(chamber = ifelse(chamber == "H", "House", "Senate")) %>%
  rename(term = session_num)

LES <- left_join(LES, agg_stats, by = c("term", "chamber", "sponsor"))


##############################################
###  Save
##############################################

# write.csv(LES, "TN_LES.csv", row.names = FALSE)








