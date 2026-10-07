###########################################
### ESTIMATE EFFECTIVENESS SCORES FOR MI
###########################################

rm(list=ls())
options(stringsAsFactors = FALSE, scipen = 100)

library(tidyr)
library(dplyr)
library(stringr)
library(ggplot2)
library(humaniformat)
library(purrr)
library(readxl)
library(readr)

# dir.create("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/MI")
setwd("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/MI")

#######
## Legislative Data
## 1997-1998 (89th) to Present; Currently in the 100th State Legislature (2019-2020) 
bill_files <- list.files("~/Dropbox/Data/State Legislative Data/States/MI", full.names = TRUE)
bill_files <- bill_files[grepl("Bill_Details", bill_files)]
bills <- lapply(bill_files, read_csv) %>%
  bind_rows() 
rm(bill_files)

## Fixing temporary issue until rescrape
#bills <- filter(bills, !(term == "2015-2016" & grepl("^2017|^2018", bill_num)))

######
## Substantive and Significant Bills --- Via Craig's RAs --- Collapsing from multiple sheets
######

##### Manual
mi_files <- list.files("~/Dropbox/Data/State Legislative Data/Significant Bills/Manual_SS_Data/MI", full.names = TRUE)
ss_bills <- map2_df(mi_files, gsub("^.+Manual_SS_Data/[A-Z]+/", "", mi_files), ~read_excel(.x, skip = 0) %>% mutate(id = .y)) %>%
  select(1:4) %>%
  rename(bill_num = `Bill #`, sponsor = Sponsor, doc_number = `Doc #`, state_file = id) %>% 
  filter(!is.na(bill_num)) %>%
  mutate(session_year = as.numeric(gsub("MI_SS_BILLS_|.xlsx", "", state_file)), 
         term = ifelse(session_year %% 2 == 1, paste0(session_year, "-", session_year + 1), paste0(session_year - 1, "-", session_year)),
         num_only = str_extract(bill_num, "\\d++"),
         bill_type = gsub('\\.| [0-9].+$| [0-9]|[0-9].+$|[0-9]', '', gsub(" +", " ", bill_num)),
         bill_num_z = tolower(paste0(gsub('\\.| +[0-9].+$| [0-9].+$| [0-9]|[0-9].+$|[0-9]', '', gsub(" +", " ", bill_num)), 
                                     str_pad(str_extract(bill_num, "\\d++"), 4, pad= 0)))) %>%
  distinct(bill_num_z, session_year, .keep_all = TRUE)

##### Automated
ss_bills_auto <- read.csv("~/Dropbox/Data/State Legislative Data/Significant Bills/SS_Data/MI_SS_Bills.csv")
ss_bills_auto <- mutate(ss_bills_auto, term = ifelse(year %% 2 == 1, paste0(year, "-", year + 1), paste0(year - 1, "-", year)))

#################
## Cleaning Data for Easier Matching
################

## Primary Sponsor always listed first (I didn't scrape the primary parentheses note though)
## ** Only issue would be if multiple primary, but per NC, we only really want one...
bills$sponsors <- tolower(bills$sponsors)
bills$primary_sponsor <- str_trim(gsub(";.+$", "", bills$sponsors))

## Lowercase Descriptions
bills$summary <- str_trim(gsub('\r\n', ' ', tolower(bills$summary)))
bills$keywords <- tolower(bills$keywords)

## Isolate Term Years -- Most recent eletion was Nov 2017, so terms would be 2016-2017, 2018-2019
bills$session_year <- as.numeric(gsub('-.+', '', bills$bill_number))
bills <- rename(bills, term = session, bill_num = bill_number)

## Standardize Bill IDs ---- NOTE HJR's from 1997 are Letters...
bill_parts <- do.call(rbind, str_split(bills$bill_num, '-'))
bills$bill_num_z <- paste0(tolower(bill_parts[,2]), sprintf("%04s", bill_parts[,3]))
rm(bill_parts)

###############
#### Subset MI Bills DF TO Overlap with SS Data

bills <- filter(bills, bills$session_year >= 2007)


#############################################
##### Match S&S Bills to Full Set of Bills
#############################################

# ***** THis should probably be a left_join but need to be careful about double matches? ******
# ----> Loop is better generally, but should work in this case based on how bills increment within terms
# ---> Specifically: bills increment and do not repeat within 2-year term, so matching within term without account for specific years is fine
# ---> e.g, 2007-HB-5605, 2008-HB-5606

bills$SS <- 0
bills$SS_auto <- 0

for(i in 1:nrow(bills)){
  bill_range <- as.numeric(unlist(str_split(bills[i,]$term, "-")))
  #### Manually Coded
  bill_sub <- filter(ss_bills, session_year %in% bill_range[1]:bill_range[2])
  bill_sub <- filter(bill_sub, bill_num_z == bills[i,]$bill_num_z)
  if(nrow(bill_sub) > 0){
    bills[i,]$SS <- 1
  }
  #### Auto Coded
  auto_sub <- filter(ss_bills_auto, year %in% bill_range[1]:bill_range[2])
  auto_sub <- filter(auto_sub, bill_num == bills[i,]$bill_num_z)
  if(nrow(auto_sub) > 0){
    bills[i,]$SS_auto <- 1
    #ss_bills_auto[auto_sub$index,]$matched <- ss_bills_auto[auto_sub$index,]$matched + 1
  }
  print(i)
}
rm(i, bill_range, bill_sub, auto_sub) # match, j,

table(bills$SS_auto)
table(bills$SS)

### What doesn't Match?
# anti_join(ss_bills, bills, by = c("term", "bill_num_z"))
# anti_join(ss_bills_auto, bills, by = c("term" = "term", "bill_num" = "bill_num_z" ))
# filter(bills, grepl("6854", bill_num))

########################################
###### Identify Commemorative Bills
######################################
## *** DON'T USE: award 
# bills[grepl("award", tolower(bills$summary)),]$summary

### Removing: "recogni", "designa", "encourag", ---> lots of errors..., public holiday --> Holiday
vw_congress_commem <- c("expressing support", "urging", "promoting", "condol", "commemorat", "honor", "memoria",
                        "congratul",  "holiday", "rename", "for the private relief of", 
                        "for the relief of", "medal", "mint coin", "posthumous", "public holiday", "provide for correction",
                        "to name", "redisgnat", "to remove any doubt", "to rename", "retention of the name")
additions <- c("anniversary", "awareness", "day in the state of mich",  "week in the state of mich", "month in the state of mich",
               "dedicat", "festival", "celebrat", "commend", "tribute")


# title_match <- grepl(paste(c(vw_congress_commem, additions), collapse = "|"), tolower(bills$short_title))
summary_match <- grepl(paste(c(vw_congress_commem, additions), collapse = "|"), tolower(bills$summary))
keyword_match <- grepl(paste(c(vw_congress_commem, additions), collapse = "|"), tolower(bills$keywords))

bills$commem <- ifelse(summary_match | summary_match, 1, 0)
table(bills$commem)
# filter(bills, commem == 1) %>% select(short_title)

rm(title_match, summary_match, keyword_match, additions, vw_congress_commem)

###############################################
#### Function to take a given bill history for VA and code legislative stages
###############################################

# In MI: USE THE JOURNAL INDICATORS (HJ OR SJ )to identify when the bill switches chambers

# ****** NEED TO VALIDATE THIS ----> ACTIONS MAY NOT BE PERFECT...

# bill_history <- hist_sub
# this_id <- this_bill_num
# this_session <- this_session
# this_term <- this_term
evaluate_bill_hist <- function(bill_history, this_id, this_session, this_term){
  
  ### Lowercase
  bill_history$action <- tolower(bill_history$action)
  # bill_history$action <- gsub("\\.", "", bill_history$action)
  
  if(nrow(bill_history) > 0){
    
    ### Initiating Chamber
    init_chamber <- ifelse(substring(this_id, 6, 6) == "H", "House", "Senate")
    other_chamber <- ifelse(init_chamber == "House", "Senate", "House")
    # ---> This adjusts for NAs, so will only cutoff until first outchamber mention, not first break in in-chamber
    
    ### Subset to Introduction Chamber
    in_chamber <- which(bill_history$chamber == init_chamber)
    if(length(in_chamber) == 0){
      print(bill_history)
    }
    left_chamber <- which(bill_history$chamber == other_chamber)
    
    # Adjusting for sporadic outchamber actions before in-chamber action
    left_chamber <- left_chamber[which(left_chamber > min(in_chamber))]
    if(length(left_chamber) > 0){
      chamber_history <- bill_history[bill_history$order < min(left_chamber),]
    }else{
      chamber_history <- bill_history
    }

    ### Action in Committee
    # -- Don't just use reported or will pickup reporte by comm of whole
    aic_terms <- c("reported with", "reported fav", "recommendation concurred in", "committee recom")
    aic <- max(grepl(paste(aic_terms, collapse = "|"), chamber_history$action))
    # ** Double check 
    
    ### Action beyond committee -- the chamer specific terms will only matter for each speific chamber history
    abc_terms <- c("^reported with", "rules suspended", "placed on order of",  "read a second time", 'ref .+ second read', "third reading",
                   "roll call", "substitute .+ adopted")
    abc <- max(grepl(paste(abc_terms, collapse = "|"), chamber_history$action))
    # * If Passed, then necessarily ABC
    
    ### Passed Chamber
    # --> enrolled if passed by both chambers --- Enrolled and vetoed will never be in chamber_history though
    pc_terms <- c("^passed", "^adopted", "enrolled", "veto")
    pc <- max(grepl(paste(pc_terms, collapse = "|"), chamber_history$action))
    if(pc == 0 & nrow(bill_history) > nrow(chamber_history)){
      pc <- 1
    }
    
    ### Can't Pass Chamber without Action Beyond Committee
    if(abc == 0 & pc == 1){
      abc <- 1
    }
    
    ### Law
    # *** This will not count companions that became law (which always are preceded by "Comp. became Pub Ch. N")
    law <- max(grepl("^approved by the gov|^assigned pa [0-9]", bill_history$action))
  } else {
    aic <- abc <- pc <- law <- 0
  }
  
  ##### DF to Return
  bill_stages <- data.frame(bill_num = this_id, session = this_session, term = this_term, 
                            introduced = 1, action_in_comm = aic, action_beyond_comm = abc, passed_chamber = pc, law = law)
  return(bill_stages)
  
}

# rm(abc, abc_terms, aic, i, init_chamber, other_chamber, law, pc, pc_terms, this_bill_num, left_chamber, in_chamber, aic_terms, this_id, this_session, this_term)

############################
### Code Bill Histories
############################

bill_files <- list.files("~/Dropbox/Data/State Legislative Data/States/MI", full.names = TRUE)
bill_files <- bill_files[grepl("Bill_Hist", bill_files)]
bill_hist <- lapply(bill_files, read_csv) %>% bind_rows() 
rm(bill_files)

## Change COlumn Names to Align with BILL DF
bill_hist <- rename(bill_hist, term = session, bill_num = bill_number)
bill_hist$session <- gsub("-[A-Z].+$", '', bill_hist$bill_num)

## Standardize Bill IDs
# bill_parts <- do.call(rbind, str_split(bill_hist$bill_num, '-'))
# bill_hist$bill_num_z <- paste0(tolower(bill_parts[,2]), sprintf("%04s", bill_parts[,3]))
# rm(bill_parts)

### TO Lowercase
bill_hist$action <- tolower(bill_hist$action)

### Standardize Journal Page Data
unique(bill_hist$chamber)
bill_hist$journal_page <- gsub("Expected in ", "", bill_hist$journal_page)

### Set Chamber
bill_hist$chamber <- substring(bill_hist$journal_page, 1, 1)
bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate")
  
### Output Matrix
all_bill_stages <- data.frame(matrix(nrow = 0, ncol = 8))
colnames(all_bill_stages) <- c("bill_num", "session", "term", "introduced", "action_in_comm", "action_beyond_comm", "passed_chamber", "law")

### Code Each Bill
for(i in 1:nrow(bills)){
  
  this_bill_num <- bills[i,]$bill_num
  this_session <- bills[i,]$session_year
  this_term <- bills[i, ]$term
  hist_sub <- filter(bill_hist, bill_num == this_bill_num & session == this_session & this_term == term)
  
  ### Code History
  bill_stages <- evaluate_bill_hist(hist_sub, this_bill_num, this_session, this_term)
  
  ### Append to DF
  all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
  print(i)
}

bills <- left_join(rename(bills, session = session_year), all_bill_stages, by = c("bill_num", "session", "term"))

rm(this_bill_num, this_session, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist)

### Warning to investigate... In min(in_chamber) : no non-missing arguments to min; returning Inf


##################################
#### Aggregate
##################################

#### Correct Bill(s) with No Sponsor --- Using Listed Senate or House Sponsors
# filter(bills, sponsor == "") %>% View()
bills$sponsor <- bills$primary_sponsor

### *************** FIX NAME ISSUES **************
# --> E.g. Bettie Scott and Bettie Cook Scott
names(sort(table(bills$sponsor)))[grep("scott", names(sort(table(bills$sponsor))))]

### Drop bills with non-member sponsors sponsor
# bills <- filter(bills, !grepl("ethics|agriculture|judiciary subcommittee a|judiciary|health and human services", this_sponsor)) 

#### Other Variables
# bills$agg_term <- ifelse(grepl("^2016", bills_multi$session), "2015-2016", gsub(" Session", "", bills_multi$session))
bills$chamber <- tolower(substring(bills$bill_num, 6, 6))


###################
### Function to Calculate LES Scores --- Could be Simplified to Get Rid of Variations and Standardized to be in a Separate File
calc_LES <- function(bill_data, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1), auto = FALSE){
  
  # ***** DOING THESE BY ELECTORAL TERM NOT SESSION **********
  
  ### Do Inverse Probability Weighting?
  inverse_prob <- ifelse(stage_weights[1] == "inverse_prob", TRUE, FALSE)
  
  ##### Bill Weights
  bill_data$bill_weight <- reg_weight
  bill_data$bill_weight <- ifelse(bill_data$commem == 1, com_weight, bill_data$bill_weight)
  if(auto == TRUE){
    bill_data$bill_weight <- ifelse(bill_data$SS_auto == 1, ss_weight, bill_data$bill_weight)    
  } else{
    bill_data$bill_weight <- ifelse(bill_data$SS == 1, ss_weight, bill_data$bill_weight)
  }
  
  ### Output DF
  LES_dat <- data.frame(matrix(nrow = 0, ncol = 10))
  colnames(LES_dat) <- c("sponsor", "term", "chamber", "LES", "LES_rank", "BILL_wshare", "AIC_wshare", "ABC_wshare", "PASS_wshare", "LAW_wshare")
  
  ##### Loop through terms
  for(t in unique(bill_data$term)){
    term_bills <- bill_data[bill_data$term %in% t,]
    
    #### Loop though House, Senate
    for(c in unique(bill_data$chamber)){
      
      chamber_term <- term_bills[term_bills$chamber %in% c,]
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
        member_chamber_term <- chamber_term[chamber_term$sponsor %in% i,]
        
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

### UNTIL SS MERGED IN
# bills$SS <- 0

### Standard LES: Same as Congressional Measure
LES <- calc_LES(bills, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1))
# LES_standard %>% group_by(chamber, term) %>% summarize(mean_LES = mean(LES))
# LES_auto <- calc_LES(bills, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1), auto = TRUE)

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
#LES_auto <- left_join(LES_auto, agg_stats, by = c("term", "chamber", "sponsor"))

####################
## Compare LES vs LES Automated
#####################

both_measures <- left_join(select(LES, sponsor, term, chamber, LES), 
                           select(LES_auto, sponsor, term, chamber, LES), 
                           by = c("term", "chamber", "sponsor"))

ggplot(both_measures, aes(x = LES.x, y = LES.y)) + 
  geom_point() + 
  facet_wrap(term ~ chamber) +
  geom_abline(slope=1, intercept=0, lty = 2)
ggsave("/Users/PB/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/MI/MI_manual_v_automated_LES.pdf", height = 8, width = 10)

both_measures %>% 
  group_by(term, chamber) %>%
  summarize(cross_corr = cor(LES.x, LES.y))

# Correlations between .996 and .999....


#####################
### PARSE NAMES
#####################

library(purrr)

legislator_names <- map_df(LES$sponsor, parse_names) %>% 
  select(-salutation) %>%
  distinct() %>%
  mutate(nickname = str_extract(middle_name, '(?<=").*?(?=")'),
         middle_name = gsub('".+"', '', middle_name),
         last_name = gsub("[[:punct:]]", "", last_name))

LES <- left_join(LES, legislator_names, by = c("sponsor" = "full_name"))

##############################################
###  Save
##############################################

# write.csv(LES, "VA_LES.csv", row.names = FALSE)








