################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** DELAWARE *** BY SESSION
##############################################################

# *** IF re-scrape: fix the secondary sponsors gathering... no seperator (in 2012_2014 at least, weird spacing; fine in code, but odd)

###########
#### QUESTIONS
# (1) How to deal with the two substitute bills where the sponsor changes????

###################################
## (SPECIAL) SESSIONS:
## ---- Special Sessions permitted, but bills appear to carry over 
## MEMBER LISTS:
## ---- Inidividual member profiles all follow a similar pattern (but no easy list) -- see notes during cleaning at bottom
## PROCESS/RULES:
## ---- Glossary of Terms: https://legis.delaware.gov/Resources/GlossaryOfTerms
## Sponsorship/Authorship
## ---- 1 Primary Sponsor; Co-prime sponsors permitted but seperated out; Additional cosponsors also allowed.
###########################
## NOTES:
## (1) ACCOUNTING For SUBSTITUTES Recorded on Seperate pages as HS and SS
## --> So, SB10 will be substitute with new text and recorded as SS1 for SB10
## --> Coding rule: So long as sponsors match (the bills appear to usually (always?) be identical), tracking as a continuation of SB10
## ** SEE: SB344 -- http://legis.delaware.gov/BillDetail?LegislationId=14277
## ** AND: SS1 for SB344 -- http://legis.delaware.gov/BillDetail?LegislationId=14066
## (2) Sessions sometimes start in DECEMBER of election year, hence, e.g., 1998_2000 
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
library(foreach)
library(inexact)
library(tibble)

this_state <- 'DE'
keep_types <- c('HB', 'SB')

#### Output Directory
# dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd(glue("../../LES_By_State/{this_state}"))

data_files <- list.files(glue("../../../State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
sessions <- gsub(".+Bill_Details_|.csv", '', bill_files)
rm(data_files, bill_files)

### Terms/Sessions and Filespaths
terms <- 2021
t = terms
t_plus_one = t + 1
save_yrs <- as.character(glue('{as.numeric(terms)}_{as.numeric(terms) + 1}'))
t_yrs <- as.character(glue('{as.numeric(terms)-1}_{as.numeric(terms) + 1}'))
t_sessions = sessions[startsWith(sessions,as.character(t-1))]

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Terms/Sessions and Filespaths
# data_files <- list.files(glue("~/Dropbox/Data/State Legislative Data/States/{this_state}"), full.names = TRUE)
# bill_files <- data_files[grepl('Bill_Details', data_files)]
# terms <- gsub('.+Bill_Details_|.csv', '', bill_files)
# rm(data_files, bill_files)

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{save_yrs}.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- bind_rows(read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t_plus_one}.csv")))

SS_bills <- SS_bills %>% 
  filter(State == this_state) %>%
  rename(bill_id = Bill.No) %>%
  mutate(Date = gsub("Sept","Sep",Date),
         date = as.Date(gsub("\\.","",Date), "%B %d, %Y"),
         year = as.integer(format(date, "%Y")),
         term = ifelse(year %% 2 == 1, paste0(year, "_", year + 1), paste0(year - 1, "_", year)), 
         bill_id = toupper(bill_id),
         bill_id = gsub(' ','',bill_id),
         bill_id = paste0(gsub("[0-9].+", '', bill_id), str_pad(gsub("^[A-Z]+", "", bill_id), 4, pad = "0")),
         SS = 1) %>%
  select(state = State, term, year, bill_id, everything())

#### Check SS BIll Types ---> Correct to ensure standardized with Data Format
# table(SS_bills$bill_type)

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- election_years[2]



### Formulate 2-year terms -- Cover both regular and special sessions
# **** Saved as, eg, 1998_2000 because terms occassionally start in December but for standardization, saving as, eg, 1999_2000
#t_sessions <- sessions[grepl(glue('{t}|{t+1}|{t+2}|{t+3}'), sessions)]

### Skip Previously Estimated
if(glue("{this_state}_LES_{save_yrs}.csv") %in% list.files('.')){
  print(glue('. \n ~~~~ SKIPPING {save_yrs} session ---> LES scores already estimated! \n.'))
  next
}

###### SESSION IN PROGRESS
print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {save_yrs} TERM! ~~~~~~~~~~~~~~ '))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_yrs}.csv")
bills <- read.csv(bill_path)

### Clean Term/Session Variables
bills <- bills %>%
  rename(ga_num = session_num) %>%
  mutate(term = save_yrs,
         session = save_yrs)

### Drop duplicates
bills <- distinct(bills)

######## Standardize the Bill IDs
bills <- bills %>%
  rename(bill_id = bill_number) %>%
  mutate(bill_id = paste0(gsub(' [0-9].+| [0-9]+', '', bill_id), str_pad(gsub('.+ ', '', bill_id), 4, pad = '0')),
         parent_bill = ifelse(parent_bill == '', '', paste0(gsub(' [0-9].+| [0-9]+', '', parent_bill), str_pad(gsub('.+ ', '', parent_bill), 4, pad = '0'))))

######### Create ID for Substitute Record
# --- Is NOT NA when a bill was substituted and all subsequent records moved to a new bill page (but kept same sponsor)
# TWO PROBLEM BILLS (e.g., different sponsors):
# -- SB 42 (2005-2006) -- https://legis.delaware.gov/BillDetail/16755
# -- HB 421 (2007-2008) -- https://legis.delaware.gov/BillDetail/18415
bills$substitute_id <- NA
options(warn = 2)
for(i in 1:nrow(bills)){
  if(bills[i,]$bill_id %in% bills$parent_bill){
    bills[i,]$substitute_id <- paste(bills[bills$parent_bill == bills[i,]$bill_id,]$bill_id, collapse = "; ")
    if(any(!(bills[bills$parent_bill == bills[i,]$bill_id,]$sponsor %in% bills[i,]$sponsor))){
      if(bills[bills$parent_bill == bills[i,]$bill_id,]$sponsor == ''){
        ## Only One: "HB0187" in "2012_2014" # --> Sponsor name is missing but everything else is basically identical
        next
      }else if(paste(bills[i,]$bill_id, bills[i,]$term, sep="-") %in% c('SB0042-2005_2006', "HB0421-2007_2008")){
        ### For both, sponsor of substitute != sponsor of bill... Rare, but not clear how to adjust these systematically...
        next
      }else{
        print(' ***** SUBSTITTUE BILL SPONSOR DOES NOT MATCH ORIGINAL BILL SPONSOR ****** ')
        break
      }
    }
  }
}; options(warn = 1)

############### Drop Resolutions, Messages, Communications, Reports
all_bills <- bills
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) 

##########################
####### Standardize Sponsors

bills$sponsor <- tolower(bills$sponsor)
bills$sponsor <- gsub('á', 'a', bills$sponsor)
bills$sponsor <- gsub('é', 'e', bills$sponsor)
bills$sponsor <- gsub('ó', 'o', bills$sponsor)
bills$sponsor <- gsub('í', 'i', bills$sponsor)
bills$sponsor <- gsub('ñ', 'n', bills$sponsor)

bills$secondary_sponsors <- tolower(bills$secondary_sponsors)
bills$secondary_sponsors <- gsub('á', 'a', bills$secondary_sponsors)
bills$secondary_sponsors <- gsub('é', 'e', bills$secondary_sponsors)
bills$secondary_sponsors <- gsub('ó', 'o', bills$secondary_sponsors)
bills$secondary_sponsors <- gsub('í', 'i', bills$secondary_sponsors)
bills$secondary_sponsors <- gsub('ñ', 'n', bills$secondary_sponsors)

bills$cosponsors <- str_trim(tolower(bills$cosponsors))
bills$cosponsors <- gsub('á', 'a', bills$cosponsors)
bills$cosponsors <- gsub('é', 'e', bills$cosponsors)
bills$cosponsors <- gsub('ó', 'o', bills$cosponsors)
bills$cosponsors <- gsub('í', 'i', bills$cosponsors)
bills$cosponsors <- gsub('ñ', 'n', bills$cosponsors)

### LES Sponsor Var
bills <- rename(bills, LES_sponsor = sponsor)
table(bills$LES_sponsor)

### Chamber Cosponsors
bills$house_all_primary <- str_extract(bills$secondary_sponsors, "rep..+|reps..+")
bills$house_all_primary <- gsub('reps\\. |rep\\. |---$', '', gsub("---.+|sen\\..+|sens\\..+", "", bills$house_all_primary))
bills$house_all_primary <- ifelse(is.na(bills$house_all_primary), '', bills$house_all_primary)
bills$house_all_primary <- gsub('  +', ' ', gsub(',', ', ', bills$house_all_primary))
bills$senate_all_primary <- str_extract(bills$secondary_sponsors, "sen\\..+|sens\\..+")
bills$senate_all_primary <- gsub('sens\\. |sen\\. |---$', '', gsub("---.+|rep\\..+|reps\\..+", "", bills$senate_all_primary))
bills$senate_all_primary <- ifelse(is.na(bills$senate_all_primary), '', bills$senate_all_primary)
bills$senate_all_primary <- gsub('  +', ' ', gsub(',', ', ', bills$senate_all_primary))

bills$house_cosponsors <- str_extract(bills$cosponsors, "rep..+|reps..+")
bills$house_cosponsors <- gsub('reps\\. |rep\\. |---$', '', gsub("---.+|sen\\..+|sens\\..+", "", bills$house_cosponsors))
bills$house_cosponsors <- ifelse(is.na(bills$house_cosponsors), '', bills$house_cosponsors)
bills$senate_cosponsors <- str_extract(bills$cosponsors, "sen\\..+|sens\\..+")
bills$senate_cosponsors <- gsub('sens\\. |sen\\. |---$', '', gsub("---.+|rep\\..+|reps\\..+", "", bills$senate_cosponsors))
bills$senate_cosponsors <- ifelse(is.na(bills$senate_cosponsors), '', bills$senate_cosponsors)

bills$all_chamber_sponsors <- ifelse(substring(bills$bill_id,1,1) == "H", 
                                     paste(bills$LES_sponsor, bills$house_all_primary, bills$house_cosponsors, sep = ', '),
                                     paste(bills$LES_sponsor, bills$senate_all_primary, bills$senate_cosponsors, sep = ', '))
bills$all_chamber_sponsors <- gsub(',$', '', gsub(', ,', ',', str_trim(bills$all_chamber_sponsors)))
bills$all_chamber_sponsors <- str_trim(gsub('  +', ' ', gsub(',', ', ', bills$all_chamber_sponsors)))
bills <- select(bills, -c(house_all_primary, house_cosponsors, senate_all_primary, senate_cosponsors))

##### Fix Duplicate Last Names without initial
if(save_yrs == '2003_2004'){
  bills[bills$LES_sponsor == 'ennis',]$LES_sponsor <- "b. ennis" # Bruce Ennis (as opposed to D. Ennis)
  bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors <- gsub('^ennis', 'b. ennis', bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors)
  bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors <- gsub(', ennis', ', b. ennis', bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors)
}
if(save_yrs %in% c('2003_2004', '2005_2006', '2007_2008')){
  bills[bills$LES_sponsor == 'smith',]$LES_sponsor <- "w. smith" # Wayne Smith (as opposed to M. Smith)
  bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors <- gsub('^smith', 'w. smith', bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors)
  bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors <- gsub(', smith', ', w. smith', bills[substring(bills$bill_id,1,1) == "H",]$all_chamber_sponsors)
}
# filter(all_sponsors, grepl("smith", LES_sponsor))
# filter(klarner, grepl("smith", cand)) %>% select(cand, candid, year, sen, outcome, ddez) %>% filter(year == 2002)

###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "" | is.na(LES_sponsor)) %>% View()
if(nrow(filter(bills, LES_sponsor == '')) > 0 | any(is.na(bills$LES_sponsor))){
  cat('\n')
  cat(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == '')) + sum(is.na(bills$LES_sponsor))} bill(s) without a sponsor"))
  bills <- filter(bills, !(LES_sponsor == '' | is.na(LES_sponsor)))     
}

###################
###### Merge in S&S Bills
###################
# *** For DELAWARE: Bills Carryover, Special Sessions folded into main biennial term


if(t_yrs == "2018_2020"){
  SS_bills$bill_id[SS_bills$bill_id=="HB10001"] = "HB0001"
  SS_bills$bill_id[SS_bills$bill_id=="HB20002"] = "HB0002"
  SS_bills$bill_id[SS_bills$bill_id=="HB50005"] = "HB0005"
}
if(t_yrs == "2020_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="SB70007"] = "SB0007"
  SS_bills$bill_id[SS_bills$bill_id=="SB60006"] = "SB0006"
  SS_bills$bill_id[SS_bills$bill_id=="SB30003"] = "SB0003"
  SS_bills$bill_id[SS_bills$bill_id=="SB50005"] = "SB0005"
  SS_bills$bill_id[SS_bills$bill_id=="SB80008"] = "SB0008"
  SS_bills$bill_id[SS_bills$bill_id=="SB60006"] = "SB0006"
  SS_bills$bill_id[SS_bills$bill_id=="SB10001"] = "SB0001"
}

SS_term <- SS_bills %>%
  filter(term == save_yrs) %>%
  mutate(session = save_yrs) %>%
  distinct(term, session, bill_id, SS, Title)


missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS, year,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,sponsor), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",sponsor, ignore.case=T)) %>%
  arrange(sponsor) 
missing_SS_bills$bill_id


# now check to see if there are duplicate joins


duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills %>% mutate(year = as.integer(substr(session,1,4))), 
            by = c("bill_id", "term","session")) %>%
  group_by(rowname) %>% mutate(count = n()) %>%
  ungroup() %>% 
  select(count,bill_id,term,session, Title, title) %>% 
  arrange(desc(count),bill_id)

if(nrow(duplicate_SS_bills %>% filter(count > 1)) > 0 ){
  print("you have duplicates"); SS_duplicates_exist <- 1; # break
} else{
  print("no duplicates")
  SS_duplicates_exist <- 0
}

# if you have duplicated bills, you have to go into this if statement. otherwise, do the else logic. 

if(nrow(duplicate_SS_bills %>% filter(count > 1)) > 0 ){
  # now have to remove the duplicated bills that don't correspond
  write.csv(duplicate_SS_bills, glue("../../../State Legislative Data/States/{this_state}/{this_state}_duplicate_SS_bills_{t_yrs}.csv"), row.names=F)
  # edit this file in Excel, create a column called filter, put in the value "remove" if the Title from PVS doesn't match the bill description
  SS_term = read.csv(glue("../../../State Legislative Data/States/{this_state}/{this_state}_duplicate_SS_bills_{t_yrs}_edited.csv")) %>%
    filter(filter != "remove") %>%
    select(bill_id,term,session) %>% mutate(SS = 1) %>% distinct()
  
  
  
  bills <- bills %>% 
    left_join(SS_term , by = c("bill_id", "term", "session")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS)) %>% 
    distinct()
} else {
  orig_row_n = c(nrow(bills),nrow(SS_term))
  bills <- bills %>% 
    left_join(SS_term %>% select(-Title), by = c("bill_id", "term","session")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS)) %>% 
    distinct()
  SS_term = SS_term %>% 
    left_join(bills %>% select(bill_id,term,session),by=c("bill_id","term","session"))
  if(!identical(c(nrow(bills),nrow(SS_term)),orig_row_n )){print("merge failed"); break}
}

rm(all_bills, missing_SS_bills, duplicate_SS_bills)

### Check Missing
table(bills$SS)
SS_in_bills = sum(bills$SS)
SS_in_PVS = nrow(SS_term %>%
                   mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
                   filter(bill_type %in% keep_types) )

if(SS_in_bills == SS_in_PVS){
  print("all SS merged properly")
} else {
  print(glue("{SS_in_PVS} S&S bills in original dataset, but {SS_in_bills} S&S in our bills dataset"))
  
  # stuff in PVS, not in bills
  anti_join(SS_term, bills, by = c("bill_id", "term", "session"))
  
  # PVS bills that are duplicated in bills. not necessarily a problem!
  print(SS_term %>% group_by(bill_id, term) %>%
    mutate(count = n()) %>% filter(count > 1) %>% arrange(bill_id))
  
  # see if they are equal after removing double-counted bills. this is a crude proxy, you should examine more closely
  all.equal(SS_in_bills + nrow(anti_join(SS_term, bills, by = c("bill_id", "term", "session")))+
              nrow(SS_term %>% group_by(bill_id, term) %>%
                     mutate(count = n()) %>% filter(count > 1))/2,
            nrow(SS_term ))
}

#############################################
######### Code Commemorative
#############################################
bills <- commem_bills %>%
  filter(term == save_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session'))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)

######################################################
############### Code Bill History
#############################################
bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_yrs}.csv")
bill_hist <- read.csv(bill_hist_path)

### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
bill_hist <- bill_hist %>%
  rename(ga_num = session_num,
         bill_id = bill_number) %>%
  mutate(term = save_yrs,
         session = save_yrs,
         bill_id = paste0(gsub(' [0-9].+| [0-9]+', '', bill_id), str_pad(gsub('.+ ', '', bill_id), 4, pad = '0')),
         parent_bill = ifelse(parent_bill == '', '', paste0(gsub(' [0-9].+| [0-9]+', '', parent_bill), str_pad(gsub('.+ ', '', parent_bill), 4, pad = '0'))),
         substitute_id = NA)

######### Create ID for Substitute Record to Match Record in Bills
options(warn = 2)
for(b_id in unique(bill_hist$bill_id)){
  if(grepl('^HS|^SS', b_id)){next}
  if(b_id %in% bill_hist$parent_bill){
    bill_hist[bill_hist$bill_id == b_id,]$substitute_id <- paste(unique(bill_hist[bill_hist$parent_bill == b_id,]$bill_id), collapse = "; ")
  }
}; rm(b_id) 
options(warn = 1)

### Order by Order
bill_hist <- bill_hist %>%
  mutate(master_id = ifelse(grepl("^HS|^SS", bill_id), parent_bill, bill_id)) %>%
  arrange(term, session, master_id, action_date, order) %>%
  group_by(master_id) %>%
  # **** Initial Order not always right; so arranging by date and then keeping original order of actions on same date ******
  mutate(order = 1:n()) %>%
  ungroup()

### Create + Fill in Chamber Variable:
bill_hist <- bill_hist %>%
  mutate(chamber = ifelse(order == 1 & substring(bill_id, 1,1) == "H", "H", NA),
         chamber = ifelse(order == 1 & substring(bill_id, 1,1) == "S", "S", chamber),
         chamber = ifelse(is.na(chamber) & grepl("in House|by House", action), "H", chamber),
         chamber = ifelse(is.na(chamber) & grepl("in Senate|by Senate", action), "S", chamber)
  ) %>%
  group_by(term, session, master_id) %>%
  fill(chamber) %>%
  ungroup()

### Re-Coding Chamber Variable
bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate", "G" = "Governor", "CC" = "Conference")

### Clean Substitue Info From the Actions (e.e.g, HS 1 for HB 1 - Passed by)
bill_hist$action <- str_trim(gsub("HS [0-9]+ for HB [0-9]+ (- +-|-+)", '', bill_hist$action))
bill_hist$action <- str_trim(gsub("SS [0-9]+ for SB [0-9]+ (- +-|-+)", '', bill_hist$action))

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c('reported out', '^tabled in') # "^assigned.+subcomm"
## The only committee actions are introduced, assigned to, or reported out
abc_t <- c('reported out', 'amendment.+in (house|senate)', 'amendent (ha|sa).+passed', 'amendent (ha|sa).+defeated',
           'rules.+suspended.+(house|senate)', 'lifted from table')
# -- Could do 'substituted in' but not clear when it happens... assume the floor but sometimes happens before assigned to comm? weird..
pc_t <- c('^passed', 'vetoed', "passed by (house of representatives|senate)")
# --> "^passed by" may technically be what we want. Occassional "passed in [chamber] by voice vote" (which is sometimes followed by "passed by [chamber], Votes:")
law_t <- c('^signed by gov', '^enact')
## Enact = Enact w/o sign by governor

### Check Actions
# filter(bill_hist, grepl('passed by', tolower(action)) ) %>% distinct(action) %>% View() 
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
  hist_sub <- filter(bill_hist, master_id == b_id, term == save_yrs, session == s_id)
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, save_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
  bill_stages$bill_url <- bills[i,]$bill_url
  if(bill_stages$law == 0 & tolower(bills[i,]$status) %in% c("enact w/o sign", "signed")){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- bill_stages$law <- 1
  }
  if(bill_stages$passed_chamber == 0 & tolower(bills[i,]$status) %in% c("passed")){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
  }
  if(bill_stages$action_beyond_comm == 0 & tolower(bills[i,]$status) %in% c("out of committee")){
    bill_stages$action_beyond_comm <- 1
  }
  all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
  # print(i)
}
options(warn = 1)

### Check Codings
cat('\n')
all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law) ) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2)) %>% 
  as.data.frame() %>% print()

# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% View()
# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 1,]$bill_id) %>% View()

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-3}_{t-1}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% print()

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-5}_{t-3}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% print()

### MERGE to BILL DATA
bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 

### MERGE In COMMEMS
all_bill_stages <- commem_bills %>%
  filter(term == save_yrs) %>%
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

### Save Stage Info **** MERGE WITH SS **********
write.csv(all_bill_stages, glue('../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv'), row.names = FALSE )

rm(bill_hist_path, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist, aic_t, abc_t, pc_t, law_t)
rm(b_id, s_id, b_spon, bill_hist)

####################################################
############### Identify Unique Legislators via SLER
####################################################

## Import and Clean Sponsors Name to Match
all_sponsors <- bills %>%
  mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'H', "H", "S")) %>%
  select(LES_sponsor, chamber, passed_chamber, law) %>%
  mutate(term = save_yrs) %>%
  group_by(LES_sponsor, chamber, term) %>%
  summarize(num_sponsored_bills = n(), 
            sponsor_pass_rate = sum(passed_chamber) / n(),
            sponsor_law_rate = sum(law) / n(),
            num_cosponsored_bills = NA) %>%
  ungroup()

#### Adding Legislators Who COSPONSORED A BILL but did not SPONSOR
unique_cospon <- str_trim(unique(unlist(str_split(bills$all_chamber_sponsors, ', '))))
for(nonspon in unique_cospon){
  if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != '' & !is.na(nonspon)){
    chamb <- unique(substring(bills[grepl(nonspon, bills$all_chamber_sponsors),]$bill_id, 1, 1))
    if("H" %in% chamb & "S" %in% chamb){
      print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
    }else{
      all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = chamb, term = save_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0) 
    }
  }
}

######## Cosponsorship Info 
#bills$cospon_match <- paste(bills$LES_sponsor, bills$coauthors, sep = '; ')
for(i in 1:nrow(all_sponsors)){
  c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
  sn <- all_sponsors[i,]$LES_sponsor
  sn <- gsub('\\)', '\\\\)', gsub("\\(", '\\\\(', sn))
  search_term <- paste0("^", sn, ',|^', sn, '$|, ', sn, ',|, ', sn, '$')
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(search_term, tolower(c_sub$all_chamber_sponsors)))
  ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
}; rm(sn, search_term)
# bills <- select(bills, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))
if(min(all_sponsors$num_cosponsored_bills) < 0){
  print("negative cosponsors"); break
} else{print("cosponsors valid")}


#######################
#### CLEAN NAMES
all_sponsors$last_name <- gsub('^[^ ]+ ', '', all_sponsors$LES_sponsor)
all_sponsors$first_name <- str_extract(all_sponsors$LES_sponsor, "[a-z]\\.[a-z]\\. |[a-z]\\. |[a-z] ")
all_sponsors$first_name <- ifelse(is.na(all_sponsors$first_name), '', substring(all_sponsors$first_name,1,1))

all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update First/Last Names for Matching 
if(t_yrs %in% c("2008_2010", "2010_2012")){
  all_sponsors[all_sponsors$LES_sponsor == 'd.e. williams',]$first_name <-  "dennis e."
  all_sponsors[all_sponsors$LES_sponsor == 'd.p. williams',]$first_name <-  "dennis p."
}
if(t_yrs %in% c("2008_2010", "2010_2012", '2012_2014', '2014_2016', '2016_2018')){
  all_sponsors[all_sponsors$LES_sponsor == 'q. johnson',]$first_name <-  "s"
}





all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(LES_sponsor,substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name))


legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(terms)) | startsWith(legiscan_sessions,as.character(terms+1)) ]

legiscan = foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% distinct()


# if(t_yrs == "2019_2020") {
#   legiscan = legiscan %>% 
#     bind_rows(legiscan %>% filter(people_id == 19205) %>% mutate(role = "Rep", district = "HD-006")) %>% 
#     bind_rows(legiscan %>% filter(people_id == 19048) %>% mutate(role = "Rep", district = "HD-003"))
#   
# }


legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = case_when(
    n > 2 ~ paste0(substr(first_name,1,1),". ",  substr(middle_name,1,1)," ", last_name),
    T ~ last_name)) %>%
  group_by(match_name) %>% 
  mutate(n = n()) %>% 
  mutate(match_name = ifelse(n > 1, paste0(substr(first_name,1,1),". ",  substr(middle_name,1,1)," ", last_name),
                             match_name)) %>% 
  mutate(match_name_chamber = tolower(paste(match_name,substr(district,1,1),sep="-"))) 


# inexact::inexact_addin()
# left: legiscan_adj
# right: all_sponsors
# method: osa
# mode: full

if(save_yrs == "2019_2020"){
  all_sponsors2 = 
    # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "short-h" = "d. short-h",
        "williams-h" = "k. williams-h",
        "smith-h" = "michael smith-h",
        "s.  johnson-h" = "q. johnson-h"
      )
    )
  
}

if(save_yrs == "2021_2022"){
  all_sponsors2 = # You added custom matches:
    # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "smith-h" = "michael smith-h",
        "moore-h" = "s. moore-h"
      )
    )
  
}

#### Clean
legis_data <- all_sponsors2 %>%
  rename(data_name = LES_sponsor) %>%
  mutate(sponsor = ifelse(!is.na(name), name, str_to_title(match_name)), 
         term = t_yrs,
         chamber = substr(district,1,1)) %>%
  select(sponsor, data_name, name, klarner_id = people_id, chamber , party, district, term, num_sponsored_bills, num_cosponsored_bills, sponsor_pass_rate, sponsor_law_rate) %>%
  arrange(chamber, sponsor) %>% 
  distinct()


# now need to remove zero-LES legislators who never actually served. see documentation file on how this is generated

removal_legislators = read.csv("../../Estimate LES/Zero_LES_legislators_Coded.csv") %>% 
  filter(state == this_state & term == t_yrs & not_actually_in_chamber == T) %>% 
  mutate(chamber = substr(chamber,1,1))

if(nrow(removal_legislators) > 0){
  legis_data = anti_join(legis_data, removal_legislators,
                         by = c("klarner_id" = "legiscan_id", "chamber"))
}

########################
### Estimate Scores + Add in Relatd Variables
#########################

### Check if bills in data without an ID'd sponsor
# filter(bills, !(bills$primary_sponsor %in% legis_data$data_name))
bills <- bills %>% #select(-sponsor) %>%
  rename(sponsor = LES_sponsor) %>%
  mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'H', 'H', 'S'))

### Standard LES: Same as Congressional Measure
source('../../Estimate LES/calc_LES_fx.R')

LES <- calc_LES(bills, legis_data, save_yrs, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1))
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
LES_noWeights <- calc_LES(bills, legis_data, save_yrs, ss_weight = 5, reg_weight = 5, com_weight = 5, stage_weights = c(1,1,1,1,1))
LES_noWeights <- rename(LES_noWeights, LES_nw = LES) %>% select(1:6, LES_nw)
LES <- left_join(LES, LES_noWeights, by = intersect(colnames(LES), colnames(LES_noWeights)))
rm(LES_noWeights)

### Fix Term Variable
LES <- rename(LES, term = session)

#### Merge Agg Stats back in
LES <- legis_data %>%
  select(sponsor, chamber, party,district,  num_sponsored_bills, sponsor_pass_rate, sponsor_law_rate, num_cosponsored_bills) %>%
  mutate(chamber = ifelse(chamber == "H", "House", "Senate")) %>%
  left_join(LES, ., by = c("sponsor", "chamber")) %>%
  select(-klarner_name) %>% 
  rename(legiscan_id = klarner_id) %>%
  select(sponsor, data_name, legiscan_id, term, chamber, district, party, LES, everything())


#### If LES == 0 and --- , "num_cosponsored_bills"
LES[LES$LES == 0 & is.na(LES$num_sponsored_bills), c('num_sponsored_bills', 'sponsor_pass_rate', 'sponsor_law_rate')] <- 0

############### Save LES
write.csv(LES, glue("{this_state}_LES_{save_yrs}.csv"), row.names = FALSE)
rm(LES)


cat(glue(". \n *********************** SESSION {save_yrs} ~~> DONE  ***********************"))


################# ****** END LOOP


rm(all_sponsors, bills, legis_data, SS_bills, commem_bills, H_elec_year, S_elec_year, i, keep_types)
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES, house_term_length, sen_term_length) # 
rm(t, election_years, klarner_gs, m_sub, nonspon, unique_cospon, c_sub, save_yrs, chamb) # 


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by SPECIAL ELECTION --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
# Historical Election Results (back to 2003): https://www.sos.ms.gov/Elections-Voting/Pages/Election-Results-By-Year.aspx
########################################################################################################################
### FULL ROSTER: 
#########################


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
#     session chamber   N AIC ABC PASS LAW
# 1 2003_2004       H 549 398 435  342 209
# 2 2003_2004       S 355 239 284  244 200
### DROP:
# -- sharp, thomas b. -- no record of  him in chamber that term: https://legis.delaware.gov/AssemblyMember/141/Sharp


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
#     session chamber   N AIC ABC PASS LAW
# 1 2005_2006       H 546 381 416  329 220
# 2 2005_2006       S 407 268 316  267 221
# ***** NO ISSUES AFTER NAME CORRECTIONS ******


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
#     session chamber   N AIC ABC PASS LAW
# 1 2007_2008       H 526 369 407  321 225
# 2 2007_2008       S 328 227 265  236 197
#### APPOINTED/WON SPECIAL ~ HOUSE:
# -- CARSON (william)
# -- HASTINGS (greg)
# -- SHORT (byron) -- Last name duplicated; won't print
#### APPOINTED/WON SPECIAL ~ SENATE:
# -- ENNIS (bruce, via H)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 2 bill(s) without a sponsor
#     session chamber   N AIC ABC PASS LAW
# 1 2009_2010       H 496 355 376  314 269
# 2 2009_2010       S 321 208 250  222 207
#### APPOINTED/WON SPECIAL ~ HOUSE:
# -- BRIGGS KING (ruth)
# -- KOVACH (thomas)
#### APPOINTED/WON SPECIAL ~ SENATE:
# -- BOOTH (joseph, via H)
# -- ENNIS (bruce, T - 1, via H)
#### DROP:
# -- mcwilliams, diana m. -- Not in chamber in 2009: https://legis.delaware.gov/AssemblyMember/144/McWilliams
# -- vaughn, james t. -- Not in chamber in 2009: https://legis.delaware.gov/AssemblyMember/144/Vaughn


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 2 bill(s) without a sponsor
#     session chamber   N AIC ABC PASS LAW
# 1 2011_2012       H 405 290 297  253 219
# 2 2011_2012       S 273 215 229  212 189
# ***** NO ISSUES AFTER NAME CORRECTIONS ******


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
#     session chamber   N AIC ABC PASS LAW
# 1 2013_2014       H 424 325 334  287 261
# 2 2013_2014       S 267 205 226  203 182
### DROP:
# *** For all three, elections were held in Nov 2012 to fill replacement 
# *** (This may have been a redistricting thing -- all senate districts had an election that year...)
# -- connor, dorinda a. -- Not in chamber in 2013: https://legis.delaware.gov/AssemblyMember/146/Connor
# -- booth, joseph w. -- Not in chamber in 2013: https://legis.delaware.gov/AssemblyMember/146/Booth
# -- bunting, george h. jr. -- Not in chamber in 2013: https://legis.delaware.gov/AssemblyMember/146/Bunting


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 2 bill(s) without a sponsor
#     session chamber   N AIC ABC PASS LAW
# 1 2015_2016       H 443 336 343  274 239
# 2 2015_2016       S 291 223 237  209 190
### APPOINTED/WON SPECIAL ~ HOUSE:
# -- BENTZ (david)
### DROP:
# -- venables, robert l. sr. -- had to run in 2012 and 2014; lost 2014, but still showing up because won 2012 --> DROP


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
#     session chamber   N AIC ABC PASS LAW
# 1 2017_2018       H 481 389 379  315 290
# 2 2017_2018       S 265 199 210  184 161
### WON SPECIAL ~ SENATE:
# -- HANSEN (stephanie, via H)
### DROP:
# -- halllong, bethany -- resigned January 17, 2017 after being elected Lt. Gov. = Only in office 10 days



# filter(klarner, grepl("hansen", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 19 & sen == 1 & outcome == "w") %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)



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

### FIX MISMATCHES
# LES[LES$data_name %in% "carter" & LES$term == "2016_2019",]$klarner_id <- NA
# LES[LES$data_name %in% "carter" & LES$term == "2016_2019",]$klarner_name <- NA
# LES[LES$data_name %in% "carter" & LES$term == "2016_2019",]$sponsor <- 'carter, joel'

### ****Still missing***** ---> Rest are not in Klarner or 2017-2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[3]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, missing, still_missing)


### Fix Missing
name_matches <- data.frame(LES_name = 'ennis', k_name = 'ennis, bruce c.')
name_matches <- add_row(name_matches, LES_name = 'king, s', k_name = 'king, ruth briggs')
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
# fill_missing <- data.frame(LES_name = "jones, wilbert", new_name = 'jones, wilbert l.', party = 'd', district = 82, exper = 'none')
# # fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')
# 
# for(i in 1:nrow(fill_missing)){
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
# }

#### *** 2017_2018 Special winners: If any of below run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "hansen", c('party', 'sponsor')] <- list('d', "hansen, stephanie")
# LES[LES$sponsor == "zzzzzzzz", c('party', 'sponsor')] <- c('zzzzz', "zzzzzzz")

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)
# rm(fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

# *** NONE NEEDED AT PRESENT ***

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
# **** ALL data should be right (assuming still in chamber) EXCEPT Majority Member IN STAGGERED STATES ********
# senate <- filter(hf_data, chamber == "Senate")
# senate$year <- senate$year + 2
# senate$term <- paste0(senate$year + 1, "_", senate$year + 2)
# senate$MajorityMember <- NA
# hf_data <- bind_rows(hf_data, senate) %>% arrange(year, chamber)
# rm(senate)

#### This will include new rows for terms where folks didn't hold office but won't be a problem as they won't merge
# --> e.g., if served 2000-2004, this will add a 2005-2006 row; but because they didn't serve that term, won't merge into LES data
new_rows <- filter(hf_data, year == 9999)
for(i in 1:nrow(hf_data)){
  cand_rows <- filter(hf_data, CandId == hf_data[i,]$CandId)
  if(!((hf_data[i,]$year + 2) %in% cand_rows$year)){
    new_row <- hf_data[i,]
    new_row$year <- new_row$year + 2
    new_row$term <- paste0(new_row$year + 1, "_", new_row$year + 2)
    new_rows <- bind_rows(new_rows, new_row)
  }
}

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
# LES[LES$sponsor %in% c('zzzzzzzz'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches:
# -- Evelyn 'Tina' Fallon; 
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1991_1992', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('atkin', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

### Lot of the missing are special election winners in 2016+
name_matches <- data.frame(LES_name = 'king, ruth briggs', SM_name = 'Briggs King, Ruth')
#name_matches <- add_row(name_matches, LES_name = 'bentz, david', SM_name = 'zzzzzzz')
# ** Caulk: SM have him as switching to indep in 1997 but didn't happen until Feb. 2005 --> Using the record with most time coverage
name_matches <- add_row(name_matches, LES_name = 'caulk, wallace jr.', SM_name = 'Caulk, G. Wallace Jr.')
# ** Hudson: 2015-16 on 2nd row
name_matches <- add_row(name_matches, LES_name = 'hudson, deborah d.', SM_name = 'Hudson, Deborah')
# ** Smith: Not recorded as marshall in data but is her maiden name
name_matches <- add_row(name_matches, LES_name = 'smith, melanie george', SM_name = 'Marshall, Melanie George')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

##########
### MORE DETAILED FIXES
##########

### Dennis E. Williams AND Dennis P. Williams
LES[LES$sponsor == 'williams, dennis e.',]$SM_name <-  ideo[ideo$name == 'Williams, Dennis' & ideo$house2014 %in% 1,]$name
LES[LES$sponsor == 'williams, dennis e.',]$SM_party <- ideo[ideo$name == 'Williams, Dennis' & ideo$house2014 %in% 1,]$party
LES[LES$sponsor == 'williams, dennis e.',]$np_score <- ideo[ideo$name == 'Williams, Dennis' & ideo$house2014 %in% 1,]$np_score
LES[LES$sponsor == 'williams, dennis p.',]$SM_name <-  ideo[ideo$name == 'Williams, Dennis' & ideo$house2003 %in% 1,]$name
LES[LES$sponsor == 'williams, dennis p.',]$SM_party <- ideo[ideo$name == 'Williams, Dennis' & ideo$house2003 %in% 1,]$party
LES[LES$sponsor == 'williams, dennis p.',]$np_score <- ideo[ideo$name == 'Williams, Dennis' & ideo$house2003 %in% 1,]$np_score


########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########
# filter(LES, grepl("atkins", sponsor)) %>% select(1:7, party, SM_name, SM_party)

#### John Atkins -- Switched R to D in pre 2008 election
LES[LES$sponsor == 'atkins, john c.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Atkins, John' & ideo$party == 'D',]$name
LES[LES$sponsor == 'atkins, john c.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Atkins, John' & ideo$party == 'D',]$party
LES[LES$sponsor == 'atkins, john c.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Atkins, John' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'atkins, john c.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Atkins, John' & ideo$party == 'R',]$name
LES[LES$sponsor == 'atkins, john c.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Atkins, John' & ideo$party == 'R',]$party
LES[LES$sponsor == 'atkins, john c.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Atkins, John' & ideo$party == 'R',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################

##### Drop Nicknames
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

##### Specific Name Changes
LES[LES$sponsor == "paradee, w. charles iii",]$sponsor <- "paradee, william charles iii"


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 2003 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(2009:2020) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2003:2008) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 2003 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(2003:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
#LES[as.numeric(substring(LES$term,1,4)) %in% c(2012:2019) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


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
  scale_color_manual(values=c("dodgerblue2",  'gray50', "red2", 'gray50'))

##### CHECK OUTLIERS 
# filter(LES, party == 'd' & np_score > 0 & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & np_score < 0 & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + in_majority + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

