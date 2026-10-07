

#######################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** CALIFORNIA *** BY TERM
########################################################################

#####################
##### NOTES FOR CA:
#####################
# SPECIAL SESSIONS --- Bill numbers change - e.g., SB X1-1 vs SB 1 -- Seems like non-special bills can continue into special sessions
# ---- That is: Bills don't die formally (in the system, at least) until end of 2-year term
# Names are oddly formatted for those with same last name (in some years, at least)
# ---- EG: 2015-2016: Two last names == Allen, but first name is only specified for TRAVIS, not BEN
####################
#### About Committee Bills (http://www.leginfo.ca.gov/rules/assembly_rules.pdf):
# 47 - (f) A committee bill may not be introduced unless it contains the signatures of a majority of all of the members, 
# including the chairperson, of the committee. If all of the members of a committee sign the bill, at the option of the committee 
# chairperson the committee members’ names need not appear as authors in the heading of the printed bill.
# 60 - A chairperson of a standing committee may not preside at a committee hearing to consider a bill of which he or she is the sole 
# author or the lead author, except that the Chairperson of the Committee on Budget may preside at the hearing of the Budget Bill by the Committee on Budget.

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
library(tibble)
library(foreach)
library(inexact)

this_state <- 'CA'
keep_types <- c("AB", "SB", "ABX", "SBX")

#### Output Directory
#dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd(glue("../../LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Data Directory
data_files <- list.files(glue("../../../State Legislative Data/States/{this_state}/database_files/parsed_db_files"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
terms_all <- unique(c("93_94", "95_96", "97_98", '99_00', gsub('.+Details_|.csv', '', bill_files)))
rm(data_files, bill_files)

### Terms/Sessions and Filespaths
terms <- 2023
t = terms
t_plus_one = t + 1
t_yrs <- as.numeric(terms)
t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 1}'))
t_sessions = terms_all[grepl(as.character(t-2000),terms_all) | grepl(as.character(t-2000+1),terms_all)]


#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv"))
commem_bills$bill_id <- toupper(commem_bills$bill_id)

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

#### Check SS BIll Types 
# table(SS_bills$bill_type)
# filter(SS_bills, bill_type == "A")

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- '11_12'


### Skip Previously Estimated
if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
  print(glue('. \n \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n \n.'))
  next
}

###### SESSION IN PROGRESS
print(glue('. \n \n ~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! \n \n.'))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/database_files/parsed_db_files/{this_state}_Bill_Details_{t_sessions}.csv")
bills <- read.csv(bill_path)
bills <- rename(bills, bill_id_web = bill_id, bill_id_orig = bill_num)

######## Standardize the Bill IDs
id_parts <- str_split_fixed(bills$bill_id_orig, "-|\\_", 2)
bills$bill_id <- ifelse(grepl('X', id_parts[,1]), paste0(id_parts[,1], "-", str_pad(id_parts[,2], 4, pad = "0")), 
                        paste0(id_parts[,1], str_pad(id_parts[,2], 4, pad = "0")))  
rm(id_parts)

### Check for duplicates
if(nrow(bills) != nrow(distinct(bills))){
  cat(" \n ~~~> DUPLICATE BILLS \n\n .")
  break
}

#### Clean/ID
bills <- bills %>%
  mutate(term = t_yrs,
         special = ifelse(grepl("x|X", toupper(bill_id)), 1, 0),
         session = ifelse(grepl("x|X", bill_id), paste0("SS", gsub('[A-Za-z]+|\\-.+|\\_.+', '', bill_id)), "RS"))



##### Manual Name Fixes
if(t_yrs == "2005_2006"){
  bills[bills$lead_author == "Baca Jr. (H)",]$lead_author <- "Baca (H)"
  bills$authors <- gsub("Baca Jr. \\(H\\)", "Baca (H)", bills$authors)
  bills[bills$lead_author == "Runner (S)",]$lead_author <- "George Runner (S)"
  bills$authors <- gsub("; Runner \\(S\\)", "; George Runner (S)", bills$authors)
  bills$authors <- gsub("^Runner \\(S\\)", "George Runner (S)", bills$authors)
  bills$coauthors <- gsub("^Runner \\(S\\)", "George Runner (S)", bills$coauthors)
}else if(t_yrs == "2017_2018"){
  bills[bills$lead_author == "Gonzalez (H)",]$lead_author <- "Gonzalez Fletcher (H)"
  bills$authors <- gsub("Gonzalez \\(H\\)", "Gonzalez Fletcher (H)", bills$authors)
  bills$coauthors <- gsub("Gonzalez \\(H\\)", "Gonzalez Fletcher (H)", bills$coauthors)
} else if(t_yrs == "2019_2020"){
  bills$lead_author = gsub("Kamlager ", "Kamlager-Dove ",bills$lead_author)
  bills$authors = gsub("Kamlager ", "Kamlager-Dove ",bills$authors)
  bills$coauthors = gsub("Kamlager ", "Kamlager-Dove ",bills$coauthors)
} else if(t_yrs == "2021_2022"){
  bills$lead_author = gsub("Kamlager ", "Kamlager-Dove ",bills$lead_author)
  bills$authors = gsub("Kamlager ", "Kamlager-Dove ",bills$authors)
  bills$coauthors = gsub("Kamlager ", "Kamlager-Dove ",bills$coauthors)
} else if (t_yrs == "2023_2024"){
  bills$lead_author = gsub("Boerner Horvath", "Boerner",bills$lead_author)
  bills$authors = gsub("Boerner Horvath", "Boerner",bills$authors)
  bills$coauthors = gsub("Boerner Horvath ", "Boerner",bills$coauthors)
}

############### Standardize Sponsors
### If intro_sponsor is blank, use first primary sponsor (order is nonalphabetical, set in json as 1,2...)
if(as.numeric(substring(t_yrs, 1, 4)) < 1999){
  bills$authors <- gsub('AUTHOR\\(S\\): |\\. ÿ|\\.$|^Members ', '', bills$authors)
  bills$primary_sponsor <- str_trim(gsub(",.+|\\(.+| and .+", '', bills$authors))
}else{
  bills$primary_sponsor <- gsub(' \\((H|S)\\)', '', bills$lead_author)
}

### Specific Corrections
bills <- mutate(bills, primary_sponsor = ifelse(grepl('^Greene \\)', primary_sponsor), 'Greene', primary_sponsor))
bills$primary_sponsor <- gsub('Introduced by.+ Member |Introduced by.+ Senator | on behalf of the committee', '', bills$primary_sponsor)
bills$primary_sponsor <- gsub("^Assembly Member |^Assembly Members |^Senator |^Senators |^Members |^Member ", '', bills$primary_sponsor)
bills$primary_sponsor <- str_trim(bills$primary_sponsor)
bills$primary_sponsor <- gsub('\\.$', '', bills$primary_sponsor)

### Name Fixes
if(t_yrs == '1993_1994'){
  bills[bills$primary_sponsor == "Andal Brulte",]$primary_sponsor <- "Andal"
}else if(t_yrs == "2001_2002"){
  bills[bills$primary_sponsor == "Romero and",]$primary_sponsor <- "Romero"
}else if(t_yrs == "2013_2014"){ # McLeod introduced, but then won US House seat -- http://leginfo.legislature.ca.gov/faces/billStatusClient.xhtml?bill_id=201320140SB13
  bills[bills$primary_sponsor %in% "Negrete McLeod", c("lead_author", "authors")] <- "Beall (S)"
  bills[bills$primary_sponsor %in% "Negrete McLeod",]$primary_sponsor <- "Beall"
}  else if(t_yrs %in% c("2019_2020","2021_2022")){
  bills$primary_sponsor[bills$primary_sponsor == "Kamlager"] <- "Kamlager-Dove"
}

### 2007-2008 - 1 Sponsor = '------' --> http://www.leginfo.ca.gov/pub/07-08/bill/asm/ab_0001-0050/abx1_2_bill_20081130_history.html
#E.g., Introduced by the Committee on Judiciary as presented by Assembly Member Weggeland on behalf of the committee
if(t_yrs == "2007_2008"){
  bills <- filter(bills, !grepl('^------$', primary_sponsor))
}

############### Drop Resolutions

all_bills <- bills
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|_.+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)

#### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
bills$primary_sponsor <- tolower(bills$primary_sponsor)
bills$primary_sponsor <- gsub('á', 'a', bills$primary_sponsor)
bills$primary_sponsor <- gsub('é', 'e', bills$primary_sponsor)
bills$primary_sponsor <- gsub('ó', 'o', bills$primary_sponsor)
bills$primary_sponsor <- gsub('í', 'i', bills$primary_sponsor)
bills$primary_sponsor <- gsub('ñ', 'n', bills$primary_sponsor)

bills$authors <- tolower(bills$authors)
bills$authors <- gsub('á', 'a', bills$authors)
bills$authors <- gsub('é', 'e', bills$authors)
bills$authors <- gsub('ó', 'o', bills$authors)
bills$authors <- gsub('í', 'i', bills$authors)
bills$authors <- gsub('ñ', 'n', bills$authors)

bills$coauthors <- tolower(bills$coauthors)
bills$coauthors <- gsub('á', 'a', bills$coauthors)
bills$coauthors <- gsub('é', 'e', bills$coauthors)
bills$coauthors <- gsub('ó', 'o', bills$coauthors)
bills$coauthors <- gsub('í', 'i', bills$coauthors)
bills$coauthors <- gsub('ñ', 'n', bills$coauthors)

#### Willie Brown left office in June 1995; Valerie Brown was still in office
#### From then on, all bills Valerie sponsored were labeled only 'Brown' instead of 'Valerie Brown'
#### First was AB 2199 --- Introduced February 1995, after Brown's departure -- All others have higher bill nums
if(t_yrs == '1995_1996'){
  bills[bills$primary_sponsor == "brown",]$primary_sponsor <- "valerie brown"
}

if(t_yrs == '2003_2004'){
  bills[bills$primary_sponsor == "horton",]$primary_sponsor <- "shirley horton"
} 

###################
###### Merge in S&S Bills
###################
# *** For CA: ALL BILL NUMBERS ARE UNIQUE, 1 2-year biennium

if(t_yrs == "2019_2020"){
  SS_bills$bill_id[SS_bills$bill_id=="SB10001"]="SB0001"
  SS_bills$bill_id[SS_bills$bill_id=="SB80008"]="SB0008"
  SS_bills$bill_id[SS_bills$bill_id=="AB50005"]="AB0005"
  SS_bills$bill_id[SS_bills$bill_id=="SB50005"]="SB0005"
  SS_bills$bill_id[SS_bills$bill_id=="AB40004"]="AB0004"
} else if(t_yrs == "2021_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="SB40004"]="SB0004"
  SS_bills$bill_id[SS_bills$bill_id=="AB70007"]="AB0007"
  SS_bills$bill_id[SS_bills$bill_id=="SB20002"]="SB0002"
  SS_bills$bill_id[SS_bills$bill_id=="SB90009"]="SB0009"
} else if (t_yrs == "2023_2024"){
  SS_bills$bill_id[SS_bills$bill_id=="AB10001" & SS_bills$Title == "Authorizes Unionization of Legislative Staff"]="AB0001"
  SS_bills$bill_id[SS_bills$bill_id=="AB10001" & SS_bills$Title == "Amends Turnaround and Maintenance Costs Related to Transportation Fuels and Inventories"]="ABX2-0001"
  SS_bills$bill_id[SS_bills$bill_id=="SB20002"]="SB0002"
  SS_bills$bill_id[SS_bills$bill_id=="SB40004"]="SB0004"
  SS_bills$bill_id[SS_bills$bill_id=="AB80008"]="AB0008"
}

SS_term <- SS_bills %>% 
  # filter(term == t_yrs) %>% 
  mutate(SS = 1) %>% 
  distinct(term, bill_id, SS, Title)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,primary_sponsor), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",primary_sponsor, ignore.case=T)) %>%
  arrange(primary_sponsor) 
missing_SS_bills$bill_id




# now check to see if there are duplicate joins

duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills , 
            by = c("bill_id", "term")) %>%
  group_by(rowname) %>% mutate(count = n()) %>%
  ungroup() %>% 
  select(count,bill_id,term,session, Title, title) %>% 
  arrange(desc(count),bill_id)

if(nrow(duplicate_SS_bills %>% filter(count > 1)) > 0 ){
  print("you have duplicates"); SS_duplicates_exist <- 1; break
} else{
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
  bills2 <- bills %>% 
    left_join(SS_term %>% select(-Title) %>% distinct(), by = c("bill_id", "term")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  SS_term2 = SS_term %>% 
    left_join(bills %>% select(bill_id,term,session),by=c("bill_id","term"))
  if(!identical(c(nrow(bills2),nrow(SS_term2)),orig_row_n )){print("merge failed"); break} else{
    bills = bills2; SS_term = SS_term2; rm(bills2, SS_term2)
  }
}

### Check Missing
table(bills$SS)
SS_in_bills = sum(bills$SS)
SS_in_PVS = nrow(SS_term  %>%
                   mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
                   filter(bill_type %in% keep_types) )

if(SS_in_bills == SS_in_PVS){
  print("all SS merged properly")
} else {
  print(glue("{SS_in_PVS} S&S bills in original dataset, but {SS_in_bills} S&S in our bills dataset"))
  
  # stuff in PVS, not in bills
  print(anti_join(SS_term, bills, by = c("bill_id", "term", "session")))
  
  # PVS bills that are duplicated in bills. not necessarily a problem!
  print(SS_term %>% group_by(bill_id, term) %>%
          mutate(count = n()) %>% filter(count > 1) %>% arrange(bill_id))
  
  # see if they are equal after removing double-counted bills. this is a crude proxy, you should examine more closely
  all.equal(SS_in_bills + nrow(anti_join(SS_term, bills, by = c("bill_id", "term", "session")))+
              nrow(SS_term %>% group_by(bill_id, term) %>%
                     mutate(count = n()) %>% filter(count > 1))/2,
            nrow(SS_term ))
}

rm(all_bills, missing_SS_bills, duplicate_SS_bills)


#### DROP COMMITTEE BILLS
# comm_corrections <- ifelse(grepl("^Committee", bills$primary_sponsor), sub("\\(", "zzzz", bills$authors), NA)
# ----> These are all just a list of the committee, with Chairman first... 
if(any(grepl('^Comm|^Special Comm', bills$primary_sponsor, ignore.case = T))){
  print(glue("-----> Dropping {nrow(filter(bills, grepl('^Comm|^Special Comm', primary_sponsor, ignore.case = T)))} bill(s) sponsored by COMMITTEE"))
  bills <- filter(bills, !(grepl("^Comm|^Special Comm", primary_sponsor, ignore.case = T)))     
}


### CHeck Missing Sponsors
# filter(bills, primary_sponsor == "" | is.na(primary_sponsor)) %>% View()
if(any(is.na(bills$primary_sponsor) | bills$primary_sponsor == '')){
  print(glue(" ~~> Dropping {nrow(filter(bills, primary_sponsor == '' | is.na(primary_sponsor)))} bills without a sponsor"))
  bills <- filter(bills, primary_sponsor != "" & !is.na(primary_sponsor)) 
}


####################
#### Code Commemorative
###################

bills <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session'))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)


#################################################
############ Code Bill History
########################################################
bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/database_files/parsed_db_files/{this_state}_Bill_Histories_{t_sessions}.csv")

bill_hist <- read.csv(bill_hist_path)
bill_hist <- bill_hist %>%
  mutate(term = t_yrs,
         session = ifelse(grepl("x|X", bill_id), paste0("SS", gsub('[A-Za-z]+|\\-.+|\\_.+', '', bill_id)), "RS"))

######## Standardize BillHist Bill IDs
if(as.numeric(substring(t_yrs, 1, 4)) < 1999){
  bill_hist$bill_id_orig <- toupper(bill_hist$bill_id)
}else{
  bill_hist <- bill_hist %>%
    mutate(bill_num = ifelse(is.na(bill_num), substring(bill_id, 10, nchar(bill_id)), bill_num)) %>%
    rename(bill_id_web = bill_id, bill_id_orig = bill_num)
}
id_parts <- str_split_fixed(bill_hist$bill_id_orig, "-|\\_", 2)
bill_hist$bill_id <- ifelse(grepl('X', id_parts[,1]), 
                            paste0(id_parts[,1], "-", str_pad(id_parts[,2], 4, pad = "0")), 
                            paste0(id_parts[,1], str_pad(id_parts[,2], 4, pad = "0")))
rm(id_parts)


##### CA: Fix Order = 0 /// No Order
if(as.numeric(substring(t_yrs, 1, 4)) < 1999){
  bill_hist <- bill_hist %>%
    group_by(bill_id) %>%
    arrange(bill_id, order) %>%
    mutate(order = ifelse(order != 1:n(), 1:n(), order))
}else{
  bill_hist <- bill_hist %>%
    group_by(bill_id) %>%
    arrange(bill_id, date, hist_id) %>%
    mutate(order = 1:n())
}
bill_hist <- ungroup(bill_hist) %>% arrange(bill_id, order)

#### Function to fix Chamber Varible
fix_chamber_var <- function(bill_id, chamb, acts){
  code_chamber <- ifelse(substring(unique(bill_id), 1, 1) == "A", "House", "Senate")
  sen_match <- grep('^in senate', tolower(acts)); names(sen_match) <-  rep('Senate', length(sen_match))
  h_match <- grep('^in assembly', tolower(acts)); names(h_match) <- rep('House', length(h_match))
  chamber_vec <- sort(c(sen_match, h_match))
  output <- rep(NA, length(chamb))
  for(j in 1:length(chamb)){
    if(j %in% chamber_vec){
      code_chamber <- names(chamber_vec[which(chamber_vec == j)])
    }
    output[j] <- code_chamber
  }
  return(output)
}

### Apply Function
bill_hist <- bill_hist %>% 
  group_by(bill_id) %>%
  mutate(chamber = fix_chamber_var(bill_id, chamber, action)) %>%
  ungroup()

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
source('../../../State Legislatures/Estimate LES/code_billhist_fx.R')

### Set State-Specific Terms for Identifying Each Stage
# -- Skipping Hearing Canceled | From Committee encompasses a few procedural items as well
aic_t <- c("^in committee: (placed|motion|reconsider|referred|held|hearing for|further|refused)", "^from committee chair.+:",
           "^from committee with author", "^from committee: (do pass|amend|be placed)",
           "set.+ hearing. (referred|failed|further|held|testimony)")
abc_t <- c("^from committee: (do pass|amend|be placed)", "^from committee chair.+:", "from committee with author",
           "read second", "^re-referred")
pc_t <- c("read third.+ passed|^enrolled|to enrollment|^vetoed", "passed\\. ordered to the (senate|assembly)")
law_t <- c("^chaptered by|approved by the gover|chapter \\d+, statutes")

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

### Get rid of double spaces in data from 1999+
bill_hist$action <- gsub("  +", " ", bill_hist$action)

### Code History Stages for Each Bill --- Subset Hist File, Code, Save, Repeat
options(warn = 2)
for(i in 1:nrow(bills)){
  b_id = as.character(bills[i,]$bill_id)
  b_spon = bills[i,]$primary_sponsor
  s_id = bills[i,]$session
  hist_sub <- filter(bill_hist, bill_id == b_id, term == t_yrs) 
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
  bill_stages$bill_url <- bills[i,]$hist_url
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



# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% filter(!grepl('^by resolution|^first read|^prefiled', tolower(action))) %>% View()
# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 1,]$bill_id) %>% View()

# compare with prior session

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-2}_{t-1}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% print()

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-4}_{t-3}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% print()


### MERGE to BILL DATA
bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 

### MERGE In COMMEMS
all_bill_stages <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))

### MERGE In S&S
all_bill_stages <- SS_term %>%
  select(bill_id, term, SS, session) %>%
  left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session')) %>%
  mutate(SS = ifelse(is.na(SS), 0, SS))
rm(SS_term)

### Adjust Commems if SS == 1
all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)

### Save Stage Info
write.csv(all_bill_stages, glue('../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv'), row.names = FALSE )

rm(bill_hist_path, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist, aic_t, abc_t, pc_t, law_t)
rm(b_id, s_id, b_spon, bill_hist, fix_chamber_var)

####################################################
############### Identify Unique Legislators via SLER
####################################################

## Import and Clean Sponsors Name to Match
all_sponsors <- bills %>%
  select(primary_sponsor, chamber, passed_chamber, law) %>%
  mutate(term = t_yrs, 
         LES_sponsor = primary_sponsor) %>%
  group_by(LES_sponsor, term, chamber) %>%
  summarize(num_sponsored_bills = n(), 
            sponsor_pass_rate = sum(passed_chamber) / n(),
            sponsor_law_rate = sum(law) / n()) %>%
  ungroup()

######## Cosponsorship Info --- For OH: Only have cosponsor info for most recent years
all_sponsors$num_cosponsored_bills <- NA
if(as.numeric(substring(t_yrs, 1, 4)) < 1999){
  bills$coauthor_match <- tolower(bills$authors)
}else{
  bills$coauthor_match <- tolower(paste(bills$authors, bills$coauthors, sep = "; "))
}
for(i in 1:nrow(all_sponsors)){
  c_sub <- filter(bills, substring(toupper(bill_id), 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'A', 'S')  )
  search_name <- str_replace_all(all_sponsors[i,]$LES_sponsor, "(\\W)", "\\\\\\1")
  search_name <- paste0("^", search_name, "|", ' ', search_name)
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, c_sub$coauthor_match))
  ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
}
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))
if(min(all_sponsors$num_cosponsored_bills) < 0){
  print("negative cosponsors"); break
} else{print("cosponsors valid")}


################
### CLEAN NAMES
##############

if(as.numeric(substring(t_yrs, 1, 4)) < 1999){
  all_sponsors <- left_join(all_sponsors, map_df(all_sponsors$LES_sponsor, parse_names), by = c("LES_sponsor" = "full_name")) %>%
    select(-salutation) %>%
    mutate(last_name = ifelse(is.na(last_name), first_name, last_name),
           first_name = ifelse(last_name == first_name, NA, first_name))
}else{
  author_tbl <- read.delim(glue("../../../State Legislative Data/States/CA/database_files/pubinfo_{substring(t_yrs, 1, 4)}/LEGISLATOR_TBL.dat"), header=FALSE, quote = "`") %>%
    rename(LES_sponsor = V5, first_name = V6, last_name = V7, party = V12, full_name = V3) %>%
    mutate(chamber = ifelse(grepl("assembly", tolower(V10)), "H", "S")) %>%
    select(LES_sponsor, chamber, full_name, first_name, last_name, party) %>%
    mutate_if(is.character, tolower) %>% mutate(chamber = toupper(chamber)) %>% 
    mutate(LES_sponsor =  str_trim(gsub('ñ', 'n', LES_sponsor)),
           LES_sponsor =  gsub('é', 'e', LES_sponsor),
           LES_sponsor =  gsub('á', 'a', LES_sponsor))
  all_sponsors <- left_join(all_sponsors, author_tbl, by = c("LES_sponsor", "chamber")) %>%
    mutate(last_name = ifelse(is.na(last_name), LES_sponsor, last_name),
           last_name =  str_trim(gsub('ñ', 'n', last_name)),
           last_name =  gsub('é', 'e', last_name),
           last_name =  gsub('á', 'a', last_name),
           last_name = gsub(" jr| sr| ii+", "", last_name))
  rm(author_tbl)
}
all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor) %>% distinct()

### Fix Prefixes to Last Names
if(as.numeric(substring(t_yrs, 1, 4)) < 1999){
  for(i in 1:nrow(all_sponsors)){
    if(all_sponsors[i,]$first_name %in% "de"){
      all_sponsors[i,]$last_name <- paste0('de ', all_sponsors[i,]$last_name)
      all_sponsors[i,]$first_name <- NA
    }else if(all_sponsors[i,]$first_name %in% "la"){
      all_sponsors[i,]$last_name <- paste0('la ', all_sponsors[i,]$last_name)
      all_sponsors[i,]$first_name <- NA
    }
  }
}


all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(LES_sponsor,substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name,party))


legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(terms)) | startsWith(legiscan_sessions,as.character(terms+1)) ]

legiscan = foreach::foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% distinct()


if(t_yrs == "2019_2020") {
  legiscan = bind_rows(legiscan, 
                       legiscan %>% filter(people_id == 14114) %>% mutate(role = "Rep", district = "HD-067"),
                       legiscan %>% filter(people_id == 14099) %>% mutate(role = "Rep", district = "HD-001"),
                       legiscan %>% filter(people_id == 18075) %>% mutate(role = "Rep", district = "HD-037"),
                       legiscan %>% filter(people_id == 14101) %>% mutate(role = "Rep", district = "HD-013"),
                       legiscan %>% filter(people_id == 19627) %>% mutate(role = "Rep", district = "HD-054")
  )
  
} else if (t_yrs == "2021_2022"){
  legiscan = bind_rows(legiscan,
                       legiscan %>% filter(people_id == 19627) %>% mutate(role = "Sen", district = "SD-030")) %>% 
    filter(people_id != 23099)
}


legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = case_when(
    n > 2 ~ paste(first_name, substr(middle_name,1,1), last_name),
    n == 2 ~  paste(first_name,last_name),
    T ~ last_name)) %>%
  group_by(match_name) %>% 
  mutate(n = n()) %>% 
  mutate(match_name = ifelse(n > 1, paste(first_name, substr(middle_name,1,1), last_name),
                             match_name)) %>% 
  mutate(match_name_chamber = tolower(paste(match_name,substr(district,1,1),sep="-"))) 


# inexact::inexact_addin()
# left: legiscan_adj
# right: all_sponsors
# method: osa
# mode: full

if(t_yrs == "2019_2020"){
  all_sponsors2 = 
    # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "melissa a melendez-s" = NA_character_,
        "susan t eggman-s" = NA_character_,
        "monique  limon-s" = NA_character_,
        "susan rubio-s" = "rubio-s",
        "brian  dahle-s" = "dahle-s",
        "jeff stone-s" = "stone-s",
        "rendon-h" = NA_character_,
        "brian  dahle-h" = "dahle-h",
        "gaines-s" = NA_character_,
        "gonzalez-s" = "lena gonzalez-s"
      )
    )
  
}

if(t_yrs == "2021_2022"){
  all_sponsors2 = # You added custom matches:
    # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "gonzalez fletcher-h" = "lorena gonzalez-h",
        "susan rubio-s" = "rubio-s",
        "shirley weber-h" = NA_character_,
        "alvarez-h" = NA_character_,
        "brian dahle-s" = "dahle-s",
        "rendon-h" = NA_character_,
        "mckinnor-h" = NA_character_,
        "rob bonta-h" = "bonta-h",
        "haney-h" = NA_character_,
        "wilson-h" = NA_character_
      )
    )
  
}

if(t_yrs == "2023_2024"){
  all_sponsors2 = # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "susan rubio-s" = "rubio-s",
        "brian dahle-s" = "dahle-s"
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

################
####### Estimate Scores + Add in Relatd Variables
########################

bills <- bills %>%
  mutate(sponsor = tolower(primary_sponsor)) %>% 
  mutate(chamber = ifelse(substring(toupper(bill_id), 1, 1) == 'A', 'H', 'S'))

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

#### LES Without Weights + Merge
LES_noWeights <- calc_LES(bills, legis_data, t_yrs, ss_weight = 5, reg_weight = 5, com_weight = 5, stage_weights = c(1,1,1,1,1))
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

#### If LES == 0 and 
LES[LES$LES == 0 & is.na(LES$num_sponsored_bills), c('num_sponsored_bills', 'sponsor_pass_rate', 'sponsor_law_rate')] <- 0

############### Save LES
write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
rm(LES)

cat(glue(" \n \n \n SESSION {t_yrs} ~~> DONE \n \n \n __________________________________________________________"))

################# ****** END LOOP

rm(all_sponsors, bills, legis_data, SS_bills, elec_year, i, keep_types)
rm(k_matches, klarner_sub, m_sub, km, bill_path, t, t_yrs, c_sub)
rm(commem_bills, calc_LES)

########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by SPECIAL ELECTION --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
####################################################################################

# ~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1993_1994 TERM! ~~~~~~~~~~~~~~~~~~~~
# -----> Dropping 133 bill(s) sponsored by COMMITTEE
### WON SPECIAL ~ HOUSE:
# -- ALBY; BUSTAMANTE; DUCHENY; MCPHERSON; ROGAN
### WON SPECIAL ~ SENATE:
# -- HURTT; JOHANNESSEN (maurice); ROBERTI; WYMAN
# -----> Roberti resigned district 23 in 1992 to run for D20 after Alan Robbins resigned
#### DROP:
# -- bronzan, bruce -- Resigned effective Dec 23, 1992 session -- https://www.latimes.com/archives/la-xpm-1992-10-27-mn-800-story.html

# ~~~~~~~~~~~~~~~~~~~~ESTIMATING SCORES FOR THE 1995_1996 TERM! ~~~~~~~~~~~~~~~~~~~~
# -----> Dropping 159 bill(s) sponsored by COMMITTEE
# ~~~~~> Dropping 114 bills without a sponsor
### WON SPECIAL ~ HOUSE:
# -- ACKERMAN; BAUGH (scott); MARGETT; MILLER (gary)

# ~~~~~~~~~~~~~~~~~~~~ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~~~~~~~
# -----> Dropping 243 bill(s) sponsored by COMMITTEE
### WON SPECIAL ~ HOUSE:
# -- CEDILLO (gil)

# ~~~~~~~~~~~~~~~~~~~~ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~~~~~~~
# -----> Dropping 408 bill(s) sponsored by COMMITTEE
### WON SPECIAL ~ HOUSE:
# -- BOCK (audie)


# ~~~~~~~~~~~~~~~~~~~~ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~~~~~~~
# -----> Dropping 353 bill(s) sponsored by COMMITTEE
### WON SPECIAL ~ HOUSE:
# -- BOGH (russ)
# -- CHU (judy)
### WON SPECIAL ~ SENATE:
# -- SOTO (nell)
### DROP:
# -- leja, jan --- gave up seat after pleading guilty to campaign finance violations -- https://www.latimes.com/archives/la-xpm-2000-dec-02-mn-60115-story.html


# ~~~~~~~~~~~~~~~~~~~~ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~~~~~~~
# -----> Dropping 425 bill(s) sponsored by COMMITTEE
#### ---------> No issues!

# ~~~~~~~~~~~~~~~~~~~~ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~~~~~~~
# -----> Dropping 289 bill(s) sponsored by COMMITTEE
### WON SPECIAL ~ HOUSE:
# -- LIEU (ted)

# ~~~~~~~~~~~~~~~~~~~~ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~~~~~~~
# session chamber    N  AIC  ABC PASS LAW
# 1 2007_2008       a 2993 2546 2625 1659 844
# 2 2007_2008       s 1739 1432 1339  988 541
### WON SPECIAL ~ HOUSE:
# -- FUENTES (feloipe)
# -- FURUTANI (warren)
### WON SPECIAL ~ SENATE:
# -- HARMAN (tom)
### IN HOUSE:
# -- ALARCON (richard) -- resigned after winning seat on LA City Council
# ------> Was in assembly for 102 days, shortest time since 1981

# ~~~~~~~~~~~~~~~~~~~~ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~~~~~~~
# -----> Dropping 376 bill(s) sponsored by COMMITTEE
### WON SPECIAL ~ HOUSE:
# -- BRADFORD (steven)
# -- GATTO (mike) ---- Author of a committee bill after updated --- need to decide if using introdcuer or final sponsor
# -- NORBY (chris)

# ~~~~~~~~~~~~~~~~~~~~ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~~~~~~~
# -----> Dropping 316 bill(s) sponsored by COMMITTEE
### WON SPECIAL ~ SENATE:
# -- BLAKESLEE (sam)
# -- EMMERSON (bill)
# -- LIEU (ted)
### DROP:
# oropeza, jenny -- died october 20, 2010


# ~~~~~~~~~~~~~~~~~~~~ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~~~~~~~
# -----> Dropping 303 bill(s) sponsored by COMMITTEE
### WON SPECIAL ~ HOUSE:
# -- DABABNEH
# -- GONZALEZ (lorena)
# -- RIDLEY-THOMAS (SEBASTIAN ---> KLARNER HAS HIM AS HIS FATHER, MARK)
# -- RODRIGUEZ (freddie)
### WON SPECIAL ~ SENATE:
# -- LIEU (ted, past session)
# -- NIELSEN (jim)
# -- VIDAK (andy)
#### DROP:
# -- calderon, charles m. --> Retired after winning reelection; son, Ian Calderon, replaced him
# ********* Note: KLarner has charles continuing on so fixed at top (though IDs remain the same)


# ~~~~~~~~~~~~~~~~~~~~ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~~~~~~~
# -----> Dropping 315 bill(s) sponsored by COMMITTEE
#### WON SPECIAL ~ SENATE:
# -- GLAZER (steve)
# -- HALL (isadore, iii)
# -- MOORLACH (john)
# -- MORRELL (mike)
# -- RUNNER (sharon)


# ~~~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~~~~~~~
# -----> Dropping 285 bill(s) sponsored by COMMITTEE
### WON SPECIAL ~ HOUSE:
# -- CARILLO (wendy maria carillo dono)
### IN HOUSE:
# -- rendon, anthony == speaker
### Name Fix
# -- Lorena Gonzalez == Lorena Gonzalez Fletcher


# filter(klarner, grepl("runner", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 23 & sen == 1 & outcome == 'w') %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)


########################################################################################################################################################
########################################################################################################################################################
######## NAME FIX RECORDS ---- 1993 - 2018 = CLEAN
########################################################################################################################################################
########################################################################################################################################################

library(readr)

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
  name_sub <- filter(LES, grepl(glue("^{name}"), sponsor))
  # If there is only ONE UNIQUE id that matches the name
  if(any(!is.na(name_sub$klarner_id)) & length(unique(na.omit(name_sub$klarner_id))) == 1 ){
    LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$sponsor <- unique(name_sub[!is.na(name_sub$klarner_id),]$sponsor) 
    LES[grepl(name, LES$sponsor) & is.na(LES$klarner_name),]$klarner_name <- unique(na.omit(name_sub$klarner_name))
    LES[grepl(name, LES$sponsor) & is.na(LES$klarner_id),]$klarner_id <- unique(na.omit(name_sub$klarner_id))   
    print(glue(' ~~ {name} ~~ Matched to --> {unique(na.omit(name_sub$klarner_name))}'))
  }
}

### Manual Fixes
LES[LES$sponsor == "miller" & LES$term == "1995_1996",]$klarner_id <- 16351
LES[LES$sponsor == "miller" & LES$term == "1995_1996", c('sponsor', 'klarner_name')] <- "miller, gary"

# LES[LES$sponsor == "chu" & LES$term == "2001_2002",]$klarner_id <- 15735
# LES[LES$sponsor == "chu" & LES$term == "2001_2002", c('sponsor', 'klarner_name')] <- "chu, judy"

### FOUR Still missing = One-Term Specials or Elected in Specials
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[1]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl(paste0('^', name), cand)) %>% select(cand, year, etype, sen, outcome, candid) # %>%  View()
# rm(name, missing, name_sub, still_missing)

### Fix Missing
name_matches <- data.frame(LES_name = 'dababneh, matthew', k_name = 'dababneh, matt')
name_matches <- add_row(name_matches, LES_name = 'glazer, steven', k_name = 'glazer, steve')
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
# filter(klarner, candid == 13434) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

### Not In Klarner
fill_missing <- data.frame(LES_name = "roberti", new_name = 'roberti, david', party = 'd', district = 23, exper = 'pastother', k_id = 13150)
fill_missing <- add_row(fill_missing, LES_name = "bock, elizabeth", new_name = 'bock, audie elizabeth', party = 'nonmaj', district = 16, exper = 'none', k_id = 14102) # Green party, switched to dem in 2000
### 2017-2018 Special
fill_missing <- add_row(fill_missing, LES_name = "carrillo, wendy", new_name = 'carrillo, wendy', party = 'd', district = 51, exper = 'none', k_id = NA)
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz', k_id = NA)

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
  if(!is.na(fill_missing[i,]$k_id)){
    LES[LES$sponsor == fill_missing[i,]$new_name & is.na(LES$klarner_id),]$klarner_id <- fill_missing[i,]$k_id
  }
}

rm(klarner_sub, this_sponsor_LES, c, name, t, sponsor_rows, sponsor_sub, second_year)
rm(fill_missing, i)

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
senate <- filter(hf_data, chamber == "Senate")
senate$year <- senate$year + 2
senate$term <- paste0(senate$year + 1, "_", senate$year + 2)
senate$MajorityMember <- NA

## Drop Don Rogers -- Recorded in Both 1990 and 1992 Elections
senate <- filter(senate, !(term == '1993_1994' & CandId == 12970))
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
ideo$match_name <- tolower(ideo$name)
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
    ### Try Data Name
    ideo_match <- filter(ideo[check_last,], match_name == LES[i,]$data_name)
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

# **** Clearing ALL Multi-Matches as they are Last Name Only*********
LES <- LES %>%
  group_by(SM_name) %>% 
  mutate(fix_mismatch = ifelse(!is.na(SM_name) & length(unique(sponsor)) > 1, 1, 0)) %>% 
  ungroup()

LES[LES$fix_mismatch %in% 1, c('SM_name', 'SM_party', 'np_score') ] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
## ** Notable Names -- Karen 'Jackie' Speier
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame() #%>% View()
# filter(ideo, grepl('baca', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'alarcon, richard', SM_name = 'Alarcón, Richard')
name_matches <- add_row(name_matches, LES_name = 'allen, ben', SM_name = 'Allen, Ben')
name_matches <- add_row(name_matches, LES_name = 'allen, doris', SM_name = 'Allen')
name_matches <- add_row(name_matches, LES_name = 'allen, michael', SM_name = 'Allen, Michael')
name_matches <- add_row(name_matches, LES_name = 'alquist, alfred e.', SM_name = 'Alquist')
name_matches <- add_row(name_matches, LES_name = 'alquist, elaine white', SM_name = 'Alquist, Elaine')
name_matches <- add_row(name_matches, LES_name = 'baca, joe', SM_name = 'Baca')
# ************ SM Name is wrong here -- this record should be Baca, Joe Jr.
name_matches <- add_row(name_matches, LES_name = 'baca, joe jr.', SM_name = 'Baca, Joe Sr.')
name_matches <- add_row(name_matches, LES_name = 'bates, patricia c.', SM_name = 'Bates, Patricia')
name_matches <- add_row(name_matches, LES_name = 'bates, tom', SM_name = 'Bates')
name_matches <- add_row(name_matches, LES_name = 'bermudez, rudy', SM_name = 'Bermúdez, Rudy')
name_matches <- add_row(name_matches, LES_name = 'calderon, charles m.', SM_name = 'Calderon')
name_matches <- add_row(name_matches, LES_name = 'calderon, ian charles', SM_name = 'Calderon, Ian')
name_matches <- add_row(name_matches, LES_name = 'calderon, ronald s.', SM_name = 'Calderon, Ronald')
name_matches <- add_row(name_matches, LES_name = 'calderon, thomas m.', SM_name = 'Calderon, Thomas')
name_matches <- add_row(name_matches, LES_name = 'campbell, john', SM_name = 'Campbell, John III')
name_matches <- add_row(name_matches, LES_name = 'campbell, robert j.', SM_name = 'Campbell, Robert J.')
name_matches <- add_row(name_matches, LES_name = 'cannella, anthony', SM_name = 'Cannella, Anthony')
name_matches <- add_row(name_matches, LES_name = 'cannella, sal', SM_name = 'Cannella')
name_matches <- add_row(name_matches, LES_name = 'cardenas, tony', SM_name = 'Cárdenas, Tony')
# name_matches <- add_row(name_matches, LES_name = 'gordon, mike', SM_name = 'zzzzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'gordon, rich', SM_name = 'Gordon, Richard')
name_matches <- add_row(name_matches, LES_name = 'hill, frank', SM_name = 'Hill')
name_matches <- add_row(name_matches, LES_name = 'hill, jerry', SM_name = 'Hill, Gerald')
name_matches <- add_row(name_matches, LES_name = 'jones, bill', SM_name = 'Jones')
name_matches <- add_row(name_matches, LES_name = 'jones, brian', SM_name = 'Jones, Brian')
name_matches <- add_row(name_matches, LES_name = 'jones, dave', SM_name = 'Jones, Dave')
name_matches <- add_row(name_matches, LES_name = 'mcleod, gloria negrete', SM_name = 'Negrete McLeod, Gloria')
name_matches <- add_row(name_matches, LES_name = 'miller, gary', SM_name = 'Miller')
name_matches <- add_row(name_matches, LES_name = 'miller, jeff', SM_name = 'Miller, Jeff')
name_matches <- add_row(name_matches, LES_name = 'montanez, cindy', SM_name = 'Montañez, Cindy')
name_matches <- add_row(name_matches, LES_name = 'mountjoy, dennis', SM_name = 'Mountjoy, Dennis Lee')
name_matches <- add_row(name_matches, LES_name = 'mountjoy, richard (dick)', SM_name = 'Mountjoy')
name_matches <- add_row(name_matches, LES_name = 'mullin, gene', SM_name = 'Mullin, Eugene')
name_matches <- add_row(name_matches, LES_name = 'nunez, fabian', SM_name = 'Núñez, Fabian')
name_matches <- add_row(name_matches, LES_name = 'perez, john a.', SM_name = 'Pérez, John')
name_matches <- add_row(name_matches, LES_name = 'perez, manuel', SM_name = 'Pérez, V.') # Victor Manuel Perez
name_matches <- add_row(name_matches, LES_name = 'saldana, lori', SM_name = 'Saldaña, Lori')
name_matches <- add_row(name_matches, LES_name = 'strickland, tony', SM_name = 'Strickland, Anthony')
name_matches <- add_row(name_matches, LES_name = 'thompson, bruce', SM_name = 'Thompson, Bruce')
name_matches <- add_row(name_matches, LES_name = 'thompson, mike', SM_name = 'Thompson')
name_matches <- add_row(name_matches, LES_name = 'torres, art', SM_name = 'Torres')
name_matches <- add_row(name_matches, LES_name = 'torres, norma j.', SM_name = 'Torres, Norma')
name_matches <- add_row(name_matches, LES_name = 'wright, cathie', SM_name = 'Wright')
name_matches <- add_row(name_matches, LES_name = 'wright, roderick', SM_name = 'Wright, Roderick')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

#### Manual Edits -- First two have X Party listed; Mcpherson is split over two rows..
# filter(LES, sponsor == 'mcpherson, bruce') %>% select(1:6, party, SM_party, SM_name)
# filter(ideo, grepl('mcpherson', tolower(name))) 
LES[LES$sponsor == 'cortese, dominic l. (dom)',]$SM_name <-  ideo[ideo$name == 'Cortese' & ideo$party %in% 'D',]$name
LES[LES$sponsor == 'cortese, dominic l. (dom)',]$SM_party <- ideo[ideo$name == 'Cortese' & ideo$party %in% 'D',]$party
LES[LES$sponsor == 'cortese, dominic l. (dom)',]$np_score <- ideo[ideo$name == 'Cortese' & ideo$party %in% 'D',]$np_score

LES[LES$sponsor == 'horcher, paul v.',]$SM_name <-  ideo[ideo$name == 'Horcher' & ideo$party %in% 'R',]$name
LES[LES$sponsor == 'horcher, paul v.',]$SM_party <- ideo[ideo$name == 'Horcher' & ideo$party %in% 'R',]$party
LES[LES$sponsor == 'horcher, paul v.',]$np_score <- ideo[ideo$name == 'Horcher' & ideo$party %in% 'R',]$np_score

LES[LES$sponsor == 'mcpherson, bruce',]$SM_name <-  ideo[ideo$name == 'McPherson, Bruce' & ideo$senate1997 %in% 1,]$name
LES[LES$sponsor == 'mcpherson, bruce',]$SM_party <- ideo[ideo$name == 'McPherson, Bruce' & ideo$senate1997 %in% 1,]$party
LES[LES$sponsor == 'mcpherson, bruce',]$np_score <- ideo[ideo$name == 'McPherson, Bruce' & ideo$senate1997 %in% 1,]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname)


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1993 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:1994, 1997:2020) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzz) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

## *** 1995-1996 *** 40-40 Vote for speaker, with 1 R defecting to Willie Brown (D) -- https://www.latimes.com/archives/la-xpm-1994-12-06-mn-5661-story.html
## ---> Jan 24, 1995 - Brown elected speaker 40-39 after expelling an R; agreed to share power -- https://www.latimes.com/archives/la-xpm-1995-01-25-mn-24204-story.html
## ---> Defecting R (Horcher) became indep; Committee chairs split; half D, half R, with Brown presiding
## ---> June, 1995 - Dems elect Doris Allen (R) speaker; no R's vote yea; Brown was in chamber as D leader for 6 months thereafter until resigning to become SF Mayor
## ---> https://www.latimes.com/archives/la-xpm-1995-06-06-mn-9978-story.html
## ---> Sep. 1995 -- Brian Setencich (R) elected speaker... with support of Allen and D's...
## ---> Jan. 1996 -- Curt Pringle (R) elected speaker (by republicans..) -- https://www.latimes.com/archives/la-xpm-1996-01-05-mn-21284-story.html
## **** CODING ALL 0 ********

### Senate -- 1993 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzz) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()

### Remove Numbers from Names -- Need to check to make sure not collapsing two people to one name
LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

### Remove Nicknames
LES$sponsor <- str_trim(gsub(" \\([^\\)]+\\)", "", LES$sponsor))

#### Manual Fixes
LES[LES$sponsor %in% c("negretemcleod, gloria", "mcleod, gloria negrete"),]$sponsor <- "mcleod, gloria negrete"
LES[LES$sponsor == "jackson, hannahbeth",]$sponsor <- "jackson, hannah-beth"
LES[LES$sponsor == "achadjian, k. h. katcho",]$sponsor <- "achadjian, khatchik h."
LES[LES$sponsor == "jonessawyer, reggie",]$sponsor <- "jones-sawyer, reginald sr."
LES[LES$sponsor == "chang, lingling",]$sponsor <- "chang, ling-ling"
LES[LES$sponsor == "perez, manuel",]$sponsor <- "perez, victor manuel"
# LES[LES$sponsor == "zzzzzzzz"),]$sponsor <- "zzzzzzzz"

### Party Fix
LES[LES$sponsor == "killea, lucy" & LES$term %in% c("1993_1994", "1995_1996"),]$party <- 'nonmaj'

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

### Save by Term
for(t in unique(LES$term)){
  LES_sub <- filter(LES, term == t)
  write.csv(LES_sub, glue("Merged/{this_state}_LES_{t}_M.csv"), row.names = FALSE)  
}

#################################
#### Plot
###################################

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


ggplot2::ggplot(LES, aes(x = np_score, y = LES)) + 
  geom_point(aes(color = party)) + 
  geom_smooth(formula = "y ~ x", method = 'lm', se = FALSE) + 
  facet_wrap(~ term) + 
  scale_color_manual(values=c("dodgerblue2", "purple2", "red2"))

#########
ggplot(LES, aes(x = np_score, y = LES, color = party)) + 
  geom_point() + 
  geom_smooth(formula = "y ~ x", method = 'lm', se = FALSE) + 
  facet_wrap(~ term) + 
  scale_color_manual(values=c("dodgerblue2", 'gray50', "red2"))

##### CHECK OUTLIERS
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')











######################
#### FASTLINK MATCHING
#######################
# LES_match <- select(LES, sponsor, klarner_id) %>% 
#   rename(match_name = sponsor) %>%
#   mutate(last_name = gsub(',.+', '', match_name)) %>%
#   distinct()
# 
# ## MATCH TO SHOR/MCCARTY DATA --- Low threshold shouldn't be a problem because 1:1 matching and will take highest probability match
# library(fastLink)
# ideo_matches <- fastLink(LES_match, ideo,
#                          varnames = c("match_name", "last_name"),
#                          stringdist.match =  c("match_name", "last_name"),
#                          # partial.match =  c("match_name", "last_name"),
#                          dedupe.matches = TRUE, ### All LES Names Should be Unique Now
#                          cut.a = .90,
#                          threshold.match = .85)
# ideo_matches <- bind_cols(LES_match[ideo_matches$matches$inds.a,], ideo[ideo_matches$matches$inds.b, c('name', 'party', 'np_score')])
# ideo_matches <- rename(ideo_matches, SM_name = name, SM_party = party) %>%
#   select(-klarner_id) %>%
#   select(-last_name) %>%
#   distinct()
# 
# LES <- left_join(LES, ideo_matches, by = c("sponsor" = "match_name"))
# 
# for(i in 1:nrow(LES)){
#   if(is.na(LES[i,]$SM_name)){
#     check_last <- grep(gsub(",.+|\\'", '', LES[i,]$sponsor), gsub(",.+|\\'", '', tolower(ideo$name) ))  
#     ## Check First initial if more than 1 last name match
#     if(length(check_last) > 1){
#       ideo_match <- ideo[check_last,][substring(gsub('.+, ', '', tolower(ideo[check_last,]$name)), 1, 1) == substring(gsub(".+, ", '', LES[i,]$sponsor), 1, 1),]
#     } else{
#       ideo_match <- ideo[check_last,]
#     }
#     if(nrow(ideo_match) == 1){
#       LES[LES$sponsor == LES[i,]$sponsor,]$SM_name <- ideo_match$name
#       LES[LES$sponsor == LES[i,]$sponsor,]$SM_party <- ideo_match$party
#       LES[LES$sponsor == LES[i,]$sponsor,]$np_score <- ideo_match$np_score
#       cat(" \n Manually matched ", toupper(LES[i,]$sponsor), " to ", toupper(ideo_match$name), "\n .")
#     }
#   }
# }
# 
# ### Mismatches
# LES[LES$sponsor == 'gordon, mike',c('SM_name', 'SM_party', 'np_score')] <- NA
# LES[LES$sponsor == 'hill, jerry', c('SM_name', 'SM_party', 'np_score')] <- NA
# LES[LES$sponsor == 'thompson, bruce', c('SM_name', 'SM_party', 'np_score')] <- NA
# 
# ### MANUAL FIXES -- Can Cross-Check Years to Make sure Correct Match
# # filter(LES, is.na(np_score)) %>% select(sponsor, klarner_name, data_name) %>% distinct()
# # ilter(ideo, grepl(', mike', tolower(name))) %>% select(name, party, st, np_score)
# 
# LES[LES$sponsor == "millendermcdonald, juanita", 'np_score'] <- ideo[ideo$name == "McDonald",]$np_score
# LES[LES$sponsor == "millendermcdonald, juanita", 'SM_party'] <- ideo[ideo$name == "McDonald",]$party
# LES[LES$sponsor == "millendermcdonald, juanita", 'SM_name'] <- ideo[ideo$name == "McDonald",]$name
# 
# LES[LES$sponsor == "hill, jerry", 'np_score'] <- ideo[ideo$name == "Hill, Gerald",]$np_score
# LES[LES$sponsor == "hill, jerry", 'SM_party'] <- ideo[ideo$name == "Hill, Gerald",]$party
# LES[LES$sponsor == "hill, jerry", 'SM_name'] <-  ideo[ideo$name == "Hill, Gerald",]$name
# 
# LES[LES$sponsor == "thompson, bruce", 'np_score'] <- ideo[ideo$name == "Thompson, Bruce",]$np_score
# LES[LES$sponsor == "thompson, bruce", 'SM_party'] <- ideo[ideo$name == "Thompson, Bruce",]$party
# LES[LES$sponsor == "thompson, bruce", 'SM_name'] <-  ideo[ideo$name == "Thompson, Bruce",]$name
# 
# LES[LES$sponsor == "perez, john a.", 'np_score'] <- ideo[ideo$name == "Pérez, John",]$np_score
# LES[LES$sponsor == "perez, john a.", 'SM_party'] <- ideo[ideo$name == "Pérez, John",]$party
# LES[LES$sponsor == "perez, john a.", 'SM_name'] <-  ideo[ideo$name == "Pérez, John",]$name
# 
# LES[LES$sponsor == "perez, manuel", 'np_score'] <- ideo[ideo$name == "Pérez, V.",]$np_score ##== V Manuel Perez
# LES[LES$sponsor == "perez, manuel", 'SM_party'] <- ideo[ideo$name == "Pérez, V.",]$party
# LES[LES$sponsor == "perez, manuel", 'SM_name'] <-  ideo[ideo$name == "Pérez, V.",]$name
# 
# LES[LES$sponsor == "talamanteseggman, susan", 'np_score'] <- ideo[ideo$name == "Eggman, Susan",]$np_score
# LES[LES$sponsor == "talamanteseggman, susan", 'SM_party'] <- ideo[ideo$name == "Eggman, Susan",]$party
# LES[LES$sponsor == "talamanteseggman, susan", 'SM_name'] <- ideo[ideo$name == "Eggman, Susan",]$name
# 
# rm(ideo, ideo_matches, LES_match, ideo_match, check_last, i)

