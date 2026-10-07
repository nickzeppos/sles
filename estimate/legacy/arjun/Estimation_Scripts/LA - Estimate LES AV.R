
##########################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** LOUISIANA *** BY SESSION
##########################################################################

###################################
## TERMS: 
## ---- FOUR YEARS -- **** Missing first year (1996) of 1996-1999 Term ***
## ---- Bills do NOT Carryover --> Must be reintroduced in each term.
## SPECIAL SESSIONS:
## ---- Seperate files, bill numbers re-start; Merge on Special
## MEMBER LISTS:
## ---- SENATE: http://senate.la.gov/Documents/Membership/1880membership.pdf
## PROCESS:
## ---- Senate Rules: http://senate.legis.state.la.us/documents/rules/rulesoforder.pdf
## ------> Senate Rule 13.11 -- Floor can mandate a committee hold a hearing nad report a bill, so even if discharged, AIC
## ------> Not clear this is true in House (e.g,. 'Discharged from the Committee on {ZZZZ}')
## Sponsorship/Authorship
## ---- 
###########################
##### ******* NOTES *****
# (1) *** We do NOT have data for the 1996 of the 1996_1999 Term ***
# (2) Bills that are 'reported without action' ---> Still coding as AIC -- Typically include a vote tally, meaning decision to report vs not
# (3) CODING reported unfavorably here as ABC because seems to hit the calendar (or typically still result in something here); Past contexts more likely to die or have no following action
# (4) Speaker typically chosen by the governor! Terms are oddly adjacent to normal terms.


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
library(tibble)
library(inexact)

this_state <- 'LA'
keep_types <- c("HB", "SB")

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

data_files <- list.files(glue("../../../State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
sessions <- gsub(".+Bill_Details_|.csv", '', bill_files)
rm(data_files, bill_files)

### Terms/Sessions and Filespaths
terms <- 2020
t = terms
t_yrs <- as.numeric(terms)
t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 3}'))
t_sessions = sessions[grepl(as.character(t),sessions) | grepl(as.character(t+1),sessions) | 
                        grepl(as.character(t+2),sessions) | grepl(as.character(t+3),sessions)]

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- bind_rows(read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t+1}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t+2}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t+3}.csv")))

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
  select(state = State, term, year, bill_id, everything()) %>% 
  mutate(term = t_yrs) # maryland fix



###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[6]



### Skip Previously Estimated
if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
  print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
  next
}

###### SESSION IN PROGRESS
print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_sessions[1]}.csv")
bills <- read.csv(bill_path)   
bills$session <- as.character(bills$session)

### If multiple sessions in different files, read in those as well
if(length(t_sessions) > 1){
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{s}.csv")
    s_bills <- read.csv(bill_path)
    s_bills$session <- as.character(s_bills$session)
    bills <- bind_rows(bills, s_bills)
  }
  rm(s, s_bills)
}

### Clean Term/Session Variables
bills$term <- t_yrs
bills$session_type <- recode(substring(bills$session_key, 3, nchar(bills$session_key)), '1ES' = 'SS1', '2ES' = 'SS2', '3ES' = 'SS3')
bills$session <- paste(bills$session, bills$session_type, sep = '-')
bills <- select(bills, -session_type)

### Check for duplicates
if(nrow(bills) != nrow(distinct(bills))){
  cat(" \n ~~~> DUPLICATE BILLS \n\n .")
  break
}

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_number)
bills$bill_id <- paste0(gsub('[0-9]+', '', bills$bill_id), str_pad(gsub('[A-Z]+', '', bills$bill_id), 4, pad = '0'))
bills <- arrange(bills, session, bill_id)

############### Drop Resolutions, Messages, Communications, Reports
all_bills <- bills
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)

############### Standardize Sponsors + Ensuring Cosponsorship match down the line

#### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
bills$author <- tolower(bills$author)
bills$author <- gsub('á|ã¡', 'a', bills$author)
bills$author <- gsub('é|ã©', 'e', bills$author)
bills$author <- gsub('ó', 'o', bills$author)
bills$author <- gsub('í', 'i', bills$author)
bills$author <- gsub('ñ|ã±', 'n', bills$author)

### All (other) authors --- Sometimes this includes the main sponsor, sometimes not
bills$all_authors <- tolower(bills$all_authors)
bills$all_authors <- gsub('á|ã¡', 'a', bills$all_authors)
bills$all_authors <- gsub('é|ã©', 'e', bills$all_authors)
bills$all_authors <- gsub('ó', 'o', bills$all_authors)
bills$all_authors <- gsub('í', 'i', bills$all_authors)
bills$all_authors <- gsub('ñ|ã±', 'n', bills$all_authors)  

#### Adjusting for Bills By Request -- Need to do BEFORE splitting
if(any(grepl('\\(br\\)|by request| br$', bills$authors))){
  print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
  #print(glue("-----> KEEPING {nrow(filter(bills, grepl('(br)|by request', authors)))} bill(s) introduced BY REQUEST"))
}

#### Fix Errors with Author/All Authors Variable -- Seems to be miscoded sometimes when same last name
## filter(bills, author != gsub(';.+', '', all_authors)) %>% View()
if(t_yrs == "1996_1999"){
  ### Jean Doerge -- Husband, everettt, died April 1998 -- All bills prior to 1999 are him (and show him hin all_authors)
  bills[bills$author == 'jean m. doerge' & bills$session != '1999-RS',]$author <- 'everett g. doerge'
  ### Some Bills by Wilson Fields Coded as Cleo Fields in All Authors Variable; Cleo not elected until Dec 1997
  bills$all_authors <- ifelse(bills$author == 'wilson fields' & gsub(';.+', '', bills$all_authors) == 'cleo fields', gsub('cleo fields', 'wilson fields', bills$all_authors), bills$all_authors)
  ### John Guidry bills coded as Jesse Guidry in All Authors -- Jessed Served in House through 1981..
  bills$all_authors <- ifelse(bills$author == 'john m. guidry' & gsub(';.+', '', bills$all_authors) == 'jesse guidry', gsub('jesse guidry', 'john m. guidry', bills$all_authors), bills$all_authors)
}else if(t_yrs == "2004_2007"){
  ### Bill Strain -- He died in 1999, should be Michael... This is correct in all_authors var.
  bills[bills$author == 'r.h. "bill" strain',]$author <- 'michael g. strain'
  ### Variations in Derrick Shepherds Names
  bills$all_authors <- ifelse(bills$author == 'derrick shepherd' & gsub(';.+', '', bills$all_authors) == 'derrick d. t. shepherd', gsub('derrick d. t. shepherd', 'derrick shepherd', bills$all_authors), bills$all_authors)
}

#### Fix Last, First Name Formattings
if(t_yrs == '2008_2011'){
  bills[bills$author == 'badon, bobby g.',]$author <- 'bobby g. badon'
  bills$all_authors <- gsub('badon, bobby g.', 'bobby g. badon', bills$all_authors)
}

#### Fix MUltiple Formats
if(t_yrs == "2016_2019"){
  bills[bills$author %in% c("c. denise marcelle", "denise marcelle"),]$author <- 'denise marcelle'
  bills$all_authors <- gsub("c. denise marcelle", "denise marcelle", bills$all_authors)
} 

# if(t_yrs == "2020_2023"){
#   
#   bills[bills$author %in% c("r. bret allain"),]$author <- 'r. l. bret allain, ii'
#   bills$all_authors <- gsub("r. bret allain", "r. l. bret allain, ii", bills$all_authors)
# }

#############
#### LES Sponsor Variable
bills$LES_sponsor <- bills$author
table(bills$LES_sponsor)



###################
###### Merge in S&S Bills
###################
# *** For LOUISIANA: Merging on Year and Special -- When multiple specials, assuming special with most proposed bills


if(t_yrs == "2020_2023"){
  SS_bills$bill_id[SS_bills$bill_id=="SB60006"] = "SB0006"
  SS_bills$bill_id[SS_bills$bill_id=="SB50005"] = "SB0005"
  SS_bills$bill_id[SS_bills$bill_id=="SB10001"] = "SB0001"
  SS_bills$bill_id[SS_bills$bill_id=="HB80008"] = "HB0008"
  SS_bills$bill_id[SS_bills$bill_id=="HB40004"] = "HB0004"
  SS_bills$bill_id[SS_bills$bill_id=="HB10001"] = "HB0001"
}

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, year, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS, year,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,author), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",author, ignore.case=T)) %>%
  arrange(author) 
unique(missing_SS_bills$bill_id) 




# now check to see if there are duplicate joins


duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills %>% mutate(year = as.integer(substr(session,1,4))), 
            by = c("bill_id", "term", "year")) %>%
  group_by(rowname) %>% mutate(count = n()) %>%
  ungroup() %>% 
  select(count,bill_id,term,session, Title, short_title, year) %>% 
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
  bills2 <- bills %>% mutate(year = as.integer(substr(session,1,4)))%>% 
    left_join(SS_term %>% select(-Title) %>% distinct(), by = c("bill_id", "term","year")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  SS_term2 = SS_term %>% 
    left_join(bills %>% mutate(year = as.integer(substr(session,1,4))) %>% select(bill_id,term,session, year),by=c("bill_id","term","year"))
  if(!identical(c(nrow(bills2),nrow(SS_term2)),orig_row_n )){print("merge failed"); break} else{
    bills = bills2; SS_term = SS_term2; rm(bills2, SS_term2)
  }
}

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
  SS_term %>% group_by(bill_id, term) %>%
    mutate(count = n()) %>% filter(count > 1) %>% arrange(bill_id)
  
  # see if they are equal after removing double-counted bills. this is a crude proxy, you should examine more closely
  all.equal(SS_in_bills + nrow(anti_join(SS_term, bills, by = c("bill_id", "term", "session")))+
              nrow(SS_term %>% group_by(bill_id, term) %>%
                     mutate(count = n()) %>% filter(count > 1))/2,
            nrow(SS_term ))
}

rm(all_bills, missing_SS_bills, duplicate_SS_bills)

#######################################################
############### Code Commemorative
########################################################

bills <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session')) # %>% View()
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)

###### CHeck Missing Sponsors
if(nrow(filter(bills, LES_sponsor == '')) > 0){
  print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without an author"))
  bills <- filter(bills, !(LES_sponsor == ''))     
}

#### DROP Committee Bills
if(any(grepl('committee|^rules$', bills$LES_sponsor))){
  print(glue("-----> DROPPING {nrow(filter(bills, grepl('committee|^rules$', LES_sponsor)))} bill(s) introduced BY COMMITTEE"))
  bills <- filter(bills, !grepl('committee|^rules$', LES_sponsor))
}

########################################################
############### Code Bill History
########################################################
bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
bill_hist <- read.csv(bill_hist_path)
bill_hist$session <- as.character(bill_hist$session)

### If multiple sessions in different files, read in those as well
if(length(t_sessions) > 1){
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
    s_hist <- read.csv(bill_path)
    s_hist$session <- as.character(s_hist$session)
    bill_hist <- bind_rows(bill_hist, s_hist)
  }
  rm(s, s_hist)
}

### Clean Term/Session Variables
bill_hist$term <- t_yrs
bill_hist$session_type <- recode(substring(bill_hist$session_key, 3, nchar(bill_hist$session_key)), '1ES' = 'SS1', '2ES' = 'SS2', '3ES' = 'SS3')
bill_hist$session <- paste(bill_hist$session, bill_hist$session_type, sep = '-')
bill_hist <- select(bill_hist, -session_type)

######## Standardize BillHist Bill IDs
bill_hist <- rename(bill_hist, bill_id = bill_number)
bill_hist$bill_id <- paste0(gsub('[0-9]+', '', bill_hist$bill_id), str_pad(gsub('[A-Z]+', '', bill_hist$bill_id), 4, pad = '0'))

bill_hist <- arrange(bill_hist, session, bill_id, order)

### Standardize Chamber Variable
bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate")

#### Adjusting '^reported without action' to not count as AIC
#### ---> NOT Doing this because often includes a vote tally so still action -- maybe just means no bill adjustments or hearing?
# bill_hist$action <- ifelse(grepl('^Reported without action', bill_hist$action), 
#                            gsub('^Reported without action', 'not AIC~Reported without action', bill_hist$action), bill_hist$action)
# 
#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c('^reported', 'scheduled to be heard', 'considered but not reported', 'reported favorably', 
           'reported with [a-z]', 'reported by', 'reported without a[a-z]+')
abc_t <-c('^reported', 'reported without action', 'special order', 'read third time', 'engrossed', 
          'third reading', 'regular calendar', 'called from the calendar', 
          'floor amendments')
### 'returned to the calendar' creates a lot of errors, often immediately after intorduction before assigned to comm
### Comm can report Unfavorably, but still gets read to the Chamber (see Senate Rule 10.12) --> Any bill that gets reported --> ABC
pc_t <- c('finally passed', 'passed.+sent to the (house|senate)', 'ordered to the (house|senate)',
          'received in the (house|senate)', 'enrolled')
law_t <- c('signed by the governor', 'act no. [0-9]+', 'act # [0-9]+', '^effective date')
# filter(bill_hist, grepl('^returned to the calendar', tolower(action))) %>% mutate(action = gsub('\\(.+', '', action)) %>% distinct(action) 

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


read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-4}_{t-1}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% print()

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-8}_{t-5}_Bill_Stage_Codings.csv")) %>% 
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
  select(bill_id, term, session, SS) %>%
  left_join(all_bill_stages, ., by = c('bill_id', 'term', "session")) %>%
  mutate(SS = ifelse(is.na(SS), 0, SS))

### Adjust Commems if SS == 1
table(all_bill_stages$SS, all_bill_stages$commem)
all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
rm(SS_term)

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
  mutate(term = t_yrs) %>%
  group_by(LES_sponsor, chamber, term) %>%
  summarize(num_sponsored_bills = n(), 
            sponsor_pass_rate = sum(passed_chamber) / n(),
            sponsor_law_rate = sum(law) / n()) %>%
  ungroup()

######## Cosponsorship Info --- INCLUDES Main Sponsor
all_sponsors$num_cosponsored_bills <- NA
bills$cospon_match <- bills$all_authors #paste(bills$authors, bills$coauthors, sep = ';')
for(i in 1:nrow(all_sponsors)){
  c_sub <- filter(bills, substring(bill_id, 1, 1) == all_sponsors[i,]$chamber)
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
  ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
}
bills <- select(bills, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))


if(min(all_sponsors$num_cosponsored_bills) < 0){
  print("negative cosponsors"); break
} else{print("cosponsors valid")}


###########
## PARSE NAMES
parsed_names <- map_df(all_sponsors$LES_sponsor, parse_names) %>% select(-salutation) %>% distinct() ### Need distinct in case people switch chambers
parsed_names$first_name <- gsub('\"', '', parsed_names$first_name)
parsed_names$nickname <- gsub('\\"', '', str_extract(parsed_names$middle_name, '\\".+\\"'))
parsed_names$middle_name <- gsub('  +', ' ', gsub('\\".+\\"', '', parsed_names$middle_name))
parsed_names$last_name <- gsub(',$', '', parsed_names$last_name)

all_sponsors <- left_join(all_sponsors, parsed_names, by = c("LES_sponsor" = "full_name"))
all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update First/Last Names for Matching 
if(t_yrs %in% c('1996_1999') ){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "yvonne welch", "dorseywelch", all_sponsors$last_name)
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "naomi w. farve", "whitewarrenfarve", all_sponsors$last_name)
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "sharon weston", "westonbroome", all_sponsors$last_name)
}
if(t_yrs %in% c("2000_2003")){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "yvonne welch", "dorseywelch", all_sponsors$last_name)
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "sharon weston broome", "westonbroome", all_sponsors$last_name)  
}
if(t_yrs == "2004_2007"){
  all_sponsors[all_sponsors$LES_sponsor == '"tank" powell', c("first_name", "nickname")] <- list("henry", "tank")
  all_sponsors[all_sponsors$LES_sponsor == 'jalila jefferson',]$last_name <- 'jeffersonbullock'
}
if(t_yrs %in% c("2004_2007", "2008_2011")){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "yvonne dorsey", "dorseywelch", all_sponsors$last_name)
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "sharon weston broome", "westonbroome", all_sponsors$last_name)  
  all_sponsors[all_sponsors$LES_sponsor == 'karen gaudet st. germain',]$last_name <- 'saintgermain'
  all_sponsors[all_sponsors$LES_sponsor == 'cheryl gray evans',]$last_name <- 'gray'
  all_sponsors[all_sponsors$LES_sponsor == '"butch" gautreaux', c("first_name", "nickname")] <- list("d. a.", "butch")
}
if(t_yrs == '2008_2011'){
  all_sponsors[all_sponsors$LES_sponsor == 'regina ashford barrow',]$last_name <- 'ashfordbarrow'
  all_sponsors[all_sponsors$LES_sponsor == 'karen carter peterson',]$last_name <- 'carter'
  all_sponsors[all_sponsors$LES_sponsor == 'charmaine marchand stiaes',]$last_name <- 'marchand'
}
if(t_yrs %in% c("2012_2015")){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "yvonne dorsey-colomb", "dorseywelch", all_sponsors$last_name)
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "sharon weston broome", "westonbroome", all_sponsors$last_name)  
  all_sponsors[all_sponsors$LES_sponsor == 'karen gaudet st. germain',]$last_name <- 'saintgermain'
  all_sponsors[all_sponsors$LES_sponsor == 'sherri smith buffington',]$last_name <- 'cheek' # Previously cheek
}
if(t_yrs %in% c("2012_2015", "2016_2019")){
  all_sponsors[all_sponsors$LES_sponsor == 'regina ashford barrow',]$last_name <- 'ashfordbarrow'
  all_sponsors[all_sponsors$LES_sponsor == 'karen carter peterson',]$last_name <- 'carterpeterson'
  all_sponsors[all_sponsors$LES_sponsor == 'jim morris', c("first_name", "nickname")] <- list('james', 'jim')
}
if(t_yrs %in% c("2016_2019")){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "yvonne colomb", "dorseywelch", all_sponsors$last_name)
}



all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(LES_sponsor,substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name, middle_name, suffix, nickname))


legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(terms)) | startsWith(legiscan_sessions,as.character(terms+1)) |
                                        startsWith(legiscan_sessions,as.character(terms+2)) | startsWith(legiscan_sessions,as.character(terms+3))]

legiscan = foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% distinct() 

if(t_yrs == "2020_2023"){
  legiscan = bind_rows(legiscan,
                       legiscan %>% filter(people_id == 17901) %>% mutate(role = "Sen", district = "SD-007")) %>% 
    filter(name != 'R Allain') %>% 
   filter(! (people_id == 21508 & district == "HD-020")) %>% 
    filter(! (people_id %in% c(21352,17888) & party == "R")) %>%  # party switchers D -> R. they're D for a majority of the term tho, so remove the R types
    filter(! (people_id == 5328 & substr(district,1,1) == "S")) %>% # moves to house at the start of 20
    filter(! (people_id == 5328 & party == "R" & substr(district,1,1) == "H")) # goes to house and then switches
}



legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  mutate(match_name = name) %>% 
  mutate(match_name_chamber = tolower(paste(match_name,substr(district,1,1),sep="-"))) 



# inexact::inexact_addin()
# left: legiscan_adj
# right: all_sponsors
# method: osa
# mode: full

if(t_yrs == "2020_2023"){
  all_sponsors2 = 
    # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "william wheat-h" = 'william "bill" wheat , jr.-h',
        "regina barrow-s" = "regina ashford barrow-s",
        "vincent st. blanc-h" = "vincent j. st. blanc , iii-h",
        "kyle green-h" = "kyle m. green , jr.-h",
        "john illg-h" = 'john "big john" illg , jr.-h',
        "michael fesi-s" = 'michael "big mike" fesi-s',
        "reggie bagala-h" = NA_character_,
        "mack white-s" = 'mack bodi white, jr.-s',
        "aimee freeman-h" = "aimee adatto freeman-h",
        "wilford carter-h" = "wilford carter , sr.-h",
        "barbara freiberg-h" = "barbara reich freiberg-h",
        "fred mills-s" = "fred h. mills, jr.-s",
        "gary smith-s" = "gary l. smith, jr.-s",
        "patrick cortez-s" = "patrick page cortez-s",
        "robert owen-h" = 'robert "bob" owen-h',
        "john morris-s" = 'jay morris-s'
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


##############################################################################
############### Estimate Scores + Add in Relatd Variables
##############################################################################
### Check if bills in data without an ID'd sponsor
bills <- bills %>% #select(-sponsors, cosponsors) %>%
  rename(sponsor = LES_sponsor) %>%
  mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'H', 'H', 'S'))

### Standard LES: Same as Congressional Measure
source('../../Estimate LES/calc_LES_fx.R')

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

cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************"))


################# ****** END LOOP

#agg_stats, matches
rm(all_sponsors, bills, legis_data, SS_bills, commem_bills, elec_year, i, keep_types)
rm(k_matches, klarner_sub, m_sub, km, bill_path, t_yrs, calc_LES) # c_sub
rm(t, terms, klarner_gs, c_sub, t_sessions, parsed_names, match_name2)

########################################################################################################################################################
############################################################################################################################################################# NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
### ***** NOTE: Only recording special wins for legislators with NO RECORD of special win in Klarner
#################

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1996_1999 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- MORRISH (dan); ROMERO (errol); SCHWEGMANN (melinda); CARTER (robert); WADDELL (wayne)
### WON SPECIAL ~ SENATE:
# -- THEUNISSEN (gerald, via H)
### IN HOUSE
# -- SNEED (jennifer, elected March 1999, https://en.wikipedia.org/wiki/Jennifer_Sneed_Heebe)
### IN SENATE:
# -- THEUNISSEN (gerald, won Senate special a year into house term)
# -- TARVER (gregory)
### DROP ---> ONLY BECAUSE DON'T HAVE 1996 -- If had that session, would keep:
# -- ACKAL (resigned at some point mid-first year... was seated)
# -- GUZZARDO (buster, scandal broke April 1996, then resigned, https://www.google.com/search?q=buster+guzzardo+louisiana+resign&oq=buster+guzzardo+louisiana+resign&aqs=chrome..69i57j33l2.3320j0j7&sourceid=chrome&ie=UTF-8)
# -- PICARD (cecil, left in Sep 1996 to become superintendent)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2000_2003 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- DOWNS (hollis)
### KLARNER ERROR:
# -- MCMAINS (chuck) -- No match, but was in office thorugh 2001; Gary beard coded as wining in 1999 but take seat until 2001 special
# -- DUPREE (reggie) -- Won 2001 Senate runoff, which is in Klarner, but 1999 House win is missing
# -- WINDHORST (stephen) -- Resigned in october after winning judicial election.. but was in office until then.. https://en.wikipedia.org/wiki/Stephen_J._Windhorst
### IN SENATE:
# -- TARVER (gregory)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2004_2007 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- ANDERS (andy); CHANDLER (billy); KLECKLEY (chuck); CRAVINS JR (don)
# -- GREENE (hunter); MORRIS (jim); ROBIDEAUX (joel); LAFONTA (juan)
# -- LORUSSO (nick); WILLIAMS (patrick); ASHFORD BARROW (regina);
# -- HARRIS (terrell)
# -- MORRELL (jp/jeanpaul) --> Won't show, duplicated last name, succeeded aruthur morrell in 2006
# -- GUILLORY (elbert) --> won't show, duplicated last name, succeeded Cravins JR. who also won a special
### WON SPECIAL ~ SENATE:
# -- CASSIDY (bill)
# -- SHEPHERD (derrick, via H)
# -- MURRAY (edwin, via H)
# -- QUINN (julie)
# -- WESTON BROOME (sharon)
### DROP:
# -- leblanc, j. luke --> Resigned January 2004 upon appointment
### RECODED:
# -- 'r.h. "bill" strain' -- should be michael g. strain -- Bill Died 1999, http://house.louisiana.gov/pubinfo/Press_Releases/strain.htm

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2008_2011 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- SEABUAGH; HONORE; ERNST (appointed as temporary fill-in for Lorusso, who took leave for military service -- returned 2010) -- https://ballotpedia.org/Gregory_Ernst
# -- MORENO; BROSSETT; THIERRY; THIBAUT; HUVAL; LANDRY; CARMODY; BISHOP (wesley)
### WON SPECIAL ~ SENATE:
# -- APPEL, WILLARD-LEWIS (past h); CLAITOR; GUILLORY (elbert, via H)
# -- MILLS (via H); MORRELL (via H); PERRY (jonathan, via H)
# -- CARTER-PETERSON (via H); CHABERT
### DROP:
# -- powell, mike -- resigned after winning reelection (https://en.wikipedia.org/wiki/Mike_Powell_(Louisiana_politician))

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2012_2015 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- IVEY; MIGUEZ; OURSO; WOODRUFF; HALL; BOUIE; STOKES
# -- JOHNSON (mike, duplicated last name, won't print in list)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2016_2019 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
## -- TURNER, LARVADAIN III; JORDAN; STAGNI; STEFANSKI; MARINO; 
## -- BRASS; WRIGHT; DUBUISSON; MUSCARELLO; THOMAS; 
## -- CREWS; DUPLESSIS; BOURRIAQUE; MOSS; MCMAHEN
### KLARNER MISSING ~ HOUSE:
# -- DWIGHT (stephen) -- Won outright in primary, 100% of vote... Klarner has Brett Geymann as winner, but he was term-limited
### WON SPECIAL ~ SENATE:
# -- HENSGENS (via H)
# -- PRICE (via H)
### KLARNER MISSING ~ SENATE:
# -- LAMBERT (eddie) -- Term limited in H, ran for Senate, won wint 100%
### DROP:
# -- geymann, brett -- Term limited, Klarner has him as winning, but he couldn't run.
# -- edwards, ronnie -- Passed away 44 days into term; sworn in but never went back to capitol bc of treatment --https://www.theadvocate.com/baton_rouge/news/politics/legislature/article_44531126-35c7-535a-8ee8-8d6f5779230e.html#
# -- amedee, jody -- Same as geymann; couldn't run bc of term limits but coded as winner in Klarner


# filter(klarner, grepl('edwards, ronnie', cand) ) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(klarner, ddez == 59 & outcome == 'w') %>% select(cand, year, sen, etype, outcome, ddez, candid)


########################################################################################################
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
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_id <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_name <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$sponsor <- "owlett, clint"

########## ****Still missing***** 
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[1]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber)
# filter(klarner, grepl(gsub(',.+|-', '', name), cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, still_missing)

name_matches <- data.frame(LES_name = "carter, robert", k_name = 'carter, robby')
name_matches <- add_row(name_matches, LES_name = 'windhorst, stephen', k_name = 'windhorst, steve')
name_matches <- add_row(name_matches, LES_name = 'morris, jim', k_name = 'morris, james h. (jim)')
name_matches <- add_row(name_matches, LES_name = 'morrell, j.p.', k_name = 'morrell, jeanpaul j.')
name_matches <- add_row(name_matches, LES_name = 'lorusso, nick', k_name = 'lorusso, nicholas j. (nick)')
name_matches <- add_row(name_matches, LES_name = 'willard-lewis, cynthia', k_name = 'willardlewis, cynthia')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', k_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', k_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}

##### Matches requiring greater precision
LES[LES$data_name %in% "donald cravins jr." & is.na(LES$klarner_name),]$klarner_id <- 281738
LES[LES$data_name %in% "donald cravins jr." & is.na(LES$klarner_name),]$sponsor <- "cravins, donald (don) jr."
LES[LES$data_name %in% "donald cravins jr." & is.na(LES$klarner_name),]$klarner_name <- "cravins, donald (don) jr."

rm(name_matches, i)

############## Check for Duplicates
### Remaining = Switched from House to Senate
for(t in unique(LES$term)){
  print(glue(' ************ {t} ***************'))
  check_dup <- filter(LES, term == t & !is.na(klarner_name)) %>%
    group_by(chamber) %>%
    mutate(dup = duplicated(klarner_id)) 
  if(any(check_dup$dup)){
    filter(LES, term == t & klarner_id %in% check_dup[check_dup$dup == TRUE,]$klarner_id) %>%
      select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>%
      print()
  }
}
rm(check_dup, k_sub, name_sub, exact, missing)

###########################################################
###########################################################
####### Fix Name Variations
# --> E.g., Data Name = Same, but Klarner Name/ID Changes --> Difficult to track
###################
# LES %>% group_by(data_name) %>% filter(length(unique(sponsor))> 1 & !is.na(data_name)) %>% arrange(data_name) %>% View()

LES[LES$data_name %in% "ronnie johns",]$sponsor <- 'johns, ronnie'
LES[LES$data_name %in% 'b.l. "buddy" shaw',]$sponsor <- 'shaw, b. l. (buddy)'  ## HC Shaw originally in Klarner.. but def him
LES[LES$data_name %in% c("karen carter peterson", 'karen r. carter'),]$sponsor <- 'carter peterson, karen r.'
LES[LES$data_name %in% "patrick page cortez",]$sponsor <- 'cortez, patrick (page)'
LES[LES$data_name %in% "steve scalise",]$sponsor <- 'scalise, stephen j.'
LES[LES$sponsor %in% "durand, sydniemae maraist",]$sponsor <- 'durand, sydnie mae maraist'

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

### Manually Fix Those Not in Klarner
fill_missing <- data.frame(LES_name = "harris, terrell", party = 'd', district = 87, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "ernst, gregory", party = 'r', district = 94, exper = 'none')
# ** WOn't need below after Klarner Update
fill_missing <- add_row(fill_missing, LES_name = "dwight, stephen", party = 'r', district = 35, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "marino, joseph", party = 'i', district = 85, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "stagni, joe", party = 'r', district = 92, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "stefanski, john", party = 'r', district = 42, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "wright, mark", party = 'r', district = 77, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "crews, raymond", party = 'r', district = 8, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "brass, ken", party = 'd', district = 58, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "duplessis, royce", party = 'd', district = 93, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "dubuisson, mary", party = 'r', district = 90, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "mcmahen, wayne", party = 'r', district = 10, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "turner, christopher", party = 'r', district = 12, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "muscarello, nicholas", party = 'r', district = 86, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "moss, stuart", party = 'r', district = 33, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "larvadain, ed", party = 'd', district = 26, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "bourriaque, ryan", party = 'r', district = 47, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzt", party = 'zzzz', district = zzzz, exper = 'none')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
}

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t, fill_missing, i)

#########################################################
############ Match to Hall/Fouirnaies
########################################################

library(readstata13)

### Hall and Fournaies 2018, Covers 1988 - 2014
### --> https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/PGCVDP
hf_data <- read.dta13("~/Dropbox/Data/Hall_Fouirnaies_2018/How_Do_interest_Groups_Seek_Access_to_Committees.dta")
hf_data <- filter(hf_data, state == this_state)
hf_data <- hf_data[,c(colnames(hf_data)[c(1:3, 7:14, 204, 208, 217)], colnames(hf_data)[grepl('cmt_', colnames(hf_data))])]
hf_data$term <- paste0(hf_data$year + 1, "_", hf_data$year + 4)
hf_data$chamber <- ifelse(hf_data$chamber == 'senate', 'Senate', "House")

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
LES[LES$term == '2016_2019', set_NA] <- NA

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
#### -----> Added code to check for party switches
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
    ### Check Last + First Name
    if(nrow(ideo_match) > 1 & length(unique(ideo_match$name)) != 1 ){
      ideo_match <- filter(ideo_match, match_name == gsub(' [a-z]\\.$', '', LES[i,]$sponsor) )    
    }
    ### Check Party if Still Too Long
    if(nrow(ideo_match) == 2 & length(unique(ideo_match$name)) == 1 & any(ideo_match$party == 'R') &any(ideo_match$party == 'D')){
      if(length(unique(LES[LES$sponsor == LES[i,]$sponsor,]$party)) == 2){
        for(p in c('d', 'r')){
          LES[LES$sponsor == LES[i,]$sponsor & LES$party == p,]$SM_name <- ideo_match[ideo_match$party == toupper(p),]$name
          LES[LES$sponsor == LES[i,]$sponsor & LES$party == p,]$SM_party <- ideo_match[ideo_match$party == toupper(p),]$party
          LES[LES$sponsor == LES[i,]$sponsor & LES$party == p,]$np_score <- ideo_match[ideo_match$party == toupper(p),]$np_score
        }
        next
      }else{
        ideo_match <- filter(ideo_match, party == toupper(LES[i,]$party))  
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

### Fix Mismatches
# Doerge -- Everett and Jean Collapsed -- > half of years are Everett, so keeping him
LES[LES$sponsor == 'doerge, jean m.', c("SM_name", "SM_party", 'np_score')] <- NA

### MANUAL FIXES ____ *** LA SM DATA QUALITY POOR ****
# filter(LES, is.na(np_score)) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('chabert', tolower(name))) %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'bagneris, dennis', SM_name = 'Bagneris, Dennis R., Sr.')
# name_matches <- add_row(name_matches, LES_name = 'amedee, beryl adams', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'anders, andy', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'badon, bobby g.', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'bourriaque, ryan', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'brass, ken', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'brossett, jared', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'burns, henry', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'burrell, roy', SM_name = 'zzzzzz')
name_matches <- add_row(name_matches, LES_name = 'cortez, patrick (page)', SM_name = 'Cortez, Patrick Page')
name_matches <- add_row(name_matches, LES_name = 'cravins, donald (don)', SM_name = 'Cravins, Donald R. "Don"')
name_matches <- add_row(name_matches, LES_name = 'cravins, donald (don) jr.', SM_name = 'Cravins, Donald R. Jr.')
# name_matches <- add_row(name_matches, LES_name = 'crews, raymond', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'doerge, jean m.', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'dove, gordon', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'dubuisson, mary', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'duplessis, royce', SM_name = 'zzzzzz')
name_matches <- add_row(name_matches, LES_name = 'ellington, noble', SM_name = 'Ellington, Noble Edward') ## Two records
# name_matches <- add_row(name_matches, LES_name = 'foil, franklin j.', SM_name = 'zzzzzz')
name_matches <- add_row(name_matches, LES_name = 'gray, cheryl', SM_name = 'Gray Evans, Cheryl')
# name_matches <- add_row(name_matches, LES_name = 'greene, hunter', SM_name = 'zzzzzz')
name_matches <- add_row(name_matches, LES_name = 'guillory, elcie joseph', SM_name = 'Guillory, Elcie')
# name_matches <- add_row(name_matches, LES_name = 'guinn, john e. (johnny)', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'hardy, rickey', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'harrison, joe', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'hazel, chris', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'henderson, reed s.', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'hill, dorothy sue', SM_name = 'zzzzzz')
name_matches <- add_row(name_matches, LES_name = 'hollis, ken', SM_name = 'Hollis, J. Kendrick "Ken"')
name_matches <- add_row(name_matches, LES_name = 'honore, dalton', SM_name = 'Honoré, Dalton W.')
name_matches <- add_row(name_matches, LES_name = 'jenkins, louis (woody)', SM_name = 'Jenkins')
# name_matches <- add_row(name_matches, LES_name = 'johnson, mike', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'jones, sam', SM_name = 'zzzzzz')
name_matches <- add_row(name_matches, LES_name = 'jordan, max', SM_name = 'Jordan, J. Lomax "Max", Jr.')
# name_matches <- add_row(name_matches, LES_name = 'katz, kay kellogg', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'lafonta, juan', SM_name = 'zzzzzz')
name_matches <- add_row(name_matches, LES_name = 'lambert, louis j. jr.', SM_name = 'Lambert, Louis J., Jr.')
# name_matches <- add_row(name_matches, LES_name = 'landry, nancy', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'larvadain, ed', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'lebas, h. bernard', SM_name = 'zzzzzz')
name_matches <- add_row(name_matches, LES_name = 'long, jimmy 1', SM_name = 'Long')
# name_matches <- add_row(name_matches, LES_name = 'marino, joseph', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'mcmahen, wayne', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'morrell, arthur a.', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'morris, james h. (jim)', SM_name = 'zzzzzz')
name_matches <- add_row(name_matches, LES_name = 'morris, jay (jay)', SM_name = 'Morris, John III')
# name_matches <- add_row(name_matches, LES_name = 'moss, stuart', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'muscarello, nicholas', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'ponti, erich', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'pugh, steve', SM_name = 'zzzzzz')
name_matches <- add_row(name_matches, LES_name = 'smith, jack d.', SM_name = 'Smith, J.D.')
# name_matches <- add_row(name_matches, LES_name = 'smith, jane h.', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'stagni, joe', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'stefanski, john', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'talbot, kirk', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'thomas, polly', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'tucker, james w. (jim)', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'turner, christopher', SM_name = 'zzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'waddell, wayne', SM_name = 'zzzzzz')
name_matches <- add_row(name_matches, LES_name = 'white, mack (bodi) jr.', SM_name = 'White, Mack Jr.')
name_matches <- add_row(name_matches, LES_name = 'white, malinda brumfield', SM_name = 'White, Malinda B.')
name_matches <- add_row(name_matches, LES_name = 'williams, patrick c.', SM_name = 'Williams, Patrick C.')
# name_matches <- add_row(name_matches, LES_name = 'wright, mark', SM_name = 'zzzzzz')
name_matches <- add_row(name_matches, LES_name = 'wright, tommy', SM_name = 'Wright, Thomas')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

######################
#### Party Switchers
########################
# filter(ideo, grepl("guillory, elb", tolower(name)))
# filter(LES, grepl("guillory, elb", sponsor)) %>% select(1:6, party, SM_name, SM_party)

#### Norby Chabert -- Won as Dem in 2009 Special, switched parties in March 2011
LES[LES$sponsor == "chabert, norby (norby)" & LES$term == "2008_2011",]$party <- 'd'
LES[LES$sponsor == 'chabert, norby (norby)' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Chabert, Norbert' & ideo$party == 'R',]$name
LES[LES$sponsor == 'chabert, norby (norby)' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Chabert, Norbert' & ideo$party == 'R',]$party
LES[LES$sponsor == 'chabert, norby (norby)' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Chabert, Norbert' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'chabert, norby (norby)' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Chabert, Norbèrt' & ideo$party == 'D',]$name
LES[LES$sponsor == 'chabert, norby (norby)' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Chabert, Norbèrt' & ideo$party == 'D',]$party
LES[LES$sponsor == 'chabert, norby (norby)' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Chabert, Norbèrt' & ideo$party == 'D',]$np_score

#### Elbert Guillory -- D to R, May 2013 + Double Recorded for House and Senate? or
ideo <- filter(ideo, !(name == "Guillory, Elbert" & house2001 %in% 1 ))
LES[LES$sponsor == "guillory, elbert lee" & LES$term == "2012_2015",]$party <- 'r'
LES[LES$sponsor == 'guillory, elbert lee' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Guillory, Elbert' & ideo$party == 'R',]$name
LES[LES$sponsor == 'guillory, elbert lee' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Guillory, Elbert' & ideo$party == 'R',]$party
LES[LES$sponsor == 'guillory, elbert lee' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Guillory, Elbert' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'guillory, elbert lee' & LES$party == 'd' & LES$chamber == "House",]$SM_name <-  ideo[ideo$name == 'Guillory, Elbert' & ideo$party == 'D' & ideo$house2007 %in% 1,]$name
LES[LES$sponsor == 'guillory, elbert lee' & LES$party == 'd' & LES$chamber == "House",]$SM_party <- ideo[ideo$name == 'Guillory, Elbert' & ideo$party == 'D' & ideo$house2007 %in% 1,]$party
LES[LES$sponsor == 'guillory, elbert lee' & LES$party == 'd' & LES$chamber == "House",]$np_score <- ideo[ideo$name == 'Guillory, Elbert' & ideo$party == 'D' & ideo$house2007 %in% 1,]$np_score
LES[LES$sponsor == 'guillory, elbert lee' & LES$party == 'd' & LES$chamber == "Senate",]$SM_name <-  ideo[ideo$name == 'Guillory, Elbert' & ideo$party == 'D' & ideo$senate2010 %in% 1,]$name
LES[LES$sponsor == 'guillory, elbert lee' & LES$party == 'd' & LES$chamber == "Senate",]$SM_party <- ideo[ideo$name == 'Guillory, Elbert' & ideo$party == 'D' & ideo$senate2010 %in% 1,]$party
LES[LES$sponsor == 'guillory, elbert lee' & LES$party == 'd' & LES$chamber == "Senate",]$np_score <- ideo[ideo$name == 'Guillory, Elbert' & ideo$party == 'D' & ideo$senate2010 %in% 1,]$np_score

## Ligi doesn't appear to have switched despite SM data coding (also has him in chamber as D/R simultaneouslhy)
LES[LES$sponsor == 'ligi, tony' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Ligi, Anthony Jr.' & ideo$party == 'R',]$name
LES[LES$sponsor == 'ligi, tony' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Ligi, Anthony Jr.' & ideo$party == 'R',]$party
LES[LES$sponsor == 'ligi, tony' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Ligi, Anthony Jr.' & ideo$party == 'R',]$np_score

LES[LES$sponsor == 'michot, mike' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Michot, Michael' & ideo$party == 'R',]$name
LES[LES$sponsor == 'michot, mike' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Michot, Michael' & ideo$party == 'R',]$party
LES[LES$sponsor == 'michot, mike' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Michot, Michael' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'michot, mike' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Michot' & ideo$party == 'D',]$name
LES[LES$sponsor == 'michot, mike' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Michot' & ideo$party == 'D',]$party
LES[LES$sponsor == 'michot, mike' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Michot' & ideo$party == 'D',]$np_score

LES[LES$sponsor == 'flavin, dan' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Flavin, Daniel Thomas' & ideo$party == 'R',]$name
LES[LES$sponsor == 'flavin, dan' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Flavin, Daniel Thomas' & ideo$party == 'R',]$party
LES[LES$sponsor == 'flavin, dan' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Flavin, Daniel Thomas' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'flavin, dan' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Flavin' & ideo$party == 'D',]$name
LES[LES$sponsor == 'flavin, dan' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Flavin' & ideo$party == 'D',]$party
LES[LES$sponsor == 'flavin, dan' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Flavin' & ideo$party == 'D',]$np_score

LES[LES$sponsor == 'thomas, jerry' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Thomas, Jerry' & ideo$party == 'R',]$name
LES[LES$sponsor == 'thomas, jerry' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Thomas, Jerry' & ideo$party == 'R',]$party
LES[LES$sponsor == 'thomas, jerry' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Thomas, Jerry' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'thomas, jerry' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Thomas' & ideo$party == 'D',]$name
LES[LES$sponsor == 'thomas, jerry' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Thomas' & ideo$party == 'D',]$party
LES[LES$sponsor == 'thomas, jerry' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Thomas' & ideo$party == 'D',]$np_score

LES[LES$sponsor == 'theunissen, gerald (jerry)' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Theunissen, Gerald' & ideo$party == 'R',]$name
LES[LES$sponsor == 'theunissen, gerald (jerry)' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Theunissen, Gerald' & ideo$party == 'R',]$party
LES[LES$sponsor == 'theunissen, gerald (jerry)' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Theunissen, Gerald' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'theunissen, gerald (jerry)' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Theunissen' & ideo$party == 'D',]$name
LES[LES$sponsor == 'theunissen, gerald (jerry)' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Theunissen' & ideo$party == 'D',]$party
LES[LES$sponsor == 'theunissen, gerald (jerry)' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Theunissen' & ideo$party == 'D',]$np_score

LES[LES$sponsor == 'barham, robert j.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Barham, Robert' & ideo$party == 'R',]$name
LES[LES$sponsor == 'barham, robert j.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Barham, Robert' & ideo$party == 'R',]$party
LES[LES$sponsor == 'barham, robert j.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Barham, Robert' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'barham, robert j.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Barham, Robert Jocelyn' & ideo$party == 'D',]$name
LES[LES$sponsor == 'barham, robert j.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Barham, Robert Jocelyn' & ideo$party == 'D',]$party
LES[LES$sponsor == 'barham, robert j.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Barham, Robert Jocelyn' & ideo$party == 'D',]$np_score

LES[LES$sponsor == 'cain, james david' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Cain, James' & ideo$party == 'R',]$name
LES[LES$sponsor == 'cain, james david' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Cain, James' & ideo$party == 'R',]$party
LES[LES$sponsor == 'cain, james david' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Cain, James' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'cain, james david' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Cain, James David' & ideo$party == 'D',]$name
LES[LES$sponsor == 'cain, james david' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Cain, James David' & ideo$party == 'D',]$party
LES[LES$sponsor == 'cain, james david' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Cain, James David' & ideo$party == 'D',]$np_score

ideo <- filter(ideo, !(name == "Champagne, Simone" & house2004 %in% 1) )
LES[LES$sponsor == 'champagne, simone' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Champagne, Simone' & ideo$party == 'R',]$name
LES[LES$sponsor == 'champagne, simone' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Champagne, Simone' & ideo$party == 'R',]$party
LES[LES$sponsor == 'champagne, simone' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Champagne, Simone' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'champagne, simone' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Champagne, Simone' & ideo$party == 'D',]$name
LES[LES$sponsor == 'champagne, simone' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Champagne, Simone' & ideo$party == 'D',]$party
LES[LES$sponsor == 'champagne, simone' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Champagne, Simone' & ideo$party == 'D',]$np_score

ideo <- filter(ideo, !(name == "Chaney, Charles" & house2007 %in% 1) )
LES[LES$sponsor == 'chaney, charles r. (bubba)' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Chaney, Charles' & ideo$party == 'R',]$name
LES[LES$sponsor == 'chaney, charles r. (bubba)' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Chaney, Charles' & ideo$party == 'R',]$party
LES[LES$sponsor == 'chaney, charles r. (bubba)' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Chaney, Charles' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'chaney, charles r. (bubba)' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Chaney, Charles' & ideo$party == 'D',]$name
LES[LES$sponsor == 'chaney, charles r. (bubba)' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Chaney, Charles' & ideo$party == 'D',]$party
LES[LES$sponsor == 'chaney, charles r. (bubba)' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Chaney, Charles' & ideo$party == 'D',]$np_score

ideo <- filter(ideo, !(name == "Mills, Fred Jr." & house2011 %in% 1) )
LES[LES$sponsor == 'mills, fred h. jr.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Mills, Fred Jr.' & ideo$party == 'R',]$name
LES[LES$sponsor == 'mills, fred h. jr.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Mills, Fred Jr.' & ideo$party == 'R',]$party
LES[LES$sponsor == 'mills, fred h. jr.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Mills, Fred Jr.' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'mills, fred h. jr.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Mills, Fred Jr.' & ideo$party == 'D',]$name
LES[LES$sponsor == 'mills, fred h. jr.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Mills, Fred Jr.' & ideo$party == 'D',]$party
LES[LES$sponsor == 'mills, fred h. jr.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Mills, Fred Jr.' & ideo$party == 'D',]$np_score

ideo <- filter(ideo, !(name == "Fannin, James" & party == "R" & house2015 %in% 1) )
LES[LES$sponsor == 'fannin, james r. (jim)' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Fannin, James' & ideo$party == 'R',]$name
LES[LES$sponsor == 'fannin, james r. (jim)' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Fannin, James' & ideo$party == 'R',]$party
LES[LES$sponsor == 'fannin, james r. (jim)' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Fannin, James' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'fannin, james r. (jim)' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Fannin, James' & ideo$party == 'D',]$name
LES[LES$sponsor == 'fannin, james r. (jim)' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Fannin, James' & ideo$party == 'D',]$party
LES[LES$sponsor == 'fannin, james r. (jim)' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Fannin, James' & ideo$party == 'D',]$np_score

###############################
#### Manual Edits (Needs more precision...)
LES[LES$sponsor == 'armes, james',]$SM_name <- ideo[ideo$name == 'Armes, James III' & ideo$house2007 %in% 1,]$name
LES[LES$sponsor == 'armes, james',]$SM_party <- ideo[ideo$name == 'Armes, James III' & ideo$house2007 %in% 1,]$party
LES[LES$sponsor == 'armes, james',]$np_score <- ideo[ideo$name == 'Armes, James III' & ideo$house2007 %in% 1,]$np_score

LES[LES$sponsor == 'ashfordbarrow, regina',]$SM_name <- ideo[ideo$name == 'Barrow, Regina' & ideo$house2006 %in% 1,]$name
LES[LES$sponsor == 'ashfordbarrow, regina',]$SM_party <- ideo[ideo$name == 'Barrow, Regina' & ideo$house2006 %in% 1,]$party
LES[LES$sponsor == 'ashfordbarrow, regina',]$np_score <- ideo[ideo$name == 'Barrow, Regina' & ideo$house2006 %in% 1,]$np_score

LES[LES$sponsor == 'burns, tim',]$SM_name <- ideo[ideo$name == 'Burns, Timothy' & ideo$house2004 %in% 1,]$name
LES[LES$sponsor == 'burns, tim',]$SM_party <- ideo[ideo$name == 'Burns, Timothy' & ideo$house2004 %in% 1,]$party
LES[LES$sponsor == 'burns, tim',]$np_score <- ideo[ideo$name == 'Burns, Timothy' & ideo$house2004 %in% 1,]$np_score

LES[LES$sponsor == 'burns, tim',]$SM_name <- ideo[ideo$name == 'Burns, Timothy' & ideo$house2004 %in% 1,]$name
LES[LES$sponsor == 'burns, tim',]$SM_party <- ideo[ideo$name == 'Burns, Timothy' & ideo$house2004 %in% 1,]$party
LES[LES$sponsor == 'burns, tim',]$np_score <- ideo[ideo$name == 'Burns, Timothy' & ideo$house2004 %in% 1,]$np_score

## Rick Gallot -- Two records for same individual.. One house, one senate, so splitting based on chamber
LES[LES$sponsor == 'gallot, richard (rick) jr.' & LES$chamber == "House",]$SM_name <- ideo[ideo$name == 'Gallot, Richard' & ideo$senate2012 %in% 1,]$name
LES[LES$sponsor == 'gallot, richard (rick) jr.' & LES$chamber == "House",]$SM_party <- ideo[ideo$name == 'Gallot, Richard' & ideo$senate2012 %in% 1,]$party
LES[LES$sponsor == 'gallot, richard (rick) jr.' & LES$chamber == "House",]$np_score <- ideo[ideo$name == 'Gallot, Richard' & ideo$senate2012 %in% 1,]$np_score
LES[LES$sponsor == 'gallot, richard (rick) jr.' & LES$chamber == "Senate",]$SM_name <- ideo[ideo$name == 'Gallot, Richard' & ideo$house2002 %in% 1,]$name
LES[LES$sponsor == 'gallot, richard (rick) jr.' & LES$chamber == "Senate",]$SM_party <- ideo[ideo$name == 'Gallot, Richard' & ideo$house2002 %in% 1,]$party
LES[LES$sponsor == 'gallot, richard (rick) jr.' & LES$chamber == "Senate",]$np_score <- ideo[ideo$name == 'Gallot, Richard' & ideo$house2002 %in% 1,]$np_score

LES[LES$sponsor == 'hines, walker',]$SM_name <- ideo[ideo$name == 'Hines, Walker' & ideo$house2006 %in% 1,]$name
LES[LES$sponsor == 'hines, walker',]$SM_party <- ideo[ideo$name == 'Hines, Walker' & ideo$house2006 %in% 1,]$party
LES[LES$sponsor == 'hines, walker',]$np_score <- ideo[ideo$name == 'Hines, Walker' & ideo$house2006 %in% 1,]$np_score

LES[LES$sponsor == 'franklin, ab',]$SM_name <- ideo[ideo$name == 'Franklin, Albert' & ideo$house2005 %in% 1,]$name
LES[LES$sponsor == 'franklin, ab',]$SM_party <- ideo[ideo$name == 'Franklin, Albert' & ideo$house2005 %in% 1,]$party
LES[LES$sponsor == 'franklin, ab',]$np_score <- ideo[ideo$name == 'Franklin, Albert' & ideo$house2005 %in% 1,]$np_score

## multiple records for seemingly no reason
LES[LES$sponsor == 'jackson, michael 1',]$SM_name <- ideo[ideo$name == 'Jackson, Michael' & ideo$house2002 %in% 1,]$name
LES[LES$sponsor == 'jackson, michael 1',]$SM_party <- ideo[ideo$name == 'Jackson, Michael' & ideo$house2002 %in% 1,]$party
LES[LES$sponsor == 'jackson, michael 1',]$np_score <- ideo[ideo$name == 'Jackson, Michael' & ideo$house2002 %in% 1,]$np_score

LES[LES$sponsor == 'labruzzo, john',]$SM_name <- ideo[ideo$name == 'LaBruzzo, John Jr.' & ideo$house2004 %in% 1,]$name
LES[LES$sponsor == 'labruzzo, john',]$SM_party <- ideo[ideo$name == 'LaBruzzo, John Jr.' & ideo$house2004 %in% 1,]$party
LES[LES$sponsor == 'labruzzo, john',]$np_score <- ideo[ideo$name == 'LaBruzzo, John Jr.' & ideo$house2004 %in% 1,]$np_score

LES[LES$sponsor == 'leger, walt iii',]$SM_name <- ideo[ideo$name == 'Leger, Walter III' & ideo$house2016 %in% 1,]$name
LES[LES$sponsor == 'leger, walt iii',]$SM_party <- ideo[ideo$name == 'Leger, Walter III' & ideo$house2016 %in% 1,]$party
LES[LES$sponsor == 'leger, walt iii',]$np_score <- ideo[ideo$name == 'Leger, Walter III' & ideo$house2016 %in% 1,]$np_score

LES[LES$sponsor == 'monica, nickie',]$SM_name <- ideo[ideo$name == 'Monica, Nickie' & ideo$house2013 %in% 1,]$name
LES[LES$sponsor == 'monica, nickie',]$SM_party <- ideo[ideo$name == 'Monica, Nickie' & ideo$house2013 %in% 1,]$party
LES[LES$sponsor == 'monica, nickie',]$np_score <- ideo[ideo$name == 'Monica, Nickie' & ideo$house2013 %in% 1,]$np_score

## Morrell -- House/Senate
LES[LES$sponsor == 'morrell, jeanpaul j.' & LES$chamber == "House",]$SM_name <- ideo[ideo$name == 'Morrell, Jean-Paul' & ideo$house2006 %in% 1,]$name
LES[LES$sponsor == 'morrell, jeanpaul j.' & LES$chamber == "House",]$SM_party <- ideo[ideo$name == 'Morrell, Jean-Paul' & ideo$house2006 %in% 1,]$party
LES[LES$sponsor == 'morrell, jeanpaul j.' & LES$chamber == "House",]$np_score <- ideo[ideo$name == 'Morrell, Jean-Paul' & ideo$house2006 %in% 1,]$np_score
LES[LES$sponsor == 'morrell, jeanpaul j.' & LES$chamber == "Senate",]$SM_name <- ideo[ideo$name == 'Morrell, Jean-Paul' & ideo$senate2009 %in% 1,]$name
LES[LES$sponsor == 'morrell, jeanpaul j.' & LES$chamber == "Senate",]$SM_party <- ideo[ideo$name == 'Morrell, Jean-Paul' & ideo$senate2009 %in% 1,]$party
LES[LES$sponsor == 'morrell, jeanpaul j.' & LES$chamber == "Senate",]$np_score <- ideo[ideo$name == 'Morrell, Jean-Paul' & ideo$senate2009 %in% 1,]$np_score

LES[LES$sponsor == 'richardson, clif',]$SM_name <- ideo[ideo$name == 'Richardson, Clifton' & ideo$house2006 %in% 1,]$name
LES[LES$sponsor == 'richardson, clif',]$SM_party <- ideo[ideo$name == 'Richardson, Clifton' & ideo$house2006 %in% 1,]$party
LES[LES$sponsor == 'richardson, clif',]$np_score <- ideo[ideo$name == 'Richardson, Clifton' & ideo$house2006 %in% 1,]$np_score

LES[LES$sponsor == 'richard, jerome (dee)',]$SM_name <- ideo[ideo$name == 'Richard, Jerome' & ideo$party == 'D',]$name
LES[LES$sponsor == 'richard, jerome (dee)',]$SM_party <- ideo[ideo$name == 'Richard, Jerome' & ideo$party == 'D',]$party
LES[LES$sponsor == 'richard, jerome (dee)',]$np_score <- ideo[ideo$name == 'Richard, Jerome' & ideo$party == 'D',]$np_score

LES[LES$sponsor == 'roy, chris jr.',]$SM_name <- ideo[ideo$name == 'Roy, Chris Jr.' & ideo$house2000 %in% 1,]$name
LES[LES$sponsor == 'roy, chris jr.',]$SM_party <- ideo[ideo$name == 'Roy, Chris Jr.' & ideo$house2000 %in% 1,]$party
LES[LES$sponsor == 'roy, chris jr.',]$np_score <- ideo[ideo$name == 'Roy, Chris Jr.' & ideo$house2000 %in% 1,]$np_score

LES[LES$sponsor == 'saintgermain, karen gaudet',]$SM_name <- ideo[ideo$name == 'St Germain, Karen' & ideo$house2004 %in% 1,]$name
LES[LES$sponsor == 'saintgermain, karen gaudet',]$SM_party <- ideo[ideo$name == 'St Germain, Karen' & ideo$house2004 %in% 1,]$party
LES[LES$sponsor == 'saintgermain, karen gaudet',]$np_score <- ideo[ideo$name == 'St Germain, Karen' & ideo$house2004 %in% 1,]$np_score

LES[LES$sponsor == 'schroder, john m.',]$SM_name <- ideo[ideo$name == 'Schroder, John Sr.' & ideo$house2008 %in% 1,]$name
LES[LES$sponsor == 'schroder, john m.',]$SM_party <- ideo[ideo$name == 'Schroder, John Sr.' & ideo$house2008 %in% 1,]$party
LES[LES$sponsor == 'schroder, john m.',]$np_score <- ideo[ideo$name == 'Schroder, John Sr.' & ideo$house2008 %in% 1,]$np_score

LES[LES$sponsor == 'strain, r. h. (bill)',]$SM_name <- ideo[ideo$name == 'Strain' & ideo$party == "D",]$name
LES[LES$sponsor == 'strain, r. h. (bill)',]$SM_party <- ideo[ideo$name == 'Strain' & ideo$party == "D",]$party
LES[LES$sponsor == 'strain, r. h. (bill)',]$np_score <- ideo[ideo$name == 'Strain' & ideo$party == "D",]$np_score

LES[LES$sponsor == 'strain, michael g. (mike)',]$SM_name <- ideo[ideo$name == 'Strain' & ideo$party == "R",]$name
LES[LES$sponsor == 'strain, michael g. (mike)',]$SM_party <- ideo[ideo$name == 'Strain' & ideo$party == "R",]$party
LES[LES$sponsor == 'strain, michael g. (mike)',]$np_score <- ideo[ideo$name == 'Strain' & ideo$party == "R",]$np_score

LES[LES$sponsor == 'templet, ricky j.',]$SM_name <- ideo[ideo$name == 'Templet, Ricky' & ideo$house2013 %in% 1,]$name
LES[LES$sponsor == 'templet, ricky j.',]$SM_party <- ideo[ideo$name == 'Templet, Ricky' & ideo$house2013 %in% 1,]$party
LES[LES$sponsor == 'templet, ricky j.',]$np_score <- ideo[ideo$name == 'Templet, Ricky' & ideo$house2013 %in% 1,]$np_score

LES[LES$sponsor == 'woodruff, ebony',]$SM_name <-  ideo[ideo$name == 'Woodruff, Ebony' & ideo$house2014 %in% 1,]$name
LES[LES$sponsor == 'woodruff, ebony',]$SM_party <- ideo[ideo$name == 'Woodruff, Ebony' & ideo$house2014 %in% 1,]$party
LES[LES$sponsor == 'woodruff, ebony',]$np_score <- ideo[ideo$name == 'Woodruff, Ebony' & ideo$house2014 %in% 1,]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname)

#################
### Clean Sponsor Names + Other Fixes
##################

#### Drop Numbers at End --> OK bc no identical names that need to be corrected
# filter(LES, grepl(" [0-9]$", sponsor)) %>% distinct(sponsor)
LES$sponsor <- gsub(' [0-9]$', '', LES$sponsor)

##### Drop Nicknames
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Jennifer Sneed is/was a Republican
LES[LES$sponsor == "sneed, jennifer l.",]$party <- 'r'

### Tom Greene switched parties, D to R, January 1996
LES[LES$sponsor == "greene, thomas a." & LES$term == "1996_1999",]$party <- 'r'

### 2017-2018
LES[LES$sponsor == "larvadain, ed",]$sponsor <- "larvadain, ed iii"


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1996 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1996:2010) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2011:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1996 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1996:2010) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2011:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


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
            max_LES = max(LES)) %>%
  arrange(party, term) %>%
  as.data.frame()

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
  scale_color_manual(values=c("dodgerblue2", "gray50", 'gray50', "red2"))

##### CHECK OUTLIERS 
## --- Walker Hines switched in Final Year, not clear why he's coded as R x 2 in SM (plus there for years he wasn't in office?)
## --- Wooton switched in to D to R in 2007, then Indep in 2010, but SM doesn't have Dem years
## --- Pope -- SM Error, has always been R
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')
# stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

