

##################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** INDIANA *** BY SESSION
##################################################################

# ********* UPDATE NOTEL
# --- If updating and future sessions have special sessions, need to check S&S merge
# --- Currently assuming all == RS given no special mentions, but may not hold in future
# **********


###################################
## SPECIAL SESSIONS:
## ---- BILLS CARROVER ---> NEW SESSION, MUST BE REINTRODUCED, BUT NUMBER STAYS THE SAME
## -------> See, e.g., HB 1230 in 2018 and 2018 Special (http://iga.in.gov/legislative/2018/bills/house/1230 ~~~~ http://iga.in.gov/legislative/2018ss1/bills/house/1230)
## -------> ALSO a small number of bills that get reintroduced from Term to Term.. See, e.g., HB1014 in 2017 and 2018 RS
## MEMBER LISTS:
## ---- http://iga.in.gov/legislative/2019/bylegislator
## ---- By Legislation -- http://www.in.gov/legislative/2414.htm
## PROCESS:
## ---- House Rules: http://www.in.gov/legislative/session/houserules.pdf
## Sponsorship/Authorship
## -- Author = In-Chamber; COauthor = Joins with Author
## -- Sponsor = Out-Chamber; Cosponsor = Joins with Sponsor from Out-Chamber
## -- In rare cases where author not listed, using coauthors, though occassionally out of order (or alphabetized in groups (primary, co))
## -- Num_cosponsored bills count will be off for duplicate last names -- they don't always record the identifying initial
###########################
##### NOTES:
# - (A) For 2013-2014: 
# ----> (1) Many spnonsor names are missing from webpage in 2014; need to pull them from the actions
# ----> (2) Ignoring 1, format of names is different for 2013 and 2014 --> Need to adjust to be identical
# - (B) Adjusting bills that carry over from regular to specials --> Basically an extension of what happened in the regular
# ------> Not adjusting for carryover between sessions as appears to be more just reintroduction
###########################

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

this_state <- 'IN'
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
terms <- 2021
t = terms
t_plus_one = t + 1
t_yrs <- as.numeric(terms)
t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 1}'))
t_sessions = sessions[grepl(as.character(t),sessions) | grepl(as.character(t+1),sessions)]

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv"))
commem_bills$bill_id = gsub(" ", "", commem_bills$bill_id)
commem_bills$bill_id = paste0(gsub("[0-9].+", '', commem_bills$bill_id), str_pad(gsub("^[A-Z]+", "", commem_bills$bill_id), 4, pad = "0"))

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


###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[6]



### DROP 2009 Special -- NO HISTORIES POSTED: http://www.in.gov/apps/lsa/session/billwatch/billinfo?year=1092&session=1&request=all
if(t_yrs == "2009_2010"){
  t_sessions <- t_sessions[!grepl('2009_Special', t_sessions)]
}

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
bills$session <- ifelse(grepl(' Special', bills$session), gsub(' Special', '-SS', bills$session), paste0(bills$session, "-RS"))

### Check for duplicates
if(nrow(bills) != nrow(distinct(bills))){
  cat(" \n ~~~> DUPLICATE BILLS \n\n .")
  break
}

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_number)
bills <- arrange(bills, session, bill_id)
bills$bill_id = gsub(" ", "", bills$bill_id)
bills$bill_id = paste0(gsub("[0-9].+", '', bills$bill_id), str_pad(gsub("^[A-Z]+", "", bills$bill_id), 4, pad = "0"))

############### Drop Resolutions, Messages, Communications, Reports
all_bills <- bills
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)

#### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
bills$authors <- tolower(bills$authors)
bills$authors <- gsub('á|ã¡', 'a', bills$authors)
bills$authors <- gsub('é|ã©', 'e', bills$authors)
bills$authors <- gsub('ó', 'o', bills$authors)
bills$authors <- gsub('í', 'i', bills$authors)
bills$authors <- gsub('ñ|ã±', 'n', bills$authors)
bills$authors <- gsub('  +', ' ', bills$authors)

### Authors/Coauthors = Bill Originating chamber; Sponsors/Cosponsors = Outchamber?
bills$coauthors <- tolower(bills$coauthors)
bills$coauthors <- gsub('á|ã¡', 'a', bills$coauthors)
bills$coauthors <- gsub('é|ã©', 'e', bills$coauthors)
bills$coauthors <- gsub('ó', 'o', bills$coauthors)
bills$coauthors <- gsub('í', 'i', bills$coauthors)
bills$coauthors <- gsub('ñ|ã±', 'n', bills$coauthors)  
bills$coauthors <- gsub('  +', ' ', bills$coauthors)

### Sponsors/Cosponsors = Outchamber -- 2015+ ONLY
bills$cosponsors <- tolower(bills$cosponsors)
bills$cosponsors <- gsub('á|ã¡', 'a', bills$cosponsors)
bills$cosponsors <- gsub('é|ã©', 'e', bills$cosponsors)
bills$cosponsors <- gsub('ó', 'o', bills$cosponsors)
bills$cosponsors <- gsub('í', 'i', bills$cosponsors)
bills$cosponsors <- gsub('ñ|ã±', 'n', bills$cosponsors)  
bills$cosponsors <- gsub('  +', ' ', bills$cosponsors)

#### For 2014: Add names from Actions and Convert to same format as 2013 (Last, F instead of First Last)
# ------> Need to ensure that idnividuals records aren't split 
if(t == 2013){
  source("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/Estimation_Scripts/Clean_Indiana_2014_fx.R")
  bills <- clean_IN_2014(bills)
  
  ### Fix Misspelled Name
  bills$authors <- gsub("neimeyer", "niemeyer", bills$authors)
  bills$coauthors <- gsub("neimeyer", "niemeyer", bills$coauthors)
  #bills$cosponsors <- gsub("neimeyer", "niemeyer", bills$cosponsors)
}

#### For 2015-2016: Fix Eric (Allan) Koch
if(t == 2015){
  bills$authors <- gsub("eric allan koch", "eric koch", bills$authors)
  bills$coauthors <- gsub("eric allan koch", "eric koch", bills$coauthors)
  bills$cosponsors <- gsub("eric allan koch", "eric koch", bills$cosponsors)
}

#### Adjusting for Bills By Request -- Need to do BEFORE splitting
if(any(grepl('\\(br\\)|by request| br$', bills$authors))){
  print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
  #print(glue("-----> KEEPING {nrow(filter(bills, grepl('(br)|by request', authors)))} bill(s) introduced BY REQUEST"))
}

#### LES Sponsor Variable
bills$LES_sponsor <- tolower(gsub(';.+| and .+', '', bills$authors))
bills$LES_sponsor <- str_trim(gsub("senator\\(s\\)|senators|senator|sen\\.", '', bills$LES_sponsor))
bills$LES_sponsor <- str_trim(gsub("representative\\(s\\)|representatives|representative|rep\\.", '', bills$LES_sponsor))
if(t >= 2005){
  ## Start seperating by commas
  bills$LES_sponsor <- gsub(',.+', '', bills$LES_sponsor)
}
table(bills$LES_sponsor)

###### CHeck Missing Sponsors
# *** NOTE: Coauthors Variable includes main sponsor, typically ordered right but not always (e.g., sometimes alphabetical and doesn't match author column)
# filter(bills, LES_sponsor == "") %>% View()
if(nrow(filter(bills, LES_sponsor == '')) > 0){
  print(glue("-----> Recoding {nrow(filter(bills, LES_sponsor == '' & coauthors != ''))} bill(s) missing author using coauthors"))
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == '', gsub(';.+|,.+', '', bills$coauthors), bills$LES_sponsor)
}

#### Manual Name Fixes
if(t_yrs == "1999_2000" ){
  bills[bills$LES_sponsor == 'hume, l.',]$LES_sponsor <- "l. hume"
  bills$authors <- gsub('hume, l.', 'l. hume', bills$authors)
}
if(t_yrs == "2001_2002" ){
  bills[bills$LES_sponsor == 'hume',]$LES_sponsor <- "l. hume"
  bills$authors <- gsub('hume', 'l. hume', bills$authors)
}
### *** ONLY NEEDED IF SPECIAL SESSION INCLUDED (WHERE WE DON"T HAVE ACTIONS)
# if(t_yrs == "2009_2010"){ # http://www.in.gov/legislative/bills/1092/IN/IN1011.1.html
#   bills[bills$LES_sponsor == 'brown',]$LES_sponsor <- "c. brown"
#   bills$authors <- gsub('representative brown', 'representative c. brown', bills$authors)
# }

#############################################
###### Merge in S&S Bills
#############################################
# *** For INDIANA: Specials in 2002, 2009, 2018, but 2002 = all resolutions and 2009 = missing actions
# ---> For 5 SS bills in 2018, assuming RS (none mention Special)

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, Title, year) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS, year,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,authors), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",authors, ignore.case=T)) %>%
  arrange(authors) 
missing_SS_bills$bill_id



# now check to see if there are duplicate joins


duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills %>% mutate(year = as.integer(substr(session,1,4))), 
            by = c("bill_id", "term", "year")) %>%
  group_by(rowname) %>% mutate(count = n()) %>%
  ungroup() %>% 
  select(count,bill_id,term,session, Title, summary) %>% 
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

#################################################################
############### Code Commemorative
#################################################################

bills <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session'))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)

if(nrow(filter(bills, LES_sponsor == '')) > 0){
  print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without an author"))
  bills <- filter(bills, !(LES_sponsor == ''))     
}

#### DROP Committee Bills
if(any(grepl('committee|^rules$', bills$LES_sponsor))){
  print(glue("-----> DROPPING {nrow(filter(bills, grepl('committee|^rules$', LES_sponsor)))} bill(s) introduced BY COMMITTEE"))
  bills <- filter(bills, !grepl('committee|^rules$', LES_sponsor))
}


##############################################################################
############### Code Bill History
##############################################################################

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
bill_hist$session <- ifelse(grepl(' Special', bill_hist$session), gsub(' Special', '-SS', bill_hist$session), paste0(bill_hist$session, "-RS"))


######## Standardize BillHist Bill IDs
bill_hist <- rename(bill_hist, bill_id = bill_number)
bill_hist <- arrange(bill_hist, session, bill_id, order) %>%
  mutate(action = str_trim(action))
bill_hist$bill_id = gsub(" ", "", bill_hist$bill_id)
bill_hist$bill_id = paste0(gsub("[0-9].+", '', bill_hist$bill_id), str_pad(gsub("^[A-Z]+", "", bill_hist$bill_id), 4, pad = "0"))


### Standardize Chamber Variable
bill_hist$chamber <- recode(toupper(bill_hist$chamber), "H" = "House", "S" = "Senate")

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

### Recoding Failed Committee Action
if( any(grepl('Committee report: rejected', bill_hist$action)) ){
  bill_hist[bill_hist$action == "Committee report: rejected",]$action <- 'aic~committee report: rejected' 
}

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c('^committee report', "^aic~committee report:")
abc_t <-c("^committee report", "second reading", "third reading", "rules suspended", 'engrossed')
# ---> All bills reported out of committee unless Report = REJECTED
# ---> Engrossment occurs after second reading
# ---> Could also use 'house rule' and 'senate rule' but yields a bit of error (e.g., HB1097 2002~RS)
pc_t <- c('third reading: passed', 'referred to the senate', 'referred to the house', 'conference comm')
# ---> Conference committee as check..
law_t <- c('^public law', 'signed by the gov')
# filter(bill_hist, grepl('^committee report', tolower(action))) %>% distinct(action) 
# filter(bill_hist, grepl('^committee report', tolower(action))) %>% filter(substring(bill_id,1,1) == 'S')group_by(action) %>% summarize(n = n()) %>% arrange(desc(n))
# filter(bill_hist, chamber == "Executive") %>% distinct(action)
# filter(bill_hist, bill_id == 'SB 1' & session == '71-SS5') %>% select(bill_id, session, chamber, action_date, action, order)

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
# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% View()
# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% select(action) %>% distinct() %>% filter(!grepl('referred|introduced|prefile', tolower(action))) %>% View()


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

### MERGE IN BILL HIST CODINGS
bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 

### ADJUST OUTCOMES FOR SPECIAL REINTRODUCTIONS
### -- KEEPING MOST ADVANCED OUTCOME OF BILLS RE-INTRODUCED IN SPECIAL
### -- So if became law in special, count that; if had action in comm in regular, keep that
### -- Same for S&S
if(any(grepl('-SS', bills$session))){
  cat("------> Checking Special Session Bill Reintroductions")
  cat("\n")
  for(b_id in unique(bills$bill_id)){
    bill_sub <- filter(bills, bill_id == b_id)
    ## Only Adjust if Special Bill with Matching Number
    if( nrow(bill_sub) > 2 | ( nrow(bill_sub) == 2 & any(grepl('-SS', bill_sub$session))) ){
      keep_title <- bill_sub[duplicated(bill_sub$title),]$title
      ### Skip if Not Duplicated
      if(length(keep_title) == 0){ next }
      bill_sub <- filter(bill_sub, title == keep_title) %>% arrange(session)
      ## Break if bill introduced 3 times; Skip if not duplicates
      if(nrow(bill_sub) > 2){  print(' **** CHECK BILL SUB **** '); break } 
      ## Update History
      bills[bills$bill_id == b_id & bills$title == keep_title,]$action_in_comm <- max(bill_sub$action_in_comm)
      bills[bills$bill_id == b_id & bills$title == keep_title,]$action_beyond_comm <- max(bill_sub$action_beyond_comm)
      bills[bills$bill_id == b_id & bills$title == keep_title,]$passed_chamber <- max(bill_sub$passed_chamber)
      bills[bills$bill_id == b_id & bills$title == keep_title,]$law <- max(bill_sub$law)
      bills[bills$bill_id == b_id & bills$title == keep_title,]$SS <- max(bill_sub$SS)
      ## Keep Most Recently Introduced Version (= Drop first in bill_sub)
      bills <- filter(bills, !(bill_id == b_id & session == bill_sub$session[1]) )
      cat(glue('~~ Adjusted outcomes for {b_id} -- Reintroduced in Special Session'))
      cat('\n')
    }
  }
  rm(keep_title, bill_sub)
}

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

### Save Stage Info 
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

######## Add in Individuals WHo COAUTHORED but DID NOT AUTHOR for 2015+ ---- CAN'T really do this for pre-2015
if(t >= 2015){
  unique_cospon <- str_trim(unique(unlist(str_split(paste(bills$coauthors, bills$cosponsors, sep = "; "), '; '))))
  for(nonspon in unique_cospon){
    name_adj <- gsub("^sen. |^rep. ", '', nonspon)
    if(!(name_adj %in% all_sponsors$LES_sponsor) & name_adj != ''){
      chamb <- ifelse(grepl("^sen\\. ", nonspon), 'S', 'H')
      if("H" %in% chamb & "S" %in% chamb){
        print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
      }else{
        all_sponsors <- add_row(all_sponsors, LES_sponsor = name_adj, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0)
        #print(nonspon)
      }
    }
  }
}

######## Cosponsorship Info
all_sponsors$num_cosponsored_bills <- NA
bills$cospon_match <- paste(bills$authors, bills$coauthors, sep = ';')
if(t != 2013){
  ### **** Would need to use actions to get coauthor data for 2014 as many rows are missing
  for(i in 1:nrow(all_sponsors)){
    c_sub <- filter(bills, substring(bill_id, 1, 1) == all_sponsors[i,]$chamber)
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
    ### ONLY need to adjust this way if sponsored and cosponsored column are the same
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  }
}
bills <- select(bills, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))

if(min(all_sponsors$num_cosponsored_bills) < 0){
  print("negative cosponsors"); break
} else{print("cosponsors valid")}

#######################
#### CLEAN NAMES
#######################
if(t < 2015){
  all_sponsors$last_name <- ifelse(grepl('^[a-z]\\.|, [a-z]\\.', all_sponsors$LES_sponsor), gsub('^[a-z]\\. |, [a-z]\\.', '', all_sponsors$LES_sponsor), all_sponsors$LES_sponsor)
  all_sponsors$first_name <- ifelse(grepl('^[a-z]\\.', all_sponsors$LES_sponsor), gsub('\\. .+', '', all_sponsors$LES_sponsor),
                                    ifelse(grepl(', [a-z]\\.', all_sponsors$LES_sponsor), gsub('.+, |\\.', '', all_sponsors$LES_sponsor), ''))
}else if(t >= 2015){
  p_names <- parse_names(all_sponsors$LES_sponsor)
  all_sponsors$last_name <- p_names$last_name
  all_sponsors$first_name <- p_names$first_name
  rm(p_names)
}

all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update Last Names for Matching 
if(t_yrs %in% c('2005_2006', "2007_2008", "2009_2010") ){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "walorski", "walorskiswihart", all_sponsors$last_name)
}
if(t_yrs == "2005_2006"){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "antich-carr", "antich", all_sponsors$last_name)
}
if(t >= 2007 & t <= 2018){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "candelaria reardon", "reardon", all_sponsors$last_name)
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




legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>%  
  mutate(match_name = name) %>% 
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
        "elizabeth rowray-h" = NA_character_,
        "julie olthoff-h" = NA_character_,
        "michelle davis-h" = NA_character_,
        "fady qaddoura-s" = NA_character_,
        "scott baldwin-s" = NA_character_,
        "shelli yoder-s" = NA_character_,
        "mara reardon-h" = "mara candelaria reardon-h",
        "harold slager-h" = NA_character_,
        "jake teshka-h" = NA_character_,
        "mitch gore-h" = NA_character_,
        "renee pack-h" = NA_character_,
        "cindy ledbetter-h" = NA_character_,
        "craig snow-h" = NA_character_,
        "zach payne-h" = NA_character_,
        "maureen bauer-h" = NA_character_,
        "mike andrade-h" = NA_character_,
        "blake johnson-h" = NA_character_,
        "joanna king-h" = NA_character_,
        "john jacob-h" = NA_character_,
        "chris jeter-h" = NA_character_,
        "kyle walker-s" = NA_character_
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
        "elizabeth brown-s" = "liz brown-s"
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


###############################################
########### Estimate Scores + Add in Relatd Variables
##################################################
### Check if bills in data without an ID'd sponsor
View(filter(bills, !(bills$LES_sponsor %in% legis_data$data_name)))
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
rm(t, terms, klarner_gs, c_sub, t_sessions, nonspon, name_adj, chamb, unique_cospon)
rm(clean_IN_2014)

########################################################################################################################################################
############################################################################################################################################################# NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# -----> Recoding 25 bill(s) missing author using coauthors
# -----> Dropping 26 bill(s) without an author
# -----> DROPPING 25 bill(s) introduced BY COMMITTEE
### APPOINTED ~ HOUSE:
# -- DUMEZICH (daniel)
# -- WEINZAPFEL (jonathan)
### APPOINTED ~ SENATE:
# -- LUBBERS (teresa)
# -- LUTZ (larry) -- appointed after Sen. O'day passed away -- http://www.ingrouponline.com/legislativesourcebook/lutzlarrye.pdf
# -- SMITH (samuel)
### IN HOUSE:
# -- FESKO (timothy) -- resigned nov 1999 -- replcaed by dumezich https://www.nwitimes.com/uncategorized/dumezich-has-position-rival-says/article_d804e8d8-20bf-595a-ae86-dd5aaa33b8ae.html
# -- MANNWEILER (paul)
### DROP:
# -- lutz, larry e. --- appointed to S
# -- oday, joseph f. -- passed away prior to term start


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# -----> Recoding 1 bill(s) missing author using coauthors
# -----> Dropping 4 bill(s) without an author
# -----> DROPPING 49 bill(s) introduced BY COMMITTEE
## APPOINTED ~ HOUSE:
# -- NOE (cindy) -- https://en.wikipedia.org/wiki/Cindy_Noe
# -- RESKE (scott)
## APPOINTED ~ SENATE:
# -- LUTZ (larry) -- at start of 4 year term (see last term)
### IN HOUSE:
# -- MANNWEILER (paul) -- no record of resignation... but final term -- mannweiler, paul s.
### NAME DUPLICATES:
# -- hume and l. hume --> can't find any indication these are different people...

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 23 bill(s) without an author
# -----> DROPPING 25 bill(s) introduced BY COMMITTEE
## APPOINTED ~ HOUSE:
# -- GUTWEIN (eric)
# -- MESSER (luke)
# -- VAN HAAFTEN (william)
## APPOINTED ~ SENATE:
# -- DEMBOWSKI (nancy)
## DROP:
# smith, michael d. -- Gutwein (successor) appointed Nov 2002

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 45 bill(s) without an author
## APPOINTED ~ HOUSE
# -- BELL (matthew)
# -- CROUCH (suzanne)
# -- TYLER (dennis)
# -- C. BOTTORFF (carlene) -- Duplicate Name, won't show
## APPOINTED ~ SENATE:
# -- BECKER (vaneta, via H)
# -- KRUSE (dennis, via H)
# -- TALLIAN (karen, via H)
### NAME FIX:
# -- Rose Antich --> Antich-Carr
#### DUPLICATES FIXED:
# --- BOTTORFF --- James passed away, must have been replaced by wife Carlene -- https://www.newsandtribune.com/news/state-rep-jim-bottorff-dies/article_36d588ac-a461-5d09-b5a0-be24bde86208.html
# ---> https://www.newsandtribune.com/news/local_news/carlene-bottorff-to-finish-husband-s-term/article_aa2f9ed9-d0f0-5d26-beef-73476f737dcf.html

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 45 bill(s) without an author
### APPOINTED ~ HOUSE:
# -- BARTLETT (john) 
# -- BlANTON (sandra)
# -- SIMMS (greg, didn't run in 2008) - https://en.wikipedia.org/wiki/Greg_Simms
# -- STEUERWALD (gregory)
# -- VANDENBURGH (rochelle)
## APPOINTED ~ SENATE:
# -- ARNOLD (jim)
# -- BECKER (vaneta, via H, past term)
# -- CHARBONNEAU (ED)
### IN HOUSE:
# -- BAUER (b. patrick)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 36 bill(s) without an author
### APPOINTED ~ SENATE
# -- BUCK (james 'jim')
# -- HOLDMAN (travis)
# -- SCHNEIDER (scott)
### IN HOUSE:
# -- MCCLAIN (richard)
# -- BEHNING (robert)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 54 bill(s) without an author
### APPOINTED ~ SENATE
# -- GLICK (susan)
# -- SCHNEIDER (scott, previous term)
### IN HOUSE:
# -- BAUER (b. patrick)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 24 bill(s) without an author
#   session chamber   N AIC ABC PASS LAW
# 1 2013-RS       H 589 199 199  175 146
# 2 2013-RS       S 619 226 226  213 146
# 3 2014-RS       H 419 151 153  146 117
# 4 2014-RS       S 389 180 182  163 107
#### APPOINTED ~ HOUSE:
# -- COX (casey)
# -- SULLIVAN (holli)



# ~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
#   session chamber   N AIC ABC PASS LAW
# 1 2015-RS       H 641 203 203  174 135
# 2 2015-RS       S 527 250 250  218 122
# 3 2016-RS       H 401 126 126  116 102
# 4 2016-RS       S 365 159 159  151 113
#### APPOINTED ~ HOUSE: 
# -- COOK (anthonyh)
# -- SCHAIBLEY (donna)
# -- ELLINGTON (jeff)
# -- LYNESS (randy)
#### APPOINTED ~ NAMES DUPLICATED --> WON'T PRINT:
# -- HARRIS (donna) -- appointed following husbands death, didn't run again
# -- BANKS (amanda) -- appointed to fill hsubands seat during leave of absence
#### DROP:
# -- braun, steven -- became workforce dev commissioner in nov 2014
# -- turner, p. eric -- resigned in nov 2014 amid scandal


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
#   session chamber   N AIC ABC PASS LAW
# 1 2017-RS       H 651 179 179  164 138
# 2 2017-RS       S 527 215 215  198 132
# 3 2018-RS       H 425 141 141  130 101
# 4 2018-RS       S 405 181 181  172 109
# 5 2018-SS       H   5   0   5    5   5
# ------> Checking Special Session Bill Reintroductions
# ~~ Adjusted outcomes for HB1230 -- Reintroduced in Special Session
# ~~ Adjusted outcomes for HB1315 -- Reintroduced in Special Session
#### APPOINTED ~ HOUSE:
# -- LINDAUER (shane)
# -- BARTELS (steve)
#### APPOINTED ~ SENATE:
# -- ZAY (andy)
# -- BUCHANAN (brian)
# -- SPARTZ (victoria)


# filter(klarner, grepl('', cand)  ) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(klarner, ddez == 46) %>% select(cand, year, sen, etype, outcome, ddez, candid)


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
# name = still_missing[9]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, missing, name_sub, still_missing)

name_matches <- data.frame(LES_name = "smith", k_name = 'smith, samuel jr.')
name_matches <- add_row(name_matches, LES_name = 'lutz', k_name = 'lutz, larry e.')
name_matches <- add_row(name_matches, LES_name = 'van haaften', k_name = 'vanhaaften, william trent')
name_matches <- add_row(name_matches, LES_name = 'gutwein', k_name = 'gutwein, eric a.')
name_matches <- add_row(name_matches, LES_name = 'arnold', k_name = 'arnold, jim')
# name_matches <- add_row(name_matches, LES_name = 'schneider', k_name = 'schneider, scott')
name_matches <- add_row(name_matches, LES_name = 'sullivan', k_name = 'sullivan, holli')
name_matches <- add_row(name_matches, LES_name = 'lyness, randy', k_name = 'lyness, randall j.')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', k_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}

##### Matches requiring greater precision
# LES[LES$sponsor %in% "zzzzzz" & LES$term %in% "2005_2006",]$klarner_id <- zzzzzz
# LES[LES$sponsor %in% "zzzzzz" & LES$term %in% "2005_2006",]$klarner_name <- "zzzzzz"
# LES[LES$sponsor %in% "zzzzzzzz" & LES$term %in% "2005_2006",]$sponsor <- "zzzzzzz"

rm(name_matches, i)

############## Check for Duplicates
### ---> If two people with same last name and one won in a special, will lead to duplicates
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
rm(check_dup, k_sub, exact)


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
fill_missing <- data.frame(LES_name = "bottorff, c", new_name = 'bottorff, carlene', party = 'd', district = 71, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "harris, donna", new_name = 'harris, donna', party = 'd', district = 2, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "banks, amanda", new_name = 'banks, amanda', party = 'r', district = 17, exper = 'none')
#### 2017+
fill_missing <- add_row(fill_missing, LES_name = "bartels, steve", new_name = 'bartels, stephen', party = 'r', district = 74, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "lindauer, shane", new_name = 'lindauer, shane', party = 'r', district = 63, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "zay, andy", new_name = 'zay, andy', party = 'r', district = 17, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "spartz, victoria", new_name = 'spartz, victoria', party = 'r', district = 20, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "buchanan, brian", new_name = 'buchanan, brian', party = 'r', district = 7, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)


#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

### Fix Issue with Jrs in 2017
LES[LES$sponsor == 'presseljr, james r.',]$sponsor <- 'pressel, james r., jr.'
LES[LES$sponsor == 'harrisjr, earl l.',]$sponsor <- 'harris, earl l., jr.'

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

#### Expanding the Necessary Senate Rows + Adding back in
# **** ALL data should be right (assuming still in chamber) EXCEPT Majority Member IN STAGGERED STATES ??? ********
# ----> Seniority will be based of complete 4-year terms... so 1,1,2,2,etc.
# ----> 4 year staggered terms ---> Expand 25 Seantors per year
senate <- filter(hf_data, chamber == "Senate")
senate$year <- senate$year + 2
senate$term <- paste0(senate$year + 1, "_", senate$year + 2)
senate$MajorityMember <- NA 
hf_data <- bind_rows(hf_data, senate) %>% arrange(year, chamber)
rm(senate)

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

rm(hf_data)

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
  
  ### SKIP IF ONLY PRE-1993 
  in_chamber_terms <- LES[LES$sponsor == LES[i,]$sponsor,]$term
  if(!any(substring(in_chamber_terms, 1, 4) >= 1993)){
    next
  }
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

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches: Dorothy Suzanne “Sue” Landske; Allen Lucas Messer; Darryl Brent Waltz
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

## Mismatch
## ******* NOTE: SM NAME ERROR - Amanda Banks should be James Banks (D17, identical time period in Senate) *****
# LES[LES$sponsor %in% c('banks, james e.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('nancy', tolower(name))) %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

name_matches <- data.frame(LES_name = 'banks, james e.', SM_name = 'Banks, Amanda')
# name_matches <- add_row(name_matches, LES_name = 'barnes, john f.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'brown, liz', SM_name = 'Brown, Elizabeth M.')
# name_matches <- add_row(name_matches, LES_name = 'clements, jacqueline r.', SM_name = 'zzzzzzz')
## ********* Note: SM Name Error: Earl Harris collapsed into Donna Harris (who succeeded him after he died) **********
name_matches <- add_row(name_matches, LES_name = 'harris, earl', SM_name = 'Harris, Donna J.')
## ******** Luke Kenley is collapsed into Howard Kenley
name_matches <- add_row(name_matches, LES_name = 'kenley, luke', SM_name = 'Kenley, Howard')
# name_matches <- add_row(name_matches, LES_name = 'michael, nancy a.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'miller, pete', SM_name = 'Miller, Peter')
name_matches <- add_row(name_matches, LES_name = 'morris, bob', SM_name = 'Morris, Robert')
# name_matches <- add_row(name_matches, LES_name = 'pearson, joseph r.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'reardon, mara candelaria', SM_name = 'Candelaria Reardon, Mara')
name_matches <- add_row(name_matches, LES_name = 'smith, jim c.', SM_name = 'Smith, James')
name_matches <- add_row(name_matches, LES_name = 'smith, samuel jr.', SM_name = 'Smith, Samuel Jr.')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

###############
#### Manual Edits (Needs more precision...)
###############

#### Richard Hamm -- Split over two rows
LES[LES$sponsor == 'hamm, richard l.',]$SM_name <-  ideo[ideo$name == 'Hamm, Richard' & ideo$house2014 %in% 1,]$name
LES[LES$sponsor == 'hamm, richard l.',]$SM_party <- ideo[ideo$name == 'Hamm, Richard' & ideo$house2014 %in% 1,]$party
LES[LES$sponsor == 'hamm, richard l.',]$np_score <- ideo[ideo$name == 'Hamm, Richard' & ideo$house2014 %in% 1,]$np_score

#### Donna Schaibley -- Split over two rows
LES[LES$sponsor == 'schaibley, donna',]$SM_name <-  ideo[ideo$name == 'Schaibley, Donna' & ideo$house2015 %in% 1,]$name
LES[LES$sponsor == 'schaibley, donna',]$SM_party <- ideo[ideo$name == 'Schaibley, Donna' & ideo$house2015 %in% 1,]$party
LES[LES$sponsor == 'schaibley, donna',]$np_score <- ideo[ideo$name == 'Schaibley, Donna' & ideo$house2015 %in% 1,]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname)


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1999 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1999:2004, 2007:2010) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2005:2006, 2011:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1999 - 2020
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzz) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1999:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################

##### Drop Nicknames
# filter(LES, grepl('\\(|\\"', sponsor)) %>% distinct(sponsor)
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]$', sponsor)) %>% distinct(sponsor)
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

#### Name Fixes
LES[LES$sponsor == 'grubb, f. dale',]$sponsor <- 'grubb, floyd dale'
LES[LES$sponsor == 'burton, woody',]$sponsor <- 'burton, charles'
LES[LES$sponsor == 'landske, sue',]$sponsor <- 'landske, dorothy sue'
LES[LES$sponsor == 'messer, luke',]$sponsor <- 'messer, allen luke'
LES[LES$sponsor == 'bright, billy',]$sponsor <- 'bright, william'
LES[LES$sponsor == 'hoy, phil',]$sponsor <- 'hoy, george philip'
LES[LES$sponsor == 'glick, c. susan',]$sponsor <- 'glick, susan'
LES[LES$sponsor == 'waltz, brent',]$sponsor <- 'waltz, darryl brent'
LES[LES$sponsor == 'perfect, chip',]$sponsor <- 'perfect, clyde a.'


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
  scale_color_manual(values=c("dodgerblue2", "red2", "gray50"))

##### CHECK OUTLIERS
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')
# stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

