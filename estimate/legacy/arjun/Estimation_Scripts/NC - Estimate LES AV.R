

##########################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** NORTH CAROLINA *** BY SESSION
##########################################################################

###################################
## (SPECIAL) SESSIONS:
## ---- Bills carryover from regular to regular session (one long biennium)
## ---- Special session bills broken out, bill numbers RESTART
## MEMBER LISTS:
## ---- See below (via internet archive)
## PROCESS/RULES:
## ---- 
## Sponsorship/Authorship
## ---- Multiple Primary Sponsors Permitted + Committee Sponsorship Permitted
## ---- SPONSORSHIP Caps per Term
###########################
## NOTES:
## -- Could go back to 1973 via PDF: https://www.ncleg.gov/Documents/1#\Legislative%20Analysis%20Division\Bill%20Histories%20(1973-1984)
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
library(tibble)
library(foreach)
library(inexact)

this_state <- 'NC'
keep_types <- c("Bill")

#### Output Directory
#dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd(glue("../../LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Terms/Sessions and Filespaths

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
t_sessions = sessions[startsWith(sessions,as.character(t)) | startsWith(sessions,as.character(t+1))]



#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- bind_rows(read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t_plus_one}.csv"),colClasses = c("character")))

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
# t <- terms[1]



### Skip Previously Estimated
if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
  print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
  next
}

###### SESSION IN PROGRESS
print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_sessions[1]}.csv")
bills <- read_csv(bill_path, col_types = cols())

### If multiple sessions in different files, read in those as well
if(length(t_sessions) > 1){
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{s}.csv")
    s_bills <- read_csv(bill_path, col_types = cols())
    bills <- bind_rows(bills, s_bills)
  }
  rm(s, s_bills)
}

### Check for duplicates
if(nrow(bills) != nrow(distinct(bills))){
  cat(" \n ~~~> DUPLICATE BILLS \n\n .")
  break
}

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_num)

##### Clean bill type
bills$bill_type <- gsub('\\(.+\\)', '', bills$bill_type)
bills$bill_type <- gsub('\\/ SL.+', '', bills$bill_type)
## Correcting Coding Errors
bills$bill_type <- ifelse(grepl('^Bill +\\/ Res\\.', bills$bill_type), 'Resolution', bills$bill_type)
bills$bill_type <- gsub('\\/.+', '', bills$bill_type)
bills$bill_type <- str_trim(gsub('  +', ' ', bills$bill_type))

############### Drop Resolutions, Messages, Communications, Reports
# bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
table(bills$bill_type)
all_bills <- bills
bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)

### Attributes:
# table(str_split(paste(bills$attributes, collapse = "; "), '; '))

##########################
####### Standardize Sponsors

#### Standardize
bills$primary_sponsors <- gsub('á', 'a', bills$primary_sponsors)
bills$primary_sponsors <- gsub('é', 'e', bills$primary_sponsors)
bills$primary_sponsors <- gsub('ó', 'o', bills$primary_sponsors)
bills$primary_sponsors <- gsub('í', 'i', bills$primary_sponsors)
bills$primary_sponsors <- gsub('ñ', 'n', bills$primary_sponsors)
bills$primary_sponsors <- tolower(bills$primary_sponsors)

bills$cosponsors <- gsub('á', 'a', bills$cosponsors)
bills$cosponsors <- gsub('é', 'e', bills$cosponsors)
bills$cosponsors <- gsub('ó', 'o', bills$cosponsors)
bills$cosponsors <- gsub('í', 'i', bills$cosponsors)
bills$cosponsors <- gsub('ñ', 'n', bills$cosponsors)
bills$cosponsors <- tolower(bills$cosponsors)

#### Primary Sponsor = 1st Sponsor
bills$primary_sponsors <- str_trim(gsub('\\(primary\\)', '', bills$primary_sponsors))

#### Fix Name Issues 
if(t_yrs == '1993_1994'){
  # See: https://www.carolana.com/NC/1900s/nc_1900s_senate_1993-1994.html
  bills$primary_sponsors <- gsub('winner of buncombe', 'd. winner', bills$primary_sponsors) 
  bills$primary_sponsors <- gsub('winner of mecklenburg', 'l. winner', bills$primary_sponsors) 
  bills$cosponsors <- gsub('winner of buncombe', 'd. winner', bills$cosponsors) 
  bills$cosponsors <- gsub('winner of mecklenburg', 'l. winner', bills$cosponsors) 
}
if(t_yrs %in% c('1993_1994', '1995_1996', '2001_2002') ){
  # See: https://www.carolana.com/NC/1900s/nc_1900s_senate_1993-1994.html
  bills$primary_sponsors <- gsub('martin of guilford', 'w. martin', bills$primary_sponsors) 
  bills$cosponsors <- gsub('martin of guilford', 'w. martin', bills$cosponsors) 
}
if(t_yrs == '1995_1996'){
  ### Name wrong in NC Data
  bills$primary_sponsors <- gsub('ballentine', 'ballantine', bills$primary_sponsors) 
  bills$cosponsors <- gsub('ballentine', 'ballantine', bills$cosponsors) 
}
if(t_yrs %in% c('1993_1994', '1995_1996', '1997_1998', '1999_2000', '2001_2002') ){
  # See: https://www.carolana.com/NC/1900s/nc_1900s_senate_1993-1994.html
  bills$primary_sponsors <- gsub('martin of pitt', 'r. martin', bills$primary_sponsors) 
  bills$cosponsors <- gsub('martin of pitt', 'r. martin', bills$cosponsors) 
}
if(t_yrs %in% c('2001_2002') ){
  # https://web.archive.org/web/20020625230944/http://www.ncleg.net/gascripts/members/Senate/senate_member_list.pl
  bills$primary_sponsors <- gsub('shaw of cumberland', 'l. shaw', bills$primary_sponsors) 
  bills$cosponsors <- gsub('shaw of cumberland', 'l. shaw', bills$cosponsors) 
  bills$primary_sponsors <- gsub('shaw of guilford', 'r. shaw', bills$primary_sponsors) 
  bills$cosponsors <- gsub('shaw of guilford', 'r. shaw', bills$cosponsors) 
}
if(t_yrs %in% c('2005_2006', '2007_2008', '2009_2010') ){
  # https://web.archive.org/web/20020625230944/http://www.ncleg.net/gascripts/members/Senate/senate_member_list.pl
  bills$primary_sponsors <- gsub('berger of franklin', 'd. berger', bills$primary_sponsors) 
  bills$cosponsors <- gsub('berger of franklin', 'd. berger', bills$cosponsors) 
  bills$primary_sponsors <- gsub('berger of rockingham', 'p. berger', bills$primary_sponsors) 
  bills$cosponsors <- gsub('berger of rockingham', 'p. berger', bills$cosponsors) 
}
if(t_yrs %in% c('2005_2006') ){ # First name coded for ed jones, not earl jones
  bills$primary_sponsors <- gsub('^jones', 'earl jones', bills$primary_sponsors) 
  bills$primary_sponsors <- gsub('; jones', '; earl jones', bills$primary_sponsors) 
  bills$cosponsors <- gsub('^jones', 'earl jones', bills$cosponsors) 
  bills$cosponsors <- gsub('; jones', '; earl jones', bills$cosponsors) 
}

### LES SPONSOR Var
bills$LES_sponsor <- gsub(';.+', '', bills$primary_sponsors)
bills$LES_sponsor <- gsub('\\.$', '', bills$LES_sponsor)
table(bills$LES_sponsor)

### Distinguish between Duplicate Last Names without First (using introduced bills to identify)
# filter(bills, LES_sponsor == 'wilson') %>% select(session, bill_id, bill_url) %>% arrange(session, bill_id) %>% as.data.frame()
if(t_yrs == "1993_1994"){
  ### Judy and John (Jack) Hunt -- Judy appears to retire mid-term -- Not listed here and jack serves in 1995: https://www.carolana.com/NC/1900s/nc_1900s_house_1993-1994.html
  bills[bills$LES_sponsor %in% "hunt" & bills$session == "1993-RS" & bills$bill_id %in% c("H0219", "H0839", 'H1083', 'H1295'),]$LES_sponsor <- 'hunt, judy'
  bills[bills$LES_sponsor %in% "hunt" & substring(bills$bill_id, 1, 1) == "H",]$LES_sponsor <- 'hunt, john'    
}else if(t_yrs == "1997_1998"){
  ### Howard and Robert Hunter
  bills[bills$LES_sponsor %in% "hunter" & bills$session == "1997-RS" & bills$bill_id %in% c('H0099', 'H0100', 'H0278', 'H0279', 'H0302', 'H0367', 'H0503', 'H0504', 'H0505', 'H0506', 'H0651', 'H0805', 'H0827', 'H0944', 'H1060', 'H1178', 'H1179', 'H1539', 'H1689'),]$LES_sponsor <- 'h. hunter'
  bills[bills$LES_sponsor %in% "hunter" & bills$session == "1997-RS" & bills$bill_id %in% c('H0191', 'H0192', 'H0193', "H0507", 'H0831', 'H0873', 'H0893', 'H0947', 'H1132', 'H1139', 'H1140', 'H1158', 'H1214', 'H1399', 'H1400'),]$LES_sponsor <- 'r. hunter'
  ### Connie and William (Gene) Wilson
  bills[bills$LES_sponsor %in% "wilson" & bills$session == "1997-RS" & bills$bill_id %in% c("H0003", 'H0497', 'H0536', 'H0587', 'H0612', 'H1162', 'H1293', 'H1317', 'H1422', 'H1424', 'H1429', 'H1481', 'H1530', 'H1702'),]$LES_sponsor <- 'c. wilson'
  bills[bills$LES_sponsor %in% "wilson" & bills$session == "1997-RS" & bills$bill_id %in% c("H0143", 'H0994', 'H1211'),]$LES_sponsor <- 'g. wilson'  
}

#### Adjusting for Bills By Request -- Need to do BEFORE splitting
if(any(grepl("\\(by request\\)|\\(by re\\)| +request", bills$LES_sponsor))){
  cat('\n')
  print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
}



##########################
###### Merge in S&S Bills
#########################
# *** For NORTH CAROLINA: Bills carry over during regular (one biennium), but numbers re-start for all special sessions
# ---> Need to merge on Id and Session 
# ---> Capping Bill numbers at SESSION MAX and assuming bill is from SS with most bills proposed

if(t_yrs == "2019_2020"){
  SS_bills$bill_id[SS_bills$bill_id=="S0168"] = "SB0168" # typo on PVS?
  
}

if(t_yrs=="2021_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="HB0172"] = "HR0172" # it's actually a resolution
}

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,primary_sponsors), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% c("HB","SB")) %>% select(-bill_type) %>%
  filter(! grepl("committee",primary_sponsors, ignore.case=T)) %>%
  arrange(primary_sponsors) 
unique(missing_SS_bills$bill_id) 



# now check to see if there are duplicate joins

duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills , 
            by = c("bill_id", "term")) %>%
  group_by(rowname) %>% mutate(count = n()) %>%
  ungroup() %>% 
  select(count,bill_id,term,session, Title, short_title) %>% 
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

####################################################
############### Code Commemorative
####################################################

bills <- commem_bills %>%
  filter(term == t_yrs) %>% 
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session'))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)

#### List of Committees to Re-code
comms <- c('agriculture', 'judiciary', 'alcoholic beverage', 'appropriations', 'judiciary i',
           'local and regional government ii', 'state government', 'congressional redistricting',
           'health and human services', 'rules, calendar, and operations of the house', 'ethics',
           'rules')

#### Drop Uncoded Committees
if(nrow(filter(bills,  grepl(paste(comms, collapse = "|"), LES_sponsor))) > 0 | any(grepl('committee', bills$LES_sponsor))){
  cat('\n')
  cat(glue('-----> Dropping {nrow(filter(bills, grepl(paste(comms, collapse = "|"), LES_sponsor) | grepl("committee", LES_sponsor) ))} Committee Sponsored Bills (N = {nrow(bills)})'))
  bills <- filter(bills, !(grepl(paste(comms, collapse = "|"), LES_sponsor) | grepl("committee", LES_sponsor)))
}

#### Fill in Missing Sponsors with Full Sponsor List
# bills$LES_sponsor <- ifelse(bills$LES_sponsor == "", gsub(';.+', '', bills$coauthors), bills$LES_sponsor)

###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "") %>% View()
if(nrow(filter(bills, LES_sponsor == '')) > 0 | any(is.na(bills$LES_sponsor)) ){
  cat('\n')
  cat(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == '')) + sum(is.na(bills$LES_sponsor))} bill(s) without a sponsor"))
  bills <- filter(bills, !(LES_sponsor == '' | is.na(LES_sponsor)))     
}

####################################################
############### Code Bill History
####################################################

bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
bill_hist <- read_csv(bill_hist_path, col_types = cols())

## If multiple sessions, read in those as well
if(length(t_sessions) > 1){
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
    s_hist <- read_csv(bill_path, col_types = cols())
    bill_hist <- bind_rows(bill_hist, s_hist)
  }
  rm(s, s_hist)
}

######## Standardize BillHist Bill IDs
bill_hist <- rename(bill_hist, bill_id = bill_num) 

### Rearrange + create order variable that covers both chambers
bill_hist <- arrange(bill_hist, session, bill_id, order) 

### Coding Chamber Variable
if(nrow(filter(bill_hist, chamber == '')) > 0 | any(is.na(bill_hist$chamber))){
  bill_hist[grepl('^ratified|to gov\\.|signed by gov\\.|^ch\\. sl|^veto', tolower(bill_hist$action) ) & (bill_hist$chamber == "" | is.na(bill_hist$chamber)),]$chamber <- 'Executive'
}

### Standardize Chamber Variable
# bill_hist$chamber <- recode(bill_hist$chamber, "A" = "House", "S" = "Senate", "G" = "Governor")

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c("^reptd", 'assigned to.+subcomm')
# "withdrawn from com" --> Often --> re-referral... if it skips committee, something else will likely happen
abc_t <- c("^reptd fav", '^reptd without', '^reptd com sub', '^reptd as amend',
           "amend adopted", "amend failed", 'amend pending', 'amends ruled material', 'amend recon',
           "added to calendar", "placed on cal for", "passed 2nd", "failed 2nd")
# Dropping "engrossed" as occassionally happens BEFORE bill passes chamber of introduction
pc_t <- c("passed 3rd reading", "passed 2nd \\& 3rd reading", "^adopted$", "message sent to", 
          '^rec from house', '^rec from senate')
if(t_yrs == "1993_1994"){ # records include "incorporated ch.[0-9]+" for 1993/94 -- presumably if language is adopted in other legislation
  law_t <- c("signed by gov", '^ratified', 'ratified ch.[0-9]+')
}else{
  law_t <- c("signed by gov", "ch\\. sl [0-9]+", '^ratified', 'ch.[0-9]+')
}

### Check Actions
# filter(bill_hist, grepl('engrossed', tolower(action))) %>% distinct(action) %>% View()
# filter(bill_hist, grepl('effective date', tolower(action))) %>% group_by(action) %>% summarize(n = n()) %>% arrange(desc(n)) %>% View()

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
  if(bill_stages$passed_chamber == 0 & any(grepl("engrossed|^rec from", tolower(hist_sub$action))) & length(unique(hist_sub$chamber)) > 1){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
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
# filter(bill_hist, session == '1987-RS' & bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 1 & all_bill_stages$session == '1987-RS',]$bill_id) %>% View()


read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-2}_{t-1}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% filter(!grepl("-S",session)) %>%  print()

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-4}_{t-3}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% filter(!grepl("-S",session)) %>% print()


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

### Adjust Commems if SS == 1
table(all_bill_stages$SS, all_bill_stages$commem)
all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
rm(SS_term)


### Save Stage Info **** MERGE WITH COMMEM + SS
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

######## Cosponsorship Info --- For NV: 2011+ Cosponsorship info may be sporadic
all_sponsors$num_cosponsored_bills <- NA
bills$cospon_match <- paste(bills$LES_sponsor, bills$primary_sponsors, bills$cosponsors, sep = '; ')
bills$cospon_match <- gsub('; NA', '', bills$cospon_match)
for(i in 1:nrow(all_sponsors)){
  c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
  ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
}
bills <- select(bills, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills)) 


if(min(all_sponsors$num_cosponsored_bills) < 0){
  print("negative cosponsors"); break
} else{print("cosponsors valid")}

### Manually Fixed First Name Legislators will have wrong cosponsor count
if(t_yrs == "1993_1994"){
  all_sponsors[all_sponsors$LES_sponsor %in% c("hunt, judy", "hunt, john"),]$num_cosponsored_bills <- NA
}else if(t_yrs == "1997_1998"){
  all_sponsors[all_sponsors$LES_sponsor %in% c("r. hunter", "h. hunter"),]$num_cosponsored_bills <- NA
  all_sponsors[all_sponsors$LES_sponsor %in% c("c. wilson", "g. wilson"),]$num_cosponsored_bills <- NA
}

#######################
#### CLEAN NAMES
all_sponsors$last_name <- gsub(',.+|^[a-z]\\. ', '', all_sponsors$LES_sponsor)
all_sponsors$first_name <- ifelse(grepl(',|^[a-z]\\. ', all_sponsors$LES_sponsor), gsub('.+, |\\..+', '', all_sponsors$LES_sponsor), '')
all_sponsors$first_name <- ifelse(all_sponsors$first_name %in% c('jr', 'sr', 'ii', 'iii', 'iv'), '', all_sponsors$first_name)
all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update First Names, Last Names for Matching 
if(t_yrs == "1993_1994"){
  all_sponsors[all_sponsors$LES_sponsor == "thompson",]$first_name <-  "r" ## G. Thompson first name included, R. Thompson Not
  all_sponsors[all_sponsors$LES_sponsor == "wilson",]$first_name <-  "p" ## C. Wilson first name included, P. Wilson Not
}
if(t_yrs %in% c('1995_1996', '1999_2000', '2003_2004') ){
  all_sponsors[all_sponsors$LES_sponsor %in% c("g. wilson", "wilson, g"),]$first_name <- 'w' # William E. 'Gene' Wilson
}
if(t_yrs %in% c('2005_2006')){
  all_sponsors[all_sponsors$LES_sponsor %in% c("ed jones", "earl jones"),]$last_name <- 'jones' 
  all_sponsors[all_sponsors$LES_sponsor == "ed jones",]$first_name <- 'ed' 
  all_sponsors[all_sponsors$LES_sponsor == "earl jones",]$first_name <- 'earl' 
}
if(t_yrs %in% c('2013_2014')){
  all_sponsors[all_sponsors$LES_sponsor == "r. brawley",]$first_name <- 'c' # C. Robert Brawley
  all_sponsors[all_sponsors$LES_sponsor == "w. brawley",]$first_name <- 'b' # Bill Brawley
}
if(t_yrs %in% c('2017_2018')){
  all_sponsors[all_sponsors$LES_sponsor == "white",]$last_name <- 'mcdowellwhite' # Donna McDowell White
  all_sponsors[all_sponsors$LES_sponsor == "williams",]$last_name <- 'huntwilliams' # Linda Hunt Williams
  all_sponsors[all_sponsors$LES_sponsor == "smith",]$last_name <- 'smithingram' # Erica Smith Ingram
  
  all_sponsors[all_sponsors$LES_sponsor %in% c("bert jones", "brenden jones"),]$last_name <- 'jones' 
  all_sponsors[all_sponsors$LES_sponsor == "bert jones",]$first_name <- 'bert' 
  all_sponsors[all_sponsors$LES_sponsor == "brenden jones",]$first_name <- 'brenden' 
  
  all_sponsors[all_sponsors$LES_sponsor %in% c("destin hall", "duane hall"),]$last_name <- 'hall' 
  all_sponsors[all_sponsors$LES_sponsor == "destin hall",]$first_name <- 'destin' 
  all_sponsors[all_sponsors$LES_sponsor == "duane hall",]$first_name <- 'duane' 
}






all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>% 
  mutate(match_name_chamber = tolower(paste(str_remove_all(LES_sponsor, '"\\s*.*?\\s*"'),substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name))

legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(t)) | startsWith(legiscan_sessions,as.character(t+1))]

legiscan = foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% select(-c(votesmart_id,knowwho_pid, ballotpedia,nickname)) %>%  distinct() 


# if(t_yrs == "2019_2020") {
#   legiscan = bind_rows(legiscan,
#                        legiscan %>% filter(people_id == 19565) %>% mutate(role = "Rep", district = "HD-060"),
#                        legiscan %>% filter(people_id == 19563) %>% mutate(role = "Rep", district = "HD-019"))
#   
# }

legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name, role) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = ifelse(n >= 2, glue("{substr(first_name,1,1)}. {last_name}"), last_name)) %>%
  mutate(match_name_chamber = tolower(paste(match_name,substr(district,1,1),sep="-")))

# inexact::inexact_addin()
# left: legiscan_adj
# right: all_sponsors
# method: osa
# mode: full

if(t_yrs == "2019_2020"){
  all_sponsors2 = 
    # You added custom matches:
    # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "cooper-suggs-h" = NA_character_,
        "smith-ingram-s" = "smith-s",
        "forde-hawkins-h" = "hawkins-h",
        "schollander-h" = NA_character_,
        "michaux-s" = NA_character_,
        "proctor-s" = NA_character_,
        "pate-s" = NA_character_,
        "craven-s" = NA_character_,
        "carter-h" = NA_character_,
        "baker-h" = NA_character_,
        "w. alexander-s" = "t. alexander-s",
        "j. johnson-h" = NA_character_
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
        "buansi-h" = NA_character_,
        "pyrtle-h" = NA_character_,
        "carter-h" = NA_character_,
        "loftis-h" = NA_character_
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

##################################################
######### Estimate Scores + Add in Relatd Variables
##################################################
### Check if bills in data without an ID'd sponsor
View(filter(bills, !(bills$LES_sponsor %in% legis_data$data_name)))
bills <- bills %>% #select(-sponsor) %>%
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

#### If LES == 0 and --- , "num_cosponsored_bills"
LES[LES$LES == 0 & is.na(LES$num_sponsored_bills), c('num_sponsored_bills', 'sponsor_pass_rate', 'sponsor_law_rate')] <- 0

############### Save LES
write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
rm(LES)


cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************"))


################# ****** END LOOP

#agg_stats, matches
rm(all_sponsors, bills, legis_data, SS_bills, H_elec_year, S_elec_year, i, keep_types)
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES, c_sub) # 
rm(t, terms, klarner_gs, comms, m_sub, commem_bills, t_sessions)


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by APPOINTMENT BY GOVERNOR --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
## Member Lists -- 2005+ -- https://web.archive.org/web/20050830132910/http://www.ncleg.net/gascripts/members/memberList.pl?sChamber=House
## Possibly 1997 - 2000+ -- https://web.archive.org/web/19971210145828/http://www.ncga.state.nc.us/
## ALL YEARS: https://www.carolana.com/NC/1900s/nc_1900s_general_assembly.html
## OFFICIAL NC STATE MANUALS: http://digital.ncdcr.gov/cdm/ref/collection/p16062coll9/id/15378
###########
### ALL African-American Members: https://www.ncleg.net/library/Documents/African-Americans.pdf
### ----> Also has appointment, resignation dates.
############

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1993_1994 TERM! ~~~~~~~~~~~~~~ 
## ---> SESSION HOUSE ROSTER: https://www.carolana.com/NC/1900s/nc_1900s_house_1993-1994.html
## APPOINTED ~ HOUSE:
# -- ADAMS (alma); CHURCH (walter); CROMER (andy); CULPEPPER (bill)
# -- KINNEY (ted); MOSLEY (jane); SMITH (ronald); YONGUE (douglas)
## APPOINTED ~ SENATE:
# -- LUCAS (jeanne)
### NAMES FIXED:
# -- John and Judy Hunt --> added first names
### DROP:
# -- ethridge, bruce -- 1st name technically wilbur; Roster above does not include him.. may have served partial term.
# -- jeralds, luther r. (nick) -- Died after election win: https://www.ncleg.net/EnactedLegislation/Resolutions/PDF/1993-1994/Res1993-3.pdf
# -- fletcher, ray c. -- not in 93/94 Manual

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1996 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 6 Committee Sponsored Bills (N = 2989)
#### APPOINTED ~ HOUSE:
# -- J. ROBINSON --> Won't show - last name duplicated -- Filled Snowden's seat ---> https://www.carolana.com/NC/1900s/nc_1900s_house_1995-1996.html
#### IN HOUSE:
# -- BRUBAKER -- Speaker
#### IN SENATE:
# -- BASNIGHT
# -- SAWYER --> https://www.carolana.com/NC/1900s/nc_1900s_senate_1995-1996.html

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~
# -----> Dropping 8 Committee Sponsored Bills (N = 3256)
### APPOINTED ~ HOUSE: 
# -- ALLEN -- Obit says elected but official returns suggest MIke Wilkins won -- so he must have replaced him -- https://web.archive.org/web/20110110010846/http://www.newsobserver.com/2010/12/25/881154/gordon-p-allen-former-legislator.html
### APPOINTED ~ SENATE:
# -- JENKINS (thomas k) ---> Must have taken over for clark plexico?
# -- MOORE, K -- Appointed 8/6/97 -- https://web.archive.org/web/19980130210837/http://www.ncga.state.nc.us/.html1997/senate/senators/senators.html
# -- PURCELL -- Appointed 7/23/97 -- https://web.archive.org/web/19980130210837/http://www.ncga.state.nc.us/.html1997/senate/senators/senators.html
### DROP:
# -- plexico, clark --> Seems to have been replaced by Jenkins + this says he served 1990 - 1996 -- https://www.blueridgenow.com/news/20021022/a-dedicated-legislator-justus-remembered
### NAMES FIXED:
# -- Howard and Robert Hunter --> added first names
# -- Connie and WIlliam (Gene) Wilson --> added first names


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# ---> Member List: https://web.archive.org/web/20001210074300/http://www.ncga.state.nc.us/gascripts/members/house/house_member_list.pl
# -----> Dropping 75 bill(s) without a sponsor
### APPOINTED ~ HOUSE:
# -- POPE (4.13.99)
# -- WEISS (11.29.99)
### IN HOUSE:
# -- TEAGUE
# -- CARPENTER (Resigned 5/3/00)
# -- NEELY (Resigned 4/7/99)
# -- MOSLEY (Died 9/28/99)
# -- MOORE (Resigned 5/7/00)
# -- BRASWELL (Resigned 2/11/00)
### IN SENATE
# -- BASNIGHT (President Pro Tem)
# -- CARRINGTON

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# -----> MEMBER LIST: https://web.archive.org/web/20020624174556/http://www.ncleg.net/gascripts/members/house/house_member_list.pl
# -----> Dropping 3 Committee Sponsored Bills (N = 3163)
### APPOINTED ~ HOUSE:
# -- M. CRAWFORD -- Appointed 4/11/2001 -- Last name duplicate -- won't print in list
### IN HOUSE:
# -- CREECH; BLACK; HIATT; WILSON; WOMBLE; ESPOSITO
### IN SENATE:
# -- BASNIGHT

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# TERM CONTEXT: https://en.wikipedia.org/wiki/Richard_T._Morgan --> 2 speakers, coalitions of Reps with Dems
# -----> Dropping 10 bill(s) without a sponsor
### APPOINTED ~ HOUSE:
# -- FISHER -- http://www.electsusanfisher.org/about/
### APPOINTED ~ SENATE:
# -- HUNT -- https://www.carolana.com/NC/2000s/nc_2000s_senate_2003-2004.html
# -- NESBITT -- Appointed 2/4/2004 via H: https://www.carolana.com/NC/2000s/nc_2000s_senate_2003-2004.html
### IN HOUSE:
# -- MORGAN -- Rep Speaker -- Later removed from party exec committee for disloyalty for supporting bipartisan coalition
# -- BLACK -- Dem Speaker
# -- MCMAHAN
# -- NESBITT -- appointed to Senate after 1 year
# -- BASNIGHT
# -- CARRINGTON

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# ----> MEMBER LISTS: https://web.archive.org/web/20061221193316/http://www.ncleg.net/gascripts/members/memberList.pl?sChamber=Senate
### APPOINTED ~ HOUSE:
# -- SPEAR -- 1/2006 -- https://en.wikipedia.org/wiki/Timothy_L._Spear 
# -- ED JONES -- Name won't show, last name duplicated -- https://en.wikipedia.org/wiki/Edward_Jones_(North_Carolina_politician)
### APPOINTED ~ SENATE:
# -- BLAND -- 2/1/2006
# -- MILLER -- Appointed 3/8/2006, resigned 2 months later -- https://web.archive.org/web/20061222221143/http://www.ncleg.net/gascripts/members/viewMember.pl?sChamber=Senate&nUserID=202
### IN HOUSE:
# -- HALL (john, died 3/17/2005)
# -- MORGAN
# -- DOCKHAM
# -- BLACK
### IN SENATE:
# -- BASNIGHT

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- BLUE -- Appointed to fill Allen's seat
# -- BRYANT -- 1/23/2007
# -- COTHAM -- 3/22/2007
# -- FURR -- 8/15/2007
# -- HUGHES -- 4/8/2008
# -- MOBLEY -- 1/23/2007
### APPOINTED ~ SENATE:
# -- JONES (ed, 1/24/2007)
# -- MCKISSICK (4/17/2007)
### IN HOUSE: 
# -- BRISSON
# -- HACKNEY
# -- BLACK (resigned 2/14/2007)
# -- CUNNINGHAM (resigned 12/31/2007)
### IN SENATE:
# -- BASNIGHT
# -- LUCAS (jeanne, died 3/9/2007)
### DROP:
# -- allen, bernard -- Died 10/14/2006 -- https://en.wikipedia.org/wiki/Bernard_Allen_(U.S._politician)
# -- hunter, howard j. jr. -- Died 1/7/2007 -- https://web.archive.org/web/20081219191614/http://www.ncleg.net/gascripts/members/memberList.pl?sChamber=House
# -- jones, edward (ed) -- FROM HOUSE -- Appointed to Senate
# -- holloman, robert l -- Died 1/8/2007 -- https://web.archive.org/web/20081219192025/http://www.ncleg.net/gascripts/members/memberList.pl?sChamber=Senate

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> https://web.archive.org/web/20101120193635/http://www.ncleg.net/gascripts/members/memberList.pl?sChamber=House
# -----> https://web.archive.org/web/20101120193640/http://www.ncleg.net/gascripts/members/memberList.pl?sChamber=Senate
### APPOINTED ~ HOUSE:
# -- HEAGARTY; ILER; INGLE; JACKSON; PARFITT 
### APPOINTED ~ SENATE:
# -- BLUE (via H, 5/19/2009)
# -- DICKSON
# -- WALTERS
### IN HOUSE:
# -- BRISSON; LANGDON; HACKNEY; MILLS; WHILDEN
### IN SENATE: 
# -- BASNIGHT
### DROP:
# -- coleman, linda -- resigned 1/11/2009


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# -----> https://web.archive.org/web/20121123181651/http://www.ncleg.net/gascripts/members/memberList.pl?sChamber=House
# -----> https://web.archive.org/web/20121124040845/http://www.ncleg.net/gascripts/members/memberList.pl?sChamber=Senate
# -----> Dropping 16 Committee Sponsored Bills (N = 2047)
### APPOINTED ~ HOUSE:
# -- MCGUIRT
### APPOINTED ~ SENATE:
# -- CARNEY; WESTMORELAND; WHITE
### IN HOUSE:
# -- GIBSON (resigned 3/3/2011)
# -- TILLIS
### IN SENATE:
# -- BERGER
### DROP:
# -- BASNIGHT -- resigned 1/25/2011

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# -----> https://web.archive.org/web/20141116215441/http://www.ncleg.net/gascripts/members/memberList.pl?sChamber=House
# -----> Dropping 15 Committee Sponsored Bills (N = 1987)
### APPOINTED ~ HOUSE:
# -- DOBSON; MEYER; RICHARDSON; YOUNTS
### APPOINTED ~ SENATE:
# -- BRYANT; FOUSHEEE
### DROP:
# -- bryant, angela r. -- resigned 1/4/2013
# -- gillespie, mitch -- resigned 1/6/2013
# -- jones, edward (ed) -- died 12/14/2012

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# -----> https://web.archive.org/web/20161202132608/http://ncleg.net/gascripts/members/memberList.pl?sChamber=House
# -----> https://web.archive.org/web/20161026092552/http://www.ncleg.net/gascripts/members/memberList.pl?sChamber=Senate
# -----> Dropping 3 Committee Sponsored Bills (N = 1989)
### APPOINTED ~ HOUSE:
# -- J. MOORE (justin)
# -- K. HALL (kyle)
# -- MURPHY
# -- ROBINSON
# -- SGRO
# -- W. RICHARDSON -- Last Name Duplicated, won't print 
### APPOINTED ~ SENATE:
# -- LOWE
### IN HOUSE:
# -- GRAHAM (george)
# -- MICHAUX
# -- T. MOORE (tim)
### DROP:
# -- starnes, edgar v. -- resigned 1/13/2015
# -- parmon, earline w. -- resigned 1/28/2015

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# ------> https://web.archive.org/web/20181109112127/https://www.ncleg.net/gascripts/members/memberList.pl?sChamber=House
# ------> https://web.archive.org/web/20181105221716/https://ncleg.net/gascripts/members/memberList.pl?sChamber=senate
# -----> Dropping 2 Committee Sponsored Bills (N = 1868)
### APPOINTED ~ HOUSE:
# -- BLACK; BUTLER; MOREY
### IN HOUSE:
# -- AUTRY; EARLE; PRESNELL
### DROP:
# -- hamilton, susi -- resigned 1/26/2017
# -- hall, larry d. -- resigned 1/16/2017
# -- luebke, paul -- died 10/29/2016

# filter(klarner, grepl("mcguirt", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(klarner, ddez == 29 & sen == 1 & outcome == 'w') %>% arrange(year, cand) %>% select(cand, year, sen, etype, outcome, ddez, candid)


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

### Error Fixes
LES[LES$data_name %in% "black" & LES$term %in% "2017_2018",]$klarner_id <- NA
LES[LES$data_name %in% "black" & LES$term %in% "2017_2018",]$klarner_name <- NA
LES[LES$data_name %in% "black" & LES$term %in% "2017_2018",]$sponsor <- "black, maryann eaddy"

### ****Still missing***** 
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[21]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term)
# filter(klarner, grepl('morey', cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, missing, name_sub, still_missing)

### Fix Missing
name_matches <- data.frame(LES_name = 'smith', k_name = 'smith, ronald l. (ronnie)') ## SKipped a year
name_matches <- add_row(name_matches, LES_name = 'adams', k_name = 'adams, alma')
name_matches <- add_row(name_matches, LES_name = 'lucas', k_name = 'lucas, jeanne h.')
name_matches <- add_row(name_matches, LES_name = 'allen', k_name = 'allen, gordon p.')
name_matches <- add_row(name_matches, LES_name = 'jenkins', k_name = 'jenkins, thomas k.')
name_matches <- add_row(name_matches, LES_name = 'hunt', k_name = 'hunt, ralph a.')
name_matches <- add_row(name_matches, LES_name = 'jones', k_name = 'jones, edward (ed)')
name_matches <- add_row(name_matches, LES_name = 'jackson', k_name = 'jackson, darren')
name_matches <- add_row(name_matches, LES_name = 'dickson', k_name = 'dickson, margaret highsmith')
name_matches <- add_row(name_matches, LES_name = 'white', k_name = 'white, stan m.')
name_matches <- add_row(name_matches, LES_name = 'westmoreland', k_name = 'westmoreland, wes')
name_matches <- add_row(name_matches, LES_name = 'richardson', k_name = 'richardson, bobbie j.')
name_matches <- add_row(name_matches, LES_name = 'younts', k_name = 'younts, roger')
name_matches <- add_row(name_matches, LES_name = 'robinson', k_name = 'robinson, george') ## Appointed again after time out of office
name_matches <- add_row(name_matches, LES_name = 'butler', k_name = 'butler, deb') # ****** MAY NOT BE NEEDED AFTER KLARNER UPDATE
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')
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
fill_missing <-  data.frame(LES_name = "miller", new_name = 'miller, william b.', party = 'r', district = 31, exper = 'none') ## https://web.archive.org/web/20061222221143/http://www.ncleg.net/gascripts/members/viewMember.pl?sChamber=Senate&nUserID=202
fill_missing <- add_row(fill_missing, LES_name = "sgro", new_name = 'sgro, christopher m.', party = 'd', district = 58, exper = 'none') ## https://web.archive.org/web/20161202132608/http://ncleg.net/gascripts/members/viewMember.pl?sChamber=House&nUserID=705 // # https://en.wikipedia.org/wiki/Chris_Sgro
#### 2017-2018
fill_missing <- add_row(fill_missing, LES_name = "black, maryann eaddy", new_name = 'black, maryann eaddy', party = 'd', district = 29, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "morey", new_name = 'morey, marcia', party = 'd', district = 30, exper = 'none')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

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
# ** NOTE: hunter, howard jaque iii matches to father -- record is both of them, but only  1 of 13 years is iii vs jr.
LES[LES$sponsor %in% c('clark, robert b. iii', 'davis, dennis', 'hunter, howard jaque iii', 'jones, brenden harding', 
                       'lee, hugh', 'moore, richard b.', 'wilson'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# Notable Names: James Mark McDaniel; Janet KAY Hagan; Jacob Curtis Blackwood; Ernest 'Wil' Neumann
# Benjamin Stephenson Goss; Kenneth Marcus Brandon? (dates overlap); James 'Jay' Cecil Adams? (dates overlap)
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# ------> FOR NC ---> NO HOUSE DATA FOR 1993-1994, NO SENATE DATA FOR 1993_1994 + 1995_1996
# filter(LES, is.na(np_score)) %>% filter( !(term %in% c('1993_1994','2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('miller', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'alexander, kelly', SM_name = 'Alexander Jr, Kelly M') 
name_matches <- add_row(name_matches, LES_name = 'barefoot, chad', SM_name = 'Barefoot, John Chadwick')
name_matches <- add_row(name_matches, LES_name = 'bell, john', SM_name = 'Bell IV, John Richard')
name_matches <- add_row(name_matches, LES_name = 'brawley, bill', SM_name = 'Brawley, William M.')
name_matches <- add_row(name_matches, LES_name = 'brown, rayne', SM_name = 'Brown, Alicia') ## Alicia - https://www.google.com/search?q=rayne+brown+north+carolina&oq=rayne+brown+north+carolina&aqs=chrome..69i57.3391j0j4&sourceid=chrome&ie=UTF-8
name_matches <- add_row(name_matches, LES_name = 'bryan, rob', SM_name = 'Bryan III, Robert P')
name_matches <- add_row(name_matches, LES_name = 'davis, ted jr.', SM_name = 'Davis Jr, Robert Theodore')
name_matches <- add_row(name_matches, LES_name = 'dickson, margaret highsmith', SM_name = 'Dickson')
# name_matches <- add_row(name_matches, LES_name = 'edwards, c. r.', SM_name = 'zzzz') # There is an Edwards for 1995-96 but parties and districts don't match
name_matches <- add_row(name_matches, LES_name = 'floyd, pearl burris', SM_name = 'Burris-Floyd, Pearl')
# name_matches <- add_row(name_matches, LES_name = 'furr, kevin', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'goodwin, wayne', SM_name = 'Goodwin')
name_matches <- add_row(name_matches, LES_name = 'hall, duane', SM_name = 'Hall II, Duane R')
name_matches <- add_row(name_matches, LES_name = 'hanes, edward (ed) jr.', SM_name = 'Hanes Jr, Edward Francis')
# name_matches <- add_row(name_matches, LES_name = 'hobbs, fred m.', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'horn, craig', SM_name = 'Horn, Dwight Craig')
name_matches <- add_row(name_matches, LES_name = 'horn, jim', SM_name = 'Horn, William James')
name_matches <- add_row(name_matches, LES_name = 'hunt, john j.', SM_name = 'Hunt')
# name_matches <- add_row(name_matches, LES_name = 'hunter, howard jaque iii', SM_name = 'zzzzz') ## Record = him and his father
name_matches <- add_row(name_matches, LES_name = 'jackson, brent', SM_name = 'Jackson, William') # William Brent Jackson
# name_matches <- add_row(name_matches, LES_name = 'jenkins, thomas k.', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'jeter, charles', SM_name = 'Jeter Jr, Charles Roper')
# name_matches <- add_row(name_matches, LES_name = 'johnson, ralph c.', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'jones, edward (ed)', SM_name = 'Jones, Edward')
name_matches <- add_row(name_matches, LES_name = 'langdon, james h. jr.', SM_name = 'Langdon Jr, James H')
name_matches <- add_row(name_matches, LES_name = 'lee, hugh', SM_name = 'Lee')
# name_matches <- add_row(name_matches, LES_name = 'little, teena s.', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'martin, grier', SM_name = 'Martin III, David') # David Grier Martin iii
# name_matches <- add_row(name_matches, LES_name = 'mckoy, henry', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'michaux, h. m. (mickey) jr.', SM_name = 'Michaux Jr, Henry M')
name_matches <- add_row(name_matches, LES_name = 'miller, brad', SM_name = 'Miller, Ralph') # Ralph Bradley Miller
# name_matches <- add_row(name_matches, LES_name = 'miller, ken j.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'miller, william b.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'moore, joy', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'moore, richard', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'moore, tim', SM_name = 'Moore, Timothy Keith')
name_matches <- add_row(name_matches, LES_name = 'moore, tony p.', SM_name = 'Moore, Tony Sr.')
name_matches <- add_row(name_matches, LES_name = 'newton, e. s. (buck)', SM_name = 'Newton, Eldon III')
# name_matches <- add_row(name_matches, LES_name = 'parnell, david', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'plexico, clark', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'robinson, jonathan', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'sawyer, tom', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'sherron, j. k.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'simpson, daniel reid', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'smith, paul s.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'snowden, macon s.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'speed, james d.', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'thompson, gregg', SM_name = 'Thompson, Gregory James')
name_matches <- add_row(name_matches, LES_name = 'tolson, norris', SM_name = 'Tolson, E. Norris')
name_matches <- add_row(name_matches, LES_name = 'tucker, tommy', SM_name = 'Tucker, Wyatt Thomas')
name_matches <- add_row(name_matches, LES_name = 'walker, r. tracy', SM_name = 'Walker, Ronald Tracy')
name_matches <- add_row(name_matches, LES_name = 'warren, edward n. (ed)', SM_name = 'Warren, Edward')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

########## Manual Edits (Needs more precision...)

### Zeno Edwards was Rep from 1993-1996, Dem from 1999-2002 -- Appears to have two matches, one from time as Dem, one from one of the R terms
# -----> Fixing only the Repub Rows
LES[LES$sponsor == 'edwards, zeno l. jr.' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Edwards',]$name
LES[LES$sponsor == 'edwards, zeno l. jr.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Edwards',]$party
LES[LES$sponsor == 'edwards, zeno l. jr.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Edwards',]$np_score

### Frances Cummings --- Switched from D to R in 1995 --- Only have votes from time as Rep
LES[LES$sponsor == 'cummings, frances m.' & LES$party == 'd',]$SM_name <- NA
LES[LES$sponsor == 'cummings, frances m.' & LES$party == 'd',]$SM_party <- NA
LES[LES$sponsor == 'cummings, frances m.' & LES$party == 'd',]$np_score <- NA

### Bobby Hall, Served 1993-1994, 1996-1998 --- Ran as D in 1992, R in 1996, Vote data is only from time as a republican
LES[LES$sponsor == 'hall, bobby' & LES$party == 'd',]$SM_name <- NA
LES[LES$sponsor == 'hall, bobby' & LES$party == 'd',]$SM_party <- NA
LES[LES$sponsor == 'hall, bobby' & LES$party == 'd',]$np_score <- NA

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1993 - 2020 
LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:1994, 1999:2010) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:1998, 2011:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### *** SPLIT CONTROL IN 2003-2004: Dem and Rep Speaker; Committee Co-Chairs
# --> https://www.ncleg.gov/DocumentSites/HouseDocuments//2003-2004%20Session/Journals/2003%20House%20Journal%20-%20Volume%201.pdf
# ** ---> CODING ALL 0
LES[LES$chamber == 'House' & LES$term == '2003_2004',]$in_majority <- 0

### Senate -- 1993 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:2010) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2011:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

##### Drop Nicknames
# filter(LES, grepl('\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

#### Manual Fixes
LES[LES$sponsor == 'brown, rayne',]$sponsor <- 'brown, alicia rayne'
LES[LES$sponsor == 'mcdowellwhite, donna',]$sponsor <- 'mcdowell-white, donna'
LES[LES$sponsor == 'redwine, e. david',]$sponsor <- 'redwine, edward david'
LES[LES$sponsor == 'ellis, j. sam',]$sponsor <- 'ellis, james samuel'
LES[LES$sponsor == 'brawley, c. robert',]$sponsor <- 'brawley, clyde robert'
LES[LES$sponsor == 'ives, w. m.',]$sponsor <- 'ives, william maner'
LES[LES$sponsor == 'cunningham, w. pete',]$sponsor <- 'cunningham, william pete'
LES[LES$sponsor == 'odom, t. l.',]$sponsor <- 'odom, thomas lafontine'
LES[LES$sponsor == 'soles, r. c. jr.',]$sponsor <- 'soles, robert c. jr.'
LES[LES$sponsor == 'martin, r. l.',]$sponsor <- 'martin, robert l.'
LES[LES$sponsor == 'owens, w. c. jr.',]$sponsor <- 'owens, william c. jr.'
LES[LES$sponsor == 'mcmahan, w. edwin',]$sponsor <- 'mcmahan, william edwin'
LES[LES$sponsor == 'aldridge, m. w.',]$sponsor <- 'aldridge, marvin warren'
LES[LES$sponsor == 'haire, r. phillip',]$sponsor <- 'haire, robert phillip'
LES[LES$sponsor == 'melton, o. max',]$sponsor <- 'melton, olin max'
LES[LES$sponsor == 'pope, j. arthur',]$sponsor <- 'pope, james arthur'
# LES[LES$sponsor == 'holliman, l. hugh',]$sponsor <- 'holliman, hugh' # Don't want to recode as lindsey or may miscode gender
LES[LES$sponsor == 'swindell, a. b.',]$sponsor <- 'swindell, alvin b.'
LES[LES$sponsor == 'neumann, wil',]$sponsor <- 'neumann, ernest wil'
LES[LES$sponsor == 'hurley, pat b.',]$sponsor <- 'hurley, patricia b.'
LES[LES$sponsor == 'cotham, tricia',]$sponsor <- 'cotham, patricia ann'
LES[LES$sponsor == 'goss, steve',]$sponsor <- 'goss, benjamin steve'
LES[LES$sponsor == 'boles, jamie',]$sponsor <- 'boles, james larry'
LES[LES$sponsor == 'hamilton, susi',]$sponsor <- 'hamilton, susan holladay'
LES[LES$sponsor == 'wells, andy',]$sponsor <- 'wells, wilfred andrew jr.'
LES[LES$sponsor == 'woodard, mike',]$sponsor <- 'woodard, james michael'
LES[LES$sponsor == 'zachary, lee',]$sponsor <- 'zachary, walter lee'
LES[LES$sponsor == 'tally, ms. lura',]$sponsor <- 'tally, lura self'

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
  scale_color_manual(values=c("dodgerblue2", 'gray', "red2"))

##### CHECK OUTLIERS
### Oscar Harris: No evience he was ever a Rep. SM seemingly wrong. 
### Tony P. Moore: switches back and forth multiple times... not clear what to do with him
## ---> He's a democrat when in office, however, and changes after the fact, so SM is coded wrong (matched to his eventual party not in office party)
# filter(LES, party == 'd' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

