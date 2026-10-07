
#############################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** TEXAS *** BY SESSION
#############################################################

###################################
## SPECIAL SESSIONS:
## ---- Seperate Files; Bill Numbers Re-start
## MEMBER LISTS:
## ---- https://lrl.texas.gov/legeLeaders/members/lrlhome.cfm
## ------> Mobile version: https://lrl.texas.gov/mobile/index.cfm
## ---- https://capitol.texas.gov/Members/Members.aspx?Chamber=H
## ---- https://capitol.texas.gov/Members/Members.aspx?Chamber=S
## PROCESS:
## ---- https://tlc.texas.gov/docs/legref/legislativeprocess.pdf
## Sponsorship/Authorship
## -- Authors/Coauthors = INtroducing Chamber, SPonsors = Outchamber
## -- https://www.houstonpublicmedia.org/articles/news/2017/04/25/197620/who-actually-writes-the-bills-your-texas-legislators-sponsor/
###########################
## NOTES:
## -- (1) NOT KEEPING if only in chamber for 2 weeks -- (e.g., Steve Ogden - 1997-1998)
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

this_state <- 'TX'
keep_types <- c("HB", "SB")

#### Output Directory
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
s_num = 0.5*(terms - 2019)+86
t_sessions = sessions[startsWith(sessions,as.character(s_num)) ]


#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{s_num}.csv"))
commem_bills <- mutate(commem_bills, bill_id = paste0(gsub(' [0-9]+', '', bill_id), str_pad(gsub('[A-Z]+ ', '', bill_id), 4, pad = 0))) %>%
  distinct() %>% mutate(term = t_yrs)

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

SS_bills$bill_id = gsub("AB","HB", SS_bills$bill_id)


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
bills$session_num <- s_num
bills$session_type <- recode(gsub(glue("^{s_num}"), '', bills$session), 'R' = 'RS', '1' = 'SS1', '2' = 'SS2', '3' = 'SS3', '4' = 'SS4', '5' = 'SS5', '6' = 'SS6', '7' = 'SS7')
bills$session <- paste0(bills$session_num, '-', bills$session_type)
bills <- select(bills, -c(session_num, session_type)) 

### Check for duplicates
if(nrow(bills) != nrow(distinct(bills))){
  cat(" \n ~~~> DUPLICATE BILLS \n\n .")
  break
}

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_number) %>% 
  mutate(bill_id = toupper(bill_id),
         bill_id = paste0(gsub(' [0-9]+', '', bill_id), str_pad(gsub('[A-Z]+ ', '', bill_id), 4, pad = 0))) %>%
  arrange(session, bill_id)

############### Drop Resolutions, Messages, Communications, Reports
all_bills <- bills
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)

############### Standardize Sponsors + Extracting + Ensuring Cosponsorship match down the line
# bills$sponsor_dist <- str_extract(bills$sponsor, 'HD [0-9]+|SD [0-9]+')
# bills$sponsor_party <- str_extract(bills$sponsor, '\\([A-Z]\\)')
# table(bills$sponsor_dist); table(bills$sponsor_party)

#### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
bills$authors <- tolower(bills$authors)
bills$authors <- gsub('á|ã¡', 'a', bills$authors)
bills$authors <- gsub('é|ã©', 'e', bills$authors)
bills$authors <- gsub('ó', 'o', bills$authors)
bills$authors <- gsub('í', 'i', bills$authors)
bills$authors <- gsub('ñ|ã±', 'n', bills$authors)

### Authors/Coauthors = Bill Originating chamber; Sponsors/Cosponsors = Outchamber?
bills$coauthors <- tolower(bills$coauthors)
bills$coauthors <- gsub('á|ã¡', 'a', bills$coauthors)
bills$coauthors <- gsub('é|ã©', 'e', bills$coauthors)
bills$coauthors <- gsub('ó', 'o', bills$coauthors)
bills$coauthors <- gsub('í', 'i', bills$coauthors)
bills$coauthors <- gsub('ñ|ã±', 'n', bills$coauthors)  

#### Adjusting for Bills By Request -- Need to do BEFORE splitting
if(any(grepl('\\(br\\)|by request| br$', bills$authors))){
  print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
  #print(glue("-----> KEEPING {nrow(filter(bills, grepl('(br)|by request', authors)))} bill(s) introduced BY REQUEST"))
}


#### LES Sponsor Variable
bills$LES_sponsor <- tolower(gsub(';.+', '', bills$authors))
table(bills$LES_sponsor)

#### Extract Nickname
bills$nickname <- str_trim(gsub('\\"', '', str_extract(bills$LES_sponsor, ' ".+"$')))
bills$LES_sponsor <- str_trim(gsub('  +', ' ', gsub(' ".+$"', '', bills$LES_sponsor)))

### Fixing Character Encoding Issue -- Need the dot ('ã.') for some reason..
if(t_yrs %in% c("2013_2014", "2015_2016", "2017_2018", "2019_2020")){
  bills[grepl('rodrã|rodriguez', bills$LES_sponsor) & substring(bills$bill_id, 1, 1) == 'S',]$LES_sponsor <- "rodriguez, jose" # José Rodríguez -- https://en.wikipedia.org/wiki/Jos%C3%A9_R._Rodr%C3%ADguez
  bills <- mutate(bills, coauthors = ifelse(grepl("^S", bill_id), gsub("rodrã.", "rodri", coauthors), coauthors))
  bills <- mutate(bills, coauthors = ifelse(grepl("^S", bill_id), gsub("rodriguez", "rodriguez, jose", coauthors), coauthors))
}

bills = bills %>% mutate_at(vars(authors,coauthors,sponsors,cosponsors, LES_sponsor),
                            function(x) gsub("anchã­a","anchia",x,ignore.case=T))





###################
###### Merge in S&S Bills
###################
# *** For TEXAS: Bills carry over during regular sessions (one biennium), but numbers re-start for all special sessions
# ---> There are MANY special sessions and SS bills... need to merge on ID and session
# ---> Capping Bill numbers at SESSION MAX and assuming bill is from SS with most bills proposed when missing special_num

if(t_yrs == "2019_2020"){
  SS_bills$bill_id[SS_bills$bill_id=="HB10001"]="HB0001"
}
if(t_yrs == "2021_2022"){
  SS_bills$bill_id[SS_bills$bill_id %in% c("SB60006", "SB10001", "SB20002", "SB30003", 
                                           "HB90009", "SB70007", "SB40004", "SB90009", "SB80008")] = 
    sub("(SB|HB)\\d(\\d+)", "\\1\\2", 
        SS_bills$bill_id[SS_bills$bill_id %in% c("SB60006", "SB10001", "SB20002", "SB30003", 
                                                 "HB90009", "SB70007", "SB40004", "SB90009", "SB80008")])
  
}

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS, year,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,sponsors), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",sponsors, ignore.case=T)) %>%
  arrange(sponsors) 
unique(missing_SS_bills$bill_id) 



# now check to see if there are duplicate joins

duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills , 
            by = c("bill_id", "term")) %>%
  group_by(rowname) %>% mutate(count = n()) %>%
  ungroup() %>% 
  select(count,bill_id,term,session, Title, summary) %>% 
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

###########################################################################
############### Code Commemorative
###########################################################################

bills <- commem_bills %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session'))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)


#### DROP Committee Bills
if(any(grepl('committee', bills$authors))){
  print(glue("-----> DROPPING {nrow(filter(bills, grepl('committee', authors)))} bill(s) introduced BY COMMITTEE"))
  break
  # bills <- filter(bills, !grepl('committee', LES_sponsor))
}

###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "") %>% View()
if(nrow(filter(bills, LES_sponsor == '')) > 0){
  print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
  bills <- filter(bills, !(LES_sponsor == ''))     
}

###########################################################################
############### Code Bill History
###########################################################################

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
bill_hist$session_num <- s_num
bill_hist$session_type <- recode(gsub(glue("^{s_num}"), '', bill_hist$session), 'R' = 'RS', '1' = 'SS1', '2' = 'SS2', '3' = 'SS3', '4' = 'SS4', '5' = 'SS5', '6' = 'SS6', '7' = 'SS7')
bill_hist$session <- paste0(bill_hist$session_num, '-', bill_hist$session_type)
bill_hist <- select(bill_hist, -c(session_num, session_type)) 

######## Standardize BillHist Bill IDs
bill_hist <- rename(bill_hist, bill_id = bill_number) %>% 
  mutate(bill_id = toupper(bill_id),
         bill_id = paste0(gsub(' [0-9]+', '', bill_id), str_pad(gsub('[A-Z]+ ', '', bill_id), 4, pad = 0))) %>%
  arrange(session, bill_id, order)

### Standardize Chamber Variable
# bill_hist$chamber <- recode(toupper(bill_hist$chamber), "H" = "House", "S" = "Senate")

### Committee Status Variable for ABC Check
bills <- bills %>% mutate(main_chamb_comm_status = ifelse(substring(bill_id,1,1) == "H", hc_status, sc_status))

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c('public hearing', 'testimony', 'pending in committee', '^reported f')
# -- 'public hearing' captures scheduling and consideration /// 'testiony' = 'recieved + taken in comm and subcomm
# -- '^reported f' = favorabily and unfavorably from comm and subcomm but NOT reported engrossed or enrolled
abc_t <- c('^reported favorably w', '1st printing sent', 'calendar', 'read 2nd time', 'point of order', '^amend', 'motion to', 
           'record vote', 'read 3rd time', 'rules suspended', 'pass', 'fail')
# '^amend = amended, amends offered, amend tabled, amend withdrawn
pc_t <- c('^passed$', '^passed as amended$', '^reported engross', 'sent to the senate', 'sent to the house')
# -- Need to be careful with passed -- passed to engrossment = passed to vote on engrossment
law_t <- c('signed by the gov', '^effective', 'filed w/o the gov.+ signature')
# filter(bill_hist, grepl('rereferred to committee', tolower(action))) %>% select(bill_id, action)
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
  ### Check if out of committee in originating chamber
  if(bill_stages$action_beyond_comm == 0 & !is.na(bills[i,]$main_chamb_comm_status) & tolower(bills[i,]$main_chamb_comm_status) == "out of committee"){
    bill_stages$action_beyond_comm <- 1
  }
  ### Check if In or Out of Committee in Opposing Chamber
  if(bill_stages$passed_chamber == 0 & substring(b_id,1,1) == "H" & !is.na(bills[i,]$sc_status) & bills[i,]$sc_status != ""){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
  }else if(bill_stages$passed_chamber == 0 & substring(b_id,1,1) == "S"  & !is.na(bills[i,]$hc_status) & bills[i,]$hc_status != ""){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
  }
  ### Check Vetos
  #????
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

#### Adding Legislators Who COSPONSORED A BILL but did not SPONSOR
unique_cospon <- str_trim(unique(unlist(str_split(bills$coauthors, '; '))))

for(nonspon in unique_cospon){
  if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != ''){
    ns_adj <- gsub('\\(', '\\\\(', gsub('\\)', '\\\\)', nonspon))
    chamb <- unique(substring(bills[grepl(ns_adj, bills$coauthors),]$bill_id, 1, 1))
    ### Skip Lt Govs
    if((t_yrs == "1989_1990" & nonspon == "hobby") | (t_yrs == "1993_1994" & nonspon == "bullock")){
      next 
    }
    ### Skip Last Names Duplicated Across Chambers
    if(t_yrs %in% c("2017_2018") & nonspon == "rodriguez"){
      next
    }
    ### Check to Make Sure in ONE Chamber
    if("H" %in% chamb & "S" %in% chamb){
      print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
    }else{
      all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0)
    }
  }
}

######## Number of Cosponsored BIlls
all_sponsors$num_cosponsored_bills <- NA
for(i in 1:nrow(all_sponsors)){
  c_sub <- filter(bills, substring(bill_id, 1, 1) == all_sponsors[i,]$chamber)
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$coauthors)))
  ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  # all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
}
# all_sponsors <- select(all_sponsors, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))


if(min(all_sponsors$num_cosponsored_bills) < 0){
  print("negative cosponsors"); break
} else{print("cosponsors valid")}

############
## CLEAN NAMES
#############

all_sponsors$last_name <- gsub(',.+', '', all_sponsors$LES_sponsor)
all_sponsors$first_name <- ifelse(grepl(',', all_sponsors$LES_sponsor), gsub('.+, ', '', all_sponsors$LES_sponsor), '')
all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update Last Names for Matching 
if(t_yrs %in% c('2005_2006', "2007_2008", "2009_2010") ){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "gonzalez toureilles", "toureilles", all_sponsors$last_name)
}
if(t_yrs %in% c("2009_2010")){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "rios ybarra", "ybarra", all_sponsors$last_name)
}
if(t_yrs %in% c("2011_2012", "2013_2014")){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "hernandez luna", "hernandez", all_sponsors$last_name)   
}



all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(LES_sponsor,substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name))


legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(t)) | startsWith(legiscan_sessions,as.character(t+1))]

legiscan = foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% select(-c(votesmart_id,knowwho_pid, ballotpedia,nickname)) %>%  distinct() 


if(t_yrs == "2019_2020"){
  legiscan = bind_rows(legiscan,
                       legiscan %>% filter(people_id == 16834) %>%
                         mutate(role = "Rep", district = "HD-076"),
                       legiscan %>% filter(people_id == 5904) %>%
                         mutate(role = "Rep", district = "HD-119"))
}
if(t_yrs == "2021_2022"){
  legiscan = legiscan %>% filter(name != "Victoria Neave Criado") # she is in here twice
}



### Create Match Variables

legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = case_when(
    n > 2 ~ paste0(last_name, " ",first_name, substr(middle_name,1,1)),
    n == 2 ~  paste(last_name,first_name),
    T ~ last_name)) %>%
  group_by(match_name) %>% 
  mutate(n = n()) %>% 
  mutate(match_name = ifelse(n > 1, paste0(last_name, " ",first_name, substr(middle_name,1,1)),
                             match_name)) %>% 
  mutate(match_name_chamber = tolower(paste(match_name,substr(district,1,1),sep="-")))


# inexact::inexact_addin()
# left: legiscan_adj
# right: all_sponsors
# method: osa
# mode: full

if(t_yrs == "2019_2020"){
  all_sponsors2 = # You added custom matches:
    # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "gutierrez roland-s" = NA_character_,
        "bonnen dennis-h" = NA_character_,
        "blanco cesar-s" = NA_character_,
        "johnson nathan-s" = "johnson-s",
        "lucio eddie-s" = "lucio-s",
        "anderson-h" = 'anderson, charles \"doc\"-h',
        "munoz-h" = 'munoz, jr.-h',
        "romero-h" = 'romero, jr.-h',
        "sherman-h" = 'sherman, sr.-h'
      )
    )
}

if(t_yrs=="2021_2022"){
  # You added custom matches:
  all_sponsors2 = inexact::inexact_join(
    x  = legiscan_adj,
    y  = all_sponsors,
    by = "match_name_chamber",
    method = "osa",
    mode = "full",
    custom_match = c(
      "parker nathaniel-s" = NA_character_,
      "johnson nathan-s" = "johnson-s",
      "springer drew-h" = NA_character_,
      "lucio eddie-s" = "lucio-s",
      "munoz-h" = "munoz, jr.-h",
      "sherman-h" = "sherman, sr.-h",
      "phelan-h" = NA_character_,
      "romero-h" = "romero, jr.-h"
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
  arrange(chamber, sponsor)


# now need to remove zero-LES legislators who never actually served. see documentation file on how this is generated

removal_legislators = read.csv("../../Estimate LES/Zero_LES_legislators_Coded.csv") %>% 
  filter(state == this_state & term == t_yrs & not_actually_in_chamber == T) %>% 
  mutate(chamber = substr(chamber,1,1))

if(nrow(removal_legislators) > 0){
  legis_data = anti_join(legis_data, removal_legislators,
                         by = c("klarner_id" = "legiscan_id", "chamber"))
}

############################
###### Estimate Scores + Add in Relatd Variables
############################

### Check if bills in data without an ID'd sponsor
# filter(bills, !(bills$primary_sponsor %in% legis_data$data_name))
bills <- bills %>% select(-sponsors, cosponsors) %>%
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

rm(all_sponsors, bills, legis_data, SS_bills, elec_year, i, keep_types)
rm(k_matches, klarner_sub, m_sub, km, bill_path, t_yrs, calc_LES) # c_sub
rm(t, terms, klarner_gs, c_sub, match_name2, s_num, t_sessions, commem_bills)
rm(ns_adj, nonspon, unique_cospon)

########################################################################################################################################################
########################################################################################################################################################
##### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
#### MEMBER DATABASE: https://lrl.texas.gov/legeLeaders/members/lrlhome.cfm
########################################################################################################################################################

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1989_1990 TERM! ~~~~~~~~~~~~~~ 
# WON SPECIAL ~ HOUSE: 
# -- BLACK (layton) -- https://lrl.texas.gov/legeLeaders/members/memberdisplay.cfm?memberID=84
# -- HARTLAND (charles)
# -- VANDERVOORT (ken) -- https://lrl.texas.gov/mobile/memberDisplay.cfm?memberID=692
# WON SPECIAL ~ SENATE: 
# -- ELLIS (rodney) -- https://lrl.texas.gov/mobile/memberDisplay.cfm?memberID=37
# IN HOUSE: 
# -- PATRONELLA (left 2/2/1989) -- https://lrl.texas.gov/mobile/memberDisplay.cfm?memberID=383
# DROP:
# -- HOBBY = William P. Hobby = Lt. Gov

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1991_1992 TERM! ~~~~~~~~~~~~~~ 
# WON SPECIAL ~ HOUSE:
# -- COLEMAN (garnet)
# -- HAMRIC (peggy)
# -- MAXEY
# -- MCCALL (brian)
# -- TURNER (bob/robert, won't show bc duplicate fixed)
# WON SPECIAL ~ SENATE: 
# -- SIBLEY
# NAME FIX:
# -- turner, bob --> turner, robert --> through 2002
# DROP:
# -- guerrero, lena --> Elected but never seated -- https://lrl.texas.gov/legeLeaders/members/memberDisplay.cfm?memberID=346&searchparams=chamber=~city=~countyID=0~RcountyID=~district=~first=le~gender=~last=guerrero~leaderNote=~leg=72~party=~roleDesc=~Committee=
# -- connelly, barry --> Resigned the day after swearing in -- https://lrl.texas.gov/legeLeaders/members/memberDisplay.cfm?memberID=334&searchparams=chamber=~city=~countyID=0~RcountyID=~district=~first=~gender=~last=connelly~leaderNote=~leg=72~party=~roleDesc=~Committee=

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1993_1994 TERM! ~~~~~~~~~~~~~~ 
# WON SPECIAL ~ HOUSE:
# -- GRAY (patricia)
# -- LUNA (vilma)
# IN HOUSE: 
# -- LANEY (james)
# DROP:
# -- hury, james -- Resigned on 9/23/1992, Died Oct 1992 -- Must have won re-election prior to that -- https://lrl.texas.gov/legeLeaders/members/memberDisplay.cfm?memberID=356&searchparams=chamber=~city=~countyID=0~RcountyID=~district=~first=~gender=~last=hury~leaderNote=~leg=72~party=~roleDesc=~Committee=
# -- cavazos, eddie -- Never sworn in -- https://lrl.texas.gov/legeLeaders/members/memberDisplay.cfm?memberID=317&searchparams=chamber=~city=~countyID=0~RcountyID=~district=~first=~gender=~last=cavazos~leaderNote=~leg=72~party=~roleDesc=~Committee=

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1996 TERM! ~~~~~~~~~~~~~~ 
# WON SPECIAL ~ HOUSE:
# -- STAPLES (todd)
# IN HOUSE: 
# -- LANEY (james)
# DROP:
# -- bomer, elton -- Elected, never sworn in -- https://lrl.texas.gov/legeLeaders/members/memberDisplay.cfm?memberID=85&searchparams=chamber=~city=~countyID=0~RcountyID=~district=~first=~gender=~last=bomer~leaderNote=~leg=72~party=~roleDesc=~Committee=

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
# WON SPECIAL ~ HOUSE: 
# -- ROMAN (bill)
# WON SPECIAL ~ SENATE:
# -- CARONA (john, via H)
# -- DUNCAN (robert, via H)
# -- OGDEN (via H)
# In HOUSE:
# -- LANEY (james)
# DROP:
# -- ogden, steve IN HOUSE -- Was seated for only 2 weeks before resigning to take senate seat.

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# IN HOUSE: 
# -- LANEY (james)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~
# WON SPECIAL ~ HOUSE: 
# -- RAYMOND (richard pena, 1 year gap, switched districts) -- https://lrl.texas.gov/legeLeaders/members/memberDisplay.cfm?memberID=228&searchparams=chamber=~city=~countyID=0~RcountyID=~district=~first=~gender=~last=raymond~leaderNote=~leg=75~party=~roleDesc=~Committee=
# IN HOUSE: 
# -- LANEY (james)
# DROP:
# -- cuellar, henry -- Won, appointed as Secretary of State, never seated

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~
# WON SPECIAL ~ HOUSE: 
# -- ESCOBAR
# -- PHILLIPS (won 12/2002 special)
# WON SPECIAL ~ HOUSE: 
# -- ELTIFE (kevin)
# DROP:
# -- clark, ron -- elected, never sworn in

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# WON SPECIAL ~ HOUSE: 
# -- HERNANDEZ
# -- STRAUS
# -- HOWARD (donna, name duplicated, won't print)
# WON SPECIAL ~ SENATE: 
# -- ELTIFE (T-1, 3/2004, through 2006)
# APPOINTED TEMPORARILY (for national guard service):
# -- ISETT (cheri) --- Duplicate Fixed -- Selected by husband to serve in his absence -- https://lrl.texas.gov/legeLeaders/members/memberDisplay.cfm?memberID=5621&searchparams=chamber=~city=~countyID=0~RcountyID=~district=~first=~gender=~last=isett~leaderNote=~leg=79~party=~roleDesc=~Committee=
# -- NORIEGA (melissa) --- Duplicate Fixed -- https://en.wikipedia.org/wiki/Melissa_Noriega
# IN HOUSE: 
# -- CRADDICK
# DROP:
# -- jones, elizabeth ames -- elected, never sworn in 
# NAME FIXED: 
# -- gonzalez toureilles --> toureilles
# NOTE:
# -- noriega, rick -- served in national guard, 1/11/2005 - 8/27/2005

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# WON SPECIAL ~ HOUSE: 
# -- O'Day (sworn in 1/24/2007 -- didn't run again)
# IN HOUSE: 
# -- CRADDICK
# DROP: 
# -- dawson, glenda -- won 11/7/2006 election despite passing away 9/12/2006
# NAME FIX:
# -- lucio iii --> updated klarner name to match (through 2018)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# WON SPECIAL ~ SENATE: 
# -- HUFFMAN
# NAME FIX:
# -- rios ybarra --> ybarra (tara rios)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# IN HOUSE: 
# -- STRAUS (joe)
# NAME FIX:
# -- hernandez luna --> hernandez (through 2014)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# WON SPECIAL ~ SENATE: 
# -- GARCIA (sylvia)
# IN HOUSE: 
# -- STRAUS (joe)
# DROP: 
# -- gallegos, mario v. jr. -- Elected bu never sworn in -- died 10/2012
# NAME FIX:
# -- rodrãguez & rodriguez in Senate == José Rodríguez (https://en.wikipedia.org/wiki/Jos%C3%A9_R._Rodr%C3%ADguez) --> 2020
# ---> Note that the name with the character encoding issue has more than just a symbol issue

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# WON SPECIAL ~ HOUSE: 
# -- BERNAL
# -- CYRIER
# -- MINJAREZ (ina)
# -- SCHUBERT
# WON SPECIAL ~ SENATE: 
# -- CREIGHTON
# -- GARCIA
# -- KOLKHORST
# -- MENENDEZ (via H)
# -- PERRY (via H)
# IN HOUSE: 
# -- STRAUS (joe)
# DROP:
# -- kolkhorst, lois w. FROM HOUSE -- won senate special pre-swearing in
# -- villareal, mike -- Elected but never sworn in

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# IN HOUSE: 
# -- STRAUS (joe)


# filter(klarner, grepl('morten', cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(klarner, ddez == 46) %>% select(cand, year, sen, etype, outcome, ddez, candid)


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

### Manual Fixes
# ### Won in 2018 special -- will fix itself with updated klarner_data
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_id <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_name <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$sponsor <- "owlett, clint"


########## ****Still missing***** 
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[8]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, missing, name_sub, still_missing)

name_matches <- data.frame(LES_name = "ellis", k_name = 'ellis, rodney')
name_matches <- add_row(name_matches, LES_name = 'hernandez', k_name = 'hernandez, ana e.')
name_matches <- add_row(name_matches, LES_name = 'garcia', k_name = 'garcia, sylvia r.') # Elected in special early in 4-year term
name_matches <- add_row(name_matches, LES_name = 'perry', k_name = 'perry, charles')
#name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', k_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}

##### Matches requiring greater precision
# LES[LES$sponsor %in% "keith-agaran" & LES$term %in% "2009_2010",]$klarner_id <- 297229
# LES[LES$sponsor %in% "keith-agaran" & LES$term %in% "2009_2010",]$klarner_name <- "keithagaran, gil s. (coloma)"
# LES[LES$sponsor %in% "keith-agaran" & LES$term %in% "2009_2010",]$sponsor <- "keithagaran, gil s. (coloma)"

rm(name_matches, i)

############## Check for Duplicates
### ---> If two people with same last name and one won in a special, will lead to duplicates
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
rm(check_dup, k_sub, exact, missing, name_sub, name, t)


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
## --  ROMAN = Bill Roman = 1-term, 1997-1998, elected in special, not in Klarner
## --  ISETT, CHERI = Briefly filled husbands seat during absence -- never elected
## --  O'DAY = Mike O'DAY = 1-term, 2007-2008, elected in special, not in Klarner

LES[LES$sponsor == "roman",]$party <- 'r'
LES[LES$sponsor == "roman",]$district <- 14
LES[LES$sponsor == "roman",]$exper <- 'none'
LES[LES$sponsor == "roman",]$sponsor <- 'roman, william b.'

LES[LES$sponsor == "noriega, melissa",]$party <- 'd'
LES[LES$sponsor == "noriega, melissa",]$district <- 145
LES[LES$sponsor == "noriega, melissa",]$exper <- 'none'
LES[LES$sponsor == "noriega, melissa",]$sponsor <- "noriega, melissa"

LES[LES$sponsor == "isett, cheri",]$party <- 'r'
LES[LES$sponsor == "isett, cheri",]$district <- 84
LES[LES$sponsor == "isett, cheri",]$exper <- 'none'
LES[LES$sponsor == "isett, cheri",]$sponsor <- 'isett, cheri nannette'

LES[LES$sponsor == "o'day",]$party <- 'r'
LES[LES$sponsor == "o'day",]$district <- 29
LES[LES$sponsor == "o'day",]$exper <- 'none'
LES[LES$sponsor == "o'day",]$sponsor <- "o'day, michael"

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
  filter(dup == TRUE)

### Set Committees to NA for Years without Data -- May have matched candids in year range
set_NA <- colnames(hf_data)
set_NA <- set_NA[!(set_NA %in% c("CandId", 'chamber', "year", "term")) ]
LES[LES$term == '1989_1990', set_NA] <- NA
LES[LES$term == '1991_1992', set_NA] <- NA
LES[LES$term == '2017_2018', set_NA] <- NA

rm(hf_data, set_NA, i)

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
  filter(grepl("----", name_matches)) %>%
  as.data.frame()

#### FIX MISMATCHES
# -- Johnson, J = Jerry K. Johnson (1989-1996); 
LES[LES$sponsor %in% c('johnson, jarvis d.', 'munoz, sergio'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1989_1990', '1991_1992', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('anderson', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'anchia, rafael', SM_name = 'Anchía, Rafael')
name_matches <- add_row(name_matches, LES_name = 'campbell, ben', SM_name = 'Campbell')
# name_matches <- add_row(name_matches, LES_name = "carriker, steven (steve)", SM_name = "zzzz")
name_matches <- add_row(name_matches, LES_name = 'chavez, norma', SM_name = 'Chávez, Norma')
name_matches <- add_row(name_matches, LES_name = 'cook, john', SM_name = 'Cook')
name_matches <- add_row(name_matches, LES_name = 'flores, yolanda navarro', SM_name = 'Flores')
name_matches <- add_row(name_matches, LES_name = 'guillen, ryan', SM_name = 'Guillén, Ryan')
# name_matches <- add_row(name_matches, LES_name = "haley, j. w. (bill)", SM_name = "zzzz")
name_matches <- add_row(name_matches, LES_name = 'harris, jack', SM_name = 'Harris')
name_matches <- add_row(name_matches, LES_name = 'harris, o. h. (ike)', SM_name = 'Harris')
name_matches <- add_row(name_matches, LES_name = 'hernandez, christine', SM_name = 'Hernandez')
# name_matches <- add_row(name_matches, LES_name = "isett, cheri nannette", SM_name = "zzzz")
name_matches <- add_row(name_matches, LES_name = 'james, mary denny', SM_name = 'Denny, Mary')
name_matches <- add_row(name_matches, LES_name = 'keffer, bill', SM_name = 'Keffer, William')
name_matches <- add_row(name_matches, LES_name = 'lucio, eddie jr.', SM_name = 'Lucio, Eddie Jr.')
name_matches <- add_row(name_matches, LES_name = 'lucio, eddie iii', SM_name = 'Lucio, Eduardo III')
name_matches <- add_row(name_matches, LES_name = 'luna, gregory', SM_name = 'Luna')
name_matches <- add_row(name_matches, LES_name = 'marquez, marisa', SM_name = 'Márquez, Marisa Marquez')
name_matches <- add_row(name_matches, LES_name = 'menendez, jose', SM_name = 'Menéndez, José')
name_matches <- add_row(name_matches, LES_name = 'miller, rick', SM_name = 'Miller, DF') # Dana Fontaine "Rick" Miller
# name_matches <- add_row(name_matches, LES_name = "minjarez, ina", SM_name = "zzzzzzzzzz")
name_matches <- add_row(name_matches, LES_name = "munoz, sergio", SM_name = "Munoz")
name_matches <- add_row(name_matches, LES_name = 'nevarez, poncho', SM_name = 'Nevárez, Poncho')
name_matches <- add_row(name_matches, LES_name = 'nixon, drew', SM_name = 'Nixon')
# name_matches <- add_row(name_matches, LES_name = "noriega, melissa", SM_name = "zzzzzzzzzz") # presumably collapsed into rick noriega given fill-in
name_matches <- add_row(name_matches, LES_name = "noriega, richard", SM_name = "Noriega, Rick")
# name_matches <- add_row(name_matches, LES_name = "parker, carl", SM_name = "zzzz")
name_matches <- add_row(name_matches, LES_name = 'pena, aaron', SM_name = 'Peña, Aaron')
name_matches <- add_row(name_matches, LES_name = 'pierson, paula hightower', SM_name = 'Hightower-Pierson, Paula')
name_matches <- add_row(name_matches, LES_name = 'price, al', SM_name = 'Price')
name_matches <- add_row(name_matches, LES_name = 'price, four', SM_name = 'Price, Walter IV')
name_matches <- add_row(name_matches, LES_name = 'rodriguez, ciro d.', SM_name = 'Rodriguez')
name_matches <- add_row(name_matches, LES_name = 'rodriguez, eddie', SM_name = 'Rodríguez, Eddie')
name_matches <- add_row(name_matches, LES_name = 'romero, ramon jr.', SM_name = 'Romero Jr, Ramon')
name_matches <- add_row(name_matches, LES_name = 'solis, jim', SM_name = 'Solís, Jim')
name_matches <- add_row(name_matches, LES_name = 'taylor, van', SM_name = 'Taylor, Nicholas') # Nicholas Van Taylor
name_matches <- add_row(name_matches, LES_name = 'torres, gerard', SM_name = 'Torres')
name_matches <- add_row(name_matches, LES_name = 'toureilles, yvonne gonzalez', SM_name = 'Gonzales Toureilles')
name_matches <- add_row(name_matches, LES_name = 'turner, jim', SM_name = 'Turner')
name_matches <- add_row(name_matches, LES_name = 'ybarra, tara rios', SM_name = 'Rios')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}


#### Manual Edits (Needs more precision...)

### Two Edmund Kuempels in SM Data -- Second appears to be John Kuempel (perhaps Edmund jr? -- either way, terms match up)
LES[LES$sponsor == 'kuempel, edmund',]$SM_name <- ideo[ideo$name == 'Kuempel, Edmund' & ideo$house1993 %in% 1,]$name
LES[LES$sponsor == 'kuempel, edmund',]$SM_party <- ideo[ideo$name == 'Kuempel, Edmund' & ideo$house1993 %in% 1,]$party
LES[LES$sponsor == 'kuempel, edmund',]$np_score <- ideo[ideo$name == 'Kuempel, Edmund' & ideo$house1993 %in% 1,]$np_score

LES[LES$sponsor == 'kuempel, john',]$SM_name <- ideo[ideo$name == 'Kuempel, Edmund' & ideo$house2012 %in% 1,]$name
LES[LES$sponsor == 'kuempel, john',]$SM_party <- ideo[ideo$name == 'Kuempel, Edmund' & ideo$house2012 %in% 1,]$party
LES[LES$sponsor == 'kuempel, john',]$np_score <- ideo[ideo$name == 'Kuempel, Edmund' & ideo$house2012 %in% 1,]$np_score

### Jerry Patterson (R)
LES[LES$sponsor == 'patterson, jerry',]$SM_name <- ideo[ideo$name == 'Patterson' & ideo$senate1995 %in% 1,]$name
LES[LES$sponsor == 'patterson, jerry',]$SM_party <- ideo[ideo$name == 'Patterson' & ideo$senate1995 %in% 1,]$party
LES[LES$sponsor == 'patterson, jerry',]$np_score <- ideo[ideo$name == 'Patterson' & ideo$senate1995 %in% 1,]$np_score

## Lyndon Pete Patterson (D)
LES[LES$sponsor == 'patterson, l. (pete)',]$SM_name <- ideo[ideo$name == 'Patterson' & ideo$house1993 %in% 1,]$name
LES[LES$sponsor == 'patterson, l. (pete)',]$SM_party <- ideo[ideo$name == 'Patterson' & ideo$house1993 %in% 1,]$party
LES[LES$sponsor == 'patterson, l. (pete)',]$np_score <- ideo[ideo$name == 'Patterson' & ideo$house1993 %in% 1,]$np_score

### Michael L Galloway -- Senate
LES[LES$sponsor == 'galloway, michael l.',]$SM_name <- ideo[ideo$name == 'Galloway' & ideo$senate1995 %in% 1,]$name
LES[LES$sponsor == 'galloway, michael l.',]$SM_party <- ideo[ideo$name == 'Galloway' & ideo$senate1995 %in% 1,]$party
LES[LES$sponsor == 'galloway, michael l.',]$np_score <- ideo[ideo$name == 'Galloway' & ideo$senate1995 %in% 1,]$np_score

### Carolyn Galloway -- House
LES[LES$sponsor == 'galloway, carolyn',]$SM_name <- ideo[ideo$name == 'Galloway' & ideo$house1997 %in% 1,]$name
LES[LES$sponsor == 'galloway, carolyn',]$SM_party <- ideo[ideo$name == 'Galloway' & ideo$house1997 %in% 1,]$party
LES[LES$sponsor == 'galloway, carolyn',]$np_score <- ideo[ideo$name == 'Galloway' & ideo$house1997 %in% 1,]$np_score


########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########
### Check Party Mismatches
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% arrange(sponsor, term) %>% as.data.frame()
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% arrange(sponsor, term) %>% as.data.frame()
# filter(ideo, grepl("england", tolower(name)))

# *** Warren Chisum
LES[LES$sponsor == 'chisum, warren' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Chisum' & ideo$party == 'D',]$name
LES[LES$sponsor == 'chisum, warren' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Chisum' & ideo$party == 'D',]$party
LES[LES$sponsor == 'chisum, warren' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Chisum' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'chisum, warren' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Chisum, Warren' & ideo$party == 'R',]$name
LES[LES$sponsor == 'chisum, warren' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Chisum, Warren' & ideo$party == 'R',]$party
LES[LES$sponsor == 'chisum, warren' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Chisum, Warren' & ideo$party == 'R',]$np_score

# ## Billy Clemons -- Switched to R in Final Term, Sep. 1995, not clear what to prefer but might as well code as R for that term then
LES[LES$sponsor == 'clemons, billy' & LES$term != '1995_1996',]$SM_name <- ideo[ideo$name == 'Clemons' & ideo$party == "D",]$name
LES[LES$sponsor == 'clemons, billy' & LES$term != '1995_1996',]$SM_party <- ideo[ideo$name == 'Clemons' & ideo$party == "D",]$party
LES[LES$sponsor == 'clemons, billy' & LES$term != '1995_1996',]$np_score <- ideo[ideo$name == 'Clemons' & ideo$party == "D",]$np_score
LES[LES$sponsor == 'clemons, billy' & LES$term == '1995_1996',]$SM_name <- ideo[ideo$name == 'Clemons' & ideo$party == "R",]$name
LES[LES$sponsor == 'clemons, billy' & LES$term == '1995_1996',]$SM_party <- ideo[ideo$name == 'Clemons' & ideo$party == "R",]$party
LES[LES$sponsor == 'clemons, billy' & LES$term == '1995_1996',]$np_score <- ideo[ideo$name == 'Clemons' & ideo$party == "R",]$np_score

# *** Chuck Hopson
LES[LES$sponsor == 'hopson, chuck' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Hopson, Chuck' & ideo$party == 'D',]$name
LES[LES$sponsor == 'hopson, chuck' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Hopson, Chuck' & ideo$party == 'D',]$party
LES[LES$sponsor == 'hopson, chuck' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Hopson, Chuck' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'hopson, chuck' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Hopson, Chuck' & ideo$party == 'R',]$name
LES[LES$sponsor == 'hopson, chuck' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Hopson, Chuck' & ideo$party == 'R',]$party
LES[LES$sponsor == 'hopson, chuck' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Hopson, Chuck' & ideo$party == 'R',]$np_score

# *** Todd Hunter
LES[LES$sponsor == 'hunter, todd a.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Hunter, T' & ideo$party == 'D',]$name
LES[LES$sponsor == 'hunter, todd a.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Hunter, T' & ideo$party == 'D',]$party
LES[LES$sponsor == 'hunter, todd a.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Hunter, T' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'hunter, todd a.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Hunter, Todd' & ideo$party == 'R',]$name
LES[LES$sponsor == 'hunter, todd a.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Hunter, Todd' & ideo$party == 'R',]$party
LES[LES$sponsor == 'hunter, todd a.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Hunter, Todd' & ideo$party == 'R',]$np_score

# *** J. M. 'Jose' Lozano
LES[LES$sponsor == 'lozano, j. m.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Lozano, Jose' & ideo$party == 'D',]$name
LES[LES$sponsor == 'lozano, j. m.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Lozano, Jose' & ideo$party == 'D',]$party
LES[LES$sponsor == 'lozano, j. m.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Lozano, Jose' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'lozano, j. m.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Lozano, Jose' & ideo$party == 'R',]$name
LES[LES$sponsor == 'lozano, j. m.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Lozano, Jose' & ideo$party == 'R',]$party
LES[LES$sponsor == 'lozano, j. m.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Lozano, Jose' & ideo$party == 'R',]$np_score

# *** Aaron Peña
# Switched D to R on 12/14/2010 -- https://lrl.texas.gov/mobile/memberDisplay.cfm?memberID=5554
LES[LES$sponsor == "pena, aaron" & LES$term == "2011_2012",]$party <- 'r'
LES[LES$sponsor == 'pena, aaron' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Peña' & ideo$party == 'D',]$name
LES[LES$sponsor == 'pena, aaron' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Peña' & ideo$party == 'D',]$party
LES[LES$sponsor == 'pena, aaron' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Peña' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'pena, aaron' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Peña, Aaron' & ideo$party == 'R',]$name
LES[LES$sponsor == 'pena, aaron' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Peña, Aaron' & ideo$party == 'R',]$party
LES[LES$sponsor == 'pena, aaron' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Peña, Aaron' & ideo$party == 'R',]$np_score

# *** Allan Ritter
LES[LES$sponsor == 'ritter, allan b.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Ritter, Allan' & ideo$party == 'D',]$name
LES[LES$sponsor == 'ritter, allan b.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Ritter, Allan' & ideo$party == 'D',]$party
LES[LES$sponsor == 'ritter, allan b.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Ritter, Allan' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'ritter, allan b.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Ritter, Allan' & ideo$party == 'R',]$name
LES[LES$sponsor == 'ritter, allan b.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Ritter, Allan' & ideo$party == 'R',]$party
LES[LES$sponsor == 'ritter, allan b.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Ritter, Allan' & ideo$party == 'R',]$np_score

## Richard Williamson -- Switched D to R on 12.6.1993 --> R for 2nd half of 1993_1994 term = keeping as D
LES[LES$sponsor == 'williamson, richard f.' & !(LES$term %in% c('1995_1996', '1997_1998')),]$SM_name <- ideo[ideo$name == 'Williamson' & ideo$party == "D",]$name
LES[LES$sponsor == 'williamson, richard f.' & !(LES$term %in% c('1995_1996', '1997_1998')),]$SM_party <- ideo[ideo$name == 'Williamson' & ideo$party == "D",]$party
LES[LES$sponsor == 'williamson, richard f.' & !(LES$term %in% c('1995_1996', '1997_1998')),]$np_score <- ideo[ideo$name == 'Williamson' & ideo$party == "D",]$np_score
LES[LES$sponsor == 'williamson, richard f.' & LES$term %in% c('1995_1996', '1997_1998'),]$SM_name <- ideo[ideo$name == 'Williamson' & ideo$party == "R",]$name
LES[LES$sponsor == 'williamson, richard f.' & LES$term %in% c('1995_1996', '1997_1998'),]$SM_party <- ideo[ideo$name == 'Williamson' & ideo$party == "R",]$party
LES[LES$sponsor == 'williamson, richard f.' & LES$term %in% c('1995_1996', '1997_1998'),]$np_score <- ideo[ideo$name == 'Williamson' & ideo$party == "R",]$np_score

# *** Kirk England
LES[LES$sponsor == 'england, kirk' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'England, Kirk' & ideo$party == 'D',]$name
LES[LES$sponsor == 'england, kirk' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'England, Kirk' & ideo$party == 'D',]$party
LES[LES$sponsor == 'england, kirk' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'England, Kirk' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'england, kirk' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'England, Kirk' & ideo$party == 'R',]$name
LES[LES$sponsor == 'england, kirk' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'England, Kirk' & ideo$party == 'R',]$party
LES[LES$sponsor == 'england, kirk' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'England, Kirk' & ideo$party == 'R',]$np_score


rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last, in_chamber_terms)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

##### Drop Nicknames
# filter(LES, grepl('\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct() %>% arrange(sponsor)
LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

#### Manual Fixes
# mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% arrange(sponsor) %>% as.data.frame()
LES[LES$sponsor == 'armbrister, k.',]$sponsor <- 'armbrister, kenneth l.'
LES[LES$sponsor == 'creighton, c. brandon',]$sponsor <- 'creighton, charles brandon'
LES[LES$sponsor == 'lozano, j. m.',]$sponsor <- 'lozano, jose m.'
LES[LES$sponsor == 'price, four',]$sponsor <- 'price, walter thomas iv'
LES[LES$sponsor == 'sheffield, j. d.',]$sponsor <- 'sheffield, jesse d.'
LES[LES$sponsor == 'taylor, van',]$sponsor <- 'taylor, nicholas van' # Van seems to be middle name (that he goes by)
LES[LES$sponsor == 'wentworth, jeffrey',]$sponsor <- 'wentworth, earl jeffrey'
LES[LES$sponsor == 'west, g. e.',]$sponsor <- 'west, george e.'
LES[LES$sponsor == 'delco, mrs. wilhemina',]$sponsor <- 'delco, wilhemina'
LES[LES$sponsor == "green, r. e.",]$sponsor <- 'green, raymond eugene'

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1989 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1989:2002) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2003:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1989 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1989:1996) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


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
ggplot2::ggplot(LES, aes(x = np_score, y = LES, color = party)) + 
  geom_point() + 
  geom_smooth(formula = "y ~ x", method = 'lm', se = FALSE) + 
  facet_wrap(~ term) + 
  scale_color_manual(values=c("dodgerblue2", "red2", "gray50"))

##### CHECK OUTLIERS
# filter(LES, party == 'd' & np_score > .5) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & np_score < -.25) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')
# stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

