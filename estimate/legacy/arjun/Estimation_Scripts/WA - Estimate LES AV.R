################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** WASHINGTON *** BY SESSION
##############################################################


###################################
## (SPECIAL) SESSIONS:
## ----> Bill ID's are unique for two-year term
## ----> Bills rollover from regular to special sessions via resolution/are reintroduced (e.g., see: https://app.leg.wa.gov/billsummary?BillNumber=1500&Initiative=false&Year=2011)
## MEMBER LISTS:
## ---- http://leg.wa.gov/History/Legislative/Documents/MembersOfLeg2018.pdf
## PROCESS/RULES:
## ---- PROCESS: http://leg.wa.gov/legislature/Pages/Overview.aspx
## ---- HOUSE RULES: http://leg.wa.gov/House/Pages/HouseRules.aspx
## Sponsorship/Authorship
## ---- 
###########################
### NOTES:
## (1) "Executive session scheduled, but no action was taken in the House Committeee..." ---> AIC? Currently yes. Also records IF action was taken..
## -------> Doesn't show up for every bill so seems like this means its on session agenda: https://www.washington.edu/opb/state-operations/legislative-process-terms/
#########################


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

this_state <- 'WA'
keep_types <- c('Bill')

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

# data_rows_new = data.frame(sapply(bill_files, function(x) read.csv(x) %>% nrow())) %>% 
#   rownames_to_column("year")
# data_rows_old = data.frame(sapply(gsub("States/WA","States/WA/Old WA versions", bill_files), function(x) read.csv(x) %>% nrow())) %>% 
#   rownames_to_column("year") %>% 
#   mutate(year = gsub("Old WA versions/","",year))
# 
# View(full_join(data_rows_old,data_rows_new))
# 
# change_years = full_join(data_rows_old,data_rows_new) %>% 
#   mutate(year = gsub('../../../State Legislative Data/States/WA/WA_Bill_Details_','',year),
#          year = gsub('.csv','',year))
# names(change_years) = c("year","old count", "new count")
# 
# comp_1718 = anti_join(read.csv(bill_files[12]),
#                       read.csv(gsub("States/WA","States/WA/Old WA versions", bill_files[12])),
#                       by="bill_number")


### Terms/Sessions and Filespaths
terms <- 2021
t = terms
t_plus_one = t + 1
t_yrs <- as.numeric(terms)
t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 1}'))
t_sessions = sessions[startsWith(sessions,as.character(t)) | startsWith(sessions,as.character(t+1))]


#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv"))
commem_bills$bill_id = gsub("2E","",commem_bills$bill_id)
commem_bills$bill_id = gsub("E","",commem_bills$bill_id)


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
# t <- terms[2]

### Skip Previously Estimated
if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
  print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
  next
}

###### SESSION IN PROGRESS
print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_yrs}.csv")
bills <- read.csv(bill_path)
bills <- arrange(bills, bill_number)

### Clean Term/Session Variables
bills$term <- t_yrs
bills$session <- t_yrs

### Drop duplicates
bills <- distinct(bills)

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_number)
bills$companion = ifelse(startsWith(bills$companion,"E"),substr(bills$companion,2,nchar(bills$companion)),
                         bills$companion)
bills$bill_id = gsub("2E","",bills$bill_id)
bills$bill_id = gsub("E","",bills$bill_id)

############### Drop Resolutions, Messages, Communications, Reports
# bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
table(bills$bill_type)
all_bills <- bills
bills <- filter(bills, bill_type %in% keep_types) 

##########################
####### Standardize Sponsors

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

### LES Sponsor Var
bills$LES_sponsor <- str_trim(gsub('\\"[^"]+\\"', '', bills$primary_sponsor))
bills$cosponsors <- str_trim(gsub('\\"[^"]+\\"', '', bills$cosponsors))
table(bills$LES_sponsor)

#### Adjusting for Bills By Request -- Need to do BEFORE splitting
if(any(grepl("request", bills$LES_sponsor))){
  print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
}



###################
###### Merge in S&S Bills
###################
# *** For WASHINGTON: Regular and Special Session Records collapsed into One Long Biennium; Bills Carryover + Numbers Unique


SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS,Title), bills,
                              by = c("bill_id", "term")) %>% 
  anti_join(bills, by =c("bill_id"="companion","term")) %>% 
  left_join(all_bills %>% select(bill_id,term,primary_sponsor), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% c("HB","SB")) %>% select(-bill_type) %>%
  filter(! grepl("committee",primary_sponsor, ignore.case=T)) %>%
  arrange(primary_sponsor) 
unique(missing_SS_bills$bill_id) 



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
                   filter(bill_type %in% c("HB","SB" ) ))

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

###################################################
############### Code Commemorative
###################################################

bills <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session'))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)

###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "" | is.na(LES_sponsor)) %>% View()
if(nrow(filter(bills, LES_sponsor == '')) > 0 | any(is.na(bills$LES_sponsor))){
  cat('\n')
  cat(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == '')) + sum(is.na(bills$LES_sponsor))} bill(s) without a sponsor"))
  bills <- filter(bills, !(LES_sponsor == '' | is.na(LES_sponsor)))     
}

###################################################
############### Code Bill History
###################################################

bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_yrs}.csv")
bill_hist <- read.csv(bill_hist_path)

######## Standardize BillHist Bill IDs
bill_hist <- rename(bill_hist, bill_id = bill_number) 

### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
bill_hist$term <- t_yrs
bill_hist$session <- t_yrs
bill_hist$bill_id = gsub("2E","",bill_hist$bill_id)
bill_hist$bill_id = gsub("E","",bill_hist$bill_id)

### Order by Order
bill_hist <- arrange(bill_hist, term, bill_id, order)

### Re-Coding Chamber Variable
bill_hist$chamber <- ifelse(bill_hist$chamber == 'O', 'G', bill_hist$chamber)
bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate", "G" = "Governor", "CC" = "Conference")
bill_hist = bill_hist %>% 
  # E means engrossing chamber, that's the chamber of the bill number
  mutate(chamber = case_when(
    chamber == "E" & grepl("HB",bill_id) ~ "House", 
    chamber == "E" & grepl("SB",bill_id) ~ "Senate",
    T ~ chamber
  ))

### Identifying Committees
bill_hist$action <- ifelse(grepl('^[A-Z][A-Z]+ - ', bill_hist$action), paste0('Committee_', bill_hist$action), bill_hist$action)

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
## ****  Actions become a bit more detailed from 2003 on -- specifically committee actions ***

aic_t <- c('committee_', '^minority', 'do pass', 'do not pass', 'without recommendation', 'do confirm', 'committee amendment',
           'public hearing', 'executive session', 'executive action taken')
## --> Added comittee_ prefix to make committee action clear
## --> Committee_ABC - Majority == Committee Report (signed by majority); if a minority report, included on next line
abc_t <- c('committee_.+majority', '^passed to rules committee', 'referred to rules 2 review',
           'second reading', 'third reading', 
           'committee amend.+adopted', '^amended\\.')
## --> If a majority report OR if to rules = Reported out; need both because first will also catch bills that then get referred to a new comm
## -- Commitee Amend.+Adopted will catch those that were and were not adopted
pc_t <- c('third reading, passed')
law_t <- c('governor signed', 'governor partially vetoed', '^chapter [0-9]+', '199[0-9] laws', '20[0-9][0-9] laws')

### Check Actions
# filter(bill_hist, grepl('^third reading', tolower(action))) %>% distinct(action) %>% View()
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

### Make Sure No Excess text in Bill Action
bill_hist$action <- str_trim(bill_hist$action)

### Code History Stages for Each Bill --- Subset Hist File, Code, Save, Repeat
# Ignoring chamber switch for a handful of bills with incorrect chambers early on -- Doesn't dramatically change coding
options(warn = 2)
for(i in 1:nrow(bills)){
  b_id = bills[i,]$bill_id
  s_id = bills[i,]$session
  b_spon = bills[i,]$LES_sponsor
  hist_sub <- filter(bill_hist, bill_id == b_id, term == t_yrs, session == s_id)
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t,
                                    ignore_chamber_switch = TRUE) 
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
# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 1,]$bill_id) %>% View()


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
# table(all_bill_stages$SS, all_bill_stages$commem)
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
unique_cospon <- str_trim(unique(unlist(str_split(bills$cosponsors, '; '))))
for(nonspon in unique_cospon){
  if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != ''){
    chamb <- unique(substring(bills[grepl(nonspon, bills$cosponsors),]$bill_id, 1, 1))
    if("H" %in% chamb & "S" %in% chamb){
      print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
    }else{
      all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0) 
    }
  }
}

######## Cosponsorship Info --- For OH: Only have cosponsor info for most recent years
all_sponsors$num_cosponsored_bills <- NA
bills$cospon_match <- paste(bills$LES_sponsor, bills$primary_sponsor, bills$cosponsors, sep = '; ')
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

#######################
#### CLEAN NAMES
all_sponsors$last_name <- gsub(',.+', '', all_sponsors$LES_sponsor)
all_sponsors$first_name <- gsub('.+, ', '', all_sponsors$LES_sponsor)
all_sponsors$first_name <- gsub('\\..+| .+', '', all_sponsors$first_name)

all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update Last Names for Matching 
if(t_yrs %in% c("1993_1994")){
  all_sponsors[all_sponsors$LES_sponsor == 'kohl, jeanne',]$last_name <-  "kohlwelles"
}
if(t_yrs %in% c('2011_2012', '2013_2014')){
  all_sponsors[all_sponsors$LES_sponsor == 'holmquist newbry, janea',]$last_name <-  "holmquist"
}
if(t_yrs %in% c("2017_2018")){
  all_sponsors[all_sponsors$LES_sponsor == 'mosbrucker, gina',]$last_name <-  "mccabe"
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
} %>% select(-c(votesmart_id,knowwho_pid, ballotpedia,nickname,suffix)) %>%  distinct() 


if(t_yrs == "2019_2020") {
  legiscan = bind_rows(legiscan,
                       legiscan %>% filter(people_id == 10541) %>% mutate(role = "Rep", district = "HD-001"),
                       legiscan %>% filter(people_id == 16132) %>% mutate(role = "Rep", district = "HD-038")) 

}

if(t_yrs == "2021_2022"){
  legiscan = legiscan %>% 
    mutate(district = ifelse(people_id == 20686, "HD-008", district)) %>% 
    bind_rows(legiscan %>% filter(people_id == 18283) %>% mutate(role = "Sen", district = "SD-044"))
}

legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name, role) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = glue("{last_name}, {first_name}")) %>%
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
        "graham, virginia-h" = "graham, jenny-h",
        "santos, sharon-h" = "santos, sharon tomiko-h"
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
        "wilcox, james-h" = NA_character_,
        "santos, sharon-h" = "santos, sharon tomiko-h",
        "jinkins, laurie-h" = NA_character_
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

rm(all_sponsors, bills, legis_data, SS_bills, H_elec_year, S_elec_year, i, keep_types)
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES, commem_bills) # 
rm(t, terms, klarner_gs, m_sub, match_name2, nonspon, unique_cospon, c_sub) # 


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by APPOINTMENT BY BOARD OF COUNTY COMMISSIONERS --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
### --> BUT will often have 'special' elections at the next general to fill Senate seats
########################################################################################################################
### FULL ROSTER: http://leg.wa.gov/History/Legislative/Documents/MembersOfLeg2018.pdf
#########################

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1991_1992 TERM! ~~~~~~~~~~~~~~ 
#     session chamber    N AIC ABC PASS LAW
# 1 1991_1992       H 1194 632 637  471 228
# 2 1991_1992       S  992 463 467  307 165
### TEMP IN SENATE:
# -- KREIDLER (lela) -- Last name duplicated, won't show -- filled in for husband for 4 months while on military leave
### IN HOUSE:
# -- KING (joe, speaker)
### DROP:
# -- dejarnatt, arlie u. -- died august 1990

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1993_1994 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 1993_1994       H 1107 623 623  463 305
# 2 1993_1994       S  953 488 493  318 214
### APPOINTED ~ HOUSE:
# -- CONWAY
# -- PATTERSON
### APPOINTED ~ SENATE:
# -- FRANKLIN (rosa, via H)
# -- LUDWIG (curtis, via H) -- http://leg.wa.gov/History/Senate/ClassPhotos/Documents/PDF/1993a.pdf
# -- SCHOW (ray, appointed 1/7/1994)
### IN HOUSE:
# -- EBERSOLE
# -- COOKE
### DROP:
# -- hine, lorraine --- resigned 1/13/1993 - http://web.leg.wa.gov/WomenInTheLegislature/Members/MemberBios/HineLA_1981.pdf
# -- hansen, frank (tub) -- died Dec. 29th 1991 - http://community.seattletimes.nwsource.com/archive/?date=19911230&slug=1325763


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1996 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 1995_1996       H 1158 590 594  469 249
# 2 1995_1996       S 1115 559 564  426 231
### APPOINTED ~ HOUSE:
# -- MURRAY (ed); SCHEUERMAN (carl); STERK (mark)
# -- SOMMERS (duane); appointed to fill seat, last name duplicated, won't print
### APPOINTED ~ SENATE:
# -- GOINGS; PALMER (hal); SWECKER; THIBAUDEAU (pat, via H); ZARELLI
### IN HOUSE:
# -- BALLARD
### DROP: 
# -- smith, linda -- resigned january 2, 1995 -- elected to Congress
# -- amondson, neil 1 -- resigned 1/3/1995


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 1997_1998       H 1288 720 726  558 280
# 2 1997_1998       S 1117 586 588  417 233
### APPOINTED ~ HOUSE:
# -- BLALOCK; EICKMEYER; KENNEY; MCCUNE (james)
### APPOINTED ~ SENATE:
# -- JACOBSEN; KLINE; PATTERSON; SWANSON; THIBAUDEAU
### IN HOUSE:
# -- BALLARD; JACOBSEN 
# -- If not using cospon ---> TALCOTT; GARDNER; CHOPP
### DROP:
# -- patterson, julia -- never seated, resigned to take senate seat in January
# -- smith, adam -- resigned, elected to Congress 11/5/1996
# -- owen, brad -- resigned 1/15/1997 -- elected lt. gov.
# -- anderson, calvin -- died august 4, 1995
# -- rinehart, nita -- resigned 12/10/1996


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 1999_2000       H 1297 463 465  329 221
# 2 1999_2000       S 1111 591 592  408 194
### APPOINTED ~ HOUSE:
# -- COX
### APPOINTED ~ SENATE:
# -- SHEAHAN
### IN HOUSE:
# -- If not using cospon -----> BALLARD; LISK; CHOPP
### IN SENATE: 
# -- If not using cospon -----> MCDONALD (Dan)
### DROP:
# -- prince, eugene a. -- resigned 1/10/1999 -- appointed to chair state liquor board

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2001_2002       H 1268 525 528  333 238
# 2 2001_2002       S 1217 619 623  361 186
#### APPOINTED ~ HOUSE:
# -- BERKEY (jean)
# -- HOLMQUIST (jane'a)
# -- MARINE (joe)
#### IN HOUSE:
# -- If not using cospon ----->  BALLARD; CHOPP
### DROP:
# -- radcliff, renee -- resigned 1/10/2001
# -- scott, patricia -- died 1/7/2001
# -- heavey, michael -- resigned 08/21/2000 -- appointed to king county court


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2003_2004       H 1283 877 643  482 249
# 2 2003_2004       S 1087 800 620  378 206
### APPOINTED ~ HOUSE: 
# -- BLAKE
### APPOINTED ~ SENASTE:
# -- DOUMIT (via H)
### IN HOUSE:
# -- COHOPP
# -- If not using cospon -----> MCMORRIS; SEHLIN
### IN SENATE:
# -- If not using cospon ----->  POULSEN
### DROP: 
# -- doumit, mark l. IN HOUSE -- Appointed to senate before being seated
# -- snyder, sid -- resigned Nov 8, 2002

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N  AIC ABC PASS LAW
# 1 2005_2006       H 1372 1009 731  489 336
# 2 2005_2006       S 1142  850 678  317 215
### APPOINTED ~ HOUSE:
# -- TAKKO (dean)
### IN HOUSE:
# -- HATFIELD; CHOPP
# -- If not using cospon -----> SUMP; CHANDLER
### DROP:
# -- west, jim -- resigned 12/23/2003 to become mayor of spokane
# -- hale, patricia s. (pat) -- resigned 5/6/2004, appointed to US Small Business Admin.
# -- reardon, aaron -- resigned 12/31/2003, elected county exec.


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N  AIC ABC PASS LAW
# 1 2007_2008       H 1461 1048 754  499 319
# 2 2007_2008       S 1179  882 711  332 238
### APPOINTED ~ HOUSE:
# -- HERRERA; LIIAS; LOOMIS (liz); NELSON (sharon); SCHMICK (joe); SMITH (norma)
### APPOINTED/SPECIAL ~ SENATE:
# -- CLEMENTS (jim, appointed for year, someone else won special)
# -- HATFIELD (brian)
# -- KING (curtis)
### IN HOUSE:
# -- If not using cospon -----> CHOPP
### DROP:
# -- deccio, alex -- resigned 1/1/2007
# -- doumit, mark l. -- resigned 11/1/2006

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2009_2010       H 1455 974 715  481 326
# 2 2009_2010       S 1215 903 697  399 282
### APPOINTED ~ HOUSE: 
# -- COX (don, past H); FAGAN; NEALEY; TAYLOR (david); GORDON (randy)
# -- GRANT (laura) -- appointed to fill seat of William Grant; matches to him falsely bc he doesn't sponsor, but fixed.
### IN HOUSE:
# -- CHOPP
### DROP:
# -- hailey, stephen v. -- died 12/28/2008
# -- poulsen, erik -- resigned 10/1/2007

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2011_2012       H 1190 795 560  399 272
# 2 2011_2012       S 1025 749 570  285 203
### APPOINTED ~ HOUSE:
# -- HANSEN; POLLET; WYLIE
### APPOINTED ~ SENATE:
# -- BAXTER (jeff, appt. for year)
# -- PADDEN (won special in 11/2011 to take seat from baxter)
# -- ROLFES (christine, july 2011)
### IN HOUSE:
# -- CHOPP
# -- If not using cospon -----> DEBOLT
### IN SENATE:
# -- If not using cospon -----> HILL
### DROP:
# -- mccaslin, bob -- resigned 1/5/2011
# -- jarrett, fred -- resigned 12/18/2009

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2013_2014       H 1101 809 613  413 215
# 2 2013_2014       S  951 682 500  298 187
### APPOINTED ~ HOUSE:
#-- GREGERSON; MURI; ROBINSON (june); WALKINSHAW
### APPOINTED ~ SENATE:
# -- BROWN (sharon)
# -- SCHLICHTER (nathan)
# -- SMITH (john s., appointed, Dansel won special year later)
### IN HOUSE:
# -- CHOPP
# -- If not using cospon -----> CROUSE (larry, resigned 12/31/2013); DEBOLT; KRISTIANSEN
### DROP:
# -- morton, bob -- resigned 12/31/2012
# -- kilmer, derek c. -- resigned 12/10/2012, elected to Congress
# -- white, scott -- died 11/21/2011

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2015_2016       H 1291 898 661  354 186
# 2 2015_2016       S 1122 838 670  361 197
### APPOINTED ~ HOUSE:
# -- DYE (mary); FRAME (noel); GREGORY (carol); KUDERER (patty); ROSSETTI (jd)
### IN HOUSE:
# -- CHOPP
# -- If not using cospon -----> KRISTIANSEN
### DROP:
# -- freeman, roger -- died 10/29/2014
# -- carrell, mike -- died 5/29/2013

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
# 1 2017_2018       H 1301 901 649  413 257
# 2 2017_2018       S 1026 811 628  363 189
### APPOINTED ~ HOUSE:
# -- ESLICK; IRWIN; MAYCUMBER; SLATTER; VALDEZ
### APPOINTED/WON SPECIAL ~ SENATE:
# -- DHINGRA; FORTUNATO; KUDERER; ROSSI; SALDANA; SHORT
### IN HOUSE:
# -- CHOPP
# -- If not using cospon -----> WILCOX; KRISTIANSEN
## DROP:
# -- fortunato, phil -- IN HOUSE -- appointed to senate before taking house seat
# -- kohlwelles, jeanne -- resigned 12/31/2015, elected to King County COuncil
# -- kuderer, patty -- IN HOUSE -- appointed to senate 1/5/2017
# -- roach, pam -- resigned 1/3/2017 -- elected to Pierce County Council
# -- jayapal, pramila -- resigned 12/11/2016, elected to Congress
# -- hill, andy -- died 10/31/2016
# -- habib, cyrus -- resigned 1/4/2017, elected Lt. Gov.


# filter(klarner, grepl("chopp", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 33 & sen == 1 & year == 1990) %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)


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

###### NAME FIXES
LES[LES$sponsor == "holmquist, jane'a",]$sponsor <- "holmquist, janea"

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
# LES[LES$data_name %in% "kuhn",]$klarner_id <- NA
# LES[LES$data_name %in% "kuhn",]$klarner_name <- NA
# LES[LES$data_name %in% "kuhn",]$sponsor <- 'kuhn, john r.'

### ****Still missing***** ---> Rest are not in Klarner or 2017-2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[9]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl('eslick', cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, missing, name_sub, still_missing)


### Fix Missing
name_matches <- data.frame(LES_name = 'schmick, joe', k_name = 'schmick')
name_matches <- add_row(name_matches, LES_name = 'muri, dick', k_name = 'muri, richard (dick)')
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
rm(check_dup, k_sub, exact, name_sub, missing, t)


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
fill_missing <- data.frame(LES_name = "kreidler, lela", new_name = 'kreidler, lela', party = 'd', district = 22, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "scheuerman, carl", new_name = 'scheuerman, carl', party = 'd', district = 29, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "palmer, hal", new_name = 'palmer, hal', party = 'r', district = 18, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "blalock, john", new_name = 'blalock, john', party = 'd', district = 33, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "marine, joe", new_name = 'marine, joe', party = 'r', district = 21, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "grant, laura", new_name = 'grant-herriot, laura', party = 'd', district = 16, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "baxter, jeff", new_name = 'baxter, jeff', party = 'r', district = 4, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "smith, john", new_name = 'smith, john s.', party = 'r', district = 7, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "rossetti, jd", new_name = 'rossetti, j.d.', party = 'd', district = 19, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "slatter, vandana", new_name = 'slatter, vandana', party = 'd', district = 48, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

#### *** 2017_2018: If any of below run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "irwin, morgan",]$party <- 'r'
LES[LES$sponsor == "eslick, carolyn",]$party <- 'r'
LES[LES$sponsor == "maycumber, jacquelin",]$party <- 'r'
LES[LES$sponsor == "valdez, javier",]$party <- 'd'
LES[LES$sponsor == "saldana, rebecca",]$party <- 'd'
LES[LES$sponsor == "dhingra, manka",]$party <- 'r'

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t, fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

### Fix Mismatch -- Jr/Sr Collapsed to Same Klarner ID
LES[LES$sponsor == 'mccaslin, bob' & as.numeric(substring(LES$term, 1, 4)) < 2010,]$sponsor <- 'mccaslin, robert sr.'
LES[LES$sponsor == 'mccaslin, bob' & as.numeric(substring(LES$term, 1, 4)) >= 2015,]$sponsor <- 'mccaslin, robert jr.'

### Fix Names
LES[LES$sponsor == 'hargrove, jim 1',]$sponsor <- 'hargrove, james e.'
LES[LES$sponsor == 'hargrove, steve 2',]$sponsor <- 'hargrove, steve'
LES[LES$sponsor == 'sprenkle, arthur c. 1',]$sponsor <- 'sprenkle, arthur c.'
LES[LES$sponsor == 'peery, w. kim 1',]$sponsor <- 'peery, w. kim'
LES[LES$sponsor == 'nelson, richard (dick) 1',]$sponsor <- 'nelson, richard p.'
LES[LES$sponsor == 'mclean, alex m. 1',]$sponsor <- 'mclean, alex m.'
LES[LES$sponsor == 'grant, bill 1',]$sponsor <- 'grant, william a.'
LES[LES$sponsor == 'moyer, john a. 1',]$sponsor <- 'moyer, john a'
LES[LES$sponsor == 'bailey, cliff 1',]$sponsor <- 'bailey, cliff'
LES[LES$sponsor == 'amondson, neil 1',]$sponsor <- 'amondson, neil'
LES[LES$sponsor == 'holm, barbara j. 1',]$sponsor <- 'holm, barbara j.'
#### LES[LES$sponsor == 'zzzzzzz',]$sponsor <- 'zzzzzzz'
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()


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
ideo$match_name <- str_extract(tolower(ideo$name), '[a-z]+, [a-z]+')
ideo$match_name <- ifelse(is.na(ideo$match_name), tolower(ideo$name), ideo$match_name)
ideo$last_name <- gsub(',.+', '', tolower(ideo$name))

#### UNTIL SM DATA UPDATE -- DROPPING 2015_2016 Duplicates
# ** Good number of legislators that cross over into 2015-2016 are repeated on new rows...
# ** Also I think one of the Richard Debolts is actually Ed Orcutt -- Debolt is listed as D-18 (Ed's district) but he's D-20
ideo <- filter(ideo, !duplicated(paste(name, party)))

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
LES[LES$sponsor %in% c('johnson, stanley c. (stan)', 'mccaslin, robert jr.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches: John 'Rod' Blalock
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% arrange(sponsor) %>% distinct() %>% as.data.frame()

#### FIX MISMATCHES
# LES[LES$sponsor %in% c('zzzzzzzzzzzzzzzzz'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1991_1992', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('orc', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

### Most of these have 2015-2016 Duplicates but with slight name variation (e.g., Middle initial with period or no initial)
# ---> Always picking the one with most years
name_matches <- data.frame(LES_name = 'appleton, sherry', SM_name = 'Appleton, Sherry V')
name_matches <- add_row(name_matches, LES_name = 'clibborn, judy', SM_name = 'Clibborn, Judith R') 
name_matches <- add_row(name_matches, LES_name = 'cody, eileen l.', SM_name = 'Cody, Eileen L')
name_matches <- add_row(name_matches, LES_name = 'debolt, richard', SM_name = 'DeBolt, Richard C') 
name_matches <- add_row(name_matches, LES_name = 'dunshee, hans', SM_name = 'Dunshee, Hans M') 
name_matches <- add_row(name_matches, LES_name = 'elliot, ian', SM_name = 'Elliott, Ian')
name_matches <- add_row(name_matches, LES_name = 'fagan, susan', SM_name = 'Fagan, Susan K')
name_matches <- add_row(name_matches, LES_name = 'goodman, roger e.', SM_name = 'Goodman, Roger E')
name_matches <- add_row(name_matches, LES_name = 'gregerson, mia suling', SM_name = 'Gregerson, Mia Su-ling')
# name_matches <- add_row(name_matches, LES_name = 'hansen, drew', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'hansen, mick', SM_name = 'Hansen, Michael B')
name_matches <- add_row(name_matches, LES_name = 'hargrove, mark', SM_name = 'Hargrove, Mark D')
name_matches <- add_row(name_matches, LES_name = 'harris, paul', SM_name = 'Harris, Paul L')
name_matches <- add_row(name_matches, LES_name = 'herrera, jaime', SM_name = 'Herrera Beutler, Jaime')
name_matches <- add_row(name_matches, LES_name = 'hickel, tim', SM_name = 'Hickel, Timothy')
name_matches <- add_row(name_matches, LES_name = 'holmquist, janea', SM_name = 'Holmquist Newbry, Janéa')
name_matches <- add_row(name_matches, LES_name = 'kagi, ruth', SM_name = 'Kagi, Ruth Lecocq')
name_matches <- add_row(name_matches, LES_name = 'kenney, phyllis g.', SM_name = 'Kenney, Phyllis G')
name_matches <- add_row(name_matches, LES_name = 'kilduff, christine', SM_name = 'Killduff, Christine')
name_matches <- add_row(name_matches, LES_name = 'kohlwelles, jeanne', SM_name = 'Kohl-Welles, Jeanne E')
# name_matches <- add_row(name_matches, LES_name = 'mccaslin, robert jr.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'mcmorris, cathy', SM_name = 'McMorris Rodgers, Cathy')
name_matches <- add_row(name_matches, LES_name = 'nealey, terry r.', SM_name = 'Nealey, Terry')
# name_matches <- add_row(name_matches, LES_name = 'orcutt, ed', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'ormsby, timm s.', SM_name = 'Ormsby, Timm S')
# name_matches <- add_row(name_matches, LES_name = 'rasmussen, a. l.', SM_name = 'zzzzzzz') # A.L. 'Slim' Rasmussen -- NOT Marilyn
name_matches <- add_row(name_matches, LES_name = 'reykdal, chris', SM_name = 'Reykdal, Chris P')
name_matches <- add_row(name_matches, LES_name = 'rodne, jay r.', SM_name = 'Rodne, Jay R')
name_matches <- add_row(name_matches, LES_name = 'schmick', SM_name = 'Schmick, Joseph S')
name_matches <- add_row(name_matches, LES_name = 'short, shelly', SM_name = 'Short, Shelly A')
name_matches <- add_row(name_matches, LES_name = 'smith, norma', SM_name = 'Smith, Norma C')
name_matches <- add_row(name_matches, LES_name = 'springer, lawrence s.', SM_name = 'Springer, Lawrence S')
name_matches <- add_row(name_matches, LES_name = 'taylor, david', SM_name = 'Taylor, David V')
# name_matches <- add_row(name_matches, LES_name = 'wilson, lynda', SM_name = 'zzzzzzz') # NOT Sim
name_matches <- add_row(name_matches, LES_name = 'wylie, sharon', SM_name = 'Wylie, Sharon')
name_matches <- add_row(name_matches, LES_name = 'young, jesse', SM_name = 'Young, Jesse L.')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########

# *** Tom Campbell ran as D but switched to R in JANUARY 1995 -- see page 9: http://leg.wa.gov/History/Legislative/Documents/MembersOfLeg2018.pdf
LES[LES$sponsor == 'campbell, tom' & LES$term == '1995_1996',]$party <- 'r'
LES[LES$sponsor == 'campbell, tom' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'Campbell, Tom' & ideo$party == 'D',]$name
LES[LES$sponsor == 'campbell, tom' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Campbell, Tom' & ideo$party == 'D',]$party
LES[LES$sponsor == 'campbell, tom' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Campbell, Tom' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'campbell, tom' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Campbell, Tom' & ideo$party == 'R',]$name
LES[LES$sponsor == 'campbell, tom' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Campbell, Tom' & ideo$party == 'R',]$party
LES[LES$sponsor == 'campbell, tom' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Campbell, Tom' & ideo$party == 'R',]$np_score

# *** Dave Mastin ran as D but switched to R in JULY 1995 
LES[LES$sponsor == 'mastin, dave' & LES$term == '1995_1996',]$party <- 'r'
LES[LES$sponsor == 'mastin, dave' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'Mastin, Dave' & ideo$party == 'D',]$name
LES[LES$sponsor == 'mastin, dave' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Mastin, Dave' & ideo$party == 'D',]$party
LES[LES$sponsor == 'mastin, dave' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Mastin, Dave' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'mastin, dave' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Mastin, Dave' & ideo$party == 'R',]$name
LES[LES$sponsor == 'mastin, dave' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Mastin, Dave' & ideo$party == 'R',]$party
LES[LES$sponsor == 'mastin, dave' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Mastin, Dave' & ideo$party == 'R',]$np_score

# *** Paul Zellinksky -- Lost office as Dem, Ran term later as Republican
### ---> Pretty sure Shor/McCarty have his switch to Dem miscoded as Jr -- He was Sr and the switch was same guy - http://archive.kitsapsun.com/news/local/businessman-politician-zellinsky-dies-at-82-ep-1264000532-354493631.html/
LES[LES$sponsor == 'zellinsky, paul' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'Zellinsky Jr, Paul' & ideo$party == 'D',]$name
LES[LES$sponsor == 'zellinsky, paul' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Zellinsky Jr, Paul' & ideo$party == 'D',]$party
LES[LES$sponsor == 'zellinsky, paul' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Zellinsky Jr, Paul' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'zellinsky, paul' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Zellinsky' & ideo$party == 'R',]$name
LES[LES$sponsor == 'zellinsky, paul' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Zellinsky' & ideo$party == 'R',]$party
LES[LES$sponsor == 'zellinsky, paul' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Zellinsky' & ideo$party == 'R',]$np_score

# *** Fred Jarrett --- R in 2006 election, Switched to D in Dec 2007; Ran for Senate as D and stayed that way for year before resignation
LES[LES$sponsor == 'jarrett, fred' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'Jarrett, Fred' & ideo$party == 'D',]$name
LES[LES$sponsor == 'jarrett, fred' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Jarrett, Fred' & ideo$party == 'D',]$party
LES[LES$sponsor == 'jarrett, fred' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Jarrett, Fred' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'jarrett, fred' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Jarrett, Fred' & ideo$party == 'R',]$name
LES[LES$sponsor == 'jarrett, fred' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Jarrett, Fred' & ideo$party == 'R',]$party
LES[LES$sponsor == 'jarrett, fred' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Jarrett, Fred' & ideo$party == 'R',]$np_score

# *** Bill Finkbeiner -- Dem from 1993-1994 in House, Rep. from 1995 to 2006 in Senate
LES[LES$sponsor == 'finkbeiner, bill' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'Finkbeiner, Bill' & ideo$party == 'D',]$name
LES[LES$sponsor == 'finkbeiner, bill' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Finkbeiner, Bill' & ideo$party == 'D',]$party
LES[LES$sponsor == 'finkbeiner, bill' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Finkbeiner, Bill' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'finkbeiner, bill' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Finkbeiner, Bill' & ideo$party == 'R',]$name
LES[LES$sponsor == 'finkbeiner, bill' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Finkbeiner, Bill' & ideo$party == 'R',]$party
LES[LES$sponsor == 'finkbeiner, bill' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Finkbeiner, Bill' & ideo$party == 'R',]$np_score

# *** Rodney Tom -- Dem from 2003-2006 in House; Rep from 2007 to 2014 in Senate
LES[LES$sponsor == 'tom, rodney' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'Tom, Rodney' & ideo$party == 'D',]$name
LES[LES$sponsor == 'tom, rodney' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Tom, Rodney' & ideo$party == 'D',]$party
LES[LES$sponsor == 'tom, rodney' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Tom, Rodney' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'tom, rodney' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Tom, Rodney' & ideo$party == 'R',]$name
LES[LES$sponsor == 'tom, rodney' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Tom, Rodney' & ideo$party == 'R',]$party
LES[LES$sponsor == 'tom, rodney' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Tom, Rodney' & ideo$party == 'R',]$np_score

# *** Mark Miloscia -- Dem from 1999 to 2012 in House; Rep from 2015 to 2018 in Senate -- Formally switched in 2014 to run
LES[LES$sponsor == 'miloscia, mark' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Miloscia, Mark' & ideo$party == 'D',]$name
LES[LES$sponsor == 'miloscia, mark' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Miloscia, Mark' & ideo$party == 'D',]$party
LES[LES$sponsor == 'miloscia, mark' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Miloscia, Mark' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'miloscia, mark' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Miloscia, Mark' & ideo$party == 'R',]$name
LES[LES$sponsor == 'miloscia, mark' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Miloscia, Mark' & ideo$party == 'R',]$party
LES[LES$sponsor == 'miloscia, mark' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Miloscia, Mark' & ideo$party == 'R',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

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
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

#### Manual Fixes
LES[LES$sponsor == "schmick",]$sponsor <- "schmick, joseph s."


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1991 - 2020
# ** Split Control --- 1999-2000 --> Co-sharing Agreement --> CODING ALL 0 
# ** In 2001, split again, but Dems picked up a seat in 2002 coding as Dems for 2001:2002
LES[as.numeric(substring(LES$term,1,4)) %in% c(1991:1994, 2001:2020) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:1998) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1
LES[LES$chamber == 'House' & LES$term == "1999_2000",]$in_majority <- 0

### Senate -- 1991 - 2020
# ** 2017:2018 -- Reps controlled until a Nov 2017 Special gave Dems control ---> CODING ALL IN MAJORITY FOR 2017_2018
# --> In 2013_2014 there was a Rep-Dem Majority Caucus?? - https://en.wikipedia.org/wiki/Majority_Coalition_Caucus
LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:1996, 1999:2002, 2005:2012, 2017:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1991:1992, 1997:1998, 2003:2004, 2013:2017) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1
LES[LES$chamber == 'Senate' & LES$term == "2017_2018",]$in_majority <- 1

### 2013-2014: Rodney Tom (D) was Majority Leader of Majority Coalition Caucus; Tim Sheldon was President Pro-Tem
# ** http://old.seattletimes.com/text/2019906686.html
LES[LES$chamber == 'Senate' & LES$term == "2013_2014" & LES$sponsor %in% c("tom, rodney", "sheldon, tim"),]$in_majority <- 1


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

##### CHECK OUTLIERS ---- No switchers remaining through 2018!
# -- Note, though, that Tim Sheldon was part of the 'Majority Coalition Caucus' that gave Reps control (with him and 1 other D - rodney tom) in 2012
# filter(LES, party == 'd' & np_score > 0) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & np_score < 0) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

