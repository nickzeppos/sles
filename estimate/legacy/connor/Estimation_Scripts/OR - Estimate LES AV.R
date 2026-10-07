################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** OREGON *** BY SESSION
##############################################################

# ************* SPONSORS NOT ALL RIGHT --- See, e.g., SB0010, 2007 --> Wrong Order


###################################
## (SPECIAL) SESSIONS:
## ---- Regular Sessions split across years (sometimes) but BILL NUBMERS do not restart
## ---- Special Session bills broken out, but bill numbers are unique
## MEMBER LISTS:
## ----
## PROCESS/RULES:
## ---- 
## Sponsorship/Authorship
## ---- Tons of bills sponsored at the request of an agency, introduced by speaker/president, with explicit statement that 
## ---------> introduction neither implies support or opposition of the bill
###########################
## NOTES:
## (1) A few of the bills have incorrect sponsors (outchamber sponsor listed first)
## ---> In some cases, however, sponsors change over time... so do we want initial or final sponsor?
## (2) Could "Summary of Major Legislation Reports???" for S&S--- See: https://www.oregonlegislature.gov/lpro --- scroll to bottom
#################################

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
library(inexact)
library(tibble)
library(foreach)

this_state <- 'OR'
keep_types <- c('HB', 'SB')

#### Output Directory
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd(glue("../../LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

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

commem_bills = commem_bills %>% 
  mutate(term = t_yrs) %>%
  mutate(session = paste0(substr(session,1,4),"-",substr(session,5,nchar(session))),
         session = gsub("R1","RS",session),
         session = gsub("-S","-SS",session)) 


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
# t <- terms[4]



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
bills$chapter_num <- as.numeric(bills$chapter_num)
bills$LC_num <- as.character(bills$LC_num)

### If multiple sessions in different files, read in those as well
if(length(t_sessions) > 1){
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{s}.csv")
    s_bills <- read_csv(bill_path, col_types = cols()) %>%
      mutate(session = as.character(session))
    if(nrow(s_bills) == 0){ next }
    s_bills$chapter_num <- as.numeric(s_bills$chapter_num)
    s_bills$LC_num <- as.character(s_bills$LC_num)
    bills <- bind_rows(bills, s_bills)
  }
  rm(s, s_bills)
}

######## Subset to Term Data + Standardize the Bill IDs
bills <- bills %>% 
  mutate(term = t_yrs) %>%
  mutate(session_1 = substr(session,1,4),
         session_2 = substr(session,5,nchar(session)),
         session = ifelse(session_2 == "R1", paste0(session_1,"-RS"),
                          paste0(session_1,"-SS",nchar(session_2))))

############### Drop Resolutions, Messages, Communications, Reports
all_bills <- bills
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) 

##########################
####### Standardize Sponsors

bills$primary_sponsors <- tolower(bills$primary_sponsors)
bills$primary_sponsors <- gsub('á', 'a', bills$primary_sponsors)
bills$primary_sponsors <- gsub('é', 'e', bills$primary_sponsors)
bills$primary_sponsors <- gsub('ó', 'o', bills$primary_sponsors)
bills$primary_sponsors <- gsub('í', 'i', bills$primary_sponsors)
bills$primary_sponsors <- gsub('ñ', 'n', bills$primary_sponsors)

bills$cosponsors <- tolower(bills$cosponsors)
bills$cosponsors <- gsub('á', 'a', bills$cosponsors)
bills$cosponsors <- gsub('é', 'e', bills$cosponsors)
bills$cosponsors <- gsub('ó', 'o', bills$cosponsors)
bills$cosponsors <- gsub('í', 'i', bills$cosponsors)
bills$cosponsors <- gsub('ñ', 'n', bills$cosponsors)

### FIll NA's
bills$primary_sponsors <- ifelse(is.na(bills$primary_sponsors), '', bills$primary_sponsors)
bills$cosponsors <- ifelse(is.na(bills$cosponsors), '', bills$cosponsors)

#### Manual Name Fixes:
if(t_yrs == '2007_2008'){
  bills$primary_sponsors <- gsub('mr\\. speaker', 'merkley', bills$primary_sponsors)
  bills$cosponsors <- gsub('mr\\. speaker', 'merkley', bills$cosponsors)
  bills[bills$bill_id == 'SB0010',]$primary_sponsors <- "sen brown; sen president courtney; rep merkley"
}
if(t >= 2009){
  bills$primary_sponsors <- gsub('rep speaker', 'rep', bills$primary_sponsors)
  bills$cosponsors <- gsub('rep speaker', 'rep', bills$cosponsors)
}
if(t >= 2007){
  bills$primary_sponsors <- gsub('president courtney', 'courtney', bills$primary_sponsors)
  bills$cosponsors <- gsub('president courtney', 'courtney', bills$cosponsors)
}
if(t_yrs == '2009_2010'){
  bills[bills$bill_id == 'HB2781',]$primary_sponsors <- 'rep gilliam; sen girod'
  bills[bills$bill_id == 'HB2414',]$primary_sponsors <- 'rep buckley; sen monroe; sen morse'
  bills[bills$bill_id == 'SB1022',]$primary_sponsors <- 'sen edwards; rep hoyle'
}
if(t >= 2013 & t <= 2018){
  bills$primary_sponsors <- gsub('sen baertschiger jr', 'sen baertschiger', bills$primary_sponsors)
  bills$cosponsors <- gsub('sen baertschiger jr', 'sen baertschiger', bills$cosponsors)
}
if(t_yrs == "2015_2016"){# Misordered (via RA validation)
  bills[bills$bill_id == 'SB0921',]$primary_sponsors <- 'sen courtney; sen hansell; rep kotek'
}
if(t_yrs == '2017_2018'){
  bills[bills$bill_id == 'SB1051',]$primary_sponsors <- 'sen boquist; rep stark; rep kotek'
  bills[bills$bill_id == 'HB2496',]$primary_sponsors <- 'rep smith db; rep mckeown; sen roblan'
}

if(t_yrs == "2019_2020"){
  bills$primary_sponsors[startsWith(bills$primary_sponsors,"rep williamson") & bills$bill_type == "SB"] = "sen taylor; rep williamson; rep salinas"
}
if(t_yrs == "2021_2022"){
  bills$primary_sponsors[bills$bill_id == "SB0562"] = "sen hansell; sen riley; rep sanchez"
  bills$primary_sponsors = gsub('sen gelser blouin', 'sen gelser', bills$primary_sponsors)
  bills$cosponsors = gsub('sen gelser blouin', 'sen gelser', bills$cosponsors)
}



### Primary Sponsor
bills$primary_sponsor <- gsub(';.+', '', bills$primary_sponsors)

### Remove Titles
bills$primary_sponsors <- gsub("^sen |^rep ", "", bills$primary_sponsors)
bills$primary_sponsors <- gsub(" sen | rep ", " ", bills$primary_sponsors)
bills$cosponsors <- gsub("^sen |^rep ", "", bills$cosponsors)
bills$cosponsors <- gsub(" sen | rep ", " ", bills$cosponsors)  


### LES Sponsor Var
bills <- mutate(bills, LES_sponsor = gsub('^sen |^rep ', '', primary_sponsor))
table(bills$LES_sponsor)



###################
###### Merge in S&S Bills
###################
# *** For OREGON: BIll Numbers are all Unique --> Merge on ID only



if(t_yrs == "2019_2020"){
  SS_bills$bill_id[SS_bills$bill_id=="SB30003"]="SB0003"
} 
if(t_yrs == "2021_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="SB80008"]="SB0008"
  SS_bills$bill_id[SS_bills$bill_id=="SB50005"]="SB0005"
  
}

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,summary), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",summary, ignore.case=T)) %>%
  arrange(summary) 
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

#######################################
############### Code Commemorative
#######################################

bills <- commem_bills %>%
  # filter(term == t_yrs) %>%
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

###### DROP COMMITTEE BILLS
if(nrow(filter(bills, grepl("committee", LES_sponsor))) > 0){
  cat('\n')
  cat(glue("-----> Dropping {nrow(filter(bills, grepl('committee', LES_sponsor)))} bill(s) sponsored by COMMITTEE"))
  cat('\n')
  bills <- filter(bills, !grepl('committee', LES_sponsor) & ! grepl('introduced presession by request', LES_sponsor))     
}


####################################################
############### Code Bill History
####################################################

bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
bill_hist <- read_csv(bill_hist_path, col_types = cols()) %>% 
  mutate_at(vars(session,action_date),as.character)
bill_hist <- distinct(bill_hist)

# If multiple sessions, read in those as well
if(length(t_sessions) > 1){
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
    s_hist <- read.csv(bill_path)
    if(nrow(s_hist) == 0){ next }
    bill_hist <- bind_rows(bill_hist, s_hist)
  }
  rm(s, s_hist)
}

######## Clean Term/Session Variables + Standardize the Bill IDs
bill_hist <- bill_hist %>% 
  mutate(term = t_yrs) %>%
  mutate(session_1 = substr(session,1,4),
         session_2 = substr(session,5,nchar(session)),
         session = ifelse(session_2 == "R1", paste0(session_1,"-RS"),
                          paste0(session_1,"-SS",nchar(session_2))))

### Order by Order
# *** May want to just drop the committee records?
bill_hist <- bill_hist %>% 
  arrange(term, session, bill_id, action_date, order, comm_order) %>%
  group_by(term, session, bill_id) %>%
  mutate(order = 1:n()) %>%
  ungroup() %>%
  select(-comm_order)

### Re-Coding Chamber Variable
cat('\n')
bill_hist <- bill_hist %>%
  mutate(chamber = ifelse(chamber == 'J', NA, chamber)) %>%
  group_by(term, session, bill_id) %>%
  fill(chamber) %>%
  ungroup()

bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate", "G" = "Governor", "CC" = "Conference")

#### Adjusting Committee Report Actions
# bill_hist <- bill_hist %>%
#   mutate(action = tolower(action),
#          action = ifelse(grepl('do pass|do not pass|report without recommend|place on.+calendar', action) & !grepl("house|senate", action), paste0('committee: ', action), action))

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c('reported out', '^recommendation', '^(without|minority|majority) recommendation', 
           'do pass', 'do not pass', 'be adopted', 'do adopt','work session held', 
           '(work session|public hearing): heard', '(work session|public hearing) held')
abc_t <- c('reported out', '^recommendation', '^(without|minority|majority) recommendation',
           'second reading', 'third reading', '^pass', '^fail')
pc_t  <- c('third reading.+passed', '^passed\\.')
law_t <- c('governor signed', '^chapter [0-9]+', 'effective date', "filed with secretary of state without governor's")

### Check Actions
# filter(bill_hist, grepl('governor', tolower(action))) %>% distinct(action) %>% View()
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
  hist_sub <- filter(bill_hist, bill_id == b_id, term == t_yrs, session == s_id)
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
  bill_stages$bill_url <- bills[i,]$bill_url
  #### Check Style/Form Vetoes -- If gov returns for style/form, law after majority of each house passes
  if(bill_stages$law == 0 & !is.na(bills[i,]$chapter_num) ){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- bill_stages$law <- 1
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


read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-2}_{t-1}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% filter(!grepl("SS",session)) %>%  print()

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-4}_{t-3}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% filter(!grepl("SS",session)) %>%  print()

### MERGE to BILL DATA
bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 

### MERGE In COMMEMS
all_bill_stages <- commem_bills %>%
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
            sponsor_law_rate = sum(law) / n(),
            num_cosponsored_bills = NA) %>%
  ungroup()

#### Adding Legislators Who COSPONSORED A BILL but did not SPONSOR
### NEed to keep the 'sen' and 'rep' to do this otherwise will get both chambers
# unique_cospon <- str_trim(unique(unlist(str_split(bills$cosponsors, '; '))))
# 
# for(nonspon in unique_cospon){
#   if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != '' & !grepl('committee|by request', nonspon)){
#     #ns_adj <- gsub('\\(', '\\\\(', gsub('\\)', '\\\\)', nonspon))
#     chamb <- unique(substring(bills[grepl(nonspon, bills$cosponsors),]$bill_id, 1, 1))
#     if("H" %in% chamb & "S" %in% chamb){
#       print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
#     }else{
#       all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0)
#     }
#   }
# }

######## Cosponsorship Info 
bills$cospon_match <- paste(bills$LES_sponsor, bills$cosponsors, sep = '; ')
for(i in 1:nrow(all_sponsors)){
  c_sub <- bills[substring(bills$bill_id, 1, 1) %in% ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S'),]
  sn <- gsub('\\.', '\\\\.', gsub('\\(', '\\\\(', gsub('\\)', '\\\\)', all_sponsors[i,]$LES_sponsor)))
  ## NEED TO ACCOUNT FOR overlapping NAMES
  search_term <- paste0("^", sn, '$|^', sn, ';|; ', sn, '$|; ', sn, ';')
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(search_term, tolower(c_sub$cospon_match)))
  ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
}
bills <- select(bills, -cospon_match)
rm(sn, search_term, c_sub)
#View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))


if(min(all_sponsors$num_cosponsored_bills) < 0){
  print("negative cosponsors"); break
} else{print("cosponsors valid")}

#######################
#### CLEAN NAMES
all_sponsors$first_name <- ifelse(grepl(' [a-z]\\.$| [a-z]$', all_sponsors$LES_sponsor), str_extract(all_sponsors$LES_sponsor, ' [a-z]\\.$| [a-z]$'), '')
all_sponsors$first_name <- str_trim(gsub('\\.', '', all_sponsors$first_name))
all_sponsors$last_name <- str_trim(gsub(' [a-z]\\.$| [a-z]$', '', all_sponsors$LES_sponsor))

all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update Last Names for Matching 
if(t >= 2009 & t <= 2014){
  all_sponsors[all_sponsors$LES_sponsor == 'bailey',]$last_name <-  "kopelbailey"
}
if(t >= 2015 & t <= 2018){
  all_sponsors[all_sponsors$LES_sponsor == 'smith warner',]$last_name <-  "warner"
}
if(t == 2017){
  all_sponsors[all_sponsors$LES_sponsor == 'smith db',]$last_name <-  "brocksmith"
  all_sponsors[all_sponsors$LES_sponsor == 'alonso leon',]$last_name <-  "leon"
  all_sponsors[all_sponsors$LES_sponsor == 'reschke',]$last_name <-  "wernerreschke"
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
} %>% select(-c(votesmart_id,knowwho_pid, ballotpedia)) %>%  distinct() 


if(t_yrs == "2019_2020") {
  legiscan = bind_rows(legiscan,
                       legiscan %>% filter(people_id == 19565) %>% mutate(role = "Rep", district = "HD-060"),
                       legiscan %>% filter(people_id == 19563) %>% mutate(role = "Rep", district = "HD-019"))
  
}

legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = case_when(
    n > 2 ~ paste0(last_name, " ",substr(first_name,1,1), substr(middle_name,1,1)),
    n == 2 ~  paste(last_name,substr(first_name,1,1)),
    T ~ last_name)) %>%
  group_by(match_name) %>% 
  mutate(n = n()) %>% 
  mutate(match_name = ifelse(n > 1, paste0(last_name, " ",substr(first_name,1,1), substr(middle_name,1,1)),
                             match_name)) %>% 
  mutate(match_name_chamber = tolower(paste(match_name,ifelse(substr(role,1,1)=="S","s","h"),sep="-")))


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
        "lawrence spence a-h" = NA_character_,
        "dexter-h" = NA_character_,
        "beyer e-h" = NA_character_,
        "johnson m-h" = NA_character_,
        "owens-h" = NA_character_,
        "warner-h" = "smith warner-h"
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
        "beyer e-h" = NA_character_,
        "johnson m-h" = NA_character_,
        "hieb-h" = NA_character_,
        "warner-h" = "smith warner-h",
        "nelson-h" = NA_character_,
        "sosa-h" = NA_character_
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
filter(bills, !(bills$LES_sponsor %in% legis_data$data_name))
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
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES) # 
rm(t, terms, klarner_gs, t_sessions, commem_bills)


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
## Vacancies filled by APPOINTMENT BY COUNTY BOARD--- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
### Search Members: 
#########################


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1207 bill(s) without a sponsor
# session chamber    N AIC ABC PASS LAW
# 1  2007R1       H 1009 555 359  226 176
# 2  2007R1       S  588 333 226  159 142
# 3  2008S1       S   27  24  20   10  10
### APPOINTED ~ HOUSE:
# -- GILLIAM (vic)
### APPOINTED ~ SENATE:
# -- HASS (mark, past/via H)
### DROP:
# -- sumner, mac -- resigned dec. 2006 after being diagnosed with cancer


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1259 bill(s) without a sponsor
# session chamber   N AIC ABC PASS LAW
# 1  2009R1       H 909 511 303  247 206
# 2  2009R1       S 534 266 173  143 134
# 3  2010S1       H  48  39  31   29  26
# 4  2010S1       S  58  42  25   24  21
### APPOINTED ~ HOUSE:
# -- DOHERTY (margaret)
# -- FREDERICK (lew)
# -- HOYLE (val)
### APPPOINTED ~ SENATE:
# -- EDWARDS (chris, via H, appt august 2009)
### WON ELECTION TO SENATE --> Klarner Errror:
# -- GIROD (fred, via H, appointed in january 2008, beat Bob McDonald in November)
### DROP:
# -- avakian, brad -- resigned in August 2006 to become State Labor Commissioner


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1298 bill(s) without a sponsor
# session chamber    N AIC ABC PASS LAW
# 1  2011R1       H 1119 594 312  239 202
# 2  2011R1       S  502 254 167  122 100
# 3  2012R1       H  111  70  35   26  24
# 4  2012R1       S   52  36  23   20  18
### APPOINTED ~ HOUSE:
# -- KENY-GUYER (allisa)
### WON ELECTION TO SENATE --> Klarner Errror:
# -- GIROD (fred, via H, appointed in january 2008, beat Bob McDonald in November)
### DROP:
# -- carter, margaret -- resigned August 2009 to take position with oregon DHS


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1184 bill(s) without a sponsor
# session chamber   N AIC ABC PASS LAW
# 1  2013R1       H 913 522 353  256 229
# 2  2013R1       S 504 250 170  132 117
# 3  2014R1       H 107  89  60   48  42
# 4  2014R1       S  54  43  35   30  25
### APPOINTED ~ SENATE: 
# -- CLOSE (betsy)
### DROP:
# -- morse, frank -- resigned in Sep. 2012
# -- bonamici, suzanne -- resigned Nov. 2011 to take US House seat


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1121 bill(s) without a sponsor
# session chamber   N AIC ABC PASS LAW
# 1  2015R1       H 998 592 385  280 252
# 2  2015R1       S 604 343 234  160 147
# 3  2016R1       H 104  89  63   50  43
# 4  2016R1       S  67  56  43   35  31
# DROP:
# -- dingfelder, jackie -- resigned senate seat, oct. 2013


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1138 bill(s) without a sponsor
# session chamber   N AIC ABC PASS LAW
# 1  2017R1       H 967 541 374  242 210
# 2  2017R1       S 637 332 186  121 107
# 3  2018R1       H 107  88  70   52  48
# 4  2018R1       S  30  24  20   18  17
# 5  2018S1       H   1   1   1    1   1
### APPOINTED ~ HOUSE:
# -- BONHAM (daniel)
# -- HELFRICH (jeff)
# -- LEWIS (rick)
# -- SALINAS (andrea)
### APPOINTED ~ SENATE:
# -- MANNING JR (james)
### DROP:
# -- bates, alan c. -- died in office, august 2016
# -- edwards, chris -- resigned november 2016 -- https://www.opb.org/news/article/oregon-senator-eugene-resign-chris-edwards/


# filter(klarner, grepl("bonham", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 9 & sen == 1 & year == 2008) %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)



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

### Error Fixes -- Mismatches: SCOTT Fiegen != Kristie Fiegen
# LES[LES$data_name == 'fiegen' & LES$term == "2015_2016",]$klarner_id <- NA
# LES[LES$data_name == 'fiegen' & LES$term == "2015_2016",]$klarner_name <- NA
# LES[LES$data_name == 'fiegen' & LES$term == "2015_2016",]$sponsor <- 'fiegen, scott'

### ****Still missing*****
# still_missing <- unique(LES[is.na(LES$klarner_id),]$sponsor)
# name = still_missing[3]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl('helf', cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, still_missing)

### Fix Missing
name_matches <- data.frame(LES_name = 'edwards', k_name = 'edwards, chris')
name_matches <- add_row(name_matches, LES_name = 'keny-guyer', k_name = 'kenyguyer, alissa')
#name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')
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
rm(check_dup, k_sub, exact, name_sub, missing)


########################################################################################################################################################
########################################################################################################################################################
######## AGGREGATE AND MATCH TO EXTERNAL
########################################################################################################################################################
########################################################################################################################################################

### Klarner State Leg. Election Data --- Using Adjusted File From Top
klarner_sub <- filter(klarner, sab == this_state & year >= min_year - 5 & outcome == 'w')
klarner_sub <- select(klarner_sub, year, sen, ddez, dno, term, termz, cando, cand, candid, partyz, partyz, exper, outcome, etype)

#### KLARNER IS YEAR OF ELECTION, Not TERM
LES$exper <- LES$party <- LES$district <- NA
LES$district <- as.double(LES$district)
LES$party <- as.character(LES$party)
LES$exper <- as.character(LES$exper)

for(name in unique(LES$sponsor)){
  this_sponsor_LES <- LES[LES$sponsor == name,]
  sponsor_rows <- filter(klarner_sub, candid %in% na.omit(this_sponsor_LES$klarner_id)) %>% distinct()
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
          if(nrow(sponsor_sub) >= 2){
            if(sponsor_sub$year[1] == sponsor_sub$year[2]){
              sponsor_sub = filter(sponsor_sub, etype == 'g')
            }
          }
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
# filter(LES, is.na(party)) %>% select(sponsor, term, klarner_id, district, party, exper) %>% as.data.frame()
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

#### Missing Data
LES[LES$sponsor == "dembrow, michael e.",]$party <- "d"

### Not In Klarner
fill_missing <- data.frame(LES_name = "helfrich", new_name = 'helfrich, jeff', party = 'r', district = 52, exper = 'none')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

#### *** 2017_2018: All in chamber in 2019 = Won't be needed once klarner updates ****
LES[LES$sponsor == "lewis", c('party', 'sponsor')] <- list('r', 'lewis, rick')
LES[LES$sponsor == "salinas", c('party', 'sponsor')] <- list('d', 'salinas, andrea')
LES[LES$sponsor == "bonham", c('party', 'sponsor')] <- list('r', 'bonham, daniel')
LES[LES$sponsor == "manning jr", c('party', 'sponsor')] <- list('d', 'manning, james i. jr')

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)
rm(fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()

### Fix/Clean Names
LES[LES$sponsor == 'kopelbailey, jules',]$sponsor <- "kopel-bailey, jules"
LES[LES$sponsor == 'kenyguyer, alissa',]$sponsor <- "keny-guyer, alissa"
LES[LES$sponsor == 'monnesanderson, laurie',]$sponsor <- "anderson, laurie monnes"
LES[LES$sponsor == 'brocksmith, david',]$sponsor <- "smith, david brock"
LES[LES$sponsor == 'steinerhayward, elizabeth',]$sponsor <- "hayward, elizabeth steiner"
LES[LES$sponsor == 'vegapederson, jessica',]$sponsor <- "pederson, jessica vega"


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

# *** Oregon SM Data starts in 1997 ***

ideo <- readstata13::read.dta13("~/Dropbox/Data/Shor_McCarty_Data/shor_mccarty_1993_2016_individual_data_May_2018.dta")
ideo <- filter(ideo, st == this_state) %>% select(-st_id)
ideo$match_name <- str_extract(tolower(ideo$name), '[a-z]+, [a-z]+')
ideo$match_name <- ifelse(is.na(ideo$match_name), tolower(ideo$name), ideo$match_name)
ideo$last_name <- gsub(',.+', '', tolower(ideo$name))

## **** A HANDFUL OF 2015-2016 DUPLICATES ---> Eliminating for nwo...
ideo <- filter(ideo, !duplicated(paste(name, party, sep = '-')))

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

# LES[LES$sponsor %in% c('zzzzzzzz'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# Notable Names: C. Gene Whisnant; L. Scott Bruun; J. Frank Morse; Bernard "Ben" Westlund; Matthew Walter "Wally" Hicks
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

LES[LES$sponsor %in% c('anderson, laurie monnes', 'fahey, julie', 'lewis, rick'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('westlund', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'anderson, laurie monnes', SM_name = 'Monnes Anderson, Laurie')
name_matches <- add_row(name_matches, LES_name = 'brown, kate', SM_name = 'Brown')
name_matches <- add_row(name_matches, LES_name = 'hayward, elizabeth steiner', SM_name = 'Steiner Hayward, Elizabeth')
# --> TWO Elizabeth Johnson Rows; 1 is Elizabeth K.
name_matches <- add_row(name_matches, LES_name = 'johnson, elizabeth (betsy)', SM_name = 'Johnson, Elizabeth')
name_matches <- add_row(name_matches, LES_name = 'prozanski, floyd', SM_name = 'Prozanski Jr, Floyd')
name_matches <- add_row(name_matches, LES_name = 'shields, chip', SM_name = 'Shields, William')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}


########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########

### zzzzzzz -- Dem to Rep to Dem -- basically was a Republican for 2008 election and switched back in Oct 2010
# LES[LES$sponsor == "zzzzzz" & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'zzzzz' & ideo$party == 'R',]$name
# LES[LES$sponsor == "zzzzzz" & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'zzzzz' & ideo$party == 'R',]$party
# LES[LES$sponsor == "zzzzzz" & LES$party == 'r',]$np_score <- ideo[ideo$name == 'zzzzz' & ideo$party == 'R',]$np_score
# LES[LES$sponsor == "zzzzzz" & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'zzzzz' & ideo$party == 'D',]$name
# LES[LES$sponsor == "zzzzzz" & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'zzzzz' & ideo$party == 'D',]$party
# LES[LES$sponsor == "zzzzzz" & LES$party == 'd',]$np_score <- ideo[ideo$name == 'zzzzz' & ideo$party == 'D',]$np_score

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
# filter(LES, grepl('[0-9]', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

#### Manual Fixes
LES[LES$sponsor == 'hicks, wally',]$sponsor <- 'hicks, matthew walter'


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 2003 - 2020 -- Uncomment R row once 2003-2006 data added
# ** 2011_2012: SPLIT CONTROL, "Co" Agremment --> Co-Speakers, Co-Committee Chairs
LES[as.numeric(substring(LES$term,1,4)) %in% c(2007:2010, 2013:2020) & LES$chamber == 'House' & LES$party %in% 'd',]$in_majority <- 1
# LES[as.numeric(substring(LES$term,1,4)) %in% c(2003:2006) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1
LES[LES$term == "2011_2012" & LES$chamber == "House",]$in_majority <- 1

### Senate -- 2003 - 2020
# ** 2003_2004: Divide Power Contract: Key Leadership and Committee Posts divided between parties
LES[as.numeric(substring(LES$term,1,4)) %in% c(2005:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzzz) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1
# LES[LES$term == "2003_2004" & LES$chamber == "Senate",]$in_majority <- 1

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
    Ideology_Missing = round(sum(is.na(np_score))/n(), 2))  %>%
  as.data.frame()

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
  scale_color_manual(values=c("dodgerblue2", "red2", 'gray50'))

##### CHECK OUTLIERS ---> ALL REMAINING PARTIES MATCH THOSE IN SHOR-MCCARTY DATA
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

