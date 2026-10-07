

##########################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** HAWAII *** BY SESSION
##########################################################################

#### FOR FUTURE
## ******** GO BACK AND DOUBLE CHECK WE HAVE ALL THE SPECIAL SESSIONS FROM SCRAPER -- https://www.capitol.hawaii.gov/archives/2003.aspx#1
## NEED: 2001 SS1/2/3; 2003 SS1; 2005 SS1; 2007 - 2009 SS1/2/3; 2010 SS1/2; 2011-2012 SS1
## 2013 SS1/2; 2014-2015 SS1; 2016-2017 SS1/2/3; 2018-2019 SS1/2?
## ******** MIght want to double check that don't need to ignore_chamber_switch in hist coding
# ---------> Every once in a while a house scheduling action or something pops up before the bill is transmitted to the House -- Might be chamber coding errors
#######################


## QUESTIONS
# ** BY REQUEST BILLS: In 1999_2000 session, ~2000 out of 5800 bills; 2 members with 750, 1 with 200, then sparse
# ---> Two big proposers are President of Senate (Mizuguchi) and Speaker of the House (Say)
# ---> From Leg. Glossary: By Request - "These words or the initials, BR, follow the name of the introducer of a legislative measure to indicate that the 
# introducer does not necessarily endorse the measure but is introducing it as a courtesy." ( https://www.capitol.hawaii.gov/glossary.aspx?show=all )
# --------> ALAN say KEEP (5/6/2019) --- Logic is they still choose to introduce it and it points to variation in leadership 


###################################
## SPECIAL SESSIONS:
## ---- Yes; folded in; NEED to make sure to merge on session as bill numbers start fresh 
## MEMBER LISTS:
## ---- (H) https://www.capitol.hawaii.gov/session2014/docs/HouseYearBook.pdf
## ---- (S) https://www.capitol.hawaii.gov/session2014/docs/SenateYearBook.pdf
## PROCESS:
## -- https://www.capitol.hawaii.gov/docs/CitizensGuide.pdf
## -- https://www.capitol.hawaii.gov/docs/SenateRules.pdf
## -- https://www.capitol.hawaii.gov/docs/HouseRules.pdf
## Sponsorship/Authorship
## -- 
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
library(tibble)
library(inexact)

this_state <- 'HI'
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
terms <- 2019
t = terms
t_plus_one = t + 1
t_yrs <- as.numeric(terms)
t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 1}'))
t_sessions = sessions[grepl(as.character(t),sessions) | grepl(as.character(t+1),sessions)]

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv"))
commem_bills <- commem_bills %>%
  mutate(bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = 0)),
         session = ifelse(session == 'Regular', paste0(term, "-RS"), gsub('Special \\(|\\)', '', session)),
         session = ifelse(!grepl("RS", session), paste0(substring(session, 1, 4), '-SS-', toupper(substring(session, 5, 5))), session))

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
# t <- terms[8]



### Skip Previously Estimated
if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
  print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
  next
}

###### SESSION IN PROGRESS
print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} session! ~~~~~~~~~~~~~~ '))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_yrs}.csv")
bills <- read.csv(bill_path)   
bills <- rename(bills, subjects = package)

### Clean Term/Session Variables
bills <- bills %>%
  mutate(term = t_yrs,
         session = ifelse(session_type == 'Regular', paste0(t_yrs, "-RS"), gsub('Special \\(|\\)', '', session_type)),
         session = ifelse(!grepl("RS", session), paste0(substring(session, 1, 4), '-SS-', toupper(substring(session, 5, 5))), session))

### Check for duplicates
if(nrow(bills) != nrow(distinct(bills))){
  cat(" \n ~~~> DUPLICATE BILLS \n\n .")
  break
}

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_number) %>% 
  mutate(bill_id = toupper(bill_id),
         bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = 0)))

############### Drop Resolutions, Messages, Communications, Reports
all_bills <- bills
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)

############### Standardize Sponsors
bills$introducers <- tolower(bills$introducers)

#### Adjusting for Bills By Request -- Need to do BEFORE splitting
if(any(grepl('\\(br\\)|by request| br$', bills$introducers))){
  print(glue("-----> KEEPING {nrow(filter(bills, grepl('(br)|by request', introducers)))} bill(s) introduced BY REQUEST"))
  bills$introducers <- str_trim(gsub(' \\(br\\)| \\(introduced by req.+\\)| br$', '', bills$introducers))
}

#### Splitting Multi-Sponsored Bills + SAVING A VESION OF THE NAME FOR TABULATING COSPONSORSHIP
# *** THESE are non-alphabetized so 1st = Primary --- Also differences in upper/lowercase (possibly house/senate)
bills$cospon_match <- ifelse(grepl('^[a-z ]+; [a-z]\\.;|^[a-z ]+; [a-z]\\.$', bills$introducers), str_extract(bills$introducers, '^[a-z ]+; [a-z]\\.;|^[a-z ]+; [a-z]\\.$'), gsub(';.+', '', bills$introducers))
bills$cospon_match <- gsub(';$', '', bills$cospon_match)

# **** NEED to do the ifelse otherwise some First Initials get left out (e.g., 'OSHIRO; M.;' in 1999)
bills$LES_sponsor <- ifelse(grepl(';', bills$cospon_match), paste0(gsub('.+; |;$', '', bills$cospon_match), ' ', gsub(';.+', '', bills$cospon_match)), bills$cospon_match)



#### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
bills$LES_sponsor <- gsub('á', 'a', bills$LES_sponsor)
bills$LES_sponsor <- gsub('é', 'e', bills$LES_sponsor)
bills$LES_sponsor <- gsub('ó', 'o', bills$LES_sponsor)
bills$LES_sponsor <- gsub('í', 'i', bills$LES_sponsor)


#### Name Fixes
if(t_yrs == "2011_2012"){
  # Blake Oshiro Resigns in Nov 2011 (https://en.wikipedia.org/wiki/Blake_Oshiro) 
  # System stops recording first intial for Marcus Oshiro -- below will just recode those 2012 obs.
  bills[bills$LES_sponsor == 'oshiro',]$cospon_match <- "m. oshiro"
  bills$introducers <- ifelse(grepl('year=2012', bills$bill_url), gsub('oshiro;|oshiro$', 'm. oshiro;', bills$introducers), bills$introducers)
  bills[bills$LES_sponsor == 'oshiro',]$LES_sponsor <- "m. oshiro"
}
if(t_yrs == '2013_2014'){
  # Lauren Cheape changes name to Masumoto mid-term --> Standardizing
  bills[bills$LES_sponsor == "cheape",]$cospon_match <- 'matsumoto'
  bills[bills$LES_sponsor == "cheape",]$LES_sponsor <- 'matsumoto'
  bills$introducers <- gsub('cheape', "matsumoto", bills$introducers)
}

###################
###### Merge in S&S Bills
###################
# *** For HAWAII: Need Complete Speical Records...
# ---> For Now, Capping Bill numbers at SESSION MAX and assuming bill is from SS with most bills proposed 


SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, Title) %>% 
  mutate(SS = 1)


missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,introducers), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",introducers, ignore.case=T)) %>%
  arrange(introducers) 
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
    left_join(SS_term %>% select(-Title), by = c("bill_id", "term")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS)) %>% 
    distinct()
  SS_term = SS_term %>% 
    left_join(bills %>% select(bill_id,term,session),by=c("bill_id","term"))
  if(!identical(c(nrow(bills),nrow(SS_term)),orig_row_n )){print("merge failed"); break}
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


######################################################################
############### Code Commemorative
######################################################################

bills <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session'))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)


#### DROP Committee Bills
if(any(grepl('committee', bills$LES_sponsor))){
  print(glue("-----> DROPPING {nrow(filter(bills, grepl('committee', LES_sponsor)))} bill(s) introduced BY COMMITTEE"))
  break
  # bills <- filter(bills, !grepl('committee', LES_sponsor))
}

###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "") %>% View()
if(nrow(filter(bills, LES_sponsor == '')) > 0){
  print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
  bills <- filter(bills, !(LES_sponsor == ''))     
}

######################################################################
############### Code Bill History
######################################################################
bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_yrs}.csv")
bill_hist <- read.csv(bill_hist_path)

######## Standardize BillHist Bill IDs
bill_hist <- rename(bill_hist, bill_id = bill_number) %>% 
  mutate(bill_id = toupper(bill_id),
         bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = 0)))

### Clean Term/Session Variables
bill_hist <- bill_hist %>%
  mutate(term = t_yrs,
         session = ifelse(session_type == 'Regular', paste0(t_yrs, "-RS"), gsub('Special \\(|\\)', '', session_type)),
         session = ifelse(!grepl("RS", session), paste0(substring(session, 1, 4), '-SS-', toupper(substring(session, 5, 5))), session))

### Standardize Chamber Variable
bill_hist$chamber <- recode(toupper(bill_hist$chamber), "H" = "House", "S" = "Senate")
bill_hist <- bill_hist %>% 
  group_by(bill_id) %>%
  mutate(chamber = ifelse(chamber == "D", lag(chamber), chamber)) %>%
  ungroup()
# D == used for 'Carried over to YYYY Reg Session'

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
# ** NOTE: Can't find any bills that failed on 2nd/3rd reading... hard to capture that in ABC (but should )
aic_t <- c('scheduled to be heard by', 'the committ.+ recommend', 'scheduled for decision making on', 'the votes in [a-z]+ were', 'conference room [0-9]',
           'the committ.+has sched', 'the committ.+ deferred')
# ** 'conference room [0-9]' could also pick up conference committees but subsetting to intro-chamber only will prevent this
abc_t <- c('^reported from', '^report adopted', 'passed second reading', 'failed to pas second', 'passed third reading', 'failed to pass third',
           '48.+notice', 'forty-eight.+notice', 'placed on the cal.+ for', 'floor amendment',  '^laid on the table',
           '^re-referral to the committ', 'recalled from the commit', 'recommitted to the commit')
## ** 48 hours/hrs. notice
pc_t <- c('passed third reading', 'transmitted to house', 'transmitted to senate', 'enrolled') #technically only enrolled if passes both, but good check if missed
# ** Ok to include both transmitted to because of subsetting to intro chamber actions
law_t <- c('^act [0-9]+')

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
all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
rm(SS_term)

### Save Stage Info **** MERGE WITH SS **********
write.csv(all_bill_stages, glue('../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv'), row.names = FALSE )

rm(bill_hist_path, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist, aic_t, abc_t, pc_t, law_t)
rm(b_id, s_id, b_spon, bill_hist)

##############################################################
######## Identify Unique Legislators via SLER
##########################################################

## Import and Clean Sponsors Name to Match
all_sponsors <- bills %>%
  mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'H', "H", "S")) %>%
  select(LES_sponsor, chamber, passed_chamber, law, cospon_match) %>%
  mutate(term = t_yrs) %>%
  group_by(LES_sponsor, chamber, term, cospon_match) %>%
  summarize(num_sponsored_bills = n(), 
            sponsor_pass_rate = sum(passed_chamber) / n(),
            sponsor_law_rate = sum(law) / n()) %>%
  ungroup()

######## Get Number of COsponsored Bills
all_sponsors$num_cosponsored_bills <- NA
for(i in 1:nrow(all_sponsors)){
  c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == "H", "H", "S"))
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$cospon_match, tolower(c_sub$introducers)))
  ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
}
all_sponsors <- select(all_sponsors, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))
if(min(all_sponsors$num_cosponsored_bills) < 0){
  print("negative cosponsors"); break
} else{print("cosponsors valid")}


#########################
## Clean Names
######################
all_sponsors$last_name <- ifelse(!grepl('\\.', all_sponsors$LES_sponsor), all_sponsors$LES_sponsor, gsub('^[a-z]\\. ', '', all_sponsors$LES_sponsor))
all_sponsors$first_name <- ifelse(grepl('\\.', all_sponsors$LES_sponsor), gsub('\\..+', '', all_sponsors$LES_sponsor), '')
all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update Last Names for Matching 
if(t >= 2005 & t < 2014 ){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "cabanilla", "cabanillaarakawa", all_sponsors$last_name)
}
if(t >= 2013 & t < 2020){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "matsumoto", "cheape-matsumoto", all_sponsors$last_name)
}
if(t >= 2015 & t < 2020){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "fukumoto chang", "fukumoto", all_sponsors$last_name)
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
    T ~  paste0(last_name))) %>%
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
        "cruz-s" = "dela cruz-s",
        "c.  thielen-h" = "thielen-h",
        "chang-s" = "s. chang-s",
        "lee-h" = "c. lee-h"
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
        "costales-h" = NA_character_,
        "cruz-s" = "dela cruz-s"
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

########################################################################################################################
############### Estimate Scores + Add in Relatd Variables
#######################################################################################################################################

### Check if bills in data without an ID'd sponsor
View(filter(bills, !(bills$LES_sponsor %in% legis_data$data_name)))
bills <- bills %>% #select(bills, -sponsor) %>%
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
LES[LES$LES == 0 & is.na(LES$num_sponsored_bills), c('num_sponsored_bills', 'sponsor_pass_rate', 'sponsor_law_rate', "num_cosponsored_bills")] <- 0

############### Save LES
write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
rm(LES)


cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************"))


################# ****** END LOOP

rm(all_sponsors, bills, legis_data, SS_bills, elec_year, i, keep_types)
rm(k_matches, klarner_sub, m_sub, km, bill_path, t_yrs, calc_LES, c_sub)
rm(t, terms, klarner_gs, commem_bills)


########################################################################################################################################################
############################################################################################################################################################# NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 2062 bill(s) introduced BY REQUEST
# -----> Dropping 1 bill(s) without a sponsor
#        session chamber    N  AIC  ABC PASS LAW
# 1 1999_2000-RS       H 3020 1660 1254  759 332
# 2 1999_2000-RS       S 2842 1686 1162  718 247
# APPOINTED: 
# -- ESPERO (filled seat left by Oshiro, P, resignation) -- https://en.wikipedia.org/wiki/Will_Espero

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 1850 bill(s) introduced BY REQUEST
#        session chamber    N  AIC  ABC PASS LAW
# 1 2001_2002-RS       H 2854 1443 1060  633 294
# 2 2001_2002-RS       S 2727 1645 1169  819 259
# APPOINTED to H: 
# -- KOKUBUN (russel) --- Appointed to 2nd district in 2000
# DROP: 
# -- LEVIN (Andy) ---> Per Senate Yearbook, Served 1989-2000 ---> timing implies never seated for 2001 term

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 1537 bill(s) introduced BY REQUEST
#        session chamber    N  AIC  ABC PASS LAW
# 1 2003_2004-RS       H 2990 1604 1108  561 213
# 2 2003_2004-RS       S 2954 1698 1259  777 223
# ******** NO NAME ERRORS *************

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 1545 bill(s) introduced BY REQUEST
#        session chamber    N  AIC  ABC PASS LAW
# 1 2005_2006-RS       H 3260 1766 1261  674 244
# 2 2005_2006-RS       S 3187 1679 1269  759 230
# APPOINTED to H: 
# -- CARROLL (Mele, 2005)
# -- HARBIN (Bev, 2006)
# -- STEVENS (Anne, 2006)
# IN CHAMBER: 
# -- Kaho'ohalahala (Sol) ---> Resigned in February 2005 to take a bureaucratic position (https://en.wikipedia.org/wiki/Sol_Kahoohalahala)
# NAME FIX: cabanilla to cabanillaarakawa (through 2013-2014)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 2031 bill(s) introduced BY REQUEST
#        session chamber    N  AIC  ABC PASS LAW
# 1 2007_2008-RS       H 3451 1873 1401  804 263
# 2 2007_2008-RS       S 3259 1738 1281  763 196
# ******** NO NAME ERRORS *************

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 1592 bill(s) introduced BY REQUEST
#        session chamber    N  AIC  ABC PASS LAW
# 1 2009_2010-RS       H 2993 1421 1044  553 139
# 2 2009_2010-RS       S 2641 1325  954  719 194
# APPOINTED to H: 
# -- KEITH-AGARAN (Gil, 1/2009) - https://ballotpedia.org/Gilbert_Keith-Agaran 
# DROP: 
# -- Bob NAKASONE --- Died in Office, Dec 2008, post-election --- http://the.honoluluadvertiser.com/article/2008/Dec/09/ln/hawaii812090354.html

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 1428 bill(s) introduced BY REQUEST
#        session chamber    N  AIC  ABC PASS LAW
# 1 2011_2012-RS       H 2884 1531 1145  640 268
# 2 2011_2012-RS       S 2631 1331  989  761 246
# APPOINTED to H: 
# -- JORDAN (2011)
# -- KAWAKAMI (2011)
# -- OKAMURA (2012)
# APPOINTED to S: 
# -- KAHELE (2011)
# -- SHIMABUKURO (2010)
# -- SOLOMON (2011)
# DROP from HOUSE: 
# -- SHIMABUKURO -- Appointed in 2010 to Senate seat, post H win -- https://www.capitol.hawaii.gov/memberpage.aspx?member=shimabukuro
# DROP: 
# -- KOKUBUN (russel) resigned 12/2010 for admin post -- https://www.hawaiinewsnow.com/story/13762309/state-sen-kokubun-resigns-for-administration-post/

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 1375 bill(s) introduced BY REQUEST
#        session chamber    N  AIC  ABC PASS LAW
# 1 2013_2014-RS       H 2668 1476 1086  703 224
# 2 2013_2014-RS       S 2506 1318 1041  757 255
# 3    2013-SS-B       H   12    3    3    3   3
# 4    2013-SS-B       S    1    1    1    1   1
# APPOINTED to H: 
# -- CREAGAN (richard, 1/2014)
# -- WOODSON (justin, 1/2013)
# NAME FIX: 
# -- Switched Lauren Cheape Bills to Lauren Matsumoto
# -- Matched Lauren Matsumoto to Lauren Cheapematsumoto in Klarner (through 2020)
# DROP: 
# -- Shan TSUTSUI --> Resigned to become Lt. Gov in Dec. 2012 -- https://en.wikipedia.org/wiki/Shan_Tsutsui

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 1293 bill(s) introduced BY REQUEST
#        session chamber    N  AIC  ABC PASS LAW
# 1 2015_2016-RS       H 2775 1438 1121  660 235
# 2 2015_2016-RS       S 2506 1450 1148  705 238
# APPOINTED to H: 
# -- DECOITE (lynn, 2/2015)
# NAME FIX: Switched Beth Fukomoto Chang to Beth Fukomoto

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 1440 bill(s) introduced BY REQUEST
#        session chamber    N  AIC  ABC PASS LAW
# 1 2017_2018-RS       H 2754 1584 1320  741 235
# 2 2017_2018-RS       S 2424 1461 1195  790 164
# 3    2017-SS-A       S    4    4    3    3   3
# APPOINTED to H: 
# -- LEARMONT (lei, 12/2017)
# -- TODD (chris, 1/5/2017)
# DROP: 
# -- TSUJI (clifton) --> passed away 11/15/2016, after winning office -- https://en.wikipedia.org/wiki/Clift_Tsuji


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

### ****Still missing*****
### Bev HARBIN - Appointed, then asked to resign amid controversy
### TODD + LEARMONT = 2017_2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[2]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, still_missing)

## GIL KEITH-AGARAN
LES[LES$sponsor %in% "keith-agaran" & LES$term %in% "2009_2010",]$klarner_id <- 297229
LES[LES$sponsor %in% "keith-agaran" & LES$term %in% "2009_2010",]$klarner_name <- "keithagaran, gil s. (coloma)"
LES[LES$sponsor %in% "keith-agaran" & LES$term %in% "2009_2010",]$sponsor <- "keithagaran, gil s. (coloma)"

### DEREK KAWAKAMI
LES[LES$sponsor %in% "kawakami" & LES$term %in% "2011_2012",]$klarner_id <- 310670
LES[LES$sponsor %in% "kawakami" & LES$term %in% "2011_2012",]$klarner_name <- "kawakami, derek s. k."
LES[LES$sponsor %in% "kawakami" & LES$term %in% "2011_2012",]$sponsor <- "kawakami, derek s. k."

############## Check for Duplicates
### ---> If two people with same last name and one won in a special, will lead to duplicates
### Remaining = Switched from House to Senate
for(t in unique(LES$term)){
  print(glue(' ************ {t} ***************'))
  check_dup <- filter(LES, term == t & !is.na(klarner_id)) %>%
    group_by(chamber) %>%
    mutate(dup = duplicated(klarner_id)) 
  if(any(check_dup$dup)){
    filter(LES, term == t & klarner_id %in% check_dup[check_dup$dup == TRUE,]$klarner_id) %>%
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

###### IF MISSING CHECK LOSERS
# filter(LES, is.na(party)) %>% select(sponsor, term, klarner_id, district, party, exper)
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

### Manually Fix Those Not in Klarner
LES[LES$sponsor == "harbin",]$party <- 'd'
LES[LES$sponsor == "harbin",]$district <- 28
LES[LES$sponsor == "harbin",]$exper <- 'none'
LES[LES$sponsor == "harbin",]$sponsor <- 'harbin, bev'

### 2017-2018+
LES[LES$sponsor == "todd",]$party <- 'd'
LES[LES$sponsor == "todd",]$district <- 2
LES[LES$sponsor == "todd",]$exper <- 'none'
LES[LES$sponsor == "todd",]$sponsor <- 'todd, chris toshiro'

LES[LES$sponsor == "learmont",]$party <- 'd'
LES[LES$sponsor == "learmont",]$district <- 46
LES[LES$sponsor == "learmont",]$exper <- 'none'
LES[LES$sponsor == "learmont",]$sponsor <- 'learmont, lei r.'

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()

#### Romy Cachola Split over 2 Klarner Rows
LES[LES$sponsor %in% c('cachola, romy', 'cachola, romy m.'),]$sponsor <- 'cachola, romy m.'


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
    LES[LES$sponsor %in% LES[i,]$sponsor,]$SM_name <- ideo_match$name
    LES[LES$sponsor %in% LES[i,]$sponsor,]$SM_party <- ideo_match$party
    LES[LES$sponsor %in% LES[i,]$sponsor,]$np_score <- ideo_match$np_score
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

# Fukumoto matches to change because of dataname
LES[LES$sponsor %in% c('fukumoto, beth', 'tanaka, joe s.', 'tanaka, kam'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# Notable Names: Jun "Felipe" Abinsay; Mele Carroll == Diana Carroll
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES -- LOTS OF MISSINGNESS for HI --- No 2015_2016 Legislators, for example.. and only half the house in 2013_2014
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2015_2016', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('westlund', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'aduja, melodie william', SM_name = 'Williams Aduja')
#name_matches <- add_row(name_matches, LES_name = 'aquino, henry james c.', SM_name = 'zzzzz')
#name_matches <- add_row(name_matches, LES_name = 'cheapematsumoto, lauren', SM_name = 'zzzzz')
#name_matches <- add_row(name_matches, LES_name = 'choy, isaac w.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'coffman, denny', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'creagan, richard p.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'cullen, ty', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'delacruz, donovan', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'djou, charles kong', SM_name = 'Kong Djou')
# name_matches <- add_row(name_matches, LES_name = 'fale, richard', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'fontaine, george r.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'fukumoto, beth', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'galuteria, brickwood m.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'hashem, mark jun', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'ichiyama, linda e.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'ing, kaniela', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'johanson, aaron ling', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'jordan, jo', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'kahele, gilbert', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'kawakami, derek s. k.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'keithagaran, gil s. (coloma)', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'kidani, michelle', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'kim, donna mercado', SM_name = 'Mercado Kim')
# name_matches <- add_row(name_matches, LES_name = 'kobayashi, bert', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'kouchi, ronald d.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'lee, chris kalani', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'lowen, nicole', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'matsuura, richard', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'morikawa, daynette (dee)', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'nakashima, mark m.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'ohno, takashi', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'onishi, richard h. k.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'riviere, gil', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'ruderman, russell e.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'ryan, pohai', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'takayama, gregg', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'tamayo, tulsi gabbard', SM_name = 'Gabbard Tarnayo')
name_matches <- add_row(name_matches, LES_name = 'tanaka, joe s.', SM_name = 'Tanaka')
name_matches <- add_row(name_matches, LES_name = 'tanaka, kam', SM_name = 'Tanaka, Kameo')
# name_matches <- add_row(name_matches, LES_name = 'thielen, laura', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'woodson, justin', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'wooley, jessica', SM_name = 'zzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

#### Manual Edits (Needs more precision...)
LES[LES$sponsor == 'anderson, whitney',]$SM_name <- ideo[ideo$name == 'Anderson' & ideo$senate1999 %in% 1,]$name
LES[LES$sponsor == 'anderson, whitney',]$SM_party <- ideo[ideo$name == 'Anderson' & ideo$senate1999 %in% 1,]$party
LES[LES$sponsor == 'anderson, whitney',]$np_score <- ideo[ideo$name == 'Anderson' & ideo$senate1999 %in% 1,]$np_score

### Marshall Ige --- SM have him as an Rep., but all evidence suggests he's a Dem.

##### Party Switchers
## Gerald Michael Gabbard -- NOTE: He switched from R to D in Nov 2007 (Starts in senate in Jan 2007)
LES[LES$sponsor == 'gabbard, mike' & LES$term %in% c("2007_2008", '2009_2010'),]$party <- 'd'
LES[LES$sponsor == 'gabbard, mike',]$SM_name <- ideo[ideo$name == 'Gabbard, Gerald' & ideo$party == 'D',]$name
LES[LES$sponsor == 'gabbard, mike',]$SM_party <- ideo[ideo$name == 'Gabbard, Gerald' & ideo$party == 'D',]$party
LES[LES$sponsor == 'gabbard, mike',]$np_score <- ideo[ideo$name == 'Gabbard, Gerald' & ideo$party == 'D',]$np_score

## Karen L. Awana -- Switch to Dem in 2007, but SM data only has her as an R, so can't fix
# LES[LES$sponsor == "awana, karen l." & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Awana, Karen' & ideo$party == 'R',]$name
# LES[LES$sponsor == "awana, karen l." & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Awana, Karen' & ideo$party == 'R',]$party
# LES[LES$sponsor == "awana, karen l." & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Awana, Karen' & ideo$party == 'R',]$np_score
# LES[LES$sponsor == "awana, karen l." & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Awana, Karen' & ideo$party == 'D',]$name
# LES[LES$sponsor == "awana, karen l." & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Awana, Karen' & ideo$party == 'D',]$party
# LES[LES$sponsor == "awana, karen l." & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Awana, Karen' & ideo$party == 'D',]$np_score

rm(ideo, check_last, name_matches, d_name, i)


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1999 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1999:2020) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzzz) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1999 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1999:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzzz) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()

##### Drop Nicknames
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Manual Fixes
LES[LES$sponsor == "carroll, mele",]$sponsor <- 'carroll, diana mele'
LES[LES$sponsor == "english, j. kalani",]$sponsor <- 'english, jamie kalani'
LES[LES$sponsor == "gabbard, mike",]$sponsor <- 'gabbard, gerald michael'
LES[LES$sponsor == "inouye, lorraine rode",]$sponsor <- 'inouye, lorraine rodero'


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
            max_LES = max(LES)) # %>%View()

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
## -- Ige Appears to be a SM error; Awana, only have a Rep. record in SM
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

