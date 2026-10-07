
################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** ARKANSAS *** BY SESSION
##############################################################

###################################
## (SPECIAL) SESSIONS:
## ---- Special sessions folded into General Assembly File BUT BILL NUMBERS RE-START
## MEMBER LISTS:
## ---- See validation at bottom: lists via previous legislatures tab on main site + good availability via the internet archive
## PROCESS/RULES:
## ---- Second reading often comes before committee assignement
## Sponsorship/Authorship
## ---- For most years, only primary sponsor is listed (e.g., "primary_name et al")
## ---- Committee Bills allowed --> Attribute to sponsor??
###########################
## NOTES:
## ** 
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
library(tibble)
library(inexact)

this_state <- 'AR'
keep_types <- c('HB', 'SB')

#### Output Directory
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
t_num = (terms - 2005)/2 + 85
t_sessions = sessions[startsWith(sessions,as.character(t_num))]

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv"))
commem_bills <- mutate(commem_bills, bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = 0)))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- bind_rows(read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{terms}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{terms+1}.csv")))

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

### Also:
# -- Pretty sure thomas moore (1998, D33, House) won a special; rita hale was the incumbent, died in office. 
# -- Hudson Hallum did not win the 2012 election (fred smith did): https://www.latimes.com/nation/politics/la-na-fred-smith-arkansas-20140518-story.html

### Fix Later Sessions Function
fix_sessions <- function(bill_df, t_yrs){
  if(t_yrs == "2007_2008"){
    bill_df$session <- recode(bill_df$session, 'Regular Session' = "Regular Session, 2007", 'First Extraordinary Session of 86th General Assembly' = 'First Extraordinary Session, 2008')
  }else if(t_yrs == "2009_2010"){
    bill_df$session <- recode(bill_df$session, 'Regular Session' = "Regular Session, 2009", 'Fiscal Session' = 'Fiscal Session, 2010')
  }else if(t_yrs == "2011_2012"){
    bill_df$session <- recode(bill_df$session, 'Regular Session' = "Regular Session, 2011", 'Fiscal Session' = 'Fiscal Session, 2012')
  }else if(t_yrs == "2013_2014"){
    bill_df$session <- recode(bill_df$session, 'Regular Session' = "Regular Session, 2013")
  }
  return(bill_df)
}

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[2]



### Formulate 2-year terms -- Cover both regular and special sessions

### Skip Previously Estimated
if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
  print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
  next
}

###### SESSION IN PROGRESS
print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))

############### Read in data
############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_sessions[1]}.csv")
bills <- read.csv(bill_path)
bills <- arrange(bills, bill_num)

### Fix sessions
bills <- fix_sessions(bills, t_yrs)

### Clean Term/Session Variables
bills <- rename(bills, bill_id = bill_num) %>% 
  mutate(term = t_yrs,
         ga_num = gsub(' .+', '', ga_num),
         bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = 0)),
         session_year = gsub('.+, ', '', session),
         session = gsub(', [0-9]+', '', session),
         session = recode(session, 'Regular Session' = 'RS', 
                          'First Extraordinary Session' = 'SS1',
                          'Second Extraordinary Session' = 'SS2',
                          'Third Extraordinary Session' = 'SS3',
                          'Fourth Extraordinary Session' = 'SS4',
                          'Fiscal Session' = 'FS'),
         session = paste(session_year, session, sep = "-"))

### Check for duplicates
if(nrow(bills) != nrow(distinct(bills))){
  cat(" \n ~~~> DUPLICATE BILLS \n\n .")
  break
}



##########################
####### Standardize Sponsors

bills$primary_sponsor <- tolower(bills$primary_sponsor)
bills$primary_sponsor <- gsub('á', 'a', bills$primary_sponsor)
bills$primary_sponsor <- gsub('é', 'e', bills$primary_sponsor)
bills$primary_sponsor <- gsub('ó', 'o', bills$primary_sponsor)
bills$primary_sponsor <- gsub('í', 'i', bills$primary_sponsor)
bills$primary_sponsor <- gsub('ñ', 'n', bills$primary_sponsor)

### Manual Fixes TO NAMES
if(t_yrs == "1997_1998"){
  bills$primary_sponsor <- gsub('mcgeheee', 'mcgehee', bills$primary_sponsor) #Note -- there is a mcgee and a mcgehee
  bills$primary_sponsor <- gsub('flangin', 'flanagin', bills$primary_sponsor)
  bills[bills$primary_sponsor == 'hudson et al',]$primary_sponsor <- "j. hudson et al"  #http://www.arkleg.state.ar.us/assembly/1997/R/Bills/HB1156.pdf
}else if(t_yrs == "1999_2000"){
  bills[bills$primary_sponsor == 'brown',]$primary_sponsor <- "j. brown" # http://www.arkleg.state.ar.us/assembly/1999/R/Pages/BillInformation.aspx?measureno=SB963
}else if(t_yrs == "2003_2004"){ # L. Prater based on the amendment filed on the bill
  bills[bills$primary_sponsor == 'prater',]$primary_sponsor <- "l. prater" # http://www.arkleg.state.ar.us/assembly/2003/R/Amendments/HB1005-H1.pdf
  bills[bills$primary_sponsor == 'elliott',]$primary_sponsor <- "j. elliott" # Only one elliott
}else if(t_yrs == "2009_2010"){ # Only 1 wyatt in senate
  bills[bills$primary_sponsor == 'wyatt',]$primary_sponsor <- "d. wyatt" 
}
# filter(bills, grepl("wyatt", primary_sponsor) ) %>% distinct(primary_sponsor)
# filter(klarner, grepl('wyatt', cand) & year >= 2002 & outcome == "w") %>% distinct(year, sen, cand, outcome) %>% arrange(year)

### LES Sponsor Var
bills$LES_sponsor <- gsub(' et al$| \\&.+', '', str_trim(bills$primary_sponsor))
table(bills$LES_sponsor)
# ---> JBC = Joint Budget Committee

############### Drop Resolutions, Messages, Communications, Reports
all_bills <- bills
bills <- mutate(bills, bill_type = str_trim(toupper(gsub('[0-9].+|[0-9]+', '', bill_id))))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) 

#### Adjusting for Bills By Request -- Need to do BEFORE splitting
if(any(grepl("request", bills$LES_sponsor))){
  print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
}

#### Temporary list of committees to drop:
bills$LES_sponsor <- gsub('  +', ' ', bills$LES_sponsor)
drop_comms <- c("efficiency", "house agri", "house cty co", "house management", "house mgmt. comm.",
                "jbc", "jic ins", "judiciary", "public hlth comm", "senate efficiency", "st. ag.",
                "insurance", "revenue", "state agencies")



###################
###### Merge in S&S Bills
###################

if(t_yrs == "2019_2020"){
  SS_bills$bill_id[SS_bills$bill_id=="SB20002"] = "SB0002"
} else if(t_yrs == "2021_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="SB10001"] = "SB0001"
  SS_bills$bill_id[SS_bills$bill_id=="SB60006"] = "SB0006"
  SS_bills$bill_id[SS_bills$bill_id=="SB20002"] = "SB0002"
  SS_bills$bill_id[SS_bills$bill_id=="SB40004"] = "SB0004"
  SS_bills$bill_id[SS_bills$bill_id=="SB20002"] = "SB0002"
}


SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, year, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS, year,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,primary_sponsor), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",primary_sponsor, ignore.case=T)) %>%
  arrange(primary_sponsor) 
unique(missing_SS_bills$bill_id) 

# now check to see if there are duplicate joins


duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills %>% mutate(year = as.integer(substr(session,1,4))), 
            by = c("bill_id", "term","year")) %>%
  group_by(rowname) %>% mutate(count = n()) %>%
  ungroup() %>% 
  select(count,bill_id,term,year,session, Title, title) %>% 
  arrange(desc(count),year,bill_id)

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
    select(bill_id,term,session,year) %>% mutate(SS = 1) %>% distinct()
  
  
  
  bills <- bills %>% 
    left_join(SS_term %>% select(-year), by = c("bill_id", "term", "session")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS)) %>% 
    distinct()
} else {
  orig_row_n = c(nrow(bills),nrow(SS_term))
  bills <- bills %>% 
    left_join(SS_term %>% select(-Title), by = c("bill_id", "term","session"="year")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS)) %>% 
    distinct()
  SS_term = SS_term %>% 
    left_join(bills %>% select(bill_id,term,session),by=c("bill_id","term","year"="session"))
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
  SS_term %>% group_by(bill_id, term) %>%
    mutate(count = n()) %>% filter(count > 1) %>% arrange(bill_id)
  
  # see if they are equal after removing double-counted bills. this is a crude proxy, you should examine more closely
  all.equal(SS_in_bills + nrow(anti_join(SS_term, bills, by = c("bill_id", "term", "session")))+
              nrow(SS_term %>% group_by(bill_id, term) %>%
                     mutate(count = n()) %>% filter(count > 1))/2,
            nrow(SS_term ))
}


########## Merge in Commemoratives
bills <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session'))

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)

### Drop/Fix Committee Bills
if(nrow(bills[bills$LES_sponsor %in% drop_comms | grepl('committee', bills$LES_sponsor),]) > 0){
  cat('\n')
  cat(glue("-----> Dropping {nrow(bills[bills$LES_sponsor %in% drop_comms | grepl('committee', bills$LES_sponsor),])} bills sponsored by COMMITTEE"))
  bills <- filter(bills, !(LES_sponsor %in% drop_comms | grepl('committee', LES_sponsor)))
}

###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "" | is.na(LES_sponsor)) %>% View()
if(nrow(filter(bills, LES_sponsor == '')) > 0 | any(is.na(bills$LES_sponsor))){
  cat('\n')
  cat(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == '')) + sum(is.na(bills$LES_sponsor))} bill(s) without a sponsor"))
  bills <- filter(bills, !(LES_sponsor == '' | is.na(LES_sponsor)))     
}

############### Code Bill History
bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
bill_hist <- read.csv(bill_hist_path)

### Fix Session Var
bill_hist <- fix_sessions(bill_hist, t_yrs)

### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
bill_hist <- rename(bill_hist, bill_id = bill_num) %>% 
  mutate(term = t_yrs,
         ga_num = gsub(' .+', '', ga_num),
         bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = 0)),
         session_year = gsub('.+, ', '', session),
         session = gsub(', [0-9]+', '', session),
         session = recode(session, 'Regular Session' = 'RS', 
                          'First Extraordinary Session' = 'SS1',
                          'Second Extraordinary Session' = 'SS2',
                          'Third Extraordinary Session' = 'SS3',
                          'Fourth Extraordinary Session' = 'SS4',
                          'Fiscal Session' = 'FS'),
         session = paste(session_year, session, sep = "-"))

### Order by Order
bill_hist <- arrange(bill_hist, term, session, bill_id, order)

### Re-Coding Chamber Variable
# bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate", "G" = "Governor", "CC" = "Conference")

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c('returned by the committee', 'do pass', 'do not pass', 'without recommendation')
abc_t <- c('returned by the committee', 'engrossed', 'third time', 'amendment.+read', 're-referred to',
           'committee of the whole', 'placed on the calendar', 'placed on calendar')
# Engrossment occurss after amended, not passed chamber
pc_t <- c('third time and passed', 'transmitted to the senate', 'transmitted to the house')
law_t <- c('act [0-9]+')

### Check Actions
# filter(bill_hist, grepl('placed on the calendar', tolower(action))) %>% distinct(bill_id, action) %>% View()
# filter(bill_hist, grepl('effective date', tolower(action))) %>% group_by(action) %>% summarize(n = n()) %>% arrange(desc(n)) %>% View()
# bill_hist[bill_hist$bill_id == "SB648",]
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
options(warn = 2)
for(i in 1:nrow(bills)){
  b_id = bills[i,]$bill_id
  s_id = bills[i,]$session
  b_spon = bills[i,]$LES_sponsor
  hist_sub <- filter(bill_hist, bill_id == b_id, term == t_yrs, session == s_id)
  ## Error -- SB1 & 2 in 2000-SS2 -- No Senate Actions Listed but was passed out of the senate and became a law
  if(t_yrs == "1999_2000" & b_id %in% c("SB0001", 'SB0002') & s_id == "2000-SS2"){
    bill_stages <- data.frame(bill_id = b_id, term = t_yrs, session = s_id, LES_sponsor = b_spon, introduced = 1, action_in_comm = 1, action_beyond_comm = 1, passed_chamber = 1, law = 1)
  }else{
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
  }
  ### check if act
  if(bill_stages$law == 0 & !is.na(bills[i,]$act_num)){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- bill_stages$law <- 1
  }
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
  left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))
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
  mutate(term = t_yrs) %>%
  group_by(LES_sponsor, chamber, term) %>%
  summarize(num_sponsored_bills = n(), 
            sponsor_pass_rate = sum(passed_chamber) / n(),
            sponsor_law_rate = sum(law) / n()) %>%
  ungroup()

#### Adding Legislators Who COSPONSORED A BILL but did not SPONSOR
# unique_cospon <- str_trim(unique(unlist(str_split(bills$cosponsors, '; '))))
# for(nonspon in unique_cospon){
#   if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != ''){
#     chamb <- unique(substring(bills[grepl(nonspon, bills$cosponsors),]$bill_id, 1, 1))
#     if("H" %in% chamb & "S" %in% chamb){
#       print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
#     }else{
#       all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0) 
#     }
#   }
# }

######## Cosponsorship Info --- For OH: Only have cosponsor info for most recent years
all_sponsors$num_cosponsored_bills <- NA
# bills$cospon_match <- paste(bills$LES_sponsor, bills$primary_sponsor, bills$cosponsors, sep = '; ')
# for(i in 1:nrow(all_sponsors)){
#   c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
#   all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
#   ### ONLY need to adjust this way if sponsored and cosponsored column are the same
#   all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
# }
# bills <- select(bills, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))

#######################
#### CLEAN NAMES
all_sponsors$last_name <- ifelse(grepl("^[a-z]\\. ", all_sponsors$LES_sponsor), gsub('^[a-z]\\. ', '', all_sponsors$LES_sponsor), all_sponsors$LES_sponsor)
all_sponsors$first_name <- ifelse(grepl("^[a-z]\\. ", all_sponsors$LES_sponsor), gsub('\\. .+', '', all_sponsors$LES_sponsor), '')
all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)


#### Update Last Names for Matching 
if(t_yrs %in% c("1997_1998")){
  all_sponsors[all_sponsors$LES_sponsor == 'ingram',]$last_name <-  "owensingram"
}else if(t_yrs == "2017_2018"){
  all_sponsors[all_sponsors$LES_sponsor == 'barker',]$last_name <-  "eubanksbarker"
}



all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(LES_sponsor,substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name))


legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(terms)) | startsWith(legiscan_sessions,as.character(terms+1))]

legiscan = foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% distinct()





legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name,) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = case_when(
    n > 2 ~ paste0(substr(first_name,1,1), substr(middle_name,1,1), ". ", last_name),
    n == 2 ~  paste(substr(first_name,1,1),last_name,sep=". "),
    T ~ last_name)) %>%
  group_by(match_name) %>% 
  mutate(n = n()) %>% 
  mutate(match_name = ifelse(n > 1, paste0(substr(first_name,1,1),". ", last_name),
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
        "deffenbaugh-h" = NA_character_,
        "springer-h" = NA_character_,
        "meeks-h" = "s. meeks-h",
        "hammer-s" = "k. hammer-s",
        "eads-s" = "l. eads-s",
        "gray-h" = "m. gray-h",
        "nicks-h" = NA_character_,
        "sturch-s" = "j. sturch-s",
        "ennett-h" = NA_character_,
        "mcgrew-h" = NA_character_,
        "walker-h" = NA_character_,
        "c. cooper-h" = NA_character_,
        "berry-h" = NA_character_
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
        "deffenbaugh-h" = NA_character_,
        "mcelroy-h" = NA_character_,
        "pitsch-s" = NA_character_,
        "springer-h" = NA_character_,
        "j. dotson-s" = NA_character_,
        "fulfer-s" = NA_character_,
        "meeks-h" = "s. meeks-h",
        "hammer-s" = "k. hammer-s",
        "clark-s" = "a. clark-s",
        "eads-s" = "l. eads-s",
        "mcnair-h" = NA_character_,
        "nicks-h" = NA_character_,
        "beaty-h" = "beaty jr.-h",
        "s. flowers-s" = NA_character_,
        "mayberry-h" = "j. mayberry-h",
        "d. garner-h" = NA_character_,
        "d. ferguson-h" = NA_character_,
        "gray-h" = "m. gray-h",
        "s. berry-h" = NA_character_
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
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES) # 
rm(t, terms, klarner_gs, m_sub, ga_txt, drop_comms, fix_sessions) # 


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by SPECIAL ELECTION --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
### FULL ROSTERS on session pages-- http://www.arkleg.state.ar.us/assembly/2013/2013R/Pages/Previous%20Legislatures.aspx
### Wayback Machine Starts ~ 1997: https://web.archive.org/web/19970327193747/http://www.arkleg.state.ar.us/
### ---> Can gt committees this way
#########################

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 453 bills sponsored by COMMITTEE
### WON SPECIAL ~ SENATE:
# -- WYRICK (phill) -- via H, switched parties to do it - https://talkbusiness.net/2011/08/more-party-switches-from-arkansas-history/
### IN HOUSE:
# -- bennett, m. dee
# -- bond, pat
# -- walker, wilma
### DROP:
# -- snyder, victor f. -- never re-seated, won seat in Congress -- https://talkbusiness.net/2011/08/more-party-switches-from-arkansas-history/

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 645 bills sponsored by COMMITTEE
#### Klarner missing:
# -- hale (rita) -- Sponsored bills, died early in term, but not in Klarner oddly (http://ark-women-legislators.blogspot.com/2008/06/rita-rowell-hale.html)
#### IN HOUSE:
# -- oglesby, steve
# -- moore, thomas -- same seat as Hale... must have won her seat in special, but in klarner for whatever reason
# -- jeffress, gene
# -- gipson, bill
# -- eason, john a
#### DROP

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- mack, dewayne
### WON SPECIAL ~ SENATE:
# -- miller, paul
### IN HOUSE:
# -- eason, john a,
# -- willis, arnell
### DROP:
# -- hughes, randy -- no evidence he was seated; successor jay bradford was seated by early 2001 per wayback machine
# -- wilson, nick -- paul miller won seat in special, switched to D10 in 2002

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 422 bills sponsored by COMMITTEE
#### DROP --- MAY have been there for first extraordinary session but no records for regular session...
# -- simes, alvin --- No evidence in chamber for 2nd half of term; missing from wayback as of Feb 2003
# -- cash, claud v. --- No evidence in chamber for 2nd half of term; missing from wayback as of Feb 2003


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 334 bills sponsored by COMMITTEE
### DROP:
# -- gullett, brenda b. --- No evidence she was in chamber... Had elections again in 2004 in her district

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 337 bills sponsored by COMMITTEE
### IN HOUSE:
# -- burkes, aaron


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 614 bills sponsored by COMMITTEE
# IN HOUSE:
# -- davis, otis
# -- rice, terry
# -- dale, robert e.
# -- gaskill, billy
# -- house, jim


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 588 bills sponsored by COMMITTEE
### WON SPECIAL ~ HOUSE:
# -- cozart, bruce
### WON SPECIAL ~ SENATE
# -- lamoureux, michael
### IN HOUSE:
# -- smith, fred -- served for 19 days, was in legal trouble, was able to run again 2 years later
# -- dickinson, jody
# -- wagner, charolette
### DROP:
# -- trusty, sharon -- lamoureux was elected in 2009 special to replace her
# -- crass, keith -- died october 27, 2010, but still won: https://arktimes.com/arkansas-blog/2010/10/27/candidate-keith-crass-dies


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 619 bills sponsored by COMMITTEE
### IN HOUSE:
# -- f. smith = smith, fred = klarner wrong; won as green candidate; D votes were not counted bc D cand had been convicted: https://www.latimes.com/nation/politics/la-na-fred-smith-arkansas-20140518-story.html
# -- hopper, karen
### DROP:
# -- hallum, hudson -- didn't win, see f. smith note above
# -- Four 2010 senate winners --> this was a 2-year term
# ----> crumbly, jack; harrelson, steve; fletcher, mike; pritchard, bill

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 597 bills sponsored by COMMITTEE
### WON SPECIAL ~ SENATE:
# -- j. cooper
# -- standridge
### IN HOUSE:
# -- holcomb, judge mike
# -- henderson, kenneth
### DROP:
# -- lamoureux, michael -- resigned mid senate term, before 2015 session started
# -- bookout, paul -- resigned in 2013; was conviced of wire fraud in 2014
# -- 4 who lost or didn't run in 2014 senate elections:
# -----> holland, bruce; key, johnny; wyatt, david wayne; thompson, robert f.

#~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 552 bills sponsored by COMMITTEE
### IN HOUSE:
# - burch, leanne
# - cavenaugh, frances


# filter(klarner, grepl("key, j", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(year, cand) %>% distinct() %>% as.data.frame() 
# filter(klarner, ddez == 17 & sen == 1 & outcome == 'w') %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)




##########################################################################################################
##########################################################################################################
##### MATCHING MISSING (SPECIALS) TO KLARNER USING OTHER TERMS WHERE SPONSOR IS PRESENT
##########################################################################################################

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
# name = still_missing[1]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl('eslick', cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, still_missing)


### Fix Missing
name_matches <- data.frame(LES_name = 'miller', k_name = 'miller, paul')
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

###### IF MISSING, CHECK ELCTION LOSERS
# filter(LES, is.na(party)) %>% select(sponsor, term, klarner_id, district, party, exper)
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

### Not In Klarner
# fill_missing <- data.frame(LES_name = "zzzzzzz", new_name = 'zzzzzzzz', party = 'zzzz', district = zzzz, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')

# for(i in 1:nrow(fill_missing)){
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
# }

#### *** 2017_2018: NO MISSING ****
#LES[LES$sponsor == "zzzzzzzz",]$party <- 'zzzzzz'

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)
# rm(fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

#### Drop Nicknames
LES$sponsor <- gsub(' \\([^\\)]+\\)', '', LES$sponsor)

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

#### IF Duplicates in SM DATA:
# ideo <- filter(ideo, !duplicated(paste(name, party)))

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
LES[LES$sponsor %in% c('johnson, blake'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# Notable Mismatches: James 'Ed' Wilkinson; Joseph "Jodie" Mahony'; Eugene 'Bud' Candada
# -- Elizabeth "Jane" English; David Burnett == Charles; James Talley = Brent;
# -- Logan Jett seems to be Joe Jett; Scott Baltz = Joseph Baltz
# --> A number of these names may be wrong, but dates overlap
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1991_1992', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('baker, t', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

name_matches <- data.frame(LES_name = 'baker, tommy lee', SM_name = 'Baker, Tommy')
name_matches <- add_row(name_matches, LES_name = 'baker, tom', SM_name = 'Baker, Thomas G.') # Not clear these are the same -- significant gap
name_matches <- add_row(name_matches, LES_name = 'bell, nate', SM_name = 'Bell, Jerry')  # Jerry - https://en.wikipedia.org/wiki/Nate_Bell
#name_matches <- add_row(name_matches, LES_name = 'betts, monty', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'bookout, jerry', SM_name = 'Bookout, Paul-Jerry')
name_matches <- add_row(name_matches, LES_name = 'bookout, paul', SM_name = 'Bookout, Paul')
#name_matches <- add_row(name_matches, LES_name = 'carroll, richard', SM_name = 'zzzzzzz')
#name_matches <- add_row(name_matches, LES_name = 'cole, steve', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'evans, david', SM_name = 'Evans, A. David')
name_matches <- add_row(name_matches, LES_name = 'evans, lenville', SM_name = 'Evans, William Sr.') # = William - https://votesmart.org/bill/2485/7637/27914/allen-maxwell-voted-yea-passage-hb-1173-reversal-of-the-bmi-report-card-requirement
name_matches <- add_row(name_matches, LES_name = 'holland, bruce', SM_name = 'Holland, F.') # Franklin Bruce Holland
name_matches <- add_row(name_matches, LES_name = 'jeffress, gene', SM_name = 'Jeffress, Harmon') # Harmon Gene Jeffress
name_matches <- add_row(name_matches, LES_name = 'johnson, blake', SM_name = 'Johnson, Lowell') # Lowell Blake Johnson
name_matches <- add_row(name_matches, LES_name = 'johnson, j. p.', SM_name = 'Johnson, J.P. Bob')
name_matches <- add_row(name_matches, LES_name = 'lewellen, bill', SM_name = 'Lewellen, Roy C.') # Roy Bill Lewellen - https://www.apnews.com/aa7732b664342d1aa2eecf73166ad296
name_matches <- add_row(name_matches, LES_name = 'malone, percy', SM_name = 'Malone, W. Percy')
#name_matches <- add_row(name_matches, LES_name = 'mayberry, andy', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'mcgehee, w. k. jr.', SM_name = 'McGee') # There's also a Ben McGee, so this must be misspelled; lines up right timewise
name_matches <- add_row(name_matches, LES_name = 'nicks, milton jr.', SM_name = 'Nicks Jr, Milton')
#name_matches <- add_row(name_matches, LES_name = 'nix, barbara', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'owensingram, marian d.', SM_name = 'Owens, Marian D.')
name_matches <- add_row(name_matches, LES_name = 'roebuck, gene', SM_name = 'Roebuck')
name_matches <- add_row(name_matches, LES_name = 'smith, mark alan', SM_name = 'Smith, Mark Alan')
name_matches <- add_row(name_matches, LES_name = 'smith, roger', SM_name = 'Smith, M. Roger')
name_matches <- add_row(name_matches, LES_name = 'smith, terry', SM_name = 'Smith, William') # William Terry Smith
#name_matches <- add_row(name_matches, LES_name = 'standridge, greg', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'walker, bill', SM_name = 'Walker, William L, Jr.')
name_matches <- add_row(name_matches, LES_name = 'williams, mayor eddie joe', SM_name = 'Williams, Eddie')
name_matches <- add_row(name_matches, LES_name = 'wood, jim', SM_name = 'Wood, James E. Jr.')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########
### Check Party Mismatches
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% arrange(sponsor, term) %>% as.data.frame()
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% arrange(sponsor, term) %>% as.data.frame()

#### Unfixable:
# -- Fred Smith, Elected as Dem in 2010, switched to green party in 2012, then back to dem in 2014
# -- Mike Holcomb, switched D to R in 2015, only have D record in SM data

### Claud Cash: Klarner error -- he was a democrat, not republican: https://arktimes.com/arkansas-blog/2013/08/28/former-senator-plans-run-for-jonesboro-senate-seat
LES[LES$sponsor == 'cash, claud v.',]$party <- 'd'

# *** Phill Wyrick -- Switched Parties to Win Senate Seat -- Only have data from the term he switched
# --> https://talkbusiness.net/2011/08/more-party-switches-from-arkansas-history/
LES[LES$sponsor == "wyrick, phill",]$party <- 'r'
LES[LES$sponsor == 'wyrick, phill' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Wyrick, Phil' & ideo$party == 'R',]$name
LES[LES$sponsor == 'wyrick, phill' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Wyrick, Phil' & ideo$party == 'R',]$party
LES[LES$sponsor == 'wyrick, phill' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Wyrick, Phil' & ideo$party == 'R',]$np_score

### Linda Collins-Smith -- Switch D to R --- Matched Correctly but 2011_2012 party wrong
LES[LES$sponsor == "collinssmith, linda",]$party <- 'r'

### Bill Walters -- Was a Republican 1983 - 2000; but switched in 2008 to run for his wife's seat.. Per Wiki...
LES[LES$sponsor == "walters, bill",]$party <- "r"

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

##### Drop Nicknames
# filter(LES, grepl('\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
# LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)
LES[LES$sponsor == 'fitch, jon 1',]$sponsor <- 'fitch, jon stuart'

#### Remove Honorifics
LES$sponsor <- gsub(', judge |, mayor ', ', ', LES$sponsor)

#### Manual Fixes
LES[LES$sponsor == 'dellarosa, jana',]$sponsor <- 'della rosa, jana wootton'
LES[LES$sponsor == 'wilkinson, ed',]$sponsor <- 'wilkinson, james ed'
LES[LES$sponsor == 'cook, m. olin',]$sponsor <- 'cook, milton olin'
LES[LES$sponsor == 'canada, bud',]$sponsor <- 'canada, eugene bud'
LES[LES$sponsor == 'judy, jan',]$sponsor <- 'judy, janice ann'
LES[LES$sponsor == 'english, jane',]$sponsor <- 'english, elizabeth jane'
LES[LES$sponsor == 'talley, brent',]$sponsor <- 'talley, james brent'
LES[LES$sponsor == 'gates, jp mickey',]$sponsor <- 'gates, mickey'
# LES[LES$sponsor == 'zzzzz',]$sponsor <- 'zzzzzz'


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1991 - 2020 --- Dem Control Through 2012; R Control Thereafter
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2012) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2013:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1991 - 2020 --- Same: Dem Control Through 2012; R Control Thereafter
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2012) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2012:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1

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
    Ideology_Missing = round(sum(is.na(np_score))/n(), 2)) %>% as.data.frame()

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

##### CHECK OUTLIERS ---- 
# filter(LES, party == 'd' & SM_party == "R") %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & SM_party == 'D') %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()


### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + in_majority + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

