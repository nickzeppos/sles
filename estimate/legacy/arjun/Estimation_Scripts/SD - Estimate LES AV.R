################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** SOUTH DAKOTA *** BY SESSION
##############################################################


###################################
## (SPECIAL) SESSIONS:
## ---- Special session recorded in seperate files; bill numbers restart
## ---- Bills do not carryover from regular session to regular session; again, numbers start from bottom (HB1001, SB1)
## MEMBER LISTS:
## ---- Search: http://sdlegislature.gov/Legislators/Historical_Listing/default.aspx?Session=2019
## ---- Can also use the session-specific guides, e.g., http://sdlegislature.gov/docs/legsession/2016/guide.pdf
## PROCESS/RULES:
## ---- 2017 Rules: http://sdlegislature.gov/docs/legsession/2017/2017Redbook.pdf
## Sponsorship/Authorship
## ---- One primary sponsor; multiple cosponsors permitted
## ---- Commiteee sponsored bills permitted
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
library(tibble)
library(foreach)
library(inexact)

this_state <- 'SD'
keep_types <- c('HB', 'SB')

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
# t <- terms[9]


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

######## Fix Bill(s) with missing/partial sponsors
if(t_yrs == "1997_1998"){
  bills[bills$bill_id == "HB1001" & bills$session == "1997-SS",]$all_sponsors <- gsub('Smidt, Va and', 'Smidt, Van Gerpen, Weber, and Wetz and', bills[bills$bill_id == "HB1001" & bills$session == "1997-SS",]$all_sponsors)
}

######## Subset to Term Data + Standardize the Bill IDs
bills <- bills %>%
  mutate(term = t_yrs,
         bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('^[A-Z]+', '', bill_id), 4, pad = '0'))) %>%
  select(-session_year) %>%
  rename(sponsors = all_sponsors) %>%
  mutate(by_request_of = str_trim(gsub('at the request of|^the | by request', '', str_extract(sponsors, "at the request of.+"))),
         primary_sponsor = gsub('  +', ' ', str_trim(gsub('by request', '', primary_sponsor))),
         sponsors = str_trim(gsub('at the request.+| by request', '', sponsors)))

############### Drop Resolutions, Messages, Communications, Reports
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
all_bills <- bills
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) 

##########################
####### Standardize Sponsors

bills$sponsors <- tolower(bills$sponsors)
bills$sponsors <- gsub('á', 'a', bills$sponsors)
bills$sponsors <- gsub('é', 'e', bills$sponsors)
bills$sponsors <- gsub('ó', 'o', bills$sponsors)
bills$sponsors <- gsub('í', 'i', bills$sponsors)
bills$sponsors <- gsub('ñ', 'n', bills$sponsors)

#### Manual Name Fixes:
if(t_yrs == '1997_1998'){
  bills$sponsors <- gsub('morford,', 'morford-burg,', bills$sponsors)
  bills$sponsors <- gsub('morford ', 'morford-burg ', bills$sponsors)   
  bills$sponsors <- gsub('morford$', 'morford-burg', bills$sponsors)
}else if(t_yrs == '1999_2000'){
  bills$sponsors <- gsub('fischer- clemens', 'fischer-clemens', bills$sponsors)
}else if(t_yrs == "2003_2004"){
  bills$sponsors <- gsub('ham-burr', 'ham', bills$sponsors)
  bills[bills$primary_sponsor == "Senator Ham-Burr",]$primary_sponsor <- 'Senator Ham'
}else if(t_yrs == "2007_2008"){
  bills$sponsors <- gsub('turbak berry', 'turbak', bills$sponsors)
  bills[bills$primary_sponsor == "Senator Turbak Berry",]$primary_sponsor <- 'Senator Turbak'
  ### First Names included despite no duplicates (though two are similar)
  bills$sponsors <- gsub('peterson \\(jim\\)', 'peterson', bills$sponsors)
  bills[bills$primary_sponsor == "Senator Peterson (Jim)",]$primary_sponsor <- 'Senator Peterson'
  bills$sponsors <- gsub('schmidt \\(dennis\\)', 'schmidt', bills$sponsors)
  bills[bills$primary_sponsor == "Senator Schmidt (Dennis)",]$primary_sponsor <- 'Senator Schmidt'
  bills$sponsors <- gsub('smidt \\(orville\\)', 'smidt', bills$sponsors)
  bills[bills$primary_sponsor == "Senator Smidt (Orville)",]$primary_sponsor <- 'Senator Smidt'
}else if(t_yrs == '2009_2010'){
  ### No other greenfield until 2015
  bills$sponsors <- gsub('greenfield \\(brock\\)', 'greenfield', bills$sponsors)
  #bills[bills$primary_sponsor == "Representative Greenfield (Brock)",]$primary_sponsor <- 'Representative Greenfield'
}else if(t_yrs == '2011_2012'){
  bills$sponsors <- gsub('jensen \\(phil\\)', 'jensen', bills$sponsors)
}else if(t_yrs == '2013_2014'){
  ### Coded as Netherton (married name) for single bill as cosponsor
  bills$sponsors <- gsub('netherton', 'haggar (jenna)', bills$sponsors)
  ### Both Buhl and Buhl O'donnell in this term
  bills$sponsors <- gsub("buhl o'donnell", "buhl", bills$sponsors)
  bills[bills$primary_sponsor == "Senator Buhl O'Donnell",]$primary_sponsor <- "Senator Buhl"
  ### Bunch of folks with first names even though no duplicate in chamber
  bills$sponsors <- gsub('greenfield \\(brock\\)', 'greenfield', bills$sponsors)
  bills$sponsors <- gsub('peterson \\(jim\\)', 'peterson', bills$sponsors)
  bills$sponsors <- gsub('jensen \\(phil\\)', 'jensen', bills$sponsors)
  bills$sponsors <- gsub('sutton \\(billie\\)', 'sutton', bills$sponsors)
  #### Chuck Jones appointed to Senate mid-term (Dec. 17, 2013) -- Doesn't sponsor until 2014
  bills[bills$session == '2013-RS',]$sponsors <- gsub(' jones$', ' jones (tom)', bills[bills$session == '2013-RS',]$sponsors)
  bills[bills$session == '2013-RS',]$sponsors <- gsub(' jones,', ' jones (tom),', bills[bills$session == '2013-RS',]$sponsors)
  bills[bills$session == '2013-RS',]$sponsors <- gsub(' jones and', ' jones (tom) and', bills[bills$session == '2013-RS',]$sponsors)
  bills[bills$primary_sponsor == "Senator Jones",]$primary_sponsor <- 'Senator Jones (Tom)'
}else if(t_yrs == '2015_2016'){
  # Again, Coded as Netherton (married name) for single bill as cosponsor
  bills$sponsors <- gsub('netherton', 'haggar (jenna)', bills$sponsors)
}else if(t_yrs == '2017_2018'){
  bills$sponsors <- gsub('nesibaand otten \\(ernie\\)', 'nesiba and otten (ernie)', bills$sponsors)
  bills[bills$primary_sponsor == 'Senator Nesibaand Otten (Ernie)',]$primary_sponsor <- 'Senator Nesiba'
}

### Get Cosponsors from Initiating Chamber
bills <- bills %>%
  mutate(chamber_sponsors = ifelse(substring(bill_id,1,1) == "H", gsub('and senator.+', '', sponsors), gsub('and represent.+', '', sponsors)),
         chamber_sponsors = ifelse(grepl('^the committee', sponsors) & !grepl('representative|senator', sponsors), '', chamber_sponsors),
         chamber_sponsors = gsub(', and | and |, ', '; ', gsub('^represe[a-z]+ |^senat[a-z]+ ', '', chamber_sponsors)))
bills$chamber_sponsors <- gsub('á', 'a', bills$chamber_sponsors)
bills$chamber_sponsors <- gsub('é', 'e', bills$chamber_sponsors)
bills$chamber_sponsors <- gsub('ó', 'o', bills$chamber_sponsors)
bills$chamber_sponsors <- gsub('í', 'i', bills$chamber_sponsors)
bills$chamber_sponsors <- gsub('ñ', 'n', bills$chamber_sponsors)

bills$primary_sponsor <- tolower(bills$primary_sponsor)
bills$primary_sponsor <- gsub('á', 'a', bills$primary_sponsor)
bills$primary_sponsor <- gsub('é', 'e', bills$primary_sponsor)
bills$primary_sponsor <- gsub('ó', 'o', bills$primary_sponsor)
bills$primary_sponsor <- gsub('í', 'i', bills$primary_sponsor)
bills$primary_sponsor <- gsub('ñ', 'n', bills$primary_sponsor)
bills$primary_sponsor <- gsub('^senator |^representative |^introduced by | \\(prime\\)', '', bills$primary_sponsor)

### LES Sponsor Var
bills <- rename(bills, LES_sponsor = primary_sponsor)
table(bills$LES_sponsor)



###################
###### Merge in S&S Bills
###################
# *** For SOUTH DAKOTA: Bills DO NOT carry over during regular or special sessions; restart at S1/HB1001
# ---> Adjusting Bill numbers using SESSION MAX and assuming bill is from SS with most bills proposed

if(t_yrs == "2019_2020"){
  SS_bills$bill_id[SS_bills$bill_id=="SB60006"]="SB0006"
}
if(t_yrs == "2021_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="SB30003"]="SB0003"
}

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, year, Title) %>% 
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
  left_join(bills %>% mutate(year = as.integer(substr(session,1,4))), 
            by = c("bill_id", "term","year")) %>%
  group_by(rowname) %>% mutate(count = n()) %>%
  ungroup() %>% 
  select(count,bill_id,term,year,session, Title, summary) %>% 
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

############################################################
############### Code Commemorative
######################################################

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

###### DROP COMMITTEE BILLS
if(nrow(filter(bills, grepl("committee", LES_sponsor))) > 0){
  cat('\n')
  cat(glue("-----> Dropping {nrow(filter(bills, grepl('committee', LES_sponsor)))} bill(s) sponsored by COMMITTEE"))
  cat('\n')
  bills <- filter(bills, !grepl('committee', LES_sponsor))     
}


################################################
############### Code Bill History
################################################

bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
bill_hist <- read.csv(bill_hist_path)

# If multiple sessions, read in those as well
if(length(t_sessions) > 1){
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
    s_hist <- read.csv(bill_path)
    bill_hist <- bind_rows(bill_hist, s_hist)
  }
  rm(s, s_hist)
}

######## Clean Term/Session Variables + Standardize the Bill IDs
bill_hist <- bill_hist %>%
  mutate(term = t_yrs,
         bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('^[A-Z]+', '', bill_id), 4, pad = '0'))) %>%
  select(-session_year)

### Order by Order
bill_hist <- bill_hist %>% 
  arrange(term, session, bill_id, action_date, order) %>%
  filter(bill_id %in% bills$bill_id)

## CODE CHAMBER
bill_hist <- bill_hist %>%
  group_by(term, session, bill_id) %>%
  mutate(chamber = ifelse(order == 1, substring(bill_id,1,1), NA),
         # Doing governor before chambers because these usually have HJ/SJ at end of line
         chamber = ifelse(is.na(chamber) & grepl("to the governor|by governor", tolower(action)), "G", chamber),
         chamber = ifelse(is.na(chamber) & grepl("^house|read in house|referred to house| h\\.j\\. [0-9]+", tolower(action)), "H", chamber),
         chamber = ifelse(is.na(chamber) & grepl("^senate|read in senate|referred to senate| s\\.j\\. [0-9]+", tolower(action)), "S", chamber)
  ) %>%
  fill(chamber) %>%
  ungroup()

### Re-Coding Chamber Variable
bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate", "G" = "Governor", "CC" = "Conference")

#### Adjusting Committee Report Actions
bill_hist <- bill_hist %>%
  mutate(action = tolower(action),
         action = gsub("\\s+", " ", action), 
         action = ifelse(grepl('do pass|do not pass|report without recommend|place on.+calendar', action) & !grepl("house|senate", action), paste0('committee: ', action), action))

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c('committee hearing', 'scheduled for hearing', '^(house|senate).+ amendment \\([a-z]-[0-9]+\\)',
           'committee.+(do pass|do not pass|without recommend)', '^committee:.+calendar')
# committee amendments distinguished by numbers in parentheticals at end of line
abc_t <- c('committee.+do pass.+passed', 'motion to amend.+(h\\.j\\.|s\\.j\\.)', '^(house|senate).+on calendar', 'second reading',
           '^house of representatives do pass', '^senate do pass', '^placed on consent',
           'motion to strike the \"not\"')
# Do not pass or without recommendation don't automatically get put on calendar; require motion on floor
# --> See rule 6f-6: http://sdlegislature.gov/docs/legsession/2017/2017Redbook.pdf
# motion to strike the not == floor action to convert a do not pass report into a do pass to be voted on by chamber
# Need to qualify motion to amend with hj/sj as committee motions to amend recorded in later years (and sometimes coded as house appropriations motion...)
pc_t  <- c('^house of representatives do pass.+passed', '^senate do pass.+passed')
law_t <- c('signed by governor', 'signed by the governor', 'delivered to sec. of state',
           'delivered veto override to the secretary of state')

### Check Actions
# filter(bill_hist, grepl('delivered veto override', tolower(action))) %>% distinct(action) %>% View()
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
  if(bill_stages$law == 0 & any(grepl('^house.+vetoed for style.+passed', hist_sub$action)) & any(grepl('^senate.+vetoed for style.+passed', hist_sub$action))){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- bill_stages$law <- 1
  }
  #### Check Overrides
  if(bill_stages$law == 0 & any(grepl('^house.+veto override passed', hist_sub$action)) & any(grepl('^senate.+veto override passed', hist_sub$action))){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- bill_stages$law <- 1
  }
  #### Committee Motions to Amend
  if(bill_stages$action_in_comm == 0 & any(grepl("motion to amend", hist_sub$action)) & !any(grepl('motion to amend.+(h\\.j\\.|s\\.j\\.)', hist_sub$action))){
    bill_stages$action_in_comm <- 1
  }
  #### Check Passages that are Reconsidered, Don't Leave Chamber
  if(bill_stages$passed_chamber == 1 & bill_stages$law == 0){
    if(any(grepl("(house of representatives|senate) reconsidered, passed", hist_sub$action)) & length(unique(hist_sub$chamber)) == 1){
      bill_stages$passed_chamber <- 0
    }
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
            sponsor_law_rate = sum(law) / n(),
            num_cosponsored_bills = NA) %>%
  ungroup()

#### Adding Legislators Who COSPONSORED A BILL but did not SPONSOR
unique_cospon <- str_trim(unique(unlist(str_split(bills$chamber_sponsors, '; '))))
unique_cospon = gsub(" \\(prime\\)","",unique_cospon)

for(nonspon in unique_cospon){
  if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != ''){
    ns_adj <- gsub('\\(', '\\\\(', gsub('\\)', '\\\\)', nonspon))
    chamb <- unique(substring(bills[grepl(ns_adj, bills$chamber_sponsors),]$bill_id, 1, 1))
    if(t_yrs %in% c('2009_2010', '2011_2012', '2013_2014') & nonspon == 'olson'){
      next
    }else if("H" %in% chamb & "S" %in% chamb){
      print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
    }else{
      all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0)
    }
  }
}

######## Cosponsorship Info 
bills$cospon_match <- paste(bills$LES_sponsor, bills$chamber_sponsors, sep = '; ')
for(i in 1:nrow(all_sponsors)){
  c_sub <- bills[substring(bills$bill_id, 1, 1) %in% ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S'),]
  sn <- gsub('\\(', '\\\\(', gsub('\\)', '\\\\)', all_sponsors[i,]$LES_sponsor))
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
all_sponsors$first_name <- ifelse(grepl('\\([a-z]+\\)', all_sponsors$LES_sponsor), str_extract(all_sponsors$LES_sponsor, '\\([a-z]+\\)'), '')
all_sponsors$first_name <- gsub('\\(|\\)', '', all_sponsors$first_name)
all_sponsors$last_name <- str_trim(gsub('\\([a-z]+\\)', '', all_sponsors$LES_sponsor))

all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### *****************************
#### Update Last Names for Matching 
if(t >= 2009 & t <= 2010){
  all_sponsors[all_sponsors$LES_sponsor == 'turbak berry',]$last_name <-  "berry"
}
if(t >= 2009 & t <= 2012){
  all_sponsors[all_sponsors$LES_sponsor == 'iron cloud iii',]$last_name <-  "iron cloud"
}
if(t >= 2015 & t <= 2016){
  all_sponsors[all_sponsors$LES_sponsor == "buhl o'donnell",]$last_name <-  "odonnell"
}
if(t >= 2017 & t <= 2018){
  all_sponsors[all_sponsors$LES_sponsor == "netherton",]$last_name <-  "haggar"
  all_sponsors[all_sponsors$LES_sponsor == "peterson (sue)",]$last_name <-  "lucas-peterson"
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


if(t_yrs == "2019_2020") {
  legiscan = legiscan %>% mutate(district = case_when(
    people_id == 14692 & role == "Rep" ~ "HD-019",
    people_id == 18695 & role == "Rep" ~ "HD-023",
    T ~ district))
}
if(t_yrs == "2021_2022") {
  View(legiscan %>% group_by(people_id) %>% mutate(count = n()) %>% filter(count > 1) %>% 
         group_by(people_id,role) %>% mutate(count = n()) %>% filter(count == 1) %>% arrange(name))
  legiscan = legiscan %>%
    filter(! (role == "Rep" & people_id %in% c(8296,8303,8319, 8324, 14418, 14501, 17145, 17826, 18692))) %>% 
    filter(! (role == "Sen" & people_id %in% c(14497 ))) 
}

legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = case_when(
    n >= 2 ~ paste0(last_name, " (",first_name,")"),
    T ~ last_name)) %>%
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
        "lucas-peterson-h" = "peterson (sue)-h",
        "lake (john)-h" = "lake-h",
        "lake (john)-s" = "lake-s",
        "kolbeck-s" = "kolbeck (jack)-s",
        "crabtree-s" = NA_character_,
        "schneider-h" = NA_character_,
        "johnson (david)-h" = "johnson-h",
        "rhoden-h" = NA_character_,
        "peterson-h" = "peterson (kent)-h"
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
        "lucas-peterson-h" = "peterson (sue)-h",
        "hoffman-h" = "hoffman (charlie)-h",
        "johnson (david)-s" = "johnson-s",
        "kolbeck-s" = "kolbeck (jack)-s",
        "peterson-h" = "peterson (kent)-h"
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
rm(t, terms, klarner_gs, nonspon, unique_cospon, t_sessions, chamb, m_sub, ns_adj)
rm(commem_bills, match_name2)

########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by GUBERNATORIAL APPOINTMENT --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
### Search Members (Historical): http://sdlegislature.gov/Legislators/Historical_Listing/default.aspx
#########################

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 280 bill(s) sponsored by COMMITTEE
### APPOINTED ~ HOUSE:
# -- BROWN (gary)
# -- SOLUM (burdette)
### APPOINTED ~ SENATE:
# -- BROSZ (don, via H)
# -- BROWN (arnold, past/via H)
### DROP:
# -- negstad, richard b. -- died January 11, 1997 -- https://www.geni.com/people/Richard-Negstad/6000000001498452366


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 208 bill(s) sponsored by COMMITTEE
### APPOINTED ~ HOUSE:
# -- EARLEY (william)
# -- HEINEMAN (phyllis)
### DROP:
# -- rost, judy -- resigned janury 4, 1999: http://sdlegislature.gov/Legislators/Historical_Listing/LegislatorDetail.aspx?MemberID=3123


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 255 bill(s) sponsored by COMMITTEE
### APPOINTED ~ SENATE:
# -- CRADDUCK (rebekah, Dec. 4, 2001, http://sdlegislature.gov/Legislators/Historical_Listing/LegislatorDetail.aspx?MemberID=3708)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 272 bill(s) sponsored by COMMITTEE
### APPOINTED ~ HOUSE:
# -- WEEMS (keri)
### APPOINTED ~ SENATE:
# -- JASPERS (mike, via H)
# -- KURTENBACH (AL, January 27, 2004)
# -- LAPOINTE (michael)
### DROP:
# -- jaspers, mike -- IN HOUSE -- appointed to senate after winning house seat but prior to be seated
# -- richter, mitch -- elected by never served
# -- hagen, richard (dick) --- died september 2002; elected despite this.


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 270 bill(s) sponsored by COMMITTEE
### APPOINTED ~ HOUSE:
# -- JERKE (gary)
#### DROP:
# -- vangerpen, billy l. -- resigned january 3, 2005 (but later elected again)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 326 bill(s) sponsored by COMMITTEE
### APPOINTED ~ HOUSE:
# -- GOSCH (brian)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 273 bill(s) sponsored by COMMITTEE
### APPOINTED ~ HOUSE:
# -- CONZET (kristin)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 242 bill(s) sponsored by COMMITTEE
### APPOINTED ~ HOUSE:
# -- SCOTT (dave, Nov. 2011)
### APPOINTED ~ SENATE:
# -- JUHNKE (kent, via H)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 273 bill(s) sponsored by COMMITTEE
### APPOINTED ~ HOUSE:
# -- ANDERSON (david)
# -- LANGER (kris)
### APPOINTED ~ SENATE:
# -- CURD (r. blake)
# -- SOLANO (alan)
# -- JONES (chuck) -- name duplicated, won't print


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 246 bill(s) sponsored by COMMITTEE
### APPOINTED ~ HOUSE:
# -- STEINHAUER (wayne)
### APPOINTED ~ SENATE:
# -- FIEGEN (scott)
# -- SHORMA (william)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 243 bill(s) sponsored by COMMITTEE
#### APPOINTED ~ HOUSE:
# -- BARTHEL (doug)
# -- DIEDRICH (michael)
# -- LUST (david)
# -- WIESE (marli)
#### DROP:
# -- dryden, dan -- died August 2016, but still won reelection (or already had)


# filter(klarner, grepl("buhl", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 124 & sen ==0 & year < 2000) %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)


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
LES[LES$data_name == 'fiegen' & LES$term == "2015_2016",]$klarner_id <- NA
LES[LES$data_name == 'fiegen' & LES$term == "2015_2016",]$klarner_name <- NA
LES[LES$data_name == 'fiegen' & LES$term == "2015_2016",]$sponsor <- 'fiegen, scott'

### ****Still missing*****
# still_missing <- unique(LES[is.na(LES$klarner_id),]$sponsor)
# name = still_missing[9]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl('barthel', cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, missing, still_missing)

### Fix Missing
name_matches <- data.frame(LES_name = 'solum', k_name = 'solum, burdette c.')
name_matches <- add_row(name_matches, LES_name = 'gosch', k_name = 'gosch, brian')
name_matches <- add_row(name_matches, LES_name = 'scott', k_name = 'scott, dave')
#name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')
for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}
rm(name_matches, i)

### Manual for Alan Solano -- Multiple Klarner IDs because of a klarner name error that is corrected above
LES[LES$sponsor == 'solano' & LES$term == "2013_2014",]$klarner_id <- 333122
LES[LES$sponsor == 'solano' & LES$term == "2013_2014",]$klarner_name <- 'solano, alan d.'
LES[LES$sponsor == 'solano' & LES$term == "2013_2014",]$sponsor <- 'solano, alan d.'

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
# filter(LES, is.na(party)) %>% select(sponsor, term, klarner_id, district, party, exper) %>% as.data.frame()
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

### Not In Klarner
fill_missing <- data.frame(LES_name = "brown, gary", new_name = 'brown, gary', party = 'r', district = 32, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "kurtenbach", new_name = 'kurtenbach, al', party = 'r', district = 4, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "fiegen, scott", new_name = 'fiegen, scott', party = 'r', district = 25, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "shorma", new_name = 'shorma, william j.', party = 'r', district = 16, exper = 'none')
for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

#### *** 2017_2018: If any of below run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "barthel", c('party', 'sponsor')] <- list('r', 'barthel, doug')
LES[LES$sponsor == "wiese", c('party', 'sponsor')] <- list('r', 'wiese, marli')

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)
rm(fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()

### Fix Names
LES[LES$sponsor %in% c('buhl, angie', 'odonnell, angie buhl'),]$sponsor <- "buhl-o'donnell, angie"
LES[LES$sponsor == 'vangerpen, billy l.',]$sponsor <- "van gerpen, billy l."
LES[LES$sponsor == 'vangerpen, edward',]$sponsor <- "van gerpen, edward"
LES[LES$sponsor == 'vanetten, donald d.',]$sponsor <- "van etten, donald d."
LES[LES$sponsor == 'vannorman, thomas james',]$sponsor <- "van norman, thomas james"
LES[LES$sponsor %in% c('turbak, nancy j.', 'berry, nancy turbak'),]$sponsor <- "turbak-berry, nancy j."
LES[LES$sponsor == 'lucaspeterson, sue k.',]$sponsor <- "lucas-peterson, sue k."
LES[LES$sponsor == 'haggar, jenna',]$sponsor <- "netherton-haggar, jenna j."

### Cleaner
LES[LES$sponsor == 'nesselhuf, b. j. (bj)',]$sponsor <- "nesselhuf, benjamin j."
LES[LES$sponsor == 'curd, r. blake',]$sponsor <- "curd, richard blake"
LES[LES$sponsor == 'wollmann, mathew',]$sponsor <- "wollmann, matthew"

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

# **** A HANDFUL OF 2015-2016 DUPLICATES ---> Eliminating for nwo...
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

LES[LES$sponsor %in% c('johnson, douglas w.', 'johnson, william j.', 'johnson, david'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# Notable Names: Marion 'Michael' Rounds; Walter 'Dale' Slaughter (seemingly, times overlap)
# -- J. Johnston = "John Mark Johnston"
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('thompson$', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'cutler, steven (steve)', SM_name = 'Cutler')
name_matches <- add_row(name_matches, LES_name = 'fiegen, kristie', SM_name = 'Fiegen')
#name_matches <- add_row(name_matches, LES_name = 'greenfield, lana j.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'ham, arlene h.', SM_name = 'Ham-Burr, Arlene')
#name_matches <- add_row(name_matches, LES_name = 'haverly, terri', SM_name = 'zzzzzzz')
#name_matches <- add_row(name_matches, LES_name = 'johnson, douglas w.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'johnson, william j.', SM_name = 'Johnson') # record from Senate == william
name_matches <- add_row(name_matches, LES_name = 'otten, ernie jr.', SM_name = 'Otten, Ernie') # Senator; Jr row is House
name_matches <- add_row(name_matches, LES_name = 'peterson, bill', SM_name = 'Peterson, William')
name_matches <- add_row(name_matches, LES_name = 'thompson, bill', SM_name = 'Thompson, William')
name_matches <- add_row(name_matches, LES_name = 'thompson, jim d.', SM_name = 'Thompson')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

### Names with 2015-2016 Duplicates (Eliminated above, but may be needed if change process)
#name_matches <- add_row(name_matches, LES_name = 'munson, david r.', SM_name = 'zzzzzzz')
#name_matches <- add_row(name_matches, LES_name = 'novstrup, david', SM_name = 'zzzzzzz')
#name_matches <- add_row(name_matches, LES_name = 'schaefer, james', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

########
## Incorrect SM Names but correct time period:

LES[LES$sponsor == 'otten, herman',]$SM_name  <- paste0(ideo[ideo$name == 'Otten, Ernest Jr.',]$name, " -- ***")
LES[LES$sponsor == 'otten, herman',]$SM_party <- ideo[ideo$name == 'Otten, Ernest Jr.',]$party
LES[LES$sponsor == 'otten, herman',]$np_score <- ideo[ideo$name == 'Otten, Ernest Jr.',]$np_score

## Same but there's also a Edwin Olson Jr. row that is a Rep (Mel is a Dem)
LES[LES$sponsor == 'olson, mel',]$SM_name  <- paste0(ideo[ideo$name == 'Olson, Edwin Jr.' & ideo$party == "D",]$name, " -- ***")
LES[LES$sponsor == 'olson, mel',]$SM_party <- ideo[ideo$name == 'Olson, Edwin Jr.' & ideo$party == "D",]$party
LES[LES$sponsor == 'olson, mel',]$np_score <- ideo[ideo$name == 'Olson, Edwin Jr.' & ideo$party == "D",]$np_score

########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########

### James Bradford -- Dem to Rep to Dem -- basically was a Republican for 2008 election and switched back in Oct 2010
LES[LES$sponsor == "bradford, james" & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Bradford, Jim' & ideo$party == 'R',]$name
LES[LES$sponsor == "bradford, james" & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Bradford, Jim' & ideo$party == 'R',]$party
LES[LES$sponsor == "bradford, james" & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Bradford, Jim' & ideo$party == 'R',]$np_score
LES[LES$sponsor == "bradford, james" & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Bradford, Jim' & ideo$party == 'D',]$name
LES[LES$sponsor == "bradford, james" & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Bradford, Jim' & ideo$party == 'D',]$party
LES[LES$sponsor == "bradford, james" & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Bradford, Jim' & ideo$party == 'D',]$np_score

### Ryan maher -- Dem to Rep. in 2011
LES[LES$sponsor == "maher, ryan" & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Maher, Ryan' & ideo$party == 'R',]$name
LES[LES$sponsor == "maher, ryan" & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Maher, Ryan' & ideo$party == 'R',]$party
LES[LES$sponsor == "maher, ryan" & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Maher, Ryan' & ideo$party == 'R',]$np_score
LES[LES$sponsor == "maher, ryan" & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Maher, Ryan' & ideo$party == 'D',]$name
LES[LES$sponsor == "maher, ryan" & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Maher, Ryan' & ideo$party == 'D',]$party
LES[LES$sponsor == "maher, ryan" & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Maher, Ryan' & ideo$party == 'D',]$np_score

# *** Jenna Netherton/Haggar --- Won as I, SM has her as I for 2011-2012, but must have become R in 2011 when seated
# ---> ideology scores bascially the same, however
LES[LES$sponsor == "netherton-haggar, jenna j." & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Haggar, Jenna' & ideo$party == 'R',]$name
LES[LES$sponsor == "netherton-haggar, jenna j." & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Haggar, Jenna' & ideo$party == 'R',]$party
LES[LES$sponsor == "netherton-haggar, jenna j." & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Haggar, Jenna' & ideo$party == 'R',]$np_score
LES[LES$sponsor == "netherton-haggar, jenna j." & LES$party == 'nonmaj',]$SM_name <-  ideo[ideo$name == 'Haggar, Jenna' & ideo$party == 'X',]$name
LES[LES$sponsor == "netherton-haggar, jenna j." & LES$party == 'nonmaj',]$SM_party <- ideo[ideo$name == 'Haggar, Jenna' & ideo$party == 'X',]$party
LES[LES$sponsor == "netherton-haggar, jenna j." & LES$party == 'nonmaj',]$np_score <- ideo[ideo$name == 'Haggar, Jenna' & ideo$party == 'X',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()

##### Drop Nicknames
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]$', sponsor))
LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1997 - 2020
#LES[as.numeric(substring(LES$term,1,4)) %in% c(2007:2020) & LES$chamber == 'House' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1997 - 2020
#LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:1994) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
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
  scale_color_manual(values=c("dodgerblue2",  'gray50', "red2", 'gray50'))

##### CHECK OUTLIERS ---> ALL REMAINING PARTIES MATCH THOSE IN SHOR-MCCARTY DATA
# filter(LES, party == 'd' & np_score > 0) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & np_score < 0) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

