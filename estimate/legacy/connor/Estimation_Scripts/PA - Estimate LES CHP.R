
############################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** PENNSYLVANIA *** BY SESSION
############################################################

###################################
## SPECIAL SESSIONS:
## --- Bills are introduced in the regular session after the special session... But bill numbers restart at one so need to merge on id and session
## MEMBER LISTS:
## --- https://www.legis.state.pa.us/cfdocs/legis/BiosHistory/ViewAll.cfm?body=H
## --- https://www.legis.state.pa.us/cfdocs/legis/BiosHistory/ViewAll.cfm?body=S
###########################
## NOTES:
# (1) NOT filling in "prime sponsor withdrew" with first cosponsor
# (2) THERE ARE RESOLUTIONS THAT ARE LISTED AS HB --- E.G, 1991 HB1 --- "A Joint Resolution" vs "An Act..."
# -------> Dropping for now -- mostly seem to be joint resolutions for constitutional amendments
#####################


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
library(inexact)
library(readr)
library(foreach)
library(tibble)

this_state <- 'PA'
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
terms <- 2023
t = terms
t_plus_one = t + 1
t_yrs <- as.numeric(terms)
t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 1}'))
t_sessions = sessions[startsWith(sessions,as.character(t)) | startsWith(sessions,as.character(t+1))]



#### COMMEMORATIVE BILLS ####
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv")) %>%
  mutate(session = gsub("_0", "-RS", gsub("_1", "-SS1", gsub("_2", "-SS2", session))))

#### SUBSTANTIVE AND SIGNIFICANT BILLS ####
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
# t <- terms[10]

### Formulate 2-year terms -- Cover both regular and special sessions


###### SESSION IN PROGRESS
print(glue('\n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} session! ~~~~~~~~~~~~~~ '))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_sessions[1]}.csv")
bills <- read.csv(bill_path)   

### If multiple sessions, read in those as well
if(length(t_sessions) > 1){  
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{s}.csv")
    s_bills <- read.csv(bill_path)   
    bills <- bind_rows(bills, s_bills)
  }
  rm(s, s_bills)
}

### Check for duplicates
if(nrow(bills) != nrow(distinct(bills))){
  cat(" \n ~~~> DUPLICATE BILLS \n\n .")
  bills = distinct(bills)
}

######## Standardize the Bill IDs + Session IDs
bills <- rename(bills, bill_id = bill_number) %>%
  mutate(term = t_yrs,
         session = gsub("_0", "-RS", gsub("_1", "-SS1", gsub("_2", "-SS2", session))))

############### Drop Resolutions, Messages, Communications, Reports
all_bills <- bills
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)


###################
##### Standardize Sponsors
bills$sponsor <- str_trim(gsub('representative |senator ', '', tolower(bills$sponsor)))
if (as.numeric(substring(t_yrs, 1, 4)) < 2023){
  bills$all_sponsors <- tolower(bills$all_sponsors)
} else if (as.numeric(substring(t_yrs, 1, 4)) >= 2023){
  bills$all_sponsors <- str_trim(gsub('representative |senator |rep. |sen. ', '', tolower(bills$all_sponsors)))
}

######### Bills By Request
if(any(grepl('by request', bills$sponsor))){
  print(glue("-----> KEEPING {nrow(filter(bills, grepl('by request', sponsor)))} bill(s) introduced BY REQUEST"))
  bills$sponsor <- str_trim(gsub('\\(by request\\)', '', bills$sponsor))
}


#### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
bills$sponsor <- gsub('á', 'a', bills$sponsor)
bills$sponsor <- gsub('é', 'e', bills$sponsor)
bills$sponsor <- gsub('ó', 'o', bills$sponsor)
bills$sponsor <- gsub('í', 'i', bills$sponsor)
bills$sponsor <- gsub('ñ', 'n', bills$sponsor)

bills$all_sponsors <- gsub('á', 'a', bills$all_sponsors)
bills$all_sponsors <- gsub('é', 'e', bills$all_sponsors)
bills$all_sponsors <- gsub('ó', 'o', bills$all_sponsors)
bills$all_sponsors <- gsub('í', 'i', bills$all_sponsors)
bills$all_sponsors <- gsub('ñ', 'n', bills$all_sponsors)



#### LES SPONSOR VARIABLE
bills$LES_sponsor <- bills$sponsor
table(bills$LES_sponsor)

###################
###### Merge in S&S Bills
###################
# *** For PENNSYLVANIA: Bills carry over during regular (one biennium), but numbers re-start for all special sessions
# ---> Need to merge on Id and Session 
# ---> Capping Bill numbers at SESSION MAX and assuming bill is from SS with most bills proposed


if(t_yrs == "2021_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="SB20002"]="SB0002"
}
if(t_yrs == "2023_2024"){
  SS_bills$bill_id[SS_bills$bill_id=="SB70007"]="SB0007"
  SS_bills$bill_id[SS_bills$bill_id=="SB80008"]="SB0008"
  SS_bills$bill_id[SS_bills$bill_id=="HB10001"]="HB0001"
  SS_bills$bill_id[SS_bills$bill_id=="HB20002"]="HB0002"
  SS_bills$bill_id[SS_bills$bill_id=="SB10001"]="SB0001"
}


SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,sponsor), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% c("HB","SB")) %>% select(-bill_type) %>%
  filter(! grepl("committee",sponsor, ignore.case=T)) %>%
  arrange(sponsor) 
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

##############################################
############### Code Commemorative
##############################################

bills <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  inner_join(bills, ., by = c('bill_id', 'term', 'session'))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)


##### Drop Joint Resolutions coded as HB/SB
bills <- filter(bills, !grepl('^a joint resolution|^a conc[a-z]+ resolution', tolower(title) ))

### Drop Bills With No Prime Sponsor
bills <- filter(bills, !(sponsor %in% c('prime sponsor withdrew', 'sponsors withdrawn', 'withdrawn') ) )

###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "") %>% View()
if(nrow(filter(bills, sponsor == '')) > 0){
  print(glue("-----> Dropping {nrow(filter(bills, sponsor == ''))} bills without a sponsor"))
  bills <- filter(bills, !(sponsor == ''))     
}

##############################################
############### Code Bill History
##############################################

bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
bill_hist <- read.csv(bill_hist_path)

### If multiple sessions, read in those as well
if(length(t_sessions) > 1){  
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
    s_hist <- read.csv(bill_path)   
    bill_hist <- bind_rows(bill_hist, s_hist)
  }
  rm(s, s_hist)
}

######## Standardize BillHist Bill IDs
bill_hist <- rename(bill_hist, bill_id = bill_number) %>%
  mutate(term = t_yrs, 
         session = gsub("_0", "-RS", gsub("_1", "-SS1", gsub("_2", "-SS2", session))))

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
# ** Could count laid on the table or dropped from calendar as abc... but if it never comes of the table...
# ** Should we just assume reported = AIC here? Discharges don't record as reported
aic_t <- c('^reported', 'reported as committed', 'reported as amended', "reported with request") ## ^reported captures the 3 that follow
abc_t <- c("^reported", "removed from table", "^re-referred", "^re-committed", "second consideration", "third consideration", 
           "defeated on final", "vote.+ reconsider", "placed on.+calendar")
pc_t <- c("final passage", "signed in")
law_t <- c('approved by the gov', 'act no\\. [0-9]')
# IF keeping resolutions coded as HB's --- Add 'pamphlet laws resolution' + 'passed sessions of'

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
  if(bill_stages$law == 0 & grepl("^Act No\\. [0-9]+", bills[i,]$status)){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- bill_stages$law <- 1
  }
  bill_stages$bill_url <- bills[i,]$bill_url
  all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
  #print(i)
}
options(warn = 1)

### This code will mistakenly mark bills that are defeated on final passage as passing their parent chamber due to 
### "Third consideration and defeated on final passage" (and, sometimes, "Motion to reconsider final passage") in history

if (t_yrs == "2023_2024"){
  all_bill_stages$passed_chamber[all_bill_stages$bill_id == "HB0335"] <- 0
  all_bill_stages$passed_chamber[all_bill_stages$bill_id == "HB0338"] <- 0
  all_bill_stages$passed_chamber[all_bill_stages$bill_id == "HB1458"] <- 0
  all_bill_stages$passed_chamber[all_bill_stages$bill_id == "HB1459"] <- 0
  all_bill_stages$passed_chamber[all_bill_stages$bill_id == "HB2206"] <- 0
}

for(j in 1:nrow(bills)){
  id = bills$bill_id[j]
  bill_history <- filter(bill_hist, bill_id == id)
  init_chamber <- ifelse(substring(tolower(id), 1, 1) == "s", "Senate", "House")
  other_chamber <- ifelse(init_chamber == "House", "Senate", "House")
  in_chamber <- which(bill_history$chamber == init_chamber)
  not_in_chamber <- which(bill_history$chamber == other_chamber)
  chamber_switch <- not_in_chamber[which(not_in_chamber > min(in_chamber))]
  if(length(chamber_switch) > 0) {
    chamber_history <- bill_history[bill_history$order < min(chamber_switch),]
  } else {
    chamber_history <- bill_history
  }  
  if (sum(grepl("defeated on final passage|motion to reconsider final passage", tolower(chamber_history$action)) > 0)) {
    print(glue("CHECK FOR FAILED FINAL PASSAGE VOTE ({id})"))
  }
}

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
  select(bill_id, term, session, SS) %>%
  left_join(all_bill_stages, ., by = c('bill_id', 'term', "session")) %>%
  mutate(SS = ifelse(is.na(SS), 0, SS))

### Adjust Commems if SS == 1
table(all_bill_stages$SS, all_bill_stages$commem)
all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
rm(SS_term)

# ### Save Stage Info **** MERGE WITH SS **********
# if(!dir.exists(glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}'))){
#   dir.create(glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}'))
# }
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
  group_by(LES_sponsor, term, chamber) %>%
  summarize(num_sponsored_bills = n(), 
            sponsor_pass_rate = sum(passed_chamber) / n(),
            sponsor_law_rate = sum(law) / n()) %>%
  ungroup()

######## Cosponsorship Info
all_sponsors$num_cosponsored_bills <- NA
bills$cospon_match <- paste(bills$LES_sponsor, bills$all_sponsors, sep = '; ')
for(i in 1:nrow(all_sponsors)){
  c = ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')
  c_sub <- filter(bills, substring(bill_id, 1, 1) == c)
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
  ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
}
bills <- select(bills, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))
rm(c, c_sub)


if(min(all_sponsors$num_cosponsored_bills) < 0){
  print("negative cosponsors"); break
} else{print("cosponsors valid")}

##############
### CLEAN NAMES
############
all_sponsors <- left_join(all_sponsors, map_df(all_sponsors$LES_sponsor, parse_names), by = c("LES_sponsor" = "full_name")) %>%
  select(-salutation) %>%
  mutate(last_name = ifelse(is.na(last_name), first_name, last_name),
         first_name = ifelse(!is.na(first_name) & first_name == last_name, '', first_name)) %>%
  arrange(chamber, LES_sponsor) %>%
  distinct() ## If someone switches chambers, merge above will double them up

all_sponsors$first_name <- gsub('\\.', '', all_sponsors$first_name)
all_sponsors$middle_name <- gsub('\\.', '', all_sponsors$middle_name)

### Fix Two-Word Last Names
all_sponsors$last_name <- ifelse(all_sponsors$first_name == "van", paste0('van ', all_sponsors$last_name), all_sponsors$last_name)
all_sponsors$last_name <- ifelse(all_sponsors$first_name == "de", paste0('de ', all_sponsors$last_name), all_sponsors$last_name)
all_sponsors$first_name <- ifelse(all_sponsors$first_name %in% c("van", "de"), '', all_sponsors$first_name)




all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(LES_sponsor,substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name,middle_name,suffix))

legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(t)) | startsWith(legiscan_sessions,as.character(t+1))]

legiscan = foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% select(-c(votesmart_id,knowwho_pid, ballotpedia)) %>%  distinct() 


# special from house to senate
if(t_yrs == "2021_2022"){
  legiscan$role[legiscan$people_id==14146] == "Sen"
  legiscan = bind_rows(legiscan,
                       legiscan %>% filter(people_id == 14146) %>% 
                         mutate(role = "Rep", district = "HD-113"))
} else if (t_yrs == "2023_2024"){
  legiscan$role[legiscan$people_id==14146] == "Sen"
  legiscan = bind_rows(legiscan,
                       legiscan %>% filter(people_id == 14146) %>% 
                         mutate(role = "Rep", district = "HD-113"))
}


# Formatting of name changes beginning in 2023-2024

if (t < 2023){
  legiscan_adj = legiscan %>% 
    filter(committee_id == 0) %>% 
    group_by(last_name,role) %>% 
    mutate(n = n()) %>%
    ungroup() %>% 
    mutate(match_name = case_when(
      n > 2 ~ paste0(substr(first_name,1,1), substr(middle_name,1,1)," ",last_name),
      n == 2 ~  paste(substr(first_name,1,1),last_name),
      T ~ last_name)) %>%
    group_by(match_name,role) %>% 
    mutate(n2 = n()) %>% 
    mutate(match_name = ifelse(n2 > 1, paste0(substr(first_name,1,1), substr(middle_name,1,1)," ",last_name),
                               match_name)) %>% 
    mutate(match_name_chamber = tolower(paste(match_name,substr(district,1,1),sep="-")))
} else {
  legiscan_adj = legiscan %>% 
    filter(committee_id == 0) %>% 
    mutate(n = n()) %>%
    ungroup() %>% 
    mutate(match_name = paste(first_name,last_name)) %>%
    mutate(match_name_chamber = tolower(paste(match_name,substr(district,1,1),sep="-")))
}

# inexact::inexact_addin()
# left: legiscan_adj
# right: all_sponsors
# method: osa
# mode: full

if(t_yrs == "2019_2020"){
  all_sponsors2 = # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "davanzo-h" = NA_character_,
        "ellis-h" = NA_character_,
        "m keller-h" = "m. k. keller-h",
        "alloway-s" = NA_character_
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
        "reese-h" = NA_character_,
        "arnold-s" = NA_character_,
        "welby-h" = NA_character_,
        "dillon-s" = NA_character_,
        "blake-s" = NA_character_,
        "curry-h" = NA_character_
      )
    )
  
  
}

# A few notes regarding the Legiscan data: Craig Williams is "w williams-h" bc his first name is Wendell;
# Milou Mackenzie is "v mackenzie-h" bc her first name is Victoria; 
# Amen Brown is just "brown-h" because he's technically the only Brown in the House (Marla Gallo Brown is "gallo brown-h");
# Mike Jones is "p jones-h" bc his first name is Paul

if(t_yrs == "2023_2024"){
  all_sponsors2 = # You added custom matches:
    # inexact::inexact_join(
    #   x  = legiscan_adj,
    #   y  = all_sponsors,
    #   by = "match_name_chamber",
    #   method = "osa",
    #   mode = "full",
    #   custom_match = c(
    #     "schlegel culver-s" = "culver-s",
    #     "schlegel culver-h" = NA_character_,
    #     "olsommer-h" = NA_character_,
    #     "bradford-h" = NA_character_,
    #     "gallo brown-h" = "m. brown-h",
    #     "gillen-h" = NA_character_,
    #     "davis-h" = "t. davis-h",
    #     "carroll-h" = NA_character_,
    #     "zabel-h" = NA_character_,
    #     "k harris-h" = NA_character_,
    #     "brown-h" = "a. brown-h"
    #   )
    # )
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "jeffrey olsommer-h" = NA_character_,
        "matthew bradford-h" = NA_character_,
        "andre carroll-h" = NA_character_,
        "michael zabel-h" = NA_character_,
        "wendell williams-h" = "craig williams-h",
        "keith harris-h" = NA_character_,
        "mark gillen-h" = NA_character_,
        "v. mackenzie-h" = "milou mackenzie-h",
        "robert mercuri-h" = "robert w. mercuri-h",
        "lynda schlegel culver-h" = NA_character_
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


##############################
##### Estimate Scores + Add in Relatd Variables

### Check if bills in data without an ID'd sponsor
filter(bills, !(bills$LES_sponsor %in% legis_data$data_name))
bills <- select(bills, -sponsor) %>%
  rename(sponsor = LES_sponsor) %>%
  mutate(chamber = substring(bill_id, 1, 1))

### Standard LES: Same as Congressional Measure
source('../../Estimate LES/calc_LES_fx.R')

LES <- calc_LES(bills, legis_data, t_yrs, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1))
summ_stats <- LES %>% group_by(chamber) %>% summarize(mean_LES = mean(LES))

### Need to use this isTRUE business otherwise will sometimes return 1 != 1 -- https://stackoverflow.com/questions/9508518/why-are-these-numbers-not-equal
if(!isTRUE(all.equal(sum(summ_stats$mean_LES), nrow(summ_stats)))){
  print("----> CHECK LES --- MEAN != 1 ---> BREAK")
  print(summ_stats)
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


cat(glue(". \n  **************** TERM {t_yrs} ~~> DONE  ***********************"))
cat('\n __________________________________________________________ \n')


################# ****** END LOOP

#agg_stats, matches
rm(all_sponsors, bills, legis_data, SS_bills, elec_year, i, keep_types)
rm(k_matches, klarner_sub, m_sub, km, bill_path, t, t_yrs, calc_LES)
rm(commem_bills, t_sessions)

########################################################################################################################################################
########################################################################################################################################################
######## NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1989_1990 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 4 bill(s) introduced BY REQUEST
### Won Special:
# -- MIHALICH 
# -- PESCI 
# -- LAVELLE -- won June 1990 special for Senate (hence missingness in 1991/92 as well)
### IN CHAMBER: 
# -- BLACK; SNYDER; HOWLETT
### DROP:
# -- Livengood -- died in 1988, after winning reelection

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1991_1992 session! ~~~~~~~~~~~~~~ 
### Won Special:
# -- LAVELLE (june 1990, term throuogh 1992)
### IN CHAMBER: 
# -- RIEGER (william)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1993_1994 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 1 bill(s) introduced BY REQUEST
### Won Special:
# -- BURNS  -- Won 1994 Special, lost Subsequent election ****** LOTS OF BURNS in DATA -- NEED TO MAKE SURE SHE MATCHES RIGHT POST-HOC
# -- HECKLER  -- resigned from House in 1993 (August), Elected to Senate in special
# -- MARKS  --  seated after Stinson was removed; only served one term...---> https://www.legis.state.pa.us/cfdocs/legis/BiosHistory/MemBio.cfm?ID=5552&body=S
# -- STINSON  --- Won 1993 Special, Removed 2/18/1994 after judge declared him loser as a result of fraud
### IN CHAMBER: 
# -- BEBKO-JONES; HENNESSEY; BUTKOWITZ; RIEGER; LINTON


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1996 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 3 bill(s) introduced BY REQUEST
### Won Special:
# -- HASTE; MYERS (john); COSTA; HUGHES (via H); PICCOLA; THOMPSON
### IN CHAMBER: 
# -- RIEGER; CARN


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 1 bill(s) introduced BY REQUEST
### Won Special:
# -- HARHAI; MAHER; MCILHINNEY; CONTI (via H)
### IN CHAMBER: 
# -- RYAN (matthew, speaker)
# -- RIEGER
# -- JOSEPHS
### NAME Change:
# -- Teresa FORCIER from BROWN

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 session! ~~~~~~~~~~~~~~ 
### Won Special:
# -- WANSACZ  -- Elected in June 2000 special
# -- WATERS  -- Elected in May 1999 special
### IN CHAMBER: 
# -- SHANER; RYAN; RIEGER


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 1 bill(s) introduced BY REQUEST
### Won Special:
# -- BROOKS; SCAVELLO; TURZAI; ERICKSON; ORIE
### IN CHAMBER: 
# -- SHANER; RIEGER; KELLER; HORSEY; MANDERINO
# --- Odd though: Shaner cosponsored 1986 pieces of legislation! but no primary sponsorship...
# https://www.legis.state.pa.us/cfdocs/legis/BS/bs_action.cfm?mbrBody=H&SessID=20010&Sponsors=H%7C52%7C0%7CJAMES+E.+SHANER


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 session! ~~~~~~~~~~~~~~ 
### Won Special:
# -- DENLINGER + GOOD + KILLION + MILLARD + MUSTIO ~~ House
# -- GORDNER + PIPPY + PILEGGI ~~~ Senate
### IN CHAMBER: 
# -- FABRIZIO; KOTIK; SHANER; RYAN; RIEGER
# ---------> Note: Ryan (speaker) --- died in office --- March 29, 2003
### DROP:
# -- ZIMMERMAN -- Died in Nov 2002 prior to taking oath of office but after election


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 session! ~~~~~~~~~~~~~~ 
### Won Special:
# -- BEYER + FLAHERTY + PARKER + SIPTROTH ~~~ House
# -- BROWNE (via H); DINNIMAN; FONTANA; WASHINGTON (via H) ~~~ Senate
### IN CHAMBER: 
# -- KOTIK; SHANER; SAMUELSON; RIEGER
### DROP:
# -- LEWIS --- Resigned from House post-election in Dec. 2004 




# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 session! ~~~~~~~~~~~~~~ 
# -----> KEEPING 2 bill(s) introduced BY REQUEST
### Won Special:
# -- DINNIMAN (T-1)
### IN CHAMBER: 
# -- KOTIK



# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 session! ~~~~~~~~~~~~~~ 
### Won Special:
# -- KNOWLES; ARGALL (via H); MENSCH (via H)
### IN CHAMBER: 
# -- TRUE; REESE; QUIGLEY
### DROP:
# -- RHOADES (james) -- died in office (senate) in October 2008
### NAME Note:
# -- Vanessa Brown coded as Vanessa Lowerybrown in Klarner


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 session! ~~~~~~~~~~~~~~ 
### Won Special:
# -- DEANCUNNANE + JAMES + MACKENZIE + NEILSON + SCHMOTZER ~~~ House
# -- ARGALL (T-1) + BREWSTER + SCHWANK


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 session! ~~~~~~~~~~~~~~ 
### Won Special:
# -- SCHREIBER 
# -- TOPPER
# -- VULAKOVICH 
# -- MILLER D. -- Name duplicated, won't print
### IN CHAMBER: 
# -- KOTIK + DELISSIO + KINSEY
### DROP:
# -- DEPASQUALE -- RESIGNED January 13, 2013 


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 session! ~~~~~~~~~~~~~~ 
### Won Special:
# -- BULLOCK 
# -- COOK-ARTIS
# -- KRUEGER-BRANEKY
# -- MCCLINTON 
# -- NEILSON -- Resigned in mid-2014, Won Philly Council seat, Then Elected to PA House again in Special in 2015... Odd..
# -- ROTHAM 
# -- WHITE = MARTINA WHITE --- Won 2015 special --- 2016 Reelection in Klarner as Mary Jo White (who was def not in chamber)
# -- KILLION won Senate special; resigned from House May 10, 2016
# -- RESCHENTHALER
# -- SABATINA won Senate special; resigned from House June 9, 2015
### IN CHAMBER: 
# -- BRADFORD + SABATINA (until June 9..)
### DROP:
# -- BOYLE (brendan) -- took Congressional seat --- Never seated
# -- EVANS (dwight) -- took Congressional seat --- Never seated


##  ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 session! ~~~~~~~~~~~~~~ 
### Won Special:
# -- OWLETT + O'NEAL + TAI (May 2018 specials)
# -- VAZQUEZ (March 2017)
### In Chamber:
# -- GERGERLY -- Resigned Nov 2017
# -- MAHER
# -- BRADFORD
# -- MCNEILL (daniel) -- died Sep 2017 (replaced by jeanne mcneill)
# -- VITALI
#### DROP:
# ACOSTA = Resigned January 3, 2017 -- Never seated



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

###### Manual Fixes
# BARBARA BURNS --- PA 1993-1994 --- LOSER in 1994 Nov Election
LES[LES$data_name %in% "burns" & LES$term %in% "1993_1994",]$sponsor <- 'burns, barbara'
# ** No Klarner records...

### Wallis Brooks -- Won Feb. 2002 special, lost subsequent campaign
LES[LES$data_name %in% "brooks" & LES$term %in% "2001_2002",]$klarner_id <- 195266
LES[LES$data_name %in% "brooks" & LES$term %in% "2001_2002",]$klarner_name <- "brooks, wallis"
LES[LES$data_name %in% "brooks" & LES$term %in% "2001_2002",]$sponsor <- "brooks, wallis w."

### Martin Schmotzer, appointed April 2012, lost subsequent campaign but not in klarner --> Mismatched to Losing Candidate, Amy Schmotzer
LES[LES$data_name %in% "schmotzer" & LES$term %in% "2011_2012",]$klarner_id <- NA
LES[LES$data_name %in% "schmotzer" & LES$term %in% "2011_2012",]$klarner_name <- NA
LES[LES$data_name %in% "schmotzer" & LES$term %in% "2011_2012",]$sponsor <- "schmotzer, martin michael"

### Won in 2018 special -- will fix itself with updated klarner_data
LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_id <- NA
LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_name <- NA
LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$sponsor <- "owlett, clint"

### Won in 2018 special -- will fix itself with updated klarner_data
LES[LES$data_name %in% "j. mcneill" & LES$term %in% "2017_2018",]$klarner_id <- NA
LES[LES$data_name %in% "j. mcneill" & LES$term %in% "2017_2018",]$klarner_name <- NA
LES[LES$data_name %in% "j. mcneill" & LES$term %in% "2017_2018",]$sponsor <- "mcneill, jeanne"


### FOUR Still missing = Elected in Late Specials
### Burns, Haste, Martina White = NOT IN KLARNER
### Gayle Wright + Tonyelle Cook-Artis= Won special, lost general = Not in Klarner
### Last 5 more recent
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[8]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, still_missing)


## Bruce Marks -- 1993_1994 --- Took office upon declaration of fraud by winner Stinson (https://www.legis.state.pa.us/cfdocs/legis/BiosHistory/MemBio.cfm?ID=4985&body=S)
LES[LES$sponsor %in% "marks" & LES$term %in% "1993_1994",]$klarner_id <- 192628
LES[LES$sponsor %in% "marks" & LES$term %in% "1993_1994",]$klarner_name <- "marks, bruce s."
LES[LES$sponsor %in% "marks" & LES$term %in% "1993_1994",]$sponsor <- "marks, bruce s."

### Jay Costa Jr --- Elected in 1996 Special for Senate
LES[LES$sponsor %in% "costa" & LES$term %in% "1995_1996",]$klarner_id <- 193129
LES[LES$sponsor %in% "costa" & LES$term %in% "1995_1996",]$klarner_name <- "costa, jay jr. 1"
LES[LES$sponsor %in% "costa" & LES$term %in% "1995_1996",]$sponsor <- "costa, jay jr. 1"

### Cherelle Parker
LES[LES$sponsor %in% "parker" & LES$term %in% "2005_2006",]$klarner_id <- 279385
LES[LES$sponsor %in% "parker" & LES$term %in% "2005_2006",]$klarner_name <- "parker, cherelle l."
LES[LES$sponsor %in% "parker" & LES$term %in% "2005_2006",]$sponsor <- "parker, cherelle l."

### Shawn Flaherty -- Won special, Lost General
LES[LES$sponsor %in% "flaherty" & LES$term %in% "2005_2006",]$klarner_id <- 279069
LES[LES$sponsor %in% "flaherty" & LES$term %in% "2005_2006",]$klarner_name <- "flaherty, shawn t."
LES[LES$sponsor %in% "flaherty" & LES$term %in% "2005_2006",]$sponsor <- "flaherty, shawn t."

### R Lee James
LES[LES$sponsor %in% "james" & LES$term %in% "2011_2012",]$klarner_id <- 318260
LES[LES$sponsor %in% "james" & LES$term %in% "2011_2012",]$klarner_name <- "james, r. lee"
LES[LES$sponsor %in% "james" & LES$term %in% "2011_2012",]$sponsor <- "james, r. lee"

### Rudolph Vulakovich
LES[LES$sponsor %in% "vulakovich" & LES$term %in% "2013_2014",]$klarner_id <- 332303
LES[LES$sponsor %in% "vulakovich" & LES$term %in% "2013_2014",]$klarner_name <- "vulakovich, rudolph p."
LES[LES$sponsor %in% "vulakovich" & LES$term %in% "2013_2014",]$sponsor <- "vulakovich, rudolph p."

### kruegerbraneky, leanne t.
LES[LES$sponsor %in% "krueger-braneky" & LES$term %in% "2015_2016",]$klarner_id <- 332595
LES[LES$sponsor %in% "krueger-braneky" & LES$term %in% "2015_2016",]$klarner_name <- "kruegerbraneky, leanne t."
LES[LES$sponsor %in% "krueger-braneky" & LES$term %in% "2015_2016",]$sponsor <- "kruegerbraneky, leanne t."


############# Check for Duplicates
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
rm(check_dup, k_sub, exact, name, name_sub, missing, t)


########################################################################################################################################################
########################################################################################################################################################
######## AGGREGATE AND MATCH TO EXTERNAL
########################################################################################################################################################
########################################################################################################################################################

### Klarner State Leg. Election Data --- Using Adjusted File From Top
klarner_sub <- filter(klarner, sab == this_state & year >= min_year - 2 & outcome == 'w')
klarner_sub <- select(klarner_sub, caseid, year, sen, ddez, dno, term, termz, cando, cand, candid, partyt, partyt, exper, outcome, etype)

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
      LES[LES$sponsor %in% name,]$party <- sponsor_rows[1,]$partyt
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
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$party <- sponsor_sub[1,]$partyt
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$exper <- sponsor_sub[1,]$exper
        }else{
          if(nrow(sponsor_rows) > 0){
            sponsor_rows <- arrange(sponsor_rows, year)
            LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$party <- ifelse(is.logical(na.omit(unique(sponsor_rows$partyt))), NA, na.omit(unique(sponsor_rows$partyt))[1] )
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
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyt, outcome)

### Not In Klarner -- https://www.legis.state.pa.us/cfdocs/legis/BiosHistory/index.cfm?body=H
fill_missing <- data.frame(LES_name = "burns, barbara", new_name = 'burns, barbara', party = 'd', district = 20, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "haste", new_name = 'haste, jeffrey t.', party = 'r', district = 104, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "wright, g", new_name = 'wright, gayle marie', party = 'd', district = 2, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "schmotzer, martin michael", new_name = 'schmotzer, martin michael', party = 'd', district = 22, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "white", new_name = 'white, martina', party = 'r', district = 170, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "cook-artis", new_name = 'cook-artis, tonyelle', party = 'd', district = 200, exper = 'none')
### 2017-2018
fill_missing <- add_row(fill_missing, LES_name = "o'neal, t", new_name = "o'neal, timothy j.", party = 'r', district = 48, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "davis, a", new_name = 'davis, austin', party = 'd', district = 35, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "tai", new_name = 'tai, helen', party = 'd', district = 178, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "owlett, clint", new_name = 'owlett, clint', party = 'r', district = 68, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "mcneill, jeanne", new_name = 'mcneill, jeanne', party = 'd', district = 133, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "vazquez", new_name = 'vazquez, emilio', party = 'd', district = 197, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)
rm(fill_missing, i)

########################################################
################ Name Correction
########################################################

## Misspelled in both Klarner and Legislative data (but correct on legislative history page)
LES[LES$sponsor == "schrimenti, tom",]$sponsor <- 'scrimenti, tom'

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

### Fix Mismatches: 
# -- Corman, J. Doyle = Jr; 
LES[LES$sponsor %in% c('corman, j. doyle', 'lewis, h. craig', 'murphy, thomas j.', 'salvatore, frank a.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% arrange(sponsor) %>% as.data.frame()

### Fix Mismatches: 
# LES[LES$sponsor %in% c('zzzzzzzzz'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# -- NO SM Data before 1996
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1987_1988', '1989_1990', '1991_1992', '1993_1994', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('brook', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'brown, teresa e.', SM_name = 'Forcier, Teresa')
# name_matches <- add_row(name_matches, LES_name = 'baker, earl m.', SM_name = 'zzzzz')
# name_matches <- add_row(name_matches, LES_name = 'brooks, wallis w.', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'corman, j. doyle', SM_name = 'Corman')
# name_matches <- add_row(name_matches, LES_name = 'dawida, michael', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'harper, kate', SM_name = 'Harper, Catherine')
# name_matches <- add_row(name_matches, LES_name = 'haste, jeffrey t.', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'king, david o.', SM_name = 'King')
name_matches <- add_row(name_matches, LES_name = 'lynch, james c.', SM_name = 'Lynch, Jim')
name_matches <- add_row(name_matches, LES_name = 'mihalich, herman', SM_name = 'Michalich, Herman')
name_matches <- add_row(name_matches, LES_name = 'mowery, harold f. jr.', SM_name = 'Mowery Jr., Harold')
# name_matches <- add_row(name_matches, LES_name = 'richardson, david p.', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'schlegelculver, lynda', SM_name = 'Culver Schlegel, Lynda')
# name_matches <- add_row(name_matches, LES_name = 'shumaker, john j.', SM_name = 'zzzzz')
name_matches <- add_row(name_matches, LES_name = 'vulakovich, rudolph p.', SM_name = 'Vulakovich, Randy') # https://www.followthemoney.org/entity-details?eid=13005583
name_matches <- add_row(name_matches, LES_name = 'white, mary jo', SM_name = 'White, Mary Jo')
name_matches <- add_row(name_matches, LES_name = 'williams, hardy', SM_name = 'Williams, Anthony')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

###### Manual Matches
## Two Gibson Armstrongs:
LES[LES$sponsor == 'armstrong, gibson c.',]$SM_name <-  ideo[ideo$name == 'Armstrong, Gibson' & ideo$house2006 %in% 1,]$name
LES[LES$sponsor == 'armstrong, gibson c.',]$SM_party <- ideo[ideo$name == 'Armstrong, Gibson' & ideo$house2006 %in% 1,]$party
LES[LES$sponsor == 'armstrong, gibson c.',]$np_score <- ideo[ideo$name == 'Armstrong, Gibson' & ideo$house2006 %in% 1,]$np_score
LES[LES$sponsor == 'armstrong, gibson e.',]$SM_name <-  ideo[ideo$name == 'Armstrong, Gibson' & ideo$senate2003 %in% 1,]$name
LES[LES$sponsor == 'armstrong, gibson e.',]$SM_party <- ideo[ideo$name == 'Armstrong, Gibson' & ideo$senate2003 %in% 1,]$party
LES[LES$sponsor == 'armstrong, gibson e.',]$np_score <- ideo[ideo$name == 'Armstrong, Gibson' & ideo$senate2003 %in% 1,]$np_score

########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########
### ** D --> R, D record missing (too early): 
### ** Pat Carone, Edward Krebs, Thomas Stish (but can still correct klarner)
# filter(LES, party == 'd' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% arrange(sponsor) %>% as.data.frame()
# filter(LES, party == 'r' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### *** John Gordner -- Switched D to R on October 1, 2001 -- https://www.legis.state.pa.us/cfdocs/legis/BiosHistory/MemBio.cfm?ID=96&body=H
LES[LES$sponsor == 'gordner, john r.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Gordner, John R.' & ideo$party == 'R',]$name
LES[LES$sponsor == 'gordner, john r.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Gordner, John R.' & ideo$party == 'R',]$party
LES[LES$sponsor == 'gordner, john r.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Gordner, John R.' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'gordner, john r.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Gordner, John' & ideo$party == 'D',]$name
LES[LES$sponsor == 'gordner, john r.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Gordner, John' & ideo$party == 'D',]$party
LES[LES$sponsor == 'gordner, john r.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Gordner, John' & ideo$party == 'D',]$np_score

### *** John Lawless --- Switched D to R on Dec. 3 2001: https://www.legis.state.pa.us/cfdocs/legis/BiosHistory/MemBio.cfm?ID=96&body=H
LES[LES$sponsor == 'lawless, john a.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Lawless, John' & ideo$party == 'R',]$name
LES[LES$sponsor == 'lawless, john a.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Lawless, John' & ideo$party == 'R',]$party
LES[LES$sponsor == 'lawless, john a.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Lawless, John' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'lawless, john a.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Lawless, John' & ideo$party == 'D',]$name
LES[LES$sponsor == 'lawless, john a.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Lawless, John' & ideo$party == 'D',]$party
LES[LES$sponsor == 'lawless, john a.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Lawless, John' & ideo$party == 'D',]$np_score

### R. Tracy Seyfert -- Miscoded in Klarner as a Democrat in 1996 -- Was always a R: https://www.legis.state.pa.us/cfdocs/legis/BiosHistory/MemBio.cfm?ID=242&body=H
LES[LES$sponsor == "seyfert, tracy",]$party <- 'r'

### Thomas B. Stish -- Miscoded in Klarner as a Democrat in 1995 -- Switched mid 1994: https://www.legis.state.pa.us/cfdocs/legis/BiosHistory/MemBio.cfm?ID=315&body=H
LES[LES$sponsor == "stish, thomas b." & LES$term == "1995_1996",]$party <- 'r'

### Joseph B. Scarnati iii -- Won as Independent in 2000, immediately switched to become Republican in January 2001
LES[LES$sponsor == "scarnati, joseph b. iii" & LES$term %in% c('2001_2002', '2003_2004'),]$party <- 'r'

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1989 - 2020
# ** 2007-2008 had an R speaker elected mostly by Dems as a compromise.. but not clear they got the committees and D's still much higher on LES
LES[as.numeric(substring(LES$term,1,4)) %in% c(1989:1994, 2007:2010) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:2006, 2011:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1989 - 2020
# ** Dems held senate for most of 1993-1994 term -- Bob Mellow (D) was president pro tem -- but Rep's took control again in March 1994 (Robert Jubelirer (R) took pro tem spot)
# ** Lt. Gov is President, but the Pro Tem is functionally the leader of the senate
LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:1994) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1985:1992, 1995:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


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
LES[LES$sponsor == 'phillipshill, kristin lee',]$sponsor <- 'phillips-hill, kristin lee'
LES[LES$sponsor == 'astorinokulik, anita',]$sponsor <- 'kulik, anita astorino'
LES[LES$sponsor == 'mcilvainesmith, barbara',]$sponsor <- 'mcilvaine-smith, barbara'
LES[LES$sponsor == 'lowerybrown, vanessa l.',]$sponsor <- 'brown, vanessa lowery'
LES[LES$sponsor == 'corman, j. doyle',]$sponsor <- 'corman, jacob doyle jr.'
LES[LES$sponsor == 'corman, jacob doyle',]$sponsor <- 'corman, jacob doyle iii'
# LES[LES$sponsor == 'egolf, c. allan',]$sponsor <- 'egolf, c. allan'
LES[LES$sponsor == 'harhai, r. ted',]$sponsor <- 'harhai, robert ted'
LES[LES$sponsor == 'costa, dom',]$sponsor <- 'costa, dominic'
LES[LES$sponsor == 'miranda, j. p.',]$sponsor <- 'miranda, jose p.'
LES[LES$sponsor == 'carone, pat',]$sponsor <- 'carone, patricia'
#LES[LES$sponsor == 'zzzzzzzzzzz',]$sponsor <- 'zzzzzzzzzzzzzz'

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
  scale_color_manual(values=c("dodgerblue2",  "gray50", 'red2'))

##### CHECK OUTLIERS
# filter(LES, party == 'd' & np_score > .25) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & np_score < -.25) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
