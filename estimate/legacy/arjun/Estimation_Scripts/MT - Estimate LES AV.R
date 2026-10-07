

###############################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** MONTANA *** BY SESSION
###############################################################


###################################
## (SPECIAL) SESSIONS:
## ---- Bill numbers carry over during regular session (one continuous biennium)
## ---- For Specials: Bill numbers restart at 1 --> Make sure to merge on SS 
## ---------> BUT only ever ONE special session per biennium
## MEMBER LISTS:
## ---- https://leg.mt.gov/legislator-information
## PROCESS:
## ----
## Sponsorship/Authorship
## -- One Sponsor Listed; No Coauthor Information; Rules on this TBD
###########################
### ~~~~ NOTES ~~~~
##  (1) 'First Reading ~ [(H) Appropriations]' shows up in more recent years? See 2017, HB0012 plus others. Is this AIC? Seemingly NO
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
library(inexact)
library(foreach)
library(tibble)

this_state <- 'MT'
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
terms <- 2021
t = terms
t_plus_one = t + 1
t_yrs <- as.numeric(terms)
t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 1}'))
data_files <- list.files(glue("../../../State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
sessions <- gsub('.+Bill_Details_|.csv', '', bill_files)
rm(data_files, bill_files)
t_sessions = sessions[startsWith(sessions,as.character(t)) | startsWith(sessions,as.character(t+1))]



#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- bind_rows(read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t_plus_one}.csv"),
                               colClasses=c("character")))

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



### Formulate 2-year terms -- Cover both regular and special sessions


### Skip Previously Estimated
if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
  print(glue('. \n ~~~~ SKIPPING {t_yrs} TERM ---> LES scores already estimated! \n.'))
  next
}

###### SESSION IN PROGRESS
print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_sessions[1]}.csv")
bills <- read.csv(bill_path)   

### If multiple sessions in different files, read in those as well
if(length(t_sessions) > 1){
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{s}.csv")
    s_bills <- read.csv(bill_path)
    bills <- bind_rows(bills, s_bills)
  }
  rm(s, s_bills)
}

### Clean Term/Session Variables
bills$term <- t_yrs
bills$session <- bills$session_type
bills <- select(bills, -session_type)

### Check for duplicates
if(nrow(bills) != nrow(distinct(bills))){
  cat(" \n ~~~> DUPLICATE BILLS \n\n .")
  break
}

####### MT ONLY --- DROP BILL DRAFT
# table(bills$drafter, gsub(' .+|;.+|,.+', '', bills$subjects)) %>% View()
all_bills <- bills
bills <- filter(bills, bill_draft == 0)

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_number) %>% mutate(bill_id = toupper(bill_id))

############### Drop Resolutions, Messages, Communications, Reports
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)

############### Standardize Sponsors + Extracting + Ensuring Cosponsorship match down the line
bills$sponsor_dist <- str_extract(bills$sponsor, 'HD [0-9]+|SD [0-9]+')
bills$sponsor_party <- str_extract(bills$sponsor, '\\([A-Z]\\)')
table(bills$sponsor_dist); table(bills$sponsor_party)
# filter(bills, sponsor_dist == '' | bills$sponsor_party == '')

#### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
bills$sponsor <- tolower(bills$sponsor)
bills$sponsor <- gsub('á', 'a', bills$sponsor)
bills$sponsor <- gsub('é', 'e', bills$sponsor)
bills$sponsor <- gsub('ó', 'o', bills$sponsor)
bills$sponsor <- gsub('í', 'i', bills$sponsor)
bills$sponsor <- gsub('ñ', 'n', bills$sponsor)
bills$sponsor <- gsub('\\|', '', bills$sponsor)

#### Adjusting for Bills By Request -- Need to do BEFORE splitting
if(any(grepl('\\(br\\)|by request| br$', bills$sponsor))){
  print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
  #print(glue("-----> KEEPING {nrow(filter(bills, grepl('(br)|by request', introducers)))} bill(s) introduced BY REQUEST"))
}


#### Extract Nickname
bills$nickname <- gsub('\\(|\\)', '', str_extract(bills$sponsor, '\\([a-z][a-z ]+\\)'))
bills$sponsor <- gsub('  +', ' ', gsub('\\([a-z][a-z ]+\\)', '', bills$sponsor))

#### LES Sponsor Variable
bills$LES_sponsor <- tolower(gsub(' \\(.+', '', bills$sponsor))
sort(table(bills$LES_sponsor))



###################
###### Merge in S&S Bills
###################
# *** For MONTANA: Bills carry over during regular (one biennium), but numbers re-start for all special sessions
# ---> Need to merge on Id and Session 
# ---> Capping Bill numbers at SESSION MAX and assuming bill is from SS1 (never an SS2,3,4)

if(t_yrs == "2019_2020"){
  SS_bills$bill_id[SS_bills$bill_id=="HB20002"] = "HB0002"
}

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_number,term,title), c("bill_id"="bill_number", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",title, ignore.case=T)) %>%
  arrange(title) 
unique(missing_SS_bills$bill_id) 

# now check to see if there are duplicate joins

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
SS_in_PVS = nrow(SS_term %>% 
                   mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
                   filter(bill_type %in% keep_types)  )

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


#### DROP Committee Bills
if(any(grepl('committee', bills$sponsor))){
  print(glue("-----> DROPPING {nrow(filter(bills, grepl('committee', sponsor)))} bill(s) introduced BY COMMITTEE"))
  break
  # bills <- filter(bills, !grepl('committee', LES_sponsor))
}

###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "") %>% View()
if(nrow(filter(bills, LES_sponsor == '')) > 0){
  print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
  bills <- filter(bills, !(LES_sponsor == ''))     
}


####################################################
############### Code Bill History
####################################################

bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
bill_hist <- read.csv(bill_hist_path)

## If multiple sessions, read in those as well
if(length(t_sessions) > 1){
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
    s_hist <- read.csv(bill_path)
    bill_hist <- bind_rows(bill_hist, s_hist)
  }
  rm(s, s_hist)
}

######## Standardize BillHist Bill IDs
bill_hist <- rename(bill_hist, bill_id = bill_number) %>% mutate(bill_id = toupper(bill_id))

### Clean Term/Session Variables
bill_hist$term <- t_yrs
bill_hist$session <- bill_hist$session_type
bill_hist <- select(bill_hist, -session_type) %>% 
  arrange(session, bill_id, order)

### Drop Draft Requests + Drafting Actions
bill_hist <- filter(bill_hist, bill_id != '') %>%
  filter(chamber != 'C')

##### Re-ORDER to Account for Missing Actions from Dropping Bill Draft Office
bill_hist <- bill_hist %>%
  group_by(session, bill_id) %>%
  mutate(order = 1:n()) %>%
  ungroup()

### Standardize Chamber Variable
bill_hist$chamber <- recode(toupper(bill_hist$chamber), "H" = "House", "S" = "Senate")
### *** Could fill in blanks, which appear to be mostly just "Chapter Number Assigned" = LAW

### Remove Chamber at beginning of Action
bill_hist$action <- gsub('^\\([A-Z]\\) ', '', bill_hist$action)

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c('^hearing ~', '^committee executive action', '^committee report', 'tabled in committee')
abc_t <- c("2nd reading", '3rd reading', 'second reading', 'third reading')   #, 'rereferred to committee' -- not always abc, sometimes after first reading
pc_t <- c('3rd reading passed', 'third reading passed', 'transmitted to senate', 'transmitted to house', 'enrolling', 'signed by speaker')
law_t <- c('signed by governor', 'chapter number assigned')
# filter(bill_hist, grepl('rereferred to committee', tolower(action))) %>% select(bill_id, action)
# filter(bill_hist, bill_id == 'SB0541') %>% select(chamber, action_date, action, order)

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
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t, 
                                    ignore_chamber_switch = TRUE) ### Need this to account for a few out of order actions (chamber a before b finishes)
  bill_stages$bill_url <- bills[i,]$bill_url
  ### Correcting AIC to 0 if sponsor requested bill not be heard (typically then tabled --> 1)
  if(bill_stages$action_in_comm == 1 & sum(bill_stages[,5:9]) == 2 & any(grepl("bill not heard at sponsor", tolower(hist_sub$action)))){
    bill_stages$action_in_comm <- 0
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

######## MT: NO Cosponsorship Info
all_sponsors$num_cosponsored_bills <- NA
# for(i in 1:nrow(all_sponsors)){
#   c_sub <- filter(bills, substring(bill_id, 1, 1) == all_sponsors[i,]$chamber)
#   all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$sponsor)))
#   ### ONLY need to adjust this way if sponsored and cosponsored column are the same
#   all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
# }
# all_sponsors <- select(all_sponsors, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))

#######################
#### CLEAN NAMES
parsed_names <- map_df(all_sponsors$LES_sponsor, parse_names) %>% select(-salutation) %>% distinct() ### Need distinct in case people switch chambers
all_sponsors <- left_join(all_sponsors, parsed_names, by = c("LES_sponsor" = "full_name"))
all_sponsors$last_name <- gsub(',$', '', all_sponsors$last_name)
all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update Last Names for Matching 
# 2001+ -- 3 House terms, 2 senate terms, now back in house (term limits are 8 years, but must be consec.)
all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "jonathan windy boy", "windy boy", all_sponsors$last_name)
# 2017+
all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "sharon stewart peregoy", "stewart-peregoy", all_sponsors$last_name)
### Peggy Arnott Bergsagel
if(t_yrs == '1999_2000'){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "peggy bergsagel", "bersagel-arnott", all_sponsors$last_name)
}
### Emily swanson (maiden) stonington -- switched back in 2000
if(t_yrs == "2001_2002" | t_yrs == "2003_2004"){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "emily stonington", "swanson", all_sponsors$last_name)
}
## Frosty Boss Ribs -- 2009-2012?
all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "frosty boss ribs", "calfbossribs", all_sponsors$last_name)

### Ellie Hill (Smith)
all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "ellie hill smith", "hill", all_sponsors$last_name)


all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(gsub('.{1}$', '',(sub("\\(.*", "",LES_sponsor))),substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name, middle_name,suffix))


legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(t)) | startsWith(legiscan_sessions,as.character(t+1))]

legiscan = foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% select(-nickname) %>%  distinct()


# if(t_yrs == "2019_2020") {
#   legiscan = bind_rows(legiscan,
#                        legiscan %>% filter(people_id == 634) %>% mutate(role = "Sen", district = "SD-020"),
#                        legiscan %>% filter(people_id == 14653) %>% mutate(role = "Rep", district = "HD-029"))
#   
# }
# 
# if(t_yrs == "2021_2022") {
#   legiscan = bind_rows(legiscan %>% mutate(role = ifelse(people_id == 20325, "Sen", role)),
#                        legiscan %>% filter(people_id == 20325) %>% mutate(role = "Rep", district = "HD-044"))
#   
# }

legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  mutate(match_name_chamber = tolower(paste(name,substr(district,1,1),sep="-")))

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
        "bob keenan-s" = NA_character_
      )
    )
  
}

if(t_yrs == "2021_2022"){
  all_sponsors2 = # You added custom matches:
    # You added custom matches:
    # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "kim abbott-h" = NA_character_
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
###### Estimate Scores + Add in Related Variables
#############################

### Check if bills in data without an ID'd sponsor
filter(bills, !(bills$LES_sponsor %in% legis_data$data_name))
bills <- bills %>% select(-sponsor) %>%
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


cat(glue(". \n *********************** TERM {t_yrs} ~~> DONE  ***********************"))


################# ****** END LOOP

#agg_stats, matches
rm(all_sponsors, bills, legis_data, SS_bills, elec_year, i, keep_types)
rm(k_matches, klarner_sub, m_sub, km, bill_path, t_yrs, calc_LES) # c_sub
rm(t, terms,t_sessions, klarner_gs, parsed_names, commem_bills)

########################################################################################################################################################
############################################################################################################################################################# NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 session! ~~~~~~~~~~~~~~ 
# APPOINTED to H:
# --- TREXLER --- Won in 1996, someone else won in 1998, unclear if he switched districts.. Doesn't seem to have moved to Senate...
# ---------------- He's also recorded in Minutes as being in the chamber in March 1999 -- https://leg.mt.gov/bills/MinutesPDF/990318BUH_Hm1.pdf
# --- LENHART --- Lost 1998 Senate election, won 2000 H election ---> Must have been appointed/won special in interim?
# --- ELLINGSON (jon) --- Elected to House in 94/96, Senate in 98/02 ---> https://helenair.com/news/state-and-regional/former-state-sen-jon-ellingson/article_e3f2dc6c-50e6-11e3-b46a-001a4bcf887a.html
# IN CHAMBER: 
# -- JOHNSON (john) -- served 12 years starting 1988: https://missoulian.com/news/local/obituaries/john-h-johnson/article_bbe10080-09c0-11df-bba5-001cc4c03286.html
# -- ORR (Scott) -- In as of January 1999 --- https://leg.mt.gov/bills/MinutesPDF/990128TAH_Hm1.pdf
# -- BROOK (Vivian) -- Won Senate Election in 1994, 1998, no reason to believe left office... https://missoulian.com/news/local/obituaries/vivian-morgan-brooke/article_1d46d9af-07a0-5a76-9d80-331bc4c52439.html
# DROP: 
# -- BENEDICT (Steve) --- Resigned on election day --> https://missoulian.com/uncategorized/women-hold-seats-in-montana-legislature-a-record/article_2966497c-36fe-56ef-8dc7-854f06752245.html
# NAME FIX: 
# -- Peggy Bergsagel --> Peggy Bergsagel-Arnott (techically Arnott Bergsagel but reverse matches)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 session! ~~~~~~~~~~~~~~ 
# ELECTED to S: 
# -- ELLINGSON (jon) -- Unclear why missing elec data in 98, see above.
# SEEMINGLY IN CHAMBER: 
# -- TRAMELLI, ADAMS, STEINBEISSER, PEASE
# ---> All in voting scorecards --> Voted --> Must not have sponsored bills: https://mtvoters.org/wordpress/wp-content/uploads/2016/04/2001-MCV-Scorecard.pdf
# NAME FIX: 
# -- Emily Stonington --> Emily Swanson -- https://www.bozemandailychronicle.com/news/politics/emily-who-swanson-switching-back-to-her-maiden-name/article_46f61805-7fcc-51bd-8855-31a3b8906738.html

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 session! ~~~~~~~~~~~~~~ 
# IN CHAMBER, NO BILLS: 
# -- MOOD; HAWK --> See https://leg.mt.gov/legislator-information/?session_select=80 

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 session! ~~~~~~~~~~~~~~ 
# APPOINTED/WON SPECIAL to S: 
# -- LEWIS (dave)
# -- ESSMANN (jeff)
# IN CHAMBER: 
# -- EVERETT (George) 

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 session! ~~~~~~~~~~~~~~ 
# APPOINTED to H: 
# -- HOLLENBAUGH (galen) -- https://en.wikipedia.org/wiki/Galen_Hollenbaugh
# APPOINTED to S: 
# -- KAUFMANN (christine) -- https://en.wikipedia.org/wiki/Christine_Kaufmann_(Montana_politician)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 session! ~~~~~~~~~~~~~~ 
# IN CHAMBER: 
# -- REGIER 
# -- HOLLANDSWORTH
# -- KASTEN
# DROP: 
# -- GROESBECK --- Passed away Dec. 2008
# DUPLICATES FIXED: 
# -- Pat Noonan != Art Noonan --- For some reason Art is missing from klarner data

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 session! ~~~~~~~~~~~~~~ 
# APPOINTED to S: 
# -- MOWBRAY (Carmine)
# IN CHAMBER: 
# -- SKATTUM (Dan)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 session! ~~~~~~~~~~~~~~ 
# APPOINTED to S:  
# -- BOULANGER (Scott) -- https://ballotpedia.org/Scott_Boulanger
# IN CHAMBER:  
# -- BLASDEL (Mark, speaker) 
# -- CALFBOSSRIBS 
# -- HAGSTROM 
# -- VANCE

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 session! ~~~~~~~~~~~~~~ 
# APPOINTED to H: 
# -- RICHMOND (tom) -- https://billingsgazette.com/news/state-and-regional/govt-and-politics/tom-richmond-appointed-to-state-legislature/article_7f3bfea7-7765-5622-bf5b-e169a867d4df.html
# DROP:  
# -- MCNIVEN (Jon) ---> Resigned in Nov 2014

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 session! ~~~~~~~~~~~~~~ 
# APPOINTED to S:  
# -- BOLAND (carlie) -- https://www.greatfallstribune.com/story/news/local/2017/02/06/boland-takes-oath-senate-office/97567662/
# -- TEMPEL (Russel) -- https://www.havredailynews.com/story/2018/10/09/local/tempel-tuss-face-off-in-senate-district-14-race-russ-tempel-republican/520783.html
# IN CHAMBER:  
# -- GUNDERSON (steve) 
# -- BARTEL (dan) 
# -- MORTENSEN (dale) 
# -- FLEMING (john) 
# -- SMALL (jason)
# NAME FIX:  
# -- Ellie Hill Smith --> Ellie Hill


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


### ****Still missing***** 
# MOWBRAY + TEMPEL = Appointed, didn't run again, not in Klarner
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[1]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, still_missing)

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

### Manually Fix Those Not in Klarner
LES[LES$sponsor == "mowbray, carmine",]$party <- 'r'
LES[LES$sponsor == "mowbray, carmine",]$district <- 6
LES[LES$sponsor == "mowbray, carmine",]$exper <- 'none'

LES[LES$sponsor == "tempel, russel",]$party <- 'r'
LES[LES$sponsor == "tempel, russel",]$district <- 14
LES[LES$sponsor == "tempel, russel",]$exper <- 'none'

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

### Fix Klarner Name with Numeric
LES[LES$sponsor == 'thomas, william g. 2',]$sponsor <- 'thomas, william g.'

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
LES[LES$sponsor %in% c('barrett, dick'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
## ** Notable Names -- Durward 'Butch' Waddill; Carlie 'Cydnie' Boland
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# ---- Remaining are mostly from 2009_2010
# filter(LES, is.na(np_score)) %>% filter( !(term %in% c('2017_2018')) ) %>% group_by(sponsor, party) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame() 
# filter(ideo, grepl('hutton', tolower(name))) %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'barrett, dick', SM_name = 'Barrett, Richard')
# name_matches <- add_row(name_matches, LES_name = 'bean, russell', SM_name = 'zzzzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'beck, bill', SM_name = 'Beck, William Sr.')
# name_matches <- add_row(name_matches, LES_name = 'beck, paul', SM_name = 'zzzzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'boniek, joel', SM_name = 'zzzzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'devlin, gerry', SM_name = 'Devlin') # Two devlins, other has first name
# name_matches <- add_row(name_matches, LES_name = 'fleming, john', SM_name = 'zzzzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'galt, wylie', SM_name = 'Galt, Errol') # Errol Wylie Galt
# name_matches <- add_row(name_matches, LES_name = 'getz, dennis', SM_name = 'zzzzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'hansen, ken', SM_name = 'Hansen, Kenneth')
# name_matches <- add_row(name_matches, LES_name = 'hutton, rowlie d.', SM_name = 'zzzzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'kipp, george g. iii.', SM_name = 'Kipp III, George G')
# name_matches <- add_row(name_matches, LES_name = 'roundstone, j. david', SM_name = 'zzzzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'steenson, cheryl', SM_name = 'zzzzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'swanson, emily', SM_name = 'Stonington') # Maiden name
name_matches <- add_row(name_matches, LES_name = 'wagner, bob', SM_name = 'Wagner, Robert')
name_matches <- add_row(name_matches, LES_name = 'wilson, william f. (bill)', SM_name = 'Wilson, Bill')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

# #### Manual Edits (Needs more precision...)
# LES[LES$sponsor == 'anderson, whitney',]$SM_name <- ideo[ideo$name == 'Anderson' & ideo$senate1999 %in% 1,]$name
# LES[LES$sponsor == 'anderson, whitney',]$SM_party <- ideo[ideo$name == 'Anderson' & ideo$senate1999 %in% 1,]$party
# LES[LES$sponsor == 'anderson, whitney',]$np_score <- ideo[ideo$name == 'Anderson' & ideo$senate1999 %in% 1,]$np_score


rm(ideo, check_last, name_matches, d_name, i, lastname)

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term) # Coding 1995+ in case expand back

LES$in_majority <- 0

### House -- 1995 - 2020
# ** Split 25-25 in 2005_2006 and 2009_2010 --> Law (via NCSL) saws that leaders appointed by governor's party
# ---> Dem Control in 2005_2006 and 2009_2010
LES[as.numeric(substring(LES$term,1,4)) %in% c(2005:2006, 2009:2010) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:2004, 2007:2008, 2011:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1995 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(2005:2008) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:2004, 2009:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1

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
LES[LES$sponsor == 'callahan, t. m.',]$sponsor <- 'callahan, tim m.'
LES[LES$sponsor == 'swanson, emily',]$sponsor <- 'stonington, emily swanson'
LES[LES$sponsor == 'hansen, kris',]$sponsor <- 'hansen, kristin'
LES[LES$sponsor == 'ross, jack',]$sponsor <- 'ross, john m.'
LES[LES$sponsor == 'jones, llew',]$sponsor <- 'jones, llewelyn'
LES[LES$sponsor == 'kottel, deb',]$sponsor <- 'kottel, deborah'
LES[LES$sponsor == 'buttrey, edward',]$sponsor <- 'buttrey, francis edward'


#### Party Fix
# ****** No record that Frank J. Smith switched parties... error in Klarner? 
# E.g., his page from 1999 session shows D: https://leg.mt.gov/legislator-information/roster/individual/1494
LES[LES$sponsor == "smith, frank j.",]$party <- 'd'

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
  scale_color_manual(values=c("dodgerblue2", "gray50", "red2"))

##### CHECK OUTLIERS
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

