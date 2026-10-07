################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** TENNESSEE *** BY SESSION
##############################################################

###################################
## (SPECIAL) SESSIONS:
## ---- Bills Carryover From Regular to Regular Session (One Biennium) -- Numbers Start at 1 for both chambers
## ---- Special Session Bill Numbers = Unique Identifiers -- 1st Special, numbers start at 7000, Second Special, 9000
## MEMBER LISTS:
## ---- HOUSE ROSTERS: http://www.capitol.tn.gov/house/archives/
## ---- SENATE ROSTERS: http://www.capitol.tn.gov/senate/archive/
## PROCESS/RULES:
## ---- 
## Sponsorship/Authorship
## ---- 
###########################
## NOTES:
## (1) NOT accounting for companion records on same page 
## ----- e.g., companion passage recorded (HB0010 in 109th)
## ----- Introducing a companion is customary, but from process website seems like the main authors bill is usually the one that's substituted for the companion (e.g., it replaces it)
## (2) Caption bills = bills that are proposed without final language (so just the caption)? -- http://archive.knoxnews.com/opinion/columnists/tom-humphrey/humphrey-caption-bills-101-when-titles-matter-ep-409994570-359381131.html
## (3) How to code party control in 2009_2010 in House??? See notes...
## ------ AT PRESENT (after re-running) coding BOTH PARTIES as in_majority
## (4) Is being assigned to a subcommittee AIC? e.g., "assigned to s/c"
## -------> At present, no, seems like happens automatically AND we generally know if something happened in the subcommittee
## -------> test <- filter(bill_hist, grepl(", ref to education", tolower(action))) %>% select(bill_id, order) %>% mutate(order = order + 1)
## -------> filter(bill_hist, paste0(bill_id, "-", order) %in% paste0(test$bill_id, "-", test$order)) %>% View()
###################

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

this_state <- 'TN'
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
terms <- 2023
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




###### SESSION IN PROGRESS
print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_sessions[1]}.csv")
bills <- read.csv(bill_path)

### If multiple sessions in different files, read in those as well
if(length(t_sessions) > 1){
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path2 <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{s}.csv")
    s_bills <- read.csv(bill_path2)
    if(nrow(s_bills) == 0){ next }
    bills <- bind_rows(bills, s_bills)
  }
  rm(s, s_bills)
}
bills <- arrange(bills, bill_id)

### Clean Term/Session Variables
bills <- bills %>%
  mutate(session = recode(session, 'General Assembly' = 'RS', 'Special Session' = 'SS1', '1st Special Session' = 'SS1', '2nd Special Session' = 'SS2'),
         fiscal_summary = ifelse(fiscal_summary == "Not Available", '', fiscal_summary),
         summary = ifelse(summary == 'Abstract summarizes the bill.', '', summary)) 

if(t_yrs == '2015_2016'){
  bills[grepl('[A-Z]+70[0-9][0-9]', bills$bill_id),]$session <- 'SS1'
  bills[grepl('[A-Z]+90[0-9][0-9]', bills$bill_id),]$session <- 'SS2'
}

### Drop duplicates
bills <- distinct(bills)

############### Drop Resolutions, Messages, Communications, Reports
all_bills <- bills
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) 

##########################
####### Standardize Sponsors

bills$sponsor <- tolower(bills$sponsor)
bills$sponsor <- gsub('á', 'a', bills$sponsor)
bills$sponsor <- gsub('é', 'e', bills$sponsor)
bills$sponsor <- gsub('ó', 'o', bills$sponsor)
bills$sponsor <- gsub('í', 'i', bills$sponsor)
bills$sponsor <- gsub('ñ', 'n', bills$sponsor)

bills$cosponsors <- tolower(bills$cosponsors)
bills$cosponsors <- gsub('á', 'a', bills$cosponsors)
bills$cosponsors <- gsub('é', 'e', bills$cosponsors)
bills$cosponsors <- gsub('ó', 'o', bills$cosponsors)
bills$cosponsors <- gsub('í', 'i', bills$cosponsors)
bills$cosponsors <- gsub('ñ', 'n', bills$cosponsors)

### Fix Errors --- For whatever reason, some of these pull the main sponsor of that bill number from the CURRENT Term...
if(t_yrs == '2001_2002'){
  bills$sponsor <- gsub('williams, sen.', 'williams', bills$sponsor)
  bills$cosponsors <- gsub('williams, sen.', 'williams', bills$cosponsors)
}else if(t_yrs == '2007_2008'){
  bills[bills$sponsor == "parkinson",]$sponsor <- "campfield"
  bills[bills$sponsor == "akbari",]$sponsor <- "marrero b"
}else if(t_yrs == "2009_2010"){
  bills[bills$sponsor == "daniel",]$sponsor <- "odom"
}else if(t_yrs == '2011_2012'){
  bills[bills$sponsor == "gant",]$sponsor <- "casada"
  bills[bills$sponsor == "bailey",]$sponsor <- "bell"
}else if(t_yrs == '2013_2014'){
  bills[bills$sponsor == "crawford",]$sponsor <- "mccormick"
  bills[bills$sponsor == "lamar",]$sponsor <- "mccormick"
  bills[bills$sponsor == "pody" & substring(bills$bill_id,1,1) == 'S',]$sponsor <- "massey"
}else if(t_yrs == '2015_2016'){
  bills[bills$sponsor == "rowland",]$sponsor <- "casada"
  bills[bills$sponsor == "robinson",]$sponsor <- "norris"
}else if(t_yrs == '2017_2018'){
  bills[bills$sponsor == "rudder",]$sponsor <- "gilmore"
  #bills[bills$sponsor == "lamar",]$sponsor <- "mccormick"
}

### LES Sponsor Var
bills <- rename(bills, LES_sponsor = sponsor)
table(bills$LES_sponsor)

#### Adjusting for Bills By Request -- Need to do BEFORE splitting
if(any(grepl("request", bills$LES_sponsor))){
  print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
}


###################
###### Merge in S&S Bills
###################
# *** For TENNESSEE: Bill Numbers Uniquely Identify All Bills --> Merge on Term and ID only
##################

if(t_yrs == "2019_2020"){
  SS_bills$bill_id[SS_bills$bill_id=="HB10001"]="HB0001"
  SS_bills$bill_id[SS_bills$bill_id=="SB90009"]="SB0009"
} else if (t_yrs == "2023_2024"){
  SS_bills$bill_id[SS_bills$bill_id=="SB30003"]="SB0003"
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
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
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

##########################################
############### Code Commemorative
##########################################

bills <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session'))
table(bills$commem)

bills$commem <- ifelse(is.na(bills$commem), 0, bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)

###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "" | is.na(LES_sponsor)) %>% View()
if(nrow(filter(bills, LES_sponsor == '')) > 0 | any(is.na(bills$LES_sponsor))){
  cat('\n')
  cat(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == '')) + sum(is.na(bills$LES_sponsor))} bill(s) without a sponsor"))
  bills <- filter(bills, !(LES_sponsor == '' | is.na(LES_sponsor)))     
}


########################################################
############### Code Bill History
########################################################

bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
bill_hist <- read.csv(bill_hist_path)

## If multiple sessions, read in those as well
if(length(t_sessions) > 1){
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path2 <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
    s_hist <- read.csv(bill_path2)
    if(nrow(s_hist) == 0){ next }
    # s_hist$journal_page <- as.character(s_hist$journal_page)
    bill_hist <- bind_rows(bill_hist, s_hist)
  }
  rm(s, s_hist)
}

### Clean Term/Session Variables
bill_hist <- bill_hist %>%
  mutate(session = recode(session, 'General Assembly' = 'RS', 'Special Session' = 'SS1', '1st Special Session' = 'SS1', '2nd Special Session' = 'SS2'),
         date = as.Date(date, format = '%m/%d/%Y')) 

if(t_yrs == '2015_2016'){
  bill_hist[grepl('[A-Z]+70[0-9][0-9]', bill_hist$bill_id),]$session <- 'SS1'
  bill_hist[grepl('[A-Z]+90[0-9][0-9]', bill_hist$bill_id),]$session <- 'SS2'
}

### Order by Order
bill_hist <- arrange(bill_hist, term, bill_id, order)

### Re-Coding Chamber Variable
bill_hist$chamber <- ifelse(bill_hist$chamber == '' & grepl("^Effective|Pub. Ch.|Pr. Ch.|Governor", bill_hist$action), 'G', bill_hist$chamber)
bill_hist$chamber <- ifelse(bill_hist$chamber == '' & bill_hist$order == 1, substring(bill_hist$bill_id, 1, 1), bill_hist$chamber)
if(any(bill_hist$chamber == "")){
  bill_hist <- bill_hist %>% group_by(term, session, bill_id) %>% mutate(chamber = ifelse(chamber == "", NA, chamber)) %>% fill(chamber) %>% ungroup()
}
bill_hist$chamber <- recode(bill_hist$chamber, "house" = "House", 'H' = "House", "senate" = "Senate", 'S' = "Senate", "G" = "Governor", "CC" = "Conference")

### Get rid of periods + Excess Space in action text
bill_hist$action <- gsub("\\.", '', bill_hist$action)
bill_hist$action <- str_trim(gsub(" +", ' ', bill_hist$action))

### Adjust Subcommmittee Recommendation Rows to distinctly seperate from comm reports
# filter(bill_hist, grepl("rec.+for pass.+s/c", tolower(action))) %>% View()
bill_hist <- mutate(bill_hist, action = ifelse(grepl("rec.+for pass.+s/c", tolower(action)), paste0("subcomm rec: ", action), action))

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c("placed on s/c cal", 's/c ref to', "rec for pass", "rec\\. for pass",  "recommended for pass", "^subcomm rec",
           "placed on cal.+ comm", "placed on.+ comm cal", "failed in s/c", 'failed in.+committee')
# --S/C Failures imply hearing/vote
abc_t <- c("placed on.+(regular|consent|local bill) calendar", "ref.+calendar \\& rules comm", "ref.+s cal comm",
           "^(rec|rec\\.|recommended) for pass",
           "^(h|house) adopted am", "^(s|senate) adopted am", "passed (h|s)", "^failed (h\\b|s\\b|house|senate)", "failed to pass (h|s)",
           "amendment.+withdrawn", "engrossed", 'comp.+subst', 'subst.+for comp')
## --> *** Can't use "^rec\\. for pass\\. ref" or "recommended for pass" generally because subcomms use same language
## --> Instead: Updating S/C Recs above so language is unique
## --> ALso: ABC if referred to H Calendar & Rules Committee/S Calendar Comm --> Means made it out of origin committee
pc_t <- c("engrossed", "^passed h", "^passed s")
law_t <- c("^pub ch|^pr ch|signed by governor")
# *** This will not count companions that became law (which always are preceded by "Comp. became Pub Ch. N")

### Check Actions
# filter(bill_hist, grepl('^rec.+pass', tolower(action))) %>% distinct(action) %>% View()
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
options(warn = 2)
for(i in 1:nrow(bills)){
  b_id = bills[i,]$bill_id
  s_id = bills[i,]$session
  b_spon = bills[i,]$LES_sponsor
  hist_sub <- filter(bill_hist, bill_id == b_id, term == t_yrs, session == s_id)
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t,
                                    ignore_chamber_switch = TRUE) ## Need this to account for random miscoded chambers (e.g., HB0731 in 2005)
  bill_stages$bill_url <- bills[i,]$bill_url
  all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
  # print(i)
}
options(warn = 1)

### Check Codings
cat('\n')
all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(term, session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law) ) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2)) %>% 
  as.data.frame() %>% print()
# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% View()
# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 1,]$bill_id) %>% View()


read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-2}_{t-1}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(term, session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% print()

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-4}_{t-3}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(term, session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% print()

# Not much information available about bill stages, but can get total number of introductions by chamber/session here: https://www.capitol.tn.gov/legislation/archives.html

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
            sponsor_law_rate = sum(law) / n(),
            num_cosponsored_bills = NA) %>%
  ungroup()

#### Adding Legislators Who COSPONSORED A BILL but did not SPONSOR
# unique_cospon <- str_trim(unique(unlist(str_split(bills$cosponsors, ', '))))
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
bills$cospon_match <- paste(bills$LES_sponsor, bills$cosponsors, sep = ', ')
for(i in 1:nrow(all_sponsors)){
  c_sub <- bills[substring(bills$bill_id, 1, 1) %in% ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S'),]
  sn <- all_sponsors[i,]$LES_sponsor
  sn <- gsub('\\)', '\\\\)', gsub("\\(", '\\\\(', sn))
  ## NEED TO ACCOUNT FOR overlapping NAMES
  search_term <- paste0("^", sn, ',|^', sn, '$|, ', sn, ',|, ', sn, '$')
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(search_term, tolower(c_sub$cospon_match)))
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
# *** The (abc) at end of names appear to be counties (though some are odd (wil?))
# *** Removing but should be manually corrected at beginning
#all_sponsors$LES_sponsor <- gsub(" \\(.+", '', all_sponsors$LES_sponsor)

all_sponsors$last_name <- gsub(' [a-z]$|, [a-z]\\.$|, [a-z][a-z]+| [a-z]\\.$', '', all_sponsors$LES_sponsor)
all_sponsors$last_name <- str_trim(gsub(',$|\\.$', '', all_sponsors$last_name))
all_sponsors$first_name <- ifelse(grepl(' [a-z]$|, [a-z]\\.$|, [a-z]$|, [a-z][a-z]+| [a-z]\\.$', all_sponsors$LES_sponsor), str_extract(all_sponsors$LES_sponsor, " [a-z]$|, [a-z]\\.$|, [a-z]$|, [a-z][a-z]+| [a-z]\\.$"), '')
all_sponsors$first_name <- str_trim(gsub(', |\\.$', '', all_sponsors$first_name))
#all_sponsors$first_name <- gsub('\\..+| .+', '', all_sponsors$first_name)

all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update Names for Matching --> FIXING DUPLICATES WITH COUNTY IDs
# filter(all_sponsors, grepl('\\(', LES_sponsor)) 
# filter(klarner, grepl("^(b|s)uttry", cand)) %>% select(cand, sen, year, outcome, ddez, candid) %>% filter(year == 2002)
if(t_yrs == "1995_1996"){
  all_sponsors[all_sponsors$LES_sponsor == 'cole (carter)', c('last_name', 'first_name')] <- list('cole', 'ralph')
  all_sponsors[all_sponsors$LES_sponsor == 'cole (dyer)', c('last_name', 'first_name')] <- list('cole', 'ronnie')
  all_sponsors[all_sponsors$LES_sponsor == 'jones u (shel)', c('last_name', 'first_name')] <- list('jones', 'u')
  all_sponsors[all_sponsors$LES_sponsor == 'jones r (shel)', c('last_name', 'first_name')] <- list('jones', 'rufus')
  all_sponsors[all_sponsors$LES_sponsor == 'jones s.', c('last_name', 'first_name')] <- list('jones', 'sherry')
  all_sponsors[all_sponsors$LES_sponsor == 'turner (ham)', c('last_name', 'first_name')] <- list('turner', 'brenda')
  all_sponsors[all_sponsors$LES_sponsor == 'turner (shelby)', c('last_name', 'first_name')] <- list('turner', 'larry')
  all_sponsors[all_sponsors$LES_sponsor == 'williams (unio)', c('last_name', 'first_name')] <- list('williams', 'micheal') 
  all_sponsors[all_sponsors$LES_sponsor == 'williams (wil)', c('last_name', 'first_name')] <- list('williams', 'mike') 
}else if(t_yrs == "1997_1998"){
  all_sponsors[all_sponsors$LES_sponsor == 'clabough (h)', c('last_name', 'first_name')] <- list('clabough', 'bill')
  all_sponsors[all_sponsors$LES_sponsor == 'cole (carter)', c('last_name', 'first_name')] <- list('cole', 'ralph')
  all_sponsors[all_sponsors$LES_sponsor == 'cole (dyer)', c('last_name', 'first_name')] <- list('cole', 'ronnie')
  all_sponsors[all_sponsors$LES_sponsor == 'jones u (shel)', c('last_name', 'first_name')] <- list('jones', 'u')
  all_sponsors[all_sponsors$LES_sponsor == 'turner (ham)', c('last_name', 'first_name')] <- list('turner', 'brenda')
  all_sponsors[all_sponsors$LES_sponsor == 'turner (shelby)', c('last_name', 'first_name')] <- list('turner', 'larry')
  # MIke missing from list, but in photo - http://www.capitol.tn.gov/house/archives/100GA/100GA.htm 
  all_sponsors[all_sponsors$LES_sponsor == 'walker (blount)', c('last_name', 'first_name')] <- list('walker', 'mike') 
  all_sponsors[all_sponsors$LES_sponsor == 'walker (rhea)', c('last_name', 'first_name')] <- list('walker', 'raymond')
  all_sponsors[all_sponsors$LES_sponsor == 'williams (wil)', c('last_name', 'first_name')] <- list('williams', 'mike') 
}else if(t_yrs == "1999_2000"){
  all_sponsors[all_sponsors$LES_sponsor == 'cole (carter)', c('last_name', 'first_name')] <- list('cole', 'ralph')
  all_sponsors[all_sponsors$LES_sponsor == 'cole (dyer)', c('last_name', 'first_name')] <- list('cole', 'ronnie')
  all_sponsors[all_sponsors$LES_sponsor == 'davis (cocke)', c('last_name', 'first_name')] <- list('davis', 'ronnie')
  all_sponsors[all_sponsors$LES_sponsor == 'davis (wash)', c('last_name', 'first_name')] <- list('davis', 'david')
  all_sponsors[all_sponsors$LES_sponsor == 'turner (ham)', c('last_name', 'first_name')] <- list('turner', 'brenda')
  all_sponsors[all_sponsors$LES_sponsor == 'turner (shelby)', c('last_name', 'first_name')] <- list('turner', 'larry')
  all_sponsors[all_sponsors$LES_sponsor == 'jones u (shel)', c('last_name', 'first_name')] <- list('jones', 'u')
  all_sponsors[all_sponsors$LES_sponsor == 'walker (rhea)', c('last_name', 'first_name')] <- list('walker', 'raymond')
  all_sponsors[all_sponsors$LES_sponsor == 'williams (wil)', c('last_name', 'first_name')] <- list('williams', 'mike') 
  all_sponsors[all_sponsors$LES_sponsor == 'springer, p',]$first_name <- 'kenneth' # Kenneth "Pete" Springer
}else if(t_yrs == "2001_2002"){
  all_sponsors[all_sponsors$LES_sponsor == 'cole (carter)', c('last_name', 'first_name')] <- list('cole', 'ralph')
  all_sponsors[all_sponsors$LES_sponsor == 'cole (dyer)', c('last_name', 'first_name')] <- list('cole', 'ronnie')
  all_sponsors[all_sponsors$LES_sponsor == 'davis (cocke)', c('last_name', 'first_name')] <- list('davis', 'ronnie')
  all_sponsors[all_sponsors$LES_sponsor == 'davis (wash)', c('last_name', 'first_name')] <- list('davis', 'david')
  all_sponsors[all_sponsors$LES_sponsor == 'jones u (shel)', c('last_name', 'first_name')] <- list('jones', 'u')
  all_sponsors[all_sponsors$LES_sponsor == 'turner (dav)', c('last_name', 'first_name')] <- list('turner', 'mike')
  all_sponsors[all_sponsors$LES_sponsor == 'turner (ham)', c('last_name', 'first_name')] <- list('turner', 'brenda')
  all_sponsors[all_sponsors$LES_sponsor == 'turner (shelby)', c('last_name', 'first_name')] <- list('turner', 'larry')
}else if(t_yrs == "2003_2004"){
  all_sponsors[all_sponsors$LES_sponsor == 'brooks (knox)', c('last_name', 'first_name')] <- list('brooks', 'harry')
  all_sponsors[all_sponsors$LES_sponsor == 'brooks (shelby)', c('last_name', 'first_name')] <- list('brooks', 'henri')
}else if(t_yrs == '2005_2006'){
  all_sponsors[all_sponsors$LES_sponsor == 'brooks (knox)', c('last_name', 'first_name')] <- list('brooks', 'harry')
  all_sponsors[all_sponsors$LES_sponsor == 'brooks (shelby)', c('last_name', 'first_name')] <- list('brooks', 'henri')
}else if(t_yrs == '2007_2008'){
  all_sponsors[all_sponsors$LES_sponsor == 'brooks h', c('last_name', 'first_name')] <- list('brooks', 'harry')
}


#### Fix Repeat Name Issues
if(t >= 1995 & t <= 2018){ # Left office Jan 2019
  all_sponsors[all_sponsors$LES_sponsor %in% c("halteman harwel", "harwell"),]$last_name <- 'harwellhaltman'
}
if (t >= 2005 & t <= 2012){
  all_sponsors[all_sponsors$LES_sponsor %in% c("marrero", "marrero b"),]$last_name <- 'robinsonmarrero'
}
if(t >= 1999 & t <= 2003){
  all_sponsors[all_sponsors$LES_sponsor == 'hagood',]$last_name <- 'woodson'
}




all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(LES_sponsor,substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name))

session_num = 1/2 * (t - 2019) + 111

legiscan = read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{gsub('_','-',t_yrs)}_{scales::ordinal(c(session_num))}_General_Assembly/csv/people.csv")) %>% 
  select(-nickname) %>%  distinct()


if(t_yrs == "2019_2020") {
  legiscan = bind_rows(legiscan,
                       read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{gsub('_','-','2021_2022')}_{scales::ordinal(c(session_num+1))}_General_Assembly/csv/people.csv")) %>% 
                         filter(people_id == 21777))
}
if(t_yrs == "2021_2022") {
  legiscan = bind_rows(legiscan,
                       legiscan %>% filter(people_id == 20192) %>% mutate(role = "Sen", district = "SD-033"))
}




legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = case_when(
    n >= 2 ~ paste0(last_name, " ",substr(first_name,1,1)),
    T ~ last_name)) %>%
  mutate(match_name_chamber = tolower(paste(match_name,substr(district,1,1),sep="-")))

# inexact::inexact_addin()
# left: legiscan_adj
# right: all_sponsors
# method: osa
# mode: full

if(t_yrs == "2019_2020"){
  # You added custom matches:
  all_sponsors2 = # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "brooks h-h" = NA_character_,
        "fitzhugh-h" = NA_character_,
        "brooks k-h" = NA_character_,
        "alexander-h" = NA_character_,
        "matheny-h" = NA_character_,
        "favors-h" = NA_character_,
        "overbey-s" = NA_character_,
        "harris-s" = NA_character_,
        "sargent-h" = NA_character_,
        "jones-h" = NA_character_,
        "lollar-h" = NA_character_,
        "turner-h" = NA_character_,
        "forgety-h" = NA_character_,
        "wirgau-h" = NA_character_,
        "doss-h" = NA_character_,
        "gravitt-h" = NA_character_,
        "mcdaniel-h" = NA_character_,
        "harwell-h" = NA_character_,
        "white m-h" = "white-h",
        "johnson j-s" = "johnson-s",
        "butt-h" = NA_character_,
        "kane-h" = NA_character_,
        "goins-h" = NA_character_,
        "rogers-h" = NA_character_,
        "pitts-h" = NA_character_
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
        "overbey-s" = NA_character_,
        "harris l-s" = NA_character_,
        "martin-h" = NA_character_,
        "johnson j-s" = "johnson-s",
        "campbell h-s" = "campbell-s"
      )
    )
  
  
}

if(t_yrs == "2023_2024"){
  all_sponsors2 =
    # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "johnson j-s" = "johnson-s",
        "campbell h-s" = "campbell-s",
        "davis e-h" = "davis-h"
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
# filter(bills, !(bills$primary_sponsor %in% legis_data$data_name))
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

#### Manual check to make sure number of sponsored bills in LES matches number on state legislative website for each legislator
# House (https://wapp.capitol.tn.gov/apps/LegislatorInfo/directory.aspx?chamber=H&ga=113)
View(LES %>% filter(chamber == "House") %>% select(sponsor, data_name, district, all_bills))
# Senate (https://wapp.capitol.tn.gov/apps/LegislatorInfo/directory.aspx?chamber=S&ga=113)
View(LES %>% filter(chamber == "Senate") %>% select(sponsor, data_name, district, all_bills))

############### Save LES
write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
rm(LES)


cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************"))


################# ****** END LOOP

rm(all_sponsors, bills, legis_data, SS_bills, H_elec_year, S_elec_year, i, keep_types)
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES) # 
rm(t, terms, klarner_gs, m_sub, match_name2, c_sub, sn) # nonspon, unique_cospon, 
rm(commem_bills, search_term, t_sessions)


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by SPECIAL ELECTION (> 1 year left in term) OR COUNTY DELEGATION VOTE --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
### HOUSE ROSTERS: http://www.capitol.tn.gov/house/archives/
### SENATE ROSTERS: http://www.capitol.tn.gov/senate/archive/
### -----> OR http://www.capitol.tn.gov/senate/archives/105GA
### Wayback Machine (Members AND Committees): https://web.archive.org/web/19970807093638/http://www.legislature.state.tn.us/
#########################


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1996 TERM! ~~~~~~~~~~~~~~ 
#   session chamber    N  AIC  ABC PASS LAW
# 1      RS       H 3332 2133 1478  718 632
# 2      RS       S 3322 2065 1196  743 668
### IN HOUSE:
# -- hicks, bobby

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
#   session chamber    N  AIC  ABC PASS LAW
# 1      RS       H 3451 2177 1507  834 744
# 2      RS       S 3445 2226 1465  682 582
### APPOINTED/WON SPECIAL ~ HOUSE:
# -- walker (blount) == mike, name duplicated --> won't print -- in photo - http://www.capitol.tn.gov/house/archives/100GA/100GA.htm
### APPOINTED/WON SPECIAL ~ SENATE:
# -- CLABOUGH (bill, via H)
### IN HOUSE:
# -- hicks, bobby


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
#   session chamber    N  AIC  ABC PASS LAW
# 1      RS       H 3368 2425 1383  705 601
# 2      RS       S 3354 2227 1360  691 557
### APPOINTED/WON SPECIAL ~ SENATE:
# -- CLABOUGH (bill, via H, 1997)
# -- SPRINGER, J (janice, Name duplicate = Won't print) -- Appointed upon death of her husband, Pete Springer -- https://www.nashvillepost.com/politics/article/20450759/late-senators-son-to-challenge-incumbent
### NAME FIX:
# -- Jamie Hagood == Jamie Woodson
# -- Springer, P == Kenneth 'Pete' Springer -- Died April 2000, in office. -- https://www.nashvillepost.com/business/people/obituaries/article/20446877/state-senator-pete-springer-dies
### DROP:
# -- koella, carl jr. -- died january 1998 -- https://www.findagrave.com/memorial/91846140/carl-ohm-koella

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
#   session chamber    N  AIC  ABC PASS LAW
# 1      RS       H 3301 2118 1205  534 459
# 2      RS       S 3255 2063 1245  709 591
### APPOINTED/WON SPECIAL ~ HOUSE:
# -- CASADA (glen)
### DROP: 
# -- springer, kenneth n. (pete) -- died T-1


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
#   session chamber    N  AIC  ABC PASS LAW
# 1      RS       H 3619 2099 1171  666 608
# 2      RS       S 3529 1985 1234  583 496
### APPOINTED/WON SPECIAL ~ HOUSE:
# -- MARRERO (beverly robinson)
### APPOINTED/WON SPECIAL ~ SENATE:
# -- KILBY (tommy)
# -- WALKER M (mike, not in klarner but seated in 2003 in 12th district) --> http://www.capitol.tn.gov/bills/103/Senate/Journals/01142003od1.pdf
### DROP:
# -- davis, lincoln -- resigned after winning US House seat


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
#   session chamber    N  AIC  ABC PASS LAW
# 1      RS       H 4115 2544 1257  541 491
# 2      RS       S 4052 2390 1420  830 661
# 3     SS1       H   22    3    3    1   1
# 4     SS1       S   15    2    3    2   2
### APPOINTED/WON SPECIAL ~ HOUSE:
# -- ROWE (gary)
# -- WATSON, E (eric) -- Name duplicated, won't print
### APPOINTED/WON SPECIAL ~ SENATE:
# -- BOWERS (kathryn, via H)
# -- CHISM (sidney, interim replacement after roscoe dixon resigned and before bowers won special) -- https://sharetngov.tnsosfiles.com/sos/bluebook/05-06/2-senate.pdf
# -- FORD, O. (ophelia, won her brother john's seat after he resigned in 2005 --> Name duplicated, won't print)
### DROP:
# -- dixon, roscoe (resigned january 2005) -- https://sharetngov.tnsosfiles.com/sos/bluebook/05-06/2-senate.pdf


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
#   session chamber    N  AIC  ABC PASS LAW
# 1      RS       H 4273 2894 1460  611 545
# 2      RS       S 4278 2591 1585  948 769
### APPOINTED/WON SEPCIAL ~ HOUSE:
# -- HARDAWAY (g.a.)
# -- RICHARDSON (jeanne)
### APPOINTED/WON SEPCIAL ~ SENATE:
# -- BERKE (andy)
# -- MARRERO B (beverly, via H)
# -- ROLLER (steve, D, january 2008) -- https://www.chattanoogan.com/2008/4/11/125682/Sen.-Steve-Roller-Seeking-Full-Term.aspx
### DROP:
# -- brooks, henri e. -- resigned august 2006, http://www.tnamp.com/legislative/2007/01/10/new-faces-in-the-tennessee-state-legislature.608031
# -- cohen, stephen (steve) -- resigned to take congressional seat -- http://www.tnamp.com/legislative/2007/01/10/new-faces-in-the-tennessee-state-legislature.608031


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
#   session chamber    N  AIC  ABC PASS LAW
# 1      RS       H 4020 2725 1395  646 557
# 2      RS       S 3977 2274 1449  789 673
# 3     SS1       H   22    4    4    0   0
# 4     SS1       S   20    5    4    4   4
#### APPOINTED/WON SPECIAL ~ HOUSE:
# -- MARSH (pat)
# -- TURNER J (johnnie, succeeded larry turner in january 2010)
# -- WHITE (mark)
#### APPOINTED/WON SPECIAL ~ SENATE:
# -- HAILE (ferrell, 11/22/2010 until 3/8/2011, then won in 2012 election)
# -- KELSEY (brian)
### IN HOUSE:
# -- williams, kent
### Name Fix:
# -- No 'daniel' in chamber; just on one bill --> Odom -- Not clear why the bill page has a mistake


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
#   session chamber    N  AIC  ABC PASS LAW
# 1      RS       H 3884 2527 1300  592 546
# 2      RS       S 3811 1964 1318  706 642
### APPOINTED/WON SPECIAL ~ HOUSE:
# -- PARKINSON (antonio)
### APPOINTED/WON SEPCIAL ~ SENATE:
# -- HAILE (ferrell, 11/22/2010 until 3/8/2011, then won in 2012 election)
# -- MASSEY (becky, 11/2011)
# -- ROBERTS (kerry, march 2011)
### DROP:
# jones, ulysses jr. -- died Nov 9, 2010
# black, diane -- won US House seat, January 2011


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
#   session chamber    N  AIC  ABC PASS LAW
# 1      RS       H 2555 2082 1164  411 382
# 2      RS       S 2649 1793 1200  765 715
### APPOINTED/WON SPECIAL ~ HOUSE:
# -- AKBARI (raumesh)
# -- BAILEY (paul)
### APPOINTED/WON SPECIAL ~ SENATE:
# -- BRIGGS (richard)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
#   session chamber    N  AIC  ABC PASS LAW
# 1      RS       H 2666 2118 1212  423 400
# 2      RS       S 2691 1311 1250  803 755
# 3     SS2       H    2    2    2    0   0
# 4     SS2       S    2    2    2    2   2
### APPOINTED/WON SPECIAL ~ HOUSE:
# -- HICKS (gary)
# -- JENKINS (jamie, R, D 94)
# -- ZACHARY (jason)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
#   session chamber    N  AIC  ABC PASS LAW
# 1      RS       H 2727 2123 1206  478 448
# 2      RS       S 2758 1327 1223  724 678
### APPOINTED/WON SPECIAL ~ HOUSE: 
# -- BOYD (clark)
# -- MOON (jerome)
# -- VAUGHAN (kevin)
### APPOINTED/WON SPECIAL ~ SENATE:
# -- PODY (mark)
# -- SWANN (art)
### IN SENATE:
# -- lovell, mark -- Forced to resign after a couple months: https://www.tennessean.com/story/news/politics/2017/02/18/schmoozing-boozing-and-quiet-resignation-mark-lovells-100-days-capitol-hill/98040304/
### DROP:
# -- mcnally, james rand (randy) -- Elected LT Gov in Nov 2016


# filter(klarner, grepl("boyd", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 33 & sen == 1 & outcome == "w") %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)



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

###### Quick Name Fix
LES[LES$sponsor %in% c("marrero", "robinsonmarrero, b"),]$sponsor <- "marrero"

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
# LES[LES$data_name %in% "daniel",]$klarner_id <- NA
# LES[LES$data_name %in% "daniel",]$klarner_name <- NA
# LES[LES$data_name %in% "daniel",]$sponsor <- 'kuhn, john r.'

### ****Still missing***** ---> Rest are not in Klarner or 2017-2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[6]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl('hick', cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, missing, name_sub, still_missing)


### Fix Missing
name_matches <- data.frame(LES_name = 'white', k_name = 'white, mark')
name_matches <- add_row(name_matches, LES_name = 'hicks', k_name = 'hicks, gary w., jr.')
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
rm(check_dup, k_sub, exact, missing, name_sub, t)


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
# - http://www.capitol.tn.gov/house/archives/100GA/h8b.htm
fill_missing <- data.frame(LES_name = "walker, mike", new_name = 'walker, mike', party = 'r', district = 8, exper = 'none')
# - unclear if same mike walker; unclear district
fill_missing <- add_row(fill_missing, LES_name = "walker, m", new_name = 'walker, mike', party = 'r', district = NA, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "springer, j", new_name = 'springer, janice', party = 'd', district = 25, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "chism", new_name = 'chism, sidney', party = 'd', district = 17, exper = 'none') # Interim until Bowers elected
fill_missing <- add_row(fill_missing, LES_name = "roller", new_name = 'roller, steve', party = 'd', district = 14, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "jenkins", new_name = 'jenkins, jamison', party = 'r', district = 94, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

#### *** 2017_2018: If any of below run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "vaughan",]$party <- 'r'
LES[LES$sponsor == "vaughan",]$sponsor <- 'vaughan, kevin'
LES[LES$sponsor == "boyd",]$party <- 'r'
LES[LES$sponsor == "boyd",]$sponsor <- 'boyd, clark'

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t, fill_missing, i)

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
      ideo_match <- filter(ideo_match, match_name == gsub(' [a-z]\\.$', '', gsub(' \\(.+', '', LES[i,]$sponsor)))
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
LES[LES$sponsor %in% c('byrd, dan r.', 'cole, ralph'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches: 
# -- Larry 'Ken' Givens -- https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=122423
# -- Charles 'Bill' Sanderson
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1991_1992', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('waymon', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'bell, joe', SM_name = 'Bell')
#name_matches <- add_row(name_matches, LES_name = 'barker, judy', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'burks, tommy', SM_name = 'Burks')
name_matches <- add_row(name_matches, LES_name = 'byrd, dan r.', SM_name = 'Byrd')
name_matches <- add_row(name_matches, LES_name = 'cantrell, bruce', SM_name = 'Cantrall')
name_matches <- add_row(name_matches, LES_name = 'carter, bobby', SM_name = 'Carter, Robert')
#name_matches <- add_row(name_matches, LES_name = 'cobb, ty', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'cole, ralph', SM_name = 'Cole, William Ralph')
name_matches <- add_row(name_matches, LES_name = 'crowe, dewey e. (rusty)', SM_name = 'Crowe, Rusty')
# name_matches <- add_row(name_matches, LES_name = 'faulkner, chad', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'ford, dale', SM_name = 'Ford, Robert') # Robert Dale Ford -- https://en.wikipedia.org/wiki/Dale_Ford
name_matches <- add_row(name_matches, LES_name = 'hicks, gary w., jr.', SM_name = 'Hicks, Gary')
# name_matches <- add_row(name_matches, LES_name = 'jones, rufus', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'kyle, jim', SM_name = 'Kyle Jr, James F')
name_matches <- add_row(name_matches, LES_name = 'ramsey, bob', SM_name = 'Ramsey, Robert')
name_matches <- add_row(name_matches, LES_name = 'sargent, charles m. jr.', SM_name = 'Sargent Jr, Charles M')
# name_matches <- add_row(name_matches, LES_name = 'wilburn, leigh rosser', SM_name = 'zzzzzzz')
### -- SM's Larry Williams should be Mike Williams here: District 63, overlapping time -- https://web.archive.org/web/20001211092600/http://www.legislature.state.tn.us/House/Members/h63.htm
name_matches <- add_row(name_matches, LES_name = 'williams, mike', SM_name = 'Williams, Larry')
name_matches <- add_row(name_matches, LES_name = 'yokley, eddie', SM_name = 'Yorkley')
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

# *** Waymon 'Kent' Williams -- Removed from Republican Party in 2009 after voting with 49 Dems to become speaker; https://prabook.com/web/waymon_kent.williams/877102
# -- Removed from party in Feb 2009, so adjust that term
LES[LES$sponsor == 'williams, kent' & LES$term == "2009_2010",]$party <- 'nonmaj'
LES[LES$sponsor == 'williams, kent' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Williams, Waymon' & ideo$party == 'R',]$name
LES[LES$sponsor == 'williams, kent' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Williams, Waymon' & ideo$party == 'R',]$party
LES[LES$sponsor == 'williams, kent' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Williams, Waymon' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'williams, kent' & LES$party == 'nonmaj',]$SM_name <-  ideo[ideo$name == 'Williams, Waymon' & ideo$party == 'X',]$name
LES[LES$sponsor == 'williams, kent' & LES$party == 'nonmaj',]$SM_party <- ideo[ideo$name == 'Williams, Waymon' & ideo$party == 'X',]$party
LES[LES$sponsor == 'williams, kent' & LES$party == 'nonmaj',]$np_score <- ideo[ideo$name == 'Williams, Waymon' & ideo$party == 'X',]$np_score

#### Dewey E. 'Rusty' Crowe & Milton H. Hamilton Jr 
# -- Both switched D to R in September 1995: https://books.google.com/books?id=ES2VsDc1WeYC&pg=PA68&lpg=PA68&dq=milton+hamilton+tennessee+party+switch&source=bl&ots=n4OB4b3pMV&sig=ACfU3U1BmSFANLNCa4riJrcEWqUSIdN1tA&hl=en&sa=X&ved=2ahUKEwiqyZrMxt7oAhUE-6wKHdXjBcYQ6AEwAXoECCsQKQ#v=onepage&q=milton%20hamilton%20tennessee%20party%20switch&f=false
# -- No D row for either, so recoding 1995/1996 to R
LES[LES$sponsor == 'crowe, dewey e. (rusty)' & LES$term %in% c("1995_1996", "1997_1998"),]$party <- 'r'
LES[LES$sponsor == 'hamilton, milton h. jr.' & LES$term %in% c("1995_1996"),]$party <- 'r'

#### Charlottee Burks -- Elected as writein after husband was murdered by republican opponent!
# -- Aside: murdered was Byron Low-Tax Looper, who legally changed his name from Anthony to Low-Tax
LES[LES$sponsor == 'burks, charlotte' & LES$term %in% c("1999_2000", "2001_2002"),]$party <- 'd'

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

##### Drop Nicknames
# filter(LES, grepl('\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct() %>% as.data.frame()
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct() %>% arrange(sponsor) 
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)
LES[LES$sponsor == 'wood, bobby 1',]$sponsor <- 'wood, bobby g.'
LES[LES$sponsor == 'davis, ronnie 1',]$sponsor <- 'davis, ronnie e.'
LES[LES$sponsor == 'deberry, lois 1',]$sponsor <- 'deberry, lois marie'
LES[LES$sponsor == 'hall, steve 2',]$sponsor <- 'hall, steve' # Can't find any info, but steve 1 is in past

#### Manual Fixes
LES[LES$sponsor == 'harwellhaltman, beth',]$sponsor <- 'harwell, beth halteman' # halteman previously mispelled too
LES[LES$sponsor == 'woodson, jamie',]$sponsor <- 'woodson, jamie hagood'
LES[LES$sponsor == 'pleasant, w. c.',]$sponsor <- 'pleasant, william clyde'
LES[LES$sponsor == 'elsea, foster e.',]$sponsor <- 'elsea, foster eugene'
# LES[LES$sponsor == 'zzzzz',]$sponsor <- 'zzzzzz'


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1995 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1991:2008) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2011:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1
###LES[LES$chamber == 'House' & LES$term == "2009_2010",]$in_majority <- 0

### 2009: Kent Williams was Speaker; Removed from R Party after voting with 49 Dems to take office...
# ---> For 2009: in_majority = Dems and Kent Williams?
# ---> Williams referred to himself as 'Carter County Republican' after being removed
# ---> BUT Reps still granted a lot of committee chairs 
# ---> AND Reps won full control in January 2010 special (but Williams remained speaker)
# **** ~~~~> Coding BOTH PARTIES as in_majority
#LES[LES$sponsor == 'williams, kent' & LES$term == "2009_2010",]$in_majority <- 1
LES[LES$term == "2009_2010" & LES$chamber == "House",]$in_majority <- 1

### Senate -- 1995 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2004) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2005:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1
# -- 1995-1996 -- Control Shifted D-to-R mid-term following resignations/party switches --> Both parties in Majority
LES[LES$chamber == 'Senate' & LES$term == "1995_1996",]$in_majority <- 1
# -- 2007-2008: Per Ballotpedia, Split control, but Republicans held speakership and nearly all committee chairs..  so keeping R Control
# --> See: http://www.capitol.tn.gov/bills/105/Senate/Journals/01092007od1.PDF
# --> See: http://www.capitol.tn.gov/senate/archives/105GA/Committees/scommemb.htm


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

