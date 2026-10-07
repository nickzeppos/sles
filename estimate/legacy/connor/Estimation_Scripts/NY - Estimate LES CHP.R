#################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** NEW YORK *** BY TERM
#################################################################

#####################
##### STATE-SPECIFIC NOTES:
#####################
# SPECIAL SESSIONS --- Bills from special terms are just included in the full set of bills -- session does not appear to restart
####################

rm(list=ls())
options(stringsAsFactors = FALSE, scipen = 100)

library(tidyr)
library(dplyr)
library(stringr)
library(ggplot2)
library(humaniformat)
library(purrr)
library(readxl)
library(fastLink)
library(glue)
library(readr)
library(inexact)
library(foreach)
library(tibble)

this_state <- 'NY'
keep_types <- c("A", "S")


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
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS ####
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
         bill_id = sub("[A-Za-z]$", "", bill_id),
         bill_id = paste0(gsub("[0-9].+", '', bill_id), str_pad(gsub("^[A-Z]+", "", bill_id), 5, pad = "0")),
         SS = 1) %>%
  select(state = State, term, year, bill_id, everything())

###### term IN PROGRESS
print(glue(' \n ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM!  ~~~~~~~~~~~~~~~~~~ \n'))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t}.csv")
bills <- read.csv(bill_path)
bills$term <- t_yrs
bills$session <- NA

### Check for duplicates
if(nrow(bills) != nrow(distinct(bills))){
  cat(" \n ~~~> DUPLICATE BILLS \n\n .")
  bills = distinct(bills)
}

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_number)

############### Drop Resolutions 
# In NY, Resolutions start with B, C, E, J, K, L, R
all_bills <- bills
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)

############### Standardize Sponsors
### (MS) in sponsor name indicates a multisponsored bill 
### https://nyassembly.gov/Rules/?sec=r3#s3
bills$LES_sponsor <- str_trim(gsub("\\(ms\\)", '', tolower(bills$sponsor)))
bills$LES_sponsor <- gsub('rules \\(|\\)$', '', bills$LES_sponsor)


#### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
bills$LES_sponsor <- gsub('á', 'a', bills$LES_sponsor)
bills$LES_sponsor <- gsub('é', 'e', bills$LES_sponsor)
bills$LES_sponsor <- gsub('ó', 'o', bills$LES_sponsor)
bills$LES_sponsor <- gsub('í', 'i', bills$LES_sponsor)
bills$LES_sponsor <- gsub('ñ', 'n', bills$LES_sponsor)

###################
###### Merge in S&S Bills ####
###################
# *** For NEW YORK: One long biennial session --> Merge on Bill ID, can ignore sessions

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

################################################
############### Code Commemorative
################################################

bills <- commem_bills %>%
  select(bill_id, term, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term'))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)


### Dropping committee bills without clear sponsor information 
### NOTE: It seems only rules can introduce bills on behalf of a member?
# ---> See: https://nyassembly.gov/Rules/?sec=r4#s10
# -->  "At any time during the term, a bill or resolution may be introduced by the Committee on Rules and shall be referred to a committee; 
# provided however that all bills shall be referred to a standing committee other than the Committee on Rules, for consideration. 
# A bill or resolution introduced at the request of a member shall, if the member so requests, have his or her name included on both the original and printed 
# copies of the bill or resolution as follows:" 
if(any(bills$LES_sponsor %in% c("budget", "rules", "redistricting"))){
  print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor %in% c('budget', 'rules', 'redistricting')))} bill(s) sponsored by COMMITTEE"))
  bills <- filter(bills, !(LES_sponsor %in% c("budget", "rules", "redistricting")))
}


### CHeck Missing Sponsors
# filter(bills, primary_sponsor == "") %>% View()
if(any(bills$LES_sponsor == "")){
  print(glue(" ~~> Dropping {nrow(filter(bills, LES_sponsor == ''))} bills without a sponsor"))
  bills <- filter(bills, LES_sponsor != "") 
}

################################################
############### Code Bill History
################################################

bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t}.csv")
bill_hist <- read.csv(bill_hist_path)

# Note bills that are substituted for multiple bills
mul_sub_bills <- bill_hist %>% filter(grepl("substituted for", tolower(action))) %>% 
  mutate(sub_bill = paste0(toupper(str_sub(gsub("substituted for |substituted by ", "", action), 1, 1)), 
                           str_pad(gsub("\\D+", "", action), width = 5, side = "left", pad = "0"))) %>% 
  group_by(bill_number) %>% 
  filter(bill_number != sub_bill) %>%
  filter(n_distinct(sub_bill) > 1) %>% 
  pull(bill_number) %>% unique()

######## Standardize BillHist Bill IDs
bill_hist <- rename(bill_hist, bill_id = bill_number)
bill_hist$term <- t_yrs
bill_hist$session <- NA

### Code Chamber
bill_hist$chamber <- ifelse(bill_hist$chamber == "Assembly", "House", "Senate")
#bill_hist$chamber <- ifelse(toupper(bill_hist$action) == bill_hist$action, "Senate", "House")

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
source('../../Estimate LES/code_billhist_fx.R')

### Set State-Specific Terms for Identifying Each Stage
# https://www.nysenate.gov/how-bill-becomes-law-1
# https://www.brennancenter.org/sites/default/files/legacy/d/albanyreform_finalreport.pdf
# http://documents.nycbar.org/files/legislativeglossary.pdf

aic_t <- c("^reported", '^1st report', "held for consideration", "died in committee", "committee consideration")
# If lacks majority support, dies in comm --> implies failed vote? http://www.nyc.gov/html/moiga/pages/state/process.shtml
# Key point = not all bills have reported or died meaning some just see no action
# Remmoving "to attorney-general for opinion" from AIC - not clear its not required
abc_t <- c("^reported", '^(1st|2nd) report', "third reading", "3rd reading cal", "amended on third") 
### print number [0-9]+[a-z] - print number a/b/c means number changed to 123A/B/C after amended
### Dropping print number as leads to errors (sponsor can pull bill, amend, and reintroduce --> Print number change)
### Also Dropping amend and recommit
pc_t <- c("passed assem", "repassed assem", "passed sen", "repassed sen", "^delivered to assembly", "^delivered to senate", "^delivered to gov")
### account for 'vote reconsidered - restored to third reading' ???
law_t <- c("^signed chap", "^chapter")

### Check Actions
# filter(bill_hist, grepl('^committee discharged', tolower(action))) %>% distinct(action) %>% View()

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

### Session Var
bills$session <- bill_hist$session <- t_yrs

### Code History Stages for Each Bill --- Subset Hist File, Code, Save, Repeat
options(warn = 2)
for(i in 1:nrow(bills)){
  b_id = bills[i,]$bill_id
  b_spon = bills[i,]$LES_sponsor
  hist_sub <- filter(bill_hist, bill_id == b_id)
  # check if there is a substitution
  if(any(grepl("substituted by", hist_sub$action, ignore.case=T)) ){
    # take the last substituted by
    action = hist_sub %>% 
      filter(grepl("substituted by",action,ignore.case=T)) %>% 
      slice_tail(n=1) %>% 
      pull(action) 
    action_order = hist_sub %>% 
      filter(grepl("substituted by",action,ignore.case=T)) %>% 
      slice_tail(n=1) %>% 
      pull(order) 
    action = gsub("substituted by ","",action,ignore.case=T)
    sub_bill = paste0(
      str_to_upper(str_extract(action, "^[saSA]")),
      str_pad(str_extract(action, "(?<=^[saSA])\\d+"), 5, pad = "0")
    )
    sub_bill_hist <- filter(bill_hist, bill_id == sub_bill)
    # sub_bill_hist <- filter(hist_sub, order > action_order)
    # Take care of bill with multiple histories
    if (sub_bill %in% mul_sub_bills) {
      bill_num <- as.character(as.numeric(gsub("\\D+", "", b_id)))
      sub_row <- min(c(1:nrow(sub_bill_hist))[grepl(bill_num, tolower(sub_bill_hist$action))])
      if (sum(grepl("substitution reconsidered", tolower(sub_bill_hist$action))) > 0){
        reconsidered_row <- min(c(1:nrow(sub_bill_hist))[grepl("substitution reconsidered", tolower(sub_bill_hist$action))])
        if (sub_row < reconsidered_row){
          sub_bill_hist <- sub_bill_hist[1:reconsidered_row, ]
        }
      } else if (sum(grepl("^died", tolower(sub_bill_hist$action))) > 0){
        died_row <- min(c(1:nrow(sub_bill_hist))[grepl("^died", tolower(sub_bill_hist$action))])
        if (sub_row < died_row){
          sub_bill_hist <- sub_bill_hist[1:died_row, ]
        }
      }
    }
    hist_sub <- bind_rows(hist_sub, sub_bill_hist) %>% distinct(bill_id, session, chamber, action_date, action, term) %>% 
      arrange(action_date) %>% mutate(order = row_number())
    # bill_stages_sub <- evaluate_bill_hist(sub_bill_hist, 
    #                                       ifelse(substr(sub_bill,1,1) == "A", "S", "A"), 
    #                                       t_yrs, t_yrs, b_spon, aic_t, abc_t, pc_t, law_t,
    #                                       ignore_chamber_switch = TRUE) %>% 
    #   mutate(bill_id = sub_bill)
    # # override PASS and LAW
    # bill_stages$passed_chamber = max(bill_stages$passed_chamber, bill_stages_sub$passed_chamber)
    # bill_stages$law = max(bill_stages$law, bill_stages_sub$law)
    rm(sub_bill_hist, action,sub_bill)
  }
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, t_yrs, b_spon, aic_t, abc_t, pc_t, law_t,
                                    ignore_chamber_switch = TRUE)
  bill_stages$bill_url <- bills[i,]$bill_url
  originating_chamber = ifelse(substr(b_id,1,1)=="A","House","Senate")
  # check if there is a reconsideration/bill died
  if(bill_stages$passed_chamber == 1 & any(grepl("vote reconsidered|died", hist_sub$action[hist_sub$chamber==originating_chamber], ignore.case=T)) ){
    last_row_reconsidered = max(hist_sub %>% 
                                  filter(chamber == originating_chamber) %>% 
                                  filter(grepl("vote reconsidered",action,ignore.case=T)) %>% 
                                  slice_tail(n=1) %>% pull(order),0)
    last_row_died = max(hist_sub %>% 
                          filter(chamber == originating_chamber) %>% 
                          filter(grepl("died",action,ignore.case=T)) %>% 
                          slice_tail(n=1) %>% pull(order),0)
    max_pass_row = max(hist_sub %>% 
                         filter(chamber == originating_chamber) %>% 
                         filter(grepl(paste(pc_t, collapse="|"),action,ignore.case=T)) %>% 
                         slice_tail(n=1) %>% pull(order),0)
    sum_pass_row = hist_sub %>% 
      filter(chamber == originating_chamber) %>% 
      filter(grepl(paste(pc_t, collapse="|"),action,ignore.case=T) & 
               !grepl("deliver", action, ignore.case=T)) %>% 
      nrow()
    sum_reconsider_row = hist_sub %>% 
      filter(chamber == originating_chamber) %>% 
      filter(grepl("vote reconsidered",action,ignore.case=T)) %>% 
      nrow()
    if( (last_row_reconsidered > max_pass_row & ! (sum_pass_row > sum_reconsider_row)) |
        last_row_died > max_pass_row){
      # reconsideration happens after the last pass and there aren't more passes than reconsiders OR it dies after passing
      bill_stages$passed_chamber <- 0 # override because it didn't really pass
    }
  }
  
  # bill_stages$passed_chamber = ifelse(bill_stages$law==1, 1, bill_stages$passed_chamber)
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

# The state assembly's website lets us get lists of bills that pass each chamber and do or don't become law, letting
# us check for false negatives and false positives (https://nyassembly.gov/leg/?sh=advanced)
passed_assembly <- read.csv(glue("~/Downloads/NY_{t}_Passed_Assembly.csv"), header = FALSE) %>% mutate(bill_id = str_sub(V1, 1, 6))
passed_assembly %>% inner_join(all_bill_stages) %>% filter(passed_chamber == 0)
all_bill_stages %>% filter(passed_chamber == 1 & str_sub(bill_id, 1, 1) == "A") %>% anti_join(passed_assembly)

passed_senate <- read.csv(glue("~/Downloads/NY_{t}_Passed_Senate.csv"), header = FALSE) %>% mutate(bill_id = str_sub(V1, 1, 6))
passed_senate %>% inner_join(all_bill_stages) %>% filter(passed_chamber == 0)
all_bill_stages %>% filter(passed_chamber == 1 & str_sub(bill_id, 1, 1) == "S") %>% anti_join(passed_senate)

read.csv(glue("../../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{as.integer(t)-2}_{as.integer(t)-1}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% print()

read.csv(glue("../../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{as.integer(t)-4}_{as.integer(t)-3}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% print()

### Bill status check: https://nyassembly.gov/leg/?sh=advanced 
### (note that NY counts a bill as passing the chamber/becoming law if its counterpart in the other chamber did)

### MERGE to BILL DATA
bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 

### MERGE In COMMEMS
all_bill_stages <- commem_bills %>%
  mutate(session = t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))

### MERGE In S&S
all_bill_stages <- SS_bills %>% 
  select(bill_id, term, SS) %>%
  left_join(all_bill_stages, ., by = c('bill_id', 'term')) %>%
  mutate(SS = ifelse(is.na(SS), 0, SS))

### Adjust Commems if SS == 1
table(all_bill_stages$SS, all_bill_stages$commem)
all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
rm(SS_bills)

### Save Stage Info **** MERGE WITH SS **********
write.csv(all_bill_stages, glue('../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv'), row.names = FALSE )

rm(bill_hist_path, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist, aic_t, abc_t, pc_t, law_t)
rm(bill_hist, b_id, b_spon)

####################################################
############### Identify Unique Legislators via SLER
####################################################

## Import and Clean Sponsors Name to Match
all_sponsors <- bills %>%
  mutate(chamber = ifelse(substring(bill_id, 1, 1) == "A", "House", "Senate")) %>%
  select(LES_sponsor, chamber, passed_chamber, law) %>%
  mutate(term = t_yrs) %>%
  group_by(LES_sponsor, chamber, term) %>%
  summarize(num_sponsored_bills = n(), 
            sponsor_pass_rate = sum(passed_chamber) / n(),
            sponsor_law_rate = sum(law) / n()) %>%
  ungroup()

######## Cosponsorship Info --- 
all_sponsors$num_cosponsored_bills <- NA
bills$cospon_match <- paste(bills$LES_sponsor, tolower(bills$cosponsors), tolower(bills$multi_sponsors), sep = '; ')
for(i in 1:nrow(all_sponsors)){
  c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'House', 'A', 'S')  )
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
  ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
}
bills <- select(bills, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))


if(min(all_sponsors$num_cosponsored_bills) < 0){
  print("negative cosponsors"); break
} else{print("cosponsors valid")}

##################
### CLEAN NAMES
####################
all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(LES_sponsor,substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name))


############################################################
############## Match Sponsors Names to Legiscan Data
############################################################

legiscan = read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{gsub('_','-',t_yrs)}_General_Assembly/csv/people.csv"))

if(t_yrs == "2021_2022"){
  legiscan = legiscan %>% filter(people_id != 1348)
}

### Create Match Variables

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
  mutate(match_name_chamber = tolower(paste(match_name,substr(district,1,1),sep="-")))

# inexact::inexact_addin()
# left: legiscan_adj
# right: all_sponsors
# method: osa
# mode: full

# You added custom matches:
if(t_yrs == "2019_2020"){
all_sponsors2 = inexact::inexact_join(
  x  = legiscan_adj,
  y  = all_sponsors,
  by = "match_name_chamber",
  method = "osa",
  mode = "full",
  custom_match = c(
    "mannion-s" = NA_character_,
    "clark-h" = NA_character_,
    "meeks-h" = NA_character_,
    "burgos-h" = NA_character_,
    "brown-h" = NA_character_
  )
)
}

if(t_yrs == "2021_2022"){
  # You added custom matches:
  all_sponsors2 = inexact::inexact_join(
    x  = legiscan_adj,
    y  = all_sponsors,
    by = "match_name_chamber",
    method = "osa",
    mode = "full",
    custom_match = c(
      "chandler-waterman-h" = NA_character_,
      "blumencranz-h" = NA_character_,
      "rivera j-h" = NA_character_
    )
  )
  
}

if(t_yrs == "2023_2024"){
  all_sponsors2 = 
    # You didn't add any custom matches! Let's trust the algorithm:
    inexact::inexact_join(
      x    = legiscan_adj,
      y    = all_sponsors,
      by   = "match_name_chamber",
      method = "osa",
      mode = "full"
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

##############################
###### Estimate Scores + Add in Related Variables
#############################

### Check if bills in data without an ID'd sponsor
View(filter(bills, !(bills$LES_sponsor %in% legis_data$data_name)))

bills <- select(bills, -sponsor) %>% 
  rename(sponsor = LES_sponsor) %>% 
  mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'A', "H", "S"))

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

cat(glue(" ***************** \n \n \n TERM {t_yrs} ~~> DONE \n \n \n *************** ")); cat('\n')

################# ****** END LOOP


rm(all_sponsors, legis_data, SS_bills, elec_year, keep_types)
rm(commem_bills, t_yrs, t, c_sub, k_matches, klarner_sub, km, m_sub, bills, bill_path, calc_LES)

########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
#### Vacancies filled by SPECIAL ELECTION --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
##### Senate Wayback: https://web.archive.org/web/20000815061038/http://www.senate.state.ny.us/
##### Assembly Wayback: https://web.archive.org/web/20070628150818/http://assembly.state.ny.us/
###############################################################################


# filter(klarner, grepl("key, j", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(year, cand) %>% distinct() %>% as.data.frame() 
# filter(klarner, ddez == 17 & sen == 1 & outcome == 'w') %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 369 bill(s) sponsored by COMMITTEE
# ~~> Dropping 1 bills without a sponsor
#   session chamber     N  AIC  ABC PASS LAW
# 1 1999_2000       A 10691 5885 4381 1500 336
# 2 1999_2000       S  8045  718 3641 1897 830
### WON SPECIAL ~ HOUSE:
# -- FINCH (gary)
# -- KOLB (brian)
### WON SPECIAL ~ SENATE:
# -- COPPOLA (alfred, lost subsequent) -- https://web.archive.org/web/20100913034712/http://artvoice.com/issues/v9n36/five_questions#SlideFrame_0
# -- MORAHAN (thomas)
# -- SMITH (malcom) --> Name duplicated, won't print
# -- STAVISKY (toby ann) --> Name dup, won't print


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 391 bill(s) sponsored by COMMITTEE
# ~~> Dropping 1 bills without a sponsor
#   session chamber     N  AIC  ABC PASS LAW
# 1 2001_2002       A 11107 5558 4221 1500 381
# 2 2001_2002       S  7503  633 3322 1596 781
#### WON SPECIAL ~ HOUSE:
# -- MCDONALD (roy) -- https://www.nysenate.gov/senators/roy-j-mcdonald
# -- MCDONOUGH (david)
# -- MIRONES (matthew)
# -- ROBINSON (annettee m.)
# -- SANFORD (willaim e., lost 2002 elec) -- https://www.syracuse.com/opinion/2011/05/bill_sanford_su_crew_coach_ono.html
# -- TITUS (michele)
### WON SPECIAL ~ SENATE:
# -- ANDREWS (carl)
# -- KRUEGER (liz)


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 366 bill(s) sponsored by COMMITTEE
# ~~> Dropping 0 bills without a sponsor
#   session chamber     N  AIC  ABC PASS LAW
# 1 2003_2004       A 11142 5694 4391 1581 467
# 2 2003_2004       S  7550  440 3535 1894 855
### WON SPECIAL ~ HOUSE:
# -- BENJAMIN (michael)
# -- FIELDS (ginny a.)
# -- SALADINO (joseph s.)
# -- GUNTHER (aileen m) -- last name duplicatd, won't print ***
### DROP:
# -- davis, gloria -- resigned shortly into term due to bribery scandal -- https://web.archive.org/web/20190515051821/https://nypost.com/2002/03/13/key-dem-probed-in-bribe-scandal/


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 317 bill(s) sponsored by COMMITTEE
# ~~> Dropping 0 bills without a sponsor
#   session chamber     N  AIC  ABC PASS LAW
# 1 2005_2006       A 11967 5411 4611 1851 543
# 2 2005_2006       S  8126  438 4246 2234 882
### WON SPECIAL ~ HOUSE:
# -- ALESSI (marc)
# -- BOYLE (phillip, past H)
# -- CAMARA (karim)
# -- COLE (michael)
# -- FRIEDMAN (sylvia)
# -- GIGLIO (joseph)
# -- HAWLEY (stephen)
# -- HEVESI (andrew)
# -- MAISEL (alan)
# -- MCKEVITT (thomas)
# -- ROSENTHAL (linda)
# -- WALKER (rob)
### WON SPECIAL ~ Senate:
# -- COPPOLA (mark)
### IN CHAMBER:
# -- ferrara, donna -- resigned March 2005 after appointed to state post: https://www.liherald.com/stories/Ferrara-steps-down-from-Assembly,10946https://web.archive.org/save/https://www.liherald.com/stories/Ferrara-steps-down-from-Assembly,10946


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 278 bill(s) sponsored by COMMITTEE
# session chamber     N  AIC  ABC PASS LAW
# 1 2007_2008       A 11697 5434 4414 1594 502
# 2 2007_2008       S  8488  717 4291 2337 754
#### WON SPECIAL ~ HOUSE:
# -- AMEDORE (george jr)
# -- KELLNER (micah z.)
# -- SCHIMEL (michelle)
# -- TITONE (matthew)
# -- TOBACCO (louis)
# -- ZEBROWSKI (kenneth PAUL) -- Name Dup, won't print -- father (kenneth peter) died march 2007, son succeeded -- SAME ID IN KLARNER
# -- JOHNSON (craig) -- Name dup, won't print
### IN CHAMBER (partial):
# -- dinapoli, thomas p. -- appointed state comptroller Feb 7, 2007
### DROP:
# -- balboni, michael a. l. -- appointed to state post Dec 26. 2006


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 583 bill(s) sponsored by COMMITTEE
# ~~> Dropping 2 bills without a sponsor
#   session chamber     N  AIC  ABC PASS LAW
# 1 2009_2010       A 11269 4585 3529 1577 687
# 2 2009_2010       S  8240 1403 3340  870 304
#### WON SPECIAL ~ HOUSE:
# -- CASTELLI (robert)
# -- CRESPO (marcos)
# -- GIBSON (vanessa)
# -- MONTENSANO (michael)
# -- MURRAY (dean)
# -- MILLER (michael) -- Name dup, wont print!! ****
# -- WEPRIN (david) -- Name dup, won't print!! ***


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 125 bill(s) sponsored by COMMITTEE
#   session chamber     N  AIC  ABC PASS LAW
# 1 2011_2012       A 10663 4338 3432 1229 558
# 2 2011_2012       S  7663 1223 3715 1529 511
### WON SPECIAL ~ HOUSE:
# -- BARRETT (didi)
# -- BRINDISI (anthony)
# -- ESPINAL (rafael)
# -- GOLDFEDER (phillip)
# -- KEARNS (michael p.)
# -- MAYER (shelley)
# -- QUART (dan)
# -- RYAN (sean)
# -- SIMANOWITZ (michael)
# -- SKARTADOS (frank, past H)
# -- WALTER (raymond)
#### WON SPECIAL ~ SENATE:
# -- STOROBIN (david, lost 2012 elec.)


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 115 bill(s) sponsored by COMMITTEE
#   session chamber     N  AIC  ABC PASS LAW
# 1 2013_2014       A 10121 3863 3435 1300 559
# 2 2013_2014       S  7757  818 3963 1720 526
#### WON SPECIAL ~ HOUSE:
# -- DAVILA (maritza)
# -- PALUMBO (anthony)
# -- PICHARDO (victor)
#### WON SPECIAL ~ SENATE:
# -- TKACZYK (cecilia)
#### IN CHAMBER:
# -- rivera, jose
# -- amedore, george a. jr.

# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 102 bill(s) sponsored by COMMITTEE
# ~~> Dropping 14 bills without a sponsor
#   session chamber     N  AIC  ABC PASS LAW
# 1 2015_2016       A 10377 3706 3549 1106 454
# 2 2015_2016       S  8047  747 4392 2135 617
#### WON SPECIAL ~ HOUSE:
# -- CANCEL (alice, lost 2016 elec)
# -- CASTORINA (ronald)
# -- HARRIS (pamela)
# -- HUNTER (pamela jo)
# -- HYNDMAN (alicia)
# -- RICHARDSON (diana)
# -- WILLIAMS (jamie r.)
#### WON SPECIAL ~ SENATE:
# -- AKSHAR (frederick)
#### IN CHAMBER (partial):
# -- camara, karim -- resigned Feb 20, 2015 for admin post: https://web.archive.org/web/20190411070136/https://www.nydailynews.com/blogs/dailypolitics/karim-camara-doubles-salary-joining-team-cumo-blog-entry-1.2174585


# ~~~~~~~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM!  ~~~~~~~~~~~~~~~~~~ 
# -----> Dropping 86 bill(s) sponsored by COMMITTEE
# ~~> Dropping 7 bills without a sponsor
#   session chamber     N  AIC  ABC PASS LAW
# 1 2017_2018       A 10931 3642 3477 1192 498
# 2 2017_2018       S  9032 1002 4457 2257 505
#### WON SPECIAL ~ HOUSE:
# -- ASHBY (jacob, R, D-107)
# -- BOHEN (erik, I-D, D-142)
# -- EPSTEIN (harvey, D, D-74)
# -- ESPINAL (ari, D, D-39)
# -- FERNANDEZ (nathalie, D, D-80)
# -- MIKULIN (john, R, D-17)
# -- PELLEGRINO (christine, D, D-9)
# -- SMITH (doug m, R, D-5)
# -- STERN (steven h., D, D-10, NOT the same as Repub one who lost 2006-2016)
# -- TAGUE (christopher, R, D-102)
# -- TAYLOR (al, D, D-71)
# -- ROSENTHAL (daniel, D, D-27) -- Name dup, won't print
#### WON SPECIAL ~ SENATE:
# -- BENJAMIN (brian, D, D-30)
### IN CHAMBER (partial): 
# -- saladino, joseph s. -- resigned Jan. 31, 2017
# -- rivera, jose -- still in office

# filter(klarner, grepl("rivera, j", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(year, cand) %>% distinct() %>% as.data.frame()
# filter(klarner, ddez == 17 & sen == 1 & outcome == 'w') %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)


##########################################################################################################
##########################################################################################################
##### MATCHING MISSING (SPECIALS) TO KLARNER USING OTHER TERMS WHERE SPONSOR IS PRESENT
##########################################################################################################

########################################################################################################################################################
########################################################################################################################################################
######## AGGREGATE AND MATCH TO EXTERNAL
########################################################################################################################################################
########################################################################################################################################################

### Klarner State Leg. Election Data --- Using Adjusted File From Top
klarner_sub <- filter(klarner, sab == this_state & year >= min_year - 5 & outcome == 'w')
klarner_sub <- select(klarner_sub, year, sen, ddez, dno, term, termz, cando, cand, candid, partyz, partyt, exper, outcome, etype)

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
      LES[LES$sponsor == name,]$party <- sponsor_rows[1,]$partyt
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
              sponsor_sub = filter(sponsor_sub, etype %in% c('g', 'gs') )
            }
          }
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$district <- sponsor_sub[1,]$dno
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$party <- sponsor_sub[1,]$partyt
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$exper <- sponsor_sub[1,]$exper
        }else{
          if(nrow(sponsor_rows) > 0){
            sponsor_rows <- arrange(sponsor_rows, year)
            if(nrow(filter(sponsor_rows, etype %in% c("g", "gs"))) > 0 ){
              sponsor_rows <- filter(sponsor_rows, etype %in% c("g", "gs"))
            }
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
# filter(LES, is.na(party)) %>% select(sponsor, term, klarner_id, district, party, exper) %>% as.data.frame()
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

### Not In Klarner -- ALL FROM 2017-2018
fill_missing <- data.frame(LES_name = "pellegrino, christine", new_name = 'pellegrino, christine', party = 'd', district = 9, exper = 'none')
# Stern != the stern that LOST in prior elections and was a Repub.
fill_missing <- add_row(fill_missing, LES_name = "stern", new_name = 'stern, steven h.', party = 'd', district = 10, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "epstein", new_name = 'epstein, harvey', party = 'd', district = 74, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "espinal, ari", new_name = 'espinal, ari', party = 'd', district = 39, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "rosenthal, daniel", new_name = 'rosenthal, daniel', party = 'd', district = 27, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "fernandez", new_name = 'fernandez, nathalie', party = 'd', district = 80, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "ashby", new_name = 'ashby, jacob', party = 'r', district = 107, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "taylor", new_name = 'taylor, al', party = 'd', district = 71, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "smith", new_name = 'smith, doug m.', party = 'r', district = 5, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "tague", new_name = 'tague, christopher', party = 'r', district = 102, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "mikulin", new_name = 'mikulin, john', party = 'r', district = 17, exper = 'none')
# Bohen = Democratic-Caucusing Independent
fill_missing <- add_row(fill_missing, LES_name = "bohen", new_name = 'bohen, erik', party = 'd', district = 142, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "benjamin, brian", new_name = 'benjamin, brian', party = 'd', district = 30, exper = 'none')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)
rm(fill_missing, i)


########## 
### Fix Nonmaj Party Codes
#########
# filter(LES, party == "nonmaj") %>% select(1:6, party, district)
# filter(klarner, cand == "hoyt, william b. iii") %>% select(cand, year, sen, etype, outcome, partyz, partyt)

## Coppola was Dem when won special in Feb 2000, but lost D primary in Nov 2000 -- Ran anyway as indep/conservative and lost
# -- Challenged inc in both 2002/2004 in D primary, lost, but ran in general as R both times
LES[LES$sponsor == "coppola, alfred t." & LES$term == "1999_2000",]$party <- "d"
### Hoyt has been a Dem whole career
LES[LES$sponsor == "hoyt, william b. iii" & LES$term == "2001_2002",]$party <- "d"
### Won 2006 spcial as Dem, lost Dem primary in Sep, ran in general as Working Families cand
LES[LES$sponsor == "friedman, sylvia" & LES$term == "2005_2006",]$party <- "d"
### Coppola: Won 2006 special as Dem, lost Sep. primary, ran on conservative ticket
LES[LES$sponsor == "coppola, mark a." & LES$term == "2005_2006",]$party <- "d"
#### Won 2016 special as Dem, lost D primary for general, ran in general as Womens Equality party cand
LES[LES$sponsor == "cancel, alice" & LES$term == "2015_2016",]$party <- "d"


#########################################################
############ Match to Hall/Fouirnaies
########################################################

library(readstata13)

### Hall and Fournaies 2018, Covers 1988 - 2014
### --> https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/PGCVDP
hf_data <- read.dta13("../../../Hall_Fouirnaies_2018/How_Do_interest_Groups_Seek_Access_to_Committees.dta")
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

# *** NEW YORK SM Data starts in 1994 ***

ideo <- readstata13::read.dta13("../../../Shor_McCarty_Data/shor_mccarty_1993_2016_individual_data_May_2018.dta")
ideo <- filter(ideo, st == this_state) %>% select(-st_id)
ideo$match_name <- str_extract(tolower(ideo$name), '[a-z]+, [a-z]+')
ideo$match_name <- ifelse(is.na(ideo$match_name), tolower(ideo$name), ideo$match_name)
ideo$last_name <- gsub(',.+', '', tolower(ideo$name))

### Name Correction
ideo[ideo$name == "Diaz Jr, Ruben",]$name <- "Diaz Sr, Ruben"

## **** A HANDFUL OF 2015-2016 DUPLICATES ---> Eliminating for now...
#ideo <- filter(ideo, !duplicated(paste(name, party, sep = '-')))

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

LES[LES$sponsor %in% c('coppola, alfred t.', 'coppola, mark a.', 'diaz, ruben sr.', 'miller, melissa l.', 'delarosa, carmen n.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

LES[LES$sponsor %in% c('jones, d. billy'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES -- MOst of remaining missing = 2015-2016 special
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('cast', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'boyland, william f.', SM_name = 'Boyland, William')
name_matches <- add_row(name_matches, LES_name = 'boyland, william f. jr.', SM_name = 'Boyland, William Jr.')
# name_matches <- add_row(name_matches, LES_name = 'akshar, frederick j., ii', SM_name = 'zzzzzzz')
## *** SM BORELLI == Mispelled
name_matches <- add_row(name_matches, LES_name = 'borelli, joseph', SM_name = 'Borrelli, Joe')
# name_matches <- add_row(name_matches, LES_name = 'cancel, alice', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'castorina, ronald, jr.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'comrie, leroy g. jr.', SM_name = 'Comrie Jr, Leroy')
## *** Coppolas are collapsed into 1 observation (Senate 2000, 2006) -- Attributing to both as balanced and quite liberal (so should be directionally correct)
name_matches <- add_row(name_matches, LES_name = 'coppola, alfred t.', SM_name = 'Coppola')
name_matches <- add_row(name_matches, LES_name = 'coppola, mark a.', SM_name = 'Coppola')
## *** SM name fixed above: it's SR not JR
name_matches <- add_row(name_matches, LES_name = 'diaz, ruben sr.', SM_name = 'Diaz Sr, Ruben')
## *** Unclear if he is actually JR but timing matches up
name_matches <- add_row(name_matches, LES_name = 'flanagan, john', SM_name = 'Flanagan Jr, John J')
name_matches <- add_row(name_matches, LES_name = 'hooper, earlene hill', SM_name = 'Hill, Earlene H.')
name_matches <- add_row(name_matches, LES_name = 'rivera, j. gustavo', SM_name = 'Rivera, J.')
name_matches <- add_row(name_matches, LES_name = 'rivera, jose', SM_name = 'Rivera, José')
name_matches <- add_row(name_matches, LES_name = 'sepulveda, luis r.', SM_name = 'Sepúlveda, Luis Sepulveda')
# name_matches <- add_row(name_matches, LES_name = 'williams, jaime r.', SM_name = 'zzzzzzz')
## *** Zebrowskis are collapsed as JR and SR --> Matching ONLY to JR as he represents majority of data
name_matches <- add_row(name_matches, LES_name = 'zebrowski, kenneth paul', SM_name = 'Zebrowski Jr, Kenneth P')
# name_matches <- add_row(name_matches, LES_name = 'zebrowski, kenneth peter', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}


#######################
### PARTY SWITCHERS
#############################

#### Check for Potential Switchers
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()

## **** Nancy Lorraine Hoffman --- Switched to Republican in ~1998: https://www.nytimes.com/2004/11/10/nyregion/in-syracuse-a-shaky-hold-on-a-senate-seat.html
## -- Note: if go back further in time with data, will need to mach earlier observations to Dem. Score in SM data
LES[LES$sponsor == 'hoffmann, nancy larraine' & LES$term == "1999_2000",]$party <- 'r'
LES[LES$sponsor == 'hoffmann, nancy larraine' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Hoffmann, Nancy Larraine' & ideo$party == 'R',]$name
LES[LES$sponsor == 'hoffmann, nancy larraine' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Hoffmann, Nancy Larraine' & ideo$party == 'R',]$party
LES[LES$sponsor == 'hoffmann, nancy larraine' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Hoffmann, Nancy Larraine' & ideo$party == 'R',]$np_score
# LES[LES$sponsor == 'hoffmann, nancy larraine' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Hoffmann, Nancy Larraine' & ideo$party == 'D',]$name
# LES[LES$sponsor == 'hoffmann, nancy larraine' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Hoffmann, Nancy Larraine' & ideo$party == 'D',]$party
# LES[LES$sponsor == 'hoffmann, nancy larraine' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Hoffmann, Nancy Larraine' & ideo$party == 'D',]$np_score

## **** Fred Thiele Jr --- Republican 1989 - 2009, switched to Indep Oct 1 2009 and subsequently caucused with Dems: https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=6129
# LES[LES$sponsor == 'thiele, fred w. jr.', c("term", "party")]
LES[LES$sponsor == 'thiele, fred w. jr.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Thiele, Fred Jr.' & ideo$party == 'R',]$name
LES[LES$sponsor == 'thiele, fred w. jr.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Thiele, Fred Jr.' & ideo$party == 'R',]$party
LES[LES$sponsor == 'thiele, fred w. jr.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Thiele, Fred Jr.' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'thiele, fred w. jr.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Thiele, Fred Jr.' & ideo$party == 'D',]$name
LES[LES$sponsor == 'thiele, fred w. jr.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Thiele, Fred Jr.' & ideo$party == 'D',]$party
LES[LES$sponsor == 'thiele, fred w. jr.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Thiele, Fred Jr.' & ideo$party == 'D',]$np_score

## **** Michael Spano -- Swtiched R to D in July 2007: https://www.nytimes.com/2007/07/12/nyregion/12mbrfs-SWITCH.html
# LES[LES$sponsor == 'spano, michael j.', c("term", "party")]
LES[LES$sponsor == 'spano, michael j.' & LES$term == "2007_2008",]$party <- 'd'
LES[LES$sponsor == 'spano, michael j.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Spano, Michael J' & ideo$party == 'R',]$name
LES[LES$sponsor == 'spano, michael j.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Spano, Michael J' & ideo$party == 'R',]$party
LES[LES$sponsor == 'spano, michael j.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Spano, Michael J' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'spano, michael j.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Spano, Mike' & ideo$party == 'D',]$name
LES[LES$sponsor == 'spano, michael j.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Spano, Mike' & ideo$party == 'D',]$party
LES[LES$sponsor == 'spano, michael j.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Spano, Mike' & ideo$party == 'D',]$np_score

## **** Ronald Tocci -- Lost D Primary in 2002, ran in general as R and won
LES[LES$sponsor == 'tocci, ronald c.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Tocci, Ronald' & ideo$party == 'R',]$name
LES[LES$sponsor == 'tocci, ronald c.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Tocci, Ronald' & ideo$party == 'R',]$party
LES[LES$sponsor == 'tocci, ronald c.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Tocci, Ronald' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'tocci, ronald c.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Tocci, Ronald C.' & ideo$party == 'D',]$name
LES[LES$sponsor == 'tocci, ronald c.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Tocci, Ronald C.' & ideo$party == 'D',]$party
LES[LES$sponsor == 'tocci, ronald c.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Tocci, Ronald C.' & ideo$party == 'D',]$np_score

### Olga Mendez -- Left Democratic party in December 2002 --> Served as R in 2003-2004
LES[LES$sponsor == 'mendez, olga a.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Mendez, Olga' & ideo$party == 'R',]$name
LES[LES$sponsor == 'mendez, olga a.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Mendez, Olga' & ideo$party == 'R',]$party
LES[LES$sponsor == 'mendez, olga a.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Mendez, Olga' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'mendez, olga a.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Mendez, Olga A' & ideo$party == 'D',]$name
LES[LES$sponsor == 'mendez, olga a.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Mendez, Olga A' & ideo$party == 'D',]$party
LES[LES$sponsor == 'mendez, olga a.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Mendez, Olga A' & ideo$party == 'D',]$np_score

### Joseph Robach -- Left Democratic party in 2002, uncler when, ran for Senate as R
LES[LES$sponsor == 'robach, joseph e.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Robach, Joseph E.' & ideo$party == 'R',]$name
LES[LES$sponsor == 'robach, joseph e.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Robach, Joseph E.' & ideo$party == 'R',]$party
LES[LES$sponsor == 'robach, joseph e.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Robach, Joseph E.' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'robach, joseph e.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Robach, Joseph E.' & ideo$party == 'D',]$name
LES[LES$sponsor == 'robach, joseph e.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Robach, Joseph E.' & ideo$party == 'D',]$party
LES[LES$sponsor == 'robach, joseph e.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Robach, Joseph E.' & ideo$party == 'D',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)


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
LES[as.numeric(substring(LES$term,1,4)) %in% c(2009:2010, 2019:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1999:2008, 2011:2018) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1

#### NOTES + FIXES FOR Cross-Party Caucusing in 2009-2010, 2013-2014, 2017-2018
# 2009-2010: Dems won control, but handful of dems refused to support party leadership; both parties had control for a while
# ---> Ultimately, after a bit of stalemate, Pedro Espada (D) became majority leader --> D Control
# 2013-2014: "Independent Democratic Conference" (which formed in prior term) formed coalition with Republicans for Majority
# 2015-2016: Republicans won outright control -- IDC still worked with them, but not central piece of the coalition..?
# 2017-2018 - The IDC rejoined Dems BUT Simcha Felder (D) caucused with the Republicans to give them majority - https://www.vox.com/2018/4/23/17259112/new-york-special-election-shelley-mayer-julie-killian-simcha-felder
# ------> This happened over the course of the first year, up to April 2018 when the IDC dissolved and rejoined Dems...
# ------> Oddly it was Felder who urged them to rejoin... Weird: https://www.nytimes.com/2017/05/24/nyregion/simcha-felder-independent-democratic-conference-senate.html
# * Members: Jeff Klein (leader); Marisol Alcantara; Tony Avella; Jesse Hamilton; Jose Peralta; David Valesky
# * ------- David Carlucci; Diane Savino
# ---> See: (1) https://www.vox.com/policy-and-politics/2018/9/14/17859200/idc-new-york-primaries-democrats-biaggi-klein
# ---> See: (2) https://en.wikipedia.org/wiki/Independent_Democratic_Conference

## 2013-2014
# Source: Announcment pre 2013-2014 session: https://www.nysenate.gov/newsroom/press-releases/independent-democratic-conference-senate-republicans-announce-creation
# ---> Klein was formally part of the leadership -- lost that position in 2015 when R's took more seats
idc_2013 <- c("klein, jeffrey", "savino, diane j.", "valesky, david j.", "carlucci, david s.", "smith, malcolm a.")
LES[LES$term == '2013_2014' & LES$chamber == "Senate" & LES$sponsor %in% idc_2013,]$in_majority <- 1

## 2017-2018 -- Simcha Felder caucuased with R's --> Majority
LES[LES$term == '2017_2018' & LES$chamber == "Senate" & LES$sponsor == "felder, simcha",]$in_majority <- 1


############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

##### Drop Nicknames
# filter(LES, grepl('\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
LES$sponsor <- str_trim(gsub('  +', ' ', gsub('\\(.+\\)', '', LES$sponsor)))

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)
LES[LES$sponsor == "dandrea, robert 1",]$sponsor <- "d'andrea, robert"


### Manual Fixes
LES[LES$sponsor == "espinal, ari",]$sponsor <- 'espinal, aridia'
LES[LES$sponsor == "hyerspencer, donna j.",]$sponsor <- "hyer-spencer, donna janele"
LES[LES$sponsor == "brookkrasny, alec",]$sponsor <- "brook-krasny, alec"
LES[LES$sponsor == "delarosa, carmen n.",]$sponsor <- "de la rosa, carmen n."
LES[LES$sponsor == "hassellthompson, ruth h.",]$sponsor <- "hassell-thompson, ruth"
LES[LES$sponsor == "jeanpierre, kimberly",]$sponsor <- "jean-pierre, kimberly"
LES[LES$sponsor == "peoplesstokes, crystal d.",]$sponsor <- "peoples-stokes, crystal d."
LES[LES$sponsor == "phefferamato, stacey g.",]$sponsor <- "pheffer-amato, stacey g."
LES[LES$sponsor == "rhoddcummings, pauline",]$sponsor <- "rhodd-cummings, pauline"
LES[LES$sponsor == "stewartcousins, andrea",]$sponsor <- "stewart-cousins, andrea"


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

##### CHECK OUTLIERS ---- 
# filter(LES, party == 'd' & np_score > 0) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & np_score < 0) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')



