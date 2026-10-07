

#####################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** ILLINOIS *** BY SESSION
#####################################

###################################
## SPECIAL SESSIONS:
## -- Bills carryover from regular to regular; MANY special sessions... 
## -- Special bills are OFTEN BUT NOT ALWAYS duplicates from the regular session, with identical bill histories (e.g., SB1115 in 95th regular is identical in the special)
## -- See, e.g., ids = filter(bills, grepl("Special", session) & !grepl(".R", bill_id)) %>% pull(bill_id); filter(bills, bill_id %in% ids) %>% View()
## -- Or: https://www.ilga.gov/legislation/BillStatus.asp?DocNum=2055&GAID=9&DocTypeID=HB&LegId=30902&SessionID=51&GA=95#actions
## -- Vs: https://www.ilga.gov/legislation/BillStatus.asp?DocNum=2055&GAID=9&DocTypeID=HB&LegId=30902&SessionID=52&SpecSess=1&GA=95#actions
## -- BUT THIS IS NOT ALWAYS THE CASE (see SB0001 in 97th)
## ---------> ONLY KEEPING THOSE WITHOUT IDENTICAL HISTORIES
## -- https://medium.com/state-matters/how-does-session-work-in-illinois-456d59c487e9
## MEMBER LISTS:
## Illinois Blue Book: https://www.cyberdriveillinois.com/publications/illinois_bluebook/legroster.pdf
## All Blue Books: http://www.idaillinois.org/ui/custom/default/collection/default/resources/custompages/bin/edi.php?collection=bb&startrec=51
## PROCESS
## -- Account for different types of vetos... "full veto, a line-item veto, a reduction veto, or an amendatory veto." (via medium post above)
###########################

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
library(readr)
library(inexact)
library(tibble)
library(foreach)

this_state <- 'IL'
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

sessions <- sort(as.numeric(gsub('.+Details_|.csv', '', bill_files)))
rm(data_files, bill_files)

session <- 102
t = as.character(2*(session-101)+2019)
t_plus_one = as.character(as.integer(t) + 1)

t_yrs <- as.numeric(t)
t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 1}'))

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{session}.csv"))
commem_bills <- commem_bills %>%
  mutate(s_num = gsub('.+Assembly.-.|[a-z]+.Special Session', '', session, perl = TRUE),
         session = ifelse(!grepl("Special", session), "RS", paste0("SS", s_num))) %>%
  select(-s_num)

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



### Formulate 2-year terms -- Cover both regular and special sessions
t_sessions <- session

### Skip Previously Estimated
if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
  print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
  next
}

###### SESSION IN PROGRESS
print(glue('\n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} session! ~~~~~~~~~~~~~~ '))

 ############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_sessions[1]}.csv")
bills <- read.csv(bill_path)   

### Drop Duplicates --- Arise because first page of Resolutions also includes other types below, so end up clicking that link x 3
bills <- distinct(bills)

######## Standardize the Bill IDs + Sessions
bills <- rename(bills, bill_id = bill_number) %>%
  mutate(term = t_yrs,
         s_num = gsub('.+Assembly.-.|[a-z]+.Special Session', '', session, perl = TRUE),
         session = ifelse(!grepl("Special", session), "RS", paste0("SS", s_num))) %>%
  select(-s_num)

############### Standardize Sponsors
bills$LES_sponsor <- ifelse(grepl('^H', bills$bill_id), bills$house_sponsors, ifelse(grepl('^S', bills$bill_id), bills$senate_sponsors, NA) )
bills$LES_sponsor <- tolower(gsub(";.+", "", bills$LES_sponsor))
bills$LES_sponsor <- gsub("^mr\\. ", "", bills$LES_sponsor)
##### *********** NOTE: Before 2003 just last name; FROM 2003: First M. Last

############### Drop Resolutions, Messages, Communications, Reports
all_bills <- bills
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)

######### Bills By Request
if(any(grepl('by request', bills$LES_sponsor))){
  print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
  #print(glue("-----> KEEPING {nrow(filter(bills, grepl('by request', LES_sponsor)))} bill(s) introduced BY REQUEST"))
  #bills$LES_sponsor <- str_trim(gsub('\\(by request\\)', '', bills$LES_sponsor))
}

sort(table(bills$LES_sponsor))

#### Standardizing Accents in Names --- If left as is, will not match correctly to Klarner Data
bills$LES_sponsor <- gsub('á', 'a', bills$LES_sponsor)
bills$LES_sponsor <- gsub('é', 'e', bills$LES_sponsor)
bills$LES_sponsor <- gsub('ó', 'o', bills$LES_sponsor)
bills$LES_sponsor <- gsub('í', 'i', bills$LES_sponsor)
bills$LES_sponsor <- gsub('ñ', 'n', bills$LES_sponsor)  



###################
###### Merge in S&S Bills
###################
## *** For ILLINOIS: Special Bills are NEARLY ALL CARRIED OVER versions of Regular Sessions bills
## ---> Coding all as if regular session... small potential for error here, but should be fine...


if(t_yrs == "2019_2020"){
  SS_bills$bill_id[SS_bills$bill_id=="SB90009"] = "SB0009"
  SS_bills$bill_id[SS_bills$bill_id=="HB20002"] = "HB0002"
  SS_bills$bill_id[SS_bills$bill_id=="SB10001"] = "SB0001"
} else if(t_yrs == "2021_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="SB4004A"] = "SB0004A"
  SS_bills$bill_id[SS_bills$bill_id=="SB2002C"] = "SB0002C"
  SS_bills$bill_id[SS_bills$bill_id=="SB4004C"] = "SB0004C"
  SS_bills$bill_id[SS_bills$bill_id=="SB6006C"] = "SB0006C"
  SS_bills$bill_id[SS_bills$bill_id=="HB10001"] = "HB0001"
  SS_bills$bill_id[SS_bills$bill_id=="HB30003"] = "HB0003"
  SS_bills$bill_id[SS_bills$bill_id=="HB50005"] = "HB0005"
  SS_bills$bill_id[SS_bills$bill_id=="HB70007"] = "HB0007"
}

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,LES_sponsor,bill_url), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% 
  arrange(LES_sponsor)

missing_SS_bills$bill_id


# now check to see if there are duplicate joins


duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills, 
            by = c("bill_id", "term")) %>%
  group_by(rowname) %>% mutate(count = n()) %>%
  ungroup() %>% 
  select(count,bill_id,term,session, Title, summary) %>% 
  arrange(desc(count),bill_id)

if(nrow(duplicate_SS_bills %>% filter(count > 1)) > 0 ){
  print("you have duplicates"); SS_duplicates_exist <- 1; # break
} else{
  print("no duplicates")
  SS_duplicates_exist <- 0
}

# in IL, they reintroduce the bills with the same number in a special session. so the duplicates are okay, you can enter the next part of the loop (as long as there aren't, e.g., triple duplicates)

if(SS_duplicates_exist == 1){
  if(length(duplicate_SS_bills %>% 
            filter(count > 1) %>%
            pull(summary) %>% unique()) / length(duplicate_SS_bills %>% 
                                                 filter(count > 1) %>%
                                                 pull(summary) ) == 0.5) {SS_duplicates_exist <- 0}
}

if(SS_duplicates_exist == 1 ){
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
  bills2 <- bills %>% 
    left_join(SS_term %>% select(-Title) %>% distinct(), by = c("bill_id", "term")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  SS_term2 = SS_term %>% 
    left_join(bills %>% select(bill_id,term,session),by=c("bill_id","term"))
  if(!identical(c(nrow(bills2),nrow(SS_term2 %>% filter(!grepl("SS",session)))),orig_row_n ))
    {print("merge failed"); break} else{
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

############################################################
############### Code Commemorative
############################################################

bills <- commem_bills %>%
  select(bill_id, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'session'))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)


###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "") %>% View()
if(nrow(filter(bills, LES_sponsor == '')) > 0){
  print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
  bills <- filter(bills, !(LES_sponsor == ''))     
}


##################################################
############### Code Bill History
##################################################

bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
bill_hist <- read.csv(bill_hist_path)

######## Standardize BillHist Bill IDs
bill_hist <- rename(bill_hist, bill_id = bill_number)
bill_hist <- rename(bill_hist, chamber = action_chamber)

### Standardize Chamber Variable
bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate")

##### Set Term/Clean Session Var if Needed
bill_hist <- bill_hist %>%
  mutate(term = t_yrs,
         s_num = gsub('.+Assembly.-.|[a-z]+.Special Session', '', session, perl = TRUE),
         session = ifelse(!grepl("Special", session), "RS", paste0("SS", s_num))) %>%
  select(-s_num)
# table(bill_hist$session)

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
# --- In early years, amendments differentiated committee/floor with (~) H or (~) S at end (tilde not always there) --- with these, means committee
# --- Coding do pass as AIC --- It's a direct recommendation...
aic_t <- c('^do pass', 'senate comm[a-z]+ amend', 'house comm[a-z]+ amend', '^amendment no.+ h$', '^amendment no.+ s$', 
           'comm[a-z]+ deadline extend', 'motion filed.+comm', 
           'do pass.+short debate', 'do pass.+standard debate', 'motion do pass',
           'rec.+subcomm', 'to.+subcomm')
## Note: REMOVED 'note requested', 'note filed' from ABC (not clear when/who requests)
abc_t <- c('^do pass', '2nd read', '3rd read', 'second read', 'third read', 'house floor amend',
           'senate floor amend', '^amendment no.+[a-z][a-z]$')
### PLaced on Calendar sometimes used before assigned to committee..
pc_t <- c('passed$', 'passed [0-9]', 'passed ~', '/passed', '/pass ~', '^passed both', '^third reading - passed') 
### Need to avoid passed - [legislator name] rows --- those are motions to reconsider split across two rows
law_t <- c('^public act.+[0-9]', 'governor approved') 
# This will catch veto action --- if it becomes a public act, it overrode or was accepted, if not, didn't

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
# ---> Skipping SPECIAL SESSION BILLS IF IDENTICAL HISTORY AS REGULAR SESSION version of bill
options(warn = 2)
bills$drop <- 0
for(i in 1:nrow(bills)){
  b_id = bills[i,]$bill_id
  s_id = bills[i,]$session
  b_spon = bills[i,]$LES_sponsor
  hist_sub <- filter(bill_hist, bill_id == b_id, session == s_id)
  if(s_id != "RS"){
    reg_hist <- filter(bill_hist, bill_id == b_id, session == "RS") %>% select(-session)
    if(isTRUE(all.equal(select(hist_sub, -session), reg_hist))){
      bills[i,]$drop <- 1
      next
    }
  }
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t,
                                    ignore_chamber_switch = TRUE) #### Sometimes Non-INtro chamber has recorded actions before intro chamber done (often adding sponsors)
  bill_stages$bill_url <- bills[i,]$bill_url
  out_chamber <- ifelse(substring(b_id, 1, 1) == "H", "Senate", "House")
  if(bill_stages$passed_chamber == 0 & any(grepl(glue("^(Arrived|Arrive) in {out_chamber}"), hist_sub$action))){
    bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
  }
  all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
  # print(i)
}
options(warn = 1)

### Drop Specials that are Carried Over Regular Session Bills
bills <- filter(bills, drop == 0) %>% select(-drop)

### Check Codings
cat('\n')
all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law) ) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2)) %>% 
  as.data.frame() %>% print()
# filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% filter(!grepl('^by resolution|^first read|^prefiled', tolower(action))) %>% View()


read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{as.integer(t)-2}_{as.integer(t)-1}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% print()

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{as.integer(t)-4}_{as.integer(t)-3}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% print()

### MERGE to BILL DATA
bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 

### MERGE In COMMEMS
all_bill_stages <- commem_bills %>%
  select(bill_id, session, commem) %>%
  left_join(all_bill_stages, ., by = c('bill_id', 'session'))

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
  mutate(session = t_yrs) %>%
  group_by(LES_sponsor, chamber) %>%
  summarize(num_sponsored_bills = n(), 
            sponsor_pass_rate = sum(passed_chamber) / n(),
            sponsor_law_rate = sum(law) / n()) %>%
  ungroup()


######## Get Number of Cosponsored Bills
all_sponsors$num_cosponsored_bills <- NA
bills$cospon_match <- ifelse(substring(bills$bill_id, 1, 1) == "H", paste0(bills$LES_sponsor, '; ', bills$house_sponsors), paste0(bills$LES_sponsor, '; ', bills$senate_sponsors))
for(i in 1:nrow(all_sponsors)){
  c_sub <- filter(bills, substring(bill_id, 1, 1) == all_sponsors[i,]$chamber)
  search_name <- str_replace_all(all_sponsors[i,]$LES_sponsor, "(\\W)", "\\\\\\1")
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(search_name, tolower(c_sub$cospon_match)))
  all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
}
rm(search_name)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))

if(min(all_sponsors$num_cosponsored_bills) < 0){
  print("negative cosponsors"); break
} else{print("cosponsors valid")}

############
### CLEAN NAMES
#############


if(t < 2003){
  all_sponsors$last_name <- ifelse(!grepl(',', all_sponsors$LES_sponsor), all_sponsors$LES_sponsor, gsub(',.+', '', all_sponsors$LES_sponsor))
  all_sponsors$first_name <- ifelse(grepl(',', all_sponsors$LES_sponsor), gsub('.+,', '', all_sponsors$LES_sponsor), '')
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
} else{
  parsed <- map_df(all_sponsors$LES_sponsor, parse_names)
  all_sponsors$last_name <- gsub(',$', '', parsed$last_name)
  ### E.g. if J. Bradely using Bradley as first name --- if this creates error, switch to using initial
  all_sponsors$first_name <- ifelse(grepl('\\.$', parsed$first_name), parsed$middle_name, parsed$first_name)
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)    
}

print(glue("-----> {nrow(all_sponsors)} UNIQUE SPONSORS IDENTIFIED IN BILL DATA "))




all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(LES_sponsor,substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name))

legiscan = read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{gsub('_','-',t_yrs)}_{scales::ordinal(c(session))}_General_Assembly/csv/people.csv"))

if(t_yrs == "2019_2020"){
  legiscan = bind_rows(legiscan,
                       legiscan %>% filter(people_id == 15382) %>%
                         mutate(role = "Rep", district = "HD019"),
                       legiscan %>% filter(people_id == 1040) %>%
                         mutate(role = "Rep", district = "HD012"),
                       legiscan %>% filter(people_id == 19687) %>%
                         mutate(role = "Rep", district = "HD021"))
}



### Create Match Variables

legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  mutate(match_name_chamber = tolower(paste(name,substr(district,1,1),sep="-")))


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
        "meg loughran cappel-s" = NA_character_,
        "cristina pacione-zayas-s" = NA_character_,
        "suzanne glowiak-s" = "suzy glowiak hilton-s",
        "adriane johnson-s" = NA_character_,
        "margaret croke-h" = NA_character_,
        "gary daugherty-h" = NA_character_,
        "marcus evans-h" = "marcus c. evans, jr.-h",
        "jaime andrade-h" = "jaime m. andrade, jr.-h",
        "jackie haas-h" = NA_character_,
        "win stoller-s" = NA_character_,
        "paul evans-h" = NA_character_,
        "curtis tarver-h" = "curtis j. tarver, ii-h",
        "tim ozinga-h" = NA_character_,
        "bill brady-s" = "william e. brady-s",
        "robert martwick-s" = "robert f. martwick-s"
      )
    )
}

if(t_yrs=="2021_2022"){
  all_sponsors2 = # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "diane blair-sherlock-h" = NA_character_,
        "javier cervantes-s" = NA_character_,
        "andre thapedi-h" = NA_character_,
        "heather steans-s" = NA_character_,
        "jaime andrade-h" = "jaime m. andrade, jr.-h",
        "suzanne glowiak-s" = "suzy glowiak hilton-s",
        "diane pappas-s" = NA_character_,
        "marcus evans-h" = "marcus c. evans, jr.-h",
        "elgie sims-s" = "elgie r. sims, jr.-s",
        "andy manar-s" = NA_character_,
        "maurice west-h" = "maurice a. west, ii-h",
        "curtis tarver-h" = "curtis j. tarver, ii-h",
        "kris tharp-s" = NA_character_,
        "michael madigan-h" = NA_character_
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


################################################################################
############### Estimate Scores + Add in Relatd Variables
######################################################################

### Check if bills in data without an ID'd sponsor
# filter(bills, !(bills$primary_sponsor %in% legis_data$data_name))
bills <- bills %>% #select(bills, -sponsor) %>%
  rename(sponsor = LES_sponsor) %>%
  mutate(chamber = substring(bill_id, 1, 1))

### Standard LES: Same as Congressional Measure
cat(' \n ------> Estimating LES Scores ')
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


cat(glue(". \n  **************** SESSION {t_yrs} ~~> DONE  ***********************"))
cat('\n __________________________________________________________ \n')


################# ****** END LOOP

#agg_stats, matches
rm(all_sponsors, bills, commem_bills, legis_data, SS_bills, elec_year, i, keep_types, c_sub)
rm(k_matches, klarner_sub, m_sub, match_name2, km, bill_path, t, t_yrs, calc_LES, parsed)
rm(t_sessions, reg_hist, out_chamber)

########################################################################################################################################################
########################################################################################################################################################
######## NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# ********** IF no record of win in general, but served multiple terms following, ASSUMING Appointed *************

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 session! ~~~~~~~~~~~~~~ 
# -----> Dropping 1 bill(s) without a sponsor
#### APPOINTED ~ HOUSE:
# BERGMAN = Robert Bergman (R) = Appointed Nov 14, 1996; Served only 1 term: https://en.wikipedia.org/wiki/Robert_L._Bergman
# BRADLEY = Rich Bradley = Appointed April 1997 - https://en.wikipedia.org/wiki/Rich_Bradley
# BROWN = Michael J. Brown (R) = Appointed July 1997 - https://en.wikipedia.org/wiki/Michael_J._Brown
# HOFFMAN = JAY C. HOFFMAN = Appointed Oct 1997 --- Previously held seat -- http://www.ilga.gov/house/Rep.asp?GA=98&MemberID=2061
# OCONNOR = WILLIAM A. OCONNOR = 3 Bills --- Must have been appointed during term --- http://www.ilga.gov/legislation/legisnet90/sponsor/O'CONNOR.html
# REITZ = Dan Reitz = Appointed during term -- https://dailyegyptian.com/46626/archives/the-designation-of-randolph-county-commissioner-dan-reitz-to-the-post-previously-held-by-ill-house-rep-terry-deering-d-dubois-has-left-barb-brown-reitzs-most-formidable-opponent-in-the-selection/
# RIGHTER = Dale RIGHTER = Appointed -- https://www.lib.niu.edu/1997/ii971142.html
# RODRIGUEZ (ELBA) -- replaced Miguel Santiago (D, District 3): http://www.ilga.gov/House/transcripts/Htrans90/T020398.PDF
#### APPOINTED ~ SENATE:
# MYERS = Judith Myers  = Appointed -- https://en.wikipedia.org/wiki/Judith_A._Myers
#### DUPLICATE FIXED: 
# WALSH,L -- Larry Walsh mismatched to Thomas Walsh
### IN CHAMBER
# Thomas DUNN --- Resigned in March 1997 after appointed to judiciary
### DROP
# PEDERSEN (Bernard) -- Dies shortly after wining elect


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 session! ~~~~~~~~~~~~~~ 
#### APPOINTED
# OSTERMAN, SHARP, MITCHELL (NED), RONEN, ROSKAM, SULLIVAN, JONES (Duplicated below)
# --- Sharp = 1 term appointee -- https://en.wikipedia.org/wiki/Wanda_Sharp
# --- Mitchell, Ned = 1 term appointedd -- https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=206705
# --- Ronen appointed to Senate; moved from House -- https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=9878
# --- Roskam appointed to Senate; moved from House -- https://en.wikipedia.org/wiki/Peter_Roskam#cite_note-8
# --- Sullivan (Dave) -- https://en.wikipedia.org/wiki/Dave_Sullivan_(Illinois_politician)
### DUPLICATE FIXED 
# jones,w (wendell) --- mismatched to emil jones
### IN CHAMBER:
# Eugene MOORE -- Won election to be Cook County Recorder of Deeds in 1999; Election results are form April; though he doesn't show up in bluebook
### DROP: 
# Martin BUTLER -- Died in 1998 in office -- https://en.wikipedia.org/wiki/Marty_Butler

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 session! ~~~~~~~~~~~~~~ 
#### APPOINTED: 
# COLVIN, JEFFERSON, MARQUARDT, SIMPSON, WATSON, WRIGHT, ROSKAM, SULLIVAN, WOOLARD
# -- Marquardt appointed 1/9/2002; served one term - https://en.wikipedia.org/wiki/Roger_Marquardt
# -- Roskam/Sullivan -- See previous term
# -- SIMPSON = Suzanne D. Simpson -- Not in bluebooks, but: https://justfacts.votesmart.org/candidate/biography/33469/suzanne-simpson
# -- WRIGHT = Jonathan Wright - Appointed 6.21.2001 -- Pg 116 -- http://www.idaillinois.org/cdm/ref/collection/bb/id/43381
# -- WOOLARD = Larry Woolard = Appointed to Senate? -- Doesn't say that in bluebook (p. 130) but he also didn't win a 2000 election for Senate

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 session! ~~~~~~~~~~~~~~ 
# APPOINTED : GORDON, DUGAN, VERSCHOORE, FROEHLICH, FLIDER, PRITCHARD, MUNSON, GRUNLOH, FORBY, ALTHOFF, SODEN
# --- Grunloh --- Lost subsequent general election
# --- Forby -- Transitioned from House
# --- Soden --- Appointed, did not seek reelection
### FIXED DUPLICATE
# -- john e. bradley mismatched to richard t. bradley
### IN CHAMBER: 
# -- HOEFT, CURRY, KARPIEL (Unclear if/when she retired but seems it was her last term -- she's in the roster at top as serving in the 93rd)
### DROP
# -- SMITH -- Retired in Dec. 2002 --> After being reelected 


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 session! ~~~~~~~~~~~~~~ 
# APPOINTED to H: BEISER, GOLAR, RAMEY, TRACY, DURKIN (durkin was previously in House from 96-2000, ids match)
# APPOINTED to S: WILHELMI, AXLEY (appointed --> gen loss), MILLNER (via H), RAOUL
# DROP: STEVE DAVIS -- RESIGNED Dec. 2004, post-election, never seated --- https://en.wikipedia.org/wiki/Steve_Davis_(Illinois)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 session! ~~~~~~~~~~~~~~ 
# APPOINTED to H: RILEY, KRUPA (app --> gen loss), ARROYO
# **** Krupa was in office for VERY short time; appointed for ~1 week to fill aaron schocks seat after Congress win..
# APPOINTED TO S: DELGADO (via H in Dec. 2006, never sponsored a H bill) --> DROP in HOUSE

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 session! ~~~~~~~~~~~~~~ 
# APPOINTED to H: DELUCA, SENTE, JACKSON, CARBERRY
# ** Carberry did not run for reelection
# APPOINTED to S: MULROE, HUTCHINSON
# NAME UPDATE: 
# (1) jehan a. gordon --> gordonbooth
# (2) emily mcasey --> klunkmcasey
# FIXED DUPLICATE --- BETSY HANNIG mismatched to GARY HANNIG
# IN CHAMBER: SCULLY (--> judge Feb. 2009)  -- https://en.wikipedia.org/wiki/George_Scully_Jr.
# DROP: YOUNGE --- Passed away Dec 2008 -- https://en.wikipedia.org/wiki/Wyvetter_H._Younge


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 session! ~~~~~~~~~~~~~~ 
# APPOINTED to H: HALBROOK, KREZWICK, CARLI, SMITH (derrick), BARICKMAN, COSTELLO, 
# --------------- CABELLO, CASSIDY, GAFFNEY, DU BUCLET, WALSH
# --------------- EVANS (marcus), HAMMOND, ROTH, EVANS (paul), PENNY
# *** Didn't run in General: krezwick, carli, gaffney (lost primary), du buclet, evans (paul)?, penny?, 
# *** Note: Barickman appointed to H, then won S in 2012
# APPOINTED to S: JOHNSON (christine), LAHOOD, MCGUIRE, SANDACK (then to H), 
# ---------------- CULTRA (via H), LANDEK, REZIN (via H), JOHNSON (thomas)
# *** Didn't run in General: johnson (c) (lost primary)
# DROP: CULTRA + REZIN IN HOUSE ONLY (Appointed to S at start of term)
# DROP: MYERS --- Passed away Dec 1, 2010
# FIXED DUPLICATE --- Annazette R. Collins mismatched to Jacqueline Collins (Senate)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 session! ~~~~~~~~~~~~~~ 
# APPOINTED to H: MOELLER, STEWART, DAVIDSMEYER, ANDRADE, ANTHONY (john)
# DROP: Jim WATSON --- Resigned Dec. 3, 2012 post-win -- https://ballotpedia.org/Jim_Watson_(Illinois)
# NAME FIXES:
# (1) Deborah/Deb CONROY --> okeefeconroy
# (2) Patricia VAN PELT --> watkins

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 session! ~~~~~~~~~~~~~~ 
# APPOINTED to H: SKOOG, BOURNE, WELTER, JIMENEZ, HARPER, BUTLER
# APPOINTED to S: WEAVER, BENNETT
# FIXED DUPLICATE: laura m. murphy mismatched to matt murphy (S)
# DROP -- Wayne ROSENTHAL --- Appointed in January 2015 to head a dept -- Only listed on IL Leg Page for 97th (2011-2012) and 98th (2013-2014)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 session! ~~~~~~~~~~~~~~ 
# APPOINTED to H: MAZZOCHI, CONNOR (john), CARROLL, SLAUGHTER, BRISTOW, FINNIE, SMITH (nicholas)
# APPOINTED to S: SIMS (via H), CURRAN, ROONEY (lost 2018 general)
# DROP: Monique DAVIS -- RESIGNED Jan. 6, 2017 -- https://en.wikipedia.org/wiki/Monique_D._Davis
# IN CHAMBER: Christine RADOGNO -- Resigned July 1, 2017 -- https://en.wikipedia.org/wiki/Christine_Radogno


# filter(klarner, grepl('davis, m', cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) 
# filter(klarner, grepl('rodrig', cand)) %>% select(cand, year, sen, outcome, ddez) %>% distinct()
# filter(bills, grepl('itchel', LES_sponsor) & substring(bill_id,1,1) == "S")


#########################################################################################################
##########################################################################################################
##### MATCHING MISSING (SPECIALS, ETC) TO KLARNER USING OTHER TERMS WHERE SPONSOR IS PRESENT
##########################################################################################################
# MEMBER LISTS: http://www.ilga.gov/previousga.asp
##########################################################################################################

### Re-Load LES Data
LES_paths <- list.files('.', full.names = TRUE)
LES_paths <- LES_paths[!grepl('Archive|_All|Merged', LES_paths)]

LES <- LES_paths %>%
  lapply(read_csv, col_types = cols()) %>%
  bind_rows 

rm(LES_paths)

### Pre-fix
LES[LES$sponsor == "davidsmeyerna",]$sponsor <- 'davidsmeyer'

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

### Fix Mismatched
# *** Brown == "Michael J Brown", not "Adam Brown"
LES[LES$data_name %in% "brown" & LES$term %in% "1997_1998", c("klarner_name", "klarner_id")] <- NA
LES[LES$data_name %in% "brown" & LES$term %in% "1997_1998", "sponsor"] <- "brown, michael"

### **** STILL MISSING ********
# **** Note: 2001-2002 Wright == JONATHAN, NOT 2002 LOSER T. ALLEN
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[25]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, still_missing)

### Fix Unmatched and MisMatched
# e.g., LES[LES$data_name %in% "burns" & LES$term %in% "1993_1994",]$sponsor <- 'burns, barbara'
name_matches <- data.frame(LES_name = 'bradley', term = "1997_1998", k_name = 'bradley, richard t.', k_year = "1998")
name_matches <- add_row(name_matches, LES_name = 'brown, michael', term = "1997_1998", k_name = 'brown, michael j. (mike)', k_year = "1998")
name_matches <- add_row(name_matches, LES_name = "o'connor", term = "1997_1998", k_name = "oconnor, william a.", k_year = '1998')
name_matches <- add_row(name_matches, LES_name = "walsh, l", term = "1997_1998", k_name = "walsh, lawrence m. (larry)", k_year = '1998')
name_matches <- add_row(name_matches, LES_name = "sullivan", term = "1999_2000", k_name = "sullivan, dave", k_year = '2002')
name_matches <- add_row(name_matches, LES_name = "sullivan", term = "2001_2002", k_name = "sullivan, dave", k_year = '2002')
name_matches <- add_row(name_matches, LES_name = "walsh, l", term = "2011_2012", k_name = "walsh, lawrence larry jr.", k_year = '2012')
name_matches <- add_row(name_matches, LES_name = "johnson, t", term = "2011_2012", k_name = "johnson, thomas l.", k_year = '2000') # Appointe to Senate ~10 years post H

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor %in% name_matches[i,]$LES_name & LES$term %in% name_matches[i,]$term,]$klarner_id <- klarner[klarner$year == name_matches[i,]$k_year & klarner$cand == name_matches[i,]$k_name,]$candid
  LES[LES$sponsor %in% name_matches[i,]$LES_name & LES$term %in% name_matches[i,]$term,]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor %in% name_matches[i,]$LES_name & LES$term %in% name_matches[i,]$term,]$sponsor <- name_matches[i,]$k_name
} 
rm(i, name_matches, name_sub, missing)


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
rm(check_dup, k_sub, exact, t)

########################################################################################################################################################
########################################################################################################################################################
######## AGGREGATE AND MATCH TO EXTERNAL
########################################################################################################################################################
########################################################################################################################################################

### Klarner State Leg. Election Data --- Using Adjusted File From Top
klarner_sub <- filter(klarner, sab == this_state & year >= min_year - 4 & outcome == 'w')
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
# filter(klarner, grepl('soden', cand)) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

### Not In Klarner
# -- Bergman Appointment in Journal: http://www.ilga.gov/House/transcripts/Htrans90/T010897.PDF
fill_missing <- data.frame(LES_name = "bergman", new_name = 'bergman, robert l.', party = 'r', district = 54, exper = 'none')
# -- Elba Rodriguez replaced Miguel Santiago (D, District 3): http://www.ilga.gov/House/transcripts/Htrans90/T020398.PDF
fill_missing <- add_row(fill_missing, LES_name = "rodriguez", new_name = 'rodriguez, elba', party = 'd', district = 3, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "sharp", new_name = 'sharp, wanda', party = 'd', district = 7, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "mitchell, n", new_name = 'mitchell, ned', party = 'd', district = 59, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "wright", new_name = 'wright, jonathan', party = 'r', district = 90, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "marquardt", new_name = 'marquardt, roger', party = 'r', district = 39, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "grunloh, w", new_name = 'grunloh, william j.', party = 'd', district = 108, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "soden, r", new_name = 'soden, raymond', party = 'r', district = 23, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "hannig, b", new_name = 'hannig, betsy', party = 'd', district = 98, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "carberry, m", new_name = 'carberry, michael j.', party = 'd', district = 36, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "carli, d", new_name = 'carli, dena m.', party = 'd', district = 1, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "gaffney, k", new_name = 'gaffney, kent', party = 'r', district = 52, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "du buclet, k", new_name = 'du buclet, kimberly', party = 'd', district = 26, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "penny, s", new_name = 'penny, scott e.', party = 'd', district = 113, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "krezwick, c", new_name = 'krezwick, charles w.', party = 'd', district = 37, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "evans, p", new_name = 'evans, paul', party = 'r', district = 102, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "johnson, c", new_name = 'johnson, christine j.', party = 'r', district = 35, exper = 'none')
## *** 2017+ ****
fill_missing <- add_row(fill_missing, LES_name = "slaughter, j", new_name = 'slaughter, justin', party = 'd', district = 27, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "finnie, n", new_name = 'finnie, natalie phelps', party = 'd', district = 118, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "bristow, m", new_name = 'bristow, monica', party = 'd', district = 111, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "carroll, j", new_name = 'carroll, jonathan', party = 'd', district = 57, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "smith, n", new_name = 'smith, nicholas k.', party = 'd', district = 34, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "mazzochi, d", new_name = 'mazzochi, deanne m.', party = 'r', district = 47, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "curran, j", new_name = 'curran, john f.', party = 'r', district = 41, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "rooney, t", new_name = 'rooney, tom', party = 'r', district = 27, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t, fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **

### Manual Fixes --Supplementing/Collapsing
LES[LES$sponsor %in% c("klunkmcasey, emily", "mcasey, emily"),]$sponsor <- 'mcasey, emily klunk'
LES[LES$sponsor %in% "jonesiii, emil",]$sponsor <- "jones, emil iii"

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

rm(hf_data, set_NA)

#################################
### Shor and McCarty Data, 1993 - 2016
####################################
## *** FOR ILLINOIS: SM Data covers 1996 - 2016 ****

ideo <- readstata13::read.dta13("~/Dropbox/Data/Shor_McCarty_Data/shor_mccarty_1993_2016_individual_data_May_2018.dta")
ideo <- filter(ideo, st == this_state) %>% select(-st_id)
ideo$match_name <- str_extract(tolower(ideo$name), '[a-z]+, [a-z]+')
ideo$match_name <- ifelse(is.na(ideo$match_name), tolower(ideo$name), ideo$match_name)
ideo$last_name <- gsub(',.+', '', tolower(ideo$name))

#### Doing this row by row to more easily account for party, unique data_names, etc.
#### Starting with MT (May 7, 2019) this now cross-checks to make sure it doesn't match on last name if multiple smiths, for example.
####---> Added Code to Match Party Switchers if Both Present in Data
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
      if(length(unique(LES[LES$sponsor == LES[i,]$sponsor,]$party)) == 2){
        for(p in c('d', 'r')){
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
# -- Carroll = late 90s --> Not Jonathan (2017+)
LES[LES$sponsor %in% c('carroll, jonathan', 'walsh, lawrence larry jr.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches: None
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>%
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

#LES[LES$sponsor %in% c('zzzzzzzzzz'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES -- SM Data only goes through 2016 so matches after that are from earlier period
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('glen', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'mitchell, gerald l. (jerry)', SM_name = 'Mitchell, Jerry L.')
#name_matches <- add_row(name_matches, LES_name = 'bradford, glenn e.', SM_name = 'zzzzzzzzzzzzz')
#name_matches <- add_row(name_matches, LES_name = 'hannig, betsy', SM_name = 'zzzzzzzzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'johnson, timothy v. (tim)', SM_name = 'Johnson, Timothy')
name_matches <- add_row(name_matches, LES_name = 'jones, emil jr.', SM_name = 'Jones, Emil Jr.')
name_matches <- add_row(name_matches, LES_name = 'jones, emil iii', SM_name = 'Jones, Emil III')
# name_matches <- add_row(name_matches, LES_name = 'krupa, joan gore', SM_name = 'zzzzzzzzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'madigan, lisa', SM_name = 'zzzzzzzzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'mitchell, ned', SM_name = 'zzzzzzzzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'turner, arthur', SM_name = 'Turner, Arthur Jr.')
name_matches <- add_row(name_matches, LES_name = 'turner, arthur l.', SM_name = 'Turner, Arthur Sr.')
## ** Walsh: Jr/Sr collapsed into single row
# name_matches <- add_row(name_matches, LES_name = 'walsh, lawrence larry jr.', SM_name = 'zzzzzzzzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'watkins, patricia vanpelt', SM_name = 'Van Pelt, Patricia')
name_matches <- add_row(name_matches, LES_name = 'weaver, michael (mike)', SM_name = 'Weaver')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########

#### Check for Potential Switchers
# filter(LES, party == 'd' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()

# **** Paul Evans --- SM have him as R in 2011_2012 and D in 2015_2016 -- Weird.. Only served 2011-2012.. Confusing with Marcus Evans?
LES[LES$sponsor == 'evans, paul' & LES$term == "2011_2012",]$SM_name <-  ideo[ideo$name == 'Evans, Paul' & ideo$house2011 %in% 1,]$name
LES[LES$sponsor == 'evans, paul' & LES$term == "2011_2012",]$SM_party <- ideo[ideo$name == 'Evans, Paul' & ideo$house2011 %in% 1,]$party
LES[LES$sponsor == 'evans, paul' & LES$term == "2011_2012",]$np_score <- ideo[ideo$name == 'Evans, Paul' & ideo$house2011 %in% 1,]$np_score

# ******* Patrick J. O'Malley -- No evidence that he was ever a democrat...?
LES[LES$sponsor == "omalley, patrick j.",]$party <- 'r'
LES[LES$sponsor == 'omalley, patrick j.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == "O'Malley, Patrick" & ideo$party == 'R',]$name
LES[LES$sponsor == 'omalley, patrick j.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == "O'Malley, Patrick" & ideo$party == 'R',]$party
LES[LES$sponsor == 'omalley, patrick j.' & LES$party == 'r',]$np_score <- ideo[ideo$name == "O'Malley, Patrick" & ideo$party == 'R',]$np_score


rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)


############################################
######## MORE NAME STANDARDIZATION
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct() %>% as.data.frame()

### REMOVE NICKNAMES
LES$sponsor <- str_trim(gsub('  +', ' ', gsub('\\([^\\)]+\\)', '', LES$sponsor)))
# table(LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

#### Adjust Names:
# filter(LES, nchar(sponsor) < 15 & nchar(gsub('^rep. |^sen. ', '', data_name)) > nchar(sponsor)) %>% distinct(sponsor, data_name, klarner_name)
LES[LES$sponsor == "turner, arthur l.",]$sponsor <- "turner, arthur sr."
LES[LES$sponsor == "turner, arthur",]$sponsor <- "turner, arthur jr."
LES[LES$sponsor == "walsh, lawrence larry jr.",]$sponsor <- "walsh, lawrence jr."
LES[LES$sponsor == "philip, james",]$sponsor <- "philip, james pate"
LES[LES$sponsor == "skoog, andy",]$sponsor <- "skoog, andrew f."
# LES[LES$sponsor == "zzzzzz",]$sponsor <- "zzzzz"
# LES[LES$sponsor == "zzzzzz",]$sponsor <- "zzzzz"

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1997 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2020) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzz) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1997 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(2003:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1997:2002) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1




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
            max_LES = max(LES)) #%>% View()

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
  scale_color_manual(values=c("dodgerblue2", "purple2", "red2", "gray50"))

##### CHECK OUTLIERS --- Often indicative of party switch
# filter(LES, party == 'd' & np_score > .25) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & np_score < -.25) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
