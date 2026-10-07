

#####################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** MARYLAND *** BY SESSION
#####################################

## *********** Check scraper -- why no "ALL SPONSORS" for this: http://mgaleg.maryland.gov/webmga/frmMain.aspx?tab=subject3&ys=2006rs%2fbillfile%2fHB1070.htm

## QUESTIONS
# (1) What to do about DELEGATION-proposed bills? At present, dropping.
# ----> These delegations have chairs.... e.g., see Murphy, Margaret (http://dlslibrary.state.md.us/publications/Joint/Misc/HRLBCM_2010.pdf)
# (2) What to do about special session bill reintroductions?


##################################
## SPECIAL SESSIONS:
## ---- Seperate files; bill numbers re-start
## ----> Bills can be reintroduced. See, e.g., 'Maryland Medical Injury Compensation Reform Act' in 2003_2006 term
## MEMBER LISTS:
## ---- CURRENT: http://mgaleg.maryland.gov/webmga/frmmain.aspx?pid=legisrpage&tab=subject6
## ---- PAST SENATE: http://mgaleg.maryland.gov/webmga/frmmain.aspx?pid=legisrpage&tab=subject6&s=fsen
## ---- PAST HOUSE: http://mgaleg.maryland.gov/webmga/frmmain.aspx?pid=legisrpage&tab=subject6&s=fhse
## PROCESS:
## ---- See "MD - Legislative-Process.pdf"
## Sponsorship/Authorship
## ---- Multiple sponsors permitted
## ---- DELEGATION/COUNTY sponsorship permitted
## ---- COMMITTEE sponsorship permitted, but typically attributed directly to chair (or clean when it is)
###################################
## NOTE:
## -- ************* AGGREGATING in 4-year blocks, but DO NOT HAVE DATA FOR first year (1995) OF 1995-1998 TERM ***********
##################################

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
library(inexact)
library(tibble)

this_state <- 'MD'
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


data_files <- list.files(glue("../../../State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
sessions <- gsub(".+Bill_Details_|.csv", '', bill_files)
rm(data_files, bill_files)

### Terms/Sessions and Filespaths
terms <- 2019
t = terms
t_yrs <- as.numeric(terms)
t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 3}'))
t_sessions = sessions[grepl(as.character(t),sessions) | grepl(as.character(t+1),sessions) | 
                        grepl(as.character(t+2),sessions) | grepl(as.character(t+3),sessions)]

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- bind_rows(read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t+1}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t+2}.csv")),
                      read.csv(glue("../../../State Legislative Data/Significant Bills/Project_VoteSmart_Bills/{this_state}/{this_state}_SS_Bills_{t+3}.csv")))

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
  select(state = State, term, year, bill_id, everything()) %>% 
  mutate(term = t_yrs) # maryland fix


###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- 3



### Skip Previously Estimated
if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
  print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
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
    s_bills$session <- as.character(s_bills$session)
    bills <- bind_rows(bills, s_bills)
  }
  rm(s, s_bills)
}

### Clean Term/Session Variables
bills$term <- t_yrs
bills$session_type <- recode(gsub('^[0-9]+', '', bills$session), 'rs' = 'RS', 's1' = 'SS1', 's2' = 'SS2', 's3' = 'SS3', 's4' = 'SS4')
bills$session <- paste0(bills$session_year, '-', bills$session_type)
bills <- select(bills, -c(session_year, session_type)) 

### Check for duplicates
if(nrow(bills) != nrow(distinct(bills))){
  cat(" \n ~~~> DUPLICATE BILLS \n\n .")
  break
}

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_number)
bills <- arrange(bills, session, bill_id)

############### Drop Resolutions, Messages, Communications, Reports
all_bills <- bills
bills <- mutate(bills, bill_type = toupper(gsub('[0-9]+', '', bill_id)))
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)

#########################
###### Standardize Sponsors 

#### Accents in Names
bills$main_sponsor <- tolower(bills$main_sponsor)
bills$main_sponsor <- gsub('á|ã¡', 'a', bills$main_sponsor)
bills$main_sponsor <- gsub('é|ã©', 'e', bills$main_sponsor)
bills$main_sponsor <- gsub('ó', 'o', bills$main_sponsor)
bills$main_sponsor <- gsub('í', 'i', bills$main_sponsor)
bills$main_sponsor <- gsub('ñ|ã±', 'n', bills$main_sponsor)

### All sponsors --> Full names, but this is not present for all bills... can use to get identity of sponsor, however.... 
# bills$all_sponsors <- tolower(bills$all_sponsors)
# bills$all_sponsors <- gsub('á|ã¡', 'a', bills$all_sponsors)
# bills$all_sponsors <- gsub('é|ã©', 'e', bills$all_sponsors)
# bills$all_sponsors <- gsub('ó', 'o', bills$all_sponsors)
# bills$all_sponsors <- gsub('í', 'i', bills$all_sponsors)
# bills$all_sponsors <- gsub('ñ|ã±', 'n', bills$all_sponsors)  

##### Extract and Parse
bills$LES_sponsor <- ifelse(grepl('^chairman|^chair|committee|delegation', bills$main_sponsor), bills$main_sponsor, str_trim(tolower(gsub(',.+| and .+', '', bills$main_sponsor))))
bills$LES_sponsor <- gsub('^delegates|^delegate|^delgates|^senators|^senator', '', bills$LES_sponsor)
bills$LES_sponsor <- gsub('\n', ' ', bills$LES_sponsor)
bills$LES_sponsor <- gsub(' and delegat.+| and senato.+ | anddelegat.+| andsenato.+', '', bills$LES_sponsor)
bills$LES_sponsor <- gsub(' \\(.+', '', bills$LES_sponsor)
bills$LES_sponsor <- gsub(', the speaker,$', '', bills$LES_sponsor) ### the speaker as a cosponsor (ends with comma because of other adjustments)
bills$LES_sponsor <- gsub('matterscommittee', 'matters committee', bills$LES_sponsor) 
bills$LES_sponsor <- gsub(' ,$|,$', '', bills$LES_sponsor)
bills$LES_sponsor <- str_trim(gsub('  +', ' ', bills$LES_sponsor))

#### Manual Fixes
# filter(bills, LES_sponsor == "mcfadden blount") %>% select(-c(summary, title, subjects, bill_url))
if(t_yrs == "1995_1998"){
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "barve andgordon", "barve", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "doory andrawlings", "doory", bills$LES_sponsor)
}else if(t_yrs == "1999_2002"){
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "shriver turner", "shriver", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "wood malone", "wood", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "mchale hammen", "mchale", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "mcfadden blount", "mcfadden", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "w.baker", "w. baker", bills$LES_sponsor)
  ### Murphy = Donald E. Murphy after Timoth D. Murphy left office
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "murphy", 'd. murphy', bills$LES_sponsor)
  ### Kelly = Kevin Kelly after James Kelly left office
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "kelly", 'k. kelly', bills$LES_sponsor)
  ### Bills Miscoded as President when Should be Speaker -- http://mgaleg.maryland.gov/webmga/frmMain.aspx?tab=subject3&ys=2000rs%2fbillfile%2fHB0150.htm
  bills[bills$bill_id == "HB0150" & bills$session == '2000-RS',]$LES_sponsor <- 'speaker'
}else if(t_yrs == "2003_2006"){
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "cadden conroy", "cadden", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "costa frank", "costa", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "d. davis mchale", "d. davis", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "hixson patterson", "hixson", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "jennings cane", "jennings", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "madaleno. gutierrez", "madaleno", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "v. clagett andhaynes", "v. clagett", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('^hixson, howard, bozman', bills$LES_sponsor), "hixson", bills$LES_sponsor)
}else if(t_yrs == "2007_2010"){
  ### King = James J. King after Nancy King appointed to Senate
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "king", "j. king", bills$LES_sponsor)
}else if(t_yrs == "2011_2014"){
  ### *** Some of these are now recorded as Lastname, FirstInitial.
  bills$LES_sponsor <- ifelse(grepl('kelly, k.', bills$main_sponsor) & bills$LES_sponsor == "kelly", "k. kelly", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('kelly, a.', bills$main_sponsor) & bills$LES_sponsor == "kelly", "a. kelly", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "kelly" & bills$session == '2014-RS', "a. kelly", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('miller, a.', bills$main_sponsor) & bills$LES_sponsor == "miller", "a. miller", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('miller, w.', bills$main_sponsor) & bills$LES_sponsor == "miller", "w. miller", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('robinson, b.', bills$main_sponsor) & bills$LES_sponsor == "robinson", "b. robinson", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('robinson, s.', bills$main_sponsor) & bills$LES_sponsor == "robinson", "s. robinson", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('turner, v.', bills$main_sponsor) & bills$LES_sponsor == "turner", "v. turner", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse((grepl('turner, f.', bills$main_sponsor)|grepl('^Delegates F. Turner', bills$all_sponsors)) & bills$LES_sponsor == "turner", "f. turner", bills$LES_sponsor)
}else if(t_yrs == "2015_2018"){
  ### *** Some of these are now recorded as Lastname, FirstInitial.
  bills$LES_sponsor <- ifelse(grepl('miller, a.', bills$main_sponsor) & bills$LES_sponsor == "miller", "a. miller", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('miller, w.', bills$main_sponsor) & bills$LES_sponsor == "miller", "w. miller", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('robinson, b.', bills$main_sponsor) & bills$LES_sponsor == "robinson", "b. robinson", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('robinson, s.', bills$main_sponsor) & bills$LES_sponsor == "robinson", "s. robinson", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "robinson" & bills$session %in% c('2017-RS', '2018-RS'), "s. robinson", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('barnes, b.', bills$main_sponsor) & bills$LES_sponsor == "barnes", "b. barnes", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('barnes, d.', bills$main_sponsor) & bills$LES_sponsor == "barnes", "d. barnes", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('howard, s.', bills$main_sponsor) & bills$LES_sponsor == "howard", "s. howard", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('howard, c.', bills$main_sponsor) & bills$LES_sponsor == "howard", "c. howard", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('washington, m.', bills$main_sponsor) & bills$LES_sponsor == "washington", "m. washington", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('washington, a.', bills$main_sponsor) & bills$LES_sponsor == "washington", "a. washington", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('wilson, b.', bills$main_sponsor) & bills$LES_sponsor == "wilson", "b. wilson", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('wilson, c.', bills$main_sponsor) & bills$LES_sponsor == "wilson", "c. wilson", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "wilson" & bills$session %in% c( '2018-RS'), "c. wilson", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('young, k.', bills$main_sponsor) & bills$LES_sponsor == "young", "k. young", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('young, p.', bills$main_sponsor) & bills$LES_sponsor == "young", "p. young", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('lewis, r.', bills$main_sponsor) & bills$LES_sponsor == "lewis", "r. lewis", bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('lewis, j.', bills$main_sponsor) & bills$LES_sponsor == "lewis", "j. lewis", bills$LES_sponsor)
  ### James Proctor Died Sep. 2015, Wife Elizabeth Proctor Replaced him
  bills[bills$LES_sponsor %in% "proctor" & bills$session == "2015-RS", ]$LES_sponsor <- 'j. proctor'
  bills[bills$LES_sponsor %in% "proctor" & bills$session != "2015-RS", ]$LES_sponsor <- 'e. proctor'
} else if(t_yrs == "2019_2022"){
  bills[bills$LES_sponsor == "davis", ]$LES_sponsor <- 'd.m. davis' # only debra at the end in 22
  bills[bills$LES_sponsor == "jackson" & bills$session == "2019-RS", ]$LES_sponsor <- 'm. jackson'
  bills[bills$LES_sponsor == "jackson" & bills$session %in% c("2021-RS","2022-RS"), ]$LES_sponsor <- 'm. jackson'
  bills[bills$LES_sponsor == "branch", ]$LES_sponsor <- 't. branch'
  bills[bills$LES_sponsor == "jones" , ]$LES_sponsor <- 'a. jones' # always the speaker
  bills[bills$LES_sponsor == "watson" & substr(bills$bill_id,1,1) == "H", ]$LES_sponsor <- 'c. watson' 
  bills[bills$LES_sponsor == "elfreth kramer" , ]$LES_sponsor <- 'elfreth' 
  bills[bills$bill_id == "HB1346" , ]$LES_sponsor <- 'p. young' 
}
#filter(bills, LES_sponsor == "kelly") %>% select(-c(summary, title, subjects)) %>% View()


###################
### CODE LEADERSHIP --- Need to adjust these for duplicate last names by term...
#####################
### Code the President
if(t_yrs %in% c("1995_1998", "1999_2002", "2003_2006", "2007_2010", "2011_2014", "2015_2018") ){
  bills[grepl('president', bills$LES_sponsor),]$LES_sponsor <- 'miller'
} 
if(t_yrs == "2019_2022"){
  bills = bills %>% 
    mutate(LES_sponsor = case_when(
      grepl('president', LES_sponsor) & session == "2019-RS" ~ "miller", # he steps down at the start of 20
      grepl('president', LES_sponsor) & session != "2019-RS" ~ "ferguson",
      T ~ LES_sponsor))
}


### Code the Speaker
if(t_yrs %in% c("1995_1998", "1999_2002") ){
  bills[grepl('speaker', bills$LES_sponsor),]$LES_sponsor <- 'taylor'
} else if(t_yrs %in% c("2003_2006", "2007_2010", "2011_2014", "2015_2018")){
  bills[grepl('speaker', bills$LES_sponsor),]$LES_sponsor <- 'busch'
}

if(t_yrs == "2019_2022"){
  bills = bills %>% 
    mutate(LES_sponsor = case_when(
      grepl('speaker', LES_sponsor) & session == "2019-RS" ~ "busch", # he steps down at the start of 20
      grepl('speaker', LES_sponsor) & session != "2019-RS" ~ "a. jones",
      T ~ LES_sponsor))
}

### Code Minority Leader
if(t_yrs == '2003_2006'){
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "the minority leader" & substring(bills$bill_id, 1, 1) == "H", "edwards", bills$LES_sponsor)
}else if(t_yrs == "2007_2010"){
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "the minority leader" & substring(bills$bill_id, 1, 1) == "H", "o'donnell", bills$LES_sponsor)
}else if(t_yrs == "2011_2014"){
  ### House minority bills are from 2011; o'donnell was ousted in 2013
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "the minority leader" & substring(bills$bill_id, 1, 1) == "H", "o'donnell", bills$LES_sponsor)
  ### Brinkley -- https://en.wikipedia.org/wiki/David_R._Brinkley
  bills$LES_sponsor <- ifelse(bills$LES_sponsor %in% c("the minority leader", 'minority leader') & substring(bills$bill_id, 1, 1) == "S", "brinkley", bills$LES_sponsor)
} else if(t_yrs == "2019_2022"){
  bills$LES_sponsor <- ifelse(bills$LES_sponsor == "minority leader" & substring(bills$bill_id, 1, 1) == "H", "kipke", bills$LES_sponsor)
  # kipke steps down in 2021 but all the relevant bills are before then
  bills$LES_sponsor <- ifelse(bills$LES_sponsor %in% c("the minority leader", 'minority leader') & substring(bills$bill_id, 1, 1) == "S", "jennings", bills$LES_sponsor)
  # jennings steps down later in 2020 but all the relevant bills are his
}



###################
###### Merge in S&S Bills
###################
# *** For MARYLAND: Merging on Year and Special -- When multiple specials, assuming special with most proposals (usually only SS1 anyway)
# *** Although, in practice, no special bills identified

if(t_yrs == "2019_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="SB40004"] = "SB0004"
  SS_bills$bill_id[SS_bills$bill_id=="HB40004"] = "HB0004"
  SS_bills$bill_id[SS_bills$bill_id=="HB10001"] = "HB0001"
}

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, year, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS, year,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,main_sponsor), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",main_sponsor, ignore.case=T)) %>%
  arrange(main_sponsor) 
unique(missing_SS_bills$bill_id) 


# now check to see if there are duplicate joins


duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills %>% mutate(year = as.integer(substr(session,1,4))), 
            by = c("bill_id", "term", "year")) %>%
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

#################################################################
############### Code Commemorative
#################################################################

bills <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session'))
table(bills$commem)

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)


#### DROP DELEGATION Bills
if(any(grepl('delegation|senators$|delegates$|county', bills$LES_sponsor))){
  print(glue("-----> DROPPING {nrow(filter(bills, grepl('delegation|senators$|delegates$|county', LES_sponsor)))} bill(s) introduced BY DISTRICT DELEGATION"))
  bills <- filter(bills, !grepl('delegation|senators$|delegates$|county', LES_sponsor))
}

#### DROP Committee Bills
if(any(grepl('committee|^chair', bills$LES_sponsor))){
  print(glue("-----> DROPPING {nrow(filter(bills, grepl('committee|^chair', LES_sponsor)))} bill(s) introduced BY COMMITTEES/CHAIRS"))
  bills <- filter(bills, !grepl('committee|^chair', LES_sponsor))
}

##############
###### CHeck Missing Sponsors
# filter(bills, LES_sponsor == "") %>% View()
if(nrow(filter(bills, LES_sponsor == '')) > 0){
  print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
  bills <- filter(bills, !(LES_sponsor == ''))     
}

#################################################################
############### Code Bill History
#################################################################

bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
bill_hist <- read.csv(bill_hist_path)

### If multiple sessions in different files, read in those as well
if(length(t_sessions) > 1){
  for(s in t_sessions[2:length(t_sessions)]){
    bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
    s_hist <- read.csv(bill_path)
    s_hist$session <- as.character(s_hist$session)
    bill_hist <- bind_rows(bill_hist, s_hist)
  }
  rm(s, s_hist)
}

### Clean Term/Session Variables
bill_hist$term <- t_yrs
bill_hist$session_type <- recode(gsub('^[0-9]+', '', bill_hist$session), 'rs' = 'RS', 's1' = 'SS1', 's2' = 'SS2', 's3' = 'SS3', 's4' = 'SS4')
bill_hist$session <- paste0(bill_hist$session_year, '-', bill_hist$session_type)
bill_hist <- select(bill_hist, -c(session_year, session_type)) 

######## Standardize BillHist Bill IDs
bill_hist <- rename(bill_hist, bill_id = bill_number)
bill_hist <- arrange(bill_hist, session, bill_id, action_date) %>% 
  group_by(session, bill_id) %>%
  mutate(order = 1:n()) %>%
  ungroup()

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c('hearing', 'report by', 'favorable', 'unfavorable', 'no recommendation', 'committee amendment',
           'referred.+interim study') # interim study kills bill, but implies action
# favorable, unfavorable, favorable with amendment, or rarely, no recommendation
abc_t <- c('favorable.+report', 'report adopted', 'floor.+amendment', 'second reading', 'third reading', 'calendar')
## Report adopted/not by whole chamber (precursor to second reading)
pc_t <- c('^third reading passed', 'enrolled') 
# Enrolled = check
law_t <- c('signed by the gov', 'chapter [0-9]+')
# filter(bill_hist, grepl('enrolled', tolower(action))) %>% distinct(action)
# filter(bill_hist, chamber == "Executive") %>% distinct(action)
# filter(bill_hist, bill_id == 'SB 1' & session == '71-SS5') %>% select(bill_id, session, chamber, action_date, action, order)

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
                                    ignore_chamber_switch = TRUE) # First reading in oppo chamber seems to happen prior to passage/2nd reading in intro chamber in some cases
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


read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-4}_{t-1}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% filter(!grepl("-SS",session)) %>%  print()

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-8}_{t-5}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% filter(!grepl("-SS",session)) %>% print()

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
  left_join(all_bill_stages,.,
              by = c("bill_id", "term","session")) %>%
  mutate(SS = ifelse(is.na(SS), 0, SS)) %>% 
  distinct()

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

######## Cosponsorship Info 
# -----------> **** DON'T HAVE THIS FOR ALL BILLS + WOLD BE A PAIN WITH COMMITTEES ***********
all_sponsors$num_cosponsored_bills <- NA
# for(i in 1:nrow(all_sponsors)){
#   c <- all_sponsors[i,]$chamber
#   c_sub <- filter(bills, substring(bill_id, 1, 1) == c)
#   all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$coauthors)))
#   ### ONLY need to adjust this way if sponsored and cosponsored column are the same
#   # all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
# }
# all_sponsors <- select(all_sponsors, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))

#######################
#### CLEAN NAMES

all_sponsors$last_name <- ifelse(!grepl('^[a-z]\\. ', all_sponsors$LES_sponsor), all_sponsors$LES_sponsor, gsub('^[a-z]\\. ', '', all_sponsors$LES_sponsor))
all_sponsors$first_name <- ifelse(grepl('^[a-z]\\. ', all_sponsors$LES_sponsor), str_trim(str_extract(all_sponsors$LES_sponsor, "^[a-z]")), '')
all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update Last Names for Matching 
if(t_yrs %in% c('1999_2002')){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "v. jones", "jonesrodwell", all_sponsors$last_name)
}else if(t_yrs %in% c('2003_2006', '2007_2010')){
  all_sponsors[all_sponsors$last_name == 'jones' & all_sponsors$chamber == "S",]$last_name <- 'jonesrodwell'
}
if(t_yrs == "2011_2014"){
  all_sponsors$last_name <- ifelse(all_sponsors$LES_sponsor == "haddaway-riccio", "haddaway", all_sponsors$last_name)
}




all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>%
  mutate(match_name_chamber = tolower(paste(LES_sponsor,substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name))


legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(terms)) | startsWith(legiscan_sessions,as.character(terms+1)) |
                                        startsWith(legiscan_sessions,as.character(terms+2)) | startsWith(legiscan_sessions,as.character(terms+3))]

legiscan = foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% distinct() %>%
  filter(name != "The Speaker")

if(t_yrs == "2019_2022"){
  legiscan = legiscan %>% 
    mutate(district = ifelse(people_id == 17380 & role == "Rep", "HD-044", district),
           district = ifelse(people_id == 17377 & role == "Rep", "HD-011", district)
           
           )
}



legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name, substr(district,1,1)) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = case_when(
    n > 2 ~ paste0(substr(first_name,1,1),". ",  substr(middle_name,1,1)," ", last_name),
    T ~  paste0(last_name))) %>%
  group_by(match_name, substr(district,1,1)) %>% 
  mutate(n = n()) %>% 
  mutate(match_name = ifelse(n > 1, paste0(substr(first_name,1,1),". ", last_name),
                             match_name)) %>% 
  group_by(match_name, substr(district,1,1)) %>%
  mutate(n = n()) %>% 
  mutate(match_name = ifelse(n > 1, paste0(substr(first_name,1,1),". ", substr(middle_name,1,1), ". ",last_name),
                             match_name)) %>% 
  mutate(match_name_chamber = tolower(paste(match_name,substr(district,1,1),sep="-"))) 



# inexact::inexact_addin()
# left: legiscan_adj
# right: all_sponsors
# method: osa
# mode: full

if(t_yrs == "2019_2022"){
  all_sponsors2 = 
    # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "f. m. howell-h" = NA_character_,
        "c. s. landis-h" = "landis-h",
        "gaines-h" = NA_character_,
        "toles-h" = NA_character_,
        "d. . davis-h" = "d.m. davis-h"
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

##########################
##### Estimate Scores + Add in Relatd Variables
##########################

### Check if bills in data without an ID'd sponsor
bills <- bills %>% #select(-sponsors, cosponsors) %>%
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


cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************"))


################# ****** END LOOP

#agg_stats, matches
rm(all_sponsors, bills, legis_data, SS_bills, elec_year, i, keep_types)
rm(k_matches, klarner_sub, m_sub, km, bill_path, t_yrs, calc_LES)
rm(t, terms, klarner_gs, t_sessions, commem_bills, yrs)

########################################################################################################################################################
############################################################################################################################################################# NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
#-- GOVERNOR APPOINTS VACANCIES: http://www.ncsl.org/research/elections-and-campaigns/filling-legislative-vacancies.aspx
#-- Maryland Legislative Black Caucus -- http://dlslibrary.state.md.us/publications/Joint/Misc/HRLBCM_2010.pdf

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1998 TERM! ~~~~~~~~~~~~~~ 
# -----> DROPPING 621 bill(s) introduced BY DISTRICT DELEGATION
# -----> DROPPING 478 bill(s) introduced BY COMMITTEES/CHAIRS
## APPOINTED ~ HOUSE: 
# -- COMEAU (michael)
# -- MILLER (ellen)
# -- MOE (brian)
# -- OPARA (clay)
# -- WATSON (carmena, no record of appointment or election win)
## APPOINTED ~ SENATE:
# -- CONWAY (joan carter)
# -- FRY (donald, via H)
# -- JEFFERIES (john, past H)
# -- NEALL (robert, past H)
## IN HOUSE:
# -- MURPHY = murphy, margaret h. -- resigned at some point in 1996? see: http://dlslibrary.state.md.us/publications/Joint/Misc/HRLBCM_2010.pdf
# ----> conflicting info though... other sources suggest 1995, but again, date unclear

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2002 TERM! ~~~~~~~~~~~~~~ 
# -----> DROPPING 716 bill(s) introduced BY DISTRICT DELEGATION
# -----> DROPPING 588 bill(s) introduced BY COMMITTEES/CHAIRS
## APPOINTED ~ HOUSE:
# -- BATES (gail)
# -- BOHANAN (john)
# -- COLE (william)
# -- CROUSE (james)
# -- GAINES (tawanna)
## APPOINTED ~ SENATE:
# -- KITTLEMAN (robert, past H)
## NAME FIX:
# -- v. jones = verna jones --> verna jonesrodwell (through 2010)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2006 TERM! ~~~~~~~~~~~~~~ 
# -----> DROPPING 835 bill(s) introduced BY DISTRICT DELEGATION
# -----> DROPPING 526 bill(s) introduced BY COMMITTEES/CHAIRS
## APPOINTED ~ HOUSE
# -- CLUSTER (john)
# -- GILLELAND (terry, lost gen, potentially spelled gilliland)
# -- GOODWIN (marshall)
# -- HADDAWAY (jeannie)
# -- HENNESSY (louis)
# -- KOHL (sheryl davis)
# -- KULLEN (sue)
# -- LAWTON (jane)
# -- LEVY (murray)
# -- MAYER (wm. daniel)
# -- MILLER (warren)
# -- PUGH (catherine)
# -- SHEWELL (tanya)
## IN HOUSE:
# -- FLANAGAN = flanagan, robert l --> Left office Feb. 2003 to become Sec. of Transpo.

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> DROPPING 799 bill(s) introduced BY DISTRICT DELEGATION
# -----> DROPPING 512 bill(s) introduced BY COMMITTEES/CHAIRS
## APPOINTED ~ HOUSE:
# -- CARR (al)
# -- FRICK (bill)
# -- JENKINS (charles)
# -- NORMAN (H. Wayne)
# -- REZNIK (kirill)
# -- SERAFINI (andrew)
## APPOINTED ~ SENATE:
# -- GLASSMAN (barry, via H)
# -- HARRINGTON (david)
# -- KING (nancy, 9/2007, via H)
# -- REILLY (edward)
## NAME FIXES:
# -- KING in house --> J. King (james) aftere Nancy King appointed to Senate

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2014 TERM! ~~~~~~~~~~~~~~ 
# -----> DROPPING 707 bill(s) introduced BY DISTRICT DELEGATION
# -----> DROPPING 441 bill(s) introduced BY COMMITTEES/CHAIRS
## APPOINTED ~ HOUSE:
# -- ARENTZ (steve)
# -- FRASER-HIDALGO (david)
# -- SWAIN (darren)
## APPOINTED ~ SENATE:
# -- FELDMAN (brian, via H)
# -- HERSHEY (stephen, via H)
## NAME FIXES
# -- Formatting changed for initials, so fixed doubles for Kelly, Miller, Robinson and Turner

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2018 TERM! ~~~~~~~~~~~~~~ 
# -----> DROPPING 870 bill(s) introduced BY DISTRICT DELEGATION
# -----> DROPPING 315 bill(s) introduced BY COMMITTEES/CHAIRS
## APPOINTED ~ HOUSE:
# -- ALI (bilal)
# -- CILIBERTI (barry)
# -- CLARK (jerry)
# -- CORDERMAN (paul)
# -- GIBSON (angela)
# -- J. LEWIS (jazz)
# -- MALONE (michael)
# -- MOSBY (nick)
# -- QUEEN (pam)
# -- R. LEWIS (robbyn)
# -- ROSE (april)
# -- SANCHEZ (carlo)
# -- WILKINS (jheanelle)
# -- WIVELL (william)
## APPOINTED ~ SENATE:
# -- OAKS (nathaniel)
# -- READY (justin)
# -- S. ROBINSON (shane)
# -- SERAFINI (andrew)
# -- SMITH (william)
# -- ZUCKER (craig)
## IN HOUSE:
# -- READY (resigned 1 month in, feb 2015 for senate)
# -- CAMPOS (will, served 9 months, then convicted of corruption)
# -- SHANK (resigned 2 weeks in, Jan 21, 2015)
## NAME FIXES
# -- Fixed initials for last name duplicates for Barnes, Howard, Miller, Robinson, Washington, Wilson, Young

#########################

# filter(klarner, grepl('saqib,', cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(klarner, ddez == 46) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(bills, grepl('lewis', LES_sponsor)) %>% View()

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

### Manual Fixes --- Rose, April wont be need post-klarner update
LES[LES$data_name %in% c("ali", "sanchez", "rose") & LES$term == "2015_2018",]$klarner_id <- NA
LES[LES$data_name %in% c("ali", "sanchez", "rose") & LES$term == "2015_2018",]$klarner_name <- NA
LES[LES$data_name %in% 'ali' & LES$term == "2015_2018",]$sponsor <- "ali, bilal"
LES[LES$data_name %in% 'sanchez' & LES$term == "2015_2018",]$sponsor <- "sanchez, carlo"
LES[LES$data_name %in% 'rose' & LES$term == "2015_2018",]$sponsor <- "rose, april"

### Carmena Watson Matches to Ron Watson
LES[LES$data_name %in% "watson" & LES$term %in% '1995_1998', c("klarner_id", "klarner_name")] <- NA
LES[LES$data_name %in% "watson" & LES$term %in% '1995_1998', ]$sponsor <- "watson, carmena f."

########## ****Still missing***** 
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[17]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber)
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, missing, name_sub, still_missing)

name_matches <- data.frame(LES_name = "conway", k_name = 'conway, joan carter')
name_matches <- add_row(name_matches, LES_name = 'kittleman', k_name = 'kittleman, robert')
name_matches <- add_row(name_matches, LES_name = 'mayer', k_name = 'mayer, william daniel')
name_matches <- add_row(name_matches, LES_name = 'reilly', k_name = 'reilly, edward r.')
name_matches <- add_row(name_matches, LES_name = 'fraser-hidalgo', k_name = 'fraserhidalgo, david')
#name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', k_name = 'zzzzzzz')
#name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', k_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}

##### Matches requiring greater precision
LES[LES$sponsor %in% "miller" & LES$term %in% "1995_1998",]$klarner_id <- 94135
LES[LES$sponsor %in% "miller" & LES$term %in% "1995_1998",]$klarner_name <- "miller, ellen willis"
LES[LES$sponsor %in% "miller" & LES$term %in% "1995_1998",]$sponsor <- "miller, ellen willis"

LES[LES$sponsor %in% "miller" & LES$term %in% "2003_2006",]$klarner_id <- 274407
LES[LES$sponsor %in% "miller" & LES$term %in% "2003_2006",]$klarner_name <- "miller, warren e."
LES[LES$sponsor %in% "miller" & LES$term %in% "2003_2006",]$sponsor <- "miller, warren e."

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
    filter(LES, term == t & klarner_id %in% check_dup[check_dup$dup == TRUE,]$klarner_id) %>%
      select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>%
      print()
  }
}
rm(check_dup, k_sub, exact)


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
# filter(LES, is.na(party)) %>% select(sponsor, chamber, term, klarner_id, district, party, exper)
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

LES$district <- as.character(LES$district)

fill_missing <- data.frame(LES_name = "opara", new_name = 'opara, clay c.', party = 'd', district = "41", exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "watson, carmena f.", new_name = 'watson, carmena f.', party = 'd', district = "44", exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "jefferies", new_name = 'jefferies, john d.', party = 'd', district = "44", exper = 'pastother')
fill_missing <- add_row(fill_missing, LES_name = "cole", new_name = 'cole, williah h. iv', party = 'd', district = "47", exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "goodwin", new_name = 'goodwin, marshall t.', party = 'd', district = "40", exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "hennessy", new_name = 'hennessy, w. louis', party = 'r', district = "28", exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "jenkins", new_name = 'jenkins, charles a.', party = 'r', district = '3B', exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "harrington", new_name = 'harrington, david c.', party = 'd', district = "47", exper = 'none')
#### 2015_2018+
fill_missing <- add_row(fill_missing, LES_name = "queen", new_name = 'queen, pamela e.', party = 'd', district = "14", exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "ali, bilal", new_name = 'ali, bilal', party = 'd', district = "41", exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "mosby", new_name = 'mosby, nick j.', party = 'd', district = "40", exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "wivell", new_name = 'wivell, william j.', party = 'r', district = "2A", exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "sanchez, carlo", new_name = 'sanchez, carlo', party = 'd', district = '47B', exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "lewis, j", new_name = 'lewis, jazz m.', party = 'd', district = "24", exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "lewis, r", new_name = 'lewis, robbyn t.', party = 'd', district = "46", exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "proctor, e", new_name = 'proctor, elizabeth g.', party = 'd', district = '27A', exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "rose, april", new_name = 'rose, april r.', party = 'r', district = "5", exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "clark", new_name = 'clark, gerald w.', party = 'r', district = "29C", exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "corderman", new_name = 'corderman, paul d.', party = 'r', district = "2B", exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "gibson", new_name = 'gibson, angela c.', party = 'd', district = "41", exper = 'none')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

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
hf_data$term <- paste0(hf_data$year + 1, "_", hf_data$year + 4)
hf_data$chamber <- ifelse(hf_data$chamber == 'senate', 'Senate', "House")

### **** If > 2-Year Terms: Expand Senate Rows*****

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
LES[LES$term == '2015_2018', set_NA] <- NA

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

#### CHECK ERRORS
filter(LES, !is.na(SM_name)) %>%
  group_by(SM_name) %>% 
  summarize(name_matches = paste(unique(sponsor), collapse = "----")) %>% 
  filter(grepl("----", name_matches))

### Fix MIsmatches
LES[LES$sponsor %in% c('weir, michael h.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches: 
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

#### FIX MISMATCHES 
# Ron Watson != Carmena F. Watson
# LES[LES$sponsor %in% c('watson, ron'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('zzzz')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('watson', tolower(name))) %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

name_matches <- data.frame(LES_name = 'anderton, carl jr.', SM_name = 'Anderton Jr, Carl')
# name_matches <- add_row(name_matches, LES_name = 'ali, bilal', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'campos, will', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'clark, gerald w.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'corderman, paul d.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'dixon, richard n.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'ferguson, bill', SM_name = 'Ferguson, William IV')
name_matches <- add_row(name_matches, LES_name = 'frank, robert', SM_name = 'Frank')
name_matches <- add_row(name_matches, LES_name = 'frick, bill', SM_name = 'Frick, C.') # C. William Frick (SM frick obs should be 1 but this goes longer)
# name_matches <- add_row(name_matches, LES_name = 'gibson, angela c.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'hogan, patrick n.', SM_name = 'Hogan, Patrick')
# name_matches <- add_row(name_matches, LES_name = 'lewis, jazz m.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'lewis, robbyn t.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'miller, ellen willis', SM_name = 'Willis')
name_matches <- add_row(name_matches, LES_name = 'morgan, john s.', SM_name = 'Morgan')
# name_matches <- add_row(name_matches, LES_name = 'mosby, nick j.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'murphy, margaret h.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'parker, joan n.', SM_name = 'Parker')
# name_matches <- add_row(name_matches, LES_name = 'proctor, elizabeth g.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'shewell, tanya thornton', SM_name = 'Thornton Shewell, Tanya')
name_matches <- add_row(name_matches, LES_name = 'sydnor, charles e. iii.', SM_name = 'Sydnor III, Charles E')
name_matches <- add_row(name_matches, LES_name = 'weir, michael h.', SM_name = 'Weir')
# name_matches <- add_row(name_matches, LES_name = 'wilkins, victoria', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'young, larry', SM_name = 'Young')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

############
## Party Switches
###########

#### Patrick J Hogan --- Switched from R to D in Nov 2002
# ******** Not to be confused with Patrick N. Hogan!
LES[LES$sponsor == 'hogan, patrick j.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Hogan, Patrick John' & ideo$party == "R",]$name
LES[LES$sponsor == 'hogan, patrick j.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Hogan, Patrick John' & ideo$party == "R",]$party
LES[LES$sponsor == 'hogan, patrick j.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Hogan, Patrick John' & ideo$party == "R",]$np_score
LES[LES$sponsor == 'hogan, patrick j.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Hogan, Patrick John' & ideo$party == "D",]$name
LES[LES$sponsor == 'hogan, patrick j.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Hogan, Patrick John' & ideo$party == "D",]$party
LES[LES$sponsor == 'hogan, patrick j.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Hogan, Patrick John' & ideo$party == "D",]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname)


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1995 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:2020) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
#LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzzz) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1995 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
#LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzzz) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1



############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()

##### Drop Nicknames
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
#LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)
LES[LES$sponsor == "hoffman, barbara 1",]$sponsor <- 'hoffman, barbara a.'

### Manual Fixes
LES[LES$sponsor == "mcdonough, pat",]$sponsor <- 'mcdonough, patrick'
LES[LES$sponsor == "valderrama, kris",]$sponsor <- 'valderrama, kriselda'
LES[LES$sponsor == "norman, wayne",]$sponsor <- 'norman, h. wayne jr'
# LES[LES$sponsor == "zzzz",]$sponsor <- 'zzzz'


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
  scale_color_manual(values=c("dodgerblue2", "red2", "gray50"))

##### CHECK OUTLIERS
## -- Slade = SM Error (they have as R, he was always a D)
## -- Holt = SM Error (they have as D, he was always a R)
## -- Neall Switched R to D in November 1999 but SM only have D record: https://www.baltimoresun.com/news/bs-xpm-1999-11-13-9911130267-story.html
# filter(LES, party == 'd' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party, SM_name)
# filter(LES, party == 'r' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party, SM_name)


### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')
