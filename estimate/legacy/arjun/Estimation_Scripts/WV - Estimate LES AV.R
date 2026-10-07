################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** WEST VIRGINIA *** BY SESSION
##############################################################

###################################
## (SPECIAL) SESSIONS:
## ---- Special Session bills = HB0101, HB0201, where first two numbers denote special session number -- Not as clear in Senate
## ---- If not killed, Bills Carryover from first to second regular session BY REQUEST OF THE SPONSOR (they end up with same bill number, new set of actions)
## --------> "Any bill or joint resolution pending in the House at the adjournment of the First Regular Session of 
## the Legislature or Extended First Regular Session, which has not been rejected, tabled, or postponed indefinitely, 
## shall carry over as it was introduced to the Second Regular Session at the request of the sponsor or cosponsors 
## of the bill or resolution. The request must be made to the Clerk of the House not later than ten days before
## the commencement of the Second Regular Session." (process doc 1)
## -------> In Senate: Bill Numbers aren't consistent on carryover...
## MEMBER LISTS:
## ---- 
## PROCESS/RULES:
## ---- (1) http://www.wvlegislature.gov/educational/citizens/process.cfm#process14
## ---- Note: 1st readig occurs AFTER bill reported out of committee
## Sponsorship/Authorship
## ---- 
###########################
## NOTES:
## (1) Do we need to adapt for carryover rules?  Not doing so at present --> Requires sponsor request + Difficult to track in Senate + BIlls appear to begin fresh in new session 
# ----> With how the data is organized it is kind of like double coding; However, given that everyone can do this, should balance out...
## (2) West Virginia Eliminated Multi-Member districts in 2018
## (3) Can get the West Virginia Blue Book back in time; also includes municipal info (e.g., police chief!)
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
library(tibble)
library(foreach)
library(inexact)

this_state <- 'WV'
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
# t <- terms[1]



### Formulate 2-year terms -- Cover both regular and special sessions
#t_sessions <- sessions[grepl(glue('{t}|{t+1}'), sessions)]

### Skip Previously Estimated
if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
  print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
  next
}

###### SESSION IN PROGRESS
print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t}.csv")
bills <- read.csv(bill_path)
bills <- arrange(bills, bill_number)

bill_path2 <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t + 1}.csv")
bills2 <- read.csv(bill_path2)
bills2 <- arrange(bills2, bill_number)

bills <- bind_rows(bills, bills2)
rm(bills2, bill_path2)

### Clean Term/Session Variables
bills$term <- t_yrs
bills$session_type <- recode(bills$session_type, '1x' = 'SS1', '2x' = 'SS2', '3x' = 'SS3', '4x' = 'SS4', '5x' = 'SS5', '6x' = 'SS6', '7x' = 'SS7')
bills$session <- paste0(bills$session_year, "-", bills$session_type)

### Drop duplicates -- In first year, at least, bills duplicated
bills <- distinct(bills) 

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_number)

############### Drop Resolutions, Messages, Communications, Reports
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
table(bills$bill_type)
all_bills <- bills
bills <- filter(bills, bill_type %in% keep_types) 

##########################
####### Standardize Sponsors

bills$primary_sponsor <- tolower(bills$primary_sponsor)
bills$primary_sponsor <- gsub('á', 'a', bills$primary_sponsor)
bills$primary_sponsor <- gsub('é', 'e', bills$primary_sponsor)
bills$primary_sponsor <- gsub('ó', 'o', bills$primary_sponsor)
bills$primary_sponsor <- gsub('í', 'i', bills$primary_sponsor)
bills$primary_sponsor <- gsub('ñ', 'n', bills$primary_sponsor)

bills$cosponsors <- tolower(bills$cosponsors)
bills$cosponsors <- gsub('á', 'a', bills$cosponsors)
bills$cosponsors <- gsub('é', 'e', bills$cosponsors)
bills$cosponsors <- gsub('ó', 'o', bills$cosponsors)
bills$cosponsors <- gsub('í', 'i', bills$cosponsors)
bills$cosponsors <- gsub('ñ', 'n', bills$cosponsors)

### Adjust Richard THompson (2007-2008, when speaker)
if(t_yrs == "2007_2008"){
  ### Two Thompsons, 1 Speaker, so cleaning script below removes unique identifier
  # ---> Actually Ron THompson never sponsors or cosponsors.. Resigns in mid-2007 due to health
  bills$primary_sponsor <- gsub("mr. speaker \\(mr. thompson\\)", 'r. thompson', bills$primary_sponsor)
  bills$cosponsors <- gsub("mr. speaker \\(mr. thompson\\)", 'r. thompson', bills$cosponsors)
  #bills$primary_sponsor <- ifelse(bills$primary_sponsor == "thompson", 'r. m. thompson', bills$primary_sponsor)
  #bills$cosponsors <- gsub("^thompson", 'r. m. thompson', bills$cosponsors)
  #bills$cosponsors <- gsub("; thompson", '; r. m. thompson', bills$cosponsors)
}

### Drop Honorifics
titles <- "\\((mr|ms|mrs). speaker\\)|\\((mr|ms|mrs). president\\)|^(mr|ms|mrs). speaker \\((mr|ms|mrs).|^(mr|ms|mrs). president \\((mr|ms|mrs).|\\)$|\\(acting president\\)|mr\\. speaker \\(mr\\. "
bills$primary_sponsor <- str_trim(gsub(titles, '', bills$primary_sponsor))
bills$cosponsors <- str_trim(gsub(titles, '', bills$cosponsors))
bills$cosponsors <- gsub(" \\);|\\);|\\)$", ';', bills$cosponsors)

### Manual Fixes for Wrong Sponsors
if(t_yrs == "1993_1994"){
  bills[bills$bill_id %in% c("SB0100", "SB0101", "SB0102") & bills$session == "1993-SS1",]$primary_sponsor <- "burdette"
  bills[bills$bill_id %in% c("SB0100", "SB0101", "SB0102") & bills$session == "1993-SS1",]$cosponsors <- "boley" # Burk and Chambers = Cospon on House Companion
  ## David Miller Must have been appointed to Senate pre 1994-RS... See:http://wvutoday-archive.wvu.edu/n/2007/05/30/5778.html
  ## Assuming remaining House bills with 'miller' are margaret 'peggy' miller
  bills[bills$bill_id %in% c("HB4358", "HB4415", "HB4506") & bills$session == "1994-RS",]$primary_sponsor <- "m. miller"
  bills[substring(bills$bill_id,1,1) == 'H' & bills$session == "1994-RS" & grepl('^miller|; miller', bills$cosponsors),]$cosponsors <- gsub('miller', 'm. miller', bills[substring(bills$bill_id,1,1) == 'H' & bills$session == "1994-RS" & grepl('^miller|; miller', bills$cosponsors),]$cosponsors)
  bills[substring(bills$bill_id,1,1) == 'S' & bills$primary_sponsor == "miller",]$primary_sponsor <- 'd. miller'
  bills[substring(bills$bill_id,1,1) == 'S' & grepl('^miller|; miller', bills$cosponsors),]$cosponsors <- gsub("miller", 'd. miller', bills[substring(bills$bill_id,1,1) == 'S' & grepl('^miller|; miller', bills$cosponsors),]$cosponsors)
  ### Larry WIlliams appointed; Steve WIlliams recoded as s. williams
  bills[bills$primary_sponsor == "williams",]$primary_sponsor <- 's. williams'
  bills[grepl("^williams|; williams",bills$cosponsors),]$cosponsors <- gsub('williams', 's. williams', bills[grepl("^williams|; williams",bills$cosponsors),]$cosponsors)
}else if(t_yrs == '1997_1998'){
  ### James Rowe appointed as judge in 1997
  bills[bills$primary_sponsor == "rowe",]$primary_sponsor <- 'l. rowe'
  bills$cosponsors <- gsub('rowe', 'l. rowe', bills$cosponsors)
}else if(t_yrs == '2001_2002'){
  ### D. Martin and Martin in data --> Martin == J.E. Martin
  bills[bills$primary_sponsor == "martin",]$primary_sponsor <- 'j. martin'
  bills$cosponsors <- gsub('^martin', 'j. martin', bills$cosponsors)
  bills$cosponsors <- gsub('; martin', '; j. martin', bills$cosponsors)
}else if(t_yrs == "2009_2010"){
  ## First Initials Not Included for Cosposnorship --> Faux Duplicates
  bills[grep("^facemire|; facemire", bills$cosponsors),]$cosponsors <- gsub('facemire', 'd. facemire', bills[grep("^facemire|; facemire", bills$cosponsors),]$cosponsors)
  bills[grep("^facemyer|; facemyer", bills$cosponsors),]$cosponsors <- gsub('facemyer', 'k. facemyer', bills[grep("^facemyer|; facemyer", bills$cosponsors),]$cosponsors)
  bills[bills$primary_sponsor == "facemyer",]$primary_sponsor <- "k. facemyer"
  ### Terry Walker appointed mid-cycle
  bills[bills$primary_sponsor == "walker",]$primary_sponsor <- "walker, d."
  bills$cosponsors <- gsub('^walker$', 'walker, d.', bills$cosponsors)
  bills$cosponsors <- gsub('^walker;', 'walker, d.;', bills$cosponsors)
  bills$cosponsors <- gsub('; walker;', '; walker, d.;', bills$cosponsors)
  bills$cosponsors <- gsub('; walker$', '; walker, d.', bills$cosponsors)
}else if(t_yrs == "2011_2012"){
  ## Shott and Schoen not actually in House; must have been and old bill; name accidentally included on web; not on introduced versions
  bills[bills$bill_id == "HB4502",]$cosponsors <- "williams; perdue; shaver; perry; phillips, r.; ferro; hall"
  bills[bills$bill_id == "HB2410" & bills$session == "2011-RS",]$cosponsors <- "duke; sobonya"
  ### Only One Delegate Walker
  bills[bills$primary_sponsor == "walker",]$primary_sponsor <- "walker, d."
  bills$cosponsors <- gsub('^walker$', 'walker, d.', bills$cosponsors)
  bills$cosponsors <- gsub('^walker;', 'walker, d.;', bills$cosponsors)
  bills$cosponsors <- gsub('; walker;', '; walker, d.;', bills$cosponsors)
  bills$cosponsors <- gsub('; walker$', '; walker, d.', bills$cosponsors)
}else if(t_yrs == "2013_2014"){
  ## Nelson here = Eric Nelson: http://www.wvlegislature.gov/Bill_Status/Bills_history.cfm?input=2350&year=2013&sessiontype=RS&btype=bill
  bills[bills$bill_id == "HB2350" & bills$session == "2013-RS",]$primary_sponsor <- "nelson, e." 
  ## Evans == "A. Evans" -- http://www.wvlegislature.gov/Bill_Status/bills_text.cfm?billdoc=hb2334%20intr.htm&yr=2013&sesstype=RS&i=2334
  bills[bills$bill_id == "HB2102" & bills$session == "2013-RS",]$primary_sponsor <- "evans, a." 
  bills[bills$bill_id == "HB2334" & bills$session == "2013-RS",]$cosponsors <- "evans, a.; poling, d." 
  bills[bills$bill_id == "HB2334" & bills$session == "2014-RS",]$cosponsors <- "evans, a." 
  ## For House: Miller == Miller, C. 
  bills[bills$primary_sponsor == "miller" & substring(bills$bill_id, 1, 1) == "H",]$primary_sponsor <- "miller, c." 
  bills[substring(bills$bill_id,1,1) == "H",]$cosponsors <- gsub('^miller$', 'miller, c.', bills[substring(bills$bill_id,1,1) == "H",]$cosponsors)
  bills[substring(bills$bill_id,1,1) == "H",]$cosponsors <- gsub('^miller;', 'miller, c.;', bills[substring(bills$bill_id,1,1) == "H",]$cosponsors)
  bills[substring(bills$bill_id,1,1) == "H",]$cosponsors <- gsub('; miller;', '; miller, c.;', bills[substring(bills$bill_id,1,1) == "H",]$cosponsors)
  bills[substring(bills$bill_id,1,1) == "H",]$cosponsors <- gsub('; miller$', '; miller, c.', bills[substring(bills$bill_id,1,1) == "H",]$cosponsors)
}else if(t_yrs == "2015_2016"){
  ### Linda Goode Phillips leaves office in 2015
  bills[bills$primary_sponsor == "phillips" & bills$session_year == 2016,]$primary_sponsor <- "phillips, r."
  bills[grepl('^phillips;|^phillips$|; phillips;|; phillips$', bills$cosponsors),]$cosponsors <- gsub('phillips', 'phillips, r.', bills[grepl('^phillips;|^phillips$|; phillips;|; phillips$', bills$cosponsors),]$cosponsors)
  ### Daniel Hall leaves office in 2015
  bills[bills$primary_sponsor == "hall" & bills$session_year == 2016,]$primary_sponsor <- "m. hall"
  bills[grepl('^hall;|^hall$', bills$cosponsors),]$cosponsors <- gsub('^hall', 'm. hall', bills[grepl('^hall;|^hall$', bills$cosponsors),]$cosponsors)
  bills[grepl('; hall;|; hall$', bills$cosponsors),]$cosponsors <- gsub('hall', 'm. hall', bills[grepl('; hall;|; hall$', bills$cosponsors),]$cosponsors)
}else if(t_yrs == "2017_2018"){
  ### Nancy (Reagan) FOster resigned Sep 1, 2017 -- https://www.wvgazettemail.com/news/politics/nancy-foster-resigns-from-wv-house/article_43ab4b7b-a067-5382-9e42-f71be5d3458e.html
  bills[bills$session_year == 2018 & bills$primary_sponsor == "foster",]$primary_sponsor <- 'foster, n.'
  bills[bills$session_year == 2018,]$cosponsors <- gsub('foster', 'foster, n.', bills[bills$session_year == 2018,]$cosponsors)
} else if(t_yrs == "2021_2022"){
  # jeffrey pack leaves office in 2021, so the remaining one is larry
bills[bills$session_year == 2022 & bills$primary_sponsor == "pack",]$primary_sponsor <- 'pack, l.' 
  bills[bills$session_year == 2022,]$cosponsors <- gsub('pack', 'pack, l.', bills[bills$session_year == 2022,]$cosponsors)
  bills[bills$session_year == 2022,]$cosponsors <- gsub('pack, l., l.', 'pack, l.', bills[bills$session_year == 2022,]$cosponsors)
  # joe jeffries resigns in 2022
  bills[bills$session_year == 2022 & bills$primary_sponsor == "jeffries" & bills$bill_type == "HB",]$primary_sponsor <- 'jeffries, d.' 
  bills[bills$session_year == 2022 & bills$bill_type == "HB",]$cosponsors <- gsub('jeffries', 'jeffries, d.', bills[bills$session_year == 2022 & bills$bill_type == "HB",]$cosponsors)
  bills[bills$session_year == 2022 & bills$bill_type == "HB",]$cosponsors <- gsub('jeffries, d., d.', 'jeffries, d.', bills[bills$session_year == 2022 & bills$bill_type == "HB",]$cosponsors)
  

  
  
}

### LES Sponsor Var
bills <- rename(bills, LES_sponsor = primary_sponsor)
table(bills$LES_sponsor)

if(t_yrs == "2019_2020"){
  bills$LES_sponsor = gsub(' \\(by request',"",bills$LES_sponsor) # checked with Peter, ok to do this year
}

#### Adjusting for Bills By Request -- Need to do BEFORE splitting
if(any(grepl("request", bills$LES_sponsor))){
  print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
}


###################
###### Merge in S&S Bills
###################
# *** For WEST VIRGINIA: Bills carry over during regular BY REQUEST OF SPONSOR (one biennium)
# ---> In House, numbers usually the same, in senate, more variable...


if(t_yrs == "2019_2020"){
  SS_bills$bill_id[SS_bills$bill_id=="SB10001"]="SB0001"
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
  left_join(all_bills %>% select(bill_id,term,primary_sponsor), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",primary_sponsor, ignore.case=T)) %>%
  arrange(primary_sponsor) 
unique(missing_SS_bills$bill_id) 


# now check to see if there are duplicate joins


duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills , 
            by = c("bill_id", "term", "year" = "session_year")) %>%
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
  bills2 <- bills %>% 
    left_join(SS_term %>% select(-Title) %>% distinct(), by = c("bill_id", "term","session_year" = "year")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  SS_term2 = SS_term %>% 
    left_join(bills  %>% select(bill_id,term,session, session_year),by=c("bill_id","term","year" = "session_year"))
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

###################################################
############### Code Commemorative
###################################################

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

###################################################
############### Code Bill History
###################################################

bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t}.csv")
bill_hist <- read.csv(bill_hist_path)

bill_hist_path2 <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t+1}.csv")
bill_hist2 <- read.csv(bill_hist_path2)

bill_hist <- bind_rows(bill_hist, bill_hist2)
rm(bill_hist2, bill_hist_path2)

######## Standardize BillHist Bill IDs
bill_hist <- rename(bill_hist, bill_id = bill_number) 

### Clean Term/Session Variables
bill_hist$term <- t_yrs
bill_hist$session_type <- recode(bill_hist$session_type, '1x' = 'SS1', '2x' = 'SS2', '3x' = 'SS3', '4x' = 'SS4', '5x' = 'SS5', '6x' = 'SS6', '7x' = 'SS7')
bill_hist$session <- paste0(bill_hist$session_year, "-", bill_hist$session_type)

### Drop duplicates -- In first year, at least, bills duplicated
bill_hist <- distinct(bill_hist) 

### Order by Order 
bill_hist <- arrange(bill_hist, term, session, bill_id, order) 

### For 1993-SS1 --- ORDER Variable is WRONG 
# --- This still won't be perfect (for some bills actions are missing, others too many), but should be close
if(t_yrs == "1993_1994"){
  regbills <- filter(bill_hist, session_type == "RS")
  ssbills <- filter(bill_hist, session_type == "SS1") %>% 
    arrange(term, session, bill_id, action_date) %>%
    group_by(bill_id) %>%
    mutate(order = 1:n()) %>%
    ungroup()
  
  bill_hist <- bind_rows(regbills, ssbills) %>% arrange(term, session, bill_id, order)
  rm(regbills, ssbills)
}

### Re-Coding Chamber Variable
#bill_hist$chamber <- ifelse(bill_hist$chamber == 'O', 'G', bill_hist$chamber)
bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate", "G" = "Governor", "CC" = "Conference")

### Identifying Committees
#bill_hist$action <- ifelse(grepl('^[A-Z][A-Z]+ - ', bill_hist$action), paste0('Committee_', bill_hist$action), bill_hist$action)

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c('^reported (do|be|with|in)', 'do pass', '^reported be adopted', 'committee.+reported', 'committee amendment', '^originating in')
### Occassionally has 'Reported by the clerk' = Floor Action
### ALso: Reported in comm sub.. COding as AIC/ABC as bill partially progresses
### In H: But (1) reported not always included for comm. rec & (2) when it is, do pass nearly always follows it (exception = be adopted)
### In S: Often uses "Committee amendment reported" or "Committee subsitute reported"
### Committee amendment implies committee action
### Bills can originate in committee in WV: Implies written in comm (AIC); also does not need to then be referred (thats implied, i suppose)
abc_t <- c('^reported', '(1st|2nd|3rd|first|second|third) reading', 'read (1st|2nd|3rd)',
           'house calendar', 'special calendar','^effective', 'roll no\\. [0-9]+', 'voice vote',
           'suspension of', 'unanimous consent')
# ^Effective = Effective [date]/Effective from passage = vote on effective date
pc_t <- c('^passed', 'communicated to (house|senate)', 'ordered to (house|senate)',
          'to governor', 'completed legislative action')
law_t <- c('approved by governor', 'chapter [0-9]+', 'acts 19[0-9]+', 'acts 20[0-9]+')
### Acts [Year] only used in earlier sessions. In 2018 its: "Acts, [Session Type], [Year]

### Check Actions
# filter(bill_hist, grepl('^committee', tolower(action))) %>% distinct(action) %>% View()
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
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
  bill_stages$bill_url <- bills[i,]$bill_url
  ### Check NOT ACTUALLY LAW - http://www.wvlegislature.gov/Bill_Status/Bills_history.cfm?input=4040&year=2016&sessiontype=RS&btype=bill
  if(bill_stages$law == 1 & any(grepl("clerk\\'s note.+bill is null and void", tolower(hist_sub$action))) ){
    bill_stages$law <- 0
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
            sponsor_law_rate = sum(law) / n()) %>%
  ungroup()

#### Adding Legislators Who COSPONSORED A BILL but did not SPONSOR
unique_cospon <- str_trim(unique(unlist(str_split(bills$cosponsors, '; '))))
for(nonspon in unique_cospon){
  if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != ''){
    chamb <- unique(substring(bills[grepl(nonspon, bills$cosponsors),]$bill_id, 1, 1))
    if("H" %in% chamb & "S" %in% chamb){
      print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
    }else{
      all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0) 
    }
  }
}

######## Cosponsorship Info --- For OH: Only have cosponsor info for most recent years
all_sponsors$num_cosponsored_bills <- NA
bills$cospon_match <- paste(bills$LES_sponsor, bills$primary_sponsor, bills$cosponsors, sep = '; ')
for(i in 1:nrow(all_sponsors)){
  c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
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
all_sponsors$last_name <- gsub(',.+|^[a-z]\\. | [a-z]\\.$', '', all_sponsors$LES_sponsor)
all_sponsors$first_name <- ifelse(grepl('^[a-z]\\. ', all_sponsors$LES_sponsor), gsub('\\..+', '', all_sponsors$LES_sponsor),
                                  ifelse(grepl(", [a-z]\\.$|, [a-z]$", all_sponsors$LES_sponsor), gsub('.+, |\\.$', '', all_sponsors$LES_sponsor), 
                                         ifelse(grepl(" [a-z]\\.$", all_sponsors$LES_sponsor), gsub('.+ |\\.', '', all_sponsors$LES_sponsor), "")))
all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

#### Update First/Last Names for Matching 
if(t_yrs %in% c("2001_2002", "2003_2004", "2005_2006")){
  all_sponsors[all_sponsors$LES_sponsor == 'r. m. thompson',]$first_name <-  "ron"
  all_sponsors[all_sponsors$LES_sponsor == 'r. m. thompson',]$last_name <-  "thompson"
  all_sponsors[all_sponsors$LES_sponsor == 'r. thompson',]$first_name <-  "richard"
  # L. Gil White
  all_sponsors[all_sponsors$LES_sponsor == "g. white",]$first_name <- "l"
}
if(t_yrs == "2007_2008"){
  all_sponsors[all_sponsors$LES_sponsor == 'r. thompson',]$first_name <-  "richard"
}
if(t_yrs == "2017_2018"){
  all_sponsors[all_sponsors$LES_sponsor == 'sypolt' & all_sponsors$chamber == "H",]$last_name <-  "funksypolt"
  all_sponsors[all_sponsors$LES_sponsor == 'foster, n.',]$last_name <-  "reaganfoster"
}






all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>% 
  mutate(match_name_chamber = tolower(paste(str_remove_all(LES_sponsor, '"\\s*.*?\\s*"'),substr(chamber,1,1),sep="-"))) %>% 
  select(-c(last_name,first_name))

legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(t)) | startsWith(legiscan_sessions,as.character(t+1))]

legiscan = foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% select(-c(votesmart_id,knowwho_pid, ballotpedia,nickname,suffix, middle_name)) %>%  distinct() 


if(t_yrs == "2021_2022") {
  legiscan = legiscan %>% filter(!(people_id %in% c(11797,15837,19382, 19) & role == "Rep"))

}

legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name, substr(district,1,1)) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = ifelse(n >= 2, glue("{last_name}, {substr(first_name,1,1)}."), last_name)) %>%
  mutate(match_name_chamber = tolower(paste(match_name,substr(district,1,1),sep="-")))

# inexact::inexact_addin()
# left: legiscan_adj
# right: all_sponsors
# method: osa
# mode: full

if(t_yrs == "2019_2020"){
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
        "anderson, a.-h" = NA_character_,
        "cannon-h" = NA_character_
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
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES, commem_bills)
rm(t, terms, klarner_gs, m_sub, nonspon, unique_cospon, c_sub, chamb, titles, yr) 

########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by APPOINTMENT BY GOVERNOR --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
### Directories 2004 - 2019: http://www.wvlegislature.gov/Educational/publications.cfm
### All Female State Legislators (1922-2009): http://www.wvlegislature.gov/educational/publications/legis_women.pdf
### Archive Election Results: https://sos.wv.gov/elections/Pages/HistElecResults.aspx
### Correct Waybackmachine URL: https://web.archive.org/web/20041217135349/http://www.legis.state.wv.us/
#########################

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1993_1994 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 31 bill(s) without a sponsor
### APPOINTED ~ HOUSE:
# -- fantasia
# -- frederick
# -- nichols -- http://www.hurherald.com/obits.php?id=4528
# -- yeager (emily)
# -- l. williams (name duplicate, won't print)
### APPOINTED ~ SENATE
# -- miller = d. miller (david)
# -- schoonover -- http://www.hurherald.com/obits.php?id=4528
### IN HOUSE:
# -- anderson, e. w. (bill) jr.
# -- whitley, ebb -- resigned early in term (unclear when): http://www.wvlegislature.gov/educational/publications/legis_women.pdf
# -- heston, michael a. -- no records he wasn't in the chamber; keeping for now


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1996 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 29 bill(s) without a sponsor
### APPPOINTED ~ HOUSE:
# -- kerns -- no record in klarner; appointed to House, then Senate half a year later: http://www.wvlegislature.gov/legisdocs/2016/BlueBook/0337_WVS_BlueBook.pdf
# -- kime -- lost 1996
# -- stewart -- gloria (d) -- appointed Feb 1 1996: http://www.wvlegislature.gov/educational/publications/legis_women.pdf
### APPOINTED ~ SENATE:
# -- love = (mr) shirley love, appointed 1994 -- http://politicalgraveyard.com/bio/love.html#018.64.96
# -- miller = d. miller (david)
### Drop:
# -- burk, robert w. (bob) jr. -- died in office, 1994: http://politicalgraveyard.com/bio/burgett-burkan.html#490.85.01
# -- holliday, robert k. (bob) -- resigned 1994: http://politicalgraveyard.com/bio/holliday.html#485.04.19
# -- felton, charles b. jr. -- resigned 1993: http://politicalgraveyard.com/bio/fellrath-femille.html#446.74.02

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 19 bill(s) without a sponsor
### APPPOINTED ~ HOUSE:
# -- KOMINAR -- Appointed Dev 6, 1996: http://www.wvlegislature.gov/educational/publications/Manual_PDF/29-Biographies_House.pdf
# -- SMITH -- seems like this is joe smith -- https://en.wikipedia.org/wiki/Joe_F._Smith
# -- WILLIS -- carroll (presumably)
### APPOINTED ~ SENATE:
# -- KESSLER (jeff) --- Nov 1997 - http://www.wvlegislature.gov/Educational/publications/Manual_PDF/38-Biographies_Senate.pdf
### DROP:
# -- preece, grant -- resigend pre Dec 6, 1996 -- http://www.wvlegislature.gov/educational/publications/Manual_PDF/29-Biographies_House.pdf
# -- rowe, james j. -- appointed as judge in 1997 -- https://ballotpedia.org/James_J._Rowe


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 43 bill(s) without a sponsor
### APPOINTED ~ HOUSE:
# -- CALVERT (ann)
# -- MATTALIANO (joe, D) -- https://web.archive.org/web/19990422083722/http://www.legis.state.wv.us/
# -- PAXTON (brady)
### APPOINTED ~ SENATE:
# -- DAWSON (james, D) -- https://web.archive.org/web/19990422083722/http://www.legis.state.wv.us/
# -- KESSLER (jeff, T - 1) 
### DROP:
# -- wiedebusch, robert larry -- Died Nov 1997 --- http://www.wvlegislature.gov/Educational/publications/Manual_PDF/38-Biographies_Senate.pdf

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 8 bill(s) without a sponsor
### APPOINTED ~ HOUSE:
# -- ELLEM (john)
# -- FOX (james r.) -- https://web.archive.org/web/20021004054648/http://129.71.164.29/members/capmail.cfm
# -- MORGAN (James H) -- https://ballotpedia.org/James_Morgan
### DROP:
# -- schoonover, randy -- Resigned and then convicted: https://www.apnews.com/27635d0c854b3578ac8218cc3ed16993
# -- modesitt, rick -- resigned 2000 (became county commissioner) -- https://votesmart.org/candidate/biography/26459/rick-modesitt and https://rickmodesitt.com/about/
# -- johnson, arley r. -- served 6 years = resigned in 2000: http://www.wvculture.org/wv150/JohnsonBIO.pdf
### Name Fix
# -- Two R. Thompsons: RON = R. M. Thompson: http://www.wvlegislature.gov/legisdocs/publications/info/INFOPACKET_2005.pdf


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 4 bill(s) without a sponsor
### Klarner Wrong: 
# -- MAHAN -- this makes no sense, but she appears to have won with fewer votes...? Numbers must be off: https://sos.wv.gov/elections/Documents/HistElecDocs/2002/2002%20House%20of%20Delegates%20Gen.pdf
# -----> But this suggests she won as well: http://www.wvlegislature.gov/educational/publications/legis_women.pdf
### Drop:
# -- mcgraw, warren r. ii -- per above, he lost...


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1 bill(s) without a sponsor
### KLARNER ERROR:
# -- MAHAN -- same as above
### APPOINTED ~ SENATE:
# -- lanham -- CHARLES
### DROP:
# -- wooton, john d. -- Election results say he won but same district as mahan, so she must have filled his seat: https://sos.wv.gov/elections/Documents/HistElecDocs/2004/2004%20House%20of%20Delegates%20Gen.pdf
# -- smith, lisa d. -- Resigned Dec 2004: http://www.wvlegislature.gov/joint/PubInfo/NewsArticles/2004/files/Lisa%20Smith%20steps%20down.htm


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1 bill(s) without a sponsor
### APPOINTED ~ HOUSE:
# -- GALL
# -- HIGGINS (DAVE)
# -- POLING (dan, name duplicate, won't print)
### IN HOUSE:
# -- thompson, ron -- resigned mid-2007 (health), sponsors no bills: https://en.wikipedia.org/wiki/Ron_Thompson_(West_Virginia_politician)
### DROP:
# -- beane, j. d. -- appointed to be judge, Dec 2006: https://ballotpedia.org/J.D._Beane


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- POORE 
# -- ROSS (mike) -- must not have run again, but in HOUSE, per Wayback Machine
### Drop:
# -- proudfoot, bill -- died in Dec 2008, https://www.timeswv.com/news/w-va-legislator-killed-in-crash-on-icy-road/article_7467ba5a-6914-59dc-be57-de4796f180ea.html


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
### KLARNER ERROR:
# -- MAHAN -- Definitely won, but again listed as LOSS
### APPOINTED ~ HOUSE:
# -- DISERIO
# -- MARCUM (justin) -- http://www.wvlegislature.gov/legisdocs/publications/info/membership_directory_2012.pdf
### APPOINTED ~ SENATE:
# -- KIRKENDOLL (art) -- Nov 2011
### DROP
# -- wooton, william r. -- didn't win; lost to mahan
# -- tomblin, earl ray -- elected to be governor in 2011 (was acting gov in 2010)
# -- caruth, donald t. -- died May 2010

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- BARKER (josh)
# -- FRAGALE (ron, lost in new district, then appointed)
# -- KINSEY (tim)
### APPOINTED ~ SENATE:
# -- CANN -- Appointed Jan 16, 2013
# -- COOKMAN (donald)
# -- FITZSIMMONS (rocky)
### DROP:
# -- cann, samuel j. (sam) -- IN HOUSE: appointed to S, Jan 16 2013
# -- klempa, orphy -- served through 2012 (left to become country commissioner?)
# -- minard, joe -- resigned January 2013
# -- helmick, walt -- elected commissioner of agriculture in 2012

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
### APPOINTED ~ HOUSE:
# -- ATKINSON (martin 'rick')
# -- BLACKWELL (frank)
# -- FLANIGAN (bill)
# -- SHAFFER (steve)
# -- WHITE, P. (phyllis, last name duplicate x2, won't show)
### APPOINTED ~ SENATE:
# -- ASHLEY (bob, via H)
# -- BOSO (greg)
# -- CLINE (sue)
### DROP:
# -- barnes, clark s. -- is Clerk of the Senate in 2015 after being Senator in 2013-2014. Odd.


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 2 bill(s) without a sponsor
### APPOINTED ~ HOUSE:
# -- ADKINS (chanda) 
# -- CAMPBELL (jeff)
# -- GRAVES (diana)
# -- JENNINGS (D. Roland)
# -- PACK (jeffrey)
### APPOINTED ~ SENATE:
# -- ARVON (lynne carden, via H)
# -- BALDWIN (stephen, via H)
# -- CLEMENTS (charles, past H)
# -- DRENNAN (mark)
### DROP:
# -- leonhardt, kent -- elected 2014, not in chamber in 2017
# -- nohe, david clay -- elected 2014, not in chamber in 2017
### Other Notes:
# Nancy (Reagan) FOster resigned Sep 1, 2017 -- https://www.wvgazettemail.com/news/politics/nancy-foster-resigns-from-wv-house/article_43ab4b7b-a067-5382-9e42-f71be5d3458e.html


# filter(klarner, grepl("marshallwilson", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid, party) %>% arrange(year, cand) %>% as.data.frame()
# filter(klarner, ddez == 48 & sen == 0 & year == 2012) %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz, vote)


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
LES[LES$data_name %in% "lanham" & LES$term == "2005_2006", c("klarner_id", "klarner_name")] <- NA
LES[LES$data_name %in% "lanham" & LES$term == "2005_2006",]$sponsor <- "lanham, charles"
LES[LES$data_name %in% "higgins" & LES$term == "2007_2008", c("klarner_id", "klarner_name")] <- NA
LES[LES$data_name %in% "higgins" & LES$term == "2007_2008",]$sponsor <- "higgins, david"
LES[LES$data_name %in% "white, p." & LES$term == "2015_2016", c("klarner_id", "klarner_name")] <- NA
LES[LES$data_name %in% "white, p." & LES$term == "2015_2016",]$sponsor <- "white, phyllis"

### *** WON'T BE NECESSARY WITH NEW KLARNER DATA ****
LES[LES$data_name %in% "adkins" & LES$term == "2017_2018", c("klarner_id", "klarner_name")] <- NA
LES[LES$data_name %in% "adkins" & LES$term == "2017_2018",]$sponsor <- "adkins, chanda"
### *** Also: 2017_2018 Campbell == Jeff Campbell

### ****Still missing***** 
## -- Kerns == Edward Kerns; appointed to House, then Senate ~ 6 months -- http://www.wvlegislature.gov/legisdocs/2016/BlueBook/0337_WVS_BlueBook.pdf
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[19]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, still_missing)

### Fix Missing
name_matches <- data.frame(LES_name = 'miller', k_name = 'miller, david e.')
name_matches <- add_row(name_matches, LES_name = 'love', k_name = 'love, mr. shirley')
name_matches <- add_row(name_matches, LES_name = 'smith', k_name = 'smith, joe')
name_matches <- add_row(name_matches, LES_name = 'kessler', k_name = 'kessler, jeffrey v.')
name_matches <- add_row(name_matches, LES_name = 'cookman', k_name = 'cookman, donald h.')
name_matches <- add_row(name_matches, LES_name = 'shaffer', k_name = 'shaffer, steven l.')
name_matches <- add_row(name_matches, LES_name = 'ashley', k_name = 'ashley, bob')
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
rm(check_dup, k_sub, exact, name_sub, missing, t)


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
# filter(LES, is.na(party)) %>% select(sponsor, term, chamber, klarner_id, district, party, exper)
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

### Not In Klarner
### * Kerns: No records.. but SM have him as a Dem and he was replaced by a Democrat..
fill_missing <- data.frame(LES_name = "kerns", new_name = 'kerns, edward', party = 'd', district = NA, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "mattaliano", new_name = 'mattaliano, joseph p.', party = 'd', district = 40, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "dawson", new_name = 'dawson, james', party = 'd', district = 11, exper = 'none')
### * James Fox: Can't find party, but SM have him as Dem
fill_missing <- add_row(fill_missing, LES_name = "fox", new_name = 'fox, james r.', party = 'd', district = 37, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "lanham, charles", new_name = 'lanham, charles c.', party = 'r', district = 4, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "higgins, david", new_name = 'higgins, david', party = 'd', district = 30, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "white, phyllis", new_name = 'white, phyllis', party = 'd', district = 21, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'none')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

#### *** 2017_2018: If any of below run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "campbell",]$party <- 'd'
LES[LES$sponsor == "campbell",]$sponsor <- 'campbell, jeff'
LES[LES$sponsor == "jennings",]$party <- 'r'
LES[LES$sponsor == "jennings",]$sponsor <- 'jennings, d. rolland'
LES[LES$sponsor == "graves",]$party <- 'r'
LES[LES$sponsor == "graves",]$sponsor <- 'graves, dianna'
LES[LES$sponsor == "adkins, chanda",]$party <- 'r'
LES[LES$sponsor == "drennan",]$party <- 'r'
LES[LES$sponsor == "drennan",]$sponsor <- 'drennan, mark a.'

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t, fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

### Drop Nicknames
# filter(LES, grepl('\\(|\\"', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
LES$sponsor <- str_trim(gsub(' \\([^\\)]+\\)', '', LES$sponsor))

### Drop Numerics --- None of 3 have match that will lead to collapsed individuals
# filter(LES, grepl('[0-9]', sponsor)) %>% distinct(sponsor, data_name)
LES$sponsor <- str_trim(gsub('[0-9]$', '', LES$sponsor))

### Manually Fix Names
LES[LES$sponsor == 'love, mr. shirley',]$sponsor <- 'love, shirley'
LES$sponsor <- gsub(' mrs\\.', '', LES$sponsor)


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

### IF Necessary: DROP SM DUPLICATES
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
LES[LES$sponsor %in% c('adkins, chanda', 'beach, robert c.', 'love, sam', 'miller, rodney a.', 'funksypolt, terri'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches:
# -- William Thomas 'Tom' Louisos
# -- Robert John Doyle
# -- Constantino "Jon" Amores Jr.
# -- George 'Isaac' Sponaugle
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

#### FIX MISMATCHES
LES[LES$sponsor %in% c('stewart, william'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1991_1992', '1993_1994', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('orc', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

### Most of these have 2015-2016 Duplicates but with slight name variation (e.g., Middle initial with period or no initial)
# ---> Always picking the one with most years
name_matches <- data.frame(LES_name = 'anderson, e. w. jr.', SM_name = 'Anderson, William')
name_matches <- add_row(name_matches, LES_name = 'ashley, bob', SM_name = 'Ashley, Robert')
### Mike Azinger succeeded father in 2015 --> Not distinct in SM data however; included under "Thomas" as that is his first name also
# name_matches <- add_row(name_matches, LES_name = 'azinger, mike', SM_name = 'zzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'azinger, tom', SM_name = 'Azinger, Thomas')
name_matches <- add_row(name_matches, LES_name = 'beach, robert c.', SM_name = 'Beach')
# name_matches <- add_row(name_matches, LES_name = 'bennett, john f.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'blatnik, thais', SM_name = 'zzzzzzz')
### --> 3 seperate rows: longest tenure is actually LARRY BORDER
name_matches <- add_row(name_matches, LES_name = 'border, anna', SM_name = 'Sheppard, Anna Border')
name_matches <- add_row(name_matches, LES_name = 'border, larry', SM_name = 'Border-Sheppard, Anna')
name_matches <- add_row(name_matches, LES_name = 'cole, bill', SM_name = 'Cole III, William Paul')
name_matches <- add_row(name_matches, LES_name = 'ellis, danny', SM_name = 'Ellis')
### Larry Faircloth != Larry W. Faircloth
name_matches <- add_row(name_matches, LES_name = 'faircloth, larry', SM_name = 'Faircloth, Larry')
name_matches <- add_row(name_matches, LES_name = 'faircloth, larry w.', SM_name = 'Faircloth, Larry W.')
# name_matches <- add_row(name_matches, LES_name = 'grubb, david', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'hall, mike', SM_name = 'Hall, William') ## Time periods align and no michael
name_matches <- add_row(name_matches, LES_name = 'higgins, david', SM_name = 'Higgins, Dave')
#name_matches <- add_row(name_matches, LES_name = 'love, sam', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'manchin, joe iii', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'miller, david e.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'moore, ernest', SM_name = 'zzzzzzz') # Cliff Moore = Son
name_matches <- add_row(name_matches, LES_name = 'nelson, eric jr.', SM_name = 'Nelson, Fredrik Jr.')
name_matches <- add_row(name_matches, LES_name = 'ross, michael', SM_name = 'Ross, Mike')
name_matches <- add_row(name_matches, LES_name = 'rowe, james j.', SM_name = 'Rowe')
name_matches <- add_row(name_matches, LES_name = 'skaff, doug jr.', SM_name = 'Skaff Jr, Doug')
name_matches <- add_row(name_matches, LES_name = 'smith, peggy donaldson', SM_name = 'Smith, Magaret Donaldson')
# name_matches <- add_row(name_matches, LES_name = 'stewart, william', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'tomblin, teddy', SM_name = 'Tomblin, Theodore')
name_matches <- add_row(name_matches, LES_name = 'tomblin, tom', SM_name = 'Tomblin')
name_matches <- add_row(name_matches, LES_name = 'trump, charles s. iv', SM_name = 'Trump IV, Charles S')
name_matches <- add_row(name_matches, LES_name = 'unger, john ii', SM_name = 'Unger II, John')
# name_matches <- add_row(name_matches, LES_name = 'wagner, a. keith', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'walker, martha yeager', SM_name = 'Walker')
# name_matches <- add_row(name_matches, LES_name = 'white, phyllis', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'white, rebecca i.', SM_name = 'White')
# name_matches <- add_row(name_matches, LES_name = 'whitlow, tony e.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

##########
## Split across rows:
##########

LES[LES$sponsor == 'boso, greg',]$SM_name <- ideo[ideo$name == 'Boso, Gregory' & ideo$senate2016 %in% 1,]$name
LES[LES$sponsor == 'boso, greg',]$SM_party <- ideo[ideo$name == 'Boso, Gregory' & ideo$senate2016 %in% 1,]$party
LES[LES$sponsor == 'boso, greg',]$np_score <- ideo[ideo$name == 'Boso, Gregory' & ideo$senate2016 %in% 1,]$np_score


########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########
### Check Party Mismatches
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% arrange(sponsor, term) %>% as.data.frame()
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% arrange(sponsor, term) %>% as.data.frame()

#### Unfixable:
### RON THOMPSON -- Elected as R in 1995-1996, Democrat thereafter -- but do not have SM score for period as Repub.

### Ryan Ferns -- Switch pre 2015 election
LES[LES$sponsor == 'ferns, ryan james' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Ferns, Ryan' & ideo$party == 'D',]$name
LES[LES$sponsor == 'ferns, ryan james' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Ferns, Ryan' & ideo$party == 'D',]$party
LES[LES$sponsor == 'ferns, ryan james' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Ferns, Ryan' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'ferns, ryan james' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Ferns, Ryan' & ideo$party == 'R',]$name
LES[LES$sponsor == 'ferns, ryan james' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Ferns, Ryan' & ideo$party == 'R',]$party
LES[LES$sponsor == 'ferns, ryan james' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Ferns, Ryan' & ideo$party == 'R',]$np_score

### Arnold Ryan: Klarner is wrong, he was almost certainly a Democrat in 1994 election as he lost 1996 Dem. primary (albeit a highly conservative one): https://books.google.com/books?id=ExrbUtDV1zYC&pg=PA247&lpg=PA247&dq=arnold+ryan+1996+republican&source=bl&ots=Ga-O0VoBs8&sig=ACfU3U22qQ4BIxGShn3dkhWXvMHJT21NOw&hl=en&sa=X&ved=2ahUKEwjIoerA397oAhVthq0KHXWwDhwQ6AEwEHoECGIQKQ#v=onepage&q=arnold%20ryan%201996%20republican&f=false
LES[LES$sponsor == "ryan, arnold w." & LES$term == "1995_1996",]$party <- 'd'

### Douglas Stalnaker
LES[LES$sponsor == 'stalnaker, douglas k.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Stalnaker, Douglas' & ideo$party == 'D',]$name
LES[LES$sponsor == 'stalnaker, douglas k.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Stalnaker, Douglas' & ideo$party == 'D',]$party
LES[LES$sponsor == 'stalnaker, douglas k.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Stalnaker, Douglas' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'stalnaker, douglas k.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Stalnaker, Douglas' & ideo$party == 'R',]$name
LES[LES$sponsor == 'stalnaker, douglas k.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Stalnaker, Douglas' & ideo$party == 'R',]$party
LES[LES$sponsor == 'stalnaker, douglas k.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Stalnaker, Douglas' & ideo$party == 'R',]$np_score

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

#### Manual Fixes
LES[LES$sponsor == 'funksypolt, terri',]$sponsor <- 'sypolt, terri funk'
LES[LES$sponsor == 'louisos, tom',]$sponsor <- 'louisos, william thomas'
LES[LES$sponsor == 'doyle, john',]$sponsor <- 'doyle, robert john'
LES[LES$sponsor == 'dalton, sammie',]$sponsor <- 'dalton, james sammy'
LES[LES$sponsor == 'amores, jon',]$sponsor <- 'amores, constantino jon jr.'
LES[LES$sponsor == 'hutchins, tal',]$sponsor <- 'hutchins, talmadge'
LES[LES$sponsor == 'white, c. randy',]$sponsor <- 'white, clark randy'
LES[LES$sponsor == 'webb, rusty',]$sponsor <- 'webb, charles russell'
LES[LES$sponsor == 'sponaugle, isaac',]$sponsor <- 'sponaugle, george isaac'
LES[LES$sponsor == 'takubo, tom',]$sponsor <- 'takubo, tamejiro'
# LES[LES$sponsor == 'zzzzz',]$sponsor <- 'zzzzzz'


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1993 - 2020
# --> See Speakers: http://www.wvlegislature.gov/legisdocs/publications/bluebook/2015-2016/0337_WVS_BlueBook.pdf
LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:2014) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2015:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1991 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:2014) & LES$chamber == 'Senate'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2015:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


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
  summarize(mean_LES = mean(LES),max_LES = max(LES)) # %>% View()

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
  scale_color_manual(values=c("dodgerblue2", "red2"))

##### CHECK OUTLIERS ---- No switchers remaining through 2018!
# *** Tom Louisos = Very Conservative Democrat: https://web.archive.org/web/20001207181900/http://www.legis.state.wv.us/house/houselist.html
# *** No NP_Score for early Ron Thompson Rep. Term
# filter(LES, party == 'd' & np_score > 0 & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & np_score < 0 & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + in_majority + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

