

######################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** CONNECTICUT *** BY SESSION
######################################################################
# *********************** MAY NEED DIFFERENT SCRIPTS FOR 1991-1998 AND 1999 ONWARD **********

############ ********** TO DO FOR 2017-2018 ***************************
## ***** SENATE CO-CHAIRS in 2017-2018 *******
## --- In 2017-2018, CT Senate Split --> Co-chairs:  https://web.archive.org/web/20190121020530/https://ctmirror.org/2016/12/22/deal-struck-on-who-will-run-evenly-split-ct-senate/
## ---> Solution: split credit 50/50 between D/R Co-Chairs
########### ************************************


###################################
## SPECIAL SESSIONS:
## ---- Folded in
## MEMBER LISTS:
## ---- See name records at bottom
## PROCESS/RULES:
## ---- Joint Rules: https://www.cga.ct.gov/2019/TOB/s/pdf/2019SJ-00001-R00-SB.pdf
## Sponsorship/Authorship
## ---- Hierarchy of Recoding:
## -------- (1) Use First Primary Sponsor
## -------- (2) For "Committee Bills": Use First Named Legislator on "Proposed Bill" (e.g., the Introducer)
## -------- ***** Note: For 1991-1998: Use First Cosponsor (which is nearly alsays the introducer) on committee bills (see pattern for 1999+) 
## -------- (3) For "Raised Bills", fill in as many as possible using FIRST cosponsor from in-chamber; then attribute remaining to committee chair.
## -------- (4) For Senate, 2017-2018: Attributing credit 50/50 to Committee Co-Chairs (see below)
###########################
## NOTES:
## ***** Coding BIlls *************
## -- (1) Bills are first referred to a JOINT COMMITTEE; typically then get referred to other, chamber-specific committees. Treating the joint report as getting out of committee.. 
## -- (2) Bills are SUPPOSED to go to legislative commissioners office AFTER getting out of last committee -- in practice doesn't seem to be the case... --> Ignoring for now but could code as AIC??
# --------> Why do some bills get referrred to legisltive commissioners office TWICE?????
# --------> Per joint rules, 13(e), if voted change of reference, sent to LCO to "prepare the change of reference jacket and deliver the bill or resolution..."
## ***** SENATE CO-CHAIRS in 2017-2018 *******
## --- In 2017-2018, CT Senate Split --> Co-chairs:  https://web.archive.org/web/20190121020530/https://ctmirror.org/2016/12/22/deal-struck-on-who-will-run-evenly-split-ct-senate/
## ---> Solution: split credit 50/50 between D/R Co-Chairs
## ***** COMMITTEES *******
## --- What's interesting is that members ON THE COMMMITTEE are sometimes listed as cosponsors, but they also sometimes cosponsor NON-committee bills... 
## --- From the joint rules: https://www.cga.ct.gov/2019/TOB/s/pdf/2019SJ-00001-R00-SB.pdf
## (i) Types of Bills and Resolutions in 2020 Session. In the 2020 session, only the following bills and 
## resolutions may be introduced: Those (1) relating to budgetary, revenue and financial matters, (2)
## raised by committees of the General Assembly, and (3) relating to matters certified in writing by the 
## President Pro Tempore of the Senate and the Speaker of the House to be of an emergency nature.
## ---- Also from rules, re: committee bills, section 9(a)(1):
## Committee bills and committee resolutions may be introduced only by committees... Each committee bill and 
## committee resolution shall be (A) identified as a committee bill or committee resolution, (B) endorsed with 
## the signature of each chairperson of the committee, except such chairperson may permit the vice  # *********
## chairperson of the same chamber to sign any such bill or resolution, (C) filed with the clerk of the 
## appropriate chamber, and (D) assigned a number in accordance with the provisions of subdivision (3) of this subsection.
#############################

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
library(tibble)
library(foreach)
library(inexact)

this_state <- 'CT'
keep_types <- c("HB", "SB")

#### Output Directory
# dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd(glue("../../LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Terms

data_files <- list.files(glue("../../../State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
sessions <- gsub(".+Bill_Details_|.csv", '', bill_files)
rm(data_files, bill_files)

### Terms/Sessions and Filespaths
terms <- 2017
t = terms
t_plus_one = t + 1
t_yrs <- as.numeric(terms)
t_yrs <- as.character(glue('{t_yrs}_{t_yrs + 1}'))
t_sessions = sessions[grepl(as.character(t),sessions) | grepl(as.character(t+1),sessions)]


#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("../../../State Legislative Data/Commem Bills/{this_state}_Commem_Bills_{t_yrs}.csv"))
commem_bills <- mutate(commem_bills, bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = 0)))

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

#### Check SS BIll Types ---> Correct to ensure standardized with Data Format
# table(SS_bills$bill_type)


#### Load Committee Chair Data (for filling in sponsors)
committee_chairs <- read.csv("../../../State Legislative Data/Committee_Info/CT_committee_chairs_manual AV.csv")
committee_chairs[committee_chairs$chair_last_name == 'ritter' & committee_chairs$term == "2011_2012",]$chair_last_name <- 'elizabeth ritter'

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[10]



if(t < 1999){
  print('Skipping Years BELOW 1999 FOR NOW as no "INTRODUCED_BY" DATA')
  next
}


### Skip Previously Estimated
if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
  print(glue('. \n ~~~~ SKIPPING {t_yrs} TERM ---> LES scores already estimated! \n.'))
  next
}

###### SESSION IN PROGRESS
print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))

############### Read in data
bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_sessions[1]}.csv")
bills <- read_csv(bill_path, col_types = cols())

bill_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_sessions[2]}.csv")
bills2 <- read_csv(bill_path, col_types = cols())
bills <- bind_rows(bills, bills2)
rm(bills2)

### Clean Term/Session Variables
bills$term <- t_yrs

### Check for duplicates
if(nrow(bills) != nrow(distinct(bills))){
  cat(" \n ~~~> DUPLICATE BILLS \n\n .")
  break
}

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_num) %>% 
  mutate(bill_id = gsub('-|\\.HTM', '', bill_id),
         bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = 0)))

############### Drop Resolutions, Messages, Communications, Reports
bills <- mutate(bills, bt = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
all_bills <- bills
table(bills$bt)
bills <- filter(bills, bt %in% keep_types) %>% select(-bt)

###### Fix BIll Type Variable
# --> If NA: If we know the name of the primary sponsor OR the introducer, then necessarily a PROPOSED BILL
bills$bill_type <- ifelse(is.na(bills$bill_type) & (grepl("^rep\\.|^sen\\.", tolower(bills$primary_sponsors))| grepl("^rep\\.|^sen\\.", tolower(bills$introduced_by))), 'Proposed Bill', bills$bill_type)

##########################
####### Standardize Sponsors

#### Standardize
bills$primary_sponsors <- tolower(bills$primary_sponsors)
bills$primary_sponsors <- gsub('á', 'a', bills$primary_sponsors)
bills$primary_sponsors <- gsub('é', 'e', bills$primary_sponsors)
bills$primary_sponsors <- gsub('ó', 'o', bills$primary_sponsors)
bills$primary_sponsors <- gsub('í', 'i', bills$primary_sponsors)
bills$primary_sponsors <- gsub('ñ', 'n', bills$primary_sponsors)
#bills$primary_sponsors <- gsub(', [0-9]+[a-z]+ dist.|rep. |sen. ', '', bills$primary_sponsors)

bills$cosponsors <- tolower(bills$cosponsors)
bills$cosponsors <- gsub('á', 'a', bills$cosponsors)
bills$cosponsors <- gsub('é', 'e', bills$cosponsors)
bills$cosponsors <- gsub('ó', 'o', bills$cosponsors)
bills$cosponsors <- gsub('í', 'i', bills$cosponsors)
bills$cosponsors <- gsub('ñ', 'n', bills$cosponsors)
#bills$cosponsors <- gsub(', [0-9]+[a-z]+ dist.|rep. |sen. ', '', bills$cosponsors)

bills$introduced_by <- tolower(bills$introduced_by)
bills$introduced_by <- gsub('á', 'a', bills$introduced_by)
bills$introduced_by <- gsub('é', 'e', bills$introduced_by)
bills$introduced_by <- gsub('ó', 'o', bills$introduced_by)
bills$introduced_by <- gsub('í', 'i', bills$introduced_by)
bills$introduced_by <- gsub('ñ', 'n', bills$introduced_by)

### Eliminating Nicknames and Middle Initials to Minimize Formatting Issues
# --- For middle names in parentheses, need to manually remove or might catch committees (e.g., (env))?
# ---> Probably not, but doing manually to be safe
bills$primary_sponsors <- gsub(' \\"[^\\"]+\\" | \\(jack\\) | [a-z]\\. ', ' ', bills$primary_sponsors)
bills$cosponsors <- gsub(' \\"[^\\"]+\\" | \\(jack\\) | [a-z]\\. ', ' ', bills$cosponsors)

### Edit Rogue Apostrophes for 2015+
if(t >= 2015){
  bills$primary_sponsors <- gsub("dist\\.'sen\\.", "dist.; sen.", bills$primary_sponsors)
  bills$primary_sponsors <- gsub("dist\\.'rep\\.", "dist.; rep.", bills$primary_sponsors)
  bills$primary_sponsors <- gsub("\\'$", "", bills$primary_sponsors)
}

#### Character Error
if(t == 2015){
  bills$introduced_by <- gsub('aundrã‰ bumgardner|aundrã© bumgardner|aundr.. bumgardner', 'aundre bumgardner', bills$introduced_by)
  bills$primary_sponsors <- gsub('aundrã‰ bumgardner|aundrã© bumgardner|aundr.. bumgardner', 'aundre bumgardner', bills$primary_sponsors)
  bills$cosponsors <- gsub('aundrã‰ bumgardner|aundrã© bumgardner|aundr.. bumgardner', 'aundre bumgardner', bills$cosponsors)
} 

#### Clear out Introducer Withdrawn
bills$primary_sponsors <- ifelse(bills$primary_sponsors == "introducer withdrawn", NA, bills$primary_sponsors)
bills$introduced_by <- ifelse(bills$introduced_by == "introducer withdrawn", NA, bills$introduced_by)

### LES SPONSOR Var
# --- Occasionally Senator is listed first on a house bill --> getting everything from the first in-chamber sponsor onward
# --- SEEMS TO ONLY OCCUR WHEN NAMES ARE LISTED IN NUMERICAL ORDER BY DISTRICT! 0---> DROP THESE
#bills$LES_sponsor <- ifelse(substring(bills$bill_id, 1, 1) == "H", str_extract(bills$primary_sponsors, "rep\\..+"), str_extract(bills$primary_sponsors, "sen\\..+"))
#bills$LES_sponsor <- gsub(';.+', '', bills$LES_sponsor)

### LES SPONSOR Var
if(t_yrs == "2021_2022"){
  bills$primary_sponsors <- gsub('dist.sen', 'dist.; sen', bills$primary_sponsors)
  bills$primary_sponsors <- gsub('dist.rep', 'dist.; rep', bills$primary_sponsors)
}
bills$LES_sponsor <- gsub(';.+', '', bills$primary_sponsors)
table(bills$LES_sponsor)

##### Clear Sponsor if Coded As Sponsoring an Out-Chamber Bill
if(sum(substring(bills$bill_id, 1, 1) == "H" & grepl("^sen\\.", bills$LES_sponsor)) > 0){
  bills[substring(bills$bill_id, 1, 1) == "H" & grepl("^sen\\.", bills$LES_sponsor),]$LES_sponsor <- NA
}
if(sum(substring(bills$bill_id, 1, 1) == "S" & grepl("^rep\\.", bills$LES_sponsor)) > 0){
  bills[substring(bills$bill_id, 1, 1) == "S" & grepl("^rep\\.", bills$LES_sponsor),]$LES_sponsor <- NA
}

##### Use Individual That Introduced Bill to Code Missing AND/OR Committee Bills (if not a committee)
bills$comm_sponsored_bill <- ifelse(grepl("^senate|^house|^select|committee|^request of the governor", bills$LES_sponsor), 1, 0)
bills$LES_sponsor <- ifelse((bills$comm_sponsored_bill == 1 | is.na(bills$LES_sponsor)) & !grepl("^\\([a-z ]+\\)", bills$introduced_by) & !is.na(bills$introduced_by) & bills$introduced_by != "", gsub(';.+', '', bills$introduced_by), bills$LES_sponsor)

##### Use FIRST COSPONSOR to Code Remaining RAISED BILLS for 1999+
# -- For 1991-1998: Coding "COmmittee Bills" and "Raised Bills" Using this Method bc for 1999+, introducer nearly always == first cosponsor
bills$comm_sponsored_bill2 <- ifelse(grepl("^senate|^house|^select|committee|^request of the governor", bills$LES_sponsor), 1, 0)
bills$first_cospon <- ifelse(substring(bills$bill_id, 1, 1) == "H", str_extract(bills$cosponsors, 'rep\\. .+'), str_extract(bills$cosponsors, 'sen\\. .+'))
bills$first_cospon <- gsub(';.+', '', bills$first_cospon)
if(t >= 1999){ # Using 2nd condition with bill type to get uncoded but probable raised bills
  bills$LES_sponsor <- ifelse((bills$comm_sponsored_bill2 == 1 | is.na(bills$LES_sponsor)) & (bills$bill_type %in% "Raised Bill" | grepl('\\([ a-z]+\\)', bills$introduced_by)) & !is.na(bills$first_cospon) & bills$first_cospon != '', bills$first_cospon, bills$LES_sponsor)
}else{
  bills$LES_sponsor <- ifelse((bills$comm_sponsored_bill2 == 1 | is.na(bills$LES_sponsor)) & !is.na(bills$first_cospon) & bills$first_cospon != '', bills$first_cospon, bills$LES_sponsor)
}

##### Get Committees for which we Need Committee Chairs 
# need_chair <- bills %>%
#   mutate(chamb = substring(bill_id, 1, 1), chair = '') %>%
#   filter(grepl("^senate|^house|^select|committee", LES_sponsor)) %>%
#   select(term, chamb, chair, LES_sponsor) %>%
#   distinct() %>%
#   arrange(LES_sponsor, chamb, term)
# CT_comms <- bind_rows(CT_comms, need_chair); next

##### Impute Committee-Sponsored Bills with Committee Chair
these_comms <- filter(committee_chairs, term %in% t_yrs) %>%
  mutate(suffix = substring(sprintf('%03d', chair_district), 3, 3),
         suffix = ifelse(as.numeric(suffix) %in% c(1), "st dist.", 
                         ifelse(as.numeric(suffix) %in% c(2), 'nd dist.', 
                                ifelse(as.numeric(suffix) %in% c(3), 'rd dist.', 'th dist.'))),
         new_spon = paste0(chair_last_name, ', ', chair_district, suffix), # ok if th not correct suffix
         new_spon = ifelse(chamb == "H", paste0('rep. ', new_spon), paste0('sen. ', new_spon)))

# CT Sen is split in 17-18. there is an R and D co-chair to each committee. we create two versions of each committee sponsored bill, give each chair credit for it.
if(t_yrs == "2017_2018"){
  senate_bills = bills %>% 
    filter(substring(bills$bill_id, 1, 1) == "S") %>% 
    mutate(bill_copy = 2)
  bills = bind_rows(bills %>% mutate(bill_copy = 1),senate_bills) %>% arrange(bill_id) 
}

if(nrow(these_comms) > 0){
  for(i in 1:nrow(these_comms)){
    chamb <- these_comms[i,]$chamb
    if(t_yrs == "2017_2018" & chamb == "S"){
      this_comm <- these_comms[i,]$LES_sponsor
      bills_vec <- bills[substring(bills$bill_id, 1, 1) %in% chamb & bills$LES_sponsor %in% this_comm,]$bill_id
      if(length(bills_vec) == 0){next
      } else if(
        length(bills_vec) %% 2 == 0 & identical(bills_vec[seq(2,length(bills_vec),2)],
                                                bills_vec[seq(1,length(bills_vec),2)])){
        cosponsors <- these_comms[these_comms$LES_sponsor==this_comm & these_comms$chamb=="S",]$new_spon
        bills[substring(bills$bill_id, 1, 1) %in% chamb & bills$LES_sponsor %in% this_comm,]$LES_sponsor <- rep(cosponsors, length(bills_vec)/2)
        print(paste0('(', chamb, ') ', this_comm, ' ---> ', cosponsors))
      }
    } else{
      this_comm <- these_comms[i,]$LES_sponsor
      bills[substring(bills$bill_id, 1, 1) %in% chamb & bills$LES_sponsor %in% this_comm,]$LES_sponsor <- these_comms[i,]$new_spon
      print(paste0('(', chamb, ') ', this_comm, ' ---> ', these_comms[i,]$new_spon))
    }
  }
  rm(chamb, this_comm)
}
rm(these_comms)

##### Clear Sponsor x2 if Coded As Sponsoring an Out-Chamber Bill
if(sum(substring(bills$bill_id, 1, 1) == "H" & grepl("^sen\\.", bills$LES_sponsor)) > 0){
  bills[substring(bills$bill_id, 1, 1) == "H" & grepl("^sen\\.", bills$LES_sponsor),]$LES_sponsor <- ''
}
if(sum(substring(bills$bill_id, 1, 1) == "S" & grepl("^rep\\.", bills$LES_sponsor)) > 0){
  bills[substring(bills$bill_id, 1, 1) == "S" & grepl("^rep\\.", bills$LES_sponsor),]$LES_sponsor <- ''
}

#### Extract Districts
bills$LES_sponsor <- gsub(";$", '', bills$LES_sponsor)
bills$LES_sponsor_district <- str_trim(gsub(', ', '', str_extract(bills$LES_sponsor, ", [0-9]+[a-z]+ dist.| [0-9]+[a-z]+ dist.$")))
bills$LES_sponsor <- str_trim(gsub(', [0-9]+[a-z]+ dist.| [0-9]+[a-z]+ dist.$', '', bills$LES_sponsor))
if(t_yrs == "2019_2020"){
  bills$LES_sponsor = gsub(", 77th","",bills$LES_sponsor)
}
if(t_yrs == "2021_2022"){
  bills$LES_sponsor = gsub(", 13th","",bills$LES_sponsor)
}

# if(t >= 2019){
#   # need to add semicolons in some places
#   bills$introduced_by = gsub("dist\\. ([A-Za-z])", "dist. ; \\1", bills$introduced_by)
# }

#### Getting All Unique Names and Districts
# -- Note: For 2005+ need to re-organize particular name types (e.g., smith, j. or smith, john)

if(t < 2019) {
  all_names <- na.omit(unique(c(paste0(bills$LES_sponsor, ', ', bills$LES_sponsor_district), 
                                unlist(str_split(bills$primary_sponsors, "; ")), 
                                unlist(str_split(bills$cosponsors, "; ")), 
                                unlist(str_split(bills$introduced_by, "; "))) ))
} else { # post 2019 the introduced_by are read from PDF which makes them noisy, i just skip here
  all_names <- na.omit(unique(c(paste0(bills$LES_sponsor, ', ', bills$LES_sponsor_district), 
                                unlist(str_split(bills$primary_sponsors, "; ")), 
                                unlist(str_split(bills$cosponsors, "; ")))))
}
all_names <- sort(all_names[!grepl('\\([ a-z]+\\)|committee|^house|^senate|^introducer|^request of the governor', all_names)])
all_names <- data.frame(LES_sponsor_full = all_names) %>%
  filter(LES_sponsor_full != '') %>%
  mutate(LES_sponsor_full = gsub(';$', '', str_trim(gsub('\\.\\.$', '.', LES_sponsor_full))),
         LES_sponsor_district = gsub(', ', '', str_extract(LES_sponsor_full, ', [0-9]+[a-z]+ dist.| [0-9]+[a-z]+ dist.$')),
         LES_sponsor_dataformat = gsub(', [0-9]+[a-z]+ dist.| [0-9]+[a-z]+ dist.$', '', LES_sponsor_full),
         title = gsub(' .+', '', LES_sponsor_dataformat),
         ### Converting, e.g., "Rep. Smith, John" to "Rep. John Smith"
         LES_sponsor = ifelse(grepl('^(rep\\.|sen\\.) [a-z][a-z]+, [a-z][a-z][a-z]+', LES_sponsor_dataformat),
                              paste0(title, ' ', sub('.+, ', '', LES_sponsor_dataformat), ' ', gsub('^(rep\\.|sen\\.) |,.+', '', LES_sponsor_dataformat)), 
                              LES_sponsor_dataformat),
         ### Converting, e.g., "Rep. Smith, J." to "Rep. J. Smith"
         LES_sponsor = ifelse(grepl('^(rep\\.|sen\\.) [a-z][a-z]+, [a-z]\\.$', LES_sponsor),
                              paste0(title, ' ', gsub('.+, |\\.$', '', LES_sponsor), ' ', gsub('^(rep\\.|sen\\.) |,.+', '', LES_sponsor)), 
                              LES_sponsor),
         ### Converting, e.g., "Rep. Smith J." to "Rep. J. Smith"
         LES_sponsor = ifelse(grepl("^(rep\\.|sen\\.) [a-z][a-z]+ [a-z]\\.$|^(rep\\.|sen\\.) [a-z]\\'[a-z]+ [a-z]\\.$", LES_sponsor),
                              paste0(title, ' ', gsub('.+ |\\.$', '', LES_sponsor), ' ', gsub('^(rep\\.|sen\\.) | [a-z]\\.$', '', LES_sponsor)), 
                              LES_sponsor),
         last_name = gsub('.+ |\\.', '', LES_sponsor),
         last_name = ifelse(last_name %in% c("jr", 'sr', 'ii', 'iii', 'iv'), str_extract(gsub('\\.$', '', LES_sponsor), " [a-z]+, (jr|sr|ii+|iv)$| [a-z]+ (jr|sr|ii+|iv)$"), last_name),
         last_name = str_trim(gsub(', (jr|sr|ii+|iv)$| (jr|sr|ii+|iv)$', '', last_name)),
         chamber = ifelse(grepl("^rep\\.", LES_sponsor), "H", "S"),
         dist_num = as.integer(gsub('[a-z]+ dis.+', '', LES_sponsor_district))) %>%
  distinct() %>%
  arrange(chamber, dist_num)

chamb_dist <- select(all_names, chamber, dist_num) %>% distinct()

#### Fix Individual Errors (E.g., Comma in-between first/last --> Reverse Order)
if(t == 1999){
  all_names[all_names$LES_sponsor_dataformat == "rep. john, martinez", c("LES_sponsor", "last_name")] <- c("rep. john martinez", "martinez")
  all_names[all_names$LES_sponsor_dataformat %in% c("rep. ronald san angelo", "rep. san angelo", "rep. sanangelo"), ]$LES_sponsor <- "rep. ronald san angelo"
  all_names[all_names$LES_sponsor_dataformat %in% c("rep. ronald san angelo", "rep. san angelo", "rep. sanangelo"), ]$last_name <- "san angelo"
}else if(t == 2011){
  all_names[all_names$LES_sponsor_dataformat %in% c("rep. melissa olson", "rep. melissa riley", "rep. olson", "rep. melissa olson-riley", "rep. olson-riley"), ]$LES_sponsor <- "rep. melissa olson-riley"
  all_names[all_names$LES_sponsor_dataformat %in% c("rep. melissa olson", "rep. melissa riley", "rep. olson", "rep. melissa olson-riley", "rep. olson-riley"), ]$last_name <- "olson-riley"
}else if(t == 2015){
  all_names[all_names$LES_sponsor_dataformat %in% c("rep. christine randall", "rep. christine rosati", "rep. randall", "rep. rosati"), ]$LES_sponsor <- "rep. christine rosati randall"
  all_names[all_names$LES_sponsor_dataformat %in% c("rep. christine randall", "rep. christine rosati", "rep. randall", "rep. rosati"), ]$last_name <- "randall"
}else if(t == 2017){
  all_names[all_names$LES_sponsor_dataformat %in% c("rep. kelly j.s. luxenberg", "rep. kelly juleson-scopino", "rep. kelly luxenberg", "rep. luxenberg"), ]$LES_sponsor <- "rep. kelly juleson-scopino luxenberg"
  all_names[all_names$LES_sponsor_dataformat %in% c("rep. kelly j.s. luxenberg", "rep. kelly juleson-scopino", "rep. kelly luxenberg", "rep. luxenberg"), ]$last_name <- "luxenberg"
}

#### Looping through CHAMBER-DISTRICTS and Standardizing Names Using Last Name 
# *** Note in some cases Name In Data has be standardized so need to use data format ***
no_primary_spon <- c() # Saving vector of those who didn't sponsor any bills
for(i in 1:nrow(chamb_dist)){
  this_chamb <- chamb_dist[i,]$chamber
  this_dist <- chamb_dist[i,]$dist_num
  these_names <- filter(all_names, chamber == this_chamb & dist_num == this_dist)
  last_names <- unique(these_names$last_name)
  if(nrow(these_names) == 0 | nrow(these_names) == 1){
    next
  }else{
    if(length(last_names) != 1 | any(grepl(" jr| sr| ii+| iv", these_names$LES_sponsor))){ #| paste0(this_chamb, "-", this_dist) %in% name_list[[as.character(t)]]
      print(paste0(" ~~~~ Collapsing Individuals by Last Name: ", paste0(these_names$LES_sponsor_dataformat, collapse = " -- "), " (", this_chamb, "-", this_dist, ") ~~~~"))
    }else if(any(these_names$LES_sponsor != these_names$LES_sponsor_dataformat)){
      print(paste0(" ~~~~ Collapsing Names Using Last, First Format: ", paste0(these_names$LES_sponsor_dataformat, collapse = " -- "), " (", this_chamb, "-", this_dist, ") ~~~~"))
    }
    for(name in last_names){
      ### Get Unique Individuals and Skip coding if only 1 unique name
      subset_names <- these_names[these_names$last_name == name,] 
      if(nrow(subset_names) == 1){ 
        if( nrow(bills[bills$LES_sponsor %in% subset_names$LES_sponsor_dataformat,]) == 0 ){
          no_primary_spon <- append(no_primary_spon, subset_names$LES_sponsor) 
        }
        next 
      }
      ### Recode Names with Longest (presumably most descriptive) Name
      replace_name <- unique(subset_names[nchar(subset_names$LES_sponsor) == max(nchar(subset_names$LES_sponsor)),]$LES_sponsor)
      if( nrow(bills[bills$LES_sponsor %in% subset_names$LES_sponsor_dataformat,]) > 0 ){
        bills[bills$LES_sponsor %in% subset_names$LES_sponsor_dataformat,]$LES_sponsor <- replace_name
      }else{
        no_primary_spon <- append(no_primary_spon, replace_name)
      }
      ## USING FULL SPONSOR NAME with DISTRICT otherwise with varying formats, "rep. smith" might match when we want "rep. smith, john"
      name_adjust_regex <- str_replace_all(unique(subset_names$LES_sponsor_full), "(\\W)", "\\\\\\1")
      name_adjust_regex <- paste(name_adjust_regex, collapse = "|")
      replace_name <- paste0(replace_name, ', ', subset_names$LES_sponsor_district[1])
      bills$primary_sponsors <- gsub(name_adjust_regex, replace_name, bills$primary_sponsors)
      bills$cosponsors <- gsub(name_adjust_regex, replace_name, bills$cosponsors)
      bills$introduced_by <- gsub(name_adjust_regex, replace_name, bills$introduced_by)
    }
  }
}
rm(list = ls(pattern = "^i|^this_(chamb|dist)|^name|_name|chamb_dist"))

if(t_yrs == "2019_2020"){
  bills$LES_sponsor[bills$LES_sponsor=="rep. allie -brennan"] <- "rep. raghib allie-brennan"
  bills$LES_sponsor[bills$LES_sponsor=="rep. pavalock -d'amato"] <- "rep. cara christine pavalock-d'amato"
  bills$LES_sponsor[bills$LES_sponsor=="sen. alexandra bergstein"] <- "sen. alexandra kasser"
  
}

if(t_yrs == "2021_2022"){
  bills$LES_sponsor[bills$LES_sponsor=="rep. allie -brennan"] <- "rep. raghib allie-brennan"
  bills$LES_sponsor[bills$LES_sponsor=="rep. pavalock -d'amato"] <- "rep. cara christine pavalock-d'amato"
  bills$LES_sponsor[bills$LES_sponsor=="sen. daugherty abrams"] <- "sen. mary daugherty abrams"
}

###################
###### Merge in S&S Bills
###################
# **** FOR CONNECTICUT: Special Bills folded in, continue where regualar session bills left off


if(t_yrs == "2017_2018"){
  SS_bills$bill_id[SS_bills$bill_id=="SB40004"]="SB0004"
}
if(t_yrs == "2019_2020"){
  SS_bills$bill_id[SS_bills$bill_id=="SB10001"]="SB0001"
}
if(t_yrs == "2021_2022"){
  SS_bills$bill_id[SS_bills$bill_id=="SB10001"]="SB0001"
  SS_bills$bill_id[SS_bills$bill_id=="SB20002"]="SB0002"
  SS_bills$bill_id[SS_bills$bill_id=="SB50005"]="SB0005"
  SS_bills$bill_id[SS_bills$bill_id=="SB40004"]="SB0004"
  SS_bills$bill_id[SS_bills$bill_id=="SB60006"]="SB0006"
}

SS_term <- SS_bills %>%
  filter(term == t_yrs) %>%
  distinct(term, bill_id, SS, year, Title) %>% 
  mutate(SS = 1)

missing_SS_bills <- anti_join(SS_bills %>% 
                                mutate(SS = 1) %>% 
                                distinct(term, bill_id, SS, year,Title), bills,
                              by = c("bill_id", "term")) %>% 
  left_join(all_bills %>% select(bill_id,term,primary_sponsors), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",primary_sponsors, ignore.case=T)) %>%
  arrange(primary_sponsors) 
unique(missing_SS_bills$bill_id) 

# now check to see if there are duplicate joins


duplicate_SS_bills = SS_term %>% tibble::rownames_to_column() %>% 
  left_join(bills , 
            by = c("bill_id", "term","year"= "session")) %>%
  group_by(rowname) %>% mutate(count = n()) %>%
  ungroup() %>% 
  select(count,bill_id,term,year, Title, purpose) %>% 
  arrange(desc(count),year,bill_id)

if(nrow(duplicate_SS_bills %>% filter(count > 1)) > 0 ){
  print("you have duplicates"); SS_duplicates_exist <- 1; # break
} else{
  print("no duplicates")
  SS_duplicates_exist <- 0
}

# for CT 17-18, we  don't mind the duplicates
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
    left_join(bills %>% select(bill_id,term,session),by=c("bill_id","term","year"="session")) %>% distinct()
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
  anti_join(SS_term, bills, by = c("bill_id", "term", "year"= "session"))
  
  # PVS bills that are duplicated in bills. not necessarily a problem!
  SS_term %>% group_by(bill_id, term) %>%
    mutate(count = n()) %>% filter(count > 1) %>% arrange(bill_id)
  
  # see if they are equal after removing double-counted bills. this is a crude proxy, you should examine more closely
  all.equal(SS_in_bills + nrow(anti_join(SS_term, bills, by = c("bill_id", "term", "year"="session")))+
              nrow(SS_term %>% group_by(bill_id, term) %>%
                     mutate(count = n()) %>% filter(count > 1))/2,
            nrow(SS_term ))
}


###################################################
############### Code Commemorative
####################################################
bills <- commem_bills %>%
  filter(term == t_yrs) %>%
  select(bill_id, term, session, commem) %>%
  left_join(bills, ., by = c('bill_id', 'term', 'session'))

#### Not Commemorative if SS
bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)


#### Drop Uncoded Committees
if(any(grepl('^senate|^house|^select|committee', bills$LES_sponsor))){
  cat('\n')
  print(glue('---> Dropping {sum(grepl("^senate|^house|^select|committee", bills$LES_sponsor))} Committee Sponsored Bills --> ATTRIBUTE TO CHAIR! (N = {nrow(bills)})'))
  bills <- filter(bills, !grepl('^senate|^house|^select|committee', LES_sponsor))
}

### Drop Bills with Introducer Withdrawn
if(any(grepl('introducer withdrawn', bills$LES_sponsor))){
  cat('\n')
  cat(glue('---> Dropping {sum(grepl("introducer withdrawn", bills$LES_sponsor))} Bills with Withdrawn Sponsor'))
  bills <- filter(bills, !grepl('introducer withdrawn', LES_sponsor))
}

### Checking if any more missing
if(nrow(filter(bills, LES_sponsor == '')) > 0 | any(is.na(bills$LES_sponsor)) ){
  cat('\n')
  cat(glue('---> Dropping {nrow(filter(bills, LES_sponsor == "")) + sum(is.na(bills$LES_sponsor))} Bills With NO Sponsor'))
  bills <- filter(bills, !(is.na(LES_sponsor) | LES_sponsor == ""))
}

### Drop bills sponsored by the Governor
if(any(grepl("^request of the governor", bills$LES_sponsor))){
  cat(glue('---> Dropping {nrow(filter(bills, grepl("^request of the governor", LES_sponsor)))} Bills Sponsored By THE GOVERNOR'))
  bills <- filter(bills, !grepl("^request of the governor", LES_sponsor))
}; cat('\n')

###################################################
############### Code Bill History
###################################################
bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
bill_hist <- read_csv(bill_hist_path, col_types = cols())

bill_hist_path <- glue("../../../State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[2]}.csv")
bill_hist2 <- read_csv(bill_hist_path, col_types = cols())

bill_hist <- bind_rows(bill_hist, bill_hist2)
rm(bill_hist2)

### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
bill_hist$term <- t_yrs
bill_hist <- rename(bill_hist, bill_id = bill_num) %>% 
  mutate(bill_id = gsub('-|\\.HTM', '', bill_id),
         bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = 0)))

### Re-Format Date
bill_hist$action_date <- as.Date(bill_hist$action_date, format = '%m/%d/%y')

### Rearrange + create order variable that covers both chambers
bill_hist <- arrange(bill_hist, session, bill_id, order) 

### Creating Chamber Variable
# filter(bill_hist, grepl('transmit', tolower(action))) %>% distinct(action) %>% as.data.frame()
bill_hist$chamber <- ifelse(bill_hist$order == 1, substring(bill_hist$bill_id, 1, 1), NA)
bill_hist$chamber <- ifelse(grepl('transmit.+house$', tolower(bill_hist$action)), 'S', bill_hist$chamber)
bill_hist$chamber <- ifelse(grepl('transmit.+senate$', tolower(bill_hist$action)), 'H', bill_hist$chamber)
bill_hist$chamber <- ifelse(grepl('^senate', tolower(bill_hist$action)) & is.na(bill_hist$chamber), 'S', bill_hist$chamber)
bill_hist$chamber <- ifelse(grepl('^house', tolower(bill_hist$action)) & is.na(bill_hist$chamber), 'H', bill_hist$chamber)
#--> Need to account for things like 'Adopted, Senate as amended by house'
#--> Also need to catch for change of reference as lots of joint committees
bill_hist$chamber <- ifelse(grepl('senate', tolower(bill_hist$action)) & !grepl('senate amendment|amended by senate|change of reference', tolower(bill_hist$action)) & is.na(bill_hist$chamber), 'S', bill_hist$chamber)
bill_hist$chamber <- ifelse(grepl('house', tolower(bill_hist$action)) & !grepl('house amendment|amended by house|change of reference', tolower(bill_hist$action)) & is.na(bill_hist$chamber), 'H', bill_hist$chamber)
bill_hist$chamber <- ifelse(grepl('in concurrence|^public act|^special act', tolower(bill_hist$action)) & is.na(bill_hist$chamber), substring(bill_hist$bill_id, 1, 1), bill_hist$chamber) ## In Concurrence lamost always means back to origin chamber
bill_hist$chamber <- ifelse(grepl('transmit.+gov|transmit.+secretary of state|by the governor', tolower(bill_hist$action)) & is.na(bill_hist$chamber), 'E', bill_hist$chamber)

### Filling in Remaining Uncoded Chambers Using Known Chamber-Actions
bill_hist <- group_by(bill_hist, session, bill_id) %>% fill(chamber) %>% ungroup()

### Standardize Chamber Variable
bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate", "E" = "Executive")

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c('public hearing', 'vote to draft', 'drafted by committee', 'favorable', 'unfavorable', 'change of reference', 'merged into')
### *** Bills referred first to a joint committee... should joint favorable == ABC? At present, yes, though often then referred to a chamber committee
### ---> Can't find records of unfavorable reports but coding in case they show up in unchecked years
### ---> if drafted by comm, seems to then be referred back to that committee when complete
### *** Favorable change of reference = Comm 1 refers to Comm 2 with favorable recommendation - https://www.cga.ct.gov/asp/content/terms.asp
abc_t <- c('favorable report', 'joint favorable$', 'joint favorable sub', 'house favorable', 'senate favorable', 'house calendar', 'senate calendar',
           'referred by house', 'referred by senate', 'house adopted.+amend', 'senate adopted.+amend')
# *** After committee, bills go to legislative commissioner's office to be checked for constitutionality/conflicts with other law
# --> BUT -- in records, often goes there before reported out??? So is this committee action?? 'filed with legislative commissioner', 'reported out of legislative commissioner'
# *** eems like favorable report == completed comm office process and reported to floor; joint favorable typically goes to different committee...
pc_t <- c('^house passed', '^senate passed', 'in concurrence', 'secretary of state', '^public act', '^special act')
law_t <- c('signed by governor', 'signed by the governor', 'public act', '^became law') #### text if overridden???

### Check Actions
# mutate(bill_hist, gsub('[a-z].+', '', action)) %>% group_by(action) %>% filter(grepl('FAVORABLE', action)) %>% summarize(n = n()) %>% View()
# filter(bill_hist, grepl('passed', tolower(action)) & !grepl('zzzzzz', tolower(action))) %>% distinct(action) %>% View()
# filter(bill_hist, grepl('effective date', tolower(action))) %>% group_by(action) %>% summarize(n = n()) %>% arrange(desc(n)) %>% View()
# bill_hist %>% group_by(bill_id, session) %>% mutate(keep = sum(grepl('vetoed', tolower(action)))) %>% filter(keep > 0) %>% View()

####################
### Output Matrix
all_bill_stages = tibble(bill_id = character(0),
                         term = character(0),
                         session = double(0),
                         LES_sponsor = character(0),
                         introduced = integer(0),
                         action_in_comm = integer(0),
                         action_beyond_comm = integer(0),
                         passed_chamber = integer(0),
                         law = integer(0),
                         bill_url = character(0))

if(t_yrs == "2017_2018"){
  all_bill_stages$bill_copy = integer(0)
}

### Make Sure No Excess text in Bill Action
bill_hist$action <- str_trim(tolower(bill_hist$action))

### Code History Stages for Each Bill --- Subset Hist File, Code, Save, Repeat
options(warn = 2)
for(i in 1:nrow(bills)){
  b_id = bills[i,]$bill_id
  s_id = bills[i,]$session
  b_spon = bills[i,]$LES_sponsor
  hist_sub <- filter(bill_hist, bill_id == b_id, session == s_id)
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
  bill_stages$bill_url <- bills[i,]$bill_url
  if(t_yrs == "2017_2018"){
    bill_stages$bill_copy = bills[i,]$bill_copy
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
  select(bill_id, term, year, SS) %>%
  left_join(all_bill_stages, ., by = c('bill_id', 'term', "session"="year")) %>%
  mutate(SS = ifelse(is.na(SS), 0, SS))
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

#### Add the individuals who DIDN'T PRIMARY SPONSOR A BILL to end
for(j in no_primary_spon){
  if(!any(grepl(j, all_sponsors$LES_sponsor))){ # THis should always be true, but just in case..
    c = ifelse(grepl("^rep\\.", j), "H", "S")
    all_sponsors <- add_row(all_sponsors, LES_sponsor = j, chamber = c, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0)  
  }
}
if(length(no_primary_spon) > 0){ rm(c, j) }

######## Cosponsorship Info 
# **** This is only IN-CHAMBER cosponsors (e.g. HB's cosponsored if in House, dropping SB's)
all_sponsors$num_cosponsored_bills <- NA
bills$cospon_match <- paste(bills$LES_sponsor, bills$cosponsors, sep = '; ')
for(i in 1:nrow(all_sponsors)){
  c <- substring(all_sponsors[i,]$chamber,1,1)
  c_sub <- filter(bills, substring(bill_id, 1, 1) == c)
  search_name = str_replace_all(all_sponsors[i,]$LES_sponsor, "(\\W)", "\\\\\\1")
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(search_name, tolower(c_sub$cospon_match)))
  ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
}; rm(c, c_sub, search_name)
bills <- select(bills, -cospon_match)
# View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills)) 
if(min(all_sponsors$num_cosponsored_bills) < 0){
  print("negative cosponsors"); break
} else{print("cosponsors valid")}


#######################
#### CLEAN NAMES
parsed_names <- map_df(gsub('^rep\\. |^sen\\. |\\"[^\\"]+\\"', '', all_sponsors$LES_sponsor), parse_names) %>% 
  select(-salutation) %>% 
  mutate(last_name = ifelse(middle_name %in% c("van", "de", "san"), paste0(middle_name, ' ', last_name), last_name),
         last_name = gsub('\\,$', '', last_name),
         first_name = gsub('\\,$', '', first_name),
         suffix = ifelse(is.na(suffix), '', suffix),
         suffix = ifelse(middle_name %in% c("jr", "sr", "ii", "iii", "iv"), middle_name, suffix),
         middle_name = ifelse(middle_name %in% c("van", "de", "san", "jr", "sr", "ii", "iii", "iv") | is.na(middle_name), '', middle_name)) %>%
  distinct() %>%
  as.data.frame()

all_sponsors <- all_sponsors %>%
  mutate(match_name = gsub('^rep\\. |^sen\\. |\\"[^\\"]+\\"', '', LES_sponsor)) %>% 
  left_join(., parsed_names, by = c("match_name" = "full_name")) %>%
  select(-match_name) %>%
  arrange(chamber, LES_sponsor)

#### Update First Names, Last Names for Matching 
if(t_yrs %in% c('1999_2000') ){
  all_sponsors[all_sponsors$LES_sponsor %in% c("rep. santa-maria"),]$last_name <-  "santamaria"
}
if(t_yrs %in% c("1999_2000", "2001_2002")){
  all_sponsors[all_sponsors$LES_sponsor %in% c("rep. ronald san angelo"),]$last_name <-  "sanangelo"
}
if(t >= 2003 & t <= 2018){
  all_sponsors[all_sponsors$LES_sponsor %in% c("rep. toni walker"),]$last_name <-  "edmondswalker"
}
if(t == 2011){
  all_sponsors[all_sponsors$LES_sponsor %in% c("rep. melissa olson-riley"),]$last_name <-  "olson"
}
if(t == 2015){
  all_sponsors[all_sponsors$LES_sponsor %in% c("rep. christine rosati randall"),]$last_name <-  "rosati"
}
if(t == 2017){
  all_sponsors[all_sponsors$LES_sponsor %in% c("rep. anne dauphinais"),]$last_name <-  "dubaydauphinais"
  all_sponsors[all_sponsors$LES_sponsor %in% c("rep. cara christine pavalock-d'amato"),]$last_name <-  "pavalock"
}






all_sponsors <- all_sponsors %>% 
  mutate(last_name = gsub(' .+', '', LES_sponsor),
         first_name = ifelse( !grepl(' ', LES_sponsor), NA, gsub('.+ ', '', LES_sponsor) )) %>% 
  arrange(chamber, LES_sponsor) %>%
  distinct() %>% 
  mutate(match_name_chamber = tolower(paste(str_remove_all(LES_sponsor, '"\\s*.*?\\s*"'),substr(chamber,1,1),sep="-")),
         match_name_chamber = gsub("rep. ","",gsub("sen. ","",match_name_chamber))) %>% 
  select(-c(last_name,first_name,middle_name))

legiscan_sessions = list.files(glue("../../../State Legislative Data/Legiscan/{this_state}"))
legiscan_sessions = legiscan_sessions[startsWith(legiscan_sessions,as.character(t)) | startsWith(legiscan_sessions,as.character(t+1))]

legiscan = foreach(i = legiscan_sessions, .combine = rbind) %do%{
  read.csv(glue("../../../State Legislative Data/Legiscan/{this_state}/{i}/csv/people.csv"))
} %>% select(-c(votesmart_id,knowwho_pid, ballotpedia,nickname,suffix)) %>%  distinct() 


if(t_yrs == "2017_2018") {
  legiscan = legiscan %>% 
    mutate(role = case_when(people_id %in% c(18260,5711,10806) ~ "Rep", T ~ role),
           district = case_when(people_id == 18260 ~ "HD-019",
                                people_id == 5711 ~ "HD-100",
                                people_id == 10806 ~ "HD-080",
                                T ~ district)) %>% 
    bind_rows(legiscan %>% filter(people_id == 16955) %>% 
                mutate(role = "Rep", district = "HD-068"),
              legiscan %>% filter(people_id == 5785) %>%
                mutate(role = "Rep",district = "HD-007"))
  
}


if(t_yrs == "2019_2020") {
  legiscan = legiscan  %>% 
    bind_rows(legiscan %>% filter(people_id == 18260) %>% 
                mutate(role = "Rep", district = "HD-019"))
  
}

legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(name, role) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = name) %>%
  mutate(match_name_chamber = tolower(paste(match_name,substr(district,1,1),sep="-")))

# inexact::inexact_addin()
# left: legiscan_adj
# right: all_sponsors
# method: osa
# mode: full

if(t_yrs == "2017_2018"){
  all_sponsors2 = 
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "julio concepcion-h" = NA_character_,
        "kelly luxenberg-h" = "kelly juleson-scopino luxenberg-h",
        "geoffrey luxenberg-h" = NA_character_,
        "kenneth green-h" = NA_character_,
        "timothy legeyt-h" = NA_character_,
        "joshua hall-h" = "joshua malik hall-h",
        "patricia miller-h" = "patricia billie miller-h"
      )
    )
  
}


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
        "bill simanski-h" = NA_character_,
        "andre baker-h" = NA_character_,
        "joshua hall-h" = "joshua malik hall-h",
        "anne hughes-h" = "anne meiman hughes-h",
        "jill barry-h" = NA_character_,
        "harry arora-h" = NA_character_,
        "joseph zullo-h" = NA_character_,
        "patricia miller-h" = "patricia billie miller-h"
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
        "joe de la cruz-h" = NA_character_,
        "antonio felipe-h" = NA_character_,
        "andre baker-h" = NA_character_,
        "christopher perone-h" = NA_character_,
        "anne hughes-h" = "anne meiman hughes-h",
        "patricia miller-s" = "patricia billie miller-s",
        "patricia miller-h" = "patricia billie miller-h"
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

###################################################################
############### Estimate Scores + Add in Relatd Variables
###################################################################

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

# Since we doubled the bills in 17-18 S, have to halve them now
if(t_yrs == "2017_2018"){
  LES = LES %>% 
    mutate_at(vars(starts_with("all_"),starts_with("ss_"),starts_with("c_"),
                   starts_with("s_"),starts_with("num_")),
              ~ ifelse(chamber == "Senate",./2,.)  )
}

############### Save LES
write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
rm(LES)

cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************"))


################# ****** END LOOP

#agg_stats, matches
rm(all_sponsors, bills, legis_data, SS_bills, H_elec_year, S_elec_year, i, keep_types)
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES, t_sessions) 
rm(t, terms, klarner_gs, m_sub, parsed_names, commem_bills, no_primary_spon, committee_chairs)


########################################################################################################################################################
########################################################################################################################################################
######## NAME FIX RECORDS ---> If listed below, terms have been checked and fixed (unless noted otherwise)
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by SPECIAL ELECTION --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
## MEMBER LIST: Search for First Journal of Each Term to see who was seated: https://search.cga.state.ct.us/r/adv/
###################################################

# *************************************************************************
# ~~~~~~~~~~~~ TEMPORARILY SKIPPING 1991 - 1999: Don't have info about WHO introduced each bill ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ************************************************************************

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 6 Bills With NO Sponsor
#   session chamber    N AIC ABC PASS LAW
# 1    1999       H 2121 1056 422  195 168
# 2    1999       S 1405  798 363  178 137
# 3    2000       H  932  614 351  177 148
# 4    2000       S  640  459 277  140  98
#### DISTRICT NAMES COLLAPSED
# -- H-66: Maddox
# -- H-95: Martinez + Fix for out-of-place comma
# -- H-107: Scribner/Santa-Maria
# -- H-124: Newton
# -- H-131: San Angelo 
# -- S-14: Smith
### WON SPECIAL ~ HOUSE:
# -- david scribner
# -- mary ann carson
### DROP:
# -- gyle, norma -- appointed Deputy Commissioner of Dept. of Pub. Health: https://www.ctpost.com/news/article/Rell-appoints-New-Fairfield-resident-Norma-Gyle-4592.php


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 1 Bills With NO Sponsor
# ---> Dropping 4 Bills Sponsored By THE GOVERNOR
#   session chamber    N AIC ABC PASS LAW
# 1    2001       H 2061 1116 441  156 125
# 2    2001       S 1465  909 383  169 114
# 3    2002       H  796  556 290  113  93
# 4    2002       S  665  542 310  143  75
#### DISTRICT NAMES COLLAPSED
# -- H-74: Jarjura/Noujaim
# -- H-93: Scipio/Walker
# -- H-137: Knopp/Duff
# -- H-138: Boughton/Scire
### WON SPECIAL ~ HOUSE:
# -- bob duff
# -- grace scire (lost 2002 reelection)
# -- selim noujaim 
# -- toni walker (edmonds-walker)
#### DROP:
# -- tulisano, richard d. -- resigned January 2001: https://www.newstimes.com/news/article/Former-state-Rep-Richard-Tulisano-dies-at-68-113254.php


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 3 Bills With NO Sponsor
#   session chamber    N AIC ABC PASS LAW
# 1    2003       H 1727 933 392  198 174
# 2    2003       S 1192 722 327  184 134
# 3    2004       H  692 500 312  163 142
# 4    2004       S  637 514 313  188 126
#### DISTRICT NAMES COLLAPSED
# -- H-68: flaherty/williams
# -- H-124: Clemons/Newton
# -- S-23: Penn/Newton
### WON SPECIAL ~ HOUSE:
# -- charles clemons
# -- sean williams
### WON SPECIAL ~ SENATE:
# -- ernest newton (via H, April 2003)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 2 Bills With NO Sponsor
#   session chamber    N AIC ABC PASS LAW
# 1    2005       H 2008 1156 383  190 171
# 2    2005       S 1396  906 384  199 147
# 3    2006       H  846  610 319  127 104
# 4    2006       S  703  590 320  136 101
#### DISTRICT NAMES COLLAPSED:
# -- H-1: Green
# -- H-9: Stone (christopher)
# -- H-10: Currey/Genga
# -- H-105: Greene
# -- H-134: Stone (john)
# -- H-139: Ryan (Kevin)
# -- H-141: Ryan (John)
# -- S-23: Gomes/Newton
### WON SPECIAL ~ HOUSE:
# -- catherine abercrombie
# -- henry genga
### WON SPECIAL ~ SENATE:
# -- edwin gomes
### DROP:
# -- abrams, james w. (jim) -- Never seated, appointed to judicial post? See Dist. 83: https://search.cga.state.ct.us/r/adv/dtsearch.asp?cmd=getdoc&DocId=15311&Index=I%3a%5czindex%5c2005&HitCount=0&hits=&hc=0&req=&Item=0


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 3 Bills With NO Sponsor
#   session chamber    N  AIC ABC PASS LAW
# 1    2007       H 2440 1280 436  163 141
# 2    2007       S 1488  931 397  184 133
# 3    2008       H  941  596 340  139 112
# 4    2008       S  714  563 381  175  92
#### DISTRICT NAMES COLLAPSED:
# -- H-1: Green
# -- H-11: Christ
# -- H-78: Hamzy
# -- H-86: Candelora
# -- H-95: Candelaria (juan)
# -- H-105: Greene
# -- H-108: Carson
# -- H-113: Perillo/Belden
# -- H-118: Amann
# -- H-134: Christiano
# -- H-139: Ryan (Kevin)
# -- H-141: Ryan (John)
# -- S-22: Finch/Russo
# -- S-32: Deluca/Kane
### WON SPECIAL ~ HOUSE:
# -- jason perillo
### WON SPECIAL ~ SENATE:
# -- robert d russo (March 2008, after losing twice; then lost again in Nov 2008)
# -- robert j kane

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 2 Bills With NO Sponsor
#   session chamber    N AIC ABC PASS LAW
# 1    2009       H 1730 927 449  230 146
# 2    2009       S 1172 707 401  187 128
# 3    2010       H  545 442 293  162 126
# 4    2010       S  497 421 316  154  77
#### DISTRICT NAMES COLLAPSED:
# -- H-15: Baram/McMahon
# -- H-41: Wright (Elissa)
# -- H-77: Wright (Christopher)
# -- H-120: Harkins/Hoydick
# -- H-122: Miller (lawrence)
# -- H-145: Miller (patricia)
### WON SPECIAL ~ HOUSE:
# -- david baram 
# -- laura r hoydick
### IN CHAMBER:
# -- delgobbo, kevin m.


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~
# ---> Dropping 2 Bills With NO Sponsor
#   session chamber    N AIC ABC PASS LAW
# 1    2011       H 1652 842 395  202 171
# 2    2011       S 1246 708 385  178 105
# 3    2012       H  559 444 308  189 128
# 4    2012       S  459 392 268  158  91
#### DISTRICT NAMES COLLAPSED:
# -- H-1: Ritter (matthew)
# -- H-24: Lopes/O'Brien
# -- H-31: Srinivasan
# -- H-38: Ritter (elizabeth)
# -- H-41: Wright (elissa)
# -- H-46: Olson-Riley
# -- H-57: Davis (Christopher)
# -- H-61: O'Brien (elaine)
# -- H-77: Wright (christopher)
# -- H-117: Davis (paul)
# -- H-122: Miller (lawrence)
# -- H-145: Miller (patricia)
# -- H-148: Leone/Fox
#### WON SPECIAL ~ HOUSE:
# -- charlie stallworth
# -- james albis
# -- noreen kokoruda
# -- rick lopes
# -- robert sanchez
# -- fox, daniel j. -- Name duplicated, not listed**
### WON SPECIAL ~ SENATE: 
# -- carlo leone (via H)
# -- len suzio (Feb 2011, lost 2012 /2014 elections, won 2016)
# -- terry gerrantana
### DROP: ** ALL NEVER SEATED --> See first 2011 House/Senate Journals ***
# -- mccluskey, david d.
# -- geragosian, john c. 
# -- spallone, james field 
# -- lawlor, michael p.
# -- heinrich, deborah
# -- caruso, christopher l.
# -- defronzo, donald j.
# -- gaffey, thomas p.
# -- mcdonald, andrew j.


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 2 Bills With NO Sponsor
#   session chamber    N AIC ABC PASS LAW
# 1    2013       H 1705 855 404  227 192
# 2    2013       S 1165 661 363  207 143
# 3    2014       H  597 460 323  188 141
# 4    2014       S  494 432 303  158 117
#### DISTRICT NAMES COLLAPSED:
# -- H-1: ritter (matthew)
# -- H-31: srinivasan
# -- H-38: ritter (elizabeth)
# -- H-41: wright (elissa)
# -- H-53: hulburt/belsito
# -- H-57: davis (christopher)
# -- H-61: o'brien (elaine)/zawistowski
# -- H-77: wright (christopher)
# -- H-94: holder-winfield/porter
# -- H-117: davis (paul)
# -- H-122: miller (lawrence)
# -- H-145: miller (patricia)
# -- S-10: holder-winfield/harp
### WON SPECIAL ~ HOUSE:
# -- robyn porter
# -- sam belsito
# -- tami zawistowski
### WON SPECIAL ~ SENATE:
# -- gary holder-winfield (via H)
### IN CHAMBER:
# -- backer, terrance, e. (terry)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 2 Bills With NO Sponsor
#   session chamber    N AIC ABC PASS LAW
# 1    2015       H 2064 974 422  205 143
# 2    2015       S 1140 672 368  210 134
# 3    2016       H  642 472 343  178 111
# 4    2016       S  480 436 331  173 132
#### DISTRICT NAMES COLLAPSED:
# -- H-1: ritter (matthew)
# -- H-31: srinivasan
# -- H-44: randall/rosati --> 'christine rosati randall'
# -- H-57: davis (christopher)
# -- H-75: cuevas/reyes
# -- H-112: sredzinski
# -- H-121: backer/gresko
# -- H-145: miller (patricia)
#### WON SPECIAL ~ HOUSE:
# -- geraldo reyes
# -- joseph gresko
# -- stephen harding 
# -- steven stafstrom
### WON SPECIAL ~ SENATE:
# -- edwin gomes (past S, Feb 2015)
#### DROP: 
# --> *** ALL NEVER SEATED ***
# -- scribner, david a. 
# -- grogins, auden c.
# -- ayala, andres jr.


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# ---> Dropping 8 Bills with NO Sponsor
#   session chamber    N AIC ABC PASS LAW
# 1    2017       H 2317 1059 421  218 160
# 2    2017       S 1062  595 300  165 111
# 3    2018       H  592  471 337  148 113
# 4    2018       S  543  409 297  134 101
#### DISTRICT NAMES COLLAPSED:
# H-1: ritter (matthew)
# H-7: mccrory/hall
# H-12: luxenberg/juleson-scopino -----> Name CHANGE
# H-15: baram/gibson
# H-31: srinivasan
# H-57: davis (christopher)
# H-68: berthel/polletta
# H-112: sredzinski
# H-120: hoydick/young
# H-145: miller (patricia)
### WON SPECIAL ~ HOUSE:
# -- bobby gibson
# -- dorinda borer
# -- joe polletta
# -- phillip young
# -- joshua malik hall --> NAME DUPLICATED, won't print
### WON SPECIAL ~ SENATE:
# -- douglas mccrory (via H)
# -- eric berthel (via H)
### IN CHAMBER:
# -- legeyt, timothy
#### DROP: 
# --> *** ALL NEVER SEATED ***
# -- dargan, stephen d. 
# -- coleman, eric d.
# -- kane, robert j.
# ******* NEED TO FIX CO-COMMITTEE CHAIRS *****************

# filter(klarner, grepl('dargan', cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid, vote)
# filter(klarner, ddez == 46) %>% select(cand, year, sen, etype, outcome, ddez, candid)

##########################################################################################################
##########################################################################################################
##### MATCHING MISSING (SPECIALS) TO KLARNER USING OTHER TERMS WHERE SPONSOR IS PRESENT
##########################################################################################################

### Re-Load LES Data
LES_paths <- list.files('.', full.names = TRUE)
LES_paths <- LES_paths[!grepl('Merged', LES_paths)]

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
    if(length(unique(k_sub$cand)) == 1 & length(unique(k_sub$candid)) == 1 & sum(!is.na(k_sub$candid)) != 0 ){
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

### ****Still missing***** ---> Rest are not in Klarner or 2018+
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[4]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl('young,', cand)) %>% select(cand, year, etype, sen, outcome, candid) # %>%  View()
# rm(name, missing, name_sub, still_missing)

### Fix Missing
# *** Note: 2017-2018: Dorinda Borer and Philip Young != 1996 Borer and 2008 Philip Young
name_matches <- data.frame(LES_name = 'suzio, len', k_name = 'suzio, leonard f. (len) jr.')
name_matches <- add_row(name_matches, LES_name = 'holder-winfield, gary', k_name = 'winfield, gary a.')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}
rm(name_matches, i)

## MANUAL FIXES
# LES[LES$sponsor %in% "kinon, m" & LES$term %in% "1989_1990",]$klarner_id <- 208116
# LES[LES$sponsor %in% "kinon, m" & LES$term %in% "1989_1990",]$klarner_name <- "kinon, marion h. son"
# LES[LES$sponsor %in% "kinon, m" & LES$term %in% "1989_1990",]$sponsor <- "kinon, marion h. son"

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
rm(check_dup, k_sub, exact, name_sub)


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
      LES[LES$sponsor == name,]$party <- sponsor_rows[1,]$partyt
    }
  }else{
    for(t in this_sponsor_LES$term){
      second_year <- as.numeric(str_split(t, "_")[[1]][2])
      ### Filling in by chamber to account for people who switch chambers mid-term
      for(c in this_sponsor_LES[this_sponsor_LES$term == t,]$chamber){
        sponsor_sub <- filter(sponsor_rows, (etype %in% spec_elec_codes & year == second_year ) | year < second_year )
        sponsor_sub <- filter(sponsor_sub, sen == ifelse(c == "Senate", 1, 0))
        ### Drop Independent Primaries
        sponsor_sub <- filter(sponsor_sub, etype != "indepp")
        if(nrow(sponsor_sub) > 0 ){
          sponsor_sub <- arrange(sponsor_sub, desc(year))
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$district <- sponsor_sub[1,]$dno
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$party <- sponsor_sub[1,]$partyt
          LES[LES$sponsor == name & LES$term == t & LES$chamber == c,]$exper <- sponsor_sub[1,]$exper
        }else{
          if(nrow(sponsor_rows) > 0){
            sponsor_rows <- arrange(sponsor_rows, year) %>% filter(etype != "indepp")
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
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

### Not in Klarner
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')
# for(i in 1:nrow(fill_missing)){
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
# }


#### *** 2017-2018: If any run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "gibson, bobby",]$party <- 'd'
LES[LES$sponsor == "borer, dorinda",]$party <- 'd' # Dorinda Keenan Borer
LES[LES$sponsor == "hall, joshua",]$party <- 'd' # Joshua Malik Hall
LES[LES$sponsor == "young, philip",]$party <- 'd'

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **

### REMOVE TITLES (Shouldn't be many, if any)
LES$sponsor <- gsub('^rep. |^sen. ', '', LES$sponsor)
# table(LES$sponsor)

#### Should be 1 Observation --- IDs are off
LES[LES$sponsor %in% c("kennedy, ted jr.", "kennedy, edward, jr."),]$sponsor <- "kennedy, edward jr."

### Manual Fixes --Supplementing
LES[LES$sponsor == 'santamaria, b. scott',]$sponsor <- 'santa-maria, b. scott'
LES[LES$sponsor == 'sanangelo, ronald s.',]$sponsor <- 'san angelo, ronald s.'
LES[LES$sponsor == 'edmondswalker, toni e.',]$sponsor <- 'walker, toni edmonds'
LES[LES$sponsor == 'olson, melissa',]$sponsor <- 'olson-riley, melissa'
LES[LES$sponsor == 'rosati, christine',]$sponsor <- 'randall, christine rosati'
LES[LES$sponsor == 'luxenberg, kelly j. s.',]$sponsor <- 'luxenberg, kelly juleson-scopino'
LES[LES$sponsor == 'dubaydauphinais, anne',]$sponsor <- 'dauphinais, anne dubay'
LES[LES$sponsor == 'pavalock, christine',]$sponsor <- "pavalock-d'amato, cara christine"
# --> Unclear Why name is wrong, no indcication prasad goes by sheenu on web
LES[LES$sponsor == 'srinivasan, sheenu',]$sponsor <- "srinivasan, prasad"
#LES[LES$sponsor == 'zzzzz',]$sponsor <- 'zzzzz'


#########################################################
############ Match to Hall/Fouirnaies
########################################################

library(readstata13)

### Hall and Fournaies 2018, Covers 1988 - 2014 ELECTIONS
### --> https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/PGCVDP
hf_data <- read.dta13("~/Dropbox/Data/Hall_Fouirnaies_2018/How_Do_interest_Groups_Seek_Access_to_Committees.dta")
hf_data <- filter(hf_data, state == this_state)
hf_data <- hf_data[,c(colnames(hf_data)[c(1:3, 7:14, 204, 208, 217)], colnames(hf_data)[grepl('cmt_', colnames(hf_data))])]
hf_data$term <- paste0(hf_data$year + 1, "_", hf_data$year + 2)
hf_data$chamber <- ifelse(hf_data$chamber == 'senate', 'Senate', "House")

#### For 4-yr term states, if NOT staggered: Doubling the Senate Rows + Adding back in
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

# ********** SM Data Covers 1996 -- 2016 for CONNECTICUT ***************

ideo <- readstata13::read.dta13("~/Dropbox/Data/Shor_McCarty_Data/shor_mccarty_1993_2016_individual_data_May_2018.dta")
ideo <- filter(ideo, st == this_state) %>% select(-st_id)
ideo$match_name <- str_extract(tolower(ideo$name), '[a-z]+, [a-z]+')
ideo$match_name <- ifelse(is.na(ideo$match_name), tolower(ideo$name), ideo$match_name)
ideo$last_name <- gsub(',.+', '', tolower(ideo$name))

#### UNTIL SM DATA UPDATE -- DROPPING 2015_2016 Duplicates if needed
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

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches: NONE
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

#### FIX MISMATCHES
# LES[LES$sponsor %in% c('zzzzzzzzz'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('conway', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score) %>% arrange(name)

name_matches <- data.frame(LES_name = 'garcia, edna i.', SM_name = 'GARCIA, E.I.')
# name_matches <- add_row(name_matches, LES_name = 'conway, matthew j. jr.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'esty, elizabeth', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'gerratana, theresa b.', SM_name = 'Gerratana, Terry B.')
# name_matches <- add_row(name_matches, LES_name = 'hornish, annie', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'lambert, barbara l.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'luxenberg, geoff', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'miller, patricia billie', SM_name = 'Miller, Patricia')
name_matches <- add_row(name_matches, LES_name = 'olson-riley, melissa', SM_name = 'Riley, Melissa M.')
name_matches <- add_row(name_matches, LES_name = 'peters, bob', SM_name = 'Peters, Robert')
#name_matches <- add_row(name_matches, LES_name = 'reeves, peggy', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'vahey, cristin mccarthy', SM_name = 'McCarthy Vahey, Cristin')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########

#### Check for Potential Switchers
# filter(LES, party == 'd' & np_score > 0) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & np_score < -.10) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()

# **** A lot of seemingly quite liberal-voting republicans....
# --> Ex 1: Anthony J. Tercyak, np_score == -.563, definitely a R (but succeeded by son as D): https://www.courant.com/news/connecticut/hc-xpm-2003-08-06-0308061248-story.html
# --> Ex 2: Lenny Winkler, np_score == 0.259, seemingly a R: https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=32471

# **** Peter Villano coded as Repub randomly for 2000 Election in Klarner ---> No evidence this is true ****
LES[LES$sponsor == 'villano, peter' & LES$term == "2001_2002",]$party <- 'd'

# **** Diana Urban --- Switched to Democrat in Nov 2006 right after winning re-election as R. ****
LES[LES$sponsor == 'urban, diana s.' & as.numeric(substring(LES$term, 1, 4)) >= 2007,]$party <- 'd'
LES[LES$sponsor == 'urban, diana s.' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Urban, Diana S.' & ideo$party == 'D',]$name
LES[LES$sponsor == 'urban, diana s.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Urban, Diana S.' & ideo$party == 'D',]$party
LES[LES$sponsor == 'urban, diana s.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Urban, Diana S.' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'urban, diana s.' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Urban, Diana' & ideo$party == 'R',]$name
LES[LES$sponsor == 'urban, diana s.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Urban, Diana' & ideo$party == 'R',]$party
LES[LES$sponsor == 'urban, diana s.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Urban, Diana' & ideo$party == 'R',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION
###########################################

### REMOVE NICKNAMES
LES$sponsor <- str_trim(gsub('  +', ' ', gsub('\\(.+\\)', '', LES$sponsor)))
LES$sponsor <- str_trim(gsub('  +', ' ', gsub('\\".+\\"', '', LES$sponsor)))
# table(LES$sponsor)

#### Short Names:
# filter(LES, nchar(sponsor) < 15 & nchar(gsub('^rep. |^sen. ', '', data_name)) > nchar(sponsor)) %>% distinct(sponsor, data_name, klarner_name)
LES[LES$sponsor == "greene, len",]$sponsor <- "greene, leonard"
LES[LES$sponsor == "crisco, joe",]$sponsor <- "crisco, joseph"
LES[LES$sponsor == "smith, win jr.",]$sponsor <- "smith, winthrop jr."
LES[LES$sponsor == "adinolfi, al",]$sponsor <- "adinolfi, alfred"
LES[LES$sponsor == "nafis, sandy",]$sponsor <- "nafis, sandy h."
LES[LES$sponsor == "orourke, jim",]$sponsor <- "o'rourke, james"
LES[LES$sponsor == "aman, bill",]$sponsor <- "aman, william"
LES[LES$sponsor == "russo, robert",]$sponsor <- "russo, robert d."
LES[LES$sponsor == "reeves, peggy",]$sponsor <- "reeves, margaret"
LES[LES$sponsor == "hoydick, laura",]$sponsor <- "hoydick, laura r."
LES[LES$sponsor == "sampson, rob",]$sponsor <- "sampson, robert"
LES[LES$sponsor == "vicino, tom",]$sponsor <- "vicino, thomas"
LES[LES$sponsor == "sredzinski, jp",]$sponsor <- "sredzinski, j.p."
LES[LES$sponsor == "boyd, pat",]$sponsor <- "boyd, patrick"
LES[LES$sponsor == "hall, joshua",]$sponsor <- "hall, joshua malik"
#LES[LES$sponsor == "zzzzzz",]$sponsor <- "zzzzzzz"

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

# **** CODING 1991-1998 IN CASE EXPAND DATA --> Would need to uncomment SENATE ROW ****

LES$in_majority <- 0

### House -- 1991 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1991:2020) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
#LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzz) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1991 - 2020 
# ** Power Co-sharing Agreement in 2017-2018 (MAYBE slight edge to dems, but all chairs are split and R's have proposal power akin to president).. https://web.archive.org/web/20190121020530/https://ctmirror.org/2016/12/22/deal-struck-on-who-will-run-evenly-split-ct-senate/
LES[as.numeric(substring(LES$term,1,4)) %in% c(1991:1994, 1997:2016, 2019:2020) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
#LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:1996) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1
LES[LES$term == "2017_2018" & LES$chamber == "Senate",]$in_majority <- 1

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
}; rm(LES_sub)

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
  facet_wrap(~ chamber) + 
  theme(axis.title.x = element_text(hjust = 0.5),axis.title.y = element_text(hjust = 0.5), legend.position = "bottom")

ggsave(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Figures/{this_state}_LES_ridgeplot.pdf"), height = 8, width = 7)

#########
ggplot(LES, aes(x = np_score, y = LES, color = party)) + 
  geom_point() + 
  geom_smooth(formula = "y ~ x", method = 'lm', se = FALSE) + 
  facet_wrap(~ term) + 
  scale_color_manual(values=c("dodgerblue2", "red2", 'gray50'))

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + in_majority + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')


