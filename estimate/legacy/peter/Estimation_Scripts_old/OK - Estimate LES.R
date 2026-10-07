################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** OKLAHOMA *** BY SESSION
##############################################################
# **************** CAN FILL IN MISSING SPONSORS USING ACTIONS IN MOST CASES *************************

###################################
## (SPECIAL) SESSIONS:
## ---- Special sessions are permitted; bills do not carry over into special sessions --> Bill numbers re-start
## ---- Bills generally carry over across regular sessions, but criteria vary: adjusting based on duplicate IDs and Intro dates in script
## MEMBER LISTS:
## ---- Senate: Click Historic Members: http://www.oksenate.gov/Senators/
## ---- House: https://www.okhouse.gov/Members/Historic.aspx
## PROCESS/RULES:
## ---- Process: 
## ---- Glossary: http://www.oksenate.gov/legislation/glossary.html
## ---- 2019 Senate Rules: http://www.oksenate.gov/publications/senate_rules/2019%20Senate%20Rules.pdf
## ---- 2019 House Rules: https://okhouse.gov/Documents/Rules/57/House%20Rules%20-%2057th%20Oklahoma%20Legislature%20(2019-2020).pdf
## Sponsorship/Authorship
## ---- One primary author from each chamber; Coauthors permitted (from both chambers)
###########################
## NOTES:
## (1) Dropping repeat records of the same bill that get carried over across sessions
## ----> Basically all bills get carried over in the records (even if passed), so treating as one continuous session
## ----> Keeping the record from the SECOND SESSION however, because it includes records from both first and second session
#################################

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

this_state <- 'OK'
min_year <- 1993
max_year <- 2018
keep_types <- c('HB', 'SB')
spec_elec_codes <- c('s', 'gs')
house_term_length <- 2
sen_term_length <- 4 # STAGGERED? YES

#### Output Directory
#dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Terms/Sessions and Filespaths
terms <- seq(min_year, max_year, 2)

data_files <- list.files(glue("~/Dropbox/Data/State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
sessions <- gsub(".+Bill_Details_|.csv", '', bill_files)
rm(data_files, bill_files)

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("~/Dropbox/Data/State Legislative Data/Commem Bills/{this_state}_Commem_Bills.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- read.csv("~/Dropbox/Data/State Legislative Data/Significant Bills/SS_Data/SS_Bills.csv")
SS_bills <- SS_bills %>% 
  filter(state == this_state) %>%
  rename(bill_id = bill_num) %>%
  mutate(term = ifelse(year %% 2 == 1, paste0(year, "_", year + 1), paste0(year - 1, "_", year)),
         bill_id = toupper(bill_id),
         SS = 1) %>%
  select(state, term, year, bill_id, everything())

#### Check SS BIll Types ---> Correct to ensure standardized with Data Format
# table(SS_bills$bill_type)

### Load Klarner Data 
load("~/Dropbox/Data/US Election Data/state_leg/klarner_data/196slers1967to2016_20180908.RData")
klarner <- table; rm(table)
klarner$sab <- toupper(klarner$sab)
klarner <- filter(klarner, year > min_year - 5 & sab == this_state)

#### FIX/DROP PROBLEMATIC KLARNER OBSERVATIONS (Doubles, Multi-race candidates, etc.)
# -- OK Senate -- Myers, David -- ID: 183882 -- Died in 2011... so did not run in 2016
klarner[klarner$cand == 'hobston, cal',]$cand <- 'hobson, cal' # 1 of 4 mispelled
klarner[klarner$cand == 'borwn, mike',]$cand <- 'brown, mike' # 1 of 7 mispelled
### ---> FOR ALL: IDs for corrected name rows will be WRONG **********
# ** Chuck Strohm beat Adbo in runoff: https://www.tulsaworld.com/news/local/government-and-politics/strohm-defeats-abdo-in-house-district/article_87543afd-2257-56fe-9100-f3c965942f06.html
# --> Can't fix here because he's only recorded as losing the primary

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[4]

for(t in terms){
  
  ### Formulate 2-year terms -- Cover both regular and special sessions
  t_yrs <- as.character(glue('{t}_{t+1}'))
  t_sessions <- sessions[grepl(glue('{t}|{t+1}'), sessions)]
  
  ### Skip Previously Estimated
  if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
    print(glue('. \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n.'))
    next
  }
  
  ###### SESSION IN PROGRESS
  print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))
  
  ############### Read in data
  bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_sessions[1]}.csv")
  bills <- read.csv(bill_path)
  
  ### If multiple sessions in different files, read in those as well
  if(length(t_sessions) > 1){
    for(s in t_sessions[2:length(t_sessions)]){
      bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{s}.csv")
      s_bills <- read.csv(bill_path)
      if(nrow(s_bills) == 0){ next }
      bills <- bind_rows(bills, s_bills)
    }
    rm(s, s_bills)
  }
  
  ######## Bills Carryover From Odd to Even Years --> Need to Drop Even-Year Duplicates!
  # --> Need to be careful to not drop special session bills however (where duplciates != same bill)
  # --> e.g, SB1 introduced in odd year, carries over to even year, but first special session bill will be called SB1
  # --> Bills in CONFERENCE committee do not necessarily carry over:
  # --> See most recent rules: http://www.oksenate.gov/publications/senate_rules/2019%20Senate%20Rules.pdf
  dup_bills <- c()
  for(i in 1:nrow(bills)){
    if(bills[i,]$session == paste0(t, "-RS")){
      bill_sub <- filter(bills, bill_id == bills[i,]$bill_id & grepl('RS', session))
      ## Checking if even year bill has same title (if there is an even bill with that id)
      if(nrow(bill_sub) > 1 & length(unique(bill_sub$intro_date)) == 1){
        dup_bills <- append(dup_bills, bills[i,]$bill_id)
      }
    }
  }
  
  #### Dropping the 1993 Duplicates (in some cases the second session page has more detailed actions)
  #### AND readujusting the session dates to align with introduction
  # ---> Keeping Original Session to Match to Bill History Data
  bills <- filter(bills, !(bill_id %in% dup_bills & session == paste0(t, "-RS")))
  bills$new_date <- as.numeric(substring(bills$intro_date, 1, 4))
  bills$new_date <- ifelse(bills$new_date < t, t, bills$new_date) # Adjusting for Dec T-1 Bills
  bills$new_date <- ifelse(bills$new_date >= t + 2, t + 1, bills$new_date) # Adjusting for Jan T+2 Bills
  bills$new_session <- ifelse(grepl('-RS', bills$session), paste0(bills$new_date, "-RS"), bills$session)
  bills <- rename(bills, session_orig = session)
  bills$session <- ifelse(bills$new_session == '-RS' | bills$new_session == "NA-RS", bills$session_orig, bills$new_session)
  bills <- select(bills, -c(new_session, new_date))
  rm(dup_bills, bill_sub, i)
  
  ######## Add Term Var + Standardize the Bill IDs
  bills <- bills %>%
    mutate(term = t_yrs,
           bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('^[A-Z]+', '', bill_id), 4, pad = '0')) ) %>%
    arrange(session, bill_id)
    
  ############### Drop Resolutions, Messages, Communications, Reports
  bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
  # table(bills$bill_type)
  bills <- filter(bills, bill_type %in% keep_types) 
  
  ###### Drop "Test" Bills for Online System (2015+, seemingly)
  bills <- filter(bills, !grepl("^test$|this is a test (bill|request)", tolower(title)))
  
  ##########################
  ####### Standardize Sponsors
  
  # bills$H_author <- gsub('\\(H\\)', ' (H)', bills$H_author)
  # bills$S_author <- gsub('\\(S\\)', ' (S)', bills$S_author)
  
  bills$H_author <- tolower(bills$H_author)
  bills$H_author <- gsub('á', 'a', bills$H_author)
  bills$H_author <- gsub('é', 'e', bills$H_author)
  bills$H_author <- gsub('ó', 'o', bills$H_author)
  bills$H_author <- gsub('í', 'i', bills$H_author)
  bills$H_author <- gsub('ñ', 'n', bills$H_author)
  
  bills$S_author <- tolower(bills$S_author)
  bills$S_author <- gsub('á', 'a', bills$S_author)
  bills$S_author <- gsub('é', 'e', bills$S_author)
  bills$S_author <- gsub('ó', 'o', bills$S_author)
  bills$S_author <- gsub('í', 'i', bills$S_author)
  bills$S_author <- gsub('ñ', 'n', bills$S_author)
  
  bills$coauthors <- tolower(bills$coauthors)
  bills$coauthors <- gsub('á', 'a', bills$coauthors)
  bills$coauthors <- gsub('é', 'e', bills$coauthors)
  bills$coauthors <- gsub('ó', 'o', bills$coauthors)
  bills$coauthors <- gsub('í', 'i', bills$coauthors)
  bills$coauthors <- gsub('ñ', 'n', bills$coauthors)
  bills$coauthors <- gsub('^none$', '', bills$coauthors)
  
  ### Fix Missing + Errors
  if(t_yrs == '1993_1994'){
    # **** MISSING SPONSORS
    bills[bills$session == "1993-RS" & bills$bill_id %in% c("SB0039"),]$S_author <- 'stipe (s)'
    bills[bills$session == "1993-RS" & bills$bill_id %in% c("SB0307"),]$S_author <- 'mickle (s)'
    bills[bills$session == "1993-RS" & bills$bill_id %in% c("SB0332"),]$S_author <- 'long (lewis) (s)' 
    bills[bills$session == "1993-RS" & bills$bill_id %in% c("SB0349"),]$S_author <- 'haney (s)'
    # ## SB475 + SB488 = Author changed right before passage...? Weird
    bills[bills$session == "1993-RS" & bills$bill_id %in% c("SB0475"),]$S_author <- 'williams (don) (s)'
    bills[bills$session == "1993-RS" & bills$bill_id %in% c("SB0488"),]$S_author <- 'williams (penny) (s)'
    bills[bills$session == "1994-RS" & bills$bill_id %in% c("HB2122"),]$H_author <- 'bryant (james sears) (h)' 
    bills[bills$session == "1994-RS" & bills$bill_id %in% c("SB0799"),]$S_author <- 'helton (s)'
    bills[bills$session == "1994-RS" & bills$bill_id %in% c("SB1130"),]$S_author <- 'williams (don) (s)' 
    #### NAME ERRORS
    bills[bills$S_author == 'coleman (s)',]$S_author <- 'cole (s)'
    bills$coauthors <- gsub('coleman \\(s\\)', 'cole (s)', bills$coauthors)
    bills[bills$H_author == 'johnson (h)',]$H_author <- 'johnson (glen) (h)'
    bills$coauthors <- gsub('johnson \\(h\\)', 'johnson (glen) (h)', bills$coauthors)
    bills[bills$S_author == 'williams (s)',]$S_author <- 'williams (penny) (s)'
    bills$coauthors <- gsub('williams \\(s\\)', 'williams (penny) (s)', bills$coauthors)
    bills[bills$S_author == 'long (s)',]$S_author <- 'long (lewis) (s)'
    bills$coauthors <- gsub('long \\(s\\)', 'long (lewis) (s)', bills$coauthors)    
    bills[bills$H_author == 'boyd (h)',]$H_author <- 'boyd (betty) (h)'
    bills$coauthors <- gsub('boyd \\(h\\)', 'boyd (betty) (h)', bills$coauthors)
    bills[bills$H_author == 'bryant (h)',]$H_author <- 'bryant (john) (h)'
    bills$coauthors <- gsub('bryant \\(h\\)', 'bryant (john) (h)', bills$coauthors)
    bills[bills$H_author == 'hamilton (h)',]$H_author <- 'hamilton (james) (h)'
    bills$coauthors <- gsub('hamilton \\(h\\)', 'hamilton (james) (h)', bills$coauthors)
    bills[bills$H_author == 'smith (h)',]$H_author <- 'smith (dale) (h)'
    bills$coauthors <- gsub('smith \\(h\\)', 'smith (dale) (h)', bills$coauthors)    
    bills[bills$H_author == 'vaughn (h)',]$H_author <- 'vaughn (ray) (h)'
    bills$coauthors <- gsub('vaughn \\(h\\)', 'vaughn (ray) (h)', bills$coauthors)    
    bills$coauthors <- gsub('stites \\(j.t.\\) \\(h\\)', 'stites (h)', bills$coauthors)
  }else if(t_yrs == '1995_1996'){
    bills[bills$H_author == 'boyd (h)',]$H_author <- 'boyd (betty) (h)'
    bills$coauthors <- gsub('boyd \\(h\\)', 'boyd (betty) (h)', bills$coauthors)
    bills[bills$H_author == 'pope (h)',]$H_author <- 'pope (clay) (h)'
    bills$coauthors <- gsub('pope \\(h\\)', 'pope (clay) (h)', bills$coauthors)    
    bills[bills$H_author == 'smith (h)',]$H_author <- 'smith (dale) (h)'
    bills$coauthors <- gsub('smith \\(h\\)', 'smith (dale) (h)', bills$coauthors)    
    bills[bills$H_author == 'sullivan (h)',]$H_author <- 'sullivan (leonard) (h)'
    bills$coauthors <- gsub('sullivan \\(h\\)', 'sullivan (leonard) (h)', bills$coauthors)    
    bills[bills$S_author == 'long (s)',]$S_author <- 'long (lewis) (s)'
    bills$coauthors <- gsub('long \\(s\\)', 'long (lewis) (s)', bills$coauthors)    
    bills[bills$S_author == 'williams (s)',]$S_author <- 'williams (penny) (s)'
    bills$coauthors <- gsub('williams \\(s\\)', 'williams (penny) (s)', bills$coauthors)
    bills$coauthors <- gsub('stites \\(j.t.\\) \\(h\\)', 'stites (h)', bills$coauthors)
    bills[bills$bill_id == "HB2362" & bills$session == "1996-RS",]$H_author <- 'morgan (fred) (h)'
    bills[bills$bill_id == "HB2561" & bills$session == "1996-RS",]$H_author <- 'thornbrugh (h)'
    ## Mike Johnson in House (elected to Senate in 1999)
    bills$coauthors <- gsub('johnson \\(mike\\) \\(s\\)', 'johnson \\(mike\\) \\(h\\)', bills$coauthors)
  }else if(t_yrs == '1997_1998'){
    bills[bills$H_author == 'boyd (h)',]$H_author <- 'boyd (betty) (h)'
    bills$coauthors <- gsub('boyd \\(h\\)', 'boyd (betty) (h)', bills$coauthors)
    bills[bills$H_author == 'pope (h)',]$H_author <- 'pope (clay) (h)'
    bills$coauthors <- gsub('pope \\(h\\)', 'pope (clay) (h)', bills$coauthors)    
    bills[bills$H_author == 'smith (h)',]$H_author <- 'smith (dale) (h)'
    bills$coauthors <- gsub('smith \\(h\\)', 'smith (dale) (h)', bills$coauthors)    
    bills[bills$H_author == 'sullivan (h)',]$H_author <- 'sullivan (leonard) (h)'
    bills$coauthors <- gsub('sullivan \\(h\\)', 'sullivan (leonard) (h)', bills$coauthors)    
    bills$coauthors <- gsub('stites \\(j.t.\\) \\(h\\)', 'stites (h)', bills$coauthors)
    bills[bills$bill_id == "HB3044" & bills$session == "1998-RS",]$H_author <- 'ervin (h)'
    bills[bills$bill_id == "SB0323" & bills$session == "1997-RS",]$H_author <- 'langmacher (h)'
  }else if(t_yrs == '1999_2000'){
    bills[bills$H_author == 'morgan (h)',]$H_author <- 'morgan (fred) (h)' # Name not on 1999-SS1 bills
    bills[bills$H_author == 'pope (h)',]$H_author <- 'pope (clay) (h)'
    bills$coauthors <- gsub('pope \\(h\\)', 'pope (clay) (h)', bills$coauthors)    
    bills[bills$H_author == 'smith (h)',]$H_author <- 'smith (dale) (h)'
    bills$coauthors <- gsub('smith \\(h\\)', 'smith (dale) (h)', bills$coauthors)    
    bills[bills$H_author == 'sullivan (h)',]$H_author <- 'sullivan (leonard) (h)'
    bills$coauthors <- gsub('sullivan \\(h\\)', 'sullivan (leonard) (h)', bills$coauthors)    
    bills$coauthors <- gsub('stites \\(j.t.\\) \\(h\\)', 'stites (h)', bills$coauthors)
  }else if(t_yrs == '2001_2002'){
    bills[bills$H_author == 'pope (h)',]$H_author <- 'pope (clay) (h)'
    bills$coauthors <- gsub('pope \\(h\\)', 'pope (clay) (h)', bills$coauthors)    
    bills[bills$H_author == 'smith (h)',]$H_author <- 'smith (dale) (h)'
    bills$coauthors <- gsub('smith \\(h\\)', 'smith (dale) (h)', bills$coauthors)    
    bills[bills$H_author == 'sullivan (h)',]$H_author <- 'sullivan (leonard) (h)'
    bills$coauthors <- gsub('sullivan \\(h\\)', 'sullivan (leonard) (h)', bills$coauthors)    
    bills$H_author <- gsub("o'neal", 'oneal', bills$H_author)
    bills$coauthors <- gsub("o'neal", 'oneal', bills$coauthors)
  }else if(t_yrs == '2003_2004'){
    bills[bills$H_author == 'morgan (h)',]$H_author <- 'morgan (danny) (h)'
    bills$coauthors <- gsub('morgan \\(h\\)', 'morgan (danny) (h)', bills$coauthors)
    bills[bills$H_author == 'smith (h)',]$H_author <- 'smith (dale) (h)'
    bills$coauthors <- gsub('smith \\(h\\)', 'smith (dale) (h)', bills$coauthors)
    bills[bills$H_author == 'peterson (h)',]$H_author <- 'peterson (pam) (h)'
    bills$coauthors <- gsub('peterson \\(h\\)', 'peterson (pam) (h)', bills$coauthors)
    bills$H_author <- gsub("o'neal", 'oneal', bills$H_author)
    bills$coauthors <- gsub("o'neal", 'oneal', bills$coauthors)
  }else if(t_yrs == '2005_2006'){
    ## H and S coauthor named johnson --> scraper can't parse which one to add first name to
    bills[bills$H_author == 'johnson (h)',]$H_author <- 'johnson (rob) (h)' 
    ### Missing First Names -- Two X's, one with first, one without
    bills[bills$H_author == 'morgan (h)',]$H_author <- 'morgan (danny) (h)'
    bills$coauthors <- gsub('morgan \\(h\\)', 'morgan (danny) (h)', bills$coauthors)
    bills[bills$H_author == 'peterson (h)',]$H_author <- 'peterson (pam) (h)'
    bills$coauthors <- gsub('peterson \\(h\\)', 'peterson (pam) (h)', bills$coauthors)
    bills[bills$H_author == 'miller (h)',]$H_author <- 'miller (ken) (h)'
    bills$coauthors <- gsub('miller \\(h\\)', 'miller (ken) (h)', bills$coauthors)
  }else if(t_yrs == '2007_2008'){
    ## H and S coauthor named johnson --> scraper can't parse which one to add first name to
    bills[bills$bill_id == 'HB1451' & bills$session == '2007-RS', c("H_author", "S_author")] <- c('johnson (rob) (h)', 'johnson (mike) (s)')
    bills[bills$bill_id == 'SB1383' & bills$session == '2008-RS', c("H_author", "S_author")] <- c('johnson (rob) (h)', 'johnson (mike) (s)')  
    ### Missing First Names -- Two X's, one with first, one without
    bills[bills$H_author == 'martin (h)',]$H_author <- 'martin (scott) (h)'
    bills$coauthors <- gsub('martin \\(h\\)', 'martin (scott) (h)', bills$coauthors)
    bills[bills$H_author == 'johnson (h)',]$H_author <- 'johnson (dennis) (h)'
    bills$coauthors <- gsub('johnson \\(h\\)', 'johnson (dennis) (h)', bills$coauthors)
    bills[bills$H_author == 'peterson (h)',]$H_author <- 'peterson (pam) (h)'
    bills$coauthors <- gsub('peterson \\(h\\)', 'peterson (pam) (h)', bills$coauthors)
    bills[bills$H_author == 'mcdaniel (h)',]$H_author <- 'mcdaniel (randy) (h)'
    bills$coauthors <- gsub('mcdaniel \\(h\\)', 'mcdaniel (randy) (h)', bills$coauthors)
  }else if(t_yrs == '2009_2010'){
    bills[bills$H_author == 'martin (h)',]$H_author <- 'martin (scott) (h)'
    bills$coauthors <- gsub('martin \\(h\\)', 'martin (scott) (h)', bills$coauthors)
    bills[bills$H_author == 'wright (h)',]$H_author <- 'wright (harold) (h)'
    bills$coauthors <- gsub('wright \\(h\\)', 'wright (harold) (h)', bills$coauthors)
    bills[bills$H_author == 'mcdaniel (h)',]$H_author <- 'mcdaniel (randy) (h)'
    bills$coauthors <- gsub('mcdaniel \\(h\\)', 'mcdaniel (randy) (h)', bills$coauthors)
  }else if(t_yrs == '2011_2012'){
    bills[bills$H_author == 'martin (h)',]$H_author <- 'martin (scott) (h)'
    bills$coauthors <- gsub('martin \\(h\\)', 'martin (scott) (h)', bills$coauthors)
    ## 3 McDaniels: Jeannie has name added, Randall doesn't, Curtis wins special (name included, but only coauthors)
    bills[bills$H_author == 'mcdaniel (h)',]$H_author <- 'mcdaniel (randy) (h)'
    bills$coauthors <- gsub('mcdaniel \\(h\\)', 'mcdaniel (randy) (h)', bills$coauthors)
  }else if(t_yrs == '2013_2014'){
    bills[bills$H_author == 'martin (h)',]$H_author <- 'martin (scott) (h)'
    bills$coauthors <- gsub('martin \\(h\\)', 'martin (scott) (h)', bills$coauthors)
    bills[bills$H_author == 'mcdaniel (h)',]$H_author <- 'mcdaniel (randy) (h)'
    bills$coauthors <- gsub('mcdaniel \\(h\\)', 'mcdaniel (randy) (h)', bills$coauthors)
    ### Random Coauthor rows include first name despite not being necessary
    bills$coauthors <- gsub('bennett \\(h\\)', 'bennett (john) (h)', bills$coauthors)
    bills$coauthors <- gsub('coody \\(h\\)', 'coody (ann) (h)', bills$coauthors)
    bills$coauthors <- gsub('osborn \\(h\\)', 'osborn (leslie) (h)', bills$coauthors)
    bills$H_author <- gsub("o'donnell", 'odonnell', bills$H_author)
    bills$coauthors <- gsub("o'donnell", 'odonnell', bills$coauthors)
  }else if(t_yrs == '2015_2016'){
    bills[bills$H_author == 'mcdaniel (h)',]$H_author <- 'mcdaniel (randy) (h)'
    bills$coauthors <- gsub('mcdaniel \\(h\\)', 'mcdaniel (randy) (h)', bills$coauthors)
    bills[bills$H_author == 'coody (h)',]$H_author <- 'coody (jeff) (h)'
    bills$coauthors <- gsub('coody \\(h\\)', 'coody (jeff) (h)', bills$coauthors)
    ### Spelling Error
    bills[bills$H_author == 'leewrighth (h)',]$H_author <- 'leewright (h)'
    bills$coauthors <- gsub('leewrighth \\(h\\)', 'leewright (h)', bills$coauthors)
    ### Random Coauthor rows include first name despite not being necessary
    bills$coauthors <- gsub('bennett \\(h\\)', 'bennett (john) (h)', bills$coauthors)
    bills$coauthors <- gsub('caldwell \\(h\\)', 'caldwell (chad) (h)', bills$coauthors)
    bills$coauthors <- gsub('hardin \\(h\\)', 'hardin (tommy) (h)', bills$coauthors)
    bills$coauthors <- gsub('osborn \\(h\\)', 'osborn (leslie) (h)', bills$coauthors)
    bills$H_author <- gsub("o'donnell", 'odonnell', bills$H_author)
    bills$coauthors <- gsub("o'donnell", 'odonnell', bills$coauthors)
  }else if(t_yrs == '2017_2018'){
    bills[bills$H_author == 'bennett (h)',]$H_author <- 'bennett (forrest) (h)'
    bills$coauthors <- gsub('bennett \\(h\\)', 'bennett (forrest) (h)', bills$coauthors)
    bills[bills$H_author == 'ford (h)',]$H_author <- 'ford (ross) (h)'
    bills$coauthors <- gsub('ford \\(h\\)', 'ford (ross) (h)', bills$coauthors)
    bills$H_author <- gsub("o'donnell", 'odonnell', bills$H_author)
    bills$coauthors <- gsub("o'donnell", 'odonnell', bills$coauthors)
    ### Drop 'Not Found' Coauthor
    bills[bills$bill_id == "HB1267",]$coauthors <- "bennett (john) (h)"
  }
  # filter(bills, grepl('ford \\(h', H_author)) %>% select(bill_url)
  
  ### Getting the Author from the Initiating Chamber
  # filter(bills, chamber_author == "") %>% View()
  bills$chamber_author <- ifelse(substring(bills$bill_id,1,1) == "H", bills$H_author,
                                 ifelse(substring(bills$bill_id,1,1) == "S", bills$S_author, ''))
  
  ### LES Sponsor Var
  bills$LES_sponsor <- gsub(' \\(h\\)| \\(s\\)', '', bills$chamber_author)
  # table(bills$LES_sponsor)
  
  #### Adjusting for Bills By Request -- Need to do BEFORE splitting
  if(any(grepl("request", bills$LES_sponsor))){
    print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
  }
  
  ###### Bills sponsored by committee
  if(nrow(filter(bills, grepl('committee', LES_sponsor))) > 0 ){
    cat('\n')
    cat(glue("-----> Dropping {nrow(filter(bills, grepl('committee', LES_sponsor))) } bill(s) sponsored by committee"))
    bills <- filter(bills, !grepl('committee', LES_sponsor))     
  }
  
  ###### CHeck Missing Sponsors
  # filter(bills, LES_sponsor == "" | is.na(LES_sponsor)) %>% View()
  if(nrow(filter(bills, LES_sponsor == '')) > 0 | any(is.na(bills$LES_sponsor))){
    cat('\n')
    cat(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == '')) + sum(is.na(bills$LES_sponsor))} bill(s) without a sponsor"))
    bills <- filter(bills, !(LES_sponsor == '' | is.na(LES_sponsor)))     
  }

  
  ###################
  ###### Merge in S&S Bills
  ###################
  # *** For NORTH CAROLINA: Bills carry over during regular (one biennium), but numbers re-start for all special sessions
  # ---> Need to merge on Id and Session 
  # ---> Capping Bill numbers at SESSION MAX and assuming bill is from SS with most bills proposed
  SS_term <- SS_bills %>% filter(term == t_yrs) %>% mutate(H_max = NA, S_max = NA, s_spec = NA, any_specials = any(grepl(paste0("-SS"), bills$session)))
  
  ## Adjusting Max Specials by Year
  for(year in as.numeric(str_split(t_yrs, "_")[[1]]) ){
    if(any(grepl(paste0(year, "-SS"), bills$session))){
      H_max <- filter(bills, grepl(year, session) & grepl("^H", bill_id) & grepl("SS", session)) %>% mutate(num = as.numeric(gsub('[A-Z]+', '', bill_id))) %>% pull(num)
      H_max <- ifelse(length(H_max) >= 1, max(H_max), NA)
      S_max <- filter(bills, grepl(year, session) & grepl("^S", bill_id) & grepl("SS", session)) %>% mutate(num = as.numeric(gsub('[A-Z]+', '', bill_id))) %>% pull(num)
      S_max <- ifelse(length(S_max) >= 1, max(S_max), NA)
      which_spec <- which(table(bills[grepl(year, bills$session) & grepl("SS", bills$session),]$session) == max(table(bills[grepl(year, bills$session) & grepl("SS", bills$session),]$session)))[1]
      which_spec <- names(table(bills[grepl(year, bills$session) & grepl("SS", bills$session),]$session)[which_spec])
      SS_term[SS_term$year == year,]$H_max <- H_max
      SS_term[SS_term$year == year,]$S_max <- S_max
      SS_term[SS_term$year == year,]$s_spec <- which_spec
      rm(H_max, S_max, which_spec)
    }
  }
  SS_term <- SS_term %>%
    mutate(special = ifelse((grepl("^H", bill_id) & is.na(H_max)) | (grepl("^S", bill_id) & is.na(S_max)), 0, special),
           special = ifelse(!any_specials, 0, special),
           special = ifelse(grepl("^H", bill_id) & !is.na(H_max) & special == 1 & num_only > H_max, 0, special),
           special = ifelse(grepl("^S", bill_id) & !is.na(S_max) & special == 1 & num_only > S_max, 0, special),
           session = ifelse(special == 0, paste0(year, '-RS'), ifelse(is.na(special_num), s_spec, paste0(year, '-SS', special_num)))) %>%
    distinct(term, session, bill_id, SS)
  
  ### Merge
  bills <- bills %>%
    left_join(SS_term, by = c("bill_id", "term", "session")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  
  #### Loop Through Unmerged And Check if Session is off by 1 year... Code as SS if carried over
  # ---> Only doing this for bills that are introduced and mentioned at T+1 (e.g., the carryover bills)
  unmatched <- anti_join(SS_term, bills, by = c("bill_id", "term", "session")) %>% filter(session == paste0(substring(t_yrs, 6, 9), '-RS'))
  for(i in 1:nrow(unmatched)){
    if(nrow(filter(bills, bill_id == unmatched[i,]$bill_id & !grepl("SS", unmatched[i,]$session))) == 1){
      bills[bills$bill_id == unmatched[i,]$bill_id & !grepl("SS", unmatched[i,]$session),]$SS <- 1
      SS_term[SS_term$bill_id %in% unmatched[i,]$bill_id & SS_term$session %in% unmatched[i,]$session,]$session <- paste0(substring(t_yrs, 1, 4), '-RS')
    }
  }
  
  ### Drop Duplicates if Above Process Created Any
  SS_term <- distinct(SS_term)
  
  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id", "term", "session"))
  rm(unmatched)
  
  ############################################
  ############### Code Commemorative
  ############################################
  
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, title, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'title'))
  # table(bills$commem)
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  ############################################
  ############### Code Bill History
  ############################################
  
  bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
  bill_hist <- read.csv(bill_hist_path)
  bill_hist$journal_page <- as.character(bill_hist$journal_page)
  
  # If multiple sessions, read in those as well
  if(length(t_sessions) > 1){
    for(s in t_sessions[2:length(t_sessions)]){
      bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
      s_hist <- read.csv(bill_path)
      if(nrow(s_hist) == 0){ next }
      s_hist$journal_page <- as.character(s_hist$journal_page)
      bill_hist <- bind_rows(bill_hist, s_hist)
    }
    rm(s, s_hist)
  }
  
  ######## Clean Term/Session Variables + Standardize the Bill IDs
  bill_hist <- bill_hist %>%
    mutate(term = t_yrs,
           bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('^[A-Z]+', '', bill_id), 4, pad = '0')) ) %>%
      arrange(session, bill_id)
    
  ### Order by Order
  bill_hist <- arrange(bill_hist, term, session, bill_id, action_date, order) 
  
  ### Re-Coding Chamber Variable
  bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate", "G" = "Governor", "CC" = "Conference")
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  # ** per rules, in House: a successful do not pass vote/CR constitues final action on bill
  # ** in senate: do not pass not permitted recommendation; failed do pass (as amended) = final action
  # ** Not including subcomm refs like "Referred to Appr/Sub-Finance" as AIC
  aic_t <- c('^cr', 'do pass', 'do not pass', 'committee substitute', '^reported', 'recommendation to the full comm', '; cr filed')
  # ** Second reading occurs before committee referral
  # ** Not coding do not pass as advancing beyond committee
  abc_t <- c('do pass', 'general order', '^amended', '^advanced', 'engrossed', 'third reading', 'considered')
  pc_t  <- c('measure passed', 'emergency passed', 'measure and emergency passed', 'engrossed.+to (senate|house)')
  law_t <- c('approved by governor', "becomes law without governor")

  ### Check Actions
  # filter(bill_hist, grepl('governor', tolower(action))) %>% distinct(action) %>% View()
  # filter(bill_hist, grepl('effective date', tolower(action))) %>% group_by(action) %>% summarize(n = n()) %>% arrange(desc(n)) %>% View()
  # bill_hist[bill_hist$bill_id == "H5549",]
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
                           bill_url = character(0),
                           title = character(0))
  
  ### Make Sure No Excess text in Bill Action
  bill_hist$action <- str_trim(bill_hist$action)
  
  ### Code History Stages for Each Bill --- Subset Hist File, Code, Save, Repeat
  options(warn = 2)
  for(i in 1:nrow(bills)){
    b_id = bills[i,]$bill_id
    s_orig = bills[i,]$session_orig
    s_id = bills[i,]$session
    b_spon = bills[i,]$LES_sponsor
    hist_sub <- filter(bill_hist, bill_id == b_id, term == t_yrs, session == s_orig)
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
    bill_stages$bill_url <- bills[i,]$bill_url
    bill_stages$title <- bills[i,]$title
    ##### Check if Passed Without/Over Governor Veto
    if(bill_stages$law == 0 & grepl('^filed with secretary of state', tolower(hist_sub[nrow(hist_sub),]$action))){
      bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- bill_stages$law <- 1
    }
    #### Check if Passed Chamber
    if(bill_stages$passed_chamber == 0 & any(grepl('veto|enrolled|sent to governor|ccr|conference', tolower(hist_sub$action))) ){
      bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
    }
    all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
    # print(i)
  }
  options(warn = 1)
  
  ### Check Codings
  cat('\n')
  all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
    summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% as.data.frame() %>% print()
  # filter(bill_hist, session == '2004-RS' & bill_id %in% all_bill_stages[all_bill_stages$session == '2004-RS' & all_bill_stages$action_in_comm == 0,]$bill_id) %>% View()
  # filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 1,]$bill_id) %>% View()
  
  ### MERGE to BILL DATA
  bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 
  
  ### MERGE In COMMEMS
  all_bill_stages <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, title, commem) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term', 'title')) %>%
    select(-title)
   
  ### MERGE In S&S
  all_bill_stages <- SS_term %>% 
    select(bill_id, term, session, SS) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term', "session")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))
  
  ### Adjust Commems if SS == 1
  # table(all_bill_stages$SS, all_bill_stages$commem)
  all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
  rm(SS_term)
  
  
  ### Save Stage Info **** MERGE WITH SS **********
  if(!dir.exists(glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}'))){
    dir.create(glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}'))
  }
  write.csv(all_bill_stages, glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv'), row.names = FALSE )
  
  rm(bill_hist_path, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist, aic_t, abc_t, pc_t, law_t)
  rm(b_id, s_id, s_orig, b_spon, bill_hist)
  
  ####################################################
  ############### Identify Unique Legislators via SLER
  ####################################################
  
  ## Import and Clean Sponsors Name to Match
  all_sponsors <- bills %>%
    mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'H', "H", "S")) %>%
    select(LES_sponsor, chamber_author, chamber, passed_chamber, law) %>%
    mutate(term = t_yrs) %>%
    group_by(LES_sponsor, chamber_author, chamber, term) %>%
    summarize(num_sponsored_bills = n(), 
              sponsor_pass_rate = sum(passed_chamber) / n(),
              sponsor_law_rate = sum(law) / n(),
              num_cosponsored_bills = NA) %>%
    ungroup()
  
  #### Adding Legislators Who COSPONSORED A BILL but did not SPONSOR
  unique_cospon <- str_trim(unique(unlist(str_split(bills$coauthors, '; '))))
  ## Adjusting for 1999 Special which uses old format --> Issues
  if(t_yrs == "1999_2000"){
    unique_cospon <- str_trim(unique(unlist(str_split(bills[bills$session != '1999-SS1',]$coauthors, '; '))))
  }
  for(nonspon in unique_cospon){
    if(!(nonspon %in% all_sponsors$chamber_author) & nonspon != ''){
      chamb <- toupper(gsub('\\(|\\)', '', str_extract(nonspon, '\\((h|s)\\)')))
      all_sponsors <- add_row(all_sponsors, LES_sponsor = gsub(' \\((h|s)\\)', '', nonspon), chamber_author = nonspon, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0)
    }
  }

  ######## Cosponsorship Info 
  bills$cospon_match <- paste(bills$chamber_author, bills$coauthors, sep = "; ")
  for(i in 1:nrow(all_sponsors)){
    c_sub <- bills[substring(bills$bill_id, 1, 1) %in% ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S'),]
    sn <- all_sponsors[i,]$chamber_author
    ## NEED TO ADD ESCAPE CHARACTERS
    sn <- gsub('\\(', '\\\\(', gsub('\\)', '\\\\)', sn))
    ## NEED TO ACCOUNT FOR OVERLAPPING NAMES
    search_term <- paste0("^", sn, '$|^', sn, ';|; ', sn, '$|; ', sn, ';')
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(search_term, tolower(c_sub$cospon_match)))
    ### ONLY need to adjust this way if sponsored and cosponsored column are the same
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  }
  bills <- select(bills, -cospon_match)
  #View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))

  #######################
  #### CLEAN NAMES
  all_sponsors$first_name <- str_extract(all_sponsors$LES_sponsor, '\\([a-z]+\\)')
  all_sponsors$first_name <- ifelse(is.na(all_sponsors$first_name), '', gsub('\\(|\\)', '', all_sponsors$first_name))
  all_sponsors$last_name <- gsub(' \\(.+', '', all_sponsors$LES_sponsor)
  
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  
  #### Update Last Names for Matching 
  if(t == 1993){
    all_sponsors[all_sponsors$LES_sponsor == "bryant (james sears)",]$first_name <- 'james'
  }
  if(t >= 1993 & t <= 2004){
    all_sponsors[all_sponsors$LES_sponsor == 'horner',]$last_name <-  "cisselhorner"
  }
  if(t >= 2005 & t <= 2012){
    all_sponsors[all_sponsors$LES_sponsor == 'eason mcintyre',]$last_name <-  "mcintyre"
  }
  if(t >= 2007 & t <= 2016){
    all_sponsors[all_sponsors$LES_sponsor == 'mcdaniel (randy)',]$first_name <-  "john"
  }
  
  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  H_elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  S_elec_year <- H_elec_year - 2 # Staggerred 4-Year Terms
  
  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% H_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% S_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz, middle) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)
  
  ############################################
  ############## Match Sponsors Names to Klarner Data
  ############################################
  
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), '')))
  klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]+')))  
  
  ### Subset out General Specials
  klarner_gs <- filter(klarner_sub, etype == 'gs')
  klarner_sub <- filter(klarner_sub, etype != 'gs')
  
  ### Drop Duplicated Klarner Candidats
  klarner_sub <- arrange(klarner_sub, desc(year)) %>% group_by(sen) %>% filter(!duplicated(cand)) %>% ungroup()
  
  all_sponsors$elec_year <- all_sponsors$klarner_id <- all_sponsors$klarner_name <- NA
  all_sponsors$klarner_name <- as.character(all_sponsors$klarner_name)
  all_sponsors$klarner_id <- as.double(all_sponsors$klarner_id)
  all_sponsors$elec_year <- as.double(all_sponsors$elec_year)
  
  ### Loop though and Match
  for(i in 1:nrow(all_sponsors)){
    k_matches <- filter(klarner_sub, last_name == tolower(all_sponsors[i,]$last_name)  & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
    
    ## Acccount for lack of punctuation or spaces in Klarner Data
    if(nrow(k_matches) == 0 & grepl("-|'| |\\.|`", all_sponsors[i,]$last_name)){
      k_matches <- filter(klarner_sub, last_name == gsub("-|'| ||`", '', tolower(all_sponsors[i,]$last_name)) & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
    }
    
    ## Check Partial Names (e.g., maiden_name-last_name)
    if(nrow(k_matches) == 0 & grepl("-", all_sponsors[i,]$last_name)){
      k_matches <- filter(klarner_sub, last_name == gsub(".+-", '', tolower(all_sponsors[i,]$last_name)))
    }  
    
    ## Check GS 
    if(nrow(k_matches) == 0 & nrow(klarner_gs) >= 1){
      k_matches <- filter(klarner_gs, last_name == tolower(all_sponsors[i,]$last_name)  & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
      if(nrow(k_matches) == 0){
        k_matches <- filter(klarner_gs, last_name == gsub("-|'| ||`", '', tolower(all_sponsors[i,]$last_name)))
      }
    }
    
    ### Save and Cross-Check
    if(nrow(k_matches) == 1){
      all_sponsors[i, ]$klarner_name <- k_matches$cand
      all_sponsors[i, ]$klarner_id <- k_matches$candid
      all_sponsors[i, ]$elec_year <- k_matches$year     
    } else if(nrow(k_matches) > 1 & length(unique(k_matches$cand)) != 1){
      ## Check Last, First
      m_sub <- grep(all_sponsors[i,]$match_name, k_matches$match_name)
      ### Check Without Punctuation --- Can't remove spaces unless do it for all_sponsors and k_matches
      if(length(m_sub) == 0){
        m_sub <- grep(gsub("-|'|`", '', all_sponsors[i,]$match_name), k_matches$match_name)        
      }
      ## Check First Initial 
      if(length(m_sub) == 0){
        match_name2 <- tolower(paste0(all_sponsors[i,]$last_name, ", ", substring(all_sponsors[i,]$first_name, 1, 1) ))
        m_sub <- grep(match_name2, tolower(unlist(str_extract(k_matches$cand, '[a-zA-z]+, [a-zA-z]'))))
      }
      ### Save if Match
      if(length(m_sub) == 1){
        all_sponsors[i, ]$klarner_name <- k_matches[m_sub,]$cand
        all_sponsors[i, ]$klarner_id <- k_matches[m_sub,]$candid
        all_sponsors[i, ]$elec_year <- k_matches[m_sub,]$year     
      } else{
        print(glue("MULTIPLE MATCHES ::: {all_sponsors[i,]$LES_sponsor} ::: {i} ::: CHAMBER: {all_sponsors[i,]$chamber}"))
      }
    } else if(nrow(k_matches) > 1 & length(unique(k_matches$cand)) == 1){
      all_sponsors[i, ]$klarner_name <- unique(k_matches$cand)
      all_sponsors[i, ]$klarner_id <- unique(k_matches$candid)
      eyear <- as.numeric(str_split(t_yrs, "\\_")[[1]][1]) - 1
      all_sponsors[i, ]$elec_year <- k_matches[which(abs(k_matches$year - eyear) == min(abs(k_matches$year - eyear))),]$year
      rm(eyear)
    } else{
      print(glue("MISSING MATCH ::: {all_sponsors[i,]$LES_sponsor} ::: {i} ::: CHAMBER: {all_sponsors[i,]$chamber}"))
    }
  }
  #select(all_sponsors, LES_sponsor, klarner_name) %>% View()
  
  #### Fix Mismatches
  if(t_yrs == "2001_2002"){
    all_sponsors[all_sponsors$LES_sponsor == "stites (chad)" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "2003_2004"){
    all_sponsors[all_sponsors$LES_sponsor == "peterson (pam)" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == "2005_2006"){
    all_sponsors[all_sponsors$LES_sponsor == "johnson (constance)" & all_sponsors$chamber == "S", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == '2017_2018'){
    all_sponsors[all_sponsors$LES_sponsor == "ford (ross)" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }
  
  #### CHeck for Duplicates from Matching
  duplicates <- filter(all_sponsors, !is.na(klarner_name)) %>% group_by(chamber, klarner_name) %>% filter(n() > 1)
  if(nrow(duplicates) > 0){
    cat('-----> Check DUPLICATE Sponsors \n ')
    print(select(duplicates, LES_sponsor, klarner_name, chamber) %>% as.data.frame())
  }; rm(duplicates)
  
  ##### CHECK IF ANY KLARNER CANDIDATES MISSING FROM DATA --- Includes all Senaters elected at T-1
  km <- filter(klarner_sub, !(candid %in% all_sponsors$klarner_id) )
  km <- filter(km, sen == 0 | (sen == 1 & year %in%  S_elec_year:(as.numeric(substring(t_yrs, 1, 4))) ))
  
  ### Remove candidates who won but were never seated
  if(t_yrs == '1999_2000'){
    km <- filter(km, cand != "key, charles")
  }else if(t_yrs == "2003_2004"){
    km <- filter(km, cand != 'henry, brad')
  }else if(t_yrs == '2005_2006'){
    km <- filter(km, cand != 'maddox, jim')
  }else if(t_yrs == '2011_2012'){
    km <- filter(km, cand != 'auffet, john')
    km <- filter(km, cand != 'lamb, todd')
  }else if(t_yrs == '2013_2014'){
    km <- filter(km, cand != 'myers, david')
    km <- filter(km, cand != 'rice, andrew')    
  }else if(t_yrs == '2015_2016'){
    km <- filter(km, cand != 'abdo, melissa')
    km <- filter(km, cand != 'ellis, jerry l.')
    km <- filter(km, cand != 'shumate, jabar')    
  }else if(t_yrs == '2017_2018'){
    km <- filter(km, cand != 'bingman, brian')
    km <- filter(km, cand != 'brinkley, rick')
  }
  
  #### Add Candidates who WON but Sponsored NO BILLS into Data
  if(nrow(km) > 0){
    cat(glue("-----> {as.numeric(substring(t_yrs, 1, 4)) - 1} KLARNER CANDIDATES MISSING MATCHES IN DATA ~~> ADDING INTO LIST OF LEGISLATORS: \n ."))
    print(select(km, year, sab, sen, ddez, etype, deter, cand, candid, partyz, match_name) %>% as.data.frame())
    chamb <- ifelse(km$sen == 1, "S", "H")
    for(i in 1:nrow(km)){
      all_sponsors <- add_row(all_sponsors, chamber = chamb[i], term = t_yrs, klarner_name = km$cand[i], klarner_id = km$candid[i], elec_year = km$year[i])
    }
    rm(chamb)
  }
  
  #### Clean
  legis_data <- all_sponsors %>%
    rename(data_name = LES_sponsor) %>%
    mutate(sponsor = ifelse(!is.na(klarner_name), klarner_name, tolower(match_name))) %>%
    select(-c(first_name, match_name)) %>% #middle_name, last_name, suffix,
    select(sponsor, data_name, klarner_name, klarner_id, chamber, term, elec_year, num_sponsored_bills, sponsor_pass_rate, sponsor_law_rate, num_cosponsored_bills) %>%
    arrange(chamber, sponsor)
  
  ########################
  ### Estimate Scores + Add in Relatd Variables
  #########################
  
  ### Check if bills in data without an ID'd sponsor
  # filter(bills, !(bills$primary_sponsor %in% legis_data$data_name))
  bills <- bills %>% #select(-sponsor) %>%
    rename(sponsor = LES_sponsor) %>%
    mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'H', 'H', 'S'))
  
  ### Standard LES: Same as Congressional Measure
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/calc_LES_fx.R')
  
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
    select(sponsor, chamber, num_sponsored_bills, sponsor_pass_rate, sponsor_law_rate, num_cosponsored_bills) %>%
    mutate(chamber = ifelse(chamber == "H", "House", "Senate")) %>%
    left_join(LES, ., by = c("sponsor", "chamber"))
  
  #### If LES == 0 and --- , "num_cosponsored_bills"
  LES[LES$LES == 0 & is.na(LES$num_sponsored_bills), c('num_sponsored_bills', 'sponsor_pass_rate', 'sponsor_law_rate')] <- 0
  
  ############### Save LES
  write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
  rm(LES)
  
  cat(glue(". \n *********************** SESSION {t_yrs} ~~> DONE  ***********************"))
  
}
################# ****** END LOOP

rm(all_sponsors, bills, legis_data, SS_bills, H_elec_year, S_elec_year, i, keep_types)
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES, commem_bills) # 
rm(t, terms, klarner_gs, c_sub, m_sub, nonspon, search_term, sn, unique_cospon, t_sessions, match_name2)


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by SPECIAL ELECTION --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
### Rosters: 
## ---- House: https://www.okhouse.gov/Documents/ALLHOUSE-LIST.pdf
## ---- Senate: https://www.okhouse.gov/Documents/ALLSENATE-LIST.pdf
## ---- Senate: Click Historic Members: http://www.oksenate.gov/Senators/
## ---- House: https://www.okhouse.gov/Members/Historic.aspx
#########################
# filter(klarner, grepl("morin", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 124 & sen ==0 & year < 2000) %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1993_1994 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 3 bill(s) without a sponsor
# ### WON SPECIAL ~ HOUSE:
# -- PERRY (fred)
# -- TOURE (opio)
# -- WELLS (dale)
### WON SPECIAL ~ SENATE:
# -- MONSON (angela, via H)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1996 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 8 bill(s) without a sponsor
# *** NO issues after name fixes ***


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 5 bill(s) without a sponsor
# *** NO issues after name fixes ***


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 3 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- NANCE (john)
### DROP:
# -- key, charles -- won, never seated, 


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 3 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- DEWITT (dale)
# -- STITES (chad) -- NAME DUPLICATED, won't print


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 2 bill(s) without a sponsor
#### WON SPECIAL ~ HOUSE:
# -- MASS (mike, past H) -- ran for congress, lost, then Lerblance (who took his seat) won state senate special, so he ran again
# -- PETERSON (pam) -- name duplicated, won't print
#### WON SPECIAL ~ SENATE:
# -- LASTER (charlie)
# -- LERBLANCE (richard, via H)
#### DROP:
# -- henry, brad -- no longer in chamber, per senate roster: https://www.okhouse.gov/Documents/ALLSENATE-LIST.pdf


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 3 bill(s) without a sponsor
### WON SPECIAL ~ SENATE:
# -- SCHULZ (mike)
# -- JOHNSON (constance) -- Name duplicated, won't print
### DROP:
# -- maddox, jim -- no longer in chamber, per senate roster: https://www.okhouse.gov/Documents/ALLSENATE-LIST.pdf


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1 bill(s) without a sponsor
# ******** No issues after name corrections ******


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 4 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- RUSS (todd)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 3 bill(s) without a sponsor
#### WON GENERAL ~ HOUSE --- BUT CODED AS LOSER:
# -- FOURKILLER (will) = winner --> auffet, john = loser
#### WON SPECIAL ~ HOUSE:
# -- MCDANIEL (curtis)
#### WON SPECIAL ~ SENATE:
# -- CHILDERS (greg)
# -- GRIFFIN (a.j.)
# -- MCAFFREY (al, via H)
# -- TREAT (greg)
#### DROP:
# -- auffet, john -- lost to fourkiller, klarner incorrect
# -- lamb, todd -- resigned, became Lt. Gov in Jan 2011

 
# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 6 bill(s) without a sponsor
### WON SPECIAL ~ SENATE:
# -- GRIFFIN (a.j.)
# -- MCAFFREY (al, via H)
### DROP:
# -- myers, david -- died Nov 11, 2011
# -- rice, andrew -- resigned Jan 15, 2012


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 14 bill(s) without a sponsor
# -----> Dropping 11 bills sponsored by committee
### WON SPECIAL ~ HOUSE:
# -- GOODWIN (regina)
# -- MUNSON (cyndi)
# -- STROHM (chuck) ----> WON GENERAL; klarner wrong; see note at top
### WON SPECIAL ~ SENATE:
# -- DOSSETT, JJ 
# -- MATTHEWS (kevin, via H)
### DROP:
# -- abdo, melissa -- did not win; lost to strohm
# -- ellis, jerry l. -- not in chamber in 2015; resigned in 2014
# -- shumate, jabar -- not in chamber in 2015; resigned January 6, 2015


# ~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 70 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- BOLES (brad, R, D-51)
# -- GADDIS (karen, D, D-75)
# -- ROSECRANTS (jacob, D, D-46)
# -- TAYLOR (zack, R, D-28)
# -- FORD (ross, R, D-76) -- ** Name Duplicated, won't print
### WON SPECIAL ~ SENATE:
# -- BROOKS (michael, D, D-44)
# -- DOSSETT (j.j., D, D-34)
# -- IKLEY-FREEMAN (allison, D, D-37)
# -- MURDOCK (casey, via H)
# -- ROSINO (paul, R, D-45)
### IN SENATE (partial):
# -- shortey, ralph -- resigned March 22, 2017
### DROP:
# -- bingman, brian -- served through 2016
# -- brinkley, rick -- resigned august 20, 2015 after guilty plea


# filter(klarner, grepl("ford, r", cand) ) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 86 & sen == 0 & year %in% 2010) %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)



##########################################################################################################
##########################################################################################################
##### MATCHING MISSING (SPECIALS) TO KLARNER USING OTHER TERMS WHERE SPONSOR IS PRESENT
#########################################################################################################

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
LES[LES$data_name %in% c('brooks') & LES$term == "2017_2018",]$klarner_id <- NA
LES[LES$data_name %in% c('brooks') & LES$term == "2017_2018",]$klarner_name <- NA
LES[LES$data_name %in% c('brooks') & LES$term == "2017_2018",]$sponsor <- 'brooks, michael'

### ****Still missing*****
# still_missing <- unique(LES[is.na(LES$klarner_id),]$sponsor)
# name = still_missing[1]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, missing, still_missing)

### Fix Missing
# name_matches <- data.frame(LES_name = 'zzzzzz', k_name = 'zzzzzzzz') 
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')
# for(i in 1:nrow(name_matches)){
#   LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
#   LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
#   LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
# }
# rm(name_matches, i)

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
# fill_missing <- data.frame(LES_name = "zzzzzz", new_name = 'zzzzzz', party = 'zzz', district = zzzzz, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')
#
# for(i in 1:nrow(fill_missing)){
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper
#   LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
# }

#### *** 2017_2018 (and 2015 Senate special winner): If any of below run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "dossett" & LES$term == '2015_2016', c('party', 'sponsor', 'district')] <- list('d', 'dossett, j.j.', 34)
LES[LES$sponsor == "dossett" & LES$term == '2017_2018', c('party', 'sponsor', 'district')] <- list('d', 'dossett, j.j.', 34)
LES[LES$sponsor == "taylor", c('party', 'sponsor', 'district')] <- list('r', 'taylor, zack', 28)
LES[LES$sponsor == "ford, ross", c('party', 'sponsor', 'district')] <- list('r', 'ford, ross', 76)
LES[LES$sponsor == "boles", c('party', 'sponsor', 'district')] <- list('r', 'boles, brad', 51)
LES[LES$sponsor == "ikley-freeman", c('party', 'sponsor', 'district')] <- list('d', 'ikley-freeman, allison', 37)
LES[LES$sponsor == "rosino", c('party', 'sponsor', 'district')] <- list('r', 'rosino, paul', 45)
LES[LES$sponsor == "brooks, michael", c('party', 'sponsor', 'district')] <- list('d', 'brooks, michael', 44)

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)
# rm(fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()

### Fix Names
LES[LES$sponsor == "hoskin, chuck",]$sponsor <- 'hoskin, charles thomas'


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

# ********* OKLAHOMA IDEO DATA STARTS IN 1996 ***********

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

LES[LES$sponsor %in% c('bryant, john', 'bryant, james sears', 'mcdaniel, john randall (randy)'), c('SM_name', 'SM_party', 'np_score')] <- NA
LES[LES$sponsor %in% c('roberts, darryl f.', 'smith, david l.', 'williams, danny', 'worthen, rande'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1987_1988', '1989_1990', '1991_1992', '1993_1994', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('ervin', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

name_matches <- data.frame(LES_name = 'adkins, scott', SM_name = 'Adkins')
name_matches <- add_row(name_matches, LES_name = 'bryant, john', SM_name = 'Bryant') # James Sears Bryant only in chamber 1993_1994
name_matches <- add_row(name_matches, LES_name = 'hamilton, james e.', SM_name = 'Hamilton')
name_matches <- add_row(name_matches, LES_name = 'holt, james', SM_name = 'Holt')
name_matches <- add_row(name_matches, LES_name = 'hoskin, charles thomas', SM_name = 'Hoskin, Chuck')
#name_matches <- add_row(name_matches, LES_name = 'johnson, glen d.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'mcdaniel, john randall (randy)', SM_name = 'McDaniel, Randy')
name_matches <- add_row(name_matches, LES_name = 'mcintyre, judy eason', SM_name = 'Eason McIntyre, Judy')
name_matches <- add_row(name_matches, LES_name = 'roberts, darryl f.', SM_name = 'Roberts')
name_matches <- add_row(name_matches, LES_name = 'roberts, sean', SM_name = 'Roberts, Kevin') # Kevin SEAN Roberts - https://adambrown.info/p/research/legislators/members/oklahoma/lower/kevin-sean-roberts-5314
name_matches <- add_row(name_matches, LES_name = 'vaughn, ray', SM_name = 'Vaughn, Raymond Jr.')
name_matches <- add_row(name_matches, LES_name = 'wright, gerald ged', SM_name = 'Wright')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########

# *** Mike Ervin --- D to R in 2001 --- Technically elected as a Dem in each period and only switches for final year, so need to make sure he matches there
# ---> Dates are weirdly off in SM data... suggests he went back and forth -- https://www.newson6.com/story/7700126/ervin-switches-from-democrat-to-republican
# LES[LES$sponsor == 'ervin, mike' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Ervin, Mike' & ideo$party == 'R',]$name
# LES[LES$sponsor == 'ervin, mike' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Ervin, Mike' & ideo$party == 'R',]$party
# LES[LES$sponsor == 'ervin, mike' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Ervin, Mike' & ideo$party == 'R',]$np_score
LES[LES$sponsor == 'ervin, mike' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Ervin' & ideo$party == 'D',]$name
LES[LES$sponsor == 'ervin, mike' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Ervin' & ideo$party == 'D',]$party
LES[LES$sponsor == 'ervin, mike' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Ervin' & ideo$party == 'D',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

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
LES[LES$sponsor == 'cisselhorner, maxine',]$sponsor <- 'horner, maxine cissel'
LES[LES$sponsor == 'griffin, a. j.',]$sponsor <- 'griffin, ann j.'
LES[LES$sponsor == 'hobson, cal',]$sponsor <- 'hobson, calvin j.'
LES[LES$sponsor == 'pruett, r. c.',]$sponsor <- 'pruett, raymond c.'
LES[LES$sponsor == 'boren, david daniel',]$sponsor <- 'boren, daniel david' # Klarner has his name reversed


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1993 - 2020
# --> 2006: Senate Tied 24-24 --> Negotiated Power-Sharing Agreement --> Both Parties in Majority, in some sense.
# --> Details: https://web.archive.org/web/20070821172533/http://www.oksenate.gov/news/press_releases/press_releases_2006/pr20061212a.html
LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:2004) & LES$chamber == 'House' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2005:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1
  
### Senate -- 1993 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:2006) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2009:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1
LES[LES$term == "2007_2008" & LES$chamber == "Senate",]$in_majority <- 1


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
  scale_color_manual(values=c("dodgerblue2", "red2", 'gray50'))

##### CHECK OUTLIERS 
## WEAVER: Switched parties for 1994 election, but SM data doesn't go back that far: https://oklahoman.com/article/2474486/ex-gop-incumbent-running-as-democrat-weaver-faces-republican-opponent-nov-8
# filter(LES, party == 'd' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

