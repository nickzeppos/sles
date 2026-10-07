################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** MISSISSIPPI *** BY SESSION
##############################################################

# *************** NEED TO CHECK A BUNCH OF PARTY SWITCHERS

###########
#### QUESTIONS
# (1) What to do about lack of 1996??? --> for now, constructing 1996_1999 using 3 later sessions
# (2) How to code majority control for 2007, 2011 in Senate? Both were final year of four year ter where R's took over based on switchers

###################################
## (SPECIAL) SESSIONS:
## ---- Bill numbers re-start at HB1/SB1; Bills NOT carried over from regular to special or regular to regular
## MEMBER LISTS:
## ---- See the lists of authors on each session page
## PROCESS/RULES:
## ---- http://billstatus.ls.state.ms.us/htms/billlaw.htm
## ---- House Rules: http://billstatus.ls.state.ms.us/htms/h_rules.pdf
## ---- Senate Rules: http://billstatus.ls.state.ms.us/htms/s_rules.pdf
## Sponsorship/Authorship
## ---- Author's clearly identified; coauthorship permitted
###########################
## NOTES:
## **** MISSING 1996 SESSION IN THE 1996_1999 TERM ****
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
library(tibble)
library(foreach)

this_state <- 'MS'
keep_types <- c('HB', "SB")

#### Output Directory
#dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd(glue("../../LES_By_State/{this_state}"))

### Terms/Sessions and Filespaths
data_files <- list.files(glue("../../../State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
sessions <- gsub(".+Bill_Details_|.csv", '', bill_files)
rm(data_files, bill_files)

### Terms/Sessions and Filespaths
terms <- 2020
t = terms
t_plus_one = t + 1
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
# t <- terms[6]



### Formulate 2-year terms -- Cover both regular and special sessions
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
    bills <- bind_rows(bills, s_bills)
  }
  rm(s, s_bills)
}

### Clean Term/Session Variables
bills <- bills %>%
  mutate(term = t_yrs,
         session_type = recode(session_type,'ES' = 'SS1', 'ES1' = 'SS1', 'ES2' = 'SS2', 'ES3' = 'SS3', 'ES4' = 'SS4', 'ES5' = 'SS5', 'ES6' = 'SS6'),
         session = paste(session_year, session_type, sep = "-"))

### Drop duplicates
bills <- distinct(bills)

######## Standardize the Bill IDs
bills <- rename(bills, bill_id = bill_number)

############### Drop Resolutions, Messages, Communications, Reports
## SN = Senate Nomination
bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
all_bills <- bills
table(bills$bill_type)
bills <- filter(bills, bill_type %in% keep_types) 

##########################
####### Standardize Sponsors

bills$author <- tolower(bills$author)
bills$author <- gsub('á', 'a', bills$author)
bills$author <- gsub('é', 'e', bills$author)
bills$author <- gsub('ó', 'o', bills$author)
bills$author <- gsub('í', 'i', bills$author)
bills$author <- gsub('ñ', 'n', bills$author)

bills$coauthors <- tolower(bills$coauthors)
bills$coauthors <- gsub('á', 'a', bills$coauthors)
bills$coauthors <- gsub('é', 'e', bills$coauthors)
bills$coauthors <- gsub('ó', 'o', bills$coauthors)
bills$coauthors <- gsub('í', 'i', bills$coauthors)
bills$coauthors <- gsub('ñ', 'n', bills$coauthors)

### Clean By Request Bills -- Indicated by * after name
bills$author <- str_trim(gsub("\\*$", '', bills$author))

#### Manual Fixes for Duplicate Last Names --- Identified by District
if(t_yrs == "1996_1999"){
  bills[bills$author == "davis (102nd)",]$author <- 'davis, lee jarrell'
  bills$coauthors <- gsub('davis \\(102nd\\)', 'davis, lee jarrell', bills$coauthors)  
  bills[bills$author == "davis (7th)",]$author <- 'davis, charles greg'
  bills$coauthors <- gsub('davis \\(7th\\)', 'davis, charles greg', bills$coauthors)  
  ### CHarles Davis must resign in 1997
  bills[bills$author == 'davis',]$author <- 'davis, lee jarrell'
  bills[bills$session_year > 1997 & substring(bills$bill_id,1,1) == "H",]$coauthors <- gsub('davis', 'davis, lee jarrell', bills[bills$session_year > 1997 & substring(bills$bill_id,1,1) == "H",]$coauthors)  
  bills[bills$author == "green (34th)",]$author <- 'green, james d.'
  bills$coauthors <- gsub('green \\(34th\\)', 'green, james d.', bills$coauthors)  
  bills[bills$author == "green (72nd)",]$author <- 'green, tomie t.'
  bills$coauthors <- gsub('green \\(72nd\\)', 'green, tomie t.', bills$coauthors) 
  ### Corrected Klarner: Was Mrs. David (Opal) Green, but that doesn't appear to be correct per legislative website
  bills[bills$author == "green (96th)",]$author <- 'green, david l.'
  bills$coauthors <- gsub('green \\(96th\\)', 'green, david l.', bills$coauthors)  
  bills[bills$author == "henderson (26th)",]$author <- 'henderson, leonard'
  bills$coauthors <- gsub('henderson \\(26th\\)', 'henderson, leonard', bills$coauthors)  
  bills[bills$author == "henderson (9th)",]$author <- 'henderson, clayton p.'
  bills$coauthors <- gsub('henderson \\(9th\\)', 'henderson, clayton p.', bills$coauthors)  
  bills[bills$author == "jordan (18th)",]$author <- 'jordan, terry l.'
  bills$coauthors <- gsub('jordan \\(18th\\)', 'jordan, terry l.', bills$coauthors)  
  bills[bills$author == "jordan (24th)",]$author <- 'jordan, david'
  bills$coauthors <- gsub('jordan \\(24th\\)', 'jordan, david', bills$coauthors)
  bills[bills$author == "simmons (100th)",]$author <- 'simmons, miriam'
  bills$coauthors <- gsub('simmons \\(100th\\)', 'simmons, miriam', bills$coauthors)  
  bills[bills$session_year == 1999 & bills$author == "simmons" & substring(bills$bill_id, 1, 1) == "H",]$author <- "simmons, miriam"
  bills[bills$author == "simmons (37th)",]$author <- 'simmons, cecil lamar'
  bills$coauthors <- gsub('simmons \\(37th\\)', 'simmons, cecil lamar', bills$coauthors)  
  bills[bills$author == "white (29th)" | bills$author == "white",]$author <- 'white, richard'
  bills$coauthors <- gsub('white \\(29th\\)', 'white, richard', bills$coauthors)    
  bills[bills$author == "white (5th)",]$author <- 'white, john'
  bills$coauthors <- gsub('white \\(5th\\)', 'white, john', bills$coauthors)  
}
if(t_yrs == "2000_2003"){
  ### Only 1 henderson
  bills[bills$author == "henderson (9th)",]$author <- 'henderson'
  bills$coauthors <- gsub('henderson \\(9th\\)', 'henderson', bills$coauthors)    
  bills[bills$author == "montgomery (15th)",]$author <- 'montgomery, pat'
  bills$coauthors <- gsub('montgomery \\(15th\\)', 'montgomery, pat', bills$coauthors)    
  bills[bills$author == "montgomery (74th)",]$author <- 'montgomery, keith'
  bills$coauthors <- gsub('montgomery \\(74th\\)', 'montgomery, keith', bills$coauthors)    
  bills[bills$author == "moore (100th)",]$author <- 'moore, o. k.'
  bills$coauthors <- gsub('moore \\(100th\\)', 'moore, o. k.', bills$coauthors)    
  bills[bills$author == "moore (60th)",]$author <- 'moore, john l.'
  bills$coauthors <- gsub('moore \\(60th\\)', 'moore, john l.', bills$coauthors)    
  ### John White resigned in 2002
  bills[bills$author == "white (29th)" | bills$author == "white",]$author <- 'white, richard'
  bills$coauthors <- gsub('white \\(29th\\)', 'white, richard', bills$coauthors)    
  bills[bills$session_year == 2003,]$coauthors <- gsub('white', 'white, richard', bills[bills$session_year == 2003,]$coauthors)  
  bills[bills$author == "white (5th)",]$author <- 'white, john'
  bills$coauthors <- gsub('white \\(5th\\)', 'white, john', bills$coauthors)    
}
if(t_yrs %in% c("1996_1999", '2000_2003')){
  bills[bills$author == "barnett (116th)",]$author <- 'barnett, les'
  bills$coauthors <- gsub('barnett \\(116th\\)', 'barnett, les', bills$coauthors)
  bills[bills$author == "barnett (92nd)",]$author <- 'barnett, jim c.'
  bills$coauthors <- gsub('barnett \\(92nd\\)', 'barnett, jim c.', bills$coauthors)
  bills[bills$author == "johnson (19th)",]$author <- 'johnson, timoth l.'
  bills$coauthors <- gsub('johnson \\(19th\\)', 'johnson, timoth l.', bills$coauthors)    
  bills[bills$author == "johnson (38th)",]$author <- 'johnson, robert l. iii'
  bills$coauthors <- gsub('johnson \\(38th\\)', 'johnson, robert l. iii', bills$coauthors)    
  bills[bills$author == "scott (17th)",]$author <- 'scott, eloise'
  bills$coauthors <- gsub('scott \\(17th\\)', 'scott, eloise', bills$coauthors)    
  bills[bills$author == "scott (80th)",]$author <- 'scott, omeria'
  bills$coauthors <- gsub('scott \\(80th\\)', 'scott, omeria', bills$coauthors)   
  bills[bills$author == "smith (35th)",]$author <- 'smith, charlie'
  bills$coauthors <- gsub('smith \\(35th\\)', 'smith, charlie', bills$coauthors)    
}
if(t_yrs %in% c("1996_1999", '2000_2003', '2004_2007')){
  bills[bills$author == "robinson (63rd)",]$author <- 'robinson, walter l.'
  bills$coauthors <- gsub('robinson \\(63rd\\)', 'robinson, walter l.', bills$coauthors)    
  bills[bills$author == "robinson (84th)",]$author <- 'robinson, eric'
  bills$coauthors <- gsub('robinson \\(84th\\)', 'robinson, eric', bills$coauthors)   
  bills[bills$author == "smith (59th)",]$author <- 'smith, clayton'
  bills$coauthors <- gsub('smith \\(59th\\)', 'smith, clayton', bills$coauthors)    
}
if(t_yrs %in% c("1996_1999", '2000_2003', '2004_2007', '2008_2011')){
  bills[bills$author == "coleman (29th)",]$author <- 'coleman, linda'
  bills$coauthors <- gsub('coleman \\(29th\\)', 'coleman, linda', bills$coauthors)
  bills[bills$author == "coleman (65th)",]$author <- 'coleman, mary h.'
  bills$coauthors <- gsub('coleman \\(65th\\)', 'coleman, mary h.', bills$coauthors)    
  bills[bills$author == "smith (27th)",]$author <- 'smith, ferr'
  bills$coauthors <- gsub('smith \\(27th\\)', 'smith, ferr', bills$coauthors)    
  bills[bills$author == "smith (39th)",]$author <- 'smith, jeffrey'
  bills$coauthors <- gsub('smith \\(39th\\)', 'smith, jeffrey', bills$coauthors)    
}
if(t_yrs == "2004_2007"){
  ### Matches to a thomas who won, then lost special after election was disputed (and thus was never seated)
  bills[bills$author == 'thomas' & substring(bills$bill_id,1,1) == "S",]$author <- 'thomas, joseph c.'
  bills[substring(bills$bill_id,1,1) == "S",]$coauthors <- gsub('thomas', 'thomas, joseph c.', bills[substring(bills$bill_id,1,1) == "S",]$coauthors)    
}
if(t_yrs %in% c("2004_2007", "2008_2011")){
  bills[bills$author == "baker (74th)",]$author <- 'baker, mark'
  bills$coauthors <- gsub('baker \\(74th\\)', 'baker, mark', bills$coauthors)
  bills[bills$author == "baker (8th)",]$author <- 'baker, larry j.'
  bills$coauthors <- gsub('baker \\(8th\\)', 'baker, larry j.', bills$coauthors)
  bills[bills$author == "hamilton (109th)",]$author <- 'hamilton, frank'
  bills$coauthors <- gsub('hamilton \\(109th\\)', 'hamilton, frank', bills$coauthors)
  bills[bills$author == "hamilton (6th)",]$author <- 'hamilton, eugene forrest'
  bills$coauthors <- gsub('hamilton \\(6th\\)', 'hamilton, eugene forrest', bills$coauthors)
  bills[bills$author == "jackson (11th)",]$author <- 'jackson, robert'
  bills$coauthors <- gsub('jackson \\(11th\\)', 'jackson, robert', bills$coauthors)
  bills[bills$author == "jackson (15th)",]$author <- 'jackson, gary'
  bills$coauthors <- gsub('jackson \\(15th\\)', 'jackson, gary', bills$coauthors)
  bills[bills$author == "jackson (32nd)",]$author <- 'jackson, sampson ii'
  bills$coauthors <- gsub('jackson \\(32nd\\)', 'jackson, sampson ii', bills$coauthors)
  bills[bills$author == "lee (35th)",]$author <- 'lee, perry'
  bills$coauthors <- gsub('lee \\(35th\\)', 'lee, perry', bills$coauthors)
  bills[bills$author == "lee (47th)",]$author <- 'lee, ezell'
  bills$coauthors <- gsub('lee \\(47th\\)', 'lee, ezell', bills$coauthors)
  bills[bills$author == "rogers (14th)",]$author <- 'rogers, margaret ellis'
  bills$coauthors <- gsub('rogers \\(14th\\)', 'rogers, margaret ellis', bills$coauthors)
  bills[bills$author == "rogers (61st)",]$author <- 'rogers, ray'
  bills$coauthors <- gsub('rogers \\(61st\\)', 'rogers, ray', bills$coauthors)
}
if(t_yrs == "2008_2011"){
  bills[bills$author == "buck (5th)" | bills$author == 'buck',]$author <- 'buck, kelvin'
  bills$coauthors <- gsub('buck \\(5th\\)', 'buck, kelvin', bills$coauthors)
  ### Was just Buck in 2008
  bills[bills$session_year == 2008,]$coauthors <- gsub('buck', 'buck, kelvin', bills[bills$session_year == 2008,]$coauthors)  
  ### Kimberly (Campbell) Buck --- Changed Last Name to Buck between 2008/2009 sessions
  ### ---> Need to Adjust (David 'Tad') Campbell first
  bills[grepl("campbell", bills$coauthors) & bills$session_year > 2008,]$coauthors <- gsub("campbell", "campbell, david", bills[grepl("campbell", bills$coauthors) & bills$session_year > 2008,]$coauthors )
  bills$coauthors <- gsub('campbell \\(84th\\)', 'campbell, david', bills$coauthors)
  bills[bills$author == "buck (72nd)",]$author <- 'buck, kimberly campbell'
  bills$coauthors <- gsub('buck \\(72nd\\)', 'buck, kimberly campbell', bills$coauthors)
  bills$coauthors <- gsub('campbell \\(72nd\\)', 'buck, kimberly campbell', bills$coauthors)
  ### Albert Butler entered Senate in 2010
  bills[bills$author == "butler (36th)",]$author <- 'butler, albert'
  bills$coauthors <- gsub('butler \\(36th\\)', 'butler, albert', bills$coauthors)
  bills[bills$author == "butler (38th)" | bills$author == "butler",]$author <- 'butler, kelvin e.'
  bills$coauthors <- gsub('butler \\(38th\\)', 'butler, kelvin e.', bills$coauthors)
  bills$coauthors <- gsub('^butler;|^butler$', 'butler, kelvin e.;', bills$coauthors)  
  bills$coauthors <- gsub('; butler;|; butler$', '; butler, kelvin e.;', bills$coauthors)  
  ### Vincent Davis must have resigned in 2009
  bills[bills$author == "davis (1st)" | bills$author == 'davis',]$author <- 'davis, doug'
  bills$coauthors <- gsub('davis \\(1st\\)', 'davis, doug', bills$coauthors)
  bills[bills$session_year > 2009,]$coauthors <- gsub('davis', 'davis, doug', bills[bills$session_year > 2009,]$coauthors)  
  bills[bills$author == "davis (36th)",]$author <- 'davis, e. vincent'
  bills$coauthors <- gsub('davis \\(36th\\)', 'davis, e. vincent', bills$coauthors)
  bills[bills$author == "evans (70th)",]$author <- 'evans, james'
  bills$coauthors <- gsub('evans \\(70th\\)', 'evans, james', bills$coauthors)
  bills[bills$author == "evans (91st)",]$author <- 'evans, robert e.'
  bills$coauthors <- gsub('evans \\(91st\\)', 'evans, robert e.', bills$coauthors)
  bills[bills$author == "huddleston (15th)",]$author <- 'huddleston, mac'
  bills$coauthors <- gsub('huddleston \\(15th\\)', 'huddleston, mac', bills$coauthors)
  bills[bills$author == "huddleston (30th)",]$author <- 'huddleston, robert e.'
  bills$coauthors <- gsub('huddleston \\(30th\\)', 'huddleston, robert e.', bills$coauthors)
  ### Wilbert took office in 2010
  bills[bills$author == "jones (82nd)",]$author <- 'jones, wilbert l.'
  bills$coauthors <- gsub('jones \\(82nd\\)', 'jones, wilbert l.', bills$coauthors)
  bills[bills$author == "jones (111th)",]$author <- 'jones, brandon'
  bills$coauthors <- gsub('jones \\(111th\\)', 'jones, brandon', bills$coauthors)
  bills[bills$author == 'jones' & bills$session_year < 2010 & substring(bills$bill_id,1,1) == "H",]$author <- "jones, brandon"
  bills[substring(bills$bill_id,1,1) == "H",]$coauthors <- gsub('^jones;|^jones$', 'jones, brandon;', bills[substring(bills$bill_id,1,1) == "H",]$coauthors)  
  bills[substring(bills$bill_id,1,1) == "H",]$coauthors <- gsub('; jones;|; jones$', '; jones, brandon;', bills[substring(bills$bill_id,1,1) == "H",]$coauthors)  
}
if(t_yrs == '2012_2015'){
  bills[bills$author == "brown (20th)",]$author <- 'brown, chris'
  bills$coauthors <- gsub('brown \\(20th\\)', 'brown, chris', bills$coauthors)
  bills[bills$author == "brown (66th)",]$author <- 'brown, cecil c.'
  bills$coauthors <- gsub('brown \\(66th\\)', 'brown, cecil c.', bills$coauthors)
  bills[bills$author == "buck (5th)" | bills$author == 'buck',]$author <- 'buck, kelvin'
  bills$coauthors <- gsub('buck \\(5th\\)', 'buck, kelvin', bills$coauthors)
  ### Reverted to Kimberly Campbell in 2014?
  bills[bills$author == "buck (72nd)",]$author <- 'buck, kimberly campbell'
  bills$coauthors <- gsub('buck \\(72nd\\)', 'buck, kimberly campbell', bills$coauthors)
  bills[bills$author == "campbell",]$author <- 'buck, kimberly campbell'
  bills[bills$session_year > 2013 & grepl('campbell', bills$coauthors),]$coauthors <- gsub('campbell', 'buck, kimberly campbell', bills[bills$session_year > 2013 & grepl('campbell', bills$coauthors),]$coauthors)
  bills[bills$author == "butler (36th)",]$author <- 'butler, albert'
  bills$coauthors <- gsub('butler \\(36th\\)', 'butler, albert', bills$coauthors)
  bills[bills$author == "butler (38th)" | bills$author == "butler",]$author <- 'butler, kelvin e.'
  bills$coauthors <- gsub('butler \\(38th\\)', 'butler, kelvin e.', bills$coauthors)
  bills[bills$author == "coleman (29th)",]$author <- 'coleman, linda'
  bills$coauthors <- gsub('coleman \\(29th\\)', 'coleman, linda', bills$coauthors)
  bills[bills$author == "coleman (65th)",]$author <- 'coleman, mary h.'
  bills$coauthors <- gsub('coleman \\(65th\\)', 'coleman, mary h.', bills$coauthors)   
  bills[bills$author == "evans (43rd)",]$author <- 'evans, michael t.'
  bills$coauthors <- gsub('evans \\(43rd\\)', 'evans, michael t.', bills$coauthors)
  bills[bills$author == "evans (70th)",]$author <- 'evans, james'
  bills$coauthors <- gsub('evans \\(70th\\)', 'evans, james', bills$coauthors)
  bills[bills$author == "evans (91st)",]$author <- 'evans, robert e.'
  bills$coauthors <- gsub('evans \\(91st\\)', 'evans, robert e.', bills$coauthors)
  bills[bills$author == "huddleston (15th)",]$author <- 'huddleston, mac'
  bills$coauthors <- gsub('huddleston \\(15th\\)', 'huddleston, mac', bills$coauthors)
  bills[bills$author == "huddleston (30th)",]$author <- 'huddleston, robert e.'
  bills$coauthors <- gsub('huddleston \\(30th\\)', 'huddleston, robert e.', bills$coauthors)
  bills[bills$author == "jackson (11th)",]$author <- 'jackson, robert'
  bills$coauthors <- gsub('jackson \\(11th\\)', 'jackson, robert', bills$coauthors)
  bills[bills$author == "jackson (15th)",]$author <- 'jackson, gary'
  bills$coauthors <- gsub('jackson \\(15th\\)', 'jackson, gary', bills$coauthors)
  bills[bills$author == "jackson (32nd)",]$author <- 'jackson, sampson ii'
  bills$coauthors <- gsub('jackson \\(32nd\\)', 'jackson, sampson ii', bills$coauthors)
  bills[bills$author == "rogers (14th)",]$author <- 'rogers, margaret ellis'
  bills$coauthors <- gsub('rogers \\(14th\\)', 'rogers, margaret ellis', bills$coauthors)
  bills[bills$author == "rogers (61st)",]$author <- 'rogers, ray'
  bills$coauthors <- gsub('rogers \\(61st\\)', 'rogers, ray', bills$coauthors)
  bills[bills$author == "simmons (12th)",]$author <- 'simmons, derrick t.'
  bills$coauthors <- gsub('simmons \\(12th\\)', 'simmons, derrick t.', bills$coauthors)
  bills[bills$author == "simmons (13th)",]$author <- 'simmons, willie'
  bills$coauthors <- gsub('simmons \\(13th\\)', 'simmons, willie', bills$coauthors)
  bills[bills$author == "smith (27th)",]$author <- 'smith, ferr'
  bills$coauthors <- gsub('smith \\(27th\\)', 'smith, ferr', bills$coauthors)    
  bills[bills$author == "smith (39th)",]$author <- 'smith, jeffrey'
  bills$coauthors <- gsub('smith \\(39th\\)', 'smith, jeffrey', bills$coauthors) 
}
if(t_yrs == '2016_2019'){
  bills[bills$author == "bell (21st)",]$author <- 'bell, donnie'
  bills$coauthors <- gsub('bell \\(21st\\)', 'bell, donnie', bills$coauthors)
  bills[bills$author == "bell (65th)",]$author <- 'bell, christopher m.'
  bills$coauthors <- gsub('bell \\(65th\\)', 'bell, christopher m.', bills$coauthors)
  bills[bills$author == "evans (45th)",]$author <- 'evans, michael t.'
  bills$coauthors <- gsub('evans \\(45th\\)', 'evans, michael t.', bills$coauthors)
  bills[bills$author == "evans (91st)",]$author <- 'evans, robert e.'
  bills$coauthors <- gsub('evans \\(91st\\)', 'evans, robert e.', bills$coauthors)
  ### Listed just as gibbs for one bill in 2016
  bills[bills$author == "gibbs (36th)" | bills$author == 'gibbs',]$author <- 'gibbs, karl malinski'
  bills[grepl('^gibbs$|^gibbs;|; gibbs;|; gibbs$', bills$coauthors),]$coauthors <- gsub('gibbs', 'gibbs, karl malinski', bills[grepl('^gibbs$|^gibbs;|; gibbs;|; gibbs$', bills$coauthors),]$coauthors)
  bills$coauthors <- gsub('gibbs \\(36th\\)', 'gibbs, karl malinski', bills$coauthors)
  bills[bills$author == "gibbs (72nd)",]$author <- 'gibbs, debra'
  bills$coauthors <- gsub('gibbs \\(72nd\\)', 'gibbs, debra', bills$coauthors)
  ### Robert Huddleston retired in 2018, mac's name switches to just huddleston
  bills[bills$author == "huddleston (30th)",]$author <- 'huddleston, robert e.'
  bills$coauthors <- gsub('huddleston \\(30th\\)', 'huddleston, robert e.', bills$coauthors)
  bills[bills$author %in% c("huddleston (15th)", "huddleston"),]$author <- 'huddleston, mac'
  bills$coauthors <- gsub('huddleston \\(15th\\)', 'huddleston, mac', bills$coauthors)
  bills[bills$session_year == 2019,]$coauthors <- gsub("huddleston", "huddleston, mac", bills[bills$session_year == 2019,]$coauthors)
  bills[bills$author == "jackson (11th)",]$author <- 'jackson, robert'
  bills$coauthors <- gsub('jackson \\(11th\\)', 'jackson, robert', bills$coauthors)
  bills[bills$author == "jackson (15th)",]$author <- 'jackson, gary'
  bills$coauthors <- gsub('jackson \\(15th\\)', 'jackson, gary', bills$coauthors)
  bills[bills$author == "jackson (32nd)",]$author <- 'jackson, sampson ii'
  bills$coauthors <- gsub('jackson \\(32nd\\)', 'jackson, sampson ii', bills$coauthors)
  bills[bills$author == "johnson (87th)",]$author <- 'johnson, chris'
  bills$coauthors <- gsub('johnson \\(87th\\)', 'johnson, chris', bills$coauthors)
  bills[bills$author == "johnson (94th)",]$author <- 'johnson, robert l. iii'
  bills$coauthors <- gsub('johnson \\(94th\\)', 'johnson, robert l. iii', bills$coauthors)
  bills[bills$author == "rogers (14th)",]$author <- 'rogers, margaret ellis'
  bills$coauthors <- gsub('rogers \\(14th\\)', 'rogers, margaret ellis', bills$coauthors)
  bills[bills$author == "rogers (61st)",]$author <- 'rogers, ray'
  bills$coauthors <- gsub('rogers \\(61st\\)', 'rogers, ray', bills$coauthors)
  bills[bills$author == "simmons (12th)",]$author <- 'simmons, derrick t.'
  bills$coauthors <- gsub('simmons \\(12th\\)', 'simmons, derrick t.', bills$coauthors)
  bills[bills$author == "simmons (13th)",]$author <- 'simmons, willie'
  bills$coauthors <- gsub('simmons \\(13th\\)', 'simmons, willie', bills$coauthors)
  ## Only 1 Smith
  bills[bills$author == "smith (39th)",]$author <- 'smith'
  bills$coauthors <- gsub('smith \\(39th\\)', 'smith', bills$coauthors) 
  ## Turner = Turner-Ford; switched name in 2017
  bills[bills$author == 'turner' & substring(bills$bill_id,1,1) == "S",]$author <- "turner-ford"
  bills[substring(bills$bill_id,1,1) == "S",]$coauthors <- gsub('^turner;|^turner$', 'turner-ford;', bills[substring(bills$bill_id,1,1) == "S",]$coauthors) 
  bills[substring(bills$bill_id,1,1) == "S",]$coauthors <- gsub('; turner;|; turner$', '; turner-ford;', bills[substring(bills$bill_id,1,1) == "S",]$coauthors) 
} else if(t_yrs == "2020_2023"){
  bills$author[bills$bill_id == "HB0362" & bills$session_year == 2023] = "brown (20th)"
  bills$author[bills$author=='mccray'] <- 'jackson-mccray'
  bills$author[bills$author=='jackson' & bills$bill_type == "SB"] <- 'jackson (11th)'
  bills$author[bills$author=='johnson' & bills$bill_type == "HB"] <- 'johnson (94th)'
  bills$author[bills$author=='boyd' & bills$bill_type == "HB"] <- 'boyd (19th)'
  bills$author[bills$author=='butler' & bills$bill_type == "SB"] <- 'butler (38th)'
  bills$author[bills$author=='brown' & bills$bill_type == "HB"] <- 'brown (20th)'
  bills = bills %>% 
    mutate(coauthors = ifelse(bill_type == "SB", gsub('butler', 'butler (38th)', coauthors), coauthors),
           coauthors = ifelse(bill_type == "HB", gsub('brown', 'brown (20th)', coauthors), coauthors),
           coauthors = ifelse(bill_type == "HB", gsub('boyd', 'boyd (19th)', coauthors), coauthors),
           coauthors = ifelse(bill_type == "HB", gsub('johnson', 'johnson (94th)', coauthors), coauthors),
           coauthors = ifelse(bill_type == "SB", gsub('jackson', 'jackson (11th)', coauthors), coauthors),
           coauthors = ifelse(bill_type == "HB", gsub('gibbs', 'gibbs (36th)', coauthors), coauthors),
           coauthors = ifelse(bill_type == "HB", gsub('mccray', 'jackson-mccray', coauthors), coauthors))
}
# bills[bills$author == "smith (39th)",]$bill_url
# filter(klarner, grepl('campbell', tolower(cand))) %>% select(sen, cand, ddez) %>% distinct()
# bills[bills$author == "zzzzzzz (zzzzz)",]$author <- 'zzzzz, zzzz'
# bills$coauthors <- gsub('zzzzz \\(zzzz\\)', 'zzzzz, zzzz', bills$coauthors)

### LES Sponsor Var
bills$coauthors <- gsub('\\;$', '', bills$coauthors)
bills <- rename(bills, LES_sponsor = author)
table(bills$LES_sponsor)


###################
###### Merge in S&S Bills
###################
# *** For MISSISSIPPI: Bills do NOT carry over, numbers re-start for all regular and special sessions (and there are many..)
# ---> Need to merge on ID and Session 
# ---> Capping Bill numbers at SESSION MAX and assuming bill is from SS with most proposed bills


if(t_yrs == "2020_2023"){
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
  left_join(all_bills %>% select(bill_id,term,summary), c("bill_id", "term")) %>%
  mutate(bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id))) %>% 
  filter(bill_type %in% keep_types) %>% select(-bill_type) %>%
  filter(! grepl("committee",summary, ignore.case=T)) %>%
  arrange(summary) 
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

############### Code Commemorative
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

############### Code Bill History
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

### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
bill_hist <- bill_hist %>%
  rename(bill_id = bill_number) %>%
  mutate(term = t_yrs,
         session_type = recode(session_type, 'ES' = 'SS1', 'ES1' = 'SS1', 'ES2' = 'SS2', 'ES3' = 'SS3', 'ES4' = 'SS4', 'ES5' = 'SS5', 'ES6' = 'SS6'),
         session = paste(session_year, session_type, sep = "-"))

### Order by Order
bill_hist <- arrange(bill_hist, term, session, bill_id, order)

### Clean Action Text
bill_hist$action <- str_trim(gsub('  +', ' ', gsub('\\&nbsp', ' ', bill_hist$action)))

### Fix incorrect action/chamber
if(any(grepl('^Veto', bill_hist$chamber))){
  bill_hist[grepl('^Veto', bill_hist$chamber),]$action <- "Vetoed (Veto Message)"
  bill_hist[grepl('^Veto', bill_hist$chamber),]$chamber <- "G"
}

### Fill in Missing CHamber Variable:
bill_hist <- bill_hist %>%
  mutate(chamber = ifelse(chamber == "", NA, chamber),
         chamber = ifelse(is.na(chamber) & order == 1, substring(bill_id, 1, 1), chamber),
         chamber = ifelse(is.na(chamber) & grepl('governor|veto', tolower(action)), 'G', chamber)) %>%
  group_by(term, session, bill_id) %>%
  fill(chamber) %>%
  ungroup()

### Re-Coding Chamber Variable
bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate", "G" = "Governor", "CC" = "Conference")

#### Load function to take a given bill history  and code legislative stages
# **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
# **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
# **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
source('../../Estimate LES/code_billhist_fx.R')

##### Set State-Specific Terms for Identifying Each Stage
aic_t <- c('title suff', 'do pass', 'committee substitute', 'tsdp', 'do be reref')
### If resolutions: 'do be adopted' (although title suff should get that anyway) // Nom: 'do advise and consent'
### TSDP seems to be if assigned to two committees; transmitting rec across bodies (so seemingly act as 1?)
abc_t <- c('title suff', 'do pass', 'point of order', '^pass', '^fail', '^amend', 'defeat', 'tabled', 'on calendar',
           'committee substitute adopted', 'reconsider', 'motion to', 'read the third time', 'recommitted')
pc_t <- c('^passed', 'transmitted to senate', 'transmitted to house', 'enrolled bill signed')
law_t <- c('approved by gov', 'law w.out governor', 'partially vetoed by gov')

### Check Actions
# filter(bill_hist, grepl('gov', tolower(action))) %>% distinct(action) %>% View()
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

### Code History Stages for Each Bill --- Subset Hist File, Code, Save, Repeat
cat('\n')
cat(glue('-----> Coding Bill Histories'))
options(warn = 2)
for(i in 1:nrow(bills)){
  b_id = bills[i,]$bill_id
  s_id = bills[i,]$session
  b_spon = bills[i,]$LES_sponsor
  hist_sub <- filter(bill_hist, bill_id == b_id, term == t_yrs, session == s_id)
  bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
  bill_stages$bill_url <- bills[i,]$bill_url
  if(bill_stages$law == 0 & bills[i,]$status == "Law"){
    bill_stages$passed_chamber <- bill_stages$law <- 1
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


read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-4}_{t-1}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% filter(!grepl("SS",session)) %>% print()

read.csv(glue("../../../State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t-8}_{t-5}_Bill_Stage_Codings.csv")) %>% 
  mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
  summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% 
  mutate(AIC_pct = round(AIC/N,2), ABC_pct = round(ABC/N,2), PASS_pct = round(PASS/N,2), LAW_pct = round(LAW/N,2))  %>% 
  as.data.frame() %>% filter(!grepl("SS",session)) %>%  print()

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
unique_cospon <- str_trim(unique(unlist(str_split(bills$coauthors, '; '))))
unique_cospon <- str_trim(gsub('\\*$', '', unique_cospon))
for(nonspon in unique_cospon){
  if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != '' & !is.na(nonspon)){
    ## ****
    chamb <- unique(substring(bills[grepl(nonspon, bills$coauthors),]$bill_id, 1, 1))
    if("H" %in% chamb & "S" %in% chamb){
      print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
    }else{
      all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0) 
    }
  }
}

######## Cosponsorship Info 
all_sponsors$num_cosponsored_bills <- NA
bills$cospon_match <- paste(bills$LES_sponsor, bills$coauthors, sep = '; ')
for(i in 1:nrow(all_sponsors)){
  c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
  all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match), fixed = TRUE))
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
all_sponsors$last_name <- gsub(',.+', '', all_sponsors$LES_sponsor)
all_sponsors$first_name <- ifelse(grepl(', ', all_sponsors$LES_sponsor), gsub('.+, ', '', all_sponsors$LES_sponsor), '')
all_sponsors$first_name <- gsub('\\.$', '', gsub(' .+', '', all_sponsors$first_name))

all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)

print(glue("-----> {nrow(all_sponsors)} UNIQUE SPONSORS IDENTIFIED IN BILL DATA "))

#### Update First/Last Names for Matching 
if(t_yrs %in% c("2008_2011", "2012_2015")){
  all_sponsors[all_sponsors$LES_sponsor == 'buck, kimberly campbell',]$last_name <-  "campbell"
}
if(t_yrs == "2012_2015"){
  all_sponsors[all_sponsors$LES_sponsor == 'collins',]$last_name <-  "adams"
}
if(t_yrs == "2016_2019"){
  all_sponsors[all_sponsors$LES_sponsor == 'turner-ford',]$last_name <-  "turner"
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
} %>% distinct() 

if(t_yrs == "2020_2023"){
  legiscan = legiscan %>% 
    filter(people_id != 6642)
  }


legiscan_adj = legiscan %>% 
  filter(committee_id == 0) %>% 
  group_by(last_name) %>% 
  mutate(n = n()) %>%
  ungroup() %>% 
  mutate(match_name = case_when(
    n >= 2 ~ paste0(last_name, " (",as.integer(gsub("[^0-9]", "", district)), ")"),
    T ~ last_name)) %>%
  mutate(match_name_chamber = tolower(paste(match_name,substr(district,1,1),sep="-")))



# inexact::inexact_addin()
# left: legiscan_adj
# right: all_sponsors
# method: osa
# mode: full

if(t_yrs == "2020_2023"){
  all_sponsors2 = 
    # You added custom matches:
    inexact::inexact_join(
      x  = legiscan_adj,
      y  = all_sponsors,
      by = "match_name_chamber",
      method = "osa",
      mode = "full",
      custom_match = c(
        "warren mcgee-h" = "mcgee-h",
        "potts parks-s" = "parks-s",
        "blackledge-h" = NA_character_,
        "bailey (23)-h" = NA_character_,
        "johnson (45)-s" = "johnson-s",
        "boyd (9)-s" = "boyd-s",
        "jackson (11)-h" = "jackson-h"
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
View(filter(bills, !(bills$LES_sponsor %in% legis_data$data_name)) %>% select(LES_sponsor, bill_url))
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
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES) # 
rm(t, terms, klarner_gs, m_sub, nonspon, unique_cospon, c_sub) # 
rm(commem_bills, t_sessions)

########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by SPECIAL ELECTION --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
# Historical Election Results (back to 2003): https://www.sos.ms.gov/Elections-Voting/Pages/Election-Results-By-Year.aspx
########################################################################################################################
### FULL ROSTER: 
#########################


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1996_1999 TERM! ~~~~~~~~~~~~~~
### WON SPECIAL ~ HOUSE:
# -- FLEMING; ISHEE; JENNINGS; MARKAM; MILES (William)
# -- ROBERSON; ROSS (charlie); THOMAS (sara); WALLACE; WEST
# -- SMITH, Clayton (Name Dup, won't print)
### WON SPECIAL ~ SENATE:
# -- FARRIS (ron)
# -- ROSS (charlie, via H in 1998, after winning H special in 1997)
#### DROP UNLESS 1996 DATA ADDED:
# -- bryant, phil -- appointed state auditor Nov. 1996
# -- seale, c. stevens (steve) -- left senate at some point in 1996, unclear when: https://www.linkedin.com/in/steve-seale-09a5b36
#### DROP:
# -- mills, mike -- appointed to state supreme court 1995
# -- sweet, dennis c. iii -- no evidnece he took the seat: http://www.sweetandassociates.net/dennis.htm


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2000_2003 TERM! ~~~~~~~~~~~~~~ 
# WON SPECIAL ~ HOUSE:
# -- HINES (john)
# WON SPECIAL ~ SENATE:
# -- WALDEN (charles)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2004_2007 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- COKERHAM; GARDNER; GREGORY (james gale); JOHNSON (robert l. iii)
# -- LANE; MORGAN (ken); NORQUIST; PALAZZO; WALLEY (j. shaun)
### WON SPECIAL ~ SENATE:
# -- CHASSANIOL; DAVIS (doug); FILLINGANE (joey)
# -- WHITE (richard, beat thomas after disupted general)
### DROP:
# -- thomas, dewayne -- never seated? Election disputed: http://t.jacksonfreepress.com/news/2004/jan/19/senate-panel-votes-for-new-thomaswhite-election/ ; lost special held in early february


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2008_2011 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- DELANO (scott)
# -- EURE  (casey)
# -- JONES (wilbert, name duplicate, won't print)
### WON SPECIAL ~ SENATE:
# -- COLLINS (nancy adams --> name switches in klarner for 2011 and 2015 elections)
# -- BUTLER (albert, name duplicate, won't print)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2012_2015 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- ANDERSON; DENTON; FAULKNER; JACKSON (lataisha m.)
# -- KINKADE; POWELL (brent); WILLIS (patricia)
### WON SPECIAL ~ SENATE:
# -- NORWOOD (sollie)
# -- PARKER (david)
# -- YOUNGER (charles)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2016_2019 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- ANTHONY (OTIS)
# -- CORLEY
# -- FORD (kevin)
# -- HARNESS (jeffery)
# -- HUDSON
# -- MCGEE
# -- ROSEBUD (tracey)
# -- SCOGGIN; 
# -- SHANKS (fred)
# -- TAYLOR
# -- TULLOS (kind of, see: https://ballotpedia.org/Mark_Tullos)
# -- WALLACE (price)
# -- WILKES
### WON SPECIAL ~ SENATE:
# -- CARTER (joel)
# -- MICHEL (j walter)
# -- WHALEY (neil)
### COSPONSOR CHECK:
# -- FORD = OKAY, no edits necessary
### DROP:
# -- longwitz, will -- appointed as a judge




# filter(klarner, grepl("jenifer", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(cand, year) %>% as.data.frame()
# filter(klarner, ddez == 45 & sen == 1) %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)
# filter(bills, grepl("ford", coauthors) & substring(bill_id,1,1) == 'S')


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

### -- JOEL CARTER (2016) != BRAD CARTER 1995
LES[LES$data_name %in% "carter" & LES$term == "2016_2019",]$klarner_id <- NA
LES[LES$data_name %in% "carter" & LES$term == "2016_2019",]$klarner_name <- NA
LES[LES$data_name %in% "carter" & LES$term == "2016_2019",]$sponsor <- 'carter, joel'

### Kevin Ford != Tim Ford
LES[LES$data_name %in% "ford" & LES$term == "2016_2019",]$klarner_id <- NA
LES[LES$data_name %in% "ford" & LES$term == "2016_2019",]$klarner_name <- NA
LES[LES$data_name %in% "ford" & LES$term == "2016_2019",]$sponsor <- 'ford, kevin'

### Price Wallace != Tom Wallace
LES[LES$data_name %in% "wallace" & LES$term == "2016_2019",]$klarner_id <- NA
LES[LES$data_name %in% "wallace" & LES$term == "2016_2019",]$klarner_name <- NA
LES[LES$data_name %in% "wallace" & LES$term == "2016_2019",]$sponsor <- 'wallace, price'

### ****Still missing***** ---> Rest are not in Klarner or 2017-2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[22]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, still_missing)


### Fix Missing
name_matches <- data.frame(LES_name = 'miles', k_name = 'miles, william (bill)')
name_matches <- add_row(name_matches, LES_name = 'thomas', k_name = 'thomas, sara richardson')
name_matches <- add_row(name_matches, LES_name = 'johnson', k_name = 'johnson, robert l. iii')
name_matches <- add_row(name_matches, LES_name = 'gregory', k_name = 'gregory, james gale')
name_matches <- add_row(name_matches, LES_name = 'walley', k_name = 'walley, j. shaun')
name_matches <- add_row(name_matches, LES_name = 'morgan', k_name = 'morgan, ken')
name_matches <- add_row(name_matches, LES_name = 'white', k_name = 'white, richard')
name_matches <- add_row(name_matches, LES_name = 'davis', k_name = 'davis, doug')
name_matches <- add_row(name_matches, LES_name = 'fillingane', k_name = 'fillingane, joey')
name_matches <- add_row(name_matches, LES_name = 'collins', k_name = 'adams, nancy collins')
name_matches <- add_row(name_matches, LES_name = 'powell', k_name = 'powell, brent')
name_matches <- add_row(name_matches, LES_name = 'jackson', k_name = 'jackson, lataisha m.')
name_matches <- add_row(name_matches, LES_name = 'parker', k_name = 'parker, david l.')
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

### Not In Klarner
fill_missing <- data.frame(LES_name = "jones, wilbert", new_name = 'jones, wilbert l.', party = 'd', district = 82, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper 
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

#### *** 2016_2019 Special winners: If any of below run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "hudson", c('party', 'sponsor')] <- list('d', "hudson, abe m. jr.")
LES[LES$sponsor == "scoggin", c('party', 'sponsor')] <- list('r', "scoggin, donnie")
LES[LES$sponsor == "gibbs, debra", c('party', 'sponsor')] <- list('d', "gibbs, debra h.")
LES[LES$sponsor == "ford, kevin", c('party', 'sponsor')] <- list('r', "ford, kevin")
LES[LES$sponsor == "wilkes", c('party', 'sponsor')] <- list('r', "wilkes, stacey hobgood")
LES[LES$sponsor == "corley", c('party', 'sponsor')] <- list('r', "corley, john g.")
LES[LES$sponsor == "shanks", c('party', 'sponsor')] <- list('r', "shanks, fred")
LES[LES$sponsor == "wallace, price", c('party', 'sponsor')] <- list('r', "wallace, price")
LES[LES$sponsor == "taylor", c('party', 'sponsor')] <- list('d', "taylor, cheikh a.")
LES[LES$sponsor == "anthony", c('party', 'sponsor')] <- list('d', "anthony, otis ii")
LES[LES$sponsor == "carter, joel", c('party', 'sponsor')] <- list('r', "carter, joel r. jr.")
LES[LES$sponsor == "whaley", c('party', 'sponsor')] <- list('r', "whaley, neil s.")
# LES[LES$sponsor == "zzzzzzzz", c('party', 'sponsor')] <- c('zzzzz', "zzzzzzz")

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t, fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)

# *** Note: Jenifer b. branning = Mispelled... if runs again in 2019, may be wrong in klarner

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
LES[LES$term == '2016_2019', set_NA] <- NA
rm(hf_data, set_NA)


#################################
### Shor and McCarty Data, 1993 - 2016
####################################

ideo <- readstata13::read.dta13("~/Dropbox/Data/Shor_McCarty_Data/shor_mccarty_1993_2016_individual_data_May_2018.dta")
ideo <- filter(ideo, st == this_state) %>% select(-st_id)
ideo$match_name <- str_extract(tolower(ideo$name), '[a-z]+, [a-z]+')
ideo$match_name <- ifelse(is.na(ideo$match_name), tolower(ideo$name), ideo$match_name)
ideo$last_name <- gsub(',.+', '', tolower(ideo$name))

# ************** THERE ARE A DECENT NUMBER OF DUPLICATES FOR LEGISLATORS THAT SPAN OVER 2015

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
LES[LES$sponsor %in% c('carter, joel r. jr.', 'gibbs, debra h.', 'johnson, thomas e.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# ** Notable Non-Mismatches:
# -- Thomas 'Randy' Mitchell; Irvin 'Lynn' Posey; Carl 'Jack' Gordon
# -- Alan 'Patrick' Nunnelee; Elton 'Greg' Snowden
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1991_1992', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('orc', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

### Lot of the missing are special election winners in 2016+
name_matches <- data.frame(LES_name = 'adams, nancy collins', SM_name = 'Adams Collins, Nancy')
#name_matches <- add_row(name_matches, LES_name = 'carter, joel r. jr.', SM_name = 'zzzzz') # NOT Carter = Brad Carter
#name_matches <- add_row(name_matches, LES_name = 'corley, john g.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'dixon, deborah butler', SM_name = 'Butler Dixon, Deborah')
name_matches <- add_row(name_matches, LES_name = 'foster, d. ted', SM_name = 'Foster')
#name_matches <- add_row(name_matches, LES_name = 'gibbs, debra h.', SM_name = 'zzzzzzz')
# ** Crossed with Danny Guice... non exclusive, name duplicate
#name_matches <- add_row(name_matches, LES_name = 'guice, jeffrey s. (jeff)', SM_name = 'zzzzzzz') 
name_matches <- add_row(name_matches, LES_name = 'gunn, mike', SM_name = 'Gunn')
name_matches <- add_row(name_matches, LES_name = 'hamilton, e. glenn', SM_name = 'Hamilton, Edwin Glenn')
name_matches <- add_row(name_matches, LES_name = 'hamilton, eugene forrest', SM_name = 'Hamilton, Eugene Forrest')
name_matches <- add_row(name_matches, LES_name = 'hill, angela burks', SM_name = 'Burks Hill, Angela')
name_matches <- add_row(name_matches, LES_name = 'hopson, briggs', SM_name = 'Hopson III, W. Briggs')
# name_matches <- add_row(name_matches, LES_name = 'hudson, abe m. jr.', SM_name = 'zzzzzzz')
### *** Two Rows, 1 year each
name_matches <- add_row(name_matches, LES_name = 'jackson, lataisha m.', SM_name = 'Jackson, Lataisha M')
name_matches <- add_row(name_matches, LES_name = 'johnson, thomas e.', SM_name = 'Johnson')
name_matches <- add_row(name_matches, LES_name = 'mims, sam c.', SM_name = 'Mims V, Sam C')
name_matches <- add_row(name_matches, LES_name = 'montgomery, pat', SM_name = 'Montgomery, B. Pat')
name_matches <- add_row(name_matches, LES_name = 'parks, rita potts', SM_name = 'Potts Parks, Rita')
name_matches <- add_row(name_matches, LES_name = 'rogers, ray', SM_name = 'Rogers, Nolan') # Nolan "Ray" Rogers
# name_matches <- add_row(name_matches, LES_name = 'scoggin, donnie', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'shirley, william e. jr.', SM_name = 'Shirley Jr, William E')
name_matches <- add_row(name_matches, LES_name = 'simmons, derrick t.', SM_name = 'Simmons, Derrick') # Two obs, other is Simmons, Derrick T.
# name_matches <- add_row(name_matches, LES_name = 'taylor, cheikh a.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'taylor, joe', SM_name = 'Taylor, Bobby')## ALmost certainly goes by Bobby; years overlap
# name_matches <- add_row(name_matches, LES_name = 'thomas, dewayne', SM_name = 'zzzzzzz')
## Second row is 'Turner, Bennie L.'
name_matches <- add_row(name_matches, LES_name = 'turner, bennie l.', SM_name = 'Turner, Bennie L')
name_matches <- add_row(name_matches, LES_name = 'walker, alfred l. jr.', SM_name = 'Walker')
name_matches <- add_row(name_matches, LES_name = 'walley, j. shaun', SM_name = 'Walley, J. Shaun')
name_matches <- add_row(name_matches, LES_name = 'watson, michael', SM_name = 'Watson Jr, Michael D')
# name_matches <- add_row(name_matches, LES_name = 'whaley, neil s.', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'wilkes, stacey hobgood', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'williams, charles van', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'zuber, henry b. (hank) iii', SM_name = 'Zuber III, Henry B')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

##########
### MORE DETAILED FIXES
##########

### Mark Baker (D74) wrongly listed as Larry Baker
LES[LES$sponsor == 'baker, mark',]$SM_name <-  ideo[ideo$name == 'Baker, Larry' & ideo$hdistrict2014 %in% '074',]$name
LES[LES$sponsor == 'baker, mark',]$SM_party <- ideo[ideo$name == 'Baker, Larry' & ideo$hdistrict2014 %in% '074',]$party
LES[LES$sponsor == 'baker, mark',]$np_score <- ideo[ideo$name == 'Baker, Larry' & ideo$hdistrict2014 %in% '074',]$np_score

### Clara Burnett Henderson Matched to Wrong Observation (serves 2004 to 2015)
LES[LES$sponsor == 'burnett, clara henderson',]$SM_name <-  ideo[ideo$name == 'Henderson Burnett',]$name
LES[LES$sponsor == 'burnett, clara henderson',]$SM_party <- ideo[ideo$name == 'Henderson Burnett',]$party
LES[LES$sponsor == 'burnett, clara henderson',]$np_score <- ideo[ideo$name == 'Henderson Burnett',]$np_score

### Clara Henderson Burnett in SM should be Cedric Burnett (elected 2015)
LES[LES$sponsor == 'burnett, cedric (bambam)',]$SM_name <-  ideo[ideo$name == 'Burnett, Clara Henderson',]$name
LES[LES$sponsor == 'burnett, cedric (bambam)',]$SM_party <- ideo[ideo$name == 'Burnett, Clara Henderson',]$party
LES[LES$sponsor == 'burnett, cedric (bambam)',]$np_score <- ideo[ideo$name == 'Burnett, Clara Henderson',]$np_score

### George 'Tommy' DIckerson -- May have run as Rep. in 2003 and lost? So random R row... but not clear if switch was early enough to matter for behavior (because then switches bacak in 2008)
LES[LES$sponsor == 'dickerson, tommy',]$SM_name <-  ideo[ideo$name == 'Dickerson, George' & ideo$party %in% 'D',]$name
LES[LES$sponsor == 'dickerson, tommy',]$SM_party <- ideo[ideo$name == 'Dickerson, George' & ideo$party %in% 'D',]$party
LES[LES$sponsor == 'dickerson, tommy',]$np_score <- ideo[ideo$name == 'Dickerson, George' & ideo$party %in% 'D',]$np_score

### Danny Guice --- Two Rows; other is a conflation with Jeff Guice
LES[LES$sponsor == 'guice, daniel d. (danny) jr.',]$SM_name <-  ideo[ideo$name == 'Guice, Daniel Jr.' & ideo$house1998 %in% 1,]$name
LES[LES$sponsor == 'guice, daniel d. (danny) jr.',]$SM_party <- ideo[ideo$name == 'Guice, Daniel Jr.' & ideo$house1998 %in% 1,]$party
LES[LES$sponsor == 'guice, daniel d. (danny) jr.',]$np_score <- ideo[ideo$name == 'Guice, Daniel Jr.' & ideo$house1998 %in% 1,]$np_score

### Steve Hale, Repeat Observation in 2015
LES[LES$sponsor == 'hale, steve',]$SM_name <-  ideo[ideo$name == 'Hale, Steve' & ideo$senate2012 %in% 1,]$name
LES[LES$sponsor == 'hale, steve',]$SM_party <- ideo[ideo$name == 'Hale, Steve' & ideo$senate2012 %in% 1,]$party
LES[LES$sponsor == 'hale, steve',]$np_score <- ideo[ideo$name == 'Hale, Steve' & ideo$senate2012 %in% 1,]$np_score

### John Hines (Sr) -- All Same Guy, 3 repeated observations
LES[LES$sponsor == 'hines, john w.',]$SM_name <-  ideo[ideo$name == 'Hines, John W' & ideo$house2001 %in% 1,]$name
LES[LES$sponsor == 'hines, john w.',]$SM_party <- ideo[ideo$name == 'Hines, John W' & ideo$house2001 %in% 1,]$party
LES[LES$sponsor == 'hines, john w.',]$np_score <- ideo[ideo$name == 'Hines, John W' & ideo$house2001 %in% 1,]$np_score

### Sampson Jackson (II) -- All Same Guy, 3 repeated observations
LES[LES$sponsor == 'jackson, sampson ii',]$SM_name <-  ideo[ideo$name == 'Jackson, Sampson' & ideo$senate1996 %in% 1,]$name
LES[LES$sponsor == 'jackson, sampson ii',]$SM_party <- ideo[ideo$name == 'Jackson, Sampson' & ideo$senate1996 %in% 1,]$party
LES[LES$sponsor == 'jackson, sampson ii',]$np_score <- ideo[ideo$name == 'Jackson, Sampson' & ideo$senate1996 %in% 1,]$np_score

### Brent Powell, Repeat Observations
LES[LES$sponsor == 'powell, brent',]$SM_name <-  ideo[ideo$name == 'Powell, Brent' & ideo$house2014 %in% 1,]$name
LES[LES$sponsor == 'powell, brent',]$SM_party <- ideo[ideo$name == 'Powell, Brent' & ideo$house2014 %in% 1,]$party
LES[LES$sponsor == 'powell, brent',]$np_score <- ideo[ideo$name == 'Powell, Brent' & ideo$house2014 %in% 1,]$np_score

### Charlie Smith, Switched from D to Nonmaj party, but don't observe partisanyears
LES[LES$sponsor == 'smith, charlie',]$SM_name <-  ideo[ideo$name == 'Smith, Charlie' & ideo$party %in% 'X',]$name
LES[LES$sponsor == 'smith, charlie',]$SM_party <- ideo[ideo$name == 'Smith, Charlie' & ideo$party %in% 'X',]$party
LES[LES$sponsor == 'smith, charlie',]$np_score <- ideo[ideo$name == 'Smith, Charlie' & ideo$party %in% 'X',]$np_score

### Charles Young -- 3 rows, two are Jr, 1 is Sr -- the one with jr in it is a single off year
LES[LES$sponsor == 'young, charles l. jr.',]$SM_name <-  ideo[ideo$name == 'Young, Charles' & ideo$house2012 %in% 1,]$name
LES[LES$sponsor == 'young, charles l. jr.',]$SM_party <- ideo[ideo$name == 'Young, Charles' & ideo$house2012 %in% 1,]$party
LES[LES$sponsor == 'young, charles l. jr.',]$np_score <- ideo[ideo$name == 'Young, Charles' & ideo$house2012 %in% 1,]$np_score

LES[LES$sponsor == 'young, charles l. sr.',]$SM_name <-  ideo[ideo$name == 'Young, Charles' & ideo$house1996 %in% 1,]$name
LES[LES$sponsor == 'young, charles l. sr.',]$SM_party <- ideo[ideo$name == 'Young, Charles' & ideo$house1996 %in% 1,]$party
LES[LES$sponsor == 'young, charles l. sr.',]$np_score <- ideo[ideo$name == 'Young, Charles' & ideo$house1996 %in% 1,]$np_score


########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########
# filter(LES, grepl("carmich", sponsor)) %>% select(1:7, party)

#### Fredie 'Videt' Carmichael
LES[LES$sponsor == 'carmichael, videt' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Carmichael, Fredie' & ideo$party == 'D',]$name
LES[LES$sponsor == 'carmichael, videt' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Carmichael, Fredie' & ideo$party == 'D',]$party
LES[LES$sponsor == 'carmichael, videt' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Carmichael, Fredie' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'carmichael, videt' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Carmichael, Fredie' & ideo$party == 'R',]$name
LES[LES$sponsor == 'carmichael, videt' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Carmichael, Fredie' & ideo$party == 'R',]$party
LES[LES$sponsor == 'carmichael, videt' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Carmichael, Fredie' & ideo$party == 'R',]$np_score

#### Gavin 'Frank' Hamilton
LES[LES$sponsor == 'hamilton, frank' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Hamilton, Gavin' & ideo$party == 'D',]$name
LES[LES$sponsor == 'hamilton, frank' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Hamilton, Gavin' & ideo$party == 'D',]$party
LES[LES$sponsor == 'hamilton, frank' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Hamilton, Gavin' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'hamilton, frank' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Hamilton, Gavin' & ideo$party == 'R',]$name
LES[LES$sponsor == 'hamilton, frank' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Hamilton, Gavin' & ideo$party == 'R',]$party
LES[LES$sponsor == 'hamilton, frank' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Hamilton, Gavin' & ideo$party == 'R',]$np_score

### There are others but unclear if we can do anything about this:
# See: https://en.wikipedia.org/wiki/Political_party_strength_in_Mississippi
# ----> See footnotes ~32+
# See: https://books.google.com/books?id=UXDjCgAAQBAJ&pg=PA87&lpg=PA87&dq=james+walley+mississippi+party+switch&source=bl&ots=G9hF2Oxlsz&sig=ACfU3U1UG0PllpG61TCsMJ1srF0dXVJY0w&hl=en&sa=X&ved=2ahUKEwjdq66zxPHkAhVuU98KHccXBxAQ6AEwC3oECGYQAQ#v=onepage&q=james%20walley%20mississippi%20party%20switch&f=false
# https://www.dailyleader.com/2010/12/28/sen-hyde-smith-joins-republicans/

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
LES[LES$sponsor == 'hydesmith, cindy',]$sponsor <- 'hyde-smith, cindy'
LES[LES$sponsor == 'wellssmith, carmel',]$sponsor <- 'wells-smith, carmel'
LES[LES$sponsor == 'compretta, j. p.',]$sponsor <- 'compretta, joseph p.'
LES[LES$sponsor == 'barnett, les',]$sponsor <- 'barnett, lester'
LES[LES$sponsor == 'markham, j. b. jr.',]$sponsor <- 'markham, james b. jr.'
LES[LES$sponsor == 'posey, lynn',]$sponsor <- 'posey, irvin lynn'
LES[LES$sponsor == 'canon, dr. william',]$sponsor <- 'canon, william ward'
LES[LES$sponsor == 'furniss, delma',]$sponsor <- 'furniss, vernon delma'
LES[LES$sponsor == 'snowden, greg',]$sponsor <- 'snowden, elton greg'
LES[LES$sponsor == 'moore, o. k.',]$sponsor <- 'moore, oscar kenney'
LES[LES$sponsor == 'hughes, j. p. jr.',]$sponsor <- 'hughes, jay p. jr.'
# LES[LES$sponsor == 'zzzzz',]$sponsor <- 'zzzzzz'


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1996 - 2019
LES[as.numeric(substring(LES$term,1,4)) %in% c(1996:2011) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2012:2019) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1996 - 2019
# -- Republicans gained control in 2007, the final year of 4-year term: https://books.google.com/books?id=UXDjCgAAQBAJ&pg=PA87&lpg=PA87&dq=james+walley+mississippi+party+switch&source=bl&ots=G9hF2Oxlsz&sig=ACfU3U1UG0PllpG61TCsMJ1srF0dXVJY0w&hl=en&sa=X&ved=2ahUKEwjdq66zxPHkAhVuU98KHccXBxAQ6AEwC3oECGYQAQ#v=onepage&q=james%20walley%20mississippi%20party%20switch&f=false
# -- Resulted from a special election win and 2 party switchers (James Walley and Tommy Gollot)
# -- Dems won control again in 2007 electins... http://www.jacksonfreepress.com/news/2007/nov/14/dems-bolster-power-in-legislature/
# -- Bust lost it again in in 2011, again in final year of 4-year term
LES[as.numeric(substring(LES$term,1,4)) %in% c(1996:2003) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2012:2019) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1

### Coding BOTH parties as majority for 2004_2007 and 2008_2011 -- R's took over in final year of 4 year term both times
# -- Lots of party switches during this time (both lt gov and president pro ten)
# -- https://www.picayuneitem.com/2006/12/miss-senate-president-pro-tem-not-seeking-re-election/
LES[LES$term %in% c("2004_2007", "2008_2011") & LES$chamber == 'Senate',]$in_majority <- 1


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

##### CHECK OUTLIERS 
# filter(LES, party == 'd' & np_score > 0 & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% arrange(sponsor) %>% as.data.frame() 
# filter(LES, party == 'r' & np_score < 0 & party != tolower(SM_party)) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% arrange(sponsor) 

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

