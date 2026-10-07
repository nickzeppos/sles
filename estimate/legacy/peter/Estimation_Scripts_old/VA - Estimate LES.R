

########################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** VIRGINIA *** BY SESSION
###############################################################

## ******************************************************
# ********** DO NOT HAVE S&S ARTICLES FOR 1994-1995 OR 2017 - 2019
# **************************************************
# ********** Can't Estimate 2018_2019 Scores as DO NOT HAVE LEGISLATIVE DATA
# ***************************************************************

###################################
## (SPECIAL) SESSIONS:
## ---- Bills carryover from regular session to regular session (even to odd year)
## ------> BUT ARE RECORDED SEPERATELY BY YEAR --> so if reintroduced/carried over, keeps bill number, and action is recorded on 2nd sessions page (with first page not updated)
## ---- Special Sessions seperated out AND uniquely identified by number (eg num > 2000)
## ------> **ONE EXCEPTION**: 2001-SS1 bill numbers start at HB/SB1
## MEMBER LISTS:
## ---- 
## PROCESS/RULES:
## ---- 
## Sponsorship/Authorship
## ---- 
###########################
##### NOTES:
## (1) See filter(bills, sponsor == '') %>% View() ---- Some bills don't have a chief patron identified
## ------> See http://lis.virginia.gov/cgi-bin/legp604.exe?001+sum+SB650 /// http://lis.virginia.gov/cgi-bin/legp604.exe?001+mbr+SB650
## ------> If you read the text, introducing sponsors is Marsh, but not indicated in website
## ------> ONLY 41 of these however... 
## (2) Bills sponsored by elmo cross and hunter andres in 96/97 --> they lost election but prefiled the bills in 95!
## ------> Currently dropping both AND the 1 and 2 bills, repsectively, that they sponsor
## ------> Also present for 2 members in 2000_2001; 2004_2005
## ------> DROPPING BEFORE GATHERING SPONSOR NAMES
## (3) Give any credit for "Incorporated into other legislation"??? 
## -----> At present, NO
############

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

this_state <- 'VA'
min_year <- 1994
max_year <- 2017
keep_types <- c("HB", "SB")
spec_elec_codes <- c('s')
house_term_length <- 2
sen_term_length <- 4 # STAGGERED? NO

#### Output Directory
#dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Terms
terms <- seq(min_year, max_year, 2)

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("~/Dropbox/Data/State Legislative Data/Commem Bills/{this_state}_Commem_Bills.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- read.csv("~/Dropbox/Data/State Legislative Data/Significant Bills/SS_Data/SS_Bills.csv")
SS_bills <- SS_bills %>% 
  filter(state == this_state) %>%
  rename(bill_id = bill_num) %>%
  mutate(term = ifelse(year %% 2 == 0, paste0(year, "_", year + 1), paste0(year - 1, "_", year)),
         bill_id = toupper(bill_id),
         SS = 1) %>%
  select(state, term, year, bill_id, everything())

#### Check SS BIll Types
# table(SS_bills$bill_type)

### Load Klarner Data 
load("~/Dropbox/Data/US Election Data/state_leg/klarner_data/196slers1967to2016_20180908.RData")
klarner <- table; rm(table)
klarner$sab <- toupper(klarner$sab)
klarner <- filter(klarner, year > min_year - 5 & sab == this_state)
klarner <- klarner %>% 
  select(caseid, year, sab, sen, outcome, etype, cand, candid, party, partyz, deter, ddez,
         sfips, dname, dno, deter, cand, candid, party, partyz, partyt, middle, term, termz,
         cando, exper)

#### Fix Klarner Errors
klarner[klarner$cand == 'vanlandigham, marian a.',]$cand <- 'vanlandingham, marian a.'
## --> BA Wilcox Coded as Winning but Yvonne Miller Did... She's just not there... https://ballotpedia.org/Yvonne_Miller
klarner[klarner$cand == 'wilcox, b. a.' & klarner$year == 1995,]$outcome <- 'l'
klarner$caseid <- as.character(klarner$caseid)
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 1995, sab = "VA", sen = 1, outcome = 'w', etype = 'g', cand = 'miller, yvonne b.', candid = 233051, party = 'democrat', partyz = 'd', deter = 1, ddez = "5")

### Same Person
klarner[klarner$cand == 'dillard, james e.',]$cand <- 'dillard, james h. ii' # https://en.wikipedia.org/wiki/Jim_Dillard

### Other Errors
# --> robert s bloxom jr and rober s bloxom sr == Same ID ---> Keeping as is for now (including in handcoded special below)

### Add Missing Races to Klarner Data 2008+ --- Off-setting years in some cases or else won't match right
# ----> **** ADDED THESE prior to changing process to just ID specials and not fill in.... *******
# Stephen H. Martin --- Won Special while in House --- https://en.wikipedia.org/wiki/Steve_Martin_(Virginia_politician)
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 1994, sab = "VA", sen = 1, outcome = 'w', etype = 's', cand = 'martin, stephen h.', candid = 232316, party = 'republican', partyz = 'r', deter = 1 )
# Albert Pollard VA 2008 Special to Fill Vacancy by Rob Wittman: https://www.ourcampaigns.com/RaceDetail.html?RaceID=406396
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2008, sab = "VA", sen = 0, outcome = 'w', etype = 's', cand = 'pollard, albert c. jr.', candid = 237270, party = 'democrat', partyz = 'd', deter = 1 )
# Barry Knight 2009 Special : https://web.archive.org/web/20110522192707/https://www.voterinfo.sbe.virginia.gov/election/DATA/2008/713AAEC0-0129-40BF-B769-DCC60E9CEF8E/Unofficial/8_s.shtml
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2009, sab = "VA", sen = 0, outcome = 'w', etype = 's', cand = 'knight, barry d.', candid = 306655, party = 'modernrepublican', partyz = 'r', deter = 1 )
# Charniele Herring  - 2009 Special -- https://www.richmond.com/news/va-house-swears-in-delegate-after-recount/article_ee04e3e9-51bf-5df7-aa64-3fc48b0f42ad.html
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2009, sab = "VA", sen = 0, outcome = 'w', etype = 's', cand = 'herring, charniele l.', candid = 306563, party = 'democrat', partyz = 'd', deter = 1 )
# McWaters - 2010 Special for Senate -- https://en.wikipedia.org/wiki/Jeff_McWaters#Political_career
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2010, sab = "VA", sen = 1, outcome = 'w', etype = 's', cand = 'mcwaters, jeffrey l. (jeff)', candid = 320336, party = 'modernrepublican', partyz = 'r', deter = 1 )
# Tony Wilt - 2010 Special - https://www.ourcampaigns.com/RaceDetail.html?RaceID=651645
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2010, sab = "VA", sen = 0, outcome = 'w', etype = 's', cand = 'wilt, tony o.', candid = 320433, party = 'modernrepublican', partyz = 'r', deter = 1 )
# Eileen Filler-Corn - 2010 Special - https://web.archive.org/web/20101014153111/https://www.voterinfo.sbe.virginia.gov/election/DATA/2010/839A92A6-A02A-4D1E-B5F8-91867A04D47D/Official/8_s.shtml
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2010, sab = "VA", sen = 0, outcome = 'w', etype = 's', cand = 'fillercorn, eileen', candid = 320456, party = 'democrat', partyz = 'd', deter = 1 )
# Roxann Robinson - 2010 Special - https://www.ourcampaigns.com/RaceDetail.html?RaceID=651646
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2010, sab = "VA", sen = 0, outcome = 'w', etype = 's', cand = 'robinson, roxann l.', candid = 320434, party = 'modernrepublican', partyz = 'r', deter = 1 )
# Dave Marsden - 2010 Special - https://en.wikipedia.org/wiki/David_W._Marsden
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2010, sab = "VA", sen = 1, outcome = 'w', etype = 's', cand = 'marsden, dave w.', candid = 270385, party = 'democrat', partyz = 'd', deter = 1 )
# Greg Habeeb -- 2011 Special -- https://www.ourcampaigns.com/RaceDetail.html?RaceID=702282
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2011, sab = "VA", sen = 0, outcome = 'w', etype = 's', cand = 'habeeb, gregory d.', candid = 320405, party = 'modernrepublican', partyz = 'r', deter = 1 )
# William Stanley Jr -- 2011 Special -- https://www.ourcampaigns.com/RaceDetail.html?RaceID=702281
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2011, sab = "VA", sen = 1, outcome = 'w', etype = 's', cand = 'stanley, william m. jr.', candid = 320355, party = 'modernrepublican', partyz = 'r', deter = 1 )
# Rob Krupicka -- 2012 Special -- https://www.ourcampaigns.com/RaceDetail.html?RaceID=773263
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2012, sab = "VA", sen = 0, outcome = 'w', etype = 's', cand = 'krupicka, k. rob', candid = NA, party = 'democrat', partyz = 'd', deter = 1 )
#### ----> He also won in 2013 https://www.ourcampaigns.com/RaceDetail.html?RaceID=784154 -- but Klarner has a guy ALan Krepela who actually won in 1969???
#### ---> Dropping Krepela and Adding Krupicka for subsequent term
klarner <- filter(klarner, !(year == 2013 & sab == "VA" & ddez == 45 & grepl('krepela', cand)))
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2013, sab = "VA", sen = 0, outcome = 'w', etype = 'g', cand = 'krupicka, k. rob', candid = NA, party = 'democrat', partyz = 'd', ddez = "45", deter = 1 )
#### Sam Rasoul -- 2014 Special -- http://historical.elections.virginia.gov/candidates/view/Salam-Rasoul
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2014, sab = "VA", sen = 0, outcome = 'w', etype = 's', cand = 'rasoul, s. (sam)', candid = 360628, party = 'democrat', partyz = 'd', deter = 1 )
#### Kenneth Alexander -- 2012 Special -- http://historical.elections.virginia.gov/elections/view/35364/
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2012, sab = "VA", sen = 1, outcome = 'w', etype = 's', cand = 'alexander, kenneth c.', candid = 236962, party = 'democrat', partyz = 'd', deter = 1 )
### John Cosgrove -- 2013 Special -- http://historical.elections.virginia.gov/elections/view/44929/
### ** Technically won in 2013, but coding as 2014 because he took seat AFTER session ended and has no activity in S, (but is highly ranked in House)
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2014, sab = "VA", sen = 1, outcome = 'w', etype = 's', cand = 'cosgrove, john a. jr.', candid = 236655, party = 'modernrepublican', partyz = 'r', deter = 1 )
#### Joseph Lindsey -- 2014 Speciacl -- http://historical.elections.virginia.gov/elections/view/25025/
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2014, sab = "VA", sen = 0, outcome = 'w', etype = 's', cand = 'lindsey, joseph c. (joe)', candid = 361234, party = 'democrat', partyz = 'd', deter = 1 )
#### Rip Sullivan -- 2014 Special -- http://historical.elections.virginia.gov/elections/view/25023/
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2014, sab = "VA", sen = 0, outcome = 'w', etype = 's', cand = 'sullivan, richard c. rip jr.', candid = 360910, party = 'democrat', partyz = 'd', deter = 1 )
#### Robert Bloxom -- 2014 Special -- http://historical.elections.virginia.gov/elections/view/35077/
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2014, sab = "VA", sen = 0, outcome = 'w', etype = 's', cand = 'bloxom, robert s.', candid = 233248, party = 'modernrepublican', partyz = 'r', deter = 1 )
#### Todd Pillion -- 2014 Special -- http://historical.elections.virginia.gov/elections/view/35078/
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2014, sab = "VA", sen = 0, outcome = 'w', etype = 's', cand = 'pillion, todd e.', candid = 360567, party = 'modernrepublican', partyz = 'r', deter = 1 )
#### Ben Chafin --- 2014 Sepcial for Senate -- http://historical.elections.virginia.gov/elections/view/25024/
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2014, sab = "VA", sen = 1, outcome = 'w', etype = 's', cand = 'chafin, a. benton jr.', candid = 355015, party = 'modernrepublican', partyz = 'r', deter = 1 )
#### Lynwood Lewis --- 2014 Sepcial for Senate -- http://historical.elections.virginia.gov/elections/view/35075/
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2014, sab = "VA", sen = 1, outcome = 'w', etype = 's', cand = 'lewis, l. w. jr.', candid = 232753, party = 'democrat', partyz = 'd', deter = 1 )
#### Kathleen Murphy -- Lost 2013 General, Won 2015 Special -- https://www.elections.virginia.gov/index.php/resultsreports/election-results/2015-election-results/01062015special.html
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2015, sab = "VA", sen = 0, outcome = 'w', etype = 's', cand = 'murphy, kathleen j.', candid = 355265, party = 'democrat', partyz = 'd', deter = 1 )
#### Joseph Preston -- 2015 Special -- http://historical.elections.virginia.gov/elections/view/34781/
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2015, sab = "VA", sen = 0, outcome = 'w', etype = 's', cand = 'preston, joseph e.', candid = 360152, party = 'democrat', partyz = 'd', deter = 1 )
#### Jennifer Wexton -- 2014 Special -- http://historical.elections.virginia.gov/elections/view/35074/
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2014, sab = "VA", sen = 1, outcome = 'w', etype = 's', cand = 'wexton, jennifer t.', candid = 360450, party = 'democrat', partyz = 'd', deter = 1 )
#### Lionell Spruill Sr -- 2016 Off-Cycle -- http://historical.elections.virginia.gov/elections/view/81025/
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2016, sab = "VA", sen = 1, outcome = 'w', etype = 's', cand = 'spruill, lionell sr.', candid = 236633, party = 'democrat', partyz = 'd', deter = 1 )
#### T Monty Mason -- 2016  Off-Cycle -- http://historical.elections.virginia.gov/elections/view/80942/
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2016, sab = "VA", sen = 1, outcome = 'w', etype = 's', cand = 'mason, t. monty', candid = 355615, party = 'democrat', partyz = 'd', deter = 1 )
#### Lionell Spruill Sr -- 2017 Special -- http://historical.elections.virginia.gov/elections/view/85215/
klarner <- add_row(klarner, caseid = 'PB_MANUAL', year = 2017, sab = "VA", sen = 1, outcome = 'w', etype = 's', cand = 'mcclellan, jennifer l.', candid = 270434, party = 'democrat', partyz = 'd', deter = 1 )

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[13]

for(t in terms){
  
  ### Formulate 2-year terms -- Cover both regular and special sessions
  t_yrs <- as.character(glue('{t}_{t+1}'))
  
  ### Skip Previously Estimated
  if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
    print(glue('. \n ~~~~ SKIPPING {t_yrs} TERM ---> LES scores already estimated! \n.'))
    next
  }
  
  ###### SESSION IN PROGRESS
  print(glue('. \n . \n ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE {t_yrs} TERM! ~~~~~~~~~~~~~~ '))
  
  ############### Read in data
  bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t_yrs}.csv")
  bills <- read_csv(bill_path, col_types = cols())
  
  ### Clean Term/Session Variables
  bills$term <- gsub('-', '_', bills$term)
  
  bills <- bills %>% 
    mutate(session_type = substring(session, 6, nchar(session)),
           session_type = recode(session_type, 'SESSION' = 'RS', 'SPECIAL SESSION I' = 'SS1', 'SPECIAL SESSION II' = 'SS2',
                                 'SPECIAL SESSION III' = 'SS3', 'SPECIAL SESSION IV' = 'SS4'),
           session = paste0(session_year, '-', session_type)) %>%
    select(-session_type)
  
  ### Remove Exact Duplicates
  bills <- distinct(bills) %>% arrange(term, session, bill_id)
  
  ############### Drop Resolutions, Messages, Communications, Reports
  bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
  # table(bills$bill_type)
  bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)
  
  ##########################
  ####### Standardize Sponsors
  
  ###### Dropping 2nd Chief Patron, Fixing Names, Removing Bills without Chief Patron
  bills$sponsor <- str_trim(gsub("\\(chief patron\\)", "", tolower(bills$sponsor)))
  bills$sponsor <- str_trim(gsub('\\\n.+| \\(.+\\)$|;.+|-resigned.+|-seat vac.+', '', bills$sponsor))

  #### Standardize
  bills$sponsor <- gsub('á', 'a', bills$sponsor)
  bills$sponsor <- gsub('é', 'e', bills$sponsor)
  bills$sponsor <- gsub('ó', 'o', bills$sponsor)
  bills$sponsor <- gsub('í', 'i', bills$sponsor)
  bills$sponsor <- gsub('ñ', 'n', bills$sponsor)
  bills$sponsor <- gsub('\\.|,', '', bills$sponsor)
  
  bills$senate_sponsors <- gsub('á', 'a', bills$senate_sponsors)
  bills$senate_sponsors <- gsub('é', 'e', bills$senate_sponsors)
  bills$senate_sponsors <- gsub('ó', 'o', bills$senate_sponsors)
  bills$senate_sponsors <- gsub('í', 'i', bills$senate_sponsors)
  bills$senate_sponsors <- gsub('ñ', 'n', bills$senate_sponsors)
  bills$senate_sponsors <- tolower(gsub('\\.|,', '', bills$senate_sponsors))
  
  bills$house_sponsors <- gsub('á', 'a', bills$house_sponsors)
  bills$house_sponsors <- gsub('é', 'e', bills$house_sponsors)
  bills$house_sponsors <- gsub('ó', 'o', bills$house_sponsors)
  bills$house_sponsors <- gsub('í', 'i', bills$house_sponsors)
  bills$house_sponsors <- gsub('ñ', 'n', bills$house_sponsors)
  bills$house_sponsors <- tolower(gsub('\\.|,', '', bills$house_sponsors))
  
  ### Eliminat Nicknames
  bills$sponsor <- gsub('  +', ' ', gsub('\\".+\\"|\\(.+\\)', '', bills$sponsor))
  bills$senate_sponsors <- gsub('  +', ' ', gsub('\\".+\\"|\\(.+\\)', '', bills$senate_sponsors))
  bills$house_sponsors <- gsub('  +', ' ', gsub('\\".+\\"|\\(.+\\)', '', bills$house_sponsors))
  
  #### Fix Name Issues
  # bills$sponsor <- gsub('zzzzz($|;)', 'zzzzz', bills$sponsor)
  # bills$house_sponsors <- gsub('zzzzz($|;)', 'zzzzz', bills$house_sponsors)  
  # bills$senate_sponsors <- gsub('zzzzz($|;)', 'zzzzz', bills$senate_sponsors)  
  if(t_yrs == '1994_1995'){
    bills$sponsor <- gsub('jim m shuler', 'james m shuler', bills$sponsor)
    bills$house_sponsors <- gsub('jim m shuler', 'james m shuler', bills$house_sponsors)
    bills$sponsor <- gsub('john j davies($|;)', 'john j davies iii;', bills$sponsor)
    bills$house_sponsors <- gsub('john j davies($|;)', 'john j davies iii;', bills$house_sponsors)
    bills$sponsor <- gsub('phillip hamilton', 'phillip a hamilton', bills$sponsor)
    bills$house_sponsors <- gsub('phillip hamilton', 'phillip a hamilton', bills$house_sponsors)   
    bills$sponsor <- gsub('raymond r guest($|;)', 'raymond r guest jr;', bills$sponsor)
    bills$house_sponsors <- gsub('raymond r guest($|;)', 'raymond r guest jr;', bills$house_sponsors) ### Sr died in 1991
  }else if(t_yrs == '2000_2001'){
    bills$sponsor <- gsub('bill bolling', 'william t bolling', bills$sponsor)
    bills$senate_sponsors <- gsub('bill bolling', 'william t bolling', bills$senate_sponsors)
    bills$sponsor <- gsub('^nick rerras', 'd nick rerras', bills$sponsor)
    bills$senate_sponsors <- gsub('^nick rerras', 'd nick rerras', bills$senate_sponsors)
    bills$senate_sponsors <- gsub('; nick rerras', '; d nick rerras', bills$senate_sponsors)
  }else if(t_yrs == '2002_2003'){
    bills$sponsor <- gsub('john a rollison($|;)', 'john a rollison iii;', bills$sponsor)
    bills$house_sponsors <- gsub('john a rollison($|;)', 'john a rollison iii;', bills$house_sponsors)
    # James = Jay - https://www.washingtonpost.com/archive/local/2001/11/01/james-k-jay-obrien-jr-r/0bd76d74-dd72-4fc4-b35a-955bfbe8b50c/?utm_term=.118f1f181cb7
    bills$sponsor <- gsub("jay o'brien", "james k o'brien jr", bills$sponsor)
    bills$senate_sponsors <- gsub("jay o'brien", "james k o'brien jr", bills$senate_sponsors)
  }else if(t_yrs == '2004_2005'){
    bills$sponsor <- gsub('jeannemarie devolites davis', 'jeannemarie d davis', bills$sponsor)
    bills$senate_sponsors <- gsub('jeannemarie devolites davis', 'jeannemarie d davis', bills$senate_sponsors)  
    bills$sponsor <- gsub('james h dillard($|;)', 'james h dillard ii;', bills$sponsor)
    bills$house_sponsors <- gsub('james h dillard($|;)', 'james h dillard ii;', bills$house_sponsors)  
  }else if(t_yrs == '2008_2009'){
    bills$sponsor <- gsub('dan c bowling', 'danny c bowling', bills$sponsor)
    bills$house_sponsors <- gsub('dan c bowling', 'danny c bowling', bills$house_sponsors)  
    bills$sponsor <- gsub('frank p hall', 'franklin p hall', bills$sponsor)
    bills$house_sponsors <- gsub('frank p hall', 'franklin p hall', bills$house_sponsors)
    bills$sponsor <- gsub('kenneth melvin', 'kenneth r melvin', bills$sponsor)
    bills$house_sponsors <- gsub('kenneth melvin', 'kenneth r melvin', bills$house_sponsors)
    bills$sponsor <- gsub('robert b bell($|;)', 'robert b bell iii;', bills$sponsor)
    bills$house_sponsors <- gsub('robert b bell($|;)', 'robert b bell iii;', bills$house_sponsors)
  }else if(t_yrs == '2010_2011'){
    bills$sponsor <- gsub('bill janis', 'william r janis', bills$sponsor)
    bills$house_sponsors <- gsub('bill janis', 'william r janis', bills$house_sponsors)  
  }else if(t_yrs == '2012_2013'){
    bills$sponsor <- gsub('s r iaquinto', 'salvatore r iaquinto', bills$sponsor)
    bills$house_sponsors <- gsub('s r iaquinto', 'salvatore r iaquinto', bills$house_sponsors)  
  }else if(t_yrs == '2014_2015'){
    bills$sponsor <- gsub('hyland f fowler($|;)', 'hyland f fowler jr;', bills$sponsor)
    bills$house_sponsors <- gsub('hyland f fowler($|;)', 'hyland f fowler jr;', bills$house_sponsors)
    bills$sponsor <- gsub('j morrissey', 'joseph d morrissey', bills$sponsor)
    bills$house_sponsors <- gsub('j morrissey', 'joseph d morrissey', bills$house_sponsors)  
    bills$sponsor <- gsub('r lee ware($|;)', 'r lee ware jr;', bills$sponsor)
    bills$house_sponsors <- gsub('r lee ware($|;)', 'r lee ware jr;', bills$house_sponsors)  
    bills$sponsor <- gsub('t monty mason', 't montgomery mason', bills$sponsor)
    bills$house_sponsors <- gsub('t monty mason', 't montgomery mason', bills$house_sponsors)  
  }else if(t_yrs == "2018_2019"){
    bills$sponsor <- gsub(" - resigned.+", "", bills$sponsor)
    bills$house_sponsors <- gsub(" - resigned [0-9]+\\/[0-9]+", "", bills$house_sponsors)
    bills$senate_sponsors <- gsub(" - resigned [0-9]+\\/[0-9]+", "", bills$senate_sponsors)
  }
  

  ####################
  ### LES SPONSOR Var
  bills$LES_sponsor <- gsub(';$', '', bills$sponsor)
  # table(bills$LES_sponsor)
  
  ### Filling in Missing Sponsors Where Possible
  if(nrow(filter(bills, LES_sponsor == '')) > 0 | any(is.na(bills$LES_sponsor))){
    bills$LES_sponsor <- ifelse(is.na(bills$LES_sponsor) & grepl('^S', bills$bill_id) & !grepl(';', bills$senate_sponsors), tolower(bills$senate_sponsors), bills$LES_sponsor)
    bills$LES_sponsor <- ifelse(bills$LES_sponsor == '' & grepl('^S', bills$bill_id) & !grepl(';', bills$senate_sponsors), tolower(bills$senate_sponsors), bills$LES_sponsor)
    bills$LES_sponsor <- ifelse(is.na(bills$LES_sponsor) & grepl('^H', bills$bill_id) & !grepl(';', bills$house_sponsors), tolower(bills$house_sponsors), bills$LES_sponsor)
    bills$LES_sponsor <- ifelse(bills$LES_sponsor == '' & grepl('^H', bills$bill_id) & !grepl(';', bills$house_sponsors), tolower(bills$house_sponsors), bills$LES_sponsor)
  }
  
  ### Fix Bills W/out Chief Patron (~41 Total, including above fixes, 1994 - 2017) --> Can't use full sponsor lists to fill in automatically if multiple because alphabetized
  ### ---> Filling in by looking at the text of the introduced bill
  # filter(bills, LES_sponsor == "" | is.na(bills$LES_sponsor)) %>% select(bill_id, term, house_sponsors, senate_sponsors, bill_url) %>% as.data.frame()
  # ** Weirdly, lots of similar bill numbers have this issue...
  if(t_yrs == '1994_1995'){
    bills[bills$bill_id == 'SB0377',]$LES_sponsor <- 'mark l earley'
    bills[bills$bill_id == 'SB0651',]$LES_sponsor <- 'william c wampler jr'
  }else if(t_yrs == '1998_1999'){ 
    bills[bills$bill_id == 'SB0377',]$LES_sponsor <- 'thomas k norment jr'
  }else if(t_yrs == '2000_2001'){
    bills[bills$bill_id == 'SB0650',]$LES_sponsor <- 'henry l marsh iii'
    bills[bills$bill_id == 'SB0651',]$LES_sponsor <- 'henry l marsh iii'
  }else if(t_yrs == '2006_2007'){
    bills[bills$bill_id == 'SB0651',]$LES_sponsor <- 'phillip p puckett'
  }else if(t_yrs == '2010_2011'){
    bills[bills$bill_id == 'SB0608',]$LES_sponsor <- 'john s edwards'
    bills[bills$bill_id == 'SB1070',]$LES_sponsor <- 'john s edwards'
  }
  
  ### Checking if any more missing
  if(nrow(filter(bills, LES_sponsor == '')) > 0 | any(is.na(bills$LES_sponsor)) ){
    cat('BILLS MISSING SPONSOR -------> FILL THEM IN! --------> BREAK'); break
  }
  
  ####### Keep only the SECOND RECORD for each Carry Over Bill
  # ---> See, e.g, HB117 in 1994 and 1995: https://lis.virginia.gov/cgi-bin/legp604.exe?951+sum+HB117&951+sum+HB117 
  # ---> Carried over, and action continues to be recorded on 1995 page
  # *** Below Sorts by Session (Most to Lease Recent) and -- when duplicated (and thus carried over) -- keeps only most recent record
  # ---> Doing this for both SHORT TITLE and SUMMARY as one occassionally changes but bills still functionally identical
  # ---> Also doing this after sponsor cleaning as occassionally differences across sessions
  #bills <- bills %>% arrange(desc(session), bill_id) %>% distinct(bill_id, term, short_title, LES_sponsor, .keep_all = TRUE) 
  #bills <- bills %>% arrange(desc(session), bill_id) %>% distinct(bill_id, term, summary, LES_sponsor, .keep_all = TRUE) 
  ### Instead: Just dropping Even-Year Bills when Duplicated in Odd-year (Accounting for 2001 special issue by doing just for RS bills)
  rs_bills <- filter(bills, grepl("RS", session)) %>% arrange(desc(session), bill_id) %>% distinct(bill_id, LES_sponsor, .keep_all = TRUE)
  bills <- bills %>% filter(grepl("SS", session)) %>% bind_rows(rs_bills) %>% arrange(session, bill_id)
  rm(rs_bills)
  
  ###################
  ###### Merge in S&S Bills
  ###################
  # *** For VIRGINIA: Bills carry over during regular AND DUPLICATE RECORDS HAVE BEEN DROPPED
  # --- WITH THE EXCEPTION OF THE 2001 Special Session... Special Session BIll Numbers are Unique Identifiers
  # ---> Going to merge on an Bill ID for most sessions and Adjusted Version for 2001
  SS_term <- SS_bills %>% 
    filter(term == t_yrs) %>% 
    mutate(special = ifelse(year == 2001 & ((num_only > 22 & bill_type == "HB") | (num_only > 10 & bill_type == 'SB')), 0, special),
           bill_id_adj = ifelse(year == 2001 & (special == 1 | special_num %in% 1), paste0(bill_id, '-SS'), bill_id),
           SS = 1) %>%
    distinct(term, bill_id_adj, SS)
    
  ### Merge
  if(nrow(SS_term) > 0){
    bills <- bills %>%
      mutate(bill_id_adj = ifelse(session == "2001-SS1", paste0(bill_id, "-SS"), bill_id)) %>%
      left_join(SS_term, by = c("term", "bill_id_adj")) %>%
      mutate(SS = ifelse(is.na(SS), 0, SS)) 
  }else{
    bills$SS <- 0
  }
  
  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id_adj", "term"))
  
  ######################################################
  ############### Code Commemorative
  ######################################################
  
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session'))
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  ######################################################
  ############### Code Bill History
  ######################################################
  
  bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_yrs}.csv")
  bill_hist <- read_csv(bill_hist_path, col_types = cols())
  bill_hist <- distinct(bill_hist)
  
  ### Clean Term/Session Variables + Eliminate Duplicates from Bill Coding
  bill_hist$term <- gsub('-', '_', bill_hist$term)
  
  bill_hist <- bill_hist %>% 
    mutate(session_type = substring(session, 6, nchar(session)),
           session_type = recode(session_type, 'SESSION' = 'RS', 'SPECIAL SESSION I' = 'SS1', 'SPECIAL SESSION II' = 'SS2',
                                 'SPECIAL SESSION III' = 'SS3', 'SPECIAL SESSION IV' = 'SS4'),
           session = paste0(session_year, '-', session_type)) %>%
    select(-session_type)
  
  ### Rearrange + create order variable that covers both chambers
  bill_hist <- arrange(bill_hist, session, bill_id, order) 
  
  #### Adusting Defeated By Records
  bill_hist <- bill_hist %>%
    mutate(action = tolower(action),
           action = ifelse(grepl("defeated by ", action) & !grepl("defeated by (house|senate)", action), 
                           gsub("defeated by ", "defeated in committee: ", tolower(action)), action))
  
  ### Standardize Chamber Variable
  # bill_hist$chamber <- recode(bill_hist$chamber, "A" = "House", "S" = "Senate", "G" = "Governor")
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  ### *** Tabled in + Passed by Indefinitely + Stricken + Continuation/Carry Over Requires a Vote
  ### Committees have authority to send letters to relevant agencys/study commissions (not required, but happens often)
  aic_t <- c("assigned to .+ sub-comm", "assigned.+ sub:", "reported from", "subcommittee recomm", 
             "subcommittee failed to", "tabled in", "failed to report", "^continued to \\d{4} in", 
             "^passed by .+ in", 'defeated in committee:', 'stricken from docket by',
             'letter sent to')
  abc_t <- c("^reported from", "read second time", "read third time", "engrossed", "vote:", "passed house", 
             "passed senate", "defeated by house", "defeated by senate")
  # Block Vote = 100-0
  pc_t <- c("passed house", "passed senate", "agreed to by house", "agreed to by senate",
            "vote: passage", "vote: block vote passage", "vote: adoption", "vote: block vote adoption",
            "signed by speaker", 'signed by president')
  law_t <- c("^approved by gov|^acts of assembly chapter text ")
  
  ### Check Actions
  # filter(bill_hist, grepl('gov', tolower(action))) %>% distinct(action) %>% View()
  # filter(bill_hist, grepl('effective date', tolower(action))) %>% group_by(action) %>% summarize(n = n()) %>% arrange(desc(n)) %>% View()
  
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
    hist_sub <- filter(bill_hist, bill_id == b_id, session == s_id)
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
    bill_stages$bill_url <- bills[i,]$bill_url
    ### Double CHeck Passage to Account for things like this: 
    if(bill_stages$passed_chamber == 1 & bill_stages$law == 0){
      if(!( any(grepl("communicated to (house|senate)", hist_sub$action)) | length(unique(hist_sub$chamber)) > 1)){
        bill_stages$passed_chamber <- 0
      }
    }
    all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
    # print(i)
  }
  options(warn = 1)
  
  ### Check Codings
  cat('\n')
  all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
    summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% as.data.frame() %>% print()
  # filter(bill_hist, session == '1987-RS' & bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 1 & all_bill_stages$session == '1987-RS',]$bill_id) %>% View()
  # filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 1,]$bill_id) %>% View()
  
  ### MERGE to BILL DATA
  bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 
  
  ### MERGE In COMMEMS
  all_bill_stages <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))
  
  ### MERGE In S&S
  if(nrow(SS_term) > 0){
    all_bill_stages <- mutate(all_bill_stages, bill_id_adj = ifelse(session == "2001-SS1", paste0(bill_id, '-SS'), bill_id))
    all_bill_stages <- SS_term %>% 
      select(bill_id_adj, term, SS) %>%
      left_join(all_bill_stages, ., by = c('bill_id_adj', 'term')) %>%
      mutate(SS = ifelse(is.na(SS), 0, SS)) %>%
      select(-bill_id_adj)
  }else{
    all_bill_stages$SS <- 0
  }

  
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
  rm(b_id, s_id, b_spon, bill_hist)
  
  #################################################
  ####### DROP SPONSORS WHO PREFILED BILLS FOR TERMS THEY WERE NOT IN OFFICE FOR
  #################################################
  
  if(t_yrs == '1996_1997'){
    bills <- filter(bills, !(LES_sponsor %in% c("elmo g cross", "hunter b andrews")))
  }else if(t_yrs == '2000_2001'){
    bills <- filter(bills, !(LES_sponsor %in% c("joseph v gartlan jr", "stanley c walker")))
  } else if(t_yrs == '2004_2005'){
    bills <- filter(bills, !(LES_sponsor %in% c("malford trumbo")))
  }
  
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
  all_sponsors$num_cosponsored_bills <- NA
  bills$cospon_match <- paste(bills$LES_sponsor, bills$senate_sponsors, bills$house_sponsors, sep = '; ')
  bills$cospon_match <- gsub('; na', '', bills$cospon_match)
  for(i in 1:nrow(all_sponsors)){
    c <- substring(all_sponsors[i,]$chamber,1,1)
    c_sub <- filter(bills, substring(bill_id, 1, 1) == c)
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
    ### ONLY need to adjust this way if sponsored and cosponsored column are the same
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  }; rm(c, c_sub)
  bills <- select(bills, -cospon_match)
  # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills)) 
  
  #######################
  #### CLEAN NAMES
  parsed_names <- map_df(all_sponsors$LES_sponsor, parse_names) %>% select(-salutation) %>% distinct() 
  parsed_names$last_name <- ifelse(parsed_names$middle_name %in% 'van', paste0('van ', parsed_names$last_name), parsed_names$last_name)
  parsed_names$last_name <- ifelse(parsed_names$middle_name %in% 'de', paste0('de ', parsed_names$last_name), parsed_names$last_name)
  parsed_names$middle_name <- ifelse(parsed_names$middle_name %in% c("van", "de"), '', parsed_names$middle_name)
  parsed_names$suffix <- ifelse(is.na(parsed_names$suffix), '', parsed_names$suffix )
  parsed_names$middle_name <- ifelse(is.na(parsed_names$middle_name), '', parsed_names$middle_name )
  
  all_sponsors <- left_join(all_sponsors, parsed_names, by = c("LES_sponsor" = "full_name"))
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  
  #### Update First Names, Last Names for Matching 
  if(t_yrs %in% c("1998_1999", '2000_2001', '2002_2003', '2004_2005', '2006_2007') ){
    all_sponsors[all_sponsors$LES_sponsor %in% c("jeannemarie a devolites", 'jeannemarie devolites', 'jeannemarie d davis'),]$last_name <-  "devolitesdavis"
  }
  if(t_yrs %in% c("2014_2015", '2016_2017')){
    all_sponsors[all_sponsors$LES_sponsor == 'daun s hester',]$last_name <-  "sessomshester"
  }
  
  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  H_elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  if(H_elec_year %% 4 == 3){ 
    S_elec_year <- H_elec_year
  }else{
    S_elec_year <- H_elec_year - 2
  }
  
  ### For Senate: Senate Election Year to House Year + 1 (so if 2015, 2015-2016; if 2013, 2013 to 2016)
  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% H_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% S_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz, middle) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)
  
  ######################################################
  ############## Match Sponsors Names to Klarner Data
  ######################################################
  
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- gsub('\\.', '', tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), ''))))
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
      k_matches <- filter(klarner_sub, last_name == gsub("-|'| ||`", '', tolower(all_sponsors[i,]$last_name)))
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
  if(t_yrs == "2006_2007"){
    all_sponsors[all_sponsors$LES_sponsor == "jackson h miller" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }
  
  #### CHeck for Duplicates from Matching
  duplicates <- filter(all_sponsors, !is.na(klarner_name)) %>% group_by(chamber, klarner_name) %>% filter(n() > 1)
   if(nrow(duplicates) > 0){  # any(duplicated(na.omit(all_sponsors$klarner_name)))
    cat('-----> Check DUPLICATE Sponsors \n ')
    print(select(duplicates, LES_sponsor, klarner_name, chamber) %>% as.data.frame())
  }; rm(duplicates)
  
  ##### CHECK IF ANY KLARNER CANDIDATES MISSING FROM DATA --- Includes all Senaters elected at T-1
  km <- filter(klarner_sub, !(candid %in% all_sponsors$klarner_id) )
  km <- filter(km, sen == 0 | (sen == 1 & year %in% S_elec_year:(as.numeric(substring(t_yrs, 1, 4))) ))
  
  ### Remove candidates who won but were never seated
  if(t_yrs == "1994_1995"){
    km <- filter(km, cand != 'scott, robert c.')
  }else if(t_yrs == '1998_1999'){
    km <- filter(km, cand != 'brickley, david g.')
    km <- filter(km, !(cand == 'forbes, j. randy' & sen == 0))
    km <- filter(km, cand != 'earley, mark l.')
    km <- filter(km, cand != 'goode, virgil h. jr.')
    km <- filter(km, cand != 'waddell, charles l.')
  }else if(t_yrs == '2002_2003'){
    km <- filter(km, !(cand == 'deeds, r. creigh' & sen == 0))
    km <- filter(km, cand != 'schrock, edward l.')
    km <- filter(km, cand != 'forbes, j. randy')
    km <- filter(km, cand != 'holland, richard j.')
    km <- filter(km, cand != 'couric, emily')
  }else if(t_yrs == '2006_2007'){
    km <- filter(km, cand != 'stump, jack')
    km <- filter(km, !(cand == 'mcdougle, ryan t.' & sen == 0))
    km <- filter(km, cand != 'bolling, william t. (bill)')
  }else if(t_yrs == '2008_2009'){
    km <- filter(km, cand != 'wittman, robert j.')
  }else if(t_yrs == '2010_2011'){
    km <- filter(km, cand != 'stolle, kenneth w. (ken)')
    km <- filter(km, cand != 'cuccinelli, ken t. ii')
  }else if(t_yrs == '2014_2015'){
    km <- filter(km, cand != 'ware, onzlee')
    km <- filter(km, cand != 'miller, yvonne b.')
    km <- filter(km, cand != 'northam, ralph s.')
    km <- filter(km, cand != 'blevins, harry b.')
    km <- filter(km, cand != 'herring, m. r.')
  }else if(t_yrs == '2016_2017'){
    km <- filter(km, cand != 'preston, joseph e.')
  }
  
  #### Add Candidates who WON but Sponsored NO BILLS into Data
  if(nrow(km) > 0){
    cat(glue("-----> {as.numeric(substring(t_yrs, 1, 4)) - 1} KLARNER CANDIDATES MISSING MATCHES IN DATA ~~> ADDING INTO LIST OF LEGISLATORS: \n . "))
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
    select(sponsor, data_name, klarner_name, klarner_id, chamber, term, elec_year, num_sponsored_bills, sponsor_pass_rate, sponsor_law_rate, num_cosponsored_bills) %>%
    arrange(chamber, sponsor)
  
  ####################################
  ##### Estimate Scores + Add in Related Variables
  #####################################
  
  ### Check if bills in data without an ID'd sponsor
  # filter(bills, !(bills$primary_sponsor %in% legis_data$data_name))
  bills <- bills %>% select(-sponsor) %>%
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
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES) # 
rm(t, terms, klarner_gs, m_sub, parsed_names, commem_bills)


########################################################################################################################################################
########################################################################################################################################################
######## NAME FIX RECORDS ---> If listed below, terms have been checked and fixed (unless noted otherwise)
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by SPECIAL ELECTION --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
## ASSEMBLY MEMBERS 1994 - 2003: https://web.archive.org/web/20030210223617/http://leg1.state.va.us/941/lis.htm
## Generally, data is easy to find on VA Legisators
###################################################
# *** SPECIAL ELECTION WINNERS added manually to Klarner (see top of script) between 2008 and 2015 ******
# ---> This is why no errors/edits pop out on script
###################################################

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1994_1995 TERM! ~~~~~~~~~~~~~~ 
# --> https://web.archive.org/web/20041229191250/http://leg1.state.va.us/941/mbr/MBR.HTM
### WON SPECIAL ~ HOUSE:
# -- NIXON
### WON SPECIAL ~ SENATE:
# -- MAXWELL (1993)
### IN SENATE:
# -- RUSSEL -- ~ a month -- resigned 1/26/1994 amid scandal -- https://www.dailypress.com/news/dp-xpm-19940126-1994-01-26-9401260030-story.html
### DROP:
# -- scott, robert c. -- resigned 1/3/1993 to take seat in US House

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1996_1997 TERM! ~~~~~~~~~~~~~~ 
# ---> http://leg1.state.va.us/961/mbr/MBR.HTM
### WON SPECIAL ~ HOUSE:
# -- LOVELACE
# -- RUST JR
### WON SPECIAL ~ SENATE:
# -- REYNOLDS (roscoe, January 1997)
### DROP
# -- elmo g cross -- LOST 1995 Election but somewhow still sponsored a single bill? -- See: https://historical.elections.virginia.gov/candidates/view/Elmo-G-Cross-Jr/
# -- hunter b andrews -- LOST 1995 election yet sponsored two bills
# --------> Dropping them AND the bills.... all bills prefiled in december....

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1998_1999 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- BLEVINS; MCQUIGG; WARE JR; BLACK
### WON SPECIAL ~ SENATE:
# -- FORBES; WATKINS; PUCKETT; REYNOLDS; MIMS --> All Via H, 
### DROP:
# -- brickley, david g. -- Appointed to Exec Position January 17 1998 -- https://www.ourcampaigns.com/CandidateDetail.html?CandidateID=21151
# -- forbes, j. randy IN HOUSE -- Won senate special, took seat Jan. 6, 1998
# -- earley, mark l. -- Appointed VA Attorney general Jan 17, 1998
# -- goode, virgil h. jr. -- resigned Jan 1997 after winning US House seat
# -- waddell, charles l. -- Appointed VA Sec of Transpo -- https://ead.lib.virginia.edu/vivaxtf/view?docId=tbl/viletbl00258.xml

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2000_2001 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- WELCH III; O'BANNON III; RAPP; WRIGHT
### WON SPECIAL ~ SENATE:
# -- RUFF; WAGNER ---> Both Via H
### IN HOUSE: 
# -- WILKINS JR
### DROP + PREFILED BILLS:
# -- joseph v gartlan jr -- retired at end of 1998-1999 term... https://www.washingtonpost.com/wp-srv/local/longterm/valeg/gartlan021999.htm
# -- stanley c walker -- retired at end of 1998-1999 term... https://en.wikipedia.org/wiki/Stanley_C._Walker
# -----> Like 96/97, BOTH LOST BUT PREFILED BILLS FOR NEXT SESSION

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2002_2003 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- CLINE; SHULER; ALEXANDER; HUGO   
# ---------> Shuler represented the 12th, then ran in 7th in 2001, lost, then won 2002 special for the 12th...
### WON SPECIAL ~ SENATE:
# -- RUFF; WAGNER --> Both via H, prior session
# -- BLEVINS (via H); CUCCINELLI II; DEEDS (via H)
### IN HOUSE:
# -- WILKINS
### DROP:
# -- deeds, r. creigh IN HOUSE -- took senate seat in december 2001
# -- schrock, edward l. -- won US House seat in January 2001
# -- forbes, j. randy -- won US House seat in June 2001
# -- holland, richard j. -- died April 16, 2000
# -- couric, emily -- died October 18, 2001

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2004_2005 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- MILLER (paula)
### IN HOUSE:
# -- HOWELL (william)
#### DROP:
# -- malford trumbo -- Left office at end of 2002-2003 term but prefiled a bill for subsequent term

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2006_2007 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- PEACE; BOWLING; VALENTINE
# -- MILLER, jackson --> Name Duplicated --> Won't print
### WON SEPCIAL ~ SENATE:
# -- HERRING; MCDOUGLE (via H)
### DROP
# -- stump, jack -- resigned to take state-level position -- https://www.bdtonline.com/news/longtime-public-servant-jackie-stump-remembered/article_2599f818-2938-11e6-9c4e-3bdecf991e95.html
# -- mcdougle, ryan t. IN HOUSE -- resigned to take senate seat -- never seated in House
# -- bolling, william t. (bill) -- resigned to become LT Governor, never seated in Senate


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2008_2009 TERM! ~~~~~~~~~~~~~~ 
### DROP: 
# -- wittman, robert j. -- Won US House seat in December 2007

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2010_2011 TERM! ~~~~~~~~~~~~~~ 
### DROP: 
# -- stolle, kenneth w. (ken) -- resigned to become sheriff
# -- cuccinelli, ken t. ii -- resigned to become VA attorney general

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2012_2013 TERM! ~~~~~~~~~~~~~~ 
# ******** NO ISSUES! ********

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2014_2015 TERM! ~~~~~~~~~~~~~~ 
### DROP:
# -- ware, onzlee -- resigned Nov 2013 after election win citing family issues -- https://en.wikipedia.org/wiki/Onzlee_Ware
# -- miller, yvonne b. -- died July 2012
# -- northam, ralph s. -- resigned to become LT Gov, Jan 11 2014
# -- blevins, harry b. -- resigned Aug. 2013
# -- herring, m. r. -- resigned to become VA Attorney General, Jan 11, 2014


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2016_2017 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- HAYES JR; MULLIN; HOLCOMB III
### WON SPECIAL ~ SENATE:
# -- PEAKE
### DROP:
# -- preston, joseph e. -- Wins special in 2015 for immediate seating, loses senate general in 2015

# filter(klarner, grepl('cuccin', cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid)
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
# name = still_missing[7]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl('mullin', cand)) %>% select(cand, year, etype, sen, outcome, candid) # %>%  View()
# rm(name, missing, name_sub, still_missing)

### Fix Missing
name_matches <- data.frame(LES_name = 'wright, tommy', k_name = 'wright, t. c. jr.')
name_matches <- add_row(name_matches, LES_name = "o'bannon, john", k_name = 'obannon, john m. iii')
name_matches <- add_row(name_matches, LES_name = 'hugo, timothy', k_name = 'hugo, t. d.')
name_matches <- add_row(name_matches, LES_name = 'herring, mark', k_name = 'herring, m. r.')
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
rm(check_dup, k_sub, exact, name_sub, missing, t, name)


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

### Manually Fix Those Not in (My Subset of) Klarner
# LES[LES$sponsor == "krupicka, k. rob",]$klarner_id <- NA
LES[LES$sponsor == "krupicka, k. rob",]$party <- 'd'
LES[LES$sponsor == "krupicka, k. rob",]$district <- 45
LES[LES$sponsor == "krupicka, k. rob",]$exper <- 'none'
LES[LES$sponsor == "krupicka, k. rob",]$sponsor <- 'krupicka, kenneth robert'

#### *** If any run for reelection, won't be needed once klarner updates ****
LES[LES$sponsor == "mullin, michael",]$party <- 'd'
LES[LES$sponsor == "holcomb, nd",]$party <- 'r'
LES[LES$sponsor == "holcomb, nd",]$sponsor <- "holcomb, norman dewey iii" # Nickname = Rocky
LES[LES$sponsor == "hayes, ce",]$party <- 'd'
LES[LES$sponsor == "hayes, ce",]$sponsor <- 'hayes, cliff jr.'
LES[LES$sponsor == "peake, mark",]$party <- 'r'

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **

### REMOVE NICKNAMES
LES$sponsor <- str_trim(gsub('  +', ' ', gsub('\\(.+\\)', '', LES$sponsor)))
# table(LES$sponsor)

### Manual Fixes -- Collapsing to 1 OR Adding First Name
LES[LES$sponsor %in% c('sullivan, richard c. rip jr.', 'sullivan, r. c. jr.'),]$sponsor <- 'sullivan, richard c jr.'
LES[LES$sponsor == 'dance, r. r.',]$sponsor <- 'dance, rosalyn r.'
LES[LES$sponsor == 'fowler, h. f. jr.',]$sponsor <- 'fowler, hyland f. jr.'
LES[LES$sponsor == 'fralin, w. h. jr.',]$sponsor <- 'fralin, william h. jr.'
LES[LES$sponsor == 'herring, m. r.',]$sponsor <- 'herring, mark r.'
LES[LES$sponsor == 'hugo, t. d.',]$sponsor <- 'hugo, timothy d.'
LES[LES$sponsor == 'hurt, robert 1',]$sponsor <- 'hurt, robert'
LES[LES$sponsor == 'lewis, l. w. jr.',]$sponsor <- 'lewis, lynwood w. jr.'
LES[LES$sponsor == 'mathieson, r. w.',]$sponsor <- 'mathieson, robert w.'
LES[LES$sponsor == 'potts, h. r. jr.',]$sponsor <- 'potts, h. russell jr.'
LES[LES$sponsor == 'rasoul, s.',]$sponsor <- 'rasoul, sam'
LES[LES$sponsor == 'rerras, d. n.',]$sponsor <- 'rerras, d. nick'
LES[LES$sponsor == 'wright, t. c. jr.',]$sponsor <- 'wright, thomas c. jr.'

### Robert S. Bloxom -- SPlitting SR and JR ---> Wrong in Klarner so ID's will still be the same...
LES[LES$data_name %in% 'robert s bloxom',]$sponsor <- 'bloxom, robert s. sr.'
LES[LES$data_name %in% 'robert s bloxom jr',]$sponsor <- 'bloxom, robert s. jr.'

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
#hf_data$match_name <- tolower(hf_data$Klarner_name)
#hf_data$sen <- ifelse(hf_data$chamber == 'senate', 1, 0)
hf_data$chamber <- ifelse(hf_data$chamber == 'senate', 'Senate', "House")

#### Doubling the Senate Rows + Adding back in
senate <- filter(hf_data, chamber == "Senate" & year %% 4 == 3)
senate$year <- senate$year + 2
senate$term <- paste0(senate$year + 1, "_", senate$year + 2)
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
LES[LES$term == '2016_2017', set_NA] <- NA
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

### SM Names Matched to Multiple LES Sponsors
# ---> so this will catch all errors except those where Sponsor A is Matched to Voter Score B, when Sponsor B and Voter A are missing
filter(LES, !is.na(SM_name)) %>%
  group_by(SM_name) %>% 
  summarize(name_matches = paste(unique(sponsor), collapse = "----")) %>% 
  filter(grepl("----", name_matches))

#### FIX MISMATCHES
LES[LES$sponsor %in% c('garrett, tom a. jr.'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES ------- VA SM Data starts in 1996
# filter(LES, is.na(np_score)) %>% filter( !(term %in% c('1994_1995')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('bloxom', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

name_matches <- data.frame(LES_name = 'bloxom, robert s. sr.', SM_name = 'Bloxom, Robert S.') ### Sr/Jr have same klarner id..
name_matches <- data.frame(LES_name = 'bloxom, robert s. jr.', SM_name = 'Bloxom Jr, Robert S') 
name_matches <- add_row(name_matches, LES_name = 'broman, george e. jr.', SM_name = 'Broman Jr., George')
name_matches <- add_row(name_matches, LES_name = 'colgan, charles j.', SM_name = 'Colgan Sr, Charles J')
name_matches <- add_row(name_matches, LES_name = 'davis, glenn r. jr.', SM_name = 'Davis Jr, Glenn R')
name_matches <- add_row(name_matches, LES_name = 'edmunds, james e. ii', SM_name = 'Edmunds II, James E')
name_matches <- add_row(name_matches, LES_name = 'fowler, hyland f. jr.', SM_name = 'Fowler Jr, Hyland F')
name_matches <- add_row(name_matches, LES_name = 'garrett, tom a. jr.', SM_name = 'Garrett Jr, Thomas A')
name_matches <- add_row(name_matches, LES_name = 'harris, robert e.', SM_name = 'Harris')
# name_matches <- add_row(name_matches, LES_name = 'hayes, cliff jr.', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'helsel, gordon c. jr.', SM_name = 'Helsel Jr, Gordon C')
# name_matches <- add_row(name_matches, LES_name = 'holcomb, norman dewey iii', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'howell, algie t. jr.', SM_name = 'Howell Jr, Algie T')
name_matches <- add_row(name_matches, LES_name = 'krupicka, kenneth robert', SM_name = 'Krupicka, K.') ### Two Rows
name_matches <- add_row(name_matches, LES_name = 'lewis, lynwood w. jr.', SM_name = 'Lewis Jr, Lynwood W')
name_matches <- add_row(name_matches, LES_name = 'marsh, henry l. iii', SM_name = 'Marsh III, Henry L')
name_matches <- add_row(name_matches, LES_name = 'marshall, daniel w. iii', SM_name = 'Marshall III, Daniel W')
name_matches <- add_row(name_matches, LES_name = 'massie, jimmie p. iii', SM_name = 'Massie III, James P')
name_matches <- add_row(name_matches, LES_name = 'mcquigg, michele b.', SM_name = 'McQuigg, Michéle')
# name_matches <- add_row(name_matches, LES_name = 'mullin, michael', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'murphy, w. tayloe jr.', SM_name = 'Murphy')
name_matches <- add_row(name_matches, LES_name = 'norment, thomas k. jr.', SM_name = 'Norment Jr, Thomas K')
name_matches <- add_row(name_matches, LES_name = 'obannon, john m. iii', SM_name = "O'Bannon III, John M")
# name_matches <- add_row(name_matches, LES_name = 'peake, mark', SM_name = 'zzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'pollard, albert c. jr.', SM_name = 'Pollard Jr, Albert C')
name_matches <- add_row(name_matches, LES_name = 'ruff, frank m.', SM_name = 'Ruff Jr, Frank M')
name_matches <- add_row(name_matches, LES_name = 'stanley, william m. jr.', SM_name = 'Stanley Jr, William M')
name_matches <- add_row(name_matches, LES_name = 'sullivan, richard c jr.', SM_name = 'Sullivan Jr, Richard C')
name_matches <- add_row(name_matches, LES_name = 'waddell, charles l.', SM_name = 'Waddell')
name_matches <- add_row(name_matches, LES_name = 'wexton, jennifer t.', SM_name = 'Wexton, Jennifer T.')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

#### Manual Edits (Needs more precision...)

### Split Across Two Rows
LES[LES$sponsor == 'hanger, emmett w. jr.',]$SM_name <- ideo[ideo$name == 'Hanger Jr, Emmett W' & ideo$senate1996 %in% 1,]$name
LES[LES$sponsor == 'hanger, emmett w. jr.',]$SM_party <- ideo[ideo$name == 'Hanger Jr, Emmett W' & ideo$senate1996 %in% 1,]$party
LES[LES$sponsor == 'hanger, emmett w. jr.',]$np_score <- ideo[ideo$name == 'Hanger Jr, Emmett W' & ideo$senate1996 %in% 1,]$np_score

LES[LES$sponsor == 'rasoul, sam',]$SM_name <- ideo[ideo$name ==  'Rasoul, Sam' & ideo$house2015 %in% 1,]$name
LES[LES$sponsor == 'rasoul, sam',]$SM_party <- ideo[ideo$name == 'Rasoul, Sam' & ideo$house2015 %in% 1,]$party
LES[LES$sponsor == 'rasoul, sam',]$np_score <- ideo[ideo$name == 'Rasoul, Sam' & ideo$house2015 %in% 1,]$np_score

########## 
### Party Switcheers
###############
### Check Party Mismatches
# filter(LES, party == 'd' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% arrange(sponsor, term) %>% as.data.frame()
# filter(LES, party == 'r' & tolower(SM_party) != party) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% arrange(sponsor, term) %>% as.data.frame()

#### Richard Fisher, District 35 --> See no evidence that he was ever a democrat, but districts line up
# ------> SM party == wrong

# LES[LES$sponsor == 'hayes, robert wes jr.' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'Hayes, R.W.' & ideo$party == 'D',]$name
# LES[LES$sponsor == 'hayes, robert wes jr.' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Hayes, R.W.' & ideo$party == 'D',]$party
# LES[LES$sponsor == 'hayes, robert wes jr.' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Hayes, R.W.' & ideo$party == 'D',]$np_score
# LES[LES$sponsor == 'hayes, robert wes jr.' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Hayes, Robert Wesley' & ideo$party == 'R',]$name
# LES[LES$sponsor == 'hayes, robert wes jr.' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Hayes, Robert Wesley' & ideo$party == 'R',]$party
# LES[LES$sponsor == 'hayes, robert wes jr.' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Hayes, Robert Wesley' & ideo$party == 'R',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# ----> Jr/Sr collapsed in Klarner

##### Drop Nicknames
# filter(LES, grepl('\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
# LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)
LES[LES$sponsor == 'jones, jerrauld c. 1',]$sponsor <- 'jones, jerrauld corey'
LES[LES$sponsor == 'couric, emily 1',]$sponsor <- 'couric, emily'

#### Manual Fixes
LES[LES$sponsor == 'sessomshester, daun',]$sponsor <- 'hester, daun sessoms'


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1994 - 2020
# ** 1998-1999 - SPLIT CONTROL (50 D, 49 R, 1 Ind-R) --> Power-sharing Agreement --> ALL CODED 0 ---> See NCSL
LES[as.numeric(substring(LES$term,1,4)) %in% c(1994:1997) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2000:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1
LES[LES$chamber == "House" & LES$term == '1998_1999',]$in_majority <- 0

### Senate -- 1994 - 2020
# ** 1996_1997 - SPLIT CONTROL --> Power Sharing Agreement --> All Coded 0 --> See NCSL
# ** 2014_2015 - CONTROL SPLIT MIDWAY (6 months, 1.5 months( --> SWITCH IN POWER --> All Coded 1 ---> See Pres Pro tems: https://en.wikipedia.org/wiki/President_pro_tempore_of_the_Senate_of_Virginia
LES[as.numeric(substring(LES$term,1,4)) %in% c(1994:1995, 2008:2011) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1998:2007, 2012:2013, 2016:2019) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1
LES[LES$chamber == "Senate" & LES$term == '1996_1997',]$in_majority <- 0
LES[LES$chamber == "Senate" & LES$term == '2014_2015',]$in_majority <- 1

### Split Control 
# "In the Virginia Senate (1995), a Democratic lieutenant governor presided, the Finance Committee got co-chairs; six committees had Democratic 
# chairs, and four had Republican chairs. The Virginia House of Delegates elected a Democratic speaker and then adopted a power-sharing agreement."
# ---> Goes on to explain how the House plan work (more sharing, all co-chairs)
# ---> http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx

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
  scale_color_manual(values=c("dodgerblue2",  'gray50', "red2", 'gray50'))

######## Check Outliers
# filter(LES, party == 'd' & np_score > .25) %>% select(sponsor, term, chamber, np_score, party, SM_party)
# filter(LES, party == 'r' & np_score < -.25) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')



