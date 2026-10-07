
################################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** ALASKA *** BY SESSION
###############################################################

#####################
# SPECIAL SESSIONS
# --- Essentially just extend the main session, so bills folded into general group; bill numbers continue to ascend
# PROCESS:
# --- http://w3.legis.state.ak.us/docs/pdf/legprocess.pdf (includes first two quotes below)
# BILL SPONSORSHIP
# --- "All bills must be introduced by a legislator, a legislative committee, or the Governor through the Rules Committee."
# --- "Once a bill has been prepared by Legal Services, the prime sponsor (either an individual legislator or a committee chair) receives..."
# --- Only committee-sponsored bills can be introduced after the 35th day of the second regular session. --> http://w3.legis.state.ak.us/docs/pdf/uniform_rules.pdf
# --- (**Q**) Code committee bills as committee chair?
# MEMBERS
# --- Could pretty easily pull from the member list... http://www.akleg.gov/basis/mbr_info.asp?session=19
####################
#### **** NOTES:
# -- COMMITTEE BILLS DROPPED -- Could Gather Info to Attribute to Chair (or other individual?)
# -- Bills Introduced BY REQUEST OF GOVERNOR == Dropped (Always Introduced via Rules Comm)
# -- Keeping Member Bills Introduced by Request (Per Convo with Alan)
# -- For 2017-2018: Coded Republicans Caucusing with Dems as in_majority == 1
#########################

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
library(tibble)

this_state <- 'AK'
min_year <- 1993
keep_types <- c("HB", "SB")
spec_elec_codes <- c('s')
sen_term_length <- 4 #### -- TERMS ARE STAGGERED!
toCls <- function(x, cls) do.call(paste("as", cls, sep = "."), list(x))

#### Output Directory
#dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Data Directory
data_files <- list.files(glue("~/Dropbox/Data/State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]

terms <- gsub('.+Details_|.csv', '', bill_files)
rm(data_files, bill_files)

# *************************
# Dropping 2019+ for now
terms <- terms[-which(terms %in% c("31st_Legislature_2019_2020", "32nd_Legislature_2021_2022"))]
# ************************

#### COMMEMORATIVE BILLS
commem_bills <- read.csv(glue("~/Dropbox/Data/State Legislative Data/Commem Bills/{this_state}_Commem_Bills.csv"))

#### SUBSTANTIVE AND SIGNIFICANT BILLS
SS_bills <- read.csv("~/Dropbox/Data/State Legislative Data/Significant Bills/SS_Data/SS_Bills.csv")
SS_bills <- SS_bills %>% 
  filter(state == this_state) %>%
  rename(bill_id = bill_num) %>%
  mutate(term = ifelse(year %% 2 == 1, paste0(year, "_", year + 1), paste0(year - 1, "_", year)),
         bill_id = toupper(bill_id),
         bill_id = ifelse(bill_type == "S" & !grepl("^SB", bill_id), gsub("^S", "SB", bill_id), bill_id),
         SS = 1) %>%
  select(state, term, year, bill_id, everything())

#### Check SS BIll Types ---> Correct to ensure standardized with Data Format
# table(SS_bills$bill_type)

### Load Klarner Data 
load("~/Dropbox/Data/US Election Data/state_leg/klarner_data/196slers1967to2016_20180908.RData")
klarner <- table; rm(table)
klarner$sab <- toupper(klarner$sab)
klarner <- filter(klarner, year > min_year - 5 & sab == this_state)

### Fixing Error with Gary Davis
klarner[klarner$cand == 'avis, gary',]$cand <- "davis, gary"
### *** Could Change ID as well... but tha tmight hinder merging to outside sources...

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t = terms[1]

for(t in terms){
  
  ### Add 19/20 onto years
  t_yrs <- gsub('.+ture_', '', t)
  
  ### Skip Previously Estimated
  if(glue("{this_state}_LES_{t_yrs}.csv") %in% list.files('.')){
    print(glue('. \n \n ~~~~ SKIPPING {t_yrs} session ---> LES scores already estimated! \n \n.'))
    next
  }
  
  ###### SESSION IN PROGRESS
  print(glue('. \n ~~~~ ESTIMATING SCORES FOR THE {t_yrs} session! \n .'))
  
  ############### Read in data
  bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{t}.csv")
  bills <- read.csv(bill_path)

  ### Check for duplicates
  if(nrow(bills) != nrow(distinct(bills))){
    cat(" \n ~~~> DUPLICATE BILLS \n\n .")
    break
  }
  
  ######## Standardize the Bill IDs
  bills$bill_id <- paste0(gsub(' .+', '', bills$bill_id), str_pad(gsub('.+ ', '', bills$bill_id), 4, pad = "0"))
  bills$term <- t_yrs
  bills$session <- gsub(' .+', '', bills$session)
  
  ############### Drop Resolutions
  bills <- mutate(bills, bill_type = gsub('[0-9]+', '', bill_id))
  # table(bills$bill_type)
  bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)
  
  ############### Standardize Sponsors
  ### If intro_sponsor is blank, use first primary sponsor (order is nonalphabetical, set in json as 1,2...)
  
  bills$primary_sponsor <- gsub('á', 'a', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('é', 'e', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('ó', 'o', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('í', 'i', bills$primary_sponsor)
  bills$primary_sponsor <- gsub('ñ', 'n', bills$primary_sponsor)
  bills$primary_sponsor <- tolower(bills$primary_sponsor)
  
  bills$cosponsors <- gsub('á', 'a', bills$cosponsors)
  bills$cosponsors <- gsub('é', 'e', bills$cosponsors)
  bills$cosponsors <- gsub('ó', 'o', bills$cosponsors)
  bills$cosponsors <- gsub('í', 'i', bills$cosponsors)
  bills$cosponsors <- gsub('ñ', 'n', bills$cosponsors)
  
  #### Creating A Chamber Cosponsor Variable
  bills$chamber_cosponsors <- ifelse(substring(bills$bill_id, 1, 1) == 'H', gsub('SENATOR.+', '', bills$cosponsors), gsub('REPRES.+', '', bills$cosponsors))
  bills$chamber_cosponsors <- str_trim(tolower(bills$chamber_cosponsors))
  
  ### 1999-2000
  if(t_yrs == '1999_2000'){
    #bills$LES_sponsor <- ifelse(grepl("^KELLY PETE", bills$primary_sponsor), "p.kelly", bills$LES_sponsor)
    #bills$LES_sponsor <- ifelse(grepl("^KELLY TIM", bills$primary_sponsor), "t.kelly", bills$LES_sponsor)
    bills$primary_sponsor <- gsub('kelly pete', 'p.kelly', bills$primary_sponsor)
    bills$primary_sponsor <- gsub('kelly tim', 't.kelly', bills$primary_sponsor)
    bills$chamber_cosponsors <- gsub('kelly pete', 'p.kelly', bills$chamber_cosponsors)
    bills$chamber_cosponsors <- gsub('kelly tim', 't.kelly', bills$chamber_cosponsors)
  }
  
  if(t_yrs %in% c('2003_2004', "2005_2006")){
    ### THERE ARE TWO STEVENS 
    # G starts in House, resigns moves to Senate (is listed only as STEVENS in H)
    # B is in Senate --- For both, only need to adjust Senate --- House should match right.. 
    # bills$LES_sponsor <- ifelse(grepl("^STEVENS G", bills$primary_sponsor), "g.stevens", bills$LES_sponsor)
    # bills$LES_sponsor <- ifelse(grepl("^STEVENS B", bills$primary_sponsor), "b.stevens", bills$LES_sponsor)  
    bills$primary_sponsor <- gsub('stevens g\\b', 'g.stevens', bills$primary_sponsor)
    bills$primary_sponsor <- gsub('stevens b\\b', 'b.stevens', bills$primary_sponsor)
    bills$chamber_cosponsors <- gsub('stevens g\\b', 'g.stevens', bills$chamber_cosponsors)
    bills$chamber_cosponsors <- gsub('stevens b\\b', 'b.stevens', bills$chamber_cosponsors)
  }
  
  ### Elected as Mary Kapsner, Changed name to Mary Nelson --- Only in office one term so this will match to klarner entry
  ### [Putting Klarner match first]
  if(t_yrs == '2007_2008'){
    #bills[bills$LES_sponsor == 'nelson',]$LES_sponsor <- "kapsner-nelson"
    bills$primary_sponsor <- gsub('nelson', 'kapsner-nelson', bills$primary_sponsor)
    bills$chamber_cosponsors <- gsub('nelson', 'kapsner-nelson', bills$chamber_cosponsors)
  }
  
  if(t_yrs %in% c('2015_2016', '2017_2018') ){
    #bills[bills$LES_sponsor == 'mackinnon',]$LES_sponsor <- "fairclough-mackinnon"
    bills$primary_sponsor <- gsub('mackinnon', 'fairclough-mackinnon', bills$primary_sponsor)
    bills$chamber_cosponsors <- gsub('mackinnon', 'fairclough-mackinnon', bills$chamber_cosponsors)
  }
  
  if(t_yrs %in% c('2017_2018', '2019_2020') ){
    #bills$LES_sponsor <- ifelse(grepl("^VON IMHOF", bills$primary_sponsor), "vonimhof", bills$LES_sponsor)
    bills$primary_sponsor <- gsub('von imhof', 'vonimhof', bills$primary_sponsor)
    bills$chamber_cosponsors <- gsub('von imhof', 'vonimhof', bills$chamber_cosponsors)
  }
  
  #### LES VAR
  bills$LES_sponsor <- tolower(bills$primary_sponsor)
  
  ### ATTRIBUTING BY REQUEST BILLS TO WHOEVER INTRODUCED IT
  bills$LES_sponsor <- ifelse(!grepl('governor', bills$LES_sponsor), gsub(' by reque.+', '', bills$LES_sponsor), gsub(' by reque.+', '-br-gov', bills$LES_sponsor))
  bills$LES_sponsor <- ifelse(grepl('^house', bills$LES_sponsor), gsub(' ', '', gsub('^house ', "committee-house-", bills$LES_sponsor)), bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(grepl('^senate', bills$LES_sponsor),gsub(' ', '', gsub('^senate ', "committee-senate-", bills$LES_sponsor)), bills$LES_sponsor)
  
  ####  *********** DROPPING COMM BILLS ***************
  ### See the doc/quotes above/below
  if(any(grepl('committee-', bills$LES_sponsor))){
    print(glue("-----> Dropping {nrow(filter(bills, grepl('committee-', LES_sponsor)))} bill(s) sponsored by COMMITTEE"))
    bills <- filter(bills, !(grepl("committee-", LES_sponsor)))     
  }
  
  ### SPLIT MUTLIPLE SPONSORS --- e.g., kott halford is KOTT AND HALFORD --- Kelly B.Davis likely the same.. 
  bills$LES_sponsor <- gsub(' [a-z].+', '', bills$LES_sponsor)
  bills$LES_sponsor <- str_trim(bills$LES_sponsor)
  # sort(table(bills$LES_sponsor))
  
  ###### CHeck Missing Sponsors
  # filter(bills, primary_sponsor == "") %>% View()
  if(any(bills$LES_sponsor == "")){
    print(glue(" ~~> Dropping {nrow(filter(bills, LES_sponsor == ''))} bills without a sponsor"))
    bills <- filter(bills, LES_sponsor != "") 
  }

  #####################
  #### Merge in S&S Bills
  ###################
  # *** For ALASKA: Bills Carryover, Numbers = Unique IDs, can merge directly on BILL_NUM and ignore year/session/specials
  SS_term <- SS_bills %>%
    filter(term == t_yrs) %>%
    distinct(term, year, bill_id, SS) #year, special
  
  bills <- bills %>%
    left_join(select(SS_term, -c(term, year)), by = c("bill_id")) %>%
    mutate(SS = ifelse(is.na(SS), 0, SS))

  ### Check Missing ---> Seem to be by committee and/or req. by governor
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_num" = "bill_id"))
  
  ############### Code Commemorative
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session')) %>%
    mutate(commem = coalesce(commem, 0))
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  #######################################
  ######## Code Bill History
  ########################################
  bill_hist_path <- gsub('_Bill_Details', '_Bill_Histories', bill_path) 
  bill_hist <- read.csv(bill_hist_path)
  
  ######## Standardize BillHist Bill IDs
  bill_hist$bill_id <- paste0(gsub(' .+', '', bill_hist$bill_id), str_pad(gsub('.+ ', '', bill_hist$bill_id), 4, pad = "0"))

  ##### Set Session Var
  bill_hist$term <- t_yrs
  bill_hist$session <- gsub(' .+', '', bill_hist$session)
  
  ### CLEAN for Match
  bill_hist$action <- str_trim(gsub('\\(H\\) |\\(S\\) ', '', bill_hist$action))
  bill_hist$action <- ifelse(bill_hist$action_location == "committeeAction", paste0('COMM: ', bill_hist$action), bill_hist$action)
  bill_hist$chamber <- ifelse(bill_hist$chamber == "H", "House", "Senate")
  
  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  # **** NOTE: ACTION LOCATION COLUMN MOSTLY RECORDS HEARINGS -- There are records of committee action outside of these
  # **** BUT: I've prefaced all committeeAction rows with 'comm:' to use in matching
  ## --> comm:.+ at == committee meetings/hearings where it is scheduled for the agenda \\\\ rpt = comm report \\\ dp/dnp is rec do pass or do not pass (could also ad am: (amend) or nr: (no rec))
  aic_t <- c("heard and held|heard \\& held|public hearing|comm:.+at.+ [am|pm]|^[^\\s]+ rpt |^dp:|^dnp:|^nr:|^am:")
  ### Post-Validation: Adding rpt here (reported = goes to next comm or rules, even if negative) 
  ### In early years seems to only use referred to for second referral, but not constant over time
  abc_t <- c("^[^\\s]+ rpt ",
             "rules to calendar", "move.+out of comm", "read the second time", "read the third time", 
             "advanced to third", "am no [0-9]+", "^passed", "^failed")
  ## --> am no [0-9]+ == amendments
  pc_t <- c("^passed|transm[a-z]+ to \\(s\\)|transm[a-z]+ to \\(h\\)|transm[a-z]+ to gov")
  law_t <- c("signed into law|effective (date of|dateof|date\\(s\\) of|date\\(s\\)of) law|chapter [0-9]+ sla")
  ### Effective date + effective date(s) + missing space in dateof
  
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
    #t_id = bills[i,]$term
    s_id = bills[i,]$session
    b_spon = bills[i,]$LES_sponsor
    hist_sub <- filter(bill_hist, bill_id == b_id, term == t_yrs, session == s_id)
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t)
    bill_stages$bill_url <- paste0('http://www.akleg.gov/basis/Bill/Detail/', gsub('[a-z]+', '', s_id), '?Root=', b_id)
    
    #AV: fixed this
    #all_bill_stages <- bind_rows(replace(all_bill_stages, Map(toCls, all_bill_stages, sapply(bill_stages,class))), bill_stages)
    all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
    # print(i)
  }
  options(warn = 1)
  
  ### Check Codings
  cat('\n')
  all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(term, chamber) %>% 
    summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% as.data.frame() %>% print()
  # filter(bill_hist, bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0,]$bill_id) %>% filter(!grepl('^by resolution|^first read|^prefiled', tolower(action))) %>% View()

  #### Quick Validation using Bill Status
  # sum(ifelse(grepl("CHAPTER", bills$status) & bills$law == 0, 1, 0))
  
  ### MERGE to BILL DATA
  bills <- left_join(bills, select(all_bill_stages, -LES_sponsor), by = c("bill_id", "term", "session")) 
  
  ### MERGE In COMMEMS
  all_bill_stages <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))
  
  ### MERGE In S&S
  all_bill_stages <- SS_term %>%
    select(bill_id, term, SS) %>% # session/year
    left_join(all_bill_stages, ., by = c('bill_id', 'term')) %>% # session/year
    mutate(SS = ifelse(is.na(SS), 0, SS))

  ### Adjust Commems if SS == 1
  all_bill_stages$commem <- ifelse(all_bill_stages$SS == 1 & all_bill_stages$commem == 1, 0, all_bill_stages$commem)
  
  ### Save Stage Info **** MERGE WITH SS **********
  if(!dir.exists(glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}'))){
    dir.create(glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}'))
  }
  write.csv(all_bill_stages, glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv'), row.names = FALSE )
  
  rm(bill_hist_path, hist_sub, bill_stages, i, all_bill_stages, evaluate_bill_hist, aic_t, abc_t, pc_t, law_t)
  rm(b_id, s_id, bill_hist, b_spon, SS_term)
  
  ####################################################
  ############### Identify Unique Legislators via SLER
  ####################################################  
  
  ## Import and Clean Sponsors Name to Match
  all_sponsors <- bills %>%
    mutate(chamber = ifelse(substring(bill_id, 1, 1) == 'H', "H", "S")) %>%
    select(LES_sponsor, chamber, passed_chamber, law) %>%
    mutate(term = t_yrs) %>%
    group_by(LES_sponsor, term, chamber) %>%
    summarize(num_sponsored_bills = n(), 
              sponsor_pass_rate = sum(passed_chamber) / n(),
              sponsor_law_rate = sum(law) / n()) %>%
    ungroup()

  #### Adding Legislators Who COSPONSORED A BILL but did not SPONSOR
  unique_cospon <- str_trim(unique(unlist(str_split(bills$chamber_cosponsors, ', '))))
  for(nonspon in unique_cospon){
    if(!(nonspon %in% all_sponsors$LES_sponsor) & nonspon != ''){
      chamb <- unique(substring(bills[grepl(nonspon, bills$chamber_cosponsors),]$bill_id, 1, 1))
      if( (nonspon == 'hoffman' & t_yrs == '1995_1996') ){ # 1 row miscoded as House --> Hoffman is in Senate
        all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = "S", term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0)
      }else if(t_yrs == '2007_2008' & nonspon %in% c("foster")){ # No record of any foster except Richard in House
        next
      }else if(t_yrs == '2009_2010' & nonspon %in% c("r.foster")){ # Not clear why foster showing up on senate bills (both fosters in House)
        next
      }else if(t_yrs == '2013_2014' & nonspon %in% c("johnson", 'kito iii')){
        all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = "H", term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0)
      }else if("H" %in% chamb & "S" %in% chamb){
        print(glue('CHECK COSPONSOR ONLY :: {nonspon} :: BOTH CHAMBERS'))
      }else{
        all_sponsors <- add_row(all_sponsors, LES_sponsor = nonspon, chamber = chamb, term = t_yrs, num_sponsored_bills = 0, sponsor_pass_rate = 0, sponsor_law_rate = 0)
      }
      rm(chamb, nonspon)
    }
  }
  rm(unique_cospon)
  
  ######## Cosponsorship Info --- For OH: Only have cosponsor info for most recent years
  all_sponsors$num_cosponsored_bills <- NA
  bills$cospon_match <- paste(bills$primary_sponsor, bills$chamber_cosponsors, sep = '; ')
  for(i in 1:nrow(all_sponsors)){
    c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
    ### ONLY need to adjust this way if sponsored and cosponsored column are the same
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  }
  bills <- select(bills, -cospon_match)
  # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))
  
  #######################
  #### CLEAN NAMES
  all_sponsors$first_name <- ifelse(grepl("^[a-z]\\.", all_sponsors$LES_sponsor), gsub('\\..+', '', all_sponsors$LES_sponsor), "")
  all_sponsors$last_name <- gsub("^[a-z]\\.", '', all_sponsors$LES_sponsor)
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  
  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  ### If Term is 2008-2009, e.g., including 2007, 2008, and Specials for 2009 (when present)
  elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% elec_year:(elec_year + 1) | (year == elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% (elec_year - sen_term_length + 2):(elec_year + 1) | (year == elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)) %>% mutate(term = t_yrs); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)

  ###################################
  ####### Match Sponsors Names to Klarner Data
  
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), '')))
  klarner_sub$match_name <- tolower(unlist(str_extract_all(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]')))
  
  ### Drop Duplicated Klarner Candidats
  klarner_sub <- arrange(klarner_sub, desc(year)) %>% filter(!duplicated(cand))
  
  all_sponsors$elec_year <- all_sponsors$klarner_id <- all_sponsors$klarner_name <- NA
  all_sponsors$klarner_name <- as.character(all_sponsors$klarner_name)
  all_sponsors$klarner_id <- as.double(all_sponsors$klarner_id)
  all_sponsors$elec_year <- as.double(all_sponsors$elec_year)
  
  ### Loop though and Match
  for(i in 1:nrow(all_sponsors)){
    #k_matches <- filter(klarner_sub, last_name == tolower(all_sponsors[i,]$last_name))
    k_matches <- filter(klarner_sub, last_name == tolower(all_sponsors[i,]$last_name) & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
    
    ## Acccount for lack of punctuation or spaces in Klarner Data
    if(nrow(k_matches) == 0 & grepl("-|'| ", all_sponsors[i,]$last_name)){
      k_matches <- filter(klarner_sub, last_name == gsub("-|'| ", '', tolower(all_sponsors[i,]$last_name)) & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
    }
    
    ## Check Name Switch [klarner_match-data_match]
    if(nrow(k_matches) == 0 & grepl("-", all_sponsors[i,]$last_name)){
      k_matches <- filter(klarner_sub, last_name == gsub("-.+", '', tolower(all_sponsors[i,]$last_name)) & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
    }   
    
    ## Account for chamber if multiple matches
    if(nrow(k_matches) > 1){
      k_matches <- filter(klarner_sub, last_name == tolower(all_sponsors[i,]$last_name) & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
    }
    
    ### Save and Cross-Check
    if(nrow(k_matches) == 1){
      all_sponsors[i, ]$klarner_name <- k_matches$cand
      all_sponsors[i, ]$klarner_id <- k_matches$candid
      all_sponsors[i, ]$elec_year <- k_matches$year     
    } else if(nrow(k_matches) > 1 & length(unique(k_matches$cand)) != 1){
      ### CHeck First Initial
      m_sub <- grep(all_sponsors[i,]$match_name, k_matches$match_name)
      ### Check Without Punctuation --- Can't remove spaces unless do it for all_sponsors and k_matches
      if(length(m_sub) == 0){
        m_sub <- grep(gsub("-|'|`", '', all_sponsors[i,]$match_name), k_matches$match_name)        
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
    } else {
      print(glue("MISSING MATCH ::: {all_sponsors[i,]$LES_sponsor} ::: {i} ::: CHAMBER: {all_sponsors[i,]$chamber}"))
    }
  }
  
  ##### Fix MIsmatches
  if(t_yrs == "2003_2004"){
    all_sponsors[all_sponsors$LES_sponsor == "g.stevens" & all_sponsors$chamber == "S", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == '2009_2010'){
    all_sponsors[all_sponsors$LES_sponsor == "t.wilson" & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }
  
  #### CHeck for Duplicates from Matching
  duplicates <- filter(all_sponsors, !is.na(klarner_name)) %>% group_by(chamber, klarner_name) %>% filter(n() > 1)
  if(nrow(duplicates) > 0){
    cat('-----> Check DUPLICATE Sponsors \n ')
    print(select(duplicates, LES_sponsor, klarner_name, chamber) %>% as.data.frame())
  }; rm(duplicates)
  
  
  ##### CHECK IF ANY KLARNER CANDIDATES MISSING FROM DATA --- Exclude Senators elected T-1
  km <- filter(klarner_sub, !(candid %in% all_sponsors$klarner_id) )
  km <- filter(km, sen == 0 | (sen == 1 & year == as.numeric(substring(t_yrs, 1, 4)) - 1 ))
  
  #### DROP WINNERS WHO WERE NEVER (OR BARELY) SEATED
  if(t_yrs == '2003_2004'){
    km <- filter(km, cand != 'murkowski, lisa a.')
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
  
  #######################
  ############### Estimate Scores + Add in Relatd Variables
  #########################
  
  ### Check if bills in data without an ID'd sponsor
  # filter(bills, !(bills$primary_sponsor %in% legis_data$data_name))
  bills <- rename(bills, sponsor = LES_sponsor) %>%
    mutate(chamber = substring(bill_id, 1, 1))
  
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
  
  #### If LES == 0 and 
  LES[LES$LES == 0 & is.na(LES$num_sponsored_bills), c('num_sponsored_bills', 'sponsor_pass_rate', 'sponsor_law_rate')] <- 0
  
  ############### Save LES
  write.csv(LES, glue("{this_state}_LES_{t_yrs}.csv"), row.names = FALSE)
  rm(LES)
  
  cat(glue(" \n \n \n SESSION {t_yrs} ~~> DONE \n \n \n __________________________________________________________"))
}
################# ****** END LOOP

rm(all_sponsors, bills, legis_data, SS_bills, commem_bills, elec_year, i, keep_types)
rm(k_matches, klarner_sub, m_sub, km, bill_path, t_yrs, nonspon, t, c_sub, calc_LES)


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by GUBERNATORIAL APPOINTMENT --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
####### NOTES -- Full Member List is Here: http://www.akleg.gov/basis/mbr_info.asp?session=30
########################################################################################################################

#### ~~~~ ESTIMATING SCORES FOR THE 1993_1994 session! 
# -----> Dropping 350 bill(s) sponsored by COMMITTEE
# ~~~~~> Dropping 1 bills without a sponsor
#   term chamber        N AIC ABC PASS LAW
# 1 1993_1994       H 368 262 174  128  66
# 2 1993_1994       S 210 149 100   67  46

#### ~~~~ ESTIMATING SCORES FOR THE 1995_1996 session! 
# -----> Dropping 288 bill(s) sponsored by COMMITTEE
# ~~~~~> Dropping 5 bills without a sponsor
#   term chamber        N AIC ABC PASS LAW
# 1 1995_1996       H 393 278 196  165 110
# 2 1995_1996       S 195 152 111   90  60
### APPOINTED ~ HOUSE:
# -- LONG (don) -- 1/12/1996

#### ~~~~ ESTIMATING SCORES FOR THE 1997_1998 session! 
# -----> Dropping 318 bill(s) sponsored by COMMITTEE
# ~~~~~> Dropping 2 bills without a sponsor
#   term chamber        N AIC ABC PASS LAW
# 1 1997_1998       H 336 243 162  113  87
# 2 1997_1998       S 199 156 112   92  61


#### ~~~~ ESTIMATING SCORES FOR THE 1999_2000 session! 
# -----> Dropping 323 bill(s) sponsored by COMMITTEE
#         term chamber   N AIC ABC PASS LAW
# 1 1999_2000       H 303 218 167  112  84
# 2 1999_2000       S 142 102  86   58  53
### IN CHAMBER:
# -- torgerson, john -- first 2 years of term 2 of 2

#### ~~~~ ESTIMATING SCORES FOR THE 2001_2002 session! 
# -----> Dropping 340 bill(s) sponsored by COMMITTEE
#         term chamber   N AIC ABC PASS LAW
# 1 2001_2002       H 363 279 211  136  96
# 2 2001_2002       S 210 146 122   75  62
### APPOINTED ~ SENATE:
# -- STEVENS (BEN)

#### ~~~~ ESTIMATING SCORES FOR THE 2003_2004 session! 
# -----> Dropping 328 bill(s) sponsored by COMMITTEE
# ~~~~~> Dropping 3 bills without a sponsor
#         term chamber   N AIC ABC PASS LAW
# 1 2003_2004       H 402 292 229  161 141
# 2 2003_2004       S 247 162 136   93  81
### APPOINTED ~ HOUSE:
# -- DAHLSTROM
# -- OGG 
# -- STEPOVICH
### APPOINTED ~ SENATE:
# -- STEDMAN
# -- STEVENS (gary) -- won't print -- resigned from House, took Senate oath Feb 2003, matches to B.Stevens
### DROP:
# -- murkowski, lisa a. -- appointed to US Senate at start of 2003 (Dahlstrom filled seat)

#### ~~~~ ESTIMATING SCORES FOR THE 2005_2006 session! 
# -----> Dropping 238 bill(s) sponsored by COMMITTEE
# ~~~~~> Dropping 2 bills without a sponsor
#        term chamber   N AIC ABC PASS LAW
# 1 2005_2006       H 388 276 224  139 104
# 2 2005_2006       S 222 140 104   68  53
#### APPOINTED ~ SENATE:
# -- HUGGINS - Appointed Sept. 2004 -- Filled remainder of term (--> 2006)


##### ~~~~ ESTIMATING SCORES FOR THE 2007_2008 session! 
# -----> Dropping 147 bill(s) sponsored by COMMITTEE
#        term chamber   N AIC ABC PASS LAW
# 1 2007_2008       H 352 250 182  112  86
# 2 2007_2008       S 240 164 128   58  49
#### APPOINTED ~ HOUSE:
# -- KELLER
#### IN HOUSE:
# -- foster, richard
#### Name Note:
# -- Mary Kapsner changes name in final term to Mary Nelson 

#####  ~~~~ ESTIMATING SCORES FOR THE 2009_2010 session! 
# -----> Dropping 184 bill(s) sponsored by COMMITTEE
# ~~~~~> Dropping 2 bills without a sponsor
#         term chamber   N AIC ABC PASS LAW
# 1 2009_2010       H 333 210 156   86  74
# 2 2009_2010       S 227 167 143   67  52
### APPOINTED ~ HOUSE:
# -- WILSON (Tammie) -- Won't print -- Took oath in Dec. 2009, mismatches with P.Wilson 
### APPOINTED ~ SENATE:
# -- COGHILL -- Appointed Oct 27, 2009 -- via House
# -- EGAN  -- Appointed April 19, 2009


#### ~~~~ ESTIMATING SCORES FOR THE 2011_2012 session! 
# -----> Dropping 126 bill(s) sponsored by COMMITTEE
#         term chamber   N AIC ABC PASS LAW
# 1 2011_2012       H 315 220 153   91  46
# 2 2011_2012       S 164 131 113   50  29


#### ~~~~ ESTIMATING SCORES FOR THE 2013_2014 session! 
# -----> Dropping 100 bill(s) sponsored by COMMITTEE
# ~~~~~> Dropping 3 bills without a sponsor
#        term chamber   N AIC ABC PASS LAW
# 1 2013_2014       H 333 219 167  111  97
# 2 2013_2014       S 177 128 106   64  58
### APPOINTED ~ HOUSE:
# -- KITO (sam, iii) --- Feb. 26. 2014


#### ~~~~ ESTIMATING SCORES FOR THE 2015_2016 session! 
# -----> Dropping 144 bill(s) sponsored by COMMITTEE
# ~~~~~> Dropping 4 bills without a sponsor
#         term chamber   N AIC ABC PASS LAW
# 1 2015_2016       H 303 167 112   63  43
# 2 2015_2016       S 140  96  66   38  29
#### APPOINTED ~ HOUSE:
# -- SPOHNHOLZ
#### Name Note:
# -- Anna Fairclough changes name to Anna MacKinnon in final two terms


#### ~~~~ ESTIMATING SCORES FOR THE 2017_2018 session! 
# -----> Dropping 163 bill(s) sponsored by COMMITTEE
# ~~~~~> Dropping 2 bills without a sponsor
#        term chamber   N AIC ABC PASS LAW
# 1 2017_2018       H 323 206 155  102  64
# 2 2017_2018       S 142  98  78   45  32
#### APPOINTED ~ HOUSE:
# -- LINCOLN
# -- ZULKOSKY
#### APPOINTED ~ SENATE:
# -- SHOWER

# filter(klarner, grepl("foster, r", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid) %>% arrange(year, cand) %>% distinct() %>% as.data.frame() 
# filter(klarner, ddez == 17 & sen == 1 & outcome == 'w') %>% select(cand, year, sen, etype, outcome, ddez, candid, deter, partyz)


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

#### Quick Name Fixes
LES[LES$sponsor == "sullivanleonard, co",]$sponsor <- "sullivan-leonard, colleen"
LES[LES$sponsor == "bunde, con",]$sponsor <- "bunde, conley ralph"
LES[LES$sponsor == 'stevens' & LES$term == '2001_2002',]$sponsor <- 'stevens, ben'
# *** This may be moot with klarner updates
LES[LES$sponsor == 'lincoln' & LES$term == '2017_2018',]$sponsor <- 'lincoln, john'

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

### *** Manual Fixes *****
# -- Don Long --- Appointed in 1/12/1996, 1-Term -- http://www.akleg.gov/basis/Member/Detail/19?code=LNG
LES[LES$sponsor == "long" & LES$term == "1995_1996",]$klarner_id <- NA
LES[LES$sponsor == "long" & LES$term == "1995_1996",]$sponsor <- "long, don"

# -- Sam Kito iii
LES[LES$sponsor == "kito iii" & LES$term == "2013_2014",]$klarner_id <- 321803
LES[LES$sponsor == "kito iii" & LES$term == "2013_2014", c('sponsor', 'klarner_name')] <- 'kito, sam s.'


### FIVE Still missing = One-Term Specials or Elected in 2015-2016 Special
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[3]; print(name)
# filter(LES, grepl(name, sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber)
# rm(name, missing, name_sub, still_missing)


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
rm(check_dup, k_sub, exact)




########################################################################################################################################################
########################################################################################################################################################
######## AGGREGATE AND MATCH TO EXTERNAL
########################################################################################################################################################
########################################################################################################################################################

### Klarner State Leg. Election Data --- Using Adjusted File From Top
klarner_sub <- filter(klarner, sab == this_state & year >= min_year - 2 & outcome == 'w')
klarner_sub <- select(klarner_sub, caseid, year, sen, ddez, dno, term, termz, cando, cand, candid, partyz, partyt, exper, outcome, etype) 

#### KLARNER IS YEAR OF ELECTION, Not TERM
LES <- add_column(LES, exper = NA, party = NA, district = NA) %>% as.data.frame()

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
fill_missing <- data.frame(LES_name = "long, don", new_name = 'long, don', party = 'd', district = 37, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "stepovich", new_name = 'stepovich, nick', party = 'r', district = 10, exper = 'none')
### *** 2017-2018: If any of below run for reelection, won't be needed once klarner updates ****
fill_missing <- add_row(fill_missing, LES_name = "lincoln, john", new_name = 'lincoln, john', party = 'd', district = 40, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "zulkosky", new_name = 'zulkosky, tiffany', party = 'd', district = 38, exper = 'none')
fill_missing$district <- as.character(fill_missing$district)
fill_missing <- add_row(fill_missing, LES_name = "shower", new_name = 'shower, mike', party = 'r', district = 'E', exper = 'none')
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

#### DROP Nicknames
LES$sponsor <- gsub(' \\([^\\)]+\\)', '', LES$sponsor)


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

### Expanding Senate to Terms (So Adding a Second Term)
# ----------> Can't use the updated version of this in other scripts because HF has full chamber for some years despite stagger
senate <- filter(hf_data, CandId == 'aaaa')
for(i in 1:nrow(hf_data)){
  if(hf_data[i,]$chamber == "House") next
  sen_sub <- filter(hf_data, CandId == hf_data[i,]$CandId)
  if( !(paste0( hf_data[i,]$year + 3, "_",  hf_data[i,]$year + 4) %in% sen_sub$term) ){
    new_row <- hf_data[i,]  
    new_row$term <- paste0( hf_data[i,]$year + 3, "_",  hf_data[i,]$year + 4)
    new_row$MajorityMember <- NA
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
ideo$match_name <- tolower(ideo$name)
ideo$last_name <- gsub(',.+', '', tolower(ideo$name))

LES <- add_column(LES, SM_name = NA, SM_party = NA, np_score = NA) %>% as.data.frame()
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
  summarize(name_matches = paste(unique(sponsor), collapse = "----"),
            .groups = "drop") %>% 
  filter(grepl("----", name_matches))

### Matches In Which LES First Name != Shor-McCarty First Name
# -- Bob Roses is John Robert 'Bob' Roses
# -- Pete Petersen is James 'Pete' Petersen
# -- Clark Bishop = Clark 'Click' Bishop
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

#### FIX MISMATCHES
## LES[LES$sponsor %in% c('zzzzzzzzz'), c('SM_name', 'SM_party', 'np_score')] <- NA

#####################
### MANUAL FIXES
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('1993_1994', '2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('long', tolower(name))) %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

name_matches <- data.frame(LES_name = 'kapsner, mary', SM_name = 'Nelson, Mary')
name_matches <- add_row(name_matches, LES_name = 'kerttula, beth', SM_name = 'Kerttula, Elizabeth')
# name_matches <- add_row(name_matches, LES_name = 'long, don', SM_name = 'zzzzzzzz')
name_matches <- add_row(name_matches, LES_name = 'munoz, cathy e.', SM_name = 'Muñoz, Cathy')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}


##### PARTY SWITCHERS
# filter(LES, sponsor == 'williams, william') %>% select(sponsor, chamber, term, party)

### Dave Donley -- Switched D to R in 1997 --- https://www.adn.com/politics/2019/01/10/anchorage-school-board-member-named-deputy-commissioner-in-dunleavy-administration/
LES[LES$sponsor == 'donley, dave' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'Donley, Dave' & ideo$party == 'D',]$name
LES[LES$sponsor == 'donley, dave' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Donley, Dave' & ideo$party == 'D',]$party
LES[LES$sponsor == 'donley, dave' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Donley, Dave' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'donley, dave' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Donley, Dave' & ideo$party == 'R',]$name
LES[LES$sponsor == 'donley, dave' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Donley, Dave' & ideo$party == 'R',]$party
LES[LES$sponsor == 'donley, dave' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Donley, Dave' & ideo$party == 'R',]$np_score

### William Williams - Elected in 2000 as Republican for 2001_2002+-- https://mustreadalaska.com/bill-williams-may-21-1943-may-12-2019/
LES[LES$sponsor == 'williams, william' & LES$party == 'd',]$SM_name <- ideo[ideo$name == 'Williams, William' & ideo$party == 'D',]$name
LES[LES$sponsor == 'williams, william' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Williams, William' & ideo$party == 'D',]$party
LES[LES$sponsor == 'williams, william' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Williams, William' & ideo$party == 'D',]$np_score
LES[LES$sponsor == 'williams, william' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Williams, William' & ideo$party == 'R',]$name
LES[LES$sponsor == 'williams, william' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Williams, William' & ideo$party == 'R',]$party
LES[LES$sponsor == 'williams, william' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Williams, William' & ideo$party == 'R',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)


############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################

### Eliminate Excess White
LES$sponsor <- str_trim(LES$sponsor)

#### Manual Fixes
LES[LES$sponsor == 'roses, bob',]$sponsor <- 'roses, john robert'
LES[LES$sponsor == 'petersen, pete',]$sponsor <- 'petersen, james pete'
LES[LES$sponsor == 'bishop, click',]$sponsor <- 'bishop, clark c.'


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY
### http://akleg.gov/docs/pdf/ROSTERALL.pdf
LES$in_majority <- 0

### House -- 1993 - 2018
LES[as.numeric(substring(LES$term,1,4)) %in% c(2017:2018) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:2016) & LES$chamber == 'House'  & LES$party %in% 'r',]$in_majority <- 1

### Fixing 1993-1994 -- Ramona Barnes (R) elected via R/I coalition
# ---> See page 71 of http://akleg.gov/docs/pdf/ROSTERALL.pdf... Barnes stayed in place even after Carl Moses switched I to D in May 1994
LES[LES$term == "1993_1994" & LES$sponsor == "moses, carl e.",]$in_majority <- 1
LES[LES$term == "1993_1994" & LES$sponsor == "willis, ed",]$in_majority <- 1

### Fixing House 2017-2018 -- Dems control House with Defecting Republicans + Independents
# --> Reps in Maj. Coalitionn -- https://en.wikipedia.org/wiki/30th_Alaska_State_Legislature
LES[LES$term == '2017_2018' & LES$chamber == 'House' & LES$sponsor %in% c('ledoux, gabrielle', 'seaton, paul k.', 'stutes, louise b.'),]$in_majority <- 1
# --> Indeps in Maj. Coalition
LES[LES$term == '2017_2018' & LES$chamber == 'House' & LES$sponsor %in% c('ortiz, daniel h.', 'grenn, jason s.'),]$in_majority <- 1
# ----> 2019-2020 has a coalitions government again!

### Senate -- 1993 - 2018
LES[as.numeric(substring(LES$term,1,4)) %in% c(2007:2012) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1993:2006, 2013:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


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


###################################
#### Plot
####################################

library(ggplot2)
library(ggridges)
library(forcats)

LES %>%
  group_by(term, party) %>%
  summarize(mean_LES = mean(LES)) # %>% View()

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

# ---> House is interesting in 2017-2018!
# https://www.adn.com/politics/2016/11/09/alaska-senate-will-remain-under-republican-control-for-the-next-two-years/
# Run by a coalition of Republicans (a small group) and Democrats


#########
ggplot2::ggplot(LES, aes(x = np_score, y = LES, color = party)) + 
  geom_point() + 
  geom_smooth(formula = "y ~ x", method = 'lm', se = FALSE) + 
  facet_wrap(~ term) + 
  scale_color_manual(values=c("dodgerblue2",  'gray50', "red2", 'gray50'))

##### CHECK OUTLIERS ----> All remaining are pretty right-leaning dems
# filter(LES, party == 'd' & np_score > .5) %>% select(sponsor, term, chamber, np_score, party, SM_party, SM_name) %>% as.data.frame()
# filter(LES, party == 'r' & np_score < -.5) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + in_majority + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

