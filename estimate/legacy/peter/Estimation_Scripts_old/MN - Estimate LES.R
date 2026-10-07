

#####################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** MINNESOTA *** BY SESSION
#####################################

###################################
## SPECIAL SESSIONS:
## ---- Special Sessions in seperate files + BILL NUMBERS RESTART --> MERGE ON SESSION
## ---- Otherwise, bills carryover from regular session to regular session without need for reintroduction
## MEMBER LISTS:
## ---- SEARCH: https://www.leg.state.mn.us/legdb/search?search=session
## ---- Background Info on Legislators: https://www.leg.state.mn.us/legdb/
## ---- Excellent Committee Data: https://www.leg.state.mn.us/legdb/comm
## ---- LEADERSHIP/SESSION INFO! -- https://www.leg.state.mn.us/lrl/history/ 
## PROCESS/RULES:
## ---- https://www.revenue.state.mn.us/local_gov/prop_tax_admin/at_manual/16_01.pdf
## Sponsorship/Authorship
## ---- Primary Author listed in search results
## ---- Multiple authors permitted --- up to 4 total -- ()
###########################
## NOTES:
## (1) DROPPING ALL 1995/1997 HOUSE SPECIAL SESSIONS BILLS ********
## ------> No history information listed + ALL have Senate sponsor --> would need to scour the journals to get this stuff
## (2) All committee action basically folded into one line... so: if report, --> AIC
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

this_state <- 'MN'
min_year <- 1995
max_year <- 2018
keep_types <- c("HF", "SF")
spec_elec_codes <- c('s', 'gs')
# sen_term_length <- VARIABLE -- 4 years if elected in years ending in 2 and 6; 2 years if ends in 0

#### Output Directory
#dir.create(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))
setwd(glue("~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/LES_By_State/{this_state}"))

### Re-Estimate ALL SCORES
# delete <- readline(glue("For {this_state}: Do you want to DELETE ALL SCORES and REESTIMATE??? (y/n)"))
# if(delete == "y"){
#   file.remove(list.files(pattern = paste0('^', this_state, "_LES_\\d{4}_\\d{4}")))
# }

### Data Directory
terms <- seq(min_year, max_year, 2)

data_files <- list.files(glue("~/Dropbox/Data/State Legislative Data/States/{this_state}"), full.names = TRUE)
bill_files <- data_files[grepl('Bill_Details', data_files)]
sessions <- gsub('.+Bill_Details_|.csv', '', bill_files)
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
         #bill_id = ifelse(bill_type == "S" & !grepl("^SB", bill_id), gsub("^S", "SB", bill_id), bill_id),
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
# klarner[klarner$cand == 'littel, robert e.',]$cand <- "littell, robert e."
# ----> IDs will still be off, but need to keep them to match to external data...

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[1]

for(t in terms){
  
  ### Formulate 2-year terms -- Cover both regular and special sessions
  t_yrs <- as.character(glue('{t}_{t+1}'))
  t_sessions <- sessions[grepl(paste0(t, "|", t + 1), sessions)]
  
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
      bills <- bind_rows(bills, s_bills)
    }
    rm(s, s_bills)
  }
  
  ### Clean Term/Session Variables
  bills$term <- t_yrs
  bills$session <- gsub('_RS', "-RS", bills$session)
  bills$session <- gsub('_S', "-SS", bills$session)
  
  ### Check for duplicates
  if(nrow(bills) != nrow(distinct(bills))){
    cat(" \n ~~~> DUPLICATE BILLS \n\n .")
    break
  }
  
  ######## Standardize the Bill IDs
  bills <- rename(bills, bill_id = bill_number)
  
  ### Code Bill Types
  # --- Cross-checking conference committee reports as opening (see HF0057 in 2001 -- header is misplaced and is actually a bill)
  bills$bill_type <- gsub('^a ', '', str_extract(tolower(bills$summary), '^a (bill|resolution|house resolution|house [a-z]+ resolution|senate resolution|senate [a-z]+ resolution|joint resolution|memorial resolution)|^conference committee report'))
  bills$bill_type <- ifelse(bills$bill_type == "conference committee report", 
                            gsub('^a | for an act', '', str_extract(tolower(bills$summary), 'a (bill for an act|resolution|house resolution|house [a-z]+ resolution|senate resolution|senate [a-z]+ resolution|joint resolution|memorial resolution)')), 
                            bills$bill_type)
  bills$bill_type <- ifelse(is.na(bills$bill_type), gsub('^a ', '', str_extract(tolower(bills$description), '^a (bill|resolution|house resolution|house [a-z]+ resolution|senate resolution|senate [a-z]+ resolution|joint resolution|memorial resolution)|^conference committee report')), bills$bill_type)
  bills$bill_type <- ifelse(is.na(bills$bill_type), gsub('^a ', '', str_extract(substring(tolower(bills$summary), 1, 45), 'a (bill|resolution|house resolution|house [a-z]+ resolution|senate resolution|senate [a-z]+ resolution|joint resolution|memorial resolution)|^conference committee report')), bills$bill_type)
  bills$bill_type <- ifelse(is.na(bills$bill_type) & grepl('^a +senate res|^senate resolution|^a senate [a-z]+ res', tolower(bills$description)), 'senate resolution', bills$bill_type)
  
  bills$bill_type <- ifelse(is.na(bills$bill_type) & grepl('^SR|^HR', bills$bill_id), 'resolution', bills$bill_type)
  bills$bill_type <- ifelse(is.na(bills$bill_type), 'bill', bills$bill_type)
  
  bills$bill_type <- gsub('house |senate ', '', bills$bill_type)
  
  ### Drop Resolutions
  # table(bills$bill_type)
  bills <- filter(bills, bill_type == 'bill')
  
  #### Drop 1995 Special Session 1 HOUSE Bills with (1) No Actions on Website AND (2) Only a SENATE Sponsor Listed
  if(t_yrs == '1995_1996'){
    bills <- filter(bills, !(session == '1995-SS1' & bill_id %in% c("HF0001", 'HF0002', 'HF0003', 'HF0004', 'HF0005')))
  }
  
  #### Drop ALL 1997/1998 Special Session HOUSE Bills --> (1) No Actions on Website AND (2) Only SENATE Sponsors Listed
  if(t_yrs == '1997_1998'){
    bills <- filter(bills, !(session %in% c('1997-SS1', '1997-SS2', '1997-SS3', '1998-SS1') & grepl("HF", bill_id))) ## ALL of the House BIlls
  }
  
  ############### Standardize Sponsors
  #### Accents in Names 
  bills$author <- tolower(bills$author)
  bills$author <- gsub('á', 'a', bills$author)
  bills$author <- gsub('é', 'e', bills$author)
  bills$author <- gsub('ó', 'o', bills$author)
  bills$author <- gsub('í', 'i', bills$author)
  bills$author <- gsub('ñ', 'n', bills$author)
  
  #### Accents in Names 
  bills$coauthors <- tolower(bills$coauthors)
  bills$coauthors <- gsub('á', 'a', bills$coauthors)
  bills$coauthors <- gsub('é', 'e', bills$coauthors)
  bills$coauthors <- gsub('ó', 'o', bills$coauthors)
  bills$coauthors <- gsub('í', 'i', bills$coauthors)
  bills$coauthors <- gsub('ñ', 'n', bills$coauthors)
  
  #### Adjusting for Bills By Request -- Need to do BEFORE splitting
  if(any(grepl('\\(br\\)|by request| br$', bills$author))){
    print("-----> BY REQUEST BILLS -- ADAPT SCRIPT --- BREAK"); break
    #print(glue("-----> KEEPING {nrow(filter(bills, grepl('(br)|by request', introducers)))} bill(s) introduced BY REQUEST"))
  }
  
  #### DROP Committee Bills
  if(any(grepl('committee', bills$author))){
    print(glue("-----> DROPPING {nrow(filter(bills, grepl('committee', author)))} bill(s) introduced BY COMMITTEE"))
    bills <- filter(bills, !grepl('committee', author))
  }
  
  #### LES Sponsor Variable
  bills$LES_sponsor <- bills$author
  # table(bills$LES_sponsor)
  
  #### Fill in Missing Sponsors with Full Sponsor List
  if(any(bills$LES_sponsor == "")){
    print(glue("-----> Filling in {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor using cosponsors"))
    bills$LES_sponsor <- ifelse(bills$LES_sponsor == "", gsub(';.+', '', bills$coauthors), bills$LES_sponsor)
  }
  
  ###### CHeck Missing Sponsors
  # filter(bills, LES_sponsor == "") %>% View()
  if(nrow(filter(bills, LES_sponsor == '')) > 0){
    print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == ''))} bill(s) without a sponsor"))
    bills <- filter(bills, !(LES_sponsor == ''))     
  }

  ###################
  ###### Merge in S&S Bills
  ###################
  # *** For MINNESSOTA: Regular session bills carry-over; Special session bill numbers restart
  # --> Merging on RS AND SPECIAL + Using Special Nums from RA PDFs
  # --> Assuming SS with most proposed bills in cases where special_num is missing
  SS_term <- SS_bills %>% filter(term == t_yrs) %>% mutate(H_max = NA, S_max = NA, s_spec = NA, any_specials = any(grepl(paste0("-SS"), bills$session)))
  
  ## Adjusting Max Specials by yr
  for(yr in as.numeric(str_split(t_yrs, "_")[[1]]) ){
    if(nrow(filter(SS_term, year == yr)) > 0 & any(grepl(paste0(yr, "-SS"), bills$session))){
      H_max <- filter(bills, grepl(yr, session) & grepl("^H", bill_id) & grepl("SS", session)) %>% mutate(num = as.numeric(gsub('[A-Z]+', '', bill_id))) %>% pull(num)
      H_max <- ifelse(length(H_max) >= 1, max(H_max), NA)
      S_max <- filter(bills, grepl(yr, session) & grepl("^S", bill_id) & grepl("SS", session)) %>% mutate(num = as.numeric(gsub('[A-Z]+', '', bill_id))) %>% pull(num)
      S_max <- ifelse(length(S_max) >= 1, max(S_max), NA)
      which_spec <- which(table(bills[grepl(yr, bills$session) & grepl("SS", bills$session),]$session) == max(table(bills[grepl(yr, bills$session) & grepl("SS", bills$session),]$session)))[1]
      which_spec <- names(table(bills[grepl(yr, bills$session) & grepl("SS", bills$session),]$session)[which_spec])
      SS_term[SS_term$year == yr,]$H_max <- H_max
      SS_term[SS_term$year == yr,]$S_max <- S_max
      SS_term[SS_term$year == yr,]$s_spec <- which_spec
      rm(H_max, S_max, which_spec)
    }
  }; rm(yr)
  
  SS_term <- SS_term %>%
    mutate(special = ifelse((grepl("^H", bill_id) & is.na(H_max)) | (grepl("^S", bill_id) & is.na(S_max)), 0, special),
           special = ifelse(!any_specials, 0, special),
           special = ifelse(grepl("^H", bill_id) & !is.na(H_max) & special == 1 & num_only > H_max, 0, special),
           special = ifelse(grepl("^S", bill_id) & !is.na(S_max) & special == 1 & num_only > S_max, 0, special),
           rs_year = substring(t_yrs, 1, 4),
           session = ifelse(special == 0, paste0(rs_year, '-RS'), ifelse(is.na(special_num), s_spec, paste0(year, '-SS', special_num)))) %>%
    distinct(term, session, bill_id, SS)
  
  ### Merge
  if(nrow(SS_term) > 0){
    bills <- bills %>% left_join(SS_term, by = c("bill_id", "term", "session")) %>% mutate(SS = ifelse(is.na(SS), 0, SS))
  }else{
    bills$SS <- 0
  }
  
  ### Check Missing
  # table(bills$SS)
  # anti_join(SS_term, bills, by = c("bill_id", "term", "session"))
  

  ######################################################################
  ############### Code Commemorative
  ######################################################################
  
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session'))
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  ######################################################################
  ############### Code Bill History
  ######################################################################
  bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
  bill_hist <- read.csv(bill_hist_path)
  bill_hist$journal_page <- as.character(bill_hist$journal_page)
  
  ## If multiple sessions, read in those as well
  if(length(t_sessions) > 1){
    for(s in t_sessions[2:length(t_sessions)]){
      bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
      s_hist <- read.csv(bill_path)
      s_hist$journal_page <- as.character(s_hist$journal_page)
      bill_hist <- bind_rows(bill_hist, s_hist)
    }
    rm(s, s_hist)
  }
  
  ######## Standardize BillHist Bill IDs
  bill_hist <- rename(bill_hist, bill_id = bill_number) 
  
  ### Clean Term/Session Variables
  bill_hist$term <- t_yrs
  bill_hist$session <- gsub('_RS', "-RS", bill_hist$session)
  bill_hist$session <- gsub('_S', "-SS", bill_hist$session)
  
  ### Rearrange + create order variable that covers both chambers
  bill_hist <- bill_hist %>%
    arrange(session, bill_id, action_date, chamber_order) %>%
    group_by(session, bill_id) %>%
    mutate(order = 1:n()) %>%
    ungroup()

  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  ## -- '1st Reading without Reference' --> SKIPPED COMMITTEE
  ## -- IF Add in RES: 'placed on desk' + 'public hearing held' + 'filed with secretary of state'
  ## -- Omitting "substituted in committee" --> Means the companion passed the other chamber and is swapped in
  aic_t <- c('^comm rpt', '^committee report', '^committee rpt', '^comm report', 'referred by chair')
  # All reports come with recoomendations of some kind ---> AIC
  # 'Committee report, no recommendation' -- bills often survive this; no examples of failed reprots
  abc_t <- c('^comm rpt', '^committee report', '^committee rpt', '^comm report', 'second reading', 
             'third reading', 'taken from table', 'rules suspended','^re-referred', 'special order', '^amend', 
             'point of order', '^general orders', 'placed on calendar')
  # 2nd reading occurs after committee report accepted, bill then put on agenda
  # 'designated special order' --> bill given floor priority for debate + other special orders = floor action
  pc_t <- c('third reading, passed', "third reading passed", '^bill was passed', 'received from house', 
            'received from senate', '^bill was repassed')
  # repassed = check (conf comm usually)
  law_t <- c('signed by gov', "governor.+approval", "effective date") 
  # *** Law error: sometimes bill will be passed, vetoed, but still listed as chapter number N or sec state filed
  # 'chapter number', 'secretary of state',
  # ---> See SF3, 2017-SS1 or https://www.revisor.mn.gov/bills/bill.php?b=house&f=hf861&ssn=0&y=2017
  # bill_hist %>% group_by(session, bill_id) %>%
  #   summarize(signed = any(grepl("signed by gov|governor.+approval", tolower(action))),
  #             veto = any(grepl("veto", tolower(action))),
  #             chapter = any(grepl("chapter number [0-9]", tolower(action))),
  #             e_date = any(grepl("effective date", tolower(action)))) # %>% filter(veto == TRUE)
  # filter(bill_hist, bill_id == "SF0905" & session == "2003-RS") %>% select(3,4,5) %>% as.data.frame()
  
  ### Check Actions
  # filter(bill_hist, grepl('effective ', tolower(action))) %>% distinct(action) %>% View()
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
  options(warn = 2)
  for(i in 1:nrow(bills)){
    b_id = bills[i,]$bill_id
    s_id = bills[i,]$session
    b_spon = bills[i,]$LES_sponsor
    hist_sub <- filter(bill_hist, bill_id == b_id & session == s_id)
    bill_stages <- evaluate_bill_hist(hist_sub, b_id, t_yrs, s_id, b_spon, aic_t, abc_t, pc_t, law_t,
                                      ignore_chamber_switch = TRUE) ## Misordered if passed on day x and immediatedly transferred to outchamber
    bill_stages$bill_url <- bills[i,]$bill_url
    all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
    # print(i)
  }
  options(warn = 1)
  
  ### Check Codings
  ## ** CAN CHECK NUM LAWS HERE: https://www.leg.state.mn.us/lrl/history/bills
  ## ** ---> NOTE: COUNT NOT ALWAYS CORRECT -- See rvest comparison below... ***
  cat("\n")
  all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
    summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% as.data.frame() %>% print()
  ### *** Using Effective Date works correctly ***
  ### *** Below shows all the correct bills, including those vetoed ***
  # library(rvest)
  # passed <- data.frame(bill_id = NA, session = NA, veto = NA)
  # pages <- c("https://www.revisor.mn.gov/laws/2001/0/", "https://www.revisor.mn.gov/laws/2001/1/", "https://www.revisor.mn.gov/laws/2002/0/", "https://www.revisor.mn.gov/laws/2002/1/")
  # for(page in pages){
  #   bill_nums <- read_html(page) %>% html_nodes("#session_table") %>% html_nodes('tr')#%>% html_text()
  #   session <- ifelse(grepl("2001/1/", page), "2001-SS1", ifelse(grepl("2002/1", page), "2002-SS1", "2001-RS"))
  #   for(row in bill_nums){
  #     cells <- html_nodes(row, 'td')
  #     if(length(cells) == 0){ next }
  #     b_id = str_trim(html_text(cells[2]))
  #     b_id = paste0(gsub("[0-9]+", "", b_id), str_pad(gsub("HF|SF", "", b_id), 4, pad = 0))
  #     veto = str_trim(html_text(cells[4]))
  #     passed <- add_row(passed, bill_id = b_id, session = session, veto = veto)
  #   }
  # }
  # passed <- filter(passed, !is.na(bill_id)) %>% mutate(known_pass = 1) %>% filter(toupper(veto) != "FULL")
  # abs <- full_join(all_bill_stages, passed, by = c("bill_id", "session")) %>% mutate(known_pass = ifelse(is.na(known_pass), 0, known_pass))
  # table(abs$law, abs$known_pass)
  # filter(abs, is.na(LES_sponsor))
  # filter(bill_hist, bill_id == "HF0057") %>% as.data.frame()
  # filter(bills, bill_id == "HF0057")
  
  ### MERGE to BILL DATA
  bills <- left_join(bills, all_bill_stages, by = intersect(colnames(bills), colnames(all_bill_stages))) 
  
  ### MERGE In COMMEMS
  all_bill_stages <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(all_bill_stages, ., by = c('bill_id', 'term', 'session'))
  
  ### MERGE In S&S
  if(nrow(SS_term) > 0){
    all_bill_stages <- SS_term %>% 
      select(bill_id, term, session, SS) %>%
      left_join(all_bill_stages, ., by = c('bill_id', 'term', "session")) %>%
      mutate(SS = ifelse(is.na(SS), 0, SS))
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
  rm(b_id, b_spon, s_id, bill_hist)
  
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
  bills$cospon_match <- paste(bills$author, bills$coauthors, sep = '; ')
  for(i in 1:nrow(all_sponsors)){
    c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
    all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
    ### ONLY need to adjust this way if sponsored and cosponsored column are the same
    all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  }
  bills <- select(bills, -cospon_match)
  # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills))
  
  ###########################
  #### CLEAN NAMES
  ##########################
  all_sponsors$last_name <- gsub(',.+', '', all_sponsors$LES_sponsor)
  all_sponsors$first_name <- ifelse(grepl(',', all_sponsors$LES_sponsor), gsub('.+, ', '', all_sponsors$LES_sponsor), '')
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  
  #### Update Last Names for Matching 
  if(t_yrs %in% c('1995_1996') ){
    all_sponsors[all_sponsors$LES_sponsor == "reichgott junge",]$last_name <-  "reichgott"
  }
  if(t_yrs %in% c("2007_2008", "2009_2010")){
    all_sponsors[all_sponsors$LES_sponsor == "torres ray",]$last_name <-  "ray"
    all_sponsors[all_sponsors$LES_sponsor == "erickson ropes",]$last_name <-  "ropes"
    all_sponsors[all_sponsors$LES_sponsor == "prettner solon",]$last_name <-  "solon"
  }
  if(t_yrs %in% c('2011_2012', "2013_2014", "2015_2016", "2017_2018") ){
    all_sponsors[all_sponsors$LES_sponsor == "torres ray",]$last_name <-  "ray"
  }
  if(t_yrs == '2017_2018'){
    all_sponsors[all_sponsors$LES_sponsor == "maye quade",]$last_name <-  "quade"
  }

  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  ### If Term is 2008-2009, e.g., including 2007, 2008, and Specials for 2009 (when present)
  H_elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  
  ### Account for shifting Terms/Elec Years (2-4-4) --- Elections in, e.g, 1990, 2000, 2010 = 2-year, all others = 4-year
  S_elec_year <- ifelse(substring(H_elec_year, 4, 4) %in% c(4, 8), H_elec_year - 2, H_elec_year) 
  sen_term_length <- ifelse(substring(S_elec_year, 4,4 ) == 0, 2, 4)
  
  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% H_elec_year:(H_elec_year + 1) | (year == H_elec_year + 2 & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% S_elec_year:(S_elec_year + sen_term_length - 1) | (year == S_elec_year + sen_term_length & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz, middle) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)
  
  #########################
  ######## Match Sponsors Names to Klarner Data, Forgoing Function because mostly only have last name
  
  ### Create Match Variables
  klarner_sub$last_name <- gsub(',.+', '', klarner_sub$cand)
  all_sponsors$match_name <- gsub('\\.', '', tolower(paste0(all_sponsors$last_name, ifelse(all_sponsors$first_name != '', paste0(", ", all_sponsors$first_name), ''))))
  klarner_sub$match_name <- tolower(unlist(str_extract(klarner_sub$cand, '[a-zA-z]+, [a-zA-z]')))
  
  ### Edit Match Name
  if(t_yrs == "1995_1996"){
    klarner_sub[klarner_sub$cand == 'anderson, bob 1',]$match_name <- 'anderson, r'
    klarner_sub[klarner_sub$cand == 'johnson, janet',]$match_name <- 'johnson, jb'
    klarner_sub[klarner_sub$cand == 'johnson, dean',]$match_name <- 'johnson, de'
    klarner_sub[klarner_sub$cand == 'johnson, douglas',]$match_name <- 'johnson, dj'
  }else if(t_yrs %in%  c('1997_1998', "1999_2000")){
    klarner_sub[klarner_sub$cand == 'johnson, janet',]$match_name <- 'johnson, jb'
    klarner_sub[klarner_sub$cand == 'johnson, dean',]$match_name <- 'johnson, de'
    klarner_sub[klarner_sub$cand == 'johnson, douglas',]$match_name <- 'johnson, dj'
    klarner_sub[klarner_sub$cand == 'johnson, david w.',]$match_name <- 'johnson, dh' # The W in Klarner is wrong... 
  }else if(t_yrs == "2001_2002"){
    all_sponsors[all_sponsors$LES_sponsor == 'johnson, dave',]$match_name <- 'johnson, dave'
    all_sponsors[all_sponsors$LES_sponsor == 'johnson, dean',]$match_name <- 'johnson, dean'
    all_sponsors[all_sponsors$LES_sponsor == 'johnson, debbie',]$match_name <- 'johnson, debbie'
    all_sponsors[all_sponsors$LES_sponsor == 'johnson, doug',]$match_name <- 'johnson, doug'
    klarner_sub[klarner_sub$cand == 'johnson, david w.',]$match_name <- 'johnson, dave'
    klarner_sub[klarner_sub$cand == 'johnson, dean',]$match_name <- 'johnson, dean'
    klarner_sub[klarner_sub$cand == 'johnson, debbie',]$match_name <- 'johnson, debbie'
    klarner_sub[klarner_sub$cand == 'johnson, douglas',]$match_name <- 'johnson, douglas'
  }else if(t_yrs %in% c('2003_2004', '2005_2006') ){
    klarner_sub[klarner_sub$cand == 'johnson, dean',]$match_name <- 'johnson, de'
    klarner_sub[klarner_sub$cand == 'johnson, debbie',]$match_name <- 'johnson, dj'
  } else if(t_yrs == '2013_2014'){
    klarner_sub[klarner_sub$cand == 'ward, john',]$match_name <- 'ward, je'
    klarner_sub[klarner_sub$cand == 'ward, joann',]$match_name <- 'ward, ja'
  }
  # filter(klarner, grepl('johnson,', cand) & sen == 1 & year == 2000 & outcome == 'w') %>% select(year, cand, sen)
  # filter(all_sponsors, grepl('johnson,', LES_sponsor)) %>% select(LES_sponsor, chamber, term, match_name)
  
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
      # ## Check First Initial
      # if(length(m_sub) == 0){
      #   match_name2 <- tolower(paste0(all_sponsors[i,]$last_name, ", ", substring(all_sponsors[i,]$first_name, 1, 1) ))
      #   m_sub <- grep(match_name2, tolower(unlist(str_extract(k_matches$cand, '[a-zA-z]+, [a-zA-z]'))))
      # }
      ## Check Middle Name
      if(length(m_sub) == 0 & grepl(', [a-z][a-z]$', all_sponsors[i,]$match_name)){  #!is.na(all_sponsors[i,]$middle_name)
        #match_name2 <- tolower(paste0(all_sponsors[i,]$last_name, ", ", all_sponsors[i,]$middle_name))
        # m_sub <- grep(match_name2, k_matches$match_name)
        k_matches$match_name2 <- paste0(k_matches$match_name, substring(k_matches$middle, 1,1))
        m_sub <- grep(all_sponsors[i,]$match_name, k_matches$match_name2)
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
  if(t_yrs == "1995_1996"){
    all_sponsors[all_sponsors$LES_sponsor == "carlson, s." & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == '1997_1998'){
    all_sponsors[all_sponsors$LES_sponsor == "clark, j." & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
    all_sponsors[all_sponsors$LES_sponsor == "otremba, m." & all_sponsors$chamber == "H", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }else if(t_yrs == '2001_2002'){
    all_sponsors[all_sponsors$LES_sponsor == "solon, y.p." & all_sponsors$chamber == "S", c("klarner_name", "klarner_id", "elec_year")] <- NA
  }
  
  #### CHeck for Duplicates from Matching
  duplicates <- filter(all_sponsors, !is.na(klarner_name)) %>% group_by(chamber, klarner_name) %>% filter(n() > 1)
  if(nrow(duplicates) > 0){
    cat('-----> Check DUPLICATE Sponsors \n ')
    print(select(duplicates, LES_sponsor, klarner_name, chamber) %>% as.data.frame())
  }; rm(duplicates)
  
  ##### CHECK IF ANY KLARNER CANDIDATES MISSING FROM DATA --- Includes all Senaters elected at T-1
  km <- filter(klarner_sub, !(candid %in% all_sponsors$klarner_id) )
  km <- filter(km, sen == 0 | (sen == 1 & year %in% S_elec_year:(as.numeric(substring(t_yrs, 1, 4))) ))
  
  ### Remove candidates who won but were never seated
  if(t_yrs == "1995_1996"){
    km <- filter(km, !(cand %in% c('benson, joanne', 'adkins, betty', 'benson, duane', 'mcgowan, pat', 'luther, william (bill)')) )
  }else if(t_yrs == "1999_2000"){
    km <- filter(km, cand != 'beckman, tracy')
  }else if(t_yrs == '2003_2004'){
    km <- filter(km, cand != 'mcelroy, dan')
    km <- filter(km, cand != 'holsten, mark')
  }else if(t_yrs == '2005_2006'){
    km <- filter(km, cand != 'knutson, dave')
  }else if(t_yrs == '2009_2010'){
    km <- filter(km, !(cand %in% c('wergin, betsy', 'neuville, tom', 'larson, dan')) )
  }else if(t_yrs == "2011_2012"){
    km <- filter(km, cand != 'sertich, anthony (tony)')
  } else if(t_yrs == "2013_2014"){
    km <- filter(km, cand != 'gottwalt, steve')
    km <- filter(km, cand != 'morrow, terry')
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
    select(-c(first_name, match_name)) %>% #middle_name, last_name, suffix,
    select(sponsor, data_name, klarner_name, klarner_id, chamber, term, elec_year, num_sponsored_bills, sponsor_pass_rate, sponsor_law_rate, num_cosponsored_bills) %>%
    arrange(chamber, sponsor)
  
  ##################################################
  ####### Estimate Scores + Add in Relatd Variables
  ##################################################
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

#agg_stats, matches
rm(all_sponsors, bills, legis_data, SS_bills, H_elec_year, S_elec_year, i, keep_types)
rm(k_matches, klarner_sub, m_sub, km, bill_path, t_yrs, calc_LES, c_sub) # 
rm(t, terms, klarner_gs, commem_bills, t_sessions)

########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
## Vacancies filled by SPECIAL ELECTIONS --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
## Outstanding Database of MN State Officials: https://www.leg.state.mn.us/legdb/results?search=session&gender=both&sess=79&body=both&q=
## ---> Includes exact dates of election, OCCUPATION, Committees, birthplace, religion, etc
####################################
# filter(klarner, grepl("corrado", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(klarner, ddez == 31 & sen == 1) %>% select(cand, year, sen, etype, outcome, ddez, candid)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1995_1996 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 5 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- GUNTHER; STANEK; WARKENTIN
# -- CARLSON (skip, last name duplicate, won't show in script)
### WON SEPCIAL ~ SENATE:
# -- FISCHBACH, KLEIS; KRAMER; LIMMER (via H)
# -- OURADA; SCHEEVEL
### DROP -- Elected 1992, not seated after 1994 H election
# -- benson, joanne 
# -- adkins, betty
# -- benson, duane
# -- mcgowan, pat
# -- luther, william (bill)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1997_1998 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 42 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- ERICKSON
# -- VENDEVEER
# -- CLARK, J -- (last name duplicate, won't show)
# -- OTREMBA, M -- (last name duplicate, won't show)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 1 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- SWAPINSKI
### WON SEPCIAL ~ SENATE:
# -- KIERLIN; KINKEL; RING; ZIEGLER
### DROP:
# -- beckman, tracy -- elected in 1996 to 4-year term, not seated in 1999

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# -----> Filling in 1 bill(s) without a sponsor using cosponsors
# -----> Dropping 1 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- BLAINE
### WON SEPCIAL ~ SENATE:
# -- MOUA
# -- SOLON (yvonne) -- won special in january after husband passed away in dec 2001 -- name duplicate, won't show up in output

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- NEWMAN; OTTO; POWELL; ZELLERS
### DROP:
# mcelroy, dan -- appointd to state position 1/5/2003
# holsten, mark -- appointed to state position on 1/17/2003

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- HAWS
### WON SEPCIAL ~ SENATE:
# -- BONOFF; CLARK; GERLACH (via H); KOCH
### DROP:
# -- knutson, dave -- elected to senate in 2002, resigned june 9, 2004, with 2+ years left

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# -----> Filling in 2 bill(s) without a sponsor using cosponsors
# -----> Dropping 1 bill(s) without a sponsor
### WON SPECIAL ~ HOUSE:
# -- DRAZKOWSKI
### WON SEPCIAL ~ SENATE:
# -- DAHLE
 
# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> Filling in 2 bill(s) without a sponsor using cosponsors
# -----> Dropping 2 bill(s) without a sponsor
### WON SEPCIAL ~ SENATE:
# -- DAHLE (in 2008)
# -- PARRY
### DROP -- All Senators who resigned prior to start of 2009 term 
# -- wergin, betsy
# -- neuville, tom
# -- larson, dan

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- ALLEN; MELIN
### WON SEPCIAL ~ SENATE:
# -- DZIEDZIC; EATON; HAYDEN (via H); KOENEN (via H); MCGUIRE (via H)
### IN HOUSE
# -- PELOWSKI (gene)
### DROP:
# -- sertich, anthony (tony) -- resigned after 1 week, 1/13/2011, to take state position

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# session chamber    N AIC ABC PASS LAW
### WON SPECIAL ~ HOUSE:
# -- JOHNSON, C (clark) --- Multiple potential mismatches
# -- THEIS
### IN HOUSE, no bills:
# -- THISSEN (paul) -- speaker
### DROP:
# -- gottwalt, steve -- won reelection but resigned 1/3/2013
# -- morrow, terry - won reelection but resigned 12/19/2012


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- ANDERSON, C
# -- ECKLUND
# -- FLANAGAN
### WON SEPCIAL ~ SENATE:
# -- ABELER (via H)

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
### WON SPECIAL ~ HOUSE:
# -- MUNSON
### WON SEPCIAL ~ SENATE:
# -- BIGHAM (past H)


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

### Error Fixes
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_id <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$klarner_name <- NA
# LES[LES$data_name %in% "owlett" & LES$term %in% "2017_2018",]$sponsor <- "owlett, clint"

### ****Still missing***** 
# ---> Remaining = 2017-2018
# still_missing <- LES[is.na(LES$klarner_id),]$sponsor
# name = still_missing[1]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term)
# filter(klarner, grepl('zzzzzzz', cand)) %>% select(cand, year, etype, sen, outcome, candid)
# rm(name, still_missing)


### Fix Missing
name_matches <- data.frame(LES_name = "carlson, s", k_name = 'carlson, skip')
name_matches <- add_row(name_matches, LES_name = "erickson", k_name = 'erickson, sondra')
name_matches <- add_row(name_matches, LES_name = 'solon, yp', k_name = 'solon, yvonne prettner')
name_matches <- add_row(name_matches, LES_name = 'otto', k_name = 'otto, rebecca l. w.')
name_matches <- add_row(name_matches, LES_name = 'clark', k_name = 'clark, tarryl')
name_matches <- add_row(name_matches, LES_name = 'anderson, c', k_name = 'anderson, chad')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_id),]$klarner_id <- unique(klarner[klarner$cand == name_matches[i,]$k_name,]$candid)
  LES[LES$sponsor == name_matches[i,]$LES_name & is.na(LES$klarner_name),]$klarner_name <- name_matches[i,]$k_name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$sponsor <- name_matches[i,]$k_name
}
rm(name_matches, i)


## MANUAL FIXES
# LES[LES$sponsor %in% "pierce" & LES$term %in% "2011_2012",]$klarner_id <- 308794
# LES[LES$sponsor %in% "pierce" & LES$term %in% "2011_2012",]$klarner_name <- "pierce, justin"
# LES[LES$sponsor %in% "pierce" & LES$term %in% "2011_2012",]$sponsor <- "pierce, justin"


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
rm(check_dup, k_sub, exact, missing, name_sub)


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

### Manually Fix Those Not in Klarner
LES[LES$sponsor == "munson",]$party <- 'r'
LES[LES$sponsor == "munson",]$district <- 23
LES[LES$sponsor == "munson",]$exper <- 'none'
LES[LES$sponsor == "munson",]$sponsor <- 'munson, jeremy'

### Manually Fixing Klarner Party Error --- Only 1 term of hers is wrong
LES[LES$sponsor == 'anderson, ellen',]$party <- 'd'

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)

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
senate <- filter(hf_data, chamber == "Senate" & substring(year, 4, 4) %in% c(2, 6))
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
LES[LES$term == '2017_2018', set_NA] <- NA
# ---> This should be fine, 2014-2015 was first 2 years, so 2016_2017 should be mostly right
rm(hf_data, set_NA)

#################################
### Shor and McCarty Data, 1993 - 2016
####################################

ideo <- readstata13::read.dta13("~/Dropbox/Data/Shor_McCarty_Data/shor_mccarty_1993_2016_individual_data_May_2018.dta")
ideo <- filter(ideo, st == this_state) %>% select(-st_id)
ideo$match_name <- str_extract(tolower(ideo$name), '[a-z]+, [a-z]+')
ideo$match_name <- ifelse(is.na(ideo$match_name), tolower(ideo$name), ideo$match_name)
ideo$last_name <- gsub(',.+', '', tolower(ideo$name))

##### Getting Rid of Duplicates in Shor/McCarty Data... Basically Same Person, but different terms
ideo <- ideo %>% mutate(num_years = rowSums(select(., starts_with("senate"), starts_with("house")), na.rm = TRUE) )
ideo <- ideo %>% mutate(name = str_trim(name)) %>% group_by(name, party) %>% filter(num_years == max(num_years)) %>% ungroup()



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
# ** Joe Bertram was in Senate 1995-1996; Bertram Record in SM Data = House 1995-1996
LES[LES$sponsor %in% c('bertram, joe', 'carlson, john m.', 'johnson, robert'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# Notable Names: NA
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

### MANUAL FIXES ----> MN: Lots of Missingness in the mid to late 2000s; not clear why as the MN website data is good...
# filter(LES, is.na(np_score)) %>% filter(term != '2017_2018') %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('thiss', tolower(name))) %>% as.data.frame() # %>% select(name, party, st, np_score, num_years)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

name_matches <- data.frame(LES_name = 'brown, chuck', SM_name = 'Brown')
name_matches <- add_row(name_matches, LES_name = 'housley, karin', SM_name = 'Housely, Karin') # SM name mispelled here
name_matches <- add_row(name_matches, LES_name = 'lynch, teresa', SM_name = 'Lynch')
name_matches <- add_row(name_matches, LES_name = 'ray, patricia torres', SM_name = 'Torres Ray, Patricia')
name_matches <- add_row(name_matches, LES_name = 'ropes, sharon', SM_name = 'Erickson Ropes, Sharon')
name_matches <- add_row(name_matches, LES_name = 'solon, yvonne prettner', SM_name = 'Prettner Solon, Yvonne')
name_matches <- add_row(name_matches, LES_name = 'vanengen, tom', SM_name = 'Van_Engen')
name_matches <- add_row(name_matches, LES_name = 'thissen, paul', SM_name = 'Thissen, Paul C') # Two matches
# name_matches <- add_row(name_matches, LES_name = 'zzzzzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

#### Manual Edits (Needs more precision...)

### Ron Erhardt switched parties, but matches only to the Dem Obs
LES[LES$sponsor == 'erhardt, ron' & LES$party == 'r',]$SM_name <- ideo[ideo$name == 'Erhardt',]$name
LES[LES$sponsor == 'erhardt, ron' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Erhardt',]$party
LES[LES$sponsor == 'erhardt, ron' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Erhardt',]$np_score

### Both below = *Multipole records with num_years identical
LES[LES$sponsor == 'anderson, paul h.',]$SM_name <- ideo[ideo$name == 'Anderson, Paul' & ideo$house2016 %in% 1,]$name
LES[LES$sponsor == 'anderson, paul h.',]$SM_party <- ideo[ideo$name == 'Anderson, Paul' & ideo$house2016 %in% 1,]$party
LES[LES$sponsor == 'anderson, paul h.',]$np_score <- ideo[ideo$name == 'Anderson, Paul' & ideo$house2016 %in% 1,]$np_score

LES[LES$sponsor == 'daudt, kurt',]$SM_name <- ideo[ideo$name == 'Daudt, Kurt' & ideo$house2015 %in% 1,]$name
LES[LES$sponsor == 'daudt, kurt',]$SM_party <- ideo[ideo$name == 'Daudt, Kurt' & ideo$house2015 %in% 1,]$party
LES[LES$sponsor == 'daudt, kurt',]$np_score <- ideo[ideo$name == 'Daudt, Kurt' & ideo$house2015 %in% 1,]$np_score

### Sheila Kiscaden -- Party Switch to INdep in 2003, never became D but caucused with them (with short exception at start)
# -- https://www.leg.state.mn.us/legdb/fulldetail?ID=10323
LES[LES$sponsor == "kiscaden, sheila" & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Kiscaden, Sheila' & ideo$party == 'R',]$name
LES[LES$sponsor == "kiscaden, sheila" & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Kiscaden, Sheila' & ideo$party == 'R',]$party
LES[LES$sponsor == "kiscaden, sheila" & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Kiscaden, Sheila' & ideo$party == 'R',]$np_score
LES[LES$sponsor == "kiscaden, sheila" & LES$party == 'nonmaj',]$SM_name <-  ideo[ideo$name == 'Kiscaden, Sheila' & ideo$party == 'D',]$name
LES[LES$sponsor == "kiscaden, sheila" & LES$party == 'nonmaj',]$SM_party <- ideo[ideo$name == 'Kiscaden, Sheila' & ideo$party == 'D',]$party
LES[LES$sponsor == "kiscaden, sheila" & LES$party == 'nonmaj',]$np_score <- ideo[ideo$name == 'Kiscaden, Sheila' & ideo$party == 'D',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1995 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:1998, 2007:2010, 2013:2014, 2019:2020) & LES$chamber == 'House'  & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1999:2006, 2011:2012, 2015:2018) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1995 - 2020
LES[as.numeric(substring(LES$term,1,4)) %in% c(1995:2010, 2013:2016) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(2011:2012, 2017:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


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
LES[LES$sponsor == 'bradley, fran',]$sponsor <- 'bradley, francis'
LES[LES$sponsor == 'price, len',]$sponsor <- 'price, leonard'
LES[LES$sponsor == 'pappas, sandy',]$sponsor <- 'pappas, sandra'
LES[LES$sponsor == 'goodwin, barb',]$sponsor <- 'goodwin, barbara'
LES[LES$sponsor == 'reiter, mady',]$sponsor <- 'reiter, madelyn'
LES[LES$sponsor == 'mcnamara, denny',]$sponsor <- 'mcnamara, dennis'
LES[LES$sponsor == 'morrow, terry',]$sponsor <- 'morrow, terence'
LES[LES$sponsor == 'day, dick',]$sponsor <- 'day, richard'
LES[LES$sponsor == 'eken, kent',]$sponsor <- 'eken, bernhard kent'
#LES[LES$sponsor == 'zzzzz',]$sponsor <- 'zzzzzz'


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
  scale_color_manual(values=c("dodgerblue2", "gray50", "red2"))

#### Check Party Switchers
# -- Berg Was Indep 1997-2000, then R, D immediately prior to 97 (but also R and Nonpartisan prior)... SM only have D
# -- Dean Johnson switched R to D in 2000, SM only have D
# -- Robert Lessard, switch to and ran as indep in 2001 -- did not caucus with eitehr party -- https://www.leg.state.mn.us/legdb/fulldetail?ID=10372
# filter(LES, party != tolower(SM_party)) %>% select(sponsor, SM_name, party, SM_party, term, chamber, np_score, LES, LES_rank) %>% arrange(sponsor, term)

### WHo's the super liberal Republican??? Andrew R Ciesla... 
# filter(LES, party == 'r' & np_score < -.5) %>% select(sponsor, SM_name, party, SM_party, term, chamber, np_score, LES, LES_rank)
# --- SM have him as a D, but def an R: https://ballotpedia.org/Andrew_Ciesla
# --- Was an R leader! https://en.wikipedia.org/wiki/Andrew_R._Ciesla

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')

### Majority Member...
# stargazer::stargazer(lm(LES ~ lag(LES) + MajorityMember + factor(sponsor) + factor(chamber), data = LES), omit = 'factor', type = 'text')

