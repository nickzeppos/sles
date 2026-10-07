
##########################################################
### ESTIMATE EFFECTIVENESS SCORES FOR *** IDAHO *** BY SESSION
################################################################

###################################
## SPECIAL SESSIONS:
## ---- Special Sessions permitted; bill numbers re-start; bills do not carry over from term to term (regular)
## MEMBER LISTS:
## ---- 
## PROCESS/RULES:
## ---- Process: https://legislature.idaho.gov/resources/howabillbecomesalaw/
## SPONSORSHIP/AUTHORSHIP
## ---- Strict limits on when individual members can propose bills:
## ---> "A bill may be introduced by a member, a group of members or a standing committee. After the 20th day 
## of the session in the House and the 12th day in the Senate, bills may be introduced only by committee. 
## After the 36th day bills may be introduced only by certain committees. In the House: State Affairs, 
## Appropriations, Education, Revenue and Taxation, Health and Welfare and Ways and Means Committee. In the 
## Senate: State Affairs, Finance, and Judiciary and Rules." from process page above (how a bill..)
## ---- FLOOR Sponsors = ID'ed at 3rd reading:
## ----> "Each bill is sponsored by a member who is known as the “floor sponsor.” This member opens and closes 
## debate in favor of passage of the bill." (process page)
#################
## FOR CODING COMMITTEE BILLS
# (1) Using the Individual who REQUESTED THE BILL
# (2) Because requestor is often an agency, filling remaining with the FLOOR MANAGER
# (3) Becauses floor managers only assigned if bill reaches floor, Dropping remaining...
# -----> Could also use... First Cosponsor? ...Committee Chair?
###########################
## NOTES:
## --- In Idaho, Members may take a leave of absence during which time a substitute replaces them
## ---------> For our purposes (1) Coding Substitutes as unique legislators; (2) Dropping elected members IFF leave covered entire term
#############################

rm(list=ls())
options(stringsAsFactors = FALSE, scipen = 999)

library(tidyr)
library(dplyr)
library(stringr)
library(ggplot2)
library(purrr)
library(readxl)
library(glue)
library(readr)

this_state <- 'ID'
min_year <- 1999 ### Have 1998 but elections are in Nov of even years, so incomplete
max_year <- 2018
keep_types <- c("H", "S")
spec_elec_codes <- c('s', 'gs')
house_term_length <- 2
sen_term_length <- 2 # STAGGERED? NA

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
         bill_id = gsub("^SB", "S", gsub("^HB", "H", bill_id)),
         SS = 1) %>%
  select(state, term, year, bill_id, everything())

#### Check SS BIll Types 
# table(SS_bills$bill_type)
# filter(SS_bills, bill_type == "S")

### Load Klarner Data 
load("~/Dropbox/Data/US Election Data/state_leg/klarner_data/196slers1967to2016_20180908.RData")
klarner <- table; rm(table)
klarner$sab <- toupper(klarner$sab)
klarner <- filter(klarner, year > min_year - 5 & sab == this_state)

#### FIX/DROP PROBLEMATIC KLARNER OBSERVATIONS (Doubles, Multi-race candidates, etc.)
# filter(klarner, grepl('robison', cand)) %>% distinct(cand)
klarner[klarner$cand == 'robinson, kenneth l.',]$cand <- "robison, kenneth l."
klarner[klarner$cand == 'arthuranthon, kelly',]$cand <- "anthon, kelly arthur" # Arthur appears to just be his middle name
klarner[klarner$cand == 'whitneymanwaring, dustin',]$cand <- "manwaring, dustin whitney" # Whitney appears to just be his middle name

###########################
## LOOP THROUGH TERMS/SESSIONS
#########################
# t <- terms[1]

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
  bills <- read_csv(bill_path, col_types = cols())
  bills$session <- as.character(bills$session)
  
  ### If multiple sessions in different files, read in those as well
  if(length(t_sessions) > 1){
    for(s in t_sessions[2:length(t_sessions)]){
      bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Details_{s}.csv")
      s_bills <- read_csv(bill_path, col_types = cols())
      s_bills$session <- as.character(s_bills$session)
      bills <- bind_rows(bills, s_bills)
    }
    rm(s, s_bills)
  }
  
  ### Clean Term/Session Variables
  bills$term <- t_yrs
  bills$session <- ifelse(grepl('spcl', bills$session), gsub('spcl', '-SS', bills$session), paste0(bills$session, '-RS'))
  
  ### Check for duplicates
  if(nrow(bills) != nrow(distinct(bills))){
    cat("\n ~~~> DUPLICATE BILLS \n\n .")
    break
  }
  
  ######## For 2009+: Merge in Info From Parsed PDFs
  if(t >= 2009){
    pdf_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Parsed_SOP_PDFs_{t_sessions[1]}.csv")
    pdf_data <- read.csv(pdf_path)
    pdf_data$session <- as.character(pdf_data$session)
    for(s in t_sessions[2:length(t_sessions)]){
      pdf_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Parsed_SOP_PDFs_{s}.csv")
      p_bills <- read.csv(pdf_path)
      p_bills$session <- as.character(p_bills$session)
      pdf_data <- bind_rows(pdf_data, p_bills)
      rm(p_bills, pdf_path)
    }
    pdf_data$term <- t_yrs
    pdf_data$session <- ifelse(grepl('spcl', pdf_data$session), gsub('spcl', '-SS', pdf_data$session), paste0(pdf_data$session, '-RS'))
    bills <- select(bills, -c(requestor_SOP, requestor_SOP_full)) %>%
      left_join(., pdf_data, by = intersect(colnames(.), colnames(pdf_data)))
  }
  
  ######## Standardize the Bill IDs
  bills <- bills %>%
    rename(bill_id = bill_number) %>% 
    mutate(bill_id = gsub('[a-z]$', '', bill_id))
  
  ############### Drop Resolutions, Messages, Communications, Reports
  bills <- mutate(bills, bill_type = toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))
  # table(bills$bill_type)
  bills <- filter(bills, bill_type %in% keep_types) %>% select(-bill_type)
  
  ##########################
  ####### Standardize Sponsors
  
  #### Standardize
  bills$author <- tolower(bills$author)
  bills$author <- gsub('á', 'a', bills$author)
  bills$author <- gsub('é', 'e', bills$author)
  bills$author <- gsub('ó', 'o', bills$author)
  bills$author <- gsub('í', 'i', bills$author)
  bills$author <- gsub('ñ', 'n', bills$author)

  bills$cosponsors <- tolower(bills$cosponsors)
  bills$cosponsors <- gsub('á', 'a', bills$cosponsors)
  bills$cosponsors <- gsub('é', 'e', bills$cosponsors)
  bills$cosponsors <- gsub('ó', 'o', bills$cosponsors)
  bills$cosponsors <- gsub('í', 'i', bills$cosponsors)
  bills$cosponsors <- gsub('ñ', 'n', bills$cosponsors)
  
  #### Manually Fix SOP FULL
  if(t == 1999){
    bills[bills$bill_id == "H0528" & bills$session == "2000-RS",]$requestor_SOP_full <- 'Chuck Everett      (208) 375-2323      David E. Kerrick   (208) 459-4574 Associated Innkeepers of Idaho'
    bills[bills$bill_id == "H0528" & bills$session == "2000-RS",]$author <- ''
  }
  
  #### Need to use the full as the constrained one cuts off some reps/senators
  bills$requestor_SOP_full <- tolower(bills$requestor_SOP_full)
  bills$requestor_SOP_full <- gsub('á', 'a', bills$requestor_SOP_full)
  bills$requestor_SOP_full <- gsub('é', 'e', bills$requestor_SOP_full)
  bills$requestor_SOP_full <- gsub('ó', 'o', bills$requestor_SOP_full)
  bills$requestor_SOP_full <- gsub('í', 'i', bills$requestor_SOP_full)
  bills$requestor_SOP_full <- gsub('ñ', 'n', bills$requestor_SOP_full)
  bills$requestor_SOP_full <- gsub('; ;', ';', bills$requestor_SOP_full)
  bills$requestor_SOP_full <- str_trim(gsub('^contact primary sponsors|^contact primary |^primary(- | )|^contact person(:|;)|^person(:|;)|^contacts(:|;)|^contacts|^contact(- | ;)|^contact|^\\(contact\\)|^names:|^names|^name:|^name :|^name(- | )|^\\(name\\)', ' ', bills$requestor_SOP_full))
  bills$requestor_SOP_full <- gsub('^:;|^;:|^; |^;|^: |^:', '', bills$requestor_SOP_full)
  bills$requestor_SOP_full <- gsub('^rep\\.|^rep |^representatives(,|:|;) |^representatives |^representative s |^repres[a-z]+ve ', 'representative ', bills$requestor_SOP_full)
  bills$requestor_SOP_full <- gsub('^sen\\.|^sen |^sentor |^senators(,|:|;) |^senators |^senator s ', 'senator ', bills$requestor_SOP_full)
  bills$requestor_SOP_full <- str_trim(gsub('  +', ' ', bills$requestor_SOP_full))
  bills$requestor_SOP_full <- str_trim(gsub('\\.$|,$', '', bills$requestor_SOP_full))
  bills$requested_by <- ifelse(grepl("^representative|^senator|^speaker|^mr\\. speaker", bills$requestor_SOP_full), str_extract(bills$requestor_SOP_full, 'mr\\. speaker|(representative|senator|speaker) [a-z]\\.[a-z]\\. \\"[a-z]+\\" [a-z]+|(representative|senator|speaker) [a-z]+ [a-z]\\.[a-z]\\. \\"[a-z]+\\" [a-z]+|(representative|senator|speaker) [a-z]+ ([a-z]|[a-z]\\.) \\"[a-z]+\\" [a-z]+|(representative|senator|speaker) [a-z]+ ([a-z]|[a-z]\\.) [a-z]+|(representative|senator|speaker) [a-z]+ \\"[a-z]+\\" [a-z]+|(representative|senator|speaker) [a-z]+ [a-z]+|(representative|senator|speaker) [a-z]+|(representative|senator|speaker) \\"[a-z]+\\" [a-z]+'), "")
  bills$requested_by <- str_trim(gsub(" and$", '', bills$requested_by))
  # select(bills, requestor_SOP, requestor_SOP_full, requested_by) %>% View()
  
  ###### Looping Through Missing Requested By Bills to Check if Any Are Reps/Sens but Missing Title
  for(full_name in unique(bills$requested_by)){
    clean_name <- gsub('senator |representative ', '', full_name)
    ## Only trying to match names with at least a first/last
    ## --> Don't want to wind up matching "senator j" to anything starting with a j...
    if(grepl(" ", clean_name)){
      clean_name <- str_replace_all(clean_name, "(\\W)", "\\\\\\1")
      name_matches <- grep(paste0('^', clean_name), bills[bills$requested_by %in% "",]$requestor_SOP_full)
      if(length(name_matches) > 0){
        bills[bills$requested_by %in% "",][name_matches,]$requested_by <- full_name
        #print(full_name)
      }
    }
  }
  rm(full_name, clean_name, name_matches)
  
  ##### Drop Matches for Bills From Wrong Chamber (Below Process will fill some in)
  bills$requested_by <- ifelse(substring(bills$bill_id, 1, 1) == "H" & grepl("^senator", bills$requested_by), '', bills$requested_by)
  bills$requested_by <- ifelse(substring(bills$bill_id, 1, 1) == "S" & grepl("^representative", bills$requested_by), '', bills$requested_by)
  
  #### Finally: Extract Any Names with Senator or Representative from remaining
  final_match <- ifelse(substring(bills$bill_id, 1, 1) == "H", 
                        str_extract(bills$requestor_SOP_full, '(representative|rep\\.|speaker) [a-z]\\.[a-z]\\. \\"[a-z]+\\" [a-z]+|(representative|rep\\.|speaker) [a-z]+ ([a-z]\\.|[a-z]) [a-z]+|(representative|rep\\.|speaker) [a-z]+ [a-z]+|(representative|rep\\.|speaker) [a-z]\\. [a-z]+|(representative|rep\\.|speaker) [a-z]+'),
                        str_extract(bills$requestor_SOP_full, '(senator|sen\\.) [a-z]\\.[a-z]\\. \\"[a-z]+\\" [a-z]+|(senator|sen\\.) [a-z]+ ([a-z]\\.|[a-z]) [a-z]+|(senator|sen\\.) [a-z]+ [a-z]+|(senator|sen\\.) [a-z]\\. [a-z]+|(senator|sen\\.) [a-z]+'))
  final_match <- gsub('^rep\\.', 'representative', final_match)
  final_match <- gsub("^sen\\.", 'senator', final_match)
  final_match <- gsub(" and$", "", final_match)
  bills$requested_by <- ifelse( (bills$requested_by == "" | is.na(bills$requested_by)) & !is.na(final_match), final_match, bills$requested_by)
  rm(final_match)
  
  ###### Clean Words that Are Not Names of End of Requested By
  bills$requested_by <- gsub(' representative$| rep$| senator$| substituting$', '', bills$requested_by)
  
  ###### Requested by Manual Edits
  if(t == 1999){
    bills[bills$requested_by %in% c("representative lee gager"),]$requested_by <- 'representative lee gagner'
    bills[bills$requested_by %in% c("representative lenore hardy"),]$requested_by <- 'representative lenore hardy barrett'
    bills[bills$requested_by %in% c("representative tom loertcher"),]$requested_by <- 'representative tom loertscher'
    ## Miller wasn't actually in chamber (left office 1998..); also requested by Doug Jones
    bills[bills$requested_by %in% c("representative maynard miller"),]$requested_by <- 'representative doug jones'
    bills[bills$H_floor_sponsor %in% c("Representative Miller(Trail)"),]$H_floor_sponsor <- 'Representative Trail'
    bills[bills$requested_by %in% c("representative mary lou"),]$requested_by <- 'representative mary lou shepherd'
    bills[bills$S_floor_sponsor %in% c("Senator Camerob"),]$S_floor_sponsor <- 'Senator Cameron'
    bills[bills$requested_by %in% c("senator diede"),]$requested_by <- 'senator deide'
    bills[bills$requested_by %in% c("senator gary"),]$requested_by <- 'senator gary schroeder'
    bills[bills$requested_by %in% c("senator cecil ingrain"),]$requested_by <- 'senator cecil ingram'
    bills[bills$requested_by %in% c("senator robbi king", "senator robbi mng"),]$requested_by <- 'senator robbi king-barrutia'
    bills$S_floor_sponsor <- gsub('Senator King$', 'Senator King-Barrutia', bills$S_floor_sponsor)
    bills[bills$requested_by %in% c("senator grant lpsen"),]$requested_by <- 'senator grant ipsen'
    bills[bills$requested_by %in% c("senator shiela sorenson"),]$requested_by <- 'senator sheila sorensen'
  }else if(t == 2001){
    bills[bills$requested_by %in% c("representative mary lou"),]$requested_by <- 'representative mary lou shepherd'
    bills[bills$requested_by %in% c("senator robbi king", "senator robbie king", "senator robbi barrutia"),]$requested_by <- 'senator robbi king-barrutia'
    bills[bills$requested_by %in% c("senator sorenson"),]$requested_by <- 'senator sheila sorensen'
    bills$H_floor_sponsor <- gsub('Kellogg\\(Duncan\\)', "Kellogg", bills$H_floor_sponsor)
    bills[bills$requested_by %in% c("representative robert schaeffer"),]$requested_by <- 'representative robert schaefer'
    bills[bills$requested_by %in% c("representative sher seliman"),]$requested_by <- 'representative sher sellman'
    bills[bills$requested_by %in% c("senator hal eunderson"),]$requested_by <- 'senator hal bunderson'
    bills[bills$requested_by %in% c("senator rurtenshaw", "senator burtenshaw"),]$requested_by <- 'senator don burtenshaw'
  }else if(t == 2003){
    bills[bills$author %in% "smith (24)",]$author <- "representative leon smith"
    bills[bills$requested_by %in% "representative john a",]$requested_by <- "representative john a. stevenson"
    bills[bills$requested_by %in% c("representative mary lou"),]$requested_by <- 'representative mary lou shepherd'
    ### Also a Paul shepherd, so will standardize downscript
    bills[bills$requested_by %in% c("representative sheperd"),]$requested_by <- 'representative shepherd'
    bills[bills$requested_by %in% c("representative w"),]$requested_by <- 'representative w. w. deal'
    bills[bills$requested_by %in% c("senator patty anne"),]$requested_by <- 'senator patti anne lodge'
    bills$S_floor_sponsor <- gsub('Compton\\(Duncan\\)', "Compton", bills$S_floor_sponsor)
    bills[bills$requested_by %in% c("senator mike eurkett"),]$requested_by <- 'senator mike burkett'
    bills$S_floor_sponsor <- gsub('Hill\\(Hill\\)', "Hill", bills$S_floor_sponsor)
    bills[bills$requested_by %in% c("senator j"),]$requested_by <- 'senator j. stanley williams'
    bills[bills$requested_by %in% c("senator r"),]$requested_by <- 'senator r. skip brandt'
    bills$S_floor_sponsor <- gsub('Sorensen\\(Sorensen\\)', "Sorensen", bills$S_floor_sponsor)
    #bills[bills$requested_by %in% c("senator r"),]$requested_by <- 'senator brandt'
    bills[bills$requested_by %in% c("senator sorenson"),]$requested_by <- 'senator sheila sorensen'
    #bills[bills$requested_by %in% c("senator bilbao substituting"),]$requested_by <- 'senator bilbao' # substituting for little (weird)
  }else if(t == 2005){
    bills[bills$requested_by %in% c("representative mary lou"),]$requested_by <- 'representative mary lou shepherd'
    bills$H_floor_sponsor <- gsub('Hart\\(Jacobson\\)', "Hart", bills$H_floor_sponsor)
    bills[bills$requested_by %in% c("speaker of the"),]$requested_by <- 'speaker bruce newcomb'
    bills[bills$requested_by %in% c("senator mike jorgensen"),]$requested_by <- 'senator mike jorgenson'
    bills[bills$requested_by %in% c("senator r"),]$requested_by <- 'senator r. skip brandt'
  }else if(t == 2007){
    bills[bills$requested_by %in% c("representative lenore hardy"),]$requested_by <- 'representative lenore hardy barrett'
    bills[bills$requested_by %in% c("representative mary lou"),]$requested_by <- 'representative mary lou shepherd'
    bills[bills$requested_by %in% c("representative leortscher"),]$requested_by <- 'representative tom loertscher'
    bills[bills$requested_by %in% c("representative russ matthews"),]$requested_by <- 'representative russ mathews'
    bills[bills$requested_by %in% c("representative m. m"),]$requested_by <- 'representative m. m. moyle'
    bills[bills$requested_by %in% c("representative bayer sen"),]$requested_by <- 'representative bayer'
    bills[bills$requested_by %in% c("representative john vander", "representative vander woude"),]$requested_by <- 'representative john vander woude'
    bills[bills$requested_by %in% c("senator patti anne"),]$requested_by <- 'senator patti anne lodge'
    bills[bills$requested_by %in% c("senator richard dick"),]$requested_by <- 'senator richard sagness' # ACTING SENATOR
    #bills[bills$requested_by %in% c("senator andreason rep"),]$requested_by <- 'senator andreason'
    #bills[bills$requested_by %in% c("senator coiner representative"),]$requested_by <- 'senator coiner'
    bills$S_floor_sponsor <- gsub('Werk\\(Douglas\\)', "Werk", bills$S_floor_sponsor)
  }else if(t == 2009){
    bills$requested_by <- gsub(' office$| phone$', '', bills$requested_by)
    bills[bills$requested_by %in% c("representative lenore hardy"),]$requested_by <- 'representative lenore hardy barrett'
    bills[bills$requested_by %in% c("representative anne pasley"),]$requested_by <- 'representative anne pasley-stuart'
    bills[bills$requested_by %in% c("representative r"),]$requested_by <- 'representative r. j. harwood'
    bills[bills$requested_by %in% c("representative ra"),]$requested_by <- 'representative raul r. labrador'
    bills[bills$requested_by %in% c("representative richwills"),]$requested_by <- 'representative richard wills'
    bills[bills$requested_by %in% c("senator patti anne"),]$requested_by <- 'senator patti anne lodge'
    bills$S_floor_sponsor <- gsub('Senator Stennettm', "Senator Michelle Stennett", bills$S_floor_sponsor)
  }else if(t == 2011){
    bills[bills$requested_by %in% c("representative lenore hardy"),]$requested_by <- 'representative lenore hardy barrett'
    bills[bills$requested_by %in% c("representative r"),]$requested_by <- 'representative r. j. harwood'
    bills[bills$requested_by %in% c("representative john vander", "representative vander woude"),]$requested_by <- 'representative john vander woude'
    bills[bills$requested_by %in% c("senator patti anne"),]$requested_by <- 'senator patti anne lodge'
  }else if(t == 2013){
    bills[bills$requested_by %in% c("representative lenore hardy"),]$requested_by <- 'representative lenore hardy barrett'
    bills[bills$requested_by %in% c("representative janie ward"),]$requested_by <- 'representative janie ward-engelking'
    bills[bills$requested_by %in% c("representative john vander", "representative vander woude"),]$requested_by <- 'representative john vander woude'
    bills[bills$requested_by %in% c("senator patti anne"),]$requested_by <- 'senator patti anne lodge'
    bills[bills$requested_by %in% c("senator cherie buckner"),]$requested_by <- 'senator cherie buckner-webb'
    bills$S_floor_sponsor <- gsub('Johnson\\(Fulcher\\)', "Johnson", bills$S_floor_sponsor)
    bills[bills$requested_by %in% c("senator janie ward"),]$requested_by <- 'senator janie ward-engelking'
  }else if(t == 2015){
    bills[bills$requested_by %in% c("representative caroline nilsson"),]$requested_by <- 'representative caroline nilsson troy'
    bills[bills$requested_by %in% c("representative john vander", "representative vander woude"),]$requested_by <- 'representative john vander woude'
    bills[bills$requested_by %in% c("senator cherie buckner"),]$requested_by <- 'senator cherie buckner-webb'
    bills[bills$requested_by %in% c("senator lori den"),]$requested_by <- 'senator lori den hartog'
    bills[bills$requested_by %in% c("senator thayne"),]$requested_by <- 'senator thayn'
  }else if(t == 2017){
    bills[bills$requested_by %in% c("representative caroline nilsson"),]$requested_by <- 'representative caroline nilsson troy'
    bills[bills$requested_by %in% c("representative john vander", "representative vander woude"),]$requested_by <- 'representative john vander woude'
    bills$H_floor_sponsor <- gsub('Tway\\(Kloc\\)', "Tway", bills$H_floor_sponsor)
    bills[bills$requested_by %in% c("senator patti anne"),]$requested_by <- 'senator patti anne lodge'
    bills[bills$requested_by %in% c("senator kelly arthur"),]$requested_by <- 'senator kelly arthur anthon'
    bills[bills$requested_by %in% c("senator cherie buckner"),]$requested_by <- 'senator cherie buckner-webb'
    bills[bills$requested_by %in% c("senator lori den"),]$requested_by <- 'senator lori den hartog'
    bills$S_floor_sponsor <- gsub('Smith\\(Rice\\)', "Smith", bills$S_floor_sponsor)
    bills[bills$requested_by %in% c("senator janie ward"),]$requested_by <- 'senator janie ward-engelking'
  }
  # filter(bills, grepl('urten', requested_by)) %>% distinct(requested_by)
  # filter(bills, grepl('hart', tolower(H_floor_sponsor)) ) %>% distinct(H_floor_sponsor)
  # filter(bills, grepl('anthon', tolower(S_floor_sponsor)) ) %>% distinct(S_floor_sponsor)
  # filter(bills, grepl('representative repre', LES_sponsor)) %>% select(-summary, bill_url) %>% as.data.frame()

  ###### Clean Floor Managers
  bills$H_floor_sponsor <- gsub('No sponsor data\\.', '', bills$H_floor_sponsor)
  bills$S_floor_sponsor <- gsub('No sponsor data\\.', '', bills$S_floor_sponsor)
  bills$origin_floor_sponsor <- ifelse(substring(bills$bill_id, 1, 1) == "H", bills$H_floor_sponsor, bills$S_floor_sponsor)
  bills$origin_floor_sponsor <- tolower(bills$origin_floor_sponsor)
  bills$origin_floor_sponsor <- gsub('á', 'a', bills$origin_floor_sponsor)
  bills$origin_floor_sponsor <- gsub('é', 'e', bills$origin_floor_sponsor)
  bills$origin_floor_sponsor <- gsub('ó', 'o', bills$origin_floor_sponsor)
  bills$origin_floor_sponsor <- gsub('í', 'i', bills$origin_floor_sponsor)
  bills$origin_floor_sponsor <- gsub('ñ', 'n', bills$origin_floor_sponsor)
  bills$origin_floor_sponsor <- gsub('representative -|representative floor (sponsorn|sponsor) -', 'representative ', bills$origin_floor_sponsor)
  bills$origin_floor_sponsor <- gsub('senator -', 'senator ', bills$origin_floor_sponsor)
  bills$origin_floor_sponsor <- str_trim(gsub('  +|\\&.+|\\&$| and .+|,.+', ' ', bills$origin_floor_sponsor))
  bills$origin_fs_dist <- str_extract(bills$origin_floor_sponsor, '\\([0-9]+\\)')
  bills$origin_floor_sponsor <- str_trim(gsub('  +', ' ', gsub('\\([0-9]+\\)', '', bills$origin_floor_sponsor)))
  bills$origin_floor_sponsor <- ifelse(grepl('^representative$|^senator$', bills$origin_floor_sponsor), '', bills$origin_floor_sponsor)
  #table(bills$origin_floor_sponsor)
  
  ###### Identify Committees and Swap in Requestor/Floor Sponsors Where Necessary/Possible
  comms <- c("agricult", "appropriat", "apropriat", "business", 'commerce', "education", 'finance',
             'environment', 'health and welfare', 'judiciary', 'administration', 'local gov', 'taxation',
             'resources', 'revenue', 'state affairs', 'transportation', 'defense', 'ways and means')
  bills$comm_sponsored_bill <- ifelse(grepl(paste(comms, collapse = "|"), bills$author), 1, 0)
  bills$LES_sponsor <- ifelse((bills$comm_sponsored_bill == 1 | bills$author == "") & bills$requested_by != "" & !is.na(bills$requested_by), 
                              bills$requested_by, 
                              ifelse((bills$comm_sponsored_bill == 1 | bills$author == "") & bills$origin_floor_sponsor != "" & !is.na(bills$origin_floor_sponsor), 
                                     bills$origin_floor_sponsor, 
                                     bills$author))
  # table(bills$LES_sponsor)
  
  ###### Drop Remaining Committee Sponsored Bills
  if(nrow(filter(bills,  grepl(paste(comms, collapse = "|"), LES_sponsor))) > 0 | any(grepl('committee', bills$LES_sponsor))){
    cat('\n')
    print(glue('-----> Dropping {nrow(filter(bills, grepl(paste(comms, collapse = "|"), LES_sponsor) | grepl("committee", LES_sponsor) ))} UNCODED Committee Sponsored Bills (N = {nrow(bills)})'))
    bills <- filter(bills, !(grepl(paste(comms, collapse = "|"), LES_sponsor) | grepl("committee", LES_sponsor)))
  }
  rm(comms)
  
  ##### DROP Bills with No SPonsor
  if(nrow(filter(bills, LES_sponsor == '')) > 0 | any(is.na(bills$LES_sponsor))){
    cat('\n')
    print(glue("-----> Dropping {nrow(filter(bills, LES_sponsor == '')) + sum(is.na(bills$LES_sponsor))} bill(s) without a sponsor"))
    bills <- filter(bills, !(LES_sponsor == '' | is.na(LES_sponsor)))     
  }
  
  ######################
  ### MANUALLY CODE SPEAKER
  #######################
  if(t_yrs %in% c("1999_2000", "2001_2002", "2003_2004", "2005_2006")){
    bills[bills$LES_sponsor %in% c("mr. speaker", 'representative mr. speaker', "speaker the", "speaker of the", "speaker of the house", "speaker newcomb", "speaker bruce newcomb", "representative bruce newcomb"),]$LES_sponsor <- "representative bruce newcomb"
  }else if(t_yrs %in% c("2007_2008")){
    bills$LES_sponsor <- gsub('lawerence denny|lawrence denney', 'lawerence denney', bills$LES_sponsor)
    bills[bills$LES_sponsor %in% c("mr. speaker", 'representative mr. speaker', "speaker the", "speaker of the", "speaker of the house", "speaker (denny|denney)", "speaker lawerence denney", "representative lawerence denney"),]$LES_sponsor <- "representative lawerence denney"
  }else if(t_yrs %in% c("2013_2014", "2015_2016", "2017_2018")){
    bills[bills$LES_sponsor %in% c("mr. speaker", 'representative mr. speaker', "speaker the", "speaker of the", "speaker of the house", "speaker bedke", "speaker scott bedke", "representative scott bedke"),]$LES_sponsor <- "representative scott bedke"
  }

  ############################
  ### Clean + Constrain LES SPONSOR Var
  bills$LES_sponsor <- gsub(',.+| and .+', '', bills$LES_sponsor)
  bills$LES_sponsor <- gsub('\\.$', '', bills$LES_sponsor)
  
  ### Add titles to missing -- !grepl for both because some are mismattched
  bills$LES_sponsor <- ifelse(substring(bills$bill_id, 1, 1) == "H" & !grepl("^repres|^senat|speaker", bills$LES_sponsor), paste0('representative ', bills$LES_sponsor), bills$LES_sponsor)
  bills$LES_sponsor <- ifelse(substring(bills$bill_id, 1, 1) == "S" & !grepl("^repres|^senat|speaker", bills$LES_sponsor), paste0('senator ', bills$LES_sponsor), bills$LES_sponsor)

  ### Reduce to Just Last Name (to account for varying name formats across records)
  bills$LES_sponsor_full <- bills$LES_sponsor
  bills$LES_sponsor <- paste0(gsub(' .+', '', bills$LES_sponsor), ' ', gsub('.+ ', '', bills$LES_sponsor))
  # table(bills$LES_sponsor)
  # select(bills, bill_id, session, author, LES_sponsor, LES_sponsor_full) %>% View()
  
  #### Add First Name for Those with Duplicate Last Names:
  # ** Need to use journals to code 2009+: https://legislature.idaho.gov/sessioninfo/2011/journals/
  if(t == 1999){
    bills[bills$LES_sponsor == "representative hansen" & (bills$LES_sponsor_full %in% "representative randy hansen" | grepl("Hansen\\(23\\)", bills$H_floor_sponsor)),]$LES_sponsor <- "representative randy hansen"
    bills[bills$LES_sponsor == "representative hansen" & (bills$LES_sponsor_full %in% "representative reed hansen" | grepl("Hansen\\(29\\)", bills$H_floor_sponsor)),]$LES_sponsor <- "representative m. reed hansen" # M. Reed Hansen
  }
  if(t %in% c(1999, 2001)){
    bills[bills$LES_sponsor == "representative field" & (bills$LES_sponsor_full %in% "representative debbie field" | grepl("Field\\(13\\)", bills$H_floor_sponsor)),]$LES_sponsor <- "representative debbie field"
    bills[bills$LES_sponsor == "representative field" & grepl("Field\\(20\\)", bills$H_floor_sponsor),]$LES_sponsor <- "representative frances field"
  }else if(t %in% c(2003, 2005)) { ### Both change districts
    bills[bills$LES_sponsor == "representative field" & (bills$LES_sponsor_full %in% "representative debbie field" | grepl("Field\\(18\\)", bills$H_floor_sponsor) | grepl("Field\\(18\\)", bills$requestor_SOP)),]$LES_sponsor <- "representative debbie field"
    bills[bills$LES_sponsor == "representative field" & grepl("Field\\(23\\)", bills$H_floor_sponsor),]$LES_sponsor <- "representative frances field"
    if(t == 2005){ # https://legislature.idaho.gov/wp-content/uploads/sessioninfo/2005/interim/trafficfinalreport.pdf
      bills[bills$LES_sponsor == "representative field" & bills$bill_id == "H0536",]$LES_sponsor <- "representative debbie field"
    }
  }
  if(t %in% c(2003, 2005, 2007, 2009, 2011) ){
    bills[bills$LES_sponsor == "representative smith" & (bills$LES_sponsor_full %in% c("representative leon smith", "representative leon e. smith") | grepl("Smith\\(24\\)", bills$H_floor_sponsor)),]$LES_sponsor <- "representative leon smith"
    bills[bills$LES_sponsor == "representative smith" & (bills$LES_sponsor_full %in% "representative elaine smith" | grepl("Smith\\(30\\)", bills$H_floor_sponsor)),]$LES_sponsor <- "representative elaine smith"
    if(t == 2009){
      bills[bills$LES_sponsor == "representative smith" & bills$bill_id %in% c("H0005", "H0106", "H0382", "H0386", "H0398", "H0472"),]$LES_sponsor <- "representative leon smith"
      bills[bills$LES_sponsor == "representative smith" & bills$bill_id %in% c("H0028"),]$LES_sponsor <- "representative elaine smith"  
    }else if(t == 2011){
      bills[bills$LES_sponsor == "representative smith" & bills$bill_id %in% c("H0008", "H0648", "H0653"),]$LES_sponsor <- "representative leon smith"
      bills[bills$LES_sponsor == "representative smith" & bills$bill_id %in% c("H0409", "H0491"),]$LES_sponsor <- "representative elaine smith"  
    }
  }
  if(t %in% c(2005, 2007)){
    bills[bills$LES_sponsor == "representative shepherd" & (bills$LES_sponsor_full %in% "representative mary lou shepherd" | grepl("Shepherd\\(2\\)", bills$H_floor_sponsor)),]$LES_sponsor <- "representative mary lou shepherd"
    bills[bills$LES_sponsor == "representative shepherd" & (bills$LES_sponsor_full %in% "representative paul shepherd" | grepl("Shepherd\\(8\\)", bills$H_floor_sponsor)),]$LES_sponsor <- "representative paul shepherd"
  }else if(t == 2009){ # Pg 178: https://legislature.idaho.gov/wp-content/uploads/sessioninfo/2009/journals/hfinal.pdf
    bills[bills$LES_sponsor == "representative shepherd" & bills$bill_id == "H0206",]$LES_sponsor <- "representative mary lou shepherd"
  }
  if(t %in% c(2007, 2009, 2011, 2013) ){
    bills[bills$LES_sponsor == "representative wood" & (bills$LES_sponsor_full %in% c("representative joan wood", "representative joan e. wood") | grepl("Wood\\(35\\)", bills$H_floor_sponsor)),]$LES_sponsor <- "representative joan wood"
    bills[bills$LES_sponsor == "representative wood" & (bills$LES_sponsor_full %in% "representative fred wood" | grepl("Wood\\(27\\)", bills$H_floor_sponsor)),]$LES_sponsor <- "representative fred wood"
    if (t == 2009){
      bills[bills$LES_sponsor %in% "representative wood" & bills$bill_id %in% c("H0267", "H0385", "H0415"),]$LES_sponsor <- "representative joan wood"
      bills[bills$LES_sponsor %in% "representative wood" & bills$bill_id %in% c("H0080", "H0123", "H0297", "H0299", "H0313", "H0315", "H0319", "H0320", "H0322", "H0330", "H0351", "H0470", "H0559", "H0642", "H0649", "H0701", "H0702", "H0715", "H0716", "H0717", "H0723"),]$LES_sponsor <- "representative fred wood" 
    }else if(t == 2011){
      bills[bills$LES_sponsor %in% "representative wood" & bills$bill_id %in% c("H0024", "H0135", "H0138", "H0232", "H0355", "H0398", "H0457"),]$LES_sponsor <- "representative joan wood"
      bills[bills$LES_sponsor %in% "representative wood" & bills$bill_id %in% c("H0023", "H0048", "H0054", "H0217", "H0324", "H0329", "H0338", "H0341", "H0503", "H0574", "H0615", "H0658", "H0682", "H0696"),]$LES_sponsor <- "representative fred wood"
    }else if(t == 2013){
      bills[bills$LES_sponsor %in% "representative wood" & bills$bill_id %in% c("H0019", "H0041", "H0290", "H0373", "H0389", "H0531"),]$LES_sponsor <- "representative joan wood"
      bills[bills$LES_sponsor %in% "representative wood" & bills$bill_id %in% c("H0188", "H0248", "H0398", "H0561"),]$LES_sponsor <- "representative fred wood"
    }
  }
  if(t %in% c(2013)){ # Neil = District 31; Eric = District 1
    bills[bills$LES_sponsor == "representative anderson" & (bills$LES_sponsor_full %in% "representative eric r. anderson"),]$LES_sponsor <- "representative eric r. anderson"
    #bills[bills$LES_sponsor == "representative anderson" & (bills$LES_sponsor_full %in% "representative zzzzzz anderson"),]$LES_sponsor <- "representative neil anderson"
    bills[bills$LES_sponsor %in% "representative anderson" & bills$bill_id %in% c("H0090"),]$LES_sponsor <- "representative eric r. anderson"
    bills[bills$LES_sponsor %in% "representative anderson" & bills$bill_id %in% c("H0003", "H0052", "H0377", "H0402"),]$LES_sponsor <- "representative neil anderson"
  }
  # filter(bills, grepl('representative smith$', LES_sponsor)) %>% distinct(LES_sponsor, LES_sponsor_full, H_floor_sponsor, bill_id, bill_url) %>% as.data.frame()
  
  ###################
  ###### Merge in S&S Bills
  ###################
  # *** For Idaho: Bills do not carryover + Nubmers restart for special sessions BUT only 1 special per year
  # ---> Capping Bill numbers at SESSION MAX and assuming bill is from SS with most bills proposed (so SS1, but keeping code flexible for future)
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
           session = ifelse(special == 0, paste0(year, '-RS'), ifelse(is.na(special_num), s_spec, paste0(year, '-SS')))) %>%
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
  
  ############################################################
  ############### Code Commemorative
  ############################################################
  bills <- commem_bills %>%
    filter(term == t_yrs) %>%
    select(bill_id, term, session, commem) %>%
    left_join(bills, ., by = c('bill_id', 'term', 'session'))
  # table(bills$commem)
  
  #### Not Commemorative if SS
  bills$commem <- ifelse(bills$SS == 1 & bills$commem == 1, 0, bills$commem)
  
  ############### Code Bill History
  bill_hist_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{t_sessions[1]}.csv")
  bill_hist <- read_csv(bill_hist_path, col_types = cols())
  bill_hist$session <- as.character(bill_hist$session)
  
  ## If multiple sessions, read in those as well
  if(length(t_sessions) > 1){
    for(s in t_sessions[2:length(t_sessions)]){
      bill_path <- glue("~/Dropbox/Data/State Legislative Data/States/{this_state}/{this_state}_Bill_Histories_{s}.csv")
      s_hist <- read_csv(bill_path, col_types = cols())
      s_hist$session <- as.character(s_hist$session)
      bill_hist <- bind_rows(bill_hist, s_hist)
    }
    rm(s, s_hist)
  }
  
  ### Clean Term/Session Variables
  bill_hist$term <- t_yrs
  bill_hist$session <- ifelse(grepl('spcl', bill_hist$session), gsub('spcl', '-SS', bill_hist$session), paste0(bill_hist$session, '-RS'))
  
  ######## Standardize BillHist Bill IDs
  bill_hist <- rename(bill_hist, bill_id = bill_number) %>%
    mutate(bill_id = gsub('[a-z]$', '', bill_id), 
           action = tolower(action))

  ### Rearrange + create order variable that covers both chambers
  bill_hist <- arrange(bill_hist, session, bill_id, order) 

  ### Coding Chamber Variable
  bill_hist$chamber <- ifelse(bill_hist$order == 1, substring(bill_hist$bill_id, 1, 1), NA)
  if(t <= 2008){
    bill_hist$chamber <- ifelse(is.na(bill_hist$chamber) & grepl("^house", bill_hist$action), "H", bill_hist$chamber)
    bill_hist$chamber <- ifelse(is.na(bill_hist$chamber) & grepl("^senate", bill_hist$action), "S", bill_hist$chamber)
    bill_hist$chamber <- ifelse(is.na(bill_hist$chamber) & grepl("^to enrol|^rpt enrol", bill_hist$action), substring(bill_hist$bill_id, 1, 1), bill_hist$chamber)
    bill_hist$chamber <- ifelse(is.na(bill_hist$chamber) & grepl("sp signed", bill_hist$action), "H", bill_hist$chamber)
    bill_hist$chamber <- ifelse(is.na(bill_hist$chamber) & grepl("pres signed", bill_hist$action), "S", bill_hist$chamber)
    bill_hist$chamber <- ifelse(is.na(bill_hist$chamber) & grepl("governor|session law|veto", bill_hist$action), "G", bill_hist$chamber)
    bill_hist$chamber <- ifelse(is.na(bill_hist$chamber) & grepl("to senate$", bill_hist$action), "H", bill_hist$chamber)
    bill_hist$chamber <- ifelse(is.na(bill_hist$chamber) & grepl("to house$", bill_hist$action), "S", bill_hist$chamber)
  }else{
    bill_hist$chamber <- ifelse(is.na(bill_hist$chamber) & grepl("^house|^(received|returned) from (the senate|senate)|^(received|returned) signed by the president", bill_hist$action), "H", bill_hist$chamber)
    bill_hist$chamber <- ifelse(is.na(bill_hist$chamber) & grepl("^senate|^(received|returned) from (the house|house)|^(received|returned) signed by the speaker", bill_hist$action), "S", bill_hist$chamber)
    #bill_hist$chamber <- ifelse(is.na(bill_hist$chamber) & grepl("^to enrol|^rpt enrol", bill_hist$action), substring(bill_hist$bill_id, 1, 1), bill_hist$chamber)
    bill_hist$chamber <- ifelse(is.na(bill_hist$chamber) & grepl("(delivered to|signed by|became law without) governor|session law|governor vetoed", bill_hist$action), "G", bill_hist$chamber)
    bill_hist$chamber <- ifelse(is.na(bill_hist$chamber) & grepl("to senate$", bill_hist$action), "H", bill_hist$chamber)
    bill_hist$chamber <- ifelse(is.na(bill_hist$chamber) & grepl("to house$", bill_hist$action), "S", bill_hist$chamber)
  }
  
  ### Fill Uncoded Chamber Variables In Order
  bill_hist <- bill_hist %>%
    group_by(term, session, bill_id) %>%
    fill(chamber) %>%
    ungroup()
  
  ### Standardize Chamber Variable
  bill_hist$chamber <- recode(bill_hist$chamber, "H" = "House", "S" = "Senate", "G" = "Governor")

  #### Load function to take a given bill history  and code legislative stages
  # **** Function requires an action column, a chamber column, and that the actions be in the correct order (so may need to reverse in some cases)
  # **** Can skip the chamber sorting with ignore_chamber = True -- Will take all actions from the initiating chamber
  # **** add_chamb = "name of chamber" will subset to intiating chamber and any other addition (e.g., "joint' or "executive")
  source('~/Dropbox/Papers/Legislative Effectiveness/State Legislatures/Estimate LES/code_billhist_fx.R')
  
  ##### Set State-Specific Terms for Identifying Each Stage
  ## Engrossment happens after amended by Floor/Committee of the WHole
  if(t <= 2008){
    aic_t <- c('rpt out', 'rec d/p')
    abc_t <- c('rpt out', '2nd rdg', '3rd rdg', 'to gen ord', 'engross')
    pc_t <- c('3rd rdg - passed', 'rls susp - passed') #, "sp signed", "pres signed")
    law_t <- c("governor signed", "session law")
  }else{
    aic_t <- c("reported out", "do pass", "do not pass", "without recommendation")
    abc_t <- c("reported out", "(second|2nd) reading", "(third|3rd) reading", "engross", "committee of the whole")
    pc_t <- c("read (third|three).+passed")
    law_t <- c("signed by governor", "session law", 'became law without governor')
  }

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
    #### Cross-Checking to See if Enrolled/Introduced In Opposite Chamber (Passed language sometimes in middle of row)
    if(t < 2009){
      if(any(grepl("to enrol|rpt enrol|sp signed|pres signed", hist_sub$action))){
        bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
      }else if(substring(b_id, 1, 1) == "H" & any(grepl('senate intro', hist_sub$action))){
        bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
      }else if(substring(b_id, 1, 1) == "S" & any(grepl('house intro', hist_sub$action))){
        bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
      }
    }else{
      if(any(grepl("signed by (the speaker|speaker)|signed by (the president|president)|(house|senate) concurred", hist_sub$action))){
        bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
      }else if(substring(b_id, 1, 1) == "H" & any(grepl('received from (the house|house)', hist_sub$action))){
        bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
      }else if(substring(b_id, 1, 1) == "S" & any(grepl('received from (the senate|senate)', hist_sub$action))){
        bill_stages$action_beyond_comm <- bill_stages$passed_chamber <- 1
      }
    }
    all_bill_stages <- bind_rows(all_bill_stages, bill_stages)
    # print(i)
  }
  options(warn = 1)
  
  ### Manuall Fix Missing Special Law (only 1..)
  if(t == 1999){
    all_bill_stages[all_bill_stages$session == "2000-SS" & all_bill_stages$bill_id == "H0001",]$law <- 1
  }
  
  ### Check Codings
  cat('\n')
  all_bill_stages %>% mutate(chamber = substring(bill_id, 1,1)) %>% group_by(session, chamber) %>% 
    summarize(N = n(), AIC = sum(action_in_comm), ABC = sum(action_beyond_comm), PASS = sum(passed_chamber), LAW = sum(law)) %>% as.data.frame() %>% print()
  # filter(bill_hist, session == '2013-RS' & bill_id %in% all_bill_stages[all_bill_stages$action_in_comm == 0 & all_bill_stages$action_beyond_comm == 1 & all_bill_stages$session == '2013-RS',]$bill_id) %>% View()
 
  ### MERGE
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
  
  ### Save Stage Info **** MERGE WITH COMMEM + SS
  if(!dir.exists(glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}'))){
    dir.create(glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}'))
  }
  write.csv(all_bill_stages, glue('~/Dropbox/Data/State Legislative Data/Bill_Stage_Codings/{this_state}/{this_state}_{t_yrs}_Bill_Stage_Codings.csv'), row.names = FALSE )
  
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
  
  ######## Cosponsorship Info --- For NV: 2011+ Cosponsorship info may be sporadic
  all_sponsors$num_cosponsored_bills <- NA
  # bills$cospon_match <- paste(bills$LES_sponsor, bills$primary_sponsors, bills$cosponsors, sep = '; ')
  # bills$cospon_match <- gsub('; NA', '', bills$cospon_match)
  # for(i in 1:nrow(all_sponsors)){
  #   c_sub <- filter(bills, substring(bill_id, 1, 1) == ifelse(all_sponsors[i,]$chamber == 'H', 'H', 'S')  )
  #   all_sponsors$num_cosponsored_bills[i] <- sum(grepl(all_sponsors[i,]$LES_sponsor, tolower(c_sub$cospon_match)))
  #   ### ONLY need to adjust this way if sponsored and cosponsored column are the same
  #   all_sponsors$num_cosponsored_bills[i] <- all_sponsors$num_cosponsored_bills[i] - all_sponsors$num_sponsored_bills[i]
  # }
  # bills <- select(bills, -cospon_match)
  # View(select(all_sponsors, LES_sponsor, num_sponsored_bills, num_cosponsored_bills)) 

  #######################
  #### CLEAN NAMES
  all_sponsors$last_name <- gsub('.+ ', '', all_sponsors$LES_sponsor)
  all_sponsors$first_name <- ifelse(str_count(all_sponsors$LES_sponsor, ' ') > 1, gsub('^(senator|representative) | [a-z]+$', '', all_sponsors$LES_sponsor), '')
  all_sponsors$first_name <- gsub(' .+|\\.', '', all_sponsors$first_name)
  all_sponsors <- arrange(all_sponsors, chamber, LES_sponsor)
  print(glue("-----> {nrow(all_sponsors)} UNIQUE SPONSORS IDENTIFIED IN BILL DATA "))
  
  #### Update First Names, Last Names for Matching 
  if(t_yrs %in% c("2007_2008", "2011_2012", "2013_2014", "2015_2016", "2017_2018")){ # Was not in office 2009-2010
    all_sponsors[all_sponsors$LES_sponsor %in% c("representative woude"),]$last_name <-  'vanderwoude'
  }
  if(t_yrs %in% c("2011_2012", "2013_2014", "2015_2016", "2017_2018")){
    all_sponsors[all_sponsors$LES_sponsor %in% c("representative buckner-webb", "senator buckner-webb"),]$last_name <-  'buckner'
  }
  
  if(t_yrs %in% c("2015_2016", "2017_2018") ){
    all_sponsors[all_sponsors$LES_sponsor %in% "senator hartog",]$last_name <-  'denhartog'
  }
  
  ### Subset Klarner State Leg. Election Data ---> Need to Account for Election Timing (Specials and Senate)
  H_elec_year <- as.numeric(substring(t_yrs, 1, 4)) - 1
  S_elec_year <- H_elec_year ### If staggered, H_elect_year - 2 to get T - 1 and T - 3
    
  klarner_H <- filter(klarner, sab == this_state & sen == 0 & (year %in% H_elec_year:(H_elec_year + house_term_length - 1) | (year == H_elec_year + house_term_length & etype %in% spec_elec_codes )))
  klarner_S <- filter(klarner, sab == this_state & sen == 1 & (year %in% S_elec_year:(S_elec_year + sen_term_length - 1) | (year == S_elec_year + sen_term_length & etype %in% spec_elec_codes )))
  klarner_sub <- suppressWarnings(bind_rows(klarner_H, klarner_S)); rm(klarner_H, klarner_S)
  
  ### Subset to Winners of Full Terms and Special Elections
  klarner_sub <- filter(klarner_sub, outcome == 'w' & (deter == 1 | etype %in% spec_elec_codes)) %>%
    select(year, sab, sfips, sen, ddez, dname, dno, etype, deter, cand, candid, party, partyz, middle) %>%
    distinct() ### Need to Subset to one result per candidate (later terms are recorded by precint...)
  
  ##################################################################
  ############## Match Sponsors Names to Klarner Data, Forgoing Function because mostly only have last name
  ##################################################################
  
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
      k_matches <- filter(klarner_sub, last_name == gsub("-|'| ||`", '', tolower(all_sponsors[i,]$last_name)) & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
    }
    
    ## Check Partial Names (e.g., maiden_name-last_name)
    if(nrow(k_matches) == 0 & grepl("-", all_sponsors[i,]$last_name)){
      k_matches <- filter(klarner_sub, last_name == gsub(".+-", '', tolower(all_sponsors[i,]$last_name))  & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
    }  
    
    ## Check GS 
    if(nrow(k_matches) == 0 & nrow(klarner_gs) >= 1){
      k_matches <- filter(klarner_gs, last_name == tolower(all_sponsors[i,]$last_name)  & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
      if(nrow(k_matches) == 0){
        k_matches <- filter(klarner_gs, last_name == gsub("-|'| ||`", '', tolower(all_sponsors[i,]$last_name)) & sen == ifelse(all_sponsors[i,]$chamber == "S", 1, 0))
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
      ### CHeck Middle If Too Man
      # if(length(m_sub) > 1){
      #   k_matches <- k_matches[m_sub,]
      #   m_sub <- which(substring(k_matches$middle, 1, 1) == all_sponsors[i,]$middle_name)
      # }
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
  if(t_yrs == "2009_2010"){
    ### Michelle Stennet acting senator for husband, Clint Stennett
    all_sponsors[all_sponsors$LES_sponsor == "senator stennett" & all_sponsors$chamber == "S", c("klarner_name", "klarner_id", "elec_year")] <- NA
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
  if(t_yrs == "1999_2000"){
    km <- filter(km, cand != 'kjellander, paul')
  }else if(t_yrs == '2001_2002'){
    km <- filter(km, cand != 'kempton, jim') 
  }else if(t_yrs == "2007_2008"){
    km <- filter(km, !(cand %in% c('deal, w. w. (bill)', 'sweet, gerry'))) 
    km <- filter(km, !(cand == "mckague, shirley" & sen == 0)) 
  }else if(t_yrs == "2009_2010"){
    km <- filter(km, cand != 'stennett, w. clinton (clint)') 
    km <- filter(km, cand != 'little, brad') 
  }else if(t_yrs == '2011_2012'){
    km <- filter(km, cand != 'geddes, robert l. jr.')
  }else if(t_yrs == '2013_2014'){
    km <- filter(km, cand != 'denney, lawerence')
  }
  
  #### Add Candidates who WON but Sponsored NO BILLS into Data
  if(nrow(km) > 0){
    cat(glue("-----> {as.numeric(substring(t_yrs, 1, 4)) - 1} KLARNER CANDIDATES MISSING MATCHES IN DATA ~~> ADDING INTO LIST OF LEGISLATORS: \n . "))
    print(select(km, year, sab, sen, ddez, etype, deter, cand, candid, partyz, match_name) %>% arrange(sen, cand) %>% as.data.frame())
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
  
  #########################################
  ####### Estimate Scores + Add in Relatd Variables
  #########################################
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
rm(k_matches, klarner_sub, km, bill_path, t_yrs, calc_LES) # 
rm(t, terms, klarner_gs, m_sub)
rm(pdf_data)


########################################################################################################################################################
########################################################################################################################################################
################### NAME FIX RECORDS 
########################################################################################################################################################
########################################################################################################################################################
# Vacancies filled by APPOINTMENT BY GOVERNOR --- https://ballotpedia.org/How_vacancies_are_filled_in_state_legislatures
########################################################################################################################
## Member Lists --- 1890 - 2016 (starts pg 218): https://sos.idaho.gov/blue_book/04_Legislative.pdf
###########
# filter(klarner, grepl("mcguirt", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(klarner, ddez == 29 & sen == 1 & outcome == 'w') %>% arrange(year, cand) %>% select(cand, year, sen, etype, outcome, ddez, candid)


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 1999_2000 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 97 UNCODED Committee Sponsored Bills (N = 1403)
#   session chamber   N AIC ABC PASS LAW
# 1 1999-RS       H 350 214 268  256 227
# 2 1999-RS       S 267 215 215  207 169
# 3 2000-RS       H 410 265 341  331 294
# 4 2000-RS       S 278 230 230  218 186
# 5 2000-SS       H   1   0   1    1   1
#### APPOINTED ~ HOUSE:
# -- CHEIRRETT (clair, ran and lost 2000)
# -- MOSS (thomas)
# -- PEARCE (monty)
# -- SHEPHERD (mary lou)
# -- SMYLIE (steve)
### APPOINTED ~ SENATE:
# -- WILLIAMS (j. stanley) -- note member list above is wrong, says he started in 2003, actually started 2000
### IN CHAMBER:
# -- barraclough, jack t.
# -- hadley, j. steven
# -- mortensen, max c.
# -- williams, j. stanley -- APPOINTED TO S in 2000
### DROP:
# -- kjellander, paul -- appointed to public utilities commission, jan 1999


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2001_2002 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 99 UNCODED Committee Sponsored Bills (N = 1267)
# session chamber   N AIC ABC PASS LAW
# 1 2001-RS       H 360 218 276  261 228
# 2 2001-RS       S 252 214 214  200 165
# 3 2002-RS       H 331 208 267  248 222
# 4 2002-RS       S 225 171 171  167 148
### APPOINTED ~ HOUSE:
# -- AIKELE (janet) -- didn't run again
# -- BEDKE (scott)
#### APPOINTED ~ SENATE:
# -- HILL (brent)
# -- LITTLE (brad)
# -- MARLEY (bert C., via H) --- ID document timing not quite right?
# -- SIMS (kathy) -- later served 2010+, which isn't in ID doc
#### IN CHAMBER:
# -- boe, donna h.
# -- callister, david 
# -- mortensen, max c.
# -- swan, george h. -- died march 2001
# -- branch, ric
# -- riggs, jack
### DROP:
# -- kempton, jim -- resigned upon commission appt, never seated


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2003_2004 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 134  UNCODED Committee Sponsored Bills (N = 1297)
# session chamber   N AIC ABC PASS LAW
# 1 2003-RS       H 402 237 312  286 253
# 2 2003-RS       S 188 149 149  146 128
# 3 2004-RS       H 350 208 277  263 237
# 4 2004-RS       S 223 181 181  169 152
#### APPOINTED ~ HOUSE:
# -- BAYER (cliff)
# -- PASLEY (anne pasley-stuart)
#### APPOINTED ~ SENATE:
# -- BILBAO (carlos) -- *** APPOINTED ACTING SENATOR, 2004, for Brad Little***** -- Pg. 2: https://legislature.idaho.gov/wp-content/uploads/sessioninfo/2004/journals/sfinal.pdf
#### IN CHAMBER
# -- barraclough, jack t.
# -- bradford, larry c.



# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2005_2006 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 104 UNCODED Committee Sponsored Bills (N = 1381)
# session chamber   N AIC ABC PASS LAW
# 1 2005-RS       H 370 220 294  281 250
# 2 2005-RS       S 219 172 172  167 155
# 3 2006-RS       H 440 256 337  323 295
# 4 2006-RS       S 246 192 192  184 164
# 5 2006-SS       H   1   0   1    1   1
# 6 2006-SS       S   1   0   0    0   0 # This is correct
### APPOINTED ~ HOUSE:
# -- BRACKETT (BERT)
### APPOINTED ~ SENATE:
# -- FULCHER (russell)
### IN CHAMBER
# -- anderson, eric

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2007_2008 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 90 UNCODED Committee Sponsored Bills (N = 1216)
# session chamber   N AIC ABC PASS LAW
# 1 2007-RS       H 313 184 252  245 215
# 2 2007-RS       S 226 179 179  173 154
# 3 2008-RS       H 337 196 273  259 231
# 4 2008-RS       S 250 201 201  195 179
### APPOINTED ~ HOUSE:
# -- HAGEDORN (MARY)
# -- KREN (steve)
# -- THOMAS (DIANA) -- 1 term, didn't run again, not in ID roster book
### APPOINTED ~ SENATE:
# -- MCKAGUE (shirley, via H)
# -- SAGNESS (richard) -- **** ACTING SENATOR for Edgar Malepeai 2008-2009 ********
### IN CHAMBER:
# -- bradford, larry c.
# -- edmunson, clete -- resigned aug 2007
### DROP:
# -- deal, w. w. (bill) -- resigned in january 2007 after appointment to agency job
# -- mckague, shirely IN HOUSE (appt to senate in january 2007)
# -- sweet, gerry -- resigned(?) -- seat filled by mckague


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2009_2010 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 92 UNCODED Committee Sponsored Bills (N = 1175)
#   session chamber   N AIC ABC PASS LAW
# 1 2009-RS       H 341 169 273  261 211
# 2 2009-RS       S 228 186 186  182 133
# 3 2010-RS       H 323 188 256  252 231
# 4 2010-RS       S 191 151 151  146 128
#### APPOINTED ~ SENATE:
# -- SAGNESS (richard) -- **** ACTING SENATOR for Edgar Malepeai 2008-2009 ********
# -- SMYSER (melinda)
# -- THORSON (jon) --  **** ACTING SENATOR for Clint Stennett in 2009 ********
# -- STENNETT (michelle) -- Name won't print! Last name duplicte... **** ACTING SENATOR for Clint Stennett in 2010 ********
#### IN CHAMBER:
# -- shepherd, paul e 
#### DROP:
# -- little, brad -- Appointed as Lt. Gov in Jan. 2009
# -- stennett, w. clinton (clint) -- On leave of absence from start of term until death in Oct 2010


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2011_2012 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 110 UNCODED Committee Sponsored Bills (N = 1119)
#   session chamber   N AIC ABC PASS LAW
# 1 2011-RS       H 318 196 253  238 203
# 2 2011-RS       S 187 154 154  149 132
# 3 2012-RS       H 321 199 252  242 211
# 4 2012-RS       S 183 152 152  147 131
### APPOINTED ~ HOUSE:
# -- BATT (gayle)
### APPOINTED ~ SENATE:
# -- JOHNSON (dan)
# -- TIPPETS (john, past H)
### IN CHAMBER:
# -- anderson, eric
# -- shepherd, paul e.
### DROP:
# -- geddes, robert l. jr. -- resigned January 2011 to become chair of Tax Commission


# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2013_2014 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 114 UNCODED Committee Sponsored Bills (N = 1087)
# session chamber   N AIC ABC PASS LAW
# 1 2013-RS       H 301 189 245  237 216
# 2 2013-RS       S 172 149 150  146 138
# 3 2014-RS       H 285 161 222  209 184
# 4 2014-RS       S 215 190 190  185 173
### APPOINTED ~ HOUSE:
# -- MCDONALD (patrick)
# -- RUBEL (ilana)
### APPOINTED ~ SENATE: 
# -- WARD-ENGELKING (janie, via H, Jan 2014)
#### IN CHAMBER:
# -- denney, lawerence -- final term, lost internal election for speaker, was running to be secretary of state



# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2015_2016 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 86 UNCODED Committee Sponsored Bills (N = 1081)
# session chamber   N AIC ABC PASS LAW
# 1 2015-RS       H 293 173 240  230 207
# 2 2015-RS       S 184 160 160  160 139
# 3 2015-SS       H   1   1   1    1   1
# 4 2016-RS       H 299 161 236  227 210
# 5 2016-RS       S 218 185 188  180 167
### APPOINTED ~ SENATE:
# -- ANTHON (kelly arthur)
# -- HARRIS (mark)
# -- JORDAN (maryanne)
### IN CHAMBER
# -- hartgen, stephen
# -- werk, elliot -- resigned Feb 17, 2015

# ~~~~~~~~~~~~ ESTIMATING SCORES FOR THE 2017_2018 TERM! ~~~~~~~~~~~~~~ 
# -----> Dropping 83 UNCODED Committee Sponsored Bills (N = 1101)
# session chamber   N AIC ABC PASS LAW
# 1 2017-RS       H 301 172 245  228 197
# 2 2017-RS       S 193 162 162  153 140
# 3 2018-RS       H 359 209 280  258 224
# 4 2018-RS       S 165 145 145  143 129
### APPPOINTED ~ HOUSE:
# -- EHARDT (barbara, D-33, R)
# -- TWAY (george, D-16B, D) -- *** ACTING REP. FOR KLOC ****
# -- WAGONER (jarom, D-10A, R)
### APPOINTED ~ SENATE
# -- POTTS (antony, l., D-33, R)
# -- SMITH (sarah) -- **** ACTING SENATOR FOR JIM RICE *** --- See p. 355: https://legislature.idaho.gov/wp-content/uploads/sessioninfo/2017/journals/sfinal.pdf
### IN CHAMBER
# -- collins, gary e.

# filter(klarner, grepl("buckner", cand)) %>% select(cand, year, sen, etype, outcome, ddez, candid)
# filter(klarner, ddez == 29 & sen == 1 & outcome == 'w') %>% arrange(year, cand) %>% select(cand, year, sen, etype, outcome, ddez, candid)


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
# LES[LES$data_name %in% c('brooks') & LES$term == "2017_2018",]$klarner_id <- NA
# LES[LES$data_name %in% c('brooks') & LES$term == "2017_2018",]$klarner_name <- NA
# LES[LES$data_name %in% c('brooks') & LES$term == "2017_2018",]$sponsor <- 'brooks, michael'

### ****Still missing*****
# still_missing <- unique(LES[is.na(LES$klarner_id),]$sponsor)
# name = still_missing[14]; print(name)
# filter(LES, grepl(paste0('^', name), sponsor)) %>% select(sponsor, data_name, klarner_name, klarner_id, term, chamber) %>% arrange(chamber, term) %>% as.data.frame()
# filter(klarner, grepl(name, cand)) %>% select(cand, year, etype, sen, outcome, candid, ddez) # %>%  View()
# rm(name, missing, still_missing)

### Fix Missing
name_matches <- data.frame(LES_name = 'shepherd', k_name = 'shepherd, mary lou') 
name_matches <- add_row(name_matches, LES_name = 'stennett', k_name = 'stennett, michelle')
name_matches <- add_row(name_matches, LES_name = 'ward-engelking', k_name = 'wardengelking, janie')
name_matches <- add_row(name_matches, LES_name = 'jordan', k_name = 'jordan, maryanne')
name_matches <- add_row(name_matches, LES_name = 'harris', k_name = 'harris, mark r.')
#name_matches <- add_row(name_matches, LES_name = 'zzzzzzz', k_name = 'zzzzzzzz')

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

###### Fix Party Error (Coded as "writein")
LES[LES$sponsor == "cheirrett, clair",]$party <- 'R'

###### IF MISSING, CHECK ELCTION LOSERS
# filter(LES, is.na(party)) %>% select(sponsor, term, klarner_id, district, party, exper) %>% as.data.frame()
# filter(klarner, candid == 246709) %>% select(cand, year, sen, etype, ddez, party, partyz, outcome)

### Not In Klarner
fill_missing <- data.frame(LES_name = "aikele", new_name = 'aikele, janet', party = 'r', district = 31, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "thomas", new_name = 'thomas, diana', party = 'r', district = 9, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "sagness", new_name = 'sagness, richard', party = 'd', district = 30, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "thorson", new_name = 'thorson, jon', party = 'd', district = 25, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "tway", new_name = 'tway, george', party = 'd', district = 16, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "potts", new_name = 'potts, antony l.', party = 'r', district = 33, exper = 'none')
fill_missing <- add_row(fill_missing, LES_name = "smith", new_name = 'smith, sarah', party = 'r', district = 10, exper = 'none')
# fill_missing <- add_row(fill_missing, LES_name = "zzzzzz", new_name = 'zzzzzzzz', party = 'zzzzz', district = zzzz, exper = 'zzzzzz')

for(i in 1:nrow(fill_missing)){
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$party <- fill_missing[i,]$party
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$district <- fill_missing[i,]$district
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$exper <- fill_missing[i,]$exper
  LES[LES$sponsor == fill_missing[i,]$LES_name & is.na(LES$klarner_id),]$sponsor <- fill_missing[i,]$new_name
}

#### *** 2017_2018: All below won reelection, so this won't be needed once klarner updates ****
LES[LES$sponsor == "wagoner" & LES$term == '2017_2018', c('party', 'sponsor', 'district')] <- list('r', 'wagoner, jarom', 10)
LES[LES$sponsor == "ehardt" & LES$term == '2017_2018', c('party', 'sponsor', 'district')] <- list('r', 'ehardt, barbara', 33)
# LES[LES$sponsor == "zzzzzzzz" & LES$term == '2017_2018', c('party', 'sponsor', 'district')] <- c('zzz', 'zzzzzz', zzzzz)

rm(klarner_sub, this_sponsor_LES, c, name, sponsor_rows, sponsor_sub, second_year, t)
rm(fill_missing, i)

#############################
#### NAME STANDARDIZATION
##############################
## ** Sponsor name variants that are the same person **
# filter(LES, !is.na(klarner_name)) %>% group_by(klarner_name) %>% filter(length(unique(sponsor)) > 1) %>% select(sponsor, data_name, klarner_name)
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name) %>% distinct()

# ***********************

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

# #### Doubling the Senate Rows + Adding back in
# # **** ALL data should be right (assuming still in chamber) EXCEPT Majority Member IN STAGGERED STATES ********
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

# ********* IDAHO IDEO DATA STARTS IN 1996 so FULL COVERAGE ***********
# ----- Handful of 2015 Observations are split over multiple rows (= two different names across years)

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

# LES[LES$sponsor %in% c('zzzzzzzzz'), c('SM_name', 'SM_party', 'np_score')] <- NA

### Matches In Which LES First Name != Shor-McCarty First Name
# Notable Names: F. Michael 'Mike' Burkett; Lucinda 'Cindy' Agidius; Milton Peter Niellsen
mutate(LES, first_name_LES = gsub(', ', '', str_extract(sponsor, ', [a-z]+')),first_name_SM = gsub(', ', '', str_extract(tolower(SM_name), ', [a-z]+'))) %>% 
  filter(first_name_LES != first_name_SM) %>% select(sponsor, SM_name) %>% distinct() %>% as.data.frame()

# LES[LES$sponsor %in% c('zzzzzzzzzz'), c('SM_name', 'SM_party', 'np_score')] <- NA

### MANUAL FIXES 
# filter(LES, is.na(np_score)) %>% filter(!(term %in% c('2017_2018')) ) %>% group_by(sponsor) %>% summarize(terms = paste0(term, collapse = "|")) %>% as.data.frame()
# filter(ideo, grepl('mcdon', tolower(name))) %>% as.data.frame()  %>% select(name, party, st, np_score)
# filter(ideo, !(ideo$name %in% LES$SM_name) ) %>% select(name, party, st, np_score)

name_matches <- data.frame(LES_name = 'geddes, robert c.', SM_name = 'Geddes, Robert C.') 
name_matches <- add_row(name_matches, LES_name = 'geddes, robert l. jr.', SM_name = 'Geddes, Robert L.')
### Gestrin == Split over two rows 2013-2016
name_matches <- add_row(name_matches, LES_name = 'gestrin, terry f.', SM_name = 'Gestrin, Terry F.')
#name_matches <- add_row(name_matches, LES_name = 'harris, mark r.', SM_name = 'zzzzzzz')
#name_matches <- add_row(name_matches, LES_name = 'sagness, richard', SM_name = 'zzzzzzz')
#name_matches <- add_row(name_matches, LES_name = 'thorson, jon', SM_name = 'zzzzzzz')
# name_matches <- add_row(name_matches, LES_name = 'zzzzz', SM_name = 'zzzzzzz')

for(i in 1:nrow(name_matches)){
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_name <- ideo[ideo$name == name_matches[i,]$SM_name,]$name
  LES[LES$sponsor == name_matches[i,]$LES_name,]$SM_party <- ideo[ideo$name == name_matches[i,]$SM_name,]$party
  LES[LES$sponsor == name_matches[i,]$LES_name,]$np_score <- ideo[ideo$name == name_matches[i,]$SM_name,]$np_score
}

###################
### Additional Matches

#### INCORRECTLY SPLIT ACROSS TWO RECORDS W/ IDENTICAL NAMES
LES[LES$sponsor == 'mcdonald, patrick',]$SM_name <-  ideo[ideo$name == 'McDonald, Patrick' & ideo$house2015 %in% 1,]$name
LES[LES$sponsor == 'mcdonald, patrick',]$SM_party <- ideo[ideo$name == 'McDonald, Patrick' & ideo$house2015 %in% 1,]$party
LES[LES$sponsor == 'mcdonald, patrick',]$np_score <- ideo[ideo$name == 'McDonald, Patrick' & ideo$house2015 %in% 1,]$np_score

LES[LES$sponsor == 'packer, kelley',]$SM_name <-  ideo[ideo$name == 'Packer, Kelley' & ideo$house2015 %in% 1,]$name
LES[LES$sponsor == 'packer, kelley',]$SM_party <- ideo[ideo$name == 'Packer, Kelley' & ideo$house2015 %in% 1,]$party
LES[LES$sponsor == 'packer, kelley',]$np_score <- ideo[ideo$name == 'Packer, Kelley' & ideo$house2015 %in% 1,]$np_score

#### INCORRECTLY CODED AS PARTY SWITCH == MATCHING TO DEM RECORDS
LES[LES$sponsor == 'whitworth, lin',]$SM_name <-  ideo[ideo$name == 'Whitworth, A. Lin' & ideo$party %in% "D",]$name
LES[LES$sponsor == 'whitworth, lin',]$SM_party <- ideo[ideo$name == 'Whitworth, A. Lin' & ideo$party %in% "D",]$party
LES[LES$sponsor == 'whitworth, lin',]$np_score <- ideo[ideo$name == 'Whitworth, A. Lin' & ideo$party %in% "D",]$np_score


########
### PARTY SWITCHERS --- Updating LES File where necessary as well
#########

# *** Zzzzzzzzzzz
# LES[LES$sponsor == 'ervin, mike' & LES$party == 'r',]$SM_name <-  ideo[ideo$name == 'Ervin, Mike' & ideo$party == 'R',]$name
# LES[LES$sponsor == 'ervin, mike' & LES$party == 'r',]$SM_party <- ideo[ideo$name == 'Ervin, Mike' & ideo$party == 'R',]$party
# LES[LES$sponsor == 'ervin, mike' & LES$party == 'r',]$np_score <- ideo[ideo$name == 'Ervin, Mike' & ideo$party == 'R',]$np_score
# LES[LES$sponsor == 'ervin, mike' & LES$party == 'd',]$SM_name <-  ideo[ideo$name == 'Ervin' & ideo$party == 'D',]$name
# LES[LES$sponsor == 'ervin, mike' & LES$party == 'd',]$SM_party <- ideo[ideo$name == 'Ervin' & ideo$party == 'D',]$party
# LES[LES$sponsor == 'ervin, mike' & LES$party == 'd',]$np_score <- ideo[ideo$name == 'Ervin' & ideo$party == 'D',]$np_score

rm(ideo, check_last, name_matches, d_name, i, lastname, num_with_same_last)

############################################
######## MORE NAME STANDARDIZATION + Data Fixes
###########################################

### Drop Numbers (from Klarner) at end of name
# filter(LES, grepl('[0-9]$', sponsor)) %>% distinct(sponsor)
# LES$sponsor <- gsub(" [0-9]$", "", LES$sponsor)

### Use SM Names/Manually Fill Double Abreviated Klarner Names
# filter(LES, grepl(', [a-z]\\. [a-z]\\.', sponsor))%>% select(sponsor, data_name, klarner_name, SM_name) %>% distinct()
LES[LES$sponsor == 'deal, w. w. (bill)',]$sponsor <- 'deal, william w.'
LES[LES$sponsor == 'taylor, w. o.',]$sponsor <- 'taylor, william o.' # William Orin Taylor - https://www.legacy.com/obituaries/idahostatesman/obituary.aspx?n=wo-taylor-bill&pid=181869328&fhid=7109
LES[LES$sponsor == 'thorne, j. l. (jerry)',]$sponsor <- 'thorne, jerrold l.'
LES[LES$sponsor == 'harwood, r. j. (dick)',]$sponsor <- 'harwood, richard j.'

##### Drop Nicknames
# filter(LES, grepl('[0-9]|\\(', sponsor))%>% select(sponsor, data_name, klarner_name, SM_name) %>% distinct()
LES$sponsor <- gsub(" \\([^\\)]+\\)", "", LES$sponsor)

#### Name Fixes
LES[LES$sponsor == 'buckner, cherie',]$sponsor <- 'buckner-webb, cherie'
LES[LES$sponsor == 'denhartog, lori',]$sponsor <- 'den hartog, lori'
LES[LES$sponsor == 'vanderwoude, john',]$sponsor <- 'vander woude, john'
LES[LES$sponsor == 'nielsen, peter',]$sponsor <- 'nielsen, milton peter'
LES[LES$sponsor == 'kingbarrutia, robbi lorene',]$sponsor <- 'king-barrutia, robbi lorene'
LES[LES$sponsor == 'pasleystuart, anne',]$sponsor <- 'pasley-stuart, anne'
LES[LES$sponsor == 'wardengelking, janie',]$sponsor <- 'ward-engelking, janie'


##############################################
########### HAND CODING MAJORITY MEMBER VARIABLE
###############################################
### CODE ALL AS 0, THEN CODE MAJORITY --> Assumes Indeps NOT in Majority
### Tie procedures: http://www.ncsl.org/research/about-state-legislatures/incaseofatie.aspx
###############################
# table(LES$term)

LES$in_majority <- 0

### House -- 1999 - 2020
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzzz) & LES$chamber == 'House' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1999:2020) & LES$chamber == 'House' & LES$party %in% 'r',]$in_majority <- 1

### Senate -- 1999 - 2020
# LES[as.numeric(substring(LES$term,1,4)) %in% c(zzzzzzz) & LES$chamber == 'Senate' & LES$party %in% 'd',]$in_majority <- 1
LES[as.numeric(substring(LES$term,1,4)) %in% c(1999:2020) & LES$chamber == 'Senate' & LES$party %in% 'r',]$in_majority <- 1


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

##### CHECK OUTLIERS ---> TONS of Dems with positive np_scores in Oklahoma...
# filter(LES, party == 'd' & np_score > 0) %>% select(sponsor, term, chamber, np_score, party, SM_party) %>% as.data.frame()
# filter(LES, party == 'r' & np_score < 0) %>% select(sponsor, term, chamber, np_score, party, SM_party)

### Within Legislator Correlations over time
stargazer::stargazer(lm(LES ~ lag(LES) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
stargazer::stargazer(lm(LES_rank ~ lag(LES_rank) + factor(sponsor) + factor(term) + factor(chamber), data = LES), omit = 'factor', type = 'text')
