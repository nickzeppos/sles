


################################################################
### CODE COMMEMORATIVE BILLS BY STATE
##############################################################
## NOTES:
## ** This file reads in data from each state, 1 by 1, and identifies commemorative bills **
###########
## TO DO:
# -- Need to add the e_terms in FOR ALL STATES
# -- Keep or get rid of special licence plates???
# -- Check for things akin to the sick leave banks for MA (or other things like 'private relief' bills... contracts/legal matters?)
###########
###########
## --> LES WORKFLOW:
## -- (1) Gather S&S 
## -- (2) Estimate Commems
## -- (3) Run Estimate LES Script
## ------ Merge S&S and Commems into Data
## ------ Code + Save Bill Stages with S&S and Commem Info
## ------ Estimate LES
## ------ Save Clean Bill Data with Bill Stage Info Merged 
## ------ Merge to external data + save
########################
### Logic/Criteria/Goals:
## -- Minimize the False Positive (e.g., False Commem ID) rate ---> Prefer to code Commems as Substantive than Substantive as Commem --> Conservative in coding
## -- If a bill appropriates money, it is NOT commemorative!
## -- Bills that allocate authority or create formal bodies also NOT commemorative
## --> Major categories here seem to be: commemorative license plates, bridge renamings, state symbols, and holidays/awareness/celebration days
####################

# ??????????????? ----------> Include Joint Resolutions? Often approved by gov (or in congress, the president...)

rm(list=ls())
options(stringsAsFactors = FALSE, scipen = 999)

library(tidyr)
library(dplyr)
library(stringr)
library(purrr)
library(glue)
library(readr)

#### Sessions you want to extract:

#### Output Directory
comm_path = dirname(rstudioapi::getActiveDocumentContext()$path)
setwd(comm_path)

#### State Data Folders
state_folders <- list.files(gsub("Commem Bills","States",comm_path), full.names = TRUE)

this_year <- as.integer(format(Sys.time(),"%Y"))

#### COMMEMORATIVE BASE TERMS - What Alan and Craig used for Congress --> Adapt by state

## ******  BASE TERMS FOR TITLES, SHORT TITLES, SUMMARIES ONLY ******
## --> KEYWORDS adapted state by state

## Dropped: 
# -- "award" -- often used w/ contracts
# -- "mint coin" --> Irrelevant
# -- "recogni", "encourag", 'designa', "redesignat", "promoting" --> Frequent errors
## Added:
# -- See additions below
# -- Case-Sensitive Search: "[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b"
# -----> \\b = word break, so space, period, end of line,etc 
# -----> Switched to \\b for MA -- wasn't working for some reason, validate it still works elsewhere
## Restricted:
# -- honor, commend
## Excluded: 
# -- See exclude below

vw_commem <- c("expressing support", "urging", "condol", "commemorat", " honor|^honor", "memoria", 
               "congratul", "public holiday", "for the relief of", "for the private relief of", "retention of the name", 
               "medal", "posthumous", "provide for correction", "to name", 'rename', "to remove any doubt")
additions <- c("anniversary", "raise awareness", 'awareness (day|week|month)', "dedicating", "celebrat", 'appreciat',
               " commend|^commend", 'official design', 'official emblem', 'remembrance', 'state symbol', 'proclamation')
exclude <- c('appropriates', 'appropriation', 'approp\\.', 'appropriating', 'to appropriate', '\\$', 'dollars', 'to fund', 'funding', 'funds',
             'expenditure', 'penalt', 'felony', 'memorial (act|law)', 'criminal', 'lien', 'statutory', 'license fee', 
             '^tax| tax', 'prohibit', 'rainy day', 'procedure', 'contract', 'firearm', 'weapon', 'inflation',
             'exempt', 'legislative intent', 'deposit', 'budget', 'tuition', 'violation', 'compensation', 'promulgate',
             'regulation', 'bonds', 'jurisdiction', 'liabilit', 'task force', 'annuity', 'probate', 'financ',
             'honor[a-z]+ discharge', 'revenue', 'compliance', 'sale of', 'health benefit', 'insurer', 'primary care', 
             'grant program', 'purchase', 'donation', 'official language', 'refund', 'election', 'capital improvements',
             'liquor sales')

#### Export Terms
# term_df <- tibble(`Terms from Volden and Wiseman (2014)` = paste0(vw_commem, collapse = "; "),
#                   `Additional Terms` = paste0(additions, collapse = "; "),
#                   `Excluded Terms` = paste0(exclude, collapse = "; "))
# term_doc <- officer::read_docx()
# term_doc <- t(term_df) %>% huxtable::huxtable(add_rownames = TRUE) %>% huxtable::as_flextable() %>% flextable::body_add_flextable(term_doc, .)
# print(term_doc, target = "~/Dropbox/Papers/Legislative Effectiveness/LES_in_States/Tables/Commemorative Coding Terms.docx")
# rm(term_doc)

############ QUESTIONS
### Drop scholarship loans?
### Drop Regulate/Oversight -- E.g., create honorary board and delegate oversight
### Renamings of departments??? (e.g., alabama)
### --> Honorable discharged vets --> tones of honor matches, not necessarily commems..
### Keep technical or non-commemorative name changes? E.g. changing name of a commission or minor shcool name change... ? See north carolina 'name change' and 'change name'
### - Virginia - HB0128 1994: Relief bills?
# ---> "Bradley A. Trussell.  Provides relief for Bradley A. Trussell, a terminally ill state employee who would like to irrevocably assign his life insurance benefits to a company, the selection of which is subject to the approval of VRS, to receive funds for financial needs during his lifetime. The bill contains an emergency clause."
# ---> About 40 total (plus other phrasings (titles usually relief of PERSON)): filter(all_bills, grepl('provides relief (for|in.+to)', tolower(summary)))



##########################################################
######## Code Commemoratives State By State
##########################################################

#############
### ALASKA ####
#############

### Read ALL Bills
this_state <- "AK"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  mutate(term = gsub('\\(|\\)', '', gsub('-', '_', str_extract(session, '\\(.+\\)'))),
         session = gsub(' Legislature.+', '', session),
         bill_id = paste0(str_trim(gsub('[0-9].+|[0-9]+', '', bill_id)), str_pad(str_trim(gsub('[A-Z]+', '', bill_id)), 4, pad = "0"))) %>%
  select(bill_id, term, session, short_title, title, keywords) %>%
  filter(grepl("^HB|^SB", bill_id))
# filter(all_bills, nchar(bill_id) != 6)

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, c('an act naming', 'establishing.+state park')) ## These are for resolutions -- , '^supporting', '^encouraging','^designating', '^expressing', '^requesting', '^opposing', '^recogniz'

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(short_title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(short_title, title)), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(exclude, collapse = "|"), tolower(short_title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(exclude, collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl("proclamations|holidays|place names", tolower(keywords)), 1, commem))

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()

# Manual fixes
all_bills[grepl('relating to designation of state water', all_bills$title), ]$commem <- 0
all_bills[grepl('declaring Election Day a holiday', all_bills$title), ]$commem <- 0
all_bills[grepl('relating to the transfer of vehicles', all_bills$title), ]$commem <- 0
all_bills[all_bills$term == '2023_2024' & !is.na(all_bills$title) &
            all_bills$title == 'An Act establishing Juneteenth Day as a legal holiday', ]$commem <- 0
all_bills[grepl('and relating to the Aviation Advisory Board', all_bills$title), ]$commem <- 0
all_bills[all_bills$term == '2023_2024' & !is.na(all_bills$title) & 
            grepl('special registration plates', all_bills$title) & grepl('fees', all_bills$keywords), ]$commem <- 0
all_bills[grepl('Honor and Remember Flag', all_bills$title), ]$commem <- 0

for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}

### Save
# all_bills <- select(all_bills, bill_id, term, session, commmem)

rm(this_state, state_files, c_terms, all_bills, t)

#############
### ALABAMA
#############
# *** NOTE: A few column errors but does not hinder coding ***

### Read ALL Bills
this_state <- "AL"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Clean
terms <- seq(1999, lubridate::year(Sys.Date()) - 1, 4)
term_df <- data.frame(year = 1999:(max(terms) + 3), term = rep(paste(terms, terms + 3, sep = '_'), each = 4) )

all_bills <- all_bills %>%
  mutate(year = as.integer(str_extract(session, '\\d{4}')),
         session = gsub(' \\d{4}$', '', session),
         session = recode(session, 'Regular Session' = 'RS', 'First Special Session' = 'SS1', 'Second Special Session' = 'SS2', 'Third Special Session' = 'SS3', 'Fourth Special Session' = 'SS4', 'Organizational Session' = 'OS'),
         session = paste(year, session, sep = '-'),
         bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('^[A-Z]+', '', bill_id), 4, pad = "0"))) %>%
  left_join(term_df, by = 'year') %>%
  select(bill_id, term, session, short_title, subject) %>%
  filter(grepl('^HB|^SB', bill_id))
# filter(all_bills, nchar(bill_id) > 8 | nchar(bill_id) < 6)

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions)

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(short_title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", short_title), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(c(exclude, 'instructional days', 'deface', 'authorize', 'memorial board') , collapse = "|"), tolower(short_title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl("state symbol|congratulatory|condolence|designation", tolower(subject)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("appropriation|administrative|insurance|abortion|crime|safety|sexual abuse|firearm|adoption", tolower(subject)), 0, commem))
## If resolutions, include "resolution.+miscellaneous" in subject coding

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()

### Save
# all_bills <- select(all_bills, bill_id, term, session, commmem)
for(t in c("2019_2022")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, all_bills, terms, term_df)

#############
### ARKANSAS
#############

### Read ALL Bills
this_state <- "AR"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows

### Fix Session Errors
all_bills[all_bills$ga_num == "86th General Assembly" & all_bills$session == "Regular Session",]$session <- "Regular Session, 2007"
all_bills[all_bills$ga_num == "86th General Assembly" & all_bills$session == 'First Extraordinary Session of 86th General Assembly',]$session <- "First Extraordinary Session, 2008"
all_bills[all_bills$ga_num == "87th General Assembly" & all_bills$session == "Regular Session",]$session <- "Regular Session, 2009"
all_bills[all_bills$ga_num == "87th General Assembly" & all_bills$session == "Fiscal Session",]$session <- "Fiscal Session, 2010"
all_bills[all_bills$ga_num == "88th General Assembly" & all_bills$session == "Regular Session",]$session <- "Regular Session, 2011"
all_bills[all_bills$ga_num == "88th General Assembly" & all_bills$session == "Fiscal Session",]$session <- "Fiscal Session, 2012"
all_bills[all_bills$ga_num == "89th General Assembly" & all_bills$session == "Regular Session",]$session <- "Regular Session, 2013"

### Clean
all_bills <- all_bills %>%
  rename(bill_id = bill_num) %>%
  mutate(ga_num = gsub(' .+', '', ga_num),
         term = (as.numeric(substring(ga_num, 1, 2)) - 85)*2 + 2005,
         term = paste0(term, "_", term + 1),
         session_year = gsub('.+, ', '', session),
         session = gsub(', [0-9]+', '', session),
         session = recode(session, 'Regular Session' = 'RS', 
                          'First Extraordinary Session' = 'SS1',
                          'Second Extraordinary Session' = 'SS2',
                          'Third Extraordinary Session' = 'SS3',
                          'Fourth Extraordinary Session' = 'SS4',
                          'Fiscal Session' = 'FS'),
         session = paste(session_year, session, sep = "-")
  ) %>%
  select(bill_id, term, session, title) %>%
  filter(grepl('^HB|^SB', bill_id))

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, "to proclaim", 'scenic highway', 'designat.+highway', "designat.+official", 'recognition day', 'to establish.+day', 'to declare.+day')
#, c('an act naming', 'establishing.+state park', 'proclomation', '^supporting', '^encouraging','^designating', '^expressing', '^requesting', '^opposing', '^recogniz'))

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c(exclude, 'retirement pension', 'stadium commission') , collapse = "|"), tolower(title)), 0, commem))
table(all_bills$term, all_bills$commem)

### Manual Fixes
all_bills[all_bills$title %in% 'TO RECOGNIZE TWIST, ARKANSAS AND A FAMOUS GUITAR NAMED "LUCILLE".',]$commem <- 1
all_bills[all_bills$title %in% 'TO AUTHORIZE LOCAL OFFICIALS TO PROVIDE FOR THE OPERATION OF ALL-TERRAIN VEHICLES ON DESIGNATED PUBLIC STREETS AND HIGHWAYS.', ]$commem <- 0
all_bills[grepl("AS A MANDATED REPORTER", all_bills$title), ]$commem <- 0

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, all_bills)


#############
### ARIZONA
#############
# ******** DATA SUMMARIES ARE BAD --> This will be noisy *******
# --> Potentially allowing some error with "^congress;" but likely demands of congress (^federal gov, on the other hand, seems highly prone to error)

### Read ALL Bills
this_state <- "AZ"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  mutate(term = gsub('_[A-Z].+', '', session),
         session = gsub('^[0-9]+_[0-9]+_', '', session),
         session = recode(session, 'First_Regular' = 'RS1', 'Second_Regular' = 'RS2', 'First_Special' = 'SS1', 'Second_Special' = 'SS2', 'Third_Special' = 'SS3', 'Fourth_Special' = 'SS4', 'Fifth_Special' = 'SS5', 'Sixth_Special' = 'SS6', 'Seventh_Special' = 'SS7', 'Eighth_Special' = 'SS8', 'Ninth_Special' = 'SS9', 'Tenth_Special' = 'SS10')) %>%
  rename(bill_id = bill_number) %>%
  select(bill_id, term, session, title, keywords, summary) %>%
  filter(grepl("^HB|^SB", bill_id)) %>%
  mutate(summary = gsub('[A-Z]+[0-9]+ - ', '', summary))

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, c('^support [a-z]', '^honor')) # For Res: "^oppos", '^recogniz',

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", summary), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(c(exclude), collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(c(exclude), collapse = "|"), tolower(summary)), 0, commem)) %>%  
  mutate(commem = ifelse(grepl("memorial|death resolution|holiday|honoring|monument", tolower(keywords)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("appropriat|alcohol|child support|bonds", tolower(keywords)), 0, commem))
## For Res: ^urging congress|^congress;|

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, all_bills)


#############
### CALIFORNIA
#############

### Read ALL Bills
this_state <- "CA"
state_files_db <- list.files('../States/CA/database_files/parsed_db_files', pattern = "Details", full.names = TRUE)
#state_files_old <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
#state_files_old <- state_files_old[grepl('Bill_Details', state_files_old) & !(gsub(".+/CA/", '', state_files_old) %in% gsub(".+db_files/", '', state_files_db) )]
#old_bills <- state_files_old %>% lapply(read_csv, col_types = cols()) %>% bind_rows 
db_bills <- state_files_db %>% lapply(read_csv, col_types = cols()) %>% bind_rows %>% mutate(bill_id = bill_num)

### Fix Terms in Old BIlls
# term_parts <- str_split_fixed(old_bills$term, '-', 2)
# term_parts[,1] <- ifelse(grepl('^9', term_parts[,1]), paste0('19', term_parts[,1]), paste0('20', term_parts[,1]))
# term_parts[,2] <- ifelse(grepl('^9', term_parts[,2]), paste0('19', term_parts[,2]), paste0('20', term_parts[,2]))
# old_bills$term <- paste(term_parts[,1], term_parts[,2], sep = "_")

### Merge + Clean
# all_bills <- select(old_bills, bill_id, term, title, topic) %>%
#   mutate(bill_id = toupper(bill_id),
#          title = gsub('TITLE: |TITLE:$', '', title),
#          topic = gsub('TOPIC: |TOPIC:$', '', topic)) %>%
#   bind_rows(., select(db_bills, bill_id, term, title, topic)) %>%
#   mutate(session = ifelse(grepl("X", bill_id), paste0("SS", gsub('[A-Za-z]+|\\-.+|\\_.+', '', bill_id)), "RS")) %>% 
#   filter(grepl("^AB|^SB", bill_id))
all_bills <- select(db_bills, bill_id, term, title, topic) %>%
  mutate(session = ifelse(grepl("X", bill_id), paste0("SS", gsub('[A-Za-z]+|\\-.+|\\_.+', '', bill_id)), "RS")) %>% 
  filter(grepl("^AB|^SB", bill_id))

id_parts <- str_split_fixed(all_bills$bill_id, "\\_|-", 2)
all_bills$bill_id <- ifelse(grepl('X', id_parts[,1]), 
                            paste0(id_parts[,1], "-", str_pad(id_parts[,2], 4, pad = "0")), 
                            paste0(id_parts[,1], str_pad(id_parts[,2], 4, pad = "0")))
#rm(old_bills, db_bills)
rm(db_bills)

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions) 
c_terms <- c_terms[!grepl("to name", c_terms)]

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", title), 1, commem)) %>%
  mutate(commem = ifelse(grepl('holiday|memorial|awareness|celebrat|day\\.|week\\.$|month\\.$', tolower(topic)), 1, commem)) %>%
  mutate(commem = ifelse(grepl('instructional day|tax|school week|schoolday|workweek', tolower(topic)), 0, commem)) %>%
  mutate(commem = ifelse(term == "2023_2024" & 
                           (grepl('capitol|appropriation|pollution', tolower(title)) | 
                              grepl('elections|colleges|civil law|campaign|districts', tolower(topic))), 0, commem))

table(all_bills$term, all_bills$commem)
# filter(all_bills, grepl('designat.+', tolower(title))) %>% View()
# filter(all_bills, commem == 1) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files_db, c_terms, all_bills, id_parts, t)


#############
### COLORADO
#############

# ************* Really limited info to do this for 2015 and earlier - JUST the bill title ************
# ---- Might be able to scrape longer summaries for some of these years, but will be super error prone
# ---- The good news is that we're not really getting many more commes in the later years with full info

### Read ALL Bills
this_state <- "CO"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  rename(bill_id = bill_number) %>%
  mutate(term = ifelse(session %% 2 == 0, paste(session - 1, session, sep = '_'), paste(session, session + 1, sep = "_")),
         session = paste(session, session_type, sep = '-'),
         bill_id = ifelse(grepl('-[0-9][0-9][0-9]$', bill_id), paste(gsub('-.+', '', bill_id), str_pad(str_trim(gsub('.+-|\n', '', bill_id)), 4, pad = '0'), sep = '-'), bill_id)
  ) %>%
  select(bill_id, term, session, title, keywords, long_title, summary) %>%
  filter(grepl("^HB|^SB", bill_id)) 

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'state holiday', 'as the state', 'renaming', 'special license plate') #, '^designat', '^proclaiming'
e_terms <- c(exclude, 'treatment centers', '^grants', 'pay day', 'burial', '^purging', 'precinct',
             'honor terms', 'same day voter', 'seven day', 'commission', 'tenancies', 'division of labor', '^reduce')

### Limited Info for 1998--2015
# --- All words capitalized ---> [A-Z][a-z]+ Day\\b will match anything with day, etc.
pre_2016 <- all_bills %>%
  filter(as.numeric(substring(session, 1, 4)) <= 2015) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem))
pre_2016[grepl('yule marble|ch.vez day', tolower(pre_2016$title)),]$commem <- 1

table(pre_2016$term, pre_2016$commem)
# filter(pre_2016, commem == 1) %>% View()
# filter(pre_2016, commem == 1 & grepl('week', tolower(title))) %>% select(bill_id, title) %>% as.data.frame() %>% View()
# filter(pre_2016, grepl('renaming', tolower(title))) %>% select(bill_id, title, commem) %>% as.data.frame() %>% View()

post_2015 <- all_bills %>%
  filter(as.numeric(substring(session, 1, 4)) > 2015) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(long_title)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title, summary)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(long_title)), 0, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>%
  # Make modifications for 2023_2024 term
  mutate(commem = ifelse(term == "2023_2024" & 
                           grepl("pay period|requires|defines.+for licensing purposes", tolower(summary)), 0, commem))

table(post_2015$term, post_2015$commem)
# filter(post_2015, commem == 1) %>% View()
# filter(post_2015, commem == 1 & grepl('week', tolower(title))) %>% select(bill_id, title) %>% as.data.frame() %>% View()
# filter(post_2015, grepl('special license', tolower(title))) %>% select(bill_id, title, long_title, summary, commem) %>% as.data.frame() %>% View()
# filter(all_bills, grepl('seven day', tolower(title))) %>% View()

all_bills <- bind_rows(pre_2016, post_2015)

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills, pre_2016, post_2015, t)


#############
### CONNECTICUT
#############

### Read ALL Bills
this_state <- "CT"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  distinct() %>%
  rename(bill_id = bill_num) %>%
  mutate(term = ifelse(session %% 2 == 0, paste0(session - 1, "_", session), paste0(session, "_", session + 1)),
         bill_id = gsub('-|\\.HTM', '', bill_id) ) %>%
  filter(grepl("^HB|^SB", toupper(gsub('[0-9].+|[0-9]+', '', bill_id)))) %>% 
  select(bill_id, term, session, title, purpose)

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'designat.+(day|week|month|year)', 'designat.+the (official|state)', '^to designate (the|an|a) .+\\"[^\\"]+\\"',
             'to designat an official', 'to designate a (portion|state|road|footbridge|highway|lake)', 'state (song|fish)\\b')
#c_terms <- c_terms[!grepl("to name", c_terms)]
e_terms = c(exclude, "^to add", "^to expand", "^to revise", "radioactive waste", 'high stake', '^to allow (the town|the state|payment)', 
            "^to eliminate", "^to convert", "^to implement", "^to prevent", "^to double fines")

all_bills <- all_bills %>%
  #filter(bill_id == "SB194" & session == 1995) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(purpose)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", title), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", purpose), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(purpose)), 0, commem))

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()

### Manual Fixes
all_bills[grepl("official state language", tolower(all_bills$title)),]$commem <- 0
all_bills[all_bills$purpose %in% 'To allow the Wallingford Community Day Care Center to serve more children.',]$commem <- 0
all_bills[grepl("^to increase parental", tolower(all_bills$purpose)),]$commem <- 0
all_bills[grepl("^to honor.+veterans", tolower(all_bills$purpose)),]$commem <- 1
all_bills[grepl("provide loan guar", tolower(all_bills$purpose)),]$commem <- 0
all_bills[grepl("to improve safety for firefighters", tolower(all_bills$purpose)),]$commem <- 0
all_bills[grepl("parking fees", tolower(all_bills$title)),]$commem <- 0
all_bills[grepl("require the commissioner", tolower(all_bills$purpose)),]$commem <- 0
all_bills[grepl("erect signs informing motorists", tolower(all_bills$purpose)),]$commem <- 0
all_bills[grepl("establish a program", tolower(all_bills$purpose)),]$commem <- 0
all_bills[grepl("grant authority", tolower(all_bills$purpose)),]$commem <- 0
all_bills[grepl("state holiday\\.$", tolower(all_bills$purpose)),]$commem <- 0
all_bills[grepl("operating hours", tolower(all_bills$title)),]$commem <- 0
all_bills[grepl("mortgage loan", tolower(all_bills$purpose)),]$commem <- 0
all_bills[grepl("reciprocal fishing agreements", tolower(all_bills$title)),]$commem <- 0
all_bills[grepl("requiring the state", tolower(all_bills$title)),]$commem <- 0
all_bills[all_bills$title %in% 'AN ACT CONCERNING BUSINESS REGISTRATIONS WITH THE OFFICE OF THE SECRETARY OF THE STATE.',]$commem <- 0
all_bills[grepl("require certain disclosures", tolower(all_bills$purpose)),]$commem <- 0
all_bills[grepl("submit a report", tolower(all_bills$purpose)),]$commem <- 0
all_bills[grepl("change the composition", tolower(all_bills$purpose)),]$commem <- 0
all_bills[grepl("concerning the fee", tolower(all_bills$title)),]$commem <- 0


### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills, t)


#############
### DELAWARE
#############
# *** Note: DE has multiple records for Substites (HS0123 and SS0123) --> Dropping these
# ---> Shouldn't affect downstream merge because of how I adjust those in main estimation script

### Read ALL Bills
this_state <- "DE"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  distinct() %>%
  rename(ga_num = session_num,
         bill_id = bill_number) %>%
  mutate(term = gsub(' - ', '_', gsub('.+\\(|\\)', '', session)),
         term = ifelse(term == "Present", paste0(this_year - 2, "_", this_year - 1), term),
         session = term,
         #parent_bill = ifelse(parent_bill == '', '', paste0(gsub(' [0-9].+| [0-9]+', '', parent_bill), str_pad(gsub('.+ ', '', parent_bill), 4, pad = '0'))),
         bill_id = paste0(gsub(' [0-9].+| [0-9]+', '', bill_id), str_pad(gsub('.+ ', '', bill_id), 4, pad = '0'))
  ) %>%
  filter(!(term %in% c("1999_2000", "2001_2002"))) %>%
  filter(grepl("^HB|^SB", bill_id)) %>% 
  select(bill_id, term, session, title, summary)

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'designat.+as the official', 'special license plate') 
#c_terms <- c_terms[!grepl("to name", c_terms)]
e_terms = c(exclude, 'directives', "commercial.+license", 'cremation', 'advisory (council|committee)', 'merit system', 'this act corrects')


all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", title), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem))

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()

### Manual Fixes
all_bills[all_bills$title %in% 'AN ACT TO RENAME THE MAIN ADMINISTRATION BUILDING OF THE DELAWARE DEPARTMENT OF AGRICULTURE IN HONOR OF FORMER DELAWARE STATE REPRESENTATIVE, STATE SENATOR AND SECRETARY OF FINANCE RICHARD S. CORDREY.',]$commem <- 1
all_bills[all_bills$summary %in% 'This Bill designates English as the official state language.',]$commem <- 0
all_bills[grepl("Under current law, 200 applications|Medical Orders for Scope of Treatment", all_bills$summary),]$commem <- 0

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills, t)

#############
### FLORIDA
#############

### Read ALL Bills
this_state <- "FL"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]

all_bills <- read_csv(state_files[1], col_types = cols()) %>% mutate(session = as.character(session))
for(sf in state_files[2:length(state_files)]){
  sf_bills <- read_csv(sf, col_types = cols()) %>% mutate(session = as.character(session))
  all_bills <- bind_rows(all_bills, sf_bills)
}; rm(sf_bills, sf)

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions) ## Res: , c("^urges")
c_terms <- c_terms[!grepl("rename|to name|awareness", c_terms)]
c_terms <- c(c_terms, "^rename")
e_terms <- c(exclude, '^revises', 'insurance', ' fee | fees ', 'child health', 'marital', 'adoption')

terms = c("2023_2024")

for(term2 in terms){
  cat("State: ", this_state, "\n Term: ",term2,"\n")
  start_year = as.integer(substr(term2,1,4))
  sub_bills <- all_bills %>%
    mutate(year = as.integer(substring(session, 1, 4)),
           term = ifelse(year %% 2 == 0, paste(year - 1, year, sep = "_"), paste(year, year + 1, sep = '_')),
           # Organizational sessions are suffixed "O" but only involve resolutions, not bills
           session_type = recode(gsub('^[0-9]+', '', session), 'A' = 'SS-A', 'B' = 'SS-B', 'C' = 'SS-C', 'D' = 'SS-D', 'E' = 'SS-E', 'F' = 'SS-F'),
           session_type = ifelse(session_type == '', 'RS', session_type),
           session = paste(year, session_type, sep = '-'),
           ### Need to Adjust For Pre A-Sessions
           # ** Occur at T-1: 2010A == 2011_2012; 2004A = 2005_2006; 2000A = 2001_2002
           term = case_when(session == '2000-SS-A' ~ '2001_2002',
                            session == '2004-SS-A' ~ '2005_2006',
                            session == '2010-SS-A' ~ '2011_2012',
                            session == '2022-SS-A' ~ '2023_2024',
                            .default = term)
    ) %>% filter(term == term2) %>% 
    rename(bill_id = bill_num) %>%
    select(bill_id, term, session, short_title, summary) %>%
    filter(grepl('^HB|^SB', bill_id))
  
  
  
  
  
  ### Need to Adjust For Pre A-Sessions
  # ** Occur at T-1: 2010A == 2011_2012; 2004A = 2005_2006; 2000A = 2001_2002
  # select(all_bills, term, session) %>% distinct() %>% as.data.frame()
  #sub_bills[sub_bills$session == '2000-SS-A',]$term <- '2001_2002'
  #sub_bills[sub_bills$session == '2004-SS-A',]$term <- '2005_2006'
  #sub_bills[sub_bills$session == '2010-SS-A',]$term <- '2011_2012'
  #sub_bills[sub_bills$session == '2022-SS-A',]$term <- '2023_2024'
  
  
  sub_bills <- sub_bills %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(short_title)), 1, 0)) %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
    mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(short_title, summary)), 1, commem)) %>% 
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(short_title)), 0, commem)) %>%
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>%
    # Make modifications for 2023_2024 term
    mutate(commem = ifelse(term == "2023_2024" & 
                             grepl("requir|creat(es|ing the) division|provides eligibility|legal holiday", tolower(summary)), 0, commem))
  
  
  # filter(all_bills, commem == 1) %>% View()
  
  ### Save
  write.csv(sub_bills, glue("{this_state}_Commem_Bills_{term2}.csv"), row.names = FALSE)
}

rm(this_state, state_files, c_terms, e_terms, term2, terms, all_bills, sub_bills, start_year)

#############
### GEORGIA
#############

### Read ALL Bills
this_state <- "GA"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'naming', 'holidays and observances')
e_terms <- c(exclude, 'honors program', 'uniform reporting system', 'in-service training', 'game and fish', 'transaction')


### Clean
all_bills <- rename(all_bills, bill_id = bill_number)
special_nums <- str_extract(all_bills$bill_id, 'EX[0-9]+$')
all_bills$bill_id <- gsub('EX[0-9]+$', '', all_bills$bill_id)
bill_parts <- str_split_fixed(all_bills$bill_id, ' ', 2)
all_bills$bill_id <- paste0(bill_parts[,1], str_pad(bill_parts[,2], 4, pad = '0'), ifelse(is.na(special_nums), '', paste0('-', special_nums)))

### Session
all_bills$session_year <- gsub('-', '_', str_extract(all_bills$session, '\\d{4}-\\d{4}|\\d{4}'))
all_bills$session <- str_trim(gsub('\\d{4}-\\d{4} |\\d{4} ', '', all_bills$session))
all_bills$session <- recode(all_bills$session, 'Regular Session' = 'RS', 'Special Session' = 'SS1', '1st Special Session' = 'SS1', 
                            '2nd Special Session' = 'SS2', '3rd Special Session' = 'SS3', '4th Special Session' = 'SS4')
all_bills$session <- paste(all_bills$session_year, all_bills$session, sep = '-')


terms = c("2023_2024")

for(term in terms){
  cat("State: ", this_state, "\n Term: ",term,"\n")
  start_year = as.integer(substr(term,1,4))
  sub_bills <- all_bills %>%
    filter(substr(session,1,4) %in% c(start_year,start_year+1)) %>%
    mutate(year = as.integer(substring(session, 1, 4)),
           term = term) %>% 
    select(bill_id, term, session, title, summary) %>%
    filter(grepl("^HB|^SB", bill_id)) %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>% 
    mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title, summary)), 1, commem)) %>% 
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>%
    # Fix cases that aren't commemorative
    mutate(commem = ifelse(grepl("tuesday following the first monday in november|historically significant documents", 
                                 tolower(summary)), 0, commem)) %>%
    mutate(commem = ifelse(grepl("appoint|monument", tolower(summary)) & term == "2023_2024", 0, commem))
  
  # filter(all_bills, commem == 1) %>% mutate(summary = gsub('a [a-z].+annotated', '', tolower(summary) )) %>% View()
  
  ### Save
  write.csv(sub_bills, glue("{this_state}_Commem_Bills_{term}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills, term, terms, special_nums, bill_parts, sub_bills)

#############
### HAWAII
#############

### Read ALL Bills
this_state <- "HI"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows %>% distinct()

### Clean
all_bills <- all_bills %>%
  mutate(term = session,
         session = session_type) %>%
  rename(bill_id = bill_number) %>%
  select(bill_id, term, session, title, summary) %>%
  filter(grepl('^HB|^SB', bill_id))

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, c("holiday")) 
c_terms <- c_terms[!grepl("rename", c_terms)]
c_terms <- c(c_terms, "^rename")
e_terms <- c(exclude, 'redevelop', 'name designees', 'delete', 'recreation activ', 'primary elec', 'hiring', 
             'marriage', 'arrest', 'honorable discharge', 'assessment', ' election')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", summary), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>%
  # Add session-specific fixes for 2023-2024
  mutate(commem = ifelse(term == "2023_2024" & 
                           (grepl("state holiday", tolower(title)) |
                              grepl("state holiday|emergency|department|definition|(establishes|renames).+program|eligibility|amends the composition", tolower(summary))), 0, commem))

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()

# write.csv(all_bills, glue("{this_state}_Commem_Bills.csv"), row.names = FALSE)

### Save
for(t in c("2019_2020","2021_2022","2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills, t)


#############
### IOWA
#############

### Read ALL Bills
this_state <- "IA"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  rename(term_dates = session_dates,
         bill_id = bill_number,
         ga_num = session) %>% 
  mutate(term = as.numeric(substring(all_bills$session_dates, 7, 10)), 
         term = paste(term, term + 1, sep = "_"),
         session = term, 
         bill_id = paste0(gsub(' .+', '', bill_id), str_pad(gsub('[A-Z]+ ', '', bill_id), 4, pad = "0" ))
  ) %>%
  select(bill_id, term, session, summary) %>%
  filter(grepl('^HF|^SF', bill_id))


### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'designation of (a|an|the) (official|state)', '\\bdesignat.+(as the|each year)',
             "annual designation", "iowa great places") 
e_terms <- c(exclude)

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, 0)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", summary), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) 

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()
# filter(all_bills, grepl("\\bdesignat.+(day|month|year|week)", tolower(summary)) & commem == 0) %>% View()

#### Manual Fixes
all_bills[grepl('prison time served', tolower(all_bills$summary)),]$commem <- 0
all_bills[grepl('qualifications of commissioners|residency qualifications', tolower(all_bills$summary)),]$commem <- 0
all_bills[grepl('dual party relay', tolower(all_bills$summary)),]$commem <- 0
all_bills[grepl('disorder proclamations', tolower(all_bills$summary)),]$commem <- 0
all_bills[grepl('honor schools', tolower(all_bills$summary)),]$commem <- 0
all_bills[grepl('posthumously conceived and born', tolower(all_bills$summary)),]$commem <- 0
all_bills[grepl('memorial hospitals', tolower(all_bills$summary)),]$commem <- 0
all_bills[grepl('to conduct a study', tolower(all_bills$summary)),]$commem <- 0
all_bills[grepl('restricted donors', tolower(all_bills$summary)),]$commem <- 0
all_bills[grepl('designating the sullivan brothers', tolower(all_bills$summary)),]$commem <- 1
all_bills[grepl('kwanzaa week', tolower(all_bills$summary)),]$commem <- 1
all_bills[grepl('emergency proclamation', tolower(all_bills$summary)),]$commem <- 0
all_bills[grepl('disaster aid', tolower(all_bills$summary)),]$commem <- 0
all_bills[grepl('social studies instruction', tolower(all_bills$summary)),]$commem <- 0

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills, t)


#############
### IDAHO
#############

### Read ALL Bills
this_state <- "ID"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols(session = 'c')) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  rename(bill_id = bill_number) %>% 
  mutate(s_year = as.numeric(substring(all_bills$session, 1, 4)), 
         term = ifelse(s_year %% 2 == 0, paste(s_year - 1, s_year, sep = "_"), paste(s_year, s_year + 1, sep = "_")),
         session = ifelse(grepl('spcl', session), gsub('spcl', '-SS', session), paste0(session, '-RS')),
         bill_id = gsub('[a-z]$', '', bill_id)
  ) %>%
  select(bill_id, term, session, title, summary) %>%
  filter(grepl('^H[0-9]|^S[0-9]', bill_id))

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'designat.+the state [a-z]+.+of idaho', 'designated$', 'designate the ([a-z]+ [a-z]+ [a-z]+|[a-z]+ [a-z]+|[a-z]+) as the state') 
c_terms <- c_terms[!grepl('renam', c_terms)]
e_terms <- c(exclude)

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title, summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("designat.+ as.+ [A-Z][a-z]+ (Road|Bridge|Highway|Lake|Trail)\\b", paste(title, summary)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem))  %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) 

#### Manual Fixes
all_bills[grepl('(adds to|adds to and amends) existing law.+license plate|license plates - wildlife|(white water|white water rafting|boy scout) license plate', tolower(all_bills$summary)) & all_bills$commem == 0,]$commem <- 1
all_bills[grepl('prohibit the authorization|license plates - (fees|surcharge)|license plate readers', tolower(all_bills$summary)) & all_bills$commem == 1,]$commem <- 0
all_bills[grepl('lottery moneys', tolower(all_bills$summary)) & all_bills$commem == 1,]$commem <- 0
all_bills[grepl('repeals existing law', tolower(all_bills$summary)) & all_bills$commem == 1,]$commem <- 0
all_bills[grepl('fiduciary duty', tolower(all_bills$summary)) & all_bills$commem == 1,]$commem <- 0
all_bills[grepl('human resources div', tolower(all_bills$summary)) & all_bills$commem == 1,]$commem <- 0
all_bills[grepl('honorarium', tolower(all_bills$summary)) & all_bills$commem == 1,]$commem <- 0
all_bills[grepl('admission eligibility|reemploymnt', tolower(all_bills$title)) & all_bills$commem == 1,]$commem <- 0
all_bills[grepl('inadmissible', tolower(all_bills$summary)) & all_bills$commem == 1,]$commem <- 0
all_bills[grepl('veteran preferences', tolower(all_bills$summary)) & all_bills$commem == 1,]$commem <- 0
all_bills[grepl('state treasurer', tolower(all_bills$summary)) & all_bills$commem == 1,]$commem <- 0
all_bills[grepl('compulsory attendance', tolower(all_bills$summary)) & all_bills$commem == 1,]$commem <- 0

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()
#filter(all_bills, grepl('license plate', tolower(paste(title, summary))) & commem == 0) %>% View()


### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills)


#############
### ILLINOIS ####
#############

### Read ALL Bills
this_state <- "IL"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

terms = c(103)

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions)
e_terms <- c(exclude, 'raffles', 'examinat', 'sale of', 'educational req', 'attendance', 'technical', 'biologic', 'vital records')



### Clean



for (term in terms){
  cat("State: ", this_state, "\n Term: ",term,"\n")
  start_year = term
  sub_bills <- all_bills %>%
    filter(session_num == start_year) %>% 
    rename(bill_id = bill_number) %>%
    select(bill_id, session_num, session, title, summary) %>%
    filter(grepl('^HB|^SB', bill_id)) %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>% 
    mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title, summary)), 1, commem)) %>% 
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>%
    # Exclude non-commemorative bills for the 2023-24 session
    mutate(commem = ifelse(term == 103 & grepl("iema|sch|leave|rest|trust code|pen cd|victim|committee", tolower(title)), 0, commem))
  table(sub_bills$commem) 
  write.csv(sub_bills, glue("{this_state}_Commem_Bills_{term}.csv"), row.names = FALSE)
}

# filter(all_bills, commem == 1) %>% View()

### Save
rm(this_state, state_files, c_terms, e_terms, all_bills, term, terms, start_year, sub_bills)

#############
### INDIANA
#############
# *********** Quality of matches poor... ***********

### Read ALL Bills
this_state <- "IN"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
# all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

all_bills <- read_csv(state_files[1], col_types = cols()) %>% mutate(session = as.character(session))
for(sf in state_files[2:length(state_files)]){
  sf_bills <- read_csv(sf, col_types = cols()) %>% mutate(session = as.character(session))
  all_bills <- bind_rows(all_bills, sf_bills)
}; rm(sf_bills, sf)

### Clean
all_bills <- all_bills %>%
  rename(bill_id = bill_number) %>%
  mutate(session_year = as.numeric(gsub(' .+', '', session)),
         term = ifelse(session_year %% 2 == 1, paste(session_year, session_year + 1, sep = "_"), paste(session_year - 1, session_year, sep = "_")),
         session = ifelse(grepl(' Special', session), gsub(' Special', '-SS', session), paste0(session, "-RS")),
         summary = iconv(summary, "ASCII", "UTF-8", sub="")) %>%
  select(bill_id, term, session, title, summary) %>%
  filter(grepl('^HB|^SB', bill_id))

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions)
e_terms <- c(exclude, 'absence', 'economics', 'school corp', 'prescription', ' crime ')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title, summary)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>%
  # Make manual fixes for 2023-2024
  mutate(commem = ifelse(grepl("requires|establishes", tolower(summary)), 0, commem))

table(all_bills$term, all_bills$commem) 
# filter(all_bills, commem == 1) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}

rm(this_state, state_files, c_terms, e_terms, all_bills, t)

#############
### KANSAS
#############


### Read ALL Bills
this_state <- "KS"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols(session = 'c')) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  mutate(s_year = as.numeric(substring(all_bills$session, 1, 4)), 
         term = ifelse(s_year %% 2 == 0, paste(s_year - 1, s_year, sep = "_"), paste(s_year, s_year + 1, sep = "_")),
         session = gsub("_","-",session)
  ) %>%
  select(bill_id, term, session, title) %>%
  filter(grepl('^HB[0-9]|^SB[0-9]', bill_id))

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, "^designat", "provid.*license plate",
             "columbus day") 
e_terms <- c(exclude)

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem))  

#### Manual Fixes
all_bills[grepl('requiring (closure|certain|the committee|kdhe)|state health care benefits|legal public holiday|article v|enacting|appoint|definition|for public middle schools', 
                tolower(all_bills$title)) & all_bills$commem == 1,]$commem <- 0

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()
#filter(all_bills, grepl('license plate', tolower(paste(title, summary))) & commem == 0) %>% View()


### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills, t)



#############
### KENTUCKY
#############

### Read ALL Bills
this_state <- "KY"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  rename(bill_id = bill_number) %>%
  filter(session_year >= 2001) %>% 
  mutate(term = ifelse(session_year %% 2 == 1, paste0(session_year, "_", session_year + 1), paste0(session_year - 1, "_", session_year)),
         session = paste(session_year, session_type, sep = "-"),
         bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('^[A-Za-z]+', '', bill_id), 4, pad = "0"))
  ) %>%
  filter(grepl('^HB|^SB', bill_id)) %>%
  distinct() %>%
  select(bill_id, term, session, title, summary) 

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'designat.+(official state|of kentucky|the commonwealth)') #, "official state", "special license plate"
e_terms <- c(exclude, 'restructure', 'term of appointment', 'funeral establishments', 'drug counselor', 'declaring an emergency')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title, summary)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) 

### Manual Fixes
all_bills[all_bills$title %in% 'AN ACT relating to the Department of Agriculture.',]$commem <- 0
all_bills[all_bills$title %in% 'AN ACT relating to reorganization.',]$commem <- 0
all_bills[all_bills$title %in% 'AN ACT designating burgoo as the state dish.',]$commem <- 1
all_bills[all_bills$title %in% 'AN ACT designating Muhlenberg County as the birthplace of thumb picking as a style of guitar playing.',]$commem <- 1
all_bills[all_bills$title %in% 'AN ACT designating Clark County, Kentucky as the birthplace of beer cheese.',]$commem <- 1
all_bills[all_bills$title %in% 'AN ACT designating Ale-8-One as the official Kentucky original soft drink.',]$commem <- 1

table(all_bills$term, all_bills$commem) 
# filter(all_bills, commem == 1) %>% View()
# filter(all_bills, commem == 1 & grepl('acute', tolower(summary))  & !grepl('^directs (comm|dot|dep)', tolower(summary))) %>% select(summary) %>% as.data.frame()
# filter(all_bills, grepl('directs.+to designat.+ as', tolower(summary))) %>% select(term,title, summary, commem) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills)

# table(all_bills$commem)
# phrase <- "designat.+(official state|of kentucky|the commonwealth)"
# filter(all_bills, commem > - 1) %>%
#   select(bill_id, title, summary, commem) %>%
#   filter(grepl(phrase, tolower(title))| grepl(phrase, tolower(summary))) %>% #
#   #filter(!grepl(paste(e_terms , collapse = "|"), tolower(title)) ) %>% # & !grepl(paste(e_terms , collapse = "|"), tolower(summary))
#   View()


#############
### LOUISIANA
#############

### Read ALL Bills
this_state <- "LA"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

terms <- seq(1996, this_year - 1, 4)
term_df <- data.frame(year = 1996:(max(terms) + 3), term = rep(paste(terms, terms + 3, sep = "_"), each = 4))

### Clean
all_bills <- all_bills %>%
  rename(bill_id = bill_number,
         year = session) %>%
  left_join(term_df, by = "year") %>%
  mutate(bill_id =  paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = '0')),
         session_type = recode(substring(session_key, 3, nchar(session_key)), '1ES' = 'SS1', '2ES' = 'SS2', '3ES' = 'SS3'),
         session = paste(year, session_type, sep = '-'),
         keywords = gsub(':.+', '', short_title),
         short_title = gsub("^[^:]*: +", "", short_title)
  ) %>%
  select(bill_id, term, session, short_title, keywords) %>%
  filter(grepl('^HB|^SB', bill_id)) %>%
  arrange(session, bill_id)

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, '^designate', '^names', '^renames', '^recognizes')
e_terms <- c(exclude, 'constitutional amend', 'delete', ' tops', 'saltwater game fish', 'utilities', 'home study')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(short_title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", short_title), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(short_title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl('state symbol|holiday|special day', tolower(keywords)), 0, commem)) 

table(all_bills$term, all_bills$commem) 
# filter(all_bills, commem == 1) %>% View()

### Save
for(t in c("2020_2023")){
  write.csv(all_bills, glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}

rm(this_state, state_files, c_terms, e_terms, all_bills)


#############
### MASSACHUSETTS
#############
# Note: Some early house bill titles in all caps so won't catch Abc Day/Week/Month

### Read ALL Bills
this_state <- "MA"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
# all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 
all_bills <- state_files %>% lapply(read.csv) %>% bind_rows 

terms <- c("193rd")

all_bills <- rename(all_bills, bill_id = bill_number)
id_parts <- str_split_fixed(all_bills$bill_id, "\\.", 2)
id_parts[,2] <- gsub('\n.+', '', id_parts[,2])
all_bills$bill_id <- paste0(id_parts[,1], str_pad(id_parts[,2], 4, pad = "0"))

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'designat') 
e_terms <- c(exclude, 'purging', 'fund', 'leasing', 'functions', 'department of', '^message|^communication', 'town meeting', 'special commission')


for(term in terms){
  cat("State: ", this_state, "\n Term: ",term,"\n")
  start_year = term
  sub_bills <- all_bills %>%
    filter(session == start_year) %>% 
    filter(grepl('^Bill', bill_type)) %>%
    select(bill_id, session, title, summary)  %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>% 
    mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title, summary)), 1, commem)) %>% 
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) 
  
  ### Manually Adding Sick Leave Banks
  # Example: https://malegislature.gov/Bills/186/S2413 --> Akin to private relief bills in Congress
  # -- Can't add above as many include ', a department of x employee' 
  sub_bills[grepl("a sick leave bank for", tolower(paste(sub_bills$title, sub_bills$summary))),]$commem <- 1 
  
  # filter(all_bills, commem == 1) %>% View()
  # filter(all_bills, commem == 1 & grepl('designat', tolower(title))) %>% View()
  # filter(all_bills, grepl('sick leave bank', tolower(title))) %>% View()
  
  ### Save
  write.csv(sub_bills, glue("{this_state}_Commem_Bills_{term}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills, id_parts, terms, term_df)

#############
### MARYLAND
#############

### Read ALL Bills
this_state <- "MD"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols(session = "character")) %>% bind_rows 

terms <- seq(1995, this_year - 1, 4)
term_df <- data.frame(session_year = 1995:(max(terms) + 3), term = rep(paste(terms, terms + 3, sep = "_"), each = 4))

### Clean
all_bills <- all_bills %>%
  left_join(term_df, by = "session_year") %>%
  rename(bill_id = bill_number) %>%
  mutate(session_type = recode(gsub('^[0-9]+', '', session), 'rs' = 'RS', 's1' = 'SS1', 's2' = 'SS2', 's3' = 'SS3', 's4' = 'SS4'),
         session = paste0(session_year, '-', session_type)
  ) %>%
  filter(grepl('^HB|^SB', bill_id)) %>%
  select(bill_id, term, session, title, general_topics, subjects, summary) 

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, '^designat', '^proclaiming') 
e_terms <- c(exclude, '^increasing', '^decreasing', '^adding', '^eliminating', '^extending', '^expanding', 
             '^codifying', ' loan',  'surviving spouse', 'economic impact')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title, summary)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>% 
  mutate(commem = ifelse(grepl('holidays and special dates', tolower(subjects)), 1, commem)) %>%
  mutate(commem = ifelse(grepl('salaries|appropriations|comptroller|elections|commission', tolower(subjects)), 0, commem)) 
### For Subjects: "Holiday" alone --> Lots of mismatches
# View(sort(table(unlist(str_split(all_bills$subjects, ";")))))

table(all_bills$term, all_bills$commem) 
# filter(all_bills, commem == 1) %>% select(summary, subjects) %>% View()
# filter(all_bills, commem == 1 & grepl('job training', tolower(subjects))) %>% select(summary) %>% as.data.frame()
# filter(all_bills, grepl('^declaring', tolower(summary))) %>% select(summary) %>% as.data.frame()

### Save
for(t in c("2019_2022")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills, terms, term_df)

#############
### MAINE
#############

### Read ALL Bills
this_state <- "ME"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read.csv) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  distinct() %>%
  rename(bill_id = paper_num) %>%
  mutate(leg_num = term,
         fy = (leg_num - 112)*2 + 1985,
         term = paste0(fy, "_", fy + 1),
         session = recode(session, 'First Regular Session' = 'RS1', 'Second Regular Session' = 'RS2', 'First Special Session' = 'SS1', 
                          'Second Special Session' = 'SS2', 'Third Special Session' = 'SS3', 'Fourth Special Session' = 'SS4', 
                          'Fifth Special Session' = 'SS5', 'Sixth Special Session' = 'SS6', 'Seventh Special Session' = 'SS7'),
         LD_num = ifelse(LD_num == '', NA, LD_num)
  ) %>%
  filter(leg_num != 112) %>%
  select(bill_id, LD_num, term, session, title)

#### Fix Bills without Session
all_bills[all_bills$term %in% '2003_2004' & all_bills$LD_num %in% 'LD0195',]$session <- 'RS1'
all_bills[all_bills$term %in% '2005_2006' & all_bills$LD_num %in% 'LD9000',]$session <- 'SS2'
all_bills[all_bills$term %in% '2007_2008' & all_bills$LD_num %in% 'LD0166',]$session <- 'RS1'
all_bills[all_bills$term %in% '2007_2008' & all_bills$LD_num %in% 'LD0394',]$session <- 'RS1'
all_bills[all_bills$term %in% '2007_2008' & all_bills$LD_num %in% 'LD2324',]$session <- 'SS1'

## Drop Resolutions and What Not --- For Maine, want the LDs (mostly)
all_bills <- filter(all_bills, !is.na(LD_num)) 
all_bills <- filter(all_bills, !grepl('^resolution|^joint resolution|^a resolution|^a joint resolution', tolower(title)))

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'the official state', 'designate a portion.+highway', 'naming')
e_terms <- c(exclude, 'day treatment services', 'provide emergency repair', 'to ratify', 'authorize')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", title), 1, commem)) %>%
  #mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) 

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()

### Manual Fixes
all_bills[all_bills$title %in% 'An Act to Establish English as the Official State Language',]$commem <- 0
all_bills[all_bills$title %in% "Resolve, to Designate the Quoddy Loop as a Scenic Way",]$commem <- 1

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, all_bills)

#############
### MICHIGAN
#############

### Read ALL Bills
this_state <- "MI"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  rename(bill_id = bill_number) %>%
  mutate(term = gsub('-', '_', session)
  ) %>%
  filter(grepl('-HB-|-SB-', bill_id)) %>%
  select(bill_id, term, session, summary, keywords) 

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'designate as') #, '^proclaiming'
e_terms <- c(exclude, 'eligib', 'infraction', 'earmark', 'monument fund', 'license to') #, '^increasing', '^decreasing', '^adding', '^eliminating', '^extending', '^expanding', '^codifying', ' loan',  'surviving spouse', 'economic impact')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, 0)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", summary), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>% 
  mutate(commem = ifelse(grepl('holidays|state: symbol|highways: name', tolower(keywords)), 1, commem)) %>%
  mutate(commem = ifelse(grepl('appropriations|insurance|criminal|crimes| tax|elections|licensing and reg', tolower(keywords)), 0, commem)) %>%
  # Take out non-commems from 2023-24
  mutate(commem = ifelse(term == "2023_2024" & grepl('critical infrastructure|public holiday|(modify|expand) powers and duties|excused absence|limited food processors|commission', tolower(summary)), 0, commem))
# View(table(gsub(':.+', '', str_trim(unlist(str_split(all_bills$keywords, ";"))))))

table(all_bills$term, all_bills$commem) 
# filter(all_bills, commem == 1) %>% select(bill_id, summary, keywords) %>% View()
# filter(all_bills, commem == 1 & grepl('license to', tolower(summary))) %>% select(summary) %>% as.data.frame()
# filter(all_bills, grepl('recognize', tolower(summary))) %>% select(summary) %>% as.data.frame()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}

rm(this_state, state_files, c_terms, e_terms, all_bills)


#############
### MINNESOTA
#############

### Read ALL Bills
this_state <- "MN"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows

terms <- seq(1995, this_year - 1, 2)
term_df <- data.frame(session_year = 1995:(max(terms) + 1), term = rep(paste(terms, terms + 1, sep = "_"), each = 2))

### Clean
all_bills <- all_bills %>%
  rename(bill_id = bill_number) %>%
  mutate(session_year = as.numeric(gsub('_.+', '', session))) %>%
  left_join(term_df, by = "session_year") %>%
  mutate(session = gsub('_RS', "-RS", session),
         session = gsub('_S', "-SS", session)
  ) %>%
  select(bill_id, term, session, description, summary)

### Code Bill Types
all_bills$bill_type <- gsub('^a ', '', str_extract(tolower(all_bills$summary), '^a (bill|resolution|house resolution|house [a-z]+ resolution|senate resolution|senate [a-z]+ resolution|joint resolution|memorial resolution)|^conference committee report'))
all_bills$bill_type <- ifelse(all_bills$bill_type == "conference committee report", gsub('^a | for an act', '', str_extract(tolower(all_bills$summary), 'a (bill for an act|resolution|house resolution|house [a-z]+ resolution|senate resolution|senate [a-z]+ resolution|joint resolution|memorial resolution)')), all_bills$bill_type)
all_bills$bill_type <- ifelse(is.na(all_bills$bill_type), gsub('^a ', '', str_extract(tolower(all_bills$description), '^a (bill|resolution|house resolution|house [a-z]+ resolution|senate resolution|senate [a-z]+ resolution|joint resolution|memorial resolution)|^conference committee report')), all_bills$bill_type)
all_bills$bill_type <- ifelse(is.na(all_bills$bill_type), gsub('^a ', '', str_extract(substring(tolower(all_bills$summary), 1, 45), 'a (bill|resolution|house resolution|house [a-z]+ resolution|senate resolution|senate [a-z]+ resolution|joint resolution|memorial resolution)|^conference committee report')), all_bills$bill_type)
all_bills$bill_type <- ifelse(is.na(all_bills$bill_type) & grepl('^a +senate res|^senate resolution|^a senate [a-z]+ res', tolower(all_bills$description)), 'senate resolution', all_bills$bill_type)

filter(all_bills, bill_type %in% NA)
all_bills$bill_type <- ifelse(is.na(all_bills$bill_type) & grepl('^SR|^HR', all_bills$bill_id), 'resolution', all_bills$bill_type)
all_bills$bill_type <- ifelse(is.na(all_bills$bill_type), 'bill', all_bills$bill_type)

### Drop Resolutions
all_bills <- filter(all_bills, bill_type == 'bill')

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, '; designating', 'recognizing our', '; renaming the', '; naming the') #, '^designat', '^proclaiming'
e_terms <- c(exclude, 'regulations', 'fiscal year', '; imposing', 'use of funds', 'establishing standards', 'repealing',
             'school district', '; clarifying', '; modifying provision', 'amendment to the minnesota constitution',
             'retirement coverage', 'memorial health caree')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(description)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(summary, description)), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(description)), 0, commem))

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% select(summary, bill_id) %>% View()
# filter(all_bills, commem == 1 & grepl('memorial health', tolower(summary))) %>% select(summary) %>% as.data.frame()
# filter(all_bills, grepl('zzzzz', tolower(summary))) %>% select(summary, commem) %>% as.data.frame() 

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}

rm(this_state, state_files, c_terms, e_terms, all_bills, terms, term_df)

#############
### MISSOURI
#############

### Read ALL Bills
this_state <- "MO"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE, recursive = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

terms <- seq(1995, this_year - 1, 2)
term_df <- data.frame(session_year = 1995:(max(terms) + 1), term = rep(paste(terms, terms + 1, sep = "_"), each = 2))

### Clean
all_bills <- all_bills %>%
  left_join(term_df, by = "session_year") %>%
  rename(bill_id = bill_number) %>%
  mutate(ga_num = session,
         session_type = recode(session_type, '1st RS' = 'RS', '2nd RS' = 'RS', 'ES' = 'SS1', '1st ES' = 'SS1', '2nd ES' = 'SS2', '3rd ES' = 'SS3'),
         session = paste0(session_year, '-', session_type)
  ) %>%
  filter(grepl('^HB|^SB', bill_id)) %>%
  select(bill_id, term, session, title, description) 

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, '^designat', 'state emblem', '^creates special license') #, '^designat', '^proclaiming'
e_terms <- c(exclude, 'trust fund', 'authority', 'employee', 'noxious', 'livestock', 'commission', '^revise', '^define', '^enacts', 'provisions', 'debt owed')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(description)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title, description)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(description)), 0, commem)) 

table(all_bills$term, all_bills$commem) 
# filter(all_bills, commem == 1) %>% View()
# filter(all_bills, commem == 1 & grepl('provisions', tolower(description))) %>% select(description) %>% as.data.frame()
# filter(all_bills, grepl('^creates special', tolower(description))) %>% select(description, commem) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}

rm(this_state, state_files, c_terms, e_terms, all_bills, terms, term_df)


#############
### MISSISSIPPI
#############

## **** Lots of Commemoratives but also lots of mismatches... would benefit from hand coding... *****

### Read ALL Bills
this_state <- "MS"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Code term
all_bills$term <- NA
all_bills[all_bills$session_year %in% 1996:1999,]$term <- "1996_1999"
all_bills[all_bills$session_year %in% 2000:2003,]$term <- "2000_2003"
all_bills[all_bills$session_year %in% 2004:2007,]$term <- "2004_2007"
all_bills[all_bills$session_year %in% 2008:2011,]$term <- "2008_2011"
all_bills[all_bills$session_year %in% 2012:2015,]$term <- "2012_2015"
all_bills[all_bills$session_year %in% 2016:2019,]$term <- "2016_2019"
all_bills[all_bills$session_year %in% 2020:2023,]$term <- "2020_2023"

### Clean
all_bills <- all_bills %>%
  rename(bill_id = bill_number) %>%
  mutate(session_type = recode(session_type, 'ES' = 'SS1', 'ES1' = 'SS1', 'ES2' = 'SS2', 'ES3' = 'SS3', 'ES4' = 'SS4', 'ES5' = 'SS5', 'ES6' = 'SS6'),
         session = paste(session_year, session_type, sep = "-")) %>%
  distinct() %>%
  filter(grepl('^HB|^SB', bill_id)) %>%
  select(bill_id, term, session, title, summary, revenue_bill, vote_req)

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, '^official state', '(designate|establish|declare).+official state')
e_terms <- c(exclude, '^emegergency', 'health care benefits', 'traditional winter', 'authorize', 'life insurance', 'require all', 'base pay',
             'clarify responsibil', 'personal injury','must be honored', 'play football', 'trust fund', 'stadium commission',
             'school term', 'school start date', 'school attendance', 'proclamation of a local emergency')

### Coding Revenue Bills and ALl bills with 3/5 or 2/3 vote requirements as Non-Commemorative
all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", summary), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>%
  mutate(commem = ifelse(revenue_bill != "No" | vote_req != 'Majority', 0, commem))

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()

### Manual Fixes
all_bills[grepl('students with disabilit', tolower(all_bills$summary)),]$commem <- 0
all_bills[grepl('official state.+language|^english language', tolower(all_bills$summary)),]$commem <- 0

### Save
for(t in c("2020_2023")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, all_bills)


#############
### MONTANA
#############
# *************** COMMEMS ARE SPARSE = ~ 5 per term ***************

### Read ALL Bills
this_state <- "MT"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE, recursive = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

terms <- seq(1999, this_year - 1, 2)
term_df <- data.frame(session_year = 1999:(max(terms) + 1), term = rep(paste(terms, terms + 1, sep = "_"), each = 2))

### Clean
all_bills <- all_bills %>%
  filter(bill_draft == 0) %>%
  rename(bill_id = bill_number,
         session = session_type) %>%
  left_join(term_df, by = "session_year") %>%
  filter(grepl('^HB|^SB', bill_id)) %>%
  select(bill_id, term, session, title, subjects) 

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, '^designat', '^name') 
e_terms <- c(exclude, '^revise', 'wake zone', 'secondary highway', '^eliminat', 'constitutional ref', 'auditor', '^move')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", title), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
  #mutate(commem = ifelse(grepl('zzzzzzzzzzzz', tolower(subjects)), 1, commem)) %>%
  mutate(commem = ifelse(grepl('appropriation|taxation', tolower(subjects)), 0, commem)) %>%
  mutate(commem = ifelse(grepl('on the capitol complex', tolower(title)), 0, commem))

# table(all_bills$term, all_bills$commem) 
# View(table(gsub('\\(.+|\\)', '', str_trim(unlist(str_split(all_bills$subjects, ";"))))))
# filter(all_bills, commem == 1) %>% View()
# filter(all_bills, commem == 1 & grepl('waters', tolower(title))) %>% select(title) %>% as.data.frame()
# filter(all_bills, grepl('^move', tolower(title))) %>% select(title, subjects, commem) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}

rm(this_state, state_files, c_terms, e_terms, all_bills, terms, term_df, t)


#############
### NORTH CAROLINA
#############
# ************** COMMEMS SPARSE -- NEED BETTER TEXT ***************

### Read ALL Bills
this_state <- "NC"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

## Cleaning Bill Type + Correcting Coding Errors
all_bills$bill_type <- gsub('\\(.+\\)', '', all_bills$bill_type)
all_bills$bill_type <- gsub('\\/ SL.+', '', all_bills$bill_type)
all_bills$bill_type <- ifelse(grepl('^Bill +\\/ Res\\.', all_bills$bill_type), 'Resolution', all_bills$bill_type)
all_bills$bill_type <- gsub('\\/.+', '', all_bills$bill_type)
all_bills$bill_type <- str_trim(gsub('  +', ' ', all_bills$bill_type))

### Clean
all_bills <- all_bills %>%
  rename(bill_id = bill_num) %>%
  filter(bill_type == 'Bill') %>%
  select(bill_id, term, session, short_title, attributes, keywords) 

### Commmem Term Adjustments + Coding
# ** Could do 'name change' but fair amount of error AND hard to tell if commemorative in nature -- checked a few and seem minor, not celebrating anyone
c_terms <- c(vw_commem, additions, '^designat.+(official|day|week|month)', 'rename.+(bridge|highway|park|for)', '^recogniz', 'special license plates') 
e_terms <- c(exclude, 'mortgage', '^schools')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(short_title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", short_title), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(short_title)), 0, commem)) %>%
  #mutate(commem = ifelse(grepl('zzzzzzzzzzzz', tolower(keywords)), 1, commem)) %>%
  mutate(commem = ifelse(grepl('appropriation|budgeting|tax|appointmen|health services|elections', tolower(keywords)), 0, commem)) %>%
  mutate(commem = ifelse(grepl('appropriation|affects constitution', tolower(attributes)), 0, commem)) 

table(all_bills$term, all_bills$commem) 
# View(table(str_trim(unlist(str_split(all_bills$keywords, ",")))))
# filter(all_bills, commem == 1) %>% View()
# filter(all_bills, commem == 1 & grepl('license plates', tolower(short_title))) %>% select(short_title, keywords) %>% as.data.frame() %>% View()
# filter(all_bills, grepl('special license plates', tolower(short_title))) %>% select(short_title, keywords, commem) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}

rm(this_state, state_files, c_terms, e_terms, all_bills)

#############
### NORTH DAKOTA
#############

### Read ALL Bills
this_state <- "ND"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  rename(bill_id = bill_number) %>%
  distinct() %>%
  filter(grepl('^HB|^SB', bill_id)) %>%
  select(bill_id, term, session, title)

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, "as the official (north dakota|state)")
c_terms <- c_terms[!grepl('proclamation', c_terms)]
e_terms <- c(exclude, 'ballots', 'trademarks', 'inspection')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  #mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", title), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) #%>%
#mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem))

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()

### Manual Fixes
all_bills[grepl('Major General C. Emerson Murry|Ralph Engelstad', all_bills$title),]$commem <- 1
all_bills[grepl('Juneteenth', all_bills$title),]$commem <- 0

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}

rm(this_state, state_files, c_terms, e_terms, all_bills, t)


#############
### NEBRASKA
#############

### *** 2 per session --- and those are borderline... ****

### Read ALL Bills
this_state <- "NE"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows

### Clean Term/Session Variables
all_bills <- all_bills %>%
  rename(bill_id = bill_number) %>%
  mutate(term = session,
         session = "RS",
         bill_id = ifelse(grepl('A[0-9]', bill_id), paste(gsub('LBA', 'LB', bill_id), 'A', sep = ''), bill_id),
         bill_id = ifelse(grepl('A[0-9]', bill_id), paste(gsub('LRA', 'LR', bill_id), 'A', sep = ''), bill_id),
         bill_id = ifelse(grepl('A[0-9]', bill_id), paste(gsub('LRCA', 'LRC', bill_id), 'A', sep = ''), bill_id),
         b_id_doc_id = paste(bill_id, gsub('.+DocumentID=', '', bill_url), sep = '-'))

### ***** MANUALLY CODE SPECIAL SESSION BILLS -- find by googling, e.g.: nebraska "101st Legislature, 1st Special Session - Day 1" and manually adjusting days forward to get all bills
spec_bills07 <- c("LB0001-6171", 'LB0002-6197', 'LB0003-6237', 'LR0001-6245', 'LR0002-6266', 'LR0003-6265', 'LR0004-6279', 'LR0005-6283', 'LR0006-6181', 'LR0007-6296', 'LR0008-6297', 'LR0009-6304', 'LR0010-6303', 'LR0011-6280')
spec_bills09 <- c("LB0001-9421", 'LB0002-9429', 'LB0003-9428', "LB0004-9422", 'LB0005-9433', 'LB0006-9440', "LB0007-9441", "LB0008-9465", "LB0009-9466", "LB0010-9443", "LB0011-9457", "LB0012-9455", "LB0013-9438", "LB0014-9468", "LB0015-9453", "LB0016-9473", 
                  'LR0001-9388', 'LR0002-9437', 'LR0003-9436', 'LR0004-9460', 'LR0005-9312', 'LR0006-9469', 'LR0007-9471', 'LR0008-9474', 'LR0009-9480', 'LR0010-9486', 'LR0011-9488', 'LR0012-9489', 'LR0013-9490', 'LR0014-9477', 
                  'LR0015-9475', 'LR0016-9498', 'LR0017-9472', 'LR0018-9492', 'LR0019-9503', 'LR0020-9516', 'LR0021-9523', 'LR0022-9529', 'LR0023-9518', 'LR0024-9385', 'LR0025-9539', 'LR0026-9535', 'LR0027-9536', 'LR0028-9540', 'LR0029-9538', 'LR0030-9537', 'LR0031-9559')
spec_bills11 <- c("LB0001-15149", 'LB0002-15277', 'LB0003-15295', "LB0004-15278",  'LB0005-15121', 'LB0006-15294', "LB0001A-15391", "LB0004A-15381", 'LR0001-15246', 'LR0002-15261', 'LR0003-15281', 'LR0004-15282', 'LR0005-15283', 
                  'LR0006-15292', 'LR0007-15293', 'LR0008-15296', 'LR0009-15173', 'LR0010-15301', 'LR0011-15297', 'LR0012-15304', 'LR0013-15302', 'LR0014-15319', 'LR0015-15318', 'LR0016-15325', 'LR0017-15305', 'LR0018-15322', 'LR0019-15345', 'LR0020-15343', 'LR0021-15356', 
                  'LR0022-15376', 'LR0023-15386', 'LR0024-15390', 'LR0025-15395', 'LR0026-15399', 'LR0027-15400', 'LR0028-15398', 'LR0029-15394', 'LR0030-15410', 'LR0031-15418', 'LR0032-15440', 'LR0033-15441', 'LR0034-15438', 'LR0035-15435')
spec_bills21 = c("LB0001-46675","LB0002-46676","LB0003-46677","LB0004-46678",
                 "LB0005-46671","LB0006-46674","LB0007-46673","LB0008-46672",
                 "LB0009-46696","LB0010-46695","LB0011-46683","LB0012-46670",
                 "LB0013-46698","LB0014-46667","LB0015-46687")
all_bills[all_bills$term == '2007_2008' & all_bills$b_id_doc_id %in% spec_bills07,]$session <- 'SS1'
all_bills[all_bills$term == '2009_2010' & all_bills$b_id_doc_id %in% spec_bills09,]$session <- 'SS1'
all_bills[all_bills$term == '2011_2012' & all_bills$b_id_doc_id %in% spec_bills11,]$session <- 'SS1'
all_bills[all_bills$term == '2021_2022' & all_bills$b_id_doc_id %in% spec_bills21,]$session <- 'SS1'

### Subset and Select Key Columns
all_bills <- all_bills %>%
  distinct() %>%
  filter(grepl('^LB', bill_id)) %>%
  select(bill_id, term, session, summary)

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions) # , "as the official (north dakota|state)"
e_terms <- c(exclude, 'rename and change', "^change", 'employment restriction', 'award provisions', '^provide', "^merge",
             'alternativce response', '^create') # , 'ballots', 'trademarks', 'inspection'

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, 0)) %>%
  #mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", summary), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) #%>%
#mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem))

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()

### Manual Fixes
#all_bills[grepl('Major General C. Emerson Murry|Ralph Engelstad', all_bills$title),]$commem <- 1

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, all_bills)


#############
### NEW HAMPSHIRE
#############

### Read ALL Bills
this_state <- "NH"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- read_csv(state_files[1], col_types = cols())
all_bills$lsr <- as.character(all_bills$lsr)
for(i in 2:length(state_files)){
  new_bills <- read_csv(state_files[i], col_types = cols(S_floor_date = "date",
                                                         H_floor_date = "date"))
  new_bills$lsr <- as.character(new_bills$lsr)
  all_bills <- bind_rows(all_bills, new_bills)
}; rm(new_bills)

### Clean Term/Session Variables
all_bills <- all_bills %>%
  rename(bill_id = bill_number,
         session = session_year) %>%
  mutate(term = ifelse(session %% 2 == 1, paste0(session, "_", session + 1), paste0(session - 1, "_", session)),
         title = str_trim(gsub('^\\([^\\)]+\\)', '', title)),
         bill_id = gsub("-.+$|[A-Z]+$", "", bill_id),
         bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('[A-Z]+', '', bill_id), 4, pad = 0)),
         session = ifelse(grepl("^SS", bill_id) | grepl("SPECIAL SESSION BILL", title), paste0(session, '-SS'), paste0(session, '-RS'))) %>%
  filter(grepl("^HB|^SB|^SS", bill_id)) %>%
  distinct() %>%
  select(bill_id, term, session, title)

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, '^naming', "^renaming", "relative to naming", 'official state', 'official new hamp', "designat.+new hampshire state") 
e_terms <- c(exclude) 

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  #mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b|^designat.+the.+[A-Z][a-z]+ (Highway|Overlook)", title), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) #%>%
#mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem))

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()

### Manual Fixes
all_bills[grepl('relative to a state police barracks honor roll', all_bills$title),]$commem <- 0
all_bills[all_bills$title == 'declaring the chinook the state dog of New Hampshire.',]$commem <- 1
all_bills[all_bills$title == 'designating the Thompson/Center Contender pistol as the official state firearm of New Hampshire.',]$commem <- 1
all_bills[all_bills$title == 'designating May 19 as a Day of Compassion.',]$commem <- 1

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}

rm(this_state, state_files, c_terms, all_bills)


#############
### NEW JERSEY
#############

### Read ALL Bills
this_state <- "NJ"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read.csv, skipNul = TRUE) %>% bind_rows 
# problems(all_bills) ## Warnings about embedded nulls for 2000_2001 but none show up here.. odd.. if use lapply(read_csv)

bill_parts <- str_split_fixed(all_bills$bill_number, '-', 2)
all_bills$bill_id <- paste0(bill_parts[,1], str_pad(as.numeric(bill_parts[,2]), 4, pad = '0')  )

### Clean
all_bills <- all_bills %>%
  rename(term = session) %>% 
  mutate(session = NA) %>%
  filter(grepl('^A[0-9]|^S[0-9]', bill_id)) %>%
  select(bill_id, term, session, title, summary) 

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, '^designates.+as the', 'directs.+to designat.+ as') # '^name'
e_terms <- c(exclude, 'alcohol', 'acute', 'pension', 'reorg', 'fundrais', 'executive order', 'interlocal', 'act\\."$',
             'service preference', '^directs doe', 'expands', '^clarifies', '^excludes', '^transfers', '^eliminates', '^requires')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title, summary)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) 

table(all_bills$term, all_bills$commem) 
# filter(all_bills, commem == 1) %>% View()
# filter(all_bills, commem == 1 & grepl('acute', tolower(summary))  & !grepl('^directs (comm|dot|dep)', tolower(summary))) %>% select(summary) %>% as.data.frame()
# filter(all_bills, grepl('directs.+to designat.+ as', tolower(summary))) %>% select(term,title, summary, commem) %>% View()

### Save
for(t in c("2018_2019","2020_2021","2022_2023")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills)


#############
### New Mexico
#############

### Read ALL Bills
this_state <- "NM"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  rename(bill_id = bill_number) %>% 
  mutate(term = ifelse(session_year %% 2 == 1, paste0(session_year, "_", session_year + 1), paste0(session_year - 1, "_", session_year)),
         session_type = recode(session_type, 'S1' = 'SS1', 'S2' = 'SS2', 'S3' = 'SS3', 'S4' = 'SS4', 'S5' = 'SS5', 'S6' = 'SS6'),
         session = paste(session_year, session_type, sep = "-")
  ) %>%
  filter(grepl('^HB|^SB', bill_id)) %>%
  distinct() %>%
  select(bill_id, term, session, title) 

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, "official state", "special license plate")
e_terms <- c(exclude, "^acquire", "^equip", 'alcohol', '^transfer', 'celebration permits')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  #mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  #mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", title), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) #%>%
#mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) 

### STB(R) or CPB in Title = Funded by bonds = Appropriation --- https://www.nmvoices.org/archives/5945
all_bills[grepl("STBR$|STB$|CPB$", all_bills$title),]$commem <- 0

### Manual Fixes
all_bills[all_bills$title %in% 'FUNERAL & MEMORIAL SERVICE DEMONSTRATIONS',]$commem <- 0

table(all_bills$term, all_bills$commem) 
# filter(all_bills, commem == 1) %>% View()
# filter(all_bills, commem == 1 & grepl('acute', tolower(summary))  & !grepl('^directs (comm|dot|dep)', tolower(summary))) %>% select(summary) %>% as.data.frame()
# filter(all_bills, grepl('directs.+to designat.+ as', tolower(summary))) %>% select(term,title, summary, commem) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}

rm(this_state, state_files, c_terms, e_terms, all_bills)


#############
### Nevada
#############

### Read ALL Bills
this_state <- "NV"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 
all_bills <- distinct(all_bills)

terms <- seq(1995, this_year - 1, 2)
term_df <- data.frame(session_year = 1995:(max(terms) + 1), term = rep(paste(terms, terms + 1, sep = "_"), each = 2))

### Clean
all_bills <- all_bills %>%
  left_join(term_df, by = 'session_year') %>%
  rename(bill_id = bill_number,
         term_num = session) %>% 
  mutate(session_type = recode(session_type, 'R' = 'RS', 'S' = 'SS', 'S2' = 'SS2'),
         session = paste(session_year, session_type, sep = '-')
  ) %>%
  filter(grepl('^AB|^SB', bill_id)) %>%
  select(bill_id, term, session, title, summary) 

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, '^designat', '^names', 'provides.+special license plate') # designat
e_terms <- c(exclude, "revises provision", 'election', 'reorganize', 'nonpartisan office', 'charter agency', 
             'child care', 'commission\\b', 'expand', 'family court', 'construction')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title, summary)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>%
  # Remove changes to legal holidays and criteria for awards
  mutate(commem = ifelse(grepl("a legal holiday|medal of distinction", tolower(title)), 0, commem))

table(all_bills$term, all_bills$commem) 
# filter(all_bills, commem == 1) %>% View()
# filter(all_bills, commem == 1 & grepl('expand', tolower(summary))) %>% select(summary) %>% as.data.frame()
# filter(all_bills, grepl('provides.+special license plate', tolower(summary))) %>% select(term,title, summary, commem) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}

rm(this_state, state_files, c_terms, e_terms, all_bills, terms, term_df)


#############
### New York ####
#############


### Read ALL Bills
this_state <- "NY"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Commmem Term Adjustments + Coding
## ** Could drop ^authoriz -- some error and mostly authorizing license plates , some of which may or may not be commem..
c_terms <- c(vw_commem, additions, 'to the memory') # , '^designat', '^names', 'provides.+special license plate'
e_terms <- c(exclude, 'eviction', 'property', 'banking', '^enacts', '^suspends', '^modernizes',
             '^expands', '^extends', 'registered', 'resuscitate', 'definition', 'entitled to', 'ballot', 'commission',
             'appointment', 'helmet', 'payment', 'provision', 'discontin', 'absence', 'power', 'easement')

terms = c("2023_2024")

for(term in terms){
  cat("State: ", this_state, "\n Term: ",term,"\n")
  start_year = as.integer(substr(term,1,4))
  ### Clean
  sub_bills <- all_bills %>%
    filter(session == start_year) %>% 
    rename(bill_id = bill_number) %>% 
    mutate(session = NA,
           term = term) %>%
    filter(grepl('^A|^S', bill_id)) %>%
    select(bill_id, term, session, short_summary, summary) %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(short_summary)), 1, 0)) %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
    mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(short_summary, summary)), 1, commem)) %>% 
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(short_summary)), 0, commem)) %>%
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>%
    # Manually get rid of 2023-2024 false positives
    mutate(commem = ifelse(term == "2023_2024" & grepl("holiday|commemorative medal|voter|registry|fee|emergency order|aid|absent|separation|renames.+office|civil service exam|game of chance|requires|council|conduct a study|facilities|renames.+board|fund|renames.+court|renaming the bridges|memorial marker|preservation|veteran status", tolower(short_summary)), 0, commem)) %>%
    mutate(commem = ifelse(term == "2023_2024" & grepl("half-staff", tolower(summary)), 0, commem))
  #sort(table(gsub(' .+', '', all_bills[all_bills$commem == 1,]$short_summary)))
  
  table(sub_bills$commem) 
  # filter(all_bills, commem == 1) %>% select(-session) %>% View()
  # filter(all_bills, commem == 1 & grepl('establishes', tolower(summary))) %>% select(short_summary, summary) %>% as.data.frame()
  # filter(all_bills, grepl('to honor', tolower(short_summary))) %>% select(term, short_summary, summary, commem) %>% View()
  
  ### Save
  write.csv(sub_bills, glue("{this_state}_Commem_Bills_{term}.csv"), row.names = FALSE)
}

rm(this_state, state_files, c_terms, e_terms, all_bills, sub_bills, terms, term, start_year)


#############
### OHIO ####
#############

### Read ALL Bills
this_state <- "OH"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols(session_year = col_character())) %>% bind_rows 


### Commmem Term Adjustments + Coding
## ** Keyword "special designation" = coded exactly how we want; however, used only from 2015+
## --> Number of commems actually increases a few yeas before that, so not clear if this coding in inflating for later years or not
c_terms <- c(vw_commem, additions, 'designate.+ as the (official|state)', 'official.+of (ohio|the state)',
             "recognize.+as the state") 
e_terms <- c(exclude, 'repeal', 'eliminate', 'health insur', 'proficiency test', 'licensing board', 'electors',
             'codify', 'payment', 'act\\."')

terms = c("2023_2024", "2024_S")

for(term in terms){
  cat("State: ", this_state, "\n Term: ",term,"\n")
  start_year = as.integer(substr(term,1,4))
  if (start_year < 2023){
    term2 <- start_year 
  } else {
    term2 <- term
  }
  sub_bills <- all_bills %>%
    filter(session_year == term2) %>% 
    rename(bill_id = bill_number,
           session = session_num) %>% 
    mutate(term = term) %>%
    filter(grepl('^HB|^SB', bill_id)) %>%
    select(bill_id, term, session, title, long_title, subjects)  %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(long_title)), 1, commem)) %>%
    mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title, long_title)), 1, commem)) %>% 
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(long_title)), 0, commem)) %>%
    mutate(commem = ifelse(grepl('special designations|official state', tolower(subjects)), 1, commem)) 
  #%>% mutate(commem = ifelse(grepl('zzzzzzzzz', tolower(subjects)), 0, commem)) 
  # View(table(str_trim(unlist(str_split(all_bills$subjects, ';')))))
  
  # table(all_bills$term, all_bills$commem) 
  # filter(all_bills, commem == 1) %>% select(-session) %>% View()
  # filter(all_bills, commem == 1 & grepl('', tolower(long_title))) %>% select(title, long_title) %>% as.data.frame()
  # filter(all_bills, grepl('to name', tolower(long_title))) %>% select(term, title, long_title, commem) %>% View()
  
  ### Save
  write.csv(sub_bills, glue("{this_state}_Commem_Bills_{term}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills, terms, term2, start_year, sub_bills)

#############
### OKLAHOMA
#############

### Read ALL Bills
this_state <- "OK"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols(intro_date = "character")) %>% bind_rows

### CLEAN
all_bills <- all_bills %>%
  mutate(yr = as.numeric(substring(session, 1,4)),
         term = ifelse(yr %% 2 == 1, paste0(yr, "_", yr + 1), paste0(yr - 1, "_", yr)),
         bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('^[A-Z]+', '', bill_id), 4, pad = '0'))) %>%
  filter(grepl('^HB|^SB', bill_id)) %>%
  select(bill_id, term, session, intro_date, title, bill_url) %>%
  distinct()

######## Bills Carryover From Odd to Even Years --> Need to Drop Even-Year Duplicates!
# --> Need to be careful to not drop special session bills however (where duplciates != same bill)
# --> e.g, SB1 introduced in odd year, carries over to even year, but first special session bill will be called SB1
# --> Bills in CONFERENCE committee do not necessarily carry over:
# --> See most recent rules: http://www.oksenate.gov/publications/senate_rules/2019%20Senate%20Rules.pdf
save_bills <- filter(all_bills, term == 'zzzzz') #%>% mutate(session_orig = NA)
for(t in seq(2023, max(as.numeric(gsub('-.+', '', all_bills$session))), 2)){
  t_yrs <- paste0(t, "_", t + 1)
  t_bills <- filter(all_bills, term == t_yrs)
  dup_bills <- c()
  for(i in 1:nrow(t_bills)){
    if(t_bills[i,]$session == paste0(t, "-RS")){
      bill_sub <- filter(t_bills, bill_id == t_bills[i,]$bill_id & grepl('RS', session))
      ## Checking if even year bill has same title (if there is an even bill with that id)
      if(nrow(bill_sub) > 1 & length(unique(bill_sub$intro_date)) == 1){
        dup_bills <- append(dup_bills, t_bills[i,]$bill_id)
      }
    }
  }
  
  #### Dropping the 1993 Duplicates (in some cases the second session page has more detailed actions)
  #### AND readujusting the session dates to align with introduction ---> Keeping Original Session to Match to Bill History Data
  t_bills <- filter(t_bills, !(bill_id %in% dup_bills & session == paste0(t, "-RS")))
  t_bills$new_date <- as.numeric(substring(t_bills$intro_date, 1, 4))
  t_bills$new_date <- ifelse(t_bills$new_date < t, t, t_bills$new_date) # Adjusting for Dec T-1 Bills
  t_bills$new_date <- ifelse(t_bills$new_date >= t + 2, t + 1, t_bills$new_date) # Adjusting for Jan T+2 Bills
  t_bills$new_session <- ifelse(grepl('-RS', t_bills$session), paste0(t_bills$new_date, "-RS"), t_bills$session)
  t_bills <- rename(t_bills, session_orig = session)
  t_bills$session <- ifelse(t_bills$new_session == '-RS' | t_bills$new_session == "NA-RS", t_bills$session_orig, t_bills$new_session)
  t_bills <- select(t_bills, -c(new_session, new_date))
  save_bills <- bind_rows(save_bills, t_bills)
  rm(dup_bills, t_bills, bill_sub, t_yrs, i)
  print(paste0(" ~~ ", t))
}

all_bills <- save_bills %>% select(-c(bill_url, intro_date))
rm(save_bills, t)

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'designat.+(official state|of oklahoma)') 
e_terms <- c(exclude, 'power of attorney', 'clarifying language', 'emergency assistance', 'modifying definition',
             'state coordinating entity', 'indigent patient care', '^mental health', "licensure req")

terms = c("2023_2024")
for(term2 in terms){
  cat("State: ", this_state, "\n Term: ",term,"\n")
  sub_bills <- all_bills %>%
    filter(term == term2) %>% 
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
    #mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
    mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title)), 1, commem)) %>%
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) #%>%
  #mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem))
  
  ### Manual Fixes
  sub_bills[sub_bills$title %in% 'State government; designating English as the official state language; providing exception. Effective date.',]$commem <- 0
  
  
  # table(all_bills$term, all_bills$commem)
  # filter(all_bills, commem == 1) %>% View()
  # filter(all_bills, commem == 1 & grepl('acute', tolower(summary))  & !grepl('^directs (comm|dot|dep)', tolower(summary))) %>% select(summary) %>% as.data.frame()
  # filter(all_bills, grepl('directs.+to designat.+ as', tolower(summary))) %>% select(term,title, summary, commem) %>% View()
  
  ### Save
  write.csv(sub_bills, glue("{this_state}_Commem_Bills_{term2}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills, sub_bills, term, term2, terms)


#############
### OREGON ####
#############

### Read ALL Bills
this_state <- "OR"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
#all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows
all_bills <- read.csv(state_files[1],colClasses = "character")
all_bills$chapter_num <- as.character(all_bills$chapter_num)
for(i in 2:length(state_files)){
  new_bills <- read.csv(state_files[i],colClasses = "character")
  new_bills$chapter_num <- as.character(new_bills$chapter_num)
  all_bills <- bind_rows(all_bills, new_bills)
}; rm(new_bills)

c_terms <- c(vw_commem, additions, '^declares.+of each year', '^designat.+(of every year|as oregon)') #, 'designat.+(official state|of oklahoma)'
e_terms <- c(exclude, '^requires', '^modifies', 'property rights', 'receive benefits', 'quarterly statement', 'registration fee', 'renames crime',
             '^directs', 'handgun license', 'removes from definition', 'zoning status', "^expands", '^revises')

terms = c("2023_2024")

for (term in terms){
  cat("State: ", this_state, "\n Term: ",term,"\n")
  start_year = as.integer(substr(term,1,4))
  sub_bills <- all_bills %>%
    filter(substr(session,1,4) %in% c(start_year, start_year+1)) %>% 
    mutate(yr = as.numeric(substring(session, 1,4)),
           term = ifelse(yr %% 2 == 1, paste0(yr, "_", yr + 1), paste0(yr - 1, "_", yr))) %>%
    filter(grepl('^HB|^SB', bill_id)) %>%
    select(bill_id, term, session, description, summary, keywords) %>%
    distinct() %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(description)), 1, 0)) %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
    mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b|designat.+[A-Z][a-z]+ (Bridge|Highway)", paste(description, summary)), 1, commem)) %>%
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(description)), 0, commem)) %>% 
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>%
    mutate(commem = ifelse(grepl("'appropriating money|emergency preparedness'", tolower(keywords)), 0, commem))
  #View(table(unlist(str_split(all_bills$keywords, "; "))))
  
  
  ### Manual Fixes
  # sub_bills[grepl("^Directs.+to name", sub_bills$summary),]$commem <- 1
  # sub_bills[sub_bills$description %in% 'Designates Act as Oregon First-Time Home Buyer Savings Account Act.',]$commem <- 0
  
  
  # table(all_bills$term, all_bills$commem)
  # filter(all_bills, commem == 1) %>% View()
  # filter(all_bills, commem == 1 & grepl('acute', tolower(summary))  & !grepl('^directs (comm|dot|dep)', tolower(summary))) %>% select(summary) %>% as.data.frame()
  # filter(all_bills, grepl('directs.+to designat.+ as', tolower(summary))) %>% select(term,title, summary, commem) %>% View()
  
  ### Save
  write.csv(sub_bills, glue("{this_state}_Commem_Bills_{term}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills, sub_bills, term, terms)


#############
### PENNSYLVANIA ####
#############

### Read ALL Bills
this_state <- "PA"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 


### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, '^an act designat', '^an act redesignat', 'official state', 'official pennsylvania',
             '^an act selecting, designating and adopting') #
e_terms <- c(exclude, 'memorial council', 'commission', 'reimburs', 'trust fund', 'definition', 'headstone',
             'regulating', '^an act requir')


terms = c("2023_2024")

for(term in terms){
  cat("State: ", this_state, "\n Term: ",term,"\n")
  start_year = as.integer(substr(term,1,4))
  sub_bills <- all_bills %>%
    filter(substr(session,1,4) == start_year) %>% 
    rename(bill_id = bill_number) %>% 
    mutate(term = gsub('-', '_', gsub(' .+', '', session_full))) %>%
    filter(grepl('^HB|^SB', bill_id)) %>%
    select(bill_id, term, session, title)  %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
    mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title)), 1, commem)) %>% 
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
    # Exclude non-commemorative bills for the 2023-24 session
    mutate(commem = ifelse(term == "2023_2024" & grepl("monument|moment of silence|authorized decorations", tolower(title)), 0, commem))
  
  # filter(all_bills, commem == 1) %>% select(-session) %>% View()
  # filter(all_bills, commem == 1 & grepl('^an act requir', tolower(title))) %>% select(title) %>% as.data.frame()
  # filter(all_bills, grepl('promote and encourage', tolower(title))) %>% select(term, title, commem) %>% View()
  write.csv(sub_bills, glue("{this_state}_Commem_Bills_{term}.csv"), row.names = FALSE)
}


### Save
rm(this_state, state_files, c_terms, e_terms, term, terms, start_year, all_bills, sub_bills)


#############
### Rhode Island
#############

# ???? TO VACATE THE FORFEITURE OR REVOCATION OF THE CHARTER OF ... ????

### Read ALL Bills
this_state <- "RI"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows

### CLEAN
all_bills <- all_bills %>%
  mutate(term = ifelse(session %% 2 == 1, paste0(session, "_", session + 1), paste0(session - 1, "_", session)),
         bill_type = toupper(gsub(' |\\s', '', bill_type)),
         bill_type = recode(bill_type, 'ANACT' = 'bill', 'HOUSERESOLUTION' = 'resolution', 'SENATERESOLUTION' = 'resolution', 'JOINTRESOLUTION' = 'joint resolution'),
         # Correct weird date parsing error in 2023 file
         intro_date = ifelse(session == 2023 & bill_id == "H6423", as.Date("2023-05-19"), intro_date)
  ) %>%
  filter(bill_type == 'bill') %>%
  select(bill_id, term, session, title) %>%
  distinct()

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'renaming')
e_terms <- c(exclude, '^authorizing', 'health and safety')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  #mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  #mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b|designat.+[A-Z][a-z]+ (Bridge|Highway)", paste(title)), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem))
#mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) 
#View(table(unlist(str_split(all_bills$keywords, "; "))))

### Manual Fixes
#all_bills[grepl("^Directs.+to name", all_bills$summary),]$commem <- 1

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()
# filter(all_bills, commem == 1 & grepl('acute', tolower(summary))  & !grepl('^directs (comm|dot|dep)', tolower(summary))) %>% select(summary) %>% as.data.frame()
# filter(all_bills, grepl('directs.+to designat.+ as', tolower(summary))) %>% select(term,title, summary, commem) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}

rm(this_state, state_files, c_terms, e_terms, all_bills, t)

#############
### South Carolina
#############

### Read ALL Bills
this_state <- "SC"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  filter(as.numeric(substring(session, 1, 4)) >= 1989) %>%
  rename(bill_id = bill_number) %>% 
  mutate(term = gsub('-', '_', session)) %>%
  filter(!grepl('resolution', tolower(bill_type))) %>%
  select(bill_id, term, session, title, summary) 

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'designat.+official state', 'designate a (road|bridge|highway) in') # designat, recog
e_terms <- c(exclude, 'school term', 'hospital district', 'repeal', 'instruct', 'commission', 'council', 'custody', ' fee',
             'candidate', 'regalia', 'school property', 'advisory committee', 'falsely', 'profit\\b', 'adjourn', 'insurance',
             'correction', 'caucus', 'paternity')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title, summary)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) 

table(all_bills$term, all_bills$commem) 
# filter(all_bills, commem == 1) %>% select(-session) %>% View()
# filter(all_bills, commem == 1 & grepl('paternity', tolower(summary))) %>% select(title, summary) %>% as.data.frame() #%>% View()
# filter(all_bills, grepl('to name', tolower(summary))) %>% select(term, title, summary, commem) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills)


#############
### South Dakota
#############

### Read ALL Bills
this_state <- "SD"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows

### Fix Bill with UTF Issue
all_bills$summary <- iconv(enc2utf8(all_bills$summary), sub = "byte")

### CLEAN
all_bills <- all_bills %>%
  mutate(bill_id = paste0(gsub('[0-9]+', '', bill_id), str_pad(gsub('^[A-Z]+', '', bill_id), 4, pad = '0')),
         term = ifelse(session_year %% 2 == 1, paste0(session_year, "_", session_year + 1), paste0(session_year - 1, "_", session_year))
  ) %>%
  filter(grepl("^HB|^SB", bill_id)) %>%
  select(bill_id, term, session, summary, keywords) %>%
  distinct()


### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'designat.+(official state|as the state|of south dakota)') # , 'renaming', designat
e_terms <- c(exclude, 'after labor day', 'honorary trusts', 'specify the position')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, 0)) %>%
  #mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b|designat.+[A-Z][a-z]+ (Bridge|Highway)", paste(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem))
#mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) 
#View(table(unlist(str_split(all_bills$keywords, "; "))))

### Manual Fixes
all_bills[grepl('Public Officers And Employees|Administrative Rules|Appropriation|Trade Regulation', all_bills$keywords),]$commem <- 0
#all_bills[grepl("^Directs.+to name", all_bills$summary),]$commem <- 1

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()
# filter(all_bills, commem == 1 & grepl('acute', tolower(summary))  & !grepl('^directs (comm|dot|dep)', tolower(summary))) %>% select(summary) %>% as.data.frame()
# filter(all_bills, grepl('directs.+to designat.+ as', tolower(summary))) %>% select(term,title, summary, commem) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills)


#############
### Tennessee
#############

# *** TONS of mismatches using base terms but good keywords at start of abstract--> Significantly paring down the list ****

### Read ALL Bills
this_state <- "TN"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>%
  lapply(function(file) {
    df <- read.csv(file)
    if (nrow(df) > 1) {
      return(df)
    } else {
      return(NULL)
    }
  }) %>%
  bind_rows()

### Commmem Term Adjustments + Coding
#c_terms <- c(vw_commem, additions) # , 'renaming', designat
c_terms <- c('^state symbols', 'holidays and days of special observance', 'highways, roads and bridges - designates', '^flags',
             'highway signs -.+\\".+(bridge|highway|pathway|road|building|parkway)(\\"|,\\")', 'expresses intent to name',
             '^naming and designating', 'honors the memory', '^special license plates')
e_terms <- c(exclude, '^alcoholic beverag', 'after labor day', 'restricts use of state flag', 'provide paid and upaid time')


terms = c("2023_2024")

for(term2 in terms){
  cat("State: ", this_state, "\n Term: ",term2,"\n")
  sub_bills <- all_bills %>%
    filter(term == term2) %>% 
    mutate(session = recode(session, 'General Assembly' = 'RS', 'Special Session' = 'SS1', '1st Special Session' = 'SS1', '2nd Special Session' = 'SS2'),
           summary = ifelse(summary == 'Abstract summarizes the bill.', '', summary)
    ) %>%
    filter(grepl("^HB|^SB", bill_id)) %>%
    select(bill_id, term, session, title, abstract, summary) %>%
    distinct() %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(abstract)), 1, commem)) %>%
    #mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
    mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b|designat.+[A-Z][a-z]+ (Bridge|Highway)", paste(title, abstract)), 1, commem)) %>%
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(abstract)), 0, commem)) #%>%
  #mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) 
  
  ### Manual Fixes
  #all_bills[grepl("^Directs.+to name", all_bills$summary),]$commem <- 1
  if (as.numeric(substr(term2, 1, 4)) >= 2023){
    sub_bills <- sub_bills %>%
      mutate(commem = ifelse(grepl("legal holiday|state museum|requires.+flag|requires local education agencies|incorporates the language", tolower(abstract)), 0, commem))
  }
  
  # table(all_bills$term, all_bills$commem)
  # filter(all_bills, commem == 1 & grepl('acute', tolower(summary))  & !grepl('^directs (comm|dot|dep)', tolower(summary))) %>% select(summary) %>% as.data.frame()
  # filter(all_bills, grepl('directs.+to designat.+ as', tolower(summary))) %>% select(term,title, summary, commem) %>% View()
  
  ### Save
  write.csv(sub_bills, glue("{this_state}_Commem_Bills_{term2}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills, sub_bills)


#############
### TEXAS ####
#############
# *** SWitched coding of keywords a bit to better adjust for issues

### Read ALL Bills
this_state <- "TX"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]

all_bills <- read.csv(state_files[1], skipNul = TRUE) %>% mutate(session = as.character(session))
for(sf in state_files[2:length(state_files)]){
  session_bills <- read.csv(sf, skipNul = TRUE) %>% mutate(session = as.character(session))
  all_bills <- bind_rows(all_bills, session_bills)
}; rm(session_bills)
all_bills <- distinct(all_bills)

years = c(2023)
terms = 0.5 * (years - 2023) + 88

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'relating (to|to a) designat', 'relating to naming') 
c_terms <- c_terms[!grepl('to name', c_terms)]
e_terms <- c(exclude, 'hospital district', 'beneficiary', 'administrator', 'advisory comm', 'award program', 'terms of office',
             'commission\\b', 'manufactur', 'trust accounts', 'veto proc')


for(term in terms){
  cat("State: ", this_state, "\n Term: ",term,"\n")
  sub_bills <- all_bills %>%
    filter(startsWith(as.character(session),as.character(term))) %>% 
    rename(bill_id = bill_number) %>% 
    mutate(term = term,
           term_num = as.numeric(substring(session, 1, 2)),
           session_type = recode(substring(session, 3, nchar(session)), 'R' = 'RS', '1' = 'SS1', '2' = 'SS2', '3' = 'SS3', '4' = 'SS4', '5' = 'SS5', '6' = 'SS6', '7' = 'SS7'),
           session = paste0(term_num, '-', session_type)) %>% 
    filter(grepl('^HB|^SB', bill_id)) %>%
    select(bill_id, term, session, summary, keywords)  %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, 0)) %>%
    mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(summary)), 1, commem)) %>% 
    mutate(commem = ifelse(grepl('^holidays', tolower(keywords)), 1, commem)) %>%
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>%
    mutate(commem = ifelse(grepl('appropriation|state employees|crimes|property interests', tolower(keywords)), 0, commem)) %>%
    # Correct some non-commemorative bills in the 88th legislature
    mutate(commem = ifelse(grepl('school district security personnel|holocaust remembrance week instruction|office for health equity|state historical marker program|texas fruit and vegetable day|open burn pit registry fund|as a state holiday|administration of certain programs|monuments or memorials|criteria for awarding', tolower(summary)), 0, commem))
  
  # Keywords: ^holidays prevents a lot of error from say election/employee holidays
  # -- 'AWARDS, MEDALS, PRIZES' & 'State Symbols' have a substantial ammount of error
  write.csv(sub_bills, glue("{this_state}_Commem_Bills_{term}.csv"), row.names = FALSE)
  
  
  
}
### Clean




rm(this_state, state_files, c_terms, e_terms, all_bills, terms, sub_bills)

#############
### UTAH
#############

### Read ALL Bills
this_state <- "UT"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>%
  lapply(function(file) {
    df <- read.csv(file)
    if (nrow(df) > 1) {
      return(df)
    } else {
      return(NULL)
    }
  }) %>%
  bind_rows()

#### CLEAN

all_bills <- all_bills %>%
  mutate(yr = as.numeric(gsub('-.+', '', session)),
         term = ifelse(yr %% 2 == 1, paste0(yr, "_", yr + 1), paste0(yr - 1, "_", yr)),
         sub_num = ifelse(grepl("Substit", bill_id), str_trim(gsub('.+[0-9]+ ', '', bill_id)), 0),
         sub_num = recode(sub_num, 'Substitute' = '1', 'Second Substitute' = '2', 'Third Substitute' = '3', 'Fourth Substitute' = '4', 'Fifth Substitute' = '5', 'Sixth Substitute' = '6', 'Seventh Substitute' = '7'),
         sub_num = as.numeric(sub_num),
         bill_id = ifelse(as.numeric(substring(session, 1, 4)), gsub(' [A-Za-z]+ Substit.+| Substit.+', '', bill_id), bill_id))

#### FOR 1997 - 2001: DROP ALL BUT FINAL SUBSTITUTE -- See Estimation Script for details
orig_bills <- all_bills
all_bills <- all_bills[0,]
for(s in unique(orig_bills$session)){
  if(as.numeric(substring(s, 1, 4)) > 2001){
    these_bills <- filter(orig_bills, session == s)
  }else{
    these_bills <- filter(orig_bills, session == s) %>%
      group_by(bill_id) %>%
      filter(sub_num == max(sub_num)) %>%
      ungroup()
  }
  all_bills <- bind_rows(all_bills, these_bills)
}; rm(orig_bills, these_bills, s)

### CLEAN
all_bills <- all_bills %>%
  mutate(bill_id = paste0(gsub('\\.| .+', '', bill_id), str_pad(gsub('.+[A-Z]\\. ', '', bill_id), 4, pad = '0'))) %>%
  filter(grepl("^HB|^SB", bill_id)) %>%
  select(bill_id, term, session, title, description) %>%
  distinct()

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, '^observing', '^designating', '(highway|road|month|week|day|bridge) designation$',
             'designating.+(a|the) state', 'special license plate') # , 'renaming', designat
e_terms <- c(exclude, 'recodif', 'school week', 'militias', 'eight hour work day', 'opiate overdose', 'upstart',
             'access highway', 'frequent crash', 'state spaceport', 'next of kin', 'oversight')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(description)), 1, commem)) %>%
  #mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b|designat.+[A-Z][a-z]+ (Bridge|Highway)", paste(title, description)), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(description)), 0, commem)) #%>%
#mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem))

### Manual Fixes
all_bills[grepl("Bee Keeping Amendments", all_bills$title),]$commem <- 0
all_bills[grepl("State Firearm Designation", all_bills$title),]$commem <- 1

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()
# filter(all_bills, commem == 1 & grepl('acute', tolower(summary))  & !grepl('^directs (comm|dot|dep)', tolower(summary))) %>% select(summary) %>% as.data.frame()
# filter(all_bills, grepl('directs.+to designat.+ as', tolower(summary))) %>% select(term,title, summary, commem) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills)

#############
### VIRGINIA
#############

### Read ALL Bills
this_state <- "VA"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'designates.+(bridge|road|highway)', 'official state',
             'special license plates for supporters')
e_terms <- c(exclude, 'state board', 'sale', 'school calendar', 'property', 'standards', 'charter', 'codifies',
             'establishes the commission', '^commission', 'reimburs', 'resettl', 'emergency')

terms = c("2018_2019", "2020_2021", "2022_2023")

for(term in terms){
  cat("State: ", this_state, "\n Term: ",term,"\n")
  start_year = as.integer(substr(term,1,4))
  ### Clean
  sub_bills <- all_bills %>%
    filter(session_year %in% c(start_year, start_year+1)) %>% 
    mutate(term = gsub('-', '_', term)) %>%
    mutate(session_type = substring(session, 6, nchar(session)),
           session_type = recode(session_type, 'SESSION' = 'RS', 'SPECIAL SESSION I' = 'SS1', 'SPECIAL SESSION II' = 'SS2', 'SPECIAL SESSION III' = 'SS3', 'SPECIAL SESSION IV' = 'SS4'),
           session = paste0(session_year, '-', session_type)
    ) %>%
    filter(grepl('^HB|^SB', bill_id)) %>%
    select(bill_id, term, session, short_title, summary) %>% 
    distinct %>% 
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(short_title)), 1, 0)) %>%
    mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
    mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(short_title, summary)), 1, commem)) %>% 
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(short_title)), 0, commem)) %>%
    mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) 
  
  # filter(all_bills, commem == 1) %>% select(-session) %>% View()
  # filter(all_bills, commem == 1 & grepl('emergency', tolower(summary))) %>% select(short_title, summary) %>% as.data.frame() #%>% View()
  # filter(all_bills, grepl('official state', tolower(summary))) %>% select(term, short_title, summary, commem) %>% View()
  
  ### Save
  write.csv(sub_bills, glue("{this_state}_Commem_Bills_{term}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills)

#############
### VERMONT
#############

### Read ALL Bills
this_state <- "VT"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read.csv) %>% bind_rows

#### CLEAN
all_bills <- all_bills %>%
  mutate(term = gsub("\\_RS|\\_SS", "", session),
         term_num = as.numeric(substring(term, 1, 4)),
         term = case_when(nchar(term) == 4 & term_num %% 2 == 0 ~ paste0(term_num - 1, "_", term),
                          nchar(term) == 4 & term_num %% 2 == 1 ~ paste0(term, "_", term_num + 1),
                          nchar(term) != 4 ~ term),
         session = gsub('[0-9]+\\_', '', session)) %>%
  filter(grepl('^H[0-9]|^S[0-9]', bill_id)) %>%
  select(bill_id, term, session, title) %>%
  distinct()

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, "designat.+(bridge|highway|road|pathway|state park|crossing)", 
             '(special|distinctive|bicentennial).+vehicle plates', "designat(e|ing) (the|a portion)", 
             "designat.+(a|the) state", "as the state [a-z]+", "state (song|coin|sport|animal|bird)") 
e_terms <- c(exclude, 'designation of.+(funds|class 1|class 2|)', 'designat.+ (tuition|state aid)',
             'disclosure of honorar')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", title), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) 

### Manual Fixes
all_bills[grepl("ALPHONSE J\\. QUESNAL STATE AIRPORT", all_bills$title),]$commem <- 1
all_bills[grepl("JEFFREY HILL RODLIFF MEMORIAL ACT", all_bills$title),]$commem <- 1
all_bills[all_bills$title == 'An act relating to the Battle of Bennington Flag',]$commem <- 1
all_bills[grepl("DISCLOSURE OF HONORARIUMS|HONORARIA", all_bills$title),]$commem <- 0
all_bills[grepl("MARINE CORPS LEAGUE MOTOR VEHICLE PLATES", all_bills$title),]$commem <- 0
all_bills[grepl("legal holiday", all_bills$title), ]$commem <- 0

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()
# filter(all_bills, commem == 1 & grepl('acute', tolower(summary))  & !grepl('^directs (comm|dot|dep)', tolower(summary))) %>% select(summary) %>% as.data.frame()
# filter(all_bills, grepl('bridge', tolower(title))) %>% select(term, title, commem) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills, t)


#############
### Washington
#############

### Read ALL Bills
this_state <- "WA"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  rename(bill_id = bill_number) %>%
  mutate(term = gsub('-', '_', term),
         session = term) %>%
  filter(bill_type == 'Bill') %>%
  select(bill_id, term, session, title, summary, approp_bill) %>%
  mutate(approp_bill = ifelse(approp_bill == TRUE, 1, 0))

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, '^designating (an |a |the)', '^recogniz', '^declaring.+day', 'special.+license plate') 
e_terms <- c(exclude, 'honors classes', '^clarifying', '^contributing', 'teaching degrees', 'pistol permit', 'design-build', 
             'changing provisions', '^defining', 'salary', 'school purposes', 'help kids speak', 'management of', 'works account', 
             'disclosure', 'shelter care', 'survivor benefits', 'renewable energy', 'adoptive parents', 'state of emergency', 'local jobs') 

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title, summary)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>%
  mutate(commem = ifelse(approp_bill == 1, 0, commem))

table(all_bills$term, all_bills$commem) 
# filter(all_bills, commem == 1) %>% select(-c(session, approp_bill)) %>% View()
# filter(all_bills, commem == 1 & grepl('^authoriz', tolower(summary))) %>% select(summary) %>% as.data.frame() #%>% View()
# filter(all_bills, grepl('awareness (day|week|month)', tolower(summary))) %>% select(term, summary, commem) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}

rm(this_state, state_files, c_terms, e_terms, all_bills)


#############
### Wisconsin
#############

### Read ALL Bills
this_state <- "WI"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read_csv, col_types = cols()) %>% bind_rows 

### Clean
all_bills <- all_bills %>%
  rename(bill_id = bill_number) %>%
  mutate(bill_id = toupper(bill_id),
         term = paste0(session, '_', session + 1),
         session = session_type
  ) %>%
  filter(grepl('^AB|^SB', bill_id)) %>%
  select(bill_id, term, session, summary) 

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'relating to: designat', 'special.+license plate')
e_terms <- c(exclude, 'election', 'reimburs', 'technology zone', 'health care', 'authorizing', 'absence')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, 0)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(summary)), 1, commem)) %>% 
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) 

table(all_bills$term, all_bills$commem) 
# filter(all_bills, commem == 1) %>% select(-session) %>% View()
# filter(all_bills, commem == 1 & grepl('authoriz', tolower(summary))) %>% select(summary) %>% as.data.frame() #%>% View()
# filter(all_bills, grepl('observance', tolower(summary))) %>% select(term, summary, commem) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills)


#############
### West Virginia
#############

### *** Tons of Mismatches --> Dropping certain of keywords ********

### Read ALL Bills
this_state <- "WV"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(read.csv) %>% bind_rows

#### CLEAN
all_bills <- all_bills %>%
  rename(bill_id = bill_number) %>%
  mutate(term = ifelse(session_year %% 2 == 1, paste0(session_year, "_", session_year + 1), paste0(session_year - 1, "_", session_year)),
         session_type = recode(session_type, '1x' = 'SS1', '2x' = 'SS2', '3x' = 'SS3', '4x' = 'SS4', '5x' = 'SS5', '6x' = 'SS6', '7x' = 'SS7'),
         session = paste0(session_year, "-", session_type)
  ) %>%
  filter(grepl('^HB|^SB', bill_id)) %>%
  select(bill_id, term, session, summary, subjects) %>%
  distinct()

### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, '^designat.+(official state|\\")', '^naming', '^declaring.+holiday') #
c_terms <- c_terms[!grepl('dedicating|memoria|proclam', c_terms)]
e_terms <- c(exclude, 'honorariums', 'rental fee', 'statewide registry', 'sunny day fund', 'retirement system', 'retirement plan', 'retirement -- public employees',
             'absentee voting', 'honors funerals', 'annual fee', 'adult day care', 'handgun permit', '^authorizing', 'misdemeanor',
             'publication requirement', 'anniversary fund', 'relating to crime')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(summary)), 1, 0)) %>%
  #mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b|designat.+[A-Z][a-z]+ (Bridge|Highway)", paste(summary)), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(summary)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(subjects)), 0, commem))

### Manual Fixes
all_bills[grepl("Designating.+official state language", all_bills$summary),]$commem <- 0
all_bills[grepl("Renaming Industrial Home for Youth as Salem Correctional Center.+under Division of", all_bills$summary),]$commem <- 0
all_bills[grepl('Recognizing outstanding students', all_bills$summary),]$commem <- 1

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()
# filter(all_bills, commem == 1 & grepl('acute', tolower(summary))  & !grepl('^directs (comm|dot|dep)', tolower(summary))) %>% select(summary) %>% as.data.frame()
# filter(all_bills, grepl('directs.+to designat.+ as', tolower(summary))) %>% select(term,title, summary, commem) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills)


#############
### Wyoming
#############

### Read ALL Bills
this_state <- "WY"
state_files <- list.files(state_folders[grepl(glue('{this_state}$'), state_folders)], full.names = TRUE)
state_files <- state_files[grepl('Bill_Details', state_files)]
all_bills <- state_files %>% lapply(function(x) read.csv(x,colClasses = "character")) %>% bind_rows

#### CLEAN
all_bills <- all_bills %>%
  rename(bill_id = bill_number) %>%
  mutate(session_year = as.integer(substr(session,1,4)),
         term = ifelse(session_year %% 2 == 1, paste0(session_year, "_", session_year + 1), paste0(session_year - 1, "_", session_year)),
         shortTitle = gsub('\\.$', '', shortTitle)
  ) %>%
  filter(grepl('^HB|^SF', bill_id)) %>%
  select(bill_id, term, session, title, shortTitle) %>%
  distinct()


### Commmem Term Adjustments + Coding
c_terms <- c(vw_commem, additions, 'state (song|coin)', "special license plate", 'declaring.+official', "^designating.+(a|the) state", 
             "designating.+(bridge|highway|road|pathway|state park)") # 
e_terms <- c(exclude, 'school starting date', 'horse program', 'game and fish licenses', 'scholarship eligibility', 'county memorial hospital',
             'executive orders', 'complimentary')

all_bills <- all_bills %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(title)), 1, 0)) %>%
  mutate(commem = ifelse(grepl(paste(c_terms, collapse = "|"), tolower(shortTitle)), 1, commem)) %>%
  mutate(commem = ifelse(grepl("[A-Z][a-z]+ Day\\b|[A-Z][a-z]+ Week\\b|[A-Z][a-z]+ Month\\b", paste(title, shortTitle)), 1, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(title)), 0, commem)) %>%
  mutate(commem = ifelse(grepl(paste(e_terms, collapse = "|"), tolower(shortTitle)), 0, commem)) 

### Manual Fixes
#all_bills[grepl("Bee Keeping Amendments", all_bills$title),]$commem <- 0

table(all_bills$term, all_bills$commem)
# filter(all_bills, commem == 1) %>% View()
# filter(all_bills, commem == 1 & grepl('acute', tolower(summary))  & !grepl('^directs (comm|dot|dep)', tolower(summary))) %>% select(summary) %>% as.data.frame()
# filter(all_bills, grepl('directs.+to designat.+ as', tolower(summary))) %>% select(term,title, summary, commem) %>% View()

### Save
for(t in c("2023_2024")){
  write.csv(all_bills %>% filter(term == t), glue("{this_state}_Commem_Bills_{t}.csv"), row.names = FALSE)
}
rm(this_state, state_files, c_terms, e_terms, all_bills)

