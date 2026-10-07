
##########################################################################################
####### Code to Parse California Legislature "Pubinfo" Files
###########################################################################################
## **** Files can be downloaded from: https://downloads.leginfo.legislature.ca.gov/
## **** Note: Full script only works from 1999 forward; earlier files record CHAPTERED BILLS ONLY + Lack history_tbl ****
############################################################################################

rm(list=ls())
options(stringsAsFactors = FALSE)
setwd(paste0(dirname(rstudioapi::getActiveDocumentContext()$path),"/../../States/CA/database_files"))

library(tidyverse)
library(XML)
# library(xml2)
library(jsonlite)
library(glue)

###### All Folders (Organized by Biennium)
all_folders <- list.files(recursive = FALSE)
all_folders <- all_folders[grepl("[1-2][0-9]+$", all_folders)]
all_folders <- all_folders[as.numeric(gsub(".+_", '', all_folders)) >= 2018]

###### Function to Read in .LOB File and Parse as XML and Return DF Row
# filepath = version_files[16925]
# xml_to_df(version_files[13895])
xml_to_df <- function(filepath){
  prog_bar$tick()#$print()
  xml_parse <- xmlTreeParse(filepath, useInternalNodes=TRUE)
  xml_root <- xmlRoot(xml_parse)
  xml_list <- xmlToList(xml_root, simplify = TRUE)
  if(!any(grepl("Description", names(xml_list)))){
    bill_id <- xml_list[[2]]
    vnum <- xml_list[[3]]
    title <- ifelse(length(xml_list[[7]]) == 1, xml_list[[7]], paste0(xml_list[[7]], collapse = " "))
    title <- gsub(' \\.$', '', str_trim(gsub(' +', ' ', gsub("NULL|\n", ' ', title))))
    topic <- ifelse(length(xml_list[[8]]$Subject) == 1, xml_list[[8]]$Subject, paste0(xml_list[[8]]$Subject, collapse = " "))
    topic <- gsub(' \\.$', '', str_trim(gsub(' +', ' ', gsub("NULL|\n", ' ', topic))))
    bill_num <- ifelse(as.numeric(xml_list[[5]]$SessionNum) == 0, paste0(xml_list[[5]]$MeasureType, "-", xml_list[[5]]$MeasureNum),
                       paste0(xml_list[[5]]$MeasureType, "X", xml_list[[5]]$SessionNum, "-", xml_list[[5]]$MeasureNum))
  }else{
    bill_id <- ifelse(length(xml_list$Description$Id) != 1, xml_list$Description$Id$text, xml_list$Description$Id)
    vnum <- xml_list$Description$VersionNum
    title <- ifelse(length(xml_list$Description$Title) == 1, xml_list$Description$Title, paste0(xml_list$Description$Title, collapse = " "))
    title <- gsub(' \\.$', '', str_trim(gsub(' +', ' ', gsub("NULL|\n", ' ', title))))
    topic <- ifelse(length(xml_list$Description$GeneralSubject$Subject) == 1, xml_list$Description$GeneralSubject$Subject, paste0(xml_list$Description$GeneralSubject$Subject, collapse = " "))
    topic <- gsub(' \\.$', '', str_trim(gsub(' +', ' ', gsub("NULL|\n", ' ', topic))))
    bill_num <- ifelse(as.numeric(xml_list$Description$LegislativeInfo$SessionNum) == 0, 
                       paste0(xml_list$Description$LegislativeInfo$MeasureType, "-", xml_list$Description$LegislativeInfo$MeasureNum),
                       paste0(xml_list$Description$LegislativeInfo$MeasureType, "X", xml_list$Description$LegislativeInfo$SessionNum, "-", xml_list$Description$LegislativeInfo$MeasureNum))
    
  }
  df_row <- data.frame(bill_id = bill_id, bill_num = bill_num, version_num = vnum, title = title, topic = topic)
  return(df_row)
}



########################################################
####### Loop Through Folders, Clean Data to Standardized Format
#######################################################

# folder = all_folders[7]
parsed_dir = paste0(dirname(rstudioapi::getActiveDocumentContext()$path),
                    "/../../States/CA/database_files/parsed_db_files")

for(folder in all_folders){
  
  #### TERM
  term <- gsub('.+_', '', folder)
  term <- paste(term, as.numeric(term) + 1, sep = "_" )
  save_yrs <- paste(substring(term, 3, 4), substring(term, 8, 9), sep = "_")
  if(glue('CA_Bill_Details_{save_yrs}.csv') %in% list.files(parsed_dir)){
    print(glue(" ****** SKIPPING {term} ~~> Term Already Parsed****** "))
    next
  }else{
    print(glue(" ****** Now Parsing... {term} ****** "))
  }
  
  #### Change to Correct Folder
  setwd(folder)
  # table(gsub("[0-9]+", "", list.files()))
  
  #### Load Bill DF + Create New Vars
  ## ** For all DFs, open pubinfo_load sql files in text edit to see column names ****
  bill_tbl <- read.delim("BILL_TBL.dat", header=FALSE, quote = "`")
  ## Accounting for "Extra Sessions" in bill_num (e.g., ABX1-1); Governor can introduce bills --> chamber = "G"
  bill_tbl <- bill_tbl %>%
    mutate_all(~replace(., . == "NULL", NA)) %>%
    rename(bill_id = V1, status = V18, location = V17, chapter_num = V10, latest_version_id = V11, version_status = V6) %>%
    mutate(bill_num = ifelse(V3 == 0, paste0(V4, "-", V5), paste0(V4, "X", V3, "-", V5)),
           term = paste0(substring(V2, 1, 4), "_", substring(V2, 5, 8)),
           chamber = ifelse(substring(bill_num, 1, 1) %in% c("A", "H"), "H", substring(bill_num, 1, 1)))

  #### Read In ALL BILL-SPECIFIC VERSION Files, Drop all but the INTRODUCED VERSION of the Bill
  version_files <- sort(list.files(pattern = "BILL_VERSION_TBL_"))
  prog_bar <- progress::progress_bar$new(format = "[:bar] :current/:total (:percent)", total = length(version_files))
  vf_tbl <- version_files %>%
    purrr::map_df(xml_to_df) %>%
    group_by(bill_num) %>%
    filter(version_num == max(version_num)) %>%
    ungroup() %>%
    rename(vf_bill_id = bill_id)
  rm(prog_bar)
  
  #### Load History DF
  if(as.numeric(substring(term, 1, 4)) >= 1999){
    hist_tbl <- read.delim("BILL_HISTORY_TBL.dat", header=FALSE, quote = "`")
    hist_tbl <- hist_tbl %>%
      mutate_all(~replace(., . == "NULL", NA)) %>%
      rename(bill_id = V1, hist_id = V2, date = V3, action = V4, order = V7, action_code = V8, 
             chamber = V10, location = V11, end_status = V13) %>%
      left_join(., select(bill_tbl, bill_id, bill_num, term), by = "bill_id") %>%
      mutate(date = format(as.Date(date), "%Y-%m-%d"))
  }


  ### Committee Hearing and/or Agenda Tables --> Don't need these -- Included in History Records
  # comm_tbl <- read.delim("~/Desktop/pubinfo_2017/COMMITTEE_HEARING_TBL.dat", header=FALSE, quote = "`")
  # #comm_tbl <- read.delim("~/Desktop/pubinfo_2017/COMMITTEE_AGENDA_TBL.dat", header=FALSE, quote = "`")
  # comm_tbl <- dplyr::mutate_all(comm_tbl, ~replace(., . == "NULL", NA))
  
  #### Load Author DF
  # *** If want members of committees that sponsored, use V6
  authors_tbl <- read.delim("BILL_VERSION_AUTHORS_TBL.dat", header=FALSE, quote = "`")
  authors_tbl <- authors_tbl %>%
    mutate_all(~replace(., . == "NULL", NA)) %>%
    rename(version_id = V1, spon_type = V2, spon_chamber = V3, short_name = V4, spon_role = V5, primary_spon_flag = V10) %>%
    mutate(primary_spon_flag = ifelse(primary_spon_flag %in% "Y", 1, 0))
  # V1 = Version ID (e.g., 20170SB58099INT, 20170SB58098ENR, 20170SB58097CHP)
  # V2 = [Legislator, Committee]; V3 = Chamber; V5 = Author_type
  
  #### Load Legislator Tablke
  # leg_tbl <- read.delim("LEGISLATOR_TBL.dat", header=FALSE, quote = "`")
  # leg_tbl <- leg_tbl %>% 
  #   mutate_all(~replace(., . == "NULL", NA)) %>%
  #   rename(district = V1, full_name = V3, chamber = V4, short_name = V5, first_name = V6, last_name = V7,
  #          middle_initial = V8, suffix = V9, title = V10, party = V12)
    
  ##############
  ### MERGE TABLES
  ##############
  
  ### Merge in Title/Topic/Description
  bill_tbl <- left_join(bill_tbl, vf_tbl, by = "bill_num")
  
  ### Merge in Authors
  authors_collapse <- authors_tbl %>%
    mutate(spon_chamber = dplyr::recode(spon_chamber, "ASSEMBLY" = "H", "SENATE" = "S"), 
           name_chamber = paste0(short_name, " (", spon_chamber, ")")) %>%
    group_by(version_id) %>%
    summarize(lead_author = paste(name_chamber[which(primary_spon_flag == 1)], collapse = "; "), # Should only be 1
              authors = paste(name_chamber[which(spon_role == "LEAD_AUTHOR")], collapse = "; "),
              coauthors = paste(name_chamber[which(spon_role %in% c("PRINCIPAL_COAUTHOR", "COAUTHOR"))], collapse = "; ")) %>%
    ungroup()
  
  bill_tbl <- bill_tbl %>% 
    mutate(vf_bill_id_adj = gsub("_+0+|_+", '', vf_bill_id)) %>%
    left_join(authors_collapse, by = c("vf_bill_id_adj" = "version_id"))
  
  #### Subset to Relevant Columns
  bill_tbl <- select(bill_tbl, bill_num, bill_id, term, chamber, title, lead_author, authors, coauthors, topic, status, chapter_num, location)
  if(as.numeric(substring(term, 1, 4)) >= 1999){
    hist_tbl <- select(hist_tbl, bill_num, bill_id, hist_id, term, chamber, date, order, action, action_code, location, end_status)
  }
  
  ###### SAVE
  #save_yrs <- paste(substring(term, 3, 4), substring(term, 8, 9), sep = "_")
  write.csv(bill_tbl, glue("../parsed_db_files/CA_Bill_Details_{save_yrs}.csv"), row.names = FALSE)
  if(as.numeric(substring(term, 1, 4)) >= 1999){
    write.csv(hist_tbl, glue("../parsed_db_files/CA_Bill_Histories_{save_yrs}.csv"), row.names = FALSE)
    rm(hist_tbl)
  }
  
  print(glue(" ****** {term} Term Parsed and Saved! ****** "))
  rm(bill_tbl, term, save_yrs, version_files, authors_tbl, authors_collapse, vf_tbl)
}

rm(all_folders, folder, xml_to_df)
