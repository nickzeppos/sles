
# sponsor_data <- all_sponsors
# klarner_data <- klarner_sub
# this_session <- s
# auto_code_fix_df <- acf_df

match_to_klarner <- function(sponsor_data, klarner_data, manual_fix_df, auto_code_fix_df, drop_df, this_session, spec_elec_codes){
  
  ### Prep Names + Subset
  klarner_data$klarner_name <- klarner_data$cand
  klarner_data <- mutate(klarner_data, chamber = ifelse(sen == 1, 'S', 'H')) %>%
    rename(elec_year = year) %>%
    mutate(partial_session = ifelse(etype %in% spec_elec_codes, 1, ifelse(is.na(etype), NA, 0)) ) %>%
    ### Eliminate Partial Rows for Senators with Partial and Non-Partial Terms (e.g., elected in 2011 special and then again in 2011 general for 2012-2013 term)
    ### Need to do it within chamber otherwise we drop switchers
    group_by(klarner_name, chamber) %>%
    filter(partial_session == max(partial_session)) %>%
    ungroup() %>%
    select(klarner_name, candid, elec_year, chamber, partial_session) %>% 
    arrange(elec_year) %>% 
    distinct()
  
  #### Drop Senators who left mid-term and don't (and shouldn't) show up in data
  klarner_data <- filter(klarner_data, !(paste(klarner_name, '---', this_session) %in% paste(drop_df$klarner_name, '---', drop_df$session)))
  
  ### Drop (Senate) Duplicates from special election wins -- Need to make sure it's not someone switching chambers
  drop_dup <- NULL
  if(any(duplicated(klarner_data$klarner_name))){
    duplicate_rows <- which(duplicated(klarner_data$klarner_name, fromLast = TRUE))
    for(row_num in duplicate_rows){
      d_name <- klarner_data[row_num,]$klarner_name
      if (nrow(distinct(select(klarner_data[klarner_data$klarner_name == d_name,], -elec_year) )) == 1){
        glue(' \n \n DROPPING DUPLICATE for {d_name} \n')
        drop_dup <- append(drop_dup, row_num)
      } 
    }
    if(!is.null(drop_dup)){
      klarner_data <- klarner_data[-drop_dup,]; rm(drop_dup) 
    }
  }
  
  #### Make sure switchers (H --> S) are coded as both being partial
  if(any(duplicated(klarner_data$klarner_name))){
    klarner_data[klarner_data$klarner_name %in% klarner_data[which(duplicated(klarner_data$klarner_name)),]$klarner_name,]$partial_session <- 1
  }
  
  ### Find Matches
  k_matches <- fastLink(sponsor_data, klarner_data,
                        varnames = c("klarner_name"),
                        stringdist.match = c("klarner_name"),
                        partial.match = c("klarner_name"),
                        dedupe.matches = FALSE,
                        cut.a = .90,
                        threshold.match = .85)
  
  ### Extract Matches
  all_matches <- bind_cols(sponsor_data[k_matches$matches$inds.a,], klarner_data[k_matches$matches$inds.b,]) %>%
    rename(klarner_format = klarner_name, klarner_name = klarner_name1)
  
  #### Check Matches --- Both Accuracy AND Account for Missing
  # ---> If we skip this step, we wind up with a list of only the matches, when we want to keep the known legislators who we don't match
  # ---> These folks are either mismatched (poor name accuracy) OR deserve LES scores of 0
  #### *********** USE THE PRINTED RESULTS TO ADD TO MANUAL FIXES DF
  
  klarner_data$sponsor_match <- NA
  cat(" \n \n ~~~~ Checking Match Quality --- Printing Multi-Matches \n ")
  for(i in 1:nrow(klarner_data)){
    these_matches <- filter(all_matches, klarner_name == klarner_data[i,]$klarner_name)
    ## Single Matches
    if(nrow(these_matches) == 1 & !(klarner_data[i,]$klarner_name %in% manual_fix_df$klarner_name)){
      klarner_data[i,]$sponsor_match <- these_matches$full_name
    } else if(nrow(these_matches) == 1 & klarner_data[i,]$klarner_name %in% manual_fix_df$klarner_name){
    ## Matches that need to be supplemented with a second name from the data
      klarner_data[i,]$sponsor_match <- manual_fix_df[manual_fix_df$klarner_name == klarner_data[i,]$klarner_name,]$sponsor_match 
      print(paste0('FIXED: ', klarner_data[i,]$klarner_name, ' ---- Set as ', manual_fix_df[manual_fix_df$klarner_name == klarner_data[i,]$klarner_name,]$sponsor_match))
    } else if(nrow(these_matches) > 1){
      ### Manually Correct Multi-Matches
      if(klarner_data[i,]$klarner_name %in% manual_fix_df$klarner_name){
        ### Need to check these folks anyway to make sure a name isn't missing still
        fix_names <- manual_fix_df[manual_fix_df$klarner_name == klarner_data[i,]$klarner_name,]$sponsor_match
        klarner_data[i,]$sponsor_match <- fix_names
        print(paste0('SUBSET: ', klarner_data[i,]$klarner_name, ' ---- Set as ', fix_names, ' from set of ', paste(these_matches$full_name, collapse = '|')))
      } else{
        print(paste0(klarner_data[i,]$klarner_name, ' ---- ', paste(these_matches$full_name, collapse = '|')))
        klarner_data[i,]$sponsor_match <- paste(these_matches$full_name, collapse = '|')
      }
    } else if(klarner_data[i,]$klarner_name %in% manual_fix_df$klarner_name){
      fix_names <- manual_fix_df[manual_fix_df$klarner_name == klarner_data[i,]$klarner_name,]$sponsor_match
      klarner_data[i,]$sponsor_match <- fix_names
      print(paste0('FIXED: ', klarner_data[i,]$klarner_name, ' ---- Set as ', fix_names))  
    }
  }
  
  ##### If No JW Match, Match on Last Name
  klarner_data$last_name <- gsub(",.+", '', klarner_data$klarner_name)
  sponsor_data$first_initial <- str_sub(sponsor_data$first_name, 1, 1)
  
  cat(" \n \n ~~~~ Filling in Missing Matches using Last Name, First Initial \n ")
  ### Note: Accounting for errors in this auto-coding process with acf_df
  for(i in 1:nrow(klarner_data)){
    if(is.na(klarner_data[i,]$sponsor_match)){
      ln_match_rows <- grep(klarner_data[i,]$last_name, sponsor_data$last_name)
      if(length(ln_match_rows) > 1){
        first_initial <- gsub(', +', '', str_extract(klarner_data[i,]$klarner_name, ', +[A-Za-z]'))
        ln_match_rows <- ln_match_rows[which(sponsor_data[ln_match_rows,]$first_initial == first_initial)]
      }
      acf_match <- filter(auto_code_fix_df, klarner_name == klarner_data[i,]$klarner_name & session == this_session)
      if(nrow(acf_match) == 1){
        klarner_data[i,]$sponsor_match <- acf_match$sponsor_match
      }else if(length(ln_match_rows) == 1){
        klarner_data[i,]$sponsor_match <- sponsor_data[ln_match_rows,]$full_name
        print(paste0(klarner_data[i,]$klarner_name, " ----> ", klarner_data[i,]$sponsor_match))
        # Sys.sleep(2)
      } else {
        if(length(ln_match_rows) > 1){
          cat(paste0(' \n ****', klarner_data[i,]$klarner_name, " ----> STILL HAS MUTLIPLE MATCHES *** \n ."))
        }
      }
    }
  }
  
  #### Check/Fix Remaining Missing -- But these may be 0s
  klarner_data$drop <- 0
  for(i in 1:nrow(klarner_data)){
    if(is.na(klarner_data[i,]$sponsor_match) & klarner_data[i,]$klarner_name %in% manual_fix_df$klarner_name){
      klarner_data[i, ]$sponsor_match <- manual_fix_df[manual_fix_df$klarner_name == klarner_data[i,]$klarner_name,]$sponsor_match
      print(paste0('MANUAL MATCH: ', klarner_data[i,]$klarner_name, ' ---- Set as ', manual_fix_df[manual_fix_df$klarner_name == klarner_data[i,]$klarner_name,]$sponsor_match))
    } else if (is.na(klarner_data[i,]$sponsor_match) & klarner_data[i,]$elec_year < as.numeric(substring(this_session, 1, 4)) - 1 ){
      # Dropping Klarner Senators who won two election ago and have no match
      klarner_data[i,]$drop <- 1
    }
  }
  klarner_data <- filter(klarner_data, drop == 0) %>% select(-drop)
  
  #### Match Rate ~ 98% of klarner_data, (90% of known sponsors, with bulk of missing from most recent term)
  # sum(!is.na(klarner_data$sponsor_match))/nrow(klarner_data)
  # sum(sapply(sponsor_data$full_name, function(x) max(grepl(x, klarner_data$sponsor_match))))/nrow(sponsor_data)
  
  
  ### Check Names that SPONSORED A BILL but are not in Klarner --- Mostly a function of Special Elections
  #filter(bills, grepl('habeeb', sponsor))
  #filter(klarner, grepl('abeeb', cand) & sab == "VA") %>% select(year, sen, ddez, cand, etype)
  all_sponsor_matches <- unlist(str_split(na.omit(klarner_data$sponsor_match), '\\|'))
  missing_from_klarner <- filter(sponsor_data, !(full_name %in% all_sponsor_matches)) %>%
    select(full_name, last_name, klarner_name) %>%
    mutate(chamber = NA) %>%
    rename(sponsor_match = full_name, sponsor = klarner_name) # Klarner_name here is klarner_format
  
  #cat(glue(' \n\n  ~~~~ {nrow(missing_from_klarner)} KNOWN sponsors missing from Klarner data!'))
  if(nrow(missing_from_klarner) != 0){
    for(i in 1:nrow(missing_from_klarner)){
      spon_bills <- filter(bills, sponsor == missing_from_klarner[i,]$sponsor_match)
      missing_from_klarner[i,]$chamber <- unique(substring(spon_bills$bill_id, 1, 1))
    }
  }
  #-------------------------> This won't catch people who switched... 

  if(nrow(filter(klarner_data, is.na(sponsor_match))) != 0){
    cat(' \n \n ~~~ KLARNER CANDIDATES Without a Match: \n ')
    print(filter(klarner_data, is.na(sponsor_match)))
  }

  if(nrow(missing_from_klarner) != 0){
    cat(' \n \n ~~~ KNOWN SPONSORS Without a Match: \n ')
    print(missing_from_klarner)  
  }

  output <- bind_rows(mutate(klarner_data, sponsor = klarner_name), missing_from_klarner) %>%
    rename(data_name = sponsor_match, klarner_id = candid) %>%
    mutate(session = this_session) %>%
    select(sponsor, data_name, klarner_name, klarner_id, chamber, session, partial_session, elec_year) %>%
    arrange(chamber, sponsor)
    
  return(output) 
  
}