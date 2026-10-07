

###########################################################################################
############## Function to Calculate LES Scores
############################################################################################
# 
# bill_data <- bills
# legislator_data <- legis_data
# session = t_yrs
# ss_weight = 10; reg_weight = 5; com_weight = 1; stage_weights = c(1,1,1,1,1)

calc_LES <- function(bill_data, legislator_data, session, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1)){
  
  #### Get Names Right in Bill File to Calculate Agg Stats
  bill_data$match_name <- NA
  bill_data$match_name <- as.character(bill_data$match_name)
  for(i in 1:nrow(legislator_data)){
    search_name <- gsub('\\)', '\\\\)', gsub('\\(', '\\\\(', legislator_data[i,]$data_name))
    if(legislator_data[i,]$chamber == "U"){
      matches <- grepl(search_name, bill_data[bill_data$chamber == "U",]$sponsor)
      bill_data[bill_data$chamber == "U",]$match_name[matches] <- legislator_data[i,]$sponsor
    }else if(legislator_data[i,]$chamber == "H"){
      matches <- grepl(search_name, bill_data[bill_data$chamber == "H",]$sponsor)
      bill_data[bill_data$chamber == "H",]$match_name[matches] <- legislator_data[i,]$sponsor
    } else{
      matches <- grepl(search_name, bill_data[bill_data$chamber == "S",]$sponsor)
      bill_data[bill_data$chamber == "S",]$match_name[matches] <- legislator_data[i,]$sponsor
    }
  }
  rm(search_name)
  
  if(nrow(filter(bill_data, is.na(match_name))) > 0){
    cat(' \n \n SPONSORS OF BILLS WITHOUT A MATCH IN LEGISLATOR DATA: \n ') 
    filter(bill_data, is.na(match_name)) %>% select(sponsor, chamber) %>% distinct() %>% print()
    cat(' \n .')
  }
  
  ### Save Coded Bill Files to SLES Directory
  this_state <- gsub(".+LES_By_State/", "", getwd())
  save_dir <- gsub("LES_By_State", "coded_bills_by_state", getwd())
  if(!dir.exists(save_dir)){ dir.create(save_dir) }
  save_data <- bill_data
  save_data$term <- session
  save_data <- save_data %>%
    rename(sles_sponsor = match_name) %>%
    inner_join(legislator_data %>% select(sponsor, data_name, klarner_id, chamber), 
               by = c("sles_sponsor" = "sponsor", "chamber"="chamber")) %>%
    mutate(chamber = ifelse(chamber == "S", "upper", "lower"),
           state = this_state) %>%
    select(any_of(c("state", "chamber", "term", "session", "bill_id", "sles_sponsor", "klarner_id",
                    "introduced", "action_in_comm", "action_beyond_comm", "passed_chamber", "law", "SS", "commem",
                    "short_title", "title", "keywords", "bill_url"))) %>%
    distinct()
  
  if(length(unique(save_data$session)) == 1){save_data$session <- NA_character_}
  if(length(unique(save_data$bill_url)) == 1){save_data$bill_url <- NA_character_}
  #glimpse(save_data)
  write.csv(save_data, paste0(save_dir, "/", this_state, "_", session, "_coded_bills.csv"), row.names = FALSE)
  
  ### Do Inverse Probability Weighting?
  inverse_prob <- ifelse(stage_weights[1] == "inverse_prob", TRUE, FALSE)
  
  ##### Bill Weights
  bill_data$bill_weight <- reg_weight
  bill_data$bill_weight <- ifelse(bill_data$commem == 1, com_weight, bill_data$bill_weight)
  bill_data$bill_weight <- ifelse(bill_data$SS == 1, ss_weight, bill_data$bill_weight)
  
  ### Output DF
  LES_dat <- data.frame(matrix(nrow = 0, ncol = 33))
  colnames(LES_dat) <- c("sponsor", "data_name", "klarner_name", "klarner_id", "session", "chamber", 
                         "LES", "LES_rank", 
                         "BILL_wshare", "AIC_wshare", "ABC_wshare", "PASS_wshare", "LAW_wshare",
                         "all_bills", "all_aic", "all_abc", "all_pass", "all_law", 
                         "ss_bills", "ss_aic", "ss_abc", "ss_pass", "ss_law", 
                         "s_bills", "s_aic", "s_abc", "s_pass", "s_law", 
                         "c_bills", "c_aic", "c_abc", "c_pass", "c_law"
                         )
  LES_dat = LES_dat %>% mutate_at(vars(sponsor,data_name,klarner_name,
                                       session,chamber), as.character)
    
  #### Loop though House, Senate
  for(c in unique(bill_data$chamber)){
    
    chamber_bills <- bill_data[bill_data$chamber == c,]
    chamber_legislators <- filter(legislator_data, chamber == c) 
    N <- nrow(chamber_legislators)
    
    ### Check if Missing Sponsors
    # which(!(chamber_bills$sponsor %in% unlist(str_split(chamber_legislators$data_name, "\\|"))))
    # unlist(str_split(chamber_legislators$data_name, "\\|"))[which(!(unlist(str_split(chamber_legislators$data_name, "\\|")) %in% chamber_bills$sponsor))]
    # chamber_bills[!(chamber_bills$sponsor %in% unlist(str_split(chamber_legislators$data_name, "\\|")))]
    # unlist(str_split(chamber_legislators$data_name, "\\|"))[!(unlist(str_split(chamber_legislators$data_name, "\\|")) %in% chamber_bills$sponsor)]

    ### Sums for each session
    BILL_denom <- sum(chamber_bills$bill_weight * chamber_bills$introduced)
    AIC_denom <- sum(chamber_bills$bill_weight * chamber_bills$action_in_comm)
    ABC_denom <- sum(chamber_bills$bill_weight * chamber_bills$action_beyond_comm)
    PASS_denom <- sum(chamber_bills$bill_weight * chamber_bills$passed_chamber)
    LAW_denom <- sum(chamber_bills$bill_weight * chamber_bills$law)
    
    ###### BY CHAMBER AND SESSION: Weight Stages by Inverse Probability of Success
    if(inverse_prob == TRUE){
      stage_weights <- rep(1, 5)
      stage_weights[1] <- 1 / (sum(chamber_bills$introduced) / nrow(chamber_bills))
      stage_weights[2] <- 1 / (sum(chamber_bills$action_in_comm) / nrow(chamber_bills))
      stage_weights[3] <- 1 / (sum(chamber_bills$action_beyond_comm) / nrow(chamber_bills))
      stage_weights[4] <- 1 / (sum(chamber_bills$passed_chamber) / nrow(chamber_bills))
      stage_weights[5] <- 1 / (sum(chamber_bills$law) / nrow(chamber_bills))
      
      # print(paste0("SESSION: ", session, " ---- Chamber: ", c, " ---- Stage Weights: "))
      # print(round(stage_weights, 2))
    }
    
    ##### Loop through members within each chamber-session
    for(i in 1:nrow(chamber_legislators)){
      
      sponsor_row <- chamber_legislators[i,]
      
      ## Getting the name(s) each sponsor is listed as in the data -- if NA, will be NA, and then coded 0 later
      if(grepl('\\|', sponsor_row$data_name)){
        sponsor_names <- unique(str_split(sponsor_row$data_name, "\\|")[[1]])
      } else{
        sponsor_names <- sponsor_row$data_name
      } 
      
      ## Subset to Bills the Member Sponsored
      sponsored_bills <- chamber_bills[chamber_bills$sponsor %in% sponsor_names,]
      if(nrow(sponsored_bills) == 0){
        LES_dat <- add_row(LES_dat, sponsor = sponsor_row$sponsor, data_name = sponsor_row$data_name, 
                           klarner_name = sponsor_row$klarner_name, klarner_id = sponsor_row$klarner_id, 
                           session = session, chamber = c, 
                           LES = 0, LES_rank = NA, 
                           BILL_wshare = 0, AIC_wshare = 0, ABC_wshare = 0, PASS_wshare = 0, LAW_wshare = 0,
                           all_bills = 0, all_aic = 0, all_abc = 0, all_pass = 0, all_law = 0, 
                           ss_bills = 0, ss_aic = 0, ss_abc = 0, ss_pass = 0, ss_law = 0, 
                           s_bills = 0, s_aic = 0, s_abc = 0, s_pass = 0, s_law = 0,
                           c_bills = 0, c_aic = 0, c_abc = 0, c_pass = 0, c_law = 0)
        # print(paste0(s, " --- ", c, " ---- ", i, ": *** ", 0, " ***"))
        next
      }

      ### Member-Level Shares for each stage of the process
      BILL_w <- stage_weights[1] * sum(sponsored_bills$bill_weight * sponsored_bills$introduced)
      AIC_w <- stage_weights[2] * sum(sponsored_bills$bill_weight * sponsored_bills$action_in_comm)
      ABC_w <- stage_weights[3] * sum(sponsored_bills$bill_weight * sponsored_bills$action_beyond_comm)
      PASS_w <- stage_weights[4] * sum(sponsored_bills$bill_weight * sponsored_bills$passed_chamber)
      LAW_w <- stage_weights[5] * sum(sponsored_bills$bill_weight * sponsored_bills$law)
      
      #### Shares
      BILL_wshare <- BILL_w/BILL_denom
      AIC_wshare <- AIC_w/AIC_denom
      ABC_wshare <- ABC_w/ABC_denom
      PASS_wshare <- PASS_w/PASS_denom
      LAW_wshare <- LAW_w/LAW_denom
      
      ### Counts of bills by sponsor for each of the 15 components + 5 totals
      sponsor_bill_counts <- sponsored_bills %>% 
        mutate(commem = coalesce(commem, 0),
               SS = coalesce(SS, 0)) %>% 
        summarize(
          all_bills = sum(introduced),
          all_aic = sum(action_in_comm),
          all_abc = sum(action_beyond_comm),
          all_pass = sum(passed_chamber),
          all_law = sum(law),
          ss_bills = sum(SS * introduced),
          ss_aic = sum(SS * action_in_comm),
          ss_abc = sum(SS * action_beyond_comm),
          ss_pass = sum(SS * passed_chamber),
          ss_law = sum(SS * law),
          s_bills = sum(ifelse(SS == 0 & commem == 0, introduced, 0)),
          s_aic = sum(ifelse(SS == 0 & commem == 0, action_in_comm, 0)),
          s_abc = sum(ifelse(SS == 0 & commem == 0, action_beyond_comm, 0)),
          s_pass = sum(ifelse(SS == 0 & commem == 0, passed_chamber, 0)),
          s_law = sum(ifelse(SS == 0 & commem == 0, law, 0)),
          c_bills = sum(commem * introduced),
          c_aic = sum(commem * action_in_comm),
          c_abc = sum(commem * action_beyond_comm),
          c_pass = sum(commem * passed_chamber),
          c_law = sum(commem * law))
      
      #### Summing for Total LES measure --- Note N/5 Adjustment done at each earlier stage
      # Standard
      adj_factor <- sum(stage_weights)
      LES <- N / adj_factor * (BILL_wshare + AIC_wshare + ABC_wshare + PASS_wshare + LAW_wshare)

      #### Record
      LES_dat <- add_row(LES_dat, sponsor = sponsor_row$sponsor, data_name = sponsor_row$data_name, 
                         klarner_name = sponsor_row$klarner_name, klarner_id = sponsor_row$klarner_id, 
                         session = session, chamber = c,
                         LES = LES, LES_rank = NA, 
                         BILL_wshare = BILL_wshare, AIC_wshare = AIC_wshare, ABC_wshare = ABC_wshare,
                         PASS_wshare = PASS_wshare, LAW_wshare = LAW_wshare,
                         all_bills = sponsor_bill_counts$all_bills, 
                         all_aic = sponsor_bill_counts$all_aic, 
                         all_abc = sponsor_bill_counts$all_abc, 
                         all_pass = sponsor_bill_counts$all_pass, 
                         all_law = sponsor_bill_counts$all_law, 
                         ss_bills = sponsor_bill_counts$ss_bills, 
                         ss_aic = sponsor_bill_counts$ss_aic, 
                         ss_abc = sponsor_bill_counts$ss_abc, 
                         ss_pass = sponsor_bill_counts$ss_pass, 
                         ss_law = sponsor_bill_counts$ss_law, 
                         s_bills = sponsor_bill_counts$s_bills,
                         s_aic = sponsor_bill_counts$s_aic, 
                         s_abc = sponsor_bill_counts$s_abc, 
                         s_pass = sponsor_bill_counts$s_pass, 
                         s_law = sponsor_bill_counts$s_law,
                         c_bills = sponsor_bill_counts$c_bills, 
                         c_aic = sponsor_bill_counts$c_aic, 
                         c_abc = sponsor_bill_counts$c_abc, 
                         c_pass = sponsor_bill_counts$c_pass, 
                         c_law = sponsor_bill_counts$c_law)
      # print(paste0(s, " --- ", c, " ---- ", sponsor_row$sponsor, ": ", LES))
    }
  }
  
  ### Calculate Individual Rank Within Each Chamber-Session
  LES_dat <- LES_dat %>%
    mutate(chamber = ifelse(tolower(chamber) == "h", "House", "Senate")) %>%
    group_by(chamber) %>%
    arrange(desc(LES), .by_group = TRUE) %>%
    mutate(LES_rank = 1:n()) %>%
    ungroup()

  return(LES_dat)
}




