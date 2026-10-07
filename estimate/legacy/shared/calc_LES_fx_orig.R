

###########################################################################################
############## Function to Calculate LES Scores
############################################################################################

# bill_data <- bills
# legislator_data <- legis_data
# session = t_yrs
# ss_weight = 10; reg_weight = 5; com_weight = 1; stage_weights = c(1,1,1,1,1)

calc_LES <- function(bill_data, legislator_data, session, ss_weight = 10, reg_weight = 5, com_weight = 1, stage_weights = c(1,1,1,1,1)){
  
  #### Get Names Right in Bill File to Calculate Agg Stats
  bill_data$match_name <- NA
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
  
  ### Do Inverse Probability Weighting?
  inverse_prob <- ifelse(stage_weights[1] == "inverse_prob", TRUE, FALSE)
  
  ##### Bill Weights
  bill_data$bill_weight <- reg_weight
  bill_data$bill_weight <- ifelse(bill_data$commem == 1, com_weight, bill_data$bill_weight)
  bill_data$bill_weight <- ifelse(bill_data$SS == 1, ss_weight, bill_data$bill_weight)
  
  ### Output DF
  LES_dat <- data.frame(matrix(nrow = 0, ncol = 13))
  colnames(LES_dat) <- c("sponsor", "data_name", "klarner_name", "klarner_id", "session", "chamber", "LES", "LES_rank", "BILL_wshare", "AIC_wshare", "ABC_wshare", "PASS_wshare", "LAW_wshare")
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
                           BILL_wshare = 0, AIC_wshare = 0, ABC_wshare = 0, PASS_wshare = 0, LAW_wshare = 0)
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
                         PASS_wshare = PASS_wshare, LAW_wshare = LAW_wshare)
      
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




