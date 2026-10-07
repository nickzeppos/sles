
###################
### Generic Function to Code How Far a Specific Bill Progressed in the Legislative Process
#################

# bill_history <- hist_sub  ### = DF with each action item for a bill on a unique row
# aic_terms <- aic_t
# abc_terms <- abc_t
# pc_terms <- pc_t
# law_terms <- law_t
# this_id <- bills[i,]$bill_id
# this_term <- bills[i,]$term
# this_session <- bills[i,]$session
# this_sponsor <- bills[i,]$LES_sponsor

require(glue)

evaluate_bill_hist <- function(bill_history, this_id, this_term, this_session, this_sponsor,
                               aic_terms, abc_terms, pc_terms, law_terms, 
                               ignore_chamber_switch = FALSE, add_chamb = NULL, nebraska = FALSE){
  
  if(length(this_session) > 1){
    print(glue("Coding Bill History: Multiple Sessions Identified for a Bill! ---> Term: {this_term}, Bill: {this_id}"))
    break
  }
  
  ### Lowercase
  bill_history$action <- tolower(bill_history$action)
  bill_history = bill_history %>% arrange(order)
  # bill_history$action <- gsub("\\.", "", bill_history$action)
  
  if(nrow(bill_history) > 0){
    
    ### Identify Chamber of Introduction
    # Note: Chamber variable should be coded as House for Assemblies before input
    if(substring(tolower(this_id), 1, 1) %in% c('s', 'h', 'a')){
      init_chamber <- ifelse(substring(tolower(this_id), 1, 1) == "s", "Senate", "House")
      other_chamber <- ifelse(init_chamber == "House", "Senate", "House")  
    }else if (grepl('SB|HB|AB', this_id)){
      ## For states with bill numbers that start with year (e.g., 1997-SB-0123)
      init_chamber <- ifelse(grepl('SB', this_id), 'Senate', 'House')
      other_chamber <- ifelse(init_chamber == "House", "Senate", "House")
    }
    
    ##### Subset to Chamber of Introduction 
    # ** Either All actions in introducing chamber (ignore_chamber_switch == TRUE) 
    # ** Or all until it switched to out-chamber (ignore_chamber_switch == FALSE == the default) 
    if(ignore_chamber_switch == TRUE){
      if(!is.null(add_chamb)){ ## Adding a additional "chamber" (e.g., Joint)
        init_chamber <- append(init_chamber, add_chamb)  
      }
      chamber_history <- filter(bill_history, chamber %in% init_chamber )
    }else if(nebraska == TRUE){
      chamber_history <- bill_history
    }else{
      ### Subset to Introduction Chamber
      in_chamber <- which(bill_history$chamber == init_chamber)
      not_in_chamber <- which(bill_history$chamber == other_chamber)
      
      # Adjusting for outchamber actions before in-chamber action
      chamber_switch <- not_in_chamber[which(not_in_chamber > min(in_chamber))]
      if(length(chamber_switch) > 0){
        chamber_history <- bill_history[bill_history$order < min(chamber_switch),]
      }else{
        chamber_history <- bill_history
      }  
    }

    ### Action in Committee
    aic <- max(grepl(paste(aic_terms, collapse = "|"), chamber_history$action))
    
    ### Action beyond committee -- the chamber specific terms will only matter for each specific chamber history
    abc <- max(grepl(paste(abc_terms, collapse = "|"), chamber_history$action))
    
    ### Passed Chamber
    pc <- max(grepl(paste(pc_terms, collapse = "|"), chamber_history$action))
    
    ### Law
    law <- max(grepl(paste(law_terms, collapse = "|"), bill_history$action))
    
    ### Necessary Conditions
    # (1) If Law, must have received action beyond committee and passed chamber
    # (2) If passed chamber but not law, must have received action beyond committee
    if(law == 1){
      abc <- pc <- 1
    }else if(pc == 1){
      abc <- 1
    }
    
  } else {
    ## If no history items:
    aic <- abc <- pc <- law <- 0
    
  }
  
  ##### DF to Return
  bill_stages <- data.frame(bill_id = this_id, term = this_term, session = this_session, LES_sponsor = this_sponsor,
                            introduced = 1, action_in_comm = aic, action_beyond_comm = abc, passed_chamber = pc, law = law)
  return(bill_stages)
  
}

