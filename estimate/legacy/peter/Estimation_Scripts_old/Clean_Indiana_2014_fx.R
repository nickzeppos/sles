
########################
### Function to Clean Indiana in 2014
########################
# (1) Fix Missing Names by imputing from Actions
# (2) Standardize in a manner akin to 2013 (which uses old format)

require(tidyr)
require(dplyr)
require(stringr)

# dat <- bills
clean_IN_2014 <- function(dat){
  
  #########################################
  ### Fill in Missing Authors Using Actions
  #########################################
  
  bill_hist <- read.csv("~/Dropbox/Data/State Legislative Data/States/IN/IN_Bill_Histories_2014.csv")
  bill_hist$session <- "2014-RS"
  bill_hist$term <- t_yrs
  bill_hist <- rename(bill_hist, bill_id = bill_number)
  bill_hist <- arrange(bill_hist, session, bill_id, order) %>% mutate(action = tolower(str_trim(action)))
  bill_hist$chamber <- recode(toupper(bill_hist$chamber), "H" = "House", "S" = "Senate")
  
  #### b_id = "HB1010"; b_id = "SB214"
  dat$missing <- ifelse(dat$session == "2014-RS" & dat$authors == "", 1, 0)
  dat[dat$missing == 1 & dat$coauthors == "",]$coauthors <- NA
  dat[dat$missing == 1 & dat$cosponsors == "",]$cosponsors <- NA
  for(b_id in unique(dat[dat$session == "2014-RS",]$bill_id)){
    if(dat[dat$session == "2014-RS" & dat$bill_id == b_id,]$missing == 1){
      hist_rows <- filter(bill_hist, bill_id == b_id)
      author_row <- filter(hist_rows, grepl("^authored by ", action)) %>% 
        slice(1) %>% # Taking only first instance of authored by
        mutate(action = gsub("authored by |\\.$", '', action))
      if(grepl("representatives", author_row$action)){
        author_row$action <- gsub("representatives", "representative", author_row$action)
        author_row$action <- gsub(", and | and |, ", "; representative ", author_row$action)
        dat[dat$session == "2014-RS" & dat$bill_id == b_id,]$authors <- author_row$action
      }else if(grepl("senators", author_row$action)){
        author_row$action <- gsub("senators", "senator", author_row$action)
        author_row$action <- gsub(", and | and |, ", "; senator ", author_row$action)
        dat[dat$session == "2014-RS" & dat$bill_id == b_id,]$authors <- author_row$action
      }else{
        dat[dat$session == "2014-RS" & dat$bill_id == b_id,]$authors <- author_row$action
      }
    }
  }
  
  #########################################
  ### Standardize Names Across Years
  #########################################
  
  ## ** Skipping Missing = 1 as those already appear to be in shortened format... may need to do some cross-checking
  all_names <- filter(dat, session == "2014-RS" & missing != 1) %>%
    mutate(full_name = paste(authors, coauthors, sep = "; "),
           full_name = gsub("; $", '', full_name)) %>%
    filter(str_trim(full_name) != '') %>%
    select(full_name) %>% 
    tidyr::separate_rows(full_name, sep = "; ") %>%
    distinct() %>%
    mutate(chamber = ifelse(grepl("^rep\\.", full_name), "H", "S"),
           last_name = gsub('.+ ', '', full_name),
           match_name = ifelse(chamber == "H", paste0('representative ', last_name), paste0('senator ', last_name)))
  
  ### Find Duplicate Last Names
  # -- group_by(all_names, chamber, match_name) %>% summarize(n = n()) %>% arrange(desc(n))
  all_names[all_names$full_name == "rep. timothy brown",]$match_name <- 'representative t. brown'
  all_names[all_names$full_name == "rep. charlie brown",]$match_name <- 'representative c. brown'
  all_names[all_names$full_name == "rep. milo smith",]$match_name <- 'representative m. smith'
  all_names[all_names$full_name == "rep. vernon smith",]$match_name <- 'representative v. smith'
  all_names[all_names$full_name == "sen. patricia miller",]$match_name <- 'senator pat miller'
  all_names[all_names$full_name == "sen. pete miller",]$match_name <- 'senator pete miller'
  all_names[all_names$full_name == "sen. richard young",]$match_name <- 'senator r. young'
  all_names[all_names$full_name == "sen. r michael young",]$match_name <- 'senator m. young'
  all_names[all_names$full_name == "rep. mara candelaria reardon",]$match_name <- 'representative candelaria reardon'
  
  #### Fix 2014 Names
  # filter(dat, grepl("steuer", authors)) %>% select(authors, coauthors) %>% View()
  for(i in 1:nrow(all_names)){
    name_escape <- str_replace_all(all_names[i,]$full_name, "(\\W)", "\\\\\\1")
    dat$authors <- gsub(name_escape, all_names[i,]$match_name, dat$authors)
    dat$coauthors <- gsub(name_escape, all_names[i,]$match_name, dat$coauthors)
    rm(name_escape )
  }; rm(i)
  
  ### Pull Rep. / Sen. off of Coauthors
  dat$coauthors <- gsub("representative |senator ", '', dat$coauthors)
  
  #########
  ### Manual Fixes
  ##########
  # ---> Most of these appear to all be cases where actions included full name for whatever reason
  # ---> So skipping missing == 1 means they get skipped in parsing
  # filter(dat, grepl("arnold", authors)) %>% select(authors)
  
  ### J. and L. Arnold -- Different Chambers, don't need initial
  dat$authors <- gsub("arnold j|arnold l", "arnold", dat$authors)
  dat$coauthors <- gsub("arnold j|arnold l", "arnold", dat$coauthors)
  
  # filter(dat, grepl("kruse", authors)) %>% select(authors)
  dat$authors <- gsub("young r michael", "m. young", dat$authors)
  dat$authors <- gsub("young r", "r. young", dat$authors)
  dat$authors <- gsub("miller patricia", "pat miller", dat$authors)
  dat$authors <- gsub("miller pete", "pete miller", dat$authors)
  dat$authors <- gsub("carlin yoder", "yoder", dat$authors)
  dat$authors <- gsub("jean breaux", "breaux", dat$authors)
  dat$authors <- gsub("jean leising", "leising", dat$authors)
  dat$authors <- gsub("mark stoops", "stoops", dat$authors)
  dat$authors <- gsub("randall head", "head", dat$authors)
  dat$authors <- gsub("susan glick", "glick", dat$authors)
  dat$authors <- gsub("greg taylor", "taylor", dat$authors)
  dat$authors <- gsub("greg walker", "walker", dat$authors)
  dat$authors <- gsub("timothy lanane", "lanane", dat$authors)
  dat$authors <- gsub("timothy skinner", "skinner", dat$authors)
  dat$authors <- gsub("frye r", "frye", dat$authors)
  dat$authors <- gsub("dennis kruse", "kruse", dat$authors)
  # dat$authors <- gsub("zzzz", "zzzz", dat$authors)  
  dat[dat$authors == "representative smith",]$authors <- "representative m. smith" # http://iga.in.gov/legislative/2014/bills/house/1042#document-a40705fb
  
  return(dat)
}
