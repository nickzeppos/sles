For anyone running the estimation scripts in the future, here is what you need to do. I assume that you have first run all the scrapers to extract the bill-level data as well as the scraper for the substantive & significant bills.

	1.	Do an initial scan of the Details and Histories spreadsheets to make sure it looks similar to the versions that were previously scraped. To do so, open the CSV files for the last iteration that was scraped (e.g., 2022) and one that is newly scraped (e.g., 2023) for each type (Details, History) and visually confirm that they are formatted the same ways and the new version is not missing any data. For example, the order variable might need to be flipped if the website went from listing the most recent actions first to the earliest actions first. 
	2.	Look at any scraping notes, make sure there's nothing you need to manually change
	3.	Make sure all the substantive and significant bills merge in correctly. Ones that are missing should be things like committee-sponsored bills, for example. Check for extra merges. If you're doing the extra merge process and there are multiple options that it could apply to, need to look at PVS site and try to manually match it based on the dates to figure out the relevant session (especially in Texas, for example).
Note: if there are committee bills, need those to be filtered post S&S. I did this in relevant states I saw but may want to extend it if the case arises in other states. 
	4.	Look at the bill counts, see whether they track past years. In some cases, Peter has pasted the output in. Otherwise, you can read in the old data and generate the counts yourself. 
	5.	Merge in the Legiscan data using the fuzzy merge
	6.	Make sure all the names are unique in the legis_data dataset, no duplicates. Otherwise the estimation will break. You may need to clean the names in the bills data (way above) or you may need to edit the names in the Legiscan dataset. 
	7.	One method to check for fails: a = legis_data %>% group_by(num_sponsored_bills, num_cosponsored_bills) %>% mutate(count = n()) %>% filter(count > 1)  . look for incorrect matches with the same last name. If you get all NAs, it's likely because some of the commemoratives or SSs are NA. can also look at the actual sponsors, e.g., bills %>% filter(substr(bill_id,1,1)=="S") %>% pull(sponsor) %>% unique() %>% sort(). and then compare this to the set of sponsors you see in legis_data. 

PA: Need to figure out what num_only variable means
TX: need to figure out num_only too. skipping the stuff with specials around line 225… 
VA: similar issue. i think it's that the old data specified special or not. 

GA: Seems that everything that passes the chamber becomes law in 19-20, wasn't the case in 17-18. Manually checked, it looks legit… 

Need to ask Peter about the cases with these specials how he does S&S merge (e.g., Alaska)
Also ask Peter about the commented lines around the bill stages, what those mean


Nice-to-haves: 

all_bill_stages <- SS_term %>%   select(bill_id, term, session, SS) %>%  left_join(all_bill_stages, ., by = c('bill_id' = 'bill_id', 'term' = 'term', "session" = "session")) %>%  mutate(SS = ifelse(is.na(SS), 0, SS)) %>%  select(-session_adj)

this type of code is going to include bills in SS that we may not (for example, commemoratives, things that aren't HB or SB. the file we save doesnt matter too much for LES estimation so not a huge deal... 

Also a nice thing to get an RA to do: look at all the 0 LES legislators across states, make sure they actually served in the chamber that session. If not, manually remove them. 

Use Legiscan to make sure you got all the specials after you scraped. I missed Wyoming's!

note to connor about CT. when tied chamber, generate a copy of the bill and do for both.

moving forward, we will always call majority party the electoral majority. indies with the party they caucus with. 

need to add a variable for each independent on which party they caucused with. 

make sure there are no negative cosponsors

make sure cosponsr are never negative

when telling connor to do inexact join, make sure the chamber is right too! 

tell connor to repull legiscan for 2024 onwards bc i had to do it temporarily for jayce. anything modified after 5/1/24 on legiscan.

when you check if bill histories are off: right_join(bill_hist, all_bill_stages %>% filter(action_in_comm == 1 & action_beyond_comm == 0)) for example. or View(right_join(bill_hist, all_bill_stages %>% filter(introduced == 1 & action_in_comm == 0)) %>% group_by(bill_id, session) %>% filter(order == max(order)))

left connor a note in new jersey but he may not re-do NJ so leave for the next person

check bill counts against legiscan? i'm not sure i have time for this one, though.