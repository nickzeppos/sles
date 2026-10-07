# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape Virginia Bills by Session

@author: PB
"""

import csv
import os
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
# import numpy as np
import time
from unicodedata import normalize

# import requests
os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/VA/')

########################
##### SCRAPE BILL DETAILS
########################
### ** Run This to Get Individual Bill Links
### ** In future should adapt to just get most recent/incomplete terms

#
#### Function to Scrape a Page
#def scrape_bill_list(this_url, year_num, session_num):
#    this_data = []
#    this_html = urlopen(this_url).read()
#    this_soup = BeautifulSoup(this_html, "lxml")   
#    
#    check_response = this_soup.body.findAll(text=re.compile('Sorry, your request could not be processed at this time'))
#    if(check_response != [] and check_response[0].strip() == 'Sorry, your request could not be processed at this time.'):
#        return 'No More Sessions'
#    
#    #session = session_num 
#    session = this_soup.find("div", {"id": "mainC"}).findChildren("h2")
#    session = session[0].get_text().strip()
#    
#    complete = 1    
#    
#    while complete != 'Finished':
#        
#        bills = this_soup.find_all("a", href=re.compile(year_num + session_num + "\\+sum"))
#        for bill in bills:
#            bill_url = 'http://lis.virginia.gov' + bill['href']
#            bill_number = bill.get_text()
#            bill_description = bill.parent.get_text().strip()  
#            this_data.append([bill_number, session, year_num, session_num, bill_description, bill_url]) 
#
#        print(year_num + "---" + session_num + "----" + str(complete))
#        
#        check_for_more = this_soup.find_all("a", href=re.compile(year_num + session_num + "\\+lst\\+ALL\\+"))
#        
#        if(len(check_for_more) == 2):
#            this_html = urlopen('http://lis.virginia.gov' + check_for_more[0]['href']).read()
#            this_soup = BeautifulSoup(this_html, "lxml")  
#            complete += 1
#            time.sleep(1)
#            
#        else:
#            complete = 'Finished'
#    
#    del this_soup, this_html, check_response, session,         
#    return this_data
#
#
##########
### LOOP INPUTS
#######
#
#### Create Year Strings to Iterate Over -- Just need to add session numbers
#years = list(range(94,100)) + list(range(0, 19))
#year_string = []
#for year in years:
#    year_string.append(str(year).zfill(2))
#
#del(years)
#
#### Web URL
#url_start = 'http://lis.virginia.gov/cgi-bin/legp604.exe?'
#url_end = '+lst+ALL'
#
#### List To Store Existing Data
#all_data = [['bill_id', 'session', 'year_num', 'session_num', 'short_title', 'bill_url']]
#
############
##### LOOP
#############
#
#for year in year_string:
#
#    print("-------------------------" + year + "-------------------------")     
#     
#    for session_iter in range(1, 10):
#    
#        this_session_url = url_start + year + str(session_iter) + url_end
#        session_data = scrape_bill_list(this_session_url, year, str(session_iter) )  
#        # print("Data Length = " + str(len(session_data))) # 16 = No More Sessions
#        
#        if(session_data == "No More Sessions"):
#            break
#        else:
#            all_data.extend(session_data)
#            time.sleep(2.5)   # Delay is important -- pages error otherwise
#            print(year + "-------------------------" + str(session_iter))   
#    #break
#

##### Save Large List
#with open("VA_Bill_Details.csv", "w", newline = "") as f:
#    writer = csv.writer(f)
#    writer.writerows(all_data)

# Read Data
with open("VA_Bill_Details.csv") as f:
    reader = csv.reader(f)
    all_data = [r for r in reader]

len(all_data)

########################
##### SCRAPE HISTORIES
########################
# ** Votes need to be scraped invidivudally... Note that the Details URLs are different for votes (so could identify them pre-scrape)

##### Output Lists
#'bill_id', 'session', 'year_num', 'session_num', 'short_title', 'bill_url'
all_histories = [['bill_id', 'session', 'date', 'chamber', 'action', 'action_order', 'action_details_url']]
all_data[0] = ['bill_id', 'session', 'year_num', 'session_num', 'short_title', 'bill_url', 'sponsor', 'house_sponsors', 'senate_sponsors', 'summary']
skipped = []

###### IF ERRORS CHECK 12598 --- IT LAGGED THERE

# all_data[i]
# all_histories[len(all_histories)-1]


# No Histories or Sponsors: 13249, 16188, 29282, 38218
# No Sponsors: 898, 

for i in range(1, len(all_data)):
 
    ### Run to fill in skipped data after done
    # if i not in skipped:
    #     continue
    
    try:
        bill_html = urlopen(all_data[i][5]).read()
        bill_soup = BeautifulSoup(bill_html, "lxml")    
    except:
        skipped.append(i)
        all_data[i] = all_data[i] + ['','','','']
        continue
    
    ### Trim
    page_body = bill_soup.findAll("div", {"id":"mainC"})
    
    ### Try Again if Scrape Call Failed
    if page_body == []:
        try:
            time.sleep(3)
            bill_html = urlopen(all_data[i][5]).read()
            bill_soup = BeautifulSoup(bill_html, "lxml")    
            page_body = bill_soup.findAll("div", {"id":"mainC"})
        except:
            skipped.append(i)
            all_data[i] = all_data[i] + ['','','','']
            continue

    ## Extract Details
    summary = bill_soup.findAll("h4", text=re.compile('SUMMARY'))
    if summary != []:
        try:
            summary = summary[0].findNext("p").get_text().replace("\r\n", " ").strip()
        except:
            summary = ''
    else:
        summary = 'NO SUMMARY'
    
    history = bill_soup.findAll("h4", text=re.compile('HISTORY'))
    history = history[0].findNext("ul", {"class":"linkSect"})  #.get_text().replace("\r\n", "").strip()
    history = history.findChildren("li")
    
    this_history = []
    order = 0
    for row in history:
        order += 1
        row_text = row.get_text().replace("\xa0", "").strip()
        row_parts = row_text.split(" " or ":", 2)
        this_date = row_parts[0]
        this_chamber = re.sub(":", "", row_parts[1]) 
        if len(row_parts) > 2:
            this_action = row_parts[2].replace("\r\n", "").strip()   
        else:
            this_action = 'No Action Text'

        a_tag = row.findChildren("a")
        if a_tag == []:
            action_details_url = 'NO DETAILS'
        else:
            action_details_url = 'http://lis.virginia.gov' + a_tag[0]['href']
 
        this_history.append(all_data[i][:2] + [this_date, this_chamber, this_action, order, action_details_url])
        
    # Add all History Items to Main File   
    all_histories.extend(this_history)
    print(str(i) + " ----- Legislative History Added")
    time.sleep(.75)
    
    ###########################################
    
    ### Sponsors --- Link is the same by has mbr instead of sum
    sponsor_url = re.sub("\+sum\+", "+mbr+", all_data[i][5])
    sponsor_html = urlopen(sponsor_url).read()
    sponsor_soup = BeautifulSoup(sponsor_html, "lxml")       
    
    ### Trim
    check_me = sponsor_soup.findAll("div", {"id":"mainC"})
    
    ### Try Again if Scrape Call Failed
    if check_me == []:
        try:
            time.sleep(3)
            sponsor_html = urlopen(sponsor_url).read()
            sponsor_soup = BeautifulSoup(sponsor_html, "lxml")       
        except:
            skipped.append(i)
            all_data[i] = all_data[i] + ['','','','']
            continue
    
    house_sponsors = []
    house_sponsors_matches = sponsor_soup.findAll("h4", text=re.compile('HOUSE PATRONS'))
    if house_sponsors_matches != []:
        for hp in range(0, len(house_sponsors_matches)):
            these_sponsors = house_sponsors_matches[hp].findNext("ul", {"class":"linkSect"})
            these_sponsors = these_sponsors.findChildren("li")
            for hm in range(0, len(these_sponsors)):
                house_sponsors.append(normalize("NFKD", these_sponsors[hm].get_text().strip()))
    
    senate_sponsors = []
    senate_sponsors_matches = sponsor_soup.findAll("h4", text=re.compile('SENATE PATRONS'))
    if senate_sponsors_matches != []:
        for sp in range(0, len(senate_sponsors_matches)):
            these_senators = senate_sponsors_matches[sp].findNext("ul", {"class":"linkSect"})
            these_senators = these_senators.findChildren("li")
            for sm in range(0, len(these_senators)):
                senate_sponsors.append(normalize("NFKD", these_senators[sm].get_text().strip()))    

    ### Will Catch Multi Sponsors in theory
    introducing_sponsor = "\n".join(s for s in house_sponsors + senate_sponsors if 'chief patron' in s.lower())
    
    ###### APPEND TO DETAILS FILE
    all_data[i] = all_data[i][0:6] + [introducing_sponsor, house_sponsors, senate_sponsors, summary]
    print(str(i) + " ----- Bill Sponsors Added")
    
    time.sleep(.75)
    #################################
    
print("Done! --- Remember to Save!!")


########### 
# Check Missing

#for i in range(1, len(all_data)):
#    if len(all_data[i]) == 6:
#        print(i)


#############
## SAVE EACH DF
#############
len(skipped)
    
#with open("VA_Bill_Details_Full.csv", "w", newline = "") as f:
#    writer = csv.writer(f)
#    writer.writerows(all_data)
#
#with open("VA_Bill_Histories.csv", "w", newline = "") as f:
#    writer = csv.writer(f)
#    writer.writerows(all_histories)
#    


################# OLD CODE
# *** JNEEDED TO ADJUST FOR MULTIPLE HOUSE/SENATE PATRONS ON SPONSOR PAGE ****
#    house_sponsors = sponsor_soup.findAll("h4", text=re.compile('HOUSE PATRONS'))
#    if house_sponsors != []:
#        house_sponsors = house_sponsors[0].findNext("ul", {"class":"linkSect"})
#        house_sponsors = house_sponsors.findChildren("li")
#    
#    senate_sponsors = sponsor_soup.findAll("h4", text=re.compile('SENATE PATRONS'))
#    if senate_sponsors != []:
#        senate_sponsors = senate_sponsors[0].findNext("ul", {"class":"linkSect"})
#        senate_sponsors = senate_sponsors.findChildren("li")
#    
#    stem = re.search('^(HB|HR|HJ|SB|SR|SJ) [0-9]', all_data[i][0], re.IGNORECASE).group(1)
#    
#    if stem == "HB" or stem == "HR" or stem == "HJ":
#        introducing_sponsor = house_sponsors[0].get_text().strip()
#        introducing_sponsor = normalize("NFKD", introducing_sponsor)
#        house_start = 1
#        senate_start = 0
#    elif stem == "SB" or stem == "SR" or stem == "SJ":
#        introducing_sponsor = senate_sponsors[0].get_text().strip()
#        introducing_sponsor = normalize("NFKD", introducing_sponsor)
#        house_start = 0
#        senate_start = 1
#    else:
#        introducing_sponsor = "UNKNOWN"
#        house_start = 0
#        senate_start = 0
#    
#    house_cospon = []
#    for j in range(house_start, len(house_sponsors)):
#        house_cospon.append(normalize("NFKD", house_sponsors[j].get_text().strip()))
#    
#    senate_cospon = []
#    for k in range(senate_start, len(senate_sponsors)):
#        senate_cospon.append(normalize("NFKD", senate_sponsors[k].get_text().strip()))
