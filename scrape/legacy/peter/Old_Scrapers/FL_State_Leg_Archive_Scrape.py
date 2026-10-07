# -*- coding: utf-8 -*-
"""
Created on Mon Nov 12 17:00:06 2018

Scrape Florida Bill Archive Page

@author: PB
"""


############ NOTES:
# 
# **************** NO LONGER NEED TO USE THIS --- ARCHIVE BILLS ADDED TO MAIN SENATE SEARCH ***************************
#
#
### Formatting poor - text errors likely abound
#######################

import csv
import os
import requests
from bs4 import BeautifulSoup
import re
import time
#import json
import urllib

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/FL/')

#######################
##### Extract Session Links
########################

## GET SESSIONS
sessions = requests.get("http://archive.flsenate.gov/Session/")
session_soup = BeautifulSoup(sessions.content, "lxml")

years = session_soup.find("select", {"name":"Year"})
years = years.findAll("option")

year_dict = {}
for y in years:
    year_dict[y['value']] = y.text.strip()
    
## DROPPING PREVIOUSY SCRAPED TERMS
scrape_years = [sy for sy in year_dict if 'FL_Bill_Details_{}.csv'.format(sy) not in os.listdir('.')]
print("Dropping Previously Scraped Sessions: " + ', '.join([y for y in year_dict if y not in scrape_years]))

## Dropping 2006 Org Session - No Data
## Dropping 2000, 2002, 2004 Org Sessions -- 1 'bill' with no data
scrape_years = [sy for sy in scrape_years if sy not in ['2000O', '2002O', '2004O', '2006O']]

######################################
##### Function to Get All Session Bills
########################################
# ** House Bills are odd, senate bills even, it seems

#year_key = scrape_years[0]

def gather_bill_urls(cy_soup, c, yk, url_list):
    bill_table = cy_soup.find("table", {'width':475})
    info = bill_table.findAll('a', {'class' : 'Session'})
    for bill in info:
        url_list.append([yk, c, bill.text.strip().replace(' ', ''), 'http://archive.flsenate.gov/Session/' + bill['href']])
    return(url_list)
    
def scrape_session_bills(year_key):
    
    year_bill_urls = []
    
    for chamber in ["House", "Senate"]:
            
        print("\n ~~~ Getting URLs for {} Bills for the {} Session ~~~ ".format(chamber, year_key))            
            
        ### Base Page
        chamber_year_url = 'http://archive.flsenate.gov/Session/index.cfm?Year={}&Chamber={}&Tab=session&Submenu=1&Start=1'.format(year_key, chamber)
        chamber_year_page = urllib.request.urlopen(chamber_year_url, timeout = 10)
        chamber_year_soup = BeautifulSoup(chamber_year_page, 'lxml')
        time.sleep(1)
        
        ### Adjust for Error for Certain Terms?
        if 'An Error Has Occurred' in chamber_year_soup.get_text().strip():
            ### Seems to occur for 2010O when only 1 page of results and Start=1 is included
            chamber_year_url = 'http://archive.flsenate.gov/Session/index.cfm?Year={}&Chamber={}&Tab=session&Submenu=1'.format(year_key, chamber)
            chamber_year_page = urllib.request.urlopen(chamber_year_url, timeout = 10)
            chamber_year_soup = BeautifulSoup(chamber_year_page, 'lxml')
            time.sleep(1)
            
        ### Check to See if Bills Are Posted
        if 'No bills are available which meet your criteria' in chamber_year_soup.get_text().strip():
            print("  -----> No {} Bills!".format(chamber))
        else:
            
            ### Find Links to All Batches of Bills -- Need to Adjust to Account for Single Pages
            ### First here doesn't work... Sometimes need 3rd table not second...
            #all_chamber_year_urls = chamber_year_soup.findAll('table', {'width':125})[1].findAll('a', {'class' : 'Session'})
            all_chamber_year_urls = chamber_year_soup.find('td', text = re.compile("Jump To:")).findParents('table')[0]
            all_chamber_year_urls = all_chamber_year_urls.findAll('a', {'class' : 'Session'})  
            all_chamber_year_urls = ['http://archive.flsenate.gov/Session/' + url['href'] for url in all_chamber_year_urls]
        
            #### Gather URLS
            year_bill_urls = gather_bill_urls(chamber_year_soup, chamber, year_key, year_bill_urls)
            num_pages = len(all_chamber_year_urls) + 1
            num = 1
            print("  -- Page {} of {}".format(num, num_pages))
            for page in all_chamber_year_urls:
                cy_page = urllib.request.urlopen(page, timeout = 5)
                time.sleep(1)
                cy_soup = BeautifulSoup(cy_page, 'lxml')
                year_bill_urls = gather_bill_urls(cy_soup, chamber, year_key, year_bill_urls)
                num += 1
                print(" -- Page {} of {}".format(num, num_pages))
        
    #### Check if Empty
    if year_bill_urls == []:
        return("No Bills Available!")
    else:
        print("\n  -----> {} Bills Found for Session {}".format(len(year_bill_urls), year_key))
        return(year_bill_urls)
#
                
        
#########################################
### Function to get Bill Data
########################################        
# this_url = 'http://archive.flsenate.gov/Session/index.cfm?Mode=Bills&SubMenu=1&Tab=session&BI_Mode=ViewBillInfo&BillNum=0076&Chamber=Senate&Year=2007&Title=-%3EBill%2520Info%3AS%25200076-%3ESession%25202007'
# this_url = 'http://archive.flsenate.gov/Session/index.cfm?Mode=Bills&SubMenu=1&Tab=session&BI_Mode=ViewBillInfo&BillNum=0002&Chamber=Senate&Year=2007A&Title=-%3EBill%2520Info%3AS%25200002-%3ESession%25202007A'
# bill_row = session_bills[1038]
# year_key = scrape_years[0]

def get_bill_data(bill_row):
    this_year = bill_row[0]
    #this_chamber = bill_row[1]
    this_bill_num = bill_row[2]
    this_url = bill_row[3]
    
    try:
        bill_page = urllib.request.urlopen(this_url, timeout = 15)
    except:
        print(" ~~> Retrying Bill Request")
        time.sleep(30)
        bill_page = urllib.request.urlopen(this_url, timeout = 30)
    time.sleep(.5)
    
    ## Parse Bill Page
    bill_soup = BeautifulSoup(bill_page, 'lxml')        
    history = bill_soup.find("pre", {"class":"billhistory"})
    if history is None:
        if 'Not Used' in bill_soup.get_text():
            return('No Data')
            
    
    header = history.find('b')
    if header is None:
        history = bill_soup.find('img', {'align':'top'}).findParents('tr')[0].findNext('tr').find('td')
        #header = history.get_text().replace('\xa0', ' ')
        #header = re.split('\d{1,2}/\d{1,2}/\d{2} HOUSE|\d{1,2}/\d{1,2}/\d{2} SENATE', header)[0].strip()
        header = ''
        for line in list(history.children):
            if line.name == 'br':
                break
            else:
                try:
                    header = header + line.get_text()
                except:
                    header = header + line
        header = re.sub('\s+', ' ', header.replace('\xa0', ' '))
     
        br_tag = history.findAll('br')[0]
        title = re.sub(';.+', '', br_tag.next.strip())
        if this_bill_num == 'H0001A' and this_year == '2007A':
            header = 'H1-A    GENERAL BILL/CS/1ST ENG by Jobs & Entrepreneurship Council; Rivera; Grimsley; Seiler; A. Gibson; (CO-SPONSORS) Anderson; Galvano; Hasner; Needelman (Compare CS/CS/H 0003-A, CS/CS/1ST ENG/H 0005-A, H 0007-A, CS/1ST ENG/H 0009-A, H 0015-A, H 0017-A, H 0019-A, H 0027-A, 1ST ENG/S 0004-A, S 0012-A)'
            title = 'Hurricane Preparedness & Insurance [EPCC];'
        elif this_bill_num == 'S0012A' and this_year == '2007A':
            header = 'S12-A  GENERAL BILL by Fasano (Similar H 0015-A, Compare CS/1ST ENG/H 0001-A, H 0027-A, 1ST ENG/S 0004-A)'
            title = 'Insurance/Unfair or Deceptive Act [EPCC]'
        elif this_bill_num == 'H0045B' and this_year == '2003B':
            header = 'H45-B GB Captain Charles "Bo" Harrison Hwy.    GENERAL BILL/1ST ENG by Fiorentino; (CO-SPONSORS) Adams; Anderson; Bilirakis; Clarke; Dean; Littlefield; Murzin; Needelman; Russell'
            title = 'Captain Charles "Bo" Harrison Hwy.;'
        elif this_bill_num == 'H9139' and this_year == '2003':
            header = 'H9139  RESOLUTION by Kallinger; (CO-SPONSORS) Bullard; Planas'
            title = 'Rogers, "Mister" Fred;'     
        elif this_bill_num == 'S2704' and this_year == '2003':
            header = 'S2704 R Homestead "Heritage Day"  RESOLUTION by Bullard'
            title = 'Homestead "Heritage Day";'
        elif this_bill_num == 'H0473' and this_year == '2002':
            header = 'H473 GBC Use of "Chamber of Commerce" Term    GENERAL BILL/CS by Council for Smarter Government; Gardiner; (CO-SPONSORS) Harrell; Negron; Stansel; Waters; Kilmer; Gannon; Melvin; Green; Sobel; Paul; Benson (Similar CS/S 0712)'
            title = 'Use of "Chamber of Commerce" Term;'   
        elif this_bill_num == 'H9115' and this_year == '2002':
            header = "H9115 R Women's History Month/March 2002     RESOLUTION by Argenziano"
            title = "Women's History Month/March 2002;"             
        elif this_bill_num == 'S0712' and this_year == '2002':
            header = 'S712 GBC Use of "Chamber of Commerce" Term    GENERAL BILL/CS by Commerce and Economic Opportunities; Dyer; (CO-SPONSORS) Futch (Similar CS/H 0473)'
            title = 'Use of "Chamber of Commerce" Term;'   
        elif this_bill_num == 'S2198' and this_year == '2002':
            header = 'S2198 GBC Emergency Telephone Number "911"     GENERAL BILL/CS by Comprehensive Planning, Local and Military Affairs; Mitchell (Similar H 1263, Compare CS/1ST ENG/H 1511, CS/CS/1ST ENG/S 1610)'
            title = 'Emergency Telephone Number "911";'   
        elif this_bill_num == 'S2456' and this_year == '2002':
            header = 'S2456 R Thomas, Rex David "Dave"     RESOLUTION by King (Similar H 9097)'
            title = 'Thomas, Rex David "Dave";'
    elif this_bill_num in ['S1260', 'S2190', 'H1835', 'S0218', 'S2956', 'H1789', 'H1901', 'H1931', 'H9139', 'S0676', 'S1368', 'S1772', 'S0002D', 'S0480', 'S0728', 'S0998', 'S1214', 'S1590']:
        if this_bill_num == 'S1260' and this_year == '2005':
            header = 'S1260    GENERAL BILL/CS by Education; Wise; (CO-SPONSORS) Dockery  (Compare H 0437, 2ND ENG/H 6005, CS/CS/2ND ENG/S 2254, CS/CS/S 2480)'
            title = ') Education'
        elif this_bill_num == 'S2190' and this_year == '2005':
            header = 'S2190    GENERAL BILL/CS by Education; Wise; (CO-SPONSORS) Dawson; Posey; Dockery; Crist  (Compare H 0533)'
            title = ') Education'
        elif this_bill_num == 'H1835' and this_year == '2004':
            header = 'H1835    GENERAL BILL/2ND ENG by Appropriations; (CO-SPONSORS) Kyle  (Similar 1ST ENG/S 2500, Compare 2ND ENG/H 1837, 1ST ENG/S 2502)'
            title = 'Appropriations'
        elif this_bill_num == 'S0218' and this_year == '2004':
            header = 'S218     GENERAL BILL/CS/2ND ENG by Criminal Justice; Crist; (CO-SPONSORS) Lynn (Similar H 1713, Compare S 0698)'
            title = 'Criminal Justice'
        elif this_bill_num == 'S2956' and this_year == '2004':
            header = 'S2956    GENERAL BILL/CS by Comprehensive Planning; Bennett  (Similar 2ND ENG/H 0143)'
            title = 'Comprehensive Planning'
        elif this_bill_num == 'H1789' and this_year == '2003':
            header = 'H1789    GENERAL BILL/1ST ENG by Appropriations; (CO-SPONSORS) Kyle  (Similar 1ST ENG/S 2500, Compare 1ST ENG/H 1791, S 1034, 1ST ENG/S 2502)'
            title = 'Appropriations;'
        elif this_bill_num == 'H1901' and this_year == '2003':
            header = 'H1901    GENERAL BILL by Health Care; (CO-SPONSORS) Attkisson; Farkas '
            title = 'Health Care;'
        elif this_bill_num == 'H1931' and this_year == '2003':
            header = 'H1931    GENERAL BILL by Health Care; (CO-SPONSORS) Farkas  (Compare 1ST  ENG/H 1105, CS/CS/3RD ENG/S 1252, CS/S 2132)'
            title = 'Health Care;'      
        elif this_bill_num == 'S0676' and this_year == '2003':
            header = 'S676     GENERAL BILL/CS/1ST ENG by Transportation; Sebesta; (CO-SPONSORS)  Bullard  (Similar CS/CS/S 2658, Compare H 0043, H 0443, 1ST ENG/H 1205,  1ST ENG/H 1373, H 1415, H 1425, 1ST ENG/H 1605, H 1773, H 1805,  CS/S 0406, CS/S 0986, CS/S 1084, CS/S 1088, CS/CS/S 1752, CS/2ND  ENG/S 1762, CS/S 2110, S 2494, CS/CS/S 2578, S 2720)'
            title = 'Transportation;'    
        elif this_bill_num == 'S1368' and this_year == '2003':
            header = 'S1368    GENERAL BILL/CS by Education; Webster; (CO-SPONSORS) Bullard  (Similar H 1443)'
            title = 'Education;'    
        elif this_bill_num == 'S1772' and this_year == '2003':
            header = 'S1772    GENERAL BILL/CS/2ND ENG by Education; Education  (Similar H 1831,  Compare 1ST ENG/H 0319, CS/CS/S 1006, CS/CS/S 1520, CS/CS/S 2738)'
            title = 'Education;'  
        elif this_bill_num == 'S0002D' and this_year == '2002D':
            header = 'S2-D     GENERAL BILL/CS by Education; Villalobos; (CO-SPONSORS) Sullivan  (Similar H 0001-D, H 0007-D)'
            title = 'Education;'   
        elif this_bill_num == 'S0480' and this_year == '2002':
            header = 'S480     GENERAL BILL/CS by Transportation; Sebesta  (Similar CS/1ST ENG/H 1839, Compare CS/2ND ENG/H 0757, CS/S 1214, S 1224)'
            title = 'Transportation;'      
        elif this_bill_num == 'S0728' and this_year == '2002':
            header = 'S728     GENERAL BILL/CS by Transportation; Latvala  (Similar H 0677, Compare CS/1ST ENG/H 0715, CS/2ND ENG/H 0757, CS/S 1214)'
            title = 'Transportation;'    
        elif this_bill_num == 'S0998' and this_year == '2002':
            header = 'S998     GENERAL BILL/CS/2ND ENG by Criminal Justice; Smith  (Similar H 1437, Compare S 1014, CS/S 1184)'
            title = 'Criminal Justice;' 
        elif this_bill_num == 'S1214' and this_year == '2002':
            header = 'S1214    GENERAL BILL/CS by Transportation; Sebesta  (Compare CS/2ND ENG/H 0035, CS/3RD ENG/H 0261, H 0383, H 0677, CS/1ST ENG/H 0715, CS/2ND ENG/H 0757, H 1181, H 1387, H 1521, CS/1ST ENG/H 1839, CS/S 0480, CS/CS/CS/1ST ENG/S 0502, S 0542, CS/S 0676, CS/S 0728, S 1224, CS/S 1524, CS/S 1532, S 2076, S 2148)'
            title = 'Transportation;'
        elif this_bill_num == 'S1590' and this_year == '2002':
            header = 'S1590    GENERAL BILL/CS/1ST ENG by Education; Villalobos  (Linked H 0271, Compare H 1435, H 1445, 1ST ENG/H 2017, 1ST ENG/S 0086, CS/S 0088, 1ST ENG/S 1540, CS/1ST ENG/S 1560, CS/CS/1ST ENG/S 1564, 1ST ENG/S 1572, CS/CS/1ST ENG/S 1586, CS/2ND ENG/S 1844, CS/1ST ENG/S 2022, CS/1ST ENG/S 2172, CS/1ST ENG/S 2488)'
            title = 'Education;'            
        else:
            header = header.text.strip().replace("\xa0", " ")
            title = history.find('u').get_text()    
    else:
        header = header.text.strip().replace("\xa0", " ")
        title = history.find('u').get_text()
           
    ### Bill Data
    bill_type = header.split(' by ', 1)[0]
    bill_type = re.split(r'\s\s+', bill_type, 1)[1] # Used to be \s+

    sponsors = header.split(' by ', 1)[1]
    # Note: CO-INTRODUCERS sometimes included.. Could Parse Further
    if '(CO-INTRODUCERS' in sponsors:
        primary_sponsors = re.split("\\(CO-INTRODUCERS\\)" , sponsors)[0]
        primary_sponsors = re.sub('; +$', '', primary_sponsors)
        if '(CO-SPONSORS' in sponsors:
            cosponsors = re.split("\\(CO-SPONSORS\\)" , sponsors)[1]
            cosponsors = cosponsors.strip().replace('  ', ' ')
        else:
            cosponsors = ''
    elif '(CO-SPONSORS' in sponsors:
        primary_sponsors = re.split("\\(CO-SPONSORS\\)" , sponsors)[0]
        primary_sponsors = re.sub('; $', '', primary_sponsors)
        cosponsors = re.split("\\(CO-SPONSORS\\)" , sponsors)[1]
        cosponsors = cosponsors.strip().replace('  ', ' ')
    else:
        primary_sponsors = sponsors
        cosponsors = ''
        
    #sponsors = header.text.split(' by ')[1].strip().replace("\xa0", " ")
    #sponsors = re.split("\\(Similar" , sponsors)[0]
      
    history_text = ' '.join(history.text.split())
    if 'EFFECTIVE DATE' in history_text:
        summary = history_text.split("EFFECTIVE DATE")[0].strip()    
        if len(summary.split(title)) == 1:
            history_text = re.sub('\s+', ' ', history_text)
            title = re.sub('\s+', ' ', title)   
        summary = title + summary.split(title)[len(summary.split(title)) - 1]  # Used to just be [1]
    elif this_bill_num == 'H9139' and this_year == '2003':
        summary = "Rogers, 'Mister' Fred; salutes Fred McFeely Rogers, one of America's consummate role models & educator of children & adults alike."
    else:
        summary = re.split('\d\d/\d\d/\d\d SENATE|\d\d/\d\d/\d\d HOUSE', history_text, 1)[0]
        summary = re.sub('.+{}'.format(header.split(' by ', 1)[1]), '', summary).strip()
        #summary = title + summary.strip()
    ## *** Could Still Yield Errors if there's a needed escape character in title
    
    ## Breaking Up Action History into Pieces...Splitting by Date-Chamber Text
    action_details = history.get_text().strip().replace("\xa0", ' ')
    action_details = ' '.join(action_details.split())
    action_details = re.split("(\d\d/\d\d/\d\d HOUSE)|(\d\d/\d\d/\d\d SENATE)", action_details)
    action_details = [i.strip() for i in action_details if i is not None]

    date_chamber_indices = [ i for i, word in enumerate(action_details) if word.endswith('HOUSE') or word.endswith('SENATE') ]
    
    bill_history = []
    order = 1
    for j in range(0, len(date_chamber_indices)):
        this_index = date_chamber_indices[j]
        if j == len(date_chamber_indices) - 1:
            next_index = len(action_details)
        else:
            next_index = date_chamber_indices[j + 1]
        
        this_date_chamber = action_details[this_index].split(' ')
        this_action = action_details[(this_index+1):next_index]
        this_action = ' '.join(this_action)
        bill_history.append([this_bill_num, this_year] + this_date_chamber + [this_action, order])
        order +=1
    
    ### Could Scrape Vote Links Here        
    vote_header = bill_soup.find('strong', text = re.compile('Vote History')).parent.parent    
    
    vote_table = vote_header.findNextSibling().find('table')
    vote_data = []
    if vote_table is not None:
        for row in vote_table.findAll('tr'):
            cells = row.findAll('td')
            vchamber = cells[0].get_text()
            if vchamber == 'Chamber:':
                continue
            rc_id = cells[1].get_text().strip()
            v_date = cells[3].get_text().strip()
            v_url = 'http://archive.flsenate.gov' + cells[5].find('a')['href']
            vote_data.append([this_bill_num, this_year, vchamber, rc_id, v_date, v_url])   
            
    #### Prep Return
    bill_details = [this_year, this_bill_num, bill_type, title, primary_sponsors, cosponsors, summary, this_url]    
    return([bill_details, bill_history, vote_data])
    
  
####################################
##### Loop Through Years (Sessions)
#######################################
# bill_row = session_bills[1394]
# bill = session_bills[894]
# this_year = scrape_years[0]
    
for this_year in scrape_years:
    
    print('\n ****** Now Scraping Data for the {} Sesssion *******'.format(this_year))
    
    ### Get Bill URLs    
    session_bills = scrape_session_bills(this_year)
    
    if session_bills == 'No Bills Available!':
        continue
        
    ### Output Lists
    all_bill_details = [['session', 'bill_num', 'bill_type', 'title', 'primary_sponsors', 'cosponsors', 'summary', 'bill_url']]
    all_bill_histories = [['bill_num', 'session', 'action_date', 'action_chamber', 'action', 'order']]
    all_vote_links = [['bill_num', 'session', 'vote_chamber', 'vote_id', 'vote_date', 'vote_url']]
    
    ### Get Bill Data
    num = 1
    total = len(session_bills)
    for bill in session_bills:
        bill_data = get_bill_data(bill)
        
        if bill_data == "No Data":
            print("\n ({}/{}) ******* NO DATA FOR {} ***** \n URL: {}\n\n".format(num, total, bill[2], bill[3])) #, bill[3]
            num += 1
            continue
        
        all_bill_details.append(bill_data[0])
        for hist in bill_data[1]:
            all_bill_histories.append(hist)
        
        if bill_data[2] != []:
            for vote in bill_data[2]:
                all_vote_links.append(vote)
        
        print("({}/{}) {} ~~~~ URL: {}".format(num, total, bill[2], bill[3])) #, bill[3]
        num += 1
        
    print("\n -------------- FINSIHED SESSION: {} ------------------ \n".format(this_year))        
  
    ### Save Session    
    with open("FL_Bill_Details_" + this_year + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(all_bill_details)
    
    with open("FL_Bill_Histories_" + this_year + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(all_bill_histories)
    
    if all_vote_links != []:
        with open("FL_Vote_URLs_" + this_year + ".csv", "w", newline = "") as f:
            writer = csv.writer(f)
            writer.writerows(all_vote_links)
    
print("\n ************** FLORIDA -- ALL SESSIONS COMPLETE ******************* \n")   

