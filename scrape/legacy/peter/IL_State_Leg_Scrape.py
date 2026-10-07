#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created Dec 3, 2018

Scrape IL State Legislative Bills

@author: pb
"""

################## NOTES:
# http://www.ilga.gov/legislation/default.asp?GA=99
# Bills available for the 90th to 100th General Assembly (1997 - Present)

import csv
import os
import re
import time
import urllib
from bs4 import BeautifulSoup

#from selenium import webdriver
#from selenium.webdriver.chrome.options import Options  
#from selenium.webdriver.support.ui import Select
# import selenium.webdriver.support.ui as ui
# from selenium.webdriver.support import expected_conditions as EC


os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/IL/')


#################################
#### Get Sessions
#################################

sessions = list(range(90, 100 + 1))

## Create List of Sessions and DROP IF ALREADY SCRAPED
sessions = [s for s in sessions if "IL_Bill_Details_" + str(s) + ".csv" not in os.listdir('.')]
     

##################################
### GET BILL PAGE URLS
##################################      
# session_num = 90

def get_bill_urls(session_num):
    
    ### Need different parameters for newest/oldest sessions
    if session_num >= 93:
        base_stem = 'http://www.ilga.gov/legislation/default.asp?GA={}'.format(session_num)
        bg_search = 'grplist'
        bg_stem = 'http://www.ilga.gov/legislation/' 
        grp_search = 'BillStatus'
        special_search = 'View Special Session'
        special_stem = 'http://www.ilga.gov/legislation/'
    else:
        base_stem = 'http://www.ilga.gov/legislation/legisnet{}/{}gatoc.html'.format(session_num, session_num)
        bg_search = 'groups'
        bg_stem = 'http://www.ilga.gov/legislation/legisnet{}/'.format(session_num)
        grp_search = 'summary|status'
        special_search = 'Session Legis'
        special_stem = 'http://www.ilga.gov'
        #special_stem92 = 'http://www.ilga.gov/legislation/legisnet{}/'.format(session_num)

        
    session_page = urllib.request.urlopen(base_stem, timeout = 5)  
    session_soup = BeautifulSoup(session_page, 'lxml')
    bill_group_urls = session_soup.find_all('a', href = re.compile(bg_search))
    bill_group_urls = [bg_stem + i['href'] for i in bill_group_urls]    
    ### Check For Speccial Session Link
    check_special = session_soup.find('a', text = re.compile(special_search))
    if check_special != None and session_num != 92:
        special_page = urllib.request.urlopen(special_stem + check_special['href'], timeout = 5)  
        special_soup = BeautifulSoup(special_page, 'lxml')
        special_bg_urls = special_soup.find_all('a', href = re.compile(bg_search))
        if special_bg_urls == [] and session_num < 93:
            # If no results means its one of th eearly years with no intermediate page, just straight to bills
            bill_group_urls.append('http://www.ilga.gov' + check_special['href'])
        else:
            special_bg_urls = [bg_stem + i['href'] for i in special_bg_urls]   
            bill_group_urls = bill_group_urls + special_bg_urls
    
    bill_urls = []
    current = 1
    total_pages = len(bill_group_urls)
    print('\n *** Gathering Bill Urls for the GA{} *** \n'.format(session_num))
    for grp_url in bill_group_urls:
        group_page = urllib.request.urlopen(grp_url, timeout = 5)
        time.sleep(1)
        group_soup = BeautifulSoup(group_page, 'lxml')
        these_urls = group_soup.find_all('a', href = re.compile(grp_search))
        these_urls = ['http://www.ilga.gov' + j['href'] for j in these_urls]
        for url in these_urls:
            bill_urls.append(url)
                
        print(' -- Page {} of {}'.format(current, total_pages))
        current += 1
 
    return(bill_urls)


##### Don't need this... 1992 Special Session has no bill details
#if these_urls == [] and session_num ==92:
#    ## Adjust for 1992 Special
#        these_urls = group_soup.find_all('a', href = re.compile('/sr/|/hr/'))
#        these_urls = ['http://www.ilga.gov/legislation/legisnet92/spss92/' + j['href'][2:] for j in these_urls]
#    else:
#        these_urls = ['http://www.ilga.gov' + j['href'] for j in these_urls]
        
##################################
### GET BILL DATA
##################################    
# bill_url = 'http://www.ilga.gov/legislation/BillStatus.asp?DocNum=1&GAID=12&DocTypeID=SB&LegId=68366&SessionID=85&GA=98'

### For all sessions 93+
def get_bill_data(session_num, bill_url):
    
    try:
        bill_page = urllib.request.urlopen(bill_url, timeout = 5).read()
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(10)
            bill_page = urllib.request.urlopen(bill_url, timeout = 10)
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            bill_page = urllib.request.urlopen(bill_url, timeout = 30)
    
    ### Get Bill Data    
    bill_soup = BeautifulSoup(bill_page, 'lxml')

    header = bill_soup.find('span', {'class', 'heading'})
    bill_num = header.get_text().replace('Bill Status of ', '')
    session_name = header.findNext("span").get_text()
    
    header2 = bill_soup.findAll("span", {'class':'heading2'})
    title = [i.findNext('span').get_text() for i in header2 if "Short Description" in i.get_text()][0]
    summary = [i.findNext('span').get_text() for i in header2 if "Synopsis" in i.get_text()][0]
    
    if bill_soup.find('span', text = 'Senate Sponsors') is not None:
        senate_sponsors = [j.get_text() for j in bill_soup.findAll('a', href = re.compile('Senator.asp')) ]
        senate_sponsors = '; '.join(senate_sponsors)
    else:
        senate_sponsors = ''
    
    if bill_soup.find('span', text = 'House Sponsors') is not None:
        house_sponsors = [j.get_text() for j in bill_soup.findAll('a', href = re.compile('Rep.asp')) ]
        house_sponsors = '; '.join(house_sponsors)
    else:
        house_sponsors = ''        
    
    ### Vote Page
    try:
        vote_url = 'http://www.ilga.gov/legislation/' + bill_soup.find('a', href = re.compile('votehistory'))['href']
    except:
        vote_url = ''
    
    ### Witness Slip Page -- Can Send in comments....
    try:
        witness_slip_url = 'http://www.ilga.gov/legislation/' + bill_soup.find('a', href = re.compile('witnessslip'))['href']
    except:
        witness_slip_url = ''    
   
    ### ACTIONS
    actions_table = bill_soup.find('a', {'name':'actions'}).findNext('table')
   
    # ---> Odd. Won't let me grab the rows with findAll('tr')
    # *** NEED To ADJUST for HEADERS
    dates = [d.get_text().strip() for d in actions_table.findAll('td', {'align':'right'})]
    chambers = [d.get_text().strip() for d in actions_table.findAll('td', {'align':'center'}) if d.get_text().strip() not in ['Date', 'Chamber']]
    actions = [d.get_text().strip() for d in actions_table.findAll('td', {'align':'left'}) if d.get_text().strip() not in ['Action']]
       
    all_actions = []
    order = 1
    for d,c,a in zip(dates, chambers, actions):
        all_actions.append([bill_num, session_num, session_name, d, c, a, order])
        order += 1
        
    #### Save to Export
    bill_details = [bill_num, session_num, session_name, title, summary, house_sponsors, senate_sponsors, bill_url, vote_url, witness_slip_url]
    return([bill_details, all_actions])


###############
### For 90 - 92nd sessions
# bill_url = 'http://www.ilga.gov/legislation/legisnet92/status/920SB1527.html'

def get_old_bill_data(session_num, bill_url):

    ### BILL SUMMARY PAGE
    summary_url = bill_url.replace('status', 'summary')
    try:
        bill_page = urllib.request.urlopen(summary_url, timeout = 5)
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(10)
            bill_page = urllib.request.urlopen(summary_url, timeout = 10)
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            bill_page = urllib.request.urlopen(summary_url, timeout = 30)
    time.sleep(.5)
       
    ### Get Bill Data    
    bill_soup = BeautifulSoup(bill_page, 'lxml')

    header = bill_soup.find('table').find('font').get_text().split(' \n\r\n')
    session_name = header[0]
    bill_num = header[1].replace("Summary of ", "")

    most_data = bill_soup.find("pre")
    title = most_data.find('em', text=re.compile('Short description:')).nextSibling.strip()
    summary = most_data.find('em', text=re.compile('Synopsis')).nextSibling.strip().replace("\r\n", "")  
    summary = ' '.join(summary.split())
    
    ### Sponsors
    try:
        senate_sponsors_tag = most_data.find('em', text = re.compile('Senate Sponsors'))
        end = senate_sponsors_tag.findNext('em', text=re.compile('Short description:|House Sponsors:'))
        senate_sponsors = []
        item = senate_sponsors_tag.findNext(['a', 'em'])
        while item != end:
            senate_sponsors.append(item.get_text())
            item = item.findNext(['a', 'em'])
        senate_sponsors = [s for s in senate_sponsors if s != '']
        senate_sponsors = '; '.join(senate_sponsors)
    except:
        senate_sponsors = ''
    
    try:
        house_sponsors_tag = most_data.find('em', text = re.compile('House Sponsors'))
        end = house_sponsors_tag.findNext('em', text=re.compile('Short description:|Senate Sponsors:'))
        house_sponsors = []
        item = house_sponsors_tag.findNext(['a', 'em'])
        while item != end:
            house_sponsors.append(item.get_text())
            item = item.findNext(['a', 'em'])
        house_sponsors = [s for s in house_sponsors if s != '']
        house_sponsors = '; '.join(house_sponsors)
    except:
        house_sponsors = ''
        
    ### No External Urls for Older Pages?
    vote_url = ''
    witness_slip_url = ''    
   
    ###########    
    ### ACTIONS
    status_url = bill_url.replace('summary', 'status')
    try:
        action_page = urllib.request.urlopen(status_url, timeout = 5)
    except:
        try:
            print("\n ~~> Retrying Bill Request -- Action Page")
            time.sleep(10)
            action_page = urllib.request.urlopen(status_url, timeout = 10)
        except:
            print("\n ~~> Retrying Bill Request x 2 -- Action Page")
            time.sleep(60)
            action_page = urllib.request.urlopen(status_url, timeout = 30)  

    action_soup = BeautifulSoup(action_page, 'lxml') 
    action_body = action_soup.find('pre')
    if action_body is not None:
        start = action_body.findAll('a', href = re.compile('sponsor'))
        if start == []:
            item = action_body.findChild()
        else:
            item = start[len(start)-1]
        while True:
            item = item.nextSibling
            if 'END OF INQUIRY' in str(item).upper():
                break
            
        #paragraph = ' '.join(item.strip().replace('\r\n', '').split())
        paragraph = item.strip().replace('\r\n', '')
        if session_num == 92:
            paragraph = re.split('([A-Z][A-Z]+-\d\d-\d\d\d\d  [A-Z] )', paragraph)
        else:            
            paragraph = re.split('(\d\d-\d\d-\d\d  [A-Z] )', paragraph)
        
        order = 1
        all_actions = []
        for i in range(1, len(paragraph), 2):
            d = paragraph[i].split('  ')[0]
            c = paragraph[i].split('  ')[1].strip()
            a = paragraph[i + 1].replace(" END OF INQUIRY", "").strip()
            a = re.sub("\s\s+", " ~ ", a)
            all_actions.append([bill_num, session_num, session_name, d, c, a, order])
            order += 1
        ### Note: This is going to yield errors still, see 90th SB0001 for example. Some tabbed notes and what not, but major actions should be fine
    else:
        print(" ~~~~> No Actions for {}".format(bill_num))
        all_actions = []
        
    #### Save to Export
    bill_details = [bill_num, session_num, session_name, title, summary, house_sponsors, senate_sponsors, bill_url, vote_url, witness_slip_url]
    return([bill_details, all_actions])



#####################################
### Loop Through Sessions, Get Bill Details
######################################
# bill_url = session_bills[6901]

for s in sessions:
    
    ### Get List of Bills and Basic Details
    session_bills = get_bill_urls(s)
    
    session_bill_details = [['bill_number', 'session_num', 'session', 'title', 'summary', 'house_sponsors', 'senate_sponsors', 'bill_url', 'vote_url', 'witness_slip_url']]    
    session_actions = [['bill_number', 'session_num', 'session', 'action_date', 'action_chamber', 'action', 'order']]
    
    print(" ~~~> Scraping Individual Bills - Session {}".format(s))

    ### Scrape Each Bill
    num = 1
    total = len(session_bills)
    for url in session_bills:
         
        ### Get Bill Data
        if s >= 93:
            this_bill = get_bill_data(s, url)
        else:
            this_bill = get_old_bill_data(s, url)
        time.sleep(1)

        ### Append to Agg Files
        session_bill_details.append(this_bill[0])

        if this_bill[1] != []:
            for action_row in this_bill[1]:
                session_actions.append(action_row)
        
        print(" -- ({}/{}) ~~ URL: {}".format(num, total, url))
        num += 1
    
    ### SAVE!
    with open("IL_Bill_Details_" + str(s) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("IL_Bill_Histories_" + str(s) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)  

    print("\n\n *********** General Assembly #{} DONE! ************** \n\n".format(s))


print(" ~~~~~~~~~~~~~ ************** ALL SESSIONS DONES *************** ~~~~~~~~~~~~~~~~ ")