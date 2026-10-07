# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape OHIO Bills

@author: PB
"""

##### NOTES:
# This file Scrapes 2015+ 
# Use Archive File for past sessions, 1997 - 2014, found at: https://www.legislature.ohio.gov/
###########################

import csv
import os
from bs4 import BeautifulSoup
import time
import re
import requests
import datetime
os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/OH/')

#######################
##### Extract Session Links
########################

this_year = datetime.datetime.now().year
session_years = [y for y in range(2015, this_year, 2)]
session_nums = [i for i in range(131, 131 + len(session_years), 1)]
sessions = [[y, n] for y,n in zip(session_years, session_nums) if y < this_year]
del session_years, session_nums

### Drop Previously Scraped
sessions = [i for i in sessions if 'OH_Bill_Details_' + '{}_{}'.format(i[0], i[0] + 1) + '.csv' not in os.listdir('.')]


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################
  
def get_page_soup(bill_url, parser = 'lxml'):
    
    ### Get HTML
    try:
        page = requests.get(bill_url, timeout = 30)
    except requests.HTTPError: # as e
        return('HTTP Error')
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(30)
            page = requests.get(bill_url, timeout = 45)
        except requests.HTTPError: # as e
            return('HTTP Error')  
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = requests.get(bill_url, timeout = 60)
    time.sleep(1)
    
    ### Return Soup
    page_soup = BeautifulSoup(page.content, parser)
    return(page_soup)    


#######################################################
########## GET BILL URLS VIA FTP SITE FOR A SESSION
#####################################################
# s = sessions[0]
# bill_data = get_session_bills(s)

def get_session_bills(s): 
    
    s_start_yr, s_num = s
    
    #### Get ALL House Bills ---- This is SLOW
    house_url = 'https://www.legislature.ohio.gov/legislation/search?generalAssemblies={}&legislationTypes=HB,HR,HCR,HJR&pageSize=5000&start=1'.format(s_num)
    house_search = requests.get(house_url, timeout = 500)
    house_soup = BeautifulSoup(house_search.content, 'lxml')
    
    #### Get ALL Senate Bills ---- This is SLOW
    senate_url = 'https://www.legislature.ohio.gov/legislation/search?generalAssemblies={}&legislationTypes=SB,SR,SCR,SJR&pageSize=5000&start=1'.format(s_num)
    senate_search = requests.get(senate_url, timeout = 500)   
    senate_soup = BeautifulSoup(senate_search.content, 'lxml')
      
    session_bills = []
    for soup in [house_soup, senate_soup]:
        bill_table = soup.find('table', {'class':'legislationTable dataGridOpen'})
        for row in bill_table.findAll('tr')[1:]:
            cells = row.findAll('td')
            bill_num = cells[1].span.get_text()
            bill_url = cells[1].a['href']
            bill_url = re.sub('../legislation', 'https://www.legislature.ohio.gov/legislation', bill_url)            
            title = cells[3].span.get_text()
            primary_sponsors = '; '.join([i.text for i in cells[4].span.findAll('div')])
            status = cells[5].span.get_text()
            session_bills.append([bill_num, s_start_yr, s_num, title, primary_sponsors, status, bill_url])
            
    ### Export
    print(' \n **** ~~~> Found {} BILL URLs for the {}-{} **** \n'.format(len(session_bills), s_start_yr, s_start_yr + 1))
    return(session_bills)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_data = get_session_bills(sessions[5])
# bill_info = bill_data[1]
# bill_info = session_bills[0]
#bn_dict = {'S. B. No.':'SB', 'S. R. No.':'SR', 'H. B. No.':'HB', 'H. R. No.':'HR', 'H. J. R.':'', 'H. J. R.':'HCR'}

def get_bill_data(bill_info):    
    
    bill_num, s_yr, s_num, title, sponsor, status, bill_url = bill_info
    
    ### CLEAN BILL NUMBERS
    # bill_num = 'S. R. No. 317'
    bill_parts = [i.strip() for i in re.split('([0-9]+)', bill_num) if i.strip() != '']
    bill_num_z = re.sub('No\\.|\\. |\\.$', '', bill_parts[0]).strip() + bill_parts[1].zfill(4)
    
    ### Get Bill Data    
    bill_soup = get_page_soup(bill_url, parser = 'lxml')
    bill_actions = []
    
    ###############
    ### Basic Info
    long_title = bill_soup.find('div', id = 'longTitle').find('span')
    if long_title:
        long_title = long_title.text.strip()
    else:
        long_title = ''
        
    ## Topics
    subjects = bill_soup.find('div', {'class':'legislationSubjects'}).findAll('a')
    subjects = '; '.join([i.text.strip() for i in subjects])
        
    ## Committees
    committees = bill_soup.find('div', {'class':'legislationCommittees'}).findAll('a')
    if committees == [] and bill_soup.find('div', {'class':'legislationCommittees'}).find('h3') is not None:
       committees = bill_soup.find('div', {'class':'legislationCommittees'}).get_text('---')
       committees = re.sub('---Committees---', '', committees.strip())
       committees = '; '.join([c for c in committees.split('---') if c != ''])
    else:
        committees = '; '.join([i.text.strip() for i in committees])

    ### PRIMARY SPONSOR(s) -- Need to adapt for whether or not links to sponsor pages are included...
    # ** Format changes with GA 133 -- URLs to sponsors vs Text List
    sponsors = bill_soup.find('div', {'class':'legislationPrimarySponsors'}).findAll('a')
    if sponsors == []:
        sponsors = bill_soup.find('div', {'class':'legislationPrimarySponsors'}).findAll('div')
    sponsors = '; '.join([re.sub('District.+', '', j.text).strip() for j in sponsors if j.text.strip() != ''])  

    ### COSPONSORS -- May need to parse these more if both House and Senate Cosponsors permitted
    cosponsor_tag = bill_soup.find('div', {'class':'legislationCosponsors'})
    if cosponsor_tag is not None:
        cosponsor_tag = cosponsor_tag.findAll('div', recursive = False)
        cosponsors = '; '.join([i.get_text(' ').strip() for i in cosponsor_tag if i.get_text(' ').strip() != ''])
    else:
        cosponsors = ''
    
    #########
    ### ACTIONS
    action_soup = get_page_soup(re.sub('legislation-summary', 'legislation-status', bill_url), parser = 'lxml')
    action_soup.find('div', {'class':'legislationStatus'})
    
    hist_table = action_soup.find('table', {'class':'dataGridOpen legislationStatusTable'})
    if hist_table is not None:
        hist_rows = hist_table.findAll('tr')
        order = len(hist_rows[2:])
        for row in hist_rows[2:]:
            cells = row.findAll('td')
            date = cells[0].span.text.strip()
            date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')
            chamber = cells[1].span.text.strip()
            action = cells[2].span.text.strip()
            comm = cells[3].span.text.strip()
            bill_actions.append([bill_num_z, s_yr, s_num, date, chamber, action, comm, order])
            order -= 1  

   
    ### OUTPUT
    bill_details = [bill_num_z, s_yr, s_num, sponsors, cosponsors, status, subjects, title, committees, long_title, bill_url]
    return([bill_details, bill_actions])
        

########################################################
############## SCRAPE SESSION(S)
############################################
# s = sessions[0]

for s in sessions:
    
    #### Output Lists
    session_bill_details = [['bill_number', 'session_year', 'session_num', 'sponsors', 'cosponsors', 'status', 'subjects', 'title', 'committees', 'long_title', 'bill_url']]    
    session_actions = [['bill_number', 'session_year', 'session_num', 'action_date', 'chamber', 'action', 'committee', 'order']]
    
    print("\n ------------------- OHIO - Now Scraping: {}-{} ---------------------- \n".format(s[0], s[0] + 1))
    
    ### Get all bills for a specific session
    session_bills = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        bill_data = get_bill_data(bill_info)
        
        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n **********".format(num, total, bill_info[0]))
            num += 1
            continue       
              
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
    
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_data[0][0], bill_data[0][-1]))
        num += 1
        
    with open("OH_Bill_Details_" + '{}_{}'.format(s[0], s[0] + 1) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("OH_Bill_Histories_" + '{}_{}'.format(s[0], s[0] + 1) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- OHIO {}-{} SCRAPED + DATA SAVED  -------------\n\n\n".format(s[0], s[0] + 1))


print("  ********************************** ALL DONE ********************************** ")
