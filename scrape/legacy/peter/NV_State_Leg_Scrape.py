# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape NEVADA Legislation 2011 - PRESENT ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# Use Archive Scrape File for Bills Before 2011
#
# Can easily get votes by switching /Overview to /Votes in bill_urls
#
# ************************
#~~~ 1985 to 1993 Sessions ~~~
## Loop through iterating bill numbers and break after three consecutive errors
## https://www.leg.state.nv.us/Session/63rd1985/reports/HistoryLibraryNELIS.cfm?SessionNumber=Nelis_85R&DocumentType=AB&BillNo=700
#
#~~~ 1995 to Present ~~
## Use the Bill Lists provdied in each tab
#
####################

import csv
import os
import urllib
import requests
from bs4 import BeautifulSoup
import time
import datetime
#from dateutil import parser as dateparser
import re

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/NV/')

#####################################
##### Extract Session Information
#######################################
# ** Could just do a range of years, but keeping this way in case something changes down the line....

session_search = urllib.request.urlopen('https://www.leg.state.nv.us/App/NELIS/REL', timeout = 30) 
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.find('ul', {'aria-labelledby':'session-selector-link'}).findAll('li')
sessions = [[i.get_text().strip(), i.find('a')['href']] for i in session_list if i.get_text().strip() != '']
sessions = [[s,'https://www.leg.state.nv.us' + h] if 'http' not in h else [s,h] for s,h in sessions]
sessions = [s.split(' ', 2) + [h]  for s,h in sessions]
for i in range(0, len(sessions)):
    sessions[i][1] = re.sub('\\(|\\)', '', sessions[i][1]) 
    sessions[i][2] = re.sub('Special Session', 'S', sessions[i][2])
    sessions[i][2] = re.sub('Session', 'R', sessions[i][2])

### Drop Previously Scraped
sessions = [s for s in sessions if 'NV_Bill_Details_' + s[1] + "_" + s[2] + '.csv' not in os.listdir('.')]

### Drop Current Session
this_year = datetime.datetime.now().year
sessions = [s for s in sessions if int(s[1]) < this_year]
print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))

del this_year, session_search, session_search_soup, session_list

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s = sessions[0]

def get_session_bills(s): 
    
    s_num, s_year, s_type, s_url = s

    print('\n~~~~ Gathering Bill URLs for the {} {} Session ({})~~~~\n'.format(s_num, s_type, s_year))

    ### Search By Bill Types
    bill_types = ['AB', 'IP', 'AJR', 'ACR', 'AR', 'SB', 'SJR', 'SCR', 'SR']
    get_bills_url_ext = '/HomeBill/GetBillsForNavSubPanels?startNumber=1&endNumber=9999&documentCode={}&_={}'
    
    ### Loop through Bill Types
    session_bills = []
    for bt in bill_types:
        bt_url = s_url + get_bills_url_ext.format(bt, time.time() * 1000)
        list_page = requests.get(bt_url)   
        time.sleep(1)
        list_soup = BeautifulSoup(list_page.content, 'lxml')    
        
        for bill in list_soup.findAll('a', href = re.compile('Overview')):
            session_bills.append([bill.get_text().strip(), s_num, s_year, s_type, 'https://www.leg.state.nv.us' + bill['href']])
        print(' -- {}'.format(bt))
            
    return(session_bills)


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################
    
def get_page_soup(bill_url, parser = 'lxml'):
    
    ### Get HTML
    try:
        page = urllib.request.urlopen(bill_url, timeout = 15)
    except urllib.error.HTTPError: # as e
        return('HTTP Error')
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(15)
            page = urllib.request.urlopen(bill_url, timeout = 30)
        except urllib.error.HTTPError: # as e
            return('HTTP Error')
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = urllib.request.urlopen(bill_url, timeout = 60)
    time.sleep(.5)
    
    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)    
    

#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = session_urls[0]
    
def get_bill_data(bill_row):    
        
    ### Scrape Bill Page 
    bill_num, s_num, s_year, s_type, bill_url = bill_row
    
    ## Fix Bill Url to Get Overview Data
    b_id = re.sub('https.+Bill/|/Overview', '', bill_url)    
    b_stem = re.sub('{}.+'.format(b_id), '', bill_url)
    new_bill_url = b_stem + 'FillSelectedBillTab?selectedTab=Overview&billKey={}&_={}'.format(b_id, time.time() * 1000)

    ### Get HTML, Soup    
    bill_soup = get_page_soup(new_bill_url)
    
    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')
         
    ## Get Basic Data
    title = bill_soup.find('div', text = re.compile('Title:'))
    title = title.findNextSibling('div').get_text().strip()

    summary = bill_soup.find('div', text = re.compile('Summary:'))
    summary = summary.findNextSibling('div').get_text().strip()
    summary = re.sub("\xa0", "", summary)

    intro_date = bill_soup.find('div', text = re.compile('Introduction Date:'))
    intro_date = intro_date.findNextSibling('div').get_text().strip()
    intro_date = datetime.datetime.strptime(intro_date, '%A, %B %d, %Y').strftime('%Y-%m-%d')    

    digest = bill_soup.find('div', text = re.compile('Digest:'))
    digest = digest.findNextSibling('div').find('span').get_text().strip()
    digest = re.sub("\r\n|\n|\t", " ", digest)
    digest = re.sub("\xa0|\s\s+", " ", digest)

    primary_sponsor = bill_soup.find('div', text = re.compile('Primary Sponsor'))
    if primary_sponsor is None:
        primary_sponsor = ''
    else:
        primary_sponsor = primary_sponsor.findNextSibling('div').findAll('a')
        primary_sponsor = [i.get_text().strip() for i in primary_sponsor]
        primary_sponsor = '; '.join(sorted(set(primary_sponsor), key=primary_sponsor.index))

    cosponsors = bill_soup.find('div', text = re.compile('Co-Sponsor'))
    if cosponsors is None:
        cosponsors = ''
    else:
        cosponsors = cosponsors.findNextSibling('div').findAll('a')
        cosponsors = [i.get_text().strip() for i in cosponsors]
        cosponsors = '; '.join(sorted(set(cosponsors), key=cosponsors.index))

    fiscal_note = bill_soup.find('div', text = re.compile('Fiscal Note'))
    if fiscal_note is None:
        fiscal_note = ''
    else:
        fiscal_note = fiscal_note.findNextSibling('div').get_text().strip()    
        fiscal_note = '; '.join(fiscal_note.split('\r\n\t\t\t\t\r\n\t\t\t\t'))

    ## Actions 
    action_table = bill_soup.find('th', text = re.compile('^Action$')).findParents('table')[0]
    action_rows = action_table.findAll('tr')
  
    bill_actions = []
    order = 1
    chamber = ''   
    
    for row in action_rows[1:]:
        cells = row.findAll('td')
        date = datetime.datetime.strptime(cells[0].get_text(), '%b %d, %Y').strftime('%Y-%m-%d')  
        action = re.sub('\xa0|\r', '', cells[1].get_text().strip())
        bill_actions.append([bill_num, s_num, s_year, s_type, chamber, date, action, order])
        order += 1
    
    ### Add in Hearings -- making sure I don't get upcoming hearings by accident (though unlikely to be that relevant)
    hearing_info = []
    past_hearings = bill_soup.find('h2', text = re.compile('Past Hearings')).findNext('th', text = re.compile('Meeting Video'))
    if past_hearings is not None:    
        past_hearings = past_hearings.findParents('table')[0]
    
        for hearing in past_hearings.findAll('tr')[1:]:
            hearing_cells = hearing.findAll('td')
            body = re.sub('\r|\n|\t', '', hearing_cells[1].get_text().strip()).replace('\xa0', ' ')
            outcome = hearing_cells[6].get_text().strip()
            h_action = '{} Committee Hearing ~ Outcome: {}'.format(body, outcome)
            h_date = datetime.datetime.strptime(hearing_cells[2].get_text(), '%b %d, %Y').strftime('%Y-%m-%d')  
            hearing_info.append([bill_num, s_num, s_year, s_type, '', h_date, h_action, ''])
    
    ### Combining and Sorting + Adding in New Order Variables
    if hearing_info != []:
        bill_actions = sorted(bill_actions + hearing_info, key = lambda x: datetime.datetime.strptime(x[5], '%Y-%m-%d'))    
        bill_actions = [x[0:7] + [index + 1] for index, x in enumerate(bill_actions)]
    
    ### OUTPUT 
    bill_details = [bill_num, s_num, s_year, s_type, intro_date, primary_sponsor, cosponsors, title, fiscal_note, summary, digest, bill_url]
    
    return([bill_details, bill_actions])
        
########################################################
############## SCRAPE SESSION(S)
############################################
# bill_num, bill_url, descrip = session_urls[6751]
# s = sessions[0]
    

### *** LOOPING THROUGH INDIVIDUAL YEARS and Aggregating Regular + Special Sessions ***
for s in sessions:

    s_num, s_year, s_type, s_url = s

    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'session_year', 'session_type', 'intro_date', 'primary_sponsors', 'cosponsors', 'title', 'fiscal_notes', 'summary', 'digest', 'bill_url']]    
    session_actions = [['bill_number',  'session', 'session_year', 'session_type', 'chamber', 'action_date', 'action', 'order']]
    
    print("\n ------------------- Now Scraping the {} {} Session ({}) ---------------------- \n".format(s_num, s_type, s_year))    
    
    ### Get all urls for a session-year, including special sessions
    session_urls = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_urls)
    for bill_row in session_urls:
        
        bill_data = get_bill_data(bill_row)
        
        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n URL: {} \n **********".format(num, total, bill_row[0], bill_row[4]))
            num += 1
            continue       
            
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
    
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[0], bill_row[4]))
        num += 1
        
    with open("NV_Bill_Details_" + s_year + "_" + s_type + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("NV_Bill_Histories_" + s_year + "_" + s_type  + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n -------------{} {} Session ({}) SCRAPED + DATA SAVED  -------------\n\n\n".format(s_num, s_type, s_year))   


print("  ********************************** ALL DONE ********************************** ")
