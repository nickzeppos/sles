# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape COLORADO Legislation 2016+ ~~~~~~~~~~~~

** USE ARCHIVE SCRAPER FOR  1998 - 2015 ** 

@author: PB
"""

##### NOTES:
# AJAX request code adapted from OpenStates
# Could Get Committee Votes + Regular Votes Easily
# When constructing LES, need to adapt for differences between Recent and Archive Formats
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
import json
import math

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/CO/')


#####################################
##### Extract Session Information
#######################################
# ** Could just do a range of years, but keeping this way in case something changes down the line....

session_search = urllib.request.urlopen('https://leg.colorado.gov/bill-search', timeout = 30) 
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.find('select', id = 'edit-field-sessions').findAll('option')
session_dict = {'Regular Session':'RS', 'Extraordinary Session':'S1'}
sessions = [[i.get_text(), re.sub(' .+', '', i.get_text()), session_dict[re.sub('[0-9]+ ', '', i.get_text())], i['value']] for i in session_list if i['value'] != 'All']

### Drop Previously Scraped
sessions = [s for s in sessions if 'CO_Bill_Details_' + s[1] + '.csv' not in os.listdir('.')]

### Drop Current Session
this_year = datetime.datetime.now().year
sessions = [s for s in sessions if int(s[1]) < this_year]
print("\n\n ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))

del this_year, session_search, session_search_soup, session_list, session_dict

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s = sessions[0]

def scrape_bill_list(s_year, s_type, bill_list, ajax_url, form, first_soup = False):
    
    ### Get HTML --> Soup
    if first_soup == False:
        this_resp = requests.post(url=ajax_url, data=form, allow_redirects=True)
        time.sleep(.25)
        this_resp = json.loads(this_resp.content.decode("utf-8"))
        this_soup = BeautifulSoup(this_resp[3]['data'], 'lxml') 
    else:
        this_soup = first_soup
        
    ### Parse Data
    these_bills = this_soup.findAll('article')
    for bill in these_bills:
        bn = bill.find('div', {'class':'field-items'}).get_text().strip()
        b_url = 'https://leg.colorado.gov' + bill.find('h4').find('a')['href']
        b_type = bill.find('div', {'class':'search-aside'}).find('div', {'class':'bill-type search-result-single-item'})
        b_type = b_type.find('div', {'class':'field-items'}).get_text().strip()
        #title = bill.find('h4').find('a').get_text()
        #Could get sponsors, short description, recent action as well, but might as well grab that on the bill page
        bill_list.append([bn,  s_year, s_type, b_type, b_url])
    
    return(bill_list)


def get_session_bills(s): 
    
    s_name, s_year, s_type, s_id = s

    print('\n~~~~ Gathering Bill URLs for the {} ~~~~\n'.format(s_name))

    ## Prep Post Request
    ajax_url = 'http://leg.colorado.gov/views/ajax'

    form = {
        'field_chamber': 'All',
        'field_bill_type': 'All',
        'field_sessions': s_id,
        'sort_bef_combine': 'field_bill_number ASC',
        'view_name': 'bill_search',
        'view_display_id': 'full',
        'view_args': '',
        'view_path': 'bill-search',
        'view_base_path': 'bill-search',
        'view_dom_id': '54db497ce6a9943741e901a9e4ab2211', #29e17e242960da9761bf65a6eb510e07
        'pager_element': '0',
        'page': '0',
    }
    
    ### First page
    resp = requests.post(url=ajax_url, data=form, allow_redirects=True)
    resp = json.loads(resp.content.decode("utf-8"))
    resp_soup = BeautifulSoup(resp[3]['data'], 'lxml') # Returns html..

    ### Get Page Info
    max_results = resp_soup.find('div', text = re.compile('Displaying.+ results')).get_text()
    max_results = re.search(r'of (\d+) results', max_results)
    max_results = int(max_results.group(1))
    total_pages = int(math.ceil(max_results / 25.0))
    
    ### Parse Data from Page 1
    bill_list = scrape_bill_list(s_year, s_type, [], ajax_url, form, first_soup = resp_soup)    
    print(" -- {}/{}".format(1, total_pages))
    
    ### Loop through subsequent pages -- Update page, scrape list
    for page_num in range(1, total_pages):
        form['page'] = page_num
        bill_list = scrape_bill_list(s_year, s_type, bill_list, ajax_url, form)    
        print(" -- {}/{}".format(page_num + 1, total_pages))

    return(bill_list)


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
# bill_row = all_session_urls[0]
    
def get_bill_data(bill_row):    
        
    ### Scrape Bill Page 
    bill_num, s_year, s_type, bill_type, bill_url = bill_row
    bill_soup = get_page_soup(bill_url)
    
    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')
         
    ## Get Basic Data
    title = bill_soup.find('h1', {'class':'node__title node-title'}).get_text().strip()
         
    long_title = bill_soup.find('div', {'class':'field field-name-field-bill-long-title field-type-text-long field-label-hidden'})
    long_title = long_title.get_text().strip()
    
    subjects = bill_soup.find('div', {'class':'field field-name-field-subjects field-type-entityreference field-label-hidden'})
    if subjects is not None:
        subjects = subjects.get_text().strip()
    else:
        subjects = ''
    
    summary = bill_soup.find('div', id = 'bill-summary-top')
    summary = re.sub('\r\n|\n+', ' ', summary.get_text()).strip()
    summary = re.sub('\s+', ' ', summary)
    
    committees = bill_soup.find('div', {'class':'committee-item'})    
    if committees is None:
        house_comm = ''
        senate_comm = ''
    else:
        house_comm = committees.find('div', text = re.compile('House'))
        if house_comm is not None:
            house_comm = house_comm.findNext('h4').get_text()
        else:
            house_comm = ''
            
        senate_comm = committees.find('div', text = re.compile('Senate'))
        if senate_comm is not None:
            senate_comm = senate_comm.findNext('h4').get_text()
        else:
            senate_comm = ''    
    
    status_items = bill_soup.find('div', {'class':'layout-status field-items'})
    status_items = status_items.findChildren('div', recursive = False)
    status = '; '.join([i.get_text().strip() for i in status_items])
    
    #### Sponsors
    sponsor_table = bill_soup.find('div', id = 'bill-documents-tabs8')
    primary = sponsor_table.find('td', text = re.compile('Prime'))
    primary = primary.findNext('td').findAll('a')
    primary = '; '.join([p.get_text() for p in primary])
    
    sponsors = sponsor_table.find('td', text = re.compile('^Sponsor'))
    sponsors = sponsors.findNext('td').findAll('a')
    sponsors = '; '.join([p.get_text() for p in sponsors])
   
    cosponsors = sponsor_table.find('td', text = re.compile('Co-sponsor'))
    cosponsors = cosponsors.findNext('td').findAll('a')
    cosponsors = '; '.join([p.get_text() for p in cosponsors])
    
    ## Actions 
    action_table = bill_soup.find('div', id = 'bill-documents-tabs7').find('table')
    action_rows = action_table.findAll('tr')
  
    bill_actions = []
    order = len(action_rows[1:])
    for row in action_rows[1:]:
        cells = row.findAll('td')
        date = datetime.datetime.strptime(cells[0].get_text(), '%m/%d/%Y').strftime('%Y-%m-%d')  
        chamber = cells[1].get_text()
        action = cells[2].get_text().strip()
        bill_actions.append([bill_num, s_year, s_type, chamber, date, action, order])
        order -= 1
    
    ### OUTPUT 
    bill_details = [bill_num, s_year, s_type, bill_type, title, subjects, status, primary, sponsors, cosponsors, house_comm, senate_comm, long_title, summary, bill_url]
    
    return([bill_details, bill_actions])
        
########################################################
############## SCRAPE SESSION(S)
############################################
# bill_num, bill_url, descrip = session_urls[6751]
# s = sessions[0]
    
### Unique Years - Ascending
unique_session_years = [str(j) for j in set([int(i[1]) for i in sessions])]

### *** LOOPING THROUGH INDIVIDUAL YEARS and Aggregating Regular + Special Sessions ***
for y in unique_session_years:

    ### Scrape_sessions
    scrape_sessions = [s for s in sessions if s[1] == y]

    #### Output Lists
    # CO Archive: [['bill_number', 'session', 'session_type', 'title', 'sponsors', 'out_chamber_sponsors', 'bill_url']]   
    session_bill_details = [['bill_number', 'session', 'session_type', 'bill_type', 'title', 'keywords', 'status', 'primary_sponsors', 'sponsors', 'cosponsors', 'house_committee', 'senate_committee', 'long_title', 'summary', 'bill_url']]
    # CO Archive does not have chamber    
    session_actions = [['bill_number', 'session', 'session_type', 'chamber', 'action_date', 'action', 'order']]
    
    print("\n ------------------- Now Scraping the {} Session(s) ---------------------- \n".format(y))    
    
    ### Get all urls for a session-year, including special sessions
    all_session_urls = []
    for s in scrape_sessions:
        this_session_urls = get_session_bills(s)
        for row in this_session_urls:
            all_session_urls.append(row)

    #### Loop through bills
    num = 1
    total = len(all_session_urls)
    for bill_row in all_session_urls:
        
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
        
    with open("CO_Bill_Details_" + y + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("CO_Bill_Histories_" + y + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- {} SESSION(S) SCRAPED + DATA SAVED  -------------\n\n\n".format(y))


print("  ********************************** ALL DONE ********************************** ")
