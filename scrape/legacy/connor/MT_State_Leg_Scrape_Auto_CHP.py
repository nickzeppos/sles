# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape MONTANA Legislation 1999 - PRESENT ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# BILL Lists Include both Introduced and UNINTRODUCED Bills
# ---> Basically, bill drafting requests that fail... For LES these would be coded 0 regardless...
# ---> But fascinating...
# ---> Can get information on who drafted it, how far it made it, etc. (C) labels in tables are for drafting period, it seems.
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
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options  # Import Options from the correct module
from selenium.common.exceptions import TimeoutException
from lxml import etree
import pandas as pd
from playwright.sync_api import sync_playwright

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#########################################
####### BEFORE RUNNING
#########################################

##### NOTES (CHP):
# Montana provides a CSV file with mostly the same information as our bill_details.csv file, so in order to make the process easier
# start out by downloading that. Navigate to https://bills.legmt.gov/ and download the information for the session(s) you're scraping
# as a CSV (you can adjust the session using the FILTERS button right next to the EXPORT button at the top right-hand corner).
# Make sure you haven't applied any other filters. Rename the folder according to the name of the session in the Session menu
# (e.g., "2023 (Regular).csv") and move it to the Bills subfolder in the State Legislative Data/States/MT directory. 

####################

print('\n\n ***************** \n ~~~> REMEMBER TO PRE-DOWNLOAD BILL INFORMATION FILES, SEE NOTES ABOVE \n ***************** \n\n')

#####################################
##### Extract Session Information
#######################################

chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(options=chrome_options)

# Going forward, it looks like this is the source of bill information--special session information for past years
# is incomplete, but hopefully they'll have full information for future specials
driver.get('https://bills.legmt.gov/')
time.sleep(4)
filters_button = driver.find_element(By.XPATH, '//*[@id="root"]/div/div/div[1]/div[1]/div[3]/div[2]/button')
time.sleep(4)
filters_button.click()
time.sleep(4)
sessions_button = driver.find_element(By.XPATH, '//*[@id="root"]/nav[1]/div/ul/li[1]/a')
time.sleep(4)
sessions_button.click()
time.sleep(4)
menu_button = driver.find_element(By.XPATH, '//*[@id="sessionYearSelect"]/div/span')
time.sleep(4)
menu_button.click()
time.sleep(2)
session_search_soup = BeautifulSoup(driver.page_source, 'html.parser')
session_list = session_search_soup.findAll('span', class_='select-option-text')
sessions = [i.get_text().strip() for i in session_list]
for i in range(0, len(sessions)):
    yr = int(sessions[i].split(' ', 1)[0])
    s_type = sessions[i].split(' ', 1)[1]
    if s_type == '(Regular)':
        s_type = 'RS'
    else:
        s_type = 'SS1' # Adjust if there are two special sessions in a single biennium
    sessions[i] = [yr, s_type, sessions[i]]

### Drop Previously Scraped
sessions = [s for s in sessions if 'MT_Bill_Details_' + str(s[0]) + "_" + s[1] + '.csv' not in os.listdir('.')]
sessions = [s for s in sessions if s[0] >= 2023]

### Drop Upcoming Session
next_year = datetime.datetime.now().year + 1
sessions = [s for s in sessions if s[0] < next_year]
print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(next_year))

del next_year, session_search_soup, session_list, yr, s_type

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s = sessions[0]
# test = get_session_bills(sessions[0])

def get_session_bills(s):

    s_year, s_type, s_id = s

    print('\n~~~~ Gathering Bill URLs for the {} {} Session ~~~~\n'.format(s_year, s_type))

    session_bills = pd.read_csv('Bills/{}.csv'.format(s_id))

    session_bills['bill_url'] = [re.sub('/lc', '', i) + '?open_tab=sum' for i in session_bills['LC URL']]
    session_bills['s_year'] = s_year
    session_bills['s_type'] = s_type
    session_bills.columns = ['bill_num' if x == 'Bill #' else x for x in session_bills.columns]
    session_bills.columns = ['lc_num' if x == 'LC #' else x for x in session_bills.columns]
    session_bills.columns = ['title' if x == 'Short Title' else x for x in session_bills.columns]
    session_bills.columns = ['status' if x == 'Status' else x for x in session_bills.columns]

    session_bills = session_bills[['bill_num', 'lc_num', 's_year', 's_type', 'status', 'title', 'bill_url']]

    return(session_bills)

##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url):

    with sync_playwright() as p:
        # Launch a browser (Chromium by default)
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(bill_url)  
        time.sleep(12)
        content = page.content()
        browser.close()

    # Parse the HTML with Beautiful Soup
    page_soup = BeautifulSoup(content, 'html.parser')

    return(page_soup)

#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = session_urls[0]
# bill_row = session_bills[0]

def get_bill_data(bill_row):

    ### Scrape Bill Page
    bill_num, lc_num, s_year, s_type, status, title, bill_url = bill_row

    ### Get HTML, Soup
    bill_soup = get_page_soup(bill_url)

    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')

    ### Bill Name
    #bill_name = bill_soup.find('h5', {'class':'modal-title'}).get_text().strip()
    #print(bill_name)

    ### Primary Sponsor
    sponsor_row = bill_soup.find('h5', {'class':'mb-0'})
    if sponsor_row is None:
        sponsor = ''
    else:
        sponsor = sponsor_row.get_text().strip()

    ### Bill Draft Requester
    requester_row = bill_soup.find('td', string = re.compile('Requester')).findNext('td').find('span')
    if requester_row is None:
        requester = ''
    else:
        requester = requester_row.get_text().strip()

    ### Bill Drafter
    drafter_row = bill_soup.find('td', string = re.compile('Drafter'))
    drafter = drafter_row.findNext('td').find('span').get_text().strip()

    ### Background Info
    subjects_row = bill_soup.find('td', string = re.compile('Subject'))
    subjects = subjects_row.findNext('td').get_text().strip()

    ### Whether Bill Was Introduced
    if status == '(C) Draft Died in Process':
        draft = 1
    else:
        draft = 0

    ## Actions --
    action_col = bill_soup.find('th', string = re.compile('Action'))
    hist_table = action_col.findNext('tbody')
    hist_rows = hist_table.findAll('tr')

    bill_actions = []

    order = len(hist_rows)
    for row in hist_rows:
        cells = row.findAll('td')
        action = cells[1].get_text().strip()
        committee = cells[2].get_text().strip()
        if committee != '':
            action = action + ' ~ [{}]'.format(committee)

        date = cells[0].get_text().strip()
        chamber = re.search('^\\([A-Z]\\)', action)
        if chamber:
            chamber = re.sub('\\(|\\)', '', action[chamber.span()[0]:chamber.span()[1]])
        else:
            chamber = ''

        bill_actions.append([bill_num, lc_num, s_year, s_type, chamber, date, action, order])
        order -= 1

    #############
    ### OUTPUT
    ##############
    bill_details = [bill_num, lc_num, s_year, s_type, draft, sponsor, requester, drafter, title, subjects, bill_url]

    return([bill_details, bill_actions])

########################################################
############## SCRAPE SESSION(S)
############################################
# bill_num, bill_url, descrip = session_urls[6751]
# s = sessions[0]


### *** LOOPING THROUGH INDIVIDUAL YEARS and Aggregating Regular + Special Sessions ***
for s in sessions:

    s_year, s_type, s_id = s

    #### Output Lists
    session_bill_details = [['bill_number', 'bill_req_number', 'session_year', 'session_type', 'bill_draft', 'sponsor', 'requester', 'drafter', 'title', 'subjects', 'bill_url']]
    session_actions = [['bill_number', 'bill_req_number', 'session_year', 'session_type', 'chamber', 'action_date', 'action', 'order']]

    print("\n ------------------- Now Scraping the {} {} Session ---------------------- \n".format(s_year, s_type))

    ### Get all urls for a session-year, including special sessions
    session_bills = get_session_bills(s)
    session_urls = session_bills.values.tolist()

    #### Loop through bills
    num = 1
    total = len(session_urls)
    for bill_row in session_urls:

        bill_data = get_bill_data(bill_row)

        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n URL: {} \n **********".format(num, total, bill_row[0], bill_row[6]))
            num += 1
            continue

        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        if bill_row[0] == '':
            print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[1], bill_row[6]))
        else:
            print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[0], bill_row[6]))
        num += 1

    with open("MT_Bill_Details_" + str(s_year) + "_" + s_type + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("MT_Bill_Histories_" + str(s_year) + "_" + s_type  + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} {} Session SCRAPED + DATA SAVED  -------------\n\n\n".format(s_year, s_type))


print("  ********************************** ALL DONE ********************************** ")
