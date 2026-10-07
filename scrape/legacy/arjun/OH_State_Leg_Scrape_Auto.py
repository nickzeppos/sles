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
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options  # Import Options from the correct module
import datetime
from selenium.common.exceptions import TimeoutException

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

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
    page_soup = BeautifulSoup(page.content, 'lxml')
    return(page_soup)


#######################################################
########## GET BILL URLS VIA FTP SITE FOR A SESSION
#####################################################
# s = sessions[0]
# bill_data = get_session_bills(s)

def get_session_bills(s):

    s_start_yr, s_num = s
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)

    #### Get ALL House Bills ---- This is SLOW
    # house_url = 'https://www.legislature.ohio.gov/legislation/search?generalAssemblies={}&legislationTypes=HB,HR,HCR,HJR&pageSize=5000&start=1'.format(s_num)
    house_url = 'https://www.legislature.ohio.gov/legislation/search?start=1&pageSize=5000&generalAssemblyNumber={}&sort=Number&extendedLegislationTypes=House%20Bill,House%20Resolution,House%20Concurrent%20Resolution,House%20Joint%20Resolution'.format(s_num)
    # house_search = requests.get(house_url, timeout = 500)
    # house_soup = BeautifulSoup(house_search.content, 'lxml')

    #### Get ALL Senate Bills ---- This is SLOW
    # senate_url = 'https://www.legislature.ohio.gov/legislation/search?generalAssemblies={}&legislationTypes=SB,SR,SCR,SJR&pageSize=5000&start=1'.format(s_num)
    senate_url = 'https://www.legislature.ohio.gov/legislation/search?start=1&pageSize=5000&generalAssemblyNumber={}&sort=Number&extendedLegislationTypes=Senate%20Bill,Senate%20Resolution,Senate%20Concurrent%20Resolution,Senate%20Joint%20Resolution'.format(s_num)
    # senate_search = requests.get(senate_url, timeout = 500)
    # senate_soup = BeautifulSoup(senate_search.content, 'lxml')

    session_bills = []
    for url in [house_url, senate_url]:
        # bill_table = soup.find('table', {'class':'legislationTable dataGridOpen'})
        driver.get(url)
        search_button = driver.find_element(By.XPATH, "//button[@aria-label='Search']")
        search_button.click()
        time.sleep(10)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        bill_table = soup.find('table', {'class':'data-grid legislation-table'})
        for row in bill_table.find_all('tr')[1:]:
            bill_num = row.find('a').get_text()
            title = row.find('td', class_='short-title-cell').span.get_text()
            sponsors = row.find('td', class_='sponsor-cell').span.findAll('div')
            primary_sponsors = '; '.join([sponsor.get_text() for sponsor in sponsors])
            status = row.find('td', class_='version-cell').span.get_text()
            bill_url = 'https://www.legislature.ohio.gov/legislation/'+row.find('a')['href']
            session_bills.append([bill_num, s_start_yr, s_num, title, primary_sponsors, status, bill_url])
        # print(session_bills)

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

    bill_soup = get_page_soup(bill_url, parser = 'html')
    bill_actions = []

    ###############
    ### Basic Info
    long_title_element = bill_soup.find('div', id='long-title')

    # Check if the element exists
    if long_title_element:
        long_title = long_title_element.get_text()
    else:
        long_title = ""

    ## Topics
    subjects_header = bill_soup.find('h2', text='Subjects')

    if subjects_header:
        subjects_div = subjects_header.find_next('div', class_='tag-link-group')
        subjects = '; '.join([a.get_text(strip=True) for a in subjects_div.find_all('a')])
    else:
        subjects = ''

    committees_header = bill_soup.find('h2', text='Committees')
    if committees_header:
        committees_div = committees_header.find_next('div', class_='tag-link-group')
        committees = '; '.join([span.get_text(strip=True) for span in committees_div.find_all('span')])
    else:
        committees = ''

    sponsors_header = bill_soup.find('h2', text='Primary Sponsors')

    if sponsors_header:
        sponsors_div = sponsors_header.find_next('div', class_='other-primary-sponsors')
        sponsors = '; '.join([span.get_text(strip=True) for span in sponsors_div.find_all('span')])
    else:
        sponsors = ''


    cosponsors_header = bill_soup.find('h2', text='Cosponsors')
    if cosponsors_header:
        cosponsors_div = cosponsors_header.find_next('div', class_='legislation-cosponsors-inner')
        cosponsor_names = [span.get_text(strip=True) for span in cosponsors_div.find_all('span') if span.get_text(strip=True)]
        cosponsors = '; '.join(cosponsor_names)
    else:
        cosponsors = ''

    #########
    ### ACTIONS

    action_soup = get_page_soup(bill_url+"/status", parser = 'lxml')
    action_soup.find('div', {'class':'legislationStatus'})

    hist_table = action_soup.find('table', {'class':'data-grid legislation-status-table'})
    if hist_table is not None:
        hist_rows = hist_table.findAll('tr')
        order = len(hist_rows[2:])
        for row in hist_rows[2:]:
            date_cell = row.find('th', class_='date-cell').find('span')
            cells = row.findAll('td')
            date_str = date_cell.get_text(strip=True)
            date = datetime.datetime.strptime(date_str, '%m-%d-%Y').strftime('%Y-%m-%d')
            chamber_cell = row.find('td', class_='chamber-cell').find('span')
            chamber = chamber_cell.get_text(strip=True) if chamber_cell else ''
            action_cell = row.find('td', class_='action-cell').find('span')
            action = action_cell.get_text(strip=True) if action_cell else ''
            committee_cell = row.find('td', class_='committee-cell').find('span')
            comm = committee_cell.get_text(strip=True) if committee_cell else ''
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
