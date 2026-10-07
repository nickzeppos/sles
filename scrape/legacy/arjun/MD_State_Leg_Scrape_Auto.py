#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 13 12:37:40 2019

@author: pb
"""

##### NOTES:
#  Data ranges 1996+, but website format changes starting in 2013.
#  Bulk Data in CSV Format available 2016+ (See Main Page, Open Legislative Data (Lower Left))
# ----> Bulk format would be easy to work with, its just a matter of getting the older stuff into that format...

# Will need to construct order variable for pre-2012 after the fact
####################

import csv
import os
import urllib
import requests
from bs4 import BeautifulSoup
import time
import datetime
import re
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options  # Import Options from the correct module
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import socket

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#######################
##### Extract Session Links
########################

session_search = urllib.request.urlopen('http://mgaleg.maryland.gov/webmga/frmLegislation.aspx?pid=legisnpage&tab=subject3', timeout = 30)
session_search_soup = BeautifulSoup(session_search, 'lxml')


### Get Sessions
session_list = session_search_soup.find('select', id = 'valueSessions').findAll('option')


session_ids = [s['value'] for s in session_list]
session_names = [s.get_text() for s in session_list]
session_years = [int(s.split(' ')[0]) for s in session_names]
# print(str(datetime.datetime.now().year))



previously_scraped = []
dirFiles = os.listdir('.')

print("Removing Previously Scraped")
for i in range(0, len(session_ids)):
    if str(datetime.datetime.now().year) in session_ids[i]:
        previously_scraped.append(i)
        print("Removing Current Session: " + session_names[i])
    if 'MD_Bill_Details_' + session_ids[i] + '.csv' in dirFiles:
        previously_scraped.append(i)



for index in sorted(previously_scraped, reverse=True):
    del session_ids[index]
    del session_names[index]
    del session_years[index]
    del session_list[index]

## Combine and Drop Previously Scraped
sessions = [[sid, name, yr] for sid, name, yr in zip(session_ids, session_names, session_years)]
this_year = datetime.datetime.now().year
sessions = [s for s in sessions if s[2] < this_year]

del session_ids, session_names, session_years

#######################################################
########## GET BILL URLS VIA FTP SITE FOR A SESSION
#####################################################
# session = sessions[0]

def get_session_bills(session):

    s_id = session[0]
    #s_name = session[1]
    #s_year = session[2]

    ## Go to Main Search Page
    search_url = 'https://mgaleg.maryland.gov/mgawebsite/search/legislation?session={}'.format(s_id)
    search_page = requests.get(search_url)
    search_soup = BeautifulSoup(search_page.content, 'lxml')

    ##### Create Bill Ranges from Panel on Left Hand Side [e.g, HB0001 - HB1464]

    dropdown_element = search_soup.find('select', {'id': 'valueCommittees'})
    committee_list = [option.get('value') for option in dropdown_element.find_all('option')[1:]]


    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    bill_info_list = []
    for comm in committee_list:
        leg_comm_url = 'https://mgaleg.maryland.gov/mgawebsite/Committees/Details/?cmte={}&ys={}&activeTab=divLegislation'.format(comm,s_id)
        driver.get(leg_comm_url)
        while True:
            comm_soup = BeautifulSoup(driver.page_source, 'lxml')
            table = comm_soup.find('table', {'id': 'billIndex'})
            if not table or 'No data available in table' in table.text:
                break
            for row in table.select('tbody tr'):
                bill_number_element = row.find('td').find('a')
                bill_number = bill_number_element.text.strip()
                bill_url_element = row.find('td').find('a')
                bill_url = 'https://mgaleg.maryland.gov' + bill_url_element['href']
                bill_info_list.append({'bill_number': bill_number, 'bill_url': bill_url})
            next_button = comm_soup.find('li', class_='paginate_button page-item next')
            try:
                driver.find_element(By.LINK_TEXT,'Next').click()
            except ElementClickInterceptedException:
                break

    unique_bill_numbers = set()
    unique_bill_info_list = []
    for entry in bill_info_list:
        bill_number = entry['bill_number']
        if bill_number not in unique_bill_numbers:
            unique_bill_info_list.append(entry)
            unique_bill_numbers.add(bill_number)

    return(unique_bill_info_list)


##############################################################
###### Functions to Create the URLs, Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):
    try:
        page = urllib.request.urlopen(bill_url, timeout = 20).read()
    except urllib.error.HTTPError: # as e
        return('HTTP Error')
    except socket.timeout:
        print("\n ~~> SOCKET TIMEOUT --- Retrying Bill Request")
        time.sleep(120)
        return(get_page_soup(bill_url))
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(15)
            page = urllib.request.urlopen(bill_url, timeout = 30).read()
        except urllib.error.HTTPError: # as e
            return('HTTP Error')
        except socket.timeout:
            print("\n ~~> SOCKET TIMEOUT --- Retrying Bill Request")
            time.sleep(120)
            return(get_page_soup(bill_url))
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = urllib.request.urlopen(bill_url, timeout = 60).read()
    time.sleep(.75)
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_num = session_bills[496]
# s = sessions[28]

def get_bill_data(s, bill):

    s_id = s[0]
    s_year = s[2]
    bill_num = bill['bill_number']
    bill_url = bill['bill_url']

    ### Scrape Bill Page
    bill_soup = get_page_soup(bill_url)

    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')
    elif 'Unable to retrieve the requested information' in bill_soup.get_text():
        return('No Data')

    title = bill_soup.find('dt', class_='col-sm-2 top-box-title').find_next('dd').text.strip()
    all_sponsors = bill_soup.find('dt', string='Sponsored by').find_next('dd').text.strip()
    status = bill_soup.find('dt', string='Status').find_next('dd').text.strip()

    primary_sponsor = [s.strip() for s in all_sponsors.split(',', 1)][0]
    primary_sponsor = ' '.join(primary_sponsor.split(' ', 1)[1:])

    chapter_num = ''
    if 'Chapter' in status:
        chapter_num = status.split('Chapter')[-1].strip()

    crossfile_bill = None

    details_section = bill_soup.find('div', class_='col-sm-2 details-section-name', string='Details')
    details_container = details_section.find_next('div', class_='col-sm-10')

    for detail_row in details_container.find_all('div', class_='container-fluid pl-0'):
        key = detail_row.find('div', class_='col-sm-12').get_text(strip=True)
        if key.startswith('Cross-filed with:'):
            crossfile_bill = detail_row.find('a').get_text(strip=True)

    committee_names = []
    committees_section = bill_soup.find('div', class_='col-sm-2 details-section-name', string='Committees')
    committees_container = committees_section.find_next('div', class_='col-sm-10')

    for committee_type in committees_container.find_all('div', class_='col-sm-6 p-0'):
        for committee_name_element in committee_type.find_all('a'):
            committee_name = committee_name_element.get_text(strip=True)
            if "Recorded Media" not in committee_name:
                committee_names.append(committee_name)

    committees = "; ".join(committee_names)

    topics_element = bill_soup.find('div', class_='details-section-name', string='File Code')
    topics_links = topics_element.find_all_next('div', class_='row collapse details-dropdown-content1')

    topics = []

    for link in topics_links:
        link_text = link.find('a')
        if link_text:
            topics.append(link_text.text.strip())

    topics_text = '; '.join(topics)

    subjects_element = bill_soup.find('div', class_='details-section-name', string='Subjects')
    subjects_rows = subjects_element.find_all_next('div', class_='row collapse details-dropdown-content2')

    subject_links = []

    for row in subjects_rows:
        link = row.find('a')
        if link:
            subject_links.append(link.text.strip())

    subjects = '; '.join(subject_links)

    summary_element = bill_soup.find('div', class_='details-section-name', string='Synopsis')
    summary = summary_element.find_next('div', class_='col-sm-10').text.strip()

    bill_actions = []
    history_section = bill_soup.find('div', class_='col-sm-2 details-section-name history-section')
    history_table = history_section.find_next('table', class_='table table-striped')

    if history_table is not None:
        history_rows = history_table.find_all('tr')[1:]  # Skip the header row
        order = 1
        for row in history_rows:
            dl_elements = row.find_all('dl', class_='row')
            for dl in dl_elements:
                chamber = dl.find('dt', class_='col-sm-2', string='Chamber').find_next('dd', class_='col-sm-10').text.strip()
                date = dl.find('dt', class_='col-sm-2', string='Legislative Date').find_next('dd', class_='col-sm-10').text.strip()
                action = dl.find('dt', class_='col-sm-2', string='Action').find_next('dd', class_='col-sm-10').text.strip()
                bill_actions.append([bill_num, s_id, s_year, chamber, date, action, order])
                order += 1

    ### OUTPUT ~~~ Note: Vote Urls are in the Documents Tab of the Newer Format
    bill_details = [bill_num, s_id, s_year, title, primary_sponsor, all_sponsors, status, crossfile_bill, committees, topics_text, subjects, summary, bill_url]
    return([bill_details, bill_actions])

########################################################
############## SCRAPE SESSION(S)
############################################
# bill_info = session_bills[4202]
# s = sessions[0]

for s in sessions:

    s_id = s[0]
    s_name = s[1]

    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'session_year', 'title', 'main_sponsor', 'all_sponsors', 'status', 'crossfile_bill', 'committees', 'general_topics', 'subjects', 'summary', 'bill_url']]
    session_actions = [['bill_number', 'session', 'session_year', 'chamber', 'action_date', 'action','order']]

    print('\n~~~~ Gathering Bill URLs for the {} ~~~~\n'.format(s_name))

    ### Get all bills for a specific session
    session_bills = get_session_bills(s)

    print('\n~~~~ Beginning Individual Bill Scraping for the {} ~~~~\n'.format(s_name))

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill in session_bills:

        bill_data = get_bill_data(s, bill)
        bill_num = bill['bill_number']
        bill_url = bill['bill_url']
        time.sleep(.5)

        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n **********".format(num, total, bill_num))
            num += 1
            continue
        elif bill_data == "No Data":
            print(" ********** \n ({}/{}) -- {} -- NO BILL PAGE! --- SKIPPING \n **********".format(num, total, bill_num))
            num += 1
            continue

        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_num, bill_data[0][12] ))
        num += 1

    with open("MD_Bill_Details_" + s_id + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("MD_Bill_Histories_" + s_id + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} SCRAPED + DATA SAVED  -------------\n\n\n".format(s_name))


print("  ********************************** ALL DONE ********************************** ")
