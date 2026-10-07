# -*- coding: utf-8 -*-
"""

Scrape Indiana Bills

Last Updated: May 2020

@author: PB
"""

##### NOTES:
#
# This Scraper gathers ONLY data from 2014+
# --> See "IN_State_Leg_Scrape_OLD for possibly defunct scraper for 1999-2013
#
# There is an API (presumably, 2014+ only): http://docs.api.iga.in.gov/introduction.html
#
# *** For 2014, many sponsors are missing -- can get them from actions
#
###########################

import csv
import os
import urllib
import urllib.request
from bs4 import BeautifulSoup
import time
from time import sleep
import datetime
import re
import socket
import json

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys
from pathlib import Path
import requests

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#######################
##### Extract Session Links
########################

##### 2014+ -- By following link to THIS YEAR it will hide that year in the list
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
this_year = datetime.datetime.today().year
next_year = this_year + 1
driver = webdriver.Chrome(options=chrome_options)

# Open the URL in the browser


# Send a GET request to the URL
driver.get('https://iga.in.gov/legislative/{}/bills'.format(this_year))
session_year_dropdown = WebDriverWait(driver, 60).until(
    EC.presence_of_element_located((By.CLASS_NAME, 'SessionDropdown_sessionYearDropdown__3aOr0'))
)
session_year_dropdown.click()
time.sleep(2)

# get Years
soup = BeautifulSoup(driver.page_source, 'html.parser')
options_div = soup.find('div', class_='css-1ap4tjg-menu')
options = options_div.find_all('div', class_='css-16at8we-option')  # Adjust the class based on your HTML structure
sessions = [option.text.strip() for option in options]



sessions = [i for i in sessions if 'Previous' not in i]
sessions = [i for i in sessions if str(this_year) not in i]
sessions = [i for i in sessions if str(next_year) not in i]
sessions = [re.sub(' Session', '', i) for i in sessions]


### Drop Previously Scraped
keep_index = [index for index, s in enumerate(sessions) if 'IN_Bill_Details_' + s.replace(' ', '_') + '.csv' not in os.listdir('.')]
sessions = [s for index, s in enumerate(sessions) if index in keep_index]
print(sessions)

#######################################################
########## GET BILL URLS VIA FTP SITE FOR A SESSION
#####################################################
# session = sessions[0]
# session_url = session_urls[0]

def get_session_bills(session):

    session_bills = []
    driver.get('https://iga.in.gov/legislative/{}/bills'.format(session.replace(' Special','ss1')))
    senate_bills_div = WebDriverWait(driver, 60).until(
        EC.presence_of_element_located((By.XPATH, "//h2[@class='bills_billsListTitle__1C8AD' and text()='Senate Bills']"))
    )
    bill_list_soup = BeautifulSoup(driver.page_source, 'lxml')

    senate_bills_div = bill_list_soup.find("h2", class_="bills_billsListTitle__1C8AD", string="Senate Bills")
    senate_bills = senate_bills_div.find_next("ul", class_="bills_billsListContent__3QgFt")
    for bill in senate_bills.find_all("li", class_="bills_billsListItem__3D3Ap"):
        bill_info = bill.text.split(": ", 1)
        bill_number = bill_info[0]
        bill_name = bill_info[1]
        bill_link = 'https://iga.in.gov' + bill.find("a")["href"]
        session_bills.append([session,bill_number,bill_name,bill_link])

    house_bills_div = bill_list_soup.find("h2", class_="bills_billsListTitle__1C8AD", string="House Bills")
    house_bills = house_bills_div.find_next("ul", class_="bills_billsListContent__3QgFt")
    for bill in house_bills.find_all("li", class_="bills_billsListItem__3D3Ap"):
        bill_info = bill.text.split(": ", 1)
        bill_number = bill_info[0]
        bill_name = bill_info[1]
        bill_link = 'https://iga.in.gov' + bill.find("a")["href"]
        session_bills.append([session,bill_number,bill_name,bill_link])

    driver.get('https://iga.in.gov/legislative/{}/resolutions'.format(session.replace(' Special','ss1')))
    resolution_lists = WebDriverWait(driver, 60).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.resolutions_resolutionsList__17l72'))
    )
    res_list_soup = BeautifulSoup(driver.page_source, 'lxml')
    resolution_lists = res_list_soup.select('.resolutions_resolutionsList__17l72')
    for resolution_list in resolution_lists:
        resolution_items = resolution_list.select('.resolutions_resolutionsListItem__3-QcP')
        for resolution_item in resolution_items:
            resolution_number = resolution_item.text.strip().split(':')[0]
            resolution_title = resolution_item.text.strip().split(': ')[1]
            resolution_link = 'https://iga.in.gov' + resolution_item.find('a')['href']
            session_bills.append([session,resolution_number,resolution_title,resolution_link])


    return(session_bills)


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_url = 'http://iga.in.gov/legislative/2017/bills/senate/1'
# bill_info = session_bills[0]

def get_bill_data(bill_info):

    this_session = bill_info[0]
    bill_num = bill_info[1]
    short_title = bill_info[2]
    bill_url = bill_info[3]
    if bill_num[:2] in ['HB', 'SB']:
        bill_type = 'bill'
    else:
        bill_type = 'resolution'

    ### Adjust bill number to 4 digits
    #bill_num_z = re.sub('[0-9]+', '', bill_num) + re.sub('[A-Z]+', '', bill_num).zfill(4)

    ### Create New URL that goes to Backend Data
    # -- /actions returns both bill and action information
    #    if bill_num[:2] in ['HB', 'SB']:
    #        if bill_num[0] == "H":
    #            backend_url = re.sub('(?<=house\\/).+', '', bill_url) + bill_num_z + '/actions'
    #        else:
    #            backend_url = re.sub('(?<=senate\\/).+', '', bill_url) + bill_num_z + '/actions'

    #### Pull Data
    driver.get(bill_url)
    time.sleep(2)
    author_sponsor_elements = WebDriverWait(driver, 60).until(
        EC.presence_of_all_elements_located((By.CLASS_NAME, 'BillDetails_memberList__1YTDP'))
    )



    bill_soup = BeautifulSoup(driver.page_source, "html.parser")

    author_sponsor_elements = bill_soup.find_all('div', class_='BillDetails_memberList__1YTDP')
    authors = ""
    cosponsors = ""
    coauthors = ""
    for element in author_sponsor_elements:
        text = element.text.strip()
        if "Co-Authored by:" in text:
            coauthors = '; '.join([c.text.strip() for c in element.find_all('a')])
        elif "Authored by:" in text:
            authors = '; '.join([a.text.strip() for a in element.find_all('a')])
        elif "Sponsored by:" in text:
            cosponsors = '; '.join([s.text.strip() for s in element.find_all('a')])

    try:
        time.sleep(1)
        view_more_button = driver.find_element(By.XPATH, '//button[text()="... View more"]')
        if view_more_button.is_displayed():
            view_more_button.click()
            time.sleep(1)
    except NoSuchElementException:
        pass

    # Extracting digest
    digest_element = bill_soup.find('div', class_='BillDetails_digest__3rGrH')
    digest_paragraphs = digest_element.find_all('p')
    summary = ' '.join([p.text.strip() for p in digest_paragraphs])
    summary = summary.replace('\xa0View less','')

    ##### Actions
    bill_actions = []
    try:
        driver.get(bill_url + '/actions')
        action_elements = WebDriverWait(driver, 60).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, 'BillAction_billAction__1E72S'))
        )
        action_soup = BeautifulSoup(driver.page_source, "html.parser")
        action_elements = action_soup.find_all('p', class_='BillAction_billAction__1E72S')

        order = 1
        for action_element in action_elements:
            chamber_text = action_element.find('span', class_='BillAction_bold__2LaQy').text.strip()
            chamber = chamber_text[0]
            date_text = action_element.find('span', class_='BillAction_bold__2LaQy').text.strip()
            date_start = date_text.find(" ") + 1  # Find the space to get the start index
            date = date_text[date_start:]
            action_text = ' '.join(action_element.stripped_strings)
            words = action_text.split()
            action = ' '.join(words[2:])
            bill_actions.append([bill_num, this_session, date, chamber, action, order])
            order += 1
    except:
        pass


    ### OUTPUT
    bill_details = [bill_num, this_session, short_title, authors, coauthors, cosponsors, summary, bill_url]

    return([bill_details, bill_actions])


########################################################
############## SCRAPE SESSION(S)
############################################
# sy = sessions[0]
# sy_url = session_urls[0]
# bill_info = session_bills[1]

for session in sessions:

    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'title', 'authors',  'coauthors', 'cosponsors', 'summary', 'bill_url']]
    session_actions = [['bill_number', 'session', 'action_date', 'chamber', 'action','order']]

    print("\n\n ------------------- Now Scraping: Session " + session + " ---------------------- \n")

    ### Get all bills for a specific session
    session_bills = get_session_bills(session)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_info[2], bill_info[3] ))
        ### Skip Vehicle Bills (Blank, no sponsor, placeholders for eventual proposals)
        if re.search('^vehicle.+(bill|resolution)', bill_info[2].lower()):
            num += 1
            continue

        ### Get and Parse Bill Data
        bill_data = get_bill_data(bill_info)

        ### Append To BIll and Actions Files
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        num += 1

    ### SAVE!
    with open("IN_Bill_Details_" + session.replace(' ', '_') + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("IN_Bill_Histories_" + session.replace(' ', '_') + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- " + session + " SCRAPED + DATA SAVED  -------------\n\n\n")


print("  ********************************** ALL DONE ********************************** ")
