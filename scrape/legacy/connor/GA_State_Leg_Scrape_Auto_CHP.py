#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created Dec 3, 2018

Scrape GA Bills 2000+

@author: AV
"""

################## NOTES:
# Votes should be easily scrapable if wanted --- Might be easier to get them here than what I'm scraping in the aggregate
# ---- http://www.legis.ga.gov/Legislation/en-US/VoteList.aspx?Chamber=2

# ******* Can get the 3 terms before 2000 at: http://www.legis.ga.gov/legislation/Archives/Default.aspx *******************

import csv
import os
import re
import time
import urllib
from bs4 import BeautifulSoup
import socket
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

#################################
#### Get Sessions
#################################



# Initialize a headless Selenium WebDriver
chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(options=chrome_options)

# Navigate to the page
url = "https://www.legis.ga.gov/search?s=1031&p=1"
driver.get(url)

# Wait for the content to be fully loaded (you may need to adjust the timeout)
wait = WebDriverWait(driver, 30)
wait.until(EC.presence_of_element_located((By.CLASS_NAME, "container")))

time.sleep(5)

# Get the page source
page_source = driver.page_source

# Parse the page source with BeautifulSoup
search_soup = BeautifulSoup(page_source, 'html.parser')
# print(search_soup)


sessions = search_soup.find('select', {'id':'session'})
sessions = sessions.find_all('option')

#current_year = str(datetime.datetime.now().year)


## Create List of Sessions and DROP IF ALREADY SCRAPED
previously_scraped = [t.replace(".csv", "").replace( "GA_Bill_Details_", "") for t in os.listdir('.')]
previously_scraped.append("Previous_Sessions")
session_list = [[s.text.strip(), s['value']] for s in sessions if s.text.strip().replace(' ', '_').replace('-', '_') not in previously_scraped]
#session_list = [item for item in session_list if current_year not in item[0]]
### Drop Sessions Before 2023
session_list = [item for item in session_list if int(item[0][:4]) >= 2023]
driver.quit()

#################################
## GET BILL PAGE URLS
#################################

# chrome_options = Options()
# chrome_options.add_argument("--headless")

# this_session = session_list[5]
def get_session_bills(this_session):
    print("\n *** Gathering Bill URLs for the {} *** \n".format(this_session[0]))
    driver = None
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(15)
    try:
        driver.get('http://www.legis.ga.gov/Legislation/en-US/Search.aspx')
    except TimeoutException as e:
        print("~~~> Timeout! -- Retry in 20 Seconds")
        time.sleep(20)
        driver.quit()
        driver = webdriver.Chrome(options=chrome_options)
        driver.get('http://www.legis.ga.gov/Legislation/en-US/Search.aspx')
    time.sleep(10)
    session_select = Select(driver.find_element(By.ID,'session'))
    session_select.select_by_value(this_session[1])
    search_button = driver.find_element(By.CLASS_NAME, "btn-primary")
    search_button.click()
    time.sleep(10)
    current_page = 1
    last_page = driver.find_element(By.CLASS_NAME, "pagination")
    underlying_elements = last_page.find_elements(By.TAG_NAME, "a")  # You can specify a different tag name as needed
    try:
        last_page = int(underlying_elements[-2].text)
    except:
        last_page = 1
    session_bills = [['bill_num', 'session', 'title', 'bill_url']]
    while True:
        bill_results = BeautifulSoup(driver.page_source, 'lxml')
        bill_table = driver.find_element(By.TAG_NAME, "tbody")
        bill_rows = bill_table.find_elements(By.TAG_NAME, "tr")
        for row in bill_rows:
            bill_link_element = row.find_element(By.TAG_NAME, "a")
            bill_num = bill_link_element.text.replace("\xa0", " ")
            bill_url = bill_link_element.get_attribute("href").replace("..", "")
            bill_title = row.find_elements(By.XPATH, "*")[1].text.strip().replace("\xa0", " ")
            session_bills.append([bill_num, this_session[0], bill_title, bill_url])
        print(" -- {} of {}".format(current_page, last_page))
        if this_session[0] == '2001 2nd Special Session' and current_page == 5:
            print("~~~ Skipping Error Page - 2001 2nd Special, Page 6")
            current_page += 2
            page_select = Select(driver.find_element_by_name("ctl00$SPWebPartManager1$g_b223cc53_ceb0_41fe_85ca_0c60eb699ad8$ctl05"))
            page_select.select_by_value('7')
            time.sleep(5)
        elif current_page + 1 <= last_page:
            current_page +=1
            next_page_link = driver.find_element(By.XPATH, "//a[@class='page-link' and contains(@aria-label, 'Next')]")
            next_page_link.click()
            time.sleep(5.5)
        else:
            print("\n *** Found ALL URLs for the {} *** \n".format(this_session[0]))
            break
    driver.close()
    return(session_bills)



##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################
#
# def get_page_soup(bill_url, parser = 'lxml'):
#     ### Get HTML
#     try:
#         page = urllib.request.urlopen(bill_url, timeout = 20).read()
#     #except urllib.error.HTTPError: # as e
#     #    return('HTTP Error')
#     except socket.timeout:
#         print("\n ~~> SOCKET TIMEOUT --- Retrying Bill Request")
#         time.sleep(120)
#         return(get_page_soup(bill_url))
#     except:
#         try:
#             print("\n ~~> Retrying Bill Request")
#             time.sleep(15)
#             page = urllib.request.urlopen(bill_url, timeout = 30).read()
#         except urllib.error.HTTPError: # as e
#             return('HTTP Error')
#         except socket.timeout:
#             print("\n ~~> SOCKET TIMEOUT --- Retrying Bill Request")
#             time.sleep(120)
#             return(get_page_soup(bill_url))
#         except:
#             print("\n ~~> Retrying Bill Request x 2")
#             time.sleep(60)
#             page = urllib.request.urlopen(bill_url, timeout = 60).read()
#     time.sleep(10)
#     ### Return Soup
#     page_soup = BeautifulSoup(page, parser)
#     return(page_soup)

def get_page_soup(bill_url, parser='lxml',time_sleep=5):
    driver = None
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(45)
    try:
        driver.get(bill_url)
    except Exception as e:
        print("~~~> Timeout! -- Retry in 20 Seconds")
        time.sleep(60)
        driver.quit()
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(bill_url)
    time.sleep(time_sleep)
    page_source = driver.page_source
    search_soup = BeautifulSoup(page_source, parser)
    return(search_soup)


#################################
## Function to Scrape BILL DATA
#################################
# bill_row = session_bills[75]
# http://www.legis.ga.gov/Legislation/en-US/display/20172018/HB/75

def fix_date(date_str):
    date_str = date_str.replace("Jan", "1").replace("Feb", "2").replace("Mar", "3").replace("Apr", "4").replace("May", "5").replace("Jun", "6")
    date_str = date_str.replace("Jul", "7").replace("Aug", "8").replace("Sep", "9").replace("Oct", "10").replace("Nov", "11").replace("Dec", "12")
    return(date_str)

def get_bill_data(bill_row, this_session):
    this_bill_num = bill_row[0]
    this_title = bill_row[2]
    this_url = bill_row[3]
    bill_soup = get_page_soup(this_url)
    bill_soup.find('tbody')
    # sponsors = bill_soup.find('div', text = "Sponsored By")
    # sponsors = sponsors.findNext("div")
    sponsors = '; '.join([f"{sponsor.get_text(strip=True)} {district.get_text(strip=True)}" for sponsor, district in zip(bill_soup.select('table.table-link a'), bill_soup.select('table.table-link td:nth-child(3)'))])
    if sponsors == "":
        print("Retrying bill soup [sponsors]")
        bill_soup = get_page_soup(this_url,time_sleep=45)
        bill_soup.find('tbody')
        sponsors = '; '.join([f"{sponsor.get_text(strip=True)} {district.get_text(strip=True)}" for sponsor, district in zip(bill_soup.select('table.table-link a'), bill_soup.select('table.table-link td:nth-child(3)'))])
    try:
        other_chamber_sponsors = '; '.join([sponsor.get_text(strip=True) for sponsor in bill_soup.select('div.sponsorByPanel td a')])
    except:
        try:
            print("Retrying bill soup [other_chamber_sponsors]")
            bill_soup = get_page_soup(this_url,time_sleep=15)
            bill_soup.find('tbody')
            other_chamber_sponsors = '; '.join([sponsor.get_text(strip=True) for sponsor in bill_soup.select('div.sponsorByPanel td a')])
        except:
            other_chamber_sponsors = ''
    committees = bill_soup.find('h2', string = "Committees")
    # Assuming you have the 'committees' element
    try:
        committee_divs = committees.find_next('div', class_='card-body').find_all('div', class_='card-text-sm col-lg-4')
        # Extract committee names
        house_comm = next((div.find_next('a').get_text(strip=True) for div in committee_divs if 'House Committee:' in div.get_text(strip=True)), None)
        senate_comm = next((div.find_next('a').get_text(strip=True) for div in committee_divs if 'Senate Committee:' in div.get_text(strip=True)), None)
    except:
        try:
            print("Retrying bill soup [committee_divs]")
            bill_soup = get_page_soup(this_url,time_sleep=15)
            bill_soup.find('tbody')
            committee_divs = committees.find_next('div', class_='card-body').find_all('div', class_='card-text-sm col-lg-4')
            # Extract committee names
            house_comm = next((div.find_next('a').get_text(strip=True) for div in committee_divs if 'House Committee:' in div.get_text(strip=True)), None)
            senate_comm = next((div.find_next('a').get_text(strip=True) for div in committee_divs if 'Senate Committee:' in div.get_text(strip=True)), None)
        except:
            house_comm = ''
            senate_comm = ''
    try:
        summary = bill_soup.find('h2', text='First Reader Summary').find_next('div', class_='card-text-sm').get_text(strip=True)
    except:
        try:
            print("Retrying bill soup [summary]")
            bill_soup = get_page_soup(this_url,time_sleep=15)
            bill_soup.find('tbody')
            summary = bill_soup.find('h2', text='First Reader Summary').find_next('div', class_='card-text-sm').get_text(strip=True)
        except:
            summary = ''


    # Assuming status_history_table contains the status history table
    this_hist = []
    these_votes = []

    # Find the "Status History" table and extract votes
    try:
        status_history_table = bill_soup.find('h2', text='Status History').find_next('table', class_='table')
        status_history_rows = status_history_table.find_all('tr')

        order = len(status_history_table.find_all("tr"))-1
        for tr in status_history_table.find_all("tr"):
            td_list = tr.find_all("td")
            if len(td_list) != 2:  # Ignore headers if present
                continue
            date = fix_date(td_list[0].text.strip())
            status = td_list[1].text.strip()
            # Assuming you have this_bill_num and this_session defined earlier
            this_hist.append([this_bill_num, this_session[0], date, status, order])
            order -= 1
        try:
            ## All First-Order Child Divs (Votes) of Vote Section
            votes_table = bill_soup.find('h2', string='Votes').find_next('table')
            # Iterate through each row in the table (skip the header row with [1:])
            for row in votes_table.find_all('tr')[1:]:
                # Extract the cells in the row
                cells = row.find_all('td')
                # Extract the date from the first cell and use the fix_date function
                date_str = cells[0].text.strip()
                vdate = fix_date(date_str)
                # Extract the vote ID from the second cell
                vote_id = cells[1].text.strip()
                # Extract the outcome columns (excluding the first two cells)
                outcome = [f"{label}({cell.text.strip()})" for label, cell in zip(["Yea", "Nay", "NV", "Exc"], cells[2:])]
                # Append the data to the these_votes list
                these_votes.append([vdate, vote_id] + outcome )
        except:
            pass
    # If page didn't load properly then reload soup and try again
    except:
        try:
            print("Retrying bill soup")
            bill_soup = get_page_soup(this_url,time_sleep=15)
            bill_soup.find('tbody')
            status_history_table = bill_soup.find('h2', text='Status History').find_next('table', class_='table')
            status_history_rows = status_history_table.find_all('tr')

            order = len(status_history_table.find_all("tr"))
            for tr in status_history_table.find_all("tr"):
                td_list = tr.find_all("td")
                if len(td_list) != 2:  # Ignore headers if present
                    continue
                date = fix_date(td_list[0].text.strip())
                status = td_list[1].text.strip()
                # Assuming you have this_bill_num and this_session defined earlier
                this_hist.append([this_bill_num, this_session[0], date, status, order])
                order -= 1
            try:
                ## All First-Order Child Divs (Votes) of Vote Section
                votes_table = bill_soup.find('h2', string='Votes').find_next('table')
                # Iterate through each row in the table (skip the header row with [1:])
                for row in votes_table.find_all('tr')[1:]:
                    # Extract the cells in the row
                    cells = row.find_all('td')
                    # Extract the date from the first cell and use the fix_date function
                    date_str = cells[0].text.strip()
                    vdate = fix_date(date_str)
                    # Extract the vote ID from the second cell
                    vote_id = cells[1].text.strip()
                    # Extract the outcome columns (excluding the first two cells)
                    outcome = [f"{label}({cell.text.strip()})" for label, cell in zip(["Yea", "Nay", "NV", "Exc"], cells[2:])]
                    # Append the data to the these_votes list
                    these_votes.append([vdate, vote_id] + outcome )
            except:
                pass
        except:
            pass

    bill_details = [this_bill_num, this_session[0], sponsors, other_chamber_sponsors, this_title, house_comm, senate_comm, summary, this_url]
    return([bill_details, this_hist, these_votes])


####################################
## Loop Through Sessions, Get Bill Details
#####################################


for s in session_list:
    ### Get List of Bills and Basic Details
    session_bills = get_session_bills(s)
    session_bill_details = [['bill_number', 'session', 'sponsors', 'oppo_chamber_sponsors', 'title', 'house_comm', 'senate_comm', 'summary', 'bill_url']]
    session_actions = [['bill_number', 'session', 'action_date', 'action', 'order']]
    session_votes = [['bill_number', 'session', 'vote_date', 'vote_id', 'num_yeas', 'num_no', 'num_notvoting', 'num_excused']]
    ## Could scrape the actual roll calls via the rollcallID later if ever needed
    print(" ~~~> Scraping Individual Bills - Session {}".format(s))
    ### Scrape Each Bill
    num = 1
    num_bills = len(session_bills[1:])
    for b in session_bills[1:]:
        ### Get Bill Data
        try:
            this_bill = get_bill_data(b, s)
            time.sleep(1)

            ### Append to Agg Files
            session_bill_details.append(this_bill[0])

            for action_row in this_bill[1]:
                session_actions.append(action_row)

            if this_bill[2] != []:
                for vote_row in this_bill[2]:
                    session_votes.append(vote_row)

            print(" -- ({}/{}) -- {} -- URL: {}".format(num, num_bills, b[0], b[3]))
            num += 1
        except:
            print("failed at index ", b)
            break

    ### SAVE!
    session_adj = s[0].replace(" ", "_").replace("-", "_")
    with open("GA_Bill_Details_" + session_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("GA_Bill_Histories_" + session_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    with open("GA_Agg_Votes_" + session_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_votes)

    print("\n\n *********** {} DONE! ************** \n\n".format(s[0]))


print(" ~~~~~~~~~~~~~ ************** ALL SESSIONS DONE *************** ~~~~~~~~~~~~~~~~ ")
