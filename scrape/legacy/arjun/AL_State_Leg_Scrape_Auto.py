# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape Alabama Bills

@author: PB
"""

##### NOTES:
# NEED TO GENERALIZE THIS TO UPDATE AFTER INITIAL SCRAPE
# *** Easiest way would be to check which terms have been scraped so far and not do those..
# -- IF re-scrape, fix the headers.... they don't get added in for some reason...
#########

import sys
import csv
import os
from urllib.request import urlopen
from bs4 import BeautifulSoup
import time
import re

from retry import retry
from timeout_decorator import timeout, TimeoutError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
#from selenium.common.exceptions import TimeoutException
#from selenium.webdriver.support.ui import WebDriverWait
#from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
#from selenium.webdriver.support import expected_conditions as EC
#from selenium.webdriver.common.by import By
#from selenium.webdriver.chrome.options import Options
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

########################
###### Extract Session Links
#########################

# *** Note: the site autoloads sessions via javascript... can't get a direct link to each session... ***
# *** Need to loop through sessions one at a time ***

session_list_page = urlopen('http://alisondb.legislature.state.al.us/Alison/SelectSession.aspx').read()
session_list_soup = BeautifulSoup(session_list_page, "lxml")

sessions_table = session_list_soup.find('table', {'id':'ContentPlaceHolder1_gvSessions'})
sessions_table = sessions_table.findAll("tr")

all_session_titles = []
for tr in sessions_table:
    this_session = tr.get_text().strip()
    if this_session != '':
        all_session_titles.append(this_session)

del session_list_page, session_list_soup, tr, this_session


#### Skip Previously Scraped
previously_scraped = []
dirFiles = os.listdir('.')
for file in dirFiles:
    if "AL_Bill_Details" in file:
        temp_session = re.sub("AL_Bill_Details_|.csv", "", file)
        temp_session = re.sub("_", " ", temp_session)
        previously_scraped.append(temp_session)
        print("Previously Scraped: " + temp_session)

all_session_titles = list(filter(lambda a: a not in previously_scraped, all_session_titles))

if len(all_session_titles) == 0:
    print("\n\n ---------- NO MORE SESSIONS! ----------- \n\n")
    sys.exit()

##############
#### Function to Extract Bill Details from Table
############
### *** This link seems to go to right place, conditional on having navigated to the section?
# http://alisondb.legislature.state.al.us/Alison/SESSBillStatusResult.aspx?BILL=HB2&WIN_TYPE=BillResult

def extract_bill_details(bill_page_soup, session):
    bill_table = bill_page_soup.find("table", {"id":"ContentPlaceHolder1_gvBills"})
    input_tags = bill_table.findAll("input") # bill_table.findAll("tr")

    this_table = []
    for input_tag in input_tags:
        if 'SponsorName$' in input_tag['onclick']:
            continue
        else:
            bill_id = input_tag['value']
            this_row = input_tag.parent.parent.findAll("td")
            sponsor = this_row[1].input['value']
            other_data = [j.get_text().strip() for j in this_row[2:len(this_row)-1]]
            temp_link = 'http://alisondb.legislature.state.al.us/Alison/SESSBillStatusResult.aspx?BILL=' + bill_id + '&WIN_TYPE=BillResult'
            this_table.append([bill_id, sponsor, session] + other_data + [temp_link])

    return(this_table)

#this_table = []
#remainder = 1 # Need to adjust this if there is no bill summary!
#for tr in range(1, len(bill_table)):
#    if tr % 3 == remainder:
#        this_row = bill_table[tr].findAll("td")
#        nextnext_row = bill_table[tr + 2]
#        bill_id = this_row[0].input['value']
#        sponsor = this_row[1].input['value']
#        other_data = [j.get_text().strip() for j in this_row[2:len(this_row)-1]]
#        short_title = nextnext_row.td.get_text().strip()
#        temp_link = 'http://alisondb.legislature.state.al.us/Alison/SESSBillStatusResult.aspx?BILL=' + bill_id + '&WIN_TYPE=BillResult'
#        this_table.append([bill_id, sponsor, session] + other_data + [short_title, temp_link])
#    else:
#        continue

####################
#### Function to Scrape Bill Details + Histories
##################

# page_soup = this_soup
def scrape_bill_hist(page_soup, b_id, sponsor, session):
    hist_table = page_soup.find("table", {"id":"ContentPlaceHolder1_gvHistory"})
    hist_table = hist_table.findAll("tr")
    all_rows = []
    for tr in hist_table[1:]:
        row_content = [j.get_text().strip() for j in tr.findAll("td")]
        all_rows.append([b_id, sponsor, session] + row_content)
    return(all_rows)


#################
### Function to Get and Retry Driver Get Calls
##################
#https://stackoverflow.com/questions/43003924/python-selenium-refresh-if-wait-more-than-10s

@retry(TimeoutError, tries=3)
@timeout(10)
def get_with_retry(driver, url):
    driver.get(url)


##########################
### OUTPUT LISTS
#########################

# bill_headers = [['bill_id', 'sponsor', 'session', 'subject', 'chamber', 'status', 'committee', 'last_action_date', 'temp_link', 'short_title']]
# hist_headers = [['bill_id', 'sponsor', 'session', 'action_date', 'chamber', 'amd_sub', 'action', 'committee', 'nay', 'yea', 'abstain', 'vote_id']]
# vote_details = ['bill_id', 'sponsor', 'session', 'vote_id']

#with open("AL_Bill_Details_Full.csv", "w", newline = "") as f:
#    writer = csv.writer(f)
#    writer.writerows(bill_headers)
#
#with open("AL_Bill_Histories.csv", "w", newline = "") as f:
#    writer = csv.writer(f)
#    writer.writerows(hist_headers)

#####################
###### SCRAPE
########################

chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(options=chrome_options)
#driver = webdriver.Firefox()

##########################################
##### LOOP THROUGH SESSIONS
for s in range(0,len(all_session_titles)):

    session_bills = [['bill_id', 'sponsor', 'session', 'subject', 'chamber', 'status', 'committee', 'last_action_date', 'temp_link', 'short_title']]
    session_histories = [['bill_id', 'sponsor', 'session', 'action_date', 'chamber', 'amd_sub', 'action', 'committee', 'nay', 'yea', 'abstain', 'vote_id']]

    time.sleep(1)

    #### Loop THROUGH CHAMBERS
    for chamber in ["HOUSE", "SENATE"]:

        if chamber == "HOUSE":
            chamber_link = 'ContentPlaceHolder1_btnHouse'
        else:
            chamber_link = 'ContentPlaceHolder1_btnSenate'

        ### Navigate to Session Page
        get_with_retry(driver, "http://alisondb.legislature.state.al.us/Alison/SelectSession.aspx")
        time.sleep(2)

        ## Unique Session Name
        this_session = all_session_titles[s]
        print("CURRENT " + chamber + " SESSION: " + this_session)

        ### Navigate to Session + Bills
        # driver.find_element(By.LINK_TEXT,this_session).click()
        driver.find_element(By.LINK_TEXT, this_session).click()
        time.sleep(2)

        ### NAVIGATE TO ALL BILLS --- Comment would work if could send as post request... with Viewstate...
        # driver.get("http://alisondb.legislature.state.al.us/Alison/SESSBillsList.aspx?MATTERTRANS={All}&SELECTEDDAY={All}&BODY=1755")
        driver.find_element(By.ID, "header_A3").click()
        bills_link = driver.find_element(By.LINK_TEXT, "Bills by Selected Matter Transaction")
        driver.execute_script("arguments[0].click();", bills_link)
        time.sleep(2)

        #### ******************************
        #### *** BILL DATA ***
        #### ******************************
        driver.find_element(By.ID, chamber_link).click()
        time.sleep(2)

        ### Show All Bills
        #matter_table = driver.find_element(By.ID, 'ContentPlaceHolder1_gvMatterTrans')
        driver.find_element(By.XPATH, "/html/body/form/div[3]/div[3]/div[4]/div[2]/div/table/tbody/tr[1]").click()
        time.sleep(10)

        ### Pull out Bill Details + Check that page is right
        bills_soup = BeautifulSoup(driver.page_source, "lxml")
        if bills_soup.findAll("span", {"id":"ContentPlaceHolder1_lblCount"}) == []:
            driver.back()
            time.sleep(2)
            driver.find_element(By.XPATH, "/html/body/form/div[3]/div[3]/div[4]/div[2]/div/table/tbody/tr[1]").click()
            time.sleep(10)
            bills_soup = BeautifulSoup(driver.page_source, "lxml")

        total_num = bills_soup.find("span", {"id":"ContentPlaceHolder1_lblCount"}).get_text().strip()

        ## Creating here in case no observations
        these_bill_histories = []
        these_bills = []
        these_res = []

        if total_num == '0 Instruments':
            print("\n ------ NO BILLS FOR SESSION: " + this_session + " in the " + chamber + " --------- \n")
        else:
            #### Pull Out Data from Table
            these_bills = extract_bill_details(bills_soup, this_session)
            print(chamber + " Bill Table Extracted: " + re.sub("Instruments", "Total Bills", total_num))

            ### Extract Bill Histories
            for i in range(0, len(these_bills)):
                get_with_retry(driver, these_bills[i][8])
                time.sleep(1)
                this_soup = BeautifulSoup(driver.page_source, "lxml")
                if 'Some unexpected error occured in the application' in this_soup.get_text():
                    time.sleep(5)
                    get_with_retry(driver, these_bills[i][8])
                    this_soup = BeautifulSoup(driver.page_source, "lxml")
                    print("RETRY - UNEXPECTED ERROR")
                    if 'Some unexpected error occured in the application' in this_soup.get_text():
                        continue
                        print("RETRY FAILED - SKIPPING BILL")
                elif this_soup.find("table", {"id":"ContentPlaceHolder1_gvHistory"}) == None:
                    time.sleep(5)
                    get_with_retry(driver, these_bills[i][8])
                    this_soup = BeautifulSoup(driver.page_source, "lxml")
                    print("RETRY - NONETYPE OBJECT")

                short_title = this_soup.find('span', id = 'ContentPlaceHolder1_lblShotTitle').get_text().strip()
                these_bills[i] = these_bills[i] + [short_title]
                this_hist = scrape_bill_hist(this_soup, these_bills[i][0], these_bills[i][1], these_bills[i][2])

                ### Check for date errors --- IF any dates that are > 1 year over session (e.g., a 2005 in a 2003 session), retry bill page
                dates = [re.sub('.+/', '', d[3]) for d in this_hist if re.sub('.+/', '', d[3]) != '']
                dates = list(set([int(d) for d in dates]))
                sy = int(re.sub('.+ ', '', this_session))
                bad_dates = [d for d in dates if sy + 1 < d]
                if(bad_dates != []):
                    get_with_retry(driver, these_bills[i][8])
                    time.sleep(1)
                    this_soup = BeautifulSoup(driver.page_source, "lxml")
                    short_title = this_soup.find('span', id = 'ContentPlaceHolder1_lblShotTitle').get_text().strip()
                    these_bills[i] = these_bills[i] + [short_title]
                    this_hist = scrape_bill_hist(this_soup, these_bills[i][0], these_bills[i][1], these_bills[i][2])

                ## Save Histories
                for hist_row in this_hist:
                    these_bill_histories.append(hist_row)
                print("---- Bill History ~ " + str(i))

        #### ******************************
        #### *** RESOLUTION DATA ***
        #### ******************************
        driver.find_element(By.LINK_TEXT,"Resolutions").click()
        time.sleep(2)
        res_link = driver.find_element(By.LINK_TEXT,"Resolutions by Selected Matter Transaction")
        driver.execute_script("arguments[0].click();", res_link)
        time.sleep(2)

        ### *** BILL DATA***
        driver.find_element(By.ID, chamber_link).click()
        time.sleep(2)

        ### Show All Resolutions
        #matter_table = driver.find_element(By.ID, 'ContentPlaceHolder1_gvMatterTrans')
        driver.find_element(By.XPATH, "/html/body/form/div[3]/div[3]/div[4]/div[2]/div/table/tbody/tr[1]").click()
        time.sleep(10)

        ### Pull out Bill Details
        res_soup = BeautifulSoup(driver.page_source, "lxml")
        if res_soup.findAll("span", {"id":"ContentPlaceHolder1_lblCount"}) == []:
            driver.back()
            time.sleep(2)
            driver.find_element(By.XPATH, "/html/body/form/div[3]/div[3]/div[4]/div[2]/div/table/tbody/tr[1]").click()
            time.sleep(10)

        total_num = res_soup.find("span", {"id":"ContentPlaceHolder1_lblCount"}).get_text().strip()

        if total_num == '0 Instruments':
            print("\n ------ NO RESOLUTIONS FOR SESSION: " + this_session + " in the " + chamber + " --------- \n")
        else:
            #### Pull Out Data from Table
            these_res = extract_bill_details(res_soup, this_session)
            print(chamber + " Resolution Table Extracted: " + re.sub("Instruments", "Total Resolutions", total_num))

            ### Extract Resolution Histories + Append to BIll FIlE
            for i in range(0, len(these_res)):
                get_with_retry(driver, these_res[i][8])
                time.sleep(1)
                this_soup = BeautifulSoup(driver.page_source, "lxml")
                if 'Some unexpected error occured in the application' in this_soup.get_text():
                    time.sleep(5)
                    get_with_retry(driver, these_res[i][8])
                    this_soup = BeautifulSoup(driver.page_source, "lxml")
                    print("RETRY - UNEXPECTED ERROR")
                    if 'Some unexpected error occured in the application' in this_soup.get_text():
                        continue
                        print("RETRY FAILED - SKIPPING BILL")
                elif this_soup.find("table", {"id":"ContentPlaceHolder1_gvHistory"}) == None:
                    time.sleep(5)
                    get_with_retry(driver, these_res[i][8])
                    this_soup = BeautifulSoup(driver.page_source, "lxml")
                    print("RETRY - NONETYPE OBJECT")

                short_title = this_soup.find('span', id = 'ContentPlaceHolder1_lblShotTitle').get_text().strip()
                these_res[i] = these_res[i] + [short_title]
                this_hist = scrape_bill_hist(this_soup, these_res[i][0], these_res[i][1], these_res[i][2])

                ### Check for date errors --- IF any dates that are > 1 year over session (e.g., a 2005 in a 2003 session), retry bill page
                dates = [re.sub('.+/', '', d[3]) for d in this_hist if re.sub('.+/', '', d[3]) != '']
                dates = list(set([int(d) for d in dates]))
                sy = int(re.sub('.+ ', '', this_session))
                bad_dates = [d for d in dates if sy + 1 < d]
                if(bad_dates != []):
                    get_with_retry(driver, these_res[i][8])
                    time.sleep(1)
                    this_soup = BeautifulSoup(driver.page_source, "lxml")
                    short_title = this_soup.find('span', id = 'ContentPlaceHolder1_lblShotTitle').get_text().strip()
                    these_res[i] = these_res[i] + [short_title]
                    this_hist = scrape_bill_hist(this_soup, these_res[i][0], these_res[i][1], these_res[i][2])

                for hist_row in this_hist:
                    these_bill_histories.append(hist_row)
                print("---- Resolution History ~ " + str(i))

        ### *** Could Insert Something With the Votes Here ***

        ### Aggregate
        session_bills = session_bills + these_bills + these_res
        session_histories = session_histories + these_bill_histories
        print("\n---------- Chamber: " + chamber + " --- Session: " + this_session + " --------------- \n" )

    ### SAVE
    session = re.sub(" ", "_", this_session)

    with open("AL_Bill_Details_" + session + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bills)

    with open("AL_Bill_Histories_" + session + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_histories)

    print("\n ************************* " + this_session + " DATA SAVED ************************ \n" )
    time.sleep(5)








##################
#### OUPTUT LISTS
###################
#
#start_loop = int(input("Where do you want to start the loop? If at beginning, enter 1: "))
#
#if start_loop == 1:
#    all_histories = [['bill_id', 'session', 'bill_url', 'special_format', 'date', 'action', 'order']]
#    all_data = [['bill_id', 'session', 'bill_url', 'special_format', 'companion_bill_id', 'short_title', 'sponsor', 'cosponsors', 'companion_sponsor', 'fiscal_summary', 'summary']]
#    all_votes = [['bill_id', 'session', 'bill_url', 'special_format', 'house_votes', 'senate_votes']]
#else:
#    all_histories = []
#    all_data = []
#    all_votes = []
#
#skipped = []
