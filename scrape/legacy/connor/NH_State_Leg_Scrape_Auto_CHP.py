# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape NEW HAMPSHIRE Legislation 1993 - PRESENT ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# 2011 - 2017 Data available as downloads here: http://gencourt.state.nh.us/downloads
####################


import csv
import os
import urllib
#import requests
from bs4 import BeautifulSoup
import time
import datetime
#from dateutil import parser as dateparser
import re
import socket

from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.firefox.options import Options
# from selenium.webdriver.safari.options import Options
from pathlib import Path
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#####################################
##### Extract Session Information
#######################################

### Sessions are years
this_year = datetime.datetime.now().year
sessions = [i for i in range(2023, this_year + 1) if 'NH_Bill_Details_' + str(i) + '.csv' not in os.listdir('.')]

del this_year


#### Selenium Options
driver_options = Options()
# driver_options.add_argument("--headless")

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s_yr = sessions[0]
# test = get_session_bills(1989)

def get_session_bills(s_yr):

    print('\n\n~~~~ Gathering Bill URLs for the {} Session ~~~~\n'.format(s_yr))

    session_bills = []

    ## URls Stems to List of Chamber Bills and Resolutions
    search_url = 'https://www.gencourt.state.nh.us/bill_status/legacy/bs2016/'

    #### This Misses Senate Bills starting in 2009... Odd.. Just scrapes partial page
    #### ---> Possible this means missingness in terms prior as well
    #    search_page = requests.get(search_url)
    #    search_soup = BeautifulSoup(search_page.content, 'lxml')
    #
    #    form_data = {
    #        '__VIEWSTATE' : search_soup.select('#__VIEWSTATE')[0]['value'],
    #        '__VIEWSTATEGENERATOR' : search_soup.select('#__VIEWSTATEGENERATOR')[0]['value'],
    #        '__EVENTVALIDATION' : search_soup.select('#__EVENTVALIDATION')[0]['value'],
    #        'txtsessionyear': s_yr,
    #        'cmdsubmit':'Submit'
    #        }
    #
    #    ### Get Bill Pages
    #    list_req = requests.post(search_url, data = form_data)
    #    list_soup = BeautifulSoup(list_req.content, 'lxml')

    driver = webdriver.Chrome(options=driver_options)
    #driver = webdriver.Firefox()   # Firefox needs to be downloaded but autoupdates on call
    #driver = webdriver.Safari()

    ### Prep and Run Search
    driver.get(search_url)
    input_year = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'txtsessionyear')))
    input_year.clear()
    input_year.send_keys(s_yr)
    input_year.send_keys(Keys.ENTER)

    time.sleep(15)

    ## Getting Big Table, then Finding the Nested Tables, which include bill info
    list_soup = BeautifulSoup(driver.page_source, "html.parser")
    bill_list = list_soup.findAll('table')[2]
    bill_rows = bill_list.findAll('table')
    driver.close()

    for tab in bill_rows[0:-1]:
        bill = tab.find_parent('tr')
        bill_num = bill.find('td').find('big').text.strip()
        bill_num = re.sub('\r|\n|\t+', '', bill_num)
        hist_url = 'https://www.gencourt.state.nh.us/bill_status/legacy/bs2016/' + bill.find('td').find('a', string = re.compile('Docket'))['href']
        bill_url = 'http://www.gencourt.state.nh.us/bill_status/' + bill.find('td').find('a', string = re.compile('Status'))['href']
        vote_url = bill.find('td').find('a', string = re.compile('Roll'))
        if vote_url is not None:
            vote_url = 'http://www.gencourt.state.nh.us/bill_status/' + vote_url['href']
        else:
            vote_url = ''
        title = tab.parent.find('b', string = re.compile('Title:')).nextSibling.strip()
        session_bills.append([bill_num, s_yr, title, bill_url, hist_url, vote_url])


    return(session_bills)


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):
    try:
        page = urllib.request.urlopen(bill_url, timeout = 20).read()
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
            page = urllib.request.urlopen(bill_url, timeout = 120).read()
    time.sleep(1)
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = test[0]
# bill_row = session_bills[0]

def get_bill_data(bill_row):
    bill_num, s_yr, title, bill_url, hist_url, vote_url = bill_row
    bs_parser = "lxml"
    bill_soup = get_page_soup(bill_url, bs_parser)
    if bill_soup == 'HTTP Error':
        return('HTTP Error')
    elif bill_soup.find('em', string = re.compile('Local Gov')) is None:
        bs_parser = "html5lib"
        bill_soup = get_page_soup(bill_url, bs_parser)
    lsr = re.sub('.+lsr=', '', bill_url)
    lsr = re.sub('&.+', '', lsr)
    local_gov = bill_soup.find('em', string = re.compile('Local Gov')).findNext('b')
    if local_gov is not None:
        local_gov = local_gov.text.strip()
    else:
        local_gov = ''
    chapter_num = bill_soup.find('em', string = re.compile('Chapter#')).findNext('b')
    if chapter_num is not None:
        chapter_num = chapter_num.get_text().strip()
    else:
        chapter_num = ''
    sponsors = bill_soup.find('table', id = 'drep')
    if sponsors:
        sponsors = [i.text.replace('\xa0', ' ').strip() for i in sponsors.findAll('td') if i.text.replace('\xa0', ' ').strip() != '']
        sponsors = '; '.join(sponsors)
    else:
        sponsors = ''
    try:
        bill_text_url = bill_soup.find('a', string = 'Bill Text')['href']
        if 'http' not in bill_text_url:
            bill_text_url = 'https://www.gencourt.state.nh.us/bill_status/legacy/bs2016/' + bill_text_url
        bill_text_soup = get_page_soup(bill_text_url)
        sponsors_on_bill = bill_text_soup.find(string = re.compile("INTRODUCED BY|Introduced by|Introduced By|SPONSOR|Sponsor|sponsor"))
    except:
        sponsors_on_bill = ''
    status_order = bill_soup.findAll(string = re.compile("House Status|Senate Status"))
    if ('House' in status_order[0]):
        h_inds = ['Tr1', 'Tr7', 'Tr4']
        s_inds = ['Tr9', 'Tr15', 'Tr12']
    else:
        s_inds = ['Tr1', 'Tr7', 'Tr4']
        h_inds = ['Tr9', 'Tr15', 'Tr12']
    senate_status = bill_soup.find('tr', id = s_inds[0]).findAll('td')[2].text.strip()
    senate_floor_date = bill_soup.find('tr', id = s_inds[1]).findAll('td')[2].text.strip()
    if senate_floor_date != '':
        senate_floor_date = datetime.datetime.strptime(senate_floor_date, '%m/%d/%Y').strftime('%Y-%m-%d')
    senate_comm = bill_soup.find('tr', id = s_inds[2]).findAll('td')[2].text.strip()
    house_status = bill_soup.find('tr', id = h_inds[0]).findAll('td')[2].text.strip()
    house_floor_date = bill_soup.find('tr', id = h_inds[1]).findAll('td')[2].text.strip()
    if house_floor_date != '':
        house_floor_date = datetime.datetime.strptime(house_floor_date, '%m/%d/%Y').strftime('%Y-%m-%d')
    house_comm = bill_soup.find('tr', id = h_inds[2]).findAll('td')[2].text.strip()
    action_soup = get_page_soup(hist_url, bs_parser)
    action_table = action_soup.find('table', id = 'Table1').find('table')
    action_rows = action_table.findAll('tr')
    bill_actions = []
    order = 1
    for row in action_rows[1:]:
        cells = row.findAll('td')
        if cells == []:
            continue
        date = cells[0].get_text().strip()
        if date != '':
            date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
        else:
            date = bill_actions[-1][4]
        chamber = cells[1].get_text().strip()
        action = re.sub('\s\s+', ' ', cells[2].text).strip()
        bill_actions.append([bill_num, s_yr, chamber, date, action, order])
        order += 1
    bill_details = [bill_num, s_yr, sponsors, sponsors_on_bill, title, lsr, local_gov, chapter_num, senate_status, senate_floor_date, senate_comm, house_status, house_floor_date, house_comm, bill_url, hist_url, vote_url]
    return([bill_details, bill_actions])

########################################################
############## SCRAPE SESSION(S)
############################################
# bill_row = session_urls[799]
# s_yr = sessions[0]

for s_yr in sessions:

    #### Output Lists
    session_bill_details = [['bill_number', 'session_year', 'sponsors', 'sponsors_on_bill', 'title', 'lsr', 'local_gov', 'chapter_num', 'S_status', 'S_floor_date', 'S_comm',  'H_status', 'H_floor_date', 'H_comm', 'bill_url', 'hist_url', 'vote_url']]
    session_actions = [['bill_number', 'session_year', 'chamber', 'action_date', 'action', 'order']]

    print("\n ------------------- Now Scraping the {} Session ---------------------- \n".format(s_yr))

    ### Get all urls for a session-year, including special sessions
    session_urls = get_session_bills(s_yr)

    #### Loop through bills
    num = 1
    total = len(session_urls)
    for bill_row in session_urls:
        bill_data = get_bill_data(bill_row)
        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n URL: {} \n **********".format(num, total, bill_row[0], bill_row[3]))
            num += 1
            continue
        elif bill_data == "No Data":
            print(" ********** \n ({}/{}) -- {} -- NO DATA --- SKIPPING \n URL: {} \n **********".format(num, total, bill_row[0], bill_row[3]))
            num += 1
            continue
        session_bill_details.append(bill_data[0])
        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[0], bill_row[3]))
        num += 1

    with open("NH_Bill_Details_" + str(s_yr) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("NH_Bill_Histories_" + str(s_yr) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} Session SCRAPED + DATA SAVED  -------------\n\n\n".format(s_yr))


print("  ********************************** ALL DONE ********************************** ")
