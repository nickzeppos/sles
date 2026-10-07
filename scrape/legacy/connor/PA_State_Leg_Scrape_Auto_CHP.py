# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape PA Legislation ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# Per OpenStates code, PA is continuously adding backdata...
# At present, no history data prior to 1969-1970 Session
#
# Could Scrape Votes as well - but requires multiple urls or looping through a different part of the website
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

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

# Set user agent so scraper doesn't get blocked
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'}

##################################
##### Extract Session Information
##########################################

request = urllib.request.Request('https://www.palegis.us/legislation/bills', headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'})
session_search = urllib.request.urlopen(request, timeout = 30)
session_search_soup = BeautifulSoup(session_search, 'lxml')

##chrome_options = Options()
##chrome_options.add_argument("--headless")
##driver = webdriver.Chrome(options=chrome_options)
##driver.get('https://www.legis.state.pa.us/?legacy')
##time.sleep(4)
##sessions_button = driver.find_element(By.XPATH, '/html/body/header/div/div/nav/ul/li[5]/a')
##time.sleep(4)
##sessions_button.click()
##time.sleep(2)
##session_search_soup = BeautifulSoup(driver.page_source, 'lxml')

#request = urllib.request.Request('https://www.legis.state.pa.us/cfdocs/legis/home/bills/', headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'})
#session_search = urllib.request.urlopen(request, timeout = 30)
#session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.find('select', id = 'sessionSelect').findAll('option')
sessions = [[i['value'], i.get_text().strip()] for i in session_list]

### Drop Sessions BEFORE 2023
sessions = [[s_id, name] for s_id, name in sessions if int(s_id[0:4]) >= 2023]

### Drop Previously Scraped
##sessions = [[s_id, name] for s_id, name in sessions if 'PA_Bill_Details_' + s_id + '.csv' not in os.listdir('.')]

### Drop Current Session
this_year = datetime.datetime.now().year
sessions = [[s_id, name] for s_id, name in sessions if int(s_id[0:4]) < this_year]
print("\n\n ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))


#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s_id, s_name = sessions[1]

def get_session_bills(s_id, s_name):

    print('\n~~~~ Gathering Bill URLs for the {} ~~~~\n'.format(s_name))
    session_urls = []

    ### Splitting ID into Year and Special Num
    s_year, s_special = s_id.split('_')

    ### Request Data + Session for each Chamber
    for chamber in ['H', 'S']:
        search_url = 'https://www.palegis.us/legislation/bills/bill-index?display=index&sessYr={}&sessInd={}&billBody={}&filter=bills'.format(s_year, s_special, chamber)
        req = urllib.request.Request(search_url, headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'})
        bill_search = urllib.request.urlopen(req, timeout = 30)
        chamber_soup = BeautifulSoup(bill_search, 'lxml')

        chamber_urls = chamber_soup.findAll('a', href = re.compile('legislation/bills/\d+'))
        chamber_urls = ['https://www.palegis.us' + i['href'] for i in chamber_urls]
        session_urls = session_urls + chamber_urls

    return(session_urls)


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(this_url, parser = 'lxml'):

    ### Get HTML
    try:
        request = urllib.request.Request(this_url, headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'})
        page = urllib.request.urlopen(request, timeout = 15)
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(15)
            request = urllib.request.Request(this_url, headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'})
            page = urllib.request.urlopen(request, timeout = 30)
        except urllib.error.HTTPError: # as e
            return('HTTP Error')
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = urllib.request.urlopen(this_url, timeout = 60)
    time.sleep(.25)

    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_info = session_urls[0]
# s_id, s_name = sessions[0]

def get_bill_data(bill_url, s_id, s_name):

    ### Scrape Bill Page
    bill_soup = get_page_soup(bill_url)

    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')

    ### Bill Information in Header
    bn_tag = re.search('/[\w\d]+$', bill_url).group()
    bill_type = re.search('[a-zA-Z]+', bn_tag).group()
    bill_id = re.search('\d+', bn_tag).group()
    bill_num = bill_type.upper() + bill_id.zfill(4)

    ### Probably need if not none statements here... wait for it to break first
    title = bill_soup.find('div', {'class':'col-md-9'})
    title = title.find('div').get_text().strip().replace("\xa0", " ")

    sponsor = bill_soup.find('div', {'class':'col-12 col-md-6 mb-3'})
    sponsor = sponsor.find('strong').get_text().strip().replace("\xa0", " ")

    ## Cosponsor Memo --- Mostly fluff
    #memo_url = bill_soup.find('div', {'class':'BillInfo-Section BillInfo-CosponMemo'})
    #if memo_url is not None:
    #    memo_url = memo_url.find('div', {'class':'BillInfo-Section-Data'}).find('a')['href']
    #else:
    #    memo_url = ''

    ### All Sponsors
    cosponsors = bill_soup.find('div', {'class':'row h-100 overflow-hidden'})
    if cosponsors is not None:
        cosponsors = cosponsors.find_all('strong')
        cosponsors_list = []
        for cosponsor in cosponsors:
            cosponsors_list.append(cosponsor.get_text().strip())
        additional_cosponsors = bill_soup.find('div', {'class':'accordion mb-4'})
        if additional_cosponsors is not None:
            additional_cosponsors = additional_cosponsors.find_all('strong')
            additional_cosponsors_list = []
            for additional_cosponsor in additional_cosponsors:
                additional_cosponsors_list.append(additional_cosponsor.get_text().strip())
            cosponsors_list = cosponsors_list + additional_cosponsors_list
        all_sponsors = [sponsor] + cosponsors_list
        all_sponsors = '; '.join(all_sponsors)
    else:
        all_sponsors = sponsor

    if cosponsors is not None:
        if additional_cosponsors is not None:
            status = bill_soup.find_all('div', {'class':'mb-4'})
            status = status[1]
            status = status.get_text().strip().replace("\xa0", " ")
        else:
            status = bill_soup.find('div', {'class':'mb-4'})
            status = status.get_text().strip().replace("\xa0", " ")
    else:
        status = bill_soup.find('div', {'class':'mb-4'})
        status = status.get_text().strip().replace("\xa0", " ")

    ########### Actions -- If no actions, tab isn't there, shows cosponsors
    bill_actions = []

    if cosponsors is not None:
        if additional_cosponsors is not None:
            hist_table = bill_soup.find_all('div',  {'class':'accordion-collapse collapse'})
            hist_table = hist_table[1]
        else:
            hist_table = bill_soup.find('div',  {'class':'accordion-collapse collapse'})
    else:
        hist_table = bill_soup.find('div',  {'class':'accordion-collapse collapse'})
    
    hist_table = hist_table.find('table', {'class':'table table-striped w-100 w-md-75 w-lg-50 ms-3'})
    order = 1

    if bill_num[0:1] == 'H':
        chamber = 'House'
    else:
        chamber = 'Senate'

    exec_regex = re.compile('In the.+ Governor|Approved.+ Governor|Presented.+ Governor|Became Law.+Governor|Vetoed by|Veto No|Filed in the Office of the Secret|Pamphlet Laws|Item Veto')
    electorate_regex = re.compile('by the Electorate|Vote by Electorate')

    if hist_table is not None:
        for row in hist_table.findAll('tr'):
            cell = row.findAll('td')[1]
            cell = cell.get_text().replace('\xa0', ' ').strip()
            cell = re.sub('\s\s+', ' ', cell)
            # Skipping Chamber-Switch Rows
            if 'In the Senate' in cell:
                chamber = 'Senate'
                continue
            elif 'In the House' in cell:
                chamber = 'House'
                continue
            elif 'Signed in House' in cell:
                chamber = 'House'
            elif 'Signed in Senate' in cell:
                chamber = 'Senate'
            elif exec_regex.search(cell):
                chamber = 'Executive'
            elif electorate_regex.search(cell):
                chamber = 'Electorate'

            ## Fixing a 1875 error
            cell = re.sub('l975', '1975', cell)

            #Adjusting for Act Rows without a date
            if ('Act No.' in cell or 'Veto No.' in cell or 'Item Veto' in cell) and s_id.split('_')[0] not in cell:
                action = cell
                date = bill_actions[-1][3]
            elif 'Pamphlet Laws Resolution' in cell or 'Passed Sessions of ' in cell:
                action = cell
                date = bill_actions[-1][3]
            elif 'Vote by which conference committee report was' in cell or 'Vote by Electorate, See History' in cell:
                action = cell
                date = bill_actions[-1][3]
            elif cell == 'INAUGURAL COMMITTEE ON THE PART OF THE SENATE:':
                break
            else:
                #txt_pieces = re.split(', (?=[JFMASOND][a-z]+ [0-9]+, [12][0-9][0-9][0-9])|, (?=[JFMASOND][a-z]+\\. [0-9]+, [12][0-9][0-9][0-9])| (?=[JFMASOND][a-z]+ [0-9]+, [12][0-9][0-9][0-9])| (?=[JFMASOND][a-z]+\\. [0-9]+, [12][0-9][0-9][0-9])| (?=[JFMASOND][a-z]+\\. [0-9]+, [12][0-9][0-9][0-9])|  (?=[JFMASOND][a-z]+\\. [0-9]+, [12][0-9][0-9][0-9])', cell)
                txt_pieces = re.split(', (?=[JFMASOND][A-Za-z]+ [0-9]+, [12][0-9][0-9][0-9])|, (?=[JFMASOND][A-Za-z]+\\. [0-9]+, [12][0-9][0-9][0-9])| (?=[JFMASOND][A-Za-z]+ [0-9]+, [12][0-9][0-9][0-9])| (?=[JFMASOND][A-Za-z]+\\. [0-9]+, [12][0-9][0-9][0-9])| (?=[JFMASOND][A-Za-z]+\\. [0-9]+, [12][0-9][0-9][0-9])|  (?=[JFMASOND][A-Za-z]+\\. [0-9]+, [12][0-9][0-9][0-9])| (?=[JFMASOND][A-Za-z]+\\. [0-9]+ [12][0-9][0-9][0-9])', cell)
                if len(txt_pieces) == 1:
                    action = cell
                    date = bill_actions[-1][3]
                else:
                    if len(txt_pieces) == 2:
                        action, date = txt_pieces
                    else:
                        action = cell
                        date = txt_pieces[len(txt_pieces)-1]

                    date = re.sub('\\(.+\\)', '', txt_pieces[len(txt_pieces)-1]).strip()
                    date = re.sub('(?<=, \d{4}).+', '', date)
                    if '.' in date:
                        date = date.replace('Sept.', 'Sep.').replace('SEPT.', 'Sep.')
                        if ',' in date:
                            date = datetime.datetime.strptime(date, '%b. %d, %Y').strftime('%Y-%m-%d')
                        else:
                            date = datetime.datetime.strptime(date, '%b. %d %Y').strftime('%Y-%m-%d')
                    else:
                        try:
                            date = datetime.datetime.strptime(date, '%B %d, %Y').strftime('%Y-%m-%d')
                        except:
                            date = datetime.datetime.strptime(date, '%b %d, %Y').strftime('%Y-%m-%d')

            bill_actions.append([bill_num, s_id, chamber, date, action, order])
            order += 1

    ### OUTPUT ~~~ Note: Vote Urls are in the Documents Tab of the Newer Format
    bill_details = [bill_num, s_id, s_name, sponsor, all_sponsors, title, status, bill_url]
    return([bill_details, bill_actions])

########################################################
############## SCRAPE SESSION(S)
############################################
# bill_url = session_urls[812]
# s_id, s_name = sessions[0]

for s_id, s_name in sessions:

    #### Output Lists
    session_bill_details = [['bill_number', 'session',  'session_full', 'sponsor', 'all_sponsors', 'title', 'status', 'bill_url']]
    session_actions = [['bill_number', 'session', 'chamber', 'action_date', 'action','order']]

    print("\n ------------------- Now Scraping the {} ---------------------- \n".format(s_name))

    ### Get all bills for a specific session
    session_urls = get_session_bills(s_id, s_name)

    #### Loop through bills
    num = 1
    total = len(session_urls)
    for bill_url in session_urls:

        bill_data = get_bill_data(bill_url, s_id, s_name)

        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- HTTP ERROR --- SKIPPING \n URL: {} **********".format(num, total, bill_url))
            num += 1
            continue

        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_data[0][0], bill_url ))
        num += 1

    with open("PA_Bill_Details_" + s_id + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("PA_Bill_Histories_" + s_id + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} SESSION SCRAPED + DATA SAVED  -------------\n\n\n".format(s_name))


print("  ********************************** ALL DONE ********************************** ")
