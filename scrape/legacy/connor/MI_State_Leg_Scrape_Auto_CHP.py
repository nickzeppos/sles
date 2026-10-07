# -*- coding: utf-8 -*-
"""
Created on Wed Dec 12 14:55:04 2018

*** SCRAPE MICHIGAN STATE LEGISLATIVE BILLS ***

@author: PB
"""

import csv
import os
# import re
import time
import urllib
from bs4 import BeautifulSoup
import requests
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#################################
#### Get Sessions
#################################

session_page = urllib.request.urlopen('https://www.legislature.mi.gov/Bills')
session_soup = BeautifulSoup(session_page, 'lxml')

#### Create List of Sessions and DROP IF ALREADY SCRAPED
session_select = session_soup.find('select', id = 'sessionAutoUpdate')
sessions = [i.get_text() for i in session_select.findAll('option') if i.get_text() != 'All']

sessions = [s for s in sessions if int(s[0:4]) >= 2023 ]
sessions = [s for s in sessions if int(s[0:4]) < 2025 ]

## DROP IF ALREADY SCRAPED
##sessions = [s for s in sessions if "MI_Bill_Details_" + s.replace("-", "_") + ".csv" not in os.listdir('.')]


##################################
### GET BILL PAGE URLS
##################################
# session = '2015-2016'
# bill_type_list = bill_types['Bills']

def get_bill_urls(session):

    start_url = 'https://www.legislature.mi.gov/Search/ExecuteSearch?sessions={}&docTypes=House%20Bill,Senate%20Bill,House%20Resolution,Senate%20Resolution,House%20Joint%20Resolution,Senate%20Joint%20Resolution,House%20Concurrent%20Resolution,Senate%20Concurrent%20Resolution'.format(session)

    req = requests.get(start_url)
    req_soup = BeautifulSoup(req.content, 'lxml')
    bill_table = req_soup.find('table').findAll('tr')

    ### Parse Data
    this_bill_data = []
    for row in bill_table[1:]:
        cells = row.findAll('td')
        bill_num = cells[0].find('a')['href'].split("objectName=")[1].split('&')[0]
        friendly_url = 'https://legislature.mi.gov/Bills/Bill?ObjectName=' + bill_num
        bill_categ = cells[1].get_text()
        descrip = cells[2].get_text().split("Last Action:")[0]
        this_bill_data.append([session, bill_num, bill_categ, descrip, friendly_url])

    return(this_bill_data)


#### Need to Cycle Through All 4 Bill Types!
# test = get_bill_urls(sessions[0], bill_types['Bills'])


##################################
### GET BILL DATA
##################################
# bill_url = 'http://legislature.mi.gov/doc.aspx?1998-SB-1376'
# bill_num = '1997-HB-4004'
# session = 'zzz'

def get_bill_data(session, bill_num, bill_type, summary, bill_url):

    try:
        bill_page = urllib.request.urlopen(bill_url, timeout = 5).read()
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(10)
            bill_page = urllib.request.urlopen(bill_url, timeout = 10)
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            bill_page = urllib.request.urlopen(bill_url, timeout = 30)

    ### Get Bill Data
    bill_soup = BeautifulSoup(bill_page, 'lxml')

    ### Sponsors
    sponsor_list = bill_soup.find('div', id = 'SponsorList')
    if sponsor_list is not None:
        sponsor_list = [i.get_text().replace('\xa0', ' ').strip() for i in sponsor_list.findAll('a') if 'DistrictMaps' not in i['href']]
        sponsor_list = '; '.join(sponsor_list)
    else:
        sponsor_list = ''

    ### Categories
    category_list = bill_soup.find('div', id = 'CateogryList') #[sic]--the id is misspelled
    if category_list is not None:
        category_list = [i.get_text().replace('\xa0', ' ').strip() for i in category_list.findAll('a')]
        category_list = '; '.join(category_list)
    else:
        category_list = ''

    ### Summary --- Already have this from URL Scrape
    # summary = bill_soup.find('span', id = 'frg_billstatus_ObjectSubject').get_text().strip()
    summary = summary.replace('\r\n', '').strip()

    ### Actions
    ## NOTE: (House actions in lowercase, Senate actions in UPPERCASE)
    ## --> Can also use journal, with the exception of governor
    all_actions = []
    action_table = bill_soup.find('table')
    if action_table is not None:
        order = 1
        for row in action_table.findAll('tr')[1:]:
            cells = row.findAll('td')
            date = cells[0].get_text()
            journal_page = cells[1].get_text().replace('\xa0', ' ').strip()
            action = cells[2].get_text().replace('\xa0', ' ')
            all_actions.append([bill_num, session, date, journal_page, action, order])
            order += 1

    ### Get Vote Urls???

    #### Save to Export
    bill_details = [bill_num, session, bill_type, summary, sponsor_list, category_list, bill_url]
    return([bill_details, all_actions])


#####################################
### Loop Through Sessions, Get Bill Details
######################################
# bill_url = session_bills[6901]
# s = '1995-1996'

for s in sessions:

    print("\n *** Gathering Bill URLs for the {} MI Legislative Session *** \n".format(s))

    ### Get List of Bills and Basic Details
    session_bills = get_bill_urls(s)
    print(" ~~ Bill URLs Found")
    all_session_urls = session_bills

    session_bill_details = [['bill_number', 'session', 'bill_type', 'summary', 'sponsors', 'keywords', 'bill_url']]
    session_actions = [['bill_number', 'session', 'action_date', 'journal_page', 'action', 'order']]


    ### Scrape Each Bill/Resolution
    num = 1
    total = len(all_session_urls)

    print("\n ~~~> Scraping {} Individual Bills & Resolutions - Session {} \n".format(total, s))

    for bill_row in all_session_urls:

        ### Get Bill Data -- Each Type ONE AT A TYPE
        this_bill = get_bill_data(s, bill_row[1], bill_row[2], bill_row[3], bill_row[4])
        time.sleep(1)

        ### Append to Agg Files
        session_bill_details.append(this_bill[0])

        if this_bill[1] != []:
            for action_row in this_bill[1]:
                session_actions.append(action_row)

        print(" -- ({}/{}) ~~ URL: {}".format(num, total, bill_row[4]))
        num += 1

    ### SAVE!
    with open("MI_Bill_Details_" + s.replace("-", "_") + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("MI_Bill_Histories_" + s.replace("-", "_") + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n *********** General Assembly #{} DONE! ************** \n\n".format(s))


print(" ~~~~~~~~~~~~~ ************** ALL SESSIONS DONES *************** ~~~~~~~~~~~~~~~~ ")
