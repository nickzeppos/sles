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

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#####################################
##### Extract Session Information
#######################################

session_search = urllib.request.urlopen('http://laws.leg.mt.gov/legprd/law0203w$.startup?P_SESS=20191', timeout = 30)
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.find('select', {'name':'P_SESS'}).findAll('option')
sessions = [[i.get_text().strip(), i['value']] for i in session_list]

for i in range(0, len(sessions)):
    yr = int(sessions[i][0].split(' ', 1)[0])
    s_type = sessions[i][1][4:]
    if s_type == '1':
        s_type = 'RS'
    else:
        s_type = 'SS' + str(int(s_type) - 1)
    sessions[i] = [yr, s_type, sessions[i][1]]

### Drop Previously Scraped
sessions = [s for s in sessions if 'MT_Bill_Details_' + str(s[0]) + "_" + s[1] + '.csv' not in os.listdir('.')]

### Drop Current Session
this_year = datetime.datetime.now().year
sessions = [s for s in sessions if s[0] < this_year]
print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))

del this_year, session_search, session_search_soup, session_list, yr, s_type

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s = sessions[0]
# test = get_session_bills(sessions[0])

def get_session_bills(s):

    s_year, s_type, s_id = s

    print('\n~~~~ Gathering Bill URLs for the {} {} Session ~~~~\n'.format(s_year, s_type))

    s_req = requests.get('http://laws.leg.mt.gov/legprd/LAW0217W$BAIV.return_all_bills?P_SESS={}'.format(s_id))
    s_soup = BeautifulSoup(s_req.content, 'lxml')

    session_bills = []

    introduced_table = s_soup.find('th', string = re.compile('Primary Sponsor')).findParents('table')[0]
    unintroduced_table = s_soup.find('th', string = re.compile('Requestor')).findParents('table')[0]

    bill_draft = 0
    requester = ''
    for intro in introduced_table.findAll('tr')[1:]:
        cells = intro.findAll('td')
        bill_num = cells[0].find('a', href = re.compile('P_BILL_NO'))
        bill_url = 'http://laws.leg.mt.gov/legprd/' + bill_num['href']
        bill_num = re.split('\s+', bill_num.get_text())
        bill_num = bill_num[0] + bill_num[1].zfill(4)

        lc_num = cells[1].get_text().strip()
        sponsor = re.sub('\s\s+', ' ', cells[2].get_text().strip())
        status = cells[3].get_text().strip()
        # status_date = cells[4].get_text().strip()
        title = cells[5].get_text().strip()

        session_bills.append([bill_num, lc_num, s_year, s_type, bill_draft, sponsor, requester, status, title, bill_url])

    bill_num = ''
    bill_draft = 1
    sponsor = ''
    for draft in unintroduced_table.findAll('tr')[1:]:
        cells = draft.findAll('td')
        lc_num = cells[0].find('a', href = re.compile('P_BILL_DFT'))
        bill_url = 'http://laws.leg.mt.gov/legprd/' + lc_num['href']
        lc_num = lc_num.get_text().strip()

        requester = re.sub('\s\s+', ' ', cells[2].get_text().strip())
        status = cells[3].get_text().strip()
        title = cells[5].get_text().strip()

        session_bills.append([bill_num, lc_num, s_year, s_type, bill_draft, sponsor, requester, status, title, bill_url])

    return(session_bills)


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):

    ### Get HTML
    try:
        page = urllib.request.urlopen(bill_url, timeout = 20)
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
    time.sleep(1)

    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = session_urls[0]
# bill_row = session_bills[0]

def get_bill_data(bill_row):

    ### Scrape Bill Page
    bill_num, lc_num, s_year, s_type, draft, sponsor, requester, status, title, bill_url = bill_row

    ### Get HTML, Soup
    bill_soup = get_page_soup(bill_url)

    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')

    ## Get Bill Data
    sponsor_table = bill_soup.find('th', string = re.compile('Sponsor')).findParents('table')[0]
    sponsor_rows = sponsor_table.findAll('tr')

    ### Primary Sponsors
    if sponsor == '':
        sponsor = [row.findAll('td') for row in sponsor_rows if row.find('td', string = re.compile('Primary Sponsor')) is not None]
        sponsor = [re.sub('&nbsp', '', i[2].get_text().strip()) + ' ' + re.sub('&nbsp', '', i[3].get_text().strip()) + ' ' + re.sub('&nbsp', '', i[1].get_text().strip()) for i in sponsor]
        sponsor =  '; '.join([re.sub('\s\s+', ' ', i).strip() for i in sponsor])

    ### Bill Draft Requester
    if requester == '':
        requester = [row.findAll('td') for row in sponsor_rows if row.find('td', string = re.compile('Requester')) is not None]
        requester = [re.sub('&nbsp', '', i[2].get_text().strip()) + ' ' + re.sub('&nbsp', '', i[3].get_text().strip()) + ' ' + re.sub('&nbsp', '', i[1].get_text().strip()) for i in requester]
        requester =  '; '.join([re.sub('\s\s+', ' ', i).strip() for i in requester])

    ### Bill Drafter
    drafter = [row.findAll('td') for row in sponsor_rows if row.find('td', string = re.compile('Drafter')) is not None]
    drafter = [re.sub('&nbsp', '', i[2].get_text().strip()) + ' ' + re.sub('&nbsp', '', i[3].get_text().strip()) + ' ' + re.sub('&nbsp', '', i[1].get_text().strip()) for i in drafter]
    drafter =  '; '.join([re.sub('\s\s+', ' ', i).strip() for i in drafter])

    ## Background Info Table
    subject_table = bill_soup.find('th', string = re.compile('Subject Code'))
    if subject_table is not None:
        subject_table = subject_table.findParents('table')[0]
        subject_rows = subject_table.findAll('tr')
        subjects = '; '.join([i.find('td').get_text().strip() for i in subject_rows[1:]])
    else:
        subjects = ''

    ## Actions --
    hist_table = bill_soup.find('th', string = re.compile('Action')).findParents('table')[0]
    hist_rows = hist_table.findAll('tr')

    bill_actions = []

    order = len(hist_rows) - 1
    for row in hist_rows[1:]:
        cells = row.findAll('td')
        action = cells[0].get_text().strip()
        supp_info = re.sub('&nbsp', '', cells[4].get_text().strip())
        if supp_info != '':
            action = action + ' ~ [{}]'.format(supp_info)

        date = cells[1].get_text().strip()
        date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')

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
    session_urls = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_urls)
    for bill_row in session_urls:

        bill_data = get_bill_data(bill_row)

        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n URL: {} \n **********".format(num, total, bill_row[0], bill_row[3]))
            num += 1
            continue

        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        if bill_row[0] == '':
            print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[1], bill_row[9]))
        else:
            print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[0], bill_row[9]))
        num += 1

    with open("MT_Bill_Details_" + str(s_year) + "_" + s_type + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("MT_Bill_Histories_" + str(s_year) + "_" + s_type  + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} {} Session SCRAPED + DATA SAVED  -------------\n\n\n".format(s_year, s_type))


print("  ********************************** ALL DONE ********************************** ")
