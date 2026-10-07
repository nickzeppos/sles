# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape NEBRASKA Legislation 2007 - PRESENT ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# Sessions Start in Odd Numbered Years, so use below URL to get, eg, 2011, then 2012
# https://nebraskalegislature.gov/bills/search_by_date.php?SessionDay=2011
####################


import csv
import os
import urllib
import urllib.request
import urllib.error
# import requests
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
##### Get Session Information
#######################################

### Sessions are two-year terms, starting in odd years
this_year = datetime.datetime.now().year

sessions = [str(i) + '_' + str(i + 1) for i in range(2023, this_year + 1, 2) if 'NE_Bill_Details_' + str(i) + '_' + str(i + 1) + '.csv' not in os.listdir('.')]

del this_year

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


#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s = sessions[0]
# test = get_session_bills(s)

def get_session_bills(s):

    s_yr1, s_yr2 = s.split('_')

    print('\n\n~~~~ Gathering Bill URLs for the {}-{} Session ~~~~\n'.format(s_yr1, s_yr2))

    session_bills = []

    ## URls Stems to List of Chamber Bills and Resolutions
    sy_list_url = 'https://nebraskalegislature.gov/bills/search_by_date.php?SessionDay={}'

    ### Get Odd/Even-Year Bills
    for yr in s.split('_'):
        list_soup = get_page_soup(sy_list_url.format(yr))
        list_table = list_soup.find('table')

        for row in list_table.findAll('tr')[1:]:
            cells = row.findAll('td')
            bill_num = cells[0].get_text().strip()
            primary_sponsor = cells[1].get_text().strip()
            status = cells[2].get_text().strip()
            summary = cells[3].get_text().strip()
            bill_url = 'https://nebraskalegislature.gov' + cells[0].find('a')['href']
            session_bills.append([bill_num, s, primary_sponsor, status, summary, bill_url])

    return(session_bills)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = test[0]
# bill_row = session_bills[0]

def get_bill_data(bill_row):

    ### Scrape Bill Page
    bill_num, s, primary_sponsor, status, summary, bill_url = bill_row

    ### Get HTML, Soup
    bill_soup = get_page_soup(bill_url)

    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')

    ### Standardize bill_num
    bt = re.sub('[0-9]+', '', bill_num)
    bn = re.sub('[A-Z]+', '', bill_num)
    bill_num = bt + bn.zfill(4)

    ## Get Action Table
    action_table = bill_soup.find('table')
    action_rows = action_table.findAll('tr')

    ### Action Scrape for Both Formats
    bill_actions = []
    order = len(action_rows[1:])
    chamber = 'Unicameral'
    for row in action_rows[1:]:
        cells = row.findAll('td')
        date = cells[0].get_text().strip()
        date = datetime.datetime.strptime(date, '%b %d, %Y').strftime('%Y-%m-%d')
        action = cells[1].get_text().strip()
        journal_page = cells[2].get_text().strip()

        bill_actions.append([bill_num, s, chamber, date, action, journal_page, order])
        order = order - 1

    #############
    ### OUTPUT
    ##############
    bill_details = [bill_num, s, primary_sponsor, status, summary, bill_url]

    return([bill_details, bill_actions])

########################################################
############## SCRAPE SESSION(S)
############################################
# bill_num, bill_url, descrip = session_urls[6751]
# s_yr = sessions[0]

for s in sessions:

    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'primary_sponsor', 'status',  'summary', 'bill_url']]
    session_actions = [['bill_number', 'session', 'chamber', 'action_date', 'action', 'journal_page', 'order']]

    print("\n ------------------- Now Scraping the {} Session ---------------------- \n".format(s))

    ### Get all urls for a session-year, including special sessions
    session_urls = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_urls)
    for bill_row in session_urls:

        bill_data = get_bill_data(bill_row)

        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n URL: {} \n **********".format(num, total, bill_row[0], bill_row[5]))
            num += 1
            continue

        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[0], bill_row[5]))
        num += 1

    with open("NE_Bill_Details_" + s + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("NE_Bill_Histories_" + s + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} Session SCRAPED + DATA SAVED  -------------\n\n\n".format(s))


print("  ********************************** ALL DONE ********************************** ")
