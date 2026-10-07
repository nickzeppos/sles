# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape NY Legislation ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# API Adapted from OpenStates Code
# Could also pull votes or more data from the 'memo' tab
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

s = 1999
bill_redo_list = ['A00824', 'A01213', 'A04736', 'A05763', 'A07459', 'A08187', 'A10030', 'S03071']
#
# session_search = urllib.request.urlopen('https://nyassembly.gov/leg/?sh=advanced', timeout = 30)
# session_search_soup = BeautifulSoup(session_search, 'lxml')
#
# ### Get Sessions
# session_list = session_search_soup.find('select', id = 'term').findAll('option')
# sessions = [i['value'] for i in session_list]
#
# ### Drop Previously Scraped
# sessions = [s for s in sessions if 'NY_Bill_Details_' + s + '.csv' not in os.listdir('.')]
#
# ### Drop Current Session
# this_year = datetime.datetime.now().year
# sessions = [s for s in sessions if int(s) < this_year]
# print("\n\n ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))


#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s_id, s_name = sessions[1]

def get_session_bills(s):

    print('\n~~~~ Gathering Bill URLs for the {}-{} Session ~~~~\n'.format(s, str(int(s) + 1)))

    ## Prep Search
    search_url = 'https://nyassembly.gov/leg/?sh=advanced'
    form_data = {'evt_fld': 'Search', 'by':'a', 'term':s, 'leg_type': 'A', 'comm_status': 'A'}

    ## Get Page Results
    req = requests.post(search_url, data = form_data)
    soup = BeautifulSoup(req.content, 'lxml')

    ## Return bill Number, URL, Description
    ## *** Bills without descriptions appear to have no data?
    bill_info = [[i.get_text(), 'https://nyassembly.gov/leg/' + i['href'], i.nextSibling] for i in soup.findAll('a', href = re.compile('\\?bn'))]

    return(bill_info)


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):
    this_url = bill_url + '&Summary=Y&Actions=Y' # ?default_fld=&leg_video=
    this_url = this_url.replace(' ','')
    try:
        page = urllib.request.urlopen(this_url, timeout = 15)
    except urllib.error.HTTPError: # as e
        return('HTTP Error')
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(15)
            page = urllib.request.urlopen(this_url, timeout = 30)
        except urllib.error.HTTPError: # as e
            return('HTTP Error')
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = urllib.request.urlopen(this_url, timeout = 60)
    time.sleep(.25)
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)


#######################
###### Functions to Scrape Individual Bills
#########################
# s = sessions[0]

def get_bill_data(bill_num, bill_url, descrip, s):

    ### Scrape Bill Page
    bill_soup = get_page_soup(bill_url)

    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')

    ## Get Basic Data
    summary_table = bill_soup.find('h3', id = 'jump_to_Summary')
    action_table = bill_soup.find('h3', id = 'jump_to_Actions')

    ## Check if all data loaded - if not, change parser
    if action_table is None:
        bill_soup = get_page_soup(bill_url, parser = 'html5lib')
        summary_table = bill_soup.find('h3', id = 'jump_to_Summary')
        action_table = bill_soup.find('h3', id = 'jump_to_Actions')

    ## If No Data, Exit
    if summary_table is None:
        return('Bill Not Found')
    elif 'Unable to Contact Server' in bill_soup.get_text():
        return('Bill Not Found')
    else:
        summary_table = summary_table.findNext('table')

    companion = summary_table.find('td', string = 'SAME AS').findNext('td').get_text()
    if companion == "No Same As":
        companion = ''
    else:
        companion = companion.strip()

    sponsor = summary_table.find('td', string = 'SPONSOR').findNext('td').get_text()
    cosponsor = summary_table.find('td', string = 'COSPNSR').findNext('td').get_text()
    multi_sponsor = summary_table.find('td', string = 'MLTSPNSR').findNext('td').get_text()

    summary_tag = summary_table.findAll('td', {'colspan':2})
    if summary_tag is None:
        summary = ''
    else:
        summary = summary_tag[len(summary_tag)-1].get_text()

    ## Actions
    action_table = action_table.findNext('table')

    bill_actions = []
    order = 1
    for row in action_table.findAll('tr'):
        #row = action_table.findAll('tr')[1]
        #firstrow_text = row.find('td').get_text().strip()
        text_entries = []
        for td in row.findAll('td'):
            # Check if the stripped text content of the <td> is not empty
            if td.get_text().strip() not in ['&nbsp', '']:
                text_entries.append(td)
        if text_entries == [] or re.search('^BILL NO', text_entries[0].get_text().strip()):
            continue
        else:
            try:
                date = datetime.datetime.strptime(text_entries[0].get_text().strip(), '%m/%d/%Y').strftime('%Y-%m-%d')
            except ValueError:
                continue
            action = text_entries[1].get_text()
            #~~~ CAPITALIZATION SEEMS TO INDICATE CHAMBER -- Gov follows initiating chamber
            if action.upper() == action:
                chamber = "Senate"
            else:
                chamber = "Assembly"
            bill_actions.append([bill_num, s, chamber, date, action, order])
            order += 1

    ### OUTPUT ~~~ Note: Vote Urls are in the Documents Tab of the Newer Format
    bill_details = [bill_num, s, sponsor, cosponsor, multi_sponsor, descrip, summary, bill_url]
    return([bill_details, bill_actions])

########################################################
############## SCRAPE SESSION(S)
############################################
# bill_num, bill_url, descrip = session_urls[6751]
# s = sessions[0]



#### Output Lists
session_bill_details = [['bill_number', 'session', 'sponsor', 'cosponsors', 'multi_sponsors', 'short_summary', 'summary', 'bill_url']]
session_actions = [['bill_number', 'session', 'chamber', 'action_date', 'action','order']]

print("\n ------------------- Now Scraping the {}-{} Session---------------------- \n".format(s,  str(int(s) + 1)))

### Get all bills for a specific session
session_urls = get_session_bills(s)

#### Loop through bills
num = 1
total = len(session_urls)
for bill_row in session_urls:

    bill_num, bill_url, descrip = bill_row

    if bill_num in bill_redo_list:
        bill_data = get_bill_data(bill_num, bill_url, descrip, s)

        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n URL: {} \n **********".format(num, total, bill_num, bill_url))
            num += 1
            continue
        elif bill_data == "Bill Not Found":
            print(" ********** \n ({}/{}) -- {} -- BILL NOT FOUND --- SKIPPING \n URL: {} \n **********".format(num, total, bill_num, bill_url))
            num += 1
            continue

        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_num, bill_url))
        num += 1

    with open("NY_Bill_Histories2_" + str(s) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

print("\n\n\n ------------- {} SESSION SCRAPED + DATA SAVED  -------------\n\n\n".format(s))


print("  ********************************** ALL DONE ********************************** ")
