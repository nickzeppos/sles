#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 13 12:37:40 2019

~~~~~ Scraping MA Bills and Actions ~~~~~~~~~

@author: pb
"""

# -*- coding: utf-8 -*-

##### NOTES:
# Data ranges from 2009 - present --- No further archive available
# Some data may be available via lexisnexis or westlaw, but access limited to vandy law
# 2005-2006 is on website but only 1 bill
# Data includes initiative petitions -- with names of citizen sponsors -- usually read in and
#
# Sponsor information is weird --- lots of missingness
####################

import csv
import os
import urllib
import requests
from bs4 import BeautifulSoup
import time
import datetime
import re
import math
import socket
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#######################
##### Extract Session Links
########################

session_search = urllib.request.urlopen('https://malegislature.gov/Bills/Search?SearchTerms=&Page=1', timeout = 30)
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.find('div', {'class':'refinerGroup top'}).findAll('input', {'type':'checkbox'})

session_names = [re.sub('\\(.*\\)', '', i.parent.get_text()).strip() for i in session_list]
session_tokens = [i['data-refinertoken'] for i in session_list]

current_session = str(int((datetime.datetime.now().year - 1637 )/2))

### Combine and Drop Previously Scraped, Drop 184th (1 Bill)
sessions = [[name, token] for name, token in zip(session_names, session_tokens) if 'MA_Bill_Details_' + name + '.csv' not in os.listdir('.') and name not in ['184th', '185th'] and current_session not in name]

del session_names, session_tokens, session_list, session_search, session_search_soup

#######################################################
########## GET BILL URLS VIA FTP SITE FOR A SESSION
#####################################################
# s_name, s_token = sessions[-1]
# page_content = r.content

def parse_results(page_content, url_list, s_name):
    time.sleep(.5)
    this_soup = BeautifulSoup(page_content, 'lxml')
    results_table = this_soup.find('table', id = 'searchTable')
    table_rows = results_table.findAll('tr')
    new_data = []
    for row in table_rows[1:]:
        cells = row.findAll('td')
        bn = cells[1].get_text().strip()
        filed_by = cells[2].get_text().strip()
        descrip = cells[3].get_text().strip()
        b_url = 'https://malegislature.gov' + cells[3].find('a')['href']
        new_data.append([bn, s_name, filed_by, descrip, b_url])
    return(url_list + new_data)

def get_session_bills(s_name, s_token):

    print('\n~~~~ Gathering Bill URLs for the {} Session ~~~~\n'.format(s_name))
    bill_urls = []

    ### Request Data + Session
    form_data = {"SearchTerms": '', "Page": 1, "Refinements[lawsgeneralcourt]":s_token, 'SortManagedProperty':'lawsbillnumber', 'Direction':'asc'}
    search_url = 'https://malegislature.gov/Bills/Search'
    request_session = requests.Session()

    ### Get First Page + Save Data
    r = request_session.post(url=search_url, data=form_data)
    bill_urls = parse_results(r.content, bill_urls, s_name)

    ### Get total number of pages
    first_soup = BeautifulSoup(r.content, 'lxml')
    total_results = first_soup.find('div', {'class':'searchResultSummary'})
    total_results = int(re.search("([0-9]+) results|([0-9]) results", total_results.get_text().strip()).group(0).replace(' results', ''))
    num_pages = math.ceil(total_results / 25)
    print(" -- {}/{} ".format(1, num_pages))

    ### Loop through all results pages
    for page_num in range(2, num_pages + 1):
        form_data['Page'] = page_num
        try:
            r = request_session.post(url=search_url, data=form_data, timeout = 15)
        except:
            print("\n ~~> Retrying Post Request")
            time.sleep(30)
            r = request_session.post(url=search_url, data=form_data, timeout = 60)
        bill_urls = parse_results(r.content, bill_urls, s_name)
        print(" -- {}/{} ".format(page_num, num_pages))
        time.sleep(.25)

    return(bill_urls)


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):

    ### Get HTML
    try:
        page = urllib.request.urlopen(bill_url, timeout = 15)
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
    time.sleep(.25)

    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_info = session_bills[0]
# s_name, s_token = sessions[-1]

def get_bill_data(bill_info, s_name):

    ### Scrape Bill Page
    bill_url = bill_info[4]
    bill_soup = get_page_soup(bill_url + '/BillHistory')

    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')

    ### Bill Information in Header
    bill_num = bill_info[0]
    filed_by, title = bill_info[2:4]

    bill_type = bill_soup.find('h1').next.replace(bill_num, '').strip()

    summary = bill_soup.find('p', id = 'pinslip')
    if summary is not None:
        summary = summary.get_text()
    else:
        summary = bill_soup.find('div', id = 'contentContainer').find('h2')
        if summary is not None:
            summary = summary.get_text()
        else:
            summary = ''

    #### Bill Info in Unformatted Center Box
    info_box = bill_soup.find('dl', {'class':'list-unstyled billInfo'})

    status = ''
    status_tag = info_box.find('dt', text = re.compile('Status'))
    if status_tag is not None and status_tag.findNextSibling().name == 'dd':
        status = status_tag.findNextSibling().get_text().strip()
    # https://malegislature.gov/Bills/188/H3843

    city_town = ''
    city_town_tag = info_box.find('dt', text = re.compile('City/Town'))
    if city_town_tag is not None and city_town_tag.findNextSibling().name == 'dd':
        city_town = city_town_tag.findNextSibling().get_text()
        city_town = re.sub('\r\n|  ', '', city_town).strip()

    ### Committees
    committee_links = bill_soup.findAll('a', href = re.compile('/Committees/Detail'))
    committees = ''
    if committee_links is not None:
        committees = [c.get_text().strip() for c in committee_links]
        committees = '; '.join(set(committees))

    ### Petitioner = in leg, sponsors = not?
    presenter = ''
    presenter_tag = info_box.find('dt', text = re.compile('Presenter'))
    if presenter_tag is not None and presenter_tag.findNextSibling().name == 'dd':
        presenter = presenter_tag.findNextSibling().get_text().strip()
        ## Not all presenters are sponsors, e.g., https://malegislature.gov/Bills/191/SD381
        ## Should include By Request in those cases

    sponsor = ''
    sponsor_tag = info_box.find('dt', text = re.compile('Sponsor'))
    if sponsor_tag is not None and sponsor_tag.findNextSibling().name == 'dd':
        sponsor = sponsor_tag.findNextSibling().get_text().strip()

    ### Cosponsor
    cosponsor_soup = get_page_soup(bill_url + '/Cosponsor')
    all_petitioners = ''
    cosponsors = ''
    person_table = cosponsor_soup.find('h3', {'class':'fnSingleTab'})
    if person_table is not None and person_table.get_text() in ['Cosponsors', 'Petitioners']:
        if cosponsor_soup.find('h3', {'class':'fnSingleTab'}).get_text() == "Petitioners":
            all_petitioners = [i.find('td').get_text() for i in person_table.findNext('table').findAll('tr')]
            all_petitioners = '; '.join(all_petitioners)
        else:
            cosponsors = [i.find('td').get_text() for i in person_table.findNext('table').findAll('tr')]
            cosponsors = '; '.join(cosponsors)

    ########### Actions -- If no actions, tab isn't there, shows cosponsors
    bill_actions = []
    hist_tab = bill_soup.find('h3',  {'class':'fnSingleTab'})
    if hist_tab is not None and 'History' in hist_tab.get_text():
        action_table = bill_soup.find('div', id = 'searchResults').find('table')
        order = 1
        for row in action_table.findAll('tr')[1:]:
            cells = row.findAll('td')
            date = cells[0].get_text()
            date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            chamber = cells[1].get_text().strip()
            action = cells[2].get_text().strip()
            bill_actions.append([bill_num, s_name, chamber, date, action, order])
            order += 1

    ### OUTPUT ~~~ Note: Vote Urls are in the Documents Tab of the Newer Format
    bill_details = [bill_num, s_name, bill_type, filed_by, presenter, all_petitioners, sponsor, cosponsors, title, status, city_town, summary, committees, bill_url]
    return([bill_details, bill_actions])

########################################################
############## SCRAPE SESSION(S)
############################################
# bill_info = session_bills[4202]
# s_name, s_token = sessions[0]

for s_name, s_token in sessions:

    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'bill_type', 'filed_by', 'presenter', 'all_petitioners', 'sponsor', 'cosponsors', 'title', 'status', 'city_town', 'summary', 'committees', 'bill_url']]
    session_actions = [['bill_number', 'session', 'chamber', 'action_date', 'action','order']]

    print("\n ------------------- Now Scraping the {} MA General Court ---------------------- \n".format(s_name))

    ### Get all bills for a specific session
    session_bills = get_session_bills(s_name, s_token)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        try:
            bill_data = get_bill_data(bill_info, s_name)
        except socket.timeout:
            print(" ~~> Socket Timeout -- Retrying the Operation \n URL: {} \n".format(bill_info[4]))
            time.sleep(60)
            bill_data = get_bill_data(bill_info, s_name)

        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n URL: {} **********".format(num, total, bill_info[0], bill_info[4]))
            num += 1
            continue

        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_info[0], bill_info[4] ))
        num += 1

    with open("MA_Bill_Details_" + s_name + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("MA_Bill_Histories_" + s_name + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} SESSION SCRAPED + DATA SAVED  -------------\n\n\n".format(s_name))


print("  ********************************** ALL DONE ********************************** ")
