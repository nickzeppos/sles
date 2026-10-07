# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape North Carolina Bills

@author: PB
"""

import csv
import os
import requests
from bs4 import BeautifulSoup
import re
import datetime
import socket
import urllib
import time
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#######################
##### Extract Session Links
########################

this_year = datetime.datetime.now().year

### Get Sessions (Via OpenStates, this URL populates search select)
session_list = requests.get('https://webservices.ncleg.net/sessionselectlist/false')
session_soup = BeautifulSoup(session_list.content, "lxml")

sessions = [[i['data-session-year'], i['value'], i.get_text()] for i in session_soup.findAll('option')]

s_dict = {'Session':'RS', 'Extra Session':'SS1', 'First Extra Session':'SS1', 'Second Extra Session':'SS2',
          'Third Extra Session':'SS3', 'Fourth Extra Session':'SS4', 'Fifth Extra Session':'SS5',
          'Special Session':'SS1', '1st Special Session':'SS1', '2nd Special Session':'SS2'}

for s in sessions:
    sy, stype = s[2].split(' ', 1)
    sy = re.sub('-.+', '', sy)
    stype = s_dict[stype]
    sessions = [i + [sy + '-' + stype] if i == s else i for i in sessions ]

### Drop Previously Scraped
print('\n ************* DROPPING PREVIOUSLY SCRAPED SESSIONS ************* \n')
sessions = [i for i in sessions if 'NC_Bill_Details_{}'.format(i[3].replace('-', '_')) + '.csv' not in os.listdir('.')]

### Drop Current Session
print('\n ************* DROPPING CURRENT YEAR SESSIONS ************* \n')
sessions = [i for i in sessions if int(i[0]) < this_year]

del this_year, session_list, session_soup, s_dict



##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################


def get_page_soup(bill_url, parser = 'lxml'):
    try:
        page = urllib.request.urlopen(bill_url, timeout = 20).read()
    except urllib.error.HTTPError: # as e
        return('HTTP Error')
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
            page = urllib.request.urlopen(bill_url, timeout = 60).read()
    time.sleep(1)
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)


########################################################
##### FUNCTION TO SCRAPE BILL DETAILS
##########################################################

# s = sessions[0]
def get_session_bills(s):
    s_value = s[1]
    s_data = {'Session':s_value, 'tab':'Chamber', 'Chamber':''}
    session_bills = []
    list_req = requests.get("https://www.ncleg.gov/Legislation/Bills/LastActionByYear/{}/All".format(s[1]))
    list_soup = BeautifulSoup(list_req.content, 'lxml')
    bill_table = list_soup.find('table', {'id': 'bill-report'})
    for c in ['House', 'Senate']:
        s_data['Chamber'] = c
        for bill in bill_table.findAll('tr')[1:]:
            cells = bill.findAll('td')
            bid = cells[0].text
            if '(' in bid:
                bid, companion = re.split('\\(', bid)
                bid = bid.strip()
                companion = re.sub('=|\\)', '', companion).strip()
            else:
                companion = ''
            shorttitle = cells[3].a.text.strip()
            bill_url = 'https://www.ncleg.gov' + cells[3].a['href']
            bill_items = [bid, companion, s_value, shorttitle, bill_url]
            if bill_items not in session_bills:
                session_bills.append(bill_items)
    return(session_bills)

#########################################################################
####### Function to Scrape a Bill Page
##########################################################################

# bill_info = session_bills[0]
# s = sessions[0]



def get_bill_data(bill_info):

    bill_num, companion, s_value, short_title, bill_url = bill_info
    term = '{}_{}'.format(s[0], str(int(s[0]) + 1))
    sess_id = s[3]

    ### CLEAN BILL NUMBERS
    bill_parts = [i.strip() for i in re.split('([0-9]+)', bill_num) if i.strip() != '']
    bill_num_z = bill_parts[0] + bill_parts[1].zfill(4)

    ### Get Bill Data
    bill_soup = get_page_soup(bill_url, parser = 'lxml')

    if bill_soup == 'HTTP Error':
        return("HTTP Error")

    ### BILL TYPE
    bill_type = bill_soup.find('div', {'class':'col-12 col-sm-6 h2 text-center order-sm-2'}).text.strip()
    bill_type = re.sub('^House|^Senate|[0-9]+', '', bill_type).strip()

    ### Bill Details
    attributes = bill_soup.find('div', string = re.compile('Attributes:')).find_next_sibling("div").get_text()
    counties = bill_soup.find('div', string = re.compile('Counties:')).find_next_sibling("div").get_text()
    statutes = bill_soup.find('div', string = re.compile('Statutes:')).find_next_sibling("div").get_text()
    keywords = bill_soup.find('div', string = re.compile('Keywords:')).find_next_sibling("div").get_text().lower()

    #### SPONSORS
    sponsors = bill_soup.find('div', string = re.compile('Sponsors:')).find_next_sibling("div")
    sponsors = sponsors.findChildren("div", recursive = False)
    sponsors = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', '', str(sponsors)).replace('\xa0', '').replace('\r', '').replace('\n', '').replace('[', '').replace(']', ''))
    match = re.search(r'(.+?)\s*\(Primary\)\s*(.+)?', sponsors)
    primary_sponsors = '; '.join([s.strip() for s in re.split(r'[;,]', match.group(1).strip()) if s.strip()])
    if match.group(2):
        cosponsors = '; '.join([s.strip() for s in re.split(r'[;,]', match.group(2).strip()) if s.strip()])
    else:
        cosponsors = ''

    #########
    ### ACTIONS
    bill_actions = []
    history = bill_soup.find("h6", string = re.compile('History'))
    hist_table = history.findNext('div', {'class':'card-body'})

    hist_rows = hist_table.findAll('div', {'class':'row'})
    if hist_rows != []:
        order = len(hist_rows)
        for row in hist_rows:
            date = row.find('div', string = re.compile('Date:')).find_next_sibling("div").get_text().strip()
            if date != '':
                date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')

            chamber = row.find('div', string = re.compile('Chamber:')).find_next_sibling("div").get_text()
            action = row.find('div', string = re.compile('Action:')).find_next_sibling("div").get_text().strip()
            vote_cell = row.find('div', string = re.compile('Votes:'))
            if vote_cell is not None:
                vote_cell = vote_cell.find_next_sibling("div")
                votes = vote_cell.get_text().strip('\n')
                votes = re.sub('None', '', votes)
                if votes != '':
                    votes = '{} ~~ https://www.ncleg.gov{}'.format(votes, vote_cell.a['href'])
            else:
                votes = ''
            bill_actions.append([bill_num_z, term, sess_id, date, chamber, action, order, votes])
            order -= 1

    ### OUTPUT
    bill_details = [bill_num_z, term, sess_id, bill_type, companion, primary_sponsors, cosponsors, short_title, attributes, counties, statutes, keywords, bill_url]
    return([bill_details, bill_actions])


########################################################
############## SCRAPE SESSION(S)
############################################
# s = sessions[22]

for s in sessions:

    #### Output Lists
    session_bill_details = [['bill_num', 'term', 'session', 'bill_type', 'companion', 'primary_sponsors', 'cosponsors', 'short_title', 'attributes', 'counties', 'statutes', 'keywords', 'bill_url']]
    session_actions = [['bill_num', 'term', 'session', 'action_date', 'chamber', 'action', 'order', 'votes']]

    print("\n ------------- NORTH CAROLINA - Scraping: {} ---------------- \n".format(s[2]))

    ### Get all bills for a specific session
    session_bills = get_session_bills(s)
    print("\n  ***** ~~~~~> FOUND {} BILL URLS ***** \n".format(len(session_bills)))

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        bill_data = get_bill_data(bill_info)

        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING -- {}\n **********".format(num, total, bill_info[0], bill_info[-1]))
            num += 1
            continue

        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_data[0][0], bill_data[0][-1]))
        num += 1

    with open("NC_Bill_Details_{}".format(s[3].replace('-', '_')) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("NC_Bill_Histories_{}".format(s[3].replace('-', '_')) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n -------- NORTH CAROLINA --- {} ---  SCRAPED + DATA SAVED  ---------\n\n\n".format(s[2]))


print("  ********************************** ALL DONE ********************************** ")
