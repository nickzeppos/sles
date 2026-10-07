# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape Texas Bills

@author: PB
"""

##### NOTES:
#
###########################

import csv
import os
import re
from ftplib import FTP
import urllib
from bs4 import BeautifulSoup
import time
import datetime
from pathlib import Path
import urllib.request

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#######################
##### Extract Session Links
########################

session_ftp = FTP('ftp.legis.state.tx.us')
session_ftp.login()
session_ftp.cwd('/bills/')
sessions = session_ftp.nlst()
session_ftp.close()
### Drop Sessions Before 2023 (87th legislative session was 2021-22)
sessions = [i for i in sessions if int(i[:2]) > 87]
### Drop Previously Scraped
sessions = [i for i in sessions if 'TX_Bill_Details_' + i + '.csv' not in os.listdir('.')]


#######################################################
########## GET BILL URLS VIA FTP SITE FOR A SESSION
#####################################################

def get_session_bills(session, base_ftp = 'ftp.legis.state.tx.us'):
    ftp = FTP(base_ftp)
    ftp.login()
    bill_urls = []

    ## Navigate to Session Author Folder
    ftp.cwd('/bills/{}/reports/author/'.format(session))

    ## Loop Through Author Pages to Find NEW BILL URLS (Coauthored bills will be repeated)
    author_list = [a for a in ftp.nlst() if 'htm' in a.lower() and a.lower() != 'index.htm']
    N = len(author_list)
    num = 1
    for author in author_list:
        this_author = urllib.request.urlopen('ftp://ftp.legis.state.tx.us/bills/{}/reports/author/{}'.format(session, author))
        time.sleep(1)
        this_soup = BeautifulSoup(this_author, 'lxml')
        these_bills = this_soup.findAll('a', href = re.compile('BillLookup'))
        for bill in these_bills:
            this_url = bill['href'].replace('amp;', '')
            if this_url not in bill_urls:
                bill_urls.append(this_url)
        print(" -- Author Page {}/{} ".format(num, N))
        num += 1

    ftp.close()

    # Make sure to check for any bills missing from the author pages
    if session == '883':
        bill_urls.append('http://capitol.texas.gov/BillLookup/History.aspx?LegSess=883&Bill=SB81')
    return(bill_urls)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_url = 'https://capitol.texas.gov/BillLookup/History.aspx?LegSess=81R&Bill=HB3'
# session = '81R'

chamber_dict = {'H':'House', 'S':'Senate', 'E':'Executive'}

def get_bill_data(session, bill_url):

    try:
        bill_page = urllib.request.urlopen(bill_url, timeout = 5)
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(10)
            bill_page = urllib.request.urlopen(bill_url, timeout = 15)
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            bill_page = urllib.request.urlopen(bill_url, timeout = 30)

    ### Get Bill Data
    try:
        bill_soup = BeautifulSoup(bill_page, 'lxml')
    except TimeoutError:
        print("\n timeout error")
        time.sleep(30)
        try:
            bill_soup = BeautifulSoup(bill_page, 'lxml')
        except TimeoutError:
            print("\n timeout error 2x")
            time.sleep(90)
            try:
                bill_soup = BeautifulSoup(bill_page, 'lxml')
            except:
                print("restarting")
                get_bill_data(session, bill_url)



    #########
    ### Base Details
    ########

    bill_num = bill_soup.find('span', id = 'usrBillInfoTabs_lblBill').get_text()
    summary = bill_soup.find('td', id = 'cellCaptionText').get_text().strip()
    try:
        keywords = bill_soup.find('td', id = 'cellSubjects').contents
        keywords = [re.sub(' +', ' ',i) for i in keywords if str(i) != '<br/>']
        keywords = '; '.join(keywords)
    except:
        keywords = ''

    ##########
    ### Committees
    ############
    try:
        house_comm_row = bill_soup.find('td', string = re.compile('House Committee:'))
        house_comm = house_comm_row.findNext('td').find('a').get_text()
        if '1' in house_comm_row['id']:
            house_comm_status = bill_soup.find('td', id = 'cellComm1CommitteeStatus').get_text()
        elif '2' in house_comm_row['id']:
            house_comm_status = bill_soup.find('td', id = 'cellComm2CommitteeStatus').get_text()
        else:
            house_comm_status = ''
    except:
        house_comm = ''
        house_comm_status = ''

    try:
        senate_comm_row = bill_soup.find('td', string = re.compile('Senate Committee:'))
        senate_comm = senate_comm_row.findNext('td').find('a').get_text()
        if '1' in senate_comm_row['id']:
            senate_comm_status = bill_soup.find('td', id = 'cellComm1CommitteeStatus').get_text()
        elif '2' in senate_comm_row['id']:
            senate_comm_status = bill_soup.find('td', id = 'cellComm2CommitteeStatus').get_text()
        else:
            senate_comm_status = ''
    except:
        senate_comm = ''
        senate_comm_status = ''

    ############
    ### Affiliated Members
    #############
    try:
        authors = bill_soup.find('td', id = 'cellAuthors').get_text().strip().replace(' | ', '; ')
    except:
        authors = ''

    try:
        coauthors = bill_soup.find('td', id = 'cellCoauthors').get_text().strip().replace(' | ', '; ')
    except:
        coauthors = ''

    try:
        sponsors = bill_soup.find('td', id = 'cellSponsors').get_text().strip().replace(' | ', '; ')
    except:
        sponsors = ''

    try:
        cosponsors = bill_soup.find('td', id = 'cellCosponsors').get_text().strip().replace(' | ', '; ')
    except:
        cosponsors = ''

    # ******* COULD ALSO GET CONFERENCE MEMBERS

    ############
    ## Bill History
    ############
    hist_table = bill_soup.find('table', id = 'usrBillInfoActions_tblActions').findNext('table')
    table_rows = hist_table.findAll('tr')

    bill_actions = []
    order = len(table_rows) - 1
    for row in table_rows[1:]:
        cells = row.findAll('td')
        chamber = chamber_dict[cells[0].get_text().strip()]
        action = cells[1].get_text().strip()
        comment = cells[2].get_text().strip()
        date = cells[3].get_text().strip()
        date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
        journal_page = cells[5].get_text().strip()
        bill_actions.append([bill_num, session, chamber, date, action, comment, journal_page, order])
        order -= 1

    #### Save to Export
    bill_details = [bill_num, session, summary, keywords, authors, coauthors, sponsors, cosponsors, house_comm, house_comm_status, senate_comm, senate_comm_status, bill_url]
    return([bill_details, bill_actions])




########################################################
############## SCRAPE SESSION(S)
############################################

for s in sessions:

    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'summary', 'keywords',  'authors', 'coauthors', 'sponsors', 'cosponsors', 'house_committee', 'hc_status', 'senate_committee', 'sc_status', 'bill_url']]
    session_actions = [['bill_number', 'session', 'chamber', 'action_date', 'action', 'comment', 'journal_page', 'order']]

    print("\n ----- Now Scraping: Session " + s + " ---------\n")

    ### Get all bills for a specific session
    session_bills = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for b_url in session_bills:

        bill_data = get_bill_data(s, b_url)
        time.sleep(1)

        ### Append Data
        if bill_data == "No Data":
            num += 1
            continue

        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_data[0][0], b_url))
        num += 1

    with open("TX_Bill_Details_" + s + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("TX_Bill_Histories_" + s + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- " + s + " SCRAPED + DATA SAVED  -------------\n\n\n")


print("  ********************************** ALL DONE ********************************** ")
