# -*- coding: utf-8 -*-
"""
Created on Mon Nov 12 17:00:06 2018

@author: PB
"""

###################################################
##### SCRAPE FLORIDA BILLS, 1998 to Present
# *** NOTE:
# -- Senate data back to 1998 is exellent; HOUSE data looks like its still being filled in for earliest terms...
# ---> STARTING at 2001 for now, can go bakc in time further later on...
# ---> HOWEVER: Senate data does not idenitfy sponsors with duplicate names as well... and House data only does so for House members?
##################################################

import csv
import os
import requests
from bs4 import BeautifulSoup
import re
import time
import urllib
import datetime
import socket
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#######################
##### GET SESSIONS
########################

sessions = requests.get("http://flsenate.gov/Session/Bills")
session_soup = BeautifulSoup(sessions.content, "lxml")

years = session_soup.find("select", {"name":"SessionYear", "id":"session-name"})
years = years.findAll("option")
years = [y['value'] for y in years]

#### Drop Previously Scraped
previously_scraped = []
for y in years:
    if 'FL_Agg_Votes_' + y + '.csv' in os.listdir('.'):
        previously_scraped.append(y)

for y in previously_scraped:
    years.remove(y)

years = [y for y in years if int(y[0:4]) >= 2023]

#### Dropping Subsequent Session
next_year = datetime.datetime.now().year + 1
print('\n ~~~~~~~~~~~~ DROPPING {} SESSION  *************** \n'.format(next_year) )
years = [y for y in years if int(y[0:4]) < next_year]

del next_year, session_soup, sessions

###############################
##### GET Bill URLS
##############################

def gather_page_urls(soup, yk, url_list):
    bill_table = soup.find("div", id = "billListDiv").find("tbody")
    if bill_table is None:
        return []
    rows = bill_table.findAll('tr')
    for bill in rows:
        bill_num = bill.find("a").get_text()
        bill_url = 'http://flsenate.gov' + bill.find("a")['href']
        cells = bill.findAll("td")
        title = cells[0].text.strip()
        sponsor = ' '.join(cells[1].get_text().strip().split())
        url_list.append([yk, bill_num, sponsor, title, bill_url ])
    return(url_list)


def scrape_session_bills(year_key):
    print("\n ~~~ Getting URLs for the {} Session ~~~ ".format(year_key))
    year_bill_urls = []
    base_url = 'http://flsenate.gov/Session/Bills/{}?chamber=both&searchOnlyCurrentVersion=True&isIncludeAmendments=False&isFirstReference=True&citationType=FL%20Statutes&pageNumber={}'
    session_start_url = base_url.format(year_key, 1)
    first_page = urllib.request.urlopen(session_start_url, timeout = 10)
    first_soup = BeautifulSoup(first_page, 'lxml')
    last_page = first_soup.find("div", {"class":"ListPagination"}).findAll("a")
    if last_page == []:
        last_page = 1
    else:
        last_page = last_page[len(last_page) - 2].get_text()
    year_bill_urls = gather_page_urls(first_soup, year_key, year_bill_urls)
    print("  -- Page 1 of {}".format(last_page))
    for num in range(2, int(last_page) + 1):
        this_url = base_url.format(year_key, num)
        try:
            this_page = urllib.request.urlopen(this_url, timeout = 5)
        except:
            time.sleep(5)
            this_page = urllib.request.urlopen(this_url, timeout = 5)
        time.sleep(1)
        this_soup = BeautifulSoup(this_page, 'lxml')
        year_bill_urls = gather_page_urls(this_soup, year_key, year_bill_urls)
        print(" -- Page {} of {}".format(num, last_page))
    if year_bill_urls == []:
        return("No Bills Available!")
    else:
        print("\n  -----> {} Bills Found for Session {}".format(len(year_bill_urls), year_key))
        return(year_bill_urls)


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



#############################
### Function to Get Bill Data
#############################

# bill_row = bill
# bill_soup.find("span", text = re.compile('Preserving the Deferred Action for Childhood Arrivals'))

def get_bill_data(bill_row):
    this_year = bill_row[0]
    this_bill_num = bill_row[1]
    this_title = bill_row[3]
    this_url = bill_row[4]
    bill_soup = get_page_soup(this_url)
    if 'A bill with that number does not exist in the selected session' in bill_soup.get_text().strip():
        return("No Bill Data")
    search_title = this_title.replace("(", "\\(").replace(")", "\\)").replace('[', '\\[').replace(']', '\\]')
    summary = bill_soup.find("span", text = re.compile(search_title + ';'))
    if summary is None:
        summary = bill_soup.find("span", text = re.compile(search_title + '\s+;'))
        if summary is None:
            summary = bill_soup.find('span', text = re.compile(search_title + '$'))
            if summary is None:
                return("No Bill Data")
    other_details = summary.parent.findPreviousSibling()
    summary = summary.parent.text.strip().replace(this_title + '; ', '')
    other_details = re.split(' by | by\r\n | by\n| by\r', other_details.text.strip())
    bill_type = other_details[0].strip()
    if len(other_details) == 1 and 'by' not in other_details[0]:
        print('*** ---> NO SPONSORS ON PAGE: {}'.format(this_url))
        sponsors = ''
        cosponsors = ''
    else:
        sponsors = other_details[1].split('(CO-INTRODUCERS)')
        if len(sponsors) == 2:
            cosponsors = ' '.join(sponsors[1].replace("\r\n", "").split())
            sponsors = ' '.join(sponsors[0].replace("\r\n", "").split())
        else:
            sponsors = ' '.join(sponsors[0].replace("\r\n", "").split())
            cosponsors = ''
    sponsors = re.sub(' ;$', '', re.sub(' ; ', '; ', sponsors))
    cosponsors = re.sub(' ;$', '', re.sub(' ; ', '; ', cosponsors))
    hist_tab = bill_soup.find('div', id = 'tabBodyBillHistory').find("tbody")
    rows = hist_tab.findAll('tr')
    bill_history = []
    order = 1
    for row in rows:
        cells = row.findAll('td')
        date = cells[0].text
        if date[-3] == '/':
            date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')
        else:
            date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
        chamber = cells[1].text
        actions = re.split('\r\n| .HJ [0-9][0-9]+; | .SJ [0-9][0-9]+; ', cells[2].text.strip())
        actions = [i for i in actions if i != '']
        for action in actions:
            bill_history.append([this_year, this_bill_num, chamber, date, action.replace('• ', ''), order])
            order += 1
    vote_tab = bill_soup.find('div', id = 'tabBodyVoteHistory')
    bill_votes = []
    if vote_tab.find("span", text = re.compile('No Committee Vote History Available')) is None:
        commitee_votes = vote_tab.find("h4", text = 'Vote History - Committee').findNextSibling()
        rows = commitee_votes.findAll("tr")
        for row in rows[1:]:
            cells = row.findAll("td")
            vote_id = cells[0].text.strip().replace("\r\n", " ")
            vote_date = cells[1].text.strip().split(" ")[0]
            vote_body = cells[2].text.strip()
            vote_result = cells[3].text.strip()
            vote_url = 'http://flsenate.gov' + cells[3].find('a')['href']
            bill_votes.append([this_year, this_bill_num, vote_id, vote_date, vote_body, vote_result, vote_url])
    if vote_tab.find("span", text = re.compile('No Vote History Available')) is None:
        floor_votes = vote_tab.find("h4", text = 'Vote History - Floor').findNextSibling()
        rows = floor_votes.findAll("tr")
        for row in rows[1:]:
            cells = row.findAll("td")
            vote_id = cells[0].text.strip().replace("\r\n", " ")
            vote_date = cells[1].text.strip().split(" ")[0]
            vote_body = cells[2].text.strip()
            vote_result = cells[3].text.strip()
            vote_url = 'http://flsenate.gov' + cells[3].find('a')['href']
            bill_votes.append([this_year, this_bill_num, vote_id, vote_date, vote_body, vote_result, vote_url])
    bill_details = bill_row + [bill_type, sponsors, cosponsors, summary]
    return([bill_details, bill_history, bill_votes])



####################################
##### LOOP THROUGH SESSIONS
#####################################
# bill = session_bills[469]
# this_year = '2006'

for this_year in years:

    print('\n ****** Now Scraping Data for the {} Sesssion *******'.format(this_year))

    ### Get Bill URLs
    session_bills = scrape_session_bills(this_year)
    time.sleep(1)

    ### Skip Session if No Data on Page -- Continue doesn't work here for some reason
    if session_bills == 'No Bills Available!':
        print('\n *****  NO BILLS AVAILABLE FOR SESSION {}  -- SKIPPING! *****'.format(this_year))
        continue

    ### For Output
    session_bill_details = [['session', 'bill_num', 'primary_sponsor', 'short_title', 'bill_url', 'bill_type', 'sponsors', 'cosponsors', 'summary']]
    session_bill_histories = [['session', 'bill_num', 'chamber', 'action_date', 'action', 'order']]
    session_votes = [['session', 'bill_num', 'vote_id', 'vote_date', 'vote_body', 'vote_result', 'vote_url']]

    ### Get Bill Data
    num = 1
    for bill in session_bills:
        bill_data = get_bill_data(bill)

        if bill_data == "No Bill Data":
            print('  -- ({}) {}: No Bill Data -- {}'.format(num, bill[1], bill[4]))
            continue

        session_bill_details.append(bill_data[0])
        for hist_item in bill_data[1]:
            session_bill_histories.append(hist_item)

        if bill_data[2] != []:
            for vote_item in bill_data[2]:
                session_votes.append(vote_item)

        print('  -- ({}) {}: {}'.format(num, bill_data[0][1], bill_data[0][4]))
        num += 1

    ### SAVE!
    with open("FL_Bill_Details_" + this_year + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("FL_Bill_Histories_" + this_year + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_histories)

    with open("FL_Agg_Votes_" + this_year + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_votes)

    print('\n ******     Session {} Complete!     *******'.format(this_year))
