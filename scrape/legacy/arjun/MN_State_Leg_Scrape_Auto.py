# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape MN Legislation ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# In theory, can get more detailed committee actions/long description, but data seems just ok.
#
# Getting BILL/RESOLUTION type not easy... need to scrape first paragraph of bill text. Should start with 'A bill' or 'A resolution'
####################

import csv
import os
import urllib
import requests
from bs4 import BeautifulSoup
import time
import datetime
from dateutil import parser as dateparser
import re
import socket
#import itertools
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

##################################
##### Extract Session Information
##########################################

session_search = urllib.request.urlopen('https://www.revisor.mn.gov/bills/status_search.php?search=advanced', timeout = 30)
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.find('select', id = 'session').findAll('option')
sessions = [[i.get_text().split(', ')[0], i.get_text().split(', ')[1], i['value']] for i in session_list]

### Create Shortened Session
for i in range(0, len(sessions)):
    s = sessions[i]
    new = s[1][0:4]
    if 'Session' in s[1]:
        new = new + "_S" + s[1][5]
    else:
        new = new + "_RS"
    sessions[i].append(new)

### Drop Previously Scraped
sessions = [s for s in sessions if 'MN_Bill_Details_' + s[3] + '.csv' not in os.listdir('.') ]

### Drop Current Session
this_year = datetime.datetime.now().year
sessions = [s for s in sessions if int(s[1][0:4]) < this_year ]

del session_search, session_search_soup, session_list, i, s, new


#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
#this_session = sessions[1]

def get_session_bills(this_session):

    sess, s_name, s_id, s_short = this_session

    print('\n~~~~ Gathering Bill URLs for the {} Session ~~~~\n'.format(s_name))
    unique_bills = []
    bill_info = []

    search_url = 'https://www.revisor.mn.gov/bills/status_result.php?body={}&search=advanced&session={}&submit_advanced=GO&search_type=andor&keyword_type=all&size=9999'

    #### Get URLs/Info for each Chamber
    # ** Adjusting for potential repeat bills via bills from other chamber -- which seems to happen sometimes
    # ---> KEY POINT: If senate bill, need to keep url with b=Senate; same for house... otherwise author code will yield no author.
    for chamber in ['House', 'Senate']:
        req = requests.get(search_url.format(chamber, s_id))
        time.sleep(1)
        req_soup = BeautifulSoup(req.content, 'lxml')
        chamber_data = req_soup.find('table', {'class':'table table-sm table-responsive'})
        for row in chamber_data.findAll('tr')[1:]:
            cells = row.findAll('td')
            bn = cells[1].get_text().strip()
            ## Only record House bills from house search and senate bills from senate search
            if (chamber == "House" and bn[0:1] == 'S') or (chamber == "Senate" and bn[0:1] == 'H'):
                continue

            ### Make sure URLs go to Intro Chamber Page -- Previous skipping should mostly take care of this
            if bn[0:1] == 'H':
                b_url = 'https://www.revisor.mn.gov' + cells[1].find('a')['href']
                b_url = b_url.replace('b=Senate', 'b=House')
            elif bn[0:1] == 'S':
                b_url = 'https://www.revisor.mn.gov' + cells[1].find('a')['href']
                b_url = b_url.replace('b=House', 'b=Senate')
            else:
                b_url = 'https://www.revisor.mn.gov' + cells[1].find('a')['href']
            #num_actions = int(cells[2].get_text().strip())
            #last_action_date = cells[3].get_text().strip()
            author = cells[6].get_text().strip()
            descrip = cells[7].get_text().strip()
            if bn not in unique_bills:
                unique_bills.append(bn)
                bill_info.append([bn, s_short, author, descrip, b_url])

    return(bill_info)

##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):

    ### Get HTML
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

    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)

#######################
###### Functions to Scrape Individual Bills
#########################
# bill_info = session_bills[0]
# this_session = sessions[0]

def get_bill_data(bill_info, this_session):

    ### Base Data
    sess, s_name, s_id, s_short = this_session
    bill_num, s_short, author, descrip, bill_url = bill_info

    ### Scrape Bill Page
    bill_soup = get_page_soup(bill_url)

    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')

    ### Supplementary Data -- Most in URL Info File Already
    # ~ Author code below will catch senate coauthors as well
    all_authors = bill_soup.find('div', {'class':'author'})
    if all_authors is None:
        all_authors = ''
    else:
        all_authors = '; '.join([a.get_text().strip() for a in all_authors.findAll('a')])

    if bill_num[0:1] == "H":
        outchamber_authors = bill_soup.find('h3', {'class':'senate'}, text = re.compile('Senate Authors'))
    else:
        outchamber_authors = bill_soup.find('h3', {'class':'house'}, text = re.compile('House Authors'))

    if outchamber_authors is None:
        outchamber_authors = ''
    else:
        outchamber_authors = outchamber_authors.findNext('div', {'class':'author'})
        outchamber_authors = '; '.join([a.get_text().strip() for a in outchamber_authors.findAll('a')])

    companion = bill_soup.find(text = re.compile('Companion:'))
    if companion is not None:
        if companion.name is None:
            companion = companion.parent.find('a').get_text()
        companion = re.sub('\s\s+', ' ', companion.strip().replace('\r\n', ''))
        companion = companion.replace('Companion: ', '').replace('None', '')
    else:
        companion = ''

    ########## Summary
    text_url = bill_soup.find('a', href = re.compile('bills/text.php|text.php'))
    if text_url is None:
        print('----> No Summary for {}!'.format(bill_num))
        summary = ''
    else:
        ### BILLS
        if bill_num[0:2] in ['HF', 'SF']:
            if text_url['href'][0:1] == '/':
                text_url = 'https://www.revisor.mn.gov' + text_url['href']
            else:
                 text_url = 'https://www.revisor.mn.gov/bills/' + text_url['href']

            text_soup = get_page_soup(text_url)
            if re.search('No Text File Found|Document [A-Z]+[0-9]+ was not found', text_soup.text):
                summary = ''
            else:
                header = text_soup.find('span', {'class':'btitle_prolog'})
                if header is None:
                    header = text_soup.find('div', id = 'xtend').find('pre')
                    if header is None:
                        header = text_soup.find('div', id = 'xtend')

                    summary = header.get_text(' ').strip().lower()
                    summary = re.sub('\n', ' ', summary)
                    summary = re.sub('whereas.+|be it enacted by.+', '', summary)
                    summary = re.sub('[1-2]\\.[0-9]+ +', '', summary)
                    summary = re.sub('  +', ' ', summary).strip()
                else:
                    summary = header.findParents('p')[0].text
                    summary = re.sub('  +', ' ', summary).strip()

        else:
            ### Resolutions don't redirect for some reason..
            text_url = 'https://www.revisor.mn.gov/bills/' + text_url['href']
            s_key = re.sub('\\&.+', '', re.sub('^.+session=', '', text_url))
            s_num = re.sub('\\&.+', '', re.sub('^.+session_number=', '', text_url))
            if bill_num[0] == 'H':
                text_url = 'https://www.house.leg.state.mn.us/resolutions/{}/{}/{}.htm'.format(s_key, s_num, bill_num)
            else:
                s_key = s_key.replace('ls', '')
                bt = re.sub('[0-9].+', '', bill_num)
                bn = re.sub('[A-Z]+', '', bill_num)
                bn = str(int(bn))
                text_url = 'https://www.senate.mn/resolutions/display_resolution.php?ls={}&bill_type={}&bill_number={}&ss_number={}&ss_year=2017'.format(s_key, bt, bn, s_num, s_short[0:4])
                # text_url = 'https://www.senate.leg.state.mn.us/resolutions/{}/{}/{}.htm'.format(s_key, s_num, bill_num)
            text_soup = get_page_soup(text_url)

            if text_soup == 'HTTP Error' or 'No Text File Found' in text_soup.text:
                summary = ''
            else:
                header = text_soup.find('div', {'class':'xtend'})
                if header is None:
                    header = text_soup.findAll('pre')[1]

                summary = header.get_text(' ').strip().lower()
                summary = re.sub('\r\n|\n|\r', ' ', summary)
                summary = re.sub('whereas.+|be it enacted by.+', '', summary)
                summary = re.sub('[1-2]\\.[0-9]+ +', '', summary)
                summary = re.sub('  +', ' ', summary).strip()

    ########### Actions -- If no actions, tab isn't there, shows cosponsors
    bill_actions = []
    for chamber in ['House', 'Senate']:
        chamber_order = 1
        chamber_actions = bill_soup.find('div', {'class':chamber.lower()})
        if chamber_actions is None:
            continue
        else:
            chamber_rows = chamber_actions.findAll('tr')
            for row in chamber_rows:
                cells = row.findAll('td')
                date = cells[0].get_text()
                action = cells[1].find('div', {'class':'col'}).get_text().strip()
                action = re.sub('\s\s+', ' ', action)

                if date == '':
                    date = re.findall('\d{1,2}/\d{1,2}/\d{2,4}', action)
                    #date = dateutil.parser.parse(action, fuzzy = True)
                    if len(date) > 0:
                        date = date[len(date) - 1] # Taking date at end of string if multiple
                        action = action.replace(date, '').strip()
                        date = dateparser.parse(date.strip())
                        date = date.strftime('%Y-%m-%d')
                    else:
                        date = bill_actions[-1][3]
                else:
                    date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')

                notes = cells[1].find('div', {'class':'action_item col'}).findAll('span')
                action_note = ''
                journal_page = ''
                if notes is not None and notes != []:
                    journal_page = re.sub('pg.', '', notes[0].get_text()).strip()
                    if journal_page == 'Intro':
                        journal_page = ''
                    if len(notes) == 2:
                        action_note = notes[1].get_text()
                        action_note = re.sub('\r\n|\s\s+', ' ', action_note)
                bill_actions.append([bill_num, s_short, chamber, date, action, journal_page, action_note, chamber_order])
                chamber_order += 1

    ### OUTPUT ~~~ Note: Vote Urls are in the Documents Tab of the Newer Format
    bill_details = [bill_num, s_short, author, all_authors, outchamber_authors, companion, descrip, summary, bill_url]
    return([bill_details, bill_actions])

########################################################
############## SCRAPE SESSION(S)
############################################
# bill_info = session_bills[3228]
# s_name, s_token = sessions[1]
# this_session = sessions[0]

for this_session in sessions:

    sess, s_name, s_id, s_short = this_session

    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'author', 'coauthors', 'outchamber_authors', 'companion', 'description', 'summary', 'bill_url']]
    session_actions = [['bill_number', 'session', 'chamber', 'action_date', 'action', 'journal_page', 'action_note', 'chamber_order']]

    print("\n ---------- Now Scraping the {} ({}) ------------ \n".format(sess, s_name))

    ### Get all bills for a specific session
    session_bills = get_session_bills(this_session)

    ### Drop Duplicates
    #session_bills.sort()
    #session_bills = list(session_bills for session_bills,_ in itertools.groupby(session_bills))

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        bill_data = get_bill_data(bill_info, this_session)

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

    with open("MN_Bill_Details_" + s_short + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("MN_Bill_Histories_" + s_short + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} ({}) SCRAPED + DATA SAVED  -------------\n\n\n".format(sess, s_name))


print("  ********************************** ALL DONE ********************************** ")
