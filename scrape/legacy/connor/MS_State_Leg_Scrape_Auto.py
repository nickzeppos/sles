# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape Mississippi Legislation 1997 - PRESENT ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# Urls for Bill Lists Change in 2007 (.htm vs .xml), 1998 (no /pdf, still =.htm)
# History format difference is just /pdf/ or not -- splits at 1998
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

session_search = urllib.request.urlopen('http://billstatus.ls.state.ms.us/sessions.htm', timeout = 30)
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.findAll('a', href = re.compile('mainmenu'))
sessions = [[i.get_text().strip(), 'http://billstatus.ls.state.ms.us' + i['href']] for i in session_list]

s_dict = {'Regular Session':'RS', 'First Extraordinary Session':'ES1', 'Second Extraordinary Session':'ES2',
          'Third Extraordinary Session':'ES3', 'Fourth Extraordinary Session':'ES4', 'Fifth Extraordinary Session':'ES5',
          'Extraordinary Session':'ES'}

for i in range(0, len(sessions)):
    yr, s_type = sessions[i][0].split(' ', 1)
    yr = int(yr)
    # print(yr, s_type)
    s_type = s_dict[s_type]
    if yr >= 2008:
        list_url = sessions[i][1].replace('mainmenu.htm', 'all_measures/allmsrs.xml')
    # elif yr >= 1999: list_url = sessions[i][1].replace('mainmenu.htm', 'all_measures/allmsrs.htm')
    else:
        list_url = sessions[i][1].replace('mainmenu.htm', 'all_measures/allmsrs.htm')
    session_stem = sessions[i][1].replace('/mainmenu.htm', '')

    sessions[i] = [yr, s_type, list_url, session_stem]

### Drop Previously Scraped
sessions = [s for s in sessions if 'MS_Bill_Details_' + str(s[0]) + "_" + s[1] + '.csv' not in os.listdir('.')]

### Drop Current Session
this_year = datetime.datetime.now().year
sessions = [s for s in sessions if s[0] < this_year]
print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))

del this_year, session_search, session_search_soup, session_list, yr, s_type, list_url, session_stem, s_dict

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s = sessions[55]
# test = get_session_bills(sessions[26])

def get_session_bills(s):

    s_year, s_type, s_url, s_stem = s

    print('\n~~~~ Gathering Bill URLs for the {} {} Session ~~~~\n'.format(s_year, s_type))

    s_req = requests.get(s_url)
    s_soup = BeautifulSoup(s_req.content, 'lxml')

    session_bills = []
    if s_year >= 2008:
        bill_rows = s_soup.findAll('msrgroup')
        for bill in bill_rows:

            bn = re.sub('[A-Z]+', '', bill.find('measure').text).strip()
            bs = re.sub('[0-9]+', '', bill.find('measure').text).strip()
            bill_num = bs + bn.zfill(4)

            bill_url = s_stem + re.sub('^\\.\\.', '', bill.find('actionlink').text.strip())
            session_bills.append([bill_num, s_year, s_type, bill_url])

    else:

        history_rows = s_soup.findAll('a', href = re.compile('history'))

        for bill in history_rows:

            bill_url = s_stem + re.sub('^\\.\\.', '', bill['href'])

            bill_num = re.sub('^.+/|.htm|.xml', '', bill_url)
            bn = re.sub('[A-Z]+', '', bill_num).strip()
            bs = re.sub('[0-9]+', '', bill_num).strip()
            bill_num = bs + bn.zfill(4)

            session_bills.append([bill_num, s_year, s_type, bill_url])

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
    bill_num, s_year, s_type, bill_url = bill_row

    ### Get HTML, Soup
    bill_soup = get_page_soup(bill_url)

    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')

    ## Get Basic Data by Format
    if s_year >= 2008:

        ### XML format..
        if bill_soup.find('longtitle') is not None:
            title = bill_soup.find('longtitle').text
        else:
            title = ''

        summary = bill_soup.find('shorttitle').text

        author = bill_soup.findAll('principal')
        author = '; '.join([i.find('p_name').text for i in author])

        coauthors = bill_soup.find('additional')
        if coauthors is not None:
            coauthors = '; '.join([i.text for i in bill_soup.findAll('co_name')])
        else:
            coauthors = ''

        ## Background Info Table
        status = bill_soup.find('disposition').text
        revenue_bill = bill_soup.find('revenue').text
        vote_type = bill_soup.find('votetype').text

        committees = bill_soup.find('committees')
        if committees.findAll('house') is not None:
            house_comm = '; '.join([i.text for i in bill_soup.findAll('h_name')])
        else:
            house_comm = ''

        if committees.findAll('senate') is not None:
            senate_comm = '; '.join([i.text for i in bill_soup.findAll('s_name')])
        else:
            senate_comm = ''

        ## Actions --- Could Grab Vote Urls ('act_vote') if wanted..
        bill_actions = []
        hist_items = bill_soup.findAll('action')
        for item in hist_items:
            order = int(item.find('act_number').text.strip())
            act_desc = re.split('\s+', item.find('act_desc').text, 2)
            if len(act_desc) < 3 or '(' not in act_desc[1]:
                date = act_desc[0]
                chamber = ''
                action = re.split('\s+', item.find('act_desc').text, 1)[1]
            else:
                date, chamber, action = act_desc
                chamber = re.sub('\\(|\\)', '', chamber)
            date = datetime.datetime.strptime(date + '/' + str(s_year), '%m/%d/%Y').strftime('%Y-%m-%d')
            action = action.strip()
            bill_actions.append([bill_num, s_year, s_type, chamber, date, action, order])


    else:

        if bill_soup.find('a', {'name':'title'}) is not None:
            title = bill_soup.find('a', {'name':'title'}).findParents('p')[0].get_text().strip()
            title = re.sub('Title:\s+', '', title)
        else:
            title = ''

        summary = bill_soup.find('em', text = re.compile('^Description'))
        summary = summary.findParents('p')[0].get_text().strip()
        summary = re.sub('Description:\s+', '', summary)

        author = bill_soup.find('em', text = re.compile('Principal Author'))
        if author is None and bill_num[0:2] == 'SN':
            author = bill_soup.find('em', text = re.compile('Appointed By'))
            author = author.findParents('p')[0].get_text().strip()
            author = re.sub('Appointed By:\s+', '', author)
        else:
            author = author.findParents('p')[0].get_text().strip()
            author = re.sub('Principal Author:\s+', '', author)

        coauthors = bill_soup.find('em', text = re.compile('Additional Author'))
        if coauthors is not None:
            coauthors = coauthors.findParents('p')[0].findAll('a')
            if coauthors is not None:
                coauthors = '; '.join([i.get_text().strip() for i in coauthors])
            else:
                coauthors = coauthors.findParents('p')[0].get_text().strip()
                coauthors = re.sub('Additional Authors:\s+', '', coauthors)
                coauthors = re.sub(', ', '; ', coauthors)
        else:
            coauthors = ''

        ## Background Info Table
        background = bill_soup.find('em', text = re.compile('^Background Information'))
        background = background.findNext('table').findAll('tr')
        #status = background[1].findAll('td')[2].get_text().strip()
        #revenue_bill = background[3].findAll('td')[2].get_text().strip()
        #vote_type = background[4].findAll('td')[2].get_text().strip()

        status = [i for i in background if i.findAll('td', text = re.compile("Disposition")) != []]
        status = status[0].find('td', {'align':'LEFT'}).get_text().strip()

        revenue_bill = [i for i in background if i.findAll('td', text = re.compile("Revenue")) != []]
        revenue_bill = revenue_bill[0].find('td', {'align':'LEFT'}).get_text().strip()

        vote_type = [i for i in background if i.findAll('td', text = re.compile("Vote type|Three|3/5ths")) != []]
        vote_type = vote_type[0].find('td', {'align':'LEFT'}).get_text().strip()
        if vote_type.lower() in ['yes', 'no']:
            vt_dict = {'yes':'Three/Fifths', 'no':'Majority'}
            vote_type = vt_dict[vote_type.lower()]

        house_comm = bill_soup.findAll('a', href = re.compile('House_cmte'))
        if house_comm != []:
            house_comm = '; '.join([i.get_text(strip = True) for i in house_comm])
        else:
            house_comm = ''

        senate_comm = bill_soup.findAll('a', href = re.compile('Senate_cmte'))
        if senate_comm != []:
            senate_comm = '; '.join([i.get_text(strip = True) for i in senate_comm])
        else:
            senate_comm = ''

        ## Actions --
        hist_table = bill_soup.find('em', text = re.compile('History of Actions'))
        hist_rows = hist_table.findNext('table').findAll('tr')

        bill_actions = []

        if s_year >= 2001 or (s_year == 2000 and s_type == 'ES1'):

            for row in hist_rows:
                cells = row.findAll('td')
                order = int(cells[0].get_text().strip())
                date, chamber, action = re.split('\s+', cells[2].get_text(), 2)
                date = datetime.datetime.strptime(date + '/' + str(s_year), '%m/%d/%Y').strftime('%Y-%m-%d')
                if '(' not in chamber:
                    chamber = ''
                    action = re.split('\s+', cells[2].get_text(), 1)[1]
                else:
                    chamber = re.sub('\\(|\\)', '', chamber)
                action = action.strip()
                bill_actions.append([bill_num, s_year, s_type, chamber, date, action, order])
        else:

            for row in hist_rows:
                cells = row.findAll('td')
                order = int(cells[0].get_text().strip())
                date = cells[1].get_text()
                date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')
                chamber = re.sub('\\(|\\)', '', cells[2].get_text()).strip()
                action = cells[4].get_text().strip()
                bill_actions.append([bill_num, s_year, s_type, chamber, date, action, order])

    #############
    ### OUTPUT
    ##############
    bill_details = [bill_num, s_year, s_type, author, coauthors, title, status, revenue_bill, vote_type, house_comm, senate_comm, summary, bill_url]

    return([bill_details, bill_actions])

########################################################
############## SCRAPE SESSION(S)
############################################
# bill_num, bill_url, descrip = session_urls[6751]
# s = sessions[0]


### *** LOOPING THROUGH INDIVIDUAL YEARS and Aggregating Regular + Special Sessions ***
for s in sessions:

    s_year, s_type, s_url, s_stem = s

    #### Output Lists
    session_bill_details = [['bill_number', 'session_year', 'session_type', 'author', 'coauthors', 'title', 'status', 'revenue_bill', 'vote_req', 'house_comm', 'senate_comm','summary', 'bill_url']]
    session_actions = [['bill_number', 'session_year', 'session_type', 'chamber', 'action_date', 'action', 'order']]

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

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[0], bill_row[3]))
        num += 1

    with open("MS_Bill_Details_" + str(s_year) + "_" + s_type + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("MS_Bill_Histories_" + str(s_year) + "_" + s_type  + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n -------------{} {} Session SCRAPED + DATA SAVED  -------------\n\n\n".format(s_year, s_type))


print("  ********************************** ALL DONE ********************************** ")
