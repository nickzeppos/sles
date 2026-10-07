#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created Dec 3, 2018

Scrape IA Bills/Sponsors

@author: pb
"""

################## NOTES:
# SPONSORS != FLOOR MANAGERS --- From looking at bill headers, we want sponsors...
# ** Need to extract list of sponsors from bill to be able to get the lead sponsor..
#
# Study Bills -- http://www.theiowastatesman.com/1003/bill
# ** Used to explore an issue, can be proposed by gov; if passed by comm, re-introduced as legislative bill
# ** ---> THEN DO NOT OBSERVE COMMITTEE STAGE FOR STUDY BILL
####################


import csv
import os
import re
import time
import urllib
import urllib.request
from bs4 import BeautifulSoup
import datetime
import socket
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#################################
#### Get Sessions
#################################

## BillS search page
search_page = urllib.request.urlopen('https://www.legis.iowa.gov/legislation/BillBook', timeout = 5)
search_soup = BeautifulSoup(search_page, "lxml")

### All Sessions from 1995 onward
session_list = search_soup.find('select', {'name':'gaList'})
#session_nums = [i['value'] for i in session_list.findAll('option') if int(i['value']) >= 76]
#session_dates = [re.sub("^.+\\(|\\)", "", i.get_text())for i in session_list.findAll('option') if int(i['value']) >= 76]
sessions = [[i['value'], re.sub("^.+\\(|\\)", "", i.get_text())] for i in session_list.findAll('option') if int(i['value']) >= 90]

### Check Previously Scraped
sessions = [i for i in sessions if 'IA_Bill_Details_{}.csv'.format(i[0]) not in os.listdir('.')]

del search_page, search_soup, session_list

##################################
### Get Session-Bill List
##################################

def get_session_bills(s):
    s_num, s_dates = s
    print("\n ~~~ Searching for bills for Session {} ~~~ ".format(s_num))
    session_page = urllib.request.urlopen('https://www.legis.iowa.gov/legislation/BillBook?ga={}'.format(s_num), timeout = 5)
    session_soup = BeautifulSoup(session_page, "lxml")
    senate = session_soup.find('select', id = 'senateSelect')
    senate_bills = [i['value'] for i in senate.findAll('option')if i['value'] != '-1']
    house = session_soup.find('select', id = 'houseSelect')
    house_bills = [i['value'] for i in house.findAll('option')if i['value'] != '-1']
    print(" ~~~> Found {} Bills Total \n".format(len(house_bills) + len(senate_bills)))
    return(house_bills + senate_bills)


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):
    try:
        page = urllib.request.urlopen(bill_url, timeout = 120).read()
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
            page = urllib.request.urlopen(bill_url, timeout = 450).read()
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
    time.sleep(.75)
    page_soup = BeautifulSoup(page, parser)
    # Sometimes it doesn't get the page info; address this
    if "Bill" not in page_soup.get_text():
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(15)
            page = urllib.request.urlopen(bill_url, timeout = 450).read()
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
        time.sleep(.75)
        page_soup = BeautifulSoup(page, parser)
    return(page_soup)


####################################
#### Get Bill Details and History
####################################
# ** https://www.legis.iowa.gov/legislation/billTracking/billHistory?ga=83&billName=SR19
# -->  Sponsors included on top of bill descrip
#######################################
# s_num = '83'
# s_dates = 'zzzzz'
# bill_num = 'SR 19'
# this_url = 'https://www.legis.iowa.gov/legislation/billTracking/billHistory?ga=79&billName=HF765'

def get_bill_data(s, bill_num):

    s_num, s_dates = s
    b_num_adj = bill_num.replace(' ', '')
    this_url = 'https://www.legis.iowa.gov/legislation/billTracking/billHistory?ga={}&billName={}'.format(s_num, b_num_adj)
    bill_soup = get_page_soup(this_url)
    if bill_soup == "HTTP Error":
        return(['No Data', this_url])
    error_bar = bill_soup.find('div', {'class':'notificationBar error'})
    if error_bar:
        time.sleep(60)
        bill_soup = get_page_soup(this_url)
        if bill_soup.find('div', {'class':'notificationBar error'}):
            time.sleep(300)
            bill_soup = get_page_soup(this_url)
            if bill_soup.find('div', {'class':'notificationBar error'}):
                return(['No Data', this_url])

    ### In Newer Years the divideVerts actually seperate companions..
    ### By using the with_companions soup, will just grab first (main bill) data for older bill
    with_companions = bill_soup.find("div", {"class":"divideVert"})
    #main_bill_details = with_companions.findNext('div')
    if with_companions is None:
        return(['No Data', this_url])

    main_bill_history = with_companions.findNext('table')

    ## Check for No Data --- Using ONLY the main bill (not companions) for most bc sometimes companions will match "Data not available..."
    if not main_bill_history:
        if 'Data not available for the selected bill' in bill_soup.get_text().strip():
            return(['No Data', this_url])
    elif 'Data not available for the selected bill' in main_bill_history.get_text().strip():
        #### Skipping Re-try for Study Bills
        if re.search('^HSB|^SSB', b_num_adj):
            return(['No Data', this_url])
        ### Re-Try to make sure missingness not in error
        time.sleep(120)
        bill_soup = get_page_soup(this_url)
        with_companions = bill_soup.find("div", {"class":"divideVert"})
        main_bill_history = with_companions.findNext('table')
        if 'Data not available for the selected bill' in main_bill_history.get_text().strip():
            return(['No Data', this_url])

    ####### Get Floor Managers
    floor_managers = with_companions.find(string = re.compile('Floor Managers:'))
    if floor_managers is None:
        floor_managers = ''
    else:
        floor_managers = floor_managers.parent
        floor_managers = [fm.get_text() for fm in floor_managers.findAll('a')]
        floor_managers = '; '.join(floor_managers)

    ####### Get Floor Managers by Chamber --- DON"T HAVE THIS FOR 76th - 79th SESSIONS
    ## https://www.legis.iowa.gov/legislation/BillBook?ga=87&billName=HF+2502&billVersion=i&action=getBillRelatedInfo&bl=false
    S_floor_manager = ''
    H_floor_manager = ''
    if int(s_num) >= 80:
        billbook_url = 'https://www.legis.iowa.gov/legislation/BillBook?ga={}&billName={}&billVersion=i&action=getBillRelatedInfo&bl=false'.format(s_num, bill_num.replace(' ', '+'))
        billbook_soup = get_page_soup(billbook_url)
        fm_head_row = billbook_soup.find(string = re.compile("Floor Manager"))
        if fm_head_row:
            fm_row1 = fm_head_row.findParent('tr').findNextSibling('tr')
            fm_row2 = fm_row1.findNextSibling('tr')
            for row in [fm_row1, fm_row2]:
                if 'Senate:' in row.text:
                    S_floor_manager = row.a.text.strip()
                elif 'House:' in row.text:
                    H_floor_manager = row.a.text.strip()

    ###### Get Other Details
    lbd = with_companions.find('a', string = "Lobbyist Declarations")
    lobbyist_declarations = ''
    if lbd is not None:
        lobbyist_declarations = 'https://www.legis.iowa.gov' + lbd['href']

    sponsor_div = with_companions.find('div', {'class':'divideVert'})
    if sponsor_div is None:
        lead_sponsor = ''
        cosponsors = ''
        short_summary = with_companions.findNext('div').get_text().strip()
    else:
        short_summary = sponsor_div.findNext('div').get_text().strip()
        if sponsor_div.get_text() == '':
            lead_sponsor = ''
            cosponsors = ''
        else:
            all_sponsors = sponsor_div.get_text().split('By ', 1)[1].strip()
            lead_sponsor = all_sponsors.split(', ', 1)[0] #.split(' and ')[0]
            if len(all_sponsors.split(', ', 1)) > 1:
                cosponsors = all_sponsors.split(', ', 1)[1].replace(' and ', '; ').replace(', ', '; ')
            else:
                cosponsors = ''

    related_tags = with_companions.find(string = 'All Related Bills to Selected Bill')
    related_bills = []
    if related_tags is None:
        related_bills = ''
    else:
        related_tags = related_tags.parent.parent
        while True:
            related_tags = related_tags.findNext('div')
            if related_tags.findAll('a') != []:
                this_tag = related_tags.find('a')
                if '#' in this_tag['href']:
                    related_bills.append(related_tags.find('a').get_text())
                else:
                    break
            else:
                break
        related_bills = '; '.join(related_bills)

    companion_bill_tags = with_companions.find('span', {'title':'Companion Bills'})
    companion_bills = []
    if companion_bill_tags is None:
        companion_bills = ''
    else:
        while True:
            companion_bill_tags = companion_bill_tags.findNext()
            if companion_bill_tags.name == 'a':
                companion_bills.append(companion_bill_tags.get_text())
            else:
                break
        companion_bills = '; '.join(companion_bills)

    bill_history = []
    order = 1
    table_rows = main_bill_history.findAll('tr')
    if table_rows[1].get_text() == "Data not available for the selected bill.":
        pass
    else:
        for row in table_rows[1:]:
            cells = row.findAll('td')
            date = cells[0].get_text()
            if date == '':
                continue
            date = datetime.datetime.strptime(date, '%B %d, %Y').strftime('%Y-%m-%d')
            if len(cells) == 2:
                action = cells[1].get_text().strip()
            else:
                action = cells[2].get_text().strip()
            bill_history.append([bill_num, s_num, date, action, order])
            order += 1

    bill_details = [bill_num, s_num, s_dates, lead_sponsor, cosponsors, floor_managers, H_floor_manager, S_floor_manager, short_summary, related_bills, companion_bills, this_url, lobbyist_declarations]
    return([bill_details, bill_history])


#####################################
### Loop Through Sessions, Get Bill Details
######################################
# s = sessions[0]
# bill_num = session_bills[1800]

for s in sessions:

    s_num, s_dates = s

    ### Get List of Bills and Basic Details
    session_bills = get_session_bills(s)

    session_bill_details = [['bill_number', 'session', 'session_dates', 'primary_sponsor', 'cosponsors', 'floor_managers', 'H_floor_manager', 'S_floor_manager', 'summary', 'related_bills', 'companion_bills', 'bill_url', 'lobbying_url']]
    session_actions = [['bill_number', 'session', 'action_date', 'action', 'order']]

    print("\n ~~~> Scraping Individual Bills - Session {} \n".format(s_num))

    ### Scrape Each Bill
    num = 1
    total = len(session_bills)
    for bill_num in session_bills:

        ### Get Bill Data
        this_bill = get_bill_data(s, bill_num)
        time.sleep(1)

        if this_bill[0] == 'No Data':
            print("\n\n -- ({}/{}) ** NO DATA FOR {} ** \n URL: {}".format(num, total, bill_num, this_bill[1]))
            num +=1
            continue

        ### Append to Agg Files
        session_bill_details.append(this_bill[0])

        for action_row in this_bill[1]:
            session_actions.append(action_row)

        print(" -- ({}/{}) -- {} -- URL: {}".format(num, total, bill_num, this_bill[0][-2]))
        num += 1

    ### SAVE!
    with open("IA_Bill_Details_" + s_num + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("IA_Bill_Histories_" + s_num + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n *********** {} DONE! ************** \n\n".format(s_num))


print(" ~~~~~~~~~~~~~ ************** ALL SESSIONS DONE *************** ~~~~~~~~~~~~~~~~ ")
