# -*- coding: utf-8 -*-
"""
Created on Mon Feb 11 10:21:01 2019

~~~~~~~ Scrape Washington Legislation 1991 - PRESENT ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# Special Sessions are folded into bill histories -- so if reintroduced, keeps bill number, and action is recorded there
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
import socket
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#####################################
##### Extract Session Information
#######################################

session_search = urllib.request.urlopen('https://app.leg.wa.gov/billsummary', timeout = 30)
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.find('select', id = 'Year')
sessions = [[i['value'], i.get_text()] for i in session_list.findAll('option')]

### Drop Previously Scraped
sessions = [s for s in sessions if 'WA_Bill_Details_' + s[1].replace('-', '_') + '.csv' not in os.listdir('.')]

### Drop Current Session
next_year = datetime.datetime.now().year + 1
next_year = 2025
sessions = [s for s in sessions if int(s[0]) < next_year]
print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(next_year))

### Drop Sessions Prior to 2023
sessions = [s for s in sessions if int(s[0]) >= 2023]

del next_year, session_search, session_search_soup, session_list

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s = sessions[10]
# test = get_session_bills(2018)

def get_session_bills(s):
    s_id_start, s_yrs = s
    print('\n\n\t~~~~ Gathering Bill URLs for the {} Session ~~~~\n'.format(s_yrs))
    session_bills = []
    for s_id in [s_id_start, str(int(s_id_start)+1)]:
        get_leg_url = 'http://wslwebservices.leg.wa.gov/legislationservice.asmx/GetLegislationByYear?year={}'
        list_req = requests.get(get_leg_url.format(s_id))
        list_soup = BeautifulSoup(list_req.content, 'lxml')
        for bill in list_soup.findAll('legislationinfo'):
            if int(bill.substituteversion.text) > 0: # directs to the same page as the main version
                continue
            bill_num = bill.billid.text
            biennium = bill.biennium.text
            bill_type = bill.longlegislationtype.text
            agency = bill.originalagency.text
            item = [bill_num, s_yrs, biennium, bill_type, agency]
            if item not in session_bills:
                session_bills.append([bill_num, s_yrs, biennium, bill_type, agency])
    return(session_bills)


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

#def get_page_soup(bill_url, parser = 'lxml'):
#
#    ### Get HTML
#    try:
#        page = urllib.request.urlopen(bill_url, timeout = 20)
#    except urllib.error.HTTPError: # as e
#        return('HTTP Error')
#    except:
#        try:
#            print("\n ~~> Retrying Bill Request")
#            time.sleep(15)
#            page = urllib.request.urlopen(bill_url, timeout = 30)
#        except urllib.error.HTTPError: # as e
#            return('HTTP Error')
#        except:
#            print("\n ~~> Retrying Bill Request x 2")
#            time.sleep(60)
#            page = urllib.request.urlopen(bill_url, timeout = 60)
#    time.sleep(.5)
#
#    ### Return Soup
#    page_soup = BeautifulSoup(page, parser)
#    return(page_soup)

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


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = test[100]
# bill_row = session_bills[0]

def get_bill_data(bill_row):
    bill_num, s_yrs, biennium, bill_type, start_chamber = bill_row
    num_only = bill_num.split(' ')[1]
    bill_url = 'http://wslwebservices.leg.wa.gov/legislationservice.asmx/GetLegislation?biennium={}&billNumber={}'.format(biennium, num_only)
    bill_soup = get_page_soup(bill_url)
    bill_num_z = bill_num.replace(' ', '')
    main_bill = bill_soup.legislation
    if bill_soup == 'HTTP Error':
        return('HTTP Error')
    title = main_bill.legaltitle.text
    summary = main_bill.longdescription.text
    appropriations = main_bill.appropriations.text
    intro_date = main_bill.introduceddate.text.split('T')[0]
    veto = main_bill.veto.text
    partial_veto = main_bill.partialveto.text
    companion = '; '.join([i.billid.text.replace(' ', '') for i in bill_soup.findAll('companion')])
    sponsor_url = 'http://wslwebservices.leg.wa.gov/legislationservice.asmx/GetSponsors?biennium={}&billId={}'.format(biennium, bill_num.replace(' ', '%20'))
    sponsor_soup = get_page_soup(sponsor_url)
    primary_sponsors = '; '.join(['{}, {}'.format(i.lastname.text, i.firstname.text) for i in sponsor_soup.findAll('sponsor') if i.type.text == 'Primary'])
    cosponsors = '; '.join(['{}, {}'.format(i.lastname.text, i.firstname.text)for i in sponsor_soup.findAll('sponsor') if i.type.text == 'Secondary'])
    if bill_type == 'Initiative':
        initiative = 'true'
    else:
        initiative = 'false'
    action_url = 'https://app.leg.wa.gov/billsummary?BillNumber={}&Initiative={}&Year={}'.format(bill_num.split()[1] , initiative, biennium[0:4])
    action_soup = get_page_soup(action_url)
    history_table = action_soup.find('div', {'class':'historytable'}).parent
    txt_breaks = [i.text.strip() for i in history_table.findAll('p')]
    # Remove reference to originating chamber in txt_breaks so that history_table and txt_breaks have same length
    # NOTE: this will need to be updated with appropriate years in future iterations
    if '2023 REGULAR SESSION' in txt_breaks:
        txt_breaks.pop(txt_breaks.index('2023 REGULAR SESSION') + 1)
    if '2024 REGULAR SESSION' in txt_breaks:
        txt_breaks.pop(txt_breaks.index('2024 REGULAR SESSION') + 1)
    if '2023 1ST SPECIAL SESSION' in txt_breaks:
        txt_breaks.pop(txt_breaks.index('2023 1ST SPECIAL SESSION') + 1)
    hist_sections = action_soup.findAll('div', {'class':'historytable'})
    if len(hist_sections) != len(txt_breaks):
        print('Length of bill history table does not match length of list of chamber identifiers!')
    yr = ''
    base_chamber = bill_num[0:1]
    order = 1
    bill_actions = []
    for section, txt_br in zip(hist_sections, txt_breaks):
        if 'session' in txt_br.lower():
            # Note that bills prefiled in December will get labelled with the session year (i.e., the following year)
            # rather than the correct calendar year
            yr = txt_br[0:4]
            chamber = base_chamber
        else:
            chamber = re.sub('IN THE ', '', txt_br)[0:1]
        date = ''
        for row in section.findAll('div', recursive = False):
            ## Accounting for multiple actions on one day
            if row.div is not None and row.div.text.strip() != '':
                month_day = row.div.text.strip()
                date = month_day + ', ' + yr
                date = datetime.datetime.strptime(date, '%b %d, %Y').strftime('%Y-%m-%d')
            action = re.sub('\n|\r|\([^)]*\)', '', row.text)
            action = re.sub(month_day, '', action).strip()
            bill_actions.append([bill_num_z, s_yrs, date, action, chamber, order])
            order += 1
    bill_details = [bill_num_z, s_yrs, bill_type, intro_date, primary_sponsors, cosponsors, title, summary, appropriations, veto, partial_veto, companion, bill_url]
    return([bill_details, bill_actions])

########################################################
############## SCRAPE SESSION(S)
############################################
# bill_row = session_urls[10]
# s = sessions[-1]
#
# word_freq = Counter()
#
# types = [i[0] for i in session_urls]
# # Iterate over each sublist
# for i in types:
#     word = i.split()[0]  # Split by space and get the first part (the word)
#     word_freq[word] += 1  # Update the frequency
#
# # Output the word frequencies
# for word, freq in word_freq.items():
#     print(f"{word}: {freq}")

for s in sessions:
    session_bill_details = [['bill_number', 'term', 'bill_type', 'intro_date', 'primary_sponsor', 'cosponsors', 'title', 'summary', 'approp_bill', 'veto', 'partial_veto', 'companion', 'bill_url']]
    session_actions = [['bill_number', 'term', 'action_date', 'action', 'chamber', 'order']]
    print("\n ------------------- Now Scraping the {} TERM ---------------------- \n".format(s[1]))
    session_urls = get_session_bills(s)
    num = 1
    total = len(session_urls)
    for bill_row in session_urls:
        bill_data = get_bill_data(bill_row)
        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n **********".format(num, total, bill_row[0]))
            num += 1
            continue
        session_bill_details.append(bill_data[0])
        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
        print(" ({}/{}) -- {}".format(num, total, bill_row[0]))
        num += 1
    with open("WA_Bill_Details_" + s[1].replace('-', '_') + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
    with open("WA_Bill_Histories_" + s[1].replace('-', '_')+ ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
    print("\n\n\n ------------- {} TERM SCRAPED + DATA SAVED  -------------\n\n\n".format(s[1]))


print("  ********************************** ALL DONE ********************************** ")
