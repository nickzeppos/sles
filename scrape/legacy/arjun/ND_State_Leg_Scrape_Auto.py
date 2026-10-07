# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape NORTH DAKOTA Legislation 1999 - PRESENT ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# All action occurs in the first few months of odd-years
# Interim Sessions seem mainly work-related -- No bills listed, sometimes drafts.
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
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options  # Import Options from the correct module
from selenium.common.exceptions import TimeoutException

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#####################################
##### Extract Session Information
#######################################

session_search = urllib.request.urlopen('https://www.legis.nd.gov/assembly', timeout = 30)
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.findAll('a',string = re.compile("\d.+ Leg.+Assembly"))
sessions = [[i.get_text().strip(), i['href']] for i in session_list]

for i in range(0, len(sessions)):
    pieces = sessions[i][0].split(' ')
    ga_num = pieces[0]
    yr_range = re.sub('\\(|\\)', '', pieces[3]).split('-')
    if len(yr_range[1]) == 2:
        yr_range[1] = yr_range[0][:2] + yr_range[1]
    yr_range = '_'.join(yr_range)
    sessions[i] = [ga_num, yr_range, sessions[i][1]]

### Drop Sessions Before 1997
sessions = [s for s in sessions if int(s[1][:4]) >= 1997]

### Drop Previously Scraped
sessions = [s for s in sessions if 'ND_Bill_Details_' + s[0] + '_' + s[1] + '.csv' not in os.listdir('.')]

### Drop Current Session
this_year = datetime.datetime.now().year
sessions = [s for s in sessions if int(s[1][:4]) < this_year and int(s[1][5:]) != this_year]
print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))

del this_year, session_search, session_search_soup, session_list, yr_range, ga_num

for sublist in sessions:
    sublist[2] = "https://www.ndlegis.gov" + sublist[2]


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):
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
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)


#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s = sessions[0]
# test = get_session_bills(sessions[0])

def get_session_bills(s):
    ga_num, s_yrs, s_url = s
    print('\n~~~~ Gathering Bill URLs for the {} Session ({}) ~~~~\n'.format(ga_num, s_yrs))
    session_bills = []
    s_url = s_url.replace('regular','')
    main_page = get_page_soup(s_url + 'bill-index.html?')
    bill_divs = main_page.find_all('div', class_='col bill')
    for bill_div in bill_divs:
        bill_name = bill_div.find('li', class_='bill-name').text.strip()
        bill_url = bill_div.find('a', class_='card-link')['href']
        s_type = bill_div.find('li', class_='session-type').text.strip()
        s_type = 'RS' if s_type == 'Regular' else 'SS'
        session_bills.append([bill_name,ga_num, s_yrs, s_type,s_url + bill_url])
    return(session_bills)
    #
    # ## URls Stems to List of Chamber Bills and Resolutions
    # chamber_stems = [ '/bill-text/house-bill.html', '/bill-text/senate-bill.html']
    # if main_page.find('a', href = re.compile('https://www.legis.nd.gov/assembly/[^/]+/special')):
    #     chamber_stems = chamber_stems + ['/special-session/bill-text/house-bill.html', '/special-session/bill-text/senate-bill.html']
    # bill_type_dict = {'1':'HB', '2':'SB', '3':'HCR', '4':'SCR', '5':'HR', '6':'SR', '7':'HMR', '8':'SMR'}
    #
    # ### Looping through lists of bills by type
    # for c_stem in chamber_stems:
    #
    #     if 'special' in c_stem:
    #         s_type = 'SS'
    #     else:
    #         s_type = 'RS'
    #
    #     s_req = requests.get(s_url + c_stem)
    #     print(s_req)
    #     s_soup = BeautifulSoup(s_req.content, 'lxml')
    #     a_tags = s_soup.findAll('a', href = re.compile('bill-actions'))
    #
    #     ### Unique Bill Urls
    #     if 'special' in c_stem:
    #         these_urls = [s_url + '/special-session' + re.sub('\\.\\.', '', a['href'])for a in a_tags]
    #     else:
    #         these_urls = [s_url + re.sub('\\.\\.', '', a['href'])for a in a_tags]
    #     these_urls = sorted(set(these_urls))
    #     these_nums = [re.sub('^.+actions/ba|.html|.htm', '', i.lower()) for i in these_urls]
    #     these_nums = [bill_type_dict[i[:1]] + i for i in these_nums]
    #
    #     these_urls = [[n, ga_num, s_yrs, s_type, u] for n, u in zip(these_nums, these_urls)]
    #     for item in these_urls:
    #         session_bills.append(item)
    #
    # print(session_bills)
    # exit()
    # print("collected {} bills".format(len(session_bills)))
    # return(session_bills)



#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = session_urls[0]

def get_bill_data(bill_row):

    ### Scrape Bill Page
    bill_num, ga_num, s_years, s_type, bill_url = bill_row

    ### Get HTML, Soup
    bill_soup = get_page_soup(bill_url)

    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')

    #### Table Changes Format in 2011 (More Defined)
    # if int(s_years[:4]) > 2011:
    #
    #     ### Construct Header
    #     main_content = bill_soup.find('div', id = 'application')
    #     header = [i.get_text() for i in main_content.findAll('p') if i.get_text() != 'Back to top' and 'HJ=' not in i.get_text()]
    #
    #     ## Get Action Table
    #     action_table = bill_soup.find('table', {'summary':re.compile('Number Breakdown')})
        # action_rows = action_table.findAll('tr')
        # action_rows = action_rows[1:]
    #
    # #### 2011_2012
    # elif int(s_years[:4]) == 2011:
    #     info_table = bill_soup.findAll('table',  {'summary':re.compile('Number Breakdown')})
    #     skip = 1
    #     header = []
    #     for row in info_table[0].findAll('tr'):
    #         if row.find('hr') is not None:
    #             break
    #         else:
    #             header.append(row.get_text().strip())
    #             skip += 1
    #
    #     action_rows = info_table[1].findAll('tr')
    #     action_rows = [i for i in action_rows if i.find('hr') is None]
    #
    # #### Pre-2011
    # else:
    #
    #     ## Get Table with Bill Data
    #     #info_table = bill_soup.find('a', href = re.compile('journal')).findParents('table')[0]
    #     info_table = bill_soup.find('table',  {'summary':re.compile('Number Breakdown')})
    #
    #     if info_table is None:
    #         return('No Data')
    #
    #     ### Identify/Extract header with sponsors and title + find where to split for action table
    #     skip = 1
    #     header = []
    #     for row in info_table.findAll('tr'):
    #         if row.find('hr') is not None:
    #             break
    #         else:
    #             header.append(row.get_text().strip())
    #             skip += 1
    #
    #     ### Split off Action table -- Drop Seperators
    #     action_rows = info_table.findAll('tr')[skip:]
    #     action_rows = [i for i in action_rows if i.find('hr') is None]

    ########################

    ### Parse Header
    # if len(header) == 1 and re.search('Introduced by|^Rep |^Sen', header[0]):
    #     title = ''
    #     sponsors = [header[0]]
    # else:
    #     for i in range(0, len(header)):
    #         if re.search('^A |^AN |^Relating |^Prefile [Ww]ithdrawn', header[i]):
    #             title_index = i
    #             break

        # title = ' '.join(header[title_index:])
        # sponsors = header[:title_index]

    title_header = bill_soup.find('h5', string='Title')
    title = title_header.find_next('p', class_='show-more').text.strip()

    sponsors_header = bill_soup.find('h5', string='Sponsors')
    sponsors = sponsors_header.find_next('p').text.strip()
    sponsors = re.sub('Introduced by ', '', sponsors)
    sponsors = re.sub(',',';',sponsors)
    primary_sponsor = re.sub(',.+|;.+', '', sponsors)

    action_soup = get_page_soup(re.sub('bill-overview/bo','bill-actions/ba', bill_url))
    action_table = action_soup.find('table', {'class': 'simple-table'})
    try:
        action_rows = action_table.findAll('tr')
        action_rows = action_rows[1:]
    except AttributeError:
        action_rows = []

    if re.search('Prefile [Ww]ithdrawn', title) and len(action_rows) == 0:
        bill_details = [bill_num, ga_num, s_years, primary_sponsor, sponsors, title, bill_url]
        bill_actions = [[bill_num, ga_num, s_years, bill_num[0:1], '', 'Prefile Withdrawn', '', 1]]
        return([bill_details, bill_actions])

    ### Action Scrape for Both Formats
    bill_actions = []
    order = 1
    for row in action_rows:
        cells = row.findAll(['th', 'td'])
        ## Skip if no date and no letters in action
        if cells[0].get_text().strip() == '' and not re.search('[A-Za-z]', cells[2].get_text()):
            continue
        #if cells[1].get_text().strip() == 'oth' or cells[1].get_text().strip() == 'stu': continue
        date = cells[0].get_text().strip()
        if date != '':
            date = date + '/' + s_years[:4]
            date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
        else:
            date = bill_actions[-1][4]
        chamber = cells[1].get_text().strip()
        action = cells[2].get_text().strip()
        if len(cells) == 3:
            journal_page = ''
        else:
            journal_page = cells[3].get_text().strip()

        bill_actions.append([bill_num, ga_num, s_years, s_type, chamber, date, action, journal_page, order])
        order += 1


    #############
    ### OUTPUT
    ##############
    bill_details = [bill_num, ga_num, s_years, s_type, primary_sponsor, sponsors, title, bill_url]

    return([bill_details, bill_actions])

########################################################
############## SCRAPE SESSION(S)
############################################
# bill_row = session_urls[847]
# s = sessions[-3]

for s in sessions:

    ga_num, s_years, s_url = s

    #### Output Lists
    session_bill_details = [['bill_number', 'ga_num', 'term', 'session', 'primary_sponsor', 'all_sponsors', 'title', 'bill_url']]
    session_actions = [['bill_number',  'ga_num', 'term', 'session', 'chamber', 'action_date', 'action', 'journal_page', 'order']]

    print("\n ------------------- Now Scraping the {} Legislative Assembly ({})---------------------- \n".format(ga_num, s_years))

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
        elif bill_data == "No Data":
            print(" ********** \n ({}/{}) -- {} -- NO DATA --- SKIPPING \n URL: {} \n **********".format(num, total, bill_row[0], bill_row[3]))
            num += 1
            continue

        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[0], bill_row[4]))
        num += 1

    with open("ND_Bill_Details_" + ga_num + "_" + s_years + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("ND_Bill_Histories_" + ga_num + "_" + s_years  + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} Session ({}) SCRAPED + DATA SAVED  -------------\n\n\n".format(ga_num, s_years))


print("  ********************************** ALL DONE ********************************** ")
