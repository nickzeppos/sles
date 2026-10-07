# -*- coding: utf-8 -*-
"""
Created on Nov 19, 2019

Scrape KANSAS Bills

@author: PB
"""


###########################
##### NOTES:
# ~~~ Call API via:
# -- Current Session: http://kslegislature.org/li/api/v11/rev-1/bill_status/sb1/
# -- 2018: http://kslegislature.org/li_2018/api/v11/rev-1/bill_status/sb1/
# -- 2016: http://kslegislature.org/li_2016/api/v11/rev-1/bill_status/sb1/
# -- Doesn't seem to work for 2016s or earlier
# -- Documentation: http://www.kslegislature.org/klois/includes/kliss_restian_interface_guide_v11.pdf
# ~~~
###########################

import csv
import os
from bs4 import BeautifulSoup
import time
import re
import requests
import urllib
import socket
import datetime
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options  # Import Options from the correct module
import datetime
from selenium.common.exceptions import TimeoutException

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#######################
##### Extract Session Links
########################

session_req = requests.get('http://kslegislature.org/li/historical/', timeout = 30)
session_soup = BeautifulSoup(session_req.content, 'lxml')

### Get Sessions
session_list = session_soup.find("a", href = re.compile('li\\/historical')).findNextSibling('ul')
sessions = [[i.text.strip(), 'http://kslegislature.org' + i['href']] for i in session_list.findAll('a')]
sessions = [i for i in sessions if i[0] not in ['1997 - 2010 Sessions', '2000 - 2010 Committee Data']]

for i in range(len(sessions)):
    s_name = re.sub(' Special Session', '_SS', sessions[i][0])
    s_name = re.sub(" Regular Sessions", "_RS", s_name)
    s_name = re.sub('-', '_', s_name)
    sessions[i] = [s_name] + sessions[i]

### Drop Previously Scraped
sessions = [s for s in sessions if 'KS_Bill_Details_{}.csv'.format(s[0]) not in os.listdir('.')]

### Drop Current Session
this_year = datetime.datetime.now().year
sessions = [s for s in sessions if int(s[0][:4]) < this_year]
print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))

del session_req, session_soup, session_list, s_name, i, this_year

##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):
    try:
        page = urllib.request.urlopen(bill_url, timeout = 40).read()
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


#######################################################
########## GET BILL DATA FOR A SESSION
#####################################################
# s = sessions[0]

def get_session_bills(s):

    s_id, s_name, s_url = s
    session_bills = []

    print('\n\n\t~~~~ Gathering Bill URLs for the {} ~~~~\n'.format(s_name))
    ### Different url parts for specials and regular sessions (shorter s_ids == specials)
    if len(s_id) == 7:
        yr = int(s_id[:4])
        if yr % 2 == 0:
            url_b = 'b' + str(yr - 1) + "_" + str(yr)[2:]
        else:
            url_b = 'b' + str(yr) + "_" + str(yr + 1)[2:]
    else:
        url_b = 'b' + re.sub('_20', '_', re.sub('_RS|_SS', '', s_id))

    # if s[0] in ["2020_SS", "2021_SS"]:
    #     url_b = "b" + s[0][0:4] + "s" # did it differently this time

    bill_url = s_url + url_b + '/measures/bills/'
    res_url = s_url + url_b + '/measures/resos/'
    conres_url = s_url + url_b + '/measures/concurs/'
    eros_url = s_url + url_b + '/measures/eros/' # Executive Reorginazation Orders

    ### Fix Resolution URL for 2011_2012 -- divided by year for some reason BUT data is mostly identical (but more for year2)
    if s_id == '2011_2012_RS':
        res_url1 = re.sub('measures/resos/', 'year1/measures/resos/', res_url)
        res_url2 = re.sub('measures/resos/', 'year2/measures/resos/', res_url)
        all_urls = [bill_url, res_url1, res_url2, conres_url, eros_url]
    else:
        all_urls = [bill_url, res_url, conres_url, eros_url]

    ### Loop through Pages with Different Types of Bills
    for url in all_urls:
        if s_id == "2019_2020_RS":
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url)
            # element_id = "module-list bill-tab-content tab-content"
            # element = WebDriverWait(driver, 30).until(
            #     EC.presence_of_element_located((By.ID, element_id))
            # )

            # Once the element appears, you can retrieve its HTML content
            page_html = driver.page_source

            # Parse the HTML using BeautifulSoup
            url_soup = BeautifulSoup(page_html, "html.parser")
        else:
            url_soup = get_page_soup(url)
        # print(url)
        bill_list_tabs = url_soup.findAll('ul', {'class':'module-list bill-tab-content tab-content'})
        ### Loop through each tab (e.g., SB1 - SB10) on each page
        for tab in bill_list_tabs:
            ### Loop through each bill within each tab
            for bill in tab.findAll('a'):
                bn, title = bill.text.strip().split(' - ', 1)
                bn = re.sub('.+ ', '', bn)
                if not re.search('[A-Z]', bn):
                    bn = re.sub('.+measures/|/$', '', bill['href']).upper()
                title = title.strip()
                session_bills.append([bn, s_id, title, 'http://kslegislature.org' + bill['href']])
        ### Different process for EROs Orders:
        if bill_list_tabs == [] and 'eros' in url:
            bill_list_tabs = url_soup.findAll('ul', {'class':'module-list'})
            for tab in bill_list_tabs:
                ### Loop through each order within each tab
                for order in tab.findAll('a'):
                    bn, title = order.text.strip().split(', ', 1)
                    bn = re.sub('Executive Reorganization Order No\\. ', 'ERO', bn)
                    title = title.strip()
                    session_bills.append([bn, s_id, title, 'http://kslegislature.org' + order['href']])

    ### Return List of Bills
    print('\n\n **** ~~~> Found Data for {} Bills TOTAL for the {} **** \n'.format(len(session_bills), s_name))
    return(session_bills)


#######################################################
########## Get and Parse Individual Bill Data via API (2009+)
#####################################################
# s = sessions[7]
# bill_info = session_bills[524]

def get_bill_data(bill_info):

    ### 2008 and earlier won't have s_yr or act_num
    bill_num, s_id, title, bill_url = bill_info

    ### Get Bill Page
    bill_soup = get_page_soup(bill_url)

    #################
    #### Basic Info
    #################

    bill_num = re.sub('[0-9].+|[0-9]+', '', bill_num) + re.sub('^[A-Z]+', '', bill_num).zfill(4)

    ### Bill text Versions
    text_row_items = bill_soup.findAll('tbody', id = re.compile('version-tab-'))
    if text_row_items != []:
        version_status = []
        for tab in text_row_items:
            version_status = version_status + [i.td.text.strip() for i in tab.findAll('tr')]
        version_status = '; '.join(version_status)
    else:
        version_status = ''

    ################
    #### SPONSORS
    ###############

    ### CUrrent Sponsor
    current_sponsor = bill_soup.find('ul', id = 'sponsor-tab-1')
    if current_sponsor:
        current_sponsor = '; '.join([i.text.strip() for i in current_sponsor.findAll('li')])
    else:
        current_sponsor = ''

    ### Introduced By
    orig_sponsor = bill_soup.find('ul', id = 'introduce-tab-1')
    if orig_sponsor:
        orig_sponsor = '; '.join([i.text.strip() for i in orig_sponsor.findAll('li')])
    else:
        orig_sponsor = ''

    ### Requested By
    requested_by = bill_soup.find(text = re.compile('Requested for introduction'))
    if requested_by:
        requested_by = requested_by.findParent('div').findNextSibling('div')
        requested_by = '; '.join([i.text.strip() for i in requested_by.findAll('div')])
    else:
        requested_by = ''

    if re.search('^ERO', bill_num):
        current_sponsor = bill_soup.find('h3', text = re.compile('Sponsor')).nextSibling.strip()
        orig_sponsor = current_sponsor

    ################
    #### ACTIONS
    ###############

    bill_actions = []
    #action_table = bill_soup.find('span', {'class':'bill_history_legend'})
    action_table = bill_soup.find('th', text = "Chamber")
    if action_table:
        action_rows = action_table.findParent('table').findAll('tr')
        order = len(action_rows[1:])
        for row in action_rows[1:]:
            cells = row.findAll('td')
            date = cells[0].text.strip()
            if date != '' and re.search(',', date):
                date = re.sub('[A-Z][a-z]+, ', '', date)
                date = datetime.datetime.strptime(date, '%b %d, %Y').strftime('%Y-%m-%d')
            else:
                date = re.sub('^[A-Z][a-z]+ ', '', date)
                date = datetime.datetime.strptime(date, '%d %b %Y').strftime('%Y-%m-%d')
            chamber = cells[1].text.strip()
            action = re.sub('  +', ' ', re.sub('\n', '', cells[2].text.strip())).strip()
            journal_page = cells[3].text.strip()
            bill_actions.append([bill_num, s_id, chamber, date, action, order, journal_page])
            order = order - 1

    ### Combine Items
    bill_details = [bill_num, s_id, version_status, orig_sponsor, current_sponsor, requested_by, title, bill_url]
    return([bill_details, bill_actions])



########################################################
############## SCRAPE SESSION(S)
############################################
# s = sessions[2]
# bill_info = session_bills[4]

for s in sessions:

    #### Output Lists
    session_bill_details = [['bill_id', 'session', 'version_status', 'original_sponsor', 'current_sponsor', 'requested_by', 'title', 'bill_url']]
    session_actions = [['bill_id', 'session', 'chamber', 'action_date', 'action', 'order', 'journal_page']]

    print("\n ------------------- Now Scraping: {} ---------------------- \n".format(s[0]))

    ### Get all bills for a specific session
    session_bills = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        ### Skip bills with no webpages
        if s[0] + '-' + bill_info[0] in ['2013_2014_RS-HB2070', '2011_2012_RS-HB2210']:
            print(" ********** \n ({}/{}) -- MANUALLY ID'ED BILL TO SKIP -- {} \n **********".format(num, total, bill_info[-1]))
            num += 1
            continue

        ### Get Basic Details
        bill_data = get_bill_data(bill_info)
        time.sleep(3)

        ### Check if error
        if bill_data == 'HTTP Error':
            print(" ********** \n ({}/{}) -- HTTP ERROR -- SKIPPING -- {} \n **********".format(num, total, bill_info[-1]))
            num += 1
            continue

        ### Get Actions
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_info[0], bill_info[-1]))
        num += 1

    with open("KS_Bill_Details_{}.csv".format(s[0]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("KS_Bill_Histories_{}.csv".format(s[0]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} SCRAPED + DATA SAVED  -------------\n\n\n".format(s[0]))


print("  ********************************** ALL DONE ********************************** ")
