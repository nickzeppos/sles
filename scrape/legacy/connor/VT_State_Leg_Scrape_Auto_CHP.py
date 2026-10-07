# -*- coding: utf-8 -*-
"""
Created on Nov 19, 2019

Scrape VERMONT Bills

@author: PB
"""


###########################
##### NOTES:
#
# API for future use: https://legislature.vermont.gov/docs/api/v1/index.html
# ---> See documentaiton, goes back to 2009-2010 Session
# ---> Ex + API KEY = https://legislature.vermont.gov/api/v1/bill/list?Biennium=2014&Body=All&Status=All
#
# API does not appear to record sponsors for Resolutions
# --- But labeled as 'offered by' on bills
#
# NO RESOLUTIONS for 1985_1986; present for all subsequent sessions
###########################


import csv
import os
from bs4 import BeautifulSoup
import re
import time
import urllib
import urllib.request
from urllib.request import Request, urlopen
import datetime
from pathlib import Path
import http.cookiejar
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options  # Import Options from the correct module
import datetime
from selenium.common.exceptions import TimeoutException
import json
import socket

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#######################
##### Extract Session Links
########################

chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(options=chrome_options)

# Navigate to the page
driver.get('https://legislature.vermont.gov/bill/search/2024')

dropdown_locator = (By.ID, 'Form_SelectSession_selected_session')
dropdown_element = WebDriverWait(driver, 90).until(EC.presence_of_element_located(dropdown_locator))

session_soup = BeautifulSoup(driver.page_source, 'html.parser')
dropdown_element = session_soup.find('select', {'id': 'Form_SelectSession_selected_session'})
dropdown_options = dropdown_element.find_all('option')
sessions = [[option.text.strip(),'https://legislature.vermont.gov/bill/search/'+option['value'].strip()+'?'] for option in dropdown_options]

for i in range(len(sessions)):
    s_name = re.sub(' Special Session', '_SS', sessions[i][0])
    s_name = re.sub(" Session", "_RS", s_name)
    s_name = re.sub('-', '_', s_name)
    sessions[i][0] = s_name

### Drop Previously Scraped
sessions = [s for s in sessions if 'VT_Bill_Details_{}.csv'.format(s[0]) not in os.listdir('.')]

### Drop Pre-2022 sessions
sessions = [s for s in sessions if int(s[0][:4]) > 2022]
print("\n\n\t ~~~~ DROPPING PRE-2022 SESSIONS ~~~~ \n\n")

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



##############################################################
###### Functions to Request and Clean Data in JSON format from API
################################################################

def get_json(json_url):
    try:
        json_req = requests.get(json_url)
    except socket.timeout:
        print("\n ~~> SOCKET TIMEOUT --- Retrying Bill Request")
        time.sleep(120)
        return(get_json(json_url))
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(15)
            json_req = requests.get(json_url)
        except socket.timeout:
            print("\n ~~> SOCKET TIMEOUT --- Retrying Bill Request")
            time.sleep(120)
            return(get_json(json_url))
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            json_req = requests.get(json_url)
    time.sleep(1)
    json_out = json.loads(json_req.text)['data']
    return(json_out)


#######################################################
########## GET BILL DATA FOR A SESSION
#####################################################
# s = sessions[0]

##### *** Non-Public API Url Pulled from OpenStates Code ***
def get_session_bills(s):

    s_id, s_url = s
    session_bills = []

    print('\n\n\t~~~~ Gathering Bill URLs for the {} Session ~~~~\n'.format(s_id))

    #### Use API for Newer Sessions --- 2009+
    if(int(s_id[:4]) >= 2009):
        s_url = re.sub('http:', 'https:', s_url)
        s_url = s_url[:-1]
        bill_url = re.sub('search', 'loadBillsIntroduced', s_url)
        res_url = re.sub('search', 'loadAllResolutionsByChamber', s_url)
        #o_url = re.sub('search', 'loadBillsReleased', s_url)

        bill_req = requests.get(bill_url)
        bill_json = json.loads(bill_req.text)['data']

        res_req = requests.get(res_url)
        res_json = json.loads(res_req.text)['data']

        all_json = bill_json + res_json
        for j in all_json:
            act_num = ''
            if 'ActNo' in j.keys():
                if j['ActNo'] not in ['Act', '']:
                    act_num = j['ActNo']
            b_url = re.sub('search', 'status', s_url) + '/' + j['BillNumber']
            session_bills.append([j['BillNumber'], s_id, j['Title'], act_num, b_url])

    else:
        id_stem = re.sub('.+Session=', '', s_url)

        ### Loop Through Chambers, Then Bills, Resolutions
        for c in ['H', 'S']:
            for bt in ['bills', 'resolutn']:
                this_url = 'http://www.leg.state.vt.us/docs/{}.cfm?Body={}&Session={}'.format(bt, c, id_stem)
                this_soup = get_page_soup(this_url)
                these_bills = this_soup.findAll('dt')
                for bill in these_bills:
                    b_id = bill.b.text.strip()
                    b_url = 'http://www.leg.state.vt.us/database/status/summary.cfm?Bill={}&Session={}'.format(b_id, id_stem)
                    session_bills.append([b_id, s_id, bill.b.nextSibling.strip(), '', b_url])

    #### Fix Missing Bill Number + Wrong URL for 2012
    if s_id == '2011_2012_RS':
        for i in range(len(session_bills)):
            if session_bills[i][-1] == 'https://legislature.vermont.gov/bill/status/2012/H.C.R.' and session_bills[i][3] == 'R-529':
                session_bills[i][0] = 'H.C.R.3000'
                session_bills[i][-1] = 'https://legislature.vermont.gov/bill/status/2012/H.C.R.3000'
    elif s_id == '1989_1990_RS':
        for i in range(len(session_bills)):
            if session_bills[i][0] == 'JRH90A':
                session_bills[i][0] = 'JRH90'
                session_bills[i][-1] = 'http://www.leg.state.vt.us/database/status/summary.cfm?Bill=JRH90&Session=1990'

    ### Return List of Bills
    print('\n\n **** ~~~> Found Data for {} Bills TOTAL for the {} **** \n'.format(len(session_bills), s_id))
    return(session_bills)



#######################################################
########## Get and Parse Individual Bill Data via API (2009+)
#####################################################
# s = sessions[7]
# bill_info = session_bills[100]

def get_bill_data(bill_info):

    ### 2008 and earlier won't have s_yr or act_num
    bill_num, s_id, title, act_num, bill_url = bill_info

    ### Get Bill Page
    bill_soup = get_page_soup(bill_url)

    #################
    #### Basic Info
    #################

    bill_num = re.sub('\\.', '', bill_num)
    bill_num = re.sub('[0-9].+|[0-9]+', '', bill_num) + re.sub('^[A-Z]+', '', bill_num).zfill(4)

    text_row_items = bill_soup.find('ul', {'class':'bill-path'})
    if text_row_items:
        version_status = '; '.join([i.text.strip() for i in text_row_items.findAll('a')])
    else:
        version_status = ''

    statutes = bill_soup.find('table', id = 'bill-related-statutes')
    if statutes:
        statutes = '; '.join([i.text.strip() for i in statutes.findAll('td')])
    else:
        statutes = ''

    ### Act_num
    bn_row = bill_soup.find('div', {'class':'bill-title'}).h1
    if bn_row.find('span'):
        act_num = bn_row.find('span').text.strip()

    ################
    #### SPONSORS
    ###############

    spon_table = bill_soup.find('dt', string = re.compile('Sponsor\\(s\\)'))
    if spon_table:
        spon_table = spon_table.findNextSibling('dd').ul
        primary_sponsor = re.sub('  +', ' ', spon_table.li.text  )
        cosponsors = []
        for s in spon_table.findAll('li'):
            spon = re.sub('  +', ' ', s.text)
            if spon not in ['Additional Sponsors', primary_sponsor, 'Less…']:
                cosponsors.append(spon)
        cosponsors = '; '.join(cosponsors)
    else:
        primary_sponsor = ''
        cosponsors = ''


    ################
    #### ACTIONS
    ###############
    # *** Using the API to make sure I get ALL of the actions (if > 50)

    ### Adapted from Open States
    try:
        driver.get(bill_url)
        select_locator = (By.NAME, "bill-detailed-status-table_length")
        select_element = WebDriverWait(driver, 90).until(EC.presence_of_element_located(select_locator))
        driver.execute_script("arguments[0].value = '-1';", select_element)
        driver.execute_script("arguments[0].dispatchEvent(new Event('change'))", select_element)
        time.sleep(0.5)

        table_locator = (By.ID, "bill-detailed-status-table")
        table_element = WebDriverWait(driver, 90).until(EC.presence_of_element_located(table_locator))
        time.sleep(0.5)
        bill_actions = []
        rows = table_element.find_elements(By.XPATH, ".//tbody/tr")
        order = len(rows)
        for row in rows:
            columns = row.find_elements(By.TAG_NAME, "td")
            chamber, date, journal_page, calendar, status, action = [column.text for column in columns]
            date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            action = re.sub('\\<[^\\>]+\\>', '', action)
            bill_actions.append([bill_num, s_id, chamber, date, status, action, order, journal_page])
            order -= 1
    except ValueError:
        print("trying again")
        driver.get(bill_url)
        select_locator = (By.NAME, "bill-detailed-status-table_length")
        select_element = WebDriverWait(driver, 90).until(EC.presence_of_element_located(select_locator))
        driver.execute_script("arguments[0].value = '-1';", select_element)
        driver.execute_script("arguments[0].dispatchEvent(new Event('change'))", select_element)
        time.sleep(5)

        table_locator = (By.ID, "bill-detailed-status-table")
        table_element = WebDriverWait(driver, 90).until(EC.presence_of_element_located(table_locator))
        time.sleep(5)
        bill_actions = []
        rows = table_element.find_elements(By.XPATH, ".//tbody/tr")
        order = len(rows)
        for row in rows:
            columns = row.find_elements(By.TAG_NAME, "td")
            chamber, date, journal_page, calendar, status, action = [column.text for column in columns]
            date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            action = re.sub('\\<[^\\>]+\\>', '', action)
            bill_actions.append([bill_num, s_id, chamber, date, status, action, order, journal_page])
            order -= 1


    ### Combine Items
    bill_details = [bill_num, s_id, act_num, version_status, primary_sponsor, cosponsors, title, statutes, bill_url]
    return([bill_details, bill_actions])


#######################################################
########## Get and Parse Older (Pre-2009) Individual Bill Data via BIll Pages
#####################################################
# s = sessions[7]
# bill_info = session_bills[100]

vote_types = {'P':'Passed', 'F':'Failed'}

def get_pre2009_bill_data(bill_info):

    ### 2008 and earlier won't have s_yr or act_num
    bill_num, s_id, title, act_num, bill_url = bill_info

    ### Get Bill Page
    bill_soup = get_page_soup(bill_url)

    #################
    #### Basic Info
    #################

    bill_num = re.sub('\\.', '', bill_num)
    bill_num = re.sub('[0-9].+|[0-9]+', '', bill_num) + re.sub('^[A-Z]+', '', bill_num).zfill(4)

    text_row_items = bill_soup.findAll('a', href = re.compile('docs/legdoc.cfm'))
    version_status = '; '.join([i.text.strip() for i in text_row_items if i.text.strip() != 'Act Summary'])

    statutes = ''

    ### Act_num
    bn_row = bill_soup.find('td', string = re.compile('Act No:'))
    if bn_row:
        act_num = re.sub('\n', ' ', bn_row.parent.text).strip()

    ################
    #### SPONSORS
    ###############
    # Not always alphabetized so should be OK: http://www.leg.state.vt.us/database/status/summary.cfm?Bill=H.0188&Session=2006

    ### Go to "More Sponsors" Page if Needed
    more_sponsors_tag = bill_soup.find('a', href = re.compile('sponsors.cfm'))
    if more_sponsors_tag:
        sponsor_soup = get_page_soup('http://www.leg.state.vt.us/database/status/' + more_sponsors_tag['href'])
        spon_table = sponsor_soup.find('h3', string = re.compile("Spon")).findNext('table')
        all_sponsors = [i.text.strip() for i in spon_table.findAll('td')]
        primary_sponsor = all_sponsors[0].strip()
        cosponsors = '; '.join([i.strip() for i in all_sponsors[1:]])
    else:
        spon_table = bill_soup.find('td', string = re.compile('Sponsor\\(s\\):'))
        if spon_table:
            all_sponsors = [i.text.strip() for i in spon_table.findNextSibling('td').findAll('b')]
            if all_sponsors == []:
                primary_sponsor = ''
                cosponsors = ''
            else:
                primary_sponsor = all_sponsors[0].strip()
                if(len(all_sponsors) == 1):
                    cosponsors = ''
                else:
                    cosponsors = [i.strip() for i in all_sponsors[1:]]
        else:
            primary_sponsor = ''
            cosponsors = ''

    ################
    #### ACTIONS
    ###############
    # *** NEED TO MANUALLY CONSTRUCT FROM DATES PROVIDED ****

    bill_actions = []
    if bill_num[0] == "H":
        chamber_order = ['House', 'Senate']
    else:
        chamber_order = ['Senate', 'House']

    ### Loop through Chamber Action Sections
    for c in chamber_order:
        c_status = bill_soup.find('h3', text = "{} Status:".format(c)).findNextSibling('blockquote').table
        if c_status:
            ### Current Status if VETO (Need for Overrides)
            current = c_status.find('td', string = re.compile('Current Status')).findNextSibling('td').b.text.strip()
            if 'veto' in current.lower():
                current_date = c_status.find('td', string = re.compile('Status Date:')).findNextSibling('td').b.text.strip()
                current_date = datetime.datetime.strptime(current_date, '%m/%d/%Y').strftime('%Y-%m-%d')
                bill_actions.append([bill_num, s_id, c, current_date, '', current, '', ''])
            ### Introduction/Readings
            read1st = c_status.find('td', string = re.compile('1st Reading:'))#.findNextSibling('td').b.text.strip()
            read2nd = c_status.find('td', string = re.compile('2nd Reading:'))#.findNextSibling('td').b.text.strip()
            read3rd = c_status.find('td', string = re.compile('3rd Reading:'))#.findNextSibling('td').b.text.strip()
            for read_row in [[read1st, '1st'], [read2nd, '2nd'], [read3rd, '3rd']]:
                read_date = read_row[0].findNextSibling('td').b.text.strip()
                ### Skip items without date
                if read_date == '':
                    continue
                read_date = datetime.datetime.strptime(read_date, '%m/%d/%Y').strftime('%Y-%m-%d')
                read_action = read_row[0].findNextSibling('td').findNextSibling('td').text.strip()
                ### Add readings with and without actions with date
                if read_action == '':
                    bill_actions.append([bill_num, s_id, c, read_date, '', '{} Reading'.format(read_row[1]), '', ''])
                else:
                    bill_actions.append([bill_num, s_id, c, read_date, '', '{} on {} Reading'.format(read_action, read_row[1]), '', ''])

        #### Committee Reports
        c_comm = bill_soup.find('h3', text = "{} Committee Reports:".format(c))
        if c_comm:
            c_comm = c_comm.findNextSibling('blockquote').table
            if c_comm:
                for row in c_comm.findAll('tr')[1:]:
                    cells = row.findAll('td')
                    c_name = cells[0].b.text.strip()
                    ref_date = cells[1].b.text.strip()
                    if ref_date != '':
                        ref_date = datetime.datetime.strptime(ref_date, '%m/%d/%Y').strftime('%Y-%m-%d')
                        bill_actions.append([bill_num, s_id, c, ref_date, '', 'Referred to Committee: {}'.format(c_name), '', ''])
                    report_date = cells[2].b.text.strip()
                    report_content = cells[3].b.text.strip()
                    report_action = cells[5].b.text.strip()
                    if report_date != '' and report_content == '':
                        report_date = datetime.datetime.strptime(report_date, '%m/%d/%Y').strftime('%Y-%m-%d')
                        bill_actions.append([bill_num, s_id, c, report_date, '', 'Reported from {}'.format(c_name), '', ''])
                    elif report_date != '':
                        report_date = datetime.datetime.strptime(report_date, '%m/%d/%Y').strftime('%Y-%m-%d')
                        bill_actions.append([bill_num, s_id, c, report_date, '', '{} Report {} by {}'.format(report_content, report_action, c_name), '', ''])

        #### Amendments
        c_amend = bill_soup.find('h3', string = re.compile("{} Amendmen.+:".format(c)))
        if c_amend:
            c_amend = c_amend.findNextSibling('blockquote').table
            if c_amend:
                for row in c_amend.findAll('tr')[1:]:
                    cells = row.findAll('td')
                    proposer_name = cells[0].b.text.strip()
                    amend_date = cells[1].b.text.strip()
                    if amend_date != '':
                        amend_date = datetime.datetime.strptime(amend_date, '%m/%d/%Y').strftime('%Y-%m-%d')
                        amend_action = cells[3].b.text.strip()
                        bill_actions.append([bill_num, s_id, c, amend_date, '', 'Floor Amendment ({}): {}'.format(proposer_name, amend_action), '', ''])

        #### Roll Calls
        c_rollcall = bill_soup.find('h3', string = re.compile("{} Roll Call.+:".format(c)))
        if c_rollcall:
            c_rollcall = c_rollcall.findNextSibling('blockquote').table
            if c_rollcall:
                for row in c_rollcall.findAll('tr')[1:]:
                    cells = row.findAll('td')
                    rc_date = cells[0].b.text.strip()
                    if rc_date != '':
                        rc_date = datetime.datetime.strptime(rc_date, '%m/%d/%Y').strftime('%Y-%m-%d')
                        rc_question = cells[1].b.text.strip()
                        if rc_question in ['test', 'testing']:
                            continue
                        rc_outcome = cells[4].b.text.strip()
                        if rc_outcome != '':
                            rc_outcome = vote_types[cells[4].b.text.strip()]
                        bill_actions.append([bill_num, s_id, c, rc_date, '', 'Roll Call Vote: {} (Q: {})'.format(rc_outcome, rc_question), '', ''])

    #### Conference Committee
    conf_comm = bill_soup.find('h3', string = re.compile("Conference Committees:"))
    c = 'Conference Committee'
    if conf_comm:
        conf_comm = conf_comm.findParent('a').findNextSibling('blockquote').table
        ### House listed on in left column even on senate bills
        if conf_comm:
            first_row_cells = conf_comm.findAll('tr')[1].findAll('td')
            H_appt_date = first_row_cells[1].b.text.strip()
            S_appt_date = first_row_cells[3].b.text.strip()
            if H_appt_date != '':
                H_appt_date = datetime.datetime.strptime(H_appt_date, '%m/%d/%Y').strftime('%Y-%m-%d')
                bill_actions.append([bill_num, s_id, c, H_appt_date, '', 'House Conference Committe Members Appointed', '', ''])
            if S_appt_date != '':
                S_appt_date = datetime.datetime.strptime(S_appt_date, '%m/%d/%Y').strftime('%Y-%m-%d')
                bill_actions.append([bill_num, s_id, c, S_appt_date, '', 'Senate Conference Committe Members Appointed', '', ''])

    #### Governor Actions
    gov_actions = bill_soup.find('h3', string = re.compile("Governor's Actions"))
    c = 'Governor'
    if gov_actions:
        gov_actions = gov_actions.findNextSibling('blockquote').table
        if gov_actions:
            ## Agreed Upon by House and Seante
            agreement = gov_actions.find('td', string = re.compile('House and Senate Agreement'))
            agreement_date = agreement.findNextSibling('td').b.text.strip()
            if agreement_date != '':
                agreement_date = datetime.datetime.strptime(agreement_date, '%m/%d/%Y').strftime('%Y-%m-%d')
                bill_actions.append([bill_num, s_id, c, agreement_date, '', 'House and Senate Agreement', '', ''])
            ### Sent to Governor
            sent_to_gov = gov_actions.find('td', string = re.compile('Sent to Governor'))
            sent_to_gov = sent_to_gov.findNextSibling('td').b.text.strip()
            if sent_to_gov != '':
                sent_to_gov = datetime.datetime.strptime(sent_to_gov, '%m/%d/%Y').strftime('%Y-%m-%d')
                bill_actions.append([bill_num, s_id, c, sent_to_gov, '', 'Sent to Governor', '', ''])
            ### Governor's Action
            governor_action = gov_actions.find('td', string = re.compile("Governor's Action"))
            governor_action = governor_action.findNextSibling('td').b.text.strip()
            gov_action_date = gov_actions.find('td', string = re.compile("^Date:"))
            gov_action_date = gov_action_date.findNextSibling('td').b.text.strip()
            if gov_action_date != '':
                gov_action_date = datetime.datetime.strptime(gov_action_date, '%m/%d/%Y').strftime('%Y-%m-%d')
                bill_actions.append([bill_num, s_id, c, gov_action_date, '', '{} by Governor'.format(governor_action), '', ''])

    ### Combine Items
    bill_details = [bill_num, s_id, act_num, version_status, primary_sponsor, cosponsors, title, statutes, bill_url]
    return([bill_details, bill_actions])




########################################################
############## SCRAPE SESSION(S)
############################################
# s = sessions[0]
# bill_info = session_bills[1081]

for s in sessions:

    #### Output Lists
    session_bill_details = [['bill_id', 'session', 'act_num', 'verstion_status', 'primary_sponsor', 'cosponsors', 'title', 'statutes', 'bill_url']]
    session_actions = [['bill_id', 'session', 'chamber', 'action_date', 'status', 'action', 'order', 'journal_page']]

    print("\n ------------------- Now Scraping: {} ---------------------- \n".format(s[0]))

    ### Get all bills for a specific session
    session_bills = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        if bill_info[4] == 'https://legislature.vermont.gov/bill/status/2022/S.R.6':
            num += 1
            continue
        ### Get Basic Details
        if int(s[0][:4]) >= 2009:
            bill_data = get_bill_data(bill_info)
        else:
            bill_data = get_pre2009_bill_data(bill_info)

        ### Check if error
        if bill_data == 'HTTP Error':
            print(" ********** \n ({}/{}) -- HTTP ERROR -- SKIPPING -- {} \n **********".format(num, total, bill_data[8]))
            num += 1
            continue

        ### Get Actions
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_info[0], bill_info[-1]))
        num += 1

    with open("VT_Bill_Details_{}.csv".format(s[0]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("VT_Bill_Histories_{}.csv".format(s[0]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} SCRAPED + DATA SAVED  -------------\n\n\n".format(s[0]))


print("  ********************************** ALL DONE ********************************** ")
