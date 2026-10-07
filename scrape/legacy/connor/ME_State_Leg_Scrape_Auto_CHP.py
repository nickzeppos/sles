# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape MAINE Bills

@author: PB
"""


#################
######### *** NOTES *****
# -- Using two different processes to get he bill numbers (112-120th session & 121st onward)
# -- THE version for the older sessions does not pull Communications, ORDERS, SP/HP Appointments, or Sentiments
# -----> No way to go those for these bills, but shouldn't be a problem
# -----> Can't use the same get_bills function because need the ID from the url for 121+
#######################

import csv
import os
import urllib
import requests
from bs4 import BeautifulSoup
import time
import datetime
import re
import socket
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

#### Get Data for Older Sessions--no longer necessary
##session_search = requests.get('http://legislature.maine.gov/bills/default_ps.asp', timeout = 30)
##session_search_soup = BeautifulSoup(session_search.content, 'lxml')
##session_list = session_search_soup.find('option', {'value':'120'}).parent.findAll('option')
##old_sessions = [[i['value'], ''] for i in session_list if int(i['value']) < 120 and int(i['value']) > 0]

#### Get Data for Newer Sessions
session_search = requests.get('http://legislature.maine.gov/LawMakerWeb/doadvancedsearch.asp', timeout = 30)
session_search_soup = BeautifulSoup(session_search.content, 'lxml')
session_list = session_search_soup.find('select', {'name':'LegSession'}).findAll('option')

session_nums = [s['value'] for s in session_list]
recent_sessions = [s.get_text() for s in session_list]
recent_sessions = [re.sub('[a-z]+ Legislature', '', s).replace('2001-2002', '120') for s in recent_sessions]
recent_sessions = [[s, n] for s, n in zip(recent_sessions, session_nums)]

#### Combine And Drop Previously Scraped; Also Drop Sessions From Before 2023
#sessions = old_sessions + recent_sessions
sessions = [i for i in recent_sessions if int(i[0]) > 130]
sessions = [i for i in sessions if 'ME_Bill_Details_' + i[0] + '.csv' not in os.listdir('.')]

chrome_options = Options()
chrome_options.add_argument("--headless")


#######################################################
########## GET BILL URLS for 120th SESSION ONWARD
#####################################################
# Adapting OpenStates Code
# session = sessions[0]

def parse_session_results(driver, newurl, leg_list, s_name):
    driver.get(newurl)
    time.sleep(1)
    this_soup = BeautifulSoup(driver.page_source, 'lxml')
    bill_links = this_soup.findAll('a', href = re.compile('summary'))
    clean_bills = [[s_name, a.get_text().strip(), 'http://legislature.maine.gov/LawMakerWeb/' + a['href']] for a in bill_links]
    if len(clean_bills) == 0:
        print("trying again")
        time.sleep(5)
        this_soup = BeautifulSoup(driver.page_source, 'lxml')
        bill_links = this_soup.findAll('a', href = re.compile('summary'))
        clean_bills = [[s_name, a.get_text().strip(), 'http://legislature.maine.gov/LawMakerWeb/' + a['href']] for a in bill_links]
        if len(clean_bills) == 0:
            print("failed")
            exit()
    leg_list = leg_list + clean_bills
    return(leg_list)

def get_session_bills(session):
    s_name = session[0]
    s_id_num = session[1]
    search_url = 'https://legislature.maine.gov/LawMakerWeb/advancedsearch.asp'
    request_session = requests.Session()
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(search_url)
    form_data = {
        "Session": s_name,
    }
    print('~~~~ Gathering Bill URLs for the {} Session ~~~~'.format(s_name))
    leg_session_dropdown = driver.find_element(By.ID, 'LegSession')
    select = Select(leg_session_dropdown)
    for option in select.options:
        if option.text.startswith(s_name):
            select.select_by_value(option.get_attribute("value"))
            break  # Exit the loop after selecting the option
    search_button = driver.find_element(By.NAME, 'search')
    search_button.click()
    new_page_source = driver.page_source
    this_soup = BeautifulSoup(new_page_source, 'html.parser')
    total = this_soup.find(string = re.compile("Results 1")).strip()
    total = int(re.sub('.+\\(of |\\)', '', total))
    bill_urls = []
    startswith = 1
    while startswith < total:
        newurl = 'https://legislature.maine.gov/LawMakerWeb/searchresults.asp?StartWith={}'.format(startswith)
        bill_urls = parse_session_results(driver, newurl, bill_urls, s_name)
        print(' -- {}/{}'.format(min(startswith + 24, total), total))
        time.sleep(1)
        startswith = min(startswith + 25, total)
    return(bill_urls)


#######################################################
########## GET BILL URLS for ALL SESSION UP TO 119th
#####################################################
# ** Could technically use this for ALL of the sessions, but...
# ** No way to ge the ID's to go straight to the ACTION PAGE for 121 onwards
#########
# NOTE: This version does not pull Communications, ORDERS, SP/HP Appointments, or Sentiments

def get_old_session_bills(session):

    s_name = session[0]

    search_url = 'http://legislature.maine.gov/bills/search_ps.asp'
    request_session = requests.Session()

    ### Request Data --- Don't Need From/To or Paper Type --- If Empty/None, will return ALL Session Bills
    form_data = {
        "PID":1456,
        "snum": re.sub('[a-z]+', '', s_name),
        "paperNumberPrefix": "%",
        "lrFrom": 1,
        "lrTo": 99999,
        "sec1": 0,
        "amend_filing_no_prefix": "%",
        "sec2": 0,
        "sec3": 0,
        "phYYFr": 2019,
        "phYYTo": 2019,
        "wkYYFr": 2019,
        "wkYYTo": 2019,
        "sec4": 0,
        "sponsorSession": re.sub('[a-z]+', '', s_name),
        "sec5": 0,
        "hStatYYFr": 2019,
        "hStatYYTo": 2019,
        "sStatYYFr": 2019,
        "sStatYYTo": 2019,
        "sec6": 0,
        "sec7": 0,
        "sec10": 0,
        "tmyYYFr": 2019,
        "tmyYYTo": 2019,
        "submit":'Search'
    }

    print('~~~~ Gathering Bill URLs for the {} Session ~~~~'.format(s_name))

    ### Get First Page + Total Number of Bills
    r = request_session.post(url=search_url, data=form_data)
    time.sleep(3)
    this_soup = BeautifulSoup(r.content, 'lxml')

    results = this_soup.find('table', id = 'search-results')
    all_rows = results.findAll('tr', {'class':'final_row'})
    all_urls = ['http://www.mainelegislature.org/legis/bills/' + a.find('a', {'class':'small_info_btn'})['href'] for a in all_rows]
    all_bills = [[s_name, re.sub('.+\\&paper=|\\&PID.+', '', url), url] for url in all_urls]

    print('---------> Found {} BILLS TOTAL  '.format(len(all_bills)))

    return(all_bills)


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################


def get_page_soup(page_url, parser = 'lxml', sleep = 5):
    try:
        page = urllib.request.urlopen(page_url, timeout = 20).read()
    except urllib.error.HTTPError: # as e
        return('HTTP Error')
    except socket.timeout:
        print("\n ~~> SOCKET TIMEOUT --- Retrying Bill Request")
        time.sleep(120)
        return(get_page_soup(page_url))
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(15)
            page = urllib.request.urlopen(page_url, timeout = 30).read()
        except urllib.error.HTTPError: # as e
            return('HTTP Error')
        except socket.timeout:
            print("\n ~~> SOCKET TIMEOUT --- Retrying Bill Request")
            time.sleep(120)
            return(get_page_soup(page_url))
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = urllib.request.urlopen(page_url, timeout = 60).read()
    time.sleep(sleep)
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)


#######################
###### Function to Scrape Individual Bills
#########################
### **** For Older Bills (120th and Prior) = JUST Need one page
### **** For Newer Bills (121s and later) = Need to also use the chamber status page...


def get_bill_data(s_id_num, bill_info):
    s_name = bill_info[0]
    bill_num = bill_info[1]
    if int(s_name) <= 119:
        paper_num = bill_info[1]
        bill_url = bill_info[2]
        ld_num = ''
    else:
        chamber_status_url = bill_info[2]
        id_num = re.sub('^.+ID=', '', chamber_status_url)
        if 'LD' in bill_num:
            paper_num = ''
            bill_url = 'http://legislature.maine.gov/legis/bills/display_ps.asp?PID=1456&snum={}&paper=&paperld=l&ld={}'.format(s_name, re.sub('LD ', '', bill_num))
            ld_num = 'LD' + re.sub('LD ', '', bill_num).zfill(4)
            paper_num = ''
        else:
            ld_num = ''
            paper_num = re.sub('[0-9]+', '', bill_num).strip() + re.sub('[A-Za-z]+ |[A-Za-z]+', '', bill_num).zfill(4)
            bill_url = 'http://legislature.maine.gov/legis/bills/display_ps.asp?PID=1456&snum={}&paper={}'.format(s_name, bill_num)
    bill_soup = get_page_soup(bill_url, 'html5lib')
    bill_actions = []
    spon_tab = bill_soup.find('span', {'class':'story_heading'}, string = re.compile('Bill Sponsors'))
    if spon_tab:
        spon_tab = spon_tab.findParent('div', id = re.compile('sec[0-9]'))
        all_sponsors = spon_tab.p.get_text(' ')
        all_sponsors = re.sub('  +', ' ', all_sponsors)
        primary_sponsor = re.sub('Cosponsored.+', '', all_sponsors)
        primary_sponsor = re.sub('Presented by |\\. $|\\.$', '', primary_sponsor)
        if 'Cosponsored' in all_sponsors:
            cosponsors = re.sub('.+Cosponsored by |\\. $|\\.$', '', all_sponsors)
            cosponsors = re.sub(' and ', ', ', cosponsors)
        else:
            cosponsors = ''
    elif int(s_name) >= 120:
        sponsor_url = 'http://legislature.maine.gov/LawMakerWeb/sponsors.asp?ID={}'.format(id_num)
        sponsor_soup = get_page_soup(sponsor_url)
        sponsor_table = sponsor_soup.find('table', {'class':'sectionbody'})
        all_sponsors = sponsor_table.find('td', string = re.compile('Sponsored By'))
        if all_sponsors is None:
            primary_sponsor = ''
        else:
            primary_sponsor = all_sponsors.findNext('td').get_text()
        cosponsors = sponsor_table.find('td', string = re.compile('Cosponsored By'))
        if cosponsors is None:
            cosponsors = ''
        else:
            cosponsors = cosponsors.findNext('td').get_text(', ').strip()
            cosponsors = re.sub(' ,', ',', cosponsors)
            cosponsors = re.sub('  +', ' ', cosponsors)
            cosponsors = re.sub(',$', '', cosponsors)
    else:
        primary_sponsor = ''
        cosponsors = ''
    if 'Cannot find requested paper' in bill_soup.text:
        use_chamber_page = True
        chamber_soup = get_page_soup(chamber_status_url, sleep = 5)
        session = ''
        info_table = chamber_soup.findAll('td', {'class':'sectionheading'})[0].findParents('table')[0]
        info_table = info_table.findAll('td', {'class':'sectionbody'})
        if len(info_table) == 3:
            title = info_table[1].get_text().replace('"', '').strip()
            #sponsor = info_table[2].get_text().replace('Sponsored by ', '').strip()
        elif len(info_table) == 2 and 'Sponsored by' not in bill_soup.get_text():
            title = info_table[1].get_text().replace('"', '').strip()
        bn = info_table[0].get_text().strip()
        if ld_num == '' and 'LD' in bn:
            ld_num = re.sub('\\(.+', '', bn)
        if paper_num == '':
            paper_num = re.sub('LD [0-9]+ \\(|\\)', '', bn)
            paper_num = re.sub('[0-9]+', '', paper_num).strip() + re.sub('[A-Za-z]+ |[A-Za-z]+', '', paper_num).zfill(4)
        status_header = bill_soup.find(string = re.compile("Status Summary"))
        if status_header is None:
            ref_comm = ''
            ref_date = ''
            chapter_num = ''
            fiscal_status = ''
            final_status = ''
        else:
            status_table = status_header.findParents('table')[0]
            fiscal_status = ''
            final_status = ''
            comm_tag = status_table.find('td', string = re.compile('Reference Committee'))
            ref_comm = ''
            if comm_tag is not None:
                ref_comm = comm_tag.findNext('td').get_text().strip()
            chapter_tag = status_table.find('td', string = re.compile('Chapter'))
            chapter_num = ''
            if chapter_tag is not None:
                chapter_num = chapter_tag.findNext('td').get_text().strip()
    else:
        use_chamber_page = False
        title = bill_soup.find('h2', {'class':'ldTitle'}).text.strip()
        session_full = bill_soup.find('h1', id = 'siteName').text.strip()
        session = re.sub('.+Legislature, ', '', session_full)
        if paper_num == '':
            paper_num = bill_soup.find('input', {'name':'paperld'})
            paper_num = paper_num.findNextSibling('span').text.strip()
            paper_num = re.sub('[0-9]+', '', paper_num).strip() + re.sub('[A-Za-z]+ |[A-Za-z]+', '', paper_num).zfill(4)
        if ld_num == '':
            ld_num = bill_soup.find('div',id = 'ld_box')
            ld_num = 'LD' + ld_num.input['value'].zfill(4)
            if ld_num == 'LD0000':
                ld_num = ''
        fiscal_status = bill_soup.find('span', {'class':'inlineHeading'}, string = re.compile("Fiscal Status"))
        if fiscal_status:
            fiscal_status = fiscal_status.findNextSibling('span').text.strip()
        else:
            fiscal_status = ''
        final_status = bill_soup.findAll("span", {'class': re.compile('tlnk-final')})
        final_status = '; '.join([re.sub('  +', ' ', i.get_text(' ').strip()) for i in final_status])
        chapter_num = bill_soup.find('span', {'class':'story_heading'}, string = re.compile("Chaptered Law"))
        if chapter_num:
            chapter_num = chapter_num.findParent('p').find('span', {'class':re.compile('tlnk-dnld')})
            chapter_num = '; '.join([i.text.strip() for i in chapter_num if i.text.strip() != ''])
            chapter_num = re.sub(' ,', ',', chapter_num)
        else:
            chapter_num = ''
        comm_tab = bill_soup.find('span', {'class':'story_heading'}, string = re.compile('Status In Comm')).findParent('div', id = re.compile('sec[0-9]'))
        ref_comm = comm_tab.find('span', {'class':'inlineHeading'}, text = 'Referred to')
        if ref_comm:
            ref_date = ref_comm.findNextSibling('span', {'class':'inlineData'}).findNextSibling('span', {'class':'inlineData'}).text.strip()
            ref_date = datetime.datetime.strptime(re.sub('\\.', '', ref_date), '%b %d, %Y').strftime('%Y-%m-%d')
            ref_comm = ref_comm.findNextSibling('span', {'class':'inlineData'}).text.strip()
        else:
            ref_date = ''
            ref_comm = ''
        comm_docket = comm_tab.find('table', {'name':'CDtab'})
        if comm_docket:
            comm_docket_rows = comm_docket.findAll('tr')
            order = 1
            for row in comm_docket_rows[2:]:
                cells = row.findAll('td')
                date = cells[0].text.strip()
                date = datetime.datetime.strptime(date, '%b %d, %Y').strftime('%Y-%m-%d')
                chamber = 'Joint Committee'
                gen_action = cells[1].text.strip()
                result = cells[2].text.strip()
                if result != '':
                    detailed_action = '{} - {}'.format(gen_action, result)
                else:
                    detailed_action = gen_action
                bill_actions.append([ld_num, paper_num, s_name, session, chamber, date, gen_action, detailed_action, order])
                order += 1
    if int(s_name[0:3]) <= 120 and use_chamber_page == False:
        chamber_status_url = 'http://legislature.maine.gov/LawMakerWeb/summary.asp?paper={}&SessionID={}'.format(paper_num, s_id_num)
        if paper_num[0] == 'H':
            search_order = ['House Docket', 'Senate Docket']
        else:
            search_order = ['Senate Docket', 'House Docket']
        for chamb in search_order:
            docket = bill_soup.find('span', {'class':'story_heading'}, string = re.compile(chamb))
            if docket is None:
                next
            if docket.findParent('tbody'):
                order = 1
                docket_rows = docket.findParent('tbody').findAll('tr')
                for row in docket_rows[2:]:
                    cells = row.findAll('td')
                    date = cells[0].text.strip()
                    date = datetime.datetime.strptime(date, '%b %d, %Y').strftime('%Y-%m-%d')
                    chamber = chamb.replace(' Docket', '')
                    gen_action = cells[1].text.strip()
                    detailed_action = cells[2].text.strip()
                    bill_actions.append([ld_num, paper_num, s_name, session, chamber, date, gen_action, detailed_action, order])
                    order += 1
    else:
        if bill_soup.find('span', {'class':'story_heading'}, string = re.compile("House Docket|Senate Docket")):
            print(' ******* POST-120th BILL WITH HOUSE OR SENATE DOCKET TAB: {} ********************'.format(bill_url))
            exit()
        action_url = 'http://legislature.maine.gov/LawMakerWeb/dockets.asp?ID={}'.format(id_num)
        action_soup = get_page_soup(action_url, sleep = 2)
        action_table = action_soup.findAll('td', {'class':'sectionheading'})[1].findParents('table')[0]
        if 'Related Links' in action_table.get_text():
            pass # Means no actions found
        else:
            action_rows = action_table.findAll('tr')
            order = 1
            for row in action_rows[1:]:
                cells = row.findAll('td')
                date = cells[0].get_text()
                date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
                chamber = cells[1].get_text()
                actions = cells[2].get_text() ### Could split these based on double spaces...
                actions = actions.split('.  ')
                for act in actions:
                    bill_actions.append([ld_num, paper_num, s_name, session, chamber, date, '', act, order])
                    order += 1
    bill_details = [ld_num, paper_num, s_name, session, title, primary_sponsor, cosponsors, ref_comm, ref_date, final_status, chapter_num, bill_url, chamber_status_url]
    return([bill_details, bill_actions])



########################################################
############## SCRAPE SESSION(S)
############################################
# bill_info = session_bills[858]
# s = sessions[0]


for s in sessions:

    s_name = s[0]
    s_id_num = s[1]

    #### Output Lists

    # *********** CHANGE BELOW *****************
    session_bill_details = [['LD_num', 'paper_num', 'term', 'session', 'title', 'primary_sponsor', 'cosponsors', 'reference_comm', 'reference_date', 'status', 'chapter_num', 'bill_url', 'chamber_status_url']]
    session_actions = [['LD_num', 'paper_num', 'term', 'session', 'chamber', 'action_date', 'action_general', 'action_detailed','order']]

    print("\n ------------------- Now Scraping the Maine Legislature #{}  ---------------------- \n".format(s_name))

    if int(s_name) <= 119:
        session_bills = get_old_session_bills(s)
    else:
        session_bills = get_session_bills(s)

    # with open("ME_Bill_URLs_" + s_name + ".csv", "w", newline = "") as f:
    #     writer = csv.writer(f)
    #     writer.writerows(session_bills)
    #
    # session_bills = []
    #
    # # Open the CSV file and read its contents into the list of lists
    # with open("ME_Bill_URLs_" + s_name + ".csv", newline='') as file:
    #     csv_reader = csv.reader(file)
    #     for row in csv_reader:
    #         session_bills.append(row)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:
        if bill_info[1] in ['SP0000', 'HP0000']:
            print(" ********** \n ({}/{}) -- {} -- MISSING BILL NUMBER --- SKIPPING \n URL: {} \n **********".format(num, total, bill_info[1], bill_info[2]))
            continue
        bill_data = get_bill_data(s_id_num, bill_info)
        if bill_data == "HTTP Error":
            time.sleep(120)
            bill_data = get_bill_data(s_id_num, bill_info)
            if bill_data == "HTTP Error":
                print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n URL: {} \n **********".format(num, total, bill_info[1], bill_info[2]))
                num += 1
                continue
        session_bill_details.append(bill_data[0])
        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_info[1], bill_info[2]))
        num += 1

    with open("ME_Bill_Details_" + s_name + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("ME_Bill_Histories_" + s_name + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} SESSION SCRAPED + DATA SAVED  -------------\n\n\n".format(s_name))


print("  ********************************** ALL DONE ********************************** ")
