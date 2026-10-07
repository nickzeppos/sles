# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape Louisiana Bills

@author: PB
"""

##### NOTES:
# ---
###########################

import csv
import os
import urllib
from bs4 import BeautifulSoup
import time
import datetime
import re
#import requests
#from robobrowser import RoboBrowser
#import robobrowser  

from selenium import webdriver
from selenium.webdriver.chrome.options import Options 
#from selenium.webdriver.common.keys import Keys
#import selenium.webdriver.support.ui as ui
from selenium.webdriver.support.ui import Select
#from selenium.webdriver.common.action_chains import ActionChains

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/LA/')

#######################
##### Extract Session Links
########################

session_search = urllib.request.urlopen('http://www.legis.la.gov/Legis/SessionInfo/SessionInfo.aspx', timeout = 30) 
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.find('table', id = 'ctl00_ctl00_PageBody_DataListSessions').findAll('a')

#### Construct Keys
# RS = Reg. Sess. / ES = Extraordinary Sess / OS = Org. Sess
# First/Second/THird get numbers... so: 2018 Third Extraordinary Session = 183ES

session_years = [re.sub(' .+$', '', s.get_text()) for s in session_list]
sessions = [s.get_text() for s in session_list]
sessions_short = [s.replace('Regular Session', 'RS').replace('Extraordinary Session', 'ES').replace('Organizational Session', 'OS') for s in sessions]
session_keys = [s[2:4] + s[5:].replace('First ', '1').replace('Second ', '2').replace('Third ', '3') for s in sessions_short]
sessions_short = [s.replace('First', '1st').replace('Second', '2nd').replace('Third', '3rd').replace(' ', '_') for s in sessions_short]

### Drop Previously Scraped
sessions = [[yr, s, short, key] for yr, s, short, key in zip(session_years, sessions, sessions_short, session_keys) if 'LA_Bill_Details_' + short + '.csv' not in os.listdir('.')]

### Drop Org Session with No Data
sessions = [[yr, s, short, key] for yr, s, short, key in sessions if key not in ['04OS', "12OS"]]

### Drop Current Session
this_year = datetime.datetime.now().year
sessions = [[yr, s, short, key] for yr, s, short, key in sessions if int(yr) < this_year]
print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))



#######################################################
########## GET BILL URLS VIA FTP SITE FOR A SESSION
#####################################################
#s_key = '99RS'
# session_bills = get_session_bills(s_key = '00RS')

chrome_options = Options()  
chrome_options.add_argument("--headless")  
def get_session_bills(s_key): 
    
    session_url = 'http://www.legis.la.gov/Legis/BillSearch.aspx?sid={}'.format(s_key)
    session_hb = 'http://www.legis.la.gov/Legis/FinalDisposition.aspx?c=H&sid={}'.format(s_key)
    session_sb = 'http://www.legis.la.gov/Legis/FinalDisposition.aspx?c=S&sid={}'.format(s_key)
    
    print(" \n ~~ Gathering Bill URLs for Session {} ~~~ \n".format(s_key))
    
    #### Get Bills -- There are full lists, no need to loop through search page...
    hb_list = urllib.request.urlopen(session_hb)
    hb_soup = BeautifulSoup(hb_list, 'lxml')
    all_hbs = hb_soup.findAll('a', href = re.compile('BillInfo'))
    all_hbs = [[s_key, 'HB' + hb.get_text(), 'http://www.legis.la.gov/Legis/' + hb['href']] for hb in all_hbs]  
    print(' -- HB ~ {}'.format(len(all_hbs)))

    sb_list = urllib.request.urlopen(session_sb)
    sb_soup = BeautifulSoup(sb_list, 'lxml')
    all_sbs = sb_soup.findAll('a', href = re.compile('BillInfo'))
    all_sbs = [[s_key, 'SB' + sb.get_text(), 'http://www.legis.la.gov/Legis/' + sb['href']] for sb in all_sbs]  
    print(' -- SB ~ {}'.format(len(all_sbs)))
    
    all_legislation = all_hbs + all_sbs
    
    #### Get Resolutions
    # driver = webdriver.Safari()
    driver = webdriver.Chrome(options = chrome_options) 
    driver.get(session_url)
    time.sleep(.5)
    if 'There are no bills associated with' in driver.page_source:
        return('No bills')
    driver.find_element_by_id('ctl00_ctl00_PageBody_PageContent_btnHeadRange').click()
    time.sleep(.5)
    type_select = Select(driver.find_element_by_id('ctl00_ctl00_PageBody_PageContent_ddlInstTypes2'))
    bill_types = [i.text for i in type_select.options if i.text not in ['HB', 'SB', 'ACT']]
    driver.close()
    
    ## SR's are Study Requests --- 'HSR', 'SSR', 'SCSR', 'HCSR'; CR's are Concurrent Resolutions
    for bt in bill_types:
        ### *** NOTE: If only 1 bill, jumps straight to it
        
        #driver = webdriver.Safari()
        driver = webdriver.Chrome(options = chrome_options) 
        driver.get(session_url)
        time.sleep(.5)
        driver.find_element_by_id('ctl00_ctl00_PageBody_PageContent_btnHeadRange').click()
        time.sleep(.5)
        type_select = Select(driver.find_element_by_id('ctl00_ctl00_PageBody_PageContent_ddlInstTypes2'))
        
        ## This works on headless chrome but not reg or safari...
        if type_select.first_selected_option.text != bt:
            type_select.select_by_visible_text(bt)
        time.sleep(.5)
        
        ### Check if only 1 Bill
        num_stop = driver.find_element_by_id('ctl00_ctl00_PageBody_PageContent_tbBillNumStop')
        num_stop = num_stop.get_attribute('value')
        
        driver.find_element_by_id('ctl00_ctl00_PageBody_PageContent_btnSearchByInstRange').click()
        time.sleep(5)
        
        ### If Only 1 Bill, Jumps Straight to the Bill Page (Types with - Bills Are Dropped from Select Box)
        if num_stop == '1':
            results_page = BeautifulSoup(driver.page_source, 'lxml')
            page_data = [[s_key, results_page.find('span', id = 'ctl00_PageBody_LabelBillID').get_text(), re.sub('&sbi=y', '', driver.current_url)]]
            all_legislation = all_legislation + page_data
            print(' -- {} ~ {}'.format(bt, '1'))   
            
        else:
            ### Looping Through Results Pages
            num = 0
            while True:
                results_page = BeautifulSoup(driver.page_source, 'lxml')
                page_data = results_page.findAll('a', href = re.compile('BillInfo'))
                page_data = [[s_key, i.text, 'http://www.legis.la.gov/Legis/' + i['href']] for i in page_data if i.text != 'more...']
                all_legislation = all_legislation + page_data
                num = num + len(page_data)
                print(' -- {} ~ {}'.format(bt, num))
                
                next_page = driver.find_element_by_link_text('>')
                if next_page.get_attribute('disabled') == 'true': ### len(page_data) == 100
                    break
                else:
                    next_page.click()
                    time.sleep(5)
                    num+= 1
        
        driver.close()
    
    print('\n ~~~ All Bill URLs for Session {} Scraped ~~~ \n'.format(s_key))    
    return(all_legislation)
            
    
#######################
###### Functions to Scrape Individual Bills
#########################
# bill_url = 'https://legislature.idaho.gov/sessioninfo/2007/legislation/H0001/'    
# bill_url = 'https://legislature.idaho.gov/sessioninfo/2012/legislation/SR103/'
# sy = '2007'  

def get_bill_data(session_info, bill_info):    
    
    bill_num = bill_info[1]
    bill_url = bill_info[2]
    
    s_year = session_info[0]
    s_key = session_info[3]
    
    try:
        bill_page = urllib.request.urlopen(bill_url, timeout = 10)
    except urllib.error.HTTPError as e:
        return('HTTP Error')
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(10)
            bill_page = urllib.request.urlopen(bill_url, timeout = 30)
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            bill_page = urllib.request.urlopen(bill_url, timeout = 60)
    
   
    bill_soup = BeautifulSoup(bill_page, 'lxml')
    bill_actions = []
    
    ### Exit if no bill data
    if 'A Bill could not be found to match the parameter' in bill_soup.get_text().strip():
        return('Invalid Bill Parameter')
     
    ### Get Bill Data    
    author = bill_soup.find('a', id = 'ctl00_PageBody_LinkAuthor').get_text()
    short_title = bill_soup.find('span', id = 'ctl00_PageBody_LabelShortTitle').get_text().replace('\xa0', ' ').strip()
    status = bill_soup.find('span', id = 'ctl00_PageBody_LabelCurrentStatus').get_text().replace('\xa0', ' ').strip()
    status = re.sub('Current Status: ', '', status).strip()
    
    ### Get Coauthors/Cosponsors
    sponsor_url = bill_url.replace('BillInfo', 'BillDocs') + "&t=authors"
    try:
        sponsor_page = urllib.request.urlopen(sponsor_url, timeout = 10)
    except urllib.error.HTTPError as e:
        print('\n\n' + str(e.code) + ' Sponsor Error ---- Skipping: ' + sponsor_url + '\n\n')
        sponsor_page = 'HTTP Error'
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(10)
            sponsor_page = urllib.request.urlopen(sponsor_url, timeout = 30)
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            sponsor_page = urllib.request.urlopen(sponsor_url, timeout = 60)
    time.sleep(.5)
    
    if sponsor_page == "HTTP Error":
        sponsors = ''
    else: 
        sponsor_soup = BeautifulSoup(sponsor_page, 'lxml')
        sponsor_table = sponsor_soup.find('table')
        sponsors = '; '.join([i.get_text() for i in sponsor_table.findAll('a')])
        
    ### Vote Urls
    vote_url = bill_soup.find('a', href = re.compile('BillDocs.+t=votes'))
    if vote_url is None:
        vote_url = ''
    else:
        vote_url = bill_url.replace('BillInfo', 'BillDocs') + "&t=votes"
    
    ### Get Actions 
    #tables = bill_soup.find('div', id = 'ctl00_PageBody_PanelBillInfo').findAll('table')
    action_table = bill_soup.find('tr', {'class':'ResultsListDark'})
    action_table = action_table.findParents('table')[0]
    
    table_rows = action_table.findAll('tr')
    order = len(table_rows) - 1
    
    for row in table_rows[1:]:
        cells = row.findAll('td')
        date = cells[0].get_text() + '/' + s_year
        date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
        
        chamber = cells[1].get_text()
        journal_page = cells[2].get_text().strip()
        action = cells[3].get_text().strip()
        
        bill_actions.append([bill_num, s_year, s_key, chamber, date, journal_page, action, order])
        order -= 1
        
    ### OUTPUT
    bill_details = [bill_num, s_year, s_key, short_title, status, author, sponsors, bill_url, vote_url ]
    return([bill_details, bill_actions])
        

########################################################
############## SCRAPE SESSION(S)
############################################
# bill_info = session_bills[0]
# session_info = s
    
for s in sessions:
    
    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'session_key', 'short_title', 'status',  'author', 'all_authors', 'bill_url', 'vote_url']]
    session_actions = [['bill_number', 'session', 'session_key', 'chamber', 'action_date', 'journal_page', 'action','order']]
    
    print("\n ------------------- Now Scraping: Session " + s[3] + " ---------------------- \n")    
    
    ### Get all bills for a specific session
    session_bills = get_session_bills(s[3])
    
    if session_bills == 'No bills':
        print('\n ~~~ No Bills Associated With Session {} ~~~ \n'.format(s[3]))
        continue

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        bill_data = get_bill_data(s, bill_info)
        time.sleep(1)
        
        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n URL: {} \n **********".format(num, total, bill_info[1], bill_info[2]))
            num += 1
            continue
        elif bill_data == 'Invalid Bill Parameter':
            print(" ********** \n ({}/{}) -- {} -- INVALID BILL PARAMETER --- SKIPPING \n URL: {} \n **********".format(num, total, bill_info[1], bill_info[2]))
            num += 1
            continue
        
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
    
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_data[0][0], bill_data[0][7]))
        num += 1
        
    with open("LA_Bill_Details_" + s[2] + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("LA_Bill_Histories_" + s[2] + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n --------- " + s[1] + " SCRAPED + DATA SAVED  ----------\n\n\n")


print("  ********************************** ALL DONE ********************************** ")
