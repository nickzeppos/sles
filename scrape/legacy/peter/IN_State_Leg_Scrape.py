# -*- coding: utf-8 -*-
"""

Scrape Indiana Bills

Last Updated: May 2020

@author: PB
"""

##### NOTES: 
#
# This Scraper gathers ONLY data from 2014+
# --> See "IN_State_Leg_Scrape_OLD for possibly defunct scraper for 1999-2013
#
# There is an API (presumably, 2014+ only): http://docs.api.iga.in.gov/introduction.html
#
# *** For 2014, many sponsors are missing -- can get them from actions
#
###########################

import csv
import os
import urllib
from bs4 import BeautifulSoup
import time
import datetime
import re
import socket
import json

#from selenium import webdriver
#from selenium.webdriver.chrome.options import Options  
os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/IN/')

#######################
##### Extract Session Links
########################

##### 2014+ -- By following link to THIS YEAR it will hide that year in the list
this_year = datetime.datetime.today().year
sessions = urllib.request.urlopen('http://iga.in.gov/legislative/{}/session/home/'.format(this_year))
soup = BeautifulSoup(sessions, 'lxml')   

sessions = [re.sub("Archive |Current ", "", t.get_text().strip()) for t in soup.findAll('li', {'class':'session-item'})]
session_urls = ['http://iga.in.gov' + t.find('a')['href'] for t in soup.findAll('li', {'class':'session-item'})]

sessions = [i for i in sessions if 'Previous' not in i]
sessions = [re.sub(' Regular Session| Session', '', i) for i in sessions]
session_urls = [i for i in session_urls if 'archive' not in i]


### Drop Previously Scraped
keep_index = [index for index, s in enumerate(sessions) if 'IN_Bill_Details_' + s.replace(' ', '_') + '.csv' not in os.listdir('.')]
sessions = [s for index, s in enumerate(sessions) if index in keep_index]
session_urls = [url for index, url in enumerate(session_urls) if index in keep_index]


#######################################################
########## GET BILL URLS VIA FTP SITE FOR A SESSION
#####################################################
# session = sessions[0]
# session_url = session_urls[0]

def get_session_bills(session, session_url): 

    bill_list_url = session_url.replace("session/home", "bills")
    res_list_url = session_url.replace("session/home", "resolutions")
    
    ###### Get Bill and Resolution Pages
    bill_list_page = urllib.request.urlopen(bill_list_url, timeout = 60)
    bill_list_soup = BeautifulSoup(bill_list_page, 'lxml')   
    time.sleep(5)
    res_list_page = urllib.request.urlopen(res_list_url, timeout = 60)
    res_list_soup = BeautifulSoup(res_list_page, 'lxml')   
    time.sleep(5)
    
    ############
    ### Extract URLs to Bill Pages + Basic Info --- TWO DIFFERENT VERSIONS
    #############
    
    all_urls = []
    url_search = 'bills/house/[0-9]|bills/senate/[0-9]|simple|concurrent|joint/'
    url_stem = 'http://iga.in.gov'

    all_bills = bill_list_soup.findAll('a', href = re.compile(url_search))
    all_resolutions = res_list_soup.findAll('a', href = re.compile(url_search))
    
    for item in all_bills:
            bill_num = item.get_text().split(':')[0].strip().replace(' ', '')
            short_title = item.get_text().split(':')[1].strip()
            this_url = url_stem + item['href']
            all_urls.append([bill_num, session, short_title, this_url])
            
    for item in all_resolutions:
        bill_num = item.get_text().replace(' ', '')
        short_title = item.parent.get_text().split(':')[1].strip()
        this_url = url_stem + item['href']
        all_urls.append([bill_num, session, short_title, this_url])
         
    return(all_urls)


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################
    
def get_page_soup(bill_url, parser = 'lxml'):
    
    ### Get HTML
    try:
        page = urllib.request.urlopen(bill_url, timeout = 20).read()
    #except urllib.error.HTTPError: # as e
    #    return('HTTP Error')
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
    time.sleep(2.5)
    
    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup) 


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_url = 'http://iga.in.gov/legislative/2017/bills/senate/1'
# bill_info = session_bills[0]

def get_bill_data(bill_info):    
    
    bill_num = bill_info[0]
    this_session = bill_info[1]
    short_title = bill_info[2]
    bill_url = bill_info[3]
    if bill_num[:2] in ['HB', 'SB']:
        bill_type = 'bill'
    else:
        bill_type = 'resolution'
        
    ### Adjust bill number to 4 digits
    #bill_num_z = re.sub('[0-9]+', '', bill_num) + re.sub('[A-Z]+', '', bill_num).zfill(4)
    
    ### Create New URL that goes to Backend Data
    # -- /actions returns both bill and action information
    backend_url = bill_url + '/actions'
    #    if bill_num[:2] in ['HB', 'SB']:
    #        if bill_num[0] == "H":
    #            backend_url = re.sub('(?<=house\\/).+', '', bill_url) + bill_num_z + '/actions'
    #        else:
    #            backend_url = re.sub('(?<=senate\\/).+', '', bill_url) + bill_num_z + '/actions'
    
    #### Pull Data
    bill_soup = get_page_soup(backend_url)
    bill_json = json.loads(bill_soup.find('p').text)

    ### Get Summary and Version Number (for Authors)
    if bill_type == 'bill':
        summary = bill_json['bill']['digest']
        bill_version = bill_json['bill']['name']
    else:
        summary = bill_json['bill']['description']
        bill_version = bill_json['bill']['name']
        
    ##### Get Authors/Sponsors --- Could also get conferees
    ## Authors = Primary (multiple permitted); coauthors = in-chamber cosponsors; sponsors = outchamber cosponsors
    # sponsor_url = re.sub('senate/|house/', '', backend_url)
    # sponsor_url = re.sub('/actions', '.{}./legislators'.format(version_num), sponsor_url)
    # Filling missing backend data -- SB 2 in 2014
    
    sponsor_url = re.sub('senate/.+|house/.+', '', backend_url)
    sponsor_url = sponsor_url + bill_version + '/legislators'
    
    ## Need to account for 2014 bills with missing sponsors! Can add them in with actions
    spon_soup = get_page_soup(sponsor_url)
    if spon_soup == "HTTP Error" and this_session == '2014':
        authors = ''
        coauthors = ''
        cosponsors = ''
    else:
        spon_json = json.loads(spon_soup.find('p').text)
        authors =  '; '.join([j['full_name'] for j in spon_json['members']['authors']])
        coauthors =  '; '.join([j['full_name'] for j in spon_json['members']['co_authors']])
        cosponsors =  '; '.join([j['full_name'] for j in spon_json['members']['sponsors']])

    ##### Actions
    action_json = bill_json['actions']
    
    bill_actions = []
    order = len(action_json)
    for item in action_json:
        chamber = item['chamber']
        date = datetime.datetime.strptime(item['verbose_date'], '%m/%d/%Y').strftime('%Y-%m-%d')
        action = item['text']
        bill_actions.append([bill_num, this_session, date, chamber, action, order])
        order -= 1
        
    ### OUTPUT
    bill_details = [bill_num, this_session, short_title, authors, coauthors, cosponsors, summary, bill_url]
    
    return([bill_details, bill_actions])
        

########################################################
############## SCRAPE SESSION(S)
############################################
# sy = sessions[0]
# sy_url = session_urls[0]
# bill_info = session_bills[1]

for sy, sy_url in zip(sessions, session_urls):
    
    #### Output Lists 
    session_bill_details = [['bill_number', 'session', 'title', 'authors',  'coauthors', 'cosponsors', 'summary', 'bill_url']]    
    session_actions = [['bill_number', 'session', 'action_date', 'chamber', 'action','order']]
    
    print("\n\n ------------------- Now Scraping: Session " + sy + " ---------------------- \n")    

    ### Get all bills for a specific session
    session_bills = get_session_bills(sy, sy_url)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:
        
        ### Skip Vehicle Bills (Blank, no sponsor, placeholders for eventual proposals)
        if re.search('^vehicle.+(bill|resolution)', bill_info[2].lower()):
            continue
        
        ### Get and Parse Bill Data
        bill_data = get_bill_data(bill_info)
        
        ### Append To BIll and Actions Files
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
    
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_data[0][0], bill_data[0][7]))
        num += 1
    
    ### SAVE!
    with open("IN_Bill_Details_" + sy.replace(' ', '_') + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("IN_Bill_Histories_" + sy.replace(' ', '_') + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- " + sy + " SCRAPED + DATA SAVED  -------------\n\n\n")


print("  ********************************** ALL DONE ********************************** ")










