# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape SOUTH DAKOTA Bills

@author: PB
"""


###########################
##### NOTES:
# *** Can easily get List of Bills Sponsored by Member if Needed (but always listed first)
# *** OR COMMITTEES: http://sdlegislature.gov/sessions/2001/mem.htm
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


os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/SD/')


#######################
##### Extract Session Links
########################

session_search = requests.get('http://sdlegislature.gov/Legislative_Session/archive.aspx', timeout = 30) 
session_search_soup = BeautifulSoup(session_search.content, 'html5lib')

### Get Sessions
session_table = session_search_soup.find('table', id = 'tblSessionArchive').find('tbody')
session_rows = [i.findAll('td') for i in session_table.findAll('tr')]
sessions = [[i[0].text.strip(), i[1]['data-title']] for i in session_rows]
sessions = [[i[0][:4] + '-SS', int(i[0][:4]), i[1]] if 'Special' in i[0] else [i[0] + '-RS', int(i[0][:4]), i[1]] for i in sessions ] 

### Drop Previously Scraped
sessions = [s for s in sessions if 'SD_Bill_Details_{}.csv'.format(s[0].replace('-', '_')) not in os.listdir('.')]

### Drop Current Session
this_year = datetime.datetime.now().year
sessions = [s for s in sessions if s[1] < this_year]
print("\n\n\t ~~~~ DROPPING SESSION(S) THAT INCLUDE {} ~~~~ \n\n".format(this_year))

del session_search, session_search_soup, session_table, session_rows, this_year


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
    time.sleep(.75)
    
    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)    


#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s = sessions[18]

def get_session_bills(s): 
    
    s_name, s_yr, s_id = s
    
    ### Different Pages Pre/Post-20089
    if s_yr >= 2008:
        list_url = 'http://sdlegislature.gov/Legislative_Session/Bills/Default.aspx?Session={}'.format(s_id)
        list_soup = get_page_soup(list_url)
        url_stem = 'http://sdlegislature.gov/Legislative_Session/Bills/'
        session_bills = [[re.sub('\xa0', '', i.text.strip()), s_name, s_yr, s_id, url_stem + i.a['href']] for i in list_soup.findAll('td', {'data-title':'Bill'})]
    else:
        list_url = 'http://sdlegislature.gov/sessions/{}/billlist.htm'.format(s_id)
        list_soup = get_page_soup(list_url)
        url_stem = 'http://sdlegislature.gov/sessions/{}/'.format(s_id)
        session_bills = [[re.sub('\xa0| ', '', i.text.strip()), s_name, s_yr, s_id, url_stem + i['href']] for i in list_soup.findAll('a', href = re.compile('^[0-9]+.htm|^[A-Z]+[0-9]+.htm'))]

    ### Return
    print(' \n **** ~~~> Found {} BILL URLs for {} **** \n'.format(len(session_bills), s_name))
    return(session_bills)


###################################################
############ Functions to Scrape Individual Bills
#####################################################

    
############
#### 2008 - Present
###########   

def get_bill_data(bill_info):    
    
    bill_num, s_name, s_yr, s_id, bill_url = bill_info
    
    #### Get Bill Page
    bill_soup = get_page_soup(bill_url, 'lxml')    
    
    ##### Basic Details
    summary = ''
    primary_sponsor = ''
    all_sponsors = ''
    keywords = ''
    details = bill_soup.find('div', id = 'ctl00_ContentPlaceHolder1_ctl00_BillDetail')
    
    summary_label = details.find('label', text = re.compile('Purpose'))
    if summary_label:
        summary = summary_label.findNext('div').text.strip()
    
    keyword_label = details.find('label', text = re.compile('Keywords'))
    if keyword_label:
        keywords = keyword_label.findNext('div')
        keywords = '; '.join([i.text.strip() for i in keywords.findAll('a')])
    
    sponsor_label = details.find('label', text = re.compile('Sponsors'))
    if sponsor_label:
        all_sponsors = sponsor_label.findNext('div').get_text()
        primary_sponsor = re.sub(',.+| and .+| at the request.+', '', all_sponsors)
        primary_sponsor = re.sub('Representatives', 'Representative', primary_sponsor)
        primary_sponsor = re.sub('Senators', 'Senator', primary_sponsor)
     
    #### Get Actions
    bill_actions = []
    actions_table = bill_soup.find('table', id = re.compile('tblBillActions'))
    if actions_table:
        order = 1        
        actions_table = actions_table.find('tbody')
        for row in actions_table.findAll("tr"):
            cells = row.findAll('td')
            date = cells[0].text.strip()
            if date != '':
                date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')     
            action = cells[1].text.strip()
            vote_url = ''
            if cells[1].a and 'RollCall' in cells[1].a['href']:
                vote_url = 'http://sdlegislature.gov' + cells[1].a['href']
            bill_actions.append([bill_num, s_name, s_yr, date, action, order, vote_url])
            order += 1
    
    # Return Data
    bill_details = [bill_num, s_name, s_yr, primary_sponsor, all_sponsors, summary, keywords, bill_url]
    return([bill_details, bill_actions])
    
    
######################
##### 1997 - 2007
#####################
    
def get_older_bill_data(bill_info):    
    
    bill_num, s_name, s_yr, s_id, bill_url = bill_info
    
    #### Get Bill Page
    bill_soup = get_page_soup(bill_url, 'lxml')    
    
    ##### Basic Details
    summary = ''
    primary_sponsor = ''
    all_sponsors = ''
    keywords = ''
    
    main_table = bill_soup.find('table')
    current_row = main_table.find('tr')
    
    #### Sponsors
    current_row = current_row.findNext('tr')
    if s_yr != 2000:
        all_sponsors = current_row.get_text().strip()
    else:
        text_url = 'http://sdlegislature.gov/sessions/{}/bills/{}P.htm'.format(s_id, bill_num)
        text_soup = get_page_soup(text_url)
        sponsor_row = text_soup.find(text = re.compile('Introduced by:'))
        all_sponsors = sponsor_row.parent.text.strip()
    
    all_sponsors = re.sub('\xa0|\r\n|\n|\r|Introduced by:|^By ', ' ', all_sponsors)
    all_sponsors = re.sub('  +', ' ', all_sponsors).strip()
        
    primary_sponsor = re.sub(',.+| and .+| at the request.+', '', all_sponsors)
    primary_sponsor = re.sub('Representatives', 'Representative', primary_sponsor)
    primary_sponsor = re.sub('Senators', 'Senator', primary_sponsor)    
    
    ### Summary
    current_row = current_row.findNext('tr')
    summary = current_row.text.strip()
    
    ### Keywords
    keyword_row = main_table.find('b', text = re.compile('Subject Index:'))
    if keyword_row:
        keyword_row = keyword_row.parent
        keywords = '; '.join([i.text.strip() for i in keyword_row.findAll('a')])
     
    #### Get Actions
    bill_actions = []
    actions_table = bill_soup.find('b', text = "Date")
    if actions_table:
        order = 1        
        action_row = actions_table.findParent('tr')
        while True:
            action_row = action_row.findNextSibling("tr")
            if action_row is None:
                break
            cells = action_row.findAll('td')
            date = cells[0].text.strip()
            if date != '':
                date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')     
            action = cells[1].text.strip()
            vote_url = ''
            if cells[1].a and 'rollcall' in cells[1].a['href']:
                vote_url = 'http://sdlegislature.gov' + cells[1].a['href']
            bill_actions.append([bill_num, s_name, s_yr, date, action, order, vote_url])
            order += 1
    
    # Return Data
    bill_details = [bill_num, s_name, s_yr, primary_sponsor, all_sponsors, summary, keywords, bill_url]
    return([bill_details, bill_actions])
         
    
    
########################################################
############## SCRAPE SESSION(S)
############################################
# s = sessions[-9]
# bill_info = session_bills[304]
    
for s in sessions:
    
    #### Output Lists
    session_bill_details = [['bill_id', 'session', 'session_year', 'primary_sponsor', 'all_sponsors', 'summary', 'keywords', 'bill_url']]
    session_actions = [['bill_id', 'session', 'session_year', 'action_date', 'action', 'order', 'vote_url']]
    
    print("\n ------------------- Now Scraping: {} ---------------------- \n".format(s[0]))
    
    ### Get all bills for a specific session
    session_bills = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        ### Get Basic Details
        if s[1] >= 2008:
            bill_data = get_bill_data(bill_info)
        else:
            bill_data = get_older_bill_data(bill_info)
        
        ### Check for Error
        if bill_data == 'HTTP Error':
            print(" ********** \n ({}/{}) -- HTTP ERROR -- SKIPPING -- {} \n **********".format(num, total, bill_info[4]))
            num += 1
            continue           
        
        ### Get Actions
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
    
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_info[0], bill_info[4]))
        num += 1
        
    with open("SD_Bill_Details_{}.csv".format(s[0].replace('-', '_')), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("SD_Bill_Histories_{}.csv".format(s[0].replace('-', '_')), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- {} SCRAPED + DATA SAVED  -------------\n\n\n".format(s[0]))


print("  ********************************** ALL DONE ********************************** ")
