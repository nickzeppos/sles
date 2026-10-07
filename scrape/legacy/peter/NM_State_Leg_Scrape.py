# -*- coding: utf-8 -*-
"""
Created on Mon Feb 11 10:21:01 2019

~~~~~~~ Scrape NEW MEXICO Legislation 1996 - PRESENT ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# See https://www.nmlegis.gov/Legislation/Action_Abbreviations for action explanations
####################

import csv
import os
import urllib
import requests
from bs4 import BeautifulSoup
import time
import datetime
import re

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/NM/')

#####################################
##### Extract Session Information
#######################################

session_search = urllib.request.urlopen('https://www.nmlegis.gov/Legislation/Legislation_List', timeout = 30) 
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.find('select', id = 'MainContent_ddlSessionStart')
sessions = [[i['value'], i.get_text()] for i in session_list.findAll('option')]

s_dict = {'Regular':'RS', '1st Special':'S1', '2nd Special':'S2', '3rd Special':'S3', 'Extraordinary':'ES'}
sessions = [ [val, s.split(' ')[0], s_dict[s.split(' ', 1)[1]] ] for val, s in sessions ]

### Drop Previously Scraped
sessions = [s for s in sessions if 'NM_Bill_Details_' + '{}_{}'.format(s[1], s[2]) + '.csv' not in os.listdir('.')]

### Drop Current Session
this_year = datetime.datetime.now().year
sessions = [s for s in sessions if int(s[1]) < this_year]
print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))

del this_year, session_search, session_search_soup, session_list

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s = sessions[0]
# test = get_session_bills(s)

def get_session_bills(s): 
    
    s_id, s_yr, s_type = s
    
    print('\n\n\t~~~~ Gathering Bill URLs for the {}-{} Session ~~~~\n'.format(s_yr, s_type))

    session_bills = []
    
    search_url = 'https://www.nmlegis.gov/Legislation/Legislation_List'
    
    r = requests.session()
    search_page = r.get(search_url, timeout = 30) 
    search_soup = BeautifulSoup(search_page.content, 'lxml')

    form_data = {'__EVENTTARGET': '', '__EVENTARGUMENT': ''}
    form_data['__VIEWSTATE'] = search_soup.select('#__VIEWSTATE')[0]['value']
    form_data['__VIEWSTATEGENERATOR'] = search_soup.select('#__VIEWSTATEGENERATOR')[0]['value']
    form_data['__EVENTVALIDATION'] = search_soup.select('#__EVENTVALIDATION')[0]['value']
    form_data['ctl00$MainContent$ddlSessionStart'] = s_id
    form_data['ctl00$MainContent$ddlSessionEnd'] = s_id
    form_data['ctl00$MainContent$chkSearchBills'] = 'on'       
    form_data['ctl00$MainContent$chkSearchMemorials'] = 'on'
    form_data['ctl00$MainContent$chkSearchResolutions'] = 'on'
    form_data['ctl00$MainContent$btnSearch'] = 'Go'       
    form_data['ctl00$MainContent$ddlResultsPerPage'] = 2000       
             
    ### Get Bill List
    list_req = r.post(search_url, data = form_data)
    list_soup = BeautifulSoup(list_req.content, 'lxml')
    list_table = list_soup.find('table', id = 'MainContent_gridViewLegislation')
    
    # if len(list_table.findAll('tr')[1:]) == 2002: print(' --- 2000+ BILLS ---> NEED TO ADJUST FOR MUlTIPLE PAGES)')
    
    for row in list_table.findAll('tr')[1:]:
        
        if 'class' in row.attrs and row.attrs['class'] == ['gridview-pagination']:
            break
        
        cells = row.findAll('td')  
        bill_num = re.sub('^\\*', '', cells[0].text.strip()) # * indicate bills with emergency clause  = goes into effect immediately
        title = cells[1].text.strip()
        sponsors = '; '.join([p.text.strip() for p in cells[2].findAll('a')])
        bill_url = 'https://www.nmlegis.gov/Legislation/' + cells[0].find('a')['href']
        
        session_bills.append([bill_num, s_yr, s_type, title, sponsors, bill_url])
    
    #### Checking Second Page: --- E.g., 2009 RS
    next_page = list_table.find('tr', {'class':'gridview-pagination'})
    if next_page:
        print(" ~~~ Pulling Data from 2nd Page ")
        
        ### Update Form Content
        #ajax_path = list_soup.find('script', {'src': re.compile('/bundles/MsAjaxJs\\?v')})
        #list_req = r.get(url = 'https://www.nmlegis.gov' + ajax_path['src'])
        form_data['__EVENTTARGET'] = 'ctl00$MainContent$gridViewLegislation'
        form_data['__EVENTARGUMENT'] = 'Page$2'
        form_data['__VIEWSTATE'] = list_soup.select('#__VIEWSTATE')[0]['value']
        # form_data['__VIEWSTATEGENERATOR'] = list_soup.select('#__VIEWSTATEGENERATOR')[0]['value']
        form_data['__EVENTVALIDATION'] = list_soup.select('#__EVENTVALIDATION')[0]['value']
        del form_data['ctl00$MainContent$btnSearch']
        del form_data['__VIEWSTATEGENERATOR']
        
        ### Get Page
        list_req = r.post(search_url, data = form_data)
        list_soup = BeautifulSoup(list_req.content, 'lxml')
        list_table = list_soup.find('table', id = 'MainContent_gridViewLegislation')
        
        ### Save Data
        for row in list_table.findAll('tr')[1:]:
        
            if 'class' in row.attrs and row.attrs['class'] == ['gridview-pagination']:
                break
        
            cells = row.findAll('td')  
            bill_num = re.sub('^\\*', '', cells[0].text.strip()) # * indicate bills with emergency clause  = goes into effect immediately
            title = cells[1].text.strip()
            sponsors = '; '.join([p.text.strip() for p in cells[2].findAll('a')])
            bill_url = 'https://www.nmlegis.gov/Legislation/' + cells[0].find('a')['href']
            
            session_bills.append([bill_num, s_yr, s_type, title, sponsors, bill_url])
    
    if len(session_bills) == 4000:
        print( ' \n\n\n *********** EXACTLY 4000 BILLS --- NEED TO ACCOUNT FOR 3rd PAGE ************** \n\n\n')
        exit()  

    return(session_bills)    
    

##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################
    
def get_page_soup(bill_url, parser = 'lxml'):
    
    ### Get HTML
    try:
        page = requests.get(bill_url, timeout = 20)
    except requests.exceptions.HTTPError: # as e
        return('HTTP Error')
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(15)
            page = requests.get(bill_url, timeout = 30)
        except requests.exceptions.HTTPError: # as e
            return('HTTP Error')
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = requests.get(bill_url, timeout = 60)
    time.sleep(.5)
    
    ### Return Soup
    page_soup = BeautifulSoup(page.content, parser)
    return(page_soup)    
    

#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = test[100]
# bill_row = session_bills[0]

def get_bill_data(bill_row):    
        
    ### Scrape Bill Page 
    bill_num, s_yr, s_type, title, sponsors, bill_url = bill_row
    
    ### Get HTML, Soup    
    bill_soup = get_page_soup(bill_url)
    
    ### Standardize bill_num
    bill_num = bill_num.split(' ')     
    bill_num = bill_num[0] + bill_num[1].zfill(4)
    
    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error') 

    #### Extract Data
    status = bill_soup.find('a', id = 'MainContent_formViewLegislation_linkLocation').text.strip()
    comm_reports = bill_soup.findAll('a', id = re.compile('MainContent_tabContainerLegislation_tabPanelReports_dataListReports_linkPDF_'))
    comm_reports = '; '.join([i.find('span').text.strip() for i in comm_reports])
      
    ## Get Action Table    
    action_table = bill_soup.find('div', id = 'MainContent_tabContainerLegislation_tabPanelActions')
    action_table = action_table.findAll('table')[1]
    
    bill_actions = []
    order = 1
    # item = action_table.findAll('tr')[0]
    for item in action_table.findAll('tr'):
        txt = item.find('span')
        if txt is None:
            continue
        txt = txt.get_text("~~~").strip()
        if len(txt.split('~~~')) == 1:
            leg_day = ''
            date = ''
            action = txt
        else:
            txt = txt.split('~~~')
            date = [re.sub('.+: ', '', i) for i in txt if 'Calendar Day' in i]
            if date != []:
                date = datetime.datetime.strptime(date[0], '%m/%d/%Y').strftime('%Y-%m-%d')      
            else:
                date = ''
            
            leg_day = [re.sub('.+: ', '', i) for i in txt if 'Legislative Day' in i]
            if leg_day != []:
                leg_day = 'LD: {}'.format(leg_day[0])
            else:
                leg_day = ''
                
            action = txt[len(txt) - 1]

        bill_actions.append([bill_num, s_yr, s_type, date, leg_day, action, order])
        order += 1
        
    ### Could Get Votes with Vote Tab...

    #############
    ### OUTPUT 
    ##############
    
    bill_details = [bill_num, s_yr, s_type, sponsors, title, status, comm_reports, bill_url]
    
    return([bill_details, bill_actions])
        
########################################################
############## SCRAPE SESSION(S)
############################################
# bill_num, bill_url, descrip = session_urls[6751]
# s = sessions[10]
    
for s in sessions:
    
    #### Output Lists
    session_bill_details = [['bill_number', 'session_year', 'session_type', 'sponsors', 'title', 'status', 'comm_reports', 'bill_url']]    
    session_actions = [['bill_number', 'session_year', 'session_type', 'action_date', 'legislative_day', 'action', 'order']]
    
    print("\n ------------------- Now Scraping the {}-{} Session ---------------------- \n".format(s[1], s[2]))    
    
    ### Get all urls for a session-year, including special sessions
    session_urls = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_urls)
    for bill_row in session_urls:
        
        bill_data = get_bill_data(bill_row)
        
        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING -- URL: {}\n **********".format(num, total, bill_row[0], bill_row[5]))
            num += 1
            continue       
            
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[0], bill_row[5]))
        num += 1
        
    with open("NM_Bill_Details_" +'{}_{}'.format(s[1], s[2]) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("NM_Bill_Histories_" + '{}_{}'.format(s[1], s[2]) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- {}-{} Session SCRAPED + DATA SAVED  -------------\n\n\n".format(s[1], s[2]))   


print("  ********************************** ALL DONE ********************************** ")
