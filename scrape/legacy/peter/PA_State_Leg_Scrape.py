# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape PA Legislation ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# Per OpenStates code, PA is continuously adding backdata... 
# At present, no history data prior to 1969-1970 Session
#
# Could Scrape Votes as well - but requires multiple urls or looping through a different part of the website
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

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/PA/')


##################################
##### Extract Session Information
##########################################

session_search = urllib.request.urlopen('https://www.legis.state.pa.us/cfdocs/legis/home/bills/', timeout = 30) 
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.find('select', id = 'billSessions').findAll('option')
sessions = [[i['value'], i.get_text().strip()] for i in session_list]

### Drop Sessions BEFORE 1969
sessions = [[s_id, name] for s_id, name in sessions if int(s_id[0:4]) >= 1969]

### Drop Previously Scraped
sessions = [[s_id, name] for s_id, name in sessions if 'PA_Bill_Details_' + s_id + '.csv' not in os.listdir('.')]

### Drop Current Session
this_year = datetime.datetime.now().year
sessions = [[s_id, name] for s_id, name in sessions if int(s_id[0:4]) < this_year]
print("\n\n ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))


#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s_id, s_name = sessions[1]

def get_session_bills(s_id, s_name): 

    print('\n~~~~ Gathering Bill URLs for the {} ~~~~\n'.format(s_name))
    session_urls = []
    
    ### Splitting ID into Year and Special Num
    s_year, s_special = s_id.split('_')

    ### Request Data + Session for each Chamber
    for chamber in ['H', 'S']:
        search_url = 'https://www.legis.state.pa.us/cfdocs/legis/bi/BillIndx.cfm?sYear={}&sIndex={}&bod={}'.format(s_year, s_special, chamber)
        req = requests.get(search_url)
        chamber_soup = BeautifulSoup(req.content, 'lxml')
        
        chamber_urls = chamber_soup.findAll('a', href = re.compile('billinfo.cfm'))        
        chamber_urls = ['https://www.legis.state.pa.us' + i['href'] for i in chamber_urls]
        session_urls = session_urls + chamber_urls
    
    return(session_urls)


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################
    
def get_page_soup(this_url, parser = 'lxml'):
    
    ### Get HTML
    try:
        page = urllib.request.urlopen(this_url, timeout = 15)
    except urllib.error.HTTPError: # as e
        return('HTTP Error')
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(15)
            page = urllib.request.urlopen(this_url, timeout = 30)
        except urllib.error.HTTPError: # as e
            return('HTTP Error')
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = urllib.request.urlopen(this_url, timeout = 60)
    time.sleep(.25)
    
    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)
    

#######################
###### Functions to Scrape Individual Bills
#########################
# bill_info = session_urls[0]
# s_id, s_name = sessions[0]
    
def get_bill_data(bill_url, s_id, s_name):    

    ### Scrape Bill Page 
    bill_soup = get_page_soup(bill_url)
    
    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')
        
    ### Bill Information in Header
    bn_pieces = re.sub('http.+body=', '', bill_url).split('&')
    bill_num = bn_pieces[0] + bn_pieces[1].replace('type=', '') + bn_pieces[2].replace('bn=', '').zfill(4)
    
    ### Probably need if not none statements here... wait for it to break first
    title = bill_soup.find('div', {'class':'BillInfo-SectionWrapper'})
    title = title.find('div', {'class':'BillInfo-Section-Data'}).get_text().strip().replace("\xa0", " ")
    
    sponsor = bill_soup.find('div', {'class':'BillInfo-Section BillInfo-PrimeSponsor'})
    sponsor = sponsor.find('div', {'class':'BillInfo-Section-Data'}).get_text().strip().replace("\xa0", " ")

    status = bill_soup.find('div', {'class':'BillInfo-Section BillInfo-LastAction'})
    status = status.find('div', {'class':'BillInfo-Section-Data'}).get_text().strip().replace("\xa0", " ")
    
    ## Cosponsor Memo --- Mostly fluff
    #memo_url = bill_soup.find('div', {'class':'BillInfo-Section BillInfo-CosponMemo'})
    #if memo_url is not None:
    #    memo_url = memo_url.find('div', {'class':'BillInfo-Section-Data'}).find('a')['href']
    #else:
    #    memo_url = ''
    
    ##### Get Legislative History:
    action_soup = get_page_soup(bill_url.replace('billinfo.cfm', 'bill_history.cfm'))
    
    ### All Sponsors
    all_sponsors = action_soup.find('div', {'class':'BillInfo-Section BillInfo-PrimeSponsor'})
    all_sponsors = all_sponsors.find('div', {'class':'BillInfo-Section-Data'}).get_text().strip()
    all_sponsors = '; '.join(re.split(', and | and |, ', all_sponsors))
   
    ########### Actions -- If no actions, tab isn't there, shows cosponsors
    bill_actions = []
    hist_table = action_soup.find('div',  {'class':'BillInfo-Section BillInfo-Actions'})
    hist_table = hist_table.find('table', {'class':'DataTable'})
    order = 1
    
    if bill_num[0:1] == 'H':
        chamber = 'House'
    else:
        chamber = 'Senate'
        
    exec_regex = re.compile('In the.+ Governor|Approved.+ Governor|Presented.+ Governor|Became Law.+Governor|Vetoed by|Veto No|Filed in the Office of the Secret|Pamphlet Laws|Item Veto')
    electorate_regex = re.compile('by the Electorate|Vote by Electorate')
    
    if hist_table is not None:
        for row in hist_table.findAll('tr'):
            cell = row.findAll('td')[2]
            cell = cell.get_text().replace('\xa0', ' ').strip()
            cell = re.sub('\s\s+', ' ', cell)
            # Skipping Chamber-Switch Rows
            if 'In the Senate' in cell:
                chamber = 'Senate'
                continue
            elif 'In the House' in cell:
                chamber = 'House'
                continue
            elif 'Signed in House' in cell:
                chamber = 'House'
            elif 'Signed in Senate' in cell:
                chamber = 'Senate'
            elif exec_regex.search(cell):
                chamber = 'Executive'
            elif electorate_regex.search(cell):
                chamber = 'Electorate'
            
            ## Fixing a 1875 error
            cell = re.sub('l975', '1975', cell)
            
            #Adjusting for Act Rows without a date
            if ('Act No.' in cell or 'Veto No.' in cell or 'Item Veto' in cell) and s_id.split('_')[0] not in cell:
                action = cell
                date = bill_actions[-1][3]
            elif 'Pamphlet Laws Resolution' in cell or 'Passed Sessions of ' in cell:
                action = cell
                date = bill_actions[-1][3] 
            elif 'Vote by which conference committee report was' in cell or 'Vote by Electorate, See History' in cell:
                action = cell
                date = bill_actions[-1][3] 
            elif cell == 'INAUGURAL COMMITTEE ON THE PART OF THE SENATE:':
                break
            else:
                #txt_pieces = re.split(', (?=[JFMASOND][a-z]+ [0-9]+, [12][0-9][0-9][0-9])|, (?=[JFMASOND][a-z]+\\. [0-9]+, [12][0-9][0-9][0-9])| (?=[JFMASOND][a-z]+ [0-9]+, [12][0-9][0-9][0-9])| (?=[JFMASOND][a-z]+\\. [0-9]+, [12][0-9][0-9][0-9])| (?=[JFMASOND][a-z]+\\. [0-9]+, [12][0-9][0-9][0-9])|  (?=[JFMASOND][a-z]+\\. [0-9]+, [12][0-9][0-9][0-9])', cell)            
                txt_pieces = re.split(', (?=[JFMASOND][A-Za-z]+ [0-9]+, [12][0-9][0-9][0-9])|, (?=[JFMASOND][A-Za-z]+\\. [0-9]+, [12][0-9][0-9][0-9])| (?=[JFMASOND][A-Za-z]+ [0-9]+, [12][0-9][0-9][0-9])| (?=[JFMASOND][A-Za-z]+\\. [0-9]+, [12][0-9][0-9][0-9])| (?=[JFMASOND][A-Za-z]+\\. [0-9]+, [12][0-9][0-9][0-9])|  (?=[JFMASOND][A-Za-z]+\\. [0-9]+, [12][0-9][0-9][0-9])| (?=[JFMASOND][A-Za-z]+\\. [0-9]+ [12][0-9][0-9][0-9])', cell)                          
                if len(txt_pieces) == 1:
                    action = cell
                    date = bill_actions[-1][3]
                else:  
                    if len(txt_pieces) == 2:
                        action, date = txt_pieces
                    else:
                        action = cell
                        date = txt_pieces[len(txt_pieces)-1]

                    date = re.sub('\\(.+\\)', '', txt_pieces[len(txt_pieces)-1]).strip()
                    date = re.sub('(?<=, \d{4}).+', '', date)
                    if '.' in date:
                        date = date.replace('Sept.', 'Sep.').replace('SEPT.', 'Sep.')
                        if ',' in date:
                            date = datetime.datetime.strptime(date, '%b. %d, %Y').strftime('%Y-%m-%d')     
                        else:
                            date = datetime.datetime.strptime(date, '%b. %d %Y').strftime('%Y-%m-%d')                                
                    else:
                        try:
                            date = datetime.datetime.strptime(date, '%B %d, %Y').strftime('%Y-%m-%d')   
                        except:
                            date = datetime.datetime.strptime(date, '%b %d, %Y').strftime('%Y-%m-%d')   
                
            bill_actions.append([bill_num, s_id, chamber, date, action, order])
            order += 1
    
    ### OUTPUT ~~~ Note: Vote Urls are in the Documents Tab of the Newer Format
    bill_details = [bill_num, s_id, s_name, sponsor, all_sponsors, title, status, bill_url]
    return([bill_details, bill_actions])
        
########################################################
############## SCRAPE SESSION(S)
############################################
# bill_url = session_urls[812]
# s_id, s_name = sessions[0]
    
for s_id, s_name in sessions:

    #### Output Lists
    session_bill_details = [['bill_number', 'session',  'session_full', 'sponsor', 'all_sponsors', 'title', 'status', 'bill_url']]    
    session_actions = [['bill_number', 'session', 'chamber', 'action_date', 'action','order']]
    
    print("\n ------------------- Now Scraping the {} ---------------------- \n".format(s_name))    
    
    ### Get all bills for a specific session
    session_urls = get_session_bills(s_id, s_name)

    #### Loop through bills
    num = 1
    total = len(session_urls)
    for bill_url in session_urls:
        
        bill_data = get_bill_data(bill_url, s_id, s_name)
        
        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- HTTP ERROR --- SKIPPING \n URL: {} **********".format(num, total, bill_url))
            num += 1
            continue
            
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
    
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_data[0][0], bill_url ))
        num += 1
        
    with open("PA_Bill_Details_" + s_id + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("PA_Bill_Histories_" + s_id + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- {} SESSION SCRAPED + DATA SAVED  -------------\n\n\n".format(s_name))


print("  ********************************** ALL DONE ********************************** ")
