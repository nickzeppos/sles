# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape West Virginia Legislation 1993 - PRESENT ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# USE 1x, 2x, ..., 7x to check special sessions
# Format appears standard back to 1993
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

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/WV/')

#####################################
##### Extract Session Information
#######################################


### Sessions are years
this_year = datetime.datetime.now().year
sessions = [i for i in range(1993, this_year) if 'WV_Bill_Details_' + str(i) + '.csv' not in os.listdir('.')]

del this_year

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s_yr = sessions[0]
# test = get_session_bills(2018)

def get_session_bills(s_yr): 
    
    print('\n\n~~~~ Gathering Bill URLs for the {} Session ~~~~\n'.format(s_yr))

    session_bills = []
    
    ## URls Stems to List of Chamber Bills and Resolutions
    bill_stem = 'http://www.wvlegislature.gov/Bill_Status/Bills_all_bills.cfm?year={}&sessiontype={}&btype={}'
    res_stem = 'http://www.wvlegislature.gov/Bill_Status/res_list.cfm?year={}&sessiontype={}&btype={}'
    
    s_types = ['RS', '1x', '2x', '3x', '4x', '5x', '6x', '7x']
    t_num = 0
    
    ### Looping through until no special session data
    while True:  
        
        ### Get Bills
        bill_req = requests.get(bill_stem.format(s_yr, s_types[t_num], 'bill'))
        bill_soup = BeautifulSoup(bill_req.content, 'lxml')
        these_bills = [ [b.get_text(), s_yr, s_types[t_num], 'http://www.wvlegislature.gov/Bill_Status/' + b['href']] for b in bill_soup.findAll('a', href = re.compile('istory.cfm'))]
        time.sleep(1)
        
        #### Get Resolutions
        res_req = requests.get(res_stem.format(s_yr, s_types[t_num], 'res'))
        res_soup = BeautifulSoup(res_req.content, 'lxml')
        these_res = [ [b.get_text(), s_yr, s_types[t_num], 'http://www.wvlegislature.gov/Bill_Status/' + b['href']] for b in res_soup.findAll('a', href = re.compile('istory.cfm'))]
        time.sleep(1)
        
        if these_bills == [] and these_res == []:
            break
        else:
            for b in these_bills:
                session_bills.append(b)
                
            for r in these_res:
                session_bills.append(r)
                
            print('  --- {}, {}'.format(s_yr, s_types[t_num]))    
            t_num += 1
            
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
#    time.sleep(1)
#    
#    ### Return Soup
#    page_soup = BeautifulSoup(page, parser)
#    return(page_soup)    

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
    time.sleep(1)
    
    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)    
  

#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = test[0]
# bill_row = session_bills[0]
   
def get_bill_data(bill_row):    
        
    ### Scrape Bill Page 
    bill_num, s_yr, s_type, bill_url = bill_row

    ### Get HTML, Soup    
    bill_soup = get_page_soup(bill_url)
    
    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')
    elif re.search('Bill.+does not exist', bill_soup.get_text()):
        return('No Data')

    ### Standardize bill_num
    bill_num = bill_num.split(' ', 1)
    bill_num = bill_num[0] + bill_num[1].zfill(4)        

    #### Extract Data
    summary = bill_soup.find('strong', text = re.compile('SUMMARY'))
    summary = summary.parent.findNextSibling().text.strip()

    primary_sponsor = bill_soup.find('strong', text = re.compile('LEAD SPONSOR'))
    #sponsor_url =  primary_sponsor.parent.findNextSibling().find('a')['href']
    primary_sponsor = primary_sponsor.parent.findNextSibling().text.strip()
    
    cosponsors = bill_soup.find('strong', text = re.compile('^SPONSORS'))
    if cosponsors is not None:
        cosponsors = cosponsors.parent.findNextSibling().findAll('a')
        cosponsors = '; '.join([i.text.strip() for i in cosponsors])
    else:
        cosponsors = ''
    
    subjects = bill_soup.find('strong', text = re.compile('SUBJECT'))
    if subjects is not None:
        subjects = subjects.parent.findNextSibling().findAll('a')
        subjects = '; '.join([i.text.strip() for i in subjects])
    else:
        subjects = ''

    companion = bill_soup.find('strong', text = re.compile('SAME AS|SIMILAR TO'))
    if companion is not None:
        companion = companion.parent.findNextSibling().text.strip()
    else:
        companion = ''

    ## Get Action Table                
    action_table = bill_soup.find('table', {'class':'tabborder'})
    action_rows = action_table.findAll('tr')
        
    ### Action Scrape for Both Formats
    bill_actions = []
    order = len(action_rows[1:])
    for row in action_rows[1:]:
        cells = row.findAll('td')
        if len(cells) == 1:
            continue
        chamber = cells[0].get_text().strip()
        action = cells[1].get_text().strip()
        date = cells[2].get_text().strip()
        if date != '':
            date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')      
        else:
            date = bill_actions[-1][4]               
        journal_page = cells[3].get_text().strip()
            
        bill_actions.append([bill_num, s_yr, s_type, chamber, date, action, journal_page, order])
        order = order - 1
        
        
    #############
    ### OUTPUT 
    ##############
    bill_details = [bill_num, s_yr, s_type, primary_sponsor, cosponsors, summary, subjects, companion, bill_url]
    
    return([bill_details, bill_actions])
        
########################################################
############## SCRAPE SESSION(S)
############################################
# bill_row = session_urls[44]
# s_yr = sessions[0]
    
for s_yr in sessions:

    #### Output Lists
    session_bill_details = [['bill_number', 'session_year', 'session_type', 'primary_sponsor', 'cosponsors', 'summary', 'subjects', 'companion', 'bill_url']]    
    session_actions = [['bill_number', 'session_year', 'session_type', 'chamber', 'action_date', 'action', 'journal_page', 'order']]
    
    print("\n ------------------- Now Scraping the {} Session ---------------------- \n".format(s_yr))    
    
    ### Get all urls for a session-year, including special sessions
    session_urls = get_session_bills(s_yr)

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

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[0], bill_row[3]))
        num += 1
        
    with open("WV_Bill_Details_" + str(s_yr) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("WV_Bill_Histories_" + str(s_yr) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- {} Session SCRAPED + DATA SAVED  -------------\n\n\n".format(s_yr))   


print("  ********************************** ALL DONE ********************************** ")
