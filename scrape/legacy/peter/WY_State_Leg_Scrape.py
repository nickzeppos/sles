# -*- coding: utf-8 -*-
"""
Created on Tue Feb 12 15:56:45 2019

~~~~~~~ Scrape WYOMING Legislation 2001 - PRESENT ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# Special Sessions are folded into bill histories -- so if reintroduced, keeps bill number, and action is recorded there
####################

import csv
import os
#import urllib
import requests
#from bs4 import BeautifulSoup
import time
import datetime
import json
#from dateutil import parser as dateparser
import re

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/WY/')

#####################################
##### Extract Session Information
#######################################

### All Sessions from 2001 to Present Year - 1
this_year = datetime.datetime.now().year
sessions = [s for s in range(2001, this_year, 1) if 'WY_Bill_Details_' + str(s) + '.csv' not in os.listdir('.')]

del this_year

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# sy = sessions[0]
# test = get_session_bills(2018)

def get_session_bills(sy): 
    
    print('\n\n\t~~~~ Gathering Bill URLs for the {} Session ~~~~\n'.format(sy))

    session_bills = []
     
    ### Get Bill List
    list_url = 'https://web.wyoleg.gov/LsoService/api/BillInformation?$filter=Year%20eq%20{}&$orderby=BillNum'
    list_req = requests.get(list_url.format(str(sy)))
    bill_data = list_req.json()
    bill_api_stem = 'https://web.wyoleg.gov/LsoService/api/BillInformation/{}/{}'
    
    for bill in bill_data:
        
        bill_num = bill['billNum']
        bill_url = bill_api_stem.format(sy, bill_num)
        
        session_bills.append([bill_num, sy, bill_url])
    
    return(session_bills)    
    

##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################
    
def get_json(bill_url):
    
    ### Get HTML
    try:
        page = requests.get(bill_url, timeout = 20)
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(15)
            page = requests.get(bill_url, timeout = 30)
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = requests.get(bill_url, timeout = 60)
    time.sleep(.5)
    
    ### Return Soup
    page_json = json.loads(page.content.decode('utf-8'))
    return(page_json)    


#######################
###### Functions to Scrape Individual Bills
#########################
# https://web.wyoleg.gov/LsoService/Help/Api/GET-api-BillInformation-year-billNumber-calendarDate
# bill_row = test[100]
# bill_row = session_bills[0]

def get_bill_data(bill_row):    
        
    ### Scrape Bill Page 
    bill_num, sy, bill_url = bill_row
 
    ### Get HTML, Soup    
    bill_json = get_json(bill_url)

    #### Extract Data
    title = re.sub('\s\s+', ' ', bill_json['billTitle'].strip())
    shortTitle = bill_json['catchTitle'].strip()
    chapter_num = bill_json['chapter']
    status = bill_json['billStatus']
    
    p_names = [i['name'] for i in bill_json['sponsors'] if i['primarySponsor'] == True ]
    p_titles = [i['sponsorTitle'] for i in bill_json['sponsors'] if i['primarySponsor'] == True ]
    primary_sponsor = '; '.join([name if title == None else '{} {}'.format(title, name) for name, title in zip(p_names, p_titles)])
    #primary_sponsor = '; '.join(['{} {}'.format(i['sponsorTitle'], i['name']).strip() for i in bill_json['sponsors'] if i['primarySponsor'] == True])
    
    co_names = [i['name'] for i in bill_json['sponsors'] if i['primarySponsor'] == False ]
    co_titles = [i['sponsorTitle'] for i in bill_json['sponsors'] if i['primarySponsor'] == False ]
    cosponsors = '; '.join([name if title == None else '{} {}'.format(title, name) for name, title in zip(co_names, co_titles)])
    #cosponsors = '; '.join(['{} {}'.format(i['sponsorTitle'], i['name']).strip() for i in bill_json['sponsors'] if i['primarySponsor'] == False])

    ## Get Actions --- Votes Would be easy to get with bill_json['rollCalls']    
    bill_actions = []
    order = len(bill_json['billActions'])
    for item in bill_json['billActions']:
        action = item['statusMessage'].strip()
        date = item['statusDate'].split('T')[0]
        chamber = item['location']
        voteid = item['voteId']
        bill_actions.append([bill_num, sy, date, chamber, action, voteid, order])
        order = order - 1

    #############
    ### OUTPUT 
    ##############
    
    bill_details = [bill_num, sy, primary_sponsor, cosponsors, status, chapter_num, shortTitle, title, bill_url]
    return([bill_details, bill_actions])
        
########################################################
############## SCRAPE SESSION(S)
############################################
# bill_num, bill_url, descrip = session_urls[6751]
# s = sessions[0]
    
for s in sessions:
    
    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'primary_sponsor', 'cosponsors', 'status', 'chapter_num', 'shortTitle', 'title', 'bill_url']]    
    session_actions = [['bill_number', 'session', 'action_date', 'chamber', 'action', 'voteid', 'order']]
    
    print("\n ------------------- Now Scraping the {} Session ---------------------- \n".format(s))    
    
    ### Get all urls for a session-year, including special sessions
    session_urls = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_urls)
    for bill_row in session_urls:
        
        bill_data = get_bill_data(bill_row)    
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {}".format(num, total, bill_row[0], bill_row[2]))
        num += 1
        
    with open("WY_Bill_Details_" + str(s) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("WY_Bill_Histories_" + str(s) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- {} Session SCRAPED + DATA SAVED  -------------\n\n\n".format(s))   


print("  ********************************** ALL DONE ********************************** ")
