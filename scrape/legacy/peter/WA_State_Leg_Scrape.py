# -*- coding: utf-8 -*-
"""
Created on Mon Feb 11 10:21:01 2019

~~~~~~~ Scrape Washington Legislation 1991 - PRESENT ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# Special Sessions are folded into bill histories -- so if reintroduced, keeps bill number, and action is recorded there
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

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/WA/')

#####################################
##### Extract Session Information
#######################################

session_search = urllib.request.urlopen('https://app.leg.wa.gov/billsummary', timeout = 30) 
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.find('select', id = 'Year')
sessions = [[i['value'], i.get_text()] for i in session_list.findAll('option')]

### Drop Previously Scraped
sessions = [s for s in sessions if 'WA_Bill_Details_' + s[1].replace('-', '_') + '.csv' not in os.listdir('.')]

### Drop Current Session
this_year = datetime.datetime.now().year
sessions = [s for s in sessions if int(s[0]) < this_year]
print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))

### Drop Sessions Prior to 1991
sessions = [s for s in sessions if int(s[0]) >= 1991]

del this_year, session_search, session_search_soup, session_list

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s = sessions[10]
# test = get_session_bills(2018)

def get_session_bills(s): 
    
    s_id, s_yrs = s
    
    print('\n\n\t~~~~ Gathering Bill URLs for the {} Session ~~~~\n'.format(s_yrs))

    session_bills = []
    
    get_leg_url = 'http://wslwebservices.leg.wa.gov/legislationservice.asmx/GetLegislationByYear?year={}'
    
    ### Get Bill List
    list_req = requests.get(get_leg_url.format(s_id))
    list_soup = BeautifulSoup(list_req.content, 'lxml')
    
    for bill in list_soup.findAll('legislationinfo'):
        
        ### These will all direct to same page - don't need them
        if int(bill.substituteversion.text) > 0 or int(bill.engrossedversion.text) > 0:
            continue
        
        bill_num = bill.billid.text
        biennium = bill.biennium.text
        bill_type = bill.longlegislationtype.text
        agency = bill.originalagency.text
        
        session_bills.append([bill_num, s_yrs, biennium, bill_type, agency])
    
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
#    time.sleep(.5)
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
# bill_row = test[100]
# bill_row = session_bills[0]

def get_bill_data(bill_row):    
        
    ### Scrape Bill Page 
    bill_num, s_yrs, biennium, bill_type, start_chamber = bill_row
    num_only = bill_num.split(' ')[1]   
    bill_url = 'http://wslwebservices.leg.wa.gov/legislationservice.asmx/GetLegislation?biennium={}&billNumber={}'.format(biennium, num_only)
    
    ### Get HTML, Soup    
    bill_soup = get_page_soup(bill_url)
    
    ### Standardize bill_num
    bill_num_z = bill_num.replace(' ', '')     
    
    ### Get data for the main bill, not companions/substitutes
    ### But Use all for action history..
    main_bill = bill_soup.legislation  
    
    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error') 

    #### Extract Data
    title = main_bill.legaltitle.text
    summary = main_bill.longdescription.text
    appropriations = main_bill.appropriations.text
    intro_date = main_bill.introduceddate.text.split('T')[0]
    veto = main_bill.veto.text
    partial_veto = main_bill.partialveto.text
    
    companion = '; '.join([i.billid.text.replace(' ', '') for i in bill_soup.findAll('companion')])
    
    ### Get Sponsors
    sponsor_url = 'http://wslwebservices.leg.wa.gov/legislationservice.asmx/GetSponsors?biennium={}&billId={}'.format(biennium, bill_num.replace(' ', '%20'))
    sponsor_soup = get_page_soup(sponsor_url)
    primary_sponsors = '; '.join(['{}, {}'.format(i.lastname.text, i.firstname.text) for i in sponsor_soup.findAll('sponsor') if i.type.text == 'Primary'])
    cosponsors = '; '.join(['{}, {}'.format(i.lastname.text, i.firstname.text)for i in sponsor_soup.findAll('sponsor') if i.type.text == 'Secondary']) 
     
    ## Get Action Table -- API Doesn't yield any actions for 1991 -- 2000 Or CHAMBER information

    # if s_yrs in ['1991-1992', '1993-1994', '1995-1996', '1997-1998', '1999-2000']:
    if bill_type == 'Initiative':
        initiative = 'true'
    else:
        initiative = 'false'
    action_url = 'https://app.leg.wa.gov/billsummary?BillNumber={}&Initiative={}&Year={}'.format(re.sub('[A-Z]+ ', '', bill_num), initiative, biennium[0:4])
    
    action_soup = get_page_soup(action_url)
    history_table = action_soup.find('div', {'class':'historytable'}).parent
    txt_breaks = [i.text.strip() for i in history_table.findAll('p')]    
    hist_sections = action_soup.findAll('div', {'class':'historytable'})
    yr = ''
    base_chamber = bill_num[0:1]
    order = 1    
    bill_actions = []
    for section, txt_br in zip(hist_sections, txt_breaks):
        if 'session' in txt_br.lower():
            yr = txt_br[0:4]
            chamber = base_chamber
        else:
            chamber = re.sub('IN THE ', '', txt_br)[0:1]
  
        date = ''
        for row in section.findAll('div', recursive = False):
            ## Accounting for multiple actions on one day
            if row.div is not None and row.div.text.strip() != '':
                month_day = row.div.text.strip()
                date = month_day + ', ' + yr
                date = datetime.datetime.strptime(date, '%b %d, %Y').strftime('%Y-%m-%d')
            action = re.sub('\n|\r|\([^)]*\)', '', row.text)
            action = re.sub(month_day, '', action).strip()
            
            bill_actions.append([bill_num_z, s_yrs, date, action, chamber, order])
            order += 1

    #### BELOW ONLY WORKS for 2000+ AND doesn't get the CHAMBER
    # else:
    #        action_url = 'http://wslwebservices.leg.wa.gov/legislationservice.asmx/GetLegislativeStatusChangesByBillNumber?biennium={}&billNumber={}&beginDate={}&endDate={}'
    #        beginDate = str(int(s_yrs.split('-')[0]) - 1) + '-01-01'
    #        endDate = str(int(s_yrs.split('-')[1]) + 1) + '-01-01'
    #        action_url = action_url.format(biennium, num_only, beginDate, endDate)
    #            
    #        action_soup = get_page_soup(action_url)
    #        action_items = action_soup.findAll('legislativestatus')
    #        ### Could Get Votes with GetRollCalls?biennium={}&billNumber={}
    #         
    #        ### Action Scrape for Both Formats
    #        order = 1
    #        bill_actions = []
    #        for item in action_items:
    #            action = item.historyline.text.strip()
    #            date = item.actiondate.text.split('T')[0]            
    #            # status = item.status.text.strip()
    #            bill_actions.append([bill_num_z, s_yrs, date, action, order])
    #        order += 1

    #############
    ### OUTPUT 
    ##############
    
    bill_details = [bill_num_z, s_yrs, bill_type, intro_date, primary_sponsors, cosponsors, title, summary, appropriations, veto, partial_veto, companion, bill_url]
    
    return([bill_details, bill_actions])
        
########################################################
############## SCRAPE SESSION(S)
############################################
# bill_row = session_urls[10]
# s = sessions[-1]
    
for s in sessions:
    
    #### Output Lists
    session_bill_details = [['bill_number', 'term', 'bill_type', 'intro_date', 'primary_sponsor', 'cosponsors', 'title', 'summary', 'approp_bill', 'veto', 'partial_veto', 'companion', 'bill_url']]    
    session_actions = [['bill_number', 'term', 'action_date', 'action', 'chamber', 'order']]
    
    print("\n ------------------- Now Scraping the {} TERM ---------------------- \n".format(s[1]))    
    
    ### Get all urls for a session-year, including special sessions
    session_urls = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_urls)
    for bill_row in session_urls:
        
        bill_data = get_bill_data(bill_row)
        
        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n **********".format(num, total, bill_row[0]))
            num += 1
            continue       
            
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {}".format(num, total, bill_row[0]))
        num += 1
        
    with open("WA_Bill_Details_" + s[1].replace('-', '_') + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("WA_Bill_Histories_" + s[1].replace('-', '_')+ ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- {} TERM SCRAPED + DATA SAVED  -------------\n\n\n".format(s[1]))   


print("  ********************************** ALL DONE ********************************** ")
