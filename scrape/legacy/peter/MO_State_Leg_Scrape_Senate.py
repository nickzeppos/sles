# -*- coding: utf-8 -*-
"""
Created on Fri Feb 1 2019

~~~~~~~ Scrape MISSOURI Legislation 1995 - PRESENT ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# Need to Scrape House and Senate Pages Seperately... 
#
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

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/MO/')

#####################################
##### Extract Session Information
#######################################
# ** Could just do a range of years, but keeping this way in case something changes down the line....

session_search = urllib.request.urlopen('https://www.senate.mo.gov/pastsessions/', timeout = 30) 
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
sessions = []
session_list = session_search_soup.find('div', {'class':'entry-content'}).find('ul').findAll('li', {'type':'square'}, recursive = False)
for s in session_list:
    a_tags = s.findAll('a')
    yr, ga_num, s_type = a_tags[0].get_text().split(' – ')
    yr = int(yr)
    ga_num = re.sub(' Gen.+', '', ga_num)
    for a in a_tags:
        s_type = re.sub('.+ – ', '', a.get_text().strip()).replace('Regular Session', 'RS')
        s_type = re.sub(str(yr) + ' ', '', s_type).replace('Extraordinary Session', 'ES')
        if s_type == 'ES':
            s_type = '1st ' + s_type
        else:
            s_type = s_type.replace('First', '1st').replace('Second', '2nd')          
        sessions.append([ga_num, yr, s_type, a['href']])
        #print(str(yr) + ' --- ' + ga_num + ' --- ' + s_type)

### Drop Previously Scraped
sessions = [s for s in sessions if 'MO_Bill_Details_' + s[0] + '_Senate.csv' not in os.listdir('Senate')]

### Drop Current Session
#this_year = datetime.datetime.now().year
#sessions = [s for s in sessions if int(s[1]) < this_year]
#print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))

del session_search, session_search_soup, session_list

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# ga_num = '99th'

def get_session_bills(ga_num): 

    s = [s for s in sessions if s[0] == ga_num] 

    print('\n~~~~ Gathering Bill URLs for the {} SENATE Session ~~~~\n'.format(ga_num))

    session_bills = []
    for i in range(0, len(s)):
        s_url = s[i][3]
        s_year = s[i][1]
        s_type = s[i][2]
        
        ### Find Bill List Page
        if s_year == 2001:
            r_url = 'https://www.senate.mo.gov/01info/bil-list.htm'
            ss_url = 'https://www.senate.mo.gov/01info/sp/bil-list.htm'
            list_url = [r_url, ss_url]
        elif s_year == 1997 and s_type in ['1st ES', '2nd ES']:
            list_url = [s_url]
        else:
            s_page = requests.get(s_url)
            time.sleep(.5)
            s_soup = BeautifulSoup(s_page.content, 'lxml')
            
            list_url = s_soup.find('a', text = re.compile('List.+Senate Bills'))
            if list_url is None:
                list_url = s_soup.find('a', href = re.compile(str(s_year)[2:4]+ 'info/bi.+list'))
                if list_url is None:
                    list_url = s_soup.find('a', href = re.compile(str(s_year)[2:4]+ 'info.+bi.+list'))
                    if list_url is None:
                        list_url = s_soup.find('a', href = re.compile('BillList'))
            if 'http' not in list_url['href']:
                list_url = ['https://www.senate.mo.gov' + list_url['href']]
            else:
                list_url = [list_url['href']]      
        
        ### Go to List Page
        for l_url in list_url:
            
            list_page = requests.get(l_url)
            time.sleep(.5)
            list_soup = BeautifulSoup(list_page.content, 'lxml')
        
            ### Extract based on Page Format
            bill_stem = 'https://www.senate.mo.gov/'
            if s_year >= 2005:
                bill_stem = 'https://www.senate.mo.gov/{}info/BTS_Web/'.format(str(s_year)[2:4])
                these_urls = list_soup.findAll('a', href = re.compile('Bill.aspx.+BillID'))
                these_urls = [[ga_num, s_year, s_type, i.get_text(), bill_stem + i['href'].strip() ] for i in these_urls]
            elif s_year >= 2003:
                these_urls = list_soup.findAll('a', href = re.compile('bills/S'))   
                these_urls = [[ga_num, s_year, s_type, i.get_text(), bill_stem + re.sub('^/', '', i['href'].strip()) ]for i in these_urls]   
            elif s_year == 1997 and s_type in ['1st ES', '2nd ES']:
                these_urls = list_soup.findAll('a', href = re.compile('s\dinfo/bills/S'))   
                these_urls = [[ga_num, s_year, s_type, i.get_text(), bill_stem + re.sub('^/', '', i['href'].strip()) ]for i in these_urls]
            else:
                these_urls = list_soup.findAll('a', href = re.compile(str(s_year)[2:4] + 'info/bills/S'))
                if these_urls == []:
                    these_urls = list_soup.findAll('a', href = re.compile('bills/S'))   
                    bill_stem = bill_stem + str(s_year)[2:4] + 'info/'
                these_urls = [[ga_num, s_year, s_type, i.get_text(), bill_stem + re.sub('^/', '', i['href'].strip()) ]for i in these_urls]
            
            for bill in these_urls:
                session_bills.append(bill)
                
        print(' -- {}, {}'.format(s_year, s_type))
            
    return(session_bills)

##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################
    
def get_page_soup(bill_url, parser = 'lxml'):
    
    ### Get HTML
    try:
        page = urllib.request.urlopen(bill_url, timeout = 15)
    except urllib.error.HTTPError: # as e
        return('HTTP Error')
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(15)
            page = urllib.request.urlopen(bill_url, timeout = 30)
        except urllib.error.HTTPError: # as e
            return('HTTP Error')
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = urllib.request.urlopen(bill_url, timeout = 60)
    time.sleep(.75)
    
    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)    
    

#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = session_urls[0]
    
def get_bill_data(bill_row):    
        
    ### Bill Data
    ga_num, s_year, s_type, bill_num, bill_url = bill_row
    
    ### Standardize Bill Number    
    bill_num_z = bill_num.split(' ')  
    bill_num_z = bill_num_z[0] + bill_num_z[1].zfill(4)        
    
    ### Get HTML, Soup    
    bill_soup = get_page_soup(bill_url)
    
    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')
         
    ### For Actions
    bill_actions = []
    order = 1         
         
    ## Extract Data ~~~~ 2005 Onward
    if s_year >= 2005:
        
        title = bill_soup.find('span', id = 'lblBillTitle').get_text().strip()
        description = bill_soup.find('span', id = 'lblBriefDesc').get_text().strip()
        
        ### Chamber Sposnor
        sponsor_tag = bill_soup.findAll('a', id = 'hlSponsor')
        sponsor_urls = '; '.join([i['href'] for i in sponsor_tag])
        sponsors = '; '.join([i.get_text().strip() for i in sponsor_tag])
            
        ### Outchamber Sponsor
        if bill_num[0:1] == 'H':    
            outchamber_spon = bill_soup.find('a', id = 'hlSSponsor').get_text().strip()
        elif bill_num[0:1] == 'S':
            outchamber_spon = bill_soup.find('a', id = 'hlHSponsor').get_text().strip()
        else:
            outchamber_spon = ''
            
        ### Cosponsors
        cospon_url = bill_soup.find('a', id = 'hlCoSponsors')
        if cospon_url is not None:
            cospon_soup = get_page_soup(bill_url.replace("Bill.aspx", "CoSponsors.aspx"))
            cosponsors = cospon_soup.findAll('a', id = re.compile('dgCoSponsors'))
            cosponsors = '; '.join([c.get_text().strip() for c in cosponsors])
        else:
            cosponsors = ''
            
        lr_num = bill_soup.find('span', id = 'lblLRNum').get_text().strip()
        #journal_page = bill_soup.find('span', id = 'lblJrnPage').get_text().strip()
        
        committees = bill_soup.find('a', id = 'hlCommittee')
        committees = '; '.join([c.get_text().strip() for c in committees])
            
        effective_date = bill_soup.find('span', id = 'lblEffDate').get_text().strip()
        
        summary = bill_soup.find('span', id = 'lblSummary').get_text().strip()
        summary = summary.replace('\t', ' ')

        #### Get Bill Actions
        action_soup = get_page_soup(bill_url.replace('Bill.aspx', 'Actions.aspx'))
        if action_soup != 'HTTP Error':
            action_table = action_soup.find('table', id = 'Table5').findAll('table')[1]
            action_rows = action_table.findAll('tr')
            
            for row in action_rows:
                cells = row.findAll('td')
                date = datetime.datetime.strptime(cells[0].get_text(), '%m/%d/%Y').strftime('%Y-%m-%d')  
                action = re.sub('\xa0|\r\n|\n|\r', '', cells[1].get_text().strip())
                journal_page = cells[2].get_text().strip()
                bill_actions.append([bill_num_z, ga_num, s_year, s_type, '', date, action, journal_page, order])
                order += 1
        else:
            print("ACTIONS PAGE ERROR -- SKIPPING -- {}".format(bill_num_z))
        
    ## Extract Data ~~~~ 1995 - 2005 Onward    
    else:
        
        title = bill_soup.find('b', text = "Title:").parent
        title = title.findNextSibling('td').get_text().strip()
        
        description = bill_soup.find('b', text = re.compile(bill_num)).findParents('td')[0]
        description = description.findNextSibling('td').get_text().strip()     
        description = re.sub('\s\s+', ' ', description)
                
        ### Chamber Sposnor
        sponsor_tag = bill_soup.find('b', text = "Sponsor:").parent
        sponsor_tag = sponsor_tag.findNextSibling('td').findAll('a')
        sponsors = '; '.join([i.get_text().strip() for i in sponsor_tag])
        sponsor_urls =  '; '.join([i['href'] for i in sponsor_tag])
            
        ### Outchamber Sponsor
        outchamber_spon = ''
        if 'handler' in bill_soup.get_text().lower():
            print(" ************* CHECK OUTCHAMBER SPONSORS ******************** \n URL: {} \n".format(bill_url))
            
        ### Cosponsors
        cospon_url = bill_soup.find('a', href = re.compile('info/cos/'))
        if cospon_url is not None:
            cospon_soup = get_page_soup(bill_url.replace("/bills/", "/cos/"))
            cosponsors = cospon_soup.findAll('a', href = re.compile('info/members/mem'))
            cosponsors = '; '.join([c.get_text().strip() for c in cosponsors])
        else:
            cosponsors = ''    

        lr_num = bill_soup.find('b', text = 'LR Number:').parent
        lr_num = lr_num.findNextSibling('td').get_text().strip()

        committees = bill_soup.find('b', text = 'Committee:').parent
        committees = committees.findNextSibling('td').findAll('a')
        committees = '; '.join([c.get_text().strip() for c in committees])
            
        effective_date = bill_soup.find('b', text = 'Effective Date:').parent
        effective_date = effective_date.findNextSibling('td').get_text().strip()
        
        summary = bill_soup.find('b', text = re.compile('Current Bill Summary'))
        if summary is None:
            summ_soup = get_page_soup(bill_url.replace('/bills/', '/summs/intro/'))
            if summ_soup == 'HTTP Error':
                summary = ''
            else:
                summary = re.sub('\r\n|\n|\r', ' ', summ_soup.get_text().strip())
                summary = re.sub('\s\s+', ' ', summary)
        else:
            summary = summary.findParents('center')[0]
            summary = [re.sub('\r\n', '', i.get_text().strip()) for i in summary.findNextSiblings('p')]
            summary = ' '.join([re.sub('\s\s+', ' ', i) for i in summary if re.sub('\s\s+', ' ', i) != ''])

        #### Get Bill Actions
        if s_year >= 2000:
            action_soup = get_page_soup(bill_url.replace("/bills/", "/actions/").replace('.htm', 'a.htm'))
        elif s_year in [1997, 1998, 1999]:
            action_soup = get_page_soup(bill_url.replace("/bills/", "/actions/").replace('.htm', 'act.htm'))
        elif s_year in [1995, 1996]:
            action_soup = get_page_soup(bill_url.replace("/bills/", "/action/").replace('.htm', 'act.htm'))

        if action_soup != 'HTTP Error':
            action_table = action_soup.findAll('table')
            if action_table is not None:
                action_rows = action_table[1].findAll('tr')
                for row in action_rows:
                    cells = row.findAll('td')
                    if cells == []:
                        continue
                    date = cells[0].get_text().strip()
                    if date in ['03/32/02', 'C1/28/02', '20/52/30', 'Q0/50/80']:
                        fix_dates = {'03/32/02':'03/31/02', 'C1/28/02':'01/28/02', '20/52/30':'05/23/01', 'Q0/50/80':'05/08/03'}
                        date = fix_dates[date]
                    if date == '':
                        date = bill_actions[-1][5]
                    else:
                        date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')  
                    action = re.sub('\xa0|\r\n|\n|\r', '', cells[1].get_text().strip())
                    action = re.sub('\s\s+', ' ', action)
                    if len(cells) > 2:
                        journal_page = cells[2].get_text().strip()
                    else:
                        journal_page = ''
                    bill_actions.append([bill_num_z, ga_num, s_year, s_type, '', date, action, journal_page, order])
                    order += 1
     
    
    #####################    
    ### OUTPUT 
    #####################
    
    bill_details = [bill_num_z, ga_num, s_year, s_type, title, description, sponsors, sponsor_urls, cosponsors, outchamber_spon, lr_num, committees, effective_date, summary, bill_url]
    
    return([bill_details, bill_actions])
        
########################################################
############## SCRAPE SESSION(S)
############################################    

### *** LOOPING THROUGH NUMBERED SESSIONS --- EG, 99th, not years ********
all_ga_nums = sorted(set([s[0] for s in sessions]))

for ga_num in all_ga_nums:

    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'session_year', 'session_type', 'title', 'description', 'primary_sponsor', 'ps_url', 'cosponsors', 'outchamber_sposnor', 'LR_number', 'committees', 'effective_date', 'summary', 'bill_url']]    
    session_actions = [['bill_number', 'session', 'session_year', 'session_type', 'chamber', 'action_date', 'action', 'journal_page', 'order']]
    
    print("\n ------------------- Now Scraping the {} Session ---------------------- \n".format(ga_num))    
    
    ### Get all urls for a session-year, including special sessions
    session_urls = get_session_bills(ga_num)

    #### Loop through bills
    num = 1
    total = len(session_urls)
    for bill_row in session_urls:
        
        bill_data = get_bill_data(bill_row)
        
        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n URL: {} \n **********".format(num, total, bill_row[0], bill_row[4]))
            num += 1
            continue       
            
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
    
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[3], bill_row[4]))
        num += 1
        
    with open("Senate/MO_Bill_Details_" + ga_num + "_Senate.csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("Senate/MO_Bill_Histories_" + ga_num + "_Senate.csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- {} SENATE Session SCRAPED + DATA SAVED  -------------\n\n\n".format(ga_num))   


print("  ********************************** ALL DONE ********************************** ")
