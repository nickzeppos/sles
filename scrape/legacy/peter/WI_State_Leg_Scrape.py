# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape Wisconsin Legislation 1995 - PRESENT ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# Special Sessions designated by Month-Year
####################


import csv
import os
import urllib
# import requests
from bs4 import BeautifulSoup
import time
import datetime
#from dateutil import parser as dateparser
import re

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/WI/')

#####################################
##### Get Session Information
#######################################

### Sessions are years
this_year = datetime.datetime.now().year
sessions = [i for i in range(1995, this_year, 2) if 'WI_Bill_Details_' + str(i) + '.csv' not in os.listdir('.')]

del this_year


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################
    
def get_page_soup(bill_url, parser = 'lxml'):
    
    ### Get HTML
    try:
        page = urllib.request.urlopen(bill_url, timeout = 20)
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
    time.sleep(1)
    
    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)    
    

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s_yr = sessions[0]
# test = get_session_bills(2001)

def get_session_bills(s_yr): 
    
    print('\n\n~~~~ Gathering Bill URLs for the {} Session and Special Sessions ~~~~\n'.format(s_yr))

    session_bills = []
    
    ## URls Stems to List of Chamber Bills and Resolutions
    search_start = 'https://docs.legis.wisconsin.gov/{}/proposals'.format(s_yr)
    start_soup = get_page_soup(search_start)
    
    ### Get URLs to each Subsession (e.g., reg, Jan 2017 special, etc.)
    s_page_urls = start_soup.findAll('a', href = re.compile('\d{4}/proposals/'))
    s_page_urls = [[i.get_text(), 'https://docs.legis.wisconsin.gov' + i['href']] for i in s_page_urls]
    
    ### Looping through session paes, chambers, bill types
    for sess, s_url in s_page_urls:
        
        if 'regular session' in sess.lower():
            s_type = 'RS'
        else:
            s_type = sess.replace(' Special Session', '')
            s_type = 'SS ' + datetime.datetime.strptime(s_type, '%B %Y').strftime('%m/%y')      
        
        print('\n ~~~~~~~~~~~~~~~ {}, {}'.format(s_yr, s_type))        
        
        s_page_soup = get_page_soup(s_url)        
        chamber_urls = s_page_soup.findAll('a', href = re.compile('\d{4}/proposals/.+/asm|\d{4}/proposals/.+/sen'))
        chamber_urls = ['https://docs.legis.wisconsin.gov' + i['href'] for i in chamber_urls]
        
        ### Chambers -- Not all special sessions have both
        for c_url in chamber_urls:
            
            print('\n +++++++++ {}'.format(c_url))
            chamber_page_soup = get_page_soup(c_url)
            bill_list_urls = chamber_page_soup.findAll('a', href = re.compile('\d{4}/proposals/.+/asm/[A-Za-z]+|\d{4}/proposals/.+/sen/[A-Za-z]+'))
            bill_list_urls = ['https://docs.legis.wisconsin.gov' + i['href'] for i in bill_list_urls]
            
            for bl_url in bill_list_urls:
                
                print(' -- {}'.format(bl_url))
                bill_list_soup = get_page_soup(bl_url)
                these_bills = bill_list_soup.find('ul', {'class':['docLinks', 'infoLinks']})
                these_bills = [[re.sub('.+/', '', i['href']), s_yr, s_type, 'https://docs.legis.wisconsin.gov' + i['href']] for i in these_bills.findAll('a')]
                for bill in these_bills:
                    session_bills.append(bill)
            
    return(session_bills)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = test[0]
# bill_row = session_urls[0]
   
def get_bill_data(bill_row):    
        
    ### Scrape Bill Page 
    bill_num, s_yr, s_type, bill_url = bill_row

    ### Get HTML, Soup    
    bill_soup = get_page_soup(bill_url)
    
    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')
    
    ### Standardize bill_num
    bt = re.sub('[0-9]+', '', bill_num)
    bn = re.sub('[A-Za-z]+', '', bill_num)
    bill_num = bt + bn.zfill(4)

    #### Extract Data
    summary = bill_soup.find('div', {'class':'proposalTitle'}).findNextSibling()
    summary = summary.find('p').get_text("~~~")
    summary = summary.split("~~~")[0].strip()
    if re.search('^[Rr]elating to', summary) and re.search(' [Rr]elating to', summary):
        summary = re.sub(' [Rr]elating to.+$', '', summary )

    status = bill_soup.find('div', {'class':'propStatus'})
    if status is not None:
        status = re.sub('Status: ', '', status.get_text().strip())
    else:
        status = ''

    ### Need to get Sponsors out of Actions
    action_table = bill_soup.find('h2', text = re.compile('History')).findNext('table', {'class':'history'})
    if len(action_table.findAll('tr')) == 1:
         bill_details = [bill_num, s_yr, s_type, '', '', '', status, summary, bill_url]
         return([bill_details, []])
    
    sponsors = action_table.find(text = re.compile('ntroduced by|osponsor.+ |ntroduced priv[a-z]+ by')).parent.get_text()
    if 'cospon' in sponsors.lower():
        sponsors = re.sub('\s\s+', ' ', sponsors)
        primary_sponsor = re.sub(',.+|;.+|Introduced by Rep[a-z]+ |Introduced by Sen[a-z]+ |Introduced by ||Introduced priv[a-z]+ by Rep[a-z]+ |Introduced priv[a-z]+ by Sen[a-z]+ ', '', sponsors)
        cosponsors = re.sub('.+[a-z] {},|.+[a-z] {};|.+[a-z] {}'.format(primary_sponsor, primary_sponsor, primary_sponsor), '', sponsors).strip()
        #if '~~~' in cosponsors: cosponsors = re.split('~~~', cosponsors) else: 
        cosponsors = re.split('[Cc]osponsored by Sen[a-z]+ |[Cc]osponsored by Rep[a-z]+ |[Cc]osponsored by', cosponsors)
        outchamber_spon = '; '.join([i.strip() for i in re.split(',| and ', cosponsors[1])] )
        outchamber_spon = re.sub('.+ Rep[a-z]+ |.+ Sen[a-z]+ |\\.$|;$', '', outchamber_spon)
        
        cosponsors = '; '.join([i.strip() for i in re.split(',| and ', cosponsors[0])] )
        cosponsors = re.sub('\\.$|;$', '', cosponsors)
        
    else:
        primary_sponsor = re.sub(',.+|Introduced by Rep[a-z]+ |Introduced by Sen[a-z]+ |Introduced by |Introduced priv[a-z]+ by Rep[a-z]+ |Introduced priv[a-z]+ by Sen[a-z]+ ', '', sponsors)
        cosponsors = re.sub('.+ {},|.+ {}'.format(primary_sponsor, primary_sponsor), '', sponsors).strip()
        cosponsors = '; '.join([i.strip() for i in re.split(',| and ', cosponsors)] )
        #primary_sponsor = re.sub('~~~', '', primary_sponsor)
        #cosponsors = re.sub('~~~', '', cosponsors)
        outchamber_spon = ''
        
    ## Get Action Table                
    action_table = bill_soup.find('h2', text = re.compile('History')).findNext('table', {'class':'history'})
    action_rows = action_table.findAll('tr')
        
    ### Action Scrape for Both Formats
    bill_actions = []
    order = 1
    for row in action_rows[1:]:
        cells = row.findAll('td')
        date, chamber = cells[0].get_text().strip().split(' ')
        date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')      
        action = cells[1].get_text().strip()
        journal_page = cells[2].get_text().strip()
            
        bill_actions.append([bill_num, s_yr, s_type, chamber, date, action, journal_page, order])
        order += 1
        
    #############
    ### OUTPUT 
    ##############
    bill_details = [bill_num, s_yr, s_type, primary_sponsor, cosponsors, outchamber_spon, status, summary, bill_url]
    
    return([bill_details, bill_actions])
        
########################################################
############## SCRAPE SESSION(S)
############################################
# bill_row = session_urls[1153]
# s_yr = sessions[0]
    
for s_yr in sessions:

    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'session_type', 'primary_sponsor', 'cosponsors', 'outchamber_sponsors', 'status', 'summary', 'bill_url']]    
    session_actions = [['bill_number', 'session', 'session_type', 'chamber', 'action_date', 'action', 'journal_page', 'order']]
    
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
            
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[0], bill_row[3]))
        num += 1
        
    with open("WI_Bill_Details_" + str(s_yr) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("WI_Bill_Histories_" + str(s_yr) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- {} Session/Special Sessions SCRAPED + DATA SAVED  -------------\n\n\n".format(s_yr))   


print("  ********************************** ALL DONE ********************************** ")
