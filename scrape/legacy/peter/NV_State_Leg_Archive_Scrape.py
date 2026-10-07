# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape NEVADA Legislation 1995 -- 2010 ~~~~~~~~~~~~

# ********** 1985+ is possible...


@author: PB
"""

##### NOTES:
# Use Non-Archive Scrape File for Bills After 2010
#
# ************************
#~~~ 1985 to 1993 Sessions ~~~
## Loop through iterating bill numbers and break after three consecutive errors
## https://www.leg.state.nv.us/Session/63rd1985/reports/HistoryLibraryNELIS.cfm?SessionNumber=Nelis_85R&DocumentType=AB&BillNo=700
#
#~~~ 1995 to Present ~~
## Use the Bill Lists provdied in each tab
# ---> Need to update this to better oranize cosponsors (see, e.g., https://www.leg.state.nv.us/Session/75th2009/Reports/history.cfm?ID=284)
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

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/NV/')

#####################################
##### Extract Session Information
#######################################
# Format: ['27th', '2013', 'S', 'https://www.leg.state.nv.us/App/NELIS/REL/27th2013Special']

session_search = urllib.request.urlopen('https://www.leg.state.nv.us/Session/', timeout = 30) 
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
sessions = []
session_list = session_search_soup.findAll('a', href = re.compile('/Session/'))
for i in range(0, len(session_list)):
    s_url = 'https://www.leg.state.nv.us' + session_list[i]['href']
    s_text = session_list[i].find('div').get_text()
    s_text = s_text.split(' ', 2)
    s_num = s_text[0]
    s_year = re.sub('\\(|\\)', '',s_text[1]) 
    s_type = re.sub('Special Session', 'S', s_text[2])
    s_type = re.sub('Session', 'R', s_type)
    sessions.append([s_num, s_year, s_type, s_url])

### Drop Previously Scraped
sessions = [s for s in sessions if 'NV_Bill_Details_' + s[1] + "_" + s[2] + '.csv' not in os.listdir('.')]

### Drop Current Session
this_year = datetime.datetime.now().year
sessions = [s for s in sessions if int(s[1]) < this_year]
print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))

### Drop Years BEFORE 1995
sessions = [s for s in sessions if int(s[1]) >= 1995]
print("\n\n\t ~~~~ DROPPING SESSIONS PRECEDING 1995 ~~~~ \n\n")


del s_text, s_num, s_year, s_type, s_url, this_year, session_search, session_search_soup, session_list

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s = sessions[-1]

def get_session_bills(s): 
    
    s_num, s_year, s_type, s_url = s

    print('\n~~~~ Gathering Bill URLs for the {} {} Session ({})~~~~\n'.format(s_num, s_type, s_year))
    
    session_bills = []
    
    if int(s_year) > 1999:
        ### Search By Bill Types
        url_full = s_url + '/Reports/HistListBills.cfm?DoctypeID={}'
    
        ### Loop through Bill Types (1 - 9, check for none of type)
        for bt in range(1, 10):
            list_page = requests.get(url_full.format(bt))   
            time.sleep(1)
            list_soup = BeautifulSoup(list_page.content, 'lxml')    
            
            for bill in list_soup.findAll('a', href = re.compile('history.cfm\\?ID=')):
                session_bills.append([bill.get_text().strip(), s_num, s_year, s_type, s_url + '/Reports/' + bill['href']])
            
            print(' -- {}/9'.format(bt))
    elif int(s_year) == 1999:

        url_full = s_url + '/Reports/HistListBills.cfm?Doctype={}'
    
        for bt in range(1, 10):
            list_page = requests.get(url_full.format(bt))   
            time.sleep(1)
            list_soup = BeautifulSoup(list_page.content, 'lxml')    
            
            for bill in list_soup.findAll('a', href = re.compile('history.cfm\\?ID=')):
                session_bills.append([bill.get_text().strip(), s_num, s_year, s_type, s_url + '/Reports/' + bill['href']])
            
            print(' -- {}/9'.format(bt))
            
    elif int(s_year) == 1997:
        
        stems = ['abResults.cfm', 'ARResults.cfm?bill_id_id_name=HJR', 'ARResults.cfm?bill_id_id_name=HR', 'ARResults.cfm?bill_id_id_name=HCR',
                 'sbResults.cfm', 'SRResults.cfm?bill_id_id_name=SJR', 'SRResults.cfm?bill_id_id_name=SR', 'SRResults.cfm?bill_id_id_name=SCR']    
        num = 1
        for stem in stems:
            list_page = requests.get(s_url + '/tracking/' + stem)   
            time.sleep(1)
            list_soup = BeautifulSoup(list_page.content, 'lxml')    
            
            for bill in list_soup.findAll('a', href = re.compile('Detail.cfm.+ID=')):
                session_bills.append([bill.get_text().strip(), s_num, s_year, s_type, s_url + '/tracking/' + bill['href']])
            
            print(' -- {}/8'.format(num))    
            num += 1    
            
    elif int(s_year) == 1995:
        
        ## If going back further, would need to change stems and bill_url_stem to rotate sessions
        stems = ['/95AB.cfm', '/95AJR.cfm', '/95ACR.cfm', '/95AR.cfm', '/95SB.cfm', '/95SJR.cfm', '/95SCR.cfm', '/95SR.cfm']
        bill_url_stem = 'https://www.leg.state.nv.us/Session/68th1995/reports/HistoryLibraryNELIS.cfm?SessionNumber=Nelis_95R&DocumentType={}&BillNo={}'
        num = 1
        for stem in stems:
            bt_stem = re.sub('/95|.cfm', '', stem)
            list_page = requests.get(s_url + stem)   
            time.sleep(1)
            list_soup = BeautifulSoup(list_page.content, 'lxml')    
            for bill in list_soup.findAll('a', href = re.compile('95bills')):
                bn = re.sub(bt_stem + '|_.+|[A-Z]+$', '', bill.get_text())
                this_url = bill_url_stem.format(bt_stem, int(bn))
                session_bills.append([bt_stem + bn, s_num, s_year, s_type, this_url])
            
            print(' -- {}/8'.format(num))  
            num += 1
            
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
    time.sleep(.5)
    
    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)    
    

#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = session_urls[0]
    
def get_bill_data(bill_row):    
        
    ### Scrape Bill Page 
    bill_num, s_num, s_year, s_type, bill_url = bill_row
    
    ### Get HTML, Soup    
    bill_soup = get_page_soup(bill_url)
    
    ### If Error, Exit
    if bill_soup == 'HTTP Error' or (bill_soup.find('title') is not None and bill_soup.find('title').text == 'Error'):
        return('HTTP Error')
         
    ## Get Basic Data
    title = ''

    summary = bill_soup.find(['em', 'i'], text = re.compile('BDR'))
    if summary is None:
        summary = bill_soup.find(['em', 'i'])
        
    if summary is not None:
        summary = summary.get_text().replace('\xa0', ' ').strip()
    else:
        summary = bill_soup.find(['b'], text = re.compile('Summary:'))
        summary = summary.nextSibling.strip()

    intro_date = bill_soup.find(['strong', 'b'], text = re.compile('Introduced'))
    if int(s_year) == 1997:
        intro_date = intro_date.nextSibling.strip()
        intro_date = datetime.datetime.strptime(intro_date, '%m/%d/%y').strftime('%Y-%m-%d')    
    elif int(s_year) <= 2002:
        intro_date = intro_date.nextSibling.strip()
        intro_date = datetime.datetime.strptime(intro_date, '%b %d, %Y').strftime('%Y-%m-%d')    
    else:
        intro_date = re.sub('\r\n|\t', '', intro_date.parent.get_text()).strip()
        intro_date = re.sub('Introduced in.+ on:|Introduced in.+ on|Introduced.+:|Introduced on', '', re.sub('\s\s+|\\.', ' ', intro_date)).strip()
        #intro_date = intro_date.split(s_year)[0] + s_year
        intro_date = datetime.datetime.strptime(intro_date, '%b %d, %Y').strftime('%Y-%m-%d')    

    digest = ''
    #digest = bill_soup.find('div', text = re.compile('Digest:'))
    #digest = digest.findNextSibling('div').find('span').get_text().strip()
    #digest = re.sub("\r\n|\n|\t", " ", digest)
    #digest = re.sub("\xa0|\s\s+", " ", digest)

    ### Primary SPonsor always first, sometimes inclues all sponsors
    primary_sponsor = bill_soup.find(['strong', 'b'], text = re.compile('^By$|^By:$|^By: $'))
    if primary_sponsor is None:
        if int(s_year) <= 1997:
             primary_sponsor = bill_soup.find(['strong', 'b'], text = re.compile('Introduced By|Introduced by'))
             primary_sponsor = primary_sponsor.nextSibling.strip()
             primary_sponsor = '; '.join([i.strip() for i in re.split('\r\n|\n', primary_sponsor)])
        else:
            primary_sponsor = ''
    else:
        primary_sponsor = re.sub('By: |By:|By', '', primary_sponsor.parent.get_text()).strip()
        primary_sponsor = re.sub('\\(Bolded.+\\)', '', primary_sponsor).strip()
        primary_sponsor = re.split(s_year + "|" + str(int(s_year) -1 ), primary_sponsor)
        primary_sponsor = primary_sponsor[len(primary_sponsor) - 1].strip()
        if '\r\n' in primary_sponsor:
            primary_sponsor = '; '.join([i.replace(',', '').strip() for i in re.split('\r\n|\n', primary_sponsor) if i.strip() != ''])
            # **** Need to change this to work for cosponsors (e.g., https://www.leg.state.nv.us/Session/75th2009/Reports/history.cfm?ID=284)
        else:
            primary_sponsor = '; '.join([i.strip() for i in primary_sponsor.split(',') if i.strip() != ''])
        ### For some years primary are bolded, but not always... 
        
    fiscal_note1 = bill_soup.find(text = re.compile('Effect on Local.+:'))
    fiscal_note2 = bill_soup.find(text = re.compile('Effect on Sta.+:'))
    if fiscal_note1 is None and fiscal_note2 is None:
        fiscal_note = ''
    elif fiscal_note1 is None:
        fiscal_note = fiscal_note2.strip()
    elif fiscal_note2 is None:
        fiscal_note = fiscal_note1.strip()
    else:
        fiscal_note = '; '.join([fiscal_note1.strip(), fiscal_note2.strip()])

    ## Actions ---- Format switches in 2002 and again in 1997
    bill_actions = []
    action_table = bill_soup.find(['strong', 'b'], text = re.compile('Bill History'))
    chamber = ''   
    date = ''    
    
    if int(s_year) == 2010:
        order = 1
        sub_table = action_table.findParents('p')[0].findNextSibling('table')
        while True:
            sub_rows = sub_table.findAll('tr')
            date = sub_rows[0].get_text().strip()
            date = datetime.datetime.strptime(date, '%b %d, %Y').strftime('%Y-%m-%d')  
            for li in sub_rows[1].findAll('li'):
                action = li.get_text().strip()
                bill_actions.append([bill_num, s_num, s_year, s_type, chamber, date, action, order])
                order += 1
            if sub_table.findNextSibling().name == 'table':
                sub_table = sub_table.findNextSibling()
            else:
                break
           
    elif int(s_year) >= 2003:
        order = 1
        action_table = action_table.findParents('table')[0]
        #if action_table is not None:
        action_rows = action_table.findAll('tr')
        for row in action_rows[1:]:
            ## Check if Date Row
            date_check = row.find('p')
            if date_check is not None:
                date = date_check.get_text().strip()
                date = ' '.join([i.strip() for i in re.split(', | |\\.|,', date, 2)])
                date = datetime.datetime.strptime(date, '%b %d %Y').strftime('%Y-%m-%d')  
                continue
            elif row.get_text().strip() == '':
                continue
            else:   #elif row.find('li') is not None:
                sub_rows = row.findAll('li')
                for sr in sub_rows:
                    action = sr.get_text().replace('Â\xa0', ' ').replace('\xa0', ' ')
                    bill_actions.append([bill_num, s_num, s_year, s_type, chamber, date, action, order])
                    order += 1
           
    elif int(s_year) >= 1999:
        ### Iterate through rows
        order = 0
        if action_table is None:
            action_table = bill_soup.find('hr')
        row = action_table.nextSibling.nextSibling
        while True: 
            if row ==  ' ' or row == '\n':
                row = row.nextSibling
                continue
            elif row.name == 'p':
                date = row.get_text().strip()
                date = ' '.join([i.strip() for i in re.split(', | |\\.|,', date, 2)])
                date = datetime.datetime.strptime(date, '%b %d %Y').strftime('%Y-%m-%d')  
                row = row.nextSibling
                continue
            elif row.name == 'li':
                action = row.get_text().replace('Â\xa0', ' ').replace('\xa0', ' ')
                order += 1
            elif 'br' in row.name:
                break
            bill_actions.append([bill_num, s_num, s_year, s_type, chamber, date, action, order])
            row = row.nextSibling
            
    elif int(s_year) == 1997:
        
        action_table = bill_soup.find('ul')
        order = 1
        for row in action_table.findAll('li'):
            row_text = row.text.strip()
            if row_text == '':
                continue
            if not re.search('[A-Za-z]', row_text) and not re.search('\r\n', row_text):
                continue
            date, action = [i.strip() for i in row_text.split('\r\n')]
            date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')  
            action = action.replace('\xa0', ' ')
            bill_actions.append([bill_num, s_num, s_year, s_type, chamber, date, action, order])
            order += 1
            
    else:
        action_table = action_table.findParents('table')[0]
        #if action_table is not None:
        action_rows = action_table.findAll('tr')
        order = 1
        for row in action_rows[1:]:
            if row.get_text().strip() == '':
                continue
            cells = row.findAll('td')
            date = cells[0].get_text().strip()
            if date != '':
                date = datetime.datetime.strptime(date, '%b %d, %Y').strftime('%Y-%m-%d')  
            else:
                date = bill_actions[-1][5]
            action = cells[1].get_text().strip()
            bill_actions.append([bill_num, s_num, s_year, s_type, chamber, date, action, order])
            order += 1

    ### Add in Hearings
    ### Generic Indicator for Committee Action
    any_h_hearing = 0
    any_s_hearing = 0
    past_hearings = bill_soup.find(['strong', 'b'], text = re.compile('Past Hearings'))
    if past_hearings is not None:    
        past_hearings = past_hearings.findParents('table')[0].findAll('tr')
        for h in past_hearings:
            if 'Past Hearings' in h.get_text().strip() or h.get_text().strip() == '':
                continue
            else:
                if 'Assembly' in h.get_text():
                    any_h_hearing = 1
                if 'Senate' in h.get_text():
                    any_s_hearing = 1
    elif int(s_year) == 1997:
         past_hearings = bill_soup.find(['strong', 'b'], text = re.compile('Heard in .+ Comm'))
         if past_hearings is not None:
             for h in past_hearings.findParents('table')[0].findAll('tr')[1:]:
                 h_text = h.get_text().strip()
                 if 'Assembly' in h_text:
                     h_text = [i.strip() for i in re.split('\n', h_text, 1) if i.strip() != '']
                     any_h_hearing = max(any_h_hearing, len(h_text) - 1)
                 if 'Senate' in h_text:
                     h_text = [i.strip() for i in re.split('\n', h_text, 1) if i.strip() != '']
                     any_s_hearing = max(any_s_hearing, len(h_text) - 1)
                     
    elif int(s_year) == 1995:
        ### Can definitely code if received hearing in main chamber, not necessarily in oppo chamber
        any_h_hearing = ''
        any_s_hearing = ''
        past_hearings = bill_soup.find(text = re.compile('Dates discussed'))
        if past_hearings is not None and bill_num[0:1] == 'A':
            any_h_hearing = 1
        if past_hearings is not None and bill_num[0:1] == 'S':
            any_s_hearing = 1   
            
    ### Dates are wrong in old data... so these get fed in in wrong order    
    #    hearing_info = []
    #    past_hearings = bill_soup.find('strong', text = re.compile('Past Hearings'))
    #    if past_hearings is not None:    
    #        past_hearings = past_hearings.findParents('table')[0]
    #    
    #        for hearing in past_hearings.findAll('tr')[1:]:
    #            if'Past Hearings' in hearing.get_text().strip():
    #                continue
    #            hearing_cells = hearing.findAll('td')
    #            body = re.sub('\r|\n|\t', '', hearing_cells[0].get_text().strip()).replace('\xa0', ' ')
    #            outcome = hearing_cells[2].get_text().strip()
    #            h_action = '{} Committee Hearing ~ Outcome: {}'.format(body, outcome)
    #            h_date = re.sub('\d\d:.*|Minutes', '', hearing_cells[1].get_text().strip()).strip()
    #            h_date = datetime.datetime.strptime(h_date, '%b-%d-%Y').strftime('%Y-%m-%d')  
    #            hearing_info.append([bill_num, s_num, s_year, s_type, '', h_date, h_action, ''])
    
    ### Combining and Sorting + Adding in New Order Variables
    #    if hearing_info != []:
    #        bill_actions = sorted(bill_actions + hearing_info, key = lambda x: datetime.datetime.strptime(x[5], '%Y-%m-%d'))    
    #        bill_actions = [x[0:7] + [index + 1] for index, x in enumerate(bill_actions)]
    
    ### OUTPUT 
    bill_details = [bill_num, s_num, s_year, s_type, intro_date, primary_sponsor, title, any_h_hearing, any_s_hearing, fiscal_note, summary, digest,  bill_url]
    
    return([bill_details, bill_actions])
        
########################################################
############## SCRAPE SESSION(S)
############################################
# bill_row = session_urls[0]
# s = sessions[0]
    

### *** LOOPING THROUGH INDIVIDUAL YEARS and Aggregating Regular + Special Sessions ***
for s in sessions:

    s_num, s_year, s_type, s_url = s

    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'session_year', 'session_type', 'intro_date', 'sponsors', 'title', 'hcomm_hearing', 'scomm_hearing', 'fiscal_notes', 'summary', 'digest', 'bill_url']]    
    session_actions = [['bill_number',  'session', 'session_year', 'session_type', 'chamber', 'action_date', 'action', 'order']]
    
    print("\n ------------------- Now Scraping the {} {} Session ({}) ---------------------- \n".format(s_num, s_type, s_year))    
    
    ### Get all urls for a session-year, including special sessions
    session_urls = get_session_bills(s)

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
    
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[0], bill_row[4]))
        num += 1
        
    with open("NV_Bill_Details_" + s_year + "_" + s_type + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("NV_Bill_Histories_" + s_year + "_" + s_type  + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n -------------{} {} Session ({}) SCRAPED + DATA SAVED  -------------\n\n\n".format(s_num, s_type, s_year))   


print("  ********************************** ALL DONE ********************************** ")
