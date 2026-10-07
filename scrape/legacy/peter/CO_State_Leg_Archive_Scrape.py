# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape COLORADO Legislation 1998 - 2015 ~~~~~~~~~~~~

** USE NON-ARCHIVE SCRAPER FOR CURRENT/RECENT SESSIONS ** 

@author: PB
"""

##### NOTES:
# Skipping 1997 - No Status/History Information
# 1998/1999 Formats are Different
####################

import csv
import os
import urllib
import requests
import bs4
from bs4 import BeautifulSoup
import time
import datetime
#from dateutil import parser as dateparser
import re
import itertools

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/CO/')


#####################################
##### Extract Session URLS
#######################################
# ** Could just do a range of years, but keeping this way in case something changes down the line....

previous_sessions = urllib.request.urlopen('http://www.leg.state.co.us/clics/cslFrontPages.nsf/PrevSessionInfo?OpenForm', timeout = 30) 
previous_soup = BeautifulSoup(previous_sessions, 'lxml')

### Get Session Years (tables not exclusive to years..)
session_tables = previous_soup.findAll('font', text = re.compile('Legislative Session Information'))
session_years = [i.get_text().replace(' Legislative Session Information', '').strip() for i in session_tables]
all_urls = previous_soup.findAll('a', text = re.compile('Bills|Resolutions|Memorials'))

session_types = {'':'RS', 'a':'RS', 'b':'S1', 'c':'S2', 's':'S1', 's2':'S2'}
session_list = []

for i in range(0, len(session_years)):
    this_year = session_years[i]
    if this_year in ['1997']:
        continue
    else:
        these_pages = [j.get_text() for j in all_urls if this_year in j.get_text()]
        these_types = [j.split(' ')[0] for j in these_pages]
        these_types = [session_types[j[4:]] for j in these_types]
        these_urls = [j['href'] for j in all_urls if this_year in j.get_text()]
        for j in range(0, len(these_pages)):
            if 'http' not in these_urls[j]:
                these_urls[j] = 'http://www.leg.state.co.us' + these_urls[j]
            session_list.append([this_year, these_types[j], these_pages[j], these_urls[j]])

### Drop Previously Scraped
session_list = [s for s in session_list if 'CO_Bill_Details_' + s[0] + '.csv' not in os.listdir('.')]


#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# year = '2012'

### *** Function to scrape a particular list of bills ***
def scrape_bill_list(bill_list):
    
    year, bill_type, session_type, list_url = bill_list

    ### Get HTMl/Soupt
    list_page = requests.get(list_url)    
    time.sleep(.5)
    list_soup = BeautifulSoup(list_page.content, 'lxml')  
    
    # Find Bill Table
    bill_data = []
    bill_table = list_soup.find('table')
    table_rows = bill_table.findAll('tr')
    if len(table_rows) == 0:
        return([])
    
    ## If Data, Loop through Rows
    for row in table_rows:
        cells = row.findAll('td')
        if len(cells) == 1:
            continue
        else:
            bill_num = cells[0].find('a').get_text().replace('.pdf', '').replace('.PDF', '')
            ## Skip Bills that don't match bill type from URL (Relevant for some older sessions)
            if re.sub('[0-9].*', '', bill_num) != bill_type:
                continue
            #bill_pdf_url = 'http://www.leg.state.co.us' + cells[0].find('a')['href']
            title = cells[1].next.next
            if str(title) == '<br/>':
                title = ''
            if title == '':
                sponsors = cells[1].next.next.next
            else:
                if isinstance(cells[1].next.next.next.next, bs4.element.NavigableString):
                    sponsors = cells[1].next.next.next.next
                else:
                    sponsors = re.split(" (?=[A-Z][A-Z]+--)", title, 1)[1]
   
            sponsors, out_chamber_spon = re.split('--+', sponsors)
            
            sponsors = [i.strip() for i in re.split('\\&|and', sponsors) if i.strip() not in ['', '...', '(NONE)']]
            sponsors = '; '.join(sponsors)
            
            out_chamber_spon = [i.strip() for i in re.split('\\&|and', out_chamber_spon) if i.strip() not in ['', '...', '(NONE)']]
            out_chamber_spon = '; '.join(out_chamber_spon)

            i = 2    
            history_url = ''
            while i <= len(cells):
                if cells[i].get_text() == "History":
                    ## need to just extract the target frame
                    history_url = 'http://www.leg.state.co.us' + re.sub('.+target=', '', cells[i].find('a')['href'])
                    break
                else:
                    i += 1
                    
            bill_data.append([bill_num, year, session_type, title, sponsors, out_chamber_spon, history_url])        
    
    return(bill_data)


### *** Function to get URLs to all Lists and Scrape Iteratively ***  
def get_session_bills(year): 

    print('\n~~~~ Gathering Bill URLs for the {} Session(s) ~~~~\n'.format(year))
 
    ## Subset to Appropriate Session-years - Only 1 RS for all post 2000
    sy_rs_urls = [s for s in session_list if s[0] == year and s[1] == 'RS']
    sy_s1_urls = [s for s in session_list if s[0] == year and s[1] == 'S1']
    sy_s2_urls = [s for s in session_list if s[0] == year and s[1] == 'S2']
    
    ## Page Format Differences Between pre-2000 and 2000+
    year_stem = year[2:4]
    scrape_urls = []

    if int(year) >= 2000:    
        ### URL Format Differences between 2000 - 2003 and 2004+
        if int(year) >= 2004:
            base_url = re.sub('csl.nsf.*', 'csl.nsf/bf-1?OpenView&StartKey={}{}-0001&Count=5000', sy_rs_urls[0][3])
            if sy_s1_urls != []:
                s1_url = re.sub('csl.nsf.*', 'csl.nsf/bf-1?OpenView&StartKey={}{}-0001&Count=5000', sy_s1_urls[0][3]) 
            if sy_s2_urls != []:
                s2_url = re.sub('csl.nsf.*', 'csl.nsf/bf-1?OpenView&StartKey={}{}-0001&Count=5000', sy_s2_urls[0][3])  
        else:
            base_url = re.sub('pubhome.nsf.*', 'inetcbill.nsf/bf-1?OpenView&StartKey={}{}-0001&Count=5000', sy_rs_urls[0][3])  
            if sy_s1_urls != []:
                s1_url = re.sub('pubhome.nsf.*', 'inetcbill.nsf/bf-1?OpenView&StartKey={}{}-0001&Count=5000', sy_s1_urls[0][3])  
            if sy_s2_urls != []:
                s2_url = re.sub('pubhome.nsf.*', 'inetcbill.nsf/bf-1?OpenView&StartKey={}{}-0001&Count=5000', sy_s2_urls[0][3])                  
        
        ### Construct URLs to Bill Lists for Each Bill Type
        for bill_type in ['HB', 'SB', 'HCR', 'HJR', 'HR', 'HM', 'HJM', 'SCR', 'SJR', 'SR', 'SM', 'SJM']:
            scrape_urls.append([year, bill_type, 'RS', base_url.format(bill_type, year_stem)]) 
                
        if sy_s1_urls != []:
            for bill_type in ['HB', 'SB', 'HCR', 'HJR', 'HR', 'HM', 'HJM', 'SCR', 'SJR', 'SR', 'SM', 'SJM']:
                scrape_urls.append([year, bill_type, 'S1', s1_url.format(bill_type, year_stem)])          
        
        if sy_s2_urls != []:
            for bill_type in ['HB', 'SB', 'HCR', 'HJR', 'HR', 'HM', 'HJM', 'SCR', 'SJR', 'SR', 'SM', 'SJM']:
                scrape_urls.append([year, bill_type, 'S2', s2_url.format(bill_type, year_stem)])      
                
        ### Loop through pages and scarpe Info + URLs
        session_info = []
        for bill_list in scrape_urls:
            
            these_bills = scrape_bill_list(bill_list)
            if these_bills != []:
                for bill in these_bills:
                    session_info.append(bill)
                    
            print(" -- {} {} ~ {}".format(year, bill_list[2], bill_list[1]))
    
    else:
        
        ### Only Reg Sessions for 1998/1999
        ### ** Hist URLs are a bit worthless here as its all on one big (parseable) page
        
        session_info = []
        for bill_list in sy_rs_urls:
            session_type = bill_list[1]
            list_page = requests.get(bill_list[3], headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'})    
            time.sleep(.25)
            list_soup = BeautifulSoup(list_page.content, 'html5lib')  
            bill_table = list_soup.findAll('table')
            if len(bill_table) == 2:
                bill_table = bill_table[1]
                table_rows = bill_table.findAll('tr')
            else:
                bill_table = bill_table[0]
                table_rows = bill_table.findAll('tr')[1:]
                
            for row in table_rows:
                cells = row.findAll('td')
                if year == '1999':
                    bill_num = cells[2].get_text()
                    title = cells[5].get_text()
                    if bill_num[0:1].lower() == 'h':
                        sponsors = cells[4].get_text().replace('\\(None\\)', '')
                        out_chamber_spon = cells[3].get_text().replace('\\(None\\)', '')
                    else:
                        sponsors = cells[3].get_text().replace('\\(None\\)', '').strip()
                        out_chamber_spon = cells[4].get_text().replace('\\(None\\)', '').strip()
                    history_url = 'http://www.leg.state.co.us/preclics/1999/' + cells[0].find('a')['href']    
                elif year == '1998':
                    bill_num = cells[2].get_text()
                    if not re.search('[0-9][0-9]-', bill_num):
                        bt = re.sub('[0-9].*', '', bill_num).strip()
                        bt = bt.upper() + year_stem
                        bn = re.sub('^[a-z]+(?=[0-9])', '', bill_num).zfill(4)
                        bill_num = bt + "-" + bn
                    else: 
                        bill_num = bill_num.replace('\n', '')
                        bill_num = '-'.join([i.zfill(4) for i in bill_num.split('-')])
                        
                    title = cells[4].get_text().strip()
                    sponsors = re.sub('\r\n|\n', '', cells[3].get_text().strip())
                    out_chamber_spon = ''
                    history_url = 'http://www.leg.state.co.us/preclics/1998/' + cells[0].find('a')['href']

                session_info.append([bill_num, year, session_type, title, sponsors, out_chamber_spon, history_url])        
    
    ## Return bill Number, URL, Description
    return(session_info)  
    

#######################
###### Functions to Scrape Individual Bills
#########################
# s = sessions[0]
    
def get_bill_history(bill_row):    

    bill_num = bill_row[0]
    sy = bill_row[1]
    stype = bill_row[2]
    hist_url = bill_row[6]
    
    ### Get HTML
    try:
        hist_page = urllib.request.urlopen(hist_url, timeout = 15)
    except urllib.error.HTTPError: # as e
        return('HTTP Error')
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(15)
            hist_page = urllib.request.urlopen(hist_url, timeout = 30)
        except urllib.error.HTTPError: # as e
            return('HTTP Error')
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            hist_page = urllib.request.urlopen(hist_url, timeout = 60)
    time.sleep(.5)    
    
    hist_soup = BeautifulSoup(hist_page, 'lxml')
    date_note = hist_soup.find('b', text = re.compile('The date the bill'))
    
    ## CHeck if Data
    if hist_soup.find(text = re.compile('No History Found')) is not None:
        return([])
    
    ## Actions 
    history_stuff = date_note.findNext('b')
    history_stuff = re.split('\r\n|\n', history_stuff.get_text())
    
    bill_history = []
    order = 1
    for item in history_stuff:
        date, action = re.split(' ', item, 1)
        if len(date) > 10 and ('AM ' in action or 'PM ' in action):
            date = date.split(':')[0]
            action = re.sub('^AM \d+:\d+ |^PM \d+:\d+ ', '', action)
        date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')  
        bill_history.append([bill_num, sy, stype, date, action, order])
        order += 1
    
    return(bill_history)
        
        
########################################################
############## SCRAPE SESSION(S)
############################################
# bill_num, bill_url, descrip = session_urls[6751]
# s = session_list[0]
# bill_row = session_urls[2]
        
for s in session_list:
    
    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'session_type', 'title', 'sponsors', 'out_chamber_sponsors', 'bill_url']]    
    session_actions = [['bill_number', 'session', 'session_type', 'action_date', 'action', 'order']]
    
    print("\n ------------------- Now Scraping the {} Session(s) ---------------------- \n".format(s[0]))
    
    ### Get all bills for a specific session
    year = s[0]
    session_urls = get_session_bills(year)
    
    ## Drop Duplicates -- Might be one single case
    session_urls.sort()
    session_urls = list(session_urls for session_urls,_ in itertools.groupby(session_urls))
    
    ## Dropping Bills with issues (e.g., server error --> login)    
    session_urls = [i for i in session_urls if i[0] not in ['HJR07-1007']]    
    session_urls = [i for i in session_urls if i[6] not in ['http://www.leg.state.co.us/2002a/inetcbill.nsf/billsummary/27E9CD226BDE2D8387256B8B00692A5D']]
   
    #### Loop through bills
    num = 1
    total = len(session_urls)
    print('\n~~~~ Gathering Bill Histories for the {} Session(s) ~~~~\n'.format(year))

    if int(year) >= 2000:
                
        for bill_row in session_urls:
            
            ## SAVE BILL DETAILS -- Not scraped here, already have data form initial scrape
            session_bill_details.append(bill_row)
    
            ## Get History Info
            bill_hist = get_bill_history(bill_row)
            if bill_hist != [] and bill_hist != 'HTTP Error':
                for hist_row in bill_hist:
                    session_actions.append(hist_row)
        
            print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[0], bill_row[6]))
            num += 1
            
    else:
        
        ## Append Details Data to Main File
        for bill_row in session_urls:
            session_bill_details.append(bill_row)
            
        ### Get History for ALL Bills - Start by Getting House and Senate History Pages
        unique_hist_urls = [re.sub('\\#.+', '', i[6].lower()) for i in session_urls]
        unique_hist_urls = set(unique_hist_urls)
        
        year_stem = year[2:4]
        for hist_url in unique_hist_urls:
            hist_page = urllib.request.urlopen(hist_url, timeout = 30)
            hist_soup = BeautifulSoup(hist_page, 'lxml')
            hist_stuff = hist_soup.find('font', {'size':'4'})
            hist_items = hist_stuff.findAll('a', href = re.compile('[a-z]'))        
            for item in hist_items:
                bill_num = re.sub('.+/|.htm', '', item['href'])
                bt = re.sub('[0-9].*', '', bill_num)
                bt = bt.upper() + year_stem
                bn = re.sub('^[a-z]+(?=[0-9])', '', bill_num).zfill(4)
                bill_num = bt + "-" + bn

                loop_text = item.findNextSiblings('br')
                if len(loop_text) >= 3:
                    loop_text = item.findNextSiblings('br')[2].next
                else:
                    loop_text = item.findNextSiblings('b')[0].findChildren('br')[1].next
                order = 1
                while True:
                    ## Clean if rogue \r\n in text
                    if re.search('\\*\\*+', str(loop_text)):
                        break
                    elif str(loop_text) in ['<br>', '<br/>']:
                        loop_text = loop_text.next    
                    else:
                        try:
                            date, action = re.split('  ', loop_text.strip(), 1)
                            if '\r\n' in date:
                                date, action = re.split('  ', loop_text.strip().split('\r\n')[1], 1)
                            if ' ' not in date or re.search('[a-z]', date):
                                date = re.sub(' [A-Z].+', '', loop_text.strip(), 1).strip()
                                date = re.sub('\s\s+', ' ', date)
                                action = re.sub('\d+ +\d+ ', '', loop_text.strip(), 1).strip()
                        except:
                            date = re.sub(' [A-Z].+', '', loop_text.strip(), 1)
                            action = re.sub('\d+ \d+ ', '', loop_text.strip(), 1).strip()
                        date = datetime.datetime.strptime(date + " " + year, '%m %d %Y').strftime('%Y-%m-%d')  
                        #print([bill_num, year, 'RS', date, action, order])
                        session_actions.append([bill_num, year, 'RS', date, action, order]) 
                        loop_text = loop_text.next    
                        order += 1
        
    ###### SAVE
    with open("CO_Bill_Details_" + year + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("CO_Bill_Histories_" + year + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- {} SESSION SCRAPED + DATA SAVED  -------------\n\n\n".format(year))


print("  ********************************** ALL DONE ********************************** ")
