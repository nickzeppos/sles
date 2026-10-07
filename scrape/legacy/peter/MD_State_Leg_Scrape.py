#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 13 12:37:40 2019

@author: pb
"""

##### NOTES:
#  Data ranges 1996+, but website format changes starting in 2013.
#  Bulk Data in CSV Format available 2016+ (See Main Page, Open Legislative Data (Lower Left))
# ----> Bulk format would be easy to work with, its just a matter of getting the older stuff into that format... 

# Will need to construct order variable for pre-2012 after the fact
####################

import csv
import os
import urllib
import requests
from bs4 import BeautifulSoup
import time
import datetime
import re

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/MD/')

#######################
##### Extract Session Links
########################

session_search = urllib.request.urlopen('http://mgaleg.maryland.gov/webmga/frmLegislation.aspx?pid=legisnpage&tab=subject3', timeout = 30) 
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.find('select', id = 'ContentPlaceHolder1_cboSession').findAll('option')

session_ids = [s['value'] for s in session_list]
session_names = [s.get_text() for s in session_list]
session_years = [int(s.split(' ')[0]) for s in session_names]

### Combine and Drop Previously Scraped
sessions = [[sid, name, yr] for sid, name, yr in zip(session_ids, session_names, session_years) if 'MD_Bill_Details_' + sid + '.csv' not in os.listdir('.')]

del session_ids, session_names, session_years

#######################################################
########## GET BILL URLS VIA FTP SITE FOR A SESSION
#####################################################
# session = sessions[0]

def get_session_bills(session): 

    s_id = session[0]
    #s_name = session[1]
    #s_year = session[2]
    
    ## Go to Main Search Page
    search_url = 'http://mgaleg.maryland.gov/webmga/frmLegislation.aspx?pid=legisnpage&tab=subject3&ys={}'.format(s_id)
    search_page = requests.get(search_url)
    search_soup = BeautifulSoup(search_page.content, 'lxml')
    
    bill_numbers = []
    
    ##### Create Bill Ranges from Panel on Left Hand Side [e.g, HB0001 - HB1464]
    leftpanel = search_soup.find('div', id = 'LegisLeft')
    leg_boxes = leftpanel.findAll('table', {'class':'box1leg'})
    
    leg_boxes = [i for i in leg_boxes if 'Enacted Legislation' not in i.get_text() and 'Status of All Legislation' not in i.get_text()]
    
    for box in leg_boxes:        
        split_box = re.split('Bills|Joint Resolutions|Simple Resolutions',box.get_text())
        split_box = [i.strip() for i in split_box if i.strip() not in ['House', 'Senate']]
        for part in split_box:
            start_stop = [p.strip() for p in part.split('-')]
            if len(start_stop) == 1:
                #print(' ~~ SOLO BILL: {}'.format(start_stop))
                bill_numbers = bill_numbers + start_stop
            else:
                stem = re.sub('[0-9]+$', '', start_stop[0])    
                start = int(re.sub(stem, '', start_stop[0]))
                stop = int(re.sub(stem, '', start_stop[1]))            
                bill_range = [stem + str(i).zfill(4) for i in range(start, stop + 1) ]
                bill_numbers = bill_numbers + bill_range
    
    return(bill_numbers)


##############################################################
###### Functions to Create the URLs, Scrape A Page, Try Again if Needed, and Return Soup
################################################################
    
def get_page_soup(bill_num, tab, s_id, s_year, parser = 'lxml'):
    
    #### Create URLS --- Tab {1: Summary, 2: Documents, 3: History}
    if s_year >= 2013:
        page_url = 'http://mgaleg.maryland.gov/webmga/frmMain.aspx?id={}&stab=0{}&pid=billpage&tab=subject3&ys={}'.format(bill_num, tab, s_id)
    else:
        page_url = 'http://mgaleg.maryland.gov/webmga/frmMain.aspx?tab=subject3&ys={}/billfile/{}.htm'.format(s_id, bill_num)

    ### Get HTML
    try:
        page = urllib.request.urlopen(page_url, timeout = 15)
    except urllib.error.HTTPError as e:
        return('HTTP Error')
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(15)
            page = urllib.request.urlopen(page_url, timeout = 30)
        except urllib.error.HTTPError as e:
            return('HTTP Error')
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = urllib.request.urlopen(page_url, timeout = 60)
    time.sleep(.5)
    
    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return([page_soup, page_url])
    

#######################
###### Functions to Scrape Individual Bills
#########################
# bill_num = session_bills[496]
# s = sessions[28]
    
def get_bill_data(s, bill_num):    
       
    s_id = s[0]
    s_year = s[2]

    ### Scrape Bill Page 
    bill_soup, bill_url = get_page_soup(bill_num, 1, s_id, s_year)
    
    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')
    elif 'Unable to retrieve the requested information' in bill_soup.get_text():
        return('No Data')
    
    #### NEW BILL PAGE FORMAT: 2013 - PRESENT
    if s_year >= 2013:
        
        ### Main Tab
        bill_headers = bill_soup.findAll('table', {'class':'billheader'})[1].findAll('h3', {'class':'h3billright'})
        
        title = bill_headers[0].get_text().strip()
        primary_sponsor = bill_headers[1].get_text().strip()
        status = bill_headers[2].get_text().strip()
        
        chapter_num = ''
        if status.find('Chapter') != -1:
            chapter_num = status[status.find('Chapter'):].replace('Chapter ', '').strip()
        
        summary_table = bill_soup.find('table', {'class':'billsum'})
        
        summary_row = summary_table.find('th', text = re.compile('Synopsis'))
        summary = ''
        if summary_row is not None:
            summary = summary_row.findNextSibling().get_text().strip()
           
        sponsors_row = summary_table.find('th', text = re.compile('All Sponsors'))
        all_sponsors = ''
        if sponsors_row is not None:
            all_sponsors = sponsors_row.findNextSibling().get_text().strip()

        add_facts_row = summary_table.find('th', text = re.compile('Additional Facts'))
        crossfile_bill = ''
        if add_facts_row is not None:
            if 'Cross-filed with' in add_facts_row.findNextSibling().get_text():
                crossfile_bill = add_facts_row.findNextSibling().find('a').get_text().strip()
                
        committees_row = summary_table.find('th', text = re.compile('Committee'))
        committees = ''
        if committees_row is not None:
            committees = committees_row.findNextSibling().get_text().replace('&nbsp&nbsp', '; ').strip()
            committees = re.sub(';$', '', committees)

        topic_row = summary_table.find('th', text = re.compile('Broad Subject'))
        topics = ''
        if topic_row is not None:
            topics = topic_row.findNextSibling().get_text().replace('&nbsp&nbsp', '; ').strip()
            topics = re.sub(';$', '', topics)
            
        subjects_row = summary_table.find('span', text = re.compile('Narrow Subject'))
        subjects = ''
        if subjects_row is not None:
            subjects = subjects_row.parent.findNextSibling()
            subjects = [i.get_text().strip() for i in subjects if i.get_text() != '']
            subjects = '; '.join(subjects)
        
        #### History Tab ~~ Using the html5lib parser because LXML yields rows from two sibling tables
        bill_actions = []
        action_soup, action_url = get_page_soup(bill_num, 3, s_id, s_year, parser = 'html5lib')
        action_table = action_soup.find('table', {'class':'billgrid'})
        
        ## Only update cell if content changes or is not blank
        if action_table is not None:
            
            action_rows = action_table.findAll('tr')
            first_row_cells = action_rows[1].findAll('td')
            chamber = first_row_cells[0].get_text()
            date = first_row_cells[2].get_text()
            date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            action = first_row_cells[3].get_text().strip()
            bill_actions.append([bill_num, s_id, s_year, chamber, date, action, 1])
            
            order = 2
            for row in action_rows[2:]:
                cells = row.findAll('td')
                if cells[0].get_text().strip() != '':
                    chamber = cells[0].get_text().strip()
                if cells[2].get_text().strip() != '':
                    date = cells[2].get_text().strip()
                    date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
                action = cells[3].get_text().strip()
                bill_actions.append([bill_num, s_id, s_year, chamber, date, action, order])
                order += 1
        
    #### OLD PAGE FORMAT: 1996 -- 2012 -- Using if statements to check if string to account for some hyperlinking, varies across years
    else:
        status = ''    
        committees = ''
        
        title = bill_soup.find('dt', text = re.compile('Entitled'))
        title = title.findNextSibling().get_text().strip()
        
        chapter_num = bill_soup.find(text = re.compile('CHAPTER NUMBER'))
        if chapter_num is not None:
            if isinstance(chapter_num, str): # This will break in python 2
                chapter_num = re.sub('CHAPTER NUM.+ (?=[0-9])', '', chapter_num.strip())
            else:
                chapter_num = re.sub('CHAPTER NUM.+ (?=[0-9])', '', chapter_num.get_text().strip())
        else:
            chapter_num = ''
            
        crossfile_bill = bill_soup.find(text = re.compile('Crossfiled with'))
        if crossfile_bill is not None:
           crossfile_bill = crossfile_bill.findNextSibling().get_text().strip()
           crossfile_bill = crossfile_bill.replace("HOUSE BILL ", "HB").replace("SENATE BILL ", "SB")
        else:
            crossfile_bill = ''

        topics = bill_soup.find(text = re.compile('File Code'))
        if topics is not None:
            topics = topics.find_next_siblings('a')
            topics = '; '.join([a.get_text().strip() for a in topics])
        else:
            topics = ''
            
        summary = bill_soup.find('a', {'name':'Synopsis'})
        if summary is not None:
            summary = summary.findNext('p').get_text().strip()
            summary = re.sub('\r\n|\n', ' ', summary)
        else:
            summary = ''
        
        #### Sponsors listed at top, sometimes at bottom, sometimes with hyperlinks, sometimes not
        #### Sponsor at top isn't always single sponsor. 
        #### Top and Bottom have different capitalizations (By vs by)
        # sponsors =  bill_soup.findAll('dt', text = re.compile('Sponsored by|Sponsored By'))
        primary_sponsor = bill_soup.find('dt', text = re.compile('Sponsored By'))
        primary_sponsor = primary_sponsor.findNextSibling().get_text().strip()
        primary_sponsor = re.sub('\r\n', '', primary_sponsor)
        primary_sponsor = re.sub('\(.*\)', '', primary_sponsor).strip()    
        
        all_sponsors = bill_soup.find('dt', text = re.compile('Sponsored by'))
        if all_sponsors is not None:
            all_sponsors = all_sponsors.find_next_siblings('dd')
            all_sponsors = [re.sub('\s+', ' ', i.find('a').get_text().strip()) for i in all_sponsors]
            all_sponsors = '; '.join(all_sponsors)
        else:
            all_sponsors = ''

        subjects = bill_soup.findAll('a', href = re.compile('/subjects/'))
        if subjects is not None:
            subjects = '; '.join([i.get_text().strip() for i in subjects])
        else:
            subjects = ''
            
        #### History --- All of the history headers are H5's!
        bill_actions = []
        hist_section = bill_soup.find('a', {'name':'History'}).parent
        
        for chamber_action in hist_section.find_next_siblings('h5'):
            chamber = chamber_action.get_text().strip().replace(' Action', '')
            chamber = re.sub('Action after passage in Senate and House', 'Post Passage', chamber)
            
            these_actions = chamber_action.findNextSibling()
            order = ''
            ### Making sure there's data..
            if these_actions.name != 'dl' or these_actions.get_text().strip() in ['No Action', '']:
                continue
            else:
                for date_tag in these_actions.findAll('dt'):
                    date = date_tag.get_text().strip()
                    if date == 'p' or 'dp' in date: # Fix for http://mgaleg.maryland.gov/webmga/frmMain.aspx?tab=subject3&ys=2009rs/billfile/SB0950.htm
                        continue
                    elif date in ['out', 'tution'] and bill_num in ['SB0510']:
                        date = bill_actions[-1][4]
                    elif date == '8)' and bill_num == 'SB0389':
                        continue
                    elif len(date) > 5:
                        date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
                    else:
                        date = datetime.datetime.strptime(date + '/' + str(s_year), '%m/%d/%Y').strftime('%Y-%m-%d')
                    
                    ### Loop Through Actions
                    action_tag = date_tag.nextSibling
                    while True:
                        if action_tag is None or action_tag.name != 'dd':
                            break
                        
                        action = action_tag.get_text().strip()
                        if action == 'Pre-filed':
                            ### Need to lag Pre-files by a year usually -- cross-checking months
                            this_date = date_tag.get_text()
                            next_date = date_tag.findNext('dt').get_text()
                            if int(re.sub('/.+', '', this_date)) > int(re.sub('/.+', '', next_date)):
                                prefile_date = datetime.datetime.strptime(date_tag.get_text() + '/' + str(s_year - 1), '%m/%d/%Y').strftime('%Y-%m-%d')
                                bill_actions.append([bill_num, s_id, s_year, chamber, prefile_date, action, order])
                            else:
                                bill_actions.append([bill_num, s_id, s_year, chamber, date, action, order])
                        else:
                            bill_actions.append([bill_num, s_id, s_year, chamber, date, action, order])
                        ## Move to next action
                        action_tag = action_tag.nextSibling

    ### OUTPUT ~~~ Note: Vote Urls are in the Documents Tab of the Newer Format
    bill_details = [bill_num, s_id, s_year, title, primary_sponsor, all_sponsors, status, crossfile_bill, committees, topics, subjects, summary, bill_url]
    return([bill_details, bill_actions])
        
########################################################
############## SCRAPE SESSION(S)
############################################
# bill_info = session_bills[4202]
# s = sessions[0]
    
for s in sessions:
    
    s_id = s[0]
    s_name = s[1]
    
    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'session_year', 'title', 'main_sponsor', 'all_sponsors', 'status', 'crossfile_bill', 'committees', 'general_topics', 'subjects', 'summary', 'bill_url']]    
    session_actions = [['bill_number', 'session', 'session_year', 'chamber', 'action_date', 'action','order']]
    
    print('\n~~~~ Gathering Bill URLs for the {} ~~~~\n'.format(s_name))
    
    ### Get all bills for a specific session
    session_bills = get_session_bills(s)

    print('\n~~~~ Beginning Individual Bill Scraping for the {} ~~~~\n'.format(s_name))
    
    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_num in session_bills:

        bill_data = get_bill_data(s, bill_num)
        time.sleep(.5)
        
        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n **********".format(num, total, bill_num))
            num += 1
            continue
        elif bill_data == "No Data":
            print(" ********** \n ({}/{}) -- {} -- NO BILL PAGE! --- SKIPPING \n **********".format(num, total, bill_num))
            num += 1
            continue
  
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
    
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_num, bill_data[0][12] ))
        num += 1
        
    with open("MD_Bill_Details_" + s_id + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("MD_Bill_Histories_" + s_id + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- {} SCRAPED + DATA SAVED  -------------\n\n\n".format(s_name))


print("  ********************************** ALL DONE ********************************** ")
