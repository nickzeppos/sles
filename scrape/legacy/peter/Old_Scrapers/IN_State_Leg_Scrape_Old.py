# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape Indiana Bills

@author: PB
"""

##### NOTES: ************** This No Longer Works ******************
#
# ~~~~ THE NON-ARCHIVE SCRAPER:
# ------> it runs, but skips data (e.g., breaks on unloaded sponsors) or misses most bill's actions ********************
# ------> See new script for backdoor API access method?  
#
# ~~~~ THE ARCHIVE SCRAPER
# -----> 1999 NO LONGER POSTED
# -----> Also: Need to fix everything after the session links part
# -----> bill and resolution pages are sepearate now, so can just go to the different pages
# -----> But should work with minor adjustments... 
#
# THERE IS AN API: http://docs.api.iga.in.gov/introduction.html
# (Presumably for 2014+)
###########################

import csv
import os
import urllib
from bs4 import BeautifulSoup
import time
import datetime
import re

from selenium import webdriver
from selenium.webdriver.chrome.options import Options  
# from selenium.webdriver.firefox.options import Options  
# from selenium.webdriver.safari.options import Options  
os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/IN/')

#######################
##### Extract Session Links
########################

##### 1999 - 2013
#sessions_archive = urllib.request.urlopen('http://in.gov/legislative/2414.htm')
#archive_soup = BeautifulSoup(sessions_archive, 'lxml')
#
#sessions_archive = [t.get_text() for t in archive_soup.find('div', id = 'col2content').findAll('h3') if int(t.get_text()[:4]) >= 1999 ]
#archive_urls = [h['href'] for h in archive_soup.findAll('a', href = re.compile('billinfo\\?year')) if 'request' not in h['href']]
#
#if len(sessions_archive) != len(archive_urls):
#    print("\n\n\n ******** SESSION ISSUE -- SESSION NAMES AND URLS DO NOT MATCH  ************** \n\n\n")

### Archive Moved... URLS TO BILL PAGES for 2000 to 2013
# ***** 1999 NOT INCLUDED = SAVING OLD DATA ***********
sessions_archive = [str(y) for y in range(2000, 2014)]
archive_urls = ["http://iga.in.gov/legislative/archive/bills/{}/".format(y) for y in range(2000, 2014)]


##### 2014+ -- By following link to THIS YEAR it will hide that year in the list
this_year = datetime.datetime.today().year
sessions_new = urllib.request.urlopen('http://iga.in.gov/legislative/{}/session/home/'.format(this_year))
new_soup = BeautifulSoup(sessions_new, 'lxml')   

sessions_new = [re.sub("Archive |Current ", "", t.get_text().strip()) for t in new_soup.findAll('li', {'class':'session-item'})]
new_urls = ['http://iga.in.gov' + t.find('a')['href'] for t in new_soup.findAll('li', {'class':'session-item'})]

sessions_new = [i for i in sessions_new if 'Previous' not in i]
new_urls = [i for i in new_urls if 'archive' not in i]

#### Combine 
sessions = sessions_archive + sessions_new
sessions = [re.sub(' Regular Session| Session', '', i) for i in sessions]
session_urls = archive_urls + new_urls

### Drop Previously Scraped
keep_index = [index for index, s in enumerate(sessions) if 'IN_Bill_Details_' + s.replace(' ', '_') + '.csv' not in os.listdir('.')]
sessions = [s for index, s in enumerate(sessions) if index in keep_index]
session_urls = [url for index, url in enumerate(session_urls) if index in keep_index]


#######################################################
########## GET BILL URLS VIA FTP SITE FOR A SESSION
#####################################################
# session = sessions[17]
# session_url = session_urls[17]

def get_session_bills(session, session_url): 

    year = session[0:4]
    if int(year) >= 2014:
        bill_list_url = session_url.replace("session/home", "bills")
        res_list_url = session_url.replace("session/home", "resolutions")
    else:
        bill_list_url = session_url + '&request=all'
        res_list_url = session_url + '&request=getAllResolutions'
    
    ###### Get Bill and Resolution Pages
    bill_list_page = urllib.request.urlopen(bill_list_url, timeout = 30)
    bill_list_soup = BeautifulSoup(bill_list_page, 'lxml')   
    time.sleep(3)
    res_list_page = urllib.request.urlopen(res_list_url, timeout = 30)
    res_list_soup = BeautifulSoup(res_list_page, 'lxml')   
    time.sleep(3)
    
    ############
    ### Extract URLs to Bill Pages + Basic Info --- TWO DIFFERENT VERSIONS
    #############
    
    all_urls = []
    if int(year) >= 2014:
        url_search = 'bills/house/[0-9]|bills/senate/[0-9]|simple|concurrent|joint/'
        url_stem = 'http://iga.in.gov'
    else:
        url_search = 'docno'
        url_stem = 'http://www.in.gov'

    all_bills = bill_list_soup.findAll('a', href = re.compile(url_search))
    all_resolutions = res_list_soup.findAll('a', href = re.compile(url_search))
    
    if int(year) >= 2014:
        for item in all_bills:
            bill_num = item.get_text().split(':')[0].strip().replace(' ', '')
            short_title = item.get_text().split(':')[1].strip()
            this_url = url_stem + item['href']
            all_urls.append([bill_num, session, short_title, this_url])
        for item in all_resolutions:
            bill_num = item.get_text().replace(' ', '')
            short_title = item.parent.get_text().split(':')[1].strip()
            this_url = url_stem + item['href']
            all_urls.append([bill_num, session, short_title, this_url])
    else:
        for item in all_bills + all_resolutions:
            bill_num = item.get_text().replace(' ', '')
            short_title = re.sub('\\.$', '', item.parent.get_text().split(' -- ')[1].strip())
            this_url = url_stem + item['href']
            all_urls.append([bill_num, session, short_title, this_url])
         
    return(all_urls)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_url = 'http://iga.in.gov/legislative/2017/bills/senate/1'
# bill_info = session_bills[0]

def get_bill_data(bill_info):    
    
    bill_num = bill_info[0]
    this_session = bill_info[1]
    short_title = bill_info[2]
    bill_url = bill_info[3]
    year = this_session[0:4]
    
    bill_actions = []
        
    ##### Scraper for new years having issues -- quick fixing with Selenium.. probably not most efficient
    ##### --- Also changed all the findall('li') below to findall('a')
    ##### --- AND action line
    #    try:
    #        bill_page = urllib.request.urlopen(bill_url, timeout = 10)  
    #    except urllib.error.HTTPError as e:
    #        print('\n\n' + str(e.code) + ' Error ---- Skipping: ' + bill_url + '\n\n')
    #        return('HTTP Error')
    #    except:
    #        try:
    #            print("\n ~~> Retrying Bill Request")
    #            time.sleep(15)
    #            bill_page = urllib.request.urlopen(bill_url, timeout = 30)
    #        except:
    #            print("\n ~~> Retrying Bill Request x 2")
    #            time.sleep(60)
    #            bill_page = urllib.request.urlopen(bill_url, timeout = 60)
    #    time.sleep(1)
    #    
    #    ### Get Bill Data    
    #    bill_soup = BeautifulSoup(bill_page, 'lxml')
    
    
    ### Prep Selenium (remember close driver at end) -- Need to switch options at top if using chrome v firefox
    # --- Chrome has been having issues with popups for a keychain password..
    if int(year) >= 2014:
        driver_options = Options()  
        driver_options.add_argument("--headless")  
        #driver = webdriver.Safari()  
        driver = webdriver.Chrome(options=driver_options)  
    
    if int(year) >= 2014:
        
        #### Get Page + Soup
        driver.get(bill_url)
        time.sleep(2.5)
        bill_soup = BeautifulSoup(driver.page_source, 'lxml')
        
        #### Check if Data
        if bill_num == 'SB2' and year == 2014:
            pass
        elif bill_soup.find('h4', text = re.compile('^Authored by')) is None:
            time.sleep(25)
            driver.get(bill_url)
            time.sleep(10)
            bill_soup = BeautifulSoup(driver.page_source, 'lxml') 
        
        ### Parse
        summary = bill_soup.find('span', id = 'bill-digest').get_text()
        summary_invisible = bill_soup.find('span', id = 'digest-remaining-words')
        if summary_invisible is not None:
            summary = summary + ' ' + summary_invisible.get_text()
                
        ### Authors, Coauthors, Sponsors
        authors = bill_soup.find('h4', text = re.compile('^Authored by'))
        if authors is not None:
            authors = authors.findNext('ul')
            #authors = '; '.join([re.sub('\\.$|,', '', a.get_text().strip()) for a in authors.findAll('li')])
            authors = '; '.join([re.sub('\\.$|,', '', a.get_text().strip()) for a in authors.findAll('a')])
        else:
            authors = ''
            
        coauthor_tag = bill_soup.find('h4', text = re.compile('Co-Authored by'))
        coauthors = ''
        if coauthor_tag is not None:
            #coauthor_tag = coauthor_tag.findNext('ul').findAll('li')
            coauthor_tag = coauthor_tag.findNext('ul').findAll('a')
            coauthors = '; '.join([re.sub('\\.$|,', '', a.get_text().strip()) for a in coauthor_tag])
        
        ### Sponsors appear to be all from the opposing chambere, so coauthor probably = in-chamber cosponsor
        sponsor_tag = bill_soup.find('h4', text = re.compile('Sponsored by'))
        cosponsors = ''
        if sponsor_tag is not None:
            #sponsor_tag = sponsor_tag.findNext('ul').findAll('li')
            sponsor_tag = sponsor_tag.findNext('ul').findAll('a')
            cosponsors = '; '.join([re.sub('\\.$|,', '', a.get_text().strip()) for a in sponsor_tag])
                
        ##### Actions
        # -- http://iga.in.gov/legislative/2018ss1/bills/HB1230.02./legislators
        action_table = bill_soup.find('table', {'class':'actions-table table table-striped'})
        if action_table is None:
            action_table = bill_soup.find('table', {'class':'actions-table table'})           
        
        if action_table.get_text().strip() != "None currently available.":
            action_rows = action_table.findAll('tr')
            order = len(action_rows)
            for row in action_rows:
                cells = row.find('td').findAll('span')
                chamber = cells[0].get_text()
                date = datetime.datetime.strptime(cells[1].get_text() , '%m/%d/%Y').strftime('%Y-%m-%d')
                # action = [i for i in row.find('td').get_text().split('\n') if i != ''][-1]
                # action = row.find('b').nextSibling.strip()
                ## Get Action -- Sub out Chamber/Date -- Remove whitespace
                action = row.find('dd').get_text()
                action = re.sub(row.find('b').get_text(), '', action)
                action = re.sub('  +', ' ', action).strip()
                if action == cells[1].get_text():
                    action = ''
                bill_actions.append([bill_num, this_session, date, chamber, action, order])
                order -= 1
        
        ### **** Could also get vote information here or fiscal notes
        
    else:
        
        try:
            bill_page = urllib.request.urlopen(bill_url, timeout = 10)  
        except urllib.error.HTTPError as e:
            print('\n\n' + str(e.code) + ' Error ---- Skipping: ' + bill_url + '\n\n')
            return('HTTP Error')
        except:
            try:
                print("\n ~~> Retrying Bill Request")
                time.sleep(15)
                bill_page = urllib.request.urlopen(bill_url, timeout = 30)
            except:
                print("\n ~~> Retrying Bill Request x 2")
                time.sleep(60)
                bill_page = urllib.request.urlopen(bill_url, timeout = 60)
        time.sleep(1)
        
        ### Get Bill Data    
        bill_soup = BeautifulSoup(bill_page, 'lxml')
        
        
        summary = bill_soup.find(text = re.compile('DIGEST OF '))
        if summary is None and 'Bill Withdrawn' in bill_soup.get_text().strip():
            return("No data")
        elif summary is None and bill_soup.find(text = re.compile('Digest not yet available')) is not None:
            summary = ''
        else:
            if int(year) > 1999:
                while True:
                    summary = summary.parent
                    if summary.name == 'table':
                        break
                summary = re.sub('\\&nbsp', '', summary.get_text()).strip()
                summary = re.sub("DIGEST OF INTRODUCED BILL|DIGEST OF [A-Z]+ [0-9]+", "", summary).strip()
                if summary[0:1] == '(':
                    summary = summary.split(')', 1)[1].strip()
            else:
                summary = bill_soup.findAll('table')
                summary = re.sub('\\&nbsp', '', summary[1].get_text()).strip()
            
        action_url = bill_url.replace('request=getBill', 'request=getActions')
        
        try:
            action_page = urllib.request.urlopen(action_url, timeout = 15)
        except urllib.error.HTTPError as e:
            print('\n\n' + str(e.code) + 'ACTION PAGE Error ---- Skipping: ' + action_url + '\n\n')
            return('HTTP Error')
        except:
            try:
                print("\n ~~> Retrying Action Request")
                time.sleep(10)
                action_page = urllib.request.urlopen(action_url, timeout = 30)
            except:
                print("\n ~~> Retrying Action Request x 2")
                time.sleep(60)
                action_page = urllib.request.urlopen(action_url, timeout = 60)   
        time.sleep(1)
        
        action_soup = BeautifulSoup(action_page, 'lxml')
        
        ### These are alphabetical
        coauthors = action_soup.find('strong', text = re.compile('Authors:'))
        coauthors = coauthors.parent.get_text().replace('\xa0', ' ', ).replace('Authors:', '').strip()
        
        ### Author seems to always be listed in first action...
        authors = action_soup.find('td', text = re.compile('Authored by'))
        if authors is not None:
            authors = authors.get_text().replace('Authored by ', '').strip()
        else:
            authors = action_soup.find('td', text = re.compile(' added as author'))     
            if authors is not None:
                authors = authors.get_text().replace(' added as author', '').strip()
            else:
                print( " \n\n\n ~~~ No Authors! Check action page: {} \n\n\n".format(action_url))
                authors = ''
    
        cosponsors = ''
        
        action_rows = action_soup.findAll('table')[1].findAll('tr')
        order = 1
        for row in action_rows[1:]:
            cells = row.findAll('td')
            date = datetime.datetime.strptime(cells[0].get_text() , '%m/%d/%Y').strftime('%Y-%m-%d')
            chamber = cells[1].get_text()
            action = cells[3].get_text().strip()
            bill_actions.append([bill_num, this_session, date, chamber, action, order])
            order += 1
    
    ### OUTPUT
    bill_details = [bill_num, this_session, short_title, authors, coauthors, cosponsors, summary, bill_url]
    
    if int(year) >= 2014:
        driver.close()
    
    return([bill_details, bill_actions])
        

########################################################
############## SCRAPE SESSION(S)
############################################
# sy = sessions[4]
# sy_url = session_urls[4]
# bill_info = session_bills[845]

for sy, sy_url in zip(sessions, session_urls):
    
    #### Output Lists 
    session_bill_details = [['bill_number', 'session', 'title', 'authors',  'coauthors', 'cosponsors', 'summary', 'bill_url']]    
    session_actions = [['bill_number', 'session', 'action_date', 'chamber', 'action','order']]
    
    print("\n\n ------------------- Now Scraping: Session " + sy + " ---------------------- \n")    

    ### Get all bills for a specific session
    session_bills = get_session_bills(sy, sy_url)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:
        
        bill_data = get_bill_data(bill_info)
        #time.sleep(1)
        
        if bill_data in ["HTTP Error", "No data"]:
            print(" ({}/{}) ~~~~~~~ HTTP Error OR No Data --- SKIPPING ~~~~~~~~".format(num, total))
            num += 1
            continue
            
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
    
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_data[0][0], bill_data[0][7]))
        num += 1
        
    with open("IN_Bill_Details_" + sy.replace(' ', '_') + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("IN_Bill_Histories_" + sy.replace(' ', '_') + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- " + sy + " SCRAPED + DATA SAVED  -------------\n\n\n")


print("  ********************************** ALL DONE ********************************** ")










