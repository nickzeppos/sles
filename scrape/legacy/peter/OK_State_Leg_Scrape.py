# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape OKLAHOMA Bills

@author: PB
"""
###########################
##### NOTES:
# ********* 
###########################

import csv
import os
from bs4 import BeautifulSoup, NavigableString
import time
import re
import requests
import urllib
import socket
import datetime

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/OK/')


#######################
##### Extract Session Links
########################

session_search = requests.get('http://www.oklegislature.gov/TextOfMeasures.aspx', timeout = 30) 
session_search_soup = BeautifulSoup(session_search.content, 'lxml')

### Get Sessions
session_list = session_search_soup.find('select', id = 'ctl00_ContentPlaceHolder1_cbxSessionId')
s_dict = {'Regular Session':'RS', 'Special Session':'SS1', 'First Special Session':'SS1', 'Second Special Session':'SS2', 'Third Special Session':'SS3'}
sessions = [[i['value'], int(re.sub(' .+', '', i.text)), s_dict[re.sub('^[0-9]+ ', '', i.text)] ] for i in session_list.findAll('option')]

### Drop Previously Scraped
sessions = [s for s in sessions if 'OK_Bill_Details_{}_{}.csv'.format(s[1], s[2]) not in os.listdir('.')]

### Drop Current Session
this_year = datetime.datetime.now().year
sessions = [s for s in sessions if s[1] < this_year]
print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))

del session_search, session_search_soup, session_list, s_dict, this_year

##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################
    
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
    time.sleep(.75)
    
    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)    



#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s = sessions[-10]

def get_session_bills(s): 
    
    s_id, s_yr, s_type = s
    search_form_url = 'http://www.oklegislature.gov/TextOfMeasures.aspx'
    
    search_page = requests.get(search_form_url)
    search_soup = BeautifulSoup(search_page.content, 'lxml')
    
    form_data = {
        '__EVENTTARGET':'ctl00$ContentPlaceHolder1$cbxSessionId',
        '__VIEWSTATE' : search_soup.select('#__VIEWSTATE')[0]['value'],
        '__VIEWSTATEGENERATOR' : search_soup.select('#__VIEWSTATEGENERATOR')[0]['value'],
        '__EVENTVALIDATION' : search_soup.select('#__EVENTVALIDATION')[0]['value'],
        'ctl00$ContentPlaceHolder1$rbChamber': '',
        'ctl00$ContentPlaceHolder1$format': 'rbPDF',
        'ctl00$ContentPlaceHolder1$cbxSessionId': s_id,
        'ctl00$ContentPlaceHolder1$lbxMeasureStatus': 'INT',
        'ctl00$ContentPlaceHolder1$Button1': 'Search',
        'ctl00_ToolkitScriptManager1_HiddenField': ';;AjaxControlToolkit, Version=3.5.40412.0, Culture=neutral, PublicKeyToken=28f01b0e84b6d53e:en-US:1547e793-5b7e-48fe-8490-03a375b13a33:de1feab2:f9cec9bc:35576c48'
        }
        
    ### Get Bill Data
    bill_urls = []
    for chamber in ['H', 'S']:
        form_data['ctl00$ContentPlaceHolder1$rbChamber'] = chamber
        page = requests.post(url=search_form_url, data=form_data, allow_redirects=True)
        soup = BeautifulSoup(page.content, 'lxml')
        bill_table = soup.find('table', id = 'ctl00_ContentPlaceHolder1_tblTomData')
        for b in bill_table.findAll('tr')[2:-1]:
            if b.text.strip() == '':
                continue
            cells = b.findAll('td', recursive = False)
            bn = cells[0].a.text.strip()
            intro_date = cells[1].h2.text.strip()
            if intro_date != '':
                intro_date = datetime.datetime.strptime(intro_date, '%m/%d/%Y').strftime('%Y-%m-%d')     
            short_title = cells[2].text.strip()
            authors = cells[3].text.strip()
            if authors != '' and not re.search('\\(H\\)|\\(S\\)', authors):
                print('CHECK AUTHORS -- NO CHAMBER INFO  ----->' + bn)
                continue
            else:
                authors = [i.strip() for i in authors.split(',')]
                authors = [re.sub('\\(', ' (', i) for i in authors]
                H_author = '; '.join([i for i in authors if '(H)' in i])
                S_author = '; '.join([i for i in authors if '(S)' in i])
            bill_url = 'http://www.oklegislature.gov/BillInfo.aspx?Bill={}&Session={}'.format(bn, s_id)
            bill_urls.append([bn, s_yr, s_type, s_id, intro_date, H_author, S_author, short_title, bill_url])
        
    ### Drop Repeat/Error Bills
    # --> 2018: All three are missing a number (e.g., HB301 == HB3017); this shows up on actions page under versions; the correct numbers, however, are already in the data --> Drop these repeats
    if s_yr == 2018 and s_type == 'RS':     
        bill_urls = [i for i in bill_urls if i[0] not in ['HB65', 'HB289', 'HB301']]
        

    ### Return
    print(' \n **** ~~~> Found {} BILL URLs for {}-{} **** \n'.format(len(bill_urls), s_yr, s_type))
    return(bill_urls)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_data = get_session_bills(sessions[5])
# bill_info = session_bills[841]
    
def get_bill_data(bill_info):    
    
    bill_num, s_yr, s_type, s_id, intro_date, H_author, S_author, short_title, bill_url = bill_info
    session = '{}-{}'.format(s_yr, s_type)
    
    #### Get Bill Page
    bill_soup = get_page_soup(bill_url)   
    
    #### Check Authors --- Adjust if First Names Present
    H_stem = re.sub(' \\((H|S)\\)', '', H_author).lower()
    S_stem = re.sub(' \\((H|S)\\)', '', S_author).lower()
    
    author1 = bill_soup.find('a', id = 'ctl00_ContentPlaceHolder1_lnkAuth')
    if author1 and "author not found" not in author1.text.strip().lower():
        author1 = author1.text.strip()
        if re.search('\\([a-z][a-z]+|\\([a-z]\\.[a-z]', author1.lower()):
            # Skip if both H and S sponsors match (and H/S spon both exist)
            if H_stem in author1.lower() and S_stem in author1.lower() and H_stem != '' and S_stem != '':
                pass
            elif H_stem != '' and H_stem in author1.lower():
                H_author = author1 + " (H)"
            elif S_stem != '' and S_stem in author1.lower():
                S_author = author1 + " (S)"
    
    author2 = bill_soup.find('a', id = 'ctl00_ContentPlaceHolder1_lnkOtherAuth')
    if author2 and "author not found" not in author2.text.strip().lower():
        author2 = author2.text.strip()
        if re.search('\\([a-z][a-z]+|\\([a-z]\\.[a-z]', author2.lower()):
            if H_stem in author2.lower() and S_stem in author2.lower() and H_stem != '' and S_stem != '':
                pass
            elif H_stem != '' and H_stem in author2.lower():
                H_author = author2 + " (H)"
            elif S_stem != '' and S_stem in author2.lower():
                S_author = author2 + " (S)"
    
    #### Get Coauthors:
    coauthors = ''
    coauthor_table = bill_soup.find('table', id = 'ctl00_ContentPlaceHolder1_TabContainer1_TabPanel6_tblCoAuth')
    if coauthor_table:
        coauthor_rows = coauthor_table.findAll('td', text = re.compile('\\(H\\)|\\(S\\)'))
        if coauthor_rows:
            coauthors = '; '.join([i.text for i in coauthor_rows])
        else:
            coauthor_rows = coauthor_table.findAll('td', text = re.compile('coauthor|cosponsor'))
            if coauthor_rows:
                coauthors = [re.sub('.+coauthor|.+cosponsor', '', i.text).strip() for i in coauthor_rows]
                coauthors = [c.replace('Representative ', '') + " (H)" if 'Representative' in c else c for c in coauthors]
                coauthors = [c.replace('Senator ', '') + " (S)" if 'Senator' in c else c for c in coauthors]
                coauthors = '; '.join(coauthors)
                
    #### Get Actions
    bill_actions = []
    actions_table = bill_soup.find('table', id = 'ctl00_ContentPlaceHolder1_TabContainer1_TabPanel1_tblHouseActions')    
    if actions_table:
        order = 1        
        for row in actions_table.findAll("tr")[2:]:
            if row.text.strip() == '' or row.text.strip() == 'None':
                continue
            cells = row.findAll('td')
            action = cells[0].text.strip()
            journal_page = cells[1].text.strip()
            action_date = cells[2].text.strip()    
            if action_date != '':
                action_date = datetime.datetime.strptime(action_date, '%m/%d/%Y').strftime('%Y-%m-%d')     
            chamber = cells[3].text.strip()
            bill_actions.append([bill_num, session, chamber, action_date, journal_page, action, order])
            order += 1
    
    # Data from Bill Text page
    bill_details = [bill_num, session, intro_date, H_author, S_author, coauthors, short_title, bill_url]
    return([bill_details, bill_actions])
         
    
#######################
###### Scrape Bills using Advanced Bill Search (Need to use for 1991 special)
#########################
    
#, view_state, view_state_gen, event_val
def get_bill_data_advanced_search(bill_info):    
    
    bill_num, s_yr, s_type, s_id, intro_date, H_author, S_author, short_title, bill_url = bill_info
    session = '{}-{}'.format(s_yr, s_type)
    
    ### Get Form Data
    req_url = 'http://webserver1.lsb.state.ok.us/WebApplication2/WebForm1.aspx'    
    advanced_soup = get_page_soup(req_url)
    view_state = advanced_soup.select('#__VIEWSTATE')[0]['value']
    view_state_gen = advanced_soup.select('#__VIEWSTATEGENERATOR')[0]['value']
    event_val = advanced_soup.select('#__EVENTVALIDATION')[0]['value']
    time.sleep(1) 
    
    #### Create URL for Request
    form_data = {
        '__VIEWSTATE': view_state,
        '__VIEWSTATEGENERATOR': view_state_gen,
        '__EVENTVALIDATION':event_val,
        'tbFirst':bill_num,
        'cbxSession':s_id,
        'cbxHouseAuthor':-1,
        'cbxSenAuthor':-1,
        'Button2':'Retrieve',
        'cbxAuthRole':2
    }
    
    #### Get Bill Page
    bill_response = requests.post(req_url, data = form_data)
    time.sleep(2)
    bill_soup = BeautifulSoup(bill_response.content, 'lxml') 
    
    #### Use Previously Scraped Sponsors
    coauthors = bill_soup.find('i', text = re.compile('CoAuthors:'))
    if coauthors:
        count = 0
        while count < 10:
            coauthors = coauthors.nextSibling
            if isinstance(coauthors, NavigableString):
                break
            count+=1
                
    #### Get Actions
    bill_actions = []
    actions_table = bill_soup.find('i', text = re.compile("Bill History"))
    if actions_table:
        actions_table = actions_table.findNext('table') 
        order = 1        
        for row in actions_table.findAll("tr")[1:]:
            if row.text.strip() == '' or row.text.strip() == 'None':
                continue
            cells = row.findAll('td')
            action = cells[5].text.strip()
            chamber = cells[2].text.strip()
            if chamber == "S":
                journal_page = cells[4].text.strip()
            else:
                journal_page = cells[3].text.strip()
                
            action_date = cells[0].text.strip()    
            if action_date != '':
                action_date = datetime.datetime.strptime(action_date.split(' ')[0], '%m/%d/%Y').strftime('%Y-%m-%d')     

            bill_actions.append([bill_num, session, chamber, action_date, journal_page, action, order])
            order += 1
    
    # Data from Bill Text page
    bill_details = [bill_num, session, intro_date, H_author, S_author, coauthors, short_title, bill_url]
    return([bill_details, bill_actions])  
    
    
########################################################
############## SCRAPE SESSION(S)
############################################
# s = sessions[-10]
# bill_info = session_bills[5]
    
for s in sessions:
    
    #### Output Lists
    session_bill_details = [['bill_id', 'session', 'intro_date', 'H_author', 'S_author', 'coauthors', 'title', 'bill_url']]    
    session_actions = [['bill_id', 'session', 'chamber', 'action_date', 'journal_page', 'action', 'order']]
    
    print("\n ------------------- Now Scraping: {}-{} ---------------------- \n".format(s[1], s[2]))
    
    ### Get all bills for a specific session
    session_bills = get_session_bills(s)
          
    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        ### Get Basic Details
        if s[0] == '991X':
            try:
                bill_data = get_bill_data_advanced_search(bill_info) #, view_state, view_state_gen, event_val
            except:
                time.sleep(10)
                bill_data = get_bill_data_advanced_search(bill_info)
        else:
            bill_data = get_bill_data(bill_info)
        
        if bill_data == 'HTTP Error':
            print(" ********** \n ({}/{}) -- HTTP ERROR -- SKIPPING -- {} \n **********".format(num, total, bill_info[8]))
            num += 1
            continue           
        
        ### Get Actions
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
    
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_info[0], bill_info[8]))
        num += 1
        
        
    with open("OK_Bill_Details_{}_{}.csv".format(s[1], s[2]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("OK_Bill_Histories_{}_{}.csv".format(s[1], s[2]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- {}-{} SCRAPED + DATA SAVED  -------------\n\n\n".format(s[1], s[2]))


print("  ********************************** ALL DONE ********************************** ")
