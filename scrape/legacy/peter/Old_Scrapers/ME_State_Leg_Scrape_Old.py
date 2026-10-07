# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape MAINE Bills

@author: PB
"""

##### NOTES:
# *** NEED TO DO THE BELOW THOUGH -- FINAL DISPOSITION NOT ALWAYS RECORDED ON OLDER PAGE... SEE LD2100 IN 120TH ****
#
# Scraping ALL Bill Types for Now -- May Need to Drop Them Down the Line (e.g., Convention Orders (CO))
# Current Page goes back to 120th (2000), but check out below, might be able to get to 112th -- But harder to find bills
# --------> http://legislature.maine.gov/bills/default_ps.asp?snum=112&PID=1456
# -------> Follows this pattern? http://www.mainelegislature.org/legis/bills/display_ps.asp?ld=1&snum=118
# -------> Starting 121st, house/senate docket not provided on these pages, neet to go to chamber status page... which is shit
#
# *** CAN GET LISTS OF ALL THE BILLS FROM HERE: http://legislature.maine.gov/legis/bills/bills_123rd/billtexts/
# ---> BACK TO THE 199TH AT LEAST
#
# Will need to use general bill status infromation to discern full legislative history --- Gov Actions aren't always directly stated in actions
# Non-Action Table info saved in Bill File
####################

import csv
import os
import urllib
import requests
from bs4 import BeautifulSoup
import time
import datetime
import re
import socket

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/ME/')


#######################
##### Extract Session Links
########################

session_search = urllib.request.urlopen('http://legislature.maine.gov/LawMakerWeb/doadvancedsearch.asp', timeout = 30) 
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.find('select', {'name':'LegSession'}).findAll('option')

session_nums = [s['value'] for s in session_list]
sessions = [s.get_text() for s in session_list]
sessions = [re.sub(' Legislature', '', s).replace('2001-2002', '120th') for s in sessions]

### Drop Previously Scraped
# sessions = [[s, n] for s, n in zip(sessions, session_nums)]
sessions = [[s, n] for s, n in zip(sessions, session_nums) if 'ME_Bill_Details_' + s + '.csv' not in os.listdir('.')]

#######################################################
########## GET BILL URLS VIA FTP SITE FOR A SESSION
#####################################################
# Adapting OpenStates Code
# session = sessions[0]

def parse_session_results(html, leg_list, s_name):
    this_soup = BeautifulSoup(html, 'lxml')
    bill_links = this_soup.findAll('a', href = re.compile('summary'))    
    clean_bills = [[s_name, a.get_text().strip(), 'http://legislature.maine.gov/LawMakerWeb/' + a['href']] for a in bill_links]
    leg_list = leg_list + clean_bills
    return(leg_list)

def get_session_bills(session): 

    s_name = session[0]
    s_num = session[1]
    
    ## Adapting OpenStates Code
    search_url = 'http://legislature.maine.gov/LawMakerWeb/doadvancedsearch.asp'
    request_session = requests.Session()
    
    bill_urls = []
    
    ### Request Data --- Don't Need From/To or Paper Type --- If Empty/None, will return ALL Session Bills
    form_data = {
        "PaperType": "None", #bill_type,
        "LegSession": s_num,
        #"PaperNumberFrom": 1,
        #"PaperNumberTo": 99999,
        "LRType": "None",
        "Sponsor": "None",
        "Introducer": "None",
        "Committee": "None",
        "AmdFilingChamber": "None",
        "RollcallChamber": "None",
        "Action": "None",
        "ActionChamber": "None",
        "GovernorAction": "None",
        "FinalLawType": "None",
        "search.x":46,
        "search.y":13
    }
    
    print('~~~~ Gathering Bill URLs for the {} Session ~~~~'.format(s_name))
    
    ### Get First Page + Total Number of Bills
    r = request_session.post(url=search_url, data=form_data)
    this_soup = BeautifulSoup(r.content, 'lxml')
    
    total = this_soup.find(text = re.compile("Results 1")).strip()
    total = int(re.sub('.+\\(of |\\)', '', total))
    
    bill_urls = parse_session_results(r.content, bill_urls, s_name)
    
    #### Loop Through Subsequent Pages
    startswith = 1
    while startswith < total:
        startswith = min(startswith + 25, total)
        r = request_session.get('http://legislature.maine.gov/LawMakerWeb/searchresults.asp', params={'StartWith': startswith})    
        bill_urls = parse_session_results(r.content, bill_urls, s_name)
        print(' -- {}/{}'.format(min(startswith + 24, total), total))
        time.sleep(1)
        
    return(bill_urls)


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################
    
    
def get_page_soup(page_url, parser = 'lxml'):
    
    ### Get HTML
    try:
        page = urllib.request.urlopen(page_url, timeout = 20).read()
    except urllib.error.HTTPError: # as e
        return('HTTP Error')
    except socket.timeout:
        print("\n ~~> SOCKET TIMEOUT --- Retrying Bill Request")
        time.sleep(120)
        return(get_page_soup(page_url))
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(15)
            page = urllib.request.urlopen(page_url, timeout = 30).read()
        except urllib.error.HTTPError: # as e
            return('HTTP Error')
        except socket.timeout:
            print("\n ~~> SOCKET TIMEOUT --- Retrying Bill Request")
            time.sleep(120)
            return(get_page_soup(page_url))
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = urllib.request.urlopen(page_url, timeout = 60).read()
    time.sleep(1)
    
    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)    
    
#######################
###### Functions to Scrape Individual Bills
#########################
# bill_info = bill_urls[2500]
# s_num = '4'   
    
def get_bill_data(s_num, bill_info):    
    
    s_name = bill_info[0]
    bill_num = bill_info[1]
    bill_url = bill_info[2]
    id_num = re.sub('^.+ID=', '', bill_url)
    
    ### Scrape Bill Page 
    bill_soup = get_page_soup(bill_url)
    
    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')
    
    ### Basic Details
    info_table = bill_soup.findAll('td', {'class':'sectionheading'})[0].findParents('table')[0]
    info_table = info_table.findAll('td', {'class':'sectionbody'})
    
    if len(info_table) == 3:
        summary = info_table[1].get_text().replace('"', '').strip()
        sponsor = info_table[2].get_text().replace('Sponsored by ', '').strip()
    elif len(info_table) == 2 and 'Sponsored by' not in bill_soup.get_text():
        summary = info_table[1].get_text().replace('"', '').strip()
        sponsor = ''
        
    #### Get Bill Number if LD (Legislative Document) ---> These are the bills we want (Not all HP's/SP's are bills, per say)
    if bill_num[0:2] == "LD":
        ld_num = bill_num
        bill_num = re.sub('LD [0-9]+ \\(|\\)', '', info_table[0].get_text().strip())
    else:
        ld_num = ''
    
    
    #### Status Table
    # status_table = bill_soup.findAll('td', {'class':'sectionheading'})[1].findParents('table')[0]
    status_header = bill_soup.find(text = re.compile("Status Summary"))
    
    if status_header is None:
        comm = ''
        H_engrossed = ''
        S_engrossed = ''
        gov_action = ''
        chapter = ''
        final_law_type = ''
        final_date = ''
    else:
        status_table = status_header.findParents('table')[0]
        
        comm_tag = status_table.find('td', text = re.compile('Reference Committee'))
        comm = ''
        if comm_tag is not None:
            comm = comm_tag.findNext('td').get_text().strip()
        
        H_eng_tag = status_table.find('td', text = re.compile('Engrossed by House'))
        H_engrossed = ''
        if H_eng_tag is not None:
            H_engrossed = H_eng_tag.findNext('td').get_text().strip()
            H_engrossed = datetime.datetime.strptime(H_engrossed, '%m/%d/%Y').strftime('%Y-%m-%d')
            
        S_eng_tag = status_table.find('td', text = re.compile('Engrossed by Senate'))
        S_engrossed = ''
        if S_eng_tag is not None:
            S_engrossed = S_eng_tag.findNext('td').get_text().strip()
            S_engrossed = datetime.datetime.strptime(S_engrossed, '%m/%d/%Y').strftime('%Y-%m-%d')
            
        gov_tag = status_table.find('td', text = re.compile('Governor Action'))
        gov_action = ''
        if gov_tag is not None:
            gov_action = gov_tag.findNext('td').get_text().strip()
        
        chapter_tag = status_table.find('td', text = re.compile('Chapter'))
        chapter = ''
        if chapter_tag is not None:
            chapter = chapter_tag.findNext('td').get_text().strip()

        final_law_tag = status_table.find('td', text = re.compile('Final Law Type'))
        final_law_type = ''
        if final_law_tag is not None:
            final_law_type = final_law_tag.findNext('td').get_text().strip()

        final_date_tag = status_table.find('td', text = re.compile('^Date$'))
        final_date = ''
        if final_date_tag is not None:
            final_date = final_date_tag.findNext('td').get_text().strip()
            final_date = datetime.datetime.strptime(final_date, '%m/%d/%Y').strftime('%Y-%m-%d')
                  
    ### Vote URl
    vote_url = 'http://legislature.maine.gov/LawMakerWeb/rollcalls.asp?ID={}'.format(id_num)
    
    ### Cosponsors --- Speaker, Representative, Senator + Locations included + Last names in CAPS
    sponsor_url = 'http://legislature.maine.gov/LawMakerWeb/sponsors.asp?ID={}'.format(id_num)
    sponsor_soup = get_page_soup(sponsor_url)
    sponsor_table = sponsor_soup.find('table', {'class':'sectionbody'})
    
    ### Checking in case some bills have multiple primary sponsors
    all_sponsors = sponsor_table.find('td', text = re.compile('Sponsored By'))
    if all_sponsors is not None:
        all_sponsors = all_sponsors.findNext('td').get_text()
        if re.sub(' of [A-z][a-z].+$', '', all_sponsors).lower() != sponsor.lower():
            print("\n ************** MULTIPLE SPONSORS **************** \n" )
            print(all_sponsors)
            print(bill_url + '\n **************************************************\n')
    
    cosponsors = sponsor_table.find('td', text = re.compile('Cosponsored By'))
    if cosponsors is None:
        cosponsors = ''
    else:
        cosponsors = cosponsors.findNext('td').get_text().strip()
        cosponsors = [re.sub(' of [A-Z][a-z]+', '', i) for i in re.split('  +', cosponsors)]
        cosponsors = '; '.join(cosponsors)
    
    ### Actions
    bill_actions = []
    action_url = 'http://legislature.maine.gov/LawMakerWeb/dockets.asp?ID={}'.format(id_num)
    action_soup = get_page_soup(action_url)
    action_table = action_soup.findAll('td', {'class':'sectionheading'})[1].findParents('table')[0]
    if 'Related Links' in action_table.get_text():
        pass # Means no actions found
    else:
        action_rows = action_table.findAll('tr')
        order = 1
        for row in action_rows[1:]:
            cells = row.findAll('td')
            date = cells[0].get_text()
            date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            chamber = cells[1].get_text()
            actions = cells[2].get_text() ### Could split these based on double spaces...
            actions = actions.split('.  ')
            for act in actions:
                bill_actions.append([bill_num, ld_num, s_name, chamber, date, act, order])
                order += 1       
        
    ### OUTPUT
    bill_details = [bill_num, ld_num, s_name, summary, sponsor, cosponsors, comm, H_engrossed, S_engrossed, gov_action, chapter, final_law_type, final_date, bill_url, vote_url]
    return([bill_details, bill_actions])
        
########################################################
############## SCRAPE SESSION(S)
############################################
# bill_info = session_bills[5000]
# s = sessions[0]  
    

for s in sessions:
    
    s_name = s[0]
    s_num = s[1]
    
    #### Output Lists
    session_bill_details = [['bill_number', 'LD_number', 'session', 'summary', 'sponsor', 'cosponsors', 'reference_committee', 'H_engross_date', 'S_engross_date', 'gov_action', 'chapter', 'final_law_type', 'final_date',  'bill_url', 'vote_url']]    
    session_actions = [['bill_number', 'LD_number','session', 'chamber', 'action_date', 'action','order']]
    
    print("\n ------------------- Now Scraping the {} Session ---------------------- \n".format(s_name))    
    
    ### Get all bills for a specific session
    session_bills = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        bill_data = get_bill_data(s_num, bill_info)
        time.sleep(1)
        
        if bill_data == "HTTP Error":
            time.sleep(120)
            bill_data = get_bill_data(s_num, bill_info)
            if bill_data == "HTTP Error":
                print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n URL: {} \n **********".format(num, total, bill_info[1], bill_info[2]))
                num += 1
                continue
            
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
    
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_info[1], bill_info[2]))
        num += 1
        
    with open("ME_Bill_Details_" + s_name + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("ME_Bill_Histories_" + s_name + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- {} SESSION SCRAPED + DATA SAVED  -------------\n\n\n".format(s_name))


print("  ********************************** ALL DONE ********************************** ")
