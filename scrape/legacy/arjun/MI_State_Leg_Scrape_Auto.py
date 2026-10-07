# -*- coding: utf-8 -*-
"""
Created on Wed Dec 12 14:55:04 2018

*** SCRAPE MICHIGAN STATE LEGISLATIVE BILLS ***

@author: PB
"""

import csv
import os
# import re
import time
import urllib
from bs4 import BeautifulSoup
import requests
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#################################
#### Get Sessions
#################################

session_page = urllib.request.urlopen('https://www.legislature.mi.gov/(S(k44dsi1kuoyyr0emm5jixz3c))/mileg.aspx?page=Bills')
session_soup = BeautifulSoup(session_page, 'lxml')

#### Create List of Sessions and DROP IF ALREADY SCRAPED
session_select = session_soup.find('select', id = 'frg_bills_BrowseBills_LstHouseYear')
sessions = [i.get_text() for i in session_select.findAll('option') if i.get_text() != 'Select Session']


#### Drop the Sessions with Minimal Info (Too Old, Pre-1997?)
# From Website: A SUBSET of documents (including the bill as introduced, the enrolled version if passed and
# analyses/summaries) are available for sessions prior to 1997 and are provided by the Library of Michigan.'''
# --> 1995-1996 data looks good for bills -- NO RESOLUTIONS; <= 1993-1994 does not have history information for most bills
sessions = [s for s in sessions if int(s[0:4]) >= 1995 ]

## DROP IF ALREADY SCRAPED
sessions = [s for s in sessions if "MI_Bill_Details_" + s.replace("-", "_") + ".csv" not in os.listdir('.')]

### Drop Current Session
sessions = [s for s in sessions if s != '2023-2024']
print(" ***  SKIPPING 2023-2024 SESSION FOR NOW -- Manual edit code to scrape *******")

##################################
### GET BILL PAGE URLS
##################################
# session = '2015-2016'
# bill_type_list = bill_types['Bills']

bill_types = {
    'Bills': ['Bills', 'frg_bills', 'BrowseBills'],
    'Resolutions': ['Resolutions', 'frg_resolutions', 'BrowseResolutions'],
    'Joint': ['JointResolutions', 'frg_jointresolutions', 'BrowseJRs'],
    'Concurrent': ['ConcurrentResolutions', 'frg_concurrentresolutions', 'BrowseCRs']
}


def get_bill_urls(session, bill_type_list):

    bill_type = bill_type_list[0]
    frg_stem = bill_type_list[1]
    browse_stem = bill_type_list[2]

    ### Create Base Request -- Need Chamber-Specific Forms -- Can't do both simultaneously
    # form_url = 'https://www.legislature.mi.gov/(S(igxn0svzovc45opdvxp1ajfa))/mileg.aspx?page={}'.format(bill_type)
    # form_url = 'https://www.legislature.mi.gov/(S(5x3jgvo2nwmgrq4klntkvchx))/mileg.aspx?page={}'.format(bill_type)
    start_url = 'https://www.legislature.mi.gov/mileg.aspx?page={}'.format(bill_type)
    req = requests.get(start_url)
    ### Need to update url as it changes??? odd..
    form_url = req.url
    req_soup = BeautifulSoup(req.content, 'lxml')
    viewstate = req_soup.select("#__VIEWSTATE")[0]['value']
    viewstategenerator = req_soup.select("#__VIEWSTATEGENERATOR")[0]['value']

    base_form = {
    '__VIEWSTATE':viewstate,
    '__VIEWSTATEGENERATOR':viewstategenerator,
    '__EVENTTARGET':'',
    '__EVENTARGUMENT':'',
    '__SCROLLPOSITIONX':0,
    '__SCROLLPOSITIONY':542,
    }

    ### Chamber-Specific Forms --- NOTE Difference in Browse Button
    house_form = {**base_form, **{
    '{}$LegislativeSession$LegislativeSession'.format(frg_stem) : session,
    '{}${}$LstHouseYear'.format(frg_stem, browse_stem) : session,
    '{}${}$Button1'.format(frg_stem, browse_stem) : 'Browse',
    '{}${}$LstSenateYear'.format(frg_stem, browse_stem): 'Select Session'
    }}
    senate_form = {**base_form, **{
    '{}$LegislativeSession$LegislativeSession'.format(frg_stem) : session,
    '{}${}$LstHouseYear'.format(frg_stem, browse_stem) : 'Select Session',
    '{}${}$Button2'.format(frg_stem, browse_stem) : 'Browse',
    '{}${}$LstSenateYear'.format(frg_stem, browse_stem): session
    }}

    ### Get HOUSE Bills
    h_page = requests.post(url=form_url, data=house_form, cookies=req.cookies, allow_redirects=True )
    h_soup = BeautifulSoup(h_page.content, "lxml")
    h_table = h_soup.find('table', id = 'frg_executesearch_SearchResults_Results').findAll('tr')
    time.sleep(1)
    ### Get SENATE Bills
    s_page = requests.post(url=form_url, data=senate_form, cookies=req.cookies, allow_redirects=True)
    s_soup = BeautifulSoup(s_page.content, "lxml")
    s_table = s_soup.find('table', id = 'frg_executesearch_SearchResults_Results').findAll('tr')
    time.sleep(1)

    ### Parse Data
    this_bill_data = []
    for table in [h_table, s_table]:
        for row in table[1:]:
            cells = row.findAll('td')
            #bill_num = cells[0].get_text()
            #bill_num = re.sub(' of .+$', '', bill_num)
            #bill_url = 'https://www.legislature.mi.gov/(S(igxn0svzovc45opdvxp1ajfa))/' + cells[0].find('a')['href']
            bill_num = cells[0].find('a')['href'].split("objectname=")[1].split('&')[0]
            friendly_url = 'http://legislature.mi.gov/doc.aspx?' + bill_num
            bill_categ = cells[1].get_text()
            descrip = cells[2].get_text().split("Last Action:")[0]
            this_bill_data.append([session, bill_num, bill_categ, descrip, friendly_url])

    return(this_bill_data)


#### Need to Cycle Through All 4 Bill Types!
# test = get_bill_urls(sessions[0], bill_types['Bills'])


##################################
### GET BILL DATA
##################################
# bill_url = 'http://legislature.mi.gov/doc.aspx?1998-SB-1376'
# bill_num = '1997-HB-4004'
# session = 'zzz'

def get_bill_data(session, bill_num, bill_type, summary, bill_url):

    try:
        bill_page = urllib.request.urlopen(bill_url, timeout = 5).read()
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(10)
            bill_page = urllib.request.urlopen(bill_url, timeout = 10)
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            bill_page = urllib.request.urlopen(bill_url, timeout = 30)

    ### Get Bill Data
    bill_soup = BeautifulSoup(bill_page, 'lxml')

    ### Sponsors
    sponsor_list = bill_soup.find('span', id = 'frg_billstatus_SponsorList')
    if sponsor_list is not None:
        sponsor_list = [i.get_text().replace('\xa0', ' ').strip() for i in sponsor_list.findAll('a') if 'DistrictMaps' not in i['href']]
        sponsor_list = '; '.join(sponsor_list)
    else:
        sponsor_list = ''

    ### Categories
    category_list = bill_soup.find('span', id = 'frg_billstatus_CategoryList')
    if category_list is not None:
        category_list = [i.get_text().replace('\xa0', ' ').strip() for i in category_list.findAll('a')]
        category_list = '; '.join(category_list)
    else:
        category_list = ''

    ### Summary --- Already have this from URL Scrape
    # summary = bill_soup.find('span', id = 'frg_billstatus_ObjectSubject').get_text().strip()

    ### Actions
    ## NOTE: (House actions in lowercase, Senate actions in UPPERCASE)
    ## --> Can also use journal, with the exception of governor
    all_actions = []
    action_table = bill_soup.find('table', id = 'frg_billstatus_HistoriesGridView')
    if action_table is not None:
        order = 1
        for row in action_table.findAll('tr')[1:]:
            cells = row.findAll('td')
            date = cells[0].get_text()
            journal_page = cells[1].get_text().replace('\xa0', ' ')
            action = cells[2].get_text().replace('\xa0', ' ')
            all_actions.append([bill_num, session, date, journal_page, action, order])
            order += 1

    ### Get Vote Urls???

    #### Save to Export
    bill_details = [bill_num, session, bill_type, summary, sponsor_list, category_list, bill_url]
    return([bill_details, all_actions])


#####################################
### Loop Through Sessions, Get Bill Details
######################################
# bill_url = session_bills[6901]
# s = '1995-1996'

for s in sessions:

    print("\n *** Gathering Bill URLs for the {} MI Legislative Session *** \n".format(s))

    ### Get List of Bills and Basic Details
    session_bills = get_bill_urls(s, bill_types['Bills'])
    print(" ~~ Bill URLs Found")
    if s != '1995-1996':
        session_res = get_bill_urls(s, bill_types['Resolutions'])
        print(" ~~ Resolution URLs Found")
        session_jointres = get_bill_urls(s, bill_types['Joint'])
        print(" ~~ Joint Resolution URLs Found")
        session_conres = get_bill_urls(s, bill_types['Concurrent'])
        print(" ~~ Concurrent Resolution URLs Found")
        all_session_urls = session_bills + session_res + session_jointres + session_conres
    else:
        all_session_urls = session_bills

    session_bill_details = [['bill_number', 'session', 'bill_type', 'summary', 'sponsors', 'keywords', 'bill_url']]
    session_actions = [['bill_number', 'session', 'action_date', 'journal_page', 'action', 'order']]


    ### Scrape Each Bill/Resolution
    num = 1
    total = len(all_session_urls)

    print("\n ~~~> Scraping {} Individual Bills & Resolutions - Session {} \n".format(total, s))

    for bill_row in all_session_urls:

        ### Get Bill Data -- Each Type ONE AT A TYPE
        this_bill = get_bill_data(s, bill_row[1], bill_row[2], bill_row[3], bill_row[4])
        time.sleep(1)

        ### Append to Agg Files
        session_bill_details.append(this_bill[0])

        if this_bill[1] != []:
            for action_row in this_bill[1]:
                session_actions.append(action_row)

        print(" -- ({}/{}) ~~ URL: {}".format(num, total, bill_row[4]))
        num += 1

    ### SAVE!
    with open("MI_Bill_Details_" + s.replace("-", "_") + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("MI_Bill_Histories_" + s.replace("-", "_") + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n *********** General Assembly #{} DONE! ************** \n\n".format(s))


print(" ~~~~~~~~~~~~~ ************** ALL SESSIONS DONES *************** ~~~~~~~~~~~~~~~~ ")
