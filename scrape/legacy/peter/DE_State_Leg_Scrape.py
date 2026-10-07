#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 11 13:28:35 2018

Scrape DE Bills

@author: pb
"""

############# NOTES:
### Older Bills are in the data - Could cycle back from min legislationid in data to 1 - no actions, it seems
########################

import csv
import os
import time
import urllib
from bs4 import BeautifulSoup
import requests
import datetime
import json

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/DE/')


#################################
#### Get Sessions
#################################

search_page = requests.get('https://legis.delaware.gov/AllLegislation')
search_soup = BeautifulSoup(search_page.content, "lxml")

sessions = search_soup.find_all('span', {'class':'checkboxList gaCheckboxes'})
session_nums = []
session_names = []
for group in sessions:
    for session in group.findAll("input"):
        session_nums.append(session['value'])
    for session in group.findAll("label"):
        session_names.append(session.get_text())

## ******** DROP CURRENT YEAR AND PREVIOUSLY SCRAPED *********** 
this_year = datetime.datetime.now().year
sessions = [i for i in zip(session_names, session_nums) if int(i[0][7:11]) < this_year ]
sessions = [i for i in sessions if 'DE_Bill_Details_{}.csv'.format(i[0][0:11].replace(' - ', '_')) not in os.listdir('.') ]
# session_names = session_names[1:]
# session_nums = session_nums[1:]

del session_names, session_nums

############
## CHECK IF ALREADY SCRAPED
############
       
#previously_scraped = []
#dirFiles = os.listdir('.')
#    
#for t in range(0, len(session_names)):
#    this_s_adj = session_names[t].split(" (")[0].replace(" ", "").replace("-", "_") 
#    if 'DE_Bill_Details_' + this_s_adj + '.csv' in dirFiles:
#        previously_scraped.append(t)
#        print("Previously Scraped: " + session_names[t])
#    
#for index in sorted(previously_scraped, reverse=True):
#    del session_names[index]
#    del session_nums[index]
    
    
#################################
## Function to Parse Json Results
#################################     
# page_json = this_page
# bill = page_json['Data'][0]
    
def parse_json_bills(page_json):       
    these_bills = []
    for bill in page_json['Data']:
        bill_number = bill['LegislationNumber']
        legislationId = bill['LegislationId']
        session_num = bill['AssemblyId']
        session = bill['AssemblyName']
        intro_date = bill['IntroductionDateTime'].strip('/Date(').strip(')')
        intro_date = datetime.datetime.fromtimestamp(int(intro_date)/1000.0).strftime('%Y-%m-%d')
        sponsor = bill['Sponsor']
        title = bill['LongTitle']
        bill_url = 'https://legis.delaware.gov/BillDetail?LegislationId={}'.format(legislationId)
        if bill['Synopsis'] is None:
            summary = ''
        else:
            summary = bill['Synopsis'].replace("\r\n", " ")
        if bill['SubstituteParentLegislationDisplayCode'] is None:
            parent_bill = ''
        else:
            parent_bill = bill['SubstituteParentLegislationDisplayCode']
        if bill['StatusName'] is None:
            status = ''
            status_id = ''
            status_date = ''
        else:
            status = bill['StatusName'].replace("\r\n", " ")
            status_id = bill['LegislationStatusId']
            status_date = bill['LegislationStatusDateTime'].strip('/Date(').strip(')')
            status_date = datetime.datetime.fromtimestamp(int(status_date)/1000.0).strftime('%Y-%m-%d')
        these_bills.append([bill_number, parent_bill, legislationId, session_num, session, sponsor, intro_date, status, status_id, status_date, title, summary, bill_url])
    return(these_bills)
    
#################################
## Function to Search for a Page of Bill Details -- Returns JSON
#################################     
# * Adapted from OpenStates
    
def post_search(session_num, page_number, per_page = 200):
    search_form_url = 'https://legis.delaware.gov/json/AllLegislation/GetAllLegislation'
    form = {
        'page': page_number,
        'pageSize': per_page,
        'selectedGA[0]': session_num,
        'coSponsorCheck': 'True',
        'selectedLegislationTypeId[0]': '1',
        'selectedLegislationTypeId[1]': '2',
        'selectedLegislationTypeId[2]': '3',
        'selectedLegislationTypeId[3]': '4',
        # Ignore the `Amendment` legislation type, `5`
        'selectedLegislationTypeId[4]': '6',
        'sort': '',
        'group': '',
        'filter': '',
        'sponsorName': '',
        'fromIntroDate': '',
        'toIntroDate': '',
    }
    page = requests.post(url=search_form_url, data=form, allow_redirects=True).json()
    return page

################################3
## Function to Scrape a Specific Session
#################################     
# session_num = s_num
# page_number = 1

def get_session_bills(session_num):
    print(" *** Gathering Bill Details for Session Number {} *** ".format(session_num))
    
    session_legislation = []
    page_number = 1
    
    while True:
        this_page = post_search(session_num, page_number)
        if not this_page['Data']:
            break

        else:
            page_bills = parse_json_bills(this_page)
            for this_bill in page_bills:
                session_legislation.append(this_bill)
            print(" -- Page {}".format(page_number))
            page_number += 1
    print(" -- No More Bills!".format(session_num))
    return(session_legislation)

##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################  

   
def get_page_soup(bill_url, parser = 'lxml'):
    
    ### Get HTML
    try:
        page = requests.get(bill_url, timeout = 30)
    except urllib.error.HTTPError: # as e
        return('HTTP Error')
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(30)
            page = requests.get(bill_url, timeout = 60)
        except urllib.error.HTTPError: # as e
            return('HTTP Error')
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = requests.get(bill_url, timeout = 60)
    time.sleep(.5)
    
    ### Return Soup
    page_soup = BeautifulSoup(page.content, parser)
    return(page_soup)    



##########################################
###### Functions to Get Votes and Actions
##########################################
# * Also adapted from openstates 
# * Would be quicker to just go direct to the bill page (below) but yields no data via urlopen/requests
# bill_url = 'https://legis.delaware.gov/BillDetail?LegislationId={}'.format(legislationId)

# legislation_id = 26803

def scrape_votes(bill_num, parent_bill, legislation_id, session_num):
    
    votes_url = 'https://legis.delaware.gov/json/BillDetail/GetVotingReportsByLegislationId'
    form = {
        'legislationId': legislation_id,
        'sort': '',
        'group': '',
        'filter': '',
    }

    these_votes = []
    try:
        response = requests.post(url=votes_url, data=form, allow_redirects=True)
    except:
        try:
            print(" ~~> Retrying Votes Request")
            time.sleep(10)
            response = requests.post(url=votes_url, data=form, allow_redirects=True)
        except:
            print(" ~~> Retrying Votes Request x 2")
            time.sleep(30)
            response = requests.post(url=votes_url, data=form, allow_redirects=True)
            
    if response.content:
        bill_votes = json.loads(response.content.decode('utf-8'))
        if bill_votes['Total'] > 0:
            for row in bill_votes['Data']:
                vote_date = row['TakenAtDateTime'].strip('/Date(').strip(')')
                vote_date = datetime.datetime.fromtimestamp(int(vote_date)/1000.0).strftime('%Y-%m-%d')
                this_vote = [bill_num, parent_bill, legislation_id, session_num, row['ChamberName'], row['RollCallId'], vote_date, row['RollCallResultTypeName'], row['VoteRequirementCode'], row['YesTotal'], row['NoTotal'], row['VacantTotal'], row['ConflictTotal'], row['NotVotingTotal']]
                these_votes.append(this_vote)
    return(these_votes)


# bill_num, parent_bill, legislation_id, session_num = b[0:5]
def scrape_actions(bill_num, parent_bill, legislation_id, session_num):
    
    actions_url = 'https://legis.delaware.gov/json/BillDetail/GetRecentReportsByLegislationId'
    form = {
        'legislationId': legislation_id,
        'sort': '',
        'group': '',
        'filter': '',
    }

    these_actions = []
    try:
        response = requests.post(url=actions_url, data=form, allow_redirects=True)
    except:
        try:
            print(" ~~> Retrying Actions Request")
            time.sleep(10)
            response = requests.post(url=actions_url, data=form, allow_redirects=True)
        except:
            print(" ~~> Retrying Actions Request x 2")
            time.sleep(30)
            response = requests.post(url=actions_url, data=form, allow_redirects=True)
            
    if response.content:
        bill_actions = json.loads(response.content.decode('utf-8'))
        if bill_actions['Total'] > 0:
            order = 1
            for row in bill_actions['Data']:
                action_date = row['OccuredAtDateTime'].strip('/Date(').strip(')')
                action_date = datetime.datetime.strptime(action_date, '%m/%d/%y').strftime('%Y-%m-%d')
                this_action = [bill_num, parent_bill, legislation_id, session_num, action_date, row['LegislationActionLogId'], row['ActionDescription'], order]
                these_actions.append(this_action)
                order += 1
    return(these_actions)

# details_row = b
def scrape_append_sponsors(details_row):
    
    bill_soup = get_page_soup(details_row[-1])    
    
    if bill_soup.find("label", text = "Co-Sponsor(s):") is None:
        print('--> Missing Sponsor Data --> Retryin page request')
        time.sleep(30)
        bill_soup = get_page_soup(details_row[-1])
    
    #    legislation_id = details_row[2]
    #    try:
    #        bill_page = requests.get('https://legis.delaware.gov/BillDetail?LegislationId={}'.format(legislation_id))
    #    except:
    #        print(" ~~> Retrying Sponsor Request")
    #        time.sleep(10)
    #        bill_page = requests.get('https://legis.delaware.gov/BillDetail?LegislationId={}'.format(legislation_id))

    # bill_soup = BeautifulSoup(bill_page.content, "lxml")
    # ### sponsor = bill_soup.find("label", text = "Primary Sponsor:")
    # ### sponsor = sponsor.findNext("div").get_text().strip("\n").replace("\n", "")
    try: 
        secondary_spon = bill_soup.find("label", text = "Additional Sponsor(s):")
        secondary_spon = secondary_spon.findNext("div").get_text().strip("\n").replace("\n", "")
       
        cosponsors = bill_soup.find("label", text = "Co-Sponsor(s):")
        cosponsors = cosponsors.findNext("div").get_text().replace("\n\n", "---").replace("\n", " ").replace("  ", " ").strip()
    except:
        print("Sponsor Scrape Error")
        secondary_spon = ""
        cosponsors = ""

    return(details_row[0:6] + [secondary_spon, cosponsors] + details_row[6:])
    

####################################
## Loop Through Sessions, Get Bill Details
#####################################
# s = sessions[0]
# b = session_bills[0]

for s in sessions:
    
    s_name, s_num = s
    
    ### Get List of Bills and Basic Details
    session_bills = get_session_bills(s_num)
    session_bill_details = [['bill_number', 'parent_bill', 'legislationId', 'session_num', 'session', 'sponsor', 'secondary_sponsors', 'cosponsors', 'intro_date', 'status', 'status_id', 'status_date', 'title', 'summary', 'bill_url']]
    
    session_actions = [['bill_number', 'parent_bill', 'legislationId', 'session_num', 'action_date', 'actionId', 'action', 'order']]
    session_votes = [['bill_number', 'parent_bill', 'legislationId', 'session_num', 'chamber', 'RollCallId', 'vote_date', 'result', 'vote_req', 'num_yeas', 'num_no', 'num_vacant', 'num_conflict', 'num_notvoting']]  
    ## Could scrape the actual roll calls via the rollcallID later if ever needed
    
    print(" ~~~> Scraping Individual Bills - Session {}".format(s_name))
    
    ### Scrape Each Bill
    num = 1
    total = len(session_bills)
    for b in session_bills:
    
        #### Get Sponsors
        session_bill_details.append(scrape_append_sponsors(b))
        #time.sleep(.5)
        
        #### Get Actions
        b_data = scrape_actions(b[0], b[1], b[2], b[3])
        for action in b_data:
            session_actions.append(action)    
        time.sleep(.5)
    
        #### Get Votes
        v_data = scrape_votes(b[0], b[1], b[2], b[3])
        for vote in v_data:
            session_votes.append(vote)    
        time.sleep(.5)   
        
        print(" -- ({}/{}) -- {} -- URL: {}".format(num, total, b[0], b[-1]))
        num += 1
    
    ### SAVE!
    term_adj = s_name.split(" (")[0].replace(" ", "").replace("-", "_")
    
    with open("DE_Bill_Details_" + term_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("DE_Bill_Histories_" + term_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)  
    
    with open("DE_Agg_Votes_" + term_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_votes)  

    print(" ***** Session {} DONE! *******".format(s_name))





#
#################################3
### Function to Cycle Through All Search Result Pages
##################################
#    
#chrome_options = Options()  
#chrome_options.add_argument("--headless")  
#
#def get_session_bills(session_name, session_num):
#    # driver = webdriver.Chrome(chrome_options=chrome_options)  
#    driver = webdriver.Firefox()  
#    driver.get('https://legis.delaware.gov/AllLegislation')
#    
#    ## Show and Uncheck all Checkboxes
#    driver.find_element_by_id("seeMoreAssemblies").click()
#    checkboxes = driver.find_elements_by_xpath("//input[@type='checkbox']")
#    for checkbox in checkboxes:
#        if checkbox.is_selected():
#            checkbox.click()
#    
#    ## Switch to correct session
#    driver.find_element_by_xpath("//input[@type='checkbox' and @value = '{}']".format(session_num)).click()
#    time.sleep(5)
#    
#    ## Output List
#    session_legislation = []
#        
#    ## Loop Through Pages
#    total_bills = driver.find_element_by_xpath("//span[@class = 'k-pager-info k-label']")
#    total_bills = int(total_bills.text.split(" of ")[1].strip(" items"))
#    total_pages = math.ceil(total_bills / 20)
#    
#    for i in range(1, total_pages):
#        
#        ##### GET PAGE DATA
#        
#        #### Go to Next Page
#        
#        driver.find_elements_by_xpath("//a[@class = 'k-link' and @data-page = '{}']".format(next_page))
#        
#        //div[@class="pagination"]/ol/li/a[text()="%s"]
#    
#    