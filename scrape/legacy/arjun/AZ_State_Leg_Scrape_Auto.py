# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape Arizona Bills

@author: PB
"""

##### NOTES:
# 1990 SESSIONS APPEAR TO LACK DATA IN THE BILL STATUS SYSTEM
# A number of the early special sessions are also in this category...
################################################

# ************ FIND THE API????? https://apps.azleg.gov/api/Session/
# https://apps.azleg.gov/api/Bill/?billNumber=SB1100&sessionId=10

import csv
import os
import re
import requests
#from requests.packages.urllib3.util.retry import Retry
#from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
import time
import json
import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
#from selenium.common.exceptions import TimeoutException
#from selenium.webdriver.support.ui import WebDriverWait
#from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
#from selenium.webdriver.support import expected_conditions as EC
#from selenium.webdriver.common.by import By

from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#######################
##### Extract Session Links
########################

session_page = requests.get('https://www.azleg.gov/bills/')
session_soup = BeautifulSoup(session_page.content, "lxml")

session_list = session_soup.find("select", {'class':'selectSession'})
session_list = session_list.findAll('option')

session_nums = [j['value'] for j in session_list]
session_list = [j.get_text() for j in session_list]

session_short = [re.sub('- .+ - | Session', '', j) for j in session_list]
for j in range(0, len(session_short)):
    yr = session_short[j][0:4]
    if int(yr) % 2 == 0:
        sy = str(int(yr) - 1) + "_" + str(yr)
    else:
        sy = str(int(yr)) + "_" + str(int(yr) + 1)
    session_short[j] = sy + session_short[j][4:].replace(' ', '_')

### Drop Previously Scraped
for i in range(len(session_short)-1, -1, -1):
    if 'AZ_Bill_Details_' + session_short[i] + '.csv' in os.listdir('.') or str(datetime.datetime.now().year) in session_short[i]:
        del session_short[i]
        del session_nums[i]
        del session_list[i]

#####################################################
####### Function to Scrape Bills for a Specific Session
##################################################

### GET BILL PAGE HTML ---- THIS WORKS BUT THE REQUEST IS DENIED --- WOULD NEED APPROVAL (SEE TEXT IN RESPONS)
# req = requests.get('https://www.azleg.gov/bills/')
# session_form_url = 'https://www.azleg.gov/azlegwp/setsession.php'
# form = {'sessionID': session_id}
# session_page = requests.post(url=session_form_url, data=form, cookies=req.cookies, allow_redirects=True)
# session_soup = BeautifulSoup(session_page.content, "lxml")
##################

# session = '2018 - Fifty-third Legislature - Second Regular Session'
# session_num = 119
def get_session_bills(session, session_num):

    ### GET BILL PAGE HTML ---- THIS WORKS BUT THE REQUEST IS DENIED --- WOULD NEED APPROVAL (SEE TEXT IN RESPONS)
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    #driver = webdriver.Firefox()
    driver.get('https://www.azleg.gov/bills/')
    select = Select(driver.find_element(By.CLASS_NAME,'selectSession'))
    select.select_by_visible_text(session)
    time.sleep(5)
    session_page = driver.page_source
    session_soup = BeautifulSoup(session_page, "lxml")
    bill_list = session_soup.findAll('a', {'class':'faqLink'})

    bill_urls = [bill['href'] for bill in bill_list]
    bill_num = [bill.get_text() for bill in bill_list]
    short_title = [bill.parent.findNext('td').get_text().strip() for bill in bill_list]

    bill_details = []
    for url, b_num, st in zip(bill_urls, bill_num, short_title):
        bill_details.append([b_num, session, session_num, st, url])

    time.sleep(1)
    driver.close()

    print(" \n " + str(len(bill_details)) + " BILLS GATHERED FOR: " + session + " \n")
    return(bill_details)


#######################
###### Functions to Scrape Individual Bills
#########################
## *** Adapting OpenStates Scraper for AZ: https://github.com/openstates/openstates/blob/master/openstates/az/bills.py

#### Get and Scrape Sponsor JSON
def get_sponsors(internal_id):
    sponsors_url = 'https://apps.azleg.gov/api/BillSponsor/?id={}'.format(internal_id)
    try:
        sponsor_page = requests.get(sponsors_url, timeout = 5)
    except:
        time.sleep(10)
        print(" ~~~> Retrying Sponsor Request!")
        sponsor_page = requests.get(sponsors_url, timeout = 15)
    time.sleep(.25)

    sponsor_json = json.loads(sponsor_page.content.decode('utf-8'))
    introducing_sponsor = ''
    primary_sponsors = []
    cosponsors = []
    ## Loop Through Sponsors
    for sponsor in sponsor_json:
        ### Append to Sponsor Type Lists
        ### Using Short Names because not all first/last name combos right with Jr's/what not
        if sponsor['SponsorType'] == 'Prime (1st Signer)':
            introducing_sponsor = sponsor['Legislator']['MemberShortName']
            primary_sponsors.append(sponsor['Legislator']['MemberShortName'])
        elif 'Prime' in sponsor['SponsorType']:
            primary_sponsors.append(sponsor['Legislator']['MemberShortName'])
        else:
            cosponsors.append(sponsor['Legislator']['MemberShortName'])

    #'; '.join(primary_sponsors.split())

    #print(" --------- SPONSORS SCRAPED")
    return([introducing_sponsor, primary_sponsors, cosponsors])


#### Get and Scrape Keywords
def get_keywords(internal_id):
    keywords_url = 'https://apps.azleg.gov/api/Keyword/?billStatusId={}'.format(internal_id)
    try:
        kw_page = requests.get(keywords_url, timeout = 5)
    except:
        time.sleep(10)
        print(" ~~~> Retrying Keyword Request!")
        kw_page = requests.get(keywords_url, timeout = 15)
    time.sleep(.25)

    kw_json = json.loads(kw_page.content.decode('utf-8'))
    keywords = []
    for kw in kw_json:
        keywords.append(kw['Keyword'])
    #print(" --------- KEYWORDS SCRAPED")
    return('; '.join(keywords))


#####################
#### Get Actions -- No Simple List...
######################
# this_bill_json = bill_json
# b_num = bill_num
# sess_name = 'test'
# sess_num = '105'

chamber_dict = {'S': 'Senate', 'H': 'House', 'G': 'Governor', 'SS':'Secretary of State'}
action_dict = {'DP':'Do pass',
               'DPA':'Do pass amended',
               'DPA/SE':'Do pass amended/strike everything',
               'SE':'Strike everything',
               'DPA/SE ON RECON':'Do pass amended/strike everything on reconsideration',
               'DP ON REREFER':'Do pass on rereferral',
               'DPA/SE CORRECTED':'Do pass amended/strike everything',
               'DPA:C&P': 'Do pass amended/constitutional and in proper form',
               'DPA/SE ON REREF':'Do pass amended/strike everything on rereferral',
               'DPA (CORRECTED)':'Do pass amended',
               'DP ON RECON':'Do pass on reconsideration',
               'Passed':'Passed',
               'Failed':'Failed',
               'PASSED':'Passed',
               'FAILED':'Failed',
               'PFC': 'Proper for consideration',
               'C&P':'Constitutional and in proper form',
               'HELD':'Held',
               'DISC/HELD':'Discussed and held',
               'RETAINED':'Retained',
               'RET ON CAL':'Retained on the Calendar',
               'RET FOR CON':'returned for consideration',
               'None':'None',
               'W/D':'Withdrawn',
               'PFC W/FL': 'Proper for consideration with floor amendment',
               'PFCA':'Proper for consideration amended',
               'PFCA W/FL': 'Proper for consideration amended with floor ammendment',
               'DISC/ONLY':'Discussion only',
               'DP/PFC': 'Do pass/proper for consideration',
               'DPA/PFC': 'Do pass amended/proper for consideration',
               'DP/PFC W/FL':'Do pass/proper for consideration with floor amendment',
               'DP/C&P':'Do pass/constitutional and in proper form',
               'DPA/PFC W/FL':'Do pass amended/proper for consideration with floor amendment',
               'AMEND C&P':'Amended constitutional and in proper form',
               'HELD ON RECON':'Held on reconsideration',
               'DPA ON RECON':'Do pass amended on reconsideration',
               'NOT HEARD':'Not heard',
               'DP/PFCA':'Do pass/proper for consideration amended',
               'C&P ON RECON':'Constitutional and in proper form on reconsideration',
               'AM C&P ON RECON':'Amended C&P on reconsideration',
               'REMOVAL REQ':'Removal request from rules committee',
               'DPA ON REREFER':'Do pass amended on rereferral',
               'C&P ON REREF':'Constitutional and in proper form on rereferral',
               'CONCUR': 'Recommend to concur',
               'CONCUR FAILED':'Motion to concur failed',
               'FAILED ON RECON':'Failed on reconsideration',
               'DNP':'Do not pass',
               'DPA/C&P': 'Do pass amended constitutional and in proper form',
               'NOT CONCUR':'Recommend not concur',
               'RULE 8J PROPER': 'proper legislation and not deemed derogatory or insulting',
               'REC REREF TO COM':'Recommend rereferal to committee',
               'REREF JUD':'Rereferred to judiciary committee',
               'S/C':'subcommittee',
               'S/C REPORTED':'Subcommittee reported',
               'FURTHER AMENDED':'Further amended',
               'DISC PETITION':'Discharge petition',
               'DISC/S/C':'Discussed in subcommittee',
               'REREF WM':'Rereferred to Ways and Means Committee',
               'AM C&P ON REREF':'Amend C&P on rereferral',
               'HELD 1 WK':'Held in committee 1 week',
               'REREF GOVOP':'Rerefferred to gov. op. committee',
               'DP W/MIN RPT':'Do pass with minority report',
               'HELD INDEF':'Held in committee indefinitely',
               'C&P AS AM BY JU':'constitutional and in proper form as amended',
               'C&P AS AM BY EN':'constitutional and in proper form as amended',
               'C&P AS AM BY GO':'constitutional and in proper form as amended',
               'C&P AS AM GOVOP':'constitutional and in proper form as amended',
               'C&P AS AM BY TR':'constitutional and in proper form as amended',
               'C&P AS AM BY WM':'constitutional and in proper form as amended',
               'C&P AS AM BY HE':'constitutional and in proper form as amended',
               'C&P AS AM BY APPR':'constitutional and in proper form as amended',
               'C&P W/FL': 'constitutional and in proper form with floor amendment'}

def check_action(action, internal_id):
    if action not in [k for k in action_dict.keys()]:
        print("\n ~~~~ ACTION *** {} *** NOT IN DICTIONARY ~~~~ \n".format(action))
        test_url = 'https://apps.azleg.gov/BillStatus/BillOverview/{}'.format(internal_id)
        new_value = input("\n\n ******** \n\n Check the Bill Page: {} \n\n What value would you like to use?  \n\n REMEMBER TO ADD IT INTO THE CODE \n\n  ******** \n\n".format(test_url))
        action_dict[action] = new_value

def get_actions(this_bill_json, b_num, sess_num, sess_name, internal_id):

    actions = [] #billid, session, chamber, action_date, action

    ### Introduced
    introduced_date = this_bill_json['DateIntroduced']
    if introduced_date == None or introduced_date == '':
        return(actions)

    if '/' in introduced_date:
        introduced_date = datetime.datetime.strptime(introduced_date, '%m/%d/%Y').strftime('%Y-%m-%d')
    intro_chamber = chamber_dict[b_num[0:1]]
    actions.append([b_num, sess_num, sess_name, intro_chamber, this_bill_json['DateIntroduced'], '{} Introduced'.format(b_num)])

    ### Transmitted to...
    for t in this_bill_json['BodyTransmittedTo']:
        transmit_to = chamber_dict[t['LegislativeBody'].strip()]
        transmit_date = t['TransmitDate'].split('T')[0]
        actions.append([b_num, sess_num, sess_name, '', transmit_date, 'Transmitted to {}'.format(transmit_to)])

    ### First Readings - Complete or Waived
    if this_bill_json['Senate1stRead'] != None:
        actions.append([b_num, sess_num, sess_name, 'Senate', this_bill_json['Senate1stRead'].split('T')[0], 'First Reading Complete'])
    elif this_bill_json['Senate1stWaived'] != None:
        actions.append([b_num, sess_num, sess_name, 'Senate', this_bill_json['Senate1stWaived'].split('T')[0], 'First Reading Waived'])

    if this_bill_json['House1stRead'] != None:
        actions.append([b_num, sess_num, sess_name, 'House', this_bill_json['House1stRead'].split('T')[0], 'First Reading Complete'])
    elif this_bill_json['House1stWaived'] != None:
        actions.append([b_num, sess_num, sess_name, 'House', this_bill_json['House1stWaived'].split('T')[0], 'First Reading Waived'])

    ### Second Readings - Complete or Waived
    if this_bill_json['Senate2ndRead'] != None:
        actions.append([b_num, sess_num, sess_name, 'Senate', this_bill_json['Senate2ndRead'].split('T')[0], 'Second Reading Complete'])
    elif this_bill_json['Senate1stWaived'] != None:
        actions.append([b_num, sess_num, sess_name, 'Senate', this_bill_json['Senate2ndWaived'].split('T')[0], 'Second Reading Waived'])

    if this_bill_json['House2ndRead'] != None:
        actions.append([b_num, sess_num, sess_name, 'House', this_bill_json['House2ndRead'].split('T')[0], 'Second Reading Complete'])
    elif this_bill_json['House2ndWaived'] != None:
        actions.append([b_num, sess_num, sess_name, 'House', this_bill_json['House2ndWaived'].split('T')[0], 'Second Reading Waived'])

    #### Committee Assignments -- INCLUDES ACTIONS/RECS when Reported OUT
    for comm in this_bill_json['StandingCommittee']:
        comm_name = comm['Committee']['CommitteeName']
        comm_abbrev = comm['Committee']['CommitteeShortName']
        assigned_date = ''
        if comm['AssignedDate'] != None:
            assigned_date = comm['AssignedDate'].split('T')[0]
        comm_chamber = chamber_dict[comm['Committee']['LegislativeBody']]
        if comm['Committee']['IsSubCommittee'] == True:
            comm_type = 'Subcommittee'
        else:
            comm_type = 'Committee'
        actions.append([b_num, sess_num, sess_name, comm_chamber, assigned_date, 'Assigned to {} {}~{} ({})'.format(comm_chamber, comm_type, comm_abbrev, comm_name)])
        if comm['ReportDate'] != None:
            report_date = comm['ReportDate'].split('T')[0]
            reported_rec = comm['Action']
            check_action(reported_rec, internal_id)
            actions.append([b_num, sess_num, sess_name, comm_chamber, report_date, 'Reported {} from {} {}~{} ({})'.format(reported_rec, comm_chamber, comm_type, comm_abbrev, comm_name)])
        elif comm['DischargeDate'] != None:
            discharge_date = comm['DischargeDate'].split('T')[0]
            actions.append([b_num, sess_num, sess_name, comm_chamber, discharge_date, 'Discharged from {} {}~{} ({})'.format(comm_chamber, comm_type, comm_abbrev, comm_name)])
        ## Sometimes just coded as Action = None.. but this should be implied by no match down the line
        #elif comm['Action'] == 'None':
        #    actions.append([b_num, sess_num, sess_name, comm_chamber, assigned_date, 'No Action - {} {}~{} ({})'.format(comm_chamber, comm_type, comm_abbrev, comm_name)])

    #### Actions --- SKIPPING Committee Actions Which are in previous loop
    for status in this_bill_json['BillStatusAction']:

        if status['Committee']['TypeName'] == 'Standing':
            continue

        chamber = chamber_dict[status['Committee']['LegislativeBody']]
        check_action(status['Action'], internal_id)
        if status['Committee']['CommitteeName'] in ['Third Reading', 'Final Reading']:
            action_date = ''
            if status['ReportDate'] != None:
                action_date = status['ReportDate'].split('T')[0]
            this_action = action_dict[status['Action']]
            actions.append([b_num, sess_num, sess_name, chamber, action_date, '{} {}'.format(this_action, status['Committee']['CommitteeName'])])
        elif status['Committee']['CommitteeName'] == 'Concurrence':
            # Seems to be procedural, but no actual action
            pass
        elif 'Committee of the Whole' in status['Committee']['CommitteeName']:
            action_date = "" if status['ReportDate'] == None else status['ReportDate'].split('T')[0]
            this_action = status['Action']
            actions.append([b_num, sess_num, sess_name, chamber, action_date, '{} ~ {} ({})'.format(status['Committee']['CommitteeName'], this_action, action_dict[this_action])])
        elif status['Committee']['CommitteeName'] == 'Conference Committee':
            if status['AssignedDate'] is not None:
                assigned_date = status['AssignedDate'].split('T')[0]
                actions.append([b_num, sess_num, sess_name, chamber, assigned_date, 'Bill Assigned to {} Conference Committee'.format(chamber)])
            ## If No Reported Data, Held in Conf Comm: e.g, https://apps.azleg.gov/BillStatus/BillOverview/69882
            if status['ReportDate'] is not None:
                action_date = status['ReportDate'].split('T')[0]
                actions.append([b_num, sess_num, sess_name, chamber, action_date, 'Bill Reported From {} Conference Committee'.format(chamber)])
        elif status['Committee']['TypeName'] == 'Floor' and 'Motion' in status['Committee']['CommitteeName']:
            action_date = ''
            if status['ReportDate'] is not None:
                action_date = status['ReportDate'].split('T')[0]
            this_action = status['Committee']['CommitteeName']
            actions.append([b_num, sess_num, sess_name, chamber, action_date, '{} Floor Motion - {}'.format(chamber, this_action)])
        elif status['ReportDate'] != None and status['Action'] != 'None':
            action_date = status['ReportDate'].split('T')[0]
            this_action = status['Action']
            actor = status['Committee']['CommitteeName']
            actions.append([b_num, sess_num, sess_name, chamber, action_date, '{} ~ {} ({})'.format(actor, this_action, action_dict[this_action])])
            print(" \n\n STATUS CODING --- {} ~ {} ({}) \n\n".format(actor, this_action, action_dict[this_action]))
            print("~~> For ref: https://apps.azleg.gov/BillStatus/BillOverview/{} \n\n".format(internal_id))

    ### Governor
    if this_bill_json['GovernorAction'] == 'Signed':
        if this_bill_json['GovernorActionDate'] is not None:
            action_date = this_bill_json['GovernorActionDate'].split('T')[0]
        else: # If no gov action using the presumed final transmit date to gov
            action_dates = [d['TransmitDate'] for d in this_bill_json['BodyTransmittedTo']]
            action_date = max(action_dates).split('T')[0]
        actions.append([b_num, sess_num, sess_name, 'Executive', action_date, 'Signed by Governor'])

    elif this_bill_json['GovernorAction'] == 'Vetoed':
        if this_bill_json['GovernorActionDate'] is not None:
            action_date = this_bill_json['GovernorActionDate'].split('T')[0]
        else: # If no gov action using the presumed final transmit date to gov
            action_dates = [d['TransmitDate'] for d in this_bill_json['BodyTransmittedTo']]
            action_date = max(action_dates).split('T')[0]
        actions.append([b_num, sess_num, sess_name, 'Executive', action_date, 'Vetoed by Governor'])

    ### Veto Override --- Veto Overrides are coded in the Status Actions
    #if this_bill_json['VetoOverride'] is not None and this_bill_json['VetoOverride'] != '':
    #    input('\n\n ****** VETO OVERRIDE --- https://apps.azleg.gov/BillStatus/BillOverview/{}  ***** \n\n'.format(internal_id))

    ### Chapter Assigned  --- !=0 adjusts for a 1995 second special issue where it has a number (0) but no seeming actions
    if this_bill_json['ChapterNumber'] is not None and this_bill_json['ChapterNumber'] != 0:
        # Assuming no veto override... but need an example of that to code right
        if this_bill_json['GovernorActionDate'] is not None:
            final_date = this_bill_json['GovernorActionDate'].split('T')[0]
        else: # If no gov action using the presumed final transmit date to gov
            final_dates = [d['TransmitDate'] for d in this_bill_json['BodyTransmittedTo']]
            final_date = max(final_dates).split('T')[0]
        actions.append([b_num, sess_num, sess_name, 'Executive', final_date, 'LAW - Chapter Number {}'.format(this_bill_json['ChapterNumber'])])

    ### Final Disposition
    # this_bill_json['FinalDisposition'] # = Held in Committees

    #print(" --------- ACTIONS SCRAPED")
    return(actions)


####################################
#### MAIN BILL SCRAPE FUNCTION
#################################
### ----> GET JSON USING BELOW FORMAT
#bill_num = bill[0]
#session_name = this_session
#session_num = this_session_num

def scrape_bill(bill_num, session_name, session_num):

    ### GET BILL PAGE JSON
    bill_url = 'https://apps.azleg.gov/api/Bill/?billNumber={}&sessionId={}'.format(bill_num, session_num)
    try:
        bill_page = requests.get(bill_url, timeout = 5)
    except:
        time.sleep(10)
        print(" ~~~> Retrying Bill Page Request!")
        bill_page = requests.get(bill_url, timeout = 15)
    time.sleep(.5)

    ### Extract JSON
    bill_json = json.loads(bill_page.content.decode('utf-8'))

    if bill_json == None:
        print(" -- No bill content: {} ~~~> URL: {}".format(bill_num, bill_url))
        return('No Data')

    ### Pull out basic bill details
    # bill_num = bill_json['Number']
    internal_id = bill_json['BillId']
    short_title = ''
    if bill_json['ShortTitle'] != None:
        short_title = bill_json['ShortTitle'].strip()
    descrip = bill_json['Description'].strip()

    ### Get Sponsors
    sponsor_list = get_sponsors(internal_id)
    intro_sponsor = sponsor_list[0]
    all_primary = '; '.join(sponsor_list[1])
    cosponsors = '; '.join(sponsor_list[2])

    ### Get Keywords
    keywords = get_keywords(internal_id)

    ### Get Actions
    these_actions = get_actions(bill_json, bill_num, session_num, session_name, internal_id)

    ### Bill Details List
    bill_details = [bill_num, session_num, session_name, internal_id, short_title, intro_sponsor, all_primary, cosponsors, keywords, descrip, bill_url]

    return([bill_details, these_actions])



########################################################
############## SCRAPE SESSION(S)
############################################

for s in range(0, len(session_list)):

    ## Session Details
    this_session = session_list[s]
    #this_session_adj = this_session.replace(' ', '').replace('-', '_')

    this_session_num = session_nums[s]
    this_session_short = session_short[s]

    #### Output Lists
    session_bill_details = [['bill_number', 'session_num', 'session', 'bill_id_num', 'title', 'intro_sponsor', 'primary_sponsors', 'cosponsors', 'keywords', 'summary', 'bill_json']]
    session_actions = [['bill_number', 'session_num', 'session', 'chamber', 'action_date', 'action']]

    print("\n ----- Now Scraping: " + this_session + " ---------\n")

    ### Get all bills for a specific session
    session_bills = get_session_bills(this_session, this_session_num)

    #### Loop through bills -- Request + Clean JSON
    num = 1
    total = len(session_bills)
    for bill in session_bills:

        ### Skip Small Number of Miscellaneous Motions
        if bill[0][0:1] == "M":
            num += 1
            continue
        else:
            bill_data = scrape_bill(bill[0], this_session_short, this_session_num)

        ### Append Data
        if bill_data == "No Data":
            num += 1
            continue

        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill[0], bill_data[0][10]))
        num += 1

    with open("AZ_Bill_Details_" + this_session_short + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("AZ_Bill_Histories_" + this_session_short + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- " + this_session + " SCRAPED + DATA SAVED  -------------\n\n\n")
    time.sleep(5)

print("  ********************************** ALL DONE ********************************** ")
