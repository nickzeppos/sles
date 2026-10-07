# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape OREGON Bills

@author: PB
"""

###########################
##### NOTES:
# ********* OREGON has an API: https://api.oregonlegislature.gov/odata/odataservice.svc/
# ---------> Documentation is not especially good, but structuring off of openstates
#
# ***** NEED TO USE THE API TO GET CORRECT SPONSOR ORDERING ****
# -- First Name on the website is not correct; they're ordered alphabetically
# -- HOWEVER: in API, MeasureSponsorId is ordered (lowerst to highest) according to how names are listed on printed bill
# -- See, e.g., https://olis.leg.state.or.us/liz/2009R1/Measures/Overview/HR5 AND correspodning bill text + API info
# -- UPDATE **** API Order not always right either?... still likely better..fixed the few errors whre R/S order switched, but some within chamber issues probably remain..
#
# **** CAN GET 2003 - 2006 via OLIS PAGE:
# -------> E.g., https://olis.leg.state.or.us/liz/2003R1/Measures/Overview/HCR1
# -------> BUT NEED TO MANUALLY PARSE INTRODUCED BILL TEXT TO GET CORRECT SPONSOR ORDER

###########################

import csv
import os
from bs4 import BeautifulSoup
import time
import re
import requests
import urllib
import socket
import datetime
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#######################
##### Extract Session Links
########################

session_req = requests.get('https://api.oregonlegislature.gov/odata/odataservice.svc/LegislativeSessions', timeout = 30)
session_soup = BeautifulSoup(session_req.content, 'lxml')

### Get Sessions + Manually Add 2003 - 2006
#pre_api_sessions = [ [2003, '2003R1', '2003 Regular Session'], [2005, '2005R1', '2005 Regular Session'], [2006, '2006S1', '2006 Special Session'] ]
#sessions = [[int(key.text[0:4]), key.text, title.text] for key,title in zip(session_soup.findAll('d:sessionkey'), session_soup.findAll('d:sessionname'))]
sessions = [[int(s.find("d:sessionkey").text[0:4]), s.find("d:sessionkey").text, s.find("d:sessionname").text] for s in session_soup.findAll("entry")]
#sessions = pre_api_sessions + sessions

### Drop Previously Scraped
sessions = [s for s in sessions if 'OR_Bill_Details_{}.csv'.format(s[1]) not in os.listdir('.')]

### Drop Sessions Prior to 2023
sessions = [s for s in sessions if s[0] >= 2023]

### Drop Current Session
next_year = datetime.datetime.now().year + 1
sessions = [s for s in sessions if s[0] < next_year]
print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(next_year))

del session_req, session_soup, next_year

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
########## GET BILL DATA FOR A SESSION
#####################################################
# s = sessions[7]

##### *** Adapted from OpenStates Code ***
def get_session_bills(s):

    s_yr, s_id, s_name = s
    api_base = 'https://api.oregonlegislature.gov/odata/odataservice.svc/'

    print('\n\n **** Gathering Session Data ****' )

    #### Adusted OpenStates Code
    # -- In theory, per ODATA model, should be ablt to add CommitteeReports, but doesn't apepar to exist: https://api.oregonlegislature.gov/odata/odataservice.svc/
    # -- Can get committee roll calls with CommitteeVotes in Measures (or add back in vote call)
    resources = dict(
        legislators='LegislativeSessions(\'{session}\')/Legislators',
        #legislator='Legislators(LegislatorCode=\'{legislator_code}\',SessionKey=\'{session}\')',
        committees='LegislativeSessions(\'{session}\')/Committees',
        #committee_members='Committees(CommitteeCode=\'{committee}\','
        #                  'SessionKey=\'{session}\')/CommitteeMembers',
        measures='LegislativeSessions(\'{session}\')/Measures'
                 '?$expand=MeasureSponsors,MeasureDocuments,MeasureHistoryActions,CommitteeAgendaItems'
                 #'CommitteeAgendaItems/CommitteeProposedAmendments',
    )

    ####### Get Session Committees
    comm_url = api_base + resources['committees'].format(session = s_id)
    comm_req = requests.get(comm_url, headers = {'Accept':"application/json"})
    these_comms = comm_req.json()['value']
    session_committees = [[i['CommitteeCode'], i['CommitteeName'], i['CommitteeType'], i['HouseOfAction'], i['ParentCommitteeCode']] for i in these_comms]
    time.sleep(2)

    ####### Get Session Legislators
    legislator_url = api_base + resources['legislators'].format(session = s_id)
    legislator_req = requests.get(legislator_url, headers = {'Accept':"application/json"})
    these_legislators = legislator_req.json()['value']
    session_legislators = [[i['LegislatorCode'], i['Title'], i['FirstName'], i['LastName'], i['Chamber'], i['Party'], i['DistrictNumber']] for i in these_legislators]
    time.sleep(2)

    ####### Match Session Legislators to Committees?????
    # ---- Could do this but not clear the value at this point -- Would need to loop through committees and save

    #########################
    ####### Get Bills
    #[b['MeasurePrefix'] + str(b['MeasureNumber']) for b in these_bills]
    #[b['MeasurePrefix'] + str(b['MeasureNumber']) for b in bill_json]

    bill_url = api_base + resources['measures'].format(session=s_id)

    #### Loop Through Pages
    page = 500
    skip = 0
    bill_json = []
    while True:
        this_url = bill_url + '&$top={page}&$skip={skip}'.format(page = page, skip = skip)
        this_req = requests.get(this_url, headers = {'Accept':"application/json"})
        these_bills = this_req.json()['value']
        if these_bills == []:
            break

        ### Add Bills to List
        bill_json = bill_json + these_bills

        ### Prep Call for More Bills, if Needed
        if len(these_bills) != page:
            break
        else:
            skip = skip + page
            print(' --- {}'.format(skip))

    ### Return List of Bills
    print('\n\n **** ~~~> Found Data for {} Bills TOTAL for the {} **** \n'.format(len(bill_json), s_name))
    return(session_committees, session_legislators, bill_json)


#######################################################
########## Parse the Bill Data Returned from the API
#####################################################
# s = sessions[1]
# b = session_bills[0]
# b = bill

def parse_bill_data(b, s):

    s_yr, s_id, s_name = s

    #################
    #### Basic Info
    #################
    # Other stuff: b['PrefixMeaning'], b['EffectiveDate'], b['FiscalAnalyst'], b['RevenueEconomist'],
    # -- b['MeasureDocuments'], b['EmergencyClause']
    bill_num = b['MeasurePrefix'] + str(b['MeasureNumber']).zfill(4)
    bill_url = 'https://olis.leg.state.or.us/liz/{}/Measures/Overview/{}'.format(s_id, bill_num)
    lc_num = b['LCNumber']

    keywords = ''
    descrip = ''
    summary = ''
    by_request = ''
    rev_impact = ''
    fiscal_impact = ''
    chapter_num = ''

    if b['RelatingTo']:
        keywords = re.sub('Relating to |\\.$', '', b['RelatingTo'].strip())

    if b['CatchLine']:
        descrip = re.sub('  +', ' ', re.sub('\n|\t|\r', ' ', b['CatchLine'].strip()))

    if b['MeasureSummary']:
        summary = re.sub('  +', ' ', re.sub('\n|\t|\r', ' ', b['MeasureSummary'].strip()))

    if b['AtTheRequestOf']:
        by_request = b['AtTheRequestOf']

    if b['RevenueImpact']:
        rev_impact = b['RevenueImpact']

    if b['FiscalImpact']:
        fiscal_impact = b['FiscalImpact']

    #### Status/Outcomes
    status = b['CurrentLocation'].strip()
    vetoed = b['Vetoed']
    if b['ChapterNumber']:
        chapter_num = b['ChapterNumber']

    ################
    #### SPONSORS
    ###############
    # ** See note at top: Need to record based on order of MeasureSponsorId (appearst o be in that order usually anyway?)
    # ** LegislatoreCode = Typo in API

    all_sponsors = []
    for spon in b['MeasureSponsors']:
        if spon['SponsorType'] == 'Committee':
            all_sponsors.append([spon['CommitteeCode'] + ' (Committee)', int(spon['MeasureSponsorId']), spon['SponsorLevel']])
        elif spon['SponsorType'] == 'Member':
            all_sponsors.append([spon['LegislatoreCode'], int(spon['MeasureSponsorId']), spon['SponsorLevel']])
        elif spon['SponsorType'] == 'Presession':
            ### BIlls are filed by President of Senate (or Speaker of House?) by request of some outside agency
            ### Explicitly states neither in favor or opposed
            ### See, eg, https://olis.leg.state.or.us/liz/2009R1/Downloads/MeasureDocument/SB61
            all_sponsors.append(['Introduced presession by request', int(spon['MeasureSponsorId']), spon['SponsorLevel']])
        else:
            print(" **** UNCODED SPONSOR TYPE ---- {} ----> BREAK".format(bill_num))
            break
        ## Sort
        all_sponsors.sort(key=lambda x: x[1])

    primary_sponsors = '; '.join([i[0] for i in all_sponsors if i[2] == 'Chief'])
    cosponsors = '; '.join([i[0] for i in all_sponsors if i[2] == 'Regular'])


    ################
    #### ACTIONS
    ###############

    bill_actions = []

    #### Committee Actions --- Don't Necessarily Need these but provides a bit more detail
    comm_order = 1
    for item in b['CommitteeAgendaItems']:

        date = item['MeetingDate'].split('T')[0]
        chamber = item['CommitteCode'][0]
        action =  '{} Committee ~ {}: {}'.format(item['CommitteCode'], item['MeetingType'], item['Action'])

        bill_actions.append([bill_num, s_id, chamber, date, action, '', comm_order, ''])
        comm_order += 1


    #### All Actions, includes comm info, but less detail
    order = 1
    for item in b['MeasureHistoryActions']:

        date = item['ActionDate'].split('T')[0]
        chamber = item['Chamber']
        action =  item['ActionText']
        vote_txt = ''
        if item['VoteText']:
            vote_txt = item['VoteText']

        bill_actions.append([bill_num, s_id, chamber, date, action, order, '', vote_txt])
        order += 1

    ### Combine Items
    bill_details = [bill_num, s_id, lc_num, status, rev_impact, fiscal_impact, keywords, chapter_num, vetoed, primary_sponsors, cosponsors, by_request, descrip, summary, bill_url]
    return([bill_details, bill_actions])




########################################################
############## SCRAPE SESSION(S)
############################################
# s = sessions[15]
# bill = session_bills[0]
# bill = [b for b in session_bills if b['MeasurePrefix'] + str(b['MeasureNumber']).zfill(4) == "SB0921"][0]


for s in sessions:

    if re.search('I1$', s[1]):
        continue

    #### Output Lists
    session_bill_details = [['bill_id', 'session', 'LC_num', 'status', 'rev_impact', 'fiscal_impact', 'keywords', 'chapter_num', 'vetoed', 'primary_sponsors', 'cosponsors', 'by_request', 'description', 'summary', 'bill_url']]
    session_actions = [['bill_id', 'session', 'chamber', 'action_date', 'action', 'order', 'comm_order', 'vote']]

    print("\n ------------------- Now Scraping: {} ---------------------- \n".format(s[2]))

    ### Get all bills for a specific session
    session_committees, session_legislators, session_bills = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill in session_bills:

        ### Get Basic Details
        bill_data = parse_bill_data(bill, s)

        #if bill_data == 'HTTP Error':
        #    print(" ********** \n ({}/{}) -- HTTP ERROR -- SKIPPING -- {} \n **********".format(num, total, bill_info[8]))
        #    num += 1
        #    continue

        ### Get Actions
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_data[0][0], bill_data[0][-1]))
        num += 1

    with open("OR_Committees_{}.csv".format(s[1]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_committees)

    with open("OR_Legislators_{}.csv".format(s[1]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_legislators)

    with open("OR_Bill_Details_{}.csv".format(s[1]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("OR_Bill_Histories_{}.csv".format(s[1]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} SCRAPED + DATA SAVED  -------------\n\n\n".format(s[1]))


print("  ********************************** ALL DONE ********************************** ")





#####################
##### RAW SPONSOR INFO FROM BILL TEXT
####################
## txt_url = 'https://olis.leg.state.or.us/liz/2008S1/Downloads/MeasureDocument/SB1059/Introduced'
#txt_url = 'https://olis.leg.state.or.us/liz/{}/Downloads/MeasureDocument/{}/Introduced'.format(s_id, bill_num)
#
#
#from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
#from pdfminer.converter import TextConverter
#from pdfminer.layout import LAParams
#from pdfminer.pdfpage import PDFPage
#from io import StringIO
#
#def convert_pdf_to_txt(path):
#    rsrcmgr = PDFResourceManager()
#    retstr = StringIO()
#    codec = 'utf-8'
#    laparams = LAParams()
#    device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
#    fp = open(path, 'rb')
#    interpreter = PDFPageInterpreter(rsrcmgr, device)
#    password = ""
#    maxpages = 0
#    caching = True
#    pagenos=set()
#
#    for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password,caching=caching, check_extractable=True):
#        interpreter.process_page(page)
#
#    text = retstr.getvalue()
#
#    fp.close()
#    device.close()
#    retstr.close()
#    return text
#
#from pathlib import Path
#filename = Path('zzz_bill_text.pdf')
#response = requests.get(txt_url)
#filename.write_bytes(response.content)
#text = convert_pdf_to_txt("zzz_bill_text.pdf")
#re.search('sponsored +by +', text.lower())
