# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape OREGON Bills

@author: PB
"""

###########################
##### NOTES:
#
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# ************************ INCOMPLETE -- 2003-2006, SPONSORS IN ALPHABETICAL ORDER **************************
# ------> Could pull bill text from here and then parse... will be a pain: 
# ~~~~~~~~~~~~~~~~~ BILL TEXT: https://www.oregonlegislature.gov/bills_laws/Pages/archived-bills.aspx
# ------> This version (NO LONGER? Feb 2020) works to gather bill urls, but didn't go further because of sponsor order issues
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#
#
# ****** ARCHIVE SCRAPER GATHERS BILLS FOR 2003 - 2006 **************
# --- Oregon has an API, but data only goes to 2007
# 
# **** CAN GET 2003 - 2006 via OLIS PAGE:
# -------> E.g., https://olis.leg.state.or.us/liz/2003R1/Measures/Overview/HCR1
# -------> BUT NEED TO MANUALLY PARSE INTRODUCED BILL TEXT TO GET CORRECT SPONSOR ORDER
# ******** ---> BUT BILL TEXT DOESN"T SEEM TO EXIST?????
#
#
#
# BELOW CODE DOESN"T WORK (ANYMORE?)
# --- Could always just set the bill ranges manually (eg, SB1-Sb995) via the bill icon in upper right from link below
# --- list_url = 'https://olis.leg.state.or.us/liz/{}/Measures/list/'.format(s_id)
# --- alternative is to use Selenium to grab the bills (and titles!)
# --- May want to double check alphabetized problem above
#
###########################

import csv
import os
from bs4 import BeautifulSoup
import time
import re
import urllib
import socket
import datetime
#import json

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/OR/')


#######################
##### Session Information
########################

sessions = [ [2003, '2003R1', '2003 Regular Session'], [2005, '2005R1', '2005 Regular Session'], [2006, '2006S1', '2006 Special Session'] ]


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
    time.sleep(1)
    
    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)    


#######################################################
########## GET BILL DATA FOR A SESSION
#####################################################
# s = sessions[1]

##### *** Adapted from OpenStates Code ***
def get_session_bills(s): 
    
    s_yr, s_id, s_name = s
    list_url = 'https://olis.leg.state.or.us/liz/{}/Measures/list/'.format(s_id)
    
    print('\n\n **** Gathering Session Data ****' )
    
    #### Get House Page with Groups of Bills for each Session
    list_soup = get_page_soup(list_url)
    list_grp_urls = ['https://olis.leg.state.or.us' + i['data-load-action'] for i in list_soup.findAll('ul', {'data-load-action':re.compile('MeasureGroupedListing')})]
    
    session_bills = []
    for grp_url in list_grp_urls:
        grp_soup = get_page_soup(grp_url)
        grp_bills = grp_soup.findAll('a', {'href':re.compile('Measures\\/Overview')})
        for bill in grp_bills:
            session_bills.append([bill.text.strip(), s_id, bill['title'], 'https://olis.leg.state.or.us' + bill['href']])


    ####### Get Session Committees
    comm_url = 'https://olis.leg.state.or.us/liz/{}/Navigation/CommitteeMainNav'.format(s_id)
    comm_soup = get_page_soup(comm_url)
    comm_tags = comm_soup.findAll('a', {'href':re.compile('Committees.+Overview')})
    session_committees = []
    for comm in comm_tags:
        c_type = comm.findParent('ul', id = re.compile('Committees'))['id']
        session_committees.append([re.sub('.+Committees\\/|\\/Overview', '', comm['href']), comm.text.strip(), c_type, re.sub('Committees', '', c_type), ''] )
    
    ####### Get Session Legislators --- Could get these in a similar way to COmmittees... not clear its necessary
    #legislator_url = ''
    #session_legislators = [[i['LegislatorCode'], i['Title'], i['FirstName'], i['LastName'], i['Chamber'], i['Party'], i['DistrictNumber']] for i in these_legislators]
        
    ### Return List of Bills
    print('\n\n **** ~~~> Found Data for {} Bills TOTAL for the {} **** \n'.format(len(session_bills), s_name))
    return(session_committees, session_bills)
  

#######################################################
########## Parse the Bill Data Returned from the API
#####################################################
# s = sessions[7]  
# bill = session_bills[0]
#
#def get_bill_data(bill, s): 
#
#    s_yr, s_id, s_name = s
#    bill_num, s_id, title, bill_url = bill
#    
#    #################
#    #### Basic Info
#    #################
#    bill_num = bill_num.split(' ')[0] + bill_num.split(' ')[1].zfill(4)
#    lc_num = b['LCNumber']
#    
#    keywords = ''
#    descrip = ''
#    summary = ''
#    by_request = ''
#    rev_impact = ''
#    fiscal_impact = ''
#    chapter_num = ''
#    
#    if b['RelatingTo']:
#        keywords = re.sub('Relating to |\\.$', '', b['RelatingTo'].strip())
#
#    if b['CatchLine']:
#        descrip = re.sub('  +', ' ', re.sub('\n|\t|\r', ' ', b['CatchLine'].strip()))
#
#    if b['MeasureSummary']:
#        summary = re.sub('  +', ' ', re.sub('\n|\t|\r', ' ', b['MeasureSummary'].strip()))
#    
#    if b['AtTheRequestOf']:
#        by_request = b['AtTheRequestOf']
#
#    if b['RevenueImpact']:
#        rev_impact = b['RevenueImpact']
#        
#    if b['FiscalImpact']:
#        fiscal_impact = b['FiscalImpact']
#    
#    #### Status/Outcomes
#    status = b['CurrentLocation'].strip()
#    vetoed = b['Vetoed']
#    if b['ChapterNumber']:
#        chapter_num = b['ChapterNumber'] 
#    
#    ################
#    #### SPONSORS
#    ###############
#    # ** See note at top: Need to record based on order of MeasureSponsorId (appearst o be in that order usually anyway?)
#    # ** LegislatoreCode = Typo in API
#    
#    all_sponsors = []
#    for spon in b['MeasureSponsors']:
#        if spon['SponsorType'] == 'Committee':
#            all_sponsors.append([spon['CommitteeCode'] + ' (Committee)', int(spon['MeasureSponsorId']), spon['SponsorLevel']])
#        elif spon['SponsorType'] == 'Member':
#            all_sponsors.append([spon['LegislatoreCode'], int(spon['MeasureSponsorId']), spon['SponsorLevel']])
#        elif spon['SponsorType'] == 'Presession':
#            ### BIlls are filed by President of Senate (or Speaker of House?) by request of some outside agency
#            ### Explicitly states neither in favor or opposed
#            ### See, eg, https://olis.leg.state.or.us/liz/2009R1/Downloads/MeasureDocument/SB61
#            all_sponsors.append(['Introduced presession by request', int(spon['MeasureSponsorId']), spon['SponsorLevel']])
#        else:
#            print(" **** UNCODED SPONSOR TYPE ---- {} ----> BREAK".format(bill_num))
#            break
#        ## Sort
#        all_sponsors.sort(key=lambda x: x[1])
#        
#    primary_sponsors = '; '.join([i[0] for i in all_sponsors if i[2] == 'Chief'])
#    cosponsors = '; '.join([i[0] for i in all_sponsors if i[2] == 'Regular'])          
#   
#    ################
#    #### ACTIONS
#    ###############
#    
#    bill_actions = []
#    
#    #### Committee Actions --- Don't Necessarily Need these but provides a bit more detail
#    comm_order = 1
#    for item in b['CommitteeAgendaItems']:
#        
#        date = item['MeetingDate'].split('T')[0]
#        chamber = item['CommitteCode'][0]
#        action =  '{} Committee ~ {}: {}'.format(item['CommitteCode'], item['MeetingType'], item['Action'])
#
#        bill_actions.append([bill_num, s_id, chamber, date, action, '', comm_order, ''])
#        comm_order += 1            
#
#    
#    #### All Actions, includes comm info, but less detail
#    order = 1
#    for item in b['MeasureHistoryActions']:
#
#        date = item['ActionDate'].split('T')[0]
#        chamber = item['Chamber']
#        action =  item['ActionText']
#        vote_txt = ''
#        if item['VoteText']:
#            vote_txt = item['VoteText']
#
#        bill_actions.append([bill_num, s_id, chamber, date, action, order, '', vote_txt])
#        order += 1               
#
#    ### Combine Items
#    bill_details = [bill_num, s_id, lc_num, status, rev_impact, fiscal_impact, keywords, chapter_num, vetoed, primary_sponsors, cosponsors, by_request, descrip, summary, bill_url]    
#    return([bill_details, bill_actions])
#    



########################################################
############## SCRAPE SESSION(S)
############################################
# s = sessions[0]
# bill = session_bills[0]
    
for s in sessions:
    
    #### Output Lists
    session_bill_details = [['bill_id', 'session', 'LC_num', 'status', 'rev_impact', 'fiscal_impact', 'keywords', 'chapter_num', 'vetoed', 'primary_sponsors', 'cosponsors', 'by_request', 'description', 'summary', 'bill_url']]    
    session_actions = [['bill_id', 'session', 'chamber', 'action_date', 'action', 'order', 'comm_order', 'vote']]

    print("\n ------------------- Now Scraping: {} ---------------------- \n".format(s[2]))
    
    ### Get all bills for a specific session
    session_committees, session_bills = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill in session_bills:

        ### Get Basic Details
        bill_data = get_bill_data(bill, s)
        
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
        
    #with open("OR_Legislators_{}.csv".format(s[1]), "w", newline = "") as f:
    #    writer = csv.writer(f)
    #    writer.writerows(session_legislators)
        
    with open("OR_Bill_Details_{}.csv".format(s[1]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("OR_Bill_Histories_{}.csv".format(s[1]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- {} SCRAPED + DATA SAVED  -------------\n\n\n".format(s[1]))


print("  ********************************** ALL DONE ********************************** ")
