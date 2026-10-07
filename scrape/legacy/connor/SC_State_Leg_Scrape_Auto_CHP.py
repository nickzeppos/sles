# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape SOUTH CAROLINA Legislation 1975 - PRESENT ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# **** Looping through sponsors; can't loop through subjects because response chunks too large sometimes, even with fix in OpenStates Code ***
#
####################

import csv
import os
import urllib
import requests
from bs4 import BeautifulSoup
import time
import datetime
#from dateutil import parser as dateparser
import re
# import socket

### To fixing an error that occurs when page has a lot of results -- google value error that comes up
import http.client
http_vsn = http.client.HTTPConnection._http_vsn
http_vsn_str = http.client.HTTPConnection._http_vsn_str
from pathlib import Path
import urllib.request

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])
#####################################
##### Extract Session Information
#######################################

session_request = urllib.request.Request('https://www.scstatehouse.gov/actionsearch.php', headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'})
session_search = urllib.request.urlopen(session_request, timeout = 30)
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.find('select', id = 'session')
sessions = [re.sub('\\(|\\)', '', i.get_text()).split(' - ') for i in session_list.findAll('option')]

### Drop Previously Scraped
sessions = [s for s in sessions if 'SC_Bill_Details_' + s[1].replace('-', '_') + '.csv' not in os.listdir('.')]

### Drop Pre-2023
sessions = [s for s in sessions if int(s[1][:4]) >= 2023]

### Drop Upcoming Session--not needed right now; use this code if it becomes necessary in the future
#next_year = datetime.datetime.now().year + 1
#sessions = [s for s in sessions if int(s[1][:4]) < next_year]
#print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(next_year))

del session_search, session_search_soup, session_list
#del next_year

##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):

    ### Get HTML
    try:
        request = urllib.request.Request(bill_url, headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'})
        page = urllib.request.urlopen(request, timeout = 30).read()
    except urllib.error.HTTPError: # as e
        return('HTTP Error')
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(30)
            request = urllib.request.Request(bill_url, headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'})
            page = urllib.request.urlopen(request, timeout = 45).read()
        except urllib.error.HTTPError: # as e
            return('HTTP Error')
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            request = urllib.request.Request(bill_url, headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'})
            page = urllib.request.urlopen(request, timeout = 60).read()
    time.sleep(1)

    ### Return Soup
    page_soup = BeautifulSoup(page, parser)

    return(page_soup)


#######################################################
########## GET BILLS FOR A SESSION
#####################################################
# s = sessions[0]
# test = get_session_bills(s)
# s_num = '123'

def get_session_bills(s):

    s_num, s_yrs = s

    print('\n\n\t~~~~ Gathering Bills for the {} Session by SPONSOR ~~~~\n'.format(s_yrs))

    ### Sponsors Search
    sponsor_url = 'https://www.scstatehouse.gov/sponsorsearch.php'
    form_data  = {
        'GETMEMBERS':'H',
        'SESSION':s_num,
        'PERM_SPONSOR_CODE': '0',
        'PAGETYPE':'0',
    }

    ### Representatives
    H_req = requests.post(sponsor_url, data = form_data)
    H_soup = BeautifulSoup(H_req.content, 'lxml')
    reps = [[i['value'], i.text.strip().replace('\xa0', ' '), 'H'] for i in H_soup.find('select', id = 'Representative').findAll('option')][1:]

    ### Senators
    form_data['GETMEMBERS'] = 'S'
    S_req = requests.post(sponsor_url, data = form_data)
    S_soup = BeautifulSoup(S_req.content, 'lxml')
    sens = [[i['value'], i.text.strip().replace('\xa0', ' '), 'S'] for i in S_soup.find('select', id = 'Senator').findAll('option')][1:]

    #### Output Lists
    session_bills = []
    all_actions = []
    scraped_bill_nums = [] # keeping track of scraped id's to prevent duplicates

    ### Toggle HTTP Connection Setting
    http.client.HTTPConnection._http_vsn = 10
    http.client.HTTPConnection._http_vsn_str = 'HTTP/1.0'

    #### Loop through sponsors
    #loop_data = { 'session': s_num, 'Senator': 0, 'Representative': 0, 'prime': 'Y', 'summary': 'B','headerfooter': '1'   }

    total_spon = len(reps + sens)
    num = 1
    for spon in reps + sens:

        if spon[2] == 'H':
            s_id = 0
            h_id = spon[0]
        else:
            s_id = spon[0]
            h_id = 0

        spon_soup = get_page_soup(sponsor_url + '?session={}&Senator={}&Representative={}&prime=Y&summary=B&headerfooter=1'.format(s_num, s_id, h_id))


        ## Get Bill Divs
        spon_results = spon_soup.find('div', id = 'resultsbox')
        spon_results = spon_results.findAll('div')
        # ** May need to move the findall below the none check

        ## Skip if no bills
        if 'No prime sponsored legislation during this session' in spon_results[-1].text:
            print(' -- ({}/{}) {} -- ** NO SPONSORED BILLS **'.format(num, total_spon, spon[1]))
            num += 1
            continue

        ## Check if all results recevied...
        attempts = 1
        while 'TOTAL' not in spon_results[-1].text:
            print(" ***** Partial Results... Retrying x {} ******".format(str(attempts) ) )
            time.sleep(20 * attempts)
            spon_soup = get_page_soup(sponsor_url + '?session={}&Senator={}&Representative={}&prime=Y&summary=B&headerfooter=1'.format(s_num, s_id, h_id))
            spon_results = spon_soup.find('div', id = 'resultsbox')
            spon_results = spon_results.findAll('div')
            attempts += 1
            if attempts >= 5:
                print(" ******* CANNOT GET FULL RESULTS ************** " )
                break

        ## Looping through bills
        for item in spon_results:

            # Only working with divs that contain bill info
            first_child = item.findChild()
            if first_child and first_child.name == 'a' and first_child.has_attr('name'):

                ### Basic Details
                num_only = first_child['name']
                details = item.find('span').text.strip()
                bill_num = details.split(num_only)[0].strip() + num_only
                bill_num = re.sub('\\*', '', bill_num)

                ## Skip if already scraped --- Updating primary sponsor variable if multiple
                if bill_num in scraped_bill_nums:
                    ## Appending new subject to existing data for that bill_number
                    update_row = [i for i in session_bills if i[0] == bill_num]
                    if len(update_row) != 1:
                        break
                    update_row[0][3] = update_row[0][3] + '; {}'.format(spon[1])
                    session_bills = [update_row[0] if i[0] == bill_num else i for i in session_bills]
                    continue
                else:
                    scraped_bill_nums.append(bill_num)

                bill_type, sponsors = re.split(', By |, by | By | by |, By$', details)
                if '(' in bill_type:
                    bill_type = re.sub('\\(.+\\)', '', bill_type) # remove Rat/Act #
                bill_type = bill_type.split(num_only)[1].strip()
                bill_type = re.sub('^\\(.+\\)', '', bill_type).strip()
                sponsors = re.sub(' and |, and |, ', '; ', sponsors.replace('\xa0', ' ')).strip()
                sponsors = re.sub('Ways; Means', 'Ways and Means', sponsors)
                primary_sponsor = spon[1]

                ### Title Disappears 112 and earlier
                if int(s_num) > 112:
                    title = item.find('b', string = re.compile('Summary:')).next_sibling.strip()
                    if not item.find('b', string = re.compile('Summary:')).findNext(string = re.compile('\xa0+A |\xa0+AN ')) is None:
                        summary = item.find('b', string = re.compile('Summary:')).findNext(string = re.compile('\xa0+A |\xa0+AN ')).strip()
                        summary = re.sub(' - ratified title$', '', summary)
                else:
                    title = ''
                    summary = item.find('span').next_sibling
                    if summary == 'Similar (':
                        summary = summary.findNext('br').next.strip()
                    else:
                        summary = summary.strip()

                session_bills.append([bill_num, s_yrs, bill_type, primary_sponsor, sponsors, title, summary])

                ### Get Actions on Bill
                action_table = item.find('table')
                order = 1
                if action_table is not None:
                    for row in action_table.findAll('tr'):
                        cells = row.findAll('td')
                        date = cells[0].text.strip()
                        date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')
                        chamber = cells[1].text.strip()
                        action = cells[2].text.strip()
                        journal_page = ''
                        if cells[2].find('a') and 'journal' in cells[2].find('a').text.lower():
                            journal_page = cells[2].find('a').text
                            action = re.sub('\({}\\)'.format(journal_page), '', action).strip()
                        all_actions.append([bill_num, s_yrs, chamber, date, action, journal_page, order])
                        order += 1

        print(' -- ({}/{}) {}'.format(num, total_spon, spon[1]))
        num += 1

    ### Undo HTTP Connection Changes
    http.client.HTTPConnection._http_vsn = http_vsn
    http.client.HTTPConnection._http_vsn_str = http_vsn_str

    return([session_bills, all_actions])


########################################################
############## SCRAPE SESSION(S)
############################################
# s = sessions[5]

for s in sessions:

    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'bill_type', 'primary_sponsor', 'sponsors', 'title', 'summary']]
    session_actions = [['bill_number', 'session', 'chamber', 'action_date', 'action', 'journal_page', 'order']]

    print("\n ------------------- Now Scraping the {} Session ---------------------- \n".format(s[1]))

    ### Get all urls for a session-year, including special sessions
    session_data = get_session_bills(s)
    session_bill_details = session_bill_details + session_data[0]
    session_actions = session_actions + session_data[1]

    #### Save
    with open("SC_Bill_Details_" + s[1].replace('-', '_') + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("SC_Bill_Histories_" + s[1].replace('-', '_') + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} Session SCRAPED + DATA SAVED  -------------\n\n\n".format(s[1]))


print("  ********************************** ALL DONE ********************************** ")







#######################################################################################
#### Original Code that Looped Through Subjects -- Encounters issue with too many bills as a response
#### ---> which is a problem, because we only get partial data then
#########################################################################################

#    #### Get bills via subject search
#    subject_url = 'https://www.scstatehouse.gov/subjectsearch.php'
#    form_data = {
#        'GETINDEX':'Y',
#        'SESSION':s_num,
#        'PAGETYPE': '0',
#        'INDEXCODE':'0',
#        'INDEXTEXT':'',
#        'AORB': 'B'
#    }
#
#    ### Get Subject List -- Skipping 'Select Subject Index' and '----ACTS CITED BY POPULAR NAME---'
#    s_req = requests.post(subject_url, data = form_data)
#    s_soup = BeautifulSoup(s_req.content, 'lxml')
#    subjects = [[i['value'], i.text.strip()] for i in s_soup.find('select', id = 'indexcode').findAll('option')][2:]
#
#    session_bills = []
#    all_actions = []
#    # keeping track of scraped id's to prevent duplicates
#    scraped_bill_nums = []
#
#    ### Toggle HTTP Connection Setting
#    http_vsn = http.client.HTTPConnection._http_vsn
#    http_vsn_str = http.client.HTTPConnection._http_vsn_str
#    http.client.HTTPConnection._http_vsn = 10
#    http.client.HTTPConnection._http_vsn_str = 'HTTP/1.0'
#
#    subj_num = 1
#    total_subj = len(subjects)
#    for subj in subjects:
#        #subj_req = requests.get(subject_url + '?AORB=B&session={}&indexcode={}&summary=B&headerfooter=1'.format(s_num, subj[0]))
#        #subj_soup = BeautifulSoup(subj_req.content, 'lxml')
#        subj_soup = get_page_soup(subject_url + '?AORB=B&session={}&indexcode={}&summary=S&headerfooter=1'.format(s_num, subj[0]))
#        subj_results = subj_soup.find('div', id = 'resultsbox')
#
#        ## Skip if no bills
#        if subj_results is None:
#            print(' -- ({}/{}) {}'.format(subj_num, total_subj, subj[1]))
#            subj_num += 1
#            continue
#
#        ## Looping through bills
#        subj_results = subj_results.findAll('div')
#
#        ## Check if all results recevied...
#        if 'TOTAL' not in subj_results[-1].text:
#            print(" ***** Partial Results... Retrying ******" )
#            time.sleep(30)
#            subj_soup = get_page_soup(subject_url + '?AORB=B&session={}&indexcode={}&summary=B&headerfooter=1'.format(s_num, subj[0]))
#            subj_results = subj_soup.find('div', id = 'resultsbox')
#            subj_results = subj_results.findAll('div')
#
#        for item in subj_results:
#
#            # Only working with divs that contain bill info
#            first_child = item.findChild()
#            if first_child and first_child.name == 'a' and first_child.has_attr('name'):
#
#                ### Basic Details
#                num_only = first_child['name']
#                details = item.find('span').text.strip()
#                bill_num = details.split(num_only)[0].strip() + num_only
#                bill_num = re.sub('\\*', '', bill_num)
#
#                ## Skip if already scraped (e.g., different subject)
#                if bill_num in scraped_bill_nums:
#                    ## Appending new subject to existing data for that bill_number
#                    update_row = [i for i in session_bills if i[0] == bill_num]
#                    if len(update_row) != 1:
#                        break
#                    update_row[0][6] = update_row[0][6] + '; {}'.format(subj[1])
#                    session_bills = [update_row[0] if i[0] == bill_num else i for i in session_bills]
#                    continue
#                else:
#                    scraped_bill_nums.append(bill_num)
#
#                bill_type, sponsors = re.split(', By |, by | By | by ', details)
#                bill_type = bill_type.split(num_only)[1].strip()
#                bill_type = re.sub('^\\(.+\\)', '', bill_type).strip()
#                sponsors = re.sub(' and |, and |, ', '; ', sponsors.replace('\xa0', ' ')).strip()
#                sponsors = re.sub('Ways; Means', 'Ways and Means', sponsors)
#
#                title = item.find('b', string = re.compile('Summary:')).next_sibling.strip()
#                summary = item.find('b', string = re.compile('Summary:')).findNext(string = re.compile('\xa0+A |\xa0+AN ')).strip()
#                summary = re.sub(' - ratified title$', '', summary)
#                summary = ''
#
#                session_bills.append([bill_num, s_yrs, bill_type, sponsors, title, summary, subj[1]])
#
#                ### Get Actions on Bill
#                action_table = item.find('table')
#                order = 1
#                if action_table is not None:
#                    for row in action_table.findAll('tr'):
#                        cells = row.findAll('td')
#                        date = cells[0].text.strip()
#                        date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')
#                        chamber = cells[1].text.strip()
#                        action = cells[2].text.strip()
#                        journal_page = ''
#                        if cells[2].find('a') and 'journal' in cells[2].find('a').text.lower():
#                            journal_page = cells[2].find('a').text
#                            action = re.sub('\({}\\)'.format(journal_page), '', action).strip()
#                        all_actions.append([bill_num, s_yrs, chamber, date, action, journal_page, order])
#                        order += 1
#
#        print(' -- ({}/{}) {}'.format(subj_num, total_subj, subj[1]))
#        subj_num += 1
#
#
#    ### Undo HTTP Connection Changes
#    http.client.HTTPConnection._http_vsn = http_vsn
#    http.client.HTTPConnection._http_vsn_str = http_vsn_str
#
#    return([session_bills, all_actions])
