# -*- coding: utf-8 -*-
"""
Created on Fri Feb 1 2019

~~~~~~~ Scrape MISSOURI HOUSE Legislation 1995 - PRESENT ~~~~~~~~~~~~

@author: PB
"""

##### NOTES:
# Need to Scrape House and Senate Pages Seperately...
#
# Main House Page Only Goes back to 2000, but can get House Bills by Iterating through Years, starting with:
# https://house.mo.gov/content.aspx?info=/bills95/bills95/HB1.HTM ---> HB764.HTM
# then: https://house.mo.gov/content.aspx?info=/bills96/bills96/HB765.HTM
# ALso do HCR's (start at 1), HJRs (Continue); HECs
# If: 'The file you are looking for is not available' -- check if hb or next year starts at same point
#
#
# ******* CAN GET BILL NUMBERS AND ITTLES (SANS DETAILS) AT: https://house.mo.gov/billtracking/bills95/billist.htm **********
# ******* BILL PAGES MAY HAVE MOVED??? NEW FORMAT...?: https://house.mo.gov/billtracking/bills97/bills97/HB116.HTM ****
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
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#####################################
##### Extract Session Information
#######################################
# ** Could just do a range of years, but keeping this way in case something changes down the line....

session_search = urllib.request.urlopen('https://house.mo.gov/LegislationSP.aspx', timeout = 30)
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
sessions = []
session_list = session_search_soup.find('div', id = 'ExpandedPanel4').findAll('a')

### Need GA Numbers (e.g., 99th)
this_year = datetime.datetime.now().year
start_ga = 88
ga_dict = {}
for y in range(1995, this_year, 2):
    if start_ga % 10 in [0, 4, 5, 6, 7, 8, 9]:
        use_ga = str(start_ga) + 'th'
    else:
        use_ga = str(start_ga) + {1:'st', 2:'nd', 3:'rd'}[start_ga % 10]
    ga_dict[y] = use_ga
    ga_dict[y + 1] = use_ga
    start_ga += 1

### Start by adding current session--won't be listed with the others
current_name = session_search_soup.find('select', id = 'sessions').find('option', selected = "").get_text().strip()
current_year, current_type = current_name.split(' ', 1)
current_year = int(current_year)
current_type = current_type.replace('Regular Session', 'RS')
current_type = current_type.replace('Extraordinary Session', 'ES')
current_type = current_type.replace('Special Session', 'SS')
current_num = ga_dict[this_year]
current_session = session_search_soup.find('div', id = 'ExpandedPanel1').find('a', id = 'Bill List')
current_url = 'https://house.mo.gov/' + current_session['href']
sessions.append([current_num, current_year, current_type, current_url])

### Creating List of Session Urls -- Format splits in MIddle
for a in session_list:
    yr, s_type = a.get_text().split(' ', 1)
    yr = int(yr)
    ga_num = ga_dict[yr]
    s_type = re.sub('Opens in a.+', '', s_type)
    s_type = re.sub('.+ – ', '', s_type.strip()).replace('Regular Session', 'RS')
    #s_type = re.sub('.+ – ', '', a.get_text().strip()).replace('Regular Session', 'RS')
    s_type = re.sub(str(yr) + ' ', '', s_type).replace('Extraordinary Session', 'ES')
    s_type = re.sub(str(yr) + ' ', '', s_type).replace('Special Session', 'SS')
    if 'PastSessions.aspx' in a['href']:
        s_url = a['href'].replace('PastSessions.aspx', 'https://house.mo.gov/billlist.aspx')
        s_url = s_url + '&select=chamber:h'
    elif 'PastSessionsHTML' in a['href'] and yr >= 2003:
        s_url = 'https://house.mo.gov/billtracking/bills{}/billist.htm'.format(a['id'])
        if yr == 2003 and s_type == "1st ES":
            s_url = 'https://house.mo.gov/billtracking/spec03/billist.htm'
        elif yr == 2003 and s_type == "RS":
            s_url = 'https://house.mo.gov/billtracking/bills03/billist.htm'
    else:
        req = requests.get('https://house.mo.gov/' + a['href'])
        time.sleep(.5)
        req_soup = BeautifulSoup(req.content, 'lxml')
        s_url = req_soup.find('a', href = re.compile('billist.htm'))
        s_url = 'https://house.mo.gov' + s_url['href']

    sessions.append([ga_num, yr, s_type, s_url])
    # print(str(yr) + ' --- ' + ga_num + ' --- ' + s_type)

### Appending Older Years
sessions.append(['90th', 1999, 'RS', 'https://house.mo.gov/billtracking/bills99/billist.htm'])
sessions.append(['89th', 1998, 'RS', 'https://house.mo.gov/billtracking/bills98/billist.htm'])
sessions.append(['89th', 1997, 'RS', 'https://house.mo.gov/billtracking/bills97/billist.htm'])
sessions.append(['89th', 1997, '1st ES', 'https://house.mo.gov/billtracking/spec97/billist.htm'])
sessions.append(['88th', 1996, 'RS', 'https://house.mo.gov/billtracking/bills96/billist.htm'])
sessions.append(['88th', 1995, 'RS', 'https://house.mo.gov/billtracking/bills95/billist.htm'])

### Drop Previously Scraped
sessions = [s for s in sessions if 'MO_Bill_Details_' + s[0] + '_House.csv' not in os.listdir('House')]
sessions = [s for s in sessions if s[1] > 2022]

### Drop Current Session
#this_year = datetime.datetime.now().year
#sessions = [s for s in sessions if int(s[1]) < this_year]
#print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))

del session_search, session_search_soup, session_list
del this_year, start_ga, y, ga_dict

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# ga_num = '95th'


def get_session_bills(ga_num):

    s = [s for s in sessions if s[0] == ga_num]

    print('\n~~~~ Gathering Bill URLs for the {} HOUSE Session ~~~~\n'.format(ga_num))

    session_bills = []
    for i in range(0, len(s)):
        s_url = s[i][3]
        s_year = s[i][1]
        s_type = s[i][2]

        ### Go To Bill List Page
        s_page = requests.get(s_url)
        time.sleep(.5)
        s_soup = BeautifulSoup(s_page.content, 'lxml')

        ### Get URLs
        if s_year > 2010:
            bill_stem = 'https://house.mo.gov/'
            these_a_tags = s_soup.findAll('a', href = re.compile('Bill.aspx\\?bill'))

            ### Need to correct the URLs to go the frame with data --> e.g., BillContent.aspx?bill=GRP1&year=2012&code=R&style=new
            these_urls = [bill_stem + re.sub('Bill.aspx\\?', 'BillContent.aspx?', i['href'].strip()) + '&style=new' for i in these_a_tags]
            these_urls = [[ga_num, s_year, s_type, i.get_text().strip(), url ] for i, url in zip(these_a_tags, these_urls)]

        else:
            these_urls = s_soup.findAll('a', href = re.compile('/bills/H'))
            if these_urls == []:
                these_urls = s_soup.findAll('a', href = re.compile('/bills{}/H'.format(str(s_year)[2:4])))

            bill_stem = 'https://house.mo.gov'
            if 'http' not in these_urls[0]['href'] and these_urls[0]['href'][0:1] == '/':
                these_urls = [[ga_num, s_year, s_type, re.sub(' ', '', i.get_text()).strip(), bill_stem + i['href'].strip() ] for i in these_urls]
            elif 'http' not in these_urls[0]['href']:
                these_urls = [[ga_num, s_year, s_type, re.sub(' ', '', i.get_text()).strip(), bill_stem + '/' + i['href'].strip() ] for i in these_urls]
            else:
                these_urls = [[ga_num, s_year, s_type, re.sub(' ', '', i.get_text()).strip(), i['href'].strip() ] for i in these_urls]

        ## Append To Main File
        for bill in these_urls:
            session_bills.append(bill)

        print(' -- {}, {}'.format(s_year, s_type))

    return(session_bills)



##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):

    ### Get HTML
    try:
        page = urllib.request.urlopen(bill_url, timeout = 15)
    except urllib.error.HTTPError: # as e
        return('HTTP Error')
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(15)
            page = urllib.request.urlopen(bill_url, timeout = 30)
        except urllib.error.HTTPError: # as e
            return('HTTP Error')
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = urllib.request.urlopen(bill_url, timeout = 60)
    time.sleep(.5)

    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = session_urls[0]

def get_bill_data(bill_row):

    ### Bill Data
    ga_num, s_year, s_type, bill_num, bill_url = bill_row

    ### Standardize Bill Number
    bill_num_z = re.sub('[0-9].+|[0-9]$', '', bill_num ) + re.sub('^[A-Z]+', '', bill_num ).zfill(4)

    ### Get HTML, Soup
    bill_soup = get_page_soup(bill_url)

    ### If Error, Exit
    if bill_soup == 'HTTP Error':
        return('HTTP Error')

    ### For Actions
    bill_actions = []
    order = 1

    ## Extract Data ~~~~ 2005 Onward
    if s_year > 2010:

        ### Table with Bill Info
        bill_junk = bill_soup.find('div', id = 'BillJunk')
        if bill_junk is None:
            return('No Data')
        else:
            bill_junk = bill_junk.find('table')


        title = '' #bill_soup.find('span', id = 'lblBillTitle').get_text().strip()
        description = bill_soup.find('div', {'class':'BillDescription'}).get_text().strip()

        ### Chamber Sposnor
        sponsor_tag = bill_junk.find('th', text = re.compile('^Sponsor'))
        sponsor_tag = sponsor_tag.findNextSibling('td').findAll('a')
        if sponsor_tag == []:
            sponsor_tag = bill_junk.find('th', text = re.compile('^Sponsor'))
            sponsors = sponsor_tag.findNextSibling('td').get_text().strip()
            sponsor_urls = ''
        else:
            sponsor_urls = '; '.join([i['href'] for i in sponsor_tag])
            sponsors = '; '.join([i.get_text().strip() for i in sponsor_tag])

        ### Outchamber Sponsor
        outchamber_spon = ''

        ### Cosponsors
        cospon_tag = bill_junk.find('th', text = re.compile('^Co.+onsor'))
        if cospon_tag is not None:
            cospon_tag = cospon_tag.findNextSibling('td').findAll('a')
            if cospon_tag == []:
                cospon_tag = bill_junk.find('th', text = re.compile('^Co.+onsor'))
                cosponsors = cospon_tag.findNextSibling('td').get_text().strip()
            else:
                cosponsors = '; '.join([i.get_text().strip() for i in cospon_tag])
        else:
            cosponsors = ''

        lr_num = bill_junk.find('th', text = re.compile('^LR Num')).findNextSibling('td').get_text().strip()
        #journal_page = bill_soup.find('span', id = 'lblJrnPage').get_text().strip()


        effective_date = bill_junk.find('th', text = re.compile('Effective Date')).findNextSibling('td').get_text().strip()

        ### summarys are all pdfs -- could easily turn them to text, but not clear needed
        summary = ''

        #### Get Bill Actions --- ./BillActions.aspx?bill=HB1030&year=2012&code=R
        action_soup = get_page_soup(bill_url.replace('BillContent.aspx', 'BillActions.aspx').replace('&style=new', ''))
        action_table = action_soup.find('table', id = 'actionTable')
        action_rows = action_table.findAll('tr')

        committees = action_soup.findAll('a', href = re.compile('committees'))
        if committees is not None:
            committees = '; '.join([re.sub('^.+: ', '', i.text.strip()) for i in committees])
        else:
            committees = ''

        for row in action_rows[1:]:
            cells = row.findAll('td')
            date = datetime.datetime.strptime(cells[0].get_text(), '%m/%d/%Y').strftime('%Y-%m-%d')
            action = re.sub('\xa0|\r\n|\n|\r', '', cells[2].get_text().strip())
            journal_page = cells[1].get_text().strip()
            bill_actions.append([bill_num_z, ga_num, s_year, s_type, '', date, action, journal_page, order])
            order += 1

    ## Extract Data ~~~~ 1995 - 2005 Onward
    else:

        title = bill_soup.findAll('title')
        if len(title) == 1:
            title = title[0].get_text()
        else:
            title = title[1].get_text()

        if len(title.split(' - ')) == 3:
            title = title.split(' - ')[1]

        bn_space = re.sub('[0-9].+|[0-9]$', '', bill_num ) + " " + re.sub('H[A-Z]+', '', bill_num )
        description = bill_soup.find('b', text = re.compile(bn_space + "|" + bill_num))
        description = description.findParents('td')[0]
        description = description.findNextSibling('td').get_text().strip()
        description = re.sub('\r\n|\n|\s\s+', ' ', description)

        ### Chamber Sposnor -- THis will always get the main sponsor, but might miss secondary sponsors occassionally
        ### See: https://house.mo.gov/content.aspx?info=/bills061/bills/HB1479.htm
        sponsor_tag = bill_soup.find('b', text = "Sponsor:").parent
        sponsor_tag = sponsor_tag.findNextSibling('td').findAll('a')
        if sponsor_tag == []:
            sponsor_tag = bill_soup.find('b', text = "Sponsor:").parent
            sponsors = sponsor_tag.findNextSibling().find('em').get_text().strip()
            sponsor_urls = ''
        else:
            sponsors = '; '.join([i.get_text().strip() for i in sponsor_tag])
            sponsor_urls =  [i['href'] for i in sponsor_tag]# )
            if 'http' not in sponsor_urls[0]:
                sponsor_urls = '; '.join(['https://house.mo.gov' + i for i in sponsor_urls])
            else:
                sponsor_urls = '; '.join(sponsor_urls)

        ### Outchamber Sponsor
        outchamber_spon = ''

        ### Cosponsors -- May not always include ALL cosponsors
        cosponsor_tag = bill_soup.find('b', text = "CoSponsor:")
        if cosponsor_tag is not None:
            cosponsor_tag = cosponsor_tag.parent.findNextSibling('td').findAll('a')
            if cosponsor_tag != []:
                cosponsors = '; '.join([i.get_text().strip() for i in cosponsor_tag])
            else:
                cosponsors = ''
        else:
            cosponsors = ''

        lr_num = bill_soup.find('b', text = re.compile('LR Number:'))
        lr_num = re.sub('LR Number:', '', lr_num.parent.get_text()).strip()
        if lr_num == '':
            lr_num = bill_soup.find('b', text = re.compile('LR Number:')).parent.nextSibling.get_text().strip()


        committees = ''

        effective_date = bill_soup.find('b', text = re.compile('Effective Date:')).nextSibling
        if effective_date is None:
            effective_date = bill_soup.find('b', text = re.compile('Effective Date:')).parent.nextSibling.get_text().strip()
        ## This must be missing data - common in mid-90s
        if effective_date == '00/00/00':
            effective_date = ''


        summary = bill_soup.find('a', {'name':['introduced', 'perfected']})
        if summary is not None:
            summary = summary.find('pre')
            summary = [re.sub('\r\n', ' ', i).strip() for i in re.split('\r\n\r\n', summary.get_text()) if i.strip() != '']
            if summary == []:
                summary = ''
            else:
                summary = ' '.join(summary) #summary[len(summary) - 1]
        else:
            summary = ''

        #### Get Bill Actions
        yr_stem = str(s_year)[2:4]
        if s_year >= 2003:
            action_url = re.sub('bills/H', 'action/aH', bill_url)
        else:
            action_url = bill_url.replace('bills' + yr_stem + '/H', 'action' + yr_stem + '/aH')

        action_soup = get_page_soup(action_url, parser = 'html5lib')
        action_table = action_soup.find('table')
        action_rows = action_table.findAll('tr')

        for row in action_rows[1:]:
            cells = row.findAll('td')
            if cells == []:
                continue
            date = cells[0].get_text().strip()
            if date == '':
                date = bill_actions[-1][5]
            elif date in ['00/31/4200', '00/22/3199']:
                date_dict = {'00/31/4200':'03/14/2000', '00/22/3199':'02/23/1999'}
                date = datetime.datetime.strptime(date_dict[date], '%m/%d/%Y').strftime('%Y-%m-%d')
            elif len(date.split('/')[2]) == 4:
                date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            else:
                date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')

            journal_page = cells[1].get_text().strip()
            action = re.sub('\xa0|\r\n|\n|\r', '', cells[2].get_text().strip())
            action = re.sub('\s\s+', ' ', action)

            bill_actions.append([bill_num_z, ga_num, s_year, s_type, '', date, action, journal_page, order])
            order += 1

    #####################
    ### OUTPUT
    #####################

    bill_details = [bill_num_z, ga_num, s_year, s_type, title, description, sponsors, sponsor_urls, cosponsors, outchamber_spon, lr_num, committees, effective_date, summary, bill_url]

    return([bill_details, bill_actions])

########################################################
############## SCRAPE SESSION(S)
############################################
# bill_row = session_urls[1229]

### *** LOOPING THROUGH NUMBERED SESSIONS --- EG, 99th, not years ********
all_ga_nums = sorted(set([s[0] for s in sessions]))

for ga_num in all_ga_nums:

    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'session_year', 'session_type', 'title', 'description', 'primary_sponsor', 'ps_url', 'cosponsors', 'outchamber_sposnor', 'LR_number', 'committees', 'effective_date', 'summary', 'bill_url']]
    session_actions = [['bill_number', 'session', 'session_year', 'session_type', 'chamber', 'action_date', 'action', 'journal_page', 'order']]

    print("\n ------------------- Now Scraping the {} Session ---------------------- \n".format(ga_num))

    ### Get all urls for a session-year, including special sessions
    session_urls = get_session_bills(ga_num)

    #### Loop through bills
    num = 1
    total = len(session_urls)
    for bill_row in session_urls:

        bill_data = get_bill_data(bill_row)

        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n URL: {} \n **********".format(num, total, bill_row[0], bill_row[4]))
            num += 1
            continue
        elif bill_data == "No Data":
            print(" ********** \n ({}/{}) -- {} -- NO DATA --- SKIPPING \n URL: {} \n **********".format(num, total, bill_row[0], bill_row[4]))
            num += 1
            continue

        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[3], bill_row[4]))
        num += 1

    with open("House/MO_Bill_Details_" + ga_num + "_House.csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("House/MO_Bill_Histories_" + ga_num + "_House.csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} HOUSE Session SCRAPED + DATA SAVED  -------------\n\n\n".format(ga_num))


print("  ********************************** ALL DONE ********************************** ")








############## CODE FOR > 2010 --- Save until know it works as written
#title = bill_soup.find('span', id = 'lblBillTitle').get_text().strip()
#description = bill_soup.find('span', id = 'lblBriefDesc').get_text().strip()
#
#### Chamber Sposnor
#sponsor_tag = bill_soup.findAll('a', id = 'hlSponsor')
#sponsor_urls = '; '.join([i['href'] for i in sponsor_tag])
#sponsors = '; '.join([i.get_text().strip() for i in sponsor_tag])
#
#### Outchamber Sponsor
#if bill_num[0:1] == 'H':
#    outchamber_spon = bill_soup.find('a', id = 'hlSSponsor').get_text().strip()
#elif bill_num[0:1] == 'S':
#    outchamber_spon = bill_soup.find('a', id = 'hlHSponsor').get_text().strip()
#else:
#    outchamber_spon = ''
#
#### Cosponsors
#cospon_url = bill_soup.find('a', id = 'hlCoSponsors')
#if cospon_url is not None:
#    cospon_soup = get_page_soup(bill_url.replace("Bill.aspx", "CoSponsors.aspx"))
#    cosponsors = cospon_soup.findAll('a', id = re.compile('dgCoSponsors'))
#    cosponsors = '; '.join([c.get_text().strip() for c in cosponsors])
#else:
#    cosponsors = ''
#
#lr_num = bill_soup.find('span', id = 'lblLRNum').get_text().strip()
##journal_page = bill_soup.find('span', id = 'lblJrnPage').get_text().strip()
#
#committees = bill_soup.find('a', id = 'hlCommittee')
#committees = '; '.join([c.get_text().strip() for c in committees])
#
#effective_date = bill_soup.find('span', id = 'lblEffDate').get_text().strip()
#
#summary = bill_soup.find('span', id = 'lblSummary').get_text().strip()
#summary = summary.replace('\t', ' ')
#
##### Get Bill Actions
#action_soup = get_page_soup(bill_url.replace('Bill.aspx', 'Actions.aspx'))
#action_table = action_soup.find('table', id = 'Table5').findAll('table')[1]
#action_rows = action_table.findAll('tr')
#
#for row in action_rows:
#    cells = row.findAll('td')
#    date = datetime.datetime.strptime(cells[0].get_text(), '%m/%d/%Y').strftime('%Y-%m-%d')
#    action = re.sub('\xa0|\r\n|\n|\r', '', cells[1].get_text().strip())
#    journal_page = cells[2].get_text().strip()
#    bill_actions.append([bill_num_z, ga_num, s_year, s_type, '', date, action, journal_page, order])
#    order += 1
