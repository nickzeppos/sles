# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape IDAHO Bills

@author: PB
"""

##### NOTES:
# --- Need to find a way to validate teh sponsors; mostly committees; sometimes co-sponsors available but requires pdf scraping
# --- 'Floor sponsors' exist but they only are shown on 3rd reading and aren't necessarily the author/cosponsors
###########################

import csv
import os
import urllib
import urllib.request
from bs4 import BeautifulSoup
import time
import datetime
import re
import socket
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#######################
##### Extract Session Links
########################

session_search = urllib.request.urlopen('https://legislature.idaho.gov/sessioninfo/2018/legislation/minidata/', timeout = 30)
session_search_soup = BeautifulSoup(session_search, 'lxml')

### Get Sessions
session_list = session_search_soup.find('select', id = 'ddlsessions').findAll('option')

#session_urls = [i['value'] for i in session_list]
sessions_full = [i.get_text() for i in session_list]
sessions_short = [ i.replace(' Extraordinary', 'spcl').replace(' Session', '') for i in sessions_full]

### Drop Previously Scraped
# sessions = [i for i in sessions_short]
sessions = [i for i in sessions_short if 'ID_Bill_Details_' + i + '.csv' not in os.listdir('.')]

### Drop pre-2023
sessions = [i for i in sessions if int(i[:4]) >= 2023]

### Drop Current Year
#this_year = datetime.datetime.now().year
#sessions = [i for i in sessions if not re.search("^" + str(this_year), i)]

del session_search, session_search_soup, session_list, sessions_full, sessions_short#, this_year

#######################################################
########## GET BILL URLS VIA FTP SITE FOR A SESSION
#####################################################

def get_session_bills(session):
    '''
    NOTE: Sessions are 1-year but bill numbers grouped by term (e.g., 2009-2010)
    This means that first bill of 2010 session picks up at last number + 1 of 2009 session
    '''

    session_url = 'https://legislature.idaho.gov/sessioninfo/{}/legislation/minidata/'.format(session)

    try:
        session_page = urllib.request.urlopen(session_url, timeout = 15)
    except urllib.error.HTTPError as e:
        print(' ---> Session request failed with error code - %s.' %e.code)
        if e.code == 404:
            return('404 error')
        else:
            print("\n ~~> Retrying Session Page Request")
            time.sleep(10)
            session_page = urllib.request.urlopen(session_url, timeout = 60)

    session_soup = BeautifulSoup(session_page, 'lxml')

    ############
    ### Extract URLs to Bill Pages + Basic Info --- TWO DIFFERENT VERSIONS
    #############
    bill_urls = []

    if int(session[0:4]) <= 2008:
        ## ** 1999 T0 2008 **

        a_tags = session_soup.findAll('a', href = re.compile('legislation/[A-Za-z]'))

        for a in a_tags:
            bill_num = a.get_text()
            this_url = 'https://legislature.idaho.gov' + a['href']
            bill_data = re.split('\r\n|\n', a.nextSibling)[0]
            short_title, status = [i.strip() for i in re.split('\\.\\.\\.+', bill_data)]
            bill_urls.append([bill_num, session, short_title, status, this_url])
    else:
        ## ** 2009 TO PRESENT **

        bill_rows = session_soup.findAll('tr', id = re.compile('bill[A-Z]'))

        for row in bill_rows:
            cells = row.findAll('td')
            bill_num = cells[0].get_text().strip()
            this_url = 'https://legislature.idaho.gov' + cells[0].find('a')['href']
            short_title = cells[1].get_text().strip()
            status = cells[3].get_text().strip() + cells[4].get_text().strip()
            bill_urls.append([bill_num, session, short_title, status, this_url])

    return(bill_urls)



##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):

    ### Get HTML
    try:
        page = urllib.request.urlopen(bill_url, timeout = 30).read()
    except urllib.error.HTTPError: # as e
        return('HTTP Error')
    except socket.timeout:
        print("\n ~~> SOCKET TIMEOUT --- Retrying Bill Request")
        time.sleep(120)
        return(get_page_soup(bill_url))
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(30)
            page = urllib.request.urlopen(bill_url, timeout = 450).read()
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



#######################
###### Functions to Scrape Individual Bills
#########################
# bill_url = 'https://legislature.idaho.gov/sessioninfo/2007/legislation/H0001/'
# bill_url = 'https://legislature.idaho.gov/sessioninfo/2012/legislation/SR103/'
# sy = '2007'

def get_bill_data(sy, bill_info):

    bill_num = bill_info[0]
    short_title = bill_info[2]
    status = bill_info[3]
    bill_url = bill_info[4]

    ### Get Bill Data
    bill_soup = get_page_soup(bill_url)
    bill_actions = []

    if bill_soup == "HTTP Error":
        return(bill_soup)

    if int(sy[0:4]) <= 2008:
        ## ** 1999 T0 2008 **
        bill_status = bill_soup.find('a', text = re.compile('Bill Status|Daily Data Tracking History')).findNext('pre')
        bill_status = [i for i in bill_status.get_text().split('\n') if i != '']
        ### Removing \r's and ******* only rows (some resolutions)
        bill_status = [i.strip() for i in bill_status if i.strip() != '' and re.sub('\\*+', ' ', i).strip() != '']

        ### Possible this is the author
        ### *** Floor Sponsor often listed in history but requires a third reading (see: https://legislature.idaho.gov/resources/howabillbecomesalaw/)
        ### Weirdly, Floor sponsors is not always a cosponsor... see: https://legislature.idaho.gov/sessioninfo/2018/legislation/s1341/
        author = re.sub('^.+by ', '', bill_status[0])
        cosponsors = ''
        del bill_status[0]

        ### URL to Statement of Purpose
        SOP_url = "use_bill_url"

        ### Grab the Contact from the SOP --> = REQUESTOR of Bill
        # *** But note: not all requestors == Legislators; but can get them but subsetting to names starting with Rep or Sen
        SOP_box = bill_soup.find('a', {"name": "sop"})

        ### Check if ANY SOP BOX -- Can be completelu missing sometimes
        if SOP_box is None and bill_soup.find(text = re.compile('^CONTACT: |^Contact: ')) is None:
            requestor_SOP = ''
            requestor_SOP_full = ''
        else:
            SOP_box = SOP_box.findNext('pre')

            ### Use different method for handful of pages with no SOP box (it's listed after instead)
            if SOP_box is None or SOP_box.text.strip() == "":
                requestor_SOP = bill_soup.find(text = re.compile('^CONTACT:|^Contact:|^\nContact:'))
                if requestor_SOP is None:
                    requestor_SOP = bill_soup.find(text = re.compile('^CONTACT:|^Contact:|^ +Contact:'))
                requestor_SOP_full = requestor_SOP.findNextSibling(text = re.compile('[A-Za-z]'))
                if not requestor_SOP_full:
                    requestor_SOP_full = requestor_SOP.findParent('p').findNextSibling('p')
                    if requestor_SOP_full is None:
                        requestor_SOP_full = ''
                    else:
                        requestor_SOP_full = requestor_SOP_full.text
                elif hasattr(requestor_SOP_full, 'text'):
                    if 'Bill Number:' in requestor_SOP_full.text:
                        requestor_SOP_full = requestor_SOP.findParent('p').findNextSibling('p').span.text
                requestor_SOP = re.sub('^CONTACT:|^Contact:|[0-9][0-9].+', '', requestor_SOP.strip()).strip()
                if re.search('\n|\r', requestor_SOP):
                    split_req = [re.sub(', +$', '', i).strip() for i in re.split('\r|\n', requestor_SOP) if i.strip() not in ['', 'or', 'and']]
                    requestor_SOP = split_req[0]
                    requestor_SOP_full = '; '.join(split_req) + '; ' + requestor_SOP_full.strip()
                else:
                    requestor_SOP_full = requestor_SOP + '; ' + requestor_SOP_full.strip()
                requestor_SOP_full = re.sub('; $', '', requestor_SOP_full)
            else:
                SOP_box = [i for i in SOP_box.get_text().split('\n') if i != '']
                SOP_box = [i.strip() for i in SOP_box if i.strip() != '' and re.sub('\\*+', '', i).strip() != '']
                SOP_box = [re.sub('\t|  +', ' ', i) for i in SOP_box]

                contact_index = [i for i, j in enumerate(SOP_box) if re.search('^CONTACT', j.upper())]
                if len(contact_index) > 1:
                    contact_with_colon = [i for i, j in enumerate(SOP_box) if re.search('^CONTACT:|^CONTACT$', j.upper())]
                    if len(contact_with_colon) == 1:
                        contact_index = contact_with_colon
                    else:
                        print("MULTIPLE CONTACT INFO MATCHES ~~~~~  USING FIRST CONTACT REFERENCE ~~~~~ SEE: {}".format(bill_url))
                elif len(contact_index) == 0:
                    contact_index = [i for i, j in enumerate(SOP_box) if re.search('^SPONSOR', j.upper())]
                    if len(contact_index) == 0:
                        contact_index = [i for i, j in enumerate(SOP_box) if re.search('^NAME: ', j.upper())]

                #### Skip Remaining Missing --- Some of these may list co-sponsors
                # -- See, eg, https://legislature.idaho.gov/sessioninfo/2008/legislation/H0599
                if len(contact_index) == 0:
                    print("MISSING CONTACT INFO MATCHES ~~~~~ SEE: {}".format(bill_url))
                    requestor_SOP = ''
                    requestor_SOP_full = ''
                else:
                    ### Drop Everything Pre-Contact Info AND Drop Final Row (whcih is often all uppercase)
                    contact_index = contact_index[0]
                    SOP_box = SOP_box[(contact_index):]
                    SOP_box = [i for i in SOP_box if not re.search('statement of purpose|fiscal note', i.lower())]   # Dropped: i.upper() != i --> errors when CONTACT is all caps
                    #### Drop First Line if Just Contact
                    if re.search("^CONTACT$|^CONTACT:$", SOP_box[0].upper().strip()):
                        SOP_box = SOP_box[1:]
                    if SOP_box == []:
                        requestor_SOP = ''
                        requestor_SOP_full = ''
                    else:
                        requestor_SOP = re.sub('Contact: +|Name: +|Sponsor: +|CONTACT: +|NAME: +|SPONSOR: +', '', SOP_box[0])
                        ## Drop Excess Agencies from name from earlier years
                        if re.search(',', requestor_SOP) and not re.search(', [jr|sr|ii+|iv]', requestor_SOP):
                            requestor_SOP = re.sub(',.+', '', requestor_SOP)
                        requestor_SOP_full = '; '.join([re.sub('Contact:|Name:|Sponsor:|CONTACT:|NAME:|SPONSOR:', '', i).strip() for i in SOP_box])

        ### Skip To History
        summary = ''
        while True:
            text = bill_status[0]
            if re.match('^\d\d/\d\d', text) is None:
                summary = summary + ' ' + text
                bill_status.remove(text)
            else:
                break
        summary = summary.strip()

        ### Get Floor Sponsor(s)
        # **** Note: Sometimes Floor Spon length > 2 ---> Occurs when bill goes back to originating chamber...
        # **** Example: https://legislature.idaho.gov/sessioninfo/1999/legislation/H0055/
        # **** ---> Using just the first two matches; error likely to be minimal
        floor_sponsors = [i for i in bill_status if re.search('Floor\\sSponsor', i)]
        floor_sponsors = [re.sub('\xa0', ' ', i).strip() for i in floor_sponsors]
        H_floor_sponsor = ''
        S_floor_sponsor = ''
        if len(floor_sponsors) > 0:
            if bill_num[0] == "H":
                H_floor_sponsor = "Representative " + re.sub('Floor (Sponsors|Sponsor) -', '', floor_sponsors[0]).strip()
                if len(floor_sponsors) >= 2:
                    S_floor_sponsor = "Senator " + re.sub('Floor (Sponsors|Sponsor) -', '', floor_sponsors[1]).strip()
            elif bill_num[0] == "S":
                S_floor_sponsor = "Senator " + re.sub('Floor (Sponsors|Sponsor) -', '', floor_sponsors[0]).strip()
                if len(floor_sponsors) >= 2:
                    H_floor_sponsor = "Representative " + re.sub('Floor (Sponsors|Sponsor) -', '', floor_sponsors[1]).strip()

        ### Drop Everything From Effective Date On
        eff_date_index = [i for i, txt in enumerate(bill_status) if re.match('Effective: \d\d', txt)]
        if len(eff_date_index) != 0:
            bill_status = bill_status[:eff_date_index[-1]]

        ### Collapse Actions to Single Item
        for i in reversed(range(len(bill_status))):
            if re.match('^\d\d/\d\d', bill_status[i]) is None:
                bill_status[i - 1] = bill_status[i - 1] + '; ' + bill_status[i]
                bill_status[i] = ''
        actions = [re.sub('\s+|; $', ' ', a).strip() for a in bill_status if a != '']

        #### Check Last Row for Occassional Effective Date Error (starts in 2002)
        #if sy == '2002' and bill_num == "H0442": actions = actions[:-1] # Dropping the incorrectly formatted effective date that errors out
        if re.search('^\d\d/\d\d/\d\d,|^\d\d/\d\d/\d\d$', actions[-1]):
            actions = actions[:-1]

        ### Actions To List
        order = 1
        for item in actions:
            date, action = [j.strip() for j in item.split(' ', 1)]
            ### Adjusting for Date Errrors --- Month or day > what is possible -- Benchmarking of previous date
            ### E.g., https://legislature.idaho.gov/sessioninfo/2006/legislation/H0570/
            if date == '':
                date = bill_actions[-1][2]
            elif len(date) == 8:
                date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')
            elif int(date[:2]) > 12:
                last_date = bill_actions[-1][2]
                if int(date[3:5]) > int(last_date[-2:]):
                    date = last_date[5:7] + date[2:]
                else:
                    date = str(int(last_date[5:7]) + 1) + date[2:]
                date = datetime.datetime.strptime(date + '/' + sy[0:4], '%m/%d/%Y').strftime('%Y-%m-%d')
            elif int(date[3:5]) > 31:
                last_date = bill_actions[-1][2]
                date = date[0:3] + last_date[-2:]
                date = datetime.datetime.strptime(date + '/' + sy[0:4], '%m/%d/%Y').strftime('%Y-%m-%d')
            else:
                date = datetime.datetime.strptime(date + '/' + sy[0:4], '%m/%d/%Y').strftime('%Y-%m-%d')

            bill_actions.append([bill_num, sy, date, action, order])
            order += 1

    else:
        ## ** 2009 TO PRESENT **

        author = bill_soup.find('td',{'style':re.compile('2px dotted')}).findNext('td')
        author = author.get_text().replace('by ', '').strip()

        cosponsors = bill_soup.find('a', text = re.compile('Legislative Co-sponsors'))
        if cosponsors is not None:
            cosponsors = 'https://legislature.idaho.gov' + cosponsors['href']
        else:
            cosponsors = ''

        ### URL to Statement of Purpose (PDF)
        SOP_url = bill_soup.find('a', id = re.compile('SOP$'))
        if SOP_url:
            SOP_url = "https://legislature.idaho.gov" + SOP_url['href']
        else:
            SOP_url = bill_soup.find('a', text = re.compile('Statement of Purpose'))
            if SOP_url:
                SOP_url = "https://legislature.idaho.gov" + SOP_url['href']
            else:
                SOP_url = re.sub(".gov", ".gov/wp-content/uploads", bill_url) + "SOP.pdf"

        ### Blank for requestor_SOP Scraped from pre-2008
        requestor_SOP = 'in_PDF'
        requestor_SOP_full = 'in_PDF'

        ### SUmmary
        summary_table = bill_soup.findAll('table', {'class':'bill-table'})
        if len(summary_table) == 3:
            summary_table = summary_table[1]
            # ** Will break if not 3 tables... which is good, so i can adapt the code correctly
        summary = summary_table.get_text().strip()

        #### Get Floor Sponsor(s)
        floor_sponsors = bill_soup.findAll(text = re.compile("Floor\\sSponsor"))
        floor_sponsors = [re.sub('\xa0', ' ', i).strip() for i in floor_sponsors]
        H_floor_sponsor = ''
        S_floor_sponsor = ''
        if len(floor_sponsors) != 0:
            if bill_num[0] == "H":
                H_floor_sponsor = "Representative " + re.sub('Floor (Sponsors|Sponsor) -', '', floor_sponsors[0]).strip()
                if len(floor_sponsors) == 2:
                    S_floor_sponsor = "Senator " + re.sub('Floor (Sponsors|Sponsor) -', '', floor_sponsors[1]).strip()
            elif bill_num[0] == "S":
                S_floor_sponsor = "Senator " + re.sub('Floor (Sponsors|Sponsor) -', '', floor_sponsors[0]).strip()
                if len(floor_sponsors) == 2:
                    H_floor_sponsor = "Representative " + re.sub('Floor (Sponsors|Sponsor) -', '', floor_sponsors[1]).strip()

        ### Action History
        hist_table = bill_soup.findAll('table', {'class':'bill-table'})
        if len(hist_table) == 3:
            hist_table = hist_table[2].findAll('tr')

        #### Get All Actions
        order = 1
        for row in hist_table:
            cells = row.findAll('td')
            date = cells[1].get_text().strip()
            if date == '':
                #* Adjusting for table rows with no dates -- appear to be actions on same day as previous
                date = bill_actions[-1][2]
            else:
                date = datetime.datetime.strptime(date + '/' + sy[0:4], '%m/%d/%Y').strftime('%Y-%m-%d')
            action = ' '.join([t.replace('\xa0', ' ') for t in cells[2].stripped_strings])
            bill_actions.append([bill_num, sy, date, action, order])
            order += 1

    ### OUTPUT
    bill_details = [bill_num, sy, short_title, status, author, cosponsors, summary, bill_url, requestor_SOP, requestor_SOP_full, SOP_url, H_floor_sponsor, S_floor_sponsor]
    return([bill_details, bill_actions])


########################################################
############## SCRAPE SESSION(S)
############################################
# bill_info = session_bills[300]
# sy = sessions[0]

for sy in sessions:

    #### Output Lists
    session_bill_details = [['bill_number', 'session', 'title', 'status',  'author', 'cosponsors', 'summary', 'bill_url', "requestor_SOP", "requestor_SOP_full", "SOP_url", "H_floor_sponsor", "S_floor_sponsor"]]
    session_actions = [['bill_number', 'session', 'action_date', 'action','order']]

    print("\n ------------------- Now Scraping: Session " + sy + " ---------------------- \n")

    ### Get all bills for a specific session
    session_bills = get_session_bills(sy)

    if session_bills == '404 error':
        print("\n\n ******** SKIPPING {} SESSION --- 404 ERROR ******** \n\n".format(sy))
        continue

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        bill_data = get_bill_data(sy, bill_info)

        if bill_data == "HTTP Error":
            print(" ******* \n ({}/{}) -- {} -- ERROR -- SKIPPING --- URL: {} \n *******".format(num, total, bill_info[0], bill_info[-1]))
            num += 1
            continue

        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_data[0][0], bill_data[0][7]))
        num += 1

    with open("ID_Bill_Details_" + sy + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("ID_Bill_Histories_" + sy + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- " + sy + " SCRAPED + DATA SAVED  -------------\n\n\n")


print("  ********************************** ALL DONE ********************************** ")
