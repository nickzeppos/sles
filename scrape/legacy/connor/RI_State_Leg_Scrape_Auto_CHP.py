# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape Rhode Island Bills

@author: PB
"""
###########################
##### NOTES:
# *********
###########################

import csv
import os
from bs4 import BeautifulSoup
import time
import re
import requests
import datetime
from collections import OrderedDict
import sys
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#######################
##### Extract Session Links
########################

### Sessions by Year
this_year = datetime.datetime.now().year
sessions = [i for i in range(2023, this_year + 1)]

### Drop Previously Scraped
sessions = [i for i in sessions if 'RI_Bill_Details_{}.csv'.format(i) not in os.listdir('.')]


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):

    ### Get HTML
    try:
        page = requests.get(bill_url, timeout = 30, verify = False)
    except requests.HTTPError: # as e
        return('HTTP Error')
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(30)
            page = requests.get(bill_url, timeout = 45, verify = False)
        except requests.HTTPError: # as e
            return('HTTP Error')
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = requests.get(bill_url, timeout = 60, verify = False)
    time.sleep(.75)

    ### Return Soup
    page_soup = BeautifulSoup(page.content, parser)
    return(page_soup)



#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s = sessions[0]

def get_session_bills(s):

    yr_stem = str(s)[2:]
    H_url = "http://webserver.rilin.state.ri.us/BillText{}/HouseText{}/HouseText{}.html".format(yr_stem, yr_stem, yr_stem)
    S_url = "http://webserver.rilin.state.ri.us/BillText{}/SenateText{}/SenateText{}.html".format(yr_stem, yr_stem, yr_stem)

    ### Get House/Senate Page
    H_soup = get_page_soup(H_url, 'lxml')
    S_soup = get_page_soup(S_url, 'lxml')

    #### Get House/Senate Bills and Text Page URLS
    if s <= 2011:
        H_urls = H_soup.find_all("option", {'value':re.compile('BillText.+\\.htm')})
        H_urls = [[i.text.strip(), s, 'http://webserver.rilin.state.ri.us' + i['value']] for i in H_urls]

        S_urls = S_soup.find_all("option", {'value':re.compile('BillText.+\\.htm')})
        S_urls = [[i.text.strip(), s, 'http://webserver.rilin.state.ri.us' + i['value']] for i in S_urls]
    else:
        H_stem = re.sub('HouseText[0-9][0-9].html', '', H_url)
        H_urls = H_soup.find_all("td", {'class':'bill_col3'})
        H_urls = [[re.sub('.htm', '', i.a['href']), s, H_stem + i.a['href']] for i in H_urls]

        S_stem = re.sub('SenateText[0-9][0-9].html', '', S_url)
        S_urls = S_soup.find_all("td", {'class':'bill_col3'})
        S_urls = [[re.sub('.htm', '', i.a['href']), s, S_stem + i.a['href']] for i in S_urls]

    ### Drop Substitute Records (which are basically just duplicates since not recording bill text)
    H_urls = [i for i in H_urls if not re.search('[A-Za-z]$', i[0])]
    S_urls = [i for i in S_urls if not re.search('[A-Za-z]$', i[0])]

    ### Drop Articles --- These are smaller sections of a Larger piece of legislation
    H_urls = [i for i in H_urls if not re.search('Article', i[0])]
    S_urls = [i for i in S_urls if not re.search('Article', i[0])]

    ### Combine and return
    session_urls = H_urls + S_urls
    print(' \n **** ~~~> Found {} BILL URLs for the {} SESSION **** \n'.format(len(session_urls), s))
    return(session_urls)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_data = get_session_bills(sessions[5])
# bill_info = session_bills[841]

def get_bill_data(bill_info):

    bill_num, s_yr, bill_txt_url = bill_info

    ### Skip Substitutes and Repeats of Engrossed/Amended Bills
    if "SUB" in bill_num or "Sub" in bill_num or re.search('[A-Za-z]$', bill_num):
        return("SKIP")

    ##############
    ### BILL DETAILS
    #############

    # Initialize empty bill vars
    title = ''
    sponsors_full = ''
    intro_date = ''
    ref_comm = ''
    bill_type = ''

    ## Bills missing HTML pages (using PDFs to fill in manually)
    if bill_num == 'S1017' and s_yr == 2007:
        title = "RELATING TO TAXATION - LEVY AND ASSESSMENT OF LOCAL TAXES - COLLECTION OF TAXES GENERALLY"
        sponsors_full = "Senator Maryellen Goodwin"
        intro_date = "2007-05-10"
        ref_comm = "Senate Judiciary"
        bill_type = 'AN ACT'
    elif bill_num == 'H8089' and s_yr == 2008:
        title = "RELATING TO BUSINESSES AND PROFESSIONS - OPTOMETRISTS"
        sponsors_full = "Representatives McNamara, Malik"
        intro_date = "2008-03-27"
        ref_comm = "House Health, Education & Welfare"
        bill_type = 'AN ACT'
    elif bill_num == 'H6250' and s_yr == 2011:
        title = "RELATING TO SPORTS, RACING, AND ATHLETICS"
        sponsors_full = "Representatives Jackson, Lally, Silva, DaSilva, Keable"
        intro_date = "2011-06-14"
        ref_comm = "House Finance"
        bill_type = 'AN ACT'
    elif bill_num == 'H6423' and s_yr == 2023:
        title = "RESPECTFULLY REQUESTING THE DEPARTMENT OF ADMINISTRATION AND THE OFFICE OF INFORMATION TECHNOLOGY TO REVIEW AND EVALUATE THE USE AND DEVELOPMENT OF ARTIFICIAL INTELLIGENCE (AI) AND AUTOMATED DECISION SYSTEMS AND PROVIDE RECOMMENDATIONS REGARDING ONGOING AND UPCOMING PLANS TO EXPAND THEIR USE AND CURRENT SECURITY AND IMPLEMENTATION PROCEDURES"
        sponsors_full = "Representatives Stewart, Baginski, Carson, Morales, Boylan, Alzate, McEntee, Spears, Handy, and Shanley"
        intro_date = "May 19, 2023"
        ref_comm = "House Innovation, Internet, & Technology"
        bill_type = 'HOUSE RESOLUTION'
    else:

        ### ***** Getting data for all other bills ****
        bill_soup = get_page_soup(bill_txt_url, parser = 'lxml')

        # Bill title
        pot = bill_soup.findAll("p", {"class": "TITLE"})
        n_pot = bill_soup.findAll("p", {"class": "RI_TITLE"})
        if pot:
            #title = ' '.join([i.text.strip()for i in pot])
            title = [i.text.strip()for i in pot]
        elif n_pot:
            #title = ' '.join([i.text.strip()for i in n_pot])
            title = [i.text.strip()for i in n_pot]
        elif bill_num == 'S0989' and s_yr == 2017:
            title = ['AUTHORIZING THE STATE TO ENTER INTO A FINANCING LEASE AND PAYMENT AGREEMENTS IN CONNECTION THE CONSTRUCTION OF A BALLPARK IN THE CITY OF PAWTUCKET']
        else:
            return('STOP')

        title = list(OrderedDict.fromkeys(title)) # Drop Duplicates while retaining order (sometimes title listed twice)
        title = ' '.join(title)
        title = re.sub('\xa0', ' ', title)
        title = re.sub('  +', ' ', title)

        # Bill introducer
        pot = bill_soup.find_all("p", {"class": "HEADFONTLEFT_TOP"})
        if pot:
            sponsors_full = [p.text.strip() for p in pot if p.find("b") and "Introduced By:" in p.find("b").text][0]
            sponsors_full = re.sub('Introduced By: |&nbsp', '', sponsors_full)
            sponsors_full = re.sub('  +| and ', ' ', sponsors_full).strip()

        # Bill date introduced/referred to
        b_date = bill_soup.find('b', string = re.compile('Date Introduced'))
        if b_date:
            intro_date = re.sub('.+Introduced: ', '', b_date.findParent('p').text).strip()
            intro_date = datetime.datetime.strptime(intro_date, '%B %d, %Y').strftime('%Y-%m-%d')

        # Bill referred to
        b_ref = bill_soup.find('b', string = re.compile('Referred To:'))
        if b_ref:
            ref_comm = re.sub('.+To: ', '', b_ref.findParent('p').text).strip()

        # Bill Type
        bt = bill_soup.find('p', {'class':'TITLE_TYPE'})
        if bt:
            bill_type = re.sub(' ', '', bt.text.strip())
            bill_type = re.sub('\xa0', ' ', bill_type)
            bill_type = re.sub('  +', ' ', bill_type)

    #####################
    #### ACTIONS PAGE --- Could do all at same time but too difficult to parse actions that way
    #####################

    num_only = int(re.sub("\D", "", bill_num[1:]))
    action_url = 'http://status.rilin.state.ri.us/bill_history_report.aspx?year={}&bills={}'.format(s_yr, num_only)

    ### Get Data
    bill_actions = []
    action_soup = get_page_soup(action_url, "lxml")
    actions_list = action_soup.find_all("div", {"style": "margin-left: 5%"})

    ### Sponsors -- Last Names ONLY
    b_by = action_soup.find('b', text = "BY")
    if b_by:
        sponsors_short = re.sub('BY|\xa0', '', b_by.findParent('div').text)
    else:
        sponsors_short = ''

    order = 1
    for act in actions_list:
        action_date = act.text[:11][:-1]
        action_date = datetime.datetime.strptime(action_date, '%m/%d/%Y').strftime('%Y-%m-%d')
        action = act.text[11:]

        # Compile into list entry
        bill_actions.append([bill_num, s_yr, action_date, action, order])
        order += 1


    # Data from Bill Text page
    bill_details = [bill_num, s_yr, bill_type, intro_date, ref_comm, sponsors_short, sponsors_full, title, bill_txt_url, action_url]
    return([bill_details, bill_actions])



########################################################
############## SCRAPE SESSION(S)
############################################
# s = sessions[0]
# bill_info = session_bills[2375]

for s in sessions:

    #### Output Lists
    session_bill_details = [['bill_id', 'session', 'bill_type', 'intro_date', 'ref_comm', 'sponsors', 'sponsors_full',  'title', 'bill_url', 'history_url']]
    session_actions = [['bill_id', 'session', 'action_date', 'action', 'order']]

    print("\n ------------------- Now Scraping the {} Session ---------------------- \n".format(s))

    ### Get all bills for a specific session
    session_bills = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        ### Get Basic Details
        bill_data = get_bill_data(bill_info)

        if bill_data == "STOP":
            print(" ********** \n ({}/{}) -- {} -- TITLE ERROR --- CHECK ME \n **********".format(num, total, bill_info[2]))
            sys.exit("TITLE ERROR")
        elif bill_data == 'SKIP':
            print(" ********** \n ({}/{}) -- DUPLICATE OR SUBSTITUTE RECORD -- SKIPPING -- {} \n **********".format(num, total, bill_info[2]))
            num += 1
            continue

        ### Get Actions
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_info[0], bill_info[2]))
        num += 1


    with open("RI_Bill_Details_{}.csv".format(s), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("RI_Bill_Histories_{}.csv".format(s), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {} SESSION SCRAPED + DATA SAVED  -------------\n\n\n".format(s))


print("  ********************************** ALL DONE ********************************** ")
