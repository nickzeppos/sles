# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape UTAH Bills

@author: PB
"""
###########################
##### NOTES:
# ***** Adapted from Hugh's Initial Scrapers
######
# ** For 2002+: Can drop anylanguage about substitute from bill title: all actions recorded on same page across substitues
# ** For 1997-2001: Need to Keep only the final substitute; actions stop on ,eg, HB1 when substituted but the HB1 Sub page has all actions (pre and post substitution)
###########################

import csv
import os
from bs4 import BeautifulSoup, NavigableString
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

session_search = requests.get('https://le.utah.gov/bills/bills_By_Session.jsp', timeout=30)

# Parse the HTML content using BeautifulSoup
session_search_soup = BeautifulSoup(session_search.content, 'html.parser')

# Find the table with id="bills-table"
bill_table = session_search_soup.find('table', {'id': 'bills-table'})
session_list = []
a_elements = bill_table.find_all('a')
for a_element in a_elements:
    a_text = a_element.get_text(strip=True)
    year, *session_parts = a_text.split(None, 1)
    session_type = ' '.join(session_parts)
    if year.isdigit():
        session_list.append([int(year), session_type, a_element['href']])
    else:
        continue


### Get Sessions
# session_list = session_search_soup.find('ul', {'class':'bills-alternate'}).findAll('a')
# session_list = [i for i in session_list if not re.search('imaging|digital', i['href'])]
s_dict = {'General Session':'RS', 'First Special Session':'S1', 'Second Special Session':'S2', 'Third Special Session':'S3',
          'Fourth Special Session':'S4', 'Fifth Special Session':'S5', 'Sixth Special Session':'S6',
          'Veto Override Session':'SS_Veto', 'First House Session':'HS1', 'First House Extraordinary Session':'Y1',
          'First Senate Extraordinary Session':'X1', 'House Session': 'HS1'}

sessions = [[i[0], s_dict[re.sub('^[0-9]+ +|^[0-9]+\r\n +', '', i[1])], i[2]] for i in session_list]


### Drop Veto Sessions -- All relevant info in main session actions
sessions = [s for s in sessions if s[1] != 'SS_Veto']

### Drop Previously Scraped
sessions = [s for s in sessions if 'UT_Bill_Details_{}_{}.csv'.format(s[0], s[1]) not in os.listdir('.')]

### Drop Upcoming Session
next_year = datetime.datetime.now().year + 1
sessions = [s for s in sessions if s[0] < next_year and s[0] > 2022]
print("\n\n\t ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(next_year))

del session_search, session_search_soup, session_list, s_dict, next_year


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):

    ### Get HTML
    try:
        # page = urllib.request.urlopen(bill_url, timeout = 20).read()
        page = requests.get(bill_url, timeout=20).content
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
            # page = urllib.request.urlopen(bill_url, timeout = 30).read()
            page = requests.get(bill_url, timeout=30).content
        except urllib.error.HTTPError: # as e
            return('HTTP Error')
        except socket.timeout:
            print("\n ~~> SOCKET TIMEOUT --- Retrying Bill Request")
            time.sleep(120)
            return(get_page_soup(bill_url))
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            # page = urllib.request.urlopen(bill_url, timeout = 60).read()
            page = requests.get(bill_url, timeout=60).content
    time.sleep(.75)

    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)



#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s = sessions[-15]
# lp = list_page_urls[0]

def get_session_bills(s):

    s_yr, s_type, s_url = s

    session_page = requests.get(s_url)
    page_soup = BeautifulSoup(session_page.content, 'lxml')

    #### 1997-2001: Need to Loop Through Pages
    #### 2002+: Expandable tabs load dynamically
    session_bills = []
    if s_yr <= 2001:

        list_page_urls = page_soup.findAll('a', href = re.compile('\\~{}.+ht\\.htm|^[A-Z][A-Z]+[0-9]+ht\\.htm'.format(s_yr)))
        if s_yr == 2000:
            list_page_urls = ['https://le.utah.gov/session/2000/' + i['href'] for i in list_page_urls]
        else:
            list_page_urls = ['https://le.utah.gov' + i['href'] for i in list_page_urls]

        for lp in list_page_urls:
            list_soup = get_page_soup(lp)
            list_bills = list_soup.findAll('dt')
            these_bills = [[i.a.text.strip(), s_yr, s_type, i.strong.text.strip(), i.strong.nextSibling.strip(),  'https://le.utah.gov' + i.a['href']] for i in list_bills]
            session_bills = session_bills + these_bills
    elif s_yr == 2013 and s_type == 'HS1':
        this_t = 'House Rules Resolution Forming Special Investigative Committee'
        session_bills = ['H.R. 9001', s_yr, s_type, this_t, 'Rep. Sanpei, Dean', 'https://le.utah.gov/~2013h1/bills/static/HR9001.html']
        pass
    else:
        list_tabs = page_soup.findAll('div', id = re.compile('^g[0-9]+'))
        list_ids = []
        ### Get the Tab IDs ---- if HB 1-49 = HB00; if HB50-99 = HB50; HB100-149 = HB100, etc
        for tab in list_tabs:
            list_ids = list_ids + [re.sub('^r', '', i['id']) for i in tab.findAll('div')]

        ### Get Bill IDs and Urls
        for lp_id in list_ids:
            lp = '{}&bills={}'.format(s_url, lp_id)
            list_soup = get_page_soup(lp)
            list_bills = list_soup.findAll('dt')
            these_bills = [[i.a.text.strip(), s_yr, s_type, i.b.text.strip(), i.i.text.strip(),  'https://le.utah.gov' + i.a['href']] for i in list_bills]
            session_bills = session_bills + these_bills

    ### Return
    print(' \n **** ~~~> Found {} BILL URLs for {}-{} **** \n'.format(len(session_bills), s_yr, s_type))
    return(session_bills)


##################
#### Function for yielding content between two DOM elements - necessary for getting bill description

def btwn(s, e):
    while s and s != e:
        if isinstance(s, NavigableString):
            t = s.strip()
            if len(t):
                yield t
        s = s.next_element

##########################################
###### Function to Scrape Individual Bills
###### ********** 2002 and later ***************
##########################################
# bill_data = get_session_bills(sessions[5])
# bill_info = session_bills[0]

def get_bill_data(bill_info):

    bill_num, s_yr, s_type, title, sponsor, bill_url = bill_info

    ### Clean Info Vars
    session = '{}-{}'.format(s_yr, s_type)
    if 'substitute' in bill_num.lower():
        bill_num = re.sub('[a-z][a-z]+ substitute| substitute', '', bill_num.lower()).strip()
        bill_num = bill_num.upper()

    #### Get Bill Page
    bill_soup = get_page_soup(bill_url)

    #### Primary sponsor
    try:
        primary_sponsor = bill_soup.find(id="billsponsordiv").find("a").text
    except:
        primary_sponsor = sponsor # Formats will be off but will make it easy to fill in any missing

    #### Floor sponsor
    try:
        floor_sponsor = bill_soup.find(id="floorsponsordiv").find("a").text
    except:
        floor_sponsor = ""

    ### Drafting Attorney
    try:
        drafting_attorney = bill_soup.find('b', string = re.compile('Drafting Attorney')).parent.text.strip()
        drafting_attorney = re.sub("Drafting Attorney:", '', drafting_attorney).strip()
    except:
        drafting_attorney = ""

    ### Last Action + Date
    try:
        last_action = bill_soup.find('b', string = re.compile('Last Action:')).parent.text.strip()
        last_action = re.sub('  +', ' ', re.sub("Last Action:", '', last_action).strip())
    except:
        last_action = ""

    ### Chapter Num
    try:
        chapter_num = bill_soup.find('b', string = re.compile('Session Law Chapter:')).parent.text.strip()
        chapter_num = re.sub("Session Law Chapter:", '', chapter_num).strip()
    except:
        chapter_num = ""

    ### Committee Note (Shows action not reflected in actions?)
    try:
        comm_note = bill_soup.find('b', string = re.compile('Committee Note:')).parent.text.strip()
        comm_note = re.sub("Committee Note:", '', comm_note).strip()
    except:
        comm_note = ""

    ### Keyword Topics
    keywords = bill_soup.findAll('a', href = re.compile('RelatedBill'))
    if keywords == []:
        keywords = ''
    else:
        keywords = '; '.join([i.text.strip() for i in keywords])


    ###########
    #### Get Bill Description
    if bill_num[0] == "H":
        r_url = re.sub('static', 'hbillint', bill_url)
        r_url = re.sub('\\.html$', '.htm', r_url)
    else:
        r_url = re.sub('static', 'sbillint', bill_url)
        r_url = re.sub('\\.html$', '.htm', r_url)

    txt_soup = get_page_soup(r_url)

    ### Need to use different method for 2002-2003
    try:
        if s_yr >= 2004:
            gen_desc = ""

            # Get starting <b> element for "general description"
            for f_b in txt_soup.find_all("b"):
                if "General Description:" in f_b.text:
                    s_b = f_b
                    break

            # Get ending <b> element for "highlighted provisions"
            for f_b in txt_soup.find_all("b"):
                if "Highlighted Provisions:" in f_b.text:
                    e_b = f_b
                    break

            # Get DOM contents between s_b.next_sibiling and e_b, filter with regex
            for line in btwn(s_b.next_sibling, e_b):
                re_syn = re.compile(r"(?:\d+\s+)(.*)")
                re_matches = re.search(re_syn, line)
                if re_matches:
                    gen_desc += " " + re_matches.group(1)

            gen_desc = gen_desc[1:]
        else:
            b_tags = txt_soup.findAll('b')
            # Get the start of the description (always comes after the sponsor summary)
            for i in range(len(b_tags)):
                if 'Sponsor' in b_tags[i].text:
                    b_tags = b_tags[i+1:]
                    break

            ### Get the end
            for i in range(len(b_tags)):
                if re.search(' WP |^[0-9A-Z]+-[0-9A-Z]+[0-9A-Z]+', b_tags[i].text.strip()):
                    b_tags = b_tags[:i]
                    break

            gen_desc = re.sub('  +', ' ', ' '.join([i.text.strip() for i in b_tags]))
    except:
        gen_desc = ""

    ##############
    ### Get Actions

    bill_actions = []
    actions_table = bill_soup.find("div", {"id": "billStatus"})
    if actions_table:
        order = 1
        for row in actions_table.findAll("tr")[1:]:
            if row.text.strip() == '' or row.text.strip() == 'None':
                continue
            cells = row.findAll('td')
            date = cells[0].text.strip()
            if re.search('AM|PM', date):
                date = re.sub(' \\([^\\)]+\\)', '', date)
            if date != '':
                date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            action = cells[1].text.strip()
            chamber = ''
            if cells[1].font['color'] == '#CC0000':
                chamber = 'House'
            elif cells[1].font['color'] == '#3333FF':
                chamber = 'Senate'
            elif cells[1].font['color'] == '#25B223':
                chamber = 'Fiscal'
            location = cells[2].text.strip()
            vote = cells[3].text.strip()
            if vote != '' and cells[3].a:
                vote_url = 'https://le.utah.gov' + cells[3].a['href']
            else:
                vote_url = ''
            bill_actions.append([bill_num, session, chamber, location, date, action, vote, vote_url, order])
            order += 1

    # Data from Bill Text page
    bill_details = [bill_num, session, primary_sponsor, floor_sponsor, drafting_attorney, chapter_num, last_action, title, gen_desc, comm_note, bill_url]
    return([bill_details, bill_actions])


##########################################
###### Functions to Scrape Individual Bills
###### ********** 1997 - 2001 ***************
##########################################
# bill_data = get_session_bills(sessions[5])
# bill_info = session_bills[0]

def get_old_bill_data(bill_info):

    bill_num, s_yr, s_type, title, sponsor, bill_url = bill_info

    ### Clean Info Vars
    session = '{}-{}'.format(s_yr, s_type)

    #### Get Bill Page
    bill_soup = get_page_soup(bill_url)

    #### Primary sponsor
    primary_sponsor = re.sub('\\(|\\)', '', sponsor)

    #### Floor sponsor
    floor_sponsor = ""
    if re.search('floor sponsor', bill_soup.text.lower()):
        print("********************* CHECK FLOOR SPONSOR ***********************")
        exit()

    ### Drafting Attorney
    drafting_attorney = bill_soup.find(string = re.compile('Drafting Attorney:'))
    if drafting_attorney:
        drafting_attorney = [i for i in re.split('\r\n|\xa0\xa0', drafting_attorney) if 'Drafting Attorney' in i.strip()][0]
        drafting_attorney = re.sub("Drafting Attorney:", '', drafting_attorney).strip()
    else:
        drafting_attorney = ""

    ### Last Action + Date
    last_action = bill_soup.find(string = re.compile('Last Action'))
    if last_action:
        last_action = [i for i in re.split('\r\n|\xa0\xa0', last_action) if 'Last Action' in i.strip()][0]
        last_action = re.sub('  +', ' ', re.sub("Last Action:", '', last_action).strip())
    else:
        last_action = ""

    ### Chapter Num
    chapter_num = bill_soup.find(string = re.compile('Session Law Chapter:'))
    if chapter_num:
        chapter_num = [i for i in re.split('\r\n|\xa0\xa0', chapter_num) if 'Session Law Chapter' in i.strip()][0]
        chapter_num = re.sub("Session Law Chapter:", '', chapter_num).strip()
    else:
        chapter_num = ""

    ### Committee Note (Shows action not reflected in actions?)
    comm_note = ""

    ### Keyword Topics
    keywords = bill_soup.findAll('a', href = re.compile('subjbill|RelatedBill|relatedbill'))
    if keywords == []:
        keywords = ''
    else:
        keywords = '; '.join([re.sub('\\[|\\]', '', i.text).strip() for i in keywords])

    #### Get Bill Description
    if bill_num[0] == "H":
        r_url = re.sub('htmdoc/hbillhtm', 'bills/hbillint', bill_url)
        r_url = re.sub('\\.html$', '.htm', r_url)
    else:
        r_url = re.sub('htmdoc/sbillhtm', 'bills/sbillint', bill_url)
        r_url = re.sub('\\.html$', '.htm', r_url)

    txt_soup = get_page_soup(r_url)
    gen_desc = ""

    ### Need to use different method for 1997-2000 and 2001
    #    if s_yr == 1997:
    #        start = txt_soup.find(string = re.compile('Sponsor')).parent
    #        gen_desc = start.findNext('br').next.strip()
    #        gen_desc = re.sub('  +', ' ', re.sub('\r\n', ' ', gen_desc)).strip()
    if s_yr <= 2000:
        start = txt_soup.find(string = re.compile('Sponsor')).findParent('center')
        if start is None:
            start = txt_soup.find(string = re.compile('Sponsor')).findParent('b')
        current = start.findNext('br').next
        count = 0
        while True:
            if count >= 250:
                print(' *********** CHECK EXCESSIVE LOOP: {} *******************'.format(r_url))
                break
            if isinstance(current, NavigableString) and re.search('^AMENDS:|^ENACTS:|This act affects sections of Utah Code|Be it enacted by the Legislature of the State of Utah', current.strip()):
                break
            if isinstance(current, NavigableString):
                if current.strip() != '' and current.strip().upper() == current.strip() and not re.search('^[0-9]+$', current.strip()):
                    gen_desc = gen_desc + ' ' + re.sub('^[0-9]+  +', '', re.sub('\xa0', ' ', current.strip()))
                current = current.next_element
            elif isinstance(current.next_element, NavigableString) and current.next_element.strip().upper() == current.next_element.strip():
                #current.next_element.strip() != '' and
                current = current.next_element
            else:
                break
            count+=1
        gen_desc = re.sub('  +', ' ', re.sub('\r\n', ' ', gen_desc)).strip()
    elif s_yr == 2001:
        b_tags = txt_soup.findAll('b')
        # Get the start of the description (always comes after the sponsor summary)
        for i in range(len(b_tags)):
            if 'Sponsor' in b_tags[i].text:
                b_tags = b_tags[i+1:]
                break

        ### Get the end
        for i in range(len(b_tags)):
            if re.search(' WP |^[0-9A-Z]+-[0-9A-Z]+[0-9A-Z]+', b_tags[i].text.strip()):
                b_tags = b_tags[:i]
                break

        gen_desc = re.sub('  +', ' ', ' '.join([i.text.strip() for i in b_tags]))

    ##############
    ### Get Actions
    if bill_num[0] == "H":
        a_url = re.sub('htmdoc/hbillhtm', 'status/hbillsta', bill_url)
        a_url = re.sub('\\.htm$|\\.html$', '.txt', a_url)
    else:
        a_url = re.sub('htmdoc/sbillhtm', 'status/sbillsta', bill_url)
        a_url = re.sub('\\.htm$|\\.html$', '.txt', a_url)

    ### Get Soup and Drop Non-Actions
    action_soup = get_page_soup(a_url)
    action_text = [i.strip() for i in action_soup.text.split('\r\n') if i.strip() != '']
    for i in range(len(action_text)):
        if re.search('^[0-9]+/[0-9]+/[0-9][0-9]| LRGC', action_text[i]):
            action_text = action_text[i:]
            break

    #### Fix Action Rows for specific bills that create (rare) errors == pattern nonconforming
    if s_yr == 1999 and 'S.B. 102' in bill_num:
        for i in range(len(action_text)):
            if action_text[i] == '02/22/99 Senate/ comm report/ substituted/placed on ConsentSSUB':
                action_text[i] = '02/22/99 Senate/ comm report/ substituted/placed on Consent    SSUB'
    elif s_yr == 1998 and bill_num == 'H.B. 111 Substitute':
        for i in range(len(action_text)):
            if action_text[i] == '03/04/98 House signed by Speaker/enrolled                  lrgcen':
                action_text[i] = '03/04/98 House signed by Speaker/enrolled                  LRGCEN'

    #### Loop Through Actions and Parse Line of text
    bill_actions = []
    if len(action_text) > 0:
        order = 1
        for line in action_text:
            if line.strip() == '' or line.strip() == 'None':
                continue
            line_parts = re.split('  +([A-Z][A-Z0-9][A-Z]+)| ([A-Z][A-Z0-9][A-Z]+)$', line)
            line_parts = [i for i in line_parts if i != None]
            date, action = line_parts[0].split(' ', 1)
            if date != '':
                date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')
            action = action.strip()
            chamber = ''
            if re.search('^House', action):
                chamber = 'House'
            elif re.search('^Senate', action):
                chamber = 'Senate'
            location = line_parts[1]
            vote = line_parts[2].strip()
            vote_url = ''

            bill_actions.append([bill_num, session, chamber, location, date, action, vote, vote_url, order])
            order += 1

    # Data from Bill Text page
    bill_details = [bill_num, session, primary_sponsor, floor_sponsor, drafting_attorney, chapter_num, last_action, title, gen_desc, comm_note, bill_url]
    return([bill_details, bill_actions])



########################################################
############## SCRAPE SESSION(S)
############################################
# s = sessions[-1]
# bill_info = session_bills[143]
# session_bills = [i for i in session_bills if not re.search('^H.B.|^S.B.', i[0])]

for s in sessions:

    #### Output Lists
    session_bill_details = [["bill_id", "session", "primary_sponsor", "floor_sponsor", "drafting_attorney", "chapter_num", "last_action", "title", "description", "comm_note", "bill_url"]]
    session_actions = [['bill_id', 'session', 'chamber', 'location', 'action_date', 'action', 'vote', 'vote_url', 'order']]

    print("\n ------------------- Now Scraping: {}-{} ---------------------- \n".format(s[0], s[1]))

    ### Get all bills for a specific session
    session_bills = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        ### Get Basic Details
        if s[0] <= 2001:
            bill_data = get_old_bill_data(bill_info)
            #print(bill_data)
        else:
            bill_data = get_bill_data(bill_info)

        if bill_data == 'HTTP Error':
            print(" ********** \n ({}/{}) -- HTTP ERROR -- SKIPPING -- {} \n **********".format(num, total, bill_info[5]))
            num += 1
            continue

        ### Get Actions
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_info[0], bill_info[5]))
        num += 1

    with open("UT_Bill_Details_{}_{}.csv".format(s[0], s[1]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("UT_Bill_Histories_{}_{}.csv".format(s[0], s[1]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    print("\n\n\n ------------- {}-{} SCRAPED + DATA SAVED  -------------\n\n\n".format(s[1], s[2]))


print("  ********************************** ALL DONE ********************************** ")
