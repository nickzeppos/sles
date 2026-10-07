#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 29 09:29:18 2020

 ********** Scrape 199X-2004 Bills from CONNECTICUT *********

@author: pb
"""

#######################################
#### NOTE:
#######################################
### For 1991 - 1998 CANNOT pull 'proposed bill' text to get introducer of committee bills.
### ---> Page only yields the committee bill text, which lists the committee
### ---> In cases where a committee bill doesn't get written, retains the introducers name
### ---> But this may be problematic as we'll be more likely to identify Introducer when bill dies...
####################################################


import csv
import os
import re
import time
#from ftplib import FTP
import urllib
from bs4 import BeautifulSoup, NavigableString
import datetime
import requests
import socket
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/CT/')


##############################################################
###### Session Info
################################################################

## Range of "Archive" Bills to Scrape
session_years = [i for i in range(1991, 2005, 1)]

## DROPPING PREVIOUSY SCRAPED TERMS
session_years = [sy for sy in session_years if 'CT_Bill_Details_{}.csv'.format(sy) not in os.listdir('.')]


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):
    
    ### Get HTML
    try:
        page = requests.get(bill_url, timeout = 30, verify = False)
        # **************** NEED TO SET VERIFY = FALSE to scrape CT Page... Not ideal, so use sparingly
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
            page = requests.get(bill_url, timeout = 60, verify = False)
        except urllib.error.HTTPError: # as e
            return('HTTP Error')
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = requests.get(bill_url, timeout = 120, verify = False)
    time.sleep(1)
    
    ### Return Soup
    page_soup = BeautifulSoup(page.content, parser)
    return(page_soup)    
  



##############################################################
###### Get List of Bills for a Specific Session
################################################################
# sy = sessions[0]  
# row = bill_rows[2153:2155]  

def get_session_bills(sy):
    
    print("\n***** Searching for bills for {} CT Legislative Session *****\n".format(sy))    
    
    ### Requesting up to 15,000 Bill Records... Averages seem to be in the 5000s or less so this should be more than enough buffer
    search_url = "https://search.cga.state.ct.us/r/adv/dtsearch.asp?posted=posted&name=&number=&numberconn=and&titleopt=phrase&title=&titleconn=and&requestopt=phrase&request=&year1={}&selectItemyear1={}&numres=15000&sort=name&sortorder=ascend&posted=posted&submit1="
    search_url = search_url.format(sy, sy)
    
    ### Get Search Results
    search_soup = get_page_soup(search_url)
    bill_rows = [i for i in search_soup.find('table').findAll('tr')[1:]]
    base_url = 'https://www.cga.ct.gov/asp/cgabillstatus/cgabillstatus.asp?selBillType=Bill&which_year={}&bill_num={}'
        
    #### Loop through bills to clean/parse
    session_bills = []
    all_bill_nums = []
    for row in bill_rows:
        cells = row.findAll('td')
        yr = int(cells[2].text.strip())
        bn = cells[4].text.strip()
        tob_url_stem = cells[5].a.text
        bt_stem = tob_url_stem[4:6]
        title = cells[6].text.strip()
        
        ### Adjust for Bill Numbers that are Missing
        if bn == '':
            bn = tob_url_stem[8:12]
            bn = str(int(bn))
            
        ### Skip Final 12 Bills after SR202 in 1995 --- These are Duplicates with errors in the bill numbers
        if sy == 1995 and int(cells[1].text) >= 3501 and bn == '202':
            continue

        ### Add HB/SB to BIll Nums (Already included for Resolutions)
        if not re.search('[A-Z]', bn.upper()):
            bill_num = tob_url_stem[4:6] + bn
        else:
            bill_num = bn.upper()
            
        ### Skip if Repeat Record of ALready Scraped BIll (e.g., Substitute, Amended, Etc.)
        if bill_num in all_bill_nums:
            continue
        else:
            all_bill_nums.append(bill_num)
            
        ### Create Bill URLs -- HB/SB are just numbers; Resolutions include bill type
        if bt_stem in ['HB', 'SB']:
            bill_url = base_url.format(yr, bn)
        else:
            bill_url = base_url.format(yr, bill_num)
            
        #### Create URL to Page with INtroducer Info (tob == pb_url in other script)
        tob_url_stem = re.sub('‑', '-', tob_url_stem)
        if yr < 2000:
            tob_url = 'https://www.cga.ct.gov/ps{}/tob/{}/{}'.format(str(yr)[2:], bt_stem[0].lower(), tob_url_stem)
        else:
            tob_url = 'https://www.cga.ct.gov/{}/tob/{}/{}'.format(str(yr), bt_stem[0].lower(), tob_url_stem)
        
        #### Append to List
        session_bills.append([bill_num, yr, title, bill_url, tob_url])
        
    #### Return Data 
    print("\n~~~~> Found {} Bills Total \n".format(len(session_bills)))
    return(session_bills)
    


######################################################
#### FUNCTION TO CLEAN BILL STATUS/DETAIL PAGE
#####################################################
# bill_row = session_bills[24]

def get_bill_data(sy, bill_row):
    
    bill_num, yr, title, bill_url, tob_url = bill_row
    
    #### Get Main Page
    this_soup = get_page_soup(bill_url)
    
    ### Use different parser if bill history not listed
    if this_soup.find('table', {'summary':'Bill history'}) is None:
        this_soup = get_page_soup(bill_url, parser = 'html5lib')

    ### Get Bill Details
    sop = this_soup.find('h4', text = title)
    if sop is None:
        sop = this_soup.find('h4')
    sop = sop.findNextSibling('p').text.strip()
    
    #### Get Primary Sponsors -- Need to skip/loop past page breaks
    primary_spon_tag = this_soup.find('h5', text = re.compile("Introduced"))
    primary_sponsors = []
    while True:
        primary_spon_tag = primary_spon_tag.nextSibling
        if primary_spon_tag is None:
            break
        elif isinstance(primary_spon_tag, NavigableString):
            if primary_spon_tag.strip() != '':
                primary_sponsors.append(primary_spon_tag.strip())
    primary_sponsors = '; '.join(primary_sponsors)
            
    #### Get Cosponsors
    #cospon_tag = this_soup.find('h4', text = re.compile("Co-sponsors"))
    cosponsors = [i.text.strip() for i in this_soup.findAll('div', {'class':'large-6 medium-6 columns'})]
    cosponsors = [i for i in cosponsors if re.search('^representative|^rep\\.|^senator|^sen\\.| [0-9]+[a-z]+ dist', i.lower())]
    cosponsors = '; '.join(cosponsors)
    
    ### Get Bill History/Actions
    hist_table = this_soup.find('table', {'summary':'Bill history'})
    hist_rows = hist_table.tbody.findAll('tr')
    bill_hist = []
    order = len(hist_rows)
    for row in hist_rows:
        cells = row.findAll('td')
        date = datetime.datetime.strptime(cells[1].text.strip(), '%m/%d/%Y').strftime('%Y-%m-%d')    
        action = cells[3].text.strip()
        bill_hist.append([bill_num, yr, date, action, order, bill_url])
        order = order - 1
    
    ######## Get Original Introducers IF Committee Bill
    # *************** Can't do this for 1991 - 1998: Only yields the committee bill text, not the proposed bill *****************
    if sy >= 1999:
        tob_soup = get_page_soup(tob_url)
    
        #### Code Bill Type
        if tob_soup.find(text = re.compile('Proposed (Bill|House|Senate)')):
            bill_type = 'Proposed Bill'
        elif tob_soup.find(text = re.compile('Raised (Bill|House|Senate)')):
            bill_type = "Raised Bill"
        else:
            bill_type = ""
    
        ### Get Introducers of Proposed Bill    --- Adapting for Multiple Formats...         
        intro_by = tob_soup.find(text = re.compile('Introduced by:|Introduced by')).parent
        if not re.search('Introduced by:$|Introduced by$', intro_by.text.strip()):
            introduced_by = re.sub('Introduced by:|Introduced by', '', intro_by.text.strip()).strip()
        else:
            intro_by = intro_by.findParent('td')
            introduced_by = []
            start_loc = 1
            ### Adjust for Bills with Seperate Table of Names
            if intro_by is None or intro_by.findParent('tr') is None:
                intro_by = tob_soup.find(text = re.compile('Introduced by:')).findParent('p')
                intro_by = intro_by.findNextSibling('table')
                start_loc = 0
                if not re.search('rep\\.|sen\\.', intro_by.text.lower()):
                    intro_by = []
            elif intro_by.findNextSibling('td') is None:
                intro_by = intro_by.findParent('tr').findNext('tr')
                start_loc = 0
            elif intro_by.findNextSibling('td').text.strip() == '':
                intro_by = intro_by.findParent('tr').findNext('tr')
                start_loc = 0
            else:
                intro_by = intro_by.findParent('tr')
                
            #### Loop Trough Rows
            for td in intro_by.findAll('td')[start_loc:]:
                introduced_by = introduced_by + [i.font.text.strip() for i in td.findAll('p')]
            introduced_by = '; '.join(introduced_by)     
    else:
        bill_type = ''
        introduced_by = ''
    
    bill_details = [bill_num, yr, bill_type, primary_sponsors, title, sop, cosponsors, introduced_by, bill_url, tob_url]
    return([bill_details, bill_hist])
    

#####################################
### Loop Through Sessions, Get Bill Details
######################################
# sy = session_years[0]
# bill_row = session_bills[1527]
   
for sy in session_years:
        
    ### Get List of Bills and Basic Details
    session_bills = get_session_bills(sy)
    
    ### Output Lists
    bill_details_data = [['bill_num', 'session', 'bill_type', 'primary_sponsors', 'title', 'purpose', 'cosponsors', 'introduced_by', 'bill_url', 'proposed_bill_pdf_url']]
    bill_hist_data = [['bill_num', 'session', 'action_date', 'action', 'order', 'bill_url']]    
 
    ### Scrape Each Bill
    num = 1
    total = len(session_bills)
    for bill_row in session_bills:
        
        ### Get Bill Data
        this_bill = get_bill_data(sy, bill_row)
        
        if this_bill[0] == 'No Data':
            print("\n\n -- ({}/{}) ** NO DATA FOR {} ** \n URL: {}".format(num, total, bill_row[0], bill_row[-1]))
            num +=1
            continue
        
        ### Append to Agg Files
        bill_details_data.append(this_bill[0])

        for action_row in this_bill[1]:
            bill_hist_data.append(action_row)

        print(" -- ({}/{}) -- {} -- URL: {}".format(num, total, bill_row[0], bill_row[-1]))
        num += 1
    
    ### SAVE!
    with open("CT_Bill_Details_" + str(sy) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(bill_details_data)
        
    with open("CT_Bill_Histories_" + str(sy) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(bill_hist_data)  
    
    print("\n\n *********** {} SESSION - DONE! ************** \n\n".format(sy))


print(" ~~~~~~~~~~~~~ ************** ALL SESSIONS DONES *************** ~~~~~~~~~~~~~~~~ ")
