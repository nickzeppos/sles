# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape Alaska Bills

@author: PB
"""

##### NOTES:
## Cosponsors will include main sponsors from chamber followed by oppo chamber sponsors (seperator = REPRESENTATIVE(S) or SENATOR(S))
#
# ---> They have an API now? http://www.akleg.gov/basis/BasisPublicServiceAPI.pdf
################################################

import csv
import os
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
import re
import time

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/AK/')

#######################
##### Extract Session Links
########################

session_page = requests.get('http://www.akleg.gov/basis/Home/BillsandLaws')
session_soup = BeautifulSoup(session_page.content, "lxml")

session_list = session_soup.find("a", text = re.compile("Past Legislatures \\(Archives\\)"))
session_list = session_list.findNext("ul").findAll("li")

session_urls = [j.findChildren("a")[0]['href'] for j in session_list]
session_list = [j.findChildren("a")[0].get_text() for j in session_list]

session_urls.remove('http://www.akleg.gov/basis/folio.asp')
session_list.remove('Infobases (1982-1992)')

### Skip Previously Scraped
previously_scraped = []
dirFiles = os.listdir('.')  

for i in range(0, len(session_list)):
    sess_adj = re.sub(" \\(| |-", "_", session_list[i])
    sess_adj = re.sub("\\)", "", sess_adj)
    if 'AK_Bill_Details_' + sess_adj + '.csv' in dirFiles:
        previously_scraped.append(i)
        print("Previously Scraped: " + session_list[i])

for index in sorted(previously_scraped, reverse=True):
    del session_urls[index]
    del session_list[index]
    
#######################
###### Function to Scrape Individual Bills
#########################

# bill_url = 'http://www.akleg.gov/basis/Bill/Detail/30?Root=HB2'

def scrape_bill(bill_url, session):
    
    ### GET BILL PAGE HTML
    try:
        req = requests.Session()
        req.mount('http://', HTTPAdapter(max_retries=retries))
        bill_page = req.get(bill_url, timeout = 5)
        bill_soup = BeautifulSoup(bill_page.content, "lxml")
    except:
        time.sleep(5)
        req = requests.Session()
        req.mount('http://', HTTPAdapter(max_retries=retries))
        bill_page = req.get(bill_url, timeout = 5)
        bill_soup = BeautifulSoup(bill_page.content, "lxml")
    
    ### EXTRACT BILL DETAILS
    bill_info = bill_soup.findAll("ul", {"class":"information"})    

    bill_id = bill_info[0].find("span", text = re.compile("^Bill"))
    bill_id = ' '.join(bill_id.findNext().get_text().strip().split())
    
    status = bill_info[0].find("span", text = re.compile("^Current Status"))
    status = ' '.join(status.findNext().get_text().strip().split())
    
    status_date = bill_info[0].find("span", text = re.compile("^Status Date"))
    status_date = ' '.join(status_date.findNext().get_text().strip().split())
    
    version = bill_info[1].find("span", text = re.compile("^Bill Version"))
    version = ' '.join(version.findNext().get_text().strip().split())
    
    short_title = bill_info[1].find("span", text = re.compile("^Short Title"))
    short_title = ' '.join(short_title.findNext().get_text().strip().split()).lower()

    sponsors = bill_info[2].find("span", text = re.compile("^Sponsor"))
    sponsors = ' '.join(sponsors.findNext().get_text().strip().split())
    
    primary_sponsor = sponsors.split(",", maxsplit = 1)[0]
    primary_sponsor = primary_sponsor.replace("REPRESENTATIVES ", "").replace("REPRESENTATIVE ", "")
    primary_sponsor = primary_sponsor.replace("SENATORS ", "").replace("SENATOR ", "")
    
    if len(sponsors.split(",")) > 1:
        cosponsors = sponsors.split(", ", maxsplit = 1)[1]
    else:
        cosponsors = ''
        
    title = bill_info[2].find("span", text = re.compile("^Title"))
    title = ' '.join(title.findNext().get_text().strip().split())
    title = title.replace('"', '').replace('.', '')
    
    keyword_links = bill_soup.findAll('a', href=re.compile("subject="))
    keywords = []
    for link in keyword_links:    
        keywords.append(link.get_text())
    keywords = '--'.join(keywords).lower()   
    
    bill_details = [bill_id, session, version, status, status_date, short_title, primary_sponsor, cosponsors, title, keywords]    
    
    ### EXTRACT TABLE DETAILS
    action_table = bill_soup.find("div", {"class":"actions"}).findAll("tr")
    action_details = []
    order = 1
    for row in action_table[1:]:
        action_location = row['class'][0]
        action_date = row.find("time", {"data-label":" date"})['datetime']
        journal_page = row.find("span", {"data-label":"Page"}).find("a")
        if journal_page != None:
            journal_page = journal_page.get_text()
            journal_link = 'http://www.akleg.gov' + row.find("span", {"data-label":"Page"}).find("a")['href'].replace(' ', '')
        else:
            journal_page = ''
            journal_link = ''
        action = row.find("span", {"data-label":"Text"}).get_text()
        chamber = action.split(")")[0].replace("(", "")
        action_details.append([bill_id, session, chamber, action_location, action_date, action, journal_page, journal_link, order])
        order += 1
        
    ### NEXT BILL URL
    next_bill_url = bill_soup.find("a", text = re.compile("Next Bill"))['href']
    next_bill_url = next_bill_url.replace(" ", "")
    
    return([bill_details, action_details, next_bill_url])
    
# Votes need to be scraped via the journal, linked on the page.....

###########################
####### SCRAPE SESSION(S)
###########################
# LOGIC:
# Start with HB1 and continuously follow next bill button 
# Stop at "We found No Bill in the ZZZZ Legislature" Or Until next Link == http://www.akleg.gov/basis/Bill/Detail/ZZZZ?Root=

### Retries Arg for Request
retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[ 500, 502, 503, 504 ])

### LOOP THROUGH SESSIONS

for s in range(0, len(session_urls)):
    
    ## Session Details
    this_session = session_list[s] 
    this_session_adj = re.sub(" \\(| |-", "_", this_session)
    this_session_adj = re.sub("\\)", "", this_session_adj)
    
    print(" ------------- Starting the " + this_session + " ------------------\n")    
    
    session_num = session_urls[s].replace('http://www.akleg.gov/basis/Home/BillsandLaws/', '')
    HB1_url = 'http://www.akleg.gov/basis/Bill/Detail/' + session_num + '?Root=HB1'
    
    ### Output Headers
    session_bills = [['bill_id', 'session', 'bill_version', 'status', 'status_date', 'short_title', 'primary_sponsor', 'cosponsors', 'title', 'keywords']]
    session_histories = [['bill_id', 'session', 'chamber', 'action_location', 'action_date', 'action', 'journal_page', 'journal_link', 'order']]
    
    ### HB1
    this_bill = scrape_bill(HB1_url, this_session)
    session_bills.append(this_bill[0])
    for hist in this_bill[1]:
        session_histories.append(hist)
    next_bill_url = this_bill[2]
    print("(1) " + this_bill[0][0])
    
    ### LOOP THROUGH BILLS
    moreBills = True
    count = 2
    while moreBills == True:
        this_bill = scrape_bill(next_bill_url, this_session)
        time.sleep(1.5)
        session_bills.append(this_bill[0])
        for hist in this_bill[1]:
            session_histories.append(hist)
        next_bill_url = this_bill[2]
        print("(" + str(count) + ") " + this_bill[0][0])
        if next_bill_url == 'http://www.akleg.gov/basis/Bill/Detail/' + session_num + '?Root=':
            moreBills = False
        # Below is manual edits for missing bills - because using next bill to scroll through, need to manually set next page when errors
        elif next_bill_url == 'http://www.akleg.gov/basis/Bill/Detail/20?Root=HJR68':
            next_bill_url = 'https://www.akleg.gov/basis/Bill/Detail/20?Root=HJR201'
        elif next_bill_url == 'http://www.akleg.gov/basis/Bill/Detail/19?Root=HB295':
            next_bill_url = 'http://www.akleg.gov/basis/Bill/Detail/19?Root=HB296'
        elif next_bill_url == 'http://www.akleg.gov/basis/Bill/Detail/18?Root=HB324':
            next_bill_url = 'http://www.akleg.gov/basis/Bill/Detail/18?Root=HB325'
        elif next_bill_url == 'http://www.akleg.gov/basis/Bill/Detail/18?Root=HB373':
            next_bill_url = 'http://www.akleg.gov/basis/Bill/Detail/18?Root=HB374'
        elif next_bill_url == 'http://www.akleg.gov/basis/Bill/Detail/18?Root=SB83':
            next_bill_url = 'http://www.akleg.gov/basis/Bill/Detail/18?Root=SB84'            
        count += 1
        
    with open("AK_Bill_Details_" + this_session_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bills)
        
    with open("AK_Bill_Histories_" + this_session_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_histories)
        
    print(" ------------- " + this_session + " SCRAPED + DATA SAVED  -------------\n\n\n")
    time.sleep(5)

print("  ********************************** ALL DONE ********************************** ")
