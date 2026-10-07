# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape Tennessee Bills

@author: PB
"""

##### NOTES:
# NEED TO GENERALIZE THIS TO UPDATE AFTER INITIAL SCRAPE

import csv
import os
from urllib.request import urlopen
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
import re
# import numpy as np
import time
from unicodedata import normalize

from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options  

from retry import retry
from timeout_decorator import timeout, TimeoutError

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/TN/')

########################
###### Extract Session Links
#########################
# *** Note: this does not include current session

#archive_page = urlopen('http://www.capitol.tn.gov/legislation/archives.html').read()
#archive_soup = BeautifulSoup(archive_page, "lxml")
#
#sessions = archive_soup.find("h2", text = re.compile("Bills and Resolutions")).parent
#sessions = sessions.findAll("li", text = re.compile("General Assembly"))
#
#session_data = [] #['session', 'session_url']
#
#for i in range(0, len(sessions)):
#    this_session = sessions[i].findNext("li")
#    this_url = this_session.find("a", text = re.compile("Bills and Resolutions"))['href']
#    this_label = sessions[i].get_text()
#    session_data.append([this_label, this_url])
#    specials = this_session.findAll("a", text = re.compile("Special Session"))
#    if specials != []:
#        for j in range(0, len(specials)):
#            this_url = specials[j]['href']
#            ### Adjustments for Special Session URLS
#            if this_url[0:1] == "/":
#                this_url = 'http://www.capitol.tn.gov' + this_url
#            elif this_url[0:4] != "http":
#                this_url = 'http://www.capitol.tn.gov/legislation/' + this_url
#            this_label = specials[j].get_text()
#            session_data.append([this_label, this_url])
#    print(sessions[i].get_text())
#    
#
######################
####### Get Bill URLS
#########################
#
#### Output
#bill_urls = [['bill_id', 'session', 'bill_url', 'special_format']]
#
##### This will retry a webpage (with a increasing delay per attempt) if there is a connection error
#req = requests.Session()
#retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[ 500, 502, 503, 504 ])
#req.mount('http://', HTTPAdapter(max_retries=retries))
#
#### LOOP
#for i in range(0, len(session_data)):
#    session_label = session_data[i][0]
#    session_url = session_data[i][1]
#    # session_page = urlopen(session_url).read()
#    # this_soup = BeautifulSoup(session_page, "lxml")
#    session_page = req.get(session_url)
#    this_soup = BeautifulSoup(session_page.content, "lxml")
#    
#    ### If/Else Because: Some pages are Lists of Bills, others are Groups of Bills
#    these_bills = this_soup.findAll("a", href = re.compile("Billinfo|BillInfo"))
#    if these_bills != []:
#        for b in these_bills:
#            bill_urls.append([b.get_text(), session_label, b['href'], "No"])
#        time.sleep(5)
#    elif session_label in ['104th Special Session', '1st Special Session', '2nd Special Session', '99th Special Session']:        
#        bill_groups = this_soup.findAll("a", href = re.compile("HJR|SJR|HR|SR|HB|SB"))
#        for bills in bill_groups:
#            bills_page = req.get(re.sub("SpecSessIndex.htm|SpecSessIndex2.htm", bills['href'], session_url))
#            bills_soup = BeautifulSoup(bills_page.content, "lxml")
#            these_bills = bills_soup.findAll("a", href = re.compile("BillStatus|Billstatus"))
#            for b in these_bills:
#                bill_urls.append([b.get_text(), session_label, re.sub("SpecSessIndex.htm", b['href'], session_url), "Yes"])
#            time.sleep(1)
#    else:
#        bill_groups = this_soup.findAll("a", href = re.compile("Billindex|BillIndex"))
#        for bills in bill_groups:
#            # bills_page = urlopen('http://wapp.capitol.tn.gov/apps/archives/' + bills['href']).read()
#            # bills_soup = BeautifulSoup(bills_page, "lxml")
#            bills_page = req.get('http://wapp.capitol.tn.gov/apps/archives/' + bills['href'])
#            bills_soup = BeautifulSoup(bills_page.content, "lxml")
#            these_bills = bills_soup.findAll("a", href = re.compile("Billinfo|BillInfo"))
#            for b in these_bills:
#                bill_urls.append([b.get_text(), session_label, b['href'], "No"])
#            time.sleep(1)
#    print(session_label)
#
#### Adjust 101st Session Label
#for i in range(0, len(bill_urls)):
#    if bill_urls[i][1] == '1st Special Session':
#        bill_urls[i][1] = '101st 1st Special Session'
#        print("Fixed 1st")
#    if bill_urls[i][1] == '2nd Special Session':
#        bill_urls[i][1] = '101st 2nd Special Session'    
#        print("Fixed 2nd")
#

##################################################################
#######################################
##### Save Large List
###################################################################

#with open("TN_Bill_Details.csv", "w", newline = "") as f:
#    writer = csv.writer(f)
#    writer.writerows(bill_urls)


##########
#### Read Data on Bill Page URLs
###########

with open("TN_Bill_Details.csv") as f:
    reader = csv.reader(f)
    bill_urls = [r for r in reader]


#################
### Function to Get and Retry Driver Get Calls
##################
#https://stackoverflow.com/questions/43003924/python-selenium-refresh-if-wait-more-than-10s

@retry(TimeoutError, tries=3)
@timeout(10)
def get_with_retry(driver, url):
    driver.get(url)

########################
##### SCRAPE BILL DETAILS
########################
### ** Run This to Get Individual Bill Links
### ** In future should adapt to just get most recent/incomplete terms


#### This will retry a webpage (with a increasing delay per attempt) if there is a connection error
#req = requests.Session()
#retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[ 500, 502, 503, 504 ])
#req.mount('http://', HTTPAdapter(max_retries=retries))


#######
### FUNCTION TO SCRAPE BILL DATA
######

# this_soup = page_soup
# bill_details = bill_urls[i]

def scrape_bill(this_soup, bill_details):
    
    #### Bill Numbers
    companion_bill_num = this_soup.find("span", {"id" : "lblCompNumber"}).get_text().strip()
    companion_bill_num = re.sub('\(|\)| ', '', companion_bill_num)
    
    ## Prime + Co-Prime + Companion Sponsor
    prime_sponsor = this_soup.find("span", {"id" : "lblBillPrimeSponsor"}).get_text().strip()
    prime_sponsor = re.sub("by \*", "", prime_sponsor)
    coprime_sponsors = this_soup.findAll("span", {"id" : "lblBillCoPrimeSponsor"})
    if coprime_sponsors != []:
        coprime_sponsors = re.sub("^, ", "", coprime_sponsors[0].get_text().strip())
    else:
        coprime_sponsors = ''
    companion_sponsor = this_soup.find("span", {"id" : "lblCompPrimeSponsor"}).get_text().strip()
    companion_sponsor = re.sub("by \*", "", companion_sponsor)
    
    ### Abstract
    short_title = this_soup.find("span", {"id" : "lblAbstract"}).get_text().strip()
    
    ### FIscal + Regular Summaries
    fiscal_summary = this_soup.find("span", {"id" : "lblFiscal"}).get_text().strip()
    fiscal_summary = ' '.join(re.sub("\n+", "-----", fiscal_summary).split())
    
    summary = this_soup.find("span", {"id" : "lblSummary"}).get_text().strip()
    summary = ' '.join(re.sub("\n+", "-----", summary).split()) 
    
    ### Vote Data -- UNPARSED -- Can be split by bill number
    house_vote_data = this_soup.find("span", {"id" : "lblHouseVoteData"}).get_text().strip()
    house_vote_data = ' '.join(re.sub("\xa0", " ", house_vote_data).split())
    senate_vote_data = this_soup.find("span", {"id" : "lblSenateVoteData"}).get_text().strip()
    senate_vote_data = ' '.join(re.sub("\xa0", " ", senate_vote_data).split())
    
    ### Action Table --- NOTE: SKIPPING Companion Action Table --- ID: gvBillCoActionHistory   
    # **** Data not needed... Will be scraped later and all info is in main column
    # **** ALSO: Cosponsors only show up for main bill so gathering via companion would lose them
    #if bill_details[0][0:1] == 'H':
    #    chamber = 'house'
    #else:
    #     chamber = 'senate'
    table = this_soup.find("table", {"id":"gvBillActionHistory"})
    table = table.findAll("tr")
    order = len(table) - 1
    action_history = []
    for tr in table[1:len(table)]:
        action = tr.findAll("td")[0].get_text().strip()
        date = tr.findAll("td")[1].get_text().strip()
        action_history.append(bill_details + [date, action, order])
        order -= 1
        
    bill_details_full = bill_details + [companion_bill_num, short_title, prime_sponsor, coprime_sponsors, companion_sponsor, fiscal_summary, summary]
    vote_details = bill_details + [house_vote_data, senate_vote_data]
    
    return([bill_details_full, action_history, vote_details])
        
######################################################
        
        
#######
### FUNCTION TO SCRAPE BILL DATA from OLDER SPECIAL SESSIONS
######

#this_soup = page_soup
#bill_details = bill_urls[i]
def scrape_special_bill(this_soup, bill_details):
    
    #### Bill Numbers
    companion_bill_num = ''
    
    ## Prime + Co-Prime + Companion Sponsor
    prime_sponsor = this_soup.find("a", {"target" : "new"}).parent.get_text()
    prime_sponsor = re.sub("\(HB.+|\(SB.+|\(\*HB.+|\(\*SB.+", "", prime_sponsor).strip() 
    prime_sponsor = re.sub(".+ by \*|\.", "", prime_sponsor).strip()     
    
    coprime_sponsors = re.sub("^[a-zA-Z]+, \*|\*", "", prime_sponsor).strip()     
    prime_sponsor = re.sub(",.+$", "", prime_sponsor).strip()   
    
    companion_sponsor = ''
    
    ### Abstract
    short_title = this_soup.find("a", {"target" : "new"}).parent.parent.findNext("p").get_text().strip()
    
    ### Action Table ---  Note: this will yield extra blank rows when opposing chamber has more actions
    table = this_soup.findAll("table")
    last_table = table[len(table)-1].findAll("tr")
    order = len(last_table) - 1
    action_history = []
    for tr in last_table:
        # Note this works even when senate SB action is recorded as those are TR items 2 and 3
        if  tr.findAll("td") == []:
            continue
        else:
            action = tr.findAll("td")[0].get_text().strip()
            date = tr.findAll("td")[1].get_text().strip()
            action_history.append(bill_details + [date, action, order])
            order -= 1
          
    ### FIscal + Regular Summaries
    time.sleep(1)
    req = requests.get(re.sub("BillStatus", "BillSummary", bill_details[2]))
    sum_soup = BeautifulSoup(req.content, "lxml")
    if sum_soup.get_text() == 'The page cannot be displayed because an internal server error has occurred.':
        fiscal_summary = ''
        summary = ''
    else:
        fiscal_summary = sum_soup.find("u", text = re.compile("Fiscal Summary")).parent.get_text().strip()
        fiscal_summary = ' '.join(re.sub("\n+", "-----", fiscal_summary).split())
        
        summary = sum_soup.get_text()
        if "Bill Summary for *" + bill_details[0] in summary:
            summary = summary.split("Bill Summary for *" + bill_details[0])[1].strip()
        else:
            summary = summary.split("Bill Summary for " + bill_details[0])[1].strip()
        summary = ' '.join(summary.split())
    
    vote_details = []
    bill_details_full = bill_details + [companion_bill_num, short_title, prime_sponsor, coprime_sponsors, companion_sponsor, fiscal_summary, summary]
    return([bill_details_full, action_history, vote_details])
        
######################################################


##################
#### OUPTUT LISTS
###################

start_loop = int(input("Where do you want to start the loop? If at beginning, enter 1: "))

if start_loop == 1:
    all_histories = [['bill_id', 'session', 'bill_url', 'special_format', 'date', 'action', 'order']]
    all_data = [['bill_id', 'session', 'bill_url', 'special_format', 'companion_bill_id', 'short_title', 'sponsor', 'cosponsors', 'companion_sponsor', 'fiscal_summary', 'summary']]
    all_votes = [['bill_id', 'session', 'bill_url', 'special_format', 'house_votes', 'senate_votes']]
else:
    all_histories = []
    all_data = []
    all_votes = []
    
skipped = []


#### Script below will append so need to clear out old files
# *********************


########################
#### Start Selenium + SCRAPE
#######################

### ** Deprecatd
#dcap = dict(DesiredCapabilities.PHANTOMJS)
#dcap["phantomjs.page.settings.userAgent-"] = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36")
#driver = webdriver.PhantomJS(desired_capabilities = dcap)

chrome_options = Options()  
chrome_options.add_argument("--headless")  
driver = webdriver.Chrome(chrome_options=chrome_options)  
# driver = webdriver.Firefox()  

############
#### LOOP
###########

for i in range(start_loop, len(bill_urls)):
    
    ### Issue with URLs for 101st 2nd Special Session
    if(bill_urls[i][2] == 'http://www.capitol.tn.gov/legislation/Archives/101GA/bills/SpecSessIndex2.htm'):
        bill_urls[i][2] = 'http://www.capitol.tn.gov/legislation/Archives/101GA/bills/BillStatus/' + bill_urls[i][0] + '.htm'
    
    ### Navigate to Web Page    
    get_with_retry(driver, bill_urls[i][2])
    
    ### Check Page Load
    check_soup = BeautifulSoup(driver.page_source, "lxml")
    if check_soup.get_text() == 'The page cannot be displayed because an internal server error has occurred.':
        get_with_retry(driver, bill_urls[i][2])
        check_soup2 = BeautifulSoup(driver.page_source, "lxml")
        if check_soup2.get_text() == 'The page cannot be displayed because an internal server error has occurred.':
            skipped.append(i)
            print("ERROR -- SKIPPED BILL URL NUMBER " + str(i))
            continue
    if check_soup.findAll('span', id = 'lblErrorPanelMsg') != []:
        skipped.append(i)
        print("ERROR -- BILL NOT FOUND -- " + str(i))
        continue      
    
    ### Show Cosponsors + Wait for it to show up
    if bill_urls[i][3] == "Yes": 
        pass
    else:
        cospon_button = driver.find_elements_by_id('lnkShowCoPrimes') 
        if cospon_button != []:
            cospon_button[0].click()

            timeout = 5
            try:
                element_present = EC.presence_of_element_located((By.XPATH, "//a[@class='coprimelink-hide']"))
                WebDriverWait(driver, timeout).until(element_present)
            except TimeoutException:
                skipped.append(i)
                print("Timed out waiting for page to load")

    #### Extract Bill Data + Adjust for Special Format or Not
    page_soup = BeautifulSoup(driver.page_source, "lxml")
    
    if bill_urls[i][3] == "Yes":
        bill_data = scrape_special_bill(page_soup, bill_urls[i])
    else:
        bill_data = scrape_bill(page_soup, bill_urls[i])
        
    #### APPEND
    all_data.append(bill_data[0])
    for hist in bill_data[1]:
        all_histories.append(hist)
    if bill_data[2] != []:
        all_votes.append(bill_data[2])
    
    ## Pause
    print(i)
    time.sleep(1)
    
    ### Save Every 100 Bills
    if i % 100 == 0:
        with open("TN_Bill_Details_Full.csv", "a", newline = "") as f:
            writer = csv.writer(f)
            writer.writerows(all_data)
            
        all_data = []
        
        with open("TN_Bill_Histories.csv", "a", newline = "") as f:
            writer = csv.writer(f)
            writer.writerows(all_histories)
    
        all_histories = []
                
        with open("TN_Votes.csv", "a", newline = "") as f:
            writer = csv.writer(f)
            writer.writerows(all_votes)
    
        all_histories = []
        
        print("DATA WRITTEN TO FILE")

print("DONE!")

#############
## SAVE EACH DF
#############
# len(skipped)
    
#with open("TN_Bill_Details_Full.csv", "w", newline = "") as f:
#    writer = csv.writer(f)
#    writer.writerows(all_data)
#
#with open("TN_Bill_Histories.csv", "w", newline = "") as f:
#    writer = csv.writer(f)
#    writer.writerows(all_histories)
#    
#with open("TN_Votes.csv", "w", newline = "") as f:
#    writer = csv.writer(f)
#    writer.writerows(all_votes)
