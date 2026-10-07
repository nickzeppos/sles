# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape Tennessee Bills

@author: PB
"""

#################
### NOTE:
### -- Can delete special_format scraper if this runs --- TN webpage updated specials to match more recent format...? ***********
#################


import csv
import os
import urllib
from bs4 import BeautifulSoup
import re
import time
import datetime
import socket
import gc
import requests

from selenium import webdriver
#from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException

from retry import retry
from timeout_decorator import timeout, TimeoutError
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

########################
###### Extract Session Links
#########################
# *** Note: this does not include current session
### (Can probably drop some of this if we'll be updating each session going forward,
### but keep it just in case)

archive_page = requests.get('http://www.capitol.tn.gov/legislation/archives.html', verify=True)
archive_soup = BeautifulSoup(archive_page.content, "lxml")

### Session Tabs
sessions = archive_soup.find("h2", string = re.compile("Bills and Resolutions")).parent
sessions = sessions.findAll("li", string = re.compile("General Assembly"))

session_data = []
for i in range(0, len(sessions)):
    this_session = sessions[i].findNext("li")
    this_url = this_session.find("a", string = re.compile("Bills and Resolutions"))['href']
    this_label = sessions[i].get_text()
    session_data.append([this_label, this_url])
    session_page = requests.get(this_url, verify = True)
    session_soup = BeautifulSoup(session_page.content , "lxml")
    lblgeneratedcontent_element = session_soup.find(id='lblgeneratedcontent')
    links = lblgeneratedcontent_element.find_all('a')
    for link in links:
        if "Extraordinary" in link.text.strip():
            session_data.append([this_label + ": " + link.text.strip(), link['href']])

### Because we don't have the current session, have to add it
session_num = int(session_data[0][0][:3])
curr_session_num = session_num + 1
curr_session_url = re.sub(str(session_num), str(curr_session_num), session_data[0][1])
curr_session_label = re.sub(str(session_num), str(curr_session_num), session_data[0][0])
curr_session_data = [[curr_session_label, curr_session_url]]
curr_session_page = requests.get(curr_session_url, verify = True)
curr_session_soup = BeautifulSoup(curr_session_page.content , "lxml")
lblgeneratedcontent_element = curr_session_soup.find(id='lblgeneratedcontent')
links = lblgeneratedcontent_element.find_all('a')
for link in links:
        if "Extraordinary" in link.text.strip():
            curr_session_data.append([curr_session_label + ": " + link.text.strip(), link['href']])
session_data = curr_session_data + session_data 

#### Split GA Num + Adjust 101st Session Label
for i in range(0, len(session_data)):
    if session_data[i][0] == '1st Special Session':
        session_data[i][0] = '101st 1st Special Session'
    if session_data[i][0] == '2nd Special Session':
        session_data[i][0] = '101st 2nd Special Session'
    ga_num = session_data[i][0].split(' ', 1)[0]
    if "First" in session_data[i][0]:
        s_type = "SS1"
    elif "Second" in session_data[i][0]:
        s_type = "SS2"
    elif "Third" in session_data[i][0]:
        s_type = "SS3"
    elif "Fourth" in session_data[i][0]:
        s_type = "SS4"
    elif "Fifth" in session_data[i][0]:
        s_type = "SS5"
    else:
        s_type = "RS"
    term = int(re.sub('[a-z]+', '', ga_num))
    term = 1896 + term + (term - 99)
    term = '{}_{}'.format(term, term+1)
    session_data[i] = [ga_num, term, s_type, session_data[i][1]]

del archive_page, archive_soup, sessions, i, this_session, this_url, this_label, ga_num, term, s_type

### Drop Previously Scraped
session_data = [s for s in session_data if 'TN_Bill_Details_{}_{}.csv'.format(s[1], s[2]) not in os.listdir('.')]

### Drop Pre-2023
session_data = [s for s in session_data if int(s[1][:4]) > 2022]

### Drop Current Session
#this_year = datetime.datetime.now().year
#session_data = [s for s in session_data if int(s[1][5:]) < this_year]

#print("\n\n\t ~~~~ DROPPING GENERAL ASSEMBLIES THAT INCLUDE {} ~~~~ \n\n".format(this_year))


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
            page = urllib.request.urlopen(bill_url, timeout = 60).read()
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
    time.sleep(.65)

    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)


######################
####### Get Bill URLS
#########################
# s = session_data[0]
# bills = bill_groups[0]

def get_session_bills(s):

    ga_num, term, s_type, s_url = s

    s_soup = get_page_soup(s_url)

    session_urls = [] #[['bill_id', 'ga_num', 'term', 'session', 'bill_url', 'special_format']]

    ### If/Else Because: Some pages are Lists of Bills, others are Groups of Bills
    these_bills = s_soup.findAll("a", href = re.compile("Billinfo|BillInfo"))
    if these_bills != []:
        for b in these_bills:
            session_urls.append([b.get_text(), ga_num, term, s_type, b['href'], 'No'])

    elif '{} {}'.format(ga_num, s_type) in ['104th Special Session', '1st Special Session', '2nd Special Session', '99th Special Session']:

        bill_groups = s_soup.findAll("a", href = re.compile("HJR|SJR|HR|SR|HB|SB"))
        for bills in bill_groups:
            bills_soup = get_page_soup(re.sub("SpecSessIndex.htm|SpecSessIndex2.htm", bills['href'], s_url))
            these_bills = bills_soup.findAll("a", href = re.compile("BillStatus|Billstatus"))
            if these_bills:
                for b in these_bills:
                    session_urls.append([b.get_text(), ga_num, term, s_type, re.sub("SpecSessIndex.htm", b['href'], s_url), 'Yes'])
            else:
                these_bills = bills_soup.findAll("a", href = re.compile("BillInfo|Billinfo"))
                for b in these_bills:
                    session_urls.append([b.get_text(), ga_num, term, s_type, b['href'], 'Yes'])
    elif '{} {}'.format(ga_num, s_type) in ['106th Special Session']:
        bill_groups = s_soup.findAll("a", href = re.compile("Billindex|BillIndex"))
        for bills in bill_groups:
            bills_soup = get_page_soup(bills['href'])
            these_bills = bills_soup.findAll("a", href = re.compile("Billinfo|BillInfo"))
            for b in these_bills:
                session_urls.append([b.get_text(), ga_num, term, s_type, b['href'], 'No'])
    else:
        bill_groups = s_soup.findAll("a", href = re.compile("Billindex|BillIndex"))
        bill_groups = [i for i in bill_groups if 'Extraordinary' not in i.text]
        for bills in bill_groups:
            bills_soup = get_page_soup('http://wapp.capitol.tn.gov/apps/archives/' + bills['href'])
            these_bills = bills_soup.findAll("a", href = re.compile("Billinfo|BillInfo"))
            for b in these_bills:
                session_urls.append([b.get_text(), ga_num, term, s_type, b['href'], 'No'])

    return(session_urls)


############################################################
### FUNCTIONS TO SCRAPE BILL DATA
################################################################
# this_soup = bill_soup

def scrape_bill(this_soup, bill_info):

    #### Bill Numbers
    companion_bill_num = this_soup.find("span", {"id" : "lblCompNumber"})
    if companion_bill_num:
        companion_bill_num = companion_bill_num.get_text().strip()
        companion_bill_num = re.sub('\(|\)| ', '', companion_bill_num)
    else:
        companion_bill_num = ''

    ## Prime + Co-Prime + Companion Sponsor
    prime_sponsor = this_soup.find("span", {"id" : "lblBillPrimeSponsor"}).get_text().strip()
    prime_sponsor = re.sub("by \*", "", prime_sponsor)
    coprime_sponsors = this_soup.findAll("span", {"id" : "lblBillCoPrimeSponsor"})
    if coprime_sponsors != []:
        coprime_sponsors = re.sub("^, ", "", coprime_sponsors[0].get_text().strip())
    else:
        coprime_sponsors = ''

    companion_sponsor = this_soup.find("span", {"id" : "lblCompPrimeSponsor"})
    if companion_sponsor:
        companion_sponsor = companion_sponsor.get_text().strip()
        companion_sponsor = re.sub("by \*", "", companion_sponsor)
    else:
        companion_sponsor = ''

    ### Title/Caption
    title = this_soup.find("span", {"id" : "lblCaptionText"})
    if title:
        title = title.get_text().strip()
    else:
        title = ''

    ## Abstract
    abstract = this_soup.find("span", {"id" : "lblAbstract"}).get_text().strip()

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
        chamber = ''
        if tr.has_attr('class'):
            chamber = tr['class'][0]
        action = tr.findAll("td")[0].get_text().strip()
        date = tr.findAll("td")[1].get_text().strip()
        action_history.append(bill_info[:4] + [date, chamber, action, order])
        order -= 1

    bill_details = bill_info[:4] + [companion_bill_num, prime_sponsor, coprime_sponsors, companion_sponsor, title, abstract, fiscal_summary, summary, bill_info[4]]
    vote_details = bill_info[:4] + [house_vote_data, senate_vote_data]

    return([bill_details, action_history, vote_details])


###############################
### FUNCTION TO SCRAPE BILL DATA from OLDER SPECIAL SESSIONS
####################################
# this_soup = page_soup

#def scrape_special_bill(this_soup, bill_info):
#
#    #### Bill Numbers
#    companion_bill_num = ''
#
#    ## Prime + Co-Prime + Companion Sponsor
#    prime_sponsor = this_soup.find("a", {"target" : "new"}).parent.get_text()
#    prime_sponsor = re.sub("\(HB.+|\(SB.+|\(\*HB.+|\(\*SB.+", "", prime_sponsor).strip()
#    prime_sponsor = re.sub(".+ by \*|\.", "", prime_sponsor).strip()
#
#    coprime_sponsors = re.sub("^[a-zA-Z]+, \*|\*", "", prime_sponsor).strip()
#    prime_sponsor = re.sub(",.+$", "", prime_sponsor).strip()
#
#    companion_sponsor = ''
#
#    ### Abstract
#    title = ''
#    abstract = this_soup.find("a", {"target" : "new"}).parent.parent.findNext("p").get_text().strip()
#
#    ### Action Table ---  Note: this will yield extra blank rows when opposing chamber has more actions
#    table = this_soup.findAll("table")
#    last_table = table[len(table)-1].findAll("tr")
#    order = len(last_table) - 1
#    action_history = []
#    for tr in last_table:
#        # Note this works even when senate SB action is recorded as those are TR items 2 and 3
#        if  tr.findAll("td") == []:
#            continue
#        else:
#            action = tr.findAll("td")[0].get_text().strip()
#            date = tr.findAll("td")[1].get_text().strip()
#            action_history.append(bill_info + [date, action, order])
#            order -= 1
#
#    ### Fiscal + Regular Summaries
#    sum_soup = get_page_soup(re.sub("BillStatus", "BillSummary", bill_info[4]))
#    if sum_soup == 'HTTP Error' or 'The page cannot be displayed because an internal server error has occurred' in sum_soup.get_text():
#        fiscal_summary = ''
#        summary = ''
#    else:
#        fiscal_summary = sum_soup.find("u", string = re.compile("Fiscal Summary")).parent.get_text().strip()
#        fiscal_summary = ' '.join(re.sub("\n+", "-----", fiscal_summary).split())
#
#        summary = sum_soup.get_text()
#        if "Bill Summary for *" + bill_info[0] in summary:
#            summary = summary.split("Bill Summary for *" + bill_info[0])[1].strip()
#        else:
#            summary = summary.split("Bill Summary for " + bill_info[0])[1].strip()
#        summary = ' '.join(summary.split())
#
#    vote_details = []
#    bill_details = bill_info[:4] + [companion_bill_num, prime_sponsor, coprime_sponsors, companion_sponsor, title, abstract, fiscal_summary, summary, bill_info[4]]
#    return([bill_details, action_history, vote_details])
#



#################
### Function to Get Main Page and Retry Driver Get Calls
##################
#https://stackoverflow.com/questions/43003924/python-selenium-refresh-if-wait-more-than-10s

@retry(TimeoutError, tries=3)
@timeout(10)
def get_with_retry(driver, url):
    driver.get(url)
    time.sleep(.5)

###############################
# url = bill_info[4]

def get_main_page(driver, bill_info):

    ### Navigate to Web Page
    url = bill_info[4]
    get_with_retry(driver, url)

    ### Check Page Load
    check_soup = BeautifulSoup(driver.page_source, "lxml")
    if 'The page cannot be displayed because an internal server error has occurred' in check_soup.get_text():
        time.sleep(60)
        get_with_retry(driver, url)
        time.sleep(5)
        check_soup = BeautifulSoup(driver.page_source, "lxml")
        if 'The page cannot be displayed because an internal server error has occurred' in check_soup.get_text():
            time.sleep(180)
            get_with_retry(driver, url)
            time.sleep(15)
            check_soup = BeautifulSoup(driver.page_source, "lxml")
            if 'The page cannot be displayed because an internal server error has occurred' in check_soup.get_text():
                #skipped.append(bill_info)
                print("ERROR -- SKIPPED BILL -- {}".format(url))
                return('SKIP')

    if check_soup.findAll('span', id = 'lblErrorPanelMsg') != []:
        time.sleep(60)
        get_with_retry(driver, url)
        time.sleep(5)
        check_soup = BeautifulSoup(driver.page_source, "lxml")
        if check_soup.findAll('span', id = 'lblErrorPanelMsg') != []:
            time.sleep(180)
            get_with_retry(driver, url)
            time.sleep(15)
            check_soup = BeautifulSoup(driver.page_source, "lxml")
            if check_soup.findAll('span', id = 'lblErrorPanelMsg') != []:
                print("ERROR -- BILL NOT FOUND -- {}".format(url))
                return('SKIP')

    ### Show Cosponsors if Present
    timeout = 60
    # cospon_button = driver.find_element(By.ID, 'lnkShowCoPrimes')
    # if cospon_button != []:
    #     cospon_button.click()
    #     try:
    #         element_present = EC.presence_of_element_located((By.XPATH, "//a[@class='coprimelink-hide']")) # //a[@class='coprimelink-hide']
    #         WebDriverWait(driver, timeout).until(element_present)
    #     except TimeoutException:
    #         print("Timed out waiting for page to load --> Retrying!")
    #         return('Retry')

    try:
        # Find the element with the given ID
        cospon_button = driver.find_element(By.ID, 'lnkShowCoPrimes')

        # If the element is found, proceed with interactions
        if cospon_button.is_displayed():
            cospon_button.click()
            try:
                element_present = EC.presence_of_element_located((By.XPATH, "//a[@class='coprimelink-hide']"))
                WebDriverWait(driver, timeout).until(element_present)
            except TimeoutException:
                print("Timed out waiting for page to load --> Retrying!")
                return('Retry')
                # Handle the retry logic as needed
    except NoSuchElementException:
        pass

    ### Show Caption Text if Present
    # caption_button = driver.find_element(By.ID, 'lnkShowCaptionText')
    # if caption_button != []:
    #     caption_button.click()
    #     try:
    #         element_present = EC.presence_of_element_located((By.XPATH, "//a[@id='lnkShowCaptionText' and @class='coprimelink-hide']"))
    #         WebDriverWait(driver, timeout).until(element_present)
    #     except TimeoutException:
    #         print("Timed out waiting for page to load --> Retrying!")
    #         return('Retry')

    try:
        caption_button = driver.find_element(By.ID, 'lnkShowCaptionText')
        if caption_button.is_displayed():
            caption_button.click()
            try:
                element_present = EC.presence_of_element_located((By.XPATH, "//a[@id='lnkShowCaptionText' and @class='coprimelink-hide']"))
                WebDriverWait(driver, timeout).until(element_present)
            except TimeoutException:
                print("Timed out waiting for page to load --> Retrying!")
                return('Retry')
    except NoSuchElementException:
        pass

    #### Return Main Page Soup
    page_soup = BeautifulSoup(driver.page_source, "lxml")
    return(page_soup)


########################################################
############## SCRAPE BILLS BY SESSION
############################################
# s = session_data[0]
# bill_info = session_bills[0]

### Selenium Options
chrome_options = Options()
chrome_options.add_argument("--headless")

##### Loop Through Sessions
for s in session_data:

    #### Output Lists
    session_bill_details = [['bill_id', 'ga_num', 'term', 'session', 'companion_bill_id', 'sponsor', 'cosponsors', 'companion_sponsor', 'title', 'abstract', 'fiscal_summary', 'summary', 'bill_url']]
    session_actions = [['bill_id', 'ga_num', 'term', 'session', 'date', 'chamber', 'action', 'order']]
    session_votes = [['bill_id', 'ga_num', 'term', 'session', 'house_votes', 'senate_votes']]

    print("\n ------------------- Now Scraping: {} {} ({}) ---------------------- \n".format(s[0], s[2], s[1]))

    ### Get all bills for a specific session
    session_bills = get_session_bills(s)

    ### Start Selenium
    driver = webdriver.Chrome(options=chrome_options)
    # driver = webdriver.Firefox()

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        #### Need to Use Selenium to get Initial Page otherwise can't get Coprime Sponsors or Summary
        bill_soup = get_main_page(driver, bill_info)

        ### Retry if Page Load Issue
        if bill_soup == 'Retry':
            gc.collect()
            time.sleep(60)
            bill_soup = get_main_page(driver, bill_info)

        ### Skipp bills with errors or missing data
        if bill_soup == 'SKIP':
            session_bill_details.append(bill_info[0:4] + [ '' ] * 7 + ['NO DATA ON BILL PAGE', bill_info[4]])
            print(" ********** \n ({}/{}) -- {} -- ERROR ON BILL PAGE: SKIPPING - {} \n **********".format(num, total, bill_info[0], bill_info[4]))
            continue

        #### Clean Using Regular or Special Format Scraper
        bill_data = scrape_bill(bill_soup, bill_info)

        ### Don't need the special format scraper with website updates???
        #if bill_info[5] == 'No':
        #    bill_data = scrape_bill(bill_soup, bill_info)
        #else:
        #    bill_data = scrape_special_bill(bill_soup, bill_info)

        ### Save Bill Details
        session_bill_details.append(bill_data[0])

        ### Save Actions
        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)

        ### Save Vote Data
        if bill_data[2] != []:
            for vote_row in bill_data[2]:
                session_votes.append(vote_row)

        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_info[0], bill_info[4]))
        num += 1

    #### Close Selenium Driver
    driver.quit()
    del driver

    #### Session Type for Save
    #### Save Data
    with open("TN_Bill_Details_{}_{}.csv".format(s[1], s[2]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)

    with open("TN_Bill_Histories_{}_{}.csv".format(s[1], s[2]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)

    with open("TN_Votes_{}_{}.csv".format(s[1], s[2]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_votes)

    print("\n\n\n ------------- {} {} SCRAPED + DATA SAVED  -------------\n\n\n".format(s[0], s[1]))


print("\n\n\n ********************* ALL SESSIONS COMPLETE ********************* \n\n\n ")
print('\n\n\n ********************* CHECK "NO DATA ON BILL PAGE" BILLS *************** \n\n\n')
