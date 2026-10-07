#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created Dec 3, 2018

Scrape GA Bills 2000+

@author: pb
"""

################## NOTES:
# Votes should be easily scrapable if wanted --- Might be easier to get them here than what I'm scraping in the aggregate
# ---- http://www.legis.ga.gov/Legislation/en-US/VoteList.aspx?Chamber=2

# ******* Can get the 3 terms before 2000 at: http://www.legis.ga.gov/legislation/Archives/Default.aspx *******************

import csv
import os
import re
import time
import urllib
from bs4 import BeautifulSoup
import socket

from selenium import webdriver
from selenium.webdriver.chrome.options import Options  
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException
# import selenium.webdriver.support.ui as ui
# from selenium.webdriver.support import expected_conditions as EC


os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/GA/')


#################################
#### Get Sessions
#################################

search_page = urllib.request.urlopen('http://www.legis.ga.gov/Legislation/en-US/Search.aspx', timeout = 5)
search_soup = BeautifulSoup(search_page, "lxml")

sessions = search_soup.find('select', {'id':'ctl00_SPWebPartManager1_g_3ddc9629_a44e_4724_ae40_c80247107bd6_Session'})
sessions = sessions.find_all('option')

## Create List of Sessions and DROP IF ALREADY SCRAPED
previously_scraped = [t.replace(".csv", "").replace( "GA_Bill_Details_", "") for t in os.listdir('.')]
previously_scraped.append("Previous_Sessions")
session_list = [[s.text.strip(), s['value']] for s in sessions if s.text.strip().replace(' ', '_').replace('-', '_') not in previously_scraped]
     
#################################
## GET BILL PAGE URLS
#################################      

chrome_options = Options()  
chrome_options.add_argument("--headless")  
    
# this_session = session_list[5]  
def get_session_bills(this_session):
    print("\n *** Gathering Bill URLs for the {} *** \n".format(this_session[0]))
    
    driver = webdriver.Chrome(options=chrome_options)  
    #driver = webdriver.Firefox()   # Firefox needs to be downloaded but autoupdates on call
    #driver = webdriver.Safari()

    driver.set_page_load_timeout(15)
    try:
        driver.get('http://www.legis.ga.gov/Legislation/en-US/Search.aspx')
    except TimeoutException as e:
        print("~~~> Timeout! -- Retry in 20 Seconds")
        time.sleep(20)
        driver.quit()
        driver = webdriver.Chrome(options=chrome_options)
        driver.get('http://www.legis.ga.gov/Legislation/en-US/Search.aspx')
    
    #driver.get('http://www.legis.ga.gov/Legislation/en-US/Search.aspx')
    session_select = Select(driver.find_element_by_id('ctl00_SPWebPartManager1_g_3ddc9629_a44e_4724_ae40_c80247107bd6_Session'))
    session_select.select_by_value(this_session[1])
    time.sleep(5)
    
    current_page = 1    
    last_page = driver.find_element_by_name("ctl00$SPWebPartManager1$g_b223cc53_ceb0_41fe_85ca_0c60eb699ad8$ctl05")
    last_page = int(last_page.find_elements_by_tag_name("option")[-1].text)
    
    session_bills = [['bill_num', 'session', 'title', 'bill_url']]   
    
    while True:
        
        ### Scrape Page
        bill_results = BeautifulSoup(driver.page_source, 'lxml')        
        #bill_table = bill_results.find('div', id = 'content')
        bill_rows = bill_results.find_all('div', {'class':['oddLegRow', 'evenLegRow']})        
        
        for row in bill_rows:
            cells = row.find_all("span")
            bill_num = cells[0].get_text().replace("\xa0", " ")
            bill_url = 'http://www.legis.ga.gov/Legislation' + cells[0].findChild("a")['href'].replace("..", "")
            bill_title = cells[1].get_text().replace("\xa0", " ").strip()
            ## Title is generous; more like keywords
            session_bills.append([bill_num, this_session[0], bill_title, bill_url])
           
        print(" -- {} of {}".format(current_page, last_page))
        
        ### Click Next
        # ----> First if statement skips a page that has no data and no next button
        if this_session[0] == '2001 2nd Special Session' and current_page == 5:
            print("~~~ Skipping Error Page - 2001 2nd Special, Page 6")
            current_page += 2
            page_select = Select(driver.find_element_by_name("ctl00$SPWebPartManager1$g_b223cc53_ceb0_41fe_85ca_0c60eb699ad8$ctl05"))
            page_select.select_by_value('7')
            time.sleep(5)            
        elif current_page + 1 <= last_page:
            current_page +=1
            driver.find_element_by_xpath("//a[@title='Select Next Page']").click()
            time.sleep(2)
        else:
            print("\n *** Found ALL URLs for the {} *** \n".format(this_session[0]))
            break
    
    driver.close()
    return(session_bills)   
   
   
    
##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################
    
def get_page_soup(bill_url, parser = 'lxml'):
    
    ### Get HTML
    try:
        page = urllib.request.urlopen(bill_url, timeout = 20).read()
    #except urllib.error.HTTPError: # as e
    #    return('HTTP Error')
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
    
    
#################################
## Function to Scrape BILL DATA
#################################     
# bill_row = session_bills[75]
# http://www.legis.ga.gov/Legislation/en-US/display/20172018/HB/75

def fix_date(date_str):
    date_str = date_str.replace("Jan", "1").replace("Feb", "2").replace("Mar", "3").replace("Apr", "4").replace("May", "5").replace("Jun", "6")
    date_str = date_str.replace("Jul", "7").replace("Aug", "8").replace("Sep", "9").replace("Oct", "10").replace("Nov", "11").replace("Dec", "12")
    return(date_str)

def get_bill_data(bill_row, this_session):
    this_bill_num = bill_row[0]
    this_title = bill_row[2]
    this_url = bill_row[3]
    
    bill_soup = get_page_soup(this_url)
    
    #    try:
    #        bill_page = urllib.request.urlopen(this_url, timeout = 5)
    #    except:
    #        try:
    #            print("\n ~~> Retrying Bill Request")
    #            time.sleep(10)
    #            bill_page = urllib.request.urlopen(this_url, timeout = 10)
    #        except:
    #            print("\n ~~> Retrying Bill Request x 2")
    #            time.sleep(60)
    #            bill_page = urllib.request.urlopen(this_url, timeout = 30)
    #            
    #    bill_soup = BeautifulSoup(bill_page, 'lxml')
    
    sponsors = bill_soup.find('div', text = "Sponsored By")
    sponsors = sponsors.findNext("div")
    sponsors = '; '.join([i.text for i in sponsors.findAll("a")])

    try:
        other_chamber_sponsors = bill_soup.find('div', text = re.compile("Sponsored In.+By"))
        other_chamber_sponsors = other_chamber_sponsors.findNext("div")
        other_chamber_sponsors = '; '.join([i.text for i in other_chamber_sponsors.findAll("a")])
    except:
        other_chamber_sponsors = ''
        
    committees = bill_soup.find('div', text = "Committees")
    committees = committees.findNext("div").findAll("span")
    house_comm = committees[0].text.replace("HC: ", "")
    senate_comm = committees[1].text.replace("SC: ", "")
        
    summary = bill_soup.find('div', text = re.compile("Summary")).findNext("div").get_text().strip()
    
    hist_divs = bill_soup.find('div', text = re.compile("Status History")).findNext("div")
    
    this_hist = []
    order = len(hist_divs.findAll("div"))
    for div in hist_divs.findAll("div"):
        date_action = div.text.split(" - ")  
        date = fix_date(date_action[0])
        this_hist.append([this_bill_num, this_session[0], date, date_action[1], order])
        order -= 1
        
    these_votes = []
    try:
        ## All First-Order Child Divs (Votes) of Vote Section
        votes = bill_soup.find('div', text = "Votes").findNext("div").findAll(recursive=False)
        for vote in votes:
            vote_spans = vote.findAll(["span", "div"])
            date_vote_id = vote_spans[0].text.split(" - ")
            vdate = fix_date(date_vote_id[0])
            vote_url = 'http://www.legis.ga.gov' + vote_spans[0].findChild("a")['href']
            outcome = [count.text for count in vote_spans[1:]]
            these_votes.append([this_bill_num, this_session[0], vdate, date_vote_id[1]] + outcome + [vote_url])
    except:
        pass
    
    bill_details = [this_bill_num, this_session[0], sponsors, other_chamber_sponsors, this_title, house_comm, senate_comm, summary, this_url]    
    return([bill_details, this_hist, these_votes])    
    
    
####################################
## Loop Through Sessions, Get Bill Details
#####################################


for s in session_list:
    
    ### Get List of Bills and Basic Details
    session_bills = get_session_bills(s)
    
    session_bill_details = [['bill_number', 'session', 'sponsors', 'oppo_chamber_sponsors', 'title', 'house_comm', 'senate_comm', 'summary', 'bill_url']]    
    session_actions = [['bill_number', 'session', 'action_date', 'action', 'order']]
    session_votes = [['bill_number', 'session', 'vote_date', 'vote_id', 'num_yeas', 'num_no', 'num_notvoting', 'num_excused']]  
    ## Could scrape the actual roll calls via the rollcallID later if ever needed
    
    print(" ~~~> Scraping Individual Bills - Session {}".format(s))
    
    ### Scrape Each Bill
    num = 1
    for b in session_bills[1:]:
         
        ### Get Bill Data
        this_bill = get_bill_data(b, s)
        time.sleep(1)
        
        ### Append to Agg Files
        session_bill_details.append(this_bill[0])

        for action_row in this_bill[1]:
            session_actions.append(action_row)
       
        if this_bill[2] != []:
            for vote_row in this_bill[2]:
                session_votes.append(vote_row) 
        
        print(" -- ({}) -- {} -- URL: {}".format(num, b[0], b[3]))
        num += 1
    
    ### SAVE!
    session_adj = s[0].replace(" ", "_").replace("-", "_")
    with open("GA_Bill_Details_" + session_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("GA_Bill_Histories_" + session_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)  
    
    with open("GA_Agg_Votes_" + session_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_votes)  

    print("\n\n *********** {} DONE! ************** \n\n".format(s[0]))


print(" ~~~~~~~~~~~~~ ************** ALL SESSIONS DONES *************** ~~~~~~~~~~~~~~~~ ")
