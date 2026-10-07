# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape Virginia Bills by Session

@author: PB
"""

##### NOTES:
# Special Sessions are folded into bill histories -- so if reintroduced, keeps bill number, and action is recorded there
####################

import csv
import os
import urllib
from bs4 import BeautifulSoup
import time
import datetime
#from dateutil import parser as dateparser
import re
from unicodedata import normalize
import socket

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/VA/')

#####################################
##### Extract Session Information
#######################################

this_year = datetime.datetime.now().year
sessions = list(range(1994, this_year - 1, 2))
#### Sessions Labeled Yearly on website but bill numbers increase through two-year terms

# Urls follow: 'http://lis.virginia.gov/cgi-bin/legp604.exe?{}{}+lst+ALL' with last-2-digit yr string and session_num

### Drop Previously Scraped
sessions = [s for s in sessions if 'VA_Bill_Details_' + '{}_{}.csv'.format(s, s+1) not in os.listdir('.')]

del this_year


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################
    
def get_page_soup(bill_url, parser = 'lxml'):
    
    ### Get HTML
    try:
        page = urllib.request.urlopen(bill_url, timeout = 20).read()
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
     

#######################################################
########## GET BILL URLS FOR A SESSION
#####################################################
# s_yr = 1994
# test = get_term_bills(1994)

def get_term_bills(s_yr): 
    
    ### Both Session Years
    term = '{}-{}'.format(s_yr, s_yr + 1)
    print('\n\n\t~~~~ Gathering Bill URLs for the {} Session ~~~~\n'.format(term))

    term_bills = []
    
    for yr in [s_yr, s_yr + 1]:
        for session_iter in range(1, 10):
            this_url = 'http://lis.virginia.gov/cgi-bin/legp604.exe?{}{}+lst+ALL'.format(str(yr)[2:4], session_iter)
            this_soup = get_page_soup(this_url)
            
            ### Exit Loop if no more pages
            check_response = this_soup.body.findAll(text=re.compile('Sorry, your request could not be processed at this time'))
            if(check_response != [] and check_response[0].strip() == 'Sorry, your request could not be processed at this time.'):
                break            
            
            ### Sessions within Each Term --- But note that the general sessions roll bills over
            session = this_soup.find("div", {"id": "mainC"}).findChildren("h2")
            session = session[0].get_text().strip()            
            
            complete = 1    
            print(" ~~~~~~~~~ {} ~~~~~~~~~ ".format(session))   
            while True:       

                these_bills = this_soup.find_all("a", href=re.compile("{}{}\\+sum".format(str(yr)[2:4], session_iter)))            
                these_bills = [[i.text.strip(), term, yr, session, i.parent.get_text().strip(), 'http://lis.virginia.gov' + i['href']] for i in these_bills]
                
                for bill in these_bills:
                    term_bills.append(bill)
                    
                print("  -- " + str(complete))
                
                check_for_more = this_soup.find_all("a", href=re.compile("{}{}\\+lst\\+ALL\\+".format(str(yr)[2:4], session_iter)))
                
                this_soup.find('b', text = re.compile('More...'))
                
                if(len(check_for_more) == 2):
                    this_url = 'http://lis.virginia.gov' + check_for_more[0]['href']
                    this_soup = get_page_soup(this_url)
                    complete += 1 
                else:
                    break
    
    return(term_bills)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = test[100]
# bill_row = term_bills[0]
# test = get_bill_data(term_bills[2])

def get_bill_data(bill_row):    
        
    ### Scrape Bill Page 
    bill_num, term, s_yr, session, descrip, bill_url = bill_row
    bill_num = bill_num.split(' ')[0] + bill_num.split(' ')[1].zfill(4)   
    
    ### Get HTML, Soup    
    bill_soup = get_page_soup(bill_url)
    
    ## Extract Details
    summary = bill_soup.findAll("h4", text=re.compile('SUMMARY'))
    if summary != []:
        try:
            summary = summary[0].findNext("p").get_text().replace("\r\n", " ").strip()
        except:
            summary = ''
    else:
        summary = ''
    
    history = bill_soup.findAll("h4", text=re.compile('HISTORY'))
    if history == []:
        time.sleep(5)
        bill_soup = get_page_soup(bill_url)
        history = bill_soup.findAll("h4", text=re.compile('HISTORY'))
        
    history = history[0].findNext("ul", {"class":"linkSect"})  #.get_text().replace("\r\n", "").strip()
    history = history.findChildren("li")
    
    bill_history = []
    order = 1
    
    for row in history:
        row_text = row.get_text().replace("\xa0", "").strip()
        row_parts = row_text.split(" " or ":", 2)
        date = row_parts[0]
        date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')     
        chamber = re.sub(":", "", row_parts[1]) 
        if len(row_parts) > 2:
            action = row_parts[2].replace("\r\n", "").strip()   
        else:
            action = ''

        a_tag = row.findChildren("a")
        if a_tag == []:
            action_details_url = ''
        else:
            action_details_url = 'http://lis.virginia.gov' + a_tag[0]['href']
 
        bill_history.append([bill_num, term, s_yr, session, date, chamber, action, order, action_details_url])
        order += 1
    
    ###########################################
    
    ### Sponsors --- Link is the same by has mbr instead of sum
    sponsor_url = re.sub("\+sum\+", "+mbr+", bill_url)
    sponsor_soup = get_page_soup(sponsor_url)
    
    house_sponsors = []
    house_sponsors_matches = sponsor_soup.findAll("h4", text=re.compile('HOUSE PATRONS'))
    if house_sponsors_matches != []:
        for hp in range(0, len(house_sponsors_matches)):
            these_sponsors = house_sponsors_matches[hp].findNext("ul", {"class":"linkSect"})
            these_sponsors = these_sponsors.findChildren("li")
            for hm in range(0, len(these_sponsors)):
                house_sponsors.append(normalize("NFKD", these_sponsors[hm].get_text().strip()))
    
    senate_sponsors = []
    senate_sponsors_matches = sponsor_soup.findAll("h4", text=re.compile('SENATE PATRONS'))
    if senate_sponsors_matches != []:
        for sp in range(0, len(senate_sponsors_matches)):
            these_senators = senate_sponsors_matches[sp].findNext("ul", {"class":"linkSect"})
            these_senators = these_senators.findChildren("li")
            for sm in range(0, len(these_senators)):
                senate_sponsors.append(normalize("NFKD", these_senators[sm].get_text().strip()))    

    ### Will Catch Multi Sponsors in theory
    introducing_sponsor = "\n".join(s for s in house_sponsors + senate_sponsors if 'chief patron' in s.lower())

    house_sponsors = '; '.join(house_sponsors)
    senate_sponsors = '; '.join(senate_sponsors)
        
    #############
    ### OUTPUT 
    ##############
    
    bill_details = [bill_num, term, s_yr, session, descrip, introducing_sponsor, house_sponsors, senate_sponsors, summary, bill_url]
    
    return([bill_details, bill_history])
        
########################################################
############## SCRAPE SESSION(S)
############################################
# s = sessions[0]
    
for s in sessions:
    
    #### Output Lists
    term_bill_details = [['bill_id', 'term', 'session_year', 'session', 'short_title', 'sponsor', 'house_sponsors', 'senate_sponsors', 'summary', 'bill_url']]    
    term_actions = [['bill_id', 'term', 'session_year', 'session', 'action_date', 'chamber', 'action', 'order', 'action_details_url']]

    print("\n ------------------- Now Scraping the {}-{} Session ---------------------- \n".format(s, s + 1))    
    
    ### Get all urls for a session-year, including special sessions
    term_urls = get_term_bills(s)

    #### Loop through bills
    num = 1
    total = len(term_urls)
    for bill_row in term_urls:
        
        bill_data = get_bill_data(bill_row)
        
        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n **********".format(num, total, bill_row[0]))
            num += 1
            continue       
            
        term_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                term_actions.append(action_row)

        print(" ({}/{}) -- {}".format(num, total, bill_row[5]))
        num += 1
        
    with open("VA_Bill_Details_" + str(s) + '_' + str(s+1) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(term_bill_details)
        
    with open("VA_Bill_Histories_" +  str(s) + '_' + str(s+1) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(term_actions)
        
    print("\n\n\n ------------- {}-{} Session SCRAPED + DATA SAVED  -------------\n\n\n".format(s, s+1))   


print("  ********************************** ALL DONE ********************************** ")



