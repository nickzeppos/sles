# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape Arkansas Bills

@author: PB
"""

import csv
import os
import urllib
from bs4 import BeautifulSoup
import re
import time
import socket
import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options 
from selenium.webdriver.common.keys import Keys
import selenium.webdriver.support.ui as ui

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/AR/')

###########################
#### ADAPTING OPEN STATES CODE? No Data in the Txt Files.... Not Possible...
############################
# https://github.com/openstates/openstates/blob/master/openstates/ar/bills.py
#
#def get_utf_16_ftp_content(url):
#    # Rough to do this within Scrapelib, as it doesn't allow custom decoding
#    raw = urllib.request.urlopen(url).read().decode('utf-16')
#    # Also, legislature may use `NUL` bytes when a cell is empty
#    NULL_BYTE_CODE = '\x00'
#    text = raw.replace(NULL_BYTE_CODE, '')
#    return text
#    
#url = "ftp://www.arkleg.state.ar.us/SessionInformation/LegislativeMeasures.txt"
#page = csv.reader(StringIO(get_utf_16_ftp_content(url)), delimiter='|')
#
#for row in page:
#    print(row)
#    break


########################
###### Identify Sessions
#########################
# Need to Drop Everything Before the 81st --- No Actions Listed - Just Bill Name and Sponsor

session_search = urllib.request.urlopen('http://www.arkleg.state.ar.us/SearchCenter/Pages/historicalbil.aspx')
session_soup = BeautifulSoup(session_search, 'lxml')

session_table = session_soup.find('table', id = 'ctl00_m_g_1c6d174c_cc5d_4260_9e4d_0b6f1436d9e8_ctl00_ctl04_D')
#sessions = session_table.select('td[class*="dxtlSelectionCell"]')
sessions = session_table.findAll('tr')

session_names = [s.find('td', {'class':'dxtl dxtl__B0'}).get_text() for s in sessions]
session_row_id = [s['id'] for s in sessions]

### Keeping only the full xxth General Assembly (not the regular/extra session subsets)
ga_details = [[s, x] for [s,x] in zip(session_names, session_row_id) if 'General Assembly' in s]

### Dropping Previously Scraped and Old Years without Sufficient Data
dont_scrape = ['76th General Assembly', '77th General Assembly', '78th General Assembly', '79th General Assembly', '80th General Assembly']

for i in range(0, len(ga_details)):
    if ga_details[i][0] in dont_scrape:
        continue
    ga_adj = re.sub(" ", "_", ga_details[i][0])
    if 'AR_Bill_Details_' + ga_adj + '.csv' in os.listdir('.'):
        dont_scrape.append(ga_details[i][0])
        print("Previously Scraped: " + ga_details[i][0])


ga_details = [[s,x] for [s,x] in ga_details if s not in dont_scrape  ]

####################################
#### Get List of Bills for a Session
####################################
# this_ga = ga_details[4]
# soup = this_page_soup
# bill_list = []

def get_page_urls(soup, bill_list, this_ga):
    table = soup.find('div', id = 'WebPartWPQ5')
    bills = table.findAll('a', href = re.compile('BillInformation.aspx'))
    for b in bills:
        #[['http://www.arkleg.state.ar.us' + b['href'], b.text.replace('\xa0', ' ').split(' - ')] for b in bills]
        url = 'http://www.arkleg.state.ar.us' + b['href']
        details = b.text.replace('\xa0', ' ').split(' - ')
        bill_list.append([details[0], this_ga, details[1], url])
    return(bill_list)
    
### Note: Session is a list of 2 here [name, id for row on search page]
def get_ga_urls(this_ga):
    
    print("\n ~~~~ Gathering Links to Bill Pages ~~~~ \n ")
        
    chrome_options = Options()  
    chrome_options.add_argument("--headless")  
    driver = webdriver.Chrome(options=chrome_options)  
    # driver = webdriver.Firefox()      
    driver.get('http://www.arkleg.state.ar.us/SearchCenter/Pages/historicalbil.aspx')
    wait = ui.WebDriverWait(driver, 10)
    
    ## Uncheck All -- Need to Click Twice
    all_button = driver.find_element_by_xpath('//span[@id="ctl00_m_g_1c6d174c_cc5d_4260_9e4d_0b6f1436d9e8_ctl00_ctl04_R-0_D"]')
    all_button.click()
    time.sleep(5)
    all_button = driver.find_element_by_xpath('//span[@id="ctl00_m_g_1c6d174c_cc5d_4260_9e4d_0b6f1436d9e8_ctl00_ctl04_R-0_D"]')
    all_button.click()
    time.sleep(5)
    driver.save_screenshot('check_search_boxes.png')
    
    ## Click GA to Scrape
    ga_button = driver.find_element_by_id(this_ga[1] + "_D")
    ga_button.click()
    time.sleep(2)
    
    ## Search
    driver.find_element_by_xpath("//input[@name='ctl00$m$g_1c6d174c_cc5d_4260_9e4d_0b6f1436d9e8$ctl00$ctl19']").click()
    time.sleep(10)
    
    ## Iterate Through Pages, Grabbing Links, Checking if More Pages
    more_pages = True
    ga_bills = []
    num = 1
    while more_pages == True:
        ## Results Page
        this_page_soup = BeautifulSoup(driver.page_source, 'lxml')
        ## Extract Urls on Page
        ga_bills = get_page_urls(this_page_soup, ga_bills, this_ga[0])
        print(" -- Results Page {}".format(num))
        num += 1
        ## Check if More Pages
        if this_page_soup.find('a', {'class':'LEV-LBNEXT'}) is not None:
            current_page = this_page_soup.find('a', {'class':'LEV-LBNUM', 'disabled':'disabled'}).text
            next_page = '[' + str(int(current_page.replace('[', '').replace(']', '')) + 1) + ']'
            next_button = driver.find_element_by_class_name("LEV-LBNEXT")
            next_button.send_keys(Keys.ENTER)
            ## Wait until next page loads
            wait.until(lambda driver: driver.find_element_by_xpath("//*[contains(text(), '" + next_page + "')]").is_displayed())
            time.sleep(.5)
        else:
            more_pages = False
    
    ## Close and Return Data
    driver.close()
    return(ga_bills)


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
    
    if page_soup.find('td', text = re.compile("Bill Number")) is None:
        time.sleep(10)
        bill_page = urllib.request.urlopen(bill_url, timeout = 15)
        time.sleep(5)
        page_soup = BeautifulSoup(bill_page, "lxml")
    
    return(page_soup)     


  
########################
####### Function to Scrape Individual Bills
##########################
# bill_row = all_session_bills[0]
# error: http://www.arkleg.state.ar.us/assembly/2017/2018S2/Pages/CoSponsors.aspx?measureno=HB1003

def get_bill_data(bill_row):
    
    bill_num, this_ga, title, bill_url = bill_row
    
    bill_soup = get_page_soup(bill_url)
        
    ## Supplement Bill Details
    act_num = bill_soup.find('td', text = re.compile("Act Number"))
    if act_num is not None:
        act_num = act_num.findNext('td').text.strip()
    else:
        act_num = ''
    
    intro_date = bill_soup.find('td', text = re.compile("Introduction Date"))
    if intro_date is not None:
        intro_date = intro_date.findNext('td').text.strip().split(" ")[0]
    else:
        intro_date = ''
     
    ### Need the SESSION -- Eg, Regular, First Extra....
    this_session = bill_soup.find('span', {'class': 'breadcrumbCurrent'}).text.strip()
      
    ### Get Sponsors
    # primary_sponsor = bill_soup.find('td', text = re.compile("Lead Sponsor"))
    # primary_sponsor = primary_sponsor.findNext('td').text.strip()
    
    cosponsor_link = bill_soup.find('a', text = re.compile("View Cosponsor"))
    if cosponsor_link is None:
        all_primary = bill_soup.find('td', text = re.compile("Lead Sponsor"))
        all_primary = all_primary.findNext('td').text.strip()
        all_cosponsors = ''
    else:
        try:
            # sponsor_page = urllib.request.urlopen('http://www.arkleg.state.ar.us/assembly/2011/2011R/Pages/CoSponsors.aspx?measureno=SB821')
            sponsor_page = urllib.request.urlopen(cosponsor_link['href'], timeout = 5)
        except:
            print(" ~~~> Retrying Cosponsor Page Request - {}".format(bill_row[3]))
            time.sleep(10)
            sponsor_page = urllib.request.urlopen(cosponsor_link['href'], timeout = 10)
        time.sleep(.75)
        
        # Get Cosponsors
        sponsor_table = BeautifulSoup(sponsor_page, 'lxml')
        # sponsor_table = sponsor_table.find('div', id = 'ctl00_m_g_14614233_b753_4c29_9c16_2e08eb727b99')        
        # sponsor_table = sponsor_table.find('table' , {'class':'s4-wpTopTable'})
        sponsor_table = sponsor_table.find('div' , {'class':'ms-WPBody'})        
        if sponsor_table is None:
            sponsor_page = urllib.request.urlopen(cosponsor_link['href'], timeout = 5)
            time.sleep(10)
            sponsor_table = BeautifulSoup(sponsor_page, 'lxml')
            sponsor_table = sponsor_table.find('div', id = 'ctl00_m_g_14614233_b753_4c29_9c16_2e08eb727b99')    
            
        all_sponsors = [ td.text.strip() for td in sponsor_table.findAll('td') if td.text.strip() != '']
        if all_sponsors == []:
            all_primary = bill_soup.find('td', text = re.compile("Lead Sponsor"))
            all_primary = all_primary.findNext('td').text.strip()
            all_cosponsors = ''
        else:
            primary_index = all_sponsors.index("Primary Sponsor(s)")
            try: 
                co_index = all_sponsors.index("CoSponsors")
            except:
                co_index = len(all_sponsors)
            all_primary = '; '.join(all_sponsors[(primary_index + 1):co_index])
            all_cosponsors = '; '.join(all_sponsors[(co_index + 1):])
            
    ### Bill Histories
    bill_hist = []  
    hist_tab = bill_soup.find('table', id = re.compile('gvBillHistory_DXMainTable'))
    rows = hist_tab.findAll('tr', {'class': re.compile('dxgvDataRow')})
    
    ## Constraing Bill with Error -- Out of Order and Repeated Action
    if this_session == 'Regular Session, 1997' and bill_num == 'SB58':
        rows = rows[0:-2]
    
    ### Loop through Actions
    order = len(rows)
    for row in rows:
        cells = row.findAll('td')
        clean_row = [cell.get_text().strip() for cell in cells]
        chamber = clean_row[0]
        date = clean_row[1].split(' ')[0]
        date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')    
        action = clean_row[2]
        vote_url = clean_row[3]
        
        if 'votes' in cells[3].text.lower():
            clean_row[3] = cells[3].findChild('a')['href']
        bill_hist.append([bill_num, this_ga, this_session, chamber, date, action, order, vote_url])
        order -= 1
        
    ## Return Data
    bill_details = [bill_num, this_ga, this_session] + [all_primary, act_num, intro_date, all_cosponsors] + [title, bill_url]
    return([bill_details, bill_hist])
    
###########################
#### Loop Through All Bills
##############################
# ga = ga_details[1]
# bill_row = these_bills[1389]

for ga in ga_details:
    
    if '92nd General Assembly' in ga[0]:
        print("\n  **** SKIPPING 92nd General Assembly -- Current 2019 Session ****  \n")
        continue
    else:
        print("\n  **** Gathering Data for the {} ****  \n".format(ga[0]))

    these_bills = get_ga_urls(ga)    

    #### Fix Broken Links --- SB996 is linked to the fisacal session when url should go to regular session
    if ga[0] == '87th General Assembly':
        for i in range(0, len(these_bills)):
            if these_bills[i][0] == 'SB996' and these_bills[i][3] == 'http://www.arkleg.state.ar.us/assembly/2009/2010F/Pages/BillInformation.aspx?measureno=SB996':
                these_bills[i][3] = 'http://www.arkleg.state.ar.us/assembly/2009/R/Pages/BillInformation.aspx?measureno=SB996'
                print("Bill URL Error Fixed -- {}".format(these_bills[i][0]))
                
    ### Scrape Actions and Bill Details
    print("\n ~~~~ Scraping Individual Bill Pages - {} Bills Total ~~~~ \n".format(len(these_bills)))
    
    ### Full Data Lists
    ga_bill_details = [['bill_num', 'ga_num', 'session', 'primary_sponsors', 'act_num', 'intro_date', 'cosponsors', 'title', 'bill_url']]
    ga_bill_histories = [['bill_num', 'ga_num', 'session', 'chamber', 'action_date', 'action', 'order', 'vote_url']]   
    
    ### Looping Through All Bills in Session
    num = 1
    for this_bill in these_bills:
        if this_bill[3] == 'http://www.arkleg.state.ar.us/assembly/2001/R/Pages/BillInformation.aspx?measureno=SB889':
            print("Skipping Bill with Error")
            continue
        bill_data = get_bill_data(this_bill)
        ga_bill_details.append(bill_data[0])
        for hist in bill_data[1]:
            ga_bill_histories.append(hist)
        print(" -- ({}) {}: {}".format(num, this_bill[0], this_bill[3]))
        num += 1

    print("\n ************ Finished {} ************ \n".format(ga[0]))   
    
    ### SAVE!
    ga_adj = ga[0].replace(' ', '_')
    with open("AR_Bill_Details_" + ga_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(ga_bill_details)
        
    with open("AR_Bill_Histories_" + ga_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(ga_bill_histories)  


print("\n\n ************ ALL SESSIONS COMPLETE ************ \n\n".format(ga[0]))   