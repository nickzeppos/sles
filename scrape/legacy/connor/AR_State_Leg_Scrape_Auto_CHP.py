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
import math

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
import selenium.webdriver.support.ui as ui
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from pathlib import Path


# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

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

session_table = session_soup.find('div', id = 'treeBienniumSession')

ga_details = []
for assembly_item in session_table.select('li > input[id^="biennium"]')[1:]:
    assembly_label = assembly_item.find_next('label').get_text(strip=True)
    session_names = []
    for session_item in assembly_item.find_next('ul').select('li > input[id^="session"]'):
        session_label = session_item.find_next('label', class_='session').get_text(strip=True)
        session_names.append(session_label)
    ga_details.append([assembly_label,session_names])

### Dropping Previously Scraped and Pre-2023 Sessions
dont_scrape = [item[0] for item in ga_details if int(item[0][:2]) < 94]
for i in range(0, len(ga_details)):
    ga_adj = re.sub(" ", "_", ga_details[i][0])
    if 'AR_Bill_Details_' + ga_adj + '.csv' in os.listdir('.'):
        dont_scrape.append(ga_details[i][0])
        print("Previously Scraped: " + ga_details[i][0])

ga_details = [[s,a] for [s,a] in ga_details if s not in dont_scrape  ]

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

def get_ga_urls(this_ga):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get('https://www.arkleg.state.ar.us/Bills/ViewBills?type=HB&ddBienniumSession=2023%2F2023S1')
    wait = ui.WebDriverWait(driver, 10)
    dropdown = Select(driver.find_element(By.ID,'ddBienniumSessionViewBills'))
    option_texts = [option.text.split(' - ', 1)[1] for option in dropdown.options[1:]]
    option_texts_full = [option.text for option in dropdown.options[1:]]
    ga_bills = []
    for session_label in this_ga[1]:
        print("\n ~~~~ Gathering Links for {}: {} ~~~~ \n ".format(this_ga[0],session_label))
        matching_option = next(opt for opt in option_texts if session_label in opt)
        matching_option_text = next(opt for opt in option_texts if session_label in opt)
        matching_option_full = option_texts_full[option_texts.index(matching_option_text)]
        words = matching_option_full.split()
        arg1 = words[0]
        session_type = matching_option_full.split(' - ', 1)[1].split(', ')[0]
        year = words[-1]
        if session_type == 'Regular Session':
            arg2 = f'{year}R'
        elif session_type == 'Fiscal Session':
            arg2 = f'{year}F'
        elif session_type.startswith('First'):
            arg2 = f'{year}S1'
        elif session_type.startswith('Second'):
            arg2 = f'{year}S2'
        elif session_type.startswith('Third'):
            arg2 = f'{year}S3'
        for bill_type in ['HB','SB']:
            driver.get('https://www.arkleg.state.ar.us/Bills/ViewBills?type={}&ddBienniumSession={}%2F{}'.format(bill_type,arg1,arg2))
            page_info_element = driver.find_element(By.XPATH, '//div[@class="row tableSectionFooter"]/div[@class="col-md-12"]')
            match = re.match(r"Page (\d+) of (\d+)", page_info_element.text)
            current_page = int(match.group(1))
            last_page = int(match.group(2))
            while True:
                print("Extracting bills: {}/{}".format(current_page,last_page))
                rows = driver.find_elements(By.XPATH, '//div[@role="grid"]/div[@role="row"]')
                for row in rows[1:]:
                    row_text = row.text
                    bill_number_end = row_text.find('\n', 0)
                    bill_number = row_text[0:bill_number_end].strip()
                    bill_link_element = row.find_element(By.XPATH, './/div[@role="gridcell"][1]/div/a')
                    bill_link = bill_link_element.get_attribute("href")
                    ga_bills.append([bill_number,this_ga[0],session_label,bill_link])
                if current_page < last_page:
                    try:
                        driver.find_element(By.CSS_SELECTOR, '#cookieConsent .btn.btn-tertiary').click()
                    except:
                        pass
                    next_page_link_xpath = f'//div[@class="row tableSectionFooter"]/div[@class="col-md-12"]/a[text()="{current_page + 1}"]'
                    driver.find_element(By.XPATH, next_page_link_xpath).click()
                    current_page += 1
                else:
                    break
    driver.close()
    return(ga_bills)


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################

def get_page_soup(bill_url, parser = 'lxml'):
    try:
        page = urllib.request.urlopen(bill_url, timeout = 3).read()
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
    page_soup = BeautifulSoup(page, parser)
    if page_soup.find('td', string = re.compile("Bill Number")) is None:
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
    bill_num, this_ga, this_session, bill_url = bill_row
    bill_soup = get_page_soup(bill_url)
    bill_title_element = bill_soup.find('h1', style='font-size:24px; text-transform:uppercase;')
    bill_title = bill_title_element.text[bill_title_element.text.find(' - ')+3:]
    act_number_cell = bill_soup.find('div', {'class': 'col-md-4', 'role': 'gridcell'}, string='Act Number:')
    if act_number_cell is not None:
        act_number_element = act_number_cell.find_next('div', {'class': 'col-md-8', 'role': 'gridcell'})
        act_number_text = act_number_element.text.strip()
        act_num = ''.join(filter(str.isdigit, act_number_text))
    else:
        act_num = ''
    intro_date = bill_soup.find('div', string='Introduction Date:')
    if intro_date is not None:
        intro_date = intro_date.findNext('div').text.strip().split("\xa0")[0]
    else:
        intro_date = ''
    all_primary = bill_soup.find('div', string='Lead Sponsor:')
    all_primary = all_primary.find_next('div', {'class': 'col-md-8', 'role': 'gridcell'}).find('a').text.strip()
    all_cosponsors = '' # there are cosponsors, but it seems that peter doesn't care about them by the end
    bill_hist = []
    bill_status_history_section = bill_soup.find('h3', string='Bill Status History')
    table_rows = bill_status_history_section.find_next('div', {'role': 'grid'}).find_all('div', {'role': 'row'})
    order = len(table_rows)
    for row in table_rows[1:]:
        chamber = row.find('div', {'aria-colindex': '1', 'role': 'gridcell'}).text.strip()
        date = row.find('div', {'aria-colindex': '2', 'role': 'gridcell'}).text.strip().split("\xa0")[0]
        date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
        action = row.find('div', {'aria-colindex': '3', 'role': 'gridcell'}).text.strip()
        vote_url = 'https://www.arkleg.state.ar.us' + row.find('div', {'aria-colindex': '4', 'role': 'gridcell'}).find('a')['href'] if row.find('div', {'aria-colindex': '4', 'role': 'gridcell'}).find('a') else ''
        bill_hist.append([bill_num, this_ga, this_session, chamber, date, action, order, vote_url])
        order -= 1
    bill_details = [bill_num, this_ga, this_session] + [all_primary, act_num, intro_date, all_cosponsors] + [bill_title, bill_url]
    return([bill_details, bill_hist])

###########################
#### Loop Through All Bills
##############################
# ga = ga_details[1]
# bill_row = these_bills[1389]

for ga in ga_details:
    these_bills = get_ga_urls(ga)
    if ga[0] == '87th General Assembly':
        for i in range(0, len(these_bills)):
            if these_bills[i][0] == 'SB996' and these_bills[i][3] == 'http://www.arkleg.state.ar.us/assembly/2009/2010F/Pages/BillInformation.aspx?measureno=SB996':
                these_bills[i][3] = 'http://www.arkleg.state.ar.us/assembly/2009/R/Pages/BillInformation.aspx?measureno=SB996'
                print("Bill URL Error Fixed -- {}".format(these_bills[i][0]))
    print("\n ~~~~ Scraping Individual Bill Pages - {} Bills Total ~~~~ \n".format(len(these_bills)))
    ga_bill_details = [['bill_num', 'ga_num', 'session', 'primary_sponsors', 'act_num', 'intro_date', 'cosponsors', 'title', 'bill_url']]
    ga_bill_histories = [['bill_num', 'ga_num', 'session', 'chamber', 'action_date', 'action', 'order', 'vote_url']]
    num = 1
    for this_bill in these_bills:
        print(" -- ({}/{}) {}: {}".format(num, len(these_bills), this_bill[0], this_bill[3]))
        if this_bill[3] == 'http://www.arkleg.state.ar.us/assembly/2001/R/Pages/BillInformation.aspx?measureno=SB889':
            print("Skipping Bill with Error")
            continue
        bill_data = get_bill_data(this_bill)
        ga_bill_details.append(bill_data[0])
        for hist in bill_data[1]:
            ga_bill_histories.append(hist)
        num += 1
    print("\n ************ Finished {} ************ \n".format(ga[0]))
    ga_adj = ga[0].replace(' ', '_')
    with open("AR_Bill_Details_" + ga_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(ga_bill_details)
    with open("AR_Bill_Histories_" + ga_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(ga_bill_histories)


print("\n\n ************ ALL SESSIONS COMPLETE ************ \n\n".format(ga[0]))
