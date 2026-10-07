#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 31 13:02:04 2018

SCRAPE HAWAII BILLS

@author: pb
"""


import csv
import os
from bs4 import BeautifulSoup
import re
import time
import urllib
import urllib.request
from urllib.request import Request, urlopen
import datetime
from pathlib import Path
import http.cookiejar
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options  # Import Options from the correct module
import datetime
from selenium.common.exceptions import TimeoutException
from fake_useragent import UserAgent

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

###### ******************** NOTES ********************************************
# -- Different bill page formats for 1999-2000, 2001-2007, 2008+
# -- Data does NOT include special sessions BEFORE 2008
# ** --> Formats are inconsistent and not many bills to begin with
# ***********************************************************************

def open_url_fn(url, max_retries=10, backoff_factor=1.5):
    ua = UserAgent()
    last_exception = None
    for attempt in range(1, max_retries + 1):
        headers = {
            'User-Agent': ua.random,
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
        }
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()     # <-- raises HTTPError on 4xx/5xx
            return (resp.content, resp.url)
        except requests.exceptions.RequestException as e:
            last_exception = e
            wait = backoff_factor ** attempt
            time.sleep(wait)
    http_err = requests.exceptions.HTTPError(
        f"Failed to fetch {url} after {max_retries} attempts"
    )
    raise http_err from last_exception



#######################
##### Extract Session Links
########################

# ****** Legislature Archives divided by YEAR but bill numbers are for two-year legislative sessions ********
# See, eg, https://data.capitol.hawaii.gov/session1999/status/HB100_his_.htm and https://data.capitol.hawaii.gov/session2000/status/HB100_his_.htm

## GET SESSION YEARS
max_year = datetime.datetime.now()
max_year = max_year.year
sessions = [i for i in range(1999, max_year, 2)]
sessions = [str(i) + "_" + str(i + 1) for i in sessions]

## DROPPING PREVIOUSLY SCRAPED TERMS
sessions = [sy for sy in sessions if 'HI_Bill_Details_{}.csv'.format(sy) not in os.listdir('.')]
# sessions = ['1999_2000','2001_2002']
# sessions.reverse()
sessions = ['2019_2020']


#######################################################
########## GET BILL URLS + Special Sessions 2008+
#####################################################
# 2007-2008 Session Bills should overlap but some bills don't show up with 2008 URL so doing it both ways..

# session = sessions[0]

def get_session_bills(session):
    session_years = [int(i) for i in session.split("_") if int(i) <= max_year]
    # If bills continue into subsequent session of the legislature, they'll get additional history which we don't capture.
    # To avoid this, get bills from even-year session first, then double back to get odd-year bills that don't have even-year history.
    session_years.reverse()
    sy_bills = []
    all_bill_nums = [] #### Loop Through Both Years to Catch Any Bills that Aren't Listed on Both Pages (all are, it seems...)
    for year in session_years:
        print("Extracting bills for {}".format(year))
        url = 'https://data.capitol.hawaii.gov/sessions/session{}/bills/'.format(year)
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        WebDriverWait(driver, 240).until(EC.presence_of_element_located((By.LINK_TEXT, '[To Parent Directory]')))
        reg_soup = BeautifulSoup(driver.page_source, 'lxml')
        bill_tags = reg_soup.findAll('a', string = re.compile('pdf|PDF|Pdf|htm|HTM|Htm'))
        if year >= 2013:
            url_base = 'https://data.capitol.hawaii.gov/session/archives/measure_indiv_Archives.aspx?billtype={}&billnumber={}&year={}'
        elif year >= 2008:
            url_base = 'https://data.capitol.hawaii.gov/session/archives/measure_indiv_archives8-12.aspx?billtype={}&billnumber={}&year={}'
            # url_base = 'https://data.capitol.hawaii.gov/Archives/measure_indiv_Archives8-12.aspx?billtype={}&billnumber={}&year={}'
        elif year == 2007:
            url_base = 'https://data.capitol.hawaii.gov/sessions/session{}/status/{}.htm'
        elif year >= 2001:
            url_base = 'https://data.capitol.hawaii.gov/sessions/session{}/status/{}.asp'
        else:
            url_base = 'https://data.capitol.hawaii.gov/sessions/session{}/status/{}_his_.htm'
        for bill in bill_tags:
            this_bill = re.split('_|-|.PDF|.pdf|.Pdf|.html|.HTML|.Html|.htm|.HTM|.Htm', bill.get_text(), 1)[0].upper()
            this_bill = re.sub('COPYOF|COPY OF', '', this_bill).strip()
            if this_bill not in all_bill_nums and this_bill[0:2] not in ['DC', 'GM', 'JC'] and this_bill[0:3] != 'ACT':
                stem = re.sub('\d+', '', this_bill)
                nums = re.sub('[A-Z]+', '', this_bill)
                this_bill = re.sub(' ', '', this_bill)
                if year >= 2008:
                    this_url = url_base.format(stem, nums, year)
                else:
                    this_url = url_base.format(year, this_bill)
                sy_bills.append([session, this_bill, this_url])
                all_bill_nums.append(this_bill)
        if year >= 2023: #### Check for Special Sessions + LOOP THROUGH THEM
            for sp_sess in ['a','b','c','d','e']: # haven't seen more than 3 specials in a year, putting 5 in to be safe but you could add more
                url = 'https://data.capitol.hawaii.gov/session/splsession.aspx?year={}{}'.format(year,sp_sess)
                try:
                    driver.get(url)
                    wait = WebDriverWait(driver, 10)
                    table = wait.until(EC.presence_of_element_located((By.ID, 'MainContent_GridViewReports')))
                except:
                    pass
                table_html = table.get_attribute('outerHTML')
                archive_soup = BeautifulSoup(table_html, 'html.parser')
                for row in archive_soup.select('tr')[1:]:
                    this_bill = row.find('td', {'style': 'color:Red;'}).get_text(strip=True)
                    this_url = 'https://data.capitol.hawaii.gov'+ row.find('a', {'style': 'color:Blue;font-weight:bold;'}).get('href')
                    sy_bills.append([session, this_bill, this_url])
        elif year >= 2011:
            for sp_sess in ['a','b','c','d','e']: # haven't seen more than 3 specials in a year, putting 5 in to be safe but you could add more
                url = 'https://data.capitol.hawaii.gov/session/splsession.aspx?year={}{}'.format(year,sp_sess)
                try:
                    driver.get(url)
                    wait = WebDriverWait(driver, 10)
                    table = wait.until(EC.presence_of_element_located((By.ID, 'ctl00_MainContent_GridViewReports')))
                except:
                    try:
                        driver.get(url)
                        wait = WebDriverWait(driver, 10)
                        table = wait.until(EC.presence_of_element_located((By.ID, 'MainContent_GridViewReports')))
                    except:
                        pass
                table_html = table.get_attribute('outerHTML')
                archive_soup = BeautifulSoup(table_html, 'html.parser')
                for row in archive_soup.select('tr')[1:]:
                    this_bill = row.find('td', {'style': 'color:Red;'}).get_text(strip=True)
                    this_url = 'https://data.capitol.hawaii.gov'+ row.find('a', {'style': 'color:Blue;font-weight:bold;'}).get('href')
                    sy_bills.append([session, this_bill, this_url])
    return(sy_bills)

# def open_url_fn(url):
#     ua = UserAgent()
#     req = Request(
#         url=url,
#         headers={'User-Agent': ua.random}
#     )
#     return(urlopen(req).read(), urlopen(req).url)




#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = session_bills[6]
# session = '1999-2000'
# bill_url = 'https://data.capitol.hawaii.gov/session2007/status/HB1073.htm'
# bill_url = 'https://data.capitol.hawaii.gov/session1999/status/HB10_his_.htm'

def get_bill_data(session, bill_row):
    bill_num = bill_row[1]
    bill_url = bill_row[2]
    bill_url = re.sub(' ','',bill_url)
    session_type = 'Regular'
    if 'measure_indivss' in bill_url:
        session_type = 'Special ({})'.format(re.sub("^.+year=", "", bill_url))
    try:
        page = open_url_fn(bill_url)
        bill_page = page[0]
        page_url = page[1]
    except requests.exceptions.HTTPError as e:
        return(['HTTP Error'])
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(10)
            page = open_url_fn(bill_url)
            bill_page = page[0]
            page_url = page[1]
        except:
            try:
                print("\n ~~> Retrying Bill Request x 2")
                time.sleep(60)
                page = open_url_fn(bill_url)
                bill_page = page[0]
                page_url = page[1]
            except:
                return(['Bill load error'])
    if page_url != bill_url:
        if page_url in ['https://data.capitol.hawaii.gov/home.aspx', 'https://data.capitol.hawaii.gov/errorpage.aspx', 'https://data.capitol.hawaii.gov/session/measurenotfound.aspx?type=archive', 'https://www.capitol.hawaii.gov/session/measurenotfound.aspx?type=invalid']:
            return(['Bill page does not exist'])
    bill_soup = BeautifulSoup(bill_page, 'html.parser')
    bill_actions = []
    if re.compile(r"Measure .+? not found", re.IGNORECASE).search(bill_soup.get_text()):
        return(["Bill page does not exist"])
    if int(session.split('_')[0]) >= 2013:
        measure_table = bill_soup.find('table', {'id': 'measure-info'})
        if session_type == 'Regular':
            title = measure_table.find(
                'span',
                id=re.compile(r'MainContent_ListView1_measure_titleLabel_\d+$')
            ).get_text(strip=True)
            report_title = measure_table.find(
                'span',
                id=re.compile(r'MainContent_ListView1_report_titleLabel_\d+$')
            ).get_text(strip=True)
            summary = measure_table.find(
                'span',
                id=re.compile(r'MainContent_ListView1_descriptionLabel_\d+$')
            ).get_text(strip=True)
            comp_span = measure_table.find(
                'span',
                id=re.compile(r'MainContent_ListView1_companionLabel_\d+$')
            )
            companion = comp_span.find('a').get_text(strip=True) if comp_span and comp_span.find('a') else ''
            package = measure_table.find(
                'span',
                id=re.compile(r'MainContent_ListView1_package_acroLabel_\d+$')
            ).get_text(strip=True)
            introducers = measure_table.find(
                'span',
                id=re.compile(r'MainContent_ListView1_introducerLabel_\d+$')
            ).get_text(strip=True).replace(', ', '; ')
        else:
            title = measure_table.find(
                'span',
                id=re.compile(r'(?:.*_)?Label1_\d+$')
            ).get_text(strip=True)
            report_title = measure_table.find(
                'span',
                id=re.compile(r'(?:.*_)?Label2_\d+$')
            ).get_text(strip=True)
            summary = measure_table.find(
                'span',
                id=re.compile(r'(?:.*_)?Label3_\d+$')
            ).get_text(strip=True)
            companion = measure_table.find(
                'span',
                id=re.compile(r'(?:.*_)?Label4_\d+$')
            ).get_text(strip=True)
            package = measure_table.find(
                'span',
                id=re.compile(r'(?:.*_)?Label5_\d+$')
            ).get_text(strip=True)
            introducers = measure_table.find(
                'span',
                id=re.compile(r'(?:.*_)?Label7_\d+$')
            ).get_text(strip=True).replace(', ', '; ')
        status_div = bill_soup.find(
            'div',
            id=re.compile(r'(?:.*_)?MainContent_UpdatePanel1$')
        )
        status_table = status_div.find(
            'table',
            id=re.compile(r'(?:.*_)?MainContent_GridViewStatus$')
        )
        order = len(status_table) - 3
        for row in status_table.find_all('tr')[1:]:  # Skip the first row (header)
            date = row.find('td', {'style': 'color:Black;'}).get_text(strip=True)
            date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            chamber = row.find('td', {'style': 'color:Black;font-weight:normal;'}).get_text(strip=True)
            action = row.find_all('td')[2].get_text(strip=True)
            bill_actions.append([bill_num, session, session_type, chamber, date, action, order])
            order -= 1
    elif int(session.split('_')[0]) >= 2009:
        if session_type == 'Regular':
            measure_table = bill_soup.select_one('#MainContent_MainContent_ListView1_itemPlaceholderContainer table')
            title = measure_table.find(
                'span',
                id='MainContent_MainContent_ListView1_measure_titleLabel_0'
            ).get_text(strip=True)
            report_title = measure_table.find(
                'span',
                id='MainContent_MainContent_ListView1_report_titleLabel_0'
            ).get_text(strip=True)
            summary = measure_table.find(
                'span',
                id='MainContent_MainContent_ListView1_descriptionLabel_0'
            ).get_text(strip=True)
            companion = measure_table.find(
                'span',
                id='MainContent_MainContent_ListView1_companionLabel_0'
            ).get_text(strip=True)
            package = measure_table.find(
                'span',
                id='MainContent_MainContent_ListView1_package_acroLabel_0'
            ).get_text(strip=True)
            introducers = measure_table.find(
                'span',
                id='MainContent_MainContent_ListView1_introducerLabel_0'
            ).get_text(strip=True
            ).replace(', ', '; ')
        else:
            measure_table = bill_soup.select_one('#MainContent_ListView1_itemPlaceholderContainer table')
            title          = measure_table.find('span', id='MainContent_ListView1_Label1_0').get_text(strip=True)
            report_title   = measure_table.find('span', id='MainContent_ListView1_Label2_0').get_text(strip=True)
            summary        = measure_table.find('span', id='MainContent_ListView1_Label3_0').get_text(strip=True)
            companion      = measure_table.find('span', id='MainContent_ListView1_Label4_0').get_text(strip=True)
            package        = measure_table.find('span', id='MainContent_ListView1_Label5_0').get_text(strip=True)
            introducers    = measure_table.find('span', id='MainContent_ListView1_Label7_0').get_text(strip=True).replace(', ', '; ')
        status_div = bill_soup.find(
            'div',
            id=re.compile(r'(?:.*_)?MainContent_UpdatePanel1$')
        )
        status_table = status_div.find(
            'table',
            id=re.compile(r'(?:.*_)?MainContent_GridViewStatus$')
        )
        order = len(status_table) - 3
        for row in status_table.find_all('tr')[1:]:  # Skip the first row (header)
            date = row.find('td', {'style': 'color:Black;'}).get_text(strip=True)
            date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            chamber = row.find('td', {'style': 'color:Black;font-weight:normal;'}).get_text(strip=True)
            action = row.find_all('td')[2].get_text(strip=True)
            bill_actions.append([bill_num, session, session_type, chamber, date, action, order])
            order -= 1
    elif int(session.split('_')[0]) == 2008:
        measure_table = bill_soup.find('table', {'id': 'measure-info'})
        if session_type == 'Regular':
            title = measure_table.find('span', id='ctl00_MainContent_ListView1_ctrl0_measure_titleLabel').get_text(strip=True)
            report_title = measure_table.find('span', id='ctl00_MainContent_ListView1_ctrl0_report_titleLabel').get_text(strip=True)
            summary = measure_table.find('span', id='ctl00_MainContent_ListView1_ctrl0_descriptionLabel').get_text(strip=True)
            companion = measure_table.find('span', id='ctl00_MainContent_ListView1_ctrl0_companionLabel').get_text(strip=True)
            package = measure_table.find('span', id='ctl00_MainContent_ListView1_ctrl0_package_acroLabel').get_text(strip=True)
            introducers = measure_table.find('span', id='ctl00_MainContent_ListView1_ctrl0_introducerLabel').get_text(strip=True).replace(', ', '; ')
        else:
            title = measure_table.find('span', id='ctl00_MainContent_ListView1_ctrl0_Label1').get_text(strip=True)
            report_title = measure_table.find('span', id='ctl00_MainContent_ListView1_ctrl0_Label2').get_text(strip=True)
            summary = measure_table.find('span', id='ctl00_MainContent_ListView1_ctrl0_Label3').get_text(strip=True)
            companion = measure_table.find('span', id='ctl00_MainContent_ListView1_ctrl0_Label4').get_text(strip=True)
            package = measure_table.find('span', id='ctl00_MainContent_ListView1_ctrl0_Label5').get_text(strip=True)
            introducers = measure_table.find('span', id='ctl00_MainContent_ListView1_ctrl0_Label7').get_text(strip=True).replace(', ', '; ')
        status_div = bill_soup.find(
            'div',
            id=re.compile(r'(?:.*_)?MainContent_UpdatePanel1$')
        )
        status_table = status_div.find(
            'table',
            id=re.compile(r'(?:.*_)?MainContent_GridViewStatus$')
        )
        # status_div = bill_soup.find('div', {'id': 'ctl00_MainContent_UpdatePanel1'})
        # status_table = status_div.find('table', {'id': 'ctl00_MainContent_GridViewStatus'})
        order = len(status_table) - 3
        for row in status_table.find_all('tr')[1:]:  # Skip the first row (header)
            date = row.find('td', {'style': 'color:Black;'}).get_text(strip=True)
            date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            chamber = row.find('td', {'style': 'color:Black;font-weight:normal;'}).get_text(strip=True)
            action = row.find_all('td')[2].get_text(strip=True)
            bill_actions.append([bill_num, session, session_type, chamber, date, action, order])
            order -= 1
    elif int(session.split('_')[0]) >= 2001:
        title = bill_soup.find(text = re.compile('^Measure Title:')).findNext().get_text().strip()
        report_title = bill_soup.find(text = re.compile('^Report Title:')).findNext().get_text().strip()
        summary = bill_soup.find(text = re.compile('^Description:')).findNext().get_text().strip()
        companion = bill_soup.find(text = re.compile('^Companion:')).findNext().get_text().strip()
        package = bill_soup.find(text = re.compile('^Package:')).findNext().get_text().strip()
        introducers = bill_soup.find(text = re.compile('^Introducer')).findNext().get_text().strip()
        introducers = introducers.replace(', ', '; ')
        hist_table = bill_soup.find('th', text = re.compile("Status Text"))
        hist_table = hist_table.findParents('table')[0]
        table_rows = hist_table.findAll('tr')
        order = len(table_rows)
        for row in table_rows[1:]:
            cells = row.findAll('td')
            chamber = cells[1].get_text().strip()
            action = cells[2].get_text().strip()
            date = cells[0].get_text().strip()
            if len(date.split('/')[2]) == 4:
                date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            elif len(date) > 10: ### See, eg, https://data.capitol.hawaii.gov/session2006/status/hb1021.asp
                date = date.split(' ')[0]
                date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            else:
                date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')
            bill_actions.append([bill_num, session, session_type, chamber, date, action, order])
            order -= 1
    else:
        hist_split_str = "\r\n\n\n\n\r\n "
        text_rows = re.split("\r\n\n\r\n|\r\n\n\n\n\r\n |\n\n\r\n", bill_soup.get_text())
        if 'Description:' not in bill_soup.get_text():
            bill_page = open_url_fn(bill_url)
            bill_soup = BeautifulSoup(bill_page, 'html5lib')
            text_rows = re.split("\n", bill_soup.get_text())
            text_rows = [i for i in text_rows if i != '']
            hist_split_str = "\n\n\n\n"
        title = text_rows[1].strip()
        title = re.sub('\s+', ' ', title)
        summary_list = [i for i in text_rows if re.match("^Description: ", i) is not None]
        summary = summary_list[0].lower().replace('description: ', '').strip()
        rt_list = [i for i in text_rows if re.match("^report title: ", i.lower()) is not None]
        report_title = ''
        if rt_list != []:
            report_title = rt_list[0].lower().replace('report title: ', '').strip()
        companion_list = [i for i in text_rows if re.match("^companion ", i.lower()) is not None]
        companion = ''
        if companion_list != []:
            companion = companion_list[0].lower().replace('companion bill: ', '').replace('companion: ', '')
            companion = re.sub('\s+', ' ', companion).strip()
        package_list = [i for i in text_rows if re.match("^subjects: ", i.lower()) is not None or re.match("^package: ", i.lower()) is not None]
        package = ''
        if package_list != []:
            package = package_list[0].lower().replace('subjects: ', '').replace('package: ', '').strip()
            package = re.sub('\s+', ' ', package).strip()
        introducers = [i for i in text_rows if re.match("^By Representative|^By Senator", i) is not None]
        if introducers == []:
            introducers = ''
        else:
            introducers = re.sub('^By Representative\\(s\\) |^By Senator\\(s\\) ', '', introducers[0]).replace(', ', '; ').replace(' / ', '; ').replace('/', '; ')
        if bill_num == "SB1509": # Character errors --> parsing issues
            summary = re.sub("\s\s\s+.+$", "", summary )
            hist_text = '1-27-99        S Introduced and passed First Reading  1-28-99        S Referred to 1. ECD 2. WAM '
        else:
            hist_text = re.split(hist_split_str, bill_soup.get_text())
            if len(hist_text) == 1:
                hist_text = re.split('\r\n\n\n\n\r', bill_soup.get_text())
            hist_text = hist_text[1].strip().replace("19-  - 1 ", "1-19- ")
            hist_text = hist_text.replace('- 1-19', '1-19- ') #  - 1-19
            hist_text = hist_text.replace('21-  - 1 ', '1-21- ')
            hist_text = hist_text.replace('- 1-20 ', '1-20- ')
            hist_text = hist_text.replace('24-  - 1 ', '1-24- ')
            hist_text = re.sub('- 1-24', '1-24- ', hist_text) #  - 1-24| - 1-24|
        if re.match('\d-\d\d- ', hist_text) is not None and session == "1999_2000":
            yr_nums = re.sub(".+session|/status.+$", "", bill_url)
            hist_text = re.sub('(?<=1-\d\d-) ', yr_nums[2:4], hist_text)
        if bill_num == "HB1931" and session == "1999_2000":  # Fixing https://data.capitol.hawaii.gov/session2000/status/HB1931_his_.htm
            hist_text = re.sub('01-2 H', 'H', hist_text)
        hist_text = re.split('(\d-\d-\d\d   +[H|S]) |(\d- \d-\d\d   +[H|S]) |(\d-\d\d-\d\d   +[H|S]) |(\d\d-\d-\d\d   +[H|S]) |(\d\d- \d-\d\d   +[H|S]) |(\d\d-\d\d-\d\d   +[H|S]) ', hist_text)
        hist_text = [re.sub('\r\n|\n', ' ', i).strip() for i in hist_text if i is not None]
        hist_text = [i for i in hist_text if i != '']
        if hist_text[0] == '-':
            hist_text = hist_text[1:] ## Adapting for https://data.capitol.hawaii.gov/session1999/status/SB10_his_.htm
        dates = [i[0:len(i)-1].strip() for index, i in enumerate(hist_text) if index % 2 == 0]
        chambers = [i[-1] for index, i in enumerate(hist_text) if index % 2 == 0]
        actions = [re.sub('\s+', ' ', i) for index, i in enumerate(hist_text) if index % 2 == 1]
        order = 1
        for j in range(0, len(dates)):
            this_date = dates[j]
            if this_date == '':
                this_date = bill_actions[-1][4]
            else:
                this_date = datetime.datetime.strptime(this_date, '%m-%d-%y').strftime('%Y-%m-%d')
            this_action = actions[j].strip()
            bill_actions.append([bill_num, session, session_type, chambers[j], this_date, this_action, order])
            order += 1
    bill_details = [bill_num, session, session_type, title, summary, report_title, companion, package, introducers, bill_url]
    return([bill_details, bill_actions])


########################################################
############## SCRAPE SESSION(S)
############################################
# bill_row = session_bills[3]

for session in sessions:
    session_bill_details = [['bill_number', 'session', 'session_type', 'title',  'summary', 'report_title', 'companion_bills', 'package', 'introducers', 'bill_url']]
    session_actions = [['bill_number', 'session', 'session_type', 'chamber', 'action_date', 'action', 'order']]
    print("\n ----- Now Scraping: Session " + session.replace("_", "-") + " ---------\n")
    session_bills = get_session_bills(session)
    num = 1
    total = len(session_bills)
    skipped_bill_indexes = []
    for bill_row in session_bills[(num-1):]:
        bill_data = get_bill_data(session, bill_row)
        if bill_data[0] == 'HTTP Error':
            print("\n\n ******** SKIPPING {} --- HTTP ERROR ******** \n ---> URL: {} \n\n".format(bill_row[1], bill_row[2]))
            skipped_bill_indexes.append(num)
            num += 1
            continue
        elif bill_data[0] == 'Bill page does not exist':
            if bill_row[2].endswith("2008"):
                print("Trying with 2007 URL instead")
                pattern = re.compile(r'billtype=(?P<bt>[^&]*)&billnumber=(?P<bn>[^&]*)&year=(?P<yr>\d{4})')
                m = pattern.search(bill_row[2])
                billtype  = m.group('bt')
                billnumber = m.group('bn')
                year       = int(m.group('yr'))-1
                bill_row[2] = f'https://data.capitol.hawaii.gov/sessions/session{year}/status/{billtype}{billnumber}.htm'
                bill_data = get_bill_data(session, bill_row)
                if bill_data[0] == 'HTTP Error':
                    print("\n\n ******** SKIPPING {} --- HTTP ERROR ******** \n ---> URL: {} \n\n".format(bill_row[1], bill_row[2]))
                    skipped_bill_indexes.append(num)
                    num += 1
                    continue
                elif bill_data[0] == 'Bill page does not exist':
                    print("\n\n ******** SKIPPING {} --- NO BILL PAGE ******** \n ---> URL: {} \n\n".format(bill_row[1], bill_row[2]))
                    skipped_bill_indexes.append(num)
                    num += 1
                    continue
            else:
                print("\n\n ******** SKIPPING {} --- NO BILL PAGE ******** \n ---> URL: {} \n\n".format(bill_row[1], bill_row[2]))
                skipped_bill_indexes.append(num)
                num += 1
                continue
        session_bill_details.append(bill_data[0])
        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_data[0][0], bill_data[0][9]))
        if num % 50 == 0:
            print("  Time: {}".format(datetime.datetime.now()))
        num += 1
    print("probable skipped rows: {}".format(' '.join(str(x) for x in skipped_bill_indexes)))     # Some bills will raise HTTP errors: go back and get these (have user enter manually, with a space between them)
    skipped_bill_rows = input("Enter numbers of skipped bills: ").split()
    skipped_bill_rows = [int(i) - 1 for i in skipped_bill_rows]
    skipped_session_bills = [session_bills[i] for i in skipped_bill_rows]
    skipped_bill_indexes2 = []
    num = 1
    total = len(skipped_session_bills)
    for bill_row in skipped_session_bills[(num-1):]:
        bill_data = get_bill_data(session, bill_row)
        if bill_data[0] == 'HTTP Error':
            print("\n\n ******** SKIPPING {} --- HTTP ERROR ******** \n ---> URL: {} \n\n".format(bill_row[1], bill_row[2]))
            skipped_bill_indexes2.append(num)
            num += 1
            continue
        elif bill_data[0] == 'Bill page does not exist':
            print("\n\n ******** SKIPPING {} --- NO BILL PAGE ******** \n ---> URL: {} \n\n".format(bill_row[1], bill_row[2]))
            num += 1
            continue
        session_bill_details.append(bill_data[0])
        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_data[0][0], bill_data[0][9]))
        num += 1
    with open("HI_Bill_Details_" + session + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
    with open("HI_Bill_Histories_" + session + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        print("\n\n\n ------------- " + session + " SCRAPED + DATA SAVED  -------------\n\n\n")


print("  ********************************** ALL DONE ********************************** ")
