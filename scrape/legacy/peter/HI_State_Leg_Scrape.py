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
import datetime

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/HI/')


###### ******************** NOTES ********************************************
# -- Different bill page formats for 1999-2000, 2001-2007, 2008+
# -- Data does NOT include special sessions BEFORE 2008
# ** --> Formats are inconsistent and not many bills to begin with
# ***********************************************************************


#######################
##### Extract Session Links
########################

# ****** Legislature Archives divided by YEAR but bill numbers are for two-year legislative sessions ********
# See, eg, https://www.capitol.hawaii.gov/session1999/status/HB100_his_.htm and https://www.capitol.hawaii.gov/session2000/status/HB100_his_.htm

## GET SESSION YEARS
max_year = datetime.datetime.now()
max_year = max_year.year
sessions = [i for i in range(1999, max_year + 1, 2)]
sessions = [str(i) + "_" + str(i + 1) for i in sessions]

## DROPPING PREVIOUSY SCRAPED TERMS
sessions = [sy for sy in sessions if 'HI_Bill_Details_{}.csv'.format(sy) not in os.listdir('.')]


#######################################################
########## GET BILL URLS + Special Sessions 2008+
#####################################################
# 2007-2008 Session Bills should overlap but some bills don't show up with 2008 URL so doing it both ways..

# session = sessions[0]

def get_session_bills(session): 

    session_years = [int(i) for i in session.split("_") if int(i) <= max_year]
    
    sy_bills = []
    all_bill_nums = []
    
    #### Loop Through Both Years to Catch Any Bills that Aren't Listed on Both Pages (all are, it seems...)
    for year in session_years:
        
        #### Get Bill Numbers for Regular Session
        reg_session = urllib.request.urlopen('https://www.capitol.hawaii.gov/session{}/bills/'.format(year))
        time.sleep(.5)
        reg_soup = BeautifulSoup(reg_session, 'lxml')
        bill_tags = reg_soup.findAll('a', text = re.compile('pdf|PDF|Pdf|htm|HTM|Htm'))

        if year >= 2013:
            url_base = 'https://www.capitol.hawaii.gov/Archives/measure_indiv_Archives.aspx?billtype={}&billnumber={}&year={}'
        elif year >= 2008:
            url_base = 'https://www.capitol.hawaii.gov/Archives/measure_indiv_Archives8-12.aspx?billtype={}&billnumber={}&year={}'
        elif year == 2007: 
            url_base = 'https://www.capitol.hawaii.gov/session{}/status/{}.htm'
        elif year >= 2001:
            url_base = 'https://www.capitol.hawaii.gov/session{}/status/{}.asp'
        else:
            url_base = 'https://www.capitol.hawaii.gov/session{}/status/{}_his_.htm'
        
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
        
        #### Check for Special Sessions + LOOP THROUGH THEM
        if year >= 2011:
            archive_page = urllib.request.urlopen('https://www.capitol.hawaii.gov/archives/{}.aspx'.format(year))
            time.sleep(.5)
            archive_soup = BeautifulSoup(archive_page, 'lxml')
    
            specials = archive_soup.findAll('a', text = re.compile('Special Session'))
            for ss in specials:
                ss_year = re.sub('^.+year=', '', ss['href'])
                ss_page = urllib.request.urlopen('https://www.capitol.hawaii.gov' + ss['href'].replace('..', ''))
                time.sleep(.5)
                ss_soup = BeautifulSoup(ss_page, 'lxml')
                ss_bills = ss_soup.find('div', id = 'ContentPlaceHolderCol1_PanelSS')
                ss_bills = ss_bills.findAll('a', href = re.compile('measure_indivSS'))
                for b in ss_bills:
                    this_bill = re.split('_', b.get_text(), 1)[0]
                    if this_bill[0:2] not in ['DC', 'GM', 'JC']:
                        stem = re.sub('\d+', '', this_bill)
                        nums = re.sub('[A-Z]+', '', this_bill)
                        this_url = 'https://www.capitol.hawaii.gov/measure_indivSS.aspx?billtype={}&billnumber={}&year={}'.format(stem, nums, ss_year)
                        sy_bills.append([session, this_bill, this_url])
                
    return(sy_bills)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_row = session_bills[6]
# session = '1999-2000'  
# bill_url = 'https://www.capitol.hawaii.gov/session2007/status/HB1073.htm'
# bill_url = 'https://www.capitol.hawaii.gov/session1999/status/HB10_his_.htm'
    
def get_bill_data(session, bill_row):
    
    ### Base Bill Details
    bill_num = bill_row[1]
    bill_url = bill_row[2]
    session_type = 'Regular'
    if 'measure_indivSS' in bill_url:
        session_type = 'Special ({})'.format(re.sub("^.+year=", "", bill_url))
  
    ### Get HTML
    try:
        bill_page = urllib.request.urlopen(bill_url, timeout = 10)
    except urllib.error.HTTPError as e:
        return(['HTTP Error'])
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(10)
            bill_page = urllib.request.urlopen(bill_url, timeout = 20)
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            bill_page = urllib.request.urlopen(bill_url, timeout = 60)

    ### Check if redirected to error page
    if bill_page.url != bill_url:
        if bill_page.url in ['https://www.capitol.hawaii.gov/home.aspx', 'https://www.capitol.hawaii.gov/errorpage.aspx']:
            return(['Bill page does not exist'])
        
    ### Get HTML
    bill_soup = BeautifulSoup(bill_page, 'lxml')
    
    ################
    ### GET BILL DATA --- PARSE DIFFERENTLY DEPENDING ON YEAR
    ################
    bill_actions = []
    
    ### 2008 to Present
    if int(session.split('_')[0]) >= 2008:

        ### Base Details
        title = bill_soup.find('span', id = 'ContentPlaceHolderCol1_ListView1_measure_titleLabel_0').get_text().strip()
        report_title = bill_soup.find('span', id = 'ContentPlaceHolderCol1_ListView1_report_titleLabel_0').get_text().strip()
        summary = bill_soup.find('span', id = 'ContentPlaceHolderCol1_ListView1_descriptionLabel_0').get_text().strip()
        companion = bill_soup.find('span', id = 'ContentPlaceHolderCol1_ListView1_companionLabel_0').get_text().strip()
        package = bill_soup.find('span', id = 'ContentPlaceHolderCol1_ListView1_package_acroLabel_0').get_text().strip()
        #current_referral = bill_soup.find('span', id = 'ContentPlaceHolderCol1_ListView1_current_referralLabel_0').get_text().strip()
        introducers = bill_soup.find('span', id = 'ContentPlaceHolderCol1_ListView1_introducerLabel_0').get_text().strip()    
        introducers = introducers.replace(', ', '; ')
    
        ###  Bill History
        hist_table = bill_soup.find('table', id = 'ContentPlaceHolderCol1_GridViewStatus')
        table_rows = hist_table.findAll('tr')
        
        order = 1
        for row in table_rows[1:]:
            cells = row.findAll('td')
            chamber = cells[1].get_text().strip()
            action = cells[2].get_text().strip()
            date = cells[0].get_text().strip()
            date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            bill_actions.append([bill_num, session, session_type, chamber, date, action, order])
            order += 1
    
    ### 2001 - 2007      
    elif int(session.split('_')[0]) >= 2001:
        
        ### Base Details
        title = bill_soup.find(text = re.compile('^Measure Title:')).findNext().get_text().strip()
        report_title = bill_soup.find(text = re.compile('^Report Title:')).findNext().get_text().strip()
        summary = bill_soup.find(text = re.compile('^Description:')).findNext().get_text().strip()
        companion = bill_soup.find(text = re.compile('^Companion:')).findNext().get_text().strip()
        package = bill_soup.find(text = re.compile('^Package:')).findNext().get_text().strip()
        #current_referral = bill_soup.find('span', id = 'ContentPlaceHolderCol1_ListView1_current_referralLabel_0').get_text().strip()
        introducers = bill_soup.find(text = re.compile('^Introducer')).findNext().get_text().strip()
        introducers = introducers.replace(', ', '; ')
    
        ###  Bill History
        hist_table = bill_soup.find('th', text = re.compile("Status Text"))
        hist_table = hist_table.findParents('table')[0]
        table_rows = hist_table.findAll('tr')
        
        order = 1
        for row in table_rows[1:]:
            cells = row.findAll('td')
            chamber = cells[1].get_text().strip()
            action = cells[2].get_text().strip()
            date = cells[0].get_text().strip()
            if len(date.split('/')[2]) == 4:
                date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            elif len(date) > 10: ### See, eg, https://www.capitol.hawaii.gov/session2006/status/hb1021.asp
                date = date.split(' ')[0]
                date = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            else:
                date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')    
            bill_actions.append([bill_num, session, session_type, chamber, date, action, order])
            order += 1
    
    ## For the 1999-2000 Term
    else:
        hist_split_str = "\r\n\n\n\n\r\n "
        
        ### Page Text
        text_rows = re.split("\r\n\n\r\n|\r\n\n\n\n\r\n |\n\n\r\n", bill_soup.get_text())

        ### Adjusting for Odd Results where 2nd Half of Page Doesn't Show up
        ### * Eg: https://www.capitol.hawaii.gov/session1999/status/HB156_his_.htm
        if 'Description:' not in bill_soup.get_text():
            bill_page = urllib.request.urlopen(bill_url, timeout = 15)
            bill_soup = BeautifulSoup(bill_page, 'html5lib')
            text_rows = re.split("\n", bill_soup.get_text())
            text_rows = [i for i in text_rows if i != '']
            hist_split_str = "\n\n\n\n"
            
        ### Bill Details
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
        
        #### History 
        # * --- Note: the triple spaces are important in second re.split
        # * --- Need the [H|S] otherwise will pickup random dates within actions (e.g. heard on xyz at time z)
        #hist_text = re.split("\r\n\n\n\n\r\n ", bill_soup.get_text())[1]
        
        if bill_num == "SB1509": # Character errors --> parsing issues
            summary = re.sub("\s\s\s+.+$", "", summary )
            hist_text = '1-27-99        S Introduced and passed First Reading  1-28-99        S Referred to 1. ECD 2. WAM '
        #elif bill_num in ['SB2091', 'SB2098', 'SB2103', 'SB2159', 'SB2210', 'SB2532', 'SB2537', 'SB2538', 'SB2539', 'SB2540', 'SB2543', 'SB2546', 'SB2553', 'SB2555', 'SB2556', 'SB2557']: 
        else:
            hist_text = re.split(hist_split_str, bill_soup.get_text())
            if len(hist_text) == 1:
                hist_text = re.split('\r\n\n\n\n\r', bill_soup.get_text())
            hist_text = hist_text[1].strip().replace("19-  - 1 ", "1-19- ")
            hist_text = hist_text.replace('- 1-19', '1-19- ') #  - 1-19
            hist_text = hist_text.replace('21-  - 1 ', '1-21- ')
            hist_text = hist_text.replace('- 1-20 ', '1-20- ')
            hist_text = re.sub('- 1-24', '1-24- ', hist_text) #  - 1-24| - 1-24|
        #else:
        #    hist_text = re.split(hist_split_str, bill_soup.get_text())[1]
           
        ## Fixing missing years -- e.g., https://www.capitol.hawaii.gov/session2000/status/HB1774_his_.htm
        #if bill_num in ['HB1774', 'HB1793', 'HB1888', 'HB1892'] and session == "1999_2000":
        if re.match('\d-\d\d- ', hist_text) is not None and session == "1999_2000":
            yr_nums = re.sub(".+session|/status.+$", "", bill_url)
            hist_text = re.sub('(?<=1-\d\d-) ', yr_nums[2:4], hist_text)
        
        if bill_num == "HB1931" and session == "1999_2000":  # Fixing https://www.capitol.hawaii.gov/session2000/status/HB1931_his_.htm
            hist_text = re.sub('01-2 H', 'H', hist_text)
            
        hist_text = re.split('(\d-\d-\d\d   +[H|S]) |(\d- \d-\d\d   +[H|S]) |(\d-\d\d-\d\d   +[H|S]) |(\d\d-\d-\d\d   +[H|S]) |(\d\d- \d-\d\d   +[H|S]) |(\d\d-\d\d-\d\d   +[H|S]) ', hist_text)
        hist_text = [re.sub('\r\n|\n', ' ', i).strip() for i in hist_text if i is not None]
        hist_text = [i for i in hist_text if i != '']
        
        if hist_text[0] == '-':
            hist_text = hist_text[1:] ## Adapting for https://www.capitol.hawaii.gov/session1999/status/SB10_his_.htm
        
        dates = [i[0:len(i)-1].strip() for index, i in enumerate(hist_text) if index % 2 == 0]
        chambers = [i[-1] for index, i in enumerate(hist_text) if index % 2 == 0]
        actions = [re.sub('\s+', ' ', i) for index, i in enumerate(hist_text) if index % 2 == 1]
        #chamber_actions = [i.strip() for index, i in enumerate(hist_text) if index % 2 == 1]
        #chambers = [i[0:1] for i in chamber_actions]
        #actions = [i[1:].strip() for i in chamber_actions]
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
        
    #### Save to Export
    bill_details = [bill_num, session, session_type, title, summary, report_title, companion, package, introducers, bill_url]
    return([bill_details, bill_actions])


########################################################
############## SCRAPE SESSION(S)
############################################
# bill_row = session_bills[3]

for session in sessions:
    
    #### Output Lists 
    session_bill_details = [['bill_number', 'session', 'session_type', 'title',  'summary', 'report_title', 'companion_bills', 'package', 'introducers', 'bill_url']]    
    session_actions = [['bill_number', 'session', 'session_type', 'chamber', 'action_date', 'action', 'order']]
    
    print("\n ----- Now Scraping: Session " + session.replace("_", "-") + " ---------\n")    
    
    ### Get all bills for a specific session
    session_bills = get_session_bills(session)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_row in session_bills:
        
        bill_data = get_bill_data(session, bill_row)
        time.sleep(1)
        
        if bill_data[0] == 'HTTP Error':
            print("\n\n ******** SKIPPING {} --- HTTP ERROR ******** \n ---> URL: {} \n\n".format(bill_row[1], bill_row[2]))
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