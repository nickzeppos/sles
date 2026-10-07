# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape California Bills

@author: PB
"""

################## NOTES:
# Can pull data from FTP Page ---- ftp://leginfo.ca.gov/pub
#################
# * There is also a sql database of sorts (inside the bill folder in above ftp and linked via new legislature page)
# * Only way to get the data from 1993-1999 is via the FTP page... but long term it looks like that won't be updated

import csv
import os
# import requests # Requests doesn't support FTP urls
# from requests.packages.urllib3.util.retry import Retry
# from requests.adapters import HTTPAdapter
# from bs4 import BeautifulSoup
import re
import time
from ftplib import FTP
import pandas as pd
import urllib
from bs4 import BeautifulSoup
import socket
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

##############################
#### FUNCTIONS TO GATHER BILL LINKS
#############################

#def get_ftp_subdirs(base_ftp, session_ftp, chamber):
#    ftp = FTP(base_ftp)
#    ftp.login()
#    if chamber == 'H':
#        ftp.cwd(session_ftp + 'asm')
#    elif chamber == 'S':
#        ftp.cwd(session_ftp + 'sen')
#    folders = ftp.nlst()
#    ftp.close()
#    return(folders)
#
#def get_ftp_bills(base_ftp, session_ftp, chamber, folder):
#    ftp = FTP(base_ftp)
#    ftp.login()
#    time.sleep(2.5)
#    if chamber == 'H':
#        chamber_session_ftp = session_ftp + 'asm'
#    elif chamber == 'S':
#        chamber_session_ftp = session_ftp + 'sen'
#    ftp.cwd(chamber_session_ftp + '/' + folder + '/')
#    try:
#        bill_info_links = [x for x in ftp.nlst() if '.pdf' not in x]
#    except:
#        print('ERROR - {}'.format(folder))
#        bill_info_links = []
#    ftp.close()
#    return(bill_info_links)
#
#def clean_bill_links(session, chamber, folder, bill_link_list):
#    if bill_link_list == []:
#        return(bill_link_list)
#    else:
#        bill_link_sub = [x for x in  bill_link_list if 'introduced' in x or 'status' in x or 'history' in x]
#        bill_id = [x.split('_bill')[0] for x in bill_link_sub]
#        link_type = [re.sub('.html', '', x.split('_')[-1]) for x in bill_link_sub]
#        link_type = [re.sub('introduced', 'intro_text', x) for x in link_type]
#        if chamber == 'H':
#            session_stem = 'ftp://leginfo.ca.gov/pub/{}/bill/asm/{}/{}'
#        elif chamber == 'S':
#            session_stem = 'ftp://leginfo.ca.gov/pub/{}/bill/sen/{}/{}'
#        full_url = [session_stem.format(session, folder, x) for x in bill_link_sub]
#
#        cleaned_links = []
#        for a, b, c in zip(bill_id, link_type, full_url):
#            cleaned_links.append([a, session, chamber, b, c])
#
#        return(cleaned_links)
#
#################
##### GATHER URLS TO BILL DATA
###################
#
#sessions = ['93-94', '95-96', '97-98', '99-00', '01-02', '03-04', '05-06', '07-08', '09-10', '11-12', '13-14', '15-16']
#all_bill_urls = []
#
#for s in sessions:
#    s_ftp = 'pub/{}/bill/'.format(s)
#
#    ### Get Subdirectories -- Groups of 50ish bills
#    H_subdirs = get_ftp_subdirs(base_ftp = 'leginfo.ca.gov', session_ftp = s_ftp, chamber = 'H')
#    time.sleep(1)
#    S_subdirs = get_ftp_subdirs(base_ftp = 'leginfo.ca.gov', session_ftp = s_ftp, chamber = 'S')
#    time.sleep(1)
#
#    ### Get And Clean Bill HOUSE Links
#    for H_sd in H_subdirs:
#        try:
#            these_links = get_ftp_bills(base_ftp = 'leginfo.ca.gov', session_ftp = s_ftp, chamber = 'H', folder = H_sd)
#        except:
#            time.sleep(60)
#            these_links = get_ftp_bills(base_ftp = 'leginfo.ca.gov', session_ftp = s_ftp, chamber = 'H', folder = H_sd)
#        cleaned_urls = clean_bill_links(session = s, chamber = 'H', folder = H_sd, bill_link_list = these_links)
#        if cleaned_urls != []:
#            for row in cleaned_urls:
#                all_bill_urls.append(row)
#            print(H_sd)
#        else:
#            print(H_sd + " -- SERVER FOLDER EMPTY")
#
#    ### SENATE
#    for S_sd in S_subdirs:
#        try:
#            these_links = get_ftp_bills(base_ftp = 'leginfo.ca.gov', session_ftp = s_ftp, chamber = 'S', folder = S_sd)
#        except:
#            time.sleep(60)
#            these_links = get_ftp_bills(base_ftp = 'leginfo.ca.gov', session_ftp = s_ftp, chamber = 'S', folder = S_sd)
#        cleaned_urls = clean_bill_links(session = s, chamber = 'S', folder = S_sd, bill_link_list = these_links)
#        if cleaned_urls != []:
#            for row in cleaned_urls:
#                all_bill_urls.append(row)
#            print(S_sd)
#        else:
#            print(S_sd + " -- SERVER FOLDER EMPTY")
#
#    print("----- {} SESSION DONE----- ".format(s))
#
#############################################
########## SAVE
###############################################
#
#with open("CA_Bill_Data_urls.csv", "w", newline = "") as f:
#    writer = csv.writer(f)
#    writer.writerows(all_bill_urls)
#



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
    time.sleep(.5)

    ### Return Soup
    page_soup = BeautifulSoup(page, parser)
    return(page_soup)



######################################################################################
######################################################################################
####### FUNCTIONS TO EXTRACT BILL DATA FROM FTP LINKS
######################################################################################
######################################################################################

#this_ftp_url = status_url.values[0]

#### FTP Pages are just text -- Function cleans them by collapsing lines, removing tabs.
def scrape_and_clean_ftp_text(this_ftp_url):

    ## Get Page
    #try:
    #    this_page = urllib.request.urlopen(this_ftp_url, timeout = 5)
    #except:
    #    print("** URL Request Failed -- RETRY! **")
    #    this_page = urllib.request.urlopen(this_ftp_url, timeout = 5)
    #this_soup = BeautifulSoup(this_page, 'lxml')
    #time.sleep(.25)

    this_soup = get_page_soup(this_ftp_url, "lxml")

    ## Clean Text - Get rid of Colon tabs and Blank Lines
    ## ** Need to get rid of blank lines right away or occassionally creates errors (eg, 95-96 Bill SB954)
    lines = this_soup.text.replace('\t:', ':').split('\n')
    lines = [line.strip('\r') for line in lines if line not in '']

    ### If a line starts with a tab ---> Collapse to previous row
    i = len(lines) - 1
    while i >= 0:
        this_line = lines[i]
        if this_line[0:1] == '\t':
            lines[i-1] = lines[i-1] + lines[i].replace("\t", " ")
            lines[i] = ''
        # print(i)
        i = i - 1

    ## Multi-spaces and eliminating newly created blank lines
    lines = [re.sub('\s\s+', ' ', line.replace('\t', ' ')) for line in lines]
    lines = [line for line in lines if line not in '']
    return(lines)

#########################

def get_bill_history(hist_text, bill_id, term, chamber):

    author = [item for item in hist_text if item.startswith("AUTHOR")][0].replace("AUTHOR: ", "")

    ## Subset to List of Actions + Get Year Rows
    hist_sub = hist_text[(hist_text.index("BILL HISTORY") + 1):]
    year_re = re.compile("^\d\d\d\d$")
    all_years = list(filter(year_re.match, hist_sub))
    year_indexes = [hist_sub.index(i) for i in all_years]

    ### Fix Specific Year.. 07-08 - AB 363 - Years Wrong
    if bill_id == 'ab_363' and term == '07-08':
        del hist_sub[14]
        del hist_sub[12]

    ### Loop Through Sets of Actions by Year
    all_actions = []
    order = len(hist_sub) - 2
    for i in range(0, len(all_years)):
        this_year = all_years[i]
        this_index = year_indexes[i]
        if i + 1 == len(all_years):
            year_sub = hist_sub[(this_index + 1):]
        else:
            year_sub = hist_sub[(this_index+1):year_indexes[i+1]]
        for row in year_sub:
            month = row.split(' ')[0]
            day = row.split(' ')[1]
            action = row.split(day + ' ')[1]
            date = month + " " + day + ", " + this_year
            all_actions.append([bill_id, term, chamber, author, date, order, action])
            order -= 1
    return(all_actions)

######################################################################################
####### EXTRACT BILL DATA FROM FTP LINKS
######################################################################################

### TERMS
terms = ['93-94', '95-96', '97-98', '99-00', '01-02', '03-04', '05-06',
         '07-08', '09-10', '11-12', '13-14', '15-16', '17-18', '19-20', '21-22']

### DROPPING PREVIOUSY SCRAPED TERMS
previously_scraped = []
dirFiles = [file for file in os.listdir('.') if file not in 'CA_Bill_Data_urls.csv']

for i in range(0, len(terms)):
    term_adj = re.sub("-", "_", terms[i])
    if 'CA_Bill_Details_' + term_adj + '.csv' in dirFiles:
        previously_scraped.append(i)
        print("Previously Scraped: " + terms[i])

for index in sorted(previously_scraped, reverse=True):
    del terms[index]

############
### Load Saved Data with Links to Bill Information
bill_info = pd.read_csv('CA_Bill_Data_urls.csv', names = ["bill_id", "term", "chamber", "link_type", "ftp_url"])



##################
### SCRAPE BY TERM
#################
# term = terms[0]
# bill_id = unique_bill_ids[0]

for term in terms:

    print("\n ------------------ STARTING TERM: {} ------------------ \n".format(term))

    ### Subset to Term And Get Bill IDs
    term_bill_df = bill_info.loc[bill_info['term'] == term]
    term_bill_df = term_bill_df.loc[term_bill_df['link_type'] != 'intro_text']
    unique_bill_ids = list(term_bill_df['bill_id'].drop_duplicates())

    ### Term Output DFs
    bill_status_data = [['bill_id', 'term', 'chamber', 'title', 'authors', 'bill_id_text', 'topic', 'status_url', 'hist_url']]
    bill_hist_data = [['bill_id', 'term', 'chamber', 'author', 'date', 'order', 'action']]
    term_adj = term.replace("-", "_")

    ### Loop Through Bills
    num = 1
    total = len(unique_bill_ids)
    for bill_id in unique_bill_ids:
        bill_df = term_bill_df.loc[term_bill_df['bill_id'] == bill_id]
        chamber = bill_df.iloc[0].chamber

        status_url = bill_df[bill_df['link_type'] == 'status'].ftp_url
        if status_url.empty is False:
            status_url_adj = status_url.values[0].replace('ftp://', 'http://www.')
        else:
            status_url_adj = ''

        hist_url = bill_df[bill_df['link_type'] == 'history'].ftp_url
        if hist_url.empty is False:
            hist_url_adj = hist_url.values[0].replace('ftp://', 'http://www.')
        else:
            hist_url_adj = ''

        ##### BILL STATUS
        if status_url.empty == True:
            bill_status_data.append([bill_id, term, '', '', '', '', '', '', ''])
        else:
            try:
                status_lines = scrape_and_clean_ftp_text(status_url_adj)
            except:
                time.sleep(10)
                status_lines = scrape_and_clean_ftp_text(status_url_adj)

            title = [item for item in status_lines if item.startswith("TITLE")]
            authors = [item for item in status_lines if item.startswith("AUTHOR")]
            measure_text = [item for item in status_lines if item.startswith("MEASURE")]
            topic = [item for item in status_lines if item.startswith("TOPIC")]

            bill_status_data.append([bill_id, term, chamber, title[0], authors[0], measure_text[0], topic[0], status_url_adj, hist_url_adj])

        ##### BILL HISTORY
        if hist_url.empty == True:
            bill_hist_data.append([bill_id, term, '', '', '', ''])
        else:
            try:
                hist_lines = scrape_and_clean_ftp_text(hist_url_adj)
            except:
                time.sleep(10)
                hist_lines = scrape_and_clean_ftp_text(hist_url_adj)

            this_hist = get_bill_history(hist_lines, bill_id, term, chamber)
            for hist in this_hist:
                bill_hist_data.append(hist)

        print("({}/{}) Bill {} --- URL: {}".format(num, total, bill_id, status_url_adj))
        num += 1

    print("\n ------------------ FINSIHED TERM: {} ------------------ \n".format(term))

    ### SAVE!
    with open("CA_Bill_Details_" + term_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(bill_status_data)

    with open("CA_Bill_Histories_" + term_adj + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(bill_hist_data)
