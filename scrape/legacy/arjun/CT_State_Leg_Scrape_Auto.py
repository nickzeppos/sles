# -*- coding: utf-8 -*-
"""
Created on Fri Nov 16 15:19:49 2018

SCRIPT TO SCRAPE BILLS FROM CONNECTICUT GENERAL ASSEMBLY

@author: PB
"""

import csv
import os
import re
import time
from ftplib import FTP
import urllib
from bs4 import BeautifulSoup
import datetime
from pathlib import Path
import urllib.request
import requests
import PyPDF2
from io import BytesIO
from urllib3.exceptions import InsecureRequestWarning
import warnings

warnings.simplefilter('ignore', InsecureRequestWarning)
# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

######################################################
#### FUNCTION TO GATHER STATUS AND VOTE URLS FOR A SESION
#####################################################
# ** Note: Could also get committee votes via ts directory or financial notes via fn
# ** Currently returns the status (history/sponsor link and vote links) in seperate lists (bc diff lengths)

def get_ftp_urls(base_ftp, sy):
    ftp = FTP(base_ftp)
    ftp.login()
    status_urls = []
    vote_urls = []
    for chamber in ['h', 's']:
        ftp.cwd('/' + sy + '/cbs/{}/'.format(chamber))
        for status in [u for u in ftp.nlst() if 'htm' in u.lower()]:
            status_urls.append('ftp://ftp.cga.ct.gov/{}/cbs/{}/'.format(sy, chamber) + status)
        ftp.cwd('/' + sy + '/vote/{}/'.format(chamber))
        for vote in [u for u in ftp.nlst() if 'htm' in u.lower()]:
            vote_urls.append('ftp://ftp.cga.ct.gov/{}/vote/{}/'.format(sy, chamber) + vote)
    ftp.close()
    status_urls = [[x,y] for x,y in zip([re.sub('.+/|.htm', '', s) for s in status_urls], status_urls)]
    vb = [re.sub('.+-R00|-.+$', '', v) for v in vote_urls]
    vb = [b[0:2] + '-' + b[2:] for b in vb]
    vote_urls = [[x,y] for x,y in zip(vb, vote_urls)]
    return([status_urls, vote_urls])


#####################################################
########### FUNCTION To GET BILL HTML and CONVERT TO SOUP
#####################################################

def get_page_soup(this_url, parser = "lxml"):
    try:
        this_page = urllib.request.urlopen(this_url, timeout = 15)
    except:
        print("\n  ** URL Request Failed -- RETRY! **  \n")
        time.sleep(30)
        this_page = urllib.request.urlopen(this_url, timeout = 30)
    this_soup = BeautifulSoup(this_page, parser)
    time.sleep(.75)
    return(this_soup)


######################################################
#### FUNCTION TO CLEAN BILL STATUS/DETAIL PAGE
#####################################################
# status_row = bill_status
# status_row = all_bill_status_urls[152]
# status_row[1] = 'ftp://ftp.cga.ct.gov/2010/cbs/s/SB-0013.htm'

def clean_status_page(sy, status_row):
    if status_row[0] == 'SJ-0046' and sy == '2009':
        title = 'RESOLUTION PROPOSING AN AMENDMENT TO THE CONSTITUTION OF THE STATE CONCERNING THE PROCEDURES OF THE COURTS.'
        bill_details = [status_row[0], sy, 'Raised Bill', 'Judiciary Committee', title, '', '', '(JUD)', status_row[1], 'https://www.cga.ct.gov/2009/TOB/S/2009SJ-00046-R00-SB.htm']
        bill_hist = [[status_row[0], sy, '2009-02-19', 'REFERRED TO JOINT COMMITTEE ON Judiciary Committee', 1, status_row[1]],
                     [status_row[0], sy, '2009-03-20', 'PUBLIC HEARING 03/26', 2, status_row[1]]]
        return([bill_details, bill_hist])
    this_soup = get_page_soup(status_row[1])
    lines = this_soup.text.replace('\t:', ':').split('\n')
    lines = [line.strip('\r') for line in lines if line not in '']
    if len(lines) == 1 or "Introducer(s):" not in lines:
        if len(lines) > 1:
            lines = [l for l in lines if "Introducer(s):" in l]
        try:
            primary_sponsors = lines[0].split("Introducer(s):")[1].split("Title:")[0]
        except:
            primary_sponsors = ''
        title = lines[0].split("Title:")[1].split("Statement of Purpose:")[0].strip()
        sop = lines[0].split("Statement of Purpose:")[1].split("Bill History:")[0].strip()
        cosponsors = lines[0].split('Co-sponsor(s):')[1].replace("\xa0\xa0", "; ")
        cosponsors = cosponsors.replace("Dist.Sen.", "Dist.; Sen.").replace("Dist.Rep.", "Dist.; Rep.")
        hist_text = lines[0].split("Bill History:")[1].split("Co-sponsor(s)")[0]
        hist_text = re.split('(\d\d-\d\d-\d\d\d\d)', hist_text)[1:]
        bill_hist = []
        order = 1
        for date, action in zip(hist_text[::2], hist_text[1::2]):
            if re.search('[0-9]\\/[0-9]', date):
                date_clean = datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            else:
                date_clean = datetime.datetime.strptime(date, '%m-%d-%Y').strftime('%Y-%m-%d')
            bill_hist.append([status_row[0], sy, date_clean, action.strip(), order, status_row[1]])
            order += 1
    else:
        sponsor_start = lines.index("Introducer(s):")
        title_start = [i for i, l in enumerate(lines) if l.startswith("Title:") == True][0]
        sop_start= [i for i, l in enumerate(lines) if l.startswith("Statement of Purpose:") == True][0]
        hist_start = lines.index("Bill History:") # Colon important here..
        cospon_start = [i for i, l in enumerate(lines) if l.startswith("Co-sponsor") == True][0]
        primary_sponsors = '; '.join(lines[(sponsor_start + 1):title_start])
        title = ' '.join(lines[title_start:sop_start]).replace("Title: ", "")
        sop = ' '.join(lines[sop_start:hist_start]).replace("Statement of Purpose: ", "")
        cosponsors = '; '.join([l for l in lines[(cospon_start + 1):] if l.strip() != ''])
        bill_hist = []
        order = 1
        for h in lines[(hist_start+1):cospon_start]:
            date_clean = datetime.datetime.strptime(h.split(' ', 1)[0], '%m/%d/%y').strftime('%Y-%m-%d')
            action = h.split(' ', 1)[1]
            bill_hist.append([status_row[0], sy, date_clean, action, order, status_row[1]])
            order += 1
    bn = re.sub('.+/', '', status_row[1])
    bn = re.split('-', bn)
    bn[1] = re.sub('.HTM|.htm', '', bn[1]).zfill(5)
    url_stem = re.sub('[A-Z][A-Z]-[0-9]+.HTM|[A-Z][A-Z]-[0-9]+.htm', '', status_row[1])
    if re.search("^H", bn[0]): ## if HJ, still ends with HB
        end_stem = "HB"
    elif re.search("^S", bn[0]):
        end_stem = "SB"
    pb_pdf = [re.sub('/cbs/', '/TOB/', url_stem) + 'pdf/' + sy + bn[0] + '-' + bn[1] + '-R00-' + end_stem + ".pdf"]
    pb_pdf = re.sub('ftp://ftp.', 'https://www.', pb_pdf[0]) ### This isn't machine readable but will work via browser
    try:
        pb_soup = requests.get(pb_pdf, verify=False)
    except urllib.error.URLError:
        print("\n  ** PROPOSED BILL URL Request Failed -- CODING Introducer AS NULL **  \n")
        bill_type = ''
        introduced_by = ''
    else:
        pdf_content = BytesIO(pb_soup.content)
        all_text = ""
        with pdf_content as pdf_file:
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            for page_number in range(len(pdf_reader.pages)):
                text = pdf_reader.pages[page_number].extract_text()
                all_text += text
        if re.search(r'Proposed (Bill|House|Senate)', all_text):
            bill_type = 'Proposed Bill'
        elif re.search(r'Raised (Bill|House|Senate)', all_text):
            bill_type = 'Raised Bill'
        else:
            bill_type = 'Unknown'
        match = re.search(r'Introduced by:(.*?)\n\s*\n', all_text, re.DOTALL)
        try:
            introduced_by = match.group(1).strip()
            introduced_by = re.sub(r'\s+', ' ', introduced_by.replace('\n', '; ').replace(' ;',';')).strip()
        except:
            introduced_by = ''
    bill_details = [status_row[0], sy, bill_type, primary_sponsors, title, sop, cosponsors, introduced_by, status_row[1], pb_pdf]
    return([bill_details, bill_hist])

######################################################
#### FUNCTION TO CLEAN VOTE PAGE
#####################################################
# vote_row = vote_urls[100]

#def clean_vote_page(sy, vote_row):
#
#    try:
#        this_page = urllib.request.urlopen(vote_row[1], timeout = 5)
#    except:
#        print("** URL Request Failed -- RETRY! **")
#        this_page = urllib.request.urlopen(vote_row[1], timeout = 5)
#
#    this_soup = BeautifulSoup(this_page, 'lxml')
#    time.sleep(.25)
#
#    ## Could Write this Eventually if we want it...
#    ## Example pag: ftp://ftp.cga.ct.gov/2010/vote/h/2010HV-00101-R00HB05534-HV.htm


######################################################
#### Check Previously Scraped Sessions + SCRAPE DATA
#####################################################

### BASE VARIABLES
base_ftp = 'ftp.cga.ct.gov'
this_year = datetime.datetime.now().year
this_year = 2023
session_years = [str(i) for i in range(2005, this_year)]

## DROPPING PREVIOUSY SCRAPED TERMS
scrape_years = [sy for sy in session_years if 'CT_Bill_Details_{}.csv'.format(sy) not in os.listdir('.')]
print("Dropping Previously Scraped Sessions: " + ', '.join([y for y in session_years if y not in scrape_years]))

## Skipping Missing Year: 2006 - 2009
# ***********************
# scrape_years = [sy for sy in scrape_years if int(sy) not in range(2006, 2010)]
# **********************


### SCRAPE
# sy = scrape_years[0]
# bill_status = all_bill_status_urls[26]

for sy in scrape_years:

    print('\n ****** Now Scraping Data for the {} Sesssion *******'.format(sy))

    all_bill_urls = get_ftp_urls(base_ftp, sy)
    all_bill_status_urls = all_bill_urls[0]
    all_bill_vote_urls = all_bill_urls[1]

    ### Output Lists
    bill_details_data = [['bill_num', 'session', 'bill_type', 'primary_sponsors', 'title', 'purpose', 'cosponsors', 'introduced_by', 'bill_url', 'proposed_bill_pdf_url']]
    bill_hist_data = [['bill_num', 'session', 'action_date', 'action', 'order', 'bill_url']]

    ### Loop Through Bills Status Pages
    num = 1
    total = len(all_bill_status_urls)
    for bill_status in all_bill_status_urls:
        this_status = clean_status_page(sy, bill_status)
        bill_details_data.append(this_status[0])
        for this_hist in this_status[1]:
            bill_hist_data.append(this_hist)

        #print(this_status[0][7])
        print("({}/{}) {} -- {}".format(num, total, this_status[0][0], this_status[0][8]))
        num += 1

    ### VOTES -- If needed, write function to parse and add loop here
    # ---> Loop through all_bill_vote_urls

    print("\n ------------------ FINISHED SESSION: {} ------------------ \n".format(sy))

    ### SAVE!
    with open("CT_Bill_Details_" + sy + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(bill_details_data)

    with open("CT_Bill_Histories_" + sy + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(bill_hist_data)



print("\n ---------------- CONNECTICUT -- ALL SESSIONS COMPLETE -----------------\n".format(sy))
