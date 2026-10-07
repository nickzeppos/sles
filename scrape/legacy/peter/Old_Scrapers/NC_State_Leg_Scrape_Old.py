# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape North Carolina Bills

@author: PB
"""


import csv
import os
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import numpy as np

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/NC/')

########################
##### SCRAPE BILL DETAILS
########################
### ** Run This to Get Individual Bill Links
### ** In future should adapt to just get most recent/incomplete terms


#### Get URLS to Term Specific Bill Lists
#html = urlopen("https://www.ncleg.net/gascripts/applications/billswithaction/?biennium=2017").read()
#terms_soup = BeautifulSoup(html, "lxml")   
#select_options = terms_soup.select('select[name=ctl00$ContentPlaceHolder1$DropDownList1] > option')
#
#session_labels = [i.get_text() for i in select_options]
#session_values = [i["value"] for i in select_options]
#
#del(html, terms_soup, select_options)
#
#### Function to Scrape a Page
#def scrape_bill_list(s_label, s_value):
#    this_data = []
#    this_html = urlopen("https://www.ncleg.net/gascripts/applications/billswithaction/?biennium=" + s_value).read()
#    this_soup = BeautifulSoup(this_html, "lxml")   
#    table = this_soup.find('table', attrs={'id':'tblBills'})
#    rows = table.find_all('tr', class_ = "tabrow")
#    
#    for row in rows:
#        cols = row.find_all('td')
#        bill_url = cols[0].find("a")["href"]
#        cols = [ele.text.strip() for ele in cols]
#        this_data.append([s_label] + [cols[0]] + cols[3:7] + [bill_url]) 
#
#    return this_data
#
#### List To Store Existing Data
#all_data = [['session', 'bill_id', 'short_title', 'recent_action_date', 'recent_actor', 'recent_action', 'bill_url']]
#
###### Loop Through All Lists and Store
#for label, value in zip(session_labels, session_values):
#    term_data = scrape_bill_list(label, value)  
#    all_data.extend(term_data)
#    print(label)
#
##### Save Large List
#with open("NC_Bill_Details.csv", "w", newline = "") as f:
#    writer = csv.writer(f)
#    writer.writerows(all_data)

# Read Data
with open("NC_Bill_Details.csv") as f:
    reader = csv.reader(f)
    all_data = [r for r in reader]

len(all_data)

########################
##### SCRAPE HISTORIES
########################

#### Output Lists
all_histories = [['session', 'bill_id', 'date', 'chamber', 'action', 'vote_results']]
all_data[0] = ['session', 'bill_id', 'short_title', 'recent_action_date', 'recent_actor', 'recent_action', 'bill_url',
              'primary_sponsors', 'cosponsors', 'attributes', 'counties', 'statutes', 'keywords']

skipped = []

##### Scrape Bill Histories and Details
for i in range(1, len(all_data)):
    
    try:
        bill_html = urlopen(all_data[i][6]).read()
        bill_soup = BeautifulSoup(bill_html, "lxml")    
    except:
        skipped.append(i)
        all_data[i] = all_data[i] + ['','','','','','']
        continue
    
    ### Update Bill Details
    bill_divs = bill_soup.find_all("div", class_ = "col-4 col-sm-3 col-xl-2 text-right pad-row misc-info-label")    
    div_labels = np.array([j.get_text() for j in bill_divs])

    sponsors = bill_soup.find('div', text = re.compile('Sponsors:')).find_next_sibling("div")
    sponsors = sponsors.findChildren("div", recursive = False)
    primary_sponsors = [j.get_text() for j in sponsors[0].findAll("a")]  
    if(len(sponsors) == 2):
        cosponsors = [j.get_text() for j in sponsors[1].findAll("a")]   
    else:
        cosponsors = ''
    
    attributes = bill_soup.find('div', text = re.compile('Attributes:')).find_next_sibling("div").get_text()
    counties = bill_soup.find('div', text = re.compile('Counties:')).find_next_sibling("div").get_text()
    statutes = bill_soup.find('div', text = re.compile('Statutes:')).find_next_sibling("div").get_text()
    keywords = bill_soup.find('div', text = re.compile('Keywords:')).find_next_sibling("div").get_text().lower()
    
    all_data[i] = all_data[i] + [primary_sponsors, cosponsors, attributes, counties, statutes, keywords]

    #### NOTE: If want to scrape votes:
    #### --- Need to adjuste len(history) - 1 below and instead isolate History and Vote Sections
    #### --- Not immediately clear how to do it becaue headers are buried in text

    ### Scrape Histories
    history = bill_soup.findAll("div", class_ = "card-body")
    history = history[len(history)-1].findChildren("div", attrs = {"class":"row"})
    this_history = []
    for j in range(0, len(history)):
        date = history[j].find('div', text = re.compile('Date:')).find_next_sibling("div").get_text()
        chamber = history[j].find('div', text = re.compile('Chamber:')).find_next_sibling("div").get_text()
        action = history[j].find('div', text = re.compile('Action:')).find_next_sibling("div").get_text()
        votes = history[j].find('div', text = re.compile('Votes:')).find_next_sibling("div").get_text().strip('\n')
        this_history.append(all_data[i][:2] + [date, chamber, action, votes])

    all_histories.extend(this_history)

    print(i)
    
print("Done! --- Remember to Save!!")

##############
### SAVE EACH DF
##############
# * Only Skipped 12 bills
    
#with open("NC_Bill_Details_Full.csv", "w", newline = "") as f:
#    writer = csv.writer(f)
#    writer.writerows(all_data)
#
#with open("NC_Bill_Histories.csv", "w", newline = "") as f:
#    writer = csv.writer(f)
#    writer.writerows(all_histories)





