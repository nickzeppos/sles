#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 13:47:12 2020


 ************** SCRIPT TO SCRAPE STATEMENT OF PURPOSE PDFS FROM IDAHO STATE LEGISLATURE BILL PAGES (2009+) AND PARSE ****************

@author: pb
"""


#import csv
import os
import urllib
from urllib.request import urlretrieve
#import PyPDF2
from tika import parser
#import glob
import datetime
import re
import time
import pandas as pd
from pathlib import Path

current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#######################
##### Identify Sessions to Parse
########################

### All Years 2009 to Current Year - 1
parse_years_regex = '|'.join([str(i) for i in range(2009, datetime.datetime.now().year - 1)])
parse_files = [i for i in os.listdir('.') if re.search(parse_years_regex, i) and re.search('Bill_Details', i)]

#### Create Directory For Text Documents if Needed
for file in parse_files:
    clean_dir = "SOP_documents/" + re.sub('.+Details_|.csv', '', file)
    if not os.path.exists(clean_dir):
        os.mkdir(clean_dir)

#### Drop Previously Parsed PDFs
# parse_files = [[i, re.sub('Bill_Details', 'Parsed_SOP_PDFs', i)] for i in parse_files ]
parse_files = [[i, re.sub('Bill_Details', 'Parsed_SOP_PDFs', i)] for i in parse_files if re.sub('Bill_Details', 'Parsed_SOP_PDFs', i) not in os.listdir('.')]


####################################################################################
######  Function to Download a Bill PDF, Convert To Text, Delete File, Return Text
###################################################################################
#### See: https://stackoverflow.com/questions/34837707/how-to-extract-text-from-a-pdf-file
#### ---> Can Use this to Batch Parse via Tika Parser... See Loop

##### Construct Filepath and Download PDF
def save_pdf(pdf_url, s, bill_num):
    pdf_path = 'SOP_documents/{}/ID_{}_{}.pdf'.format(s, s, bill_num)
    if not os.path.exists(pdf_path):
        try:
            urlretrieve(pdf_url, pdf_path)
            time.sleep(1)
        except urllib.error.HTTPError:
            return('HTTP Error')
    return(pdf_path)

#### Parse a PDF to Text
def read_pdf(pdf_path):
    pdf_text_dict = parser.from_file(pdf_path)
    return(pdf_text_dict)

####  Taking the Ouput from tika.parser and (1) Cleaning; (2) Eliminating All Text up to 'Contact'; (3) Removing Excess at End
# pdf_text = text['content']
def parse_pdf(pdf_text):
    txt_clean = pdf_text.lower().strip()
    txt_clean = re.sub('\xad', '-', txt_clean)
    ### Need to be careful about splitting to get start of contact info -- formats vary
    txt_split = re.split('\ncontact:|\nsponsor:|\nname:', txt_clean, 1)
    if len(txt_split) == 1:
        txt_split = re.split('contact:|sponsor:|name:', txt_clean, 1)
    if len(txt_split) == 1:
        txt_split = re.split('\ncontact|\nsponsor|\nname', txt_clean, 1)
    txt_clean = txt_split[1]
    txt_clean = re.sub('\xa0|revised\\s|revised$', ' ',  txt_clean).strip()
    txt_clean = re.sub('\\sstatement of purpose.+|\\sfiscal note.+|disclaimer.+', '', txt_clean).strip()
    txt_clean = re.sub("^statement of purpose|^fiscal note", '', txt_clean).strip() ### Occoasionally in front of contact info...
    txt_clean = re.sub("^[\\/\\-]", ' ', txt_clean).strip()
    txt_clean = re.sub('^contact[^\\s]+\\s', '', txt_clean)
    txt_clean = re.sub('^name:|^sponsor:', '', txt_clean)

    #### Return Key Vars
    requestor_SOP = re.sub("(phone:|office:|office of|bureau of|[a-z]+ commission|idaho |\\([0-9]+\\))(.|\s)+", "", txt_clean.strip())
    requestor_SOP = re.sub(",.+| and .+", "", requestor_SOP)
    requestor_SOP = re.sub('\n.+', ' ', requestor_SOP).strip()

    requestor_SOP_full = re.sub('\n|\r', ' ', txt_clean)
    requestor_SOP_full = re.sub('  +', ' ', requestor_SOP_full).strip()

    return([requestor_SOP, requestor_SOP_full])


########################################################
############## SCRAPE SESSION(S)
############################################
# read_file, write_file = parse_files[9]

for read_file, write_file in parse_files:

    #### Read In Data and Construct Session Variable
    s = re.sub('.+Bill_Details_|.csv', '', read_file)
    s_df = pd.read_csv(read_file)

    print("\n ------------------- Now Scraping SOP PDFs for the " + s + " Session ---------------------- \n")

    ### Subset Session Data to Relevant Variables
    s_df = s_df[['bill_number', 'session', 'requestor_SOP', 'requestor_SOP_full', 'SOP_url']]
    s_df['pdf_filepath'] = ''

    ### Save ALL the PDFS
    total = len(s_df)
    for index, row in s_df.iterrows():
        fp = save_pdf(pdf_url = row.SOP_url, s = s, bill_num = row.bill_number)
        if fp == "HTTP Error":
            s_df.at[index, 'pdf_filepath'] = 'Skip - HTTP Error'
        else:
            s_df.at[index, 'pdf_filepath'] = fp
        print(" ~~ ({}/{}) {}".format(index + 1, total, row.bill_number))

    #### Convert the PDFs to Text, Store in Relevant Pandas DF Cells
    print("\n ********* ALL PDFs Saved! Now Converting to Text and Parsing! ********* \n")

    #session_pdf_text = []
    total = len(s_df)
    for index, row in s_df.iterrows():
        if row.pdf_filepath == 'Skip - HTTP Error':
            print(" ~~ ({}/{}) {}  **** HTTP ERROR = NO PDF ***** {}".format(index + 1, total, row.bill_number, row.SOP_url))
            continue
        text = read_pdf(row.pdf_filepath)
        #session_pdf_text.append(text)
        sop_req = parse_pdf(text['content'])
        s_df.at[index, 'requestor_SOP'] = sop_req[0]
        s_df.at[index, 'requestor_SOP_full'] = sop_req[1]
        print(" ~~ ({}/{}) {} -- {}".format(index + 1, total, row.bill_number, sop_req[0]))

    #### Subset to Relevant Columns and SAVE
    s_df = s_df[['bill_number', 'session', 'requestor_SOP', 'requestor_SOP_full']]
    s_df.to_csv(write_file, sep=',', index=False)

    print("\n\n ------------------- SOP DATA SCRAPED AND SAVED ~  " + s + " Session ---------------------- \n\n")


print("  ********************************** ALL DONE ********************************** ")







#######################
### *** Below Doesn't Work Because the PDFs are Encrypted
###########
#def save_pdf(pdf_url, s, bill_num):
#
#    ##### Construct Filepath and Download PDF
#    save_path = 'SOP_documents/{}/ID_{}_{}.pdf'.format(s, s, bill_num)
#    urlretrieve(pdf_url, save_path)
#
#    with open(save_path, 'rb') as this_pdf:
#        pdf_reader = PyPDF2.PdfFileReader(this_pdf)
#        pdf_reader.isEncrypted
#        pdf_reader.decrypt('password')
#        first_page = pdf_reader.getPage(0)
#        first_page_text = first_page.extractText()
#########################################
