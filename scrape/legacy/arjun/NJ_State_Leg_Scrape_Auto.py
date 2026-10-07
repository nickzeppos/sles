# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:56:57 2019

~~~~~~~ Scrape NJ Legislation 1996 - Present ~~~~~~~~~~~~

@author: PB
"""

#import csv
import os
import datetime
#from dateutil import parser as dateparser
import re
# from dbfread import DBF # for 2000
import pandas as pd
# from simpledbf import Dbf5
from pathlib import Path

# import requests
current_state = str(Path(__file__).name)
os.chdir(Path.cwd())
os.chdir('../States/'+current_state[:2])

#########################################
####### BEFORE RUNNING
#########################################

##### NOTES (AV):
# First you have to manually download the MS Access files from https://www.njleg.state.nj.us/legislative-downloads?downloadType=Bill_Tracking
# Next, you need to convert the files to CSVs. I did this using DBeaver, which you can download off the internet
# The two files you need are the MainBill table and the BillHist table. You can see how I formatted the names as CSVs
# This assumes you've downloaded those CSVs and continues from there

####################


print('\n\n ***************** \n ~~~> REMEMBER TO PRE-DOWNLOAD BULK FILES, SEE NOTES ABOVE \n ***************** \n\n')


##########################################
####### Session Info
##########################################

this_year = datetime.datetime.now().year
sessions = [i for i in range(1996, this_year, 2) if 'NJ_Bill_Details_' + str(i) + "_" + str(i+1) + '.csv' not in os.listdir('.')]
sessions = [str(i) + "_" + str(i+1) for i in sessions]


### Dropping Partial Data
# this_year = datetime.datetime.now().year
# sessions = [i for i in sessions if str(this_year) not in i.split('_')]

print("\n\n ~~~~ DROPPING SESSION THAT INCLUDES {} ~~~~ \n\n".format(this_year))


##########################################
####### Function to Clean + Reformat Bill Data
##########################################
# bdf = bill_df

def clean_bill_data(bdf, s):

    print("\n ~~~ Cleaning the Bill File for the {} Session ~~~~ \n".format(s) )

    ## Early File Colnames are ALl CAPS
    bdf.columns = [i.upper() for i in bdf.columns]
    bdf.columns = ['CURRENTSTATUS' if x == 'CURRENTSTA' else x for x in bdf.columns]
    bdf.columns = ['EFFECTIVEDATE' if x == 'EFFECTIVED' else x for x in bdf.columns]
    bdf.columns = ['SECONDPRIME' if x == 'SECONDPRIM' else x for x in bdf.columns]
    bdf.columns = ['IDENTICALBILLNUMBER' if x == 'IDENTICALB' else x for x in bdf.columns]

    ## Clean Bill Number
    bdf['BILLTYPE'] = [i.strip() for i in bdf['BILLTYPE']]
    bdf['bill_number'] = bdf['BILLTYPE'] + '-' + [i.zfill(4) for i in bdf['BILLNUMBER'].map(str)] #[i.zfill(4) for i in bdf.BillNumber]

    ### Adding Session
    bdf['session'] = s

    ## NaN's to ''
    bdf = bdf.fillna('')

    ## Change Columns to Match Existing Format
    bdf.columns = ['title' if x == 'ABSTRACT' else x for x in bdf.columns]
    bdf.columns = ['status' if x == 'CURRENTSTATUS' else x for x in bdf.columns]
    bdf.columns = ['summary' if x == 'SYNOPSIS' else x for x in bdf.columns]
    bdf.columns = ['intro_date' if x == 'INTRODATE' else x for x in bdf.columns]
    bdf.columns = ['effective_date' if x == 'EFFECTIVEDATE' else x for x in bdf.columns]
    bdf.columns = ['chapter_num' if x == 'CHAPTERLAW' else x for x in bdf.columns]

    ### Standardizing Dates
    if int(s.split('_')[0]) >= 2014:
        bdf['intro_date'] = [i.split(' ')[0] for i in bdf['intro_date']]
        bdf['intro_date'] = [datetime.datetime.strptime(i, '%Y-%m-%d').strftime('%Y-%m-%d') if i != '' else '' for i in bdf['intro_date']]

        bdf['effective_date'] = [i.split(' ')[0] for i in bdf['effective_date']]
        bdf['effective_date'] = [datetime.datetime.strptime(i, '%Y-%m-%d').strftime('%Y-%m-%d') if i != '' else '' for i in bdf['effective_date']]

    ### Companion Bills
    bdf.columns = ['companion' if x == 'IDENTICALBILLNUMBER' else x for x in bdf.columns]
    bdf['companion'] = [re.sub('\\(.+\\)|\\(.+\\}| [a-z].+', '', i) for i in bdf['companion']]

    ### Fixing Companion Bill Numbers, Adjusting for Multiple Companions, Formatting Sponsors
    bdf['primary_sponsors'] = ''
    for i in range(0, len(bdf)):
        ### Companion Bills
        these_bills = bdf['companion'].iloc[i]
        these_bills = these_bills.strip().split(' ')
        stems = [re.sub('[0-9]+', '', j) for j in these_bills]
        nums = [re.sub('[A-Z]+', '', j).zfill(4) for j in these_bills]
        these_bills = '; '.join([s + '-' + n if n != '0000' else '' for s,n in zip(stems, nums)])
        bdf.loc[i, 'companion'] = these_bills

        ### Fixing Sponsors
        sponsors = bdf[['FIRSTPRIME', 'SECONDPRIME', 'THIRDPRIME']].iloc[i]
        bdf.loc[i, 'primary_sponsors'] = '; '.join([j.strip() for j in list(sponsors) if j.strip() != ''])

    ### Subset to Relevant Colums
    bdf = bdf[['bill_number', 'session', 'title', 'status', 'primary_sponsors', 'intro_date', 'effective_date', 'chapter_num', 'companion', 'summary']]
    return(bdf)


########################################
#### Dictionary to Parse Actions
#########################################
# From OpenStates, with some additions for earlier years

actions = {
    'INT 1RA AWR 2RA': (
        'Introduced, 1st Reading without Reference, 2nd Reading',
        'introduction',
    ),
    'INT 1RS SWR 2RS': (
        'Introduced, 1st Reading without Reference, 2nd Reading',
        'introduction',
    ),
    'REP 2RA': ('Reported out of Assembly Committee, 2nd Reading', 'committee-passage'),
    'REP 2RS': ('Reported out of Senate Committee, 2nd Reading', 'committee-passage'),
    'REP/ACA 2RA': (
        'Reported out of Assembly Committee with Amendments, 2nd Reading',
        'committee-passage',
    ),
    'REP/SCA 2RS': (
        'Reported out of Senate Committee with Amendments, 2nd Reading',
        'committee-passage',
    ),
    'R/S SWR 2RS': ('Received in the Senate without Reference, 2nd Reading', None),
    'R/A AWR 2RA': ('Received in the Assembly without Reference, 2nd Reading', None),
    'R/A 2RAC': ('Received in the Assembly, 2nd Reading on Concurrence', None),
    'R/S 2RSC': ('Received in the Senate, 2nd Reading on Concurrence', None),
    'REP/ACS 2RA': ('Reported from Assembly Committee as a Substitute, 2nd Reading', None),
    'REP/SCS 2RS': ('Reported from Senate Committee as a Substitute, 2nd Reading', None),
    'AA 2RA': ('Assembly Floor Amendment Passed', 'amendment-passage'),
    'SA 2RS': ('Senate Amendment', 'amendment-passage'),
    'SUTC REVIEWED': ('Reviewed by the Sales Tax Review Commission', None),
    'PHBC REVIEWED': ('Reviewed by the Pension and Health Benefits Commission', None),
    'SUB FOR': ('Substituted for', None),
    'SUB BY': ('Substituted by', None),
    'PA PBH': ('Passed Assembly (Passed Both Houses)', 'passage'),
    'PS PBH': ('Passed Senate (Passed Both Houses)', 'passage'),
    'PA': ('Passed Assembly', 'passage'),
    'PS': ('Passed Senate', 'passage'),
    'PS FILE': ('Passed Senate and Filed', 'passage'),
    'PA FILE': ('Passed Assembly and Filed', 'passage'),
    'APP W/LIV': (
        'Approved with Line Item Veto',
        ['executive-signature', 'executive-veto-line-item'],
    ),
    'APP': ('Approved', 'executive-signature'),
    'AV R/A': ('Absolute Veto, Received in the Assembly', 'executive-veto'),
    'AV R/S': ('Absolute Veto, Received in the Senate', 'executive-veto'),
    'CV R/A': ('Conditional Veto, Received in the Assembly', 'executive-veto'),
    'CV R/A 1RAG': (
        'Conditional Veto, Received in the Assembly, 1st Reading/Governor Recommendation',
        'executive-veto',
    ),
    'CV R/A 2RAG':('Conditional Veto, Received in the Assembly, 2nd Reading/Governor Recommendation', 'executive-veto'),
    'CV R/S': ('Conditional Veto, Received in the Senate', 'executive-veto'),
    'PV': ('Pocket Veto - Bill not acted on by Governor-end of Session', 'executive-veto'),
    '2RSG': ("2nd Reading on Concur with Governor's Recommendations", None),
    'CV R/S 2RSG': (
        "Conditional Veto, Received, 2nd Reading on Concur with Governor's Recommendations",
        None,
    ),
    'CV R/S 1RSG': (
        "Conditional Veto, Received, 1st Reading on Concur with Governor's Recommendations",
        None,
    ),
    'R/S 2RSG': (
        "Received in the Senate, 2nd Reading - Concur. w/Gov's Recommendations",
        None,
    ),
    'R/A 2RAG': (
        "Received in the Assembly, 2nd Reading - Concur. w/Gov's Recommendations",
        None,
    ),
    '1RAG': ('First Reading/Governor Recommendations Only', None),
    '2RAG': ("2nd Reading in the Assembly on Concur. w/Gov's Recommendations", None),
    'R/A': ("Received in the Assembly", None),
    'REF SBA': (
        'Referred to Senate Budget and Appropriations Committee',
        'referral-committee',
    ),
    'RSND/V': ('Rescind Vote', None),
    'RSND/ACT OF': ('Rescind Action', None),
    'RCON/V': ('Reconsidered Vote', None),
    'CONCUR AA': ("Concurred by Assembly Amendments", None),
    'CONCUR SA': ('Concurred by Senate Amendments', None),
    'SS 2RS': ('Senate Substitution', None),
    'AS 2RA': ('Assembly Substitution', None),
    'ER': ('Emergency Resolution', None),
    'FSS': ('Filed with Secretary of State', None),
    'LSTA': ('Lost in the Assembly', None),
    'LSTS': ('Lost in the Senate', None),
    'SEN COPY ON DESK': ('Placed on Desk in Senate', None),
    'ASM COPY ON DESK': ('Placed on Desk in Assembly', None),
    'COMB/W': ('Combined with', None),
    'MOTION': ('Motion', None),
    'PUBLIC HEARING': ('Public Hearing Held', None),
    'PH ON DESK SEN': ('Public Hearing Placed on Desk Senate Transcript Placed on Desk', None),
    'PH ON DESK ASM': (
        'Public Hearing Placed on Desk Assembly Transcript Placed on Desk',
        None
    ),
    'W': ('Withdrawn from Consideration', 'withdrawal')
}

### THese actions need to be partially matched because they always include a committee name
comm_actions = {
    'INT 1RA REF': (
        'Introduced in the Assembly, Referred to',
        ['introduction', 'referral-committee'],
    ),
    'INT 1RS REF': (
        'Introduced in the Senate, Referred to',
        ['introduction', 'referral-committee'],
    ),
    'R/S REF': ('Received in the Senate, Referred to', 'referral-committee'),
    'R/A REF': ('Received in the Assembly, Referred to', 'referral-committee'),
    'TRANS': ('Transferred to', 'referral-committee'),
    'RCM': ('Recommitted to', 'referral-committee'),
    'REP/ACA REF': (
        'Reported out of Assembly Committee with Amendments and Referred to',
        'referral-committee',
    ),
    'REP/ACS REF': (
        'Reported out of Senate Committee with Amendments and Referred to',
        'referral-committee',
    ),
    'REP REF': ('Reported and Referred to', 'referral-committee'),
}

com_vote_motions = {
    'r w/o rec.': 'Reported without recommendation',
    'r w/o rec. ACS': (
        'Reported without recommendation out of Assembly committee as a substitute'
    ),
    'r w/o rec. SCS': (
        'Reported without recommendation out of Senate committee as a substitute'
    ),
    'r w/o rec. Sca': (
        'Reported without recommendation out of Senate committee with amendments'
    ),
    'r w/o rec. Aca': (
        'Reported without recommendation out of Assembly committee with amendments'
    ),
    'r/ACS': 'Reported out of Assembly committee as a substitute',
    'r/Aca': 'Reported out of Assembly committee with amendments',
    'r/SCS': 'Reported out of Senate committee as a substitute',
    'r/Sca': 'Reported out of Senate committee with amendments',
    'r/favorably': 'Reported favorably out of committee',
}

##########################################
####### Function to Clean + Reformat History Data
##########################################
# hdf = hist_df

def clean_hist_data(hdf, s):

    print("\n ~~~ Cleaning the Action File for the {} Session ~~~~ \n".format(s) )

    ## Early File Colnames are ALl CAPS
    hdf.columns = [i.upper() for i in hdf.columns]

    ## Clean Bill Number
    hdf['BILLTYPE'] = [i.strip() for i in hdf['BILLTYPE']]
    hdf['bill_number'] = hdf['BILLTYPE'] + '-' + [i.zfill(4) for i in hdf['BILLNUMBER'].map(str)] #[i.zfill(4) for i in bdf.BillNumber]

    ### Adding Session
    hdf['session'] = s

    ## NaN's to ''
    hdf = hdf.fillna('')

    ## Change Columns to Match Existing Format
    hdf.columns = ['chamber' if x == 'HOUSE' else x for x in hdf.columns]
    hdf.columns = ['action' if x == 'ACTION' else x for x in hdf.columns]
    hdf.columns = ['order' if x == 'SEQUENCE' else x for x in hdf.columns]
    hdf.columns = ['action_date' if x == 'DATEACTION' else x for x in hdf.columns]

    if int(s.split('_')[0]) >= 2014:
        hdf['action_date'] = [i.split(' ')[0] for i in hdf['action_date']]
        hdf['action_date'] = [datetime.datetime.strptime(i, '%Y-%m-%d').strftime('%Y-%m-%d') for i in hdf['action_date']]

    ### Code Actions
    hdf['action_openstates'] = ''

    ### Coding Actions and Saving OpenStates Codes
    # *** EXACT MATCHES
    for action in actions:
        replacements = actions[action]
        hdf['action'] = [a.replace(action, replacements[0]) if action == a else a for a in hdf['action']]
        hdf['action_openstates'] = ['; '.join([i for i in [os, replacements[1]] if i != '']) if action == a else os for a, os in zip(hdf['action'], hdf['action_openstates'])]

    # *** PARTIAL MATCHES
    for action in comm_actions:
        replacements = comm_actions[action]
        hdf['action'] = [a.replace(action, replacements[0]) if action in a else a for a in hdf['action']]
        hdf['action_openstates'] = ['; '.join([i for i in [os, replacements[1]] if i != '']) if action in a else os for a, os in zip(hdf['action'], hdf['action_openstates'])]

    for action in com_vote_motions:
        replacements = com_vote_motions[action]
        hdf['action'] = [a.replace(action, replacements) if action in a else a for a in hdf['action']]

    ## Subset
    hdf = hdf[['bill_number', 'session', 'chamber', 'action_date', 'action', 'order']]

    return(hdf)


##########################################
####### Loop Over Sessions
##########################################
# s = sessions[2]

for s in sessions:
    print("\n\n *********************************************************************************************" )
    ### Reading In Data, Formating As Pandas DBF, Post 2013 stored as TXT files
    if int(s.split('_')[0]) >= 2014:
        bill_df = pd.read_csv('MDBs/nj_{}_main.csv'.format(s))
        hist_df = pd.read_csv('MDBs/nj_{}_hist.csv'.format(s))
    elif int(s.split('_')[0]) == 2000:
        #bill_dbf = Dbf5('dbfs/{}/MAINBILL.DBF'.format(s))
        bill_dbf = DBF('dbfs/{}/MAINBILL.DBF'.format(s))
        bill_df = pd.DataFrame(iter(bill_dbf))

        hist_dbf = Dbf5('dbfs/{}/BILLHIST.DBF'.format(s))
        hist_df = hist_dbf.to_dataframe()
    else:
        bill_dbf = Dbf5('dbfs/{}/MAINBILL.DBF'.format(s))
        hist_dbf = Dbf5('dbfs/{}/BILLHIST.DBF'.format(s))

        bill_df = bill_dbf.to_dataframe()
        hist_df = hist_dbf.to_dataframe()

    ### Cleaning the Two Files
    bill_df = clean_bill_data(bill_df, s)
    hist_df = clean_hist_data(hist_df, s)

    ### Save
    bill_df.to_csv('NJ_Bill_Details_{}.csv'.format(s), index=False)
    hist_df.to_csv('NJ_Bill_Histories_{}.csv'.format(s), index=False)

    print("\n ~~~ {} Session - Data Cleaned and Saved! ~~~~ \n".format(s) )
    print(" ********************************************************************************************* \n\n")

print(" \n\n **********************************  DONE ********************************** \n\n")
