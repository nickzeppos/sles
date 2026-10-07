# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape OHIO Bill ARCHIVE

@author: PB
"""

##### NOTES:
# This file Scrapes 1997 -- 2014! Use the non-archive scraper for 2015+
# 
# **** RESOLUTION DATA DOES NOT START UNTIL 126th GA -- 2006-2006+
# **** ----> BUT SPARSE STATUS INFO FOR HR's and SR's (not CR/JRs) EVEN WHEN PRESENT for 127th+ ***********
# **** ----> SKIPPING FOR NOW
#
# **** MIGHT BE ABLE TO GET RESOLUTIONS THROUGH HERE: 
# -- HOUSE: http://lsc.state.oh.us/coderev/anh122.nsf/All%20House%20Bills%20and%20Resolutions
# -- SENATE: http://lsc.state.oh.us/coderev/ans122.nsf/All%20Senate%20Bills%20and%20Resolutions
# ------> Can swap in other session nums for anh{122}
###########################
### SPECIAL SESSION for 125th: http://archives.legislature.state.oh.us/SpecialSessionIntroduced.cfm

import csv
import os
from bs4 import BeautifulSoup
import time
import re
import requests
import datetime
os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/OH/')

#######################
##### Extract Session Links
########################

session_years = [y for y in range(1997, 2014, 2)]
session_nums = [i for i in range(122, 131, 1)]
sessions = [[y, n] for y,n in zip(session_years, session_nums)]
del session_years, session_nums

### Drop Previously Scraped
sessions = [i for i in sessions if 'OH_Bill_Details_' + '{}_{}'.format(i[0], i[0] + 1) + '.csv' not in os.listdir('.')]


##############################################################
###### Functions to Scrape A Page, Try Again if Needed, and Return Soup
################################################################
  
def get_page_soup(bill_url, parser = 'lxml'):
    
    ### Get HTML
    try:
        page = requests.get(bill_url, timeout = 30)
    except requests.HTTPError: # as e
        return('HTTP Error')
    except:
        try:
            print("\n ~~> Retrying Bill Request")
            time.sleep(30)
            page = requests.get(bill_url, timeout = 45)
        except requests.HTTPError: # as e
            return('HTTP Error')  
        except:
            print("\n ~~> Retrying Bill Request x 2")
            time.sleep(60)
            page = requests.get(bill_url, timeout = 60)
    time.sleep(1)
    
    ### Return Soup
    page_soup = BeautifulSoup(page.content, parser)
    return(page_soup)    


#######################################################
########## GET BILL URLS VIA FTP SITE FOR A SESSION
#####################################################
# s = sessions[0]
# bill_data = get_session_bills(s)
    
def get_session_bills(s): 
    
    s_start_yr, s_num = s
    
    #### Get ALL House  and Senate Bills
    bill_list_url = 'http://archives.legislature.state.oh.us/bill_search.cfm?NUMBER=&HOUSE=HS&SESSION={}&SUBMIT=GO'.format(s_num)
    bill_search = requests.get(bill_list_url, timeout = 500)
    bill_soup = BeautifulSoup(bill_search.content, 'lxml')
    
    bill_table = bill_soup.find('table', {'class':'billSearch'})
    bill_a_tags = bill_table.findAll('a')
    
    session_bills = [[i.text.strip(), s_start_yr, s_num, 'http://archives.legislature.state.oh.us' + i['href'] ] for i in bill_a_tags if 'ID={}'.format(s_num) in i['href']]
    
    ####### ------> SKIPPING BECAUSE NOT ALL HAVE STATUS INFO (Especially HR and SR's)
    #### 2015 Special Session Bills
    #    if s_num == 125:
    #        print("\n ***** MANUALLY DO 2015 SPECIAL! --- Need to look at the PDF's to back out info **** \n" )
    #    
    #    #### Get RESOLUTIONS for 126th+ (You can get the res numbers for the 125th but no data on the pages)
    #    if s_num >= 126:
    #        res_list_url = 'http://archives.legislature.state.oh.us/res_search.cfm?NUMBER=&HOUSE=HS&type=JCS&SESSION={}&SUBMIT=GO'.format(s_num)
    #        res_search = requests.get(res_list_url, timeout = 500)
    #        res_soup = BeautifulSoup(res_search.content, 'lxml')
    #        
    #        res_table = res_soup.find('table', {'class':'billSearch'})
    #        res_a_tags = res_table.findAll('a')
    #        
    #        session_res = [[i.text.strip(), s_start_yr, s_num, 'http://archives.legislature.state.oh.us' + i['href'] ] for i in res_a_tags if 'ID={}'.format(s_num) in i['href']]
    #        session_bills = session_bills + session_res
    #        print(' \n **** ~~~> Found {} BILL & RESOLUTION URLs for the {}-{} **** \n'.format(len(session_bills), s_start_yr, s_start_yr + 1))
    #    else:
    #        print(' \n **** ~~~> Found {} BILL URLs for the {}-{} **** \n'.format(len(session_bills), s_start_yr, s_start_yr + 1))
    print(' \n **** ~~~> Found {} BILL URLs for the {}-{} **** \n'.format(len(session_bills), s_start_yr, s_start_yr + 1))
    
    ### Export
    return(session_bills)


def parse_date(date):
    clean_date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')
    return(clean_date)

#######################
###### Functions to Scrape Individual Bills
#########################
# bill_data = get_session_bills(sessions[5])
# bill_info = bill_data[1]
# bill_info = session_bills[0]

action_dict = {'A':'Amended', 'E':'Effective', 'P':'Postponed', 'R':'Rereferred', 'S':'Substituted', 'V':'Vetoed', '*':'Miscellaneous',
               'L':'Failed to pass', 'F':'Failed to pass'}

def get_bill_data(bill_info):    
    
    bill_num, s_yr, s_num, bill_url = bill_info
    
    ### CLEAN BILL NUMBERS
    bill_num_z = bill_num.split(' ')
    bill_num_z = bill_num_z[0] + bill_num_z[1].zfill(4)    
    
    ### Get Bill Data    
    bill_soup = get_page_soup(bill_url, parser = 'lxml')
    
    ###############
    ### Summary/Status
    title = ''
    status = ''  ## Code below only gets bill version, doesn't necessarily equate to status
    #bill_soup.find('a', href = re.compile('bills.cfm')).text.strip()
    
    long_title = bill_soup.find('blockquote')
    if long_title is None:
        ### Check if seperate bill text page
        title_url = bill_soup.find('a', text = re.compile('View Bill Text'))
        if title_url is not None:
            title_soup = get_page_soup('http://archives.legislature.state.oh.us' + title_url['href'])
            long_title = title_soup.find('blockquote')
    if long_title is not None:
        long_title = re.sub('\n|\r', ' ', long_title.text.strip())
        long_title = re.sub('  +', ' ', long_title).strip()

    ##############
    ### Status Page
    hist_url = bill_url.replace('bills.cfm', 'billstatus.cfm')
    hist_url = hist_url.replace('res.cfm', 'resstatus.cfm')
    hist_soup = get_page_soup(hist_url, parser = 'lxml')

    if 'Error 404' in hist_soup.get_text():
        return('HTTP Error')
        
    ################
    ### ADJUSTING FOR VARIATIONS IN FORMAT ACROSS SESSIONS -- Format Changes 122- zzz; zzz - 127; 128 - 130 
    bill_actions = []
    
    ################################
    #### 122nd and 123rd SESSION
    if s_num <= 123:
        
        ## Topics
        subjects = hist_soup.find('b', text = re.compile('Subject:')).parent.text
        subjects = re.sub('Subject: ', '', subjects)
    
        ## Sponsor
        sponsors = hist_soup.find('b', text = re.compile('Sponsor:')).parent.text
        sponsors = re.sub('Sponsor: +', '', sponsors)
        cosponsors = ''
        
        ## House/Senate Rows
        house_row = hist_soup.find(text = re.compile('House Action')).findParent('tr').findNext('tr').findNext('tr')
        house_row = house_row.findAll('td')
        senate_row = hist_soup.find(text = re.compile('Senate Action')).findParent('tr').findNext('tr').findNext('tr')
        senate_row = senate_row.findAll('td')
    
        ## Committees
        H_comms = 'House ' + house_row[1].text.strip()
        S_comms = 'Senate ' + senate_row[1].text.strip()
        committees = '; '.join([i for i in [H_comms, S_comms] if i[-1:] != ' '])
        
        #### Loop through chamber
        order = ''
        for row in [[house_row, 'House'], [senate_row, 'Senate']]:
            chamber = row[1]
            action_items = row[0] #.findAll('td')
            ### Introduced
            if action_items[0].text.strip() == '':
                continue
            else:
                date = parse_date(action_items[0].text.strip())
                comm = action_items[1].text.strip()
                bill_actions.append([bill_num_z, s_yr, s_num, date, chamber, 'Introduced in {} and referred to committee'.format(chamber), comm, order])
            
            ### Committee Action
            c_action = action_items[3].text.strip()
            c_date = action_items[4].text.strip()
            if c_action == 'P' and c_date != '':
                bill_actions.append([bill_num_z, s_yr, s_num, c_date, chamber, 'Committee Report: {}'.format(action_dict[c_action]), comm, order])
            if c_action != '' and c_date != '':
                bill_actions.append([bill_num_z, s_yr, s_num, c_date, chamber, 'Committee Report: {} and reported on {}'.format(action_dict[c_action], c_date), comm, order])
            elif c_date != '':
                bill_actions.append([bill_num_z, s_yr, s_num, c_date, chamber, 'Committee Report: Reported out of committee on {}'.format(c_date), comm, order])
            else:
                continue
            
            ### Floor Action
            f_action = action_items[6].text.strip()
            f_date = action_items[7].text.strip()
            if f_action != '' and f_date != '' and f_action != 'A':
                bill_actions.append([bill_num_z, s_yr, s_num, f_date, chamber, 'Floor Action: {}'.format(action_dict[f_action]), '', order])
            elif f_action == 'A' and f_date != '':
                bill_actions.append([bill_num_z, s_yr, s_num, f_date, chamber, 'Floor Action: Passed on 3rd Consideration as Amended', '', order])
            elif f_date != '':
                bill_actions.append([bill_num_z, s_yr, s_num, f_date, chamber, 'Floor Action: Passed on 3rd Consideration', '', order])
            else:
                continue
            
        ### Conference Committee Action
        conf_comm_row = hist_soup.find(text = re.compile('Conference Committee')).findParent('tr').findNext('tr')
        if "Governor's Action" not in conf_comm_row.text and conf_comm_row.text.strip() != '':
            conf_items = conf_comm_row.findAll('td')
            to_conf_date = conf_items[1].text.strip()
            concur_date = conf_items[2].text.strip()
            if to_conf_date != '':
                bill_actions.append([bill_num_z, s_yr, s_num, parse_date(to_conf_date), 'Conference', 'Sent to Conference Committee', '', order])
            if concur_date != '':
                bill_actions.append([bill_num_z, s_yr, s_num, parse_date(concur_date), 'Conference', 'Concurrence in Conference Committee', '', order])   
                
        ### Governor Action
        governor_row = hist_soup.find(text = re.compile("Governor's Action")).findParent('tr')
        if governor_row.findNext('tr').text.strip() != '':
            gov_items = governor_row.findNext('tr').findAll('td')
            if gov_items[1].text.strip() != '' and 'veto' not in str(gov_items[1].text).lower():
                gov_date = parse_date(gov_items[1].text.strip())
                bill_actions.append([bill_num_z, s_yr, s_num, gov_date, 'Governor', 'Approved by Governor', '', order])
                e_date = gov_items[4].text.strip()
                if e_date == '00/00/00' and gov_items[3].text.strip() == 'E':
                    bill_actions.append([bill_num_z, s_yr, s_num, e_date, 'Governor', 'Effective Date: Multiple or Not Recorded'.format(e_date), '', order])
                elif e_date == '' and gov_items[2].text.strip() == '*':
                    note = hist_soup.find('font', text = re.compile('Miscellaneous Notes|Notes')).findParent('tr').findNextSibling()
                    note = re.sub('^\\* |Sections.+effective.+', '', note.text.strip()).strip()
                    note = re.sub(';$', '', note)
                    bill_actions.append([bill_num_z, s_yr, s_num, gov_date, 'Governor', 'Other Action Note: {}'.format(note), '', order])
                else:
                    e_date = parse_date(e_date)
                    bill_actions.append([bill_num_z, s_yr, s_num, e_date, 'Governor', 'Effective Date: {}'.format(e_date), '', order])
            elif gov_items[1].text.strip() != '' and 'veto' in str(gov_items[1].text).lower():
                gov_date = parse_date(gov_items[4].text.strip())
                bill_actions.append([bill_num_z, s_yr, s_num, gov_date, 'Governor', 'Vetoed by Governor', '', order])
    
    ##################################
    ### 124th -- 127th SESSION -- RESOLUTIONS start in 126th --> Need to account for not being referred to Comm always          
    elif s_num <= 127:

        ### Term Variations:
        if s_num == 124:
            ## Topics
            subjects = hist_soup.find('b', text = re.compile('Subject:')).parent.nextSibling.text.strip()
        
            ## Sponsor
            sponsors = hist_soup.find('b', text = re.compile('Sponsor:')).parent.nextSibling.text.strip()
            cosponsors = ''

            ## House/Senate Columns -- If HB, First Col is House; if SB, first col is Senate
            action_table = hist_soup.find('font', text = re.compile('Actions')).findParent('table')
        else:
            ### Topics, Sponsors, Chamber Columns
            subjects = hist_soup.find('b', text = re.compile('Subject:')).findParent('font').get_text()
            subjects = re.sub('Subject:', '', subjects).strip()
        
            sponsors = hist_soup.find('b', text = re.compile('Primary Spon.+:|Sponsor:')).findParent('font').get_text()
            sponsors = re.sub('Primary Spon.+:|Sponsor:', '', sponsors).strip()
            sponsors = re.sub('\xa0', ' ', sponsors)
            sponsors = re.sub('  +', ' ', sponsors)
            cosponsors = ''
        
            ## House/Senate Columns -- If HB, First Col is House; if SB, first col is Senate
            action_table = hist_soup.find('font', text = re.compile('Action by Chamber')).findParent('table')
            
        ################################
        ### Prep For Loop
        order = ''
        committees = []
        if(bill_num_z[0:1] == 'H'):
            chambers = [['House', '27%'], ['Senate', '21%']]
        else:
            chambers = [['Senate', '27%'], ['House', '21%']]
 
        ### Code Chamber Actions -- Same format for both sessions 
        for c_data in chambers:
            chamber = c_data[0]
            col = c_data[1]
        
            ### INTRO + Referral
            intro_date = action_table.find('b', text = re.compile("Introduced")).findParent('tr')
            intro_date = intro_date.find('td', {'width':col}).text.strip()
            if intro_date != '':
                intro_date = parse_date(intro_date)
                comm = action_table.find('b', text = re.compile('Committee Assigned')).findParent('tr')
                comm = comm.find('td', {'width':col}).text.strip()
                bill_actions.append([bill_num_z, s_yr, s_num, intro_date, chamber, 'Introduced in {}'.format(chamber), '', order])
                if comm != '':
                    committees = committees + [chamber + ' ' + comm]
                    bill_actions.append([bill_num_z, s_yr, s_num, intro_date, chamber, 'Referred to Committee', comm, order])
            
            ### Committee Report Date and Action
            c_date = action_table.find('b', text = re.compile('Committee Report')).findParent('tr')
            c_date = c_date.find('td', {'width':col})
            c_action = c_date.findPrevious('td', {'width':'11%'}).div.text.strip() 
            c_date = c_date.text.strip()            
            if c_date != '':
                c_date = parse_date(c_date)
                if c_action == '':
                    bill_actions.append([bill_num_z, s_yr, s_num, c_date, chamber, 'Committee Report: Reported out of committee on {}'.format(c_date), comm, order])
                elif c_action[0:1] == '*':
                    c_action = re.sub('^\\* +', '', c_action)
                    bill_actions.append([bill_num_z, s_yr, s_num, c_date, chamber, 'Committee Report: {} and reported on {}'.format(action_dict[c_action], c_date), comm, order])
                    note = hist_soup.find('font', text = re.compile('Miscellaneous Notes|Notes')).findParent('tr').findNextSibling()
                    note = re.sub('^\\* |Sections.+effective.+', '', note.text.strip()).strip()
                    note = re.sub(';$', '', note)
                    bill_actions.append([bill_num_z, s_yr, s_num, c_date, chamber, 'Committee Action Note: {}'.format(note), comm, order])
                elif c_action == 'P':
                    bill_actions.append([bill_num_z, s_yr, s_num, c_date, chamber, 'Committee Report: Postponed'.format(action_dict[c_action], c_date), comm, order])
                else:
                    bill_actions.append([bill_num_z, s_yr, s_num, c_date, chamber, 'Committee Report: {} and reported on {}'.format(action_dict[c_action], c_date), comm, order])
                
            ### Floor Action
            f_date = action_table.find('b', text = re.compile('Passed 3rd Consideration')).findParent('tr')
            f_date = f_date.find('td', {'width':col})
            f_action = f_date.findPrevious('td', {'width':'11%'}).div.text.strip() 
            f_date = f_date.text.strip()       
            note = ''
            if f_date != '':   
                if f_action[0:1] == '*':
                    f_action = re.sub('\\* +', '', f_action)
                    note = hist_soup.find('font', text = re.compile('Miscellaneous Notes|Notes')).findParent('tr').findNext('tr')
                    note = re.sub('^\\* |Sections.+effective.+|Eff\\. Date.+', '', note.text.strip()).strip()
                    note = re.sub(';$', '', note)
    
                f_date = parse_date(f_date)
                if f_action == 'A':
                    bill_actions.append([bill_num_z, s_yr, s_num, f_date, chamber, 'Floor Action: Passed on 3rd Consideration as Amended', '', order])
                elif f_action != '':
                    bill_actions.append([bill_num_z, s_yr, s_num, f_date, chamber, 'Floor Action: {}'.format(action_dict[f_action]), '', order])
                else:
                    bill_actions.append([bill_num_z, s_yr, s_num, f_date, chamber, 'Floor Action: Passed on 3rd Consideration', '', order])  
                
                if note != '':
                    bill_actions.append([bill_num_z, s_yr, s_num, f_date, chamber, 'Floor Action Note: {}'.format(note), '', order])
        
        ### Other Action --- Break once no info
        ### To Conf Committee
        to_conf = hist_soup.find(text = re.compile('To Conference Committee'))
        if to_conf is not None:
            to_conf = to_conf.findParent('tr')
            to_conf_date = re.sub('To Conference Committee', '', to_conf.text.strip()).strip()
            if to_conf_date != '' and to_conf_date[0:1].isnumeric() == False:
                to_conf_date = parse_date(to_conf_date[1:].strip())
                bill_actions.append([bill_num_z, s_yr, s_num, to_conf_date, 'Conference', 'Sent to Conference Committee', '', order])   
                note = hist_soup.find('font', text = re.compile('Miscellaneous Notes|Notes')).findParent('tr').findNextSibling()
                note = re.sub('^\\* |Sections.+effective.+', '', note.text.strip()).strip()
                note = re.sub(';$', '', note)
                bill_actions.append([bill_num_z, s_yr, s_num, to_conf_date, 'Conference', 'Conference Committee Action Note: {}'.format(note), '', order])    
            elif to_conf_date != '':
                to_conf_date = parse_date(to_conf_date)
                bill_actions.append([bill_num_z, s_yr, s_num, to_conf_date, 'Conference', 'Sent to Conference Committee', '', order])   
        
        ### Conference Committee Action -- Not breaking in case No CC
        conf_comm_row = hist_soup.find(text = re.compile('Concurrence'))
        if conf_comm_row is not None:
            conf_comm_row = conf_comm_row.findParent('tr')
            concur_date = re.sub('Concurrence', '', conf_comm_row.text.strip()).strip()
            
            if re.sub('\\*', '', concur_date) != '' and concur_date[0:1].isnumeric() == False:
                concur_date = parse_date(concur_date[1:].strip())
                bill_actions.append([bill_num_z, s_yr, s_num, concur_date, 'Conference', 'Concurrence in Conference Committee', '', order])   
                note = hist_soup.find('font', text = re.compile('Miscellaneous Notes|Notes')).findParent('tr').findNextSibling()
                note = re.sub('^\\* |Sections.+effective.+', '', note.text.strip()).strip()
                note = re.sub(';$', '', note)
                bill_actions.append([bill_num_z, s_yr, s_num, concur_date, 'Conference', 'Conference Committee Action Note: {}'.format(note), '', order])    
            elif re.sub('\\*', '', concur_date) != '':
                concur_date = parse_date(concur_date)
                bill_actions.append([bill_num_z, s_yr, s_num, concur_date, 'Conference', 'Concurrence in Conference Committee', '', order])   
        
        ### Sent to Governor
        to_gov = hist_soup.find(text = re.compile('Sent to Governor'))
        if to_gov is None:
            pass
        else:
            to_gov = to_gov.findParent('tr')            
            to_gov_date = re.sub('Sent to Governor', '', to_gov.text.strip())
            if to_gov_date != '':
                to_gov_date = parse_date(to_gov_date)
                bill_actions.append([bill_num_z, s_yr, s_num, to_gov_date, 'Governor', 'Sent to Governor', '', order])    
        
                ### End of 10-day period -- Not breaking loop in case unreported 
                end_period = hist_soup.find(text = re.compile('End of 10-day period'))
                if end_period is None:
                    pass
                else:
                    end_period = end_period.findParent('tr')            
                    end_period_date = re.sub('End.+period', '', end_period.text.strip())
                    if end_period_date != '':
                        end_period_date = parse_date(end_period_date)
                        bill_actions.append([bill_num_z, s_yr, s_num, end_period_date, 'Governor', 'End of 10-day period for Governor Action', '', order])    

        #### Gov Action
        governor_row = hist_soup.find(text = re.compile("Governor's Action"))
        if governor_row is None:
            pass
        else:
            governor_row = governor_row.findParent('tr')            
            gov_date = re.sub("Governor's Action", '', governor_row.text.strip()).strip()
      
            ## Check Specific Actions, e.g., vetos
            gov_action = ''
            if gov_date != '' and gov_date[0:1].isalpha() == True:
                gov_action = gov_date[0:1]
                gov_date = gov_date[1:]
                
            if gov_date != '' and gov_action == 'V':
                bill_actions.append([bill_num_z, s_yr, s_num, parse_date(gov_date), 'Governor', 'Vetoed by Governor', '', order])
            elif gov_date != '' and gov_action != '':
                 print('CHECK BILL --- Other Gov Action: {}'.format(hist_url))
                 bill_actions.append([bill_num_z, s_yr, s_num, parse_date(gov_date), 'Governor', "Governors Action: {}".format(action_dict[gov_action]), '', order])
            elif gov_date != '':
                bill_actions.append([bill_num_z, s_yr, s_num, gov_date, 'Governor', 'Approved by Governor', '', order])
            
            
        #### Effective Date
        e_date = hist_soup.find(text = re.compile('Effective Date'))
        if e_date is not None:
            e_date = e_date.findParent('tr').find('td', {'width':re.compile('58%|59%') }).text.strip()
            if e_date != '' and e_date != '00/00/00':
                e_date = parse_date(e_date)
                bill_actions.append([bill_num_z, s_yr, s_num, e_date, 'Governor', 'Effective Date: {}'.format(e_date), '', order])
            elif e_date == '00/00/00':
                bill_actions.append([bill_num_z, s_yr, s_num, e_date, 'Governor', 'Effective Date: Multiple or Not Recorded'.format(e_date), '', order])

        ####### Combine Committe Info
        committees = '; '.join(committees)
    
    ######################
    #### 128th to 130th Sessions
    elif s_num >= 128:
        
        ### Topics, Sponsors, Chamber Columns
        subjects = hist_soup.find('b', text = re.compile('Subject:')).findParent('font').get_text()
        subjects = re.sub('Subject:', '', subjects).strip()
    
        sponsors = hist_soup.find('b', text = re.compile('Primary Spon.+:|Sponsor:')).findParent('font').get_text()
        sponsors = re.sub('Primary Spon.+:|Sponsor:', '', sponsors).strip()
        sponsors = re.sub('\xa0', ' ', sponsors)
        sponsors = re.sub('  +', ' ', sponsors)
        cosponsors = ''
    
        ## House/Senate Columns -- If HB, First Col is House; if SB, first col is Senate
        action_table = hist_soup.find('font', text = re.compile('Action by Chamber')).findParent('table')
        order = ''
        committees = []
        if(bill_num_z[0:1] == 'H'):
            chambers = [['House', 0], ['Senate', 1]]
        else:
            chambers = [['Senate', 0], ['House', 1]]
 
        ### Code Chamber Actions -- Same format for both sessions 
        for c_data in chambers:
            chamber = c_data[0]
            col = c_data[1]
        
            ### INTRO + Referral
            intro_date = action_table.find('b', text = re.compile("Introduced")).findParent('tr')
            intro_date = intro_date.findAll('td', {'width':'32%'})[col].text.strip()
            if intro_date != '':
                intro_date = parse_date(intro_date)
                comm = action_table.find('b', text = re.compile('Committee Assigned')).findParent('tr')
                comm = comm.findAll('td', {'width':'32%'})[col].text.strip()
                bill_actions.append([bill_num_z, s_yr, s_num, intro_date, chamber, 'Introduced in {}'.format(chamber), '', order])
                if comm != '':
                    committees = committees + [chamber + ' ' + comm]
                    bill_actions.append([bill_num_z, s_yr, s_num, intro_date, chamber, 'Referred to Committee', comm, order])
                    
            ### Committee Report Date and Action
            c_date = action_table.find('b', text = re.compile('Committee Report')).findParent('tr')
            c_date = c_date.findAll('td', {'width':'32%'})[col]
            c_action = c_date.findPrevious('td', {'width':'8%'}).text.strip() 
            c_date = c_date.text.strip()            
            if c_date != '':
                c_date = parse_date(c_date)
                if c_action == '':
                    bill_actions.append([bill_num_z, s_yr, s_num, c_date, chamber, 'Committee Report: Reported out of committee on {}'.format(c_date), comm, order])
                elif c_action[0:1] == '*':
                    c_action = re.sub('^\\* +', '', c_action)
                    bill_actions.append([bill_num_z, s_yr, s_num, c_date, chamber, 'Committee Report: {} and reported on {}'.format(action_dict[c_action], c_date), comm, order])
                    note = hist_soup.find('b', text = re.compile('Notes')).findParent('tr')
                    if note is not None:
                        note = note.findNextSibling()
                    else:
                        note = hist_soup.find('b', text = re.compile('Notes')).findNext('td')
                    note = re.sub('^\\* |Sections.+effective.+', '', note.text.strip()).strip()
                    note = re.sub(';$', '', note)
                    bill_actions.append([bill_num_z, s_yr, s_num, c_date, chamber, 'Committee Action Note: {}'.format(note), comm, order])
                elif c_action == 'P':
                    bill_actions.append([bill_num_z, s_yr, s_num, c_date, chamber, 'Committee Report: Postponed'.format(action_dict[c_action], c_date), comm, order])
                else:
                    bill_actions.append([bill_num_z, s_yr, s_num, c_date, chamber, 'Committee Report: {} and reported on {}'.format(action_dict[c_action], c_date), comm, order])
                    
            ### Floor Action
            f_date = action_table.find('b', text = re.compile('Passed 3rd Consideration')).findParent('tr')
            f_date = f_date.findAll('td', {'width':'32%'})[col]
            f_action = f_date.findPrevious('td', {'width':'8%'}).text.strip() 
            f_action = re.sub('\\* +', '', f_action)
            f_date = f_date.text.strip()       
            if f_date != '':
                f_date = parse_date(f_date)
                if f_action == 'A':
                    bill_actions.append([bill_num_z, s_yr, s_num, f_date, chamber, 'Floor Action: Passed on 3rd Consideration as Amended', '', order])
                elif f_action != '':
                    bill_actions.append([bill_num_z, s_yr, s_num, f_date, chamber, 'Floor Action: {}'.format(action_dict[f_action]), '', order])
                else:
                    bill_actions.append([bill_num_z, s_yr, s_num, f_date, chamber, 'Floor Action: Passed on 3rd Consideration', '', order])  
                
            ### Further Action
            further_date = action_table.find('b', text = re.compile('Further Action')).findParent('tr')
            further_date = further_date.findAll('td', {'width':'32%'})[col]
            futher_action = further_date.findPrevious('td', {'width':'8%'}).text.strip() 
            further_date = further_date.text.strip()       
            if further_date != '':
                further_date = parse_date(further_date)
                if futher_action != '':
                    bill_actions.append([bill_num_z, s_yr, s_num, further_date, chamber, 'Further Floor Action: {}'.format(action_dict[futher_action]), '', order])
                else:
                    bill_actions.append([bill_num_z, s_yr, s_num, further_date, chamber, 'Further Floor Action -- Details Unknown', '', order])  
        
        ### Other Action --- Break once no info
        ### To Conf Committee
        to_conf = hist_soup.find(text = re.compile('To Conference Committee'))
        if to_conf is not None:
            to_conf = to_conf.findParent('tr')
            to_conf_date = re.sub('To Conference Committee', '', to_conf.text.strip()).strip()
            if to_conf_date != '' and to_conf_date[0:1].isnumeric() == False:
                to_conf_date = parse_date(to_conf_date[1:].strip())
                bill_actions.append([bill_num_z, s_yr, s_num, to_conf_date, 'Conference', 'Sent to Conference Committee', '', order])   
                note = hist_soup.find('b', text = re.compile('Notes')).findParent('tr')
                if note is not None:
                    note = note.findNextSibling()
                else:
                    note = hist_soup.find('b', text = re.compile('Notes')).findNext('td')
                note = re.sub('^\\* |Sections.+effective.+', '', note.text.strip()).strip()
                note = re.sub(';$', '', note)
                bill_actions.append([bill_num_z, s_yr, s_num, to_conf_date, 'Conference', 'Conference Committee Action Note: {}'.format(note), '', order])    
            elif to_conf_date != '':
                to_conf_date = parse_date(to_conf_date)
                bill_actions.append([bill_num_z, s_yr, s_num, to_conf_date, 'Conference', 'Sent to Conference Committee', '', order])   
        
        ### Conference Committee Action -- Not breaking in case No CC
        conf_comm_row = hist_soup.find(text = re.compile('Concurrence'))
        if conf_comm_row is not None:
            conf_comm_row = conf_comm_row.findParent('tr')
            concur_date = re.sub('Concurrence', '', conf_comm_row.text.strip()).strip()
            if concur_date != '' and concur_date[0:1].isnumeric() == False:
                if concur_date == '*':
                    pass ### Conf Committee Action in note but didn't make it out
                else:
                    concur_date = parse_date(concur_date[1:].strip())
                    bill_actions.append([bill_num_z, s_yr, s_num, concur_date, 'Conference', 'Concurrence in Conference Committee', '', order])   
                    note = hist_soup.find('font', text = re.compile('Miscellaneous Notes|Notes')).findParent('tr')
                    if note is not None:
                        note = note.findNextSibling()
                    else:
                        note = hist_soup.find('b', text = re.compile('Notes')).findNext('td')
                    note = re.sub('^\\* |Sections.+effective.+', '', note.text.strip()).strip()
                    note = re.sub(';$', '', note)
                    bill_actions.append([bill_num_z, s_yr, s_num, concur_date, 'Conference', 'Conference Committee Action Note: {}'.format(note), '', order])    
            elif concur_date != '':
                concur_date = parse_date(concur_date)
                bill_actions.append([bill_num_z, s_yr, s_num, concur_date, 'Conference', 'Concurrence in Conference Committee', '', order])   
        
        ### Sent to Governor
        to_gov = hist_soup.find(text = re.compile('Sent to Governor'))
        if to_gov is not None:
            to_gov = to_gov.findParent('tr')            
            to_gov_date = re.sub('Sent to Governor', '', to_gov.text.strip()).strip()
            if to_gov_date != '':
                to_gov_date = parse_date(to_gov_date)
                bill_actions.append([bill_num_z, s_yr, s_num, to_gov_date, 'Governor', 'Sent to Governor', '', order])    

        ### End of 10-day period -- Not breaking loop in case unreported 
        end_period = hist_soup.find(text = re.compile('End of 10-day period'))
        if end_period is None:
            pass
        else:
            end_period = end_period.findParent('tr')            
            end_period_date = re.sub('End.+period', '', end_period.text.strip()).strip()
            if end_period_date != '':
                end_period_date = parse_date(end_period_date)
                bill_actions.append([bill_num_z, s_yr, s_num, end_period_date, 'Governor', 'End of 10-day period for Governor Action', '', order])    

        #### Gov Action
        governor_row = hist_soup.find(text = re.compile("Governor's Action"))
        if governor_row is None:
            pass
        else:
            governor_row = governor_row.findParent('tr')            
            gov_date = re.sub("Governor's Action", '', governor_row.text.strip()).strip()
      
            ## Check Specific Actions, e.g., vetos
            gov_action = ''
            if gov_date != '' and gov_date[0:1].isalpha() == True:
                gov_action = gov_date[0:1]
                gov_date = gov_date[1:]
                
            if gov_date != '' and gov_action == 'V':
                bill_actions.append([bill_num_z, s_yr, s_num, parse_date(gov_date), 'Governor', 'Vetoed by Governor', '', order])
            elif gov_date != '' and gov_action != '':
                 print('CHECK BILL --- Other Gov Action: {}'.format(hist_url))
                 bill_actions.append([bill_num_z, s_yr, s_num, parse_date(gov_date), 'Governor', "Governors Action: {}".format(action_dict[gov_action]), '', order])
            elif gov_date != '':
                bill_actions.append([bill_num_z, s_yr, s_num, gov_date, 'Governor', 'Approved by Governor', '', order])
                
            
        #### Effective Date
        e_date = hist_soup.find(text = re.compile('Effective Date'))
        if e_date is not None:
            e_date = e_date.findParent('tr').find('td', {'width':'32%'}).text.strip()
            if e_date != '' and e_date != '00/00/00':
                e_date = parse_date(e_date)
                bill_actions.append([bill_num_z, s_yr, s_num, e_date, 'Governor', 'Effective Date: {}'.format(e_date), '', order])
            elif e_date == '00/00/00':
                bill_actions.append([bill_num_z, s_yr, s_num, e_date, 'Governor', 'Effective Date: Multiple or Not Recorded'.format(e_date), '', order])

        ####### Combine Committe Info
        committees = '; '.join(committees)
      
    ### OUTPUT
    bill_details = [bill_num_z, s_yr, s_num, sponsors, cosponsors, status, subjects, title, committees, long_title, bill_url]
    return([bill_details, bill_actions])
        

########################################################
############## SCRAPE SESSION(S)
############################################
# s = sessions[4]
# bill_info = session_bills[10]
    
for s in sessions:
    
    #### Output Lists
    session_bill_details = [['bill_number', 'session_year', 'session_num', 'sponsors', 'cosponsors', 'status', 'subjects', 'title', 'committees', 'long_title', 'bill_url']]    
    session_actions = [['bill_number', 'session_year', 'session_num', 'action_date', 'chamber', 'action', 'committee', 'order']]
    
    print("\n ------------------- OHIO - Now Scraping: {}-{} ---------------------- \n".format(s[0], s[0] + 1))
    
    ### Get all bills for a specific session
    session_bills = get_session_bills(s)

    #### Loop through bills
    num = 1
    total = len(session_bills)
    for bill_info in session_bills:

        bill_data = get_bill_data(bill_info)
        
        if bill_data == "HTTP Error":
            print(" ********** \n ({}/{}) -- {} -- HTTP ERROR --- SKIPPING \n **********".format(num, total, bill_info[0]))
            num += 1
            continue       
              
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
    
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_data[0][0], bill_data[0][-1]))
        num += 1
        
    with open("OH_Bill_Details_" + '{}_{}'.format(s[0], s[0] + 1) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("OH_Bill_Histories_" + '{}_{}'.format(s[0], s[0] + 1) + ".csv", "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- OHIO {}-{} SCRAPED + DATA SAVED  -------------\n\n\n".format(s[0], s[0] + 1))


print("  ********************************** ALL DONE ********************************** ")
