# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 11:43:58 2018

Scrape Kentucky Bills

@author: PB
"""
###########################
##### NOTES:
# ********* SENATE BILL LINKS MISSING FROM 2002 RS --- ADDING IN MANUALLY -- HIGHEST SB == SB294
#################################
# ********* CAN GET BILLS PRIOR TO 1997 ---- E.G, FOr 1986, 1988, 1990 FOLLOW THIS PATTERN TO GET LISTS OF BILLS: https://apps.legislature.ky.gov/Record/86RS/h1.htm
# ********* For resolutions: https://apps.legislature.ky.gov/Record/86RS/hr.htm
# ********* NEED to IDENTIFY STEM Changes  VIA: https://apps.legislature.ky.gov/lrcsearch
###########################

import csv
import os
from bs4 import BeautifulSoup
import time
import re
import requests
import datetime

os.chdir('/Users/PB/Dropbox/Data/State Legislative Data/States/KY/')

#######################
##### Extract Session Links
########################

session_page = requests.get('https://apps.legislature.ky.gov/record/pastses.html', timeout = 30) 
session_page_soup = BeautifulSoup(session_page.content, 'lxml')

### Get Sessions
session_list = session_page_soup.findAll('a', href = re.compile('https://apps.legislature.ky.gov/record/'))
session_list = [[re.sub('\r\n +', ' ', i.text).strip(), i['href']] for i in session_list]

### Clean, Drop Current Year
this_year = datetime.datetime.now().year
sessions = [ re.split(' ', txt, 1) +  [url] for txt, url in session_list if int(re.split(' ', txt, 1)[0]) < this_year]

### Clean Session ID
s_dict = {'Regular Session': 'RS', 'Regular Session Prefiled Bills':'RS_Prefile',
          'Special Session': 'SS1', '2nd Special Session': 'SS2', 'Special Session (May)':'SS1', 'Special Session (Sept)':'SS2'}
sessions = [[int(yr), s_dict[s], url] for yr, s, url in sessions]

### Drop Previously Scraped
sessions = [i for i in sessions if 'KY_Bill_Details_' + '{}_{}'.format(i[0], i[1]) + '.csv' not in os.listdir('.')]


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
    
    s_yr, s_type, s_url = s
    
    ### Three Different Version -- 1997 - 2007, 2007 SS2, 2008+ -- BUt 1 and 2 work with same code despite slightly different pages
    if s_yr >= 2008:
        ### Go to List of ALl Bills/Amendments for a Session -- e.g., https://apps.legislature.ky.gov/record/18rs/bills_and_amendments_by_date.html
        session_soup = get_page_soup(s_url.replace('/record.html', '/bills_and_amendments_by_date.html'))
    
        ### Extract URLs for All Bills --- html$ prevents regex from grabbing amendments (which are on bill pages, e.g. #SCA1)
        bill_urls = session_soup.findAll('a', href = re.compile('H[A-Z]+[0-9]+.html$|h[a-z]+[0-9]+.html$|S[A-Z]+[0-9]+.html$|s[a-z]+[0-9]+.html$'))
        bill_urls = [[re.sub('.html', '', i['href']),  s_yr, s_type, s_url.replace('record.html', i['href'])] for i in bill_urls]
    else:
        ### Get Lists of House, Senate Bills and Resolutions
        #session_soup = get_page_soup(s_url)
        #list_pages = session_soup.findAll('a', href = re.compile("bills_|res_"))
        bill_urls = []
        for lp in ['bills_S.htm', 'bills_H.htm', 'res_S.htm', 'res_H.htm']:
            list_soup = get_page_soup(re.sub('record.htm', lp, s_url))
            for i in list_soup.findAll('a', href = re.compile('H[A-Z]+[0-9]+.htm|h[a-z]+[0-9]+.htm|S[A-Z]+[0-9]+.htm$|s[a-z]+[0-9]+.htm')):
                bill_urls.append([re.sub('.html|.htm', '', i['href']), s_yr, s_type, re.sub('record.ht.+$', i['href'], s_url)])
                
        ### Adding Senate Bills for 2002 -- Not on Senate Page SB1 to SB294
        if s_yr == 2002 and s_type == 'RS':
            for i in range(1, 295):
                bn = 'SB{}'.format(i)
                b_url = 'https://apps.legislature.ky.gov/record/02rs/{}.htm'.format(bn)
                bill_urls.append([bn, s_yr, s_type, b_url])
                
    ### Export
    print(' \n **** ~~~> Found {} BILL URLs for {} {} **** \n'.format(len(bill_urls), s_yr, s_type))
    return(bill_urls)


#######################
###### Functions to Scrape Individual Bills
#########################
# bill_data = get_session_bills(sessions[5])
# bill_info = session_bills[841]
    
def get_bill_data(bill_info):    
    
    bill_num, s_yr, s_type, bill_url = bill_info
    
    ### Get Bill Data    
    bill_soup = get_page_soup(bill_url, parser = 'lxml')
    bill_actions = []
    
    #### TWO Differing Formats: 2008+ is good, <2008 is less structured
    if s_yr >= 2008:
        
        title = bill_soup.find('th', text = 'Title')
        if title is not None:
            title = title.parent.find('td').get_text().strip()
        else:
            title = ''
        
        summary = bill_soup.find('th', text = re.compile('Summary') )
        if summary is not None:
            summary = summary.parent.find('td').get_text().strip()
        else:
            summary = ''
        
        all_sponsors = bill_soup.find('th', text = re.compile('Sponsor') ).parent
        all_sponsors = all_sponsors.find('td').get_text().strip()        
        all_sponsors = '; '.join([i.strip() for i in re.split(', |\r\n|\n', all_sponsors) if i.strip() != ''])
        
        ### *** Can also get Bill Draft Request Number but doesn't seem like the requests are public
        
        ### BIll HISTORY
        bill_actions = []        
        hist_table = bill_soup.find('h4', text = re.compile('Actions'))
        if hist_table is not None:
            order = 1
            hist_table = hist_table.findNext('table')
            for row in hist_table.findAll('tr'):
                date = row.find('th').get_text().strip()
                date = datetime.datetime.strptime(date, '%m/%d/%y').strftime('%Y-%m-%d')
                ### Actions for a date are collapsed --- so expanding them 
                actions = row.findAll('li')
                for act in actions:
                    act_z = act.get_text().strip()
                    bill_actions.append([bill_num, s_yr, s_type, date, act_z, order])
                    order += 1  
    
    ##### Bills 2007 and Earlier    
    elif 'HB' in bill_num or 'SB' in bill_num:
        
        info_span = bill_soup.find('a', href = re.compile('bill.doc'))
        
        if info_span is None:
            info_span = bill_soup.find(text = re.compile('\\(BR [0-9]+\\)')).parent
            if 'WITHDRAWN' in info_span.get_text():
                withdraw_txt = [i.strip() for i in info_span.get_text('----').split('----') if i.strip() != '']
                if len(withdraw_txt) == 2:
                    all_sponsors = ''
                    hist_p = [withdraw_txt[1]]
                else:
                    split_cell = [i for i, txt in enumerate(withdraw_txt) if ' - ' in txt][0]
                    all_sponsors = ' '.join(withdraw_txt[:split_cell + 1]).split(' - ')[-1].strip()
                    #if len(all_sponsors) > 1:
                    #    all_sponsors = [i for i in all_sponsors if not re.search('\\(BR|HB [0-9]|SB [0-9]', i)]
                    #    all_sponsors = all_sponsors[-1].strip()
                    #else:
                    #    all_sponsors = all_sponsors[0].strip()
                    hist_p = withdraw_txt[split_cell + 1:]
                    
                bill_details = [bill_num, s_yr, s_type, '', all_sponsors, '', bill_url]
                
                
                order = 1
                for row in hist_p:
                    date, actions = row.split('-', 1)   
                    date = datetime.datetime.strptime('{}, {}'.format(date, s_yr), '%b %d, %Y').strftime('%Y-%m-%d')
                    ### Actions for a date are collapsed --- so expanding them 
                    actions = re.split('; ', actions)
                    for act in actions:
                        bill_actions.append([bill_num, s_yr, s_type, date, act, order])
                        order += 1
                return([bill_details, bill_actions])
            else:
                print('CHECK CASE : {}'.format(bill_url))
                exit()
                
        ### Find INfo
        info_span = info_span.parent
        p_tags = info_span.findAll('p') # When amendments are listed, they are in between p1, p2
        
        ### Skip BILL if no information
        if p_tags == [] and re.search('\\(BR [0-9]+\\)$', bill_soup.text.strip()):
            print('CHECK CASE --- NO BILL INFO -- {}'.format(bill_url))
            bill_details = [bill_num, s_yr, s_type, '', '', '', bill_url]
            bill_actions = []
            return(bill_details, bill_actions)
        elif 'Not introduced for technical reasons' in info_span.text:
            return("Not Introduced")

        ## Get Title and Summary 
        if p_tags == []:
            title = bill_soup.find(text = re.compile('AN ACT')).strip()
            title = re.sub('\r\n', '', title)
            title = re.sub(' +', ' ', title)   
            summary = bill_soup.find(text = re.compile('AN ACT')).nextSibling.nextSibling.strip()
            summary = re.sub('\r\n', '', summary)
            summary = re.sub(' +', ' ', summary)     
        else:
            title_and_summary = [i.strip() for i in re.split('\r\n|\n|\xa0\xa0+', p_tags[0].get_text()) if i.strip() != '']
            title = title_and_summary[0]        
            if len(title_and_summary) == 1:
                summary = ''
            else:
                summary = title_and_summary[1]
            
        ### Sponsors
        sponsor_loop = bill_soup.find(text = re.compile('\\(BR [0-9]+\\)'))
        if sponsor_loop is None:
            sponsor_loop = bill_soup.find(text = re.compile('\\(BR\r\n +[0-9]+\\)'))
            
        sponsor_txt = sponsor_loop
        while True:
            sponsor_loop = sponsor_loop.next
            if sponsor_loop.name == 'p':
                break
            elif sponsor_loop.name == 'a' or sponsor_loop.name == 'br':
                pass
            else:
                sponsor_txt = sponsor_txt + sponsor_loop
        all_sponsors = re.split(' - | -\r\n', sponsor_txt, 1)[1].strip()        
        all_sponsors = re.sub(', |,\r\n', '; ', all_sponsors)
        if '\r\n' in all_sponsors:
            all_sponsors = re.sub('\r\n', '', all_sponsors)
            all_sponsors = re.sub(' +', ' ', all_sponsors)
        
        ### History -- From LAST p_tag --- sometimes 2, sometimes later (e.g., if Amendments)
        if p_tags == []:
            hist_p = bill_soup.find('p', {'style': re.compile('margin-bottom')})
            if hist_p is None:
                info_p = info_span.findParent('p')
                hist_p = info_p.find_next_siblings("p")                
                hist_p = [i for i in hist_p if i.text != ''][-1]
            hist_p = [re.sub('\r\n', '', i.strip()) for i in re.split('\xa0\xa0', hist_p.text) if i.strip() != '']
            hist_p = [re.sub(' +', ' ', i.strip()) for i in hist_p if re.sub(' +', ' ', i.strip()) != '']
        else:
            p_tags = [i for i in p_tags if 'End of page' not in i.text]
            hist_p = p_tags[-1].get_text()
            hist_p = [i.strip() for i in re.split('\r\n|\r|\n', hist_p) if i.strip() != '']           
        
        ### Get History Info --- BUT Skip bills without any history information
        bill_actions = []
        order = 1
        if s_yr == 2002 and s_type == 'RS' and bill_num == 'SB120':
            pass
        else:
            for row in hist_p:
                if re.search('\\(Prefiled.+\\)', row):
                    continue
                date, actions = row.split('-', 1)   
                date = datetime.datetime.strptime('{}, {}'.format(date, s_yr), '%b %d, %Y').strftime('%Y-%m-%d')
                ### Actions for a date are collapsed --- so expanding them 
                actions = re.split('; ', actions)
                for act in actions:
                    bill_actions.append([bill_num, s_yr, s_type, date, act, order])
                    order += 1
                
    ##### Resolutions 2007 and Earlier
    else:
        info_span = bill_soup.find('a', href = re.compile('bill.doc'))
        
        if info_span:
            info_span = info_span.parent
        else:
            info_span = bill_soup.find(text = re.compile('\\(BR [0-9]+\\)')).parent
            if 'WITHDRAWN' in bill_soup.get_text():
                info_txt = [i.strip() for i in info_span.get_text('----').split('----') if i.strip() != '']
                split_cell = [i for i, txt in enumerate(info_txt) if ' - ' in txt][0]
                all_sponsors = ' '.join(info_txt[:split_cell + 1]).split(' - ')[1].strip()
                bill_details = [bill_num, s_yr, s_type, '', all_sponsors, '', bill_url]
                
                if 'WITHDRAWN' in info_span.get_text():
                    hist_p = info_txt[split_cell + 1:]
                else:
                    hist_p = info_span.findParent('p').find_next_siblings("p")
                    hist_p = [i for i in hist_p if i.text.strip() != ''][-1]
                    hist_p = [re.sub('\r\n', '', i.strip()) for i in re.split('\xa0\xa0', hist_p.text) if i.strip() != '']
                    hist_p = [re.sub(' +', ' ', i.strip()) for i in hist_p if re.sub(' +', ' ', i.strip()) != '']
                    
                order = 1
                for row in hist_p:
                    date, actions = row.split('-', 1)   
                    date = datetime.datetime.strptime('{}, {}'.format(date, s_yr), '%b %d, %Y').strftime('%Y-%m-%d')
                    actions = re.split('; ', actions)
                    for act in actions:
                        bill_actions.append([bill_num, s_yr, s_type, date, act, order])
                        order += 1
                return([bill_details, bill_actions])
            
            else:
                print('CHECK CASE : {}'.format(bill_url))
                exit()
        
        ## Get Title and Summary From First P_Tag
        title = info_span.findAll('br', recursive = False)
        if title == []:
            title = ''
        else:
            title = title[-1].previous
            if title.name == 'br':
                title = title.previous.strip()
            else:
                title = title.strip()
            
        ## If Still No Match
        if title == '' and info_span.findAll('br', recursive = False):
            title = info_span.findAll('br', recursive = False)[-1].next.strip()
        summary = ''
        
        ### Sponsors
        #all_sponsors = info_span.findAll('a', recursive = False)[-1].nextSibling.strip()
        #print('\n SPONSORS BEFORE SPLIT: ' + all_sponsors + '\n')  
        #all_sponsors = all_sponsors.split(' - ', 1)[1].strip()
        sponsor_loop = bill_soup.find(text = re.compile('\\(BR [0-9]+\\)'))
        sponsor_txt = sponsor_loop
        while True:
            sponsor_loop = sponsor_loop.next
            if sponsor_loop.name == 'p' or (sponsor_loop == '\n' and sponsor_loop.previous.name == 'br'):
                break
            elif sponsor_loop.name == 'a' or sponsor_loop.name == 'br':
                pass
            else:
                sponsor_txt = sponsor_txt + sponsor_loop
        all_sponsors = re.split(' - | -\r\n', sponsor_txt, 1)[1].strip()        
        all_sponsors = re.sub(', |,\r\n', '; ', all_sponsors)
        if '\r\n' in all_sponsors:
            all_sponsors = re.sub('\r\n', '', all_sponsors)
            all_sponsors = re.sub(' +', ' ', all_sponsors)
            
        ### History -- From LAST p_tag --- sometimes 2, sometimes later (e.g., if Amendments)
        hist_p = info_span.findAll('p')
        if hist_p == []:
            info_p = info_span.findParent('p')
            hist_p = info_p.find_next_siblings("p")                
            hist_p = [i for i in hist_p if i.text != '' and 'Vote History' not in i.text][-1]
            hist_p = [re.sub('\r\n', '', i.strip()) for i in re.split('\xa0\xa0', hist_p.text) if i.strip() != '']
            hist_p = [re.sub(' +', ' ', i.strip()) for i in hist_p if re.sub(' +', ' ', i.strip()) != '']
        else:
            hist_p = [i for i in hist_p if 'End of page' not in i.text]
            hist_p = hist_p[-1].text    
            hist_p = [i.strip() for i in re.split('\r\n|\r|\n', hist_p) if i.strip() != '']           
        
        order = 1
        for row in hist_p:
            if re.search('\\(Prefiled.+\\)', row):
                continue
            date, actions = row.split('-', 1)   
            date = datetime.datetime.strptime('{}, {}'.format(date, s_yr), '%b %d, %Y').strftime('%Y-%m-%d')
            ### Actions for a date are collapsed --- so expanding them 
            actions = re.split('; ', actions)
            for act in actions:
                bill_actions.append([bill_num, s_yr, s_type, date, act, order])
                order += 1
        
   
    ### OUTPUT
    bill_details = [bill_num, s_yr, s_type, title, all_sponsors, summary, bill_url]
    return([bill_details, bill_actions])
        

########################################################
############## SCRAPE SESSION(S)
############################################
# s = sessions[0]
# bill_info = session_bills[1568]
    
for s in sessions:
    
    #### Output Lists
    session_bill_details = [['bill_number', 'session_year', 'session_type', 'title', 'sponsors', 'summary', 'bill_url']]    
    session_actions = [['bill_number', 'session_year', 'session_type', 'action_date', 'action', 'order']]
    
    print("\n ------------------- Now Scraping: {} {}  ---------------------- \n".format(s[0], s[1]))
    
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
        elif bill_data == "Not Introduced":
            print(" ********** \n ({}/{}) -- {} -- BILL NOT INTRODUCED FOR TECHNICAL REASONS: SKIPPING \n **********".format(num, total, bill_info[0]))
            continue
        
        session_bill_details.append(bill_data[0])

        if bill_data[1] != []:
            for action_row in bill_data[1]:
                session_actions.append(action_row)
    
        print(" ({}/{}) -- {} -- URL: {}".format(num, total, bill_data[0][0], bill_data[0][6]))
        num += 1
        
    with open("KY_Bill_Details_{}_{}.csv".format(s[0], s[1]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_bill_details)
        
    with open("KY_Bill_Histories_{}_{}.csv".format(s[0], s[1]), "w", newline = "") as f:
        writer = csv.writer(f)
        writer.writerows(session_actions)
        
    print("\n\n\n ------------- {} {} SCRAPED + DATA SAVED  -------------\n\n\n".format(s[0], s[1]))


print("  ********************************** ALL DONE ********************************** ")
