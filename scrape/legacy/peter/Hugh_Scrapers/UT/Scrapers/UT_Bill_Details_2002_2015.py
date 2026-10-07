# UTAH - BILL DETAILS CSV GENERATOR FOR PRE-API SESSIONS 2002_2005 (NOT FORWARD COMPATIBLE)
# (API ACCESS IS AVAILABLE ON LE.UTAH.GOV STARTING 2016)

# Dependencies
from bs4 import BeautifulSoup, NavigableString
import requests, urllib.request, json, re
import csv

def main():
    # Define structure of bill details CSV
    bill_details_csv = [["bill_number", "session", "last_act_date", "last_act_details", "sponsor", "floor_sponsor", "title", "general_description"]]

    # Reading the aggregate list UT_Bill_Index_2002_2015
    with open("UT_Bill_Index_2002_2015.csv") as csv_f:
        csv_r = list(csv.reader(csv_f, delimiter=","))

    # Iterate progressively through each bill returned from API for session in question
    # Start section at beginning index with 2002GS
    s_section = "2002GS"
    for row in csv_r[1:]:
        
        session = row[1]
        
        if session != s_section:
            # Generate CSV with existing entries
            with open("UT_Bill_Details_" + str(s_section) + ".csv", "w") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerows(bill_details_csv)
            csv_file.close()
            
            # Send alert to console
            print("CSV generated for " + s_section)
            
            # Reset bill_details_csv
            bill_details_csv = [["bill_number", "session", "last_act_date", "last_act_details", "sponsor", "floor_sponsor", "title", "general_description"]]

            # Start over with following year, reset s_section
            s_section = str(session)

        # Continue general process
        bill_number = row[0]

        # SWITCH ADDITION FOR SPECIAL SESSIONS - R
        if "G" in session:
            r_url = "https://le.utah.gov/~" + session[:4] + "/bills/static/" + bill_number + ".html"
        else:
            r_url = "https://le.utah.gov/~" + session + "/bills/static/" + bill_number + ".html"

        p = requests.get(r_url)
        soup = BeautifulSoup(p.content, "html.parser")

        # BASIC BILL DETAILS
        # These details are all loaded statically on the bill's main information page (Python's "requests" module only loads static DOM). We'll handle the dynamically-loaded elements afterward.

        # Number
        b_number = bill_number

        # Title
        try:
            re_syn = re.compile(r"(?:.*?\d+\s+)(.*)")
            re_matches = re.search(re_syn, soup.find("h3").text)
            b_title = re_matches.group(1)
        except:
            b_title = ""

        # Sponsor
        try:
            b_sponsor = soup.find(id="billsponsordiv").find("a").text
        except:
            b_sponsor = ""

        # Floor sponsor
        try:
            b_floor_sponsor = soup.find(id="floorsponsordiv").find("a").text
        except:
            b_floor_sponsor = ""

        # Last action - date and details
        b_last_act_dat = ""
        b_last_act_det = ""
        try:
            for ul in soup.find_all("ul", {"class": "billinfoulm"}):
                if ul.find("b").text == "Information":
                    i_el_li = ul.find_all("li")[1].text.replace("Last Action: ", "").split(", ")
                    b_last_act_dat = i_el_li[0]
                    b_last_act_det = i_el_li[1]
                break
        except:
            b_last_act_dat = ""
            b_last_act_det = ""

        # SESSION DETAILS AND BILL SUMMARY
        # Time to make a new soup request because the bill text is dynamically loaded into the prior parent page. Here, we'll request the bill text directly from its separate (static) source.

        # Construct the proper URL for scraping bill text (depends on whether bill is part of general or special session, House or Senate
        if "G" in session:
            # Request static source for general session bills
            r_url = "https://le.utah.gov/~" + session[:4]
        else:
            # Request static source for special session bills
            r_url = "https://le.utah.gov/~" + session
        if "H" in bill_number:
            # Request static source for House bills
            r_url += "/bills/hbillint/" + bill_number + ".htm"
        else:
            # Request static source for Senate bills
            r_url += "/bills/sbillint/" + bill_number + ".htm"

        p = requests.get(r_url)
        soup = BeautifulSoup(p.content, "html.parser")

        # Session details
        try:
            for c in soup.find_all("center"):
                if "SESSION" in c.text:
                    b_session_details = c.text.replace("SESSION", "").lower()
                    break
        except:
            b_session_details = ""

        # General description
        try:
            b_gen_desc = ""
    
            # Get starting <b> element for "general description"
            for f_b in soup.find_all("b"):
                if "General Description:" in f_b.text:
                    s_b = f_b
                    break
    
            # Get ending <b> element for "highlighted provisions"
            for f_b in soup.find_all("b"):
                if "Highlighted Provisions:" in f_b.text:
                    e_b = f_b
                    break
    
            # Get DOM contents between s_b.next_sibiling and e_b, filter with regex
            for line in btwn(s_b.next_sibling, e_b):
                re_syn = re.compile(r"(?:\d+\s+)(.*)")
                re_matches = re.search(re_syn, line)
                if re_matches:
                    # Append to b_gen_desc, continue
                    b_gen_desc += " " + re_matches.group(1)
            b_gen_desc = b_gen_desc[1:]
        except:
            b_gen_desc = ""

        # WRAPPING IT ALL TOGETHER!
        b_complete_list = [b_number, b_session_details, b_last_act_dat, b_last_act_det, b_sponsor, b_floor_sponsor, b_title, b_gen_desc]
    
        # Append b_complete_list to our globally-scoped CSV
        bill_details_csv.append(b_complete_list)
    
        # Provide a progress update in the command-line interface
        print("Completed: " + b_number + " " + b_session_details)
    
# Function for yielding content between two DOM elements - necessary for getting bill description
def btwn(s, e):
    while s and s != e:
        if isinstance(s, NavigableString):
            t = s.strip()
            if len(t):
                yield t
        s = s.next_element

if __name__ == "__main__":
    main()