# UTAH - Script for generating records of histories for each bill
# This script leverages the le.utah.gov API in order to query for bill numbers -> should be forward compatible

# Dependencies
from bs4 import BeautifulSoup, NavigableString
import requests, urllib.request, json, re
import csv

def main():
    # List of sessions for which we can access details through the le.utah.gov API
    sessions_2016_2019 = ("2016GS", "2016S2", "2016S3", "2016S4", "2017GS", "2017S1", "2018GS", "2018S2", "2018S3", "2019GS")

    # Secret token for le.utah.gov API access - registered to hugh@virginia.edu
    api_secret = "B55433A24F4562784B3CA053CBEB6931"

    # Define structure of bill histories CSV
    bill_histories_csv = [["bill_number", "session", "act_location", "act_date", "act_summary", "act_order", "vote"]]
        
    # Iterate progressively through each session availble via API
    for session in sessions_2016_2019:
    
        req_url = "https://glen.le.utah.gov/bills/" + session + "/billlist/" + api_secret
        req_raw = requests.get(req_url)

        json_data = json.loads(req_raw.text)

        # Iterate progressively through each bill returned from API for session in question
        for bill in json_data["bills"]:

            # Bill session info
            if "G" in session:
                # Request static source for general session bills
                r_url = "https://le.utah.gov/~" + session[:4]
            else:
                # Request static source for special session bills
                r_url = "https://le.utah.gov/~" + session
            if "H" in bill["number"]:
                # Request static source for House bills
                r_url += "/bills/hbillint/" + bill["number"] + ".htm"
            else:
                # Request static source for Senate bills
                r_url += "/bills/sbillint/" + bill["number"] + ".htm"

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

            # Now onto bill history info
            r_url = "https://le.utah.gov/~" + session[:4] + "/bills/static/" + bill["number"] + ".html"

            p = requests.get(r_url)
            soup = BeautifulSoup(p.content, "html.parser")
            
            try:
                b_s_div = soup.find("div", {"id": "billStatus"}).find_all("tr")
            
                # Each row in bill status table
                for i in range(len(b_s_div)):
                    if i > 0:
                        c_vals = b_s_div[i].find_all("td")
                        bill_act = [bill["number"], b_session_details, c_vals[2].text, c_vals[0].text, c_vals[1].text, i, c_vals[3].text]
                        bill_histories_csv.append(bill_act)
                        print("Added: " + bill["number"] + " in " + b_session_details + " with index " + str(i))
            except:
                None
                
            print("Bill complete")
            
    # FINALLY, write contents of bill_histories_csv to a CSV file!
    with open('UT_Bill_Histories_2016_2019.csv', 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(bill_histories_csv)
    csv_file.close()

if __name__ == "__main__":
    main()