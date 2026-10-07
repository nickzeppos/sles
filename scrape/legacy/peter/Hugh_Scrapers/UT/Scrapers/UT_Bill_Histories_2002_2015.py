# UTAH - Script for generating records of histories for each bill
# This script leverages a brute-forced solution to pre-API bill listings in order to obtain bill numbers. Not forward compatible.

# Dependencies
from bs4 import BeautifulSoup, NavigableString
import requests, urllib.request, json, re
import csv

def main():
    # Define structure of bill histories CSV
    bill_histories_csv = [["bill_number", "session", "act_location", "act_date", "act_summary", "act_order", "vote"]]

    # Reading the aggregate list UT_Bill_Index_2002_2015
    with open("UT_Bill_Index_2002_2015.csv") as csv_f:
        csv_r = list(csv.reader(csv_f, delimiter=","))

    # Iterate progressively through history of each bill returned from API for session in question
    for row in csv_r[1:]:
        if len(row) > 1:
            try:
                bill_number = row[0]
                session = row[1]
        
                # Bill session info
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

                # Now onto bill history info
                r_url = "https://le.utah.gov/~" + session[:4] + "/bills/static/" + bill_number + ".html"

                p = requests.get(r_url)
                soup = BeautifulSoup(p.content, "html.parser")
    
                try:
                    b_s_div = soup.find("div", {"id": "billStatus"}).find_all("tr")

                    # Each row in bill status table
                    for i in range(len(b_s_div)):
                        if i > 0:
                            c_vals = b_s_div[i].find_all("td")
                            bill_act = [bill_number, b_session_details, c_vals[2].text, c_vals[0].text, c_vals[1].text, i, c_vals[3].text]
                            bill_histories_csv.append(bill_act)
                            print("Added: " + bill_number + " in " + b_session_details + " with index " + str(i))
                except:
                    None
        
                print("Bill complete")
            except:
                print("Empty row at index")
    
        # FINALLY, write contents of bill_histories_csv to a CSV file!
    with open('UT_Bill_Histories_2002_2015.csv', 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(bill_histories_csv)
    csv_file.close()
    
if __name__ == "__main__":
    main()