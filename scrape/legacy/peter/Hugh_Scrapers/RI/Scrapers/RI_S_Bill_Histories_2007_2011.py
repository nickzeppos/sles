# RHODE ISLAND - Senate Bill Histories for 2012_2018

# Dependencies
from bs4 import BeautifulSoup, NavigableString
import requests, urllib.request, json, re
import csv

def main():
    # Year range
    for y in range(2007, 2012):
        yr = str(y)[2:]
        
        # Define structure of bill histories CSV (yearly)
        bill_histories_csv = [["bill_number", "session", "act_date", "act_details", "act_order"]]
        
        # Request url
        r_url = "http://webserver.rilin.state.ri.us/BillText" + yr + "/SenateText" + yr + "/SenateText" + yr + ".html"
        
        # Request Senate bill listings from each year in range
        p = requests.get(r_url)
        soup = BeautifulSoup(p.content, "html.parser")
        
        select = soup.find_all("select")
        
        q = select[len(select)-1]
        
        ql = q.find("option").text.split()
        
        try:
            for n in ql:
                bill_number = n
                if "SUB" not in bill_number:
                    # Set initial vars to empty/default
                    bill_session = ""
                    bill_act_date = ""
                    bill_act_details = ""
                
                    try:
                        b_url = "http://webserver.rilin.state.ri.us/BillText" + yr + "/SenateText" + yr + "/" + bill_number + ".htm"
        
                        p = requests.get(b_url)
                        soup = BeautifulSoup(p.content, "html.parser")
                        
                        # Get bill session
                        pot = soup.find_all("p", {"class": "HEADFONT"})
                        if pot:
                            for p in pot:
                                if p.find("b") and ("SESSION" in p.find("b").text or "A.D." in p.find("b").text):
                                    bill_session = p.find("b").text.strip()
                                elif p.find("span") and ("SESSION" in p.find("span").text or "A.D." in p.find("span").text):
                                    bill_session = p.find("span").text.strip()
                    except:
                        print("Error retrieving session information...")
                        bill_session = str(y)
                
                    # Get bill history info
                    re_num = int(re.sub("\D", "", bill_number[1:]))
                    b_url = "http://status.rilin.state.ri.us/bill_history_report.aspx?year=" + str(y) + "&bills=" + str(re_num)

                    p = requests.get(b_url)
                    soup = BeautifulSoup(p.content, "html.parser")

                    if soup:
                        try:
                            bill_act_order = 1
                            for act in soup.find_all("div", {"style": "margin-left: 5%"}):
                                if act.text:
                                    bill_act_date = act.text[:11][:-1]
                                    bill_act_details = act.text[11:]
                            
                                    # Compile into list entry
                                    bill_history_entry = [bill_number, bill_session, bill_act_date, bill_act_details, bill_act_order]
                                    bill_histories_csv.append(bill_history_entry)
                            
                                    # Increment bill_act_order counter
                                    bill_act_order = bill_act_order + 1
                            
                            print("Completed: " + bill_number + " in " + bill_session)
                        except:
                            print("Error: b = " + str(re_num) + ", y = " + str(y) + "...")
        except:
            print("Could not locate element of type <option>")

        # Write contents of bill_histories_csv to a CSV file!
        with open("RI_S_Bill_Histories_" + str(y) + ".csv", "w") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerows(bill_histories_csv)
        csv_file.close()
        print("Generated: RI_S_Bill_Histories_" + str(y) + ".csv!")

if __name__ == "__main__":
    main()