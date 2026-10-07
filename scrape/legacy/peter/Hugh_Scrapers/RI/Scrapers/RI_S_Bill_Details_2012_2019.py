# RHODE ISLAND - Senate Bill Details for 2012_2018

# Dependencies
from bs4 import BeautifulSoup, NavigableString
import requests, urllib.request, json, re
import csv

def main():
    # Define structure of bill details CSV
    bill_details_csv = [["bill_number", "session", "bill_chamber", "introducer", "date_introduced", "referred_to", "bill_title"]]
    
    # Year range
    for i in range(12, 20):
        yr = str(i)
        
        # SENATE BILLS
        # Senate bill listings url
        h_r_url = "http://webserver.rilin.state.ri.us/BillText" + yr + "/SenateText" + yr + "/SenateText" + yr + ".html"
        
        # Request Senate bill listings from each year in range
        p = requests.get(h_r_url)
        soup = BeautifulSoup(p.content, "html.parser")
        
        try:
            for b_r in soup.find("div", {"class": "bill_panel"}).find_all("tr"):
                bill_number = b_r.find_all("td")[0].text
                if "SUB" not in bill_number:
                    b_url = "http://webserver.rilin.state.ri.us/BillText" + yr + "/SenateText" + yr + "/" + bill_number + ".htm"
            
                    p = requests.get(b_url)
                    soup = BeautifulSoup(p.content, "html.parser")
            
                    # Initialize empty bill vars
                    bill_title = ""
                    bill_introducer = ""
                    bill_date_introduced = ""
                    bill_referred_to = ""
                    bill_session = ""
            
                    # Bill title
                    pot = soup.find("p", {"class": "TITLE"})
                    n_pot = soup.find("p", {"class": "RI_TITLE"})
                    if pot:
                        bill_title = pot.text
                    elif n_pot:
                        bill_title = n_pot.text
                
                    # Bill introducer
                    pot = soup.find_all("p", {"class": "HEADFONTLEFT_TOP"})
                    if pot:
                        for p in pot:
                            if p.find("b") and "Introduced By:" in p.find("b").text:
                                bill_introducer = p.text.replace("Introduced By: ", "").replace("     ", "")
                
                    # Bill date introduced/referred to
                    pot = soup.find_all("p", {"class": "HEADFONTLEFT"})
                    if pot:
                        for p in pot:
                            if p.find("b"):
                                if "Date Introduced:" in p.find("b").text:
                                    bill_date_introduced = p.text.replace("Date Introduced: ", "").replace("     ", "")
                                elif "Referred To:" in p.find("b").text:
                                    bill_referred_to = p.text.replace("Referred To: ", "").replace("     ", "")
            
                    # Bill referred to
                    pot = soup.find("p", {"class": "HEADFONTLEFT_BOTTOM"})
                    if pot:
                        if pot.find("b") and "Referred To:" in pot.find("b").text:
                            bill_referred_to = pot.text.replace("Referred To: ", "").replace("     ", "")
                    
                    # Bill session
                    pot = soup.find_all("p", {"class": "HEADFONT"})
                    if pot:
                        for p in pot:
                            if p.find("b") and ("SESSION" in p.find("b").text or "A.D." in p.find("b").text):
                                bill_session = p.find("b").text
                            elif p.find("span") and ("SESSION" in p.find("span").text or "A.D." in p.find("span").text):
                                bill_session = p.find("span").text
            
                    # Aggregate bill entry
                    b_complete_list = [bill_number, bill_session, "Senate", bill_introducer, bill_date_introduced, bill_referred_to, bill_title]
        
                    # Append b_complete_list to aggregate CSV
                    bill_details_csv.append(b_complete_list)
        
                    # Provide a progress update in the command-line interface
                    print("Completed: " + bill_number + " " + bill_session)
        except:
            print("No element with class: bill_panel")
                
    # Write contents of bill_details_csv to a CSV file!
    with open("RI_S_Bill_Details_2012_2019.csv", "w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(bill_details_csv)
    csv_file.close()
    print("Generated: RI_S_Bill_Details_2012_2019.csv!")

if __name__ == "__main__":
    main()