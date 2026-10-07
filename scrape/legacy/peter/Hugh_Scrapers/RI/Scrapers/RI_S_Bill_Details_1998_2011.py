# RHODE ISLAND - Senate Bill Details for 1998_2011

# Dependencies
from bs4 import BeautifulSoup, NavigableString
import requests, urllib.request, json, re
import csv

def main():
    # Format year range
    for y in range(1998, 2012):
        yr = str(y)[2:]
        
        # Define structure of bill details CSV
        bill_details_csv = [["bill_number", "session", "bill_chamber", "introducer", "date_introduced", "referred_to", "bill_title"]]
        
        # Request url
        r_url = "http://webserver.rilin.state.ri.us/BillText" + yr + "/SenateText" + yr + "/SenateText" + yr + ".html"
        
        # Request Senate bill listings from each year in range
        p = requests.get(r_url)
        soup = BeautifulSoup(p.content, "html.parser")
        
        select = soup.find_all("select")
        
        q = select[len(select)-1]
        
        ql = q.find("option").text.split()
        
        for n in ql:
            bill_number = n
            
            if "SUB" not in bill_number and "Sub" not in bill_number:
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
                else:
                    # Early bills
                    h3 = soup.find_all("h3")
                    if h3:
                        if bill_title == "":
                            bill_title = h3[len(h3)-1].text.strip()
                    else:
                        # Mid-00+
                        f = soup.find_all("font", {"size": "3"})
                        if len(f) > 4:
                            c = f[4].find_all("p")
                            if c:
                                bill_title = c[len(c)-1].text.strip()
                            else:
                                bill_title = str(c).strip()
                        else:
                            try:
                                # ...
                                l_d = soup.find_all("p", {"align": "CENTER"})
                                if (l_d) and len(l_d) > 10:
                                    c = l_d[8]
                                    bill_title = c.text.strip()
                            except:
                                print("Exception encountered (fs:3)")
            
                # Bill introducer
                pot = soup.find_all("p", {"class": "HEADFONTLEFT_TOP"})
                if pot:
                    for p in pot:
                        if p.find("b") and "Introduced By:" in p.find("b").text:
                            bill_introducer = p.text.replace("Introduced By: ", "").replace("     ", "")
                else:
                    # Early bills introducer/introduced/referred to
                    p = soup.text
                    re1 = re.search("Introduced By:(.*)", p)
                    if re1:
                        if bill_introducer == "":
                            bill_introducer = re1.group(1).strip()
                    re2 = re.search("Date Introduced(.*)", p)
                    if re2:
                        if bill_date_introduced == "":
                            bill_date_introduced = re2.group(1).replace(":", "").strip()
                    re3 = re.search("Referred To:(.*)", p)
                    if re3:
                        if bill_referred_to == "":
                            bill_referred_to = re3.group(1).strip()
            
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
                else:
                    # Early bills
                    for h3 in soup.find_all("h3"):
                        if h3.find("center") and ("SESSION" in h3.find("center").text or "A.D." in h3.find("center").text):
                            bill_session = h3.find("center").text
                    if bill_session == "":
                        # Mid-00+
                        p_c = soup.find_all("p", {"align": "CENTER"})
                        for p in p_c:
                            if p.text and ("SESSION" in p.text or "A.D." in p.text):
                                bill_session = p.text.strip()
        
                # Aggregate bill entry
                b_complete_list = [bill_number, bill_session, "Senate", bill_introducer, bill_date_introduced, bill_referred_to, bill_title]
                
                # Append b_complete_list to aggregate CSV
                bill_details_csv.append(b_complete_list)
    
                # Provide a progress update in the command-line interface
                print("Completed: " + bill_number + " " + bill_session)
        
        # Generate CSV for Senate bills from each year
        with open("RI_S_Bill_Details_" + str(y) + ".csv", "w") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerows(bill_details_csv)
        csv_file.close()
        print("Generated: RI_S_Bill_Details_" + str(y) + ".csv!")
        
if __name__ == "__main__":
    main()