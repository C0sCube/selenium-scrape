import json, os, csv,urllib3, time, requests
from datetime import datetime
import pandas as pd

today = datetime.now()
csvpath = r"C:\Users\kaustubh.keny\Projects\co.csv"
csvdata = pd.read_csv(csvpath)

_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://www.mca.gov.in/",
}
outputcsv = r"Company_CIN_" + today.strftime("%Y_%b_%d") + ".csv"
if not os.path.exists(outputcsv):
    with open(outputcsv, 'w', newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(["Company Name", "CIN", "ROC Name", "Date of Incorporation", "Company Status", "Error"])
        f.close()


def strip_company_name(coname):
    company = str(coname).upper()
    if company.find("LIMITED") or company.find("LTD") or company.find("PRIVATE") or company.find("PVT"):
        company = company.replace('LIMITED','').replace('LTD','').replace('PRIVATE','').replace('PVT','')
    elif company.find("LIMITED") == -1 or company.find("PRIVATE") == -1 or company.find("LTD") == -1 or company.find("PVT") == -1:
        company = company.rsplit(' ', 1)[0]
    if coname.upper() == company:
        return "NotFound"
    else:
        print("Company Name Stripped : " + company)
        return company


def fetch_company_cin(coname):
    company = str(coname)
    flag = 0
    while flag == 0:
        if not company == 'NotFound':
            url = 'https://www.mca.gov.in/bin/mca/mds/commonSearch?module=MDS&searchKeyWord=' + company + '&searchType=autosuggest&mdsSearchType=company'
            time.sleep(3)
            response = requests.get(url,verify=False) # headers=_headers
            if response.status_code == 200:
                # print()
                data = response.json()
                print(data)
                time.sleep(5)
                if data["error"] == "":
                    flag = 1
                    if not data["data"]["result"] == "":
                        return data["data"]["result"][0]["cnNmbr"]
                    else:
                        return None
                else:
                    company = strip_company_name(company)
        else:
            flag = 1
            with open(outputcsv, 'a', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow([coname, "", "", "", "", "Company Not Found"])
                csvfile.close()
                print(coname + ":Company Not Found")


def fetch_company_master(cocin, coname):
    payload = {'ID': cocin, 'requestID': 'cin'}
    time.sleep(10)
    response = requests.post('https://www.mca.gov.in/bin/MDSMasterDataServlet', data=str(payload),
                             headers=_headers)
    time.sleep(10)
    if response.status_code == 200:
        try:
            try:
                data1 = response.json()
                time.sleep(10)
            except:
                print(coname + " : Something went wrong with Json Conversion")
            if data1["error"] == "":
                filename ="C:/Users/shailesh.dubey/PycharmProjects/pythonProject/CompanyJson/" + cocin + ".json"
                with open(filename, 'w') as f:
                    json.dump(data1["data"], f)
                    f.close()
                if data1["data"]:
                    if data1["data"]["companyData"]:
                        with open(outputcsv, 'a', newline='') as csvfile:
                            csv_writer = csv.writer(csvfile)
                            csv_writer.writerow([data1["data"]["companyData"]["company"], cocin,
                                                 data1["data"]["companyData"]["rocName"],
                                                 data1["data"]["companyData"]["dateOfIncorporation"],
                                                 data1["data"]["companyData"]["llpStatus"],
                                                 data1["message"]])
                            csvfile.close()
                            print(coname + " : " + data1["message"])


        except:
            print(coname + " : Something went wrong")
            with open(outputcsv, 'a', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow([coname, "", "", "", "Something went wrong"])
                csvfile.close()

    else:
        print(coname + " Not found")
        with open(outputcsv, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow([coname, "", "", "", response.status_code])
            csvfile.close()


if __name__ == "__main__":

    for CoName in csvdata['CoName']:
        time.sleep(5)
        print(f"Running for {CoName}")
        CIN = fetch_company_cin(CoName)
        if CIN:
            print(f"CIN Found {CIN}")
            fetch_company_master(CIN, CoName)
 