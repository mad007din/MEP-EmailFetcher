import requests as rq
import re
import pandas as pd
import os.path
from time import sleep
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FxOpt
from selenium.webdriver.common.by import By
import datetime
from installed_browsers import browsers as brow
from sys import exit

##
## Too lazy to implement a logger. Print does it as well :D
##


countries = {"Belgien" : "BE",
                 "Bulgarien" : "BG",
                 "Daenemark" : "DK",
                 "Deutschland" : "DE",
                 "Estland" : "EE",
                 "Finnland" : "FI",
                 "Frankreich" : "FR",
                 "Griechenland" : "GR",
                 "Irland" : "IE",
                 "Italien" : "IT",
                 "Kroatien" : "HR",
                 "Lettland" : "LV",
                 "Litauen" : "LT",
                 "Luxemburg" : "LU",
                 "Malta" : "MT",
                 "Niederlande" : "NL",
                 "Oesterreich" : "AT",
                 "Polen" : "PL",
                 "Portugal" : "PT",
                 "Rumänien" : "RO",
                 "Schweden" : "SE",
                 "Slowakei" : "SK",
                 "Slowenien" : "SI",
                 "Spanien" : "ES",
                 "Tschechien" : "CZ",
                 "Ungarn" : "HU",
                 "Zypern" : "CY"}
directory_to_savefiles = "/path/to/files/"
validation_api_key = "-"

def getList(debug: bool, country_code: str) -> pd.DataFrame:
    """_summary_

    Args:
        debug (bool): Should be the MEP-Websites and the number of found MEPs be printed to the console?
        country_code (str): The two-char country code based on ISO-3166-2
    Returns:
        pd.DataFrame: The dataframe with all urls of the MEPs of the country
    """
    
    website = "https://www.europarl.europa.eu/meps/en/search/advanced?countryCode=" + country_code

    req = rq.get(website)
    html_overview = req.text

    #url_to_sedcard = "https://www.europarl.europa.eu/meps/en/"
    regex_searc_sedcard = r"https:\/\/www\.europarl\.europa\.eu\/meps\/en\/\d+"
    all_people = re.findall(regex_searc_sedcard,html_overview)
    number_people = len(all_people)

    if debug:
        print(f"Number of people: {number_people}. URLs to their MEP-Profile:")
        for i in all_people:
            print(i)
    
    df = pd.DataFrame({'URL':all_people})
    return(df)

def requestMail_JS(url: str, driver: webdriver) -> str:
    """_summary_

    Args:
        url (str): The url to the MEP-Website
        driver (webdriver): The webdriver that is build in main

    Returns:
        str: A string containing of the E-Mail adress and the full name of the MEP, seperated by a '|' so it can be splitted later easier
    """ 
    print(f"Requesting: {url}",end="")
    driver.get(url)
    name = driver.find_element(by=By.CLASS_NAME,value="sln-member-name")
    name = name.text
    print(f" --- Name of MEP: {name}",end=" --- ")
    
    mail = driver.find_element(by=By.CLASS_NAME,value="link_email")
    mailaddress  = mail.get_attribute("href")
    mailaddress = re.sub(r"^.*:","",mailaddress)
    print(f"E-Mail found: {mailaddress}")
    
    data = mailaddress + "|" + name
    return(data)

def request_each_and_get_mail(df: pd.DataFrame, driver: webdriver) -> pd.DataFrame:
    """_summary_

    Args:
        df (pd.DataFrame): The data.frame with the URLs to each MEP
        driver (webdriver): The webdriver build in main

    Returns:
        pd.DataFrame: The dataframe with a new column, the mailadress to each MEP
    """
    df_mail = df.copy()
    df_mail['DATA'] = df_mail["URL"].apply(requestMail_JS,driver = driver)
    return(df_mail)

def main(countries: dict[str,str], directory_to_savefiles: str, API_KEY: str = "-") -> None:
    """_summary_

    Firefox must be installed!

    Args:
        countries (dict[str,str]): A dictionary with the 27 EU-States (or less). Key is not important, can be english, german or whatever. Value must be an ISO 3166-2 Code
        directory_to_savefiles (str): The path to the DIRECTORY where the files should be stored
        API_KEY (str): An API-KEY for Email Verifier (https://rapidapi.com/mr_admin/api/email-verifier)
    """
    
    browser = list(brow())
    for i in range(len(browser)):
        b = browser[i].values()
        if "firefox" in b:
            print("Firefox installed! Script can be continued!")
            break
        else:
            print("Sadly no firefox is installed! Please install firefox if you wish to continue!")        
            exit(0)
    
    start = datetime.datetime.now()
    validateMail = False
    debug = False

    print(countries.keys())

    print("Starting Firefox driver...")
    options = FxOpt()
    options.add_argument("--headless")
    driver = webdriver.Firefox(options=options)
    
    if directory_to_savefiles[-1:] != "/":
        print("Adding '/' to savefile_path")
        directory_to_savefiles = directory_to_savefiles + "/"
    
    for country, country_code in countries.items():
        print(f"\n\nSearching MEPs from {country}\n")

        save_file = directory_to_savefiles + country_code +".csv"
        if not os.path.exists(save_file):
            URLs = getList(debug, country_code)
            final_df = request_each_and_get_mail(URLs.copy(),driver=driver)
        print("Data fetched!")
        
        print("Transform data...")
        print(final_df)
        final_df[["MAIL","NAME"]] = final_df.DATA.str.split("|",expand=True)
        print(final_df)
        final_df[["FIRST_NAME","LAST_NAME"]] = final_df.NAME.str.split(" ",expand=True,n=1)
        print(final_df)
        final_df = final_df.drop('NAME', axis=1).drop('DATA', axis=1)
        print(final_df)
        
        try:
            print(f"Saving final data for {country}")
            final_df.to_csv(save_file,index=False)
        except Exception as e:
            print(f"Couldn't save file! Reason: {e}\nHit Ctrl-C if you don't want to continue!\nWaiting 5 seconds till continuation of script!\nValidation of Mailadresses will be SKIPPED for this one!")
            sleep(5)
            continue
        
        if validateMail:
            if API_KEY != "-":
                final_df = val_Mails(final_df.copy(),API_KEY)
            else:
                print("Mail validation is set on true but no key is given!")
        else:
            print("Validation of Mails will not be done!")
        print("Saving Data with validation_info")
        final_df.to_csv(save_file,index=False)
        
    print("Closing Firefox_Driver...")
    driver.quit()
    end = datetime.datetime.now()
    diff = end - start
    print(f"Processtime: {diff}")
        

def val_Mails(df: pd.DataFrame, API_KEY: str, plan: str = "Basic") -> pd.DataFrame:
    """_summary_
    This checks if mail existst. API offers 4 plans.
    Basic: 5 requests per minute -> one request every 12 seconds. Wait 14 seconds between request to have a puffer.
    Pro:  20 reqeusts per minute -> one request every 3 scondes. Wait 4 seconds between request to have a puffer.
    Ultra: 40 req/minute -> one request every 1.5 seconds. Wait 2 seconds between request to have a puffer.
    Mega: One request/second -> Waittime is 1 second

    Each Mail is checked separatly in a for loop.

    Args:
        df (pd.DataFrame): The dataframe with E-Mails
        API_KEY (str): The API-KEY for validation
        plan (str, optional): The plan used. Defaults to "Basic".

    Returns:
        pd.DataFrame: The dataframe with added column: VALID_MAIL and the result of each mail.
    """
    match plan:
        case "Basic":
            waittime_between_request = 14
        case "Pro":
            waittime_between_request = 4
        case "Ultra":
            waittime_between_request = 2
        case "Mega":
            waittime_between_request = 1
        case _:
            print("No valid plan! Setting to Basic plan")
            waittime_between_request = 14
    status_tmp = []
    print("Validating Mails:")
    print("\n", end="\r")
    api =  "https://email-checker.p.rapidapi.com/verify/v1"
    for x in df['MAIL']:
        print(f"Mailadresse: {x}")
        query = {"email": x}
        print(f"Query: {query}")
        header = {"x-rapidapi-key": API_KEY,
                  "x-rapidapi-host": "email-checker.p.rapidapi.com"
                  }
        resp = rq.get(api,headers=header,params=query)
        if resp.ok:
            print(resp.json())
            status = resp.json()["status"]
            print(f"Status: {status}")
            status_tmp.append(status)
        else:
            print(f"Request failed with error code {resp.status_code}")
            status_tmp.append("HTTP-Error")
        
        for i in range(waittime_between_request,0,-1):
            print(f"Please wait [{i} seconds left]", end="\r")
            sleep(1)
            
    df["VALID_MAIL"] = status_tmp
    return(df)

if __name__ == "__main__":
    main(countries, directory_to_savefiles,validation_api_key)