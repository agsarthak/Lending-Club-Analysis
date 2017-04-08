
import requests
import os
import zipfile
import urllib.request
from bs4 import BeautifulSoup

############### Cleanup required directories ###############
def cleanup_dir():
    try:
        if not os.path.exists('loan_data_downloaded_zips'):
            os.makedirs('loan_data_downloaded_zips', mode=0o777)
                
        if not os.path.exists('loan_data_downloaded_zips_unzipped'):
            os.makedirs('loan_data_downloaded_zips_unzipped', mode=0o777)
        
        if not os.path.exists('reject_loan_data_downloaded_zips'):
            os.makedirs('reject_loan_data_downloaded_zips', mode=0o777)
        
        if not os.path.exists('reject_loan_data_downloaded_zips_unzipped'):
            os.makedirs('reject_loan_data_downloaded_zips_unzipped', mode=0o777)
        
        if not os.path.exists('cleanFiles'):
            os.makedirs('cleanFiles', mode=0o777)

        print('Directories cleanup complete.')
    except Exception as e:
        print(str(e))
        exit()
    
        
##################### Download loan data zips ###############
def loan_data_download():
    url = 'https://www.lendingclub.com/info/download-data.action'
    page = urllib.request.urlopen(url)
    soup = BeautifulSoup(page,"lxml") # parse the page and save it in soup format 
    values = soup.find("div", { "id" : "loanStatsFileNamesJS" }).text
    dropdownoptions=values.split('|')
    dropdownoptions.pop()
    session_requests = requests.session()
    
    for option in dropdownoptions: #iterate over the options, place attribute value in list
        url="https://resources.lendingclub.com/"+option
        r = session_requests.get(url,stream=True)
        with open(os.path.join('loan_data_downloaded_zips',option), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        print("Downloaded Zip:",option)
    print("All zip downloaded!")    

##################### Unzip loan data zips ###############    
def loan_data_unzip_extract():
    try:
        zip_files = os.listdir('loan_data_downloaded_zips')
        for f in zip_files:
            z = zipfile.ZipFile(os.path.join('loan_data_downloaded_zips', f), 'r')
            for file in z.namelist():
                if file.endswith('.csv'):
                    z.extract(file, r'loan_data_downloaded_zips_unzipped')
        print("All loan data zips extracted!")
    except Exception as e:
            print("Error in Extracting")
            exit()

##################### Download loan data zips ###############
def reject_loan_data_download():
    url = 'https://www.lendingclub.com/info/download-data.action'
    page = urllib.request.urlopen(url)
    soup = BeautifulSoup(page,"lxml") # parse the page and save it in soup format 
    values = soup.find("div", { "id" : "rejectedLoanStatsFileNamesJS" }).text
    dropdownoptions=values.split('|')
    dropdownoptions.pop()
    session_requests = requests.session()
    
    for option in dropdownoptions: #iterate over the options, place attribute value in list
        url="https://resources.lendingclub.com/"+option
        r = session_requests.get(url,stream=True)
        with open(os.path.join('reject_loan_data_downloaded_zips',option), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        print("Downloaded Zip:",option)   

##################### Unzip loan data zips ###############    
def reject_loan_data_unzip_extract():
    zip_files = os.listdir('reject_loan_data_downloaded_zips')
    print('files read')
    for f in zip_files:
        z = zipfile.ZipFile(os.path.join('reject_loan_data_downloaded_zips', f), 'r')
        for file in z.namelist():
            if file.endswith('.csv'):
                z.extract(file, r'reject_loan_data_downloaded_zips_unzipped')
    print("All Rejected loan data zips extracted!")
    
           
          
