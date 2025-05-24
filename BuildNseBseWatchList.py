import csv
import requests
import logging
from DropboxClient import DropboxClient
import zipfile
import os
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

# Setup logging configuration
logging.basicConfig(
    level=logging.INFO,  # Log all INFO and above (INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Format for logging
    handlers=[
        logging.FileHandler("BuildNseBseWatchList.Log"),  # Log output to file
        logging.StreamHandler()  # Also log to the console
    ]
)

def GenerateWatchListForNifty(tls_name, csv_url):
    try:
        global session
        # Step 1: Download CSV data from the given URL
        logging.info(f"Downloading CSV from URL: {csv_url}")
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = session.get(csv_url, allow_redirects=True, headers=headers)
        response.raise_for_status()  # Raise error for bad responses (404, etc.)
        logging.info("CSV download successful")

        # Step 2: Decode CSV content (assuming it's UTF-8 encoded)
        csv_content = response.content.decode('utf-8').splitlines()
        logging.info(f"CSV content successfully loaded. Preparing to parse.")

        # Step 3: Open the .tls file to write the symbols
        tls_filename = f"{tls_name}.tls"
        with open(tls_filename, "w") as tls_file:
            reader = csv.DictReader(csv_content)

            # Step 4: Loop through CSV and write the Symbol column to the .tls file
            for row in reader:
                symbol = row.get('Symbol')  # Extract the Symbol column
                if symbol:  # Ensure Symbol column exists and isn't empty
                    tls_file.write(symbol + "\n")
        logging.info(f"{tls_filename} file created successfully!")
        return 1

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download the CSV. Error: {e}")
        return 0
    except KeyError:
        logging.error("The CSV does not have a 'Symbol' column.")
        return 0
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return 0

def GenerateAllWatchListForNIFTY():
    # Hardcode the TLS file names and their respective CSV URLs
    tls_files_and_urls = [
        ("NIFTY 50", "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv"),
        ("NIFTY NEXT 50", "https://www.niftyindices.com/IndexConstituent/ind_niftynext50list.csv"),
        ("NIFTY 100", "https://www.niftyindices.com/IndexConstituent/ind_nifty100list.csv"),
        ("NIFTY 200", "https://www.niftyindices.com/IndexConstituent/ind_nifty200list.csv"),
        ("NIFTY 500", "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"),
        ("NIFTY MIDCAP 150", "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv"),
        ("NIFTY MIDCAP 50", "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap50list.csv"),
        ("NIFTY MIDCAP 100", "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap100list.csv"),
        ("NIFTY SMALLCAP 250", "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv"),
        ("NIFTY SMALLCAP 50", "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap50list.csv"),
        ("NIFTY AUTO", "https://www.niftyindices.com/IndexConstituent/ind_niftyautolist.csv"),
        ("NIFTY FINANCIAL SERVICES", "https://www.niftyindices.com/IndexConstituent/ind_niftyfinancelist.csv"),
        ("NIFTY FMCG", "https://www.niftyindices.com/IndexConstituent/ind_niftyfmcglist.csv"),
        ("NIFTY IT", "https://www.niftyindices.com/IndexConstituent/ind_niftyitlist.csv"),
        ("NIFTY MEDIA", "https://www.niftyindices.com/IndexConstituent/ind_niftymedialist.csv"),
        ("NIFTY METAL", "https://www.niftyindices.com/IndexConstituent/ind_niftymetallist.csv"),
        ("NIFTY PHARMA", "https://www.niftyindices.com/IndexConstituent/ind_niftypharmalist.csv"),
        ("NIFTY PRIVATE BANK", "https://www.niftyindices.com/IndexConstituent/ind_nifty_privatebanklist.csv"),
        ("NIFTY PSU BANK", "https://www.niftyindices.com/IndexConstituent/ind_niftypsubanklist.csv"),
        ("NIFTY REALTY", "https://www.niftyindices.com/IndexConstituent/ind_niftyrealtylist.csv"),
        ("NIFTY CONSUMER DURABLES", "https://www.niftyindices.com/IndexConstituent/ind_niftyconsumerdurableslist.csv"),
        ("NIFTY OIL AND GAS", "https://www.niftyindices.com/IndexConstituent/ind_niftyoilgaslist.csv"),
        ("NIFTY CAPITAL MARKETS", "https://www.niftyindices.com/IndexConstituent/ind_niftyCapitalMarkets_list.csv"),
        ("NIFTY COMMODITIES", "https://www.niftyindices.com/IndexConstituent/ind_niftycommoditieslist.csv"),
        ("NIFTY CORE HOUSING", "https://www.niftyindices.com/IndexConstituent/ind_niftyCoreHousing_list.csv"),
        ("NIFTY CPSE", "https://www.niftyindices.com/IndexConstituent/ind_niftycpselist.csv"),
        ("NIFTY ENERGY", "https://www.niftyindices.com/IndexConstituent/ind_niftyenergylist.csv"),
        ("NIFTY EV", "https://www.niftyindices.com/Index_Statistics/ind_niftyEv_NewAgeAutomotive_list.csv"),
        ("NIFTY HOUSING", "https://www.niftyindices.com/IndexConstituent/ind_niftyhousing_list.csv"),
        ("NIFTY INDIA CONSUMPTION", "https://www.niftyindices.com/IndexConstituent/ind_niftyconsumptionlist.csv"),
        ("NIFTY INDIA DEFENSE", "https://www.niftyindices.com/IndexConstituent/ind_niftyindiadefence_list.csv"),
        ("NIFTY INDIA DIGITAL", "https://www.niftyindices.com/IndexConstituent/ind_niftyindiadigital_list.csv"),
        ("NIFTY MNC", "https://www.niftyindices.com/IndexConstituent/ind_niftymnclist.csv"),
        ("NIFTY MOBILITY", "https://www.niftyindices.com/IndexConstituent/ind_niftymobility_list.csv"),
        ("NIFTY PSE", "https://www.niftyindices.com/IndexConstituent/ind_niftypselist.csv"),
        ("NIFTY NON-CYCLICAL CONSUMER", "https://www.niftyindices.com/IndexConstituent/ind_niftynon-cyclicalconsumer_list.csv"),
        ("NIFTY TRANSPORTATION AND LOGISTICS", "https://www.niftyindices.com/IndexConstituent/ind_niftytransportationandlogistics%20_list.csv"),
        # Add more tuples as needed
    ]
    
    for tls_name, csv_url in tls_files_and_urls:
        logging.info(f"Generating .TLS file for {tls_name} from {csv_url}")
        status_Code=GenerateWatchListForNifty(tls_name, csv_url)
        if(status_Code==1):
            localfileName=f"{tls_name}.tls"
            dropBoxUploadPath=f"/NSEBSEBhavCopy/Amibroker_Watchlists/{tls_name}.tls"
            global dropboxClient
            dropboxClient.upload_file(localfileName, dropBoxUploadPath)
            logging.info(f"Uploaded to Dropbox for  {tls_name} from {csv_url} at : {dropBoxUploadPath} ")
        else:
            logging.info(f"No Files to Upload to Dropbox for  {tls_name} from {csv_url}")

def GenerateWatchListForBse(tls_name, bse_api_url):
    try:
        global session
        # Step 1: Setup headers for the API request
        
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.6",
            "Connection": "keep-alive",
            "Referer": "https://www.asiaindex.co.in/indices/code/16/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Sec-GPC": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "sec-ch-ua": "\"Brave\";v=\"129\", \"Not A(Brand\";v=\"8\", \"Chromium\";v=\"129\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        
        #headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        
        logging.info(f"Fetching data from BSE API: {bse_api_url}")
        
        # Step 2: Make the API call
        response = session.get(bse_api_url, headers=headers,timeout=10)
        response.raise_for_status()  # Raise error for bad responses
        logging.info("BSE API call successful")

        # Step 3: Parse the JSON response
        data = response.json()
        logging.info("JSON response successfully parsed.")

        # Step 4: Extract the SCRIP_CODE from the response
        symbols = [entry.get('SCRIP_CODE') for entry in data['Table']]  # Adjusted to get SCRIP_CODE
        
        # Step 5: Write symbols to .tls file
        tls_filename = f"{tls_name}.tls"
        with open(tls_filename, "w") as tls_file:
            for symbol in symbols:
                if symbol:  # Ensure symbol is not empty
                    tls_file.write(symbol + "\n")
        
        logging.info(f"{tls_filename} file created successfully!")
        return 1
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from the BSE API. Error: {e}")
        return 0
    except KeyError:
        logging.error("The JSON response does not have a 'Table' key.")
        return 0
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return 0

def GenerateAllWatchListForBse():
    # Hardcode the TLS file names and their respective CSV URLs
    tls_files_and_urls = [
        ("BSE 100",                     "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=22"),
        ("BSE 150 MIDCAP",              "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=102"),
        ("BSE 200",                     "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=23"),
        ("BSE 250 LARGEMIDCAP",         "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=104"),
        ("BSE 250 SMALLCAP",            "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=103"),
        ("BSE 400 MIDSMALLCAP",         "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=105"),
        ("BSE 500",                     "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=17"),
        ("BSE AUTO",                    "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=42"),
        ("BSE BANKEX",                  "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=53"),
        ("BSE CAPITAL GOODS",           "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=25"),
        ("BSE COMMODITIES",             "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=88"),
        ("BSE CONSUMER",                "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=89"),
        ("BSE CONSUMER DURABLES",       "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=27"),
        ("BSE CPSE",                    "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=80"),
        ("BSE ENERGY",                  "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=90"),
        ("BSE FMCG",                    "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=83"),
        ("BSE FINANCIAL SERVICES",      "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=91"),
        ("BSE HEALTHCARE",              "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=84"),
        ("BSE HOUSING",                 "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=1052"),
        ("BSE INFRASTRUCTURE",          "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=79"),
        ("BSE MANUFACTURING",           "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=86"),
        ("BSE INDUSTRIALS",             "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=92"),
        ("BSE IT",                      "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=85"),
        ("BSE IPO",                     "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=72"),
        ("BSE LARGECAP",                "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=93"),
        ("BSE METAL",                   "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=35"),
        ("BSE MIDCAP",                  "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=81"),
        ("BSE OIL & GAS",               "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=37"),
        ("BSE POWER",                   "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=69"),
        ("BSE PRIVATE BANKS",           "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=114"),
        ("BSE PSU",                     "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=44"),
        ("BSE REALTY",                  "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=67"),
        ("BSE SENSEX",                  "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=16"),
        ("BSE SENSEX 50",               "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=98"),
        ("BSE SERVICES",                "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=121"),
        ("BSE SMALLCAP",                "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=82"),
        ("BSE TECK",                    "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=45"),
        ("BSE TELECOMMUNICATION",       "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=96"),
        ("BSE UTILITIES",               "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=97"),
        ("BSE SENSEX NEXT 30",          "https://www.asiaindex.co.in/AsiaIndexAPI/api/Codewise_Indices/w?code=127"),

        # Add more tuples as needed
    ]

    for tls_name, csv_url in tls_files_and_urls:
        logging.info(f"Generating .TLS file for {tls_name} from {csv_url}")
        status_Code=GenerateWatchListForBse(tls_name, csv_url)
        if(status_Code==1):
            localfileName=f"{tls_name}.tls"
            dropBoxUploadPath=f"/NSEBSEBhavCopy/Amibroker_Watchlists/{tls_name}.tls"
            global dropboxClient
            dropboxClient.upload_file(localfileName, dropBoxUploadPath)
            logging.info(f"Uploaded to Dropbox for  {tls_name} from {csv_url} at : {dropBoxUploadPath} ")
        else:
            logging.info(f"No Files to Upload to Dropbox for  {tls_name} from {csv_url}")

def GenerateNseDerivativesWatchlist():
    # Get the current date
    today = datetime.now()
    # Generate URLs for the last 5 days
    urls = []
    for i in range(5):
        date = today - timedelta(days=i)
        date_str = date.strftime("%Y%m%d")  # Format as YYYYMMDD
        url = f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip"
        urls.append(url)
    directory=''
    for url in urls:
        try:
            print(f"Checking URL: {url}")
            # Send GET request to download the zip file
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            response = requests.get(url,allow_redirects=True,headers=headers)
            if response.status_code == 200:
                print(f"Downloaded: {url}")
                # If response is valid, save the zip file in memory
                with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
                    zip_ref.extractall("downloaded_zip")
                    print("ZIP file extracted")
                directory = "downloaded_zip"
            else:
                print(f"Failed to download from {url}, status code {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {url}: {e}")
    
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            csv_path = os.path.join(directory, filename)
            # Load CSV into DataFrame
            df = pd.read_csv(csv_path)

            # Filter the data
            filtered_df = df[(df['StrkPric'].isna()) & (df['FinInstrmTp'] == 'STF')]

            # Get unique TckrSymb values
            unique_symbols = filtered_df['TckrSymb'].dropna().unique()

            # Write to .tls file
            with open("Derivatives.tls", "w") as tls_file:
                for symbol in unique_symbols:
                    tls_file.write(f"{symbol}\n")
            print("Filtered data written to Derivatives.tls")
            dropBoxUploadPath=f"/NSEBSEBhavCopy/Amibroker_Watchlists/Derivatives.tls"
            global dropboxClient
            dropboxClient.upload_file("Derivatives.tls", dropBoxUploadPath)

if __name__ == "__main__":
    global dropboxClient
    global session
    try:
        dropboxClient=DropboxClient()
        session=requests.Session()
        GenerateAllWatchListForNIFTY()
        session=requests.Session()
        GenerateAllWatchListForBse()
        GenerateNseDerivativesWatchlist()
    finally:
        logging.shutdown()  # Flush and close the log file
        # Get the current time in IST
        ist = timezone('Asia/Kolkata')
        log_file_path = os.path.abspath("BuildNseBseWatchList.Log")
        print(f'Logfile is located locally at : {log_file_path}')
        logFileNameInDropBox=f'/NSEBSEBhavcopy/Logs/{datetime.strftime(datetime.now(ist),'%Y-%m-%d %H-%M-%S').upper()}-BuildNseBseWatchList.Log'
        dropBoxClient.upload_file(log_file_path,logFileNameInDropBox)
        print(f'Log File have been Uploaded to {logFileNameInDropBox}.')
