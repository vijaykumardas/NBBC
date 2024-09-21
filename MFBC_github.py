import requests
import os
import html
import logging
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
from io import StringIO
from DropboxClient import DropboxClient
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Disable InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Setup logging
logging.basicConfig(
    filename='MFBhavCopyDownload.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)

# Mutual fund codes and download preferences (from your original script)
dictMFCodes=(
                (62,'360 ONE Mutual Fund (Formerly Known as IIFL Mutual Fund)',False),
                (39,'ABN  AMRO Mutual Fund',False),
                (3,'Aditya Birla Sun Life Mutual Fund',True),
                (50,'AEGON Mutual Fund',False),
                (1,'Alliance Capital Mutual Fund',False),
                (53,'Axis Mutual Fund',True),
                (75,'Bajaj Finserv Mutual Fund',False),
                (48,'Bandhan Mutual Fund',True),
                (46,'Bank of India Mutual Fund',False),
                (4,'Baroda BNP Paribas Mutual Fund',True),
                (36,'Benchmark Mutual Fund',False),
                (59,'BNP Paribas Mutual Fund',False),
                (32,'Canara Robeco Mutual Fund',True),
                (60,'Daiwa Mutual Fund',False),
                (31,'DBS Chola Mutual Fund',False),
                (38,'Deutsche Mutual Fund',False),
                (6,'DSP Mutual Fund',True),
                (47,'Edelweiss Mutual Fund',True),
                (40,'Fidelity Mutual Fund',False),
                (51,'Fortis Mutual Fund',False),
                (27,'Franklin Templeton Mutual Fund',True),
                (8,'GIC Mutual Fund',False),
                (49,'Goldman Sachs Mutual Fund',False),
                (63,'Groww Mutual Fund',True),
                (9,'HDFC Mutual Fund',True),
                (76,'Helios Mutual Fund',False),
                (37,'HSBC Mutual Fund',True),
                (20,'ICICI Prudential Mutual Fund',True),
                (57,'IDBI Mutual Fund',False),
                (11,'IL&S Mutual Fund',False),
                (65,'IL&FS Mutual Fund (IDF)',False),
                (14,'ING Mutual Fund',False),
                (42,'Invesco Mutual Fund',True),
                (70,'ITI Mutual Fund',False),
                (16,'JM Financial Mutual Fund',True),
                (43,'JPMorgan Mutual Fund',False),
                (17,'Kotak Mahindra Mutual Fund',True),
                (56,'L&T Mutual Fund',False),
                (18,'LIC Mutual Fund',True),
                (69,'Mahindra Manulife Mutual Fund',True),
                (45,'Mirae Asset Mutual Fund',True),
                (19,'Morgan Stanley Mutual Fund',False),
                (55,'Motilal Oswal Mutual Fund',True),
                (54,'Navi Mutual Fund',True),
                (21,'Nippon India Mutual Fund',True),
                (73,'NJ Mutual Fund',False),
                (78,'Old Bridge Mutual Fund',False),
                (58,'PGIM India Mutual Fund',False),
                (44,'PineBridge Mutual Fund',False),
                (34,'PNB Mutual Fund',False),
                (64,'PPFAS Mutual Fund',True),
                (10,'Principal Mutual Fund',False),
                (13,'quant Mutual Fund',True),
                (41,'Quantum Mutual Fund',True),
                (74,'Samco Mutual Fund',False),
                (22,'SBI Mutual Fund',True),
                (52,'Shinsei Mutual Fund',False),
                (67,'Shriram Mutual Fund',True),
                (2,'Standard Chartered Mutual Fund',False),
                (24,'SUN F&amp;C Mutual Fund',False),
                (33,'Sundaram Mutual Fund',True),
                (25,'Tata Mutual Fund',True),
                (26,'Taurus Mutual Fund',True),
                (72,'Trust Mutual Fund',False),
                (61,'Union Mutual Fund',True),
                (28,'UTI Mutual Fund',True),
                (71,'WhiteOak Capital Mutual Fund',False),
                (77,'Zerodha Mutual Fund',False),
                (29,'Zurich India Mutual Fund',False)
)
listOfStringsToStrip=[  'Open Ended Schemes ( Balanced )',
                    'Open Ended Schemes ( ELSS )',
                    'Open Ended Schemes ( Fund of Funds - Domestic )',
                    'Open Ended Schemes ( GOLD ETFs )',
                    'Open Ended Schemes ( Gilt )',
                    'Open Ended Schemes ( Growth )',
                    'Open Ended Schemes ( Income )',
                    'Open Ended Schemes ( Liquid )',
                    'Open Ended Schemes ( Other ETFs )',
                    'Reliance Mutual Fund',
                    'Open Ended Schemes ( Equity Scheme - Large Cap Fund )',
                    'Open Ended Schemes ( Equity Scheme - Large & Mid Cap Fund )',
                    'Open Ended Schemes ( Equity Scheme - Mid Cap Fund )',
                    'Open Ended Schemes ( Equity Scheme - Small Cap Fund )',
                    'Open Ended Schemes ( Equity Scheme - ELSS )',
                    'Open Ended Schemes ( Debt Scheme - Overnight Fund )',
                    'Open Ended Schemes ( Debt Scheme - Liquid Fund )',
                    'Open Ended Schemes ( Debt Scheme - Money Market Fund )',
                    'Open Ended Schemes ( Debt Scheme - Corporate Bond Fund )',
                    'Open Ended Schemes ( Debt Scheme - Banking and PSU Fund )',
                    'Open Ended Schemes ( Debt Scheme - Gilt Fund )',
                    'Open Ended Schemes ( Hybrid Scheme - Aggressive Hybrid Fund )',
                    'Open Ended Schemes ( Hybrid Scheme - Dynamic Asset Allocation or Balanced Advantage )',
                    'Open Ended Schemes ( Hybrid Scheme - Arbitrage Fund )',
                    'Open Ended Schemes ( Hybrid Scheme - Equity Savings )',
                    'Open Ended Schemes ( Other Scheme - Index Funds )',
                    'Open Ended Schemes ( Other Scheme - Other  ETFs )',
                    'Open Ended Schemes ( Other Scheme - FoF Overseas )',
                    'Open Ended Schemes ( Other Scheme - FoF Domestic )',
                    'Open Ended Schemes ( Equity Scheme - Flexi Cap Fund )'
                ]
def is_valid_row(row):
    """
    Check if a row is a valid data row.
    A valid row should have the expected number of columns and should not be empty or a scheme description.
    """
    columns = row.split(';')
    # Check if the row has the expected number of columns (8 columns in this case)
    if len(columns) == 8:
        # Additional check: 'Scheme Code' column should be numeric
        retValue= columns[0].isdigit() 
        #logging.info(f"{columns[0]} is Digit : {columns[0].isdigit()}")
    else:
        retValue= False
    #if(retValue==True):
    #    logging.info(f"{row} is a Valid Row")
    #else:
    #    logging.info(f"{row} is a Invalid Valid Row")
    return retValue    
    
# Function to fetch NAV history for mutual funds
def fetch_nav_history(start_date, end_date, output_dir):
    amfi_url_format = 'https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf={0}&tp=1&frmdt={1}&todt={2}'
    combined_data = ""  # To store all the mutual fund data in a single string
    
    for mf_code, mf_name, should_download in dictMFCodes:
        if should_download:
            try:
                
                final_download_url = amfi_url_format.format(mf_code, start_date, end_date)
                logging.info(f"Downloading NAV history for {mf_name} (MF Code: {mf_code})... Url : {final_download_url}")
                response = requests.get(final_download_url, verify=False)
                response.raise_for_status()

                str_response = html.unescape(response.content.decode())

                # Clean up the response string
                for strip_text in listOfStringsToStrip:
                    str_response = str_response.replace(strip_text, "")

                str_response = str_response.replace("\r", "\n").splitlines()  # Split into lines

                # Filter and keep only valid rows
                valid_rows = [row for row in str_response if is_valid_row(row)]

                # Append valid rows to combined data
                combined_data += "\n".join(valid_rows) + "\n"

                logging.info(f"Successfully fetched NAV history for {mf_name}.")
            except requests.exceptions.RequestException as e:
                logging.error(f"Error downloading NAV history for {mf_name}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error processing {mf_name}: {e}")
    #logging.info(f"Combined Data : {combined_data}")
    # Process the combined data into a DataFrame
    if combined_data:
        try:
            # Create DataFrame from combined_data using StringIO
            data = StringIO(combined_data)
            df = pd.read_csv(data, delimiter=';', header=None,
                             names=['Scheme Code', 'Scheme Name', 'ISIN Div Payout/ISIN Growth',
                                    'ISIN Div Reinvestment', 'Net Asset Value', 'Repurchase Price',
                                    'Sale Price', 'Date'])
            logging.info(df)
            # Keep only required columns and rename them
            df = df[['Scheme Code', 'Scheme Name', 'Net Asset Value', 'Date']]
            df.columns = ['TICKER', 'FULLNAME', 'CLOSE', 'DATE_YMD']

            # Convert DATE_YMD to YYYYMMDD format
            df['DATE_YMD'] = pd.to_datetime(df['DATE_YMD'], format='%d-%b-%Y').dt.strftime('%Y%m%d')

            # Add additional columns with specified values
            df['TICKER'] = 'MF' + df['TICKER'].astype(str)
            df['OPEN'] = df['CLOSE']
            df['HIGH'] = df['CLOSE']
            df['LOW'] = df['CLOSE']
            df['VOLUME'] = 0
            df['INDUSTRYNAME'] = ''
            df['SECTORNAME'] = ''
            df['ALIAS'] = ''
            df['ADDRESS'] = ''
            df['COUNTRY'] = ''
            df['CURRENCY'] = ''
            df['OPENINT'] = 0
            df['AUX1'] = 0
            df['AUX2'] = 0

            # Reorder columns as required
            df = df[['DATE_YMD', 'TICKER', 'FULLNAME', 'OPEN', 'HIGH', 'LOW', 'CLOSE',
                     'VOLUME', 'INDUSTRYNAME', 'SECTORNAME', 'ALIAS', 'ADDRESS',
                     'COUNTRY', 'CURRENCY', 'OPENINT', 'AUX1', 'AUX2']]

            # Get the current time in IST
            ist = timezone('Asia/Kolkata')
            current_time_ist = datetime.now(ist)
            filename = f"{current_time_ist.strftime('%Y-%m-%d')}-MF-BHAVCOPY.CSV"
            file_path = os.path.join(output_dir, filename)

            # Write DataFrame to CSV
            df.to_csv(file_path, index=False, encoding='utf-8')
            dropBoxClient.upload_file(file_path,f'/NSEBSEBhavcopy/DailyBhavcopy/{filename}')
            logging.info(f"All data written to {file_path}")
        except Exception as e:
            logging.error(f"Error processing or writing data: {e}")

# Main function
def main():
    # Input number of historical days or use default
    historical_days = "10" #input("For how many days of data to fetch (Default 30): ")
    if not historical_days.isdigit():
        historical_days = 30
    else:
        historical_days = int(historical_days)
    
    # Get the current time in IST
    ist = timezone('Asia/Kolkata')
    end_date = datetime.now(ist).date()
    start_date = end_date - timedelta(days=historical_days)
    
    # Format dates for the URL
    formatted_start_date = start_date.strftime("%d-%b-%Y").upper()
    formatted_end_date = end_date.strftime("%d-%b-%Y").upper()
    
    logging.info(f"Fetching data from {formatted_start_date} to {formatted_end_date}")
    
    # Set output directory
    output_dir = 'MF_NAV_History'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Fetch NAV history for specified mutual funds
    fetch_nav_history(formatted_start_date, formatted_end_date, output_dir)

    logging.info("Mutual Fund NAV History Download Completed.")
    logging.shutdown()  # Flush and close the log file
    log_file_path = os.path.abspath("MFBhavCopyDownload.Log")
    print(f'Logfile is located locally at : {log_file_path}')
    logFileNameInDropBox=f'/NSEBSEBhavcopy/Logs/{datetime.now(ist).strftime(tday,'%Y-%m-%d-%H%M%S').upper()}-MFBhavCopyDownload.log'
    dropBoxClient.upload_file(log_file_path,logFileNameInDropBox)
    print(f'Log File have been Uploaded to {logFileNameInDropBox}.')

dropBoxClient=DropboxClient()
if __name__ == "__main__":
    main()
