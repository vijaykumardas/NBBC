import requests
import os
import html
import logging
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
from io import StringIO
from DropboxClient import DropboxClient
from zoneinfo import ZoneInfo  # Only available in Python 3.9+
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from dotenv import load_dotenv

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
                (62,'360 ONE Mutual Fund',True),
                (85,'Abakkus Mutual Fund',True),
                (39,'ABN AMRO Mutual Fund',True),
                (3,'Aditya Birla Sun Life Mutual Fund',True),
                (50,'AEGON Mutual Fund',True),
                (1,'Alliance Capital Mutual Fund',True),
                (80,'Angel One Mutual Fund',True),
                (53,'Axis Mutual Fund',True),
                (75,'Bajaj Finserv Mutual Fund',True),
                (48,'Bandhan Mutual Fund',True),
                (46,'Bank of India Mutual Fund',True),
                (4,'Baroda BNP Paribas Mutual Fund',True),
                (36,'Benchmark Mutual Fund',True),
                (59,'BNP Paribas Mutual Fund',True),
                (32,'Canara Robeco Mutual Fund',True),
                (81,'Capitalmind Mutual Fund',True),
                (84,'Choice Mutual Fund',True),
                (60,'Daiwa Mutual Fund',True),
                (31,'DBS Chola Mutual Fund',True),
                (38,'Deutsche Mutual Fund',True),
                (6,'DSP Mutual Fund',True),
                (47,'Edelweiss Mutual Fund',True),
                (40,'Fidelity Mutual Fund',True),
                (51,'Fortis Mutual Fund',True),
                (27,'Franklin Templeton Mutual Fund',True),
                (8,'GIC Mutual Fund',True),
                (49,'Goldman Sachs Mutual Fund',True),
                (63,'Groww Mutual Fund',True),
                (9,'HDFC Mutual Fund',True),
                (76,'Helios Mutual Fund',True),
                (37,'HSBC Mutual Fund',True),
                (20,'ICICI Prudential Mutual Fund',True),
                (57,'IDBI Mutual Fund',True),
                (11,'IL&FS Mutual Fund',True),
                (65,'IL&FS Mutual Fund (IDF)',True),
                (14,'ING Mutual Fund',True),
                (42,'Invesco Mutual Fund',True),
                (70,'ITI Mutual Fund',True),
                (82,'Jio BlackRock Mutual Fund',True),
                (16,'JM Financial Mutual Fund',True),
                (43,'JPMorgan Mutual Fund',True),
                (17,'Kotak Mahindra Mutual Fund',True),
                (56,'L&T Mutual Fund',True),
                (18,'LIC Mutual Fund',True),
                (69,'Mahindra Manulife Mutual Fund',True),
                (45,'Mirae Asset Mutual Fund',True),
                (19,'Morgan Stanley Mutual Fund',True),
                (55,'Motilal Oswal Mutual Fund',True),
                (54,'Navi Mutual Fund',True),
                (21,'Nippon India Mutual Fund',True),
                (73,'NJ Mutual Fund',True),
                (78,'Old Bridge Mutual Fund',True),
                (58,'PGIM India Mutual Fund',True),
                (44,'PineBridge Mutual Fund',True),
                (34,'PNB Mutual Fund',True),
                (64,'PPFAS Mutual Fund',True),
                (10,'Principal Mutual Fund',True),
                (13,'quant Mutual Fund',True),
                (41,'Quantum Mutual Fund',True),
                (74,'Samco Mutual Fund',True),
                (22,'SBI Mutual Fund',True),
                (52,'Shinsei Mutual Fund',True),
                (67,'Shriram Mutual Fund',True),
                (2,'Standard Chartered Mutual Fund',True),
                (24,'SUN F&C Mutual Fund',True),
                (33,'Sundaram Mutual Fund',True),
                (25,'Tata Mutual Fund',True),
                (26,'Taurus Mutual Fund',True),
                (83,'The Wealth Company Mutual Fund',True),
                (72,'Trust Mutual Fund',True),
                (79,'Unifi Mutual Fund',True),
                (61,'Union Mutual Fund',True),
                (28,'UTI Mutual Fund',True),
                (71,'WhiteOak Capital Mutual Fund',True),
                (77,'Zerodha Mutual Fund',True),
                (29,'Zurich India Mutual Fund',True)
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
    # Format dates for the URL
    formatted_start_date = start_date.strftime("%d-%b-%Y").upper()
    formatted_end_date = end_date.strftime("%d-%b-%Y").upper()
    
    logging.info(f"Fetching data from {formatted_start_date} to {formatted_end_date}")
    for mf_code, mf_name, should_download in dictMFCodes:
        if should_download:
            try:
                
                final_download_url = amfi_url_format.format(mf_code, formatted_start_date, formatted_end_date)
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
                             names=['Scheme Code', 'NAV Name', 'Plan','Option','ISIN Div Payout/ISIN Growth',
                                    'ISIN Div Reinvestment', 'Net Asset Value', 'Date'])
            raw_file_path = os.path.join(output_dir, "MFBhavcopyRaw.csv")
            df.to_csv(raw_file_path, index=False, encoding='utf-8')
            logging.info("\n%s", df.head(40).to_string(index=False))
           # Define valid (Plan, Option) pairs
            valid_combos = [
                ("Regular", "Direct"),
                ("Direct Plan", "(Growth)"),
                ("Direct Plan", "Cumulative"),
                ("Direct Plan", "Direct Growth"),
                ("Direct Plan", "Growth"),
                ("Direct Plan", "GROWTH Option"),
                ("Direct Plan", "Growth Option Option"),
                ("Direct Plan", "Growth Option"),
                ("Direct Plan", "Growth ")
            ]

            # Filter DataFrame by matching tuples
            df = df[df[['Plan', 'Option']].apply(tuple, axis=1).isin(valid_combos)]

            # Log neatly formatted table
            logging.info("\n%s", df.head(10).to_string(index=False))



            # Keep only required columns and rename them
            df = df[['Scheme Code', 'NAV Name', 'Net Asset Value', 'Date']]
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
            # Remove rows where CLOSE is zero
            df = df[df['CLOSE'] != 0]
            
            # Keep rows where FULLNAME contains "Direct"
            # AND drop rows containing IDCW, IDWC, Income Distribution, or Dividend
            #additionalRemoveFilters = "IDCW|IDWC|Income Distribution|Dividend|Bonus|Payout"
            #df = df[
            #    df['FULLNAME'].str.contains("Direct", case=False, na=False) &
            #    ~df['FULLNAME'].str.contains(additionalRemoveFilters, case=False, na=False)
            #    ]
            # Get the current time in IST
            current_time_ist = datetime.now(ZoneInfo('Asia/Kolkata'))
            filename = f"{end_date.strftime('%Y-%m-%d')}-MF-BHAVCOPY.CSV"
            file_path = os.path.join(output_dir, filename)

            # Write DataFrame to CSV
            df.to_csv(file_path, index=False, encoding='utf-8')
            global dropboxClient
            dropBoxClient.upload_file(file_path,f'/NSEBSEBhavcopy/DailyBhavcopy/{filename}')
            logging.info(f"All data written to {file_path}")
        except Exception as e:
            logging.error(f"Error processing or writing data: {e}")

# Main function
def main():
    # Input number of historical days or use default
    global dropboxClient
    try:
        historical_days = 15
        
        # Get the current time in IST
        end_date = datetime.now(ZoneInfo('Asia/Kolkata')).date()
        start_date = end_date - timedelta(days=historical_days)
        
        # Set output directory
        output_dir = 'MF_NAV_History'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Break into 90-day chunks
        chunk_size = 90
        current_start = start_date

        while current_start < end_date:
            current_end = min(current_start + timedelta(days=chunk_size), end_date)
            print(f"Fetching NAV history from {current_start} to {current_end}...")
            fetch_nav_history(current_start, current_end, output_dir)
            current_start = current_end  # move to next chunk
            
        # Fetch NAV history for specified mutual funds
        #fetch_nav_history(start_date, end_date, output_dir)
    except:
        logging.info("Mutual Fund NAV History Download Completed.")
        current_ist_time = datetime.now(ZoneInfo('Asia/Kolkata'))
        logging.shutdown()  # Flush and close the log file
        log_file_path = os.path.abspath("MFBhavCopyDownload.Log")
        print(f'Logfile is located locally at : {log_file_path}')
        logFileNameInDropBox=f"/NSEBSEBhavcopy/Logs/{datetime.strftime(current_ist_time,'%Y-%m-%d %H-%M-%S').upper()}-MFBhavCopyDownload.log"
        dropBoxClient.upload_file(log_file_path,logFileNameInDropBox)
        print(f"Log File have been Uploaded to {logFileNameInDropBox}.")

dropBoxClient=0
if __name__ == "__main__":
    load_dotenv()
    dropBoxClient=DropboxClient()
    # Configure pandas display so columns don't get cut off
    pd.set_option('display.max_columns', None)   # show all columns
    pd.set_option('display.width', None)        # auto-fit width
    pd.set_option('display.colheader_justify', 'center')  # center headers
    main()
