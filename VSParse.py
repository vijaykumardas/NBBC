import json
import requests
import ast
from os.path import exists
import csv
import logging
import progressbar
import datetime
from lxml import html
import copy
import time
from bs4 import BeautifulSoup
import dropbox
from DropboxClient import DropboxClient
import pandas as pd
import sqlite3
import math
from pytz import timezone
logging.basicConfig(filename="ValueStocksProcess.Log",level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S')


def GetNseEquityData():
    NSE_Equity_List_csv_url="https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    nse_Master_Equity_List_File='01.MASTER_EQUITY_L.CSV'
    file_exists = exists(nse_Master_Equity_List_File)
    if(file_exists):
        print(nse_Master_Equity_List_File + " Found.")
    else:
        print(nse_Master_Equity_List_File + " not Found. Hence Downloading")
        req = requests.get(NSE_Equity_List_csv_url)
        url_content = req.content
        csv_file = open(nse_Master_Equity_List_File, 'wb')
        csv_file.write(url_content)
        csv_file.close()
        print(nse_Master_Equity_List_File + " Saved.")
    with open(nse_Master_Equity_List_File, 'r') as file:
        reader = csv.DictReader(
            file, fieldnames=['SYMBOL','NAME OF COMPANY','SERIES','DATE OF LISTING','PAID UP VALUE','MARKET LOT','ISIN NUMBER','FACE VALUE'])
        data = list(reader)
        return data[1:len(data)]



def GetStockInfoFromDLevels(NseMasterRow):
    # some JSON:
    urlFormat='https://ws.dlevels.com/get-autosearch-stock?term={NseCode}&pageName='
    url=urlFormat.format(NseCode=NseMasterRow["SYMBOL"])
    #print(url)
    response = session.get(url)
    if(response.status_code==200):
        #print(response.text)
        responseJson=response.text

        # parse x:
        y = json.loads(responseJson)
        if(y['response']!=[]):
            # the result is a Python dictionary:
            #print(y['response'][0])
            #print(y['response'][0]['Symbol_Name'])
            foundItem=None
            for item in y['response']:
                #print(item)
                if(item["EXCHANGE_NAME"]==NseMasterRow["SYMBOL"]):
                    foundItem=item
                    break
            if(foundItem is not None):
                dictInfo= {"SYMBOL":NseMasterRow["SYMBOL"],"NAME":NseMasterRow["NAME OF COMPANY"],"DLEVEL_KEY":foundItem['Symbol_Name'].replace(' ','_')}
                #print(dictInfo)
                return dictInfo
    else:
        print("Error")
def BuildAndSaveDLevelBasicInfo():
    nseEquityData=GetNseEquityData() 
    #nseEquityData=nseEquityData[:20]
    logging.debug(nseEquityData)
    Master_Equity_l_w_Dlevel_info='02.MASTER_EQUITY_L_W_DLEVEL_INFO.CSV'
    file_exists = exists(Master_Equity_l_w_Dlevel_info)
    csv_columns=['SYMBOL','NAME','DLEVEL_KEY']
    if(file_exists):
        print("DLevelBasicInfo File : "+Master_Equity_l_w_Dlevel_info + " Found.")
    else:
        print("DLevelBasicInfo File : "+Master_Equity_l_w_Dlevel_info + " Not Found. Hence Building...")
        dLevelInfo=[]
        widgets = [' [',progressbar.Timer(format= 'Building DLevel Stock Info: %(elapsed)s'),'] ', progressbar.Bar('*'),' (',progressbar.Counter(format='%(value)02d/%(max_value)d'), ') ',]
 
        bar = progressbar.ProgressBar(max_value=len(nseEquityData),widgets=widgets).start()
        logging.debug("Total Symbols to Process : "+str(len(nseEquityData)))
        progressCounter=0
        for row in nseEquityData:
            try:
                #print(row)
                if(row["SERIES"]=='EQ' or row["SERIES"]=="BE"):
                    logging.debug("Getting StockInfo from DLevel for :"+row["SYMBOL"])
                    dLevelInfoRow=GetStockInfoFromDLevels(row)
                    if(dLevelInfoRow != None):
                        dLevelInfo.append(GetStockInfoFromDLevels(row))
                else:
                    logging.debug("Skipping "+row["SYMBOL"]+" Since the Series is not EQ or BE. The Symbol is :"+row["SERIES"])
            except Exception as Argument:
                logging.debug("Exception While getting StockInfo from DLevel for "+str(row["SYMBOL"])+". Exception="+str(Argument))
            finally:
                progressCounter+=1
                bar.update(progressCounter)
                time.sleep(1/50)
                logging.debug("Symbols Processed : "+str(progressCounter))
        try:
            if(len(dLevelInfo) > 0):
                with open(Master_Equity_l_w_Dlevel_info, 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                    writer.writeheader()
                    for data in dLevelInfo:
                        writer.writerow(data)
                print("DLevelBasicInfo has been Written to : "+Master_Equity_l_w_Dlevel_info)
                logging.debug("DLevelBasicInfo has been Written to : "+Master_Equity_l_w_Dlevel_info)
            else:
                print("DLevelBasicInfo Could not be  Written to : "+Master_Equity_l_w_Dlevel_info + ". Since No Data")
                logging.debug("DLevelBasicInfo Could not be  Written to : "+Master_Equity_l_w_Dlevel_info + ". Since No Data")
        except Exception as Argument:
            logging.debug("DLevelBasicInfo Could not be  Written to : "+Master_Equity_l_w_Dlevel_info + ". Due to Exception: "+Argument)
        #print(dLevelInfo)
    file_exists = exists(Master_Equity_l_w_Dlevel_info)
    if(file_exists):
        with open(Master_Equity_l_w_Dlevel_info, 'r') as file:
            reader = csv.DictReader(
                file, fieldnames=csv_columns)
            data = list(reader)
            return data[1:len(data)]
            
'''
Following Method is not in Use.
'''
def GetStockAdvancedInfoFromDLevels(BasicInfoRow):
    # Request the page
    pageBasicFundamentals = session.get('https://www.valuestocks.in/en/fundamentals-nse-stocks/lti_is_equity')
     
    # Parsing the page
    # (We need to use page.content rather than
    # page.text because html.fromstring implicitly
    # expects bytes as input.)
    tree = html.fromstring(pageBasicFundamentals.content) 
     
    # Get element using XPath
    Sector              =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[1]/div/div/div[1]/span/text()')
    MarketCapElement    =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[1]/div/div/div[2]/span/text()')
    x = MarketCapElement[0].replace('\r','').replace('\n','')
    x=" ".join(x.split()).split('(')
    MarketCapText=x[0]
    MarketCapNum=x[1].replace("Cr)",'')

    FundamentalScore    =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[3]/div/div/div[1]/h4/text()')
    FundamentalsText    =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[2]/div/div/div[2]/h4/text()')
    ValuationRange      =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[4]/div/div/div[1]/h4/text()')
    ValuationText       =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[4]/div/div/div[2]/h4/text()')
    PePs                =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[4]/div/div/div[2]/h4/text()[3]')
    PePsValues=PePs[0].split('|')
    PriceToEarning      = PePsValues[0].replace("P/E: ",'').replace(' ','')
    PriceToSales        = PePsValues[1].replace("P/S: ",'').replace(' ','')
def GetStockAdvancedInfoFromDLevels1(row):
    rowBackup=copy.deepcopy(row)
    logging.debug("START: Fetching Advanced Info for :"+rowBackup["SYMBOL"]+" having dlevelKey:"+rowBackup["DLEVEL_KEY"])
    # some JSON:
    try:
        #1. Get Info from the Web Service Call.
        urlFormat='https://ws.dlevels.com/vs-api?platform=web&action=Fundamental%20Report&param_list={dLevel_Key}'
        url=urlFormat.format(dLevel_Key=rowBackup["DLEVEL_KEY"].replace("_","%20"))
        logging.debug("Fetching Advanced Info using url:"+url)
        response = session.get(url)
        if(response.status_code==200):
            responseJson=response.text
            y = json.loads(responseJson)
            if(y['response']!=[] and len(y['response'])==2):
                rowBackup.update(y['response'][1][0])
        #2. Get the Info from Parsing the Data.
        #pageBasicFundamentalsFormat = "https://www.valuestocks.in/en/fundamentals-nse-stocks/{dLevelKey}"
        #pageBasicFundamentalsUrl=pageBasicFundamentalsFormat.format(dLevelKey=rowBackup["DLEVEL_KEY"])
        #pageResponse=session.get(pageBasicFundamentalsUrl)
        #tree = html.fromstring(pageResponse.content) 
        #Sector              =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[1]/div/div/div[1]/span/text()')
        ValuationRange      =   "0-0"#tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[4]/div/div/div[1]/h4/text()')
        
        # Retrieving Additional Valuation Information
        #valuationUrlFormat="https://www.valuestocks.in/en/stocks-valuation/{dLevel_Key}"
        #valuationurl=valuationUrlFormat.format(dLevel_Key=rowBackup["DLEVEL_KEY"])
        #responseValuation=session.get(valuationurl)
        #try:
        #    if(response.status_code==200):
        #        soup=BeautifulSoup(responseValuation.content,'html.parser')
        #        s=soup.find_all('td',class_="stock_data_algnmnt")
        ValuationAsPerDCF="0"#s[2].text
        ValuationAsPerGraham="0"#s[3].text
        ValuationAsPerEarning="0"#s[4].text
        ValuationAsPerBookValue="0"#s[5].text
        ValuationAsPerSales="0"#s[6].text
        SectorPE="0"#s[10].text
        #else:
        #        logging.debug("Error Response from Url:"+valuationurl)
        #except Exception as Argument:
        #    logging.debug("Exception during Reading Valuation Data"+Argument)
        
        return {
        "DATENUM":datetime.datetime.now().strftime('%Y%m%d'),
        "DATE": datetime.datetime.now().strftime('%d-%b-%Y'),
        "SYMBOL":rowBackup["SYMBOL"],
        "NAME":rowBackup["NAME"],
        "SECTOR":rowBackup["SECTOR"],
        "CMP":rowBackup["LastClose"],
        "VALUATION":rowBackup["valuation"],
        "FAIRRANGE":ValuationRange,
        "PE":rowBackup["Pe"],
        "SECTORPE":SectorPE,
        "MARKETCAP":rowBackup["MarketCap"],
        "MKCAPTYPE":rowBackup['MkCapType'],
        "TREND":rowBackup["technical_trend"],
        "FUNDAMENTAL":rowBackup["stock_fundamental"],
        "MOMENTUM":rowBackup["price_momentum"],
        "DERATIO":rowBackup["Deratio"],
        "PRICETOSALES":rowBackup["PriceToSales"],
        "PLEDGE":rowBackup["Pledge"],
        "QBS":rowBackup["Qbs"].replace("/","(")+")" if len(rowBackup["Qbs"])>0 else rowBackup["Qbs"],
        "QBS%":rowBackup["qbs_perc"],
        "AGS":rowBackup["Ags"].replace("/","(")+")" if len(rowBackup["Ags"])>0 else rowBackup["Ags"],
        "AGS%":rowBackup["ags_perc"],
        "VALUATION_DCF":ValuationAsPerDCF,
        "VALUATION_GRAHAM":ValuationAsPerGraham,
        "VALUATION_EARNING":ValuationAsPerEarning,
        "VALUATION_BOOKVALUE":ValuationAsPerBookValue,
        "VALUATION_SALES":ValuationAsPerSales
        
        }
    except Exception as Argument:
        print("ERROR: Error Fetching Advanced Info for :"+rowBackup["SYMBOL"]+" having dlevelKey:"+rowBackup["DLEVEL_KEY"])
        print("Exception: "+str(Argument))
        logging.debug("ERROR: Error Fetching Advanced Info for :"+rowBackup["SYMBOL"]+" having dlevelKey:"+rowBackup["DLEVEL_KEY"])
        logging.debug("Exception: "+str(Argument))
    finally:
        print("FINISHED: Fetching Advanced Info for :"+rowBackup["SYMBOL"]+" having dlevelKey:"+rowBackup["DLEVEL_KEY"])
        logging.debug("FINISHED: Fetching Advanced Info for :"+rowBackup["SYMBOL"]+" having dlevelKey:"+rowBackup["DLEVEL_KEY"])

def BuildAndSaveAdvancedDLevelInfo(Dlevel_Advanced_info,Dlevel_Failed_Info):
    global dropboxClient
    status_code=True
    nseEquityData = BuildAndSaveDLevelBasicInfo()
    
    if len(nseEquityData) > 0:
        print("DLevel Basic Info available, Proceeding to Build Advance Info Sheet")
        logging.debug("DLevel Basic Info available, Proceeding to Build Advance Info Sheet")
    else:
        print("DLevel Basic Info not available, Check if 02.MASTER_EQUITY_L_W_DLEVEL_INFO.CSV Exists and Contains the data")
        logging.debug("DLevel Basic Info not available, Check if 02.MASTER_EQUITY_L_W_DLEVEL_INFO.CSV Exists and Contains the data")
        return False
    
    csv_columns = ["DATENUM","DATE", "SYMBOL", "NAME", "SECTOR", "CMP", "VALUATION", "FAIRRANGE", "PE", "SECTORPE", "MARKETCAP", "MKCAPTYPE", "TREND", "FUNDAMENTAL", "MOMENTUM", "DERATIO", "PRICETOSALES", "PLEDGE", "QBS", "QBS%", "AGS", "AGS%", "VALUATION_DCF", "VALUATION_GRAHAM", "VALUATION_EARNING", "VALUATION_BOOKVALUE", "VALUATION_SALES"]
    
    dLevelInfo = []
    dLevelInfoFailure = []

    # Fetch advanced stock information
    for row in nseEquityData:
        try:
            print("Processing Advanced Data for :" + row["SYMBOL"])
            logging.debug("Processing Advanced Data for :" + row["SYMBOL"])
            dLevelInfoRow = GetStockAdvancedInfoFromDLevels1(row)
            if dLevelInfoRow != None:
                dLevelInfo.append(dLevelInfoRow)
            else:
                dLevelInfoFailure.append(row)
                print("Unable to Get Advance Stock Info for Symbol:" + row["SYMBOL"])
                logging.debug("Unable to Get Advance Stock Info for Symbol:" + row["SYMBOL"])
        except Exception as Argument:
            dLevelInfoFailure.append(row)
            print("Some Exception while fetching the Advanced Info for :" + row["SYMBOL"])
            print("Exception: " + str(Argument))
            logging.debug("Some Exception while fetching the Advanced Info for :" + row["SYMBOL"])
            logging.debug("Exception: " + str(Argument))
    
    # Writing advanced stock info to CSV
    try:
        if len(dLevelInfo) > 0:
            with open(Dlevel_Advanced_info, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                for data in dLevelInfo:
                    writer.writerow(data)
            print("DLevelAdvancedInfo has been Written to: " + Dlevel_Advanced_info)
            logging.debug("DLevelAdvancedInfo has been Written to: " + Dlevel_Advanced_info)

            # Uploading the generated CSV to Dropbox
            dropbox_path = f"/NSEBSEBhavcopy/ValueStocks/{Dlevel_Advanced_info}"  # Adjust the Dropbox folder path as needed
            dropboxClient.upload_file(Dlevel_Advanced_info, dropbox_path)
            print("DLevelAdvancedInfo : " + Dlevel_Advanced_info +" has been Uploaded to DropBox at : " + dropbox_path)
            logging.debug("DLevelAdvancedInfo : " + Dlevel_Advanced_info +" has been Uploaded to DropBox at : " + dropbox_path)
        else:
            print("No data to write for Advanced Info CSV")
            logging.debug("No data to write for Advanced Info CSV")
            status_code=False

    except IOError:
        print("I/O error while writing to " + Dlevel_Advanced_info)
        logging.debug("I/O error while writing to " + Dlevel_Advanced_info)
        status_code=False

    # Handle failures (if any) for logging purposes
    try:
        if len(dLevelInfoFailure) > 0:
            with open(Dlevel_Failed_Info, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=["SYMBOL", "NAME", "DLEVEL_KEY"])
                writer.writeheader()
                for data in dLevelInfoFailure:
                    writer.writerow(data)
            logging.debug("Dlevel_Failed_Info has been Written to: " + Dlevel_Failed_Info)
    except IOError:
        logging.debug("I/O error while writing to " + Dlevel_Failed_Info)
    return status_code


def GenerateAmibrokerTlsForFundamentals(file_path):
    print("Starting the process of generating Amibroker TLS files.")
    logging.info("Starting the process of generating Amibroker TLS files.")
    
    # Define the column names based on the provided CSV structure
    column_names = [
        'DATE','SYMBOL', 'NAME', 'SECTOR', 'CMP', 'VALUATION', 'FAIRRANGE', 'PE', 'SECTORPE',
        'MARKETCAP', 'MKCAPTYPE', 'TREND', 'FUNDAMENTAL', 'MOMENTUM', 'DERATIO', 
        'PRICETOSALES', 'PLEDGE', 'QBS', 'QBS%', 'AGS', 'AGS%', 'VALUATION_DCF', 
        'VALUATION_GRAHAM', 'VALUATION_EARNING', 'VALUATION_BOOKVALUE', 'VALUATION_SALES'
    ]
    
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path, names=column_names, header=None)
        print(f"Successfully read the CSV file: {file_path}")
        logging.info(f"Successfully read the CSV file: {file_path}")
    except Exception as e:
        print(f"Error reading the CSV file: {file_path}. Exception: {e}")
        logging.error(f"Error reading the CSV file: {file_path}. Exception: {e}")
        return
    
    # Dictionary to map FUNDAMENTAL values to output filenames
    fundamentals_to_files = {
        "Good Financials": "Good Fundamentals.tls",
        "Great Financials": "Great Fundamentals.tls",
        "Moderate Financials": "Moderate Fundamentals.tls",
        "Poor Financials": "Poor Fundamentals.tls"
    }
    
    # Iterate over each fundamental type and write corresponding SYMBOL column to file
    for fundamental, output_file in fundamentals_to_files.items():
        try:
            filtered_df = df[df['FUNDAMENTAL'] == fundamental]
            
            # Extract the SYMBOL column and save to a .tls file
            filtered_df[['SYMBOL']].to_csv(output_file, index=False, header=False)
            print(f"Wrote {len(filtered_df)} symbols to {output_file}")
            logging.info(f"Wrote {len(filtered_df)} symbols to {output_file}")
            dropbox_path = f"/NSEBSEBhavcopy/Amibroker_Watchlists/{output_file}"  # Adjust the Dropbox folder path as needed
            dropboxClient.upload_file(output_file, dropbox_path)
            print(f'{output_file} Uploaded to Dropbox at : {dropbox_path}')
            logging.info(f'{output_file} Uploaded to Dropbox at : {dropbox_path}')
        except Exception as e:
            print(f"Error writing to file: {output_file}. Exception: {e}")
            logging.error(f"Error writing to file: {output_file}. Exception: {e}")
    
    # Create the "Great and Good Fundamentals" file
    try:
        output_file="Great and Good Fundamentals.tls"
        great_and_good_df = df[df['FUNDAMENTAL'].isin(['Good Financials', 'Great Financials'])]
        great_and_good_df[['SYMBOL']].to_csv(output_file, index=False, header=False)
        print(f"Wrote {len(great_and_good_df)} symbols to {output_file}")
        logging.info(f"Wrote {len(great_and_good_df)} symbols to {output_file}")
        dropbox_path = f"/NSEBSEBhavcopy/Amibroker_Watchlists/{output_file}"  # Adjust the Dropbox folder path as needed
        dropboxClient.upload_file(output_file, dropbox_path)
        print(f'{output_file} Uploaded to Dropbox at : {dropbox_path}')
        logging.info(f'{output_file} Uploaded to Dropbox at : {dropbox_path}')
    except Exception as e:
        print(f"Error writing to Great and Good Fundamentals.tls. Exception: {e}")
        logging.error(f"Error writing to Great and Good Fundamentals.tls. Exception: {e}")
    
    print("Files created successfully.")
    logging.info("Process completed successfully.")

def ImportValueStocksToSqlLiteDB(csv_file_path,db_file_path):
    dtype={'DATENUM':int,'DATE':str,'SYMBOL':str,'NAME':str,'SECTOR':str,'CMP':float,'VALUATION':str,'FAIRRANGE':str,'PE':float,'SECTORPE':float,'MARKETCAP':float,'MKCAPTYPE':str,'TREND':str,'FUNDAMENTAL':str,'MOMENTUM':str,'DERATIO':float,'PRICETOSALES':float,
    'PLEDGE':float,'QBS':str,'QBS%':str,'AGS':str,'AGS%':str,'VALUATION_DCF':float,'VALUATION_GRAHAM':float,'VALUATION_EARNING':float,'VALUATION_BOOKVALUE':float,'VALUATION_SALES':float}
    # Load the CSV data
    csv_data = pd.read_csv(csv_file_path,dtype=dtype)
    csv_data['FUNDAMENTAL']=csv_data['FUNDAMENTAL'].fillna('')
    csv_data['SECTOR']=csv_data['SECTOR'].fillna('')
    csv_data['VALUATION']=csv_data['VALUATION'].fillna('')
    csv_data['MKCAPTYPE']=csv_data['MKCAPTYPE'].fillna('')
    csv_data['TREND']=csv_data['TREND'].fillna('')
    csv_data['MOMENTUM']=csv_data['MOMENTUM'].fillna('')
    
    

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    # Insert DATE_ID into VS_META_IMPORTDATE and fetch the ID for use in VS_IMPORT table
    for index, row in csv_data.iterrows():
        
        datenum = row['DATENUM']
        date = row['DATE']

        # Insert the date into VS_META_IMPORTDATE
        cursor.execute(
            """
            INSERT OR IGNORE INTO VS_META_IMPORTDATE (DATENUM, DATE)
            VALUES (?, ?)
            """,
            (datenum, date)
        )

        # Fetch the DATE_ID for the inserted/updated date
        cursor.execute("SELECT ID FROM VS_META_IMPORTDATE WHERE DATENUM = ?", (datenum,))
        date_id = cursor.fetchone()[0]

        # Handle SYMBOL and COMPANY_NAME
        symbol = row['SYMBOL']
        company_name = row['NAME']
        print("Processing for " + symbol + " "+company_name)
        # Insert into VS_META_STOCKINFO if not exists
        cursor.execute(
            """
            INSERT OR IGNORE INTO VS_META_STOCKINFO (SYMBOL_ID, NAME)
            VALUES (?, ?)
            """,
            (symbol, company_name)
        )

        # Fetch the STOCK_ID for the inserted/updated stock info
        cursor.execute("SELECT ID FROM VS_META_STOCKINFO WHERE SYMBOL_ID = ?", (symbol,))
        stock_id = cursor.fetchone()[0]

        # Handle SECTOR
        sector = row['SECTOR']
        if(sector != ''):
            # Insert into VS_META_SECTOR if not exists
            cursor.execute(
                """
                INSERT OR IGNORE INTO VS_META_SECTOR (SECTOR_NAME)
                VALUES (?)
                """,
                (sector,)
            )

            # Fetch the SECTOR_ID for the inserted/updated sector
            cursor.execute("SELECT ID FROM VS_META_SECTOR WHERE SECTOR_NAME = ?", (sector,))
            sector_id = cursor.fetchone()[0]
        else:
            sector_id = 1
            
        # Handle VALUATION
        valuation = row['VALUATION']
        if(valuation != ''):
            # Insert into VS_META_VALUATION if not exists
            cursor.execute(
                """
                INSERT OR IGNORE INTO VS_META_VALUATION (VALUATION)
                VALUES (?)
                """,
                (valuation,)
            )

            # Fetch the VALUATION_ID for the inserted/updated valuation
            cursor.execute("SELECT ID FROM VS_META_VALUATION WHERE VALUATION = ?", (valuation,))
            valuation_id = cursor.fetchone()[0]
        else:
            valuation_id = 1
            
        # Handle MKCAPTYPE
        mkcaptype = row['MKCAPTYPE']
        if(mkcaptype != ''):
            # Insert into VS_META_MARKETCAPTYPE if not exists
            cursor.execute(
                """
                INSERT OR IGNORE INTO VS_META_MARKETCAPTYPE (MARKETCAPTYPE)
                VALUES (?)
                """,
                (mkcaptype,)
            )

            # Fetch the MKCAPTYPE_ID for the inserted/updated market cap type
            cursor.execute("SELECT ID FROM VS_META_MARKETCAPTYPE WHERE MARKETCAPTYPE = ?", (mkcaptype,))
            mkcaptype_id = cursor.fetchone()[0]
        else:
            mkcaptype_id = 1
            
        # Handle TREND
        trend = row['TREND']
        if(trend != ''):
            # Insert into VS_META_TREND if not exists
            cursor.execute(
                """
                INSERT OR IGNORE INTO VS_META_TREND (TREND)
                VALUES (?)
                """,
                (trend,)
            )

            # Fetch the TREND_ID for the inserted/updated trend
            cursor.execute("SELECT ID FROM VS_META_TREND WHERE TREND = ?", (trend,))
            trend_id = cursor.fetchone()[0]
        else:
            trend_id=1
        
        # Handle FUNDAMENTAL
        fundamental = row['FUNDAMENTAL']
        #if(company_name == 'Future Enterprises Limited'):
        #    #print(row['FUNDAMENTAL'].dtype)
        #    print(row['FUNDAMENTAL'] is None)
        #    print(math.isnan(row['FUNDAMENTAL']))
        #    print(row)
        #    print(csv_data.dtypes)
        if(fundamental != ''):
            # Insert into VS_META_FUNDAMENTAL if not exists
            cursor.execute(
                """
                INSERT OR IGNORE INTO VS_META_FUNDAMENTAL (FUNDAMENTAL)
                VALUES (?)
                """,
                (fundamental,)
            )

            # Fetch the FUNDAMENTAL_ID for the inserted/updated fundamental
            cursor.execute("SELECT ID FROM VS_META_FUNDAMENTAL WHERE FUNDAMENTAL = ?", (fundamental,))
            fundamental_id = cursor.fetchone()[0]
        else:
            fundamental_id = 1
        
        # Handle MOMENTUM
        momentum = row['MOMENTUM']
        if(momentum != ''):
            # Insert into VS_META_MOMEMTUM if not exists
            cursor.execute(
                """
                INSERT OR IGNORE INTO VS_META_MOMEMTUM (MOMEMTUM)
                VALUES (?)
                """,
                (momentum,)
            )

            # Fetch the MOMEMTUM_ID for the inserted/updated momentum
            cursor.execute("SELECT ID FROM VS_META_MOMEMTUM WHERE MOMEMTUM = ?", (momentum,))
            momentum_id = cursor.fetchone()[0]
        else:
            momentum_id = 1
        
        # Insert data into VS_IMPORT table
        cursor.execute(
            """
            INSERT INTO VS_IMPORT (IMPORT_DATE_ID,SYMBOL_ID,SECTOR_ID,CMP,VALUATION_ID,
                              FAIR_RANGE,PE,SECTOR_PE,MARKET_CAP,MARKETCAPTYPEID,TREND_ID,
                              FUNDAMENTAL_ID,MOMEMTUM_ID,DERATIO,PRICETOSALES,PLEDGE,QBS,
                              [QBS%],AGS,[AGS%],VALUATION_DCF,VALUATION_GRAHAM,VALUATION_EARNING,VALUATION_BOOKVALUE,VALUATION_SALES)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                date_id,stock_id,sector_id,row['CMP'],valuation_id,
                row['FAIRRANGE'],row['PE'],row['SECTORPE'],row['MARKETCAP'],mkcaptype_id,trend_id,
                fundamental_id,momentum_id,row['DERATIO'],row['PRICETOSALES'],row['PLEDGE'],row['QBS'],
                row['QBS%'],row['AGS'],row['AGS%'],row['VALUATION_DCF'],row['VALUATION_GRAHAM'],row['VALUATION_EARNING'],row['VALUATION_BOOKVALUE'],row['VALUATION_SALES']
            )
        )

    # Commit the transaction and close the connection
    conn.commit()
    conn.execute("VACUUM")
    conn.close()


    
    
#row={"SYMBOL":"LTIM","NAME":"LTIMindtree Limited","DLEVEL_KEY":"lti_is_equity"}
#GetStockAdvancedInfoFromDLevels1(row)
global dropboxClient
dropboxClient=DropboxClient()
session = requests.Session()
#BuildAndSaveDLevelBasicInfo()
now = datetime.datetime.now()
Dlevel_Advanced_info = now.strftime("%Y%m%d-%H%M%S") + '-3.DLEVEL_ADVANCED_INFO.CSV'
Dlevel_Failed_Info = now.strftime("%Y%m%d-%H%M%S") + "-3.DLEVEL_ADVANCED_INFO_FAILURE.CSV"
BuildAndSaveAdvancedDLevelInfo(Dlevel_Advanced_info,Dlevel_Failed_Info)

# Test Code Below to download the File From DropBox and Build Amibroker TLS
#dropboxClient.download_file("/NSEBSEBhavCopy/ValueStocks/20250118-193932-3.DLEVEL_ADVANCED_INFO.CSV")
#Dlevel_Advanced_info="20250118-193932-3.DLEVEL_ADVANCED_INFO.CSV"
GenerateAmibrokerTlsForFundamentals(Dlevel_Advanced_info)

# Update the SQL Lite DB with the Information
print("About to Update the SQL Lite DB with the ValueStocks Info")
dropboxClient.download_file("/NSEBSEBhavcopy/ValueStocks/SQLLiteDB/ValueStocksDB.db","ValueStocksDB.db");
ImportValueStocksToSqlLiteDB(Dlevel_Advanced_info,"ValueStocksDB.db")
dropboxClient.upload_file("ValueStocksDB.db","/NSEBSEBhavcopy/ValueStocks/SQLLiteDB/ValueStocksDB.db");
print("SQLLite DB Uploaded to /NSEBSEBhavcopy/ValueStocks/SQLLiteDB/ValueStocksDB.db")

logging.shutdown()  # Flush and close the log file
# Get the current time in IST
ist = timezone('Asia/Kolkata')
log_file_path = os.path.abspath("ValueStocksProcess.Log")
print(f'Logfile is located locally at : {log_file_path}')
logFileNameInDropBox=f'/NSEBSEBhavcopy/Logs/{datetime.strftime(datetime.now(ist),'%Y-%m-%d %H-%M-%S').upper()}-ValueStocksProcess.Log'
dropBoxClient.upload_file(log_file_path,logFileNameInDropBox)
print(f'Log File have been Uploaded to {logFileNameInDropBox}.')

