import pandas as pd
import io
from io import BytesIO
from io import StringIO
from jugaad_data.nse import NSELive
import json
import ast
import os
from os.path import exists
import csv
import progressbar
from lxml import html
import copy
import time
from bs4 import BeautifulSoup
from tqdm import tqdm
import requests
from requests import Session
import urllib3
import datetime
from datetime import datetime,timedelta
import glob
import shutil
import urllib
import zipfile
import logging
import tempfile
import PortfolioUpdate
import dropbox
from DropboxClient import DropboxClient
from pytz import timezone
from BseHelper import BseHelper


logging.basicConfig(filename="NSEBSEBhavCopyDownload.Log",level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S')

pd.options.mode.chained_assignment = None  # default='warn'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}


# Folder path on Dropbox
DROPBOX_FOLDER_PATH = '/NSEBSEBhavCopy/ValueStocks'

def GetMostRecentValueStocksDataFile():
    try:
        global dropBoxClient
        recentValueStockFile=dropBoxClient.get_most_recent_file('/NSEBSEBhavCopy/ValueStocks')
        dropBoxClient.download_file(recentValueStockFile)
        print(f"Successfully Downloaded : {recentValueStockFile}")
    except Exception as e:
        print(f"Error getting MostRecentValueStocksDataFile from Dropbox : {e}")
        return None

def isUrlValid(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        logger.debug("Checking for the Validity of the Url :"+url)
        r=requests.get(url, allow_redirects=True,headers=headers)
        if(r.status_code==200):
            logger.debug("GET : "+url +" Returned Status Code : 200 OK")
            return True
        else:
            #print(r.status_code)
            logger.debug("GET : "+url +" Returned Failire Status Code : "+str(r.status_code))
            return False
    except Exception as e:
        logger.debug("GET : "+url +" Resulted Exception "+ str(e))
        return False
    else:
        return True

def GetValueStockInputFile():
    #StockDDataPath="C:\\VijayDas\\SamsungT5\\Finance\\BhavCopyArchive\\StockD\\"
    #InputFilesList=dir_list = glob.glob(StockDDataPath+'ALL_*.txt')
    #ValueStocksDataPath="C:\\VijayDas\\SamsungT5\\Finance\\Scripts\\ValueStocks\\"
    ValueStocksDataFiles=glob.glob('*3.DLEVEL_ADVANCED_INFO.CSV')
    ValueStocksDataFiles.sort(reverse=True)
    ValueStocksDataFile=ValueStocksDataFiles[0]
    logger.debug("Returned ValueStock Input File : " + ValueStocksDataFile)
    return ValueStocksDataFile

def GetNseEquityListDF():
    try:    
        NSE_Equity_List_csv_url="https://archives.nseindia.com/content/equities/EQUITY_L.csv"
        temp_dir = tempfile.gettempdir()
        nse_Master_Equity_List_File=os.path.join(temp_dir, datetime.strftime(datetime.today(),'%Y%m%d-').upper()+'NSE_EQUITY_L.csv')
        file_exists = exists(nse_Master_Equity_List_File)
        if(file_exists):
            logger.debug("NSE Equity List File found at :"+nse_Master_Equity_List_File)
        else:
            logger.debug("NSE Equity List File not found at :"+nse_Master_Equity_List_File+". Hence Downloading")
            req = requests.get(NSE_Equity_List_csv_url)
            url_content = req.content
            csv_file = open(nse_Master_Equity_List_File, 'wb')
            csv_file.write(url_content)
            csv_file.close()
            logger.debug("NSE Equity List File Saved at :"+nse_Master_Equity_List_File)
            df=pd.read_csv(nse_Master_Equity_List_File)
            return df
    except Exception as e:
        logger.exception("ERROR Occured  having the details as : ")

def GetAdditionalData(NseStockCode,retry=0):
    global nselive
    quotesJson=''
    for x in range(3):
        try:
            logger.debug("About To FETCH  Data For SYMBOL: "+NseStockCode + " using JUGAAD-DATA")
            quotesJson = nselive.stock_quote(NseStockCode)
            logger.debug("Retrieved Stock_Quotes for SYMBOL: "+NseStockCode + " from JUGAAD-DATA")
            logger.debug("Stock_Quotes for SYMBOL: "+NseStockCode + " : "+ str(quotesJson))
            issuedSize = quotesJson['securityInfo']['issuedSize']
            closePrice = 0
            if(quotesJson['priceInfo']['close']>0):
                    closePrice = quotesJson['priceInfo']['close']
            elif(quotesJson['priceInfo']['lastPrice']>0):
                closePrice = quotesJson['priceInfo']['lastPrice']
                    
            return {
                'MACRO': quotesJson['industryInfo']['macro'],
                'SECTOR': quotesJson['industryInfo']['sector'],
                'INDUSTRY': quotesJson['industryInfo']['basicIndustry'],
                'ISSUEDSIZE': issuedSize,
                'FULLMARKETCAP': int(issuedSize * closePrice)
                }
        except Exception as e:
            logger.debug(f"Due to an Exception, Sleeping for 5 mins will establish the connection again and proceed with download. [Exception = {str(e)}],  [quotesJson={quotesJson}]") 
            #del nselive
            #time.sleep(180)
            #nselive = NSELive()
    logger.debug(f"Retried for 3 times but Still Exception Occurs for Symbol {NseStockCode}. Hence Returning an Empty Data") 
    return {
            'MACRO': 'NOMACRO',
            'SECTOR': 'NOSECTOR',
            'INDUSTRY': 'NOINDUSTRY',
            'ISSUEDSIZE': 0,
            'FULLMARKETCAP': 0.00
            }
def GetMasterNSEData():
    try:
        global dropBoxClient
        temp_dir = tempfile.gettempdir()
        NseMasterDataForToday=os.path.join(temp_dir, datetime.strftime(datetime.today(),'%Y%m%d-').upper()+'NSEMASTERDATA.csv')
        NseMasterDataForTodayinDropBox=f'/nsebsebhavcopy/DailyBhavCopy/Temp/{datetime.strftime(datetime.today(),'%Y%m%d-').upper()}NSEMASTERDATA.csv'
        if( dropBoxClient.file_exists(NseMasterDataForTodayinDropBox)):
            dropBoxClient.download_file(NseMasterDataForTodayinDropBox,NseMasterDataForToday)
        file_exists = exists(NseMasterDataForToday)
        if(file_exists):
            logger.debug("NSE Master Data File found at :"+NseMasterDataForToday+", Hence no need to Build. Just return the Dataframe")
        else:
            bseHelper= BseHelper() #['SYMBOL', 'FULLNAME', 'ISIN_NUMBER', 'INDUSTRYNAME', 'SECTORNAME', 'MARKETCAP']
            dfBseScripList=bseHelper.GetAllBseScrips()
            dfBseScripList=df.rename(columns={"INDUSTRYNAME": "INDUSTRY","SECTORNAME":"SECTOR","MARKETCAP":"FULLMARKETCAP"})
			
            df=GetNseEquityListDF()
            df=df[['SYMBOL','ISIN NUMBER','NAME OF COMPANY']]
            df=df.rename(columns={"NAME OF COMPANY": "FULLNAME","ISIN NUMBER": "ISIN_NUMBER"})
            df=df[~df['SYMBOL'].str.endswith('-RE')]
            #df=df.iloc[0:10].copy()
            #df.reset_index(drop=True,inplace=True)
            df['MACRO'] = 'NOMACRO'
            df['SECTOR'] = 'NOSECTOR'
            df['INDUSTRY'] = 'NOINDUSTRY'
            df['ISSUEDSIZE'] = 0
            df['FULLMARKETCAP'] = 0.00
			
            dfMerged = pd.merge(df,dfBseScripList, on='ISIN_NUMBER', how='left',suffixes=('', '_new'))
            df_merged['INDUSTRY'] = df_merged['INDUSTRY_new'].fillna(df_merged['INDUSTRY'])
            df_merged['SECTOR'] = df_merged['SECTOR_new'].fillna(df_merged['SECTOR'])
            df_merged['FULLMARKETCAP'] = df_merged['FULLMARKETCAP_new'].fillna(df_merged['FULLMARKETCAP'])
			
            df_final = df_merged.drop(columns=['INDUSTRY_new', 'SECTOR_new', 'FULLMARKETCAP_new'])
            
            
            df_final.columns = ['SYMBOL','FULLNAME','MACRO','SECTOR','INDUSTRY','ISSUEDSIZE','FULLMARKETCAP']
            df_final.to_csv(NseMasterDataForToday, header = True,index = False)
            logger.debug("NSE Master Data File Saved at :"+NseMasterDataForToday)
            dropBoxClient.upload_file(NseMasterDataForToday,NseMasterDataForTodayinDropBox)
        df=pd.read_csv(NseMasterDataForToday)
        return df
    except Exception as e:
        logger.exception("ERROR: An Error Occured while Building the NSE Master Data.")
        
def GetMasterNSEData_OLD():
    try:
        global dropBoxClient
        temp_dir = tempfile.gettempdir()
        NseMasterDataForToday=os.path.join(temp_dir, datetime.strftime(datetime.today(),'%Y%m%d-').upper()+'NSEMASTERDATA.csv')
        NseMasterDataForTodayinDropBox=f'/nsebsebhavcopy/DailyBhavCopy/Temp/{datetime.strftime(datetime.today(),'%Y%m%d-').upper()}NSEMASTERDATA.csv'
        if( dropBoxClient.file_exists(NseMasterDataForTodayinDropBox)):
            dropBoxClient.download_file(NseMasterDataForTodayinDropBox,NseMasterDataForToday)
        file_exists = exists(NseMasterDataForToday)
        if(file_exists):
            logger.debug("NSE Master Data File found at :"+NseMasterDataForToday+", Hence no need to Build. Just return the Dataframe")
        else:
            df=GetNseEquityListDF()
            df=df[['SYMBOL','NAME OF COMPANY']]
            df=df.rename(columns={"NAME OF COMPANY": "FULLNAME"})
            df=df[~df['SYMBOL'].str.endswith('-RE')]
            #df=df.iloc[0:10].copy()
            #df.reset_index(drop=True,inplace=True)
            df['MACRO'] = 'NOMACRO'
            df['SECTOR'] = 'NOSECTOR'
            df['INDUSTRY'] = 'NOINDUSTRY'
            df['ISSUEDSIZE'] = 0
            df['FULLMARKETCAP'] = 0.00
            
            # Initialize tqdm progress bar
            #tqdm.pandas(desc="Building NSE Master Data for Stocks")
            widgets = [' [',progressbar.Timer(format= 'Building NSE Master Data for Stocks: %(elapsed)s'),'] ', progressbar.Bar('*'),' (',progressbar.Counter(format='%(value)02d/%(max_value)d'), ') ',]
            bar = progressbar.ProgressBar(max_value=len(df),widgets=widgets).start()
            progressCounter=0
            # Process each symbol and update DataFrame
            for index, row in df.iterrows():
                try:
                    symbol=row['SYMBOL']
                    logger.debug("About To Process  SEQ: "+str(progressCounter) + " SYMBOL: "+symbol)
                    stock_data = GetAdditionalData(symbol)
                    logger.debug("Retrieved Additional Data for SEQ:  "+str(progressCounter) + " SYMBOL: "+symbol + " Additional Data= " + str(stock_data))
                    df.at[index, 'MACRO'] = stock_data['MACRO']
                    df.at[index, 'SECTOR'] = stock_data['SECTOR']
                    df.at[index, 'INDUSTRY'] = stock_data['INDUSTRY']
                    df.at[index, 'ISSUEDSIZE'] = stock_data['ISSUEDSIZE']
                    df.at[index, 'FULLMARKETCAP'] = stock_data['FULLMARKETCAP']
                    logger.debug("Updated the Additional Data for SEQ: "+str(progressCounter) + " SYMBOL: "+symbol)
                    logger.debug("Updated Row is : " + str(df.loc[index]))
                finally:
                    progressCounter+=1
                    bar.update(progressCounter)
                    if(progressCounter % 150 == 0):
                        global nselive
                        del nselive
                        time.sleep(10)
                        nselive = NSELive()
                        
            df.columns = ['SYMBOL','FULLNAME','MACRO','SECTOR','INDUSTRY','ISSUEDSIZE','FULLMARKETCAP']
            df.to_csv(NseMasterDataForToday, header = True,index = False)
            logger.debug("NSE Master Data File Saved at :"+NseMasterDataForToday)
            dropBoxClient.upload_file(NseMasterDataForToday,NseMasterDataForTodayinDropBox)
        df=pd.read_csv(NseMasterDataForToday)
        return df
    except Exception as e:
        logger.exception("ERROR: An Error Occured while Building the NSE Master Data.")

def DownloadNSEBhavCopy(dateRange):
    for tday in dateRange:
        try:
            dMMyFormatUpperCase = datetime.strftime(tday,'%d%b%Y').upper()
            filenamedate = datetime.strftime(tday,'%Y-%m-%d').upper()
            monthUppercase = datetime.strftime(tday,'%b').upper()
            year = datetime.strftime(tday,'%Y')
            YYYYMMMddFormatForBhavCopy=datetime.strftime(tday,'%Y%m%d') # 20240708 for 08-Jul-2024
            # https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_20240708_F_0000.csv.zip
            url_bhav = 'https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_'+YYYYMMMddFormatForBhavCopy+'_F_0000.csv.zip'
            logger.debug("NSE BhavCopy Url Formed : " + url_bhav)
            if(isUrlValid(url_bhav)):
                #1. Download the Delivery Data from NSE for a Specific Day.
                #2. Download the NSE BhavCopy  from NSE for a Specific Day.
                #3. Merge the Delivery Details with NSE BhavCopy
                #4. Merge the BhavCopy with ValueStocks Details.
                dmyformat = datetime.strftime(tday,'%d%m%Y')
                logger.debug("1. Download the Delivery Data from NSE for a Specific Day.")
                #https://nsearchives.nseindia.com/archives/equities/mto/MTO_08072024.DAT
                url_dlvry ='https://nsearchives.nseindia.com/archives/equities/mto/MTO_'+ dmyformat + '.DAT'
                logger.debug("NSE Delivery Data Url Formed : " + url_dlvry)
                
                if not isUrlValid(url_dlvry):
                    print("NSE Delivery Data Url seems not Valid. Url : " + url_dlvry)
                    logger.debug("ERROR: NSE Delivery Data Url seems not Valid. Url : " + url_dlvry)
                    continue
                #r = requests.get(url_dlvry,timeout = 3,allow_redirects=True,headers=headers).content
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
                r = requests.get(url_dlvry,allow_redirects=True,headers=headers).content
                deliveryDf = pd.read_csv(io.StringIO(r.decode('utf-8')),skiprows=3)
                deliveryDf = deliveryDf[ deliveryDf['Name of Security'] == 'EQ']
                deliveryDf = deliveryDf.rename(columns={"Sr No": "SYMBOL",'Deliverable Quantity(gross across client level)':'TOTTRDQTY'})
                deliveryDf.drop(['Record Type', 'Name of Security', 'Quantity Traded','% of Deliverable Quantity to Traded Quantity'], inplace=True, axis=1)
                logger.debug("NSE Delivery Data Parsed Into Dataframe Successfully")
                
                logger.debug("2. Download the NSE BhavCopy  from NSE for a Specific Day.")
                
                r = requests.get(url_bhav,allow_redirects=True,headers=headers).content
                zip_file = zipfile.ZipFile(BytesIO(r))
                file_list = zip_file.namelist()
                with zip_file.open(file_list[0]) as file:
                    nseBhavCopyDf = pd.read_csv(file,parse_dates=['TradDt'])
                nseBhavCopyDf= nseBhavCopyDf[nseBhavCopyDf.SctySrs.isin(["EQ","BE"])]
                timestampForDF=datetime.strftime(tday,'%Y%m%d')
                nseBhavCopyDf['TIMESTAMP']=timestampForDF
                nseBhavCopyDf = nseBhavCopyDf[['TckrSymb','TIMESTAMP','OpnPric','HghPric','LwPric','ClsPric','TtlTradgVol']]
                nseBhavCopyDf=nseBhavCopyDf.rename(columns={"TckrSymb": "SYMBOL",'TIMESTAMP':'DATE_YMD','OpnPric':'OPEN','HghPric':'HIGH','LwPric':'LOW','ClsPric':'CLOSE','TtlTradgVol':'TOTTRDQTY'})
                logger.debug("NSE Bhavcopy Data Parsed Into Dataframe Successfully")
                logger.debug("3. Merge the Delivery Details with NSE BhavCopy")
                nseBhavCopyDf = nseBhavCopyDf.merge(deliveryDf, on='SYMBOL', how='left')
                nseBhavCopyDf['TOTTRDQTY_y']=nseBhavCopyDf['TOTTRDQTY_y'].fillna(nseBhavCopyDf['TOTTRDQTY_x'])
                nseBhavCopyDf.drop(['TOTTRDQTY_x'],inplace=True, axis=1)
                nseBhavCopyDf=nseBhavCopyDf.rename(columns={"TOTTRDQTY_y": "VOLUME"})
                logger.debug("3. Merge the Delivery Details with NSE BhavCopy, Completed Successfully")
                
                # Enriching with ValueStocks Data
                logger.debug("#4. Merge the BhavCopy with ValueStocks Details.")
                ValueStockDataCsvFile=GetValueStockInputFile()
                dfValueStocksInfo = pd.read_csv(ValueStockDataCsvFile)
                dfValueStocksInfoFiltered=dfValueStocksInfo[['SYMBOL','FUNDAMENTAL','VALUATION','MKCAPTYPE']]
                dfMergedBhavCopyWithValueStocksInfo=pd.merge(nseBhavCopyDf, dfValueStocksInfoFiltered, on ='SYMBOL', how ="left")
                dfMergedBhavCopyWithValueStocksInfo = dfMergedBhavCopyWithValueStocksInfo.rename(columns={'FUNDAMENTAL':'ALIAS','VALUATION':'ADDRESS','MKCAPTYPE':'COUNTRY'})
                logger.debug("#4. Merge the BhavCopy with ValueStocks Details. Completed Successfully")
                

                # Enriching the Data With Additional Details from NSE
                dfMergedBhavCopyWithValueStocksInfo["CURRENCY"]=""
                dfMergedBhavCopyWithValueStocksInfo["OPENINT"]=0
                dfFinalBhavcopy=dfMergedBhavCopyWithValueStocksInfo[dfMergedBhavCopyWithValueStocksInfo.columns]
                if(tday.date() <= datetime.today().date()):
                    dfMasterNseData=GetMasterNSEData()
                    dfFinalBhavcopy = pd.merge(dfFinalBhavcopy, dfMasterNseData, on ='SYMBOL', how ="left")
                    logger.debug(dfFinalBhavcopy[dfFinalBhavcopy['SYMBOL']=="AARTIIND"])
                    #logger.debug(dfFinalBhavcopy.columns)
                    dfFinalBhavcopy=dfFinalBhavcopy.rename(columns={"SYMBOL":"TICKER","INDUSTRY":"INDUSTRYNAME","SECTOR":"SECTORNAME","ISSUEDSIZE":"AUX1","FULLMARKETCAP":"AUX2"})
                    logger.debug(dfFinalBhavcopy[dfFinalBhavcopy['TICKER']=="AARTIIND"])
                    dfFinalBhavcopy=dfFinalBhavcopy[['DATE_YMD','TICKER','FULLNAME','OPEN','HIGH','LOW','CLOSE','VOLUME','INDUSTRYNAME','SECTORNAME','ALIAS','ADDRESS','COUNTRY','CURRENCY','OPENINT','AUX1','AUX2']]
                    logger.debug(dfFinalBhavcopy[dfFinalBhavcopy['TICKER']=="AARTIIND"])
                else:
                    # set with Empty Values.
                    dfFinalBhavcopy["FULLNAME"]=""
                    dfFinalBhavcopy["INDUSTRYNAME"]=""
                    dfFinalBhavcopy["SECTORNAME"]=""
                    dfFinalBhavcopy["AUX2"]=0.00
                    dfFinalBhavcopy["AUX1"]=0
                #filename = filenamedate + '-NSE-EQ.csv'
                #dfFinalBhavcopy.to_csv(filename, header = True,index = False,date_format='%Y%m%d' )
                
                #print(datetime.strftime(tday,'%d-%b-%Y').upper() + ":   ==>  "+filename + "   [Done]")
                #dfFinalBhavcopy=pd.read_csv(filename)
                return dfFinalBhavcopy
            else:
                print(datetime.strftime(tday,'%d-%b-%Y').upper() + ":   ==>  No Data. Non-Trading Day")
        except Exception as e:
            logger.exception("ERROR : An Exception Occured while downloading NSE BhavCopy : ")
            

def BuildNseSectoralAndIndustryBhavCopy():
    try:
        # Generate the output file name with the current date
        current_date = datetime.now().strftime('%Y-%m-%d')
        output_csv_file_path = f'{current_date}-NSE_IS.csv'
            
        file_exists = exists(output_csv_file_path)
        if(file_exists):
            logger.debug("NSE Sectoral and Industry Data File found at :"+output_csv_file_path+", Hence no need to Build. Just return the Dataframe")
        else:
            df=GetMasterNSEData()
            
            logger.debug(df)
            # Create the summary by MACRO
            summary_macro = df.groupby('MACRO')['FULLMARKETCAP'].sum().reset_index()
            summary_macro['TICKER'] = summary_macro['MACRO'].apply(lambda x: f'MACRO: {x}')
            summary_macro['SECTORNAME'] = 'Macros'
            summary_macro['INDUSTRYNAME'] = 'Macros'
            summary_macro['FULLNAME'] = summary_macro['MACRO']
            summary_macro = summary_macro[['TICKER', 'FULLNAME', 'FULLMARKETCAP','SECTORNAME','INDUSTRYNAME']]
            logger.debug(summary_macro)
            logger.debug("SUMMARY OF DATA by MACRO Completed")
            
            # Create the summary by SECTOR
            summary_sector = df.groupby('SECTOR')['FULLMARKETCAP'].sum().reset_index()
            logger.debug(summary_sector)
            summary_sector['TICKER'] = summary_sector['SECTOR'].apply(lambda x: f'SECTOR: {x}')
            summary_sector['SECTORNAME'] = 'Macros'
            summary_sector['INDUSTRYNAME'] = 'Sectors'
            summary_sector['FULLNAME'] = summary_sector['SECTOR']
            summary_sector = summary_sector[['TICKER', 'FULLNAME','FULLMARKETCAP','SECTORNAME','INDUSTRYNAME']]
            logger.debug(summary_sector)
            logger.debug("SUMMARY OF DATA by SECTOR Completed")
            
            # Create the summary by INDUSTRY
            summary_industry = df.groupby(['INDUSTRY','SECTOR'])['FULLMARKETCAP'].sum().reset_index()
            summary_industry['TICKER'] = summary_industry['INDUSTRY'].apply(lambda x: f'INDUSTRY: {x}')
            summary_industry['SECTORNAME'] = summary_industry['SECTOR']
            summary_industry['INDUSTRYNAME'] = summary_industry['INDUSTRY']
            summary_industry['FULLNAME'] = summary_industry['INDUSTRY']
            summary_industry = summary_industry[['TICKER', 'FULLNAME','FULLMARKETCAP','SECTORNAME','INDUSTRYNAME']]
            logger.debug(summary_industry)
            logger.debug("SUMMARY OF DATA by INDUSTRY Completed")
            
            # Combine all summaries into one DataFrame
            combined_summary = pd.concat([summary_macro, summary_sector, summary_industry], ignore_index=True)
            #combined_summary.to_csv("CombinedSummary.csv", index=False)
            logger.debug(combined_summary)
            # Add the DATE column with the current date in YYYYMMDD format
            
            current_date = datetime.now().strftime('%Y%m%d')
            combined_summary.insert(0, 'DATE_YMD', current_date)
            # Add additional columns with specified values
            
            combined_summary['OPEN'] = (combined_summary['FULLMARKETCAP'] / 100000).round(2)
            combined_summary['HIGH'] = (combined_summary['FULLMARKETCAP'] / 100000).round(2)
            combined_summary['LOW'] = (combined_summary['FULLMARKETCAP'] / 100000).round(2)
            combined_summary['CLOSE'] = (combined_summary['FULLMARKETCAP'] / 100000).round(2)
            combined_summary['AUX2'] = combined_summary['FULLMARKETCAP']
            logger.debug("-----------------------------------------")
            # Set the values for these columns to be empty
            additional_columns = ['VOLUME', 'ALIAS','ADDRESS','COUNTRY','CURRENCY','OPENINT','AUX1']
            for column in additional_columns:
                combined_summary[column] = ''
            
            # Set the SECTOR column to the same as CLASSIFICATION for appropriate rows
            #combined_summary['SECTOR'] = combined_summary['SYMBOL']
            
            # Reorder the columns
            
            column_order = ['DATE_YMD','TICKER','FULLNAME','OPEN','HIGH','LOW','CLOSE','VOLUME','INDUSTRYNAME','SECTORNAME','ALIAS','ADDRESS','COUNTRY','CURRENCY','OPENINT','AUX1','AUX2']
            combined_summary = combined_summary[column_order]
            
            # Save the combined summary to a new CSV file
            #combined_summary.to_csv(output_csv_file_path, index=False)
            #print(datetime.now().strftime('%d-%b-%Y').upper() + ":   ==>  "+output_csv_file_path + "   [Done]")
            #logger.debug("NSE_SECTOR_IND_BHAVCOPY Successfully saved to " + output_csv_file_path)
            return combined_summary
        #df=pd.read_csv(output_csv_file_path)
        #return df
    except Exception as e:
        logger.exception("ERROR: Error Occured during Building NSE_SECTOR_IND_BHAVCOPY")
        
            
def GetBSEDeliveryData(date):
    logger.debug("Downloading the BSE Delivery Data for the date = " + str(date))
    try:    
        Bse_Delivery_Data_UrlFormat="https://www.bseindia.com/BSEDATA/gross/{year}/SCBSEALL{ddmm}.zip"
                                   # https://www.bseindia.com/BSEDATA/gross/2023/SCBSEALL0509.zip
        Bse_Delivery_Data_Url=Bse_Delivery_Data_UrlFormat.format(year=datetime.strftime(date,'%Y'),ddmm=datetime.strftime(date,'%d%m'))
        DeliveryDataFileName_Format="SCBSEALL{ddmm}.TXT"
        DeliveryDataFileNameInZIP=DeliveryDataFileName_Format.format(ddmm=datetime.strftime(date,'%d%m'))
        #print(Bse_Delivery_Data_Url)
        #headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
        logger.debug(" Bse Delivery Data Url : " +  Bse_Delivery_Data_Url)
        if(isUrlValid(Bse_Delivery_Data_Url)):
            r = requests.get(Bse_Delivery_Data_Url,timeout = 3,allow_redirects=True,headers=headers)
            unzipcsvfile= zipfile.ZipFile(io.BytesIO(r.content))
            #print(BhavCopyFileNameinZIP)
            bseDeliveryDf = pd.read_csv(unzipcsvfile.open(DeliveryDataFileNameInZIP),sep='|')
            bseDeliveryDf.columns = bseDeliveryDf.columns.str.replace('`', '\'')
            bseDeliveryDf=bseDeliveryDf.rename(columns={"DATE": "TIMESTAMP",'SCRIP CODE':'SYMBOL','DELIVERY QTY':'TOTTRDQTY','DELIVERY VAL':'DELIVERY_VAL','DAY\'S VOLUME':'DAYSVOLUME','DAY\'S TURNOVER':'DAYSTURNOVER','DELV. PER.':'DELVPER'})
            bseDeliveryDf.drop(['TIMESTAMP','DELIVERY_VAL','DAYSVOLUME','DAYSTURNOVER','DELVPER'],inplace=True, axis=1)
            logger.debug("Downloading the BSE Delivery Data for the date = " + str(date)+ " has been Successful and returned the Result.")
            return bseDeliveryDf
    except Exception as e:
        logger.debug("ERROR: An Exception Occured Details = "+str(e))
        
def DownloadBSEBhavCopy(dateRange):
    logger.debug("Downloading the BSE BhavCopy for the Date Range : "+str(dateRange))
    for tday in dateRange:
        try:
            dateForFilename= datetime.strftime(tday,'%Y-%m-%d').upper()
            Bse_BhavCopy_Url_Format="https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{YYYYMMDD}_F_0000.CSV"
            YYYYMMDD=datetime.strftime(tday,'%Y%m%d')
            timestampForDF=datetime.strftime(tday,'%Y%m%d')
            Bse_BhavCopy_Url=Bse_BhavCopy_Url_Format.format(YYYYMMDD=YYYYMMDD)
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
            
            if(isUrlValid(Bse_BhavCopy_Url)):
                #print(Bse_BhavCopy_Url + " Valid Url")
                r = requests.get(Bse_BhavCopy_Url,timeout = 3,allow_redirects=True,headers=headers).content
                bseBhavCopyDf = pd.read_csv(io.StringIO(r.decode('utf-8')))
                
                bseBhavCopyDf['TIMESTAMP']=timestampForDF
                bseBhavCopyDf = bseBhavCopyDf[['FinInstrmId','TIMESTAMP','OpnPric','HghPric','LwPric','ClsPric','TtlTradgVol','FinInstrmNm']]
                bseBhavCopyDf.columns = ['SYMBOL','TIMESTAMP','OPEN','HIGH','LOW','CLOSE','TOTTRDQTY','FinInstrmNm']
                bseDeliveryDf=GetBSEDeliveryData(tday)

                bseBhavCopyDf=bseBhavCopyDf.merge(bseDeliveryDf, on='SYMBOL', how='left')
                #print(bseBhavCopyDf.columns)
                bseBhavCopyDf['TOTTRDQTY_y']=bseBhavCopyDf['TOTTRDQTY_y'].fillna(bseBhavCopyDf['TOTTRDQTY_x'])
                bseBhavCopyDf.drop(['TOTTRDQTY_x'],inplace=True, axis=1)
                bseBhavCopyDf=bseBhavCopyDf.rename(columns={"TOTTRDQTY_y": "TOTTRDQTY"})
                bseBhavCopyDf["FULLNAME"]= bseBhavCopyDf['FinInstrmNm'].str.title()
                bseBhavCopyDf["SECTORNAME"]=''
                bseBhavCopyDf["INDUSTRYNAME"]=''
                bseBhavCopyDf["ALIAS"]=''
                bseBhavCopyDf["ADDRESS"]=''
                bseBhavCopyDf["COUNTRY"]=''
                bseBhavCopyDf["CURRENCY"]=''
                bseBhavCopyDf["OPENINT"]=0
                bseBhavCopyDf["AUX1"]=0
                bseBhavCopyDf["AUX2"]=0
                bseBhavCopyDf.columns = ['TICKER','DATE_YMD','OPEN','HIGH','LOW','CLOSE','VOLUME','FinInstrmNm','FULLNAME','INDUSTRYNAME','SECTORNAME','ALIAS','ADDRESS','COUNTRY','CURRENCY','OPENINT','AUX1','AUX2']
                column_order = ['DATE_YMD','TICKER','FULLNAME','OPEN','HIGH','LOW','CLOSE','VOLUME','INDUSTRYNAME','SECTORNAME','ALIAS','ADDRESS','COUNTRY','CURRENCY','OPENINT','AUX1','AUX2']
                bseBhavCopyDf=bseBhavCopyDf[column_order]
                #filename = dateForFilename + '-BSE-EQ.csv'
                #bseBhavCopyDf.to_csv(filename, header = True,index = False,date_format='%Y%m%d')
                #print(datetime.strftime(tday,'%d-%b-%Y').upper() + ":   ==>  "+filename + "   [Done]")
                #logger.debug("BSE BHAVCOPY for " + datetime.strftime(tday,'%d-%b-%Y').upper() + ":   ==>  "+filename + "   [Done]")
                return bseBhavCopyDf
            else:
                logger.debug("BSE BHAVCOPY for " + datetime.strftime(tday,'%d-%b-%Y').upper() + ":   ==>  No Data. Non-Trading Day " + Bse_BhavCopy_Url)
                print(datetime.strftime(tday,'%d-%b-%Y').upper() + ":   ==>  No Data. Non-Trading Day " + Bse_BhavCopy_Url)
        except Exception as e:
            logger.exception("ERROR: An Exception Occured ")

def DownloadNSEIndexBhavCopy(tday):
    NseIndexfileName="ind_close_all_{0}.csv".format(tday.strftime("%d%m%Y"))
    NseIndexSnapShopUrl="https://archives.nseindia.com/content/indices/{0}".format(NseIndexfileName)
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
    #r = requests.get(NseIndexSnapShopUrl, allow_redirects=True,headers=headers,timeout=1)
    try:
        r = requests.get(NseIndexSnapShopUrl,headers=headers)
        if r.status_code == 200:
            dfNseBhavCopy=pd.read_csv(io.StringIO(r.content.decode('utf-8')),parse_dates=["Index Date"], date_format="%d-%m-%Y")
            dfNseBhavCopy.columns=['TICKER', 'DATE_YMD1', 'OPEN', 'HIGH',
            'LOW', 'CLOSE', 'Points Change', 'Change(%)',
            'VOLUME', 'Turnover (Rs. Cr.)', 'P/E', 'P/B', 'Div Yield']
            dfNseBhavCopy=dfNseBhavCopy[['DATE_YMD1','TICKER','TICKER','OPEN', 'HIGH',
            'LOW', 'CLOSE','VOLUME']]
            dfNseBhavCopy.columns=['DATE_YMD1','TICKER', 'FULLNAME','OPEN', 'HIGH',
            'LOW', 'CLOSE','VOLUME']
            dfNseBhavCopy['DATE_YMD'] = dfNseBhavCopy['DATE_YMD1'].dt.strftime('%Y%m%d')
            dfNseBhavCopy['INDUSTRYNAME']="NSE Indices"
            dfNseBhavCopy['SECTORNAME']="Indices"
            dfNseBhavCopy['ALIAS']=''
            dfNseBhavCopy['ADDRESS']=''
            dfNseBhavCopy['COUNTRY']=''
            dfNseBhavCopy['CURRENCY']=''
            dfNseBhavCopy['OPENINT']=0.0
            dfNseBhavCopy['AUX1']=0.0
            dfNseBhavCopy['AUX2']=0.0
            column_order = ['DATE_YMD','TICKER','FULLNAME','OPEN','HIGH','LOW','CLOSE','VOLUME','INDUSTRYNAME','SECTORNAME','ALIAS','ADDRESS','COUNTRY','CURRENCY','OPENINT','AUX1','AUX2']
            return dfNseBhavCopy[column_order]
        else:
            logger.info("Error : "+NseIndexSnapShopUrl)
    except requests.exceptions.RequestException:
        # If a timeout or other exception occurs, print a timeout message
        logger.exception(f'Timeout occurred Probable holiday {tday}.')
        
def GetBSEindexDataBhavCopy():
    # Define the CURL command headers and URL
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.8",
        "origin": "https://www.bseindia.com",
        "priority": "u=1, i",
        "referer": "https://www.bseindia.com/",
        "sec-ch-ua": "^\"Chromium^\"^;v=^\"128^\"^, ^\"Not;A=Brand^\"^;v=^\"24^\"^, ^\"Brave^\"^;v=^\"128^\"^",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "^\"Windows^\"^",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "sec-gpc": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    }

    urls = ["https://api.bseindia.com/BseIndiaAPI/api/MktCapBoard_indstream/w?cat=1&type=2",
            "https://api.bseindia.com/BseIndiaAPI/api/MktCapBoard_indstream/w?cat=2&type=2",
            "https://api.bseindia.com/BseIndiaAPI/api/MktCapBoard_indstream/w?cat=3&type=2",
            "https://api.bseindia.com/BseIndiaAPI/api/MktCapBoard_indstream/w?cat=4&type=2"]
    # List to store DataFrames
    dataframes = []
    try:
        for url in urls:
            # Send the GET request with headers
            response = requests.get(url, headers=headers)

            # Check for successful response (status code 200)
            if response.status_code == 200:
                # Parse JSON response
                data = response.json()
                # Convert 'RealTime' data to DataFrame
                if 'RealTime' in data:
                    df_realtime = pd.DataFrame(data['RealTime'])
                    
                    # Append DataFrame to list
                    dataframes.append(df_realtime)
                else:
                    print(f"Key 'RealTime' not found in the response from URL: {url}")
            else:
                print(f"Request failed with status code: {response.status_code} for URL: {url}")
        # Concatenate all DataFrames into a single DataFrame
        if dataframes:
            dfBseIndexBhavCopy = pd.concat(dataframes, ignore_index=True)
            dfBseIndexBhavCopy.columns=['ScripFlagCode','INDX_CD','TICKER','OPEN','HIGH','LOW','CLOSE','Prev_Close','Chg','ChgPer','Week52High','Week52Low','AUX2','MktcapPerc','NET_TURN','TurnoverPerc','DT_TM','WebURL']
            dfBseIndexBhavCopy=dfBseIndexBhavCopy[['TICKER','OPEN','HIGH','LOW','CLOSE','AUX2']]
            dfBseIndexBhavCopy['DATE_YMD'] = datetime.now().strftime('%Y%m%d')
            dfBseIndexBhavCopy['FULLNAME']=dfBseIndexBhavCopy['TICKER']
            dfBseIndexBhavCopy['VOLUME']=0
            dfBseIndexBhavCopy['INDUSTRYNAME']='BSE Indices'
            dfBseIndexBhavCopy['SECTORNAME']='Indices'
            dfBseIndexBhavCopy['ALIAS']=''
            dfBseIndexBhavCopy['ADDRESS']=''
            dfBseIndexBhavCopy['COUNTRY']=''
            dfBseIndexBhavCopy['CURRENCY']=''
            dfBseIndexBhavCopy['OPENINT']=0
            dfBseIndexBhavCopy['AUX1']=0
            column_order = [
            'DATE_YMD', 'TICKER', 'FULLNAME', 'OPEN', 'HIGH', 'LOW', 'CLOSE',
            'VOLUME', 'INDUSTRYNAME', 'SECTORNAME', 'ALIAS', 'ADDRESS',
            'COUNTRY', 'CURRENCY', 'OPENINT', 'AUX1', 'AUX2'
        ]
            dfBseIndexBhavCopy=dfBseIndexBhavCopy[column_order]
            #dfBseIndexBhavCopy.to_csv('BSEIndexBhavCopy.csv', index=False)
            return dfBseIndexBhavCopy
        else:
            print("No dataframes to concatenate.")

    except requests.exceptions.RequestException as e:
        logger.exception("Error Generating BSE BhavCopy")

logger=logging.getLogger("NSEBSEBhavCopyBuilder")
#dateformat MM/DD/YYYY
#enter start and end date as per your requirement
Session = requests.Session()
global nselive
global dropBoxClient
dropBoxClient=DropboxClient()
bseHelper= BseHelper()
nselive = NSELive()
# Download the Most Recent ValueStocks DataFile.
# Get the most recent file
GetMostRecentValueStocksDataFile()

historicalDays=1#input("For How many days of Data to Fetch (Default 1): ")
if(historicalDays == ''):
    historicalDays = 1
EndDate=datetime.today()
#EndDate = datetime.now() - timedelta(days=2) # for debugging purpose only
dt = pd.date_range(end=EndDate, periods=int(historicalDays))
dataframestoWrite=[]
for tday in dt:
    
    #1. NSE Index BhavCopy 
    dfNseIndexBhavCopy=DownloadNSEIndexBhavCopy(tday)
    if(dfNseIndexBhavCopy is not None and dfNseIndexBhavCopy.shape[0] > 1):
        print("NSE Index Bhavcopy Data : OK")
        dataframestoWrite.append(dfNseIndexBhavCopy)
    else:
        print("NSE Index Bhavcopy Data : NOT OK")

    #2. BSE Index BhavCopy     
    dfBseindexBhavCopy=GetBSEindexDataBhavCopy()
    if(dfBseindexBhavCopy is not None and dfBseindexBhavCopy.shape[0] > 1):
        print("BSE Index Bhavcopy Data : OK")
        dataframestoWrite.append(dfBseindexBhavCopy)
    else:
        print("BSE Index Bhavcopy Data : NOT OK")

    #3. NSE Sector and Industry Bhavcopy     
    dfNseSectoralAndIndustryBhavCopy=BuildNseSectoralAndIndustryBhavCopy()
    if(dfNseSectoralAndIndustryBhavCopy is not None and dfNseSectoralAndIndustryBhavCopy.shape[0] > 1):
        print("NSE Sectoral Data : OK")
        dataframestoWrite.append(dfNseSectoralAndIndustryBhavCopy)
    else:
        print("NSE Sectoral Data : NOT OK")
    
    #4. NSE Equity Bhavcopy    
    dfNSEBhavCopy=DownloadNSEBhavCopy(pd.date_range(start=tday ,end=tday,periods=1))
    if(dfNSEBhavCopy is not None and dfNSEBhavCopy.shape[0] > 1):
        print("NSE Stocks Bhavcopy Data : OK")
        dataframestoWrite.append(dfNSEBhavCopy)
    else:
        print("NSE Stocks Bhavcopy Data : NOT OK")
    
    #5. BSE Sector and Industry Bhavcopy
    dfBseSectoralAndIndustryBhavCopy=bseHelper.BuildBseSectoralAndIndustryBhavCopy()
    if(dfBseSectoralAndIndustryBhavCopy is not None and dfBseSectoralAndIndustryBhavCopy.shape[0] > 1):
        print("BSE Sector and Industry Bhavcopy : OK")
        dataframestoWrite.append(dfBseSectoralAndIndustryBhavCopy)
    else:
        print("BSE Sector and Industry Bhavcopy : NOT OK")

    #6. BSE Equity Bhavcopy    
    dfBSEBhavCopy=bseHelper.DownloadBSEBhavCopy(pd.date_range(start=tday,end=tday,periods=1))
    if(dfBSEBhavCopy is not None and dfBSEBhavCopy.shape[0] > 1):
        print("BSE Stocks Bhavcopy Data : OK")
        dataframestoWrite.append(dfBSEBhavCopy)
    else:
        print("BSE Stocks Bhavcopy Data : NOT OK")
        
    
    merged_df = pd.concat(dataframestoWrite, ignore_index=True)
    dateForFilename= datetime.strftime(tday,'%Y-%m-%d').upper()
    filename = dateForFilename + '-NSE-BSE-IS-ALL-EQ.CSV'
    merged_df.to_csv(filename, header = True,index = False,date_format='%Y%m%d')
    
    #7. Portfolio Bhavcopy to be added at the end. Post the availability of the data
    dfPortfolioSummary=PortfolioUpdate.main()
    print(dfPortfolioSummary)
    merged_df=pd.concat([merged_df,dfPortfolioSummary], ignore_index=True)
    merged_df.to_csv(filename, header = True,index = False,date_format='%Y%m%d')
    print(datetime.strftime(tday,'%d-%b-%Y').upper() + ":   ==>  "+filename + "   [Done]")
    dataframestoWrite=[]
    # Uploading the generated CSV to Dropbox
    fileNameToDropbox = f"/NSEBSEBhavcopy/DailyBhavCopy/{filename}"  # Adjust the Dropbox folder path as needed
    dropBoxClient.upload_file(filename, fileNameToDropbox)
    print(f"Complete BhavCopy have been Uploaded to Dropbox at : {fileNameToDropbox}")
    
    logging.shutdown()  # Flush and close the log file
    # Get the current time in IST
    ist = timezone('Asia/Kolkata')
    log_file_path = os.path.abspath("NSEBSEBhavCopyDownload.Log")
    print(f'Logfile is located locally at : {log_file_path}')
    logFileNameInDropBox=f'/NSEBSEBhavcopy/Logs/{datetime.strftime(datetime.now(ist),'%Y-%m-%d %H-%M-%S').upper()}-NSEBSEBhavCopyDownload.Log'
    dropBoxClient.upload_file(log_file_path,logFileNameInDropBox)
    print(f'Log File have been Uploaded to {logFileNameInDropBox}.')
