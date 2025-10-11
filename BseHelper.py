import os
import pandas as pd
import requests
import logging
import time
import io
from io import BytesIO
from io import StringIO
import zipfile
from datetime import datetime, timedelta
from DropboxClient import DropboxClient
class BseHelper:
    def __init__(self):
        self.session = requests.Session()
        self.base_url_header = "https://api.bseindia.com/BseIndiaAPI/api/ComHeadernew/w?quotetype=EQ&scripcode={scripcode}&seriesid="
        self.base_url_trading = "https://api.bseindia.com/BseIndiaAPI/api/StockTrading/w?flag=&quotetype=EQ&scripcode={scripcode}"
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "origin": "https://www.bseindia.com",
            "referer": "https://www.bseindia.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
        }
        self.logger = logging.getLogger(__name__)
        self.dropboxClient=DropboxClient()
        logging.basicConfig(level=logging.DEBUG)

        # Initialize the cache DataFrame and the cache modified tracker
        self._bse_industry_sector_cache = self._load_industry_sector_cache()
        self.industry_name_fix_df = self._load_industry_name_fix()
        self.Cache_Modified = False  # This will track if the cache has been modified

    def _load_industry_name_fix(self):
        """Load the industry name fix mapping from BSEIndustryNameFix.csv."""
        file_path = 'BSEIndustryNameFix.csv'
        self.dropboxClient.download_file(f"/nsebsebhavcopy/DailyBhavCopy/Temp/{file_path}",file_path)
        if os.path.exists(file_path):
            self.logger.info("Loading industry name fixes from BSEIndustryNameFix.csv")
            return pd.read_csv(file_path)
        else:
            self.logger.error(f"{file_path} not found. Industry name fixes will not be applied.")
            return pd.DataFrame(columns=["BSE_INDUSTRYNAME", "BSE_INDUSTRYNAME_FIXED"])
            
    def _load_industry_sector_cache(self):
        """Loads the industry and sector cache from the CSV file if present."""
        file_path = 'BSEIndustrySectorMaster.csv'
        self.dropboxClient.download_file(f"/nsebsebhavcopy/DailyBhavCopy/Temp/{file_path}",file_path)
        if os.path.exists(file_path):
            self.logger.info("Loading industry and sector cache from BSEIndustrySectorMaster.csv")
            return pd.read_csv(file_path)
        else:
            self.logger.info("Industry and sector cache file not found. Initializing an empty cache.")
            return pd.DataFrame(columns=["INDUSTRYNAME", "SECTORNAME","MACRONAME"])

    def _save_industry_sector_cache(self):
        """Saves the updated industry and sector cache back to the CSV file if modified."""
        if self.Cache_Modified:  # Only save if the cache has been modified
            file_path = 'BSEIndustrySectorMaster.csv'
            self._bse_industry_sector_cache.to_csv(file_path, index=False)
            self.dropboxClient.upload_file(file_path,f"/nsebsebhavcopy/DailyBhavCopy/Temp/{file_path}",)
            self.logger.info(f"Updated industry and sector cache saved to {file_path} and Uploaded to DropBox at /nsebsebhavcopy/DailyBhavCopy/Temp/{file_path}")
            # Note: _cache_modified will NOT be reset, even after saving.
            # The flag will remain True after a save, as requested.

    def _update_industry_sector_cache(self, industry, sector,macros):
        """Updates the cache with new INDUSTRYNAME and SECTORNAME if not already present."""
        if pd.notnull(industry) and industry.strip() != '' and pd.notnull(sector) and sector.strip() != '':
            existing_row = self._bse_industry_sector_cache[self._bse_industry_sector_cache['INDUSTRYNAME'] == industry]
            
            if not existing_row.empty:
                # Update the SECTORNAME if it exists and both values are valid
                self._bse_industry_sector_cache.loc[existing_row.index[0], 'SECTORNAME'] = sector
                self._bse_industry_sector_cache.loc[existing_row.index[0], 'MACRONAME'] = macros
                self.logger.info(f"Updated sector for existing industry in cache: ({industry}, {sector}, {macros})")
            else:
                # Add new entry if it doesn't exist
                new_entry = pd.DataFrame({"INDUSTRYNAME": [industry], "SECTORNAME": [sector],"MACRONAME":[macros]})
                self._bse_industry_sector_cache = pd.concat([self._bse_industry_sector_cache, new_entry], ignore_index=True)
                self.logger.info(f"Added new industry-sector pair to cache: ({industry}, {sector})")
            
            self.Cache_Modified = True  # Mark the cache as modified


    def _cache_contains_industry_sector(self, industry, sector):
        """Checks if the cache already contains the given INDUSTRYNAME and SECTORNAME."""
        return ((self._bse_industry_sector_cache["INDUSTRYNAME"] == industry) &
                (self._bse_industry_sector_cache["SECTORNAME"] == sector)).any()

    def _GetBseScripList(self, url):
        """Private method to fetch BSE scrip list from the given URL."""
        try:
            self.logger.info(f"Fetching data from {url}")
            response = self.session.get(url, headers=self.headers)
            response.raise_for_status()  # Raise an exception for any HTTP errors
            data = response.json()

            return pd.DataFrame(data)
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from {url}: {e}")
            return pd.DataFrame()

    def _get_sector_name(self, industry, ticker):
        """Fetch SECTORNAME from the cache or the API if not found."""
        # Query cache for the SECTORNAME
        sector_row = self._bse_industry_sector_cache[self._bse_industry_sector_cache['INDUSTRYNAME'] == industry]

        if not sector_row.empty and pd.notnull(sector_row.iloc[0]['SECTORNAME']) and sector_row.iloc[0]['SECTORNAME'].strip() != '':
            # Return SECTORNAME from cache
            return sector_row.iloc[0]['SECTORNAME']
        else:
            # If not found in cache, make API call to get additional info
            additional_info = self._GetAdditionalScripInfo(ticker)

            if additional_info and 'INDUSTRYNEW' in additional_info:
                sectorname = additional_info['INDUSTRYNEW']
                # Update the cache with the new INDUSTRYNAME and SECTORNAME
                self._update_industry_sector_cache(industry, sectorname,"NOMACROS")
                return sectorname

            # Fallback if no sectorname is found
            return None
    def _get_macro_name(self, industry, ticker):
        """Fetch SECTORNAME from the cache or the API if not found."""
        # Query cache for the SECTORNAME
        sector_row = self._bse_industry_sector_cache[self._bse_industry_sector_cache['INDUSTRYNAME'] == industry]

        if not sector_row.empty and pd.notnull(sector_row.iloc[0]['MACRONAME']) and sector_row.iloc[0]['MACRONAME'].strip() != '':
            # Return SECTORNAME from cache
            return sector_row.iloc[0]['MACRONAME']
        else:
            return ''

    def GetAllBseScrips(self):
        """Fetch combined BSE scrips from both Equity and EQT0 segments and return a filtered DataFrame with additional info."""
        """ Incase of any issue to diagnose the API Please visit https://www.bseindia.com/corporates/List_Scrips.html"""
        
        url1_T_Plus_0 =  "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&segment=EQT0&status=Active"
        url2_T_Plus_1 =  "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&segment=Equity&status=Active"
        
        df1_T_Plus_0 = self._GetBseScripList(url1_T_Plus_0)
        df2_T_Plus_1 = self._GetBseScripList(url2_T_Plus_1)
        combined_df = pd.concat([df1_T_Plus_0, df2_T_Plus_1], ignore_index=True)

        # Filter and rename columns
        filtered_df = combined_df[['SCRIP_CD', 'Scrip_Name', 'ISIN_NUMBER', 'INDUSTRY', 'Mktcap']]
        filtered_df.columns = ['SYMBOL', 'FULLNAME', 'ISIN_NUMBER', 'INDUSTRYNAME', 'MARKETCAP']  # Rename columns
        
        other_data_file="2025-10-08-22-43-21-NSE-BSE-IS-ALL-EQ.CSV"
        self.dropboxClient.download_file(f"/nsebsebhavcopy/DailyBhavCopy/{other_data_file}",other_data_file)
        # Load the external CSV file for filling missing INDUSTRYNAME values
        other_data_df = pd.read_csv(other_data_file, usecols=['TICKER', 'INDUSTRYNAME'])

        # Create a dictionary from TICKER to INDUSTRYNAME for lookup
        ticker_to_industry = dict(zip(other_data_df['TICKER'], other_data_df['INDUSTRYNAME']))

        # Define a function to fill missing INDUSTRYNAME using the dictionary
        def fill_industry(row):
            if pd.isna(row['INDUSTRYNAME']) or row['INDUSTRYNAME'].strip() == '':
                return ticker_to_industry.get(row['SYMBOL'], row['INDUSTRYNAME'])
            else:
                return row['INDUSTRYNAME']

        # Apply the function to fill missing INDUSTRYNAME values
        filtered_df['INDUSTRYNAME'] = filtered_df.apply(fill_industry, axis=1)

        
        # Fix INDUSTRYNAME based on BSEIndustryNameFix.csv mapping
        industry_name_fix_dict = dict(zip(self.industry_name_fix_df['BSE_INDUSTRYNAME'], self.industry_name_fix_df['BSE_INDUSTRYNAME_FIXED']))
        filtered_df.loc[:,'INDUSTRYNAME'] = filtered_df['INDUSTRYNAME'].replace(industry_name_fix_dict)
        
        # Convert MARKETCAP to numeric, handle errors and set invalid parsing to NaN
        filtered_df.loc[:,'MARKETCAP'] = pd.to_numeric(filtered_df['MARKETCAP'], errors='coerce')
        
        # Adjust the MARKETCAP column
        filtered_df.loc[:,'MARKETCAP'] = filtered_df['MARKETCAP'].apply(lambda x: round(x * 10000000, 2))  # Divide by 100 and round to 2 decimal places

        # Add SECTORNAME column
        filtered_df=filtered_df[filtered_df['INDUSTRYNAME']!='RANDOMXYZ'].copy()
        filtered_df.loc[:, 'SECTORNAME'] = filtered_df.apply(
            lambda row: self._get_sector_name(row['INDUSTRYNAME'], row['SYMBOL']) if pd.notnull(row['INDUSTRYNAME']) and row['INDUSTRYNAME'].strip() != '' else None,
            axis=1)
        
        #Add MACRONAME column
        filtered_df=filtered_df[filtered_df['INDUSTRYNAME']!='RANDOMXYZ'].copy()
        filtered_df.loc[:, 'MACRONAME'] = filtered_df.apply(
            lambda row: self._get_macro_name(row['INDUSTRYNAME'], row['SYMBOL']) if pd.notnull(row['INDUSTRYNAME']) and row['INDUSTRYNAME'].strip() != '' else None,
            axis=1)
        
        
        filtered_df.loc[:,'SYMBOL']=filtered_df['SYMBOL'].astype('int64')
        filtered_df.loc[:,'FULLNAME']=filtered_df['SYMBOL'].astype('str')
        filtered_df.loc[:,'ISIN_NUMBER']=filtered_df['ISIN_NUMBER'].astype('str')
        filtered_df.loc[:,'INDUSTRYNAME']=filtered_df['INDUSTRYNAME'].astype('str')
        filtered_df.loc[:,'SECTORNAME']=filtered_df['SECTORNAME'].astype('str')
        filtered_df.loc[:,'MACRONAME']=filtered_df['MACRONAME'].astype('str')
        filtered_df.loc[:,'MARKETCAP'] = filtered_df['MARKETCAP'].astype('float64')
        filtered_df.to_csv("BseAllScrips.csv", index=False)
        # Combine the DataFrames
        return filtered_df[['SYMBOL', 'FULLNAME', 'ISIN_NUMBER', 'INDUSTRYNAME', 'SECTORNAME', 'MACRONAME','MARKETCAP']]

    def _GetAdditionalScripInfo(self, scripcode):
        """Private method to fetch detailed stock information for a given scrip code."""
        try:
            url_header = self.base_url_header.format(scripcode=scripcode)
            url_trading = self.base_url_trading.format(scripcode=scripcode)

            self.logger.info(f"Fetching additional scrip info for scripcode: {scripcode}")
            response_header = self.session.get(url_header, headers=self.headers)
            response_trading = self.session.get(url_trading, headers=self.headers)

            if response_header.status_code == 200 and response_trading.status_code == 200:
                content_type_header = response_header.headers.get("Content-Type", "")
                content_type_trading = response_trading.headers.get("Content-Type", "")

                if "application/json" not in content_type_header or "application/json" not in content_type_trading:
                    self.logger.warning(f"Unexpected content type for scripcode {scripcode}: {content_type_header} or {content_type_trading}")
                    return None

                data_header = response_header.json()
                data_trading = response_trading.json()

                header_info = {
                    "INDUSTRYNEW": data_header.get("IndustryNew"),  # Assuming 'IndustryNew' is part of the response
                    "SECTORNAME": data_header.get("Sector"),
                    "MktCapFull": data_trading.get("MktCapFull")
                }
                return header_info
            else:
                self.logger.error(f"Failed to fetch data for scripcode {scripcode}. Status code: {response_header.status_code}, {response_trading.status_code}")
                return None
        except requests.RequestException as e:
            self.logger.error(f"Error while fetching stock info for scripcode {scripcode}: {e}")
            return None

    def close(self):
        """Call this method to save the cache if modified when closing the helper."""
        self._save_industry_sector_cache()  # Save only if modified
        
    def BuildBseSectoralAndIndustryBhavCopy(self):
        try:
            # Generate the output file name with the current date
            current_date = datetime.now().strftime('%Y-%m-%d')
            output_csv_file_path = f'{current_date}-BSE_IS.csv'
                
            file_exists = os.path.exists(output_csv_file_path)
            if(file_exists):
                self.logger.debug("BSE Sectoral and Industry Data File found at :"+output_csv_file_path+", Hence no need to Build. Just return the Dataframe")
            else:
                df=self.GetAllBseScrips()
                
                # Create the summary by SECTOR
                summary_sector = df.groupby('SECTORNAME')['MARKETCAP'].sum().reset_index()
                self.logger.debug(summary_sector)
                summary_sector['TICKER'] = summary_sector['SECTORNAME'].apply(lambda x: f'BSE_SECTOR: {x}')
                summary_sector['FULLNAME'] = summary_sector['SECTORNAME'].apply(lambda x: f'{x}')
                summary_sector['SECTORNAME'] = 'Macros'
                summary_sector['INDUSTRYNAME'] = 'Sectors'
                summary_sector = summary_sector[['TICKER', 'FULLNAME','MARKETCAP','SECTORNAME','INDUSTRYNAME']]
                
                # Removing the Entries where SECTOR is not present.
                summary_sector= summary_sector[summary_sector['TICKER']!='BSE_SECTOR: None'].copy()
                summary_sector= summary_sector[summary_sector['TICKER']!='BSE_SECTOR: '].copy()
                self.logger.debug(summary_sector)
                self.logger.debug("SUMMARY OF DATA by SECTOR Completed")
                
                # Create the summary by INDUSTRY
                summary_industry = df.groupby(['INDUSTRYNAME','SECTORNAME'])['MARKETCAP'].sum().reset_index()
                summary_industry['TICKER'] = summary_industry['INDUSTRYNAME'].apply(lambda x: f'BSE_INDUSTRY: {x}')
                summary_industry['SECTORNAME'] = summary_industry['SECTORNAME']
                summary_industry['INDUSTRYNAME'] = summary_industry['INDUSTRYNAME']
                summary_industry['FULLNAME'] = summary_industry['INDUSTRYNAME']
                summary_industry = summary_industry[['TICKER', 'FULLNAME','MARKETCAP','SECTORNAME','INDUSTRYNAME']]
                
                # Removing the Entries where INDUSTRY is not present.
                summary_industry= summary_industry[summary_industry['TICKER']!='BSE_INDUSTRY: '].copy()
                summary_industry= summary_industry[summary_industry['TICKER']!='BSE_INDUSTRY: None'].copy()
                
                self.logger.debug(summary_industry)
                self.logger.debug("SUMMARY OF DATA by INDUSTRY Completed")
                
                # Combine all summaries into one DataFrame
                combined_summary = pd.concat([summary_sector, summary_industry], ignore_index=True)
                #combined_summary.to_csv("CombinedSummary.csv", index=False)
                self.logger.debug(combined_summary)
                # Add the DATE column with the current date in YYYYMMDD format
                
                current_date = datetime.now().strftime('%Y%m%d')
                combined_summary.insert(0, 'DATE_YMD', current_date)
                # Add additional columns with specified values
                
                combined_summary['OPEN'] = (combined_summary['MARKETCAP'] / 100000).round(2)
                combined_summary['HIGH'] = (combined_summary['MARKETCAP'] / 100000).round(2)
                combined_summary['LOW'] = (combined_summary['MARKETCAP'] / 100000).round(2)
                combined_summary['CLOSE'] = (combined_summary['MARKETCAP'] / 100000).round(2)
                combined_summary['AUX2'] = combined_summary['MARKETCAP']
                self.logger.debug("-----------------------------------------")
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
                combined_summary.to_csv(output_csv_file_path, index=False)
                #print(datetime.now().strftime('%d-%b-%Y').upper() + ":   ==>  "+output_csv_file_path + "   [Done]")
                #logger.debug("NSE_SECTOR_IND_BHAVCOPY Successfully saved to " + output_csv_file_path)
                return combined_summary
            #df=pd.read_csv(output_csv_file_path)
            #return df
        except Exception as e:
            self.logger.exception("ERROR: Error Occured during Building BSE_SECTOR_IND_BHAVCOPY")
    def GetBSEDeliveryData(self, date):
        self.logger.debug("Downloading the BSE Delivery Data for the date = " + str(date))
        try:    
            Bse_Delivery_Data_UrlFormat="https://www.bseindia.com/BSEDATA/gross/{year}/SCBSEALL{ddmm}.zip"
                                       # https://www.bseindia.com/BSEDATA/gross/2023/SCBSEALL0509.zip
            Bse_Delivery_Data_Url=Bse_Delivery_Data_UrlFormat.format(year=datetime.strftime(date,'%Y'),ddmm=datetime.strftime(date,'%d%m'))
            DeliveryDataFileName_Format="SCBSEALL{ddmm}.TXT"
            DeliveryDataFileNameInZIP=DeliveryDataFileName_Format.format(ddmm=datetime.strftime(date,'%d%m'))
            #print(Bse_Delivery_Data_Url)
            #headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
            self.logger.debug(" Bse Delivery Data Url : " +  Bse_Delivery_Data_Url)
        
            r = self.session.get(Bse_Delivery_Data_Url,allow_redirects=True,headers=self.headers)
            unzipcsvfile= zipfile.ZipFile(io.BytesIO(r.content))
            #print(BhavCopyFileNameinZIP)
            bseDeliveryDf = pd.read_csv(unzipcsvfile.open(DeliveryDataFileNameInZIP),sep='|')
            bseDeliveryDf.columns = bseDeliveryDf.columns.str.replace('`', '\'')
            bseDeliveryDf=bseDeliveryDf.rename(columns={"DATE": "TIMESTAMP",'SCRIP CODE':'SYMBOL','DELIVERY QTY':'TOTTRDQTY','DELIVERY VAL':'DELIVERY_VAL','DAY\'S VOLUME':'DAYSVOLUME','DAY\'S TURNOVER':'DAYSTURNOVER','DELV. PER.':'DELVPER'})
            bseDeliveryDf.drop(['TIMESTAMP','DELIVERY_VAL','DAYSVOLUME','DAYSTURNOVER','DELVPER'],inplace=True, axis=1)
            self.logger.debug("Downloading the BSE Delivery Data for the date = " + str(date)+ " has been Successful and returned the Result.")
            return bseDeliveryDf
        except Exception as e:
            self.logger.exception("ERROR: An Exception Occured Details = "+str(e))
            
    def DownloadBSEBhavCopy(self,dateRange):
        self.logger.debug("Downloading the BSE BhavCopy for the Date Range : "+str(dateRange))
        for tday in dateRange:
            try:
                dateForFilename= datetime.strftime(tday,'%Y-%m-%d').upper()
                Bse_BhavCopy_Url_Format="https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{YYYYMMDD}_F_0000.CSV"
                YYYYMMDD=datetime.strftime(tday,'%Y%m%d')
                timestampForDF=datetime.strftime(tday,'%Y%m%d')
                Bse_BhavCopy_Url=Bse_BhavCopy_Url_Format.format(YYYYMMDD=YYYYMMDD)
                headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
                
                self.logger.debug("BSE Bhavcopy Url = "+ Bse_BhavCopy_Url)
                r = self.session.get(Bse_BhavCopy_Url,allow_redirects=True,headers=headers).content
                bseBhavCopyDf = pd.read_csv(io.StringIO(r.decode('utf-8')))
                
                bseBhavCopyDf['TIMESTAMP']=timestampForDF
                bseBhavCopyDf = bseBhavCopyDf[['FinInstrmId','TIMESTAMP','OpnPric','HghPric','LwPric','ClsPric','TtlTradgVol','FinInstrmNm']]
                bseBhavCopyDf.columns = ['SYMBOL','TIMESTAMP','OPEN','HIGH','LOW','CLOSE','TOTTRDQTY','FinInstrmNm']
                
                # Update the Dataframe with Delivery Data
                bseDeliveryDf=self.GetBSEDeliveryData(tday)
                bseBhavCopyDf=bseBhavCopyDf.merge(bseDeliveryDf, on='SYMBOL', how='left')
                #print(bseBhavCopyDf.columns)
                bseBhavCopyDf['TOTTRDQTY_y']=bseBhavCopyDf['TOTTRDQTY_y'].fillna(bseBhavCopyDf['TOTTRDQTY_x'])
                bseBhavCopyDf.drop(['TOTTRDQTY_x'],inplace=True, axis=1)
                bseBhavCopyDf=bseBhavCopyDf.rename(columns={"TOTTRDQTY_y": "TOTTRDQTY"})
                
                #Update the Dataframe with Sector and industry and MarkerCap info
                bseScripdf=self.GetAllBseScrips()
                bseScripdf=bseScripdf[['SYMBOL','INDUSTRYNAME','SECTORNAME','MARKETCAP']]
                bseScripdf.columns=['SYMBOL','INDUSTRYNAME','SECTORNAME','AUX2']
                bseScripdf['SYMBOL']=bseScripdf['SYMBOL'].astype('int64')
                bseBhavCopyDf=bseBhavCopyDf.merge(bseScripdf, on='SYMBOL', how='left')
                
                # Handle Remaining Columns
                bseBhavCopyDf["FULLNAME"]= bseBhavCopyDf['FinInstrmNm'].str.title()
                bseBhavCopyDf["ALIAS"]=''
                bseBhavCopyDf["ADDRESS"]=''
                bseBhavCopyDf["COUNTRY"]=''
                bseBhavCopyDf["CURRENCY"]=''
                bseBhavCopyDf["OPENINT"]=0
                bseBhavCopyDf["AUX1"]=0
                self.logger.debug(bseBhavCopyDf.columns)
                bseBhavCopyDf.columns = ['TICKER','DATE_YMD','OPEN','HIGH','LOW','CLOSE','FinInstrmNm','VOLUME','INDUSTRYNAME','SECTORNAME','AUX2','FULLNAME','ALIAS','ADDRESS','COUNTRY','CURRENCY','OPENINT','AUX1']
                column_order = ['DATE_YMD','TICKER','FULLNAME','OPEN','HIGH','LOW','CLOSE','VOLUME','INDUSTRYNAME','SECTORNAME','ALIAS','ADDRESS','COUNTRY','CURRENCY','OPENINT','AUX1','AUX2']
                bseBhavCopyDf=bseBhavCopyDf[column_order]
                return bseBhavCopyDf
            except Exception as e:
                self.logger.exception("ERROR: An Exception Occured " + str(e))


# Example usage:
if __name__ == "__main__":
    start_time = time.time()  # Start the timer

    bse_helper = BseHelper()
    combined_df = bse_helper.GetAllBseScrips()
    
    end_time = time.time()  # End the timer
    duration = end_time - start_time  # Calculate the duration

    if not combined_df.empty:
        print(combined_df.head())  # Display the first few rows
        combined_df.to_csv('BSEMasterData.csv', index=False)  # Write DataFrame to CSV file
        print("Data saved to BSEMasterData.csv")
    else:
        logging.error("No data available to display.")

    print(f"Processing took {duration:.2f} seconds.")  # Print the duration
    if(bse_helper.Cache_Modified):
        print("Cache Have been Modified")
    else:
        print("Cache Have not been Modified")
        
    tday = datetime.now() - timedelta(days=2)
    dfBseBhavCopy=bse_helper.DownloadBSEBhavCopy(pd.date_range(start=tday, end=tday, periods=1))
    dfBseBhavCopy.to_csv('BSEBhavCopy.csv', index=False)  # Write DataFrame to CSV file
        
    bse_helper.BuildBseSectoralAndIndustryBhavCopy()
    bse_helper.close()  # Ensure cache is saved before exiting
