from fyers_apiv3 import fyersModel
import pandas as pd
import numpy as np
from datetime import datetime
import pytz

def fetch_candle_data(Symbol,range_from,range_to):
    appId = open("FYERS_APP_ID.TXT","r").read()
    access_token=open("FYERS_ACCESS_TOKEN.TXT","r").read()
    fyers = fyersModel.FyersModel(client_id=appId, is_async=False, token=access_token, log_path="")
    data = {
    "symbol":Symbol,
    "resolution":"5",
    "date_format":"1",
    "range_from":range_from,
    "range_to":range_to,
    "cont_flag":"1"
    }
    response = fyers.history(data=data)
    print(response)
    df = pd.DataFrame(response['candles'], columns=['epoch', 'open', 'high', 'low', 'close', 'volume'])
	
    # Convert epoch to datetime (optional)
    df['datetime'] = pd.to_datetime(df['epoch'], unit='s')
    df['datetime'] = df['datetime'].dt.tz_localize('UTC')
    # Now, convert to IST (Indian Standard Time)
    ist_timezone = pytz.timezone('Asia/Kolkata')
    df['datetime_ist'] = df['datetime'].dt.tz_convert(ist_timezone)
    df = df[['epoch', 'datetime_ist', 'open', 'high', 'low', 'close', 'volume']]
    return df

def main():
    
    candle_data = fetch_candle_data("NSE:NIFTY2540923300CE","2025-03-01","2025-04-05")
    candle_data.to_csv('NIFTY2540923300CE.csv', index=False) 

if __name__ == "__main__":
    main()