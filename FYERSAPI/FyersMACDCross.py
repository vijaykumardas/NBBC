import pandas as pd
import numpy as np
from datetime import datetime
from fyers_apiv3 import fyersModel

def fetch_candle_data():
    appId = open("FYERS_APP_ID.TXT","r").read()
    access_token=open("FYERS_ACCESS_TOKEN.TXT","r").read()
    #"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiZDoxIiwiZDoyIiwieDowIiwieDoxIiwieDoyIl0sImF0X2hhc2giOiJnQUFBQUFCbjhwTkVzcW1sbzloNFItWF93WWJpTzhMNGtZUDctUW1WT3BGNzdDNFE0UFV4ZS1MQVVnVUxhN3A2eGRkcUVYSnE1MF9WanNiTzRvaGM2THRZV3lqSWtINFBhenM1TUJ4QlQwbEFKQ1I3NFkydk9CVT0iLCJkaXNwbGF5X25hbWUiOiIiLCJvbXMiOiJLMSIsImhzbV9rZXkiOiIwNzU2NTc0NzEzNmFlNmY1NTE3YTUwYTEwYWM0ZTYwODQzZWJkMDhjNzY4NDE2NTY4MzI3NDk1OSIsImlzRGRwaUVuYWJsZWQiOiJOIiwiaXNNdGZFbmFibGVkIjoiTiIsImZ5X2lkIjoiWVYxNjcyNCIsImFwcFR5cGUiOjEwMCwiZXhwIjoxNzQzOTg1ODAwLCJpYXQiOjE3NDM5NTA2NjAsImlzcyI6ImFwaS5meWVycy5pbiIsIm5iZiI6MTc0Mzk1MDY2MCwic3ViIjoiYWNjZXNzX3Rva2VuIn0.pcsMmCgcF5iwfEHb_I1UiGEHjGq0uvpPrg_rLyCTJ-4"
    fyers = fyersModel.FyersModel(client_id=appId, is_async=False, token=access_token, log_path="")
    data = {
    "symbol":"NSE:NIFTY2540923300CE",
    "resolution":"5",
    "date_format":"1",
    "range_from":"2025-04-04",
    "range_to":"2025-04-05",
    "cont_flag":"1"
    }
    response = fyers.history(data=data)
    print(response)
    return response['candles']

def calculate_macd(df, fast=72, slow=70, signal=9):
    df['EMA_fast'] = df['close'].ewm(span=fast, adjust=False).mean()
    df['EMA_slow'] = df['close'].ewm(span=slow, adjust=False).mean()
    df['MACD'] = df['EMA_fast'] - df['EMA_slow']
    df['Signal'] = df['MACD'].ewm(span=signal, adjust=False).mean()
    df['MACD_Hist'] = df['MACD'] - df['Signal']
    return df

def detect_zero_line_cross(df):
    df['Prev_MACD'] = df['MACD'].shift(1)

    def check_cross(row):
        if row['Prev_MACD'] < 0 and row['MACD'] > 0:
            return 'MACD crossed UP the zero line'
        elif row['Prev_MACD'] > 0 and row['MACD'] < 0:
            return 'MACD crossed DOWN the zero line'
        return None

    df['MACD_Zero_Cross'] = df.apply(check_cross, axis=1)
    return df

def alert_macd_cross(df):
    alerts = df[df['MACD_Zero_Cross'].notnull()]
    for _, row in alerts.iterrows():
        time_str = datetime.fromtimestamp(row['epoch']).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{time_str}] ALERT: {row['MACD_Zero_Cross']}")

def main():
    candle_data = fetch_candle_data()

    # Create DataFrame
    df = pd.DataFrame(candle_data, columns=['epoch', 'open', 'high', 'low', 'close', 'volume'])
	
    # Convert epoch to datetime (optional)
    df['datetime'] = pd.to_datetime(df['epoch'], unit='s')

    # MACD Calculation
    df = calculate_macd(df)
    print(df)
    # Detect MACD zero-line cross
    df = detect_zero_line_cross(df)

    # Alert
    alert_macd_cross(df)

if __name__ == "__main__":
    main()
