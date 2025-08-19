import os
import time
import pytz
import traceback
import pandas as pd
import schedule
from datetime import datetime, timedelta
from fyers_apiv3 import fyersModel
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging

LOG_FILE = "SpreadMonitor.log"

logging.basicConfig(
    filename=LOG_FILE,
    filemode="a",  # Append mode
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("SpreadMonitor")

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

RESOLUTION = "5"
DAYS_TO_CACHE = 15

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
    logger.info(response)
    df = pd.DataFrame(response['candles'], columns=['epoch', 'open', 'high', 'low', 'close', 'volume'])
	
    # Convert epoch to datetime (optional)
    df['datetime'] = pd.to_datetime(df['epoch'], unit='s')
    df['datetime'] = df['datetime'].dt.tz_localize('UTC')
    # Now, convert to IST (Indian Standard Time)
    ist_timezone = pytz.timezone('Asia/Kolkata')
    df['datetime_ist'] = df['datetime'].dt.tz_convert(ist_timezone)
    df = df[['epoch', 'datetime_ist', 'open', 'high', 'low', 'close', 'volume']]
    return df
    
def get_ist_now():
    return datetime.now(pytz.timezone("Asia/Kolkata"))


def get_cache_file(symbol):
    safe_symbol = symbol.replace(":", "_")
    return os.path.join(CACHE_DIR, f"{safe_symbol}_data.csv")


def get_macd_cache_file():
    return os.path.join(CACHE_DIR, "macd_cache.csv")


def ensure_cache(symbol):
    cache_file = get_cache_file(symbol)
    if os.path.exists(cache_file):
        return

    logger.info(f"📦 Building cache for {symbol}...")

    today = get_ist_now().date()
    range_from = (today - timedelta(days=DAYS_TO_CACHE)).strftime("%Y-%m-%d")
    range_to = today.strftime("%Y-%m-%d")

    try:
        df = fetch_candle_data(symbol, range_from, range_to)
        df.to_csv(cache_file, index=False)
        logger.info(f"✅ Cached {len(df)} rows for {symbol}")
    except Exception as e:
        logger.info(f"❌ Error while building cache for {symbol}: {e}")
        traceback.print_exc()


def update_cache(symbol):
    cache_file = get_cache_file(symbol)
    df_cache = pd.read_csv(cache_file, parse_dates=["datetime_ist"])

    last_time = df_cache["datetime_ist"].max()
    now = get_ist_now()

    # Only fetch for today
    range_from = now.strftime("%Y-%m-%d")
    range_to = now.strftime("%Y-%m-%d")

    try:
        df_new = fetch_candle_data(symbol, range_from, range_to)
        df_new = df_new[df_new["datetime_ist"] > last_time]
        if not df_new.empty:
            df_all = pd.concat([df_cache, df_new], ignore_index=True)
            df_all.to_csv(cache_file, index=False)
            logger.info(f"🔄 Updated cache for {symbol} with {len(df_new)} new rows")
        else:
            logger.info(f"ℹ️ No new data for {symbol}")
    except Exception as e:
        logger.info(f"❌ Error updating cache for {symbol}: {e}")
        traceback.print_exc()


def get_price_series(symbol):
    cache_file = get_cache_file(symbol)
    return pd.read_csv(cache_file, parse_dates=["datetime_ist"])


def compute_macd(series, fast_len, slow_len):
    ema_fast = series.ewm(span=fast_len, adjust=False).mean()
    ema_slow = series.ewm(span=slow_len, adjust=False).mean()
    return ema_fast - ema_slow

def fetch_and_check():
    logger.info(f"\n🕒 Running fetch_and_check at {get_ist_now().strftime('%Y-%m-%d %H:%M:%S')}")
    configs = load_spread_configs()
    for _, row in configs.iterrows():
        monitor_flag = str(row.get('monitor', 'TRUE')).strip().upper()
        if monitor_flag == 'TRUE':
            spread_name = row.get('SpreadName', 'Unnamed')
            long_symbol = row.get('LongInstrument')
            short_symbol = row.get('ShortInstrument')
            macd_fast = row.get('macd_fast')
            macd_slow = row.get('macd_slow')
            
            logger.info(f"Monitoring Spread: {spread_name} | Long: {long_symbol} | Short: {short_symbol} with MACD Settings as MACD Fast: {macd_fast} | MACD Slow: {macd_slow}")
            monitor_spread(row)

        else:
            logger.info(f"{row.get('SpreadName', 'Unnamed')}: Monitoring is disabled via CSV (monitor=FALSE)")

def schedule_task():
    now_ist = get_ist_now()
    if now_ist.time() >= datetime.strptime("09:20:01", "%H:%M:%S").time() and \
       now_ist.time() <= datetime.strptime("15:30:05", "%H:%M:%S").time():
        fetch_and_check()
    else:
        logger.info(f"🕔 {now_ist.strftime('%H:%M:%S')} — Outside trading window. Skipping.")

# === TELEGRAM ALERT SETUP ===
TELEGRAM_BOT_TOKEN = "7810420344:AAFMpUBPqiU9UvnI-aXxeiaMmpX5OpYLg80"  # Replace with your token
TELEGRAM_CHAT_ID = "787034385"  # Replace with your chat ID


def send_telegram_alert(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        }
        response = requests.post(url, data=payload)
        if response.status_code != 200:
            logger.info(f"⚠️ Telegram error: {response.text}")
        else:
            logger.info(f"📬 Telegram alert sent: {message}")
    except Exception as e:
        logger.info(f"❌ Failed to send Telegram alert: {e}")

# === EMAIL ALERT SETUP ===
EMAIL_SENDER = "vijaykumardas@gmail.com"       # Replace with your email
EMAIL_PASSWORD = "zeuq vwru zlho tajd"  # App password
EMAIL_RECEIVER = "vijaykumardas@gmail.com" # Replace with recipient email


def send_email_alert(subject, message):
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_SENDER
        msg['To'] = EMAIL_RECEIVER
        msg['Subject'] = subject

        msg.attach(MIMEText(message, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()

        logger.info(f"📧 Email alert sent: {subject}")
    except Exception as e:
        logger.info(f"❌ Failed to send email alert: {e}")

SPREAD_CONFIG_FILE = "spreads_config.csv"

def monitor_spread(row):
    spread_name = row['SpreadName']
    symbol_long = row['LongInstrument']
    symbol_short = row['ShortInstrument']
    fast_len = int(row['macd_fast'])
    slow_len = int(row['macd_slow'])

    try:
        ensure_cache(symbol_long)
        ensure_cache(symbol_short)
        update_cache(symbol_long)
        update_cache(symbol_short)

        df_long = get_price_series(symbol_long)
        df_short = get_price_series(symbol_short)

        df = pd.merge(df_short, df_long, on="datetime_ist", suffixes=("_short", "_long"))
        df["price_diff"] = df["close_short"] - df["close_long"]
        df["macd"] = compute_macd(df["price_diff"], fast_len, slow_len)

        macd_file = os.path.join(CACHE_DIR, f"{spread_name}_macd.csv")
        df.to_csv(macd_file, index=False)

        if len(df) >= 2:
            prev_macd = df.iloc[-2]["macd"]
            curr_macd = df.iloc[-1]["macd"]
            time_label = df.iloc[-1]["datetime_ist"]

            if prev_macd < 0 and curr_macd > 0:
                msg = f"📈BULLISH Alert  On  {spread_name} : MACD crossed ABOVE zero at {time_label}.\n\n\nSHORT {symbol_long} at : {df.iloc[-1]["close_long"]}, LONG {symbol_short} at : {df.iloc[-1]["close_short"]}."
                logger.info(msg)
                send_telegram_alert(msg)
                send_email_alert(f"{spread_name} - Bullish Crossover", msg)

            elif prev_macd > 0 and curr_macd < 0:
                msg = f"📉BEARISH Alert On  {spread_name} : MACD crossed BELOW zero at {time_label}.\n\n\nSHORT {symbol_short} at : {df.iloc[-1]["close_short"]}, LONG {symbol_long} at : {df.iloc[-1]["close_long"]}."
                logger.info(msg)
                send_telegram_alert(msg)
                send_email_alert(f"{spread_name} - Bearish Crossover", msg)
        else:
            logger.info(f"⚠️ {spread_name}: Not enough data to detect crossover")

    except Exception as e:
        logger.info(f"❌ Error in monitoring {spread_name}: {e}")
        logger.info(f"❌ Error in monitoring {spread_name}: {e}")
        traceback.print_exc()

def load_spread_configs():
    logger.info("Reading the Config File {SPREAD_CONFIG_FILE}")
    return pd.read_csv(SPREAD_CONFIG_FILE)

if __name__ == "__main__":
    #fetch_and_check()
    #send_telegram_alert("Hi Vijay. Alert from Python Code is here.")
    #send_email_alert("Nifty Call Spread Alert","Hi Vijay. Alert from Python Code is here.")
    
    # Scheduler starts here
    '''
    now = datetime.now()
    target_time = now.replace(hour=9, minute=20, second=1, microsecond=0)

    # If it's already past 9:20 AM, you might want to wait until 9:20 AM next day
    if now > target_time:
        target_time += timedelta(days=1)

    # Sleep until 9:20 AM
    time_to_wait = (target_time - now).total_seconds()
    if time_to_wait > 0:
        time.sleep(time_to_wait)
    '''    
    schedule.every(5).minutes.do(schedule_task)

    logger.info("📅 Scheduler started. Running every 5 minutes from 09:20:05 to 15:30 IST...")

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logger.info("🛑 Scheduler manually stopped.")
            break
        except Exception as e:
            logger.info(f"❌ Error in scheduler loop: {e}")
            traceback.print_exc()
            time.sleep(5)
    