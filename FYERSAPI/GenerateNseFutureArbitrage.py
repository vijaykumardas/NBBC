#!/usr/bin/env python3

"""
NSE Futures Arbitrage Info Generator with Market Timing Control and Email Notification

Reads SYMBOL, FUT1, FUT2 from CSV and writes spot/future prices and diffs to output CSV.

Filters arbitrage opportunities by FUT1_FUT2_%DIFF outside (0.3, 0.8) and sends email if any found.

Runs once or every 5 minutes only during NSE market hours (configurable).
"""

import csv
import os
import sys
import time
from datetime import datetime, time as dtime, timedelta
import pytz
from fyers_apiv3 import fyersModel
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Optional: tqdm for progress bar (install with pip if not available)
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

# === Configurable Parameters ===
ADHERE_TO_NSE_TIMING = False      # Set False for single run (debugging)
RUN_INTERVAL_MINS = 5            # Interval between runs within market hours
TIMEZONE = 'Asia/Kolkata'        # NSE market timezone

# NSE market timings (IST)
MARKET_OPEN_TIME = dtime(9, 15)
MARKET_CLOSE_TIME = dtime(15, 30)
MARKET_DAYS = {0, 1, 2, 3, 4}    # Monday=0 ... Friday=4

# Email configuration
EMAIL_SENDER = "vijaykumardas@gmail.com"
EMAIL_PASSWORD = "zeuq vwru zlho tajd"  # Use app password
EMAIL_RECEIVER = "vijaykumardas@gmail.com"

# Fyers API initialization
CLIENT_ID = open("FYERS_APP_ID.TXT", "r").read().strip()  # e.g., 'YOUR_APP_ID-100'
ACCESS_TOKEN = open("FYERS_ACCESS_TOKEN.TXT", "r").read().strip()  # obtained via auth flow
fyers = fyersModel.FyersModel(token=ACCESS_TOKEN, is_async=False, client_id=CLIENT_ID)

# Input and output files
INPUT_CSV = 'NSEArbitrageSymbolConfig.csv'
OUTPUT_CSV = 'NSEArbitrageInfo.csv'
OPPORTUNITY_CSV = 'NSEArbitrageOppertunity.csv'

BATCH_SIZE = 40

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def batch_fetch_prices(symbols):
    """Fetch prices for list of symbols in batch from Fyers API."""
    prices = {}
    total_batches = (len(symbols) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"Fetching prices in {total_batches} batch{'es' if total_batches > 1 else ''}...")
    for batch_num, batch in enumerate(chunks(symbols, BATCH_SIZE), start=1):
        batch_symbols = ",".join(batch)
        try:
            resp = fyers.quotes({"symbols": batch_symbols})
            if resp.get('s') == 'ok' and resp.get('d'):
                for item in resp['d']:
                    if item.get('s') == 'ok':
                        sym = item.get('n')
                        lp = item['v'].get('lp', 'NA')
                        try:
                            prices[sym] = float(lp)
                        except:
                            prices[sym] = 'NA'
            else:
                print(f"Warning: Response not OK for batch {batch_num}")
        except Exception as e:
            print(f"Error fetching batch {batch_num}: {e}")
        print(f" Completed batch {batch_num} of {total_batches}")
    return prices

def calc_diff(fut_price, spot_price):
    if isinstance(fut_price, float) and isinstance(spot_price, float) and spot_price != 0:
        diff = fut_price - spot_price
        pct = (diff / spot_price) * 100
        return diff, pct
    return 'NA', 'NA'

def try_round(val):
    if isinstance(val, float):
        return round(val, 2)
    return val

def csv_to_html_table(csv_file_path):
    """Convert CSV file content to an HTML table string."""
    html = "<html><body><h3>NSE Arbitrage Opportunities</h3><table border='1' cellpadding='4' cellspacing='0' style='border-collapse:collapse'>"
    with open(csv_file_path, newline='') as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            tag = 'th' if i == 0 else 'td'
            html += "<tr>" + "".join(f"<{tag} style='padding:6px'>{cell}</{tag}>" for cell in row) + "</tr>"
    html += "</table></body></html>"
    return html

def send_email_with_attachment(subject, html_body, attachment_path):
    """Send email with HTML body and attachment."""
    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    msg['Subject'] = subject

    # Attach HTML body
    msg.attach(MIMEText(html_body, 'html'))

    # Attach file
    with open(attachment_path, 'rb') as f:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(attachment_path)}"')
        msg.attach(part)

    # Connect and send
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {e}")

def is_market_open():
    """Check if current IST time is within NSE market hours Monday to Friday."""
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    if now.weekday() not in MARKET_DAYS:
        return False
    current_time = now.time()
    if MARKET_OPEN_TIME <= current_time <= MARKET_CLOSE_TIME:
        return True
    return False

def wait_until_market_open():
    """Sleep until market open time on next trading day (if not open now)."""
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    if is_market_open():
        return  # Already open

    # Calculate next open datetime
    days_ahead = 0
    day_check = now.weekday()
    while True:
        day_check = (day_check + 1) % 7
        days_ahead += 1
        if day_check in MARKET_DAYS:
            break
    next_open_date = (now + timedelta(days=days_ahead)).date()
    next_open_datetime = datetime.combine(next_open_date, MARKET_OPEN_TIME)
    next_open_datetime = tz.localize(next_open_datetime)
    sleep_seconds = (next_open_datetime - now).total_seconds()
    if sleep_seconds > 0:
        print(f"Market closed now. Sleeping until next market open at {next_open_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        time.sleep(sleep_seconds)

def process():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Reading input file '{INPUT_CSV}'...")
    rows = []
    unique_symbols = set()
    with open(INPUT_CSV, newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            rows.append(row)
            unique_symbols.add(row['SYMBOL'])
            unique_symbols.add(row['FUT1'])
            unique_symbols.add(row['FUT2'])
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Total symbols to fetch prices for: {len(unique_symbols)}")

    prices_dict = batch_fetch_prices(list(unique_symbols))

    # Write full output to OUTPUT_CSV
    fieldnames = [
        'UNDERLAYING','SYMBOL', 'SPOT_PRICE',
        'FUT1_PRICE', 'FUT1_DIFF', 'FUT1_DIFF%',
        'FUT2_PRICE', 'FUT2_DIFF', 'FUT2_DIFF%',
        'FUT1_FUT2_%DIFF'
    ]
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Writing output to '{OUTPUT_CSV}'...")
    with open(OUTPUT_CSV, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        row_iter = tqdm(rows, desc="Processing symbols", unit="symbol") if TQDM_AVAILABLE else rows
        total_written = 0
        failed_spot = 0

        filtered_rows = []  # For filtered arbitrage opportunities

        for row in row_iter:
            symbol = row['SYMBOL']
            fut1 = row['FUT1']
            fut2 = row['FUT2']

            spot = prices_dict.get(symbol, 'NA')
            p1 = prices_dict.get(fut1, 'NA')
            p2 = prices_dict.get(fut2, 'NA')

            if spot == 'NA':
                failed_spot += 1

            d1, pct1 = calc_diff(p1, spot)
            d2, pct2 = calc_diff(p2, spot)

            fut1_fut2_diff = 'NA'
            if isinstance(pct2, float) and isinstance(pct1, float):
                fut1_fut2_diff = pct2 - pct1

            out_row = {
                'UNDERLAYING': row.get('SYMBOLNAME', ''),
                'SYMBOL': symbol,
                'SPOT_PRICE': try_round(spot),
                'FUT1_PRICE': try_round(p1),
                'FUT1_DIFF': try_round(d1),
                'FUT1_DIFF%': try_round(pct1),
                'FUT2_PRICE': try_round(p2),
                'FUT2_DIFF': try_round(d2),
                'FUT2_DIFF%': try_round(pct2),
                'FUT1_FUT2_%DIFF': try_round(fut1_fut2_diff)
            }

            writer.writerow(out_row)
            total_written += 1

            # Filter arbitrage opportunities: FUT1_FUT2_%DIFF not between 0.3 and 0.8 exclusive
            try:
                val = float(fut1_fut2_diff)
                if val < 0.3 or val > 0.8:
                    filtered_rows.append(out_row)
            except:
                # skip if not a float
                pass

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Completed processing {total_written} symbols.")
    if failed_spot > 0:
        print(f"Warning: Spot price not found for {failed_spot} symbols.")
    print(f"Output written to {OUTPUT_CSV} in {os.getcwd()}")

    # Write filtered arbitrage opportunities if any
    if filtered_rows:
        with open(OPPORTUNITY_CSV, 'w', newline='') as fop:
            writer = csv.DictWriter(fop, fieldnames=fieldnames)
            writer.writeheader()
            for r in filtered_rows:
                writer.writerow(r)

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Arbitrage opportunities written to '{OPPORTUNITY_CSV}'.")

        # Construct HTML table for email
        html_table = csv_to_html_table(OPPORTUNITY_CSV)

        # Email subject and body
        subject = "NSE Arbitrage Opportunities Alert"
        body = html_table

        # Send email
        send_email_with_attachment(subject, body, OPPORTUNITY_CSV)
    else:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] No arbitrage opportunities found; no email sent.")

def main_loop():
    """Main loop to run arbitrage process respecting NSE timing if enabled."""
    if not ADHERE_TO_NSE_TIMING:
        # Run once and exit (debug mode)
        print("Running once without NSE timing adherence (debug mode).")
        process()
        return

    # Loop running every RUN_INTERVAL_MINS within market hours
    while True:
        if is_market_open():
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] NSE market is open. Running process.")
            process()
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sleeping for {RUN_INTERVAL_MINS} minutes before next run.")
            time.sleep(RUN_INTERVAL_MINS * 60)
        else:
            wait_until_market_open()

if __name__ == '__main__':
    main_loop()
