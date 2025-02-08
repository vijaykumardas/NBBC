import pandas as pd
from datetime import datetime
import os
import logging
import sys
import dropbox
from DropboxClient import DropboxClient

def validate_portfolio_columns(portfolio):
    missing_columns = [col for col in required_columns if col not in portfolio.columns]
    if missing_columns:
        print(f"Error: Missing columns in portfolio CSV: {', '.join(missing_columns)}")
        sys.exit(1)  # Exit the script with an error code

def fetch_latest_price(ticker,portfolioDate):
    """Fetch the latest stock price for the given ticker from a CSV file."""
    # Get today's date and construct the filename
    today = portfolioDate.strftime('%Y-%m-%d')
    filename = f"{today}-NSE-BSE-IS-ALL-EQ.CSV"
    
    file_path = os.path.join(price_data_dir, filename)
    
    # Check if the file exists
    if not os.path.exists(file_path):
        logging.error(f"Price data file not found: {file_path}")
        return None
    
    try:
        # Read the CSV file
        price_data = pd.read_csv(file_path)
        
        # Filter data for the specified ticker
        ticker_data = price_data[price_data['TICKER'] == ticker]
        
        # If ticker data is not empty, return the closing price rounded to 2 decimal places
        if not ticker_data.empty:
            latest_close_price = round(ticker_data.iloc[0]['CLOSE'], 2)
            return latest_close_price
        else:
            logging.warning(f"Ticker {ticker} not found in the price data file.")
            return None
    except Exception as e:
        logging.error(f"Error fetching latest price: {e}")
        return None

def fetch_ohlc_data(ticker,portfolioDate):
    """Fetch OHLC data for the given ticker from a CSV file."""
    # Get today's date and construct the filename
    today = portfolioDate.strftime('%Y-%m-%d')
    filename = f"{today}-NSE-BSE-IS-ALL-EQ.CSV"
    file_path = os.path.join(price_data_dir, filename)
    
    # Check if the file exists
    if not os.path.exists(file_path):
        logging.error(f"OHLC data file not found: {file_path}")
        return {
            'Open': None,
            'High': None,
            'Low': None,
            'Close': None
        }
    
    try:
        # Read the CSV file
        ohlc_data = pd.read_csv(file_path)
        
        # Filter data for the specified ticker
        ticker_data = ohlc_data[ohlc_data['TICKER'] == ticker]
        
        # If ticker data is not empty, return OHLC data rounded to 2 decimal places
        if not ticker_data.empty:
            return {
                'Open': round(float(ticker_data.iloc[0]['OPEN']), 2),
                'High': round(float(ticker_data.iloc[0]['HIGH']), 2),
                'Low': round(float(ticker_data.iloc[0]['LOW']), 2),
                'Close': round(float(ticker_data.iloc[0]['CLOSE']), 2)
            }
        else:
            logging.warning(f"Ticker {ticker} not found in the OHLC data file.")
            return {
                'Open': None,
                'High': None,
                'Low': None,
                'Close': None
            }
    except Exception as e:
        logging.error(f"Error fetching OHLC data for {ticker}: {e}")
        return {
            'Open': None,
            'High': None,
            'Low': None,
            'Close': None
        }

def update_portfolio_with_latest_prices(portfolio,portfolioDate):
    """Update portfolio with the latest prices and current value."""
    # Filter out sold entries
    unsold_portfolio = portfolio[portfolio['Sell Date'].isna()].copy()
    
    # Add column for latest closing prices and current value
    unsold_portfolio['Current Price'] = unsold_portfolio['Ticker'].apply(fetch_latest_price,portfolioDate)
    unsold_portfolio['Current Value'] = round(unsold_portfolio['Current Price'] * unsold_portfolio['Quantity'],2)
    
    # Update only unsold entries
    updated_portfolio = portfolio.copy()
    updated_portfolio.update(unsold_portfolio[['Ticker', 'Current Price', 'Current Value']])
    
    # Set Current Price and Current Value to zero for sold entries
    sold_portfolio = portfolio[~portfolio['Sell Date'].isna()].copy()
    sold_portfolio['Current Price'] = 0.0
    sold_portfolio['Current Value'] = 0.0
    
    # Reorder the portfolio: sold entries first, then unsold entries
    ordered_portfolio = pd.concat([sold_portfolio, unsold_portfolio], ignore_index=True)
    ordered_portfolio = ordered_portfolio.sort_values(by=['Sell Date', 'Buy Date'], ascending=[False, True])
    
    return ordered_portfolio

def update_portfolio_with_ohlc(portfolio,portfolioDate):
    
    """Update portfolio with OHLC data."""
    # Filter out unsold entries
    unsold_portfolio = portfolio[portfolio['Sell Date'].isna()].copy()
    
    # Fetch OHLC data for unsold entries
    ohlc_data = [fetch_ohlc_data(ticker,portfolioDate) for ticker in unsold_portfolio['Ticker']]
    ohlc_df = pd.DataFrame(ohlc_data)
    
    # Ensure the DataFrame aligns with the unsold portfolio by resetting index
    unsold_portfolio = unsold_portfolio.reset_index(drop=True)
    
    # Concatenate OHLC data with unsold portfolio
    unsold_portfolio = pd.concat([unsold_portfolio, ohlc_df], axis=1)
    return unsold_portfolio

def save_portfolio(portfolio, filename):
    """Save updated portfolio with latest prices to CSV."""
    column_order = [
        'Portfolio Name', 'Ticker', 'Buy Date', 'Buy Price', 'Quantity', 'Sell Date', 'Sell Price', 'Current Price',
        'Current Value']
    portfolio = portfolio[column_order]
    portfolio.to_csv(filename, index=False)

def calculate_ohlc_summary(portfolio):
    """Calculate OHLC summary for each portfolio."""
    # Filter out sold entries
    unsold_portfolio = portfolio[portfolio['Sell Date'].isna()]
    
    # Calculate OHLC summary for each portfolio
    portfolio_summary = unsold_portfolio.groupby('Portfolio Name').agg(
        Open=('Open', lambda x: round((x * unsold_portfolio.loc[x.index, 'Quantity']).sum(), 2)),
        High=('High', lambda x: round((x * unsold_portfolio.loc[x.index, 'Quantity']).sum(), 2)),
        Low=('Low', lambda x: round((x * unsold_portfolio.loc[x.index, 'Quantity']).sum(), 2)),
        Close=('Close', lambda x: round((x * unsold_portfolio.loc[x.index, 'Quantity']).sum(), 2)),
    ).reset_index()

    # Add a total summary row for all portfolios
    total_summary = pd.DataFrame({
    'Portfolio Name':'ALLPORTFOLIO',
    'Open': round(portfolio_summary['Open'].sum(),2),
    'High': round(portfolio_summary['High'].sum(),2),
    'Low': round(portfolio_summary['Low'].sum(),2),
    'Close': round(portfolio_summary['Close'].sum(),2)
    },index=['Portfolio Name'])

    # Append the total summary to the portfolio summary
    portfolio_summary = pd.concat([portfolio_summary, total_summary], ignore_index=True)
    
    return portfolio_summary

def save_ohlc_summary(summary,portfolioDate):
    """Save OHLC summary to a new CSV file with date and additional columns."""
    # Construct the filename
    today = portfolioDate.strftime('%Y-%m-%d')
    filename = f"{today}-PORTFOLIOSUMMARY.CSV"
    file_path = os.path.join(price_data_dir, filename)
    
    # Add additional columns with empty values
    additional_columns = [
        'VOLUME', 'INDUSTRYNAME', 'SECTORNAME', 'ALIAS', 'ADDRESS',
        'COUNTRY', 'CURRENCY', 'OPENINT', 'AUX1', 'AUX2'
    ]
    
    for col in additional_columns:
        summary[col] = ''
    
    # Add DATE_YMD column with today's date
    summary['DATE_YMD'] = portfolioDate.strftime('%Y%m%d')
    
    # Rename 'Portfolio Name' to 'TICKER'
    summary.rename(columns={'Portfolio Name': 'TICKER',"Open":"OPEN","High":"HIGH","Low":"LOW","Close":"CLOSE"}, inplace=True)
    
    # Add FULLNAME column with the same value as TICKER
    summary['FULLNAME'] = summary['TICKER']
    summary['INDUSTRYNAME'] = 'Portfolio'
    summary['SECTORNAME'] = 'Portfolios'
    # Reorder columns
    column_order = [
        'DATE_YMD', 'TICKER', 'FULLNAME', 'OPEN', 'HIGH', 'LOW', 'CLOSE',
        'VOLUME', 'INDUSTRYNAME', 'SECTORNAME', 'ALIAS', 'ADDRESS',
        'COUNTRY', 'CURRENCY', 'OPENINT', 'AUX1', 'AUX2'
    ]
    summary = summary[column_order]
    
    # Save the DataFrame to a CSV file
    #summary.to_csv(file_path, index=False)
    #print(file_path + ": ==> OK")
    return summary

# File paths
portfolio_file = 'portfolio.csv'
price_data_dir = os.getcwd()  # Set to the current working directory
ohlc_summary_file = ''  # To be set dynamically based on the date

# Required columns in portfolio.csv
required_columns = ['Ticker', 'Quantity', 'Portfolio Name', 'Buy Date', 'Buy Price', 'Sell Date', 'Sell Price', 'Current Price', 'Current Value']

def main(portfolioDate):

    if (portfolioDate == ''):
        portfolioDate=datetime.today()

    dropBoxClient= DropboxClient()
    # Download the portfolio.csv from Drop Box.
    dropBoxClient.download_file("/NSEBSEBhavcopy/Portfolio/portfolio.csv", "portfolio.csv")
    # Load portfolio from CSV
    portfolio = pd.read_csv(portfolio_file)

    # Validate columns in portfolio CSV
    validate_portfolio_columns(portfolio)

    # Update the portfolio with the latest closing prices
    updated_portfolio = update_portfolio_with_latest_prices(portfolio,portfolioDate)

    # Save the updated portfolio to CSV
    save_portfolio(updated_portfolio, portfolio_file)

    # upload the portfolio file to DropBox.
    portfolio_filepath=os.path.abspath("portfolio.csv")
    dropBoxClient.upload_file(portfolio_filepath, "/NSEBSEBhavcopy/Portfolio/portfolio.csv")
    
    # Update portfolio with OHLC data
    portfolio_with_ohlc = update_portfolio_with_ohlc(updated_portfolio,portfolioDate)
    #print(portfolio_with_ohlc)
    # Calculate OHLC summary
    ohlc_summary = calculate_ohlc_summary(portfolio_with_ohlc)
    #print("OHLC Portfolio Summary:")
    #print(ohlc_summary)

    # Save OHLC summary to a separate CSV file
    ohlc_summary=save_ohlc_summary(ohlc_summary,portfolioDate)
    return ohlc_summary

if __name__ == "__main__":
    main()
