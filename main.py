import os
import sys
import yfinance as yf
import time
import requests
import feedparser
import io
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlencode
import threading
import concurrent.futures


import urllib.request
import json



# Initialize VADER sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

def get_stock_symbols(min_price=1, max_price=20):
    """
    Fetches all NASDAQ stocks with last sale price below max_price using the NASDAQ stock screener API.
    
    Returns:
        list: List of stock symbols with last sale price below max_price.
    """
    url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=NASDAQ"
    headers = {
        "User-Agent": "Mozilla/5.0",  # Some endpoints require a user-agent
        "Accept": "application/json, text/plain, */*"
    }
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()  # Raises an error for bad responses
    
    # Parse the JSON response
    data = response.json()

    # Extract the stock data from the 'rows' key
    stocks = data['data']['table']['rows']
    
    # Convert the stock data into a pandas DataFrame for easier manipulation
    df = pd.DataFrame(stocks)
    
    # Clean the 'lastsale' column:
    # - Remove the '$' symbol
    # - Convert to numeric, coercing errors (e.g., 'n/a') to NaN
    df['lastsale'] = pd.to_numeric(df['lastsale'].str.replace('$', '', regex=False), errors='coerce')
    
    # Filter for stocks with last sale price below $max_price
    # NaN values will be excluded since NaN < max_price is False
    filtered_df = df[(df['lastsale'] >= min_price) & (df['lastsale'] < max_price)]
    
    # Extract the list of symbols from the filtered DataFrame
    symbols = filtered_df['symbol'].tolist()
    
    return symbols


MAX_NEWS_AGE = '1h'
GROUP_SIZE = 20

def fetch_news(symbols, print_lock, counter, counter_lock):
    """
    Fetches news for a group of stock symbols from Google News RSS feed.
    Args:
        symbols (list): List of stock symbols.
        print_lock (threading.Lock): Lock to synchronize console output.
    """
    query = f'({' OR '.join(symbols)}) AND stock when:{MAX_NEWS_AGE}'
    # Removed time.sleep(5) since staggering is handled in main()

    # Construct the Google News RSS feed URL
    base_url = "https://news.google.com/rss/search"
    params = {
        'q': query,
        'hl': 'en-US',  # Language: English
        'gl': 'US',    # Location: United States
        'ceid': 'US:en'
    }
    url = base_url + '?' + urlencode(params)

    # Fetch the RSS feed
    response = requests.get(url)
    if response.status_code != 200:
        with print_lock:
            print(f"Failed to fetch the RSS feed for {', '.join(symbols)}. Status code: {response.status_code}")
        return

    # Parse the RSS feed
    feed = feedparser.parse(response.text)

    news_list = []
    for entry in feed.entries:
        news_list.append(entry)

    # Update the news counter
    with counter_lock:
        counter[0] += len(news_list)

    # Display the results with synchronized output
    with print_lock:
        if not news_list:
            print(f"No news articles found for {', '.join(symbols)} in the past {MAX_NEWS_AGE}.")
        else:
            print(f"\nFound {len(news_list)} news articles for {', '.join(symbols)} in the past {MAX_NEWS_AGE}:\n")
            for i, news in enumerate(news_list, 1):
                print(f"Article {i}:")
                print(f"Title: {news.title}")
                print(f"Link: {news.link}")
                print(f"Published: {news.published}")
                print()

def main():
    """
    Main function to run the app:
    - Selects stocks based on criteria
    - Fetches news for all stock groups in parallel
    - Prints results to the console
    """
    # print starting time
    start_time = datetime.now()
    print(f"Start at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    stocks = get_stock_symbols()
    
    if not stocks:
        print("No stocks selected based on criteria. Exiting.")
        return
    
    print(f"Selected {len(stocks)} stocks")
    time.sleep(5)
    
    print("=" * 50)
    print(f"Update at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    stock_groups = [stocks[i:i + GROUP_SIZE] for i in range(0, len(stocks), GROUP_SIZE)]
    
    # Initialize a lock for synchronized printing
    print_lock = threading.Lock()
    counter_lock = threading.Lock()
    total_news_counter = [0]  # Using a list to allow modification in threads
    
    # Use ThreadPoolExecutor to fetch news in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for group in stock_groups:
            executor.submit(fetch_news, group, print_lock, total_news_counter, counter_lock)
            time.sleep(0.5)  # Stagger submissions to respect rate limits

    # Print the total number of news articles found
    print("=" * 50)
    print(f"Total news articles found across all groups: {total_news_counter[0]}")
    print("=" * 50)

    # print ending time
    print(f"End at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # print total execution time
    print(f"Total execution time: {datetime.now() - start_time}")

if __name__ == "__main__":
    main()