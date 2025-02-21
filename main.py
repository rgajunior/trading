import requests
import csv
import time
from datetime import datetime, timedelta
import yfinance as yf
from textblob import TextBlob
import feedparser

# Constants
NASDAQ_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
CSV_FILE = "filtered_stocks.csv"
CACHE_DURATION = 24 * 60 * 60  # 24 hours in seconds
NEWS_CACHE = {}  # {news_id: (sentiment, timestamp)}
RSS_FEED_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^IXIC"  # NASDAQ news feed

### Step 1: Fetch NASDAQ Symbols
def fetch_nasdaq_symbols():
    """Fetch all NASDAQ stock symbols from the provided URL."""
    try:
        response = requests.get(NASDAQ_URL)
        response.raise_for_status()
        lines = response.text.splitlines()
        # Extract symbols from each line (format: SYMBOL|NAME|...)
        symbols = [line.split('|')[0] for line in lines if '|' in line and 'Symbol' not in line]
        return symbols
    except requests.RequestException as e:
        print(f"Error fetching NASDAQ symbols: {e}")
        return []

### Step 2: Filter Stocks by Price and Float
def filter_stocks(symbols):
    """Filter stocks with price between $2-$20 and float < 20M using yfinance."""
    filtered = []
    for symbol in symbols:
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            price = info.get('regularMarketPrice', 0)  # Current price
            float_shares = info.get('floatShares', float('inf'))  # Float shares
            if 2 <= price <= 20 and float_shares < 20_000_000:
                filtered.append(symbol)
                print(f"Filtered {symbol}: Price={price}, Float={float_shares}")

                # break loop when filtered stocks reach 5
                if len(filtered) == 5:
                    break

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
    return filtered

def save_to_csv(symbols):
    """Save filtered symbols to a CSV file with a timestamp."""
    with open(CSV_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Symbol', 'Timestamp'])
        timestamp = time.time()
        for symbol in symbols:
            writer.writerow([symbol, timestamp])

def load_from_csv():
    """Load filtered symbols from CSV if within 24 hours."""
    try:
        with open(CSV_FILE, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            data = list(reader)
            if data and (time.time() - float(data[0][1])) < CACHE_DURATION:
                return [row[0] for row in data]
            else:
                return None
    except (FileNotFoundError, IndexError, ValueError):
        return None

def get_filtered_stocks():
    """Get filtered stocks, using cache if available and fresh."""
    cached = load_from_csv()
    if cached is not None:
        print("Using cached stock data.")
        return cached
    print("Fetching new stock data...")
    symbols = fetch_nasdaq_symbols()
    filtered = filter_stocks(symbols)
    save_to_csv(filtered)
    return filtered

### Step 3: Fetch and Analyze News
def fetch_news():
    """Fetch latest NASDAQ-related news from an RSS feed."""
    try:
        feed = feedparser.parse(RSS_FEED_URL)
        if feed.bozo:
            print(f"Error parsing RSS feed: {feed.bozo_exception}")
            return []
        return feed.entries
    except Exception as e:
        print(f"Error fetching news: {e}")
        return []

def analyze_sentiment(text):
    """Analyze sentiment of text on a scale from -10 to 10."""
    analysis = TextBlob(text)
    sentiment = analysis.sentiment.polarity * 10  # Convert polarity (-1 to 1) to -10 to 10
    return round(sentiment, 2)

def process_news(news_items, symbols):
    """Process news items, identify symbols, and assign sentiment."""
    current_time = time.time()
    sentiments = {symbol: 0 for symbol in symbols}  # Default to neutral

    for item in news_items:
        # Use 'link' or 'id' as a unique identifier; fall back to title if missing
        news_id = item.get('id', item.get('link', item.title))
        # Check cache first
        if news_id in NEWS_CACHE and (current_time - NEWS_CACHE[news_id][1]) < 7200:  # 2 hours
            sentiment, _ = NEWS_CACHE[news_id]
        else:
            # Combine title and summary (if available) for analysis
            text = item.title + " " + item.get('summary', '')
            sentiment = analyze_sentiment(text)
            NEWS_CACHE[news_id] = (sentiment, current_time)

        # Match stock symbols in the news text
        for symbol in symbols:
            if symbol in text:
                # Only update if news is within the last hour
                if (current_time - time.mktime(item.published_parsed)) < 3600:  # 1 hour
                    sentiments[symbol] = sentiment
                    print(f"News for {symbol}: '{item.title}' - Sentiment: {sentiment}")
                break

    return sentiments

def clean_cache():
    """Remove news cache entries older than 2 hours."""
    current_time = time.time()
    to_remove = [k for k, v in NEWS_CACHE.items() if (current_time - v[1]) > 7200]
    for k in to_remove:
        del NEWS_CACHE[k]

### Main Application Loop
def main():
    """Main loop to continuously monitor and analyze stock news."""
    print("Starting Live Stock News Research App...")
    while True:
        # Get filtered stock list (cached or fresh)
        symbols = get_filtered_stocks()
        if not symbols:
            print("No stocks found or error occurred. Retrying in 60 seconds...")
            time.sleep(60)
            continue

        # Fetch and process news
        news_items = fetch_news()
        if not news_items:
            print("No news fetched. Retrying in 60 seconds...")
            time.sleep(60)
            continue

        sentiments = process_news(news_items, symbols)
        
        # Display current sentiments (could be used for trading decisions)
        for symbol, sentiment in sentiments.items():
            if sentiment != 0:  # Only show non-neutral sentiments
                print(f"{symbol}: Sentiment = {sentiment}")

        # Clean up old cache entries
        clean_cache()

        # Wait before the next iteration
        print(f"Waiting 60 seconds before next check... (Cache size: {len(NEWS_CACHE)})")
        time.sleep(10)

if __name__ == "__main__":
    main()