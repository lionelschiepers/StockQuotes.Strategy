import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import json

# Configuration
BATCH_SIZE = 50
PRICE_LIMIT = 100
HIST_DAYS = 90
BASE_URL = "https://stockquote.lionelschiepers.synology.me/api/yahoo-finance"
HIST_URL = "https://stockquote.lionelschiepers.synology.me/api/yahoo-finance-historical"

def get_tickers():
    with open('tickers.json', 'r') as f:
        return json.load(f)

def safe_get(url):
    while True:
        try:
            response = requests.get(url)
            if response.status_code == 429:
                retry_after = response.json().get('retryAfter', 5)
                print(f"Rate limited. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None

def batch_price_filter(tickers):
    candidates = []
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i+BATCH_SIZE]
        symbols = ",".join(batch)
        url = f"{BASE_URL}?symbols={symbols}&fields=symbol,shortName,regularMarketPrice"
        data = safe_get(url)
        if data:
            for item in data:
                price = item.get('regularMarketPrice')
                if price is not None and price < PRICE_LIMIT:
                    candidates.append({
                        'symbol': item['symbol'],
                        'price': price,
                        'name': item.get('shortName', '')
                    })
        time.sleep(0.5)
    return candidates

def calculate_ema(series, span):
    return series.ewm(span=span, adjust=False).mean()

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0))
    loss = (-delta.where(delta < 0, 0))
    
    # Wilder's Smoothing
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_adx(df, period=14):
    df = df.copy()
    df['up_move'] = df['high'].diff()
    df['down_move'] = -df['low'].diff()
    
    df['plus_dm'] = np.where((df['up_move'] > df['down_move']) & (df['up_move'] > 0), df['up_move'], 0)
    df['minus_dm'] = np.where((df['down_move'] > df['up_move']) & (df['down_move'] > 0), df['down_move'], 0)
    
    df['tr'] = np.maximum(df['high'] - df['low'], 
                          np.maximum(abs(df['high'] - df['close'].shift(1)), 
                                     abs(df['low'] - df['close'].shift(1))))
    
    atr = df['tr'].ewm(alpha=1/period, adjust=False).mean()
    plus_di = 100 * (df['plus_dm'].ewm(alpha=1/period, adjust=False).mean() / atr)
    minus_di = 100 * (df['minus_dm'].ewm(alpha=1/period, adjust=False).mean() / atr)
    
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    adx = dx.ewm(alpha=1/period, adjust=False).mean()
    return adx

def calculate_macd(series):
    ema12 = calculate_ema(series, 12)
    ema26 = calculate_ema(series, 26)
    macd = ema12 - ema26
    signal = calculate_ema(macd, 9)
    return macd, signal

def calculate_rvi(series, std_period=10, smooth_period=14):
    std = series.rolling(window=std_period).std()
    diff = series.diff()
    
    up = np.where(diff > 0, std, 0)
    down = np.where(diff < 0, std, 0)
    
    up_series = pd.Series(up, index=series.index)
    down_series = pd.Series(down, index=series.index)
    
    # Wilder's Smoothing
    avg_up = up_series.ewm(alpha=1/smooth_period, adjust=False).mean()
    avg_down = down_series.ewm(alpha=1/smooth_period, adjust=False).mean()
    
    rvi = 100 * (avg_up / (avg_up + avg_down))
    return rvi

def deep_analysis(candidates):
    results = []
    end_date = datetime(2026, 2, 15)
    start_date = end_date - timedelta(days=HIST_DAYS)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    total = len(candidates)
    for idx, c in enumerate(candidates, 1):
        symbol = c['symbol']
        print(f"[{idx}/{total}] Analyzing {symbol}...", end='\r', flush=True)
        url = f"{HIST_URL}?ticker={symbol}&from={start_str}&to={end_str}"
        data = safe_get(url)
        if not data:
            continue
            
        quotes = data.get('quotes', [])
        if len(quotes) < 50:
            continue
            
        df = pd.DataFrame(quotes)
        if df.empty:
            continue
            
        if isinstance(df['date'].iloc[0], str):
            df['date'] = pd.to_datetime(df['date'])
        else:
            df['date'] = pd.to_datetime(df['date'], unit='s')
        df = df.sort_values('date')
        
        close = df['close']
        price = close.iloc[-1]
        
        ema50 = calculate_ema(close, 50).iloc[-1]
        rsi_series = calculate_rsi(close, 14)
        rsi_today = rsi_series.iloc[-1]
        rsi_3d_ago = rsi_series.iloc[-4] if len(rsi_series) >= 4 else None
        
        adx = calculate_adx(df, 14).iloc[-1]
        macd, macd_signal = calculate_macd(close)
        rvi = calculate_rvi(close, 10, 14).iloc[-1]
        
        # Filters
        if price > ema50 and adx < 30 and 30 <= rsi_today <= 50:
            if rsi_3d_ago is not None and rsi_today > rsi_3d_ago:
                diff_pct = ((price - ema50) / ema50) * 100
                results.append({
                    'Symbol': symbol,
                    'Name': c['name'],
                    'Price': round(price, 2),
                    'EMA50': round(ema50, 2),
                    'ADX': round(adx, 2),
                    'RSI': round(rsi_today, 2),
                    'RVI': round(rvi, 2),
                    'MACD': round(macd.iloc[-1], 2),
                    'Signal': round(macd_signal.iloc[-1], 2),
                    'DiffPct': round(diff_pct, 2)
                })
        time.sleep(0.5)
    return results

def main():
    print("Fetching tickers...")
    tickers = get_tickers()
    
    print(f"Phase 1: Screening {len(tickers)} tickers for price < ${PRICE_LIMIT}...")
    candidates = batch_price_filter(tickers)
    print(f"Found {len(candidates)} candidates.")
    
    print("Phase 2 & 3: Deep analysis and filtering...")
    final_results = deep_analysis(candidates)
    
    print("Phase 4: Sorting and Reporting...")
    final_results.sort(key=lambda x: x['DiffPct'])
    
    df_results = pd.DataFrame(final_results)
    if not df_results.empty:
        print("\nFinal Report:")
        print(df_results.to_string(index=False))
    else:
        print("\nNo stocks matched the criteria.")

if __name__ == "__main__":
    main()
