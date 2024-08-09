import collections
from collections import abc
collections.MutableMapping = abc.MutableMapping
collections.Mapping = abc.Mapping
collections.Sequence = abc.Sequence
collections.Callable = abc.Callable
import os
import time
from datetime import datetime
from binance.client import Client as bnc
import ccxt
import cbpro
import pandas as pd
import numpy as np


# write output to file
def output(message):
    with open(wire_path, "a") as wire:
        wire.write(message)


# find common assets between binance and coinbase api keys
def syncWallets():
    # binance
    try:
        bnc_nfo = bnc.get_exchange_info()
        bnc_assets = set(symbol['baseAsset'] for symbol in bnc_nfo['symbols'])
    except Exception as e:
        output(f"{e}\n\n")   
        return None
    output(f"{bnc_assets}\n{len(bnc_assets)} Binance\n\n")

    # coinbase
    try:
        cbc_nfo = cbc.fetch_currencies()
        cbc_assets = set(cbc_nfo.keys())
    except Exception as e:
        output(f"{e}\n\n")
    output(f"{cbc_assets}\n{len(cbc_assets)} Coinbase\n\n")
    
    # invalid includes stable and delisted
    shared = bnc_assets & cbc_assets
    invalid = {"REP", "CELO", "USDC", "DAI", "USDT", "AMP", "WBTC", "JASMY", "HNT", "SPELL"}
    valid = shared - invalid
    
    output(f"{shared}\n{len(shared)} Shared\n\n")
    output(f"{invalid}\n{len(invalid)} Invalid\n\n")
    output(f"{valid}\n{len(valid)} Valid\n\n")
    return valid


# get spot price
def getPrice(asset): 
    try:
        ticker = cbp.get_product_ticker(product_id = asset + "-USD")
        return float(ticker['price'])
    except Exception as e:  
        output(f"{asset} Error {e}\n")
        return None


##################
## BEGIN SCRIPT ##
##################

# track runtime and variables
start_time = time.time()
wire_path = "/home/dev/code/tmp/" + str(datetime.now().strftime("%Y-%m-%d %H-%M-%S")) + ".txt"
cbp = cbpro.PublicClient()
bnc = bnc(os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET"), tld = "us")
cbc = ccxt.coinbase({'apiKey': os.getenv("COINBASE_API_KEY"), 'secret': os.getenv("COINBASE_API_SECRET")})
pd.set_option('display.max_columns', None)

# main
valid = syncWallets()

# candlestick dataframes
for asset in valid:
    try:    
        # fetch kline data
        klines = bnc.get_klines(symbol = asset + "USDT", interval = bnc.KLINE_INTERVAL_30MINUTE, limit = 1000)
        df = pd.DataFrame(klines, columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore'])
        df = df.rename(columns = {'Close Time': 'ds', 'Number of Trades': 'Trades'})
        df['ds'] = pd.to_datetime(df['ds'], unit = 'ms')
        
        # convert multiple columns to float64
        df = df.astype({'Open': 'float64', 'High': 'float64', 'Low': 'float64', 'Close': 'float64', 'Volume': 'float64'})

        # log of one plus percentage change is the target variable
        df['y'] = np.log(1 + df['Close'].pct_change())
             
        # remove redundant columns
        df = df[['ds', 'y', 'Open', 'High', 'Low', 'Close', 'Volume', 'Trades']]
   
        output(f"{asset}\n{df}\n{df.dtypes}\n\n")
    except Exception as e:
        output(f"{asset} Error {e}\n")

# capture prices at time of trade
trade_prices = dict()
for asset in valid:
    try:
        price = getPrice(asset)
        if price:
            trade_prices[asset] = price
    except Exception as e:
        output(f"{asset} Error {e}\n")
output(f"{trade_prices}\n{len(trade_prices)} Prices\n\n")

# calculate runtime
end_time = time.time()
seconds = end_time - start_time
output(f"Runtime\n")
output(f"{round(seconds, 2)} seconds\n")
output(f"{round((seconds / 60), 2)} minutes\n\n")

