import collections
from collections import abc
collections.MutableMapping = abc.MutableMapping
collections.Mapping = abc.Mapping
collections.Sequence = abc.Sequence
collections.Callable = abc.Callable
import os
import sys
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
def sync_wallets():
    try:
        # binance
        bnc_nfo = bnc.get_exchange_info()
        bnc_assets = set(symbol['baseAsset'] for symbol in bnc_nfo['symbols'])
        output(f"{bnc_assets}\n{len(bnc_assets)} Binance\n\n")

        # coinbase
        cbc_nfo = cbc.fetch_currencies()
        cbc_assets = set(cbc_nfo.keys())
        output(f"{cbc_assets}\n{len(cbc_assets)} Coinbase\n\n")
        
        # invalid includes stable and delisted
        shared = bnc_assets & cbc_assets
        invalid = {"REP", "CELO", "USDC", "DAI", "USDT", "AMP", "WBTC", "JASMY", "HNT", "SPELL"}
        sw_valid = shared - invalid
        
        output(f"{shared}\n{len(shared)} Shared\n\n")
        output(f"{invalid}\n{len(invalid)} Invalid\n\n")
        output(f"{sw_valid}\n{len(sw_valid)} Valid\n\n")
        return sw_valid
    except Exception as e:
        output(f"sync_wallets() Error {e}\n\n")
        return None


# get candlestick dataframes
def get_data(gd_valid):
    gd_data = dict()   # asset: df
    try:
        for asset in gd_valid:
            current_asset = asset

            # fetch kline data
            klines = bnc.get_klines(symbol = asset + "USDT", interval = bnc.KLINE_INTERVAL_30MINUTE, limit = 1000)
            df = pd.DataFrame(klines, columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore'])
            df = df.rename(columns = {'Close Time': 'ds', 'Number of Trades': 'Trades'})
            df['ds'] = pd.to_datetime(df['ds'], unit = 'ms')
            
            # convert multiple ohlcv to float64
            df = df.astype({'Open': 'float64', 'High': 'float64', 'Low': 'float64', 'Close': 'float64', 'Volume': 'float64'})

            # log of one plus percentage change is the target variable
            df['y'] = np.log(1 + df['Close'].pct_change())
                 
            # remove redundant columns
            df = df[['ds', 'y', 'Open', 'High', 'Low', 'Close', 'Volume', 'Trades']]
            
            gd_data[asset] = df
            output(f"{asset}\n{df}\n{df.dtypes}\n\n")
        return gd_data
    except Exception as e:
        output(f"get_data() {current_asset} Error {e}\n\n")
        return None


# get spot price
def get_price(gp_asset): 
    try:
        ticker = cbp.get_product_ticker(product_id = gp_asset + "-USD")
        return float(ticker['price'])
    except Exception as e:  
        output(f"get_price() {gp_asset} Error {e}\n")
        return None


# store prices at time of trade
def capture_prices(cp_valid):
    cp_trade_prices = dict()
    try:
        for asset in cp_valid:
            current_asset = asset
            price = get_price(asset)
            if price:
                cp_trade_prices[asset] = price
        output(f"{cp_trade_prices}\n{len(cp_trade_prices)} Prices\n\n")
        return cp_trade_prices
    except Exception as e:
        output(f"capture_prices() {current_asset} Error {e}\n\n")
        return None


###########
## BEGIN ##
###########

## VARS ##
start_time = time.time()                    # track runtime 
# output file
wire_path = "/home/dev/code/tmp/" + str(datetime.now().strftime("%Y-%m-%d %H-%M-%S")) + ".txt"
cbp = cbpro.PublicClient()                  # coinbase spot price
# binance candlestick data
bnc = bnc(os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET"), tld = "us")
# coinbase check balance and trade
cbc = ccxt.coinbase({'apiKey': os.getenv("COINBASE_API_KEY"), 'secret': os.getenv("COINBASE_API_SECRET")})
pd.set_option('display.max_columns', None)  # pandas output formatting
# pd.set_option('display.max_rows', None)

## MAIN ##
valid = sync_wallets()
if not valid:
    output(f"Stop at sync_wallets()\n\n")
    sys.exit()

data = get_data(valid)
if not data:
    output(f"Stop at get_data()\n\n")
    sys.exit()

trade_prices = capture_prices(valid)
if not trade_prices:
    output(f"Stop at capture_prices\n\n")
    sys.exit()

# calculate runtime
end_time = time.time()
seconds = end_time - start_time
output(f"Runtime\n")
output(f"{round(seconds, 2)} seconds\n")
output(f"{round((seconds / 60), 2)} minutes\n\n")

