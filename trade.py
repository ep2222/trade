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
import talib


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


# get top ten highest average true range volatility
def get_top_atr(gta_assets, granularity):
    gd_atr = dict()     # asset: average relative atr
    if granularity == 1:
        interval_input = bnc.KLINE_INTERVAL_1MINUTE
        interval_output = "One"
    elif granularity == 3:
        interval_input = bnc.KLINE_INTERVAL_3MINUTE
        interval_output = "Three"
    elif granularity == 5:
        interval_input = bnc.KLINE_INTERVAL_5MINUTE
        interval_output = "Five"

    try:
        for asset in gta_assets:
            current_asset = asset
            # fetch kline data
            klines = bnc.get_klines(symbol = asset + "USDT", interval = interval_input, limit = 1000)
            df = pd.DataFrame(klines, columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore'])
            df['Close Time'] = pd.to_datetime(df['Close Time'], unit = 'ms')
            
            # convert multiple ohlcv to float64
            df = df.astype({'Open': 'float64', 'High': 'float64', 'Low': 'float64', 'Close': 'float64', 'Volume': 'float64'})
            # relative atr normalizes prices
            df['ATR'] = talib.ATR(df['High'], df['Low'], df['Close'])
            df['relative_ATR'] = df['ATR'] / df['Close']
            df = df.dropna()
            
            # calculate average relative atr for the asset
            avg_atr = df['relative_ATR'].mean()
            gd_atr[asset] = avg_atr
        
        # return the top ten assets with the highest average relative atr
        top_10 = dict(list(sorted(gd_atr.items(), key = lambda item: item[1], reverse = True)[:10]))
        output(f"{top_10}\n10 {interval_output} Minute ATR\n\n")
        return set(top_10.keys())
    except Exception as e:
        output(f"get_top_atr() {current_asset} Error {e}\n\n")
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
cbp = cbpro.PublicClient()                  # coinbase spot prices
# binance candlestick data
bnc = bnc(os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET"), tld = "us")
# coinbase assets
cbc = ccxt.coinbase({'apiKey': os.getenv("COINBASE_API_KEY"), 'secret': os.getenv("COINBASE_API_SECRET")})
pd.set_option('display.max_columns', None)  # pandas output formatting
# pd.set_option('display.max_rows', None)

## MAIN ##
assets = sync_wallets()
if not assets:
    output(f"Stop at sync_wallets()\n\n")
    sys.exit()

# select assets for modeling
# top 30 most volatile average true range assets by three time difference granularities
one_min = get_top_atr(assets, 1)                        # 1 minute
th3_min = get_top_atr(assets - one_min, 3)              # 3 minutes
fiv_min = get_top_atr(assets - one_min - th3_min, 5)    # 5 minutes
modeling = one_min | th3_min | fiv_min
output(f"{modeling}\n30 Modeling Assets\n\n")

if not one_min or not th3_min or not fiv_min:
    output(f"Stop at get_top_atr()\n\n")
    sys.exit()

trade_prices = capture_prices(assets)
if not trade_prices:
    output(f"Stop at capture_prices\n\n")
    sys.exit()

# calculate runtime
end_time = time.time()
seconds = end_time - start_time
output(f"Runtime\n")
output(f"{round(seconds, 2)} seconds\n")
output(f"{round((seconds / 60), 2)} minutes\n\n")

