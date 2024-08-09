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
    output(f"{bnc_assets}\n{len(bnc_assets)} Binance\n\n")

    # coinbase
    try:
        cbc_nfo = cbc.fetch_currencies()
        cbc_assets = set(cbc_nfo.keys())
    except Exception as e:
        output(f"{e}\n\n")
    output(f"{cbc_assets}\n{len(cbc_assets)} Coinbase\n\n")

    # shared
    shared = bnc_assets & cbc_assets
    output(f"{shared}\n{len(shared)} Shared\n\n")
    
    # invalid
    invalid = {"REP", "CELO", "USDC", "DAI", "USDT", "AMP", "WBTC", "JASMY", "HNT", "SPELL"}
    output(f"{invalid}\n{len(invalid)} Invalid\n\n")

    # valid
    valid = shared - invalid
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
bnc = bnc(os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET"), tld="us")
cbc = ccxt.coinbase({'apiKey': os.getenv("COINBASE_API_KEY"), 'secret': os.getenv("COINBASE_API_SECRET")})
pd.set_option('display.max_columns', None)

# main
valid = syncWallets()

# candlestick dataframes
for asset in valid:
    try:    
        # fetch kline data
        klines = bnc.get_klines(symbol = asset + "USDT", interval = bnc.KLINE_INTERVAL_30MINUTE, limit = 2000)
        df = pd.DataFrame(klines, columns=['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore'])

        # convert timestamps to a readable date format
        df['Open Time'] = pd.to_datetime(df['Open Time'], unit='ms')
        df['Close Time'] = pd.to_datetime(df['Close Time'], unit='ms')
        output(f"{asset}\n{df}\n\n")
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

