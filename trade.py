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


# write output to file
def output(message):
    with open(wire_path, "a") as wire:
        wire.write(message)


# find common assets between binance and coinbase api keys
def syncWallets():
    # binance
    try:
        bnc_api = bnc(os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET"), tld="us")
        bnc_nfo = bnc_api.get_exchange_info()
        bnc_assets = set(symbol['baseAsset'] for symbol in bnc_nfo['symbols'])
    except Exception as e:
        output(f"{e}\n\n")   
    output(f"{bnc_assets}\n{len(bnc_assets)} Binance\n\n")

    # coinbase
    try:
        cbc = ccxt.coinbase({'apiKey': os.getenv("COINBASE_API_KEY"), 'secret': os.getenv("COINBASE_API_SECRET")})
        cbc_nfo = cbc.fetch_currencies()
        cbc_assets = set(cbc_nfo.keys())
    except Exception as e:
        output(f"{e}\n\n")
    output(f"{cbc_assets}\n{len(cbc_assets)} Coinbase\n\n")

    # shared
    shared = bnc_assets.intersection(cbc_assets)
    output(f"{shared}\n{len(shared)} Shared\n\n")
    
    # invalid
    invalid = {"REP", "CELO", "USDC", "DAI", "USDT"}
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


# track runtime
start_time = time.time()
wire_path = "/home/dev/code/tmp/" + str(datetime.now().strftime("%Y-%m-%d %H-%M-%S")) + ".txt"
cbp = cbpro.PublicClient()

# void main
shared = syncWallets()

# capture prices at time of trade
prices = dict()
for asset in shared:
    price = getPrice(asset)
    if price:
        prices[asset] = price
output(f"{prices}\n{len(prices)} Prices\n\n")

# calculate runtime
end_time = time.time()
seconds = end_time - start_time
output(f"Runtime\n")
output(f"{round(seconds, 2)} seconds\n")
output(f"{round((seconds / 60), 2)} minutes\n\n")

