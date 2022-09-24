
import os
import pandas as pd
import requests
import re
import datetime
import time
from urllib.parse import quote_plus
from tqdm import tqdm
DATA = os.getcwd()

class ListingDate:
    def __init__(self):
        # symbol=NYKAA&series=EQ
        self.url = "https://www1.nseindia.com//marketinfo/companyTracker/compInfo.jsp?"
        self.session = requests.session()
        self.header = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"}
        self.output_url = os.path.join(DATA, 'listing.csv')

    def get_listing_date(self, symbol, series="EQ"):
        resp = self.session.get(f"{self.url}symbol={quote_plus(symbol)}&series={quote_plus(series)}", headers = self.header, timeout=10)
        if resp.status_code != 200:
            print(resp.status_code)
            raise ValueError(str(resp.status_code))
        text = re.compile("<[^>]+>").sub('', resp.text).strip().replace("\n\n\n","\n")
        dt = re.findall("Date of Listing \(NSE\) : [\d+]{2}-[\w+]{3}-[\d+]{4}", text)[0].split(":")[-1].strip()
        lisd = datetime.datetime.strptime(dt, "%d-%b-%Y").strftime("%Y-%m-%d")
        isin = re.findall("ISIN : [\w+]+", text)[0].split(':')[-1].strip()
        comp = re.findall("^[\w*\s*\w*]*", text)[0].strip().split("\n")[0].strip()
        return comp, isin, lisd

    def read(self):
        self.df = pd.read_csv(self.output_url)
        return self

    def write(self):
        self.df.to_csv(self.output_url, index=None)
        return self
    
    def read_or_create(self):
        if not os.path.exists(self.output_url):
            self.df = pd.DataFrame(columns=['Company', 'isin', 'symbol', 'listing_date'])
            self.write()
        self.read()
        return self
    
    def append(self, row):
        self.df.loc[len(self.df)] = row
        self.write()




if __name__ == "__main__":
    d = ListingDate()
    df = pd.read_csv(os.path.join(DATA,"nifty500.csv"))
    n500 = df.Symbol.to_list()
    series = df.Series.to_list()
    nse_dict = {sym:ser for sym, ser in zip(n500, series)}
    output = d.read_or_create().df.symbol.to_list()
    symbols = set(n500) - set(output) # ignore already downloaded
    print(f"Downloading {len(symbols)} symbols.")
    for symbol in tqdm(symbols, position=0, leave=False):
        try:
            company, isin, lisd  = d.get_listing_date(symbol, nse_dict[symbol])
            d.append((company, isin, symbol, lisd))
        except Exception as e:
            print(e, symbol)
        time.sleep(1)