import websocket
from config import KEY_LIVE
from threading import Thread
import json
from time import sleep
import requests
from datetime import datetime, timedelta
import pickle

def unix(date):
    y, m, d = [int(d) for d in date.split('-')]
    o = int(datetime(y, m, d, 9, 30).strftime("%s")) * 1000000000
    c = int(datetime(y, m, d, 16).strftime("%s")) * 1000000000
    return o, c

def from_unix(unix):
    t = datetime.fromtimestamp(unix)
    return str(t)

def read_data(f):
    with open(f, 'r') as df:
        return json.loads(df.read())

def dump_data(f, d):
    with open(f, 'w') as df:
        json.dump(d, df, indent=4)

def read_data_bin(f):
    with open(f, 'rb') as df:
        return pickle.load(df)

def dump_data_bin(f, d):
    with open(f, 'wb') as df:
        pickle.dump(d, df)

def get_days(s, e):
    days = []
    sy, sm, sd = [int(d) for d in s.split('-')]
    ey, em, ed = [int(d) for d in e.split('-')]
    start = datetime(sy, sm, sd)
    end = datetime(ey, em, ed)
    delta = end - start
    for i in range(delta.days + 1):
        days.append(str(start + timedelta(i))[:10])
    return days


class AlpacaSocket(object):

	def __init__(self, key, secret, tickers, on_message=None):

		self.base = 'wss://data.alpaca.markets/stream'
		self.key = key
		self.secret = secret
		self.on_message = on_message
		self.tickers = tickers

	def _on_error(self, ws, error):
		print(error)

	def start(self):
		self.ws = websocket.WebSocketApp(self.base,
                            on_message=self.on_message,
                            on_error=self._on_error,
                            on_close=self._on_close)
		self.ws.on_open = self._authenticate
		self.ws.run_forever()
		print('Connection Lost, Restarting')
		self.start()

	def close(self):
		self.ws.send({'action': 'unlisten', 'data': {'streams': [f'T.{t}' for t in self.tickers]}})
		self.ws.close()

	def _on_close(self, ws):
		print('Closed Connection')

	def _authenticate(self):
		auth = json.dumps({'action':'authenticate', 'data': {'key_id': self.key, 'secret_key': self.secret}})
		self.ws.send(auth)
		self.subscribe_tickers(self.tickers)

	def subscribe_tickers(self, tickers):
		req = json.dumps({'action':'listen','data': {'streams': [f'T.{t}' for t in self.tickers]}})
		self.ws.send(req)


class PolygonRest(object):

    def __init__(self, key):
        self.base = 'https://api.polygon.io'
        self.key = key
        self.date = lambda: str(datetime.now())[:10]

    def get_stocks(self):
        req_url = f'{self.base}/v2/reference/tickers?apiKey={self.key}?sort=ticker&market=STOCKS&perpage=50&page=100&active=true'
        raw = requests.get(req_url).text
        return json.loads(raw)

    def get_historical_tickers(self, symbol, date, limit=50000):
        o, c = unix(date)
        req_url = f'{self.base}/v2/ticks/stocks/trades/{symbol}/{date}?apiKey={self.key}&timestamp={o}&timestampLimit={c}&limit=50000'
        raw = requests.get(req_url).text
        return json.loads(raw)

    def get_financials(self, symbol):
        req_url = f'{self.base}/v2/reference/financials/{symbol}?apiKey={self.key}'
        raw = requests.get(req_url).text
        return json.loads(raw)

    def get_after_hours(self, symbol, date):
        o, c = unix(date)
        req_url = f'{self.base}/v2/ticks/stocks/trades/{symbol}/{date}?apiKey={self.key}&timestamp={c}&limit=50000'
        raw = requests.get(req_url).text
        return json.loads(raw)

    def get_candles(self, symbol, start, end=None):
        if end is None: end = self.date()
        req_url = f'{self.base}/v2/aggs/ticker/{symbol}/range/2/day/{start}/{end}?apiKey={self.key}'
        raw = requests.get(req_url).text
        return json.loads(raw)

    def get_bars(self, start, end=None, timespan='1Min'):
        symbols = self.get_all_symbols()
        if end is None: end = self.date()
        days = get_days(start, end)
        for day in days:
            print(day)
            result = {}
            ret = {}
            for sym in symbols:
                req_url = f'{self.base}/v2/aggs/ticker/{sym}/range/1/minute/{day}/{day}?apiKey={self.key}'
                raw = requests.get(req_url).text
                data = json.loads(raw)
                if 'resultsCount' in data:
                    if data['resultsCount'] != 0:
                        result[sym] = []
                        for r in data['results']:
                            t = int(r['t']/1000)
                            date, time = from_unix(t)[:10], from_unix(t)[11:]
                            hrs, min_ = [float(s) for s in time.split(':')[:-1]]
                            rel = hrs + min_/100 
                            if rel >= 9.3 and rel < 16:
                                v, o, c, h, l = r['v'], r['o'], r['c'], r['h'], r['l']
                                result[sym].append([v, o, c, h, l])
            for sym in result:
                if len(result[sym]) > 380:
                    ret[sym] = result[sym]
            if len(ret) > 0:
                dump_data_bin(f'data/minute/{day}', ret)


    def get_all_candles(self, start, end=None, dump=False):
        result = {}
        if end is None: end = self.date()
        for date in get_days(start, end):
            req_url = f'{self.base}/v2/aggs/grouped/locale/US/market/STOCKS/{date}?apiKey={self.key}'
            raw = requests.get(req_url).text
            data = json.loads(raw)
            if int(data['resultsCount']) != 0:
                dump = {}
                for r in data['results']:
                    s, v, o, c, h, l = r['T'], r['v'], r['o'], r['c'], r['h'], r['l']
                    dump[s] = [v, o, c, h, l]
                    if s not in result: result[s] = [[v, o, c, h, l]]
                    else: result[s].append([v, o, c, h, l])
                if dump is True: dump_data_bin(f'data/day/{date}', dump)
        return result

    def get_all_symbols(self):
        symbols = []
        date = self.date()
        req_url = f'{self.base}/v2/aggs/grouped/locale/US/market/STOCKS/2020-05-14?apiKey={self.key}'
        raw = requests.get(req_url).text
        data = json.loads(raw)
        if int(data['resultsCount']) != 0:
            for r in data['results']:
                symbols.append(r['T'])
        return symbols

    def get_stats(self, sym, s, e=None):
        if e is None: e = self.date()
        req_url = f'{self.base}/v2/aggs/ticker/{sym}/range/1/day/{s}/{e}?apiKey={self.key}'
        raw = requests.get(req_url).text
        data = json.loads(raw)
        vs, os, hps, lps = [], [], [], []
        try:
            for c in data['results']:
                v, o, h, l = c['v'], c['o'], c['h'], c['l']
                hp, lp = ((h - o)/o * 100), ((o - l)/o * 100)
                vs.append(v) 
                os.append(o)
                hps.append(hp)
                lps.append(lp)
        except:
            pass
        return sum(vs)/len(vs), sum(os)/len(os), sum(hps)/len(hps), sum(lps)/len(lps)


    def get_last_price(self, symbol):
        req_url = f'{self.base}/v1/last/stocks/{symbol}?apiKey={self.key}'
        raw = requests.get(req_url).text
        data = json.loads(raw)
        return float(data['last']['price'])

'''
poly = PolygonRest(KEY_LIVE)
print(poly.get_last_price('AAPL'))
poly.get_bars(start='2020-07-06')
'''