import alpaca_trade_api as alpaca
from time import sleep
from datetime import datetime, timedelta
import numpy as np
from polygon import AlpacaSocket, PolygonRest
from math import floor
import sys
import re
import json
from threading import Thread
from config import KEY_LIVE
from sigres import SignalRegression

KEY = ''
SECRET = ''

def date():
    return str(datetime.now())[:10]

def timestamp():
    return str(datetime.now())[11:19]

def read_data(f):
    with open(f, 'r') as df:
        return json.loads(df.read())

def now():
    return datetime.now()

def until_open(extra):
    now = datetime.now()
    y, m, d = [int(s) for s in str(now)[:10].split('-')]
    market_open = datetime(y, m, d, 9, 30) + timedelta(minutes=extra)
    return ((market_open - now).seconds)

def market_close(unix):
    t = datetime.fromtimestamp(unix)
    y, m, d = [int(s) for s in date().split('-')]
    return((datetime(y, m, d, 16, 0, 0) - t).seconds)

def market_open():
    now = datetime.now()
    y, m, d = [int(s) for s in str(now)[:10].split('-')]
    mo = datetime(y, m, d, 9, 30)
    return mo

def until_close(now):
    y, m, d = [int(s) for s in str(now)[:10].split('-')]
    return((datetime(y, m, d, 16, 0, 0) - now).seconds)

def minutes_from_open(now):
    y, m, d = [int(s) for s in str(now)[:10].split('-')]
    mo = datetime(y, m, d, 9, 30)
    return floor((now - mo).seconds/60)

class AlgoBot(object):

    def __init__(self, symbols, funds=10000, margin=.005, freq=20, wait=True,
                sleeptime=10, stop_loss=.015, take_profit=.02, sandbox=True):

        print('Starting Model Training')

        self.base = 'https://paper-api.alpaca.markets' if sandbox is True else 'https://api.alpaca.markets'
        self.data_url = 'https://data.alpaca.markets'
        self.client = alpaca.REST(KEY, SECRET, self.base)
        self.polygon = PolygonRest(KEY)
        self.sigres = SignalRegression()
        self.symbols = symbols
        self.margin = margin
        self.freq = freq
        self.take_profit = take_profit
        self.calb = .002
        self.stop_loss = stop_loss
        self.finished = []
        self.alloc = funds/len(symbols)
        self.alert = '({}) [+] {} of {} shares of {} at {} per share \n'
        self.sleeptime = sleeptime
        self.active, self.models, self.barsets, self.funds, self.profits = [{} for i in range(5)]
        for sym in self.symbols:
            self.funds[sym] = funds/(len(self.symbols))
            print(f'Training Model for [{sym}]')
            model = self.sigres.generate_model(sym, self.freq, self.margin)
            self.models[sym] = model
        print('All Models Generated \n')
        if wait is True:
            self._wait()
        bars = self.client.get_barset(
                symbols=self.symbols, 
                timeframe='minute', 
                limit=self.freq)
        for sym in self.symbols:
            _bars = [[b.v, b.o, b.c, b.h, b.l] for b in bars[sym]]
            self.barsets[sym] = _bars

    def start(self):
 
        conn = alpaca.StreamConn(
            KEY, SECRET, 
            base_url=self.base,
            data_url=self.data_url,
            data_stream='polygon'    
        )

        @conn.on(r'^trade_updates$')
        async def handle_trade(conn, channel, data):
            try:
                order = data.order
                sym, qty, side, fill_price = (order['symbol'], 
                                            int(order['filled_qty']), 
                                            order['side'],
                                            order['filled_avg_price'])
                type_ = data.event
                if type_ == 'fill':
                    act = self.active[sym]
                    _id, type_, limit = act['id'], act['type'], act['limit']
                    if side == 'buy':
                        if type_ == 'long':
                            print(self.alert.format(timestamp(), 
                                        'Longed', qty,
                                        sym, fill_price,))
                            self.funds[sym] -= qty * float(fill_price)
                        elif type_ == 'short':
                            print(self.alert.format(timestamp(), 
                                        'Executed Short', qty,
                                        sym, fill_price,))
                            self.funds[sym] -= qty * float(fill_price)
                            del self.active[sym]
                    elif side == 'sell':
                        if type_ == 'long':
                            print(self.alert.format(timestamp(), 
                                        'Executed Long', qty,
                                        sym, fill_price,))
                            self.funds[sym] += qty * float(fill_price)
                            del self.active[sym]
                        elif type_ == 'short':
                            print(self.alert.format(timestamp(), 
                                        'Shorted', qty,
                                        sym, fill_price,))
                            self.funds[sym] += qty * float(fill_price)

            except Exception as error:
                self._log(error)

        @conn.on(r'^AM$')  
        async def on_minute_bars(conn, channel, _bar):
            sym = _bar.symbol
            bar = self._handle(_bar)
            self.barsets[sym].append(bar)
            if len(self.barsets[sym]) >= self.freq:
                self.barsets[sym].pop(0)
                bars = self.barsets[sym]
                minute = minutes_from_open(now())
                sig, hp, lp = self.models[sym].predict(bars, minute)
                high_prediction, low_prediction = hp * (1 - self.calb), lp * (1 + self.calb)
                latest_price = self.polygon.get_last_price(sym)
                if sym in self.active:
                    act = self.active[sym]
                    _id, type_, limit = act['id'], act['type'], act['limit']
                    if self._fill(_id) is None:
                        if type_ == 'long':
                            qty = floor(self.funds[sym]/latest_price)
                            if sig == 1:
                                if low_prediction != limit:
                                    self.client.cancel_order(_id)
                                    self._buy(sym, qty, low_prediction)
                            elif sig == -1:
                                self.client.cancel_order(_id)
                                sleep(1)
                                self._sell(sym, qty, high_prediction)

                        elif type_ == 'short':
                            qty = floor(self.funds[sym]/latest_price)
                            if sig == -1:
                                if high_prediction != limit:
                                    self.client.cancel_order(_id)
                                    self._sell(sym, qty, high_prediction)
                            elif sig == 1:
                                self.client.cancel_order(_id)
                                sleep(1)
                                self._buy(sym, qty, low_prediction)
                        if sig == 0:
                            self.client.cancel_order(_id)
                            del self.active[sym]
                else:
                    if (self.funds[sym] - self.alloc)/self.alloc >= self.take_profit:
                        self.finished.append(sym)
                    if sym not in self.finished:
                        qty = floor(self.funds[sym]/latest_price)
                        if sig == 1:
                            self._buy(sym, qty, low_prediction)
                        else:
                            self._sell(sym, qty, high_prediction)

        streams = ['trade_updates'] + [f'AM.{sym}' for sym in self.symbols]
        try:
            conn.run(streams)
        except Exception as error:
            self._log(error)
            self.start()
      

    def _handle(self, bar):
        return (bar.volume, bar.open,
                bar.close, bar.high,
                bar.low)
        
    def _log(self, error):

        print(f'Error at [{timestamp()}]:')
        print(error)
        return

    def _buy(self, sym, qty, price):
        try:
            stop_loss = price * (1 - self.stop_loss)
            goal = price * (1 + self.margin)
            order = self.client.submit_order(
                                symbol=sym, side='buy', 
                                type='limit', limit_price=price, 
                                qty=qty, time_in_force='day',
                                order_class='bracket',
                                stop_loss={'stop_price': stop_loss},
                                take_profit={'limit_price': goal}
                                )
            self.active[sym] = {'type': 'long',
                                'id': order.id,
                                'limit': price}
        except Exception as error:
            self._log(error)
            return None
        return price

    def _sell(self, sym, qty, price):
        try:
            stop_loss = price * (1 + self.stop_loss)
            goal = price * (1 - self.margin)
            order = self.client.submit_order(
                                symbol=sym, side='sell', 
                                type='limit', limit_price=price, 
                                qty=qty, time_in_force='day',
                                order_class='bracket',
                                stop_loss={'stop_price': stop_loss},
                                take_profit={'limit_price': goal}
                                )
            self.active[sym] = {'type': 'short',
                                'id': order.id,
                                'qty': qty,
                                'limit': price}
        except Exception as error:
            self._log(error)
            return None
        return price
   

    def _fill(self, _id):
        fill_price = self.client.get_order(_id).filled_avg_price
        return float(fill_price) if fill_price is not None else None

    def _canceled(self, _id):
        return self.client.get_order(_id).status == 'canceled'

    def _wait(self):
        time = until_open(self.freq)
        print(f'Sleeping {time} seconds until Market Open')
        sleep(time)
        print(f'Starting Bot at {now()} \n')

    def _liquidate(self):
        for order in self.client.list_orders():
            self.client.cancel_order(order.id)
        for position in self.client.list_positions():
            qty = int(position.qty)
            sym = position.symbol
            Thread(target=self._sell, args=(sym, qty)).start()
            pass

    def _cancel_all(self, sym):

        for order in self.client.list_orders():
            if order.symbol == sym:
                _id = order.id
                self.client.cancel_order(_id)
        return

    
    def _get_orders(self, sym):

        orders = []
        for order in self.client.list_orders():
            if order.symbol == sym:
                orders.append(order.id)
        return orders 


symbols = ['NCLH', 'MRO', 'SAVE', 'ERI']
ab = AlgoBot(symbols=symbols, wait=True)
ab.start()