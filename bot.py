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


class AlgoBot(object):

    def __init__(self, symbols, funds=5000, margin=.005, freq=20, wait=True,
                sleeptime=10, stop_loss=.01, take_profit=.02, sandbox=True):

        print('Starting Model Training')

        self.base = 'https://paper-api.alpaca.markets' if sandbox is True else 'https://api.alpaca.markets'
        self.data_url = 'https://data.alpaca.markets'
        self.client = alpaca.REST(KEY, SECRET, self.base)
        self.polygon = PolygonRest(KEY_LIVE)
        self.sigres = SignalRegression()
        self.symbols = symbols
        self.margin = margin
        self.freq = freq
        self.take_profit = take_profit
        self.calb = .002
        self.stop_loss = stop_loss
        self.alloc = funds/len(symbols)
        self.alert = '({}) [+] {} {} shares of {} at {} per share \n'
        self.sleeptime = sleeptime
        self.active, self.models, self.barsets, self.funds, self.profits = [{} for i in range(5)]
        for sym in self.symbols:
            bars = self.client.get_barset(
                    symbols=self.symbols, 
                    timeframe='minute', 
                    limit=self.freq)
            _bars = [[b.v, b.o, b.c, b.h, b.l] for b in bars[sym]]
            self.barsets[sym] = _bars
            self.funds[sym] = funds/(len(self.symbols))
            print(f'Training Model for [{sym}]')
            model = self.sigres.generate_model(sym, self.freq, self.margin)
            self.models[sym] = model
        print('All Models Generated \n')
        if wait is True:
            self._wait()

    def _updates(self):

        conn = alpaca.StreamConn(
            KEY, SECRET, 
            base_url=self.base,
            data_url=self.data_url,
            data_stream='alpacadatav1'    
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
                    if side == 'buy':
                        print(self.alert.format(timestamp(), 
                                                'Bought', qty,
                                                sym, fill_price,))
                        self.funds[sym] -= qty * float(fill_price)
                        self.active[sym] = {'type': 'sell',
                                            'qty': qty,}
                    elif side == 'sell':
                        print(self.alert.format(timestamp(), 
                                                'Sold', qty,
                                                sym, fill_price,))
                        self.funds[sym] += qty * float(fill_price)
                        del self.active[sym]
            except Exception as error:
                self._log(error)

        @conn.on(r'^AM$')
        async def on_minute_bars(conn, channel, _bar):
            sym = _bar.symbol
            bar = self._handle(_bar)
            self.barsets[sym].pop(0)
            self.barsets[sym].append(bar)
            bars = self.barsets[sym]
            sig, hp, lp = self.models[sym].predict(bars)
            high_prediction, low_prediction = hp * (1 - self.calb), lp * (1 + self.calb)
            latest_price = self.polygon.get_last_price(sym)
            if sym in self.active:
                    act = self.active[sym]
                    _id, type_, qty, limit = act['id'], act['type'], act['qty'], act['limit']
                    if type_ == 'buy' and self._fill(_id) is None:
                            if sig == 1:
                                if low_prediction != limit:
                                    self.client.cancel_order(_id)
                                    qty = floor(self.funds[sym]/latest_price)
                                    self._buy(sym, qty, low_prediction)
                            else:
                                self.client.cancel_order(_id)
                                del self.active[sym]
            else:
                if sig == 1:
                    qty = floor(self.funds[sym]/latest_price)
                    self._buy(sym, qty, low_prediction)

        streams = ['trade_updates']
        conn.run(streams)
        self._updates()

    def _handle(self, bars):
        return (bar.volume, bar.open,
                bar.close, bar.high,
                bar.low)

    def start(self):

        
        Thread(target=self._barset_updater).start()
        for sym in self.symbols:
            Thread(target=self._ticker, args=(sym,)).start()
        self._updates()

    def _ticker(self, sym):

        while until_close(now()) > 60:
            try:
                bars = self.barsets[sym]
                sig, hp, lp = self.models[sym].predict(bars)
                high_prediction, low_prediction = hp * (1 - self.calb), lp * (1 + self.calb)
                latest_price = self.polygon.get_last_price(sym)
                if sym in self.active:
                        act = self.active[sym]
                        type_, qty, limit = act['type'], act['qty'], act['limit']
                        if type_ == 'buy' and self._fill(_id) is None:
                                if sig == 1:
                                    if low_prediction != limit:
                                        self.client.cancel_order(_id)
                                        qty = floor(self.funds[sym]/latest_price)
                                        self._buy(sym, qty, low_prediction)
                                else:
                                    self.client.cancel_order(_id)
                                    del self.active[sym]
                else:
                    if sig == 1:
                        qty = floor(self.funds[sym]/latest_price)
                        self._buy(sym, qty, low_prediction)
                
            except Exception as error:
                self._log(error)
            sleep(30)
        return 

    def _barset_updater(self):
        while until_close(now()) > 60:
            bars = self.client.get_barset(
                    symbols=self.symbols, 
                    timeframe='minute', 
                    limit=self.freq)
            for sym in self.barsets:
                _bars = [[b.v, b.o, b.c, b.h, b.l] for b in bars[sym]]
                self.barsets[sym] = _bars
            sleep(60)

        
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
            self.active[sym] = {'type': 'buy',
                                'id': order.id,
                                'qty': qty,
                                'limit': price}
        except Exception as error:
            self._log(error)
            return None
        return price

    def _sell(self, sym, qty, price):
        try:
            order = self.client.submit_order(
                                symbol=sym, side='sell', 
                                type='limit', limit_price=price, 
                                qty=qty, time_in_force='day',)
            self.active[sym]['type'] = 'sell'
            self.active[sym]['id'] = order.id
            return 
        except Exception as error:
            self._log(error)
            return price
   

    def _fill(self, _id):
        fill_price = self.client.get_order(_id).filled_avg_price
        return float(fill_price) if fill_price is not None else None

    def _canceled(self, _id):

        return self.client.get_order(_id).status == 'canceled'

    def _wait(self):
        time = until_open(20)
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


symbols = ['NCLH', 'PLAY', 'PENN', 'ERI']
ab = AlgoBot(symbols=symbols, wait=False)
ab.start()