import json
from tensorflow import keras
import tensorflow as tf
import numpy as np
import os
from random import shuffle
from scipy.optimize import minimize
from polygon import PolygonRest
from config import KEY_LIVE
import pickle
from math import floor
from sklearn import preprocessing


LOCATION = 'data/minute'

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

def raw_dump(s=None, e=None):
    if s is None and e is None:
        files = sorted(os.listdir(LOCATION))
        dump = [read_data_bin(f'{LOCATION}/{file}') for file in files]
        return dump
    if e is None:
        files = sorted(os.listdir(LOCATION))[:-s]
        dump = [read_data_bin(f'{LOCATION}/{file}') for file in files]
        return dump
    files = sorted(os.listdir(LOCATION))[-e:-s]
    if s == 0: files = sorted(os.listdir(LOCATION))[-e:]
    dump = [read_data_bin(f'{LOCATION}/{file}') for file in files]
    return dump

def common(dump):
    symbols = []
    for data in dump:
        symbols.append([s for s in data])
    r = symbols[0]
    for sym in symbols:
        r = list(set(r) & set(sym))
    return r

def unique(dump):
    symbols = []
    for data in dump:
        for s in data:
            if s not in symbols:
                symbols.append(s)
    return symbols

#vochl

def backtest_model(min_, freq, thresh, stop_loss, model, short=True):
    shares, waiting, profit, pos, type_ = [0 for i in range(5)]
    funds = 1000
    last_freq = min_[:freq]
    last_buy = 0
    minute = freq
    for m in min_[freq:]:
        v, price, c, high, low = m
        sig, hg, lg = model.predict(last_freq, minute)
        high_goal, low_goal = hg * .998, lg * 1.002
        if type_ == 0:
            if profit/funds * 100 > 2:
               # break
               pass
        if type_ == 1:
            goal = (last_buy * (1 + thresh))
            sl = last_buy * (1 - stop_loss)
            if high >= goal:
                p = shares * (goal - last_buy)
                print(f'[{minute}] Executed {shares} shares at {goal} per share')
                profit += p
                funds += p
                type_ = 0
            elif low <= sl:
                p = shares * (sl - last_buy)
                print(f'[{minute}] Executed {shares} shares at {sl} per share')
                profit += p
                funds += p
                type_ = 0
        elif type_ == -1:
            goal = (last_buy * (1 - thresh))
            sl = last_buy * (1 + stop_loss)
            if high >= goal:
                p = shares * (last_buy - goal)
                print(f'[{minute}] Executed {shares} shares at {goal} per share')
                profit += p
                funds += p
                type_ = 0
            elif low <= sl:
                p = shares * (last_buy - sl)
                print(f'[{minute}] Executed {shares} shares at {sl} per share')
                profit += p
                funds += p
                type_ = 0
        #print(sig, low_goal, high_goal, price, low, high)
        if low <= low_goal and type_ == 0 and sig == 1:
            type_ = 1
            shares = floor(funds/price)
            last_buy = low_goal
            print(f'[{minute}] Bought {shares} shares at {last_buy} per share')
        elif high >= high_goal and type_ == 0 and sig == -1 and short is True:
            type_ = -1
            shares = floor(funds/price)
            last_buy = high_goal
            print(f'[{minute}] Shorted {shares} shares at {last_buy} per share')
        minute += 1
        last_freq.pop(0)
        last_freq.append(m)
    last_close = min_[-1][2]
    if type_ == 1: profit += shares * (last_close - last_buy)
    return profit/funds * 100


def prevelance(dump, sym):
    return len([1 for d in dump if sym in d])/len(dump)

'''
d = raw_dump()
sym = unique(d)
symbols = []
for s in sym:
    if prevelance(d, s) > .9:
        symbols.append(s)

print(sym)

'''