from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, ExtraTreesClassifier, ExtraTreesRegressor
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from controls import *
from minute import common, backtest_model, raw_dump
from sklearn.model_selection import train_test_split 
import numpy as np
from sklearn.model_selection import GridSearchCV
from datetime import datetime, timedelta
from math import floor

class SignalRegression(object):

    def __init__(self):

        self.dump = raw_dump(1, 71)
        self.stat_fun = [np.amin, np.amax, np.median, np.var, np.std, np.sum, np.ptp, np.average]

    def _signalize(self, dump, sym, freq, thresh):

        inp, signals, highs, lows = [], [], [], []
        for min_ in dump:
            minute = 0
            for i in range(len(min_)-2*freq+1):
                _in = [[c[n] for c in min_[i:freq+i]] for n in range(5)]
                _inp = [minute]
                for arr in _in:
                    _inp += [fun(arr) for fun in self.stat_fun]
                inp.append(_inp)
                close = min_[i:freq+i][-1][2]
                future = (min_[freq+i:2*freq+i])
                _lows, _highs = [c[4] for c in future], [c[3] for c in future]
                high, low = max(_highs), min(_lows)
                high_p = (high - close)/close
                low_p = (close - low)/close
                highs.append(high)
                lows.append(low)
                if high_p > thresh: signals.append(1)
                elif low_p > thresh: signals.append(-1)
                else: signals.append(0)  
                minute += 1
        X, Y_high, Y_low, Y_signals = [np.array(l) for l in [inp, highs, lows, signals]]
        return X, Y_high, Y_low, Y_signals

    def generate_model(self, sym, freq, thresh):

        symbol_dump = [d[sym] for d in self.dump if sym in d]
        
        X, Y_high, Y_low, Y_signals = self._signalize(symbol_dump, sym, freq, thresh)

        low_regressor = RandomForestRegressor()
        high_regressor = RandomForestRegressor()
        classifier = RandomForestClassifier()
        low_x_train, low_x_test, low_y_train, low_y_test = train_test_split(X, Y_low, 
                                                                        test_size=.2, 
                                                                        random_state=1)
        high_x_train, high_x_test, high_y_train, high_y_test = train_test_split(X, Y_high, 
                                                                        test_size=.2, 
                                                                        random_state=1)
        sig_x_train, sig_x_test, sig_y_train, sig_y_test = train_test_split(X, Y_signals, 
                                                                        test_size=.2, 
                                                                        random_state=1)

        low_regressor.fit(low_x_train, low_y_train)
        high_regressor.fit(high_x_train, high_y_train)
        classifier.fit(sig_x_train, sig_y_train)

        low_reg_r = low_regressor.score(low_x_test, low_y_test)
        high_reg_r = high_regressor.score(high_x_test, high_y_test)
        clf_acc = classifier.score(sig_x_test, sig_y_test)

        print('Low R Squared: ', low_reg_r)
        print('High R Squared: ', high_reg_r)
        print('Classifier Accuracy: ', clf_acc)

        return (SignalRegressionModel(classifier,
                                    high_regressor, 
                                    low_regressor)) 

    def backtest(self, sym, freq, thresh, stop_loss, model):

        test_data = raw_dump(0, 1)[0][sym]
        profit = backtest_model(test_data, freq, thresh, stop_loss, model)
        print('Profit:', profit)
        return profit

class SignalRegressionModel(object):

    def __init__(self, classifier, high_regressor, low_regressor):

        self.high_regressor = high_regressor
        self.low_regressor = low_regressor
        self.classifier = classifier
        self.stat_fun = [np.amin, np.amax, np.median, np.var, np.std, np.sum, np.ptp, np.average]

    def predict(self, inp, minute):

        _in = [[c[n] for c in inp] for n in range(5)]
        _inp = [[minute]]
        for arr in _in:
            _inp[0] += [fun(arr) for fun in self.stat_fun]
        signal, high_pred, low_pred = (self.classifier.predict(_inp)[0],
                                    self.high_regressor.predict(_inp)[0],
                                    self.low_regressor.predict(_inp)[0],)
        return signal, high_pred, low_pred

'''
sym = 'MRO'
freq = 20
stop_loss = .015
thresh = .005
sigres = SignalRegression()
model = sigres.generate_model(sym, freq, thresh)
sigres.backtest(sym, freq, thresh, stop_loss, model)
#['NCLH', 'MRO', 'SAVE', 'ERI']
'''