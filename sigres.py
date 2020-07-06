from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, ExtraTreesClassifier, ExtraTreesRegressor
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from controls import *
from minute import common, backtest_model, raw_dump, backtest_model_2
from sklearn.model_selection import train_test_split 
import numpy as np
from sklearn.model_selection import GridSearchCV


class SignalRegression(object):

    def __init__(self):

        self.dump = raw_dump(1, 71)
        self.classifier_params = {
            'n_estimators': (1, 200),
            'criterion': ('gini', 'entropy'),
            'max_depth': (1, 100),
            'max_features': ('auto', 'sqrt', 'log2'),
        }

    def _signalize(self, dump, sym, freq, thresh):

        inp, signals, highs, lows = [], [], [], []
        for min_ in dump:
            for i in range(len(min_)-2*freq+1):
                _in = np.array([c[0:] for c in min_[i:freq+i]])
                inp.append(_in.flatten())
                close = _in[-1][2]
                future = (min_[freq+i:2*freq+i])
                _lows, _highs = [c[4] for c in future], [c[3] for c in future]
                high, low = max(_highs), min(_lows)
                high_p = (high - close)/close
                low_p = (close - low)/close
                highs.append(high)
                lows.append(low)
                signals.append(1 if high_p > thresh else 0)   
        X, Y_high, Y_low, Y_signals = [np.array(l) for l in [inp, highs, lows, signals]]
        return X, Y_high, Y_low, Y_signals

    def generate_model(self, sym, freq, thresh):

        symbol_dump = [d[sym] for d in self.dump if sym in d]
        
        X, Y_high, Y_low, Y_signals = self._signalize(symbol_dump, sym, freq, thresh)

        low_regressor = ExtraTreesRegressor()
        high_regressor = ExtraTreesRegressor()
        classifier = ExtraTreesClassifier()
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

        return (GenerativeStatisticsModel(classifier,
                                        high_regressor, 
                                        low_regressor)) 

    def backtest(self, sym, freq, stop_loss, model):

        test_data = raw_dump(0, 1)[0][sym]
        profit = backtest_model(test_data, freq, stop_loss, model)
        print('Profit:', profit)
        return profit

class SignalRegressionModel(object):

    def __init__(self, classifier, high_regressor, low_regressor):

        self.high_regressor = high_regressor
        self.low_regressor = low_regressor
        self.classifier = classifier

    def predict(self, inp):

        _inp = [np.array(inp).flatten()]
        signal, high_pred, low_pred = (self.classifier.predict(_inp)[0],
                                    self.high_regressor.predict(_inp)[0],
                                    self.low_regressor.predict(_inp)[0],)
        return signal, high_pred, low_pred


sym = 'MRO'
freq = 20
stop_loss = .01
thresh = .005
gen_stat = GenerativeStatistics()
model = gen_stat.generate_model(sym, freq, thresh)
gen_stat.backtest(sym, freq, stop_loss, model)
#NCLH, MRO, PENN, PLAY
