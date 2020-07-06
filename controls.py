import json
import pickle

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