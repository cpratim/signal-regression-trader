from alpaca_trade_api import REST as AlpacaRest

KEY = ''
SECRET = ''

base = 'https://paper-api.alpaca.markets'

client = AlpacaRest(KEY, SECRET, base)

def cancel_all(sym):

    for order in client.list_orders():

        if order.symbol == sym:
            _id = order.id
            client.cancel_order(_id)

def _get_orders(sym):

    orders = []
    for order in client.list_orders():
        if order.symbol == sym:
            print(order)
            orders.append(order.id)
    return orders 

orders = _get_orders('NCLH')
print(orders)
