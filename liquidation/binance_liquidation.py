import websocket
from websocket import create_connection
from loguru import logger
import json
import redis
import random

import time


redis_connect = redis.Redis(host='47.107.228.85', port=6379, password='20ab20!2#Spider!alxmH')
url = url = 'wss://fstream.binance.com/ws/btcusdt@forceOrder'

while True:
    try:
        ws = create_connection(url, http_proxy_host="127.0.0.1", http_proxy_port=random.randint(8080, 8323))
        # ws.send('{"op": "subscribe", "args": ["liquidation:XBTUSD"]}')
        # logger.info(ws.recv())


        while True:
            result = json.loads(ws.recv())
            if result.get("o"):
                data = result.get("o")
                item = {}
                item['Time'] = int(data['T'] / 1000)
                item['Title'] = 'SWAP'
                item['Pair1'] = data['s'].replace("USDT", "")
                item['Pair2'] = 'USDT'
                item['Price'] = float(data['p'])
                item['Liquidation'] = 'Long' if data['S'] == 'SELL' else 'Short'
                item['Volume'] = float(data['q'])
                item['USD'] = float("%.2f" % (item['Volume'] * item['Price']))

                redis_key = "binance:futures:liquidation:{}_{}_forced_liquidation"
                redis_connect.lpush(redis_key.format(item['Pair1'], item['Title']), json.dumps(item))
                logger.info(item)

    except websocket._exceptions.WebSocketConnectionClosedException as e:
        pass
    except Exception as e:
        logger.error(e)
