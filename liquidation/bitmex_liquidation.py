import websocket
from websocket import create_connection
from loguru import logger
import json
import redis
import random

import time


redis_connect = redis.Redis(host='47.107.228.85', port=6379, password='20ab20!2#Spider!alxmH')
url = "wss://www.bitmex.com/realtime?subscribe=liquidation:XBTUSD"

while True:
    try:
        ws = create_connection(url, http_proxy_host="127.0.0.1", http_proxy_port=random.randint(8080, 8323))
        # ws.send('{"op": "subscribe", "args": ["liquidation:XBTUSD"]}')
        # logger.info(ws.recv())

        while True:
            result = json.loads(ws.recv())
            if result.get("action") == 'insert':
                t = int(time.time())
                data_list = result.get("data")
                for data in data_list:
                    item = {}
                    item['Time'] = t

                    item['Title'] = 'SWAP'

                    item['Pair1'] = 'BTC' if 'XBT' in data['symbol'] else data['symbol'][:3]
                    item['Pair2'] = 'USD'
                    item['Price'] = float(data['price'])
                    item['Liquidation'] = 'Long' if data['side'] == 'Sell' else 'Short'
                    item['Volume'] = data['leavesQty']
                    item['USD'] = data['leavesQty']

                    redis_key = "bitmex:futures:liquidation:{}_{}_forced_liquidation"
                    redis_connect.lpush(redis_key.format(item['Pair1'], item['Title']), json.dumps(item))
                    logger.info(item)

    except websocket._exceptions.WebSocketConnectionClosedException as e:
        pass
    except Exception as e:
        logger.error(e)
