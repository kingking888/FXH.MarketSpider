#!/usr/bin/env python
# -*- coding:utf-8 -*-

import requests
import time
import random
import json
import redis
from loguru import logger


# 现货:
btc_spot_url = "https://www.okex.com/api/spot/v3/instruments/BTC-USDT/trades?limit=1"
# 期货:
btc_cw_url = "https://www.okex.com/api/futures/v3/instruments/BTC-USD-200320/trades?limit=1"
btc_nw_url = "https://www.okex.com/api/futures/v3/instruments/BTC-USD-200327/trades?limit=1"
btc_cq_url = "https://www.okex.com/api/futures/v3/instruments/BTC-USD-200626/trades?limit=1"
btc_nq_url = "https://www.okex.com/api/futures/v3/instruments/BTC-USD-200925/trades?limit=1"
btc_swap_url = "https://www.okex.com/api/swap/v3/instruments/BTC-USD-SWAP/trades?limit=1"


def send_request():
    while True:
        try:
            # proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8232))}
            proxies = {"https": "http://127.0.0.1:{}".format(1080)}
            btc_spot_price = float(requests.get(btc_spot_url, proxies=proxies).json()[0]['price'])
            btc_cw_price = float(requests.get(btc_cw_url, proxies=proxies).json()[0]['price'])
            btc_nw_price = float(requests.get(btc_nw_url, proxies=proxies).json()[0]['price'])
            btc_cq_price = float(requests.get(btc_cq_url, proxies=proxies).json()[0]['price'])
            btc_nq_price = float(requests.get(btc_nq_url, proxies=proxies).json()[0]['price'])
            btc_swap_price = float(requests.get(btc_swap_url, proxies=proxies).json()[0]['price'])

            data = {
                "Time": int(time.time() * 1000),
                "BTC": {
                    "CW": float("%.2f" % (btc_cw_price - btc_spot_price)),
                    "NW": float("%.2f" % (btc_nw_price - btc_spot_price)),
                    "CQ": float("%.2f" % (btc_cq_price - btc_spot_price)),
                    "NQ": float("%.2f" % (btc_nq_price - btc_spot_price)),
                    "SWAP": float("%.2f" % (btc_swap_price - btc_spot_price)),
                    "Market": "USD",
                    "Price": {
                        "SPOT": btc_spot_price,
                        "CW": btc_cw_price,
                        "NW": btc_nw_price,
                        "CQ": btc_cq_price,
                        "NQ": btc_nq_price,
                        "SWAP": btc_swap_price
                    }
                }
            }

            redis_connect = redis.Redis(host="47.107.228.85", port=6379, password="20ab20!2#Spider!alxmH")
            redis_connect.set("okex:difference_in_price", json.dumps(data))

            logger.info("Push: {}".format(data))
            break
        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    while True:
        send_request()
        time.sleep(10)

