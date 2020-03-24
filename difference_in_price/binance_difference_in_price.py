#!/usr/bin/env python
# -*- coding:utf-8 -*-

import requests
import time
import random
import json
import redis
from loguru import logger


# 现货:
btc_spot_url = "https://api.binance.com/api/v3/trades?symbol=BTCUSDT&limit=1"
# 期货:
btc_swap_url = "https://fapi.binance.com/fapi/v1/trades?symbol=BTCUSDT&limit=1"


def send_request():
    while True:
        try:
            proxies = {"https" : "http://127.0.0.1:{}".format(random.randint(8080, 8232))}
            btc_spot_price = float(requests.get(btc_spot_url, proxies=proxies).json()[0]['price'])
            btc_swap_price = float(requests.get(btc_swap_url, proxies=proxies).json()[0]['price'])

            data = {
                "Time": int(time.time() * 1000),
                "CW": 0,
                "NW": 0,
                "CQ": 0,
                "NQ": 0,
                "SWAP": float("%.2f" % (btc_swap_price - btc_spot_price)),
                "Price": {
                    "SPOT": btc_spot_price,
                    "CW": 0,
                    "NW": 0,
                    "CQ": 0,
                    "NQ": 0,
                    "SWAP": btc_swap_price
                }
            }

            redis_connect = redis.Redis(host="47.107.228.85", port=6379, password="20ab20!2#Spider!alxmH")
            redis_connect.set("binance:btc:usdt:difference_in_price", json.dumps(data))

            logger.info("Push: {}".format(data))
            break
        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    while True:
        send_request()
        time.sleep(10)
