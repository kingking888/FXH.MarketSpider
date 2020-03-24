#!/usr/bin/env python
# -*- coding:utf-8 -*-

import requests
import time
import random
import json
import redis
from loguru import logger

# BTC结算合约
btc_usd_url = "https://fx-api.gateio.ws/api/v4/futures/btc/contracts/BTC_USD"
# USDT结算合约
btc_usdt_url = "https://fx-api.gateio.ws/api/v4/futures/usdt/contracts/BTC_USDT"
# BTC/USDT 现货价
btc_spot_url = "https://data.gateio.life/api2/1/ticker/btc_usdt"

def send_request():
    while True:
        try:

            # proxies = {"https" : "http://127.0.0.1:{}".format(random.randint(8080, 8232))}
            proxies = {}
            btc_spot_price = float(requests.get(btc_spot_url, proxies=proxies).json()['last'])
            btc_futures_price = {
                "btc_usd_price": float(requests.get(btc_usd_url, proxies=proxies).json()['last_price']),
                "btc_usdt_price": float(requests.get(btc_usdt_url, proxies=proxies).json()['last_price'])
            }

            redis_connect = redis.Redis(host="47.107.228.85", port=6379, password="20ab20!2#Spider!alxmH")

            for key in btc_futures_price:
                data = {
                    "Time": int(time.time() * 1000),
                    "CW": 0,
                    "NW": 0,
                    "CQ": 0,
                    "NQ": 0,
                    "SWAP": float("%.2f" % (btc_futures_price.get(key) - btc_spot_price)),
                    "Price": {
                        "SPOT": btc_spot_price,
                        "CW": 0,
                        "NW": 0,
                        "CQ": 0,
                        "NQ": 0,
                        "SWAP": btc_futures_price.get(key)
                    }
                }

                if 'usdt' in key:
                    redis_key = "gate-io:btc:usdt:difference_in_price"
                else:
                    redis_key = "gate-io:btc:usd:difference_in_price"

                redis_connect.set(redis_key, json.dumps(data))
                logger.info("Push: {}".format(data))
            break
        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    while True:
        send_request()
        time.sleep(10)
