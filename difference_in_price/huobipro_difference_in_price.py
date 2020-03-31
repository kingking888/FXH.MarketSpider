#!/usr/bin/env python
# -*- coding:utf-8 -*-

import requests
import time
import random
import json
import redis
from loguru import logger


# 现货:
btc_spot_url = "https://api.huobi.pro/market/trade?symbol=btcusdt&limit=1"
# 期货:
btc_cw_url = "https://api.btcgateway.pro/market/trade?symbol=BTC_CW&limit=1"
btc_nw_url = "https://api.btcgateway.pro/market/trade?symbol=BTC_NW&limit=1"
btc_cq_url = "https://api.btcgateway.pro/market/trade?symbol=BTC_CQ&limit=1"
btc_swap_url = "https://api.btcgateway.pro/swap-ex/market/trade?contract_code=BTC-USD&limit=1"


def send_request():
    while True:
        try:
            proxies = {"https" : "http://127.0.0.1:{}".format(random.randint(8080, 8232))}
            btc_spot_price = float(requests.get(btc_spot_url, proxies=proxies).json()['tick']['data'][0]['price'])
            btc_cw_price = float(requests.get(btc_cw_url, proxies=proxies).json()['tick']['data'][0]['price'])
            btc_nw_price = float(requests.get(btc_nw_url, proxies=proxies).json()['tick']['data'][0]['price'])
            btc_cq_price = float(requests.get(btc_cq_url, proxies=proxies).json()['tick']['data'][0]['price'])
            btc_swap_price = float(requests.get(btc_swap_url, proxies=proxies).json()['tick']['data'][0]['price'])

            data = {
                "Time": int(time.time() * 1000),
                "CW": float("%.2f" % (btc_cw_price - btc_spot_price)),
                "NW": float("%.2f" % (btc_nw_price - btc_spot_price)),
                "CQ": float("%.2f" % (btc_cq_price - btc_spot_price)),
                "NQ": 0,
                "SWAP": float("%.2f" % (btc_swap_price - btc_spot_price)),
                "Price": {
                    "SPOT": btc_spot_price,
                    "CW": btc_cw_price,
                    "NW": btc_nw_price,
                    "CQ": btc_cq_price,
                    "NQ": 0,
                    "SWAP": btc_swap_price
                }
            }

            redis_connect = redis.Redis(host="47.107.228.85", port=6379, password="20ab20!2#Spider!alxmH")
            redis_connect.set("huobipro:btc:usd:difference_in_price", json.dumps(data))

            logger.info("Push: {}".format(data))
            break
        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    while True:
        send_request()
        time.sleep(10)
