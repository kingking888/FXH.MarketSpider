#!/usr/bin/env python
# -*- coding:utf-8 -*-

import requests
import re
import time
import redis
import json

from loguru import logger



headers = {
"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
"cache-control": "max-age=0",
"cookie": "__cfduid=d70c0d9c28fe8b67ddf4016a9dc3b8dec1576650362; _ga=GA1.2.1603170977.1576650364; _xicah=60fc1366-6c7951eb; cf_clearance=c60cc928236eb8eec990b51f47dd6fb23b299fd0-1577786125-0-250; _gid=GA1.2.1329463.1577786126; showCoins=btc,eth,xrp,bch,ltc,bsv",
"referer": "https://bitinfocharts.com/zh/comparison/activeaddresses-btc-eth.html",
"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36"}

url = "https://bitinfocharts.com/zh/comparison/activeaddresses-btc.html"


def send_request():
    html = requests.get(url, headers=headers).text

    pattern = re.compile("(\[\[(.*?)\]\])", re.S)
    result = pattern.findall(html)[0][0]

    btc_addresses = result.replace("new Date(", "").replace(")", "").replace("null", "0")

    # print(len(btc_addresses))
    btc_addresses = eval(btc_addresses)

    data_list = []
    for item in btc_addresses:
        # print(item)
        item[0] = int(time.mktime(time.strptime(item[0], "%Y/%m/%d")))
        item[1] = item[1] if item[1] else 0
        data_list.append(item)

    redis_connect.lpush("Coin:ReduceHalf:btc-active-addresse-list", json.dumps(data_list))
    logger.info("Push: {}".format(data_list))
    #
    # redis_connect.rpop("Coin:ReduceHalf:btc-active-addresse-list")
    # logger.info("pop 完成...")


if __name__ == "__main__":
    # redis_connect = redis.Redis(host="122.228.200.88", port=6378, db=0, password="redis123456")
    redis_connect = redis.Redis(host="47.107.228.85", port=6379, db=0, password="20ab20!2#Spider!alxmH")

    while True:
        try:
            send_request()
            time.sleep(600)
        except Exception as e:
            logger.error(e)
            time.sleep(10)
            redis_connect = redis.Redis(host="47.107.228.85", port=6379, db=0, password="20ab20!2#Spider!alxmH")
