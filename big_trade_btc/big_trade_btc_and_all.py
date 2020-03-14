#!/usr/bin/env python
# -*- coding:utf-8 -*-

import requests
import json
import redis
import time
import random
from loguru import logger


data_set = set()
redis_connect = redis.Redis(host='47.107.228.85', port=6379, password="20ab20!2#Spider!alxmH")

while True:
    try:
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            # "accept-encoding" : "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "cache-control": "max-age=0",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537."+ str(random.randint(11, 99))

        }

        url = 'https://api.whale-alert.io/feed.csv'
        proxies={"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}
        text = requests.get(url, headers=headers, proxies=proxies).text

        line_list = text.split("\n")
        tx_list = [line.split(",") for line in line_list]

        for tx in tx_list:
            item = {
                "ID": tx[0],
                "Time": tx[1],
                "Symbol": tx[2],
                "Value": tx[3],
                "Price": tx[4],
                # "Transfer": tx[5],
                "InputEntity": tx[6],
                "InputAddressType": tx[7],
                "OutputEntity": tx[8],
                "OutputAddressType": tx[9]
            }

            if "exchange" in [item['InputAddressType'], item['OutputAddressType']]:
                data = json.dumps(item)
                if data not in data_set:
                    data_set.add(data)
                    logger.info(data)
                    redis_connect.lpush("big_trade_all", data)
                else:
                    # logger.info("repeat")
                    pass

        if len(data_set) >= 1000:
            data_set = set(list(data_set)[:200])
        time.sleep(0.1)
    except Exception as e:
        logger.error(e)



