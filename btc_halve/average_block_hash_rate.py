#!/usr/bin/env python
# -*- coding:utf-8 -*-


import time
from datetime import datetime
from loguru import logger

import redis
import requests
import json

headers = {
    "accept": "*/*",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    "content-length": "66",
    "content-type": "application/json",
    "origin": "https://chain.info",
    "referer": "https://chain.info/chart/averageBlockHashRate",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"
}

def send_request(url):
    try:
        start = int(str(datetime.now())[:10].replace("-", ""))
        payload = json.dumps({"query": ["averageBlockHashRate"], "start": start, "offset": 10000})
        resp = requests.post(url, data=payload, headers=headers)
        return resp
    except Exception as e:
        print(e)
        return

def parse_response(resp):
    # print(resp.content)
    data = resp.json()
    save_data(data['data'])

def save_data(data):
    data_list = []
    for item in data:
        d = str(item.pop('coinDate'))
        item['date'] = "-".join([d[:4], d[4:6], d[6:8]])
        utc_time = int(time.mktime(time.strptime(item['date'], "%Y-%m-%d"))) - 28800
        hash_rate = item['averageBlockHashRate'] * 10 ** 15

        data = [hash_rate, utc_time]
        data_list.append(data)
        # print("deal: ", data)

    redis_connect.lpush("Coin:ReduceHalf:average-block-hash-rate-list", json.dumps(data_list))
    logger.info("Push: {}".format(data_list))
    # redis_connect.rpop("Coin:ReduceHalf:average-block-hash-rate-list")
    # logger.info("pop 完成...")


def main(url):
    resp = send_request(url)
    if resp:
        parse_response(resp)


if __name__ == "__main__":
    # redis_connect = redis.Redis(host="122.228.200.88", port=6378, db=0, password="redis123456")
    redis_connect = redis.Redis(host="47.107.228.85", port=6379, db=0, password="20ab20!2#Spider!alxmH")

    while True:
        try:
            url = "https://api.chain.info/v1/stats/dailydata"
            main(url)
            time.sleep(600)
            # time.sleep(3600)c
        except Exception as e:
            logger.error(e)
            time.sleep(10)
            redis_connect = redis.Redis(host="47.107.228.85", port=6379, db=0, password="20ab20!2#Spider!alxmH")
