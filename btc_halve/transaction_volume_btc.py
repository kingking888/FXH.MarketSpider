#/usr/bin/env python
# -*- coding:utf-8 -*-

import time

from loguru import logger
import redis
import requests
import json


def send_request(url):
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "origin": "https://blockchair.com",
        "referer": "https://blockchair.com/",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"
    }

    try:
        resp = requests.get(url, headers=headers)
        return resp
    except:
        return

def parse_response(resp):
    data = resp.json()
    save_data(data['data'])


def save_data(data):
    data_list = []
    for item in data:
        utc_time = int(time.mktime(time.strptime(item['date'], "%Y-%m-%d"))) - 28800
        total = item['sum(output_total)'] / 100000000
        data = [total, utc_time]
        data_list.append(data)
        # logger.info("deal: ", data)

    redis_connect.lpush("Coin:ReduceHalf:transaction-volume-btc-list", json.dumps(data_list))
    logger.info("Push: {}".format(data_list))
    # redis_connect.rpop("Coin:ReduceHalf:transaction-volume-btc-list")
    # logger.info("pop 完成...")


def main(url):
    resp = send_request(url)
    if resp:
        parse_response(resp)
    else:
        print("err")


if __name__ == "__main__":
    redis_connect = redis.Redis(host="r-wz9jjob47pi7m6rykxpd.redis.rds.aliyuncs.com", port=6379, db=0, password="20ab20!2#Spider!alxmH")

    while True:
        try:
            # redis_connect = redis.Redis(host="122.228.200.88", port=6378, db=0, password="redis123456")
            # redis_connect = redis.Redis(host="r-j6ce6n77kflx9yxalqpd.redis.rds.aliyuncs.com", port=6379, db=0, password="Spider#!AbcK982_Kline")
            url = "https://api.blockchair.com/bitcoin/blocks?a=date,sum(output_total)"
            main(url)
            # time.sleep(3600)
            time.sleep(600)
        except Exception as e:
            logger.error(e)
            time.sleep(10)
            redis_connect = redis.Redis(host="r-wz9jjob47pi7m6rykxpd.redis.rds.aliyuncs.com", port=6379, db=0, password="20ab20!2#Spider!alxmH")
