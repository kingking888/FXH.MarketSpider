#!/usr/bin/env python
# -*- coding:utf-8 -*-


import time
from datetime import datetime
from loguru import logger

import redis
import requests
import json
from datetime import datetime

headers = {
	"accept":"application/json, text/plain, */*",
	"accept-encoding":"gzip, deflate, br",
	"accept-language":"zh-CN,zh;q=0.9,en;q=0.8",
	"origin":"https://blockchair.com",
	"referer":"https://blockchair.com/",
	"sec-fetch-mode":"cors",
	"sec-fetch-site":"same-site",
	"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36"
}


def send_request(url):
    try:
        resp = requests.get(url, headers=headers)
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
        utc_time = int(time.mktime(time.strptime(item['date'], "%Y-%m-%d"))) - 28800
        data = [item["sum(cdd_total)"], utc_time]
        data_list.append(data)
        # print("deal: ", data)

    redis_connect.lpush("Coin:ReduceHalf:coindays-destroyed-list", json.dumps(data_list))
    logger.info("Push: {}".format(data_list))
    # redis_connect.rpop("Coin:ReduceHalf:coindays-destroyed-list")
    # logger.info("pop 完成...")


def main(url):
    resp = send_request(url)
    if resp:
        parse_response(resp)


if __name__ == "__main__":
    redis_connect = redis.Redis(host="r-wz9jjob47pi7m6rykxpd.redis.rds.aliyuncs.com", port=6379, db=0, password="20ab20!2#Spider!alxmH")

    while True:
        try:
            time_id = str(datetime.utcnow())[:10]
            url = f"https://api.blockchair.com/bitcoin/transactions?a=date,sum(cdd_total)&q=time(2009-01-03..{time_id})"
            logger.info(url)
            main(url)
            time.sleep(600)

        except Exception as e:
            logger.error(e)
            time.sleep(10)
            redis_connect = redis.Redis(host="r-wz9jjob47pi7m6rykxpd.redis.rds.aliyuncs.com", port=6379, db=0, password="20ab20!2#Spider!alxmH")
