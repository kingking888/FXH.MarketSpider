#!/usr/bin/env python
# -*- coding:utf-8 -*-


import time
from datetime import datetime
from loguru import logger
from lxml import etree
import random

import redis
import requests
import json
from datetime import datetime

headers1 = {
	"accept":"application/json, text/plain, */*",
	"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36"
}

headers2 = {
	"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
	"Accept-Encoding": "gzip, deflate, br",
	"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
	"Cache-Control": "max-age=0",
	"Connection": "keep-alive",
	"Cookie": "_ga=GA1.2.1244702135.1576054842; intercom-id-yttqkdli=13951abc-335d-422b-a82e-91738c393c52; _globalGA=GA1.2.1244702135.1576054842; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2216f0df8ebec652-08d5742f329bb5-6701434-2073600-16f0df8ebedb02%22%2C%22%24device_id%22%3A%2216f0df8ebec652-08d5742f329bb5-6701434-2073600-16f0df8ebedb02%22%2C%22props%22%3A%7B%7D%7D; acw_tc=2760825e15788329245613124e099621fdac97a89428a4b28ec16e28317751; _gid=GA1.2.732179981.1578997835; _globalGA_gid=GA1.2.955375280.1578997835; io=qNLvlYryHklF-_48AJ2D; acw_sc__v2=5e1e725fd7bb6b2c93c939b464539306070f3ef6; acw_sc__v3=5e1e72604872fb483c2862e9f20890415d8e2c77; XSRF-TOKEN=eyJpdiI6Ilg5Q2hLbkhoakllWFVVRjh1T3djdXc9PSIsInZhbHVlIjoiV0h0S3RtU01IMGE3Q21NMUgxSWJ6K2pPeG5hTU8xcHBKRXY3MmtPV3F2SjVCT3hwSUlPMGJlcGF4Z2RlNCs1WCIsIm1hYyI6ImM5YTI1MDhhNWZlMTI0NGE1MDgzZGE2ZGM4OGY3MWYxZDRlZmRmMWQ2Y2ZkYzUzNGNiYWE3NmIyNmQ5MTA4OWEifQ%3D%3D; laravel_session=eyJpdiI6IjBPRE42bXdGdjJHNnZYRm92dXJnM0E9PSIsInZhbHVlIjoiWk1TM1V5b2ZpdVZVZFRcL1Nic3VOdHFqdkhwMm00SnN3aWVENUFwWkRpZ0xrcHNQeFdyWmpvNUs2MlFLMWh6RFYiLCJtYWMiOiI0ZjNlOWE0ZTYyMDE0M2JkNDczMGUzZmI1MTU3NjQyNTk5NWZkOTQxY2QzNTQ3NmQ5ODcyMmMxM2JhNzJmNGRjIn0%3D; LeHHli2MjjevYkpabW4Ol0edXbKQFh1Xv2q5KjBl=eyJpdiI6Im5vS1FJOE5IZDJWMzFsaTFNNHJJbXc9PSIsInZhbHVlIjoid0E2dTZSTmZWY0tGRFhVd2k4bzRJeHNIOTVSMVRreVRsR2NTUmlHR05Ha0NnN0VDeDlhMTNES1lkZDN0OFZoK2p4MSt4M0ZwZ0QzTkY0dHhxM05iVkRYcERORjBSUUYyRnYyTVJCMlIrQlUrOGE1NGc3eVN6ZURnQTBLUmtjUExWMnROK0dlUVlvTlpNd0NuRTltenQwcUVxdWxMK1B2eUt5MHFzXC9YMDREa05lNGpScVJWTG1kVG9VcmFpTXdpTnhzaHJCanpZempZVmRmMndNZlBSK3AzY1E2YlRITUlYeE8rRmc2WXVVbGhIekl2VEtHdGdrcDRnVU04emsyRjI0TGlHdCtEUzdEV0RzcllyWmh1cW9YT1wvTXhaVGNjOWI5ZHRXTFRYZlREWm9cLzF4dGhBR2ZvV1ZTaE80WEFFYkgiLCJtYWMiOiJhY2I4NjExMWY3MmFkMWYzZWYzNDFmODc3ZGEyZmZiNTM0ODdlMGU3NmFhNjQ2ZTU1ZmMwYzI2ZmViNDEwYjNiIn0%3D",
	"Host": "btc.com",
	"Upgrade-Insecure-Requests": "1",
	"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36"
}

def send_request():
    url1 = "https://chain.api.btc.com/v3/block/latest"
    url2 = "https://btc.com/"

    proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}
    resp1 = requests.get(url1, headers=headers1, proxies=proxies)
    data = resp1.json()['data']

    proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}
    resp2 = requests.get(url2, headers=headers2, proxies=proxies)
    new_data = parse_response(resp2)

    proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}
    obj2 = etree.HTML(requests.get("https://btc.com/stats/unconfirmed-tx", headers=headers2, proxies=proxies).content)
    best_fees = obj2.xpath('//b[@id="fees_recommended_bk"]/text()')[0].strip() + " BTC/KvB"

    save_data(data, new_data, best_fees)


def parse_response(resp):
    html = resp.content
    obj = etree.HTML(html)

    new_data = {}
    # 未确认的交易数
    new_data['unconfirmed_txs_count'] = obj.xpath('//span[@class="tx-count"]/text()')[0]
    # 近2周区块体积中位数
    new_data['median_block_volume'] = obj.xpath("//div[@class='panel-body']/ul[5]/li/dl[2]/dd//text()")[0]
    text = "".join(obj.xpath("//div[@class='panel-body']/ul/li[3]/dl/dd//text()"))
    # 24h 每T 算力收益
    new_data['every_T_earnings'] = text.replace(" ", "").replace("\n", "")
    # 全网算力
    new_data['hashrate'] = obj.xpath("//div[@class='panel-body']/ul[1]/li[1]/dl/dd/span/text()")[0].strip()
    # 全网难度
    new_data['difficulty'] = "".join(obj.xpath("//div[@class='panel-body']/ul[1]/li[2]/dl/dd//text()")).replace("\n", "").replace(" ", "")
    # 预计下次难度
    new_data['next_difficulty_estimated'] = "".join(obj.xpath("//div[@class='panel-body']/ul[2]/li[1]/dl/dd//text()")).replace("\n", "").replace(" ", "")
    # 下次调整时间
    t = "".join(obj.xpath("//div[@class='panel-body']/ul[2]/li[2]/dl/dd//text()")).replace("\n", "").replace(" ", "")
    d = t.replace("天", ".").replace("小时", "").split(".")
    new_data['next_difficulty_date'] = int(time.time()) + int(d[0]) * 86400 + int(d[1]) * 3600

    return new_data


def save_data(data, new_data, best_fees):
    item = {
        "timestamp": data.get("timestamp"),
        "hash": data.get("hash"),
        "height": data.get("height"),
        "size": data.get("size"),
        "reward_block": data.get("reward_block"),
        "reward_fees": data.get("reward_fees"),
        "confirmations": data.get("confirmations"),
        "tx_count": data.get("tx_count"),
        "relayed": data.get("extras").get("pool_name"),
        "remaining_block_quantity": 630000 - data.get("height"),
        'unconfirmed_txs_count': new_data.get('unconfirmed_txs_count'),
        'median_block_volume': new_data.get('median_block_volume'),
        "every_T_earnings": new_data.get("every_T_earnings"),  # 1T*24H=0.00001702BTC
        "best_fees": best_fees,
        "hashrate": new_data.get('hashrate'),
        "difficulty": new_data.get('difficulty'),
        "next_difficulty_estimated": new_data.get('next_difficulty_estimated'),
        "next_difficulty_date": new_data.get('next_difficulty_date'),
    }

    while True:
        try:
            redis_connect.lpush("Coin:ReduceHalf:btc_block_info", json.dumps(item))
            logger.info("Push: {}".format(item))
            break
        except:
            pass


if __name__ == "__main__":
    redis_connect = redis.Redis(host="r-wz9jjob47pi7m6rykxpd.redis.rds.aliyuncs.com", port=6379, db=0, password="20ab20!2#Spider!alxmH")
    while True:
        try:
            # redis_connet = redis.Redis(host="r-j6ce6n77kflx9yxalqpd.redis.rds.aliyuncs.com", port=6379, db=0, password="Spider#!AbcK982_Kline")
            send_request()
            time.sleep(30)
        except Exception as e:
            logger.error(e)
            time.sleep(1)
            redis_connect = redis.Redis(host="r-wz9jjob47pi7m6rykxpd.redis.rds.aliyuncs.com", port=6379, db=0, password="20ab20!2#Spider!alxmH")

