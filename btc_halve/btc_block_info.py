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
    "Cookie": "_ga=GA1.2.1244702135.1576054842; intercom-id-yttqkdli=13951abc-335d-422b-a82e-91738c393c52; _globalGA=GA1.2.1244702135.1576054842; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2216f0df8ebec652-08d5742f329bb5-6701434-2073600-16f0df8ebedb02%22%2C%22%24device_id%22%3A%2216f0df8ebec652-08d5742f329bb5-6701434-2073600-16f0df8ebedb02%22%2C%22props%22%3A%7B%7D%7D; acw_tc=2760825815863112314588210e2bcb838cf02e5312110188d0ed38629eb169; _gid=GA1.2.1985573557.1586311234; _globalGA_gid=GA1.2.683565384.1586311234; hideHeaderLayer=false; acw_sc__v2=5e8d8ef1400d64d50e702341c66486e2fbe9b567; _gat=1; _gat_globalGA=1; io=zNYIMKbXACkfLsIkA3lc; acw_sc__v3=5e8d915f66ffafaa4ff8ca2779898ce651243b61; XSRF-TOKEN=eyJpdiI6IjV5WVZtZ2pjYlZ2VFlMRzBBTjZBMEE9PSIsInZhbHVlIjoicXphbE1HSlVNS0MyclYzbjhoUjBsRFNEeTQ4OGVCMDZjbk5lbUFKUDJyQTczQXNQeTZZZnpuRFM5MUpmUXJIUiIsIm1hYyI6IjU1ZDJkY2IyNWYyMGFhZjM3N2UwOGU2OGE0NTI3MmFiZjIyMzgyYTNjNDZiZjMyMmI4MzQ4MTMyN2VmOGIxOTgifQ%3D%3D; laravel_session=eyJpdiI6ImpmOVJKd2pER0lkeTRUOXRsaGtwRXc9PSIsInZhbHVlIjoiNnhPaFMrUGNpcDNYWFN0NGFLblJmWW1zZU1xT0hmbDRwU2VuSmlsa3lKUUd5WnF4ckxTUjBKVCtkUHRoMjFlTiIsIm1hYyI6IjBiZGJmOWJkYmNjM2ZlMjAxZmZkNzBmOTdmNTY4ZTNmNWIzNGVmYWFlZDdiODRhNTg5M2E1YTYzN2ZhNzExY2IifQ%3D%3D",
    "Host": "btc.com",
    "Referer": "https://btc.com/",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36"
}

def send_request():
    url1 = "https://chain.api.btc.com/v3/block/latest"
    url2 = "https://btc.com/"
    url3 = "https://btc.com/stats/unconfirmed-tx"

    proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}
    resp1 = requests.get(url1, headers=headers1, proxies=proxies)
    data = resp1.json()['data']

    proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}
    resp2 = requests.get(url2, headers=headers2, proxies=proxies)
    new_data = parse_response(resp2)

    proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}
    obj2 = etree.HTML(requests.get(url3, headers=headers2, proxies=proxies).content)
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
    # 预测减半时间
    new_data['next_halve_time'] = obj.xpath("//div[@class='panel-body']/ul[3]/li/dl/dd/div[1]/span[2]/text()")[0]

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
        "next_halve_time": new_data.get("next_halve_time"),
    }

    while True:
        try:
            redis_connect.lpush("Coin:ReduceHalf:btc_block_info", json.dumps(item))
            logger.info("Push: {}".format(item))
            break
        except:
            pass


if __name__ == "__main__":
    redis_connect = redis.Redis(host="47.107.228.85", port=6379, db=0, password="20ab20!2#Spider!alxmH")
    while True:
        try:
            # redis_connet = redis.Redis(host="r-j6ce6n77kflx9yxalqpd.redis.rds.aliyuncs.com", port=6379, db=0, password="Spider#!AbcK982_Kline")
            send_request()
            time.sleep(30)
        except Exception as e:
            logger.error(e)
            time.sleep(1)
            redis_connect = redis.Redis(host="47.107.228.85", port=6379, db=0, password="20ab20!2#Spider!alxmH")

