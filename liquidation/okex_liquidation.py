#!/usr/bin/env python
# -*- coding:utf-8 -*-

import time
import json
from datetime import datetime
import threading
import random

import requests
import redis
from loguru import logger


# 是否使用代理（如果为True，则代理按 127.0.0.1:8080-8232；如果为False，则不使用代理）
proxies = True

liquidation_info_list = []
last_liquidation_info_list = []


class OkexSpider(object):
    def __init__(self):
        self.redis_connect = redis.Redis(host='47.107.228.85', port=6379, password='20ab20!2#Spider!alxmH')
        self.last_time = 0

    def main(self, index):
        logger.info('数字货币：{} {} 强制平仓数据获取开始...'.format(liquidation_info_list[index]['symbol'], liquidation_info_list[index]['timeid']))

        while True:
            liquidation = liquidation_info_list[index]

            self.symbol = liquidation['symbol']
            self.timeid = liquidation['timeid']
            self.liquidation_url = liquidation['liquidation_url']
            self.redis_key = "okex:futures:liquidation:{}_{}_forced_liquidation".format(self.symbol.split("-")[0], self.timeid)

            try:
                result_list = requests.get(self.liquidation_url, proxies=proxy()).json()
                data_list = result_list[::-1]
                for data in data_list:
                    utc_time = data.get("created_at").replace("T", " ").replace("Z", "")
                    struct_time = datetime.strptime(utc_time, "%Y-%m-%d %H:%M:%S.%f")
                    t = int(time.mktime(struct_time.timetuple()))
                    if t > self.last_time:
                        item = {}
                        item["Time"] = t
                        item["Pair1"] = self.symbol.split("-")[0]
                        item["Pair2"] = self.symbol.split("-")[1]
                        item["Title"] = self.timeid
                        item["Price"] = float(data['price'])
                        item["Liquidation"] = "Long" if data['type'] == "3" else "Short"
                        item["Volume"] = int(data['size'])
                        item["USD"] = int(data['size']) * 100 if item["Pair1"] == 'BTC' else int(data['size']) * 10

                        self.redis_connect.lpush(self.redis_key, json.dumps(item))

                        self.last_time = t
                        logger.info(item)
                    else:
                        continue
            except Exception as e:
                logger.error(e)
                logger.error('数字货币： {}-USD-{} connect ws error, retry...'.format(self.symbol, self.timeid))


def proxy():
    global proxies
    if proxies:
        return {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8232))}
    else:
        return None

def get_liquidation():
    while True:
        try:
            futures_url = "https://www.okex.com/api/futures/v3/instruments"
            swap_url = "https://www.okex.com/api/swap/v3/instruments"

            # proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}

            futures_list = requests.get(futures_url, proxies=proxy()).json()
            futures_list += requests.get(swap_url, proxies=proxy()).json()

            futures_symbol_list = []
            for futures in futures_list:
                if futures.get("quote_currency") == "USD":
                    item = {}
                    item['symbol'] = futures['instrument_id']
                    timeid = futures.get('alias')
                    if timeid == 'this_week':
                        item['timeid'] = 'CW'
                    elif timeid == 'next_week':
                        item['timeid'] = 'NW'
                    elif timeid == 'quarter':
                        item['timeid'] = 'CQ'
                    elif timeid == 'bi_quarter':
                        item['timeid'] = 'NQ'
                    else:
                        item['timeid'] = 'SWAP'
                    futures_symbol_list.append(item)

            futures_liquidation_url = "https://www.okex.com/api/futures/v3/instruments/{}/liquidation?status=1"
            swap_liquidation_url = "https://www.okex.com/api/swap/v3/instruments/{}/liquidation?status=1"

            liquidation_list = []
            for futures_symbol in futures_symbol_list:
                if futures_symbol['timeid'] == 'SWAP':
                    futures_symbol.setdefault('liquidation_url', swap_liquidation_url.format(futures_symbol.get("symbol")))
                else:
                    futures_symbol.setdefault('liquidation_url', futures_liquidation_url.format(futures_symbol.get("symbol")))
                # logger.info(futures_symbol)
                liquidation_list.append(futures_symbol)


            # 获取OKEx 所有合约对的 强平订单信息
            # [{'symbol': 'XRP-USD-200221', 'timeid': 'CW', 'liquidation_url': 'https://www.okex.com/api/futures/v3/instruments/XRP-USD-200221/liquidation?status=1'}]
            # print(liquidation_list)
            # print(len(liquidation_list))

            global liquidation_info_list, last_liquidation_info_list
            # list
            #_ = [liquidation.setdefault('index', index) for index, liquidation in enumerate(liquidation_list) if "BTC" in liquidation['symbol']]
            # liquidation_info_list = liquidation_list

            # dict
            liquidation_info_list = {index: liquidation for index, liquidation in enumerate(liquidation_list)}
            if last_liquidation_info_list != liquidation_info_list:
                last_liquidation_info_list = liquidation_info_list
                for liquidation_info in liquidation_info_list.items():
                    logger.info(liquidation_info)

            time.sleep(5)
        except Exception as e:
            logger.error(e)

if __name__ == "__main__":

    # 子线程组
    thread_list = []

    t = threading.Thread(target=get_liquidation, args=())
    thread_list.append(t)
    t.start()

    while True:
        if liquidation_info_list:
            for index in liquidation_info_list:
                spider = OkexSpider()
                t = threading.Thread(target=spider.main, args=(index,))
                thread_list.append(t)
                t.start()
            break
        time.sleep(1)

    while True:
        length = len(threading.enumerate())
        logger.info('当前运行的线程数为：%d' % length)
        time.sleep(60)
        if length <= 1:
            break

    # 主线程等待子线程执行完毕
    for t in thread_list:
        t.join()

