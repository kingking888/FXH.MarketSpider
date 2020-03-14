#!/usr/bin/env python
# -*- coding:utf-8 -*-

import requests
import json
import random
import time
import redis
from loguru import logger


class HuobiproSpider(object):
    def __init__(self):
        self.contract_info_url = "https://api.btcgateway.pro/api/v1/contract_contract_info"
        self.liquidation_url = "https://api.btcgateway.pro/api/v1/contract_liquidation_orders?symbol=BTC&trade_type=0&create_date=7&page_size=50"
        self.btc_info = None

        self.redis_connect = redis.Redis(host='47.107.228.85', port=6379, password='20ab20!2#Spider!alxmH')
        self.redis_key = "huobipro:futures:liquidation:{}_{}_forced_liquidation"

        self.last_time = 0
        self.first_run = True

    def main(self):
        logger.info('数字货币：BTC 强制平仓数据获取开始：')
        while True:
            try:
                if int(time.time()) % 300 > 290 or self.first_run:
                    logger.info("正在获取最新合约信息(每5分钟更新): ")
                    contract_info_list = requests.get(self.contract_info_url).json()['data']
                    # 获取BTC币种数据   {'BTC200221': 'this_week', 'BTC200228': 'next_week', 'BTC200327': 'quarter'}
                    self.btc_info = {contract_info['contract_code']: contract_info['contract_type'] for contract_info in contract_info_list if contract_info['symbol'] == "BTC"}
                    logger.info(self.btc_info)
                    self.first_run = False
                proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}
                # result = requests.get(self.liquidation_url, proxies=proxies).json()
                result = requests.get(self.liquidation_url).json()

                if result['status'] == 'ok':
                    data_list = result['data']['orders'][::-1]
                    for data in data_list:

                        t = data['created_at'] // 1000
                        if t > self.last_time:
                            item = {}
                            item['Time'] = t
                            if self.btc_info[data['contract_code']] == 'this_week':
                                item['Title'] = 'CW'
                            elif self.btc_info[data['contract_code']] == 'next_week':
                                item['Title'] = 'NW'
                            else:
                                item['Title'] = 'CQ'

                            item['Pair1'] = data['symbol']
                            item['Pair2'] = 'USD'
                            item['Price'] = data['price']
                            item['Liquidation'] = 'Short' if data['direction'] == 'sell' else 'Long'
                            item['Volume'] = data['volume']
                            item['USD'] = data['volume'] * 100

                            self.last_time = t
                            self.redis_connect.lpush(self.redis_key.format(item['Pair1'], item['Title']), json.dumps(item))
                            logger.info(item)
                        else:
                            continue

            except Exception as e:
                logger.error(e)
                logger.info('数字货币：connect ws error, retrying...')


if __name__ == "__main__":
    spider = HuobiproSpider()
    spider.main()

