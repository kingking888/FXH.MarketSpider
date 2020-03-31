import requests
import time
import json
import redis
from loguru import logger
import random


class HuobiproSpider(object):
    def __init__(self):
        self.url = "https://api.hbdm.com/api/v1/contract_open_interest"
        self.info_url = "https://api.hbdm.com/api/v1/contract_contract_info"
        self.swap_btc_url = "https://api.btcgateway.pro/swap-api/v1/swap_open_interest"
        # self.redis_connect = redis.Redis(host='122.228.200.88', port=6378, db=0, password="redis123456")
        self.redis_connect = redis.Redis(
            host='47.107.228.85',
            port=6379,
            db=0,
            password="20ab20!2#Spider!alxmH"
        )

    def send_request(self):
        data_list = requests.get(self.info_url).json()['data']
        while True:
            ts = int(time.time())
            if ts % 60 != 0:
                time.sleep(0.9)
                continue

            for data in data_list:
                if data['symbol'] == "BTC":
                    params = {"symbol": data['symbol'], "contract_type": data['contract_type']}
                    while True:
                        try:
                            response = requests.get(self.url, params=params, proxies={"https": "http://127.0.0.1:{}".format(random.randint(8081, 8323))})
                            self.parse_response(response, ts)
                            break
                        except Exception as e:
                            logger.error(e)
                            logger.error("正在重新发送请求...")

            while True:
                try:
                    response = requests.get(self.swap_btc_url, proxies={"https": "http://127.0.0.1:{}".format(random.randint(8081, 8323))})
                    self.parse_response(response, ts)
                    break
                except Exception as e:
                    logger.error(e)
                    logger.error("正在重新发送请求...")

            logger.info("采集结束，一分钟后再次采集...")
            time.sleep(20)


    def parse_response(self, response, ts):
        item = {}
        item['Time'] = ts
        data = response.json()['data'][0]
        contract_type = data.get('contract_type')

        if contract_type == 'this_week':
            item['Title'] = 'CW'
        elif contract_type == 'next_week':
            item['Title'] = 'NW'
        elif contract_type == 'quarter':
            item['Title'] = 'CQ'
        else:
            item['Title'] = "SWAP"

        item['Pair1'] = data['symbol']
        item['Pair2'] = 'USD'
        item['Volume'] = data['volume']
        item['Usd'] = data['volume'] * 100

        redis_key_name = "huobipro:futures:open_interest:{}_{}".format(item["Pair1"], item['Title'])
        while True:
            try:
                self.redis_connect.lpush(redis_key_name, json.dumps(item))
                logger.info(f"push: {item}")
                break
            except:
                logger.error("数据库存储失败，正在重试...")



    def main(self):
        self.send_request()


if __name__ == "__main__":
    spider = HuobiproSpider()
    spider.main()
