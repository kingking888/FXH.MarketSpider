import requests
import time
import json
import redis
from loguru import logger
import random


class BinanceSpider(object):
    def __init__(self):
        #https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT
        self.url = "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"
        # self.info_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        #self.redis_connect = redis.Redis(host='122.228.200.88', port=6378, db=0, password="redis123456")
        self.redis_connect = redis.Redis(
            host='47.107.228.85',
            port=6379,
            db=0,
            password="20ab20!2#Spider!alxmH"
        )

    def send_request(self):

        while True:
            try:
                ts = int(time.time())
                if ts % 60 != 0:
                    time.sleep(0.9)
                    continue

                response = requests.get(self.url, proxies={"https": "http://127.0.0.1:{}".format(random.randint(8081, 8323))})
                price = requests.get("https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT").json()["price"]
                self.parse_response(response, price, ts)

                logger.info("采集结束，一分钟后再次采集...")
                time.sleep(20)

            except Exception as e:
                logger.error(e)
                logger.error("正在重新发送请求...")


    def parse_response(self, response, price, ts):
        data = response.json()
        item = {}
        item['Time'] = ts
        item['Title'] = "SWAP"

        item['Pair1'] = "BTC"
        item['Pair2'] = 'USDT'
        item['Volume'] = float(data['openInterest'])

        item['Usd'] = item['Volume'] * float(price)

        redis_key_name = "binance:futures:open_interest:{}_{}".format(item["Pair1"], item['Title'])
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
    spider = BinanceSpider()
    spider.main()
