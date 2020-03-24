import requests
import time
import json
import redis
from loguru import logger
import random


class BitmexSpider(object):
    def __init__(self):
        self.url = "https://www.bitmex.com/api/v1/instrument?symbol="
        self.info_url = "https://www.bitmex.com/api/v1/instrument/activeIntervals"
        # self.redis_connect = redis.Redis(host='122.228.200.88', port=6378, db=0, password="redis123456")
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

                instrument_info = requests.get(self.info_url, proxies={"https": "http://127.0.0.1:{}".format(random.randint(8081, 8323))}).json()
                # now_symbol_list = [symbol for symbol in symbol_list if "XBT" in symbol]
                xbt_instrument_info = {symbol: interval for symbol, interval in zip(instrument_info.get("symbols"), instrument_info.get("intervals")) if "XBT" in symbol}
                print(xbt_instrument_info)

                for symbol in xbt_instrument_info:
                    response = requests.get(self.url + symbol, proxies={"https": "http://127.0.0.1:{}".format(random.randint(8081, 8323))})
                    self.parse_response(response, ts, xbt_instrument_info, symbol)

                logger.info("采集结束，一分钟后再次采集...")
                time.sleep(20)

            except Exception as e:
                logger.error(e)
                logger.error("正在重新发送请求...")

    def parse_response(self, response, ts, xbt_instrument_info, symbol):
        data = response.json()[0]
        interval = xbt_instrument_info[symbol]
        item = {}
        item['Time'] = ts
        if "perpetual" in interval:
            item['Title'] = "SWAP"
        elif "biquarterly" in interval:
            item['Title'] = "NQ"
        #elif "biquarterly" in interval:
        else:
            item['Title'] = "CQ"

        item['Pair1'] = "XBT"
        item['Pair2'] = 'USD'
        item['Volume'] = data['openInterest']
        item['Usd'] = data['openInterest']

        redis_key_name = "bitmex:futures:open_interest:{}USD_{}".format(item["Pair1"], item['Title'])
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
    spider = BitmexSpider()
    spider.main()
