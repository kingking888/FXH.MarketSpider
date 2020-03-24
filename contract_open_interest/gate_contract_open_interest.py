import requests
import time
import json
import redis
from loguru import logger
import random


# # BTC结算合约
# btc_usd_url = "https://fx-api.gateio.ws/api/v4/futures/btc/contracts/BTC_USD"
# # USDT结算合约
# btc_usdt_url = "https://fx-api.gateio.ws/api/v4/futures/usdt/contracts/BTC_USDT"
# # BTC/USDT 现货价
# btc_spot_url = "https://data.gateio.life/api2/1/ticker/btc_usdt"

class GateSpider(object):
    def __init__(self):
        self.btc_url = "https://fx-api.gateio.ws/api/v4/futures/btc/contracts"
        self.usdt_url = "https://fx-api.gateio.ws/api/v4/futures/usdt/contracts"
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

                data_list = requests.get(self.btc_url).json()
                data_list += requests.get(self.usdt_url).json()
                self.parse_data(data_list, ts)

                logger.info("采集结束，一分钟后再次采集...")
                time.sleep(20)
            except Exception as e:
                logger.error(e)
                logger.error("正在重新发送请求...")


    def parse_data(self, data_list, ts):
        for data in data_list:
            if 'BTC' in data.get("name"):
                item = {}
                item['Time'] = ts
                item['Title'] = "SWAP"
                item['Pair1'] = "BTC"
                item['Volume'] = int(data['position_size'])

                if data.get('name') == 'BTC_USD':
                    item['Pair2'] = "USD"
                    item['Usd'] = item['Volume']

                elif data.get('name') == 'BTC_USDT':
                    item['Pair2'] = "USDT"
                    item['Usd'] = int(item['Volume'] * float(data.get("quanto_multiplier")) * float(data.get("index_price")))

                redis_key_name = "gate-io:futures:open_interest:{}_{}_{}".format(item["Pair1"], item["Pair2"], item['Title'])
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
    spider = GateSpider()
    spider.main()
