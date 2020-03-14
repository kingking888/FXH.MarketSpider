import requests
import time
import random
from datetime import datetime
import json
import redis
from loguru import logger
url = "https://www.okex.com/v3/futures/pc/market/longShortPositionRatio/BTC?t="+ str(int(time.time() * 1000)) +"&unitType=0"

class OkexSpider(object):
    def __init__(self):
        self.url = "https://www.okex.me/v3/futures/pc/market/longShortPositionRatio/BTC?unitType=0"
        #self.redis_connect = redis.Redis(host='122.228.200.88', port=6378, db=0, password="redis123456")
        self.redis_connect = redis.Redis(
            host='47.107.228.85',
            port=6379,
            db=0,
            password="20ab20!2#Spider!alxmH"
        )
        self.redis_key_name = "okex:futures:ratios:long_short_position_ratio_btc"

    def send_request(self):
        logger.info("程序启动!")
        while True:
            if datetime.utcnow().minute % 5 == 0:
                logger.info("正在采集数据...")
                while True:
                    try:
                        #params = {"t": str(int(time.time() * 1000)), "unitType": "0"}
                        response = requests.get(url, proxies={"https": "http://127.0.0.1:{}".format(random.randint(8081, 8323))})
                        self.parse_response(response)
                        break
                    except Exception as e:
                        logger.error(e)
                        logger.error("正在重新发送请求...")

                logger.info("采集结束，五分钟后再次采集...")
                time.sleep(62)
            time.sleep(2)



    def parse_response(self, response):
        data = response.json()['data']
        print(data)
        data = list(zip(data['timestamps'], data['ratios']))[-1:]


        for timestamps, ratios in data:
            item = {"timestamps": timestamps, "ratios": ratios}
            while True:
                try:
                    logger.info(f"push: {item}")
                    self.redis_connect.lpush(self.redis_key_name, json.dumps(item))
                    break
                except:
                    logger.error("数据库存储失败，正在重试...")


    def main(self):
        self.send_request()


if __name__ == "__main__":
    spider = OkexSpider()
    spider.main()
