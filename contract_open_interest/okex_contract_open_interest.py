import requests
import time
import json
import redis
from loguru import logger
import random


class OkexSpider(object):
    def __init__(self):
        self.swap_url = "https://www.okex.com/api/swap/v3/instruments/{}/open_interest"
        self.futures_url = "https://www.okex.com/api/futures/v3/instruments/{}/open_interest"
        self.futures_info_url = "https://www.okex.com/api/futures/v3/instruments"
        self.swap_info_url = "https://www.okex.com/api/swap/v3/instruments"
        # self.redis_connect = redis.Redis(host='122.228.200.88', port=6378, db=0, password="redis123456")
        self.redis_connect = redis.Redis(
            host='47.107.228.85',
            port=6379,
            db=0,
            password="20ab20!2#Spider!alxmH"
        )

    def send_request(self):
        while True:
            ts = int(time.time())
            if ts % 60 != 0:
                time.sleep(0.9)
                continue
            data_list = requests.get("https://www.okex.com/api/futures/v3/instruments").json()
            data_list += requests.get("https://www.okex.com/api/swap/v3/instruments").json()

            item_list = []
            for data in data_list:
                if data['is_inverse'] == "true" or data['is_inverse'] is True:
                    item = {}
                    item['symbol'] = data['instrument_id']
                    item['quote'] = data['quote_currency']
                    if data.get('alias'):
                        timeid = data.get('alias')
                        if timeid == 'this_week':
                            item['timeid'] = 'CW'
                        elif timeid == 'next_week':
                            item['timeid'] = 'NW'
                        elif timeid == 'quarter':
                            item['timeid'] = 'CQ'
                        else:
                            item['timeid'] = 'NQ'
                    else:
                        item['timeid'] = 'SWAP'
                    item_list.append(item)

            data_list = [item for item in item_list if item['symbol'].split("-")[0] == "BTC"]
            print(data_list)

            for data in data_list:
                if data['symbol'].split("-")[0] == "BTC":
                    if data['timeid'] == 'SWAP':
                        url = self.swap_url.format(data['symbol'])
                    else:
                        url = self.futures_url.format(data['symbol'])

                    while True:
                        try:
                            response = requests.get(url, proxies={"https": "http://127.0.0.1:{}".format(random.randint(8081, 8323))})
                            self.parse_response(response, data['timeid'], ts)
                            break
                        except Exception as e:
                            logger.error(e)
                            logger.error("正在重新发送请求...")


            logger.info("采集结束，一分钟后再次采集...")
            time.sleep(20)

    def parse_response(self, response, timeid, ts):
        data = response.json()
        item = {}
        #utc_time = data["timestamp"].replace("T", " ")[:-5]
        #struct_time = time.strptime(utc_time, "%Y-%m-%d %H:%M:%S")

        item["Time"] = ts
        item['Title'] = timeid
        item['Pair1'] = data['instrument_id'].split("-")[0]
        item['Pair2'] = data['instrument_id'].split("-")[1]
        item['Volume'] = int(data['amount'])
        item['Usd'] = int(data['amount']) * 100  # BTC 合约每张价值 100 USD

        redis_key_name = "okex:futures:open_interest:{}_{}".format(item["Pair1"], item['Title'])
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
    spider = OkexSpider()
    spider.main()
