import requests
import time
import random
import json
import redis
from loguru import logger


class OkexSpider(object):
    def __init__(self):

        # 现货:
        self.btc_spot_url = "https://www.okex.com/api/spot/v3/instruments/BTC-USDT/trades?limit=1"
        # 交割合约:
        # btc_cw_url = "https://www.okex.com/api/futures/v3/instruments/BTC-USD-200214/trades?limit=1"
        self.btc_futures_url = "https://www.okex.com/api/futures/v3/instruments/{}/trades?limit=1"
        # 永续合约
        self.btc_swap_url = "https://www.okex.com/api/swap/v3/instruments/{}/trades?limit=1"
        self.redis_connect = redis.Redis(host="47.107.228.85", port=6379,
                                         password="20ab20!2#Spider!alxmH")

        # 是否使用代理（如果为True，则代理按 127.0.0.1:8080-8232；如果为False，则不使用代理）
        self.proxies = True

        self.item_list = []
        self.last_item_list = []

    def send_request(self):
        while True:
            #if int(time.time()) % 60 > 20 or self.first_run:
            try:
                self.get_instruments()
                if self.item_list != self.last_item_list:
                    logger.info("获取新的合约列表: {}".format(self.item_list))
                    self.last_item_list = self.item_list

                btc_spot_price = float(requests.get(self.btc_spot_url, proxies=self.proxy).json()[0]['price'])

                for item in self.item_list:
                    if item['timeid'] == 'CW':
                        url = self.btc_futures_url.format(item['symbol'])
                        btc_cw_price = float(requests.get(url, proxies=self.proxy).json()[0]['price'])

                    elif item['timeid'] == 'NW':
                        url = self.btc_futures_url.format(item['symbol'])
                        btc_nw_price = float(requests.get(url, proxies=self.proxy).json()[0]['price'])

                    elif item['timeid'] == 'CQ':
                        url = self.btc_futures_url.format(item['symbol'])
                        btc_cq_price = float(requests.get(url, proxies=self.proxy).json()[0]['price'])

                    elif item['timeid'] == 'NQ':
                        url = self.btc_futures_url.format(item['symbol'])
                        btc_nq_price = float(requests.get(url, proxies=self.proxy).json()[0]['price'])

                    elif item['timeid'] == 'SWAP':
                        url = self.btc_swap_url.format(item['symbol'])
                        btc_swap_price = float(requests.get(url, proxies=self.proxy).json()[0]['price'])

                data = {
                    "Time": int(time.time() * 1000),
                    "CW": float("%.2f" % (btc_cw_price - btc_spot_price)),
                    "NW": float("%.2f" % (btc_nw_price - btc_spot_price)),
                    "CQ": float("%.2f" % (btc_cq_price - btc_spot_price)),
                    "NQ": float("%.2f" % (btc_nq_price - btc_spot_price)),
                    "SWAP": float("%.2f" % (btc_swap_price - btc_spot_price)),
                    "Price": {
                        "SPOT": btc_spot_price,
                        "CW": btc_cw_price,
                        "NW": btc_nw_price,
                        "CQ": btc_cq_price,
                        "NQ": btc_nq_price,
                        "SWAP": btc_swap_price
                    }
                }

                while True:
                    try:
                        self.redis_connect.set("okex:btc:usd:difference_in_price", json.dumps(data))
                        logger.info("Push: {}".format(data))
                        break
                    except:
                        self.redis_connect = redis.Redis(host="47.107.228.85", port=6379, password="20ab20!2#Spider!alxmH")

            except Exception as e:
                logger.error(e)



    def get_instruments(self):
        futures_list = requests.get("https://www.okex.com/api/futures/v3/instruments", proxies=self.proxy).json()
        futures_list += requests.get("https://www.okex.com/api/swap/v3/instruments", proxies=self.proxy).json()

        for futures in futures_list:
            # if futures['quote_currency'] == 'USD':
            if futures['base_currency'] == 'BTC':
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
                    item['timeid'] = "SWAP"

                #print(item)
                self.item_list.append(item)
    @property
    def proxy(self):
        if self.proxies:
            return {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8232))}
        else:
            return None

if __name__ == "__main__":
    spider = OkexSpider()
    spider.send_request()
