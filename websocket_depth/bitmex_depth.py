
import time
import os
import redis
import gc
import threading
import json

import requests
import random

from lib.decorator import tail_call_optimized
from lib.logger import Logger
from lib.config_manager import Config

import ssl
ssl._create_default_https_context = ssl._create_unverified_context


class BitmexDepthSpider(object):
    def __init__(self, logger, symbol, exchange, req, depth_type):
        self.logger = logger
        self.symbol = symbol
        self.exchange = exchange
        self.req = req
        self.depth_type = depth_type
        self.last_item = None
        self.data = {}
        self.keys = {}

    # 防止python 递归调用 堆栈溢出 @tail_call_optimized
    @tail_call_optimized
    def task_thread(self):
        self.logger.info('数字货币：{} {} 数据获取开始时间：{}'.format(self.symbol, self.depth_type, time.strftime("%Y-%m-%d %H:%M:%S")))

        # 反复尝试建立websocket连接
        while True:
            try:
                # 是否需要使用代理（目前huobi不需要代理）
                proxy = self.exchange.get("proxy")
                # 获取建立websocket的请求链接
                socket_url = self.exchange.get("socket_url")

                if proxy == "true":
                    proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}
                    result = requests.get("https://www.bitmex.com/api/v1/orderBook/L2?symbol={}&depth=100".format(self.symbol), proxies=proxies)
                else:
                    result = requests.get("https://www.bitmex.com/api/v1/orderBook/L2?symbol={}&depth=100".format(self.symbol))

                # break
                self.save_result_redis(result)
                time.sleep(5)
            except Exception as e:
                self.logger.error(e)
                self.logger.error(result)
                self.logger.info('数字货币： {} {} connect ws error, retry...'.format(self.symbol, self.depth_type))
                gc.collect()
                time.sleep(1)


    def save_result_redis(self, result):
        try:
            data_list = result.json()
            # print(data_list)
        except Exception as e:
            self.logger.info(e)
            self.logger.info("result数据有误...")
            return

        asks_list = data_list[:100]
        bids_list = data_list[100:]

        item = {}

        item["Time"] = int(time.time() * 1000) - 200

        #item["Pair1"] = self.symbol

        # if self.symbol in ["XBT7D_U105", "XBT7D_D95", "ETHH20", "LTCH20", "XRPH20", "BCHH20", "ADAH20", "EOSH20", "TRXH20"]:
        #     item["Pair2"] = "XBT"
        # elif self.symbol in ["XBTUSD", "ETHUSD",  "XBTH20", "XBTM20"]:
        #     item["Pair2"] = "USD"
        # else:
        #     raise ValueError("find new symbol: {}".format(symbol))
        #if self.symbol in ["XBTUSD", "ETHUSD",  "XBTH20", "XBTM20", "XRPUSD"]:
        #    item["Pair2"] = "USD"
        #else:
        #    item["Pair2"] = "XBT"

        #item["Title"] = self.depth_type
        item["Sells"] = [[asks['price'], asks['size']] for asks in asks_list][::-1]  # 按价格升序[6, 7, 8, 9, 10]
        item["Buys"] = [[bids['price'], bids['size']] for bids in bids_list]  # 按价格降序[5, 4, 3, 2, 1]

        redis_key_name = "bitmex:futures:depth:{}_{}_depth_100".format(self.symbol, self.depth_type)

        while True:
            try:
                redis_connect.lpush(redis_key_name, json.dumps(item))
                # if int(time.time()) % 5 == 0:
                #     self.logger.info("push item")
                redis_connect.ltrim(redis_key_name, 0, 19999)
                break
            except Exception as e:
                self.logger.error(e)

    def start(self):
        while True:
            if self.task_thread():
                break

class MyThread(threading.Thread):
    def __init__(self, target, args):
        super().__init__()
        self.target = target
        self.args = args

    def run(self):
        self.target(*self.args)


if __name__ == "__main__":
    # k线 logger日志
    logger = Logger.get_logger("bitmex_depth_log")
    # 获取代码目录绝对路径
    last_dir = os.path.abspath(os.path.dirname(os.getcwd()))

    # 创建 conf/common_conf/common_conf.yaml 配置对象
    common_path = '{}/conf/common_conf/common_conf.yaml'.format(last_dir)
    common_config = Config(common_path)

    # 读取redis数据库配置，并创建redis数据库连接
    redis_conf = common_config.get_value("redis")
    redis_connect = redis.Redis(**redis_conf)
    logger.info("redis初始化成功.")

    # 创建 conf/script_conf/depth_socket/heyue.yaml 配置对象
    script_path = '{}/conf/script_conf/depth_socket/bitmex.yaml'.format(last_dir)
    script_config = Config(script_path)

    # 获取所有交易所的 采集配置
    exchange = script_config.get_value("bitmex")

    # 是否需要使用代理（目前huobi不需要代理）
    proxy = exchange.get("proxy")
    pair_url = exchange.get("pair_url")
    depth_info_list = exchange.get("depth_info")
    symbol_dict = exchange.get("symbol_dict")

    # 代理和requests报头
    proxies = {
        "https": "https://127.0.0.1:1080",
    }
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36',
        'source': 'web'
    }

    # 子线程组
    thread_list = []

    # 获取所有k线采集方案(1次)
    for depth_info in depth_info_list:
        for symbol in symbol_dict:
            depth = depth_info.get('depth')
            req = "{" + depth[1: -1].format(symbol=symbol) + "}"
            depth_type = symbol_dict[symbol]
            spider = BitmexDepthSpider(logger, symbol, exchange, req, depth_type)
            t = MyThread(target=spider.start, args=())
            thread_list.append(t)
            t.start()
            time.sleep(0.2)
    time.sleep(1)

    while True:
        length = len(threading.enumerate())
        logger.info('当前运行的线程数为：%d' % length)
        time.sleep(10)
        if length <= 1:
            break

    # 主线程等待子线程执行完毕
    for t in thread_list:
        t.join

