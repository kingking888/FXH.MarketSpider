import time
import os
import redis
import gzip
import zlib
import threading
import json
import requests
import random

from websocket import create_connection
from lib.decorator import tail_call_optimized
from lib.logger import Logger
from lib.config_manager import Config


class CoinbaseKlineSpider(object):
    def __init__(self, logger, symbol, exchange, req, kline_type, pair1, pair2):
        self.logger = logger
        self.symbol = symbol
        self.exchange = exchange
        self.req = req
        self.kline_type = kline_type
        self.pair1 = pair1
        self.pair2 = pair2

    # 防止python 递归调用 堆栈溢出 @tail_call_optimized
    @tail_call_optimized
    def task_thread(self):
        self.logger.info('数字货币：{} {} 数据获取开始时间：{}'.format(self.pair1, self.pair2, time.strftime("%Y-%m-%d %H:%M:%S")))

        print(self.req)
        # 反复尝试建立websocket连接
        while True:
            utc_time = int(time.time()) % 60
            if utc_time == 0:
                time.sleep(3)
                try:
                    # 是否需要使用代理（目前huobi不需要代理）
                    proxy = self.exchange.get("proxy")
                    # 获取建立websocket的请求链接
                    # socket_url = self.exchange.get("socket_url")

                    if proxy == "true":
                        proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}
                        result = requests.get(self.req, proxies=proxies)

                    else:
                        result = requests.get(self.req)

                    self.save_result_redis(result)
                except Exception as e:
                    self.logger.error(e)
                    self.logger.info('数字货币： {} {} connect ws error, retry...'.format(self.pair1, self.pair2))
            else:
                time.sleep(0.2)


    def save_result_redis(self, result):
        data_list = result.json()
        tick = data_list[0]
        item = {}
        item["Time"] = tick[0]

        #item["Pair1"] = self.pair1
        #item["Pair2"] = self.pair2
        #item["Title"] = self.kline_type
        item["Open"] = tick[3]
        item["Close"] = tick[4]
        item["High"] = tick[2]
        item["Low"] = tick[1]
        item["Amount"] = tick[5]
        item["Volume"] = (item["Open"] + item["Close"] + item["High"] + item["Low"]) / 4 * item["Amount"]
        # print(item)

        redis_key_name = "coinbase:spot:kline:{}_{}_1min_kline".format(self.pair1, self.pair2)
        redis_key_name2 = "yuanhao:coinbase:spot:kline:{}_{}_1min_kline".format(self.pair1, self.pair2)

        # now_time = int(time.time() / 60) * 60
        while True:
            try:
                redis_connect.lpush(redis_key_name, json.dumps(item))
                redis_connect.lpush(redis_key_name2, json.dumps(item))
                self.logger.info("push item: {}_{} {}".format(self.pair1, self.pair2, self.last_item))
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
    logger = Logger.get_logger("coinbase_kline_log")
    # 获取代码目录绝对路径
    last_dir = os.path.abspath(os.path.dirname(os.getcwd()))

    # 创建 conf/common_conf/common_conf.yaml 配置对象
    common_path = '{}/conf/common_conf/common_conf.yaml'.format(last_dir)
    common_config = Config(common_path)

    # 读取redis数据库配置，并创建redis数据库连接
    redis_conf = common_config.get_value("redis")
    redis_connect = redis.Redis(**redis_conf)
    logger.info("redis初始化成功.")

    # 创建 conf/script_conf/kline_socket/heyue.yaml 配置对象
    script_path = '{}/conf/script_conf/kline_socket/coinbase.yaml'.format(last_dir)
    script_config = Config(script_path)

    # 获取所有交易所的 采集配置
    exchange = script_config.get_value("coinbase")

    # 是否需要使用代理（目前huobi不需要代理）
    proxy = exchange.get("proxy")
    pair_url = exchange.get("pair_url")
    kline_info_spot = exchange.get("kline_info_spot")
    url = exchange.get("socket_url_spot")

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
    for kline_info in kline_info_spot:
        kline = kline_info.get('kline')
        kline_type = kline_info.get('kline_type')
        symbol = kline_info.get('symbol')
        req = url.format(kline)
        pair1 = "BTC"
        pair2 = symbol
        spider = CoinbaseKlineSpider(logger, symbol, exchange, req, kline_type, pair1, pair2)
        t = MyThread(target=spider.start, args=())
        thread_list.append(t)
        t.start()
        time.sleep(1)
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

