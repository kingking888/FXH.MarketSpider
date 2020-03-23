
import time
import os
import redis
import zlib
import threading
import json
import requests
import gc
import random

from websocket import create_connection
from lib.decorator import tail_call_optimized
from lib.logger import Logger
from lib.config_manager import Config

import ssl
ssl._create_default_https_context = ssl._create_unverified_context


class OkexKlineSpider(object):
    def __init__(self, logger, symbol, exchange, req, kline_type, pair1, pair2):
        self.logger = logger
        self.symbol = symbol
        self.exchange = exchange
        self.req = req
        self.kline_type = kline_type
        self.pair1 = pair1
        self.pair2 = pair2
        self.last_item = None
        self.count = 0

    @staticmethod
    def deflate_decode(result):
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        inflated = decompress.decompress(result)
        inflated += decompress.flush()
        result = inflated.decode("utf-8")
        return result

    # 防止python 递归调用 堆栈溢出 @tail_call_optimized
    @tail_call_optimized
    def task_thread(self):
        self.logger.info('数字货币：{} {} 数据获取开始时间：{}'.format(self.pair1, self.pair2, time.strftime("%Y-%m-%d %H:%M:%S")))

        # 反复尝试建立websocket连接
        while True:
            try:
                # 是否需要使用代理（目前huobi不需要代理）
                # proxy = self.exchange.get("proxy")
                # 获取建立websocket的请求链接
                # socket_url = self.exchange.get("socket_url")

                if self.exchange.get("proxy") == "true":
                    ws = create_connection(self.exchange.get("socket_url"), http_proxy_host="127.0.0.1", http_proxy_port=random.randint(8080, 8323))
                else:
                    ws = create_connection(self.exchange.get("socket_url"))
                break
            except Exception as e:
                self.logger.error(e)
                self.logger.error('数字货币： {} {} connect ws error, retry...'.format(self.symbol, self.kline_type))
                time.sleep(1)

        logger.info("数字货币： {} {} connect success".format(self.symbol, self.kline_type))
        # 获取数据加密类型（gzip）
        utype = self.exchange.get("utype")

        # 发送了各币种的各k线的websocket请求
        self.logger.info("当前采集方案: {}".format(self.req))
        ws.send(self.req)

        # 获取数据：
        try:
            while True:
                data = ws.recv()
                if data != '':
                    result = self.deflate_decode(data)
                    if result != 'pong':
                        self.save_result_redis(result)
                    else:
                        time.sleep(0.1)
                ws.send("ping")

        except Exception as e:
            self.logger.error(e)
            self.logger.error(result)
            self.logger.error("数字货币：{} {} 连接中断，reconnect.....".format(self.symbol, self.kline_type))
            ws.close()
            gc.collect()
            # 如果连接中断，递归调用继续
            self.task_thread()


    def save_result_redis(self, result):
        result = json.loads(result)
        if result.get("data"):
            tick = result.get("data")[0].get("candle")

            item = {}
            utc_time = tick[0].replace("T", " ").replace(".000Z", "")
            struct_time = time.strptime(utc_time, "%Y-%m-%d %H:%M:%S")
            item["Time"] = int(time.mktime(struct_time)) + 28800
            #item["Pair1"] = self.pair1
            #item["Pair2"] = self.pair2
            #item["Title"] = self.kline_type
            item["Open"] = eval(tick[1])
            item["Close"] = eval(tick[4])
            item["High"] = eval(tick[2])
            item["Low"] = eval(tick[3])
            item["Amount"] = eval(tick[5])
            item["Volume"] = float((item["Open"] + item["Close"] + item["High"] + item["Low"]) / 4 * item["Amount"])
            # print(item)
            redis_key_name = "okex:spot:kline:{}_{}_1min_kline".format(self.pair1, self.pair2)
            redis_key_name2 = "yuanhao:okex:spot:kline:{}_{}_1min_kline".format(self.pair1, self.pair2)
            # now_time = int(time.time() / 60) * 60

            if self.last_item is None:
                self.last_item = item

            if item["Time"] == self.last_item["Time"]:
                #print("----Same time, save new item: ", item)
                self.last_item = item

            elif item["Time"] > self.last_item["Time"]:
                #print("--------Different time, push last item and new item: ")
                while True:
                    try:
                        redis_connect.lpush(redis_key_name, json.dumps(self.last_item))
                        redis_connect.lpush(redis_key_name2, json.dumps(self.last_item))
                        self.logger.info("push item: {}_{} {}".format(self.pair1, self.pair2, self.last_item))
                        self.last_item = item
                        break
                    except Exception as e:
                        self.logger.error(e)
            else:
                redis_connect.lpop(redis_key_name)
                redis_connect.lpop(redis_key_name2)
                while True:
                    try:
                        redis_connect.lpush(redis_key_name, json.dumps(item))
                        redis_connect.lpush(redis_key_name2, json.dumps(item))
                        self.logger.info("update item: {}_{} {}".format(self.pair1, self.pair2, self.item))
                        break
                    except Exception as e:
                        self.logger.error("Push Error: {}".format(e))


class MyThread(threading.Thread):
    def __init__(self, target, args):
        super().__init__()
        self.target = target
        self.args = args

    def run(self):
        self.target(*self.args)


if __name__ == "__main__":
    # k线 logger日志
    logger = Logger.get_logger("okex_kline_log")
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
    script_path = '{}/conf/script_conf/kline_socket/okex.yaml'.format(last_dir)
    script_config = Config(script_path)

    # 获取所有交易所的 采集配置
    exchange = script_config.get_value("okex")

    # 是否需要使用代理（目前huobi不需要代理）
    proxy = exchange.get("proxy")
    pair_url_spot = exchange.get("pair_url_spot")
    kline_info_spot = exchange.get("kline_info_spot")

    # 代理和requests报头
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36',
        'source': 'web'
    }

    while True:
        # 子线程组
        thread_list = []

        if pair_url_spot:
            while True:
                try:
                    # 获取当前页面币种信息，目前huobi不需要代理，其他需要代理
                    if proxy == "true":
                        resp = requests.get(pair_url_spot, headers=headers, proxies={"https": "https://127.0.0.1:{}".format(random.randint(8080, 8323))}).json()
                    else:
                        resp = requests.get(pair_url_spot, headers=headers).json()
                    break
                except:
                    pass

            #####################################################################
            # 获取所有币种信息（data 列表）
            data_list = resp
            # print(data_list)
            # 获取所有合约币种名称（BTC、ETC、ETH、EOS、LTC、BCH、XRP、TRX、BSV）
            # symbol_list = [[data.get("symbol"), data.get("base-currency"), data.get("quote-currency")] for data in data_list]
            symbol_list = [[data.get("instrument_id"), data.get("base_currency"), data.get("quote_currency")] for data in
                           data_list if data.get("base_currency") == "BTC"]

            print(symbol_list)
            print(len(symbol_list))
            #####################################################################

            # 获取所有k线采集方案(3次)
            for kline_info in kline_info_spot:
                # 迭代每个币种，并构建该币种k线 websocket请求(9次)
                for index, symbol in enumerate(symbol_list):
                    kline_type = kline_info.get("kline_type")
                    kline = kline_info.get('kline')

                    req = "{" + kline[1: -1].format(symbol=symbol[0]) + "}"
                    pair1 = symbol[1].upper()
                    pair2 = symbol[2].upper()
                    spider = OkexKlineSpider(logger, symbol[0], exchange, req, kline_type, pair1, pair2)
                    t = MyThread(target=spider.task_thread, args=())
                    thread_list.append(t)
                    t.start()
                    time.sleep(0.2)

        while True:
            length = len(threading.enumerate())
            logger.info('当前运行的线程数为：%d' % length)
            time.sleep(10)
            if length <= 1:
                break

        # 主线程等待子线程执行完毕
        for t in thread_list:
            t.join

