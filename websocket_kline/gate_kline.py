
import time
import os
import redis
import gzip
import zlib
import threading
import json
import requests
import random
import copy

from websocket import create_connection
from lib.decorator import tail_call_optimized
from lib.logger import Logger
from lib.config_manager import Config

import ssl
ssl._create_default_https_context = ssl._create_unverified_context


class GateKlineSpider(object):
    def __init__(self, logger, symbol, exchange, req, kline_type):
        self.logger = logger
        self.symbol = symbol
        self.exchange = exchange
        self.req = req
        self.kline_type = kline_type
        self.last_item = None
        self.last_realtime = None

    # 防止python 递归调用 堆栈溢出 @tail_call_optimized
    @tail_call_optimized
    def task_thread(self):
        self.logger.info('数字货币：{} {} 数据获取开始时间：{}'.format(self.symbol, self.kline_type, time.strftime("%Y-%m-%d %H:%M:%S")))

        # 反复尝试建立websocket连接
        while True:
            try:
                # 是否需要使用代理（目前huobi不需要代理）
                proxy = self.exchange.get("proxy")
                # 获取建立websocket的请求链接
                # 区分 USDT结算合约 和 BTC结算合约
                socket_url = self.exchange.get("socket_url_usdt") if "USDT" in self.symbol else self.exchange.get("socket_url_btc")

                if proxy == "true":
                    ws = create_connection(socket_url, http_proxy_host="127.0.0.1", http_proxy_port=random.randint(8080, 8323))
                else:
                    ws = create_connection(socket_url)
                break
            except Exception as e:
                self.logger.error(e)
                self.logger.info('数字货币： {} {} connect ws error, retry...'.format(self.symbol, self.kline_type))
                time.sleep(1)


        logger.info("数字货币： {} {} connect success".format(self.symbol, self.kline_type))
        # 获取数据加密类型（gzip）
        utype = self.exchange.get("utype")

        # 发送了各币种的各k线的websocket请求
        print("req:", self.req)
        ws.send(self.req)

        # 获取数据：
        try:
            while True:
                # 设置 websocket 超时时间, 时间太久会导致 kline 一分钟没数据，因目前交易所采集稳定暂时不设置
                # ws.settimeout(30)
                # 接收websocket响应
                result = ws.recv()
                # 加密方式 gzip
                if utype == 'gzip':
                    try:
                        result = gzip.decompress(result).decode('utf-8')
                    except:
                        pass
                # 加密方式 deflate
                elif utype == "deflate":
                    decompress = zlib.decompressobj(-zlib.MAX_WBITS)
                    inflated = decompress.decompress(result)
                    inflated += decompress.flush()
                    result = inflated.decode()
                # 加密方式 未加密
                elif utype == "string":
                    pass

                # 如果websocket响应是 ping
                if result == "ping":
                    # 则构建 pong 回复给服务器，保持连接
                    ws.send("pong")
                else:
                    # self.logger.info(result)
                    self.save_result_redis(result)

        except Exception as e:
            logger.error(e)
            logger.error(result)
            logger.error("数字货币：{} {} 连接中断，reconnect.....".format(self.symbol, self.kline_type))
            # 如果连接中断，递归调用继续
            self.task_thread()


    def save_result_redis(self, result):
        result = json.loads(result)

        if result['event'] == 'update':
            # [{'t': 1584859920, 'v': 0, 'c': '6344.4', 'h': '6344.4', 'l': '6344.4', 'o': '6344.4', 'n': '1m_BTC_USD'}]}
            info_list = result['result']

            for info in info_list:
                item = {}
                item["Time"] = info.get("t")
                #item["Pair1"] = self.symbol
                #item["Pair2"] = "USD"
                #item["Title"] = self.kline_type
                item["Open"] = info.get("o")
                item["Close"] = info.get("c")
                item["High"] = info.get("h")
                item["Low"] = info.get("l")
                item["Amount"] = 0
                item["Volume"] = info.get("v")
                # print(item)

                realtime_item = copy.copy(item)
                # -------------- realtime
                redis_key_name_realtime = "gate-io:futures:rtkline:{}_{}_{}_realtime_kline".format(self.symbol, self.coin, self.kline_type)

                realtime_item = item
                realtime_item['time'] = int(time.time() * 1000)
                if self.last_realtime is None:
                    self.last_realtime = realtime_item

                if realtime_item['time'] - self.last_realtime['time'] > 1000:
                    redis_connect.lpush(redis_key_name_realtime, json.dumps(realtime_item))
                    try:
                        redis_connect.ltrim(redis_key_name_realtime, 0, 299)
                    except:
                        pass
                    self.last_realtime = realtime_item
                del realtime_item

                # -------------- 1min time
                redis_key_name = "gate-io:futures:kline:{}_{}_1min_kline".format(self.symbol, self.kline_type)
                # now_time = int(time.time() / 60) * 60

                if self.last_item is None:
                    self.last_item = item

                if item["Time"] == self.last_item["Time"]:
                    # print("----Same time, save new item: ", item)
                    self.last_item = item
                elif item["Time"] > self.last_item["Time"]:
                    while True:
                        try:
                            redis_connect.lpush(redis_key_name, json.dumps(self.last_item))
                            # redis_connect.ltrim(redis_key_name, 0, 19999)
                            self.logger.info("push item: {} {}".format(self.symbol, self.last_item))
                            self.last_item = item
                            break
                        except Exception as e:
                            self.logger.error("Push Error: {}".format(e))
                else:
                    redis_connect.lpop(redis_key_name)
                    while True:
                        try:
                            redis_connect.lpush(redis_key_name, json.dumps(item))
                            self.logger.info("update item: {} {}".format(self.symbol, item))
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
    logger = Logger.get_logger("gate_kline_log")
    # 获取代码目录绝对路径
    last_dir = os.path.abspath(os.path.dirname(os.getcwd()))
    print(last_dir)

    # 创建 conf/common_conf/common_conf.yaml 配置对象
    common_path = '{}/conf/common_conf/common_conf.yaml'.format(last_dir)
    common_config = Config(common_path)

    # 读取redis数据库配置，并创建redis数据库连接
    redis_conf = common_config.get_value("redis")
    redis_connect = redis.Redis(**redis_conf)
    logger.info("redis初始化成功.")

    # 创建 conf/script_conf/kline_socket/heyue.yaml 配置对象
    script_path = '{}/conf/script_conf/kline_socket/gate.yaml'.format(last_dir)
    script_config = Config(script_path)

    # 获取所有交易所的 采集配置
    exchange = script_config.get_value("gate")

    # 是否需要使用代理（目前huobi不需要代理）
    proxy = exchange.get("proxy")
    pair_url_list = exchange.get("pair_url_list")
    kline_info = exchange.get("kline_info")

    # 代理和requests报头
    proxies = {
        "https": "https://127.0.0.1:8080",
    }
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36',
        'source': 'web'
    }

    # 子线程组
    thread_list = []

    if pair_url_list:
        resp_list = []
        # 获取当前页面币种信息，目前huobi不需要代理，其他需要代理
        for pair_url in pair_url_list:
            # if proxy == "true":
            #     resp_list += requests.get(pair_url, headers=headers, proxies=proxies).json()
            # else:
            #     resp_list += requests.get(pair_url, headers=headers).json()
            resp_list += requests.get(pair_url, headers=headers).json()

        #####################################################################
        # 获取所有合约币种信息（data 列表）
        symbol_list = [resp['name'] for resp in resp_list]
        print(symbol_list)
        #####################################################################

        # 获取所有k线采集方案(3次)
        #for kline_info in kline_info_list:
        # 迭代每个币种，并构建该币种k线 websocket请求(9次)
        for symbol in symbol_list:
            kline_type = kline_info.get('kline_type')
            kline = kline_info.get('kline')
            req = "{" + kline[1: -1].format(symbol=symbol) + "}"
            spider = GateKlineSpider(logger, symbol, exchange, req, kline_type)
            t = MyThread(target=spider.task_thread, args=())
            thread_list.append(t)
            t.start()
            time.sleep(0.2)
        time.sleep(1)

    while True:
        length = len(threading.enumerate())
        # print("#" * 100)
        logger.info('当前运行的线程数为：%d' % length)
        # print("#" * 100)
        time.sleep(10)
        if length <= 1:
            break

    # 主线程等待子线程执行完毕
    for t in thread_list:
        t.join

