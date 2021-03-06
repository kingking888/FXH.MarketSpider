
import time
import os
import redis
import gzip
import zlib
import threading
import json
import requests
import random
import gc

from websocket import create_connection
from lib.decorator import tail_call_optimized
from lib.logger import Logger
from lib.config_manager import Config

import ssl
ssl._create_default_https_context = ssl._create_unverified_context


class HuobiProTradeSpider(object):
    def __init__(self, logger, symbol, exchange, req, trade_type):
        self.logger = logger
        self.symbol = symbol
        self.exchange = exchange
        self.req = req
        self.trade_type = trade_type
        self.last_item = None
        # self.influxdb_connect = InfluxDBClient(host="122.228.200.73", port=8086, username="demo", password="fxhdemo", database="transaction")


    # 防止python 递归调用 堆栈溢出 @tail_call_optimized
    @tail_call_optimized
    def task_thread(self):
        self.logger.info('数字货币：{} {} 实时trade数据获取开始时间：{}'.format(self.symbol, self.trade_type, time.strftime("%Y-%m-%d %H:%M:%S")))

        # 反复尝试建立websocket连接
        while True:
            try:
                # 是否需要使用代理（目前huobi不需要代理）
                proxy = self.exchange.get("proxy")
                # 获取建立websocket的请求链接
                if self.trade_type == 'SWAP':
                    socket_url = self.exchange.get("socket_url_swap")
                else:
                    socket_url = self.exchange.get("socket_url")


                if proxy == "true":
                    ws = create_connection(socket_url, http_proxy_host="127.0.0.1", http_proxy_port=random.randint(8080, 8323))
                else:
                    ws = create_connection(socket_url)
                break
            except Exception as e:
                self.logger.error(e)
                self.logger.info('数字货币： {} {} connect ws error, retry...'.format(self.symbol, self.trade_type))
                time.sleep(1)


        logger.info("数字货币： {} {} connect success".format(self.symbol, self.trade_type))
        # 获取数据加密类型（gzip）
        utype = self.exchange.get("utype")

        # 发送了各币种的各k线的websocket请求
        print("req:", self.req)
        ws.send(self.req)

        # 获取数据：
        try:
            while True:
                # 设置 websocket 超时时间, 时间太久会导致 trade 一分钟没数据，因目前交易所采集稳定暂时不设置
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
                if result[:7] == '{"ping"':
                    # 则构建 pong 回复给服务器，保持连接
                    ts = result[8:21]
                    pong = '{"pong":' + ts + '}'
                    #print(pong)
                    ws.send(pong)
                    # ws.send(self.req)
                else:
                    # self.logger.info(result)
                    self.save_result_redis(result)

        except Exception as e:
            self.logger.error(e)
            self.logger.error("数字货币：{} {} 连接中断，reconnect.....".format(self.symbol, self.trade_type))
            ws.close()
            gc.collect()
            # 如果连接中断，递归调用继续
            self.task_thread()


    def save_result_redis(self, result):
        result = json.loads(result)
        if result.get("ch"):
            # ch = result.get("ch")  # market.BTC_CQ.trade.detail
            data_list = result.get("tick").get("data")
            for data in data_list:

                item = {}
                # 成交时间
                item["Time"] = data.get("ts")
                # 成交id
                item['ID'] = str(data.get("id"))
                #item["Pair1"] = self.symbol
                #item["Pair2"] = "USD"
                #item["Title"] = self.trade_type
                # 成交价
                item["Price"] = float(data.get("price"))
                # 主动成交方向 ( 0买  1卖)
                item["Direction"] = int(0 if data.get("direction") == 'buy' else 1)
                # Amount 成交量(币)，买卖双边成交量之和
                item["Amount"] = float(data.get("vol") if data.get("vol") else 0)
                # Volume 成交量(张)，买卖双边成交量之和
                item["Volume"] = float(data.get("amount") if data.get("amount") else 0)
                # print(item)

                redis_key_name = "huobipro:futures:trade:{}_{}_trade_detail".format(self.symbol, self.trade_type)
                while True:
                    try:
                        redis_connect.lpush(redis_key_name, json.dumps(item))
                        # if int(time.time()) % 5 == 0:
                        #     self.logger.info("push item")
                        # redis_connect.ltrim(redis_key_name, 0, 19999)
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
    logger = Logger.get_logger("huobipro_trade_log")
    # 获取代码目录绝对路径
    last_dir = os.path.abspath(os.path.dirname(os.getcwd()))

    # 创建 conf/common_conf/common_conf.yaml 配置对象
    common_path = '{}/conf/common_conf/common_conf.yaml'.format(last_dir)
    common_config = Config(common_path)

    # 读取redis数据库配置，并创建redis数据库连接
    redis_conf = common_config.get_value("redis")
    redis_connect = redis.Redis(**redis_conf)
    logger.info("redis初始化成功.")


    # 创建 conf/script_conf/trade_socket/heyue.yaml 配置对象
    script_path = '{}/conf/script_conf/trade_socket/huobipro.yaml'.format(last_dir)
    script_config = Config(script_path)

    # 获取所有交易所的 采集配置
    exchange = script_config.get_value("huobipro")

    # 是否需要使用代理（目前huobi不需要代理）
    proxy = exchange.get("proxy")
    pair_url = exchange.get("pair_url")
    pair_url_swap = exchange.get("pair_url_swap")

    trade_info_list = exchange.get("trade_info")
    trade_info_swap = exchange.get("trade_info_swap")


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

    if pair_url and pair_url_swap:
        # 获取当前页面币种信息，目前huobi不需要代理，其他需要代理
        if proxy == "true":
            resp = requests.get(pair_url, headers=headers, proxies=proxies).json()
            resp_swap = requests.get(pair_url_swap, headers=headers, proxies=proxies).json()

        else:
            resp = requests.get(pair_url, headers=headers).json()
            resp_swap = requests.get(pair_url_swap, headers=headers).json()

        #####################################################################
        # 获取所有合约币种信息（data 列表）
        data_list = resp.get("data")
        # 获取所有合约币种名称（BTC、ETC、ETH、EOS、LTC、BCH、XRP、TRX、BSV）
        symbol_list = list(set([data.get("symbol") for data in data_list]))
        print(symbol_list)

        # swap
        data_swap_list = resp_swap.get('data')
        symbol_swap_list = [data_swap.get("symbol") for data_swap in data_swap_list]
        print(symbol_swap_list)

        #####################################################################


        # 获取所有k线采集方案(3次)
        for trade_info in trade_info_list:
            # 迭代每个币种，并构建该币种k线 websocket请求(9次)
            for symbol in symbol_list:
                trade_type = trade_info.get("trade_type")
                trade = trade_info.get('trade')
                req = "{" + trade[1: -1].format(symbol=symbol) + "}"

                spider = HuobiProTradeSpider(logger, symbol, exchange, req, trade_type)
                t = MyThread(target=spider.task_thread, args=())
                thread_list.append(t)
                t.start()
                time.sleep(0.2)

        # swap
        for symbol_swap in symbol_swap_list:
            trade_type_swap = trade_info_swap.get("trade_type")
            trade_swap = trade_info_swap.get('trade')
            req = "{" + trade_swap[1: -1].format(symbol=symbol_swap) + "}"
            spider = HuobiProTradeSpider(logger, symbol_swap, exchange, req, trade_type_swap)
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

