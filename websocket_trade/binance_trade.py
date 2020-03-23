
import time
import os
import redis
import gzip
import zlib
import threading
import json
import random
import requests
import gc

from websocket import create_connection
from lib.decorator import tail_call_optimized
from lib.logger import Logger
from lib.config_manager import Config

import ssl
ssl._create_default_https_context = ssl._create_unverified_context


class BinanceTradeDetail(object):
    def __init__(self, logger, symbol, exchange, req, trade_type):
        self.logger = logger
        self.symbol = symbol
        self.exchange = exchange
        self.req = req
        self.trade_type = trade_type
        self.last_item = None

    # 防止python 递归调用 堆栈溢出 @tail_call_optimized
    @tail_call_optimized
    def task_thread(self):
        self.logger.info('数字货币：{} {} 数据获取开始时间：{}'.format(self.symbol, self.trade_type, time.strftime("%Y-%m-%d %H:%M:%S")))

        # 反复尝试建立websocket连接
        while True:
            try:
                # 是否需要使用代理（目前huobi不需要代理）
                proxy = self.exchange.get("proxy")
                # 获取建立websocket的请求链接
                socket_url = self.exchange.get("socket_url").format(self.req)
                print(socket_url)

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
        # print("req:", self.req)
        # ws.send(self.req)

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
                    ws.send(pong)
                    # ws.send(tradeStr_trade)
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
        if result.get("data"):
            tick = result.get("data")
            item = {}
            # binance 数据是utc+8时间
            item["Time"] = tick.get("T")
            item['ID'] = str(tick.get("a"))
            #item["Pair1"] = self.symbol
            #item["Pair2"] = "USDT"
            #item["Title"] = self.trade_type
            # price: 成交价
            item["Price"] = float(tick.get("p"))
            # direction: 主动成交方向 (0买， 1卖)
            item["Direction"] = int(1 if tick.get("m") else 0)
            item["Amount"] = float(tick.get("q"))
            # volume: 成交量(张)，买卖双边成交量之和
            item["Volume"] = float(0)
            # print(item)

            redis_key_name = "binance:futures:trade:{}_{}_trade_detail".format(self.symbol, self.trade_type)
            while True:
                try:
                    redis_connect.lpush(redis_key_name, json.dumps(item))
                    # self.last_item = item
                    # if int(time.time()) % 5 == 0:
                    #     self.logger.info("push item")
                    redis_connect.ltrim(redis_key_name, 0, 19999)
                    break
                except Exception as e:
                    self.logger.error(e)


class MyThread(threading.Thread):
    def __init__(self, target, args):
        super().__init__()
        self.target = target
        self.args = args

    def run(self):
        self.target(*self.args)


if __name__ == "__main__":
    # k线 logger日志
    logger = Logger.get_logger("binance_trade_log")
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
    script_path = '{}/conf/script_conf/trade_socket/binance.yaml'.format(last_dir)
    script_config = Config(script_path)

    # 获取所有交易所的 采集配置
    exchange = script_config.get_value("binance")

    # 是否需要使用代理（目前huobi不需要代理）
    proxy = exchange.get("proxy")
    pair_url = exchange.get("pair_url")
    # trade_info_list = exchange.get("trade_info")
    
    if proxy == 'true':
        symbol_list = requests.get(pair_url, proxies={"https":"http://127.0.0.1:8300"}).json().get("symbols")
    else:
        symbol_list = requests.get(pair_url).json().get("symbols")


    trade_info_list = []

    for symbol in symbol_list:
        coin = symbol.get("symbol")
        depth = {"trade" : symbol.get("symbol").lower(), "trade_type" :"SWAP", "symbol": coin[:-4]}
        trade_info_list.append(depth)
    print(trade_info_list)
    print(len(trade_info_list))

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
    for trade_info in trade_info_list:
        req = trade_info.get('trade')
        trade_type = trade_info.get("trade_type")
        symbol = trade_info.get("symbol")

        spider = BinanceTradeDetail(logger, symbol, exchange, req, trade_type)
        t = MyThread(target=spider.task_thread, args=())
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

