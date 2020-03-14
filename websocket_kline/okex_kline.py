import time
import os
import redis
import gzip
import zlib
import threading
import json
import requests
import gc
import random
from datetime import datetime
from datetime import timedelta

from websocket import create_connection
from lib.decorator import tail_call_optimized
from lib.logger import Logger
from lib.config_manager import Config


# memony 56MB ~ 60MB  -> 64MB

class OkexKlineSpider(object):
    def __init__(self, logger, symbol, exchange, req, kline_type):
        self.logger = logger
        self.symbol = symbol
        self.exchange = exchange
        self.req = req
        self.kline_type = kline_type
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
        self.logger.info('数字货币：{} {} 数据获取开始时间：{}'.format(self.symbol, self.kline_type, time.strftime("%Y-%m-%d %H:%M:%S")))

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
        while True:
            try:
                now = datetime.utcnow()
                if now.weekday() == 4 and now.hour == 8 and now.minute == 0 and 59 > now.second > 1:
                    self.logger.info("================{} {} 开始交割，正在重新设定订阅方案================".format(self.symbol, self.kline_type))
                    # ws.send(self.req.replace("sub", "unsub"))

                    if self.kline_type in ['CW', 'NW']:
                        new_time_id = None
                        if self.kline_type == 'CW':
                            new_time_id = str(datetime.utcnow() + timedelta(days=7))[2:10].replace("-", "")
                        elif self.kline_type == 'NW':
                            new_time_id = str(datetime.utcnow() + timedelta(days=14))[2:10].replace("-", "")

                        self.req = "{" + self.req[1: -1].format(symbol=self.symbol, timeid=new_time_id) + "}"

                        self.logger.info("{} {} {} 新的订阅方案: {}".format(self.symbol, self.kline_type, new_time_id, self.req))

                        while True:
                            try:
                                ws = create_connection(self.exchange.get("socket_url"), http_proxy_host="127.0.0.1",
                                                       http_proxy_port=random.randint(8080, 8323))
                                ws.send(self.req)
                                result = ws.recv()
                                result = self.deflate_decode(result)
                            except:
                                # print("--result: {} {}".format(type(result), result))
                                continue

                            if result["event"] == "subscribe":
                                self.logger.info("订阅频道更新成功： {}".format(result))
                                break
                            else:
                                self.logger.info("订阅频道暂未更新： {}".format(result))
                                time.sleep(10)
                                continue
                    else:
                        self.logger.info("季度合约 与 永续合约暂不处理.")
                        break
                else:
                    pass

                try:
                    # 设置 websocket 超时时间, 时间太久会导致 kline 一分钟没数据，因目前交易所采集稳定暂时不设置
                    ws.settimeout(20)
                    # 接收websocket响应
                    result = ws.recv()
                except:
                    ws.send("ping")


                # 加密方式 gzip
                if utype == 'gzip':
                    try:
                        result = gzip.decompress(result).decode('utf-8')
                    except:
                        pass
                # 加密方式 deflate
                elif utype == "deflate":
                    try:
                        result = self.deflate_decode(result)
                    except:
                        pass
                # 加密方式 未加密
                elif utype == "string":
                    pass

                # 如果websocket响应是 pong
                if result == 'pong':
                    # 则继续获取响应，保持连接
                    pass
                else:
                    # self.logger.info(result)
                    self.save_result_redis(result)

            except Exception as e:
                logger.info(e)
                logger.info("数字货币：{} {} 连接中断，reconnect.....".format(self.symbol, self.kline_type))
                # 如果连接中断，递归调用继续
                self.task_thread()


    def save_result_redis(self, result):
        result = json.loads(result)
        if result.get("data"):
            tick = result.get("data")[0].get("candle")

            item = {}
            utc_time = tick[0].replace("T", " ").replace(".000Z", "")
            struct_time = time.strptime(utc_time, "%Y-%m-%d %H:%M:%S")
            item["Time"] = int(time.mktime(struct_time)) + 28800  # okex 数据是utc时间
            #item["Pair1"] = self.symbol
            #item["Pair2"] = "USD"
            #item["Title"] = self.kline_type
            item["Open"] = eval(tick[1])
            item["Close"] = eval(tick[4])
            item["High"] = eval(tick[2])
            item["Low"] = eval(tick[3])
            item["Amount"] = eval(tick[6])
            item["Volume"] = eval(tick[5])
            # print(item)

            redis_key_name = "okex:futures:kline:{}_{}_1min_kline".format(self.symbol, self.kline_type)
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
                        self.logger.info(self.last_item)
                        self.last_item = item
                        break
                    except Exception as e:
                        self.logger.error(e)
            else:
                print(result)
                raise ValueError("Redis data error: item time little to last_item time.")


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
    pair_url = exchange.get("pair_url")
    kline_info_list = exchange.get("kline_info")

    # 代理和requests报头
    proxies = {
        "https": "https://127.0.0.1:8300",
    }
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36',
        'source': 'web'
    }

    while True:
        # 子线程组
        thread_list = []

        if pair_url:
            # 获取当前页面币种信息，目前huobi不需要代理，其他需要代理
            if proxy == "true":
                resp = requests.get(pair_url, headers=headers, proxies=proxies).json()
            else:
                resp = requests.get(pair_url, headers=headers).json()

            #####################################################################
            # 获取所有合约币种信息（data 列表）
            data_list = resp.get("data").get("usd")
            # 获取所有合约币种名称（BTC、ETC、ETH、EOS、LTC、BCH、XRP、TRX、BSV）
            symbol_list = [data.get("symbolDesc") for data in data_list]
            print(symbol_list)

            # contract_info_list = [[contract.get("id")[2:8] for contract in contracts] for contracts in contracts_list]
            contracts_list = [data.get("contracts") for data in data_list]

            # ['191122', '191129', '191227', 'SWAP']
            time_id_list = [contract.get("id")[2:8] for contract in contracts_list[0]] + ["SWAP"]
            #####################################################################

            # 获取所有k线采集方案(4次)
            for kline_info, time_id in zip(kline_info_list, time_id_list):

                # 迭代每个币种，并构建该币种k线 websocket请求 (9次)
                for symbol in symbol_list:
                    kline_type = kline_info.get("kline_type")
                    kline = kline_info.get('kline')
                    # '{"op":"subscribe","args":"futures/depth:{symbol}-USD-{timeid}"}'
                    req = "{" + kline[1: -1].format(symbol=symbol, timeid=time_id) + "}"
                    spider = OkexKlineSpider(logger, symbol, exchange, req, kline_type)
                    #t = MyThread(target=spider.task_thread, args=())
                    t = MyThread(target=spider.task_thread, args=())
                    thread_list.append(t)
                    t.start()

        while True:
            length = len(threading.enumerate())
            logger.info('当前运行的线程数为：%d' % length)
            time.sleep(10)
            if length <= 1:
                break

        # 主线程等待子线程执行完毕
        for t in thread_list:
            t.join
