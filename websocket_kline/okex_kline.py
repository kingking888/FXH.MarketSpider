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


futures_info_dict = []
last_futures_info_dict = []


class OkexKlineSpider(object):
    def __init__(self, logger, exchange):
        self.logger = logger
        self.exchange = exchange
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
    def task_thread(self, index):
        self.symbol = futures_info_dict[index]['pair1']
        self.coin = futures_info_dict[index]['pair2']
        self.kline_type = futures_info_dict[index]['timeid']
        self.req = futures_info_dict[index]['kline']
        self.logger.info('数字货币：{} {} ：{} 数据获取开始时间'.format(self.symbol,  self.coin, self.kline_type))

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
                self.logger.error('数字货币： {} {} {} connect ws error, retry...'.format(self.symbol,  self.coin, self.kline_type))
                time.sleep(1)

        logger.info("数字货币： {} {} {} connect success".format(self.symbol,  self.coin, self.kline_type))
        # 获取数据加密类型（gzip）
        utype = self.exchange.get("utype")

        # 发送了各币种的各k线的websocket请求
        self.logger.info("当前采集方案: {}".format(self.req))
        ws.send(self.req)

        # 获取数据：
        try:
            while True:
                if self.req != futures_info_dict[index]['kline']:
                    raise TypeError("{} 合约已经更新: {}，需要重新发送请求...".format(self.req, futures_info_dict[index]['kline']))

                data = ws.recv()
                if data != '':
                    result = self.deflate_decode(data)
                    if result != 'pong':
                        self.save_result_redis(result)
                    else:
                        time.sleep(0.1)
                ws.send("ping")

        except Exception as e:
            logger.error(e)
            logger.error("数字货币：{} {} {} 连接中断，reconnect.....".format(self.symbol,  self.coin, self.kline_type))
            ws.close()
            gc.collect()
            # 如果连接中断，递归调用继续
            self.task_thread(index)


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

            redis_key_name = "okex:futures:kline:{}_{}_{}_1min_kline".format(self.symbol, self.coin, self.kline_type)
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
                        self.logger.info("push item: {}_{}_{} {}".format(self.symbol, self.coin, self.kline_type, self.last_item))
                        self.last_item = item
                        break
                    except Exception as e:
                        self.logger.error(e)
            else:
                redis_connect.lpop(redis_key_name)
                while True:
                    try:
                        redis_connect.lpush(redis_key_name, json.dumps(item))
                        self.logger.info("update item: {}_{}_{} {}".format(self.symbol, self.coin, self.kline_type, item))
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


def get_instruments():
    while True:
        try:
            futures_url = "https://www.okex.com/api/futures/v3/instruments"
            swap_url = "https://www.okex.com/api/swap/v3/instruments"

            # proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}

            proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}
            logger.info(proxies)
            futures_list = requests.get(futures_url, proxies=proxies).json()

            proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}
            logger.info(proxies)
            futures_list += requests.get(swap_url, proxies=proxies).json()



            futures_kline = '{"op":"subscribe","args":"futures/candle60s:instrument_id"}'
            swap_kline = '{"op":"subscribe","args":"swap/candle60s:instrument_id"}'

            futures_info_list = []
            for futures in futures_list:
                item = {}
                item['pair1'] = futures.get('base_currency')
                item['pair2'] = futures.get("quote_currency")
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
                    item['timeid'] = 'SWAP'

                if item['timeid'] == 'SWAP':
                    item['kline'] = swap_kline.replace("instrument_id", futures.get('instrument_id'))
                else:
                    item['kline'] = futures_kline.replace("instrument_id", futures.get('instrument_id'))
                # {'pair1': 'BTC', 'pair2': 'USD', 'timeid': 'NQ', 'kline': '{"op":"subscribe","args":"futures/candle60s:BTC-USD-200925"}'}
                futures_info_list.append(item)

            global futures_info_dict, last_futures_info_dict
            # print(futures_info_list)
            # {0: {'pair1': 'XRP', 'pair2': 'USD', 'timeid': 'CW', 'kline': '{"op":"subscribe","args":"futures/candle60s:XRP-USD-200320"}'}
            futures_info_dict = {index: futures for index, futures in enumerate(futures_info_list)}
            if last_futures_info_dict != futures_info_dict:
                last_futures_info_dict = futures_info_dict
                for futures_info in futures_info_dict.items():
                    logger.info(futures_info)
            time.sleep(60)
        except Exception as e:
            logger.error(e)
            time.sleep(30)



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

    while True:
        # 子线程组
        thread_list = []


        t = MyThread(target=get_instruments, args=())
        thread_list.append(t)
        t.start()

        # globals futures_info_dict
        while True:
            if futures_info_dict:
                # 迭代每个币种，并构建该币种k线 websocket请求 (9次)
                for index in futures_info_dict:
                    spider = OkexKlineSpider(logger, exchange)
                    t = MyThread(target=spider.task_thread, args=(index,))
                    thread_list.append(t)
                    t.start()
                break
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
