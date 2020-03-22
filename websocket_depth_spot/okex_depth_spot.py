import time
from datetime import datetime
from datetime import timedelta
import os
import redis
import gzip
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


# memony 56MB ~ 60MB  -> 64MB

class OkexDepthSpider(object):
    def __init__(self, logger, symbol, exchange, req, depth_type, pair1, pair2):
        self.logger = logger
        self.symbol = symbol
        self.exchange = exchange
        self.req = req
        self.depth_type = depth_type
        self.pair1 = pair1
        self.pair2 = pair2
        self.last_item = None
        self.bids_p = None
        self.asks_p = None


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
                self.logger.info('数字货币： {} {} connect ws error, retry...'.format(self.symbol, self.depth_type))
                time.sleep(1)

        logger.info("数字货币： {} {} connect success".format(self.symbol, self.depth_type))
        # 获取数据加密类型（gzip）
        utype = self.exchange.get("utype")

        # 发送了各币种的各k线的websocket请求
        self.logger.info("当前采集方案: {}".format(self.req))
        ws.send(self.req)

        # 获取数据：
        while True:
            try:
                try:
                    # 设置 websocket 超时时间, 时间太久会导致 depth 一分钟没数据，因目前交易所采集稳定暂时不设置
                    ws.settimeout(20)
                    # 接收websocket响应
                    result = ws.recv()
                except:
                    print("req:", self.req)
                    # ws.send(self.req)
                    ws.send("ping")
                    continue

                # 加密方式 gzip
                if utype == 'gzip':
                    try:
                        result = gzip.decompress(result).decode('utf-8')
                    except:
                        pass
                # 加密方式 deflate
                elif utype == "deflate":
                    try:
                        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
                        inflated = decompress.decompress(result)
                        inflated += decompress.flush()
                        result = inflated.decode("utf-8")
                    except:
                        pass

                # 加密方式 未加密
                elif utype == "string":
                    pass

                # 如果websocket响应是 pong
                if result == 'pong':
                    # 则继续获取响应，保持连接
                    self.logger.info(result)
                else:
                    # self.logger.info(result)
                    self.save_result_redis(result)

            except Exception as e:
                logger.info(e)
                logger.info("数字货币：{} {} 连接中断，reconnect.....".format(self.symbol, self.depth_type))
                ws.close()
                del ws
                gc.collect()
                # 如果连接中断，递归调用继续
                time.sleep(60)
                self.task_thread()

    def save_result_redis(self, result):
        result = json.loads(result)
        # print(result)
        if result.get('action'):
            data = result['data'][0]
            timestamp = data['timestamp']

            if result['action'] == 'partial':
                # 获取首次全量深度数据
                self.bids_p = data['bids']
                self.asks_p = data['asks']


            elif result['action'] == 'update':
                # 获取合并后数据
                self.bids_p = self.update_bids(result, self.bids_p)
                self.asks_p = self.update_asks(result, self.asks_p)

            item = {}
            utc_time = timestamp.replace("T", " ").replace("Z", "")
            struct_time = datetime.strptime(utc_time, "%Y-%m-%d %H:%M:%S.%f")
            # okex 数据是utc时间，转换国内需要加8个时区
            item["Time"] = int(time.mktime(struct_time.timetuple()) * 1000.0 + struct_time.microsecond / 1000.0) + 28800000
            #item["Pair1"] = self.pair1
            #item["Pair2"] = self.pair2
            #item["Title"] = self.depth_type

            # Sells
            item["Sells"] = [[float(a), float(b)] for a, b, _ in self.bids_p[:100]] # 按价格降序[5, 4, 3, 2, 1]

            # Buys
            item["Buys"] = [[float(a), float(b)]for a, b, _ in self.asks_p[:100]]  # 按价格升序[6, 7, 8, 9, 10]

            # print(item)

            redis_key_name = "okex:spot:depth:{}_{}_depth_100".format(self.pair1, self.pair2)
            # now_time = int(time.time() / 60) * 60
            while True:
                try:
                    redis_connect.lpush(redis_key_name, json.dumps(item))
                    # if int(time.time()) % 5 == 0:
                    #     self.logger.info("push item")
                    redis_connect.ltrim(redis_key_name, 0, 19999)
                    # print(item)
                    break
                except Exception as e:
                    self.logger.error(e)

    def update_bids(self, res, bids_p):
        # 获取增量bids数据
        bids_u = res['data'][0]['bids']
        # print(timestamp + '增量数据bids为：' + str(bids_u))
        # print('档数为：' + str(len(bids_u)))
        # bids合并
        for i in bids_u:
            bid_price = i[0]
            for j in bids_p:
                if bid_price == j[0]:
                    if i[1] == '0':
                        bids_p.remove(j)
                        break
                    else:
                        del j[1]
                        j.insert(1, i[1])
                        break
            else:
                if i[1] != "0":
                    bids_p.append(i)
        else:
            bids_p.sort(key=lambda price: self.sort_num(price[0]), reverse=True)
            # print(timestamp + '合并后的bids为：' + str(bids_p) + '，档数为：' + str(len(bids_p)))
        return bids_p

    def update_asks(self, res, asks_p):
        # 获取增量asks数据
        asks_u = res['data'][0]['asks']
        # print(timestamp + '增量数据asks为：' + str(asks_u))
        # print('档数为：' + str(len(asks_u)))
        # asks合并
        for i in asks_u:
            ask_price = i[0]
            for j in asks_p:
                if ask_price == j[0]:
                    if i[1] == '0':
                        asks_p.remove(j)
                        break
                    else:
                        del j[1]
                        j.insert(1, i[1])
                        break
            else:
                if i[1] != "0":
                    asks_p.append(i)
        else:
            asks_p.sort(key=lambda price: self.sort_num(price[0]))
            # print(timestamp + '合并后的asks为：' + str(asks_p) + '，档数为：' + str(len(asks_p)))
        return asks_p

    @staticmethod
    def sort_num(n):
        if n.isdigit():
            return int(n)
        else:
            return float(n)

    def check(self, bids, asks):
        if len(bids) >= 25 and len(asks) >= 25:
            bids_l = []
            asks_l = []
            for i in range(1, 26):
                bids_l.append(bids[i - 1])
                asks_l.append(asks[i - 1])
            bid_l = []
            ask_l = []
            for j in bids_l:
                str_bid = ':'.join(j[0 : 2])
                bid_l.append(str_bid)
            for k in asks_l:
                str_ask = ':'.join(k[0 : 2])
                ask_l.append(str_ask)
            num = ''
            for m in range(len(bid_l)):
                num += bid_l[m] + ':' + ask_l[m] + ':'
            new_num = num[:-1]
            int_checksum = zlib.crc32(new_num.encode())
            fina = self.change(int_checksum)
            return fina
        else:
            self.logger.error("depth data < 25 is error")
            print('深度数据少于25档')


    def change(self, num_old):
        num = pow(2, 31) - 1
        if num_old > num:
            out = num_old - num * 2 - 2
        else:
            out = num_old
        return out



class MyThread(threading.Thread):
    def __init__(self, target, args):
        super().__init__()
        self.target = target
        self.args = args

    def run(self):
        self.target(*self.args)


if __name__ == "__main__":
    # k线 logger日志
    logger = Logger.get_logger("okex_depth_log")
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
    script_path = '{}/conf/script_conf/depth_socket/okex.yaml'.format(last_dir)
    script_config = Config(script_path)

    # 获取所有交易所的 采集配置
    exchange = script_config.get_value("okex")

    # 是否需要使用代理（目前huobi不需要代理）
    proxy = exchange.get("proxy")
    pair_url_spot = exchange.get("pair_url_spot")
    depth_info_spot = exchange.get("depth_info_spot")

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
            symbol_list = [[data.get("instrument_id"), data.get("base_currency"), data.get("quote_currency")] for data
                           in data_list if data.get("base_currency") == "BTC"]

            print(symbol_list)
            print(len(symbol_list))
            #####################################################################

            # 获取所有k线采集方案(3次)
            for depth_info in depth_info_spot:
                # 迭代每个币种，并构建该币种k线 websocket请求(9次)
                for index, symbol in enumerate(symbol_list):
                    depth_type = depth_info.get("depth_type")
                    depth = depth_info.get('depth')

                    req = "{" + depth[1: -1].format(symbol=symbol[0]) + "}"
                    pair1 = symbol[1].upper()
                    pair2 = symbol[2].upper()
                    spider = OkexDepthSpider(logger, symbol[0], exchange, req, depth_type, pair1, pair2)
                    t = MyThread(target=spider.task_thread, args=())
                    thread_list.append(t)
                    t.start()
                    print(index)

        while True:
            length = len(threading.enumerate())
            logger.info('当前运行的线程数为：%d' % length)
            time.sleep(10)
            if length <= 1:
                break

        # 主线程等待子线程执行完毕
        for t in thread_list:
            t.join

