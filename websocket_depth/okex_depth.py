
import time
from datetime import datetime
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


futures_info_dict = []
last_futures_info_dict = []


class OkexDepthSpider(object):
    def __init__(self, logger, exchange):
        self.logger = logger
        self.exchange = exchange
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
    def task_thread(self, index):
        while True:
            try:
                self.symbol = futures_info_dict[index]['pair1']
                self.coin = futures_info_dict[index]['pair2']
                self.depth_type = futures_info_dict[index]['timeid']
                self.req = futures_info_dict[index]['depth']
                self.logger.info('数字货币：{} {} ：{} 数据获取开始时间'.format(self.symbol,  self.coin, self.depth_type))
                break
            except Exception as e:
                logger.error(e)
                time.sleep(10)

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
                self.logger.error('数字货币： {} {} {} connect ws error, retry...'.format(self.symbol, self.coin, self.depth_type))
                time.sleep(1)

            logger.info("数字货币： {} {} {} connect success".format(self.symbol, self.coin, self.depth_type))
        # 获取数据加密类型（gzip）
        utype = self.exchange.get("utype")

        # 发送了各币种的各k线的websocket请求
        self.logger.info("当前采集方案: {}".format(self.req))
        ws.send(self.req)

        # 获取数据：
        try:
            while True:
                if self.req != futures_info_dict[index]['depth']:
                    raise TypeError("{} 合约已经更新: {}，需要重新发送请求...".format(self.req, futures_info_dict[index]['depth']))

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
            # self.logger.error(result)
            self.logger.error("数字货币：{} {} {} 连接中断，reconnect.....".format(self.symbol,  self.coin, self.depth_type))
            ws.close()
            gc.collect()
            # 如果连接中断，递归调用继续
            self.task_thread(index)


    def save_result_redis(self, result):
        result = json.loads(result)
        if result.get('action'):
            data = result['data'][0]
            timestamp = data['timestamp']

            if result['action'] == 'partial':
                # 获取首次全量深度数据
                self.bids_p = data['bids']
                self.asks_p = data['asks']

                # 校验checksum
                # checksum = result['data'][0]['checksum']
                # print(timestamp + '推送数据的checksum为：' + str(checksum))
                # check_num = self.check(self.bids_p, self.asks_p)
                # print(timestamp + '校验后的checksum为：' + str(check_num))
                # if check_num == checksum:
                #     print("校验结果为：True")

            elif result['action'] == 'update':
                # 获取合并后数据
                self.bids_p = self.update_bids(result, self.bids_p, timestamp)
                self.asks_p = self.update_asks(result, self.asks_p, timestamp)

                # 校验checksum
                # checksum = result['data'][0]['checksum']
                # print(timestamp + '推送数据的checksum为：' + str(checksum))
                # check_num = self.check(self.bids_p, self.asks_p)
                # print(timestamp + '校验后的checksum为：' + str(check_num))
                # if check_num == checksum:
                #     print("校验结果为：True")
                #     pass
                # else:
                #     print("校验结果为：False，正在重新订阅")
                #     raise ValueError("校验结果为：False，正在重新订阅...")


            # ch = result.get("ch")  # market.BTC_CQ.depth.detail
            # data = result.get("data")[0]

            item = {}
            utc_time = timestamp.replace("T", " ").replace("Z", "")
            struct_time = datetime.strptime(utc_time, "%Y-%m-%d %H:%M:%S.%f")
            # okex 数据是utc时间，转换国内需要加8个时区
            item["Time"] = int(time.mktime(struct_time.timetuple()) * 1000.0 + struct_time.microsecond / 1000.0) + 28800000
            #item["Pair1"] = self.symbol
            #item["Pair2"] = "USD"
            #item["Title"] = self.depth_type

            # Sells
            # item_bids = {}
            # for i in self.bids_p[:100]:
            #     item_bids[float(i[0])] = int(i[1])
            # item["Sells"] = list(item_bids.items())  # 按价格升序[6, 7, 8, 9, 10]
            item["Sells"] = [[float(a), int(b)] for a, b, _, _ in self.bids_p[:100]]  # 按价格升序[6, 7, 8, 9, 10]

            # Buys
            # item_asks = {}
            # for i in self.asks_p[:100]:
            #     item_asks[float(i[0])] = int(i[1])
            # item["Buys"] = list(item_asks.items())  # 按价格降序[5, 4, 3, 2, 1]
            item["Buys"] = [[float(a), int(b)]for a, b, _, _ in self.asks_p[:100]]  # 按价格降序[5, 4, 3, 2, 1]

            # print(item)

            redis_key_name = "okex:futures:depth:{}_{}_{}_depth_100".format(self.symbol, self.coin, self.depth_type)
            # now_time = int(time.time() / 60) * 60
            while True:
                try:
                    redis_connect.lpush(redis_key_name, json.dumps(self.last_item))
                    self.last_item = item
                    # if int(time.time()) % 5 == 0:
                    #     self.logger.info("push item")
                    redis_connect.ltrim(redis_key_name, 0, 19999)
                    break
                except Exception as e:
                    self.logger.error(e)


    def update_bids(self, res, bids_p, timestamp):
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

    def update_asks(self, res, asks_p, timestamp):
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



def get_instruments():
    while True:
        try:
            futures_url = "https://www.okex.com/api/futures/v3/instruments"
            swap_url = "https://www.okex.com/api/swap/v3/instruments"

            # proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}

            # proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}
            futures_list = requests.get(futures_url).json()

            # proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}
            futures_list += requests.get(swap_url).json()
            logger.info("> > > 获取合约列表成功 < < <")


            futures_depth = '{"op":"subscribe","args":"futures/depth:instrument_id"}'
            swap_depth = '{"op":"subscribe","args":"swap/depth:instrument_id"}'

            futures_info_list = []
            for futures in futures_list:
                item = {}
                item['pair1'] = futures.get('base_currency')
                item['pair2'] = futures.get("quote_currency")
                if item['pair2'] == 'USDT':
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
                        item['depth'] = swap_depth.replace("instrument_id", futures.get('instrument_id'))
                    else:
                        item['depth'] = futures_depth.replace("instrument_id", futures.get('instrument_id'))
                    # {'pair1': 'BTC', 'pair2': 'USD', 'timeid': 'NQ', 'depth': '{"op":"subscribe","args":"futures/candle60s:BTC-USD-200925"}'}
                    futures_info_list.append(item)

            global futures_info_dict, last_futures_info_dict
            # print(futures_info_list)
            # {0: {'pair1': 'XRP', 'pair2': 'USD', 'timeid': 'CW', 'depth': '{"op":"subscribe","args":"futures/candle60s:XRP-USD-200320"}'}
            futures_info_dict = {index: futures for index, futures in enumerate(futures_info_list)}
            if last_futures_info_dict != futures_info_dict:
                last_futures_info_dict = futures_info_dict
                for futures_info in futures_info_dict.items():
                    logger.info("> > > 最新合约列表已更新 < < < ")
                    logger.info(futures_info)
            time.sleep(60)
        except Exception as e:
            time.sleep(30)



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
    pair_url = exchange.get("pair_url")
    depth_info_list = exchange.get("depth_info")


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
                    # depth_type = depth_info.get("depth_type")
                    # depth = depth_info.get('depth')
                    # '{"op":"subscribe","args":"futures/depth:{symbol}-USD-{timeid}"}'
                    # req = "{" + depth[1: -1].format(symbol=symbol, timeid=time_id) + "}"
                    # spider = OkexKlineSpider(logger, symbol, exchange, req, depth_type)
                    spider = OkexDepthSpider(logger, exchange)
                    #t = MyThread(target=spider.task_thread, args=())
                    t = MyThread(target=spider.task_thread, args=(index,))
                    thread_list.append(t)
                    t.start()
                    time.sleep(0.2)
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
