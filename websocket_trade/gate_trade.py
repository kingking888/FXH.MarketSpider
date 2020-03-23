
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


# 记录实时的BTC指数价
btc_index_price = 0

class GateTradeSpider(object):
    def __init__(self, logger, symbol, exchange, req, trade_type, multiplier):
        self.logger = logger
        self.symbol = symbol
        self.exchange = exchange
        self.req = req
        self.trade_type = trade_type
        self.multiplier = multiplier

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
                # 区分 USDT结算合约 和 BTC结算合约
                socket_url = self.exchange.get("socket_url_usdt") if "USDT" in self.symbol else self.exchange.get("socket_url_btc")

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
                result = ws.recv()

                # 如果websocket响应是 ping
                if result == "ping":
                    # 则构建 pong 回复给服务器，保持连接
                    ws.send("pong")
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

        if result['event'] == 'update':
            # [{"size":228,"id":4190122,"create_time":1584876392,"price":"6167.7","contract":"BTC_USD"}]
            info_list = result['result']

            for info in info_list:
                item = {}
                item["Time"] = int(str(info.get("create_time")) + str(int(time.time() * 1000))[-3:])
                # 成交id
                item['ID'] = str(info.get("id"))
                #item["Pair1"] = self.symbol
                #item["Pair2"] = "USD"
                #item["Title"] = self.trade_type
                # 成交价
                item["Price"] = float(info.get("price"))
                # 主动成交方向 ( 0买  1卖)
                item["Direction"] = int(0 if info.get("size") > 0 else 1)
                # Volume 成交量(张)，买卖双边成交量之和
                item["Volume"] = abs(info.get("size") if info.get("size") else 0)


                # Amount 成交量(USD/USDT)
                if "USDT" in self.symbol:
                    # USDT合约：Amount（USDT） = 成交价 * volume * 转换系数
                    item["Amount"] = float("%.2f" % (item["Price"] * item["Volume"] * self.multiplier))
                else:
                    # USD合约：Amount（USD） = 成交价 * volume * 转换系数 * btc_index_price
                    global btc_index_price
                    item['Amount'] = float("%.2f" % (item["Price"] * item["Volume"] * self.multiplier * btc_index_price))
                # print(item)

                redis_key_name = "gate-io:futures:trade:{}_{}_trade_detail".format(self.symbol, self.trade_type)
                # now_time = int(time.time() / 60) * 60

                while True:
                    try:
                        redis_connect.lpush(redis_key_name, json.dumps(item))
                        # self.logger.info(item)
                        # self.last_item = item
                        # if int(time.time()) % 5 == 0:
                        #     self.logger.info("push item")
                        redis_connect.ltrim(redis_key_name, 0, 19999)
                        break
                    except Exception as e:
                        self.logger.error(e)
                        self.logger.error(item)


def get_index_price():
    while True:
        try:
            ws = create_connection("wss://fx-ws.gateio.ws/v4/ws/btc")
            ws.send('{"time" : 123456, "channel" : "futures.tickers", "event": "subscribe", "payload" : ["BTC_USD"]}')
            while True:
                data = json.loads(ws.recv())
                if data.get('event') == 'update':
                    # {"result":[{"contract":"BTC_USD","last":"6259.6","change_percentage":"3.82","funding_rate":"0.0001","mark_price":"6354.45","index_price":"6354.21","total_size":"5572326","volume_24h":"3293727","quanto_base_rate":"","volume_24h_usd":"3293727","volume_24h_btc":"518","funding_rate_indicative":"0.0001","volume_24h_quote":"3293727","volume_24h_settle":"526","volume_24h_base":"526"}]}
                    global btc_index_price
                    btc_index_price = float(data.get("result")[0].get("index_price"))
                    logger.info(" > > > 当前 BTC 指数价为 : {} < < <".format(btc_index_price))
        except Exception as e:
            logger.error(e)


class MyThread(threading.Thread):
    def __init__(self, target, args):
        super().__init__()
        self.target = target
        self.args = args

    def run(self):
        self.target(*self.args)


if __name__ == "__main__":
    # k线 logger日志
    logger = Logger.get_logger("gate_trade_log")
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

    # 创建 conf/script_conf/trade_socket/heyue.yaml 配置对象
    script_path = '{}/conf/script_conf/trade_socket/gate.yaml'.format(last_dir)
    script_config = Config(script_path)

    # 获取所有交易所的 采集配置
    exchange = script_config.get_value("gate")

    # 是否需要使用代理（目前huobi不需要代理）
    proxy = exchange.get("proxy")
    pair_url_list = exchange.get("pair_url_list")
    trade_info = exchange.get("trade_info")




    # 代理和requests报头
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
            #     resp_list += requests.get(pair_url, headers=headers, proxies={"https": "https://127.0.0.1:{}".format(8080, 8323)}).json()
            # else:
            #     resp_list += requests.get(pair_url, headers=headers).json()
            resp_list += requests.get(pair_url, headers=headers).json()

        #####################################################################
        # 获取所有合约币种信息 与 转换乘数 [['ZEC_USDT', '0.01'], ['EOS_USDT', '1'], ['BCH_USDT', '0.01'], ['BCH_USD', '0.000001']]
        symbol_list = [[resp['name'], float(resp['quanto_multiplier'])] for resp in resp_list]
        print(symbol_list)
        #####################################################################

        t = MyThread(target=get_index_price, args=())
        thread_list.append(t)
        t.start()
        time.sleep(1)


        while True:
            if btc_index_price:
                # 获取所有k线采集方案(3次)
                #for trade_info in trade_info_list:
                # 迭代每个币种，并构建该币种k线 websocket请求(9次)
                for symbol, multiplier in symbol_list:
                    trade_type = trade_info.get('trade_type')
                    trade = trade_info.get('trade')
                    req = "{" + trade[1: -1].format(symbol=symbol) + "}"
                    spider = GateTradeSpider(logger, symbol, exchange, req, trade_type, multiplier)
                    t = MyThread(target=spider.task_thread, args=())
                    thread_list.append(t)
                    t.start()
                    time.sleep(0.2)
                break
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

