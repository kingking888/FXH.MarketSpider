import requests
import random
import time
import redis
import json

from loguru import logger

headers = {
	"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
	"accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
	"cache-control": "max-age=0",
	"cookie": "_ga=GA1.2.1199862298.1574926826; __cfduid=da42e4cdf12e703e2ab9f80f00a27e6781577534385; _gid=GA1.2.1545321358.1578825994",
	"upgrade-insecure-requests": "1",
	"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36"
}

url = "https://open.chain.info/v1/entity/bigTrade"

last_time = 0

def send_request():
    redis_connect = redis.Redis(host="47.107.228.85", port=6379, db=0, password="20ab20!2#Spider!alxmH")
    global last_time

    while True:
        proxies = {"https": "http://127.0.0.1:{}".format(random.randint(8080, 8323))}
        res = requests.get(url, headers=headers, proxies=proxies).json()
        data_list = res.get("data")
        if data_list:
            for data in data_list:
                if data.get("value") >= 50 and data.get("timestamp") > last_time:
                    logger.info("获取新的数据..")
                    item = {
                        'Time': data['timestamp'],
                        'InputAddress': data['inputAddress'],
                        'InputAddressType': data['inputAddressType'],
                        'InputEntity': data['inputEntity'],
                        'OutputAddress': data['outputAddress'],
                        'OutputAddressType': data['outputAddressType'],
                        'OutputEntity': data['outputEntity'],
                        'Txid': data['txid'],
                        'Type': data['type'],
                        'Value': data['value'],
                        'Source': data['url']
                    }
                    last_time = item['Time']
                    logger.info(item)

                    while True:
                        try:
                            redis_connect.lpush("big_trade_btc", json.dumps(item))
                            break
                        except Exception as e:
                            logger.error(e)
                            redis_connect = redis.Redis(host="r-wz9jjob47pi7m6rykxpd.redis.rds.aliyuncs.com", port=6379, db=0, password="20ab20!2#Spider!alxmH")

                    logger.info("采集完成，稍后重试...")
        else:
            # logger.info("没有数据,正在重试..")
            time.sleep(0.5)



        """
                {
                    date: "2020-01-12 12:11:24 GMT",
                    inputAddress: "multi",
                    inputAddressType: "",
                    inputEntity: "Huobi",
                    outputAddress: "1MmDYdMcRU7px5Tah6ahr4vXGdyjKo1ohs",
                    outputAddressType: "",
                    outputEntity: "Unknown Entity",
                    timestamp: 1578831084,
                    txid: "8f88c686e7544a5bb9dcb7d3b7a38c390b93d790bd51c5c7a9461476812db3df",
                    type: "Multi-Withdraw",
                    url: "https://chain.info/8f88c686e7544a5bb9dcb7d3b7a38c390b93d790bd51c5c7a9461476812db3df",
                    value: 50
                }
        """

if __name__ == "__main__":

    while True:
        try:
            send_request()
        except Exception as e:
            time.sleep(10)
            logger.error(e)
            logger.error("程序错误，正在重试...")
