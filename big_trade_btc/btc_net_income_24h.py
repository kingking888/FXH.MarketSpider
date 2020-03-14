#!/usr/bin/env python
# -*- coding:utf-8 -*-

import requests
import time
import json
import redis
from loguru import logger


last_time = 0

while True:
    url = "https://api.chain.info/v2/entity/list"
    try:
        result = requests.get(url).json()

        if result.get("success"):
            all_info = {}
            data = result.get('data')

            block_height = data.get("blockHeight")
            update_time = data.get("date")
            if update_time > last_time:
                last_time = update_time

                entity_list = data.get("entityList")
                item_list = []
                for entity in entity_list:
                    item = {}
                    item["Name"] = entity.get("name")  # 交易所名，如"Huibo"
                    item["Url"] = entity.get("url")  # - 网站url "https://www.Coinbase.com/"
                    item["Rank"] = entity.get("rank")  # 列表中交易所的rank，列表顺序为rank  - 排行

                    item["AddressCount"] = entity.get("addressCount")  # 这个交易所在这个高度的地址数      - 地址数
                    item["Balance"] = entity.get("balance")  # 此交易所在此高度的BTC余额   - 链上余额(BTC)

                    # item["bigTotalExpense"] = entity.get("bigTotalExpense")  # 该交易所前一天大贸易输出端的BTC金额
                    # item["bigTotalIncome"] = entity.get("bigTotalIncome")  # 该交易所最后一天大贸易输入端BTC的金额
                    item["BigTradeNum_24h"] = entity.get("bigTotalNum")  # 该交易所在最后一天的大交易数量 -  24h大额充提笔数
                    item["NetIncome_24h"] = entity.get("netIncome")  # 本交易所在此高度和最后一天高度的BTC差额          - 24h净流入(BTC)

                    # item["coldBalance"] = entity.get("coldBalance")  # 此交易所冷钱包在此高度的BTC余额
                    # item["coldNum"] = entity.get("coldNum")  # 冷钱包地址的号码
                    # item["hotBalance"] = entity.get("hotBalance")  # 此交易所热钱包在此高度的BTC余额
                    # item["hotNum"] = entity.get("hotNum")  # 热钱包地址的编号
                    # item["rechargeBalance"] = entity.get("rechargeBalance")  # 本交易所的BTC余额在此高度存入钱包
                    # item["rechargeNum"] = entity.get("rechargeNum")  # 存储钱包地址的编号

                    item_list.append(item)

                all_info['Time'] = update_time
                all_info['BlockHeight'] = block_height
                all_info['Info'] = item_list

                while True:
                    try:
                        redis_connect = redis.Redis(host="47.107.228.85", port=6379,
                                                    password="20ab20!2#Spider!alxmH")
                        redis_connect.lpush("BTC_NetIncome_24h", json.dumps(all_info))

                        logger.info("Push: {}".format(all_info))
                        break
                    except Exception as e:
                        logger.error(e)

            time.sleep(300)
    except Exception as e:
        logger.error("程序执行错误: {}".format(e))


