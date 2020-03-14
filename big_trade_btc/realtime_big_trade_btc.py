#!/usr/bin/env python
# -*- coding:utf-8 -*-

import socketio
from loguru import logger
import json
import redis


redis_connect = redis.Redis(host="47.107.228.85", port=6379, db=0, password="20ab20!2#Spider!alxmH")
sio = socketio.Client(logger=True)


@sio.on('connect')
def on_connect():
    logger.info('正在连接: https://open.chain.info/v1/socket')
    sio.emit('newnotification')
    sio.sleep(0)
    logger.info('连接成功。')

@sio.on('newNotification')
def on_newNotification(data):
    data = json.loads(data)
    save_data(data)
    sio.emit('newnotification')

def save_data(data):
    item = {
        'Time': data['date'],
        'InputAddress': data['input_address'],
        'InputAddressType': data['input_address_type'],
        'InputEntity': data['input_entity'],
        'OutputAddress': data['output_address'],
        'OutputAddressType': data['output_address_type'],
        'OutputEntity': data['output_entity'],
        'Txid': data['txid'],
        'Type': data['type'],
        'Value': data['value'],
        'Source': data['url']
    }
    logger.info("获取新的数据: ", item)

    while True:
        try:
            global redis_connect
            redis_connect.lpush("big_trade_btc", json.dumps(item))
            break
        except Exception as e:
            logger.error(e)
            redis_connect = redis.Redis(host="r-wz9jjob47pi7m6rykxpd.redis.rds.aliyuncs.com", port=6379, db=0, password="20ab20!2#Spider!alxmH")
    logger.info("采集完成，稍后重试...")



@sio.on('disconnect')
def on_disconnect():
    logger.info('正在断开...')


if __name__ == '__main__':
    # var socket = io('https://open.chain.info', {path: '/v1/socket'});
    # sio.connect('https://open.chain.info', namespaces='/v1/socket')
    sio.connect(url='https://open.chain.info', socketio_path='/v1/socket')
    sio.wait()




