import requests
import time
import json
import random

# stock_list = {
#     "道琼斯工业指数": "gb_$dji",  # 30
#     "纳斯达克指数": "gb_ixic",  # 5200
#     "标准普尔500指数": "gb_$inx"  # 500
# }

# stock_name_list = ["道琼斯工业指数", "纳斯达克指数", "标准普尔500指数"]
stock_list = ["us.dji", "us.ixic", "us.inx"]

while True:
    utc_time = int(time.time() * 1000)
    if utc_time % 5000 == 0:
        url = "https://hq.sinajs.cn/etag.php?_={}&list=gb_$dji,gb_ixic,gb_$inx".format(utc_time)
        #url = "https://hq.sinajs.cn/etag.php?_={}&list=s_sh000001,s_sz399001,sh000300".format(utc_time)


        html = requests.get(url).text
        print(html)
        result_list = [data[data.find("=")+2:-2].split(',') for data in html.split("\n")][:-1]
        print(result_list)
        item_list = []
        for stock, result in zip(stock_list, result_list):
            # print(result)
            item = {}
            item['name'] = stock
            item['price'] = result[1]
            item['increase_rate'] = result[2]
            item['increase_amount'] = result[4]
            item['open'] = result[5]
            item['close'] = result[-2]
            item['high'] = result[6]
            item['low'] = result[7]
            item['volume'] = result[10]
            item['year_high'] = result[8]
            item['year_low'] = result[9]

            item_list.append(item)

        data = {
            'time': utc_time,
            'data': item_list
        }

        print(data)