import requests
import time
import json
import redis
import threading

from lxml import etree
from loguru import logger

class StockMarket(object):
    def __init__(self):
        self.utc_time = 0
        self.item_list = []
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Cookie": "s=dd11up8zw3; xq_a_token=2ee68b782d6ac072e2a24d81406dd950aacaebe3; xq_r_token=f9a2c4e43ce1340d624c8b28e3634941c48f1052; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTU4NzUyMjY2MSwiY3RtIjoxNTg1MTU1NDYyMTEzLCJjaWQiOiJkOWQwbjRBWnVwIn0.mgIcYRJJYCVbuUMRNGsGi0imPyS4C7jbmaAw1vcfCQ_60mUfG0hI9JVplXQ2waX7pNBunAYry0WPyLCaobKOW8M5QVQ_opdTG9RdPEeqODXwXBXsM3GrXo0Q3VNSPsUykppcDltCtpkzl1ZoPgHVCP7Zr0eGtsRrjtHMaRH9C5EqquhSdRDqnyGK3gKJS3sjHWNzjiGiHz-35EdRWlDpXjI4fqdl0mMPfxsydJpvhzdXFGO3xA3fCnF3mEAUXvgChYRewVV-4X548mB3Ipz6AzqmOQbsyGyU4qAdaDMyQwTA60hqRGsugY08nNHouTI0GVaTNswKpQ1lQCA5x160tg; u=191585155493535; cookiesu=191585155493535; device_id=eff311f8bff3877c0be80f1a3de53f43; Hm_lvt_1db88642e346389874251b5a1eded6e3=1585155494,1585155538; _ga=GA1.2.1747436951.1585157634; _gid=GA1.2.25489312.1585157634; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1585166075",
            # "Host": "stock.xueqiu.com",
            # "Sec-Fetch-Dest": "document",
            # "Sec-Fetch-Mode": "navigate",
            # "Sec-Fetch-Site": "none",
            # "Sec-Fetch-User": "?1",
            # "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
        }

        self.stock_base_url = "https://stock.xueqiu.com/v5/stock/quote.json?symbol={}&extend=detail"
        self.stock_name_list = [".IXIC", ".INX", ".DJI", "SH000001", "SH000300", "HKHSI"]
        # self.stock_name_list = ["SH000001", "SZ399001", "SH000300"]

        self.n225_url = "https://cn.investing.com/indices/japan-ni225"

        self.xau_url = "https://hq.sinajs.cn/?list=hf_XAU"
        self.xau_year_high = 1703.39
        self.xau_year_low = 1266.35

        self.redis_connect = redis.Redis(host="47.107.228.85", port=6379, password="20ab20!2#Spider!alxmH")

    def get_us_a(self):
        for stock_name in self.stock_name_list:
            url = self.stock_base_url.format(stock_name)
            response = requests.get(url, headers=self.headers)

            data = response.json()['data']['quote']

            item = {
                'name': data['symbol'],
                'price': data['current'],
                'increase_rate': data['percent'],
                'increase_amount': data['chg'],
                'amplitude': data['amplitude'],
                'open': data['open'],
                'close': data['last_close'],
                'high': data['high'],
                'low': data['low'],
                'volume': data['volume'],
                'amount': data['amount'],
                'year_high': data['high52w'],
                'year_low': data['low52w'],
            }

            self.item_list.append(item)

    def get_n225(self):

        html = requests.get(self.n225_url, headers=self.headers).text
        obj = etree.HTML(html)

        data1 = obj.xpath('//span[@dir="ltr"]//text()')
        data2 = obj.xpath('//span[@class="float_lang_base_2 bold"]//text()')

        _open = float(data1[4].replace(",", ""))
        _close = float(data1[3].replace(",", ""))
        _high = float(data1[7].replace(",", ""))
        _low = float(data1[5].replace(",", ""))

        item = {
            'name': 'N225',
            'price': float(data1[0].replace(",", "")),
            'increase_rate': float(data1[2].replace("+", "").replace("%", "")),
            'increase_amount': float(data1[1].replace(",", "")),
            'amplitude': float("%2f" % ((_high - _low) / _close * 100)),
            'open': _open,
            'close': _close,
            'high': _high,
            'low': _low,
            'volume': data2[1] if data2[1] != 'N/A' else int(data2[4].replace(",", "")) // 3,
            'amount': None,
            'year_high': float(data2[5].split(" - ")[0].replace(",", "")),
            'year_low': float(data2[5].split(" - ")[1].replace(",", ""))
        }

        self.item_list.append(item)

    def get_xau(self):
        html = requests.get(self.xau_url).text.strip()
        data = html[html.find("=") + 2:-2].split(",")

        _price = float(data[0])
        _high = float(data[4])
        _low = float(data[5])
        _close = float(data[7])
        _open = float(data[8])

        if _price > self.xau_year_high:
            self.xau_year_high = _price

        if _price < self.xau_year_low:
            self.xau_year_low = _price

        item = {
            'name': 'XAU',
            'price': _price,
            'increase_rate': float("%2f" % (_price - _close)),
            'increase_amount': float("%2f" % ((_price - _close) / _close * 100)),
            'amplitude': float("%2f" % ((_high - _low) / _close * 100)),
            'open': _open,
            'close': _close,
            'high': _high,
            'low': _low,
            'volume': None,
            'amount': None,
            'year_high': self.xau_year_high,
            'year_low': self.xau_year_low
        }

        self.item_list.append(item)

    def main(self):
        while True:
            self.utc_time = int(time.time() * 1000)
            if self.utc_time % 10000 == 0:
                self.item_list = []

                try:
                    # 获取美股和A股（数据源雪球网）
                    self.get_us_a()
                    # 获取日经225（数据源英为财经）
                    self.get_n225()
                    # 获取伦敦金（数据源新浪财经）
                    self.get_xau()
                except Exception as e:
                    logger.error(e)
                    continue

                data = {
                    'time': self.utc_time,
                    'data': self.item_list
                }

                while True:
                    try:
                        self.redis_connect.lpush("StockMarket_history", json.dumps(data))
                        self.redis_connect.set("StockMarket", json.dumps(data))
                        logger.info(data)
                        break
                    except Exception as e:
                        logger.error(e)

if __name__ == "__main__":
    spider = StockMarket()
    spider.main()



