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
            "Cookie": "Hm_lpvt_1db88642e346389874251b5a1eded6e3=1587097163; Hm_lvt_1db88642e346389874251b5a1eded6e3=1587097163; device_id=24700f9f1986800ab4fcc880530dd0ed; u=381587097161400; xq_a_token=48575b79f8efa6d34166cc7bdc5abb09fd83ce63; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTU4OTY4MjczMCwiY3RtIjoxNTg3MDk3MTE3NTY0LCJjaWQiOiJkOWQwbjRBWnVwIn0.kCqe33_NoaXzQ4LTWh8vdS1nWPo4gOtDygHmZXPnVy0xgO6ecTgLN5neE3-zRG9USBzcxpr_OJ5VUl4o6hZK-YXWEMRJwLTDMpSBKT4FPBlP-vN3RLBLT2jFoT7XTN26vyv6JxNJ2YYr6eSsiKq_xsgusV53xgotlQ0G89gOOvoKEIBQkQJxc65VQePR42r6Ve3QJy-wQcWHPd8wst0RdV0nSv15QwgFPT63xJ3e3CXR6tserflKV8equd6d_goKiW4SyImihJIL2qPW9BvEzPZgFOEk0ln9BONXgo0eo4UPzcII0nWemnXKvIfFfurNikkkkUhxIqXFNcjq7j2GRg; xq_r_token=7dcc6339975b01fbc2c14240ce55a3a20bdb7873; xqat=48575b79f8efa6d34166cc7bdc5abb09fd83ce63",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Host": "stock.xueqiu.com",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Safari/605.1.15",
            "Accept-Language": "zh-cn",
            "Accept-Encoding": "gzip, br, deflate",
            "Connection": "keep-alive"
        }


        self.headers_investing = {
            "Cookie": "G_ENABLED_IDPS=google; Hm_lpvt_a1e3d50107c2a0e021d734fe76f85914=1587097271; Hm_lvt_a1e3d50107c2a0e021d734fe76f85914=1587097258; SideBlockUser=a%3A2%3A%7Bs%3A10%3A%22stack_size%22%3Ba%3A1%3A%7Bs%3A11%3A%22last_quotes%22%3Bi%3A8%3B%7Ds%3A6%3A%22stacks%22%3Ba%3A1%3A%7Bs%3A11%3A%22last_quotes%22%3Ba%3A1%3A%7Bi%3A0%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A3%3A%22178%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A20%3A%22%2Findices%2Fjapan-ni225%22%3B%7D%7D%7D%7D; _ga=GA1.2.352537061.1587097255; _gid=GA1.2.1697814951.1587097256; nyxDorf=YmUxYW4mYz5kMmlkMn84PWcoNG5maQ%3D%3D; __gads=ID=8d10341d248390f7-227bb89dacc100cb:T=1587097255:RT=1587097255:S=ALNI_MY-AMBU97UXscUAHgPOz3MKMPy8fA; _gat=1; _gat_allSitesTracker=1; prebid_page=0; prebid_session=0; adBlockerNewUserDomains=1587097253; PHPSESSID=mhk6k3bpp7lk851murtbmuoek5; StickySession=id.96487130819.589cn.investing.com; geoC=HK",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Host": "cn.investing.com",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Safari/605.1.15",
            "Accept-Language": "zh-cn",
            "Accept-Encoding": "br, gzip, deflate",
            "Connection": "keep-alive"
        }

        self.stock_base_url = "https://stock.xueqiu.com/v5/stock/quote.json?symbol={}&extend=detail"
        self.stock_name_list = [".IXIC", ".INX", ".DJI", "SH000001", "SH000300", "HKHSI"]
        # self.stock_name_list = ["SH000001", "SZ399001", "SH000300"]

        self.n225_url = "https://cn.investing.com/indices/japan-ni225"

        self.xau_oil_url = "https://hq.sinajs.cn/?list=hf_XAU,hf_OIL,hf_CL"
        self.xau_year_high = 1738.70
        self.xau_year_low = 1266.18

        self.brent_year_high = 75.6
        self.brent_year_low = 21.65

        self.wti_year_high = 66.6
        self.wti_year_low = 17.31

        self.redis_connect = redis.Redis(host="47.107.228.85", port=6379, password="20ab20!2#Spider!alxmH")

    def get_us_a(self):
        """
            获取美股和A股数据
        """
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
        """
            获取日经225数据
        """

        html = requests.get(self.n225_url, headers=self.headers_investing).text
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

    def get_xau_oil(self):
        """
            获取伦敦金和原油数据
        """
        text_list = requests.get(self.xau_oil_url).text.split(";")[:-1]
        data_list = [text[text.find("=") + 2:-2].split(",") for text in text_list]
        name_list = ['XAU', 'BRENT', 'WTI']

        year_high = '--'
        year_low = '--'

        for name, data in zip(name_list, data_list):
            # print(data)

            _price = float(data[0])
            _high = float(data[4])
            _low = float(data[5])
            _close = float(data[7])
            _open = float(data[8])

            if name == 'XAU':
                if _price > self.xau_year_high:
                    self.xau_year_high = _price
                if _price < self.xau_year_low:
                    self.xau_year_low = _price

                year_high = self.xau_year_high
                year_low = self.xau_year_low

            elif name == 'BRENT':
                if _price > self.brent_year_high:
                    self.brent_year_high = _price
                if _price < self.brent_year_low:
                    self.brent_year_low = _price

                year_high = self.brent_year_high
                year_low = self.brent_year_low

            elif name == 'WTI':
                if _price > self.wti_year_high:
                    self.wti_year_high = _price
                if _price < self.wti_year_low:
                    self.wti_year_low = _price

                year_high = self.wti_year_high
                year_low = self.wti_year_low

            item = {
                'name': name,
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
                'year_high': year_high,
                'year_low': year_low
            }
            # print(item)
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
                    # 获取伦敦金和原油（数据源新浪财经）
                    self.get_xau_oil()
                except Exception as e:
                    logger.error(e)
                    continue


                data = {
                    'time': self.utc_time,
                    'data': self.item_list
                }

                while True:
                    try:
                        # self.redis_connect.lpush("StockMarket_history", json.dumps(data))
                        self.redis_connect.set("StockMarket", json.dumps(data))
                        logger.info(data)
                        break
                    except Exception as e:
                        logger.error(e)

if __name__ == "__main__":
    spider = StockMarket()
    spider.main()



