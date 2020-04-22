import requests
import json

hyd_url = "https://wx.heyuedi.com/alarm/liquidation/summary?symbol=total"
# bicoin_url = "https://blz.bicoin.com.cn/OlrStock/stock/getAllStock?timeType=24h"

hyd_headers = {
    "Host": "wx.heyuedi.com",
    "Connection": "keep-alive",
    "Accept": "*/*",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1",
    "Referer": "https://wx.heyuedi.com/alarm/liquidation/index",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,en-US;q=0.8",
    "Cookie": "__cfduid=da3bee96e1662d5650a5fed8084a5d57c1586842761; Hm_lvt_e8fd9358dc6c766363e933b25708aa7a=1586842761"
}

# bicoin_headers = {
#     "Host": "blz.bicoin.com.cn",
#     "Connection": "keep-alive",
#     "Pragma": "no-cache",
#     "Cache-Control": "no-cache",
#     "Accept": "*/*",
#     "Origin": "https://test1.bicoin.info",
#     "User-Agent": "Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.92 Mobile Safari/537.36",
#     "Referer": "https://test1.bicoin.info/place-mgr/stock/state",
#     "Accept-Encoding": "gzip, deflate",
#     "Accept-Language": "zh-CN,en-US;q=0.8",
#     "X-Requested-With": "com.temperaturecoin"
# }


data = requests.get(hyd_url, headers=hyd_headers, verify=False).json()['data']

data.get('total')


