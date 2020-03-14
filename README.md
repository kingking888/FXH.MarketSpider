
## fxhspider

###### 注：默认 项目绝对路径 /root/FXH.MarketSpider/
    
##### 0. 运行环境搭建与配置
    1. 点击链接 https://www.anaconda.com/distribution，安装Anaconda Python3版本
    2. 通过 pip / conda 安装 ：loguru、redis、pyyaml、websocket-client
    3. 命令行输入 vi ~/.bash_profile 打开环境配置，输入 export PYTHONPATH=$PYTHONPATH:/root/FXH.MarketSpider/并保存退出，再通过命令行 source .bash_profile 生效，即配置好Python项目运行路径。
    
##### 1. 项目目录结构：

    - conf : 配置文件相关
        - common_conf: 设置存储数据的redis数据库信息
        - script_conf: 各交易所websocket采集参数配置信息
    - lib : 日志、递归堆栈溢出、配置文件读取处理相关
    - logs : 存储临时日志（实时日志存储于 spider/logs/ 下）

    - websocket_depth :交易所 合约深度数据采集
    - websocket_depth_spot : 交易所 现货深度数据采集
    - websocket_kline : 交易所 合约K线数据采集
    - websocket_kline_spot : 交易所 现货K线深度数据采集
    - websocket_trade : 交易所 合约实时成交数据采集
    - websocket_trade_spot : 交易所 现货实时成交数据采集

    - big_trade_btc : 大额转账 与 交易所24小时净流入数据采集
    - btc_halve : BTC减半行情相关数据采集
    - contract_open_interest : 各交易所持仓量 与 OKEx多空人数比采集
    - difference_in_price : 各交易所合约与现货价差 采集
    - liquidation : 交易所爆仓数据采集

    - spider ： 该目录下保存了各个爬虫执行的shell脚本，将通过 nohup 命令进行后台运行，并保存日志到 spider/logs/下

##### 2. 执行指定爬虫
    - 进入 spider/目录，如需启动某个爬虫，通过 ./ 执行即可，如 ./okex_spider 即可启动OKEx合约数据采集。

##### 3. 查看爬虫执行状态/停止爬虫
    - 进入 spider/目录，通过 ./spider 即可查看当前系统中正在运行的采集程序，可通过 kill PID 进行终止。
