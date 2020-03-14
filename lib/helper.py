
from datetime import datetime
import time


class HelpTool:
    @staticmethod
    def get_vesrion(common_config):
        is_debug = common_config.get_value("isdebug")
        option = "debug"
        if is_debug != True:
            option = "release"
        return option

    @staticmethod
    def format_dingding(message):
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        info = "注意：异常信息\n时间：{0}\n异常：{1}".format(now_time, message)
        return info

    @staticmethod
    def get_timestamp():
        times = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        time_array = time.strptime(times, "%Y-%m-%d %H:%M:%S")
        time_stamp = int(time.mktime(time_array))
        return time_stamp