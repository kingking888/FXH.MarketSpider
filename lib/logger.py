
import logging
import logging.config
import os


class Logger:

    @staticmethod
    def get_logger(name):
        """
        获取日志器对象
        :param name:
        :return:
        """
        log_path = Logger.get_path()
        logging.config.fileConfig(log_path)
        return logging.getLogger(name)

    @staticmethod
    def get_path():
        """
        获取配置文件路径
        :return:
        """
        full_path = os.path.realpath(__file__)
        while os.sep in full_path:
            f = full_path.rsplit(os.sep, 1)
            dirlist = []
            for dirpath, dirname, filename in os.walk(f[0]):
                for i in filename:
                    dirlist.append(os.path.join(dirpath, i))
            for file in dirlist:
                if "logging.conf" in file:
                    return file
            full_path = f[0]
        return ""

if __name__ == "__main__":
    Logger.get_logger("trade_log")
