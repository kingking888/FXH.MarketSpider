# 钉钉告警

from dingtalkchatbot.chatbot import DingtalkChatbot


class Notify:
    @staticmethod
    def send(url, info, func_format=None):
        """
        将消息发给钉钉
        :param url: 钉钉机器人url
        :param info: 消息
        :param func_format: 对消息进行格式化处理
        :return:
        """
        sender = DingtalkChatbot(url)
        if func_format:
            info = func_format(info)

        sender.send_text(msg=info, is_at_all=True)
