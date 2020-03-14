import yaml

class Config:
    def __init__(self, path):
        # 读取yaml文件并转为json数据
        self._conf = open(path, 'rb')
        self._json = yaml.load(self._conf)

    def __del__(self):
        self._close()

    def __enter__(self):
        return self

    def __exit__(self, typ, val, tb):
        self._close()

    def _close(self):
        self._conf.close()

    def get_value(self, *args):
        """获取配置内容
        """
        if not args:
            return self._json
        is_tree_style = repr(args).find(r'/') != -1
        if is_tree_style and len(args) > 1:
            raise ValueError('args error')
        elif is_tree_style and len(args) == 1:
            params = tuple(args[0].split(r'/'))
        else:
            params = args
        d = self._json
        for p in params:
            d = self._get(p, d)
            if not d:
                break
        return d

    def _get(self, key, d):
        return d.get(key, None)

if __name__ == "__main__":
    pass