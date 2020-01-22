import config as conf


class Log:

    def info(self, e, scheme=''):
        if isinstance(e, conf.ConfigException):
            print('[INFO]', f'{e.scheme}:', e.message)
        elif isinstance(e, str):
            if scheme:
                print('[INFO]', f'{scheme}: {e}')
            else:
                print('[INFO]', e)

    def error(self, e, scheme=''):
        if isinstance(e, conf.ConfigException):
            print('[ERROR]', f'{e.scheme}:', e.message)
        elif isinstance(e, Exception):
            print('[ERROR]', f'{scheme}', e)
        elif isinstance(e, str):
            if scheme:
                print('[ERROR]', f'{scheme}: {e}')
            else:
                print('[ERROR]', e)
