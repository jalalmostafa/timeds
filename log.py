import config as conf


class Log:

    def info(self, e, scheme=''):
        if isinstance(e, conf.ConfigException):
            print('[INFO]', '%s:' % (e.scheme), e.message)
        elif isinstance(e, str):
            if scheme:
                print('[INFO]', '%s: %s' % (scheme, e))
            else:
                print('[INFO]', e)

    def error(self, e, scheme=''):
        if isinstance(e, conf.ConfigException):
            print('[ERROR]', '%s:' % (e.scheme), e.message)
        elif isinstance(e, Exception):
            print('[ERROR]', scheme, e)
        elif isinstance(e, str):
            if scheme:
                print('[ERROR]', '%s: %s' % (scheme, e))
            else:
                print('[ERROR]', e)
