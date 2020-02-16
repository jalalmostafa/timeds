import config as conf
import logging as log
import sys


class Log:

    def __init__(self):
        log.basicConfig(stream=sys.stdout, level=log.INFO,
                        format='[%(asctime)s] [%(levelname)s] [%(scheme)s] [%(db)s]: %(message)s')

    def info(self, msg, **kwargs):
        log.info(msg, extra=self._construct_params(kwargs))

    def error(self, msg, **kwargs):
        log.error(msg, extra=self._construct_params(kwargs))

    def exception(self, e, **kwargs):
        log.exception(e, extra=self._construct_params(kwargs))

    def batch_dynamic(self, count, time, table_name, **kwargs):
        log.info('%s record(s) were inserted into the dynamic table %s in %s sec' % (
            count, table_name, int(time)), extra=self._construct_params(kwargs))

    def batch_include(self, batch_nb, count, table_name, latest, time, read_time, write_time, **kwargs):
        log.info('Batch #%s: %s records were inserted into [%s] at %s. Total: %s sec (read=%s, write=%s)' % (
            batch_nb, count, table_name, latest, int(time), int(read_time), int(write_time)), extra=self._construct_params(kwargs))

    def database_created(self, **kwargs):
        log.info('Database was created', extra=self._construct_params(kwargs))

    def view_created(self, view_name, **kwargs):
        log.info('View %s was created', extra=self._construct_params(kwargs))

    def dynamic_recreated(self, table_name, **kwargs):
        log.info('(re)creating dynamic table %s' %
                 (table_name), extra=self._construct_params(kwargs))

    def reflecting_source(self, **kwargs):
        log.info('Reflecting source database',
                 extra=self._construct_params(kwargs))

    def reflecting_target(self, **kwargs):
        log.info('Reflecting target database',
                 extra=self._construct_params(kwargs))

    def bootstrapped_with(self, stmt, **kwargs):
        log.info('Bootstrapped server with %s' %
                 (stmt), extra=self._construct_params(kwargs))

    def _construct_params(self, kwargs):
        return {
            'scheme': kwargs.get('scheme', None) or '',
            'db': kwargs.get('db', None) or ''
        }
