import re
from sqlalchemy import create_engine, inspect
from connectors import connectors, dialect_options


def create_connection_string(driver, host, port, username, password, db=''):
    if not db:
        return '%s+%s://%s:%s@%s:%s/INFORMATION_SCHEMA' % (driver, connectors[driver], username, password, host, port)

    return '%s+%s://%s:%s@%s:%s/%s' % (driver, connectors[driver], username, password, host, port, db)


def get_engine(host_conf, db_name='', **kwargs):
    main_conn_string = create_connection_string(
        host_conf.driver, host_conf.host, host_conf.port,
        host_conf.username, host_conf.password, db=db_name)
    return create_engine(main_conn_string, **kwargs)


def get_databases_like(engine, regex):
    schemas = inspect(engine).get_schema_names()
    return [schema for schema in schemas if re.match(regex, schema)]


def get_dialect_options(driver):
    options = dialect_options[driver]
    return {'%s_%s' % (driver, key): value for key, value in options.items()}


def get_db_tables(engine, db_name):
    return inspect(engine).get_table_names(schema=db_name)
