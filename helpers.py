import re
from sqlalchemy import create_engine, inspect
from connectors import connectors


def create_connection_string(driver, host, port, username, password, db=''):
    if not db:
        return f'{driver}+{connectors[driver]}://{username}:{password}@{host}:{port}/INFORMATION_SCHEMA'

    return f'{driver}+{connectors[driver]}://{username}:{password}@{host}:{port}/{db}'


def get_engine(host_conf, db_name='', **kwargs):
    main_conn_string = create_connection_string(
        host_conf.driver, host_conf.host, host_conf.port,
        host_conf.username, host_conf.password, db=db_name)
    return create_engine(main_conn_string, **kwargs)


def get_databases_like(engine, regex):
    schemas = inspect(engine).get_schema_names()
    return [schema for schema in schemas if re.match(regex, schema)]


def get_db_tables(engine, db_name):
    return inspect(engine).get_table_names(schema=db_name)
