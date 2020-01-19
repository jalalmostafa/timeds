import re
from sqlalchemy import create_engine, inspect
from connectors import connectors

def create_connection_string(driver, host, port, username, password, db=''):
    if not db:
        return f'{driver}+{connectors[driver]}://{username}:{password}@{host}:{port}/INFORMATION_SCHEMA'

    return f'{driver}+{connectors[driver]}://{username}:{password}@{host}:{port}/{db}'


def get_databases_like(engine, regex):
    schemas = inspect(engine).get_schema_names()
    return [schema for schema in schemas if re.match(regex, schema)]


def get_db_tables(engine, db_name):
    return inspect(engine).get_table_names(schema=db_name)
