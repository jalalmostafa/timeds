from sqlalchemy import create_engine
import json
from helpers import create_connection_string, get_databases_like


class ConfigException(Exception):
    def __init__(self, scheme, message):
        self.message = message
        self.scheme = scheme

    def __str__(self):
        return f'[{self.scheme}] {self.message}'


class SchemeProperty:
    def __init__(self, full_name, propertyType, required, default=''):
        self.full_name = full_name
        self.type = propertyType
        self.required = required
        self.default = default

    def __str__(self):
        return f'(full_name={self.full_name}, type={self.type}, required={self.required}, default={self.default})'


db_structure = {
    'host': SchemeProperty('Host name or IP', str, True,),
    'port': SchemeProperty('Server port', int, True,),
    'driver': SchemeProperty('Database type', ['mysql'], True,),
    'username': SchemeProperty('Database user', str, True,),
    'password': SchemeProperty('Database user password', str, True,),
}

source_strucutre = {**db_structure,
                    'db_pattern': SchemeProperty('Source database name', str, True,),
                    }

target_structure = {**db_structure,
                    'db': SchemeProperty('Target database name', str, False,),
                    }

root_structure = {
    'source': SchemeProperty('Source server', source_strucutre, True),
    'target': SchemeProperty('Target server', target_structure, True),
    'exclude_tables': SchemeProperty('Excluded tables', str, False, default=[],),
    'include_tables': SchemeProperty('Table names', str, False, default=[],),
    'dynamic_tables': SchemeProperty('No-timestamp tables (recreated on every sync operation)', list, False, default=[],),
    'timestamp_column': SchemeProperty('Timestamp column name', str, False, default='Time',),
    'batch_size': SchemeProperty('Batch size', int, False, default=100000,),
}


class Scheme:
    def __init__(self, name, scheme_dict):
        self.name = name
        self._check(scheme_dict)
        self.conf = ConfigDict(scheme_dict)

    def __getattr__(self, name):
        return self.conf

    def __str__(self):
        return ''.join([f'        {name}: {self.conf[name]}\n' for name in self.conf])

    def _property_check(self, name, prototype, value):
        full_name = prototype.full_name
        propertyType = prototype.type

        if prototype.required and not value:
            raise ConfigException(
                self.name, f'Missing value: {name}. {full_name} is required!')

        if value:
            if isinstance(propertyType, list):
                if value not in propertyType:
                    raise ConfigException(
                        self.name, f'Invalid value: {name}. {full_name.lower()} is invalid!')
            elif isinstance(propertyType, dict):
                if not value:
                    raise ConfigException(
                        self.name, f'Invalid value: {name}. {full_name.lower()} is invalid!')

                for attr in propertyType:
                    prototype = propertyType[attr]
                    self._property_check(
                        attr, prototype, value.get(attr, None))

            elif type(value) is not propertyType:
                raise ConfigException(
                    self.name, f'Invalid value: {name}. {full_name.lower()} is invalid!')

    def _check(self, raw_scheme):
        self._property_check(
            'source', root_structure['source'], raw_scheme['source'])

        source = raw_scheme['source']
        src_conn_string = create_connection_string(
            source['driver'], source['host'], source['port'], source['username'], source['password'])
        src_engine = create_engine(src_conn_string)
        src_databases = get_databases_like(src_engine, source.db_pattern)

        if len(src_databases) == 0:
            raise ConfigException(
                self.name, f'Source database pattern matches 0 databases')

        self._property_check(
            'target', root_structure['target'], raw_scheme['target'])

        if 'db' in raw_scheme['target'] and len(src_databases) != 1:
            raise ConfigException(
                self.scheme, f'Source regex {source.db_pattern} matches {len(src_databases)} databases in target. "target.db" option should not be there')

        exclude_tables = raw_scheme.get('exclude_tables', None)
        self._property_check(
            'exclude_tables', root_structure['exclude_tables'], exclude_tables)

        include_tables = raw_scheme.get('include_tables', None)
        self._property_check(
            'include_tables', root_structure['include_tables'], include_tables)

        dynamic_tables = raw_scheme.get('dynamic_tables', None)
        self._property_check(
            'dynamic_tables', root_structure['dynamic_tables'], dynamic_tables)

        timestamp_column = raw_scheme.get('timestamp_column', None)
        prototype = root_structure['timestamp_column']
        if timestamp_column is not None:
            self._property_check('timestamp_column',
                                 prototype, timestamp_column)
        else:
            raw_scheme['timestamp_column'] = prototype.default

        batch_size = raw_scheme.get('batch_size', None)
        prototype = root_structure['batch_size']
        if batch_size is not None:
            self._property_check('batch_size',
                                 prototype, batch_size)
        else:
            raw_scheme['batch_size'] = prototype.default


class ConfigDict:
    def __init__(self, data):
        self.dict = data
        for key in self.dict:
            if type(self.dict[key]) is dict:
                self.dict[key] = ConfigDict(self.dict[key])

    def __iter__(self):
        return iter(self.dict)

    def __getitem__(self, key):
        return self.dict[key]

    def __getattr__(self, name):
        return self.dict[name]

    def __str__(self):
        return ''.join([f'{name}: {self.dict[name]}\n' for name in self.dict])


class Config:
    """
    Sample configuration file:
        {
            "replication_scheme_name": {
                "source": {
                    "host": string,
                    "port": int,
                    "driver": string,
                    "db_pattern": regex,
                    "username": string,
                    "password": string,
                },
                "target": {
                    "host": string,
                    "port": int,
                    "driver": string,
                    "db"?: string,
                    "username": string,
                    "password": string,
                },
                "exclude_tables"?: regex,
                "include_tables"?: regex,
                "dynamic_tables"?: regex,
                "timestamp_column"?: string | "Time",
                "batch_size"?: 100000
            }
        }
    """

    def __init__(self, file_name):
        with open(file_name) as conf_file:
            self.conf = json.load(conf_file) or []
            for scheme_name in self.conf:
                self.conf[scheme_name] = Scheme(
                    scheme_name, self.conf[scheme_name])

    def __iter__(self):
        return iter(self.conf)

    def __getitem__(self, key):
        return self.conf[key]

    def __str__(self):
        return "Replication Schemes:\n" + ''.join([f"    {name}:\n{self.conf[name]}" for name in self.conf])
