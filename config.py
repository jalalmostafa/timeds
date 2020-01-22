import json
from sqlalchemy import create_engine
from connectors import connectors, supported_dbs
from helpers import create_connection_string, get_databases_like


class ConfigException(Exception):
    def __init__(self, scheme, message):
        self.message = message
        self.scheme = scheme

    def __str__(self):
        return f'[{self.scheme}] {self.message}'


class SchemeProperty:
    def __init__(self, full_name, propertyType, required, child_type='', default=''):
        self.full_name = full_name
        self.type = propertyType
        self.required = required
        self.default = default
        self.child_type = child_type

    def __str__(self):
        return f'(full_name={self.full_name}, type={self.type}, required={self.required}, default={self.default})'


host_structure = {
    'host': SchemeProperty('Host name or IP', str, True,),
    'port': SchemeProperty('Server port', int, True,),
    'driver': SchemeProperty('Database type', supported_dbs, True,),
    'username': SchemeProperty('Database user', str, True,),
    'password': SchemeProperty('Database user password', str, True,),
}

db_structure = {
    'source': SchemeProperty('Source database name', str, True,),
    'target': SchemeProperty('Target database name', str, False,),
    'timestamp_column': SchemeProperty('Timestamp column name', str, False, default='Time',),
    'exclude_tables': SchemeProperty('Excluded tables', str, False, default=[],),
    'include_tables': SchemeProperty('Table names', str, False, default=[],),
    'dynamic_tables': SchemeProperty('No-timestamp tables (recreated on every sync operation)', list, False, default=[],),
    'replicate_views': SchemeProperty('Views replication', bool, False, default=False,),
}

root_structure = {
    'source': SchemeProperty('Source server', host_structure, True),
    'target': SchemeProperty('Target server', host_structure, True),
    'batch_size': SchemeProperty('Batch size', int, False, default=100000,),
    'databases': SchemeProperty('Source and target databases', list, True, child_type=SchemeProperty('database', db_structure, True))
}


class Scheme:
    def __init__(self, name, scheme_dict):
        self.name = name
        conf = self._check(scheme_dict)
        self.conf = ConfigDict(conf)

    def __getattr__(self, name):
        return self.conf[name]

    def __str__(self):
        return '\n'.join([f'        {name}:\n{self.conf[name]}' for name in self.conf])

    def _property_check(self, name, prototype, value):

        if not prototype:
            raise ConfigException(self.name, f'Unrecognized option {name}')

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
                    value[attr] = self._property_check(
                        attr, propertyType.get(attr, None), value.get(attr, None))
            elif type(value) is not propertyType:
                raise ConfigException(
                    self.name, f'Invalid value: {name}. {full_name.lower()} is invalid!')
            elif prototype.child_type:
                for i, v in enumerate(value):
                    value[i] = self._property_check(
                        name, prototype.child_type, v)
            return value

        return prototype.default

    def _check(self, raw_scheme):
        return {
            k: self._property_check(k, root_structure[k], raw_scheme.get(k, None)) for k in root_structure
        }


class ConfigDict:
    def __init__(self, data):
        self.dict = data
        for key, value in self.dict.items():
            if isinstance(value, dict):
                self.dict[key] = ConfigDict(value)
            elif isinstance(value, list):
                for idx, elm in enumerate(value):
                    if isinstance(elm, dict):
                        self.dict[key][idx] = ConfigDict(elm)

    def __iter__(self):
        return iter(self.dict)

    def __getitem__(self, key):
        return self.dict[key]

    def __getattr__(self, name):
        return self.dict[name]

    def __str__(self):
        return '\n'.join([f'            {name}: {self.dict[name]}' for name in self.dict])


class Config:
    """
    Sample configuration file:
        {
            "replication_scheme_name": {
                "source": {
                    "host": string,
                    "port": int,
                    "driver": string,
                    "username": string,
                    "password": string,
                },
                "target": {
                    "host": string,
                    "port": int,
                    "driver": string,
                    "username": string,
                    "password": string,
                },
                "databases": [{
                    "source": regex,
                    "target"?: string,
                    "timestamp_column"?: string | "Time",
                    "exclude_tables"?: regex,
                    "include_tables"?: regex,
                    "dynamic_tables"?: regex,
                    "replicate_views"?: boolean
                }],
                "batch_size"?: 100000
            }
        }
    """

    def __init__(self, file_name):
        with open(file_name) as conf_file:
            conf = json.load(conf_file) or {}
            self.conf = {}
            for scheme_name in conf:
                self.conf[scheme_name] = Scheme(scheme_name, conf[scheme_name])

    def __iter__(self):
        return iter(self.conf)

    def __getitem__(self, key):
        return self.conf[key]

    def __str__(self):
        return "Replication Schemes:" + '\n'.join([f"    {name}:\n{self.conf[name]}" for name in self.conf])
