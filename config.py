import json


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

root_structure = {
    'source': SchemeProperty('Source server', dict, True),
    'target': SchemeProperty('Target server', dict, True),
    'source_databases': SchemeProperty('Source database name', str, True,),
    'target_database': SchemeProperty('Target database name', str, False,),
    'exclude_tables': SchemeProperty('Excluded tables', str, False, default=[],),
    'just_like_tables': SchemeProperty('Table names', str, False, default=[],),
    'recreate_tables': SchemeProperty('No-timestamp tables (recreated on every sync operation)', list, False, default=[],),
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
            elif type(value) is not propertyType:
                raise ConfigException(
                    self.name, f'Invalid value: {name}. {full_name.lower()} is invalid!')

    def _server_check(self, raw_scheme, server_key):
        server = raw_scheme[server_key]
        prototype = root_structure[server_key]
        if not server:
            raise ConfigException(
                self.name, f'{prototype.full_name} is not configured')

        for attr in server:
            prototype = db_structure.get(attr, None)
            if prototype is None:
                raise ConfigException(
                    self.name, f'Unrecognized configuration option: {attr}')
            self._property_check(attr, prototype, server[attr])

    def _check(self, raw_scheme):
        self._server_check(raw_scheme, 'source')
        self._server_check(raw_scheme, 'target')

        source_databases = raw_scheme.get('source_databases', None)
        self._property_check(
            'source_databases', root_structure['source_databases'], source_databases)

        target_database = raw_scheme.get('target_database', None)
        self._property_check(
            'target_database', root_structure['target_database'], target_database)

        excludes = raw_scheme.get('exclude_tables', None)
        self._property_check(
            'exclude_tables', root_structure['exclude_tables'], excludes)

        just_like = raw_scheme.get('just_like_tables', None)
        self._property_check(
            'just_like_tables', root_structure['just_like_tables'], just_like)

        recreate = raw_scheme.get('recreate_tables', None)
        self._property_check(
            'recreate_tables', root_structure['recreate_tables'], recreate)

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
                "source_databases": regex,
                "target_database": string,
                "exclude_tables": regex,
                "just_like_tables": regex,
                "recreate_tables": regex,
                "timestamp_column": string | "Time",
                "batch_size": 100000
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
