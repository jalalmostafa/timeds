import json


class ConfigException(Exception):
    def __init__(self, scheme, message):
        self.message = message
        self.scheme = scheme

    def __str__(self):
        return f'[{self.scheme}] {self.message}'


class SchemeProperty:
    def __init__(self, full_name, propertyType, required, default='', linked=''):
        self.full_name = full_name
        self.type = propertyType
        self.required = required
        self.default = default
        self.linked = linked


class Scheme:
    _structure = {
        'host': SchemeProperty('Host name or IP', str, True,),
        'port': SchemeProperty('Server port', int, True,),
        'driver': SchemeProperty('Database type', ['mysql'], True,),
        'db': SchemeProperty('Database name', str, True,),
        'db_pattern': SchemeProperty('Database name pattern', str, True,),
        'username': SchemeProperty('Database user', str, True,),
        'password': SchemeProperty('Database user password', str, True,),
        'excludes': SchemeProperty('Excluded tables', list, False, default=[],),
        'just_like': SchemeProperty('Table names', list, False, default=[],),
        'recreate': SchemeProperty('No-timestamp tables (recreated on every sync operation)', list, False, default=[],),
        'timestamp_column': SchemeProperty('Timestamp column name', str, False, default='Time',),
    }

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

        if (isinstance(propertyType, list) and value not in propertyType) or type(value) is not propertyType:
            raise ConfigException(
                self.name, f'Invalid value: {name}. {full_name.lower()} is invalid!')

    def _check(self, raw_scheme):
        source = raw_scheme['source']
        if not source:
            raise ConfigException(
                self.name, 'Source database is not configured')

        if 'db' in source and 'db_pattern' in source:
            raise ConfigException(
                self.name, 'Only one option is allowed: \'db\' or \'db_pattern\'')

        for src_attr in source:
            prototype = self._structure[src_attr]
            self._property_check(src_attr, prototype, source[src_attr])

        target = raw_scheme['target']
        if not target:
            raise ConfigException(
                self.name, 'Target database is not configured')

        if 'db' in target and 'db_pattern' in target:
            raise ConfigException(
                self.name, 'Only one option is allowed: \'db\' or \'db_pattern\'')

        for target_attr in target:
            prototype = self._structure[target_attr]
            self._property_check(target_attr, prototype, source[target_attr])

        if ('db' in target and 'db_pattern' in source) or ('db_pattern' in target and 'db' in source):
            raise ConfigException(
                self.name, 'Mischievous Configiration: Either use db or db_pattern for both source and target databases')

        excludes = raw_scheme['excludes']
        self._property_check('excludes', self._structure['excludes'], excludes)

        just_like = raw_scheme['just_like']
        self._property_check(
            'just_like', self._structure['just_like'], just_like)

        recreate = raw_scheme['recreate']
        self._property_check('recreate', self._structure['recreate'], recreate)

        timestamp_column = raw_scheme['timestamp_column']
        self._property_check(
            'timestamp_column', self._structure['timestamp_column'], timestamp_column)

        return True


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
                    "driver": "mysql", Only mysql is supported now
                    "db": string,
                    "db_pattern": regex,
                    "username": string,
                    "password": string,
                },
                "target": {
                    "host": string,
                    "port": int,
                    "driver": "mysql", Only mysql is supported now
                    "db": string,
                    "db_pattern": regex,
                    "username": string,
                    "password": string,
                },
                "excludes": [regex ...],
                "just_like": [regex ...],
                "recreate": [regex ...],
                "timestamp_column": string | "Time"
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
