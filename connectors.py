# dialect => connector package
connectors = {
    'mysql': 'pymysql'
}

# dialect => dict(option: value)
dialect_options = {
    'mysql': {
        'engine': 'InnoDB'
    }
}

supported_dbs = [db for db in connectors.keys()]
