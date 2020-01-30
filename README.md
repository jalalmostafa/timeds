# timeds

Replicates insert-only tables, views and dynamic tables (through table recreation).

## Requirements

- Python 3.5

### Python Packages

- SQLAlchemy
- sqlalchemy-utils
- sqlalchemy-views
- pid
- pymysql

### Source2Image

Environment variables:

- `CONFIG_FILE` (required). Path to configuration file
- `SLEEP_TIME` (required). Sleep time in seconds between iteration and another

## Config File

```json
{
    "replication_scheme_name": {
        "source": {
            "host": string,
            "port": int,
            "driver": "mysql",
            "username": string,
            "password": string,
        },
        "target": {
            "host": string,
            "port": int,
            "driver": "mysql",
            "username": string,
            "password": string,
        },
        "databases": [{
            "source": regex,
            "target"?: string,
            "naming_strategy"?: "replace" | "exact" | "original",
            "exclude_tables"?: regex,
            "include_tables"?: regex,
            "dynamic_tables"?: regex
        }],
        "batch_size"?: 100000
    },
    "replication_scheme_name": { ... }
}
```

## Notes

- Do not forget to set dynamic tables in `dynamic_tables` or `exclude_tables` to prevent replicating them on automatic replication runs
- Use `--only-dynamic-and-views` or `-d` to replicate dynamic views on a manual fashion
