import re
import threading as th
from helpers import get_engine, get_databases_like
from log import Log
from sqlalchemy import Table, MetaData, inspect, func, text, select
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy_views import CreateView


class DbReplicator(th.Thread):
    def __init__(self, scheme, config, src_db, trg_db,
                 include_tables=[], exclude_tables=[], dynamic_tables=[], replicate_views=False, timestamp_column='Time'):
        super().__init__()
        self.scheme = scheme
        self.log = Log()
        self.scheme_conf = config
        self.src_db = src_db
        self.trg_db = trg_db
        self.include_tables = include_tables
        self.exclude_tables = exclude_tables
        self.dynamic_tables = dynamic_tables
        self.replicate_views = replicate_views
        self.timestamp_column = timestamp_column

        self.src_engine = get_engine(self.scheme_conf.source, self.src_db)
        self.trg_engine = get_engine(self.scheme_conf.target, self.trg_db)
        if not database_exists(self.trg_engine.url):
            create_database(self.trg_engine.url)
            self.log.info(
                f'Database {self.trg_db} was created', scheme=self.scheme)

    def _to_target_table(self, target_metadata, src_table):
        if src_table.name in target_metadata.tables:
            return target_metadata.tables[src_table.name]

        return src_table.tometadata(target_metadata)

    def _run_transaction(self, trg_conn, stmt, finish_msg, stmt_params=None):
        with trg_conn.begin() as transaction:
            try:
                trg_conn.execute(stmt, stmt_params)
            except Exception as e:
                transaction.rollback()
                self.log.error(e, scheme=self.scheme)
            else:
                transaction.commit()
                self.log.info(finish_msg, scheme=self.scheme)

    def _do_views(self, trg_conn, target_metadata, views):
        for v in views:
            trg_view = self._to_target_table(target_metadata, v)
            if not trg_view.exists():
                view_definition = inspect(self.src_engine) \
                    .get_view_definition(v.name)
                view_definition = view_definition[view_definition.lower().index(
                    'select'):]
                stmt = CreateView(trg_view, text(view_definition))
                stmt_msg = f'View {v.name} was created in {self.trg_db}'
                self._run_transaction(trg_conn, stmt, stmt_msg,)

    def _do_dynamic(self, trg_conn, target_metadata, dynamic_tables):
        for src_table in dynamic_tables:
            table = self._to_target_table(target_metadata, src_table)
            if table.exists():
                table.delete(None).execute()
            else:
                table.create()

            values = src_table.select().execute().fetchall()
            insert_stmt = table.insert(None)
            stmt_msg = f'{len(values)} record(s) were inserted into the dynamic table {self.trg_db}.{table.name}'
            self._run_transaction(trg_conn, insert_stmt,
                                  stmt_msg, stmt_params=values)

    def _do_include(self, trg_conn, target_metadata, time_tables):
        for src_table in time_tables:
            table = self._to_target_table(target_metadata, src_table)
            table.create(checkfirst=True)

            insert_stmt = table.insert(None)
            batch_nb = 0
            count = -1
            while True:
                count = select([func.count(table.c[self.timestamp_column])]).scalar() \
                    if count == -1 else count

                data_query = src_table \
                    .select(offset=count, limit=self.scheme_conf.batch_size) \
                    if count else src_table.select(limit=self.scheme_conf.batch_size)

                values = data_query.execute().fetchall()
                if not values:
                    break
                batch_nb += 1
                stmt_msg = f'Batch #{batch_nb}: {len(values)} record(s) were inserted into the table {self.trg_db}.{table.name} at offset {count}'
                self._run_transaction(
                    trg_conn, insert_stmt, stmt_msg, stmt_params=values)
                count += len(values)

    def run(self):
        with self.trg_engine.connect() as trg_connection:

            src_metadata = MetaData(bind=self.src_engine,)
            self.log.info(
                f'Reflecting source database {self.src_db}', scheme=self.scheme)
            src_metadata.reflect(views=self.replicate_views)
            src_views = inspect(self.src_engine).get_view_names()

            trg_metadata = MetaData(bind=self.trg_engine,)
            self.log.info(
                f'Reflecting target database {self.trg_db}', scheme=self.scheme)
            try:
                trg_metadata.reflect(views=self.replicate_views)
            except Exception as e:
                self.log.error(e, self.scheme)

            include_tables = src_metadata.tables.values()
            dynamic_tables = []
            exclude_tables = []

            if self.replicate_views:
                views = [src_metadata.tables[tab]
                         for tab in src_metadata.tables if tab in src_views]
                self._do_views(trg_connection, trg_metadata, views)

            if self.include_tables:
                include_tables = [src_metadata.tables[tab]
                                  for tab in src_metadata.tables
                                  if re.match(self.include_tables, tab)]

            if self.dynamic_tables:
                dynamic_tables = [src_metadata.tables[tab]
                                  for tab in src_metadata.tables
                                  if re.match(self.dynamic_tables, tab)]

            if self.exclude_tables:
                exclude_tables = [src_metadata.tables[tab]
                                  for tab in src_metadata.tables
                                  if re.match(self.exclude_tables, tab)]

            include_tables = [table for table in include_tables
                              if table not in exclude_tables and table not in dynamic_tables and table.name not in src_views]

            if dynamic_tables:
                self._do_dynamic(trg_connection,
                                 trg_metadata, dynamic_tables)

            if include_tables:
                self._do_include(trg_connection, trg_metadata, include_tables)


class SchemeReplicator:

    def __init__(self, scheme, config):
        self.config = config
        self.scheme = scheme

    def _get_db_name(self, db_conf, original):
        if not db_conf.naming_strategy or db_conf.naming_strategy == 'original':
            return original

        if db_conf.naming_strategy == 'exact':
            return db_conf.target

        if db_conf.naming_strategy == 'replace':
            return re.sub(db_conf.source, db_conf.target, original)

    def run(self):
        replicators = []

        for db_conf in self.config.databases:
            main_engine = get_engine(self.config.source)
            dbs = get_databases_like(main_engine, db_conf.source)
            for db in dbs:
                trg_db = self._get_db_name(db_conf, db)
                replicator = DbReplicator(self.scheme, self.config, db, trg_db, include_tables=db_conf.include_tables,
                                          exclude_tables=db_conf.exclude_tables, dynamic_tables=db_conf.dynamic_tables, replicate_views=db_conf.replicate_views, timestamp_column=db_conf.timestamp_column)
                replicators.append(replicator)
                replicator.start()

        return replicators
