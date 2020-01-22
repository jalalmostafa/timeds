import re
import threading as th
from helpers import get_engine, get_databases_like
from log import Log
from sqlalchemy import Table, MetaData, inspect, func
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy_views import CreateView


class DbReplicator(th.Thread):
    def __init__(self, scheme, config, src_db, trg_db='',
                 include_tables=[], exclude_tables=[], dynamic_tables=[], replicate_views=False, timestamp_column='Time'):
        self.scheme = scheme
        self.log = Log()
        self.scheme_conf = config
        self.src_db = src_db
        self.trg_db = trg_db if trg_db else src_db
        self.include_tables = include_tables
        self.exclude_tables = exclude_tables
        self.dynamic_tables = dynamic_tables
        self.replicate_views = replicate_views
        self.timestamp_column = timestamp_column

        self.src_engine = get_engine(self.scheme_conf.source, self.src_db)
        self.trg_engine = get_engine(self.scheme_conf.target, self.trg_db)
        if not database_exists(self.trg_engine.url):
            create_database(self.trg_engine.url)

    def _to_target_table(self, target_metadata, src_table):
        table = Table(src_table.name, target_metadata,
                      src_table.columns, keep_existing=True, extend_existing=True)
        return table

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
            view_definition = inspect(self.src_engine) \
                .get_view_definition(v.name)

            stmt = CreateView(trg_view, view_definition)
            stmt_msg = f'View {v.name} was created in {self.trg_db}'
            self._run_transaction(trg_conn, stmt, stmt_msg,)

    def _do_dynamic(self, src_conn, trg_conn, target_metadata, dynamic_tables):
        for src_table in dynamic_tables:
            table = self._to_target_table(target_metadata, src_table)
            if table.exists():
                table.delete(None)
            else:
                table.create(checkfirst=True)

            select_all_query = src_table.select()
            insert_stmt = table.insert(None)
            with src_conn.execute(select_all_query) as result:
                values = result.fetchall()
                stmt_msg = f'{len(values)} record(s) were inserted into a dynamic table {self.trg_db}.{table.name}'
                self._run_transaction(
                    trg_conn, insert_stmt, stmt_msg, stmt_params=values)

    def _do_include(self, src_conn, trg_conn, target_metadata, time_tables):
        for src_table in time_tables:
            table = self._to_target_table(target_metadata, table)
            table.create(checkfirst=True)

            insert_stmt = table.insert(None)
            while True:
                max_time_stmt = table \
                    .select(func.max(table.c[self.timestamp_column]))
                max_time = src_conn.execute(max_time_stmt).scalar()

                data_query = src_table \
                    .select(whereclause=src_table.c[self.timestamp_column] > max_time, limit=self.scheme_conf.batch_size) \
                    if max_time else src_table.select(limit=self.scheme_conf.batch_size)

                with src_conn.execute(data_query) as result:
                    values = result.fetchall()
                    if not values:
                        break

                    stmt_msg = f'{len(values)} record(s) were inserted into a dynamic table {self.trg_db}.{table.name}'
                    self._run_transaction(
                        trg_conn, insert_stmt, stmt_msg, stmt_params=values)

    def run(self):
        with self.src_engine.connect() as src_connection, self.trg_engine.connect() as trg_connection:

            src_metadata = MetaData(bind=self.src_engine,)
            src_metadata.reflect(views=True)

            src_views = inspect(self.src_engine).get_view_names()

            trg_metadata = MetaData(bind=self.trg_engine,)
            trg_metadata.reflect(views=True)

            include_tables = src_metadata.tables.values()
            dynamic_tables = []
            exclude_tables = []

            if self.replicate_views:
                views = [src_metadata.tables[tab]
                         for tab in src_metadata.tables if tab in views]
                self._do_views(trg_connection, trg_metadata, views)

            if self.include_tables:
                include_tables = [src_metadata.tables[tab]
                                  for tab in src_metadata.tables
                                  if re.match(self.include_tables, tab.name)]

            if self.dynamic_tables:
                dynamic_tables = [src_metadata.tables[tab]
                                  for tab in src_metadata.tables
                                  if re.match(self.dynamic_tables, tab.name)]

            if self.exclude_tables:
                exclude_tables = [src_metadata.tables[tab]
                                  for tab in src_metadata.tables
                                  if re.match(self.exclude_tables, tab.name)]

            include_tables = [table for table in include_tables
                              if table not in exclude_tables and table not in dynamic_tables and table.name not in src_views]

            if dynamic_tables:
                self._do_dynamic(src_connection, trg_connection,
                                 trg_metadata, dynamic_tables)

            if include_tables:
                self._do_include(src_connection, trg_connection,
                                 trg_metadata, include_tables)


class SchemeReplicator:

    def __init__(self, scheme, config):
        self.config = config
        self.scheme = scheme
        self.replicators = []

    def run(self):
        for db_conf in self.config.databases:
            main_engine = get_engine(self.config.source)
            dbs = get_databases_like(main_engine, db_conf.source)
            for db in dbs:
                if db_conf.target:
                    trg_db = re.sub(db_conf.source, db_conf.target, db)
                replicator = DbReplicator(self.scheme, self.config, db,
                                          trg_db=trg_db, include_tables=db_conf.include_tables,
                                          exclude_tables=db_conf.exclude_tables, dynamic_tables=db_conf.dynamic_tables, replicate_views=db_conf.replicate_views, timestamp_column=db_conf.timestamp_column)
                self.replicators.append(replicator)
                replicator.start()

        return self.replicators
