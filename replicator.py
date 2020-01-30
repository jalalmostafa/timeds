import re
import threading as th
from helpers import get_engine, get_databases_like
from log import Log
from sqlalchemy import MetaData, inspect, text
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy_views import CreateView


class DbReplicator(th.Thread):
    def __init__(self, scheme, config, src_db, trg_db, only_dynamic_and_views=False,
                 include_tables=[], exclude_tables=[], dynamic_tables=[],):
        super().__init__()
        self.only_dynamic_and_views = only_dynamic_and_views
        self.scheme = scheme
        self.log = Log()
        self.scheme_conf = config
        self.src_db = src_db
        self.trg_db = trg_db
        self.include_tables = include_tables
        self.exclude_tables = exclude_tables
        self.dynamic_tables = dynamic_tables

        self.src_engine = get_engine(self.scheme_conf.source, self.src_db)
        self.trg_engine = get_engine(self.scheme_conf.target, self.trg_db)
        if not database_exists(self.trg_engine.url):
            create_database(self.trg_engine.url)
            self.log.info(
                'Database %s was created' % (self.trg_db), scheme=self.scheme)

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
                select_index = view_definition.lower().index('select')
                view_definition = view_definition[select_index:]
                stmt = CreateView(trg_view, text(view_definition))
                stmt_msg = 'View %s was created in %s' % (v.name, self.trg_db)
                self._run_transaction(trg_conn, stmt, stmt_msg,)

    def _do_dynamic(self, trg_conn, target_metadata, dynamic_tables):
        for src_table in dynamic_tables:
            table = self._to_target_table(target_metadata, src_table)
            if table.exists():
                table.drop()
            self.log.info(
                '(re)creating dynamic table %s.%s' % (self.trg_db, table.name))
            table.create()

            values = src_table.select().execute()
            with trg_conn.begin() as transaction:
                try:
                    for v in values:
                        stmt = table.insert(None).values(v)
                        trg_conn.execute(stmt,)
                except Exception as e:
                    transaction.rollback()
                    self.log.error(e, scheme=self.scheme)
                else:
                    transaction.commit()
                    self.log.info(
                        '%s record(s) were inserted into the dynamic table %s.%s' % (
                            values.rowcount, self.trg_db, table.name),
                        scheme=self.scheme)

    def _do_include(self, trg_conn, target_metadata, time_tables):
        for src_table in time_tables:
            table = self._to_target_table(target_metadata, src_table)
            table.create(checkfirst=True)

        for src_table in time_tables:
            table = self._to_target_table(target_metadata, src_table)

            batch_nb = 0
            count = -1
            while True:
                count = table.count().scalar() if count == -1 else count

                data_query = src_table \
                    .select(offset=count, limit=self.scheme_conf.batch_size) \
                    if count else src_table.select(limit=self.scheme_conf.batch_size)

                values = data_query.execute()

                if not values.rowcount:
                    break

                with trg_conn.begin() as transaction:
                    try:
                        for v in values:
                            stmt = table.insert(None).values(v)
                            trg_conn.execute(stmt,)
                    except Exception as e:
                        transaction.rollback()
                        self.log.error(e, scheme=self.scheme)
                    else:
                        transaction.commit()
                        self.log.info(
                            'Batch #%s: %s record(s) were inserted into the table %s.%s at offset %s' % (batch_nb, values.rowcount, self.trg_db, table.name, count), scheme=self.scheme)

                batch_nb += 1
                count += values.rowcount

    def run(self):
        with self.trg_engine.connect() as trg_connection:
            src_metadata = MetaData(bind=self.src_engine,)
            self.log.info(
                'Reflecting source database %s' % (self.src_db), scheme=self.scheme)
            src_metadata.reflect(views=self.only_dynamic_and_views)

            trg_metadata = MetaData(bind=self.trg_engine,)

            src_views = inspect(self.src_engine).get_view_names()
            include_tables = src_metadata.tables.values()
            dynamic_tables = []
            exclude_tables = []

            self.log.info(
                'Reflecting target database %s' % (self.trg_db), scheme=self.scheme)
            try:
                trg_metadata.reflect(views=self.only_dynamic_and_views)
            except Exception as e:
                self.log.error(e, self.scheme)

            if self.dynamic_tables:
                dynamic_tables = [tab for tab in include_tables
                                  if re.match(self.dynamic_tables, tab) and tab not in src_views]
                if self.only_dynamic_and_views:
                    self._do_dynamic(
                        trg_connection, trg_metadata, dynamic_tables)

                    views = [tab for tab in include_tables if tab in src_views]
                    self._do_views(trg_connection, trg_metadata, views)

            if self.include_tables:
                include_tables = [tab for tab in include_tables
                                  if re.match(self.include_tables, tab)]

            if self.exclude_tables:
                exclude_tables = [tab for tab in include_tables
                                  if re.match(self.exclude_tables, tab)]

            include_tables = [table for table in include_tables
                              if table not in exclude_tables and table not in dynamic_tables and table.name not in src_views]

            if include_tables:
                self._do_include(
                    trg_connection, trg_metadata, include_tables)


class SchemeReplicator:

    def __init__(self, scheme, config, only_dynamic_and_views=False):
        self.config = config
        self.scheme = scheme
        self.only_dynamic_and_views = only_dynamic_and_views

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
                replicator = DbReplicator(self.scheme, self.config, db, trg_db, only_dynamic_and_views=self.only_dynamic_and_views,
                                          include_tables=db_conf.include_tables, exclude_tables=db_conf.exclude_tables, dynamic_tables=db_conf.dynamic_tables,)
                replicators.append(replicator)
                replicator.start()

        return replicators
