import re
import threading as th
import time
from helpers import get_engine, get_databases_like, get_dialect_kwargs
from log import Log
from sqlalchemy import MetaData, inspect, text, exc, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy_views import CreateView


class DbReplicator(th.Thread):
    def __init__(self, scheme, config, src_db, trg_db, only_dynamic_and_views=False,
                 include_tables=[], exclude_tables=[], dynamic_tables=[], order_by='',):
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
        self.order_by = order_by

        self.src_engine = get_engine(
            self.scheme_conf.source, self.src_db, pool_recycle=7200)
        self.trg_engine = get_engine(
            self.scheme_conf.target, self.trg_db, pool_recycle=7200)
        self.TargetSession = sessionmaker(bind=self.trg_engine)
        self.dialect_kwargs = get_dialect_kwargs(
            self.scheme_conf.target.driver)

        if not database_exists(self.trg_engine.url):
            create_database(self.trg_engine.url)
            self.log.database_created(scheme=self.scheme, db=self.trg_db)

    def _to_target_table(self, target_metadata, src_table):
        if src_table.name in target_metadata.tables:
            return target_metadata.tables[src_table.name]

        table = src_table.tometadata(target_metadata)
        for key, value in self.dialect_kwargs.items():
            table.dialect_kwargs[key] = value

        return table

    def _run_transaction(self, session, stmt, stmt_params=None):
        try:
            session.execute(stmt, stmt_params)
        except Exception as e:
            session.rollback()
            raise e
        else:
            session.commit()
        finally:
            session.close()

    def _run_target_transaction(self, stmt, stmt_params=None):
        session = self.TargetSession()
        self._run_transaction(session, stmt, stmt_params=stmt_params)

    def _do_views(self, target_metadata, views):
        for v in views:
            trg_view = self._to_target_table(target_metadata, v)
            if not trg_view.exists():
                view_definition = inspect(self.src_engine) \
                    .get_view_definition(v.name)
                select_index = view_definition.lower().index('select')
                view_definition = view_definition[select_index:]
                stmt = CreateView(trg_view, text(view_definition))
                try:
                    self._run_target_transaction(stmt,)
                except Exception as e:
                    self.log.exception(e, scheme=self.scheme, db=self.trg_db)
                else:
                    self.log.view_created(
                        v.name, scheme=self.scheme, db=self.trg_db)

    def _do_dynamic(self, target_metadata, dynamic_tables):
        session = self.TargetSession()
        for src_table in dynamic_tables:
            table = self._to_target_table(target_metadata, src_table)
            if table.exists():
                table.drop()
            self.log.dynamic_recreated(
                table.name, scheme=self.scheme, db=self.trg_db)
            table.create()

            values = src_table.select().execute()
            start = time.time()
            try:
                for v in values:
                    stmt = table.insert(None).values(v)
                    session.execute(stmt,)
            except Exception as e:
                session.rollback()
                self.log.exception(e, scheme=self.scheme, db=self.trg_db)
            else:
                session.commit()
                end = time.time()
                self.log.batch_dynamic(
                    values.rowcount, end - start, table.name, scheme=self.scheme, db=self.trg_db)
            finally:
                session.close()

    def _do_include(self, target_metadata, time_tables):
        for src_table in time_tables:
            table = self._to_target_table(target_metadata, src_table)
            table.create(checkfirst=True)

        batch_nb = 1
        for src_table in time_tables:
            table = self._to_target_table(target_metadata, src_table)

            latest = None
            while True:
                start = time.time()
                session = self.TargetSession()

                try:
                    latest = select([table.c[self.order_by]]).limit(1).order_by(table.c[self.order_by].desc()).scalar()
                    data_query = src_table.select(limit=self.scheme_conf.batch_size).order_by(src_table.c[self.order_by])

                    if latest:
                        data_query = data_query.where(src_table.c[self.order_by] > latest)

                    read_start = time.time()
                    result_values = data_query.execute()
                    values = result_values.fetchall()
                    read_end = time.time()
                    if len(values):
                        write_start = time.time()
                        stmt = table.insert(None)
                        session.execute(stmt, values)
                    else:
                        break
                except (exc.OperationalError, exc.InternalError) as e:
                    self.log.exception(e, scheme=self.scheme, db=self.trg_db)
                except Exception as e:
                    session.rollback()
                    self.log.exception(e, scheme=self.scheme, db=self.trg_db)
                else:
                    session.commit()
                    write_end = time.time()
                    end = time.time()
                    self.log.batch_include(batch_nb, len(values), table.name, latest, end-start,
                                           read_end-read_start, write_end-write_start, scheme=self.scheme, db=self.trg_db)
                    batch_nb += 1
                finally:
                    session.close()

    def run(self):
        src_metadata = MetaData(bind=self.src_engine,)
        self.log.reflecting_source(scheme=self.scheme, db=self.src_db)
        src_metadata.reflect(views=self.only_dynamic_and_views)

        trg_metadata = MetaData(bind=self.trg_engine,)

        src_views = inspect(self.src_engine).get_view_names()
        include_tables = src_metadata.tables.values()
        dynamic_tables = []
        exclude_tables = []

        self.log.reflecting_target(scheme=self.scheme, db=self.trg_db)
        try:
            trg_metadata.reflect(
                views=self.only_dynamic_and_views, **self.dialect_kwargs)
        except Exception as e:
            self.log.exception(e, scheme=self.scheme)

        if self.dynamic_tables:
            dynamic_tables = [tab for tab in include_tables
                              if re.match(self.dynamic_tables, tab.name) and tab not in src_views]
            if self.only_dynamic_and_views:
                self._do_dynamic(trg_metadata, dynamic_tables)

                views = [tab for tab in include_tables if tab.name in src_views]
                self._do_views(trg_metadata, views)

        if not self.only_dynamic_and_views:
            if self.include_tables:
                include_tables = [tab for tab in include_tables
                                  if re.match(self.include_tables, tab.name)]

            if self.exclude_tables:
                exclude_tables = [tab for tab in include_tables
                                  if re.match(self.exclude_tables, tab.name)]

            include_tables = [table for table in include_tables
                              if table not in exclude_tables and table not in dynamic_tables and table.name not in src_views]

            if include_tables:
                self._do_include(trg_metadata, include_tables)


class SchemeReplicator:

    def __init__(self, scheme, config, only_dynamic_and_views=False):
        self.config = config
        self.scheme = scheme
        self.only_dynamic_and_views = only_dynamic_and_views
        self.log = Log()

    def _get_db_name(self, db_conf, original):
        if not db_conf.naming_strategy or db_conf.naming_strategy == 'original':
            return original

        if db_conf.naming_strategy == 'exact':
            return db_conf.target

        if db_conf.naming_strategy == 'replace':
            return re.sub(db_conf.source, db_conf.target, original)

    def run(self):
        replicators = []

        main_engine = get_engine(self.config.source)
        trg_engine = get_engine(self.config.target)
        execute_first = self.config.target.execute_first
        if execute_first:
            try:
                trg_engine.execute(execute_first)
            except Exception as e:
                self.log.exception(e, scheme=self.scheme,)
            else:
                self.log.bootstrapped_with(execute_first, scheme=self.scheme)

        for db_conf in self.config.databases:
            dbs = get_databases_like(main_engine, db_conf.source)
            for db in dbs:
                trg_db = self._get_db_name(db_conf, db)
                replicator = DbReplicator(self.scheme, self.config, db, trg_db, only_dynamic_and_views=self.only_dynamic_and_views,
                                          include_tables=db_conf.include_tables, exclude_tables=db_conf.exclude_tables, dynamic_tables=db_conf.dynamic_tables, order_by=db_conf.order_by,)
                replicators.append(replicator)
                replicator.start()

        return replicators
