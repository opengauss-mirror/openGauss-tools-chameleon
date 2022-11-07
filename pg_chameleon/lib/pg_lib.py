import datetime
import decimal
import io
import json
import multiprocessing as mp
import os
import sys
import time

import binascii
import py_opengauss

from pg_chameleon import sql_token, ColumnType

# from MariaDB 10.2.7, Literals in the COLUMN_DEFAULT column in the Information Schema COLUMNS table
# are now quoted to distinguish them from expressions. https://mariadb.com/kb/en/mariadb-1027-release-notes/
COLUMNDEFAULT_INCLUDE_QUOTE_VER = 7 + sql_token.VERSION_SCALE * 2 + \
                                  (sql_token.VERSION_SCALE * sql_token.VERSION_SCALE) * 10


class pg_encoder(json.JSONEncoder):
    def default(self, obj):
        if 	isinstance(obj, datetime.time) or \
            isinstance(obj, datetime.datetime) or  \
            isinstance(obj, datetime.date) or \
            isinstance(obj, decimal.Decimal) or \
            isinstance(obj, datetime.timedelta) or \
            isinstance(obj, set) or\
            isinstance(obj, frozenset) or\
            isinstance(obj, bytes):

            return str(obj)
        return json.JSONEncoder.default(self, obj)

class pgsql_source(object):
    def __init__(self):
        """
            Class constructor, the method sets the class variables and configure the
            operating parameters from the args provided t the class.
        """
        self.schema_tables = {}
        self.schema_mappings = {}
        self.schema_loading = {}
        self.schema_list = []
        self.schema_only = {}

    def __del__(self):
        """
            Class destructor, tries to disconnect the postgresql connection.
        """
        pass

    def __set_copy_max_memory(self):
        """
            The method sets the class variable self.copy_max_memory using the value stored in the
            source setting.

        """
        copy_max_memory = str(self.source_config["copy_max_memory"])[:-1]
        copy_scale = str(self.source_config["copy_max_memory"])[-1]
        try:
            int(copy_scale)
            copy_max_memory = self.source_config["copy_max_memory"]
        except:
            if copy_scale =='k':
                copy_max_memory = str(int(copy_max_memory)*1024)
            elif copy_scale =='M':
                copy_max_memory = str(int(copy_max_memory)*1024*1024)
            elif copy_scale =='G':
                copy_max_memory = str(int(copy_max_memory)*1024*1024*1024)
            else:
                print("**FATAL - invalid suffix in parameter copy_max_memory  (accepted values are (k)ilobytes, (M)egabytes, (G)igabytes.")
                sys.exit(3)
        self.copy_max_memory = copy_max_memory


    def __init_sync(self):
        """
            The method calls the common steps required to initialise the database connections and
            class attributes within sync_tables,refresh_schema and init_replica.
        """
        self.source_config = self.sources[self.source]
        self.out_dir = self.source_config["out_dir"]
        self.copy_mode = self.source_config["copy_mode"]
        self.pg_engine.lock_timeout = self.source_config["lock_timeout"]
        self.pg_engine.grant_select_to = self.source_config["grant_select_to"]
        self.source_conn = self.source_config["db_conn"]
        self.__set_copy_max_memory()
        self.pgsql_conn = self.__connect_db()
        self.pg_engine.connect_db()
        self.schema_mappings = self.pg_engine.get_schema_mappings()
        self.pg_engine.schema_tables = self.schema_tables


    def __connect_db(self):
        """
            Connects to PostgreSQL using the parameters stored in self.dest_conn. The dictionary is built using the parameters set via adding the key dbname to the self.pg_conn dictionary.
            This method's connection and cursors are widely used in the procedure except for the replay process which uses a
            dedicated connection and cursor.

            :return: a dictionary with the objects connection and cursor
            :rtype: dictionary
        """
        if self.source_conn:
            strconn = "opengauss://%(host)s:%(port)s/%(database)s?[connect_timeout]=%(connect_timeout)s" % self.source_conn
            pgsql_conn = py_opengauss.open(strconn, user=self.dest_conn["user"], password=self.dest_conn["password"], sslmode="disable")
            pgsql_conn.settings['client_encoding']=self.source_conn["charset"]
            pgsql_conn.execute("set session_timeout = 0;")
        else:
            self.logger.error("Undefined database connection string. Exiting now.")
            sys.exit()

        return pgsql_conn

    def __export_snapshot(self, queue):
        """
            The method exports a database snapshot and stays idle in transaction until a message from the parent
            process tell it to exit.
            The method stores the snapshot id in the queue for the parent's usage.

            :param queue: the queue object used to exchange messages between the parent and the child
        """
        self.logger.debug("exporting database snapshot for source %s" % self.source)
        sql_snap = """
            SELECT pg_export_snapshot();
        """
        db_conn = self.__connect_db()
        x = db_conn.xact('REPEATABLE READ')
        x.start()
        stmt = db_conn.prepare(sql_snap)
        snapshot_id = stmt.first()
        queue.put(snapshot_id)
        continue_loop = True
        while continue_loop:
            continue_loop = queue.get()
            time.sleep(5)
        x.commit()

    def __build_table_exceptions(self):
        """
            The method builds two dictionaries from the limit_tables and skip tables values set for the source.
            The dictionaries are intended to be used in the get_table_list to cleanup the list of tables per schema.
            The method manages the particular case of when the class variable self.tables is set.
            In that case only the specified tables in self.tables will be synced. Should limit_tables be already
            set, then the resulting list is the intersection of self.tables and limit_tables.
        """
        self.limit_tables = {}
        self.skip_tables = {}
        limit_tables = self.source_config["limit_tables"]
        skip_tables = self.source_config["skip_tables"]

        if self.tables !='*':
            tables = [table.strip() for table in self.tables.split(',')]
            if limit_tables:
                limit_tables = [table for table in tables if table in limit_tables]
            else:
                limit_tables = tables
            self.schema_only = {table.split('.')[0] for table in limit_tables}


        if limit_tables:
            table_limit = [table.split('.') for table in limit_tables]
            for table_list in table_limit:
                list_exclude = []
                try:
                    list_exclude = self.limit_tables[table_list[0]]
                    list_exclude.append(table_list[1])
                except KeyError:
                    list_exclude.append(table_list[1])
                self.limit_tables[table_list[0]]  = list_exclude
        if skip_tables:
            table_skip = [table.split('.') for table in skip_tables]
            for table_list in table_skip:
                list_exclude = []
                try:
                    list_exclude = self.skip_tables[table_list[0]]
                    list_exclude.append(table_list[1])
                except KeyError:
                    list_exclude.append(table_list[1])
                self.skip_tables[table_list[0]]  = list_exclude



    def __get_table_list(self):
        """
            The method pulls the table list from the information_schema.
            The list is stored in a dictionary  which key is the table's schema.
        """
        sql_tables="""
            SELECT
                table_name
            FROM
                information_schema.TABLES
            WHERE
                    table_type='BASE TABLE'
                AND table_schema=$1
            ;
        """
        for schema in self.schema_list:
            stmt = self.pgsql_conn.prepare(sql_tables)
            table_list = [table["table_name"] for table in stmt(schema)]
            try:
                limit_tables = self.limit_tables[schema]
                if len(limit_tables) > 0:
                    table_list = [table for table in table_list if table in limit_tables]
            except KeyError:
                pass
            try:
                skip_tables = self.skip_tables[schema]
                if len(skip_tables) > 0:
                    table_list = [table for table in table_list if table not in skip_tables]
            except KeyError:
                pass

            self.schema_tables[schema] = table_list

    def __create_destination_schemas(self):
        """
            Creates the loading schemas in the destination database and associated tables listed in the dictionary
            self.schema_tables.
            The method builds a dictionary which associates the destination schema to the loading schema.
            The loading_schema is named after the destination schema plus with the prefix _ and the _tmp suffix.
            As postgresql allows, by default up to 64  characters for an identifier, the original schema is truncated to 59 characters,
            in order to fit the maximum identifier's length.
            The mappings are stored in the class dictionary schema_loading.
        """
        for schema in self.schema_list:
            destination_schema = self.schema_mappings[schema]
            loading_schema = "_%s_tmp" % destination_schema[0:59]
            self.schema_loading[schema] = {'destination':destination_schema, 'loading':loading_schema}
            self.logger.debug("Creating the schema %s." % loading_schema)
            self.pg_engine.create_database_schema(loading_schema)
            self.logger.debug("Creating the schema %s." % destination_schema)
            self.pg_engine.create_database_schema(destination_schema)

    def __get_table_metadata(self, table, schema):
        """
            The method builds the table's metadata querying the information_schema.
            The data is returned as a dictionary.

            :param table: The table name
            :param schema: The table's schema
            :return: table's metadata as a cursor dictionary
            :rtype: dictionary
        """
        sql_metadata="""
            SELECT
                col.attname as column_name,
                (
                    SELECT
                        pg_catalog.pg_get_expr(def.adbin, def.adrelid)
                    FROM
                        pg_catalog.pg_attrdef def
                    WHERE
                            def.adrelid = col.attrelid
                        AND def.adnum = col.attnum
                        AND col.atthasdef
                ) as column_default,
                col.attnum as ordinal_position,
                CASE
                    WHEN typ.typcategory ='E'
                    THEN
                        'enum'
                    WHEN typ.typcategory='C'
                    THEN
                        'composite'

                ELSE
                    pg_catalog.format_type(col.atttypid, col.atttypmod)
                END
                AS type_format,
                (
                    SELECT
                        pg_get_serial_sequence(format('%%I.%%I',tabsch.nspname,tab.relname), col.attname) IS NOT NULL
                    FROM
                        pg_catalog.pg_class tab
                        INNER JOIN pg_catalog.pg_namespace tabsch
                        ON	tab.relnamespace=tabsch.oid
                    WHERE
                        tab.oid=col.attrelid
                ) as col_serial,
                typ.typcategory as type_category,
                CASE
                    WHEN typ.typcategory='E'
                    THEN
                    (
                        SELECT
                            string_agg(quote_literal(enumlabel),',')
                        FROM
                            pg_catalog.pg_enum enm
                        WHERE enm.enumtypid=typ.oid
                    )
                    WHEN typ.typcategory='C'
                    THEN
                    (
                        SELECT
                            string_agg(
                                format('%%I %%s',
                                    attname,
                                    pg_catalog.format_type(atttypid, atttypmod)
                                )
                            ,
                            ','
                            )
                        FROM
                            pg_catalog.pg_attribute
                        WHERE
                            attrelid=format(
                                '%%I.%%I',
                                sch.nspname,
                                typ.typname)::regclass
                            )
                END AS typ_elements,
                col.attnotnull as not_null
            FROM
                pg_catalog.pg_attribute col
                INNER JOIN pg_catalog.pg_type typ
                    ON  col.atttypid=typ.oid
                INNER JOIN pg_catalog.pg_namespace sch
                    ON typ.typnamespace=sch.oid
            WHERE
                    col.attrelid = %s::regclass
                AND NOT col.attisdropped
                AND col.attnum>0
            ORDER BY
                col.attnum
            ;
            ;
        """
        tab_regclass = "'%s.%s'" % (schema, table)
        stmt = self.pgsql_conn.prepare(sql_metadata % tab_regclass)
        table_metadata=stmt()
        return table_metadata


    def __create_destination_tables(self):
        """
            The method creates the destination tables in the loading schema.
            The tables names are looped using the values stored in the class dictionary schema_tables.
        """
        for schema in self.schema_tables:
            table_list = self.schema_tables[schema]
            for table in table_list:
                table_metadata = self.__get_table_metadata(table, schema)
                self.pg_engine.create_table(table_metadata, None, table, schema, 'pgsql')

    def __drop_loading_schemas(self):
        """
            The method drops the loading schemas from the destination database.
            The drop is performed on the schemas generated in create_destination_schemas.
            The method assumes the class dictionary schema_loading is correctly set.
        """
        for schema in self.schema_loading:
            loading_schema = self.schema_loading[schema]["loading"]
            self.logger.debug("Dropping the schema %s." % loading_schema)
            self.pg_engine.drop_database_schema(loading_schema, True)

    def __copy_data(self, schema, table, db_conn):

        sql_snap = """
            SET TRANSACTION SNAPSHOT %s;
        """
        loading_schema = self.schema_loading[schema]["loading"]
        from_table = "%s.%s" % (schema, table)
        to_table = "%s.%s" % (loading_schema, table)

        if self.snapshot_id:
            x = db_conn.xact('REPEATABLE READ')
            x.start()
            db_conn.execute(sql_snap % self.snapshot_id)
        else:
            x = db_conn.xact()
            x.start()

        send_stmt = db_conn.prepare("COPY %s to STDOUT" % from_table)
        receive_stmt = db_conn.prepare("COPY %s from STDIN" % to_table)
        receive_stmt.load_chunks(send_stmt.chunks())
        x.commit()


    def __create_indices(self):
        """
            The method loops over the tables, queries the origin's database and creates the same indices
            on the loading schema.
        """
        db_conn = self.__connect_db()
        sql_get_idx = """
            SELECT
                CASE
                    WHEN con.conname IS NOT NULL
                    THEN
                        format('ALTER TABLE %%I ADD CONSTRAINT %%I %%s ;',tab.relname,con.conname,pg_get_constraintdef(con.oid))
                    ELSE
                        format('%%s ;',regexp_replace(pg_get_indexdef(idx.oid), '("?\w+"?\.)', ''))
                END AS ddl_text,
                CASE
                    WHEN con.conname IS NOT NULL
                    THEN
                        format('primary key on %%I',tab.relname)
                    ELSE
                        format('index %%I on %%I',idx.relname,tab.relname)
                END AS ddl_msg,
                CASE
                    WHEN con.conname IS NOT NULL
                    THEN
                        True
                    ELSE
                        False
                END AS table_pk
            FROM

                pg_class tab
                INNER JOIN pg_namespace sch
                ON
                    sch.oid=tab.relnamespace
                INNER JOIN pg_index ind
                ON
                    ind.indrelid=tab.oid
                INNER JOIN pg_class idx
                ON
                    ind.indexrelid=idx.oid
                LEFT OUTER JOIN pg_constraint con
                ON
                        con.conrelid=tab.oid
                    AND	idx.oid=con.conindid

            WHERE
                (
                        contype='p'
                    OR 	contype IS NULL
                )
                AND	tab.relname='%s'
                AND	sch.nspname='%s'
            ;
        """

        for schema in self.schema_tables:
            table_list = self.schema_tables[schema]
            for table in table_list:
                loading_schema = self.schema_loading[schema]["loading"]
                destination_schema = self.schema_loading[schema]["destination"]
                self.pg_engine.pgsql_conn.settings['search_path'] = loading_schema
                stmt = db_conn.prepare(sql_get_idx% (table, schema))
                idx_tab = stmt()
                for idx in idx_tab:
                    self.logger.info('Adding %s', (idx[1]))
                    try:
                        self.pg_engine.pgsql_conn.execute(idx[0])
                    except:
                        self.logger.error("an error occcurred when executing %s" %(idx[0]))
                    if idx[2]:
                        self.pg_engine.store_table(destination_schema, table, ['foo'], None)
        db_conn.close()

    def __copy_tables(self):
        """
            The method copies the data between tables, from the postgres source and the corresponding
            postgresql loading schema. Before the process starts a snapshot is exported in order to get
            a consistent database copy at the time of the snapshot.
        """

        db_conn = self.__connect_db()
        sql_recovery = """
            SELECT pg_is_in_recovery();
        """
        x = pgsql_conn.xact()
        x.start()
        stmt = pgsql_conn.prepare(sql_recovery)
        db_in_recovery = stmt.first()
        x.commit()
        if not db_in_recovery:
            queue = mp.Queue()
            snap_exp = mp.Process(target=self.__export_snapshot, args=(queue,), name='snap_export',daemon=True)
            snap_exp.start()
            self.snapshot_id = queue.get()
            self.consistent = False
        else:
            self.snapshot_id = None
            self.consistent = False

        for schema in self.schema_tables:
            table_list = self.schema_tables[schema]
            for table in table_list:
                self.__copy_data(schema, table, db_conn)
        if not db_in_recovery:
            queue.put(False)
        db_conn.close()

    def init_replica(self):
        """
            The method performs a full init replica for the given source
        """
        self.logger.debug("starting init replica for source %s" % self.source)
        self.__init_sync()
        self.schema_list = [schema for schema in self.schema_mappings]
        self.__build_table_exceptions()
        self.__get_table_list()
        self.__create_destination_schemas()
        self.pg_engine.schema_loading = self.schema_loading
        self.pg_engine.set_source_status("initialising")
        try:
            self.__create_destination_tables()
            self.__copy_tables()
            self.__create_indices()
            self.pg_engine.grant_select()
            self.pg_engine.swap_schemas()
            self.__drop_loading_schemas()
            self.pg_engine.set_source_status("initialised")
            fake_master = [{'File': None, 'Position': None }]
            self.pg_engine.set_source_highwatermark(fake_master, consistent=self.consistent)
            notifier_message = "init replica for source %s is complete" % self.source
            self.notifier.send_message(notifier_message, 'info')
            self.logger.info(notifier_message)
        except:
            self.__drop_loading_schemas()
            self.pg_engine.set_source_status("error")
            notifier_message = "init replica for source %s failed" % self.source
            self.notifier.send_message(notifier_message, 'critical')
            self.logger.critical(notifier_message)


            raise



class pg_engine(object):
    def __init__(self):
        python_lib=python_lib=os.path.dirname(os.path.realpath(__file__))
        self.sql_dir = "%s/../sql/" % python_lib
        self.sql_upgrade_dir = "%s/upgrade/" % self.sql_dir
        self.table_ddl={}
        self.idx_ddl={}
        self.type_ddl={}
        self.idx_sequence=0
        self.type_dictionary = {
            ColumnType.M_INTEGER.value:ColumnType.O_INTEGER.value,
            ColumnType.M_MINT.value:ColumnType.O_INTEGER.value,
            ColumnType.M_TINT.value:ColumnType.O_INTEGER.value,
            ColumnType.M_SINT.value:ColumnType.O_INTEGER.value,
            ColumnType.M_INT.value:ColumnType.O_INTEGER.value,
            ColumnType.M_BINT.value:ColumnType.O_BINT.value,
            ColumnType.M_VARCHAR.value:ColumnType.O_C_CHAR_VAR.value,
            ColumnType.M_CHAR_VAR.value:ColumnType.O_C_CHAR_VAR.value,
            ColumnType.M_TEXT.value:ColumnType.O_C_TEXT.value,
            ColumnType.M_CHAR.value:ColumnType.O_C_CHARACTER.value,
            ColumnType.M_DATATIME.value:ColumnType.O_TIMESTAP_NO_TZ.value,
            ColumnType.M_DATE.value:ColumnType.O_DATE.value,
            ColumnType.M_TIME.value:ColumnType.O_TIME_NO_TZ.value,
            ColumnType.M_TIMESTAMP.value:ColumnType.O_TIMESTAP_NO_TZ.value,
            ColumnType.M_TTEXT.value:ColumnType.O_C_TEXT.value,
            ColumnType.M_MTEXT.value:ColumnType.O_C_TEXT.value,
            ColumnType.M_LTEXT.value:ColumnType.O_C_TEXT.value,
            ColumnType.M_HEX_T_BLOB.value:ColumnType.O_BLOB.value,
            ColumnType.M_HEX_M_BLOB.value:ColumnType.O_BLOB.value,
            ColumnType.M_HEX_L_BLOB.value:ColumnType.O_BLOB.value,
            ColumnType.M_HEX_BLOB.value:ColumnType.O_BLOB.value,
            ColumnType.M_BINARY.value:ColumnType.O_BYTEA.value,
            ColumnType.M_VARBINARY.value:ColumnType.O_BYTEA.value,
            ColumnType.M_DECIMAL.value:ColumnType.O_NUMBER.value,
            ColumnType.M_DEC.value:ColumnType.O_NUMBER.value,
            ColumnType.M_NUM.value:ColumnType.O_NUMBER.value,
            ColumnType.M_DOUBLE.value:ColumnType.O_NUMBER.value,
            ColumnType.M_DOUBLE_P.value:ColumnType.O_NUMBER.value,
            ColumnType.M_FLOAT.value:ColumnType.O_NUMBER.value,
            ColumnType.M_FLOAT4.value:ColumnType.O_NUMBER.value,
            ColumnType.M_FLOAT8.value:ColumnType.O_NUMBER.value,
            ColumnType.M_REAL.value:ColumnType.O_NUMBER.value,
            ColumnType.M_FIXED.value:ColumnType.O_NUMBER.value,
            ColumnType.M_BIT.value:ColumnType.O_INTEGER.value,
            ColumnType.M_YEAR.value:ColumnType.O_INTEGER.value,
            ColumnType.M_ENUM.value:ColumnType.O_ENUM.value,
            ColumnType.M_SET.value:ColumnType.O_SET.value,
            ColumnType.M_JSON.value:ColumnType.O_JSON.value,
            ColumnType.M_BOOL.value:ColumnType.O_BOOLEAN.value,
            ColumnType.M_BOOLEAN.value:ColumnType.O_BOOLEAN.value,
        }
        self.dest_conn = None
        self.pgsql_conn = None
        self.logger = None
        self.idx_sequence = 0
        self.lock_timeout = 0
        self.max_range_column = 4
        self.mysql_version = 0
        self.hash_part_key_type = ColumnType.get_opengauss_hash_part_key_type()
        self.character_type = ColumnType.get_opengauss_char_type()
        self.date_type = ColumnType.get_opengauss_date_type()
        self.default_value_map = {
            # without time zone
            'curdate()': "CURRENT_DATE",
            'curtime()': "('now'::text)::time",
            'current_timestamp()': "pg_systimestamp()::timestamp",
            'current_timestamp': "pg_systimestamp()::timestamp",
            '0000-00-00 00:00:00': "NULL",
            '0000-00-00': "NULL"
        }
        for i in range(7):
            str = "current_timestamp(%s)" % i
            self.default_value_map[str] = str

        self.migrations = [
            {'version': '2.0.1',  'script': '200_to_201.sql'},
            {'version': '2.0.2',  'script': '201_to_202.sql'},
            {'version': '2.0.3',  'script': '202_to_203.sql'},
            {'version': '2.0.4',  'script': '203_to_204.sql'},
            {'version': '2.0.5',  'script': '204_to_205.sql'},
            {'version': '2.0.6',  'script': '205_to_206.sql'},
            {'version': '2.0.7',  'script': '206_to_207.sql'},
        ]

    def check_postgis(self):
        """
            The method checks whether postgis is present or not on the
        """
        sql_check = """
            SELECT
                count(*)=1
            FROM
                pg_extension
            WHERE
                extname='postgis';
        ;"""
        self.connect_db()
        stmt = self.pgsql_conn.prepare(sql_check)
        postgis_check = stmt.first()
        self.postgis_present = postgis_check
        if self.postgis_present:
            spatial_data = {
            ColumnType.M_C_GIS_GEO.value:ColumnType.O_GEO.value,
            ColumnType.M_C_GIS_POINT.value:ColumnType.O_GEO.value,
            ColumnType.M_C_GIS_LINESTR.value:ColumnType.O_GEO.value,
            ColumnType.M_C_GIS_POLYGON.value:ColumnType.O_GEO.value,
            ColumnType.M_S_GIS_MUL_POINT.value:ColumnType.O_GEO.value,
            ColumnType.M_S_GIS_GEOCOL.value:ColumnType.O_GEO.value,
            ColumnType.M_S_GIS_GEOCOL2.value:ColumnType.O_GEO.value,
            ColumnType.M_S_GIS_MUL_LINESTR.value:ColumnType.O_GEO.value,
            ColumnType.M_S_GIS_MUL_POLYGON.value:ColumnType.O_GEO.value
            }
        else:
            spatial_data = {
            ColumnType.M_C_GIS_GEO.value:ColumnType.O_POINT.value,
            ColumnType.M_C_GIS_POINT.value:ColumnType.O_POINT.value,
            ColumnType.M_C_GIS_LINESTR.value:ColumnType.O_PATH.value,
            ColumnType.M_C_GIS_POLYGON.value:ColumnType.O_POLYGON.value,
            ColumnType.M_S_GIS_MUL_POINT.value:ColumnType.O_BYTEA.value,
            ColumnType.M_S_GIS_GEOCOL.value:ColumnType.O_BYTEA.value,
            ColumnType.M_S_GIS_GEOCOL2.value:ColumnType.O_BYTEA.value,
            ColumnType.M_S_GIS_MUL_LINESTR.value:ColumnType.O_BYTEA.value,
            ColumnType.M_S_GIS_MUL_POLYGON.value:ColumnType.O_BYTEA.value
            }
        self.type_dictionary.update(spatial_data.items())
        return postgis_check
    def __del__(self):
        """
            Class destructor, tries to disconnect the postgresql connection.
        """
        self.disconnect_db()

    def connect_db(self):
        """
            Connects to PostgreSQL using the parameters stored in self.dest_conn. The dictionary is built using the parameters set via adding the key dbname to the self.pg_conn dictionary.
            This method's connection and cursors are widely used in the procedure except for the replay process which uses a
            dedicated connection and cursor.
        """
        if self.dest_conn and not self.pgsql_conn:
            strconn = "opengauss://%(host)s:%(port)s/%(database)s" % self.dest_conn
            self.pgsql_conn = py_opengauss.open(strconn, user=self.dest_conn["user"], password=self.dest_conn["password"], sslmode="disable")
            self.pgsql_conn.settings['client_encoding']=self.dest_conn["charset"]
            self.pgsql_conn.execute("set session_timeout = 0;")
        elif not self.dest_conn:
            self.logger.error("Undefined database connection string. Exiting now.")
            sys.exit()
        elif self.pgsql_conn:
            self.logger.debug("There is already a database connection active.")


    def disconnect_db(self):
        """
            The method disconnects the postgres connection if there is any active. Otherwise ignore it.
        """
        if self.pgsql_conn:
            self.pgsql_conn.close()
            self.pgsql_conn = None

    def set_lock_timeout(self):
        """
            The method sets the lock timeout using the value stored in the class attribute lock_timeout.
        """
        self.logger.debug("Changing the lock timeout for the session to %s." % self.lock_timeout)
        #self.pgsql_conn.execute("SET LOCK_TIMEOUT =%s;",  (self.lock_timeout, ))

    def unset_lock_timeout(self):
        """
            The method sets the lock timeout using the value stored in the class attribute lock_timeout.
        """
        self.logger.debug("Disabling the lock timeout for the session." )
        #self.pgsql_conn.execute("SET LOCK_TIMEOUT ='0';")

    def create_replica_schema(self):
        """
            The method installs the replica schema sch_chameleon if not already  present.
        """
        self.logger.debug("Trying to connect to the destination database.")
        self.connect_db()
        num_schema = self.check_replica_schema()
        if num_schema == 0:
            self.logger.debug("Creating the replica schema.")
            file_schema = open(self.sql_dir+"create_schema.sql", 'r')
            sql_schema = file_schema.read()
            file_schema.close()
            self.pgsql_conn.execute(sql_schema)

        else:
            self.logger.warning("The replica schema is already present.")

    def detach_replica(self):
        """
            The method detach the replica from mysql, resets all the sequences and creates the foreign keys
            using the dictionary extracted from mysql. The result is a stand alone set of schemas ready to work.

            The foreign keys are first created invalid then validated in a second time.
        """
        self.connect_db()
        self.set_source_id()
        schema_mappings = self.get_schema_mappings()
        fk_list = []
        fk_counter = 0
        sql_gen_reset = """
            SELECT
                format('SELECT setval(%%L::regclass,(select max(%%I) FROM %%I.%%I));',
                    replace(replace(column_default,'nextval(''',''),'''::regclass)',''),
                    column_name,
                    table_schema,
                    table_name
                ),
                replace(replace(column_default,'nextval(''',''),'''::regclass)','') as  seq_name
            FROM
                    information_schema.columns
            WHERE
                    table_schema IN (
                        SELECT
                            (jsonb_each_text(jsb_schema_mappings)).value
                        FROM
                            sch_chameleon.t_sources
                        WHERE
                            i_id_source=%s
                        )
                AND	column_default like 'nextval%%'

        ;"""
        stmt = self.pgsql_conn.prepare(sql_gen_reset % self.i_id_source)
        reset_statements = stmt()
        try:
            for statement in reset_statements:
                self.logger.info("resetting the sequence  %s" % statement[1])
                self.pgsql_conn.execute(statement[0])
        except Exception as e:
                    self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.code, e.message))
                    self.logger.error(statement)
        except:
            raise
        if not self.keep_existing_schema:
            for foreign_key in self.fk_metadata:
                table_name = foreign_key["table_name"]
                table_schema = schema_mappings[foreign_key["table_schema"]]
                fk_name = ("%s_%s") % (foreign_key["constraint_name"][0:20] ,  str(fk_counter))
                fk_cols = foreign_key["fk_cols"]
                referenced_table_name = foreign_key["referenced_table_name"]
                referenced_table_schema = schema_mappings[foreign_key["referenced_table_schema"]]
                ref_columns = foreign_key["ref_columns"]
                update_rule = foreign_key["update_rule"]
                delete_rule = foreign_key["delete_rule"]
                fk_list.append({'fkey_name':fk_name, 'table_name':table_name, 'table_schema':table_schema})
                sql_fkey = ("""
                    ALTER TABLE "%s"."%s" ADD CONSTRAINT "%s" FOREIGN KEY (%s) REFERENCES "%s"."%s" (%s)
                    ON UPDATE %s ON DELETE %s NOT VALID;
                    """ %
                        (
                            table_schema,
                            table_name,
                            fk_name,
                            fk_cols,
                            referenced_table_schema,
                            referenced_table_name,
                            ref_columns,
                            update_rule,
                            delete_rule
                        )
                    )
                fk_counter+=1
                self.logger.info("creating invalid foreign key %s on table %s.%s" % (fk_name, table_schema, table_name))
                try:
                    self.pgsql_conn.execute(sql_fkey)
                except Exception as e:
                        self.logger.error("could not create the foreign key %s on table %s.%s" % (fk_name, table_schema, table_name))
                        self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.code, e.message))
                        self.logger.error("STATEMENT: %s " % (sql_fkey))


            for fkey in fk_list:
                self.logger.info("validating %s on table %s.%s"  % (fkey["fkey_name"], fkey["table_schema"], fkey["table_name"]))
                sql_validate = 'ALTER TABLE "%s"."%s" VALIDATE CONSTRAINT "%s";' % (fkey["table_schema"], fkey["table_name"], fkey["fkey_name"])
                try:
                    self.pgsql_conn.execute(sql_validate)
                except Exception as e:
                        self.logger.error("could not validate the foreign key %s on table %s" % (fkey["table_name"], fkey["fkey_name"]))
                        self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.code, e.message))
                        self.logger.error("STATEMENT: %s " % (sql_validate))
        self.drop_source()

    def get_inconsistent_tables(self):
        """
            The method collects the tables in not consistent state.
            The informations are stored in a dictionary which key is the table's name.
            The dictionary is used in the read replica loop to determine wheter the table's modifications
            should be ignored because in not consistent state.

            :return: a dictionary with the tables in inconsistent state and their snapshot coordinates.
            :rtype: dictionary
        """
        sql_get = """
            SELECT
                v_schema_name,
                v_table_name,
                t_binlog_name,
                i_binlog_position
            FROM
                sch_chameleon.t_replica_tables
            WHERE
                t_binlog_name IS NOT NULL
                AND i_binlog_position IS NOT NULL
                AND i_id_source = %s
        ;
        """
        inc_dic = {}
        stmt = self.pgsql_conn.prepare(sql_get % (self.i_id_source))
        inc_results = stmt()
        for table  in inc_results:
            tab_dic = {}
            dic_key = "%s.%s" % (table[0], table[1])
            tab_dic["schema"]  = table[0]
            tab_dic["table"]  = table[1]
            tab_dic["log_seq"]  = int(table[2].split('.')[1])
            tab_dic["log_pos"]  = int(table[3])
            inc_dic[dic_key] = tab_dic
        return inc_dic


    def grant_select(self):
        """
            The method grants the select permissions on all the tables on the replicated schemas to the database roles
            listed in the source's variable grant_select_to.
            In the case a role doesn't exist the method emits an error message and skips the missing user.
        """
        if self.grant_select_to:
            for schema in  self.schema_loading:
                schema_loading = self.schema_loading[schema]["loading"]
                self.logger.info("Granting select on tables in schema %s to the role(s) %s." % (schema_loading,','.join(self.grant_select_to)))
                for db_role in self.grant_select_to:
                    sql_grant_usage = ("GRANT USAGE ON SCHEMA \"{}\" TO \"{}\";").format(schema_loading, db_role)
                    sql_alter_default_privs = ("ALTER DEFAULT PRIVILEGES IN SCHEMA \"{}\" GRANT SELECT ON TABLES TO \"{}\";").format(schema_loading, db_role)
                    try:
                        self.pgsql_conn.execute(sql_grant_usage)
                        self.pgsql_conn.execute(sql_alter_default_privs)
                        for table in self.schema_tables[schema]:
                            self.logger.info("Granting select on table %s.%s to the role %s." % (schema_loading, table,db_role))
                            sql_grant_select = ("GRANT SELECT ON TABLE \"{}\".\"{}\" TO \"{}\";").format(schema_loading, table, db_role)
                            try:
                                self.pgsql_conn.execute(sql_grant_select)
                            except Exception as er:
                                self.logger.error("SQLCODE: %s SQLERROR: %s" % (er.code, er.message))
                    except Exception as e:
                        if e.code == "42704":
                            self.logger.warning("The role %s does not exist" % (db_role, ))
                        else:
                            self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.code, e.message))

    def set_read_paused(self, read_paused):
        """
            The method sets the read proces flag b_paused to true for the given source.
            The update is performed for the given source and for the negation of b_paused.
            This approach will prevent unnecessary updates on the table t_last_received.

            :param read_paused: the flag to set for the read replica process.
        """
        not_read_paused = not read_paused
        sql_pause = """
            UPDATE sch_chameleon.t_last_received
                SET b_paused=%s
            WHERE
                    i_id_source=%s
                AND	b_paused=%s
            ;
        """
        self.pgsql_conn.execute(sql_pause % (read_paused, self.i_id_source, not_read_paused))

    def set_replay_paused(self, read_paused):
        """
            The method sets the read proces flag b_paused to true for the given source.
            The update is performed for the given source and for the negation of b_paused.
            This approach will prevent unnecessary updates on the table t_last_received.

            :param read_paused: the flag to set for the read replica process.
        """
        not_read_paused = not read_paused
        sql_pause = """
            UPDATE sch_chameleon.t_last_replayed
                SET b_paused=%s
            WHERE
                    i_id_source=%s
                AND	b_paused=%s
            ;
        """
        self.pgsql_conn.execute(sql_pause % (read_paused, self.i_id_source, not_read_paused))

    def __check_maintenance(self):
        """
            The method returns the flag b_maintenance for the current source.
            :return:
            :rtype: boolean
        """
        sql_count = """
            SELECT
                b_maintenance
            FROM
                sch_chameleon.t_sources
            WHERE
                    i_id_source=%s
            ;
        """
        stmt = self.pgsql_conn.prepare(sql_count % (self.i_id_source))
        maintenance_running = stmt.first()
        return maintenance_running

    def __start_maintenance(self):
        """
            The method sets the flag b_maintenance to true for the given source
        """
        sql_start = """
            UPDATE sch_chameleon.t_sources
                SET b_maintenance='t'
            WHERE i_id_source=%s;
        """
        self.pgsql_conn.execute(sql_start % (self.i_id_source))

    def end_maintenance(self):
        """
            The method sets the flag b_maintenance to false for the given source
        """
        sql_end = """
            UPDATE sch_chameleon.t_sources
                SET b_maintenance='f'
            WHERE i_id_source=%s;
        """
        self.pgsql_conn.execute(sql_end % (self.i_id_source))

    def __pause_replica(self, others):
        """
            The method pause the replica updating the b_paused flag for the given current source or the other sources in the target database
        """
        if others:
            where_cond = """WHERE i_id_source<>%s; """
        else:
            where_cond = """WHERE i_id_source=%s; """

        sql_pause = """
            UPDATE sch_chameleon.t_sources
                SET b_paused='t'
            %s
        """ % where_cond
        self.pgsql_conn.execute(sql_pause % (self.i_id_source))

    def __resume_replica(self, others):
        """
            The method resumes the replica updating the b_paused flag for the given current source or the other sources in the target database
        """
        if others:
            where_cond = """WHERE i_id_source<>%s; """
        else:
            where_cond = """WHERE i_id_source=%s; """

        sql_resume = """
            UPDATE sch_chameleon.t_sources
                SET b_paused='f'
            %s
        """ % where_cond
        self.pgsql_conn.execute(sql_resume % (self.i_id_source))

    def __set_last_maintenance(self):
        """
            The method updates the field ts_last_maintenance for the given source in the table t_sources
        """
        sql_set = """
            UPDATE sch_chameleon.t_sources
                SET ts_last_maintenance=now()
            WHERE
                i_id_source=%s;
        """
        self.pgsql_conn.execute(sql_set % (self.i_id_source))


    def get_replica_paused(self):
        """
            The method returns the status of the replica. This value is used in both read/replay replica methods for updating the corresponding flags.
            :return: the b_paused flag for the current source
            :rtype: boolean
        """
        sql_get = """
            SELECT
                b_paused
            FROM
                sch_chameleon.t_sources
            WHERE
                i_id_source=%s
            ;
        """
        stmt = self.pgsql_conn.prepare(sql_get % (self.i_id_source))
        replica_paused = stmt.first()
        return replica_paused


    def __wait_for_self_pause(self):
        """
            The method returns the status of the replica. This value is used in both read/replay replica methods for updating the corresponding flags.
            :return: the b_paused flag for the current source
            :rtype: boolean
        """
        sql_wait = """
            SELECT
                CASE
                    WHEN src.enm_status IN ('stopped','initialised','synced')
                        THEN 'proceed'
                    WHEN src.enm_status = 'running'
                        THEN
                            CASE
                                WHEN
                                        src.b_paused
                                    AND	rcv.b_paused
                                    AND	rep.b_paused
                                THEN
                                    'proceed'
                                WHEN
                                        src.b_paused
                                THEN
                                    'wait'
                                ELSE
                                    'abort'
                            END
                    ELSE
                        'abort'
                END AS t_action,
                src.enm_status,
                rcv.b_paused,
                rep.b_paused,
                src.b_paused

            FROM
                sch_chameleon.t_sources src
                INNER JOIN sch_chameleon.t_last_received rcv
                ON
                    src.i_id_source=rcv.i_id_source
                INNER JOIN sch_chameleon.t_last_replayed rep
                ON
                    src.i_id_source=rep.i_id_source

            WHERE
                    src.i_id_source=%s
            ;
        """
        self.logger.info("Waiting for the replica daemons to pause")
        wait_result = 'wait'
        while wait_result == 'wait':
            stmt = self.pgsql_conn.prepare(sql_wait % (self.i_id_source))
            wait_result = stmt.first()
            time.sleep(5)

        return wait_result

    def __vacuum_full_log_tables(self):
        """
            The method runs a VACUUM FULL on the log tables for the given source
        """
        sql_vacuum = """
            SELECT
                v_log_table,
                format('VACUUM FULL sch_chameleon.%%I ;',
                v_log_table
                )
            FROM
            (
                SELECT
                    unnest(v_log_table) AS v_log_table
                FROM
                    sch_chameleon.t_sources
                WHERE
                    i_id_source=%s
            ) log
            ;
        """
        stmt = self.pgsql_conn.prepare(sql_vacuum % (self.i_id_source))
        vacuum_sql = stmt()
        for sql_stat in vacuum_sql:
            self.logger.info("Running VACUUM FULL on the table %s" % (sql_stat[0]))
            try:
                self.pgsql_conn.execute(sql_stat[1])
            except:
                self.logger.error("An error occurred when running VACUUM FULL on the table %s" % (sql_stat[0]))



    def __vacuum_log_tables(self):
        """
            The method runs a VACUUM on the log tables for the given source
        """
        sql_vacuum = """
            SELECT
                v_log_table,
                format('VACUUM sch_chameleon.%%I ;',
                v_log_table
                )
            FROM
            (
                SELECT
                    unnest(v_log_table) AS v_log_table
                FROM
                    sch_chameleon.t_sources
                WHERE
                    i_id_source=%s
            ) log
            ;
        """
        stmt = self.pgsql_conn.prepare(sql_vacuum% (self.i_id_source, ))
        vacuum_sql = stmt()
        for sql_stat in vacuum_sql:
            self.logger.info("Running VACUUM on the table %s" % (sql_stat[0]))
            try:
                self.pgsql_conn.execute(sql_stat[1])
            except:
                self.logger.error("An error occurred when running VACUUM on the table %s" % (sql_stat[0]))


    def run_maintenance(self):
        """
            The method runs the maintenance for the given source.
            After the replica daemons are paused the procedure detach the log tables from the parent log table and performs a VACUUM FULL againts the tables.
            If any error occurs the tables are attached to the parent table and the replica daemons resumed.

        """
        self.logger.info("Pausing the replica daemons")
        self.connect_db()
        self.set_source_id()
        check_maintenance = self.__check_maintenance()
        if check_maintenance:
            self.logger.info("The source is already in maintenance. Skipping the maintenance run.")
        else:
            self.__start_maintenance()
            self.__pause_replica(others=False)
            wait_result = self.__wait_for_self_pause()
            if wait_result == 'abort':
                self.logger.error("Cannot proceed with the maintenance")
                return wait_result
            if self.full:
                self.__vacuum_full_log_tables()
            else:
                self.__vacuum_log_tables()
            self.__set_last_maintenance()
            self.logger.info("Resuming the replica daemons")
            self.__resume_replica(others=False)
            self.end_maintenance()
            self.disconnect_db()
            notifier_message = "maintenance for source %s is complete" % self.source
            self.notifier.send_message(notifier_message, 'info')
            self.logger.info(notifier_message)


    def replay_replica(self):
        """
            The method replays the row images in the target database using the function
            fn_replay_mysql. The function returns a composite type.
            The first element is a boolean flag which
            is true if the batch still require replay. it's false if it doesn't.
            In that case the while loop ends.
            The second element is a, optional list of table names. If any table cause error during the replay
            the problem is captured and the table is removed from the replica. Then the name is returned by
            the function. As the function can find multiple tables with errors during a single replay run, the
            table names are stored in a list (Actually is a postgres array, see the create_schema.sql file for more details).

             Each batch which is looped trough can also find multiple tables so we return a list of lists to the replica_engine's
             calling method.

        """
        tables_error = []
        replica_paused = self.get_replica_paused()
        if replica_paused:
            self.logger.info("Replay replica is paused")
            self.set_replay_paused(True)
        else:
            self.set_replay_paused(False)
            continue_loop = True
            self.source_config = self.sources[self.source]
            replay_max_rows = self.source_config["replay_max_rows"]
            exit_on_error = True if self.source_config["on_error_replay"]=='exit' else False
            while continue_loop:
                sql_replay = """SELECT * FROM sch_chameleon.fn_replay_mysql(%s,%s,%s);"""
                stmt = self.pgsql_conn.prepare(sql_replay % (replay_max_rows, self.i_id_source, exit_on_error))
                replay_status = stmt.first()
                if replay_status[0]:
                    self.logger.info("Replayed at most %s rows for source %s" % (replay_max_rows, self.source) )
                replica_paused = self.get_replica_paused()
                if replica_paused:
                    break
                continue_loop = replay_status[0]
                function_error = replay_status[1]
                if function_error:
                    raise Exception('The replay process crashed')
                if replay_status[2]:
                    tables_error.append(replay_status[2])
        return tables_error


    def set_consistent_table(self, table, schema):
        """
            The method set to NULL the  binlog name and position for the given table.
            When the table is marked consistent the read replica loop reads and saves the table's row images.

            :param table: the table name
        """
        sql_set = """
            UPDATE sch_chameleon.t_replica_tables
                SET
                    t_binlog_name = NULL,
                    i_binlog_position = NULL
            WHERE
                    i_id_source = %s
                AND	v_table_name = '%s'
                AND	v_schema_name = '%s'
            ;
        """
        self.pgsql_conn.execute(sql_set % (self.i_id_source, table, schema))

    def get_table_pkey(self, schema, table):
        """
            The method queries the table sch_chameleon.t_replica_tables and gets the primary key
            associated with the table, if any.
            If there is no primary key the method returns None

            :param schema: The table schema
            :param table: The table name
            :return: the primary key associated with the table
            :rtype: list

        """
        sql_pkey = """
            SELECT
                v_table_pkey
            FROM
                sch_chameleon.t_replica_tables
            WHERE
                    v_schema_name='%s'
                AND	v_table_name='%s'
            ;
        """
        stmt = self.pgsql_conn.prepare(sql_pkey % (schema, table))
        table_pkey = stmt.first()
        return table_pkey


    def cleanup_replayed_batches(self):
        """
            The method cleanup the replayed batches for the given source accordingly with the source's parameter  batch_retention
        """
        self.connect_db()
        source_config = self.sources[self.source]
        batch_retention = source_config["batch_retention"]
        self.logger.debug("Cleaning replayed batches for source %s older than %s" % (self.source,batch_retention) )
        sql_cleanup = """
            DELETE FROM
                sch_chameleon.t_replica_batch
            WHERE
                    b_started
                AND b_processed
                AND b_replayed
                AND now()-ts_replayed>'%s'::interval
                AND i_id_source=%s
            ;
        """
        self.pgsql_conn.execute(sql_cleanup % (batch_retention, self.i_id_source ))
        self.disconnect_db()

    def generate_ddl(self, token,  destination_schema):
        """
            The method builds the DDL using the tokenised SQL stored in token.
            The supported commands are
            RENAME TABLE
            DROP TABLE
            TRUNCATE
            CREATE TABLE
            ALTER TABLE
            DROP PRIMARY KEY

            :param token: A dictionary with the tokenised sql statement
            :param destination_schema: The ddl destination schema mapped from the mysql corresponding schema
            :return: query the DDL query in the PostgreSQL dialect
            :rtype: string

        """

        count_table = self.__count_table_schema(token["name"], destination_schema)
        query=""
        if token["command"] =="CREATE TABLE":
            table_metadata = token["columns"]
            table_name = token["name"]
            index_data = token["indices"]
            # now we don't support create parition table in start_replica stage
            # add create partition
            partition_metadata = token["partition"]
            table_ddl = self.__build_create_table_mysql(table_metadata, partition_metadata, table_name, destination_schema, temporary_schema=False)
            if table_ddl == "":
                return ""
            table_enum = ''.join(table_ddl["enum"])
            table_statement = table_ddl["table"]
            index_ddl = self.build_create_index( destination_schema, table_name, index_data)
            table_pkey = index_ddl[0]
            table_indices = ''.join([val for key ,val in index_ddl[1].items()])
            self.store_table(destination_schema, table_name, table_pkey, None)
            table_comment = self.build_table_comment(destination_schema, table_name, token["comment"])
            query = "%s %s %s %s" % (table_enum, table_statement,  table_indices, table_comment)
        else:
            if count_table == 1:
                if token["command"] == "RENAME TABLE":
                    old_name = token["name"]
                    new_name = token["new_name"]
                    query = """ALTER TABLE "%s"."%s" RENAME TO "%s" """ % (destination_schema, old_name, new_name)
                    table_pkey = self.get_table_pkey(destination_schema, old_name)
                    self.store_table(destination_schema, new_name, table_pkey, None)
                elif token["command"] == "DROP TABLE":
                    query=""" DROP TABLE IF EXISTS "%s"."%s";""" % (destination_schema, token["name"])
                elif token["command"] == "TRUNCATE":
                    query=""" TRUNCATE TABLE "%s"."%s" CASCADE;""" % (destination_schema, token["name"])
                elif token["command"] == "ALTER TABLE":
                    query=self.build_alter_table(destination_schema, token)
                elif token["command"] == "DROP PRIMARY KEY":
                    self.__drop_primary_key(destination_schema, token)
                elif token["command"] == "TALTER TABLE":
                    # TALTER TABLE sum all the alter table DDL, so we need to move all into this part: build_talter_table
                    query = self.build_talter_table(destination_schema, token)
                elif token["command"] == "CREATE INDEX FULL":
                    if token["UFS"] == "UNIQUE":
                        command = "CREATE UNIQUE INDEX"
                    else:
                        command = "CREATE INDEX"
                    index_type = ''
                    if token["index_type"]:
                        index_type = "USING %s" % (token["index_type"])
                    key_part = token["key_part"]
                    index_option = token["index_option"]
                    if index_option.upper().find("USING") != -1:
                        index_type = index_option
                    index_name = token["index_name"]
                    table_name = token["name"]
                    if self.have_table_partitions(destination_schema, token["name"]):
                        local = " local "
                    else:
                        local = ""
                    if key_part.find("(")==-1:
                        index_name = "%s_%s" % (table_name, index_name)
                    else:
                        [index_name, key_part] = self.find_key_from_express(destination_schema, table_name, index_name, key_part)
                    query = """%s %s.%s ON %s.%s %s (%s) %s;""" % (
                        command, destination_schema, index_name, destination_schema, table_name, index_type, key_part, local)
                    if index_option.upper().find("COMMENT") != -1:
                        comments = []
                        comment = {}
                        comment["name"] = index_name
                        comment["type"] = "INDEX"
                        comment["comment"] = index_option[index_option.upper().find("COMMENT")+7:].strip().strip("'").strip('"')
                        comments.append(comment)
                        column_comment = self.build_table_comment(destination_schema, table_name, comments)
                        query += "%s" % (column_comment)
                elif token["command"] == "DROP INDEX FULL":
                    command = "DROP INDEX"
                    index_name = token["name"] + "_" + token["index_name"]
                    query = """%s %s.%s ;""" % (command, destination_schema, index_name)
            else:
                self.logger.debug('Only one table can be operated on at a time !')
        return query

    def build_enum_ddl(self, schema, enm_dic):
        """
            The method builds the enum DDL using the token data.
            The postgresql system catalog  is queried to determine whether the enum exists and needs to be altered.
            The alter is not written in the replica log table but executed as single statement as PostgreSQL do not allow the alter being part of a multi command
            SQL.

            :param schema: the schema where the enumeration is present
            :param enm_dic: a dictionary with the enumeration details
            :return: a dictionary with the pre_alter and post_alter statements (e.g. pre alter create type , post alter drop type)
            :rtype: dictionary
        """
        enum_name="enum_%s_%s" % (enm_dic['table'], enm_dic['column'])

        sql_check_enum = """
            SELECT
                typ.typcategory,
                typ.typname,
                sch_typ.nspname as typschema,
                CASE
                    WHEN typ.typcategory='E'
                    THEN
                    (
                        SELECT
                            array_agg(enumlabel)
                        FROM
                            pg_enum
                        WHERE
                            enumtypid=typ.oid
                    )
                END enum_list
            FROM
                pg_type typ
                INNER JOIN pg_namespace sch_typ
                    ON  sch_typ.oid = typ.typnamespace

            WHERE
                    sch_typ.nspname='%s'
                AND	typ.typname='%s'
            ;
        """
        stmt = self.pgsql_conn.prepare(sql_check_enum % (schema,  enum_name))
        type_data=stmt.first()
        return_dic = {}
        pre_alter = ""
        post_alter = ""
        column_type = enm_dic["type"]
        self.logger.debug(enm_dic)
        if type_data:
            if type_data[0] == 'E' and enm_dic["type"] == ColumnType.O_ENUM.value:
                self.logger.debug('There is already the enum %s, altering the type')
                new_enums = [val.strip() for val in enm_dic["enum_list"] if val.strip() not in type_data[3]]
                sql_add = []
                for enumeration in  new_enums:
                    sql_add =  """ALTER TYPE "%s"."%s" ADD VALUE '%s';""" % (type_data[2], enum_name, enumeration)
                    self.pgsql_conn.execute(sql_add)

            elif type_data[0] != 'E' and enm_dic["type"] == ColumnType.O_ENUM.value:
                self.logger.debug('The column will be altered in enum, creating the type')
                pre_alter = """CREATE TYPE "%s"."%s" AS ENUM (%s);""" % (schema,enum_name, enm_dic["enum_elements"])

            elif type_data[0] == 'E' and enm_dic["type"] != ColumnType.O_ENUM.value:
                self.logger.debug('The column is no longer an enum, dropping the type')
                post_alter = """DROP TYPE "%s"."%s"; """ % (schema,enum_name)
            column_type = """ "%s"."%s" """ % (schema, enum_name)
        elif not type_data and enm_dic["type"] == ColumnType.O_ENUM.value:
                self.logger.debug('Creating a new enumeration type %s' % (enum_name))
                pre_alter = """CREATE TYPE "%s"."%s" AS ENUM (%s);""" % (schema,enum_name, enm_dic["enum_elements"])
                column_type = """ "%s"."%s" """ % (schema, enum_name)

        return_dic["column_type"] = column_type
        return_dic["pre_alter"] = pre_alter
        return_dic["post_alter"]  = post_alter
        return return_dic


    def build_alter_table(self, schema, token):
        """
            The method builds the alter table statement from the token data.
            The function currently supports the following statements.
            DROP TABLE
            ADD COLUMN
            CHANGE
            MODIFY

            The change and modify are potential source of breakage for the replica because of
            the mysql implicit fallback data types.
            For better understanding please have a look to

            http://www.cybertec.at/why-favor-postgresql-over-mariadb-mysql/

            :param schema: The schema where the affected table is stored on postgres.
            :param token: A dictionary with the tokenised sql statement
            :return: query the DDL query in the PostgreSQL dialect
            :rtype: string

        """
        alter_cmd = []
        ddl_pre_alter = []
        ddl_post_alter = []
        query_cmd=token["command"]
        table_name=token["name"]

        for alter_dic in token["alter_cmd"]:
            if alter_dic["command"] == 'DROP':
                alter_cmd.append("%(command)s %(name)s CASCADE" % alter_dic)
            elif alter_dic["command"] == 'ADD':

                column_type=self.get_data_type(alter_dic, schema, table_name)
                column_name = alter_dic["name"]
                enum_list = str(alter_dic["dimension"]).replace("'", "").split(",")
                enm_dic = {'table':table_name, 'column':column_name, 'type':column_type, 'enum_list': enum_list, 'enum_elements':alter_dic["dimension"]}
                enm_alter = self.build_enum_ddl(schema, enm_dic)
                ddl_pre_alter.append(enm_alter["pre_alter"])
                column_type= enm_alter["column_type"]
                if 	column_type in [ColumnType.O_C_CHAR_VAR.value, ColumnType.O_C_CHARACTER.value, ColumnType.O_NUM.value,ColumnType.O_NUMBER.value, ColumnType.O_BIT.value, ColumnType.O_FLOAT.value]:
                        column_type = column_type+"("+str(alter_dic["dimension"])+")"
                if alter_dic["default"]:
                    default_value = "DEFAULT %s::%s" % (alter_dic["default"], column_type.strip())
                else:
                    default_value=""
                alter_cmd.append("%s \"%s\" %s NULL %s" % (alter_dic["command"], column_name, column_type, default_value))
            elif alter_dic["command"] == 'CHANGE':
                sql_rename = ""
                sql_type = ""
                old_column=alter_dic["old"]
                new_column=alter_dic["new"]
                column_name = old_column
                enum_list = str(alter_dic["dimension"]).replace("'", "").split(",")

                column_type=self.get_data_type(alter_dic, schema, table_name)
                default_sql = self.generate_default_statements(schema, table_name, old_column, new_column)
                enm_dic = {'table':table_name, 'column':column_name, 'type':column_type, 'enum_list': enum_list, 'enum_elements':alter_dic["dimension"]}
                enm_alter = self.build_enum_ddl(schema, enm_dic)

                ddl_pre_alter.append(enm_alter["pre_alter"])
                ddl_pre_alter.append(default_sql["drop"])
                ddl_post_alter.append(enm_alter["post_alter"])
                ddl_post_alter.append(default_sql["create"])
                column_type= enm_alter["column_type"]

                if column_type==ColumnType.O_C_CHAR_VAR.value or column_type==ColumnType.O_C_CHARACTER.value or\
                    column_type==ColumnType.O_NUM.value or column_type==ColumnType.O_BIT.value or\
                    column_type==ColumnType.O_NUMBER.value or column_type==ColumnType.O_FLOAT.value:
                        column_type=column_type+"("+str(alter_dic["dimension"])+")"
                sql_type = """ALTER TABLE "%s"."%s" ALTER COLUMN "%s" SET DATA TYPE %s  USING "%s"::%s ;;""" % (schema, table_name, old_column, column_type, old_column, column_type)
                if old_column != new_column:
                    sql_rename="""ALTER TABLE "%s"."%s" RENAME COLUMN "%s" TO "%s" ;""" % (schema, table_name, old_column, new_column)

                query = ' '.join(ddl_pre_alter)
                query += sql_type+sql_rename
                query += ' '.join(ddl_post_alter)
                return query

            elif alter_dic["command"] == 'MODIFY':
                column_type=self.get_data_type(alter_dic, schema, table_name)
                column_name = alter_dic["name"]

                enum_list = str(alter_dic["dimension"]).replace("'", "").split(",")
                default_sql = self.generate_default_statements(schema, table_name, column_name)
                enm_dic = {'table':table_name, 'column':column_name, 'type':column_type, 'enum_list': enum_list, 'enum_elements':alter_dic["dimension"]}
                enm_alter = self.build_enum_ddl(schema, enm_dic)

                ddl_pre_alter.append(enm_alter["pre_alter"])
                ddl_pre_alter.append(default_sql["drop"])
                ddl_post_alter.append(enm_alter["post_alter"])
                ddl_post_alter.append(default_sql["create"])
                column_type= enm_alter["column_type"]
                if column_type==ColumnType.O_C_CHAR_VAR.value or column_type==ColumnType.O_C_CHARACTER.value or\
                    column_type==ColumnType.O_NUM.value or column_type==ColumnType.O_BIT.value or\
                    column_type==ColumnType.O_FLOAT.value:
                        column_type=column_type+"("+str(alter_dic["dimension"])+")"
                query = ' '.join(ddl_pre_alter)
                query +=  """ALTER TABLE "%s"."%s" ALTER COLUMN "%s" SET DATA TYPE %s USING "%s"::%s ;""" % (schema, table_name, column_name, column_type, column_name, column_type)
                query += ' '.join(ddl_post_alter)
                return query
        query = ' '.join(ddl_pre_alter)
        query +=  """%s "%s"."%s" %s;""" % (query_cmd , schema,  table_name,', '.join(alter_cmd))
        query += ' '.join(ddl_post_alter)
        return query

    def build_talter_table(self, schema, token):
        """
        The method builds the alter table statement from the token data.
        build_talter_table is just like parse_t_alter_table
        Covers various types of alter table statements and guide different types of alter table statements to corresponding functions
        the function is like build_t_alter_x where x is a number
        and the build function build_t_alter_x is match the parse function parse_t_alter_x in sql_util

        For better understanding please have a look to
            the function parse_t_alter_table in sql_util.py

        After build ALTER TABLE statement in function build_t_alter_x, we can build the partition_options

            :param schema: The schema where the affected table is stored on postgres.
            :param token: A dictionary with the tokenised sql statement
            :return: query the DDL query in the PostgreSQL dialect
            :rtype: string

        """
        query = ""
        alter_id = token["alter_id"]
        if alter_id < 100:
            method_name = "build_t_alter_" + str(alter_id)
            method = getattr(self, method_name)
            query = method(schema, token)
        alt_par = token["alt_partition"]
        if not alt_par:
            return query
        for alt_item in alt_par["par_alt"]:
            querys = ""
            command = alt_item["command"]
            if command == "ADD":
                querys = "ALTER TABLE %s.%s %s ;" % (schema, alt_item["tbl_name"], alt_item["part"])
            elif command == "DROP":
                for dim in alt_item["part"]:
                    querys += "ALTER TABLE %s.%s DROP PARTITION %s ;" % (schema, alt_item["tbl_name"], dim)
            elif command == "TRUNCATE":
                for dim in alt_item["part"]:
                    if dim.upper() == "ALL":
                        dimall = self.get_table_partitions(schema, alt_item["tbl_name"])
                        for dim_item in dimall:
                            querys += "ALTER TABLE %s.%s TRUNCATE PARTITION %s ;" % (schema, alt_item["tbl_name"], dim_item)
                        break
                    querys += "ALTER TABLE %s.%s TRUNCATE PARTITION %s ;" % (schema, alt_item["tbl_name"], dim)
            elif command == "COALESCE":
                num = int(alt_item["part"][0])
                dims = self.get_table_partitions(schema, alt_item["tbl_name"])
                if self.list_or_hash(schema, alt_item["tbl_name"]):
                    print("List partition and hash partition do not support composite partition in openGauss, online migration cannot migrate this type")
                    return ""
                querys += "ALTER TABLE %s.%s MERGE PARTITIONS " % (schema, alt_item["tbl_name"])
                for dim in dims[-num:]:
                    querys += "%s," % dim
                querys = querys.rstrip(",")
                querys += " INTO PARTITION %s;" % (dims[-num-1])
            elif command == "REORGANIZE":
                if len(alt_item["part1"]) == 1:
                    querys += "ALTER TABLE %s.%s SPLIT PARTITION %s INTO (" % (schema, alt_item["tbl_name"], alt_item["part1"][0])
                    for dim in alt_item["part2"]:
                        if dim[1]:
                            querys += " PARTITION %s VALUES LESS THAN (%s)," % (dim[0], dim[1])
                        elif dim[2]:
                            if self.list_or_hash(schema, alt_item["tbl_name"]):
                                print("REORGANIZE or SPLIT LIST/HASH PARTITION is not support yet")
                                return ""
                            else:
                                #Inface Only LIST PARTITION can use VALUES IN partition definition
                                querys += " PARTITION %s VALUES (%s)," % (dim[0], dim[2])
                    querys = querys.rstrip(",")
                    querys += ");"
                elif len(alt_item["part2"]) == 1:
                    querys += "ALTER TABLE %s.%s MERGE PARTITIONS " % (schema, alt_item["tbl_name"])
                    for dim in alt_item["part1"]:
                        querys += "%s," % dim
                    querys = querys.rstrip(",")
                    querys += " INTO PARTITION %s;"%(alt_item["part2"][0][0])
                else:
                    querys += "ALTER TABLE %s.%s MERGE PARTITIONS " % (schema, alt_item["tbl_name"])
                    for dim in alt_item["part1"]:
                        querys += "%s," % (dim)
                    querys = querys.rstrip(",")
                    querys += " INTO PARTITION p0_tmp;"
                    querys += "ALTER TABLE %s.%s SPLIT PARTITION p0_tmp INTO (" % (
                    schema, alt_item["tbl_name"])
                    for dim in alt_item["part2"]:
                        if dim[1]:
                            querys += " PARTITION %s VALUES LESS THAN (%s)," % (dim[0], dim[1])
                        elif dim[2]:
                            if self.list_or_hash(schema, alt_item["tbl_name"]):
                                print("REORGANIZE or SPLIT LIST/HASH PARTITION is not support yet")
                                return ""
                            else:
                                querys += " PARTITION %s VALUES (%s)," %(dim[0], dim[2])
                    querys = querys.rstrip(",")
                    querys += ");"
            elif command == "EXCHANGE":
                querys += "ALTER TABLE %s.%s EXCHANGE PARTITION (%s) WITH TABLE %s.%s ;" % (schema, alt_item["tbl_name"], alt_item["part1"], schema, alt_item["part2"])
            else:
                print("The statement is not supported")
            query += querys
        return query

    def list_or_hash(self, schema, tbl_name):
        """
        Judge whether the partition table is list, hash or other types
        """
        self.logger.info("get partition type from the table %s.%s" % (schema, tbl_name))
        sql_lh = """
            SELECT partstrategy
            FROM
                pg_partition
            WHERE
                parentid in(
            SELECT relfilenode
            FROM
            pg_class tab
            INNER JOIN pg_namespace sch
            ON
            sch.oid=tab.relnamespace
            WHERE
            tab.relname='%s'
            AND sch.nspname='%s'
            )
            ;
        """
        stmt = self.pgsql_conn.prepare(sql_lh % (tbl_name, schema))
        lh = stmt()
        if lh[0][0] == b'l' or lh[0][0] == b'h':
            return True
        return False

    def build_t_alter_0(self, schema, token):
        """
        Please have a look to build_talter_table.
        build_t_alter_0 build sql token from sql.util.py parse_t_alter_0,In the ALTER TABLE statement, build the alter_option
        alter_option:
            | ADD [COLUMN] col_name column_definition
                [FIRST | AFTER col_name]
            | ADD [COLUMN] (col_name column_definition,...)
        """
        query = self.build_t_alter_1(schema, token)
        return query

    def build_t_alter_1(self, schema, token):
        """
        build_t_alter_1 build sql token from sql.util.py parse_t_alter_1,In the ALTER TABLE statement, build the alter_option
        alter_option:
            | ADD [COLUMN] col_name column_definition
                [FIRST | AFTER col_name]
            | ADD [COLUMN] (col_name column_definition,...)
        """
        query = ""
        tbl_name = token["name"]
        for alter_dic in token["alter_cmd"]:
            idx = 0
            tdx = 0
            compress_mode = ""
            collate = alter_dic["collate"]
            querys = """ALTER TABLE "%s"."%s" ADD COLUMN %s %s %s %s""" % (
                schema, tbl_name, alter_dic["col_name"], alter_dic["data_type"], compress_mode, collate)

            constraint_name = tbl_name + '_' + alter_dic["col_name"] + '_key'
            column_constraint = "CONSTRAINT %s " % (constraint_name)
            column_constraint += """%s %s %s""" % (
                alter_dic["null"], alter_dic["check"], alter_dic["default"]
            )
            if alter_dic["null"] or alter_dic["check"] or alter_dic["default"]:
                tdx = 1
            if alter_dic["unique"] or alter_dic["primary"] or alter_dic["references"]:
                idx = 1
            if alter_dic["unique"]:
                column_constraint += " UNIQUE"
            if alter_dic["primary"]:
                column_constraint += " PRIMARY KEY"
            # referenced
            if alter_dic["references"]:
                column_constraint += alter_dic["references"]
                if alter_dic["references_match"]:
                    column_constraint += alter_dic["references_match"]
                if alter_dic["references_on"]:
                    column_constraint += alter_dic["references_on"]
            if tdx+idx:
                querys = querys + column_constraint
            querys += ';'
            if idx:
                querys += "ALTER TABLE %s.%s RENAME CONSTRAINT %s TO %s;" % (schema, tbl_name, constraint_name, alter_dic["col_name"] )
            query += querys
        return query

    def build_t_alter_2(self, schema, token):
        """
        build_t_alter_2 build sql token from sql.util.py parse_t_alter_2,In the ALTER TABLE statement, build the alter_option
        alter_option:
            ADD {INDEX | KEY} [index_name]
                [index_type] (key_part,...) [index_option] ...
        """
        query = ""

        for alter_dic in token["alter_cmd"]:
            index_option = {}
            for i in range(1, 6):
                index_option[i] = alter_dic["index_option_"+str(i)]
            table_name = token["name"]
            index_name = alter_dic["index_name"]
            key_part = alter_dic["key_part"]
            if key_part.find("(") == -1:
                index_name = "%s_%s" % (table_name, index_name)
            else:
                [index_name, key_part] = self.find_key_from_express(schema, table_name, index_name, key_part)
            if index_option[2]:
                alter_dic["index_type"] = index_option[2]
            if self.have_table_partitions(schema, token["name"]):
                local = " local "
            else:
                local = ""
            query = "CREATE INDEX %s.%s ON %s.%s %s (%s) %s;" % (schema, index_name, schema, table_name, alter_dic["index_type"], key_part, local)
        return query

    def build_t_alter_3(self, schema, token):
        """
        build_t_alter_3 build sql token from sql.util.py parse_t_alter_3,In the ALTER TABLE statement, build the alter_option
        alter_option:
            ADD {FULLTEXT | SPATIAL} [INDEX | KEY] [index_name]
                (key_part,...) [index_option] ...
        """
        query = self.build_t_alter_2(schema, token)
        return query

    def build_t_alter_4(self, schema, token):
        """
        build_t_alter_4 build sql token from sql.util.py parse_t_alter_4,In the ALTER TABLE statement, build the alter_option
        alter_option:
            ADD [CONSTRAINT [symbol]] PRIMARY KEY
                [index_type] (key_part,...) [index_option] ...
        """
        query = ""
        tbl_name = token["name"]
        for alter_dic in token["alter_cmd"]:
            index_type = alter_dic["index_type"]
            key_part = alter_dic["key_part"]
            index_option = {}
            for i in range(1, 6):
                index_option[i] = alter_dic["index_option_" + str(i)]
            if index_option[2]:
                index_type = index_option[2]
            index_constraint = ""
            constraint_name = alter_dic["symbol"]
            if constraint_name:
                index_constraint = "CONSTRAINT %s " % (constraint_name)
            index_parameters = ""
            query = "ALTER TABLE %s.%s ADD %s PRIMARY KEY %s;" % (schema, tbl_name, index_constraint, key_part)
        return query

    def build_t_alter_5(self, schema, token):
        """
        build_t_alter_5 build sql token from sql.util.py parse_t_alter_5,In the ALTER TABLE statement, build the alter_option
        alter_option:
            ADD [CONSTRAINT [symbol]] UNIQUE [INDEX | KEY]
                [index_name] [index_type] (key_part,...)
                [index_option] ...
        """
        query = ""
        tbl_name = token["name"]
        for alter_dic in token["alter_cmd"]:
            index_constraint = ""
            constraint_name = alter_dic["symbol"]
            if constraint_name:
                index_constraint = "CONSTRAINT %s " % (constraint_name)
            index_name = alter_dic["index_name"]
            index_type = alter_dic["index_type"]
            key_part = alter_dic["key_part"]
            compress_mode = ""
            collate_collation = ""
            index_parameters = ""
            index_option = {}
            for i in range(1, 6):
                index_option[i] = alter_dic["index_option_" + str(i)]
            if index_option[2]:
                index_type = index_option[2]
            table_name = token["name"]
            if key_part.find("(") == -1:
                index_name = "%s_%s" % (table_name, index_name)
            else:
                [index_name, key_part] = self.find_key_from_express(schema, table_name, index_name, key_part)
            if index_option[2]:
                alter_dic["index_type"] = index_option[2]
            if self.have_table_partitions(schema, token["name"]):
                local = " local "
            else:
                local = ""
            query += """CREATE UNIQUE INDEX %s.%s ON %s.%s %s (%s) %s;""" % (schema, index_name, schema, table_name, alter_dic["index_type"], key_part, local)
        return query

    def build_t_alter_6(self, schema, token):
        """
        build_t_alter_6 build sql token from sql.util.py parse_t_alter_6,In the ALTER TABLE statement, build the alter_option
        alter_option:
            ADD [CONSTRAINT [symbol]] FOREIGN KEY
                [index_name] (col_name,...)
                reference_definition
        need to change child_parent_id_fkey to child_ibfk_1
        """
        query = ""
        tbl_name = token["name"]
        for alter_dic in token["alter_cmd"]:
            index_constraint = ""
            constraint_name = alter_dic["symbol"]
            if constraint_name:
                index_constraint = "CONSTRAINT %s_%s " % (tbl_name, constraint_name)
            index_name = alter_dic["index_name"]
            index_type = alter_dic["index_type"]
            key_part = alter_dic["key_part"]
            compress_mode = ""
            collate_collation = ""
            index_parameters = ""
            # ADD table_constraint
            refer = ""
            if alter_dic["references"]:
                references = alter_dic["references"].replace('REFERENCES', 'REFERENCES %s.'%(schema))
                refer += references
                if alter_dic["references_match"]:
                    refer += alter_dic["references_match"]
                if alter_dic["references_on"]:
                    refer += alter_dic["references_on"]
            query = """ALTER TABLE "%s"."%s" ADD %s FOREIGN KEY %s""" % (
                schema, tbl_name, index_constraint, key_part)
            if refer:
                query += " %s;" % refer
            else:
                query += ';'
            if not index_constraint:
                key_p = key_part.replace("(","").replace(")","").strip()
                i = 1
                old_name = "%s_%s_fkey" % (tbl_name, key_p)
                new_name = "%s_%s_ibfk_%s" % (tbl_name, tbl_name, i)
                query += "ALTER TABLE %s.%s RENAME CONSTRAINT %s TO %s;" % (schema, tbl_name, old_name, new_name)
        return query

    def build_t_alter_8(self, schema, token):
        """
        build_t_alter_8 build sql token from sql.util.py parse_t_alter_8,In the ALTER TABLE statement, build the alter_option
        alter_option:
            DROP {CHECK | CONSTRAINT} symbol
        """
        query = ""
        tbl_name = token["name"]
        for alter_dic in token["alter_cmd"]:
            cons = self.get_table_constraints(schema, token['name'])
            indx = self.get_table_indexes(schema, token['name'])
            symbol = "%s_%s" % (tbl_name, alter_dic["symbol"])
            if symbol in cons:
                query = "ALTER TABLE %s.%s DROP CONSTRAINT %s;" % (schema, tbl_name, symbol)
            elif symbol in indx:
                query = "DROP INDEX %s.%s;" % (schema, symbol)
        return query

    def build_t_alter_13(self, schema, token):
        """
        build_t_alter_13 build sql token from sql.util.py parse_t_alter_13,In the ALTER TABLE statement, match the alter_option
        alter_option:
            CHANGE [COLUMN] old_col_name new_col_name column_definition
            [FIRST | AFTER col_name]
            THE FIRST KIND column definition
        """
        query = self.build_alter_table(schema, token["alter_cmd"])
        return query

    def build_t_alter_14(self, schema, token):
        """
        build_t_alter_14 build sql token from sql.util.py parse_t_alter_14,In the ALTER TABLE statement, match the alter_option
        alter_option:
            CHANGE [COLUMN] old_col_name new_col_name column_definition
            [FIRST | AFTER col_name]
            THE second kind column definition
        """
        query = self.build_alter_table(schema, token["alter_cmd"])
        return query

    def build_t_alter_19(self, schema, token):
        """
        build_t_alter_19 build sql token from sql.util.py parse_t_alter_19,In the ALTER TABLE statement, build the alter_option
        alter_option:
            DROP {INDEX | KEY} index_name
        """
        query = ""
        for alter_dic in token["alter_cmd"]:
            query = "DROP INDEX %s.%s_%s;" % (schema, token["name"], alter_dic["col_name"])
        return query

    def build_t_alter_20(self, schema, token):
        """
        build_t_alter_20 build sql token from sql.util.py parse_t_alter_20,In the ALTER TABLE statement, build the alter_option
        alter_option:
            DROP PRIMARY KEY
        """
        query = ""
        self.__drop_primary_key(schema, token)
        return query

    def build_t_alter_21(self, schema, token):
        """
        build_t_alter_21 build sql token from sql.util.py parse_t_alter_21,In the ALTER TABLE statement, build the alter_option
        alter_option:
            DROP FOREIGN KEY fk_symbol
        """
        query = ""
        for alter_dic in token["alter_cmd"]:
            query = "ALTER TABLE %s.%s DROP CONSTRAINT %s_%s;" % (schema, token["name"], token["name"], alter_dic["col_name"])
        return query

    def build_t_alter_22(self, schema, token):
        """
        build_t_alter_22 build sql token from sql.util.py parse_t_alter_22,In the ALTER TABLE statement, build the alter_option
        alter_option:
            DROP [COLUMN] col_name
        If you want to drop constraints with, please use DROP CONSTRAINT symbol
        """
        query = ""
        for alter_dic in token["alter_cmd"]:
            query = "ALTER TABLE %s.%s DROP COLUMN %s;" % (schema, token["name"], alter_dic["col_name"])
        return query

    def build_t_alter_25(self, schema, token):
        """
        build_t_alter_25 build sql token from sql.util.py parse_t_alter_25,In the ALTER TABLE statement, match the alter_option
        alter_option:
            MODIFY [COLUMN] col_name column_definition
            [FIRST | AFTER col_name
            the second kind of column_definition
        """
        query = self.build_alter_table(schema, token["alter_cmd"])
        return query

    def build_t_alter_26(self, schema, token):
        """
        build_t_alter_26 build sql token from sql.util.py parse_t_alter_26,In the ALTER TABLE statement, match the alter_option
        alter_option:
            MODIFY [COLUMN] col_name column_definition
            [FIRST | AFTER col_name
            the first kind of column_definition
        """
        query = self.build_alter_table(schema, token["alter_cmd"])
        return query


    def build_t_alter_30(self, schema, token):
        """
        build_t_alter_30 build sql token from sql.util.py parse_t_alter_30,In the ALTER TABLE statement, build the alter_option
        alter_option:
            RENAME [TO | AS] new_tbl_name
        """
        query = ""
        for alter_dic in token["alter_cmd"]:
            tbl_name = token["name"]
            new_name = alter_dic["new_name"]
            query = """ALTER TABLE "%s"."%s" RENAME TO "%s" ;""" % (schema, tbl_name, new_name)
            indexs = self.get_table_indexes(schema, tbl_name)
            for tmp in indexs:
                new_index = tmp[tmp.find(tbl_name)+len(tbl_name):].strip()
                new_index = new_name + new_index
                querys = """ALTER INDEX "%s"."%s" RENAME TO %s ;""" % (schema, tmp, new_index)
                query += querys
            constraints = self.get_table_constraints(schema, tbl_name)
            tbl = tbl_name + '_'
            for tmp in constraints:
                if tmp.find(tbl) != -1:
                    new_index = tmp[tmp.find(tbl)+len(tbl):].strip()
                    new_index = new_name + "_" + new_index
                    querys = """ALTER TABLE "%s"."%s" RENAME CONSTRAINT %s TO %s ;""" % (schema, new_name, tmp, new_index)
                    query += querys

        return query

    def get_table_indexes(self, schema, tbl_name):
        """
        This function is used to extract all index names in the table
        """
        self.logger.info("get indexes from the table %s.%s" % (schema, tbl_name))
        sql_indexes = """
                    SELECT indexname
                    FROM
                        pg_indexes
                    WHERE
                    tablename='%s'
                    AND schemaname='%s'
                    ;
                """
        stmt = self.pgsql_conn.prepare(sql_indexes % (tbl_name, schema))
        indexes_names = stmt()
        if indexes_names is None:
            return []
        indexes = []
        for part in indexes_names:
            if part[0] != tbl_name:
                indexes.append(part[0])
        return indexes

    def get_table_constraints(self, schema, tbl_name):
        """
        This function is used to extract all constraints names in the table
        """
        self.logger.info("get constraints from the table %s.%s" % (schema, tbl_name))
        sql_constraints = """
                    SELECT conname 
                    FROM pg_constraint con
                        INNER JOIN pg_class tab 
                        ON con.conrelid=tab.oid
                    WHERE tab.relname='%s';
                    ;
                """
        stmt = self.pgsql_conn.prepare(sql_constraints % (tbl_name))
        cons_names = stmt()
        if cons_names is None:
            return []
        cons = []
        for part in cons_names:
            if part[0] != tbl_name:
                cons.append(part[0])
        return cons

    def find_same_index_or_constraint(self, schema, tbl_name, str_name):
        index_name = self.get_table_indexes(schema, tbl_name)
        i = 1
        tmp = str_name + "_" + str(i)
        while tmp in index_name:
            i += 1
            tmp = str_name + "_" + str(i)
        constraint_name = self.get_table_constraints(schema, tbl_name)
        j = 1
        tmp1 = str_name + "_" + str(j)
        while tmp1 in constraint_name:
            j += 1
            tmp1 = str_name + "_" + str(j)
        if j > i:
            tmp = tmp1
        return tmp

    def find_key_from_express(self, schema, tbl_name, index_name, key_part):
        test = self.get_table_columns(schema, tbl_name)
        if key_part[0] == '(':
            key_p = key_part[1:-1].strip()
            if key_p.find('(') != -1:
                key_part = key_p
        part1 = key_part[:key_part.find('(')].strip()
        part2 = key_part.replace(part1, "").replace("(", "").replace(")", "").strip()
        if part1 in test:
            if index_name == "":
                index_name = part1
            return [tbl_name + "_" + index_name, part1]
        elif part2 in test:
            if index_name == "":
                index_name = part2
            return [tbl_name + "_" + index_name, part2]
        elif part1=='':
            return [tbl_name + "_" + index_name, part2]
        else:
            function_index = "function_index"
            tmp = self.find_same_index_or_constraint(schema, tbl_name, function_index)
            function_index = tbl_name + "_" + tmp
            return [function_index, key_part]

    def get_table_columns(self, schema, tbl_name):
        sql_metadata = """
            SELECT
                col.attname as column_name
            FROM
                pg_catalog.pg_attribute col
                INNER JOIN pg_catalog.pg_type typ
                    ON  col.atttypid=typ.oid
                INNER JOIN pg_catalog.pg_namespace sch
                    ON typ.typnamespace=sch.oid
            WHERE
                    col.attrelid = '%s'::regclass
                AND NOT col.attisdropped
                AND col.attnum>0
            ORDER BY
                col.attnum
            ;
            ;
                """
        tab_regclass = "%s.%s" % (schema, tbl_name)
        stmt = self.pgsql_conn.prepare(sql_metadata % tab_regclass)
        column_name = stmt()
        columns = []
        for part in column_name:
            if part[0] != tbl_name:
                columns.append(part[0])
        return columns

    def have_table_partitions(self, schema, tbl_name):
        partition = self.get_table_partitions(schema, tbl_name)
        if partition:
            return True
        else:
            return False

    def get_table_partitions(self, schema, tbl_name):
        """
        This function is used to extract all partition names in the partition table
        """
        self.logger.info("get partitions from the table %s.%s" % (schema, tbl_name))
        sql_partition = """
            SELECT relname
            FROM
                pg_partition
            WHERE
                parentid in(
            SELECT relfilenode
            FROM
            pg_class tab
            INNER JOIN pg_namespace sch
            ON
            sch.oid=tab.relnamespace
            WHERE
            tab.relname='%s'
            AND sch.nspname='%s'
            )
            ;
        """
        stmt = self.pgsql_conn.prepare(sql_partition % (tbl_name, schema))
        partition_names = stmt()
        if partition_names is None:
            return []
        partitions = []
        for part in partition_names:
            if part[0]!=tbl_name:
                partitions.append(part[0])
        return partitions

    def __drop_primary_key(self, schema, token):
        """
            The method drops the primary key for the table.
            As tables without primary key cannot be replicated the method calls unregister_table
            to remove the table from the replica set.
            The drop constraint statement is not built from the token but generated from the information_schema.

            :param schema: The table's schema
            :param token: the tokenised query for drop primary key
        """
        self.logger.info("dropping primary key for table %s.%s" % (schema, token["name"],))
        sql_gen="""
            SELECT  DISTINCT
                format('ALTER TABLE %%I.%%I DROP CONSTRAINT %%I;',
                table_schema,
                table_name,
                constraint_name
                )
            FROM
                information_schema.key_column_usage
            WHERE
                    table_schema='%s'
                AND table_name='%s'
            ;
        """
        stmt = self.pgsql_conn.prepare(sql_gen % (schema, token["name"]))
        value_check=stmt.first()
        if value_check:
            sql_drop=value_check
            self.pgsql_conn.execute(sql_drop)
            # Now we support replica table without pk, so no need to call self.unregister_table(schema, token["name"])

    def __count_active_sources(self):
        """
            The method counts all the sources with state not in 'ready' or 'stopped'.
            The method assumes there is a database connection active.
        """
        sql_count = """
            SELECT
                count(*)
            FROM
                sch_chameleon.t_sources
            WHERE
                enm_status NOT IN ('ready','stopped','initialised')
            ;
        """
        stmt = self.pgsql_conn.prepare(sql_count)
        source_count = stmt.first()
        return source_count

    def get_active_sources(self):
        """
            The method counts all the sources with state not in 'ready' or 'stopped'.
            The method assumes there is a database connection active.
        """
        self.connect_db()
        sql_get = """
            SELECT
                t_source
            FROM
                sch_chameleon.t_sources
            WHERE
                enm_status NOT IN ('ready','stopped')
            ;
        """
        stmt = self.pgsql_conn.prepare(sql_get)
        source_get = stmt()
        self.disconnect_db()
        return source_get

    def upgrade_catalogue_v20(self):
        """
            The method applies the migration scripts to the replica catalogue version 2.0.
            The method checks that all sources are in stopped or ready state.
        """
        sql_view = """
            CREATE OR REPLACE VIEW sch_chameleon.v_version
                AS
                    SELECT %s::TEXT t_version
        ;"""

        self.connect_db()
        sources_active = self.__count_active_sources()
        if sources_active[0] == 0:
            catalog_version = self.get_catalog_version()
            catalog_number = int(''.join([value  for value in catalog_version.split('.')]))
            self.connect_db()
            for migration in self.migrations:
                migration_version = migration["version"]
                migration_number = int(''.join([value  for value in migration_version.split('.')]))
                if migration_number>catalog_number:
                    migration_file_name = '%s/%s' % (self.sql_upgrade_dir, migration["script"])
                    print("Migrating the catalogue from version %s to version %s" % (catalog_version,  migration_version))
                    migration_data = open(migration_file_name, 'r')
                    migration_sql = migration_data.read()
                    migration_data.close()
                    self.pgsql_conn.execute(migration_sql)
                    self.pgsql_conn.execute(sql_view % (migration_version))
        else:
            print('There are sources in running or syncing state. You shall stop all the replica processes before upgrading the catalogue.')
            sys.exit()



    def upgrade_catalogue_v1(self):
        """
            The method upgrade a replica catalogue  from version 1 to version 2.
            The original catalogue is not altered but just renamed.
            All the existing data are transferred into the new catalogue loaded  using the create_schema.sql file.
        """
        replay_max_rows = 10000
        self.__v2_schema = "_sch_chameleon_version2"
        self.__current_schema = "sch_chameleon"
        self.__v1_schema = "_sch_chameleon_version1"
        self.connect_db()
        upgrade_possible = True

        sql_get_min_max = """
            SELECT
                sch_chameleon.binlog_max(
                    ARRAY[
                        t_binlog_name,
                        i_binlog_position::text
                    ]
                ),
                sch_chameleon.binlog_min(
                    ARRAY[
                        t_binlog_name,
                        i_binlog_position::text
                    ]
                )
            FROM
                sch_chameleon.t_replica_tables
            WHERE
                i_id_source=%s
            ;

        """

        sql_migrate_tables = """
            WITH t_old_new AS
                (
                SELECT
                    old.i_id_source as id_source_old,
                    new.i_id_source as id_source_new,
                    new.t_dest_schema
                FROM
                    _sch_chameleon_version1.t_sources  old
                    INNER JOIN (
                            SELECT
                                i_id_source,
                                (jsonb_each_text(jsb_schema_mappings)).value as t_dest_schema
                            FROM
                                sch_chameleon.t_sources

                           ) new
                    ON old.t_dest_schema=new.t_dest_schema
                )
            INSERT INTO sch_chameleon.t_replica_tables
                (
                    i_id_source,
                    v_table_name,
                    v_schema_name,
                    v_table_pkey,
                    t_binlog_name,
                    i_binlog_position,
                    b_replica_enabled
                )

            SELECT
                id_source_new,
                v_table_name,
                t_dest_schema,
                string_to_array(replace(v_table_pkey[1],'"',''),',') as table_pkey,
                bat.t_binlog_name,
                bat.i_binlog_position,
                't'::boolean as b_replica_enabled

            FROM
                _sch_chameleon_version1.t_replica_batch bat
                INNER JOIN _sch_chameleon_version1.t_replica_tables tab
                ON tab.i_id_source=bat.i_id_source

                INNER JOIN t_old_new
                ON tab.i_id_source=t_old_new.id_source_old
            WHERE
                    NOT bat.b_processed
                AND  bat.b_started

        ;
        """

        sql_mapping = """

            WITH t_mapping AS
                (
                    SELECT json_each_text('%s'::json) AS t_sch_map
                )

            SELECT
                mapped_schema=config_schema as match_mapping,
                mapped_list,
                config_list
            FROM
            (
                SELECT
                    count(dst.t_sch_map) as mapped_schema,
                    string_agg((dst.t_sch_map).value,' ') as mapped_list
                FROM
                    t_mapping dst
                    INNER JOIN sch_chameleon.t_sources src
                    ON
                            src.t_dest_schema=(dst.t_sch_map).value
                        AND	src.t_source_schema= (dst.t_sch_map).key
            ) cnt_map,
            (
                SELECT
                    count(t_sch_map) as config_schema,
                    string_agg((t_sch_map).value,' ') as config_list
                FROM
                    t_mapping

            ) cnt_cnf
            ;

        """

        self.logger.info("Checking if we need to replay data in the existing catalogue")
        sql_check = """
            SELECT
                src.i_id_source,
                src.t_source,
                count(log.i_id_event)
            FROM
                sch_chameleon.t_log_replica log
                INNER JOIN sch_chameleon.t_replica_batch bat
                    ON log.i_id_batch=bat.i_id_batch
                INNER JOIN sch_chameleon.t_sources src
                    ON src.i_id_source=bat.i_id_source
            GROUP BY
                src.i_id_source,
                src.t_source
            ;

        """
        stmt = self.pgsql_conn.prepare(sql_check)
        source_replay = stmt()
        if source_replay:
            for source in source_replay:
                id_source = source[0]
                source_name = source[1]
                replay_rows = source[2]
                self.logger.info("Replaying last %s rows for source %s " % (replay_rows, source_name))
                continue_loop = True
                while continue_loop:
                    sql_replay = """SELECT sch_chameleon.fn_process_batch(%s,%s);"""
                    stmt = self.pgsql_conn.prepare(sql_replay% (replay_max_rows, id_source, ))
                    replay_status = stmt.first()
                    continue_loop = replay_status
                    if continue_loop:
                        self.logger.info("Still replaying rows for source %s" % ( source_name, ) )
        self.logger.info("Checking if the schema mappings are correctly matched")
        for source in self.sources:
            schema_mappings = json.dumps(self.sources[source]["schema_mappings"])
            stmt = self.pgsql_conn.prepare(sql_mapping% (schema_mappings, ))
            config_mapping = stmt.first()
            source_mapped = config_mapping[0]
            list_mapped = config_mapping[1]
            list_config = config_mapping[2]
            if not source_mapped:
                self.logger.error("Checks for source %s failed. Matched mappings %s, configured mappings %s" % (source, list_mapped, list_config))
                upgrade_possible = False
        if upgrade_possible:
            try:
                self.logger.info("Renaming the old schema %s in %s " % (self.__v2_schema, self.__v1_schema))
                sql_rename_old = ("ALTER SCHEMA \"{}\" RENAME TO \"{}\";").format(self.__current_schema, self.__v1_schema)
                self.pgsql_conn.execute(sql_rename_old)
                self.logger.info("Installing the new replica catalogue " )
                self.create_replica_schema()
                for source in self.sources:
                    self.source = source
                    self.add_source()

                self.pgsql_conn.execute(sql_migrate_tables)
                for source in self.sources:
                    self.source = source
                    self.set_source_id()
                    stmt = self.pgsql_conn.prepare(sql_get_min_max% (self.i_id_source, ))
                    min_max = stmt.first()
                    max_position = min_max[0]
                    min_position = min_max[1]

                    master_data = {}
                    master_status = []
                    master_data["File"] = min_position[0]
                    master_data["Position"] = min_position[1]
                    master_status.append(master_data)
                    self.save_master_status(master_status)

                    master_status = []
                    master_data["File"] = max_position[0]
                    master_data["Position"] = max_position[1]
                    master_status.append(master_data)
                    self.set_source_highwatermark(master_status, False)

            except:
                self.rollback_upgrade_v1()
        else:
            self.logger.error("Sanity checks for the schema mappings failed. Aborting the upgrade")
            self.rollback_upgrade_v1()
        self.disconnect_db()

    def rollback_upgrade_v1(self):
        """
            The procedure rollsback the upgrade dropping the schema sch_chameleon and renaming the version 1 to the
        """
        sql_check="""
            SELECT
                count(*)
            FROM
                information_schema.schemata
            WHERE
                schema_name='%s'
        """
        stmt = self.pgsql_conn.prepare(sql_check % (self.__v1_schema))
        v1_schema = stmt.first()
        if v1_schema == 1:
            self.logger.info("The schema %s exists, rolling back the changes" % (self.__v1_schema))
            stmt = self.pgsql_conn.prepare(sql_check% (self.__current_schema, ))
            curr_schema = stmt.first()
            if curr_schema == 1:
                self.logger.info("Renaming the current schema %s in %s" % (self.__current_schema, self.__v2_schema))
                sql_rename_current = ("ALTER SCHEMA \"{}\" RENAME TO \"{}\";").format(self.__current_schema, self.__v2_schema)
                self.pgsql_conn.execute(sql_rename_current)
            sql_rename_old = ("ALTER SCHEMA \"{}\" RENAME TO \"{}\";").format((self.__v1_schema), (self.__current_schema))
            self.pgsql_conn.execute(sql_rename_old)
        else:
            self.logger.info("The old schema %s does not exists, aborting the rollback" % (self.__v1_schema))
            sys.exit()
        self.logger.info("Rollback successful. Please note the catalogue version 2 has been renamed to %s for debugging.\nYou will need to drop it before running another upgrade" % (self.__v2_schema, ))



    def unregister_table(self, schema,  table):
        """
            This method is used to remove a table from the replica catalogue.
            The table is just deleted from the table sch_chameleon.t_replica_tables.

            :param schema: the schema name where the table is stored
            :param table: the table name to remove from t_replica_tables
        """
        self.logger.info("unregistering table %s.%s from the replica catalog" % (schema, table,))
        sql_delete=""" DELETE FROM sch_chameleon.t_replica_tables
                    WHERE
                            v_table_name='%s'
                        AND	v_schema_name='%s'
                    ;
                        """
        self.pgsql_conn.execute(sql_delete % (table, schema))

    def cleanup_source_tables(self):
        """
            The method cleans up the tables for active source in sch_chameleon.t_replica_tables.

        """
        self.logger.info("deleting all the table references from the replica catalog for source %s " % (self.source,))
        sql_delete=""" DELETE FROM sch_chameleon.t_replica_tables
                    WHERE
                        i_id_source=%s
                    ;
                        """
        self.pgsql_conn.execute(sql_delete % (self.i_id_source))



    def cleanup_table_events(self):
        """
            The method cleans up the log events in the source's log tables for the given tables

        """
        sql_get_log_tables = """
            SELECT
                v_log_table
            FROM
                sch_chameleon.t_sources
            WHERE
                i_id_source=%s
            ;
        """

        stmt = self.pgsql_conn.prepare(sql_get_log_tables % (self.i_id_source))
        log_tables = stmt.first()
        list_conditions = []
        for schema in self.schema_tables:
            for table_name in self.schema_tables[schema]:
                table_schema = self.schema_loading[schema]["destination"]
                where_cond = "format('%%I.%%I','%s','%s')" % (table_schema, table_name)
                list_conditions.append(where_cond)
        sql_cleanup = "DELETE FROM sch_chameleon.\"{}\" WHERE format('%%I.%%I',v_schema_name,v_table_name) IN (%s) ;" % ' ,'.join(list_conditions)
        for log_table in log_tables:
            self.logger.debug("Cleaning up log events in log table %s " % (log_table,))
            sql_clean_log = (sql_cleanup).format((log_table))
            self.pgsql_conn.execute(sql_clean_log)


    def __count_table_schema(self, table, schema):
        """
            The method checks if the table exists in the given schema.

            :param table: the table's name
            :param schema: the postgresql schema where the table should exist
            :return: the count from pg_tables where table name and schema name are the given parameters
            :rtype: integer
        """
        sql_check = """
            SELECT
                count(*)
            FROM
                pg_tables
            WHERE
                    schemaname='%s'
                AND	tablename='%s';
        """
        stmt = self.pgsql_conn.prepare(sql_check % (schema, table ))
        count_table = stmt.first()
        return count_table


    def write_ddl(self, token, query_data, destination_schema):
        """
            The method writes the DDL built from the tokenised sql into PostgreSQL.

            :param token: the tokenised query
            :param query_data: query's metadata (schema,binlog, etc.)
            :param destination_schema: the postgresql destination schema determined using the schema mappings.
        """
        pg_ddl = self.generate_ddl(token, destination_schema)
        self.logger.debug("Translated query: %s " % (pg_ddl,))
        log_table = query_data["log_table"]
        insert_vals = (
                query_data["batch_id"],
                token["name"],
                query_data["schema"],
                query_data["binlog"],
                query_data["logpos"],
                pg_ddl
            )
        sql_insert=("""
            INSERT INTO "sch_chameleon".\"{}\"
                (
                    i_id_batch,
                    v_table_name,
                    v_schema_name,
                    enm_binlog_event,
                    t_binlog_name,
                    i_binlog_position,
                    t_query
                )
            VALUES
                (
                    %s,
                    '%s',
                    '%s',
                    'ddl',
                    '%s',
                    %s,
                    '%s'
                )
            ;
        """).format((log_table))

        self.pgsql_conn.execute(sql_insert % insert_vals)


    def get_tables_disabled(self, format="csv"):
        """
            The method returns a CSV or a python list of tables excluded from the replica.
            The origin's schema is determined from the source's schema mappings jsonb.

            :return: CSV list of tables excluded from the replica
            :rtype: text

        """
        if format=='csv':
            select_clause = """string_agg(format('%s.%s',(t_mappings).key,v_table_name),',') """
        elif format=='list':
            select_clause = """array_agg(format('%s.%s',(t_mappings).key,v_table_name)) """
        sql_get = """
            SELECT
                %s
            FROM
                sch_chameleon.t_replica_tables tab
                INNER JOIN
                (
                    SELECT
                        i_id_source,
                        jsonb_each_text(jsb_schema_mappings) as t_mappings
                    FROM
                    sch_chameleon.t_sources

                ) src
                ON
                        tab.i_id_source=src.i_id_source
                    AND	tab.v_schema_name=(t_mappings).value
            WHERE
                NOT tab.b_replica_enabled
            ;
        """ % select_clause
        stmt = self.pgsql_conn.prepare(sql_get)
        tables_disabled = stmt.first()
        return tables_disabled

    def swap_source_log_table(self):
        """
            The method swaps the sources's log table and returns the next log table stored in the v_log_table array.
            The method expects an active database connection.

            :return: The t_log_replica's active subpartition
            :rtype: text

        """
        sql_log_table="""
            UPDATE sch_chameleon.t_sources
            SET
                v_log_table=ARRAY[v_log_table[2],v_log_table[1]]

            WHERE
                i_id_source=%s
            RETURNING
                v_log_table[1]
            ;
        """
        self.set_source_id()
        stmt = self.pgsql_conn.prepare(sql_log_table % (self.i_id_source))
        log_table = stmt.first()
        self.logger.debug("New log table : %s " % (log_table,))
        return log_table




    def get_batch_data(self):
        """
            The method updates the batch status to started for the given source_id and returns the
            batch informations.

            :return: psycopg2 fetchall results without any manipulation
            :rtype: psycopg2 tuple

        """
        sql_batch="""
            WITH t_created AS
                (
                    SELECT
                        max(ts_created) AS ts_created
                    FROM
                        sch_chameleon.t_replica_batch
                    WHERE
                            NOT b_processed
                        AND	NOT b_replayed
                        AND	i_id_source=%s
                )
            UPDATE sch_chameleon.t_replica_batch
            SET
                b_started=True
            FROM
                t_created
            WHERE
                    t_replica_batch.ts_created=t_created.ts_created
                AND	i_id_source=%s
            RETURNING
                i_id_batch,
                t_binlog_name,
                i_binlog_position,
                v_log_table,
                t_gtid_set

            ;
        """
        stmt = self.pgsql_conn.prepare(sql_batch % (self.i_id_source, self.i_id_source,  ))
        return stmt()


    def drop_replica_schema(self):
        """
            The method removes the service schema discarding all the replica references.
            The replicated tables are kept in place though.
        """
        self.logger.debug("Trying to connect to the destination database.")
        self.connect_db()
        file_schema = open(self.sql_dir+"drop_schema.sql", 'r')
        sql_schema = file_schema.read()
        file_schema.close()
        self.pgsql_conn.execute(sql_schema)

    def get_catalog_version(self):
        """
            The method returns if the replica schema's version

            :return: the version string selected from sch_chameleon.v_version
            :rtype: text
        """
        schema_version = None
        sql_version = """
            SELECT
                t_version
            FROM
                sch_chameleon.v_version
            ;
        """
        self.connect_db()
        try:
            stmt = self.pgsql_conn.prepare(sql_version)
            schema_version = stmt.first()
            self.disconnect_db()
        except:
            schema_version = None
        return schema_version

    def check_replica_schema(self):
        """
            The method checks if the sch_chameleon exists

            :return: count from information_schema.schemata
            :rtype: integer
        """
        sql_check="""
            SELECT
                count(*)
            FROM
                information_schema.schemata
            WHERE
                schema_name='sch_chameleon'
        """

        stmt = self.pgsql_conn.prepare(sql_check)
        num_schema = stmt.first()
        return num_schema

    def check_schema_mappings(self, exclude_current_source=False):
        """

            The default is false.

            The method checks if there is already a destination schema in the stored schema mappings.
            As each schema should be managed by one mapping only, if the method returns None  then
            the source can be store safely. Otherwise the action. The method doesn't take any decision
            leaving this to the calling methods.
            The method assumes there is a database connection active.
            The method returns a list or none.
            If the list is returned then contains the count and the destination schema name
            that are already present in the replica catalogue.

            :param exclude_current_source: If set to true the check excludes the current source name from the check.
            :return: the schema already mapped in the replica catalogue.
            :rtype: list
        """
        if exclude_current_source:
            exclude_id = self.i_id_source
        else:
            exclude_id = -1
        schema_mappings = json.dumps(self.sources[self.source]["schema_mappings"])
        if schema_mappings=='null':
            print("Schema mapping cannot be empty. Check your configuration file.")
            sys.exit()
        else:
            sql_check = """
                WITH t_check  AS
                (
                        SELECT
                            (jsonb_each_text(jsb_schema_mappings)).value AS dest_schema
                        FROM
                            sch_chameleon.t_sources
                        WHERE
                            i_id_source <> %s
                    UNION ALL
                        SELECT
                            value AS dest_schema
                        FROM
                            json_each_text('%s'::json)
                )
            SELECT
                count(dest_schema),
                dest_schema
            FROM
                t_check
            GROUP BY
                dest_schema
            HAVING
                count(dest_schema)>1
            ;
            """
            stmt = self.pgsql_conn.prepare(sql_check % (exclude_id, schema_mappings, ))
            check_mappings = stmt.first()
            return check_mappings

    def check_source(self):
        """
            The method checks if the source name stored in the class variable self.source is already present.
            As this method is used in both add and drop source it just retuns the count of the sources.
            Any decision about the source is left to the calling method.
            The method assumes there is a database connection active.

        """
        sql_check = """
            SELECT
                count(*)
            FROM
                sch_chameleon.t_sources
            WHERE
                t_source='%s';
        """
        stmt = self.pgsql_conn.prepare(sql_check % (self.source, ))
        num_sources = stmt.first()
        return num_sources

    def add_source(self):
        """
            The method adds a new source to the replication catalog.
            The method calls the function fn_refresh_parts() which generates the log tables used by the replica.
            If the source is already present a warning is issued and no other action is performed.
        """
        self.logger.debug("Checking if the source %s already exists" % self.source)
        self.connect_db()
        num_sources = self.check_source()

        if num_sources == 0:
            check_mappings = self.check_schema_mappings()
            if check_mappings:
                self.logger.error("Could not register the source %s. There is a duplicate destination schema in the schema mappings." % self.source)
            else:
                self.logger.debug("Adding source %s " % self.source)
                schema_mappings = json.dumps(self.sources[self.source]["schema_mappings"])
                source_type = self.sources[self.source]["type"]
                log_table_1 = "t_log_replica_%s_1" % self.source
                log_table_2 = "t_log_replica_%s_2" % self.source
                sql_add = """
                    INSERT INTO sch_chameleon.t_sources
                        (
                            t_source,
                            jsb_schema_mappings,
                            v_log_table,
                            enm_source_type
                        )
                    VALUES
                        (
                            '%s',
                            '%s',
                            ARRAY['%s','%s'],
                            '%s'
                        )
                    ;
                """
                self.pgsql_conn.execute(sql_add% (self.source, schema_mappings, log_table_1, log_table_2, source_type))

                sql_parts = """SELECT sch_chameleon.fn_refresh_parts() ;"""
                self.pgsql_conn.execute(sql_parts)
                self.insert_source_timings()
        else:
            self.logger.warning("The source %s already exists" % self.source)

    def drop_source(self):
        """
            The method deletes the source from the replication catalogue.
            The log tables are dropped as well, discarding any replica reference for the source.
        """
        self.logger.debug("Deleting the source %s " % self.source)
        self.connect_db()
        num_sources = self.check_source()
        if num_sources == 1:
            sql_delete = """ DELETE FROM sch_chameleon.t_sources
                        WHERE  t_source='%s'
                        RETURNING v_log_table
                        ; """
            stmt = self.pgsql_conn.prepare(sql_delete% (self.source, ))
            source_drop = stmt.first()
            for log_table in source_drop:
                sql_drop = """DROP TABLE sch_chameleon."%s"; """ % (log_table)
                try:
                    self.pgsql_conn.execute(sql_drop)
                except:
                    self.logger.debug("Could not drop the table sch_chameleon.%s you may need to remove it manually." % log_table)
        else:
            self.logger.debug("There is no source %s registered in the replica catalogue" % self.source)

    def get_schema_list(self):
        """
            The method gets the list of source schemas for the given source.
            The list is generated using the mapping in sch_chameleon.t_sources.
            Any change in the configuration file is ignored
            The method assumes there is a database connection active.
        """
        self.logger.debug("Collecting schema list for source %s" % self.source)
        sql_get_schema = """
            SELECT
                (jsonb_each_text(jsb_schema_mappings)).key
            FROM
                sch_chameleon.t_sources
            WHERE
                t_source='%s';

        """
        stmt = self.pgsql_conn.prepare(sql_get_schema% (self.source, ))
        schema_list = [schema[0] for schema in stmt()]
        self.logger.debug("Found origin's replication schemas %s" % ', '.join(schema_list))
        return schema_list

    def __build_create_table_pgsql(self, table_metadata,table_name,  schema, temporary_schema=True):
        """
            The method builds the create table statement with any enumeration or composite type associated to the table
            using the postgresql's metadata.
            The returned value is a dictionary with the optional composite type/enumeration's ddl with the create table without indices or primary keys.
            The method assumes there is a database connection active.

            :param table_metadata: the column dictionary extracted from the source's information_schema or builty by the sql_parser class
            :param table_name: the table name
            :param destination_schema: the schema where the table belongs
            :return: a dictionary with the optional create statements for enumerations and the create table
            :rtype: dictionary
        """
        table_ddl = {}
        ddl_columns = []
        def_columns = ''
        if temporary_schema:
            destination_schema = self.schema_loading[schema]["loading"]
        else:
            destination_schema = schema
        ddl_head = 'CREATE TABLE "%s"."%s" (' % (destination_schema, table_name)
        ddl_tail = ");"
        ddl_enum=[]
        ddl_composite=[]
        for column in table_metadata:
            column_name = column["column_name"]
            if column["column_default"]:
                default_value = column["column_default"]
            else:
                default_value = ''
            if column["not_null"]:
                col_is_null="NOT NULL"
            else:
                col_is_null="NULL"
            column_type = column["type_format"]
            if column_type == "enum":
                enum_type = '"%s"."enum_%s_%s"' % (destination_schema, table_name[0:20], column["column_name"][0:20])
                sql_drop_enum = 'DROP TYPE IF EXISTS %s CASCADE;' % enum_type
                sql_create_enum = 'CREATE TYPE %s AS ENUM (%s);' % ( enum_type,  column["typ_elements"])
                ddl_enum.append(sql_drop_enum)
                ddl_enum.append(sql_create_enum)
                column_type=enum_type
            if column_type == "composite":
                composite_type = '"%s"."typ_%s_%s"' % (destination_schema, table_name[0:20], column["column_name"][0:20])
                sql_drop_composite = 'DROP TYPE IF EXISTS %s CASCADE;' % composite_type
                sql_create_composite = 'CREATE TYPE %s AS (%s);' % ( composite_type,  column["typ_elements"])
                ddl_composite.append(sql_drop_composite)
                ddl_composite.append(sql_create_composite)
                column_type=composite_type
            if column["col_serial"]:
                default_value = ''
                if column_type == ColumnType.O_BINT.value:
                    column_type = 'bigserial'
                else:
                    column_type = 'serial'
                default_value = ''
            ddl_columns.append('"%s" %s %s %s' % (column_name, column_type, default_value, col_is_null))
        def_columns=str(',').join(ddl_columns)
        table_ddl["enum"] = ddl_enum
        table_ddl["composite"] = ddl_composite
        table_ddl["table"] = (ddl_head+def_columns+ddl_tail)
        return table_ddl

    def __check_part_key_datatype(self, table_metadata, part_key, table_name, schema):
        for key in part_key:
            for column in table_metadata:
                if (key.lower() == column["column_name"].lower()):
                    column_type = self.get_data_type(column, schema, table_name)
                    if column_type not in self.hash_part_key_type:
                        self.logger.warning("%s.%s.%s can't be used as a partition key, column type: %s"\
                            % (schema, table_name, key, column_type))
                        return False
        return True

    def __trans_default_value(self, origin_default, column_type):
        if self.migrate_default_value == False or origin_default is None or origin_default == "NULL":
            return ''

        re_symbol = 'E' if column_type in self.character_type else ''
        # for mysql, mysql_version will be -1 which need to be quoted
        # for mariadb, mysql_version will be the real version num, need to be quoted when version < 10.2.7
        # when we need to quote value, we also need to add '\' to escape single quotation mark for string type
        quote = ''
        if self.mysql_version < COLUMNDEFAULT_INCLUDE_QUOTE_VER:
            if column_type in self.character_type:
                quote = '\''
                origin_default = origin_default.replace('\'', '\\\'')
            elif column_type in self.date_type:
                if origin_default.lower() in self.default_value_map:
                    origin_default = self.default_value_map.get(origin_default.lower())
                else:
                    quote = '\''
            else:
                pass
        else:
            origin_default = self.default_value_map.get(origin_default.lower(), origin_default)
        default_str = "DEFAULT %s%s%s%s" % (re_symbol, quote, origin_default, quote)
        return default_str

    def metadata_method_check(self, metadata, check_method):
        method_ck = metadata[0][check_method].upper()
        if method_ck == "RANGE" or\
            method_ck == "RANGE COLUMNS":
            return "RANGE"
        elif method_ck == "LIST" or\
            method_ck == "LIST COLUMNS":
            return "LIST"
        elif method_ck == "HASH" or method_ck == "KEY" or\
            method_ck == "LINEAR KEY" or\
            method_ck == "LINEAR HASH":
            return "HASH"
        else:
            return False

    def __build_create_table_mysql(self, table_metadata, partition_metadata, table_name,  schema, temporary_schema=True):
        """
            The method builds the create table statement with any enumeration associated using the mysql's metadata.
            The returned value is a dictionary with the optional enumeration's ddl and the create table without indices or primary keys.
            on the destination schema specified by destination_schema.
            The method assumes there is a database connection active.

            :param table_metadata: the column dictionary extracted from the source's information_schema or builty by the sql_parser class
            :param table_name: the table name
            :param destination_schema: the schema where the table belongs
            :return: a dictionary with the optional create statements for enumerations and the create table
            :rtype: dictionary
        """
        if temporary_schema:
            destination_schema = self.schema_loading[schema]["loading"]
        else:
            destination_schema = schema

        column_comments = ''
        ddl_head = 'CREATE TABLE "%s"."%s" (' % (destination_schema, table_name)
        ddl_tail = ");"
        ddl_columns = []
        ddl_enum=[]
        table_ddl = {}
        for column in table_metadata:
            if column["is_nullable"] == "NO":
                    col_is_null = "NOT NULL"
            else:
                col_is_null = "NULL"
            column_type = self.get_data_type(column, schema, table_name)
            default_value = self.__trans_default_value(column.get("column_default"), column_type)
            if 'default' in column.keys():
                if default_value == "" and column["default"] is not None and column["default"] != "":
                    default_value = " default %s " % (column["default"])

            if column_type == "enum":
                enum_type = '"%s"."enum_%s_%s"' % (destination_schema, table_name[0:20], column["column_name"][0:20])
                sql_drop_enum = 'DROP TYPE IF EXISTS %s CASCADE;' % enum_type
                sql_create_enum = 'CREATE TYPE %s AS ENUM %s;' % ( enum_type,  column["enum_list"])
                ddl_enum.append(sql_drop_enum)
                ddl_enum.append(sql_create_enum)
                column_type=enum_type
            if column_type == "set":
                column_type = column["column_type"]
            if (column_type == ColumnType.O_C_CHAR_VAR.value or column_type == ColumnType.O_C_CHARACTER.value) and\
                int(column["character_maximum_length"]) > 0:
                column_type="%s (%s)" % (column_type, str(column["character_maximum_length"]))
            if column_type == ColumnType.O_NUM.value and ('numeric_scale' in column.keys()) and str(column["numeric_scale"]) != "None":
                column_type="%s (%s,%s)" % (column_type, str(column["numeric_precision"]), str(column["numeric_scale"]))
            if column_type == ColumnType.O_NUMBER.value and ('numeric_scale' in column.keys()) and str(column["numeric_scale"]) != "None":
                            column_type="%s (%s,%s)" % (column_type, str(column["numeric_precision"]), str(column["numeric_scale"]))
            if column["extra"] == "auto_increment":
                if (column_type == ColumnType.O_INTEGER.value):
                    column_type = ColumnType.O_SERIAL.value
                else:
                    column_type = ColumnType.O_BIGSERIAL.value

            ddl_columns.append(  ' "%s" %s %s %s   ' %  (column["column_name"], column_type, default_value, col_is_null ))

            if "column_comment" in column and column["column_comment"] != "":
                column_comments = column_comments + ( 'comment on column "%s"."%s"."%s" is \'%s\';\n'\
                    % (destination_schema, table_name, column["column_name"], column["column_comment"] ) )

        table_ddl["column_comments"] = column_comments
        def_columns=str(',').join(ddl_columns)
        table_ddl["enum"] = ddl_enum
        table_ddl["composite"] = []

        # non partition table or subpartition table, ignore partition_metadata
        if partition_metadata is None or len(partition_metadata) == 0 or partition_metadata[0]["partition_method"] is None:
            table_ddl["table"] = (ddl_head + def_columns + ddl_tail)
            return table_ddl

        if partition_metadata[0]["subpartition_method"] is not None:
            if partition_metadata[0]["subpartition_method"] == "":
                self.logger.warning("%s.%s is a composite partition table, ignore subpartition" % (schema, table_name))
        #now we get the subpartition part
            else:
                subpartition_method = self.build_sub_partition(schema, table_name, table_metadata, partition_metadata)
                if subpartition_method == "":
                    return ""
                table_ddl["table"] = (ddl_head + def_columns + subpartition_method + ddl_tail)
                return table_ddl

        # get partition key num
        part_key = partition_metadata[0]["partition_expression"].replace('`', '')
        part_key_split = part_key.split(',')

        if len(part_key_split) > self.max_range_column or\
             (partition_metadata[0]["partition_method"] != "RANGE COLUMNS" and len(part_key_split) > 1):
            self.logger.warning("%s.%s's partition key num(%d) exceed max value, create as normal table"\
                % (schema, table_name, len(part_key_split)))
            table_ddl["table"] = (ddl_head+def_columns+ddl_tail)
            return table_ddl

        # special case for KEY partition
        if partition_metadata[0]["partition_method"] == "KEY" or partition_metadata[0]["partition_method"] == "LINEAR KEY":
            if self.__check_part_key_datatype(table_metadata, part_key_split, table_name, schema) == False:
                table_ddl["table"] = (ddl_head+def_columns+ddl_tail)
                return table_ddl

        # ok, let's make the partition clause
        partition_method = ") PARTITION BY "
        method = self.metadata_method_check(partition_metadata,"partition_method")
        if not method:
            self.logger.warning("Unknown partition type: %s, create this table(%s.%s) as non-part table" \
                                % (partition_metadata[0]["partition_method"], schema, table_name))
            table_ddl["table"] = (ddl_head + def_columns + ddl_tail)
            return table_ddl
        else:
            partition_method += method

        partition_method += "(" + part_key + ") ("
        part_column = []
        for part_data in partition_metadata:
            if partition_metadata[0]["partition_method"] == "RANGE" or\
                partition_metadata[0]["partition_method"] == "RANGE COLUMNS":
                part_column.append(' partition %s values less than(%s) ' % (part_data["partition_name"], part_data["partition_description"]))
            elif partition_metadata[0]["partition_method"] == "LIST" or\
                partition_metadata[0]["partition_method"] == "LIST COLUMNS":
                part_column.append(' partition %s values(%s) ' % (part_data["partition_name"], part_data["partition_description"]))
            elif part_data["partition_method"] == "HASH" or part_data["partition_method"] == "KEY" or\
                part_data["partition_method"] == "LINEAR KEY" or part_data["partition_method"] == "LINEAR HASH":
                part_column.append(' partition %s ' %  (part_data["partition_name"]))
            else:
                self.logger.warning("Unknown partition type: %s, create this table(%s) as non-part table"\
                    % (partition_metadata[0]["partition_method"], table_name))
                table_ddl["table"] = (ddl_head+def_columns+ddl_tail)
                return table_ddl

        def_part = str(',').join(part_column)
        table_ddl["table"] = (ddl_head+def_columns+partition_method+def_part+ddl_tail)
        return table_ddl

    def build_sub_partition(self, schema, table_name, sub_table_metadata, sub_partition_metadata):
        """
        This function is used to compile DDL statements that generate child partitions
        Range-Range, Range-List, Range-Hash
        List-Range, List-List, List-Hash
        Hash-Range, Hash-List, Hash-Hash
        """
        part_key = sub_partition_metadata[0]["partition_expression"].replace('`', '')
        part_key_split = part_key.split(',')
        sub_part_key = sub_partition_metadata[0]["subpartition_expression"].replace('`', '')
        if part_key == sub_part_key:
            print("OpenGauss databases don't support that the partition and subpartition have the same partition key for the level-two partition tables, online migration won't migrate this scenario.")
            return ""

        if len(part_key_split) > self.max_range_column or \
                (sub_partition_metadata[0]["partition_method"] != "RANGE COLUMNS" and len(part_key_split) > 1):
            self.logger.warning("%s.%s's partition key num(%d) exceed max value, create as normal table" \
                                % (schema, table_name, len(part_key_split)))
            return ""

        # special case for KEY partition
        # but KEY is not support in subpartition
        if sub_partition_metadata[0]["partition_method"] == "KEY" or sub_partition_metadata[0][
            "partition_method"] == "LINEAR KEY":
                return ""

        # ok, let's make the partition clause
        partition_method = ") PARTITION BY "
        method = self.metadata_method_check(sub_partition_metadata,"partition_method")
        if not method:
            self.logger.warning("Unknown partition type: %s, create this table(%s.%s) as non-part table" \
                                % (sub_partition_metadata[0]["partition_method"], schema, table_name))
            return ""
        else:
            partition_method += method

        # and make the subpartition clause
        partition_method += "(" + part_key + ") SUBPARTITION BY "
        method = self.metadata_method_check(sub_partition_metadata, "subpartition_method")
        if not method:
            self.logger.warning("Unknown partition type: %s, create this table(%s.%s) as non-part table" \
                                % (sub_partition_metadata[0]["subpartition_method"], schema, table_name))
            return ""
        else:
            partition_method += method
        partition_method += "(" + sub_part_key + ") ("

        part_pcolumn = []
        part_scolumn = []
        part_p = ""
        part_s = ""
        for part_data in sub_partition_metadata:
            # partition
            if part_data["subpartition_ordinal_position"] == 1:
                if part_data["partition_method"] == "RANGE" or part_data["partition_method"] == "RANGE COLUMNS":
                    part_p = ' PARTITION %s VALUES LESS THAN (%s) '%(part_data["partition_name"], part_data["partition_description"])
                elif part_data["partition_method"] == "LIST" or part_data["partition_method"] == "LIST COLUMNS":
                    part_p = ' partition %s values(%s) ' % (part_data["partition_name"], part_data["partition_description"])
                elif part_data["partition_method"] == "HASH" or part_data["partition_method"] == "LINEAR HASH":
                    part_p = ' partition %s ' % (part_data["partition_name"])
                else:
                    self.logger.warning("Unknown partition type: %s, create this table(%s) as non-part table" \
                                        % (sub_partition_metadata[0]["partition_method"], table_name))
                    return ""
                if part_data["tablespace_name"] != "" and part_data["tablespace_name"]!= None:
                    part_p += " TABLESPACE %s " % (part_data["tablespace_name"])
                part_pcolumn.append(part_p)
                part_scolumn = []
            # subpartition
            part_s = " SUBPARTITION %s " % (part_data["subpartition_name"])
            if part_data["tablespace_name"] != "" and part_data["tablespace_name"]!= None:
                part_s += " TABLESPACE %s " % (part_data["tablespace_name"])
            part_scolumn.append(part_s)
            def_part_c = str(',').join(part_scolumn)
            part_pcolumn.pop()
            part_pcolumn.append(part_p + " (" + def_part_c + " )")
        def_part = str(',').join(part_pcolumn)
        return partition_method + def_part

    def build_table_comment(self, schema, table, comments):
        querys = ""
        for comment in comments:
            if comment["type"].upper() == "TABLE":
                query = """\n COMMENT ON TABLE %s.%s is '%s' ;""" % (schema, comment["name"], comment["comment"] )
            elif comment["type"].upper() == "COLUMN":
                query = """\n COMMENT ON COLUMN %s.%s.%s is '%s' ;""" % (schema, table, comment["name"], comment["comment"])
            elif comment["type"].upper() == "INDEX":
                query = """\n COMMENT ON INDEX "%s"."%s" is '%s' ;""" % (schema, comment["name"], comment["comment"])
            elif comment["type"].upper() == "CONSTRAINT":
                query = """\n COMMENT ON CONSTRAINT %s ON %s is '%s' ; """ % (comment["name"], table, comment["comment"])
            else:
                query = """ """
            querys += query
        return querys

    def build_create_index(self, schema, table, index_data):
        """
            The method loops over the list index_data and builds a new list with the statements for the indices.

            :param destination_schema: the schema where the table belongs
            :param table_name: the table name
            :param index_data: the index dictionary used to build the create index statements

            :return: a list with the alter and create index for the given table
            :rtype: list
        """
        idx_ddl = {}
        table_primary = []

        for index in index_data:
                table_timestamp = str(int(time.time()))
                type = index['index_n']
                self.logger.debug("Generating the DDL for index %s" % (type))
                index_columns = ['"%s"' % column for column in index["index_columns"]]
                if type =='PRIMARY':
                    pkey_name = "pk_%s_%s_%s " % (table[0:10],table_timestamp,  self.idx_sequence)
                    pkey_def = 'ALTER TABLE "%s"."%s" ADD CONSTRAINT "%s" PRIMARY KEY (%s) ;' % (schema, table, pkey_name, ','.join(index_columns))
                    idx_ddl[pkey_name] = pkey_def
                    table_primary = index["index_columns"]
                elif type =='UNIQUE':
                    index_name = "%s_%s" %(table,index["index_name"])
                    idx_def = 'CREATE UNIQUE INDEX "%s" ON "%s"."%s" (%s);' % (index_name, schema, table, ','.join(index_columns))
                    idx_ddl[index_name] = idx_def
                elif type == 'INDEX':
                    index_name = "%s_%s" %(table,index["index_name"])
                    idx_def='CREATE INDEX "%s" ON "%s"."%s" (%s);' % (index_name, schema, table, ','.join(index_columns))
                    idx_ddl[index_name] = idx_def
                elif type == 'FOREIGN':
                    fkey_name = index["index_name"]
                    tmp = self.find_same_index_or_constraint(schema, table, fkey_name)
                    fkey_name = tmp
                    key_part = index["index_columns"]
                    if index["constraint_name"]:
                        fkey_name = "%s_%s" % (table, index["constraint_name"])
                    if key_part.find("(") == -1:
                        fkey_name = "%s_%s" % (table, fkey_name)
                    else:
                        [fkey_name, key_part] = self.find_key_from_express(schema, table, fkey_name, index["index_columns"])
                    fkey_def = """ALTER TABLE "%s"."%s" ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s %s;""" % (
                    schema, table, fkey_name, key_part, schema, index["references_match"], index["references_on"])
                    idx_ddl[fkey_name] = fkey_def
                elif type == 'CHECK':
                    ckey_name = index["index_name"]
                    tmp = self.find_same_index_or_constraint(schema, table, ckey_name)
                    ckey_name = tmp
                    key_part = index["index_columns"]
                    if index["constraint_name"]:
                        ckey_name = index["constraint_name"]
                    ckey_name = "%s_%s" % (table, ckey_name)
                    ckey_def = """ALTER TABLE "%s"."%s" ADD CONSTRAINT %s CHECK (%s);""" % (schema, table, ckey_name, key_part)
                    idx_ddl[ckey_name] = ckey_def
                self.idx_sequence += 1
        return [table_primary, idx_ddl]

    def get_log_data(self, log_id):
        """
            The method gets the error log entries, if any, from the replica schema.
            :param log_id: the log id for filtering the row by identifier
            :return: a dictionary with the errors logged
            :rtype: dictionary
        """
        self.connect_db()
        if log_id != "*":
            filter_by_logid = ("WHERE log.i_id_log=%s" %  (log_id, ))
        else:
            filter_by_logid = b""
        sql_log = """
            SELECT
                log.i_id_log,
                src.t_source,
                log.i_id_batch,
                log.v_table_name,
                log.v_schema_name,
                log.ts_error,
                log.t_sql,
                log.t_error_message
            FROM
                sch_chameleon.t_error_log log
                LEFT JOIN sch_chameleon.t_sources src
                ON src.i_id_source=log.i_id_source
            %s
        ;

        """ % (filter_by_logid.decode())

        stmt = self.pgsql_conn.prepare(sql_log)
        log_error = stmt()
        self.disconnect_db()
        return log_error

    def get_status(self):
        """
            The method gets the status for all sources configured in the target database.
            :return: a list with the status details
            :rtype: list
        """
        self.connect_db()
        schema_mappings = None
        table_status = None
        replica_counters = None
        if self.source == "*":
            source_filter = ""

        else:
            source_filter = (""" WHERE  src.t_source='%s' """ % (self.source, ))
            self.set_source_id()

            sql_counters = """
                SELECT
                    sum(i_replayed) as total_replayed,
                    sum(i_skipped) as total_skipped,
                    sum(i_ddl) as total_ddl
                FROM
                    sch_chameleon.t_replica_batch
                WHERE
                    i_id_source=%s;

            """
            stmt = self.pgsql_conn.prepare(sql_counters % (self.i_id_source, ))
            replica_counters = stmt.first()


            sql_mappings = """
                SELECT
                    (mappings).key as origin_schema,
                    (mappings).value destination_schema
                FROM

                (
                    SELECT
                        jsonb_each_text(jsb_schema_mappings) as mappings
                    FROM
                        sch_chameleon.t_sources
                    WHERE
                        t_source='%s'

                ) sch
                ;
            """

            sql_tab_status = """
                WITH  tab_replica AS
                (
                    SELECT
                        b_replica_enabled,
                        v_schema_name,
                        v_table_name
                    FROM
                        sch_chameleon.t_replica_tables tab
                        INNER JOIN sch_chameleon.t_sources src
                        ON tab.i_id_source=src.i_id_source
                        WHERE
                            src.t_source='%s'
                )
                SELECT
                    i_order,
                    i_count,
                    t_tables
                FROM
                (

                    SELECT
                        0 i_order,
                        count(*) i_count,
                        array_agg(format('%%I.%%I',v_schema_name,v_table_name)) t_tables
                    FROM
                        tab_replica
                    WHERE
                        NOT b_replica_enabled
                UNION ALL
                    SELECT
                        1 i_order,
                        count(*) i_count,
                        array_agg(format('%%I.%%I',v_schema_name,v_table_name)) t_tables
                    FROM
                        tab_replica
                    WHERE
                        b_replica_enabled
                UNION ALL
                    SELECT
                        2 i_order,
                        count(*) i_count,
                        array_agg(format('%%I.%%I',v_schema_name,v_table_name)) t_tables
                    FROM
                        tab_replica
                ) tab_stat
                ORDER BY
                    i_order
            ;
            """


            stmt = self.pgsql_conn.prepare(sql_mappings % (self.source, ))
            schema_mappings = stmt()
            stmt = self.pgsql_conn.prepare(sql_tab_status % (self.source, ))
            table_status = stmt()




        sql_status = """
            SELECT
                src.i_id_source,
                src.t_source as source_name,
                src.enm_status as  source_status,
                CASE
                    WHEN rec.ts_last_received IS NULL
                    THEN
                        'N/A'::text
                    ELSE
                        (date_trunc('seconds',now())-ts_last_received)::text
                END AS receive_lag,
                coalesce(rec.ts_last_received::text,''),

                CASE
                    WHEN rep.ts_last_replayed IS NULL
                    THEN
                        'N/A'::text
                    ELSE
                        (rec.ts_last_received-rep.ts_last_replayed)::text
                END AS replay_lag,
                coalesce(rep.ts_last_replayed::text,''),
                CASE
                    WHEN src.b_consistent
                    THEN
                        'Yes'
                    ELSE
                        'No'
                END as consistent_status,
                enm_source_type,
                coalesce(date_trunc('seconds',ts_last_maintenance)::text,'N/A') as last_maintenance,
                coalesce(date_trunc('seconds',ts_last_maintenance+nullif('%%s','disabled')::interval)::text,'N/A') AS next_maintenance


            FROM
                sch_chameleon.t_sources src
                LEFT JOIN sch_chameleon.t_last_received rec
                ON	src.i_id_source = rec.i_id_source
                LEFT JOIN sch_chameleon.t_last_replayed rep
                ON	src.i_id_source = rep.i_id_source
            %s
            ;

        """ % (source_filter, )
        stmt = self.pgsql_conn.prepare(sql_status % (self.auto_maintenance, ))
        configuration_status = stmt()



        self.disconnect_db()
        return [configuration_status, schema_mappings, table_status, replica_counters]

    def insert_source_timings(self):
        """
            The method inserts the source timings in the tables t_last_received and t_last_replayed.
            On conflict sets the replay/receive timestamps to null.
            The method assumes there is a database connection active.
        """
        self.set_source_id()
        sql_replay = """
            INSERT INTO sch_chameleon.t_last_replayed
                (
                    i_id_source
                )
            VALUES
                (
                    %s
                )
            ON DUPLICATE KEY UPDATE
                    ts_last_replayed=NULL
            ;
        """
        sql_receive = """
            INSERT INTO sch_chameleon.t_last_received
                (
                    i_id_source
                )
            VALUES
                (
                    %s
                )
            ON DUPLICATE KEY UPDATE
                    ts_last_received=NULL
            ;
        """
        self.pgsql_conn.execute(sql_replay % (self.i_id_source, ))
        self.pgsql_conn.execute(sql_receive % (self.i_id_source, ))

    def  generate_default_statements(self, schema,  table, column, create_column=None):
        """
            The method gets the default value associated with the table and column removing the cast.
            :param schema: The schema name
            :param table: The table name
            :param column: The column name
            :return: the statements for dropping and creating default value on the affected table
            :rtype: dictionary
        """
        if not create_column:
            create_column = column

        regclass = """ '%s.%s' """ %(schema, table)
        sql_def_val = """
            SELECT
                (
                    SELECT
                        split_part(substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128),'::',1)
                    FROM
                        pg_catalog.pg_attrdef d
                    WHERE
                            d.adrelid = a.attrelid
                        AND d.adnum = a.attnum
                        AND a.atthasdef
                ) as default_value
                FROM
                    pg_catalog.pg_attribute a
                WHERE
                        a.attrelid = %s::regclass
                    AND a.attname='%s'
                    AND NOT a.attisdropped
            ;

        """
        stmt = self.pgsql_conn.prepare(sql_def_val % (regclass, column ))
        default_value = stmt.first()
        query_drop_default = ""
        query_add_default = ""
        if default_value:
            query_drop_default = (" ALTER TABLE \"{}\".\"{}\" ALTER COLUMN \"{}\" DROP DEFAULT;").format((schema), (table), (column))
            query_add_default = (" ALTER TABLE  \"{}\".\"{}\" ALTER COLUMN \"{}\" SET DEFAULT %s;" % (default_value)).format((schema), (table), (create_column))

        return {'drop':query_drop_default, 'create':query_add_default}


    def get_data_type(self, column, schema,  table):
        """
            The method determines whether the specified type has to be overridden or not.

            :param column: the column dictionary extracted from the information_schema or built in the sql_parser class
            :param schema: the schema name
            :param table: the table name
            :return: the postgresql converted column type
            :rtype: string
        """
        if self.type_override:
            try:

                table_full = "%s.%s" % (schema, table)
                type_override = self.type_override[column["column_type"]]
                override_to = type_override["override_to"]
                override_tables = type_override["override_tables"]
                if override_tables[0] == '*' or table_full in override_tables:
                    column_type = override_to
                else:
                    column_type = self.type_dictionary[column["data_type"]]
            except KeyError:
                column_type = self.type_dictionary[column["data_type"]]
        else:
            column_type = self.type_dictionary[column["data_type"]]
        return column_type

    def set_application_name(self, action=""):
        """
            The method sets the application name in the replica using the variable self.pg_conn.global_conf.source_name,
            Making simpler to find the replication processes. If the source name is not set then a generic PGCHAMELEON name is used.
        """
        if self.source:
            app_name = "[pg_chameleon] - source: %s, action: %s" % (self.source, action)
        else:
            app_name = "[pg_chameleon] -  action: %s" % (action)
        self.pgsql_conn.settings['application_name'] = app_name

    def write_batch(self, group_insert):
        """
            Main method for adding the batch data in the log tables.
            The row data from group_insert are mogrified in CSV format and stored in
            the string like object csv_file.

            psycopg2's copy expert is used to store the event data in PostgreSQL.

            Should any error occur the procedure fallsback to insert_batch.

            :param group_insert: the event data built in mysql_engine
        """
        csv_file=io.BytesIO()
        self.set_application_name("writing batch")
        insert_list=[]
        for row_data in group_insert:
            global_data=row_data["global_data"]
            event_after=row_data["event_after"]
            event_before=row_data["event_before"]
            log_table=global_data["log_table"]
            insert_list.append(("%s,'%s','%s','%s','%s',%s,'%s','%s',%s" %  (
                        global_data["batch_id"],
                        global_data["table"],
                        global_data["schema"],
                        global_data["action"],
                        global_data["binlog"],
                        global_data["logpos"],
                        json.dumps(event_after, cls=pg_encoder, ensure_ascii=False).replace("'", "''"),
                        json.dumps(event_before, cls=pg_encoder, ensure_ascii=False).replace("'", "''"),
                        global_data["event_time"],
                    )
                )
            )

        csv_data="\n".join(insert_list )
        csv_file.write(csv_data.encode())
        csv_file.seek(0)

        try:
            sql_copy=("""
                COPY "sch_chameleon".\"{}\"
                    (
                        i_id_batch,
                        v_table_name,
                        v_schema_name,
                        enm_binlog_event,
                        t_binlog_name,
                        i_binlog_position,
                        jsb_event_after,
                        jsb_event_before,
                        i_my_event_time
                    )
                FROM
                    STDIN
                    WITH NULL 'NULL'
                    CSV QUOTE ''''
                    DELIMITER ','
                    ESCAPE ''''
                ;
            """).format((log_table))
            stmt = self.pgsql_conn.prepare(sql_copy)
            stmt.load_rows(csv_file)
        except Exception as e:
            self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.code, e.message))
            self.logger.error("fallback to inserts")
            self.insert_batch(group_insert)
        self.set_application_name("idle")


    def insert_batch(self,group_insert):
        """
            Fallback method for the batch insert. Each row event is processed
            individually and any problematic row is discarded into the table t_discarded_rows.
            The row is encoded in base64 in order to prevent any encoding or type issue.

            :param group_insert: the event data built in mysql_engine
        """

        self.logger.debug("starting insert loop")
        for row_data in group_insert:
            global_data = row_data["global_data"]
            event_after= row_data["event_after"]
            event_before= row_data["event_before"]
            log_table = global_data["log_table"]
            event_time = global_data["event_time"]
            sql_insert=("""
                INSERT INTO sch_chameleon.\"{}\"
                    (
                        i_id_batch,
                        v_table_name,
                        v_schema_name,
                        enm_binlog_event,
                        t_binlog_name,
                        i_binlog_position,
                        jsb_event_after,
                        jsb_event_before,
                        i_my_event_time
                    )
                    VALUES
                        (
                            $1,
                            $2,
                            $3,
                            $4,
                            $5,
                            $6,
                            $7,
                            $8,
                            $9
                        )
                ;
            """).format((log_table))
            try:
                stmt = self.pgsql_conn.prepare(sql_insert)
                stmt(
                        global_data["batch_id"],
                        global_data["table"],
                        global_data["schema"],
                        global_data["action"],
                        global_data["binlog"],
                        global_data["logpos"],
                        json.dumps(event_after, cls=pg_encoder),
                        json.dumps(event_before, cls=pg_encoder),
                        event_time
                    )
            except Exception as e:
                if e.code == "22P05":
                    self.logger.warning("%s - %s. Trying to cleanup the row" % (e.code, e.message))
                    for key, value in event_after.items():
                        if value:
                            event_after[key] = str(value).replace("\x00", "")

                    for key, value in event_before.items():
                        if value:
                            event_before[key] = str(value).replace("\x00", "")

                    #event_after = {key: str(value).replace("\x00", "") for key, value in event_after.items() if value}
                    #event_before = {key: str(value).replace("\x00", "") for key, value in event_before.items() if value}
                    try:
                        stmt = self.pgsql_conn.prepare(sql_insert)
                        stmt(
                                global_data["batch_id"],
                                global_data["table"],
                                global_data["schema"],
                                global_data["action"],
                                global_data["binlog"],
                                global_data["logpos"],
                                json.dumps(event_after, cls=pg_encoder),
                                json.dumps(event_before, cls=pg_encoder),
                                event_time
                            )
                    except:
                        self.logger.error("Cleanup unsuccessful. Saving the discarded row")
                        self.save_discarded_row(row_data)
                else:
                    self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.code, e.message))
                    self.logger.error("Error when storing event data. Saving the discarded row")
                    self.save_discarded_row(row_data)
            except:
                self.logger.error("Error when storing event data. Saving the discarded row")
                self.save_discarded_row(row_data)

    def save_discarded_row(self,row_data):
        """
            The method saves the discarded row in the table t_discarded_row along with the id_batch.
            The row is encoded in base64 as the t_row_data is a text field.

            :param row_data: the row data dictionary

        """
        global_data = row_data["global_data"]
        schema = global_data["schema"]
        table  = global_data["table"]
        batch_id = global_data["batch_id"]
        str_data = '%s' %(row_data, )
        hex_row = binascii.hexlify(str_data.encode())
        hex_row = '%s' %(hex_row, )
        sql_save="""
            INSERT INTO sch_chameleon.t_discarded_rows
                (
                    i_id_batch,
                    v_schema_name,
                    v_table_name,
                    t_row_data
                )
            VALUES
                (
                    $1,
                    $2,
                    $3,
                    $4
                );
        """
        stmt = self.pgsql_conn.prepare(sql_save)
        stmt(batch_id, schema, table,hex_row)


    def create_table(self,  table_metadata, partition_metadata, table_name,  schema, metadata_type):
        """
            Executes the create table returned by __build_create_table (mysql or pgsql) on the destination_schema.

            :param table_metadata: the column dictionary extracted from the source's information_schema or builty by the sql_parser class
            :param table_name: the table name
            :param destination_schema: the schema where the table belongs
            :param metadata_type: the metadata type, currently supported mysql and pgsql
        """
        if metadata_type == 'mysql':
            table_ddl = self.__build_create_table_mysql( table_metadata, partition_metadata, table_name,  schema)
        elif metadata_type == 'pgsql':
            table_ddl = self.__build_create_table_pgsql( table_metadata,table_name,  schema)
        enum_ddl = table_ddl["enum"]
        composite_ddl = table_ddl["composite"]
        column_comments_ddl = ( table_ddl["column_comments"] if "column_comments" in table_ddl else '' )
        table_ddl = table_ddl["table"]

        for enum_statement in enum_ddl:
            self.pgsql_conn.execute(enum_statement)

        for composite_statement in composite_ddl:
            self.pgsql_conn.execute(composite_statement)

        self.pgsql_conn.execute(table_ddl)

        if column_comments_ddl != '':
            self.pgsql_conn.execute(column_comments_ddl)

    def update_schema_mappings(self):
        """
            The method updates the schema mappings for the given source.
            Before executing the updates the method checks for the need to run an update and for any
            mapping already present in the replica catalogue.
            If everything is fine the database connection is set autocommit=false.
            The method updates the schemas  in the table t_replica_tables and then updates the mappings in the
            table t_sources. After the final update the commit is issued to make the updates permanent.

            :todo: The method should run only at replica stopped for the given source. The method should also  replay all the logged rows for the given source before updating the schema mappings to avoid  to get an inconsistent replica.
        """
        self.connect_db()
        self.set_source_id()
        self.replay_replica()
        new_schema_mappings = self.sources[self.source]["schema_mappings"]
        old_schema_mappings = self.get_schema_mappings()


        if new_schema_mappings != old_schema_mappings:
            duplicate_mappings = self.check_schema_mappings(True)
            if not duplicate_mappings:
                self.logger.debug("Updating schema mappings for source %s" % self.source)
                x = self.pgsql_conn.xact()
                x.start()
                for schema in old_schema_mappings:
                    old_mapping = old_schema_mappings[schema]
                    try:
                        new_mapping = new_schema_mappings[schema]
                    except KeyError:
                        new_mapping = None
                    if not new_mapping:
                        self.logger.debug("The mapping for schema %s has ben removed. Deleting the reference from the replica catalogue." % (schema))
                        sql_delete = """
                            DELETE FROM sch_chameleon.t_replica_tables
                            WHERE
                                    i_id_source=%s
                                AND	v_schema_name='%s'
                            ;
                        """
                        self.pgsql_conn.execute(sql_delete % (self.i_id_source,old_mapping ))
                    elif old_mapping != new_mapping:
                        self.logger.debug("Updating mapping for schema %s. Old: %s. New: %s" % (schema, old_mapping, new_mapping))
                        sql_tables = """
                            UPDATE sch_chameleon.t_replica_tables
                                SET v_schema_name='%s'
                            WHERE
                                    i_id_source=%s
                                AND	v_schema_name='%s'
                            ;
                        """
                        self.pgsql_conn.execute(sql_tables % (new_mapping, self.i_id_source,old_mapping ))
                        sql_alter_schema = ("ALTER SCHEMA \"{}\" RENAME TO \"{}\";").format((old_mapping), (new_mapping))
                        self.pgsql_conn.execute(sql_alter_schema)
                sql_source="""
                    UPDATE sch_chameleon.t_sources
                        SET
                            jsb_schema_mappings='%s'
                    WHERE
                        i_id_source=%s
                    ;

                """
                self.pgsql_conn.execute(sql_source % (json.dumps(new_schema_mappings), self.i_id_source))
                x.commit()
            else:
                self.logger.error("Could update the schema mappings for source %s. There is a duplicate destination schema in other sources. The offending schema is %s." % (self.source, duplicate_mappings[1]))
        else:
            self.logger.debug("The configuration file and catalogue mappings for source %s are the same. Not updating." % self.source)
        #print (self.i_id_source)

        self.disconnect_db()

    def get_schema_mappings(self):
        """
            The method gets the schema mappings for the given source.
            The list is the one stored in the table sch_chameleon.t_sources.
            Any change in the configuration file is ignored
            The method assumes there is a database connection active.
            :return: the schema mappings extracted from the replica catalogue
            :rtype: dictionary

        """
        self.logger.debug("Collecting schema mappings for source %s" % self.source)
        sql_get_schema = """
            SELECT
                jsb_schema_mappings
            FROM
                sch_chameleon.t_sources
            WHERE
                t_source='%s';

        """
        stmt = self.pgsql_conn.prepare(sql_get_schema % (self.source, ))
        schema_mappings = stmt.first()
        return json.loads(schema_mappings)

    def set_source_status(self, source_status):
        """
            The method updates the source status for the source_name and sets the class attribute i_id_source.
            The method assumes there is a database connection active.

            :param source_status: The source status to be set.

        """
        sql_source = """
            UPDATE sch_chameleon.t_sources
            SET
                enm_status='%s'
            WHERE
                t_source='%s'
            RETURNING i_id_source
                ;
            """
        stmt = self.pgsql_conn.prepare(sql_source % (source_status, self.source, ))
        source_data = stmt.first()


        try:
            self.i_id_source = source_data
        except:
            print("Source %s is not registered." % self.source)
            sys.exit()

    def set_source_id(self):
        """
            The method sets the class attribute i_id_source for the self.source.
            The method assumes there is a database connection active.
        """
        sql_source = """
            SELECT i_id_source FROM
                sch_chameleon.t_sources
            WHERE
                t_source='%s'
            ;
            """
        stmt = self.pgsql_conn.prepare(sql_source % ( self.source, ))
        source_data = stmt.first()
        try:
            self.i_id_source = source_data
        except:
            print("Source %s is not registered." % self.source)
            sys.exit()


    def clean_batch_data(self):
        """
            This method removes all the batch data for the source id stored in the class varible self.i_id_source.

            The method assumes there is a database connection active.
        """
        sql_cleanup = """
            DELETE FROM sch_chameleon.t_replica_batch WHERE i_id_source=%s;
        """
        self.pgsql_conn.execute(sql_cleanup % (self.i_id_source, ))


    def get_replica_status(self):
        """
            The method gets the replica status for the given source.
            The method assumes there is a database connection active.
        """
        self.set_source_id()
        sql_status = """
            SELECT
                enm_status
            FROM
                sch_chameleon.t_sources
            WHERE
                i_id_source=%s
            ;
        """
        stmt = self.pgsql_conn.prepare(sql_status % (self.i_id_source, ))
        replica_status = stmt.first()
        return replica_status

    def clean_not_processed_batches(self):
        """
            The method cleans up the not processed batches rows from the table sch_chameleon.t_log_replica.
            The method should be executed only before starting a replica process.
            The method assumes there is a database connection active.
        """
        self.set_source_id()

        sql_log_tables = """
            SELECT
                unnest(v_log_table)
            FROM
                sch_chameleon.t_sources
            WHERE
                i_id_source=%s
            ;
        """
        stmt = self.pgsql_conn.prepare(sql_log_tables % (self.i_id_source, ))
        log_tables = stmt()
        for log_table in log_tables:

            sql_cleanup = ("""
                DELETE FROM sch_chameleon.\"{}\"
                WHERE
                    i_id_batch IN (
                        SELECT
                            i_id_batch
                        FROM
                            sch_chameleon.t_replica_batch
                        WHERE
                                i_id_source=%s
                            AND	NOT b_processed
                        )
                ;
            """).format((log_table[0]))
            self.logger.debug("Cleaning table %s" % log_table[0])
            self.pgsql_conn.execute(sql_cleanup % (self.i_id_source, ))


    def check_auto_maintenance(self):
        """
            This method checks if the the maintenance for the given source is required.
            The SQL compares the last maintenance stored in the replica catalogue with the NOW() function.
            If the value is bigger than the configuration parameter auto_maintenance then it returns true.
            Otherwise returns false.

            :return: flag which tells if the maintenance should run or not
            :rtype: boolean

        """
        self.set_source_id()
        sql_maintenance = """
            SELECT
                now()-coalesce(ts_last_maintenance,'1970-01-01 00:00:00'::timestamp)>'%s'::interval
            FROM
                sch_chameleon.t_sources
            WHERE
                i_id_source=%s;
        """
        stmt = self.pgsql_conn.prepare(sql_maintenance % (self.auto_maintenance, self.i_id_source, ))
        maintenance = stmt.first()
        return maintenance

    def check_source_consistent(self):
        """
            This method checks if the database is consistent using the source's high watermark and the
            source's flab b_consistent.
            If the batch data is larger than the source's high watermark then the source is marked consistent and
            all the log data stored witth the source's tables are set to null in order to ensure all the tables are replicated.
        """

        sql_check_consistent = """
            WITH hwm AS
                (
                    SELECT
                        split_part(t_binlog_name,'.',2)::integer as i_binlog_sequence,
                        i_binlog_position
                    FROM
                        sch_chameleon.t_sources
                    WHERE
                            i_id_source=%s
                        AND	not b_consistent

                )
            SELECT
                CASE
                    WHEN	bat.binlog_data[1]>hwm.i_binlog_sequence
                    THEN
                        True
                    WHEN		bat.binlog_data[1]=hwm.i_binlog_sequence
                        AND	bat.binlog_data[2]>=hwm.i_binlog_position
                    THEN
                        True
                    ELSE
                        False
                END AS b_consistent
            FROM
                (
                    SELECT
                        max(
                            array[
                                split_part(t_binlog_name,'.',2)::integer,
                                i_binlog_position
                            ]
                        ) as binlog_data
                    FROM
                        sch_chameleon.t_replica_batch
                    WHERE
                            i_id_source=%s
                        AND	b_started
                        AND	b_processed

                ) bat,
                hwm
            ;

        """
        stmt = self.pgsql_conn.prepare(sql_check_consistent % (self.i_id_source, self.i_id_source, ))
        self.logger.debug("Checking consistent status for source: %s" %(self.source, ) )
        source_consistent = stmt.first()
        if source_consistent:
            self.logger.info("The source: %s reached the consistent status" % (self.source, ) )
            sql_set_source_consistent = """
                UPDATE sch_chameleon.t_sources
                    SET
                        b_consistent=True,
                        t_binlog_name=NULL,
                        i_binlog_position=NULL
                WHERE
                    i_id_source=%s
            ;
            """
            sql_set_tables_consistent = """
                UPDATE sch_chameleon.t_replica_tables
                    SET
                        t_binlog_name=NULL,
                        i_binlog_position=NULL
                WHERE
                    i_id_source=%s
            ;
            """
            self.pgsql_conn.execute(sql_set_source_consistent % (self.i_id_source,  ))
            self.pgsql_conn.execute(sql_set_tables_consistent % (self.i_id_source,  ))
            if self.keep_existing_schema:
                self.__create_foreign_keys()
                self.__validate_fkeys()
                self.__cleanup_idx_keys()
        else:
            self.logger.debug("The source: %s is not consistent " %(self.source, ) )

    def __cleanup_idx_keys(self):
        """
            The method removes the index and keys definitions collected for the source
        """
        sql_clean_idx = """
            DELETE FROM sch_chameleon.t_indexes 
            WHERE 
                (v_schema_name,v_table_name) 
            IN 
                (
                    SELECT 
                        v_schema_name,
                        v_table_name 
                    FROM 
                        sch_chameleon.t_replica_tables 
                    WHERE i_id_source =%s
                )
            ;
        """
        sql_clean_pkeys = """
            DELETE FROM sch_chameleon.t_pkeys 
            WHERE 
                (v_schema_name,v_table_name) 
            IN 
                (
                    SELECT 
                        v_schema_name,
                        v_table_name 
                    FROM 
                        sch_chameleon.t_replica_tables 
                    WHERE i_id_source =%s
                )
            ;
        """
        sql_clean_fkeys = """
            DELETE FROM sch_chameleon.t_fkeys 
            WHERE 
                (v_schema_name,v_table_name) 
            IN 
                (
                    SELECT 
                        v_schema_name,
                        v_table_name 
                    FROM 
                        sch_chameleon.t_replica_tables 
                    WHERE i_id_source =%s
                )
            ;
        """
        self.pgsql_conn.execute(sql_clean_idx % (self.i_id_source, ))
        self.pgsql_conn.execute(sql_clean_pkeys % (self.i_id_source, ))
        self.pgsql_conn.execute(sql_clean_fkeys % (self.i_id_source, ))

    def set_source_highwatermark(self, master_status, consistent):
        """
            This method saves the master data within the source.
            The values are used to determine whether the database has reached the consistent point.

            :param master_status: the master data with the binlogfile and the log position
        """
        master_data = master_status[0]
        binlog_name = master_data["File"]
        binlog_position = master_data["Position"]
        sql_set  = """
            UPDATE sch_chameleon.t_sources
                SET
                    b_consistent=%s,
                    t_binlog_name='%s',
                    i_binlog_position=%s
            WHERE
                i_id_source=%s
            ;

        """
        self.pgsql_conn.execute(sql_set % (consistent, binlog_name, binlog_position, self.i_id_source, ))
        self.logger.info("Set high watermark for source: %s" %(self.source, ) )


    def save_master_status(self, master_status):
        """
            This method saves the master data determining which log table should be used in the next batch.
            The method assumes there is a database connection active.

            :param master_status: the master data with the binlogfile and the log position
            :return: the batch id or none if no batch has been created
            :rtype: integer
        """
        next_batch_id = None
        master_data = master_status[0]
        binlog_name = master_data["File"]
        binlog_position = master_data["Position"]
        log_table = self.swap_source_log_table()
        if "Executed_Gtid_Set" in master_data:
            executed_gtid_set = master_data["Executed_Gtid_Set"]
        else:
            executed_gtid_set = None
        try:
            event_time = master_data["Time"]
        except:
            event_time = None

        sql_master = """
            INSERT INTO sch_chameleon.t_replica_batch
                (
                    i_id_source,
                    t_binlog_name,
                    i_binlog_position,
                    t_gtid_set,
                    v_log_table
                )
            VALUES
                (
                    %s,
                    '%s',
                    %s,
                    '%s',
                    '%s'
                )
            RETURNING i_id_batch
            ;
        """

        sql_last_update = """
            UPDATE
                sch_chameleon.t_last_received
            SET
                ts_last_received=to_timestamp($1::float)
            WHERE
                i_id_source=$2
            RETURNING ts_last_received
        ;
        """

        try:
            stmt = self.pgsql_conn.prepare(sql_master % (self.i_id_source, binlog_name, binlog_position, executed_gtid_set, log_table))
            next_batch_id=stmt.first()
            stmt = self.pgsql_conn.prepare(sql_last_update)
            db_event_time = stmt.first(event_time, self.i_id_source)
            self.logger.info("Saved master data for source: %s" %(self.source, ) )
            self.logger.debug("Binlog file: %s" % (binlog_name, ))
            self.logger.debug("Binlog position:%s" % (binlog_position, ))
            self.logger.debug("Last event: %s" % (db_event_time, ))
            self.logger.debug("Next log table name: %s" % ( log_table, ))
            self.logger.debug("Next batch id: %s" % ( next_batch_id, ))

        except Exception as e:
            self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.code, e.message))
            self.logger.error(sql_master % (self.i_id_source, binlog_name, binlog_position, executed_gtid_set, log_table))

        return next_batch_id

    def reindex_table(self, schema, table):
        """
            The method run a REINDEX TABLE on the table defined by schema and name.
            :param schema: the table's schema
            :param table: the table's name
        """
        sql_reindex = ("REINDEX TABLE \"{}\".\"{}\" ;").format((schema), (table))
        self.pgsql_conn.execute(sql_reindex)

    def cleanup_idx_cons(self,schema,table):
        """
            The method cleansup the constraint and indices for the given table using the statements collected in
            collect_idx_cons.
            :param schema: the table's schema
            :param table: the table's name
        """
        sql_get_fk_drop = """
            SELECT
                v_constraint_name,
                t_fkey_drop
            FROM
                sch_chameleon.t_fkeys
            WHERE
                    v_schema_name='%s'
                AND v_table_name='%s'
            ;
            """
        sql_get_idx_drop = """
            SELECT
                v_index_name,
                t_index_drop
            FROM
                sch_chameleon.t_indexes
            WHERE
                    v_schema_name='%s'
                AND v_table_name='%s'
            ;
            """
        sql_get_pk_drop = """
            SELECT
                v_index_name,
                t_pkey_drop
            FROM
                sch_chameleon.t_pkeys
            WHERE
                    v_schema_name='%s'
                AND v_table_name='%s'
            ;
            """
        stmt = self.pgsql_conn.prepare(sql_get_fk_drop%(schema,table,))
        fk_drop=stmt()
        stmt = self.pgsql_conn.prepare(sql_get_idx_drop%(schema,table,))
        idx_drop=stmt()
        stmt = self.pgsql_conn.prepare(sql_get_pk_drop%(schema,table,))
        pk_drop=stmt()

        for fk in fk_drop:
            self.logger.info("Dropping the foreign key {}".format(fk[0],))
            try:
                self.pgsql_conn.execute(fk[1])
            except:
                pass
        for idx in idx_drop:
            self.logger.info("Dropping the index {}".format(idx[0],))
            try:
                self.pgsql_conn.execute(idx[1])
            except:
                pass
        for pk in pk_drop:
            self.logger.info("Dropping the primary key {}".format(pk[0],))
            try:
                self.pgsql_conn.execute(pk[1])
            except:
                pass
    def __create_foreign_keys(self):
        """
            The method creates the foreign keys previously dropped using the data stored in sch_chameleon.t_fkeys.
            In order to reduce the blockage the foreign keys are created invalid and validated in a second step.
        """
        sql_get_fk_create = """
            SELECT
                v_constraint_name,
                t_fkey_create,
                t_fkey_validate
            FROM
                sch_chameleon.t_fkeys
            ;
            """
        stmt = self.pgsql_conn.prepare(sql_get_fk_create)
        fk_create=stmt()
        for fk in fk_create:
            self.logger.info("Creating the foreign key {}".format(fk[0],))
            try:
                self.pgsql_conn.execute(fk[1])
            except:
                pass


    def create_idx_cons(self,schema,table):
        """
            The method creates the constraint and indices for the given table using the statements collected in
            collect_idx_cons. The foreign keys are not created at this stage as they may be left inconsistent
            during the initial replay phase.
            The foreign key creation is managed by __create_foreign_keys() which is executed when the replica reaches the
            consistent status.
            :param schema: the table's schema
            :param table: the table's name
        """

        sql_get_idx_create = """
            SELECT
                v_index_name,
                t_index_create
            FROM
                sch_chameleon.t_indexes
            WHERE
                    v_schema_name='%s'
                AND v_table_name='%s'
            ;
            """
        sql_get_pk_create = """
            SELECT
                v_index_name,
                t_pkey_create
            FROM
                sch_chameleon.t_pkeys
            WHERE
                     v_schema_name='%s'
                AND v_table_name='%s'
           ;
            """
        stmt = self.pgsql_conn.prepare(sql_get_idx_create%(schema,table,))
        idx_create=stmt()
        stmt = self.pgsql_conn.prepare(sql_get_pk_create%(schema,table,))
        pk_create=stmt()

        for pk in pk_create:
            self.logger.info("Creating the primary key {}".format(pk[0],))
            self.pgsql_conn.execute(pk[1])

        for idx in idx_create:
            self.logger.info("Creating the index {}".format(idx[0],))
            self.pgsql_conn.execute(idx[1])



    def collect_idx_cons(self,schema,table):
        """
            The method collects indices and primary keys for the given table from the views v_idx_pkeys,v_fkeys.
            :param schema: the table's schema
            :param table: the table's name
        """
        sql_index = """
            INSERT INTO sch_chameleon.t_indexes
            (
                    v_schema_name,
                    v_table_name,
                    v_index_name,
                    t_index_drop,
                    t_index_create
            )
            SELECT
                vip.v_schema_name,
                vip.v_table_name,
                vip.v_index_name,
                vip.t_sql_drop,
                vip.t_sql_create
            FROM
                sch_chameleon.v_idx_pkeys vip

            WHERE
                vip.v_schema_name ='%s'
                AND vip.v_table_name ='%s'
            AND NOT vip.b_idx_pkey
            ON DUPLICATE KEY UPDATE t_index_drop=EXCLUDED.t_index_drop,t_index_create=EXCLUDED.t_index_create
            ;
        """
        sql_pkey = """
            INSERT INTO sch_chameleon.t_pkeys
            (
                    v_schema_name,
                    v_table_name,
                    v_index_name,
                    t_pkey_drop,
                    t_pkey_create
            )
            SELECT
                vip.v_schema_name,
                vip.v_table_name,
                vip.v_index_name,
                vip.t_sql_drop,
                vip.t_sql_create
            FROM
                sch_chameleon.v_idx_pkeys vip
            WHERE
                vip.v_schema_name ='%s'
                AND vip.v_table_name ='%s'
            AND vip.b_idx_pkey
            ON DUPLICATE KEY UPDATE v_index_name = EXCLUDED.v_index_name,t_pkey_drop=EXCLUDED.t_pkey_drop,t_pkey_create=EXCLUDED.t_pkey_create;

        """

        sql_fkeys = """
            INSERT INTO sch_chameleon.t_fkeys
            (
                v_schema_name,
                v_table_name,
                v_constraint_name,
                t_fkey_drop,
                t_fkey_create,
                t_fkey_validate
            )
            SELECT
                '%s',
                '%s',
                v_fk_name,
                t_con_drop,
                t_con_create,
                t_con_validate

            FROM
                sch_chameleon.v_fkeys vf
            WHERE
                (		v_schema_referencing ='%s'
                    AND	v_table_referencing='%s'
                )
                OR (
                        v_schema_referenced ='%s'
                    AND v_table_referenced ='%s'
                    )
            ON DUPLICATE KEY UPDATE t_fkey_drop=EXCLUDED.t_fkey_drop,t_fkey_create=EXCLUDED.t_fkey_create,t_fkey_validate=EXCLUDED.t_fkey_validate;
            ;
        """
        self.pgsql_conn.execute(sql_index%(schema,table,))
        self.pgsql_conn.execute(sql_pkey%(schema,table,))
        self.pgsql_conn.execute(sql_fkeys%(schema,table,schema,table,schema,table,))

    def __validate_fkeys(self):
        """
            The method tries to validate all the invalid foreign keys in the database
        """
        sql_get_validate = """
            SELECT
                format('ALTER TABLE %I.%I VALIDATE CONSTRAINT %I;',sch.nspname,tab.relname,con.conname) AS t_con_validate,
                sch.nspname as v_schema_name,
                con.conname AS v_con_name,
                tab.relname AS v_table_name


            FROM
                pg_class tab
                INNER JOIN pg_namespace sch
                    ON sch.oid=tab.relnamespace
                INNER JOIN pg_constraint con
                    ON
                        con.connamespace=tab.relnamespace
                    AND	con.conrelid=tab.oid
            WHERE
                        con.contype in ('f')
                    AND NOT con.convalidated

            ;
        """
        stmt = self.pgsql_conn.prepare(sql_get_validate)
        fk_validate=stmt()
        for fk in fk_validate:
            self.pgsql_conn.execute(fk[0])

    def truncate_table(self, schema, table):
        """
            The method truncates the table defined by schema and name.
            :param schema: the table's schema
            :param table: the table's name
        """
        sql_truncate = ("TRUNCATE TABLE \"{}\".\"{}\";").format((schema), (table))
        self.pgsql_conn.execute(sql_truncate)

    def store_table(self, schema, table, table_pkey, master_status):
        """
            The method saves the table name along with the primary key definition in the table t_replica_tables.
            This is required in order to let the replay procedure which primary key to use replaying the update and delete.
            If the table is without primary key is not stored.
            A table without primary key is copied and the indices are create like any other table.
            However the replica doesn't work for the tables without primary key.

            If the class variable master status is set then the master's coordinates are saved along with the table.
            This happens in general when a table is added to the replica or the data is refreshed with sync_tables.

            :param schema: the schema name to store in the table  t_replica_tables
            :param table: the table name to store in the table  t_replica_tables
            :param table_pkey: a list with the primary key's columns. empty if there's no pkey
            :param master_status: the master status data .
        """
        if master_status:
            master_data = master_status[0]
            binlog_file = master_data["File"]
            binlog_pos = master_data["Position"]
        else:
            binlog_file = None
            binlog_pos = None

        sql_insert = """
            INSERT INTO sch_chameleon.t_replica_tables
                (
                    i_id_source,
                    v_table_name,
                    v_schema_name,
                    v_table_pkey,
                    t_binlog_name,
                    i_binlog_position
                )
            VALUES
                (
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    $6
                )
            ON DUPLICATE KEY UPDATE
                        v_table_pkey=EXCLUDED.v_table_pkey,
                        t_binlog_name = EXCLUDED.t_binlog_name,
                        i_binlog_position = EXCLUDED.i_binlog_position,
                        b_replica_enabled = True
            ;
        """
        stmt = self.pgsql_conn.prepare(sql_insert)
        stmt(
            self.i_id_source,
            table,
            schema,
            table_pkey,
            binlog_file,
            binlog_pos
            )


    def copy_data(self, csv_file, schema, table, column_list):
        """
            The method copy the data into postgresql using psycopg2's copy_expert.
            The csv_file is a file like object which can be either a  csv file or a string io object, accordingly with the
            configuration parameter copy_mode.
            The method assumes there is a database connection active.

            :param csv_file: file like object with the table's data stored in CSV format
            :param schema: the schema used in the COPY FROM command
            :param table: the table name used in the COPY FROM command
            :param column_list: A string with the list of columns to use in the COPY FROM command already quoted and comma separated
        """
        sql_copy='COPY "%s"."%s" (%s) FROM STDIN WITH NULL \'NULL\' CSV QUOTE \'"\' DELIMITER \',\' ESCAPE \'"\' ; ' % (schema, table, column_list)
        receive_stmt = self.pgsql_conn.prepare(sql_copy)
        receive_stmt.load_rows(csv_file)

    def insert_data(self, schema, table, insert_data , column_list):
        """
            The method is a fallback procedure for when the copy method fails.
            The procedure performs a row by row insert, very slow but capable to skip the rows with problematic data (e.g. encoding issues).

            :param schema: the schema name where table belongs
            :param table: the table name where the data should be inserted
            :param insert_data: a list of records extracted from the database using the unbuffered cursor
            :param column_list: the list of column names quoted  for the inserts
        """
        sample_row = insert_data[0]
        column_marker=','.join(['%s' for column in sample_row])

        sql_head='INSERT INTO "%s"."%s"(%s) VALUES (%s);' % (schema, table, column_list, column_marker)
        for data_row in insert_data:
            try:
                data_row = list(data_row)
                for key in range(len(data_row)):
                    if(data_row[key] != None):
                        data_row[key] = "'" + data_row[key].replace("'","''") + "'"
                    else:
                        data_row[key] = 'null'
                data_row = tuple(data_row)
                self.pgsql_conn.execute(sql_head % data_row)
            except Exception as e:
                    self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.code, e.message))
                    self.logger.error(sql_head % data_row)
            except ValueError:
                self.logger.warning("character mismatch when inserting the data, trying to cleanup the row data")
                cleanup_data_row = []
                for item in data_row:
                    if item:
                        cleanup_data_row.append(str(item).replace("\x00", ""))
                    else:
                        cleanup_data_row.append(item)
                data_row = cleanup_data_row
                try:
                    self.pgsql_conn.execute(sql_head%data_row)
                except:
                    self.logger.error("error when inserting the row, skipping the row")


            except:
                self.logger.error("unexpected error when processing the row")
                self.logger.error(" - > Table: %s.%s" % (schema, table))

    def get_existing_pkey(self,schema,table):
        """
            The method gets the primary key of an existing table and returns the field(s)
            composing the PKEY as a list.
            :param schema: the schema name where table belongs
            :param table: the table name where the data should be inserted
            :return: a list with the eventual column(s) used as primary key
            :rtype: list
        """
        sql_get_pkey = """
            SELECT
                array_agg(att.attname)
            FROM
            (
                SELECT
                    tab.oid AS taboid,
                    tab.relname AS table_name,
                    sch.nspname AS schema_name,
                    UNNEST(con.conkey) AS conkey
                FROM
                    pg_class tab
                    INNER JOIN pg_constraint con
                        ON tab.oid=con.conrelid
                    INNER JOIN pg_catalog.pg_namespace sch
                        ON tab.relnamespace = sch.oid
                WHERE
                    con.contype='p'
                    AND sch.nspname=$1
                    AND tab.relname=$2
            ) con
            INNER JOIN pg_catalog.pg_attribute att
            ON
                    con.taboid=att.attrelid
                AND con.conkey=att.attnum
            ;
        """
        stmt = self.pgsql_conn.prepare(sql_get_pkey)
        pkey_col = stmt.first(schema,table)
        return pkey_col

    def create_indices(self, schema, table, index_data):
        """
            The method loops odver the list index_data and creates the indices on the table
            specified with schema and table parameters.
            The method assumes there is a database connection active.

            :param schema: the schema name where table belongs
            :param table: the table name where the data should be inserted
            :param index_data: a list of dictionaries with the index metadata for the given table.
            :return: a list with the eventual column(s) used as primary key
            :rtype: list
        """
        idx_ddl = {}
        table_primary = []
        for index in index_data:
                table_timestamp = str(int(time.time()))
                indx = index["index_name"]
                self.logger.debug("Building DDL for index %s" % (indx))
                idx_col = [column.strip() for column in index["index_columns"].split(',')]
                index_columns = ['"%s"' % column.strip() for column in idx_col]
                non_unique = index["non_unique"]
                index_type = index["index_type"]
                if indx =='PRIMARY':
                    pkey_name = format("pk_%s_%s_%s" % (table[0:100],table_timestamp,  self.idx_sequence))
                    pkey_def = 'ALTER TABLE "%s"."%s" ADD CONSTRAINT "%s" PRIMARY KEY (%s) ;' % (schema, table, pkey_name, ','.join(index_columns))
                    idx_ddl[pkey_name] = pkey_def
                    table_primary = idx_col
                else:
                    if non_unique == 0:
                        unique_key = 'UNIQUE'
                        if table_primary == []:
                            table_primary = idx_col
                    else:
                        unique_key = ''
                    # openGauss doesn't support multi column for GIN index, so use BTREE when columns > 1
                    if index_type == 'BTREE' or len(index_columns) > 1:
                        using = 'USING BTREE(%s)' % (','.join(index_columns))
                    else:
                        using = "USING GIN(to_tsvector('simple', %s))" % (index_columns[0])
                    index_name='idx_%s_%s_%s_%s' % (indx[0:10], table[0:10], table_timestamp, self.idx_sequence)
                    idx_def='CREATE %s INDEX "%s" ON "%s"."%s" %s;' % (unique_key, index_name, schema, table, using)
                    idx_ddl[index_name] = idx_def
                self.idx_sequence+=1
        for index in idx_ddl:
            self.logger.info("Building index %s on %s.%s" % (index, schema, table))
            self.pgsql_conn.execute(idx_ddl[index])

        return table_primary

    def swap_schemas(self):
        """
            The method  loops over the schema_loading class dictionary and
            swaps the loading with the destination schemas performing a double rename.
            The method assumes there is a database connection active.
        """
        for schema in self.schema_loading:
            schema_loading = self.schema_loading[schema]["loading"]
            schema_destination = self.schema_loading[schema]["destination"]
            schema_temporary = "_rename_%s" % self.schema_loading[schema]["destination"]
            sql_dest_to_tmp = ("ALTER SCHEMA \"{}\" RENAME TO \"{}\";").format((schema_destination), (schema_temporary))
            sql_load_to_dest = ("ALTER SCHEMA \"{}\" RENAME TO \"{}\";").format((schema_loading), (schema_destination))
            sql_tmp_to_load = ("ALTER SCHEMA \"{}\" RENAME TO \"{}\";").format((schema_temporary), (schema_loading))
            x = self.pgsql_conn.xact()
            x.start()
            self.logger.info("Swapping schema %s with %s" % (schema_destination, schema_loading))
            self.logger.debug("Renaming schema %s in %s" % (schema_destination, schema_temporary))
            self.pgsql_conn.execute(sql_dest_to_tmp)
            self.logger.debug("Renaming schema %s in %s" % (schema_loading, schema_destination))
            self.pgsql_conn.execute(sql_load_to_dest)
            self.logger.debug("Renaming schema %s in %s" % (schema_temporary, schema_loading))
            self.pgsql_conn.execute(sql_tmp_to_load)
            self.logger.debug("Commit the swap transaction" )
            x.commit()

    def set_batch_processed(self, id_batch):
        """
            The method updates the flag b_processed and sets the processed timestamp for the given batch id.
            The event ids are aggregated into the table t_batch_events used by the replay function.

            :param id_batch: the id batch to set as processed
        """
        self.logger.debug("updating batch %s to processed" % (id_batch, ))
        sql_update="""
            UPDATE sch_chameleon.t_replica_batch
                SET
                    b_processed=True,
                    ts_processed=now()
            WHERE
                i_id_batch=%s
            ;
        """
        self.pgsql_conn.execute(sql_update % (id_batch, ))
        self.logger.debug("collecting events id for batch %s " % (id_batch, ))
        sql_collect_events = """
            INSERT INTO
                sch_chameleon.t_batch_events
                (
                    i_id_batch,
                    i_id_event
                )
            SELECT
                i_id_batch,
                array_agg(i_id_event)
            FROM
            (
                SELECT *
                FROM sch_chameleon.fn_get_event_datatime(%s)
            ) t_event
            GROUP BY
                    i_id_batch
            ;
        """
        self.pgsql_conn.execute(sql_collect_events % (id_batch, ))


    def __swap_enums(self):
        """
            The method searches for enumerations in the loading schemas and swaps them with the types eventually
            present in the destination schemas
        """
        sql_get_enum = """
            SELECT
                typname
            FROM
                pg_type typ
                INNER JOIN pg_namespace sch
                ON
                    typ.typnamespace=sch.oid
            WHERE
                    sch.nspname='%s'
                and	typcategory='E'
            ;
        """

        for schema in self.schema_tables:
            schema_loading = self.schema_loading[schema]["loading"]
            schema_destination = self.schema_loading[schema]["destination"]
            stmt=self.pgsql_conn.execute(sql_get_enum% (schema_loading,))
            enum_list = stmt()
            for enumeration in enum_list:
                type_name = enumeration[0]
                sql_drop_origin = ("DROP TYPE IF EXISTS \"{}\".\"{}\" CASCADE;").format((schema_destination),(type_name))
                sql_set_schema_new = ("ALTER TYPE \"{}\".\"{}\" SET SCHEMA \"{}\";").format((schema_loading),(type_name), (schema_destination))
                self.logger.debug("Dropping the original tpye %s.%s " % (schema_destination, type_name))
                self.pgsql_conn.execute(sql_drop_origin)
                self.logger.debug("Changing the schema for type %s.%s to %s" % (schema_loading, type_name, schema_destination))
                self.pgsql_conn.execute(sql_set_schema_new)


    def swap_tables(self):
        """
            The method loops over the tables stored in the class
        """
        for schema in self.schema_tables:
            schema_loading = self.schema_loading[schema]["loading"]
            schema_destination = self.schema_loading[schema]["destination"]
            for table in self.schema_tables[schema]:
                self.logger.info("Swapping table %s.%s with %s.%s" % (schema_destination, table, schema_loading, table))
                sql_drop_origin = ("DROP TABLE IF EXISTS \"{}\".\"{}\" ;").format((schema_destination),(table))
                sql_set_schema_new = ("ALTER TABLE \"{}\".\"{}\" SET SCHEMA \"{}\";").format((schema_loading),(table), (schema_destination))
                x = self.pgsql_conn.xact()
                x.start()
                self.logger.debug("Dropping the original table %s.%s " % (schema_destination, table))
                self.pgsql_conn.execute(sql_drop_origin)
                self.logger.debug("Changing the schema for table %s.%s to %s" % (schema_loading, table, schema_destination))
                self.pgsql_conn.execute(sql_set_schema_new)
                x.commit()

        self.__swap_enums()
    def create_database_schema(self, schema_name):
        """
            The method creates a database schema.
            The create schema is issued with the clause IF NOT EXISTS.
            Should the schema be already present the create is skipped.

            :param schema_name: The schema name to be created.
        """
        sql = 'select * from pg_namespace where nspname=$1'
        result = self.pgsql_conn.query.first(sql, schema_name)
        if not result:
            sql_create = ("CREATE SCHEMA \"{}\";").format((schema_name))
            self.pgsql_conn.execute(sql_create)

    def drop_database_schema(self, schema_name, cascade):
        """
            The method drops a database schema.
            The drop can be either schema is issued with the clause IF NOT EXISTS.
            Should the schema be already present the create is skipped.

            :param schema_name: The schema name to be created.
            :param schema_name: If true the schema is dropped with the clause cascade.
        """
        if cascade:
            cascade_clause = "CASCADE"
        else:
            cascade_clause = ""
        sql_drop = "DROP SCHEMA IF EXISTS \"{}\" %s;" % cascade_clause
        sql_drop = (sql_drop).format((schema_name))
        self.set_lock_timeout()
        try:
            self.pgsql_conn.execute(sql_drop)
        except:
            self.logger.error("could not drop the schema %s. You will need to drop it manually." % schema_name)

    def add_object(self, object_name, schema, dst_object_sql):
        """
        Create an object on opengauss.

        :param object_name:  the object name, use for log output
        :param schema: the object's src schema
        :param dst_object_sql: The sql statement of creating this object, which is used to dst database
        :return:
        """
        self.logger.info("Create an object %s on opengauss." % (object_name,))
        self.connect_db()
        self.pgsql_conn.execute("SET SCHEMA '%s';" % (schema,))
        self.pgsql_conn.execute(dst_object_sql)

    def insert_object_replicate_record(self, object_name, db_object_type, src_object_sql, dst_object_sql=None):
        """
        Insert a result entry of object replication into the table sch_chameleon.t_replica_object.

        :param object_name: A str of the object name, use for log output
        :param db_object_type: The object type
        :param src_object_sql: The sql statement of creating this object, which is read from src database.
        :param dst_object_sql: The sql statement of creating this object, which is used to dst database
        """
        self.logger.info("Insert an entry of %s replication into the object replication status table." % (object_name,))
        self.connect_db()

        sql_to_get_replica_source_id = """SELECT I_ID_SOURCE FROM sch_chameleon.t_sources WHERE T_SOURCE = '%s';"""
        i_id_source = self.pgsql_conn.prepare(sql_to_get_replica_source_id % (self.source,)).first()
        self.logger.debug("The replication source id is %s." % (i_id_source,))

        sql = """INSERT INTO sch_chameleon.t_replica_object(i_id_source,
                                           en_object_type,
                                           b_status,
                                           t_src_object_sql,
                                           t_dst_object_sql)
                        VALUES ('%s','%s', %s, $$%s$$, %s);"""

        # if dst_object_sql value is no none, it means the replication is successful, else failed
        if dst_object_sql:
            sql_insert = sql % (i_id_source, db_object_type.name, True, src_object_sql,
                                "$dst_object_sql$" + dst_object_sql + "$dst_object_sql$")
        else:
            sql_insert = sql % (i_id_source, db_object_type.name, False, src_object_sql, "NULL")
        self.pgsql_conn.execute(sql_insert)
