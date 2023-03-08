import codecs
import gc
import io
import logging
import multiprocessing
import os
import re
import secrets
import sys
import queue
import codecs
import json
import secrets
from os import remove
import MySQLdb
import MySQLdb.cursors
import binascii
import pymysql
from geomet import wkb
from geomet import wkt
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, GtidEvent, HeartbeatLogEvent
from pymysqlreplication.event import QueryEvent, GtidEvent, HeartbeatLogEvent, XidEvent
from pymysqlreplication.row_event import DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
from pg_chameleon import sql_token, ColumnType
from pg_chameleon.lib.parallel_replication import BinlogTrxReader, MyGtidEvent, modified_my_gtid_event
from pg_chameleon.lib.pg_lib import pg_engine
from pg_chameleon.lib.sql_util import SqlTranslator, DBObjectType
from pg_chameleon.lib.task_lib import copy_data_task, create_index_task, read_data_task
from pg_chameleon.lib.task_lib import TableMetadataTask, ColumnMetadataTask
from pg_chameleon.lib.task_lib import KeyWords

POINT_PREFIX_LEN = len('POINT ')
POLYGON_PREFIX_LEN = len('POLYGON ')
LINESTR_PREFIX_LEN = len('LINESTRING ')
WKB_PREFIX_LEN = 4
MARIADB = "MariaDB"


# we set the default max size of csv file is 2M
DEFAULT_MAX_SIZE_OF_CSV: int = 2 * 1024 * 1024
MINIUM_QUEUE_SIZE: int = 5
# max_execution_time for MySQL(ms)
MAX_EXECUTION_TIME = 9999000
RANDOM_STR = "RANDOM_STR_SUFFIX"

BINARY_BASE = 2
# 6 means the significant figures, g is python format
DEFAULT_FLOAT_FORMAT_STR = '{:6g}'
# 18 means the significant figures, 16 means the precision, e is python format
DEFAULT_DOUBLE_FORMAT_STR = '{:18.16e}'
CSV_FILE_SUB_DIR = "chameleon"

class process_state():
    PRECISION_START=0
    COUNT_EMPTY=0
    PRECISION_SUCCESS=1
    PENDING_STATUS=1
    PROCESSING_STATUS=2
    ACCOMPLISH_STATUS=3
    FAIL_STATUS=6

    @classmethod
    def is_precision_success(self, status):
        return status == self.PRECISION_SUCCESS

    @classmethod
    def is_fail_status(self, status):
        return status == self.FAIL_STATUS

class reader_cursor_manager(object):
    def __init__(self, conn_buffered, conn_unbuffered):
        self.cursor_buffered = conn_buffered.cursor()
        self.cursor_unbuffered = conn_unbuffered.cursor()

    def __del__(self):
        self.close()

    def close(self):
        self.cursor_buffered.close()
        self.cursor_unbuffered.close()

class mysql_source(object):
    def __init__(self):
        """
            Class constructor, the method sets the class variables and configure the
            operating parameters from the args provided t the class.
        """
        self.statement_skip = ['BEGIN', 'COMMIT']
        self.schema_tables = {}
        self.schema_mappings = {}
        self.schema_loading = {}
        self.schema_list = []
        self.hexify_always = ColumnType.get_mysql_hexify_always_type()
        self.postgis_spatial_datatypes = ColumnType.get_mysql_postgis_spatial_type()
        self.common_spatial_datatypes = ColumnType.get_mysql_common_spatial_type()
        self.schema_only = {}
        self.gtid_mode = False
        self.gtid_enable = False
        self.decode_map = {}
        self.write_task_queue = None
        self.read_task_queue = None
        self.is_mariadb = False
        self.version = 0
        """
            This queue index_waiting_queue is used to temporarily store index tasks.
            After all data read tasks are complete,
            all index tasks are removed from the queue and added to the write_task_queue.
        """
        self.index_waiting_queue = None
        self.table_metadata_queue = None
        self.column_metadata_queue = None
        self.__init_decode_map()
        self.sql_translator = SqlTranslator()
 
    @classmethod
    def initJson(cls):
        manager = multiprocessing.Manager()
        global managerJson 
        managerJson = manager.dict({})

    @classmethod
    def getmanagerJson(cls):
        return managerJson

    @classmethod
    def __decode_hexify_value(cls, origin_value, numeric_scale):
        return binascii.hexlify(origin_value).decode().upper()

    @classmethod
    def __decode_json_value(cls, origin_value, numeric_scale):
        return str(cls.__decode_dic_keys(origin_value)).replace("'", "\"")

    @classmethod
    def __decode_postgis_spatial_value(cls, origin_value, numeric_scale):
        return cls.__get_text_spatial(origin_value).upper()

    @classmethod
    def __decode_binary_value(cls, origin_value, numeric_scale):
        if not isinstance(origin_value, bytes):
            return origin_value
        return origin_value.decode()

    @classmethod
    def __decode_point_value(cls, origin_value, numeric_scale):
        return wkt.dumps(wkb.loads(origin_value[WKB_PREFIX_LEN:]))[POINT_PREFIX_LEN:].replace(' ', ',')

    @classmethod
    def __decode_polygon_value(cls, origin_value, numeric_scale):
        return wkt.dumps(wkb.loads(origin_value[WKB_PREFIX_LEN:]))[POLYGON_PREFIX_LEN:].replace(', ', '),(').replace(' ', ',')

    @classmethod
    def __decode_linestr_value(cls, origin_value, numeric_scale):
        return '[' + wkt.dumps(wkb.loads(origin_value[WKB_PREFIX_LEN:]))[LINESTR_PREFIX_LEN:].replace(', ', '),(').replace(' ', ',') + ']'

    @classmethod
    def __decode_float_value(cls, float_type):
        def __real_decode(origin_value, numeric_scale):
            if not numeric_scale or numeric_scale == 'NULL':
                format_str = DEFAULT_FLOAT_FORMAT_STR if (
                            float_type == ColumnType.M_FLOAT.value) else DEFAULT_DOUBLE_FORMAT_STR
            else:
                format_str = '{:.' + str(numeric_scale) +'f}'
            return float(format_str.format(origin_value))
        return __real_decode

    @classmethod
    def __decode_bit_value(cls, origin_value, numeric_scale):
        return int(origin_value, BINARY_BASE)

    @classmethod
    def __decode_set_value(cls, origin_value, numeric_scale):
        value = str(origin_value)
        binlog_list = value[1:len(value) - 1].replace("'", "").split(", ")
        type_list = numeric_scale[4:len(numeric_scale) - 1].replace("'", "").split(",")
        result_list = []
        for element in type_list:
            if element in binlog_list:
                result_list.append(element)
        return ",".join(result_list)

    @classmethod
    def __decode_default_value(cls, origin_value, numeric_scale):
        return origin_value

    def __init_decode_map(self):
        self.decode_map = {
            ColumnType.M_C_GIS_POINT.value: self.__decode_point_value,
            ColumnType.M_C_GIS_GEO.value: self.__decode_point_value,
            ColumnType.M_C_GIS_LINESTR.value: self.__decode_linestr_value,
            ColumnType.M_C_GIS_POLYGON.value: self.__decode_polygon_value,
            ColumnType.M_JSON.value: self.__decode_json_value,
            ColumnType.M_BINARY.value: self.__decode_binary_value,
            ColumnType.M_VARBINARY.value: self.__decode_binary_value,
            ColumnType.M_FLOAT.value: self.__decode_float_value(ColumnType.M_FLOAT.value),
            ColumnType.M_DOUBLE.value: self.__decode_float_value(ColumnType.M_DOUBLE.value),
            ColumnType.M_DOUBLE_P.value: self.__decode_float_value(ColumnType.M_DOUBLE.value),
            ColumnType.M_SET.value: self.__decode_set_value,
            ColumnType.M_BIT.value: self.__decode_bit_value
        }
        for v in self.hexify_always:
            self.decode_map[v] = self.__decode_hexify_value
        for v in self.postgis_spatial_datatypes:
            self.decode_map[v] = self.__decode_postgis_spatial_value

    def __del__(self):
        """
            Class destructor, tries to disconnect the mysql connection.
        """
        self.disconnect_db_unbuffered()
        self.disconnect_db_buffered()

    def check_mysql_config(self, is_strict=False):
        """
            The method check if the mysql configuration is compatible with the replica requirements.
            If all the configuration requirements are met then the return value is True.
            Otherwise is false.
            The parameters checked are
            log_bin - ON if the binary log is enabled
            binlog_format - must be ROW , otherwise the replica won't get the data
            binlog_row_image - must be FULL, otherwise the row image will be incomplete
        """
        if self.gtid_enable:
            sql_log_bin = """SHOW GLOBAL VARIABLES LIKE 'gtid_mode';"""
            self.cursor_buffered.execute(sql_log_bin)
            variable_check = self.cursor_buffered.fetchone()
            if variable_check:
                gtid_mode = variable_check["Value"]
                if gtid_mode.upper() == 'ON':
                    self.gtid_mode = True
                    sql_uuid = """SHOW SLAVE STATUS;"""
                    self.cursor_buffered.execute(sql_uuid)
                    slave_status = self.cursor_buffered.fetchall()
                    if len(slave_status)>0:
                        gtid_set=slave_status[0]["Retrieved_Gtid_Set"]
                    else:
                        sql_uuid = """SHOW GLOBAL VARIABLES LIKE 'server_uuid';"""
                        self.cursor_buffered.execute(sql_uuid)
                        server_uuid = self.cursor_buffered.fetchone()
                        gtid_set = server_uuid["Value"]
                    self.gtid_uuid = gtid_set.split(':')[0]

            else:
                self.gtid_mode = False
        else:
            self.gtid_mode = False

        sql_log_bin = """SHOW GLOBAL VARIABLES LIKE 'log_bin';"""
        self.cursor_buffered.execute(sql_log_bin)
        variable_check = self.cursor_buffered.fetchone()
        log_bin = variable_check["Value"]

        sql_log_bin = """SHOW GLOBAL VARIABLES LIKE 'binlog_format';"""
        self.cursor_buffered.execute(sql_log_bin)
        variable_check = self.cursor_buffered.fetchone()
        binlog_format = variable_check["Value"]

        sql_log_bin = """SHOW GLOBAL VARIABLES LIKE 'binlog_row_image';"""
        self.cursor_buffered.execute(sql_log_bin)
        variable_check = self.cursor_buffered.fetchone()
        if variable_check:
            binlog_row_image = variable_check["Value"]
        else:
            binlog_row_image = 'FULL'

        sql_gtid_mode = """SHOW GLOBAL VARIABLES LIKE 'gtid_mode';"""
        self.cursor_buffered.execute(sql_gtid_mode)
        variable_check = self.cursor_buffered.fetchone()
        gtid_mode = variable_check["Value"]

        sql_log_bin = """SHOW GLOBAL VARIABLES LIKE 'version';"""
        self.cursor_buffered.execute(sql_log_bin)
        variable_check = self.cursor_buffered.fetchone()
        self.is_mariadb = False if variable_check["Value"].find(MARIADB) == -1 else True
        self.version = sql_token.parse_version(variable_check["Value"])
        self.pg_engine.mysql_version = -1 if self.is_mariadb == False else self.version
        self.logger.debug("mysql version %d" % self.version)

        if self.mysql_restart_config or is_strict:
            if log_bin.upper() != 'ON' or binlog_format.upper() != 'ROW' or binlog_row_image.upper() != 'FULL' \
                    or gtid_mode.upper() != 'ON':
                self.logger.error("The MySQL configuration does not allow the replica. Exiting now")
                self.logger.error("Source settings - log_bin %s, binlog_format %s, binlog_row_image %s, gtid_mode %s"
                                  % (log_bin.upper(), binlog_format.upper(), binlog_row_image.upper(), gtid_mode.upper()))
                self.logger.error("Mandatory settings - log_bin ON, binlog_format ROW, binlog_row_image FULL, gtid_mode"
                                  " ON (only for MySQL 5.6+) ")
                sys.exit()
        else:
            self.logger.warning("Source settings - log_bin %s, binlog_format %s, binlog_row_image %s, gtid_mode %s"
                                % (log_bin.upper(), binlog_format.upper(), binlog_row_image.upper(), gtid_mode.upper()))
            self.logger.warning("Mandatory settings for online migration - log_bin ON, binlog_format ROW,"
                                " binlog_row_image FULL, gtid_mode ON (only for MySQL 5.6+) ")

    def get_connect(self, is_buffered=True):
        db_conn = self.source_config["db_conn"]
        db_conn = {key: str(value) for key, value in db_conn.items()}
        db_conn["port"] = int(db_conn["port"])
        db_conn["connect_timeout"] = int(db_conn["connect_timeout"])
        conn = MySQLdb.connect(
            host=db_conn["host"],
            user=db_conn["user"],
            port=db_conn["port"],
            password=db_conn["password"],
            charset=db_conn["charset"],
            connect_timeout=db_conn["connect_timeout"],
            autocommit=True,
            cursorclass=MySQLdb.cursors.DictCursor if is_buffered else MySQLdb.cursors.SSCursor
        )
        return conn

    def get_new_engine(self):
        new_engine = pg_engine()
        new_engine.dest_conn = self.pg_engine.dest_conn
        new_engine.logger = self.pg_engine.logger
        new_engine.source = self.pg_engine.source
        new_engine.full = self.pg_engine.full
        new_engine.type_override = self.pg_engine.type_override
        new_engine.sources = self.pg_engine.sources
        new_engine.notifier = self.pg_engine.notifier
        new_engine.migrate_default_value = self.pg_engine.migrate_default_value
        new_engine.mysql_version = -1 if self.is_mariadb == False else self.version
        new_engine.column_case_sensitive = self.pg_engine.column_case_sensitive
        return new_engine

    def connect_db_buffered(self):
        """
            The method creates a new connection to the mysql database.
            The connection is made using the dictionary type cursor factory, which is buffered.
        """
        db_conn = self.source_config["db_conn"]
        db_conn = {key:str(value) for key, value in db_conn.items()}
        db_conn["port"] = int(db_conn["port"])
        db_conn["connect_timeout"] = int(db_conn["connect_timeout"])

        self.conn_buffered=pymysql.connect(
            host = db_conn["host"],
            user = db_conn["user"],
            port = db_conn["port"],
            password = db_conn["password"],
            charset = db_conn["charset"],
            connect_timeout = db_conn["connect_timeout"],
            cursorclass=pymysql.cursors.DictCursor
        )
        self.charset = db_conn["charset"]
        self.cursor_buffered = self.conn_buffered.cursor()

    def disconnect_db_buffered(self):
        """
            The method disconnects any connection  with dictionary type cursor from the mysql database.

        """
        try:
            self.conn_buffered.close()
        except:
            pass

    def connect_db_unbuffered(self):
        """
            The method creates a new connection to the mysql database.
            The connection is made using the unbuffered cursor factory.
        """
        db_conn = self.source_config["db_conn"]
        db_conn = {key:str(value) for key, value in db_conn.items()}
        db_conn["port"] = int(db_conn["port"])
        db_conn["connect_timeout"] = int(db_conn["connect_timeout"])
        self.conn_unbuffered=pymysql.connect(
            host = db_conn["host"],
            user = db_conn["user"],
            port = db_conn["port"],
            password = db_conn["password"],
            charset = db_conn["charset"],
            connect_timeout = db_conn["connect_timeout"],
            cursorclass=pymysql.cursors.SSCursor
        )
        self.charset = db_conn["charset"]
        self.cursor_unbuffered = self.conn_unbuffered.cursor()

    def disconnect_db_unbuffered(self):
        """
            The method disconnects any unbuffered connection from the mysql database.
        """
        try:
            self.conn_unbuffered.close()
        except:
            pass

    def __build_skip_events(self):
        """
            The method builds a class attribute self.skip_events. The attribute is a dictionary with the tables and schemas listed under the three kind of skippable events  (insert,delete,update) using
            the configuration parameter skip_events.
        """
        self.skip_events = None
        if "skip_events" in  self.source_config:
            skip_events  = self.source_config["skip_events"]
            self.skip_events = {}
            if "insert" in skip_events:
                self.skip_events["insert"] = skip_events["insert"]
            else:
                self.skip_events["insert"] = []

            if "update" in skip_events:
                self.skip_events["update"] = skip_events["update"]
            else:
                self.skip_events["update"] = []

            if "delete" in skip_events:
                self.skip_events["delete"] = skip_events["delete"]
            else:
                self.skip_events["delete"] = []

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
                limit_schemas = [table.split('.')[0] for table in limit_tables]
                limit_tables = [table for table in tables if table in limit_tables or table.split('.')[0] not in limit_schemas]
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
                    try:
                        list_exclude.append(table_list[1])
                    except IndexError:
                        pass

                self.limit_tables[table_list[0]]  = list_exclude
        if skip_tables:
            table_skip = [table.split('.') for table in skip_tables]
            for table_list in table_skip:
                list_exclude = []
                try:
                    list_exclude = self.skip_tables[table_list[0]]
                    list_exclude.append(table_list[1])
                except KeyError:
                    try:
                        list_exclude.append(table_list[1])
                    except:
                        pass
                self.skip_tables[table_list[0]]  = list_exclude

    def get_table_list(self):
        """
            The method pulls the table list from the information_schema.
            The list is stored in a dictionary  which key is the table's schema.
        """
        sql_tables="""
            SELECT
                table_name as table_name,
                table_rows as table_rows
            FROM
                information_schema.TABLES
            WHERE
                    table_type='BASE TABLE'
                AND table_schema=%s
            ;
        """
        for schema in self.schema_list:
            self.cursor_buffered.execute(sql_tables, (schema))
            table_list = [table["table_name"] for table in self.cursor_buffered.fetchall()]

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
            if self.dump_json:
                for index,key in enumerate(table_list):
                    sql_tables_rows="""SELECT COUNT(*) COUNT FROM %s.%s""" % (schema, key)
                    self.cursor_buffered.execute(sql_tables_rows)
                    table_count = self.cursor_buffered.fetchone()['COUNT']
                    managerJson.update({key:{
                        "name":key,
                        "status":process_state.ACCOMPLISH_STATUS if table_count == process_state.COUNT_EMPTY else process_state.PENDING_STATUS,
                        "percent":process_state.PRECISION_START,
                        "count_rows":table_count
                    }})


    def create_destination_schemas(self):
        """
            Creates the loading schemas in the destination database and associated tables listed in the dictionary
            self.schema_tables.
            The method builds a dictionary which associates the destination schema to the loading schema.
            The loading_schema is named after the destination schema plus with the prefix _ and the _tmp suffix.
            As postgresql allows, by default up to 64  characters for an identifier, the original schema is truncated to 59 characters,
            in order to fit the maximum identifier's length.
            The mappings are stored in the class dictionary schema_loading.
            If the source parameter keep_existing_schema is set to true the method doesn't create the schemas.
            Instead assumes the schema and the tables are already there.
        """
        if self.keep_existing_schema:
            self.logger.debug("Keep existing schema is set to True. Skipping the schema creation." )
            for schema in self.schema_list:
                destination_schema = self.schema_mappings[schema]
                self.schema_loading[schema] = {'destination':destination_schema, 'loading':destination_schema}
        else:
            for schema in self.schema_list:
                destination_schema = self.schema_mappings[schema]
                loading_schema = "_%s_tmp" % destination_schema[0:59]
                self.schema_loading[schema] = {'destination':destination_schema, 'loading':loading_schema}
                self.logger.debug("Creating the loading schema %s." % loading_schema)
                self.pg_engine.create_database_schema(loading_schema)
                self.logger.debug("Creating the destination schema %s." % destination_schema)
                self.pg_engine.create_database_schema(destination_schema)

    def drop_loading_schemas(self):
        """
            The method drops the loading schemas from the destination database.
            The drop is performed on the schemas generated in create_destination_schemas.
            The method assumes the class dictionary schema_loading is correctly set.
        """
        for schema in self.schema_loading:
            loading_schema = self.schema_loading[schema]["loading"]
            self.logger.debug("Dropping the schema %s." % loading_schema)
            self.pg_engine.drop_database_schema(loading_schema, True)

    def get_partition_metadata(self, table, schema):
        """
            The method builds the table's partition metadata querying the information_schema.
            The data is returned as a dictionary.

            :param table: The table name
            :param schema: The table's schema
            :return: table's partition metadata as a cursor dictionary
            :rtype: dictionary
        """
        sql_metadata="""
            SELECT DISTINCT
                partition_ordinal_position as partition_ordinal_position,
                subpartition_ordinal_position as subpartition_ordinal_position,
                subpartition_name as subpartition_name,
                subpartition_method as subpartition_method,
                subpartition_expression as subpartition_expression,
                partition_name as partition_name,
                partition_method as partition_method,
                partition_expression as partition_expression,
                partition_description as partition_description,
                tablespace_name as tablespace_name
            FROM
                information_schema.partitions
            WHERE
                    table_schema=%s
                AND table_name=%s
            ORDER BY
                partition_ordinal_position
            ;
        """
        self.cursor_buffered.execute(sql_metadata, (schema, table))
        partition_metadata=self.cursor_buffered.fetchall()
        return partition_metadata

    def get_table_metadata(self, table, schema):
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
                column_name as column_name,
                column_default as column_default,
                ordinal_position as ordinal_position,
                data_type as data_type,
                column_type as column_type,
                character_maximum_length as character_maximum_length,
                extra as extra,
                column_key as column_key,
                is_nullable as is_nullable,
                numeric_precision as numeric_precision,
                numeric_scale as numeric_scale,
                CASE
                    WHEN data_type="enum"
                THEN
                    SUBSTRING(COLUMN_TYPE,5)
                END AS enum_list,
                column_comment AS column_comment
            FROM
                information_schema.COLUMNS
            WHERE
                    table_schema=%s
                AND	table_name=%s
            ORDER BY
                ordinal_position
            ;
        """
        self.cursor_buffered.execute(sql_metadata, (schema, table))
        table_metadata=self.cursor_buffered.fetchall()
        return table_metadata

    def get_foreign_keys_metadata(self):
        """
            The method collects the foreign key metadata for the detach replica process.
        """
        self.__init_sync()
        schema_replica = "'%s'"  % "','".join([schema.strip() for schema in self.sources[self.source]["schema_mappings"]])
        self.logger.info("retrieving foreign keys metadata for schemas %s" % schema_replica)
        sql_fkeys = """
            SELECT s.table_name,
                   s.table_schema,
                   s.constraint_name,
                   s.referenced_table_name,
                   s.referenced_table_schema,
                   s.fk_cols,
                   s.ref_columns,
                   rc.update_rule as update_rule,
                   rc.delete_rule as delete_rule
            FROM (
                SELECT
                    table_name as table_name,
                    table_schema as table_schema,
                    constraint_name as constraint_name,
                    referenced_table_name as referenced_table_name,
                    referenced_table_schema as referenced_table_schema,
                    GROUP_CONCAT(concat('"',column_name,'"') ORDER BY POSITION_IN_UNIQUE_CONSTRAINT) as fk_cols,
                    GROUP_CONCAT(concat('"',REFERENCED_COLUMN_NAME,'"') ORDER BY POSITION_IN_UNIQUE_CONSTRAINT) as ref_columns
                FROM
                    information_schema.key_column_usage
                WHERE
                        table_schema in (%s)
                    AND referenced_table_name IS NOT NULL
                    AND referenced_table_schema in (%s)
                GROUP BY
                    table_name,
                    constraint_name,
                    referenced_table_name,
                    table_schema,
                    referenced_table_schema
                ORDER BY
                    table_name
            ) s
            JOIN information_schema.referential_constraints rc ON rc.constraint_schema = s.table_schema
            AND rc.constraint_name = s.constraint_name
            AND rc.table_name = s.table_name
            ;

        """ % (schema_replica, schema_replica)
        self.cursor_buffered.execute(sql_fkeys)
        fkey_list=self.cursor_buffered.fetchall()
        self.disconnect_db_buffered()
        return fkey_list

    def create_destination_tables(self):
        """
            The method creates the destination tables in the loading schema.
            The tables names are looped using the values stored in the class dictionary schema_tables.
        """
        for schema in self.schema_tables:
            table_list = self.schema_tables[schema]
            for table in table_list:
                table_metadata = self.get_table_metadata(table, schema)
                partition_metadata = self.get_partition_metadata(table, schema)
                self.pg_engine.create_table(table_metadata, partition_metadata, table, schema, 'mysql')

    def generate_table_metadata_statement(self, schema, table, cursor=None):
        """
            The method gets table metadata including schema, table, table_count, contain primary key.

            :param schema: the origin schema
            :param table: the table name
            :param cursor: the cursor
        """
        table_count_select = """SELECT COUNT(*) COUNT FROM %s.%s""" % (schema, table)
        if cursor is None:
            self.cursor_buffered.execute(table_count_select)
            table_count = self.cursor_buffered.fetchone()['COUNT']
        else:
            cursor.execute(table_count_select)
            table_count = cursor.fetchone()['COUNT']

        contain_primary_key_select = """SELECT COUNT(*) COUNT FROM information_schema.KEY_COLUMN_USAGE 
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"""
        if cursor is None:
            self.cursor_buffered.execute(contain_primary_key_select, (schema, table))
            contain_primary_key = self.cursor_buffered.fetchone()['COUNT']
        else:
            cursor.execute(contain_primary_key_select, (schema, table))
            contain_primary_key = cursor.fetchone()['COUNT']
        if contain_primary_key == 0:
            task = TableMetadataTask(schema, table, table_count, 0)
        else:
            task = TableMetadataTask(schema, table, table_count, 1)
        self.table_metadata_queue.put(task, block=True)

    def generate_column_metadata_statement(self, schema, table, cursor=None):
        """
            The method gets column metadata including schema, table, column_name, column_index, column_datatype, column_key.

            :param schema: the origin schema
            :param table: the table name
            :param cursor: the cursor
        """
        column_metadata_select = """SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE, COLUMN_KEY FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"""
        if cursor is None:
            self.cursor_buffered.execute(column_metadata_select, (schema, table))
            column_metadata = self.cursor_buffered.fetchall()
        else:
            cursor.execute(column_metadata_select, (schema, table))
            column_metadata = cursor.fetchall()
        for a_column_metadata in column_metadata:
            task = ColumnMetadataTask(schema, table, a_column_metadata["COLUMN_NAME"], a_column_metadata["ORDINAL_POSITION"],
                                        a_column_metadata["COLUMN_TYPE"], a_column_metadata["COLUMN_KEY"])
            self.column_metadata_queue.put(task, block=True)

    def generate_select_statements(self, schema, table, cursor=None):
        """
            The generates the csv output and the statements output for the given schema and table.
            The method assumes there is a buffered database connection active.

            :param schema: the origin's schema
            :param table: the table name
            :param cursor: the cursor
            :return: the select list statements for the copy to csv and  the fallback to inserts.
            :rtype: dictionary
        """
        random = secrets.token_hex(16) + RANDOM_STR
        select_columns = {}
        sql_select = """
            SELECT
                CASE
                    WHEN
                        data_type IN ('"""+"','".join(self.hexify)+"""')
                    THEN
                        concat('hex(',column_name,')')
                    WHEN
                        data_type IN ('"""+ColumnType.M_BINARY.value+"""')
                    THEN
                        concat('concat(\\'\\\\\\\\x\\', trim(trailing \\'00\\' from hex(',column_name,')))')
                    WHEN
                        data_type IN ('"""+ColumnType.M_BIT.value+"""')
                    THEN
                        concat('cast(`',column_name,'` AS unsigned)')
                    WHEN
                        data_type IN ('"""+ColumnType.M_DATATIME.value+"""','"""+ColumnType.M_TIMESTAMP.value+"""','"""+ColumnType.M_DATE.value+"""')
                    THEN
                        concat('nullif(`',column_name,'`,cast("0000-00-00 00:00:00" as date))')
                    WHEN
                        data_type IN ('"""+"','".join(self.postgis_spatial_datatypes)+"""')
                    THEN
                        concat('ST_AsText(',column_name,')')
                    WHEN
                        data_type IN ('"""+ColumnType.M_C_GIS_POINT.value+"""', '"""+ColumnType.M_C_GIS_GEO.value+"""')
                    THEN
                        concat('SUBSTR(REPLACE(ST_AsText(',column_name,'),\\' \\',\\',\\'), 6)')
                    WHEN
                        data_type IN ('"""+ColumnType.M_C_GIS_POLYGON.value+"""')
                    THEN
                        concat('SUBSTR(REPLACE(REPLACE(ST_AsText(',column_name,'),\\',\\',\\'),(\\'), \\' \\', \\',\\'),8)')
                    WHEN
                        data_type IN ('"""+ColumnType.M_C_GIS_LINESTR.value+"""')
                    THEN
                        concat('concat(REPLACE(REPLACE(REPLACE(ST_AsText(',column_name,'),\\'LINESTRING\\',\\'[\\'),\\',\\',\\'),(\\'),\\' \\',\\',\\'),\\']\\')')

                ELSE
                    concat('cast(`',column_name,'` AS char CHARACTER SET """+ self.charset +""")')
                END
                AS select_csv,
                column_name as column_name
            FROM
                information_schema.COLUMNS
            WHERE
                table_schema=%s
                AND 	table_name=%s
            ORDER BY
                ordinal_position
            ;
        """
        if cursor is None:
            self.cursor_buffered.execute(sql_select, (schema, table))
            select_data = self.cursor_buffered.fetchall()
        else:
            cursor.execute(sql_select, (schema, table))
            select_data = cursor.fetchall()

        # We use a random string here when the value is NULL, we can't use 'NULL' to represent a NULL value,
        # cause we can store a string which is 'NULL'(4 byte length string). But a random string is also not
        # a perfect method to slove this problem. We may need to find another method to slove this later.
        select_csv = "COALESCE(REPLACE(%s, '\"', '\"\"'),'{}') ".format(random)
        select_csv = [select_csv % statement["select_csv"] for statement in select_data]
        select_stat = [statement["select_csv"] for statement in select_data]
        if self.column_case_sensitive:
            column_list = ['"%s"' % statement["column_name"] for statement in select_data]
        else:
            column_list = []
            for statement in select_data:
                if statement["column_name"].lower() in KeyWords.keyword_set:
                    column_list.append("\"" + statement["column_name"].lower() + "\"")
                else:
                    column_list.append(statement["column_name"])

        select_columns["select_csv"] = "REPLACE(CONCAT('\"',CONCAT_WS('\",\"',%s),'\"'),'\"%s\"','NULL')" % (','.join(select_csv), random)
        select_columns["select_stat"]  = ','.join(select_stat)
        select_columns["column_list"]  = ','.join(column_list)
        return select_columns

    # Use an inner class to represent transactions
    class reader_xact:
        def __init__(self, outer_obj, cursor, table_txs):
            self.outer_obj = outer_obj
            self.cursor = cursor
            self.table_txs = table_txs

        def __enter__(self):
            if self.table_txs:
                self.outer_obj.begin_tx(self.cursor)

        def __exit__(self, exc_type, exc_val, exc_tb):
            if self.table_txs:
                self.outer_obj.end_tx(self.cursor)
            else:
                self.outer_obj.unlock_tables(self.cursor)

    def begin_tx(self, cursor):
        """
            The method sets the isolation level to repeatable read and begins a transaction
        """
        self.logger.debug("set isolation level")
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        self.logger.debug("beginning transaction")
        cursor.execute("BEGIN")

    def end_tx(self, cursor):
        """
            The method ends the current transaction by rollback
            - We should never have changed source anyway
        """
        self.logger.debug("rolling back")
        cursor.execute("ROLLBACK")

    def make_tx_snapshot(self, schema, table):
        """
            The method forces creation of transaction snapshot by making a read of
            one row from the source table and discarding it
        """
        self.logger.debug("reading and discarding 1 row from `%s`.`%s`" % (schema, table))
        self.cursor_unbuffered.execute("SELECT * FROM `%s`.`%s` LIMIT 1" % (schema, table))

    def lock_table(self, schema, table, cursor):
        """
            The method flushes the given table with read lock.
            The method assumes there is a database connection active.

            :param schema: the origin's schema
            :param table: the table name
            :param cursor: the cursor
        """
        self.logger.debug("locking the table `%s`.`%s`" % (schema, table))
        sql_lock = "FLUSH TABLES `%s`.`%s` WITH READ LOCK;" %(schema, table)
        cursor.execute(sql_lock)

    def unlock_tables(self, cursor):
        """
            The method unlocks all tables
        """
        self.logger.debug("unlocking the tables")
        sql_unlock = "UNLOCK TABLES;"
        cursor.execute(sql_unlock)

    def get_master_coordinates(self, cursor_buffered=None):
        """
            The method gets the master's coordinates and return them stored in a dictionary.
            The method assumes there is a database connection active.

            :return: the master's log coordinates for the given table
            :rtype: dictionary
        """
        sql_master = "SHOW MASTER STATUS;"
        if cursor_buffered is None:
            self.cursor_buffered.execute(sql_master)
            master_status = self.cursor_buffered.fetchall()
        else:
            cursor_buffered.execute(sql_master)
            master_status = cursor_buffered.fetchall()
        return master_status

    def data_reader(self):
        with self.get_connect(True) as conn_buffered, self.get_connect(False) as conn_unbuffered:
            self.execute_task(queue=self.read_task_queue, conn_unbuffered=conn_unbuffered, conn_buffered=conn_buffered)

    def data_writer(self):
        writer_engine = self.get_new_engine()
        writer_engine.connect_db()
        writer_engine.set_source_status("initialising")
        self.execute_task(queue=self.write_task_queue, engine=writer_engine)
        writer_engine.disconnect_db()

    def execute_task(self, queue, engine=None, conn_buffered=None, conn_unbuffered=None):
        while True:
            task = queue.get(block=True)
            if task is None:
                queue.put(task, block=True)
                break
            if engine is not None:
                # this is the data_writer process
                if isinstance(task, copy_data_task):
                    self.copy_table_data(task, engine)
                elif isinstance(task, create_index_task):
                    self.create_index_process(task, engine)
                elif isinstance(task, TableMetadataTask):
                    self.write_metadata_file(task, True)
                elif isinstance(task, ColumnMetadataTask):
                    self.write_metadata_file(task, False)
                else:
                    self.logger.error("unknown write task type")
            elif conn_unbuffered is not None and conn_buffered is not None:
                # this is the data_reader process
                if isinstance(task, read_data_task):
                    self.read_data_process(conn_buffered, conn_unbuffered, task)
                else:
                    self.logger.error("unknown read task type")
            else:
                self.logger.error("this is an error process")

    def read_data_process(self, conn_buffered, conn_unbuffered, task):
        cursor_manager = reader_cursor_manager(conn_buffered, conn_unbuffered)
        destination_schema = task.destination_schema
        loading_schema = task.loading_schema
        schema = task.schema
        table = task.table

        self.logger.info("Copying the source table %s into %s.%s" % (table, loading_schema, table))
        try:
            master_status = self.read_data_from_table(schema, table, cursor_manager)
            indices = self.__get_index_data(schema=schema, table=table, cursor_buffered=cursor_manager.cursor_buffered)
            index_write_task = create_index_task(table, schema, indices, destination_schema, master_status)
            self.index_waiting_queue.put(index_write_task, block=True)
        except:
            self.logger.info("Could not copy the table %s. Excluding it from the replica." % (table))
            raise
        finally:
            cursor_manager.close()

    def write_metadata_file(self, task, is_table_task):
        """
            The method writes table and column metadata to csv files.

            :param task: the table or column metadata task
            :param is_table_task: true if is table_metadata task
        """
        json_string = json.dumps(task.__dict__)
        schema = task.schema
        csv_file_dir = self.out_dir + os.sep + CSV_FILE_SUB_DIR + os.sep
        if is_table_task:
            metadata_file = csv_file_dir + "%s_information_schema_tables.csv" % schema
        else:
            metadata_file = csv_file_dir + "%s_information_schema_columns.csv" % schema
        column_file = open(metadata_file, 'a')
        column_file.write(json_string + os.linesep)
        column_file.close()

    def read_data_from_table(self, schema, table, cursor_manager):
        """
            The method copy the data between the origin and destination table.
            The method locks the table read only mode and  gets the log coordinates which are returned to the calling method.

            :param schema: the origin's schema
            :param table: the table name
            :param cursor_buffered: the cursor which store the data in client
            :param cursor_unbuffered: the cursor which store the data in server
            :return: the log coordinates for the given table
            :rtype: dictionary
        """
        self.logger.debug("estimating rows in %s.%s" % (schema , table))
        sql_rows = """
            SELECT
                table_rows as table_rows,
                CASE
                    WHEN avg_row_length>0
                    then
                        round(({}/avg_row_length))
                ELSE
                    0
                END as copy_limit,
                transactions
            FROM
                information_schema.TABLES,
                information_schema.ENGINES
            WHERE
                    table_schema=%s
                AND	table_type='BASE TABLE'
                AND table_name=%s
                AND TABLES.engine = ENGINES.engine
            ;
        """
        cursor_buffered = cursor_manager.cursor_buffered
        cursor_unbuffered = cursor_manager.cursor_unbuffered
        sql_rows = sql_rows.format(DEFAULT_MAX_SIZE_OF_CSV)
        cursor_buffered.execute(sql_rows, (schema, table))
        count_rows = cursor_buffered.fetchone()
        total_rows = count_rows["table_rows"]
        copy_limit = int(count_rows["copy_limit"])
        table_txs = count_rows["transactions"] == "YES"
        if copy_limit == 0:
            copy_limit = 1000000
        num_slices = int(total_rows//copy_limit)
        range_slices = list(range(num_slices+1))
        total_slices = len(range_slices)

        self.logger.debug("The table %s.%s will be copied in %s  estimated slice(s) of %s rows, using a transaction %s"  % (schema, table, total_slices, copy_limit, table_txs))

        # lock the table and flush the cache
        self.lock_table(schema, table, cursor_buffered)

        # get master status
        self.logger.debug("collecting the master's coordinates for table `%s`.`%s`" % (schema, table))
        master_status = self.get_master_coordinates(cursor_buffered)

        self.generate_table_metadata_statement(schema, table, cursor_buffered)
        self.generate_column_metadata_statement(schema, table, cursor_buffered)

        select_columns = self.generate_select_statements(schema, table, cursor_buffered)
        sql_csv = "SELECT /*+ MAX_EXECUTION_TIME(%d) */ %s as data FROM `%s`.`%s`;" %\
            (MAX_EXECUTION_TIME, select_columns["select_csv"], schema, table)
        self.logger.debug("Executing query for table %s.%s" % (schema, table))

        with self.reader_xact(self, cursor_buffered, table_txs):
            cursor_unbuffered.execute(sql_csv)
            # unlock tables
            if table_txs:
                self.unlock_tables(cursor_buffered)
            slice = 0
            while True:
                out_file = '%s/%s/%s_%s_slice%d.csv' % (self.out_dir, CSV_FILE_SUB_DIR, schema, table, slice + 1)
                csv_results = cursor_unbuffered.fetchmany(copy_limit)
                if len(csv_results) == 0:
                    break
                # '\x00' is '\0', which is a illeage char in openGauss, we need to remove it, but this
                # will lead to different value stored in MySQL and openGauss, we have no choice...
                csv_data = ("\n".join(d[0] for d in csv_results)).replace('\x00', '')
                if self.copy_mode == 'file':
                    csv_file = codecs.open(out_file, 'wb', self.charset)
                    csv_file.write(csv_data)
                    csv_file.close()
                    task = copy_data_task(out_file, count_rows, table, schema, select_columns, len(csv_results), slice)
                else:
                    if self.copy_mode != 'direct':
                        self.logger.warning("unknown copy mode, use direct instead")
                    csv_file = io.BytesIO()
                    csv_file.write(csv_data.encode())
                    csv_file.seek(0)
                    task = copy_data_task(csv_file, count_rows, table, schema, select_columns, len(csv_results), slice)
                self.write_task_queue.put(task, block=True)
                slice += 1

        return master_status

    def copy_table_data(self, task, writer_engine):
        slice_insert = []
        if self.copy_mode == "direct":
            csv_file = task.csv_file
        elif self.copy_mode == "file":
            csv_file = open(task.csv_file, 'rb')
        else:
            self.logger.warning("unknown copy mode, use \'direct\' mode")
            csv_file = task.csv_file
        if csv_file is None:
            self.logger.warning("this is an empty csv file, you should check your batch for errors")
            return
        rows = task.rows
        count_rows = task.count_rows
        schema = task.schema
        table = task.table
        select_columns = task.select_columns
        total_rows = count_rows["table_rows"]
        copy_limit = int(count_rows["copy_limit"])
        loading_schema = self.schema_loading[schema]["loading"]
        column_list = select_columns["column_list"]
        if copy_limit == 0:
            copy_limit = 1000000
        num_slices = int(total_rows // copy_limit)
        range_slices = list(range(num_slices + 1))
        total_slices = len(range_slices)
        slice = task.slice
        try:
            writer_engine.copy_data(csv_file, loading_schema, table, column_list)
        except Exception as e:
            self.logger.error("SQLCODE: %s SQLERROR: %s" % (e.code, e.message))
            self.logger.info(
                "Table %s.%s error in PostgreSQL copy, saving slice number for the fallback to insert statements" % (
                loading_schema, table))
            slice_insert.append(slice)
        finally:
            csv_file.close()
            del task
            gc.collect()
        self.print_progress(slice + 1, total_slices, schema, table, rows)
        slice += 1
        if len(slice_insert) > 0:
            ins_arg = {}
            ins_arg["slice_insert"] = slice_insert
            ins_arg["table"] = table
            ins_arg["schema"] = schema
            ins_arg["select_stat"] = select_columns["select_stat"]
            ins_arg["column_list"] = column_list
            ins_arg["copy_limit"] = copy_limit
            self.insert_table_data(ins_arg)

    def insert_table_data(self, ins_arg):
        """
            This method is a fallback procedure whether copy_table_data fails.
            The ins_args is a list with the informations required to run the select for building the insert
            statements and the slices's start and stop.
            The process is performed in memory and can take a very long time to complete.

            :param ins_arg: the list with the insert arguments (slice_insert, schema, table, select_stat,column_list, copy_limit)
        """
        slice_insert= ins_arg["slice_insert"]
        table = ins_arg["table"]
        schema = ins_arg["schema"]
        select_stat = ins_arg["select_stat"]
        column_list = ins_arg["column_list"]
        copy_limit = ins_arg["copy_limit"]
        loading_schema = self.schema_loading[schema]["loading"]
        conn_unbuffered = self.get_connect(False)
        cursor_unbuffered = conn_unbuffered.cursor()
        num_insert = 1
        for slice in slice_insert:
            self.logger.info("Executing inserts in %s.%s. Slice %s. Rows per slice %s." % (
            loading_schema, table, num_insert, copy_limit,))
            offset = slice * copy_limit
            sql_fallback = "SELECT %s FROM `%s`.`%s` LIMIT %s, %s;" % (
            select_stat, schema, table, offset, copy_limit)
            cursor_unbuffered.execute(sql_fallback)
            insert_data = cursor_unbuffered.fetchall()
            self.pg_engine.insert_data(loading_schema, table, insert_data, column_list)
            num_insert += 1
        cursor_unbuffered.close()
        conn_unbuffered.close()

    def create_index_process(self, task, writer_engine):
        if task.indices is None:
            self.logger.error("index data is None")
            return
        if len(task.indices) == 0:
            self.logger.info("there are no indices be created, just store the table")
            writer_engine.store_table(task.destination_schema, task.table, [], task.master_status)
            return
        table = task.table
        destination_schema = task.destination_schema
        schema = task.schema
        loading_schema = self.schema_loading[schema]["loading"]
        master_status = task.master_status
        try:
            if self.keep_existing_schema:
                table_pkey = writer_engine.get_existing_pkey(destination_schema, table)
                self.logger.info("Collecting constraints and indices from the destination table  %s.%s" % (
                    destination_schema, table))
                writer_engine.collect_idx_cons(destination_schema, table)
                self.logger.info("Removing constraints and indices from the destination table  %s.%s" % (
                    destination_schema, table))
                writer_engine.cleanup_idx_cons(destination_schema, table)
                writer_engine.truncate_table(destination_schema, table)
            else:
                table_pkey = writer_engine.create_indices(loading_schema, table, task.indices)
            writer_engine.store_table(destination_schema, table, table_pkey, master_status)
            if self.keep_existing_schema:
                self.logger.info(
                    "Adding constraint and indices to the destination table  %s.%s" % (destination_schema, table))
                writer_engine.create_idx_cons(destination_schema, table)
        except:
            self.logger.error("create index or constraint error")

    def print_progress (self, iteration, total, schema, table, rows):
        """
            Print the copy progress in slices and estimated total slices.
            In order to reduce noise when the log level is info only the tables copied in multiple slices
            get the print progress.

            :param iteration: The slice number currently processed
            :param total: The estimated total slices
            :param table_name: The table name
        """
        if iteration>=total:
            total = iteration
        if total>1:
            self.logger.info("Table %s.%s copied %s slice of %s" % (schema, table, iteration, total))
        else:
            self.logger.debug("Table %s.%s copied %s slice of %s" % (schema, table, iteration, total))
        if self.dump_json:
            self.__copied_progress_json("table",table,(iteration/total),rows)
    
    def __copied_progress_json (self,type_val,name,value,row=0):
        if(managerJson[name]["percent"] < value):
            if(process_state.is_precision_success(value)):
                status = process_state.ACCOMPLISH_STATUS
            elif(process_state.is_fail_status(value)):
                status = process_state.FAIL_STATUS
            else:
                status = process_state.PROCESSING_STATUS
            managerJson.update({name:{
                "name":name,
                "status":status,
                "percent":value,
                "count_rows":managerJson[name]["count_rows"]
            }})



    def __create_indices(self, schema, table, pg_engine, cursor_buffered):
        """
            The method copy the data between the origin and destination table.
            The method locks the table read only mode and  gets the log coordinates which are returned to the calling method.

            :param schema: the origin's schema
            :param table: the table name
            :return: the table and schema name with the primary key.
            :rtype: dictionary
        """
        loading_schema = self.schema_loading[schema]["loading"]
        self.logger.debug("Creating indices on table %s.%s " % (schema, table))
        index_data = self.__get_index_data(schema, table, cursor_buffered)
        table_pkey = pg_engine.create_indices(loading_schema, table, index_data)
        return table_pkey

    def __get_index_data(self, schema, table, cursor_buffered):
        sql_index = """
                    SELECT
                        index_name as index_name,
                        index_type as index_type,
                        non_unique as non_unique,
                        GROUP_CONCAT(column_name ORDER BY seq_in_index) as index_columns
                    FROM
                        information_schema.statistics
                    WHERE
                            table_schema=%s
                        AND 	table_name=%s
                        AND	(index_type = 'BTREE' OR index_type = 'FULLTEXT')
                    GROUP BY
                        table_name,
                        non_unique,
                        index_name,
                        index_type
                    ;
                """
        cursor_buffered.execute(sql_index, (schema, table))
        index_data = cursor_buffered.fetchall()
        return index_data

    def init_workers(self, count, target_func, pool, worker_prefix):
        for x in range(count):
            process = multiprocessing.Process(target=target_func)
            process.daemon = True
            process.name = worker_prefix + str(x + 1)
            pool.append(process)
            process.start()

    def __copy_tables(self):
        """
            The method copies the data between tables, from the mysql schema to the corresponding
            postgresql loading schema. Before the copy starts the table is locked and then the lock is released.
            If keep_existing_schema is true for the source then the tables are truncated before the copy,
            the indices are left in place and a REINDEX TABLE is executed after the copy.
        """
        self.delete_csv_file()
        readers = self.source_config["readers"] if self.source_config["readers"] > 0 else 1
        writers = self.source_config["writers"] if self.source_config["writers"] > 0 else 1
        size = int(int(self.copy_max_memory) / DEFAULT_MAX_SIZE_OF_CSV)
        self.write_task_queue = multiprocessing.Manager().Queue(size if size > MINIUM_QUEUE_SIZE else MINIUM_QUEUE_SIZE)
        self.read_task_queue = multiprocessing.Manager().Queue()
        self.index_waiting_queue = multiprocessing.Manager().Queue()
        self.table_metadata_queue = multiprocessing.Manager().Queue()
        self.column_metadata_queue = multiprocessing.Manager().Queue()
        writer_pool = []
        reader_pool = []

        self.init_workers(readers, self.data_reader, reader_pool, "reader-")
        self.init_workers(writers, self.data_writer, writer_pool, "writer-")

        for schema in self.schema_tables:
            loading_schema = self.schema_loading[schema]["loading"]
            destination_schema = self.schema_loading[schema]["destination"]
            table_list = self.schema_tables[schema]
            for table in table_list:
                self.connect_db_buffered()
                task = read_data_task(destination_schema=destination_schema, loading_schema=loading_schema,
                                      schema=schema, table=table)
                self.read_task_queue.put(task, block=True)

        self.read_task_queue.put(None, block=True)
        self.wait_for_finish(reader_pool)
        while not self.index_waiting_queue.empty():
            task = self.index_waiting_queue.get(block=True)
            self.write_task_queue.put(task, block=True)
        while not self.table_metadata_queue.empty():
            task = self.table_metadata_queue.get(block=True)
            self.write_task_queue.put(task, block=True)
        while not self.column_metadata_queue.empty():
            task = self.column_metadata_queue.get(block=True)
            self.write_task_queue.put(task, block=True)
        self.write_task_queue.put(None, block=True)
        self.wait_for_finish(writer_pool)
        writer_pool.clear()
        reader_pool.clear()

    def wait_for_finish(self, processes):
        for process in processes:
            if process.is_alive():
                process.join()

    def delete_csv_file(self):
        """
            Delete all the csv file in out_dir
        """
        csv_file_dir = self.out_dir + os.sep + CSV_FILE_SUB_DIR
        if os.path.exists(csv_file_dir):
            file_list = os.listdir(csv_file_dir)
            for file in file_list:
                if file.endswith(".csv"):
                    os.remove(csv_file_dir + os.sep + file)
        else:
            os.mkdir(csv_file_dir)

    def set_copy_max_memory(self):
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

    def __init_postgis_state(self):
        """
            The method check postgis state and update decode map
        """
        self.postgis_present = self.pg_engine.check_postgis()
        if self.postgis_present:
            self.hexify = self.hexify_always
            self.postgis_spatial_datatypes = self.postgis_spatial_datatypes.union(self.common_spatial_datatypes)
            for v in self.common_spatial_datatypes:
                # update common_spatial_datatypes value in map, decode them as geometry text
                self.decode_map[v] = self.__decode_postgis_spatial_value
        else:
            self.hexify = self.hexify_always.union(self.postgis_spatial_datatypes)
            for v in self.postgis_spatial_datatypes:
                # update postgis_spatial_datatypes value in map, decode them as hex
                self.decode_map[v] = self.__decode_hexify_value

    def init_read_replica(self):
        """
            The method calls the pre-steps required by the read replica method.

        """

        self.replica_conn = {}
        self.source_config = self.sources[self.source]
        try:
            exit_on_error = True if self.source_config["on_error_read"]=='exit' else False
        except KeyError:
            exit_on_error = True
        self.my_server_id = self.source_config["my_server_id"]
        self.limit_tables = self.source_config["limit_tables"]
        self.skip_tables = self.source_config["skip_tables"]
        self.replica_batch_size = self.source_config["replica_batch_size"]
        self.sleep_loop = self.source_config["sleep_loop"]
        self.__init_postgis_state()
        try:
            self.connect_db_buffered()
        except:
            if exit_on_error:
                raise
            else:
                return "skip"
        self.pg_engine.connect_db()
        self.schema_mappings = self.pg_engine.get_schema_mappings()
        self.schema_replica = [schema for schema in self.schema_mappings]
        db_conn = self.source_config["db_conn"]
        self.replica_conn["host"] = str(db_conn["host"])
        self.replica_conn["user"] = str(db_conn["user"])
        self.replica_conn["passwd"] = str(db_conn["password"])
        self.replica_conn["port"] = int(db_conn["port"])
        self.__build_table_exceptions()
        self.__build_skip_events()
        self.check_mysql_config()
        if self.gtid_mode:
            master_data = self.get_master_coordinates()
            self.start_xid = master_data[0]["Executed_Gtid_Set"].split(':')[1].split('-')[0]

    def __init_sync(self):
        """
            The method calls the common steps required to initialise the database connections and
            class attributes within sync_tables,refresh_schema and init_replica.
        """
        try:
            self.source_config = self.sources[self.source]
        except KeyError:
            self.logger.error("The source %s doesn't exists " % (self.source))
            sys.exit()
        self.out_dir = self.source_config["out_dir"]
        self.copy_mode = self.source_config["copy_mode"]
        self.pg_engine.lock_timeout = self.source_config["lock_timeout"]
        self.pg_engine.grant_select_to = self.source_config["grant_select_to"]
        if "keep_existing_schema" in self.sources[self.source]:
            self.keep_existing_schema = self.sources[self.source]["keep_existing_schema"]
        else:
            self.keep_existing_schema = False
        self.set_copy_max_memory()
        self.__init_postgis_state()
        self.connect_db_buffered()
        self.pg_engine.connect_db()
        self.schema_mappings = self.pg_engine.get_schema_mappings()
        self.pg_engine.schema_tables = self.schema_tables

    def refresh_schema(self):
        """
            The method performs a sync for an entire schema within a source.
            The method works in a similar way like init_replica.
            The swap happens in a single transaction.
        """
        self.logger.debug("starting sync schema for source %s" % self.source)
        self.logger.debug("The schema affected is %s" % self.schema)
        self.__init_sync()
        self.check_mysql_config()
        self.pg_engine.set_source_status("syncing")
        self.__build_table_exceptions()
        self.schema_list = [self.schema]
        self.get_table_list()
        self.create_destination_schemas()
        try:
            self.pg_engine.schema_loading = self.schema_loading
            self.pg_engine.schema_tables = self.schema_tables
            if self.keep_existing_schema:
                self.disconnect_db_buffered()
                self.__copy_tables()
            else:
                self.create_destination_tables()
                self.disconnect_db_buffered()
                self.__copy_tables()
                self.pg_engine.grant_select()
                self.pg_engine.swap_schemas()
                self.drop_loading_schemas()
            self.pg_engine.set_source_status("initialised")
            self.connect_db_buffered()
            master_end = self.get_master_coordinates()
            self.disconnect_db_buffered()
            self.pg_engine.set_source_highwatermark(master_end, consistent=False)
            self.pg_engine.cleanup_table_events()
            notifier_message = "refresh schema %s for source %s is complete" % (self.schema, self.source)
            self.notifier.send_message(notifier_message, 'info')
            self.logger.info(notifier_message)
        except:
            if not self.keep_existing_schema:
                self.drop_loading_schemas()
            self.pg_engine.set_source_status("error")
            notifier_message = "refresh schema %s for source %s failed" % (self.schema, self.source)
            self.notifier.send_message(notifier_message, 'critical')
            self.logger.critical(notifier_message)
            raise

    def sync_tables(self):
        """
            The method performs a sync for specific tables.
            The method works in a similar way like init_replica except when swapping the relations.
            The tables are loaded into a temporary schema and the log coordinates are stored with the table
            in the replica catalogue. When the load is complete the method drops the existing table and changes the
            schema for the loaded tables to the destination schema.
            The swap happens in a single transaction.
        """
        self.logger.info("Starting sync tables for source %s" % self.source)
        self.__init_sync()
        self.check_mysql_config()
        self.pg_engine.set_source_status("syncing")
        if self.tables == 'disabled':
            self.tables = self.pg_engine.get_tables_disabled ()
            if not self.tables:
                self.logger.info("There are no disabled tables to sync")
                return
        self.logger.debug("The tables affected are %s" % self.tables)
        self.__build_table_exceptions()
        self.schema_list = [schema for schema in self.schema_mappings if schema in self.schema_only]
        self.get_table_list()
        self.create_destination_schemas()
        try:
            self.pg_engine.schema_loading = self.schema_loading
            self.pg_engine.schema_tables = self.schema_tables
            if self.keep_existing_schema:
                self.disconnect_db_buffered()
                self.__copy_tables()
            else:
                self.create_destination_tables()
                self.disconnect_db_buffered()
                self.__copy_tables()
                self.pg_engine.grant_select()
                self.pg_engine.swap_tables()
                self.drop_loading_schemas()
            self.pg_engine.set_source_status("synced")
            self.connect_db_buffered()
            master_end = self.get_master_coordinates()
            self.disconnect_db_buffered()
            self.pg_engine.set_source_highwatermark(master_end, consistent=False)
            self.pg_engine.cleanup_table_events()
            notifier_message = "the sync for tables %s in source %s is complete" % (self.tables, self.source)
            self.notifier.send_message(notifier_message, 'info')
            self.logger.info(notifier_message)
        except:
            if not self.keep_existing_schema:
                self.drop_loading_schemas()
            self.pg_engine.set_source_status("error")
            notifier_message = "the sync for tables %s in source %s failed" % (self.tables, self.source)
            self.notifier.send_message(notifier_message, 'critical')
            self.logger.critical(notifier_message)
            raise

    @classmethod
    def __get_text_spatial(cls, raw_data):
        """
            The method returns the text representation converted in postgresql format
            for the raw data point using the ST_AsText function and the regular expressions

            :param charset: The table's character set
            :param raw_data: The raw_data returned by the mysql-replication library
            :return: text representation converted in postgresql format
            :rtype: text
        """
        decoded_data=binascii.hexlify(raw_data)
        return decoded_data.decode()[8:]

    def get_table_type_map(self):
        """
            The method builds a dictionary with a key per each schema replicated.
            Each key maps a dictionary with the schema's tables stored as keys and the column/type mappings.
            The dictionary is used in the read_replica method, to determine whether a field requires hexadecimal conversion.
        """
        table_type_map = {}
        table_map = {}
        self.logger.debug("collecting table type map")
        for schema in self.schema_replica:
            sql_tables = """
                SELECT
                    t.table_schema as table_schema,
                    t.table_name as table_name,
                    SUBSTRING_INDEX(t.TABLE_COLLATION,'_',1) as character_set
                FROM
                    information_schema.TABLES t
                WHERE
                        table_type='BASE TABLE'
                    AND	table_schema=%s
                ;
            """
            self.cursor_buffered.execute(sql_tables, (schema, ))
            table_list = self.cursor_buffered.fetchall()

            for table in table_list:
                column_type = {}
                numeric_scale = {}
                sql_columns = """
                    SELECT
                        column_name as column_name,
                        data_type as data_type,
                        numeric_scale as numeric_scale,
                        column_type as column_type
                    FROM
                        information_schema.COLUMNS
                    WHERE
                            table_schema=%s
                        AND table_name=%s
                    ORDER BY
                        ordinal_position
                    ;
                """
                table_charset = table["character_set"]
                self.cursor_buffered.execute(sql_columns, (table["table_schema"], table["table_name"]))
                column_data = self.cursor_buffered.fetchall()
                for column in column_data:
                    column_type[column["column_name"]] = column["data_type"]
                    if column["data_type"] == "set":
                        numeric_scale[column["column_name"]] = column["column_type"]
                    else:
                        numeric_scale[column["column_name"]] = column["numeric_scale"]
                table_dict = {}
                table_dict["table_charset"] = table_charset
                table_dict["column_type"] = column_type
                table_dict["numeric_scale"] = numeric_scale
                table_map[table["table_name"]] = table_dict
            table_type_map[schema] = table_map
            table_map = {}
        return table_type_map

    def store_binlog_event(self, table, schema):
        """
        The private method returns whether the table event should be stored or not in the postgresql log replica.

        :param table: The table's name to check
        :param schema: The table's schema name
        :return: true if the table should be replicated, false if shouldn't
        :rtype: boolean
        """
        """
        self.tables_disabled = self.pg_engine.get_tables_disabled(format='list')

        if self.tables_disabled:
            if  "%s.%s" % (schema, table) in self.tables_disabled:
                return False

        """
        if schema in self.skip_tables:
            if table in self.skip_tables[schema]:
                return False

        if schema in self.limit_tables:
            if len(self.limit_tables[schema]) == 0:
                return True
            elif table in self.limit_tables[schema]:
                return True
            else:
                return False

        return True

    def skip_event(self, table, schema, binlogevent):
        """
            The method returns true or false if whether the event should be skipped or not.
            The dictionary self.skip_events is used for the check.

            :param table: The table's name to check
            :param schema: The table's schema name
            :param binlogevent: The binlog event to evaluate
            :return: list with first element a boolean and the second element the event type
            :rtype: listt
        """
        if isinstance(binlogevent, DeleteRowsEvent):
            event = "delete"
        elif isinstance(binlogevent, UpdateRowsEvent):
            event = "update"
        elif isinstance(binlogevent, WriteRowsEvent):
            event = "insert"

        skip_event = False

        if self.skip_events:
            if self.skip_events[event]:
                table_name = "%s.%s" % (schema, table)
                if schema in self.skip_events[event] or table_name in self.skip_events[event]:
                    skip_event = True

        return [skip_event, event]

    def __build_gtid_set(self, gtid):
        """
            The method builds a gtid set using the current gtid and
        """
        new_set = None
        gtid_pack = []
        master_data= self.get_master_coordinates()
        if "Executed_Gtid_Set" in master_data[0]:
            gtid_set = master_data[0]["Executed_Gtid_Set"]
            gtid_list = gtid_set.split(",\n")
            for gtid_item in gtid_list:
                if gtid_item.split(':')[0] in gtid:
                    gtid_old = gtid_item.split(':')
                    gtid_new = "%s:%s-%s" % (gtid_old[0],gtid_old[1].split('-')[0],gtid[gtid_old[0]])
                    gtid_pack.append(gtid_new)
                else:
                    gtid_pack.append(gtid_item)
            new_set = ",\n".join(gtid_pack)
        return new_set

    @classmethod
    def __decode_dic_keys(cls, dic_encoded):
        """
        Private method to recursively decode the dictionary keys  and values into strings.
        This is used fixing the the json data types in the __read_replica_stream method because
        at moment the mysql-replication library returns the keys of the json data types as binary values in python3.

        :param dic_encoded: The dictionary with the encoded keys
        :return: The dictionary with the decoded keys
        :rtype: dictionary
        """
        dic_decoded = {}
        lst_decode = []
        if isinstance(dic_encoded, list):
            for item in dic_encoded:
                lst_decode.append(cls.__decode_dic_keys(item))
            return lst_decode
        elif not isinstance(dic_encoded, dict):
            try:
                return dic_encoded.decode("UTF-8")
            except AttributeError:
                return dic_encoded
        else:
            for key, value in dic_encoded.items():
                try:
                    dic_decoded[key.decode("UTF-8")] = cls.__decode_dic_keys(value)
                except AttributeError:
                    dic_decoded[key] = cls.__decode_dic_keys(value)
        return dic_decoded

    def decode_event_value(self, table_name, column_map, column_name, origin_value):
        """
            Decode event value based on column type
        """
        try:
            column_type=column_map["column_type"][column_name]
        except KeyError:
            self.logger.debug("Detected inconsistent structure for the table  %s. The replay may fail. " % (table_name))
            column_type = 'text'

        if not origin_value:
            if isinstance(origin_value, bytes):
                return ''
            else:
                return origin_value

        if "inf" in str(origin_value) and column_type in ["decimal", "double", "float"]:
            return 0

        return self.decode_map.get(column_type, self.__decode_default_value)(origin_value, column_map["numeric_scale"][column_name])

    def __read_replica_stream(self, batch_data, dispatcher_packet_queue, signal_queue):
        """
        Stream the replica using the batch data. This method evaluates the different events streamed from MySQL
        and manages them accordingly. The BinLogStreamReader function is called with the only_event parameter which
        restricts the event type received by the streamer.
        The events managed are the following.
        RotateEvent which happens whether mysql restarts or the binary log file changes.
        QueryEvent which happens when a new row image comes in (BEGIN statement) or a DDL is executed.
        The BEGIN is always skipped. The DDL is parsed using the sql_token class.
        [Write,Update,Delete]RowEvents are the row images pulled from the mysql replica.

        The RotateEvent and the QueryEvent cause the batch to be closed.

        The for loop reads the row events, builds the dictionary carrying informations like the destination schema,
        the 	binlog coordinates and store them into the group_insert list.
        When the number of events exceeds the replica_batch_size the group_insert is written into PostgreSQL.
        The batch is not closed in that case and the method exits only if there are no more rows available in the stream.
        Therefore the replica_batch_size is just the maximum size of the single insert and the size of replayed batch on PostgreSQL.
        The binlog switch or a captured DDL determines whether a batch is closed and processed.

        The update row event stores in a separate key event_before the row image before the update. This is required
        to allow updates where the primary key is updated as well.

        Each row event is scanned for data types requiring conversion to hex string.

        :param batch_data: The list with the master's batch data.
        :return: the batch's data composed by binlog name, binlog position and last event timestamp read from the mysql replica stream.
        :rtype: dictionary
        """
        size_insert=0
        sql_tokeniser = sql_token()
        inc_tables = self.pg_engine.get_inconsistent_tables()
        self.tables_disabled = self.pg_engine.get_tables_disabled(format='list')
        close_batch = False
        master_data = {}
        group_insert = []
        next_gtid = {}
        id_batch = batch_data[0][0]
        log_file = batch_data[0][1]
        log_position = batch_data[0][2]
        log_table = batch_data[0][3]
        master_data["log_table"] = log_table
        if self.gtid_mode:
            gtid_position = batch_data[0][4]
            gtid_pack = gtid_position.split(",\n")
            for gtid in gtid_pack:
                gtid  = gtid.split(':')
                next_gtid[gtid [0]]  = gtid [1].split("-")[-1]
                gtid_set = self.__build_gtid_set(next_gtid)
        else:
            gtid_set = None
        stream_connected = False

        modified_my_gtid_event()
        my_stream = BinlogTrxReader(
            connection_settings = self.replica_conn,
            server_id = self.my_server_id,
            log_file = log_file,
            log_pos = log_position,
            auto_position = gtid_set,
            only_schemas = self.schema_replica,
            slave_heartbeat = self.sleep_loop,
            packet_queue = dispatcher_packet_queue
        )
        for trx in my_stream:
            print("expect enter into fetch one")

        replica_position = signal_queue.get()
        return replica_position

    def read_replica(self, dispatcher_packet_queue, signal_queue):
        """
            The method gets the batch data from PostgreSQL.
            If the batch data is not empty then method read_replica_stream is executed to get the rows from
            the mysql replica stored into the PostgreSQL database.
            When the method exits the replica_data list is decomposed in the master_data (log name, position and last event's timestamp).
            If the flag close_batch is set then the master status is saved in PostgreSQL the batch id  returned by the method is
            is saved in the class variable id_batch.
            This variable is used to determine whether the old batch should be closed or not.
            If the variable is not empty then the previous batch gets closed with a simple update of the processed flag.

        """

        skip = self.init_read_replica()
        if skip:
            self.logger.warning("Couldn't connect to the source database for reading the replica. Ignoring.")
        else:
            self.pg_engine.set_source_status("running")
            replica_paused = self.pg_engine.get_replica_paused()
            if replica_paused:
                self.logger.info("Read replica is paused")
                self.pg_engine.set_read_paused(True)
            else:
                self.pg_engine.set_read_paused(False)
                batch_data = self.pg_engine.get_batch_data()
                if len(batch_data)>0:
                    id_batch=batch_data[0][0]
                    self.logger.debug("Batch data %s " % (batch_data))
                    replica_data=self.__read_replica_stream(batch_data, dispatcher_packet_queue, signal_queue)
                    master_data = replica_data.master_data
                    close_batch = replica_data.close_batch
                    if "gtid" in master_data:
                        master_data["Executed_Gtid_Set"] = self.__build_gtid_set(master_data["gtid"])
                    else:
                        master_data["Executed_Gtid_Set"] = ""
                    if close_batch:
                        self.master_status=[master_data]
                        self.logger.debug("trying to save the master data...")
                        next_id_batch=self.pg_engine.save_master_status(self.master_status)
                        if next_id_batch:
                            self.logger.debug("new batch created, saving id_batch %s in class variable" % (id_batch))
                            self.id_batch=id_batch
                        else:
                            self.logger.debug("batch not saved. using old id_batch %s" % (self.id_batch))
                        if self.id_batch:
                            self.logger.debug("updating processed flag for id_batch %s", (id_batch))
                            self.pg_engine.set_batch_processed(id_batch)
                            self.id_batch=None
                self.pg_engine.keep_existing_schema = self.keep_existing_schema
                self.pg_engine.check_source_consistent()


            self.disconnect_db_buffered()

    def init_replica(self):
        """
            The method performs a full init replica for the given source
        """
        self.logger.debug("starting init replica for source %s" % self.source)
        self.__init_sync()
        self.check_mysql_config()
        master_start = self.get_master_coordinates()
        self.pg_engine.set_source_status("initialising")
        self.pg_engine.clean_batch_data()
        self.pg_engine.save_master_status(master_start)        
        self.pg_engine.cleanup_source_tables()
        self.schema_list = [schema for schema in self.schema_mappings]
        self.__build_table_exceptions()
        self.get_table_list()
        self.create_destination_schemas()
        try:
            self.pg_engine.insert_source_timings()
            self.pg_engine.schema_loading = self.schema_loading
            if self.keep_existing_schema:
                self.disconnect_db_buffered()
                self.__copy_tables()
            else:
                self.create_destination_tables()
                self.disconnect_db_buffered()
                self.__copy_tables()
                self.pg_engine.grant_select()
                self.pg_engine.swap_schemas()
                self.drop_loading_schemas()
            self.pg_engine.set_source_status("initialised")
            self.connect_db_buffered()
            master_end = self.get_master_coordinates()
            self.disconnect_db_buffered()
            self.pg_engine.set_source_highwatermark(master_end, consistent=False)
            notifier_message = "init replica for source %s is complete" % self.source
            self.notifier.send_message(notifier_message, 'info')
            self.logger.info(notifier_message)

        except:
            if not self.keep_existing_schema:
                self.drop_loading_schemas()
            self.pg_engine.set_source_status("error")
            notifier_message = "init replica for source %s failed" % self.source
            self.logger.critical(notifier_message)
            self.notifier.send_message(notifier_message, 'critical')
            raise

    def start_database_object_replica(self, db_object_type):
        """
        The method start a database object's replication from mysql to destination with configuration.

        :param db_object_type: the database object type, refer to enumeration class DBObjectType
        """
        self.logger.info("Starting the %s replica for source %s." % (db_object_type.value, self.source))
        self.__init_sync()

        sql_to_get_object_metadata = db_object_type.sql_to_get_object_metadata()
        sql_to_get_create_object_statement = db_object_type.sql_to_get_create_object_statement()

        for schema in self.schema_mappings:
            self.logger.info("Starting the %s replica for schema %s." % (db_object_type.value, schema))
            success_num = 0  # number of replication success records
            failure_num = 0  # number of replication fail records

            # get metadata (object name) of all objects on schema
            if self.dump_json:
                self.cursor_buffered.execute(sql_to_get_object_metadata % (schema,))
                for object_metadata in self.cursor_buffered.fetchall():
                    managerJson.update({object_metadata["OBJECT_NAME"]:{
                        "name":object_metadata["OBJECT_NAME"],
                        "status":process_state.PENDING_STATUS,
                        "percent":process_state.PRECISION_START,
                        "count_rows":0
                    }})

            self.cursor_buffered.execute(sql_to_get_object_metadata % (schema,))
            for object_metadata in self.cursor_buffered.fetchall():
                object_name = object_metadata["OBJECT_NAME"]

                # set sql mode to ANSI_QUOTES, avoiding backticks in SQL statements
                self.cursor_buffered.execute("SET SQL_MODE = 'ANSI_QUOTES';")

                # get the details required to create the object
                self.cursor_buffered.execute(sql_to_get_create_object_statement % (schema, object_name))
                create_object_metadata = self.cursor_buffered.fetchone()
                create_object_statement = self.__get_create_object_statement(create_object_metadata, db_object_type)

                # translate sql dialect in mysql format to opengauss format.
                stdout, stderr = self.sql_translator.mysql_to_opengauss(create_object_statement, self.pg_engine.column_case_sensitive)
                tran_create_view_statement = self.__get_tran_create_view_statement(db_object_type, schema, stdout)
                has_error = self.__unified_log(stderr)
                if has_error:
                    # if translation has any error, this replication also fail
                    # insert a failure record into the object replication status table
                    self.pg_engine.insert_object_replicate_record(object_name, db_object_type, create_object_statement)
                    self.logger.error("Copying the source object fail %s : %s" %(db_object_type.value, object_name))
                    if self.dump_json:
                        self.__copied_progress_json(db_object_type.value,object_name,process_state.FAIL_STATUS)
                    failure_num += 1
                    continue
                # if translate successful, add the corresponding database object to opengauss
                try:
                    self.pg_engine.add_object(object_name, self.schema_mappings[schema], tran_create_view_statement)
                    self.pg_engine.insert_object_replicate_record(object_name, db_object_type, create_object_statement,
                                                                  tran_create_view_statement)
                    self.logger.info("Copying the source object success %s : %s" %(db_object_type.value, object_name))
                    if self.dump_json:
                        self.__copied_progress_json(db_object_type.value,object_name,process_state.PRECISION_SUCCESS)
                    success_num += 1
                except Exception as e:
                    self.pg_engine.insert_object_replicate_record(object_name, db_object_type, create_object_statement)
                    self.logger.error(e)
                    self.logger.error("Copying the source object fail %s : %s" %(db_object_type.value, object_name))
                    if self.dump_json:
                        self.__copied_progress_json(db_object_type.value,object_name,process_state.FAIL_STATUS)
                    failure_num += 1

            self.logger.info("Complete the %s replica for schema %s, total %d, success %d, fail %d." % (
                db_object_type.value, schema, success_num + failure_num, success_num, failure_num))

    def __get_tran_create_view_statement(self, db_object_type, schema, stdout):
        """
        Stdout should not be execute on opengauss directly, it needs to do some field replacement.
        :param db_object_type:
        :param schema:
        :param stdout:
        :return:
        """
        if db_object_type == DBObjectType.VIEW:
            tran_create_view_statement = stdout.replace("CREATE ", "CREATE OR REPLACE ")\
                .replace(schema + ".", self.schema_mappings[schema] + ".").replace("\"" + schema + "\".", "\"" + self.schema_mappings[schema] + "\".")
        elif db_object_type == DBObjectType.TRIGGER:
            tran_create_view_statement = stdout.replace(schema + ".", self.schema_mappings[schema] + ".").replace("\"" + schema + "\".", "\"" + self.schema_mappings[schema] + "\".")
        elif db_object_type == DBObjectType.PROC or db_object_type == DBObjectType.FUNC:
            # can not end with '/', so delete it.
            tran_create_view_statement = re.sub(r"/[\s]*$", "", stdout).replace(schema + ".", self.schema_mappings[schema] + ".").replace("\"" + schema + "\".", "\"" + self.schema_mappings[schema] + "\".")
        else:
            tran_create_view_statement = stdout
        return tran_create_view_statement

    def __unified_log(self, stderr):
        """
        embed og-translator's log records into chameleon's log system
        :param stderr:
        :return:
        """
        has_error = False  # a sign of whether the translation is successful
        # the log format on og-translator is: %d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{100} - %msg%n
        for log in stderr.splitlines():
            level_name = logging.getLevelName(log.split(' ')[3])
            if level_name == logging.ERROR or not isinstance(level_name, int):
                # when level_name value is ERROR
                # it means there is an error log record in the translation, maybe the sql statement cannot be translated
                # when level_name could not be got
                # it means there is a problem with the project og-translator itself
                has_error = True
                self.logger.error(log)
            else:
                self.logger.log(level_name, log)
        return has_error

    def __get_create_object_statement(self, create_object_metadata, db_object_type):
        """
        Get the sql required to create the object, which in mysql dialect format

        :param create_object_metadata:
        :param db_object_type:
        :return:
        """
        if db_object_type == DBObjectType.VIEW:
            create_object_statement = create_object_metadata["Create View"]
        elif db_object_type == DBObjectType.TRIGGER:
            create_object_statement = create_object_metadata["SQL Original Statement"] + ";" 
        elif db_object_type == DBObjectType.PROC:
            create_object_statement = create_object_metadata["Create Procedure"]
        elif db_object_type == DBObjectType.FUNC:
            create_object_statement = create_object_metadata["Create Function"]
        else:
            create_object_statement = ""
        # self.logger.debug("The statement of creating object is %s" % (create_object_statement,))
        create_object_statement = create_object_statement.replace("elseif", "else if")
        return create_object_statement
