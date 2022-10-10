import pickle
import time
import copy
import py_opengauss
import struct
import re
import os
import threading
import queue
import pymysql
from datetime import datetime

from pg_chameleon import sql_token
from pymysql.util import byte2int
from pymysqlreplication import event
from pymysqlreplication import BinLogStreamReader, constants
from pymysqlreplication.binlogstream import MYSQL_EXPECTED_ERROR_CODES
from pymysqlreplication.packet import BinLogPacketWrapper
from pymysqlreplication.event import QueryEvent, GtidEvent, XidEvent, RotateEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent, RowsEvent
from pg_chameleon.lib.pg_lib import pg_engine
from multiprocessing import Process, Array
from ctypes import Structure, c_char_p, c_int, c_char

NUM_TRX_PRINT = 10000

class MyBinLogStreamReader(BinLogStreamReader):
    """
        The class MyBinLogStreamReader extends BinLogStreamReader in pymysqlreplication for reading
        packets and converts them into events more efficiently.
    """
    def __int__(self, connection_settings, server_id, ctl_connection_settings=None, resume_stream=False,
                 blocking=False, only_events=None, log_file=None, log_pos=None,
                 filter_non_implemented_events=True,
                 ignored_events=None, auto_position=None,
                 only_tables=None, ignored_tables=None,
                 only_schemas=None, ignored_schemas=None,
                 freeze_schema=False, skip_to_timestamp=None,
                 report_slave=None, slave_uuid=None,
                 pymysql_wrapper=None,
                 fail_on_table_metadata_unavailable=False,
                 slave_heartbeat=None):
        """
            Class constructor, the parameters are same as the super class.
        """
        super(MyBinLogStreamReader, self).__int__(connection_settings, server_id, ctl_connection_settings=None, resume_stream=False,
                 blocking=False, only_events=None, log_file=None, log_pos=None,
                 filter_non_implemented_events=True,
                 ignored_events=None, auto_position=None,
                 only_tables=None, ignored_tables=None,
                 only_schemas=None, ignored_schemas=None,
                 freeze_schema=False, skip_to_timestamp=None,
                 report_slave=None, slave_uuid=None,
                 pymysql_wrapper=None,
                 fail_on_table_metadata_unavailable=False,
                 slave_heartbeat=None)

    def fetchone(self):
        """
            The method is overridden, in super class BinLogStreamReader, fetchone method consists of two steps:
            (1)obtaining the packet based on the binlog file; (2)converting each packet to an event. To improve the
            efficiency of the entire class, in subclass MyBinLogStreamReader, fetchone method only implements the first
            step, and it only obtains the packet based on the binlog file.
        """
        while True:
            if not self._BinLogStreamReader__connected_stream:
                self._BinLogStreamReader__connect_to_stream()
            if not self._BinLogStreamReader__connected_ctl:
                self._BinLogStreamReader__connect_to_ctl()

            try:
                pkt = self._stream_connection._read_packet()
            except pymysql.OperationalError as error:
                code, message = error.args
                if code in MYSQL_EXPECTED_ERROR_CODES:
                    self._stream_connection.close()
                    self._BinLogStreamReader__connected_stream = False
                    continue
                raise
            
            if pkt.is_eof_packet():
                self.close()
                return None

            if not pkt.is_ok_packet():
                continue
            
            return pkt.read_all()


class Packet:
    """
        The class Packet is used to wrap the packet based on the binlog file.
    """
    def __init__(self, timestamp, event_type, server_id, event_size, log_pos, flags, event_data):
        """
            Class constructor, wrap the packet according to its format.
        """
        self.timestamp = timestamp
        self.event_type = event_type
        self.server_id = server_id
        self.event_size = event_size
        self.log_pos = log_pos
        self.flags = flags
        self.event_data = event_data
        self.read_bytes = 0

    def read(self, size):
        """
            The method reads several bytes from the event_data field.
        """
        size = int(size)
        begin_read = self.read_bytes
        self.read_bytes += size
        return self.event_data[begin_read:self.read_bytes]


class MyBinLogPacketWrapper(BinLogPacketWrapper):
    """
        The class MyBinLogPacketWrapper extends BinLogPacketWrapper in pymysqlreplication for implements the second step
        of the BinLogStreamReader class to convert each packet to an event.
    """
    def __init__(self, from_packet, table_map,
                 ctl_connection,
                 use_checksum,
                 allowed_events,
                 only_tables,
                 ignored_tables,
                 only_schemas,
                 ignored_schemas,
                 freeze_schema,
                 fail_on_table_metadata_unavailable):
        """
            Class constructor, it converts each packet to an event.
        """
        self.read_bytes = 0
        self.data_buffer = b''

        self.packet = from_packet
        self.charset = ctl_connection.charset

        # Header
        self.timestamp = self.packet.timestamp
        self.event_type = self.packet.event_type
        self.server_id = self.packet.server_id
        self.event_size = self.packet.event_size
        self.log_pos = self.packet.log_pos
        self.flags = self.packet.flags

        event_size_without_header = self.event_size - 23

        self.data_buffer = copy.deepcopy(self.packet.event_data)

        self.event = None
        event_class = self._BinLogPacketWrapper__event_map.get(self.event_type, event.NotImplementedEvent)

        if event_class not in allowed_events:
            return

        self.event = event_class(self, event_size_without_header, table_map,
                                 ctl_connection,
                                 only_tables=only_tables,
                                 ignored_tables=ignored_tables,
                                 only_schemas=only_schemas,
                                 ignored_schemas=ignored_schemas,
                                 freeze_schema=freeze_schema,
                                 fail_on_table_metadata_unavailable=fail_on_table_metadata_unavailable)
        if self.event._processed == False:
            self.event = None

    def read(self, size):
        """
            The method is overridden, and it's used to read several bytes.
        """
        size = int(size)
        self.read_bytes += size
        if len(self.data_buffer) > 0:
            data = self.data_buffer[:size]
            self.data_buffer = self.data_buffer[size:]
            if len(data) == size:
                return data
            else:
                return data + self.packet.read(size - len(data))
        return self.packet.read(size)

    def unread(self, data):
        """
            Push again data in data buffer. It's use when you want to extract a bit from a value a let the rest of the
            code normally read the datas.
        """
        self.read_bytes -= len(data)
        self.data_buffer = data + self.data_buffer

    def advance(self, size):
        """
            The method is consistent with that in the super class.
        """
        size = int(size)
        self.read_bytes += size
        buffer_len = len(self.data_buffer)
        if buffer_len > 0:
            self.data_buffer = self.data_buffer[size:]
            if size > buffer_len:
                self.packet.advance(size - buffer_len)
        else:
            self.packet.advance(size)


class MyGtidEvent(GtidEvent):
    """
        The class MyGtidEvent extends GtidEvent in pymysqlreplication for obtaining the two parameters last_committed
        and sequence_number during initialization for parallel replication.
    """
    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        """
            Class constructor, the parameters are same as the super class.
        """
        super(GtidEvent, self).__init__(from_packet, event_size, table_map,
                                          ctl_connection, **kwargs)

        self.commit_flag = byte2int(self.packet.read(1)) == 1
        self.sid = self.packet.read(16)
        self.gno = struct.unpack('<Q', self.packet.read(8))[0]
        self.lt_type = byte2int(self.packet.read(1))

        self.last_committed = struct.unpack('<Q', self.packet.read(8))[0]
        self.sequence_number = struct.unpack('<Q', self.packet.read(8))[0]

    def _dump(self):
        """
            The method is used to dump the event information.
        """
        print("Commit: %s" % self.commit_flag)
        print("GTID_NEXT: %s" % self.gtid)
        if hasattr(self, "last_committed"):
            print("last_committed is %s" % self.last_committed)
            print("sequence number is %s" % self.sequence_number)


class Transaction:
    """
        The class Transaction.
    """
    def __init__(self, gtid:str=None, xid:str=None, binlog_file:str=None,
                 log_pos:int=None, last_committed:str=None, sequence_number:str=None, events:list=None):
        """
            Class constructor.
        """
        self.gtid = gtid
        self.xid = xid
        self.last_committed = last_committed
        self.sequence_number = sequence_number
        self.binlog_file = binlog_file
        self.log_pos = log_pos
        self.events = events
        self.finished = False
        self.sql_list = []
        self.is_dml = True

    def isvalid(self) -> bool:
        """
            The method is used to judge whether the transaction is valid.
        """
        return self.gtid and self.last_committed is not None and \
            self.sequence_number is not None and self.binlog_file

    def dump(self):
        """
            The method is used to dump the transaction information.
        """
        print("dump transaction")
        print("gtid: %s" % self.gtid)
        print("xid: %s" % self.xid)
        print("last_committed is %s" % self.last_committed)
        print("sequence_number is %s" % self.sequence_number)
        print("binlog_file is %s" % self.binlog_file)
        print("log_pos is %s" % self.log_pos)
        print("events is %s" % self.events)
        print("sql list is %s" % self.sql_list)
        print("is dml is %s" % self.is_dml)

    def interleaved(self, last_committed, sequence_number) -> bool:
        """
            The method is used to judge whether the current transaction can be executed in parallel with other
            transaction.
        """
        assert self.last_committed < self.sequence_number
        assert last_committed < sequence_number
        return sequence_number > self.last_committed

    def fetch_sql(self, mysql_source, column_map_list) -> list:
        """
            The method id used to fetch sql from the RowsEvent.
        """
        sql_list = []
        for i in range(len(self.events)):
            event = self.events[i]
            if isinstance(event, RowsEvent):
                table = "%s.%s" % (dqstr(event.schema), dqstr(event.table))
                if isinstance(event, DeleteRowsEvent):
                    for row in event.rows:
                        sql_list.append(sql_delete(table, self.decode_value(mysql_source, table, column_map_list[i], row['values'])))
                elif isinstance(event, UpdateRowsEvent):
                    for row in event.rows:
                        sql_list.append(sql_update(table, self.decode_value(mysql_source, table, column_map_list[i], row["before_values"]),
                                               self.decode_value(mysql_source, table, column_map_list[i], row["after_values"])))
                elif isinstance(event, WriteRowsEvent):
                    for row in event.rows:
                        sql_list.append(sql_insert(table, self.decode_value(mysql_source, table, column_map_list[i], row['values'])))
        return sql_list

    def decode_value(self, mysql_source, table_name, column_map, row_value):
        """
            The method is used to convert mysql values from binlog to openGauss values
        """
        json_type_list = []
        point_polygon_type_list = []
        for column_name in row_value:
            row_value[column_name] = mysql_source.decode_event_value(table_name, column_map, column_name, row_value[column_name])
            if column_map['column_type'][column_name] == "json":
                json_type_list.append(column_name)
            if column_map['column_type'][column_name] in ["point", "polygon", "geometry"]:
                point_polygon_type_list.append(column_name)
        for i in range(len(json_type_list)):
            column_name = json_type_list[i]
            new_column_name = column_name + "::jsonb"
            row_value[new_column_name] = row_value[column_name]
            del row_value[column_name]
        for i in range(len(point_polygon_type_list)):
            column_name = point_polygon_type_list[i]
            new_column_name = column_name + "~"
            row_value[new_column_name] = row_value[column_name]
            del row_value[column_name]
        return row_value

    def get_gtid(self):
        """
            The method is used to get related information of the transaction.
        """
        gtid = self.gtid.split(':')
        next_gtid = {}
        master_data = {}
        next_gtid[gtid[0]] = gtid[1]
        master_data["gtid"] = next_gtid
        master_data["File"] = self.binlog_file
        master_data["Position"] = self.log_pos
        return master_data


def qstr(obj) -> str:
    """
        The method formats the string with the single quote.
    """
    return "'{}'".format(str(obj).replace("'", "''"))


def dqstr(obj) -> str:
    """
        The method formats the string with the double quote.
    """
    return "\"{}\"".format(str(obj))


def sql_delete(table, row) -> str:
    """
        The method gets the sql delete statement.
    """
    sql = "delete from {} where ".format(table)
    ct = 0
    l = len(row.items())
    for k, v in row.items():
        ct += 1
        if v is None:
            sql += (dqstr(k) + " is NULL")
        else:
            if k.endswith("::jsonb"):
                column_name = k[0:len(k) - 7]
                sql += (dqstr(column_name) + "::jsonb" + '=' + qstr(v))
            elif k.endswith("~"):
                column_name = k[0:len(k) - 1]
                sql += (dqstr(column_name) + "~" + '=' + qstr(v))
            else:
                sql += (dqstr(k) + '=' + qstr(v))
        if ct != l:
            sql += ' and '
    return sql


def sql_update(table, before_row, after_row) -> str:
    """
        The method gets the sql update statement.
    """
    sql = 'update {} set '.format(table)
    ct = 0
    l = len(after_row.items())
    for k, v in after_row.items():
        ct += 1
        if v is None:
            sql += (dqstr(k) + '=' + 'NULL')
        else:
            if k.endswith("::jsonb"):
                k = k[0:len(k) - 7]
            if k.endswith("~"):
                k = k[0:len(k) - 1]
            sql += (dqstr(k) + '=' + qstr(v))
        if ct != l:
            sql += ','
    sql += ' where '
    ct = 0
    for k, v in before_row.items():
        ct += 1
        if v is None:
            sql += (dqstr(k) + " is NULL")
        else:
            if k.endswith("::jsonb"):
                column_name = k[0:len(k) - 7]
                sql += (dqstr(column_name) + "::jsonb" + '=' + qstr(v))
            elif k.endswith("~"):
                column_name = k[0:len(k) - 1]
                sql += (dqstr(column_name) + "~" + "=" + qstr(v))
            else:
                sql += (dqstr(k) + '=' + qstr(v))
        if ct != l:
            sql += ' and '
    return sql


def sql_insert(table, row) -> str:
    """
        The method gets the sql insert statement.
    """
    sql = 'insert into {}('.format(table)
    keys = row.keys()
    column_name = []
    for k in keys:
        if k.endswith("::jsonb"):
            column_name.append(dqstr(k[0:len(k) - 7]))
        elif k.endswith("~"):
            column_name.append(dqstr(k[0:len(k) - 1]))
        else:
            column_name.append(dqstr(k))
    sql += ','.join(column_name)
    sql += ') values('
    ct = 0
    l = len(keys)
    for k in keys:
        ct += 1
        if row[k] is None:
            sql += "NULL"
        else:
            sql += qstr(row[k])
        if ct != l:
            sql += ','
    sql += ')'
    return sql


class ConvertToEvent:
    """
        The class ConvertToEvent
    """
    @staticmethod
    def feed_event(trx, event, mysql_source, pg_engine) -> bool:
        """
            The method is used to extract transaction information according to each event.
        """
        if isinstance(event, RowsEvent):
            store_table = mysql_source.store_binlog_event(event.table, event.schema)
            skip_event = mysql_source.skip_event(event.table, event.schema, event)
            if store_table and not skip_event[0]:
                event.schema = mysql_source.schema_mappings[event.schema]
                trx.events.append(event)
            return False
        elif isinstance(event, QueryEvent):
            if "COMMIT" == event.query:
                return True
            elif is_ddl(event.query):
                sql_tokeniser = sql_token()
                sql_tokeniser.parse_sql(event.query)
                for token in sql_tokeniser.tokenised:
                    table_name = token["name"]
                    store_table = mysql_source.store_binlog_event(table_name, str(event.schema.decode('utf-8')))
                    if store_table:
                        schema = mysql_source.schema_mappings[event.schema.decode('utf-8')]
                        trx_sql_and_schema = event.query + " SCHEMA: " + schema
                        trx.sql_list.append(trx_sql_and_schema)
                        trx.is_dml = False
                    else:
                        trx.sql_list.append("")
                        trx.is_dml = False
                return True
            else:
                return False
        elif isinstance(event, XidEvent):
            return True


def is_ddl(sql: str) -> bool:
    """
        The method is used to judge whether the sql is ddl statement.
    """
    ddl_pattern = ['create table', 'drop table', 'create index', 'drop index',
     'truncate table', 'alter table', 'alter index', 'create database', 'drop database', 'create user', 'drop user', 'create unique index', 'rename']
    no_comment = re.sub('/\*.*?\*/', '', sql, flags=re.S)
    formatted = ' '.join(no_comment.lower().split())
    return any(formatted.startswith(x) for x in ddl_pattern)


def get_destination_ddl(event, mysql_source, pg_engine):
    destination_ddl = ""
    try:
        origin_schema = event.schema.decode()
    except:
        origin_schema = event.schema
    if event.query.strip().upper() not in mysql_source.statement_skip \
            and origin_schema in mysql_source.schema_mappings:
        destination_schema = mysql_source.schema_mappings[origin_schema]
        sql_tokeniser = sql_token()
        sql_tokeniser.parse_sql(event.query)
        for token in sql_tokeniser.tokenised:
            table_name = token["name"]
            store_query = mysql_source.store_binlog_event(table_name, origin_schema)
            if store_query:
                destination_ddl = pg_engine.generate_ddl(token, destination_schema)
    return destination_ddl


def modified_my_gtid_event():
    """
        The method is used to modify event map to match MyGtidEvent rather than previous GtidEvent.
    """
    for v in dir(BinLogPacketWrapper):
        if "event_map" in v:
            variable = v
            break
    tmp_map = getattr(BinLogPacketWrapper, variable)
    tmp_map[constants.GTID_LOG_EVENT] = MyGtidEvent


class BinlogTrxReader:
    """
        The class BinlogTrxReader is used to obtain tha packet from the binlog file.
    """
    def __init__(self, connection_settings,
                 server_id,
                 log_file,
                 log_pos,
                 auto_position,
                 only_schemas,
                 slave_heartbeat,
                 packet_queue):
        """
            Class constructor.
        """
        self.packet_queue = packet_queue
        self.event_list = []
        self.log_file = log_file
        self.event_stream = None
        self.packet_stream = MyBinLogStreamReader(
            connection_settings=connection_settings,
            server_id=server_id,
            only_events=[RotateEvent, DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, QueryEvent, MyGtidEvent, XidEvent],
            log_file=log_file,
            log_pos=log_pos,
            auto_position=auto_position,
            resume_stream=True,
            only_schemas=only_schemas,
            slave_heartbeat=slave_heartbeat,
        )

    def fetch_one_packet(self):
        """
            The method is used to get packet from the packet stream.
        """
        for packet in self.packet_stream:
            self.packet_queue.put(packet)
        self.packet_queue.put(None)
        self.packet_stream.close()

    def __iter__(self):
        """
            The iterator.
        """
        return iter(self.fetch_one_packet, None)


class Txn(Structure):
    """
        The class Txn is used to encapsulate transaction for data sharing in multi-process.
    """
    _fields_ = [('flag', c_int), ('txn_sql', c_char * 2000), ('last_committed', c_int), ('sequence_number', c_int)]


def process_work(pg_engine, arr, i):
    """
        The method is used to replay the sql statements in the work process.
    """
    conn = create_connection(pg_engine)
    id = i
    write_pid(pg_engine.pid_file, "execute_sql_process")
    while True:
        if arr[id].flag == -1:
            time.sleep(0.000006)
            continue
        else:
            try:
                sql = str(arr[id].txn_sql, encoding="utf-8")
                if is_ddl(sql):
                    destination_ddl = ""
                    schema = sql[sql.find(" SCHEMA: ")+len(" SCHEMA: "):]
                    sql = sql[:sql.find(" SCHEMA: ")]
                    sql_tokeniser = sql_token()
                    sql_tokeniser.parse_sql(sql)
                    for token in sql_tokeniser.tokenised:
                        destination_ddl = pg_engine.generate_ddl(token, schema)
                    conn.execute(destination_ddl)
                else:
                    conn.execute(sql)
                arr[id].flag = -1
            except Exception as exception:
                print(exception)
                arr[id].flag = -1


def create_connection(pg_engine):
    """
        The method is used to create a connection in the work process.
    """
    str_conn = "opengauss://%(host)s:%(port)s/%(database)s" % pg_engine.dest_conn
    conn = py_opengauss.open(str_conn, user=pg_engine.dest_conn["user"],
                             password=pg_engine.dest_conn["password"])
    conn.settings['client_encoding'] = pg_engine.dest_conn["charset"]
    conn.execute("set session_timeout = 0;")
    return conn


class TransactionDispatcher:
    """
        The class TransactionDispatcher is used to dispatch transaction for parallel replication.
    """
    def __init__(self, max_thread_count: int = 50, pg_engine: pg_engine = None):
        """
            Class constructor.
        """
        self.max_thread_count = max_thread_count
        self.pg_engine = pg_engine
        self.selected_trx = None
        self.thread_list = list()

    def print_result(self, result_queue):
        while True:
            result = result_queue.get(block=True)
            print("print result replay process have finished %s transaction, time is %s" % (
                result, datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]))

    def receive_packet(self, trx_queue_list, trx_out, index):
        for trx in trx_out.recv():
            trx_queue_list[0].put(trx)
        while True:
            current_index = index.get()
            for trx in trx_out.recv():
                trx_queue_list[current_index].put(trx)

    def dispatcher(self, trx_out):
        """
            The method is used to dispatch the current transaction to a free work thread.
        """
        self.pg_engine.logger.debug("start create thread")

        shared_variable = []
        for i in range(self.max_thread_count):
            initial_variable = (-1, b'sql statement', 0, 0)
            shared_variable.append(initial_variable)
        arr = Array(Txn, shared_variable)
        self.create_process(arr, self.pg_engine)
        self.pg_engine.logger.debug("finish create thread")
        i = 0
        result_queue = queue.Queue()
        threading.Thread(target=self.print_result, args=(result_queue,)).start()

        trx_queue_list = list()
        for num in range(3):
            trx_queue_list.append(queue.Queue())
        index = queue.Queue()
        threading.Thread(target=self.receive_packet, args=(trx_queue_list, trx_out, index, )).start()

        while True:
            for trx_queue_index in range(3):
                next_index = (trx_queue_index + 1) % 3
                index.put(next_index)
                kk = 0
                trx_num = trx_queue_list[trx_queue_index].qsize()
                while kk < trx_num:
                    trx = None
                    if self.selected_trx is None:
                        trx_load = trx_queue_list[trx_queue_index].get(block=True)
                        trx = pickle.loads(trx_load)
                        i = i + 1
                        if i % NUM_TRX_PRINT == 0:
                            result_queue.put(i)
                    else:
                        trx = self.selected_trx
                    if trx is None:
                        continue

                    result_list = self.can_parallel_and_find_free_thread(trx, arr)
                    if result_list[0] == "True":
                        free_thread_index = result_list[1]
                        arr[free_thread_index].txn_sql = bytes(";".join(trx.sql_list), encoding='utf-8')
                        arr[free_thread_index].last_committed = trx.last_committed
                        arr[free_thread_index].sequence_number = trx.sequence_number
                        arr[free_thread_index].flag = 0
                        self.selected_trx = None
                        kk = kk + 1
                    else:
                        time.sleep(0.00014)
                        self.selected_trx = trx

    def can_parallel_and_find_free_thread(self, trx:Transaction, arr):
        result_list = ["False", -1]
        for i in range(self.max_thread_count):
            txn_flag = arr[i].flag
            if txn_flag != -1:
                flag = trx.interleaved(arr[i].last_committed, arr[i].sequence_number)
                if not flag:
                    return result_list
            else:
                result_list[1] = i
        if result_list[1] != -1:
            result_list[0] = "True"
        return result_list

    def create_process(self, arr, pg_engine):
        for i in range(self.max_thread_count):
            process = Process(target=process_work, args=(pg_engine, arr, i, ))
            self.thread_list.append(process)
            process.start()


class ReplicaPosition:
    """
        The class ReplicaPosition is used to record replica progress.
    """
    def __init__(self, master_data={}, close_batch=False):
        """
            Class constructor.
        """
        self.master_data = master_data
        self.close_batch = close_batch

    def reset_data(self):
        """
            Reset data.
        """
        self.master_data = {}
        self.close_batch = False

    def copy_data(self, replica_position):
        """
            Copy data.
        """
        self.master_data = replica_position.master_data
        self.close_batch = replica_position.close_batch

    def set_data(self, master_data, close_batch):
        """
            Set data.
        """
        self.master_data = master_data
        self.close_batch = close_batch

    def new_object(self):
        """
            New a object
        """
        replica_position_object = ReplicaPosition(self.master_data, self.close_batch)
        return replica_position_object


def write_pid(pid_file, pid_name):
    file_name = os.path.expanduser(pid_file)
    file = open(file_name, "a")
    file.write("%s:%s%s" % (pid_name, os.getpid(), os.linesep))
    file.close()
