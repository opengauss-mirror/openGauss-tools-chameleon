import logging
import multiprocessing as mp
import os
import os.path
import pickle
import pprint
import signal
import sys
import json
import time
from logging.handlers import TimedRotatingFileHandler
from shutil import copy
import rollbar
import yaml
from daemonize import Daemonize
from tabulate import tabulate
from pg_chameleon import pg_engine, mysql_source, pgsql_source, DBObjectType
import traceback
import struct
import queue
import threading
from datetime import datetime
from pymysql.util import byte2int
from pymysqlreplication.event import QueryEvent, GtidEvent, XidEvent, RotateEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent, RowsEvent
from pg_chameleon.lib.parallel_replication import TransactionDispatcher
from pg_chameleon.lib.parallel_replication import MyBinLogStreamReader
from pg_chameleon.lib.parallel_replication import MyBinLogPacketWrapper
from pg_chameleon.lib.parallel_replication import ConvertToEvent
from pg_chameleon.lib.parallel_replication import MyGtidEvent, modified_my_gtid_event
from pg_chameleon.lib.parallel_replication import Transaction
from pg_chameleon.lib.parallel_replication import Packet, ReplicaPosition
from pg_chameleon.lib.parallel_replication import write_pid

ROTATE_EVENT = 0x04
TABLE_MAP_EVENT = 0x13
FORMAT_DESCRIPTION_EVENT = 0x0f
GTID_EVENT = 0x10

NUM_PACKET = 1000
NUM_TRX = 500

MYSQL_PACKET_HAED_LENGTH = 20
INDEX_PARALLEL_WORKERS = 2


class rollbar_notifier(object):
    """
        This class is used to send messages to rollbar whether the key and environment variables are set
    """

    def __init__(self, rollbar_key, rollbar_env, rollbar_level, logger):
        """
            Class constructor.
        """
        self.levels = {
            "critical": 1,
            "error": 2,
            "warning": 3,
            "info": 5
        }
        self.rollbar_level = self.levels[rollbar_level]
        self.logger = logger
        self.notifier = rollbar
        if rollbar_key != '' and rollbar_env != '':
            self.notifier.init(rollbar_key, rollbar_env)
        else:
            self.notifier = None

    def send_message(self, message, level):
        """
            The method sends a message to rollbar. If it fails it just logs an error
            without causing the process to crash.
        """
        if self.notifier:
            exc_info = sys.exc_info()
            try:
                notification_level = self.levels[level]
                if notification_level <= self.rollbar_level:
                    try:
                        self.notifier.report_message(message, level)
                        if exc_info[0]:
                            self.notifier.report_exc_info(exc_info)
                    except:
                        self.logger.error("Could not send the message to rollbar.")
            except:
                self.logger.error("Wrong rollbar level specified.")


class replica_engine(object):
    """
        This class is wraps the the mysql and postgresql engines in order to perform the various activities required for the replica.
        The constructor inits the global configuration class  and setup the mysql and postgresql engines as class objects.
        The class sets the logging using the configuration parameter.

    """

    def __init__(self, args):
        """
            Class constructor.
        """
        if os.geteuid() == 0:
            print("pg_chameleon cannot be run as root")
            sys.exit(10)

        self.catalog_version = '2.0.7'
        self.upgradable_version = '1.7'
        self.lst_yes = ['yes', 'Yes', 'y', 'Y']
        self.args = args
        self.source = self.args.source
        self.initial_global_setting()
        if self.args.command == 'set_configuration_files':
            self.set_configuration_files()
            sys.exit()

        self.load_config()
        self.update_pid_file()

        self.is_debug_or_dump_json = self.args.debug or self.config['dump_json']
        log_list = self.__init_logger("global")
        self.logger = log_list[0]
        self.logger_fds = log_list[1]

        # notifier configuration
        self.notifier = rollbar_notifier(self.config["rollbar_key"], self.config["rollbar_env"],
                                         self.args.rollbar_level, self.logger)

        self.initialize_pg_engine()
        self.initialize_mysql_source()
        self.check_compress_param()
        self.initialize_pgsql_source()

        self.check_catalog_version()

        if self.args.source != '*' and self.args.command != 'add_source':
            self.pg_engine.connect_db()
            source_count = self.pg_engine.check_source()
            self.pg_engine.disconnect_db()
            if source_count == 0:
                print("FATAL, The source %s is not registered. Please add it add_source" % (self.args.source))
                sys.exit()

    def update_pid_file(self):
        cham_dir = "%s/.pg_chameleon" % os.path.expanduser('~')
        local_pid = "%s/pid/" % cham_dir
        if len(self.config["pid_dir"]) == 0:
            local_pid = "%s/pid/" % cham_dir
            self.config["pid_dir"] = local_pid
        else:
            local_pid = os.path.expanduser(self.config["pid_dir"])
        pid_file_name = local_pid + "replica.pid"
        self.pid_file = pid_file_name
        if not os.path.isdir(local_pid):
            print("creating directory %s" % local_pid)
            os.makedirs(local_pid)

    def initial_global_setting(self):
        python_lib = os.path.dirname(os.path.realpath(__file__))
        cham_dir = "%s/.pg_chameleon" % os.path.expanduser('~')
        local_conf = "%s/configuration/" % cham_dir
        self.global_conf_example = '%s/../configuration/config-example.yml' % python_lib
        self.local_conf_example = '%s/config-example.yml' % local_conf
        local_logs = "%s/logs/" % cham_dir
        local_pid = "%s/pid/" % cham_dir
        pid_file_name = local_pid + "replica.pid"
        self.pid_file = pid_file_name
        self.conf_dirs = [
            cham_dir,
            local_conf,
            local_logs,
            local_pid,

        ]
        self.__set_conf_permissions(cham_dir)

    def check_catalog_version(self):
        catalog_version = self.pg_engine.get_catalog_version()
        # safety checks
        if self.args.command == 'upgrade_replica_schema':
            self.pg_engine.sources = self.config["sources"]
            print("WARNING, entering upgrade mode. Disabling the catalogue version's check. Expected version %s,"
                  " installed version %s" % (self.catalog_version, catalog_version))
        elif self.args.command == 'enable_replica' and self.catalog_version != catalog_version:
            print("WARNING, catalogue mismatch. Expected version %s, installed version %s" %
                  (self.catalog_version, catalog_version))
        else:
            if catalog_version:
                if self.catalog_version != catalog_version:
                    print("FATAL, replica catalogue version mismatch. Expected %s, got %s" % (
                        self.catalog_version, catalog_version))
                    sys.exit()

    def initialize_pgsql_source(self):
        # pgsql_source instance initialisation
        self.pgsql_source = pgsql_source()
        self.pgsql_source.source = self.args.source
        self.pgsql_source.tables = self.args.tables
        self.pgsql_source.schema = self.args.schema.strip()
        self.pgsql_source.pg_engine = self.pg_engine
        self.pgsql_source.logger = self.logger
        self.pgsql_source.sources = self.config["sources"]
        self.pgsql_source.type_override = self.config["type_override"]
        self.pgsql_source.notifier = self.notifier

    def initialize_mysql_source(self):
        # mysql_source instance initialisation
        self.mysql_source = mysql_source()
        if self.config['dump_json']:
            self.mysql_source.initJson()
        try:
            self.mysql_source.dump_json = self.config["dump_json"]
        except KeyError:
            self.mysql_source.dump_json = False
        self.mysql_source.source = self.args.source
        self.mysql_source.tables = self.args.tables
        self.mysql_source.schema = self.args.schema.strip()
        self.mysql_source.pg_engine = self.pg_engine
        self.mysql_source.logger = self.logger
        self.mysql_source.sources = self.config["sources"]
        self.mysql_source.type_override = self.config["type_override"]
        self.mysql_source.notifier = self.notifier

        self.__get_and_check_bool_param("is_create_index", True)
        self.__get_and_check_bool_param("mysql_restart_config", True)
        self.__get_and_check_bool_param("is_skip_completed_tables", False)
        self.__get_and_check_bool_param("with_datacheck", False)

    def initialize_pg_engine(self):
        # pg_engine instance initialisation
        self.pg_engine = pg_engine()
        self.pg_engine.pid_file = self.pid_file
        self.pg_engine.dest_conn = self.config["pg_conn"]
        self.pg_engine.logger = self.logger
        self.pg_engine.source = self.args.source
        self.pg_engine.full = self.args.full
        self.pg_engine.type_override = self.config["type_override"]
        self.pg_engine.sources = self.config["sources"]
        self.pg_engine.notifier = self.notifier
        self.pg_engine.get_keywords()
        try:
            self.pg_engine.migrate_default_value = self.config["sources"][self.source]["migrate_default_value"]
        except KeyError:
            self.pg_engine.migrate_default_value = True
        try:
            index_parallel_workers = self.config["sources"][self.source]["index_parallel_workers"]
        except KeyError:
            index_parallel_workers = INDEX_PARALLEL_WORKERS
        if not self.__check_parallel_workers(index_parallel_workers):
            self.logger.warning("WARNING, the param index_parallel_workers is in [0, 32], but the current setting is "
                                "%s, so we use the default value %s" % (index_parallel_workers, INDEX_PARALLEL_WORKERS))
            index_parallel_workers = INDEX_PARALLEL_WORKERS
        self.pg_engine.index_parallel_workers = index_parallel_workers

    def check_compress_param(self):
        try:
            enable_compress = self.config["sources"][self.source]["enable_compress"]
        except KeyError:
            enable_compress = False
        if enable_compress:
            self.pg_engine.enable_compress = True
            self.pg_engine.compress_param_map = {}
            self.mysql_source.enable_compress = True
            self.__get_compresstype_param()
            self.__get_compress_level_param()
            self.__get_compress_chunk_size()
            self.__get_compress_prealloc_chunks()
            self.__get_compress_byte_convert()
            self.__get_compress_diff_convert()

    def __get_compresstype_param(self):
        try:
            compresstype = self.config["compress_properties"]["compresstype"]
        except KeyError:
            compresstype = 0
        if not type(compresstype) == int and 0 <= compresstype <= 2:
            self.logger.warning(
                "WARNING, the param compresstype is in [0, 2], but the current setting is "
                "%s, so we use the default value %s" % (compresstype, 0))
            compresstype = 0
        if compresstype != 0:
            self.pg_engine.compress_param_map["compresstype"] = compresstype

    def __get_compress_level_param(self):
        try:
            compress_level = self.config["compress_properties"]["compress_level"]
        except KeyError:
            compress_level = 0
        if not type(compress_level) == int and -31 <= compress_level <= 31:
            self.logger.warning(
                "WARNING, the param compress_level is in [0, 2], but the current setting is "
                "%s, so we use the default value %s" % (compress_level, 0))
            compress_level = 0
        if compress_level != 0:
            self.pg_engine.compress_param_map["compress_level"] = compress_level

    def __get_compress_chunk_size(self):
        try:
            compress_chunk_size = self.config["compress_properties"]["compress_chunk_size"]
        except KeyError:
            compress_chunk_size = 4096
        if not type(compress_chunk_size) == int and compress_chunk_size in [512, 1024, 2048, 4096]:
            self.logger.warning(
                "WARNING, the param compress_chunk_size is in list [512, 1024, 2048, 4096] for default 8k page, "
                "but the current setting is %s, so we use the default value %s" % (compress_chunk_size, 4096))
            compress_chunk_size = 4096
        if compress_chunk_size != 4096:
            self.pg_engine.compress_param_map["compress_chunk_size"] = compress_chunk_size

    def __get_compress_prealloc_chunks(self):
        try:
            compress_prealloc_chunks = self.config["compress_properties"]["compress_prealloc_chunks"]
        except KeyError:
            compress_prealloc_chunks = 0
        if not type(compress_prealloc_chunks) == int and 0 <= compress_prealloc_chunks <= 7:
            self.logger.warning(
                "WARNING, the param compress_prealloc_chunks is [0, 7], but the current setting is "
                "%s, so we use the default value %s" % (compress_prealloc_chunks, 0))
            compress_prealloc_chunks = 0
        if compress_prealloc_chunks != 0:
            try:
                compress_chunk_size = self.pg_engine.compress_param_map["compress_chunk_size"]
            except KeyError:
                compress_chunk_size = 4096
            if compress_chunk_size == 2048 and compress_prealloc_chunks > 3:
                self.logger.warning("WARNING, the param compress_prealloc_chunks max value is 3 when the param "
                                    "compress_chunk_size is 2048, but the the current setting is %s, so we use "
                                    "the default value %s" % (compress_chunk_size, 0))
            elif compress_chunk_size == 4096 and compress_prealloc_chunks > 1:
                self.logger.warning("WARNING, the param compress_prealloc_chunks max value is 1 when the param "
                                    "compress_chunk_size is 4096, but the the current setting is %s, so we use "
                                    "the default value %s" % (compress_chunk_size, 0))
            else:
                self.pg_engine.compress_param_map["compress_chunk_size"] = compress_chunk_size

    def __get_compress_byte_convert(self):
        try:
            compress_byte_convert = self.config["compress_properties"]["compress_byte_convert"]
        except KeyError:
            compress_byte_convert = False
        if type(compress_byte_convert) == bool and compress_byte_convert:
            self.pg_engine.compress_param_map["compress_byte_convert"] = compress_byte_convert

    def __get_compress_diff_convert(self):
        try:
            compress_diff_convert = self.config["compress_properties"]["compress_diff_convert"]
        except KeyError:
            compress_diff_convert = False
        if type(compress_diff_convert) == bool and compress_diff_convert:
            self.pg_engine.compress_param_map["compress_diff_convert"] = compress_diff_convert

    def write_Json(self):
        dump_jsons = {"total": [], "table": [], "view": [], "function": [], "trigger": [], "procedure": []}
        dump_object = self.mysql_source.getmanagerJson().copy()
        dump_object_name = ''
        if self.args.command == 'init_replica':
            dump_object_name = 'table'
        elif self.args.command == 'start_view_replica':
            dump_object_name = 'view'
        elif self.args.command == 'start_trigger_replica':
            dump_object_name = 'trigger'
        elif self.args.command == 'start_func_replica':
            dump_object_name = 'function'
        elif self.args.command == 'start_proc_replica':
            dump_object_name = 'procedure'

        for key in dump_object:
            if key == "total":
                dump_jsons["total"] = dump_object[key]
            else:
            	dump_jsons[dump_object_name].append(dump_object[key])
        with open('data_'+self.args.config+'_'+self.args.command+'.json', 'w', encoding='utf8') as f:
            f.seek(0)
            json.dump(dump_jsons,f)

    def dump_Json(self):
        while True:
            self.write_Json()
            time.sleep(2)

    def __check_param_valid(self, param):
        """
            The method is used to check whether the param is valid.
        """
        if type(param) == bool:
            return True
        elif str(param).lower() == "yes" or str(param).lower() == "no":
            return True
        return False

    def __get_and_check_bool_param(self, key, default_value):
        """
            The method is used get config value of key and check whether the param is valid.
        """
        try:
            value = self.config["sources"][self.source][key]
        except KeyError:
            value = default_value
        if self.__check_param_valid(value):
            if key == "is_create_index":
                self.mysql_source.is_create_index = value
            if key == "mysql_restart_config":
                self.mysql_source.mysql_restart_config = value
            if key == "is_skip_completed_tables":
                self.mysql_source.is_skip_completed_tables = value
            if key == "with_datacheck":
                self.mysql_source.with_datacheck = value
        else:
            self.logger.error("FATAL, the parameter " + key + " setting is improper, it should be set to "
                              "Yes or No, but current setting is %s" % value)
            sys.exit()

    def __check_parallel_workers(self, param):
        return type(param) == int and 0 <= param <= 32

    def terminate_replica(self, signal, frame):
        """
            Stops gracefully the replica.
        """
        self.logger.info("Caught stop replica signal terminating daemons and ending the replica process.")
        self.read_daemon.terminate()
        self.replay_daemon.terminate()
        self.pg_engine.connect_db()
        self.pg_engine.set_source_status("stopped")
        sys.exit(0)

    def set_configuration_files(self):
        """
            The method loops the list self.conf_dirs creating them only if they are missing.

            The method checks the freshness of the config-example.yaml and connection-example.yml
            copies the new version from the python library determined in the class constructor with get_python_lib().

            If the configuration file is missing the method copies the file with a different message.

        """

        for confdir in self.conf_dirs:
            if not os.path.isdir(confdir):
                print("creating directory %s" % confdir)
                os.mkdir(confdir)

        if os.path.isfile(self.local_conf_example):
            print("updating configuration example with %s" % self.local_conf_example)
        else:
            print("copying configuration  example in %s" % self.local_conf_example)
        copy(self.global_conf_example, self.local_conf_example)

    def load_config(self):
        """
            The method loads the configuration from the file specified in the args.config parameter.
        """
        local_confdir = "%s/.pg_chameleon/configuration/" % os.path.expanduser('~')
        self.config_file = '%s/%s.yml' % (local_confdir, self.args.config)
        if not os.path.isfile(self.config_file):
            print("**FATAL - configuration file missing. Please ensure the file %s is present." % (self.config_file))
            sys.exit()

        config_file = open(self.config_file, 'r')
        self.config = yaml.load(config_file.read(), Loader=yaml.FullLoader)
        config_file.close()

    def show_sources(self):
        """
            The method shows the sources available in the configuration file.
        """
        for item in self.config["sources"]:
            print("\n")
            print(tabulate([], headers=["Source %s" % item]))
            tab_headers = ['Parameter', 'Value']
            tab_body = []
            source = self.config["sources"][item]
            config_list = [param for param in source if param not in ['db_conn']]
            connection_list = [param for param in source["db_conn"] if param not in ['password']]
            for parameter in config_list:
                tab_row = [parameter, source[parameter]]
                tab_body.append(tab_row)
            for param in connection_list:
                tab_row = [param, source["db_conn"][param]]
                tab_body.append(tab_row)

            print(tabulate(tab_body, headers=tab_headers))

    def show_config(self):
        """
            The method loads the current configuration and displays the status in tabular output
        """
        config_list = [item for item in self.config if item not in ['pg_conn', 'sources', 'type_override']]
        connection_list = [item for item in self.config["pg_conn"] if item not in ['password']]
        type_override = pprint.pformat(self.config['type_override'], width=20)
        tab_body = []
        tab_headers = ['Parameter', 'Value']
        for item in config_list:
            tab_row = [item, self.config[item]]
            tab_body.append(tab_row)
        for item in connection_list:
            tab_row = [item, self.config["pg_conn"][item]]
            tab_body.append(tab_row)
        tab_row = ['type_override', type_override]
        tab_body.append(tab_row)
        print(tabulate(tab_body, headers=tab_headers))
        self.show_sources()

    def create_replica_schema(self):
        """
            The method creates the replica schema in the destination database.
        """
        self.logger.info("Trying to create replica schema")
        self.pg_engine.create_replica_schema()

    def drop_replica_schema(self):
        """
            The method removes the replica schema from the destination database.
        """
        self.logger.info("Dropping the replica schema")
        self.pg_engine.drop_replica_schema()

    def add_source(self):
        """
            The method adds a new replication source. A pre existence check is performed
        """
        if self.args.source == "*":
            print("You must specify a source name with the argument --source")
        else:
            self.logger.info("Trying to add a new source")
            self.pg_engine.add_source()

    def drop_source(self):
        """
            The method removes a replication source from the catalogue.
        """
        if self.args.source == "*":
            print("You must specify a source name with the argument --source")
        else:
            drp_msg = 'Dropping the source %s will remove drop any replica reference.\n Are you sure? YES/No\n' % self.args.source
            drop_src = input(drp_msg)
            if drop_src == 'YES':
                self.logger.info("Trying to remove the source")
                self.pg_engine.drop_source()
            elif drop_src in self.lst_yes:
                print('Please type YES all uppercase to confirm')

    def enable_replica(self):
        """
            The method  resets the source status to stopped and disables any leftover maintenance mode
        """
        self.pg_engine.connect_db()
        self.pg_engine.set_source_status("stopped")
        self.pg_engine.end_maintenance()

    def init_replica(self):
        """
            The method  initialise a replica for a given source and configuration.
            It is compulsory to specify a source name when running this method.
            The method checks the source type and calls the corresponding initialisation's method.

        """
        if os.path.exists(self.pid_file):
            os.remove(self.pid_file)

        if self.args.source == "*":
            print("You must specify a source name with the argument --source")
        elif self.args.tables != "*":
            print("You cannot specify a table name when running init_replica.")
        else:
            try:
                source_type = self.config["sources"][self.args.source]["type"]
            except KeyError:
                print("The source %s doesn't exists." % (self.args.source))
                sys.exit()
            self.__stop_replica()
            if source_type == "mysql":
                self.__init_mysql_replica()
            elif source_type == "pgsql":
                self.__init_pgsql_replica()

    def __init_mysql_replica(self):
        """
            The method  initialise a replica for a given mysql source within the specified configuration.
            The method is called by the public method init_replica.
        """
        if self.is_debug_or_dump_json:
            self.mysql_source.init_replica()
        else:
            if self.config["log_dest"] == 'stdout':
                foreground = True
            else:
                foreground = False
                print("Init replica process for source %s started." % (self.args.source))
            keep_fds = [self.logger_fds]
            init_pid = os.path.expanduser('%s/%s.pid' % (self.config["pid_dir"], self.args.source))
            self.logger.info("Initialising the replica for source %s" % self.args.source)
            init_daemon = Daemonize(app="init_replica", pid=init_pid, action=self.mysql_source.init_replica,
                                    foreground=foreground, keep_fds=keep_fds)
            init_daemon.start()

    def __init_pgsql_replica(self):
        """
            The method  initialise a replica for a given postgresql source within the specified configuration.
            The method is called by the public method init_replica.
        """

        if self.is_debug_or_dump_json:
            self.pgsql_source.init_replica()
        else:
            if self.config["log_dest"] == 'stdout':
                foreground = True
            else:
                foreground = False
                print("Init replica process for source %s started." % (self.args.source))
            keep_fds = [self.logger_fds]
            init_pid = os.path.expanduser('%s/%s.pid' % (self.config["pid_dir"], self.args.source))
            self.logger.info("Initialising the replica for source %s" % self.args.source)
            init_daemon = Daemonize(app="init_replica", pid=init_pid, action=self.pgsql_source.init_replica,
                                    foreground=foreground, keep_fds=keep_fds)
            init_daemon.start()

    def refresh_schema(self):
        """
            The method  reload the data from a source and only for a specified schema.
            Is compulsory to specify a source name and an origin's schema name.
            The schema mappings are honoured by the procedure automatically.
        """
        if self.args.source == "*":
            print("You must specify a source name using the argument --source")
        elif self.args.schema == "*":
            print("You must specify an origin's schema name using the argument --schema")
        else:
            self.__stop_replica()
            if self.is_debug_or_dump_json:
                self.mysql_source.refresh_schema()
            else:
                if self.config["log_dest"] == 'stdout':
                    foreground = True
                else:
                    foreground = False
                    print("Sync tables process for source %s started." % (self.args.source))
                keep_fds = [self.logger_fds]
                init_pid = os.path.expanduser('%s/%s.pid' % (self.config["pid_dir"], self.args.source))
                self.logger.info(
                    "The tables %s within source %s will be synced." % (self.args.tables, self.args.source))
                sync_daemon = Daemonize(app="sync_tables", pid=init_pid, action=self.mysql_source.refresh_schema,
                                        foreground=foreground, keep_fds=keep_fds)
                sync_daemon.start()

    def sync_tables(self):
        """
            The method  reload the data from a source only for specified tables.
            Is compulsory to specify a source name and at least one table name when running this method.
            Multiple tables are allowed if comma separated.
        """
        if self.args.source == "*":
            print("You must specify a source name using the argument --source")
        elif self.args.tables == "*":
            print(
                "You must specify one or more tables, in the form schema.table, separated by comma using the argument --tables")
        else:
            self.__stop_replica()
            if self.is_debug_or_dump_json:
                self.mysql_source.sync_tables()
            else:
                if self.config["log_dest"] == 'stdout':
                    foreground = True
                else:
                    foreground = False
                    print("Sync tables process for source %s started." % (self.args.source))
                keep_fds = [self.logger_fds]
                init_pid = os.path.expanduser('%s/%s.pid' % (self.config["pid_dir"], self.args.source))
                self.logger.info(
                    "The tables %s within source %s will be synced." % (self.args.tables, self.args.source))
                sync_daemon = Daemonize(app="sync_tables", pid=init_pid, action=self.mysql_source.sync_tables,
                                        foreground=foreground, keep_fds=keep_fds)
                sync_daemon.start()

    def __stop_all_active_sources(self):
        """
            The method stops all the active sources within the target PostgreSQL database.
        """
        active_source = self.pg_engine.get_active_sources()
        for source in active_source:
            self.source = source[0]
            self.__stop_replica()

    def upgrade_replica_schema(self):
        """
            The method upgrades an existing replica catalogue to the newer version.
            If the catalogue is from the previous version
        """
        catalog_version = self.pg_engine.get_catalog_version()
        if catalog_version == self.catalog_version:
            print("The replica catalogue is already up to date.")
            sys.exit()
        else:
            if catalog_version == self.upgradable_version:
                upg_msg = 'Upgrading the catalogue %s to the version %s.\n Are you sure? YES/No\n' % (
                catalog_version, self.catalog_version)
                upg_cat = input(upg_msg)
                if upg_cat == 'YES':
                    self.logger.info("Performing the upgrade")
                    self.pg_engine.upgrade_catalogue_v1()
                elif upg_cat in self.lst_yes:
                    print('Please type YES all uppercase to confirm')
            elif catalog_version.split('.')[0] == '2' and catalog_version.split('.')[1] == '0':
                print('Stopping all the active sources.')
                self.__stop_all_active_sources()
                print('Upgrading the replica catalogue. ')
                self.pg_engine.upgrade_catalogue_v20()
            else:
                print('Wrong starting version. Expected %s, got %s' % (catalog_version, self.upgradable_version))
                sys.exit()

    def update_schema_mappings(self):
        """
            The method updates the schema mappings for the given source.
            The schema mappings is a configuration parameter but is stored in the replica
            catalogue when the source is added. If any change is made on the configuration file this method
            should be called to update the system catalogue as well. The pg_engine method checks for any conflict before running
            the update on the tables t_sources and t_replica_tables.
            Is compulsory to specify a source name when running this method.
        """
        if self.args.source == "*":
            print("You must specify a source name with the argument --source")
        else:
            self.__stop_replica()
            self.pg_engine.update_schema_mappings()



    def read_replica(self, log_queue, log_read, trx_queue):
        """
            The method reads the replica stream for the given source and stores the row images
            in the target postgresql database.
        """
        write_pid(self.pid_file, "read_replica_process")

        if "keep_existing_schema" in self.config["sources"][self.args.source]:
            keep_existing_schema = self.config["sources"][self.args.source]["keep_existing_schema"]
        else:
            keep_existing_schema = False
        self.mysql_source.keep_existing_schema = keep_existing_schema

        self.mysql_source.logger  = log_read[0]
        self.pg_engine.logger  = log_read[0]

        self.mysql_source.init_read_replica()

        new_packet_stream = MyBinLogStreamReader(
            connection_settings=self.mysql_source.replica_conn,
            server_id=self.mysql_source.my_server_id,
            only_events=[RotateEvent, DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, QueryEvent, MyGtidEvent,
                         XidEvent],
            resume_stream=True,
            only_schemas=self.mysql_source.schema_replica,
            slave_heartbeat=self.mysql_source.sleep_loop,
        )
        if not new_packet_stream._BinLogStreamReader__connected_ctl:
            new_packet_stream._BinLogStreamReader__connect_to_ctl()

        packet_queue_list = []
        trx_queue_list = []
        packet_queue_num = 8
        initial_log_file = mp.Queue()
        signal_queue = mp.Queue()

        for i in range(packet_queue_num):
            packet_queue_list.append(mp.Pipe())
            trx_queue_list.append(mp.Pipe())

        conn_in, conn_out = mp.Pipe()
        dispatcher_packet_queue = queue.Queue(1 * 1024 * 1024)
        threading.Thread(target=self.dispatcher_packet_to_pipe, args=(dispatcher_packet_queue, conn_in)).start()

        dispatcher_packet_process = mp.Process(target=self.dispatcher_packet_to_multiple_process, name='dispatcher_packet_new', daemon=True, args=(conn_out, packet_queue_list, ))
        dispatcher_packet_process.start()

        for i in range(len(packet_queue_list)):
            packet_to_trx_process = mp.Process(target=self.convert_packet_to_trx_new_2, name='convert_packet_to_trx', daemon=True, args=(new_packet_stream, packet_queue_list, trx_queue_list, i, signal_queue, initial_log_file, ))
            packet_to_trx_process.start()

        merge_trx_process = mp.Process(target=self.merge_trx_queue_new_pipe_new, name='merge_trx_queue', daemon=True, args=(trx_queue_list, trx_queue, ))
        merge_trx_process.start()

        while True:
            try:
                self.mysql_source.read_replica(dispatcher_packet_queue, signal_queue)
                time.sleep(self.sleep_loop)
            except Exception:
                log_queue.put(traceback.format_exc())
                break

    def replay_replica(self, log_queue, log_replay, trx_queue):
        """
            The method replays the row images stored in the target postgresql database.
        """
        self.pg_engine.logger = log_replay[0]
        self.pg_engine.connect_db()
        self.pg_engine.set_source_id()
        self.pg_engine.check_postgis()
        self.logger.debug("start replay process")
        write_pid(self.pid_file, "replay_replica_process")
        transaction_dispatcher = TransactionDispatcher(6, self.pg_engine)
        transaction_dispatcher.dispatcher(trx_queue)

    def merge_trx_queue_new_pipe_new(self, trx_queue_list, trx_queue):
        """
            The method merges the transaction queue according to each packet to transaction process.
        """
        write_pid(self.pid_file, "merge_trx_process")
        local_transaction_queue = queue.Queue()
        threading.Thread(target=self.dispatcher_trx_queue_read, args=(local_transaction_queue, trx_queue)).start()
        while True:
            for index in range(len(trx_queue_list)):
                for trx in trx_queue_list[index][1].recv():
                    local_transaction_queue.put(trx)
                time.sleep(0.000001)

    def dispatcher_trx_queue_read(self, local_transaction_queue, trx_queue):
        while True:
            trxs = list()
            try:
                for i in range(NUM_TRX):
                    trx = local_transaction_queue.get(timeout=1)
                    trxs.append(trx)
                trx_queue.send(trxs)
            except Exception:
                trx_queue.send(trxs)
                time.sleep(1)

    def dispatcher_packet_to_pipe(self, dispatcher_packet_queue, conn_in):
        """
            The method dispatches packet queue to pipe, used for multi-process communication.
        """
        packets = list()
        while True:
            for i in range(NUM_PACKET):
                packet = dispatcher_packet_queue.get(block=True)
                packets.append(packet)
                if packet is None:
                    break
            conn_in.send(packets)
            packets.clear()

    def convert_packet_to_trx_new_2(self, new_packet_stream, packet_queue_list, trx_queue_list, i, signal_queue, initial_log_file):
        """
            The method receives packet from the pip and convert packet to transaction in each process.
        """
        write_pid(self.pid_file, "packet_to_trx_process")
        packet_queue = queue.Queue()
        a_packet_queue = packet_queue_list[i][1]
        threading.Thread(target=self.packet_to_trx,
                         args=(new_packet_stream, packet_queue, i, trx_queue_list, signal_queue, initial_log_file,)).start()
        while True:
            for packet in a_packet_queue.recv():
                packet_queue.put(packet)

    def packet_to_trx(self, new_packet_stream, packet_queue, k, trx_queue_list, signal_queue, initial_log_file):
        """
            The method converts packet to transaction in each process and updates the next replica position according to
            the two params. The param master_data records the binlog file and position for next replica, and the param
            close_batch decides whether to write the value of the master_data to replica progress table.
            Corresponding to packets sent, the packets received may be one of the following four forms:
            (1) no transaction, that is, it only contains three packets: RotateEvent, FormatDescriptionEvent, and None,
            which means the ending of binlog file;
            (2) n transactions(n < NUM_PACKET) and None;
            (3) NUM_PACKET transaction;
            (4) NUM_PACKET transaction and None.
            When the packets received includes None packet, it will update the master_data and close_batch, so the
            format (1), (2) and (4) need to update the two params master_data and close_batch, and each form needs to
            send transactions.
        """
        modified_my_gtid_event()
        i = 0
        kk = 0

        binlog_file = ""
        a_trx_queue = trx_queue_list[k][0]
        trx_list = list()

        trx_event_index = 0  # the index of the event in a transaction
        full_transmission = 0  # whether received NUM_PACKET transaction
        full_transmission_none = 0  # whether the next packet is none when NUM_PACKET transaction have received

        replica_position = ReplicaPosition()
        replica_position_backup = ReplicaPosition()

        while True:
            trx = Transaction()
            trx.events = []
            trx_event_index = 0
            self.mysql_source.connect_db_buffered()
            table_type_map = self.mysql_source.get_table_type_map()
            while True:
                packet = packet_queue.get()
                trx_event_index += 1
                full_transmission_none += 1
                if packet is None:
                    if trx_event_index == 3:  # Corresponding to form(1)
                        time.sleep(2)
                        trx_event_index = 0
                        signal_queue.put(replica_position.new_object())
                        replica_position.reset_data()
                        a_trx_queue.send(trx_list)
                        trx_list.clear()
                    else:
                        trx_event_index = 0
                        if full_transmission == 1 and full_transmission_none == 0:  # Corresponding to form(4)
                            signal_queue.put(replica_position_backup.new_object())
                            full_transmission = 0
                            replica_position.reset_data()
                            replica_position_backup.reset_data()
                        else:  # Corresponding to form(2)
                            a_trx_queue.send(trx_list)
                            trx_list.clear()
                            signal_queue.put(replica_position.new_object())
                            replica_position.reset_data()
                            kk = 0
                    break
                i += 1
                if not new_packet_stream._BinLogStreamReader__connected_ctl:
                    new_packet_stream._BinLogStreamReader__connect_to_ctl()

                unpack = struct.unpack('<cIcIIIH', packet[0:20])
                a_packet = Packet(unpack[1], byte2int(unpack[2]), unpack[3], unpack[4], unpack[5], unpack[6],
                                  packet[20: (20 + unpack[4] - 23)])

                binlog_event = MyBinLogPacketWrapper(a_packet, new_packet_stream.table_map,
                                                     new_packet_stream._ctl_connection,
                                                     new_packet_stream._BinLogStreamReader__use_checksum,
                                                     new_packet_stream._BinLogStreamReader__allowed_events_in_packet,
                                                     new_packet_stream._BinLogStreamReader__only_tables,
                                                     new_packet_stream._BinLogStreamReader__ignored_tables,
                                                     new_packet_stream._BinLogStreamReader__only_schemas,
                                                     new_packet_stream._BinLogStreamReader__ignored_schemas,
                                                     new_packet_stream._BinLogStreamReader__freeze_schema,
                                                     new_packet_stream._BinLogStreamReader__fail_on_table_metadata_unavailable)

                # Process different events by referring to original code in python-mysql-replication
                if binlog_event.event_type == RotateEvent:
                    new_packet_stream.log_pos = binlog_event.event.position
                    new_packet_stream.log_file = binlog_event.event.next_binlog
                    new_packet_stream.table_map = {}
                elif binlog_event.log_pos:
                    new_packet_stream.log_pos = binlog_event.log_pos

                if new_packet_stream.skip_to_timestamp and binlog_event.timestamp < new_packet_stream.skip_to_timestamp:
                    continue

                if binlog_event.event_type == TABLE_MAP_EVENT and \
                        binlog_event.event is not None:
                    new_packet_stream.table_map[binlog_event.event.table_id] = \
                        binlog_event.event.get_table()

                if binlog_event.event is None or (
                        binlog_event.event.__class__ not in new_packet_stream._BinLogStreamReader__allowed_events):
                    continue

                if binlog_event.event_type == FORMAT_DESCRIPTION_EVENT:
                    new_packet_stream.mysql_version = binlog_event.event.mysql_version

                if isinstance(binlog_event.event, RowsEvent):
                    binlog_event.event.rows

                packet_str = binlog_event.event.packet.log_pos
                event = binlog_event.event
                if i == 1 and not isinstance(event, RotateEvent):
                    binlog_file = initial_log_file.get()
                    initial_log_file.put(binlog_file)
                if isinstance(event, RotateEvent):
                    trx.binlog_file = event.next_binlog
                    binlog_file = trx.binlog_file
                    if initial_log_file.qsize() == 0:
                        initial_log_file.put(binlog_file)
                elif isinstance(event, GtidEvent):
                    if isinstance(event, MyGtidEvent):
                        trx.gtid = event.gtid
                        trx.last_committed = event.last_committed
                        trx.sequence_number = event.sequence_number
                        trx.binlog_file = binlog_file
                        column_map_list = []
                else:
                    if isinstance(event, RowsEvent):
                        if event.table in table_type_map[event.schema]:
                            single_column_map = table_type_map[event.schema][event.table]
                            column_map_list.append(single_column_map)
                    finished = ConvertToEvent.feed_event(trx, event, self.mysql_source, self.pg_engine)
                    if finished:
                        trx.finished = finished
                        if trx.is_dml and len(trx.events) > 0:
                            trx.sql_list = trx.fetch_sql(self.mysql_source, column_map_list)
                            kk += 1
                        trx.events = []
                        trx.log_pos = packet_str
                        trx_dump = pickle.dumps(trx)
                        trx_list.append(trx_dump)
                        master_data = trx.get_gtid()
                        close_batch = trx.finished
                        replica_position.set_data(master_data, close_batch)
                        if kk == NUM_PACKET:  # Corresponding to form(3) and send transactions for form(4)
                            kk = 0
                            full_transmission_none = -1
                            a_trx_queue.send(trx_list)
                            trx_list.clear()
                            full_transmission = 1
                            replica_position_backup.copy_data(replica_position)
                            replica_position.reset_data()
                        break

    def dispatcher_packet_to_multiple_process(self, conn_out, packet_queue_list):
        """
            The method receives packet from the pipe and dispatches packet to each packet to transaction process.
        """
        write_pid(self.pid_file, "dispatcher_packet_process")
        q_out = queue.Queue()
        threading.Thread(target=self.dispatcher_packet_to_every_queue, args=(q_out, packet_queue_list)).start()
        while True:
            for packet in conn_out.recv():
                q_out.put(packet)

    def dispatcher_packet_to_every_queue(self, q_out, packet_queue_list):
        """
            The method dispatches packets in order to each packet to transaction process based on the process index.
            The following parameters are explained:
            (1) None means the ending of reading packets once;
            (2) NUM_PACKET means the maximum number of transactions which will be sent, not the number of packets;
            The packets received by each process may be one of the following four forms:
            (1) no transaction, that is, it only contains three packets: RotateEvent, FormatDescriptionEvent, and None,
            which means the ending of binlog file;
            (2) n transactions(n < NUM_PACKET) and None;
            (3) NUM_PACKET transaction;
            (4) NUM_PACKET transaction and None.
        """
        packet_queue_num = len(packet_queue_list)
        batch_size = NUM_PACKET
        num = 0
        index = 0
        packet_list = list()
        packet_new = None
        trx_num = 0
        while True:
            packet = q_out.get()
            if packet is not None and len(packet) > MYSQL_PACKET_HAED_LENGTH:
                unpack = struct.unpack('<cIcIIIH', packet[0:20])
                event_type = byte2int(unpack[2])
                if event_type == GTID_EVENT:
                    num += 1
                    trx_num += 1
                packet_list.append(packet)
            if num == batch_size or packet is None:
                if packet is None:
                    packet_list.append(packet)
                fflag = 0
                if num == batch_size:
                    packet_new = q_out.get()
                    if packet_new is None:
                        fflag = 1
                        packet_list.append(packet_new)
                else:
                    fflag = 2
                num = 0
                packet_queue_list[index][0].send(packet_list)
                packet_list.clear()
                if fflag == 0:
                    packet_list.append(packet_new)
                index += 1
                if index == packet_queue_num:
                    index = 0

    def __run_replica(self):
        """
            This method is the method which manages the two separate processes using the multiprocess library.
            It can be daemonised or run in foreground according with the --debug configuration or the log
            destination.
        """
        write_pid(self.pid_file, "run_replica_process")

        if "auto_maintenance" not in self.config["sources"][self.args.source]:
            auto_maintenance = "disabled"
        else:
            auto_maintenance = self.config["sources"][self.args.source]["auto_maintenance"]

        if "gtid_enable" not in self.config["sources"][self.args.source]:
            gtid_enable = False
        else:
            gtid_enable = self.config["sources"][self.args.source]["gtid_enable"]

        self.mysql_source.gtid_enable = gtid_enable

        log_read = self.__init_logger("read")
        log_replay = self.__init_logger("replay")

        signal.signal(signal.SIGINT, self.terminate_replica)
        log_queue = mp.Queue()
        self.sleep_loop = self.config["sources"][self.args.source]["sleep_loop"]
        if self.is_debug_or_dump_json:
            check_timeout = self.sleep_loop
        else:
            check_timeout = self.sleep_loop*10
        self.logger.info("Starting the replica daemons for source %s " % (self.args.source))
        trx_in, trx_out = mp.Pipe()
        self.read_daemon = mp.Process(target=self.read_replica, name='read_replica', daemon=False, args=(log_queue, log_read, trx_in,))
        self.replay_daemon = mp.Process(target=self.replay_replica, name='replay_replica', daemon=False, args=(log_queue, log_replay, trx_out,))
        self.read_daemon.start()
        self.replay_daemon.start()
        while True:
            read_alive = self.read_daemon.is_alive()
            replay_alive = self.replay_daemon.is_alive()
            if read_alive and replay_alive:
                self.logger.debug("Replica process for source %s is running" % (self.args.source))
                self.pg_engine.cleanup_replayed_batches()
            else:
                stack_trace = log_queue.get()
                self.logger.error("Read process alive: %s - Replay process alive: %s" % (read_alive, replay_alive, ))
                self.logger.error("Stack trace: %s" % (stack_trace, ))
                if read_alive:
                    self.read_daemon.terminate()
                    self.logger.error("Replay daemon crashed. Terminating the read daemon.")
                if replay_alive:
                    self.replay_daemon.terminate()
                    self.logger.error("Read daemon crashed. Terminating the replay daemon.")
                if self.is_debug_or_dump_json:
                    replica_status = "stopped"
                else:
                    replica_status = "error"
                try:
                    self.pg_engine.connect_db()
                    self.pg_engine.set_source_status(replica_status)
                except:
                    pass
                notifier_message = "The replica process crashed.\n Source: %s\n Stack trace: %s " % (
                self.args.source, stack_trace)
                self.notifier.send_message(notifier_message, 'critical')

                break
            time.sleep(check_timeout)
            if auto_maintenance != "disabled":
                self.pg_engine.auto_maintenance = auto_maintenance
                self.pg_engine.connect_db()
                run_maintenance = self.pg_engine.check_auto_maintenance()
                self.pg_engine.disconnect_db()
                if run_maintenance:
                    self.pg_engine.run_maintenance()

        self.logger.info("Replica process for source %s ended" % (self.args.source))

    def start_replica(self):
        """
            The method starts a new replica process.
            Is compulsory to specify a source name when running this method.
        """
        if os.path.exists(self.pid_file):
            os.remove(self.pid_file)

        write_pid(self.pid_file, "start_replica_process")

        if self.args.source == "*":
            print("You must specify a source name using the argument --source")
        else:
            self.pg_engine.connect_db()
            self.logger.info("Checking if the replica for source %s is stopped " % (self.args.source))
            replica_status = self.pg_engine.get_replica_status()
            if replica_status in ['syncing', 'running', 'initialising']:
                print("The replica process is already started or is syncing. Aborting the command.")
            elif replica_status == 'error':
                print("The replica process is in error state.")
                print("You may need to check the replica status first. To enable it run the following command.")
                print("chameleon.py enable_replica --config %s --source %s " % (self.args.config, self.args.source))
            else:
                self.mysql_source.source_config = self.config["sources"][self.args.source]
                self.mysql_source.connect_db_buffered()
                self.mysql_source.check_mysql_config(True)

                self.logger.info("Cleaning not processed batches for source %s" % (self.args.source))
                self.pg_engine.clean_not_processed_batches()
                self.pg_engine.disconnect_db()
                if self.is_debug_or_dump_json:
                    self.__run_replica()
                    print("enter into run replica based on debug")
                else:
                    if self.config["log_dest"] == 'stdout':
                        foreground = True
                    else:
                        foreground = False
                        print("Starting the replica process for source %s" % (self.args.source))

                    keep_fds = [self.logger_fds]
                    app_name = "%s_replica" % self.args.source

                    replica_daemon = mp.Process(target=self.__run_replica, name='__run_replica', daemon=False)
                    replica_daemon.start()

    def __stop_replica(self):
        """
            The method reads the pid of the replica process for the given self.source and sends a SIGINT which
            tells the replica process to manage a graceful exit.
        """
        replica_pid = os.path.expanduser(self.config["pid_dir"] + "replica.pid")
        if os.path.isfile(replica_pid):
            file_pid = open(replica_pid, 'r')
            process_id = file_pid.readlines()
            file_pid.close()
            for i in range(len(process_id)):
                process_number = process_id[i].strip().split(":")[1]
                try:
                    os.kill(int(process_number), 9)
                except:
                    print("no such process for process %s" % process_number)
            print("The replica process is stopped")

    def __set_conf_permissions(self, cham_dir):
        """
            The method sets the permissions of the configuration directory to 700

            :param cham_dir: the chameleon configuration directory to fix
        """
        if os.path.isdir(cham_dir):
            os.chmod(cham_dir, 0o700)

    def stop_replica(self):
        """
            The method calls the private method __stop_replica to stop the replica process.
        """
        try:
            self.pg_engine.connect_db()
            self.pg_engine.set_source_status("stopped")
        except:
            self.logger.error("Setting source status to stopped failed.")
        self.__stop_replica()

    def stop_all_replicas(self):
        """
            The method  stops all the active replicas within the target database
        """
        self.__stop_all_active_sources()

    def show_errors(self):
        """
            displays the error log entries if any.
            If the source the error log is filtered for this source only.
        """
        log_id = self.args.logid
        self.pg_engine.source = self.args.source
        log_error_data = self.pg_engine.get_log_data(log_id)
        if log_error_data:
            if log_id != "*":
                tab_body = []
                log_line = log_error_data[0]
                tab_body.append(['Log id', log_line[0]])
                tab_body.append(['Source name', log_line[1]])
                tab_body.append(['ID Batch', log_line[2]])
                tab_body.append(['Table', log_line[3]])
                tab_body.append(['Schema', log_line[4]])
                tab_body.append(['Error timestamp', log_line[5]])
                tab_body.append(['SQL executed', log_line[6]])
                tab_body.append(['Error message', log_line[7]])
                print(tabulate(tab_body, tablefmt="simple"))
            else:
                tab_headers = ['Log id', 'Source name', 'ID Batch', 'Table', 'Schema', 'Error timestamp']
                tab_body = []
                for log_line in log_error_data:
                    log_id = log_line[0]
                    id_batch = log_line[1]
                    source_name = log_line[2]
                    table_name = log_line[3]
                    schema_name = log_line[4]
                    error_timestamp = log_line[5]
                    tab_row = [log_id, id_batch, source_name, table_name, schema_name, error_timestamp]
                    tab_body.append(tab_row)
                print(tabulate(tab_body, headers=tab_headers, tablefmt="simple"))
        else:
            print('There are no errors in the log')

    def show_status(self):
        """
            list the replica status from the replica catalogue.
            If the source is specified gives some extra details on the source status.
        """
        self.pg_engine.auto_maintenance = "disabled"
        if self.args.source != "*":
            if "auto_maintenance" in self.config["sources"][self.args.source]:
                self.pg_engine.auto_maintenance = self.config["sources"][self.args.source]["auto_maintenance"]

        self.pg_engine.source = self.args.source
        configuration_data = self.pg_engine.get_status()
        configuration_status = configuration_data[0]
        schema_mappings = configuration_data[1]
        table_status = configuration_data[2]
        replica_counters = configuration_data[3]
        tab_headers = ['Source id', 'Source name', 'Type', 'Status', 'Consistent', 'Read lag', 'Last read',
                       'Replay lag', 'Last replay']
        tab_body = []
        for status in configuration_status:
            source_id = status[0]
            source_name = status[1]
            source_status = status[2]
            read_lag = status[3]
            last_read = status[4]
            replay_lag = status[5]
            last_replay = status[6]
            consistent = status[7]
            source_type = status[8]
            last_maintenance = status[9]
            next_maintenance = status[10]
            tab_row = [source_id, source_name, source_type, source_status, consistent, read_lag, last_read, replay_lag,
                       last_replay]
            tab_body.append(tab_row)
        print(tabulate(tab_body, headers=tab_headers, tablefmt="simple"))
        if schema_mappings:
            print('\n== Schema mappings ==')
            tab_headers = ['Origin schema', 'Destination schema']
            tab_body = []
            for mapping in schema_mappings:
                origin_schema = mapping[0]
                destination_schema = mapping[1]
                tab_row = [origin_schema, destination_schema]
                tab_body.append(tab_row)
            print(tabulate(tab_body, headers=tab_headers, tablefmt="simple"))
        if table_status:
            print('\n== Replica status ==')
            # tab_headers = ['',  '',  '']
            tab_body = []
            tables_no_replica = table_status[0]
            tab_row = ['Tables not replicated', tables_no_replica[1]]
            tab_body.append(tab_row)
            tables_with_replica = table_status[1]
            tab_row = ['Tables replicated', tables_with_replica[1]]
            tab_body.append(tab_row)
            tables_all = table_status[2]
            tab_row = ['All tables', tables_all[1]]
            tab_body.append(tab_row)
            tab_row = ['Last maintenance', last_maintenance]
            tab_body.append(tab_row)
            tab_row = ['Next maintenance', next_maintenance]
            tab_body.append(tab_row)
            if replica_counters:
                tab_row = ['Replayed rows', replica_counters[0]]
                tab_body.append(tab_row)
                tab_row = ['Replayed DDL', replica_counters[2]]
                tab_body.append(tab_row)
                tab_row = ['Skipped rows', replica_counters[1]]
                tab_body.append(tab_row)
            print(tabulate(tab_body, tablefmt="simple"))
            if tables_no_replica[2]:
                print('\n== Tables with replica disabled ==')
                print("\n".join(tables_no_replica[2]))

    def detach_replica(self):
        """
            The method terminates the replica process. The source is removed from the table t_sources with all the associated data.
            The schema sequences in are reset to the max values in the corresponding tables, leaving
            the postgresql database as a standalone snapshot.
            The method creates the foreign keys existing in MySQL as well.
            Is compulsory to specify a source name when running this method.
        """
        if self.args.source == "*":
            print("You must specify a source name with the argument --source")
        elif self.args.tables != "*":
            print("You cannot specify a table name when running detach_replica.")
        else:
            drp_msg = 'Detaching the replica will remove any reference for the source %s.\n Are you sure? YES/No\n' % self.args.source
            drop_src = input(drp_msg)
            if drop_src == 'YES':
                if "keep_existing_schema" in self.config["sources"][self.args.source]:
                    keep_existing_schema = self.config["sources"][self.args.source]["keep_existing_schema"]
                else:
                    keep_existing_schema = False
                self.pg_engine.keep_existing_schema = keep_existing_schema
                if not keep_existing_schema:
                    self.pg_engine.fk_metadata = self.mysql_source.get_foreign_keys_metadata()
                self.__stop_replica()
                self.pg_engine.detach_replica()
            elif drop_src in self.lst_yes:
                print('Please type YES all uppercase to confirm')

    def run_maintenance(self):
        """
            The method runs a maintenance process on the target postgresql database specified in the given source.
        """
        maintenance_pid = os.path.expanduser('%s/%s_maintenance.pid' % (self.config["pid_dir"], self.args.source))
        if self.args.source == "*":
            print("You must specify a source name with the argument --source")
        else:
            if self.is_debug_or_dump_json:
                self.pg_engine.run_maintenance()
            else:
                if self.config["log_dest"] == 'stdout':
                    foreground = True
                else:
                    self.logger.info("Starting the maintenance on the source %s" % (self.args.source,))
                    foreground = False
                    print("Starting the maintenance process for source %s" % (self.args.source))
                    keep_fds = [self.logger_fds]

                    app_name = "%s_maintenance" % self.args.source
                    maintenance_daemon = Daemonize(app=app_name, pid=maintenance_pid,
                                                   action=self.pg_engine.run_maintenance, foreground=foreground,
                                                   keep_fds=keep_fds)
                    try:
                        maintenance_daemon.start()
                    except:
                        print("The  maintenance process is already started. Aborting the command.")

    def __init_logger(self, logger_name):
        """
        The method initialise a new logger object using the configuration parameters.
        The formatter is different if the debug option is enabler or not.
        The method returns a new logger object and sets the logger's file descriptor in the class variable
        logger_fds, used when the process is demonised.

        :param logger_name: the name of the logger used to build the file name and get the correct logger
        :return: list with logger and file descriptor
        :rtype: list

        """
        log_dir = self.config["log_dir"]
        log_level = self.config["log_level"]
        log_dest = self.config["log_dest"]
        log_days_keep = self.config["log_days_keep"]
        log_level_map = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL
        }
        config_name = self.args.config
        source_name = self.args.source
        debug_mode = self.is_debug_or_dump_json
        if source_name == '*':
            log_name = "%s_general" % (config_name)
        elif logger_name == "global":
            log_name = "%s_%s" % (config_name, source_name)
        else:
            log_name = "%s_%s_%s" % (config_name, source_name, logger_name)

        log_file = os.path.expanduser('%s/%s.log' % (log_dir, log_name))
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        if debug_mode:
            str_format = "%(asctime)s.%(msecs)03d %(processName)s %(levelname)s %(filename)s (%(lineno)s): %(message)s"
        else:
            str_format = "%(asctime)s %(processName)s %(levelname)s: %(message)s"
        formatter = logging.Formatter(str_format, "%Y-%m-%d %H:%M:%S")

        if log_dest == 'stdout' or debug_mode:
            fh = logging.StreamHandler(sys.stdout)
        elif log_dest == 'file':
            fh = TimedRotatingFileHandler(log_file, when="d", interval=1, backupCount=log_days_keep)
        else:
            print("Invalid log_dest value: %s" % log_dest)
            sys.exit()

        if debug_mode:
            log_level = 'debug'

        fh.setLevel(log_level_map.get(log_level, logging.DEBUG))
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        logger_fds = fh.stream.fileno()
        return [logger, logger_fds]

    def __start_database_object_replica(self, db_object_type):
        """
        This method start a database object's replication with a given source and configuration.

        :param db_object_type: the database object type, refer to enumeration class DBObjectType
        """
        if self.args.source == "*":
            self.logger.error("You must specify a source name with the argument --source")
        elif self.args.tables != "*":
            self.logger.error(
                "You cannot specify a table name when running start_%s_replica" % (db_object_type.name.lower(),))
        else:
            try:
                source_type = self.config["sources"][self.args.source]["type"]
            except KeyError:
                self.logger.error("The source %s doesn't exists." % (self.args.source,))
                sys.exit()

            if source_type == "mysql":
                self.__start_mysql_database_object_replica(db_object_type)
            elif source_type == "pgsql":
                self.logger.warning("The case that --source=pgsql is not supported currently.")

    def __start_mysql_database_object_replica(self, db_object_type):
        """
        The method start a database object's replication from mysql to destination with configuration.

        :param db_object_type: the database object type, refer to enumeration class DBObjectType
        """
        if self.is_debug_or_dump_json:
            self.mysql_source.start_database_object_replica(db_object_type)
        else:
            if self.config["log_dest"] == 'stdout':
                foreground = True
            else:
                foreground = False
            keep_fds = [self.logger_fds]
            init_pid = os.path.expanduser('%s/%s.pid' % (self.config["pid_dir"], self.args.source))
            self.logger.debug("Initialising the daemon replica process for source mysql.")
            init_daemon = Daemonize(app="start_%s_replica" % (db_object_type.name.lower(),), pid=init_pid,
                                    action=self.mysql_source.start_database_object_replica,
                                    privileged_action=lambda: (db_object_type,),
                                    foreground=foreground, keep_fds=keep_fds)
            init_daemon.start()

    def start_view_replica(self):
        """
        The method start a replication on views for a given source and configuration.
        """
        self.logger.info("Ready to start the view replica for source %s" % (self.args.source,))
        self.__start_database_object_replica(DBObjectType.VIEW)

    def start_trigger_replica(self):
        """
        The method start a replication on triggers for a given source and configuration.
        """
        self.logger.info("Ready to start the trigger replica for source %s" % (self.args.source,))
        self.__start_database_object_replica(DBObjectType.TRIGGER)

    def start_func_replica(self):
        """
        The method start a replication on functions for a given source and configuration.
        """
        self.logger.info("Ready to start the function replica for source %s" % (self.args.source,))
        self.__start_database_object_replica(DBObjectType.FUNC)

    def start_proc_replica(self):
        """
        The method start a replication on procedures for a given source and configuration.
        """
        self.logger.info("Ready to start the procedure replica for source %s" % (self.args.source,))
        self.__start_database_object_replica(DBObjectType.PROC)

    def start_index_replica(self):
        """
        The method start a replication on index for a given source and configuration.
        """
        self.logger.info("Ready to start the index replica for source %s" % (self.args.source,))
        self.mysql_source.start_index_replica()
