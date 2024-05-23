#!/usr/bin/env python
import multiprocessing
import traceback
import os
import time
import json
from pkg_resources import get_distribution
__version__ = get_distribution('chameleon')
import argparse
from pg_chameleon import replica_engine
from pg_chameleon import mysql_source

commands = [
    'show_config',
    'show_sources',
    'show_status',
    'create_replica_schema',
    'drop_replica_schema',
    'upgrade_replica_schema',
    'add_source',
    'drop_source',
    'init_replica',
    'enable_replica',
    'update_schema_mappings',
    'refresh_schema',
    'sync_tables',
    'start_replica',
    'stop_replica',
    'detach_replica',
    'set_configuration_files',
    'show_errors',
    'run_maintenance',
    'stop_all_replicas',
    'start_view_replica',
    'start_trigger_replica',
    'start_proc_replica',
    'start_func_replica',
    'start_index_replica'
]

command_help = ','.join(commands)
config_help = """Specifies the configuration to use without the suffix yml. If  the parameter is omitted then ~/.pg_chameleon/configuration/default.yml is used"""
schema_help = """Specifies the schema within a source. If omitted all schemas for the given source are affected by the command. Requires the argument --source to be specified"""
source_help = """Specifies the source within a configuration. If omitted all sources are affected by the command."""
tables_help = """Specifies the tables within a source . If omitted all tables are affected by the command."""
logid_help = """Specifies the log id entry for displaying the error details"""
debug_help = """Forces the debug mode with logging on stdout and log level debug."""
version_help = """Displays pg_chameleon's installed  version."""
rollbar_help = """Overrides the level for messages to be sent to rolllbar. One of: "critical", "error", "warning", "info". The Default is "info" """
full_help = """When specified with run_maintenance the switch performs a vacuum full instead of a normal vacuum. """
truncate_help = """Truncate the existing tables instead of replacing them."""

parser = argparse.ArgumentParser(description='Command line for pg_chameleon.',  add_help=True)
parser.add_argument('command', type=str, help=command_help)
parser.add_argument('--config', type=str,  default='default',  required=False, help=config_help)
parser.add_argument('--schema', type=str,  default='*',  required=False, help=schema_help)
parser.add_argument('--source', type=str,  default='*',  required=False, help=source_help)
parser.add_argument('--tables', type=str,  default='*',  required=False, help=tables_help)
parser.add_argument('--logid', type=str,  default='*',  required=False, help=logid_help)
parser.add_argument('--debug', default=False, required=False, help=debug_help, action='store_true')
parser.add_argument('--version', action='version', help=version_help,version='{version}'.format(version=__version__))
parser.add_argument('--rollbar-level', type=str, default="info", required=False, help=rollbar_help)
parser.add_argument('--full', default=False, required=False, help=full_help, action='store_true')
args = parser.parse_args()

try:
    replica = replica_engine(args)
except Exception as e:
    error_msg = traceback.format_exc()
    print(error_msg)

if (replica.config['dump_json'] and (args.command == 'init_replica' or args.command == 'start_view_replica' or args.command == 'start_trigger_replica' or args.command == 'start_func_replica' or args.command == 'start_proc_replica')):
    try:
        dump_thread = multiprocessing.Process(target=replica.dump_Json)
        dump_thread.start()

        getattr(replica, args.command)()

        dump_thread.terminate()
        replica.write_Json()
        with open("migration_completed_status", 'a') as f:
            f.write(args.command + " finished" + os.linesep)
        replica.logger.info(args.command + " finished.")
    except Exception as e:
        error_msg = traceback.format_exc()
        print(error_msg)
elif args.debug:

    getattr(replica, args.command)()
    replica.logger.info(args.command + " finished.")
else:
    try:
        getattr(replica, args.command)()
        replica.logger.info(args.command + " finished.")
    except Exception as e:
        print("ERROR - Invalid command" )
        error_msg = traceback.format_exc()
        print(error_msg)
        print(command_help)
