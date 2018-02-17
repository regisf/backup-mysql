#!/usr/bin/env python3
"""
backupdb : Simple backup a database using JSON
(c) Regis FLORET <regisfloret@ŋmail.com>


"""

import argparse
import configparser
import datetime
import json
import logging
import sys
from typing import Dict, List, NamedTuple

import abc
import mysql.connector
import os
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 3306

L = logging.getLogger(__name__)
L.addHandler(logging.StreamHandler(stream=sys.stdout))


class DatabaseEntry(NamedTuple):
    """
    Database information container
    """
    host: str
    user: str
    password: str
    database: str
    port: int


class Cursor:
    """
    Simple wrapper around MySQL cursor for using the `with`  keyword
    It autocloses the cursor and autocommit
    """

    def __init__(self, conn, **kwargs):
        self.conn = conn
        self.cur = None
        self.kwargs = kwargs

    def __enter__(self) -> MySQLCursor:
        self.cur = self.conn.cursor(**self.kwargs)
        return self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.commit()
        self.cur.close()


def user_encoder(obj):
    if isinstance(obj, datetime.datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')


class Configuration:
    def __init__(self, config_file: str):
        self.config = configparser.ConfigParser()
        self.read_configuration_file(config_file)

    def read_configuration_file(self, config_file: str):
        if not os.path.exists(config_file):
            sys.exit(f"The configuration file {config_file}) don't exists")

        if not os.path.isfile(config_file):
            sys.exit(f"The configuration file {config_file}) is not a file")

        with open(config_file, 'r') as file_config:
            self.config.read_file(file_config)

    @property
    def tables(self):
        tables_entry = self.config.get('tables', 'tables')
        return list(filter(lambda x: x != '', tables_entry.split('\n')))

    @property
    def source_database(self) -> DatabaseEntry:
        source_entry = self.config['backup']
        return DatabaseEntry(host=source_entry.get('host') or DEFAULT_HOST,
                             user=source_entry.get('user'),
                             password=source_entry.get('password'),
                             database=source_entry.get('database'),
                             port=source_entry.get('port') or DEFAULT_PORT)

    @property
    def destination_database(self) -> DatabaseEntry:
        dest_entry = self.config['restore']
        return DatabaseEntry(host=dest_entry.get('host') or DEFAULT_HOST,
                             user=dest_entry.get('user'),
                             password=dest_entry.get('password'),
                             database=dest_entry.get('database'),
                             port=dest_entry.get('port') or DEFAULT_PORT)


class Action(abc.ABC):
    """
    Abstract class for the backup and the restore action
    """
    def __init__(self, conn: MySQLConnection, tables: List[str], output: str):
        self.conn = conn
        self.tables = tables
        self.output = output

    @abc.abstractmethod
    def process_action(self):
        pass

    def create_file_name(self, table_name: str) -> str:
        return os.path.join(self.output, f'{table_name}.json')


class BackupAction(Action):
    def process_action(self):
        with Cursor(self.conn, dictionary=True) as cursor:
            for table in self.tables:
                L.info(f'Saving {table}')
                cursor.execute(f'SELECT * FROM {table}')
                rows = [row for row in cursor]
                self.save_as_json(table, rows)

    def save_as_json(self, table_name: str, data: List[Dict]):
        result = json.dumps(data, default=user_encoder)

        with open(self.create_file_name(table_name), 'w', encoding='utf-8') as json_file:
            json_file.write(result)


class RestoreAction(Action):
    def process_action(self):
        for table in self.tables:
            L.info(f'Restoring {table}')

            if not self.file_exists(table):
                L.error(f'The file "{self.create_file_name(table)}" doesn\'t exist. Skipping.')

                continue

            if not self.table_exists(table):
                L.warning(f'Table "{table} not found on destination database. Skipping.')
                continue

            content = self.load_json_file(table)
            content = self.compare_content(table, content)

            fields = self.get_keys(content)
            if not fields:
                L.warning(f'Table {table} is empty')
                continue

            self.insert_into_db(table, content, fields)

    def insert_into_db(self, table: str, content: List[Dict], fields: List):
        query = 'INSERT INTO {table_name} ({fields}) VALUES ({values})'.format(table_name=table,
                                                                               fields=','.join(fields),
                                                                               values=','.join(['%s'] * len(fields)))
        values = [list(row.values()) for row in content]

        with Cursor(self.conn) as cursor:
            cursor.execute('SET foreign_key_checks = 0;')

            for value in values:
                try:
                    cursor.execute(query, value)  # Execture many [()]
                except mysql.connector.errors.DataError as err:
                    print(f'ca merde à cet endroit {value[0]} because {err}')

                except mysql.connector.errors.IntegrityError as err:
                    print(f"Erreur d'integrité pour {value[0]} because {err}")

    def load_json_file(self, table_name: str):
        with open(self.create_file_name(table_name), 'r') as json_file:
            content = json_file.read()
            return json.loads(content)

    def table_exists(self, table_name: str) -> bool:
        with Cursor(self.conn) as cursor:
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            return cursor.fetchone() is not None

    def file_exists(self, table_name: str) -> bool:
        file_name = self.create_file_name(table_name)

        return os.path.exists(file_name) and os.path.isfile(file_name)

    def get_keys(self, content: List[Dict], simple: bool = False) -> List:
        if len(content):
            return [f'`{key}`' if not simple else key for key in content[0].keys()]
        return []

    def compare_content(self, table: str, content: List[Dict]):
        keys = self.get_keys(content, simple=True)
        db_structure = self.get_table_structure(table)
        result_set = set(keys) - set(db_structure)

        if result_set:
            print(f'Table {table} as a diff {result_set}')
            for rs in result_set:
                print(f'{rs} is in source but not on the dest -> Remove from database results')
                for row in content:
                    row.pop(rs)

        return content

    def get_table_structure(self, table: str) -> List:
        with Cursor(self.conn) as cursor:
            cursor.execute(f'DESC {table}')
            fields = [field[0] for field in cursor]

            return fields


class Application:
    def __init__(self):
        self.arguments = self.install_arguments_parser()
        self.config = Configuration(self.arguments.config_file)
        self.db_src = self.create_databases_connection(self.config.source_database, self.arguments.backup)
        self.db_dest = self.create_databases_connection(self.config.destination_database, self.arguments.restore)

        self.backup_if_required()
        self.restore_if_required()

    def backup_if_required(self):
        if self.arguments.backup:
            action = BackupAction(self.db_src, self.config.tables, self.arguments.directory)
            action.process_action()

    def restore_if_required(self):
        if self.arguments.restore:
            action = RestoreAction(self.db_dest, self.config.tables, self.arguments.directory)
            action.process_action()

    def create_databases_connection(self, info: DatabaseEntry, creating: bool):
        if creating:
            return mysql.connector.connect(user=info.user,
                                           password=info.password,
                                           database=info.database,
                                           host=info.host,
                                           port=info.port)

    def install_arguments_parser(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--backup',
                            dest='backup',
                            action='store_true',
                            help='Backup the database')
        parser.add_argument('--restore',
                            dest='restore',
                            action='store_true',
                            help='Restore the database')
        parser.add_argument('-f',
                            '--file',
                            dest='config_file',
                            default=os.path.join(os.getcwd(), 'config.cfg'),
                            help='The configuration file where the information are stored (default is ./config.cfg)')
        parser.add_argument('-d',
                            '--directory',
                            dest='directory',
                            default=os.getcwd(),
                            help="The directory where to store or retrieve JSON files")
        parser.add_argument('-v',
                            '--verbose',
                            dest='verbose',
                            action='store_true',
                            default=False,
                            help="Be verbose and tell what's going on")

        args = parser.parse_args()

        if not (args.backup or args.restore):
            sys.exit('An action is required. Either backup or restore.')

        if not os.path.isdir(args.directory):
            sys.exit("The destination directory doesn't exist")

        if args.verbose:
            L.setLevel(logging.INFO)

        return args


if __name__ == '__main__':
    Application()
