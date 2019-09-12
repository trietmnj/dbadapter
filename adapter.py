"""Idiosyncratic adapters to different sql management systems"""
import re
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__)))  # add wd to path

from dbconnector import DatabaseManager
from dbadapter import exceptions


class SQLServer(DatabaseManager):
    """Adapt base connector to SQL Server"""
    def setupConnection(self, credentials):
        import pyodbc
        connString = (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f"SERVER={credentials['server']};"
            f"DATABASE={credentials['database']};"
            f"UID={credentials['username']};"
            f"PWD={credentials['password']}")
        self._conn = pyodbc.connect(connString)
        self._cursor = self.conn.cursor()

    def testQuery(self, sql):
        """Limit query return to 10 rows"""
        firstWord = sql.split()[0]
        if firstWord == 'select':
            sql = sql.replace('select', 'select top 10', 1)
        elif firstWord == 'SELECT':
            sql = sql.replace('SELECT', 'select top 10', 1)
        else:
            raise exceptions.InvalidTestQuery(f"Unable to run '{sql}'")
        return self.getData(sql)

    def dropTableIfExists(self, table: str, verify=False):
        if not verify:
            raise exceptions.FalseVerifyException(
                'Must set verify=True to drop table')
        sql = \
            f"""
                IF OBJECT_ID('{table}', 'U') IS NOT NULL
                DROP TABLE {table};
            """
        self.runSQL(sql, verify=True)

    def createTable(self, table: str, dataVars: dict, foreignKeys:dict):
        """ Creates table
            database.createTable(
                table='customer', 
                dataVars=dict(name='VARCHAR(20)', address='VARCHAR(50)'),
                foreignKeys=dict(insider_id='insiderTrading.Insider')
                drop=True,
                verify=True
            )
        """
        pkey = table if '.' not in table else re.findall(
            "(?<=\.)\w+", table)[0]
        sql = f"""CREATE TABLE {table} (
            {pkey.lower()}_id INT NOT NULL IDENTITY PRIMARY KEY"""
        for var in dataVars.items():
            sql += ', {} {}'.format(var[0], var[1])
        for key, val in foreignKeys.items():
            sql += f', {key} INT FOREIGN KEY REFERENCES {val}'
        sql += ');'
        self.runSQL(sql, verify=True)


class PostgreSQL(DatabaseManager):
    """Adapt base connector to a PostgreSQL server"""
    def setupConnection(self, credentials):
        import psycopg2 as pg2
        self._conn = pg2.connect(
            host=credentials['server'],
            database=credentials['database'],
            user=credentials['username'],
            password=credentials['password'])
        self._cursor = self._conn.cursor()

    def testQuery(self, sql):
        """Limit selected data to the first 10 entries"""
        sql += " limit 10"
        return self.getData(sql)

    def dropTableIfExists(self, table: str, verify=False):
        if not verify:
            raise exceptions.FalseVerifyException(
                'Must set verify=True to drop table')
        sql = f'DROP TABLE IF EXISTS {table};'
        self.runSQL(sql, verify=True)

    # TODO fix foreign key relationship
    def createTable(self, table: str, dataVars: dict, foreignKeys=[]):
        """Generates a query to create a table"""
        sql = f"""CREATE TABLE {table} (
            {table}_id SERIAL PRIMARY KEY"""
        for var in dataVars.items():
            sql += ', {} {}'.format(var[0], var[1])
        for key in foreignKeys:
            sql += ', {}_id INTEGER'.format(key)
        sql += ');'
        self.runSQL(sql, verify=True)