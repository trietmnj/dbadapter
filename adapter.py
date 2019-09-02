import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from dbconnector import DatabaseManager
from exceptions import InvalidTestQuery


class SQLServer(DatabaseManager):
    """Adapt database connector to SQL Server"""
    def __init__(self, credentials: dict):

        import pyodbc
        connString = (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f"SERVER={credentials['server']},1433;"
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
            raise InvalidTestQuery

        return self.getData(sql)

class PostgreSQL(DatabaseManager):
    """Adapt base connector to a PostgreSQL server"""
    def __init__(self, credentials):

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
