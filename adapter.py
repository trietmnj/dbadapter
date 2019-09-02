import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from dbconnector import DatabaseManager


class SQLServer(DatabaseManager):
    """Adapt database connector to SQL Server"""
    def __init__(self, credentials: dict):

        import pyodbc
        self._conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f"SERVER={credentials['server']};"
            f"DATABASE={credentials['database']};"
            f"UID={credentials['username']};"
            f"PWD={credentials['password']}")
        self._cursor = self.conn.cursor()


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
