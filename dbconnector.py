import pandas as pd
import re

import exceptions


class DatabaseManager:
    """
    Manages the connection to a PostgreSQL database.
    Make sure to run .commit() to update changes.

    :methods:
        __init__
        __del__
        createTable
        commit
        getData
        testQuery
        close

    :attributes (read-only):
        conn
        cursor
    """
    _conn, _cursor = None, None

    @property
    def conn(self):
        '''Ensures self._conn will not be modified after init'''
        return self._conn

    @property
    def cursor(self):
        '''Ensures self._cursor will not be modified after init'''
        return self._cursor

    def commit(self):
        '''Commit pending transactions to database'''
        self.conn.commit()

    def __init__(self):
        '''Setup connection - implement in derived classes '''
        pass

    def __del__(self):
        '''Close connection to database'''
        self.close()

    def createTable(self, table: str, dataVars: dict, foreignKeys=[], drop=False, verify=False):
        '''
        Drop and recreate table. Must set verify to authenticate action.

        db.createTable(
            table='customer', 
            dataVars=dict(name='VARCHAR(20)', address='VARCHAR(50)'),
            foreignKeys=['store', 'address']
            drop=True,
            verify=True
            )
        '''

        if not verify:
            raise exceptions.FalseVerifyException(
                'Must set verify=True to create new table')
        if drop:
            sql = 'DROP TABLE IF EXISTS {};'.format(table)
            self.cursor.execute(sql)
            self.commit()

        # create table
        sql = '''CREATE TABLE IF NOT EXISTS {} (
            {}_id SERIAL PRIMARY KEY'''.format(table, table)
        for key in foreignKeys:
            sql += ', {}_id INTEGER'.format(key)
        for var in dataVars.items():
            sql += ', {} {}'.format(var[0], var[1])
        sql += ');'
        self.cursor.execute(sql)
        self.commit()

    def getData(self, sql: str) -> pd.DataFrame:
        '''
        Returns query data inside a DataFrame. Returns None if there is no 
        corresponding entry
        
        data = db.getData('SELECT * FROM customer')
        '''
        if 'drop' in sql and (
            len(re.findall("(?<=').+drop.+(?=')", sql)) \
                != len(re.findall('drop', sql))): # check for malicious DROP
            print(sql)
            raise exceptions.SQLDropException(
                'SQL query is specifying a drop command')
        data = pd.read_sql(sql, self._conn)
        if len(data) == 0:
            return None
        else:
            return data

    def testQuery(self, sql):
        """Limit selected data to the first 10 entries - implement in adapter"""
        pass

    def close(self):
        """
        Close cursor and conn
        """
        self.cursor.close()
        self.conn.close()

    def runSQL(self, sql: str, verify=False):
        "Run manual SQL, must verify"
        if not verify:
            raise exceptions.FalseVerifyException(
                'Must set verify=True to run custom SQL')

        self.cursor.execute(sql)
        self.commit()

    def queryId(self, selectSql, insertSql):
        """Query for key, ensures obs exists"""
        idResult = self.getData(selectSql)
        if idResult is None:
            self.runSQL(insertSql, verify=True)
        return self.getData(selectSql).values[0][0]
