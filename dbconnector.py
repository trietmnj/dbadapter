from itertools import compress

import pandas as pd
import re

from dbadapter import exceptions


class DatabaseManager:
    """
    Base class with generalized database functionalities
    """
    _conn, _cursor = None, None

    def __init_subclass__(cls):
        """Enforce methods implementation in derived classes"""
        attrs = [
            'setupConnection', 'getData', 'dropTableIfExists', 'genSQLCreateTable']
        for attr in attrs:
            if not hasattr(cls, attr): 
                raise NotImplementedError(f'Must implement {attr} in {cls}')

    def __init__(self, credentials):
        self.setupConnection(credentials)
        print('Database connection opened.')

    def __enter__(self):
        """Set up context manager"""
        return self

    def __exit__(self):  # __exit__ will trigger even for error raised 
        """Clean up"""   # during the with statement
        self.close()
        print('Database connection closed.')

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
            self.dropTableIfExists(table)
        sql = self.genSQLCreateTable(table, dataVars, foreignKeys)
        
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
        sql = sql.replace('\n', ' ').strip()
        self.cursor.execute(sql)
        self.commit()

    def queryId(self, selectSql, insertSql):
        """Query for key, ensures obs exists"""
        idResult = self.getData(selectSql)
        if idResult is None:
            self.runSQL(insertSql, verify=True)
        return self.getData(selectSql).values[0][0]
