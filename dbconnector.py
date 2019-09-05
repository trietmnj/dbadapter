import pandas as pd
import re
import pyodbc

from dbadapter import exceptions


class DatabaseManager:
    """
    Base class with generalized database functionalities
    """
    _conn, _cursor = None, None

    def __init_subclass__(cls):
        """Enforce methods implementation in derived classes"""
        attrs = [
            'setupConnection', 'getData', 'dropTableIfExists', 'createTable']
        for attr in attrs:
            if not hasattr(cls, attr): 
                raise NotImplementedError(f'{attr}() not implemented in {cls}')

    def __init__(self, credentials):
        self.setupConnection(credentials)
        print('Database connection opened.')

    def __enter__(self):
        """Set up context manager"""
        return self

    def __exit__(self, type, value, traceback):  # __exit__ will trigger even for error raised 
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

    def getData(self, sql: str) -> pd.DataFrame:
        """
        Returns query data inside a DataFrame. Returns None if there is no 
        corresponding entry
        
        data = db.getData('SELECT * FROM customer')
        """
        if 'drop' in sql and (
            len(re.findall("(?<=').+drop.+(?=')", sql)) \
                != len(re.findall('drop', sql))): # check for malicious DROP
            print(sql)
            raise exceptions.SQLDropException(
                'SQL query is specifying a drop command')
        data = pd.read_sql(sql, self._conn)

        if len(data) == 0:
            return None
        elif len(data)==1 and len(data.columns)==1 and data.iloc[0,0] is None:
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
        """Run manual SQL, must verify"""
        if not verify:
            raise exceptions.FalseVerifyException(
                'Must set verify=True to run custom SQL')
        sql = sql.replace('\n', ' ').strip()
        try:
            self.cursor.execute(sql)
            self.commit()
        except (pyodbc.ProgrammingError, pyodbc.DataError) as e:
            print('Problem SQL:')
            print(sql)
            raise e

    def queryId(self, selectSql, insertSql):
        """Query for key, ensures obs exists"""
        idResult = self.getData(selectSql)
        if idResult is None:
            self.runSQL(insertSql, verify=True)
        return self.getData(selectSql).values[0][0]
