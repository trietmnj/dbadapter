# dbadapter
Streamline read/write data to a relational database

Usage:


    from dbadapter import adapter
    from datsup import settings

    credentials = settings.readConfig('settings.ini')['auth']
    database = adapter.SQLServer(credentials)
    
    sql = \
        """
            create table tableName (
                table_id int primary key
            )    
        """
    data = database.runSQL(sql)

    sql = \
        """
            select top 10 * from sampleTable    
        """
    data = database.getData(sql)


settings.ini file should look like:
    [auth]
    server: <x.x.x.x,xxxx>
    database: <databaseName>
    username: <userName>
    password: <password>
