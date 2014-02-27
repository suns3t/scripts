import MySQLdb
import datetime, sys, getopt
from operator import itemgetter

USER_NAME="huyhoan"
USER_PWD="changethispassword"
HOST_NAME="mysql.rc.pdx.edu"

db = MySQLdb.connect(host=HOST_NAME,
        user=USER_NAME,
        passwd=USER_PWD)

cursor = db.cursor()

cursor.execute("SHOW DATABASES")

databases = {}

for row in cursor:
    database_name = row[0]
    update_time_query = "select update_time from information_schema.tables where table_schema='%s' order by update_time desc limit 1" % database_name
    cursor.execute(update_time_query)

    for row in cursor:
        update_time = row[0]
        if update_time is not None:
            # This is a MysISAM engine database, we are good to go
            databases[database_name] = str(update_time)

        else:
            #This is an InnoDB engine, UPDATE_TIME columns are always None
            # Let's look at columns where data_type='datetime' or column_name='timestamp'
            query_table = "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema='%s' AND (data_type='datetime' OR column_name='timestamp') AND column_name<>'match'" % database_name
            cursor.execute(query_table)

            min_time = "1970-01-01 00:00:00"
            update_time = min_time
            for row in cursor:
                table_name = row[0]
                column_name = row[1]
                cursor.execute("USE %s" % database_name)
                cursor.close()
                cursor = db.cursor()

                if column_name == "timestamp":
                    # Now column_name is timestamp, the value of it will be in Unix time
                    # query_column = "USE %s; SELECT MAX(FROM_UNIXTIME(%s)) FROM %s" % (database_name,column_name, table_name)
                    cursor.execute("SELECT MAX(FROM_UNIXTIME(`%s`)) FROM `%s`" % (column_name, table_name))

                    temp_date = cursor.fetchone()[0]
                    if temp_date is not None:
                        update_time = max(update_time, temp_date)

                else:
                    cursor.execute( 'SELECT CAST(MAX(`%s`) AS CHAR) FROM `%s`' % (column_name, table_name))

                    temp_date = cursor.fetchone()[0]
                    if temp_date is not None:
                        update_time = max(update_time, temp_date)

            if update_time == min_time:
                databases[database_name] = 0
            else:
                databases[database_name] = str(update_time)

for key in sorted(databases.iterkeys()):
    print "%s - %s" % (key, databases[key])
