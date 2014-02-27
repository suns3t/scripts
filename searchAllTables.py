import MySQLdb
import sys

USER_NAME="huyhoan"
USER_PWD="changethispassword"
HOST_NAME="mysql.rc.pdx.edu"
database_name = "ohp_commonspot"
search_string =  sys.argv[1] 
db = MySQLdb.connect(host=HOST_NAME,
        user=USER_NAME,
        passwd=USER_PWD)

cursor = db.cursor()


cursor.execute("USE %s" % database_name)
cursor.execute("SHOW TABLES")

print "TABLE_NAME - COLUMN_NAME - COLUMN_CONTENT"
for table in cursor:
	table_name = table[0]
	query_column_name = "SELECT column_name FROM information_schema.columns WHERE table_name = '%s' and table_schema = '%s'" % (table_name, database_name)
	cursor.execute(query_column_name)

	for column in cursor:
		column_name = column[0]
		query_string = "SELECT %s FROM %s WHERE %s LIKE '%s'" % (column_name, table_name, column_name, search_string)
		cursor.execute(query_string)

		for row in cursor:
			print "%s - %s - %s " % (table_name, column_name, row[0])
