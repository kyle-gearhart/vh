import time
import psycopg2
import sys

print "Testing postgres select time using psycopg2 \n"

conn_string = "host='10.132.71.48' dbname='vh_test' user='postgres' password='c7M2wy4gt36d3YD'"

print "Connecting to database\n ->%s" % (conn_string)

try:
	conn = psycopg2.connect(conn_string)
except Exception as inst:
	print "Unable to connect to database"
	print (type(inst))
	print (inst.args)
	print (inst)

cur = conn.cursor()

try:
	cur.execute("SELECT * FROM prefload")
except:
	print "Unable to select from prefload"

y = 0

while y < 1:
	rows = cur.fetchall()

	x = 0
	for row in rows:

		if x < 10:
			print (row)

		x = x + 1
	y = y + 1


print "All done"


