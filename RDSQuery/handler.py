import pymysql

endpoint = "database-migraciones.c5vthlbc6wtm.us-east-1.rds.amazonaws.com"
username = "admin"
password = "*************"
database_name = "migraciones"

connection = pymysql.connect(host=endpoint, user=username,
							 passwd=password, db=database_name)

def lambda_handler(event, context):
	cursor = connection.cursor()
	cursor.execute("SELECT * FROM OECD")

	rows = cursor.fetchall()

	for row in rows:
		print("{0} {1}".format(row[0], row[1]))