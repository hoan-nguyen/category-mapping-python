
from pyexcel_ods import get_data
import json
import mysql.connector


mydb = mysql.connector.connect(
	host="wikireviews-staging-db.codti60ftvmt.us-east-1.rds.amazonaws.com",
	user="root",
	passwd="mRlxUJMhQW3",
	database="wr_beta"

)

db = mydb.cursor()

data = get_data("OSM Category and Subcategories.ods")

# test call procedure
print("CALL PROCEDURE...")
db.execute("select * from wr_beta.taxonomy_taxonomy where category = %s and parent_id = %s", ('Leisure, Sport and Gaming', 5520,))

check = db.fetchall()
print('check = ', check)

procRes = db.fetchone()
print(procRes)
print("FINISH CALLING PROCEDURE...")

def addCategories(line):
	# categories = [line[2], line[3], line[4], line[5]]
	line.pop()
	categories = line[1: len(line)]

	for i in range(len(categories)):
		category = categories[i].strip()
		if category == '':
			continue

		# handle level 1
		if i == 0:
			db.execute("select * from wr_beta.taxonomy_taxonomy where category = %s and parent_id = %s", (category, 1,))
			res = db.fetchall()

			if len(res) > 0:
				continue
			else:
				db.callproc('add_taxonomy', (category , 1, category, 1,0,1, None))
				continue

		# handle from level 2 ->  5
		parent_category = line[i]
		get_parent_id_sql = "select id from wr_beta.taxonomy_taxonomy where category = %s"

		try:
			db.execute(get_parent_id_sql, (parent_category,))
			parent_id_list = db.fetchall()

			for parent_id in list(parent_id_list):

				# check category is existing or not
				sql_cmd = "select * from wr_beta.taxonomy_taxonomy where category = %s and parent_id = %s"
				val = (category, parent_id[0])

				db.execute(sql_cmd, val)
				res = db.fetchall()

				if len(res) > 0:
					continue
				else:
					db.callproc('add_taxonomy', (category , parent_id[0], category, 1,0,1, None))

		except Exception as e:
			raise
		else:
			pass
		finally:
			pass


logFile = open("OSMlog.txt","w")
newCreatedCnt = 0

for e in data["OSM Taxonomy"]:
	e = e + [''] * (6- len(e))
	
	if  e[5] != '' and e[5] != 'id':
		print("e = ", e)

		addCategories(e)

print("DONE PROCESSING")

