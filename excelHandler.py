
from pyexcel_ods import get_data
import json
import mysql.connector


mydb = mysql.connector.connect(
	host="52.0.121.30",
	user="root",
	passwd="wiki@abc",
	database="wr_staging"  #"category_mapping"

)
db = mydb.cursor()

data = get_data("data_categories.ods")
# print(data["WR Biz Taxonomy Yelp Mapping"])

# test call procedure
print("CALL PROCEDURE...")
db.callproc('add_taxonomy_test', ('TEST01' , 1,'TEST01', 1,1,1,1))
procRes = db.fetchone()
print(procRes)
print("FINISH CALLING PROCEDURE...")


logFile = open("log.txt","w")
newCreatedCnt = 0
for e in data["WR Biz Taxonomy Yelp Mapping"]:
	# print("e = ", e)
	e = e + [''] * (10- len(e))
	found = False
	index = -1
	for i, value in reversed(list(enumerate(e))):
		if value == 1:
			index = i
			found = True
			break
	if found:
		print("e = ", e, "index = ", index)
		category = ''
		category = e[index - 5].strip()

		# find ids of parent categories
		parent = e[index - 6].strip()
		id_list = []
		sql = "select * from wr_staging.test_taxonomy_taxonomy where category = %s"
		val = (parent,)

		try:
			db.execute(sql, val)
			# list of ids will be fetched
			result = db.fetchall()

			# there are two cases here:
			"""
			if parent is not existing  => create parent then get the id of parent => insert into DB
			else => traverse a result list => insert every single item into DB
			"""
			if len(result) == 0:
				#insert parent into DB
				db.callproc('add_taxonomy_test', (parent , 1, parent, 1,1,1,1))

				# GET ID of parent
				get_id_parent_sql = "select id from wr_staging.test_taxonomy_taxonomy where category = %s"
				get_id_parent_val = (parent,)

				try:
					db.execute(get_id_parent_sql, get_id_parent_val)
					id_parent = db.fetchall()

					# insert sub-category into db with new created parent
					db.callproc('add_taxonomy_test', (category , id_parent, category, 1,1,1,1))
					newCreatedCnt += 1

				except Exception as e:
					logFile.write(e)
					raise
				else:
					pass
				finally:
					pass

			else:
				for res in result:
					print(category, '=>' ,res)
					try:
						db.callproc('add_taxonomy_test', (category , res[0], category, 1,1,1,1))
						newCreatedCnt += 1
					except Exception as e:
						logFile.write(e)
						raise
					else:
						pass
					finally:
						pass

		except Exception as e:
			logFile.write(e)
			raise
		else:
			pass
		finally:
			pass

print('newCreatedCnt = ', newCreatedCnt)
file = open("result.txt","w")
file.write(json.dumps(data))

print("DONE")

# 9 - 4
# 8 - 3
# 7 - 2
# 6 - 1