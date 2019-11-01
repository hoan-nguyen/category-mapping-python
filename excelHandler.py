
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

data = get_data("data_categories.ods")
# print(data["WR Biz Taxonomy Yelp Mapping"])

# test call procedure
print("CALL PROCEDURE...")
# db.execute("select * from wr_beta.taxonomy_taxonomy where category = 'Bingo Halls' , parent_id = 58")
# db.execute("select * from wr_beta.taxonomy_taxonomy where category = %s and parent_id = %s", ('Bingo Halls', 58))

db.execute("select * from wr_beta.taxonomy_taxonomy where category = %s and parent_id = %s", ('Internet', 296))
check = db.fetchall()
print('check = ', check)
# db.callproc('add_taxonomy', ('TEST01' , 1,'TEST01', 1,1,1,1))

# db.callproc('add_taxonomy', ('Farms' , 30995, 'Farms', 1,1,1,1))
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
		# print("e = ", e, "index = ", index)
		category = ''
		category = e[index - 5].strip()

		# find ids of parent categories
		parent = e[index - 6].strip()
		id_list = []
		sql = "select * from wr_beta.taxonomy_taxonomy where category = %s"
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
				db.callproc('add_taxonomy', (parent , 1, parent, 1,0,1,None))

				# GET ID of parent
				get_id_parent_sql = "select id from wr_beta.taxonomy_taxonomy where category = %s"
				get_id_parent_val = (parent,)

				try:
					db.execute(get_id_parent_sql, get_id_parent_val)
					id_parent = db.fetchall()

					# insert sub-category into db with new created parent
					db.callproc('add_taxonomy', (category , id_parent, category, 1,0,1,None))
					print("new parent created: ", category, id_parent)
					newCreatedCnt += 1
					logFile.write("new created: " + category)

				except Exception as e:
					logFile.write(str(e))
					# raise
				else:
					pass
				finally:
					pass

			else:
				for res in result:
					# print(category, '=>' ,res)
					try:

						db.execute("select * from wr_beta.taxonomy_taxonomy where category = %s and parent_id = %s", (category, res[0],))
						check = db.fetchall()

						if len(check) == 0:
							db.callproc('add_taxonomy', (category , res[0], category, 1,0,1,None))
							logFile.write("new created: " + category)
							newCreatedCnt += 1
							print("new created: ", category, res[0])
					except Exception as e:
						# print('res = ', res)
						logFile.write(str(e) +  ' => ' + str(res))
						# raise
					else:
						pass
					finally:
						pass

		except Exception as e:
			logFile.write(str(e))
			raise
		else:
			pass
		finally:
			pass

print('newCreatedCnt = ', newCreatedCnt)
file = open("result.txt","w")
# file.write(json.dumps(data))

print("DONE")

# 9 - 4
# 8 - 3
# 7 - 2
# 6 - 1
