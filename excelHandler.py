
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
	e = e[1:]
	found = False
	index = -1
	for i, value in reversed(list(enumerate(e))):
		if value == 1:
			index = i
			found = True
			break
	if found:
		print("e = ", e, "index = ", index)
		parentId = 115
		category = e[0].strip()

		sql = "select id from wr_beta.taxonomy_taxonomy where category = %s and parent_id = %s"
		val = (category, parentId)

		try:
			db.execute(sql, val)
			result = db.fetchall()

			if len(result) == 0:
				print("no existing")

				# create new
				db.callproc('add_taxonomy', (category , parentId, category, 1,0,1,None))
				logFile.write("new PARENT created: " + category + " with parentId = " + str(parentId))
				newCreatedCnt += 1
				# get id of the new one
				db.execute("select id from wr_beta.taxonomy_taxonomy where category = %s and parent_id = %s", (category, parentId))
				new_id = db.fetchall()

				# update parentId
				parentId = new_id[0][0]

			else:
				for res in result:
					print("res: ", res)

					parentId = res[0]

			for i in range(1, 4):
				print("current: ", e[i], "i: ", i)

				category = e[i].strip()

				if category == '': break
				# check the current category is existing or not

				print("parentId = ", parentId, "category = ", category)
				db.execute("select id from wr_beta.taxonomy_taxonomy where category = %s and parent_id = %s", (category, parentId))
				curr_result = db.fetchall()
				if len(curr_result) > 0:
					print("curr_result = ", curr_result)
					parentId = curr_result[0][0]
					continue
				else:
					db.callproc('add_taxonomy', (category , parentId, category, 1,0,1,None))
					db.execute("select id from wr_beta.taxonomy_taxonomy where category = %s and parent_id = %s", (category, parentId))
					newCreatedCnt += 1

					logFile.write("new created: " + category + " with parentId = " + str(parentId))
					new_id = db.fetchall()

					parentId = new_id


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

