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
f = open('update_active_log.txt', 'w+')


# NOTE: Only remove the categories under Business(id 115)
def removeCategoriesFromTree(category):

	# get id of category under Business
	db.execute("select id from wr_beta.taxonomy_taxonomy where category = %s and parent_id = %s", (category, 115))
	category_id = db.fetchall()

	queue = [category_id[0][0]]

	while len(queue) > 0:
		curr_id  = queue.pop()

		db.execute("select id from wr_beta.taxonomy_taxonomy where parent_id = %s", (curr_id, ))
		list_child_id = db.fetchall()

		# no childs => remove
		if len(list_child_id) == 0:
			print('curr_id: ', curr_id)

			# TODO
			# REMOVE FROM DATABASE
			db.execute("delete from wr_beta.taxonomy_taxonomy where id = %s", (curr_id, ))

		else:
			# we add curr_id to queue again cuz it still has reference
			queue.append(curr_id)

			for child in list_child_id:
				print('child: ', child)
				queue.append(child[0])

		pass

def update_isactive_categories(category):
	try:
		
		db.execute("select id from wr_beta.taxonomy_taxonomy where category = %s and parent_id = %s", (category, 115))
		category_id = db.fetchall()

		if len(category_id) != 0:
			print('category needs to be updated = ', category, ' with id = ', category_id[0][0])
			sql = 'update wr_beta.taxonomy_taxonomy set is_active = 0 where id = %s'
			db.execute(sql, (category_id[0][0], ))
			mydb.commit()
			
			f.write(category)
	except Exception as e:
		print(str(e))
		# raise
	else:
		pass
	finally:
		pass


def getCategoriesFromDB():
	sql = "select category from wr_beta.taxonomy_taxonomy where parent_id = 115"

	arr = []
	try:
		db.execute(sql)
		result = db.fetchall()

		for element in result:
			arr.append(element[0])

	except Exception as e:
		raise
	else:
		pass
	finally:
		pass

	return list(set(arr))

def getCategoriesFromFile(file):
	data = get_data(file)

	categories = set()
	for e in data["WR Biz Taxonomy Yelp Mapping"]:
		e = e + [''] * (10- len(e))

		if e[1].strip() == '': continue
		# print(e)
		categories.add(e[1].strip())

	return list(categories)



if __name__ == "__main__":
	# print("START PROGRAM...")
	# update_isactive_categories('Forestry and Logging')
	
	categories_in_file = set(getCategoriesFromFile("data_categories.ods"))
	categories_in_db = getCategoriesFromDB()

	print(categories_in_file)
	print(categories_in_db)

	need_to_update_list = set()
	for category in categories_in_db:
		if category not in categories_in_file:
			need_to_update_list.add(category)

	need_to_update_list = list(need_to_update_list)
	for category in need_to_update_list:
		update_isactive_categories(category)

	print("DONE UPDATE !!!")
