import json
import mysql.connector


mydb = mysql.connector.connect(
	host="wikireviews-staging-db.codti60ftvmt.us-east-1.rds.amazonaws.com",
	user="root",
	passwd="mRlxUJMhQW3",
	database="wr_beta"

)

db = mydb.cursor()

def get_id_under_business():

	# get id of category under Business
	db.execute("select id from wr_beta.taxonomy_taxonomy where parent_id = %s", (115,))
	category_id = db.fetchall()

	print((category_id))
	queue = []
	for _id in category_id:
		queue.append(_id[0])

	res = []
	while len(queue) > 0:
		curr_id  = queue.pop()
		res.append(curr_id)

		db.execute("select id from wr_beta.taxonomy_taxonomy where parent_id = %s", (curr_id, ))
		list_child_id = db.fetchall()

		# no childs => continue
		if len(list_child_id) == 0:
			continue

		else:

			for child in list_child_id:
				print('child: ', child)
				queue.append(child[0])

		pass

	return res


if __name__ == "__main__":
	print("START PROGRAM...")
	list_id = get_id_under_business()
	print(list_id)