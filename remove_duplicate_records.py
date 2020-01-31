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

db.execute("SET FOREIGN_KEY_CHECKS=0")


def duplicate_processor():
	
	low_id = 35491565
	high_id = low_id + 100

	while True:

		try:
			db.execute("SELECT * FROM wr_beta.reviews_userentries WHERE id >= %s and id < %s order by id desc", (low_id, high_id))
			result = db.fetchall()

		except Exception as e:
			print(e)
			pass
		else:
			pass
		finally:
			pass

		if len(result) == 0:
			break

		low_id = high_id
		high_id += 100
		for e in result:
			print(e)
			name = e[1]
			country = e[4]
			additional_info = e[7]
			content_type_id = e[8]
			slug = e[14]

			print(name, country, additional_info, content_type_id, slug)
			sql = "SELECT * FROM wr_beta.reviews_userentries WHERE id >= 35491565 and name = %s and country = %s and additional_info = %s and content_type_id = %s and slug = %s order by id desc"
			try:
				db.execute(sql, (name, country, additional_info, content_type_id, slug))
				records = db.fetchall()
				print("number of records = ", len(records))

				list_of_ids = []
				for i in range(len(records)):
					if i == 0:
						print("i = ", records[i][0])
						continue

					list_of_ids.append(records[i][0])


				format_strings = ','.join(['%s'] * len(list_of_ids))

				# DELETE RECORDS IN users_address
				db.execute("DELETE FROM wr_beta.users_address WHERE entries_id IN (%s)" % format_strings, tuple(list_of_ids))
				mydb.commit()

				# DELETE RECORDS IN reviews_website
				db.execute("DELETE FROM wr_beta.reviews_website WHERE entries_id IN (%s)" % format_strings, tuple(list_of_ids))
				mydb.commit()

				# DELETE RECORDS IN reviews_phone
				db.execute("DELETE FROM wr_beta.reviews_phone WHERE entries_id IN (%s)" % format_strings, tuple(list_of_ids))
				mydb.commit()

				# DELETE RECORDS IN reviews_userentries
				db.execute("DELETE FROM wr_beta.reviews_userentries WHERE id IN (%s)" % format_strings, tuple(list_of_ids))
				mydb.commit()


			except Exception as e:
				print(e)
			else:
				pass
			finally:
				pass

		pass

if __name__ == "__main__":

	duplicate_processor()
	print("DONE UPDATE !!!")