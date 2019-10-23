from data_loading_utils import read_lines_from_file_as_data_chunks
import json
import mysql.connector
import os
import multiprocessing

CHUNK_SIZE = 10000 # configure this variable depending on your machine's hardware configuration

mydb = mysql.connector.connect(
	host="52.0.121.30",
	user="root",
	passwd="wiki@abc",
	database="category_mapping"
)


# callback method
def process_lines(data, eof, file_name):
	
	if not eof:
		temp = json.loads(data)
		categories = []
		business_info = ""

		if "categories" in temp:
			categories = listProcessor(temp["categories"])
		if "business_info" in temp:
			business_info = temp["business_info"]
		db = mydb.cursor()
		sql = "INSERT INTO categories_raw (categories, business_info) VALUES (%s, %s)"

		if len(categories) > 0:
			print("command")

			for e in categories:
				val = (e, str(business_info))
				try:
					db.execute(sql, val)
					mydb.commit()
				except Exception as e:
					print('val: ', val)
					raise
				else:
					pass
				finally:
					pass
	else:
		print("DONE PROCESSING: ", file_name)
		os.rename(file_name, file_name + '.txt')

    	

def listProcessor(arr):
	res = []
	for i in range(len(arr)):
		for j in range(len(arr[i])):
			res.append(arr[i][j][0])
	return res

def listAllFiles(path):
 	arr = os.listdir(path)
 	return arr


if __name__ == "__main__":

	path = "data_wiki"
	res = listAllFiles(path)

	p = multiprocessing.Pool()
	for file in res:
		p.apply_async(read_lines_from_file_as_data_chunks(path + "/" + file, chunk_size=CHUNK_SIZE, callback=process_lines), [file])

	p.close()
	p.join()

	print("DONE PROCESSING")
