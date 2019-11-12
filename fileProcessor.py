from data_loading_utils import read_lines_from_file_as_data_chunks
import json
import mysql.connector
import os
import multiprocessing
from ast import literal_eval

CHUNK_SIZE = 10000 # configure this variable depending on your machine's hardware configuration

mydb = mysql.connector.connect(
	host="52.0.121.30",
	user="root",
	passwd="wiki@abc",
	database="category_mapping"
)
db = mydb.cursor()

f = open('category_log.txt', 'w+')

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
		# db = mydb.cursor()
		sql = "INSERT INTO categories_US (categories, business_info) VALUES (%s, %s)"

		if len(categories) > 0:
			print("processing in a file : " , file_name)

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


def duplicateProcessor(val):
	# db = mydb.cursor()
	# val = ('Bars', )
	sql = "select * from category_mapping.categories_US where categories = %s"

	dict = {}
	try:
		db.execute(sql, val)
		result = db.fetchall()

		for x in result:
			if x[2] == '':
				# print('haha: ', x)
				continue

			try:
				s = str(literal_eval(x[2]))
				if '[' in s:
					# print('===== ', s)
					s = squareBracketHandler(s)
					s = '{'+s+'}'
					# continue

				# handle string to remove the special character
				s  = stringHandler(s)
				s = s.replace('\'', '\"')
				obj = json.loads(str(s))

				# print("after processing string: ",s)
				# print('obj=',obj)
				
				temp_keys = list(obj.keys())

				for key in temp_keys:
					if key not in dict:
						dict[key] = [obj[key]]
					else:
						t = dict[key]
						t.append(obj[key])
						dict[key] = list(set(t))

				# print('dict:',dict)

			except Exception as e:
				print('err : ', e, 'val : ', val, 'literal_eval(x[2]):',literal_eval(x[2]))
				print('s =',s)
				f.write(str(val))
				f.write("\n")
				# raise
			else:
				pass
			finally:
				pass
		print('dict:',dict)

		# delete old data
		deleteCmd = "delete from category_mapping.categories_US where categories = %s"
		db.execute(deleteCmd, val)

		# insert new data
		insertCmd = "INSERT INTO categories_US (categories, business_info) VALUES (%s, %s)"
		insertValues = (val[0], str(dict))

		db.execute(insertCmd, insertValues)
		mydb.commit()

		return dict
		# mydb.close()

	except Exception as e:
		f.write(str(val))
		f.write("\n")
		# raise
	else:
		pass
	finally:
		# mydb.close()
		pass

def stringHandler(s):
	res = ""
	for i in range(len(s)):
		if s[i] == 'u' and s[i+1] == "'":
			if s[i+2] == "," or s[i+2] == '}' or s[i+2] == ':' or s[i+2] == ' ':
				res += s[i]
			else:
				continue
		else:
			res += s[i]
	return res

def squareBracketHandler(s):
	flag = False
	res = ''
	for i in range(len(s)):
		if s[i] == '[':
			flag = True
			continue
		if s[i] == ']':
			flag = False
			continue
		if s[i] == ',' and flag:
			res += ':'
		else:
			res += s[i]
	return res


def getAllKeys():
	# db = mydb.cursor()
	sql = "select categories from category_mapping.categories_US"

	arr = []
	try:
		db.execute(sql)
		result = db.fetchall()

		for element in result:
			# arr.append(element[0])
			arr.append(element)

	except Exception as e:
		raise
	else:
		pass
	finally:
		pass

	return list(set(arr))


if __name__ == "__main__":
	print("START PROGRAM...")

	print(squareBracketHandler("[[u'Accepts Credit Cards', u'Yes'], [u'Offers Military Discount', u'Yes']]"))
	print(squareBracketHandler("[['Takes Reservations', 'Yes'], ['Delivery', 'No'], ['Take-out', 'Yes'], ['Accepts Credit Cards', 'Yes'], ['Good for Kids', 'Yes'], ['Good for Groups', 'Yes'], ['Alcohol', 'Beer & Wine Only'], ['Smoking', 'No'], ['Wi-Fi', 'Free'], ['Has TV', 'Yes'], ['Waiter Service', 'No']]"))
	te = '{"By Appointment Only": "No", "Legal ID": u""n/a""}'

	# print(stringHandler("{u'Accepts Apple Pay': u'Yes', u'By Appointment Only': u'No', u'Accepts Credit Cards': u'Yes'}"))
	# print(stringHandler("{u'uau': u'Yes', u'By Appointment Only': u'No', u'Accepts Credit Cards': u'Yesu'}"))
	# print(stringHandler("{u'uau': u'Yesu' , u'By Appointment Only': u'No', u'Accepts Credit Cards': u'Yesu'}"))

	s = "{'By Appointment Only': 'Yes', 'Accepts Bitcoin': 'Yes', 'Accepts Credit Cards': 'Yes', 'Bike Parking': 'Yes', 'Parking': 'Private Lot'}"
	s = s.replace('\'', '\"')
	obj = json.loads(str(s))
	print('obj:', obj)

	print(stringHandler("{u'By Appointment Only': u'Yes', u'Accepts Bitcoin': u'Yes', u'Accepts Credit Cards': u'Yes', u'Bike Parking': u'Yes', u'Parking': u'Private Lot'}"))

	# duplicateProcessor(('Property Management',))

	# path = "/data_wiki"
	# res = listAllFiles(path)

	# p = multiprocessing.Pool()
	# for file in res:
	# 	print('file: ', file)
	# 	if '.txt' in file:
	# 		continue
	# 	else:
	# 		p.apply_async(read_lines_from_file_as_data_chunks(path + "/" + file, chunk_size=CHUNK_SIZE, callback=process_lines), [file])

	# p.close()
	# p.join()

	# print("DONE PROCESSING ALL FILES")
	f.write("start to handle ...")
	f.write("\n")
	# DUPLICATE HANDLE
	# keys = getAllKeys()
	# for e in keys:
	# 	# print(e)
	# 	duplicateProcessor(e)
	# duplicateProcessor(('Insurance', )) # Insurance Limos
	# print("PROCESSING DONE: ", len(keys))

	# val = "{'Has ATM': ['Yes'], 'By Appointment Only': ['Yes', 'No'], 'Open to All': ['Yes', 'No'], 'Gender Neutral Restrooms': ['Yes'], 'Offers Military Discount': ['Yes'], 'Accepts Credit Cards': ['Yes', 'No'], 'Accepts Cryptocurrency': ['Yes', 'No'], 'Accepts Insurance': ['Yes', 'No'], 'Accepts Apple Pay': ['Yes', 'No'], 'Accepts Google Pay': ['Yes', 'No'], 'Wi-Fi': ['Paid', 'No', 'Free'], 'Good for Kids': ['Yes'], 'Accepts Bitcoin': ['Yes', 'No'], 'Accepts Android Pay': ['No'], 'Take-out': ['Yes', 'No'], 'Bike Parking': ['Yes', 'No'], 'Parking': ['Private Lot', 'Valet, Street, Private Lot'], 'Wheelchair Accessible': ['Yes'], 'Caters': ['No'], 'Good for Working': ['Yes'], 'Outdoor Seating': ['Yes'], 'Dogs Allowed': ['Yes']}"

	# insertCmd = "INSERT INTO categories_US (categories, business_info) VALUES (%s, %s)"
	# insertValues = ('Insurance', val)

	# db.execute(insertCmd, insertValues)
	# mydb.commit()

	print("DONE PROCESSING!!!")
