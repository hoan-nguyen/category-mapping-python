import os
import math

# def folder_size(path='.'):
#     total = 0
#     for entry in os.scandir(path):
#         if entry.is_file():
#             ext = os.path.splitext(entry)[-1].lower()
#             if '.txt' in ext:
#             	print(entry)
#             	total += entry.stat().st_size
#         elif entry.is_dir():
#             total += folder_size(entry.path)
#     return total

def get_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)

            if '.txt' in fp:
            	print(fp)
            	total_size += os.path.getsize(fp)
    return total_size

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])

if __name__ == "__main__":
	# print(convert_size(folder_size("data_wiki")))
	print(convert_size(get_size("/data_wiki")))