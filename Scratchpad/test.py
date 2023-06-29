import json
import os
import csv

# with open("student.json","r") as f:
#     data = json.load(f)
#     print(data)

# print(os.path.dirname(__file__))

with open("student.csv","r") as f:
    reader = csv.reader(f)
    for row in reader:
        print(row)

my_list = [1,2,3,4]
_lst = iter(my_list)
next(_lst)
for i in _lst:
    print(i)