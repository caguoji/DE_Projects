import pandas as pd
import glob
import os


#variables
input_path = os.environ.get('INPUT_FOLDER')
output_path = os.environ.get('OUT_FOLDER')
filename = 'all_years.csv'
find_csv_files = glob.glob(os.path.join(input_path,'*csv'))

#list to collect all files
_all_data = []

for file in find_csv_files:
    print("Retrieving this file "+ file)
    data = pd.read_csv(file)
    _all_data.append(data)

print("Concatenating the files")
all_years = pd.concat(_all_data)

print("Saving the output")
all_years.to_csv(output_path+"/"+filename)


