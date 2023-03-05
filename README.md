# Projects

1. Business Scenario
We are going to process Canadian Climate data.

Usually, when we want to download the weather data manually, we go to https://climate.weather.gc.ca/historical_data/search_historic_data_e.html.

But this time, you are going to use a data engineer way to download some data from Canada Climate website, and output one file.

2. Business Requirements
Please download the data from Canadian Climate.
Please concatenate the downloaded files into one final csv file, called all_years.csv. This is the output of the lab.
Please upload your scripts and final csv file all_years.csv to the repository you created in the Github.
3. Deliverable
You need to submit a shell script, a python script and all_years.csv to the github repository you created for the lab.

Shell script: You will use the shell script to control every operation, including data downloading, log setting, python script running.
Python script: While the Python script is used to concatenate all the data into one file.
all_years.csv: This is the output file you concatenate all the downloads.
4. Program Procedure
download data with shell command
concatenate data to one file with the python script
save output file in the python script
print out SUCCESS with shell command.
The Shell script will call the Python Script to finish the Python work.

5. Specification Detail
We only need the data of Station ID = 48549.
The year range of the data we want is from 2020 to 2022.
We only want the data in February.
The data will be downloaded in hourly format.
The output file will be named as all_years.csv.
