#!/bin/bash

###############################################
# Part 1: SET DEFAULT VARIABLES
filenametime1=$(date +"m%d%Y%H%%M%S")


###############################################
# Part 2: SET VARIABLES

export BASE_FOLDER='/home/naijadev/Projects/Toronto'
export SCRIPTS_FOLDER=${BASE_FOLDER}'/Scripts'
export INPUT_FOLDER=${BASE_FOLDER}'/Data_In'
export OUT_FOLDER=${BASE_FOLDER}'/Data_Out'
export LOGDIR=${BASE_FOLDER}'/Logs'
export SHELL_SCRIPT_NAME='shell_script'
#export LOG_FILE=${LOGDIR}/${SHELL_SCRIPT_NAME}_${filenametime1}.log



################################################
# Part 3:   GO TO SCRIPTS FOLDER AND RUN SCRIPT

cd ${SCRIPTS_FOLDER}


################################################
# Part 4: SET LOG RULES

exec > >(tee ${LOG_FILE}) 2>&1

################################################
# Part 5: DOWNLOAD DATA

echo "Start download data"

for year in {2020..2022};
do wget -N --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=48549&Year=${year}&Month=2&Day=14&timeframe=1&submit= Download+Data" -O ${INPUT_FOLDER}/${year}.csv;
done;

###############################################
# Part 6: CHECKING FOR ERRORS IN BASH SCRIPT

RC1=$?
#RC1 =0 ==> Script run is successful

if [${RC1} != 0]; then
    echo "DOWNLOAD DATA FAILED"
    echo "[ERROR:] RETURN CODE:  ${RC1}"
    echo "[ERROR:] REFER TO THE LOG FOR THE REASON FOR THE FAILURE."
    exit 1
fi

##################################################
# Part 7: RUN PYTHON
echo "Start to run Python Script"
python3 ${SCRIPTS_FOLDER}/python_script.py

###############################################
# Part 8: CHECKING FOR ERRORS IN PYTHON SCRIPT

RC1=$?
#RC1 =0 ==> Script run is successful

if [${RC1} != 0]; then
    echo "DOWNLOAD DATA FAILED"
    echo "[ERROR:] RETURN CODE:  ${RC1}"
    echo "[ERROR:] REFER TO THE LOG FOR THE REASON FOR THE FAILURE."
    exit 1
fi

echo "PROGRAM SUCCEEDED"

exit 0