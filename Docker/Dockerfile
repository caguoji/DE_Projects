FROM python:3.7

#path for the container
WORKDIR /Lab2

#1st step to mount volume, 2nd step in terminal
VOLUME /Lab2/output/
VOLUME /Lab2/input/

#Add python script to image and container
ADD py_script.py /Lab2

#Include environment variables used in python script
ENV INPUT_DIR="/Lab2/input" 
ENV OUTPUT_DIR="/Lab2/output"

#Install pandas
RUN pip install pandas==1.3.5

#keeps the container running
CMD tail -f /dev/null

#spinning up container
# docker run 
# --rm  --> removes any container of the same name
# -t --> tells container to use linux system
# -d --> detached mode, if not detached the process happens in current terminal not in background
# -v --> needs 2 path names, 1st is path for virtual machine, 2nd is path for container,  
# image name --> name tagged when created image