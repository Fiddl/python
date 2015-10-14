"""
    Joins zipped time-stamped CSV files into single CSV. Two additional columns are created "Start Time" and "End Time"
    and the time values are added to each CSV row.

    Example:
    YYYY-MM-DD_HH_mm_SS_HH_mm_SS.zip

    INPUT:
    2015-10-01_00_00_00_00_10_00.zip [zipped CSV files capturing data from October 1st, 2015 from 12:00am to 12:10am]
    2015-10-01_01_00_00_01_10_00.zip [zipped CSV files capturing data October 1st, 2015 from 1:00am to 1:10am]
    2015-10-01_01_00_00_01_10_00.zip [zipped CSV files capturing data October 1st, 2015 from 2:00am to 2:10am]
    ...
    2015-10-01_01_00_00_01_10_00.zip [zipped CSV files capturing data October 1st, 2015 from 11:50pm to 12:00am]

    Script will combine all zipped files defined over a regular expression into a single CSV file.

    OUTPUT:
    CONTAINING_FOLDER_NAME.csv [The first 10 minutes of data captured every hour on October 1st, 2015
"""
__author__='Nathan Edwards'
from ctypes import *
from Tkinter import *
import sys
import zipfile
import datetime
import os
import re
import csv
import datetime

if (len(sys.argv) < 2):
    print 'usage: python join.py <regular expression>'

expr = sys.argv[1]+'.zip'
output_name = os.path.relpath(".","..")+'.csv' 
regex = re.compile(expr)
files = [f for f in os.listdir('.') if re.search(regex, f)]

# unzip files
for zip_filename in files:
    zf = zipfile.ZipFile(zip_filename, 'r')
    for zfilename in zf.namelist():
        newFile = open (zfilename, "wb")
        newFile.write (zf.read (zfilename))
    newFile.close()
    zf.close()

#add timestamp
first_file = 1
for file in [f for f in os.listdir('.') if re.search (r'.*.csv', f)]:
    if (os.stat(file).st_size != 0):
        with open(file, 'r') as csvinput:
            with open(output_name, 'a')as csvoutput:
                if (first_file == 1):
                    filename = os.path.split(file)
                    time_values = filename[1].split("_")
                    print time_values
                    date = re.sub('-', '', time_values[0])
                    start_time = re.sub('-', '', time_values[1])
                    end_time = re.sub('[-OUT.csv]', '', time_values[2])
                    writer = csv.writer(csvoutput, lineterminator='\n')
                    reader = csv.reader(csvinput)
    
                    start = datetime.datetime.strptime(date+start_time, "%Y%m%d%H%M%S")
                    end = datetime.datetime.strptime(date+end_time, "%Y%m%d%H%M%S")
    
                    print start
                    print end
    
                    all = []
                    row = next(reader)
                    row.append('Start Time')
                    row.append('End Time')
                    all.append(row)
    
                    for row in reader:
                        row.append(start)
                        row.append(end)
                        all.append(row)
    
                    writer.writerows(all)
                    csvinput.close()
                    os.remove(file)
                    first_file = 0
                else:
                    filename = os.path.split(file)
                    time_values = filename[1].split("_")
                    print time_values
                    date = re.sub('-', '', time_values[0])
                    start_time = re.sub('-', '', time_values[1])
                    end_time = re.sub('[-OUT.csv]', '', time_values[2])
                    writer = csv.writer(csvoutput, lineterminator='\n')
                    reader = csv.reader(csvinput)
    
                    start = datetime.datetime.strptime(date+start_time, "%Y%m%d%H%M%S")
                    end = datetime.datetime.strptime(date+end_time, "%Y%m%d%H%M%S")
    
                    print start
                    print end
    
                    row = next(reader) #skip header
                    all = []
                    row = next(reader)
                    for row in reader:
                        row.append(start)
                        row.append(end)
                        all.append(row)
    
                    writer.writerows(all)
                    csvinput.close()
                    os.remove(file)
