#!/bin/bash

set -e    # For exiting on error
NOW=`date '+%F %H:%M:%S'`;  # A variable for current date and time

mkdir -p logs
echo "$NOW - INFO - Starting Pipeline" >> logs/ETL_Logs.log
source /home/shiva/venv/bin/activate 
python3 News_API_ETL.py

echo "$NOW - INFO - Pipeline Executed" >> logs/ETL_Logs.log