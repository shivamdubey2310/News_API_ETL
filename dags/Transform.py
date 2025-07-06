import pyspark.sql as ps
import pyspark.sql.functions as psf
import pyspark.sql.types as pst
from dotenv import load_dotenv
import os
import requests
import json
import logging
import pandas as pd
import datetime
import time

# For loading .env
load_dotenv()

logging_path = os.getenv("LOGGING_PATH")

# Customizing logging.basicConfig() to format logging 
logging.basicConfig(
    level = logging.DEBUG,
    filename = f"{logging_path}/ETL_Logs.log",
    encoding = "utf-8",
    filemode = "a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)


def transformation_func():
    """Main transformation function"""
    
    logging.info("Starting transforming data")
    