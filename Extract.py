# Python file to extract data

from dotenv import load_dotenv
import os
import logging
import datetime

# For loading .env
load_dotenv()

logging_path = os.getenv("LOGGING_PATH")

if logging_path is not None and not os.path.exists(logging_path):
    os.makedirs(logging_path, exist_ok=True)

# Customizing logging.basicConfig() to format logging 
logging.basicConfig(
    level = logging.INFO,
    filename = f"{logging_path}/ETL_Logs.log",
    encoding = "utf-8",
    filemode = "a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

def getting_current_date():
    """
        This function returns today's date.
    Returns:
        now: str
        today's date
    """

    # Getting today's date
    now = datetime.datetime.now()
    now = datetime.datetime.strftime(now, "%Y-%m-%d")

    # Saving date in .env
    os.putenv("TODAY", now.replace("-", "_"))

    return now
    

def extraction_func():
    """A function to extract data from API and save it in a parquet file"""
    
    import requests
    import json
    import pandas as pd
    import time
    
    logging.info("Extracting data")
    
    API_KEY = os.getenv("API_KEY")
    curr_date = getting_current_date()
    BaseUrl = os.getenv("BASE_URL")

    try:
        articles_list = []
        pageNo = 1
        
        while True:
            url = f"{BaseUrl}&from={curr_date}&apiKey={API_KEY}&pageSize=10&page={pageNo}"
            response = requests.get(url)
            response_json = response.json()
            articles_json = response_json.get("articles", [])
            
            # Terminating if page not exists
            if not articles_json:
                break
            
            articles_list += articles_json

            pageNo += 1
            time.sleep(1)
    
        articles_df = pd.json_normalize(articles_list)
        articles_df = pd.DataFrame(articles_df)
        
        # Dropping useless column and renaming one
        articles_df.drop("source.id", axis="columns", inplace=True)
        articles_df["source_name"] = articles_df["source.name"]
        articles_df.drop("source.name", axis="columns", inplace=True)
        
        # Saving pandas dataframe as parquet
        file_path = os.getenv("PARQUET_PATH")
        if file_path is None:
            raise ValueError("PARQUET_PATH environment variable is not set")
        
        os.makedirs(file_path, exist_ok=True) 
        file_name = os.path.join(file_path, "data.parquet")
        articles_df.to_parquet(file_name, index=True)

    except requests.HTTPError as e:
        logging.error(f"HTTPError occurred - {e}")
        print("Extraction failed")
        raise ValueError("Extraction failed")

    except json.JSONDecodeError as e:
        logging.error(f"Json not decoded properly - {e}")
        print("Extraction failed")
        raise ValueError("Extraction failed")
    
    except Exception as e:
        logging.error(f"An error occurred - {e}")
        print("Extraction failed")
        raise ValueError("Extraction failed")

    logging.info("Extracted data successfully!!!")