from dotenv import load_dotenv
import os
import logging
import pandas as pd
import datetime
import pymongo

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


def establishing_connection():
    """Establishing connection to mongodb
    Returns:
    ------
        myClient - Mongodb Client
    """

    logging.info("Trying to establish connection to mongodb")
    myClient = None

    try:
        connection_string = os.getenv("CONNECTION_STRING")
        myClient = pymongo.MongoClient(connection_string)
        # Verifying connection
        db_list = myClient.list_database_names()
    
    except Exception as e:
        logging.error(f"Any other error occured while establishing connection to mongodb : {e}")

    else:
        logging.info(f"Connection Established successfully to mongodb")    

    return myClient


def creating_db_and_collection(myClient):
    """Creating database and collections
    params
    --------
        myClient(connection_object): Mongodb connection object
    returns
    ------
        coll_variable : variable of collection name
    """

    logging.info(f"Creating db and collections")

    try:
        db_name = os.getenv("DATABASE_NAME")
        db_variable = myClient[db_name]

        collection_name = datetime.datetime.now().strftime("%Y_%m_%d")        
        coll_variable = db_variable[collection_name]

    except Exception as e:
        logging.error(f"An error occured when creating db and collections: {e}")
    
    logging.info(f"Creating db Successful!!!")

    return coll_variable


def loading_data(myClient, coll_variable):
    """Creating database and collections
    Params
    --------
        myClient(connection_object): Mongodb connection object
    """

    try:
        file_path = os.getenv("PARQUET_PATH")
        if file_path is None:
            raise ValueError("PARQUET_PATH environment variable is not set")
            
        file_name = os.path.join(file_path, "data_transformed.parquet")
        df = pd.read_parquet(file_name)
        
        df_dict = df.to_dict('records')

        # inserting data
        coll_variable.insert_many(df_dict)
        return True

    except Exception as e:
        logging.error(f"Data Loading unsuccessful- {e}")
        return False
    

def loading_func():
    myClient = establishing_connection()
    
    if myClient is None:
        logging.error("Failed to establish MongoDB connection")
        raise ValueError("Loading failed - MongoDB connection error")

    coll_variable = creating_db_and_collection(myClient)

    result = loading_data(myClient, coll_variable)

    if result:
        logging.info("Data Loading successfull")
    else:
        logging.error("Data loading unsuccessfull")
        raise ValueError("Loading failed")