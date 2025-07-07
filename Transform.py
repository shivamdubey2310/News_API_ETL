import logging
import os
from dotenv import load_dotenv
import pyspark.sql as ps
import pyspark.sql.functions as psf
import pyspark.sql.types as pst
import pandas as pd

# For loading .env
load_dotenv()

logging_path = os.getenv("LOGGING_PATH")

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

def creating_spark_session():
    """This function creates Spark Session
    Return:
    ----------
        spark - sparkSessionObject
    """
    try:
        spark = (ps.SparkSession
         .builder
         .appName("Transform_News_API")
         .getOrCreate()
        )
        return spark
        
    except Exception as e:
        logging.info(f"An error occurred while creating spark session - {e}")
        print("Trasformation failed")
        return None
 

def data_cleaning(spark):
    """Reads and Cleans data
    parameters:
    -----
        spark - sparkSessionObject
    
    Returns:
    ------
        df - spark Dataframe(cleaned)
    """

    logging.info("Cleaning Data")

    file_path = os.getenv("PARQUET_PATH")
    if file_path is None:
        raise ValueError("PARQUET_PATH environment variable is not set")
    
    file_name = os.path.join(file_path, "data.parquet")
    df = (spark
          .read
          .format("parquet")
          .load(file_name)
    )

    # Dropping useless column
    df = df.drop("__index_level_0__")

    # Handling Nulls
    df = df.fillna("unknown")
    
    # Handling duplicates
    df = df.dropDuplicates()

    return df


def converting_string_to_datetime(df):
    """This function converts publishedAt string to pandas Datetime
    parameters:
    ---------
        df : spark DataFrame
    Returns :
    --------
        df : spark DataFrame (converted)
    """
    
    # Defining UDF 
    def strtodatetime_fun(publishedAt: pd.Series) -> pd.Series:
        return pd.to_datetime(publishedAt, utc=True)

    # Making pandas UDF
    strtodatetime_udf = psf.pandas_udf(strtodatetime_fun, returnType=pst.TimestampType())
    
    # applying UDF
    df = df.withColumn("publishedAt", strtodatetime_udf(psf.col("publishedAt")))
    
    return df


def saving_to_file(spark, df):
    """
        Function to save the spark df back to a parquet file.
    Parameters:
    ---------
        spark : spark SparkSession
            sparkSession Object

        df : spark dataFrame
            dataframe to save into a file
    Returns:
    --------
        is_successful : bool
        status of the operation
    """

    is_successful = False
    
    try:
        file_path = os.getenv("PARQUET_PATH")
        if file_path is None:
            raise ValueError("PARQUET_PATH environment variable is not set")
        
        file_name = os.path.join(file_path, "data_transformed.parquet")
        
        (
            df.write.format("parquet")
            .mode("overwrite")
            .option("compression", "snappy")
            .save(file_name)
        )
        is_successful = True

    except Exception as e:
        logging.error(f"An error occurred while saving df in data_transformed.parquet - {e}")
        is_successful = False

    return is_successful


def transformation_func():
    """Main transformation function"""
    

    logging.info("Starting transforming data")
    spark = creating_spark_session()

    if spark is None:
        return
    
    logging.info("Starting data cleaning!!!")    
    df = data_cleaning(spark)
    
    logging.info("Converting string to datetime cleaning!!!")    
    df = converting_string_to_datetime(df)

    # Saving df to a parquet file
    logging.info("Trying to save dataframe back to a parquet file.")
    is_successful = saving_to_file(spark, df)
    
    if is_successful:
        logging.info("Transformation completed")
    else:
        logging.error("An Error occurred in transformation")
        raise ValueError("Transformation failed")