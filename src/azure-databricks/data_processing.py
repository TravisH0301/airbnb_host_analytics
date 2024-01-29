###############################################################################
# Name: data_processing.py
# Description: This script processes the raw Airbnb datasets and load the 
#              compiled dataset into the silver layer of ADLS gen2
#              as a delta lake table.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
import os
os.system("pip install pyyaml")
import yaml
from functools import reduce

import pandas as pd
from pyspark.sql import DataFrame

from utils.utils import set_azure_storage_config


def load_compile_data(snapshot_dates: list, source_location: str):
    """This function loads the raw Airbnb datasets from 
    the bronze layer of the ADLS. The datasets are then
    compiled and returned as a dataframe.
    
    Parameters
    ----------
    snapshot_date: list
        list of snapshot dates in string
    source_location: str
        Source file location of ADLS
        
    Returns
    -------
    dataframe
    """
    df_raw_list = []
    for snapshot_date in snapshot_dates:
        df_raw = spark.read.format("parquet") \
            .load(source_location.format(snapshot_date))
        df_raw_list.append(df_raw)

    return reduce(DataFrame.unionAll, df_raw_list)


def process_data(df, query):
    """This function processes the given dataset
    using Spark SQL and returns a dataframe.
    
    Parameters
    ----------
    df: dataframe
        Spark dataframe
    query: str
        SQL query to execute
        
    Returns
    -------
    dataframe
    """
    df.createOrReplaceTempView("airbnb_raw")
    df_processed = spark.sql(query)

    return df_processed


def main():
    print("Process has started.")

    # Configure storage account credentials
    print("Configuring storage account credentials...")
    set_azure_storage_config(spark, dbutils)

    # Load raw Airbnb datasets
    print("Loading raw Airbnb datasets...")
    snapshot_dates = [
        "2023-03-13",
        "2023-04-09",
        "2023-05-13",
        "2023-06-06"
    ]
    source_location = f"abfss://airbnb-host-analytics@{storage_account_name}" \
        ".dfs.core.windows.net/bronze/raw_dataset_{}.parquet"
    df_raw_compiled = load_compile_data(snapshot_dates, source_location)

    # Process raw dataset
    """
    The raw dataset is processed and filtered with the following conditions:
    - Host identity must be verified for valid hosts.
    - Room availability for next 60, 90 and 365 days must not be zero to ensure
      only available listings are considered.
    - Municipality of the listing must be metropolitan municipalities to eliminate
      rural area effect.
    - Listing must be an entire home/apt to limit diversity of the listing type.
    - Price must be within the range between median and upper limit (median + 1.5 IQR)
      to filter out outliers and reduce price factor.
    
    *Metropolitan Melbourne municipalities: Banyule, Bayside, Boroondara, Brimbank,
    Cardinia, Casey, Darebin, Frankston, Glen Eira, Greater Dandenong, Hobsons Bay,
    Hume, Kingston, Knox, Manningham, Maribyrnong, Maroondah, Melbourne, Melton,
    Monash, Moonee Valley, Moreland, Mornington Peninsula, Nillumbik, Port Phillip,
    Stonnington, Whitehorse, Whittlesea, Wyndham, Yarra, Yarra Ranges
    """
    print("Processing raw dataset...")
    ## Load data processing query
    with open("./conf/sql.yaml") as f:
        conf = yaml.safe_load(f)
        query = conf["data_processing"]["airbnb_processed"]
    ## Apply processing query
    df_airbnb_processed = process_data(df_raw_compiled, query)

    # Store processed dataset as delta lake table in silver layer
    print("Saving Delta Lake tables in silver layer...")
    target_location = f"abfss://airbnb-host-analytics@{storage_account_name}" \
        ".dfs.core.windows.net/silver/airbnb_processed"
    df_airbnb_processed.write.format("delta").mode("overwrite").save(target_location)
    
    print("Process has completed.")

if __name__ == "__main__":
    main()