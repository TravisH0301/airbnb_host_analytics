###############################################################################
# Name: data_processing.py
# Description: This script processes the raw Airbnb datasets and load the 
#              compiled dataset into the silver layer of ADLS gen2
#              as a delta lake table.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
from functools import reduce

import pandas as pd
from pyspark.sql import DataFrame

from utils import utils


def load_compile_data(
        spark,
        dbutils,
        container_name,
        file_path,
        file_type,
        snapshot_dates
    ):
    """This function loads the raw Airbnb datasets from 
    the bronze layer of the ADLS. The datasets are then
    compiled and returned as a dataframe.
    
    Parameters
    ----------
    spark: object
        Spark session
    dbutils: object
        Databricks util object
    container_name: str
        ADLS container name
    file_path: str
        Source file path in ADLS
    file_type: str
        Dataset storage format - e.g., parquet or delta
    snapshot_date: list
        list of snapshot dates in string
        
    Returns
    -------
    dataframe
    """
    df_raw_list = []
    for snapshot_date in snapshot_dates:
        df_raw = utils.load_data_to_df(
            spark,
            dbutils,
            container_name,
            file_path.format(snapshot_date),
            file_type
        )
        df_raw_list.append(df_raw)

    return reduce(DataFrame.unionAll, df_raw_list)


def main():
    logger.info("Process has started.")

    # Configure storage account credentials
    logger.info("Configuring storage account credentials...")
    utils.set_azure_storage_config(spark, dbutils)

    # Load raw Airbnb datasets
    logger.info("Loading raw Airbnb datasets...")
    snapshot_dates = [
        "2023-03-13",
        "2023-04-09",
        "2023-05-13",
        "2023-06-06"
    ]
    container_name, file_path, file_type = (
        "airbnb-host-analytics",
        "bronze/raw_dataset_{}.parquet",
        "parquet"
    )
    df_raw_compiled = load_compile_data(
        spark,
        dbutils,
        container_name,
        file_path,
        file_type,
        snapshot_dates
    )

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
    logger.info("Processing raw dataset...")
    ## Load data processing query
    query_name = "airbnb_processed"
    query = utils.get_query(query_name)
    ## Apply processing query
    df_airbnb_processed = utils.process_data(spark, df_raw_compiled, query)

    # Store processed dataset as delta lake table in silver layer
    logger.info("Saving Delta Lake tables in silver layer...")
    file_path, file_type, save_mode = (
        "silver/airbnb_processed",
        "delta",
        "overwrite"
    )
    utils.load_df_to_adls(
        spark,
        dbutils,
        df_airbnb_processed,
        container_name,
        file_path,
        file_type,
        save_mode
    )
    
    logger.info("Process has completed.")


if __name__ == "__main__":
    logger = utils.set_logger()
    main()