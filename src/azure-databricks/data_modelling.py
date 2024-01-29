###############################################################################
# Name: data_modelling.py
# Description: This script conducts dimensional modelling to build dimension and
#              fact tables. The tables are loaded into the gold layer of
#              ADLS gen2 as a delta lake tables.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
import os
os.system("pip install pyyaml")
import yaml

from utils import utils


def main():
    print("Process has started.")

    # Configure storage account credentials
    print("Configuring storage account credentials...")
    utils.set_azure_storage_config(spark, dbutils)

    # Load processed dataset
    container_name, file_path, file_type = (
        "airbnb-host-analytics",
        "silver/airbnb_processed"
        "delta"
    )
    df_airbnb_processed = utils.load_data_to_df(
        spark,
        dbutils,
        container_name,
        file_path,
        file_type
    )
    

    print("Process has completed.")

if __name__ == "__main__":
    main()