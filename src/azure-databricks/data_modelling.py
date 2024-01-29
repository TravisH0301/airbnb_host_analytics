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

from utils.utils import set_azure_storage_config


def main():
    print("Process has started.")

    # Configure storage account credentials
    print("Configuring storage account credentials...")
    set_azure_storage_config(spark, dbutils)


    print("Process has completed.")

if __name__ == "__main__":
    main()