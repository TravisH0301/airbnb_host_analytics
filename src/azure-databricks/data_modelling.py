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

    # Model Host dimension table
    """Host dimension table contains host details with
    the following attributes:
    - host_id: Host unique identification
    - is_super_host: Indicator of super host
    - has_profile_photo: Indicator of profile photo
    - about_word_count: Host about description word count
    - year_of_exp: Years of experience in hosting
    - listing_count: Number of property listed
    
    And this dimension follows SCD type 2.
    """

    # Model Listing dimension table
    """Listing dimension table contains listing property details
    with the following attributes:
    - listing_id: Listing property unique identification
    - municipality: Suburb
    - latitude: Latitude
    - longitude: Latitude
    - price: Listing price
    - review_count: Number of reviews
    
    And this dimension follows SCD type 2.
    """

    # Model Occupancy fact table
    """Occupancy fact table contains occupancy rate within the 
    next 30 days of the listing properties with the following
    attributes:
    - id: Unique identifiation
    - listing_id: Listing identification
    - host_id: Host identification
    - occupancy_rate: Occupancy rate within next 30 days
    - snapshot_date: Monthly snapshot date

    This table is a monthly snapshot fact table containing 
    occupancy rate records at monthly intervals.
    """
    

    print("Process has completed.")

if __name__ == "__main__":
    main()