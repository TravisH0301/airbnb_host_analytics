###############################################################################
# Name: data_modelling.py
# Description: This script conducts dimensional modelling to build dimension and
#              fact tables. The tables are loaded into the gold-dev layer of
#              the data lakehouse.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
from utils import utils


def main():
    logger.info("Process has started.")

    # Configure storage account credentials
    logger.info("Configuring storage account credentials...")
    utils.set_azure_storage_config(spark, dbutils)

    # Load processed dataset
    logger.info("Loading processed dataset...")
    container_name, file_path, file_type = (
        "airbnb-host-analytics",
        "silver/airbnb_processed",
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
    - HOST_ID: Host unique identification
    - IS_SUPERHOST: Indicator of super host
    - HAS_PROFILE_PHOTO: Indicator of profile photo
    - ABOUT_WORD_COUNT: Host about description word count
    - YEAR_OF_EXP: Years of experience in hosting
    - LISTING_COUNT: Number of property listed
    - START_DATE: SCD type 2 start date
    - END_DATE: SCD type 2 end date
    - CURRENT_IND: Indicator of current record

    And this dimension follows SCD type 2.
    """
    logger.info("Creating Host dimension table...")
    query_name = "airbnb_dim_host"
    query = utils.get_query(query_name)
    df_airbnb_dim_host = utils.process_data(spark, df_airbnb_processed, query)

    # Model Listing dimension table
    """Listing dimension table contains listing property details
    with the following attributes:
    - LISTING_ID: Listing property unique identification
    - MUNICIPALITY: Suburb
    - LATITUDE: Latitude
    - LONGITUDE: Latitude
    - PRICE: Listing price
    - REVIEW_COUNT: Number of reviews
    - START_DATE: SCD type 2 start date
    - END_DATE: SCD type 2 end date
    - CURRENT_IND: Indicator of current record

    And this dimension follows SCD type 2.
    """
    logger.info("Creating Listing dimension table...")
    query_name = "airbnb_dim_listing"
    query = utils.get_query(query_name)
    df_airbnb_dim_listing = utils.process_data(spark, df_airbnb_processed, query)

    # Model Occupancy fact table
    """Occupancy fact table contains occupancy rate within the
    next 30 days of the listing properties with the following
    attributes:
    - OCCUPANCY_ID: Unique identifiation
    - LISTING_ID: Listing identification
    - HOST_ID: Host identification
    - OCCUPANCY_RATE: Occupancy rate within next 30 days
    - SNAPSHOT_YEAR_MONTH: Snapshot year month in YYYYMM

    This table is a monthly snapshot fact table containing
    occupancy rate records at monthly intervals.
    """
    logger.info("Creating Occupancy fact table...")
    query_name = "airbnb_fact_occupancy"
    query = utils.get_query(query_name)
    df_airbnb_fact_occupancy = utils.process_data(spark, df_airbnb_processed, query)

    # Save data model in gold-dev layer of data lakehouse
    """The tables are loaded into the gold-dev layer first.
    The data quality of the tables are going to be checked prior
    to entering the gold layer.
    """
    df_path_dict = {
        "gold-dev/airbnb_dim_host": df_airbnb_dim_host,
        "gold-dev/airbnb_dim_listing": df_airbnb_dim_listing,
        "gold-dev/airbnb_fact_occupancy": df_airbnb_fact_occupancy
    }
    save_mode = "overwrite"
    for file_path, df in df_path_dict.items():
        logger.info(f"Saving table {file_path[9:]} into gold-dev layer...")
        utils.load_df_to_adls(
            spark,
            dbutils,
            df,
            container_name,
            file_path,
            file_type,
            save_mode
        )

    logger.info("Process has completed.")


if __name__ == "__main__":
    logger = utils.set_logger()
    main()
