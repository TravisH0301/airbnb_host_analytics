###############################################################################
# Name: metric_layer.py
# Description: This script creates a metric layer on top of the dimensional
#              data model and stores into the gold layer of the data lakehouse.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
from utils import utils


def main():
    logger.info("Process has started.")

    # Configure storage account credentials
    logger.info("Configuring storage account credentials...")
    utils.set_azure_storage_config(spark, dbutils)

    # Load dimensional data model
    logger.info("Loading dimensional data model...")
    container_name, file_type = (
        "airbnb-host-analytics",
        "delta"
    )
    file_path_li = [
        "gold/airbnb_dim_host",
        "gold/airbnb_dim_listing",
        "gold/airbnb_fact_occupancy"
    ]
    for file_path in file_path_li:
        df_name = file_path[5:]
        logger.info(f"Loading {df_name}...")
        df = utils.load_data_to_df(
            spark,
            dbutils,
            container_name,
            file_path,
            file_type
        )
        df.createOrReplaceTempView(df_name)

    # Create metric layer table
    """Metric layer table contains records about host's characteristics
    and their listing's occupancy rate in the next 30 days.
    This table is built on top of the dimensional data model to allows
    one to easily analyse the effect of host characteristics on thier
    listing performance.

    For the analytics purpose, the table is built with the up-to-date
    data from the dimensional model with the following attributes:
    - HOST_ID: Host unique identification
    - IS_MULTI_LISTING: Indicator of multiple listings
    - YEAR_OF_EXP_CATEGORY: Years of experience categorised in
        - 0
        - 1-4
        - 4-7
        - 7-10
        - >10
    - HAS_PROFILE_PHOTO: Indicator of profile photo
    - IS_SUPERHOST: Indicator of super host
    - TOTAL_REVIEW_CATEGORY: Host's total listing review count categorised in
        - 0
        - 1-10
        - 10-50
        - 50-100
        - >100
    - HOST_ABOUT_LENGTH_CATEGORY: Host about description word count categorised in
        - Short (x<=2Q)
        - Concise (2Q<x<=3Q)
        - Detailed (3Q<x<=Upper Limit)
        - Lengthy (>Upper Limit)
      Note that the median (2Q) word count is 8 words
    - AVERAGE_OCCUPANCY_RATE: Average occupancy rate of host's listings
    - SNAPSHOT_YEAR_MONTH: Record snapshot year month in YYYYMM
    """
    logger.info("Creating metric layer table...")
    query_name = "airbnb_metric_host_occupancy"
    query = utils.get_query(query_name)
    df_airbnb_metric = spark.sql(query)

    # Save metric layer table as Delta Lake tables in ADLS
    logger.info("Saving metric layer table...")
    file_path = "gold/airbnb_metric_host_occupancy"
    save_mode = "overwrite"
    utils.load_df_to_adls(
        spark,
        dbutils,
        df_airbnb_metric,
        container_name,
        file_path,
        file_type,
        save_mode
    )

    logger.info("Process has completed.")


if __name__ == "__main__":
    logger = utils.set_logger()
    main()
