###############################################################################
# Name: metric_layer.py
# Description: This script creates a metric layer on top of the dimensional
#              data model and stores into the gold layer of the ADLS gen2.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
from utils import utils


def main():
    print("Process has started.")

    # Configure storage account credentials
    print("Configuring storage account credentials...")
    utils.set_azure_storage_config(spark, dbutils)

    # Load dimensional data model
    print("Loading dimensional data model...")
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
        print(f"Loading {df_name}...")
        df = utils.load_data_to_df(
            spark,
            dbutils,
            container_name,
            file_path,
            file_type
        )
        df.createOrReplaceTempView(df_name)

    # Create metric layer table
    query_name = "metric_layer"
    query = utils.get_query(query_name)
    df_airbnb_metric = spark.sql(query)
    
    # Save metric layer table as Delta Lake tables in ADLS
    print("Saving metric layer table...")
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

    print("Process has completed.")


if __name__ == "__main__":
    main()