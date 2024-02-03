###############################################################################
# Name: metrics_quality_check.py
# Description: This script validates data quality of the metrics dataset
#              in the gold-dev layer. Once validated, the dataset is loaded into
#              the gold layer of the data lakehouse.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
from utils import utils
from utils.great_expectations_utils import (
    gx_checkpoint_generator,
    validate_dateset
)


def main():
    logger.info("Process has started.")

    # Configure storage account credentials
    logger.info("Configuring storage account credentials...")
    utils.set_azure_storage_config(spark, dbutils)

    # Load metrics layer dataset from gold-dev layer
    logger.info("Loading metrics layer dataset from gold-dev layer...")
    container_name, file_path, file_type = (
        "airbnb-host-analytics",
        "gold-dev/airbnb_metrics_host_occupancy",
        "delta"
    )
    df = utils.load_data_to_df(
        spark,
        dbutils,
        container_name,
        file_path,
        file_type
    )

    # Create Great Expectations (GX) checkpoint generator instance
    logger.info("Creating a Great Expectations checkpoint generator...")
    checkpoint_generator = gx_checkpoint_generator()

    # Validate dataset and move it into gold layer if validated successfully
    # Run data validation using Great Expectations
    dataset_name = file_path[9:]
    logger.info(f"Validating metrics layer dataset {dataset_name}...")
    results = validate_dateset(
        checkpoint_generator,
        dataset_name,
        df
    )
    results_status = results.list_validation_results()[0]["success"]

    # Move dataset into gold layer if validated
    if results_status:
        logger.info("Data validation has been successful.")
        logger.info(f"Moving dataset {dataset_name} to gold layer...")
        file_path, save_mode = (
            "gold/airbnb_metrics_host_occupancy",
            "overwrite"
        )
        utils.load_df_to_adls(
            spark,
            dbutils,
            df,
            container_name,
            file_path,
            file_type,
            save_mode
        )

    else:
        raise Exception(
            f"Data validation has failed with {dataset_name}."
            f"\n\n{results}"
        )

    logger.info("Process has completed.")


if __name__ == "__main__":
    logger = utils.set_logger()
    main()
