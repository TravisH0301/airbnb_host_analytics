###############################################################################
# Name: model_quality_check.py
# Description: This script validates data quality of the dimensionl model
#              datasets in the gold-dev layer. Once validated, the datasets are
#              loaded into the gold layer of the data lakehouse.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
from utils import utils
from utils.great_expectations_utils import gx_checkpoint_generator


def validate_dateset(checkpoint_generator, dataset_name, df):
    """this function validate the given dataset
    using Great Expectations.

    Parameters
    ----------
    checkpoint_generator: obj
        Great Expectations checkpoint generator class instance
    dataset_name: str
        Name of dataset
    df: Spark dataframe
        Spark dataframe

    Returns
    -------
    Great Expectations checkpoint results
    """
    yaml_prefix = dataset_name[7:]  # e.g., dim_host
    expectation_suite_yaml_path = f"./conf/{yaml_prefix}_expectation_suite.yaml"

    checkpoint = checkpoint_generator.create_checkpoint(
        checkpoint_name=f"{yaml_prefix}_checkpoint",
        df=df,
        expectation_suite_yaml_path=expectation_suite_yaml_path
    )

    return checkpoint.run()


def main():
    logger.info("Process has started.")

    # Configure storage account credentials
    logger.info("Configuring storage account credentials...")
    utils.set_azure_storage_config(spark, dbutils)

    # Load dimensional model tables from gold-dev layer
    logger.info("Loading dimensional model tables from gold-dev layer...")
    container_name, file_paths, file_type = (
        "airbnb-host-analytics",
        [
            "gold-dev/airbnb_dim_host",
            "gold-dev/airbnb_dim_listing",
            "gold-dev/airbnb_fact_occupancy"
        ],
        "delta"
    )
    df_dict = {}
    for file_path in file_paths:
        dataset_name = file_path[9:]
        df = utils.load_data_to_df(
            spark,
            dbutils,
            container_name,
            file_path,
            file_type
        )
        df_dict[dataset_name] = df

    # Create Great Expectations (GX) checkpoint generator instance
    logger.info("Creating a Great Expectations checkpoint generator...")
    checkpoint_generator = gx_checkpoint_generator()

    # Validate datasets and move them into gold layer if validated successfully
    for dataset_name, df in df_dict.items():
        # Run data validation using Great Expectations
        logger.info(f"Validating dimensional model dataset {dataset_name}...")
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
                f"gold/{dataset_name}",
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
