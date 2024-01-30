###############################################################################
# Name: data_quality_check.py
# Description: This script validates data quality of the tables in the gold 
#              layer of the data lakehouse using Great Expectations.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
import os
os.system("pip install great_expectations")
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

from utils import utils


def main():
    print("Process has started.")

    # Configure storage account credentials
    print("Configuring storage account credentials...")
    utils.set_azure_storage_config(spark, dbutils)

    # Create Great Expectations (GX) data context
    print("Creating Great Expectations data context...")
    context = gx.get_context()

    # Load metric layer dataset
    print("Loading metric layer dataset...")
    container_name, file_path, file_type = (
        "airbnb-host-analytics",
        "gold/airbnb_metric_host_occupancy",
        "delta"
    )
    df = utils.load_data_to_df(
        spark,
        dbutils,
        container_name,
        file_path,
        file_type
    )

    # Create GX datasource using Spark dataframe
    print("Creating Great Expectations datasource...")
    dataframe_datasource = context.sources.add_or_update_spark(
        name="in_memory_datasource",
    )
    dataframe_asset = dataframe_datasource.add_dataframe_asset(
        name="metric_layer_dataset",
        dataframe=df,
    )

    # Create GX batch request using datasource
    batch_request = dataframe_asset.build_batch_request()

    # Create GX validator with expectation suite
    print("Creating Great Expectations validator...")
    suite_name = "metric_layer_data_quality_check"
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    # Define test cases
    print("Defining test cases...")
    for col in df.columns:
        validator.expect_column_values_to_not_be_null(col)
        if ("IS_" in col) or ("HAS_" in col):
            validator.expect_column_values_to_be_in_set(col, ["Y", "N"])
    validator.save_expectation_suite(discard_failed_expectations=False)

    # Create checkpoint
    checkpoint_name = "metric_layer_checkpoint"
    checkpoint = Checkpoint(
        name=checkpoint_name,
        run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
        data_context=context,
        batch_request=batch_request,
        expectation_suite_name=suite_name,
        action_list=[
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
        ],
    )
    context.add_or_update_checkpoint(checkpoint=checkpoint)

    # Run checkpoint
    checkpoint_result = checkpoint.run()

    checkpoint_result_status = checkpoint_result.list_validation_results()[0]["success"]
    checkpoint_results = checkpoint_result.list_validation_results()[0]["results"]

    if checkpoint_result_status is False:
        raise Exception (
            "Great Expectation checkpoint has failed with the following" \
            f" results: \n\n{checkpoint_results}"
        )
    else:
        print("Test results:", checkpoint_result_status)
