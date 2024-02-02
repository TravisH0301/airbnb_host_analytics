###############################################################################
# Name: great_expectations_utils.py
# Description: This script contains Great Expectations utilities to
#              aid data validation process.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
import yaml

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint


class gx_checkpoint_generator():
    """This class provides an interface to generate
    a Great Expectaions (GX) checkpoint.
    This class instance is to generate multiple checkpoints
    based on the given parameters to the method create_checkpoint().
    """
    def __init__(self):
        """When instatiated, a data context is automatically
        created as an entry point to Great Expectations.
        """
        self.context = self.__create_data_context()

    def __create_data_context(self):
        """This method creates a data context
        for Great Expectations, which is used as an
        entry point to the system.

        Parameters
        ----------
        None

        Returns
        -------
        context: gx.data_context
            Entry point to Great Expectations
        """
        context = gx.get_context()
        return context

    def __create_spark_batch_request(self, df):
        """This method creates a Spark batch request with the
        given Spark dataframe. And the created batch request is
        defined as an instance variable.

        Note that batch request is a pointer that defines how to
        access and retrieve a dataset from a data source.
        On the other hand, batch is a dataset object.
        Hence, when dealing with dynamic data source, batch request is
        a good approach to fetch the up-to-date data.

        Parameters
        ----------
        df: Spark dataframe
            Spark dataframe to set as a batch request

        Returns
        -------
        None
        """
        # Create datasource
        dataframe_datasource = self.context.sources.add_or_update_spark(
            name="datasource",
        )
        # Create dataframe asset by adding Spark dataframe to datasource
        dataframe_asset = dataframe_datasource.add_dataframe_asset(
            name="dataset",
            dataframe=df,
        )
        # Create batch request using created dataframe asset
        self.batch_request = dataframe_asset.build_batch_request()

    def __create_expectation_suite(self, expectation_suite_yaml_path):
        """This method creates an expectation suite with the
        suite name extracted from the given YAML file.

        Note that this method only creates an empty expectation suite,
        and the expectations require to be added to the suite by
        using the method __add_expectation().

        Parameters
        ----------
        expectation_suite_yaml_path: str or Path
            Path to the expectation suite configuration file

        Returns
        -------
        None
        """
        with open(expectation_suite_yaml_path) as f:
            self.expectation_suite_yaml = yaml.safe_load(f)

        self.expectation_suite_name = self.expectation_suite_yaml[
            'expectation_suite_name'
        ]
        self.context.add_expectation_suite(
            expectation_suite_name=self.expectation_suite_name
        )

    def __create_validator(self):
        """This method creates a validator.
        Validator is responsible for running expectation suites
        against batch datasets.
        """

        self.validator = self.context.get_validator(
            batch_request=self.batch_request,
            expectation_suite_name=self.expectation_suite_name,
        )

    def __add_expectation(self):
        """This method adds expectations from the expectation suite
        configuration file to the expectation suite via the validator.
        """
        # Loop through expectations to add to validator
        for expectation in self.expectation_suite_yaml["expectations"]:
            expectation_type = expectation["expectation_type"]
            kwargs = expectation["kwargs"]
            # Call expectation type method with keyword arguments
            # e.g., Result = self.validator.expect_column_values_to_not_be_null(col)
            getattr(self.validator, expectation_type)(**kwargs)

        # Save added expectations
        self.validator.save_expectation_suite(discard_failed_expectations=False)

    def create_checkpoint(
        self,
        checkpoint_name,
        df,
        expectation_suite_yaml_path
    ):
        """This method creates and returns a checkpoint with
        the given to the parameters.

        The returned checkpoint object can validate the dataset
        using run() method.

        Parameters
        ----------
        checkpoint_name: str
            Name of checkpoint
        df: Spark dataframe
            Spark dataframe
        expectation_suite_yaml_path: str or Path
            Path to the expectation suite configuration file

        Returns
        checkpoint: obj
            Great expectations checkpoint
            """
        self.__create_spark_batch_request(df)
        self.__create_expectation_suite(expectation_suite_yaml_path)
        self.__create_validator()
        self.__add_expectation()

        checkpoint = Checkpoint(
            name=checkpoint_name,
            run_name_template="%Y%m%d-%H%M%S",
            data_context=self.context,
            batch_request=self.batch_request,
            expectation_suite_name=self.expectation_suite_name,
            action_list=[
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
            ],
        )

        return checkpoint


def validate_dateset(checkpoint_generator, dataset_name, df):
    """This function validates the given dataset
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
