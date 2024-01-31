###############################################################################
# Name: test_utils.py
# Description: This script conducts unit test and integration test of 
#              utils.py script.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
import os
import yaml
import sys
sys.path.append("src/azure-databricks/utils")

from pyspark.sql import SparkSession, Row
from unittest import TestCase, main

from utils import get_query, process_data


class test_functions(TestCase):
    def test_get_query(self):
        """This checks functionality of reading YAML file."""
        # Create mock YAML file
        mock_query = {
            "query": "SELECT * FROM A",
        }
        mock_query_name = "query"
        mock_yaml_filename = "mock_sql.yaml"
        with open(mock_yaml_filename, "w") as file:
            yaml.dump(mock_query, file, default_flow_style=False)

        # Test function
        expected_result = "SELECT * FROM A"
        query = get_query(mock_query_name, mock_yaml_filename)
        
        self.assertEqual(query, (expected_result))

    def test_process_data(self):
        """This checks functionality of data processing."""
        # Create mock Spark dataframe
        os.environ["USER"] = "test_user"
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([
            Row(id=1, val=2),
            Row(id=2, val=3),
            Row(id=4, val=5)
        ])
        df.createOrReplaceTempView("dataset")

        # Create mock query
        query = "SELECT MAX(val) FROM dataset"

        # Test function
        expected_result = 5
        result = process_data(spark, df, query).first()[0]

        self.assertEqual(result, expected_result)

if __name__ == "__main__":
    main()


