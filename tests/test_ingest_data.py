###############################################################################
# Name: test_ingest_data.py
# Description: This script conducts unit test and integration test 
#              with ingest_data.py script.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
import yaml
import sys
sys.path.append("src/azure-function")

from unittest import TestCase, main

from ingest_data import fetch_data, load_storage_cred


class test_functions(TestCase):
    def test_fetch_data(self):
        """This checks the integration with the API."""
        url = "http://data.insideairbnb.com/australia/vic/melbourne/2023-03-13" \
            "/data/listings.csv.gz"
        df = fetch_data(url)

        self.assertFalse(df.empty)

    def test_load_storage_cred(self):
        """This checks functionality of reading YAML file."""
        # Create mock YAML file
        mock_data = {
            "account_name": "abc",
            "account_key": "123"
        }
        mock_yaml_filename = "mock_cred.yaml"
        with open(mock_yaml_filename, "w") as file:
            yaml.dump(mock_data, file, default_flow_style=False)
        # Test function
        expected_result = ("abc", "123")
        account_cred = load_storage_cred(mock_yaml_filename)

        self.assertEqual(account_cred, expected_result)


if __name__ == "__main__":
    main()
