###############################################################################
# Name: test_utils.py
# Description: This script conducts unit test and integration test of 
#              utils.py script.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
import yaml
import sys
sys.path.append("src/azure-databricks/utils")

from unittest import TestCase, main

from utils import get_query


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
        query = get_query(mock_query_name, mock_yaml_filename)
        
        self.assertEqual(query, ("SELECT * FROM A"))

if __name__ == "__main__":
    main()


