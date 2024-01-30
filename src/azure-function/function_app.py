###############################################################################
# Name: function_app.py
# Description: This script loads Airbnb raw datasets into the bronze layer of
#              the ADLS gen2 when triggered via HTTP.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
import azure.functions as func
import logging

from ingest_data import main

    
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="data_ingestion")
def load_raw_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Run main function to fetch and load data into ADLS
    main()

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )