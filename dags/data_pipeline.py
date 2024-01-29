###############################################################################
# Name: data_pipepline.py
# Description: This DAG script orchestrates data processes to build the
#              data lakehouse in Azure ADLS gen2.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
from datetime import datetime
from airflow import DAG

from airflow.operators.bash import BashOperator


with DAG(
    dag_id="Airbnb_Host_Analytics",
    default_args={
        "owner": "Travis Hong",
        "start_date": datetime(2024, 01, 30),
        "schedule_interval": "once",
        "retries": 0,
        # Switched off emailing as unable
        # "on_success_callback": email_sender.dag_complete_alert,
        # "on_failure_callback": email_sender.dag_failure_alert
    }
) as dag:
    
    # Task to 
    test = BashOperator(
        task_id="test",
        bash_command="echo hello world",
        dag=dag
    )

    # Task to 
    test2 = BashOperator(
        task_id="test2",
        bash_command="echo bye world",
        dag=dag
    )

    # Define task dependecies
    (
        test
        >> test2
    )