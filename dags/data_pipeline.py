###############################################################################
# Name: data_pipepline.py
# Description: This DAG script orchestrates data processes to build the
#              data lakehouse in Azure ADLS gen2.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/azure_airbnb_host_analytics
###############################################################################
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
  

def send_success_status_email(context):
    """This function sends out an email alert upon
    successful task execution.
    
    This function relies on the SMTP configurations in Airflow.
    https://airflow.apache.org/docs/apache-airflow/stable/howto/email-config.html#using-default-smtp
    """
    task_instance = context['task_instance']
    task_status = task_instance.current_state()

    to_email = Variable.get("email_receiver")
    subject = f"[JOB COMPLETE] Airflow - {task_instance.task_id}"
    body = f"The task {task_instance.task_id} has completed successfully.\n" \
           f"Task execution date: {context['execution_date']}\n" \
           f"Log URL: {task_instance.log_url}\n"

    send_email(to=to_email, subject=subject, html_content=body)


def send_failure_status_email(context):
    """This function sends out an email alert upon
     task execution failure.
    
    This function relies on the SMTP configurations in Airflow.
    https://airflow.apache.org/docs/apache-airflow/stable/howto/email-config.html#using-default-smtp
    """
    task_instance = context['task_instance']
    task_status = task_instance.current_state()

    to_email = Variable.get("email_receiver")
    subject = f"[JOB FAILED] Airflow - {task_instance.task_id}"
    body = f"The task {task_instance.task_id} has failed.\n" \
           f"Task execution date: {context['execution_date']}\n" \
           f"Log URL: {task_instance.log_url}\n\n"

    send_email(to=to_email, subject=subject, html_content=body)


with DAG(
    dag_id="Airbnb_Host_Analytics",
    default_args={
        "owner": "Travis Hong",
        "start_date": days_ago(1),
        "retries": 0,
        # Switched off emailing as unable
        # "on_success_callback": email_sender.dag_complete_alert,
        # "on_failure_callback": email_sender.dag_failure_alert
    },
    schedule_interval=None
) as dag:
    
    # Task to ingest raw datasets into bronze layer
    var_key = "function-key"
    function_key = Variable.get(var_key)
    function_app_name = "azure-airbnb-load-data" 
    function_name = "data_ingestion"
    data_ingestion = BashOperator(
        task_id="data_ingestion",
        bash_command="curl https://{}.azurewebsites.net/api/{}?code={}".format(
            function_app_name,
            function_name,
            function_key
        ),
        dag=dag
    )

    # Task to process raw datasets and load compiled dataset to silver layer
    data_processing = DatabricksRunNowOperator(
        task_id = 'data_processing',
        databricks_conn_id = 'databricks_default',
        job_id = 128784675279690
    )
    
    # Task to create dimensional model and load tables to gold layer
    data_modelling = DatabricksRunNowOperator(
        task_id = 'data_modelling',
        databricks_conn_id = 'databricks_default',
        job_id = 851806766336090
    )
    
    # Task to create metric layer and load table to gold layer
    metric_layer = DatabricksRunNowOperator(
        task_id = 'metric_layer',
        databricks_conn_id = 'databricks_default',
        job_id = 711715871126399
    )
    
    # Define task dependecies
    (
        data_ingestion
        >> data_processing
        >> data_modelling
        >> metric_layer
    )