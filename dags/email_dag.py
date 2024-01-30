from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
  

def send_success_status_email(context):
    task_instance = context['task_instance']
    task_status = task_instance.current_state()

    to_email = Variable.get("email_receiver")
    subject = f"Airflow Task {task_instance.task_id} {task_status}"
    body = f"The task {task_instance.task_id} finished with status: {task_status}.\n\n" \
           f"Task execution date: {context['execution_date']}\n" \
           f"Log URL: {task_instance.log_url}\n\n"

    send_email(to=to_email, subject=subject, html_content=body)


def send_failure_status_email(context):
    task_instance = context['task_instance']
    task_status = task_instance.current_state()

    to_email = Variable.get("email_receiver")
    subject = f"Airflow Task {task_instance.task_id} {task_status}"
    body = f"The task {task_instance.task_id} finished with status: {task_status}.\n\n" \
           f"Task execution date: {context['execution_date']}\n" \
           f"Log URL: {task_instance.log_url}\n\n"

    send_email(to=to_email, subject=subject, html_content=body)


with DAG(
    dag_id="test email",
    default_args={
        "owner": "Travis Hong",
        "start_date": days_ago(1),
        "retries": 0,
        "on_success_callback": send_success_status_email,
        "on_failure_callback": send_failure_status_email
    },
    schedule_interval=None
) as dag:
    
    # Task
    test = BashOperator(
        task_id="test",
        bash_command="echo hello world",
        dag=dag
    )

    test