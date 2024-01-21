from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define default_args to set the start date and other default parameters
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'weekly_batch_process',
    default_args=default_args,
    description='A DAG to run batch_process_metadata.py weekly',
    schedule_interval='@weekly',  # Set the schedule interval to run weekly
)

# Define a PythonOperator to execute batch_process_metadata.py
def run_python_script():
    # Import necessary modules and execute your Python 
    import subprocess
    subprocess.run(['python', '/path/to/batch_process_metadata.py'])

run_script_task = PythonOperator(
    task_id='run_batch_process_metadata',
    python_callable=run_python_script,
    dag=dag,
)
