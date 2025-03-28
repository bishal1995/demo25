from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    "spark_nyc_taxi_dag",
    default_args=default_args,
    schedule_interval=None,  # Set to None to run manually
    catchup=False,
)

# Spark Submit Operator to execute the PySpark script
spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    application='/opt/airflow/dags/zips/spark_jobs/jobs/rill_clickhouse.py',
    py_files='/opt/airflow/dags/zips/spark_jobs.zip',
    conn_id='spark_default',  # Make sure to set up this connection in Airflow connections
    total_executor_cores=4,  # equivalent to num-executors * executor-cores
    executor_cores=2,
    executor_memory='2g',
    driver_memory='2g',
    name='arrow-spark',
    application_args=[],  # Add any additional arguments if needed
    spark_binary='spark-submit',
    conf={
        'spark.master': 'spark://spark-master:7077'
    }
)

# Set the task execution order
spark_task