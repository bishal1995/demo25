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
spark_task = SparkSubmitOperator(
    task_id="run_nyc_taxi_spark_job",
    application="/opt/airflow/dags/spark_jobs.zip",
    conn_id="spark_default",
    executor_cores=2,
    executor_memory="2g",
    driver_memory="2g",
    num_executors=2,
    application_args=["spark_jobs.jobs.nyc_taxi"],
    conf={"spark.master": "spark://spark-master:7077"},
    dag=dag,
)

# Set the task execution order
spark_task