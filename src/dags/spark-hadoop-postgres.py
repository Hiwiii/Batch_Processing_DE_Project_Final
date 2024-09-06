import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

####################
# Parameters
####################
spark_conn = os.environ.get("spark_conn", "spark_conn")
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/assets/jars/postgresql-42.2.6.jar"

####################
# HDFS paths
####################
hdfs_raw_data_path = "hdfs://hadoop-namenode:8020/user/hadoop/data/2019-Oct.csv"
hdfs_ingested_data_path = "hdfs://hadoop-namenode:8020/user/hadoop/ingested_data"  
hdfs_preprocessed_data_path = "hdfs://hadoop-namenode:8020/user/hadoop/preprocessed_data"
hdfs_aggregated_data_path = "hdfs://hadoop-namenode:8020/user/hadoop/aggregated_data"

####################
# Postgres paths
####################

postgres_db = "jdbc:postgresql://postgres:5432/airflow"
postgres_user = "airflow"
postgres_pwd = "airflow"

####################
# DAG Definition
####################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 6),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="spark-hdfs-postgres-quarterly",
    description="This DAG handles quarterly data ingestion from HDFS, preprocessing, and aggregation with Postgres.",
    default_args=default_args,
    schedule_interval="@quarterly"
)

start = DummyOperator(task_id="start", dag=dag)

# Task to ingest the data from the CSV file in HDFS
spark_job_ingest_data = SparkSubmitOperator(
    task_id="spark_job_ingest_data",
    application="/usr/local/spark/applications/ingest-data.py",  
    name="ingest-data",
    conn_id="spark_conn",
    verbose=1,
    conf={"spark.master": spark_master},
    application_args=[hdfs_raw_data_path, hdfs_ingested_data_path],  
    dag=dag
)

# Task to preprocess the data in HDFS
spark_job_preprocess_data = SparkSubmitOperator(
    task_id="spark_job_preprocess_data",
    application="/usr/local/spark/applications/preprocess-data.py",  
    name="preprocess-data",
    conn_id="spark_conn",
    verbose=1,
    conf={"spark.master": spark_master},
    application_args=[hdfs_ingested_data_path, hdfs_preprocessed_data_path],  
    dag=dag
)

# Task to aggregate the data from HDFS and write back to Postgres
spark_job_aggregate_data = SparkSubmitOperator(
    task_id="spark_job_aggregate_data",
    application="/usr/local/spark/applications/aggregate-data.py",  
    name="aggregate-data",
    conn_id="spark_conn",
    verbose=1,
    conf={"spark.master": spark_master},
    application_args=[hdfs_preprocessed_data_path, postgres_db, postgres_user, postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag
)

end = DummyOperator(task_id="end", dag=dag)

# Set the DAG sequence
start >> spark_job_ingest_data >> spark_job_preprocess_data >> spark_job_aggregate_data >> end
