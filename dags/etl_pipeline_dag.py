"""Airflow DAG for ETL pipeline."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Note: Using BashOperator with spark-submit commands
# The transformation scripts are executed via spark-submit in the Spark cluster

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline: Bronze to Silver to Gold',
    schedule='5 * * * *',  # 5 minutes after each hour (Airflow 2.4+ uses 'schedule' instead of 'schedule_interval')
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'transformation'],
)

# Tasks - Using BashOperator with spark-submit
# These commands assume the Spark cluster is accessible and scripts are available at the specified paths
bronze_to_silver_task = BashOperator(
    task_id='bronze_to_silver',
    bash_command='spark-submit --master spark://spark-master:7077 --packages io.delta:delta-spark_2.12:3.0.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" /opt/spark/app/src/transformation/bronze_to_silver.py {{ var.value.S3_BUCKET }}',
    dag=dag,
)

silver_to_gold_task = BashOperator(
    task_id='silver_to_gold',
    bash_command='spark-submit --master spark://spark-master:7077 --packages io.delta:delta-spark_2.12:3.0.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" /opt/spark/app/src/transformation/silver_to_gold.py {{ var.value.S3_BUCKET }}',
    dag=dag,
)

# Dependencies
bronze_to_silver_task >> silver_to_gold_task
