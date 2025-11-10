from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="spark_submit_example",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command="""
        docker exec spark-master spark-submit \
          --master spark://spark-master:7077 \
          /opt/airflow/dags/jobs/example_spark_job.py
        """
    )
