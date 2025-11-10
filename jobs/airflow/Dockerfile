FROM apache/airflow:2.8.1-python3.10

USER root
RUN pip install apache-airflow-providers-apache-spark boto3 s3fs pyiceberg loguru pandas pyyaml pydantic pyarrow
USER airflow

COPY dags /opt/airflow/dags
