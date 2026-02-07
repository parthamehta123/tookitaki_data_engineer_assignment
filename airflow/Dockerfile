FROM apache/airflow:3.1.7

USER root

# Java required for SparkSubmitOperator
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean

USER airflow

# Install providers: Spark, Databricks, Docker
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-databricks \
    apache-airflow-providers-docker