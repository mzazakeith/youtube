FROM apache/airflow:2.7.2

USER root

RUN apt-get update && apt-get install -y openjdk-11-jre-headless && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install pyspark
