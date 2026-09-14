FROM apache/airflow:2.8.1-python3.11
ENV PATH="/home/airflow/.local/bin:${PATH}"
RUN pip install --no-cache-dir apache-airflow-providers-databricks
