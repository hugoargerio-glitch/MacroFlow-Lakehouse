FROM apache/airflow:2.8.1-python3.11

ENV PATH="/home/airflow/.local/bin:${PATH}" \
    PYTHONPATH="/opt/airflow"

COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow-providers-databricks>=6.0.0,<8.0.0" \
    -r /opt/airflow/requirements.txt

COPY --chown=airflow:root src /opt/airflow/src
