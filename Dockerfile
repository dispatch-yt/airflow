FROM apache/airflow:2.8.3

RUN pip install --no-cache-dir pandas
RUN pip install --no-cache-dir apache-airflow-providers-http
RUN pip install --no-cache-dir pip install 'apache-airflow[amazon]'
