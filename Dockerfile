FROM apache/airflow:2.8.3
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt