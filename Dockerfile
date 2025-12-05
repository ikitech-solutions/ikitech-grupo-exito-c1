FROM apache/airflow:2.8.3
USER root
RUN apt-get update && apt-get install -y git && apt-get clean
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --no-cache-dir git+https://github.com/toon-format/toon-python.git
