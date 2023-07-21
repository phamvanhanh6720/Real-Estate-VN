FROM apache/airflow:2.5.1-python3.9
COPY requirements.txt /

RUN pip3 install -U pip

RUN pip3 install --no-cache-dir -r /requirements.txt