FROM apache/airflow:3.3.2
COPY requirements.txt /requirements.txt
# Reinstalling the same Airflow version keeps pip from changing it while adding our package
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt