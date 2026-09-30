# syntax=docker/dockerfile:1
# Airflow image: Airflow 3 + Java 21 (PySpark 4.x runs in local mode inside the task
# processes) + the project's runtime dependencies, s3a jars, and code. docker-compose.yml
# bind-mounts the repository over /opt/project for live DAG/code reloads in development.
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

# The Airflow base image is Debian bookworm, which has no Java 21 package: copy the
# Eclipse Temurin 21 JRE instead (same Java major version as the pipeline image).
COPY --from=eclipse-temurin:21.0.12.1_1-jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}" \
    SPARK_S3A_JARS_DIR=/opt/spark-jars \
    PYTHONPATH=/opt/project

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Spark s3a:// support (STORAGE_MODE=s3), same pinned jars as the pipeline image.
ARG INSTALL_S3A_JARS=true
COPY docker/install_s3a_jars.py /tmp/install_s3a_jars.py
RUN python /tmp/install_s3a_jars.py "${SPARK_S3A_JARS_DIR}" && rm /tmp/install_s3a_jars.py

USER airflow
ARG AIRFLOW_VERSION
COPY requirements.txt /tmp/requirements.txt
# Pinning apache-airflow keeps pip from up/downgrading Airflow to satisfy our pins (TR-02).
# boto3 is left at the version Airflow ships: its Amazon provider (aiobotocore) pins
# botocore, and our code only uses stable boto3 APIs. pytest is only here to run the DAG
# integrity test inside this image.
RUN grep -v '^boto3==' /tmp/requirements.txt > /tmp/requirements-airflow.txt \
    && pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
        -r /tmp/requirements-airflow.txt pytest==9.1.1 \
    && pip check

# Project code for running without the development bind mount. Group 0 (with AIRFLOW_UID
# set to any uid) can read it, like the rest of the Airflow image.
COPY --chown=airflow:0 pyproject.toml /opt/project/
COPY --chown=airflow:0 src/ /opt/project/src/
COPY --chown=airflow:0 scripts/ /opt/project/scripts/
COPY --chown=airflow:0 sql/ /opt/project/sql/
COPY --chown=airflow:0 airflow/dags/ /opt/project/airflow/dags/
COPY --chown=airflow:0 docker/airflow/bootstrap.py /opt/project/docker/airflow/bootstrap.py
