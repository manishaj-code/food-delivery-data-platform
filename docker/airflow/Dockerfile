# syntax=docker/dockerfile:1
# Airflow image — first version (finalised in Phase 10).
# Airflow 3 + Java 21 (PySpark 4.x runs in local mode inside the task processes)
# + the project's runtime dependencies. The project itself is mounted at /opt/project.
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

# The Airflow base image is Debian bookworm, which has no Java 21 package: copy the
# Eclipse Temurin 21 JRE instead (same Java major version as the pipeline image).
COPY --from=eclipse-temurin:21-jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}"

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

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

ENV PYTHONPATH=/opt/project
