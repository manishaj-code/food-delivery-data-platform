# syntax=docker/dockerfile:1
# Pipeline application image: Python 3.12 + Java 21 (required by PySpark 4.x) on Debian slim.
#
# Targets:
#   runtime (default, last stage) — application code only; what CI builds.
#   dev     — adds requirements-dev.txt (pytest, ruff) and tests/; used by docker-compose.yml,
#             which also bind-mounts the repository over /opt/project.
# Both run as the non-root `app` user with ENTRYPOINT `python -m src.cli`.
FROM python:3.12.14-slim-trixie AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    JAVA_HOME=/usr/lib/jvm/java-21 \
    SPARK_S3A_JARS_DIR=/opt/spark-jars \
    LOCAL_LAKE_PATH=/opt/project/lake \
    SOURCE_DATA_PATH=/opt/project/data/generated

# Java 21 JRE; the arch-independent JAVA_HOME symlink keeps the image buildable on arm64.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
    && ln -s "/usr/lib/jvm/java-21-openjdk-$(dpkg --print-architecture)" "${JAVA_HOME}" \
    && rm -rf /var/lib/apt/lists/*

# Spark s3a:// support (STORAGE_MODE=s3). Pinned, checksummed jars; build with
# --build-arg INSTALL_S3A_JARS=false for a local-only image without the ~650 MB SDK bundle.
ARG INSTALL_S3A_JARS=true
COPY docker/install_s3a_jars.py /tmp/install_s3a_jars.py
RUN python /tmp/install_s3a_jars.py "${SPARK_S3A_JARS_DIR}" && rm /tmp/install_s3a_jars.py

WORKDIR /opt/project

COPY requirements.txt ./
RUN pip install -r requirements.txt

# uid 1000 matches AIRFLOW_UID: both containers write the mounted lake/ and data/ folders.
# The empty folders let the image also run without mounts (LOCAL_LAKE_PATH, SOURCE_DATA_PATH).
RUN useradd --create-home --uid 1000 app \
    && mkdir -p lake data/generated \
    && chown -R app:app lake data

# Code is owned by root and read-only for `app`; it only writes to mounted folders.
COPY pyproject.toml ./
COPY src/ src/
COPY scripts/ scripts/
COPY sql/ sql/

USER app
ENTRYPOINT ["python", "-m", "src.cli"]
CMD ["--help"]


FROM base AS dev

USER root
COPY requirements-dev.txt ./
RUN pip install -r requirements-dev.txt
COPY tests/ tests/
COPY data/sample/ data/sample/
USER app


FROM base AS runtime
