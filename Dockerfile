# Pipeline application image — Phase 1 development version (finalised in Phase 10).
# Python 3.12 + Java 21 (required by PySpark 4.x) on Debian slim.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/project

COPY requirements.txt requirements-dev.txt ./
RUN pip install -r requirements-dev.txt

COPY . .

RUN useradd --create-home --uid 1000 app && chown -R app:app /opt/project
USER app

CMD ["python", "-m", "scripts.generate_data", "--help"]
