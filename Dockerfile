# syntax=docker/dockerfile:1
# Pipeline application image — development version (finalised in Phase 10).
# Python 3.12 + Java 21 (required by PySpark 4.x) on Debian slim.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    SPARK_S3A_JARS_DIR=/opt/spark-jars

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

# Spark s3a:// support (used from Phase 4 when STORAGE_MODE=s3). Versions must match the
# Hadoop build inside PySpark 4.2.0 (hadoop-client 3.5.0 -> AWS SDK v2 bundle 2.35.4).
# The SDK bundle is ~650 MB; build with --build-arg INSTALL_S3A_JARS=false for local-only use.
ARG INSTALL_S3A_JARS=true
RUN python - <<'EOF'
import hashlib, os, pathlib, urllib.request

if os.environ.get("INSTALL_S3A_JARS", "true") != "true":
    raise SystemExit(0)
maven = "https://repo1.maven.org/maven2"
jars = {  # url -> sha1 published by Maven Central
    f"{maven}/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar":
        "9e594525d264c0db653c7f68da98b245f7d61ea5",
    f"{maven}/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar":
        "7252265e3970b214708e68a8b74a8fa8c875af1e",
    f"{maven}/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar":
        "6c9bd0f6c440c9a78e82d272f5f0252d942419f6",
}
target_dir = pathlib.Path(os.environ["SPARK_S3A_JARS_DIR"])
target_dir.mkdir(parents=True, exist_ok=True)
for url, expected in jars.items():
    target = target_dir / url.rsplit("/", 1)[1]
    urllib.request.urlretrieve(url, target)
    digest = hashlib.sha1()
    with target.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected:
        raise SystemExit(f"Checksum mismatch for {target.name}")
    print(f"Installed {target.name}")
EOF

WORKDIR /opt/project

COPY requirements.txt requirements-dev.txt ./
RUN pip install -r requirements-dev.txt

COPY . .

RUN useradd --create-home --uid 1000 app && chown -R app:app /opt/project
USER app

CMD ["python", "-m", "scripts.generate_data", "--help"]
