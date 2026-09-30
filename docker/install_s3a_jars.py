"""Install the Spark s3a:// connector jars into an image (used by both Dockerfiles).

Versions must match the Hadoop build inside PySpark 4.2.0 (hadoop-client 3.5.0 -> AWS SDK
v2 bundle 2.35.4). Each jar is checked against the SHA-1 published by Maven Central.

Usage (at build time, as root): python install_s3a_jars.py <target dir>
Set INSTALL_S3A_JARS=false to skip (the SDK bundle is ~650 MB; local mode does not need it).
"""

import hashlib
import os
import pathlib
import sys
import urllib.request

MAVEN = "https://repo1.maven.org/maven2"
JARS = {  # url -> sha1
    f"{MAVEN}/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar": (
        "9e594525d264c0db653c7f68da98b245f7d61ea5"
    ),
    f"{MAVEN}/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar": (
        "7252265e3970b214708e68a8b74a8fa8c875af1e"
    ),
    f"{MAVEN}/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/"
    "analyticsaccelerator-s3-1.3.1.jar": "6c9bd0f6c440c9a78e82d272f5f0252d942419f6",
}


def main(target_dir: pathlib.Path) -> int:
    target_dir.mkdir(parents=True, exist_ok=True)
    if os.environ.get("INSTALL_S3A_JARS", "true") != "true":
        print("INSTALL_S3A_JARS is not 'true': skipping the s3a jars")
        return 0
    for url, expected in JARS.items():
        target = target_dir / url.rsplit("/", 1)[1]
        urllib.request.urlretrieve(url, target)
        digest = hashlib.sha1()
        with target.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
        if digest.hexdigest() != expected:
            target.unlink()
            print(f"Checksum mismatch for {target.name}", file=sys.stderr)
            return 1
        print(f"Installed {target.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main(pathlib.Path(sys.argv[1])))
