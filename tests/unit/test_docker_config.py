"""Static checks of the Docker setup (FR-136, spec 11 §9–10, AC-066).

The images themselves are inspected in Phase 10 validation; these tests keep the rules
from regressing: pinned base images, a non-root final user, and a .dockerignore that keeps
secrets and local data out of the build context.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILES = [ROOT / "Dockerfile", ROOT / "docker" / "airflow" / "Dockerfile"]

if not all(path.is_file() for path in DOCKERFILES):  # e.g. the dev image without the mount
    pytest.skip("Dockerfiles are not available", allow_module_level=True)


def _instructions(path: Path, keyword: str) -> list[str]:
    pattern = re.compile(rf"^{keyword}\s+(.+)$", re.MULTILINE)
    return [match.strip() for match in pattern.findall(path.read_text(encoding="utf-8"))]


def _base_images(path: Path) -> list[str]:
    """External images named by FROM and COPY --from (stage names excluded)."""
    text = path.read_text(encoding="utf-8")
    stages = set(re.findall(r"^FROM\s+\S+\s+AS\s+(\S+)", text, re.MULTILINE | re.IGNORECASE))
    images = [line.split()[0] for line in _instructions(path, "FROM")]
    images += re.findall(r"^COPY\s+--from=(\S+)", text, re.MULTILINE)
    return [image for image in images if image not in stages]


@pytest.mark.parametrize("dockerfile", DOCKERFILES, ids=lambda p: p.parent.name or p.name)
def test_base_images_are_pinned(dockerfile: Path) -> None:
    text = dockerfile.read_text(encoding="utf-8")
    args = dict(re.findall(r"^ARG\s+(\w+)=(\S+)", text, re.MULTILINE))
    for image in _base_images(dockerfile):
        image = re.sub(r"\$\{(\w+)\}", lambda m: args[m.group(1)], image)
        name, _, tag = image.partition(":")
        assert tag, f"{image} has no tag"
        assert tag != "latest", f"{image} uses latest"
        assert re.search(r"\d+\.\d+", tag), f"{image} is not pinned to a version"


@pytest.mark.parametrize("dockerfile", DOCKERFILES, ids=lambda p: p.parent.name or p.name)
def test_final_user_is_not_root(dockerfile: Path) -> None:
    users = _instructions(dockerfile, "USER")
    # The pipeline image's last stages inherit the base stage's final USER.
    assert users, f"{dockerfile} never sets USER"
    assert users[-1] not in {"root", "0"}


def test_pipeline_image_entrypoint_is_the_cli() -> None:
    assert _instructions(ROOT / "Dockerfile", "ENTRYPOINT") == ['["python", "-m", "src.cli"]']


def test_dockerfiles_do_not_copy_secrets() -> None:
    for dockerfile in DOCKERFILES:
        for source in _instructions(dockerfile, "COPY"):
            assert ".env" not in source and ".aws" not in source, source


@pytest.mark.parametrize(
    "pattern",
    [".env", ".env.*", ".git", "lake/", "data/generated/", "*.tfstate*", "*.tfvars", "docs/"],
)
def test_dockerignore_excludes(pattern: str) -> None:
    lines = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    assert pattern in lines
