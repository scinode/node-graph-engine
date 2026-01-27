from __future__ import annotations

import os
import importlib.util
import tempfile
from pathlib import Path

import pytest

INTEGRATION_ROOT = Path(__file__).resolve().parent
TESTS_ROOT = INTEGRATION_ROOT.parent / "tests"

if TESTS_ROOT.exists():
    fixtures_path = TESTS_ROOT / "conftest.py"
    if fixtures_path.exists():
        spec = importlib.util.spec_from_file_location(
            "_ng_engine_test_fixtures", fixtures_path
        )
        module = importlib.util.module_from_spec(spec)
        if spec and spec.loader:
            spec.loader.exec_module(module)
            for name in dir(module):
                if name.startswith("_"):
                    continue
                globals()[name] = getattr(module, name)


def _integration_enabled() -> bool:
    return os.environ.get("NG_INTEGRATION") == "1"


if _integration_enabled():

    def _ensure_writable_dir(env_key: str, default_path: Path) -> None:
        value = os.environ.get(env_key)
        path = Path(value) if value else default_path
        try:
            path.mkdir(parents=True, exist_ok=True)
            test_file = path / ".write_test"
            test_file.write_text("ok")
            test_file.unlink()
        except Exception:
            path = Path(tempfile.mkdtemp(prefix=f"ng_{env_key.lower()}_"))
        os.environ[env_key] = str(path)

    _ensure_writable_dir("AIRFLOW_HOME", INTEGRATION_ROOT / ".airflow")
    _ensure_writable_dir("DAGSTER_HOME", INTEGRATION_ROOT / ".dagster")


def pytest_collection_modifyitems(config, items) -> None:
    if _integration_enabled():
        return
    skip = pytest.mark.skip(reason="Set NG_INTEGRATION=1 to run integration tests.")
    for item in items:
        item.add_marker(skip)


def pytest_configure(config) -> None:
    config.addinivalue_line(
        "markers",
        "integration: tests that require external orchestration services",
    )
