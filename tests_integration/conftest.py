from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any, Annotated, Callable

import pytest
from node_graph import Graph, namespace, task

INTEGRATION_ROOT = Path(__file__).resolve().parent


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


@pytest.fixture(scope="session", autouse=True)
def ensure_aiida_default_user() -> None:
    """Ensure the active AiiDA profile has a default user email."""
    from aiida import load_profile, orm
    from aiida.manage.manager import get_manager

    if not _integration_enabled():
        return

    load_profile()
    manager = get_manager()
    profile = manager.get_profile()
    if profile is None:
        pytest.skip("No AiiDA profile is available for integration tests.")
    if getattr(profile, "default_user_email", None):
        return

    users = list(orm.User.collection.all())
    if users:
        email = users[0].email
    else:
        email = "aiida@localhost"
        orm.User(email=email, first_name="AiiDA", last_name="User").store()
    manager.set_default_user_email(profile, email)


@pytest.fixture
def add_task() -> Callable:
    """Generate a decorated task for integration tests."""

    @task()
    def add(x, y):
        return x + y

    return add


@pytest.fixture
def multiply_node() -> Callable:
    """Generate a decorated task for integration tests."""

    @task()
    def multiply(x, y):
        return x * y

    return multiply


@pytest.fixture
def add_multiply_graph(add_task, multiply_node) -> Graph:
    @task.graph()
    def add_multiply(x, y, z) -> Annotated[dict, namespace(sum=Any, product=Any)]:
        out1 = add_task(x=x, y=y).result
        m = multiply_node(x=out1, y=z).result
        return {"sum": out1, "product": m}

    return add_multiply


@pytest.fixture
def nested_graph(add_task, add_multiply_graph) -> Graph:
    @task.graph()
    def nested(x, y, z):
        out1 = add_task(x=x, y=y).result
        out2 = add_multiply_graph(x=out1, y=y, z=z)
        out3 = add_task(x=out2.sum, y=out2.product).result
        return out3

    return nested
