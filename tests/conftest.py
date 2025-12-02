import pytest
import asyncio
from typing import Any, Annotated, Callable

from node_graph import Graph, dynamic, namespace, task

pytest_plugins = [
    "aiida.tools.pytest_fixtures",
]


@pytest.fixture(scope="session", autouse=True)
def aiida_profile(aiida_config, aiida_profile_factory):
    """Create and load a profile with RabbitMQ as broker."""
    with aiida_profile_factory(aiida_config, broker_backend="core.rabbitmq") as profile:
        yield profile


@pytest.fixture
def fixture_localhost(aiida_localhost):
    """Return a localhost `Computer`."""
    localhost = aiida_localhost
    localhost.set_default_mpiprocs_per_machine(1)
    return localhost


@pytest.fixture
def add_task() -> Callable:
    """Generate a decorated task for test."""

    @task()
    def add(x, y):
        return x + y

    return add


@pytest.fixture
def multiply_node() -> Callable:
    """Generate a decorated task for test."""

    @task()
    def multiply(x, y):
        return x * y

    return multiply


@pytest.fixture
def async_add_task() -> Callable:
    """Generate an async task for testing deferrable execution."""

    @task()
    async def async_add(x, y):
        await asyncio.sleep(0)
        return x + y

    return async_add


@pytest.fixture
def dynamic_output_node() -> Callable:
    """Generate a decorated dynamic task for test."""

    @task()
    def dynamic_output(n: int) -> Annotated[dict, dynamic(Any)]:
        return {f"output_{i}": i * 2 for i in range(n)}

    return dynamic_output


@pytest.fixture
def add_multiply_graph(add_task, multiply_node) -> Graph:
    @task.graph()
    def add_multiply(x, y, z) -> Annotated[dict, namespace(sum=Any, product=Any)]:
        print("x:", x, "y:", y, "z:", z)
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
