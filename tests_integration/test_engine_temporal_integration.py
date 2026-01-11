from __future__ import annotations

import asyncio
import time
import os

import pytest
from aiida import orm

pytest.importorskip("temporalio")

from temporalio.client import Client
from temporalio.worker import Worker

from node_graph_engine.engines.temporal import TemporalEngine

pytestmark = pytest.mark.integration


def test_temporal_engine_integration(nested_graph) -> None:
    temporal_address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")

    async def _connect_with_retry(timeout_seconds: float) -> Client:
        deadline = time.monotonic() + timeout_seconds
        last_exc: Exception | None = None
        while time.monotonic() < deadline:
            try:
                return await Client.connect(temporal_address)
            except Exception as exc:
                last_exc = exc
                await asyncio.sleep(2)
        raise RuntimeError(
            "Temporal server did not become ready in time."
        ) from last_exc

    async def _run():
        timeout = float(os.environ.get("NG_TEMPORAL_CONNECT_TIMEOUT", "300"))
        client = await _connect_with_retry(timeout)
        ng = nested_graph.build(x=1, y=2, z=3)
        engine = TemporalEngine(client=client, task_queue="ng-integration-temporal")

        async with Worker(
            client,
            task_queue="ng-integration-temporal",
            workflows=TemporalEngine.workflow_definitions(),
            activities=TemporalEngine.activity_definitions(),
        ):
            result = await engine.run_async(ng)
        return result, engine._graph_pid

    result, graph_pid = asyncio.run(_run())

    assert result["result"] == 20

    process_node = orm.load_node(graph_pid)
    assert process_node.is_finished_ok
