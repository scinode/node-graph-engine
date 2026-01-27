from __future__ import annotations

import os

import pytest
from aiida import orm

pytest.importorskip("prefect")

from node_graph_engine.engines.prefect import PrefectEngine

pytestmark = pytest.mark.integration


def test_prefect_engine_integration(nested_graph) -> None:
    if not os.environ.get("PREFECT_API_URL"):
        pytest.skip("Set PREFECT_API_URL to run Prefect integration tests.")

    ng = nested_graph.build(x=1, y=2, z=3)
    engine = PrefectEngine(flow_name="ng-integration-prefect")
    result = engine.run(ng)

    assert result["result"] == 20

    process_node = orm.load_node(engine._graph_pid)
    assert process_node.is_finished_ok
