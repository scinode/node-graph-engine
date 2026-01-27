from __future__ import annotations

import os

import pytest
from aiida import orm

pytest.importorskip("airflow")

from node_graph_engine.engines.airflow import AirflowEngine

pytestmark = pytest.mark.integration


def test_airflow_engine_integration(nested_graph) -> None:

    if not os.environ.get("AIRFLOW_HOME"):
        pytest.skip("AIRFLOW_HOME must be set for Airflow integration tests.")

    os.environ.setdefault("NG_AIRFLOW_RESULT_TIMEOUT", "100")

    ng = nested_graph.build(x=1, y=2, z=3)
    engine = AirflowEngine(dag_id="ng-integration-airflow")
    # engine.run(ng)
    # engine.submit(ng)
    result = engine.submit(ng, force_trigger=True, wait=True)
    if result is None:
        pytest.fail("Airflow did not return results for the scheduled run.")

    assert result["result"] == 20

    process_node = orm.load_node(engine._graph_pid)
    assert process_node.is_finished_ok
