from __future__ import annotations

import pytest

from aiida import orm

try:
    from airflow import DAG  # noqa: F401
    from airflow.operators.python import PythonOperator  # noqa: F401
except Exception as exc:
    pytest.skip(f"Airflow is unavailable: {exc}", allow_module_level=True)

from node_graph_engine.engines.airflow import AirflowEngine


def test_nested_graph(nested_graph) -> None:
    """Test execution of a nested graph using the AirflowEngine."""

    ng = nested_graph.build(x=1, y=2, z=3)

    engine = AirflowEngine()
    result = engine.run(ng)

    assert result["result"] == 20

    pn = orm.load_node(engine._graph_pid)
    assert pn.is_finished_ok
    assert len(pn.called) == 3
    assert pn.called[0].process_label == "add"
    assert pn.called[1].process_label == "Graph<add_multiply>"
    assert pn.called[1].inputs.x == pn.called[0].outputs.result
    assert pn.called[1].outputs.sum == 5
    assert pn.called[1].outputs.product == 15
    assert pn.called[2].process_label == "add1"
    assert pn.called[2].inputs.x == pn.called[1].outputs.sum
    assert pn.called[2].inputs.y == pn.called[1].outputs.product


def test_build_dag_creates_tasks(nested_graph) -> None:
    """Ensure the compiled Airflow DAG exposes workflow tasks."""

    ng = nested_graph.build(x=1, y=2, z=3)
    engine = AirflowEngine(dag_id="test-dag")
    dag = engine.build_dag(ng)

    task_ids = set(dag.task_dict.keys())
    assert {"add", "add_multiply", "add1", "engine__init", "engine__finalize"}.issubset(
        task_ids
    )

    init_task = dag.task_dict["engine__init"]
    finalize_task = dag.task_dict["engine__finalize"]
    assert finalize_task.trigger_rule == "all_done"

    for node_id in ("add", "add_multiply", "add1"):
        task = dag.task_dict[node_id]
        assert "engine__init" in task.upstream_task_ids
        assert "engine__finalize" in task.downstream_task_ids
        assert init_task in task.upstream_list
        assert finalize_task in task.downstream_list
