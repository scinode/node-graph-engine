from __future__ import annotations

from aiida import orm
from dagster import DagsterInstance

from node_graph_engine.engines.dagster import DagsterEngine


def test_nested_graph(nested_graph) -> None:
    """Ensure nested graphs execute correctly with the Dagster engine."""

    ng = nested_graph.build(x=1, y=2, z=3)
    engine = DagsterEngine()
    result = engine.run(ng)

    assert result["result"] == 20

    process_node = orm.load_node(engine._graph_pid)
    assert process_node.is_finished_ok
    assert len(process_node.called) == 3
    assert process_node.called[0].process_label == "add"
    assert process_node.called[1].process_label == "Graph<add_multiply>"
    assert process_node.called[1].inputs.x == process_node.called[0].outputs.result
    assert process_node.called[1].outputs.sum == 5
    assert process_node.called[1].outputs.product == 15
    assert process_node.called[2].process_label == "add1"
    assert process_node.called[2].inputs.x == process_node.called[1].outputs.sum
    assert process_node.called[2].inputs.y == process_node.called[1].outputs.product


def test_dagster_instance_persists_runs(tmp_path, nested_graph) -> None:
    """Engine should record Dagster runs when provided an instance."""

    dagster_home = tmp_path / "dagster_home"
    instance = DagsterInstance.ephemeral(str(dagster_home))

    ng = nested_graph.build(x=1, y=2, z=3)
    engine = DagsterEngine(instance=instance)
    result = engine.run(ng)

    assert result["result"] == 20

    run_id = engine.dagster_run_id
    assert run_id is not None
    stored_run = instance.get_run_by_id(run_id)
    assert stored_run is not None
    assert stored_run.job_name == engine.job_name


def test_build_job_allows_external_execution(tmp_path, nested_graph) -> None:
    """The constructed Dagster job can be executed outside the engine."""

    dagster_home = tmp_path / "dagster_home"
    instance = DagsterInstance.ephemeral(str(dagster_home))

    ng = nested_graph.build(x=1, y=2, z=3)
    engine = DagsterEngine(instance=instance)
    dagster_job = engine.build_job(ng)

    run_result = dagster_job.execute_in_process(instance=instance)
    assert run_result.success
