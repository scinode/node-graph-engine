from __future__ import annotations

import pytest

pytest.importorskip("celery")

from aiida import orm

from node_graph_engine.engines import celery as celery_module
from node_graph_engine.engines.celery import CeleryEngine


def test_nested_graph(nested_graph) -> None:
    """Test execution of a nested graph using the CeleryEngine."""

    ng = nested_graph.build(x=1, y=2, z=3)

    engine = CeleryEngine(always_eager=True)
    result = engine.run(ng)

    assert result["result"] == 20
    pn = orm.load_node(engine._graph_pid)
    assert pn.is_finished_ok
    assert len(pn.called) == 3  # 2 add nodes and 1 multiply task
    assert pn.called[0].process_label == "add"
    assert pn.called[1].process_label == "Graph<add_multiply>"
    assert pn.called[1].inputs.x == pn.called[0].outputs.result
    assert pn.called[1].outputs.sum == 5
    assert pn.called[1].outputs.product == 15
    assert pn.called[2].process_label == "add1"
    assert pn.called[2].inputs.x == pn.called[1].outputs.sum
    assert pn.called[2].inputs.y == pn.called[1].outputs.product


def test_memory_broker_defaults_to_eager(nested_graph) -> None:
    """Engines using the in-memory broker should eagerly execute tasks."""

    ng = nested_graph.build(x=1, y=2, z=3)

    engine = CeleryEngine()

    assert engine._app.conf.task_always_eager is True

    result = engine.run(ng)

    assert result["result"] == 20


def test_engine_rehydrates_celery_app() -> None:
    """A serialized Celery configuration can rebuild the application."""

    engine = CeleryEngine(always_eager=True)
    payload = engine._engine_options_payload()
    app_id = payload["celery_app_id"]

    registry = celery_module._CELERY_APP_REGISTRY
    original_app = registry.pop(app_id, None)
    assert original_app is not None

    try:
        rebuilt = CeleryEngine(
            name="rehydrated",
            _celery_app_id=app_id,
            _celery_init=payload["celery_init"],
        )

        assert rebuilt._celery_app_id == app_id
        assert rebuilt.celery_app.conf.task_always_eager is True
    finally:
        registry.pop(app_id, None)
        registry[app_id] = original_app
