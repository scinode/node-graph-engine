from __future__ import annotations
from node_graph_engine.engines.redun import RedunEngine
from aiida import orm


def test_nested_graph(nested_graph) -> None:
    """Test execution of a nested graph using the RedunEngine."""
    ng = nested_graph.build(x=1, y=2, z=3)
    engine = RedunEngine()
    result = engine.run(ng)
    assert result["result"] == 20
    pn = orm.load_node(engine._graph_pid)
    assert pn.is_finished_ok
    assert len(pn.called) == 3  # 2 add tasks and 1 multiply task
    assert pn.called[0].process_label == "add"
    assert pn.called[1].process_label == "Graph<add_multiply>"
    assert pn.called[1].inputs.x == pn.called[0].outputs.result
    assert pn.called[1].outputs.sum == 5
    assert pn.called[1].outputs.product == 15
    assert pn.called[2].process_label == "add1"
    assert pn.called[2].inputs.x == pn.called[1].outputs.sum
    assert pn.called[2].inputs.y == pn.called[1].outputs.product
