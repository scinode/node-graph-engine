from aiida import orm
from node_graph import Graph


def load_graph(pk: int | str | orm.Node) -> Graph:
    from node_graph_engine.core.utils import load_nodegraph_data

    node = orm.load_node(pk) if isinstance(pk, (int, str)) else pk

    ngdata = load_nodegraph_data(node)
    ng = Graph.from_dict(ngdata)
    return ng
