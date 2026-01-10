from node_graph.property import TaskProperty
from aiida import orm


def unwrap_aiida_data(value):
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, orm.List):
        return value.get_list()
    if isinstance(value, orm.Dict):
        return value.get_dict()
    return TaskProperty.NOT_ADAPTED


TaskProperty.register_validation_adapter(unwrap_aiida_data)
