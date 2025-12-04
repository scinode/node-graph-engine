"""Module with `Node` sub class for work processes."""

from typing import Optional, Tuple
import logging
from aiida.common.lang import classproperty

from aiida.orm.nodes.process.workflow import WorkflowNode

__all__ = ('NodeGraphNode',)


def make_dict_property(attribute_key: str, default=None):
    """
    Return a property object that gets/sets a dict attribute from `self.base.attributes`.

    :param attribute_key: the key in `self.base.attributes` for this dict
    :param default: default value to return if nothing is set
    """

    def getter(self):
        return self.base.attributes.get(attribute_key, default)

    def setter(self, value):
        self.base.attributes.set(attribute_key, value)

    return property(getter, setter)


def get_item_from_dict(base, attribute_key: str, item_key: str, default=None):
    """
    Get one value from a dict attribute (by item_key).
    """
    dct = base.attributes.get(attribute_key, {})
    return dct.get(item_key, default)


def set_item_in_dict(base, attribute_key: str, item_key: str, value):
    """
    Set one value in a dict attribute (by item_key).
    """
    dct = base.attributes.get(attribute_key, {})
    dct[item_key] = value
    base.attributes.set(attribute_key, dct)


class NodeGraphNode(WorkflowNode):
    """ORM class for all nodes representing the execution of a NodeGraph."""

    TASK_EXECUTORS_KEY = 'task_executors'
    TASK_INPUTS_KEY = 'task_inputs'
    NODEGRAPH_DATA_KEY = 'nodegraph_data'
    NODEGRAPH_DATA_SHORT_KEY = 'nodegraph_data_short'
    NODEGRAPH_ERROR_HANDLERS_KEY = 'nodegraph_error_handlers'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classproperty
    def _updatable_attributes(cls) -> Tuple[str, ...]:  # type: ignore
        # pylint: disable=no-self-argument
        return super()._updatable_attributes + (
            cls.NODEGRAPH_DATA_KEY,
            cls.TASK_INPUTS_KEY,
            cls.NODEGRAPH_DATA_SHORT_KEY,
            cls.NODEGRAPH_ERROR_HANDLERS_KEY,
            cls.TASK_EXECUTORS_KEY,
        )

    task_executors = make_dict_property(TASK_EXECUTORS_KEY, default={})
    nodegraph_data = make_dict_property(NODEGRAPH_DATA_KEY, default=None)
    task_inputs = make_dict_property(TASK_INPUTS_KEY, default=None)
    nodegraph_data_short = make_dict_property(NODEGRAPH_DATA_SHORT_KEY, default=None)
    nodegraph_error_handlers = make_dict_property(NODEGRAPH_ERROR_HANDLERS_KEY, default=None)

    