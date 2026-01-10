# trigger the decorator import
from .decorator import decorator_remote_task
from .property import TaskProperty  # trigger the property adaptor registration

__all__ = [
    "decorator_remote_task",
    "TaskProperty",
]

__version__ = "0.1.1"
