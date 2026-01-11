__all__ = ["decorator_remote_task", "TaskProperty"]

__version__ = "0.1.1"


# Lazy-load public symbols to avoid import-time side effects (e.g., Temporal sandbox).
def __getattr__(name: str):
    if name == "decorator_remote_task":
        from .decorator import decorator_remote_task

        return decorator_remote_task
    if name == "TaskProperty":
        from .property import TaskProperty

        return TaskProperty
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(list(globals().keys()) + __all__)
