from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Set, Union
from collections import defaultdict, deque
from node_graph.link import TaskLink
from node_graph import Graph
from node_graph.socket import TaggedValue
from aiida import orm, load_profile
from aiida.common.links import LinkType
from aiida.common.exceptions import ConfigurationError
from aiida.manage.manager import get_manager


_RUNTIME_TAG_MARKER = "__ng_tagged__"
_RUNTIME_TAG_UUID = "uuid"
_RUNTIME_TAG_VALUE = "value"
_RUNTIME_TUPLE_MARKER = "__ng_tuple__"


def get_nested_dict(d: Dict, name: str, **kwargs) -> Any:
    """Get the value from a nested dictionary.
    If default is provided, return the default value if the key is not found.
    Otherwise, raise ValueError.
    For example:
    d = {"data": {"abc": {"xyz": 2}}}
    name = "data.abc.xyz"
    """

    keys = name.split(".")
    current = d
    for key in keys:
        if key not in current:
            if "default" in kwargs:
                return kwargs.get("default")
            else:
                if isinstance(current, dict):
                    avaiable_keys = current.keys()
                else:
                    avaiable_keys = []
                raise ValueError(f"{name} not exist. Available keys: {avaiable_keys}")
        current = current[key]
    return current


def merge_dicts(dict1: Any, dict2: Any) -> Any:
    """Recursively merges two dictionaries."""
    for key, value in dict2.items():
        if key in dict1 and isinstance(dict1[key], dict) and isinstance(value, dict):
            # Recursively merge dictionaries
            dict1[key] = merge_dicts(dict1[key], value)
        else:
            # Overwrite or add the key
            dict1[key] = value
    return dict1


def update_nested_dict(
    base: Optional[Dict[str, Any]], key_path: str, value: Any
) -> None:
    """
    Update or create a nested dictionary structure based on a dotted key path.

    This function allows updating a nested dictionary or creating one if `d` is `None`.
    Given a dictionary and a key path (e.g., "data.abc.xyz"), it will traverse
    or create the necessary nested structure to set the provided value at the specified
    key location. If intermediate dictionaries do not exist, they will be created.
    If the resulting dictionary is empty, it is set to `None`.

    Args:
        base (Dict[str, Any] | None): The dictionary to update, which can be `None`.
                                   If `None`, an empty dictionary will be created.
        key (str): A dotted key path string representing the nested structure.
        value (Any): The value to set at the specified key.

    Example:
        base = None
        key = "data.abc.xyz"
        value = 2
        After running:
            update_nested_dict(d, key, value)
        The result will be:
            base = {"data": {"abc": {"xyz": 2}}}

    Edge Case:
        If the resulting dictionary is empty after the update, it will be set to `None`.

    """

    if base is None:
        base = {}
    keys = key_path.split(".")
    current_key = keys[0]
    if len(keys) == 1:
        # Base case: Merge dictionaries or set the value directly.
        if isinstance(base.get(current_key), dict) and isinstance(value, dict):
            base[current_key] = merge_dicts(base[current_key], value)
        else:
            base[current_key] = value
    else:
        # Recursive case: Ensure the key exists and is a dictionary, then recurse.
        if current_key not in base or not isinstance(base[current_key], dict):
            base[current_key] = {}
        base[current_key] = update_nested_dict(
            base[current_key], ".".join(keys[1:]), value
        )

    return base


def update_nested_dict_with_special_keys(data: Dict[str, Any]) -> Dict[str, Any]:
    """Update the nested dictionary with special keys like "base.pw.parameters"."""
    # Remove None

    data = {k: v for k, v in data.items() if v is not None}
    #
    special_keys = [k for k in data.keys() if "." in k]
    for key in special_keys:
        value = data.pop(key)
        update_nested_dict(data, key, value)
    return data


def _resolve_from_payload(payload: Any, socket: str) -> Any:
    if socket == "" or socket is None:
        return payload
    if isinstance(payload, dict) and socket in payload:
        return payload[socket]
    return get_nested_dict(payload, socket, default=None)


def _collect_literals(task, raw=False) -> Dict[str, Any]:
    """
    Recursively collect literal values from the task's input namespace, excluding
    values that are overridden by links at schedule time.
    """
    from node_graph.utils import tag_socket_value

    tag_socket_value(task.inputs, only_uuid=True)
    return task.inputs._collect_values(raw=raw)


def _is_encoded_tagged(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and value.get(_RUNTIME_TAG_MARKER) is True
        and _RUNTIME_TAG_UUID in value
    )


def _unwrap_tagged_value(value: Any) -> Any:
    """Return the underlying value for ``TaggedValue`` proxies."""

    if isinstance(value, TaggedValue):
        return _unwrap_tagged_value(value.__wrapped__)
    return value


def _encode_aiida_node(value: Any) -> Any:
    value = _unwrap_tagged_value(value)
    if isinstance(value, orm.Data):
        return {
            _RUNTIME_TAG_MARKER: True,
            _RUNTIME_TAG_UUID: value.uuid,
        }
    if isinstance(value, dict):
        return {k: _encode_aiida_node(v) for k, v in value.items()}
    raise TypeError(f"Cannot encode value of type: {type(value)}")


def _decode_aiida_node(value: Any) -> Any:
    if value is None:
        return None
    value = _unwrap_tagged_value(value)
    if isinstance(value, orm.Data):
        return value
    if _is_encoded_tagged(value):
        data = orm.load_node(value[_RUNTIME_TAG_UUID])
        return data
    if isinstance(value, dict):
        if _RUNTIME_TUPLE_MARKER in value:
            return tuple(_decode_aiida_node(v) for v in value[_RUNTIME_TUPLE_MARKER])
        return {k: _decode_aiida_node(v) for k, v in value.items()}
    raise TypeError(f"Cannot decode value of type: {type(value)}")


def _encode_runtime_inputs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    return {key: _encode_aiida_node(value) for key, value in kwargs.items()}


def _decode_runtime_inputs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    decoded: Dict[str, Any] = {}
    for key, value in kwargs.items():
        try:
            decoded[key] = _decode_aiida_node(value)
        except Exception as exc:
            raise TypeError(
                f"Cannot decode runtime input {key!r} (type {type(value)}): {exc}"
            ) from exc
    return decoded


def close_threadlocal_aiida_session() -> None:
    """Close the thread-local SQLAlchemy session managed by AiiDA, if available."""

    try:
        storage = get_manager().get_profile_storage()
    except (ConfigurationError, Exception):
        return

    try:
        session = storage.get_session()
    except Exception:
        return

    try:
        session.close()
    except Exception:
        pass


def reset_default_user_cache() -> None:
    """Clear the cached default AiiDA user so each thread reloads it in its own session."""

    try:
        storage = get_manager().get_profile_storage()
    except (ConfigurationError, Exception):
        return

    try:
        if hasattr(storage, "_default_user"):
            storage._default_user = None
    except Exception:
        pass


def get_default_user_email() -> str:
    """Return the default user email for the active AiiDA profile."""

    try:
        profile = get_manager().get_profile()
    except (ConfigurationError, Exception):
        profile = None

    if profile:
        email = getattr(profile, "default_user_email", None)
        if email:
            return email

    raise ConfigurationError("Could not determine the default AiiDA user email.")


def load_default_user(email: Optional[str] = None) -> orm.User:
    """Load the default AiiDA user, optionally overriding the email."""

    ensure_aiida_profile()
    resolved_email = email or get_default_user_email()
    return orm.User.collection.get(email=resolved_email)


def get_active_profile_name() -> Optional[str]:
    """Return the name of the currently loaded AiiDA profile, if any."""

    try:
        profile = get_manager().get_profile()
    except (ConfigurationError, Exception):
        return None

    if profile:
        return getattr(profile, "name", None)
    return None


def ensure_aiida_profile(profile_name: Optional[str] = None) -> None:
    """Ensure an AiiDA profile is loaded, optionally matching ``profile_name``."""

    try:
        manager = get_manager()
    except (ConfigurationError, Exception):
        manager = None

    current_profile = None
    if manager is not None:
        try:
            current_profile = manager.get_profile()
        except (ConfigurationError, Exception):
            current_profile = None

    if current_profile is not None:
        if profile_name and getattr(current_profile, "name", None) != profile_name:
            load_profile(profile_name)
        return

    if profile_name:
        load_profile(profile_name)
    else:
        load_profile()


def _resolve_tagged_value(value: Any) -> Any:
    """
    Recursively unwrap TaggedValue instances to get their raw values.
    """

    if isinstance(value, TaggedValue):
        return value.__wrapped__
    elif isinstance(value, dict):
        return {k: _resolve_tagged_value(v) for k, v in value.items()}
    return value


def _scan_links_topology(
    ng: Graph,
) -> Tuple[List[str], Dict[str, List[TaskLink]], Dict[str, Set[str]]]:
    """
    Build (1) a topological order, (2) incoming-links-per-task, and (3) a per-task set
    of required *output socket names* based on downstream edges.

    required_out_sockets helps us pre-register futures for only keys that will be needed
    by downstream nodes (plus DEFAULT_OUT, commonly used).
    """
    indeg: Dict[str, int] = {n: 0 for n in ng.get_task_names()}
    incoming: Dict[str, List[TaskLink]] = defaultdict(list)
    outgoing: Dict[str, List[Tuple[str, str]]] = defaultdict(
        list
    )  # src -> [(dst, to_sock_name)]
    required_out_sockets: Dict[str, Set[str]] = defaultdict(set)

    for lk in ng.links:
        src = lk.from_task.name
        dst = lk.to_task.name
        incoming[dst].append(lk)
        if src == "graph_ctx" or dst == "graph_ctx":
            continue
        outgoing[src].append((dst, lk.to_socket._scoped_name))
        indeg[dst] = indeg.get(dst, 0) + 1
        indeg.setdefault(src, 0)

        # Track which output sockets of src are actually used downstream
        # (_wait and _outputs are handled specially later)
        from_sock = lk.from_socket._scoped_name
        if from_sock not in ("_wait", "_outputs"):
            required_out_sockets[src].add(from_sock)

    q = deque([n for n, d in indeg.items() if d == 0])
    order: List[str] = []
    while q:
        n = q.popleft()
        order.append(n)
        for (m, _dest_sock) in outgoing.get(n, []):
            indeg[m] -= 1
            if indeg[m] == 0:
                q.append(m)

    if len(order) != len(indeg):
        raise RuntimeError(
            "Cycle detected; cannot build Prefect flow from a cyclic graph."
        )

    return order, incoming, required_out_sockets


def _build_node_link_kwargs(
    target_name: str,
    links_into_task: Iterable[TaskLink],
    source_map: Dict[str, Any],
    *,
    resolve_socket: Callable[[str, str, Dict[str, Any]], Any],
    resolve_whole: Callable[[str, Dict[str, Any]], Any],
    bundle_factory: Callable[[Dict[str, Any]], Any],
) -> Dict[str, Any]:
    """
    Shared link-merging helper:
      - skips explicit ``_wait`` edges (caller handles dependency recording),
      - routes ``_outputs`` edges to ``resolve_whole`` (entire upstream payload),
      - resolves upstream socket references with ``resolve_socket``,
      - bundles multi-fan-in edges via ``bundle_factory``.
    """
    grouped: Dict[str, List[TaskLink]] = defaultdict(list)
    for lk in links_into_task:
        if lk.to_task.name == target_name:
            grouped[lk.to_socket._scoped_name].append(lk)

    kwargs: Dict[str, Any] = {}
    for to_sock, lks in grouped.items():
        # ignore _wait edges for value propagation (handled separately)
        active_links = [lk for lk in lks if lk.from_socket._scoped_name != "_wait"]
        if not active_links:
            continue

        if len(active_links) == 1:
            lk = active_links[0]
            from_name = lk.from_task.name
            from_sock = lk.from_socket._scoped_name
            if from_sock == "_outputs":
                kwargs[to_sock] = resolve_whole(from_name, source_map)
            else:
                kwargs[to_sock] = resolve_socket(from_name, from_sock, source_map)
            continue

        bundle_payload: Dict[str, Any] = {}
        for lk in active_links:
            from_name = lk.from_task.name
            from_sock = lk.from_socket._scoped_name
            if from_sock in ("_wait", "_outputs"):
                continue
            key = f"{from_name}_{from_sock}"
            bundle_payload[key] = resolve_socket(from_name, from_sock, source_map)
        if bundle_payload:
            kwargs[to_sock] = bundle_factory(bundle_payload)

    return kwargs


def _flatten_dict(payload: Any, prefix: str = "") -> Dict[str, Any]:
    """
    Recursively flatten dict-like payloads into dotted keys.
    - If payload isn't a dict, returns {prefix or 'result': payload}.
    - For dicts, recurses depth-first; lists/tuples are recorded as whole values by default.
    """
    out: Dict[str, Any] = {}
    if isinstance(payload, orm.Data):
        key = prefix or "result"
        out[key] = payload
        return out
    if not isinstance(payload, dict):
        key = prefix or "result"
        out[key] = payload
        return out
    for k, v in payload.items():
        dotted = f"{prefix}__{k}" if prefix else str(k)
        if isinstance(v, dict):
            out.update(_flatten_dict(v, dotted))
        else:
            out[dotted] = v
    return out


def update_outputs(task: orm.ProcessNode, outputs: dict) -> None:
    """Attach new outputs to the task"""

    outputs_flat = _flatten_dict(outputs)
    outputs_stored = task.base.links.get_outgoing(
        link_type=(LinkType.CREATE, LinkType.RETURN)
    ).all_link_labels()
    outputs_new = set(outputs_flat.keys()) - set(outputs_stored)
    for link_label, output in outputs_flat.items():
        if link_label not in outputs_new:
            continue
        if isinstance(task, orm.CalculationNode):
            output.base.links.add_incoming(task, LinkType.CREATE, link_label)
        elif isinstance(task, orm.WorkflowNode):
            output.base.links.add_incoming(task, LinkType.RETURN, link_label)
        output.store()


def setup_inputs(task: orm.ProcessNode, inputs: dict) -> None:
    """Create the links between the input nodes and the ProcessNode that represents this process."""
    inputs_flat = _flatten_dict(inputs)
    for name, data in inputs_flat.items():
        # Certain processes allow to specify ports with `None` as acceptable values
        if data is None:
            continue
        if not data.is_stored:
            data.store()
        # Need this special case for tests that use ProcessNodes as classes
        if isinstance(task, orm.CalculationNode):
            task.base.links.add_incoming(data, LinkType.INPUT_CALC, name)
        elif isinstance(task, orm.WorkflowNode):
            task.base.links.add_incoming(data, LinkType.INPUT_WORK, name)


def UnavailableExecutor(*args, **kwargs):
    raise RuntimeError(
        "This executor was defined dynamically and is not available from the database snapshot."
    )


def clean_pickled_task_executor(tdata: Dict[str, Any]) -> None:
    """Clean the pickled executor in the task data."""
    from node_graph.executor import RuntimeExecutor

    # spec
    if "spec" in tdata:
        executor = tdata["spec"].get("executor", {})
        if executor.get("mode", "") == "pickled_callable":
            tdata["spec"]["executor"] = RuntimeExecutor.from_callable(
                UnavailableExecutor
            ).to_dict()
        if executor.get("mode", "") == "graph":
            ngdata = executor["graph_data"]
            for task in ngdata["tasks"].values():
                clean_pickled_task_executor(task)
    # error handler
    for name, handler in tdata.get("error_handlers", {}).items():
        if handler.get("mode", "") == "pickled_callable":
            tdata["error_handlers"][name] = RuntimeExecutor.from_callable(
                UnavailableExecutor
            ).to_dict()


def save_nodegraph_data(node: Union[int, orm.Node], ng: Graph, user: orm.User) -> None:
    from aiida.orm.utils.serialize import serialize
    from aiida_pythonjob.utils import serialize_ports

    ngdata = ng.to_dict(should_serialize=True)
    task_inputs = {}
    for name, task in ngdata["tasks"].items():
        # clean pickled executor before save to database
        task_inputs[name] = task.pop("inputs", {})
        clean_pickled_task_executor(task)
    node.nodegraph_data = ngdata
    graph_inputs = task_inputs.pop("graph_inputs", {})
    serialize_kwargs = {
        "python_data": graph_inputs,
        "port_schema": ng.spec.inputs,
    }
    if user is not None:
        serialize_kwargs["user"] = user
    graph_inputs = serialize_ports(**serialize_kwargs)
    setup_inputs(node, graph_inputs)
    task_inputs["graph_inputs"] = graph_inputs
    node.task_inputs = serialize(task_inputs)
    node.set_checkpoint(serialize(ngdata))
    return graph_inputs


def load_nodegraph_data(node: Union[int, orm.Node]) -> Optional[Dict[str, Any]]:
    """
    Get the nodegraph data from the given process node.
    """
    from aiida.orm import load_node
    from .serialize import deserialize_safe
    import yaml

    if isinstance(node, int):
        node = load_node(node)
    ngdata = node.base.attributes.get("nodegraph_data", None)
    try:
        task_inputs = deserialize_safe(node.task_inputs or "")
    except (yaml.constructor.ConstructorError, yaml.YAMLError):
        print(
            "Info: could not deserialize inputs.The nodegraph is still loaded and you can inspect tasks and outputs. "
        )
        task_inputs = {}

    for name, data in task_inputs.items():
        ngdata["tasks"][name]["inputs"] = data
    return ngdata
