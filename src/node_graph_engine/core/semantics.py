"""Utilities for carrying ontology semantics alongside task graph execution."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from aiida import orm
from aiida.common.links import LinkType
from node_graph.semantics import (
    SemanticsAnnotation,
    TaskSemantics,
    SemanticsPayload,
    SemanticsRelation,
    _SocketRef,
    _normalize_semantics_buffer,
)

_SEMANTICS_EXTRA_KEY = "_node_graph_semantics"
_SEMANTICS_BUFFER_ATTRS = ("semantics_buffer",)


def _get_semantics_buffer(graph: Any) -> Optional[Dict[str, List[Any]]]:
    """Fetch semantics buffer from a graph using supported attribute names."""

    for attr in _SEMANTICS_BUFFER_ATTRS:
        if hasattr(graph, attr):
            return getattr(graph, attr, None)
    return None


def _set_semantics_buffer(graph: Any, value: Dict[str, List[Any]]) -> None:
    """Persist semantics buffer on the primary attribute."""

    setattr(graph, "semantics_buffer", value)


def _node_reference(node: orm.Node) -> Dict[str, Any]:
    """Return a lightweight identifier for an AiiDA node."""

    label = getattr(node, "label", None) or getattr(node, "process_label", None)
    if not label:
        try:
            semantics = node.base.extras.get("semantics")
        except AttributeError:
            semantics = None
        if isinstance(semantics, list) and semantics:
            entry = next(
                (
                    item
                    for item in semantics
                    if isinstance(item, dict) and "label" in item
                ),
                None,
            )
            label = entry.get("label") if entry else None
    if not label:
        description = getattr(node, "description", None)
        label = description() if callable(description) else node.__class__.__name__
    return {
        "@id": f"aiida://node/{node.uuid}",
        "label": label,
        "node_type": node.__class__.__name__,
    }


def _normalize_socket_path(reference: str) -> str:
    """Return a normalised socket path, accepting dotted or underscored forms."""

    return reference.replace(".", "__")


def _resolve_socket_reference(
    reference: str,
    *,
    inputs_map: Mapping[str, orm.Node],
    outputs_map: Mapping[str, orm.Node],
) -> Optional[orm.Node]:
    """Return the ``Data`` task referenced by ``inputs.result`` or ``outputs__bar``."""

    normalized = _normalize_socket_path(reference)
    label = normalized
    if normalized.startswith("inputs__"):
        label = normalized[len("inputs__") :]
        return inputs_map.get(label)
    if normalized.startswith("outputs__"):
        label = normalized[len("outputs__") :]
        return outputs_map.get(label)
    # Fallback for legacy payloads that only specify the raw link label
    return inputs_map.get(label) or outputs_map.get(label)


def _collect_process_link_maps(
    process_node: orm.ProcessNode,
) -> Tuple[Mapping[str, orm.Node], Mapping[str, orm.Node]]:
    """Return ``(inputs_map, outputs_map)`` for ``process_node`` links."""

    outgoing_links = process_node.base.links.get_outgoing(
        link_type=(LinkType.CREATE, LinkType.RETURN)
    )
    outputs_map = {entry.link_label: entry.node for entry in outgoing_links}

    incoming_links = process_node.base.links.get_incoming(
        link_type=(LinkType.INPUT_CALC, LinkType.INPUT_WORK)
    )
    inputs_map = {entry.link_label: entry.node for entry in incoming_links}
    return inputs_map, outputs_map


def _apply_placeholder_extras(base_value: Any, extras: Dict[str, Any]) -> Any:
    if not extras:
        return base_value
    if isinstance(base_value, list):
        enriched: List[Any] = []
        for entry in base_value:
            if not isinstance(entry, dict):
                continue
            updated = dict(entry)
            for key, nested in extras.items():
                if nested is not None:
                    updated[key] = nested
            enriched.append(updated)
        return enriched
    if isinstance(base_value, dict):
        updated = dict(base_value)
        for key, nested in extras.items():
            if nested is not None:
                updated[key] = nested
        return updated
    return base_value


def _resolve_semantic_value(
    value: Any,
    *,
    inputs_map: Mapping[str, orm.Node],
    outputs_map: Mapping[str, orm.Node],
    subject_node: Optional[orm.Data],
) -> Any:
    """Recursively replace placeholder directives with task references."""

    if isinstance(value, dict):
        if "@socket" in value or "socket" in value:
            ref = value.get("@socket", value.get("socket"))
            task = _resolve_socket_reference(
                str(ref), inputs_map=inputs_map, outputs_map=outputs_map
            )
            if task is None:
                return None
            node_ref = _node_reference(task)
            extras = {
                key: _resolve_semantic_value(
                    nested,
                    inputs_map=inputs_map,
                    outputs_map=outputs_map,
                    subject_node=subject_node,
                )
                for key, nested in value.items()
                if key not in {"@socket", "socket"}
            }
            return _apply_placeholder_extras(node_ref, extras)
        resolved: Dict[str, Any] = {}
        for key, nested in value.items():
            processed = _resolve_semantic_value(
                nested,
                inputs_map=inputs_map,
                outputs_map=outputs_map,
                subject_node=subject_node,
            )
            if processed is not None:
                resolved[key] = processed
        return resolved
    if isinstance(value, list):
        resolved_list: List[Any] = []
        for item in value:
            processed = _resolve_semantic_value(
                item,
                inputs_map=inputs_map,
                outputs_map=outputs_map,
                subject_node=subject_node,
            )
            if processed is not None:
                resolved_list.append(processed)
        return resolved_list
    if isinstance(value, str):
        reference = _normalize_socket_path(value)
        if reference in inputs_map or reference in outputs_map:
            task = _resolve_socket_reference(
                reference, inputs_map=inputs_map, outputs_map=outputs_map
            )
            if task is not None:
                return _node_reference(task)
        if reference.startswith("inputs__") or reference.startswith("outputs__"):
            task = _resolve_socket_reference(
                reference, inputs_map=inputs_map, outputs_map=outputs_map
            )
            if task is not None:
                return _node_reference(task)
    return value


def _build_semantics_payload(
    *,
    annotation: SemanticsAnnotation,
    socket_label: str,
    inputs_map: Mapping[str, orm.Node],
    outputs_map: Mapping[str, orm.Node],
    subject_node: orm.Data,
) -> Dict[str, Any]:
    payload = annotation.to_jsonld()
    payload = _resolve_semantic_value(
        payload,
        inputs_map=inputs_map,
        outputs_map=outputs_map,
        subject_node=subject_node,
    )
    payload["socket"] = socket_label
    return payload


def _append_semantics_entry(task: orm.Data, payload: Dict[str, Any]) -> None:
    if not task.is_stored:
        task.store()
    try:
        existing = task.base.extras.get("semantics")
    except AttributeError:
        existing = None
    if existing is None:
        task.base.extras.set("semantics", [payload])
        return
    updated: List[Dict[str, Any]]
    if isinstance(existing, list):
        updated = list(existing)
    else:
        updated = [existing]
    replaced = False
    for idx, entry in enumerate(updated):
        if not isinstance(entry, dict):
            continue
        if entry.get("socket") == payload.get("socket") and entry.get(
            "label"
        ) == payload.get("label"):
            updated[idx] = payload
            replaced = True
            break
    if not replaced:
        updated.append(payload)
    task.base.extras.set("semantics", updated)


def _store_semantics_for_map(
    *,
    semantics: TaskSemantics,
    nodes_map: Mapping[str, Any],
    resolver: Callable[[str], Optional[SemanticsAnnotation]],
    inputs_map: Mapping[str, orm.Node],
    outputs_map: Mapping[str, orm.Node],
) -> None:
    for key, value in nodes_map.items():
        annotation = resolver(key)
        if annotation is None or annotation.is_empty:
            continue
        if not isinstance(value, orm.Data):
            continue
        payload = _build_semantics_payload(
            annotation=annotation,
            socket_label=key,
            inputs_map=inputs_map,
            outputs_map=outputs_map,
            subject_node=value,
        )
        _append_semantics_entry(value, payload)


def _resolve_manual_semantics_value(value: Any) -> Any:
    """Replace ``orm.Node`` objects with lightweight task references."""

    if isinstance(value, orm.Node):
        return _node_reference(value)
    if isinstance(value, dict):
        resolved: Dict[str, Any] = {}
        for key, nested in value.items():
            processed = _resolve_manual_semantics_value(nested)
            if processed is not None:
                resolved[key] = processed
        return resolved
    if isinstance(value, list):
        resolved_list: List[Any] = []
        for item in value:
            processed = _resolve_manual_semantics_value(item)
            if processed is not None:
                resolved_list.append(processed)
        return resolved_list
    return value


def _build_socket_resolver(
    process_node: orm.ProcessNode,
) -> Callable[[Optional[_SocketRef]], Optional[orm.Node]]:
    """Return a callable that resolves socket references to ``orm.Node`` instances."""

    cache: Dict[str, Tuple[Mapping[str, orm.Node], Mapping[str, orm.Node]]] = {}
    children = {
        child.process_label: child
        for child in getattr(process_node, "called", []) or []
    }

    def _resolve_process(node_label: str) -> Optional[orm.ProcessNode]:
        if node_label in children:
            return children[node_label]
        if node_label == getattr(process_node, "process_label", None):
            return process_node
        return None

    def _resolver(ref: Optional[_SocketRef]) -> Optional[orm.Node]:
        if ref is None:
            return None
        proc = _resolve_process(ref.task_name)
        if proc is None:
            return None
        if proc.uuid not in cache:
            cache[proc.uuid] = _collect_process_link_maps(proc)
        inputs_map, outputs_map = cache[proc.uuid]
        label = _normalize_socket_path(ref.socket_path)
        if ref.kind == "input":
            return inputs_map.get(label)
        return outputs_map.get(label)

    return _resolver


def _resolve_attachment_value(
    value: Any, resolver: Callable[[Optional[_SocketRef]], Optional[orm.Node]]
) -> Any:
    """Resolve pending attachment values (socket refs, orm nodes, nested structures)."""

    if isinstance(value, _SocketRef):
        return _resolve_manual_semantics_value(resolver(value))
    if isinstance(value, set):
        value = list(value)
    if isinstance(value, dict):
        resolved: Dict[str, Any] = {}
        for key, nested in value.items():
            processed = _resolve_attachment_value(nested, resolver)
            if processed is not None:
                resolved[key] = processed
        return resolved
    if isinstance(value, (list, tuple)):
        resolved_items: List[Any] = []
        for item in value:
            processed = _resolve_attachment_value(item, resolver)
            if processed is not None:
                resolved_items.append(processed)
        return resolved_items
    return _resolve_manual_semantics_value(value)


def apply_pending_semantics(process_node: orm.ProcessNode, graph: Any) -> None:
    """Flush semantics registered against graph sockets (via ``attach_semantics``)."""

    if graph is None:
        return
    pending_raw: Optional[Dict[str, List[Any]]] = _get_semantics_buffer(graph)
    pending = _normalize_semantics_buffer(pending_raw)
    if not pending.get("relations") and not pending.get("payloads"):
        return
    _set_semantics_buffer(graph, pending)

    resolver = _build_socket_resolver(process_node)

    for relation in pending.get("relations", []):
        if not isinstance(relation, SemanticsRelation):
            continue
        subject_node = resolver(relation.subject)
        if not isinstance(subject_node, orm.Data):
            continue
        if not relation.values:
            continue
        resolved = (
            _resolve_attachment_value(relation.values, resolver)
            if len(relation.values) > 1
            else _resolve_attachment_value(relation.values[0], resolver)
        )
        payload: Dict[str, Any] = {
            "relations": {relation.predicate: resolved},
        }
        if relation.label:
            payload["label"] = relation.label
        if relation.context:
            payload["context"] = dict(relation.context)
        if relation.socket_label:
            payload["socket"] = relation.socket_label
        _append_semantics_entry(subject_node, payload)

    for pending_payload in pending.get("payloads", []):
        if not isinstance(pending_payload, SemanticsPayload):
            continue
        subject_node = resolver(pending_payload.subject)
        if not isinstance(subject_node, orm.Data):
            continue
        semantics = pending_payload.semantics
        if isinstance(semantics, SemanticsAnnotation):
            annotation = semantics
        else:
            annotation = SemanticsAnnotation.from_raw(semantics)
        if annotation is None or annotation.is_empty:
            continue
        payload = annotation.to_jsonld()
        payload = _resolve_attachment_value(payload, resolver)
        if pending_payload.socket_label:
            payload["socket"] = pending_payload.socket_label
        _append_semantics_entry(subject_node, payload)


def store_socket_semantics_from_links(
    process_node: orm.ProcessNode,
    semantics: Optional[TaskSemantics],
) -> None:
    if semantics is None:
        return

    inputs_map, outputs_map = _collect_process_link_maps(process_node)

    if inputs_map:
        _store_semantics_for_map(
            semantics=semantics,
            nodes_map=inputs_map,
            resolver=semantics.resolve_input,
            inputs_map=inputs_map,
            outputs_map=outputs_map,
        )

    if outputs_map:
        _store_semantics_for_map(
            semantics=semantics,
            nodes_map=outputs_map,
            resolver=semantics.resolve_output,
            inputs_map=inputs_map,
            outputs_map=outputs_map,
        )


def register_pending_semantics(
    process_node: orm.ProcessNode, semantics: Optional[TaskSemantics]
) -> None:
    if semantics is None:
        return
    process_node.base.extras.set(_SEMANTICS_EXTRA_KEY, semantics.to_dict())


def _pop_pending_semantics(process_node: orm.ProcessNode) -> Optional[TaskSemantics]:
    try:
        raw = process_node.base.extras.get(_SEMANTICS_EXTRA_KEY)
    except AttributeError:
        raw = None
    if not raw:
        return None
    process_node.base.extras.delete(_SEMANTICS_EXTRA_KEY)
    return TaskSemantics.from_dict(raw)


def flush_registered_semantics(process_node: orm.ProcessNode) -> None:
    for child in getattr(process_node, "called", []) or []:
        flush_registered_semantics(child)
    semantics = _pop_pending_semantics(process_node)
    if semantics is None:
        return
    store_socket_semantics_from_links(process_node, semantics)


def record_graph_semantics(
    process_node: orm.ProcessNode, semantics: Optional[TaskSemantics]
) -> None:
    """Persist socket semantics for a completed graph workflow task."""

    if semantics is None:
        return
    store_socket_semantics_from_links(process_node, semantics)


def finalize_pending_semantics(
    process_node: orm.ProcessNode, graph: Any, *, success: bool
) -> None:
    """Flush child semantics and apply deferred attachments after graph completion."""

    flush_registered_semantics(process_node)
    if success:
        apply_pending_semantics(process_node, graph)
