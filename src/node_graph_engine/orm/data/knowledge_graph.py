"""Knowledge graph helpers for ontology semantics and workflow metadata."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from aiida import orm
from aiida.common.links import LinkType
from aiida.orm import QueryBuilder
from node_graph.semantics import (
    SemanticsAnnotation,
    SemanticsTree,
    TaskSemantics,
    SemanticsPayload,
    SemanticsRelation,
    _SocketRef,
    _normalize_semantics_buffer,
)


class KnowledgeGraphData(orm.Dict):
    """Light-weight AiiDA container for workflow or node-level knowledge graphs."""


    @property
    def payload(self) -> Dict[str, Any]:  # pragma: no cover - thin wrapper
        return self.get_dict()

    @classmethod
    def _maybe_existing(cls, filters: Dict[str, Any]) -> Optional["KnowledgeGraphData"]:
        qb = QueryBuilder()
        try:
            qb.append(cls, filters=filters)
            existing = qb.first()
            return existing[0] if existing else None
        except ValueError:
            # SQLite backend cannot filter nested JSON; fall back to filtering in Python.
            qb = QueryBuilder()
            qb.append(cls, filters={"extras": {"has_key": "scope"}})
            for node, in qb.iterall():  # type: ignore[misc]
                extras = getattr(node.base, "extras", None)
                if extras is None:
                    continue
                extras_map = extras.all if hasattr(extras, "all") else extras
                if not isinstance(extras_map, dict):
                    continue
                scope = filters.get("extras", {}).get("scope")
                if scope and extras_map.get("scope") != scope:
                    continue
                match = True
                for key, val in filters.get("extras", {}).items():
                    if key == "scope":
                        continue
                    if val is None:
                        continue
                    if extras_map.get(key) != val:
                        match = False
                        break
                if match:
                    return node
            return None

    @classmethod
    def get_or_create_workflow(
        cls,
        *,
        workflow_name: str,
        callable_path: Optional[str],
        package_version: Optional[str],
        identifier: Optional[str],
        payload: Dict[str, Any],
        extras: Optional[Dict[str, Any]] = None,
    ) -> Tuple["KnowledgeGraphData", bool]:
        filters: Dict[str, Any] = {
            "extras": {
                "scope": "workflow",
                "workflow_name": workflow_name,
            }
        }
        if callable_path:
            filters["extras"]["callable_path"] = callable_path
        if package_version:
            filters["extras"]["package_version"] = package_version
        if identifier:
            filters["extras"]["identifier"] = identifier

        existing = cls._maybe_existing(filters)
        if existing:
            return existing, False

        node = cls(dict=payload)
        meta = {
            "scope": "workflow",
            "workflow_name": workflow_name,
            "callable_path": callable_path,
            "package_version": package_version,
            "identifier": identifier,
        }
        meta.update(extras or {})
        node.base.extras.set_many(meta)
        node.store()
        return node, True

    @classmethod
    def get_or_create_for_node(
        cls,
        *,
        subject_uuid: str,
        payload: Dict[str, Any],
        extras: Optional[Dict[str, Any]] = None,
    ) -> Tuple["KnowledgeGraphData", bool]:
        filters = {"extras": {"scope": "node", "subject_uuid": subject_uuid}}
        existing = cls._maybe_existing(filters)
        if existing:
            return existing, False
        node = cls(dict=payload)
        meta = {"scope": "node", "subject_uuid": subject_uuid}
        meta.update(extras or {})
        node.base.extras.set_many(meta)
        node.store()
        return node, True


def _flatten_semantics_tree(
    tree: Optional[SemanticsTree], *, task_name: str, direction: str
) -> Dict[Tuple[str, str, str], SemanticsAnnotation]:
    entries: Dict[Tuple[str, str, str], SemanticsAnnotation] = {}

    def _walk(node: Optional[SemanticsTree], path: str) -> None:
        if node is None:
            return
        if node.annotation and not node.annotation.is_empty:
            key = (task_name, direction, path or "")
            entries[key] = node.annotation
        for name, child in (node.children or {}).items():
            child_path = f"{path}.{name}" if path else name
            _walk(child, child_path)
        if node.dynamic:
            dynamic_path = f"{path}.*" if path else "*"
            _walk(node.dynamic, dynamic_path)

    _walk(tree, "")
    return entries


def _node_reference(node: orm.Node) -> Dict[str, Any]:
    label = getattr(node, "label", None) or getattr(node, "process_label", None)
    if not label:
        description = getattr(node, "description", None)
        label = description() if callable(description) else node.__class__.__name__
    return {
        "uuid": str(node.uuid),
        "label": label,
        "node_type": node.__class__.__name__,
    }


def _normalize_semantics_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(entry)
    merged_relations: Dict[str, Any] = {}
    explicit_relations = normalized.get("relations")
    if isinstance(explicit_relations, dict):
        merged_relations.update(explicit_relations)
    for key in list(normalized.keys()):
        if key in {
            "label",
            "iri",
            "@id",
            "@type",
            "rdf_types",
            "context",
            "@context",
            "attributes",
            "relations",
            "source_node",
            "socket",
        }:
            continue
        if key.startswith("@"):  # keep JSON-LD keywords intact
            continue
        merged_relations[key] = normalized.pop(key)
    if merged_relations:
        normalized["relations"] = merged_relations
    return normalized


def _extract_semantics(node: orm.Node) -> List[Dict[str, Any]]:
    try:
        semantics = node.base.extras.get("semantics")
    except AttributeError:
        semantics = None
    if semantics is None:
        return []
    if isinstance(semantics, list):
        return [
            _normalize_semantics_entry(entry)
            for entry in semantics
            if isinstance(entry, dict)
        ]
    if isinstance(semantics, dict):
        return [_normalize_semantics_entry(semantics)]
    return []


def _summarise_node_attributes(node: orm.Node) -> Dict[str, Any]:
    summary: Dict[str, Any] = {}
    if isinstance(node, orm.Dict):
        payload = node.get_dict()
        compact: Dict[str, Any] = {}
        for key, value in payload.items():
            if isinstance(value, (int, float, str, bool)):
                compact[key] = value
        if compact:
            summary["dict"] = compact
    if isinstance(node, orm.StructureData):  # type: ignore[attr-defined]
        try:
            summary["formula"] = node.get_formula()
        except Exception:
            pass
    try:
        value = getattr(node, "value", None)
    except Exception:
        value = None
    if isinstance(value, (int, float, str)):
        summary["value"] = value
    return summary


def _socketref_to_ref(ref: _SocketRef) -> Dict[str, Any]:
    return {
        "graph_uuid": ref.graph_uuid,
        "task": ref.task_name,
        "socket": ref.socket_path,
        "direction": ref.kind,
    }


def _replace_socket_refs(value: Any) -> Any:
    if isinstance(value, _SocketRef):
        return _socketref_to_ref(value)
    if isinstance(value, dict):
        return {k: _replace_socket_refs(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        replaced = [_replace_socket_refs(v) for v in value]
        return replaced if isinstance(value, list) else tuple(replaced)
    return value


def _merge_annotation(
    base: Optional[SemanticsAnnotation], extra: Optional[SemanticsAnnotation]
) -> Optional[SemanticsAnnotation]:
    if base is None:
        return extra
    if extra is None:
        return base
    return base.merge(extra)


def _merge_payload_annotations(
    target: Dict[Tuple[str, str, str], SemanticsAnnotation],
    payloads: Iterable[SemanticsPayload],
) -> None:

    def _sanitize_semantics(raw: Any) -> Any:
        return _replace_socket_refs(raw)

    for pending in payloads:
        if not isinstance(pending, SemanticsPayload):
            continue
        subject = pending.subject
        if not isinstance(subject, _SocketRef):
            continue
        key = (subject.task_name, subject.kind, subject.socket_path)
        extra = SemanticsAnnotation.from_raw(_sanitize_semantics(pending.semantics))
        if extra is None or extra.is_empty:
            continue
        target[key] = _merge_annotation(target.get(key), extra)


def _merge_relation_annotations(
    target: Dict[Tuple[str, str, str], SemanticsAnnotation],
    relations: Iterable[SemanticsRelation],
) -> None:
    for relation in relations:
        if not isinstance(relation, SemanticsRelation):
            continue
        subject = relation.subject
        if not isinstance(subject, _SocketRef):
            continue
        key = (subject.task_name, subject.kind, subject.socket_path)
        payload = {
            "relations": {
                relation.predicate: _replace_socket_refs(
                    relation.values if len(relation.values) > 1 else relation.values[0]
                )
            }
        }
        if relation.label:
            payload["label"] = relation.label
        if relation.context:
            payload["context"] = dict(relation.context)
        extra = SemanticsAnnotation.from_raw(payload)
        target[key] = _merge_annotation(target.get(key), extra)


def _collect_socket_semantics(graph: Any) -> Dict[Tuple[str, str, str], SemanticsAnnotation]:
    entries: Dict[Tuple[str, str, str], SemanticsAnnotation] = {}

    for task in getattr(graph, "tasks", []) or []:
        spec = getattr(task, "spec", None)
        if spec is None:
            continue
        semantics = TaskSemantics.from_specs(spec.inputs, spec.outputs)
        if semantics is None:
            continue
        entries.update(
            _flatten_semantics_tree(
                semantics.inputs,
                task_name=getattr(task, "name", "<task>"),
                direction="input",
            )
        )
        entries.update(
            _flatten_semantics_tree(
                semantics.outputs,
                task_name=getattr(task, "name", "<task>"),
                direction="output",
            )
        )

    pending_raw = graph.knowledge_graph.semantics_buffer
    pending = _normalize_semantics_buffer(pending_raw)
    _merge_payload_annotations(entries, pending.get("payloads", []))
    _merge_relation_annotations(entries, pending.get("relations", []))
    return entries


def _jsonld_from_entries(
    entries: Dict[Tuple[str, str, str], SemanticsAnnotation]
) -> Dict[str, Any]:
    jsonld_entries: List[Dict[str, Any]] = []
    for (task_name, direction, socket_path), annotation in entries.items():
        if annotation is None or annotation.is_empty:
            continue
        payload = _replace_socket_refs(annotation.to_jsonld())
        payload["task"] = task_name
        payload["direction"] = direction
        payload["socket"] = socket_path
        payload["@id"] = f"ng://{task_name}/{direction}/{socket_path or 'socket'}"
        jsonld_entries.append(payload)
    return {"@graph": jsonld_entries}


def build_workflow_knowledge_payload(
    *,
    graph: Any,
    engine_kind: str,
) -> Optional[Dict[str, Any]]:
    metadata = getattr(graph, "_metadata", {}) or {}
    if "definition" in metadata:
        definition = metadata["definition"]
    else:
        definition = metadata
    entries = _collect_socket_semantics(graph)
    if not entries:
        return None

    payload: Dict[str, Any] = {
        "scope": "workflow",
        "workflow": {
            "name": getattr(graph, "name", None),
            "identifier": definition.get("task_identifier") if isinstance(definition, dict) else None,
            "module": definition.get("module") if isinstance(definition, dict) else None,
            "qualname": definition.get("qualname") if isinstance(definition, dict) else None,
            "callable_path": definition.get("callable_path") if isinstance(definition, dict) else None,
            "file_path": definition.get("file_path") if isinstance(definition, dict) else None,
            "package": definition.get("package") if isinstance(definition, dict) else None,
            "package_version": definition.get("package_version") if isinstance(definition, dict) else None,
        },
        "engine_kind": engine_kind,
        "semantics": {
            "jsonld": _jsonld_from_entries(entries),
        },
    }
    return payload


def persist_workflow_knowledge_graph(
    *,
    process_node: orm.WorkflowNode,
    graph: Any,
    engine_kind: str,
) -> Optional[KnowledgeGraphData]:
    print(f"Persisting workflow knowledge for process node {process_node.pk}")
    payload = build_workflow_knowledge_payload(graph=graph, engine_kind=engine_kind)
    if payload is None:
        print("No semantics found; skipping knowledge graph creation.")
        return None
    wf_meta = payload.get("workflow", {})
    knowledge, _ = KnowledgeGraphData.get_or_create_workflow(
        workflow_name=str(wf_meta.get("name") or graph.name),
        callable_path=wf_meta.get("callable_path"),
        package_version=wf_meta.get("package_version"),
        identifier=wf_meta.get("identifier") or graph.name,
        payload=payload,
    )
    try:
        process_node.base.extras.set("knowledge_graph_uuid", knowledge.uuid)
    except Exception:
        pass
    return knowledge
