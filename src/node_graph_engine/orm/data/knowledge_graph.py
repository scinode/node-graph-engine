"""Knowledge graph helpers for ontology semantics and workflow metadata."""

from __future__ import annotations

import json
import hashlib
from typing import Any, Dict, Iterable, List, Optional, Tuple

from aiida import orm
from aiida.common.links import LinkType
from aiida.orm import QueryBuilder
from node_graph.knowledge_graph import KnowledgeGraph


class KnowledgeGraphData(orm.Dict):
    """Light-weight AiiDA container for workflow or node-level knowledge graphs."""


    @property
    def payload(self) -> Dict[str, Any]:  # pragma: no cover - thin wrapper
        return self.get_dict()

    def _as_core_knowledge_graph(self) -> KnowledgeGraph:
        payload = self.get_dict() or {}
        semantics = payload.get("semantics") or payload
        workflow = payload.get("workflow") or {}
        graph_uuid = (
            semantics.get("graph_uuid")
            or semantics.get("dag_id")
            or workflow.get("identifier")
            or workflow.get("name")
        )
        return KnowledgeGraph.from_dict(semantics, graph_uuid=graph_uuid)

    def to_jsonld(self) -> Dict[str, Any]:
        """
        Return a JSON-LD representation of the stored semantics.

        If a legacy ``jsonld`` block is present it is returned verbatim.
        Otherwise we reconstruct JSON-LD from triples + sockets without
        persisting duplicate data in the database.
        """

        payload = self.get_dict() or {}
        semantics = payload.get("semantics") or payload
        jsonld = semantics.get("jsonld") if isinstance(semantics, dict) else None
        if isinstance(jsonld, dict):
            return jsonld
        rdf = self._as_core_knowledge_graph().as_rdflib()
        serialized = rdf.serialize(format="json-ld")
        if isinstance(serialized, bytes):
            serialized = serialized.decode("utf-8")
        return json.loads(serialized)

    def to_rdf_graph(self):
        """Return an ``rdflib.Graph`` parsed from the compact semantics."""

        return self._as_core_knowledge_graph().as_rdflib()

    def to_graphviz(self) -> "Digraph":
        """Render the RDF graph to a Graphviz Digraph (requires graphviz)."""

        return self._as_core_knowledge_graph().to_graphviz()

    def to_graphviz_svg(self) -> str:
        return self._as_core_knowledge_graph().to_graphviz_svg()

    def _repr_svg_(self) -> Optional[str]:  # pragma: no cover - exercised in notebooks
        return self._as_core_knowledge_graph()._repr_svg_()

    def _repr_html_(self) -> Optional[str]:  # pragma: no cover - exercised in notebooks
        return self._as_core_knowledge_graph()._repr_html_()

    @classmethod
    def _maybe_existing(cls, filters: Dict[str, Any]) -> Optional["KnowledgeGraphData"]:
        qb = QueryBuilder()
        qb.append(cls, filters=filters)
        existing = qb.first()
        return existing[0] if existing else None

    @staticmethod
    def _matches_filter_namespaces(node: orm.Node, filters: Dict[str, Any]) -> bool:
        for namespace, expected in filters.items():
            if namespace not in {"extras", "attributes"}:
                continue
            base = getattr(node.base, namespace, None)
            if base is None:
                return False
            data = base.all if hasattr(base, "all") else base
            if not isinstance(data, dict):
                return False
            if not KnowledgeGraphData._dict_matches(data, expected):
                return False
        return True

    @staticmethod
    def _dict_matches(data: Dict[str, Any], expected: Dict[str, Any]) -> bool:
        for key, val in expected.items():
            if key == "has_key":
                if isinstance(val, str) and val not in data:
                    return False
                if isinstance(val, (list, tuple, set)) and any(v not in data for v in val):
                    return False
                continue
            if val is None:
                continue
            if isinstance(val, dict):
                sub = data.get(key)
                if not isinstance(sub, dict):
                    return False
                if not KnowledgeGraphData._dict_matches(sub, val):
                    return False
                continue
            if data.get(key) != val:
                return False
        return True

    @classmethod
    def get_or_create_workflow(
        cls,
        *,
        payload: Dict[str, Any],
        extras: Optional[Dict[str, Any]] = None,
    ) -> Tuple["KnowledgeGraphData", bool]:
        kg_hash = payload.get("hash") or _hash_semantics_payload(payload)
        filters: Dict[str, Any] = {
            "attributes.hash": kg_hash
        }

        existing = cls._maybe_existing(filters)
        if existing:
            return existing, False

        node = cls(dict=payload)
        meta: Dict[str, Any] = {
            "hash": kg_hash,
        }
        workflow_meta = payload.get("workflow") if isinstance(payload, dict) else {}
        if isinstance(workflow_meta, dict):
            meta["identifier"] = workflow_meta.get("identifier")
            meta["callable_path"] = workflow_meta.get("callable_path")
            meta["package_version"] = workflow_meta.get("package_version")
        if extras:
            meta.update(extras)
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

def _hash_semantics_payload(payload: Dict[str, Any]) -> str:
    """Return a stable hash for a semantics/knowledge-graph payload."""

    semantics = payload.get("semantics") or payload
    serialized = json.dumps(semantics, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

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
    identifier = definition.get("task_identifier") if isinstance(definition, dict) else None
    if identifier is None:
        identifier = getattr(graph, "name", None)

    kg = graph.knowledge_graph.copy(graph_uuid=getattr(graph, "uuid", None))
    kg._graph = graph
    kg.update()
    if not kg.entities and not kg.links:
        return None

    semantics_payload = kg.to_dict()
    payload_hash = _hash_semantics_payload({"semantics": semantics_payload})
    payload: Dict[str, Any] = {
        "scope": "workflow",
        "workflow": {
            "name": getattr(graph, "name", None),
            "identifier": identifier,
            "module": definition.get("module") if isinstance(definition, dict) else None,
            "qualname": definition.get("qualname") if isinstance(definition, dict) else None,
            "callable_path": definition.get("callable_path") if isinstance(definition, dict) else None,
            "file_path": definition.get("file_path") if isinstance(definition, dict) else None,
            "package": definition.get("package") if isinstance(definition, dict) else None,
            "package_version": definition.get("package_version") if isinstance(definition, dict) else None,
        },
        "engine_kind": engine_kind,
        "semantics": semantics_payload,
        "hash": payload_hash,
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
        payload=payload,
        extras={"engine_kind": engine_kind},
    )
    try:
        process_node.base.extras.set("knowledge_graph_uuid", knowledge.uuid)
    except Exception:
        pass
    _attach_semantics_references(process_node, knowledge_uuid=str(knowledge.uuid))
    return knowledge


def _attach_semantics_references(
    process_node: orm.ProcessNode, *, knowledge_uuid: str
) -> None:
    """
    Record lightweight references from produced ``Data`` nodes to the knowledge graph.

    This avoids storing full semantics payloads on every run while still letting
    clients resolve the canonical socket/entity for a value.
    """

    visited: set[str] = set()

    def _walk(proc: orm.ProcessNode) -> None:
        for child in getattr(proc, "called", []) or []:
            _walk(child)
        outgoing = proc.base.links.get_outgoing(
            link_type=(LinkType.CREATE, LinkType.RETURN)
        )
        for entry in outgoing:
            node = entry.node
            if not isinstance(node, orm.Data):
                continue
            if node.uuid in visited:
                continue
            visited.add(node.uuid)
            process_label = getattr(proc, "process_label", None)
            raw_label = entry.link_label
            canonical_socket = raw_label
            if process_label:
                canonical_socket = f"{process_label}.output.{raw_label}"
            # Replace ``__`` with ``.`` to match socket identifiers in the KG.
            canonical_socket = canonical_socket.replace("__", ".")
            ref = {
                "knowledge_graph_uuid": str(knowledge_uuid),
                "task": process_label,
                "socket": raw_label,
                "canonical_socket": canonical_socket,
            }
            try:
                node.base.extras.set("semantics_ref", ref)
            except Exception:
                pass

    _walk(process_node)