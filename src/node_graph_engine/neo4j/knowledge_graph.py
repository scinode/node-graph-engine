"""Knowledge graph helpers for ontology semantics and workflow metadata."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple
from uuid import UUID, uuid4

from aiida import orm
from aiida.common.links import LinkType
from node_graph.knowledge_graph import KnowledgeGraph

try:  # pragma: no cover - optional dependency
    from neo4j import GraphDatabase
except Exception:  # pragma: no cover - handled at runtime
    GraphDatabase = None

LOGGER = logging.getLogger(__name__)

# Cached Neo4j driver so callers do not reopen connections repeatedly.
_DRIVER = None


def _hash_semantics_payload(payload: Dict[str, Any]) -> str:
    """Return a stable hash for a semantics/knowledge-graph payload."""

    semantics = payload.get("semantics") or payload
    serialized = json.dumps(semantics, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _serialize_json(value: Any) -> str:
    """Return a JSON string suitable for Neo4j property storage."""

    return json.dumps(value, sort_keys=True, default=str)


def _get_neo4j_driver():
    """Return a singleton Neo4j driver configured from environment variables."""

    global _DRIVER
    if _DRIVER is not None:
        return _DRIVER
    if GraphDatabase is None:
        raise RuntimeError(
            "Neo4j driver is not installed. Install the 'neo4j' package to enable "
            "knowledge graph persistence."
        )
    uri = os.getenv("NODE_GRAPH_NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NODE_GRAPH_NEO4J_USER")
    password = os.getenv("NODE_GRAPH_NEO4J_PASSWORD")
    auth = (user, password or "") if user else None
    _DRIVER = GraphDatabase.driver(uri, auth=auth)
    return _DRIVER


def _stringify_literal(obj: Any) -> str:
    """Best-effort stable string representation for literals stored in Neo4j."""

    if isinstance(obj, (str, int, float, bool)):
        return str(obj)
    try:
        return json.dumps(obj, sort_keys=True, default=str)
    except Exception:
        return str(obj)


def _value_kind(obj: Any) -> str:
    """Return a light-weight type marker for a literal value."""

    if obj is None:
        return "none"
    if isinstance(obj, bool):
        return "bool"
    if isinstance(obj, (int, float)):
        return "number"
    if isinstance(obj, dict):
        return "dict"
    if isinstance(obj, (list, tuple, set)):
        return "list"
    return "string"


def _prepare_sockets(sockets: Dict[str, Dict[str, Any]], kg_hash: str) -> List[Dict[str, Any]]:
    prepared: List[Dict[str, Any]] = []
    for sid, meta in sockets.items():
        prepared.append(
            {
                "id": sid,
                "label": meta.get("label"),
                "direction": meta.get("direction"),
                "task": meta.get("task"),
                "port": meta.get("port"),
                "canonical": meta.get("canonical") or sid,
                "knowledge_hash": kg_hash,
            }
        )
    return prepared


def _prepare_triples(
    triples: Iterable[List[Any]],
    socket_ids: Iterable[str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Split triples into socket-to-socket vs literal relations."""

    socket_set = set(socket_ids)
    socket_triples: List[Dict[str, Any]] = []
    literal_triples: List[Dict[str, Any]] = []
    for triple in triples or []:
        if not isinstance(triple, (list, tuple)) or len(triple) != 3:
            continue
        subj, pred, obj = triple
        entry_base = {
            "subject": str(subj),
            "predicate": str(pred),
            "raw_predicate": str(pred),
        }
        if isinstance(obj, str) and obj in socket_set:
            socket_triples.append({**entry_base, "object_socket": obj})
            continue
        literal_triples.append(
            {
                **entry_base,
                "value": _stringify_literal(obj),
                "kind": _value_kind(obj),
            }
        )
    return socket_triples, literal_triples


def _is_valid_uuid(value: Any) -> bool:
    """Return True if ``value`` is a valid UUID string."""

    try:
        UUID(str(value))
        return True
    except Exception:
        return False


def _write_knowledge_graph(
    tx,
    *,
    kg_hash: str,
    kg_uuid: str,
    semantics: Dict[str, Any],
    semantics_json: str,
    workflow: Dict[str, Any],
    workflow_json: str,
    scope: Optional[str],
    engine_kind: Optional[str],
    sockets: List[Dict[str, Any]],
    socket_triples: List[Dict[str, Any]],
    literal_triples: List[Dict[str, Any]],
) -> str:
    """Persist a knowledge graph payload into Neo4j."""

    record = tx.run(
        """
        MERGE (kg:KnowledgeGraph {uuid: $kg_uuid})
        ON CREATE SET kg.created_at = timestamp()
        SET kg.hash = $kg_hash,
            kg.payload_json = $semantics_json,
            kg.workflow_json = $workflow_json,
            kg.scope = $scope,
            kg.engine_kind = $engine_kind
        RETURN kg.uuid AS uuid
        """,
        kg_hash=kg_hash,
        kg_uuid=kg_uuid,
        semantics_json=semantics_json,
        workflow_json=workflow_json,
        scope=scope,
        engine_kind=engine_kind,
    ).single()
    stored_uuid = record["uuid"] if record else kg_uuid

    tx.run(
        """
        MATCH (kg:KnowledgeGraph {hash: $kg_hash})
        UNWIND $sockets AS socket
        MERGE (s:Socket {id: socket.id, knowledge_hash: $kg_hash})
        SET s.label = socket.label,
            s.direction = socket.direction,
            s.task = socket.task,
            s.port = socket.port,
            s.canonical = socket.canonical
        MERGE (kg)-[:HAS_SOCKET]->(s)
        """,
        kg_hash=kg_hash,
        sockets=sockets,
    )

    if socket_triples:
        tx.run(
            """
            MATCH (kg:KnowledgeGraph {hash: $kg_hash})
            UNWIND $triples AS triple
            MATCH (s:Socket {id: triple.subject, knowledge_hash: $kg_hash})
            MATCH (o:Socket {id: triple.object_socket, knowledge_hash: $kg_hash})
            MERGE (s)-[r:TRIPLE {predicate: triple.predicate}]->(o)
            SET r.raw_predicate = triple.raw_predicate
            """,
            kg_hash=kg_hash,
            triples=socket_triples,
        )

    if literal_triples:
        tx.run(
            """
            MATCH (kg:KnowledgeGraph {hash: $kg_hash})
            UNWIND $triples AS triple
            MATCH (s:Socket {id: triple.subject, knowledge_hash: $kg_hash})
            MERGE (v:Value {value: triple.value, kind: triple.kind})
            MERGE (s)-[r:TRIPLE {predicate: triple.predicate}]->(v)
            SET r.raw_predicate = triple.raw_predicate,
                r.literal_value = triple.value,
                r.literal_kind = triple.kind
            """,
            kg_hash=kg_hash,
            triples=literal_triples,
        )

    return stored_uuid


def _find_existing_knowledge_graph(
    tx, *, kg_hash: str, workflow_name: Optional[str]
) -> Optional[str]:
    """Return an existing KG UUID that matches hash (and workflow name if given)."""

    records = tx.run(
        """
        MATCH (kg:KnowledgeGraph {hash: $kg_hash})
        RETURN kg.uuid AS uuid, kg.workflow_json AS workflow_json
        """,
        kg_hash=kg_hash,
    )
    for rec in records:
        candidate_uuid = rec.get("uuid")
        wf_raw = rec.get("workflow_json")
        wf_name = None
        if wf_raw:
            try:
                wf = json.loads(wf_raw)
                wf_name = wf.get("name") or wf.get("identifier")
            except Exception:
                wf_name = None
        if workflow_name is not None:
            if wf_name is None or str(wf_name).lower() != workflow_name:
                continue
        # If the stored uuid is not a valid UUID, upgrade it in place.
        if not _is_valid_uuid(candidate_uuid):
            new_uuid = str(uuid4())
            tx.run(
                """
                MATCH (kg:KnowledgeGraph {hash: $kg_hash, uuid: $old_uuid})
                SET kg.uuid = $new_uuid
                """,
                kg_hash=kg_hash,
                old_uuid=candidate_uuid,
                new_uuid=new_uuid,
            )
            return new_uuid
        return candidate_uuid
    return None


def store_knowledge_graph(payload: Dict[str, Any]) -> str:
    """Persist the provided semantics payload into Neo4j and return its UUID."""

    semantics = payload.get("semantics") or payload
    kg_hash = payload.get("hash") or _hash_semantics_payload(payload)
    workflow_meta = payload.get("workflow") or {}
    workflow_name = workflow_meta.get("name") or workflow_meta.get("identifier")
    graph_uuid = payload.get("graph_uuid") or semantics.get("graph_uuid") or semantics.get("dag_id")
    if not graph_uuid:
        graph_uuid = str(uuid4())
    sockets = semantics.get("sockets") or {}
    semantics_json = _serialize_json(semantics)
    workflow_json = _serialize_json(payload.get("workflow", {}))
    socket_triples, literal_triples = _prepare_triples(
        semantics.get("triples", []),
        sockets.keys(),
    )
    driver = _get_neo4j_driver()
    with driver.session() as session:
        # Reuse existing KG with the same hash and workflow name to avoid duplicates.
        existing = session.execute_read(
            _find_existing_knowledge_graph,
            kg_hash=kg_hash,
            workflow_name=str(workflow_name).lower() if workflow_name else None,
        )
        if existing:
            return existing
        kg_uuid = session.execute_write(
            _write_knowledge_graph,
            kg_hash=kg_hash,
            kg_uuid=str(graph_uuid),
            semantics=semantics,
            semantics_json=semantics_json,
            workflow=payload.get("workflow", {}),
            workflow_json=workflow_json,
            scope=payload.get("scope"),
            engine_kind=payload.get("engine_kind"),
            sockets=_prepare_sockets(sockets, kg_hash),
            socket_triples=socket_triples,
            literal_triples=literal_triples,
        )
    return kg_uuid


def fetch_knowledge_graph(graph_uuid: str) -> Dict[str, Any]:
    """Fetch a knowledge graph payload from Neo4j by UUID."""

    driver = _get_neo4j_driver()
    with driver.session() as session:
        record = session.execute_read(
            lambda tx: tx.run(
                """
                MATCH (kg:KnowledgeGraph {uuid: $uuid})
                RETURN kg.payload_json AS payload_json,
                       kg.hash AS hash,
                       kg.workflow_json AS workflow_json
                """,
                uuid=graph_uuid,
            ).single()
        )
    if not record:
        raise ValueError(f"No knowledge graph with UUID {graph_uuid} found in Neo4j")
    payload_raw = record.get("payload_json")
    payload = json.loads(payload_raw) if payload_raw else {}
    if "hash" not in payload and record.get("hash"):
        payload["hash"] = record.get("hash")
    workflow_raw = record.get("workflow_json")
    if workflow_raw:
        try:
            payload.setdefault("workflow", json.loads(workflow_raw))
        except Exception:
            payload.setdefault("workflow", {})
    return payload


def fetch_all_knowledge_graphs(scope: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return lightweight summaries of all knowledge graphs stored in Neo4j."""

    driver = _get_neo4j_driver()
    query = "MATCH (kg:KnowledgeGraph) "
    if scope:
        query += "WHERE kg.scope = $scope "
    query += "RETURN kg.uuid AS uuid, kg.payload_json AS payload_json, kg.hash AS hash, kg.workflow_json AS workflow_json"
    with driver.session() as session:
        records = session.execute_read(
            lambda tx: list(tx.run(query, scope=scope or None))
        )
    summaries: List[Dict[str, Any]] = []
    for rec in records:
        payload_raw = rec.get("payload_json")
        workflow_raw = rec.get("workflow_json")
        summaries.append(
            {
                "uuid": rec["uuid"],
                "payload": json.loads(payload_raw) if payload_raw else {},
                "hash": rec.get("hash"),
                "workflow": json.loads(workflow_raw) if workflow_raw else {},
            }
        )
    return summaries

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
    if not getattr(kg, "graph_uuid", None):
        kg.graph_uuid = str(uuid4())
    kg.update()
    if not kg.entities and not kg.links:
        return None

    semantics_payload = kg.to_dict()
    payload_hash = _hash_semantics_payload({"semantics": semantics_payload})
    payload: Dict[str, Any] = {
        "scope": "workflow",
        "graph_uuid": str(kg.graph_uuid),
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
) -> Optional[str]:
    print(f"Persisting workflow knowledge for process node {process_node.pk}")
    payload = build_workflow_knowledge_payload(graph=graph, engine_kind=engine_kind)
    if payload is None:
        print("No semantics found; skipping knowledge graph creation.")
        return None
    try:
        kg_uuid = store_knowledge_graph(payload)
    except Exception as exc:  # pragma: no cover - depends on runtime Neo4j availability
        LOGGER.error("Failed to persist knowledge graph to Neo4j: %s", exc)
        return None
    try:
        process_node.base.extras.set("knowledge_graph_uuid", kg_uuid)
    except Exception:
        pass
    _attach_semantics_references(process_node, knowledge_uuid=str(kg_uuid))
    return kg_uuid


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
