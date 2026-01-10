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

try:  # pragma: no cover - optional dependency
    from neo4j import GraphDatabase
except Exception:  # pragma: no cover - handled at runtime
    GraphDatabase = None

LOGGER = logging.getLogger(__name__)

# Cached Neo4j driver so callers do not reopen connections repeatedly.
_DRIVER = None

_KG_LABEL = "KnowledgeGraph"
_KG_REQUIRED_PROPERTY_KEYS = ("hash", "uuid", "workflow_json")


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


def _parse_attr_ref(obj: Any) -> Optional[Dict[str, Any]]:
    """Return the inner ``__ng_attr_ref__`` mapping if present."""

    if isinstance(obj, dict) and "__ng_attr_ref__" in obj:
        inner = obj.get("__ng_attr_ref__") or {}
        return dict(inner) if isinstance(inner, dict) else None
    if isinstance(obj, str) and "__ng_attr_ref__" in obj:
        stripped = obj.strip()
        if stripped.startswith("{") and stripped.endswith("}"):
            try:
                parsed = json.loads(stripped)
            except Exception:
                return None
            return _parse_attr_ref(parsed)
    return None


def _is_term_string(value: str, namespaces: Dict[str, Any]) -> bool:
    """Return True if ``value`` looks like a CURIE/IRI in the current namespace context."""

    if "://" in value:
        return True
    if ":" not in value:
        return False
    prefix = value.split(":", 1)[0]
    return prefix in (namespaces or {})


def _literal_display_value(obj: Any) -> str:
    """Return a display-friendly label for a literal value node."""

    ref = _parse_attr_ref(obj)
    key = (ref or {}).get("key")
    if key:
        return str(key)
    return _stringify_literal(obj)


def _literal_value_hash(*, kind: str, raw_value: str) -> str:
    payload = f"v1:{kind}:{raw_value}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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


def _prepare_sockets(
    sockets: Dict[str, Dict[str, Any]], kg_hash: str
) -> List[Dict[str, Any]]:
    prepared: List[Dict[str, Any]] = []
    for sid, meta in sockets.items():
        canonical = meta.get("canonical") or sid
        label = meta.get("label") or canonical
        prepared.append(
            {
                "id": sid,
                "label": label,
                "direction": meta.get("direction"),
                "task": meta.get("task"),
                "port": meta.get("port"),
                "canonical": canonical,
                "name": label,
                "knowledge_hash": kg_hash,
            }
        )
    return prepared


def _prepare_triples(
    triples: Iterable[List[Any]],
    socket_ids: Iterable[str],
    namespaces: Optional[Dict[str, Any]] = None,
) -> Tuple[
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
]:
    """Split triples into socket-to-socket vs literal relations."""

    socket_set = set(socket_ids)
    socket_triples: List[Dict[str, Any]] = []
    data_value_triples: List[Dict[str, Any]] = []
    attr_ref_triples: List[Dict[str, Any]] = []
    term_triples: List[Dict[str, Any]] = []
    concept_triples: List[Dict[str, Any]] = []
    namespaces = namespaces or {}
    for triple in triples or []:
        if not isinstance(triple, (list, tuple)) or len(triple) != 3:
            continue
        subj, pred, obj = triple
        if str(pred) == "rdfs:label":
            # Redundant with the `Socket.label` property, which is already stored.
            continue
        entry_base = {
            "subject": str(subj),
            "predicate": str(pred),
            "raw_predicate": str(pred),
        }
        if isinstance(obj, str) and obj in socket_set:
            socket_triples.append({**entry_base, "object_socket": obj})
            continue
        kind = _value_kind(obj)
        raw_value = _stringify_literal(obj)
        attr_ref = _parse_attr_ref(obj)
        display_value = _literal_display_value(obj)
        is_term = isinstance(obj, str) and _is_term_string(obj, namespaces)
        entry = {
            **entry_base,
            "display_value": display_value,
            "raw_value": raw_value,
            "kind": kind,
            "value_hash": _literal_value_hash(kind=kind, raw_value=raw_value),
            "attr_key": (attr_ref or {}).get("key"),
            "attr_source": (attr_ref or {}).get("source"),
            "attr_socket": (attr_ref or {}).get("socket"),
        }
        if attr_ref:
            attr_ref_triples.append(entry)
        elif is_term:
            term_triples.append(entry)
        elif isinstance(obj, str):
            concept_triples.append(entry)
        else:
            data_value_triples.append(entry)
    return (
        socket_triples,
        data_value_triples,
        attr_ref_triples,
        term_triples,
        concept_triples,
    )


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
    workflow_json: str,
    workflow_name: Optional[str],
    workflow_identifier: Optional[str],
    namespaces_json: str,
    scope: Optional[str],
    engine_kind: Optional[str],
    socket_count: int,
    triple_count: int,
    sockets: List[Dict[str, Any]],
    socket_triples: List[Dict[str, Any]],
    attr_ref_triples: List[Dict[str, Any]],
    term_triples: List[Dict[str, Any]],
    concept_triples: List[Dict[str, Any]],
    data_value_triples: List[Dict[str, Any]],
) -> str:
    """Persist a knowledge graph payload into Neo4j."""

    record = tx.run(
        """
        MERGE (kg:KnowledgeGraph {uuid: $kg_uuid})
        ON CREATE SET kg.created_at = timestamp()
        SET kg.hash = $kg_hash,
            kg.workflow_json = $workflow_json,
            kg.workflow_name = $workflow_name,
            kg.workflow_identifier = $workflow_identifier,
            kg.namespaces_json = $namespaces_json,
            kg.name = coalesce($workflow_name, $workflow_identifier, kg.name, kg.uuid),
            kg.title = coalesce($workflow_name, $workflow_identifier, kg.title, kg.uuid),
            kg.label = coalesce($workflow_name, $workflow_identifier, kg.label, kg.uuid),
            kg.scope = $scope,
            kg.engine_kind = $engine_kind,
            kg.socket_count = $socket_count,
            kg.triple_count = $triple_count
        RETURN kg.uuid AS uuid
        """,
        kg_hash=kg_hash,
        kg_uuid=kg_uuid,
        workflow_json=workflow_json,
        workflow_name=workflow_name,
        workflow_identifier=workflow_identifier,
        namespaces_json=namespaces_json,
        scope=scope,
        engine_kind=engine_kind,
        socket_count=socket_count,
        triple_count=triple_count,
    ).single()
    stored_uuid = record["uuid"] if record else kg_uuid

    tx.run(
        """
        MATCH (kg:KnowledgeGraph {hash: $kg_hash})
        UNWIND $sockets AS socket
        MERGE (s:Socket {id: socket.id, knowledge_hash: $kg_hash})
        SET s.label = coalesce(socket.label, socket.canonical, socket.id),
            s.name = coalesce(socket.name, socket.label, socket.canonical, socket.id),
            s.title = coalesce(socket.name, socket.label, socket.canonical, socket.id),
            s.direction = socket.direction,
            s.task = socket.task,
            s.port = socket.port,
            s.canonical = coalesce(socket.canonical, socket.id)
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

    if data_value_triples:
        tx.run(
            """
            MATCH (kg:KnowledgeGraph {hash: $kg_hash})
            UNWIND $triples AS triple
            MATCH (s:Socket {id: triple.subject, knowledge_hash: $kg_hash})
            MERGE (v:DataValue {hash: triple.value_hash})
            SET v.value = triple.display_value,
                v.value_json = triple.raw_value,
                v.kind = triple.kind,
                v.name = triple.display_value,
                v.title = triple.display_value,
                v.label = triple.display_value
            MERGE (s)-[r:TRIPLE {predicate: triple.predicate}]->(v)
            SET r.raw_predicate = triple.raw_predicate,
                r.literal_value = triple.display_value,
                r.literal_json = triple.raw_value,
                r.literal_kind = triple.kind
            """,
            kg_hash=kg_hash,
            triples=data_value_triples,
        )

    if term_triples:
        tx.run(
            """
            MATCH (kg:KnowledgeGraph {hash: $kg_hash})
            UNWIND $triples AS triple
            MATCH (s:Socket {id: triple.subject, knowledge_hash: $kg_hash})
            MERGE (t:Term {curie: triple.raw_value})
            SET t.name = triple.raw_value,
                t.title = triple.raw_value,
                t.label = triple.raw_value
            MERGE (s)-[r:TRIPLE {predicate: triple.predicate}]->(t)
            SET r.raw_predicate = triple.raw_predicate
            """,
            kg_hash=kg_hash,
            triples=term_triples,
        )

    if concept_triples:
        tx.run(
            """
            MATCH (kg:KnowledgeGraph {hash: $kg_hash})
            UNWIND $triples AS triple
            MATCH (s:Socket {id: triple.subject, knowledge_hash: $kg_hash})
            MERGE (c:Concept {text: triple.raw_value})
            SET c.name = triple.raw_value,
                c.title = triple.raw_value,
                c.label = triple.raw_value
            MERGE (s)-[r:TRIPLE {predicate: triple.predicate}]->(c)
            SET r.raw_predicate = triple.raw_predicate
            """,
            kg_hash=kg_hash,
            triples=concept_triples,
        )

    if attr_ref_triples:
        tx.run(
            """
            MATCH (kg:KnowledgeGraph {hash: $kg_hash})
            UNWIND $triples AS triple
            MATCH (s:Socket {id: triple.subject, knowledge_hash: $kg_hash})
            MERGE (v:AttrRef {hash: triple.value_hash})
            SET v.key = triple.attr_key,
                v.source = triple.attr_source,
                v.socket = triple.attr_socket,
                v.value = triple.display_value,
                v.value_json = triple.raw_value,
                v.kind = triple.kind,
                v.name = triple.display_value,
                v.title = triple.display_value,
                v.label = triple.display_value
            MERGE (s)-[r:TRIPLE {predicate: triple.predicate}]->(v)
            SET r.raw_predicate = triple.raw_predicate,
                r.literal_value = triple.display_value,
                r.literal_json = triple.raw_value,
                r.literal_kind = triple.kind
            """,
            kg_hash=kg_hash,
            triples=attr_ref_triples,
        )

    return stored_uuid


def _find_existing_knowledge_graph(
    tx, *, kg_hash: str, workflow_name: Optional[str]
) -> Optional[str]:
    """Return an existing KG UUID that matches hash (and workflow name if given)."""

    try:
        tokens = tx.run(
            """
            CALL db.labels() YIELD label
            WITH collect(label) AS labels
            CALL db.propertyKeys() YIELD propertyKey
            WITH labels, collect(propertyKey) AS property_keys
            RETURN $kg_label IN labels AS has_label,
                   all(key IN $required_keys WHERE key IN property_keys) AS has_required_keys
            """,
            kg_label=_KG_LABEL,
            required_keys=list(_KG_REQUIRED_PROPERTY_KEYS),
        ).single()
        if tokens and not (tokens.get("has_label") and tokens.get("has_required_keys")):
            return None
    except Exception:
        # If token introspection isn't available (permissions / DB version),
        # fall back to running the query below.
        pass

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
    workflow_identifier = workflow_meta.get("identifier") or workflow_meta.get(
        "qualname"
    )
    graph_uuid = (
        payload.get("graph_uuid")
        or semantics.get("graph_uuid")
        or semantics.get("dag_id")
    )
    if not graph_uuid:
        graph_uuid = str(uuid4())
    sockets = semantics.get("sockets") or {}
    # Backfill socket labels from legacy `rdfs:label` triples if needed.
    for triple in semantics.get("triples", []) or []:
        if not isinstance(triple, (list, tuple)) or len(triple) != 3:
            continue
        subj, pred, obj = triple
        if str(pred) != "rdfs:label":
            continue
        subj = str(subj)
        if subj not in sockets:
            continue
        if sockets.get(subj, {}).get("label"):
            continue
        if isinstance(obj, str) and obj.strip():
            sockets[subj]["label"] = obj.strip()
    namespaces = semantics.get("namespaces") or semantics.get("context") or {}
    namespaces_json = _serialize_json(namespaces)
    workflow_json = _serialize_json(payload.get("workflow", {}))
    (
        socket_triples,
        data_value_triples,
        attr_ref_triples,
        term_triples,
        concept_triples,
    ) = _prepare_triples(
        semantics.get("triples", []),
        sockets.keys(),
        namespaces=namespaces,
    )
    driver = _get_neo4j_driver()
    with driver.session() as session:
        # Reuse existing KG with the same hash and workflow name to avoid duplicates.
        existing = session.execute_read(
            _find_existing_knowledge_graph,
            kg_hash=kg_hash,
            workflow_name=str(workflow_name).lower() if workflow_name else None,
        )
        kg_uuid = existing or str(graph_uuid)
        # Always execute the write: MERGE updates metadata and fills in missing
        # display-friendly properties (e.g. socket labels) even for reused KGs.
        kg_uuid = session.execute_write(
            _write_knowledge_graph,
            kg_hash=kg_hash,
            kg_uuid=kg_uuid,
            workflow_json=workflow_json,
            workflow_name=str(workflow_name) if workflow_name else None,
            workflow_identifier=str(workflow_identifier)
            if workflow_identifier
            else None,
            namespaces_json=namespaces_json,
            scope=payload.get("scope"),
            engine_kind=payload.get("engine_kind"),
            socket_count=len(sockets),
            triple_count=len(socket_triples)
            + len(data_value_triples)
            + len(attr_ref_triples)
            + len(term_triples)
            + len(concept_triples),
            sockets=_prepare_sockets(sockets, kg_hash),
            socket_triples=socket_triples,
            attr_ref_triples=attr_ref_triples,
            term_triples=term_triples,
            concept_triples=concept_triples,
            data_value_triples=data_value_triples,
        )
    return kg_uuid


def fetch_knowledge_graph(
    graph_uuid: str, *, include_metadata: bool = False
) -> Dict[str, Any]:
    """Fetch a knowledge graph payload from Neo4j by UUID.

    By default this returns the semantics payload (namespaces/sockets/triples).
    Set ``include_metadata=True`` to return a wrapper payload including workflow
    and engine metadata under ``payload['semantics']``.
    """

    driver = _get_neo4j_driver()
    with driver.session() as session:
        record = session.execute_read(
            lambda tx: tx.run(
                """
                MATCH (kg:KnowledgeGraph {uuid: $uuid})
                OPTIONAL MATCH (kg)-[:HAS_SOCKET]->(s:Socket)
                WITH kg, collect(DISTINCT {
                    id: s.id,
                    label: s.label,
                    direction: s.direction,
                    task: s.task,
                    port: s.port,
                    canonical: s.canonical
                }) AS sockets
                OPTIONAL MATCH (kg)-[:HAS_SOCKET]->(sub:Socket)-[r:TRIPLE]->(obj)
                WITH kg, sockets, collect(
                    CASE
                        WHEN 'Socket' IN labels(obj) THEN [sub.id, r.predicate, obj.id]
                        WHEN 'Term' IN labels(obj) THEN [sub.id, r.predicate, obj.curie]
                        WHEN 'Concept' IN labels(obj) THEN [sub.id, r.predicate, obj.text]
                        WHEN 'AttrRef' IN labels(obj) THEN [
                            sub.id,
                            r.predicate,
                            coalesce(
                                r.literal_json,
                                obj.value_json,
                                r.literal_value,
                                obj.value
                            )
                        ]
                        WHEN 'DataValue' IN labels(obj) THEN [
                            sub.id,
                            r.predicate,
                            coalesce(
                                r.literal_json,
                                obj.value_json,
                                r.literal_value,
                                obj.value
                            )
                        ]
                        WHEN 'Scalar' IN labels(obj) THEN [
                            sub.id,
                            r.predicate,
                            coalesce(
                                r.literal_json,
                                obj.value_json,
                                r.literal_value,
                                obj.value
                            )
                        ]
                        ELSE null
                    END
                ) AS triples
                RETURN kg.uuid AS uuid,
                       kg.hash AS hash,
                       kg.workflow_json AS workflow_json,
                       kg.workflow_name AS workflow_name,
                       kg.workflow_identifier AS workflow_identifier,
                       kg.scope AS scope,
                       kg.engine_kind AS engine_kind,
                       kg.namespaces_json AS namespaces_json,
                       sockets AS sockets,
                       [t IN triples WHERE t IS NOT NULL] AS triples
                """,
                uuid=graph_uuid,
            ).single()
        )
    if not record:
        raise ValueError(f"No knowledge graph with UUID {graph_uuid} found in Neo4j")

    sockets_list = record.get("sockets") or []
    sockets: Dict[str, Dict[str, Any]] = {}
    for entry in sockets_list:
        if not entry:
            continue
        sid = entry.get("id")
        if not sid:
            continue
        sockets[str(sid)] = {k: v for k, v in dict(entry).items() if k != "id"}

    triples = record.get("triples") or []

    namespaces: Dict[str, Any] = {}
    namespaces_raw = record.get("namespaces_json")
    if namespaces_raw:
        try:
            namespaces = json.loads(namespaces_raw)
        except Exception:
            namespaces = {}

    semantics: Dict[str, Any] = {
        "graph_uuid": str(record.get("uuid") or graph_uuid),
        "namespaces": namespaces,
        "sockets": sockets,
        "triples": triples,
    }

    workflow_raw = record.get("workflow_json")
    workflow: Dict[str, Any] = {}
    if workflow_raw:
        try:
            workflow = json.loads(workflow_raw) or {}
        except Exception:
            workflow = {}

    if not include_metadata:
        return semantics

    return {
        "graph_uuid": str(record.get("uuid") or graph_uuid),
        "hash": record.get("hash"),
        "scope": record.get("scope"),
        "engine_kind": record.get("engine_kind"),
        "workflow": workflow,
        "semantics": semantics,
    }


def fetch_all_knowledge_graphs(scope: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return lightweight summaries of all knowledge graphs stored in Neo4j."""

    driver = _get_neo4j_driver()
    query = "MATCH (kg:KnowledgeGraph) "
    if scope:
        query += "WHERE kg.scope = $scope "
    query += (
        "RETURN kg.uuid AS uuid, kg.hash AS hash, kg.workflow_json AS workflow_json, "
        "kg.scope AS scope, kg.engine_kind AS engine_kind, "
        "kg.socket_count AS socket_count, kg.triple_count AS triple_count"
    )
    with driver.session() as session:
        records = session.execute_read(
            lambda tx: list(tx.run(query, scope=scope or None))
        )
    summaries: List[Dict[str, Any]] = []
    for rec in records:
        workflow_raw = rec.get("workflow_json")
        summaries.append(
            {
                "uuid": rec["uuid"],
                "hash": rec.get("hash"),
                "scope": rec.get("scope"),
                "engine_kind": rec.get("engine_kind"),
                "socket_count": rec.get("socket_count"),
                "triple_count": rec.get("triple_count"),
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
    identifier = (
        definition.get("task_identifier") if isinstance(definition, dict) else None
    )
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
            "module": definition.get("module")
            if isinstance(definition, dict)
            else None,
            "qualname": definition.get("qualname")
            if isinstance(definition, dict)
            else None,
            "callable_path": definition.get("callable_path")
            if isinstance(definition, dict)
            else None,
            "file_path": definition.get("file_path")
            if isinstance(definition, dict)
            else None,
            "package": definition.get("package")
            if isinstance(definition, dict)
            else None,
            "package_version": definition.get("package_version")
            if isinstance(definition, dict)
            else None,
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

    run_id = str(
        getattr(process_node, "uuid", None) or getattr(process_node, "pk", None)
    )

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
                "run_id": run_id,
                "task": process_label,
                "socket": raw_label,
                "canonical_socket": canonical_socket,
            }
            try:
                node.base.extras.set("semantics_ref", ref)
            except Exception:
                pass

    _walk(process_node)
