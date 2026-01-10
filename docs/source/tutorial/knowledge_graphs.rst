Knowledge graphs (workflow semantics)
====================================

.. note::
   The Local engine persists workflow knowledge graphs out of the box. Other engines
   call the same helper (``persist_workflow_knowledge_graph``); ensure integration is
   enabled for your backend.

Install Neo4j
-------------


.. code-block:: bash

  pip install neo4j
  docker run   -p7474:7474    -p7687:7687    -d    -e NEO4J_AUTH=neo4j/secretgraph    neo4j:latest
  export NODE_GRAPH_NEO4J_URI="neo4j://localhost"
  export NODE_GRAPH_NEO4J_USER="neo4j"
  export NODE_GRAPH_NEO4J_PASSWORD="secretgraph"

What gets stored
----------------

- **Neo4j knowledge graph** is written once per workflow version (keyed by a stable hash)
  and referenced from the workflow ``ProcessNode`` extras.
- **Semantics source**
  - Socket annotations in task definitions (inputs/outputs)
- Runtime semantics relations/annotations are buffered internally on
  ``graph.knowledge_graph``.
- **Merge strategy**: annotations and runtime additions for the same socket are
  merged into a single payload. Attachments referencing sockets are resolved to
  lightweight references; nothing is duplicated per run.
- **Format**: stored *normalized* in Neo4j (no duplicated full JSON blob):
  - ``(:KnowledgeGraph)`` stores workflow/engine metadata plus ``namespaces_json``.
  - ``(:Socket)`` nodes store socket metadata (task/direction/port/label/canonical).
  - ``[:TRIPLE]`` relationships encode semantic predicates, pointing either to another
    ``(:Socket)`` or a ``(:Value)`` literal node.
  - The semantics payload (``namespaces``, ``sockets``, ``triples``) is reconstructed
    on demand and matches ``Graph.knowledge_graph.to_dict()``.

  .. code-block:: json

     {
       "namespaces": {"qudt": "http://qudt.org/schema/qudt/", "rdf": "...", "rdfs": "..."},
       "sockets": {"task.output.result": {"task": "task", "direction": "output", "port": "result", "label": "Band gap"}},
       "triples": [["task.output.result", "rdf:type", "qudt:QuantityValue"], ["task.output.result", "rdfs:label", "Band gap"]]
     }

Where it is stored
------------------

Knowledge graphs are persisted to Neo4j (configure via ``NODE_GRAPH_NEO4J_URI``,
``NODE_GRAPH_NEO4J_USER``, ``NODE_GRAPH_NEO4J_PASSWORD``). The workflow
``ProcessNode`` that created it stores the UUID under
``process_node.base.extras['knowledge_graph_uuid']`` for quick lookup.

Retrieving a knowledge graph
----------------------------

Example (``verdi shell``) to fetch a workflow knowledge graph by UUID stored on the workflow node:

.. code-block:: python

   from node_graph_engine.neo4j.knowledge_graph import fetch_knowledge_graph
   from aiida import orm

   kg_uuid = "your-knowledge-graph-uuid"
   semantics = fetch_knowledge_graph(kg_uuid)
   print("Sockets:", semantics["sockets"])
   print("Triples:", semantics["triples"])

Fetch metadata + semantics in one payload:

.. code-block:: python

   payload = fetch_knowledge_graph(kg_uuid, include_metadata=True)
   print("Workflow:", payload["workflow"])
   print("Engine:", payload["engine_kind"])
   print("Sockets:", payload["semantics"]["sockets"])

Use ``KnowledgeGraph.from_dict`` and visualising directly inside Jupyter-notebook:

.. code-block:: python

   from node_graph.knowledge.graph import KnowledgeGraph

   kg = KnowledgeGraph.from_dict(payload)
   kg


Notes and scope
---------------

- High-throughput friendly: semantics are stored once per workflow version, not
  per run, avoiding repeated JSON-LD blobs.
- Per-run nodes remain lean: they only carry the socket-level ontology payload
  (label/IRI/context/attributes). Agents should follow standard AiiDA provenance
  (creator process links) and, if needed, the workflow knowledge graph UUID on
  the workflow ``ProcessNode`` to marry runtime values with the workflow schema.
- Runtime additions (relations, extra attributes) are merged with the static
  annotations, so user-provided additions on sockets are preserved.
- Node-level knowledge snapshots are not stored; link from workflow knowledge to
  run nodes via provenance if you need concrete values.

Schema at a glance
------------------

- **Sockets**: metadata for each socket (task, direction, port, label).
- **Triples**: ``[subject, predicate, object]`` with subjects as socket IDs
  (``task.direction.socket``), predicates from ontology IRIs/CURIEs, and objects
  as socket IDs, IRIs, or literals.
- **Context**: merged JSON-LD context from annotations/runtime additions plus RDF/RDFS
  prefixes (available via ``semantics['namespaces']``).
