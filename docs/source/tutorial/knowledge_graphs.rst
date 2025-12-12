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
- **Format**: stored under ``payload['semantics']`` using
  ``Graph.knowledge_graph.to_dict()`` (JSON-LD is reconstructed on demand):

  .. code-block:: json

     {
       "graph_uuid": "abc123",
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

   workflow = orm.load_node("workflow-uuid-here")
   kg_uuid = workflow.base.extras.get("knowledge_graph_uuid")
   semantics = fetch_knowledge_graph(str(kg_uuid))
   print("Graph UUID:", kg_uuid)
   print("Sockets:", semantics["sockets"])
   print("Triples:", semantics["triples"])

Use ``KnowledgeGraph.from_dict`` and RDFLib/GraphViz helpers to reconstruct JSON-LD for export:

.. code-block:: python

   import json
   from rdflib import Graph
   from node_graph.knowledge_graph import KnowledgeGraph

   kg = KnowledgeGraph.from_dict(semantics, graph_uuid=kg_uuid)
   jsonld = json.loads(kg.as_rdflib().serialize(format="json-ld"))
   g = Graph().parse(data=json.dumps(jsonld), format="json-ld")
   g.serialize("knowledge.ttl", format="turtle")

Visualising
-----------

- **JSON-LD playground**: copy ``jsonld`` into https://json-ld.org/playground/ to
  explore IRIs, types, and relations.
  - Make sure you paste valid JSON (double quotes). Use ``json.dumps(jsonld)`` to
    get a JSON string; Python reprs with single quotes will be rejected.
- **Graphviz (local)**: convert JSON-LD to DOT via RDFLib + ``rdf2dot``:

  .. code-block:: python

     from rdflib import Graph
     import json

     g = Graph().parse(data=json.dumps(jsonld), format="json-ld")
     g.serialize("knowledge.ttl", format="turtle")
     # convert TTL -> DOT -> PNG (requires graphviz installed)
     # python -m rdflib.tools.rdf2dot knowledge.ttl | dot -Tpng -o knowledge.png

- This is complementary to the standard AiiDA provenance graph: provenance shows
  lineage; the knowledge graph shows merged ontology semantics for the workflow
  definition.

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
- **JSON-LD**: preserved under ``payload['semantics']['jsonld']`` for interoperability.

Interpreting and visualising
----------------------------

- Resolution path for agents/queries:
  - **Property-first (common chat flow)**: search semantics (by IRI/label/unit) across data nodes → for each hit, follow AiiDA links to the creator process → climb to the root workflow → read ``knowledge_graph_uuid`` → look up the matching socket in the KG to normalise meaning/units → use provenance to fetch related inputs/structures/sibling properties.
  - **Schema-first**: search the workflow KG for sockets matching an IRI/label (e.g. ``qudt:BulkModulus``) → enumerate workflow runs that reference that KG (via ``knowledge_graph_uuid`` on the workflow process) → pull produced data nodes via provenance → convert/align units using KG attributes.
  - **Data-first**: given a specific ``Data`` UUID, follow provenance to its creator/root workflow, fetch the KG, and interpret/compare via the canonical socket (``task``/``direction``/``socket``).
- Visualisation: load ``payload['semantics']['jsonld']`` into RDFLib and render with GraphViz
  (see example above). Socket identifiers (``ng://…``) make it easy to see which part of the
  workflow a property belongs to.

Next steps
----------

- Integrate ``persist_workflow_knowledge`` into other engines (Prefect, Dask,
  etc.) to make the behaviour backend-agnostic.
- Optional: export knowledge graphs to a triple store (GraphDB/Fuseki/RDFLib) for
  SPARQL over multiple workflows/profiles.
