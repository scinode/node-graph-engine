Knowledge graphs (workflow semantics)
====================================

.. note::
   The Local engine persists workflow knowledge graphs out of the box. Other engines
   call the same helper (``persist_workflow_knowledge_graph``); ensure integration is
   enabled for your backend.

What gets stored
----------------

- **KnowledgeGraphData** (AiiDA ``KnowledgeGraphData`` node) is written once per workflow
  version (keyed by workflow name, callable path, package version).
- **Semantics source**
  - Socket annotations in task definitions (inputs/outputs)
- Runtime semantics attachments/relations buffered in ``graph.knowledge_graph.semantics_buffer`` (falls back to ``graph.semantics_buffer`` for older graphs)
- **Merge strategy**: annotations and runtime attachments for the same socket are
  merged into a single payload. Attachments referencing sockets are resolved to
  lightweight references; nothing is duplicated per run.
- **Format**: stored as JSON-LD in ``payload['semantics']['jsonld']`` (``@graph``
  entries with ``@id``, ``task``, ``direction``, ``socket`` and ontology predicates).

Where it is stored
------------------

Knowledge graphs are persisted as AiiDA ``KnowledgeGraphData`` nodes with extras:

- ``extras.scope = 'workflow'``
- ``extras.workflow_name`` and versioning fields (callable path, package version)
- ``extras.identifier`` (task identifier/name)

The workflow ``WorkflowNode`` that created it also stores the UUID under
``process_node.base.extras['knowledge_graph_uuid']`` for quick lookup.

Retrieving a knowledge graph
----------------------------

Example (``verdi shell``) to fetch the latest workflow knowledge graph:

.. code-block:: python

   from aiida import orm
   from aiida.orm import QueryBuilder
   from node_graph_engine.data.knowledge_graph import KnowledgeGraphData
   import json

   qb = QueryBuilder()
   qb.append(KnowledgeGraphData)
   kg = qb.iterall()[-1][0] if qb.count() > 0 else None
   if kg is None:
       raise RuntimeError("No KnowledgeGraphData with scope=workflow found")
   payload = kg.get_dict()
   jsonld = payload["semantics"]["jsonld"]
   print(json.dumps(jsonld, indent=2))

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
- Runtime attachments (relations, extra attributes) are merged with the static
  annotations, so user-provided additions on sockets are preserved.
- Node-level knowledge snapshots are not stored; link from workflow knowledge to
  run nodes via provenance if you need concrete values.

Schema at a glance
------------------

- **Nodes**: one JSON-LD entry per socket (task/direction/socket). Key fields:
  - ``@id``: ``ng://{task}/{direction}/{socket}`` (stable socket identifier)
  - ``task`` / ``direction`` / ``socket``: back-references to the graph socket
  - ``label`` / ``iri`` / ``rdf_types`` / ``attributes`` / ``relations``: ontology payload
- **Relations**: predicates declared in ``attributes``/``relations`` are preserved.
- **Context**: ``@context`` carries namespace prefixes (``qudt``, ``qudt-unit``, ``prov``, etc.).

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
