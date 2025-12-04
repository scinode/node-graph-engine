Knowledge graphs (workflow semantics)
====================================

.. note::
   This feature is available in the Local engine; other engines can call the
   same helper (``persist_workflow_knowledge``) to emit knowledge graphs once
   integration is added.

What gets stored
----------------

- **KnowledgeGraphData** (AiiDA ``KnowledgeGraphData`` node) is written once per workflow
  version (keyed by workflow name, callable path, package version).
- **Semantics source**
  - Socket annotations in task definitions (inputs/outputs)
  - Runtime semantics attachments/relations buffered in ``graph.semantics_buffer``
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
- Runtime attachments (relations, extra attributes) are merged with the static
  annotations, so user-provided additions on sockets are preserved.
- Node-level knowledge snapshots are not stored; link from workflow knowledge to
  run nodes via provenance if you need concrete values.

Next steps
----------

- Integrate ``persist_workflow_knowledge`` into other engines (Prefect, Dask,
  etc.) to make the behaviour backend-agnostic.
- Optional: export knowledge graphs to a triple store (GraphDB/Fuseki/RDFLib) for
  SPARQL over multiple workflows/profiles.
