"""
Ontology-aware semantics
========================

node-graph Engine already records rich *structural* provenance through AiiDA
link types, but structure alone does not explain what a piece of data
*means*. The ontology-aware semantics feature lets you attach domain
vocabularies to sockets and automatically export that context alongside
individual AiiDA ``Data`` nodes as JSON-LD snippets. This page provides
enough background for readers new to ontologies, explains how the
implementation works, and finishes with a runnable example you can adapt to
your plugins.
"""

# %%
# Why ontologies and semantics?
# -----------------------------
#
# **Ontology (plain words)**: a curated dictionary of concepts and their
# relationships. Scientific ontologies describe things like “potential energy,”
# “graphene,” or “defect” and standardise how they are referenced. Examples:
#
# - **QUDT** (Quantities, Units, Dimensions and Data) – defines physical
#   quantities and units, e.g. ``qudt:PotentialEnergy`` or
#   ``qudt-unit:EV``.
# - **PROV-O** (W3C Provenance Ontology) – describes provenance concepts
#   such as ``prov:Entity`` (a data artefact), ``prov:Activity`` (a process),
#   and ``prov:used`` (an input relation).
# - **OBO / NOMAD / in-house schemas** – any controlled vocabulary you care
#   about can be referenced, whether public or private.
#
# **Semantics**: the act of tagging actual data with ontology identifiers so
# machines can tell *what* a number represents. Two floating-point values
# become distinct once one is tagged as “cohesive energy in eV” and the
# other as “temperature in K”.
#
# Benefits—even if you have zero ontology experience today:
#
# - **Interoperability** – exports from your workflows can be ingested by
#   ELNs, data portals, or SPARQL endpoints without custom glue code.
# - **Queryability** – by recording predicates like ``qudt:unit`` or
#   ``schema:material`` you can answer questions such as “show me all
#   workflows that emitted a cohesive energy in eV during March”.
# - **Traceability** – annotations become machine-readable documentation
#   explaining *why* a port exists and how to interpret it.

# %%
# Why structural provenance alone is insufficient
# ----------------------------------------------
#
# AiiDA’s native provenance (``INPUT_CALC``, ``INPUT_WORK``, ``CREATE``,
# ``CALL_CALC``) already captures *who-used-what*. However, those links do
# **not** store:
#
# - The physical meaning of a socket (is ``result`` energy, force,
#   magnetisation?).
# - Units, reference systems, or ontology terms.
# - Relationships to external datasets, materials IDs, or DOIs.
#
# When you share an AiiDA export the graph is consistent, but collaborators
# still need tribal knowledge to interpret each port. Ontology annotations
# let you declare the domain semantics directly at authoring time so the
# meaning travels with the data.


# %%
# How node-graph maps annotations into JSON-LD snippets
# ----------------------------------------------------
#
# The feature builds on the existing provenance recorder:
#
# 1. **Collect annotations** – every socket’s ``meta.semantics`` payload is inspected.
#    Payloads under ``semantics`` (or shorthand keys like ``iri``/``label``)
#    are normalised into internal ``SemanticsAnnotation`` objects that
#    remember ontology IDs, RDF types, namespace prefixes, custom
#    attributes, and relations.
# 2. **Observe execution** – when a task finishes, Graph flattens its
#    outputs and matches socket paths (``result``, ``stress__xx``…)
#    against the stored annotations. It performs the same matching on
#    incoming ``INPUT_CALC``/``INPUT_WORK`` links so consumer nodes can
#    record what they used.
# 3. **Emit JSON-LD snippets** – for each annotated socket the engine
#    builds a small JSON-LD payload (containing ``@context``, ``@id``,
#    ``@type``, and any predicates you supplied) and stores it alongside
#    the ``Data`` node that travelled through that socket.
# 4. **Resolve cross-socket references** – relation values can include
#    dotted socket paths like ``"outputs.band"`` that the engine rewrites
#    to ``aiida://node/...`` references pointing to sibling sockets.
#    This lets you say things like “this StructureData input has the
#    BandStructureData produced by the graph” without duplicating
#    provenance.
# 5. **Persist** – extras are appended directly to the produced/consumed
#    ``Data`` nodes under ``node.base.extras['semantics']`` as a list of
#    records, so the annotations remain available even after exporting the
#    provenance.
#
# The result is a per-node JSON-LD breadcrumb that semantic tooling can
# ingest while AiiDA continues to guarantee structural integrity.


# %%
# Feature summary and use cases
# -----------------------------
#
# - **Socket annotations drive everything** – keep using ``Float``/``Dict``
#   nodes; annotate sockets via ``meta(semantics={...})`` to add semantics.
# - **Namespace merging** – per-socket ``context`` dictionaries define
#   prefixes (``{"qudt": "http://qudt.org/schema/qudt/"}``); the engine
#   merges them into the JSON-LD ``@context`` automatically.
# - **Flexible attributes/relations** – use ``semantics.attributes`` for
#   predicate/value pairs (units, uncertainties, DOIs) and
#   ``semantics.relations`` for references to other resources (values can
#   themselves include ``{"@id": ...}``).
# - **Per-node storage** – input and output ``Data`` nodes receive their
#   respective semantics payload, enabling provenance-aware database
#   queries without extra joins. Because relation values can reference
#   other sockets via dotted paths (e.g. ``"outputs.result"``), you can
#   declare facts like “this workflow input has the band structure
#   produced downstream” without copying process metadata into extras.
# - **Typed authoring** – pass ``SemanticTag`` (a Pydantic model) with
#   your own enums instead of raw dictionaries to get IDE autocompletion
#   and validation. Known prefixes automatically pull in their
#   ``@context`` URLs, so ``qudt:unit`` does not require repeating the
#   namespace.
# - **Context defaults** – the engine ships with a small namespace registry
#   (``qudt``, ``qudt-unit``, ``prov``, ``schema``). If a predicate or IRI uses
#   one of those prefixes and no ``context`` entry is provided, the registry
#   value is injected. Extend or override globally via
#   ``register_namespace(prefix, iri)`` or per-annotation by setting
#   ``context`` explicitly.
# - **Engine-agnostic** – the same annotations work across Local, Airflow,
#   Dask, remote PythonJob, etc.


# %%
# Declaring cross-socket statements
# ---------------------------------
#
# Multi-step workflows often derive properties in later nodes but you may
# want to attach those properties to an *earlier* artefact such as the
# structure that kicked off the pipeline. Use dotted socket paths inside
# ``semantics.relations`` (or ``attributes``) to point at the socket that
# carries the property. During execution the engine replaces the path with
# an ``aiida://node/...`` reference, so the subject ``Data`` node keeps a
# live link to the derived artefact. The references are scoped to the
# inputs and outputs of the *current* task, avoiding any hard-coded
# downstream consumer knowledge.
#
#
from node_graph import task
from node_graph.socket_spec import meta, namespace
from typing import Annotated, Any

STRUCTURE_SEMANTICS = meta(
    semantics={
        "label": "Crystal structure",
        "context": {"mat": "https://example.org/mat#"},
        "relations": {
            "mat:hasProperty": [
                {
                    "socket": "outputs.result",
                    "label": "Band structure property",
                    "context": {"mat": "https://example.org/mat#"},
                }
            ],
        },
    }
)


@task()
def compute_band_structure(structure):
    return 1.0


@task.graph()
def workflow(structure: Annotated[str, STRUCTURE_SEMANTICS]):
    return compute_band_structure(structure=structure).result


# %%
# Running ``workflow`` records the band-structure semantics on the output as
# usual, and also adds a ``mat:hasProperty`` relation to the input structure
# that points at the produced ``BandStructureData`` node with the supplied
# label. This makes queries like “give me every StructureData with a band
# structure property” possible without encoding workflow-specific knowledge.
#
# Executing the snippet below prints the resulting JSON-LD records for both
# sockets so you can see the resolved ``aiida://node`` reference:
#
#

from node_graph_engine.engines.local import LocalEngine
from aiida import load_profile, orm
import json

load_profile()

graph = workflow.build(structure="test")
engine = LocalEngine()
outputs = engine.run(graph)
structure_node = orm.load_node(engine._graph_pid).inputs.structure
print(json.dumps(structure_node.base.extras.get("semantics"), indent=2))

# %%
# Attaching semantics inside workflows
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# The above cross-socket references work when the subject and object sockets
# are part of the same task. For the sockets belong to different nodes,
# you can use the ``attach_semantics`` helper function to append
# relationships at runtime.
# Call ``attach_semantics`` with a predicate, the subject, and
# one or more property sockets. The helper records the intent on the
# graph object and resolves the referenced sockets to ``aiida://node/...``
# identifiers once the workflow has finished.
#
#
from node_graph.semantics import attach_semantics


@task()
def generate(structure):
    return structure


@task()
def compute_density_of_states(structure):
    return 1.0


@task.graph()
def workflow(
    structure,
) -> Annotated[dict, namespace(output_structure=Any, bands=Any, dos=Any)]:
    mutated = generate(structure=structure).result
    bands = compute_band_structure(structure=mutated).result
    dos = compute_density_of_states(structure=mutated).result
    attach_semantics(
        mutated,
        objects=[bands, dos],
        predicate="emmo:hasProperty",
        semantics={"label": "Generated structure", "iri": "emmo:Material"},
        label="Generated structure",
        context={"emmo": "https://emmo.info/emmo#"},
        socket_label="result",
    )
    return {"output_structure": mutated, "bands": bands, "dos": dos}


# %%
# ``label`` and ``context`` describe the JSON-LD record you attach (not the
# individual relation targets). ``socket_label`` points at which socket on the
# subject node the metadata should be associated with—in this case the
# ``result`` output of ``generate``. Relation targets resolve to lightweight
# ``aiida://node/...`` references; their display label is picked from any
# semantics already stored on that node, or the node/process label as a
# fallback.


graph = workflow.build(structure="test")
engine = LocalEngine()
outputs = engine.run(graph)
print(json.dumps(outputs["output_structure"].base.extras.get("semantics"), indent=2))

# %%
# When building the graph, the AiiDA data is not yet available, so we pass
# the sockets themselves as arguments.
# After execution, converts any ``Data`` objects passed as relation targets
# into ``aiida://node/...`` references and appends/replaces the corresponding
# JSON-LD entry on the subject node.
#
#
# Use case 1 — publishable provenance bundle
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Goal: accompany a workflow result (e.g. a phase diagram) with a
# machine-readable packet that citeable repositories can ingest.
#
# 1. Annotate the sockets you care about:
#
#    .. code-block:: python
#
#       meta(
#           semantics={
#               "label": "Formation energy",
#               "iri": "qudt:Energy",
#               "rdf_types": ["qudt:QuantityValue"],
#               "attributes": {"qudt:unit": "qudt-unit:EV"},
#               "context": {"qudt": "http://qudt.org/schema/qudt/"},
#           }
#       )
#
# 2. Execute the workflow as usual. After the run, fetch
#    ``result_node.base.extras['semantics']`` for the outputs you plan to
#    publish.
# 3. Package the JSON-LD snippets next to the usual ``verdi archive``
#    output or upload them to a SPARQL endpoint.
#
# Result: reviewers or collaborators can visualise/validate provenance with
# RDF tooling (RDFLib, GraphDB, TopBraid) without recreating your
# environment.
#
# Use case 2 — semantic validation in CI
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Goal: ensure every published workflow includes specific semantic fields
# (e.g. a QUDT unit).
#
# 1. Write a pytest rule that queries produced ``Data`` nodes and inspects
#    ``semantics_payload = data_node.base.extras['semantics']``.
# 2. Assert that each entry has a ``qudt:unit`` predicate.
# 3. Fail CI if the assertion does not hold.
#
# Result: developers receive immediate feedback when a socket lacks
# metadata, keeping semantic debt under control.
#
# Use case 3 — linking to external repositories
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Goal: reference existing materials databases, ELNs, or DOIs directly from
# your provenance.
#
# 1. Add a relation entry:
#
#    .. code-block:: python
#
#       meta(
#           semantics={
#               "label": "Relaxed structure",
#               "relations": {
#                   "schema:isBasedOn": {"@id": "https://materialsproject.org/materials/mp-149"},
#               },
#           }
#       )
#
# 2. After execution, the JSON-LD snippet stored on the output includes a
#    link to the external identifier.
#
# Result: you can stitch together experimental ELNs, literature DOIs, and
# simulation archives without bespoke schema translations.


# %%
# Attaching ontology hints to sockets
# -----------------------------------
#
# Every socket spec exposes a dedicated ``meta.semantics`` attribute
# alongside the legacy ``meta.extras`` dictionary. The semantics helper
# looks for:
#
# - ``meta.semantics`` (or ``meta.extras['semantics']`` / ``ontology`` /
#   ``prov``) – the primary payload. Fields you can use:
#
#   - ``label`` – human-readable description.
#   - ``iri`` – canonical identifier for the concept (e.g.
#     ``qudt:PotentialEnergy`` or
#     ``https://purl.obolibrary.org/obo/CHEBI_27568``).
#   - ``rdf_types`` – list of additional ``@type`` entries (e.g.
#     ``qudt:QuantityValue``).
#   - ``context`` – prefix-to-IRI map so you can use short identifiers.
#   - ``attributes`` – predicate/value pairs (units, uncertainty, DOI,
#     temperature, etc.).
#   - ``relations`` – nested dictionaries describing links to other
#     resources (values can themselves be ``{"@id": ...}`` or plain
#     strings).
#
# - Convenience keys (``iri``, ``label``, ``rdf_types``) – if present
#   outside the main ``semantics`` payload (e.g. declared directly in
#   ``meta.extras``), they are folded into the payload so legacy
#   annotations still work.
# - Arbitrary extra metadata – anything else in ``meta.extras`` is
#   untouched, so you can track workflow-specific hints alongside semantics.
#
# During execution the engine stores the annotation on every ``Data`` node
# that crosses the annotated socket, so the information is available to
# QueryBuilder searches or post-processing scripts without touching the
# parent process nodes.


# %%
# Runnable example
# ----------------
#
# The script below builds a minimal workflow that computes a lattice energy
# with ASE+EMT, annotates the output socket with QUDT terms (note how the
# unit is declared with the ``qudt:unit`` predicate), executes the graph
# locally, and prints the resulting JSON-LD snippets. It requires an active
# AiiDA profile and the Local engine.

import json
import typing as t

from aiida import load_profile, orm
from node_graph import task
from node_graph.socket_spec import meta
from node_graph_engine.engines.local import LocalEngine

try:  # pragma: no cover - optional dependency for documentation builds
    from ase import Atoms
    from ase.build import bulk
except Exception:  # pragma: no cover - optional dependency
    Atoms = None  # type: ignore[assignment]
    bulk = None  # type: ignore[assignment]


profile_loaded = False
try:  # pragma: no cover - load_profile interacts with global state
    load_profile()
    profile_loaded = True
except Exception as exc:  # pragma: no cover - documentation build environments may skip AiiDA
    print(f"Skipping execution because no AiiDA profile is available: {exc}")


SEMANTICS: t.Dict[str, t.Any] = {
    "label": "Cohesive energy",
    "iri": "qudt:PotentialEnergy",
    "rdf_types": ["qudt:QuantityValue"],
    "context": {
        "qudt": "http://qudt.org/schema/qudt/",
        "qudt-unit": "http://qudt.org/vocab/unit/",
    },
    "attributes": {"qudt:unit": "qudt-unit:EV"},
    "relations": {
        "schema:isBasedOn": {
            "@id": "https://materialsproject.org/materials/mp-149",
        }
    },
}


@task()
def calc_energy(
    atoms: Atoms,
) -> t.Annotated[
    float,
    meta(
        semantics=SEMANTICS,
        extras={"workflow_hint": "emt-energy"},
    ),
]:
    """Return EMT potential energy and attach ontology metadata."""

    from ase.calculators.emt import EMT  # imported lazily for the gallery

    atoms.set_calculator(EMT())
    return atoms.get_potential_energy()


@task.graph()
def EnergyWorkflow(atoms: Atoms):
    """Single-step workflow so we get provenance + semantics automatically."""

    return calc_energy(atoms=atoms).result


if not profile_loaded or Atoms is None or bulk is None:
    print(
        "Ontology semantics demo requires AiiDA + ASE; install dependencies to run the example."
    )
else:
    aluminum = bulk("Al", "fcc", a=4.05)
    graph = EnergyWorkflow.build(atoms=aluminum)
    engine = LocalEngine(name="ontology-demo")
    outputs = engine.run(graph)
    print("\nGraph result:", outputs)

    for label, output in outputs.items():
        payload = output.base.extras.get("semantics")
        print(f"\nOutput '{label}' semantics records:")
        print(json.dumps(payload, indent=2))

    workflow_node = orm.load_node(engine._graph_pid)
    outgoing = workflow_node.base.links.get_outgoing()
    for entry in outgoing:
        try:
            semantics_payload = entry.node.base.extras.get("semantics")
        except AttributeError:
            semantics_payload = None
        if semantics_payload:
            print(
                f"\nData node '{entry.link_label}' carries {len(semantics_payload)} semantic entries."
            )


# %%
# Decoding the example annotations
# --------------------------------
#
# Each entry stored under ``node.base.extras['semantics']`` is the JSON-LD
# representation of your annotation. You will see:
#
# - ``@context`` with prefixes declared under ``semantics.context``.
# - ``@id`` derived from ``semantics.iri``.
# - ``@type`` mirroring ``semantics.rdf_types``.
# - Literal predicates from ``semantics.attributes`` and
#   relationship predicates from ``semantics.relations``.
#
# Because the payload lives on the ``Data`` node itself, you can query it via
# AiiDA’s ``QueryBuilder`` or export it with the usual provenance bundles.


# %%
# EOS workflow example
# --------------------
#
# The :doc:`tutorial on the Equation of State workflow </eos_workflow>`
# already builds a multi-step graph with relaxation, structure generation,
# bulk EMT calculations, and a Birch-Murnaghan fit. Below we extend that
# tutorial with semantic annotations so the fitted parameters and
# intermediate energy/volume points carry ontology metadata.
#
# Annotating the EOS tasks
# ~~~~~~~~~~~~~~~~~~~~~~~~
#
# Only two tasks need changes: the per-structure energy/volume calculator
# and the final EOS fitting task. The snippet below shows the additions
# (new ``meta`` semantics payloads are highlighted). You would paste these
# definitions into the tutorial notebook or script before the
# ``eos_workflow`` graph declaration.
#
# .. note::
#    The code below is for illustration and is not run as part of this script.
#
# .. code-block:: python
#
#    from typing import Annotated
#    from node_graph import meta, namespace, task
#    from ase import Atoms
#    from ase.calculators.emt import EMT
#
#
#    ENERGY_META = meta(
#        semantics={
#            "label": "Cohesive energy",
#            "iri": "qudt:PotentialEnergy",
#            "rdf_types": ["qudt:QuantityValue"],
#            "context": {
#                "qudt": "http://qudt.org/schema/qudt/",
#                "qudt-unit": "http://qudt.org/vocab/unit/",
#            },
#            "attributes": {"qudt:unit": "qudt-unit:EV"},
#        }
#    )
#
#    VOLUME_META = meta(
#        semantics={
#            "label": "Cell volume",
#            "iri": "qudt:Volume",
#            "rdf_types": ["qudt:QuantityValue"],
#            "context": {"qudt": "http://qudt.org/schema/qudt/"},
#            "attributes": {"qudt:unit": "qudt-unit:AA3"},
#        }
#    )
#
#
#    @task()
#    def calculate_energy_and_volume(atoms: Atoms) -> Annotated[
#        dict,
#        namespace(energy=ENERGY_META, volume=VOLUME_META),
#    ]:
#        atoms = Atoms.fromdict(atoms)
#        atoms calc = EMT()
#        atoms.get_potential_energy()
#        return {
#            "energy": atoms.calc.results["energy"],
#            "volume": atoms.get_volume(),
#        }
#
#
#    @task()
#    def fit_eos_model(data: Annotated[dict, "dynamic(dict)"]) -> Annotated[
#        dict,
#        namespace(
#            v0_A3=meta(
#                semantics={
#                    "label": "Equilibrium volume",
#                    "iri": "qudt:Volume",
#                    "attributes": {"qudt:unit": "qudt-unit:AA3"},
#                }
#            ),
#            e0_eV=meta(
#                semantics={
#                    "label": "Minimum energy",
#                    "iri": "qudt:PotentialEnergy",
#                    "attributes": {"qudt:unit": "qudt-unit:EV"},
#                }
#            ),
#            B_GPa=meta(
#                semantics={
#                    "label": "Bulk modulus",
#                    "iri": "qudt:BulkModulus",
#                    "rdf_types": ["qudt:QuantityValue"],
#                    "context": {
#                        "qudt": "http://qudt.org/schema/qudt/",
#                        "qudt-unit": "http://qudt.org/vocab/unit/",
#                    },
#                    "attributes": {"qudt:unit": "qudt-unit:GPA"},
#                }
#            ),
#        ),
#    ]:
#        from ase.eos import Equation Of State
#        from ase.units import kJ
#
#        volumes_list = [value["volume"] for value in data.values()]
#        energies_list = [value["energy"] for value in data values()]
#
#        eos = EquationOfState(volumes_list, energies_list)
#        v0, e0, B = eos.fit()
#        B_GPa = B / kJ * 1.0e24
#        return {"v0_A3": v0, "e0_eV": e0, "B_GPa": B_GPa}
#
#
# Running the EOS workflow with semantics
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# With the annotated tasks in place, the existing ``eos_workflow``
# definition from the tutorial needs no further changes. Build the graph,
# run it with your preferred engine (Local is shown here), and inspect the
# stored semantics on the resulting ``Data`` nodes:
#
# .. code-block:: python
#
#    # This block assumes `eos_workflow` graph is defined from the other tutorial
#
#    # from ase.build import bulk
#    # from aiida run
#
# Implementation details
# --------------------------
# The semantics feature is implemented by extending the existing
# ``TaskMeta`` to store the ``TaskSemantics`` object when building the
# task executor.
# When executing task, the engine will store the semantic information with the
# AiiDA Data nodes as extras.
# If there is cross-socket references, the engine will resolve them to `aiida://node/...` format.
