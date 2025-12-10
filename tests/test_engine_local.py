from __future__ import annotations

from aiida import orm
from node_graph import task
from node_graph.socket_spec import meta
from node_graph.semantics import SemanticTag
from node_graph.semantics import attach_semantics, attribute_ref
from node_graph_engine.engines.local import LocalEngine
from node_graph_engine.orm.data.knowledge_graph import KnowledgeGraphData
from typing import Annotated, Dict, List


SEMANTICS_EXAMPLE = {
    "label": "Cohesive energy",
    "iri": "qudt:PotentialEnergy",
    "rdf_types": ["qudt:QuantityValue"],
    "context": {
        "qudt": "http://qudt.org/schema/qudt/",
        "qudt-unit": "http://qudt.org/vocab/unit/",
    },
    "attributes": {"qudt:unit": "qudt-unit:EV"},
    "relations": {"schema:isBasedOn": {"@id": "https://example.org/materials/mp-149"}},
}

INPUT_SEMANTICS_EXAMPLE = {
    "label": "Thermal energy",
    "iri": "qudt:ThermodynamicTemperature",
    "context": {"qudt": "http://qudt.org/schema/qudt/"},
}

STRUCTURE_PROPERTY_SEMANTICS = {
    "label": "Crystal structure",
    "context": {"mat": "https://example.org/mat#"},
    "relations": {
        "mat:hasProperty": [
            {
                "socket": "outputs.result",
                "label": "Band structure property",
                "context": {"mat": "https://example.org/mat#"},
            }
        ]
    },
}

BAND_STRUCTURE_SEMANTICS = {
    "label": "Band structure",
    "context": {"mat": "https://example.org/mat#"},
}

MANUAL_STRUCTURE_SEMANTICS = {
    "label": "Generated structure (manual annotation)",
    "context": {"mat": "https://example.org/mat#"},
}

DENSITY_OF_STATES_SEMANTICS = {
    "label": "Density of states",
    "context": {"mat": "https://example.org/mat#"},
}

SEMANTICS_TAG_EXAMPLE = SemanticTag(
    label="Cohesive energy",
    iri="qudt:PotentialEnergy",
    rdf_types=("qudt:QuantityValue",),
    attributes={"qudt:unit": "qudt-unit:EV"},
)


def _load_knowledge_graph(process_node: orm.ProcessNode):
    """Return (KG node, semantics payload) for the workflow."""

    kg_uuid = process_node.base.extras.get("knowledge_graph_uuid")
    assert kg_uuid is not None
    kg = orm.load_node(kg_uuid)
    payload = kg.get_dict()
    semantics = payload.get("semantics") or payload
    return kg, semantics


def test_nested_graph(nested_graph) -> None:
    """Test execution of a nested graph using the LocalEngine."""
    ng = nested_graph.build(x=1, y=2, z=3)
    engine = LocalEngine()
    result = engine.run(ng)
    assert result["result"] == 20
    pn = orm.load_node(engine._graph_pid)
    assert pn.is_finished_ok
    assert len(pn.called) == 3  # 2 add tasks and 1 multiply task
    assert pn.called[0].process_label == "add"
    assert pn.called[1].process_label == "Graph<add_multiply>"
    assert pn.called[1].inputs.x == pn.called[0].outputs.result
    assert pn.called[1].outputs.sum == 5
    assert pn.called[1].outputs.product == 15
    assert pn.called[2].process_label == "add1"
    assert pn.called[2].inputs.x == pn.called[1].outputs.sum
    assert pn.called[2].inputs.y == pn.called[1].outputs.product


def test_async_task_execution(async_add_task) -> None:
    """Ensure async callables are executed to completion."""

    @task.graph()
    def async_flow(x, y):
        return async_add_task(x=x, y=y)

    ng = async_flow.build(x=4, y=5)

    engine = LocalEngine()
    result = engine.run(ng)

    assert result["result"] == 9


def test_socket_semantics_roundtrip() -> None:
    @task()
    def compute_energy() -> Annotated[float, meta(semantics=SEMANTICS_EXAMPLE)]:
        return 1.234

    @task.graph()
    def energy_flow():
        return compute_energy().result

    engine = LocalEngine()
    outputs = engine.run(energy_flow.build())

    result_node = outputs["result"]
    assert isinstance(result_node, orm.Float)

    # Per-run semantics are referenced via the workflow knowledge graph to avoid
    # duplicating JSON-LD on every Data node.
    assert "semantics" not in result_node.base.extras.all
    ref = result_node.base.extras.get("semantics_ref")
    assert ref["canonical_socket"] == "compute_energy.output.result"

    calc_node = result_node.creator
    assert calc_node is not None
    assert "semantics" not in calc_node.base.extras.all

    workflow_node = orm.load_node(engine._graph_pid)
    assert "semantics" not in workflow_node.base.extras.all
    _, semantics = _load_knowledge_graph(workflow_node)
    socket_meta = semantics["sockets"]["compute_energy.output.result"]
    assert socket_meta["label"] == "Cohesive energy"
    triples = semantics.get("triples", [])
    assert ["compute_energy.output.result", "qudt:unit", "qudt-unit:EV"] in triples


def test_attribute_ref_defaults_to_subject_node() -> None:
    @task()
    def emit_number() -> Annotated[
        float,
        meta(
            semantics={
                "label": "Number",
                "attributes": {"schema:value": attribute_ref("value")},
            }
        ),
    ]:
        return 3.5

    @task.graph()
    def flow():
        return emit_number().result

    engine = LocalEngine()
    outputs = engine.run(flow.build())
    node = outputs["result"]
    assert "semantics" not in node.base.extras.all
    ref = node.base.extras.get("semantics_ref")
    assert "canonical_socket" in ref
    workflow_node = orm.load_node(engine._graph_pid)
    _, semantics = _load_knowledge_graph(workflow_node)
    socket_entry = semantics["sockets"][ref["canonical_socket"]]
    # Attribute refs remain recorded in the KG semantics attributes.
    triples = semantics.get("triples", [])
    assert any(
        triple[0] == ref["canonical_socket"] and triple[1] == "schema:value"
        for triple in triples
    )


def test_semantics_accepts_pydantic_tag() -> None:
    @task()
    def compute_energy() -> Annotated[float, meta(semantics=SEMANTICS_TAG_EXAMPLE)]:
        return 2.5

    @task.graph()
    def energy_flow():
        return compute_energy().result

    engine = LocalEngine()
    outputs = engine.run(energy_flow.build())
    result_node = outputs["result"]
    assert result_node.base.extras.get("semantics") is None
    ref = result_node.base.extras.get("semantics_ref")
    workflow_node = orm.load_node(engine._graph_pid)
    _, semantics = _load_knowledge_graph(workflow_node)
    socket_meta = semantics["sockets"][ref["canonical_socket"]]
    assert socket_meta["label"] == "Cohesive energy"
    triples = semantics.get("triples", [])
    assert ["compute_energy.output.result", "qudt:unit", "qudt-unit:EV"] in triples


def test_input_socket_semantics() -> None:
    @task()
    def compute_energy() -> Annotated[float, meta(semantics=SEMANTICS_EXAMPLE)]:
        return 5.0

    @task()
    def consume_energy(
        value: Annotated[float, meta(semantics=INPUT_SEMANTICS_EXAMPLE)]
    ) -> float:
        return value + 1.0

    @task.graph()
    def energy_flow():
        energy = compute_energy()
        consumed = consume_energy(value=energy.result)
        return consumed.result

    engine = LocalEngine()
    engine.run(energy_flow.build())

    workflow_node = orm.load_node(engine._graph_pid)
    _, semantics = _load_knowledge_graph(workflow_node)
    sockets = semantics["sockets"]
    labels = {meta.get("label") for meta in sockets.values()}
    assert {"Cohesive energy", "Thermal energy"} <= labels


def test_input_semantics_can_reference_outputs() -> None:
    @task()
    def compute_band_structure(
        structure: dict,
    ) -> Annotated[dict, meta(semantics=BAND_STRUCTURE_SEMANTICS)]:
        return {"bands": [1.0, 2.0]}

    @task.graph()
    def band_structure_workflow(
        structure: Annotated[orm.Dict, meta(semantics=STRUCTURE_PROPERTY_SEMANTICS)]
    ):
        return compute_band_structure(structure=structure).result

    structure = orm.Dict(dict={"kind": "fcc"}).store()
    structure_uuid = structure.uuid
    engine = LocalEngine()
    engine.run(band_structure_workflow.build(structure=structure))

    workflow_node = orm.load_node(engine._graph_pid)
    _, semantics = _load_knowledge_graph(workflow_node)
    triples = semantics.get("triples", [])
    assert any("mat:hasProperty" in triple[1] for triple in triples)


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
            ]
        },
    }
)


def test_semantics_without_annotated_meta() -> None:
    """Plain ``meta`` values passed to ``namespace`` should retain semantics."""

    @task()
    def compute_band_structure(structure):
        return 1.0

    @task.graph()
    def workflow(structure: Annotated[str, STRUCTURE_SEMANTICS]):
        return compute_band_structure(structure=structure).result

    engine = LocalEngine()
    engine.run(workflow.build(structure="test"))
    workflow_node = orm.load_node(engine._graph_pid)
    _, semantics = _load_knowledge_graph(workflow_node)
    labels = {meta.get("label") for meta in semantics.get("sockets", {}).values()}
    assert "Crystal structure" in labels


def test_manual_semantics_attachment_handles_multiple_properties() -> None:
    @task()
    def generate_new_structure(
        structure: orm.Dict | dict,
    ) -> Annotated[orm.Dict, meta(semantics=MANUAL_STRUCTURE_SEMANTICS)]:
        if isinstance(structure, orm.Dict):
            data = dict(structure.get_dict())
        else:
            data = dict(structure)
        count = data.get("count", 0) + 1
        return orm.Dict(dict={"count": count})

    @task()
    def compute_band_structure(
        structure: orm.Dict | dict,
    ) -> Annotated[orm.Dict, meta(semantics=BAND_STRUCTURE_SEMANTICS)]:
        if isinstance(structure, orm.Dict):
            data = structure.get_dict()
        else:
            data = dict(structure)
        count = data.get("count", 0)
        return orm.Dict(dict={"bands": [float(count), float(count + 1)]})

    @task()
    def compute_density_of_states(
        structure: orm.Dict | dict,
    ) -> Annotated[orm.Dict, meta(semantics=DENSITY_OF_STATES_SEMANTICS)]:
        if isinstance(structure, orm.Dict):
            data = structure.get_dict()
        else:
            data = dict(structure)
        count = data.get("count", 0)
        return orm.Dict(dict={"density": [float(count), float(count) + 0.5]})

    @task.graph()
    def structure_workflow(structure: orm.Dict):
        first = generate_new_structure(structure=structure).result
        first_band = compute_band_structure(structure=first).result
        first_dos = compute_density_of_states(structure=first).result
        attach_semantics(
            first,
            objects=[first_band, first_dos],
            predicate="mat:hasProperty",
            label=MANUAL_STRUCTURE_SEMANTICS["label"],
            context=MANUAL_STRUCTURE_SEMANTICS["context"],
            socket_label="result",
        )
        second = generate_new_structure(structure=first).result
        second_band = compute_band_structure(structure=second).result
        second_dos = compute_density_of_states(structure=second).result
        attach_semantics(
            second,
            objects=[second_band, second_dos],
            predicate="mat:hasProperty",
            label=MANUAL_STRUCTURE_SEMANTICS["label"],
            context=MANUAL_STRUCTURE_SEMANTICS["context"],
            socket_label="result",
        )
        return second_band

    root_structure = orm.Dict(dict={"count": 0}).store()
    engine = LocalEngine()
    engine.run(structure_workflow.build(structure=root_structure))

    workflow_node = orm.load_node(engine._graph_pid)
    _, semantics = _load_knowledge_graph(workflow_node)
    triples = semantics.get("triples", [])
    property_triples = [
        t for t in triples if t[1] == "mat:hasProperty"
    ]
    assert len(property_triples) >= 4
    # Ensure both band structure and DOS properties were linked.
    objects = {t[2] for t in property_triples}
    assert any("compute_band_structure.output.result" in obj for obj in objects)
    assert any("compute_density_of_states.output.result" in obj for obj in objects)
