from __future__ import annotations

from typing import Any, Dict, List

from click.testing import CliRunner

from node_graph_engine.cli import knowledge_graph


class FakeResult:
    def __init__(self, payload: Dict[str, Any]):
        self._payload = payload

    def single(self) -> Dict[str, Any]:
        return self._payload


class FakeSession:
    def __init__(self, responses: List[Dict[str, Any]]):
        self.responses = list(responses)
        self.run_calls: List[tuple[str, Dict[str, Any]]] = []

    def run(self, query: str, **params: Any) -> FakeResult:
        self.run_calls.append((query.strip(), params))
        if not self.responses:
            raise AssertionError("Ran out of fake responses")
        return FakeResult(self.responses.pop(0))

    def __enter__(self) -> "FakeSession":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # pragma: no cover - no-op
        return None


class FakeDriver:
    def __init__(self, session: FakeSession):
        self.session_obj = session

    def session(self) -> FakeSession:
        return self.session_obj


def test_delete_all_no_graphs(monkeypatch) -> None:
    responses = [{"uuids": []}]
    session = FakeSession(responses)
    driver = FakeDriver(session)
    monkeypatch.setattr("node_graph_engine.cli._get_neo4j_driver", lambda: driver)

    runner = CliRunner()
    result = runner.invoke(knowledge_graph, ["delete", "--all"])

    assert result.exit_code == 0
    assert "No knowledge graphs found to delete." in result.output
    assert session.run_calls[0][0].startswith("MATCH (kg:KnowledgeGraph)" )


def test_delete_multiple_with_confirmation(monkeypatch) -> None:
    responses = [
        {"kg_cnt": 1, "socket_cnt": 2, "value_cnt": 3},  # counts for first
        {"kg_cnt": 0, "socket_cnt": 0, "value_cnt": 0},  # counts for second
        {"removed": 3},  # delete first
        {"removed": 0},  # delete second
    ]
    session = FakeSession(responses)
    driver = FakeDriver(session)
    monkeypatch.setattr("node_graph_engine.cli._get_neo4j_driver", lambda: driver)

    runner = CliRunner()
    result = runner.invoke(
        knowledge_graph,
        ["delete", "uuid-1", "uuid-2"],
        input="y\n",
    )

    assert result.exit_code == 0
    assert "About to delete the following knowledge graphs:" in result.output
    assert "uuid-1 (kg=1, sockets=2, values=3)" in result.output
    assert "uuid-2 (kg=0, sockets=0, values=0)" in result.output
    assert "Deleted knowledge graph uuid-1" in result.output
    assert "No knowledge graph found with UUID uuid-2" in result.output
    # Two count queries + two delete queries should have been issued.
    assert len(session.run_calls) == 4
