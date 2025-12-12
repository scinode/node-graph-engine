"""node-graph-engine CLI extensions (knowledge graph commands) using Click."""

from __future__ import annotations

import json
import sys
from typing import Sequence

import click

from node_graph_engine.neo4j.knowledge_graph import (
    fetch_all_knowledge_graphs,
    fetch_knowledge_graph,
    _get_neo4j_driver,
)


def _format_table(headers: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    """Return a simple ASCII table string."""

    widths = [len(h) for h in headers]
    rendered = []
    for row in rows:
        cells = [str(cell) for cell in row]
        rendered.append(cells)
        for idx, cell in enumerate(cells):
            widths[idx] = max(widths[idx], len(cell))
    sep = " | "
    header_line = sep.join(h.ljust(widths[i]) for i, h in enumerate(headers))
    divider = "-+-".join("-" * widths[i] for i in range(len(headers)))
    body = "\n".join(
        sep.join(cell.ljust(widths[i]) for i, cell in enumerate(row)) for row in rendered
    )
    return "\n".join([header_line, divider, body])


@click.group(name="knowledge-graph")
def knowledge_graph() -> None:
    """Knowledge graph utilities (alias: kg)."""


@knowledge_graph.command("list")
@click.option("--scope", default="workflow", help="Scope filter (e.g. workflow).")
@click.option("--limit", type=int, help="Limit number of rows.")
@click.option("--json", "as_json", is_flag=True, help="Emit full JSON payload.")
def list_kg(scope: str, limit: int | None, as_json: bool) -> None:
    """List knowledge graphs stored in Neo4j."""

    if fetch_all_knowledge_graphs is None:
        click.echo("Knowledge graph listing unavailable (missing engine dependency)", err=True)
        raise SystemExit(1)
    try:
        graphs = fetch_all_knowledge_graphs(scope=scope) or []
    except Exception as exc:  # pragma: no cover - runtime env dependent
        click.echo(f"Failed to fetch knowledge graphs: {exc}", err=True)
        raise SystemExit(1)
    if limit:
        graphs = graphs[:limit]
    if as_json:
        click.echo(json.dumps(graphs, indent=2))
        return
    rows = []
    headers = ["UUID", "Workflow", "Identifier", "Entities", "Triples", "Hash"]
    for g in graphs:
        payload = g.get("payload") or {}
        semantics = payload.get("semantics") or payload
        entities = len((semantics.get("sockets") or {}))
        triples = len((semantics.get("triples") or []))
        wf = g.get("workflow") or {}
        rows.append(
            [
                g.get("uuid") or "",
                wf.get("name") or wf.get("identifier") or "",
                wf.get("identifier") or "",
                entities,
                triples,
                (g.get("hash") or semantics.get("hash") or "")[:12],
            ]
        )
    if not rows:
        click.echo("No knowledge graphs found.")
        return
    click.echo(_format_table(headers, rows))


@knowledge_graph.command("show")
@click.argument("uuid")
def show_kg(uuid: str) -> None:
    """Show a knowledge graph payload by UUID."""

    if fetch_knowledge_graph is None:
        click.echo("Knowledge graph fetch unavailable (missing engine dependency)", err=True)
        raise SystemExit(1)
    try:
        payload = fetch_knowledge_graph(uuid)
    except Exception as exc:  # pragma: no cover - runtime env dependent
        click.echo(f"Failed to fetch knowledge graph: {exc}", err=True)
        raise SystemExit(1)
    click.echo(json.dumps(payload, indent=2))


@knowledge_graph.command("delete")
@click.argument("uuid")
@click.option("--force", is_flag=True, help="Do not prompt for confirmation.")
def delete_kg(uuid: str, force: bool) -> None:
    """Delete a knowledge graph (and its sockets/values) by UUID."""

    if not force:
        driver = _get_neo4j_driver()
        with driver.session() as session:
            counts = session.run(
                """
                MATCH (kg:KnowledgeGraph {uuid: $uuid})
                OPTIONAL MATCH (kg)-[:HAS_SOCKET]->(s:Socket)
                OPTIONAL MATCH (s)-[:TRIPLE]->(v)
                RETURN count(DISTINCT kg) AS kg_cnt,
                       count(DISTINCT s) AS socket_cnt,
                       count(DISTINCT v) AS value_cnt
                """,
                uuid=uuid,
            ).single() or {"kg_cnt": 0, "socket_cnt": 0, "value_cnt": 0}
        click.echo(
            f"About to delete KG {uuid} (kg={counts['kg_cnt']}, sockets={counts['socket_cnt']}, values={counts['value_cnt']})"
        )
        click.confirm(
            f"Delete knowledge graph {uuid} and its related nodes from Neo4j?", abort=True
        )
    driver = _get_neo4j_driver()
    with driver.session() as session:
        result = session.run(
            """
            MATCH (kg:KnowledgeGraph {uuid: $uuid})
            OPTIONAL MATCH (kg)-[:HAS_SOCKET]->(s:Socket)
            OPTIONAL MATCH (s)-[:TRIPLE]->(v)
            WITH collect(DISTINCT kg) + collect(DISTINCT s) + collect(DISTINCT v) AS nodes
            FOREACH (n IN nodes | DETACH DELETE n)
            RETURN size(nodes) AS removed
            """,
            uuid=uuid,
        ).single()
    removed = result["removed"] if result and "removed" in result else 0
    if removed:
        click.echo(f"Deleted knowledge graph {uuid}")
    else:
        click.echo(f"No knowledge graph found with UUID {uuid}", err=True)


def main(argv: list[str] | None = None) -> int:
    """Debug runner; primary use is via the node-graph CLI plugin."""

    try:
        knowledge_graph.main(args=argv, prog_name="node-graph knowledge-graph", standalone_mode=False)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
