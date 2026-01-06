"""node-graph-engine CLI extensions (knowledge graph commands) using Click."""

from __future__ import annotations

import json
import sys
from typing import Sequence

import click

from node_graph.knowledge.graph import KnowledgeGraph

from node_graph_engine.neo4j.knowledge_graph import (
    fetch_all_knowledge_graphs,
    fetch_knowledge_graph,
    _get_neo4j_driver,
)

def _truncate(value: object, max_len: int = 80) -> str:
    text = str(value)
    if max_len <= 0 or len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return text[: max_len - 3] + "..."


def _flatten_metadata(payload: dict) -> list[tuple[str, object]]:
    rows: list[tuple[str, object]] = []
    for key, value in payload.items():
        if key == "semantics":
            continue
        if key == "workflow" and isinstance(value, dict):
            for wf_key in (
                "name",
                "identifier",
                "module",
                "qualname",
                "callable_path",
                "file_path",
                "package",
                "package_version",
            ):
                if value.get(wf_key) is not None:
                    rows.append((f"workflow.{wf_key}", value.get(wf_key)))
            continue
        rows.append((key, value))
    return rows


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
        entities = g.get("socket_count")
        triples = g.get("triple_count")
        wf = g.get("workflow") or {}
        rows.append(
            [
                g.get("uuid") or "",
                wf.get("name") or wf.get("identifier") or "",
                wf.get("identifier") or "",
                entities if entities is not None else "",
                triples if triples is not None else "",
                (g.get("hash") or "")[:12],
            ]
        )
    if not rows:
        click.echo("No knowledge graphs found.")
        return
    click.echo(_format_table(headers, rows))


@knowledge_graph.command("show")
@click.argument("uuid")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    help="Emit raw JSON (disables pretty formatting).",
)
@click.option(
    "--no-metadata",
    "exclude_metadata",
    is_flag=True,
    help="Only return semantics (namespaces/sockets/triples).",
)
@click.option("--namespaces", "show_namespaces", is_flag=True, help="Include namespaces in pretty output.")
@click.option("--max-sockets", type=int, default=20, show_default=True, help="Max sockets to print in pretty output.")
@click.option("--max-triples", type=int, default=20, show_default=True, help="Max triples to print in pretty output.")
def show_kg(
    uuid: str,
    as_json: bool,
    exclude_metadata: bool,
    show_namespaces: bool,
    max_sockets: int,
    max_triples: int,
) -> None:
    """Show a knowledge graph payload by UUID."""

    if fetch_knowledge_graph is None:
        click.echo("Knowledge graph fetch unavailable (missing engine dependency)", err=True)
        raise SystemExit(1)
    try:
        payload = fetch_knowledge_graph(uuid, include_metadata=not exclude_metadata)
    except Exception as exc:  # pragma: no cover - runtime env dependent
        click.echo(f"Failed to fetch knowledge graph: {exc}", err=True)
        raise SystemExit(1)
    if as_json:
        click.echo(json.dumps(payload, indent=2))
        return

    wrapper = payload if isinstance(payload, dict) else {}
    semantics = wrapper.get("semantics") if isinstance(wrapper.get("semantics"), dict) else wrapper
    sockets = semantics.get("sockets") if isinstance(semantics, dict) else {}
    triples = semantics.get("triples") if isinstance(semantics, dict) else []
    namespaces = semantics.get("namespaces") if isinstance(semantics, dict) else {}

    click.echo(f"KnowledgeGraph UUID: {uuid}")
    click.echo(
        f"Sockets: {len(sockets or {})} | Triples: {len(triples or [])} | Namespaces: {len(namespaces or {})}"
    )

    if not exclude_metadata and isinstance(wrapper, dict) and "semantics" in wrapper:
        meta_rows = [(k, v) for k, v in sorted(_flatten_metadata(wrapper)) if v not in (None, {}, [])]
        if meta_rows:
            click.echo("")
            click.echo("Metadata")
            click.echo(_format_table(["Key", "Value"], [[k, _truncate(v, 120)] for k, v in meta_rows]))

    if show_namespaces and isinstance(namespaces, dict) and namespaces:
        click.echo("")
        click.echo("Namespaces")
        ns_rows = [[k, _truncate(v, 120)] for k, v in sorted(namespaces.items())]
        click.echo(_format_table(["Prefix", "IRI"], ns_rows))

    if isinstance(sockets, dict) and sockets:
        click.echo("")
        click.echo("Sockets")
        socket_items = list(sorted(sockets.items()))
        shown = socket_items[: max(0, max_sockets)]
        socket_rows = []
        for sid, meta in shown:
            meta = meta or {}
            label = meta.get("label") or meta.get("canonical") or sid
            socket_rows.append(
                [
                    sid,
                    _truncate(label, 50),
                    meta.get("task") or "",
                    meta.get("direction") or "",
                    meta.get("port") or "",
                    _truncate(meta.get("canonical") or sid, 50),
                ]
            )
        click.echo(_format_table(["ID", "Label", "Task", "Dir", "Port", "Canonical"], socket_rows))
        remaining = len(socket_items) - len(shown)
        if remaining > 0:
            click.echo(f"... ({remaining} more sockets; use --max-sockets to increase)")

    if isinstance(triples, list) and triples:
        click.echo("")
        click.echo("Triples")
        shown = triples[: max(0, max_triples)]
        triple_rows = []
        for triple in shown:
            if isinstance(triple, (list, tuple)) and len(triple) == 3:
                s, p, o = triple
                triple_rows.append([_truncate(s, 60), _truncate(p, 50), _truncate(o, 80)])
            else:
                triple_rows.append(["", "", _truncate(triple, 160)])
        click.echo(_format_table(["Subject", "Predicate", "Object"], triple_rows))
        remaining = len(triples) - len(shown)
        if remaining > 0:
            click.echo(f"... ({remaining} more triples; use --max-triples to increase)")


@knowledge_graph.command("delete")
@click.argument("uuids", nargs=-1)
@click.option("-a", "--all", "delete_all", is_flag=True, help="Delete all knowledge graphs.")
@click.option("--force", is_flag=True, help="Do not prompt for confirmation.")
def delete_kg(uuids: tuple[str, ...], delete_all: bool, force: bool) -> None:
    """Delete one or more knowledge graphs (and their sockets/values)."""

    if delete_all and uuids:
        click.echo("Provide either one or more UUIDs or -a/--all, not both.", err=True)
        raise SystemExit(1)
    if not delete_all and not uuids:
        click.echo("Provide at least one UUID or use -a/--all to delete everything.", err=True)
        raise SystemExit(1)

    driver = _get_neo4j_driver()
    with driver.session() as session:
        target_uuids: list[str]
        if delete_all:
            record = session.run(
                "MATCH (kg:KnowledgeGraph) RETURN collect(kg.uuid) AS uuids"
            ).single() or {}
            target_uuids = list(record.get("uuids") or [])
        else:
            target_uuids = list(uuids)

        if not target_uuids:
            click.echo("No knowledge graphs found to delete.")
            return

        def _fetch_counts(graph_uuid: str) -> dict[str, int]:
            record = session.run(
                """
                MATCH (kg:KnowledgeGraph {uuid: $uuid})
                OPTIONAL MATCH (kg)-[:HAS_SOCKET]->(s:Socket)
                OPTIONAL MATCH (s)-[:TRIPLE]->(v)
                RETURN count(DISTINCT kg) AS kg_cnt,
                       count(DISTINCT s) AS socket_cnt,
                       count(DISTINCT v) AS value_cnt
                """,
                uuid=graph_uuid,
            ).single() or {}
            return {
                "kg_cnt": int(record.get("kg_cnt", 0)),
                "socket_cnt": int(record.get("socket_cnt", 0)),
                "value_cnt": int(record.get("value_cnt", 0)),
            }

        if not force:
            click.echo("About to delete the following knowledge graphs:")
            for graph_uuid in target_uuids:
                counts = _fetch_counts(graph_uuid)
                click.echo(
                    f"  {graph_uuid} (kg={counts['kg_cnt']}, sockets={counts['socket_cnt']}, values={counts['value_cnt']})"
                )
            click.confirm("Proceed with deletion?", abort=True)

        deleted: list[str] = []
        missing: list[str] = []
        for graph_uuid in target_uuids:
            result = session.run(
                """
                MATCH (kg:KnowledgeGraph {uuid: $uuid})
                OPTIONAL MATCH (kg)-[:HAS_SOCKET]->(s:Socket)
                OPTIONAL MATCH (s)-[:TRIPLE]->(v)
                WITH collect(DISTINCT kg) + collect(DISTINCT s) + collect(DISTINCT v) AS nodes
                FOREACH (n IN nodes | DETACH DELETE n)
                RETURN size(nodes) AS removed
                """,
                uuid=graph_uuid,
            ).single()
            removed = result["removed"] if result and "removed" in result else 0
            if removed:
                deleted.append(graph_uuid)
            else:
                missing.append(graph_uuid)

    for graph_uuid in deleted:
        click.echo(f"Deleted knowledge graph {graph_uuid}")
    for graph_uuid in missing:
        click.echo(f"No knowledge graph found with UUID {graph_uuid}", err=True)


def main(argv: list[str] | None = None) -> int:
    """Debug runner; primary use is via the node-graph CLI plugin."""

    try:
        knowledge_graph.main(args=argv, prog_name="node-graph knowledge-graph", standalone_mode=False)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
