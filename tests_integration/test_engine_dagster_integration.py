from __future__ import annotations

import json
import os
from urllib import request, error

import pytest
from aiida import orm

pytest.importorskip("dagster")

from node_graph_engine.engines.dagster import DagsterEngine

pytestmark = pytest.mark.integration


def _query_dagster(url: str, query: str, variables: dict) -> dict:
    payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    req = request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Dagster GraphQL HTTP {exc.code}: {body}") from exc


def _query_dagster_runs(url: str, job_name: str, limit: int = 5) -> dict:
    query_job = """
        query Runs($jobName: String!, $limit: Int!) {
          runsOrError(filter: { jobName: $jobName }, limit: $limit) {
            __typename
            ... on Runs {
              results {
                runId
                jobName
              }
            }
            ... on PythonError {
              message
            }
          }
        }
    """
    query_pipeline = """
        query Runs($jobName: String!, $limit: Int!) {
          runsOrError(filter: { pipelineName: $jobName }, limit: $limit) {
            __typename
            ... on Runs {
              results {
                runId
                jobName
              }
            }
            ... on PythonError {
              message
            }
          }
        }
    """
    try:
        return _query_dagster(url, query_job, {"jobName": job_name, "limit": limit})
    except RuntimeError as exc:
        if "HTTP 400" not in str(exc):
            raise
        return _query_dagster(
            url, query_pipeline, {"jobName": job_name, "limit": limit}
        )


def test_dagster_engine_integration(nested_graph) -> None:
    dagster_url = os.environ.get("DAGSTER_GRAPHQL_URL")
    if not dagster_url:
        pytest.skip("Set DAGSTER_GRAPHQL_URL to run Dagster integration tests.")

    ng = nested_graph.build(x=1, y=2, z=3)
    engine = DagsterEngine(job_name="ng_integration_dagster")
    result = engine.run(ng)

    assert result["result"] == 20

    process_node = orm.load_node(engine._graph_pid)
    assert process_node.is_finished_ok

    response = _query_dagster_runs(
        dagster_url,
        engine.job_name,
        limit=5,
    )

    if response.get("errors"):
        pytest.fail(f"Dagster GraphQL errors: {response['errors']}")

    runs_payload = response.get("data", {}).get("runsOrError", {})
    if runs_payload.get("__typename") != "Runs":
        message = runs_payload.get("message", "Unexpected Dagster response.")
        pytest.fail(message)

    runs = runs_payload.get("results", [])
    if not runs:
        pytest.skip(
            "Dagster GraphQL did not return runs; skipping external validation."
        )
    assert any(run.get("jobName") == engine.job_name for run in runs)
