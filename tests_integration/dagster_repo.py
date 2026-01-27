from __future__ import annotations

from dagster import Definitions, job, op


@op
def noop() -> str:
    return "ok"


@job
def integration_job():
    noop()


defs = Definitions(jobs=[integration_job])
