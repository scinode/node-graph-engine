"""
================
Remote execution
================

Launch heavy computations on remote machines while the orchestration engine
continues scheduling other work. Remote tasks rely on AiiDA scheduler and transport
plugins to upload inputs, submit the job to a scheduler, and
retrieve the outputs back into the Graph provenance layer.
"""

# %%
# Why use remote tasks?
# ---------------------
# - Offload expensive tasks to HPC clusters or specialised appliances.
# - Reuse AiiDA's scheduler plugins (SLURM, PBS, …) for staging files and
#   monitoring runs.
# - Keep orchestration workers responsive: Airflow defers the task,
#   Prefect awaits the asynchronous coroutine, and the Local engine blocks
#   only callers that depend on the remote result.
#
# Every engine ultimately delegates to the shared coroutine, so behaviour is consistent across orchestrators.

# %%
# Configuration checklist
# -----------------------
# 1. Create AiiDA computer node that has access to the target computer,
#    Verify passwordless transport or configure SSH keys for the user that
#    will run the workflow.
# 2. Register a ``Python`` code on that computer pointing to the interpreter
#    you want to use (often the system ``python3`` or a virtual environment).
#
# .. code:: console
#
#    $ verdi computer setup
#    $ verdi code create core.code.installed --label python3 --computer localhost --filepath-executable /usr/bin/python3
#    $ verdi code show python3@localhost
#
# .. important::
#    Ensure the remote environment provides the same Python version (and
#    required packages) as the workstation driving the workflow. Use modules
#    or virtual environments inside ``metadata['options']['custom_scheduler_commands']``
#    to activate the correct interpreter.

# %%
# Passing scheduler metadata
# --------------------------
# Remote tasks accept ``metadata``, ``code`` and ``computer`` inputs in addition to the
# function's regular parameters. Populate these controls to select another
# queue, request GPUs, or activate a conda environment before the script runs.
#
# .. code:: python
#
#    metadata = {
#        "options": {
#            # Example: load a module and activate a virtual environment.
#            "custom_scheduler_commands": "module load python/3.11\nsource ~/venvs/ng/bin/activate\n",
#            # Other options such as 'max_wallclock_seconds' are forwarded to AiiDA.
#        }
#    }
#    computer = "localhost"  # or an orm.Computer instance retrieved from the profile
#
# These keys are forwarded verbatim to the calculation.

# %%
# Defining remote tasks
# ---------------------
# Remote tasks behave like regular tasks with the ``@task.remote`` decorator.
# The snippet below declares two remote functions and combines them into a
# sub-graph that can run locally, using Local, Prefect, or Airflow engine.

from typing import Annotated, Any
from aiida import load_profile, orm
from node_graph import namespace, task
from node_graph_engine.engines.local import LocalEngine

load_profile()


@task.remote()
def add(x: int, y: int) -> int:
    return x + y


@task.remote()
def multiply(x: int, y: int) -> int:
    return x * y


@task.graph()
def add_then_multiply(
    x: int,
    y: int,
    code,
) -> Annotated[dict, namespace(sum=Any, result=Any)]:
    partial = add(x=x, y=y, code=code).result
    product = multiply(x=partial, y=y, code=code).result
    return {"sum": partial, "result": product}


engine = LocalEngine(name="remote-demo")
code = orm.load_code("python3@localhost")  # Adjust to your code label
graph = add_then_multiply.build(
    x=1,
    y=2,
    code=code,
)
results = engine.run(graph)
print(results["result"])


# %%
# In interactive notebooks you can display the provenance graph inline

engine.provenance_graph

# %%
# One can see that each remote calculation has additional output sockets:
#
# - `remote_folder`: the remote working directory containing input/output files
# - `retrieved`: the retrieved files after job completion

# %%
# Supported engines
# -----------------
# - **Airflow** wraps remote calls inside :class:`AsyncNodeExecutionRequest`,
#   defers the operator, and resumes once the trigger receives a completion
#   event. No worker slots remain occupied while the scheduler watches the
#   remote job.
# - **Prefect** treats remote tasks as asynchronous tasks, awaiting the
#   completion of ``_execute_pythonjob_remote`` without blocking other
#   flows.
# - **LocalEngine** executes remote tasks synchronously, keeping the behaviour
#   straightforward for development and debugging.
#

# %%
# Monitoring remote executions
# ----------------------------
# AiiDA records every remote job as a ``CalcJobNode`` linked to the workflow.
# Use the CLI to inspect state, download files, or resubmit if needed.
#
# .. code:: console
#
#    $ verdi process list -a
#    $ verdi node show <PK>
#
# For deeper debugging, inspect the ``remote_folder`` output attached to the
# remote task: it contains input scripts, logs, and retrieved artefacts exactly
# as produced on the cluster.
