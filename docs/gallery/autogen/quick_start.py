"""
===========
Quick Start
===========

"""

# %%
# Installation
# =================
#
# Let's first install the engine with parsl support alongside AiiDA core,
# which provides the database layer for provenance storage.
#
# .. code:: console
#
#    $ pip install node-graph-engine[parsl] aiida-core
#
# After installing the packages, initialise an AiiDA profile with ``verdi presto``. This interactive setup only needs
# to be run once per environment.
#
# .. code:: console
#
#    $ verdi presto

# %%
# Creating a simple graph
# =======================
# Suppose we want to calculate ``(x + y) * z`` in two steps.
#
# - step 1: add `x` and `y`
# - step 2: then multiply the result with `z`.
#

from node_graph.decorator import task
from aiida import load_profile

load_profile()


# Define the tasks
@task()
def add(x, y):
    return x + y


@task()
def multiply(x, y):
    return x * y


# Define the graph
@task.graph()
def AddMultiply(x, y, z):
    the_sum = add(x=x, y=y).result
    return multiply(x=the_sum, y=z).result


ng = AddMultiply.build(x=1, y=2, z=3)
# Visualise the graph
ng.to_html()


# %%
# Engines and provenance
# ============================
# Run graphs using ParslEngine
from node_graph_engine.engines.parsl import ParslEngine

graph = AddMultiply.build(x=1, y=2, z=3)

engine = ParslEngine()
results = engine.run(graph)
print("results:", results)

# %%
# After the workflow completes you can inspect its provenance using the AiiDA
# CLI. The ``-a`` option ensures finished processes remain visible.
#
# .. code:: console
#
#    $ verdi process list -a
#
#     216211  12s ago    NodeGraph<AddMultiply>                                               ⏹ Finished [0]
#     216212  12s ago    add                                                                  ⏹ Finished [0]
#     216214  12s ago    multiply                                                             ⏹ Finished [0]

# %%
# In interactive notebooks you can display the provenance graph inline

engine.provenance_graph
