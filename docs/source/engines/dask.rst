.. _engines-dask:

Dask engine
===========

The Dask adaptor schedules task jobs on the standard `dask` threaded scheduler
and collects their provenance on the active AiiDA profile. It offers light-
weight parallel execution on a single machine without requiring
``dask.distributed``.

Installation
------------

1. Install the Dask extra:

   .. code-block:: console

      pip install node_graph_engine[dask]

2. Optionally adjust the global Dask configuration (for example to limit the
   number of worker threads) before running your workflow. The engine defaults
   to Dask's ``threads`` scheduler, so a simple configuration looks like:

   .. code-block:: python

      import dask

      dask.config.set(num_workers=2)

Example
-------

.. code-block:: python

   from aiida import load_profile
   from node_graph import task
   from node_graph_engine.engines.dask import DaskEngine

   load_profile()

   @task()
   def add(x, y):
       return x + y

   @task()
   def multiply(x, y):
       return x * y

   @task.graph()
   def add_then_multiply(x, y, z):
       the_sum = add(x=x, y=y).result
       return multiply(x=the_sum, y=z).result

    graph = add_then_multiply.build(x=1, y=2, z=3)
    engine = DaskEngine(name="dask-quick-start")
    outputs = engine.run(graph)
    print(outputs)


The engine manages provenance on the active AiiDA profile so that each
scheduled task creates process nodes with the correct user.

Use AiiDA commands to inspect the processes and their provenance:

.. code-block:: console

   verdi process list -a


Which will show something like:

.. code-block:: console

    216242  6s ago     NodeGraph<add_multiply>                ⏹ Finished [0]
    216243  6s ago     add                                    ⏹ Finished [0]
    216245  6s ago     multiply                               ⏹ Finished [0]

Then generate a provenance graph for a workflow:


.. code-block:: console

   verdi node graph generate 216242 -f png

Here is the resulting graph:

.. image:: ../_static/images/parsl_add_multiply_provenance.png
   :alt: Provenance graph for the add_then_multiply workflow


Configuration
-------------

The engine accepts optional keyword arguments to customise how Dask executes
work:

``scheduler``
    Override the scheduler passed to :func:`dask.compute`. The default is the
    string ``"threads"`` which activates the thread pool scheduler.

``compute_kwargs``
    Additional keyword arguments forwarded to :func:`dask.compute`. Use this to
    tweak parameters such as ``num_workers`` on a per-engine basis without
    touching global configuration.

Both options are propagated to nested graphs so that sub-workflows run with the
same Dask settings as their parent graph.
