.. _engines-parsl:

Parsl engine
============

The Parsl adaptor dispatches tasks as Parsl ``python_app`` tasks, enabling
parallel and distributed execution while keeping provenance synchronised with AiiDA.

Installation
------------

1. Install the Parsl extra:

   .. code-block:: console

      pip install node_graph_engine[parsl]

2. Create or load a Parsl configuration that matches your resources. The example below
   uses the built-in thread-pool executor. Pass the configuration to
   :class:`~node_graph_engine.engines.parsl.ParslEngine` when creating the engine.

Example
-------

.. code-block:: python

   from aiida import load_profile
   from parsl.config import Config
   from parsl.executors.threads import ThreadPoolExecutor
   from node_graph import task
   from node_graph_engine.engines.parsl import ParslEngine

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

   config = Config(executors=[ThreadPoolExecutor(max_threads=2)], strategy=None)

   engine = ParslEngine(name="parsl-quick-start", config=config)
   outputs = engine.run(graph)
   print(outputs)



Use AiiDA commands to inspect the processes and their provenance:

.. code-block:: console

   verdi process list -a


Which will show something like:

.. code-block:: console

    2222  4s ago     NodeGraph<add_then_multiply>         ⏹ Finished [0]
    2223  4s ago     add                                  ⏹ Finished [0]
    2225  4s ago     multiply                             ⏹ Finished [0]

Then generate a provenance graph for a workflow:


.. code-block:: console

   verdi node graph generate 2222 -f png

Here is the resulting graph:

.. image:: ../_static/images/parsl_add_multiply_provenance.png
   :alt: Provenance graph for the add_then_multiply workflow
