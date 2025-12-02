.. _engines-jobflow:

Jobflow engine
==============

The Jobflow adaptor maps tasks to Jobflow jobs, letting you integrate with
materials-science pipelines and store rich provenance alongside Jobflow execution
metadata.

Installation
------------

1. Install the Jobflow extra:

   .. code-block:: console

      pip install node_graph_engine[jobflow]

2. (Optional) Configure a Jobflow ``JobStore`` (for example a ``MongoStore``) if you
   want persistent storage beyond the default in-memory execution used by
   :func:`jobflow.run_locally`.

3. Load an AiiDA profile prior to launching the engine.

Example
-------

.. code-block:: python

   from aiida import load_profile
   from node_graph import task
   from node_graph_engine.engines.jobflow import JobflowEngine

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

   engine = JobflowEngine(name="jobflow-quick-start")
   outputs = engine.run(graph)
   print(outputs)

Jobflow executes the jobs locally by default.


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

.. image:: ../_static/images/jobflow_add_multiply_provenance.png
   :alt: Provenance graph for the add_then_multiply workflow
