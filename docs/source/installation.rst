Installation
============

Install the core package (which includes the provenance recorder and shared
utilities) directly from PyPI:

.. code-block:: console

   pip install --upgrade node_graph_engine

.. important::

   node-graph engine currently executes calculations locally only. Each engine interacts
   with the active AiiDA profile during runtime, so workflows must run on the same
   machine that hosts the AiiDA database.

node-graph engine ships adaptors for multiple workflow backends as optional
extras. Install only the engines you plan to use:

.. code-block:: console

   # Install with Prefect support
   pip install node_graph_engine[prefect]

   # Install with Celery support
   pip install node_graph_engine[celery]

   The Celery extra bundles the ``redis`` Python client so you can follow the worker
   setup in :ref:`engines-celery` without installing additional broker drivers. Swap the
   dependency to match the broker you intend to use (for example RabbitMQ).

   # Install with Temporal support
   pip install node_graph_engine[temporal]

   # Install with Dask support
   pip install node_graph_engine[dask]

   # Install with Parsl support
   pip install node_graph_engine[parsl]

   # Install with every engine adaptor
   pip install node_graph_engine[engines]

You can also combine extras to tailor the installation, for example
``pip install node_graph_engine[prefect,parsl]``.

In addition to the engine adaptors, install the
`AiiDA core <https://aiida.readthedocs.io/projects/aiida-core/en/latest/>`_
package to provide a database backend for provenance storage:

.. code-block:: console

   pip install aiida-core

Once the packages are available, create a local AiiDA profile (and the
associated PostgreSQL database) using :command:`verdi presto`. This command
guides you through setting up all required services and can be rerun whenever
you need to recreate the profile:

.. code-block:: console

   verdi presto

Follow the prompts to initialise the database and complete the profile
configuration. The node-graph engine examples assume that this profile exists
so that provenance can be recorded.
