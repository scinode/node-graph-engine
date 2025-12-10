Welcome to node-graph Engine's documentation!
===========================================

`node-graph <https://node-graph.readthedocs.io/>`_ provides the decorators and
APIs for building node-based workflows with rich provenance. Graph Engine
executes those node-graph workflows across multiple orchestration backends
while preserving the recorded provenance. This allows the same workflow to run
on different engines yet yield consistent records for interoperability and
long-term reproducibility of scientific results.


node-graph Engine ships adaptors for multiple backends. Browse the dedicated
:doc:`engines/index` guide for full details.

* :ref:`Dask <engines-dask>`
* :ref:`Airflow <engines-airflow>`
* :ref:`Prefect <engines-prefect>`
* :ref:`Celery <engines-celery>`
* :ref:`Dagster <engines-dagster>`
* :ref:`Parsl <engines-parsl>`
* :ref:`Redun <engines-redun>`
* :ref:`Jobflow <engines-jobflow>`
* :ref:`Executorlib (Pyiron) <engines-executorlib>`


.. note::

   The `aiida-workgraph <https://aiida-workgraph.readthedocs.io/en/latest/>`_ project
   provides an additional adaptor for the AiiDA platform, maintained separately.


Outline
-------

.. toctree::
   :maxdepth: 1

   installation
   autogen/quick_start
   engines/index
   autogen/remote_node
   autogen/ontology_semantics
   tutorial/knowledge_graphs
   eos_workflow
