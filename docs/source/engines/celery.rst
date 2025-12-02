.. _engines-celery:

Celery engine
=============

The Celery engine dispatches every task in a graph as an individual Celery task while
preserving provenance in the active AiiDA profile. Use it when you already operate a
Celery deployment and want the flexibility of scaling workers independently from the
workflow orchestrator.

Installation
------------

Install the engine extra together with an AiiDA profile. The extra pulls in Celery,
kombu, and the ``redis`` client so the adaptor can register and submit tasks using the
Redis broker referenced throughout this guide:

.. code-block:: console

   pip install node_graph_engine[celery]

If you plan to use a different transport (for example RabbitMQ), install the matching
client library for that broker instead of Redis.

Install and start a Redis server so the broker and result backend URLs below resolve to a
running service. The exact command depends on your platform (for example
``sudo apt install redis-server`` on Debian-based Linux or ``brew install redis`` on
macOS) and you may need to enable the service after installation.

Create (or reuse) an AiiDA profile for provenance storage before running any graphs. The
:command:`verdi presto` helper remains the fastest way to bootstrap a local profile.

Configuration
-------------

By default the engine spins up a lightweight Celery application that uses the
``memory://`` broker and ``rpc://`` result backend. This configuration keeps execution
local to the current process and forces Celery into eager mode so graphs complete without
an external worker. Eager execution is useful for development and unit tests, but Celery
will only execute one task at a time inside the current Python interpreter.

Provide a broker URL, result backend, or a pre-configured :class:`~celery.Celery`
instance to connect to an existing deployment:

.. code-block:: python

   from node_graph_engine.engines.celery import CeleryEngine

   engine = CeleryEngine(
       "materials-queue",
       broker_url="redis://localhost:6379/0",
       backend_url="redis://localhost:6379/1",
   )

.. note::

    When using RabbitMQ as the broker, a typical URL looks like this:

    .. code-block:: python

        broker_url="amqp://guest:guest@127.0.0.1:5672"

When the broker is anything other than ``memory://`` the engine leaves eager mode
disabled so tasks are published to the external queue. You can still override the
behaviour explicitly through the ``always_eager`` keyword argument if you want to force a
particular execution model.

Running workers
---------------

Parallel execution requires at least one Celery worker that subscribes to the same broker
URL as the engine. The snippet below creates a small ``celery_worker.py`` module that
instantiates the engine, exposes the underlying application, and can be launched with the
standard :command:`celery` CLI:

.. code-block:: python

   # celery_worker.py
   from node_graph_engine.engines.celery import CeleryEngine

   engine = CeleryEngine(
       "celery-flow",
       broker_url="redis://localhost:6379/0",
       backend_url="redis://localhost:6379/1",
   )

   celery_app = engine.celery_app

Start a worker from a shell in the same environment:

.. code-block:: console

   celery -A celery_worker.celery_app worker --loglevel=INFO

With the worker running, launch your workflow from another terminal. Each independent
task will be dispatched to the external queue and processed by the worker, allowing them
to execute concurrently.

Example
-------

The example below mirrors the quick-start workflow. Leave ``always_eager`` at its default
value so work is routed to the broker configured above:

.. code-block:: python

   from aiida import load_profile
   from node_graph import task
   from node_graph_engine.engines.celery import CeleryEngine

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

   engine = CeleryEngine(
       "celery-flow",
       broker_url="redis://localhost:6379/0",
       backend_url="redis://localhost:6379/1",
   )
   outputs = engine.run(graph)
   print(outputs)

The engine records all task executions in AiiDA and will reuse Celery workers that are
already subscribed to the configured broker. Use Celery's standard monitoring tools (such
as :command:`celery -A myapp inspect active` and flower) to observe queued and running
task jobs.
