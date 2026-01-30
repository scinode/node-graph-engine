Integration tests
=================

Integration tests live under ``tests_integration/`` and exercise real
orchestration services (Airflow, Dagster, Prefect, Temporal). These tests do
**not** use the AiiDA pytest fixtures because those fixtures create temporary
profiles that are not visible to external worker processes (for example, Airflow
workers). Instead, integration tests assume an existing AiiDA profile is
configured in the environment.

Why the AiiDA pytest fixtures are avoided
----------------------------------------

The ``aiida.tools.pytest_fixtures`` plugin creates and loads a temporary profile
inside the pytest process. External services run in separate processes and cannot
see that in-memory test profile, which leads to failures like:

- ``ConfigurationError: Could not determine the default AiiDA user email``
- ``ProfileConfigurationError: profile <name> does not exist``

To keep integration runs stable across workers, use a real profile instead.

Recommended setup
-----------------

1. Create/configure a profile (one-time):

   .. code-block:: console

      verdi presto

2. Ensure the profile has a default user:

   .. code-block:: console

      verdi user list

3. Export the profile before starting services and tests:

   .. code-block:: console

      export AIIDA_PROFILE=presto
      export NG_INTEGRATION=1
      pytest tests_integration/test_engine_airflow_integration.py -s -vv

Notes for Airflow
-----------------

- Start the Airflow triggerer when running deferrable tasks (sub-graph
  scheduling uses deferral):

  .. code-block:: console

     airflow triggerer

- Ensure the Airflow services inherit ``AIIDA_PROFILE`` so their workers load the
  same profile as the test runner.



Test locally
----------------


.. code-block:: console

   docker run --rm -it \
    -p 8080:8080 \
    -v "$PWD:/work" \
    -w /work \
    ubuntu:22.04 bash

   apt-get update
   apt-get install -y --no-install-recommends \
     ca-certificates curl git tini \
     python3 python3-venv python3-pip \
     build-essential gcc g++ \
     libffi-dev libssl-dev \
     sqlite3
   rm -rf /var/lib/apt/lists/*

   python3 -m venv .venv
   . .venv/bin/activate
   python -m pip install -U pip wheel setuptools

   pip install -e . --no-deps
   pip install "aiida-core~=2.7" "aiida-pythonjob~=0.5.1" "pytest~=7.0" "pytest-cov~=2.7" "pytest-timeout~=2.3"
   pip install "apache-airflow~=3.1"

   verdi presto

   export AIRFLOW_HOME="/tmp/airflow"
   export AIRFLOW__CORE__DAGS_FOLDER="$AIRFLOW_HOME/dags"
   export AIRFLOW__CORE__LOAD_EXAMPLES="False"
   export AIRFLOW__CORE__EXECUTOR="SequentialExecutor"
   export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL="5"
   export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL="5"

   mkdir -p "$AIRFLOW_HOME/dags"

   airflow db migrate

   nohup airflow api-server --port 8080 >"$AIRFLOW_HOME/api-server.log" 2>&1 &
   nohup airflow scheduler >"$AIRFLOW_HOME/scheduler.log" 2>&1 &
   nohup airflow dag-processor >"$AIRFLOW_HOME/dag-processor.log" 2>&1 &
   nohup airflow triggerer >"$AIRFLOW_HOME/triggerer.log" 2>&1 &

   export NG_INTEGRATION=1
   export NG_AIRFLOW_INTEGRATION=1
   export NG_AIRFLOW_RESULT_TIMEOUT=150
   export AIRFLOW_HOME="/tmp/airflow"
   export AIRFLOW__CORE__DAGS_FOLDER="$AIRFLOW_HOME/dags"

   pytest tests_integration/test_engine_airflow_integration.py -s -vv --maxfail=1 --timeout=150
