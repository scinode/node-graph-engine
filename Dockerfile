FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    AIIDA_PATH=/var/lib/aiida

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/node-graph-engine

COPY . /opt/node-graph-engine

RUN python -m pip install --upgrade pip && \
    pip install -e .

RUN chmod +x /opt/node-graph-engine/docker/entrypoint.sh

ENTRYPOINT ["/opt/node-graph-engine/docker/entrypoint.sh"]
CMD ["bash", "-lc", "sleep infinity"]
