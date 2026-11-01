# cdc-es-sync: Debezium change events (Kafka) -> Elasticsearch documents, values read from Oracle.
#
#   docker build -t era/cdc-es-sync:1.0.0 .
#
# config.yaml and sql/ are copied in as defaults; docker-compose.yml mounts the ones next to it over them,
# so a mapping change needs a container restart, not a rebuild.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first (cached unless requirements.lock changes). confluent-kafka and oracledb (thin mode) ship
# as wheels: no compiler and no Oracle client are needed.
COPY requirements.lock ./
RUN pip install -r requirements.lock

COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-deps .

COPY config.yaml ./config.yaml
COPY sql ./sql

# Not root. logs/ and dlq/ are volumes; created here so a fresh named volume inherits the owner.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app \
 && mkdir -p /app/logs /app/dlq \
 && chown -R app:app /app/logs /app/dlq
USER app

EXPOSE 9108

# /health answers 200 while the consume loop is alive, 503 when it is stuck or failed.
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
  CMD python -c "import os,urllib.request; urllib.request.urlopen('http://localhost:%s/health' % os.environ.get('CDC_HTTP_PORT','9108'), timeout=4)"

# `docker run ... check` and `... resync customer_data` work too.
ENTRYPOINT ["python", "-m", "cdc_es_sync"]
CMD ["run"]
