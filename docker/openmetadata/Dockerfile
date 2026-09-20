# A one-shot OpenMetadata ingestion runner carrying the Polaris Iceberg connector.
#
# Deliberately not FROM docker.getcollate.io/openmetadata/ingestion: that image is
# around 4 GB because it bundles Airflow and every connector Collate ships. This
# PoC drives ingestion straight from the `metadata` CLI, so none of that is used.
# Built from the repository root -- it needs openmetadata-connector/ in context.
FROM python:3.10-slim

# curl and jq are for ingest.sh: waiting on the server and pulling the bot token.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl jq ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Pinned to the server version in compose.yaml. The ingestion framework and the
# server share the entity schemas, so a mismatch shows up as validation errors.
RUN pip install --no-cache-dir openmetadata-ingestion==2.0.2.0

COPY openmetadata-connector /src/openmetadata-connector
RUN pip install --no-cache-dir /src/openmetadata-connector

COPY docker/openmetadata/ingest.sh /usr/local/bin/ingest.sh
COPY docker/openmetadata/polaris-ingestion.yaml /workflow/polaris-ingestion.yaml
RUN chmod +x /usr/local/bin/ingest.sh

ENTRYPOINT ["/usr/local/bin/ingest.sh"]
