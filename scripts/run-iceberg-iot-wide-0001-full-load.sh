#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONNECTOR_JAR="${REPO_ROOT}/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar"
SCRIPT_PATH="${REPO_ROOT}/scripts/migrate-iceberg-iot-wide-0001-full-load.scala"

if [[ ! -f "${CONNECTOR_JAR}" ]]; then
  echo "Connector jar not found: ${CONNECTOR_JAR}" >&2
  exit 1
fi

LOCAL_IP="${SPARK_LOCAL_IP:-$(hostname -I | awk '{print $1}')}"
export SPARK_LOCAL_IP="${LOCAL_IP}"

exec "${SPARK_HOME:-/opt/spark}/bin/spark-shell" \
  --master local[8] \
  --driver-memory 16g \
  --conf "spark.driver.host=${LOCAL_IP}" \
  --conf "spark.driver.bindAddress=0.0.0.0" \
  --conf "spark.local.ip=${LOCAL_IP}" \
  --conf "spark.sql.shuffle.partitions=8" \
  --conf "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.local.type=hadoop" \
  --conf "spark.sql.catalog.local.warehouse=/data/iceberg/warehouse" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --jars "${CONNECTOR_JAR}" \
  -i "${SCRIPT_PATH}"
