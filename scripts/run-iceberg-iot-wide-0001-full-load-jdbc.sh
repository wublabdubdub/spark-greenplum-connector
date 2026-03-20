#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_PATH="${REPO_ROOT}/scripts/migrate-iceberg-iot-wide-0001-full-load-jdbc.scala"
POSTGRES_DRIVER_JAR="${POSTGRES_DRIVER_JAR:-${REPO_ROOT}/spark-greenplum-connector/target/spark-ymatrix-connector_2.12-3.1.jar}"

LOCAL_IP="${SPARK_LOCAL_IP:-$(hostname -I | awk '{print $1}')}"
export SPARK_LOCAL_IP="${LOCAL_IP}"

SPARK_EXTRA_ARGS=()
if [[ -f "${POSTGRES_DRIVER_JAR}" ]]; then
  echo "Using JDBC driver jar: ${POSTGRES_DRIVER_JAR}"
  SPARK_EXTRA_ARGS+=(--jars "${POSTGRES_DRIVER_JAR}")
else
  echo "JDBC driver jar not found at ${POSTGRES_DRIVER_JAR}, falling back to --packages org.postgresql:postgresql:42.7.2"
  SPARK_EXTRA_ARGS+=(--packages "org.postgresql:postgresql:42.7.2")
fi

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
  --conf "spark.sql.catalog.gp=org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog" \
  --conf "spark.sql.catalog.gp.url=jdbc:postgresql://172.16.100.29:5432/zhangchen" \
  --conf "spark.sql.catalog.gp.user=zhangchen" \
  --conf "spark.sql.catalog.gp.password=YMatrix@123" \
  --conf "spark.sql.catalog.gp.driver=org.postgresql.Driver" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  "${SPARK_EXTRA_ARGS[@]}" \
  -i "${SCRIPT_PATH}"
