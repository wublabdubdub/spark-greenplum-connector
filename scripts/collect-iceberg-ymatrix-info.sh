#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/collect-iceberg-ymatrix-info.sh [options]

Options:
  --iceberg-table <catalog.namespace.table>
  --iceberg-catalog <catalog_name>
  --iceberg-type <hadoop|hive|rest>
  --iceberg-warehouse <path>
  --iceberg-uri <uri>
  --spark-master <master>
  --driver-memory <memory>
  --connector-jar <path>
  --ymatrix-url <jdbc_url>
  --ymatrix-user <user>
  --ymatrix-password <password>
  --ymatrix-schema <schema>
  --ymatrix-table <table>
  --extra-conf <key=value>
  --skip-count
  --output <report_file>
  --help

Environment variable equivalents are also supported:
  ICEBERG_TABLE
  ICEBERG_CATALOG
  ICEBERG_TYPE
  ICEBERG_WAREHOUSE
  ICEBERG_URI
  SPARK_MASTER
  SPARK_DRIVER_MEMORY
  CONNECTOR_JAR
  YMATRIX_URL
  YMATRIX_USER
  YMATRIX_PASSWORD
  YMATRIX_SCHEMA
  YMATRIX_TABLE

Examples:
  scripts/collect-iceberg-ymatrix-info.sh

  scripts/collect-iceberg-ymatrix-info.sh \
    --iceberg-table local.demo_db.orders \
    --iceberg-catalog local \
    --iceberg-type hadoop \
    --iceberg-warehouse /tmp/iceberg-warehouse \
    --ymatrix-url jdbc:postgresql://10.0.0.8:5432/demo \
    --ymatrix-user ymatrix_admin \
    --ymatrix-password 'secret' \
    --ymatrix-schema public \
    --ymatrix-table orders_from_iceberg
EOF
}

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

mask_secret() {
  local value="${1:-}"
  if [[ -z "${value}" ]]; then
    printf '%s\n' ""
    return
  fi

  local len=${#value}
  if (( len <= 4 )); then
    printf '%s\n' "****"
  else
    printf '%s\n' "${value:0:2}****${value: -2}"
  fi
}

report_line() {
  printf '%s\n' "$1" >>"${OUTPUT}"
}

report_cmd() {
  local title="$1"
  shift

  report_line "### ${title}"
  report_line ""
  report_line '```text'
  report_line "\$ $*"
  if "$@" >>"${OUTPUT}" 2>&1; then
    :
  else
    report_line "[command failed with exit code $?]"
  fi
  report_line '```'
  report_line ""
}

parse_jdbc_url() {
  YMATRIX_HOST=""
  YMATRIX_PORT=""
  YMATRIX_DATABASE=""

  if [[ -z "${YMATRIX_URL}" ]]; then
    return
  fi

  if [[ "${YMATRIX_URL}" =~ ^jdbc:postgresql://([^/:?]+)(:([0-9]+))?/([^?]+) ]]; then
    YMATRIX_HOST="${BASH_REMATCH[1]}"
    YMATRIX_PORT="${BASH_REMATCH[3]:-5432}"
    YMATRIX_DATABASE="${BASH_REMATCH[4]}"
  fi
}

test_tcp_port() {
  local host="$1"
  local port="$2"

  if have_cmd nc; then
    nc -z -w 3 "${host}" "${port}" >/dev/null 2>&1
    return $?
  fi

  if have_cmd timeout; then
    timeout 3 bash -lc ">/dev/tcp/${host}/${port}" >/dev/null 2>&1
    return $?
  fi

  return 2
}

detect_connector_jar() {
  local search_roots=(".")
  if [[ -n "${SPARK_HOME:-}" ]]; then
    search_roots+=("${SPARK_HOME}/jars")
  fi

  local root
  for root in "${search_roots[@]}"; do
    if [[ -d "${root}" ]]; then
      local found=""
      found="$(find "${root}" -type f -name 'spark-ymatrix-connector*.jar' | sort | head -n 1 || true)"
      if [[ -n "${found}" ]]; then
        printf '%s\n' "${found}"
        return
      fi
    fi
  done
}

derive_catalog_from_table() {
  local table_name="$1"
  if [[ "${table_name}" == *.*.* ]]; then
    printf '%s\n' "${table_name%%.*}"
  fi
}

derive_namespace_from_table() {
  local table_name="$1"
  if [[ "${table_name}" == *.* ]]; then
    printf '%s\n' "${table_name%.*}"
  fi
}

build_spark_sql_args() {
  SPARK_SQL_ARGS=()

  if [[ -n "${SPARK_MASTER}" ]]; then
    SPARK_SQL_ARGS+=(--master "${SPARK_MASTER}")
  fi

  if [[ -n "${SPARK_DRIVER_MEMORY}" ]]; then
    SPARK_SQL_ARGS+=(--driver-memory "${SPARK_DRIVER_MEMORY}")
  fi

  if [[ -n "${CONNECTOR_JAR}" ]]; then
    SPARK_SQL_ARGS+=(--jars "${CONNECTOR_JAR}")
  fi

  if [[ -n "${ICEBERG_CATALOG}" ]]; then
    SPARK_SQL_ARGS+=(
      --conf "spark.sql.catalog.${ICEBERG_CATALOG}=org.apache.iceberg.spark.SparkCatalog"
    )

    if [[ -n "${ICEBERG_TYPE}" ]]; then
      SPARK_SQL_ARGS+=(--conf "spark.sql.catalog.${ICEBERG_CATALOG}.type=${ICEBERG_TYPE}")
    fi

    if [[ -n "${ICEBERG_WAREHOUSE}" ]]; then
      SPARK_SQL_ARGS+=(--conf "spark.sql.catalog.${ICEBERG_CATALOG}.warehouse=${ICEBERG_WAREHOUSE}")
    fi

    if [[ -n "${ICEBERG_URI}" ]]; then
      SPARK_SQL_ARGS+=(--conf "spark.sql.catalog.${ICEBERG_CATALOG}.uri=${ICEBERG_URI}")
    fi
  fi

  local conf
  for conf in "${EXTRA_SPARK_CONF[@]}"; do
    SPARK_SQL_ARGS+=(--conf "${conf}")
  done
}

ICEBERG_TABLE="${ICEBERG_TABLE:-}"
ICEBERG_CATALOG="${ICEBERG_CATALOG:-}"
ICEBERG_TYPE="${ICEBERG_TYPE:-}"
ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-}"
ICEBERG_URI="${ICEBERG_URI:-}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4g}"
CONNECTOR_JAR="${CONNECTOR_JAR:-}"
YMATRIX_URL="${YMATRIX_URL:-}"
YMATRIX_USER="${YMATRIX_USER:-}"
YMATRIX_PASSWORD="${YMATRIX_PASSWORD:-}"
YMATRIX_SCHEMA="${YMATRIX_SCHEMA:-public}"
YMATRIX_TABLE="${YMATRIX_TABLE:-}"
SKIP_COUNT="false"
OUTPUT="${OUTPUT:-$(pwd)/iceberg-ymatrix-env-report-$(date +%Y%m%d-%H%M%S).md}"
EXTRA_SPARK_CONF=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --iceberg-table)
      ICEBERG_TABLE="$2"
      shift 2
      ;;
    --iceberg-catalog)
      ICEBERG_CATALOG="$2"
      shift 2
      ;;
    --iceberg-type)
      ICEBERG_TYPE="$2"
      shift 2
      ;;
    --iceberg-warehouse)
      ICEBERG_WAREHOUSE="$2"
      shift 2
      ;;
    --iceberg-uri)
      ICEBERG_URI="$2"
      shift 2
      ;;
    --spark-master)
      SPARK_MASTER="$2"
      shift 2
      ;;
    --driver-memory)
      SPARK_DRIVER_MEMORY="$2"
      shift 2
      ;;
    --connector-jar)
      CONNECTOR_JAR="$2"
      shift 2
      ;;
    --ymatrix-url)
      YMATRIX_URL="$2"
      shift 2
      ;;
    --ymatrix-user)
      YMATRIX_USER="$2"
      shift 2
      ;;
    --ymatrix-password)
      YMATRIX_PASSWORD="$2"
      shift 2
      ;;
    --ymatrix-schema)
      YMATRIX_SCHEMA="$2"
      shift 2
      ;;
    --ymatrix-table)
      YMATRIX_TABLE="$2"
      shift 2
      ;;
    --extra-conf)
      EXTRA_SPARK_CONF+=("$2")
      shift 2
      ;;
    --skip-count)
      SKIP_COUNT="true"
      shift
      ;;
    --output)
      OUTPUT="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

mkdir -p "$(dirname "${OUTPUT}")"

SPARK_SHELL_BIN="$(command -v spark-shell || true)"
SPARK_SQL_BIN="$(command -v spark-sql || true)"
SPARK_SUBMIT_BIN="$(command -v spark-submit || true)"
JAVA_BIN="$(command -v java || true)"
PSQL_BIN="$(command -v psql || true)"

if [[ -z "${CONNECTOR_JAR}" ]]; then
  CONNECTOR_JAR="$(detect_connector_jar || true)"
fi

if [[ -z "${ICEBERG_CATALOG}" && -n "${ICEBERG_TABLE}" ]]; then
  ICEBERG_CATALOG="$(derive_catalog_from_table "${ICEBERG_TABLE}" || true)"
fi

ICEBERG_NAMESPACE="$(derive_namespace_from_table "${ICEBERG_TABLE}" || true)"

parse_jdbc_url
build_spark_sql_args

TMPDIR_LOCAL="$(mktemp -d)"
trap 'rm -rf "${TMPDIR_LOCAL}"' EXIT

SOURCE_SQL_FILE="${TMPDIR_LOCAL}/source.sql"
YMATRIX_SQL_FILE="${TMPDIR_LOCAL}/ymatrix.sql"

report_line "# Iceberg to YMatrix Environment Report"
report_line ""
report_line "- Generated at: $(date '+%Y-%m-%d %H:%M:%S %Z')"
report_line "- Working directory: $(pwd)"
report_line "- Hostname: $(hostname)"
report_line "- Current user: $(whoami)"
report_line ""

report_line "## Summary"
report_line ""
report_line "- spark-shell: ${SPARK_SHELL_BIN:-NOT_FOUND}"
report_line "- spark-sql: ${SPARK_SQL_BIN:-NOT_FOUND}"
report_line "- spark-submit: ${SPARK_SUBMIT_BIN:-NOT_FOUND}"
report_line "- java: ${JAVA_BIN:-NOT_FOUND}"
report_line "- psql: ${PSQL_BIN:-NOT_FOUND}"
report_line "- connector jar: ${CONNECTOR_JAR:-NOT_FOUND}"
report_line "- iceberg table: ${ICEBERG_TABLE:-NOT_PROVIDED}"
report_line "- iceberg catalog: ${ICEBERG_CATALOG:-NOT_PROVIDED}"
report_line "- iceberg type: ${ICEBERG_TYPE:-NOT_PROVIDED}"
report_line "- iceberg warehouse: ${ICEBERG_WAREHOUSE:-NOT_PROVIDED}"
report_line "- iceberg uri: ${ICEBERG_URI:-NOT_PROVIDED}"
report_line "- ymatrix url: ${YMATRIX_URL:-NOT_PROVIDED}"
report_line "- ymatrix user: ${YMATRIX_USER:-NOT_PROVIDED}"
report_line "- ymatrix password: $(mask_secret "${YMATRIX_PASSWORD}")"
report_line "- ymatrix schema: ${YMATRIX_SCHEMA:-NOT_PROVIDED}"
report_line "- ymatrix table: ${YMATRIX_TABLE:-NOT_PROVIDED}"
report_line ""

report_line "## Local Environment"
report_line ""
report_cmd "uname -a" uname -a
report_cmd "env | sort (Spark and Iceberg subset)" bash -lc "env | sort | grep -E '^(SPARK|ICEBERG|JAVA_HOME|SCALA_HOME)=' || true"

if [[ -n "${JAVA_BIN}" ]]; then
  report_cmd "java -version" bash -lc "java -version"
fi

if [[ -n "${SPARK_SUBMIT_BIN}" ]]; then
  report_cmd "spark-submit --version" bash -lc "spark-submit --version"
fi

if [[ -n "${SPARK_SHELL_BIN}" ]]; then
  report_cmd "spark-shell --version" bash -lc "spark-shell --version"
fi

if [[ -n "${CONNECTOR_JAR}" && -f "${CONNECTOR_JAR}" ]]; then
  report_cmd "ls -lh connector jar" ls -lh "${CONNECTOR_JAR}"
else
  report_line "### Connector jar check"
  report_line ""
  report_line "Connector jar was not found automatically. If Spark does not already load it from its jars directory, provide it with --connector-jar."
  report_line ""
fi

report_line "## YMatrix Connectivity"
report_line ""

if [[ -n "${YMATRIX_HOST}" && -n "${YMATRIX_PORT}" ]]; then
  if test_tcp_port "${YMATRIX_HOST}" "${YMATRIX_PORT}"; then
    report_line "- TCP connectivity from this host to ${YMATRIX_HOST}:${YMATRIX_PORT}: OK"
  else
    report_line "- TCP connectivity from this host to ${YMATRIX_HOST}:${YMATRIX_PORT}: FAILED"
  fi
else
  report_line "- GP host/port parse from JDBC URL: SKIPPED"
fi
report_line ""

if [[ -n "${PSQL_BIN}" && -n "${YMATRIX_HOST}" && -n "${YMATRIX_PORT}" && -n "${YMATRIX_DATABASE}" && -n "${YMATRIX_USER}" && -n "${YMATRIX_PASSWORD}" ]]; then
  cat >"${YMATRIX_SQL_FILE}" <<EOF
select version();
select current_user, current_database(), current_schema();
select current_setting('server_version');
EOF

  if [[ -n "${YMATRIX_TABLE}" ]]; then
    cat >>"${YMATRIX_SQL_FILE}" <<EOF
select schemaname, tablename
from pg_catalog.pg_tables
where schemaname = '${YMATRIX_SCHEMA}'
  and tablename = '${YMATRIX_TABLE}';

select column_name, data_type
from information_schema.columns
where table_schema = '${YMATRIX_SCHEMA}'
  and table_name = '${YMATRIX_TABLE}'
order by ordinal_position;
EOF
  fi

  report_line "### psql checks"
  report_line ""
  report_line '```text'
  report_line "\$ PGPASSWORD=****** psql -h ${YMATRIX_HOST} -p ${YMATRIX_PORT} -U ${YMATRIX_USER} -d ${YMATRIX_DATABASE} -f ${YMATRIX_SQL_FILE}"
  if PGPASSWORD="${YMATRIX_PASSWORD}" psql -h "${YMATRIX_HOST}" -p "${YMATRIX_PORT}" -U "${YMATRIX_USER}" -d "${YMATRIX_DATABASE}" -f "${YMATRIX_SQL_FILE}" >>"${OUTPUT}" 2>&1; then
    :
  else
    report_line "[command failed with exit code $?]"
  fi
  report_line '```'
  report_line ""
else
  report_line "psql checks were skipped because psql or YMatrix credentials were not fully available."
  report_line ""
fi

report_line "## Iceberg Source Inspection"
report_line ""

if [[ -n "${SPARK_SQL_BIN}" && -n "${ICEBERG_TABLE}" && -n "${ICEBERG_CATALOG}" ]]; then
  {
    printf 'show catalogs;\n'
    printf 'show namespaces in %s;\n' "${ICEBERG_CATALOG}"
    if [[ -n "${ICEBERG_NAMESPACE}" ]]; then
      printf 'show tables in %s;\n' "${ICEBERG_NAMESPACE}"
    fi
    printf 'describe table %s;\n' "${ICEBERG_TABLE}"
    if [[ "${SKIP_COUNT}" != "true" ]]; then
      printf 'select count(*) as row_count from %s;\n' "${ICEBERG_TABLE}"
    fi
    printf 'select * from %s limit 5;\n' "${ICEBERG_TABLE}"
  } >"${SOURCE_SQL_FILE}"

  report_line "### spark-sql inspection"
  report_line ""
  report_line '```text'
  report_line "\$ spark-sql ${SPARK_SQL_ARGS[*]} -f ${SOURCE_SQL_FILE}"
  if spark-sql "${SPARK_SQL_ARGS[@]}" -f "${SOURCE_SQL_FILE}" >>"${OUTPUT}" 2>&1; then
    :
  else
    report_line "[command failed with exit code $?]"
  fi
  report_line '```'
  report_line ""
else
  report_line "spark-sql inspection was skipped because spark-sql, iceberg table, or iceberg catalog was not fully available."
  report_line ""
fi

report_line "## What To Send Back"
report_line ""
report_line "Send this report file back together with:"
report_line "- The exact spark-shell start command you normally use"
report_line "- Whether the YMatrix target table should be auto-created or already exists"
report_line "- Whether you want full load only, or full load plus incremental sync"
report_line ""

printf 'Report written to %s\n' "${OUTPUT}"
