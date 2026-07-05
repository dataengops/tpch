#!/usr/bin/env bash
# Run the same 22 TPC-H (PDS) SQL queries as the EMR job, but on ONE local
# machine in Spark *local mode* (no cluster). Ideal for the Ryzen 5 3600X /
# 96 GB box: the whole SF100 dataset (~25 GB parquet) fits in RAM.
#
# Reuses scripts/emr/emr_tpch_benchmark.py unchanged. In local[*] mode Spark
# runs the executor *inside the driver JVM*, so spark.driver.memory is the knob
# that matters -- we set it via PYSPARK_SUBMIT_ARGS before the JVM starts
# (a .config() call after startup would be ignored for the driver heap).
#
# Usage:
#   bash scripts/run_local_spark.sh                       # all 22, ./datain
#   DATA_DIR=~/datain DRIVER_MEM=72g bash scripts/run_local_spark.sh
#   QUERIES=1,6,9 ITERATIONS=3 bash scripts/run_local_spark.sh
#   bash scripts/run_local_spark.sh 1,6,9                 # same, as a plain arg
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "${HERE}/.." && pwd)"

# ---- config (env-overridable) ----------------------------------------------
DATA_DIR="${DATA_DIR:-${REPO}/datain}"        # folder holding the 8 *.parquet
DRIVER_MEM="${DRIVER_MEM:-72g}"               # ~75% of 96 GB; leave OS+Python headroom
CORES="${CORES:-*}"                           # local[*] = all threads; e.g. CORES=12
SHUFFLE_PARTS="${SHUFFLE_PARTS:-24}"          # ~2x threads for a single node
QUERIES="${QUERIES:-${1:-}}"                   # e.g. "1,6,9"; empty = all 22
ITERATIONS="${ITERATIONS:-1}"
TIMINGS_OUT="${TIMINGS_OUT:-${REPO}/output/local_spark_timings}"  # dir must not pre-exist

JOB="${REPO}/scripts/emr/emr_tpch_benchmark.py"

# ---- pick a python (prefer the repo venv) ----------------------------------
if [[ -x "${REPO}/.venv/bin/python" ]]; then
  PY="${REPO}/.venv/bin/python"
else
  PY="$(command -v python3 || command -v python)"
fi

# ---- sanity checks ---------------------------------------------------------
if [[ ! -f "${DATA_DIR}/lineitem.parquet" ]]; then
  echo "ERROR: ${DATA_DIR}/lineitem.parquet not found. Set DATA_DIR to your parquet folder." >&2
  exit 1
fi
rm -rf "${TIMINGS_OUT}"   # Spark's saveAsTextFile refuses to overwrite an existing dir

echo "python:      ${PY}"
echo "data dir:    ${DATA_DIR}"
echo "driver mem:  ${DRIVER_MEM}   cores: local[${CORES}]   shuffle parts: ${SHUFFLE_PARTS}"
echo "queries:     ${QUERIES:-all 22}   iterations: ${ITERATIONS}"
echo

# ---- shuffle/spill scratch dir -- must be real disk, NOT /tmp (tmpfs = RAM-backed
# on WSL2, so spilling there both competes with the JVM heap for RAM and is capped
# at whatever size tmpfs was given, causing "No space left on device").
SPARK_LOCAL_DIR="${SPARK_LOCAL_DIR:-${REPO}/.spark_local}"
rm -rf "${SPARK_LOCAL_DIR}"   # orphaned spill data survives crashes (no shutdown-hook cleanup)
mkdir -p "${SPARK_LOCAL_DIR}"

# ---- drive Spark local-mode config via submit args (must end in pyspark-shell)
export PYSPARK_SUBMIT_ARGS="--master local[${CORES}] --driver-memory ${DRIVER_MEM} --conf spark.local.dir=${SPARK_LOCAL_DIR} --conf spark.sql.shuffle.partitions=${SHUFFLE_PARTS} --conf spark.sql.parquet.enableVectorizedReader=true pyspark-shell"

ARGS=( --data-path "${DATA_DIR}" --table-suffix ".parquet" --iterations "${ITERATIONS}" --timings-out "${TIMINGS_OUT}" )
if [[ -n "${QUERIES}" ]]; then ARGS+=( --queries "${QUERIES}" ); fi

time "${PY}" "${JOB}" "${ARGS[@]}"

echo
echo "Timings CSV written under: ${TIMINGS_OUT}/part-00000"
