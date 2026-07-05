#!/usr/bin/env bash
# Runs on the EMR primary node (fetched + invoked by the command-runner step).
# 1) reshape flat SF400 parquet into per-table prefixes for the Hive connector
# 2) install a small venv (trino client, pyarrow, boto3)
# 3) register the 8 tables in Glue (DDL derived from parquet footers)
# 4) run the 22-query benchmark against the local coordinator
# 5) upload the timings CSV to S3
#
# Requires the subnet to reach S3 AND the internet (pip install). Use a public
# subnet or a NAT gateway.
set -euo pipefail

BUCKET="$1"; DATA_PREFIX="$2"; CODE_PREFIX="$3"
TIMINGS_S3="$4"; SCHEMA="$5"; REGION="$6"
# Optional 7th arg: HDFS base URI (e.g. hdfs:///tpch/sf400). When set, the
# per-table trino/ parquet is s3-dist-cp'd from S3 into HDFS on the cluster and
# the Glue tables point at hdfs:// instead of s3:// -- so queries read local
# NVMe via HDFS instead of S3. Empty = read straight from S3 (original mode).
HDFS_BASE="${7:-}"

TABLES="customer lineitem nation orders part partsupp region supplier"
WORK=/home/hadoop/trino-bench
mkdir -p "${WORK}/code"

echo "== 1. Reshape flat parquet -> per-table prefixes =="
# The per-table trino/ copies are the canonical SF400 layout (shared with Spark).
# Check the destination FIRST: if it already exists we skip, so the flat root
# *.parquet source may be deleted once the copies are made. Only when a copy is
# missing do we require the flat source and copy it.
for t in ${TABLES}; do
  SRC="s3://${BUCKET}/${DATA_PREFIX}/${t}.parquet"
  DST="s3://${BUCKET}/${DATA_PREFIX}/trino/${t}/${t}.parquet"
  SRC_KEY="${DATA_PREFIX}/${t}.parquet"
  DST_KEY="${DATA_PREFIX}/trino/${t}/${t}.parquet"
  DST_SIZE=$(aws s3api head-object --bucket "${BUCKET}" --key "${DST_KEY}" \
    --region "${REGION}" --query ContentLength --output text 2>/dev/null || true)
  if [ -n "${DST_SIZE}" ]; then
    echo "  skip (exists, ${DST_SIZE} B): ${DST}"
    continue
  fi
  SRC_SIZE=$(aws s3api head-object --bucket "${BUCKET}" --key "${SRC_KEY}" \
    --region "${REGION}" --query ContentLength --output text 2>/dev/null || true)
  if [ -z "${SRC_SIZE}" ]; then
    echo "MISSING both copy and source for table ${t}: ${DST} / ${SRC}" >&2
    exit 1
  fi
  echo "  ${SRC} -> ${DST}"
  aws s3 cp "${SRC}" "${DST}" --region "${REGION}"
done

SRC_TRINO="s3://${BUCKET}/${DATA_PREFIX}/trino/"
LOC_ARGS=()
if [ -n "${HDFS_BASE}" ]; then
  echo "== 1b. Copy S3 -> HDFS (s3-dist-cp) =="
  HDFS_DIR="${HDFS_BASE#hdfs://}"; HDFS_DIR="/${HDFS_DIR#/}"   # path part, leading /
  # Idempotent: skip the distributed copy if the marker table dir already exists.
  if hdfs dfs -test -d "${HDFS_DIR}/lineitem" 2>/dev/null; then
    echo "  skip (HDFS already populated): ${HDFS_DIR}"
  else
    echo "  ${SRC_TRINO} -> ${HDFS_BASE}"
    # s3-dist-cp is not on PATH in a bash step; invoke via its jar (fall back to
    # the bare command if a future AMI does put it on PATH).
    if command -v s3-dist-cp >/dev/null 2>&1; then
      s3-dist-cp --src "${SRC_TRINO}" --dest "${HDFS_BASE%/}/"
    else
      # `ls` no-match exits 2; `|| true` keeps set -e from killing the script.
      S3DC_JAR=$(find /usr -name 's3-dist-cp*.jar' 2>/dev/null | head -1 || true)
      if [ -z "${S3DC_JAR}" ]; then
        echo "s3-dist-cp not found (is the Hadoop application installed?)" >&2; exit 3
      fi
      hadoop jar "${S3DC_JAR}" --src "${SRC_TRINO}" --dest "${HDFS_BASE%/}/"
    fi
  fi
  # Single-node-scan fix: each table is ONE big parquet file, so s3-dist-cp uses
  # one mapper per file and writes every block to that mapper's local datanode.
  # With default replication those blocks live on only 1-2 datanodes, so Trino's
  # local-affinity scheduler pins the whole scan there. Replicate every block to
  # ALL datanodes so the scan spreads evenly across the core nodes.
  NUM_DN=$(hdfs dfsadmin -report 2>/dev/null | grep -c '^Name:' || echo 0)
  if [ "${NUM_DN}" -ge 2 ]; then
    echo "  Replicating blocks to all ${NUM_DN} datanodes (setrep -w) for balanced scans..."
    hdfs dfs -setrep -w "${NUM_DN}" "${HDFS_DIR}" || true
  fi
  echo "  HDFS usage:"; hdfs dfs -du -h -s "${HDFS_DIR}" || true
  LOC_ARGS=(--location-base "${HDFS_BASE%/}")
fi

echo "== 2. Python venv + deps =="
python3 -m venv "${WORK}/venv"
# shellcheck disable=SC1091
source "${WORK}/venv/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet trino pyarrow boto3

echo "== 3. Fetch benchmark code =="
aws s3 cp --recursive "s3://${BUCKET}/${CODE_PREFIX}/" "${WORK}/code/" --region "${REGION}"

echo "== 4. Register Glue tables =="
python "${WORK}/code/trino_setup_catalog.py" \
  --bucket "${BUCKET}" --data-prefix "${DATA_PREFIX}/trino" \
  --schema "${SCHEMA}" --region "${REGION}" "${LOC_ARGS[@]}"

echo "== 5. Run benchmark =="
python "${WORK}/code/trino_tpch_benchmark.py" \
  --host localhost --port 8889 --catalog hive --schema "${SCHEMA}" \
  --iterations 1 --timings-out "${WORK}/timings.csv"

echo "== 6. Upload timings =="
aws s3 cp "${WORK}/timings.csv" "${TIMINGS_S3}/timings.csv" --region "${REGION}"
echo "Done. Timings at ${TIMINGS_S3}/timings.csv"
