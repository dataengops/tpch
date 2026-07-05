#!/usr/bin/env bash
# Stage the per-table SF400 parquet from S3 into cluster HDFS (local NVMe) so the
# Spark benchmark scans local disk instead of S3. Fetched to the EMR primary and
# run as a command-runner step *before* the spark-submit step (see
# submit_emr_ec2.sh, HDFS_BASE mode).
#
#   $1 BUCKET       e.g. your_s3_bucket
#   $2 DATA_PREFIX  per-table prefix under the bucket, e.g. scale-400.0/trino
#   $3 HDFS_BASE    target HDFS URI, e.g. hdfs:///tpch/sf400
#   $4 REGION       e.g. us-east-1
#
# Idempotent: skips the distributed copy if the marker table dir already exists.
set -euo pipefail

BUCKET="$1"; DATA_PREFIX="$2"; HDFS_BASE="$3"; REGION="$4"

SRC="s3://${BUCKET}/${DATA_PREFIX}/"
HDFS_DIR="${HDFS_BASE#hdfs://}"; HDFS_DIR="/${HDFS_DIR#/}"   # path part, leading /

echo "== Copy S3 -> HDFS (s3-dist-cp) =="
if hdfs dfs -test -d "${HDFS_DIR}/lineitem" 2>/dev/null; then
  echo "  skip (HDFS already populated): ${HDFS_DIR}"
else
  echo "  ${SRC} -> ${HDFS_BASE}"
  # s3-dist-cp is not on PATH in a bash step; invoke via its jar (fall back to
  # the bare command if a future AMI does put it on PATH).
  if command -v s3-dist-cp >/dev/null 2>&1; then
    s3-dist-cp --src "${SRC}" --dest "${HDFS_BASE%/}/"
  else
    S3DC_JAR=$(find /usr -name 's3-dist-cp*.jar' 2>/dev/null | head -1 || true)
    if [ -z "${S3DC_JAR}" ]; then
      echo "s3-dist-cp not found (is the Hadoop application installed?)" >&2; exit 3
    fi
    hadoop jar "${S3DC_JAR}" --src "${SRC}" --dest "${HDFS_BASE%/}/"
  fi
fi

# Single-node-scan fix: each table is ONE big parquet file, so s3-dist-cp uses one
# mapper per file and writes every block to that mapper's local datanode. With
# default replication those blocks live on only 1-2 datanodes, so a locality-
# preferring scheduler pins the whole scan there. Replicate every block to ALL
# datanodes so the scan spreads evenly across the core nodes.
NUM_DN=$(hdfs dfsadmin -report 2>/dev/null | grep -c '^Name:' || echo 0)
if [ "${NUM_DN}" -ge 2 ]; then
  echo "  Replicating blocks to all ${NUM_DN} datanodes (setrep -w) for balanced scans..."
  hdfs dfs -setrep -w "${NUM_DN}" "${HDFS_DIR}" || true
fi
echo "  HDFS usage:"; hdfs dfs -du -h -s "${HDFS_DIR}" || true
echo "Done staging ${HDFS_DIR}"
