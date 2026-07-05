#!/usr/bin/env bash
# Run the 22 TPC-H (PDS) queries via run_local_spark.sh, but in small batches,
# each batch a fresh `spark-submit` process (fresh JVM).
#
# Why: run_local_spark.sh's single long-lived SparkSession accumulates OS page
# cache (from repeatedly scanning the parquet tables) for the whole 22-query
# run, inside a memory-capped WSL2 VM. That squeeze gets worse the further
# into the run you get, and no DRIVER_MEM value fixes it -- it just moves
# which query it dies on (seen crashes at 48g, 56g, 72g, 78g). A fresh JVM per
# batch resets heap + page cache between batches instead.
#
# Usage:
#   bash scripts/run_local_spark_batched.sh                  # all 22, batches of 5
#   BATCH_SIZE=4 DRIVER_MEM=56g bash scripts/run_local_spark_batched.sh
#   QUERIES=1,3,5,8,21 BATCH_SIZE=2 bash scripts/run_local_spark_batched.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "${HERE}/.." && pwd)"

BATCH_SIZE="${BATCH_SIZE:-5}"
QUERIES="${QUERIES:-$(seq -s, 1 22)}"
FINAL_OUT="${TIMINGS_OUT:-${REPO}/output/local_spark_timings}"
BATCH_ROOT="${REPO}/output/.local_spark_batches"

rm -rf "${FINAL_OUT}" "${BATCH_ROOT}"
mkdir -p "${BATCH_ROOT}"

IFS=',' read -r -a ALL_QUERIES <<< "${QUERIES}"

echo "batches:     ${#ALL_QUERIES[@]} queries in groups of ${BATCH_SIZE}"
echo

RESULT_FILES=()
batch_num=0
for ((i = 0; i < ${#ALL_QUERIES[@]}; i += BATCH_SIZE)); do
  batch_num=$((batch_num + 1))
  chunk="$(IFS=,; echo "${ALL_QUERIES[*]:i:BATCH_SIZE}")"
  batch_out="${BATCH_ROOT}/batch_${batch_num}"

  echo "=== batch ${batch_num}: queries ${chunk} ==="
  QUERIES="${chunk}" TIMINGS_OUT="${batch_out}" bash "${HERE}/run_local_spark.sh"
  echo

  RESULT_FILES+=("${batch_out}/part-00000")
done

# ---- merge per-batch CSVs into one final timings file -----------------------
mkdir -p "${FINAL_OUT}"
{
  echo "query_number,duration_s"
  for f in "${RESULT_FILES[@]}"; do
    grep -E '^[0-9]+,' "${f}"
  done
  total="$(for f in "${RESULT_FILES[@]}"; do grep -E '^[0-9]+,' "${f}"; done | cut -d, -f2 | paste -sd+ | bc)"
  echo "total,${total}"
} > "${FINAL_OUT}/part-00000"

echo "Merged timings written to: ${FINAL_OUT}/part-00000"
