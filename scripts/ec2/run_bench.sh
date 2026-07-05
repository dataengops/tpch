#!/usr/bin/env bash
# scripts/ec2/run_bench.sh — run the TPC-H benchmark on the staged NVMe data.
# Run on the box over SSH from /opt/polars-benchmark (after stage_data.sh).
#
# Cold single-pass, all 22 queries, per engine (default: duckdb + polars) -- the
# same methodology as the SF400 Spark/Trino cluster runs, so numbers are directly
# comparable. Each query runs in its OWN subprocess with a hard timeout, and a
# failure (OOM/timeout) is recorded and the run CONTINUES -- unlike the harness's
# built-in `execute_all` (check=True), which aborts the whole run on first failure.
# That resilience matters at SF400 where e.g. Polars q5 can OOM.
#
# Timings land in output/run/timings.csv (RUN_LOG_TIMINGS) and are uploaded to S3.
set -uo pipefail   # NOT -e: we tolerate per-query failures on purpose

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
[[ -n "${SCALE_FACTOR:-}" ]] || source "${HERE}/config.env"
: "${TABLES_DIR:?}"; : "${SCALE_FACTOR:?}"; : "${QUERY_TIMEOUT:?}"
: "${POLARS_STREAMING:?}"; : "${TIMINGS_S3:?}"; : "${AWS_REGION:?}"

cd "${REPO_ROOT}"
PY="${REPO_ROOT}/.venv/bin/python"
[[ -x "${PY}" ]] || PY="python3"
ENGINES="${ENGINES:-duckdb polars}"
STAMP="$(date +%Y%m%d-%H%M%S)"
RUN_LOG="${REPO_ROOT}/output/run/console-${STAMP}.log"
mkdir -p "${REPO_ROOT}/output/run"

# Cold single-pass harness config, exported to every query subprocess.
export PATH_TABLES="${TABLES_DIR}"
export SCALE_FACTOR="${SCALE_FACTOR}"
export RUN_IO_TYPE="parquet"
export RUN_PRE_RUN="false"        # cold: no untimed warm-up pass
export RUN_ITERATIONS="1"         # single timed pass
export RUN_LOG_TIMINGS="true"     # append to output/run/timings.csv
export RUN_POLARS_STREAMING="${POLARS_STREAMING}"

# Fresh timings file for this run (keep any prior one as a backup).
TIMINGS_CSV="${REPO_ROOT}/output/run/timings.csv"
[[ -f "${TIMINGS_CSV}" ]] && mv "${TIMINGS_CSV}" "${TIMINGS_CSV%.csv}-pre-${STAMP}.csv"

log() { echo "$@" | tee -a "${RUN_LOG}"; }

log "=================================================================="
log "SF${SCALE_FACTOR} benchmark | engines: ${ENGINES} | streaming=${POLARS_STREAMING}"
log "data: ${TABLES_DIR}/scale-${SCALE_FACTOR} | cold single-pass | timeout ${QUERY_TIMEOUT}s"
log "host: $(nproc) vCPU / $(free -g | awk '/^Mem:/{print $2}') GiB | $(date)"
log "=================================================================="

for eng in ${ENGINES}; do
  log ""
  log "########## ${eng} ##########"
  eng_total=0
  for i in $(seq 1 22); do
    start=$(date +%s.%N)
    if timeout "${QUERY_TIMEOUT}" "${PY}" -m "queries.${eng}.q${i}" >>"${RUN_LOG}" 2>&1; then
      end=$(date +%s.%N)
      dur=$(awk "BEGIN{printf \"%.1f\", ${end}-${start}}")
      eng_total=$(awk "BEGIN{printf \"%.1f\", ${eng_total}+${dur}}")
      log "  ${eng} q$(printf '%02d' "${i}")  OK    ${dur}s"
    else
      rc=$?
      [[ "${rc}" -eq 124 ]] && why="TIMEOUT(${QUERY_TIMEOUT}s)" || why="FAIL(rc=${rc})"
      log "  ${eng} q$(printf '%02d' "${i}")  ${why}"
    fi
  done
  log "  ${eng} wall-total (completed queries): ${eng_total}s"
done

log ""
log "== timings.csv =="
[[ -f "${TIMINGS_CSV}" ]] && cat "${TIMINGS_CSV}" | tee -a "${RUN_LOG}"

# Upload results (timings + console log) to S3.
DEST="${TIMINGS_S3}/run-${STAMP}"
log ""
log "Uploading results -> ${DEST}"
[[ -f "${TIMINGS_CSV}" ]] && aws s3 cp "${TIMINGS_CSV}" "${DEST}/timings.csv" --region "${AWS_REGION}"
aws s3 cp "${RUN_LOG}" "${DEST}/console.log" --region "${AWS_REGION}"
log "Done. Results at ${DEST}"
