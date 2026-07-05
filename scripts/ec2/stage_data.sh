#!/usr/bin/env bash
# scripts/ec2/stage_data.sh — put the SF400 parquet on local NVMe in the layout the
# harness expects:  ${TABLES_DIR}/scale-${SCALE_FACTOR}/{table}.parquet
# Run on the box over SSH (from /opt/polars-benchmark, after sourcing config.env).
#
# Two modes (STAGE_MODE in config.env):
#   generate  tpchgen-cli writes the 8 tables straight to NVMe. No S3 egress, fast,
#             bit-identical to the S3 copy (that copy was itself tpchgen output).
#   copy      aws s3 cp the shared per-table parquet down from ${DATA_S3}/<table>/.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
[[ -n "${SCALE_FACTOR:-}" ]] || source "${HERE}/config.env"
: "${TABLES_DIR:?}"; : "${SCALE_FACTOR:?}"; : "${STAGE_MODE:?}"

DEST="${TABLES_DIR}/scale-${SCALE_FACTOR}"
TABLES=(customer lineitem nation orders part partsupp region supplier)
mkdir -p "${DEST}"

# Skip if already staged (all 8 tables present) -- makes re-runs cheap.
missing=0
for t in "${TABLES[@]}"; do [[ -s "${DEST}/${t}.parquet" ]] || missing=1; done
if [[ "${missing}" -eq 0 ]]; then
  echo "All 8 tables already present in ${DEST} -- nothing to do."
  du -h -c "${DEST}"/*.parquet | tail -1
  exit 0
fi

case "${STAGE_MODE}" in
  generate)
    SCALE_NUM="${SCALE_FACTOR%.*}"                    # 400.0 -> 400 for tpchgen
    echo "== GENERATE SF${SCALE_NUM} parquet -> ${DEST} (tpchgen-cli) =="
    TPCHGEN="${HERE}/../../.venv/bin/tpchgen-cli"
    [[ -x "${TPCHGEN}" ]] || TPCHGEN="tpchgen-cli"
    # tpchgen-cli writes one <table>.parquet per table into --output-dir.
    "${TPCHGEN}" \
      --scale-factor "${SCALE_NUM}" \
      --output-dir "${DEST}" \
      --format parquet
    ;;
  copy)
    echo "== COPY SF400 parquet from ${DATA_S3} -> ${DEST} (aws s3 cp) =="
    : "${DATA_S3:?}"; : "${AWS_REGION:?}"
    for t in "${TABLES[@]}"; do
      [[ -s "${DEST}/${t}.parquet" ]] && { echo "  skip ${t} (present)"; continue; }
      # Each table dir holds exactly one parquet object -- copy it to <table>.parquet.
      key=$(aws s3 ls "${DATA_S3}/${t}/" --region "${AWS_REGION}" \
              | awk '{print $NF}' | grep -E '\.parquet$' | head -1)
      [[ -n "${key}" ]] || { echo "  no parquet under ${DATA_S3}/${t}/" >&2; exit 3; }
      echo "  ${t}: ${key}"
      aws s3 cp "${DATA_S3}/${t}/${key}" "${DEST}/${t}.parquet" --region "${AWS_REGION}"
    done
    ;;
  *)
    echo "unknown STAGE_MODE='${STAGE_MODE}' (want generate|copy)" >&2; exit 2 ;;
esac

echo "== staged tables =="
ls -lh "${DEST}"/*.parquet
du -h -c "${DEST}"/*.parquet | tail -1
