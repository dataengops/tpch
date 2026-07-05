#!/usr/bin/env bash
# Submit the SF400 TPC-H Trino benchmark to a transient EMR-on-EC2 cluster.
#
# Shape (fixed for the SF400 comparison): 1x m7i.xlarge primary + 2x r6id.8xlarge
# core = 68 vCPU / 512 GiB, under the 96-vCPU quota. Trino runs as a service on
# the coordinator; a single command-runner step runs trino_run.sh on the primary
# (reorg -> Glue setup -> benchmark -> upload), then --auto-terminate shuts down.
#
# Prereqs:
#   - AWS CLI v2 with rights to EMR + EC2 + S3 + Glue + IAM.
#   - Flat SF400 parquet at s3://$BUCKET/$DATA_PREFIX/{table}.parquet (from the
#     DuckDB/Polars plan's `aws s3 sync`).
#   - A subnet that reaches S3 AND the internet (pip install on the primary):
#     public subnet or NAT gateway.
#   - Default EMR roles (created below) able to read/write $BUCKET and use Glue.
set -euo pipefail

# ---- config (edit these) ---------------------------------------------------
REGION="${REGION:-us-east-1}"
BUCKET="${BUCKET:-your_s3_bucket}"
SUBNET_ID="${SUBNET_ID:-subnet-xxxxxxxxxxxxxxxxx}"
RELEASE="${RELEASE:-emr-7.5.0}"

DATA_PREFIX="${DATA_PREFIX:-scale-400.0}"     # flat *.parquet live here
SCHEMA="${SCHEMA:-tpch_sf400}"                # Glue database name
QUERIES="${QUERIES:-}"                        # reserved; default = all 22

PRIMARY_TYPE="${PRIMARY_TYPE:-m7i.xlarge}"
CORE_TYPE="${CORE_TYPE:-r6id.8xlarge}"
CORE_COUNT="${CORE_COUNT:-2}"

# Optional EC2 key pair for SSH access to the primary (e.g. to tunnel the Trino
# Web UI on port 8889: ssh -i key.pem -L 8889:localhost:8889 hadoop@<primary-dns>).
# Leave empty for a headless batch run. The instance's security group must also
# allow inbound TCP 22 from your IP; --auto-terminate still kills the box when
# the step finishes, so drop that below if you want to keep poking the UI.
KEY_NAME="${KEY_NAME:-}"

# HDFS mode: when set (e.g. hdfs:///tpch/sf400), trino_run.sh s3-dist-cp's the
# per-table parquet from S3 into cluster HDFS and points the Glue tables there,
# so queries read local NVMe instead of S3. Requires CONFIG_NAME=emr-trino-config-hdfs.json.
HDFS_BASE="${HDFS_BASE:-}"

# AUTO_TERMINATE=false keeps the cluster up after the step and lets the step
# FAIL WITHOUT terminating the cluster -- so you can SSH in (needs KEY_NAME), read
# logs, and re-submit steps instead of a blind auto-terminate relaunch. Remember
# to `aws emr terminate-clusters` yourself afterwards.
AUTO_TERMINATE="${AUTO_TERMINATE:-true}"

CODE_PREFIX="code/trino"
TIMINGS_S3="s3://${BUCKET}/output/trino_timings/run-$(date +%Y%m%d-%H%M%S)"
LOG_S3="s3://${BUCKET}/logs/emr/"

HERE="$(cd "$(dirname "$0")" && pwd)"
CONFIG_JSON="${HERE}/${CONFIG_NAME:-emr-trino-config.json}"
# On Git Bash / MSYS the native aws.exe cannot read a file:///c/... URI; give it
# a Windows path (C:/...). Elsewhere the plain path works as-is.
if command -v cygpath >/dev/null 2>&1; then
  CONFIG_URI="file://$(cygpath -m "${CONFIG_JSON}")"
else
  CONFIG_URI="file://${CONFIG_JSON}"
fi

# --ec2-attributes: add KeyName only when KEY_NAME is set (empty KeyName is rejected).
EC2_ATTRS="SubnetId=${SUBNET_ID}"
if [[ -n "${KEY_NAME}" ]]; then
  EC2_ATTRS="${EC2_ATTRS},KeyName=${KEY_NAME}"
fi

# vCPU per node (override for non-default instance types). Defaults match the
# original 68-vCPU shape: 4 (m7i.xlarge primary) + 32 (r6id.8xlarge core).
PRIMARY_VCPU="${PRIMARY_VCPU:-4}"
CORE_VCPU="${CORE_VCPU:-32}"
TOTAL_VCPU=$(( PRIMARY_VCPU + CORE_COUNT * CORE_VCPU ))
echo "Cluster vCPU: ${TOTAL_VCPU} (1x ${PRIMARY_TYPE} + ${CORE_COUNT}x ${CORE_TYPE}); 96-vCPU quota."

# ---- 1. default EMR roles --------------------------------------------------
echo "Ensuring default EMR roles exist..."
aws emr create-default-roles --region "${REGION}" >/dev/null
# NOTE: EMR_EC2_DefaultRole must read/write $BUCKET and have Glue access
# (AWSGlueConsoleFullAccess or an equivalent scoped policy) for the catalog.

# ---- 2. upload benchmark code ----------------------------------------------
echo "Uploading benchmark code to s3://${BUCKET}/${CODE_PREFIX}/"
for f in trino_queries.py trino_setup_catalog.py trino_tpch_benchmark.py trino_run.sh trino_bootstrap.sh; do
  aws s3 cp "${HERE}/${f}" "s3://${BUCKET}/${CODE_PREFIX}/${f}" --region "${REGION}"
done
SPILL_DIR="/mnt1/trino-spill"   # must match spiller-spill-path in the config JSON

# ---- 3. step: fetch + run the driver on the primary ------------------------
RUN_CMD="aws s3 cp s3://${BUCKET}/${CODE_PREFIX}/trino_run.sh /tmp/trino_run.sh --region ${REGION} && bash /tmp/trino_run.sh ${BUCKET} ${DATA_PREFIX} ${CODE_PREFIX} ${TIMINGS_S3} ${SCHEMA} ${REGION} ${HDFS_BASE}"

# HDFS mode needs the Hadoop application (HDFS daemons + s3-dist-cp). Trino alone
# ships no HDFS and no s3-dist-cp binary.
if [[ -n "${HDFS_BASE}" ]]; then
  APPLICATIONS=(Name=Trino Name=Hadoop)
else
  APPLICATIONS=(Name=Trino)
fi

# Interactive mode: step failure leaves the cluster up (CONTINUE) so we can debug.
if [[ "${AUTO_TERMINATE}" == "true" ]]; then
  STEP_ON_FAILURE="TERMINATE_CLUSTER"; TERMINATE_FLAG=(--auto-terminate)
else
  STEP_ON_FAILURE="CONTINUE";          TERMINATE_FLAG=(--no-auto-terminate)
fi

# ---- 4. launch the transient cluster with the step attached ----------------
echo "Launching cluster (auto-terminates when the step finishes)..."
CLUSTER_ID=$(aws emr create-cluster \
  --name pds-tpch-trino \
  --release-label "${RELEASE}" \
  --applications "${APPLICATIONS[@]}" \
  --region "${REGION}" \
  --log-uri "${LOG_S3}" \
  --use-default-roles \
  --ec2-attributes "${EC2_ATTRS}" \
  --bootstrap-actions "[
    {\"Name\":\"prep-trino-spill\",\"Path\":\"s3://${BUCKET}/${CODE_PREFIX}/trino_bootstrap.sh\",\"Args\":[\"${SPILL_DIR}\"]}
  ]" \
  --instance-groups "[
    {\"Name\":\"Primary\",\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"${PRIMARY_TYPE}\",\"InstanceCount\":1},
    {\"Name\":\"Core\",\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${CORE_TYPE}\",\"InstanceCount\":${CORE_COUNT}}
  ]" \
  --configurations "${CONFIG_URI}" \
  --steps "[
    {
      \"Type\":\"CUSTOM_JAR\",
      \"Name\":\"pds-tpch-trino\",
      \"ActionOnFailure\":\"${STEP_ON_FAILURE}\",
      \"Jar\":\"command-runner.jar\",
      \"Args\":[\"bash\",\"-c\",\"${RUN_CMD}\"]
    }
  ]" \
  "${TERMINATE_FLAG[@]}" \
  --query ClusterId --output text)

echo "Launched cluster ${CLUSTER_ID}. Timings -> ${TIMINGS_S3}/timings.csv"

# ---- 5. wait for terminal state --------------------------------------------
if [[ "${AUTO_TERMINATE}" != "true" ]]; then
  echo "Interactive cluster (no auto-terminate). Watching the step; cluster stays up after."
  echo "  SSH: ssh -i ${KEY_NAME:-<key>}.pem -L 8889:localhost:8889 hadoop@<primary-dns>"
  echo "  Terminate when done: aws emr terminate-clusters --region ${REGION} --cluster-ids ${CLUSTER_ID}"
  while true; do
    SSTATE=$(aws emr list-steps --region "${REGION}" --cluster-id "${CLUSTER_ID}" \
      --query 'Steps[0].Status.State' --output text)
    echo "  $(date +%T)  step=${SSTATE}"
    case "${SSTATE}" in
      COMPLETED) echo "Step done. Timings: ${TIMINGS_S3}/timings.csv (cluster ${CLUSTER_ID} still UP)"; break ;;
      FAILED|CANCELLED) echo "Step ${SSTATE}. Cluster ${CLUSTER_ID} still UP for debugging. Logs: ${LOG_S3}"; break ;;
    esac
    sleep 30
  done
  exit 0
fi

echo "Waiting for terminal state (Ctrl-C stops watching; cluster keeps running)..."
while true; do
  STATE=$(aws emr describe-cluster --region "${REGION}" --cluster-id "${CLUSTER_ID}" \
    --query 'Cluster.Status.State' --output text)
  REASON=$(aws emr describe-cluster --region "${REGION}" --cluster-id "${CLUSTER_ID}" \
    --query 'Cluster.Status.StateChangeReason.Message' --output text)
  echo "  $(date +%T)  ${STATE}  ${REASON}"
  case "${STATE}" in
    TERMINATED)             echo "Done. Timings: ${TIMINGS_S3}/timings.csv"; break ;;
    TERMINATED_WITH_ERRORS) echo "Cluster failed. Logs: ${LOG_S3}"; exit 1 ;;
  esac
  sleep 30
done
