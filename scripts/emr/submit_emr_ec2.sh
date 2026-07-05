#!/usr/bin/env bash
# Submit the PDS/TPC-H PySpark benchmark to a transient EMR-on-EC2 cluster.
#
# Same pattern as conv/aws/emr/submit_emr_ec2.sh: a real cluster you pick
# instances for (m6a.4xlarge core nodes: 16 vCPU / 64 GiB, 3 executors each),
# with the benchmark attached as a single spark-submit step. The cluster
# auto-terminates when the step finishes, so you pay only for the run.
#
# The job (scripts/emr/emr_tpch_benchmark.py) reads the 8 TPC-H tables as
# Parquet from S3 and times all 22 queries.
#
# Prereqs:
#   - AWS CLI v2 configured with rights to EMR + EC2 + S3 + IAM.
#   - The Parquet tables in S3, in one of two layouts (Spark reads either):
#       flat single-file  s3://$BUCKET/$DATA_PREFIX/{table}.parquet
#                         (what `make data-tables` + `aws s3 sync` produce; SF100)
#       per-table dir     s3://$BUCKET/$DATA_PREFIX/{table}/{table}.parquet
#                         (the canonical SF400 layout shared with the Trino run,
#                          created once by trino_run.sh step 1's idempotent reorg)
#     For the flat layout keep TABLE_SUFFIX=.parquet; for the shared per-table
#     layout point DATA_PREFIX at the .../trino prefix and set TABLE_SUFFIX=''.
#     See setup_wsl.md.
#   - A VPC subnet whose route table reaches S3 (S3 gateway endpoint or NAT).
#   - Default EMR roles (this script creates them with `aws emr create-default-roles`).
#
# SF400 (shared trino/ layout, read from S3) example:
#   RELEASE=emr-7.13.0 REGION=us-east-1 BUCKET=your_s3_bucket \
#   SUBNET_ID=subnet-xxxx DATA_PREFIX=scale-400.0/trino TABLE_SUFFIX='' \
#   PRIMARY_TYPE=m7i.xlarge CORE_TYPE=r6id.8xlarge CORE_COUNT=2 \
#   bash scripts/emr/submit_emr_ec2.sh
#
# SF400 HDFS mode (3x r6id.4xlarge core, s3-dist-cp -> local NVMe) example --
# set HDFS_BASE to stage the per-table parquet into HDFS and scan local disk.
# AUTO_TERMINATE=false keeps the cluster up on a crash so timings are recoverable:
#   RELEASE=emr-7.13.0 REGION=us-east-1 BUCKET=your_s3_bucket \
#   SUBNET_ID=subnet-xxxx DATA_PREFIX=scale-400.0/trino \
#   HDFS_BASE=hdfs:///tpch/sf400 SPARK_CONFIG=emr-spark-config-hdfs-3node.json \
#   PRIMARY_TYPE=m7i.xlarge CORE_TYPE=r6id.4xlarge CORE_COUNT=3 \
#   AUTO_TERMINATE=false \
#   bash scripts/emr/submit_emr_ec2.sh
set -euo pipefail

# ---- config (edit these) ---------------------------------------------------
REGION="${REGION:-us-east-1}"
BUCKET="${BUCKET:-your-bucket}"                        # bucket holding the tables
SUBNET_ID="${SUBNET_ID:-subnet-xxxxxxxxxxxxxxxxx}"    # must reach S3 (endpoint or NAT)
RELEASE="${RELEASE:-emr-7.5.0}"

# Where the Parquet tables live and the job's table naming. For the shared SF400
# per-table layout use DATA_PREFIX=scale-400.0/trino with TABLE_SUFFIX='' (reads
# .../trino/{table}/ as a dir); the flat defaults below suit SF100.
DATA_PREFIX="${DATA_PREFIX:-scale-100.0}"             # folder under the bucket
TABLE_SUFFIX="${TABLE_SUFFIX:-.parquet}"             # '' for per-table dir layout
QUERIES="${QUERIES:-}"                                # e.g. "1,6,9"; empty = all 22
ITERATIONS="${ITERATIONS:-1}"

# Cluster shape. CORE_COUNT=1 ~= a single strong box; raise it (or add task
# nodes) to scale. Each m6a.4xlarge node = 3 executors (5 cores / 16g each).
# If you change node counts, bump spark.dynamicAllocation.maxExecutors in
# emr-spark-config.json to 3 x (core + task nodes).
PRIMARY_TYPE="${PRIMARY_TYPE:-m6a.xlarge}"            # coordinator only -> small/cheap
CORE_TYPE="${CORE_TYPE:-m6a.4xlarge}"                # the throughput unit
CORE_COUNT="${CORE_COUNT:-1}"
TASK_TYPE="${TASK_TYPE:-m6a.4xlarge}"                # extra compute; cheap as spot
TASK_COUNT="${TASK_COUNT:-0}"                        # 0 = none

# HDFS mode: when HDFS_BASE is set (e.g. hdfs:///tpch/sf400) a staging step
# s3-dist-cp's the per-table parquet from s3://$BUCKET/$DATA_PREFIX into cluster
# HDFS (local NVMe) and replicates every block to all datanodes; Spark then scans
# local disk instead of S3. DATA_PREFIX must point at the per-table layout
# (e.g. scale-400.0/trino) and TABLE_SUFFIX is forced to '' for the dir layout.
# Empty = read straight from S3 (original mode).
HDFS_BASE="${HDFS_BASE:-}"

# Optional EC2 key pair for SSH into the primary (e.g. to read YARN logs on a
# failed run). Leave empty for a headless batch run; the SG must also allow TCP 22.
KEY_NAME="${KEY_NAME:-}"

# AUTO_TERMINATE=false keeps the cluster up after the step and lets a step FAIL
# WITHOUT terminating the cluster (ActionOnFailure=CONTINUE) -- so a mid-run crash
# leaves the box + logs alive and timings recoverable. Remember to
# `aws emr terminate-clusters` yourself afterwards. Default true = original
# auto-terminate behaviour (good for the quick SF100 runs).
AUTO_TERMINATE="${AUTO_TERMINATE:-true}"

DATA_PATH="s3://${BUCKET}/${DATA_PREFIX}"
if [[ -n "${HDFS_BASE}" ]]; then
  DATA_PATH="${HDFS_BASE}"
  TABLE_SUFFIX=""
fi
CODE_S3="s3://${BUCKET}/code/emr_tpch_benchmark.py"
STAGE_S3="s3://${BUCKET}/code/spark_hdfs_stage.sh"
TIMINGS_S3="s3://${BUCKET}/output/emr_timings/run-$(date +%Y%m%d-%H%M%S)"
LOG_S3="s3://${BUCKET}/logs/emr/"

HERE="$(cd "$(dirname "$0")" && pwd)"
JOB_SRC="${HERE}/emr_tpch_benchmark.py"
STAGE_SRC="${HERE}/spark_hdfs_stage.sh"
# Override with SPARK_CONFIG=emr-spark-config-hdfs-3node.json for the r6id run.
CONFIG_JSON="${HERE}/${SPARK_CONFIG:-emr-spark-config.json}"
# On Git Bash / MSYS the native aws.exe cannot read a file:///c/... URI; give it a
# Windows path (C:/...). Elsewhere the plain path works as-is.
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

# Interactive mode (AUTO_TERMINATE=false): step failure leaves the cluster up.
if [[ "${AUTO_TERMINATE}" == "true" ]]; then
  STEP_ON_FAILURE="TERMINATE_CLUSTER"; TERMINATE_FLAG=(--auto-terminate)
else
  STEP_ON_FAILURE="CONTINUE";          TERMINATE_FLAG=(--no-auto-terminate)
fi

# ---- 1. (one-time) default EMR service + EC2 instance-profile roles ---------
echo "Ensuring default EMR roles exist..."
aws emr create-default-roles --region "${REGION}" >/dev/null
# NOTE: the EMR_EC2_DefaultRole instance profile must read $BUCKET (and write the
# timings/logs prefixes). The managed AmazonElasticMapReduceforEC2Role policy
# covers most buckets; attach a bucket-scoped policy if you locked S3 down.

# ---- 2. upload the job script (+ HDFS staging script) ----------------------
echo "Uploading job script to ${CODE_S3}"
aws s3 cp "${JOB_SRC}" "${CODE_S3}" --region "${REGION}"
if [[ -n "${HDFS_BASE}" ]]; then
  echo "Uploading HDFS staging script to ${STAGE_S3}"
  aws s3 cp "${STAGE_SRC}" "${STAGE_S3}" --region "${REGION}"
fi

# ---- 3. build the step(s) --------------------------------------------------
# Run spark-submit via `bash -c` (not raw command-runner args): EMR drops an empty
# JSON arg element, so a literal --table-suffix "" (needed for the per-table dir
# layout) vanishes and argparse dies with exit 2. In a shell the '' survives as a
# real empty argv element.
SPARK_CMD="spark-submit --deploy-mode cluster ${CODE_S3}"
SPARK_CMD+=" --data-path ${DATA_PATH} --table-suffix '${TABLE_SUFFIX}'"
SPARK_CMD+=" --iterations ${ITERATIONS} --timings-out ${TIMINGS_S3}"
if [[ -n "${QUERIES}" ]]; then
  SPARK_CMD+=" --queries ${QUERIES}"
fi

SPARK_STEP="{
      \"Type\":\"CUSTOM_JAR\",
      \"Name\":\"pds-tpch-benchmark\",
      \"ActionOnFailure\":\"${STEP_ON_FAILURE}\",
      \"Jar\":\"command-runner.jar\",
      \"Args\":[\"bash\",\"-c\",\"${SPARK_CMD}\"]
    }"

STEPS="[
    ${SPARK_STEP}
  ]"
if [[ -n "${HDFS_BASE}" ]]; then
  # Staging step runs first: fetch spark_hdfs_stage.sh and s3-dist-cp S3 -> HDFS.
  STAGE_CMD="set -e; aws s3 cp ${STAGE_S3} /tmp/stage.sh --region ${REGION}; bash /tmp/stage.sh '${BUCKET}' '${DATA_PREFIX}' '${HDFS_BASE}' '${REGION}'"
  STAGE_STEP="{
      \"Type\":\"CUSTOM_JAR\",
      \"Name\":\"stage-s3-to-hdfs\",
      \"ActionOnFailure\":\"${STEP_ON_FAILURE}\",
      \"Jar\":\"command-runner.jar\",
      \"Args\":[\"bash\",\"-c\",\"${STAGE_CMD}\"]
    }"
  STEPS="[
    ${STAGE_STEP},
    ${SPARK_STEP}
  ]"
fi

# ---- 4. build the instance groups (append TASK only when TASK_COUNT > 0) ----
INSTANCE_GROUPS="[
    {\"Name\":\"Primary\",\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"${PRIMARY_TYPE}\",\"InstanceCount\":1},
    {\"Name\":\"Core\",\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${CORE_TYPE}\",\"InstanceCount\":${CORE_COUNT}}"
if [[ "${TASK_COUNT}" -gt 0 ]]; then
  INSTANCE_GROUPS+=",
    {\"Name\":\"Task\",\"InstanceGroupType\":\"TASK\",\"InstanceType\":\"${TASK_TYPE}\",\"InstanceCount\":${TASK_COUNT}}"
fi
INSTANCE_GROUPS+="
  ]"

# ---- 5. launch the transient cluster with the step(s) attached -------------
# HDFS mode needs the Hadoop application (HDFS daemons + s3-dist-cp); EMR pulls
# it in with Spark anyway, but request it explicitly so the staging step is safe.
APPS="Name=Spark"
if [[ -n "${HDFS_BASE}" ]]; then
  APPS="Name=Spark Name=Hadoop"
fi
echo "Launching cluster: 1x ${PRIMARY_TYPE} primary, ${CORE_COUNT}x ${CORE_TYPE} core, ${TASK_COUNT}x ${TASK_TYPE} task..."
CLUSTER_ID=$(aws emr create-cluster \
  --name pds-tpch-bench \
  --release-label "${RELEASE}" \
  --applications ${APPS} \
  --region "${REGION}" \
  --log-uri "${LOG_S3}" \
  --use-default-roles \
  --ec2-attributes "${EC2_ATTRS}" \
  --instance-groups "${INSTANCE_GROUPS}" \
  --configurations "${CONFIG_URI}" \
  --steps "${STEPS}" \
  "${TERMINATE_FLAG[@]}" \
  --query ClusterId --output text)

echo "Launched cluster ${CLUSTER_ID}. Timings will be written under ${TIMINGS_S3}"

# ---- 6. wait for the run to finish -----------------------------------------
# Interactive mode: watch the LAST step (the spark-submit) and leave the cluster
# up so a crash is debuggable and timings recoverable.
if [[ "${AUTO_TERMINATE}" != "true" ]]; then
  echo "Interactive cluster (no auto-terminate). Watching the benchmark step; cluster stays up after."
  echo "  SSH: ssh -i ${KEY_NAME:-<key>}.pem hadoop@<primary-dns>"
  echo "  Terminate when done: aws emr terminate-clusters --region ${REGION} --cluster-ids ${CLUSTER_ID}"
  while true; do
    SSTATE=$(aws emr list-steps --region "${REGION}" --cluster-id "${CLUSTER_ID}" \
      --query 'Steps[0].Status.State' --output text)   # Steps[0] = most recent (spark-submit)
    echo "  $(date +%T)  step=${SSTATE}"
    case "${SSTATE}" in
      COMPLETED) echo "Step done. Timings: ${TIMINGS_S3} (cluster ${CLUSTER_ID} still UP -- terminate it)"; break ;;
      FAILED|CANCELLED) echo "Step ${SSTATE}. Cluster ${CLUSTER_ID} still UP for debugging. Logs: ${LOG_S3}"; break ;;
    esac
    sleep 30
  done
  exit 0
fi

echo "It auto-terminates when the step finishes. Waiting for terminal state (Ctrl-C to stop watching)..."
while true; do
  STATE=$(aws emr describe-cluster --region "${REGION}" --cluster-id "${CLUSTER_ID}" \
    --query 'Cluster.Status.State' --output text)
  REASON=$(aws emr describe-cluster --region "${REGION}" --cluster-id "${CLUSTER_ID}" \
    --query 'Cluster.Status.StateChangeReason.Message' --output text)
  echo "  $(date +%T)  ${STATE}  ${REASON}"
  case "${STATE}" in
    TERMINATED)             echo "Done. Timings: ${TIMINGS_S3}"; break ;;
    TERMINATED_WITH_ERRORS) echo "Cluster failed. Logs: ${LOG_S3}"; exit 1 ;;
  esac
  sleep 30
done
