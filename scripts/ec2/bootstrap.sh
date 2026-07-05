#!/usr/bin/env bash
# scripts/ec2/bootstrap.sh — first-boot provisioning for the SF400 single-node box.
#
# This runs as EC2 *user-data* (as root, once, at first boot). launch.sh prepends a
# small `export ...` prelude (REPO_TGZ_S3, AWS_REGION, TABLES_DIR, SSH_USER) and
# feeds the combined script as --user-data, so the vars below arrive from launch.sh.
# It is idempotent enough to also be re-run by hand over SSH if a step fails.
#
# What it does:
#   1. format + mount the local NVMe instance-store at /mnt/nvme
#   2. install python + the slim benchmark deps into /opt/polars-benchmark/.venv
#   3. fetch + extract the repo tarball from S3 to /opt/polars-benchmark
# It deliberately does NOT stage data or run the benchmark -- those are separate,
# SSH-driven steps (stage_data.sh, run_bench.sh) so the run stays interactive.
set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
REPO_TGZ_S3="${REPO_TGZ_S3:?set by launch.sh prelude}"
TABLES_DIR="${TABLES_DIR:-/mnt/nvme/tables}"
SSH_USER="${SSH_USER:-ubuntu}"
APP_DIR="/opt/polars-benchmark"
NVME_MNT="/mnt/nvme"

echo "== [1/3] mount local NVMe instance-store =="
if ! mountpoint -q "${NVME_MNT}"; then
  # On Nitro the instance-store volume reports model "Amazon EC2 NVMe Instance
  # Storage"; the EBS root is "Amazon Elastic Block Store". Pick the instance store.
  DEV=$(lsblk -dn -o NAME,MODEL | awk '/Instance Storage/{print "/dev/"$1; exit}')
  DEV="${DEV:-/dev/nvme1n1}"
  echo "  NVMe device: ${DEV}"
  mkfs.ext4 -F -E nodiscard "${DEV}"
  mkdir -p "${NVME_MNT}"
  mount -o noatime "${DEV}" "${NVME_MNT}"
fi
mkdir -p "${TABLES_DIR}"
chown -R "${SSH_USER}:${SSH_USER}" "${NVME_MNT}"
df -h "${NVME_MNT}"

echo "== [2/3] install python + slim benchmark deps =="
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y python3 python3-venv python3-pip curl unzip
# Ubuntu 24.04 dropped the `awscli` apt package -> install AWS CLI v2 from the
# official bundle (run_bench.sh / stage_data.sh copy timings + data via `aws`).
if ! command -v aws >/dev/null 2>&1; then
  curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
  unzip -q -o /tmp/awscliv2.zip -d /tmp
  /tmp/aws/install --update
fi
python3 -m venv "${APP_DIR}/.venv"
# Only what the Polars + DuckDB harness needs -- not the full requirements.txt
# (which drags in spark/dask/modin/ray). tpchgen-cli powers GENERATE staging.
"${APP_DIR}/.venv/bin/pip" install --upgrade pip
"${APP_DIR}/.venv/bin/pip" install \
  polars==1.41.2 duckdb==1.5.3 pyarrow==24.0.0 linetimer==0.1.5 \
  pydantic==2.13.4 pydantic-settings==2.14.1 python-dotenv==1.2.2 \
  tpchgen-cli==2.0.2

echo "== [3/3] fetch + extract repo =="
mkdir -p "${APP_DIR}"
aws s3 cp "${REPO_TGZ_S3}" /tmp/repo.tgz --region "${REGION}"
tar -xzf /tmp/repo.tgz -C "${APP_DIR}"
# The repo is authored on Windows; tracked shell scripts can carry CRLF endings,
# which bash chokes on ($'\r': command not found, set: pipefail: invalid option).
# Normalize the scripts we source/run here to LF so the pipeline runs unattended.
find "${APP_DIR}/scripts" -type f \( -name '*.sh' -o -name '*.env' \) -print0 \
  | xargs -0 sed -i 's/\r$//'
chown -R "${SSH_USER}:${SSH_USER}" "${APP_DIR}"

touch "${APP_DIR}/.bootstrap.done"
echo "bootstrap complete -> ${APP_DIR} (venv, repo) ; NVMe at ${NVME_MNT}"
