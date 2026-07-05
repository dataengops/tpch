#!/usr/bin/env bash
# scripts/ec2/launch.sh — provision the SF400 single-node benchmark box (Polars +
# DuckDB on 256 GB). Same spirit as the EMR launchers: package the code, push it to
# S3, launch the instance with a user-data bootstrap that fetches it, then hand back
# SSH + the next commands. It does NOT stage data or run the benchmark -- those are
# interactive SSH steps so a crash leaves the box (and any partial timings) alive.
#
# Usage (from repo root, after confirming your IP is in the SG):
#     source scripts/ec2/config.env
#     bash scripts/ec2/launch.sh
#
# Terminate when done (you pay by the second):
#     aws ec2 terminate-instances --region "$AWS_REGION" --instance-ids <id>
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
# Allow running without sourcing config.env first.
[[ -n "${INSTANCE_TYPE:-}" ]] || source "${HERE}/config.env"

: "${AWS_REGION:?}"; : "${INSTANCE_TYPE:?}"; : "${AMI_ID:?}"; : "${KEY_NAME:?}"
: "${SG_ID:?}"; : "${SUBNET_ID:?}"; : "${REPO_TGZ_S3:?}"; : "${IAM_INSTANCE_PROFILE:?}"

# ---- 0. sanity: is this box's IP allowed to SSH in? ------------------------
MYIP="$(curl -s https://checkip.amazonaws.com || true)"
if [[ -n "${MYIP}" ]]; then
  if aws ec2 describe-security-groups --region "${AWS_REGION}" --group-ids "${SG_ID}" \
      --query "SecurityGroups[0].IpPermissions[?ToPort==\`22\`].IpRanges[].CidrIp" \
      --output text 2>/dev/null | grep -q "${MYIP}/32"; then
    echo "SSH from ${MYIP}/32 is allowed by ${SG_ID}."
  else
    echo "WARNING: ${MYIP}/32 not found in ${SG_ID} port-22 rules. Add it with:"
    echo "  aws ec2 authorize-security-group-ingress --region ${AWS_REGION} \\"
    echo "    --group-id ${SG_ID} --protocol tcp --port 22 --cidr ${MYIP}/32"
  fi
fi

# ---- 1. package the repo + push to S3 (the box fetches this in user-data) --
echo "Packaging repo -> ${REPO_TGZ_S3}"
TGZ="$(mktemp -t polars-bench-XXXX).tgz"
# git archive = only tracked files (excludes ec2.pem, data, output). Clean + small.
git -C "${REPO_ROOT}" archive --format=tar.gz -o "${TGZ}" HEAD
aws s3 cp "${TGZ}" "${REPO_TGZ_S3}" --region "${AWS_REGION}"
rm -f "${TGZ}"

# ---- 2. build user-data = config prelude + bootstrap.sh --------------------
USERDATA="$(mktemp -t userdata-XXXX).sh"
{
  echo '#!/usr/bin/env bash'
  echo "export AWS_REGION='${AWS_REGION}'"
  echo "export REPO_TGZ_S3='${REPO_TGZ_S3}'"
  echo "export TABLES_DIR='${TABLES_DIR}'"
  echo "export SSH_USER='${SSH_USER}'"
  tail -n +2 "${HERE}/bootstrap.sh"      # drop bootstrap's own shebang
} > "${USERDATA}"

# On Windows/Git-Bash the native aws.exe can't resolve a git-bash "/tmp/..." path
# behind a file:// URI (the prefix blocks msys path conversion). Rewrite to a
# mixed Windows path (C:/Users/...) when cygpath is available; a no-op elsewhere.
UD_URI="file://${USERDATA}"
if command -v cygpath >/dev/null 2>&1; then
  UD_URI="file://$(cygpath -m "${USERDATA}")"
fi

# ---- 3. launch the instance -----------------------------------------------
echo "Launching ${INSTANCE_TYPE} (${AMI_ID}) in ${SUBNET_ID}..."
IID=$(aws ec2 run-instances \
  --region "${AWS_REGION}" \
  --image-id "${AMI_ID}" \
  --instance-type "${INSTANCE_TYPE}" \
  --key-name "${KEY_NAME}" \
  --security-group-ids "${SG_ID}" \
  --subnet-id "${SUBNET_ID}" \
  --iam-instance-profile "Name=${IAM_INSTANCE_PROFILE}" \
  --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":40,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
  --user-data "${UD_URI}" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${NAME_TAG}}]" \
  --query 'Instances[0].InstanceId' --output text)
rm -f "${USERDATA}"
echo "Launched ${IID}. Waiting for it to enter 'running'..."
aws ec2 wait instance-running --region "${AWS_REGION}" --instance-ids "${IID}"

DNS=$(aws ec2 describe-instances --region "${AWS_REGION}" --instance-ids "${IID}" \
  --query 'Reservations[0].Instances[0].PublicDnsName' --output text)

cat <<EOF

================================================================================
Instance ${IID} is up: ${DNS}
Name tag: ${NAME_TAG}   type: ${INSTANCE_TYPE} (256 GiB / local NVMe)

user-data is still bootstrapping (mount NVMe, install deps, fetch repo). Watch it:
  ssh -i ${KEY_PATH} ${SSH_USER}@${DNS} \\
    'cloud-init status --wait; tail -n 20 /var/log/cloud-init-output.log; ls -l /opt/polars-benchmark/.bootstrap.done'

Then (staging -> benchmark), over SSH on the box:
  cd /opt/polars-benchmark
  source scripts/ec2/config.env
  bash scripts/ec2/stage_data.sh        # STAGE_MODE=${STAGE_MODE} (generate|copy)
  bash scripts/ec2/run_bench.sh         # cold single-pass, all 22 q, both engines

TERMINATE WHEN DONE (billed per second):
  aws ec2 terminate-instances --region ${AWS_REGION} --instance-ids ${IID}
================================================================================
EOF
