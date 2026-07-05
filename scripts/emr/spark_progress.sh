#!/usr/bin/env bash
# Show which TPC-H query the running Spark benchmark is on.
#
# Usage:
#   scripts/emr/spark_progress.sh <primary-dns> <app-id> [rm-private-ip] [pem]
#
# Example:
#   scripts/emr/spark_progress.sh \
#     ec2-52-54-110-37.compute-1.amazonaws.com \
#     application_1783223920333_0003 \
#     172.31.26.78 ec2.pem
#
# It SSHes to the primary and queries the Spark SQL REST API through the YARN
# proxy, then reports how many query executions ("collect") are done and which
# one is RUNNING. NOTE: a run WITH warm-up does 2 collects per query
# (warm-up + timed); a cold run does 1. Pass PER_QUERY=1 or 2 to override the
# default guess of 2.
set -euo pipefail

PRIMARY_DNS="${1:?primary DNS required}"
APP_ID="${2:?spark application id required}"
RM_IP="${3:-172.31.26.78}"
PEM="${4:-ec2.pem}"
PER_QUERY="${PER_QUERY:-2}"

URL="http://${RM_IP}:20888/proxy/${APP_ID}/api/v1/applications/${APP_ID}/sql?length=1000"

# Remote parser: written to a temp file on the primary (no fragile inline
# `python3 -c` indentation). Reads the SQL-API JSON from stdin.
read -r -d '' REMOTE <<REMOTE_EOF || true
set -e
cat > /tmp/spark_progress.py <<'PY'
import sys, json
per = int(sys.argv[1])
d = json.load(sys.stdin)
collects = [x for x in d if "collect" in x.get("description", "")]
running = [x for x in d if x.get("status") == "RUNNING"]
done = [x for x in collects if x.get("status") == "COMPLETED"]
print("collect execs seen : %d" % len(collects))
print("collect execs done : %d" % len(done))
if running:
    r = running[0]
    rc = [x for x in collects if x["id"] <= r["id"]]
    qnum = (len(rc) - 1) // per + 1 if rc else 0
    print("RUNNING exec id    : %d" % r["id"])
    print("current query      : q%d  (of 22)" % qnum)
else:
    qnum = (len(done) - 1) // per + 1 if done else 0
    print("nothing RUNNING; last completed query ~ q%d" % qnum)
PY
curl -s "${URL}" | python3 /tmp/spark_progress.py "${PER_QUERY}"
REMOTE_EOF

ssh -i "${PEM}" -o StrictHostKeyChecking=no -o ConnectTimeout=15 \
  "hadoop@${PRIMARY_DNS}" "${REMOTE}"
