#!/usr/bin/env bash
# EMR bootstrap action: pre-create the Trino spill directory.
#
# Runs after storage is mounted but BEFORE Trino installs/starts, so the
# coordinator can create/open spiller-spill-path on first boot. Setting the
# path only via the trino-config classification is NOT enough: EMR writes
# config.properties but never creates/chowns the dir, so Trino (user `trino`)
# cannot mkdir under the root-owned instance-store mount and crash-loops with
# "could not create spill path".
#
# The trino user does not exist yet at bootstrap time, so we make the dir
# sticky world-writable (1777, like /tmp) instead of chowning it. `mkdir -p`
# guarantees the path is creatable whether or not /mnt1 is a real NVMe mount:
# if it is, spill lands on the big instance store; if not, Trino still starts.
set -eu
SPILL="${1:-/mnt1/trino-spill}"
sudo mkdir -p "${SPILL}"
sudo chmod 1777 "${SPILL}"
echo "prepared Trino spill dir: ${SPILL}"
