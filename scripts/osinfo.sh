#!/bin/bash
# ##############################################################################
# Simple script to grab OS-specific config for Linux boxes.
# Written to investigate nightly test failures in CI-jobs, reporting # CPUs,
# available memory and disk-space where large tests are running from.
#
# Usage: osinfo.sh [0 | 1]  # 1 will print banner lines.
# ##############################################################################

set -euo pipefail
Me=$(basename "$0")

numCPUs=$(grep -c "^processor" /proc/cpuinfo)
cpuModel=$(grep "model name" /proc/cpuinfo | head -1 | cut -f2 -d':')
cpuVendor=$(grep "vendor_id" /proc/cpuinfo | head -1 | cut -f2 -d':')
totalMemGB=$(free -g | grep "^Mem:" | awk '{print $2}')

if [ $# -gt 0 ] && [ "$1" = 1 ]; then
    echo "# =============================================================================="
fi

echo "${Me}:${cpuVendor}, ${numCPUs} CPUs, ${totalMemGB} GB, ${cpuModel}"
uname -a

echo
echo "Disk space: "
df -kh .
echo "Memory available:"
free -h
if [ $# -gt 0 ] && [ "$1" = 1 ]; then
    echo "# =============================================================================="
fi
echo
