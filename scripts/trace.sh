#!/usr/bin/env bash
# Usage: scripts/trace.sh <sagaId>
# Merges every log line that mentions the saga from all three service containers, in time order.
# Works with the containerized stack (make up-all). In dev mode, grep each terminal's output instead.
set -euo pipefail
saga="${1:?usage: make trace SAGA=<sagaId>}"
for svc in order payment inventory; do
  podman logs "sagaverse-$svc-service-1" 2>&1 | grep -F "$saga" | grep '^{"@timestamp' || true
done |
  sort |
  sed -E 's/^\{"@timestamp":"[0-9-]+T([0-9:.]{12})[^"]*".*"logger":"([^"]*)".*"service":\{"name":"([^"]*)".*"message":"([^"]*)".*/\1  \3  \2  \4/' |
  sed -E 's/  io\.sagaverse\.[a-z]+\.([a-z]+\.)*/  /' |
  awk '{ printf "%s  %-18s %-22s", $1, $2, $3; $1=$2=$3=""; sub(/^ +/, ""); print " " $0 }'
