#!/usr/bin/env bash
# Chaos demo: kill inventory-service (SIGKILL), place an order, watch the saga wait while the
# sweeper resends ReserveStock, restart inventory, watch the saga complete. Needs make up-all.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require_stack
INV=sagaverse-inventory-service-1
step() { printf '\n==> %s\n' "$*"; }

step "1. Killing $INV with SIGKILL (no graceful shutdown)"
podman kill "$INV" >/dev/null
podman ps -a --filter "name=$INV" --format '    {{.Names}}: {{.Status}}'

step "2. Placing an order while inventory is dead"
id=$(place "chaos-$(date +%s)" SKU-1 1 15.00)
echo "    orderId=$id"

step "3. Saga is stuck waiting for inventory; the sweeper resends ReserveStock every step timeout (10s)"
for i in 1 2 3 4 5 6; do
  sleep 4
  json=$(order_json "$id")
  printf '    t+%2ss  order=%-9s saga=%-18s attempts=%s deadline=%s\n' $((i * 4)) \
    "$(str "$json" status)" "$(str "$json" state)" "$(num "$json" stepAttempts)" "$(str "$json" deadlineAt)"
done

step "4. Restarting $INV"
podman start "$INV" >/dev/null
for _ in $(seq 1 60); do
  curl -fs http://localhost:8083/actuator/health/readiness >/dev/null && break
  sleep 1
done
echo "    inventory-service is ready"

step "5. Waiting for the saga to finish"
await_final "$id" 60 || true
json=$(order_json "$id")
echo "    order=$(str "$json" status) saga=$(str "$json" state) sagaId=$(str "$json" sagaId)"
echo "    history:"
transitions "$json"
[[ "$(str "$json" status)/$(str "$json" state)" == "CONFIRMED/COMPLETED" ]] || { echo "saga did not complete" >&2; exit 1; }
echo
echo "Trace it across services: podman logs sagaverse-<svc>-service-1 | grep $(str "$json" sagaId)"
