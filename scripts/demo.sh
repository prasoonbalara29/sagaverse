#!/usr/bin/env bash
# Places one order per scenario against the running stack (make up-all) and prints how each ended.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require_stack

run() { # name sku qty amount expected
  local id json got
  id=$(place "demo-$(date +%s)" "$2" "$3" "$4")
  await_final "$id" 30 || true
  json=$(order_json "$id")
  got="$(str "$json" status)/$(str "$json" state)"
  printf '%-28s %-26s %-26s %s\n' "$1" "$5" "$got" "$(str "$json" sagaId)"
  [[ "$got" == "$5" ]]
}

printf '%-28s %-26s %-26s %s\n' SCENARIO EXPECTED ACTUAL SAGA_ID
ok=0
run "happy path"                 SKU-1     2 42.50   "CONFIRMED/COMPLETED"   || ok=1
run "F1 payment declined"        SKU-1     1 5000.00 "CANCELLED/FAILED"      || ok=1
run "F2 out of stock -> refund"  SKU-EMPTY 1 10.00   "CANCELLED/COMPENSATED" || ok=1
[[ $ok == 0 ]] && echo "all scenarios ended as expected" || { echo "MISMATCH, see above" >&2; exit 1; }
