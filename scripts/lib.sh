# Shared helpers for demo.sh and chaos-inventory.sh. Needs only bash, curl and sed.
ORDER_URL="${ORDER_URL:-http://localhost:8081}"

# place <customer> <sku> <qty> <amount>  -> prints the orderId
place() {
  curl -fsS -X POST "$ORDER_URL/orders" -H 'Content-Type: application/json' \
    -d "{\"customerId\":\"$1\",\"sku\":\"$2\",\"quantity\":$3,\"amount\":$4}" |
    sed -E 's/.*"orderId":"([^"]+)".*/\1/'
}

order_json() { curl -fsS "$ORDER_URL/orders/$1"; }

# str <json> <key>   first string value of key;   num <json> <key>   first numeric value
str() { sed -nE "s/.*\"$2\":\"([^\"]*)\".*/\1/p" <<<"$1"; }
num() { sed -nE "s/.*\"$2\":([0-9]+).*/\1/p" <<<"$1"; }

# await_final <orderId> <seconds>  -> waits until the order leaves PENDING
await_final() {
  local i
  for ((i = 0; i < $2; i++)); do
    [[ "$(str "$(order_json "$1")" status)" != "PENDING" ]] && return 0
    sleep 1
  done
  return 1
}

# transitions <json>  -> one "from -> to (trigger)" per line
transitions() {
  grep -oE '"from":(null|"[A-Z_]+"),"to":"[A-Z_]+","trigger":"[^"]+"' <<<"$1" |
    sed -E 's/"from":"?([A-Z_]+|null)"?,"to":"([A-Z_]+)","trigger":"([^"]+)"/    \1 -> \2  (\3)/'
}

require_stack() {
  if ! curl -fs "$ORDER_URL/actuator/health" >/dev/null; then
    echo "order-service is not reachable at $ORDER_URL. Start the stack first: make up-all" >&2
    exit 1
  fi
}
