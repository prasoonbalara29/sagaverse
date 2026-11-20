#!/usr/bin/env bash
# Usage: scripts/dlt-replay.sh <topic>     e.g. payment.commands
# Re-sends the dead letters of <topic>.DLT that haven't been replayed yet back to <topic>, exactly once.
#
# Bounded on purpose. A plain "consume the DLT, pipe to the topic" loop never ends if the record still
# fails: it is dead-lettered again, read again, replayed again (95 times in 2 minutes when tried).
# Here a consumer group remembers what was already replayed, and each run takes exactly the backlog it
# found at start. Fix the cause first; replay is safe because consumers are idempotent (ADR-0005).
# Headers (including the kafka_dlt-* exception details) are not copied to the replayed records.
set -euo pipefail
topic="${1:?usage: make dlt-replay TOPIC=<topic>}"
dlt="$topic.DLT"
group="dlt-replay-$topic"
bin=/opt/kafka/bin
kafka() { podman exec -i sagaverse-kafka-1 "$@"; }

end=$(kafka $bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$dlt" | awk -F: '{s += $3} END {print s + 0}')
done_=$(kafka $bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "$group" 2>/dev/null |
  awk -v t="$dlt" '$2 == t && $4 ~ /^[0-9]+$/ {s += $4} END {print s + 0}')
n=$((end - done_))
if ((n <= 0)); then
  echo "nothing to replay on $dlt"
  exit 0
fi
echo "replaying $n dead letter(s) from $dlt to $topic (consumer group $group)"
kafka $bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$dlt" --group "$group" \
  --from-beginning --max-messages "$n" --property print.key=true --property key.separator='|' 2>/dev/null |
  kafka $bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$topic" \
    --property parse.key=true --property key.separator='|'
echo "done"
