#!/bin/bash
# Creates every topic up front (broker auto-create is disabled, ADR-0002).
# Keep this list in sync with events/src/main/java/io/sagaverse/events/Topics.java.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
PARTITIONS="${PARTITIONS:-3}"
TOPICS=(
  payment.commands payment.replies inventory.commands inventory.replies
  payment.commands.DLT payment.replies.DLT inventory.commands.DLT inventory.replies.DLT
)

for t in "${TOPICS[@]}"; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$t" --partitions "$PARTITIONS" --replication-factor 1
done

echo "topics now on the broker:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
