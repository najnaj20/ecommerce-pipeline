#!/usr/bin/env bash
# One-command local setup for Ubuntu/Debian: installs Java + PostgreSQL,
# downloads Kafka (KRaft single-node), creates the database, and starts
# everything. Run from the repo root:
#     bash scripts/setup_local.sh
set -euo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-4.3.1}"
KAFKA_SCALA="${KAFKA_SCALA:-2.13}"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}}"

echo "==> [1/5] Installing Java 17 + PostgreSQL"
if ! command -v java >/dev/null; then
  sudo apt-get update -qq
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq openjdk-17-jre-headless
fi
if ! command -v psql >/dev/null; then
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-16
  sudo systemctl enable --now postgresql
fi

echo "==> [2/5] Downloading Kafka ${KAFKA_VERSION} (KRaft single node)"
if [ ! -d "$KAFKA_HOME" ]; then
  TMP_TGZ="$(mktemp --suffix=.tgz)"
  curl -sSL -o "$TMP_TGZ" "https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz"
  sudo mkdir -p /opt
  sudo tar -xzf "$TMP_TGZ" -C /opt
  rm -f "$TMP_TGZ"
fi
sudo chown -R "$(whoami)" "$KAFKA_HOME"

echo "==> [3/5] Creating PostgreSQL role + database"
sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='ecom'" | grep -q 1 \
  || sudo -u postgres psql -c "CREATE USER ecom WITH PASSWORD 'ecom_dev_only';"
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='ecommerce'" | grep -q 1 \
  || sudo -u postgres createdb -O ecom ecommerce

echo "==> [4/5] Formatting Kafka storage (KRaft)"
CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
if ! "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$KAFKA_HOME/config/server.properties" --standalone 2>&1 | grep -q "already formatted"; then
  echo "    (storage formatted)"
fi

echo "==> [5/5] Starting Kafka (logs: /tmp/kafka.log)"
if ! "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
  KAFKA_HEAP_OPTS="-Xmx512M -Xms256M" nohup "$KAFKA_HOME/bin/kafka-server-start.sh" \
    "$KAFKA_HOME/config/server.properties" > /tmp/kafka.log 2>&1 &
  for i in $(seq 1 60); do
    if "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
fi

echo ""
echo "✅ Setup complete. Next:"
echo "   python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
echo "   .venv/bin/python -m src.main all"
