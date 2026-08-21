"""Bronze layer — ingest raw events from Kafka into PostgreSQL.

The consumer lands every event as a single row in ``bronze.raw_events``.
Duplicates (same event_id, same kafka offset) are silently ignored via
``ON CONFLICT DO NOTHING``, making the stage idempotent.
"""
from __future__ import annotations

import json
import logging

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from config.settings import settings
from src.db import connect

log = logging.getLogger(__name__)


def _ensure_topic(topic: str, bootstrap: str) -> None:
    ac = AdminClient({"bootstrap.servers": bootstrap})
    metadata = ac.list_topics(timeout=10)
    if topic in metadata.topics:
        return
    fs = ac.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    fs[topic].result()
    log.info("Created topic %s", topic)
    # brief settle
    import time
    time.sleep(1)


def consume_land(topic: str | None = None, bootstrap: str | None = None, group: str | None = None) -> int:
    """Continuously consume and land events until no more for 10 seconds."""
    cfg = settings.kafka
    topic = topic or cfg.topic
    bootstrap = bootstrap or cfg.bootstrap_servers
    group = group or cfg.consumer_group

    _ensure_topic(topic, bootstrap)

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 60000,
    })
    consumer.subscribe([topic])

    landed = 0
    idle_rounds = 0
    try:
        while True:
            msgs = consumer.consume(num_messages=100, timeout=5.0)
            if not msgs:
                idle_rounds += 1
                if idle_rounds >= 3:
                    break
                continue
            idle_rounds = 0

            with connect() as conn:
                with conn.cursor() as cur:
                    for msg in msgs:
                        if msg.error():
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                continue
                            raise KafkaException(msg.error())
                        try:
                            evt = json.loads(msg.value().decode())
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            log.warning("Skipping non-JSON message at offset %s", msg.offset())
                            continue
                        cur.execute(
                            """
                            INSERT INTO bronze.raw_events
                                (event_id, event_type, occurred_at, payload,
                                 kafka_partition, kafka_offset)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (event_id) DO NOTHING
                            """,
                            (
                                evt.get("event_id", f"unknown_{msg.offset()}"),
                                evt.get("event_type", "unknown"),
                                evt.get("occurred_at", "1970-01-01T00:00:00Z"),
                                json.dumps(evt.get("payload", {})),
                                msg.partition(),
                                msg.offset(),
                            ),
                        )
                        landed += 1
                conn.commit()
            consumer.commit(asynchronous=False)
        log.info("Bronze land complete: %s events landed", landed)
    finally:
        consumer.close()
    return landed