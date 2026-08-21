"""E-commerce event producer — simulates order stream and publishes to Kafka.

Events are generated programmatically so the pipeline is self-contained and
requires no external data source.  A small percentage of events are deliberately
corrupted (null fields, negative prices) to exercise the DQ layer.
"""
from __future__ import annotations

import json
import logging
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

from config.settings import settings

log = logging.getLogger(__name__)

# ── helpers ────────────────────────────────────────────────────────────────

PRODUCTS = [
    {"product_id": "PROD-001", "name": "Wireless Mouse",       "category": "Electronics", "brand": "TechGear", "price": 25.99},
    {"product_id": "PROD-002", "name": "USB-C Hub",            "category": "Electronics", "brand": "ConnectPro", "price": 34.50},
    {"product_id": "PROD-003", "name": "Mechanical Keyboard",  "category": "Electronics", "brand": "KeyCraft",  "price": 89.00},
    {"product_id": "PROD-004", "name": "Standing Desk Mat",    "category": "Office",     "brand": "ErgoLife", "price": 42.00},
    {"product_id": "PROD-005", "name": "Noise-Cancelling Earbuds", "category": "Audio", "brand": "SoundPro",  "price": 129.99},
    {"product_id": "PROD-006", "name": "Laptop Stand",         "category": "Office",     "brand": "ErgoLife", "price": 55.00},
    {"product_id": "PROD-007", "name": "Webcam 1080p",         "category": "Electronics", "brand": "ViewMax",  "price": 67.99},
    {"product_id": "PROD-008", "name": "Desk Lamp LED",        "category": "Office",     "brand": "Lumi",     "price": 39.90},
    {"product_id": "PROD-009", "name": "Monitor Arm",          "category": "Office",     "brand": "ErgoLife", "price": 79.00},
    {"product_id": "PROD-010", "name": "Cable Management Kit", "category": "Accessories", "brand": "NeatDesk", "price": 14.99},
    {"product_id": "PROD-011", "name": "Portable SSD 1TB",     "category": "Storage",    "brand": "DataBank", "price": 109.99},
    {"product_id": "PROD-012", "name": "Bluetooth Speaker",    "category": "Audio",      "brand": "SoundPro", "price": 45.00},
]

CITIES = ["Jakarta", "Bandung", "Surabaya", "Yogyakarta", "Bali", "Singapore", "Kuala Lumpur", "Bangkok"]
TIERS  = ["standard", "plus", "premium"]
METHODS = ["credit_card", "bank_transfer", "e_wallet", "cod"]
NAMES = [
    "Aisha Rahman", "Budi Santoso", "Citra Dewi", "Dimas Ananda",
    "Eka Putri", "Farhan Hakim", "Gita Permata", "Hendra Wijaya",
    "Indah Lestari", "Joko Prasetyo", "Kartika Sari", "Lukman Nulhakim",
]


def _ts(base: datetime, offset_hours: float) -> str:
    return (base + timedelta(hours=offset_hours)).isoformat()


def _event_id() -> str:
    return f"evt_{uuid.uuid4().hex[:12]}"


# ── event generators ───────────────────────────────────────────────────────

def _gen_customer_created(base: datetime, offset: float, seq: int, run: str) -> dict:
    cid = f"CUST-{run}-{seq:04d}"
    return {
        "event_id": _event_id(),
        "event_type": "customer_created",
        "occurred_at": _ts(base, offset),
        "payload": {
            "customer_id": cid,
            "name": random.choice(NAMES),
            "email": f"{cid.lower()}@example.com",
            "city": random.choice(CITIES),
            "tier": random.choice(TIERS),
        },
    }


def _gen_customer_updated(existing: dict, base: datetime, offset: float) -> dict:
    return {
        "event_id": _event_id(),
        "event_type": "customer_updated",
        "occurred_at": _ts(base, offset),
        "payload": {
            "customer_id": existing["payload"]["customer_id"],
            "name": random.choice(NAMES),
            "email": existing["payload"]["email"],
            "city": random.choice(CITIES),
            "tier": random.choice(TIERS),
            "changes": ["city", "tier"] if random.random() < 0.5 else ["name"],
        },
    }


def _gen_order_created(customers: list[dict], products: list[dict],
                       base: datetime, offset: float, oid: int, run: str) -> dict:
    cust = random.choice(customers)
    num_items = random.randint(1, 4)
    chosen = random.sample(products, min(num_items, len(products)))
    total = 0
    items = []
    for idx, p in enumerate(chosen):
        qty = random.randint(1, 3)
        price = p["price"]
        total += round(price * qty, 2)
        items.append({
            "order_item_id": f"OI-{run}-{oid:04d}-{idx + 1}",
            "product_id": p["product_id"],
            "product_name": p["name"],
            "category": p["category"],
            "brand": p["brand"],
            "quantity": qty,
            "unit_price": price,
        })
    return {
        "event_id": _event_id(),
        "event_type": "order_created",
        "occurred_at": _ts(base, offset),
        "payload": {
            "order_id": f"ORD-{run}-{oid:04d}",
            "customer_id": cust["payload"]["customer_id"],
            "ordered_at": _ts(base, offset),
            "status": "pending",
            "currency": "USD",
            "items": items,
            "total": total,
        },
    }


def _gen_payment(order_evt: dict, base: datetime, offset: float) -> dict:
    return {
        "event_id": _event_id(),
        "event_type": "payment_received",
        "occurred_at": _ts(base, offset),
        "payload": {
            "payment_id": f"PAY-{order_evt['payload']['order_id']}",
            "order_id": order_evt["payload"]["order_id"],
            "amount": order_evt["payload"]["total"],
            "method": random.choice(METHODS),
            "status": random.choice(["completed", "completed", "completed", "pending"]),
        },
    }


def _maybe_corrupt(evt: dict, corruption_rate: float) -> dict:
    """Flip a few fields to None to simulate bad data."""
    if random.random() >= corruption_rate:
        return evt
    field = random.choice(["event_type", "occurred_at", "payload"])
    # Corrupt the event_type to test DQ
    if field == "event_type":
        evt["event_type"] = "unknown_event_type"
    elif field == "occurred_at":
        evt["occurred_at"] = None
    elif field == "payload":
        # null some payload fields
        if "payload" in evt and isinstance(evt["payload"], dict):
            for k in random.sample(list(evt["payload"].keys() | {"product_id": None, "quantity": None}),
                                   min(1, max(1, len(evt["payload"])))):
                if k in evt["payload"]:
                    evt["payload"][k] = None
    return evt


# ── main generation ────────────────────────────────────────────────────────

def generate_events(
    count: int = 500,
    corruption_rate: float = 0.02,
    duplicate_rate: float = 0.03,
    stream_span_hours: int = 72,
) -> list[dict]:
    """Return a list of serialisable event dicts ready for Kafka.

    A short random ``run`` token is embedded in every business ID (CUST-…,
    ORD-…, OI-…, PAY-…) so multiple batches never collide — a realistic
    property of a never-ending event stream.
    """
    base = datetime.now(timezone.utc) - timedelta(hours=stream_span_hours)
    step = stream_span_hours / count
    run = uuid.uuid4().hex[:6].upper()
    customers: list[dict] = []
    events: list[dict] = []
    order_id_counter = 0

    for i in range(count):
        offset = i * step
        # Every 10 events: add a new customer
        if i % 10 == 0:
            evt = _gen_customer_created(base, offset, len(customers) + 1, run)
            customers.append(evt)
            events.append(evt)
        # Every 10 events: update a random customer (if we have enough)
        if i % 10 == 7 and len(customers) >= 3:
            target = random.choice(customers)
            events.append(_gen_customer_updated(target, base, offset + 0.01))
        # Every 5 events: create an order
        if i % 5 == 0 and customers:
            order_id_counter += 1
            order_evt = _gen_order_created(customers, PRODUCTS, base, offset, order_id_counter, run)
            events.append(order_evt)
            # Payment follows shortly after
            events.append(_gen_payment(order_evt, base, offset + 0.02))

    # Apply corruption
    final = [_maybe_corrupt(e, corruption_rate) for e in events]

    # Inject duplicates (same event_id re-appended)
    injected = 0
    for e in list(final):
        if injected < int(count * duplicate_rate):
            if random.random() < duplicate_rate:
                final.append(e)
                injected += 1

    random.shuffle(final)
    return final


def produce(events: list[dict], topic: str, bootstrap: str) -> int:
    """Publish events to Kafka. Returns count of successfully delivered."""
    conf = {"bootstrap.servers": bootstrap, "client.id": "ecom-producer"}
    producer = Producer(conf)
    delivered = 0

    def _acked(err, msg):
        nonlocal delivered
        if err:
            log.error("Kafka delivery failed: %s", err)
        else:
            delivered += 1

    for evt in events:
        producer.produce(topic, json.dumps(evt, default=str).encode(), key=evt.get("event_id", "").encode(), callback=_acked)
        producer.poll(0)  # non-blocking

    producer.flush(30)
    return delivered