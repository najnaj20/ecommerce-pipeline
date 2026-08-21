"""Silver layer — clean, type-normalise, and deduplicate bronze events.

Transform logic is pure Python so it can be unit-tested without a database.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from config.settings import settings
from src.db import connect, get_watermark, set_watermark

log = logging.getLogger(__name__)

_EVENT_TYPES = {"customer_created", "customer_updated", "order_created", "payment_received"}


def parse_bronze_rows(rows: list[dict]) -> dict[str, list[dict]]:
    """Parse raw bronze rows into typed silver tables.

    Returns a dict of ``{table_name: [row_dict, ...]}`` with cleaned data.
    """
    customers: dict[str, list[dict]] = {}  # key by (customer_id, version)
    orders: dict[str, dict] = {}
    order_items: dict[str, dict] = {}
    payments: dict[str, dict] = {}
    customervers: dict[str, int] = {}  # track latest version per customer

    for row in rows:
        etype = row.get("event_type", "")
        if etype not in _EVENT_TYPES:
            continue  # skip unknown/corrupted types

        payload = row.get("payload", {})
        occurred_at = row.get("occurred_at", "")
        if not occurred_at:
            continue  # skip events with null timestamp

        if etype in ("customer_created", "customer_updated"):
            cid = payload.get("customer_id")
            name = payload.get("name")
            if not cid or not name:
                continue
            # Validate required fields
            if not isinstance(name, str) or not isinstance(cid, str):
                continue
            customervers[cid] = customervers.get(cid, 0) + 1
            customers[f"{cid}_{customervers[cid]}"] = {
                "customer_id": cid,
                "version": customervers[cid],
                "name": name,
                "email": str(payload.get("email", "")),
                "city": str(payload.get("city", "")),
                "tier": str(payload.get("tier", "standard")),
                "occurred_at": occurred_at,
            }

        elif etype == "order_created":
            order_id = payload.get("order_id")
            customer_id = payload.get("customer_id")
            status = payload.get("status")
            if not order_id or not customer_id or not status:
                continue  # skip orders with corrupted required fields
            orders[order_id] = {
                "order_id": order_id,
                "customer_id": customer_id,
                "ordered_at": payload.get("ordered_at", occurred_at),
                "status": status,
                "currency": payload.get("currency", "USD") or "USD",
                "occurred_at": occurred_at,
            }
            items = payload.get("items") or []
            for item in items:
                oi_id = item.get("order_item_id") if isinstance(item, dict) else None
                if not oi_id:
                    continue
                qty = item.get("quantity")
                price = item.get("unit_price")
                if qty is None or price is None or (isinstance(qty, (int, float)) and qty < 0):
                    # negative or null quantity = invalid
                    continue
                order_items[oi_id] = {
                    "order_item_id": oi_id,
                    "order_id": order_id,
                    "product_id": item.get("product_id", "UNKNOWN"),
                    "product_name": str(item.get("product_name", "")),
                    "category": str(item.get("category", "")),
                    "brand": str(item.get("brand", "")),
                    "quantity": int(qty) if isinstance(qty, (int, float)) else 0,
                    "unit_price": round(float(price), 2) if price else 0.0,
                    "occurred_at": occurred_at,
                }

        elif etype == "payment_received":
            pay_id = payload.get("payment_id")
            order_id = payload.get("order_id")
            amount = payload.get("amount")
            if not pay_id or not order_id or amount is None:
                continue
            payments[pay_id] = {
                "payment_id": pay_id,
                "order_id": order_id,
                "amount": round(float(amount), 2),
                "method": str(payload.get("method", "unknown")),
                "status": str(payload.get("status", "pending")),
                "occurred_at": occurred_at,
            }

    return {
        "customers": list(customers.values()),
        "orders": list(orders.values()),
        "order_items": list(order_items.values()),
        "payments": list(payments.values()),
    }


def load_silver(tables: dict[str, list[dict]]) -> dict[str, int]:
    """Insert parsed silver rows into PostgreSQL. Returns row counts."""
    counts: dict[str, int] = {}
    with connect() as conn:
        with conn.cursor() as cur:
            # orders
            for row in tables.get("orders", []):
                cur.execute(
                    """
                    INSERT INTO silver.orders (order_id, customer_id, ordered_at, status, currency, occurred_at)
                    VALUES (%(order_id)s, %(customer_id)s, %(ordered_at)s, %(status)s, %(currency)s, %(occurred_at)s)
                    ON CONFLICT (order_id) DO UPDATE SET status = EXCLUDED.status
                    """,
                    row,
                )
            counts["orders"] = len(tables.get("orders", []))

            # order items
            for row in tables.get("order_items", []):
                cur.execute(
                    """
                    INSERT INTO silver.order_items (order_item_id, order_id, product_id, product_name, category, brand, quantity, unit_price, occurred_at)
                    VALUES (%(order_item_id)s, %(order_id)s, %(product_id)s, %(product_name)s, %(category)s, %(brand)s, %(quantity)s, %(unit_price)s, %(occurred_at)s)
                    ON CONFLICT (order_item_id) DO NOTHING
                    """,
                    row,
                )
            counts["order_items"] = len(tables.get("order_items", []))

            # customers (versioned)
            for row in tables.get("customers", []):
                cur.execute(
                    """
                    INSERT INTO silver.customers (customer_id, version, name, email, city, tier, occurred_at)
                    VALUES (%(customer_id)s, %(version)s, %(name)s, %(email)s, %(city)s, %(tier)s, %(occurred_at)s)
                    ON CONFLICT (customer_id, version) DO NOTHING
                    """,
                    row,
                )
            counts["customers"] = len(tables.get("customers", []))

            # payments
            for row in tables.get("payments", []):
                cur.execute(
                    """
                    INSERT INTO silver.payments (payment_id, order_id, amount, method, status, occurred_at)
                    VALUES (%(payment_id)s, %(order_id)s, %(amount)s, %(method)s, %(status)s, %(occurred_at)s)
                    ON CONFLICT (payment_id) DO NOTHING
                    """,
                    row,
                )
            counts["payments"] = len(tables.get("payments", []))
        conn.commit()
    return counts


def run_silver(max_seq: int | None = None) -> dict[str, int]:
    """Incremental: read bronze rows since last watermark, parse, load silver."""
    with connect() as conn:
        watermark = get_watermark(conn, "silver")
    log.info("Silver watermark: %s", watermark)

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_seq, event_id, event_type, occurred_at, payload
                FROM bronze.raw_events
                WHERE event_seq > %s
                ORDER BY event_seq
                LIMIT %s
                """,
                (watermark, max_seq or 10000),
            )
            rows = [dict(r) for r in cur.fetchall()]

    if not rows:
        log.info("No new events for silver to process.")
        return {}

    max_seq_found = max(r["event_seq"] for r in rows)
    tables = parse_bronze_rows(rows)
    counts = load_silver(tables)

    with connect() as conn:
        set_watermark(conn, "silver", max_seq_found)

    log.info("Silver loaded: %s (watermark %s -> %s)", counts, watermark, max_seq_found)
    return counts