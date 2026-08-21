"""Unit tests for the ecommerce pipeline (no Kafka, no network).

Pure-logic tests: event generation, silver parsing, SCD2 building, as-of
lookup, date dimension.  Integration tests that need Postgres + Kafka live in
tests/test_integration.py and are skipped when those services are absent.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from src import producer, silver
from src.gold import build_scd2_customer, _as_of_customer_sk, _dim_date_rows


# ── producer ───────────────────────────────────────────────────────────────

def test_generate_events_shape():
    events = producer.generate_events(count=100, corruption_rate=0.0, duplicate_rate=0.0, stream_span_hours=24)
    assert len(events) > 0
    for evt in events:
        assert "event_id" in evt
        assert "event_type" in evt
        assert "occurred_at" in evt
        assert "payload" in evt
        json.dumps(evt)  # must be serialisable


def test_generate_events_contains_orders_and_customers():
    events = producer.generate_events(count=100, corruption_rate=0.0, duplicate_rate=0.0)
    types = {e["event_type"] for e in events}
    assert "order_created" in types
    assert "customer_created" in types
    assert "payment_received" in types


def test_generate_events_corruption():
    events = producer.generate_events(count=200, corruption_rate=1.0, duplicate_rate=0.0)
    corrupted = [e for e in events if e.get("event_type") == "unknown_event_type" or e.get("occurred_at") is None]
    assert len(corrupted) > 0, "corruption_rate=1.0 should inject bad events"


def test_generate_events_duplicates():
    events = producer.generate_events(count=200, corruption_rate=0.0, duplicate_rate=1.0)
    ids = [e["event_id"] for e in events]
    assert len(ids) != len(set(ids)), "duplicate_rate=1.0 should inject duplicate event_ids"


# ── silver parsing ─────────────────────────────────────────────────────────

def _bronze_row(etype: str, payload: dict, ts: str = "2026-08-01T10:00:00Z") -> dict:
    return {"event_seq": 1, "event_id": "evt_1", "event_type": etype,
            "occurred_at": ts, "payload": payload}


def test_silver_parses_order_with_items():
    payload = {
        "order_id": "ORD-0001",
        "customer_id": "CUST-0001",
        "ordered_at": "2026-08-01T10:00:00Z",
        "status": "pending",
        "currency": "USD",
        "items": [
            {"order_item_id": "OI-0001-1", "product_id": "PROD-001",
             "product_name": "Wireless Mouse", "category": "Electronics", "brand": "TechGear",
             "quantity": 2, "unit_price": 25.99},
        ],
    }
    tables = silver.parse_bronze_rows([_bronze_row("order_created", payload)])
    assert len(tables["orders"]) == 1
    assert len(tables["order_items"]) == 1
    assert tables["order_items"][0]["unit_price"] == 25.99
    assert tables["order_items"][0]["product_name"] == "Wireless Mouse"


def test_silver_skips_unknown_event_type():
    tables = silver.parse_bronze_rows([_bronze_row("weird_type", {})])
    assert tables == {"customers": [], "orders": [], "order_items": [], "payments": []}


def test_silver_skips_order_without_items():
    payload = {"order_id": "ORD-0002", "customer_id": "CUST-0002",
               "ordered_at": "2026-08-01T10:00:00Z", "status": "pending", "items": []}
    tables = silver.parse_bronze_rows([_bronze_row("order_created", payload)])
    assert len(tables["orders"]) == 1
    assert tables["order_items"] == []


def test_silver_skips_order_without_status():
    """An order missing its status is corrupted — silver must skip it, not crash."""
    payload = {"order_id": "ORD-0003", "customer_id": "CUST-0003",
               "ordered_at": "2026-08-01T10:00:00Z", "status": None,
               "items": [{"order_item_id": "OI-0003-1", "product_id": "PROD-001",
                          "quantity": 1, "unit_price": 10.0}]}
    tables = silver.parse_bronze_rows([_bronze_row("order_created", payload)])
    assert tables["orders"] == []
    assert tables["order_items"] == []


def test_silver_versions_customer():
    created = _bronze_row("customer_created", {"customer_id": "CUST-0001", "name": "A", "email": "a@x.com",
                                               "city": "Jakarta", "tier": "standard"}, ts="2026-08-01T10:00:00Z")
    updated = _bronze_row("customer_updated", {"customer_id": "CUST-0001", "name": "A", "email": "a@x.com",
                                               "city": "Bandung", "tier": "premium"}, ts="2026-08-02T10:00:00Z")
    tables = silver.parse_bronze_rows([created, updated])
    customers = sorted(tables["customers"], key=lambda r: r["version"])
    assert len(customers) == 2
    assert customers[0]["version"] == 1
    assert customers[1]["version"] == 2
    assert customers[1]["city"] == "Bandung"


# ── gold SCD2 ──────────────────────────────────────────────────────────────

def test_scd2_builds_history():
    rows = [
        {"customer_id": "CUST-1", "version": 1, "name": "A", "email": "a@x.com",
         "city": "Jakarta", "tier": "standard", "occurred_at": datetime(2026, 8, 1, tzinfo=timezone.utc)},
        {"customer_id": "CUST-1", "version": 2, "name": "A", "email": "a@x.com",
         "city": "Bandung", "tier": "standard", "occurred_at": datetime(2026, 8, 3, tzinfo=timezone.utc)},
    ]
    scd = build_scd2_customer(rows)
    assert len(scd) == 2
    assert scd[0]["valid_to"] == rows[1]["occurred_at"]
    assert scd[0]["is_current"] is False
    assert scd[1]["valid_to"] is None
    assert scd[1]["is_current"] is True


def test_scd2_merges_noop_updates():
    rows = [
        {"customer_id": "CUST-1", "version": 1, "name": "A", "email": "a@x.com",
         "city": "Jakarta", "tier": "standard", "occurred_at": datetime(2026, 8, 1, tzinfo=timezone.utc)},
        {"customer_id": "CUST-1", "version": 2, "name": "A", "email": "a@x.com",
         "city": "Jakarta", "tier": "standard", "occurred_at": datetime(2026, 8, 2, tzinfo=timezone.utc)},
        {"customer_id": "CUST-1", "version": 3, "name": "A", "email": "a@x.com",
         "city": "Surabaya", "tier": "standard", "occurred_at": datetime(2026, 8, 4, tzinfo=timezone.utc)},
    ]
    scd = build_scd2_customer(rows)
    assert len(scd) == 2, "no-op update (same attrs) should be merged into one period"


def test_as_of_lookup_picks_correct_version():
    versions = [
        {"customer_sk": 1, "customer_id": "CUST-1",
         "valid_from": datetime(2026, 8, 1, tzinfo=timezone.utc),
         "valid_to": datetime(2026, 8, 3, tzinfo=timezone.utc)},
        {"customer_sk": 2, "customer_id": "CUST-1",
         "valid_from": datetime(2026, 8, 3, tzinfo=timezone.utc),
         "valid_to": None},
    ]
    assert _as_of_customer_sk(versions, "CUST-1", datetime(2026, 8, 2, tzinfo=timezone.utc)) == 1
    assert _as_of_customer_sk(versions, "CUST-1", datetime(2026, 8, 5, tzinfo=timezone.utc)) == 2
    assert _as_of_customer_sk(versions, "CUST-9", datetime(2026, 8, 5, tzinfo=timezone.utc)) is None


def test_dim_date_coverage():
    sales = [
        {"ordered_at": datetime(2026, 8, 1, tzinfo=timezone.utc)},
        {"ordered_at": datetime(2026, 8, 3, tzinfo=timezone.utc)},
    ]
    rows = _dim_date_rows(sales)
    assert len(rows) == 3  # Aug 1, 2, 3
    assert rows[0]["date_sk"] == 20260801
    assert rows[0]["is_weekend"] is True  # Aug 1 2026 is a Saturday
    assert {r["full_date"].day for r in rows} == {1, 2, 3}


def test_silver_order_items_dedupe_via_dict():
    """Two identical events (duplicate) must collapse to one order item."""
    payload = {
        "order_id": "ORD-0001", "customer_id": "CUST-0001",
        "ordered_at": "2026-08-01T10:00:00Z", "status": "pending", "items": [
            {"order_item_id": "OI-0001-1", "product_id": "PROD-001", "quantity": 1, "unit_price": 10.0},
        ],
    }
    tables = silver.parse_bronze_rows([_bronze_row("order_created", payload), _bronze_row("order_created", payload)])
    assert len(tables["order_items"]) == 1  # dict keyed by order_item_id collapses dupes
