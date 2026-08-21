"""Integration tests — require a live Postgres (optionally Kafka).

These run against the real database so they verify the full SQL path:
schema creation, silver load, gold star schema, idempotency, DQ checks.

They are skipped automatically when PostgreSQL is not reachable, so the suite
is CI-safe (CI runs only the pure unit tests in test_pipeline.py).

Run explicitly:  pytest tests/test_integration.py -v
"""
from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.integration


def _pg_reachable() -> bool:
    try:
        import psycopg
        from config.settings import settings
        with psycopg.connect(settings.postgres.dsn, connect_timeout=3):
            return True
    except Exception:
        return False


REQUIRE_PG = pytest.mark.skipif(not _pg_reachable(), reason="PostgreSQL not reachable")


@REQUIRE_PG
def test_init_db_idempotent():
    from src.db import init_db
    init_db()  # run twice — must not raise
    init_db()
    assert True


@REQUIRE_PG
def test_silver_gold_roundtrip():
    """Insert crafted silver rows, build gold, assert star schema integrity."""
    from datetime import datetime, timezone
    from src.db import connect, init_db
    from src.gold import run_gold

    init_db()
    cust = "IT-CUST-1"
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM gold.fact_sales WHERE order_id LIKE 'IT-%'")
            cur.execute("DELETE FROM gold.dim_customer WHERE customer_id = %s", (cust,))
            cur.execute("DELETE FROM silver.orders WHERE order_id LIKE 'IT-%'")
            cur.execute("DELETE FROM silver.order_items WHERE order_id LIKE 'IT-%'")
            cur.execute("DELETE FROM silver.customers WHERE customer_id = %s", (cust,))
            cur.execute("DELETE FROM silver.payments WHERE order_id LIKE 'IT-%'")
            ts1 = datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc)
            ts2 = datetime(2026, 8, 2, 10, 0, tzinfo=timezone.utc)
            cur.execute(
                "INSERT INTO silver.customers (customer_id, version, name, email, city, tier, occurred_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (cust, 1, "IT Customer", "it@x.com", "Jakarta", "standard", ts1),
            )
            cur.execute(
                "INSERT INTO silver.customers (customer_id, version, name, email, city, tier, occurred_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (cust, 2, "IT Customer", "it@x.com", "Bandung", "premium", ts2),
            )
            cur.execute(
                "INSERT INTO silver.orders (order_id, customer_id, ordered_at, status, currency, occurred_at) VALUES ('IT-ORD-1', %s, %s, 'completed', 'USD', %s)",
                (cust, ts2, ts2),
            )
            cur.execute(
                """INSERT INTO silver.order_items
                   (order_item_id, order_id, product_id, product_name, category, brand, quantity, unit_price, occurred_at)
                   VALUES ('IT-OI-1', 'IT-ORD-1', 'IT-PROD-1', 'Test Widget', 'Office', 'TestBrand', 3, 19.99, %s)""",
                (ts2,),
            )
        conn.commit()

    counts = run_gold()
    assert counts["fact_sales"] >= 1

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT f.quantity, f.revenue, c.customer_id, c.city, p.product_id, d.full_date
                   FROM gold.fact_sales f
                   JOIN gold.dim_customer c ON c.customer_sk = f.customer_sk
                   JOIN gold.dim_product p  ON p.product_sk = f.product_sk
                   JOIN gold.dim_date d     ON d.date_sk = f.date_sk
                   WHERE f.order_id = 'IT-ORD-1'"""
            )
            row = cur.fetchone()
            assert row is not None
            # as-of: order on Aug 2 → customer version 2 (Bandung, premium)
            assert row["city"] == "Bandung"
            assert row["quantity"] == 3
            assert float(row["revenue"]) == pytest.approx(59.97)


@REQUIRE_PG
def test_gold_idempotent_rerun():
    """Running gold twice must not duplicate fact rows."""
    from src.db import connect, init_db
    from src.gold import run_gold

    init_db()
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM gold.fact_sales")
            before = cur.fetchone()["n"]
    run_gold()
    run_gold()
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM gold.fact_sales")
            after = cur.fetchone()["n"]
    assert after == before


@REQUIRE_PG
def test_dq_checks_run():
    from src import dq
    from src.db import init_db
    init_db()
    results = dq.run_checks()
    assert len(results) >= 4
    for r in results:
        assert r["status"] in ("PASS", "FAIL", "SKIP")
