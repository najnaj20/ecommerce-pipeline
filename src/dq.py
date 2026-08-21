"""Data quality checks — run after each layer and log PASS/FAIL to meta.dq_run.

Checks cover the classic DE concerns:
  * completeness  — no NULLs on business keys / required columns
  * uniqueness    — no duplicate order_items / events in fact
  * referential   — fact_sales rows point at existing dimensions
  * domain sanity  — prices > 0, quantities > 0, dates within expected window
"""
from __future__ import annotations

import logging

from src.db import connect

log = logging.getLogger(__name__)


def _record(conn, check_name: str, status: str, detail: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO meta.dq_run (check_name, status, detail) VALUES (%s, %s, %s)",
            (check_name, status, detail),
        )


def run_checks() -> list[dict]:
    """Execute the DQ suite; returns [{check_name, status, detail}]."""
    results: list[dict] = []
    checks = [
        _check_null_keys,
        _check_fact_uniqueness,
        _check_referential_integrity,
        _check_price_domain,
        _check_date_window,
        _check_bronze_timestamps,
    ]

    with connect() as conn:
        for fn in checks:
            name, status, detail = fn(conn)
            _record(conn, name, status, detail)
            results.append({"check_name": name, "status": status, "detail": detail})
        conn.commit()
    return results


def _check_null_keys(conn) -> tuple[str, str, str]:
    name = "no_null_business_keys"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              (SELECT count(*) FROM silver.orders      WHERE order_id IS NULL)    AS orders,
              (SELECT count(*) FROM silver.order_items WHERE order_item_id IS NULL) AS items,
              (SELECT count(*) FROM gold.fact_sales    WHERE order_item_id IS NULL) AS facts
            """
        )
        r = cur.fetchone()
    bad = {k: v for k, v in r.items() if v}
    if bad:
        return name, "FAIL", f"NULL business keys found: {bad}"
    return name, "PASS", "no NULL business keys"


def _check_fact_uniqueness(conn) -> tuple[str, str, str]:
    name = "fact_sales_unique"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) AS dupes FROM (
                SELECT order_item_id FROM gold.fact_sales
                GROUP BY order_item_id HAVING count(*) > 1
            ) t
            """
        )
        dupes = cur.fetchone()["dupes"]
    if dupes:
        return name, "FAIL", f"{dupes} duplicate order_item_id in fact_sales"
    return name, "PASS", "fact_sales order_item_id unique"


def _check_referential_integrity(conn) -> tuple[str, str, str]:
    name = "fact_referential_integrity"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) AS orphans FROM gold.fact_sales f
            LEFT JOIN gold.dim_customer c ON c.customer_sk = f.customer_sk
            LEFT JOIN gold.dim_product  p ON p.product_sk = f.product_sk
            LEFT JOIN gold.dim_date     d ON d.date_sk = f.date_sk
            WHERE c.customer_sk IS NULL OR p.product_sk IS NULL OR d.date_sk IS NULL
            """
        )
        orphans = cur.fetchone()["orphans"]
    if orphans:
        return name, "FAIL", f"{orphans} fact rows reference missing dimensions"
    return name, "PASS", "all fact_sales rows resolve to dimensions"


def _check_price_domain(conn) -> tuple[str, str, str]:
    name = "positive_price_qty"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) AS bad FROM gold.fact_sales
            WHERE unit_price <= 0 OR quantity <= 0
            """
        )
        bad = cur.fetchone()["bad"]
    if bad:
        return name, "FAIL", f"{bad} rows with non-positive price/quantity"
    return name, "PASS", "all prices and quantities positive"


def _check_date_window(conn) -> tuple[str, str, str]:
    name = "dates_in_expected_window"
    with conn.cursor() as cur:
        cur.execute("SELECT max(full_date) AS mx, min(full_date) AS mn FROM gold.dim_date")
        r = cur.fetchone()
        if not r or r["mx"] is None:
            return name, "SKIP", "no dates to check"
        cur.execute(
            "SELECT count(*) AS future FROM gold.dim_date WHERE full_date > now() + interval '1 day'"
        )
        future = cur.fetchone()["future"]
    if future:
        return name, "FAIL", f"{future} dates in the future"
    return name, "PASS", f"date range {r['mn']} .. {r['mx']}"


def _check_bronze_timestamps(conn) -> tuple[str, str, str]:
    name = "bronze_timestamps_present"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS bad FROM bronze.raw_events WHERE occurred_at IS NULL"
        )
        bad = cur.fetchone()["bad"]
    if bad:
        return name, "FAIL", f"{bad} raw events with NULL occurred_at (corrupted data)"
    return name, "PASS", "all raw events carry a timestamp"