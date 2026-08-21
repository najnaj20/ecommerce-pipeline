"""Tableau-ready exports — write gold tables to CSV files.

CSV is the lowest-common-denominator format for Tableau Desktop / Tableau
Public: point Tableau at ``data/exports/`` and it picks every file up.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from config.settings import settings
from src.db import connect

log = logging.getLogger(__name__)

# name -> SQL (use explicit columns, not *, for stable schema)
EXPORTS = {
    "fact_sales": """
        SELECT f.order_item_id, f.order_id, f.quantity, f.unit_price, f.revenue,
               f.payment_method, f.payment_status,
               d.full_date AS order_date, d.year, d.month, d.quarter, d.is_weekend,
               c.customer_id, c.name AS customer_name, c.city, c.tier,
               p.product_id, p.product_name, p.category, p.brand
        FROM gold.fact_sales f
        JOIN gold.dim_date d     ON d.date_sk = f.date_sk
        JOIN gold.dim_customer c ON c.customer_sk = f.customer_sk
        JOIN gold.dim_product p  ON p.product_sk = f.product_sk
        ORDER BY d.full_date
    """,
    "dim_customer": """
        SELECT customer_sk, customer_id, name, email, city, tier,
               valid_from, valid_to, is_current
        FROM gold.dim_customer ORDER BY customer_id, valid_from
    """,
    "dim_product": """
        SELECT product_sk, product_id, product_name, category, brand
        FROM gold.dim_product ORDER BY product_id
    """,
    "dim_date": """
        SELECT date_sk, full_date, year, month, day, quarter, day_of_week, is_weekend
        FROM gold.dim_date ORDER BY full_date
    """,
}


def export_all(out_dir: Path | None = None) -> dict[str, Path]:
    """Dump every export to CSV; returns {name: path}."""
    out_dir = out_dir or settings.export_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    with connect() as conn:
        for name, sql in EXPORTS.items():
            df = _query_to_df(conn, sql)
            path = out_dir / f"{name}.csv"
            df.to_csv(path, index=False)
            written[name] = path
            log.info("Exported %s -> %s (%d rows)", name, path, len(df))
    return written


def _query_to_df(conn, sql: str) -> pd.DataFrame:
    """Run a query and return a DataFrame without SQLAlchemy."""
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=cols)