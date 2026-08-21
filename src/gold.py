"""Gold layer — build the star schema (dimensions + fact) from silver.

Implements SCD Type 2 for the customer dimension: silver customers are
versioned (customer_id, version).  Gold turns those versions into a proper SCD2
table where each version becomes a row with valid_from / valid_to, and the
newest version is flagged is_current = TRUE.

Idempotency strategy (re-running is safe):
  1. New version rows are inserted with ``ON CONFLICT (customer_id, valid_from)
     DO NOTHING`` — duplicates are ignored.
  2. After inserting, the valid_to / is_current window for every affected
     customer is RECOMPUTED from the full version history (LEAD window
     function).  This is self-healing: even if an older run inserted rows out
     of order, the final state is always consistent.

Fact rows use an **as-of lookup**: each sale is attributed to the customer
version that was current *at the time of the order*, not the latest one.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from src.db import connect, get_watermark, set_watermark

log = logging.getLogger(__name__)


# ── SCD Type 2 builder (pure, testable) ────────────────────────────────────

def build_scd2_customer(rows: list[dict]) -> list[dict]:
    """Convert silver customers (customer_id, version) into SCD2 rows.

    ``rows`` must contain version history per customer.  Each silver row is a
    version; consecutive versions with identical attributes are merged into a
    single SCD2 period (no spurious new row for no-op updates).
    """
    if not rows:
        return []

    # group by customer, preserve version order
    by_customer: dict[str, list[dict]] = {}
    for r in rows:
        by_customer.setdefault(r["customer_id"], []).append(r)

    scd: list[dict] = []
    for cid, versions in by_customer.items():
        versions.sort(key=lambda r: r["version"])
        # Drop consecutive versions that are attribute-identical (no-op update)
        merged: list[dict] = []
        for v in versions:
            if merged and _same_attrs(merged[-1], v):
                merged[-1]["version"] = v["version"]  # extend period to latest version
            else:
                merged.append(dict(v))

        for i, v in enumerate(merged):
            valid_to = merged[i + 1]["occurred_at"] if i + 1 < len(merged) else None
            scd.append({
                "customer_id": v["customer_id"],
                "name": v["name"],
                "email": v["email"],
                "city": v["city"],
                "tier": v["tier"],
                "valid_from": v["occurred_at"],
                "valid_to": valid_to,
                "is_current": valid_to is None,
            })
    return scd


def _same_attrs(a: dict, b: dict) -> bool:
    for key in ("name", "email", "city", "tier"):
        if a.get(key) != b.get(key):
            return False
    return True


# ── dimension helpers ──────────────────────────────────────────────────────

def _dim_date_rows(sales: list[dict]) -> list[dict]:
    """Generate the date dimension covering the order date range."""
    if not sales:
        return []
    dates = sorted({r["ordered_at"].date() for r in sales if isinstance(r.get("ordered_at"), datetime)})
    if not dates:
        return []
    rows = []
    d = dates[0]
    while d <= dates[-1]:
        rows.append({
            "date_sk": int(d.strftime("%Y%m%d")),
            "full_date": d,
            "year": d.year,
            "month": d.month,
            "day": d.day,
            "quarter": (d.month - 1) // 3 + 1,
            "day_of_week": d.weekday(),          # 0=Mon .. 6=Sun
            "is_weekend": d.weekday() >= 5,
        })
        d += timedelta(days=1)
    return rows


def _load_dim_date(conn, rows: list[dict]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO gold.dim_date (date_sk, full_date, year, month, day, quarter, day_of_week, is_weekend)
            VALUES (%(date_sk)s, %(full_date)s, %(year)s, %(month)s, %(day)s, %(quarter)s, %(day_of_week)s, %(is_weekend)s)
            ON CONFLICT (date_sk) DO NOTHING
            """,
            rows,
        )


def _load_dim_product(conn, sales: list[dict]) -> dict[str, int]:
    """Upsert products from sales rows; returns {product_id: product_sk}."""
    mapping: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT product_id, product_sk FROM gold.dim_product")
        mapping = {r["product_id"]: r["product_sk"] for r in cur.fetchall()}
        for s in sales:
            pid = s["product_id"]
            if pid not in mapping:
                cur.execute(
                    """
                    INSERT INTO gold.dim_product (product_id, product_name, category, brand)
                    VALUES (%(product_id)s, %(product_name)s, %(category)s, %(brand)s)
                    ON CONFLICT (product_id) DO UPDATE
                        SET product_name = EXCLUDED.product_name,
                            category = EXCLUDED.category,
                            brand = EXCLUDED.brand
                    RETURNING product_sk
                    """,
                    s,
                )
                row = cur.fetchone()
                mapping[pid] = row["product_sk"]
    return mapping


def _insert_customer_versions(conn, scd_rows: list[dict]) -> None:
    """Idempotent insert of SCD2 rows (skip versions already present)."""
    if not scd_rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO gold.dim_customer
                (customer_id, name, email, city, tier, valid_from, valid_to, is_current)
            VALUES (%(customer_id)s, %(name)s, %(email)s, %(city)s, %(tier)s,
                    %(valid_from)s, %(valid_to)s, %(is_current)s)
            ON CONFLICT (customer_id, valid_from) DO NOTHING
            """,
            scd_rows,
        )


def _recompute_customer_windows(conn, customer_ids: list[str]) -> None:
    """Self-healing SCD2: recompute valid_to / is_current from version history.

    Using a LEAD window function over valid_from per customer, the table is
    brought to a consistent state no matter what order rows were inserted in.
    """
    if not customer_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE gold.dim_customer d
            SET valid_to   = w.next_from,
                is_current = (w.next_from IS NULL)
            FROM (
                SELECT customer_sk,
                       LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) AS next_from
                FROM gold.dim_customer
                WHERE customer_id = ANY(%s)
            ) w
            WHERE d.customer_sk = w.customer_sk
            """,
            (customer_ids,),
        )


def _customer_versions(conn, customer_ids: list[str]) -> list[dict]:
    """Fetch current SCD2 rows (sk + interval) for as-of lookups."""
    if not customer_ids:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT customer_sk, customer_id, valid_from, valid_to
            FROM gold.dim_customer
            WHERE customer_id = ANY(%s)
            ORDER BY customer_id, valid_from
            """,
            (customer_ids,),
        )
        return cur.fetchall()


def _as_of_customer_sk(versions: list[dict], customer_id: str, ordered_at: datetime) -> int | None:
    """Pick the customer version whose [valid_from, valid_to) contains the date."""
    best = None
    for v in versions:
        if v["customer_id"] != customer_id:
            continue
        if v["valid_from"] <= ordered_at and (v["valid_to"] is None or v["valid_to"] > ordered_at):
            return v["customer_sk"]
        # track latest as fallback (defensive: order before first version)
        if best is None or v["valid_from"] > best["valid_from"]:
            best = v
    return best["customer_sk"] if best else None


def _load_fact_sales(conn, fact_rows: list[dict]) -> int:
    if not fact_rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO gold.fact_sales
                (order_item_id, order_id, customer_sk, product_sk, date_sk,
                 quantity, unit_price, revenue, payment_method, payment_status)
            VALUES (%(order_item_id)s, %(order_id)s, %(customer_sk)s, %(product_sk)s, %(date_sk)s,
                    %(quantity)s, %(unit_price)s, %(revenue)s, %(payment_method)s, %(payment_status)s)
            ON CONFLICT (order_item_id) DO NOTHING
            """,
            fact_rows,
        )
        return len(fact_rows)


# ── main entry ─────────────────────────────────────────────────────────────

def run_gold() -> dict:
    """Incremental gold build: process silver orders newer than watermark."""
    with connect() as conn:
        watermark = get_watermark(conn, "gold")

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT o.order_id, o.customer_id, o.ordered_at, o.status, o.currency,
                       oi.order_item_id, oi.product_id, oi.product_name, oi.category, oi.brand,
                       oi.quantity, oi.unit_price,
                       p.method, p.status AS payment_status
                FROM silver.orders o
                JOIN silver.order_items oi ON oi.order_id = o.order_id
                LEFT JOIN silver.payments p ON p.order_id = o.order_id
                WHERE EXTRACT(EPOCH FROM o.ordered_at) > %s
                ORDER BY o.ordered_at
                """,
                (watermark,),
            )
            sales = cur.fetchall()

    if not sales:
        log.info("No new sales for gold build.")
        return {"fact_sales": 0, "dim_customer": 0, "dim_date": 0, "dim_product": 0}

    # customer version history needed for SCD2 (all versions, not just new)
    cust_ids = sorted({r["customer_id"] for r in sales})
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT customer_id, version, name, email, city, tier, occurred_at
                FROM silver.customers
                WHERE customer_id = ANY(%s)
                ORDER BY customer_id, version
                """,
                (cust_ids,),
            )
            customer_rows = cur.fetchall()

    scd_rows = build_scd2_customer(customer_rows)
    date_rows = _dim_date_rows(sales)

    # build fact rows (customer/product sk resolved after dim loads)
    fact_rows = []
    for s in sales:
        date_sk = int(s["ordered_at"].strftime("%Y%m%d"))
        qty = s["quantity"]
        price = float(s["unit_price"])
        fact_rows.append({
            "order_item_id": s["order_item_id"],
            "order_id": s["order_id"],
            "customer_id": s["customer_id"],
            "product_id": s["product_id"],
            "ordered_at": s["ordered_at"],
            "customer_sk": None,  # filled after dim loads
            "product_sk": None,
            "date_sk": date_sk,
            "quantity": qty,
            "unit_price": price,
            "revenue": round(qty * price, 2),
            "payment_method": s.get("method"),
            "payment_status": s.get("payment_status"),
        })

    with connect() as conn:
        _load_dim_date(conn, date_rows)
        product_map = _load_dim_product(conn, sales)
        _insert_customer_versions(conn, scd_rows)
        _recompute_customer_windows(conn, cust_ids)
        versions = _customer_versions(conn, cust_ids)

        for r in fact_rows:
            r["customer_sk"] = _as_of_customer_sk(versions, r["customer_id"], r["ordered_at"])
            r["product_sk"] = product_map.get(r["product_id"])
        fact_rows = [r for r in fact_rows if r["customer_sk"] is not None and r["product_sk"] is not None]
        n_fact = _load_fact_sales(conn, fact_rows)
        conn.commit()

    # advance watermark to max ordered_at seen (float epoch seconds, so the
    # boundary row is NOT re-processed on the next incremental run)
    max_ts = max(s["ordered_at"] for s in sales)
    new_watermark = max_ts.timestamp()
    with connect() as conn:
        set_watermark(conn, "gold", new_watermark)

    counts = {
        "fact_sales": n_fact,
        "dim_customer": len(scd_rows),
        "dim_date": len(date_rows),
        "dim_product": len(product_map),
    }
    log.info("Gold build complete: %s", counts)
    return counts