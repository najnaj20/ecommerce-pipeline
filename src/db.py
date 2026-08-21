"""PostgreSQL access layer: connection management + schema DDL.

Keeps every SQL DDL statement in one place so the database state is fully
reproducible: `python -m src.db init` creates bronze/silver/gold/meta schemas
idempotently.
"""
from __future__ import annotations

import logging

import psycopg
from psycopg.rows import dict_row

from config.settings import PostgresConfig, settings

log = logging.getLogger(__name__)

DDL = """
-- =====================================================================
-- Medallion architecture: bronze -> silver -> gold, plus meta for
-- watermarks & DQ run history.
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;

-- ---------- BRONZE: raw event landing (append-only, idempotent) ----------
CREATE TABLE IF NOT EXISTS bronze.raw_events (
    event_seq       BIGSERIAL PRIMARY KEY,
    event_id        VARCHAR(64)  NOT NULL UNIQUE,
    event_type      VARCHAR(32)  NOT NULL,
    -- nullable on purpose: bronze is raw landing, corrupted timestamps are
    -- allowed through and flagged by the DQ layer (silver skips them).
    occurred_at     TIMESTAMPTZ,
    payload         JSONB        NOT NULL,
    kafka_partition INT          NOT NULL,
    kafka_offset    BIGINT       NOT NULL,
    loaded_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS idx_raw_events_type ON bronze.raw_events (event_type);
CREATE INDEX IF NOT EXISTS idx_raw_events_seq  ON bronze.raw_events (event_seq);

-- ---------- SILVER: cleaned, typed, deduplicated ----------
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id VARCHAR(32)  NOT NULL,
    version     INT          NOT NULL,
    name        VARCHAR(120) NOT NULL,
    email       VARCHAR(160),
    city        VARCHAR(80),
    tier        VARCHAR(16)  NOT NULL DEFAULT 'standard',
    occurred_at TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (customer_id, version)
);

CREATE TABLE IF NOT EXISTS silver.orders (
    order_id    VARCHAR(32)  NOT NULL PRIMARY KEY,
    customer_id VARCHAR(32)  NOT NULL,
    ordered_at  TIMESTAMPTZ  NOT NULL,
    status      VARCHAR(16)  NOT NULL,
    currency    VARCHAR(3)   NOT NULL DEFAULT 'USD',
    occurred_at TIMESTAMPTZ  NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_silver_orders_cust ON silver.orders (customer_id);

CREATE TABLE IF NOT EXISTS silver.order_items (
    order_item_id VARCHAR(48)  NOT NULL PRIMARY KEY,
    order_id      VARCHAR(32)  NOT NULL,
    product_id    VARCHAR(32)  NOT NULL,
    product_name  VARCHAR(160),
    category      VARCHAR(64),
    brand         VARCHAR(64),
    quantity      INT          NOT NULL,
    unit_price    NUMERIC(12,2) NOT NULL,
    occurred_at   TIMESTAMPTZ  NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_silver_items_order ON silver.order_items (order_id);

CREATE TABLE IF NOT EXISTS silver.payments (
    payment_id VARCHAR(48)  NOT NULL PRIMARY KEY,
    order_id   VARCHAR(32)  NOT NULL,
    amount     NUMERIC(12,2) NOT NULL,
    method     VARCHAR(16)  NOT NULL,
    status     VARCHAR(16)  NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_silver_pay_order ON silver.payments (order_id);

-- ---------- GOLD: star schema (dimensions + fact) ----------
-- SCD Type 2 customer dimension: each version is one row; the current
-- version has valid_to IS NULL and is_current = TRUE.
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_sk BIGSERIAL PRIMARY KEY,
    customer_id VARCHAR(32)  NOT NULL,
    name        VARCHAR(120) NOT NULL,
    email       VARCHAR(160),
    city        VARCHAR(80),
    tier        VARCHAR(16)  NOT NULL,
    valid_from  TIMESTAMPTZ  NOT NULL,
    valid_to    TIMESTAMPTZ,
    is_current  BOOLEAN      NOT NULL DEFAULT TRUE,
    UNIQUE (customer_id, valid_from)
);
CREATE INDEX IF NOT EXISTS idx_dim_customer_id ON gold.dim_customer (customer_id, is_current);

CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_sk  BIGSERIAL PRIMARY KEY,
    product_id  VARCHAR(32)  NOT NULL UNIQUE,
    product_name VARCHAR(160) NOT NULL,
    category    VARCHAR(64),
    brand       VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_sk     INT PRIMARY KEY,
    full_date   DATE NOT NULL UNIQUE,
    year        INT  NOT NULL,
    month       INT  NOT NULL,
    day         INT  NOT NULL,
    quarter     INT  NOT NULL,
    day_of_week INT  NOT NULL,
    is_weekend  BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.fact_sales (
    fact_id       BIGSERIAL PRIMARY KEY,
    order_item_id VARCHAR(48) NOT NULL UNIQUE,
    order_id      VARCHAR(32) NOT NULL,
    customer_sk   INT         NOT NULL,
    product_sk    INT         NOT NULL,
    date_sk       INT         NOT NULL,
    quantity      INT         NOT NULL,
    unit_price    NUMERIC(12,2) NOT NULL,
    revenue       NUMERIC(14,2) NOT NULL,
    payment_method VARCHAR(16),
    payment_status VARCHAR(16),
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON gold.fact_sales (date_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_cust ON gold.fact_sales (customer_sk);

-- ---------- META: orchestration state ----------
CREATE TABLE IF NOT EXISTS meta.watermarks (
    stage      VARCHAR(32) PRIMARY KEY,
    -- DOUBLE PRECISION: silver uses event_seq (int), gold uses epoch seconds
    -- (float). Both fit comfortably.
    last_value DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta.dq_run (
    run_id      BIGSERIAL PRIMARY KEY,
    check_name  VARCHAR(80)  NOT NULL,
    status      VARCHAR(10)  NOT NULL,          -- PASS | FAIL
    detail      TEXT,
    checked_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
"""


def connect(pg: PostgresConfig | None = None):
    pg = pg or settings.postgres
    return psycopg.connect(pg.dsn, row_factory=dict_row)


def init_db(pg: PostgresConfig | None = None) -> None:
    """Create schemas + tables. Idempotent — safe to run on every deploy."""
    pg = pg or settings.postgres
    with connect(pg) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
    log.info("Database schema ready (bronze/silver/gold/meta) on %s/%s", pg.host, pg.dbname)


def get_watermark(conn, stage: str) -> float:
    with conn.cursor() as cur:
        cur.execute("SELECT last_value FROM meta.watermarks WHERE stage = %s", (stage,))
        row = cur.fetchone()
        return float(row["last_value"]) if row else 0.0


def set_watermark(conn, stage: str, value: float) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.watermarks (stage, last_value, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (stage) DO UPDATE
               SET last_value = EXCLUDED.last_value,
                   updated_at = now()
            """,
            (stage, value),
        )
    conn.commit()
