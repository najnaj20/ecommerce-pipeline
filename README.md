# ecommerce-pipeline

> 📖 [English](README.md) · [Bahasa Indonesia](README.id.md)

**Streaming e-commerce data warehouse** — a self-contained data engineering portfolio project demonstrating the **Medallion architecture** (Bronze → Silver → Gold) with **Apache Kafka**, **PostgreSQL**, **SCD Type 2**, **Prefect orchestration**, **incremental watermark loading**, **data quality checks**, and **Tableau-ready exports**.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ecommerce-pipeline                          │
│                                                                     │
│  Producer (simulated stream)                                        │
│  ┌──────────────────────────────────────┐                          │
│  │  customer_created / customer_updated │                          │
│  │  order_created + items              │──── Kafka ────┐           │
│  │  payment_received                   │(ecommerce.events)         │
│  └──────────────────────────────────────┘               │           │
│                                                         ▼           │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │  BRONZE  (raw_events)  — append-only, accepts corrupted data   │ │
│  │  Idempotent via ON CONFLICT (event_id). Schema-on-read.        │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                             │ (event_seq > watermark)                 │
│                             ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │  SILVER  — typed, deduplicated, validated                      │ │
│  │  customers (versioned) · orders · order_items · payments       │ │
│  │  Skips: unknown event types, null timestamps, null status,     │ │
│  │  negative prices, null quantities.                             │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                             │ (ordered_at > watermark)                │
│                             ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │  GOLD  — star schema, SCD Type 2, Tableau-ready               │ │
│  │  dim_customer (SCD2) · dim_product · dim_date · fact_sales     │ │
│  │  As-of customer version lookup per order date.                 │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                             │                                         │
│  ┌─────────────────────────┴─────────────────────────────────────────┐│
│  │  DQ checks  ·  Prefect flows  ·  CSV exports  ·  pytest suite   ││
│  └───────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

| Layer | Pattern | Rationale |
|-------|---------|-----------|
| **Bronze** | `ON CONFLICT (event_id) DO NOTHING` | Idempotent re-runs; deduplicates at ingestion |
| **Bronze** | `occurred_at` nullable | Raw landing accepts corrupted timestamps; DQ layer flags them |
| **Silver** | Versioned customers `(customer_id, version)` | Durable history before SCD2 collapse |
| **Silver** | Skip invalid rows, don't crash | Cleaning layer responsibility |
| **Gold** | `UNIQUE (customer_id, valid_from)` | Prevents SCD2 row duplication across re-runs |
| **Gold** | `LEAD(valid_from)` window recompute | Self-healing SCD2: consistent after any insertion order |
| **Gold** | As-of customer version lookup | Fact attributed to customer version valid **at order time** |
| **Watermark** | Silver: `event_seq` — Gold: `ordered_at` epoch (float) | Incremental processing; boundary row excluded exactly |

---

## Quick Start

### Prerequisites

- Python 3.11+
- Java 17 (for Kafka)
- PostgreSQL 16

### 1. Setup

```bash
# One-command local setup (installs Java, PostgreSQL, Kafka):
bash scripts/setup_local.sh

# Create Python virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the Pipeline

```bash
# Full end-to-end run (generate events → Kafka → Bronze → Silver → Gold → DQ → Export)
python -m src.main all

# Or step by step:
python -m src.main init        # Create schemas (idempotent)
python -m src.main produce     # Simulate events → Kafka
python -m src.main bronze      # Land events → bronze.raw_events
python -m src.main silver      # Bronze → Silver (cleaned)
python -m src.main gold        # Silver → Gold star schema (SCD2)
python -m src.main dq          # Data quality checks
python -m src.main export      # Tableau-ready CSV files
```

### 3. Verify

```bash
# Run tests (unit tests — no network, no Kafka needed)
pytest tests/ -v

# Run integration tests (requires PostgreSQL)
pytest tests/test_integration.py -v -m integration
```

### 4. View Exports

```bash
ls data/exports/
# fact_sales.csv  dim_customer.csv  dim_product.csv  dim_date.csv
```

---

## Sample Run Output

```
08:48:17 INFO    src.bronze: Bronze land complete: 184 events landed
08:48:18 INFO    src.silver: Silver loaded: {'orders': 60, 'order_items': 158, 'customers': 58, 'payments': 60}
08:48:18 INFO    src.gold: Gold build complete: {'fact_sales': 158, 'dim_customer': 46, 'dim_date': 4, 'dim_product': 12}
  [PASS] no_null_business_keys: no NULL business keys
  [PASS] fact_sales_unique: fact_sales order_item_id unique
  [PASS] fact_referential_integrity: all fact_sales rows resolve to dimensions
  [PASS] positive_price_qty: all prices and quantities positive
  [PASS] dates_in_expected_window: date range 2026-08-18 .. 2026-08-21
  [PASS] bronze_timestamps_present: all raw events carry a timestamp
```

---

## Project Structure

```
ecommerce-pipeline/
├── config/settings.py          # Central configuration (env-based)
├── src/
│   ├── producer.py             # Simulated e-commerce event stream → Kafka
│   ├── bronze.py               # Kafka consumer → raw landing (PostgreSQL)
│   ├── silver.py               # Clean, dedupe, validate → silver tables
│   ├── gold.py                 # Star schema + SCD Type 2 → gold tables
│   ├── dq.py                   # Data quality checks (6 checks)
│   ├── exports.py              # Tableau-ready CSV exports
│   ├── orchestrate.py          # Prefect flow DAG
│   ├── db.py                   # Connection management + DDL
│   └── main.py                 # CLI entry point
├── tests/
│   ├── test_pipeline.py        # 14 unit tests (pure logic, no network)
│   └── test_integration.py     # 4 integration tests (PostgreSQL)
├── scripts/
│   └── setup_local.sh          # One-command local environment setup
├── data/exports/               # Generated CSV files (gitignored)
├── .env.example                # Environment variable template
├── requirements.txt
├── pytest.ini
└── .gitignore
```

---

## What Makes This Standout

- **Production-grade patterns**: idempotent upserts, watermark incremental loading, self-healing SCD2, as-of dimension lookups
- **Honest about limits**: Kafka runs KRaft single-node locally; the producer is a simulator; the scheduler is a one-shot Prefect flow (documented as "prod would use Airflow/Prefect server")
- **Corruption injection**: 2% of events are deliberately malformed to demonstrate that the pipeline handles bad data gracefully
- **Deduplication at every layer**: bronze (`ON CONFLICT`), silver (dict key), gold (`ON CONFLICT`)
- **18 tests total**: 14 pure unit + 4 integration, covering SCD2, as-of lookup, dedup, idempotency, DQ suite
- **Bilingual README**: English + Bahasa Indonesia

---

## License

MIT