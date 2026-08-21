# ecommerce-pipeline

> 📖 [English](README.md) · [Bahasa Indonesia](README.id.md)

**Streaming e-commerce data warehouse** — proyek portofolio data engineering end-to-end yang mendemonstrasikan arsitektur **Medallion** (Bronze → Silver → Gold) dengan **Apache Kafka**, **PostgreSQL**, **SCD Type 2**, **orchestrasi Prefect**, **incremental watermark**, **data quality checks**, dan **ekspor CSV siap Tableau**.

---

## Arsitektur

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ecommerce-pipeline                          │
│                                                                     │
│  Producer (stream simulasi)                                         │
│  ┌──────────────────────────────────────┐                          │
│  │  customer_created / customer_updated │                          │
│  │  order_created + items              │──── Kafka ────┐           │
│  │  payment_received                   │(ecommerce.events)         │
│  └──────────────────────────────────────┘               │           │
│                                                         ▼           │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │  BRONZE  (raw_events)  — append-only, terima data korup       │ │
│  │  Idempoten via ON CONFLICT (event_id). Schema-on-read.        │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                             │ (event_seq > watermark)                 │
│                             ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │  SILVER  — typed, deduplikasi, validasi                       │ │
│  │  customers (versioned) · orders · order_items · payments       │ │
│  │  Melewati: event type tidak dikenal, timestamp null, status    │ │
│  │  null, harga negatif, quantity null.                           │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                             │ (ordered_at > watermark)                │
│                             ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │  GOLD  — star schema, SCD Type 2, siap Tableau               │ │
│  │  dim_customer (SCD2) · dim_product · dim_date · fact_sales     │ │
│  │  As-of lookup: versi customer yang valid saat order terjadi.   │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                             │                                         │
│  ┌─────────────────────────┴─────────────────────────────────────────┐│
│  │  DQ checks  ·  Prefect flows  ·  Ekspor CSV  ·  pytest suite   ││
│  └───────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────┘
```

### Keputusan Desain Kunci

| Layer | Pattern | Alasan |
|-------|---------|--------|
| **Bronze** | `ON CONFLICT (event_id) DO NOTHING` | Idempoten; deduplikasi di ingestion |
| **Bronze** | `occurred_at` nullable | Terima data korup; DQ layer yang menandai |
| **Silver** | Customers versioned `(customer_id, version)` | Riwayat durable sebelum SCD2 |
| **Silver** | Skip baris invalid, jangan crash | Tanggung jawab cleaning layer |
| **Gold** | `UNIQUE (customer_id, valid_from)` | Cegah duplikasi SCD2 antar run |
| **Gold** | `LEAD(valid_from)` window recompute | Self-healing SCD2: konsisten setelah urutan insert apapun |
| **Gold** | As-of lookup versi customer | Fakta diatribusikan ke versi customer yang valid **saat order** |
| **Watermark** | Silver: `event_seq` — Gold: `ordered_at` epoch (float) | Incremental; boundary row tepat tidak ikut reprocess |

---

## Quick Start

### Prasyarat

- Python 3.11+
- Java 17 (untuk Kafka)
- PostgreSQL 16

### 1. Setup

```bash
# Setup otomatis (install Java, PostgreSQL, Kafka):
bash scripts/setup_local.sh

# Buat virtual environment Python
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Jalankan Pipeline

```bash
# Full end-to-end (generate events → Kafka → Bronze → Silver → Gold → DQ → Export)
python -m src.main all

# Atau step by step:
python -m src.main init        # Buat skema tabel (idempoten)
python -m src.main produce     # Simulasi event → Kafka
python -m src.main bronze      # Landing event → bronze.raw_events
python -m src.main silver      # Bronze → Silver (cleaned)
python -m src.main gold        # Silver → Gold star schema (SCD2)
python -m src.main dq          # Data quality checks
python -m src.main export      # CSV siap Tableau
```

### 3. Verifikasi

```bash
# Unit tests (tanpa network, tanpa Kafka)
pytest tests/ -v

# Integration tests (butuh PostgreSQL)
pytest tests/test_integration.py -v -m integration
```

### 4. Lihat Hasil Ekspor

```bash
ls data/exports/
# fact_sales.csv  dim_customer.csv  dim_product.csv  dim_date.csv
```

---

## Contoh Output

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

## Struktur Proyek

```
ecommerce-pipeline/
├── config/settings.py          # Konfigurasi terpusat (env-based)
├── src/
│   ├── producer.py             # Simulasi event e-commerce → Kafka
│   ├── bronze.py               # Kafka consumer → raw landing (PostgreSQL)
│   ├── silver.py               # Cleaning, dedupe, validasi → silver tables
│   ├── gold.py                 # Star schema + SCD Type 2 → gold tables
│   ├── dq.py                   # Data quality checks (6 checks)
│   ├── exports.py              # Ekspor CSV siap Tableau
│   ├── orchestrate.py          # Prefect flow DAG
│   ├── db.py                   # Manajemen koneksi + DDL
│   └── main.py                 # CLI entry point
├── tests/
│   ├── test_pipeline.py        # 14 unit tests (logika murni, tanpa network)
│   └── test_integration.py     # 4 integration tests (PostgreSQL)
├── scripts/
│   └── setup_local.sh          # Setup environment satu perintah
├── data/exports/               # File CSV hasil generate (gitignored)
├── .env.example                # Template environment variable
├── requirements.txt
├── pytest.ini
└── .gitignore
```

---

## Yang Membuat Ini Spesial

- **Pattern production-grade**: upsert idempoten, watermark incremental, SCD2 self-healing, as-of dimension lookup
- **Jujur soal limitasi**: Kafka KRaft single-node; producer adalah simulator; scheduler adalah one-shot Prefect flow (tertulis "prod pakai Airflow/Prefect server")
- **Inject korupsi**: 2% event sengaja dirusak untuk buktikan pipeline tangani data buruk
- **Deduplikasi di setiap layer**: bronze (`ON CONFLICT`), silver (dict key), gold (`ON CONFLICT`)
- **18 tests**: 14 unit + 4 integration, mencakup SCD2, as-of lookup, dedup, idempotensi, DQ suite
- **README bilingual**: English + Bahasa Indonesia

---

## Lisensi

MIT