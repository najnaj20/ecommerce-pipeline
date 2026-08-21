"""ecommerce-pipeline: streaming e-commerce data warehouse.

CLI entry point:
    python -m src.main init        # create schemas/tables
    python -m src.main produce     # generate & publish events to Kafka
    python -m src.main bronze      # land Kafka events -> bronze.raw_events
    python -m src.main silver      # bronze -> silver (cleaned)
    python -m src.main gold        # silver -> gold star schema (SCD2)
    python -m src.main dq          # run data quality checks
    python -m src.main export      # Tableau-ready CSV exports
    python -m src.main all         # full pipeline (needs Kafka + Postgres up)
"""
from __future__ import annotations

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("main")


def main() -> int:
    args = [a for a in sys.argv[1:]]
    cmd = args[0] if args else "help"

    from config.settings import settings

    if cmd == "init":
        from src.db import init_db
        init_db()
        log.info("Database initialised.")

    elif cmd == "produce":
        from src import producer
        events = producer.generate_events(
            count=settings.kafka.event_count,
            corruption_rate=settings.kafka.corruption_rate,
            duplicate_rate=settings.kafka.duplicate_rate,
            stream_span_hours=settings.kafka.stream_span_hours,
        )
        n = producer.produce(events, settings.kafka.topic, settings.kafka.bootstrap_servers)
        log.info("Produced %d events (of %d generated) to %s", n, len(events), settings.kafka.topic)

    elif cmd == "bronze":
        from src import bronze
        n = bronze.consume_land()
        log.info("Bronze: %d events landed", n)

    elif cmd == "silver":
        from src import silver
        counts = silver.run_silver()
        log.info("Silver: %s", counts)

    elif cmd == "gold":
        from src import gold
        counts = gold.run_gold()
        log.info("Gold: %s", counts)

    elif cmd == "dq":
        from src import dq
        results = dq.run_checks()
        for r in results:
            print(f"  [{r['status']:4s}] {r['check_name']}: {r['detail']}")

    elif cmd == "export":
        from src import exports
        written = exports.export_all()
        for name, path in written.items():
            print(f"  {name}: {path}")

    elif cmd == "all":
        from src.orchestrate import run_pipeline
        result = run_pipeline()
        log.info("Pipeline result: %s", result)

    elif cmd in ("help", "--help", "-h"):
        print(__doc__)
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
