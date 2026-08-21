"""Prefect orchestration — the pipeline as a DAG of flows.

Each stage is a Prefect flow with retries and a task that surfaces failures
instead of swallowing them (no silent success).  In production you would run
this on a Prefect server / scheduled deployment; here ``run_demo`` executes
the whole chain synchronously for a local end-to-end run.
"""
from __future__ import annotations

import logging

from prefect import flow, task

from src import bronze, exports, gold, producer, silver
from src.db import init_db

log = logging.getLogger(__name__)


@task(retries=3, retry_delay_seconds=5)
def task_init_db() -> None:
    init_db()


@task(retries=3, retry_delay_seconds=5)
def task_produce() -> int:
    from config.settings import settings
    events = producer.generate_events(
        count=settings.kafka.event_count,
        corruption_rate=settings.kafka.corruption_rate,
        duplicate_rate=settings.kafka.duplicate_rate,
        stream_span_hours=settings.kafka.stream_span_hours,
    )
    return producer.produce(events, settings.kafka.topic, settings.kafka.bootstrap_servers)


@task(retries=3, retry_delay_seconds=5)
def task_bronze() -> int:
    return bronze.consume_land()


@task
def task_silver() -> dict:
    return silver.run_silver()


@task
def task_gold() -> dict:
    return gold.run_gold()


@task
def task_dq() -> list[dict]:
    return dq_check()

def dq_check() -> list[dict]:
    from src import dq
    return dq.run_checks()


@task
def task_export() -> dict:
    return exports.export_all()


@flow(name="ecommerce-pipeline", log_prints=True)
def run_pipeline() -> dict:
    """End-to-end run: produce -> bronze -> silver -> gold -> dq -> export."""
    init_db()
    produced = task_produce()
    landed = task_bronze()
    silver_counts = task_silver()
    gold_counts = task_gold()
    dq_results = task_dq()
    exports_written = task_export()
    return {
        "produced": produced,
        "landed": landed,
        "silver": silver_counts,
        "gold": gold_counts,
        "dq": dq_results,
        "exports": {k: str(v) for k, v in exports_written.items()},
    }