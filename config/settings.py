"""Central configuration for the ecommerce-pipeline project.

All runtime settings are read from environment variables with sane local
defaults, so the project runs out-of-the-box on a dev machine and can be
overridden in CI / prod via a .env file (see .env.example).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    topic: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC", "ecommerce.events"))
    consumer_group: str = field(default_factory=lambda: os.getenv("KAFKA_GROUP", "bronze-loader"))
    # How many events the producer emits per demo run.
    event_count: int = field(default_factory=lambda: int(os.getenv("EVENT_COUNT", "500")))
    # Chance (0..1) of injecting a deliberately malformed event, so the DQ
    # layer has something real to catch. Set to 0 for a clean run.
    corruption_rate: float = field(default_factory=lambda: float(os.getenv("CORRUPTION_RATE", "0.02")))
    # Re-deliver a small % of events twice to prove dedup works.
    duplicate_rate: float = field(default_factory=lambda: float(os.getenv("DUPLICATE_RATE", "0.03")))
    # Base time window (hours) the simulated stream spans.
    stream_span_hours: int = field(default_factory=lambda: int(os.getenv("STREAM_SPAN_HOURS", "72")))


@dataclass(frozen=True)
class PostgresConfig:
    host: str = field(default_factory=lambda: os.getenv("PGHOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("PGPORT", "5432")))
    dbname: str = field(default_factory=lambda: os.getenv("PGDATABASE", "ecommerce"))
    user: str = field(default_factory=lambda: os.getenv("PGUSER", "ecom"))
    password: str = field(default_factory=lambda: os.getenv("PGPASSWORD", "ecom_dev_only"))

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password}"
        )


@dataclass(frozen=True)
class Settings:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    data_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "data")
    export_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "data" / "exports")
    verbose: bool = field(default_factory=lambda: _env_bool("VERBOSE", False))

    def __post_init__(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.export_dir.mkdir(parents=True, exist_ok=True)


settings = Settings()
