"""PostgreSQL access layer + schema bootstrap."""
from __future__ import annotations
import os
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import asyncpg
import structlog

log = structlog.get_logger(__name__)

_pool: asyncpg.Pool | None = None

DATABASE_URL = os.environ["DATABASE_URL"]

SCHEMA_DDL = """
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.raw_trends (
    id TEXT PRIMARY KEY, refresh_date DATE, region TEXT, region_id TEXT, term TEXT, week DATE,
    rank INT, score DOUBLE PRECISION, ingested_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS silver.trends_clean (
    id TEXT PRIMARY KEY, refresh_date DATE, region TEXT, term TEXT, week DATE, rank INT, score DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS gold.trending_terms (
    term TEXT PRIMARY KEY, regions INT, avg_rank NUMERIC, best_rank INT
);
CREATE TABLE IF NOT EXISTS gold.region_leaders (
    region TEXT PRIMARY KEY, top_term TEXT, score DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS public.audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id TEXT NOT NULL,
    error_type TEXT NOT NULL,
    description TEXT,
    proposed_fix TEXT,
    auto_fixable BOOLEAN DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'pending_approval',
    approved_by TEXT,
    outcome TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ
);
ALTER TABLE public.audit_log ADD COLUMN IF NOT EXISTS root_cause TEXT;
ALTER TABLE public.audit_log ADD COLUMN IF NOT EXISTS action TEXT;
ALTER TABLE public.audit_log ADD COLUMN IF NOT EXISTS explanation TEXT;
ALTER TABLE public.audit_log ADD COLUMN IF NOT EXISTS error_log TEXT;
ALTER TABLE public.audit_log ADD COLUMN IF NOT EXISTS model TEXT;
ALTER TABLE public.audit_log ADD COLUMN IF NOT EXISTS confidence REAL;
ALTER TABLE public.audit_log ADD COLUMN IF NOT EXISTS rows_affected INT;
ALTER TABLE public.audit_log ADD COLUMN IF NOT EXISTS recommendation TEXT;
ALTER TABLE public.audit_log ADD COLUMN IF NOT EXISTS downstream_impact TEXT;
ALTER TABLE public.audit_log ADD COLUMN IF NOT EXISTS risk_if_approved TEXT;
ALTER TABLE public.audit_log ADD COLUMN IF NOT EXISTS risk_if_rejected TEXT;
ALTER TABLE public.audit_log ADD COLUMN IF NOT EXISTS alternatives TEXT;
CREATE TABLE IF NOT EXISTS public.pipeline_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    rows_processed INT DEFAULT 0,
    rows_quarantined INT DEFAULT 0
);
CREATE TABLE IF NOT EXISTS public.pipelines (
    id TEXT PRIMARY KEY,
    dag_id TEXT NOT NULL,
    name TEXT NOT NULL,
    layer TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'healthy',
    schedule TEXT,
    next_run TIMESTAMPTZ
);
"""

PIPELINES = [
    {"id": "trends_ingest", "dag_id": "ingest_dag", "name": "Ingest · Google Trends (BigQuery)", "layer": "bronze",
     "description": "Pulls Google Search top-25 trending terms per US region from the BigQuery public dataset into bronze.", "schedule": "@hourly"},
    {"id": "trends_validate", "dag_id": "validate_dag", "name": "Validate · Data Quality Checks", "layer": "bronze",
     "description": "Checks schema, types, nulls, duplicates, rank ranges and row counts on each batch.", "schedule": "@hourly"},
    {"id": "trends_transform", "dag_id": "transform_dag", "name": "Transform · Bronze → Silver → Gold", "layer": "gold",
     "description": "Cleans trends into silver; builds trending-terms and regional-leader gold marts.", "schedule": "@daily"},
]


def _clean_url(url: str) -> str:
    """asyncpg rejects some libpq params that Neon adds (channel_binding)."""
    p = urlparse(url)
    q = [(k, v) for k, v in parse_qsl(p.query) if k != "channel_binding"]
    return urlunparse(p._replace(query=urlencode(q)))


async def get_pool() -> asyncpg.Pool:
    assert _pool is not None, "DB pool not initialised"
    return _pool


async def init_db() -> None:
    global _pool
    _pool = await asyncpg.create_pool(_clean_url(DATABASE_URL), min_size=1, max_size=10)
    async with _pool.acquire() as conn:
        await conn.execute(SCHEMA_DDL)
        for p in PIPELINES:
            await conn.execute(
                """INSERT INTO public.pipelines (id, dag_id, name, layer, description, status, schedule, next_run)
                   VALUES ($1,$2,$3,$4,$5,'healthy',$6, now() + interval '1 hour')
                   ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, description=EXCLUDED.description""",
                p["id"], p["dag_id"], p["name"], p["layer"], p["description"], p["schedule"],
            )
    log.info("db_initialised")
