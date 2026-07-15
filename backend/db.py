"""PostgreSQL access layer + schema bootstrap + synthetic Olist seed data."""
from __future__ import annotations
import os
import random
from datetime import datetime, timezone, timedelta

import psycopg
from psycopg_pool import AsyncConnectionPool
import structlog

log = structlog.get_logger(__name__)

_pool: AsyncConnectionPool | None = None

DATABASE_URL = os.environ["DATABASE_URL"]

PIPELINES = [
    {
        "id": "olist_ingest",
        "dag_id": "ingest_dag",
        "name": "Ingest · Olist CSVs",
        "layer": "bronze",
        "description": "Pulls Olist orders/customers/products CSVs into the bronze schema.",
        "schedule": "@hourly",
    },
    {
        "id": "olist_validate",
        "dag_id": "validate_dag",
        "name": "Validate · Great Expectations",
        "layer": "bronze",
        "description": "Runs data-quality checks (nulls, schema, value ranges) on bronze.",
        "schedule": "@hourly",
    },
    {
        "id": "olist_transform",
        "dag_id": "transform_dag",
        "name": "Transform · Bronze → Silver → Gold",
        "layer": "gold",
        "description": "Cleans and aggregates data into silver and gold marts.",
        "schedule": "@daily",
    },
]

_STATES = ["SP", "RJ", "MG", "BA", "RS", "PR", "SC", "PE", "CE", "GO"]
_CITIES = ["sao paulo", "rio de janeiro", "belo horizonte", "salvador", "porto alegre",
           "curitiba", "florianopolis", "recife", "fortaleza", "goiania"]
_CATEGORIES = ["cama_mesa_banho", "beleza_saude", "esporte_lazer", "moveis_decoracao",
               "informatica_acessorios", "utilidades_domesticas", "relogios_presentes"]
_STATUSES = ["delivered", "shipped", "canceled", "processing", "invoiced"]

SCHEMA_DDL = """
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.raw_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_status TEXT,
    purchase_timestamp TIMESTAMPTZ,
    price NUMERIC
);
CREATE TABLE IF NOT EXISTS bronze.raw_customers (
    customer_id TEXT PRIMARY KEY,
    customer_city TEXT,
    customer_state TEXT,
    zip_code TEXT
);
CREATE TABLE IF NOT EXISTS bronze.raw_products (
    product_id TEXT PRIMARY KEY,
    product_category TEXT,
    price NUMERIC
);
CREATE TABLE IF NOT EXISTS silver.orders_clean (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_status TEXT,
    purchase_timestamp TIMESTAMPTZ,
    price NUMERIC
);
CREATE TABLE IF NOT EXISTS silver.customers_clean (
    customer_id TEXT PRIMARY KEY,
    customer_city TEXT,
    customer_state TEXT,
    zip_code TEXT
);
CREATE TABLE IF NOT EXISTS gold.daily_revenue (
    revenue_date DATE PRIMARY KEY,
    total_revenue NUMERIC,
    order_count INT
);
CREATE TABLE IF NOT EXISTS gold.customer_segments (
    segment TEXT PRIMARY KEY,
    customer_count INT,
    avg_spend NUMERIC
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


async def get_pool() -> AsyncConnectionPool:
    assert _pool is not None, "DB pool not initialised"
    return _pool


async def init_db() -> None:
    global _pool
    
    conninfo = DATABASE_URL + "?sslmode=require"
    
    _pool = AsyncConnectionPool(
        conninfo=conninfo,
        min_size=1,
        max_size=10,
        open=False
    )
    await _pool.open()
    
    async with _pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SCHEMA_DDL)
            for p in PIPELINES:
                await cur.execute("""
                    INSERT INTO public.pipelines (id, dag_id, name, layer, description, status, schedule, next_run)
                    VALUES (%s,%s,%s,%s,%s,'healthy',%s, now() + interval '1 hour')
                    ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, description=EXCLUDED.description
                """, (p["id"], p["dag_id"], p["name"], p["layer"], p["description"], p["schedule"]))
        await conn.commit()
    
    await _seed_data()
    log.info("db_initialised")


async def _seed_data() -> None:
    assert _pool is not None
    async with _pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM bronze.raw_orders")
            row = await cur.fetchone()
            if row and row[0] > 0:
                return

            customers, orders, products = [], [], []
            for i in range(400):
                cid = f"cust_{i:04d}"
                customers.append((cid, random.choice(_CITIES), random.choice(_STATES),
                                  f"{random.randint(10000, 99999)}"))
            for i in range(120):
                pid = f"prod_{i:04d}"
                products.append((pid, random.choice(_CATEGORIES), round(random.uniform(10, 900), 2)))
            for i in range(1000):
                oid = f"order_{i:05d}"
                cid = f"cust_{random.randint(0, 399):04d}"
                ts = datetime.now(timezone.utc) - timedelta(
                    days=random.randint(0, 30), hours=random.randint(0, 23))
                orders.append((oid, cid, random.choice(_STATUSES), ts,
                               round(random.uniform(15, 1200), 2)))

            await cur.executemany(
                "INSERT INTO bronze.raw_customers VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                customers)
            await cur.executemany(
                "INSERT INTO bronze.raw_products VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                products)
            await cur.executemany(
                "INSERT INTO bronze.raw_orders VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                orders)
            await cur.execute("""
                INSERT INTO silver.orders_clean
                SELECT order_id, customer_id, order_status, purchase_timestamp, price
                FROM bronze.raw_orders WHERE price > 0 ON CONFLICT DO NOTHING""")
            await cur.execute("""
                INSERT INTO silver.customers_clean
                SELECT customer_id, customer_city, customer_state, zip_code
                FROM bronze.raw_customers ON CONFLICT DO NOTHING""")
            await cur.execute("""
                INSERT INTO gold.daily_revenue (revenue_date, total_revenue, order_count)
                SELECT date_trunc('day', purchase_timestamp)::date, sum(price), count(*)
                FROM silver.orders_clean GROUP BY 1 ON CONFLICT (revenue_date) DO NOTHING""")
            await cur.execute("""
                INSERT INTO gold.customer_segments (segment, customer_count, avg_spend)
                VALUES ('high_value', 42, 780.50), ('mid_value', 158, 320.10), ('low_value', 200, 95.75)
                ON CONFLICT DO NOTHING""")
            await conn.commit()
            log.info("seed_complete", orders=len(orders), customers=len(customers))
