"""Pipeline Autopilot — FastAPI Control Tower API (live preview build).

Serves pipeline status, failures + proposed fixes, approve/reject healing
actions, the audit log, data lineage, and a WebSocket stream of live updates.
Backed by PostgreSQL; orchestration is simulated by orchestrator.py.
"""
from __future__ import annotations
import logging
from contextlib import asynccontextmanager
from datetime import datetime

import structlog
from fastapi import FastAPI, APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Query
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from pathlib import Path

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

import db
import orchestrator
from error_classifier import ERROR_PLAYBOOK
from ws_manager import manager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.INFO))
log = structlog.get_logger("pipeline_autopilot")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    orchestrator.start()
    log.info("api_started")
    yield
    await orchestrator.stop()


app = FastAPI(title="Pipeline Autopilot API", lifespan=lifespan)
api = APIRouter(prefix="/api")


# ---------- models ----------
class ApprovalRequest(BaseModel):
    audit_id: str
    approved_by: str = "operator"


class RejectRequest(BaseModel):
    audit_id: str
    rejected_by: str = "operator"
    reason: str = ""


# ---------- helpers ----------
def _row(r) -> dict:
    return dict(r) if r is not None else None


# ---------- routes ----------
@api.get("/")
async def root():
    return {"service": "pipeline-autopilot", "status": "online"}


@api.get("/playbook")
async def get_playbook():
    return ERROR_PLAYBOOK


@api.get("/pipelines")
async def list_pipelines():
    pool = await db.get_pool()
    async with pool.acquire() as conn:
        pipelines = await conn.fetch("SELECT * FROM public.pipelines ORDER BY name")
        result = []
        for p in pipelines:
            last = await conn.fetchrow(
                """SELECT status, started_at, finished_at, rows_processed, rows_quarantined, run_id
                   FROM public.pipeline_runs WHERE dag_id=$1 ORDER BY started_at DESC LIMIT 1""",
                p["dag_id"])
            pending = await conn.fetchval(
                "SELECT count(*) FROM public.pipeline_runs WHERE dag_id=$1", p["dag_id"])
            needs = await conn.fetchval(
                "SELECT count(*) FROM public.audit_log WHERE pipeline_id=$1 AND status='pending_approval'",
                p["id"])
            item = dict(p)
            item["last_run"] = _row(last)
            item["total_runs"] = pending
            item["needs_approval"] = bool(needs)
            result.append(item)
        return result


@api.get("/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: str):
    pool = await db.get_pool()
    async with pool.acquire() as conn:
        p = await conn.fetchrow("SELECT * FROM public.pipelines WHERE id=$1", pipeline_id)
        if p is None:
            raise HTTPException(404, "pipeline not found")
        runs = await conn.fetch(
            """SELECT * FROM public.pipeline_runs WHERE dag_id=$1 ORDER BY started_at DESC LIMIT 15""",
            p["dag_id"])
        item = dict(p)
        item["runs"] = [dict(r) for r in runs]
        return item


@api.get("/pipelines/{pipeline_id}/failures")
async def get_failures(pipeline_id: str):
    pool = await db.get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM public.audit_log
               WHERE pipeline_id=$1 AND status='pending_approval'
               ORDER BY created_at DESC""",
            pipeline_id)
        return [dict(r) for r in rows]


@api.post("/pipelines/{pipeline_id}/approve")
async def approve(pipeline_id: str, req: ApprovalRequest):
    try:
        return await orchestrator.approve_fix(pipeline_id, req.audit_id, req.approved_by)
    except ValueError as e:
        raise HTTPException(404, str(e))


@api.post("/pipelines/{pipeline_id}/reject")
async def reject(pipeline_id: str, req: RejectRequest):
    try:
        return await orchestrator.reject_fix(pipeline_id, req.audit_id, req.rejected_by, req.reason)
    except ValueError as e:
        raise HTTPException(404, str(e))


@api.get("/audit")
async def get_audit(
    pipeline_id: str | None = Query(None),
    status: str | None = Query(None),
    since: str | None = Query(None),
):
    pool = await db.get_pool()
    clauses, args = [], []
    if pipeline_id:
        args.append(pipeline_id); clauses.append(f"pipeline_id=${len(args)}")
    if status:
        args.append(status); clauses.append(f"status=${len(args)}")
    if since:
        try:
            dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(400, "invalid 'since' timestamp (use ISO 8601)")
        args.append(dt); clauses.append(f"created_at >= ${len(args)}")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT * FROM public.audit_log {where} ORDER BY created_at DESC LIMIT 500", *args)
        return [dict(r) for r in rows]


@api.get("/pipelines/{pipeline_id}/lineage")
async def get_lineage(pipeline_id: str):
    """Return a node/edge graph (bronze → silver → gold) for React Flow."""
    pool = await db.get_pool()
    async with pool.acquire() as conn:
        counts = {}
        for tbl in ["bronze.raw_orders", "bronze.raw_customers", "bronze.raw_products",
                    "silver.orders_clean", "silver.customers_clean",
                    "gold.daily_revenue", "gold.customer_segments"]:
            try:
                counts[tbl] = await conn.fetchval(f"SELECT count(*) FROM {tbl}")
            except Exception:
                counts[tbl] = 0
        pipe = await conn.fetchrow("SELECT status FROM public.pipelines WHERE id=$1", pipeline_id)
    health = pipe["status"] if pipe else "healthy"

    def node(nid, label, table, layer, x, y):
        return {"id": nid, "label": label, "rows": counts.get(table, 0), "layer": layer,
                "x": x, "y": y, "health": health if layer != "source" else "healthy"}

    nodes = [
        node("src_csv", "Olist CSVs", None, "source", 0, 120),
        node("b_orders", "bronze.raw_orders", "bronze.raw_orders", "bronze", 260, 20),
        node("b_customers", "bronze.raw_customers", "bronze.raw_customers", "bronze", 260, 120),
        node("b_products", "bronze.raw_products", "bronze.raw_products", "bronze", 260, 220),
        node("s_orders", "silver.orders_clean", "silver.orders_clean", "silver", 540, 40),
        node("s_customers", "silver.customers_clean", "silver.customers_clean", "silver", 540, 180),
        node("g_revenue", "gold.daily_revenue", "gold.daily_revenue", "gold", 820, 40),
        node("g_segments", "gold.customer_segments", "gold.customer_segments", "gold", 820, 180),
    ]
    edges = [
        ["src_csv", "b_orders"], ["src_csv", "b_customers"], ["src_csv", "b_products"],
        ["b_orders", "s_orders"], ["b_customers", "s_customers"],
        ["s_orders", "g_revenue"], ["s_customers", "g_segments"], ["s_orders", "g_segments"],
    ]
    return {"nodes": nodes, "edges": [{"source": s, "target": t} for s, t in edges]}


@app.websocket("/api/ws/pipelines")
async def ws_pipelines(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()  # keepalive / client pings
    except WebSocketDisconnect:
        await manager.disconnect(ws)
    except Exception:
        await manager.disconnect(ws)


app.include_router(api)
app.add_middleware(
    CORSMiddleware, allow_credentials=True, allow_origins=["*"],
    allow_methods=["*"], allow_headers=["*"],
)

# ---------- alert toggle ----------
class AlertToggle(BaseModel):
    enabled: bool

@api.get("/alerts/status")
async def get_alert_status():
    return {"enabled": orchestrator.get_alerts_enabled()}

@api.post("/alerts/toggle")
async def toggle_alerts(body: AlertToggle):
    orchestrator.set_alerts_enabled(body.enabled)
    return {"enabled": body.enabled}