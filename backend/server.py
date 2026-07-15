"""Pipeline Autopilot — FastAPI Control Tower API."""
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
    if db._pool:
        await db._pool.close()


app = FastAPI(title="Pipeline Autopilot API", lifespan=lifespan)
api = APIRouter(prefix="/api")


class ApprovalRequest(BaseModel):
    audit_id: str
    approved_by: str = "operator"


class RejectRequest(BaseModel):
    audit_id: str
    rejected_by: str = "operator"
    reason: str = ""


class AlertToggle(BaseModel):
    enabled: bool


def _row(r) -> dict:
    return dict(r) if r is not None else None


@api.get("/")
async def root():
    return {"service": "pipeline-autopilot", "status": "online"}


@api.get("/playbook")
async def get_playbook():
    return ERROR_PLAYBOOK


@api.get("/pipelines")
async def list_pipelines():
    pool = await db.get_pool()
    result = []
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT id, dag_id, name, layer, description, status, schedule, next_run FROM public.pipelines ORDER BY name")
            pipelines = await cur.fetchall()
            for p in pipelines:
                pid, dag_id, name, layer, desc, status, schedule, next_run = p
                await cur.execute(
                    "SELECT status, started_at, finished_at, rows_processed, rows_quarantined, run_id FROM public.pipeline_runs WHERE dag_id=%s ORDER BY started_at DESC LIMIT 1",
                    (dag_id,))
                last = await cur.fetchone()
                await cur.execute("SELECT count(*) FROM public.pipeline_runs WHERE dag_id=%s", (dag_id,))
                total = (await cur.fetchone())[0]
                await cur.execute(
                    "SELECT count(*) FROM public.audit_log WHERE pipeline_id=%s AND status='pending_approval'",
                    (pid,))
                needs = (await cur.fetchone())[0]
                result.append({
                    "id": pid, "dag_id": dag_id, "name": name, "layer": layer,
                    "description": desc, "status": status, "schedule": schedule,
                    "next_run": next_run.isoformat() if next_run else None,
                    "last_run": {
                        "status": last[0], "started_at": last[1].isoformat() if last[1] else None,
                        "finished_at": last[2].isoformat() if last[2] else None,
                        "rows_processed": last[3], "rows_quarantined": last[4], "run_id": last[5]
                    } if last else None,
                    "total_runs": total,
                    "needs_approval": bool(needs),
                })
    return result


@api.get("/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: str):
    pool = await db.get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT id, dag_id, name, layer, description, status, schedule, next_run FROM public.pipelines WHERE id=%s", (pipeline_id,))
            p = await cur.fetchone()
            if p is None:
                raise HTTPException(404, "pipeline not found")
            await cur.execute(
                "SELECT id, dag_id, run_id, status, started_at, finished_at, rows_processed, rows_quarantined FROM public.pipeline_runs WHERE dag_id=%s ORDER BY started_at DESC LIMIT 15",
                (p[1],))
            runs = await cur.fetchall()
    return {
        "id": p[0], "dag_id": p[1], "name": p[2], "layer": p[3],
        "description": p[4], "status": p[5], "schedule": p[6],
        "next_run": p[7].isoformat() if p[7] else None,
        "runs": [{"id": str(r[0]), "dag_id": r[1], "run_id": r[2], "status": r[3],
                  "started_at": r[4].isoformat() if r[4] else None,
                  "finished_at": r[5].isoformat() if r[5] else None,
                  "rows_processed": r[6], "rows_quarantined": r[7]} for r in runs]
    }


@api.get("/pipelines/{pipeline_id}/failures")
async def get_failures(pipeline_id: str):
    pool = await db.get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT id, pipeline_id, error_type, description, proposed_fix, auto_fixable,
                       status, approved_by, outcome, created_at, resolved_at
                FROM public.audit_log
                WHERE pipeline_id=%s AND status='pending_approval'
                ORDER BY created_at DESC
            """, (pipeline_id,))
            rows = await cur.fetchall()
    return [{"id": str(r[0]), "pipeline_id": r[1], "error_type": r[2], "description": r[3],
             "proposed_fix": r[4], "auto_fixable": r[5], "status": r[6], "approved_by": r[7],
             "outcome": r[8], "created_at": r[9].isoformat() if r[9] else None,
             "resolved_at": r[10].isoformat() if r[10] else None} for r in rows]


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
        args.append(pipeline_id)
        clauses.append(f"pipeline_id=%s")
    if status:
        args.append(status)
        clauses.append(f"status=%s")
    if since:
        try:
            dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(400, "invalid 'since' timestamp")
        args.append(dt)
        clauses.append(f"created_at >= %s")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT id, pipeline_id, error_type, description, proposed_fix, auto_fixable, status, approved_by, outcome, created_at, resolved_at FROM public.audit_log {where} ORDER BY created_at DESC LIMIT 500",
                args)
            rows = await cur.fetchall()
    return [{"id": str(r[0]), "pipeline_id": r[1], "error_type": r[2], "description": r[3],
             "proposed_fix": r[4], "auto_fixable": r[5], "status": r[6], "approved_by": r[7],
             "outcome": r[8], "created_at": r[9].isoformat() if r[9] else None,
             "resolved_at": r[10].isoformat() if r[10] else None} for r in rows]


@api.get("/pipelines/{pipeline_id}/lineage")
async def get_lineage(pipeline_id: str):
    pool = await db.get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            counts = {}
            for tbl in ["bronze.raw_orders", "bronze.raw_customers", "bronze.raw_products",
                        "silver.orders_clean", "silver.customers_clean",
                        "gold.daily_revenue", "gold.customer_segments"]:
                try:
                    await cur.execute(f"SELECT count(*) FROM {tbl}")
                    row = await cur.fetchone()
                    counts[tbl] = row[0] if row else 0
                except Exception:
                    counts[tbl] = 0
            await cur.execute("SELECT status FROM public.pipelines WHERE id=%s", (pipeline_id,))
            pipe = await cur.fetchone()
    health = pipe[0] if pipe else "healthy"

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


@api.get("/alerts/status")
async def get_alert_status():
    return {"enabled": orchestrator.get_alerts_enabled()}


@api.post("/alerts/toggle")
async def toggle_alerts(body: AlertToggle):
    orchestrator.set_alerts_enabled(body.enabled)
    return {"enabled": body.enabled}


@app.websocket("/api/ws/pipelines")
async def ws_pipelines(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(ws)
    except Exception:
        await manager.disconnect(ws)


app.include_router(api)
app.add_middleware(
    CORSMiddleware, allow_credentials=True, allow_origins=["*"],
    allow_methods=["*"], allow_headers=["*"],
)
