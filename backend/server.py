from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import structlog
from dotenv import load_dotenv
from fastapi import APIRouter, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

import agents
import db
import orchestrator
from error_classifier import ERROR_PLAYBOOK
from ws_manager import manager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO)
)

log = structlog.get_logger("pipeline_autopilot")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    orchestrator.start()
    log.info("api_started")

    yield

    await orchestrator.stop()


app = FastAPI(
    title="Pipeline Autopilot API",
    lifespan=lifespan,
)

api = APIRouter(prefix="/api")


class ApprovalRequest(BaseModel):
    audit_id: str
    approved_by: str = "operator"


class RejectRequest(BaseModel):
    audit_id: str
    rejected_by: str = "operator"
    reason: str = ""


class ManualFixRequest(BaseModel):
    audit_id: str
    fixed_by: str = "operator"
    action: str | None = None
    instruction: str | None = None


class AlertToggle(BaseModel):
    enabled: bool


def _row(row) -> dict | None:
    return dict(row) if row is not None else None


@api.get("/health")
async def health():
    pool = await db.get_pool()

    async with pool.acquire() as conn:
        await conn.fetchval("SELECT 1")

    return {
        "status": "healthy",
        "service": "pipeline-autopilot",
    }


@api.get("/debug")
async def debug():
    return {
        "groq_key_set": bool(os.getenv("GROQ_API_KEY")),
        "groq_model": os.getenv("GROQ_MODEL", "(not set - default llama-3.3-70b-versatile)"),
        "groq": agents.GROQ_STATUS,
        "google_creds_set": bool(os.getenv("GOOGLE_CREDENTIALS_JSON")),
        "alerts_enabled": orchestrator.get_alerts_enabled(),
        **orchestrator.diagnostics(),
    }


@api.get("/")
async def root():
    return {
        "service": "pipeline-autopilot",
        "status": "online",
    }


@api.get("/playbook")
async def get_playbook():
    return ERROR_PLAYBOOK


@api.get("/pipelines")
async def list_pipelines():
    pool = await db.get_pool()

    async with pool.acquire() as conn:
        pipelines = await conn.fetch(
            "SELECT * FROM public.pipelines ORDER BY name"
        )

        result = []

        for pipeline in pipelines:
            last = await conn.fetchrow(
                """
                SELECT
                    status,
                    started_at,
                    finished_at,
                    rows_processed,
                    rows_quarantined,
                    run_id
                FROM public.pipeline_runs
                WHERE dag_id=$1
                ORDER BY started_at DESC
                LIMIT 1
                """,
                pipeline["dag_id"],
            )

            total_runs = await conn.fetchval(
                """
                SELECT count(*)
                FROM public.pipeline_runs
                WHERE dag_id=$1
                """,
                pipeline["dag_id"],
            )

            needs_approval = await conn.fetchval(
                """
                SELECT count(*)
                FROM public.audit_log
                WHERE pipeline_id=$1
                  AND status IN ('pending_approval','rejected')
                """,
                pipeline["id"],
            )

            item = dict(pipeline)
            item["last_run"] = _row(last)
            item["total_runs"] = total_runs
            item["needs_approval"] = bool(needs_approval)

            result.append(item)

        return result


@api.get("/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: str):
    pool = await db.get_pool()

    async with pool.acquire() as conn:
        pipeline = await conn.fetchrow(
            "SELECT * FROM public.pipelines WHERE id=$1",
            pipeline_id,
        )

        if pipeline is None:
            raise HTTPException(
                status_code=404,
                detail="pipeline not found",
            )

        runs = await conn.fetch(
            """
            SELECT *
            FROM public.pipeline_runs
            WHERE dag_id=$1
            ORDER BY started_at DESC
            LIMIT 15
            """,
            pipeline["dag_id"],
        )

        item = dict(pipeline)
        item["runs"] = [dict(run) for run in runs]

        return item


@api.get("/pipelines/{pipeline_id}/failures")
async def get_failures(pipeline_id: str):
    pool = await db.get_pool()

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT *
            FROM public.audit_log
            WHERE pipeline_id=$1
              AND status IN ('pending_approval','rejected')
            ORDER BY created_at DESC
            """,
            pipeline_id,
        )

        return [dict(row) for row in rows]


@api.post("/pipelines/{pipeline_id}/approve")
async def approve(
    pipeline_id: str,
    request: ApprovalRequest,
):
    try:
        return await orchestrator.approve_fix(
            pipeline_id,
            request.audit_id,
            request.approved_by,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        )


@api.post("/pipelines/{pipeline_id}/reject")
async def reject(
    pipeline_id: str,
    request: RejectRequest,
):
    try:
        return await orchestrator.reject_fix(
            pipeline_id,
            request.audit_id,
            request.rejected_by,
            request.reason,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        )


@api.post("/pipelines/{pipeline_id}/manual-fix")
async def manual_fix(
    pipeline_id: str,
    request: ManualFixRequest,
):
    try:
        return await orchestrator.manual_fix(
            pipeline_id,
            request.audit_id,
            request.fixed_by,
            request.action,
            request.instruction,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        )


@api.get("/audit")
async def get_audit(
    pipeline_id: str | None = Query(None),
    status: str | None = Query(None),
    since: str | None = Query(None),
):
    pool = await db.get_pool()

    clauses = []
    args = []

    if pipeline_id:
        args.append(pipeline_id)
        clauses.append(f"pipeline_id=${len(args)}")

    if status:
        args.append(status)
        clauses.append(f"status=${len(args)}")

    if since:
        try:
            parsed = datetime.fromisoformat(
                since.replace("Z", "+00:00")
            )
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="invalid 'since' timestamp",
            )

        args.append(parsed)
        clauses.append(f"created_at >= ${len(args)}")

    where = (
        "WHERE " + " AND ".join(clauses)
        if clauses
        else ""
    )

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT *
            FROM public.audit_log
            {where}
            ORDER BY created_at DESC
            LIMIT 500
            """,
            *args,
        )

        return [dict(row) for row in rows]


@api.get("/pipelines/{pipeline_id}/lineage")
async def get_lineage(pipeline_id: str):
    pool = await db.get_pool()

    async with pool.acquire() as conn:
        counts = {}

        for table in [
            "bronze.raw_trends",
            "silver.trends_clean",
            "gold.trending_terms",
            "gold.region_leaders",
        ]:
            try:
                counts[table] = await conn.fetchval(
                    f"SELECT count(*) FROM {table}"
                )
            except Exception:
                counts[table] = 0

        pipeline = await conn.fetchrow(
            "SELECT status FROM public.pipelines WHERE id=$1",
            pipeline_id,
        )

    health = pipeline["status"] if pipeline else "healthy"

    def node(
        node_id,
        label,
        table,
        layer,
        x,
        y,
    ):
        return {
            "id": node_id,
            "label": label,
            "rows": counts.get(table, 0),
            "layer": layer,
            "x": x,
            "y": y,
            "health": health if layer != "source" else "healthy",
        }

    nodes = [
        node(
            "src",
            "Google Trends (BigQuery)",
            None,
            "source",
            0,
            120,
        ),
        node(
            "b_trends",
            "bronze.raw_trends",
            "bronze.raw_trends",
            "bronze",
            260,
            120,
        ),
        node(
            "s_trends",
            "silver.trends_clean",
            "silver.trends_clean",
            "silver",
            540,
            120,
        ),
        node(
            "g_terms",
            "gold.trending_terms",
            "gold.trending_terms",
            "gold",
            820,
            40,
        ),
        node(
            "g_region",
            "gold.region_leaders",
            "gold.region_leaders",
            "gold",
            820,
            200,
        ),
    ]

    edges = [
        ["src", "b_trends"],
        ["b_trends", "s_trends"],
        ["s_trends", "g_terms"],
        ["s_trends", "g_region"],
    ]

    return {
        "nodes": nodes,
        "edges": [
            {
                "source": source,
                "target": target,
            }
            for source, target in edges
        ],
    }


@api.post("/demo/trigger")
async def demo_trigger(
    error_type: str = Query("row_count_anomaly"),
):
    try:
        return await orchestrator.trigger_demo(error_type)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        )


@api.get("/alerts/status")
async def get_alert_status():
    return {
        "enabled": orchestrator.get_alerts_enabled(),
    }


@api.post("/alerts/toggle")
async def toggle_alerts(body: AlertToggle):
    orchestrator.set_alerts_enabled(body.enabled)

    return {
        "enabled": body.enabled,
    }


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
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
