from __future__ import annotations

import asyncio
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Any

import httpx
import structlog

import agents
import data_source as ds
import db
from db import PIPELINES, get_pool
from error_classifier import CRITICAL_TYPES
from ws_manager import manager

try:
    import mailer
except Exception:
    mailer = None

log = structlog.get_logger(__name__)

TICK_SECONDS = max(10, int(os.getenv("TICK_SECONDS", "45")))
FAILURE_CHANCE = float(os.getenv("FAILURE_CHANCE", "1.0"))
BATCH_TTL = int(os.getenv("BATCH_CACHE_SECONDS", "3600"))  # Google refreshes daily; avoid re-querying BigQuery
INITIAL_DELAY_SECONDS = max(0, int(os.getenv("INITIAL_DELAY_SECONDS", "1")))
DEMO_PIPELINE = "trends_ingest"
ROUTINE_TYPES = [
    "schema_mismatch",
    "null_threshold_exceeded",
    "data_type_mismatch",
    "duplicate_records",
    "invalid_range",
]
ALLOWED_TYPES = ROUTINE_TYPES + ["row_count_anomaly"]

_client: httpx.AsyncClient | None = None
_task: asyncio.Task | None = None
_last_error: str | None = None
_last_tick: str | None = None
_ticks = 0
_rows_in_batch = 0
_bigquery_ok: bool | None = None
_alerts_enabled = os.getenv("ALERTS_DEFAULT", "true").lower() == "true"
_batch_cache: dict[str, Any] = {"rows": None, "ts": 0.0}
_batch_lock = asyncio.Lock()
_held: dict[str, list[dict[str, Any]]] = {}
_last_good: dict[str, list[dict[str, Any]]] = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _pipeline(pipeline_id: str) -> dict[str, Any]:
    return next((p for p in PIPELINES if p.get("id") == pipeline_id),
                {"id": pipeline_id, "dag_id": pipeline_id, "name": pipeline_id})


async def _broadcast(event_type: str, payload: dict[str, Any]) -> None:
    try:
        await manager.broadcast(event_type, _jsonable(payload))
    except Exception as exc:
        log.warning("websocket_broadcast_failed", error=str(exc))


async def _ensure_state(conn) -> None:
    await conn.execute(
        """INSERT INTO public.orchestrator_state(id, failure_count)
           VALUES (1, 0) ON CONFLICT (id) DO NOTHING"""
    )


async def _next_failure_number(conn) -> int:
    await _ensure_state(conn)
    row = await conn.fetchrow(
        """UPDATE public.orchestrator_state
           SET failure_count = CASE WHEN failure_count >= 7 THEN 0 ELSE failure_count + 1 END
           WHERE id=1 RETURNING failure_count"""
    )
    return 8 if int(row["failure_count"]) == 0 else int(row["failure_count"])


async def _load_good(conn, pipeline_id: str) -> list[dict[str, Any]]:
    if pipeline_id in _last_good and _last_good[pipeline_id]:
        return [dict(r) for r in _last_good[pipeline_id]]
    try:
        rows = await conn.fetch(
            """SELECT row_data FROM public.pipeline_good_batches
               WHERE pipeline_id=$1 LIMIT 1""",
            pipeline_id,
        )
        if rows:
            value = rows[0]["row_data"]
            if isinstance(value, str):
                value = json.loads(value)
            if isinstance(value, list):
                _last_good[pipeline_id] = [dict(r) for r in value]
                return [dict(r) for r in value]
    except Exception:
        pass
    return []


async def _save_good(conn, pipeline_id: str, rows: list[dict[str, Any]]) -> None:
    clean = _jsonable([dict(r) for r in rows])
    await conn.execute(
        """CREATE TABLE IF NOT EXISTS public.pipeline_good_batches (
            pipeline_id TEXT PRIMARY KEY,
            row_data JSONB NOT NULL,
            row_count INTEGER NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"""
    )
    await conn.execute(
        """INSERT INTO public.pipeline_good_batches
           (pipeline_id,row_data,row_count,created_at)
           VALUES ($1,$2::jsonb,$3,NOW())
           ON CONFLICT (pipeline_id) DO UPDATE SET
           row_data=EXCLUDED.row_data,row_count=EXCLUDED.row_count,created_at=NOW()""",
        pipeline_id, json.dumps(clean), len(clean),
    )
    _last_good[pipeline_id] = [dict(r) for r in rows]


async def _has_open_failure(conn, pipeline_id: str) -> bool:
    # Historical approval incidents must not block autonomous processing.
    return False


async def _insert_audit(
    conn,
    pipeline_id: str,
    error_type: str,
    status: str,
    findings: list[dict[str, Any]],
    diagnosis: dict[str, Any],
    error_log: str,
    rows_affected: int,
) -> str:
    alternatives = diagnosis.get("alternatives", [])
    row = await conn.fetchrow(
        """INSERT INTO public.audit_log
           (pipeline_id,error_type,description,proposed_fix,auto_fixable,status,
            root_cause,action,explanation,error_log,model,confidence,rows_affected,
            recommendation,downstream_impact,risk_if_approved,risk_if_rejected,
            alternatives,created_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,NOW())
           RETURNING id""",
        pipeline_id,
        error_type,
        "; ".join(str(f.get("detail", "")) for f in findings)[:1000],
        str(diagnosis.get("explanation", ""))[:1000],
        error_type not in CRITICAL_TYPES,
        status,
        str(diagnosis.get("root_cause", ""))[:1000],
        str(diagnosis.get("action", "")),
        str(diagnosis.get("explanation", ""))[:1000],
        error_log[:4000],
        str(diagnosis.get("model", "rule-based-fallback")),
        float(diagnosis.get("confidence", 0.7)),
        rows_affected,
        str(diagnosis.get("recommendation", "Review the incident")),
        str(diagnosis.get("downstream_impact", ""))[:1000],
        str(diagnosis.get("risk_if_approved", ""))[:1000],
        str(diagnosis.get("risk_if_rejected", ""))[:1000],
        json.dumps(_jsonable(alternatives)),
    )
    return str(row["id"])


async def _update_audit(conn, audit_id: str, status: str, action: str, explanation: str) -> None:
    await conn.execute(
        """UPDATE public.audit_log SET status=$1,action=$2,explanation=$3,
           outcome=$3,resolved_at=CASE WHEN $1 IN
           ('approved','manually_fixed','auto_fixed','contained','quarantined') THEN NOW() ELSE resolved_at END
           WHERE id=$4::text::uuid""",
        status, action, explanation, audit_id,
    )


async def _get_batch() -> list[dict[str, Any]]:
    global _bigquery_ok
    async with _batch_lock:
        cached = _batch_cache["rows"]
        if cached and time.time() - _batch_cache["ts"] < BATCH_TTL:
            return [dict(r) for r in cached]
        try:
            rows = await asyncio.to_thread(ds.fetch_batch_sync)
            _bigquery_ok = True
            if rows:
                _batch_cache.update(rows=rows, ts=time.time())
            return [dict(r) for r in rows]
        except Exception:
            _bigquery_ok = False
            if cached:
                log.warning("bigquery_failed_using_cached_batch")
                return [dict(r) for r in cached]
            raise


async def _record_run(conn, pipeline_id: str, status: str, rows: int, quarantined: int = 0) -> str:
    dag_id = _pipeline(pipeline_id)["dag_id"]
    run_id = f"scheduled__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{random.randint(100, 999)}"
    await conn.execute(
        """INSERT INTO public.pipeline_runs (dag_id,run_id,status,started_at,finished_at,rows_processed,rows_quarantined)
           VALUES ($1,$2,$3, now() - interval '4 seconds', now(), $4, $5)""",
        dag_id, run_id, status, rows, quarantined)
    return run_id


async def _set_status(conn, pipeline_id: str, status: str) -> None:
    await conn.execute(
        "UPDATE public.pipelines SET status=$2, next_run=now() + interval '1 hour' WHERE id=$1",
        pipeline_id, status)


async def _inject_and_process(pipeline_id: str, forced_error_type: str | None = None,
                              count_cycle: bool = True) -> dict[str, Any]:
    global _rows_in_batch, _last_error
    pipeline = _pipeline(pipeline_id)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await _get_batch()
        _rows_in_batch = len(rows)
        if not rows:
            raise RuntimeError("BigQuery returned no rows. Check GOOGLE_CREDENTIALS_JSON, BigQuery API access, public Google Trends dataset, and query date window.")

        good = await _load_good(conn, pipeline_id)
        if not good:
            await ds.load(conn, rows)
            await _save_good(conn, pipeline_id, rows)
            good = rows

        if not forced_error_type and random.random() > FAILURE_CHANCE:   # healthy run
            await ds.load(conn, rows)
            await _save_good(conn, pipeline_id, rows)
            await _record_run(conn, pipeline_id, "success", len(rows), 0)
            await _set_status(conn, pipeline_id, "healthy")
            await _broadcast("pipeline_success", {"pipeline_id": pipeline_id, "status": "healthy"})
            return {"pipeline_id": pipeline_id, "status": "healthy", "rows_processed": len(rows)}

        if forced_error_type:
            error_type = forced_error_type
            failure_number = 8 if error_type == "row_count_anomaly" else 1
            severity = "critical" if error_type == "row_count_anomaly" else "routine"
        else:
            failure_number = await _next_failure_number(conn) if count_cycle else 1
            severity = "critical" if failure_number == 8 else "routine"
            error_type = None

        plan = await agents.generate_failure(
            _client, pipeline, len(rows), severity, failure_number
        )
        if forced_error_type:
            plan["error_type"] = forced_error_type
        if severity == "critical":
            plan["error_type"] = "row_count_anomaly"
        error_type = plan["error_type"]
        bad_rows = ds.inject(
            error_type, rows, random.Random(), plan.get("fraction")
        )
        if error_type == "row_count_anomaly":
            bad_rows = random.sample(rows, max(1, int(len(rows) * 0.4)))
        await conn.execute(
            """INSERT INTO public.pipeline_working_batches(pipeline_id,row_data,error_type,updated_at)
               VALUES($1,$2::jsonb,$3,NOW()) ON CONFLICT(pipeline_id)
               DO UPDATE SET row_data=EXCLUDED.row_data,error_type=EXCLUDED.error_type,updated_at=NOW()""",
            pipeline_id, json.dumps(_jsonable(bad_rows)), error_type,
        )
        findings = ds.validate(bad_rows, len(good))
        if not findings:
            findings = [{"type": error_type, "detail": f"Controlled {error_type} injected for incident testing."}]
        error_log, log_model = await agents.write_error_log(
            _client, pipeline, error_type, findings, len(bad_rows),
            str(plan.get("scenario", "")),
        )
        root_analysis = await agents.diagnose_with_gemini(_client, error_type, error_log, findings)
        diagnosis = await agents.remediation_agent(
            _client, error_type, error_log, findings, root_analysis
        )
        if root_analysis.get("root_cause"):
            diagnosis["root_cause"] = str(root_analysis["root_cause"])[:500]
            diagnosis["diagnostic_model"] = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
        diagnosis["model"] = diagnosis.get("model") or log_model
        diagnosis["action"] = diagnosis.get("action") or ds.ALLOWED_ACTION[error_type]
        critical = error_type in CRITICAL_TYPES
        rows_affected = abs(len(rows) - len(bad_rows))
        if not rows_affected:
            rows_affected = sum(1 for f in findings if f.get("type") == error_type)
        audit_id = await _insert_audit(
            conn, pipeline_id, error_type,
            "detected",
            findings, diagnosis, error_log, rows_affected,
        )

        # Agent 3 supplies the diagnosis and proposed action; only a fixed allowlist
        # can execute. Critical source-volume loss must never publish a partial batch.
        if critical:
            previous = await _load_good(conn, pipeline_id)
            if previous:
                await ds.load(conn, previous)
                await _update_audit(
                    conn, audit_id, "contained", "reload_last_good_batch",
                    "Autonomously isolated the incomplete incoming batch and restored "
                    "the last validated snapshot. Source freshness may be reduced; "
                    "no partial source data was published.",
                )
                await _record_run(conn, pipeline_id, "success", len(previous), len(bad_rows))
                await _set_status(conn, pipeline_id, "warning")
                await _broadcast("autonomous_containment", {
                    "pipeline_id": pipeline_id, "audit_id": audit_id,
                    "status": "contained", "action": "reload_last_good_batch",
                })
                return {"pipeline_id": pipeline_id, "audit_id": audit_id,
                        "status": "contained", "error_type": error_type,
                        "rows_processed": len(previous), "rows_quarantined": len(bad_rows)}
            await _update_audit(conn, audit_id, "quarantined", "quarantine_batch",
                                "No validated snapshot exists; unsafe batch isolated without publishing.")
            await _record_run(conn, pipeline_id, "failed", 0, len(bad_rows))
            await _set_status(conn, pipeline_id, "critical")
            await _broadcast("autonomous_containment", {
                "pipeline_id": pipeline_id, "audit_id": audit_id,
                "status": "quarantined", "action": "quarantine_batch",
            })
            return {"pipeline_id": pipeline_id, "audit_id": audit_id,
                    "status": "quarantined", "error_type": error_type}

        action = ds.ALLOWED_ACTION[error_type]
        fixed, quarantined = ds.apply_fix(action, bad_rows, good)
        remaining = ds.validate(fixed, len(good))
        if remaining:
            log.error("auto_fix_validation_failed", pipeline_id=pipeline_id, findings=remaining)
            await _update_audit(conn, audit_id, "quarantined", "quarantine_batch",
                                "Automatic fix failed revalidation; unsafe batch isolated.")
            await _record_run(conn, pipeline_id, "failed", len(bad_rows), quarantined)
            await _set_status(conn, pipeline_id, "critical")
            await _broadcast("autonomous_containment", {
                "pipeline_id": pipeline_id, "audit_id": audit_id,
                "status": "quarantined", "action": "quarantine_batch",
            })
            return {"pipeline_id": pipeline_id, "audit_id": audit_id,
                    "status": "quarantined", "error_type": error_type}
        await ds.load(conn, fixed)
        await _save_good(conn, pipeline_id, fixed)
        await _update_audit(
            conn, audit_id, "auto_fixed", action,
            f"Automatically applied whitelisted action '{action}'. Quarantined {quarantined} rows.",
        )
        await _record_run(conn, pipeline_id, "success", len(fixed), quarantined)
        await _set_status(conn, pipeline_id, "healthy")
        await _broadcast("auto_fixed", {
            "pipeline_id": pipeline_id, "audit_id": audit_id,
            "error_type": error_type, "status": "auto_fixed",
            "action": action, "rows_affected": rows_affected,
        })
        return {"pipeline_id": pipeline_id, "audit_id": audit_id,
                "status": "auto_fixed", "error_type": error_type,
                "rows_processed": len(fixed), "rows_quarantined": quarantined}


async def run_pipeline_once(pipeline_id: str) -> dict[str, Any]:
    global _last_error
    try:
        result = await _inject_and_process(pipeline_id)
        _last_error = None
        return result
    except Exception as exc:
        _last_error = str(exc)
        log.exception("pipeline_run_failed", pipeline_id=pipeline_id, error=str(exc))
        return {"pipeline_id": pipeline_id, "status": "error", "error": str(exc)}


async def _sync_state_on_boot() -> None:
    """Legacy approval records are historical; they no longer block the scheduler."""
    return None


async def scheduler_loop() -> None:
    global _ticks, _last_tick, _last_error
    await asyncio.sleep(INITIAL_DELAY_SECONDS)
    try:
        await _sync_state_on_boot()
    except Exception as exc:
        log.warning("boot_sync_failed", error=str(exc))
    log.info("orchestrator_started", tick_seconds=TICK_SECONDS)
    while True:
        try:
            _ticks += 1
            _last_tick = _now()
            # Run each pipeline on every tick, but serialize writes to shared
            # bronze/silver/gold tables to avoid competing TRUNCATE/UPSERT operations.
            for pipeline in PIPELINES:
                result = await run_pipeline_once(pipeline["id"])
                if result.get("status") == "error":
                    log.error("pipeline_tick_exception", pipeline_id=pipeline["id"],
                              error=result.get("error"))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("scheduler_tick_failed", error=str(exc))
            _last_error = str(exc)
        await asyncio.sleep(TICK_SECONDS)


def start() -> None:
    global _client, _task
    if _client is None:
        _client = httpx.AsyncClient(timeout=60.0)
    if _task is None or _task.done():
        _task = asyncio.create_task(scheduler_loop())


async def stop() -> None:
    global _client, _task
    if _task is not None:
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
        _task = None
    if _client is not None:
        await _client.aclose()
        _client = None


def diagnostics() -> dict[str, Any]:
    return {
        "ticks": _ticks,
        "last_tick": _last_tick,
        "last_error": _last_error,
        "bigquery_ok": _bigquery_ok,
        "rows_in_batch": _rows_in_batch,
        "held_critical_events": 0,
        "failure_cycle": "7 routine incidents, 1 critical incident per 8 injected incidents",
        "scheduler_running": _task is not None and not _task.done(),
        "gemini": agents.MODEL_STATUS["gemini"],
    }


def get_alerts_enabled() -> bool:
    return _alerts_enabled


def set_alerts_enabled(value: bool) -> None:
    global _alerts_enabled
    _alerts_enabled = bool(value)


async def trigger_demo(error_type: str = "row_count_anomaly") -> dict[str, Any]:
    if error_type not in ALLOWED_TYPES:
        raise ValueError(f"Unsupported error type: {error_type}")
    return await _inject_and_process(DEMO_PIPELINE, forced_error_type=error_type,
                                     count_cycle=False)
