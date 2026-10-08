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
FAILURE_CHANCE = float(os.getenv("FAILURE_CHANCE", "0.65"))
BATCH_TTL = int(os.getenv("BATCH_CACHE_SECONDS", "3600"))  # Google refreshes daily; avoid re-querying BigQuery
INITIAL_DELAY_SECONDS = max(0, int(os.getenv("INITIAL_DELAY_SECONDS", "5")))
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
        "SELECT failure_count FROM public.orchestrator_state WHERE id=1 FOR UPDATE"
    )
    current = int(row["failure_count"]) if row else 0
    number = 1 if current >= 8 else current + 1
    await conn.execute(
        "UPDATE public.orchestrator_state SET failure_count=$1 WHERE id=1",
        0 if number == 8 else number,
    )
    return number


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
    row = await conn.fetchrow(
        """SELECT 1 FROM public.audit_log
           WHERE pipeline_id=$1 AND status IN ('pending_approval','rejected','detected')
           LIMIT 1""",
        pipeline_id,
    )
    return bool(row)


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
           ('approved','manually_fixed','auto_fixed') THEN NOW() ELSE resolved_at END
           WHERE id=$4""",
        status, action, explanation, audit_id,
    )


async def _get_batch() -> list[dict[str, Any]]:
    """BigQuery batch, cached so we stay far inside the free 1 TB/month."""
    global _bigquery_ok
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
        if cached:  # keep running on the last batch if BigQuery hiccups
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
        if await _has_open_failure(conn, pipeline_id):
            return {"pipeline_id": pipeline_id, "status": "pending_approval",
                    "message": "This pipeline has an unresolved incident."}

        rows = await _get_batch()
        _rows_in_batch = len(rows)
        if not rows:
            raise RuntimeError("BigQuery returned no rows.")

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
        findings = ds.validate(bad_rows, len(good))
        if not findings:
            findings = [{"type": error_type, "detail": f"Controlled {error_type} injected for incident testing."}]
        error_log, log_model = await agents.write_error_log(
            _client, pipeline, error_type, findings, len(bad_rows),
            str(plan.get("scenario", "")),
        )
        diagnosis = await agents.remediation_agent(
            _client, error_type, error_log, findings
        )
        diagnosis["model"] = diagnosis.get("model") or log_model
        diagnosis["action"] = diagnosis.get("action") or ds.ALLOWED_ACTION[error_type]
        critical = error_type in CRITICAL_TYPES
        rows_affected = abs(len(rows) - len(bad_rows))
        if not rows_affected:
            rows_affected = sum(1 for f in findings if f.get("type") == error_type)
        audit_id = await _insert_audit(
            conn, pipeline_id, error_type,
            "pending_approval" if critical else "detected",
            findings, diagnosis, error_log, rows_affected,
        )

        if critical:
            await _record_run(conn, pipeline_id, "failed", len(bad_rows), 0)
            await _set_status(conn, pipeline_id, "critical")
            _held[audit_id] = [dict(r) for r in bad_rows]
            await _broadcast("critical_failure", {
                "pipeline_id": pipeline_id, "audit_id": audit_id,
                "error_type": error_type, "status": "pending_approval",
                "recommendation": diagnosis.get("recommendation"),
                "downstream_impact": diagnosis.get("downstream_impact"),
                "risk_if_approved": diagnosis.get("risk_if_approved"),
                "risk_if_rejected": diagnosis.get("risk_if_rejected"),
                "alternatives": diagnosis.get("alternatives", []),
            })
            if _alerts_enabled and mailer is not None:
                try:
                    await asyncio.to_thread(
                        mailer.send_alert,
                        pipeline["name"], pipeline_id, audit_id, error_type,
                        "; ".join(str(f.get("detail", "")) for f in findings),
                        diagnosis.get("root_cause", ""),
                        diagnosis.get("action", ""),
                        diagnosis.get("explanation", ""),
                        diagnosis.get("downstream_impact", ""),
                        diagnosis.get("risk_if_approved", ""),
                        diagnosis.get("risk_if_rejected", ""),
                    )
                except Exception as exc:
                    log.warning("critical_email_failed", error=str(exc))
            return {"pipeline_id": pipeline_id, "audit_id": audit_id,
                    "status": "pending_approval", "error_type": error_type}

        action = ds.ALLOWED_ACTION[error_type]
        fixed, quarantined = ds.apply_fix(action, bad_rows, good)
        if not fixed:
            fixed = good
        remaining = ds.validate(fixed, len(good))
        if remaining:
            log.warning("auto_fix_validation_findings", pipeline_id=pipeline_id,
                        findings=remaining)
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
    """After a restart, rebuild pipeline status from the persisted audit log (nothing starts from zero)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE public.pipelines p SET status='critical' WHERE EXISTS (
               SELECT 1 FROM public.audit_log a WHERE a.pipeline_id=p.id
               AND a.status IN ('pending_approval','rejected'))""")


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
            pipeline = PIPELINES[_ticks % len(PIPELINES)]   # one pipeline per tick, round-robin
            _ticks += 1
            _last_tick = _now()
            await run_pipeline_once(pipeline["id"])
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
        "held_critical_events": len(_held),
        "failure_cycle": "7 routine, 1 critical per 8 runs",
    }


def get_alerts_enabled() -> bool:
    return _alerts_enabled


def set_alerts_enabled(value: bool) -> None:
    global _alerts_enabled
    _alerts_enabled = bool(value)


async def approve_fix(pipeline_id: str, audit_id: str, approved_by: str) -> dict[str, Any]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM public.audit_log WHERE id=$1 AND pipeline_id=$2",
            audit_id, pipeline_id,
        )
        if not row:
            raise ValueError("Audit record not found.")
        if row["status"] != "pending_approval":
            raise ValueError("This incident is no longer awaiting approval.")
        good = await _load_good(conn, pipeline_id)
        if not good:
            raise RuntimeError("No last known-good batch is available.")
        await ds.load(conn, good)
        await _save_good(conn, pipeline_id, good)
        await _update_audit(conn, audit_id, "approved", "reload_last_good_batch",
                            f"Approved by {approved_by}; restored the last known-good batch.")
        await _record_run(conn, pipeline_id, "success", len(good), 0)
        await _set_status(conn, pipeline_id, "healthy")
    _held.pop(audit_id, None)
    await _broadcast("critical_resolved", {"pipeline_id": pipeline_id,
                    "audit_id": audit_id, "status": "approved", "approved_by": approved_by})
    return {"pipeline_id": pipeline_id, "audit_id": audit_id,
            "status": "approved", "action": "reload_last_good_batch"}


async def reject_fix(pipeline_id: str, audit_id: str, rejected_by: str,
                     reason: str = "") -> dict[str, Any]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM public.audit_log WHERE id=$1 AND pipeline_id=$2",
            audit_id, pipeline_id,
        )
        if not row:
            raise ValueError("Audit record not found.")
        if row["status"] != "pending_approval":
            raise ValueError("This incident is no longer awaiting approval.")
        await _update_audit(
            conn, audit_id, "rejected", "manual_fix_required",
            f"Recommendation rejected by {rejected_by}. {reason}".strip(),
        )
    await _broadcast("critical_rejected", {"pipeline_id": pipeline_id,
                    "audit_id": audit_id, "status": "rejected", "rejected_by": rejected_by})
    return {"pipeline_id": pipeline_id, "audit_id": audit_id,
            "status": "rejected", "manual_fix_required": True}


async def manual_fix(pipeline_id: str, audit_id: str, fixed_by: str,
                     action: str | None, instruction: str | None) -> dict[str, Any]:
    if not action and instruction:
        action = await agents.interpret_instruction(_client, instruction)
    if action not in ds.MANUAL_ACTIONS:
        raise ValueError("Instruction could not be mapped to an allowed manual action.")
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM public.audit_log WHERE id=$1 AND pipeline_id=$2",
            audit_id, pipeline_id,
        )
        if not row:
            raise ValueError("Audit record not found.")
        if row["status"] not in ("pending_approval", "rejected"):
            raise ValueError("This incident is not available for manual remediation.")
        if action in ("reload_last_good_batch", "skip_and_keep_previous"):
            rows = await _load_good(conn, pipeline_id)
            if not rows:
                raise RuntimeError("No last known-good batch is available.")
            await ds.load(conn, rows)
            await _save_good(conn, pipeline_id, rows)
        elif action == "accept_partial_batch":
            rows = _held.get(audit_id, [])
            if not rows:
                raise RuntimeError("The affected batch is unavailable; choose reload last known-good batch.")
            await ds.load(conn, rows)
            await _save_good(conn, pipeline_id, rows)
        await _update_audit(conn, audit_id, "manually_fixed", action,
                            f"Manual remediation completed by {fixed_by} using {ds.MANUAL_ACTIONS[action]}.")
        await _record_run(conn, pipeline_id, "success", len(rows) if action != "skip_and_keep_previous" else 0, 0)
        await _set_status(conn, pipeline_id, "healthy")
    _held.pop(audit_id, None)
    await _broadcast("manual_fix_completed", {"pipeline_id": pipeline_id,
                    "audit_id": audit_id, "status": "manually_fixed",
                    "action": action, "fixed_by": fixed_by})
    return {"pipeline_id": pipeline_id, "audit_id": audit_id,
            "status": "manually_fixed", "action": action}


async def trigger_demo(error_type: str = "row_count_anomaly") -> dict[str, Any]:
    if error_type not in ALLOWED_TYPES:
        raise ValueError(f"Unsupported error type: {error_type}")
    return await _inject_and_process(DEMO_PIPELINE, forced_error_type=error_type,
                                     count_cycle=False)
